"""The startup drain (``FlushRunner.drain``) as a stream: component tests
over real ``file:///`` stages and a flusher double that settles each
window exactly as the real one does (stage.settle on the scanned keys).

Pinned here:

- every staged row of many groups across partitions is flushed (and only
  then deleted), each row once when nothing fails; the index ends empty;
- windows are one group each, cut at group boundaries and at the scan
  budget (``flush_bytes``), always at least one row;
- a failing group never blocks the stream: the rest of its rows are
  skipped and stay staged, it stays indexed, the drain retries it until
  it lands or is stuck, and the groups after it still flush;
- partitions drain concurrently, with at most ``concurrency`` windows in
  memory (read or in flight) across all of them;
- a stop (SIGTERM) mid-drain reads no further window and keeps every
  unflushed row; a fence or a halt is terminal;
- no scan runs under the stage write lock;
- progress: the ``drain_rows``/``drain_bytes`` counters and the per
  partition start / periodic / end log lines; an older layout's keys
  are deleted alongside.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

import pytest
from flushkit import FakeMonotonic, make_config
from slatedb.uniffi import CloseReason, WriteBatch
from slatedb.uniffi import Error as SlateError
from stagekit import NOW, LockedScanSpy, fast_flush_settings

import millrace.flush as flush_mod
import millrace.stage as stage_mod
from millrace import keyspace
from millrace.flush import (
    STUCK_AFTER,
    FlushError,
    FlushHalted,
    FlushReport,
    FlushRunner,
)
from millrace.stage import FencedError, PartitionStage, StagedRecord, StageManager

pytestmark = pytest.mark.component

TOPIC = "events"


def grp(team: int, day: int) -> tuple[str | None, ...]:
    """An identity(team_id) × day(timestamp) tuple, the production shape."""
    return (str(team), str(19_000 + day))


class StreamFlusher:
    """A flusher double: records every window it is handed, optionally
    waits on ``gate`` or fails, and otherwise settles the window's exact
    keys like ``HoglakeFlusher.flush_scan``."""

    def __init__(
        self,
        *,
        flush_bytes: int = 1 << 30,
        fail: dict[tuple[str | None, ...], int] | None = None,
        gate: asyncio.Event | None = None,
        fence: bool = False,
        halt: bool = False,
    ) -> None:
        self.flush_bytes = flush_bytes
        #: group → failures left (a large number: always fails).
        self.fail = dict(fail or {})
        self.gate = gate
        self.fence = fence
        self.halt = halt
        self.windows: list[tuple[int, tuple[str | None, ...], list[int], str]] = []
        self.failures = 0
        self.inflight = 0
        self.peak = 0
        self.inflight_partitions: list[int] = []
        self.peak_partitions = 0
        self.identity_checks: list[int] = []
        self.durable: list[bool] = []
        self.completes: list[bool] = []

    async def check_stage_identity(self, stage: Any, *, pin: bool = False) -> None:
        self.identity_checks.append(stage.partition)

    def note_failure(self) -> None:
        self.failures += 1

    async def flush_group(self, stage: Any, group: Any, trigger: str) -> FlushReport:
        scan = await stage.scan(group, max_bytes=self.flush_bytes, now_us=0)
        return await self.flush_scan(stage, scan, trigger)

    async def flush_scan(
        self, stage: Any, scan: Any, trigger: str, *, durable: bool = True
    ) -> FlushReport:
        self.inflight += 1
        self.peak = max(self.peak, self.inflight)
        self.inflight_partitions.append(stage.partition)
        self.peak_partitions = max(
            self.peak_partitions, len(set(self.inflight_partitions))
        )
        try:
            self.windows.append(
                (
                    stage.partition,
                    scan.group,
                    [rk.offset for rk, _ in scan.rows],
                    trigger,
                )
            )
            self.completes.append(scan.complete)
            if self.gate is not None:
                await self.gate.wait()
            if self.fence:
                raise SlateError.Closed(CloseReason.FENCED, "newer writer")
            if self.halt:
                raise FlushHalted("table_recreated", "scripted")
            left = self.fail.get(scan.group, 0)
            if left > 0:
                self.fail[scan.group] = left - 1
                raise FlushError("scripted refusal")
            self.durable.append(durable)
            await stage.settle(scan, durable=durable)
        finally:
            self.inflight -= 1
            self.inflight_partitions.remove(stage.partition)
        return FlushReport(
            stage.topic,
            stage.partition,
            scan.group,
            trigger,
            "committed",
            staged_rows=len(scan.rows),
            staged_bytes=scan.nbytes,
        )


class AdvancingSleeper:
    """The drain's backoff sleep: advances the fake monotonic clock."""

    def __init__(self, mono: FakeMonotonic) -> None:
        self.mono = mono
        self.calls: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)
        self.mono.advance(max(seconds, 1.0))
        await asyncio.sleep(0)


def manager(root: Path) -> StageManager:
    return StageManager("file:///", f"{root}/millrace", settings=fast_flush_settings())


async def previous_process(
    root: Path, layout: dict[int, list[tuple[tuple[str | None, ...], int, bytes]]]
) -> None:
    """A previous process staged ``layout`` (partition → (group, offset,
    payload)) and went away without flushing."""
    m = manager(root)
    for partition, rows in layout.items():
        stage = await m.open_partition(TOPIC, partition)
        await stage.stage_batch(
            [StagedRecord(group=g, offset=o, payload=p) for g, o, p in rows],
            now_us=NOW,
        )
    await m.close()


async def restarted(root: Path, partitions: list[int]) -> StageManager:
    m = manager(root)
    await m.sync_assignment({(TOPIC, p) for p in partitions})
    return m


async def disk_offsets(
    stage: PartitionStage,
) -> list[tuple[tuple[str | None, ...], int]]:
    start, end = keyspace.rows_range()
    from slatedb.uniffi import KeyRange

    it = await stage._db.scan(
        KeyRange(start=start, start_inclusive=True, end=end, end_inclusive=False)
    )
    out = []
    while chunk := await it.next_batch(1000):
        for kv in chunk:
            rk = keyspace.decode_row_key(kv.key)
            out.append((rk.group, rk.offset))
    return out


async def staged_total(stages: StageManager, partitions: tuple[int, ...]) -> int:
    total = 0
    for p in partitions:
        total += len(await disk_offsets(stages.stage(TOPIC, p)))
    return total


def make_runner(
    stages: StageManager,
    flusher: Any,
    *,
    concurrency: int = 4,
    mono: FakeMonotonic | None = None,
) -> FlushRunner:
    mono = mono or FakeMonotonic()
    return FlushRunner(
        stages,
        flusher,
        make_config().planner_knobs(),
        now_us=lambda: NOW,
        monotonic=mono,
        sleep=AdvancingSleeper(mono),
        sweep_s=1.0,
        concurrency=concurrency,
    )


def many_groups(
    partitions: int, teams: int, days: int, *, pad: int = 20
) -> dict[int, list[tuple[tuple[str | None, ...], int, bytes]]]:
    layout: dict[int, list[tuple[tuple[str | None, ...], int, bytes]]] = {}
    for p in range(partitions):
        rows = []
        offset = 0
        for t in range(teams):
            for d in range(days):
                for _ in range(1 + (t + d) % 3):
                    rows.append(
                        (grp(t, d), offset, f"{p}:{offset}:".encode() + b"x" * pad)
                    )
                    offset += 1
        layout[p] = rows
    return layout


async def test_the_drain_flushes_every_row_of_many_groups_across_partitions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = many_groups(partitions=3, teams=12, days=9)
    await previous_process(tmp_path, layout)
    stages = await restarted(tmp_path, [0, 1, 2])
    assert stages.gauges().staged_groups == 0  # opening reads nothing
    flusher = StreamFlusher()
    made_durable: list[int] = []
    real = PartitionStage.make_durable

    async def spied(self: PartitionStage) -> None:
        made_durable.append(self.partition)
        await real(self)

    monkeypatch.setattr(PartitionStage, "make_durable", spied)
    r = make_runner(stages, flusher)
    await r.drain()

    flushed = sorted((p, o) for p, _, offsets, _ in flusher.windows for o in offsets)
    expected = sorted((p, o) for p, rows in layout.items() for _, o, _ in rows)
    assert flushed == expected  # every row, once
    assert {t for *_, t in flusher.windows} == {"drain"}
    assert len(flusher.windows) == 3 * 12 * 9  # one window per group
    assert sorted(flusher.identity_checks) == [0, 1, 2]
    for p in range(3):
        assert await disk_offsets(stages.stage(TOPIC, p)) == []
    assert stages.gauges().staged_groups == 0
    assert r.drain_rows == len(expected)
    assert r.drain_bytes == sum(len(b) for rows in layout.values() for *_, b in rows)
    # The stream's settles skip the per-window durability wait; each
    # partition's stream ends with one durable flush.
    assert set(flusher.durable) == {False}
    assert sorted(made_durable) == [0, 1, 2]
    await stages.close()


async def test_windows_are_cut_at_group_boundaries_and_at_the_budget(
    tmp_path: Path,
) -> None:
    rows = (
        [(grp(1, 0), o, b"a" * 10) for o in range(7)]
        + [(grp(1, 1), 7, b"b" * 35)]
        + [(grp(2, 0), 8 + o, b"c" * 10) for o in range(3)]
    )
    await previous_process(tmp_path, {0: rows})
    stages = await restarted(tmp_path, [0])
    flusher = StreamFlusher(flush_bytes=30)
    await make_runner(stages, flusher, concurrency=1).drain()
    assert [(g, offs) for _, g, offs, _ in flusher.windows] == [
        (grp(1, 0), [0, 1, 2]),
        (grp(1, 0), [3, 4, 5]),
        (grp(1, 0), [6]),  # the group boundary cuts it
        (grp(1, 1), [7]),  # over the budget alone: never wedged
        (grp(2, 0), [8, 9, 10]),
    ]
    assert await disk_offsets(stages.stage(TOPIC, 0)) == []
    await stages.close()


async def test_a_stuck_group_stays_staged_and_indexed_without_blocking_the_stream(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    rows = (
        [(grp(1, 0), o, b"a" * 10) for o in range(2)]
        + [(grp(2, 0), 2 + o, b"b" * 10) for o in range(6)]
        + [(grp(3, 0), 8 + o, b"c" * 10) for o in range(2)]
    )
    await previous_process(tmp_path, {0: rows, 1: [(grp(9, 9), 0, b"z")]})
    stages = await restarted(tmp_path, [0, 1])
    flusher = StreamFlusher(flush_bytes=20, fail={grp(2, 0): 1_000})
    # One slot: the failure is noted before the stream reads on, so the
    # stuck group's later windows are skipped, deterministically.
    r = make_runner(stages, flusher, concurrency=1)
    with caplog.at_level(logging.ERROR, logger="millrace.flush"):
        await r.drain()

    stage = stages.stage(TOPIC, 0)
    stream_windows = [
        (g, offs) for p, g, offs, t in flusher.windows if t == "drain" and p == 0
    ]
    assert stream_windows == [
        (grp(1, 0), [0, 1]),
        (grp(2, 0), [2, 3]),  # fails: [4..7] are skipped, not attempted
        (grp(3, 0), [8, 9]),  # after the stuck group: still flushed
    ]
    retries = [t for _, g, _, t in flusher.windows if g == grp(2, 0) and t != "drain"]
    assert retries == ["retry"] * (STUCK_AFTER - 1)
    assert flusher.failures == STUCK_AFTER
    # Exactly the stuck group still has rows, and exactly it is indexed.
    assert await disk_offsets(stage) == [(grp(2, 0), o) for o in range(2, 8)]
    snap = stage.snapshot()
    assert [s.group for s in snap] == [grp(2, 0)]
    assert snap[0].rows >= 6
    assert stages.stage(TOPIC, 1).snapshot() == []
    assert r.stuck_groups == 1
    assert "startup drain finished with 1 stuck group(s)" in caplog.text
    # The normal loop picks it up once it is accepted.
    flusher.fail.clear()
    flusher.flush_bytes = 1 << 30
    r._failures.clear()
    assert (await r.run_once()).committed == 0  # not due by the planner...
    assert (await r.run_once(drain=True)).committed == 1  # ...but flushable
    assert await disk_offsets(stage) == []
    assert stage.snapshot() == []
    await stages.close()


async def test_a_transient_failure_is_retried_by_the_drain(tmp_path: Path) -> None:
    rows = [(grp(1, 0), o, b"a") for o in range(3)] + [(grp(2, 0), 3, b"b")]
    await previous_process(tmp_path, {0: rows})
    stages = await restarted(tmp_path, [0])
    flusher = StreamFlusher(fail={grp(1, 0): 1})
    r = make_runner(stages, flusher)
    await r.drain()
    assert await disk_offsets(stages.stage(TOPIC, 0)) == []
    assert stages.gauges().staged_groups == 0
    assert r.stuck_groups == 0
    assert [t for _, g, _, t in flusher.windows if g == grp(1, 0)] == [
        "drain",
        "retry",
    ]
    # The retry pass's rows count as drained too.
    assert (r.drain_rows, r.drain_bytes) == (4, 4)
    await stages.close()


async def test_partitions_drain_concurrently_with_bounded_windows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = many_groups(partitions=4, teams=3, days=2)
    await previous_process(tmp_path, layout)
    stages = await restarted(tmp_path, [0, 1, 2, 3])
    gate = asyncio.Event()
    flusher = StreamFlusher(gate=gate)
    read: list[int] = []
    from millrace.stage import RowStream

    real = RowStream.next_window

    async def counted(self: RowStream) -> Any:
        window = await real(self)
        if window is not None:
            read.append(1)
        return window

    monkeypatch.setattr(RowStream, "next_window", counted)
    r = make_runner(stages, flusher, concurrency=3)
    task = asyncio.create_task(r.drain())
    for _ in range(400):
        await asyncio.sleep(0.005)
        if flusher.peak >= 3:
            break
    await asyncio.sleep(0.05)
    # Three windows in flight, from more than one partition, and not one
    # more read ahead of a free slot.
    assert flusher.peak == 3
    assert flusher.peak_partitions >= 2
    assert len(read) == 3
    gate.set()
    await asyncio.wait_for(task, timeout=30)
    assert flusher.peak == 3
    for p in range(4):
        assert await disk_offsets(stages.stage(TOPIC, p)) == []
    await stages.close()


async def test_a_stop_mid_drain_reads_no_further_window_and_keeps_the_rows(
    tmp_path: Path,
) -> None:
    layout = many_groups(partitions=2, teams=4, days=3)
    await previous_process(tmp_path, layout)
    stages = await restarted(tmp_path, [0, 1])
    gate = asyncio.Event()
    flusher = StreamFlusher(gate=gate)
    r = make_runner(stages, flusher, concurrency=2)
    task = asyncio.create_task(r.drain())
    for _ in range(400):
        await asyncio.sleep(0.005)
        if flusher.peak >= 2:
            break
    r.stop()  # SIGTERM
    gate.set()
    await asyncio.wait_for(task, timeout=30)
    assert r.halted is None
    assert len(flusher.windows) == 2  # the in-flight ones finish; no more
    left = await staged_total(stages, (0, 1))
    total = sum(len(rows) for rows in layout.values())
    flushed = sum(len(offs) for _, _, offs, _ in flusher.windows)
    assert left == total - flushed > 0
    await stages.close()


@pytest.mark.parametrize("kind", ["fence", "halt"])
async def test_a_fence_or_a_halt_mid_drain_is_terminal(
    tmp_path: Path, kind: str
) -> None:
    layout = many_groups(partitions=2, teams=3, days=3)
    await previous_process(tmp_path, layout)
    stages = await restarted(tmp_path, [0, 1])
    flusher = StreamFlusher(fence=kind == "fence", halt=kind == "halt")
    r = make_runner(stages, flusher, concurrency=1)
    expected = FencedError if kind == "fence" else FlushHalted
    with pytest.raises(expected):
        await r.drain()
    assert isinstance(r.halted, expected)
    assert len(flusher.windows) <= 2  # at most one per partition stream
    assert flusher.failures == 0
    total = sum(len(rows) for rows in layout.values())
    left = await staged_total(stages, (0, 1))
    assert left == total
    await stages.close()


async def test_no_scan_runs_under_the_write_lock_during_the_drain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    await previous_process(tmp_path, many_groups(partitions=2, teams=5, days=4))
    stages = await restarted(tmp_path, [0, 1])
    for p in (0, 1):
        batch = WriteBatch()
        batch.put(b"stats/old" + bytes([p]), b"x")
        handle = await stages.stage(TOPIC, p)._db.write(batch)
        await handle.await_durable()
    spies = [LockedScanSpy(stages.stage(TOPIC, p), monkeypatch) for p in (0, 1)]
    await make_runner(stages, StreamFlusher(flush_bytes=30)).drain()
    for spy in spies:
        assert spy.calls, "the drain should have read"
        assert spy.under_lock == []
    await stages.close()


async def test_progress_is_logged_and_counted_and_old_keys_go(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    layout = many_groups(partitions=1, teams=5, days=2)
    await previous_process(tmp_path, layout)
    stages = await restarted(tmp_path, [0])
    stage = stages.stage(TOPIC, 0)
    batch = WriteBatch()
    for key in (b"stats/a", b"sched_age/b", b"prepared/c", b"flushed/d"):
        batch.put(key, b"old")
    handle = await stage._db.write(batch)
    await handle.await_durable()
    monkeypatch.setattr(flush_mod, "DRAIN_LOG_EVERY_WINDOWS", 4)
    r = make_runner(stages, StreamFlusher(), concurrency=1)
    with caplog.at_level(logging.INFO, logger="millrace.flush"):
        await r.drain()
    lines = [m for m in caplog.messages if m.startswith("startup drain of events[0]")]
    assert lines[0] == "startup drain of events[0]: streaming its staged rows"
    periodic = [m for m in lines[1:-1] if " window(s), " in m]
    assert len(periodic) >= 2
    rows = len(layout[0])
    nbytes = sum(len(b) for *_, b in layout[0])
    assert lines[-1].startswith(
        f"startup drain of events[0] streamed: 10 window(s), {rows} row(s), "
    )
    assert "from 10 group(s)" in lines[-1]
    assert (r.drain_rows, r.drain_bytes) == (rows, nbytes)
    assert any(
        "deleted 4 key(s) of an older staging layout" in m for m in caplog.messages
    )
    for key in (b"stats/a", b"sched_age/b", b"prepared/c", b"flushed/d"):
        assert await stage._db.get(key) is None
    await stages.close()


async def test_the_drain_forgets_a_stale_index_before_streaming(
    tmp_path: Path,
) -> None:
    """The stream decides what is staged; an index entry with no rows on
    disk does not survive the drain."""
    stages = await restarted(tmp_path, [0])
    stage = stages.stage(TOPIC, 0)
    await stage.stage_batch(
        [StagedRecord(group=grp(1, 1), offset=0, payload=b"p")], now_us=NOW
    )
    stage.row_stream(max_bytes=1, now_us=NOW).hold(
        grp(5, 5), nbytes=9, rows=9, now_us=NOW
    )
    flusher = StreamFlusher()
    await make_runner(stages, flusher).drain()
    assert [g for _, g, _, _ in flusher.windows] == [grp(1, 1)]
    assert stage.snapshot() == []
    assert await disk_offsets(stage) == []
    await stages.close()


class SplitHeldFlusher(StreamFlusher):
    """Group ``grp(1, 0)`` splits into two drain windows: the first waits
    on ``fail_first`` and fails, the second waits on ``settle_second``
    and settles. Its retries fail while ``refuse`` is set."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.fail_first = asyncio.Event()
        self.settle_second = asyncio.Event()
        self.entered: list[int] = []
        self.done: list[int] = []
        self.refuse = True

    async def flush_scan(
        self, stage: Any, scan: Any, trigger: str, *, durable: bool = True
    ) -> FlushReport:
        if scan.group != grp(1, 0):
            return await super().flush_scan(stage, scan, trigger, durable=durable)
        first = scan.rows[0][0].offset
        self.windows.append(
            (stage.partition, scan.group, [rk.offset for rk, _ in scan.rows], trigger)
        )
        if scan.gen is not None:  # a scan of the index: the normal loop
            if self.refuse:
                raise FlushError("still refused")
            await stage.settle(scan, durable=durable)
        elif first == 0:
            self.entered.append(first)
            await self.fail_first.wait()
            self.done.append(first)
            raise FlushError("window 1 refused")
        else:
            self.entered.append(first)
            await self.settle_second.wait()
            await stage.settle(scan, durable=durable)
            self.done.append(first)
        return FlushReport(
            stage.topic,
            stage.partition,
            scan.group,
            trigger,
            "committed",
            staged_rows=len(scan.rows),
            staged_bytes=scan.nbytes,
        )


async def _until(pred: Any, what: str) -> None:
    for _ in range(1000):
        if pred():
            return
        await asyncio.sleep(0.005)
    raise AssertionError(f"timed out waiting for {what}")


@pytest.mark.parametrize("order", ["second_settles_first", "first_fails_first"])
async def test_a_held_group_with_split_windows_in_flight_stays_indexed(
    tmp_path: Path, order: str
) -> None:
    """Window 2 of a group is dispatched before window 1 fails; in either
    settle order the group stays indexed (held) until a normal-loop
    flush has taken its last rows."""
    rows = [(grp(1, 0), o, b"a" * 10) for o in range(4)] + [(grp(2, 0), 4, b"b")]
    await previous_process(tmp_path, {0: rows})
    stages = await restarted(tmp_path, [0])
    stage = stages.stage(TOPIC, 0)
    flusher = SplitHeldFlusher(flush_bytes=20)
    r = make_runner(stages, flusher, concurrency=2)
    task = asyncio.create_task(r.drain())
    await _until(lambda: len(flusher.entered) == 2, "both windows in flight")
    if order == "second_settles_first":
        flusher.settle_second.set()
        await _until(lambda: flusher.done == [2], "window 2 settled")
        assert stage.snapshot() == []  # nothing held yet
        flusher.fail_first.set()
    else:
        flusher.fail_first.set()
        await _until(lambda: stage.has_group(grp(1, 0)), "the hold")
        flusher.settle_second.set()
    await asyncio.wait_for(task, timeout=30)

    # Window 2's settle (complete, the group's last rows) did not release
    # the held group: window 1's rows are still staged.
    assert await disk_offsets(stage) == [(grp(1, 0), 0), (grp(1, 0), 1)]
    snap = stage.snapshot()
    assert [(s.group, s.held) for s in snap] == [(grp(1, 0), True)]
    assert r.stuck_groups == 1
    # Only a successful normal-loop flush of it releases it.
    flusher.refuse = False
    r._failures.clear()
    assert (await r.run_once(drain=True)).committed == 1
    assert await disk_offsets(stage) == []
    assert stage.snapshot() == []
    await stages.close()


async def test_settles_interleave_with_paged_reads_and_each_row_flushes_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Many pages per partition (small pages), split windows, settles
    of earlier windows landing while later pages are read, concurrency
    above one: every offset is flushed exactly once and the disk ends
    empty."""
    monkeypatch.setattr(stage_mod, "_PAGE_FETCH", 7)
    monkeypatch.setattr(stage_mod, "_PAGE_MAX_ROWS", 20)
    layout = many_groups(partitions=2, teams=20, days=8, pad=20)
    assert all(len(rows) > 10 * 20 for rows in layout.values())
    await previous_process(tmp_path, layout)
    stages = await restarted(tmp_path, [0, 1])
    flusher = StreamFlusher(flush_bytes=60)  # splits the 2- and 3-row groups
    await make_runner(stages, flusher, concurrency=3).drain()
    flushed = [(p, o) for p, _, offsets, _ in flusher.windows for o in offsets]
    assert len(flushed) == len(set(flushed))
    assert sorted(flushed) == sorted(
        (p, o) for p, rows in layout.items() for _, o, _ in rows
    )
    assert any(not complete for complete in flusher.completes)
    for p in (0, 1):
        assert await disk_offsets(stages.stage(TOPIC, p)) == []
    await stages.close()


async def test_a_read_error_mid_stream_reopens_and_flushes_every_row_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """An SST the maintenance GC deleted under the long-lived iterator
    (object-store NotFound) fails a read: the stream reopens just past
    the last key it read and the drain flushes every row once."""
    monkeypatch.setattr(stage_mod, "_PAGE_FETCH", 5)
    monkeypatch.setattr(stage_mod, "_PAGE_MAX_ROWS", 10)
    layout = many_groups(partitions=1, teams=10, days=6)
    await previous_process(tmp_path, layout)
    stages = await restarted(tmp_path, [0])
    stage = stages.stage(TOPIC, 0)
    real = stage._db.scan_with_options
    opened = 0

    class Flaky:
        def __init__(self, it: Any) -> None:
            self._it = it
            self.calls = 0

        async def next_batch(self, n: int) -> Any:
            self.calls += 1
            if opened == 1 and self.calls == 4:
                raise SlateError.Data("object not found: L0 SST deleted by GC")
            return await self._it.next_batch(n)

    async def scan_with_options(range_: Any, options: Any) -> Any:
        nonlocal opened
        if range_.start is None or not bytes(range_.start).startswith(b"rows/"):
            return await real(range_, options)  # the old-layout cleanup
        opened += 1
        return Flaky(await real(range_, options))

    monkeypatch.setattr(stage._db, "scan_with_options", scan_with_options)
    flusher = StreamFlusher()
    with caplog.at_level(logging.WARNING, logger="millrace.stage"):
        await make_runner(stages, flusher, concurrency=2).drain()
    assert opened == 2  # the first open and one reopen
    assert "reopening past the last key read" in caplog.text
    flushed = [o for _, _, offsets, _ in flusher.windows for o in offsets]
    assert sorted(flushed) == sorted(o for _, o, _ in layout[0])
    assert len(flushed) == len(set(flushed))
    assert await disk_offsets(stage) == []
    await stages.close()


async def test_a_failing_stream_cancels_its_siblings_and_logs_its_end(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    layout = many_groups(partitions=2, teams=20, days=10)
    await previous_process(tmp_path, layout)
    stages = await restarted(tmp_path, [0, 1])
    from millrace.stage import RowStream

    real = RowStream.next_window

    async def broken(self: RowStream) -> Any:
        if self._stage.partition == 1:
            raise RuntimeError("stream broke")
        return await real(self)

    monkeypatch.setattr(RowStream, "next_window", broken)

    class Slow(StreamFlusher):
        async def flush_scan(self, *a: Any, **k: Any) -> FlushReport:
            await asyncio.sleep(0.01)
            return await super().flush_scan(*a, **k)

    flusher = Slow()
    with (
        caplog.at_level(logging.INFO, logger="millrace.flush"),
        pytest.raises(RuntimeError, match="stream broke"),
    ):
        await asyncio.wait_for(make_runner(stages, flusher).drain(), 30)
    # Partition 0 was stopped, not run to its end.
    assert len(flusher.windows) < len({g for g, _, _ in layout[0]})
    assert any(
        m.startswith("startup drain of events[1] failed:") for m in caplog.messages
    )
    assert any(
        m.startswith("startup drain of events[0] cancelled:") for m in caplog.messages
    )
    await stages.close()
