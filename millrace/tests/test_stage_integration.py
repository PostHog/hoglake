"""Integration tests for the staging layer: real SlateDB on ``file:///``.

What the memory-backed component layer cannot see: persistence across a
process boundary. The hard-kill test stages through a subprocess that is
SIGKILLed mid-stream — every ACKNOWLEDGED row (the ack implies remote
durability) must be present and byte-exact on reopen, batch-atomic, and
reconciled exactly with the stats the planner would read. The reassignment
test opens the same path from a fresh manager — the
assignment-changed-between-deploys case (staged state follows the
partition, never the pod).
"""

from __future__ import annotations

import asyncio
import select
import signal
import subprocess
import sys
from pathlib import Path

import pytest
from stagekit import NOW, fast_flush_settings, payload_for, rec, window

from millrace.keyspace import OffsetRange
from millrace.stage import PartitionStage, RecoveryReport, StagedRecord, StageManager

pytestmark = pytest.mark.integration

KILL_CHILD = Path(__file__).with_name("stage_kill_child.py")

BATCH_SIZE = 10
PAYLOAD_SIZE = 1024
TEAMS = 4
KILL_AFTER_ACKS = 12
EVENT_TS_BASE = 1_700_000_000_000_000


def _read_line(proc: subprocess.Popen[str], timeout_s: float) -> str:
    ready, _, _ = select.select([proc.stdout], [], [], timeout_s)
    if not ready:
        proc.kill()
        raise AssertionError(
            f"child produced no line within {timeout_s}s; stderr:\n{proc.stderr.read()}"
        )
    line = proc.stdout.readline().strip()
    if not line:
        raise AssertionError(
            f"child exited early (rc={proc.poll()}); stderr:\n{proc.stderr.read()}"
        )
    return line


def test_durability_across_hard_kill(tmp_path: Path) -> None:
    """A subprocess stages batches and is SIGKILLed mid-stream. On reopen:
    every acknowledged row is present and byte-exact, present rows come
    in whole batches (the write batch is the atomic unit), and the stats
    prefix reconciles exactly with the row scans."""
    path = str(tmp_path / "millrace" / "events" / "0")
    proc = subprocess.Popen(
        [
            sys.executable,
            str(KILL_CHILD),
            path,
            "100000",
            str(BATCH_SIZE),
            str(PAYLOAD_SIZE),
            str(TEAMS),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    acked: list[tuple[int, int, int]] = []  # (batch_index, first_offset, last_offset)
    try:
        assert _read_line(proc, 60) == "READY"
        while len(acked) < KILL_AFTER_ACKS:
            line = _read_line(proc, 60)
            batch_index, first, last = map(int, line.split()[1:])
            assert batch_index == len(acked)
            assert first == (acked[-1][2] + 1 if acked else 0)  # contiguous stream
            assert last == first + BATCH_SIZE - 1
            acked.append((batch_index, first, last))
        proc.kill()  # SIGKILL, mid-stream
        proc.wait(timeout=30)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=30)
    assert proc.returncode == -signal.SIGKILL

    asyncio.run(_verify_after_kill(path, acked))


async def _verify_after_kill(path: str, acked: list[tuple[int, int, int]]) -> None:
    stage = await PartitionStage.open(
        "file:///", path, topic="events", partition=0, settings=fast_flush_settings()
    )
    try:
        rows_by_team: dict[int, dict[int, tuple[int, bytes]]] = {
            t: {} for t in range(TEAMS)
        }
        for team in range(TEAMS):
            async for rk, value in stage.scan_team_rows(team):
                rows_by_team[team][rk.offset] = (rk.timestamp_us, value)

        # 1. every acknowledged row survived, byte-exact, with its event ts
        for _, first, last in acked:
            for offset in range(first, last + 1):
                ts, value = rows_by_team[offset % TEAMS][offset]
                assert ts == EVENT_TS_BASE + offset
                assert value == payload_for(offset, PAYLOAD_SIZE)

        # 2. batch atomicity: present rows come in whole batches (an
        # un-acked batch may be wholly present or wholly absent, never split)
        present = {o for rows in rows_by_team.values() for o in rows}
        if present:
            for batch_index in range(max(present) // BATCH_SIZE + 1):
                batch_offsets = set(
                    range(batch_index * BATCH_SIZE, (batch_index + 1) * BATCH_SIZE)
                )
                landed = len(batch_offsets & present)
                assert landed in (0, BATCH_SIZE), f"batch {batch_index} split: {landed}"

        # 3. the stats prefix reconciles EXACTLY with the row scans
        stats = {s.team_id: s for s in await stage.iter_key_stats()}
        assert set(stats) == {t for t in range(TEAMS) if rows_by_team[t]}
        last_landed_batch = max(present) // BATCH_SIZE
        for team, rows in stats.items():
            scanned = rows_by_team[team]
            assert rows.row_count == len(scanned)
            assert rows.staged_bytes == sum(len(v) for _, v in scanned.values())
            # every team appears in every batch: the first batch set the
            # floor, the last landed batch set the watermark
            assert rows.first_staged_ts == NOW
            assert rows.last_staged_ts == NOW + last_landed_batch

        # 4. gauges agree with the stats scan, and recovery is a clean no-op
        gauges = await stage.gauges()
        assert gauges.staged_rows == len(present)
        assert gauges.oldest_first_staged_ts == NOW
        assert await stage.recover() == RecoveryReport(pending_prepared=())

        # 5. the reopened instance keeps accepting writes
        ack = await stage.stage_batch(
            [
                StagedRecord(
                    team_id=0,
                    event_ts_us=EVENT_TS_BASE + 10**9,
                    offset=10**9,
                    payload=b"after",
                )
            ],
            now_us=NOW + 10**6,
        )
        assert ack.rows == 1
    finally:
        await stage.close()


async def test_assignment_change_between_deploys(tmp_path: Path):
    """Reopen-with-new-identity: a fresh manager (a new deploy's pod, for
    this layer's purposes) opens the same partition path, recovers
    cleanly, sees the previous owner's rows and stats exactly, and keeps
    staging on top."""
    base = str(tmp_path / "millrace")
    url = "file:///"

    first_owner = StageManager(url, base, settings=fast_flush_settings())
    opened = await first_owner.open_partition("events", 3)
    assert opened.recovery == RecoveryReport(pending_prepared=())
    await opened.stage.stage_batch([rec(11, 100 + i, i) for i in range(10)], now_us=NOW)
    await opened.stage.stage_batch(
        [rec(11, 200 + i, 10 + i) for i in range(10)], now_us=NOW + 1
    )
    await first_owner.close()

    second_owner = StageManager(url, base, settings=fast_flush_settings())
    assert second_owner.path_for("events", 3) == first_owner.path_for("events", 3)
    reopened = await second_owner.open_partition("events", 3)
    stage = reopened.stage
    assert reopened.recovery == RecoveryReport(pending_prepared=())

    (s,) = await stage.iter_key_stats()
    assert s.team_id == 11
    assert s.row_count == 20
    assert s.first_staged_ts == NOW  # set once by the first owner, never advanced
    assert s.last_staged_ts == NOW + 1
    rows = [rk.offset async for rk, _ in stage.scan_team_rows(11)]
    assert rows == list(range(20))
    # adjacent ranges keep their arrival structure (no coalescing)
    assert await stage.read_offsets(11) == (
        OffsetRange("events", 3, 0, 9),
        OffsetRange("events", 3, 10, 19),
    )

    # the new owner stages on top; totals stay exact
    await stage.stage_batch(
        [rec(11, 300 + i, 20 + i) for i in range(5)], now_us=NOW + 2
    )
    (s,) = await stage.iter_key_stats()
    assert s.row_count == 25
    assert s.first_staged_ts == NOW
    assert await stage.read_offsets(11) == (
        OffsetRange("events", 3, 0, 9),
        OffsetRange("events", 3, 10, 19),
        OffsetRange("events", 3, 20, 24),
    )
    await second_owner.close()


async def test_clean_reopen_replays_nothing_and_loses_nothing(tmp_path: Path):
    """Clean shutdown then reopen: no WAL loss, no pending recovery work."""
    path = str(tmp_path / "db")
    stage = await PartitionStage.open(
        "file:///", path, topic="events", partition=0, settings=fast_flush_settings()
    )
    await stage.stage_batch([rec(2, 100 + i, i) for i in range(6)], now_us=NOW)
    await stage.commit_flushed(window(2, 0, 3))
    await stage.close()

    reopened = await PartitionStage.open(
        "file:///", path, topic="events", partition=0, settings=fast_flush_settings()
    )
    report = await reopened.recover()
    assert report == RecoveryReport(pending_prepared=())  # the settle landed atomically
    assert [rk.offset async for rk, _ in reopened.scan_team_rows(2)] == [4, 5]
    (s,) = await reopened.iter_key_stats()
    assert s.row_count == 2
    await reopened.close()
