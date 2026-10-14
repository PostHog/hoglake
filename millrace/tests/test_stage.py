"""Component tests for the per-partition staging layer (``memory:///``).

Pinned here:

- ``stage_batch`` is one durable write batch; the in-memory index is
  updated before the write (memory always covers disk); duplicate keys
  and empty batches are refused; a redelivered row re-puts in place;
- ``discover`` finds exactly the groups with rows (prefix-sharing tuples
  included) without reading their payloads;
- ``scan`` is an exact offset prefix within the byte budget and always
  holds at least one row; ``complete`` says whether it reached the end;
- ``settle`` deletes exactly the scanned keys; a group leaves the index
  only when its scan was complete and no write touched it since; a
  write during the scan keeps it and lifts ``first_staged_ts``;
- the invariant the delivery contract stands on: a row on disk always
  has its group in the index, under concurrent staging and flushing;
- the write lock covers writes only: no scan, no iterator page and no
  payload lowering ever runs under it, and lowering runs off the loop;
- the fence probe writes one ``meta/`` key, never a row;
- the manager, the shared SST cache and the SlateDB settings.
"""

from __future__ import annotations

import asyncio
import json
import random
import threading

import pytest
from slatedb.uniffi import CloseReason, DurabilityLevel, KeyRange, WriteBatch
from slatedb.uniffi import Error as SlateError
from stagekit import NOW, LockedScanSpy, fast_flush_settings, g, rec, shared_cache

from millrace import keyspace
from millrace import stage as stage_mod
from millrace.keyspace import RowKey
from millrace.stage import (
    FencedError,
    PartitionStage,
    Scan,
    StageClosedError,
    StagedRecord,
    StageError,
    StageManager,
    build_slatedb_settings,
    is_fenced,
)

pytestmark = pytest.mark.component

BIG = 1 << 40


async def disk_rows(stage: PartitionStage) -> list[tuple[RowKey, bytes]]:
    """Every row on disk (white-box, default scan options)."""
    start, end = keyspace.rows_range()
    it = await stage._db.scan(
        KeyRange(start=start, start_inclusive=True, end=end, end_inclusive=False)
    )
    out: list[tuple[RowKey, bytes]] = []
    while chunk := await it.next_batch(1000):
        out.extend((keyspace.decode_row_key(kv.key), kv.value) for kv in chunk)
    return out


async def offsets(stage: PartitionStage, team: int) -> list[int]:
    return [rk.offset for rk, _ in await disk_rows(stage) if rk.group == g(team)]


def index(stage: PartitionStage) -> dict[tuple[str | None, ...], tuple[int, int, int]]:
    return {
        s.group: (s.first_staged_ts, s.staged_bytes, s.rows) for s in stage.snapshot()
    }


# -- staging ----------------------------------------------------------------------


async def test_stage_batch_round_trip_in_key_order_and_index(stage_factory):
    stage = await stage_factory()
    recs = [rec(2, 0, 5), rec(1, 0, 9), rec(2, 0, 3), rec(1, 0, -4)]
    ack = await stage.stage_batch(recs, now_us=NOW)
    assert (ack.rows, ack.first_offset, ack.last_offset) == (4, -4, 9)
    assert ack.staged_bytes == sum(len(r.payload) for r in recs)
    rows = await disk_rows(stage)
    assert [(rk.group, rk.offset) for rk, _ in rows] == [
        (g(1), -4),
        (g(1), 9),
        (g(2), 3),
        (g(2), 5),
    ]
    assert {rk.offset: v for rk, v in rows} == {r.offset: r.payload for r in recs}
    one = sum(len(r.payload) for r in recs if r.group == g(1))
    assert index(stage)[g(1)] == (NOW, one, 2)
    # Arrivals never move first_staged_ts; sizes accumulate.
    await stage.stage_batch([rec(1, 0, 10)], now_us=NOW + 5)
    assert index(stage)[g(1)][0] == NOW
    assert index(stage)[g(1)][2] == 3
    await stage.close()


async def test_a_redelivered_row_re_puts_in_place(stage_factory):
    stage = await stage_factory()
    await stage.stage_batch([rec(1, 0, 0, b"a")], now_us=NOW)
    await stage.stage_batch([rec(1, 0, 0, b"a")], now_us=NOW + 1)
    assert await offsets(stage, 1) == [0]
    assert index(stage)[g(1)][1:] == (2, 2)  # counted twice: a harmless over-count
    await stage.close()


async def test_stage_batch_refuses_bad_input(stage_factory):
    stage = await stage_factory()
    with pytest.raises(ValueError, match="at least one record"):
        await stage.stage_batch([], now_us=NOW)
    with pytest.raises(ValueError, match="duplicate row key"):
        await stage.stage_batch([rec(1, 0, 3), rec(1, 0, 3)], now_us=NOW)
    with pytest.raises(TypeError):
        StagedRecord(group=g(1), offset=0, payload="text")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        await stage.stage_batch([rec(1, 0, 1 << 63)], now_us=NOW)
    assert await disk_rows(stage) == []
    assert stage.snapshot() == []
    await stage.close()


async def test_the_index_is_updated_before_the_write(stage_factory, monkeypatch):
    """Memory covers disk: a write that fails still left its groups in
    the index (an over-count a flush settles away), never the reverse."""
    stage = await stage_factory()

    async def failing_write(batch):
        assert g(4) in index(stage)
        raise RuntimeError("store down")

    monkeypatch.setattr(stage._db, "write", failing_write)
    with pytest.raises(RuntimeError, match="store down"):
        await stage.stage_batch([rec(4, 0, 0)], now_us=NOW)
    assert g(4) in index(stage)
    monkeypatch.undo()
    scan = await stage.scan(g(4), max_bytes=BIG, now_us=NOW)
    assert scan.rows == [] and scan.complete
    assert await stage.settle(scan)
    assert stage.snapshot() == []
    await stage.close()


async def test_every_write_awaits_durability(stage_factory, monkeypatch):
    stage = await stage_factory()
    real_write = stage._db.write
    events: list[str] = []

    class Handle:
        def __init__(self, real):
            self._real = real

        async def await_durable(self):
            await self._real.await_durable()
            events.append("durable")

    async def write(batch):
        events.append("write")
        return Handle(await real_write(batch))

    monkeypatch.setattr(stage._db, "write", write)
    await stage.stage_batch([rec(1, 0, 0)], now_us=NOW)
    assert events == ["write", "durable"]
    await stage.probe_fence(0)
    scan = await stage.scan(g(1), max_bytes=BIG, now_us=NOW)
    await stage.settle(scan)
    assert events == ["write", "durable"] * 3
    await stage.close()


# -- discovery ----------------------------------------------------------------------


async def test_discover_finds_exactly_the_groups_with_rows(memory_store):
    first = await PartitionStage.open_store(
        memory_store,
        "millrace/events/0",
        topic="events",
        partition=0,
        settings=fast_flush_settings(),
        **shared_cache(),
    )
    groups = [("a",), ("a", "b"), ("a", None), (None,), (), ("ab",), ("a", "")]
    groups += [(str(i), "x") for i in range(150)]
    await first.stage_batch(
        [
            StagedRecord(group=grp, offset=i * 10 + j, payload=b"p" * (j + 1))
            for i, grp in enumerate(groups)
            for j in range(3)
        ],
        now_us=NOW,
    )
    # Drained groups and the fence probe are not rows.
    drained = await first.scan(("a", "b"), max_bytes=BIG, now_us=NOW)
    await first.settle(drained)
    await first.probe_fence(7)
    await first.close()

    reopened = await PartitionStage.open_store(
        memory_store,
        "millrace/events/0",
        topic="events",
        partition=0,
        settings=fast_flush_settings(),
        **shared_cache(),
    )
    assert reopened.snapshot() == []
    found = await reopened.discover(now_us=NOW + 99)
    expected = sorted(
        (grp for grp in groups if grp != ("a", "b")), key=keyspace.encode_group
    )
    assert found == expected  # key order, each once
    assert {s.group for s in reopened.snapshot()} == set(expected)
    assert {s.first_staged_ts for s in reopened.snapshot()} == {NOW + 99}
    assert {(s.staged_bytes, s.rows) for s in reopened.snapshot()} == {(0, 0)}
    # Discovery reads one row per group, not the payloads.
    await reopened.close()


async def test_discover_on_an_empty_stage_finds_nothing(stage_factory):
    stage = await stage_factory()
    await stage.probe_fence(1)
    assert await stage.discover(now_us=NOW) == []
    await stage.close()


# -- scan --------------------------------------------------------------------------


async def test_a_scan_is_an_exact_prefix_within_the_budget(stage_factory):
    stage = await stage_factory()
    await stage.stage_batch(
        [rec(1, 0, o, b"x" * 10) for o in range(5)] + [rec(2, 0, 1, b"y")], now_us=NOW
    )
    capped = await stage.scan(g(1), max_bytes=25, now_us=NOW + 3)
    assert [rk.offset for rk, _ in capped.rows] == [0, 1]
    assert not capped.complete
    assert capped.nbytes == 20
    assert capped.started_us == NOW + 3
    exact = await stage.scan(g(1), max_bytes=50, now_us=NOW)
    assert [rk.offset for rk, _ in exact.rows] == [0, 1, 2, 3, 4]
    assert exact.complete
    assert exact.gen == capped.gen
    await stage.close()


async def test_a_scan_always_holds_at_least_one_row(stage_factory):
    stage = await stage_factory()
    await stage.stage_batch([rec(1, 0, o, b"z" * 100) for o in range(3)], now_us=NOW)
    scan = await stage.scan(g(1), max_bytes=1, now_us=NOW)
    assert [rk.offset for rk, _ in scan.rows] == [0]
    assert not scan.complete
    single = await stage.stage_batch([rec(9, 0, 0, b"q" * 100)], now_us=NOW)
    assert single.rows == 1
    only = await stage.scan(g(9), max_bytes=1, now_us=NOW)
    assert len(only.rows) == 1 and only.complete
    with pytest.raises(ValueError, match="max_bytes"):
        await stage.scan(g(1), max_bytes=0, now_us=NOW)
    await stage.close()


async def test_a_scan_of_an_unknown_group_is_empty_and_complete(stage_factory):
    stage = await stage_factory()
    scan = await stage.scan(g(3), max_bytes=10, now_us=NOW)
    assert (scan.rows, scan.complete, scan.gen) == ([], True, None)
    assert await stage.settle(scan)
    await stage.close()


async def test_scans_read_ahead(stage_factory, monkeypatch):
    """Row scans use the bulk read-ahead options (#380)."""
    stage = await stage_factory()
    await stage.stage_batch([rec(5, 0, i) for i in range(5)], now_us=NOW)
    seen = []
    real = stage._db.scan_with_options

    async def spied(range_, options):
        seen.append(options)
        return await real(range_, options)

    monkeypatch.setattr(stage._db, "scan_with_options", spied)
    scan = await stage.scan(g(5), max_bytes=BIG, now_us=NOW)
    assert len(scan.rows) == 5
    (options,) = seen
    assert (
        options.read_ahead_bytes,
        options.max_fetch_tasks,
        options.cache_blocks,
        options.durability_filter,
        options.dirty,
    ) == (
        4 * 1024 * 1024,
        4,
        False,
        DurabilityLevel.MEMORY,
        False,
    )
    await stage.close()


# -- settle ------------------------------------------------------------------------


async def test_settle_deletes_exactly_the_scanned_keys(stage_factory):
    stage = await stage_factory()
    await stage.stage_batch(
        [rec(1, 0, o, b"x" * 10) for o in range(6)] + [rec(2, 0, 3)], now_us=NOW
    )
    scan = await stage.scan(g(1), max_bytes=30, now_us=NOW)
    assert [rk.offset for rk, _ in scan.rows] == [0, 1, 2]
    # A row re-keyed into the scanned range after the scan survives.
    await stage.stage_batch([rec(1, 0, -1)], now_us=NOW)
    assert not await stage.settle(scan)
    assert await offsets(stage, 1) == [-1, 3, 4, 5]
    assert await offsets(stage, 2) == [3]
    await stage.close()


async def test_a_complete_untouched_scan_drains_the_group(stage_factory):
    stage = await stage_factory()
    await stage.stage_batch([rec(1, 0, o) for o in range(3)], now_us=NOW)
    scan = await stage.scan(g(1), max_bytes=BIG, now_us=NOW + 10)
    assert await stage.settle(scan)
    assert stage.snapshot() == []
    assert await offsets(stage, 1) == []
    await stage.close()


async def test_a_capped_scan_keeps_the_group_and_its_age(stage_factory):
    stage = await stage_factory()
    await stage.stage_batch([rec(1, 0, o, b"x" * 10) for o in range(4)], now_us=NOW)
    scan = await stage.scan(g(1), max_bytes=10, now_us=NOW + 50)
    assert not stage.snapshot()[0].backlog
    assert not await stage.settle(scan)
    assert index(stage)[g(1)] == (NOW, 30, 3)
    # The rows left behind are due on the next sweep, whatever their size.
    (snap,) = stage.snapshot()
    assert snap.backlog
    # A later complete settle that keeps the group (a write since) clears it.
    rest = await stage.scan(g(1), max_bytes=BIG, now_us=NOW + 60)
    await stage.stage_batch([rec(1, 0, 9)], now_us=NOW + 61)
    assert not await stage.settle(rest)
    (snap,) = stage.snapshot()
    assert not snap.backlog
    await stage.close()


async def test_a_write_during_the_scan_keeps_the_group_and_lifts_its_age(stage_factory):
    stage = await stage_factory()
    await stage.stage_batch([rec(1, 0, o, b"x" * 10) for o in range(2)], now_us=NOW)
    scan = await stage.scan(g(1), max_bytes=BIG, now_us=NOW + 100)
    assert scan.complete
    await stage.stage_batch([rec(1, 0, 7, b"y" * 5)], now_us=NOW + 101)
    assert not await stage.settle(scan)
    assert index(stage)[g(1)] == (NOW + 100, 5, 1)
    assert await offsets(stage, 1) == [7]
    # The lift never lowers.
    stale = Scan(g(1), [], True, -5, NOW - 1)
    assert not await stage.settle(stale)
    assert index(stage)[g(1)][0] == NOW + 100
    await stage.close()


async def test_a_group_created_after_the_scan_is_not_dropped(stage_factory):
    stage = await stage_factory()
    scan = await stage.scan(g(1), max_bytes=BIG, now_us=NOW)
    assert scan.gen is None
    await stage.stage_batch([rec(1, 0, 0)], now_us=NOW)
    assert not await stage.settle(scan)
    assert g(1) in index(stage)
    await stage.close()


@pytest.mark.parametrize("seed", [20261014, 1, 7, 42, 31337])
async def test_the_index_never_loses_a_group_with_rows_under_interleaving(
    stage_factory, seed
):
    """I2: under concurrent staging and flushing (seeded interleavings),
    every row on disk has its group in the index."""
    stage = await stage_factory()
    rng = random.Random(seed)
    next_offset = 0

    async def stage_some() -> None:
        nonlocal next_offset
        recs = []
        for _ in range(rng.randrange(1, 5)):
            recs.append(
                rec(rng.randrange(4), 0, next_offset, b"v" * rng.randrange(1, 9))
            )
            next_offset += 1
        await stage.stage_batch(recs, now_us=NOW + next_offset)

    async def flush_some() -> None:
        team = rng.randrange(4)
        scan = await stage.scan(
            g(team), max_bytes=rng.choice([1, 8, 30, BIG]), now_us=NOW
        )
        await asyncio.sleep(0)
        await stage.settle(scan)

    for _ in range(60):
        await asyncio.gather(
            *(
                rng.choice([stage_some, flush_some])()
                for _ in range(rng.randrange(1, 6))
            )
        )
        groups = {s.group for s in stage.snapshot()}
        for rk, _ in await disk_rows(stage):
            assert rk.group in groups
    # Draining everything empties both.
    for team in range(4):
        while True:
            scan = await stage.scan(g(team), max_bytes=BIG, now_us=NOW)
            if await stage.settle(scan):
                break
    assert await disk_rows(stage) == []
    assert stage.snapshot() == []
    await stage.close()


# -- the write lock covers writes only ----------------------------------------------


async def test_the_lock_spy_sees_a_scan_under_the_lock(stage_factory, monkeypatch):
    """Positive control for :class:`stagekit.LockedScanSpy`."""
    from slatedb.uniffi import IsolationLevel

    stage = await stage_factory()
    await stage.stage_batch([rec(5, 0, 0), rec(5, 0, 1)], now_us=NOW)
    spy = LockedScanSpy(stage, monkeypatch)
    everything = KeyRange(
        start=None, start_inclusive=False, end=None, end_inclusive=False
    )
    early = await stage._db.scan(everything)
    assert spy.under_lock == []
    async with stage._write_lock:
        await early.next_batch(1)
        it = await stage._db.scan(everything)
        await it.next_batch(10)
        prefixed = await stage._db.scan_prefix(keyspace.ROWS_PREFIX, everything)
        assert await prefixed.next() is not None
        await prefixed.seek(keyspace.encode_row_key(RowKey(g(5), 1)))
        txn = await stage._db.begin(IsolationLevel.SNAPSHOT)
        await (await txn.scan(everything)).next_batch(1)
        await txn.rollback()
        snapshot = await stage._db.snapshot()
        await (await snapshot.scan(everything)).next_batch(1)
    assert spy.under_lock == [
        "next_batch",
        "db.scan",
        "next_batch",
        "db.scan_prefix",
        "next",
        "seek",
        "txn.scan",
        "next_batch",
        "snapshot.scan",
        "next_batch",
    ]
    await stage.close()


async def test_no_scan_runs_under_the_write_lock(stage_factory, monkeypatch):
    stage = await stage_factory()
    spy = LockedScanSpy(stage, monkeypatch)
    await stage.stage_batch([rec(5, 0, o) for o in range(6)], now_us=NOW)
    await stage.discover(now_us=NOW)
    scan = await stage.scan(g(5), max_bytes=1, now_us=NOW)
    await asyncio.gather(
        stage.settle(scan),
        stage.stage_batch([rec(6, 0, 9)], now_us=NOW),
        stage.probe_fence(9),
    )
    full = await stage.scan(g(5), max_bytes=BIG, now_us=NOW)
    await stage.settle(full)
    assert spy.calls, "discovery and the scans should have read"
    assert spy.under_lock == []
    await stage.close()


async def test_no_payload_lowering_under_the_lock_or_on_the_loop(
    stage_factory, monkeypatch
):
    """Payloads are lowered into the binding (``WriteBatch.put``) on a
    worker thread before the lock is taken; deletes likewise."""
    stage = await stage_factory()
    loop_thread = threading.get_ident()
    calls: list[tuple[str, bool, bool]] = []
    real_batch = stage_mod.WriteBatch

    class SpyBatch(real_batch):  # type: ignore[misc, valid-type]
        def put(self, key: bytes, value: bytes) -> None:
            calls.append(
                (
                    "put",
                    stage._write_lock.locked(),
                    threading.get_ident() == loop_thread,
                )
            )
            super().put(key, value)

        def delete(self, key: bytes) -> None:
            calls.append(
                (
                    "delete",
                    stage._write_lock.locked(),
                    threading.get_ident() == loop_thread,
                )
            )
            super().delete(key)

    monkeypatch.setattr(stage_mod, "WriteBatch", SpyBatch)

    # The stage's own writes are the only lock holders here, so a put or
    # delete seeing the lock held would be lowering under it.
    await stage.stage_batch([rec(1, 0, o, b"p" * 64) for o in range(50)], now_us=NOW)
    scan = await stage.scan(g(1), max_bytes=BIG, now_us=NOW)
    await stage.settle(scan)
    puts = [c for c in calls if c[0] == "put"]
    deletes = [c for c in calls if c[0] == "delete"]
    assert len(puts) == 50 and len(deletes) == 50
    assert all(not locked and not on_loop for _, locked, on_loop in calls)
    assert await disk_rows(stage) == []
    # Positive control: a put under the lock, on the loop, is seen.
    async with stage._write_lock:
        stage_mod.WriteBatch().put(b"k", b"v")
    assert calls[-1] == ("put", True, True)
    await stage.close()


# -- the fence probe ---------------------------------------------------------------


async def test_probe_fence_writes_one_meta_key_and_no_row(stage_factory):
    stage = await stage_factory()
    await stage.probe_fence(41)
    await stage.probe_fence(42)
    assert await stage._db.get(keyspace.FENCE_PROBE_KEY) == (42).to_bytes(8, "big")
    assert await disk_rows(stage) == []
    assert stage.snapshot() == []
    await stage.close()


def test_is_fenced_and_fenced_error():
    assert is_fenced(SlateError.Closed(CloseReason.FENCED, "newer writer"))
    assert not is_fenced(SlateError.Closed(CloseReason.CLEAN, "local"))
    assert not is_fenced(RuntimeError("x"))
    err = FencedError([("events", 3), ("events", 1)])
    assert err.partitions == (("events", 1), ("events", 3))
    assert "events[1], events[3]" in str(err)
    assert isinstance(err, StageError)


# -- close -------------------------------------------------------------------------


async def test_close_guards(stage_factory):
    stage = await stage_factory()
    await stage.stage_batch([rec(1, 0, 0)], now_us=NOW)
    await stage.close()
    await stage.close()  # idempotent
    with pytest.raises(StageClosedError):
        await stage.stage_batch([rec(1, 0, 1)], now_us=NOW)
    with pytest.raises(StageClosedError):
        await stage.scan(g(1), max_bytes=1, now_us=NOW)
    with pytest.raises(StageClosedError):
        await stage.settle(Scan(g(1), [], True, None, NOW))
    with pytest.raises(StageClosedError):
        await stage.probe_fence(1)
    with pytest.raises(StageClosedError):
        await stage.discover(now_us=NOW)
    async with await stage_factory(partition=1) as managed:
        await managed.stage_batch([rec(1, 0, 0)], now_us=NOW)
    with pytest.raises(StageClosedError):
        await managed.probe_fence(1)


async def test_close_swallows_a_fenced_shutdown(stage_factory, monkeypatch):
    stage = await stage_factory()
    shutdown = stage._db.shutdown
    calls = 0

    async def shutdown_fenced():
        nonlocal calls
        calls += 1
        await shutdown()
        raise SlateError.Closed(CloseReason.FENCED, "newer writer owns the path")

    monkeypatch.setattr(stage._db, "shutdown", shutdown_fenced)
    await stage.close()
    await stage.close()
    assert calls == 1


@pytest.mark.parametrize(
    "error",
    [
        SlateError.Closed(CloseReason.CLEAN, "local shutdown"),
        RuntimeError("shutdown failed"),
    ],
)
async def test_close_propagates_other_shutdown_errors(
    stage_factory, monkeypatch, error
):
    stage = await stage_factory()
    shutdown = stage._db.shutdown

    async def shutdown_failed():
        await shutdown()
        raise error

    monkeypatch.setattr(stage._db, "shutdown", shutdown_failed)
    with pytest.raises(type(error)):
        await stage.close()
    await stage.close()


# -- the manager -------------------------------------------------------------------


def manager(**kwargs) -> StageManager:
    return StageManager(
        "memory:///", "millrace", settings=fast_flush_settings(), **kwargs
    )


async def test_manager_opens_discovers_and_aggregates():
    m = manager()
    assert m.path_for("events", 3) == "millrace/events/3"
    await m.sync_assignment([("events", 0), ("events", 1)], now_us=NOW)
    assert m.partitions() == (("events", 0), ("events", 1))
    await m.stage("events", 0).stage_batch([rec(1, 0, 0, b"ab")], now_us=NOW + 5)
    await m.stage("events", 1).stage_batch(
        [rec(1, 0, 0, b"c"), rec(2, 0, 1, b"d")], now_us=NOW + 2
    )
    gauges = m.gauges()
    assert (
        gauges.partitions,
        gauges.staged_bytes,
        gauges.staged_rows,
        gauges.staged_groups,
    ) == (
        2,
        4,
        3,
        3,
    )
    assert gauges.oldest_first_staged_ts == NOW + 2
    with pytest.raises(StageError, match="already open"):
        await m.open_partition("events", 0, now_us=NOW)
    await m.sync_assignment([("events", 1)], now_us=NOW)
    assert m.partitions() == (("events", 1),)
    with pytest.raises(KeyError):
        m.stage("events", 0)
    await m.close()
    await m.close()
    with pytest.raises(StageClosedError):
        await m.open_partition("events", 0, now_us=NOW)
    with pytest.raises(StageClosedError):
        await m.sync_assignment([], now_us=NOW)
    empty = manager().gauges()
    assert (empty.staged_groups, empty.oldest_first_staged_ts) == (0, None)


async def test_manager_reopen_discovers_the_previous_owners_groups(tmp_path):
    base = f"{tmp_path}/millrace"
    first = StageManager("file:///", base, settings=fast_flush_settings())
    await first.open_partition("events", 0, now_us=NOW)
    await first.stage("events", 0).stage_batch([rec(1, 0, 0), rec(2, 0, 1)], now_us=NOW)
    await first.close()
    second = StageManager("file:///", base, settings=fast_flush_settings())
    stage = await second.open_partition("events", 0, now_us=NOW + 7)
    assert sorted(s.group for s in stage.snapshot()) == [g(1), g(2)]
    await second.close()


class _SpyBuilder:
    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from slatedb.uniffi import DbBuilder

        calls: list[tuple[object, ...]] = []

        class Spy(DbBuilder):  # type: ignore[misc]
            def with_db_cache(self, db_cache: object, db_cache_id: int) -> None:
                calls.append(("with_db_cache", db_cache, db_cache_id))
                super().with_db_cache(db_cache, db_cache_id)

            def with_db_cache_disabled(self) -> None:
                calls.append(("with_db_cache_disabled",))
                super().with_db_cache_disabled()

        monkeypatch.setattr(stage_mod, "DbBuilder", Spy)
        self.calls = calls


async def test_manager_opens_every_partition_on_the_shared_cache_with_its_id(
    monkeypatch,
):
    from millrace.stage import db_cache_id_for

    spy = _SpyBuilder(monkeypatch)
    m = manager(block_cache_bytes=1 << 20, meta_cache_bytes=1 << 18)
    assert m.cache_capacity_bytes == (1 << 20, 1 << 18)
    claimed = [("events", 0), ("events", 7), ("other", 0)]
    await m.sync_assignment(claimed, now_us=NOW)
    assert [c[0] for c in spy.calls] == ["with_db_cache"] * 3
    assert len({id(c[1]) for c in spy.calls}) == 1
    assert [c[2] for c in spy.calls] == [
        db_cache_id_for(t, p) for t, p in sorted(claimed)
    ]
    await m.close()


def test_the_shared_cache_halves_carry_their_configured_capacities(monkeypatch):
    from millrace.stage import build_slatedb_cache

    built: list[int] = []
    real = stage_mod.MokaCacheOptions

    def spy(**kwargs):
        built.append(kwargs["max_capacity"])
        return real(**kwargs)

    monkeypatch.setattr(stage_mod, "MokaCacheOptions", spy)
    assert build_slatedb_cache(block_cache_bytes=1 << 20, meta_cache_bytes=1 << 18)
    assert built == [1 << 20, 1 << 18]  # block half, then meta half
    assert build_slatedb_cache(block_cache_bytes=0, meta_cache_bytes=0) is None
    assert built == [1 << 20, 1 << 18]
    assert manager().cache_capacity_bytes == (128 * 1024 * 1024, 128 * 1024 * 1024)


async def test_manager_opens_with_the_cache_disabled_on_the_zero_pair(monkeypatch):
    spy = _SpyBuilder(monkeypatch)
    m = manager(block_cache_bytes=0, meta_cache_bytes=0)
    await m.sync_assignment([("events", 0)], now_us=NOW)
    assert spy.calls == [("with_db_cache_disabled",)]
    await m.close()


def test_cache_pair_validation():
    from slatedb.uniffi import DbCache

    from millrace.stage import build_slatedb_cache

    assert build_slatedb_cache(block_cache_bytes=0, meta_cache_bytes=0) is None
    assert isinstance(
        build_slatedb_cache(block_cache_bytes=1 << 20, meta_cache_bytes=1 << 20),
        DbCache,
    )
    with pytest.raises(ValueError, match="both"):
        StageManager("memory:///", "m", block_cache_bytes=0, meta_cache_bytes=4096)
    with pytest.raises(ValueError, match="block_cache_bytes"):
        StageManager("memory:///", "m", block_cache_bytes=-1, meta_cache_bytes=4096)
    with pytest.raises(ValueError, match="below 4096"):
        build_slatedb_cache(block_cache_bytes=4095, meta_cache_bytes=4096)
    with pytest.raises(ValueError, match="base_path"):
        StageManager("memory:///", "/")


def test_db_cache_id_for_is_stable_and_distinct():
    from millrace.stage import db_cache_id_for

    a = db_cache_id_for("events", 3)
    assert a == db_cache_id_for("events", 3)
    assert a != db_cache_id_for("events", 4)
    assert a != db_cache_id_for("other", 3)
    assert 0 <= a < 1 << 64
    with pytest.raises(ValueError, match="partition"):
        db_cache_id_for("events", -1)


async def test_open_store_refuses_inconsistent_arguments(memory_store):
    from millrace.stage import build_slatedb_cache

    cache = build_slatedb_cache(block_cache_bytes=1 << 20, meta_cache_bytes=1 << 20)
    base = {"topic": "events", "partition": 0}
    with pytest.raises(ValueError, match="cache choice is required"):
        await PartitionStage.open_store(memory_store, "p", **base)
    with pytest.raises(ValueError, match="go together"):
        await PartitionStage.open_store(memory_store, "p", **base, db_cache=cache)
    with pytest.raises(ValueError, match="go together"):
        await PartitionStage.open_store(memory_store, "p", **base, db_cache_id=1)
    with pytest.raises(ValueError, match="mutually exclusive"):
        await PartitionStage.open_store(
            memory_store,
            "p",
            **base,
            db_cache=cache,
            db_cache_id=1,
            cache_disabled=True,
        )
    with pytest.raises(ValueError, match="topic"):
        await PartitionStage.open_store(
            memory_store, "p", topic="", partition=0, cache_disabled=True
        )
    with pytest.raises(ValueError, match="partition"):
        await PartitionStage.open_store(
            memory_store, "p", topic="t", partition=-1, cache_disabled=True
        )


def test_build_slatedb_settings_shape():
    """GC always off (the maintenance service owns it), the memory and L0
    knobs applied, the compactor never touched."""
    doc = json.loads(
        build_slatedb_settings(max_unflushed_bytes=1234, l0_max_ssts=5).to_json_string()
    )
    assert doc["garbage_collector_options"] is None
    assert doc["max_unflushed_bytes"] == 1234
    assert doc["l0_max_ssts"] == 5
    assert doc["compactor_options"] is not None
    with pytest.raises(ValueError, match="max_unflushed_bytes"):
        build_slatedb_settings(max_unflushed_bytes=0, l0_max_ssts=8)
    with pytest.raises(ValueError, match="l0_max_ssts"):
        build_slatedb_settings(max_unflushed_bytes=1, l0_max_ssts=0)


async def test_a_stage_works_with_the_production_settings(stage_factory):
    stage = await stage_factory(
        settings=build_slatedb_settings(max_unflushed_bytes=1 << 28, l0_max_ssts=8)
    )
    await stage.stage_batch([rec(1, 0, 0)], now_us=NOW)
    assert await offsets(stage, 1) == [0]
    await stage.close()


# -- meta/table_uuid and old-layout cleanup ----------------------------------------


async def test_the_pinned_table_uuid_round_trips_and_survives_a_reopen(stage_factory):
    stage = await stage_factory()
    assert await stage.pinned_table_uuid() is None
    await stage.pin_table_uuid("0b8ee9ba-79a1-4f3e-b7e5-6a0b6ab6f012")
    assert await stage.pinned_table_uuid() == "0b8ee9ba-79a1-4f3e-b7e5-6a0b6ab6f012"
    assert await stage._db.get(b"meta/table_uuid") == (
        b"0b8ee9ba-79a1-4f3e-b7e5-6a0b6ab6f012"
    )
    await stage.close()
    reopened = await stage_factory()
    assert await reopened.discover(now_us=NOW) == []  # not a row
    assert await reopened.pinned_table_uuid() == (
        "0b8ee9ba-79a1-4f3e-b7e5-6a0b6ab6f012"
    )
    await reopened.close()


async def test_discover_deletes_an_older_layouts_keys_and_keeps_rows_and_meta(
    stage_factory,
):
    stage = await stage_factory()
    await stage.stage_batch([rec(1, 0, 0), rec(2, 0, 1)], now_us=NOW)
    await stage.probe_fence(1)
    await stage.pin_table_uuid("u")
    # More foreign keys than one scan page, under every old prefix and
    # on both sides of rows/ and meta/.
    foreign = [
        prefix + i.to_bytes(4, "big")
        for prefix in (b"stats/", b"sched_age/", b"prepared/", b"flushed/")
        for i in range(700)
    ] + [b"\x00early", b"zzz/late"]
    batch = WriteBatch()
    for key in foreign:
        batch.put(key, b"old")
    handle = await stage._db.write(batch)
    await handle.await_durable()
    assert len(foreign) > stage_mod._SCAN_CHUNK
    found = await stage.discover(now_us=NOW)
    assert sorted(found) == [g(1), g(2)]
    for key in foreign:
        assert await stage._db.get(key) is None
    assert await stage._db.get(keyspace.FENCE_PROBE_KEY) is not None
    assert await stage.pinned_table_uuid() == "u"
    assert [rk.offset for rk, _ in await disk_rows(stage)] == [0, 1]
    await stage.close()


async def test_discover_on_a_clean_stage_deletes_nothing(stage_factory, monkeypatch):
    stage = await stage_factory()
    await stage.stage_batch([rec(1, 0, 0)], now_us=NOW)
    writes = []
    real = stage._db.write

    async def spied(batch):
        writes.append(batch)
        return await real(batch)

    monkeypatch.setattr(stage._db, "write", spied)
    await stage.discover(now_us=NOW)
    assert writes == []
    await stage.close()
