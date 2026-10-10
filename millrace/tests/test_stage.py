"""Component tests for the per-partition staging layer (``memory:///``).

Layering per docs/kafka-ingestion-plan.md: loop logic, ordering
guarantees and crash/replay semantics without docker. The SlateDB store
is one isolated ``memory:///`` resolve per test (conftest
``memory_store``); reopen scenarios share that one store object, which
is what makes memory-backed recovery meaningful at all.

What is pinned here:

- stage → scan round-trips in (team, timestamp, offset) key order;
- ``stats/`` accounting matches row scans EXACTLY (bytes, counts) and
  ``offsets/`` matches scanned offset bounds;
- set-once ``first_staged_ts`` (the property the tail SLA stands on);
- ``sched_age/`` entries created on first stage, removed on drain;
- ``commit_flushed`` deletes EXACTLY the flush's offset window —
  hypothesis sweeps interleaved (ts, offset) orders so a younger range
  of the same team, sorting inside the same byte slice, survives;
- a settle writes NO ``flushed/`` marker (the settle transaction is
  atomic and the receipt precedes it — a marker could only restate
  both);
- ``recover()`` collects legacy ``flushed/`` markers in bounded pages
  and returns pending ``prepared/`` entries;
- the offset-commit invariant as a call-order test at the ack-hook
  boundary (hedgerow's rows-then-offset rule, restated);
- gauges derived from ``stats/`` track exactly.
"""

from __future__ import annotations

import asyncio

import pytest
from hypothesis import given
from hypothesis import strategies as st
from stagekit import NOW, TABLE, fast_flush_settings, legacy_marker_kv, rec, window

from millrace import keyspace
from millrace.keyspace import (
    OffsetRange,
    PreparedKey,
    PreparedRequest,
    RowKey,
    StatsValue,
)
from millrace.stage import (
    _POISON_WRITE_CHUNK as _POISON_PURGE_CHUNK,
)
from millrace.stage import (
    AckHook,
    PartitionStage,
    PoisonedRecord,
    RecoveredPrepared,
    RecoveryReport,
    StageAck,
    StageClosedError,
    StageCorruptionError,
    StagedRecord,
    StageError,
    StageGauges,
    StageManager,
)

pytestmark = pytest.mark.component


async def team_rows(stage: PartitionStage, team_id: int) -> list[tuple[RowKey, bytes]]:
    return [kv async for kv in stage.scan_team_rows(team_id)]


# -- staging and scanning -------------------------------------------------------


async def test_stage_batch_round_trip_in_key_order(stage_factory):
    stage = await stage_factory()
    records = [
        rec(7, ts=300, offset=2),
        rec(3, ts=100, offset=0),
        rec(7, ts=100, offset=0),
        rec(3, ts=200, offset=1),
        rec(7, ts=200, offset=1),
    ]
    ack = await stage.stage_batch(records, now_us=NOW)
    assert ack == StageAck(
        topic="events",
        partition=0,
        rows=5,
        staged_bytes=sum(len(r.payload) for r in records),
        first_offset=0,
        last_offset=2,
    )
    rows7 = await team_rows(stage, 7)
    assert [(rk.timestamp_us, rk.offset) for rk, _ in rows7] == [
        (100, 0),
        (200, 1),
        (300, 2),
    ]
    assert [v for _, v in rows7] == [
        rec(7, 100, 0).payload,
        rec(7, 200, 1).payload,
        rec(7, 300, 2).payload,
    ]
    rows3 = await team_rows(stage, 3)
    assert [(rk.timestamp_us, rk.offset) for rk, _ in rows3] == [(100, 0), (200, 1)]
    await stage.close()


async def test_stats_match_row_scans_exactly(stage_factory):
    """The accounting the planner reads must equal what a flush would read."""
    stage = await stage_factory()
    batches = [
        ([rec(1, 100 + i, i) for i in range(3)], NOW),
        ([rec(2, 500 + i, 10 + i, payload=b"z" * 33) for i in range(2)], NOW + 10),
        ([rec(1, 200 + i, 3 + i) for i in range(2)], NOW + 20),
        ([rec(2, 900, 12, payload=b"tail")], NOW + 30),
    ]
    for records, now in batches:
        await stage.stage_batch(records, now_us=now)
    stats = {s.team_id: s for s in await stage.iter_key_stats()}
    assert set(stats) == {1, 2}
    for team_id, s in stats.items():
        rows = await team_rows(stage, team_id)
        assert s.row_count == len(rows)
        assert s.staged_bytes == sum(len(v) for _, v in rows)
    # one offsets range per staged batch; adjacent batches stay distinct
    # (arrival structure is identity material, never coalesced)
    assert await stage.read_offsets(1) == (
        OffsetRange("events", 0, 0, 2),
        OffsetRange("events", 0, 3, 4),
    )
    assert await stage.read_offsets(2) == (
        OffsetRange("events", 0, 10, 11),
        OffsetRange("events", 0, 12, 12),
    )
    assert stats[1].first_staged_ts == NOW and stats[1].last_staged_ts == NOW + 20
    assert stats[2].first_staged_ts == NOW + 10 and stats[2].last_staged_ts == NOW + 30
    await stage.close()


async def test_first_staged_ts_is_set_once(stage_factory):
    """Arrivals never reset ``first_staged_ts`` — enforced at the writer,
    relied on by the planner's age trigger and the tail SLA."""
    stage = await stage_factory()
    await stage.stage_batch([rec(9, 100, 0)], now_us=NOW)
    await stage.stage_batch([rec(9, 200, 1), rec(9, 300, 2)], now_us=NOW + 5_000_000)
    (s,) = await stage.iter_key_stats()
    assert s.first_staged_ts == NOW
    assert s.last_staged_ts == NOW + 5_000_000
    assert s.row_count == 3
    # the sched_age entry is still keyed by the ORIGINAL first_staged_ts
    assert await stage.iter_sched_age_through(NOW + 5_000_000) == [
        keyspace.SchedAgeKey(first_staged_ts=NOW, team_id=9)
    ]
    await stage.close()


async def test_first_staged_ts_survives_a_partial_flush(stage_factory):
    """Survivors of a partial settle keep the OLD first_staged_ts: they are
    younger, so the keep can only flush them earlier than their own
    deadline — the SLA's safe direction."""
    stage = await stage_factory()
    await stage.stage_batch([rec(9, 100 + i, i) for i in range(5)], now_us=NOW)
    await stage.stage_batch([rec(9, 500 + i, 5 + i) for i in range(5)], now_us=NOW + 60)
    key = window(9, 0, 4)
    report = await stage.commit_flushed(key)
    assert report.rows_deleted == 5
    assert not report.team_drained
    (s,) = await stage.iter_key_stats()
    assert s.first_staged_ts == NOW
    assert s.last_staged_ts == NOW + 60
    assert s.row_count == 5
    await stage.close()


async def test_sched_age_entry_lifecycle(stage_factory):
    stage = await stage_factory()
    assert await stage.iter_sched_age_through(NOW + 10**9) == []
    await stage.stage_batch([rec(1, 100, 0)], now_us=NOW)
    await stage.stage_batch([rec(2, 100, 1)], now_us=NOW + 50)
    # the age-ordered prefix scan: the cutoff excludes the younger entry
    assert await stage.iter_sched_age_through(NOW) == [
        keyspace.SchedAgeKey(first_staged_ts=NOW, team_id=1)
    ]
    assert await stage.iter_sched_age_through(NOW + 50) == [
        keyspace.SchedAgeKey(first_staged_ts=NOW, team_id=1),
        keyspace.SchedAgeKey(first_staged_ts=NOW + 50, team_id=2),
    ]
    # a second stage for team 1 does not add another entry
    await stage.stage_batch([rec(1, 200, 2)], now_us=NOW + 60)
    assert len(await stage.iter_sched_age_through(NOW + 10**9)) == 2
    # draining team 1 removes its entry; team 2's survives
    key = window(1, 0, 2)
    report = await stage.commit_flushed(key)
    assert report.team_drained
    assert await stage.iter_sched_age_through(NOW + 10**9) == [
        keyspace.SchedAgeKey(first_staged_ts=NOW + 50, team_id=2)
    ]
    await stage.close()


# -- commit_flushed: the exact-window delete ------------------------------------


@given(data=st.data())
def test_commit_flushed_deletes_exactly_the_window(data: st.DataObject) -> None:
    """Property: rows with offset in [first, last] of the SAME team die;
    every other row — older or younger, however the event timestamps
    interleave the byte order — survives. Neighbouring teams are
    untouched, and the surviving stats match a row scan exactly."""
    asyncio.run(_exactly_the_window(data))


async def _exactly_the_window(data: st.DataObject) -> None:
    from slatedb.uniffi import ObjectStore

    n = data.draw(st.integers(min_value=1, max_value=40))
    offsets = data.draw(
        st.lists(
            st.integers(min_value=0, max_value=10_000),
            min_size=n,
            max_size=n,
            unique=True,
        )
    )
    timestamps = data.draw(
        st.lists(
            st.integers(min_value=-(2**62), max_value=2**62), min_size=n, max_size=n
        )
    )
    first = data.draw(st.integers(min_value=0, max_value=10_000))
    last = data.draw(st.integers(min_value=first, max_value=10_200))
    payloads = {o: f"p:{o}".encode() for o in offsets}

    stage = await PartitionStage.open_store(
        ObjectStore.resolve("memory:///"),
        "millrace/events/0",
        topic="events",
        partition=0,
        settings=fast_flush_settings(),
    )
    await stage.stage_batch(
        [
            StagedRecord(5, t, o, payloads[o])
            for t, o in zip(timestamps, offsets, strict=True)
        ],
        now_us=NOW,
    )
    # a neighbouring team that must never be touched by team 5's settle
    await stage.stage_batch(
        [
            StagedRecord(6, t, o, b"neighbour")
            for t, o in zip(timestamps, offsets, strict=True)
        ],
        now_us=NOW,
    )

    key = window(5, first, last)
    report = await stage.commit_flushed(key)

    expected_deleted = {o for o in offsets if first <= o <= last}
    expected_survivors = set(offsets) - expected_deleted
    assert report.rows_deleted == len(expected_deleted)
    assert report.bytes_deleted == sum(len(payloads[o]) for o in expected_deleted)
    assert report.team_drained == (not expected_survivors)

    rows5 = await team_rows(stage, 5)
    assert {rk.offset for rk, _ in rows5} == expected_survivors
    assert all(v == payloads[rk.offset] for rk, v in rows5)
    rows6 = await team_rows(stage, 6)
    assert {rk.offset for rk, _ in rows6} == set(offsets)

    stats = {s.team_id: s for s in await stage.iter_key_stats()}
    assert 6 in stats and stats[6].row_count == n
    if expected_survivors:
        s5 = stats[5]
        assert s5.row_count == len(expected_survivors)
        assert s5.staged_bytes == sum(len(payloads[o]) for o in expected_survivors)
        assert s5.first_staged_ts == NOW
    else:
        assert 5 not in stats  # drained: stats and sched_age went with the rows
        assert [
            k for k in await stage.iter_sched_age_through(NOW) if k.team_id == 5
        ] == []
    await stage.close()


async def test_commit_flushed_is_idempotent(stage_factory):
    """A replayed settle (ambiguous failure, retried) rewrites the
    identical marker and reports zeros; state does not change."""
    stage = await stage_factory()
    await stage.stage_batch([rec(5, 100 + i, i) for i in range(5)], now_us=NOW)
    key = window(5, 0, 4)
    first = await stage.commit_flushed(key)
    assert (first.rows_deleted, first.team_drained) == (5, True)
    second = await stage.commit_flushed(key)
    assert (second.rows_deleted, second.bytes_deleted, second.team_drained) == (
        0,
        0,
        False,
    )
    assert await stage.iter_key_stats() == []
    assert await team_rows(stage, 5) == []
    assert await stage.read_offsets(5) == ()
    await stage.close()


async def test_commit_flushed_prunes_and_clips_offsets(stage_factory):
    stage = await stage_factory()
    await stage.stage_batch([rec(5, 100 + i, i) for i in range(10)], now_us=NOW)
    await stage.stage_batch(
        [rec(5, 200 + i, 10 + i) for i in range(10)], now_us=NOW + 1
    )
    await stage.stage_batch(
        [rec(5, 300 + i, 20 + i) for i in range(10)], now_us=NOW + 2
    )
    assert await stage.read_offsets(5) == (
        OffsetRange("events", 0, 0, 9),
        OffsetRange("events", 0, 10, 19),
        OffsetRange("events", 0, 20, 29),
    )
    # wholly-covered ranges drop
    key = window(5, 0, 9)
    await stage.commit_flushed(key)
    assert await stage.read_offsets(5) == (
        OffsetRange("events", 0, 10, 19),
        OffsetRange("events", 0, 20, 29),
    )
    # a window ending mid-range clips the straddler to its surviving head
    key = window(5, 10, 24)
    report = await stage.commit_flushed(key)
    assert report.rows_deleted == 15
    assert await stage.read_offsets(5) == (OffsetRange("events", 0, 25, 29),)
    assert {rk.offset for rk, _ in await team_rows(stage, 5)} == set(range(25, 30))
    # draining the rest removes the offsets key entirely
    key = window(5, 25, 29)
    report = await stage.commit_flushed(key)
    assert report.team_drained
    assert await stage.read_offsets(5) == ()
    await stage.close()


# -- recovery ---------------------------------------------------------------------


async def test_commit_flushed_writes_no_marker(stage_factory):
    """The settle is one atomic transaction with the receipt already in
    the caller's hand, so NOTHING is written under ``flushed/`` — the
    prefix is legacy-only. (The C2 pin: a marker would be unbounded
    garbage recover() would have to scan forever.)"""
    stage = await stage_factory()
    await stage.stage_batch([rec(5, 100 + i, i) for i in range(5)], now_us=NOW)
    report = await stage.commit_flushed(window(5, 0, 4))
    assert report.rows_deleted == 5 and report.team_drained
    assert await stage._scan_prefix(keyspace.FLUSHED_PREFIX) == []
    # ...and recovery therefore has nothing to collect.
    assert (await stage.recover()).legacy_markers_collected == 0
    await stage.close()


async def test_recover_collects_legacy_markers_without_touching_rows(memory_store):
    """A legacy ``flushed/`` marker is collected, NOT applied.

    "Marker landed, deletes didn't" is unreachable from every build of
    this code — the settle has always been ONE atomic transaction — so
    there is no real failure path that produces a marker with pending
    work, and recovery must not invent one: the marker is settled
    garbage. (The crash window that IS real — commit landed, settle
    never ran — leaves the ``prepared/`` entry, and the flusher's
    receipt-first reconcile settles it; test_flush.py's
    test_crash_between_commit_and_settle_is_receipt_settled drives that
    path.) The marker's value here is deliberately NOT a real envelope:
    the collector never decodes. Rows, stats, offsets and a pending
    ``prepared/`` entry are untouched; a second recover() finds nothing.
    """
    from slatedb.uniffi import DbBuilder, WriteBatch

    path = "millrace/events/0"
    payloads = {o: f"stuck-{o}".encode() for o in range(6)}
    marker_key, marker_value = legacy_marker_kv(9, 1, 4)
    prepared = PreparedRequest(
        keyspace.idempotency_key(TABLE, "events", 0, 9, 1, 4),
        b'{"prepared": "body"}',
        persisted_at=NOW,
        topic="events",
        partition=0,
    )

    db = await DbBuilder(path, memory_store).build()
    batch = WriteBatch()
    for offset, payload in payloads.items():
        batch.put(
            keyspace.encode_row_key(
                RowKey(9, timestamp_us=1_000 + offset, offset=offset)
            ),
            payload,
        )
    batch.put(
        keyspace.stats_key(9),
        keyspace.encode_stats_value(
            StatsValue(
                staged_bytes=sum(len(p) for p in payloads.values()),
                first_staged_ts=NOW,
                last_staged_ts=NOW,
                row_count=len(payloads),
            )
        ),
    )
    batch.put(keyspace.encode_sched_age_key(keyspace.SchedAgeKey(NOW, 9)), b"")
    batch.put(
        keyspace.offsets_key(9),
        keyspace.encode_offsets_value([OffsetRange("events", 0, 0, 5)]),
    )
    batch.put(marker_key, marker_value)
    batch.put(keyspace.prepared_key(9, 1), keyspace.encode_prepared_value(prepared))
    handle = await db.write(batch)
    await handle.await_durable()
    await db.shutdown()

    stage = await PartitionStage.open_store(
        memory_store, path, topic="events", partition=0, settings=fast_flush_settings()
    )
    report = await stage.recover()
    assert report.legacy_markers_collected == 1
    assert report.pending_prepared == (
        RecoveredPrepared(key=PreparedKey(team_id=9, first_offset=1), request=prepared),
    )
    # the rows are NOT deleted by recovery — the legacy marker carries no
    # authority; the prepared/ entry drives the window's reconcile
    assert {rk.offset for rk, _ in await team_rows(stage, 9)} == set(range(6))
    assert await stage._scan_prefix(keyspace.FLUSHED_PREFIX) == []

    again = await stage.recover()
    assert again.legacy_markers_collected == 0
    assert again.pending_prepared == report.pending_prepared
    await stage.close()


class _CountingDb:
    """A slatedb Db wrapper counting write batches and transactions —
    the bounded-recovery pins (pages of deletes, NO per-marker
    serializable transaction)."""

    def __init__(self, db):
        self._db = db
        self.writes = 0
        self.begins = 0

    def __getattr__(self, name):
        return getattr(self._db, name)

    async def write(self, batch):
        self.writes += 1
        return await self._db.write(batch)

    async def begin(self, *args, **kwargs):
        self.begins += 1
        return await self._db.begin(*args, **kwargs)


async def test_recover_collects_many_legacy_markers_in_bounded_pages(memory_store):
    """1000s of legacy markers: recover() deletes them in pages of
    ``_RECOVERY_PAGE`` — one write batch per page, ZERO transactions,
    never a full materialization-and-transact per marker (the recovery
    tax that stalled reopens past max.poll.interval.ms)."""
    from slatedb.uniffi import DbBuilder, WriteBatch

    path = "millrace/events/0"
    n_markers = 2500

    db = await DbBuilder(path, memory_store).build()
    # seed in chunks (a WriteBatch per 500 — the seeding's own bound)
    for lo in range(0, n_markers, 500):
        batch = WriteBatch()
        for i in range(lo, min(lo + 500, n_markers)):
            batch.put(keyspace.FLUSHED_PREFIX + i.to_bytes(24, "big"), b"\x01x")
        handle = await db.write(batch)
        await handle.await_durable()
    # a live team whose rows must survive the collection
    batch = WriteBatch()
    batch.put(keyspace.encode_row_key(RowKey(3, timestamp_us=100, offset=0)), b"live")
    batch.put(
        keyspace.stats_key(3),
        keyspace.encode_stats_value(
            StatsValue(
                staged_bytes=4, first_staged_ts=NOW, last_staged_ts=NOW, row_count=1
            )
        ),
    )
    handle = await db.write(batch)
    await handle.await_durable()
    await db.shutdown()

    stage = await PartitionStage.open_store(
        memory_store, path, topic="events", partition=0, settings=fast_flush_settings()
    )
    counting = _CountingDb(stage._db)
    stage._db = counting
    report = await stage.recover()

    assert report.legacy_markers_collected == n_markers
    assert report.pending_prepared == ()
    # 2500 markers / 1000 per page = 3 write batches, and not a single
    # transaction opened (the pre-fix shape was one serializable
    # transaction PLUS a full team-slice scan per marker).
    assert counting.writes == 3
    assert counting.begins == 0
    assert await stage._scan_prefix(keyspace.FLUSHED_PREFIX) == []
    assert [rk.offset for rk, _ in await team_rows(stage, 3)] == [0]

    # A second recover() is free: no markers, no writes.
    report2 = await stage.recover()
    assert report2.legacy_markers_collected == 0
    assert counting.writes == 3
    await stage.close()


async def test_recover_returns_pending_prepared_and_nothing_else(stage_factory):
    stage = await stage_factory()
    await stage.stage_batch([rec(1, 100 + i, i) for i in range(5)], now_us=NOW)
    await stage.stage_batch([rec(1, 200 + i, 5 + i) for i in range(5)], now_us=NOW + 1)
    req_a = PreparedRequest(
        keyspace.idempotency_key(TABLE, "events", 0, 1, 0, 4),
        b'{"a": 1}',
        persisted_at=NOW,
        topic="events",
        partition=0,
    )
    req_b = PreparedRequest(
        keyspace.idempotency_key(TABLE, "events", 0, 1, 5, 9),
        b'{"b": 2}',
        persisted_at=NOW,
        topic="events",
        partition=0,
    )
    await stage.persist_prepared(1, 0, req_a)
    await stage.persist_prepared(1, 5, req_b)
    # settling the first range drops its prepared entry in the same transaction
    await stage.commit_flushed(window(1, 0, 4))

    report = await stage.recover()
    assert report.legacy_markers_collected == 0
    assert report.pending_prepared == (
        RecoveredPrepared(key=PreparedKey(team_id=1, first_offset=5), request=req_b),
    )
    assert await stage.get_prepared(1, 0) is None
    assert await stage.get_prepared(1, 5) == req_b
    await stage.close()


async def test_prepared_round_trip(stage_factory):
    stage = await stage_factory()
    body = b'{"files": ["s3://bucket/a.parquet"], "read_snapshot": 42}'
    request = PreparedRequest(
        keyspace.idempotency_key(TABLE, "events", 0, 3, 10, 19),
        body,
        persisted_at=NOW,
        topic="events",
        partition=0,
    )
    await stage.persist_prepared(3, 10, request)
    assert await stage.get_prepared(3, 10) == request
    assert await stage.load_prepared() == [
        RecoveredPrepared(key=PreparedKey(team_id=3, first_offset=10), request=request)
    ]
    # a re-persist of the same key overwrites (idempotent), never duplicates
    await stage.persist_prepared(3, 10, request)
    assert len(await stage.load_prepared()) == 1
    # the persisted body comes back byte-identical (the replay contract)
    assert (await stage.get_prepared(3, 10)).body == body
    await stage.drop_prepared(3, 10)
    assert await stage.get_prepared(3, 10) is None
    assert await stage.load_prepared() == []
    await stage.close()


async def test_commit_flushed_requires_consistent_stats(memory_store):
    """Rows present with no stats entry is layout corruption — settle
    fails loudly (StageCorruptionError) rather than clamping."""
    from slatedb.uniffi import DbBuilder, WriteBatch

    path = "millrace/events/0"
    db = await DbBuilder(path, memory_store).build()
    batch = WriteBatch()
    batch.put(
        keyspace.encode_row_key(RowKey(9, timestamp_us=1_000, offset=0)), b"orphan"
    )
    handle = await db.write(batch)
    await handle.await_durable()
    await db.shutdown()

    stage = await PartitionStage.open_store(
        memory_store, path, topic="events", partition=0, settings=fast_flush_settings()
    )
    key = window(9, 0, 0)
    with pytest.raises(StageCorruptionError):
        await stage.commit_flushed(key)
    await stage.close()


# -- gauges -----------------------------------------------------------------------


async def test_gauges_track_the_stats_prefix(stage_factory):
    stage = await stage_factory()
    assert await stage.gauges() == StageGauges(
        staged_bytes=0, staged_rows=0, staged_teams=0, oldest_first_staged_ts=None
    )
    await stage.stage_batch(
        [rec(1, 100 + i, i, payload=b"abcd") for i in range(3)], now_us=NOW
    )
    await stage.stage_batch(
        [rec(2, 100 + i, 10 + i, payload=b"0123456789") for i in range(2)],
        now_us=NOW + 5,
    )
    g = await stage.gauges()
    assert g.staged_bytes == 3 * 4 + 2 * 10
    assert g.staged_rows == 5
    assert g.staged_teams == 2
    assert g.oldest_first_staged_ts == NOW
    key = window(1, 0, 2)
    await stage.commit_flushed(key)
    g = await stage.gauges()
    assert (g.staged_bytes, g.staged_rows, g.staged_teams) == (2 * 10, 2, 1)
    assert g.oldest_first_staged_ts == NOW + 5
    await stage.close()


# -- the offset-commit invariant, at the interface boundary -----------------------


async def test_offset_commit_invariant_call_order(stage_factory):
    """The ack hook is the offset-commit seam: it fires exactly once per
    successful batch, carrying the very ack the call then returns — never
    before the batch is durable, and never at all for a rejected or
    failed batch (rows-then-offset, pinned as call order)."""
    calls: list[tuple[str, StageAck]] = []
    hook: AckHook = lambda ack: calls.append(("ack", ack))
    stage = await stage_factory(ack_hook=hook)
    records = [rec(1, 100, 0), rec(1, 200, 1)]
    ack = await stage.stage_batch(records, now_us=NOW)
    calls.append(("returned", ack))
    assert calls == [("ack", ack), ("returned", ack)]
    assert (ack.first_offset, ack.last_offset) == (0, 1)

    calls.clear()
    with pytest.raises(ValueError, match="at least one record"):
        await stage.stage_batch([], now_us=NOW)
    with pytest.raises(ValueError, match="duplicate row key"):
        await stage.stage_batch([rec(1, 100, 0), rec(1, 100, 0)], now_us=NOW)
    assert calls == []
    await stage.close()


class _RecordingDb:
    """A ``Db`` wrapper whose write/commit handles record whether
    ``await_durable()`` was awaited. This is the mutation pin for the
    durability ordering the ack hook's contract rests on (PR #331 S12):
    deleting an ``await handle.await_durable()`` from a write path must
    RED the tests below — the call-order test above cannot see it (the
    hook fires after the await LINE either way).
    """

    def __init__(self, db: object) -> None:
        self._db = db
        self.write_handles: list[_RecordingHandle] = []
        self.txn_commit_handles: list[_RecordingHandle] = []

    async def write(self, batch: object) -> _RecordingHandle:
        handle = _RecordingHandle(await self._db.write(batch))  # type: ignore[attr-defined]
        self.write_handles.append(handle)
        return handle

    async def begin(self, *args: object) -> _RecordingTxn:
        return _RecordingTxn(await self._db.begin(*args), self)  # type: ignore[attr-defined]

    def __getattr__(self, name: str) -> object:
        return getattr(self._db, name)


class _RecordingHandle:
    def __init__(self, real: object) -> None:
        self._real = real
        self.awaited = False

    async def await_durable(self) -> None:
        self.awaited = True
        await self._real.await_durable()  # type: ignore[attr-defined]


class _RecordingTxn:
    def __init__(self, real: object, owner: _RecordingDb) -> None:
        self._real = real
        self._owner = owner

    async def commit(self) -> _RecordingHandle:
        real_handle = await self._real.commit()  # type: ignore[attr-defined]
        handle = _RecordingHandle(real_handle)
        self._owner.txn_commit_handles.append(handle)
        return handle

    def __getattr__(self, name: str) -> object:
        return getattr(self._real, name)


async def test_ack_hook_fires_only_after_remote_durability(stage_factory):
    """S12: at the moment the ack hook fires, the batch's write handle
    MUST already have been awaited durable — the consumer commits Kafka
    offsets off that hook, so the ordering is the at-least-once
    contract, not a nicety. Reds if the ``await_durable`` call in
    ``stage_batch`` is removed (the call-order test above stays green
    under that mutation — that is the gap this pin closes)."""
    stage = await stage_factory()
    db = _RecordingDb(stage._db)
    stage._db = db
    hook_calls: list[StageAck] = []

    def hook(ack: StageAck) -> None:
        # The write of THIS batch must be durable-awaited by now.
        assert db.write_handles, "the hook fired before any write"
        assert all(h.awaited for h in db.write_handles), (
            "the ack hook observed a write handle whose durability was "
            "never awaited — deleting await_durable() must red here"
        )
        hook_calls.append(ack)

    stage._ack_hook = hook
    await stage.stage_batch([rec(1, 100, 0)], now_us=NOW)
    assert len(hook_calls) == 1
    assert len(db.write_handles) == 1
    await stage.close()


async def test_every_write_path_awaits_durability_before_returning(stage_factory):
    """The same pin for the other "durable before return" contracts:
    ``persist_prepared`` (the replay-before-publish guard),
    ``poison_records``, and the settlement transaction's commit handle
    (``commit_flushed``)."""
    stage = await stage_factory()
    db = _RecordingDb(stage._db)
    stage._db = db

    await stage.persist_prepared(
        1,
        0,
        PreparedRequest("k", b"{}", persisted_at=NOW, topic="events", partition=0),
    )
    assert db.write_handles and all(h.awaited for h in db.write_handles)

    await stage.poison_records(
        [
            PoisonedRecord(
                offset=7,
                reason="missing_key",
                key=None,
                value=None,
                value_bytes_original=0,
            )
        ]
    )
    assert all(h.awaited for h in db.write_handles)

    await stage.stage_batch([rec(1, 100, 0)], now_us=NOW)
    await stage.commit_flushed(window(1, 0, 0))
    assert db.txn_commit_handles, "the settle committed no transaction"
    assert all(h.awaited for h in db.txn_commit_handles)
    await stage.close()


# -- SlateDB writer settings (T9) ----------------------------------------------------


def test_build_slatedb_settings_shape():
    """The JSON the binding consumes (``Settings.set`` takes JSON values
    and accepts unknown keys SILENTLY — so the pin is on the emitted
    settings document, not on the call): GC off when asked, the memory
    and L0 knobs applied, and ``compactor_options`` NEVER touched —
    disabling the writer's compactor is the parity-pinned L0 stall."""
    import json

    from millrace.stage import build_slatedb_settings

    doc = json.loads(
        build_slatedb_settings(
            gc_enabled=False, max_unflushed_bytes=1234, l0_max_ssts=5
        ).to_json_string()
    )
    assert doc["garbage_collector_options"] is None
    assert doc["max_unflushed_bytes"] == 1234
    assert doc["l0_max_ssts"] == 5
    assert doc["compactor_options"] is not None  # the writer keeps it

    doc = json.loads(
        build_slatedb_settings(
            gc_enabled=True, max_unflushed_bytes=99, l0_max_ssts=2
        ).to_json_string()
    )
    assert doc["garbage_collector_options"] is not None  # embedded GC on
    assert doc["l0_max_ssts"] == 2

    with pytest.raises(ValueError, match="max_unflushed_bytes"):
        build_slatedb_settings(gc_enabled=False, max_unflushed_bytes=0, l0_max_ssts=8)
    with pytest.raises(ValueError, match="l0_max_ssts"):
        build_slatedb_settings(gc_enabled=False, max_unflushed_bytes=1, l0_max_ssts=0)


async def test_a_stage_opens_writes_and_closes_with_gc_disabled(stage_factory):
    """The default production shape (embedded GC off) is a working
    writer, not just a valid settings document."""
    from millrace.stage import build_slatedb_settings

    stage = await stage_factory(
        settings=build_slatedb_settings(
            gc_enabled=False, max_unflushed_bytes=1 << 28, l0_max_ssts=8
        )
    )
    await stage.stage_batch([rec(1, 100, 0)], now_us=NOW)
    rows = [rk.offset async for rk, _ in stage.scan_team_rows(1)]
    assert rows == [0]
    await stage.close()


# -- poison retention: the purge (stage side of MILLRACE_POISON_RETENTION_S) ------


def _poison(offset: int, *, ts: int | None = None) -> PoisonedRecord:
    return PoisonedRecord(
        offset=offset,
        reason="malformed_key",
        key=None,
        value=b"junk",
        value_bytes_original=4,
        quarantined_at_us=ts,
    )


async def test_stage_batch_stamps_poison_with_the_staging_clock(stage_factory):
    """Consume-side quarantine entries are stamped with the batch's
    ``now_us`` (the v2 envelope) — the purge's age basis. A batch that
    lost the stamp would keep its poison forever (red without it)."""
    stage = await stage_factory()
    await stage.stage_batch([rec(1, 100, 0)], poison=[_poison(1)], now_us=NOW)
    (entry,) = await stage.scan_poison()
    assert entry.offset == 1
    assert entry.quarantined_at_us == NOW
    await stage.close()


async def test_poison_records_stamp_policy(stage_factory):
    """``poison_records`` stamps entries from its ``now_us`` argument,
    keeps an entry's own stamp when it carries one, and writes v1
    (unstamped, never purged) when no clock is available — the flush
    caller's seam until it passes its clock (integrator follow-up)."""
    stage = await stage_factory()
    await stage.poison_records([_poison(0, ts=NOW - 5), _poison(1)], now_us=NOW)
    await stage.poison_records([_poison(2)])  # no clock: stays v1
    by_offset = {p.offset: p for p in await stage.scan_poison()}
    assert by_offset[0].quarantined_at_us == NOW - 5  # the entry's own wins
    assert by_offset[1].quarantined_at_us == NOW  # filled from now_us
    assert by_offset[2].quarantined_at_us is None  # unknown age, kept forever
    await stage.close()


async def test_purge_poison_expired_deletes_only_expired_entries(stage_factory):
    """The retention rule, exactly: entries stamped before the cutoff go;
    younger entries, and entries with NO stamp (v1 — unknown age), stay.
    Staged rows are untouched."""
    stage = await stage_factory()
    await stage.poison_records(
        [_poison(0, ts=NOW - 8 * 86400_000_000), _poison(1, ts=NOW - 60_000_000)],
        now_us=NOW,
    )  # entry ts wins over now_us; offsets 0,1
    await stage.poison_records([_poison(2)], now_us=NOW - 10 * 86400_000_000)
    await stage.poison_records([_poison(3)])  # v1: unknown age
    await stage.stage_batch([rec(9, 100, 10)], now_us=NOW)

    cutoff = NOW - 7 * 86400_000_000  # 7-day retention edge
    await stage.poison_records([_poison(4, ts=cutoff)])  # exactly AT the edge
    report = await stage.purge_poison_expired(cutoff)
    assert report.deleted == 2  # offsets 0 and 2
    assert report.unreadable == 0
    assert not report.truncated
    remaining = await stage.scan_poison()
    # the boundary entry stays: expiry is STRICTLY older than the cutoff
    assert [p.offset for p in remaining] == [1, 3, 4]
    # the staged row is not the purge's business
    assert [rk.offset async for rk, _ in stage.scan_team_rows(9)] == [10]

    # Idempotent: a second pass over the same cutoff deletes nothing.
    again = await stage.purge_poison_expired(cutoff)
    assert again.deleted == 0
    assert again.scanned == 3
    await stage.close()


async def test_purge_poison_expired_bounds_its_scan_and_delete_batches(stage_factory):
    """Bounded work per pass: the scan stops at the budget (truncated,
    the next pass restarts at the front), and deletes land one
    ``_POISON_WRITE_CHUNK``-sized durable batch at a time — never one
    unbounded batch."""
    stage = await stage_factory()
    total = 2 * _POISON_PURGE_CHUNK + 1
    await stage.poison_records(
        [_poison(o, ts=NOW - 10) for o in range(total)], now_us=NOW
    )
    before = _write_batch_count(stage)
    report = await stage.purge_poison_expired(NOW)  # everything is expired
    assert report.deleted == total
    assert not report.truncated
    # one write batch per full chunk plus the remainder — not one giant one
    assert _write_batch_count(stage) - before == 3

    # the scan budget caps one pass; the prefix stays consistent
    await stage.poison_records([_poison(o, ts=NOW - 10) for o in range(7)], now_us=NOW)
    capped = await stage.purge_poison_expired(NOW, scan_budget=3)
    assert capped.scanned == 3 and capped.deleted == 3 and capped.truncated
    rest = await stage.purge_poison_expired(NOW)
    assert rest.deleted == 4 and not rest.truncated
    assert await stage.scan_poison() == []
    await stage.close()


async def test_purge_poison_expired_keeps_and_counts_undecodable_entries(
    stage_factory,
):
    """A poison value that fails to decode (foreign write, or a NEWER
    build's envelope mid-rollout) is kept and counted — a purge never
    deletes what it cannot read, and one bad entry never wedges the pass."""
    stage = await stage_factory()
    await stage.poison_records([_poison(0, ts=NOW - 10), _poison(2, ts=NOW - 10)])
    handle = await stage._db.write(_raw_put(keyspace.poison_key(1), b"\x9f garbage"))
    await handle.await_durable()

    report = await stage.purge_poison_expired(NOW)
    assert report.scanned == 3
    assert report.deleted == 2  # the decodable expired entries
    assert report.unreadable == 1
    raw = await stage._db.get(keyspace.poison_key(1))
    assert raw == b"\x9f garbage"  # kept
    await stage.close()


async def test_purge_poison_holds_no_lock_across_the_scan(stage_factory, monkeypatch):
    """The scan phase takes no write lock: a concurrent ``stage_batch``
    lands while a purge scan is parked mid-iteration. Reds if the purge
    ever takes the write lock around the whole pass (the batch would
    deadlock behind it — this test holds the scan open and requires the
    batch through first)."""
    stage = await stage_factory()
    await stage.poison_records([_poison(o, ts=NOW - 10) for o in range(5)])

    real_iter = stage._iter_range
    scan_parked = asyncio.Event()
    release_scan = asyncio.Event()
    parked = False

    async def parked_iter(start: bytes, end: bytes):
        nonlocal parked
        async for k, v in real_iter(start, end):
            yield k, v
            if not parked:
                parked = True
                scan_parked.set()
                await release_scan.wait()

    monkeypatch.setattr(stage, "_iter_range", parked_iter)
    purge = asyncio.create_task(stage.purge_poison_expired(NOW))
    await asyncio.wait_for(scan_parked.wait(), timeout=10)
    # With the scan parked, a write batch must complete (the scan holds
    # no lock; the purge has not reached its first delete batch).
    await asyncio.wait_for(stage.stage_batch([rec(1, 100, 42)], now_us=NOW), timeout=10)
    release_scan.set()
    report = await asyncio.wait_for(purge, timeout=10)
    assert report.deleted == 5
    await stage.close()


def _write_batch_count(stage: PartitionStage) -> int:
    from millrace.slatedb_metrics import snapshot_recorder

    return sum(
        m.value or 0
        for m in snapshot_recorder(stage.metrics_recorder)
        if m.name == "slatedb.db.write_batch_count"
    )


def _raw_put(key: bytes, value: bytes) -> object:
    from slatedb.uniffi import WriteBatch

    batch = WriteBatch()
    batch.put(key, value)
    return batch


# -- reopen, limits, guards ---------------------------------------------------------


async def test_reopen_preserves_rows_stats_and_offsets(stage_factory):
    """One shared memory store, close + reopen: WAL recovery reproduces
    the acknowledged state exactly (the component-layer shape of the
    file:/// hard-kill test)."""
    stage = await stage_factory()
    records = [rec(4, 100 + i, i, payload=f"row-{i}".encode()) for i in range(8)]
    await stage.stage_batch(records, now_us=NOW)
    await stage.close()

    reopened = await stage_factory()
    rows = await team_rows(reopened, 4)
    assert [(rk.offset, v) for rk, v in rows] == [
        (i, f"row-{i}".encode()) for i in range(8)
    ]
    (s,) = await reopened.iter_key_stats()
    assert s.row_count == 8
    assert s.staged_bytes == sum(len(r.payload) for r in records)
    assert s.first_staged_ts == NOW
    assert await reopened.read_offsets(4) == (OffsetRange("events", 0, 0, 7),)
    assert await reopened.recover() == RecoveryReport(pending_prepared=())
    await reopened.close()


async def test_scan_team_rows_limits(stage_factory):
    stage = await stage_factory()
    await stage.stage_batch(
        [rec(5, 100 + i, i, payload=b"0123456789") for i in range(10)], now_us=NOW
    )
    rows = [rk.offset async for rk, _ in stage.scan_team_rows(5, max_rows=4)]
    assert rows == [0, 1, 2, 3]
    rows = [rk.offset async for rk, _ in stage.scan_team_rows(5, max_bytes=35)]
    assert rows == [0, 1, 2]  # the 4th row would push the total 30 -> 40 > 35
    rows = [rk.offset async for rk, _ in stage.scan_team_rows(5, max_bytes=40)]
    assert rows == [0, 1, 2, 3]
    rows = [
        rk.offset
        async for rk, _ in stage.scan_team_rows(5, max_rows=50, max_bytes=10**9)
    ]
    assert rows == list(range(10))
    with pytest.raises(ValueError):
        stage.scan_team_rows(5, max_rows=0)
    await stage.close()


async def test_stage_batch_rejects_bad_input(stage_factory):
    stage = await stage_factory()
    with pytest.raises(ValueError, match="at least one record"):
        await stage.stage_batch([], now_us=NOW)
    with pytest.raises(ValueError, match="duplicate row key"):
        await stage.stage_batch([rec(1, 100, 0), rec(1, 100, 0)], now_us=NOW)
    with pytest.raises(TypeError, match="payload must be bytes"):
        StagedRecord(team_id=1, event_ts_us=100, offset=0, payload="str")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="unsigned 64-bit"):
        await stage.stage_batch([rec(-1, 100, 0)], now_us=NOW)
    # nothing from the rejected calls landed
    assert await stage.iter_key_stats() == []
    await stage.close()


async def test_close_guards(stage_factory):
    stage = await stage_factory()
    await stage.stage_batch([rec(1, 100, 0)], now_us=NOW)
    await stage.close()
    await stage.close()  # idempotent
    with pytest.raises(StageClosedError):
        await stage.stage_batch([rec(1, 101, 1)], now_us=NOW)
    with pytest.raises(StageClosedError):
        await stage.iter_key_stats()
    with pytest.raises(StageClosedError):
        [rk async for rk, _ in stage.scan_team_rows(1)]
    key = window(1, 0, 0)
    with pytest.raises(StageClosedError):
        await stage.commit_flushed(key)
    with pytest.raises(StageClosedError):
        await stage.gauges()
    with pytest.raises(StageClosedError):
        await stage.persist_prepared(1, 0, PreparedRequest("k", b"{}"))

    async with await stage_factory(partition=1) as managed:
        await managed.stage_batch([rec(1, 100, 0)], now_us=NOW)
    with pytest.raises(StageClosedError):
        await managed.gauges()


# -- the manager --------------------------------------------------------------------


async def test_manager_opens_recovers_and_aggregates():
    manager = StageManager("memory:///", "millrace", settings=fast_flush_settings())
    assert manager.path_for("events", 3) == "millrace/events/3"
    opened0 = await manager.open_partition("events", 0)
    opened1 = await manager.open_partition("events", 1)
    assert opened0.recovery == RecoveryReport(pending_prepared=())
    assert manager.partitions() == (("events", 0), ("events", 1))
    assert manager.stage("events", 0) is opened0.stage

    await opened0.stage.stage_batch([rec(1, 100, 0, payload=b"aaaa")], now_us=NOW)
    await opened1.stage.stage_batch(
        [rec(1, 100 + i, i, payload=b"bb") for i in range(2)], now_us=NOW + 5
    )
    g = await manager.gauges()
    assert g.partitions == 2
    assert g.staged_bytes == 4 + 2 * 2
    assert g.staged_rows == 3
    assert g.staged_teams == 2
    assert g.oldest_first_staged_ts == NOW
    assert set(g.per_partition) == {("events", 0), ("events", 1)}

    with pytest.raises(StageError, match="already open"):
        await manager.open_partition("events", 0)
    with pytest.raises(KeyError):
        manager.stage("events", 9)
    await manager.close()
    await manager.close()  # idempotent
    with pytest.raises(StageClosedError):
        await manager.open_partition("events", 5)
    with pytest.raises(ValueError, match="base_path"):
        StageManager("memory:///", "/")


async def test_manager_sync_assignment_and_reopen():
    """Assignment changes apply cleanly: revoked partitions close (staged
    state stays on the store), claimed ones open and recover. Reclaiming
    a partition — the assignment-changed-between-deploys case at the
    component layer — shows the previous owner's rows and stats intact."""
    manager = StageManager("memory:///", "millrace", settings=fast_flush_settings())
    await manager.sync_assignment([("events", 0), ("events", 1)])
    stage0 = manager.stage("events", 0)
    await stage0.stage_batch([rec(1, 100 + i, i) for i in range(4)], now_us=NOW)
    await manager.stage("events", 1).stage_batch([rec(1, 100, 0)], now_us=NOW + 7)

    sync = await manager.sync_assignment([("events", 1), ("events", 2)])
    assert sync.closed == (("events", 0),)
    assert sync.kept == (("events", 1),)
    assert set(sync.opened) == {("events", 2)}
    assert manager.partitions() == (("events", 1), ("events", 2))
    # the revoked partition's handle is closed; its rows stay on the store
    with pytest.raises(StageClosedError):
        await stage0.gauges()

    sync = await manager.sync_assignment([("events", 0), ("events", 1), ("events", 2)])
    assert set(sync.opened) == {("events", 0)}
    reclaimed = sync.opened[("events", 0)]
    assert reclaimed.recovery == RecoveryReport(pending_prepared=())
    rows = [rk.offset async for rk, _ in reclaimed.stage.scan_team_rows(1)]
    assert rows == [0, 1, 2, 3]
    (s,) = await reclaimed.stage.iter_key_stats()
    assert s.row_count == 4 and s.first_staged_ts == NOW
    await manager.close()


# -- M6: the sweep-published gauges snapshot and its staleness bound ---------------


class _Mono:
    def __init__(self) -> None:
        self.now = 10_000.0

    def __call__(self) -> float:
        return self.now


async def test_manager_gauges_serve_the_published_snapshot_while_fresh(memory_store):
    """M6(b): ``gauges(max_staleness_s=...)`` serves the sweep-published
    fold without a rescan while it is fresh; a live scan is the
    fallback once the snapshot ages out — and a fallback scan is NOT
    itself republished (a reader in fallback needs a fresh answer on
    every call, or a scrape's own scans would freeze its gauges)."""
    mono = _Mono()
    manager = StageManager(
        "memory:///",
        "millrace",
        settings=fast_flush_settings(),
        monotonic=mono,
    )
    stage = await manager.open_partition("events", 0)
    await stage.stage.stage_batch(
        [rec(1, 100 + i, i, payload=b"abcd") for i in range(3)], now_us=NOW
    )
    assert (await manager.gauges()).staged_rows == 3  # live scan

    marker = StageGauges(
        staged_bytes=999_999, staged_rows=9, staged_teams=1, oldest_first_staged_ts=None
    )
    manager.publish_gauges({("events", 0): marker})
    served = await manager.gauges(max_staleness_s=100.0)
    assert served.staged_rows == 9  # the snapshot, provably not the live 3
    # No staleness bound -> always a live scan.
    assert (await manager.gauges()).staged_rows == 3
    # Aged out -> the fallback live scan serves.
    mono.now += 101.0
    assert (await manager.gauges(max_staleness_s=100.0)).staged_rows == 3
    # The fallback did NOT republish: stage two more rows; an in-bound
    # read still answers live (a republished fallback would freeze at 3).
    await stage.stage.stage_batch(
        [rec(1, 200 + i, 10 + i, payload=b"abcd") for i in range(2)], now_us=NOW
    )
    assert (await manager.gauges(max_staleness_s=100.0)).staged_rows == 5
    await manager.close()


async def test_manager_publishes_aggregate_over_partitions(memory_store):
    """The published snapshot aggregates per-partition gauges exactly
    like the live scan (one fold for both paths)."""
    mono = _Mono()
    manager = StageManager(
        "memory:///",
        "millrace",
        settings=fast_flush_settings(),
        monotonic=mono,
    )
    one = await manager.open_partition("events", 0)
    two = await manager.open_partition("events", 1)
    await one.stage.stage_batch([rec(1, 100, 0, payload=b"abcd")], now_us=NOW)
    await two.stage.stage_batch(
        [rec(2, 100, 0, payload=b"0123456789"), rec(3, 101, 1, payload=b"xy")],
        now_us=NOW + 5,
    )
    manager.publish_gauges(
        {
            ("events", 0): StageGauges(4, 1, 1, NOW),
            ("events", 1): StageGauges(12, 2, 2, NOW + 5),
        }
    )
    served = await manager.gauges(max_staleness_s=1.0)
    assert served.partitions == 2
    assert served.staged_bytes == 16
    assert served.staged_rows == 3
    assert served.staged_teams == 3
    assert served.oldest_first_staged_ts == NOW
    await manager.close()


async def test_manager_publishes_the_sweeps_eligible_oldest(memory_store):
    """``oldest_eligible_staged_ts`` rides the published snapshot
    (the flush sweep computes it with the planner's knobs — the stage
    layer has none); a live fallback scan has no policy input and
    reports None rather than guess."""
    mono = _Mono()
    manager = StageManager(
        "memory:///",
        "millrace",
        settings=fast_flush_settings(),
        monotonic=mono,
    )
    await manager.open_partition("events", 0)
    manager.publish_gauges(
        {("events", 0): StageGauges(4, 1, 1, NOW)},
        oldest_eligible_staged_ts=NOW + 5,
    )
    served = await manager.gauges(max_staleness_s=100.0)
    assert served.oldest_eligible_staged_ts == NOW + 5
    # Aged out: the live fallback carries no eligibility.
    mono.now += 101.0
    assert (
        await manager.gauges(max_staleness_s=100.0)
    ).oldest_eligible_staged_ts is None
    # A publish with nothing eligible clears the series (None, not stale).
    manager.publish_gauges(
        {("events", 0): StageGauges(4, 1, 1, NOW)},
        oldest_eligible_staged_ts=None,
    )
    assert (
        await manager.gauges(max_staleness_s=100.0)
    ).oldest_eligible_staged_ts is None
    await manager.close()


async def test_list_prepared_scans_exactly_one_teams_entries(memory_store):
    """The flush's outstanding-entry gate: one team's ``prepared/``
    entries in first_offset order, never another team's."""
    manager = StageManager("memory:///", "millrace", settings=fast_flush_settings())
    stage = (await manager.open_partition("events", 0)).stage
    await stage.persist_prepared(
        1, 5, PreparedRequest("k" * 36, b"{}", NOW, topic="events", partition=0)
    )
    await stage.persist_prepared(
        1, 0, PreparedRequest("j" * 36, b"{}", NOW, topic="events", partition=0)
    )
    await stage.persist_prepared(
        2, 0, PreparedRequest("x" * 36, b"{}", NOW, topic="events", partition=0)
    )

    mine = await stage.list_prepared(1)
    assert [p.key.first_offset for p in mine] == [0, 5]  # key order, not write order
    assert mine[0].request.idempotency_key == "j" * 36
    assert [p.key.first_offset for p in await stage.list_prepared(2)] == [0]
    assert await stage.list_prepared(3) == []
    await manager.close()
