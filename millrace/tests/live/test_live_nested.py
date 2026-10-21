"""Nested columns and a non-team spec, end to end against the real server.

The destination has a ``properties`` struct (a long, a timestamptz, a
list and a map), a json column and scalar columns; its live spec is
``identity(properties.team_id), month(properties.ts)`` and its sort order
``properties.ts`` asc — every partition and sort source a struct leaf.
The pipeline consumes, stages by that tuple, flushes, and the server
accepts the commits: no halt, files under the right tuples, every seq at
least once, parquet that reads back with the nested values and is
key-nondecreasing by ``properties.ts``.
"""

from __future__ import annotations

import asyncio
import json
from collections import Counter
from datetime import UTC, datetime, timedelta

import livekit
import pytest
from livekit import now_us

pytestmark = pytest.mark.live

TEAMS = (3, 11, 42)
EVENTS = 300
FLUSH_DEADLINE_S = 4
DAY_US = 86_400 * livekit.MICROS_PER_SECOND


def _instant(ts_us: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=ts_us)


async def test_nested_columns_flush_under_a_struct_leaf_spec(
    lake_home, topic_factory, producer_factory, admin, client
):
    table = lake_home.create_nested_events_table()
    info = table.info(totals=False)
    assert info.partition_spec is not None
    assert len(info.partition_spec.fields) == 2
    assert info.sort_spec is not None and len(info.sort_spec.fields) == 1

    topic = topic_factory(partitions=2)
    producer = producer_factory(topic.name)
    base = now_us()
    # Payload time runs BACKWARD through the stream (late arrivals) and
    # spans two months (now and 20 days back), so every file must be sorted by properties.ts, not by
    # arrival, and each team fans out by month.
    expected: dict[int, dict] = {}
    for seq in range(EVENTS):
        team = TEAMS[seq % len(TEAMS)]
        ts = base - (EVENTS - seq) * 1_000_000 - (seq % 2) * 20 * DAY_US
        expected[seq] = livekit.nested_doc(team, ts, seq)
        producer.send(
            team,
            ts,
            seq,
            record_ts_us=base,
            value=livekit.nested_payload(team, ts, seq),
        )
    events = await asyncio.to_thread(producer.flush)
    assert len(events) == EVENTS

    expected_tuples: Counter[tuple[str | None, ...]] = Counter(
        (
            str(doc["properties"]["team_id"]),
            livekit.expected_month(doc["properties"]["ts"]),
        )
        for doc in expected.values()
    )

    cfg = livekit.live_config(
        topic=topic.name,
        group=topic.group,
        stage_base_path=f"stage/{livekit.RUN_ID}/{lake_home.slug}",
        catalog=lake_home.catalog.name,
        namespace="ns1",
        table=table.name,
        flush_deadline_s=FLUSH_DEADLINE_S,
    )
    async with livekit.Pipeline(cfg, sweep_s=0.5) as pipe:
        assert pipe.runner is not None and pipe.flusher is not None
        await livekit.wait_until_async(
            lambda: (
                pipe.runner.halted is not None
                or sum(livekit.lake_tuple_counts(table, lake_home.head()).values())
                >= EVENTS
            ),
            FLUSH_DEADLINE_S + 90,
            desc=lambda: (
                f"all {EVENTS} nested events queryable (halt: {pipe.runner.halted}; "
                f"lake: {livekit.lake_tuple_counts(table, lake_home.head())})"
            ),
        )
        # The real server accepted every commit: nothing halted, nothing
        # dropped, nothing left staged.
        assert pipe.runner.halted is None, f"halted: {pipe.runner.halted}"
        # A decision is counted once its landed commit's rows are settled
        # (a durability wait behind the stage latency), so the lake can show
        # the rows before the counter moves.
        await livekit.wait_until_async(
            lambda: pipe.flusher.stats().decisions_committed >= 1,
            30,
            desc=lambda: f"a committed decision counted ({pipe.flusher.stats()})",
        )
        stats = pipe.flusher.stats()
        assert stats.records_dropped == {}
        assert stats.orphaned_uploads == 0
        await livekit.wait_until_async(
            lambda: pipe.stages.gauges().staged_groups == 0,
            30,
            desc=lambda: f"staging residue: {pipe.stages.gauges()}",
        )

    head = lake_home.head()
    # Files land under the right (team, month) tuples, at least once.
    counts = livekit.lake_tuple_counts(table, head)
    assert set(counts) == set(expected_tuples)
    assert all(counts[t] >= n for t, n in expected_tuples.items()), counts

    # Parquet reads back with the nested values, each file under its own
    # tuple and key-nondecreasing by properties.ts.
    by_tuple = await asyncio.to_thread(
        livekit.lake_rows_by_tuple, table, head, lake_home.fs
    )
    seen: set[int] = set()
    for (team_wire, month_wire), parts in by_tuple.items():
        for part in parts:
            rows = part.to_pylist()
            stamps = [row["properties"]["ts"] for row in rows]
            assert stamps == sorted(stamps), (
                f"file under {team_wire}/{month_wire} unsorted"
            )
            for row in rows:
                want = expected[row["seq"]]
                props = row["properties"]
                assert str(props["team_id"]) == team_wire
                assert livekit.expected_month(want["properties"]["ts"]) == month_wire
                assert props["team_id"] == want["properties"]["team_id"]
                assert props["ts"] == _instant(want["properties"]["ts"])
                assert props["tags"] == want["properties"]["tags"]
                assert dict(props["attrs"]) == want["properties"]["attrs"]
                assert row["event"] == want["event"]
                assert json.loads(row["doc"]) == want["doc"]
                seen.add(row["seq"])
    assert seen == set(expected)
