"""Scenario 1 (docs/kafka-ingestion-plan.md Phase 5): end-to-end truth.

Produce a seeded Zipf-skewed event stream to the live broker; the
in-process pipeline (real consumer, real SlateDB-on-MinIO staging, real
pyhoglake writer against the server built from this checkout) runs until
everything is queryable. Then the reconciliation is EXACT: per-tenant
row counts from the manifest equal the produced counts, and a full
parquet readback carries exactly the produced ``seq`` set — no loss, no
duplication, nothing extra.
"""

from __future__ import annotations

import asyncio
from collections import Counter

import livekit
import pytest
from livekit import now_us

pytestmark = pytest.mark.live

TENANTS = 50
EVENTS = 3000
SEED = 20261009
FLUSH_DEADLINE_S = 6


async def test_end_to_end_truth(lake_home, topic_factory, producer_factory, admin):
    table = lake_home.create_events_table()
    topic = topic_factory(partitions=4)
    producer = producer_factory(topic.name)
    skew = livekit.Skew(TENANTS, seed=SEED)
    teams = skew.sample(EVENTS)
    base = now_us()

    cfg = livekit.live_config(
        topic=topic.name,
        group=topic.group,
        partitions=tuple(range(topic.partitions)),
        stage_base_path=f"stage/{livekit.RUN_ID}/{lake_home.slug}",
        catalog=lake_home.catalog.name,
        namespace="ns1",
        table=table.name,
        flush_deadline_s=FLUSH_DEADLINE_S,
        flush_sweep_s=1,
    )
    async with livekit.Pipeline(cfg, sweep_s=0.5) as pipe:
        events = await asyncio.to_thread(
            livekit.produce_batch,
            producer,
            [(team, base + seq) for seq, team in enumerate(teams)],
        )
        produced_by_team = Counter(e.team_id for e in events)
        assert sum(produced_by_team.values()) == EVENTS

        # Every produced offset is consumed and committed...
        ends = await asyncio.to_thread(
            livekit.end_offsets, admin, topic.name, range(topic.partitions)
        )
        assert sum(ends.values()) == EVENTS
        assert pipe.consumer is not None
        await livekit.wait_until_async(
            lambda: livekit.offsets_reached(
                pipe.consumer.committed_offsets(), topic.name, ends
            ),
            60,
            desc=f"committed offsets reaching the log ends {ends}",
        )

        # ...and every staged row flushes to the lake. The age trigger
        # bounds the tail: deadline + sweep + commit + poll slack.
        await livekit.wait_until_async(
            lambda: (
                sum(livekit.lake_team_counts(table, lake_home.head()).values())
                == EVENTS
            ),
            FLUSH_DEADLINE_S + 60,
            desc=f"all {EVENTS} events queryable in the lake",
        )

        # Staging drains completely: nothing staged, no team left behind.
        gauges = await pipe.stages.gauges()
        assert gauges.staged_rows == 0, f"staging residue: {gauges}"

    head = lake_home.head()

    # Metadata reconciliation (one file = one (team, month) tuple).
    counts = livekit.lake_team_counts(table, head)
    assert counts == produced_by_team, (
        f"per-tenant mismatch: "
        f"{[(t, produced_by_team[t], counts[t]) for t in sorted(produced_by_team) if produced_by_team[t] != counts[t]]}"
    )

    # Full parquet readback: exactly the produced seq set, and every row
    # carries its own team/timestamp.
    rows = await asyncio.to_thread(livekit.lake_rows, table, head, lake_home.fs)
    assert rows.num_rows == EVENTS
    readback_seqs = set(rows.column("seq").to_pylist())
    assert readback_seqs == {e.seq for e in events}
    for team, ts in zip(
        rows.column("team_id").to_pylist(), rows.column("timestamp").to_pylist()
    ):
        assert produced_by_team[team] > 0
        assert ts is not None

    # Every file lives under the live spec's partitions of THIS month.
    months = livekit.lake_month_partitions(table, head)
    assert months == {livekit.expected_month(base)}
    files = livekit.lake_files(table, head)
    assert files, "no files committed"
    for f in files:
        assert f.record_count > 0
        assert f.file_size_bytes > 0

    # The offsets the consumer committed are exactly the log ends (for
    # the partitions that received events).
    assert pipe.consumer is not None
    committed = dict(pipe.consumer.committed_offsets())
    for p, end in ends.items():
        if end > 0:
            assert committed.get((topic.name, p)) == end
