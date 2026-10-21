"""Scenario 1 (docs/kafka-ingestion-plan.md Phase 5): end-to-end truth.

Produce a seeded Zipf-skewed event stream to the live broker; the
in-process pipeline (real consumer, real SlateDB-on-MinIO staging, real
pyhoglake writer against the server built from this checkout) runs until
everything is queryable. Then the reconciliation is AT LEAST ONCE: every
produced ``seq`` is queryable at least once (duplicates are allowed by
contract), per-tenant row counts are at least the produced counts, and
nothing that was never produced appears.
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
        stage_base_path=f"stage/{livekit.RUN_ID}/{lake_home.slug}",
        catalog=lake_home.catalog.name,
        namespace="ns1",
        table=table.name,
        flush_deadline_s=FLUSH_DEADLINE_S,
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
        produced_seqs = [e.seq for e in events]
        await livekit.wait_until_async(
            lambda: livekit.all_landed(
                table, lake_home.catalog, lake_home.fs, produced_seqs
            ),
            FLUSH_DEADLINE_S + 60,
            desc=f"all {EVENTS} events queryable in the lake",
        )

        # Staging drains completely.
        await livekit.wait_until_async(
            lambda: pipe.stages.gauges().staged_groups == 0,
            30,
            desc=lambda: f"staging residue: {pipe.stages.gauges()}",
        )

    head = lake_home.head()

    # At least once, per seq and per tenant.
    seq_counts = await asyncio.to_thread(
        livekit.lake_seq_counts, table, head, lake_home.fs
    )
    livekit.assert_at_least_once(seq_counts, [e.seq for e in events])
    counts = livekit.lake_team_counts(table, head)
    assert all(counts[t] >= n for t, n in produced_by_team.items()), (
        f"per-tenant shortfall: "
        f"{[(t, n, counts[t]) for t, n in sorted(produced_by_team.items()) if counts[t] < n]}"
    )
    assert set(counts) == set(produced_by_team)

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
