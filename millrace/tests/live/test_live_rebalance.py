"""Scenario 4 (Phase 5): reassignment — staged state follows the
partition, never the pod.

Two halves:

- ``test_static_reassignment_carries_staged_state``: the deploy-time
  shape. Replica A (static assignment) stages a whole wave and flushes
  NOTHING (long deadline); A stops cleanly; replica B — a different pod
  claiming the same partitions, same group — opens the same SlateDB
  instances, recovers them (the staged rows are RIGHT THERE in its
  gauges), consumes only wave 2 (committed offsets carried over), and
  flushes everything exactly once.
- ``test_cooperative_handoff_splits_ownership``: the group shape. Two
  cooperative consumers in one group; the coordinator moves a partition
  mid-run; both owners flush disjoint slices; counts reconcile exactly
  and neither owner double-flushes a range (exactness is the proof).
"""

from __future__ import annotations

import asyncio
from collections import Counter

import livekit
import pytest
from livekit import now_us

pytestmark = pytest.mark.live

TENANTS = 8


def _cfg(lake_home, topic, *, deadline_s: int, assignment, partitions=()) -> object:
    return livekit.live_config(
        topic=topic.name,
        group=topic.group,
        partitions=partitions,
        stage_base_path=f"stage/{livekit.RUN_ID}/{lake_home.slug}",
        catalog=lake_home.catalog.name,
        namespace="ns1",
        table="events",
        flush_deadline_s=deadline_s,
        flush_sweep_s=1,
        assignment=assignment,
    )


def _produced_by_team(producer) -> Counter[int]:
    counts: Counter[int] = Counter()
    for e in producer.produced:
        counts[e.team_id] += 1
    return counts


async def test_static_reassignment_carries_staged_state(
    lake_home, topic_factory, producer_factory, admin
):
    from millrace.config import AssignmentMode

    table = lake_home.create_events_table()
    topic = topic_factory(partitions=2)
    producer = producer_factory(topic.name)
    skew = livekit.Skew(TENANTS, seed=7)

    # --- replica A: stage wave 1, flush NOTHING (deadline far away) ---
    cfg_a = _cfg(
        lake_home,
        topic,
        deadline_s=3600,
        assignment=AssignmentMode.STATIC,
        partitions=(0, 1),
    )
    wave1 = 500
    async with livekit.Pipeline(cfg_a, sweep_s=0.5) as pipe_a:
        base = now_us()
        teams = skew.sample(wave1)
        await asyncio.to_thread(
            livekit.produce_batch,
            producer,
            [(team, base + i) for i, team in enumerate(teams)],
        )
        ends = await asyncio.to_thread(
            livekit.end_offsets, admin, topic.name, range(topic.partitions)
        )
        assert pipe_a.consumer is not None
        await livekit.wait_until_async(
            lambda: livekit.offsets_reached(
                pipe_a.consumer.committed_offsets(), topic.name, ends
            ),
            60,
            desc="replica A committing wave 1",
        )
        gauges_a = await pipe_a.stages.gauges()
        assert gauges_a.staged_rows == wave1
        assert pipe_a.flusher is not None
        assert pipe_a.flusher.stats().flushes_committed == 0
        assert sum(livekit.lake_team_counts(table, lake_home.head()).values()) == 0, (
            "replica A flushed despite the far deadline"
        )

    # --- replica B: the new pod claims the same partitions (same group) ---
    cfg_b = _cfg(
        lake_home,
        topic,
        deadline_s=3,
        assignment=AssignmentMode.STATIC,
        partitions=(0, 1),
    )
    pipe_b = livekit.Pipeline(cfg_b, sweep_s=0.5, auto_flush=False)
    try:
        await pipe_b.start()
        # The partition's SlateDB instances opened with wave 1 already in
        # them: staged state followed the partition, not the pod. And the
        # consumer resumed from A's committed offsets — nothing replayed.
        gauges_b = await pipe_b.stages.gauges()
        assert gauges_b.staged_rows == wave1, (
            f"replica B sees {gauges_b.staged_rows} staged rows, expected "
            f"wave 1's {wave1}"
        )
        assert pipe_b.consumer is not None
        assert pipe_b.consumer.stats().messages_consumed == 0

        pipe_b.start_flush_loop()
        # Wave 1 flushes under the new owner.
        await livekit.wait_until_async(
            lambda: (
                sum(livekit.lake_team_counts(table, lake_home.head()).values()) == wave1
            ),
            60,
            desc="replica B flushing wave 1",
        )
        # Wave 2 arrives under B and flushes too.
        base2 = now_us()
        teams2 = skew.sample(200)
        await asyncio.to_thread(
            livekit.produce_batch,
            producer,
            [(team, base2 + i) for i, team in enumerate(teams2)],
            seq_start=wave1,
        )
        await livekit.wait_until_async(
            lambda: (
                sum(livekit.lake_team_counts(table, lake_home.head()).values())
                == wave1 + 200
            ),
            60,
            desc="wave 2 queryable under replica B",
        )
    finally:
        await pipe_b.stop()

    counts = livekit.lake_team_counts(table, lake_home.head())
    assert counts == _produced_by_team(producer)
    # B committed every flush; A committed none: no range was flushed by
    # two owners (exactness above is the row-level proof).
    assert pipe_b.flusher is not None
    stats_b = pipe_b.flusher.stats()
    assert stats_b.flushes_committed > 0
    assert stats_b.orphaned_uploads == 0


async def test_cooperative_handoff_splits_ownership(
    lake_home, topic_factory, producer_factory, admin
):
    from millrace.config import AssignmentMode

    table = lake_home.create_events_table()
    topic = topic_factory(partitions=2)
    producer = producer_factory(topic.name)
    skew = livekit.Skew(TENANTS, seed=11)

    cfg = {
        "lake_home": lake_home,
        "topic": topic,
        "deadline_s": 3,
        "assignment": AssignmentMode.COOPERATIVE,
    }
    pipe_a = livekit.Pipeline(_cfg(**cfg), sweep_s=0.5)
    pipe_b: livekit.Pipeline | None = None
    try:
        await pipe_a.start()
        base = now_us()
        teams = skew.sample(400)
        await asyncio.to_thread(
            livekit.produce_batch,
            producer,
            [(team, base + i) for i, team in enumerate(teams)],
        )
        await livekit.wait_until_async(
            lambda: sum(livekit.lake_team_counts(table, lake_home.head()).values()) > 0,
            60,
            desc="replica A flushing some of wave 1",
        )

        # B joins the group: cooperative-sticky hands it one partition.
        pipe_b = livekit.Pipeline(_cfg(**cfg), sweep_s=0.5)
        await pipe_b.start()
        assert pipe_b.consumer is not None and pipe_a.consumer is not None
        await livekit.wait_until_async(
            lambda: len(pipe_b.stages.partitions()) == 1,
            90,
            desc="the group handing partition(s) to replica B",
        )
        # Cooperative-sticky splits 2 partitions over 2 members 1+1; A's
        # application of the revoke lands on its own consume loop, so
        # converge rather than sample.
        await livekit.wait_until_async(
            lambda: len(pipe_a.stages.partitions()) == 1,
            90,
            desc="replica A releasing the moved partition",
        )

        base2 = now_us()
        teams2 = skew.sample(400)
        await asyncio.to_thread(
            livekit.produce_batch,
            producer,
            [(team, base2 + i) for i, team in enumerate(teams2)],
            seq_start=400,
        )
        await livekit.wait_until_async(
            lambda: (
                sum(livekit.lake_team_counts(table, lake_home.head()).values()) == 800
            ),
            120,
            desc="all 800 events queryable across both owners",
        )
        # Both owners really owned: each consumed and each flushed.
        stats_a = pipe_a.consumer.stats()
        stats_b = pipe_b.consumer.stats()
        assert stats_a.messages_consumed > 0 and stats_b.messages_consumed > 0
        assert stats_a.messages_consumed + stats_b.messages_consumed == 800
        assert pipe_a.flusher is not None and pipe_b.flusher is not None
        assert pipe_a.flusher.stats().flushes_committed > 0
        assert pipe_b.flusher.stats().flushes_committed > 0
    finally:
        await pipe_a.stop()
        if pipe_b is not None:
            await pipe_b.stop()

    counts = livekit.lake_team_counts(table, lake_home.head())
    assert counts == _produced_by_team(producer)
