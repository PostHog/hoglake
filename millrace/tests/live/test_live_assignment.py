"""Scenario 4 (Phase 5): reassignment — staged state follows the
partition, never the pod.

- ``test_static_reassignment_carries_staged_state``: the deploy-time
  shape. Replica A (ordinal 0 of 1) stages a whole wave and flushes
  NOTHING (long deadline); A stops cleanly; replica B — a different pod
  with the same ordinal and replica count, same group — derives the
  same partitions, opens the same SlateDB instances, and its startup
  drain flushes wave 1 BEFORE it consumes anything; it then consumes
  only wave 2 (committed offsets carried over). Every seq lands at least
  once.
- ``test_two_replicas_split_the_topic_by_ordinal``: the fleet shape
  against a real broker's metadata. Two pipelines, ``replica_count=2``,
  ordinals 0 and 1, over a two-partition topic: each claims exactly its
  modulo share, both ingest and flush their own partition, every seq
  lands at least once, and each replica's
  committed offsets name only its own partition.
"""

from __future__ import annotations

import asyncio

import livekit
import pytest
from livekit import now_us

pytestmark = pytest.mark.live

TENANTS = 8


def _cfg(
    lake_home, topic, *, deadline_s: int, replica_count: int = 1, ordinal: int = 0
) -> object:
    return livekit.live_config(
        topic=topic.name,
        group=topic.group,
        stage_base_path=f"stage/{livekit.RUN_ID}/{lake_home.slug}",
        catalog=lake_home.catalog.name,
        namespace="ns1",
        table="events",
        flush_deadline_s=deadline_s,
        slow_lane_deadline_s=max(deadline_s, 21600),
        replica_count=replica_count,
        ordinal=ordinal,
    )


async def test_static_reassignment_carries_staged_state(
    lake_home, topic_factory, producer_factory, admin
):
    table = lake_home.create_events_table()
    topic = topic_factory(partitions=2)
    producer = producer_factory(topic.name)
    skew = livekit.Skew(TENANTS, seed=7)

    # --- replica A: stage wave 1, flush NOTHING (deadline far away) ---
    cfg_a = _cfg(lake_home, topic, deadline_s=3600)
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
        assert pipe_a.stages.gauges().staged_rows == wave1
        assert pipe_a.flusher is not None
        assert pipe_a.flusher.stats().decisions_committed == 0
        assert sum(livekit.lake_team_counts(table, lake_home.head()).values()) == 0, (
            "replica A flushed despite the far deadline"
        )

    # --- replica B: the new pod for the same ordinal derives the same
    # partitions (same replica count, same group) ---
    cfg_b = _cfg(lake_home, topic, deadline_s=3)
    pipe_b = livekit.Pipeline(cfg_b, sweep_s=0.5)
    try:
        await pipe_b.start()
        # start() returns after the startup drain: wave 1 followed the
        # partition and is already queryable, and nothing was consumed
        # before the drain finished.
        assert livekit.all_landed(
            table, lake_home.catalog, lake_home.fs, range(wave1)
        ), "wave 1 not flushed by replica B's startup drain"
        assert pipe_b.consumer is not None
        assert pipe_b.consumer.stats().messages_consumed == 0
        assert pipe_b.stages.gauges().staged_groups == 0

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
            lambda: livekit.all_landed(
                table, lake_home.catalog, lake_home.fs, range(wave1 + 200)
            ),
            60,
            desc="wave 2 queryable under replica B",
        )
        # Resumed from A's committed offsets: wave 1 never re-consumed.
        assert pipe_b.consumer.stats().messages_consumed == 200
    finally:
        await pipe_b.stop()

    seq_counts = livekit.lake_seq_counts(table, lake_home.head(), lake_home.fs)
    livekit.assert_at_least_once(seq_counts, [e.seq for e in producer.produced])
    assert pipe_b.flusher is not None
    stats_b = pipe_b.flusher.stats()
    assert stats_b.decisions_committed > 0
    assert stats_b.orphaned_uploads == 0


async def test_two_replicas_split_the_topic_by_ordinal(
    lake_home, topic_factory, producer_factory
):
    """Against the broker's real metadata: two replicas over two
    partitions each derive their modulo share at start — disjoint,
    covering the topic — then both ingest and flush their own half and
    the lake reconciles exactly."""
    table = lake_home.create_events_table()
    topic = topic_factory(partitions=2)
    producer = producer_factory(topic.name)
    skew = livekit.Skew(TENANTS, seed=11)
    pipe_a = livekit.Pipeline(
        _cfg(lake_home, topic, deadline_s=3, replica_count=2, ordinal=0),
        sweep_s=0.5,
    )
    pipe_b = livekit.Pipeline(
        _cfg(lake_home, topic, deadline_s=3, replica_count=2, ordinal=1),
        sweep_s=0.5,
    )
    try:
        await pipe_a.start()
        await pipe_b.start()
        assert pipe_a.stages.partitions() == ((topic.name, 0),)
        assert pipe_b.stages.partitions() == ((topic.name, 1),)

        base = now_us()
        teams = skew.sample(400)
        await asyncio.to_thread(
            livekit.produce_batch,
            producer,
            [(team, base + i) for i, team in enumerate(teams)],
        )
        await livekit.wait_until_async(
            lambda: livekit.all_landed(
                table, lake_home.catalog, lake_home.fs, range(400)
            ),
            120,
            desc="all 400 events queryable across both replicas",
        )
        assert pipe_a.consumer is not None and pipe_b.consumer is not None
        stats_a = pipe_a.consumer.stats()
        stats_b = pipe_b.consumer.stats()
        assert stats_a.messages_consumed > 0 and stats_b.messages_consumed > 0
        assert stats_a.messages_consumed + stats_b.messages_consumed == 400
        assert set(pipe_a.consumer.committed_offsets()) == {(topic.name, 0)}
        assert set(pipe_b.consumer.committed_offsets()) == {(topic.name, 1)}
        assert pipe_a.flusher is not None and pipe_b.flusher is not None
        assert pipe_a.flusher.stats().decisions_committed > 0
        assert pipe_b.flusher.stats().decisions_committed > 0
    finally:
        await pipe_a.stop()
        await pipe_b.stop()

    seq_counts = livekit.lake_seq_counts(table, lake_home.head(), lake_home.fs)
    livekit.assert_at_least_once(seq_counts, [e.seq for e in producer.produced])
