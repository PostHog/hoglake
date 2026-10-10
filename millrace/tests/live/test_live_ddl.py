"""Scenario 8 (Phase 5): the DDL race.

A flush whose ``read_snapshot`` basis predates a concurrent
``add_column`` is refused ``ddl_since_read_snapshot`` — definitive, zero
writes — and the pipeline re-prepares against a fresh read rather than
replaying the refused payload (a replay would livelock; AGENT.md
invariant 12's typed-refusal family).

Deterministic by construction: the flusher's shape cache (TTL =
retention/4, far longer than this test) pins the pre-alter snapshot, so
the first sweep after the alter commits with a stale basis exactly the
way a mid-flush race would. No loss, no wedge: the next sweep commits
the same rows against the fresh shape, and post-alter rows carry the new
column.
"""

from __future__ import annotations

import asyncio

import livekit
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from livekit import now_us
from pyhoglake import ops

pytestmark = pytest.mark.live


async def test_ddl_race_reprepares(lake_home, topic_factory, producer_factory, admin):
    table = lake_home.create_events_table()
    topic = topic_factory(partitions=2)
    producer = producer_factory(topic.name)

    cfg = livekit.live_config(
        topic=topic.name,
        group=topic.group,
        partitions=tuple(range(topic.partitions)),
        stage_base_path=f"stage/{livekit.RUN_ID}/{lake_home.slug}",
        catalog=lake_home.catalog.name,
        namespace="ns1",
        table=table.name,
        flush_deadline_s=1,
        flush_sweep_s=1,
    )
    # The flush loop is hand-driven so the alter lands exactly between a
    # stale-basis attempt and its re-prepare.
    pipe = livekit.Pipeline(cfg, sweep_s=0.5, auto_flush=False)
    await pipe.start()
    try:
        base = now_us()
        await asyncio.to_thread(
            livekit.produce_batch,
            producer,
            [(7, base + i) for i in range(50)],
        )
        ends = await asyncio.to_thread(
            livekit.end_offsets, admin, topic.name, range(topic.partitions)
        )
        assert pipe.consumer is not None
        await livekit.wait_until_async(
            lambda: livekit.offsets_reached(
                pipe.consumer.committed_offsets(), topic.name, ends
            ),
            60,
            desc="the pre-alter rows staged",
        )
        # The age trigger needs real time to cross the 1s deadline.
        await asyncio.sleep(1.5)

        # The DDL lands while the flusher's shape cache pins the
        # pre-alter snapshot.
        await asyncio.to_thread(table.alter, [ops.add_column("region", pa.string())])

        # Sweep 1: stale basis → ddl_since_read_snapshot → re-prepare
        # (the refused payload is DROPPED, never replayed).
        report1 = await pipe.runner.run_once()
        assert report1.reprepared == 1, f"sweep 1: {report1}"
        assert report1.committed == 0
        assert sum(livekit.lake_team_counts(table, lake_home.head()).values()) == 0, (
            "a refused commit wrote rows"
        )

        # Sweep 2: re-prepared against the fresh shape → commits.
        report2 = await pipe.runner.run_once()
        assert report2.committed == 1, f"sweep 2: {report2}"
        assert sum(livekit.lake_team_counts(table, lake_home.head()).values()) == 50
        assert pipe.flusher is not None
        assert pipe.flusher.stats().reprepares == 1

        # Post-alter rows carry the new column; the pipeline keeps working.
        base2 = now_us()

        def produce_post_alter() -> None:
            for i in range(10):
                producer.send(7, base2 + i, 50 + i, extra={"region": "eu"})
            producer.flush()

        await asyncio.to_thread(produce_post_alter)
        pipe.start_flush_loop()
        await livekit.wait_until_async(
            lambda: (
                sum(livekit.lake_team_counts(table, lake_home.head()).values()) == 60
            ),
            90,
            desc="post-alter rows queryable",
        )
    finally:
        await pipe.stop()

    # Readback: files written under the post-alter schema carry region —
    # null for pre-alter rows, the produced values for post-alter rows.
    head = lake_home.head()
    region_files = 0
    for f in livekit.lake_files(table, head):
        data = await asyncio.to_thread(
            pq.read_table, f.path[len("s3://") :], filesystem=lake_home.fs
        )
        if "region" not in data.schema.names:
            continue
        region_files += 1
        seqs = data.column("seq").to_pylist()
        regions = data.column("region").to_pylist()
        for s, r in zip(seqs, regions):
            assert r == ("eu" if s >= 50 else None), (s, r)
    assert region_files >= 1, "no file written under the post-alter schema"
