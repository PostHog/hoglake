"""Scenario 8 (Phase 5): the DDL race.

A flush whose ``read_snapshot`` basis predates a concurrent
``add_column`` is refused ``ddl_since_read_snapshot`` (zero writes), and
the same decision rebuilds against a fresh read under a fresh key rather
than replaying the refused payload.

Deterministic by construction: the flusher's shape cache (TTL =
retention/4, far longer than this test) pins the pre-alter snapshot, so
the first sweep after the alter commits with a stale basis exactly the
way a mid-flush race would. No loss, no wedge, and post-alter rows carry
the new column.
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


async def test_ddl_race_rebuilds_on_a_fresh_basis(
    lake_home, topic_factory, producer_factory, admin
):
    table = lake_home.create_events_table()
    topic = topic_factory(partitions=2)
    producer = producer_factory(topic.name)

    cfg = livekit.live_config(
        topic=topic.name,
        group=topic.group,
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

        # One sweep: stale basis → ddl_since_read_snapshot → rebuild on
        # the fresh shape → commit (two attempts, one landed commit).
        assert pipe.runner is not None and pipe.flusher is not None
        report = await pipe.runner.run_once()
        assert report.committed == 1, f"sweep: {report}"
        stats = pipe.flusher.stats()
        assert stats.commit_attempts == 2, stats
        assert stats.commits == 1, stats
        assert livekit.all_landed(table, lake_home.catalog, lake_home.fs, range(50))

        # Post-alter rows carry the new column; the pipeline keeps working.
        base2 = now_us()

        def produce_post_alter() -> None:
            for i in range(10):
                producer.send(7, base2 + i, 50 + i, extra={"region": "eu"})
            producer.flush()

        await asyncio.to_thread(produce_post_alter)
        pipe.start_flush_loop()
        await livekit.wait_until_async(
            lambda: livekit.all_landed(
                table, lake_home.catalog, lake_home.fs, range(60)
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
