"""Scenario 9 (Phase 5): drop+recreate mid-flight — the incarnation guard.

The destination is dropped and recreated under the same name while the
pipeline (the real ``millrace.main`` subprocess) holds staged rows. The
pinned ``expected_table_uuid`` no longer resolves, the server refuses
``table_recreated``, and millrace HALTS LOUDLY: nonzero exit, the reason
in the log, and ZERO writes to the new incarnation (viaduck lesson 3 —
an incarnation change is a stop sign, never a silent retarget).
"""

from __future__ import annotations

import asyncio

import livekit
import pytest
from livekit import now_us

pytestmark = pytest.mark.live


async def test_drop_recreate_halts_loudly(
    lake_home, topic_factory, producer_factory, admin
):
    table = lake_home.create_events_table()
    old_uuid = table.table_uuid
    topic = topic_factory(partitions=2)
    producer = producer_factory(topic.name)

    cfg = livekit.live_config(
        topic=topic.name,
        group=topic.group,
        partitions=tuple(range(topic.partitions)),
        stage_base_path=f"stage/{livekit.RUN_ID}/{lake_home.slug}",
        catalog=lake_home.catalog.name,
        namespace="ns1",
        table="events",
        flush_deadline_s=2,
        flush_sweep_s=1,
    )
    port = livekit.free_port()
    child = livekit.Child(livekit.child_env(cfg, metrics_port=port), name="recreate")
    child.start()
    try:
        child.wait_ready(port, timeout_s=120)

        # Wave 1: lands and is queryable — the pipeline was healthy.
        base = now_us()
        await asyncio.to_thread(
            livekit.produce_batch, producer, [(7, base + i) for i in range(100)]
        )
        await livekit.wait_until_async(
            lambda: (
                sum(livekit.lake_team_counts(table, lake_home.head()).values()) == 100
            ),
            120,
            desc="wave 1 queryable",
        )

        # Wave 2: staged, not yet flushed when the ground moves.
        base2 = now_us()
        await asyncio.to_thread(
            livekit.produce_batch,
            producer,
            [(7, base2 + i) for i in range(50)],
            seq_start=100,
        )
        ends = await asyncio.to_thread(
            livekit.end_offsets, admin, topic.name, range(topic.partitions)
        )
        await livekit.wait_until_async(
            lambda: livekit.group_offsets_reached(
                livekit.committed_offsets(admin, topic.group, topic.name), ends
            ),
            60,
            desc="wave 2 staged (offsets committed)",
        )

        # Drop + recreate under the same name → a new incarnation.
        await asyncio.to_thread(table.drop)
        recreated = await asyncio.to_thread(lake_home.create_events_table, "events")
        assert recreated.table_uuid != old_uuid

        # The pipeline halts loudly: nonzero exit, the reason logged.
        rc = await asyncio.to_thread(child.wait_exit, 180)
        out = child.output()
        assert rc == 3, f"expected the halt exit code 3, got {rc}:\n{out[-3000:]}"
        assert "table_recreated" in out, out[-3000:]
        assert "HALTED" in out

        # ZERO writes to the new incarnation.
        assert (
            await asyncio.to_thread(livekit.lake_files, recreated, lake_home.head())
            == []
        )
        assert sum(livekit.lake_team_counts(recreated, lake_home.head()).values()) == 0
    finally:
        if child.proc is not None and child.proc.poll() is None:
            child.kill()
            child.wait_exit(timeout_s=30)
