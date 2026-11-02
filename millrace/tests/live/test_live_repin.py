"""A planned move to a new table: MILLRACE_STAGE_REPIN_FROM.

The stage is pinned to ``events`` by its first commits, then holds rows
that were consumed (offsets committed) but never flushed when millrace
restarts on ``events_v2`` with the knob naming ``events``. The startup
drain re-pins the stage and flushes those rows into ``events_v2``, and
consuming resumes there: every produced seq lands in exactly one table.
A last start without the knob comes up as usual.
"""

from __future__ import annotations

import asyncio
import dataclasses

import livekit
import pytest
from livekit import now_us

pytestmark = pytest.mark.live


async def test_a_move_drains_the_staged_rows_into_the_new_table(
    lake_home, topic_factory, producer_factory, admin
):
    old = lake_home.create_events_table("events")
    new = lake_home.create_events_table("events_v2")
    topic = topic_factory(partitions=2)
    producer = producer_factory(topic.name)

    def config(table: str, deadline_s: int, **overrides):
        cfg = livekit.live_config(
            topic=topic.name,
            group=topic.group,
            stage_base_path=f"stage/{livekit.RUN_ID}/{lake_home.slug}",
            catalog=lake_home.catalog.name,
            namespace="ns1",
            table=table,
            flush_deadline_s=deadline_s,
            slow_lane_deadline_s=deadline_s,
        )
        return dataclasses.replace(cfg, **overrides)

    async def produce(seqs: range):
        base = now_us()
        await asyncio.to_thread(
            livekit.produce_batch,
            producer,
            [(7, base + i) for i in range(len(seqs))],
            seq_start=seqs.start,
        )

    def landed(table, seqs: range):
        return lambda: livekit.all_landed(table, lake_home.catalog, lake_home.fs, seqs)

    async def settled(pipe):
        # Committed rows are deleted after their commit: stop before
        # that and the next start replays them (a duplicate).
        await livekit.wait_until_async(
            lambda: pipe.stages.gauges().staged_groups == 0,
            30,
            desc=lambda: f"staging residue: {pipe.stages.gauges()}",
        )

    wave1, wave2, wave3 = range(100), range(100, 150), range(150, 200)

    # Wave 1 commits to `events`: the stage pins its incarnation.
    async with livekit.Pipeline(config("events", 2)) as pipe:
        await produce(wave1)
        await livekit.wait_until_async(landed(old, wave1), 120, desc="wave 1 in events")
        await settled(pipe)

    # Wave 2 is consumed and staged, never flushed (the flush loop is off).
    async with livekit.Pipeline(config("events", 2), auto_flush=False) as pipe:
        await produce(wave2)
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
        assert pipe.staged_rows() == len(wave2)

    # The move: the drain re-pins the stage and flushes wave 2 into events_v2.
    moved = config("events_v2", 2, stage_repin_from=old.table_uuid)
    async with livekit.Pipeline(moved) as pipe:
        for t, p in pipe.stages.partitions():
            assert await pipe.stages.stage(t, p).pinned_table_uuid() == new.table_uuid
        await produce(wave3)
        await livekit.wait_until_async(
            landed(new, range(100, 200)), 120, desc="waves 2 and 3 in events_v2"
        )
        await settled(pipe)

    # The knob removed: the stage is pinned to events_v2 now.
    async with livekit.Pipeline(config("events_v2", 2)) as pipe:
        assert pipe.runner is not None and pipe.runner.halted is None

    head = lake_home.head()
    old_counts = livekit.lake_seq_counts(old, head, lake_home.fs)
    new_counts = livekit.lake_seq_counts(new, head, lake_home.fs)
    assert old_counts == dict.fromkeys(wave1, 1), old_counts
    assert new_counts == dict.fromkeys(range(100, 200), 1), new_counts
