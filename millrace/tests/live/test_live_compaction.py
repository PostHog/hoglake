"""Scenario 10 (Phase 5): compaction observation.

After a skewed run leaves a bucket of small age-triggered files, the
server's maintenance trigger (``POST /v1/catalogs/{c}/maintenance/compact``,
the same path the background loop drives — the loop is OFF in this
harness so the trigger is the only sweeper) rolls the small files up:
one group, six inputs, one output. The table stays queryable throughout
and the row counts stay EXACT — compaction changes no visible row.
"""

from __future__ import annotations

import asyncio

import livekit
import pyarrow.parquet as pq
import pytest
from livekit import now_us

pytestmark = pytest.mark.live

TEAM = 5
WAVES = 6
PER_WAVE = 10
FLUSH_DEADLINE_S = 2


async def test_compaction_rolls_up_small_files(
    lake_home, topic_factory, producer_factory, admin
):
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
        flush_deadline_s=FLUSH_DEADLINE_S,
        flush_sweep_s=1,
    )
    async with livekit.Pipeline(cfg, sweep_s=0.5):
        # Six waves, each its own flush → six small files in one
        # (team, month) bucket.
        seq = 0
        for wave in range(WAVES):
            base = now_us()
            await asyncio.to_thread(
                livekit.produce_batch,
                producer,
                [(TEAM, base + i) for i in range(PER_WAVE)],
                seq_start=seq,
            )
            seq += PER_WAVE
            want = wave + 1
            await livekit.wait_until_async(
                lambda w=want: len(livekit.lake_files(table, lake_home.head())) == w,
                60,
                desc=f"wave {wave} flushed ({wave + 1} files)",
            )

        files = livekit.lake_files(table, lake_home.head())
        assert len(files) == WAVES
        assert all(f.stats_state for f in files)

        # The compact sweep's candidate read wants hydrated stats (the
        # age flushes registered deferred) — the harness runs the
        # hydrator at 1s.
        await livekit.wait_until_async(
            lambda: all(
                f.stats_state == "provided"
                for f in livekit.lake_files(table, lake_home.head())
            ),
            60,
            desc="the hydrator backfilling deferred stats",
        )

        # Trigger one compaction sweep (the loop is off in this harness).
        result = await asyncio.to_thread(
            lake_home.client._request,
            "POST",
            f"/catalogs/{lake_home.catalog.name}/maintenance/compact",
        )
        assert result["groups_compacted"] == 1, result
        assert result["files_in"] == WAVES, result
        assert result["files_out"] == 1, result
        assert result["failed_groups"] == 0, result

        # The bucket rolled up; the table stays queryable with EXACT counts.
        head = lake_home.head()
        files_after = livekit.lake_files(table, head)
        assert len(files_after) == 1, [f.path for f in files_after]
        assert files_after[0].record_count == WAVES * PER_WAVE
        counts = livekit.lake_team_counts(table, head)
        assert counts == {TEAM: WAVES * PER_WAVE}

        # Full readback of the compacted output: exactly the produced seqs.
        data = await asyncio.to_thread(
            pq.read_table,
            files_after[0].path[len("s3://") :],
            filesystem=lake_home.fs,
        )
        assert data.num_rows == WAVES * PER_WAVE
        assert set(data.column("seq").to_pylist()) == set(range(WAVES * PER_WAVE))
