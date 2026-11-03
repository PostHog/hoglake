"""2.2b: millrace on an object-prefix table.

The option is set on the empty table before the pipeline starts, as
``ingest.events_v2`` is created, so every file millrace commits is
placed: under its team's prefix, with its ``team_id`` stats inline, and
``provided`` at commit, never pending for the hydrator. A row no prefix
can hold (a negative ``team_id``) is dropped and counted, never stuck.
"""

from __future__ import annotations

import asyncio

import livekit
import pytest
from livekit import now_us
from pyhoglake import object_prefix
from pyhoglake.ops import AlterOp

pytestmark = pytest.mark.live

FLUSH_DEADLINE_S = 2
TEAMS = (4, 42, 7)


async def test_files_are_placed_under_their_team_prefix(
    lake_home, topic_factory, producer_factory, admin
):
    table = lake_home.create_events_table()
    await asyncio.to_thread(
        table.alter, [AlterOp("set_object_prefix_field", {"name": "team_id"})]
    )
    topic = topic_factory(partitions=2)
    producer = producer_factory(topic.name)
    cfg = livekit.live_config(
        topic=topic.name,
        group=topic.group,
        stage_base_path=f"stage/{livekit.RUN_ID}/{lake_home.slug}",
        catalog=lake_home.catalog.name,
        namespace="ns1",
        table=table.name,
        flush_deadline_s=FLUSH_DEADLINE_S,
    )
    base = now_us()
    rows = [(team, base + i) for i, team in enumerate(TEAMS * 20)]
    async with livekit.Pipeline(cfg, sweep_s=0.5) as pipe:
        events = await asyncio.to_thread(
            livekit.produce_batch, producer, [*rows, (-1, base + len(rows))]
        )
        placeable = [e.seq for e in events if e.team_id >= 0]
        await livekit.wait_until_async(
            lambda: livekit.all_landed(
                table, lake_home.catalog, lake_home.fs, placeable
            ),
            FLUSH_DEADLINE_S + 60,
            desc="every placeable event queryable",
        )
        assert pipe.flusher is not None
        await livekit.wait_until_async(
            lambda: pipe.stages.gauges().staged_groups == 0,
            30,
            desc=lambda: f"staging residue: {pipe.stages.gauges()}",
        )
        assert pipe.flusher.stats().records_dropped == {"object_prefix_value": 1}

    files = livekit.lake_files(table, lake_home.head())
    data_path = lake_home.catalog.data_path
    info = await asyncio.to_thread(table.info, totals=False)
    for f in files:
        assert f.partition_values is not None
        team = f.partition_values[0]
        prefix = object_prefix.prefix(data_path, info, int(team))
        assert f.path.startswith(prefix), f.path
        assert f.stats_state == "provided", f
    assert {f.partition_values[0] for f in files if f.partition_values} == {
        str(t) for t in TEAMS
    }
