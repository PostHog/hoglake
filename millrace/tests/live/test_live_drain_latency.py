"""The startup drain of a many-group backlog, with the stage store behind
S3-like latency (the managed-warehouse dev stall on 6dffb79).

The regression: opening a stage discovered its groups with a skip-scan,
one fresh default-options iterator per group (a seek that consults every
L0 SST and sorted run: several sequential GETs, ~270 ms per group at
60 ms latency), partition after partition, before the drain flushed
anything. A pod restarting onto hours of backlog (thousands of
identity(team_id) × day(timestamp) groups) logged its assignment and
then nothing for 18+ minutes, unready, blocking the rollout. The fix
drains as a stream: one read-ahead pass over each partition's rows,
flushed window by window, partitions concurrently.

Shape: a day-partitioned destination (identity(team_id), day(timestamp))
and a 2-partition topic carrying ``TEAMS`` × ``DAYS`` groups, two rows
each, ~1.5 KiB per row. A first pipeline stages the backlog with its
flush loop off and stops (its stages close: the rows sit in L0 SSTs).
A second pipeline restarts with its STAGE store behind toxiproxy
latency; the lake stays direct and its flush loop stays off, so only
the startup drain can publish anything.

Asserts: the restart is ready (the drain finished) within
``READY_BOUND_S``; the drain left nothing staged and counted every row;
every produced seq is in the lake at least once.
"""

from __future__ import annotations

import asyncio
import dataclasses
import os
import time

import livekit
import pytest
from livekit import now_us
from pyhoglake import ops

from millrace.config import SlateDbConfig

pytestmark = pytest.mark.live

TEAMS = 150
DAYS = 24  # 3,600 groups across the two partitions
ROWS_PER_GROUP = 2
PAD_BYTES = 1400
LATENCY_MS = 60
JITTER_MS = 20
READY_BOUND_S = 120.0
STAGING_TIMEOUT_S = 180.0
DAY_US = 86_400 * livekit.MICROS_PER_SECOND
# A pod's shared block cache is churned; a small one reproduces the miss.
BLOCK_CACHE_BYTES = 1024 * 1024


def _with_stage_endpoint(endpoint: str, build):  # type: ignore[no-untyped-def]
    """Build a pipeline whose SlateDB stage store resolves to
    ``endpoint`` (read from AWS_ENDPOINT_URL when the StageManager is
    built); the lake (pyhoglake) stays direct."""
    saved = os.environ.get("AWS_ENDPOINT_URL")
    os.environ["AWS_ENDPOINT_URL"] = endpoint
    try:
        return build()
    finally:
        if saved is None:
            os.environ.pop("AWS_ENDPOINT_URL", None)
        else:
            os.environ["AWS_ENDPOINT_URL"] = saved


async def test_a_many_group_backlog_drains_quickly_behind_s3_latency(
    lake_home, topic_factory, producer_factory
):
    table = lake_home.namespace.create_table("events", livekit.events_schema())
    lake_home.tables.append(table)
    ids = {c.name: c.field_id for c in table.columns}
    table.alter(
        [
            ops.set_partition_spec(
                [
                    ops.partition_field(ids["team_id"], "identity"),
                    ops.partition_field(ids["timestamp"], "day"),
                ]
            )
        ]
    )
    topic = topic_factory(partitions=2)
    producer = producer_factory(topic.name)
    start_day = (now_us() // DAY_US - DAYS) * DAY_US
    record_ts = now_us()

    def produce() -> list[livekit.ProducedEvent]:
        seq = 0
        for row in range(ROWS_PER_GROUP):
            for team in range(1, TEAMS + 1):
                for day in range(DAYS):
                    ts = start_day + day * DAY_US + row * 1_000_000 + team
                    producer.send(
                        team, ts, seq, pad_bytes=PAD_BYTES, record_ts_us=record_ts
                    )
                    seq += 1
        return producer.flush(timeout_s=120)

    produced = await asyncio.to_thread(produce)
    events = TEAMS * DAYS * ROWS_PER_GROUP
    assert len(produced) == events
    assert {e.partition for e in produced} == {0, 1}
    payload_bytes = sum(
        len(livekit.event_payload(e.team_id, e.ts_us, e.seq, pad_bytes=PAD_BYTES))
        for e in produced
    )

    cfg = livekit.live_config(
        topic=topic.name,
        group=topic.group,
        stage_base_path=f"stage/{livekit.RUN_ID}/{lake_home.slug}",
        catalog=lake_home.catalog.name,
        namespace="ns1",
        table=table.name,
    )
    cfg = dataclasses.replace(
        cfg, slatedb=SlateDbConfig(block_cache_bytes=BLOCK_CACHE_BYTES)
    )

    # 1. A previous process stages the whole backlog, never flushes, stops.
    first = livekit.Pipeline(cfg, auto_flush=False)
    async with first:
        assert first.consumer is not None
        await livekit.wait_until_async(
            lambda: first.consumer.stats().records_staged >= events,
            timeout_s=STAGING_TIMEOUT_S,
            desc=lambda: f"staged {first.consumer.stats().records_staged}/{events}",
        )
        assert first.stages.gauges().staged_groups == TEAMS * DAYS

    # 2. The restart, its stage behind S3-like latency: ready only once
    # the startup drain has flushed everything staged.
    with livekit.S3LatencyProxy(latency_ms=LATENCY_MS, jitter_ms=JITTER_MS) as proxy:
        second = _with_stage_endpoint(
            proxy.endpoint, lambda: livekit.Pipeline(cfg, auto_flush=False)
        )
        started = time.monotonic()
        try:
            try:
                await asyncio.wait_for(second.start(), timeout=READY_BOUND_S)
            except TimeoutError:
                staged = second.stages.gauges()
                raise AssertionError(
                    f"not ready {READY_BOUND_S:.0f}s into the startup drain of "
                    f"{TEAMS * DAYS} groups / {events} rows behind {LATENCY_MS}ms "
                    f"stage latency (open partitions: {staged.partitions}, "
                    f"commits so far: "
                    f"{second.flusher.stats().commits if second.flusher else 0})"
                ) from None
            ready_s = time.monotonic() - started
            assert second.runner is not None and second.runner.halted is None
            drained_rows = getattr(second.runner, "drain_rows", None)
            drained_bytes = getattr(second.runner, "drain_bytes", None)
            print(
                f"\n[drain-latency] ready in {ready_s:.1f}s: {events} rows, "
                f"{payload_bytes / 2**20:.1f} MiB, {TEAMS * DAYS} groups "
                f"({events / ready_s:.0f} rows/s, "
                f"{payload_bytes / 2**20 / ready_s:.2f} MiB/s); "
                f"drain_rows={drained_rows} drain_bytes={drained_bytes}"
            )
            assert second.stages.gauges().staged_groups == 0
            assert drained_rows == events
            assert drained_bytes == payload_bytes
        finally:
            await second.stop()

    counts = await asyncio.to_thread(
        livekit.lake_seq_counts, table, lake_home.head(), lake_home.fs
    )
    livekit.assert_at_least_once(counts, [e.seq for e in produced])
