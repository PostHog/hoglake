"""Scenario 7 (Phase 5): the freshness SLA, measured.

Under a seeded Zipf-skewed load, every event's produce→queryable latency
is measured: the producer stamps ``seq`` per event; a poller watches the
lake (new files' parquet read back) and timestamps each seq's first
visibility. The suite asserts p99 ≤ flush deadline + margin and REPORTS
the measured distribution (the number is the point — plan Phase 5 item
7 feeds the Phase 6 model loop).

Age-triggered flushes only (tiny payloads, a huge size target): the tail
IS the whole distribution here.
"""

from __future__ import annotations

import asyncio
import statistics
import time

import livekit
import pyarrow.parquet as pq
import pytest
from livekit import now_us

pytestmark = pytest.mark.live

TENANTS = 30
EVENTS = 1500
SEED = 777
FLUSH_DEADLINE_S = 8
MARGIN_S = 15.0  # sweep + commit + poll granularity slack
POLL_S = 0.5


async def test_freshness_sla_under_skew(lake_home, topic_factory, producer_factory):
    table = lake_home.create_events_table()
    topic = topic_factory(partitions=4)
    producer = producer_factory(topic.name)
    skew = livekit.Skew(TENANTS, seed=SEED)

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
    visible: dict[int, float] = {}  # seq -> monotonic time first seen
    seen_paths: set[str] = set()
    stop_polling = asyncio.Event()

    async def poll_lake() -> None:
        while not stop_polling.is_set():
            head = await asyncio.to_thread(lake_home.head)
            files = await asyncio.to_thread(livekit.lake_files, table, head)
            new = [f for f in files if f.path not in seen_paths]
            for f in new:
                seen_paths.add(f.path)
                key = f.path[len("s3://") :]
                seqs = await asyncio.to_thread(
                    lambda k=key: (
                        pq.read_table(k, filesystem=lake_home.fs)
                        .column("seq")
                        .to_pylist()
                    )
                )
                now = time.monotonic()
                for seq in seqs:
                    if seq is not None and seq not in visible:
                        visible[seq] = now
            await asyncio.sleep(POLL_S)

    async with livekit.Pipeline(cfg, sweep_s=0.5):
        poller = asyncio.create_task(poll_lake())
        try:
            base = now_us()
            teams = skew.sample(EVENTS)

            def produce() -> None:
                for seq, team in enumerate(teams):
                    producer.send(team, base + seq, seq)
                producer.flush()

            await asyncio.to_thread(produce)
            produced_at = {e.seq: e.produced_monotonic for e in producer.produced}
            assert len(produced_at) == EVENTS

            await livekit.wait_until_async(
                lambda: len(visible) == EVENTS,
                FLUSH_DEADLINE_S + 120,
                desc=lambda: f"all {EVENTS} events queryable (seen {len(visible)})",
                interval_s=POLL_S,
            )
        finally:
            stop_polling.set()
            await poller

    latencies = sorted(visible[seq] - produced_at[seq] for seq in visible)
    assert len(latencies) == EVENTS
    p50 = statistics.median(latencies)
    p99 = latencies[int(len(latencies) * 0.99) - 1]
    worst = latencies[-1]
    print(
        f"\n[freshness] n={EVENTS} deadline={FLUSH_DEADLINE_S}s — "
        f"p50={p50:.2f}s p99={p99:.2f}s max={worst:.2f}s "
        f"(bound: deadline + {MARGIN_S}s margin = {FLUSH_DEADLINE_S + MARGIN_S}s)"
    )
    assert p99 <= FLUSH_DEADLINE_S + MARGIN_S, (
        f"p99 freshness {p99:.2f}s exceeded deadline+margin "
        f"({FLUSH_DEADLINE_S}+{MARGIN_S}s)"
    )
