"""Flush progress with the stage store behind S3-like latency (#380).

The regression: slatedb 0.17's default ``ScanOptions`` fetch ONE block
(~4 KiB) per object-store GET, one fetch at a time. Once a group's rows
live in an L0 SST (the memtable froze at ``l0_sst_size_bytes``, 64 MiB),
the flush window scan reads at block_size / GET latency: at S3 latency a
few MiB of window took minutes per flush. The fix scans with multi-MiB
read-ahead (stage._bulk_scan_options).

Shape: ONE partition, a ~100 MiB backlog over a handful of groups
produced before the pipeline starts, so the memtable freezes into an L0
SST during the initial staging burst and every group's window is mostly
SST-resident when its age deadline fires. The pipeline runs in-process
(its SlateDB tuning is in-process configuration); its STAGE store
resolves through toxiproxy with a latency toxic while the lake stays
direct. A 1 MiB block cache stands in for a pod's shared, churned one.
Flush concurrency 1, so a slow scan is a gap between committed flushes.

Asserts: the backlog reached an L0 SST before the first flush committed;
after the first flush is due, no gap between committed flushes exceeds
``GAP_S``; staged bytes never exceed the backlog and drain to zero; every
row lands at least once.
"""

from __future__ import annotations

import asyncio
import os
import time

import livekit
import pytest
from livekit import now_us

from millrace.config import SlateDbConfig
from millrace.slatedb_metrics import snapshot_recorder

pytestmark = pytest.mark.live

TEAMS = 8
EVENTS = 32_000
PAD_BYTES = 3000  # ~3.1 KiB rows: ~100 MiB backlog, past one 64 MiB memtable
FLUSH_DEADLINE_S = 45
GAP_S = 30.0
LATENCY_MS = 60
JITTER_MS = 40
STAGING_TIMEOUT_S = 150.0
DRAIN_TIMEOUT_S = 180.0
# A memtable flush inserts its data blocks into the block cache, so with
# the default 128 MiB a lone partition's L0 SST is served from memory.
# In production that cache is shared and churned; a 1 MiB block half
# reproduces the miss (the meta half keeps its default).
BLOCK_CACHE_BYTES = 1024 * 1024


def _l0_files(pipe: livekit.Pipeline) -> float:
    total = 0.0
    for recorder in pipe.stages.metrics_recorders().values():
        for snap in snapshot_recorder(recorder):
            if snap.name == "slatedb.db.l0_sst_count" and snap.value is not None:
                total += float(snap.value)
    return total


async def test_flushes_keep_pace_with_s3_latency_on_the_stage(
    lake_home, topic_factory, producer_factory
):
    table = lake_home.create_events_table()
    topic = topic_factory(partitions=1)
    producer = producer_factory(topic.name)

    base = now_us()

    def produce() -> list[livekit.ProducedEvent]:
        for seq in range(EVENTS):
            producer.send(seq % TEAMS + 1, base + seq, seq, pad_bytes=PAD_BYTES)
        return producer.flush(timeout_s=120)

    produced = await asyncio.to_thread(produce)
    assert len(produced) == EVENTS
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
        # Age lane only: the size lane never fires on this backlog.
        flush_bytes=1024 * 1024 * 1024,
        flush_deadline_s=FLUSH_DEADLINE_S,
    )
    import dataclasses

    cfg = dataclasses.replace(
        cfg,
        flush_concurrency=1,
        slatedb=SlateDbConfig(block_cache_bytes=BLOCK_CACHE_BYTES),
    )
    with livekit.S3LatencyProxy(latency_ms=LATENCY_MS, jitter_ms=JITTER_MS) as proxy:
        # SlateDB resolves the stage store from AWS_ENDPOINT_URL when the
        # StageManager is built; the lake (pyhoglake) stays direct.
        saved = os.environ.get("AWS_ENDPOINT_URL")
        os.environ["AWS_ENDPOINT_URL"] = proxy.endpoint
        try:
            pipe = livekit.Pipeline(cfg, sweep_s=0.5)
        finally:
            if saved is None:
                os.environ.pop("AWS_ENDPOINT_URL", None)
            else:
                os.environ["AWS_ENDPOINT_URL"] = saved
        async with pipe:
            assert pipe.consumer is not None and pipe.flusher is not None
            ready_at = time.monotonic()
            due_at = ready_at + FLUSH_DEADLINE_S
            staged_done_at: float | None = None
            l0_seen_at: float | None = None
            first_commit_at: float | None = None
            committed = 0
            last_progress = due_at
            gaps: list[float] = []
            peak_staged = 0

            def status(now: float) -> str:
                return (
                    f"t={now - ready_at:.0f}s committed={committed} "
                    f"gaps={[round(x, 1) for x in gaps]} staged_done="
                    f"{None if staged_done_at is None else round(staged_done_at - ready_at)}s "
                    f"l0_seen={None if l0_seen_at is None else round(l0_seen_at - ready_at)}s "
                    f"peak_staged={peak_staged / 2**20:.1f}MiB"
                )

            while True:
                await asyncio.sleep(1.0)
                now = time.monotonic()
                assert pipe.runner is not None and pipe.runner.halted is None
                staged_rows = pipe.consumer.stats().records_staged
                staged_bytes = pipe.stages.gauges().staged_bytes
                peak_staged = max(peak_staged, staged_bytes)
                if staged_done_at is None and staged_rows >= EVENTS:
                    staged_done_at = now
                if l0_seen_at is None and _l0_files(pipe) > 0:
                    l0_seen_at = now
                c = pipe.flusher.stats().decisions_committed
                if c > committed:
                    if first_commit_at is None:
                        first_commit_at = now
                    gaps.append(now - last_progress)
                    last_progress = now
                    committed = c
                if staged_done_at is None:
                    assert now - ready_at < STAGING_TIMEOUT_S, (
                        f"backlog never finished staging: {status(now)}"
                    )
                if now > last_progress + GAP_S:
                    raise AssertionError(
                        f"no committed flush for {now - last_progress:.0f}s "
                        f"(bound {GAP_S}s) — the stage scans are crawling: "
                        f"{status(now)}"
                    )
                if (
                    staged_done_at is not None
                    and committed > 0
                    and pipe.stages.gauges().staged_groups == 0
                ):
                    break
                assert now - due_at < DRAIN_TIMEOUT_S, f"never drained: {status(now)}"

            print(f"\n[stage-latency] {status(time.monotonic())}")
            assert first_commit_at is not None and staged_done_at is not None
            # The scenario's own precondition: rows sat in an L0 SST
            # before any window was scanned.
            assert l0_seen_at is not None and l0_seen_at <= first_commit_at, (
                f"the backlog never reached an L0 SST before the first flush: "
                f"{status(time.monotonic())}"
            )
            assert staged_done_at <= first_commit_at, status(time.monotonic())
            assert max(gaps) <= GAP_S
            assert peak_staged <= payload_bytes * 1.05, (
                f"peak staged {peak_staged} > backlog payloads {payload_bytes}"
            )

    counts = await asyncio.to_thread(
        livekit.lake_seq_counts, table, lake_home.head(), lake_home.fs
    )
    livekit.assert_at_least_once(counts, [e.seq for e in produced])
