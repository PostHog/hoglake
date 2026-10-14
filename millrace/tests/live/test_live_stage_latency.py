"""Flush progress with the stage store behind S3-like latency.

The regression: slatedb 0.17's default ``ScanOptions`` fetch ONE block
(~4 KiB) per object-store GET, one fetch at a time. Once a tuple's rows
live in an L0 SST (the memtable froze at ``l0_sst_size_bytes``, 64 MiB),
the flush window scan and the settlement scan — the latter inside
``commit_flushed``, holding the stage write lock — read at
block_size / GET latency. At S3 latency a few MiB of window took minutes
per flush and staging stalled behind the lock. The fix scans with
multi-MiB read-ahead (stage._bulk_scan_options; README §Scan
read-ahead).

Shape: ONE partition, a ~100 MiB backlog over a handful of tuples
produced before the child starts, so the memtable freezes into an L0
SST during the initial staging burst and every tuple's window is
mostly SST-resident when its age deadline fires. The child's STAGE
store (``AWS_ENDPOINT_URL``) goes through toxiproxy with a latency
toxic; the lake (``HOGLAKE_S3_ENDPOINT``) stays direct. A 1 MiB block
cache stands in for a pod's shared, churned one (``BLOCK_CACHE_BYTES``).
Flush concurrency 1, so a slow scan is a gap between committed flushes.

Asserts: the backlog reached an L0 SST before the first flush committed
(the scenario exercised SST reads at all); after the first flush is due,
no gap between committed flushes exceeds ``GAP_S``; staged bytes never
exceed the backlog and drain to zero; every row lands exactly once.
"""

from __future__ import annotations

import time
from collections import Counter

import livekit
import pytest
from livekit import now_us

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
MAX_UNFLUSHED_BYTES = 96 * 1024 * 1024  # dev-shaped; must exceed 64 MiB
# A memtable flush inserts its data blocks into the block cache, so with
# the default 128 MiB a lone partition's L0 SST is served from memory.
# In production that cache is shared by every partition on the pod and
# churned by each one's 64 MiB flushes; a 1 MiB block half reproduces
# the miss (the meta half keeps its default — index/filter hits stay).
BLOCK_CACHE_BYTES = 1024 * 1024


def _sum(samples: livekit.MetricSamples, name: str) -> float:
    return sum(v for (n, _), v in samples.items() if n == name)


def test_flushes_keep_pace_with_s3_latency_on_the_stage(
    lake_home, topic_factory, producer_factory
):
    table = lake_home.create_events_table()
    topic = topic_factory(partitions=1)
    producer = producer_factory(topic.name)

    base = now_us()
    for seq in range(EVENTS):
        producer.send(seq % TEAMS + 1, base + seq, seq, pad_bytes=PAD_BYTES)
    produced = producer.flush(timeout_s=120)
    assert len(produced) == EVENTS
    expected = Counter(e.team_id for e in produced)
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
        target_output_bytes=1024 * 1024 * 1024,
        flush_deadline_s=FLUSH_DEADLINE_S,
        flush_sweep_s=1,
    )
    port = livekit.free_port()
    with livekit.S3LatencyProxy(latency_ms=LATENCY_MS, jitter_ms=JITTER_MS) as proxy:
        child = livekit.Child(
            livekit.child_env(
                cfg,
                metrics_port=port,
                extra={
                    "AWS_ENDPOINT_URL": proxy.endpoint,
                    "MILLRACE_FLUSH_CONCURRENCY": "1",
                    "MILLRACE_SLATEDB_MAX_UNFLUSHED_BYTES": str(MAX_UNFLUSHED_BYTES),
                    "MILLRACE_SLATEDB_BLOCK_CACHE_BYTES": str(BLOCK_CACHE_BYTES),
                },
            ),
            name="stage-latency",
        )
        child.start()
        try:
            child.wait_ready(port, timeout_s=120)
            ready_at = time.monotonic()
            due_at = ready_at + FLUSH_DEADLINE_S
            staged_done_at: float | None = None
            l0_seen_at: float | None = None
            first_commit_at: float | None = None
            committed = 0.0
            last_progress = due_at
            gaps: list[float] = []
            peak_staged = 0.0

            def status(now: float) -> str:
                return (
                    f"t={now - ready_at:.0f}s committed={committed:.0f} "
                    f"gaps={[round(x, 1) for x in gaps]} staged_done="
                    f"{None if staged_done_at is None else round(staged_done_at - ready_at)}s "
                    f"l0_seen={None if l0_seen_at is None else round(l0_seen_at - ready_at)}s "
                    f"peak_staged={peak_staged / 2**20:.1f}MiB"
                )

            while True:
                time.sleep(1.0)
                now = time.monotonic()
                if child.proc is not None and child.proc.poll() is not None:
                    raise AssertionError(
                        f"child exited ({child.proc.returncode}) {status(now)}; "
                        f"output:\n{child.output()[-4000:]}"
                    )
                m = livekit.scrape_metrics(port)
                staged_rows = _sum(m, "millrace_records_staged_total")
                staged_bytes = _sum(m, "millrace_staged_bytes")
                peak_staged = max(peak_staged, staged_bytes)
                if staged_done_at is None and staged_rows >= EVENTS:
                    staged_done_at = now
                if l0_seen_at is None and _sum(m, "millrace_slatedb_l0_sst_files") > 0:
                    l0_seen_at = now
                c = _sum(m, "millrace_flushes_committed_total")
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
                if staged_done_at is not None and committed > 0 and staged_bytes == 0:
                    break
                assert now - due_at < DRAIN_TIMEOUT_S, f"never drained: {status(now)}"

            print(f"\n[stage-latency] {status(time.monotonic())}")
            assert first_commit_at is not None and staged_done_at is not None
            # The scenario's own precondition: rows sat in an L0 SST
            # before any window was scanned — otherwise every scan was
            # memtable-only and this test proves nothing.
            assert l0_seen_at is not None and l0_seen_at <= first_commit_at, (
                f"the backlog never reached an L0 SST before the first flush: "
                f"{status(time.monotonic())}"
            )
            assert staged_done_at <= first_commit_at, status(time.monotonic())
            assert max(gaps) <= GAP_S
            # The staged-bytes ceiling: nothing beyond the backlog's
            # payloads was ever accounted (and it drained to zero above).
            assert peak_staged <= payload_bytes * 1.05, (
                f"peak staged {peak_staged} > backlog payloads {payload_bytes}"
            )
        finally:
            child.terminate()
            try:
                child.wait_exit(60)
            except Exception:  # noqa: BLE001 - a hung shutdown is killed
                child.kill()

    counts = livekit.lake_team_counts(table, lake_home.head())
    assert counts == expected, (sorted(counts.items()), sorted(expected.items()))
