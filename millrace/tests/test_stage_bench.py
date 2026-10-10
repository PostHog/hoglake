"""SlateDB rate bench — validation item 2 of docs/kafka-ingestion.md.

NON-GATING: the ``bench`` marker is deselected from default runs
(conftest removes bench items unless ``--bench`` is passed). Run with::

    uv run pytest --bench -m bench -s

Shape: the design's per-pod slice — batches of 500 records x 1024-byte
payloads (~0.5 MiB per batch; at the 5K msg/s event rate that is ten
batches per second) — measuring ``stage_batch`` (rows + stats + sched +
offsets, durability-acked), ``iter_key_stats``, full row scans, and
``commit_flushed`` settle throughput, on ``memory:///`` and ``file:///``
(tmpdir), at the default 100 ms WAL flush interval and at 10 ms.

Results print as a table (``-s``) for the PR body. The only assertions
are correctness sanity (counts reconcile); no thresholds gate.

The whale case (validation item 2's consume-path half, T9): one
partition, ~1 KB records, batches of 750 (a 500 ms poll's worth at the
design's ~1.5K msg/s whale-partition rate), run at FULL bench speed
(sustainably serving the whale needs the max rate to clear 1.5K msg/s
with headroom — the counterfactual "defaults stall?" is answered by
measurement, below the rate question's resolution). Arms: the library
defaults vs the production settings (``build_slatedb_settings`` — GC
off, bounded unflushed bytes), each reading the stage's OWN SlateDB
recorder for write-stall counters (``backpressure_count`` /
``l0_stall_count``), so a stall is evidence, not inference. The L0
stress arm shrinks SSTs to 1 MiB so SST turnover happens at bench
scale — that is the 55 MB/s shape (an L0 SST per ~1.2 s) compressed
into seconds.
"""

from __future__ import annotations

import tempfile
import time
from dataclasses import dataclass

import pytest
from slatedb.uniffi import ObjectStore, Settings
from stagekit import NOW, fast_flush_settings, window

from millrace.slatedb_metrics import snapshot_recorder
from millrace.stage import PartitionStage, StagedRecord, build_slatedb_settings

pytestmark = pytest.mark.bench

BATCHES = 20
BATCH_SIZE = 500
PAYLOAD_BYTES = 1024
TEAMS = 25
TOTAL_ROWS = BATCHES * BATCH_SIZE


@dataclass(frozen=True)
class BenchResult:
    backend: str
    flush_interval: str
    stage_s: float
    batch_lat_ms: list[float]
    stats_scan_s: float
    row_scan_s: float
    rows_scanned: int
    settle_s: float
    rows_deleted: int

    def render(self) -> str:
        staged_mib = TOTAL_ROWS * PAYLOAD_BYTES / 2**20
        lat = sorted(self.batch_lat_ms)
        p50 = lat[len(lat) // 2]
        p95 = lat[min(len(lat) - 1, int(len(lat) * 0.95))]
        return (
            f"{self.backend:24s} flush_interval={self.flush_interval:>6s} | "
            f"stage {TOTAL_ROWS / self.stage_s:9.0f} msg/s "
            f"({staged_mib / self.stage_s:7.1f} MiB/s, "
            f"batch p50 {p50:6.1f} ms p95 {p95:6.1f} ms) | "
            f"stats scan {self.stats_scan_s * 1e3:6.1f} ms | "
            f"row scan {self.rows_scanned / self.row_scan_s:9.0f} rows/s | "
            f"settle {self.rows_deleted / self.settle_s:9.0f} rows/s"
        )


def _payload(seed: int) -> bytes:
    return bytes([seed % 251 + 1]) * PAYLOAD_BYTES


async def _run_bench(
    store: ObjectStore, path: str, settings: Settings, label: str, interval: str
) -> BenchResult:
    stage = await PartitionStage.open_store(
        store, path, topic="events", partition=0, settings=settings
    )
    offset = 0
    latencies: list[float] = []
    stage_start = time.perf_counter()
    for batch_index in range(BATCHES):
        records = [
            StagedRecord(
                team_id=(offset + i) % TEAMS,
                event_ts_us=NOW + offset + i,
                offset=offset + i,
                payload=_payload(offset + i),
            )
            for i in range(BATCH_SIZE)
        ]
        t0 = time.perf_counter()
        await stage.stage_batch(records, now_us=NOW + batch_index)
        latencies.append((time.perf_counter() - t0) * 1000)
        offset += BATCH_SIZE
    stage_s = time.perf_counter() - stage_start

    t0 = time.perf_counter()
    stats = await stage.iter_key_stats()
    stats_scan_s = time.perf_counter() - t0
    assert sum(s.row_count for s in stats) == TOTAL_ROWS
    assert sum(s.staged_bytes for s in stats) == TOTAL_ROWS * PAYLOAD_BYTES

    rows_scanned = 0
    t0 = time.perf_counter()
    for team in range(TEAMS):
        async for _rk, _v in stage.scan_team_rows(team):
            rows_scanned += 1
    row_scan_s = time.perf_counter() - t0
    assert rows_scanned == TOTAL_ROWS

    rows_deleted = 0
    t0 = time.perf_counter()
    for team in range(TEAMS):
        team_offsets = [o for o in range(TOTAL_ROWS) if o % TEAMS == team]
        report = await stage.commit_flushed(
            window(team, min(team_offsets), max(team_offsets))
        )
        assert report.team_drained
        rows_deleted += report.rows_deleted
    settle_s = time.perf_counter() - t0
    assert rows_deleted == TOTAL_ROWS
    assert await stage.iter_key_stats() == []
    await stage.close()
    return BenchResult(
        label,
        interval,
        stage_s,
        latencies,
        stats_scan_s,
        row_scan_s,
        rows_scanned,
        settle_s,
        rows_deleted,
    )


def _print(results: list[BenchResult]) -> None:
    print(
        "\n-- millrace phase-2 stage bench "
        f"({BATCHES} batches x {BATCH_SIZE} records x {PAYLOAD_BYTES} B, {TEAMS} teams) --"
    )
    for r in results:
        print(r.render())


async def test_bench_memory():
    results = [
        await _run_bench(
            ObjectStore.resolve("memory:///"),
            "bench/default",
            Settings.default(),
            "memory:///",
            "100ms",
        ),
        await _run_bench(
            ObjectStore.resolve("memory:///"),
            "bench/fast",
            fast_flush_settings(),
            "memory:///",
            "5ms",
        ),
    ]
    _print(results)


async def test_bench_file():
    with tempfile.TemporaryDirectory() as td:
        results = [
            await _run_bench(
                ObjectStore.resolve("file:///"),
                f"{td}/default",
                Settings.default(),
                "file:///(tmp)",
                "100ms",
            ),
            await _run_bench(
                ObjectStore.resolve("file:///"),
                f"{td}/fast",
                fast_flush_settings(),
                "file:///(tmp)",
                "5ms",
            ),
        ]
    _print(results)


# -- the whale shape (validation item 2's consume-path half; T9) --------------------

WHALE_ROWS = 30_000
WHALE_BATCH = 750  # a 500 ms poll's worth at ~1.5K msg/s
WHALE_PAYLOAD = 1024
WHALE_TEAMS = 40  # one hot team (0) takes ~half; the tail shares the rest


def _whale_record(i: int) -> StagedRecord:
    team = 0 if i % 2 == 0 else 1 + (i % (WHALE_TEAMS - 1))
    return StagedRecord(
        team_id=team,
        event_ts_us=NOW + i,
        offset=i,
        payload=bytes([i % 251 + 1]) * WHALE_PAYLOAD,
    )


def _stall_counters(stage: PartitionStage) -> dict[str, int]:
    """The write-stall counters from the stage's own recorder (the
    observability surface an operator would read)."""
    out: dict[str, int] = {}
    for m in snapshot_recorder(stage.metrics_recorder):
        if m.name in (
            "slatedb.db.backpressure_count",
            "slatedb.db.l0_stall_count",
            "slatedb.db.l0_sst_count",
            "slatedb.db.total_mem_size_bytes",
            "slatedb.wal.wal_buffer_flushes",
        ):
            out[m.name] = out.get(m.name, 0) + int(m.value or 0)
    return out


@dataclass(frozen=True)
class WhaleResult:
    label: str
    rows: int
    stage_s: float
    batch_lat_ms: list[float]
    stalls: dict[str, int]

    def render(self) -> str:
        lat = sorted(self.batch_lat_ms)
        p50 = lat[len(lat) // 2]
        p95 = lat[min(len(lat) - 1, int(len(lat) * 0.95))]
        p100 = lat[-1]
        mib = self.rows * WHALE_PAYLOAD / 2**20
        return (
            f"{self.label:44s} | {self.rows / self.stage_s:8.0f} msg/s "
            f"({mib / self.stage_s:6.1f} MiB/s) | batch ms p50 {p50:5.1f} "
            f"p95 {p95:5.1f} max {p100:6.1f} | "
            f"stalls bp={self.stalls.get('slatedb.db.backpressure_count', 0)} "
            f"l0={self.stalls.get('slatedb.db.l0_stall_count', 0)} "
            f"| end L0={self.stalls.get('slatedb.db.l0_sst_count', 0)} "
            f"mem={self.stalls.get('slatedb.db.total_mem_size_bytes', 0) // 2**20} MiB"
        )


async def _run_whale(
    store: ObjectStore, path: str, settings: Settings, label: str
) -> WhaleResult:
    stage = await PartitionStage.open_store(
        store, path, topic="events", partition=0, settings=settings
    )
    lat: list[float] = []
    start = time.perf_counter()
    offset = 0
    while offset < WHALE_ROWS:
        batch = [
            _whale_record(offset + i)
            for i in range(min(WHALE_BATCH, WHALE_ROWS - offset))
        ]
        t0 = time.perf_counter()
        await stage.stage_batch(batch, now_us=NOW + offset)
        lat.append((time.perf_counter() - t0) * 1000)
        offset += len(batch)
    stage_s = time.perf_counter() - start
    stalls = _stall_counters(stage)
    stats = await stage.iter_key_stats()
    assert sum(s.row_count for s in stats) == WHALE_ROWS
    await stage.close()
    return WhaleResult(label, WHALE_ROWS, stage_s, lat, stalls)


async def test_bench_whale_single_partition():
    """~1.5K msg/s x ~1 KB records on ONE partition (30K rows): the
    library defaults vs the production settings, on memory:/// and
    file:/// — with the stage's own stall counters as evidence."""
    production = build_slatedb_settings(
        gc_enabled=False, max_unflushed_bytes=256 * 1024 * 1024, l0_max_ssts=8
    )
    results = [
        await _run_whale(
            ObjectStore.resolve("memory:///"),
            "whale/default",
            Settings.default(),
            "memory:/// library-defaults",
        ),
        await _run_whale(
            ObjectStore.resolve("memory:///"),
            "whale/prod",
            production,
            "memory:/// production (gc off, 256MiB, l0=8)",
        ),
    ]
    with tempfile.TemporaryDirectory() as td:
        results.append(
            await _run_whale(
                ObjectStore.resolve("file:///"),
                f"{td}/whale-default",
                Settings.default(),
                "file:///(tmp) library-defaults",
            )
        )
        results.append(
            await _run_whale(
                ObjectStore.resolve("file:///"),
                f"{td}/whale-prod",
                build_slatedb_settings(
                    gc_enabled=False,
                    max_unflushed_bytes=256 * 1024 * 1024,
                    l0_max_ssts=8,
                ),
                "file:///(tmp) production (gc off, 256MiB, l0=8)",
            )
        )
    print(
        f"\n-- whale bench ({WHALE_ROWS} rows x {WHALE_PAYLOAD} B, batches of {WHALE_BATCH}) --"
    )
    for r in results:
        print(r.render())


async def test_bench_whale_l0_stress():
    """The 55 MB/s shape compressed: 1 MiB L0 SSTs so turnover happens at
    bench scale, the default 8-deep L0 vs a raised one — does the
    embedded compactor (default 5 s poll) keep the stall counter at 0?"""

    def stress_settings(l0_max: int) -> Settings:
        s = build_slatedb_settings(
            gc_enabled=False, max_unflushed_bytes=256 * 1024 * 1024, l0_max_ssts=l0_max
        )
        s.set("l0_sst_size_bytes", str(1024 * 1024))
        return s

    rows = 12_000  # ~12 MiB -> ~12 SSTs, 1.5x the default L0 depth
    with tempfile.TemporaryDirectory() as td:
        results = []
        for l0_max in (8, 32):
            store = ObjectStore.resolve("file:///")
            stage = await PartitionStage.open_store(
                store,
                f"{td}/stress-{l0_max}",
                topic="events",
                partition=0,
                settings=stress_settings(l0_max),
            )
            lat: list[float] = []
            start = time.perf_counter()
            offset = 0
            while offset < rows:
                batch = [
                    _whale_record(offset + i)
                    for i in range(min(WHALE_BATCH, rows - offset))
                ]
                t0 = time.perf_counter()
                await stage.stage_batch(batch, now_us=NOW + offset)
                lat.append((time.perf_counter() - t0) * 1000)
                offset += len(batch)
            stage_s = time.perf_counter() - start
            stalls = _stall_counters(stage)
            assert sum(s.row_count for s in await stage.iter_key_stats()) == rows
            await stage.close()
            results.append(
                WhaleResult(
                    f"file:/// L0 stress l0_max_ssts={l0_max}",
                    rows,
                    stage_s,
                    lat,
                    stalls,
                )
            )
    print(f"\n-- whale L0 stress ({rows} rows x {WHALE_PAYLOAD} B, 1 MiB SSTs) --")
    for r in results:
        print(r.render())
