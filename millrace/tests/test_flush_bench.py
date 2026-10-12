"""Flush sweep throughput bench — M5's measurement (docs/kafka-ingestion.md
§The flush planner; ADVERSARIAL-REVIEW-2026-10-09 M5).

NON-GATING: the ``bench`` marker is deselected from default runs
(conftest removes bench items unless ``--bench`` is passed). Run with::

    uv run pytest --bench tests/test_flush_bench.py -s

Shape: 1024 ready keys (256 teams x 4 partition stages, 2 rows each) on
``memory:///`` stages, a scripted pyhoglake double (pytest-httpx, the
flush suite's flushkit) and the fake object store. One
:meth:`FlushRunner.run_once` is the unit of measurement — per-partition
assembly (stats scan + sched_age index + point reads), the plan, and
per-decision scan -> Arrow -> parquet -> upload -> persist -> publish ->
settle — end to end. Reported: keys/s and commits/s (one commit per
decision), serial (concurrency=1) vs bounded-parallel (concurrency=4,
the ``MILLRACE_FLUSH_CONCURRENCY`` default, with the flusher's I/O pool
sized to match, as main.py wires it).

The only assertions are correctness sanity (every decided key commits
exactly once; the stage drains): the numbers print for the report.
Caveat for the numbers: memory:/// and a scripted in-process server make
the WAL flush tick (5 ms here) the dominant per-commit latency; with S3
WAL writes (~tens of ms) the serial number would be far lower and the
parallel multiplier far larger.
"""

from __future__ import annotations

import time
from typing import Any

import pytest
from flushkit import (
    CommitCapture,
    FakeMonotonic,
    FakeObjectStore,
    commit_ok,
    event_payload,
    make_config,
    script_bootstrap,
    script_options,
)
from stagekit import NOW, fast_flush_settings

from millrace.flush import FlushRunner, HoglakeFlusher
from millrace.stage import StagedRecord, StageManager

pytestmark = pytest.mark.bench

PARTITIONS = 4
TEAMS_PER_PARTITION = 256
KEYS = PARTITIONS * TEAMS_PER_PARTITION  # 1024
ROWS_PER_TEAM = 2


async def _stage_all(manager: StageManager, clock: Any) -> None:
    for partition in range(PARTITIONS):
        stage = manager.stage("events", partition)
        records = []
        for i in range(TEAMS_PER_PARTITION):
            team = partition * TEAMS_PER_PARTITION + i
            for j in range(ROWS_PER_TEAM):
                offset = i * ROWS_PER_TEAM + j
                records.append(
                    StagedRecord(
                        team_id=team,
                        event_ts_us=NOW + offset,
                        offset=offset,
                        payload=event_payload(team, NOW + offset),
                    )
                )
        # Bounded stage batches, like the consumer's poll cycles.
        for i in range(0, len(records), 512):
            await stage.stage_batch(records[i : i + 512], now_us=clock())


def _make_flusher(httpx_mock: Any, store: FakeObjectStore, io_workers: int) -> Any:
    """A resolved flusher against the scripted server (bootstrap +
    retention reads registered here, once per flusher)."""
    from pyhoglake import HoglakeClient

    script_bootstrap(httpx_mock)
    script_options(httpx_mock)
    client = HoglakeClient("http://hog.test")
    client.s3 = store
    return HoglakeFlusher.resolve(
        client,
        make_config(target_output_bytes=10),
        now_us=lambda: NOW,
        sleep=_no_sleep,
        monotonic=FakeMonotonic(),
        io_workers=io_workers,
    )


async def _no_sleep(_seconds: float) -> None:
    return None


async def _run_once_timed(
    httpx_mock: Any,
    manager: StageManager,
    store: FakeObjectStore,
    *,
    concurrency: int,
    io_workers: int,
) -> tuple[float, int, int]:
    flusher = _make_flusher(httpx_mock, store, io_workers)
    capture = CommitCapture(httpx_mock, [commit_ok(1000 + i) for i in range(KEYS)])
    runner = FlushRunner(
        manager,
        flusher,
        flusher._cfg.planner_knobs(),
        now_us=lambda: NOW,
        sleep=_no_sleep,
        sweep_s=60.0,
        concurrency=concurrency,
    )
    start = time.perf_counter()
    sweep = await runner.run_once()
    elapsed = time.perf_counter() - start
    flusher.close()
    # Correctness sanity: every ready key committed exactly once.
    assert sweep.decisions == KEYS
    assert sweep.committed == KEYS
    assert sweep.failed == 0
    assert len(capture.bodies) == KEYS
    for partition in range(PARTITIONS):
        assert await manager.stage("events", partition).iter_key_stats() == []
    return elapsed, sweep.decisions, sweep.committed


async def test_flush_sweep_throughput(httpx_mock: Any, memory_store: Any) -> None:
    rows: list[tuple[str, float, int, int]] = []
    async with StageManager(
        "memory:///", "bench", settings=fast_flush_settings()
    ) as manager:
        for partition in range(PARTITIONS):
            await manager.open_partition("events", partition)
        clock = lambda: NOW
        store = FakeObjectStore()

        await _stage_all(manager, clock)
        serial_s, keys, commits = await _run_once_timed(
            httpx_mock, manager, store, concurrency=1, io_workers=1
        )
        rows.append(("serial (concurrency=1, io=1)", serial_s, keys, commits))

        await _stage_all(manager, clock)
        parallel_s, keys, commits = await _run_once_timed(
            httpx_mock, manager, store, concurrency=4, io_workers=16
        )
        rows.append(("parallel (concurrency=4, io=16)", parallel_s, keys, commits))

    print(
        f"\n-- millrace flush sweep bench ({KEYS} ready keys x "
        f"{ROWS_PER_TEAM} rows, {PARTITIONS} partitions) --"
    )
    for label, elapsed, keys, commits in rows:
        print(
            f"{label:40s} | {elapsed:7.2f} s | {keys / elapsed:8.1f} keys/s | "
            f"{commits / elapsed:8.1f} commits/s"
        )
    print("(design requirement: ~110 ready keys/s — ADVERSARIAL-REVIEW M5)")
