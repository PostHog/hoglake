"""Scenario: pipelined commits — the stage-A/stage-B split.

One logical producer builds commit payloads in order in the producer
thread (the validate + allocate stage of a pipelined hoglake server). A
bounded number of commits stay in flight to the server at once (the
durability stage). That is the ONLY difference from commit-throughput's
strictly sequential loop — and exactly the queue shape of the proposed
pipelined committer. Payload construction is deliberately serial; only
the durability wait overlaps.

Every commit is still acknowledged by the server only when durable. The
producer never learns a snapshot id before the server durably committed
it. Correctness invariants are checked on the result set: snapshot ids
must form the dense run (submission order may permute under concurrency;
density is a set property), and row ranges must tile.
"""

from __future__ import annotations

import argparse
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from ..context import Bench
from ..fabricate import BENCH_SCHEMA, append_payload
from ..runner import FailureGuard
from ..stats import Metric, Recorder
from ..runner import InvariantViolation
from .common import (
    ScenarioReport,
    check,
    make_bench_table,
)

IO_MODE = "metadata-only"

RECORD_COUNT = 100


def _stage(report, catalog, table, depth, ops, fpc, guard):
    """One pipeline depth: serial prepare, bounded in-flight commits."""
    rec = Recorder()
    ids: list[int] = []
    lock = threading.Lock()
    sem = threading.Semaphore(depth)
    errors = [0]

    def submit(i: int, payload) -> None:
        # payload is prepared by the PRODUCER thread (the serial stage-A
        # stand-in); workers only await durability (stage B)
        t0 = time.perf_counter_ns()
        try:
            r = catalog._commit(payload)
            guard.success()
            with lock:
                ids.append(r.snapshot_id)
                rec.record_ns(time.perf_counter_ns() - t0)
        except Exception as exc:  # noqa: BLE001
            guard.failure(exc)
            with lock:
                errors[0] += 1
        finally:
            sem.release()

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=depth) as pool:
        for i in range(ops):
            payload = append_payload(catalog, table, fpc, RECORD_COUNT)
            sem.acquire()
            pool.submit(submit, i, payload)
        pool.shutdown(wait=True)
    wall = time.perf_counter() - start

    check(errors[0] == 0, f"{errors[0]} failed commits (depth={depth})")
    check(len(ids) == ops, f"acked {len(ids)} != submitted {ops}")
    srt = sorted(ids)
    if srt != list(range(srt[0], srt[0] + ops)):
        idset = set(ids)
        dups = sorted({x for x in srt if ids.count(x) > 1})
        missing = [x for x in range(srt[0], srt[-1] + 1) if x not in idset]
        raise InvariantViolation(
            f"snapshot ids not dense: n={len(ids)} min={srt[0]} max={srt[-1]} "
            f"dups={dups[:8]} missing={missing[:8]}"
        )
    return report.add(
        Metric.from_recorder(f"pipelined.d{depth}.fpc{fpc}", rec, wall,
                             files_s=ops * fpc / wall if wall else 0.0)
    )


def run(bench: Bench, args: argparse.Namespace) -> ScenarioReport:
    report = ScenarioReport(
        scenario="commit-pipelined",
        params={"depths": args.depths, "ops": args.ops,
                "files_per_commit": args.files_per_commit},
    )
    guard = FailureGuard()
    for depth in args.depths:
        catalog, table = make_bench_table(bench, f"pipe{depth}")
        for fpc in args.files_per_commit:
            _stage(report, catalog, table, depth, args.ops, fpc, guard)
        # invariant: row ranges tile [0, total). Re-resolve the table so
        # the files read uses a head basis, never a cached creation one.
        fresh = catalog.namespace(table.namespace).table(table.name)
        files = fresh.files()
        expected = args.ops * sum(args.files_per_commit) * RECORD_COUNT
        got = sum(f.record_count for f in files)
        check(got == expected,
              f"row accounting: {got} rows in files, expected {expected}")
    return report


def add_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--depths", type=int, nargs="+", default=[1, 4, 16, 64],
                   help="in-flight commit depths to measure")
    p.add_argument("--ops", type=int, default=400)
    p.add_argument("--files-per-commit", type=int, nargs="+", default=[1])
