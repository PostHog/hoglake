"""Scenario: pipelined commits — serial prepare, bounded in-flight commits.

One producer thread builds commit payloads in order while a bounded
number of commits stay in flight to the server at once. That is the
client-side queue shape a pipelined committer (a serial prepare stage
feeding an overlapping durability-wait stage) would drive — and it is
the only thing this scenario controls. It does NOT model server
internals: validation and snapshot/row-id allocation still run on the
server, inside each concurrent commit request. Prepare overlaps the
in-flight commits even at depth 1, so depth 1 is not
commit-throughput's sequential baseline (that scenario also warms up
and times fabrication inside its op loop — the latency numbers are not
directly comparable).

Ack-only-when-durable is the backend contract this bench ASSUMES, not
something it verifies: a commit response proves the server answered,
not that it waited for durability (there is no fault injection or
restart check here). What IS verified, per catalog, after all stages:

- acked snapshot ids form one anchored contiguous run starting at
  start_head + 1 (a set property — completion order may permute under
  concurrency, density may not)
- the server's persisted snapshot history after start_head is exactly
  that run, and the final head equals the last acked id
- row-id ranges tile [0, total) on a fresh head-basis table listing,
  and the listing's file-path multiset equals the submitted payloads' —
  a lost commit plus a duplicated one cannot hide behind a row sum

Latency samples cover the commit call only (fabrication is excluded —
it happens in the producer thread); wall-clock throughput includes it.
Caveat: all in-flight commits share one client process, so at high
depth the reported latencies include a client-side share (GIL, httpx
connection pool) — treat cross-depth comparisons as client-inclusive,
same as commit-contention.
"""

from __future__ import annotations

import argparse
import threading
import time
from collections import Counter
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

from pyhoglake import Catalog, Table

from ..context import Bench
from ..fabricate import append_payload
from ..runner import BenchAbort, FailureGuard
from ..stats import Metric, Recorder
from .common import (
    ScenarioReport,
    assert_dense_snapshots,
    assert_row_tiling,
    check,
    make_bench_table,
    notice,
)

#: Deliberately metadata-only: fabricated appends stress the commit
#: path's concurrency behavior without object-store IO. Pipelined
#: numbers are control-plane cost, never end-to-end write cost.
IO_MODE = "metadata-only"

RECORD_COUNT = 100


def _comma_ints(s: str) -> list[int]:
    """Comma-separated positive ints, e.g. ``1,4,16,64``."""
    try:
        vals = [int(x) for x in s.split(",")]
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"comma-separated integers expected, got {s!r}"
        ) from None
    if not vals or any(v < 1 for v in vals):
        raise argparse.ArgumentTypeError(f"values must be >= 1, got {s!r}")
    return vals


def _positive_int(s: str) -> int:
    v = int(s)
    if v < 1:
        raise argparse.ArgumentTypeError(f"must be >= 1, got {s!r}")
    return v


def _stage(
    report: ScenarioReport,
    catalog: Catalog,
    table: Table,
    ids: list[int],
    submitted: list[str],
    depth: int,
    ops: int,
    fpc: int,
    warmup: int,
    duration: float | None,
    guard: FailureGuard,
) -> int:
    """One pipeline depth: serial prepare, bounded in-flight commits.

    Payloads are built by the producer thread, in order; pool workers
    only run the commit call. Returns the number of payloads actually
    submitted (warmup + measured — a ``--duration`` cut submits fewer).
    Acked snapshot ids append to ``ids`` and every submitted payload's
    file paths to ``submitted``; the per-catalog invariants in ``run``
    consume both. Worker exceptions are retained in the futures and
    re-raised here after in-flight work drains (BenchAbort first).
    """
    rec = Recorder()
    warm = Recorder()
    lock = threading.Lock()
    sem = threading.Semaphore(depth)
    stop = threading.Event()
    errors = [0]
    futures: list[Future[None]] = []

    def submit(payload: dict[str, Any], recorder: Recorder) -> None:
        t0 = time.perf_counter_ns()
        try:
            r = catalog._commit(payload)
        except Exception as exc:
            # count BEFORE any guard call that can throw, so the tally
            # survives a tripping guard
            with lock:
                errors[0] += 1
            if not guard.is_server_failure(exc):
                # 4xx and friends are harness bugs, not server weather:
                # stop producing and propagate (exit 5), never swallow
                stop.set()
                raise
            try:
                guard.failure(exc)
            except BenchAbort:
                # server looks dead: stop producing; the abort is
                # re-raised from the future after the pool drains
                stop.set()
                raise
        else:
            guard.success()
            with lock:
                ids.append(r.snapshot_id)
                recorder.record_ns(time.perf_counter_ns() - t0)
        finally:
            sem.release()

    def drain(from_idx: int) -> None:
        """Wait out every future, then re-raise the first worker
        failure — a BenchAbort (server unresponsive) outranks anything
        else."""
        first_abort: BenchAbort | None = None
        first_other: BaseException | None = None
        for f in futures[from_idx:]:
            exc = f.exception()
            if exc is None:
                continue
            if isinstance(exc, BenchAbort):
                if first_abort is None:
                    first_abort = exc
            elif first_other is None:
                first_other = exc
        if first_abort is not None:
            raise first_abort
        if first_other is not None:
            raise first_other

    submitted_n = 0
    with ThreadPoolExecutor(max_workers=depth) as pool:

        def produce(n: int, recorder: Recorder, deadline: float | None) -> int:
            nonlocal submitted_n
            made = 0
            for _ in range(n):
                if stop.is_set():
                    break
                if deadline is not None and time.monotonic() > deadline:
                    break
                # prepare is serial by construction: only this thread
                # ever calls append_payload
                payload = append_payload(catalog, table, fpc, RECORD_COUNT)
                sem.acquire()
                if stop.is_set():  # abort landed while blocked on the sem
                    sem.release()
                    break
                with lock:
                    submitted.extend(f["path"] for f in payload["appends"][0]["files"])
                futures.append(pool.submit(submit, payload, recorder))
                submitted_n += 1
                made += 1
            return made

        produce(warmup, warm, None)
        drain(0)
        n_warm = len(futures)
        deadline = time.monotonic() + duration if duration is not None else None
        wall_t0 = time.perf_counter()
        n_measured = produce(ops, rec, deadline)
        drain(n_warm)
        wall = time.perf_counter() - wall_t0

    check(errors[0] == 0, f"{errors[0]} failed commits (depth={depth})")
    if n_measured:
        report.add(
            Metric.from_recorder(
                f"pipelined.d{depth}.fpc{fpc}",
                rec,
                wall,
                files_s=rec.count * fpc / wall if wall else 0.0,
            )
        )
    else:
        notice(
            f"commit-pipelined depth={depth} fpc={fpc}: --duration cut "
            "the stage before any measured commit; no metric emitted"
        )
    return submitted_n


def run(bench: Bench, args: argparse.Namespace) -> ScenarioReport:
    report = ScenarioReport(
        scenario="commit-pipelined",
        params={
            "depths": args.depths,
            "ops": args.ops,
            "files_per_commit": args.files_per_commit,
            "warmup": args.warmup,
            "duration": args.duration,
            "url": args.url,
        },
    )
    guard = FailureGuard()
    for depth in args.depths:
        catalog, table = make_bench_table(bench, f"pipe{depth}")
        start_head = catalog.refresh().head_snapshot_id  # after all DDL
        ids: list[int] = []
        submitted: list[str] = []
        commits = 0
        expected_rows = 0
        for fpc in args.files_per_commit:
            n = _stage(
                report,
                catalog,
                table,
                ids,
                submitted,
                depth,
                args.ops,
                fpc,
                args.warmup,
                args.duration,
                guard,
            )
            commits += n
            expected_rows += n * fpc * RECORD_COUNT

        # invariants, anchored at this catalog's post-DDL head and
        # spanning ALL its stages (no per-stage reset):
        # 1. the acked ids are exactly the contiguous run after the head
        srt = sorted(ids)
        want_ids = list(range(start_head + 1, start_head + commits + 1))
        if srt != want_ids:
            idset = set(ids)
            dups = sorted({x for x in srt if ids.count(x) > 1})
            missing = [x for x in want_ids if x not in idset]
            check(
                False,
                f"acked snapshot ids != anchored run ({start_head + 1}.."
                f"{start_head + commits}): n={len(ids)} dups={dups[:8]} "
                f"missing={missing[:8]}",
            )
        # 2. the server's persisted snapshot history tells the same
        #    story, and the head lands on the last acked id
        assert_dense_snapshots(catalog, start_head, commits)
        head = catalog.refresh().head_snapshot_id
        check(
            head == start_head + commits,
            f"head snapshot {head} != expected {start_head + commits}",
        )
        # 3. row ranges tile [0, total) on a fresh head-basis listing
        #    (re-resolved table, never a cached creation basis), and the
        #    listing holds exactly the submitted files — a lost commit
        #    paired with a duplicated one still fails here
        fresh = catalog.namespace(table.namespace).table(table.name)
        files = fresh.files()
        assert_row_tiling(fresh, expected_rows=expected_rows, files=files)
        want_paths = Counter(submitted)
        got_paths = Counter(f.path for f in files)
        if got_paths != want_paths:
            lost = list((want_paths - got_paths).elements())
            extra = list((got_paths - want_paths).elements())
            duped = sorted(p for p, n in got_paths.items() if n > 1)
            check(
                False,
                f"file listing != submitted payloads: lost={lost[:4]} "
                f"unexpected={extra[:4]} duplicated={duped[:4]}",
            )
    return report


def add_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--depths",
        type=_comma_ints,
        default=[1, 4, 16, 64],
        help="comma list of in-flight commit depths (default: 1,4,16,64)",
    )
    p.add_argument(
        "--ops",
        type=_positive_int,
        default=400,
        help="measured commits per depth (default: 400; --warmup "
        "unmeasured commits run first, --duration caps the phase)",
    )
    p.add_argument(
        "--files-per-commit",
        type=_comma_ints,
        default=[1],
        help="comma list of fabricated files per commit (default: 1)",
    )
