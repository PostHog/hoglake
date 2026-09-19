"""``hoglake-bench stream``: a continuous, high-volume event stream.

Writes synthetic events into an hour-partitioned table, in the shape
millpond produces, until it is interrupted. It is a **task**, not a
scenario: like ``seed`` it is deliberately absent from ``SCENARIOS`` (an
``all`` run that launched a forever-loop would never finish) and it flags
no regressions — but it journals a results line and declares an IO mode
like everything else, because every byte it writes is real.

What it is for
--------------
Generating the pressure the maintenance loops exist to relieve, live,
against a stack you can watch. Hour granularity is the point: partitions
close within an hour (or within seconds under ``--hours-per-minute``), so
compaction and expiry always have settled cells to work on while the
stream keeps running.

The table shape
---------------
- Partition spec is ``hour(ts)`` ALONE. The production events table is
  not partitioned per tenant, so every team lands in the same hourly
  cell. With real-time timestamps a flush usually touches one open hour
  and therefore writes ONE file — two when it straddles the boundary.
- Sort order is ``team_id`` then ``ts``, declared on the table and
  honoured by every batch before it is written. With no team
  partitioning, sortedness is the only thing that gives a reader team
  locality, which makes this table a genuine test of whether compaction
  preserves clustering (hoglake#100): a sorted table should get MORE
  prunable each compaction round, and an adjacency/concat-style merge
  that ignored sort order would destroy that.
- The skew therefore shows up as WITHIN-file distribution, not as file
  count: most rows of every file belong to the whale, the tail scattered
  through.

Stopping it
-----------
SIGINT/SIGTERM is the designed exit, not an error path:

- the **first** signal lets the in-flight flush finish. An append is one
  atomic commit, so there is no such thing as a half-published flush;
  abandoning it mid-upload would only orphan parquet for cleanup to
  reclaim. The unflushed REMAINDER of the buffer is then discarded (and
  counted), so the table only ever contains policy-triggered flushes and
  a Ctrl-C cannot leave a runt file behind for the next run to trip over.
- the **second** signal exits immediately, with code 130.

A run that ends because it hit ``--max-events`` or ``--duration`` does
flush its remainder: that data was asked for.
"""

from __future__ import annotations

import argparse
import os
import random
import signal
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import httpx
import numpy as np
import pyarrow as pa
from pyhoglake import AlreadyExistsError, Catalog, Namespace, Table, ops
from pyhoglake.errors import (
    CommitConflictError,
    HoglakeError,
    IncarnationChangedError,
)
from pyhoglake.ops import AlterOp

from ..context import Bench
from ..runner import BenchAbort
from ..seed.budget import fmt_bytes
from ..seed.vocab import build_vocabulary
from ..stats import Metric, Recorder
from . import events as E
from . import pacing
from .distribution import (
    DEFAULT_TEAMS,
    TeamDistribution,
)

#: Every byte is real: parquet encoded by the client, uploaded to the
#: object store, stats from the writer's own footer.
IO_MODE = "end-to-end"

DEFAULT_CATALOG = "stream-events"
DEFAULT_NAMESPACE = "events"
DEFAULT_TABLE = "pageviews"

#: Millpond's own defaults, adopted deliberately (millpond/config.py:580-581:
#: FLUSH_SIZE 104857600 bytes of Arrow, FLUSH_INTERVAL_MS 60000).
DEFAULT_FLUSH_MB = 100.0
DEFAULT_FLUSH_SECONDS = 60.0

DEFAULT_RATE = 20_000.0
DEFAULT_PROGRESS_SECONDS = 10.0
DEFAULT_MAX_RETRIES = 8
DEFAULT_SEED = 4740871

#: Retry backoff for transient server failures.
RETRY_BASE_S = 0.25
RETRY_CAP_S = 15.0

#: The second Ctrl-C: the conventional 128 + SIGINT.
IMMEDIATE_EXIT_CODE = 130

#: Generation chunk aims at this much wall time per batch, so progress
#: and signals are noticed promptly without a chunk-per-event loop.
CHUNK_SECONDS = 0.25
MIN_CHUNK_ROWS = 1_000
MAX_CHUNK_ROWS = 250_000

#: Rows used to measure bytes-per-event for --rate-mb-s.
CALIBRATION_ROWS = 20_000


# -- signals -----------------------------------------------------------------


def _stderr(line: str) -> None:
    print(line, file=sys.stderr, flush=True)


class StopController:
    """SIGINT/SIGTERM handling for a run that is MEANT to be interrupted.

    First signal asks for a clean stop; a second one exits on the spot.
    ``emit`` and ``immediate`` are injected so the whole thing is
    testable without raising signals at a test runner.
    """

    def __init__(
        self,
        *,
        emit: Callable[[str], None] = _stderr,
        immediate: Callable[[int], None] | None = None,
    ) -> None:
        self.emit = emit
        self._immediate = immediate if immediate is not None else os._exit
        self.signals = 0
        self.first_signal: int | None = None
        self._previous: dict[int, Any] = {}

    @property
    def stopping(self) -> bool:
        return self.signals > 0

    @property
    def stopped_by(self) -> str:
        if self.first_signal is None:
            return ""
        return signal.Signals(self.first_signal).name

    def handle(self, signum: int, frame: Any = None) -> None:
        self.signals += 1
        if self.signals == 1:
            self.first_signal = signum
            self.emit(
                f"\n{signal.Signals(signum).name} received — finishing the "
                "in-flight flush, discarding the unflushed buffer, then "
                "summarising. Press again to exit immediately."
            )
        else:
            self.emit("second signal — exiting now")
            self._immediate(IMMEDIATE_EXIT_CODE)

    def install(self) -> None:
        for sig in (signal.SIGINT, signal.SIGTERM):
            self._previous[sig] = signal.signal(sig, self.handle)

    def restore(self) -> None:
        for sig, previous in self._previous.items():
            signal.signal(sig, previous)
        self._previous.clear()


# -- failures ----------------------------------------------------------------


def classify_failure(exc: BaseException) -> str:
    """``"retry"`` for transient server trouble, ``"fatal"`` otherwise.

    Transient: transport errors, every 5xx (503 admission backpressure
    included) and commit conflicts, which are the OCC retry contract.
    Permanent: 422 validation, a changed incarnation, and anything that
    is not a server answer at all (a harness bug).
    """
    if isinstance(exc, IncarnationChangedError):
        return "fatal"
    if isinstance(exc, CommitConflictError):
        return "retry"
    if isinstance(exc, HoglakeError):
        if exc.status_code is None or exc.status_code >= 500:
            return "retry"
        return "fatal"
    if isinstance(exc, httpx.HTTPError):
        return "retry"
    return "fatal"


def with_retries(
    call: Callable[[], Any],
    *,
    totals: StreamTotals,
    max_retries: int = DEFAULT_MAX_RETRIES,
    sleep: Callable[[float], None] = time.sleep,
    jitter: Callable[[], float] = random.random,
    should_stop: Callable[[], bool] | None = None,
) -> Any:
    """Run ``call``, retrying transient failures with capped exponential
    backoff. A permanent failure stops the run with a clear message; so
    does exhausting the retries."""
    delay = RETRY_BASE_S
    for attempt in range(max_retries + 1):
        try:
            return call()
        except BaseException as exc:
            if classify_failure(exc) == "fatal":
                totals.flush_failures += 1
                raise BenchAbort(
                    f"permanent write failure ({type(exc).__name__}: {exc}) — "
                    "this is a validation/contract problem, not congestion, "
                    "so retrying cannot help. Fix the table or the batch and "
                    "start the stream again."
                ) from exc
            totals.last_error = f"{type(exc).__name__}: {exc}"
            if attempt >= max_retries or (should_stop is not None and should_stop()):
                totals.flush_failures += 1
                raise BenchAbort(
                    f"gave up after {attempt + 1} attempts at one flush "
                    f"(last: {type(exc).__name__}: {exc}); the server is not "
                    "keeping up or is down"
                ) from exc
            totals.retries += 1
            sleep(delay + jitter() * delay * 0.25)
            delay = min(delay * 2.0, RETRY_CAP_S)
    raise AssertionError("unreachable")  # pragma: no cover


# -- accounting --------------------------------------------------------------


@dataclass
class StreamTotals:
    """Everything the progress line and the summary report."""

    team_count: int
    events: int = 0
    arrow_bytes: int = 0
    files: int = 0
    flushes: int = 0
    retries: int = 0
    flush_failures: int = 0
    dropped_events: int = 0
    hours_seen: set[int] = field(default_factory=set)
    reasons: dict[str, int] = field(default_factory=dict)
    last_error: str | None = None
    per_team: np.ndarray = field(init=False)

    def __post_init__(self) -> None:
        self.per_team = np.zeros(self.team_count, dtype=np.int64)

    def record_flush(
        self,
        *,
        reason: str,
        rows: int,
        arrow_bytes: int,
        files: int,
        team_counts: Sequence[int] | np.ndarray,
    ) -> None:
        self.events += rows
        self.arrow_bytes += arrow_bytes
        self.files += files
        self.flushes += 1
        self.reasons[reason] = self.reasons.get(reason, 0) + 1
        self.per_team += np.asarray(team_counts, dtype=np.int64)


def _rate(value: float, elapsed_s: float) -> float:
    return value / elapsed_s if elapsed_s > 0 else 0.0


def progress_line(
    totals: StreamTotals,
    *,
    elapsed_s: float,
    hour: str,
    buffered_rows: int,
    buffered_bytes: int,
) -> str:
    """One periodic line: key=value tokens, human-first but greppable."""
    return " ".join(
        (
            f"[{elapsed_s:8.1f}s]",
            f"events={totals.events:,}",
            f"events_s={_rate(totals.events, elapsed_s):,.0f}",
            f"arrow_mb_s={_rate(totals.arrow_bytes, elapsed_s) / 1e6:.2f}",
            f"flushes={totals.flushes}",
            f"files={totals.files}",
            f"hours={len(totals.hours_seen)}",
            f"hour={hour}",
            f"retries={totals.retries}",
            f"failures={totals.flush_failures}",
            f"buffered_rows={buffered_rows:,}",
            f"buffered_mb={buffered_bytes / 1e6:.1f}",
        )
    )


def summary_lines(
    totals: StreamTotals,
    teams: TeamDistribution,
    *,
    elapsed_s: float,
    stopped_by: str,
    parquet_bytes: int | None = None,
    target_rate: float | None = None,
    deferred_stats: bool = False,
    top: int = 10,
) -> list[str]:
    """The end-of-run report. Pure, so it is testable without a server."""
    target = f", target {target_rate:,.0f}/s" if target_rate else ""
    reasons = ", ".join(f"{k} {v}" for k, v in sorted(totals.reasons.items())) or "none"
    lines = [
        "",
        f"=== stream summary (stopped by {stopped_by}) ===",
        f"  elapsed              {elapsed_s:,.1f}s",
        (
            f"  events               {totals.events:,} "
            f"({_rate(totals.events, elapsed_s):,.0f}/s achieved{target})"
        ),
        (
            f"  arrow bytes          {fmt_bytes(totals.arrow_bytes)} "
            f"({_rate(totals.arrow_bytes, elapsed_s) / 1e6:.2f} MB/s)"
        ),
        f"  flushes              {totals.flushes} ({reasons})",
        (
            f"  files written        {totals.files}"
            + (
                " (stats deferred — left pending for the hydrator)"
                if deferred_stats
                else ""
            )
        ),
        f"  hour partitions      {len(totals.hours_seen)}",
        (
            f"  retries              {totals.retries} "
            f"(flush failures {totals.flush_failures})"
        ),
    ]
    if parquet_bytes is not None:
        ratio = totals.arrow_bytes / parquet_bytes if parquet_bytes else 0.0
        lines.append(
            f"  parquet bytes        {fmt_bytes(parquet_bytes)} "
            f"({ratio:.2f}x smaller than Arrow)"
        )
    if totals.dropped_events:
        lines.append(
            f"  dropped on stop      {totals.dropped_events:,} events "
            "(unflushed buffer, never committed)"
        )
    if totals.last_error:
        lines.append(f"  last transient error {totals.last_error}")
    order = np.argsort(-totals.per_team)[:top]
    lines.append(f"  top {min(top, len(order))} teams by events")
    for slot in order:
        count = int(totals.per_team[slot])
        if count == 0:
            continue
        share = count / totals.events if totals.events else 0.0
        lines.append(
            f"    team {teams.team_ids[int(slot)]:<10} {count:>14,}  "
            f"{share * 100:5.1f}%"
        )
    return lines


# -- DDL ---------------------------------------------------------------------


def sort_fields_op(field_ids: Sequence[int]) -> AlterOp:
    """``set_sort_order`` over the given leaf field ids, ascending.

    Built from :class:`pyhoglake.ops.AlterOp` directly: the client has
    ``set_partition_spec`` but no ``set_sort_order`` helper yet (a
    client gap, not a wire gap — the op is in the OpenAPI AlterOp enum).
    """
    return AlterOp(
        "set_sort_order",
        {
            "sort_fields": [
                {
                    "source_field_id": fid,
                    "direction": "asc",
                    "null_order": "nulls_last",
                }
                for fid in field_ids
            ]
        },
    )


def _ensure_catalog(bench: Bench, name: str) -> Catalog:
    data_path = f"s3://{bench.cfg.bucket}/stream/{name}/"
    try:
        catalog = bench.client.create_catalog(name, data_path)
        print(f"created catalog {name!r} at {data_path}", flush=True)
    except AlreadyExistsError:
        catalog = bench.client.catalog(name)
        print(
            f"streaming into existing catalog {name!r} at {catalog.data_path}",
            flush=True,
        )
    return catalog


def _ensure_namespace(catalog: Catalog, name: str) -> Namespace:
    try:
        return catalog.create_namespace(name)
    except AlreadyExistsError:
        return catalog.namespace(name)


def ensure_stream_table(namespace: Namespace, name: str) -> Table:
    """Create the hour-partitioned, sort-ordered table, or adopt an
    existing one whose layout already matches (so a restarted stream
    keeps filling the same table)."""
    try:
        table = namespace.create_table(name, E.STREAM_SCHEMA)
    except AlreadyExistsError:
        table = namespace.table(name)
        _check_layout(table, f"{namespace.name}.{name}")
        return table
    field_id = {c.name: c.field_id for c in table.columns}
    table.alter(
        [
            ops.set_partition_spec(
                [
                    ops.partition_field(
                        field_id[E.PARTITION_COLUMN], E.PARTITION_TRANSFORM
                    )
                ]
            ),
            sort_fields_op([field_id[c] for c in E.SORT_COLUMNS]),
        ]
    )
    return table


def _check_layout(table: Table, label: str) -> None:
    info = table.info()
    live = {c.name for c in info.columns}
    if live != set(E.STREAM_SCHEMA.names):
        raise BenchAbort(
            f"{label} already exists with a different schema (live columns "
            f"{sorted(live)}); stream into a fresh --catalog/--table instead"
        )
    field_id = {c.name: c.field_id for c in info.columns}
    spec = info.partition_spec
    want_partition = [(field_id[E.PARTITION_COLUMN], E.PARTITION_TRANSFORM)]
    got_partition = (
        [(f.source_field_id, f.transform) for f in spec.fields] if spec else []
    )
    if got_partition != want_partition:
        raise BenchAbort(
            f"{label} is partitioned {got_partition!r}, not "
            f"{want_partition!r} — stream into a fresh table rather than "
            "mixing partition specs in one table"
        )
    sort = info.sort_spec
    want_sort = [field_id[c] for c in E.SORT_COLUMNS]
    got_sort = [f.source_field_id for f in sort.fields] if sort else []
    if got_sort != want_sort:
        raise BenchAbort(f"{label} declares sort order {got_sort!r}, not {want_sort!r}")


# -- the run -----------------------------------------------------------------


def _chunk_rows(rate_per_s: float) -> int:
    if rate_per_s <= 0:
        return 50_000
    return int(min(MAX_CHUNK_ROWS, max(MIN_CHUNK_ROWS, rate_per_s * CHUNK_SECONDS)))


def run(bench: Bench, args: argparse.Namespace) -> None:
    teams = TeamDistribution.build(teams=args.teams)
    speedup = (
        pacing.speedup_for_hours_per_minute(args.hours_per_minute)
        if args.hours_per_minute
        else 1.0
    )
    rng = np.random.default_rng(args.seed)
    vocab = build_vocabulary(args.seed)
    policy = pacing.FlushPolicy(
        max_bytes=int(args.flush_mb * 1e6), max_seconds=args.flush_seconds
    )

    bench.ensure_bucket()
    catalog = _ensure_catalog(bench, args.catalog)
    namespace = _ensure_namespace(catalog, args.namespace)
    table = ensure_stream_table(namespace, args.table)
    base = table.info()

    rate = args.rate
    if args.rate_mb_s:
        probe, _ = E.make_batch(
            np.random.default_rng(args.seed),
            vocab,
            CALIBRATION_ROWS,
            teams=teams,
            lo_us=0,
            hi_us=3_600_000_000,
            properties_bytes=args.properties_bytes,
        )
        per_event = max(1.0, probe.nbytes / CALIBRATION_ROWS)
        rate = args.rate_mb_s * 1e6 / per_event
        print(
            f"--rate-mb-s {args.rate_mb_s} at {per_event:.0f} Arrow bytes/event "
            f"-> {rate:,.0f} events/s",
            flush=True,
        )

    clock = pacing.StreamClock(
        int(datetime.now(UTC).timestamp() * pacing.MICROS_PER_SECOND),
        speedup=speedup,
    )
    limiter = pacing.RateLimiter(rate)
    totals = StreamTotals(team_count=len(teams.team_ids))
    stop = StopController()
    chunk = _chunk_rows(rate)

    print(
        f"streaming into {args.namespace}.{args.table} "
        f"[partition hour({E.PARTITION_COLUMN}), sort "
        f"{', '.join(E.SORT_COLUMNS)}]\n"
        f"  rate={'unlimited' if rate <= 0 else f'{rate:,.0f} events/s'}  "
        f"flush={args.flush_mb:g}MB Arrow or {args.flush_seconds:g}s  "
        f"teams={len(teams.team_ids):,} (busiest {teams.whale_id} at "
        f"{teams.whale_share * 100:.1f}%)  "
        f"event-time x{speedup:g}\n"
        "  Ctrl-C to stop (again to exit immediately)",
        flush=True,
    )

    pending: list[Any] = []
    pending_rows = 0
    pending_bytes = 0
    pending_counts = np.zeros(len(teams.team_ids), dtype=np.int64)
    pending_since = time.monotonic()
    started = time.monotonic()
    deadline = started + args.duration if args.duration else None
    last_progress = started
    latencies: list[int] = []
    last_hour = pacing.hour_label(clock.now_us())

    def flush(reason: str) -> None:
        nonlocal pending, pending_rows, pending_bytes, pending_counts
        nonlocal pending_since
        batch = E.sort_batch(
            pending[0] if len(pending) == 1 else pa.concat_tables(pending)
        )
        t0 = time.perf_counter_ns()
        result = with_retries(
            lambda: table.append(
                batch,
                author="hoglake-bench",
                message=f"stream flush {totals.flushes + 1} ({reason})",
                # With stats deferred the commit carries no column
                # statistics, so every file lands `pending` and the
                # hydrator has to fetch and parse the footer it would
                # otherwise have been handed.
                deferred_stats=args.defer_stats,
            ),
            totals=totals,
            max_retries=args.max_flush_retries,
            should_stop=lambda: stop.signals > 1,
        )
        latencies.append(time.perf_counter_ns() - t0)
        for appended in result.files:
            if appended.partition_values:
                totals.hours_seen.add(appended.partition_values[0])
        totals.record_flush(
            reason=reason,
            rows=batch.num_rows,
            arrow_bytes=pending_bytes,
            files=len(result.files),
            team_counts=pending_counts,
        )
        pending = []
        pending_rows = 0
        pending_bytes = 0
        pending_counts = np.zeros(len(teams.team_ids), dtype=np.int64)
        pending_since = time.monotonic()

    stop.install()
    try:
        while not stop.stopping:
            if args.max_events and totals.events + pending_rows >= args.max_events:
                break
            if deadline is not None and time.monotonic() >= deadline:
                break
            rows = chunk
            if args.max_events:
                rows = min(rows, args.max_events - totals.events - pending_rows)
            if rows <= 0:
                break
            limiter.acquire(rows, should_stop=lambda: stop.stopping)
            if stop.stopping:
                break
            lo_us, hi_us = clock.advance_window()
            batch, idx = E.make_batch(
                rng,
                vocab,
                rows,
                teams=teams,
                lo_us=lo_us,
                hi_us=hi_us,
                properties_bytes=args.properties_bytes,
            )
            pending.append(batch)
            pending_rows += rows
            pending_bytes += batch.nbytes
            pending_counts += np.bincount(idx, minlength=len(teams.team_ids))
            last_hour = pacing.hour_label(hi_us)

            reason = policy.trigger(
                pending_bytes=pending_bytes,
                pending_rows=pending_rows,
                age_s=time.monotonic() - pending_since,
            )
            if reason:
                flush(reason)

            now = time.monotonic()
            if now - last_progress >= args.progress_seconds:
                print(
                    progress_line(
                        totals,
                        elapsed_s=now - started,
                        hour=last_hour,
                        buffered_rows=pending_rows,
                        buffered_bytes=pending_bytes,
                    ),
                    flush=True,
                )
                last_progress = now

        if pending_rows:
            if stop.stopping:
                # Documented: the in-flight flush (if any) already
                # completed above; the remainder is dropped rather than
                # written as a runt file nobody's policy asked for.
                totals.dropped_events = pending_rows
            else:
                flush("final")
    finally:
        stop.restore()

    elapsed = time.monotonic() - started
    after = table.info()
    parquet_bytes = after.file_size_bytes - base.file_size_bytes
    stopped_by = stop.stopped_by or "reaching --max-events/--duration"
    for line in summary_lines(
        totals,
        teams,
        elapsed_s=elapsed,
        stopped_by=stopped_by,
        parquet_bytes=parquet_bytes,
        target_rate=rate if rate > 0 else None,
        deferred_stats=args.defer_stats,
    ):
        print(line, flush=True)

    written_rows = after.record_count - base.record_count
    if written_rows != totals.events:
        raise BenchAbort(
            f"the server accounts {written_rows:,} new rows but the stream "
            f"committed {totals.events:,} — the numbers disagree, which "
            "means something else is writing to this table"
        )

    metric = Metric.from_recorder(
        "stream.flush",
        _recorder(latencies),
        elapsed,
        events=totals.events,
        events_s=_rate(totals.events, elapsed),
        arrow_mb_s=_rate(totals.arrow_bytes, elapsed) / 1e6,
        files=totals.files,
        hour_partitions=len(totals.hours_seen),
        parquet_bytes=parquet_bytes,
        retries=totals.retries,
        flush_failures=totals.flush_failures,
        dropped_events=totals.dropped_events,
        whale_share_observed=(
            float(totals.per_team[0] / totals.events) if totals.events else 0.0
        ),
        stopped_by=stopped_by,
    )
    bench.append_result(
        "stream",
        _journal_params(args, rate=rate, speedup=speedup),
        [metric],
        status="ok",
        config={"url": bench.cfg.url, "profile": None},
        io_mode=IO_MODE,
    )


def _recorder(latencies: list[int]) -> Recorder:
    recorder = Recorder()
    for value in latencies:
        recorder.record_ns(value)
    return recorder


def _journal_params(
    args: argparse.Namespace, *, rate: float, speedup: float
) -> dict[str, Any]:
    return {
        "catalog": args.catalog,
        "namespace": args.namespace,
        "table": args.table,
        "rate": rate,
        "rate_mb_s": args.rate_mb_s,
        "flush_mb": args.flush_mb,
        "flush_seconds": args.flush_seconds,
        "teams": args.teams,
        "properties_bytes": args.properties_bytes,
        "hours_per_minute": args.hours_per_minute,
        "event_time_speedup": speedup,
        "seed": args.seed,
        "max_events": args.max_events,
        "duration": args.duration,
        "url": args.url,
    }


# -- CLI ---------------------------------------------------------------------


def add_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--catalog",
        default=DEFAULT_CATALOG,
        help=f"catalog to create or stream into (default {DEFAULT_CATALOG})",
    )
    p.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    p.add_argument("--table", default=DEFAULT_TABLE)
    rate = p.add_mutually_exclusive_group()
    rate.add_argument(
        "--rate",
        type=float,
        default=DEFAULT_RATE,
        help=f"target events/second; 0 = unlimited (default {DEFAULT_RATE:g})",
    )
    rate.add_argument(
        "--rate-mb-s",
        type=float,
        default=None,
        help="target Arrow MB/second instead of an event rate; converted "
        "to events/s from a measured bytes-per-event probe",
    )
    p.add_argument(
        "--flush-mb",
        type=float,
        default=DEFAULT_FLUSH_MB,
        help="flush after this many MB of buffered ARROW bytes "
        f"(default {DEFAULT_FLUSH_MB:g}, millpond's FLUSH_SIZE); the "
        "parquet that lands is smaller, by how much depends on the "
        "payload — the summary reports the measured ratio",
    )
    p.add_argument(
        "--flush-seconds",
        type=float,
        default=DEFAULT_FLUSH_SECONDS,
        help="flush after this long, whichever comes first "
        f"(default {DEFAULT_FLUSH_SECONDS:g}, millpond's FLUSH_INTERVAL_MS)",
    )
    p.add_argument(
        "--teams",
        type=int,
        default=DEFAULT_TEAMS,
        help=f"how many teams the stream covers (default {DEFAULT_TEAMS})",
    )
    p.add_argument(
        "--defer-stats",
        action="store_true",
        help="register files WITHOUT footer stats, leaving them pending for "
        "the hydrator to backfill — the only way to put the hydrator under "
        "real load, since every writer in the fleet ships its own footer",
    )
    p.add_argument(
        "--properties-bytes",
        type=int,
        default=E.DEFAULT_PROPERTIES_BYTES,
        help="size of the per-row properties text blob "
        f"(default {E.DEFAULT_PROPERTIES_BYTES})",
    )
    p.add_argument(
        "--hours-per-minute",
        type=float,
        default=None,
        help="compress event time: simulate this many hours per real "
        "minute, so hour partitions roll over without waiting "
        "(default: real time)",
    )
    p.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="stop after this many events (0 = until interrupted)",
    )
    p.add_argument(
        "--progress-seconds",
        type=float,
        default=DEFAULT_PROGRESS_SECONDS,
        help=f"progress line cadence (default {DEFAULT_PROGRESS_SECONDS:g})",
    )
    p.add_argument(
        "--max-flush-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help=f"retries per flush before giving up (default {DEFAULT_MAX_RETRIES})",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help="RNG/vocabulary seed — fixed by default",
    )
    p.add_argument(
        "--server",
        dest="url",
        default=argparse.SUPPRESS,
        help="alias for --url (the hoglake server)",
    )
