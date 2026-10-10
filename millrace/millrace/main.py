"""Process entry point: wire config, stage, consumer, planner/flush, server.

One process = one topic → one table (docs/kafka-ingestion.md
§Deployment). Startup fails fast on config or identity mismatch before
any consume begins.

Assembly: ``MILLRACE_*`` env → :class:`~millrace.config.Config` →
:class:`~millrace.stage.StageManager` (one SlateDB instance per claimed
partition, opened with the ``MILLRACE_SLATEDB_*`` settings — T9) +
:class:`~millrace.flush.HoglakeFlusher` (resolves the
destination and pins its incarnation) + :class:`~millrace.consumer.
MillraceConsumer` and :class:`~millrace.flush.FlushRunner` sharing the
StageManager on one asyncio loop, plus the poison sweeper (retention for
the ``poison/`` quarantine) and the operational HTTP surface
(:mod:`millrace.server`).

Beyond the ``MILLRACE_*`` knobs (config.py), the process reads:

- ``HOGLAKE_URL`` (required) — the hoglake server base URL;
- ``HOGLAKE_S3_ENDPOINT`` / ``HOGLAKE_S3_ACCESS_KEY`` /
  ``HOGLAKE_S3_SECRET_KEY`` / ``HOGLAKE_S3_REGION`` — the parquet write
  path's object store (pyhoglake's S3Config; absent values leave the
  ambient AWS chain);
- the standard ``AWS_*`` environment (``AWS_ENDPOINT_URL``,
  ``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``, ``AWS_REGION``,
  ``AWS_ALLOW_HTTP``) — this is what SlateDB's ``ObjectStore.resolve``
  reads for an ``s3://`` ``MILLRACE_STAGE_URL``; millrace neither maps
  nor overrides it.

Exit codes: 0 clean stop (SIGINT/SIGTERM); 2 invalid configuration;
3 the flush pipeline HALTED loudly (:class:`FlushHalted` — incarnation
guard, expiry floor, idempotency anomaly); 4 the consumer was FENCED off
a claimed partition's SlateDB path by a newer writer
(:class:`ConsumerFencedError` — a blind restart re-opens the path and
fences the NEW owner, so a supervisor must not restart this pod onto the
same assignment; README §Rollouts and fencing); 1 any other failure. A
halt or fence leaves staged state durable for an operator and is never
silent.

Probes (server.py): ``/healthz`` (liveness) 503s when the pipeline is
in a latched terminal state — the flush runner halted, the consumer
fenced, or a partition fenced out of flush scheduling — and
``/readyz`` 503s until the consumer has started and whenever liveness
fails (a pod that lost a partition's ownership is not ready to serve
it). Both decisions are pure functions (:func:`liveness_status` /
:func:`readiness_status`, millpond's ``_liveness_status`` pattern) over
latched state, so neither can flap.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import time
from collections.abc import Awaitable, Callable, Collection, Mapping, Sequence
from functools import partial
from typing import Any

from prometheus_client.core import REGISTRY, CounterMetricFamily, GaugeMetricFamily

from .config import Config, ConfigError, load_config
from .consumer import ConsumerFencedError, MillraceConsumer, create_kafka_consumer
from .flush import FlushHalted, FlushRunner, HoglakeFlusher
from .server import serve
from .slatedb_metrics import MetricSnapshot, slatedb_families, snapshot_recorder
from .stage import StageManager, build_slatedb_settings

log = logging.getLogger("millrace")

EXIT_ERROR = 1
EXIT_CONFIG = 2
EXIT_HALTED = 3
EXIT_FENCED = 4

_MICROS_PER_SECOND = 1_000_000


def _now_us() -> int:
    """The wall clock in microseconds (the production ``now_us``)."""
    return time.time_ns() // 1000


def _probe_read[T](read: Callable[[], T], fallback: T) -> T:
    """Read loop-thread state from the ops thread, surviving the GIL
    race: ``fenced_partitions`` / the stage map are mutated by the
    pipeline loop while the probe iterates them, which CPython answers
    with a bare ``RuntimeError`` (set/dict changed size). The window is
    bytecodes wide, so a couple of retries always land; the fallback (a
    DEGRADED read — the metric family or the fence detail is absent for
    one scrape) exists so a pathological churn storm degrades the probe
    rather than 500ing it. Probes stay honest either way: a missed
    observation is not a false state."""
    for _ in range(3):
        try:
            return read()
        except RuntimeError:
            continue
    log.debug("probe read degraded after retries")
    return fallback


def _exit_code_for(outcomes: Sequence[BaseException | None]) -> int:
    """The process exit code for the pipeline's task outcomes — pure, so
    the map is unit-testable without running the pipeline.

    FENCED outranks HALTED outranks a generic error: the fence is the
    most time-critical signal (a blind restart flaps against the new
    owner NOW), and a halt's reconciliation can wait for an operator
    either way. Clean outcomes (None results, plain stops) are 0.
    """
    errors = [o for o in outcomes if isinstance(o, BaseException)]
    if any(isinstance(o, ConsumerFencedError) for o in errors):
        return EXIT_FENCED
    if any(isinstance(o, FlushHalted) for o in errors):
        return EXIT_HALTED
    if errors:
        return EXIT_ERROR
    return 0


def liveness_status(
    *,
    halted: BaseException | None,
    consumer_fenced: bool,
    fenced_partitions: Collection[tuple[str, int]],
) -> str | None:
    """The /healthz decision as a pure function (millpond's
    ``_liveness_status`` pattern). None = alive; a string = the 503
    reason.

    Only LATCHED terminal states feed it — a halt never un-halts, a
    fence mark holds until the assignment revokes the partition — so the
    probe 503s on a real stall and can never flap. A runner-level fence
    (a partition fenced out of flush scheduling) means this pod lost the
    path's ownership: on a busy partition the consumer's next write
    surfaces it too (→ exit 4); on a QUIET one nothing else ever says
    it, and this pod must not pass for healthy while a newer writer owns
    its staged state.
    """
    if halted is not None:
        return f"flush pipeline HALTED: {halted}"
    if consumer_fenced:
        return (
            "consumer fenced off a partition's SlateDB path by a newer writer "
            "(do not restart onto the same assignment)"
        )
    if fenced_partitions:
        names = ", ".join(f"{t}[{p}]" for t, p in sorted(fenced_partitions))
        return f"partitions fenced out of flush scheduling: {names}"
    return None


def readiness_status(*, started: bool, liveness: str | None) -> str | None:
    """The /readyz decision: the pipeline is ready once the consumer has
    started, and unready whenever liveness fails — a halted or fenced
    pipeline (a pod that lost a partition's ownership) is not ready to
    serve, however long it has been running."""
    if not started:
        return "the consumer has not started"
    return liveness


def _s3_kwargs(env: Mapping[str, str]) -> dict[str, Any]:
    """HOGLAKE_S3_* env → pyhoglake S3Config kwargs (absent = ambient)."""
    out: dict[str, Any] = {}
    if v := env.get("HOGLAKE_S3_ACCESS_KEY", "").strip():
        out["access_key"] = v
    if v := env.get("HOGLAKE_S3_SECRET_KEY", "").strip():
        out["secret_key"] = v
    if v := env.get("HOGLAKE_S3_ENDPOINT", "").strip():
        out["endpoint_override"] = v
    if v := env.get("HOGLAKE_S3_REGION", "").strip():
        out["region"] = v
    return out


class PoisonSweeper:
    """The ``poison/`` retention loop (S14): quarantine entries are
    forensic state with a bounded lifetime
    (``MILLRACE_POISON_RETENTION_S``); without this sweeper the prefix
    only ever grows.

    One sweep per ``sweep_s``: for every currently open partition,
    :meth:`millrace.stage.PartitionStage.purge_poison_expired` with the
    cutoff ``now − retention`` — bounded per call, per-partition
    contained (a fenced/revoked partition is skipped, a broken one is
    logged and counted, neither stops the sweep). The loop body never
    raises: retention is hygiene, not correctness, and a dying sweeper
    must not take the pipeline with it. It is also SAFE to be down at
    expiry — nothing depends on poison presence, the next sweep catches
    up — so it runs as a side task off the consume/flush path.
    """

    def __init__(
        self,
        stages: StageManager,
        *,
        retention_s: int,
        sweep_s: int,
        now_us: Callable[[], int] = _now_us,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        if retention_s < 1:
            raise ValueError(f"retention_s must be >= 1, got {retention_s}")
        if sweep_s < 1:
            raise ValueError(f"sweep_s must be >= 1, got {sweep_s}")
        self._stages = stages
        self._retention_us = retention_s * _MICROS_PER_SECOND
        self._sweep_s = float(sweep_s)
        self._now_us = now_us
        self._stop_event = asyncio.Event()
        self._sleep = sleep if sleep is not None else self._interruptible_sleep
        self._stopping = False
        self.purged_total = 0
        self.sweeps_total = 0
        self.partition_failures_total = 0

    async def _interruptible_sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        except TimeoutError:
            pass

    async def sweep_once(self) -> int:
        """One pass over the open partitions; returns entries purged."""
        cutoff = self._now_us() - self._retention_us
        purged = 0
        for topic, partition in self._stages.partitions():
            try:
                stage = self._stages.stage(topic, partition)
            except KeyError:
                continue  # revoked mid-sweep
            try:
                report = await stage.purge_poison_expired(cutoff)
            except Exception:
                # Contained: SlateDB Error.Closed (a fence/revoke under
                # us), a store hiccup — the partition is retried next
                # sweep; hygiene never wedges the pipeline.
                self.partition_failures_total += 1
                log.exception(
                    "poison sweep failed for %s[%d]; retried next sweep",
                    topic,
                    partition,
                )
                continue
            purged += report.deleted
            if report.deleted or report.truncated or report.unreadable:
                log.info(
                    "poison sweep %s[%d]: deleted=%d scanned=%d unreadable=%d "
                    "truncated=%s",
                    topic,
                    partition,
                    report.deleted,
                    report.scanned,
                    report.unreadable,
                    report.truncated,
                )
        self.purged_total += purged
        self.sweeps_total += 1
        return purged

    async def run(self) -> None:
        """Sweep, pause, repeat until :meth:`stop`. A failed ITERATION is
        logged and counted and the loop continues."""
        while not self._stopping:
            try:
                await self.sweep_once()
            except Exception:
                log.exception("poison sweep iteration failed; continuing")
            await self._sleep(self._sweep_s)

    def stop(self) -> None:
        """Stop after the in-flight sweep; wakes the between-sweep pause."""
        self._stopping = True
        self._stop_event.set()


class _PipelineCollector:
    """Prometheus view of the pipeline counters and staging gauges.

    Passive observability (docs/kafka-ingestion.md §Deployment): nothing
    here feeds control flow — the consumer NEVER pauses, so the gauges
    are alert inputs, not latch drivers (SlateDB on object storage is
    the unbounded buffer; an alert on them is the entire feature).
    Counter values come from the loops' own stats snapshots; the
    staging gauges come from the StageManager's stats-derived gauges —
    the SAME snapshot the flush planner decides from, so the exposition
    can never drift from the decisions (M6: the sweep's published fold
    is served while fresh — ``gauge_max_staleness_s`` — and a live scan
    is the fallback). The gauge read hops back to the pipeline's loop
    (``run_coroutine_threadsafe``); a busy loop degrades the gauges to
    absent rather than blocking the probe.

    The alertable backlog signal is
    ``millrace_oldest_eligible_staged_age_seconds``: the age of the
    oldest staged key the flush policy found ELIGIBLE at the last sweep
    (size, age or slow lane — computed by the FlushRunner with the
    planner's own knobs and ratio, so a tiny key waiting out its slow
    lane does not read as backlog). Absent when nothing is eligible —
    and absent on a live fallback scan, which has no policy input: a
    halted sweep drops the series rather than freeze it, and the halt
    is the louder signal (/healthz). The raw per-partition oldest stays
    on ``millrace_partition_oldest_staged_age_seconds`` for forensics;
    it is NOT the alertable one.

    The SlateDB families (``millrace.slatedb_metrics``) come from the
    per-instance recorders' atomic snapshots — read directly, no loop
    hop — aggregated across the pod's partition instances. A recorder
    that fails degrades to its families absent, never a scrape failure.

    The ``millrace_partition_*`` families are the deliberate exception
    to the no-per-partition-series rule: a stuck partition must be
    FINDABLE (PR #331), and the label cardinality is bounded by the
    pod's ASSIGNED partitions (the K of the static list or the group
    assignment — tens, not the team count). ``millrace_flush_partition_
    fenced`` carries the flush runner's fenced set the same way.
    """

    def __init__(
        self,
        consumer: MillraceConsumer,
        flusher: HoglakeFlusher,
        stages: StageManager,
        loop: asyncio.AbstractEventLoop,
        *,
        runner: FlushRunner | None = None,
        sweeper: PoisonSweeper | None = None,
        gauge_max_staleness_s: float | None = None,
    ) -> None:
        self._consumer = consumer
        self._flusher = flusher
        self._stages = stages
        self._loop = loop
        self._runner = runner
        self._sweeper = sweeper
        self._gauge_max_staleness_s = gauge_max_staleness_s

    def collect(self) -> Any:
        c = self._consumer.stats()
        f = self._flusher.stats()
        counters = [
            (
                "millrace_messages_consumed_total",
                "Kafka messages consumed",
                c.messages_consumed,
            ),
            (
                "millrace_records_staged_total",
                "Records durably staged",
                c.records_staged,
            ),
            (
                "millrace_records_poisoned_total",
                "Records quarantined at consume",
                c.records_poisoned,
            ),
            (
                "millrace_kafka_commits_total",
                "Kafka offset commit attempts",
                c.commit_attempts,
            ),
            (
                "millrace_flushes_committed_total",
                "Flush decisions committed",
                f.flushes_committed,
            ),
            (
                "millrace_flushes_replayed_total",
                "Persisted commits replayed",
                f.flushes_replayed,
            ),
            (
                "millrace_receipt_settlements_total",
                "Recovered commits settled from the receipt",
                f.receipt_settlements,
            ),
            (
                "millrace_flush_reprepares_total",
                "Commits re-prepared after ddl_since_read_snapshot",
                f.reprepares,
            ),
            (
                "millrace_flush_rows_published_total",
                "Rows published to the lake",
                f.rows_published,
            ),
            (
                "millrace_flush_files_written_total",
                "Parquet files committed",
                f.files_written,
            ),
            (
                "millrace_flush_orphaned_uploads_total",
                "Uploaded objects abandoned without a commit",
                f.orphaned_uploads,
            ),
            (
                "millrace_flush_records_quarantined_total",
                "Rows quarantined at flush",
                f.records_quarantined,
            ),
        ]
        for name, doc, value in counters:
            yield CounterMetricFamily(name, doc, value=value)
        if self._sweeper is not None:
            yield CounterMetricFamily(
                "millrace_poison_entries_purged_total",
                "Quarantined (poison) entries deleted by the retention sweeper",
                value=self._sweeper.purged_total,
            )
        if self._runner is not None:
            runner = self._runner  # narrowing survives the lambda below
            fenced = GaugeMetricFamily(
                "millrace_flush_partition_fenced",
                "Whether this partition is fenced OUT of flush scheduling "
                "(its SlateDB path surfaced Error.Closed — a newer writer "
                "owns it; the mark holds until the assignment revokes it)",
                labels=["topic", "partition"],
            )
            fenced_keys: tuple[tuple[str, int], ...] = _probe_read(
                lambda: runner.fenced_partitions, ()
            )
            for topic, partition in fenced_keys:
                fenced.add_metric([topic, str(partition)], 1.0)
            yield fenced
        # SlateDB internals, aggregated across the pod's partition
        # instances. The recorder snapshots are in-process atomics — no
        # loop hop, no blocking; a recorder that fails drops its
        # families, never the scrape.
        snapshots: list[list[MetricSnapshot]] = []
        recorders: list[Any] = _probe_read(
            lambda: list(self._stages.metrics_recorders().values()), []
        )
        for recorder in recorders:
            try:
                snapshots.append(snapshot_recorder(recorder))
            except Exception:
                log.debug("a SlateDB metrics recorder snapshot failed", exc_info=True)
        yield from slatedb_families(snapshots)
        try:
            gauges = asyncio.run_coroutine_threadsafe(
                self._stages.gauges(max_staleness_s=self._gauge_max_staleness_s),
                self._loop,
            ).result(timeout=2)
        except Exception:  # noqa: BLE001 - a busy loop degrades the gauges, never the probe
            return
        yield GaugeMetricFamily(
            "millrace_staged_bytes", "Bytes currently staged", value=gauges.staged_bytes
        )
        yield GaugeMetricFamily(
            "millrace_staged_rows", "Rows currently staged", value=gauges.staged_rows
        )
        per_partition_bytes = GaugeMetricFamily(
            "millrace_partition_staged_bytes",
            "Bytes currently staged, per assigned partition (cardinality "
            "bounded by the pod's partition count, never by teams)",
            labels=["topic", "partition"],
        )
        per_partition_age = GaugeMetricFamily(
            "millrace_partition_oldest_staged_age_seconds",
            "Age of the oldest staged byte, per assigned partition "
            "(absent for a partition with nothing staged)",
            labels=["topic", "partition"],
        )
        for (topic, partition), per in sorted(gauges.per_partition.items()):
            per_partition_bytes.add_metric(
                [topic, str(partition)], float(per.staged_bytes)
            )
            if per.oldest_first_staged_ts is not None:
                age_s = (
                    max(0, _now_us() - per.oldest_first_staged_ts) / _MICROS_PER_SECOND
                )
                per_partition_age.add_metric([topic, str(partition)], age_s)
        yield per_partition_bytes
        yield per_partition_age
        if gauges.oldest_eligible_staged_ts is not None:
            age_s = (
                max(0, _now_us() - gauges.oldest_eligible_staged_ts)
                / _MICROS_PER_SECOND
            )
            yield GaugeMetricFamily(
                "millrace_oldest_eligible_staged_age_seconds",
                "Age of the oldest staged key the flush policy found "
                "ELIGIBLE (size/age/slow lane) at the last sweep — the "
                "alertable backlog signal. Absent when nothing is "
                "eligible; absent on a live fallback scan (a halted "
                "sweep drops the series rather than freeze it — the "
                "halt is the louder signal). The raw oldest stays on "
                "millrace_partition_oldest_staged_age_seconds.",
                value=age_s,
            )


async def _run(cfg: Config, hoglake_url: str, s3_kwargs: Mapping[str, Any]) -> int:
    """Assemble the pipeline and run both loops until a signal or a halt."""
    from pyhoglake import HoglakeClient, S3Config

    client = HoglakeClient(hoglake_url, s3=S3Config(**s3_kwargs))
    slatedb_settings = build_slatedb_settings(
        gc_enabled=cfg.slatedb.gc_enabled,
        max_unflushed_bytes=cfg.slatedb.max_unflushed_bytes,
        l0_max_ssts=cfg.slatedb.l0_max_ssts,
    )
    if not cfg.slatedb.gc_enabled:
        # The loud posture for the GC-off default (config.py's knob
        # table): the writer sheds its embedded GC loop because the
        # external maintenance service owns collection. This process
        # cannot observe the service's existence — the tripwires are
        # this line and the service's own sweep metrics.
        log.warning(
            "writer-internal SlateDB GC is DISABLED "
            "(MILLRACE_SLATEDB_GC_ENABLED=false, the default): this "
            "assumes the millrace maintenance service is deployed beside "
            "this fleet and sweeping — without it, staging garbage is "
            "never collected. Set MILLRACE_SLATEDB_GC_ENABLED=true to "
            "run embedded GC instead (safe under overlap either way)."
        )
    stages = StageManager(
        cfg.stage_store_url, cfg.stage_base_path, settings=slatedb_settings
    )
    flusher: HoglakeFlusher | None = None
    ops = None
    try:
        # Startup resolution is blocking I/O (fail-fast at boot); keep it
        # off the loop the consumer and flusher are about to share. The
        # flusher's I/O pool is sized to the sweep's process-wide
        # in-flight bound (M5's bounded parallelism).
        flusher = await asyncio.get_running_loop().run_in_executor(
            None,
            partial(
                HoglakeFlusher.resolve,
                client,
                cfg,
                now_us=_now_us,
                io_workers=4 * cfg.flush_concurrency,
            ),
        )
        consumer = MillraceConsumer(
            create_kafka_consumer(cfg), stages, cfg, now_us=_now_us
        )
        runner = FlushRunner(
            stages,
            flusher,
            cfg.planner_knobs(),
            now_us=_now_us,
            sweep_s=float(cfg.flush_sweep_s),
            concurrency=cfg.flush_concurrency,
        )
        sweeper = PoisonSweeper(
            stages,
            retention_s=cfg.poison.retention_s,
            sweep_s=cfg.poison.sweep_s,
        )
        started = asyncio.Event()
        consumer_fenced = asyncio.Event()

        def _liveness() -> str | None:
            fenced: tuple[tuple[str, int], ...] = _probe_read(
                lambda: runner.fenced_partitions, ()
            )
            return liveness_status(
                halted=runner.halted,
                consumer_fenced=consumer_fenced.is_set(),
                fenced_partitions=fenced,
            )

        ops = serve(
            "0.0.0.0",
            cfg.metrics_port,
            ready=lambda: readiness_status(
                started=started.is_set(),
                liveness=_liveness(),
            ),
            live=_liveness,
        )
        REGISTRY.register(
            _PipelineCollector(
                consumer,
                flusher,
                stages,
                asyncio.get_running_loop(),
                runner=runner,
                sweeper=sweeper,
                gauge_max_staleness_s=2.0 * cfg.flush_sweep_s,
            )
        )

        loop = asyncio.get_running_loop()

        def stop_all() -> None:
            # ONE handler per signal: add_signal_handler REPLACES any
            # previous handler for the same signal, so registering the two
            # stops separately would leave the consumer running forever.
            consumer.stop()
            runner.stop()
            sweeper.stop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop_all)

        async def consume_loop() -> None:
            try:
                await consumer.start()
                started.set()
                await consumer.run()  # start() is idempotent
            except ConsumerFencedError:
                # Latch it BEFORE the raise unwinds us: /healthz 503s
                # during the drain, not only after the process dies.
                consumer_fenced.set()
                raise

        consumer_task = asyncio.create_task(consume_loop(), name="millrace-consumer")
        runner_task = asyncio.create_task(runner.run(), name="millrace-flush")
        sweeper_task = asyncio.create_task(sweeper.run(), name="millrace-poison-sweep")
        tasks = {consumer_task, runner_task, sweeper_task}
        await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        # A halt (or any task failure, or a clean consumer stop) ends the
        # process: stop both loops, then drain every task before leaving.
        consumer.stop()
        runner.stop()
        sweeper.stop()
        outcomes = await asyncio.gather(*tasks, return_exceptions=True)
        for outcome in outcomes:
            if isinstance(outcome, ConsumerFencedError):
                log.critical(
                    "millrace FENCED: %s — a newer writer owns the "
                    "partition's SlateDB path; do NOT restart this pod "
                    "onto the same assignment (README §Rollouts and "
                    "fencing); staged state is durable for the owner",
                    outcome,
                )
            elif isinstance(outcome, FlushHalted):
                log.critical(
                    "millrace HALTED: %s — staged state is durable and the "
                    "pipeline does not recover on its own; fix the cause and "
                    "restart",
                    outcome,
                )
            elif isinstance(outcome, BaseException):
                log.error("millrace pipeline failed: %r", outcome, exc_info=outcome)
        return _exit_code_for(outcomes)
    finally:
        if ops is not None:
            ops.close()
        await stages.close()
        if flusher is not None:
            flusher.close()
        client.close()


def _main(env: Mapping[str, str]) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )
    try:
        cfg = load_config(env)
    except ConfigError as e:
        print(str(e), file=sys.stderr)
        return EXIT_CONFIG
    hoglake_url = env.get("HOGLAKE_URL", "").strip()
    if not hoglake_url:
        print(
            "HOGLAKE_URL is required and is not set (the hoglake server "
            "base URL, e.g. http://hoglake:8080)",
            file=sys.stderr,
        )
        return EXIT_CONFIG
    try:
        return asyncio.run(_run(cfg, hoglake_url, _s3_kwargs(env)))
    except KeyboardInterrupt:  # pragma: no cover - signal handlers own the stop
        return 0
    except Exception:
        log.exception("millrace startup failed")
        return EXIT_ERROR


def main() -> None:
    """The console-script entry point (``millrace`` / ``python -m millrace.main``)."""
    raise SystemExit(_main(os.environ))


if __name__ == "__main__":
    main()
