"""Process entry point: wire config, stage, consumer, planner/flush, server.

One process = one topic → one table (docs/kafka-ingestion.md
§Deployment). Startup fails fast on invalid config or an unresolvable
destination before any consume begins.

Assembly: ``MILLRACE_*`` env → :class:`~millrace.config.Config` →
:class:`~millrace.stage.StageManager` (one SlateDB instance per claimed
partition, opened with the ``MILLRACE_SLATEDB_*`` settings — T9) +
:func:`~millrace.destination.ensure_destination` (creates the catalog,
namespace and table from config when missing) +
:class:`~millrace.flush.HoglakeFlusher` (resolves the
destination and pins its incarnation) + :class:`~millrace.consumer.
MillraceConsumer` and :class:`~millrace.flush.FlushRunner` sharing the
StageManager on one asyncio loop, plus the operational HTTP surface
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

Exit codes: 0 clean stop (SIGINT/SIGTERM); 2 invalid configuration
(:class:`ConfigError` — including a missing destination the config
cannot create, :mod:`millrace.destination` — or an
:class:`AssignmentError` at start — the replica count against the
topic's partitions, or a topic the metadata does not know: a topology
problem, logged in one line, nothing claimed); 3 the flush pipeline
HALTED loudly (:class:`FlushHalted` — incarnation
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
import threading
import time
from collections.abc import Callable, Collection, Mapping, Sequence
from functools import partial
from typing import Any

from prometheus_client.core import REGISTRY, CounterMetricFamily, GaugeMetricFamily

from . import memdebug
from .config import Config, ConfigError, load_config
from .consumer import (
    AssignmentError,
    ConsumerFencedError,
    MillraceConsumer,
    create_kafka_consumer,
)
from .destination import DestinationError, ensure_destination
from .flush import REPREPARE_REASONS, FlushHalted, FlushRunner, HoglakeFlusher
from .grouping import GroupKeyer
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

    FENCED outranks HALTED outranks an assignment refusal outranks a
    generic error: the fence is the most time-critical signal (a blind
    restart flaps against the new owner NOW), and a halt's
    reconciliation can wait for an operator either way. An
    :class:`AssignmentError` is a topology/config problem (the replica
    count against the topic), so it shares ``ConfigError``'s exit 2.
    Clean outcomes (None results, plain stops) are 0.
    """
    errors = [o for o in outcomes if isinstance(o, BaseException)]
    if any(isinstance(o, ConsumerFencedError) for o in errors):
        return EXIT_FENCED
    if any(isinstance(o, FlushHalted) for o in errors):
        return EXIT_HALTED
    if any(isinstance(o, AssignmentError) for o in errors):
        return EXIT_CONFIG
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
    pod's ASSIGNED partitions (the K of the pod's derived share of the
    topic — tens, not the partition-tuple count). ``millrace_flush_partition_
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
        gauge_max_staleness_s: float | None = None,
    ) -> None:
        self._consumer = consumer
        self._flusher = flusher
        self._stages = stages
        self._loop = loop
        self._runner = runner
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
                "millrace_prepared_replays_total",
                "Persisted prepared/ entries republished on a receipt 404",
                f.prepared_replays,
            ),
            (
                "millrace_receipt_settlements_total",
                "Recovered commits settled from the receipt",
                f.receipt_settlements,
            ),
            (
                "millrace_flush_deduped_total",
                "Fresh flush windows settled from an existing receipt",
                f.flush_deduped,
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
                "millrace_flush_unresolved_uploads_total",
                "Objects of a dropped prepared/ entry that may be live or orphaned",
                f.unresolved_uploads,
            ),
        ]
        for name, doc, value in counters:
            yield CounterMetricFamily(name, doc, value=value)
        dropped = CounterMetricFamily(
            "millrace_records_dropped_total",
            "Records dropped as malformed (consume or flush time), by reason",
            labels=["reason"],
        )
        by_reason: dict[str, int] = dict(c.records_dropped)
        for reason, count in f.records_dropped.items():
            by_reason[reason] = by_reason.get(reason, 0) + count
        for reason, count in sorted(by_reason.items()):
            dropped.add_metric([reason], count)
        yield dropped
        reprepared = CounterMetricFamily(
            "millrace_prepared_reprepared_total",
            "prepared/ entries dropped and re-prepared from the staged rows, "
            "by the refusal that forced it",
            labels=["reason"],
        )
        for reason in REPREPARE_REASONS:
            reprepared.add_metric([reason], f.reprepared.get(reason, 0))
        yield reprepared
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
        # The pod-level SST cache's CONFIGURED capacity (0/0 when
        # disabled): static, but on the same page as the access counter
        # it sizes. The binding exposes no occupancy — the hit ratio per
        # entry_kind on millrace_slatedb_block_cache_accesses_total is
        # the only behavioral signal.
        capacity = GaugeMetricFamily(
            "millrace_slatedb_cache_capacity_bytes",
            "Configured capacity of the pod's shared SlateDB SST cache, by "
            "split-cache half (0 when the cache is disabled)",
            labels=["half"],
        )
        block_bytes, meta_bytes = self._stages.cache_capacity_bytes
        capacity.add_metric(["block"], block_bytes)
        capacity.add_metric(["meta"], meta_bytes)
        yield capacity
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
        yield GaugeMetricFamily(
            "millrace_staged_groups",
            "Partition tuples (staging groups) currently holding staged rows, "
            "summed over the pod's partitions",
            value=gauges.staged_groups,
        )
        per_partition_bytes = GaugeMetricFamily(
            "millrace_partition_staged_bytes",
            "Bytes currently staged, per assigned partition (cardinality "
            "bounded by the pod's partition count, never by partition tuples)",
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
        cfg.stage_store_url,
        cfg.stage_base_path,
        settings=slatedb_settings,
        block_cache_bytes=cfg.slatedb.block_cache_bytes,
        meta_cache_bytes=cfg.slatedb.meta_cache_bytes,
    )
    flusher: HoglakeFlusher | None = None
    ops = None
    try:
        # Startup resolution is blocking I/O (fail-fast at boot); keep it
        # off the loop the consumer and flusher are about to share. The
        # destination is created from config first (destination.py:
        # catalog, namespace, table — each if missing, the replica race
        # tolerated; a missing object the config cannot create is a
        # ConfigError, exit 2), then the flusher resolves and pins it
        # exactly as before. The flusher's I/O pool is sized to the
        # sweep's process-wide in-flight bound (M5's bounded parallelism).
        await asyncio.get_running_loop().run_in_executor(
            None, partial(ensure_destination, client, cfg)
        )
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
        # The consumer stages each record under the destination's
        # partition tuple: its keyer is built from the shape the flusher
        # just resolved (same pinned incarnation) and re-read on the
        # flusher's TTL through its own Table handle.
        consumer = MillraceConsumer(
            create_kafka_consumer(cfg),
            stages,
            cfg,
            keyer=GroupKeyer(flusher.resolved_info),
            shape=flusher.shape_reader(),
            now_us=_now_us,
        )
        runner = FlushRunner(
            stages,
            flusher,
            cfg.planner_knobs(),
            now_us=_now_us,
            sweep_s=float(cfg.flush_sweep_s),
            concurrency=cfg.flush_concurrency,
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
                gauge_max_staleness_s=2.0 * cfg.flush_sweep_s,
            )
        )

        loop = asyncio.get_running_loop()

        if memdebug.interval_s(os.environ) > 0:

            def _memdebug_probes() -> dict[str, Any]:
                sizes = memdebug.container_sizes(
                    consumer=consumer,
                    flusher=flusher,
                    runner=runner,
                    stages=stages,
                    client=client,
                    table=getattr(flusher, "_table", None),
                    http=getattr(client, "_http", None),
                )
                pool = getattr(
                    getattr(getattr(client, "_http", None), "_transport", None),
                    "_pool",
                    None,
                )
                if pool is not None:
                    sizes["http.pool.connections"] = len(
                        getattr(pool, "_connections", ())
                    )
                for part in list(getattr(stages, "_stages", {}).values()):
                    for k, v in memdebug.container_sizes(partition=part).items():
                        sizes[k] = sizes.get(k, 0) + v
                sizes["threads"] = threading.active_count()
                sizes["asyncio.tasks"] = len(asyncio.all_tasks(loop))
                sizes["loop._ready"] = len(getattr(loop, "_ready", ()))
                sizes["loop._scheduled"] = len(getattr(loop, "_scheduled", ()))
                executor = getattr(flusher, "_executor", None)
                if executor is not None:
                    sizes["flusher.executor.queue"] = executor._work_queue.qsize()
                return {
                    "sizes": sizes,
                    "slatedb": memdebug.slatedb_totals(
                        list(stages.metrics_recorders().values())
                    ),
                }

            memdebug.start_reporter(os.environ, _memdebug_probes)

        def stop_all() -> None:
            # ONE handler per signal: add_signal_handler REPLACES any
            # previous handler for the same signal, so registering the two
            # stops separately would leave the consumer running forever.
            consumer.stop()
            runner.stop()

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
        tasks = {consumer_task, runner_task}
        await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        # A halt (or any task failure, or a clean consumer stop) ends the
        # process: stop both loops, then drain every task before leaving.
        consumer.stop()
        runner.stop()
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
            elif isinstance(outcome, AssignmentError):
                # A topology refusal, not a crash: one line, no traceback.
                log.error(
                    "millrace refused to start: %s — fix MILLRACE_REPLICA_COUNT "
                    "or the topic and redeploy (nothing was claimed)",
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
    memdebug.start_tracing(env)
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
    except ConfigError as e:
        # A destination the configuration cannot create (destination.py):
        # a config refusal naming the knob, not a crash.
        print(str(e), file=sys.stderr)
        return EXIT_CONFIG
    except DestinationError as e:
        # The server could not be asked (transport, 5xx, 403): one line,
        # no traceback — the cause is in the message, and a supervisor's
        # restart is the right next move.
        log.error("millrace startup failed: %s", e)
        return EXIT_ERROR
    except Exception:
        log.exception("millrace startup failed")
        return EXIT_ERROR


def main() -> None:
    """The console-script entry point (``millrace`` / ``python -m millrace.main``)."""
    raise SystemExit(_main(os.environ))


if __name__ == "__main__":
    main()
