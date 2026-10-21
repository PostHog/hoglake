"""Process entry point: config → stages → destination → flusher →
startup drain → consumer + flush loop, plus the ops HTTP server.

Startup order matters. The stages are opened and EVERYTHING already
staged is flushed (the startup drain: one sequential pass over each
stage's rows, stages concurrently) before the consumer is
assigned its partitions. Staged state
therefore never outlives a process, which is what lets group sizes and
ages live in memory only. ``/readyz`` answers 503 for the whole drain;
the deployment's startup probe must allow for it (README).

Besides the ``MILLRACE_*`` knobs (config.py) the process reads
``HOGLAKE_URL`` (required), ``HOGLAKE_S3_*`` for the parquet writes, and
the standard ``AWS_*`` environment SlateDB's ``s3://`` store resolves.

Exit codes: 0 clean stop; 1 any other failure; 2 invalid configuration
or an assignment refusal; 3 halted (the destination table was recreated,
or the server stopped reporting ``read_snapshot_id``); 4 fenced (a newer
writer owns a claimed partition's SlateDB path; do not restart onto the
same assignment). Fenced outranks halted outranks the rest.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from functools import partial
from typing import Any

from prometheus_client.core import REGISTRY, CounterMetricFamily, GaugeMetricFamily

from .config import Config, ConfigError, load_config
from .consumer import AssignmentError, MillraceConsumer, create_kafka_consumer
from .destination import DestinationError, ensure_destination
from .flush import STUCK_AFTER, FlushHalted, FlushRunner, HoglakeFlusher
from .grouping import GroupKeyer
from .server import serve
from .slatedb_metrics import MetricSnapshot, slatedb_families, snapshot_recorder
from .stage import FencedError, StageManager, build_slatedb_settings

log = logging.getLogger("millrace")

EXIT_ERROR = 1
EXIT_CONFIG = 2
EXIT_HALTED = 3
EXIT_FENCED = 4

_MICROS_PER_SECOND = 1_000_000


def _now_us() -> int:
    return time.time_ns() // 1000


def _probe_read[T](read: Callable[[], T], fallback: T) -> T:
    """Read pipeline state from the ops thread. A dict the loop mutates
    mid-iteration raises a bare RuntimeError; a retry always lands, and
    the fallback degrades one scrape rather than failing it."""
    for _ in range(3):
        try:
            return read()
        except RuntimeError:
            continue
    return fallback


def _exit_code_for(outcomes: Sequence[BaseException | None]) -> int:
    errors = [o for o in outcomes if isinstance(o, BaseException)]
    if any(isinstance(o, FencedError) for o in errors):
        return EXIT_FENCED
    if any(isinstance(o, FlushHalted) for o in errors):
        return EXIT_HALTED
    if any(isinstance(o, AssignmentError) for o in errors):
        return EXIT_CONFIG
    if errors:
        return EXIT_ERROR
    return 0


def liveness_status(*, terminal: BaseException | None) -> str | None:
    """/healthz: None while alive; the 503 reason once a terminal
    condition (halt or fence) has latched."""
    if terminal is None:
        return None
    if isinstance(terminal, FencedError):
        return f"fenced: {terminal}"
    return f"flush pipeline HALTED: {terminal}"


def readiness_status(*, started: bool, liveness: str | None) -> str | None:
    """/readyz: unready until the startup drain finished and the consumer
    is assigned, and whenever liveness fails."""
    if not started:
        return "starting: draining staged data before consuming"
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
    """Prometheus view of the pipeline. Everything is read from in-memory
    state on the scrape thread (no I/O, no loop hop)."""

    def __init__(
        self,
        consumer: MillraceConsumer,
        flusher: HoglakeFlusher,
        stages: StageManager,
        runner: FlushRunner,
        *,
        draining: Callable[[], bool],
    ) -> None:
        self._consumer = consumer
        self._flusher = flusher
        self._stages = stages
        self._runner = runner
        self._draining = draining

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
                "Flush decisions whose commits all landed",
                f.decisions_committed,
            ),
            (
                "millrace_flush_failures_total",
                "Flush decisions that failed (rows stay staged and retry)",
                f.decisions_failed,
            ),
            ("millrace_flush_commits_total", "Hoglake commits that landed", f.commits),
            ("millrace_flush_rows_published_total", "Rows published", f.rows_published),
            (
                "millrace_flush_files_written_total",
                "Parquet files committed",
                f.files_written,
            ),
            (
                "millrace_flush_bytes_written_total",
                "Parquet bytes committed",
                f.bytes_written,
            ),
            (
                "millrace_flush_orphaned_uploads_total",
                "Uploaded objects no commit names",
                f.orphaned_uploads,
            ),
            (
                "millrace_flush_groups_stuck_total",
                f"Times a group reached {STUCK_AFTER} consecutive flush failures",
                self._runner.stuck_total,
            ),
        ]
        for name, doc, value in counters:
            yield CounterMetricFamily(name, doc, value=value)
        dropped = CounterMetricFamily(
            "millrace_records_dropped_total",
            "Records that cannot be represented (consume or flush time), by reason",
            labels=["reason"],
        )
        by_reason: dict[str, int] = dict(c.records_dropped)
        for reason, count in f.records_dropped.items():
            by_reason[reason] = by_reason.get(reason, 0) + count
        for reason, count in sorted(by_reason.items()):
            dropped.add_metric([reason], count)
        yield dropped
        refused = CounterMetricFamily(
            "millrace_flush_commit_refusals_total",
            "Commits the server refused with a 4xx, by status",
            labels=["status"],
        )
        for status, count in sorted(f.commit_refusals.items()):
            refused.add_metric([status], count)
        yield refused
        clamped = CounterMetricFamily(
            "millrace_values_clamped_total",
            "Timestamps outside years 1-9999 clamped to the nearest representable "
            "instant at flush, by column path",
            labels=["column"],
        )
        for column, count in sorted(f.values_clamped.items()):
            clamped.add_metric([column], count)
        yield clamped
        yield CounterMetricFamily(
            "millrace_flush_ambiguous_uploads_total",
            "Objects of a commit whose retries ran out on ambiguous answers "
            "(the commit may have landed; never counted as orphans)",
            value=f.ambiguous_uploads,
        )
        yield GaugeMetricFamily(
            "millrace_flush_groups_stuck",
            f"Groups currently at or past {STUCK_AFTER} consecutive flush failures (alert)",
            value=_probe_read(lambda: self._runner.stuck_groups, 0),
        )
        yield CounterMetricFamily(
            "millrace_drain_rows_total",
            "Staged rows the startup drain flushed (published or dropped)",
            value=self._runner.drain_rows,
        )
        yield CounterMetricFamily(
            "millrace_drain_bytes_total",
            "Staged payload bytes the startup drain flushed",
            value=self._runner.drain_bytes,
        )
        yield GaugeMetricFamily(
            "millrace_draining",
            "1 while the startup drain runs (the pod is not ready)",
            value=1.0 if self._draining() else 0.0,
        )
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
        capacity = GaugeMetricFamily(
            "millrace_slatedb_cache_capacity_bytes",
            "Configured capacity of the pod's shared SlateDB SST cache, by half",
            labels=["half"],
        )
        block_bytes, meta_bytes = self._stages.cache_capacity_bytes
        capacity.add_metric(["block"], block_bytes)
        capacity.add_metric(["meta"], meta_bytes)
        yield capacity
        gauges = _probe_read(self._stages.gauges, None)
        if gauges is None:
            return
        yield GaugeMetricFamily(
            "millrace_staged_bytes",
            "Bytes staged since the last drain",
            value=gauges.staged_bytes,
        )
        yield GaugeMetricFamily(
            "millrace_staged_rows",
            "Rows staged since the last drain",
            value=gauges.staged_rows,
        )
        yield GaugeMetricFamily(
            "millrace_staged_groups",
            "Groups holding staged rows",
            value=gauges.staged_groups,
        )
        now = _now_us()
        if gauges.oldest_first_staged_ts is not None:
            yield GaugeMetricFamily(
                "millrace_oldest_staged_age_seconds",
                "Age of the oldest staged group",
                value=max(0, now - gauges.oldest_first_staged_ts) / _MICROS_PER_SECOND,
            )
        due = self._runner.oldest_due_staged_ts
        if due is not None:
            yield GaugeMetricFamily(
                "millrace_oldest_eligible_staged_age_seconds",
                "Age of the oldest group the flush found due at its last sweep "
                "(the alertable backlog signal; absent when nothing is due)",
                value=max(0, now - due) / _MICROS_PER_SECOND,
            )


async def _run(cfg: Config, hoglake_url: str, s3_kwargs: Mapping[str, Any]) -> int:
    """Assemble the pipeline and run it until a signal or a terminal
    condition."""
    from pyhoglake import HoglakeClient, S3Config

    client = HoglakeClient(hoglake_url, s3=S3Config(**s3_kwargs))
    stages = StageManager(
        cfg.stage_store_url,
        cfg.stage_base_path,
        settings=build_slatedb_settings(
            max_unflushed_bytes=cfg.slatedb.max_unflushed_bytes,
            l0_max_ssts=cfg.slatedb.l0_max_ssts,
        ),
        block_cache_bytes=cfg.slatedb.block_cache_bytes,
        meta_cache_bytes=cfg.slatedb.meta_cache_bytes,
    )
    flusher: HoglakeFlusher | None = None
    ops = None
    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, partial(ensure_destination, client, cfg))
        flusher = await loop.run_in_executor(
            None,
            partial(
                HoglakeFlusher.resolve,
                client,
                cfg,
                now_us=_now_us,
                io_workers=2 * cfg.flush_concurrency,
            ),
        )
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
            sweep_s=cfg.flush_sweep_s,
            concurrency=cfg.flush_concurrency,
            # One file per group in steady state: a batch fills one commit.
            batch_max_groups=cfg.max_files_per_commit,
        )
        started = asyncio.Event()
        draining = asyncio.Event()

        def _liveness() -> str | None:
            return liveness_status(terminal=runner.halted)

        ops = serve(
            "0.0.0.0",
            cfg.metrics_port,
            ready=lambda: readiness_status(
                started=started.is_set(), liveness=_liveness()
            ),
            live=_liveness,
        )
        REGISTRY.register(
            _PipelineCollector(
                consumer, flusher, stages, runner, draining=draining.is_set
            )
        )

        def stop_all() -> None:
            # One handler per signal: add_signal_handler replaces.
            consumer.stop()
            runner.stop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop_all)

        async def drain() -> None:
            draining.set()
            started_at = time.monotonic()
            try:
                await runner.drain()
            finally:
                draining.clear()
            if runner.stopping:
                log.info("stopped during the startup drain")
                return
            log.info("startup drain finished in %.1fs", time.monotonic() - started_at)

        try:
            await consumer.start(drain=drain)
        except BaseException as e:  # noqa: BLE001 - mapped to the exit code below
            outcomes: list[BaseException | None] = [e]
            await consumer.close()
        else:
            if runner.stopping:
                await consumer.close()
                return 0
            started.set()
            tasks = {
                asyncio.create_task(consumer.run(), name="millrace-consumer"),
                asyncio.create_task(runner.run(), name="millrace-flush"),
            }
            await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
            consumer.stop()
            runner.stop()
            outcomes = list(await asyncio.gather(*tasks, return_exceptions=True))
        for outcome in outcomes:
            if isinstance(outcome, FencedError):
                log.critical(
                    "millrace FENCED: %s; do NOT restart this pod onto the same "
                    "assignment; staged state is durable for the new owner",
                    outcome,
                )
            elif isinstance(outcome, FlushHalted):
                log.critical(
                    "millrace HALTED: %s; staged state is durable; a human must "
                    "decide before restarting",
                    outcome,
                )
            elif isinstance(outcome, AssignmentError):
                log.error(
                    "millrace refused to start: %s (nothing was claimed)", outcome
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
        print("HOGLAKE_URL is required and is not set", file=sys.stderr)
        return EXIT_CONFIG
    try:
        return asyncio.run(_run(cfg, hoglake_url, _s3_kwargs(env)))
    except KeyboardInterrupt:  # pragma: no cover - signal handlers own the stop
        return 0
    except ConfigError as e:
        # A destination the configuration cannot create (destination.py).
        print(str(e), file=sys.stderr)
        return EXIT_CONFIG
    except DestinationError as e:
        log.error("millrace startup failed: %s", e)
        return EXIT_ERROR
    except Exception:
        log.exception("millrace startup failed")
        return EXIT_ERROR


def main() -> None:
    """The console-script entry point."""
    raise SystemExit(_main(os.environ))


if __name__ == "__main__":
    main()
