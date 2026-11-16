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
``HOGLAKE_URL`` (required), ``HOGLAKE_AUTH_*`` for the credential it
presents to hoglake (:func:`hoglake_auth`), ``HOGLAKE_S3_*`` for the
parquet writes, and the standard ``AWS_*`` environment SlateDB's ``s3://``
store resolves.

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
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from functools import partial
from typing import Any

import httpx
from prometheus_client.core import REGISTRY, CounterMetricFamily, GaugeMetricFamily
from pyhoglake import _variant_backend

from .config import Config, ConfigError, load_config, variant_codec
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

#: The labels pyhoglake counts the Rust codec's fallbacks and ``auto``'s
#: routes to Python under, exported at 0 from the first scrape so that
#: increase() counts a pod's first (transitional, as the codecs are).
_ENCODE_REASONS = ("mismatch", "type", "panic", "shadow_mismatch")
_DECODE_REASONS = ("mismatch", "panic")
_ROUTED = (
    *(("encode", why) for why in ("unavailable", "plan", "not_utf8")),
    *(("decode", why) for why in ("unavailable", "storage")),
)


def _codec_counts() -> tuple[
    dict[tuple[str], int],
    dict[tuple[str], int],
    dict[tuple[str, str], int],
    Counter[str],
]:
    """pyhoglake's VARIANT codec counts since import, per process, under
    its lock, so that an encode whose build raises later counts too: encode
    and decode fallbacks by reason, calls ``auto`` sent to Python by op and
    why, and what the shadow compared (``calls``, ``rows``)."""
    backend = _variant_backend
    with backend._COUNTING:
        encode = {(reason,): n for reason, n in backend.ENCODE_FALLBACKS.items()}
        decode = {(reason,): n for reason, n in backend.DECODE_FALLBACKS.items()}
        routed = dict(backend.ROUTED)
        shadowed = Counter(backend.SHADOWED)
    return encode, decode, routed, shadowed


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
        # Labelled with the destination: other writers commit on the same
        # catalog, so alerts on millrace's progress read this, not the
        # server's per-catalog commit counter. `table` is `namespace.table`:
        # the scrape honors exported labels, so a `namespace` label would
        # replace the pod's Kubernetes namespace.
        commits = CounterMetricFamily(
            "millrace_flush_commits_total",
            "Hoglake commits that landed, by destination catalog and table",
            labels=["catalog", "table"],
        )
        commits.add_metric(list(self._flusher.destination), f.commits)
        yield commits
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
        built = CounterMetricFamily(
            "millrace_flush_build_seconds_total",
            "Seconds the flush's group builds that returned a plan spent, by stage",
            labels=["stage"],
        )
        for stage, seconds in f.build_seconds.items():
            built.add_metric([stage], seconds)
        yield built
        encode, decode, routed, shadowed = _codec_counts()
        variant_families = (
            (
                "millrace_variant_rows_total",
                "Mapped variant column rows per build: encoded, sql_null, invalid",
                ["column", "outcome"],
                f.variant_rows,
            ),
            (
                "millrace_variant_encode_seconds_total",
                (
                    "Wall seconds of mapped variant columns' encode calls, less "
                    "PYHOGLAKE_VARIANT_SHADOW's, by column and the process's codec "
                    "(backend: native or python; calls auto gives Python and "
                    "fallbacks count under it), per build that returned a plan"
                ),
                ["column", "backend"],
                f.variant_encode_seconds,
            ),
            (
                "millrace_variant_invalid_total",
                "Mapped variant values written as SQL NULL as invalid, by reason",
                ["column", "reason"],
                f.variant_invalid,
            ),
            (
                "millrace_variant_values_total",
                "Mapped variant values per declared path: typed, fallback, variant_null",
                ["column", "path", "outcome"],
                f.variant_values,
            ),
            (
                "millrace_variant_codec_fallback_total",
                (
                    "Variant encodes Python redid for the Rust codec, by reason, "
                    "per process (transitional)"
                ),
                ["reason"],
                dict.fromkeys([(r,) for r in _ENCODE_REASONS], 0) | encode,
            ),
            (
                "millrace_variant_codec_decode_fallback_total",
                (
                    "Variant storage reads (verify, structure_fault) the Python codec "
                    "decided where the Rust codec disagreed or panicked, by reason, "
                    "per process (transitional)"
                ),
                ["reason"],
                dict.fromkeys([(r,) for r in _DECODE_REASONS], 0) | decode,
            ),
            (
                "millrace_variant_codec_routed_to_python_total",
                (
                    "Variant encodes and storage reads (verify, structure_fault) "
                    "PYHOGLAKE_VARIANT_CODEC=auto gave the Python codec, by op and "
                    "reason, per process (transitional)"
                ),
                ["op", "reason"],
                dict.fromkeys(_ROUTED, 0) | routed,
            ),
        )
        for name, doc, labels, counts in variant_families:
            family = CounterMetricFamily(name, doc, labels=labels)
            for key, total in sorted(counts.items()):
                family.add_metric(list(key), total)
            yield family
        yield CounterMetricFamily(
            "millrace_variant_codec_recursion_bound_rows_total",
            "Invalid variant rows past a JSON parser's recursion bound, whose "
            "verdict is the Python codec's, per build that returned a plan "
            "(transitional)",
            value=f.variant_recursion_bound_rows,
        )
        yield CounterMetricFamily(
            "millrace_variant_shadow_seconds_total",
            "Seconds PYHOGLAKE_VARIANT_SHADOW spent encoding variant columns "
            "in Python again, within the variant_encode stage, per build that "
            "returned a plan (transitional)",
            value=f.variant_shadow_seconds,
        )
        yield CounterMetricFamily(
            "millrace_variant_shadow_rows_total",
            "Variant rows PYHOGLAKE_VARIANT_SHADOW encoded in Python again and "
            "compared, per process (transitional)",
            value=shadowed["rows"],
        )
        yield CounterMetricFamily(
            "millrace_variant_shadow_calls_total",
            "Variant encodes PYHOGLAKE_VARIANT_SHADOW compared, per process; "
            "fallback reason shadow_mismatch counts the ones that differed "
            "(transitional)",
            value=shadowed["calls"],
        )
        codec = GaugeMetricFamily(
            "millrace_variant_codec_info",
            "The codec encoding variant columns: native (Rust) or python "
            "(PYHOGLAKE_VARIANT_CODEC; transitional)",
            labels=["backend"],
        )
        codec.add_metric([variant_codec()], 1)
        yield codec
        yield CounterMetricFamily(
            "millrace_flush_build_queue_seconds_total",
            "Seconds group builds waited for a millrace-hoglake pool thread",
            value=f.build_queue_seconds,
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


def hoglake_auth(env: Mapping[str, str]) -> httpx.Auth | None:
    """The credential millrace presents to hoglake, from ``HOGLAKE_AUTH_*``
    (hoglake's docs/auth.md, "Clients"): OAuth client credentials against
    the token endpoint that issues hoglake tokens, minted and refreshed by
    pyhoglake (``HOGLAKE_AUTH_TOKEN_URL``, ``HOGLAKE_AUTH_CLIENT_ID``,
    ``HOGLAKE_AUTH_CLIENT_SECRET``, and ``HOGLAKE_AUTH_AUDIENCE``, default
    ``hoglake``); or a fixed token (``HOGLAKE_AUTH_TOKEN``, or the contents
    of ``HOGLAKE_AUTH_TOKEN_FILE``, read once at startup) for a standalone
    server whose operator minted one with ``hoglake-server token mint``.

    Exactly one source, or none: none sends no ``Authorization`` header,
    which is what an ``HOGLAKE_AUTH_MODE=off`` server expects and what
    millrace always did. Two sources is a configuration error rather than
    a precedence rule, so a leftover variable can never silently pick the
    credential.
    """
    from pyhoglake.auth import BearerToken, ClientCredentials

    def read(name: str) -> str:
        return env.get(name, "").strip()

    token_url, client_id, client_secret = (
        read("HOGLAKE_AUTH_TOKEN_URL"),
        read("HOGLAKE_AUTH_CLIENT_ID"),
        read("HOGLAKE_AUTH_CLIENT_SECRET"),
    )
    token, token_file = read("HOGLAKE_AUTH_TOKEN"), read("HOGLAKE_AUTH_TOKEN_FILE")
    sources = [
        name
        for name, present in (
            (
                "HOGLAKE_AUTH_TOKEN_URL/CLIENT_ID/CLIENT_SECRET",
                bool(token_url or client_id or client_secret),
            ),
            ("HOGLAKE_AUTH_TOKEN", bool(token)),
            ("HOGLAKE_AUTH_TOKEN_FILE", bool(token_file)),
        )
        if present
    ]
    if len(sources) > 1:
        raise ConfigError(
            [
                (
                    "hoglake credential: set exactly one of client credentials "
                    "(HOGLAKE_AUTH_TOKEN_URL + HOGLAKE_AUTH_CLIENT_ID + "
                    "HOGLAKE_AUTH_CLIENT_SECRET), HOGLAKE_AUTH_TOKEN or "
                    f"HOGLAKE_AUTH_TOKEN_FILE; got {' and '.join(sources)}"
                )
            ]
        )
    if not sources:
        return None
    if token:
        return BearerToken(token)
    if token_file:
        try:
            with open(token_file, encoding="utf-8") as f:
                contents = f.read().strip()
        except OSError as e:
            raise ConfigError([f"HOGLAKE_AUTH_TOKEN_FILE {token_file!r}: {e}"]) from e
        if not contents:
            raise ConfigError([f"HOGLAKE_AUTH_TOKEN_FILE {token_file!r} is empty"])
        return BearerToken(contents)
    missing = [
        name
        for name, value in (
            ("HOGLAKE_AUTH_TOKEN_URL", token_url),
            ("HOGLAKE_AUTH_CLIENT_ID", client_id),
            ("HOGLAKE_AUTH_CLIENT_SECRET", client_secret),
        )
        if not value
    ]
    if missing:
        raise ConfigError(
            [
                (
                    "hoglake client credentials need HOGLAKE_AUTH_TOKEN_URL, "
                    "HOGLAKE_AUTH_CLIENT_ID and HOGLAKE_AUTH_CLIENT_SECRET together; "
                    f"missing {', '.join(missing)}"
                )
            ]
        )
    audience = read("HOGLAKE_AUTH_AUDIENCE") or "hoglake"
    return ClientCredentials(token_url, client_id, client_secret, audience=audience)


async def _run(
    cfg: Config,
    hoglake_url: str,
    s3_kwargs: Mapping[str, Any],
    auth: httpx.Auth | None = None,
) -> int:
    """Assemble the pipeline and run it until a signal or a terminal
    condition."""
    from pyhoglake import HoglakeClient, S3Config

    client = HoglakeClient(hoglake_url, s3=S3Config(**s3_kwargs), auth=auth)
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
        auth = hoglake_auth(env)
    except ConfigError as e:
        print(str(e), file=sys.stderr)
        return EXIT_CONFIG
    try:
        return asyncio.run(_run(cfg, hoglake_url, _s3_kwargs(env), auth))
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
