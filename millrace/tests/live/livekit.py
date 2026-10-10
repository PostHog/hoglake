"""Shared helpers for the live suite (tests/live/) — not a test module
(the name does not match ``test_*``).

The kit every scenario builds through (flushkit/kafkakit pattern): the
stack endpoints from the harness's env, the events-like destination
schema, the seeded skew producer, lake readback (metadata AND parquet),
the in-process pipeline harness, the subprocess child (kill tests) and
the commit-path fault-injection proxy (lost-response tests).

Stack env (exported by ci/live-millrace.sh; defaults match it so a
hand-started stack works the same): ``HOGLAKE_URL``,
``KAFKA_BOOTSTRAP_SERVERS``, ``HOGLAKE_S3_ENDPOINT`` /
``HOGLAKE_S3_ACCESS_KEY`` / ``HOGLAKE_S3_SECRET_KEY``. SlateDB's s3
stage store resolves from the standard ``AWS_*`` environment; the
session fixture maps the harness's ``HOGLAKE_S3_*`` onto it.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import socket
import subprocess
import sys
import threading
import time
import uuid
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from functools import partial
from typing import Any, Self

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import ConsumerGroupTopicPartitions, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from pyhoglake import HoglakeClient, S3Config, ops

from millrace.config import (
    AssignmentMode,
    AutoOffsetReset,
    Config,
    EventTimePolicy,
    PoisonConfig,
    TeamKeyCodec,
)
from millrace.consumer import MillraceConsumer, create_kafka_consumer
from millrace.flush import FlushRunner, HoglakeFlusher
from millrace.stage import StageManager

# ---------------------------------------------------------------------------
# Stack endpoints (the harness's env; defaults mirror ci/live-millrace.sh)
# ---------------------------------------------------------------------------

HOGLAKE_URL = os.environ.get("HOGLAKE_URL", "http://localhost:28080").rstrip("/")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:29092")
S3_ENDPOINT = os.environ.get("HOGLAKE_S3_ENDPOINT", "http://localhost:29000")
S3_ACCESS_KEY = os.environ.get("HOGLAKE_S3_ACCESS_KEY", "hoglake")
S3_SECRET_KEY = os.environ.get("HOGLAKE_S3_SECRET_KEY", "hoglake123")
S3_REGION = os.environ.get("HOGLAKE_S3_REGION", "us-east-1")

RUN_ID = uuid.uuid4().hex[:8]
"""Per-run disambiguator for catalogs, topics, groups and stage prefixes."""

BUCKET = "millrace-itest"
"""The one bucket: lake data under ``lake/``, SlateDB staging under ``stage/``."""

MICROS_PER_SECOND = 1_000_000


def now_us() -> int:
    """Wall clock in microseconds — the production clock for live tests."""
    return time.time_ns() // 1000


def apply_slatedb_aws_env(env: dict[str, str]) -> dict[str, str]:
    """Map the harness's MinIO settings onto the standard AWS_* variables
    SlateDB's ``ObjectStore.resolve("s3://…")`` reads. Never overrides a
    variable already set."""
    mapped = dict(env)
    mapped.setdefault("AWS_ENDPOINT_URL", S3_ENDPOINT)
    mapped.setdefault("AWS_ACCESS_KEY_ID", S3_ACCESS_KEY)
    mapped.setdefault("AWS_SECRET_ACCESS_KEY", S3_SECRET_KEY)
    mapped.setdefault("AWS_REGION", S3_REGION)
    # MinIO is plain HTTP here; the object_store crate refuses it otherwise.
    mapped.setdefault("AWS_ALLOW_HTTP", "true")
    return mapped


# ---------------------------------------------------------------------------
# The events-like destination
# ---------------------------------------------------------------------------


def events_schema() -> pa.Schema:
    """team_id (identity partition source) + timestamp (month source) + a
    few scalars, per docs/kafka-ingestion-plan.md Phase 5."""
    return pa.schema(
        [
            pa.field("team_id", pa.int64(), nullable=False),
            pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("seq", pa.int64(), nullable=True),
            pa.field("event", pa.string(), nullable=True),
            pa.field("value", pa.float64(), nullable=True),
        ]
    )


def create_events_table(namespace: Any, name: str) -> Any:
    """Create the table and set the live partition spec
    ``identity(team_id), month(timestamp)`` (spec fields bind field ids,
    so the spec alter follows the create read-back)."""
    table = namespace.create_table(name, events_schema())
    field_ids = {c.name: c.field_id for c in table.columns}
    table.alter(
        [
            ops.set_partition_spec(
                [
                    ops.partition_field(field_ids["team_id"], "identity"),
                    ops.partition_field(field_ids["timestamp"], "month"),
                ]
            )
        ]
    )
    return table


def _pad_fill(n: int, team_id: int, ts_us: int, seq: int) -> str:
    """``n`` pad characters that parquet/snappy cannot compress away:
    chained sha256 hex of the row's coordinates — deterministic per
    (team, ts, seq), distinct per row, and free of the repeats a
    dictionary/LZ pass would eat. A padded row's staged bytes survive
    into the parquet file ~1:1, which is what the whale scenario's
    size-lane arithmetic stands on (a compressible pad lands an observed
    compression ratio far below the planner's 0.2 fallback after the
    first flush, and the size threshold moves mid-scenario)."""
    import hashlib

    out: list[str] = []
    left = n
    i = 0
    while left > 0:
        out.append(
            hashlib.sha256(
                f"millrace-pad:{team_id}:{ts_us}:{seq}:{i}".encode()
            ).hexdigest()
        )
        left -= 64
        i += 1
    return "".join(out)[:n]


def event_payload(
    team_id: int,
    ts_us: int,
    seq: int,
    *,
    event: str = "pageview",
    value: float = 1.0,
    pad_bytes: int = 0,
    extra: dict[str, Any] | None = None,
) -> bytes:
    """One JSON event payload (the v1 decoder's input). ``timestamp`` is
    epoch micros; ``pad_bytes`` grows the ``event`` string to size the
    row (the fill is incompressible BY DESIGN — :func:`_pad_fill`);
    ``extra`` adds raw keys (e.g. a column added by a mid-test alter)."""
    body: dict[str, Any] = {
        "team_id": team_id,
        "timestamp": ts_us,
        "seq": seq,
        "event": event + _pad_fill(pad_bytes, team_id, ts_us, seq),
        "value": value,
    }
    if extra:
        body.update(extra)
    return json.dumps(body, separators=(",", ":")).encode()


# ---------------------------------------------------------------------------
# Seeded skew
# ---------------------------------------------------------------------------


class Skew:
    """A deterministic Zipf-ish tenant distribution (seeded, so a run
    reproduces exactly): tenant ranks 1..``tenants``, weight ∝ 1/rank."""

    def __init__(self, tenants: int, *, seed: int) -> None:
        self.teams = list(range(1, tenants + 1))
        self.weights = [1.0 / rank for rank in self.teams]
        self._rng = random.Random(seed)

    def sample(self, n: int) -> list[int]:
        return self._rng.choices(self.teams, weights=self.weights, k=n)


@dataclass(frozen=True, slots=True)
class ProducedEvent:
    """One produced Kafka record's identity, for exact reconciliation."""

    seq: int
    team_id: int
    ts_us: int
    partition: int
    offset: int
    produced_monotonic: float


class EventProducer:
    """Keyed JSON events to the live broker, with delivery-tracked
    coordinates. Not thread-safe; one producer per scenario."""

    def __init__(self, topic: str) -> None:
        self.topic = topic
        self._producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
        self.produced: list[ProducedEvent] = []
        self._pending: dict[int, tuple[int, int, float]] = {}
        self._lock = threading.Lock()
        self._errors: list[Any] = []

    def send(
        self,
        team_id: int,
        ts_us: int,
        seq: int,
        *,
        pad_bytes: int = 0,
        record_ts_us: int | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        """Queue one event; the Kafka record timestamp tracks the event
        time (ms) so row keys order like the payloads. ``record_ts_us``
        overrides the record timestamp (junk event_time scenarios keep
        the RECORD timestamp sane — the broker rejects records too far
        out — while the PAYLOAD carries the junk date). ``extra`` adds
        raw payload keys (a column added mid-test)."""
        with self._lock:
            self._pending[seq] = (team_id, ts_us, time.monotonic())

        def on_delivery(err: Any, msg: Any, *, _seq: int = seq) -> None:
            with self._lock:
                team, ts, mono = self._pending.pop(_seq)
                if err is not None:
                    self._errors.append((_seq, err))
                    return
                self.produced.append(
                    ProducedEvent(
                        seq=_seq,
                        team_id=team,
                        ts_us=ts,
                        partition=msg.partition(),
                        offset=msg.offset(),
                        produced_monotonic=mono,
                    )
                )

        self._producer.produce(
            self.topic,
            key=str(team_id).encode(),
            value=event_payload(team_id, ts_us, seq, pad_bytes=pad_bytes, extra=extra),
            timestamp=(ts_us if record_ts_us is None else record_ts_us) // 1000,
            on_delivery=on_delivery,
        )
        self._producer.poll(0)

    def flush(self, timeout_s: float = 60.0) -> list[ProducedEvent]:
        """Deliver everything queued; returns the produced events sorted
        by seq. A delivery error is a test failure, not a skip."""
        self._producer.flush(timeout_s)
        if self._errors:
            raise AssertionError(f"kafka delivery failed: {self._errors[:3]}")
        with self._lock:
            if self._pending:
                raise AssertionError(f"{len(self._pending)} events never delivered")
            return sorted(self.produced, key=lambda e: e.seq)

    def close(self) -> None:
        self._producer.flush(30.0)


def produce_batch(
    producer: EventProducer,
    assignments: Iterable[tuple[int, int]],
    *,
    seq_start: int = 0,
    pad_bytes: int = 0,
) -> list[ProducedEvent]:
    """Produce (team_id, ts_us) pairs, seqs from ``seq_start``."""
    seq = seq_start
    for team_id, ts_us in assignments:
        producer.send(team_id, ts_us, seq, pad_bytes=pad_bytes)
        seq += 1
    return producer.flush()


# ---------------------------------------------------------------------------
# Kafka admin
# ---------------------------------------------------------------------------


def kafka_admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})


def create_topic(admin: AdminClient, name: str, *, partitions: int = 4) -> None:
    futures = admin.create_topics(
        [NewTopic(name, num_partitions=partitions, replication_factor=1)]
    )
    futures[name].result(timeout=60)


def delete_topic(admin: AdminClient, name: str) -> None:
    futures = admin.delete_topics([name])
    futures[name].result(timeout=60)


def committed_offsets(admin: AdminClient, group: str, topic: str) -> dict[int, int]:
    """The consumer group's committed offset per partition (-1001 =
    no committed offset, librdkafka's OFFSET_INVALID)."""
    request = [ConsumerGroupTopicPartitions(group)]
    result = admin.list_consumer_group_offsets(request)
    partitions = result[group].result(timeout=30).topic_partitions
    return {
        tp.partition: tp.offset
        for tp in partitions
        if tp.topic == topic and tp.error is None
    }


def end_offsets(
    admin: AdminClient, topic: str, partitions: Iterable[int]
) -> dict[int, int]:
    """The log end offset per partition (the count of events ever produced
    to a fresh topic)."""
    from confluent_kafka import Consumer

    probe = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": f"millrace-probe-{RUN_ID}-{uuid.uuid4().hex[:6]}",
            "enable.auto.commit": False,
        }
    )
    try:
        return {
            p: probe.get_watermark_offsets(TopicPartition(topic, p), timeout=30)[1]
            for p in partitions
        }
    finally:
        probe.close()


# ---------------------------------------------------------------------------
# Lake readback
# ---------------------------------------------------------------------------


def head_snapshot(catalog: Any) -> int:
    return int(catalog.refresh().head_snapshot_id)


def lake_files(table: Any, snapshot: int) -> list[Any]:
    return table.files(snapshot=snapshot)


def lake_team_counts(table: Any, snapshot: int) -> Counter[int]:
    """Per-tenant row counts from the manifest alone: one file is one
    (team_id, month) partition tuple, so partition_values[0] IS the team."""
    counts: Counter[int] = Counter()
    for f in lake_files(table, snapshot):
        assert f.partition_values is not None
        counts[int(f.partition_values[0])] += f.record_count
    return counts


def lake_rows(table: Any, snapshot: int, fs: Any) -> pa.Table:
    """Full parquet readback of every live file (the queryable truth)."""
    tables = []
    for f in lake_files(table, snapshot):
        key = f.path[len("s3://") :]
        tables.append(pq.read_table(key, filesystem=fs))
    if not tables:
        return pa.table(
            {name: pa.array([], type=field.type) for name, field in []},
        )
    return pa.concat_tables(tables)


def lake_month_partitions(table: Any, snapshot: int) -> set[str]:
    """Every distinct month partition value registered (the junk check)."""
    out: set[str] = set()
    for f in lake_files(table, snapshot):
        assert f.partition_values is not None
        out.add(str(f.partition_values[1]))
    return out


def expected_month(ts_us: int) -> str:
    """The epoch-month a micros timestamp partitions under (computed
    independently: floored months since 1970-01, UTC)."""
    from datetime import UTC, datetime, timedelta

    day = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=ts_us)
    return str((day.year - 1970) * 12 + (day.month - 1))


def offsets_reached(
    committed: Mapping[tuple[str, int], int],
    topic: str,
    ends: Mapping[int, int],
) -> bool:
    """Whether committed offsets cover every partition's log end.

    Only partitions that RECEIVED events are checked: the consumer
    commits a partition strictly after staging one of its records, so a
    silent partition has no commit to wait for (single-tenant streams
    leave every other partition silent forever)."""
    nonzero = {p: end for p, end in ends.items() if end > 0}
    return bool(nonzero) and all(
        committed.get((topic, p), -1) >= end for p, end in nonzero.items()
    )


def group_offsets_reached(
    committed: Mapping[int, int], ends: Mapping[int, int]
) -> bool:
    """The AdminClient-shaped view of the same predicate (partition →
    offset, no topic key)."""
    nonzero = {p: end for p, end in ends.items() if end > 0}
    return bool(nonzero) and all(
        committed.get(p, -1) >= end for p, end in nonzero.items()
    )


# ---------------------------------------------------------------------------
# Waiting (never a bare sleep)
# ---------------------------------------------------------------------------


def wait_until(
    pred: Callable[[], bool],
    timeout_s: float,
    *,
    interval_s: float = 0.25,
    desc: str | Callable[[], str],
) -> None:
    """Poll ``pred`` until it holds; an assertion failure on timeout
    names what never happened. ``desc`` may be a thunk — pass one when
    the text carries live state, or the message freezes the value the
    call started with."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if pred():
            return
        time.sleep(interval_s)
    text = desc() if callable(desc) else desc
    raise AssertionError(f"timed out ({timeout_s}s) waiting for: {text}")


async def wait_until_async(
    pred: Callable[[], bool],
    timeout_s: float,
    *,
    interval_s: float = 0.25,
    desc: str | Callable[[], str],
) -> None:
    """``wait_until`` for async tests: the predicate's blocking I/O runs
    off the loop the pipeline shares."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if await asyncio.to_thread(pred):
            return
        await asyncio.sleep(interval_s)
    text = desc() if callable(desc) else desc
    raise AssertionError(f"timed out ({timeout_s}s) waiting for: {text}")


# ---------------------------------------------------------------------------
# Millrace config for the live stack
# ---------------------------------------------------------------------------


def live_config(
    *,
    topic: str,
    group: str,
    partitions: tuple[int, ...],
    stage_base_path: str,
    catalog: str,
    namespace: str,
    table: str,
    target_output_bytes: int = 64 * 1024 * 1024,
    flush_deadline_s: int = 900,
    slow_lane_deadline_s: int = 21600,
    # The suite's age-triggered flushes stage bytes-to-KB payloads, far
    # below any production floor — the harness drops the age lane's
    # minimum size to 1 so the deadline alone decides.
    min_flush_bytes: int = 1,
    event_time_policy: EventTimePolicy = EventTimePolicy.QUARANTINE,
    event_time_max_future_s: int = 86400,
    event_time_max_past_s: int = 30 * 86400,
    assignment: AssignmentMode = AssignmentMode.STATIC,
    stage_store_url: str = f"s3://{BUCKET}",
    flush_sweep_s: int = 1,
    metrics_port: int = 0,
) -> Config:
    """A validated live-stack Config. Stage instances land at
    ``{stage_base_path}/{topic}/{partition}`` on the MinIO bucket."""
    return Config(
        kafka_bootstrap_servers=KAFKA_BOOTSTRAP,
        kafka_topic=topic,
        kafka_group_id=group,
        assignment_mode=assignment,
        static_partitions=partitions if assignment is AssignmentMode.STATIC else None,
        # The suite produces-then-consumes with a fresh group per test;
        # production's default is LATEST (fleet policy — see config.py).
        kafka_auto_offset_reset=AutoOffsetReset.EARLIEST,
        team_key_codec=TeamKeyCodec.UTF8_DECIMAL,
        catalog=catalog,
        namespace=namespace,
        table=table,
        stage_store_url=stage_store_url,
        stage_base_path=stage_base_path,
        target_output_bytes=target_output_bytes,
        flush_deadline_s=flush_deadline_s,
        slow_lane_deadline_s=slow_lane_deadline_s,
        min_flush_bytes=min_flush_bytes,
        max_files_per_commit=512,
        poison=PoisonConfig(max_records_per_run=1000, value_max_bytes=1 << 20),
        consume_batch_size=500,
        poll_timeout_ms=100,
        event_time_policy=event_time_policy,
        metrics_port=metrics_port,
        event_time_max_past_s=event_time_max_past_s,
        event_time_max_future_s=event_time_max_future_s,
        flush_sweep_s=flush_sweep_s,
    )


def live_slate_settings(flush_interval_ms: int = 20) -> Any:
    """SlateDB settings for the live stack: the WAL flush ticker bounds
    stage-ack latency (default 100 ms); tightened for suite time only."""
    from slatedb.uniffi import Settings

    s = Settings.default()
    s.set("flush_interval", f'"{flush_interval_ms}ms"')
    return s


def s3_config() -> S3Config:
    return S3Config(
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
        endpoint_override=S3_ENDPOINT,
        region=S3_REGION,
        allow_bucket_creation=True,
    )


# ---------------------------------------------------------------------------
# The in-process pipeline
# ---------------------------------------------------------------------------


class Pipeline:
    """One millrace pipeline (consumer + flusher + runner) running
    in-process against the live stack — the scenario harness for every
    test that does not need a killable subprocess.

    ``auto_flush=False`` leaves the flush loop stopped so the test drives
    ``runner.run_once()`` by hand (deterministic DDL-race and junk
    scenarios); the consumer always runs.
    """

    def __init__(
        self,
        cfg: Config,
        *,
        auto_flush: bool = True,
        sweep_s: float | None = None,
    ) -> None:
        self.cfg = cfg
        self._auto_flush = auto_flush
        self._sweep_s = float(cfg.flush_sweep_s) if sweep_s is None else sweep_s
        self.stages = StageManager(
            cfg.stage_store_url,
            cfg.stage_base_path,
            settings=live_slate_settings(),
        )
        self.client = HoglakeClient(HOGLAKE_URL, s3=s3_config())
        self.flusher: HoglakeFlusher | None = None
        self.consumer: MillraceConsumer | None = None
        self.runner: FlushRunner | None = None
        self._tasks: list[asyncio.Task[None]] = []
        self._stopped = False

    async def start(self) -> Pipeline:
        cfg = self.cfg
        # Mirror main.py's M5 wiring: the flusher's I/O pool is sized to
        # the sweep's process-wide in-flight bound, or the parallel sweep
        # serializes on one executor thread (a shape production never
        # runs).
        self.flusher = await asyncio.get_running_loop().run_in_executor(
            None,
            partial(
                HoglakeFlusher.resolve,
                self.client,
                cfg,
                now_us=now_us,
                io_workers=4 * cfg.flush_concurrency,
            ),
        )
        consumer = MillraceConsumer(
            create_kafka_consumer(cfg), self.stages, cfg, now_us=now_us
        )
        self.consumer = consumer
        self.runner = FlushRunner(
            self.stages,
            self.flusher,
            cfg.planner_knobs(),
            now_us=now_us,
            sweep_s=self._sweep_s,
        )
        await consumer.start()
        self._tasks.append(asyncio.create_task(consumer.run()))
        if self._auto_flush:
            self._tasks.append(asyncio.create_task(self.runner.run()))
        return self

    async def stop(self) -> None:
        """Orderly stop: both loops finish their in-flight cycle; staged
        state stays durable on the store; then the stages close."""
        if self._stopped:
            return
        self._stopped = True
        assert self.consumer is not None and self.runner is not None
        self.consumer.stop()
        self.runner.stop()
        outcomes = await asyncio.gather(*self._tasks, return_exceptions=True)
        from millrace.flush import FlushHalted

        for outcome in outcomes:
            if isinstance(outcome, FlushHalted):
                continue  # the halt is the runner's to report (runner.halted)
            if isinstance(outcome, BaseException):
                raise outcome
        await self.stages.close()
        self.flusher.close()
        self.client.close()

    async def __aenter__(self) -> Self:
        return await self.start()

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    def start_flush_loop(self) -> None:
        """Start the sweep loop for a pipeline built with
        ``auto_flush=False`` (a test that first drove ``run_once`` by
        hand)."""
        assert self.runner is not None
        assert not self._auto_flush, "the flush loop is already running"
        self._auto_flush = True
        self._tasks.append(asyncio.create_task(self.runner.run()))

    def staged_rows(self) -> int:
        """Sync-free staged-row gauge read is not possible; tests read
        ``stages.gauges()`` from async context instead."""
        raise NotImplementedError


# ---------------------------------------------------------------------------
# The subprocess child (kill -9 and lost-response scenarios)
# ---------------------------------------------------------------------------


class Child:
    """A millrace subprocess (the real ``millrace.main`` entry point)
    with its stdout/stderr drained to a line buffer the test can poll."""

    def __init__(self, env: Mapping[str, str], *, name: str) -> None:
        self.env = dict(env)
        self.name = name
        self.proc: subprocess.Popen[str] | None = None
        self.lines: list[str] = []
        self._lock = threading.Lock()

    def start(self) -> None:
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "millrace.main"],
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        threading.Thread(target=self._drain, daemon=True).start()

    def _drain(self) -> None:
        assert self.proc is not None and self.proc.stdout is not None
        for line in self.proc.stdout:
            with self._lock:
                self.lines.append(line.rstrip())

    def output(self) -> str:
        with self._lock:
            return "\n".join(self.lines)

    def wait_for_log(self, needle: str, timeout_s: float) -> None:
        wait_until(
            lambda: needle in self.output(),
            timeout_s,
            desc=f"child {self.name} logging {needle!r}; output so far:\n{self.output()[-4000:]}",
        )

    def wait_ready(self, port: int, timeout_s: float = 60.0) -> None:
        import httpx

        def ready() -> bool:
            try:
                return (
                    httpx.get(f"http://127.0.0.1:{port}/readyz", timeout=2).status_code
                    == 200
                )
            except Exception:  # noqa: BLE001 - any error is "not ready yet"
                return False

        wait_until(
            ready,
            timeout_s,
            desc=f"child {self.name} /readyz on {port}; output so far:\n{self.output()[-4000:]}",
        )

    def kill(self) -> None:
        """SIGKILL — the unclean stop the durability scenarios exist for."""
        assert self.proc is not None
        self.proc.kill()

    def terminate(self) -> None:
        assert self.proc is not None
        self.proc.terminate()

    def wait_exit(self, timeout_s: float = 60.0) -> int:
        assert self.proc is not None
        return self.proc.wait(timeout=timeout_s)


def child_env(
    cfg: Config,
    *,
    hoglake_url: str | None = None,
    metrics_port: int,
    extra: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """The child process's full environment: the MILLRACE_* knobs for
    ``cfg``, the hoglake/S3 endpoints, and the AWS_* variables SlateDB's
    s3 store resolves from."""
    env = apply_slatedb_aws_env(dict(os.environ))
    env.update(
        {
            # The child logs to stdout; a piped stdout is block-buffered
            # without this, and the tests poll the log.
            "PYTHONUNBUFFERED": "1",
            "MILLRACE_KAFKA_BOOTSTRAP_SERVERS": cfg.kafka_bootstrap_servers,
            "MILLRACE_KAFKA_TOPIC": cfg.kafka_topic,
            "MILLRACE_KAFKA_GROUP_ID": cfg.kafka_group_id,
            "MILLRACE_KAFKA_ASSIGNMENT": cfg.assignment_mode.value,
            # The suite produces-then-consumes with a fresh group per
            # test, so children must start at the beginning; production's
            # default is `latest` (fleet policy — see config.py). The
            # pipeline under test is identical either way.
            "MILLRACE_KAFKA_AUTO_OFFSET_RESET": cfg.kafka_auto_offset_reset.value,
            # The FULL codec spec — value-json carries its payload field
            # inline (value-json:<field>); the bare enum value is refused
            # at boot. The property is the parser's inverse (config.py).
            "MILLRACE_TEAM_KEY_CODEC": cfg.team_key_codec_env,
            "MILLRACE_CATALOG": cfg.catalog,
            "MILLRACE_NAMESPACE": cfg.namespace,
            "MILLRACE_TABLE": cfg.table,
            "MILLRACE_STAGE_URL": f"{cfg.stage_store_url.rstrip('/')}/{cfg.stage_base_path}",
            "MILLRACE_TARGET_OUTPUT_BYTES": str(cfg.target_output_bytes),
            "MILLRACE_FLUSH_DEADLINE_S": str(cfg.flush_deadline_s),
            "MILLRACE_SLOW_LANE_DEADLINE_S": str(cfg.slow_lane_deadline_s),
            "MILLRACE_MIN_FLUSH_BYTES": str(cfg.min_flush_bytes),
            "MILLRACE_MAX_FILES_PER_COMMIT": str(cfg.max_files_per_commit),
            "MILLRACE_POLL_TIMEOUT_MS": str(cfg.poll_timeout_ms),
            "MILLRACE_CONSUME_BATCH_SIZE": str(cfg.consume_batch_size),
            "MILLRACE_EVENT_TIME_POLICY": cfg.event_time_policy.value,
            "MILLRACE_EVENT_TIME_MAX_FUTURE_S": str(cfg.event_time_max_future_s),
            "MILLRACE_EVENT_TIME_MAX_PAST_S": str(cfg.event_time_max_past_s),
            "MILLRACE_FLUSH_SWEEP_S": str(cfg.flush_sweep_s),
            "MILLRACE_METRICS_PORT": str(metrics_port),
            "HOGLAKE_URL": hoglake_url or HOGLAKE_URL,
            "HOGLAKE_S3_ENDPOINT": S3_ENDPOINT,
            "HOGLAKE_S3_ACCESS_KEY": S3_ACCESS_KEY,
            "HOGLAKE_S3_SECRET_KEY": S3_SECRET_KEY,
            "HOGLAKE_S3_REGION": S3_REGION,
        }
    )
    if cfg.static_partitions is not None:
        env["MILLRACE_KAFKA_PARTITIONS"] = ",".join(
            str(p) for p in cfg.static_partitions
        )
    if extra:
        env.update(extra)
    return env


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


# ---------------------------------------------------------------------------
# The commit-path fault-injection proxy (lost commit response, kill windows)
# ---------------------------------------------------------------------------


class CommitProxy:
    """A TCP relay in front of the hoglake server, in the test process.

    Forwards HTTP/1.1 verbatim and logs every request line (with the
    idempotency key for commits). Armed rules act on the RESPONSE of a
    matching request:

    - ``drop``: the full response is read off the server (the commit has
      LANDED) and then the client connection is closed without delivering
      a byte — a lost commit response;
    - ``hold``: the response is parked until the test releases it
      (deliver or drop) — a commit in flight while the client is killed.

    Everything else (startup reads, receipt lookups) passes through
    untouched. The proxy exists because the failure it injects is ON THE
    WIRE — a scripted fake cannot lose a real response.
    """

    def __init__(self, upstream_port: int) -> None:
        self._upstream = ("127.0.0.1", upstream_port)
        self._listener = socket.socket()
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(16)
        self.port = int(self._listener.getsockname()[1])
        self.url = f"http://127.0.0.1:{self.port}"
        self.requests: list[tuple[str, str, str | None]] = []
        self.responses: list[tuple[str, int]] = []  # (idempotency_key, status)
        self.dropped: list[str] = []
        self._lock = threading.Lock()
        self._drop_remaining: int | None = None  # None = not armed
        self._hold_next = False
        self._held: tuple[socket.socket, bytes] | None = None
        self._held_happened = threading.Event()
        self._release_event = threading.Event()
        self._release: bool | None = None
        self._closed = False
        self._threads: list[threading.Thread] = []
        self._accept = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept.start()

    # -- rules ---------------------------------------------------------------

    def arm_drop(self, count: int | None = None) -> None:
        """Drop the response of the next ``count`` commits (None: every
        commit until :meth:`disarm`)."""
        with self._lock:
            self._drop_remaining = count if count is not None else 1 << 30

    def arm_hold_next(self) -> None:
        """Park the response of the very next commit."""
        with self._lock:
            self._hold_next = True
            self._held_happened.clear()
            self._release_event.clear()
            self._release = None

    def disarm(self) -> None:
        with self._lock:
            self._drop_remaining = None
            self._hold_next = False

    def wait_held(self, timeout_s: float = 60.0) -> None:
        """Wait until a commit response is parked in the proxy."""
        if not self._held_happened.wait(timeout_s):
            raise AssertionError(
                f"no commit response reached the proxy within {timeout_s}s; "
                f"requests so far: {self.requests}"
            )

    def release_held(self, *, deliver: bool) -> None:
        """Resolve the parked response: deliver it or drop the connection."""
        with self._lock:
            self._release = deliver
            held = self._held
        self._release_event.set()
        if not deliver and held is not None:
            try:
                held[0].close()
            except OSError:
                pass

    def commit_requests(self, idempotency_key: str | None = None) -> int:
        """How many commit POSTs (optionally for one key) the server got."""
        with self._lock:
            return sum(
                1
                for method, path, key in self.requests
                if method == "POST"
                and path.endswith("/commit/prepared")
                and (idempotency_key is None or key == idempotency_key)
            )

    # -- plumbing --------------------------------------------------------------

    def close(self) -> None:
        self._closed = True
        # Resolve any parked response so no proxy thread outlives the test.
        with self._lock:
            if self._release is None:
                self._release = False
            held = self._held
        self._release_event.set()
        if held is not None:
            try:
                held[0].close()
            except OSError:
                pass
        self._listener.close()

    def _accept_loop(self) -> None:
        while not self._closed:
            try:
                downstream, _ = self._listener.accept()
            except OSError:
                return
            thread = threading.Thread(
                target=self._serve, args=(downstream,), daemon=True
            )
            thread.start()
            self._threads.append(thread)

    @staticmethod
    def _read_head(sock: socket.socket) -> bytes | None:
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = sock.recv(65536)
            if not chunk:
                return None if not data else data
            data += chunk
            if len(data) > 1 << 20:
                raise OSError("header block too large")
        return data

    @staticmethod
    def _read_exactly(sock: socket.socket, n: int) -> bytes:
        data = b""
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            if not chunk:
                raise OSError("connection closed mid-body")
            data += chunk
        return data

    def _serve(self, downstream: socket.socket) -> None:
        try:
            upstream = socket.create_connection(self._upstream, timeout=10)
        except OSError:
            downstream.close()
            return
        try:
            while not self._closed:
                head = self._read_head(downstream)
                if head is None:
                    return
                head_block, rest = head.split(b"\r\n\r\n", 1)
                lines = head_block.split(b"\r\n")
                method, path = lines[0].decode(errors="replace").split(" ", 2)[:2]
                headers = {}
                for line in lines[1:]:
                    name, _, value = line.partition(b":")
                    headers[name.strip().lower()] = value.strip()
                length = int(headers.get(b"content-length", b"0"))
                body = rest + (
                    self._read_exactly(downstream, length - len(rest))
                    if length > len(rest)
                    else b""
                )
                body = body[:length]
                key: str | None = None
                if method == "POST" and path.endswith("/commit/prepared"):
                    try:
                        key = json.loads(body).get("idempotency_key")
                    except ValueError:
                        key = None
                with self._lock:
                    self.requests.append((method, path, key))
                    drop = self._drop_remaining is not None and key is not None
                    if drop and self._drop_remaining is not None:
                        self._drop_remaining -= 1
                        if self._drop_remaining <= 0:
                            self._drop_remaining = None
                    hold = self._hold_next and key is not None
                    if hold:
                        self._hold_next = False
                upstream.sendall(head_block + b"\r\n\r\n" + body)
                response = self._read_response(upstream)
                if key is not None:
                    status_line = response.split(b"\r\n", 1)[0].decode(errors="replace")
                    status = int(status_line.split(" ", 2)[1])
                    with self._lock:
                        self.responses.append((key, status))
                if not (drop or hold):
                    downstream.sendall(response)
                    continue
                if drop:
                    with self._lock:
                        self.dropped.append(key or "?")
                    return  # both sockets close in the finally
                # hold: park until the test resolves it
                self._held = (downstream, response)
                self._held_happened.set()
                self._release_event.wait()
                with self._lock:
                    release = self._release
                if release:
                    try:
                        downstream.sendall(self._held[1])
                    except OSError:
                        pass
                self._held = None
                return
        except OSError:
            return
        finally:
            for sock in (downstream, upstream):
                try:
                    sock.close()
                except OSError:
                    pass

    def _read_response(self, upstream: socket.socket) -> bytes:
        head = self._read_head(upstream)
        if head is None:
            raise OSError("server closed without a response")
        head_block, _, rest = head.partition(b"\r\n\r\n")
        headers = {}
        for line in head_block.split(b"\r\n")[1:]:
            name, _, value = line.partition(b":")
            headers[name.strip().lower()] = value.strip()
        if b"chunked" in headers.get(b"transfer-encoding", b""):
            body = rest
            while not body.endswith(b"\r\n0\r\n\r\n") and b"\r\n0\r\n\r\n" not in body:
                chunk = upstream.recv(65536)
                if not chunk:
                    break
                body += chunk
            return head_block + b"\r\n\r\n" + body
        length = int(headers.get(b"content-length", b"0"))
        body = rest + (
            self._read_exactly(upstream, length - len(rest))
            if length > len(rest)
            else b""
        )
        return head_block + b"\r\n\r\n" + body[:length]


def run_coro(coro: Any) -> Any:
    """Drive one coroutine from a sync test (readbacks inside an async
    scenario use ``asyncio.to_thread`` instead)."""
    return asyncio.run(coro)
