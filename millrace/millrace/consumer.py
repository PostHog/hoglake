"""Kafka consumption and offset management (docs/kafka-ingestion.md §The
problem's consume loop).

Poll → stage → commit: every well-formed polled record is staged through
:meth:`millrace.stage.PartitionStage.stage_batch` under
``rows/(group, record_ts, offset)``; a record that can never become a
row is DROPPED — logged and counted, nothing written — and the
partition's Kafka offset commits STRICTLY after the batch's remote
durability (rows-then-offset, AGENT.md invariant 10 restated for a
staging area): an offset is committed only once every record at or
below it is durable in SlateDB or was dropped and counted. The consume
loop never waits on the flush loop: staging is the buffer. There is no
in-memory accumulation — every consumed record is either
durable-and-committed, dropped-and-counted, or, on a crash, replayed
from the last commit (at-least-once by construction).

The ``group`` a record stages under is the destination table's
PARTITION TUPLE for it (:class:`millrace.grouping.GroupKeyer`): the
value is parsed as a JSON object once, each live-spec field's source
scalar is read by path, coerced, transformed and rendered as the wire
string the commit's ``partition_values`` carry. The Kafka message key is
never read. The keyer is built from the table shape the flusher resolved
at startup (same pinned incarnation) and is re-read on the flusher's TTL
(a quarter of the catalog's snapshot retention, 450 s when the catalog
reports none) on the consumer's own schedule; a failed re-read logs and
keeps the old keyer. A stale keyer only groups under an older spec — the
flush fans every window out by the LIVE spec, so files stay correct — and
a spec change therefore just starts new groups while the old ones drain
by age or the slow lane (:func:`classify_message`).

One poll stages its partitions CONCURRENTLY: every partition's
``stage_batch`` (one WAL write + durability wait each) is issued as its
own task and awaited together (:func:`asyncio.gather`), and the single
commit follows only once every handle has settled — the ordering
invariant is "all acks before the commit", not "acks in sequence"
(a serial per-partition wait would cap the pod at one WAL round trip
per partition per poll). The batch is bounded by a record cap
(``MILLRACE_CONSUME_BATCH_SIZE``) AND a byte cap
(``MILLRACE_CONSUME_BATCH_MAX_BYTES``), the latter enforced by clamping
the poll's message count to the byte budget ÷ the run's observed
average record size.

Claiming follows the consumer assignment (docs/kafka-ingestion.md
§Deployment: the assignment *is* the claim, and each partition's SlateDB
instance is claimed with it):

- ``static`` (millpond's model): an explicit partition list from config,
  ``assign()`` with ``OFFSET_STORED`` — resume from the committed
  offset, fall back to ``auto.offset.reset`` where none exists.
- ``cooperative``: ``subscribe()`` with rebalance callbacks
  (cooperative-sticky). librdkafka fires the callbacks INSIDE
  ``consume()``; they only enqueue an event, and the loop applies it on
  the asyncio side through :meth:`StageManager.sync_assignment` — on
  assign the partition's stage is opened and recovered, on revoke/lost
  it is closed cleanly (flush nothing, abandon nothing: staged state is
  durable by construction and stays on the store for the new owner).
  Events are applied BEFORE the poll's messages are staged, so a
  partition revoked mid-poll has its just-fetched messages dropped
  locally — the new owner replays them from the last commit.

``auto.offset.reset`` is a config knob
(``MILLRACE_KAFKA_AUTO_OFFSET_RESET``, default ``latest`` — the fleet
policy since millpond's whole-retention replay incident). The full
tension — ``earliest`` never silently drops a fresh partition's backlog
but replays all of retention; ``latest`` starts at the head; millrace's
staged state plus receipt horizon bound the replay damage either way —
is carried by :class:`millrace.config.AutoOffsetReset` and
docs/kafka-ingestion.md §Delivery semantics.

Fencing: SlateDB is single-writer per path and fences LAZILY — a newer
writer's open succeeds, and the older writer learns it lost the path at
its next write's durability wait (``Error.Closed(FENCED)``, pinned by
tests/test_slatedb_parity.py). Under static assignment no rebalance
event announces the loss (an EKS one-at-a-time StatefulSet rollout
double-assigns the ordinal, and the new pod opens the same paths). The
loop therefore catches FENCED at the staging write, closes the stage
locally so this process can never touch the path again, and raises
:class:`ConsumerFencedError` — a halt DISTINCT from a config error or a
flush halt, so the supervisor can tell "another owner holds this path"
apart from a crash and never blindly restart this pod back onto the
same assignment (a restarted fenced pod re-opens the path and fences
the NEW owner: the flap this distinction exists to prevent). An IDLE
partition's fence has no write to raise from; it surfaces in the flush
sweep's planning scan instead, which fences the partition out of
scheduling and fails liveness (flush.py ``_note_fenced``). See the
README's rollout-barrier paragraph.

The loop stops consuming only on error. The staging gauges (staged
bytes/rows per partition, oldest ELIGIBLE staged age) are published for
alerts and never read by the loop (docs/kafka-ingestion.md
§Deployment). SlateDB's own L0 write stall (``l0_max_ssts`` with the
compactor behind) stalls the staging write itself and surfaces through
SlateDB's metrics.

Malformed records are dropped, logged and metered: each drop counts in
``millrace_records_dropped_total{reason}`` (the label collapses
``malformed_partition_source:<path>`` to ``malformed_partition_source``
to keep cardinality bounded) and logs at WARNING with topic, partition,
offset and the full reason — rate-limited to one line per (partition,
reason) per :data:`DROP_LOG_INTERVAL_S` seconds, the first drop of a
reason always logging and later lines carrying the suppressed count.
The commit covers a dropped record trivially (there is nothing to
stage), and the loop moves on.

Async discipline: the confluent-kafka Consumer is single-threaded
(librdkafka), so every blocking call — ``consume``, ``commit``,
``close`` — runs on ONE dedicated worker thread via the event loop's
executor, keeping the asyncio loop free for the flush loop that shares
it. Anything time-based (staging timestamps, the age gauge) reads the
injected ``now_us`` clock; retry waits use the injected ``sleep``.

Graceful shutdown: :meth:`stop` stops polling after the in-flight cycle;
since the commit follows every staged batch inside the cycle, committed
offsets already reflect exactly what was durably staged. Final flushing
is NOT this component's job (it is the flusher's), and the StageManager
is caller-owned — the flush loop shares it — so :meth:`close` closes
only the Kafka side.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from typing import Any, Final, Literal, Protocol, TypeVar

from confluent_kafka import (
    OFFSET_STORED,
    TIMESTAMP_NOT_AVAILABLE,
    KafkaError,
    KafkaException,
    TopicPartition,
)
from confluent_kafka import (
    Consumer as _KafkaConsumer,
)
from pyhoglake import TableInfo
from slatedb.uniffi import CloseReason
from slatedb.uniffi import Error as SlateError

from .coerce import DecodeError, load_json_object
from .config import AssignmentMode, Config
from .flush import FlushHalted
from .grouping import GroupKeyer, PartitionSourceError
from .stage import DroppedRecord, StagedRecord, StageManager

log = logging.getLogger(__name__)

_T = TypeVar("_T")

_MICROS_PER_MILLI: Final = 1_000
_INT64_MIN: Final = -(1 << 63)
_INT64_MAX: Final = (1 << 63) - 1

# The bounded consume-time drop vocabulary (the
# ``millrace_records_dropped_total`` reason label).
REASON_INVALID_OFFSET: Final = "invalid_offset"
REASON_MISSING_TIMESTAMP: Final = "missing_timestamp"
REASON_INVALID_TIMESTAMP: Final = "invalid_timestamp"
REASON_MISSING_VALUE: Final = "missing_value"
# The payload is not a strict JSON object (the keyer has nothing to read).
REASON_MALFORMED_VALUE: Final = "malformed_value"
# A partition source scalar is the wrong JSON type, uncoercible to its
# column, or untransformable. The logged reason names the field:
# ``malformed_partition_source:<dotted path>``; the metric label does not.
REASON_MALFORMED_PARTITION_SOURCE: Final = "malformed_partition_source"

#: At most one drop log line per (partition, reason) per this many
#: seconds of the injected monotonic clock.
DROP_LOG_INTERVAL_S: Final = 10.0

_COMMIT_MAX_ATTEMPTS: Final = 3
_COMMIT_BASE_DELAY_S: Final = 0.5

# Coordinator-warmup probe bounds for static start (see
# ``MillraceConsumer._await_offset_storage_ready``). A fresh broker's
# NOT_COORDINATOR answer comes back at once, so quick probes with short
# sleeps ride out the __consumer_offsets election (observed at ~1 s)
# with a wide margin; a broker that keeps refusing fails startup loudly
# instead of idling in OFFSET_STORED limbo.
_OFFSET_STORAGE_WARMUP_MAX_ATTEMPTS: Final = 24
_OFFSET_STORAGE_WARMUP_RETRY_S: Final = 0.5
_OFFSET_STORAGE_WARMUP_TIMEOUT_S: Final = 5.0


class ConsumerFencedError(Exception):
    """A claimed partition's SlateDB path fenced this writer: a NEWER
    writer opened the same path (SlateDB's lazy ``Error.Closed(FENCED)``,
    pinned by tests/test_slatedb_parity.py — an EKS one-at-a-time
    StatefulSet rollout double-assigning the ordinal is the static-mode
    shape of this). There is no rebalance event to catch instead, and
    continuing is pointless: every later write fails the same way.

    The loop closes the fenced stage locally (this process never touches
    the path again) and halts with this DISTINCT exception — distinct
    from a config error or a flush halt, so the supervisor can refuse
    the flap: a restarted fenced pod re-opens the same path and fences
    the NEW owner, and two pods then fence each other until the rollout
    replaces one (offsets never advance during the flap). ``partitions``
    names the fenced set. The offsets of the failed batch are never
    committed — the rows either landed (and the replay re-puts them
    idempotently) or did not (and the replay restages them).
    """

    def __init__(self, partitions: Sequence[tuple[str, int]]) -> None:
        self.partitions = tuple(sorted(partitions))
        super().__init__(
            "stage fenced by a newer writer (another owner holds the "
            "partition's SlateDB path): "
            + ", ".join(f"{t}[{p}]" for t, p in self.partitions)
            + " — halting rather than flapping against the new owner; "
            "do not restart this pod onto the same assignment (README: "
            "static mode needs a rollout barrier)"
        )


def _is_fenced(exc: BaseException) -> bool:
    """Whether ``exc`` is SlateDB's lazy FENCED — ``Error.Closed`` with
    reason FENCED (tests/test_slatedb_parity.py pins the shape: a newer
    writer opened the path). Any OTHER ``Closed`` reason (a local
    shutdown racing an in-flight call, a panic) is not a contested-path
    signal and never takes the fenced path."""
    return isinstance(exc, SlateError.Closed) and exc.reason == CloseReason.FENCED


class MessageView(Protocol):
    """The slice of ``confluent_kafka.Message`` the consume loop reads —
    a protocol so the scripted fake in tests needs no broker. The
    coordinate accessors are Optional in the library's own stubs (error
    events may lack them); the loop refuses coordinate-less records
    before classification."""

    def error(self) -> Any: ...
    def topic(self) -> str | None: ...
    def partition(self) -> int | None: ...
    def offset(self) -> int | None: ...
    def key(self) -> bytes | None: ...
    def value(self) -> bytes | None: ...
    def timestamp(self) -> tuple[int, int]: ...


class KafkaConsumerLike(Protocol):
    """The slice of ``confluent_kafka.Consumer`` the loop drives."""

    def consume(
        self, num_messages: int = 1, timeout: float = -1
    ) -> Sequence[MessageView]: ...
    def commit(
        self, *, offsets: list[Any], asynchronous: Literal[False]
    ) -> list[Any]: ...
    def committed(self, partitions: list[Any], timeout: float = -1) -> list[Any]: ...
    def assign(self, partitions: list[Any]) -> None: ...
    def subscribe(
        self,
        topics: list[str],
        on_assign: Any = None,
        on_revoke: Any = None,
        on_lost: Any = None,
    ) -> None: ...
    def assignment(self) -> list[Any]: ...
    def close(self) -> None: ...


def classify_message(
    keyer: GroupKeyer,
    *,
    offset: int,
    value: bytes | None,
    timestamp: tuple[int, int],
) -> StagedRecord | DroppedRecord:
    """One consumed record's fields → the record to stage under its
    partition tuple, or the drop with its reason.

    TOTAL over record content — a malformed record becomes a
    :class:`~millrace.stage.DroppedRecord` with a reason (the first
    failing check names it), so the loop never wedges on one record.
    ``timestamp`` is the Kafka ``(type, milliseconds)`` pair;
    TIMESTAMP_NOT_AVAILABLE can never key a row, so it drops. The Kafka
    message key is never read.

    The value is parsed ONCE as a strict JSON object (not an object →
    ``malformed_value``) and the keyer extracts the tuple from it; a
    malformed partition source scalar drops with the reason
    ``malformed_partition_source:<field path>``.
    """
    if offset < 0 or offset > _INT64_MAX:
        return DroppedRecord(offset, REASON_INVALID_OFFSET)
    ts_type, ts_ms = timestamp
    if ts_type == TIMESTAMP_NOT_AVAILABLE:
        return DroppedRecord(offset, REASON_MISSING_TIMESTAMP)
    event_ts_us = ts_ms * _MICROS_PER_MILLI
    if not _INT64_MIN <= event_ts_us <= _INT64_MAX:
        return DroppedRecord(offset, REASON_INVALID_TIMESTAMP)
    if value is None:
        return DroppedRecord(offset, REASON_MISSING_VALUE)
    try:
        doc = load_json_object(value)
    except DecodeError:
        return DroppedRecord(offset, REASON_MALFORMED_VALUE)
    try:
        group = keyer.group_for(doc)
    except PartitionSourceError as exc:
        return DroppedRecord(offset, f"{REASON_MALFORMED_PARTITION_SOURCE}:{exc.path}")
    return StagedRecord(
        group=group, event_ts_us=event_ts_us, offset=offset, payload=value
    )


def drop_reason_label(reason: str) -> str:
    """The ``millrace_records_dropped_total`` label for a drop reason:
    the reason itself, except ``malformed_partition_source:<path>``
    collapses to ``malformed_partition_source`` (bounded cardinality;
    the log line keeps the path)."""
    head, sep, _path = reason.partition(":")
    return head if sep and head == REASON_MALFORMED_PARTITION_SOURCE else reason


class ShapeSource(Protocol):
    """The destination shape the consumer re-reads to refresh its keyer
    (``flush.ShapeReader`` in production): ``read`` is the
    ``totals=false`` identity read pinned to the startup incarnation,
    ``ttl_s`` the re-read cadence. Both block (pyhoglake I/O) and run off
    the asyncio loop."""

    def read(self) -> TableInfo: ...
    def ttl_s(self) -> float: ...


@dataclass(frozen=True, slots=True)
class ConsumerStats:
    """Cumulative counters for the run so far (the operational surface's
    input; passive observability — nothing here feeds control flow).
    ``commit_attempts`` counts every commit RPC including retried ones;
    ``commit_retries`` counts only the retried ones. ``records_dropped``
    maps a drop reason label (:func:`drop_reason_label`) to its count."""

    messages_consumed: int
    records_staged: int
    records_dropped: Mapping[str, int]
    kafka_error_events: int
    messages_skipped_revoked: int
    commit_attempts: int
    commit_retries: int


def kafka_consumer_config(cfg: Config) -> dict[str, Any]:
    """The librdkafka client config for one pipeline.

    ``auto.offset.reset`` comes from the config knob (default ``latest``
    — the fleet policy; the tension is on
    :class:`millrace.config.AutoOffsetReset`); auto-commit and
    auto-offset-store are OFF — offsets move only through the loop's
    explicit commits, strictly after durable staging. Cooperative mode
    pins the cooperative-sticky assignor so rebalance callbacks hand out
    incremental assign/revoke events. The three fetch knobs are the
    whale-shape fetch tuning (config.py's table: the 1 MiB library
    default for ``max.partition.fetch.bytes`` capped millpond's hot
    partition at ~50 MB/s; these defaults let one hot partition's poll
    arrive in one response, keep the per-fetch aggregate ahead of
    several hot partitions, and hold ~5 s of pod inflow in the local
    queue before librdkafka throttles fetches).
    """
    config: dict[str, Any] = {
        "bootstrap.servers": cfg.kafka_bootstrap_servers,
        "client.id": f"millrace-{cfg.kafka_topic}",
        "group.id": cfg.kafka_group_id,
        "auto.offset.reset": cfg.kafka_auto_offset_reset.value,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "max.partition.fetch.bytes": cfg.kafka_max_partition_fetch_bytes,
        "fetch.max.bytes": cfg.kafka_fetch_max_bytes,
        "queued.max.messages.kbytes": cfg.kafka_queued_max_messages_kbytes,
    }
    if cfg.assignment_mode is AssignmentMode.COOPERATIVE:
        config["partition.assignment.strategy"] = "cooperative-sticky"
    return config


def create_kafka_consumer(cfg: Config) -> KafkaConsumerLike:
    """The real confluent-kafka consumer for ``cfg``."""
    return _KafkaConsumer(kafka_consumer_config(cfg))


class MillraceConsumer:
    """The consume loop for one topic → one table pipeline.

    Built over a :class:`KafkaConsumerLike` (the real
    ``confluent_kafka.Consumer`` from :func:`create_kafka_consumer`, or
    the scripted fake in tests) and a CALLER-OWNED
    :class:`millrace.stage.StageManager` — the flush loop shares the
    manager, so :meth:`close` closes only the Kafka side. Partition
    stages open/recover and close with the assignment
    (:meth:`StageManager.sync_assignment`).

    ``keyer`` maps a payload to its partition tuple (the destination's
    shape at startup). ``shape``, when given, is re-read every
    ``shape.ttl_s()`` seconds of the injected ``monotonic`` clock — on
    the consume loop's own schedule, before a poll's records are
    classified — and a fresh keyer replaces the old one; a failed
    re-read (or a shape the keyer refuses) logs and keeps the old keyer
    (a stale keyer only groups under an older spec; the flush's fanout
    by the live spec keeps files correct).

    ``now_us`` is the injected clock for staging timestamps; ``sleep``
    backs the commit retry ladder (both injectable so tests never
    sleep).
    """

    def __init__(
        self,
        kafka: KafkaConsumerLike,
        stages: StageManager,
        config: Config,
        *,
        keyer: GroupKeyer,
        now_us: Callable[[], int],
        shape: ShapeSource | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._kafka = kafka
        self._stages = stages
        self._cfg = config
        self._keyer = keyer
        self._shape = shape
        self._monotonic = monotonic
        self._next_shape_read: float | None = None
        self._shape_ttl_s: float | None = None
        self._now_us = now_us
        self._sleep = sleep
        # librdkafka's Consumer is single-threaded: EVERY call (and every
        # callback fired from inside one) stays on this one worker.
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="millrace-kafka"
        )
        self._event_lock = threading.Lock()
        self._assignment_events: list[tuple[str, tuple[tuple[str, int], ...]]] = []
        self._started = False
        self._stopping = False
        self._closed = False
        # Running observation of record sizes (key + value bytes over
        # classified records) feeding the per-poll byte cap — see _step.
        self._observed_records = 0
        self._observed_bytes = 0
        self._messages_consumed = 0
        self._records_staged = 0
        self._records_dropped: dict[str, int] = {}
        # Drop-log rate limit per (topic, partition, reason): the
        # monotonic time of the last logged line and the drops since.
        self._drop_log_at: dict[tuple[str, int, str], float] = {}
        self._drop_log_suppressed: dict[tuple[str, int, str], int] = {}
        self._kafka_error_events = 0
        self._messages_skipped_revoked = 0
        self._commit_attempts = 0
        self._commit_retries = 0
        self._committed: dict[tuple[str, int], int] = {}

    # -- lifecycle -------------------------------------------------------------

    async def start(self) -> None:
        """Open the partition stages and start consuming. Idempotent.

        Static mode opens and recovers the stages BEFORE ``assign()`` —
        a record must never arrive before its partition's stage exists.
        Cooperative mode subscribes; the first poll's rebalance
        callbacks open stages through the same ``sync_assignment`` path.
        """
        if self._started:
            return
        if self._closed:
            raise RuntimeError("consumer is closed")
        self._started = True
        cfg = self._cfg
        if cfg.assignment_mode is AssignmentMode.STATIC:
            partitions = cfg.static_partitions or ()
            sync = await self._stages.sync_assignment(
                {(cfg.kafka_topic, p) for p in partitions}
            )
            for (topic, partition), opened in sorted(sync.opened.items()):
                log.info(
                    "claimed %s[%d]: stage %s open (recovery: %d pending "
                    "prepared, %d legacy markers collected)",
                    topic,
                    partition,
                    opened.stage.path,
                    len(opened.recovery.pending_prepared),
                    opened.recovery.legacy_markers_collected,
                )
            # OFFSET_STORED: resume from the committed offset, fall back
            # to auto.offset.reset (the config knob, default latest)
            # where none exists — so a partition reassigned between
            # deploys resumes from whichever pod owned it last, and a
            # fresh one starts where the knob says.
            # The coordinator warmup is load-bearing: librdkafka resolves
            # OFFSET_STORED with ONE OffsetFetch at assign time and never
            # retries it, so a fetch that lands while a fresh broker is
            # still electing the __consumer_offsets leaders (or right
            # after a coordinator failover) leaves the partition assigned
            # but NEVER fetching. The probe rides the warmup out first.
            await self._await_offset_storage_ready(
                [TopicPartition(cfg.kafka_topic, p) for p in partitions]
            )
            self._kafka.assign(
                [TopicPartition(cfg.kafka_topic, p, OFFSET_STORED) for p in partitions]
            )
            log.info("static assignment on %s: %s", cfg.kafka_topic, list(partitions))
        else:
            self._kafka.subscribe(
                [cfg.kafka_topic],
                on_assign=self._on_assign,
                on_revoke=self._on_revoke,
                on_lost=self._on_lost,
            )
            log.info(
                "subscribed to %s (cooperative-sticky); stages follow the "
                "group assignment",
                cfg.kafka_topic,
            )

    async def run(self) -> None:
        """Consume until :meth:`stop`. The in-flight cycle finishes
        (every staged batch is committed inside its cycle by
        construction) and the Kafka consumer is closed on the way out."""
        await self.start()
        try:
            while not self._stopping:
                await self._step()
        finally:
            await self.close()

    def stop(self) -> None:
        """Ask the loop to stop after the in-flight poll cycle. Safe
        from a signal handler or another thread."""
        self._stopping = True

    async def close(self) -> None:
        """Close the Kafka consumer and the worker thread. Idempotent.
        The StageManager is caller-owned and is NOT closed here."""
        if self._closed:
            return
        self._closed = True
        self._stopping = True
        await self._call_kafka(self._kafka.close)
        await asyncio.get_running_loop().run_in_executor(
            None, self._executor.shutdown, True
        )
        log.info(
            "consumer closed (%d messages, %d staged, %d dropped, %d commits)",
            self._messages_consumed,
            self._records_staged,
            sum(self._records_dropped.values()),
            self._commit_attempts,
        )

    # -- introspection ---------------------------------------------------------

    def committed_offsets(self) -> Mapping[tuple[str, int], int]:
        """The last successfully committed offset per partition this run."""
        return dict(self._committed)

    @property
    def keyer(self) -> GroupKeyer:
        """The keyer currently grouping records."""
        return self._keyer

    def stats(self) -> ConsumerStats:
        """A snapshot of the run's counters."""
        return ConsumerStats(
            messages_consumed=self._messages_consumed,
            records_staged=self._records_staged,
            records_dropped=dict(self._records_dropped),
            kafka_error_events=self._kafka_error_events,
            messages_skipped_revoked=self._messages_skipped_revoked,
            commit_attempts=self._commit_attempts,
            commit_retries=self._commit_retries,
        )

    # -- rebalance callbacks (fired by librdkafka INSIDE consume()) -------------

    def _record_assignment_event(self, kind: str, partitions: list[Any]) -> None:
        claimed = tuple((tp.topic, tp.partition) for tp in partitions)
        with self._event_lock:
            self._assignment_events.append((kind, claimed))

    def _on_assign(self, _consumer: Any, partitions: list[Any]) -> None:
        # Runs on the Kafka worker thread, inside consume(). It must not
        # block on asyncio: enqueue; the loop applies it through
        # sync_assignment after the poll returns. (Not calling
        # incremental_assign here is fine — librdkafka then does it
        # automatically when the callback returns.)
        self._record_assignment_event("assign", partitions)

    def _on_revoke(self, _consumer: Any, partitions: list[Any]) -> None:
        self._record_assignment_event("revoke", partitions)

    def _on_lost(self, _consumer: Any, partitions: list[Any]) -> None:
        self._record_assignment_event("lost", partitions)

    # -- the loop ----------------------------------------------------------------

    async def _call_kafka(self, fn: Callable[[], _T]) -> _T:
        """Run a blocking librdkafka call on the dedicated worker thread,
        keeping the asyncio loop free for the flush loop that shares it."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, fn)

    async def _await_offset_storage_ready(self, partitions: list[Any]) -> None:
        """Warm the group coordinator before the OFFSET_STORED assign.

        librdkafka resolves ``OFFSET_STORED`` with ONE OffsetFetch per
        partition at assign time and NEVER retries it: on a fresh broker
        the fetch lands while ``__consumer_offsets`` is still being
        created (and its partition leaders elected), the broker answers
        NOT_COORDINATOR, and the partition then sits assigned but
        unfetched FOREVER — the pipeline idles with the stage empty
        (the live suite's cold-stack compaction run wedged exactly so:
        one error event consumed, nothing ever staged). A ``committed()``
        probe retries internally per call, so a bounded loop of probes
        rides the warmup out; a broker that still refuses after the
        attempts fails startup loudly (the supervisor restarts) rather
        than idling silently.
        """
        for attempt in range(1, _OFFSET_STORAGE_WARMUP_MAX_ATTEMPTS + 1):
            errors: list[Any]
            try:
                committed = await self._call_kafka(
                    partial(
                        self._kafka.committed,
                        partitions,
                        timeout=_OFFSET_STORAGE_WARMUP_TIMEOUT_S,
                    )
                )
                errors = [tp.error for tp in committed if tp.error is not None]
            except KafkaException as e:
                errors = [e]
            if not errors:
                if attempt > 1:
                    log.info(
                        "group coordinator answered after %d warmup probes",
                        attempt,
                    )
                return
            if attempt == _OFFSET_STORAGE_WARMUP_MAX_ATTEMPTS:
                raise KafkaException(
                    f"the group coordinator never answered a committed-offset "
                    f"probe for {self._cfg.kafka_group_id} after {attempt} "
                    f"attempts ({errors[0]}); the broker is not ready"
                )
            log.info(
                "waiting for the group coordinator (attempt %d/%d): %s",
                attempt,
                _OFFSET_STORAGE_WARMUP_MAX_ATTEMPTS,
                errors[0],
            )
            await self._sleep(_OFFSET_STORAGE_WARMUP_RETRY_S)

    async def _step(self) -> None:
        """One loop iteration: poll → apply assignment changes → stage →
        commit. Assignment changes land BEFORE the poll's messages are
        staged, so a partition revoked mid-poll is closed first and its
        just-fetched messages are dropped locally (the new owner replays
        them from the last commit — zero loss).

        The poll's message count is the record cap clamped by the byte
        cap: ``batch_max_bytes ÷ observed average record size`` (the
        run's cumulative mean, key + value bytes of classified records).
        librdkafka's ``consume()`` counts messages only, and every
        message it returns must be staged before its offset can commit —
        so the byte bound has to shape what we ASK for, never drop what
        we got. The floor is 1: a record larger than the whole budget
        still flows, one per poll. The first poll has no observation and
        polls up to the record cap."""
        await self._maybe_refresh_keyer()
        num_messages = self._cfg.consume_batch_size
        if self._observed_records:
            avg = max(1, self._observed_bytes // self._observed_records)
            num_messages = min(
                num_messages, max(1, self._cfg.consume_batch_max_bytes // avg)
            )
        msgs = await self._call_kafka(
            partial(
                self._kafka.consume,
                num_messages=num_messages,
                timeout=self._cfg.poll_timeout_ms / 1000,
            )
        )
        await self._apply_assignment_events()
        if msgs:
            self._messages_consumed += len(msgs)
            await self._stage_and_commit(msgs)

    async def _maybe_refresh_keyer(self) -> None:
        """Re-read the destination shape once its TTL has passed and swap
        in a fresh keyer. Blocking reads run on the default executor, off
        the asyncio loop and off the librdkafka worker. A transient
        failure is not fatal: the old keyer keeps grouping (under an older
        spec, at worst — the flush's live-spec fanout is the correctness
        backstop), and the next attempt waits one more TTL. A
        :class:`~millrace.flush.FlushHalted` from the read
        (``table_recreated``: the name now resolves to another
        incarnation; ``server_capability``) is NOT transient and
        propagates: the loop halts before polling, so nothing more is
        staged or committed against a table the pipeline no longer
        writes (main.py maps it to exit 3)."""
        shape = self._shape
        if shape is None:
            return
        loop = asyncio.get_running_loop()
        now = self._monotonic()
        if self._next_shape_read is None:
            # First step: the keyer was built from a fresh read, so the
            # first re-read is one TTL out.
            try:
                self._shape_ttl_s = await loop.run_in_executor(None, shape.ttl_s)
            except Exception:
                log.warning(
                    "reading the shape TTL failed; retrying the keyer refresh "
                    "next poll",
                    exc_info=True,
                )
                return
            self._next_shape_read = now + self._shape_ttl_s
            return
        if now < self._next_shape_read:
            return
        try:
            info = await loop.run_in_executor(None, shape.read)
            keyer = GroupKeyer(info)
            self._shape_ttl_s = await loop.run_in_executor(None, shape.ttl_s)
        except FlushHalted:
            raise
        except Exception:
            log.warning(
                "destination shape re-read failed; the consumer keeps grouping "
                "under partition spec %s (the flush's live-spec fanout keeps "
                "files correct)",
                self._keyer.spec_id,
                exc_info=True,
            )
        else:
            if (
                keyer.spec_id != self._keyer.spec_id
                or keyer.fields != self._keyer.fields
            ):
                log.info(
                    "partition spec changed (%s → %s, fields %s): new records "
                    "stage under the new tuples; existing groups drain by age "
                    "or the slow lane",
                    self._keyer.spec_id,
                    keyer.spec_id,
                    list(keyer.fields),
                )
            self._keyer = keyer
        assert self._shape_ttl_s is not None
        self._next_shape_read = self._monotonic() + self._shape_ttl_s

    async def _apply_assignment_events(self) -> None:
        """Reconcile the open stages with the consumer assignment after
        a rebalance: newly claimed partitions open + recover;
        revoked/lost ones close cleanly (flush nothing, abandon nothing
        — staged state stays on the store for the new owner)."""
        with self._event_lock:
            events, self._assignment_events = self._assignment_events, []
        if not events:
            return
        for kind, partitions in events:
            log.log(
                logging.WARNING if kind == "lost" else logging.INFO,
                "rebalance: %s %s",
                kind,
                list(partitions),
            )
        # assignment() is the settled truth after the callbacks: syncing
        # to it (rather than to event arithmetic) is self-healing.
        claimed = {(tp.topic, tp.partition) for tp in self._kafka.assignment()}
        sync = await self._stages.sync_assignment(claimed)
        for (topic, partition), opened in sorted(sync.opened.items()):
            log.info(
                "claimed %s[%d]: stage %s open (recovery: %d pending "
                "prepared, %d legacy markers collected)",
                topic,
                partition,
                opened.stage.path,
                len(opened.recovery.pending_prepared),
                opened.recovery.legacy_markers_collected,
            )
        for topic, partition in sync.closed:
            log.info(
                "released %s[%d]: stage closed; staged state stays on the "
                "store for the new owner",
                topic,
                partition,
            )

    async def _stage_and_commit(self, msgs: Sequence[MessageView]) -> None:
        """Stage one poll's records per partition — the partitions'
        batches CONCURRENTLY (one task per ``stage_batch``, so the WAL
        writes and their durability waits overlap instead of costing one
        remote round trip per partition in series) — then commit,
        strictly after EVERY partition's batch is durable, the
        contiguous handled offset of each partition the poll touched.
        gather-then-commit is the ordering invariant: all acks before
        the commit, not acks in sequence."""
        claimed = set(self._stages.partitions())
        staged: dict[tuple[str, int], list[StagedRecord]] = {}
        last_handled: dict[tuple[str, int], int] = {}
        for msg in msgs:
            err = msg.error()
            if err is not None:
                # Broker/client error events carry no stageable record;
                # they are never committed past explicitly — the next
                # handled record's commit covers the position.
                self._kafka_error_events += 1
                if err.code() != KafkaError._PARTITION_EOF:
                    log.warning("kafka error event: %s", err)
                continue
            topic, partition, offset = msg.topic(), msg.partition(), msg.offset()
            if topic is None or partition is None or offset is None:
                # A record without coordinates can neither be staged nor
                # committed past (there is no offset to commit). Counted,
                # logged, skipped.
                self._kafka_error_events += 1
                log.warning(
                    "kafka record without coordinates dropped from the "
                    "cycle: topic=%r partition=%r offset=%r",
                    topic,
                    partition,
                    offset,
                )
                continue
            key = (topic, partition)
            if key not in claimed:
                # Fetched before a revoke that the same poll delivered.
                self._messages_skipped_revoked += 1
                continue
            raw_key, raw_value = msg.key(), msg.value()
            outcome = classify_message(
                self._keyer,
                offset=offset,
                value=raw_value,
                timestamp=msg.timestamp(),
            )
            # The byte cap's observation (key + value bytes per
            # classified record — the in-memory shape the cap guards).
            self._observed_records += 1
            self._observed_bytes += (len(raw_key) if raw_key else 0) + (
                len(raw_value) if raw_value else 0
            )
            last_handled[key] = max(
                outcome.offset, last_handled.get(key, outcome.offset)
            )
            if isinstance(outcome, DroppedRecord):
                self._note_drop(topic, partition, outcome)
            else:
                staged.setdefault(key, []).append(outcome)

        now_us = self._now_us()
        keys = sorted(staged)
        # One task per partition: the WAL PUTs and durability waits are
        # in flight together; gather (return_exceptions) settles EVERY
        # batch before the outcome is examined, so a failure never
        # strands a sibling's in-flight write and the counters reflect
        # whatever did land durably.
        tasks = [
            asyncio.create_task(
                self._stages.stage(*key).stage_batch(staged[key], now_us=now_us),
                name=f"millrace-stage-{key[0]}-{key[1]}",
            )
            for key in keys
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        errors: list[BaseException] = []
        fenced: list[tuple[str, int]] = []
        for key, result in zip(keys, results, strict=True):
            if isinstance(result, BaseException):
                if _is_fenced(result):
                    fenced.append(key)
                else:
                    errors.append(result)
                continue
            self._records_staged += result.rows
        if fenced:
            for exc in errors:
                log.warning(
                    "sibling stage failure beside the fence (secondary): %r", exc
                )
            # Relinquish before raising: the local close makes it
            # impossible for this process to write the contested path
            # again, whatever the supervisor does with the exception.
            await self._relinquish_fenced(fenced)
            raise ConsumerFencedError(fenced)
        if errors:
            # The batch's offsets never commit; the durable siblings are
            # restaged idempotently by the replay.
            raise errors[0]

        if last_handled:
            # Committed offsets are next-to-fetch: last handled + 1. One
            # commit RPC for the whole poll, strictly after every
            # partition's batch is durable.
            offsets = [
                TopicPartition(topic, partition, last + 1)
                for (topic, partition), last in sorted(last_handled.items())
            ]
            await self._commit_offsets(offsets)
            for key, last in last_handled.items():
                self._committed[key] = last + 1

    def _note_drop(self, topic: str, partition: int, dropped: DroppedRecord) -> None:
        """Count one dropped record and log it — at most one line per
        (partition, reason) per :data:`DROP_LOG_INTERVAL_S`; the first
        drop of a reason always logs, and a later line names how many
        drops of that reason were suppressed since the last one. Counted
        when classified: a poll whose batch then fails replays and
        counts its drops again (the counter is an at-least-once tally,
        like the staged count)."""
        label = drop_reason_label(dropped.reason)
        self._records_dropped[label] = self._records_dropped.get(label, 0) + 1
        slot = (topic, partition, dropped.reason)
        now = self._monotonic()
        last = self._drop_log_at.get(slot)
        if last is not None and now - last < DROP_LOG_INTERVAL_S:
            self._drop_log_suppressed[slot] = self._drop_log_suppressed.get(slot, 0) + 1
            return
        suppressed = self._drop_log_suppressed.pop(slot, 0)
        self._drop_log_at[slot] = now
        log.warning(
            "record dropped: topic=%s partition=%d offset=%d reason=%s "
            "(%d more of this reason on this partition suppressed since the "
            "last line)",
            topic,
            partition,
            dropped.offset,
            dropped.reason,
            suppressed,
        )

    async def _relinquish_fenced(self, partitions: Sequence[tuple[str, int]]) -> None:
        """Close fenced partitions' stages locally (best-effort: the
        path has a newer writer, so a failure here changes nothing —
        SlateDB already refuses our writes)."""
        for topic, partition in partitions:
            try:
                await self._stages.close_partition(topic, partition)
            except Exception:
                log.warning(
                    "local close of fenced stage %s[%d] failed (the path has "
                    "a newer writer either way)",
                    topic,
                    partition,
                    exc_info=True,
                )
            else:
                log.error(
                    "FENCED: %s[%d] now has a newer writer — stage closed "
                    "locally; halting so this pod never re-opens the path "
                    "against the new owner",
                    topic,
                    partition,
                )

    async def _commit_offsets(self, offsets: list[Any]) -> None:
        """Synchronous offset commit with a small retry ladder. Commits
        are idempotent (the same offsets recommit harmlessly), so a
        retry after a lost response is safe; exhausting the ladder
        raises — the rows are durable, the restart replays, and a pod
        that cannot commit must not pretend it did."""
        for attempt in range(_COMMIT_MAX_ATTEMPTS):
            self._commit_attempts += 1
            try:
                result = await self._call_kafka(
                    partial(self._kafka.commit, offsets=offsets, asynchronous=False)
                )
                failed = (
                    [tp for tp in result if tp.error is not None]
                    if result is not None
                    else []
                )
                if not failed:
                    return
                raise KafkaException(failed[0].error)
            except KafkaException:
                if attempt == _COMMIT_MAX_ATTEMPTS - 1:
                    log.exception(
                        "offset commit failed after %d attempts — offsets "
                        "unadvanced, restart replays (at-least-once intact)",
                        _COMMIT_MAX_ATTEMPTS,
                    )
                    raise
                delay = _COMMIT_BASE_DELAY_S * (2**attempt)
                self._commit_retries += 1
                log.warning(
                    "offset commit failed (attempt %d/%d), retrying in %.1fs",
                    attempt + 1,
                    _COMMIT_MAX_ATTEMPTS,
                    delay,
                    exc_info=True,
                )
                await self._sleep(delay)
