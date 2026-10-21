"""Kafka consumption and offset management.

Poll → stage → commit. Every well-formed record is staged in the
replica's home stage under ``rows/(group, partition, offset)``
(stage.py); a record that can never become a row is dropped, logged and
counted. A partition's offset commits only after every record at or
below it is durable in SlateDB or was dropped: the only thing between
Kafka and loss. One poll is ONE durable write to the home stage, then
one offset commit for every partition it touched.

The group a record stages under is the destination's partition tuple for
it (:class:`millrace.grouping.GroupKeyer`), built from the shape the
flusher resolved at startup and re-read on the flusher's TTL. A stale
keyer only groups under an older spec: the flush fans every window out by
the live spec, so files stay correct.

Assignment is static (millpond's model): at :meth:`MillraceConsumer.start`
the topic's partition count is read once and this replica claims every
``p`` with ``p % replica_count == ordinal``. Its home stage is the slot of
the smallest claimed partition; every other existing slot ``j`` with
``j % replica_count == ordinal`` is opened for draining. The caller's
``drain`` flushes everything already staged (one sequential pass per
stage), and only then are the partitions assigned with
``OFFSET_STORED``.

Fencing: a home-stage write (or, for a poll whose records were all
dropped, a durable fence probe) that fails with SlateDB's FENCED closes
the home stage locally and raises :class:`~millrace.stage.FencedError`
(exit 4) before any offset of that poll is committed.

Every blocking librdkafka call runs on one dedicated worker thread (the
consumer is single-threaded); classification runs on a worker thread
too, so the event loop the flush shares stays responsive.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from functools import partial
from typing import Any, Final, Literal, Protocol, TypeVar

from confluent_kafka import (
    OFFSET_STORED,
    KafkaError,
    KafkaException,
    TopicPartition,
)
from confluent_kafka import (
    Consumer as _KafkaConsumer,
)
from pyhoglake import TableInfo

from .coerce import DecodeError, load_json_object
from .config import Config
from .flush import FlushHalted
from .grouping import GroupKeyer, PartitionSourceError
from .stage import DroppedRecord, FencedError, StagedRecord, StageManager, is_fenced

log = logging.getLogger(__name__)

_T = TypeVar("_T")

_INT64_MAX: Final = (1 << 63) - 1

# The consume-time drop reasons (``millrace_records_dropped_total``).
REASON_INVALID_OFFSET: Final = "invalid_offset"
REASON_MISSING_VALUE: Final = "missing_value"
REASON_MALFORMED_VALUE: Final = "malformed_value"
# Logged as ``malformed_partition_source:<dotted path>``; the label drops
# the path.
REASON_MALFORMED_PARTITION_SOURCE: Final = "malformed_partition_source"

#: At most one drop log line per (partition, reason) per this many seconds.
DROP_LOG_INTERVAL_S: Final = 10.0

#: Slots probed for a stage to drain at startup: ``j % R == ordinal``
#: for every ``j`` below this (or below the partition count, if larger).
STAGE_SLOT_SCAN_LIMIT: Final = 1024

_COMMIT_MAX_ATTEMPTS: Final = 3
_COMMIT_BASE_DELAY_S: Final = 0.5

_METADATA_TIMEOUT_S: Final = 30.0
_METADATA_MAX_ATTEMPTS: Final = 6
_METADATA_RETRY_S: Final = 2.0

# librdkafka resolves OFFSET_STORED with ONE OffsetFetch at assign time
# and never retries it, so the coordinator is warmed up first.
_OFFSET_STORAGE_WARMUP_MAX_ATTEMPTS: Final = 24
_OFFSET_STORAGE_WARMUP_RETRY_S: Final = 0.5
_OFFSET_STORAGE_WARMUP_TIMEOUT_S: Final = 5.0

#: librdkafka fetch tuning (overridable through the passthrough): one hot
#: partition's poll fits one fetch response, and the local queue holds
#: about 5 s of a 50 MB/s pod inflow before librdkafka throttles fetches.
KAFKA_FETCH_DEFAULTS: Final[Mapping[str, int]] = {
    "max.partition.fetch.bytes": 32 * 1024 * 1024,
    "fetch.max.bytes": 256 * 1024 * 1024,
    "queued.max.messages.kbytes": 262144,
}


class AssignmentError(Exception):
    """The static assignment cannot be computed: the topic is unknown or
    errored, or this ordinal's share is empty. A startup refusal (exit 2);
    nothing was claimed."""


class MessageView(Protocol):
    """The slice of ``confluent_kafka.Message`` the loop reads."""

    def error(self) -> Any: ...
    def topic(self) -> str | None: ...
    def partition(self) -> int | None: ...
    def offset(self) -> int | None: ...
    def key(self) -> bytes | None: ...
    def value(self) -> bytes | None: ...


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
    def list_topics(self, topic: str | None = None, timeout: float = -1) -> Any: ...
    def close(self) -> None: ...


def compute_assignment(
    partition_count: int, replica_count: int, ordinal: int
) -> tuple[int, ...]:
    """Every ``p`` in ``range(partition_count)`` with ``p % replica_count
    == ordinal`` (millpond's rule verbatim)."""
    return tuple(p for p in range(partition_count) if p % replica_count == ordinal)


def classify_message(
    keyer: GroupKeyer,
    *,
    offset: int,
    value: bytes | None,
    partition: int = 0,
) -> StagedRecord | DroppedRecord:
    """One record → the row to stage under its partition tuple, or the
    drop with its reason. Total over record content. The Kafka key and
    timestamp are never read."""
    if offset < 0 or offset > _INT64_MAX:
        return DroppedRecord(offset, REASON_INVALID_OFFSET)
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
    return StagedRecord(group=group, offset=offset, payload=value, partition=partition)


def drop_reason_label(reason: str) -> str:
    """The metric label: ``malformed_partition_source:<path>`` collapses
    to ``malformed_partition_source`` (bounded cardinality)."""
    head, sep, _path = reason.partition(":")
    return head if sep and head == REASON_MALFORMED_PARTITION_SOURCE else reason


class ShapeSource(Protocol):
    """The destination shape the consumer re-reads to refresh its keyer
    (``flush.ShapeReader``). Both calls block."""

    def read(self) -> TableInfo: ...
    def ttl_s(self) -> float: ...


@dataclass(frozen=True, slots=True)
class ConsumerStats:
    messages_consumed: int
    records_staged: int
    records_dropped: Mapping[str, int]
    kafka_error_events: int
    commit_attempts: int
    commit_retries: int


@dataclass(slots=True)
class _Classified:
    """One poll, classified (built on a worker thread)."""

    staged: dict[tuple[str, int], list[StagedRecord]] = field(default_factory=dict)
    last_handled: dict[tuple[str, int], int] = field(default_factory=dict)
    drops: list[tuple[str, int, DroppedRecord]] = field(default_factory=list)
    error_events: list[str] = field(default_factory=list)
    records: int = 0
    nbytes: int = 0


def _classify_poll(keyer: GroupKeyer, msgs: Sequence[MessageView]) -> _Classified:
    out = _Classified()
    for msg in msgs:
        err = msg.error()
        if err is not None:
            # No stageable record; never committed past explicitly.
            out.error_events.append(
                "" if err.code() == KafkaError._PARTITION_EOF else str(err)
            )
            continue
        topic, partition, offset = msg.topic(), msg.partition(), msg.offset()
        if topic is None or partition is None or offset is None:
            out.error_events.append(
                f"record without coordinates: topic={topic!r} "
                f"partition={partition!r} offset={offset!r}"
            )
            continue
        key = (topic, partition)
        raw_key, raw_value = msg.key(), msg.value()
        outcome = classify_message(
            keyer, offset=offset, value=raw_value, partition=partition
        )
        out.records += 1
        out.nbytes += (len(raw_key) if raw_key else 0) + (
            len(raw_value) if raw_value else 0
        )
        out.last_handled[key] = max(
            outcome.offset, out.last_handled.get(key, outcome.offset)
        )
        if isinstance(outcome, DroppedRecord):
            out.drops.append((topic, partition, outcome))
        else:
            out.staged.setdefault(key, []).append(outcome)
    return out


def kafka_consumer_config(cfg: Config) -> dict[str, Any]:
    """The librdkafka config: auto-commit and auto-offset-store off
    (offsets move only through the loop's commits, after durable
    staging), no group membership, the fetch defaults, then the
    ``MILLRACE_KAFKA_CONSUMER_*`` passthrough applied last."""
    config: dict[str, Any] = {
        "bootstrap.servers": cfg.kafka_bootstrap_servers,
        "client.id": f"millrace-{cfg.kafka_topic}-{cfg.ordinal}",
        "group.id": cfg.kafka_group_id,
        "auto.offset.reset": cfg.kafka_auto_offset_reset.value,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        **KAFKA_FETCH_DEFAULTS,
    }
    config.update(cfg.kafka_consumer_overrides)
    return config


def create_kafka_consumer(cfg: Config) -> KafkaConsumerLike:
    return _KafkaConsumer(kafka_consumer_config(cfg))


class MillraceConsumer:
    """The consume loop for one topic → one table pipeline, over a
    caller-owned :class:`~millrace.stage.StageManager` it shares with the
    flush loop."""

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
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="millrace-kafka"
        )
        self._started = False
        self._start_error: BaseException | None = None
        self._stopping = False
        self._closed = False
        self._observed_records = 0
        self._observed_bytes = 0
        self._messages_consumed = 0
        self._records_staged = 0
        self._records_dropped: dict[str, int] = {}
        self._drop_log_at: dict[tuple[str, int, str], float] = {}
        self._drop_log_suppressed: dict[tuple[str, int, str], int] = {}
        self._kafka_error_events = 0
        self._commit_attempts = 0
        self._commit_retries = 0
        self._committed: dict[tuple[str, int], int] = {}

    # -- lifecycle -------------------------------------------------------------

    async def start(self, drain: Callable[[], Awaitable[None]] | None = None) -> None:
        """Claim the assignment, open its stages, run ``drain`` (the
        startup drain: everything already staged is flushed before any
        new record is consumed), then assign. Idempotent; a failed start
        is sticky."""
        if self._started:
            return
        if self._start_error is not None:
            raise self._start_error
        if self._closed:
            raise RuntimeError("consumer is closed")
        try:
            partitions = await self._claim()
            if drain is not None:
                await drain()
            if self._stopping:
                # Stopped during the drain: never assign.
                return
            await self._assign(partitions)
        except BaseException as exc:
            self._start_error = exc
            raise
        self._started = True

    async def _claim(self) -> tuple[int, ...]:
        cfg = self._cfg
        partition_count = await self._discover_partition_count()
        partitions = compute_assignment(partition_count, cfg.replica_count, cfg.ordinal)
        if not partitions:
            raise AssignmentError(
                f"no partitions for ordinal {cfg.ordinal}: {cfg.kafka_topic} has "
                f"{partition_count} partition(s) and MILLRACE_REPLICA_COUNT="
                f"{cfg.replica_count}"
            )
        home = partitions[0]
        # Every slot this ordinal may have to drain: the homes of replicas
        # a scale-down removed. Each is probed for a SlateDB manifest.
        limit = max(partition_count, STAGE_SLOT_SCAN_LIMIT)
        existing = await self._stages.existing_slots(
            cfg.kafka_topic, range(cfg.ordinal, limit, cfg.replica_count)
        )
        drain = [s for s in existing if s != home]
        log.info(
            "assignment: ordinal %d of %d replicas over %d partitions of %s → %s; "
            "home stage slot %d, draining slots %s",
            cfg.ordinal,
            cfg.replica_count,
            partition_count,
            cfg.kafka_topic,
            list(partitions),
            home,
            drain,
        )
        await self._stages.claim(cfg.kafka_topic, home, drain)
        return partitions

    async def _assign(self, partitions: tuple[int, ...]) -> None:
        topic = self._cfg.kafka_topic
        await self._await_offset_storage_ready(
            [TopicPartition(topic, p) for p in partitions]
        )
        self._kafka.assign(
            [TopicPartition(topic, p, OFFSET_STORED) for p in partitions]
        )
        log.info("assigned %s: %s", topic, list(partitions))

    async def _discover_partition_count(self) -> int:
        """The topic's partition count from cluster metadata. Transport
        errors and LEADER_NOT_AVAILABLE are retried; an unknown or
        errored topic, or zero partitions, is an :class:`AssignmentError`."""
        topic = self._cfg.kafka_topic
        for attempt in range(1, _METADATA_MAX_ATTEMPTS + 1):
            transient: BaseException | str
            try:
                metadata = await self._call_kafka(
                    partial(self._kafka.list_topics, topic, timeout=_METADATA_TIMEOUT_S)
                )
            except KafkaException as exc:
                transient = exc
            else:
                topic_meta = metadata.topics.get(topic)
                if topic_meta is None:
                    raise AssignmentError(f"topic {topic!r} is unknown to the cluster")
                err = topic_meta.error
                if err is None:
                    count = len(topic_meta.partitions)
                    if count == 0:
                        raise AssignmentError(f"topic {topic!r} reports no partitions")
                    return count
                if err.code() != KafkaError.LEADER_NOT_AVAILABLE:
                    raise AssignmentError(f"topic {topic!r}: {err}")
                transient = f"topic {topic!r}: {err}"
            if attempt == _METADATA_MAX_ATTEMPTS:
                if isinstance(transient, BaseException):
                    raise transient
                raise KafkaException(
                    f"cluster metadata for {topic!r} never settled after "
                    f"{attempt} attempts ({transient})"
                )
            log.info(
                "waiting for cluster metadata (attempt %d/%d): %s",
                attempt,
                _METADATA_MAX_ATTEMPTS,
                transient,
            )
            await self._sleep(_METADATA_RETRY_S)
        raise AssertionError("unreachable")  # pragma: no cover

    async def run(self) -> None:
        """Consume until :meth:`stop`; closes the Kafka side on the way
        out."""
        await self.start()
        try:
            while not self._stopping:
                await self._step()
        finally:
            await self.close()

    def stop(self) -> None:
        """Stop after the in-flight poll cycle. Safe from a signal handler."""
        self._stopping = True

    async def close(self) -> None:
        """Close the Kafka consumer and its worker thread. Idempotent. The
        StageManager is caller-owned."""
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

    @property
    def started(self) -> bool:
        return self._started

    def committed_offsets(self) -> Mapping[tuple[str, int], int]:
        return dict(self._committed)

    @property
    def keyer(self) -> GroupKeyer:
        return self._keyer

    def stats(self) -> ConsumerStats:
        return ConsumerStats(
            messages_consumed=self._messages_consumed,
            records_staged=self._records_staged,
            records_dropped=dict(self._records_dropped),
            kafka_error_events=self._kafka_error_events,
            commit_attempts=self._commit_attempts,
            commit_retries=self._commit_retries,
        )

    # -- the loop ----------------------------------------------------------------

    async def _call_kafka(self, fn: Callable[[], _T]) -> _T:
        return await asyncio.get_running_loop().run_in_executor(self._executor, fn)

    async def _await_offset_storage_ready(self, partitions: list[Any]) -> None:
        """Probe ``committed()`` until the group coordinator answers. An
        OFFSET_STORED assign whose one OffsetFetch lands during a fresh
        broker's ``__consumer_offsets`` election sits assigned but never
        fetching; a broker that keeps refusing fails startup loudly."""
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
        """One poll → stage → commit cycle. The poll's message count is
        the record cap clamped by the byte cap ÷ the observed mean record
        size: librdkafka counts messages only, and every returned message
        must be staged before its offset commits."""
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
        if msgs:
            self._messages_consumed += len(msgs)
            await self._stage_and_commit(msgs)

    async def _maybe_refresh_keyer(self) -> None:
        """Re-read the destination shape once its TTL has passed and swap
        in a fresh keyer. A failed read keeps the old keyer; a
        :class:`~millrace.flush.FlushHalted` (table recreated) propagates
        before the poll, so nothing more is staged or committed."""
        shape = self._shape
        if shape is None:
            return
        loop = asyncio.get_running_loop()
        now = self._monotonic()
        if self._next_shape_read is None:
            try:
                self._shape_ttl_s = await loop.run_in_executor(None, shape.ttl_s)
            except Exception:
                log.warning(
                    "reading the shape TTL failed; retrying next poll", exc_info=True
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
                "destination shape re-read failed; keeping partition spec %s",
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
                    "stage under the new tuples",
                    self._keyer.spec_id,
                    keyer.spec_id,
                    list(keyer.fields),
                )
            self._keyer = keyer
        assert self._shape_ttl_s is not None
        self._next_shape_read = self._monotonic() + self._shape_ttl_s

    async def _stage_and_commit(self, msgs: Sequence[MessageView]) -> None:
        """Classify off the loop, stage the poll's records (every
        partition) in ONE durable write to the home stage, then commit
        ``last handled + 1`` per partition. A poll whose records were all
        dropped gets a durable fence probe in place of the batch."""
        now_us = self._now_us()
        poll = await asyncio.to_thread(_classify_poll, self._keyer, msgs)
        for text in poll.error_events:
            self._kafka_error_events += 1
            if text:
                log.warning("kafka error event: %s", text)
        self._observed_records += poll.records
        self._observed_bytes += poll.nbytes
        for topic, partition, dropped in poll.drops:
            self._note_drop(topic, partition, dropped)

        if not poll.last_handled:
            return
        home = self._stages.home_stage()
        staged = [rec for key in sorted(poll.staged) for rec in poll.staged[key]]
        try:
            if staged:
                ack = await home.stage_batch(staged, now_us=now_us)
                self._records_staged += ack.rows
            else:
                # Every handled record was dropped: a durable fence probe
                # stands in for the batch. Its value is forensic only (the
                # poll's highest handled offset, any partition).
                await home.probe_fence(max(poll.last_handled.values()))
        except Exception as e:
            if not is_fenced(e):
                raise
            key = (home.topic, home.partition)
            await self._relinquish_fenced([key])
            raise FencedError([key]) from e
        offsets = [
            TopicPartition(topic, partition, last + 1)
            for (topic, partition), last in sorted(poll.last_handled.items())
        ]
        await self._commit_offsets(offsets)
        for key, last in poll.last_handled.items():
            self._committed[key] = last + 1

    def _note_drop(self, topic: str, partition: int, dropped: DroppedRecord) -> None:
        """Count a drop; log at most one line per (partition, reason) per
        :data:`DROP_LOG_INTERVAL_S`, carrying the suppressed count."""
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
        """Close fenced stages locally so this process never writes the
        contested paths again."""
        for topic, partition in partitions:
            try:
                await self._stages.close_partition(topic, partition)
            except Exception:
                log.warning(
                    "local close of fenced stage %s[%d] failed",
                    topic,
                    partition,
                    exc_info=True,
                )
            log.error("FENCED: %s[%d] has a newer writer; halting", topic, partition)

    async def _commit_offsets(self, offsets: list[Any]) -> None:
        """Synchronous offset commit with a small retry ladder; exhaustion
        raises (rows are durable, the restart replays)."""
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
                        "offset commit failed after %d attempts; offsets "
                        "unadvanced, the restart replays",
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
