"""Kafka consumption and offset management (docs/kafka-ingestion.md §The
problem's consume loop).

Poll → stage → commit: every polled record is staged through
:meth:`millrace.stage.PartitionStage.stage_batch` — under
``rows/(team_id, event_ts, offset)`` for well-formed records, under
``poison/<offset>`` for records that can never become a row — and the
partition's Kafka offset commits STRICTLY after the batch's remote
durability (rows-then-offset, AGENT.md invariant 10 restated for a
staging area). The consume loop never waits on the flush loop: staging
is the buffer. There is no in-memory accumulation — every consumed
record is either durable-and-committed or, on a crash, replayed from the
last commit (at-least-once by construction). The ``team_id`` a record
stages under comes from the configured codec
(``MILLRACE_TEAM_KEY_CODEC``): the message key (``utf8-decimal`` /
``be64``, team-keyed topics) or a field of the JSON payload
(``value-json:<field>``, topics not keyed by team — every partition's
instance can then hold every team; :func:`classify_message`).

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
loop therefore catches FENCED at the staging write (and at the
backpressure gauge scan, where an idle partition's fence would surface),
closes the stage locally so this process can never touch the path
again, and raises :class:`ConsumerFencedError` — a halt DISTINCT from a
config error or a flush halt, so the supervisor can tell "another owner
holds this path" apart from a crash and never blindly restart this pod
back onto the same assignment (a restarted fenced pod re-opens the path
and fences the NEW owner: the flap this distinction exists to prevent).
See the README's rollout-barrier paragraph.

Backpressure is a two-level policy over the StageManager's
stats-derived gauges, each level a PURE latch
(:func:`backpressure_target`, millpond's ``_liveness_status`` pattern —
testable without I/O) with hysteresis (pause AT the high water, resume
STRICTLY BELOW the low water, hold inside the band):

- per partition, the AGE arm: a partition whose oldest staged byte ages
  past ``MILLRACE_BACKPRESSURE_PAUSE_AGE_S`` has a stuck flusher, and
  pauses ALONE — the other partitions keep flowing (a single stuck
  partition must never stall the pod). Byte pressure has no
  per-partition arm: the byte thresholds are pod-shaped, so one
  partition's bytes crossing them implies the aggregate already
  tripped — a per-partition byte arm could never be the first to fire.
- pod-wide, the BYTE arm: the AGGREGATE staged bytes crossing
  ``MILLRACE_BACKPRESSURE_PAUSE_BYTES`` is the pod's flush-debt ceiling
  (a different, legitimate limit — total staged volume is the catch-up
  working set this pod is owed), and pauses every assigned partition.

A partition pauses when either level says so and resumes only when both
clear; transitions only, computed as a set diff, so each pause/resume
fires exactly once per crossing, and a partition assigned while the pod
ceiling is engaged starts paused. The gauge read goes through the
sweep's shared snapshot when it is fresh
(``StageManager.gauges(max_staleness_s=2 × flush_sweep_s)`` — M6's
one-scan-per-sweep sharing, whose published fold carries the
per-partition gauges this policy reads) and falls back to a live scan
when the flush loop stops publishing, so backpressure never reads frozen
numbers while staging continues.

Poison pills are quarantined, counted and logged, never silently
dropped: they land in the same durable write batch as the cycle's staged
rows, the commit covers them, and the loop moves on. A FLOOD of poison
(records quarantined this run exceeding the configured bound) means a
misconfigured key codec or a producer bug, and halts the loop loudly —
before committing past the flood — via :class:`PoisonLimitExceeded`.

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
import json
import logging
import threading
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
from slatedb.uniffi import CloseReason
from slatedb.uniffi import Error as SlateError

from .config import AssignmentMode, BackpressureConfig, Config, TeamKeyCodec
from .stage import PoisonedRecord, StagedRecord, StageManager

log = logging.getLogger(__name__)

_T = TypeVar("_T")

_MICROS_PER_SECOND: Final = 1_000_000
_MICROS_PER_MILLI: Final = 1_000
_INT64_MIN: Final = -(1 << 63)
_INT64_MAX: Final = (1 << 63) - 1
_UINT64_MAX: Final = (1 << 64) - 1

# The bounded poison rejection vocabulary (the keyspace poison envelope
# stores these strings).
REASON_MISSING_KEY: Final = "missing_key"
REASON_MALFORMED_KEY: Final = "malformed_key"
REASON_INVALID_OFFSET: Final = "invalid_offset"
REASON_MISSING_TIMESTAMP: Final = "missing_timestamp"
REASON_INVALID_TIMESTAMP: Final = "invalid_timestamp"
REASON_MISSING_VALUE: Final = "missing_value"
# The value-json codec's additions: the payload is not a JSON object,
# the configured team field is absent/null in it, or the field's value
# is not a u64 integer (a string, a float, a bool, negative, > u64 max —
# mistypes are never coerced: coercion hides producer bugs).
REASON_MALFORMED_VALUE: Final = "malformed_value"
REASON_MISSING_TEAM_FIELD: Final = "missing_team_field"
REASON_MALFORMED_TEAM_FIELD: Final = "malformed_team_field"

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


class PoisonLimitExceeded(Exception):
    """The quarantine outgrew its bound. A flood of unstageable records
    means a misconfigured key codec or a producer bug, and committing
    past it would hide the damage — so the loop halts loudly WITHOUT
    committing past the flood, and the restart replays it and trips
    again until the cause is fixed."""


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
    names the fenced set; when the fence surfaced at the aggregate
    gauge read (an idle partition's fence has no write to raise from)
    the exact partition is unknowable there and the whole held set is
    named. The offsets of the failed batch are never committed — the
    rows either landed (and the replay re-puts them idempotently) or
    did not (and the replay restages them).
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
    def pause(self, partitions: list[Any]) -> None: ...
    def resume(self, partitions: list[Any]) -> None: ...
    def close(self) -> None: ...


def decode_team_key(codec: TeamKeyCodec, raw: bytes) -> int:
    """Decode a Kafka message key to ``team_id`` under the configured
    codec. Raises ValueError naming the codec and the problem; the
    caller quarantines. :attr:`TeamKeyCodec.VALUE_JSON` has no key
    decoding — the team id comes from the payload
    (:func:`decode_team_value`) — and raises here."""
    if codec is TeamKeyCodec.UTF8_DECIMAL:
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError(f"utf8-decimal team key is not UTF-8: {exc}") from exc
        # int() alone would accept whitespace, signs and underscores.
        if not text or any(c < "0" or c > "9" for c in text):
            raise ValueError(f"utf8-decimal team key is not decimal: {text!r}")
        value = int(text)
        if value > _UINT64_MAX:
            raise ValueError(f"team_id {value} exceeds the u64 key space")
        return value
    if codec is TeamKeyCodec.BE64:
        if len(raw) != 8:
            raise ValueError(f"be64 team key must be 8 bytes, got {len(raw)}")
        return int.from_bytes(raw, "big")
    raise ValueError(f"unsupported team key codec: {codec!r}")


class TeamFieldError(ValueError):
    """A ``value-json`` refusal, carrying the bounded quarantine reason
    for the poison envelope (the detail message is for the logs)."""

    def __init__(self, reason: str, detail: str) -> None:
        self.reason = reason
        super().__init__(detail)


def decode_team_value(value: bytes, field: str) -> int:
    """The ``value-json`` codec: parse the payload JSON ONCE and read the
    team id from the top-level ``field``.

    Raises :class:`TeamFieldError` (a ValueError) carrying the quarantine
    reason: ``malformed_value`` (not JSON, or JSON that is not an
    object), ``missing_team_field`` (the field is absent or null — the
    operator's misconfigured-field flood), ``malformed_team_field``
    (present but not a u64 integer; ``bool`` IS ``int`` in Python and is
    excluded explicitly, and floats/strings are never coerced — coercion
    hides producer bugs).
    """
    try:
        parsed: Any = json.loads(value)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TeamFieldError(
            REASON_MALFORMED_VALUE, f"value-json payload is not JSON: {exc}"
        ) from exc
    if not isinstance(parsed, dict):
        raise TeamFieldError(
            REASON_MALFORMED_VALUE,
            f"value-json payload is {type(parsed).__name__}, not a JSON object",
        )
    raw = parsed.get(field)
    if raw is None:
        raise TeamFieldError(
            REASON_MISSING_TEAM_FIELD,
            f"value-json payload lacks the team field {field!r}",
        )
    if type(raw) is not int:
        raise TeamFieldError(
            REASON_MALFORMED_TEAM_FIELD,
            f"value-json team field {field!r} is {type(raw).__name__}, not an integer",
        )
    if raw < 0 or raw > _UINT64_MAX:
        raise TeamFieldError(
            REASON_MALFORMED_TEAM_FIELD,
            f"value-json team field {field!r} value {raw} is outside u64",
        )
    return raw


def classify_message(
    codec: TeamKeyCodec,
    *,
    offset: int,
    key: bytes | None,
    value: bytes | None,
    timestamp: tuple[int, int],
    poison_value_max_bytes: int,
    value_field: str | None = None,
) -> StagedRecord | PoisonedRecord:
    """One consumed record's fields → the record to stage, or the poison
    quarantine entry.

    TOTAL over record content — a malformed record routes to the
    quarantine with a reason (the first failing check names it), so the
    loop never wedges on one record and never silently drops one. A
    poison entry's value is truncated to ``poison_value_max_bytes`` with
    the original length recorded. ``timestamp`` is the Kafka ``(type,
    milliseconds)`` pair; TIMESTAMP_NOT_AVAILABLE can never key a row, so
    it quarantines.

    The team extraction follows the codec: the keyed codecs read the
    message key (a missing key is poison); ``value-json`` ignores the
    key entirely and reads ``value_field`` from the payload's JSON
    (parsed once), so a topic not keyed by team stages fine — and a
    payload lacking or mistyping the field is the poison. A VALUE_JSON
    codec without ``value_field`` is a configuration bug (Config refuses
    the pair at construction) and raises rather than quarantining.
    """

    def poison(reason: str) -> PoisonedRecord:
        stored = value if value is None else value[:poison_value_max_bytes]
        return PoisonedRecord(
            offset=offset,
            reason=reason,
            key=key,
            value=stored,
            value_bytes_original=len(value) if value is not None else 0,
        )

    if offset < 0 or offset > _INT64_MAX:
        return poison(REASON_INVALID_OFFSET)
    keyed_team_id: int | None = None
    if codec is TeamKeyCodec.VALUE_JSON:
        if not value_field:
            raise ValueError("classify_message: value-json codec requires value_field")
    elif key is None:
        return poison(REASON_MISSING_KEY)
    else:
        try:
            keyed_team_id = decode_team_key(codec, key)
        except ValueError:
            return poison(REASON_MALFORMED_KEY)
    ts_type, ts_ms = timestamp
    if ts_type == TIMESTAMP_NOT_AVAILABLE:
        return poison(REASON_MISSING_TIMESTAMP)
    event_ts_us = ts_ms * _MICROS_PER_MILLI
    if not _INT64_MIN <= event_ts_us <= _INT64_MAX:
        return poison(REASON_INVALID_TIMESTAMP)
    if value is None:
        return poison(REASON_MISSING_VALUE)
    team_id: int
    if codec is TeamKeyCodec.VALUE_JSON:
        assert value_field is not None  # the same check above
        try:
            team_id = decode_team_value(value, value_field)
        except TeamFieldError as exc:
            return poison(exc.reason)
    else:
        assert keyed_team_id is not None  # the key path returned otherwise
        team_id = keyed_team_id
    return StagedRecord(
        team_id=team_id, event_ts_us=event_ts_us, offset=offset, payload=value
    )


def backpressure_target(
    *,
    staged_bytes: int,
    oldest_first_staged_ts: int | None,
    now_us: int,
    currently_paused: bool,
    knobs: BackpressureConfig,
) -> bool:
    """The pause decision as a pure function of a gauge snapshot, an
    injected clock and the current state (millpond's ``_liveness_status``
    pattern — testable without I/O).

    Latch with hysteresis: TRIP (→ paused) when total staged bytes reach
    the high water or the oldest staged byte is at least the pause age;
    CLEAR (→ resumed) only when bytes are strictly below the low water
    AND the oldest age is strictly below the resume age (or nothing is
    staged); HOLD inside the band. Strictly-positive bands are a config
    invariant (BackpressureConfig), so a fixed snapshot has a stable
    answer and a monotone gauge sweep transitions at most once per
    direction — no flapping, by construction.
    """
    age_us = None if oldest_first_staged_ts is None else now_us - oldest_first_staged_ts
    trip = staged_bytes >= knobs.pause_staged_bytes or (
        age_us is not None and age_us >= knobs.pause_oldest_age_s * _MICROS_PER_SECOND
    )
    clear = staged_bytes < knobs.resume_staged_bytes and (
        age_us is None or age_us < knobs.resume_oldest_age_s * _MICROS_PER_SECOND
    )
    return trip or (currently_paused and not clear)


@dataclass(frozen=True, slots=True)
class ConsumerStats:
    """Cumulative counters for the run so far (the operational surface's
    input; passive observability — nothing here feeds control flow).
    ``commit_attempts`` counts every commit RPC including retried ones;
    ``commit_retries`` counts only the retried ones."""

    messages_consumed: int
    records_staged: int
    records_poisoned: int
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

    ``now_us`` is the injected clock for staging timestamps and the age
    gauge; ``sleep`` backs the commit retry ladder (both injectable so
    tests never sleep).
    """

    def __init__(
        self,
        kafka: KafkaConsumerLike,
        stages: StageManager,
        config: Config,
        *,
        now_us: Callable[[], int],
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._kafka = kafka
        self._stages = stages
        self._cfg = config
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
        # Two-level backpressure state (module docstring): the pod-wide
        # byte-ceiling latch, the per-partition age latches, and the
        # pause set actually applied to librdkafka (the union, pruned to
        # the current assignment).
        self._pod_paused = False
        self._age_paused: set[tuple[str, int]] = set()
        self._applied_pause: set[tuple[str, int]] = set()
        # Running observation of record sizes (key + value bytes over
        # classified records) feeding the per-poll byte cap — see _step.
        self._observed_records = 0
        self._observed_bytes = 0
        self._poisoned_total = 0
        self._messages_consumed = 0
        self._records_staged = 0
        self._records_poisoned = 0
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
            "consumer closed (%d messages, %d staged, %d poisoned, %d commits)",
            self._messages_consumed,
            self._records_staged,
            self._records_poisoned,
            self._commit_attempts,
        )

    # -- introspection ---------------------------------------------------------

    @property
    def paused(self) -> bool:
        """Whether the loop currently holds any assigned partition
        paused (a per-partition age latch or the pod-wide byte ceiling —
        the two-level policy in the module docstring)."""
        return bool(self._applied_pause)

    def paused_partitions(self) -> frozenset[tuple[str, int]]:
        """The (topic, partition) set currently held paused."""
        return frozenset(self._applied_pause)

    def committed_offsets(self) -> Mapping[tuple[str, int], int]:
        """The last successfully committed offset per partition this run."""
        return dict(self._committed)

    def stats(self) -> ConsumerStats:
        """A snapshot of the run's counters."""
        return ConsumerStats(
            messages_consumed=self._messages_consumed,
            records_staged=self._records_staged,
            records_poisoned=self._records_poisoned,
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
        commit → backpressure. Assignment changes land BEFORE the poll's
        messages are staged, so a partition revoked mid-poll is closed
        first and its just-fetched messages are dropped locally (the new
        owner replays them from the last commit — zero loss).

        The poll's message count is the record cap clamped by the byte
        cap: ``batch_max_bytes ÷ observed average record size`` (the
        run's cumulative mean, key + value bytes of classified records).
        librdkafka's ``consume()`` counts messages only, and every
        message it returns must be staged before its offset can commit —
        so the byte bound has to shape what we ASK for, never drop what
        we got. The floor is 1: a record larger than the whole budget
        still flows, one per poll. The first poll has no observation and
        polls up to the record cap."""
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
        await self._apply_backpressure()

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
        if sync.closed:
            # The revoke itself unpauses broker-side; drop the latches
            # and the applied state so a later reclaim starts clean.
            self._age_paused.difference_update(sync.closed)
            self._applied_pause.difference_update(sync.closed)
        if self._pod_paused and sync.opened:
            # A partition assigned while the pod byte ceiling is engaged
            # inherits the pause — it must not start fetching. (The age
            # latches are per partition and start clear: the first gauge
            # read, later in this same step, latches a reclaimed
            # partition whose staged state is stale.)
            newly = sorted(sync.opened)
            self._kafka.pause([TopicPartition(t, p) for t, p in newly])
            self._applied_pause.update(newly)
            log.info(
                "backpressure: %d newly assigned partition(s) start paused",
                len(newly),
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
        poisoned: dict[tuple[str, int], list[PoisonedRecord]] = {}
        last_handled: dict[tuple[str, int], int] = {}
        cycle_poison = 0
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
                # quarantined (the poison key IS the offset). Counted,
                # logged, never committed past explicitly.
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
                self._cfg.team_key_codec,
                offset=offset,
                key=raw_key,
                value=raw_value,
                timestamp=msg.timestamp(),
                poison_value_max_bytes=self._cfg.poison.value_max_bytes,
                value_field=self._cfg.team_id_field,
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
            if isinstance(outcome, PoisonedRecord):
                cycle_poison += 1
                poisoned.setdefault(key, []).append(outcome)
                log.warning(
                    "poison record quarantined: %s[%d]@%d reason=%s",
                    key[0],
                    key[1],
                    outcome.offset,
                    outcome.reason,
                )
            else:
                staged.setdefault(key, []).append(outcome)

        if (
            cycle_poison
            and self._poisoned_total + cycle_poison
            > self._cfg.poison.max_records_per_run
        ):
            raise PoisonLimitExceeded(
                f"poison limit exceeded: "
                f"{self._poisoned_total + cycle_poison} quarantined records "
                f"this run would exceed "
                f"MILLRACE_POISON_MAX_RECORDS={self._cfg.poison.max_records_per_run}; "
                f"a flood of unstageable records means a misconfigured team "
                f"key codec or a producer bug — halting WITHOUT committing "
                f"past the flood"
            )

        now_us = self._now_us()
        keys = sorted(staged.keys() | poisoned.keys())
        # One task per partition: the WAL PUTs and durability waits are
        # in flight together; gather (return_exceptions) settles EVERY
        # batch before the outcome is examined, so a failure never
        # strands a sibling's in-flight write and the counters reflect
        # whatever did land durably.
        tasks = [
            asyncio.create_task(
                self._stages.stage(*key).stage_batch(
                    staged.get(key, []), poison=poisoned.get(key, []), now_us=now_us
                ),
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
            self._records_poisoned += result.poisoned
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
        self._poisoned_total += cycle_poison

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

    async def _apply_backpressure(self) -> None:
        """Evaluate the gauges and apply the two-level pause policy
        (module docstring): the per-partition AGE latches (a stuck flush
        stalls one partition, never the pod) and the pod-wide BYTE
        ceiling (the aggregate flush-debt limit). Transitions only,
        computed as a set diff, so each pause/resume fires exactly once
        per crossing.

        The gauges come from the sweep's SHARED snapshot when it is
        fresh (M6: one ``stats/`` scan per partition per sweep serves
        planner, consumer and scrape): the flush sweep publishes its
        fold — per-partition gauges included — every
        ``MILLRACE_FLUSH_SWEEP_S`` seconds, and this read accepts it
        while it is younger than two sweeps. An overdue snapshot (a
        halted or wedged flush loop) falls back to a live scan —
        backpressure must never read frozen numbers while the consumer
        keeps staging.
        """
        try:
            gauges = await self._stages.gauges(
                max_staleness_s=2.0 * self._cfg.flush_sweep_s
            )
        except SlateError.Closed as exc:
            if not _is_fenced(exc):
                raise
            # An idle partition's fence has no write to surface at; the
            # gauge scan is where it shows. The aggregate read cannot
            # name the partition, so the held set is named whole — the
            # process is exiting regardless.
            held = self._stages.partitions()
            await self._relinquish_fenced(held)
            raise ConsumerFencedError(held) from exc
        now_us = self._now_us()
        knobs = self._cfg.backpressure
        assigned = set(self._stages.partitions())
        # Revocation bookkeeping normally keeps this pruned; intersect
        # again so a stage that vanished any other way (a fenced close)
        # can never be resumed into fetching.
        self._applied_pause.intersection_update(assigned)

        # Pod level: BYTES ONLY. The aggregate's oldest-age is
        # deliberately not consulted — the age arm is the per-partition
        # latches' job (one partition's stuck flush must not pause the
        # other 63).
        pod = backpressure_target(
            staged_bytes=gauges.staged_bytes,
            oldest_first_staged_ts=None,
            now_us=now_us,
            currently_paused=self._pod_paused,
            knobs=knobs,
        )
        # Per partition: the AGE arm only, over that partition's own
        # gauges (staged_bytes=0 keeps the byte arm inert — the byte
        # thresholds are pod-shaped, so a per-partition byte trip would
        # imply the aggregate already tripped). A partition missing from
        # the snapshot (assigned after the sweep published) reads as
        # empty and unlatched; the live fallback covers it next step.
        age_latched: set[tuple[str, int]] = set()
        for key in sorted(assigned):
            per = gauges.per_partition.get(key)
            if backpressure_target(
                staged_bytes=0,
                oldest_first_staged_ts=(
                    None if per is None else per.oldest_first_staged_ts
                ),
                now_us=now_us,
                currently_paused=key in self._age_paused,
                knobs=knobs,
            ):
                age_latched.add(key)

        newly_latched = age_latched - self._age_paused
        newly_cleared = self._age_paused - age_latched
        if pod != self._pod_paused:
            if pod:
                log.warning(
                    "backpressure: pod byte ceiling reached — pausing every "
                    "assigned partition (staged_bytes=%d >= %d)",
                    gauges.staged_bytes,
                    knobs.pause_staged_bytes,
                )
            else:
                log.info(
                    "backpressure: pod byte ceiling cleared — the per-"
                    "partition latches decide from here (staged_bytes=%d)",
                    gauges.staged_bytes,
                )
        for topic, partition in newly_latched:
            log.warning(
                "backpressure: paused %s[%d] — its oldest staged byte "
                "reached the pause age (a stuck flush stalls this "
                "partition alone)",
                topic,
                partition,
            )
        for topic, partition in newly_cleared:
            log.info("backpressure: %s[%d] age latch cleared", topic, partition)
        self._pod_paused = pod
        self._age_paused = age_latched

        desired = age_latched | (assigned if pod else set())
        to_pause = sorted(desired - self._applied_pause)
        to_resume = sorted(self._applied_pause - desired)
        if to_pause:
            self._kafka.pause([TopicPartition(t, p) for t, p in to_pause])
        if to_resume:
            self._kafka.resume([TopicPartition(t, p) for t, p in to_resume])
        self._applied_pause = desired
