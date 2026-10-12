"""Staging key codecs (docs/kafka-ingestion.md §Staging layout).

    rows/<team_id><timestamp><offset>          → event payload bytes
    stats/<team_id>                            → StatsValue
    sched_age/<first_staged_ts><team_id>       → ∅ (empty value)
    offsets/<team_id>                          → sorted (topic, partition, first, last) ranges
    prepared/<team_id><first_offset>           → PreparedRequest envelope
    flushed/<team_id><first_offset><last_offset> → LEGACY: never written
    poison/<offset>                            → PoisonValue envelope

Topology: ONE SlateDB instance per claimed Kafka partition
(``s3://…/millrace/{topic}/{partition}``). The keyspace therefore
carries NO partition component — the instance *is* the partition, the
consumer assignment is the claim, and staged state follows the
partition to its new owner, never the pod. The ``offsets/`` side prefix
keeps ``(topic, partition, first_offset, last_offset)`` per staged
range as the flush's identity material, not as row-key material.

The LSM's ordering is load-bearing: ``rows`` key bytes must sort in
(team, timestamp, offset) order so a flush reads one contiguous, nearly
sorted range, and ``sched_age`` bytes must sort in (first_staged_ts,
team_id) order so the readiness sweep is a prefix range scan.

Encoding rules
--------------

- All multi-byte integers are big-endian and fixed width, so no field
  needs a separator.
- Signed values IN KEYS (event timestamp, Kafka offset,
  first_staged_ts) use offset binary: ``value + 2**63`` encoded as an
  unsigned 8-byte big-endian integer. This maps int64 monotonically
  onto uint64 (INT64_MIN encodes to zero bytes), so lexicographic byte
  order equals numeric order across the whole range, negatives
  included. Plain two's complement would sort every negative after
  every non-negative and break the sort property above.
- Signed values inside VALUE envelopes use plain two's complement;
  values have no ordering requirement.
- ``team_id`` is a non-negative integer, encoded unsigned 8-byte
  big-endian, which sorts numerically on its own.
- Every value envelope starts with a 1-byte version. Stored payloads
  outlive the code that wrote them; a reader refuses any version it does
  not know and keeps decoding the ones it does (``prepared`` is at
  version 3; versions 1 and 2 still decode).

Value formats
-------------

- ``stats``:   version(1) staged_bytes(u64) first_staged_ts(i64)
               last_staged_ts(i64) row_count(u64) — 33 bytes fixed.
- ``offsets``: version(1) count(u16), then count records of
               topic_len(u16) topic(utf8) partition(u32)
               first_offset(i64) last_offset(i64). Ranges are encoded
               sorted by (topic, partition, first_offset, last_offset)
               so the same set encodes to identical bytes regardless of
               assembly order.
- ``prepared``: version(1) key_len(u16) idempotency_key(utf8)
               [v3 only: topic_len(u16) topic(utf8) partition(u32)]
               body_len(u32) body(bytes) persisted_at(u64, micros since
               the Unix epoch — v2 and up). The body is the byte-exact
               prepared commit request, written once and replayed
               byte-identically after an ambiguous response.
               ``persisted_at`` is the receipt-horizon guard's input: a
               persisted request whose receipt lookup 404s may only be
               republished while it is YOUNGER than the receipt horizon —
               past it, a 404 is indistinguishable from a purged receipt
               and the flusher halts (flush.py). The v3 ``topic`` /
               ``partition`` fields are the v2 idempotency-key
               derivation inputs the envelope's key was minted from:
               they make a persisted entry self-describing, and recovery
               cross-checks them against the stage's own identity (an
               entry naming another partition is foreign state, refused
               loudly). Legacy envelopes decode with the fields they
               lack set to None — v1 additionally means ``persisted_at``
               is None: unknown age, halt-safe.
- ``flushed``:  LEGACY — pre-atomic-settlement builds wrote a marker per
               committed range (v1 value: key_len(u16) key(utf8)
               commit_snapshot_id(u64) flushed_at(i64)). Settlement has
               always been one atomic transaction, so the repair state the
               marker existed for is unreachable and nothing writes the
               prefix any more; :meth:`PartitionStage.recover` collects
               leftover markers in bounded pages WITHOUT decoding them.
- ``poison``:  version(1) reason_len(u16) reason(utf8) has_key(u8)
               [key_len(u32) key] has_value(u8) [value_len(u32) value]
               value_bytes_original(u32). The consume-time quarantine:
               records that can never become rows, stored aside durably
               (never silently dropped) with the rejection reason.
               Version 2 appends ``quarantined_at_us`` (u64, micros
               since the Unix epoch) — the retention purge's age basis
               (``PartitionStage.purge_poison_expired``). v1 envelopes
               decode with ``quarantined_at_us=None``: unknown age,
               never purged (the safe direction for forensic state —
               kept, like a pre-``persisted_at`` prepared entry is
               halt-safe).

The settlement window
---------------------

A committed flush settles by deleting EXACTLY its offset window of the
team's rows. Byte order is (team, timestamp, offset), so no byte range
narrower than the team's isolates one offset window — a younger range of
the same team sorts inside the same slice and must survive. The delete
therefore scans :func:`flushed_rows_scan_range` (the team's whole slice)
and deletes the keys :func:`row_in_flushed_range` selects; within one
partition instance a row's Kafka offset is unique, so the window
identifies its rows exactly.

Ranges
------

Range functions return half-open ``[start, end)`` byte pairs built by
prefix successor, so they select exactly the keys sharing a prefix.

Idempotency
-----------

``idempotency_key`` derives a commit's idempotency key from
``(table_uuid, topic, partition, team_id, first_offset, last_offset)`` —
a UUIDv5 over a pinned namespace and a canonical name line
``millrace:v2:table=<uuid>:topic=<topic>:partition=<n>:team=<n>:offsets=
<first>-<last>``. Derived, not random: no wall-clock, no randomness, no
per-attempt state, so the same staged range names the same commit in
every process, forever, and the server's receipt dedupes the replay
(docs/kafka-ingestion.md §Delivery semantics). The table INCARNATION is
in the name because receipts survive a table drop: without it a
dropped-and-recreated table would answer a flush from its predecessor's
receipt. TOPIC and PARTITION are in the name (v2) because Kafka offsets
are PER PARTITION: two partitions flushing one team over the same offset
range must never produce the same key — the first derivation omitted
them, and the second partition's upload would overwrite the first's
committed objects under the shared key before the server refused the
publish as a key reuse. BOTH offset ends are in the name because
"everything up to N" is not a row set once a partition rewinds. The
derivation is versioned (``v2`` in the name); changing it deliberately
invalidates all outstanding keys.

This module is pure: no I/O, no SlateDB, no clocks.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Final

ROWS_PREFIX: Final = b"rows/"
STATS_PREFIX: Final = b"stats/"
SCHED_AGE_PREFIX: Final = b"sched_age/"
OFFSETS_PREFIX: Final = b"offsets/"
PREPARED_PREFIX: Final = b"prepared/"
FLUSHED_PREFIX: Final = b"flushed/"
POISON_PREFIX: Final = b"poison/"

_INT64_MIN: Final = -(1 << 63)
_INT64_MAX: Final = (1 << 63) - 1
_UINT64_MAX: Final = (1 << 64) - 1
_UINT32_MAX: Final = (1 << 32) - 1
_UINT16_MAX: Final = (1 << 16) - 1
_SIGN_OFFSET: Final = 1 << 63

STATS_VERSION: Final = 1
OFFSETS_VERSION: Final = 1
#: The version NEW ``prepared/`` envelopes are written with. Versions 1
#: (no ``persisted_at``) and 2 (no ``topic``/``partition``) still decode;
#: see :func:`decode_prepared_value`.
PREPARED_VERSION: Final = 3
PREPARED_VERSION_LEGACY: Final = 1
#: The intermediate shape this branch wrote before the v2 key derivation:
#: ``persisted_at`` but no ``topic``/``partition``. Decode-only.
PREPARED_VERSION_V2: Final = 2
POISON_VERSION: Final = 2
#: The pre-retention shape (no ``quarantined_at_us`` tail). Decode-only:
#: an encode of an entry without the stamp keeps writing these exact
#: bytes so a mixed-version fleet never produces a value an older build
#: refuses on a path it reads.
POISON_VERSION_LEGACY: Final = 1

ROW_KEY_LEN: Final = len(ROWS_PREFIX) + 8 + 8 + 8
STATS_KEY_LEN: Final = len(STATS_PREFIX) + 8
STATS_VALUE_LEN: Final = 1 + 8 + 8 + 8 + 8
SCHED_AGE_KEY_LEN: Final = len(SCHED_AGE_PREFIX) + 8 + 8
OFFSETS_KEY_LEN: Final = len(OFFSETS_PREFIX) + 8
PREPARED_KEY_LEN: Final = len(PREPARED_PREFIX) + 8 + 8
POISON_KEY_LEN: Final = len(POISON_PREFIX) + 8


def _require_uint64(name: str, value: int) -> None:
    if not 0 <= value <= _UINT64_MAX:
        raise ValueError(f"{name} must fit in an unsigned 64-bit field, got {value}")


def _require_int64(name: str, value: int) -> None:
    if not _INT64_MIN <= value <= _INT64_MAX:
        raise ValueError(f"{name} must fit in a signed 64-bit field, got {value}")


def _encode_u64(value: int) -> bytes:
    return value.to_bytes(8, "big", signed=False)


def _encode_i64(value: int) -> bytes:
    return value.to_bytes(8, "big", signed=True)


def _encode_i64_ordered(value: int) -> bytes:
    """Offset-binary form: byte order matches numeric order on all int64."""
    return _encode_u64(value + _SIGN_OFFSET)


def _decode_i64_ordered(data: bytes) -> int:
    return int.from_bytes(data, "big", signed=False) - _SIGN_OFFSET


def _prefix_successor(prefix: bytes) -> bytes:
    """The smallest byte string strictly greater than every string with
    this prefix — the exclusive end of the prefix's half-open range."""
    stripped = prefix.rstrip(b"\xff")
    if not stripped:
        raise ValueError("a prefix of only 0xFF bytes has no successor")
    return stripped[:-1] + bytes([stripped[-1] + 1])


def _require_prefix(prefix: bytes, data: bytes, length: int) -> None:
    if len(data) != length:
        raise ValueError(f"expected {length} bytes, got {len(data)}")
    if not data.startswith(prefix):
        raise ValueError(f"key does not start with {prefix!r}: {data!r}")


# -- rows/ --------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RowKey:
    """Decoded form of a ``rows/`` key.

    ``timestamp_us`` is the event timestamp in microseconds since the
    Unix epoch; negative values (pre-1970 junk) are legal and sort
    correctly. ``offset`` is the Kafka offset of the record.
    """

    team_id: int
    timestamp_us: int
    offset: int

    def __post_init__(self) -> None:
        _require_uint64("team_id", self.team_id)
        _require_int64("timestamp_us", self.timestamp_us)
        _require_int64("offset", self.offset)


def encode_row_key(key: RowKey) -> bytes:
    """Encode so byte order matches (team, timestamp, offset) sort order."""
    return (
        ROWS_PREFIX
        + _encode_u64(key.team_id)
        + _encode_i64_ordered(key.timestamp_us)
        + _encode_i64_ordered(key.offset)
    )


def decode_row_key(data: bytes) -> RowKey:
    """Inverse of :func:`encode_row_key`."""
    _require_prefix(ROWS_PREFIX, data, ROW_KEY_LEN)
    p = len(ROWS_PREFIX)
    return RowKey(
        team_id=int.from_bytes(data[p : p + 8], "big", signed=False),
        timestamp_us=_decode_i64_ordered(data[p + 8 : p + 16]),
        offset=_decode_i64_ordered(data[p + 16 : p + 24]),
    )


def team_rows_prefix(team_id: int) -> bytes:
    """The shared prefix of every ``rows/`` key of one team."""
    _require_uint64("team_id", team_id)
    return ROWS_PREFIX + _encode_u64(team_id)


def team_rows_range(team_id: int) -> tuple[bytes, bytes]:
    """Half-open byte range selecting exactly the staged rows of one team."""
    start = team_rows_prefix(team_id)
    return start, _prefix_successor(start)


def staged_rows_range(
    team_id: int, first_offset: int, last_offset: int
) -> tuple[bytes, bytes]:
    """Half-open byte range scanned at flush time for the staged batch
    identified by ``(team_id, first_offset, last_offset)``.

    The range is NOT narrowed by offset and cannot be: offset is the
    least significant row-key field, so one batch's rows interleave with
    the team's other rows in (timestamp, offset) order, and no byte
    range narrower than the team's selects exactly the batch. A flush
    is per key — it scans the team's whole staged slice — so the exact
    range is the team's. The offsets are the flush's identity (see
    ``offsets/`` and :func:`idempotency_key`), not scan bounds.

    The post-receipt DELETE is a different shape: it must remove exactly
    the flushed range's rows and spare younger same-team rows, so it is
    the settlement selection (:func:`flushed_rows_scan_range`
    + :func:`row_in_flushed_range`), not a blind delete of this range.
    """
    _require_int64("first_offset", first_offset)
    _require_int64("last_offset", last_offset)
    if first_offset > last_offset:
        raise ValueError(f"first_offset {first_offset} > last_offset {last_offset}")
    return team_rows_range(team_id)


# -- stats/ -------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class StatsValue:
    """Decoded form of a ``stats/`` value.

    ``first_staged_ts`` / ``last_staged_ts`` are microseconds since the
    Unix epoch. ``first_staged_ts`` is set when the key's first row is
    staged after the key stood empty and is NEVER advanced by later
    arrivals — the flush planner's age trigger and the tail's freshness
    SLA stand on that property.
    """

    staged_bytes: int
    first_staged_ts: int
    last_staged_ts: int
    row_count: int

    def __post_init__(self) -> None:
        _require_uint64("staged_bytes", self.staged_bytes)
        _require_int64("first_staged_ts", self.first_staged_ts)
        _require_int64("last_staged_ts", self.last_staged_ts)
        _require_uint64("row_count", self.row_count)
        if self.first_staged_ts > self.last_staged_ts:
            raise ValueError(
                f"first_staged_ts {self.first_staged_ts} > "
                f"last_staged_ts {self.last_staged_ts}"
            )


def stats_key(team_id: int) -> bytes:
    """The ``stats/`` key for one tenant."""
    _require_uint64("team_id", team_id)
    return STATS_PREFIX + _encode_u64(team_id)


def decode_stats_key(data: bytes) -> int:
    """The team_id of a ``stats/`` key."""
    _require_prefix(STATS_PREFIX, data, STATS_KEY_LEN)
    return int.from_bytes(data[len(STATS_PREFIX) :], "big", signed=False)


def encode_stats_value(value: StatsValue) -> bytes:
    """Version 1: 33 bytes, fixed width, deterministic."""
    return (
        bytes([STATS_VERSION])
        + _encode_u64(value.staged_bytes)
        + _encode_i64(value.first_staged_ts)
        + _encode_i64(value.last_staged_ts)
        + _encode_u64(value.row_count)
    )


def decode_stats_value(data: bytes) -> StatsValue:
    """Inverse of :func:`encode_stats_value`."""
    if len(data) != STATS_VALUE_LEN:
        raise ValueError(f"expected {STATS_VALUE_LEN} bytes, got {len(data)}")
    if data[0] != STATS_VERSION:
        raise ValueError(f"unknown stats value version {data[0]}")
    return StatsValue(
        staged_bytes=int.from_bytes(data[1:9], "big", signed=False),
        first_staged_ts=int.from_bytes(data[9:17], "big", signed=True),
        last_staged_ts=int.from_bytes(data[17:25], "big", signed=True),
        row_count=int.from_bytes(data[25:33], "big", signed=False),
    )


# -- sched_age/ ---------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SchedAgeKey:
    """Decoded form of a ``sched_age/`` key (the value is always empty)."""

    first_staged_ts: int
    team_id: int

    def __post_init__(self) -> None:
        _require_int64("first_staged_ts", self.first_staged_ts)
        _require_uint64("team_id", self.team_id)


def encode_sched_age_key(key: SchedAgeKey) -> bytes:
    """Age-ordered so the readiness sweep for expired keys is a prefix
    range scan."""
    return (
        SCHED_AGE_PREFIX
        + _encode_i64_ordered(key.first_staged_ts)
        + _encode_u64(key.team_id)
    )


def decode_sched_age_key(data: bytes) -> SchedAgeKey:
    """Inverse of :func:`encode_sched_age_key`."""
    _require_prefix(SCHED_AGE_PREFIX, data, SCHED_AGE_KEY_LEN)
    p = len(SCHED_AGE_PREFIX)
    return SchedAgeKey(
        first_staged_ts=_decode_i64_ordered(data[p : p + 8]),
        team_id=int.from_bytes(data[p + 8 : p + 16], "big", signed=False),
    )


def sched_age_range_through(cutoff_ts: int) -> tuple[bytes, bytes]:
    """Half-open range selecting every ``sched_age/`` entry with
    ``first_staged_ts <= cutoff_ts`` — the readiness sweep's scan."""
    _require_int64("cutoff_ts", cutoff_ts)
    if cutoff_ts == _INT64_MAX:
        return SCHED_AGE_PREFIX, _prefix_successor(SCHED_AGE_PREFIX)
    return SCHED_AGE_PREFIX, SCHED_AGE_PREFIX + _encode_i64_ordered(cutoff_ts + 1)


# -- offsets/ -----------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class OffsetRange:
    """One Kafka ``(topic, partition, first_offset, last_offset)`` range
    covering part of a team's staged batch — the flush's identity
    material (millpond's offsets line, restated as a value envelope)."""

    topic: str
    partition: int
    first_offset: int
    last_offset: int

    def __post_init__(self) -> None:
        encoded = self.topic.encode("utf-8")
        if len(encoded) > _UINT16_MAX:
            raise ValueError(
                f"topic does not fit in a u16 length: {len(encoded)} bytes"
            )
        if not 0 <= self.partition <= _UINT32_MAX:
            raise ValueError(
                f"partition must fit in an unsigned 32-bit field, got {self.partition}"
            )
        _require_int64("first_offset", self.first_offset)
        _require_int64("last_offset", self.last_offset)
        if self.first_offset > self.last_offset:
            raise ValueError(
                f"first_offset {self.first_offset} > last_offset {self.last_offset}"
            )


def offsets_key(team_id: int) -> bytes:
    """The ``offsets/`` key recording one team's staged Kafka ranges."""
    _require_uint64("team_id", team_id)
    return OFFSETS_PREFIX + _encode_u64(team_id)


def decode_offsets_key(data: bytes) -> int:
    """The team_id of an ``offsets/`` key."""
    _require_prefix(OFFSETS_PREFIX, data, OFFSETS_KEY_LEN)
    return int.from_bytes(data[len(OFFSETS_PREFIX) :], "big", signed=False)


def _offset_range_sort_key(r: OffsetRange) -> tuple[str, int, int, int]:
    return (r.topic, r.partition, r.first_offset, r.last_offset)


def encode_offsets_value(ranges: list[OffsetRange] | tuple[OffsetRange, ...]) -> bytes:
    """Version 1: u16 count, then each range; ranges are sorted first so
    the encoding is canonical — the same set encodes to identical bytes
    regardless of assembly order."""
    ordered = sorted(ranges, key=_offset_range_sort_key)
    if len(ordered) > _UINT16_MAX:
        raise ValueError(f"too many offset ranges: {len(ordered)}")
    out = bytearray()
    out.append(OFFSETS_VERSION)
    out += len(ordered).to_bytes(2, "big")
    for r in ordered:
        topic = r.topic.encode("utf-8")
        out += len(topic).to_bytes(2, "big")
        out += topic
        out += r.partition.to_bytes(4, "big", signed=False)
        out += _encode_i64(r.first_offset)
        out += _encode_i64(r.last_offset)
    return bytes(out)


def decode_offsets_value(data: bytes) -> tuple[OffsetRange, ...]:
    """Inverse of :func:`encode_offsets_value`."""
    if len(data) < 3:
        raise ValueError(f"offsets value too short: {len(data)} bytes")
    if data[0] != OFFSETS_VERSION:
        raise ValueError(f"unknown offsets value version {data[0]}")
    count = int.from_bytes(data[1:3], "big")
    pos = 3
    ranges = []
    for _ in range(count):
        if len(data) < pos + 2:
            raise ValueError("truncated offsets value: topic length")
        topic_len = int.from_bytes(data[pos : pos + 2], "big")
        pos += 2
        # topic_len + partition(u32) + first(i64) + last(i64)
        if len(data) < pos + topic_len + 20:
            raise ValueError("truncated offsets value: range record")
        topic = bytes(data[pos : pos + topic_len]).decode("utf-8")
        pos += topic_len
        partition = int.from_bytes(data[pos : pos + 4], "big", signed=False)
        pos += 4
        first_offset = int.from_bytes(data[pos : pos + 8], "big", signed=True)
        pos += 8
        last_offset = int.from_bytes(data[pos : pos + 8], "big", signed=True)
        pos += 8
        ranges.append(
            OffsetRange(
                topic=topic,
                partition=partition,
                first_offset=first_offset,
                last_offset=last_offset,
            )
        )
    if pos != len(data):
        raise ValueError(f"trailing bytes after offsets value: {len(data) - pos}")
    return tuple(ranges)


# -- prepared/ ----------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PreparedRequest:
    """A persisted prepared commit request (Phase 4 writes these): the
    byte-exact request body plus the idempotency key it is published
    under. Persisted BEFORE publication and replayed byte-identically
    after an ambiguous response — never regenerated.

    ``persisted_at`` is the persistence instant in microseconds since the
    Unix epoch (the flusher's injected clock) and is the receipt-horizon
    guard's input: a receipt 404 may be answered with a blind replay only
    while the entry is younger than the horizon — past it, the receipt
    may have been PURGED server-side and a replay could duplicate an
    already-executed commit, so the flusher halts (flush.py). It is None
    ONLY on envelopes written by a v1 build (unknown age → halt-safe):
    :func:`encode_prepared_value` refuses to write one without it.

    ``topic`` / ``partition`` are the v2 idempotency-key derivation
    inputs the entry's key was minted from (see :func:`idempotency_key`).
    Carried since v3 so a persisted entry is self-describing: recovery
    cross-checks them against the stage's own identity, and an entry
    naming another (topic, partition) is foreign state — loud corruption,
    never a silent replay. They are None ONLY on envelopes written by a
    v1/v2 build; :func:`encode_prepared_value` refuses to write one
    without them.
    """

    idempotency_key: str
    body: bytes
    persisted_at: int | None = None
    topic: str | None = None
    partition: int | None = None

    def __post_init__(self) -> None:
        key_len = len(self.idempotency_key.encode("utf-8"))
        if key_len > _UINT16_MAX:
            raise ValueError(f"idempotency key too long: {key_len} bytes")
        if len(self.body) > _UINT32_MAX:
            raise ValueError(f"prepared body too long: {len(self.body)} bytes")
        if self.persisted_at is not None:
            _require_uint64("persisted_at", self.persisted_at)
        if (self.topic is None) != (self.partition is None):
            raise ValueError(
                "topic and partition travel together (both or neither), got "
                f"topic={self.topic!r} partition={self.partition!r}"
            )
        if self.topic is not None:
            encoded_topic = self.topic.encode("utf-8")
            if not self.topic:
                raise ValueError("topic must be non-empty")
            if len(encoded_topic) > _UINT16_MAX:
                raise ValueError(
                    f"topic does not fit in a u16 length: {len(encoded_topic)} bytes"
                )
        if self.partition is not None and not 0 <= self.partition <= _UINT32_MAX:
            raise ValueError(
                f"partition must fit in an unsigned 32-bit field, got {self.partition}"
            )


def prepared_key(team_id: int, first_offset: int) -> bytes:
    """The ``prepared/`` key of one staged batch. ``first_offset`` is the
    batch's low-water mark and disambiguates successive batches of one
    team; it sorts numerically, so a team's prepared entries scan in
    batch order."""
    _require_uint64("team_id", team_id)
    _require_int64("first_offset", first_offset)
    return PREPARED_PREFIX + _encode_u64(team_id) + _encode_i64_ordered(first_offset)


def team_prepared_range(team_id: int) -> tuple[bytes, bytes]:
    """Half-open byte range selecting exactly one team's ``prepared/``
    entries (the flush's outstanding-entry gate scans this prefix)."""
    _require_uint64("team_id", team_id)
    start = PREPARED_PREFIX + _encode_u64(team_id)
    return start, _prefix_successor(start)


@dataclass(frozen=True, slots=True)
class PreparedKey:
    """Decoded form of a ``prepared/`` key."""

    team_id: int
    first_offset: int


def decode_prepared_key(data: bytes) -> PreparedKey:
    """Inverse of :func:`prepared_key`."""
    _require_prefix(PREPARED_PREFIX, data, PREPARED_KEY_LEN)
    p = len(PREPARED_PREFIX)
    return PreparedKey(
        team_id=int.from_bytes(data[p : p + 8], "big", signed=False),
        first_offset=_decode_i64_ordered(data[p + 8 : p + 16]),
    )


def encode_prepared_value(request: PreparedRequest) -> bytes:
    """Version 3: key_len(u16) key(utf8) topic_len(u16) topic(utf8)
    partition(u32) body_len(u32) body persisted_at(u64).

    ``persisted_at`` is REQUIRED on a write — an entry whose age is
    unknown can never be checked against the receipt horizon, so writing
    one is refused outright. So are ``topic``/``partition``: the v3
    envelope carries the v2 idempotency-key derivation inputs so a
    recovered entry is self-describing (recovery cross-checks them
    against the stage's identity), and a write that cannot name them is
    corrupt by omission. (Legacy v1/v2 envelopes, which lack one or both,
    DECODE with the missing fields None — v1 is halt-safe on the replay
    path.)"""
    persisted_at = request.persisted_at
    if persisted_at is None:
        raise ValueError(
            "cannot persist a prepared request without persisted_at: an "
            "entry of unknown age can never be checked against the "
            "receipt horizon"
        )
    topic = request.topic
    partition = request.partition
    if topic is None or partition is None:
        raise ValueError(
            "cannot persist a prepared request without topic/partition: "
            "the v3 envelope carries the idempotency key's derivation "
            "inputs so a recovered entry is self-describing"
        )
    key = request.idempotency_key.encode("utf-8")
    encoded_topic = topic.encode("utf-8")
    return (
        bytes([PREPARED_VERSION])
        + len(key).to_bytes(2, "big")
        + key
        + len(encoded_topic).to_bytes(2, "big")
        + encoded_topic
        + partition.to_bytes(4, "big")
        + len(request.body).to_bytes(4, "big")
        + request.body
        + _encode_u64(persisted_at)
    )


def decode_prepared_value(data: bytes) -> PreparedRequest:
    """Inverse of :func:`encode_prepared_value`, plus the legacy forms:
    version 1 envelopes carry neither ``persisted_at`` nor
    ``topic``/``partition`` and decode with all three None — "unknown
    age", which the flusher treats as halt-safe (a receipt 404 for such
    an entry is never answered with a blind replay); version 2 envelopes
    carry ``persisted_at`` only and decode with ``topic``/``partition``
    None (recovery's derivation-input cross-check simply does not apply
    to them — nothing was ever deployed that wrote either)."""
    if len(data) < 3:
        raise ValueError(f"prepared value too short: {len(data)} bytes")
    version = data[0]
    if version not in (PREPARED_VERSION_LEGACY, PREPARED_VERSION_V2, PREPARED_VERSION):
        raise ValueError(f"unknown prepared value version {version}")
    key_len = int.from_bytes(data[1:3], "big")
    pos = 3
    if len(data) < pos + key_len:
        raise ValueError("truncated prepared value: key")
    key = bytes(data[pos : pos + key_len]).decode("utf-8")
    pos += key_len
    topic: str | None = None
    partition: int | None = None
    if version == PREPARED_VERSION:
        if len(data) < pos + 2:
            raise ValueError("truncated prepared value: topic length")
        topic_len = int.from_bytes(data[pos : pos + 2], "big")
        pos += 2
        if len(data) < pos + topic_len + 4:
            raise ValueError("truncated prepared value: topic")
        topic = bytes(data[pos : pos + topic_len]).decode("utf-8")
        pos += topic_len
        partition = int.from_bytes(data[pos : pos + 4], "big", signed=False)
        pos += 4
    if len(data) < pos + 4:
        raise ValueError("truncated prepared value: body length")
    body_len = int.from_bytes(data[pos : pos + 4], "big")
    pos += 4
    tail = 0 if version == PREPARED_VERSION_LEGACY else 8  # persisted_at(u64)
    if len(data) != pos + body_len + tail:
        raise ValueError(
            f"prepared body length mismatch: field says {body_len}, "
            f"value holds {len(data) - pos - tail}"
        )
    persisted_at = (
        int.from_bytes(data[pos + body_len :], "big", signed=False) if tail else None
    )
    return PreparedRequest(
        idempotency_key=key,
        body=bytes(data[pos : pos + body_len]),
        persisted_at=persisted_at,
        topic=topic,
        partition=partition,
    )


# -- flushed/ -----------------------------------------------------------------
#
# LEGACY prefix. Pre-atomic-settlement documentation wrote one marker per
# committed range here; settlement has in fact always been ONE atomic
# transaction (:meth:`PartitionStage.commit_flushed`), so a readable marker
# was always fully settled state — written only with the commit's receipt
# already in hand, and durable together with the deletes it claimed to
# guard. The marker therefore carried no information the surviving
# ``prepared/`` entry plus the server-side receipt do not, and nothing
# writes this prefix any more. :meth:`PartitionStage.recover` deletes
# whatever it finds under it, in bounded pages, without decoding: an extant
# marker is settled garbage, and its receipt (possibly long purged) is not
# consulted. ``FlushedKey`` lives on as the settlement-WINDOW type (the
# delete selection below); it no longer names a key this build writes.


@dataclass(frozen=True, slots=True)
class FlushedKey:
    """A settled flush window: the committed Kafka offset range
    ``(team_id, first_offset, last_offset)`` of one team's staged rows.
    Enough to delete exactly its ``rows/`` entries
    (:func:`row_in_flushed_range`). (Legacy: also the ``flushed/`` marker
    key's decoded form — see the module docstring.)"""

    team_id: int
    first_offset: int
    last_offset: int

    def __post_init__(self) -> None:
        _require_uint64("team_id", self.team_id)
        _require_int64("first_offset", self.first_offset)
        _require_int64("last_offset", self.last_offset)
        if self.first_offset > self.last_offset:
            raise ValueError(
                f"first_offset {self.first_offset} > last_offset {self.last_offset}"
            )


def flushed_range() -> tuple[bytes, bytes]:
    """Half-open range selecting every (legacy) ``flushed/`` marker — the
    recovery collector's scan bounds."""
    return FLUSHED_PREFIX, _prefix_successor(FLUSHED_PREFIX)


def flushed_rows_scan_range(key: FlushedKey) -> tuple[bytes, bytes]:
    """The scan bounds of a settlement delete: the team's whole ``rows/``
    slice. No narrower byte range can isolate the window (row-key order is
    (team, timestamp, offset)); apply :func:`row_in_flushed_range` to every
    scanned key and delete exactly those."""
    return team_rows_range(key.team_id)


def row_in_flushed_range(row: RowKey, flushed: FlushedKey) -> bool:
    """True iff ``row`` is one of the settled window's staged rows — the
    exact settlement-delete selection.

    Within one partition instance a row's Kafka offset is unique to its
    record, so the window identifies its rows exactly.
    A younger range of the same team (offsets above ``last_offset``)
    sorts INSIDE the scan range and must survive the delete.
    """
    return (
        row.team_id == flushed.team_id
        and flushed.first_offset <= row.offset <= flushed.last_offset
    )


# -- poison/ ------------------------------------------------------------------
#
# The consume-time quarantine (docs/kafka-ingestion.md §Delivery semantics):
# a record that can never become a row — an undecodable ``team_id`` key, no
# usable timestamp, a tombstone — is written here, keyed by its Kafka offset,
# in the SAME write batch as the poll cycle's staged rows. The offset commit
# that follows the batch therefore covers quarantined records exactly the way
# it covers staged ones: offsets commit strictly after durable staging, a
# poison record is durably stored aside (never silently dropped), and the
# loop never wedges on one. Nothing on the flush path reads this prefix; it
# is forensic state with an exact decode. Keys are offset-ordered within the
# partition instance, which also makes a redelivered poison record's re-put
# idempotent.


def poison_key(offset: int) -> bytes:
    """The ``poison/`` key quarantining one consumed record, by Kafka offset."""
    _require_int64("offset", offset)
    return POISON_PREFIX + _encode_i64_ordered(offset)


def decode_poison_key(data: bytes) -> int:
    """Inverse of :func:`poison_key`: the quarantined record's Kafka offset."""
    _require_prefix(POISON_PREFIX, data, POISON_KEY_LEN)
    return _decode_i64_ordered(data[len(POISON_PREFIX) :])


def poison_range() -> tuple[bytes, bytes]:
    """Half-open range selecting every ``poison/`` entry of the instance."""
    return POISON_PREFIX, _prefix_successor(POISON_PREFIX)


@dataclass(frozen=True, slots=True)
class PoisonValue:
    """Decoded form of a ``poison/`` value — the quarantine envelope.

    ``reason`` is the bounded rejection vocabulary (consumer.py's
    ``REASON_*`` constants). ``key`` / ``value`` are the record's raw bytes
    (None when the record carried none — a tombstone quarantines with
    ``value=None`` and ``value_bytes_original == 0``). The consumer caps the
    stored ``value`` at its poison byte limit; ``value_bytes_original`` then
    exceeds ``len(value)`` and records what was cut. The FLUSH path never
    caps (flush.py): its quarantines persist the whole payload, because
    settlement deletes the staged row and this entry is the only
    surviving copy.

    ``quarantined_at_us`` (v2) is the staging-clock instant the entry was
    written — the retention purge's age basis. None means unknown age
    (a v1 envelope from a pre-retention build, or a writer that had no
    clock to stamp): the purge KEEPS such entries, the safe direction
    for forensic state.
    """

    reason: str
    key: bytes | None
    value: bytes | None
    value_bytes_original: int
    quarantined_at_us: int | None = None

    def __post_init__(self) -> None:
        reason_len = len(self.reason.encode("utf-8"))
        if reason_len == 0:
            raise ValueError("reason must be non-empty")
        if reason_len > _UINT16_MAX:
            raise ValueError(f"reason does not fit in a u16 length: {reason_len} bytes")
        if self.key is not None and len(self.key) > _UINT32_MAX:
            raise ValueError(f"poison key too long: {len(self.key)} bytes")
        if self.value is None:
            if self.value_bytes_original != 0:
                raise ValueError(
                    f"value is absent but value_bytes_original is "
                    f"{self.value_bytes_original}"
                )
        else:
            if len(self.value) > _UINT32_MAX:
                raise ValueError(f"poison value too long: {len(self.value)} bytes")
            if self.value_bytes_original < len(self.value):
                raise ValueError(
                    f"value_bytes_original {self.value_bytes_original} < stored "
                    f"value length {len(self.value)}"
                )
        if self.value_bytes_original > _UINT32_MAX:
            raise ValueError(
                f"value_bytes_original does not fit in a u32 field: "
                f"{self.value_bytes_original}"
            )
        if self.quarantined_at_us is not None:
            _require_uint64("quarantined_at_us", self.quarantined_at_us)


def encode_poison_value(value: PoisonValue) -> bytes:
    """Version 2: version(1) reason_len(u16) reason(utf8) has_key(u8)
    [key_len(u32) key] has_value(u8) [value_len(u32) value]
    value_bytes_original(u32) quarantined_at_us(u64).

    An entry WITHOUT a quarantine stamp (``quarantined_at_us=None``)
    encodes as the exact version-1 bytes — the shape every build of this
    branch already reads — so stamping is the only wire change."""
    out = bytearray()
    out.append(
        POISON_VERSION if value.quarantined_at_us is not None else POISON_VERSION_LEGACY
    )
    reason = value.reason.encode("utf-8")
    out += len(reason).to_bytes(2, "big")
    out += reason
    if value.key is None:
        out.append(0)
    else:
        out.append(1)
        out += len(value.key).to_bytes(4, "big")
        out += value.key
    if value.value is None:
        out.append(0)
    else:
        out.append(1)
        out += len(value.value).to_bytes(4, "big")
        out += value.value
    out += value.value_bytes_original.to_bytes(4, "big")
    if value.quarantined_at_us is not None:
        out += value.quarantined_at_us.to_bytes(8, "big")
    return bytes(out)


def decode_poison_value(data: bytes) -> PoisonValue:
    """Inverse of :func:`encode_poison_value`. Versions 1 and 2 decode;
    v1 yields ``quarantined_at_us=None`` (unknown age — never purged)."""
    if len(data) < 4:
        raise ValueError(f"poison value too short: {len(data)} bytes")
    if data[0] not in (POISON_VERSION_LEGACY, POISON_VERSION):
        raise ValueError(f"unknown poison value version {data[0]}")
    version = data[0]
    reason_len = int.from_bytes(data[1:3], "big")
    if len(data) < 3 + reason_len + 1:
        raise ValueError("truncated poison value: reason")
    reason = bytes(data[3 : 3 + reason_len]).decode("utf-8")
    pos = 3 + reason_len

    has_key = data[pos]
    if has_key not in (0, 1):
        raise ValueError(f"poison has_key flag must be 0 or 1, got {has_key}")
    pos += 1
    key: bytes | None = None
    if has_key:
        if len(data) < pos + 4:
            raise ValueError("truncated poison value: key length")
        key_len = int.from_bytes(data[pos : pos + 4], "big")
        pos += 4
        # +1: the has_value flag must follow the key bytes.
        if len(data) < pos + key_len + 1:
            raise ValueError("truncated poison value: key")
        key = bytes(data[pos : pos + key_len])
        pos += key_len

    # On the has_key=0 path nothing so far guaranteed this flag exists.
    if len(data) < pos + 1:
        raise ValueError("truncated poison value: flags")
    has_value = data[pos]
    if has_value not in (0, 1):
        raise ValueError(f"poison has_value flag must be 0 or 1, got {has_value}")
    pos += 1
    value: bytes | None = None
    if has_value:
        if len(data) < pos + 4:
            raise ValueError("truncated poison value: value length")
        value_len = int.from_bytes(data[pos : pos + 4], "big")
        pos += 4
        if len(data) < pos + value_len + 4:
            raise ValueError("truncated poison value: value")
        value = bytes(data[pos : pos + value_len])
        pos += value_len

    if len(data) < pos + 4:
        raise ValueError(
            f"poison value length mismatch: value_bytes_original missing or "
            f"trailing bytes ({len(data) - pos} left at {pos})"
        )
    value_bytes_original = int.from_bytes(data[pos : pos + 4], "big")
    pos += 4
    quarantined_at_us: int | None = None
    if version == POISON_VERSION:
        if len(data) != pos + 8:
            raise ValueError(
                f"truncated poison value: quarantined_at_us "
                f"({len(data) - pos} byte(s) left at {pos}, need exactly 8)"
            )
        quarantined_at_us = int.from_bytes(data[pos : pos + 8], "big")
    elif len(data) != pos:
        raise ValueError(
            f"poison value length mismatch: trailing bytes "
            f"({len(data) - pos} left at {pos})"
        )
    return PoisonValue(
        reason=reason,
        key=key,
        value=value,
        value_bytes_original=value_bytes_original,
        quarantined_at_us=quarantined_at_us,
    )


# -- idempotency --------------------------------------------------------------

#: The derivation string whose UUIDv5 (URL namespace) is the idempotency
#: namespace below. Pinned so the constant's provenance is checkable.
IDEMPOTENCY_NAMESPACE_DERIVATION: Final = (
    "github.com/PostHog/hoglake millrace idempotency v1"
)

#: Fixed namespace for commit idempotency keys; equals
#: ``uuid.uuid5(uuid.NAMESPACE_URL, IDEMPOTENCY_NAMESPACE_DERIVATION)``
#: (a test pins that equality). Never changes.
IDEMPOTENCY_NAMESPACE: Final = uuid.UUID("70cd616a-eaef-5359-838c-573cb224e4f9")


def idempotency_key(
    table_uuid: str,
    topic: str,
    partition: int,
    team_id: int,
    first_offset: int,
    last_offset: int,
) -> str:
    """The commit idempotency key for one staged batch.

    A UUIDv5 over :data:`IDEMPOTENCY_NAMESPACE` and the canonical name
    ``millrace:v2:table=<table>:topic=<topic>:partition=<n>:team=<n>
    :offsets=<first>-<last>``. Pure function of its six inputs: same
    inputs, same key, in every process, forever. ``table_uuid`` is
    canonicalized (parsed and re-emitted) so case-variant spellings of
    one incarnation name one key; ``topic`` is NOT canonicalized (Kafka
    topic names are case-sensitive).

    ``topic``/``partition`` are derivation inputs (v2) because Kafka
    offsets are per partition: without them, two partitions flushing one
    team over the same offset range derived the SAME key — and the same
    object URIs, since those derive from the key — so the second
    partition's upload overwrote the first's committed objects before the
    server refused the publish as a key reuse (PR #331 review). The
    stage knows its own topic and partition; the flusher threads them in.
    """
    canonical_table = str(uuid.UUID(table_uuid))
    encoded_topic = topic.encode("utf-8")
    if not topic:
        raise ValueError("topic must be non-empty")
    if len(encoded_topic) > _UINT16_MAX:
        raise ValueError(
            f"topic does not fit in a u16 length: {len(encoded_topic)} bytes"
        )
    if not 0 <= partition <= _UINT32_MAX:
        raise ValueError(
            f"partition must fit in an unsigned 32-bit field, got {partition}"
        )
    _require_uint64("team_id", team_id)
    _require_int64("first_offset", first_offset)
    _require_int64("last_offset", last_offset)
    if first_offset > last_offset:
        raise ValueError(f"first_offset {first_offset} > last_offset {last_offset}")
    name = (
        f"millrace:v2:table={canonical_table}:topic={topic}:partition={partition}"
        f":team={team_id}:offsets={first_offset}-{last_offset}"
    )
    return str(uuid.uuid5(IDEMPOTENCY_NAMESPACE, name))
