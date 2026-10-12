"""Staging key codecs (docs/kafka-ingestion.md §Staging layout).

    rows/<group><timestamp><offset>            → event payload bytes
    stats/<group>                              → StatsValue
    sched_age/<first_staged_ts><group>         → ∅ (empty value)
    offsets/<group>                            → sorted (topic, partition, first, last) ranges
    prepared/<group><first_offset>             → PreparedRequest envelope
    flushed/…                                  → LEGACY: never written
    poison/<offset>                            → PoisonValue envelope

``<group>`` is ONE HOGLAKE PARTITION TUPLE of the destination table: the
wire strings the commit's ``partition_values`` carry for a file of that
tuple (pyhoglake ``transforms.wire_string`` of each live-spec field's
transformed source value, in spec order; None for a null component),
encoded by :func:`encode_group`. The consumer computes it per record at
consume time (:class:`millrace.grouping.GroupKeyer`); an unpartitioned
table has the empty tuple. Every key that names "what flushes together"
names a partition tuple, so a staged slice is one hoglake partition's
staged rows.

Topology: ONE SlateDB instance per claimed Kafka partition
(``s3://…/millrace/{topic}/{partition}``). The keyspace therefore
carries NO Kafka-partition component — the instance *is* the Kafka
partition, the consumer assignment is the claim, and staged state
follows the Kafka partition to its new owner, never the pod. The
``offsets/`` side prefix keeps ``(topic, partition, first_offset,
last_offset)`` per staged range as the flush's identity material, not
as row-key material.

The LSM's ordering is load-bearing: ``rows`` key bytes must sort in
(group, timestamp, offset) order so a flush reads one contiguous, nearly
sorted range, and ``sched_age`` bytes must sort in (first_staged_ts,
group) order so the readiness sweep is a prefix range scan.

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
- The group is PREFIX-FREE: ``arity(u8)``, then per component a 1-byte
  tag — ``0x00`` null, ``0x01`` present — and, when present,
  ``len(u16) utf8``. The arity byte is what makes the WHOLE tuple
  prefix-free, not just each component: without it the tuple
  ``(a,)`` (written under an older one-field spec) would be a byte
  prefix of ``(a, b)``, and the old tuple's row range would swallow
  the new tuple's rows. With it, ``prefix + group`` selects exactly one
  group's keys by prefix successor (:func:`group_rows_range`). Fixed
  trailing fields (timestamp, offset) parse off the END of a key, and
  :func:`decode_group` refuses anything that is not exactly one
  well-formed tuple.
- Every value envelope starts with a 1-byte version. A reader refuses
  any version it does not know.

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
- ``prepared``: version(4) key_len(u16) idempotency_key(utf8)
               topic_len(u16) topic(utf8) partition(u32)
               arity(u8), then per partition field name_len(u16)
               name(utf8) tag(u8) [value_len(u16) value(utf8)]
               body_len(u32) body(bytes) persisted_at(u64, micros since
               the Unix epoch). The body is the byte-exact prepared
               commit request, written once and replayed
               byte-identically after an ambiguous response.
               ``persisted_at`` is the receipt-horizon guard's input: a
               persisted request whose receipt lookup 404s may only be
               republished while it is YOUNGER than the receipt horizon —
               past it, a 404 is indistinguishable from a purged receipt
               and the flusher halts (flush.py). ``topic`` / ``partition``
               and the partition tuple are the idempotency key's
               derivation inputs: they make a persisted entry
               self-describing, and recovery cross-checks them against
               the stage's own identity and the ``prepared/`` key it
               sits under (an entry naming another partition or tuple is
               foreign state, refused loudly). The tuple travels as hoglake partition
               values — each field's display name beside its wire
               string — so the envelope reads like the file it
               registers.
- ``flushed``:  LEGACY — pre-atomic-settlement builds wrote a marker per
               committed range. Settlement has always been one atomic
               transaction, so the repair state the marker existed for is
               unreachable and nothing writes the prefix any more;
               :meth:`PartitionStage.recover` collects leftover markers in
               bounded pages WITHOUT decoding them.
- ``poison``:  version(1) reason_len(u16) reason(utf8) has_key(u8)
               [key_len(u32) key] has_value(u8) [value_len(u32) value]
               value_bytes_original(u32). The consume-time quarantine:
               records that can never become rows, stored aside durably
               (never silently dropped) with the rejection reason.
               Version 2 appends ``quarantined_at_us`` (u64, micros
               since the Unix epoch) — the retention purge's age basis
               (``PartitionStage.purge_poison_expired``). v1 envelopes
               decode with ``quarantined_at_us=None``: unknown age,
               never purged (the safe direction for forensic state).

The settlement window
---------------------

A committed flush settles by deleting EXACTLY its offset window of the
partition tuple's staged rows. Byte order is (group, timestamp, offset),
so no byte range narrower than the group's isolates one offset window —
a younger range of the same tuple sorts inside the same slice and must
survive. The delete therefore scans :func:`flushed_rows_scan_range`
(the group's whole slice) and deletes the keys
:func:`row_in_flushed_range` selects; within one Kafka-partition
instance a row's offset is unique, so the window identifies its rows
exactly.

Ranges
------

Range functions return half-open ``[start, end)`` byte pairs built by
prefix successor, so they select exactly the keys sharing a prefix
(prefix-correct for variable-length groups because the group encoding
is prefix-free).

Idempotency
-----------

``idempotency_key`` derives a commit's idempotency key from
``(table_uuid, topic, partition, partition tuple, first_offset,
last_offset)`` — a UUIDv5 over a pinned namespace and a canonical name
line ``millrace:v2:table=<uuid>:topic=<topic>:partition=<n>:group=
<k0=…/k1=…>:offsets=<first>-<last>``, where the group is
:func:`group_key_form` — the tuple's wire strings under POSITIONAL names
(``k0=42/k1=681``, ASCII, percent-encoded), so the key is a pure
function of the staged tuple and no spec change can move it (display
names such as ``team_id=42/timestamp_month=681`` appear only in the
commit message and the ``prepared/`` envelope). Derived, not random: no wall-clock, no randomness, no
per-attempt state, so the same staged range names the same commit in
every process, forever, and the server's receipt dedupes the replay
(docs/kafka-ingestion.md §Delivery semantics). The table INCARNATION is
in the name because receipts survive a table drop: without it a
dropped-and-recreated table would answer a flush from its predecessor's
receipt. TOPIC and PARTITION are in the name because Kafka offsets are
PER PARTITION: two Kafka partitions flushing one tuple over the same
offset range must never produce the same key (or the same object URIs,
which derive from it). BOTH offset ends are in the name because
"everything up to N" is not a row set once a partition rewinds.

This module is pure: no I/O, no SlateDB, no clocks.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Final
from urllib.parse import quote

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
_UINT8_MAX: Final = (1 << 8) - 1
_SIGN_OFFSET: Final = 1 << 63

STATS_VERSION: Final = 1
OFFSETS_VERSION: Final = 1
PREPARED_VERSION: Final = 4
POISON_VERSION: Final = 2
#: The pre-retention shape (no ``quarantined_at_us`` tail). An encode of
#: an entry without the stamp writes these exact bytes.
POISON_VERSION_LEGACY: Final = 1

#: Component tags of the partition-tuple encoding.
GROUP_NULL: Final = 0x00
GROUP_PRESENT: Final = 0x01

#: One hoglake partition tuple in wire form: per live-spec field, in spec
#: order, the ``partition_values`` string a file of the tuple carries
#: (None for a null component). The empty tuple is an unpartitioned
#: table's one group.
PartitionTuple = tuple[str | None, ...]

_TS_OFFSET_LEN: Final = 8 + 8
STATS_VALUE_LEN: Final = 1 + 8 + 8 + 8 + 8
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


def _require_key(prefix: bytes, data: bytes, *, min_length: int) -> None:
    if not data.startswith(prefix):
        raise ValueError(f"key does not start with {prefix!r}: {data!r}")
    if len(data) < min_length:
        raise ValueError(f"expected at least {min_length} bytes, got {len(data)}")


def _encode_utf8_u16(name: str, text: str) -> bytes:
    raw = text.encode("utf-8")
    if len(raw) > _UINT16_MAX:
        raise ValueError(f"{name} does not fit in a u16 length: {len(raw)} bytes")
    return len(raw).to_bytes(2, "big") + raw


def _decode_utf8_u16(data: bytes, pos: int, what: str) -> tuple[str, int]:
    if len(data) < pos + 2:
        raise ValueError(f"truncated {what}: length")
    length = int.from_bytes(data[pos : pos + 2], "big")
    pos += 2
    if len(data) < pos + length:
        raise ValueError(f"truncated {what}: {length} bytes declared")
    try:
        text = bytes(data[pos : pos + length]).decode("utf-8")
    except UnicodeDecodeError as e:
        raise ValueError(f"{what} is not UTF-8: {e}") from e
    return text, pos + length


# -- the partition tuple (group) ---------------------------------------------


def encode_group(group: PartitionTuple) -> bytes:
    """The prefix-free byte form of one partition tuple (module
    docstring §Encoding rules): ``arity(u8)``, then per component
    ``0x00`` (null) or ``0x01 len(u16) utf8``."""
    if len(group) > _UINT8_MAX:
        raise ValueError(f"a partition tuple has at most {_UINT8_MAX} fields")
    out = bytearray([len(group)])
    for component in group:
        if component is None:
            out.append(GROUP_NULL)
        else:
            if not isinstance(component, str):
                raise TypeError(
                    "partition tuple components are wire strings or None, got "
                    f"{type(component).__name__}"
                )
            out.append(GROUP_PRESENT)
            out += _encode_utf8_u16("partition value", component)
    return bytes(out)


def _decode_group_at(data: bytes, pos: int) -> tuple[PartitionTuple, int]:
    if len(data) < pos + 1:
        raise ValueError("corrupt partition tuple: no arity byte")
    arity = data[pos]
    pos += 1
    out: list[str | None] = []
    for index in range(arity):
        if len(data) < pos + 1:
            raise ValueError(f"corrupt partition tuple: component {index} missing")
        tag = data[pos]
        pos += 1
        if tag == GROUP_NULL:
            out.append(None)
        elif tag == GROUP_PRESENT:
            value, pos = _decode_utf8_u16(
                data, pos, f"corrupt partition tuple component {index}"
            )
            out.append(value)
        else:
            raise ValueError(
                f"corrupt partition tuple: component {index} has tag {tag:#04x}"
            )
    return tuple(out), pos


def decode_group(data: bytes) -> PartitionTuple:
    """Inverse of :func:`encode_group`. TOTAL: any input either decodes
    to exactly one tuple or raises ValueError naming the corruption — a
    truncated component, an unknown tag, non-UTF-8 bytes or trailing
    bytes are all refused, never guessed at."""
    group, pos = _decode_group_at(bytes(data), 0)
    if pos != len(data):
        raise ValueError(f"corrupt partition tuple: {len(data) - pos} trailing byte(s)")
    return group


def group_label(fields: tuple[str, ...], group: PartitionTuple) -> str:
    """The human form of one partition tuple, for logs and the commit
    message (the idempotency key uses it with positional names —
    :func:`group_key_form`): ``name=value`` per field joined by
    ``/`` (``team_id=42/timestamp_month=681``), a null component as its
    bare field name. ASCII and injective: names and values are
    percent-encoded (``/``, ``=``, ``:`` and anything non-ASCII escape),
    so no two tuples render alike. ``fields`` are hoglake's display
    names for the spec's fields (:func:`millrace.grouping.field_names`);
    an unpartitioned table renders as the empty string."""
    if len(fields) != len(group):
        raise ValueError(
            f"{len(fields)} field name(s) for a {len(group)}-field partition tuple"
        )
    parts = []
    for name, value in zip(fields, group, strict=True):
        encoded = quote(name, safe="._-")
        parts.append(encoded if value is None else f"{encoded}={quote(value, safe='')}")
    return "/".join(parts)


# -- rows/ --------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RowKey:
    """Decoded form of a ``rows/`` key: one staged row of one partition
    tuple.

    ``timestamp_us`` is the Kafka record timestamp in microseconds since
    the Unix epoch; negative values (pre-1970 junk) are legal and sort
    correctly. ``offset`` is the Kafka offset of the record.
    """

    group: PartitionTuple
    timestamp_us: int
    offset: int

    def __post_init__(self) -> None:
        _require_int64("timestamp_us", self.timestamp_us)
        _require_int64("offset", self.offset)


def encode_row_key(key: RowKey) -> bytes:
    """Encode so byte order matches (group, timestamp, offset) sort order."""
    return (
        ROWS_PREFIX
        + encode_group(key.group)
        + _encode_i64_ordered(key.timestamp_us)
        + _encode_i64_ordered(key.offset)
    )


def decode_row_key(data: bytes) -> RowKey:
    """Inverse of :func:`encode_row_key`: the trailing 16 bytes are the
    timestamp and offset, the middle is exactly one partition tuple."""
    p = len(ROWS_PREFIX)
    _require_key(ROWS_PREFIX, data, min_length=p + 1 + _TS_OFFSET_LEN)
    return RowKey(
        group=decode_group(data[p:-_TS_OFFSET_LEN]),
        timestamp_us=_decode_i64_ordered(data[-16:-8]),
        offset=_decode_i64_ordered(data[-8:]),
    )


def group_rows_prefix(group: PartitionTuple) -> bytes:
    """The shared prefix of every ``rows/`` key of one partition tuple."""
    return ROWS_PREFIX + encode_group(group)


def group_rows_range(group: PartitionTuple) -> tuple[bytes, bytes]:
    """Half-open byte range selecting exactly one partition tuple's
    staged rows — prefix successor on the FULL ``rows/ + group`` bytes,
    exact because the group encoding is prefix-free (no other tuple's
    keys share the prefix)."""
    start = group_rows_prefix(group)
    return start, _prefix_successor(start)


# -- stats/ -------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class StatsValue:
    """Decoded form of a ``stats/`` value: one partition tuple's staged
    rows and bytes.

    ``staged_bytes`` sums the staged payload bytes and ``row_count`` the
    staged rows. ``first_staged_ts`` / ``last_staged_ts`` are
    microseconds since the Unix epoch. ``first_staged_ts`` is set when
    the tuple's first row is staged after the tuple stood empty and is
    NEVER advanced by later arrivals — the flush planner's age trigger
    and the tail's freshness SLA stand on that property.
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


def stats_key(group: PartitionTuple) -> bytes:
    """The ``stats/`` key for one partition tuple."""
    return STATS_PREFIX + encode_group(group)


def decode_stats_key(data: bytes) -> PartitionTuple:
    """The partition tuple of a ``stats/`` key."""
    p = len(STATS_PREFIX)
    _require_key(STATS_PREFIX, data, min_length=p + 1)
    return decode_group(data[p:])


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
    group: PartitionTuple

    def __post_init__(self) -> None:
        _require_int64("first_staged_ts", self.first_staged_ts)


def encode_sched_age_key(key: SchedAgeKey) -> bytes:
    """Age-ordered so the readiness sweep for expired keys is a prefix
    range scan."""
    return (
        SCHED_AGE_PREFIX
        + _encode_i64_ordered(key.first_staged_ts)
        + encode_group(key.group)
    )


def decode_sched_age_key(data: bytes) -> SchedAgeKey:
    """Inverse of :func:`encode_sched_age_key`."""
    p = len(SCHED_AGE_PREFIX)
    _require_key(SCHED_AGE_PREFIX, data, min_length=p + 8 + 1)
    return SchedAgeKey(
        first_staged_ts=_decode_i64_ordered(data[p : p + 8]),
        group=decode_group(data[p + 8 :]),
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
    covering part of a partition tuple's staged batch — the flush's
    identity material (millpond's offsets line, restated as a value
    envelope)."""

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


def offsets_key(group: PartitionTuple) -> bytes:
    """The ``offsets/`` key recording one partition tuple's staged Kafka
    ranges."""
    return OFFSETS_PREFIX + encode_group(group)


def decode_offsets_key(data: bytes) -> PartitionTuple:
    """The partition tuple of an ``offsets/`` key."""
    p = len(OFFSETS_PREFIX)
    _require_key(OFFSETS_PREFIX, data, min_length=p + 1)
    return decode_group(data[p:])


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


def _check_group_fields(group: PartitionTuple, group_fields: tuple[str, ...]) -> None:
    if len(group_fields) != len(group):
        raise ValueError(
            f"{len(group_fields)} field name(s) for a {len(group)}-field "
            "partition tuple"
        )
    if any(not name for name in group_fields):
        raise ValueError("partition field names must be non-empty")


@dataclass(frozen=True, slots=True)
class PreparedRequest:
    """A persisted prepared commit request: the byte-exact request body
    plus the idempotency key it is published under and that key's
    derivation inputs. Persisted BEFORE publication and replayed
    byte-identically after an ambiguous response — never regenerated.

    ``persisted_at`` is the persistence instant in microseconds since the
    Unix epoch (the flusher's injected clock) and is the receipt-horizon
    guard's input: a receipt 404 may be answered with a blind replay only
    while the entry is younger than the horizon — past it, the receipt
    may have been PURGED server-side and a replay could duplicate an
    already-executed commit, so the flusher halts (flush.py).

    ``topic`` / ``partition`` / ``group`` (with ``group_fields``, the
    hoglake display names of the tuple's fields) are the derivation
    inputs the entry's key was minted from (see :func:`idempotency_key`):
    a persisted entry is self-describing, and recovery cross-checks them
    against the stage's own identity and the ``prepared/`` key — an entry
    naming another Kafka partition or another tuple is foreign state,
    loud corruption, never a silent replay.
    """

    idempotency_key: str
    body: bytes
    persisted_at: int
    topic: str
    partition: int
    group: PartitionTuple
    group_fields: tuple[str, ...]

    def __post_init__(self) -> None:
        key_len = len(self.idempotency_key.encode("utf-8"))
        if key_len > _UINT16_MAX:
            raise ValueError(f"idempotency key too long: {key_len} bytes")
        if len(self.body) > _UINT32_MAX:
            raise ValueError(f"prepared body too long: {len(self.body)} bytes")
        _require_uint64("persisted_at", self.persisted_at)
        if not self.topic:
            raise ValueError("topic must be non-empty")
        encoded_topic = self.topic.encode("utf-8")
        if len(encoded_topic) > _UINT16_MAX:
            raise ValueError(
                f"topic does not fit in a u16 length: {len(encoded_topic)} bytes"
            )
        if not 0 <= self.partition <= _UINT32_MAX:
            raise ValueError(
                f"partition must fit in an unsigned 32-bit field, got {self.partition}"
            )
        _check_group_fields(self.group, self.group_fields)


def prepared_key(group: PartitionTuple, first_offset: int) -> bytes:
    """The ``prepared/`` key of one staged batch. ``first_offset`` is the
    batch's low-water mark and disambiguates successive batches of one
    tuple; it sorts numerically, so a tuple's prepared entries scan in
    batch order."""
    _require_int64("first_offset", first_offset)
    return PREPARED_PREFIX + encode_group(group) + _encode_i64_ordered(first_offset)


def group_prepared_range(group: PartitionTuple) -> tuple[bytes, bytes]:
    """Half-open byte range selecting exactly one partition tuple's
    ``prepared/`` entries (the flush's outstanding-entry gate scans this
    prefix) — prefix successor on the full ``prepared/ + group`` bytes."""
    start = PREPARED_PREFIX + encode_group(group)
    return start, _prefix_successor(start)


@dataclass(frozen=True, slots=True)
class PreparedKey:
    """Decoded form of a ``prepared/`` key."""

    group: PartitionTuple
    first_offset: int


def decode_prepared_key(data: bytes) -> PreparedKey:
    """Inverse of :func:`prepared_key`: the trailing 8 bytes are the
    first offset, the middle is exactly one partition tuple."""
    p = len(PREPARED_PREFIX)
    _require_key(PREPARED_PREFIX, data, min_length=p + 1 + 8)
    return PreparedKey(
        group=decode_group(data[p:-8]),
        first_offset=_decode_i64_ordered(data[-8:]),
    )


def encode_prepared_value(request: PreparedRequest) -> bytes:
    """Version 4: key_len(u16) key(utf8) topic_len(u16) topic(utf8)
    partition(u32) arity(u8) [name_len(u16) name tag(u8) [value_len(u16)
    value]]* body_len(u32) body persisted_at(u64)."""
    out = bytearray([PREPARED_VERSION])
    out += _encode_utf8_u16("idempotency key", request.idempotency_key)
    out += _encode_utf8_u16("topic", request.topic)
    out += request.partition.to_bytes(4, "big")
    out.append(len(request.group))
    for name, value in zip(request.group_fields, request.group, strict=True):
        out += _encode_utf8_u16("partition field name", name)
        if value is None:
            out.append(GROUP_NULL)
        else:
            out.append(GROUP_PRESENT)
            out += _encode_utf8_u16("partition value", value)
    out += len(request.body).to_bytes(4, "big")
    out += request.body
    out += _encode_u64(request.persisted_at)
    return bytes(out)


def decode_prepared_value(data: bytes) -> PreparedRequest:
    """Inverse of :func:`encode_prepared_value`. Refuses any version but
    4 and any malformed or trailing byte."""
    data = bytes(data)
    if len(data) < 1:
        raise ValueError("prepared value is empty")
    if data[0] != PREPARED_VERSION:
        raise ValueError(f"unknown prepared value version {data[0]}")
    key, pos = _decode_utf8_u16(data, 1, "prepared value: key")
    topic, pos = _decode_utf8_u16(data, pos, "prepared value: topic")
    if len(data) < pos + 4 + 1:
        raise ValueError("truncated prepared value: partition / arity")
    partition = int.from_bytes(data[pos : pos + 4], "big", signed=False)
    pos += 4
    arity = data[pos]
    pos += 1
    names: list[str] = []
    values: list[str | None] = []
    for index in range(arity):
        name, pos = _decode_utf8_u16(data, pos, f"prepared value: field {index} name")
        if len(data) < pos + 1:
            raise ValueError(f"truncated prepared value: field {index} tag")
        tag = data[pos]
        pos += 1
        if tag == GROUP_NULL:
            values.append(None)
        elif tag == GROUP_PRESENT:
            value, pos = _decode_utf8_u16(
                data, pos, f"prepared value: field {index} value"
            )
            values.append(value)
        else:
            raise ValueError(f"prepared value: field {index} has tag {tag:#04x}")
        names.append(name)
    if len(data) < pos + 4:
        raise ValueError("truncated prepared value: body length")
    body_len = int.from_bytes(data[pos : pos + 4], "big")
    pos += 4
    if len(data) != pos + body_len + 8:
        raise ValueError(
            f"prepared body length mismatch: field says {body_len}, "
            f"value holds {len(data) - pos - 8}"
        )
    return PreparedRequest(
        idempotency_key=key,
        body=data[pos : pos + body_len],
        persisted_at=int.from_bytes(data[pos + body_len :], "big", signed=False),
        topic=topic,
        partition=partition,
        group=tuple(values),
        group_fields=tuple(names),
    )


# -- flushed/ -----------------------------------------------------------------
#
# LEGACY prefix. Pre-atomic-settlement documentation wrote one marker per
# committed range here; settlement has in fact always been ONE atomic
# transaction (:meth:`PartitionStage.commit_flushed`), so a readable marker
# was always fully settled state. Nothing writes this prefix any more;
# :meth:`PartitionStage.recover` deletes whatever it finds under it, in
# bounded pages, without decoding. ``FlushedKey`` is the settlement-WINDOW
# type (the delete selection below); it names no key this build writes.


@dataclass(frozen=True, slots=True)
class FlushedKey:
    """A settled flush window: the committed Kafka offset range
    ``[first_offset, last_offset]`` of one partition tuple's staged rows,
    with the tuple as hoglake partition values (``group_fields`` the
    fields' display names, ``group`` the wire strings). Enough to delete
    exactly its ``rows/`` entries (:func:`row_in_flushed_range`)."""

    group: PartitionTuple
    first_offset: int
    last_offset: int
    group_fields: tuple[str, ...]

    def __post_init__(self) -> None:
        _check_group_fields(self.group, self.group_fields)
        _require_int64("first_offset", self.first_offset)
        _require_int64("last_offset", self.last_offset)
        if self.first_offset > self.last_offset:
            raise ValueError(
                f"first_offset {self.first_offset} > last_offset {self.last_offset}"
            )

    @property
    def label(self) -> str:
        """The tuple's human form (:func:`group_label`)."""
        return group_label(self.group_fields, self.group)


def flushed_range() -> tuple[bytes, bytes]:
    """Half-open range selecting every (legacy) ``flushed/`` marker — the
    recovery collector's scan bounds."""
    return FLUSHED_PREFIX, _prefix_successor(FLUSHED_PREFIX)


def flushed_rows_scan_range(key: FlushedKey) -> tuple[bytes, bytes]:
    """The scan bounds of a settlement delete: the tuple's whole
    ``rows/`` slice. No narrower byte range can isolate the window
    (row-key order is (group, timestamp, offset)); apply
    :func:`row_in_flushed_range` to every scanned key and delete exactly
    those."""
    return group_rows_range(key.group)


def row_in_flushed_range(row: RowKey, flushed: FlushedKey) -> bool:
    """True iff ``row`` is one of the settled window's staged rows — the
    exact settlement-delete selection.

    Within one Kafka-partition instance a row's offset is unique to its
    record, so the window identifies its rows exactly. A younger range of
    the same tuple (offsets above ``last_offset``) sorts INSIDE the scan
    range and must survive the delete.
    """
    return (
        row.group == flushed.group
        and flushed.first_offset <= row.offset <= flushed.last_offset
    )


# -- poison/ ------------------------------------------------------------------
#
# The consume-time quarantine (docs/kafka-ingestion.md §Delivery semantics):
# a record that can never become a row — a payload that is not a JSON object,
# a malformed partition source, no usable timestamp, a tombstone — is written here, keyed by its Kafka offset,
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
    if len(data) != POISON_KEY_LEN:
        raise ValueError(f"expected {POISON_KEY_LEN} bytes, got {len(data)}")
    _require_key(POISON_PREFIX, data, min_length=POISON_KEY_LEN)
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


def group_key_form(group: PartitionTuple) -> str:
    """The partition tuple as the idempotency key names it:
    :func:`group_label` with positional field names ``k0``, ``k1``, … —
    a function of the tuple ALONE (``k0=42/k1=681``; empty for an
    unpartitioned table; a null component is a bare ``k<i>``)."""
    return group_label(tuple(f"k{i}" for i in range(len(group))), group)


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
    group: PartitionTuple,
    first_offset: int,
    last_offset: int,
) -> str:
    """The commit idempotency key for one staged window of one partition
    tuple.

    A UUIDv5 over :data:`IDEMPOTENCY_NAMESPACE` and the canonical name
    ``millrace:v2:table=<table>:topic=<topic>:partition=<n>:group=<k0=…/
    k1=…>:offsets=<first>-<last>``, the group rendered by
    :func:`group_key_form` — POSITIONAL names (``k<i>``), never the
    spec's display names: the key is a pure function of the staged
    tuple, so a spec change (even one of the same arity) between an
    upload and its rebuild cannot change the key or the object names
    derived from it. Display names live in the commit message and the
    ``prepared/`` envelope only. Same inputs, same key, in every
    process, forever.
    ``table_uuid`` is canonicalized (parsed and re-emitted) so
    case-variant spellings of one incarnation name one key; ``topic`` is
    NOT canonicalized (Kafka topic names are case-sensitive).

    ``topic``/``partition`` are derivation inputs because Kafka offsets
    are per partition: without them, two Kafka partitions flushing one
    tuple over the same offset range derive the SAME key — and the same
    object URIs, since those derive from the key — so the second
    partition's upload would overwrite the first's committed objects
    before the server refused the publish as a key reuse.
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
    _require_int64("first_offset", first_offset)
    _require_int64("last_offset", last_offset)
    if first_offset > last_offset:
        raise ValueError(f"first_offset {first_offset} > last_offset {last_offset}")
    name = (
        f"millrace:v2:table={canonical_table}:topic={topic}:partition={partition}"
        f":group={group_key_form(group)}"
        f":offsets={first_offset}-{last_offset}"
    )
    return str(uuid.uuid5(IDEMPOTENCY_NAMESPACE, name))
