"""Staging key codecs (docs/kafka-ingestion.md §Staging layout).

    rows/<group><offset>                       → event payload bytes
    stats/<group>                              → StatsValue
    sched_age/<first_staged_ts><group>         → ∅ (empty value)
    prepared/<group><first_offset>             → PreparedRequest envelope
    meta/fence                                 → fence-probe value (not a row)
    flushed/…                                  → LEGACY: never written

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
follows the Kafka partition to its new owner, never the pod. Kafka
offsets are unique within an instance, so ``(group, offset)`` names one
staged row.

The LSM's ordering is load-bearing: ``rows`` key bytes must sort in
(group, offset) order, so a flush reads one contiguous range and a
scan cut short by a cap is an exact offset PREFIX of the group's staged
rows (its window is ``[first scanned, last scanned]`` and holds no
unscanned row), and ``sched_age`` bytes must sort in (first_staged_ts,
group) order so the readiness sweep is a prefix range scan.

Encoding rules
--------------

- All multi-byte integers are big-endian and fixed width, so no field
  needs a separator.
- Signed values IN KEYS (Kafka offset, first_staged_ts) use offset
  binary: ``value + 2**63`` encoded as an
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
  trailing fields (the offset) parse off the END of a key, and
  :func:`decode_group` refuses anything that is not exactly one
  well-formed tuple.
- Every value envelope starts with a 1-byte version. A reader refuses
  any version it does not know.

Value formats
-------------

- ``stats``:   version(1) staged_bytes(u64) first_staged_ts(i64)
               last_staged_ts(i64) row_count(u64) — 33 bytes fixed.
- ``prepared``: version(6) key_len(u16) idempotency_key(utf8)
               topic_len(u16) topic(utf8) partition(u32)
               arity(u8), then per partition field name_len(u16)
               name(utf8) tag(u8) [value_len(u16) value(utf8)]
               body_len(u32) body(bytes) persisted_at(u64, micros since
               the Unix epoch) row_count(u64) digest(32 bytes)
               scanned_tag(u8) scanned_at(i64). The body is the
               byte-exact prepared commit request, written once and
               replayed byte-identically after an ambiguous response.
               ``row_count`` is the number of staged rows the request's
               window held when it was built — the recovery settle's
               expected count, checked against its pre-scan (a window that
               holds a different count by then is not the window the
               commit published). ``digest`` is the window's
               :func:`content_digest` (the raw 32 bytes; hex in the
               dataclass), the content the key was derived from — kept
               so a settle can verify content, not only count.
               ``scanned_at`` (tag 0x01, micros) is the staging-clock
               instant taken just before the window's scan when that scan
               saw the group's WHOLE staged slice (tag 0x00 and eight
               zero bytes when a cap cut it short): every row the settle
               leaves behind was staged after it, so every settle of the
               entry lifts the group's ``first_staged_ts`` to it.
               ``persisted_at`` is forensic: the flusher logs an
               entry's age when it replays one (flush.py) and gates
               nothing on it. ``topic`` / ``partition``
               and the partition tuple are the idempotency key's
               derivation inputs: they make a persisted entry
               self-describing, and recovery cross-checks them against
               the stage's own identity and the ``prepared/`` key it
               sits under (an entry naming another partition or tuple is
               foreign state, refused loudly). The tuple travels as hoglake partition
               values — each field's display name beside its wire
               string — so the envelope reads like the file it
               registers.
- ``meta/fence``: version(1) offset(i64) — the consumer's fence probe
               (:func:`encode_fence_probe_value`): a poll that staged
               nothing on a partition still writes this one key durably
               before committing, so a fenced writer learns it lost the
               path before it commits. Not a row: no scan, stats or
               gauge reads the ``meta/`` prefix.
- ``flushed``:  LEGACY — pre-atomic-settlement builds wrote a marker per
               committed range. Settlement has always been one atomic
               write, so the repair state the marker existed for is
               unreachable and nothing writes the prefix any more;
               :meth:`PartitionStage.recover` collects leftover markers in
               bounded pages WITHOUT decoding them.

The settlement window
---------------------

A committed flush settles by deleting its window's rows by EXACT key, in
one of two forms. The FRESH form (the flush that scanned the window)
deletes exactly the row keys its scan read: a row that entered the
window after the scan (a replay below the settled floor, a re-key) is
not one of them and survives to a later window. The WINDOW form (a
recovered ``prepared/`` entry, whose scanned keys did not survive the
restart) uses the range: byte order is (group, offset), so the window
``[first_offset, last_offset]`` is one contiguous byte range,
:func:`window_rows_range`, which a pre-scan reads (its count checked
against the entry's persisted ``row_count``) before the read keys are
deleted. Within one Kafka-partition instance a row's offset is unique,
so the window identifies its rows exactly; rows of the tuple below or
above the window sort outside the range and survive either form.

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
last_offset, content digest)`` — a UUIDv5 over a pinned namespace and a
canonical name line ``millrace:v3:table=<uuid>:topic=<topic>:partition=
<n>:group=<k0=…/k1=…>:offsets=<first>-<last>:digest=<sha256 hex>``. The
digest (:func:`content_digest`) is SHA-256 over the window's staged
rows in offset order, so the key names the window's CONTENT, not just
its bounds: a re-staged window with identical rows has the same key
(its receipt dedupes it), and different rows under the same bounds (a
recreated topic, an offset restart, a rewind that re-keys) have a
different key and publish as a new commit — never settled from another
commit's receipt. The group is
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
offset range must never produce the same key (nor share the
``…/{key}/`` object prefix under it). BOTH offset ends are in the name because
"everything up to N" is not a row set once a partition rewinds.

This module is pure: no I/O, no SlateDB, no clocks.
"""

from __future__ import annotations

import hashlib
import re
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Final
from urllib.parse import quote

ROWS_PREFIX: Final = b"rows/"
STATS_PREFIX: Final = b"stats/"
SCHED_AGE_PREFIX: Final = b"sched_age/"
PREPARED_PREFIX: Final = b"prepared/"
FLUSHED_PREFIX: Final = b"flushed/"
META_PREFIX: Final = b"meta/"
#: The fence probe's one key (module docstring §Value formats).
FENCE_PROBE_KEY: Final = META_PREFIX + b"fence"

_INT64_MIN: Final = -(1 << 63)
_INT64_MAX: Final = (1 << 63) - 1
_UINT64_MAX: Final = (1 << 64) - 1
_UINT32_MAX: Final = (1 << 32) - 1
_UINT16_MAX: Final = (1 << 16) - 1
_UINT8_MAX: Final = (1 << 8) - 1
_SIGN_OFFSET: Final = 1 << 63

STATS_VERSION: Final = 1
FENCE_PROBE_VERSION: Final = 1
PREPARED_VERSION: Final = 6

#: Component tags of the partition-tuple encoding.
GROUP_NULL: Final = 0x00
GROUP_PRESENT: Final = 0x01

#: One hoglake partition tuple in wire form: per live-spec field, in spec
#: order, the ``partition_values`` string a file of the tuple carries
#: (None for a null component). The empty tuple is an unpartitioned
#: table's one group.
PartitionTuple = tuple[str | None, ...]

_OFFSET_LEN: Final = 8
STATS_VALUE_LEN: Final = 1 + 8 + 8 + 8 + 8
FENCE_PROBE_VALUE_LEN: Final = 1 + 8
#: persisted_at(u64) + row_count(u64) + digest(32 bytes) + scanned_tag(u8)
#: + scanned_at(i64) after the body.
_PREPARED_TAIL_LEN: Final = 8 + 8 + 32 + 1 + 8
#: Tags of the envelope's optional ``scanned_at``.
SCANNED_ABSENT: Final = 0x00
SCANNED_PRESENT: Final = 0x01
#: Bytes buffered per ``hashlib`` update in :func:`content_digest` (well
#: above CPython's 2048-byte GIL-release threshold).
_DIGEST_CHUNK: Final = 1 << 20

#: A content digest as the idempotency name line carries it.
_DIGEST_FORM: Final = re.compile(r"[0-9a-f]{64}")


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
    tuple. ``offset`` is the Kafka offset of the record, unique within
    the instance (module docstring)."""

    group: PartitionTuple
    offset: int

    def __post_init__(self) -> None:
        _require_int64("offset", self.offset)


def encode_row_key(key: RowKey) -> bytes:
    """Encode so byte order matches (group, offset) sort order."""
    return ROWS_PREFIX + encode_group(key.group) + _encode_i64_ordered(key.offset)


def decode_row_key(data: bytes) -> RowKey:
    """Inverse of :func:`encode_row_key`: the trailing 8 bytes are the
    offset, the middle is exactly one partition tuple."""
    p = len(ROWS_PREFIX)
    _require_key(ROWS_PREFIX, data, min_length=p + 1 + _OFFSET_LEN)
    return RowKey(
        group=decode_group(data[p:-_OFFSET_LEN]),
        offset=_decode_i64_ordered(data[-_OFFSET_LEN:]),
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
    NEVER advanced by later arrivals; a settle that leaves rows lifts it
    to the settled scan's start (stage.py), so it bounds the staging
    time of the oldest staged row from below — the flush planner's age
    trigger and the tail's freshness SLA stand on that property.
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
    Unix epoch (the flusher's injected clock). It is forensic: the
    flusher logs the entry's age when a receipt 404 replays it, and no
    decision depends on it (flush.py).

    ``row_count`` is the number of staged rows the window held when the
    request was built: every recovery settle of the entry deletes the
    window only if it still holds exactly that many rows. ``digest`` is
    the window's :func:`content_digest` (64 lowercase hex), the content
    input of the key — persisted so a settle can verify the window's
    rows are the published ones, not only their number (a topic
    recreated under the same name can re-put the same keys with new
    payloads); that verification is not wired yet. ``scanned_at`` is the
    staging-clock instant (µs) taken just before the window's scan when
    that scan saw the group's whole staged slice, None when a cap cut it
    short: a settle of the entry lifts the group's ``first_staged_ts`` to
    it when rows remain (they were all staged after it).

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
    row_count: int
    digest: str
    scanned_at: int | None

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
        _require_uint64("row_count", self.row_count)
        if not _DIGEST_FORM.fullmatch(self.digest):
            raise ValueError(
                f"digest must be 64 lowercase hex characters, got {self.digest!r}"
            )
        if self.scanned_at is not None:
            _require_int64("scanned_at", self.scanned_at)


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
    """Version 6: key_len(u16) key(utf8) topic_len(u16) topic(utf8)
    partition(u32) arity(u8) [name_len(u16) name tag(u8) [value_len(u16)
    value]]* body_len(u32) body persisted_at(u64) row_count(u64)
    digest(32 bytes) scanned_tag(u8) scanned_at(i64)."""
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
    out += _encode_u64(request.row_count)
    out += bytes.fromhex(request.digest)
    if request.scanned_at is None:
        out.append(SCANNED_ABSENT)
        out += bytes(8)
    else:
        out.append(SCANNED_PRESENT)
        out += _encode_i64(request.scanned_at)
    return bytes(out)


def decode_prepared_value(data: bytes) -> PreparedRequest:
    """Inverse of :func:`encode_prepared_value`. Refuses any version but
    6 and any malformed or trailing byte."""
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
    if len(data) != pos + body_len + _PREPARED_TAIL_LEN:
        raise ValueError(
            f"prepared body length mismatch: field says {body_len}, "
            f"value holds {len(data) - pos - _PREPARED_TAIL_LEN}"
        )
    tail = pos + body_len
    scanned_tag = data[tail + 48]
    scanned_raw = data[tail + 49 :]
    scanned_at: int | None
    if scanned_tag == SCANNED_PRESENT:
        scanned_at = int.from_bytes(scanned_raw, "big", signed=True)
    elif scanned_tag == SCANNED_ABSENT and scanned_raw == bytes(8):
        scanned_at = None
    else:
        raise ValueError(
            f"prepared value: scanned_at has tag {scanned_tag:#04x} with "
            f"value {scanned_raw.hex()}"
        )
    return PreparedRequest(
        idempotency_key=key,
        body=data[pos:tail],
        persisted_at=int.from_bytes(data[tail : tail + 8], "big", signed=False),
        row_count=int.from_bytes(data[tail + 8 : tail + 16], "big", signed=False),
        digest=data[tail + 16 : tail + 48].hex(),
        scanned_at=scanned_at,
        topic=topic,
        partition=partition,
        group=tuple(values),
        group_fields=tuple(names),
    )


# -- flushed/ -----------------------------------------------------------------
#
# LEGACY prefix. Pre-atomic-settlement documentation wrote one marker per
# committed range here; settlement has in fact always been ONE atomic
# write (:meth:`PartitionStage.commit_flushed`), so a readable marker
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
    exactly its ``rows/`` entries (:func:`window_rows_range`)."""

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


def window_rows_range(key: FlushedKey) -> tuple[bytes, bytes]:
    """Half-open byte range selecting EXACTLY the window's staged rows:
    ``rows/<group><first_offset>`` up to (excluding)
    ``rows/<group><last_offset + 1>`` — row-key order is (group,
    offset), so the window is one contiguous slice and the settlement
    deletes every key in it. At ``last_offset == INT64_MAX`` the end is
    the group's prefix successor (no larger offset exists)."""
    prefix = group_rows_prefix(key.group)
    start = prefix + _encode_i64_ordered(key.first_offset)
    if key.last_offset == _INT64_MAX:
        return start, _prefix_successor(prefix)
    return start, prefix + _encode_i64_ordered(key.last_offset + 1)


def row_in_flushed_range(row: RowKey, flushed: FlushedKey) -> bool:
    """True iff ``row``'s offset lies in the window ``[first_offset,
    last_offset]`` — the offset half of :func:`window_rows_range`, for
    rows already scoped to the window's tuple."""
    return flushed.first_offset <= row.offset <= flushed.last_offset


# -- meta/ --------------------------------------------------------------------


def encode_fence_probe_value(offset: int) -> bytes:
    """Version 1: the highest offset the probing poll handled, as i64.
    Forensic only — the probe exists for its durable WRITE (a fenced
    writer's write fails at its durability wait), never read back by
    the pipeline."""
    _require_int64("offset", offset)
    return bytes([FENCE_PROBE_VERSION]) + _encode_i64(offset)


def decode_fence_probe_value(data: bytes) -> int:
    """Inverse of :func:`encode_fence_probe_value`."""
    if len(data) != FENCE_PROBE_VALUE_LEN:
        raise ValueError(f"expected {FENCE_PROBE_VALUE_LEN} bytes, got {len(data)}")
    if data[0] != FENCE_PROBE_VERSION:
        raise ValueError(f"unknown fence probe value version {data[0]}")
    return int.from_bytes(data[1:], "big", signed=True)


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


def content_digest(rows: Iterable[tuple[int, bytes]]) -> str:
    """SHA-256 (lowercase hex) of one window's staged rows, given as
    ``(offset, payload)`` in offset order: per row ``offset (i64,
    big-endian two's complement) || payload length (u64, big-endian) ||
    payload``. The length field makes the framing injective — no two
    distinct row sequences share an input byte string. The idempotency
    key's content input (:func:`idempotency_key`).

    The framed rows are buffered and fed to ``update`` in chunks of at
    least :data:`_DIGEST_CHUNK` bytes: CPython releases the GIL inside
    ``update`` only for inputs of 2048 bytes or more, so per-row updates
    of typical 0.1–2 KiB events would hash with the GIL held and stall
    the event loop that shares the process. Same digest either way."""
    h = hashlib.sha256()
    buf = bytearray()
    for offset, payload in rows:
        _require_int64("offset", offset)
        buf += _encode_i64(offset)
        buf += _encode_u64(len(payload))
        buf += payload
        if len(buf) >= _DIGEST_CHUNK:
            h.update(buf)
            buf.clear()
    h.update(buf)
    return h.hexdigest()


def idempotency_key(
    table_uuid: str,
    topic: str,
    partition: int,
    group: PartitionTuple,
    first_offset: int,
    last_offset: int,
    digest: str,
) -> str:
    """The commit idempotency key for one staged window of one partition
    tuple.

    A UUIDv5 over :data:`IDEMPOTENCY_NAMESPACE` and the canonical name
    ``millrace:v3:table=<table>:topic=<topic>:partition=<n>:group=<k0=…/
    k1=…>:offsets=<first>-<last>:digest=<hex>``, ``digest`` the window's
    :func:`content_digest` — so the key names the window's rows, not
    only its bounds — and the group rendered by
    :func:`group_key_form` — POSITIONAL names (``k<i>``), never the
    spec's display names: the key is a pure function of the staged
    tuple, so a spec change (even one of the same arity) between an
    upload and its rebuild cannot change the key. Display names live in the commit message and the
    ``prepared/`` envelope only. Same inputs, same key, in every
    process, forever.
    ``table_uuid`` is canonicalized (parsed and re-emitted) so
    case-variant spellings of one incarnation name one key; ``topic`` is
    NOT canonicalized (Kafka topic names are case-sensitive).

    ``topic``/``partition`` are derivation inputs because Kafka offsets
    are per partition: without them, two Kafka partitions flushing one
    tuple over the same offset range derive the SAME key, and the
    server would answer the second partition's commit from the first's
    receipt (or refuse it as a key reuse) instead of publishing it.
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
    if not _DIGEST_FORM.fullmatch(digest):
        raise ValueError(f"digest must be 64 lowercase hex characters, got {digest!r}")
    name = (
        f"millrace:v3:table={canonical_table}:topic={topic}:partition={partition}"
        f":group={group_key_form(group)}"
        f":offsets={first_offset}-{last_offset}:digest={digest}"
    )
    return str(uuid.uuid5(IDEMPOTENCY_NAMESPACE, name))
