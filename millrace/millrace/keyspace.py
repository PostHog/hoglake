"""Staging key codecs. Pure: no I/O, no SlateDB, no clocks.

    rows/<group><offset>   → raw Kafka payload bytes
    meta/fence             → offset (the consumer's fence probe; not a row)
    meta/table_uuid        → the destination incarnation first committed to

That is the whole keyspace. Group sizes and ages live in memory only
(stage.py): staged state never outlives a process, because every
replica drains its stages at startup before it consumes.

``<group>`` is one hoglake partition tuple of the destination table:
per live-spec field, in spec order, the wire string a file of that
tuple carries in ``partition_values`` (None for a null component). An
unpartitioned table has the empty tuple.

Why the byte layout matters:

- Row keys sort in ``(group, offset)`` order, so a flush reads one
  contiguous range, and a scan cut short by a byte budget is an exact
  offset PREFIX of the group's staged rows.
- The group encoding is PREFIX-FREE: ``arity(u8)``, then per component
  ``0x00`` (null) or ``0x01 len(u16) utf8``. Without the arity byte the
  tuple ``(a,)`` (keyed under an older one-field spec) would be a byte
  prefix of ``(a, b)``, and one group's scan or delete would reach into
  another group's rows.
- Offsets use offset binary (``value + 2**63`` as big-endian u64) so
  byte order equals numeric order.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final
from urllib.parse import quote

ROWS_PREFIX: Final = b"rows/"
META_PREFIX: Final = b"meta/"
#: The fence probe's one key.
FENCE_PROBE_KEY: Final = META_PREFIX + b"fence"
#: The destination incarnation this stage's rows were first committed
#: to (UTF-8 table_uuid), so a recreated table halts across restarts.
TABLE_UUID_KEY: Final = META_PREFIX + b"table_uuid"

_INT64_MIN: Final = -(1 << 63)
_INT64_MAX: Final = (1 << 63) - 1
_UINT16_MAX: Final = (1 << 16) - 1
_UINT8_MAX: Final = (1 << 8) - 1
_SIGN_OFFSET: Final = 1 << 63
_OFFSET_LEN: Final = 8

#: Component tags of the partition-tuple encoding.
GROUP_NULL: Final = 0x00
GROUP_PRESENT: Final = 0x01

#: One hoglake partition tuple in wire form.
PartitionTuple = tuple[str | None, ...]


def _require_int64(name: str, value: int) -> None:
    if not _INT64_MIN <= value <= _INT64_MAX:
        raise ValueError(f"{name} must fit in a signed 64-bit field, got {value}")


def _encode_i64_ordered(value: int) -> bytes:
    """Offset-binary form: byte order matches numeric order on all int64."""
    return (value + _SIGN_OFFSET).to_bytes(8, "big", signed=False)


def _decode_i64_ordered(data: bytes) -> int:
    return int.from_bytes(data, "big", signed=False) - _SIGN_OFFSET


def prefix_successor(prefix: bytes) -> bytes:
    """The smallest byte string strictly greater than every string with
    this prefix: the exclusive end of the prefix's half-open range."""
    stripped = prefix.rstrip(b"\xff")
    if not stripped:
        raise ValueError("a prefix of only 0xFF bytes has no successor")
    return stripped[:-1] + bytes([stripped[-1] + 1])


# -- the partition tuple (group) ---------------------------------------------


def encode_group(group: PartitionTuple) -> bytes:
    """The prefix-free byte form of one partition tuple."""
    if len(group) > _UINT8_MAX:
        raise ValueError(f"a partition tuple has at most {_UINT8_MAX} fields")
    out = bytearray([len(group)])
    for component in group:
        if component is None:
            out.append(GROUP_NULL)
            continue
        if not isinstance(component, str):
            raise TypeError(
                "partition tuple components are wire strings or None, got "
                f"{type(component).__name__}"
            )
        raw = component.encode("utf-8")
        if len(raw) > _UINT16_MAX:
            raise ValueError(
                f"partition value does not fit in a u16 length: {len(raw)} bytes"
            )
        out.append(GROUP_PRESENT)
        out += len(raw).to_bytes(2, "big") + raw
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
            continue
        if tag != GROUP_PRESENT:
            raise ValueError(
                f"corrupt partition tuple: component {index} has tag {tag:#04x}"
            )
        if len(data) < pos + 2:
            raise ValueError(f"corrupt partition tuple component {index}: length")
        length = int.from_bytes(data[pos : pos + 2], "big")
        pos += 2
        if len(data) < pos + length:
            raise ValueError(
                f"corrupt partition tuple component {index}: {length} bytes declared"
            )
        try:
            out.append(bytes(data[pos : pos + length]).decode("utf-8"))
        except UnicodeDecodeError as e:
            raise ValueError(
                f"corrupt partition tuple component {index} is not UTF-8: {e}"
            ) from e
        pos += length
    return tuple(out), pos


def decode_group(data: bytes) -> PartitionTuple:
    """Inverse of :func:`encode_group`. Total: any input decodes to
    exactly one tuple or raises ValueError (truncation, unknown tag,
    non-UTF-8, trailing bytes)."""
    group, pos = _decode_group_at(bytes(data), 0)
    if pos != len(data):
        raise ValueError(f"corrupt partition tuple: {len(data) - pos} trailing byte(s)")
    return group


def group_label(fields: tuple[str, ...], group: PartitionTuple) -> str:
    """The human form of one partition tuple, for logs and the commit
    message: ``name=value`` per field joined by ``/``
    (``team_id=42/timestamp_month=681``), a null component as its bare
    field name. Names and values are percent-encoded, so the form is
    ASCII and injective."""
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
    """Decoded ``rows/`` key: one staged row. ``offset`` is the record's
    Kafka offset, unique within the instance (one instance per Kafka
    partition)."""

    group: PartitionTuple
    offset: int

    def __post_init__(self) -> None:
        _require_int64("offset", self.offset)


def encode_row_key(key: RowKey) -> bytes:
    """Encode so byte order matches (group, offset) order."""
    return ROWS_PREFIX + encode_group(key.group) + _encode_i64_ordered(key.offset)


def decode_row_key(data: bytes) -> RowKey:
    """Inverse of :func:`encode_row_key`: the trailing 8 bytes are the
    offset, the middle is exactly one partition tuple."""
    p = len(ROWS_PREFIX)
    if not data.startswith(ROWS_PREFIX):
        raise ValueError(f"key does not start with {ROWS_PREFIX!r}: {data!r}")
    if len(data) < p + 1 + _OFFSET_LEN:
        raise ValueError(
            f"expected at least {p + 1 + _OFFSET_LEN} bytes, got {len(data)}"
        )
    return RowKey(
        group=decode_group(data[p:-_OFFSET_LEN]),
        offset=_decode_i64_ordered(data[-_OFFSET_LEN:]),
    )


def group_rows_prefix(group: PartitionTuple) -> bytes:
    """The shared prefix of every ``rows/`` key of one partition tuple."""
    return ROWS_PREFIX + encode_group(group)


def group_rows_range(group: PartitionTuple) -> tuple[bytes, bytes]:
    """Half-open byte range selecting exactly one tuple's staged rows
    (exact because the group encoding is prefix-free)."""
    start = group_rows_prefix(group)
    return start, prefix_successor(start)


def rows_range() -> tuple[bytes, bytes]:
    """Half-open byte range of every staged row."""
    return ROWS_PREFIX, prefix_successor(ROWS_PREFIX)


# -- meta/ --------------------------------------------------------------------


def encode_fence_probe_value(offset: int) -> bytes:
    """The highest offset the probing poll handled (i64, forensic only:
    the probe exists for its durable write)."""
    _require_int64("offset", offset)
    return offset.to_bytes(8, "big", signed=True)
