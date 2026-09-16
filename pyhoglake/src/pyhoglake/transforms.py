"""Client-side Iceberg partition transforms for the writer path.

Implements the server's Transform vocabulary (``identity``, ``bucket``,
``year``, ``month``, ``day``, ``hour`` — Model.kt, iceberg-federation.md
§3) plus ``truncate`` with Iceberg-spec semantics, ready for the day the
server vocabulary grows it. Semantics follow the Apache Iceberg Table
Spec exactly:

- ``year``/``month``/``day``/``hour`` produce **epoch-relative ints**
  (years/months since 1970, days/hours since the epoch — floored, so
  pre-1970 values are negative), never calendar components.
- ``bucket[N]`` is ``(murmur3_x86_32(encode(v)) & Integer.MAX_VALUE) % N``
  over the spec's Appendix-B hash encoding. NOTE: that encoding is NOT
  the bounds codec for every type — ``int``/``date`` hash as an 8-byte
  little-endian **long** (the bounds codec stores them as 4-byte ints).
- ``truncate[W]`` floors ints/longs (and decimal unscaled values) to the
  nearest lower multiple of W (negative-correct) and takes the first W
  **codepoints** of a string / W bytes of a binary.
- A null source value transforms to a null partition value (its own
  partition group), per Iceberg's null handling.
- ``json`` takes **identity only**. bucket hashes bytes and truncate
  slices them, but two documents equal as JSON (key order, whitespace,
  number spelling) have different bytes, so either transform would
  scatter equal values across partitions and prune away files that do
  match.
- ``uint32``/``uint64``/``timestamp_s``/``timestamp_ms``/``timestamp_ns``
  take identity and truncate but NOT bucket — see ``_BUCKETABLE`` for the
  hash-domain reason. The server's gate is identical
  (``AlterService.BUCKETABLE_TYPES``); the two sets are pinned equal by a
  test that parses the Kotlin.
- ``timestamp_ns`` partition values are NANOS as a decimal string, not
  an isoformat timestamp: a Python datetime cannot carry nanoseconds,
  and arrow will not even render a sub-microsecond ``timestamp[ns]`` as
  one. The other timestamp widths keep isoformat.

The wire shape for the commit's ``partition_values`` is a list of
strings by key_index (nullable — see openapi FileRegistration); the
server treats them as opaque and groups by equality, so
:func:`wire_string` defines this client's canonical stringification.

References: Apache Iceberg Table Spec — "Partition Transforms" and
Appendix B "32-bit Hash Requirements"
(https://iceberg.apache.org/spec/#partition-transforms,
https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements).
"""

from __future__ import annotations

import base64
import struct
import uuid as _uuid
from datetime import date, datetime, time
from decimal import Decimal, localcontext
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from .bounds import (
    _micros_since_epoch,
    _minimal_twos_complement,
    _nanos_since_epoch,
    _unscaled,
)
from .errors import ValidationError

_EPOCH_DATE = date(1970, 1, 1)
_MICROS_PER_HOUR = 3_600_000_000
_MICROS_PER_DAY = 86_400_000_000

#: Column types bucket() accepts. Three exclusion reasons:
#:
#:  - boolean/float/double: outside the Iceberg spec's Appendix-B hash
#:    domain outright.
#:  - json: see the note above _TRUNCATABLE.
#:  - uint32/uint64/timestamp_s/timestamp_ms/timestamp_ns: the
#:    hash-domain mismatch. Appendix B hashes the MAPPED Iceberg type's
#:    representation — timestamps as micros, uint64-as-decimal(20,0) as
#:    minimal two's-complement bytes, uint32-as-long as the
#:    zero-extended value — and hoglake has neither a cross-language
#:    contract for hashing on the mapped value nor cross-language bucket
#:    vectors proving both sides agree on it. A bucket value nobody has
#:    verified prunes silently and wrongly, so until those exist these
#:    five are identity/truncate only (they stay in _TRUNCATABLE; only
#:    bucket is withdrawn). Re-admitting them is a deliberate future
#:    change with those vectors attached, not a default.
#:
#: Must equal AlterService.BUCKETABLE_TYPES exactly — the server accepts
#: the spec, but the client is what computes the values, so a divergence
#: means an accepted spec the writer cannot honour. Pinned by a test that
#: parses the Kotlin.
_BUCKETABLE = frozenset(
    {
        "int8",
        "int16",
        "int",
        "long",
        "uint8",
        "uint16",
        "date",
        "time",
        "timestamp",
        "timestamptz",
        "string",
        "uuid",
        "binary",
        "decimal",
    }
)
#: json is deliberately absent from both gates: bucket hashes the bytes
#: and truncate slices them, but two documents that are EQUAL as JSON
#: (key order, whitespace, number spelling) have different bytes. Either
#: transform would scatter equal values across partitions and prune away
#: files that do match. Identity is the only honest transform for json —
#: the same gate the server enforces (AlterService.BUCKETABLE_TYPES
#: excludes json alongside boolean/float/double).
_TRUNCATABLE = frozenset(
    {
        "int8",
        "int16",
        "int",
        "long",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "string",
        "binary",
        "decimal",
    }
)
_YEAR_MONTH_DAY_TYPES = frozenset(
    {
        "date",
        "timestamp_s",
        "timestamp_ms",
        "timestamp",
        "timestamp_ns",
        "timestamptz",
    }
)
_HOUR_TYPES = frozenset(
    {"timestamp_s", "timestamp_ms", "timestamp", "timestamp_ns", "timestamptz"}
)

#: Integer column types that hash, truncate and stringify like int/long.
_INT_TYPES = frozenset(
    {"int8", "int16", "int", "long", "uint8", "uint16", "uint32", "uint64"}
)

_INT32_MASK = 0xFFFFFFFF
_INT31_MASK = 0x7FFFFFFF  # Java Integer.MAX_VALUE


def murmur3_32(data: bytes, seed: int = 0) -> int:
    """32-bit Murmur3 (x86 variant), signed result — bit-identical to
    Iceberg's ``BucketUtil`` (Guava ``Hashing.murmur3_32``)."""
    c1 = 0xCC9E2D51
    c2 = 0x1B873593
    h = seed & _INT32_MASK
    length = len(data)
    nblocks = length // 4
    for i in range(nblocks):
        k = int.from_bytes(data[4 * i : 4 * i + 4], "little")
        k = (k * c1) & _INT32_MASK
        k = ((k << 15) | (k >> 17)) & _INT32_MASK
        k = (k * c2) & _INT32_MASK
        h ^= k
        h = ((h << 13) | (h >> 19)) & _INT32_MASK
        h = (h * 5 + 0xE6546B64) & _INT32_MASK
    tail = data[nblocks * 4 :]
    k = 0
    if len(tail) >= 3:
        k ^= tail[2] << 16
    if len(tail) >= 2:
        k ^= tail[1] << 8
    if len(tail) >= 1:
        k ^= tail[0]
        k = (k * c1) & _INT32_MASK
        k = ((k << 15) | (k >> 17)) & _INT32_MASK
        k = (k * c2) & _INT32_MASK
        h ^= k
    h ^= length
    h ^= h >> 16
    h = (h * 0x85EBCA6B) & _INT32_MASK
    h ^= h >> 13
    h = (h * 0xC2B2AE35) & _INT32_MASK
    h ^= h >> 16
    return h - (1 << 32) if h & 0x80000000 else h


def _days_since_epoch(value: Any) -> int:
    if isinstance(value, datetime):
        raise TypeError("expected a date, got a datetime")
    if isinstance(value, date):
        return (value - _EPOCH_DATE).days
    return int(value)


def _time_micros(value: Any) -> int:
    if isinstance(value, time):
        return (
            value.hour * 3600 + value.minute * 60 + value.second
        ) * 1_000_000 + value.microsecond
    return int(value)


def bucket_encode(
    col_type: str, value: Any, type_params: dict[str, Any] | None = None
) -> bytes:
    """Iceberg Appendix-B hash input encoding for ``value``.

    Diverges from the bounds codec (:mod:`pyhoglake.bounds`) where the
    spec says so: every bucketable integer width and ``date`` hash as
    8-byte LE **longs** (``hashInt(v) = hashLong(long(v))``), not the
    4-byte forms the bounds codec stores for ``int``/``int8``/``int16``/
    ``uint8``/``uint16``/``date``. ``decimal``/``uuid``/``string``/
    ``binary`` reuse the bounds encodings, which already match the spec.

    The types _BUCKETABLE withholds fall through to the ValidationError:
    an encoding nobody has verified cross-language is worse than a
    refusal, because its partition values look fine and prune wrong.
    """
    if col_type in ("int", "long", "int8", "int16", "uint8", "uint16"):
        return struct.pack("<q", int(value))
    if col_type == "date":
        return struct.pack("<q", _days_since_epoch(value))
    if col_type == "time":
        return struct.pack("<q", _time_micros(value))
    if col_type in ("timestamp", "timestamptz"):
        return struct.pack("<q", _micros_since_epoch(value))
    if col_type == "string":
        if isinstance(value, bytes):
            return value
        return str(value).encode("utf-8")
    if col_type == "uuid":
        if isinstance(value, _uuid.UUID):
            return value.bytes
        if isinstance(value, str):
            return _uuid.UUID(value).bytes
        return bytes(value)
    if col_type == "binary":
        return bytes(value)
    if col_type == "decimal":
        scale = int((type_params or {}).get("scale", 0))
        return _minimal_twos_complement(_unscaled(value, scale))
    raise ValidationError(
        f"bucket cannot be applied to column type {col_type!r} "
        f"(Iceberg spec: bucketable types are {sorted(_BUCKETABLE)})"
    )


def bucket_hash(
    col_type: str, value: Any, type_params: dict[str, Any] | None = None
) -> int:
    """The signed 32-bit Iceberg hash of ``value`` (spec Appendix B)."""
    return murmur3_32(bucket_encode(col_type, value, type_params))


def bucket(
    col_type: str,
    value: Any,
    n: int,
    type_params: dict[str, Any] | None = None,
) -> int | None:
    """``bucket[n]``: ``(hash & Integer.MAX_VALUE) % n``; null -> null."""
    if not isinstance(n, int) or n <= 0:
        raise ValidationError(f"bucket requires a positive bucket count, got {n!r}")
    if value is None:
        return None
    return (bucket_hash(col_type, value, type_params) & _INT31_MASK) % n


def truncate(
    col_type: str,
    value: Any,
    width: int,
    type_params: dict[str, Any] | None = None,
) -> Any:
    """``truncate[width]`` with Iceberg semantics; null -> null."""
    if not isinstance(width, int) or width <= 0:
        raise ValidationError(f"truncate requires a positive width, got {width!r}")
    if value is None:
        return None
    if col_type in _INT_TYPES:
        v = int(value)
        return v - (v % width)  # Python % is floor-mod: negative-correct
    if col_type == "string":
        return str(value)[:width]  # codepoints, not bytes
    if col_type == "binary":
        return bytes(value)[:width]
    if col_type == "decimal":
        scale = int((type_params or {}).get("scale", 0))
        unscaled = _unscaled(value, scale)
        floored = unscaled - (unscaled % width)
        with localcontext() as ctx:
            ctx.prec = 60
            return Decimal(floored).scaleb(-scale)
    raise ValidationError(
        f"truncate cannot be applied to column type {col_type!r} "
        f"(truncatable types are {sorted(_TRUNCATABLE)})"
    )


def year(value: date | datetime | None) -> int | None:
    """Years since 1970 (calendar year - 1970); null -> null."""
    if value is None:
        return None
    return value.year - 1970


def month(value: date | datetime | None) -> int | None:
    """Months since 1970-01 (floored, negative before the epoch)."""
    if value is None:
        return None
    return (value.year - 1970) * 12 + (value.month - 1)


def day(value: date | datetime | None) -> int | None:
    """Days since the epoch (floored for timestamps)."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return _micros_since_epoch(value) // _MICROS_PER_DAY
    return (value - _EPOCH_DATE).days


def hour(value: datetime | None) -> int | None:
    """Hours since the epoch (floored; timestamps only)."""
    if value is None:
        return None
    return _micros_since_epoch(value) // _MICROS_PER_HOUR


def _check_temporal(transform: str, col_type: str) -> None:
    allowed = _HOUR_TYPES if transform == "hour" else _YEAR_MONTH_DAY_TYPES
    if col_type not in allowed:
        raise ValidationError(
            f"{transform} transform cannot be applied to column type "
            f"{col_type!r} (allowed: {sorted(allowed)})"
        )


def transform_value(
    transform: str,
    param: int | None,
    col_type: str,
    value: Any,
    type_params: dict[str, Any] | None = None,
) -> Any:
    """Apply one partition transform to one Python-native value."""
    if transform == "identity":
        return value
    if transform == "bucket":
        if param is None:
            raise ValidationError("bucket transform requires transform_param (N)")
        return bucket(col_type, value, param, type_params)
    if transform == "truncate":
        if param is None:
            raise ValidationError("truncate transform requires transform_param (W)")
        return truncate(col_type, value, param, type_params)
    if transform in ("year", "month", "day", "hour"):
        _check_temporal(transform, col_type)
        fn = {"year": year, "month": month, "day": day, "hour": hour}[transform]
        return fn(value)
    raise ValidationError(f"unknown partition transform {transform!r}")


def wire_string(col_type: str, transform: str, transformed: Any) -> str | None:
    """Canonical partition-value string for the commit wire (opaque to the
    server, which groups by string equality — see FileRegistration in the
    openapi spec). Null stays null (JSON null on the wire)."""
    v = transformed
    if v is None:
        return None
    if transform in ("bucket", "year", "month", "day", "hour"):
        return str(int(v))
    # identity / truncate: stringify by the SOURCE column type
    if col_type == "boolean":
        return "true" if v else "false"
    if col_type in _INT_TYPES:
        return str(int(v))
    if col_type in ("float", "double"):
        return repr(float(v))
    if col_type == "string":
        return str(v)
    # json stringifies verbatim; identity is its only transform, so this
    # is the document text exactly as it arrived.
    if col_type == "json":
        return str(v)
    if col_type in (
        "date",
        "time",
        "timestamp",
        "timestamp_s",
        "timestamp_ms",
        "timestamptz",
    ):
        return v.isoformat()
    if col_type == "timestamp_ns":
        # Nanos, never isoformat. A datetime cannot hold nanoseconds, so
        # the array driver must feed this branch raw int64 nanos (arrow
        # refuses to render a sub-microsecond timestamp[ns] as a
        # datetime at all). Scalar callers passing a datetime convert
        # here too, or the two paths would put one value in two
        # partitions.
        return str(_nanos_since_epoch(v))
    if col_type == "decimal":
        return str(v)
    if col_type == "uuid":
        if isinstance(v, _uuid.UUID):
            return str(v)
        return str(_uuid.UUID(bytes=bytes(v)))
    if col_type == "binary":
        return base64.b64encode(bytes(v)).decode("ascii")
    raise ValidationError(f"cannot stringify partition value for type {col_type!r}")


# -- nested partition sources ------------------------------------------------

#: Container column types. A partition or sort source is always a LEAF —
#: a container has no single value per row to transform.
_NESTED_TYPES = frozenset({"list", "struct", "map"})


def partition_source_array(data: pa.Table, chain: list[Any]) -> pa.ChunkedArray:
    """The source array for a partition field, given the root-to-leaf
    ``chain`` of catalog Columns that reaches it.

    A top-level column is ``data.column(name)``; a STRUCT leaf is
    extracted with ``struct_field``, which Iceberg allows as a partition
    source (``source-id`` may point at a struct's leaf field).

    Refused, by name: a container itself (no single value per row), and
    anything under a list or a map (MANY values per row — a partition key
    would have to pick one, and there is no rule that says which). The
    server refuses the same specs at DDL time; this is the client-side
    twin, so a hand-built spec fails before it writes a file.
    """
    leaf = chain[-1]
    path = ".".join(c.name for c in chain)
    if leaf.type in _NESTED_TYPES:
        raise ValidationError(
            f"partition source {path!r} is a {leaf.type!r}: a nested container has "
            "no single value per row and cannot be a partition source; use one of "
            "its leaf fields",
            status_code=None,
        )
    repeated = next((c for c in chain[:-1] if c.type in ("list", "map")), None)
    if repeated is not None:
        raise ValidationError(
            f"partition source {path!r} sits under {repeated.name!r}, a "
            f"{repeated.type!r}: a row has many such values, so it cannot be a "
            "partition source; struct leaves are the only nested fields that can",
            status_code=None,
        )
    column = data.column(chain[0].name)
    for step in chain[1:]:
        column = pc.struct_field(column, step.name)
    return column


# -- array-level driver (the fanout path) -----------------------------------


def _floordiv(arr: pa.Array, divisor: int) -> pa.Array:
    """Floor division on an int64 array (arrow's ``divide`` truncates
    toward zero; Iceberg's day/hour need flooring for pre-epoch values)."""
    q = pc.divide(arr, divisor)
    # |q * divisor| <= |arr| by construction, so this cannot overflow —
    # the checked kernels are used anyway so that no unchecked integer
    # arithmetic survives in the partition-value path at all.
    r = pc.subtract_checked(arr, pc.multiply_checked(q, divisor))
    adjust = pc.and_(pc.less(arr, 0), pc.not_equal(r, 0))
    return pc.subtract_checked(q, pc.cast(adjust, pa.int64()))


def _timestamp_micros(arr: pa.Array) -> pa.Array:
    """Raw int64 micros for a timestamp array of ANY unit.

    ``pc.cast(arr, int64)`` yields the array's own unit, not micros, so
    feeding it straight to the day/hour divisors below lands every
    timestamp[s] row 10^6 times too low, every timestamp[ms] row 10^3
    times too low, and every timestamp[ns] row 10^3 times too high.
    Exact integer math keyed on the unit fixes that; the nanos case
    FLOOR-divides (never arrow's
    truncate-toward-zero ``divide``) so a pre-epoch instant with a
    sub-microsecond remainder still lands in the earlier micro, which is
    what keeps day/hour flooring correct across the epoch.
    """
    raw = pc.cast(arr, pa.int64())
    unit = arr.type.unit
    # multiply_CHECKED, not multiply: arrow's unchecked kernel wraps on
    # int64 overflow, and the wrap is silent. A timestamp[s] tick of 2^62
    # is a perfectly legal value that scales to exactly 0 micros — every
    # such row would partition as the epoch, and pruning would then miss
    # the file for its real range. Raising is the only honest answer; the
    # caller cannot partition what it cannot represent.
    if unit == "s":
        return pc.multiply_checked(raw, 1_000_000)
    if unit == "ms":
        return pc.multiply_checked(raw, 1_000)
    if unit == "ns":
        return _floordiv(raw, 1_000)
    return raw  # "us": already the stored unit


def _temporal_ints(transform: str, arr: pa.Array, col_type: str) -> pa.Array:
    """Arrow-native year/month/day/hour over a date/timestamp column."""
    _check_temporal(transform, col_type)
    if transform == "year":
        return pc.subtract_checked(pc.year(arr), 1970)
    if transform == "month":
        years = pc.subtract_checked(pc.year(arr), 1970)
        return pc.add_checked(
            pc.multiply_checked(years, 12),
            pc.subtract_checked(pc.month(arr), 1),
        )
    if col_type == "date":  # day; hour is rejected for date by _check_temporal
        return pc.cast(pc.cast(arr, pa.int32()), pa.int64())
    micros = _timestamp_micros(arr)
    divisor = _MICROS_PER_DAY if transform == "day" else _MICROS_PER_HOUR
    return _floordiv(micros, divisor)


def transform_strings(
    transform: str,
    param: int | None,
    column: pa.ChunkedArray | pa.Array,
    col_type: str,
    type_params: dict[str, Any] | None = None,
) -> pa.Array:
    """Per-row wire partition-value strings for one spec field.

    Arrow-native for the temporal transforms; ``identity``/``bucket``/
    ``truncate`` go through Python once **per unique value** (dictionary
    encoding), never per row. Nulls stay null throughout.
    """
    arr = column.combine_chunks() if isinstance(column, pa.ChunkedArray) else column
    if transform in ("year", "month", "day", "hour"):
        return pc.cast(_temporal_ints(transform, arr, col_type), pa.string())
    if transform not in ("identity", "bucket", "truncate"):
        raise ValidationError(f"unknown partition transform {transform!r}")
    if col_type == "timestamp_ns" and pa.types.is_timestamp(arr.type):
        # Arrow REFUSES to render a sub-microsecond timestamp[ns] as a
        # datetime (to_pylist raises ValueError), and micro-aligned ones
        # it renders lossily. Cast to raw int64 nanos so the per-value
        # Python path below sees the codec's own carrier: encode_bound
        # and wire_string both take an int as nanos.
        arr = pc.cast(arr, pa.int64())

    def one(value: Any) -> str | None:
        return wire_string(
            col_type,
            transform,
            transform_value(transform, param, col_type, value, type_params),
        )

    try:
        encoded = arr.dictionary_encode()
    except (pa.ArrowNotImplementedError, pa.ArrowInvalid):
        # types without dictionary support: per-value python fallback
        return pa.array([one(v) for v in arr.to_pylist()], pa.string())
    uniques = [one(v) for v in encoded.dictionary.to_pylist()]
    if not uniques:
        return pa.array([None] * len(arr), pa.string())
    # null indices (null source rows) take() to null outputs
    return pc.take(pa.array(uniques, pa.string()), encoded.indices)
