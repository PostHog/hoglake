"""Per-column stats extraction from an in-memory parquet footer.

Operates on the ``pyarrow.parquet.FileMetaData`` the writer already holds
(never re-reads the file from object storage): value/null counts summed
across row groups; min/max only when every row group carries min/max
statistics for the column; bounds encoded to Iceberg single-value binary
by the catalog column type.
"""

from __future__ import annotations

import struct

import pyarrow as pa
import pyarrow.parquet as pq

from .bounds import encode_bound
from .models import Column, ColumnStats

# Column types whose bounds are IEEE floats: min/max over row-group
# bounds must use total-order semantics (see _float_total_order_key).
_FLOAT_TYPES = frozenset({"float", "double"})

# Column types read from Statistics.min_raw/max_raw instead of min/max.
#
# timestamp_ns is the only one, and it is not an optimization: pyarrow
# renders a timestamp[ns] statistic as a datetime, which tops out at
# microsecond resolution, so `st.min` RAISES ValueError for any value
# that is not a whole microsecond ("not safely convertible to
# microseconds") and quietly drops the sub-micro digits of the ones it
# does render. min_raw/max_raw hand back the stored int64.
#
# Every other new type needs no special case: pyarrow reports int8/
# int16/uint8/uint16/uint32/uint64 statistics as plain Python ints
# (uint64 non-negative, up to 2^64-1, NOT sign-wrapped), timestamp_s and
# timestamp_ms as datetimes that encode_bound converts to micros, and
# json as bytes, which the string branch passes through verbatim.
_RAW_STAT_TYPES = frozenset({"timestamp_ns"})

# THE TRAP those raw ints come with: min_raw is the stored int64 in the
# FILE's unit, not necessarily nanos. pyhoglake's own writer emits
# Timestamp(NANOS) for a timestamp_ns column (types.coltype_to_arrow,
# and pyarrow 25 keeps ns at parquet version 2.6 with no
# coerce_timestamps), so today every footer this function sees is
# already nanos — but the stats path is also handed footers written by
# other clients, and a MILLIS-annotated one would be off by 10^6 with
# no symptom other than wrong pruning. So the unit is read from the
# footer and scaled, exactly as the Kotlin hydrator's
# decodeTimestampNanos does; scaling up is always exact.
_NANOS_PER_UNIT = {"s": 1_000_000_000, "ms": 1_000_000, "us": 1_000, "ns": 1}


#: Everything the bound-reading path may raise on a footer it cannot
#: honestly turn into a bound. pyarrow raises ValueError for an
#: unrenderable temporal statistic, struct raises struct.error when a
#: value will not fit its format, a type mismatch surfaces as TypeError,
#: and the decimal path raises InvalidOperation (an ArithmeticError, NOT
#: a ValueError) when Decimal(str(v)) cannot parse a non-numeric
#: statistic. All of them mean the same thing here: no bound.
_BOUND_ERRORS = (ValueError, OverflowError, TypeError, ArithmeticError, struct.error)

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


def _int64_or_raise(v: int) -> int:
    """``v`` if it fits a signed 64-bit bound, else raise.

    Python ints are unbounded, so an out-of-range scaled timestamp would
    otherwise sail on and blow up inside ``struct.pack`` much later, where
    the cause is no longer obvious.
    """
    if not _INT64_MIN <= v <= _INT64_MAX:
        raise OverflowError(f"bound {v} does not fit int64")
    return v


#: Widest parquet INT(w, unsigned) leaf each catalog type can read —
#: the mirror of Kotlin's ColType.maxUnsignedParquetWidth. -1 means
#: none: either the type is not integral, or it cannot hold even an
#: 8-bit magnitude (int8 tops out at 127).
_MAX_UNSIGNED_WIDTH = {
    "uint64": 64,
    "uint32": 32,
    "long": 32,
    "time": 32,
    "timestamp_s": 32,
    "timestamp_ms": 32,
    "timestamp": 32,
    "timestamp_ns": 32,
    "timestamptz": 32,
    "uint16": 16,
    "int": 16,
    "date": 16,
    "uint8": 8,
    "int16": 8,
}

#: Catalog types that read a parquet INT32 leaf (all map to Iceberg int).
_INT32_READERS = frozenset({"int8", "int16", "uint8", "uint16", "int", "date"})

#: Catalog types whose bound comes from a BYTE_ARRAY leaf.
_BYTE_ARRAY_READERS = frozenset({"string", "json", "binary"})


def _unsigned_width(t: pa.DataType) -> int | None:
    """The leaf's unsigned INT annotation width, or None if signed."""
    for width, pred in (
        (8, pa.types.is_uint8),
        (16, pa.types.is_uint16),
        (32, pa.types.is_uint32),
        (64, pa.types.is_uint64),
    ):
        if pred(t):
            return width
    return None


def _physical(t: pa.DataType) -> str | None:
    """The parquet physical type an arrow type is written as.

    Classified explicitly rather than inferred from arrow's own
    categories, because the two do not line up where it matters:
    ``date32`` and ``time32`` are not "integer" arrow types but ARE
    parquet INT32, and getting that wrong drops bounds on ordinary date
    columns.
    """
    if pa.types.is_boolean(t):
        return "BOOLEAN"
    if pa.types.is_integer(t):
        return "INT32" if t.bit_width <= 32 else "INT64"
    if pa.types.is_float32(t):
        return "FLOAT"
    if pa.types.is_float64(t):
        return "DOUBLE"
    if pa.types.is_date32(t) or pa.types.is_time32(t):
        return "INT32"
    if pa.types.is_date64(t) or pa.types.is_time64(t) or pa.types.is_timestamp(t):
        return "INT64"
    if pa.types.is_decimal(t) or pa.types.is_fixed_size_binary(t):
        return "FIXED_LEN_BYTE_ARRAY"
    if (
        pa.types.is_string(t)
        or pa.types.is_large_string(t)
        or pa.types.is_binary(t)
        or pa.types.is_large_binary(t)
    ):
        return "BYTE_ARRAY"
    return None


def _storage(t: pa.DataType) -> pa.DataType:
    """An extension type's storage type; anything else unchanged.

    pa.json_() and pa.uuid() are extension types, and every
    ``pa.types.is_*`` predicate says no to them — so a json column's own
    footer would have looked foreign to the check below.

    Tested against BaseExtensionType, not ExtensionType: the canonical
    arrow extension types are C++-defined and subclass only the former.
    """
    return t.storage_type if isinstance(t, pa.BaseExtensionType) else t


def _bound_readable(col: Column, raw_type: pa.DataType) -> bool:
    """Whether a footer leaf of arrow type ``raw_type`` can honestly
    produce a bound for ``col``.

    This mirrors the per-arm physical/logical matching in the Kotlin
    hydrator's FooterStats.decode, arm for arm, because the two MUST
    agree on which cells produce: a bound the writer emits that the
    hydrator would have refused (or vice versa) means the same file
    prunes differently depending on which side wrote its stats.

    Without it this module simply fed whatever pyarrow handed back into
    encode_bound, so a boolean footer under a `long` column
    truthiness-coerced False/True into bounds of 0/1 — a perfectly
    well-formed bound describing data that does not exist.
    """
    t = _storage(raw_type)
    physical = _physical(t)
    if physical is None:
        return False

    width = _unsigned_width(t)
    if width is not None and width > _MAX_UNSIGNED_WIDTH.get(col.type, -1):
        # The unsigned-domain rule: an unsigned leaf of width w is
        # readable only by a type whose own domain contains [0, 2^w).
        return False

    kind = col.type
    if kind == "boolean":
        return physical == "BOOLEAN"
    if kind in _INT32_READERS:
        # All map to Iceberg int, and Kotlin accepts any INT32 leaf.
        return physical == "INT32"
    if kind == "uint32":
        # INT64, or the INT32 + INT(32, unsigned) form arrow writes.
        return physical == "INT64" or pa.types.is_uint32(t)
    if kind == "uint64":
        # The unsigned annotation is mandatory: an unannotated INT64's
        # footer bounds were ordered SIGNED and are not uint64 bounds.
        return pa.types.is_uint64(t)
    if kind == "long":
        return physical in ("INT32", "INT64")
    if kind == "float":
        return physical == "FLOAT"
    if kind == "double":
        return physical in ("DOUBLE", "FLOAT")
    if kind == "time":
        # INT64 + TIME(MICROS) or INT32 + TIME(MILLIS). Nanos is refused
        # on the Kotlin side too: a sub-micro time bound would truncate.
        return (pa.types.is_time64(t) and t.unit == "us") or (
            pa.types.is_time32(t) and t.unit == "ms"
        )
    if kind in (
        "timestamp_s",
        "timestamp_ms",
        "timestamp",
        "timestamptz",
        "timestamp_ns",
    ):
        # Unit-driven, not type-driven: the file's annotation says what
        # its int64s mean, and the scaling happens per unit.
        return pa.types.is_timestamp(t)
    if kind in ("string", "json"):
        return physical == "BYTE_ARRAY"
    if kind == "binary":
        # binary also reads FIXED_LEN_BYTE_ARRAY (a uuid-shaped file
        # under a binary column still bounds), but not a DECIMAL one:
        # those bytes are an unscaled number, not opaque content.
        return physical == "BYTE_ARRAY" or pa.types.is_fixed_size_binary(t)
    if kind == "uuid":
        return pa.types.is_fixed_size_binary(t) and t.byte_width == 16
    if kind == "decimal":
        if not pa.types.is_decimal(t):
            return False
        # Kotlin drops bounds on a scale mismatch rather than rescaling;
        # the stored unscaled value means nothing without its scale.
        declared = (col.type_params or {}).get("scale")
        return declared is None or int(declared) == t.scale
    return False


def _nanos_scale(metadata: pq.FileMetaData, name: str) -> int | None:
    """Nanos-per-tick for the footer field ``name``, or None when it is
    not a timestamp we can read.

    The unit comes from the footer's own arrow schema rather than the
    catalog type: the catalog says what the column was DECLARED as, the
    file says what its int64s MEAN, and only the second one can be
    trusted to interpret raw statistics.
    """
    try:
        field = metadata.schema.to_arrow_schema().field(name)
    except (KeyError, pa.ArrowInvalid, pa.ArrowNotImplementedError):
        return None
    if not pa.types.is_timestamp(field.type):
        return None
    return _NANOS_PER_UNIT.get(field.type.unit)


def _float_total_order_key(v: float) -> int:
    """IEEE-754 total-order sort key: the semantics of Kotlin/Java's
    ``Double.compare``, under which ``-0.0 < 0.0``.

    Python's ``min``/``max`` treat ``-0.0 == 0.0`` and keep the FIRST of
    equal values, so the winning zero's sign — and therefore the encoded
    bound bytes — would depend on row-group order (bugs.md #20). Mapping
    the float's bit pattern (sign-magnitude) to a monotone integer makes
    the choice deterministic: a min bound prefers -0.0 over +0.0, a max
    bound prefers +0.0 over -0.0, regardless of order.
    """
    (bits,) = struct.unpack("<q", struct.pack("<d", v))
    return bits if bits >= 0 else -(bits & 0x7FFFFFFFFFFFFFFF) - 1


def extract_column_stats(
    metadata: pq.FileMetaData, columns: list[Column] | tuple[Column, ...]
) -> list[ColumnStats]:
    by_name = {c.name: c for c in columns}

    # Map parquet leaf index -> top-level column name (flat schemas only;
    # nested types are not in the supported type set).
    n_cols = metadata.num_columns
    leaf_names: list[str] = []
    if metadata.num_row_groups > 0:
        rg0 = metadata.row_group(0)
        for j in range(n_cols):
            # Full path, verbatim: for the flat schemas we support the leaf
            # path IS the column name — splitting on "." misattributed a
            # top-level column literally named "a.b" (QE find, 2026-09-05).
            leaf_names.append(rg0.column(j).path_in_schema)

    # The footer's own arrow schema, resolved once: it is what says what
    # each leaf physically IS, which the catalog type cannot.
    try:
        footer_schema: pa.Schema | None = metadata.schema.to_arrow_schema()
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
        footer_schema = None

    if any(c.type == "variant" for c in columns):
        if footer_schema is None:
            return []

        def leaf_count(type_: pa.DataType) -> int:
            return (
                sum(leaf_count(type_.field(i).type) for i in range(type_.num_fields))
                if type_.num_fields
                else 1
            )

        # A nested VARIANT leaf can have the same dotted path as a different
        # top-level scalar (e.g. properties.value). Bind physical leaf positions,
        # never dotted path spelling, when the file contains nested columns.
        leaf_names = []
        for field in footer_schema:
            leaf_names.extend(
                [field.name if field.type.num_fields == 0 else ""]
                * leaf_count(field.type)
            )

    out: list[ColumnStats] = []
    for j, name in enumerate(leaf_names):
        col = by_name.get(name)
        if col is None or col.type == "variant":
            continue  # file column not in the catalog schema; nothing to report

        value_count = 0
        null_count = 0
        size_bytes = 0
        have_null_counts = True
        have_min_max = True
        mins: list = []
        maxs: list = []
        # Raw int64 stats are only interpretable once their unit is
        # known. A footer field that is not a timestamp, or whose unit we
        # cannot read, leaves the bounds NULL rather than guessing nanos
        # — and reading st.min instead is not an option, since that is
        # the call that raises on sub-microsecond values.
        raw_scale = (
            _nanos_scale(metadata, name) if col.type in _RAW_STAT_TYPES else None
        )
        if col.type in _RAW_STAT_TYPES and raw_scale is None:
            have_min_max = False

        # Does the footer leaf's physical shape actually correspond to
        # this catalog type? The Kotlin hydrator asks the same question
        # arm by arm, and the two must answer identically — otherwise the
        # same file gets bounds or not depending on which side wrote its
        # stats. An unreadable shape is an ABSENT bound, never a coerced
        # one: pyarrow will happily hand back False for a boolean footer
        # and int() will happily turn it into 0.
        try:
            leaf_type = (
                None if footer_schema is None else footer_schema.field(name).type
            )
        except KeyError:
            # A nested leaf's dotted path is not a top-level field.
            leaf_type = None
        if leaf_type is None or not _bound_readable(col, leaf_type):
            have_min_max = False

        for r in range(metadata.num_row_groups):
            rg = metadata.row_group(r)
            chunk = rg.column(j)
            value_count += rg.num_rows
            size_bytes += chunk.total_compressed_size
            st = chunk.statistics
            if st is None or not st.has_null_count:
                have_null_counts = False
                have_min_max = False
                continue
            null_count += st.null_count
            if not have_min_max:
                continue  # bounds already written off; never touch st.min
            if st.has_min_max and rg.num_rows > st.null_count:
                try:
                    if raw_scale is not None:
                        # Scaling up to nanos is exact for every unit, but
                        # not necessarily REPRESENTABLE: a MICROS footer
                        # holding a year-9999 sentinel is ~2.5e20 nanos,
                        # well past int64.
                        lo_v = _int64_or_raise(st.min_raw * raw_scale)
                        hi_v = _int64_or_raise(st.max_raw * raw_scale)
                        mins.append(lo_v)
                        maxs.append(hi_v)
                    else:
                        # st.min itself raises for a NANOS footer (pyarrow
                        # will not render sub-microsecond instants as
                        # datetimes), which a catalog `timestamp` column
                        # over a foreign ns file reaches.
                        mins.append(st.min)
                        maxs.append(st.max)
                except _BOUND_ERRORS:
                    # A bound we cannot read or represent is an ABSENT
                    # bound, never a failed commit. The Kotlin hydrator
                    # holds the same contract ("overflow must degrade to a
                    # null bound, never escape and fail the file") and the
                    # writer path must not be the stricter of the two: a
                    # single unreadable statistic would otherwise abort an
                    # append whose data is perfectly fine.
                    have_min_max = False
            elif rg.num_rows > st.null_count:
                have_min_max = False
            # an all-null row group legitimately has no min/max; skip it

        if not have_null_counts:
            # Cannot honestly report this column; omit it entirely rather
            # than ship a fabricated null_count.
            continue

        lower = upper = None
        if have_min_max and mins:
            # The SELECTION is inside the guard too, not just the encode:
            # _float_total_order_key packs its argument as a double, so a
            # non-float statistic under a catalog float/double column
            # (a foreign timestamp or string footer) raised before
            # encode_bound was ever reached.
            try:
                if col.type in _FLOAT_TYPES:
                    lo = min(mins, key=_float_total_order_key)
                    hi = max(maxs, key=_float_total_order_key)
                else:
                    lo = min(mins)
                    hi = max(maxs)
                lower = encode_bound(col.type, lo, col.type_params)
                upper = encode_bound(col.type, hi, col.type_params)
            except _BOUND_ERRORS:
                # The footer's value does not fit — or does not mean —
                # what the catalog type needs: a foreign INT64 statistic
                # under a catalog `int`, a string under a `decimal`. Same
                # rule throughout: absent, not fatal.
                lower = upper = None

        out.append(
            ColumnStats(
                field_id=col.field_id,
                value_count=value_count,
                null_count=null_count,
                size_bytes=size_bytes,
                lower_bound=lower,
                upper_bound=upper,
            )
        )
    return out
