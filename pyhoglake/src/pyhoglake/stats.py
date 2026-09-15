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

    out: list[ColumnStats] = []
    for j, name in enumerate(leaf_names):
        col = by_name.get(name)
        if col is None:
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
