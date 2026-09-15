"""Partition-transform tests: Iceberg-spec pinned vectors + properties.

The hardcoded vectors come from the Apache Iceberg Table Spec:

- Appendix B "32-bit Hash Requirements" (the bucket hash test table:
  https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements)
- "Partition Transforms" (year/month/day/hour epoch-relative examples
  and truncate semantics:
  https://iceberg.apache.org/spec/#partition-transforms)

If any of these ever fails, the implementation is wrong — the vectors
are normative, shared by every Iceberg implementation.
"""

import uuid
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal

import pyarrow as pa
import pytest
from hypothesis import given
from hypothesis import strategies as st

from pyhoglake import ValidationError, transforms
from pyhoglake.transforms import (
    bucket,
    bucket_hash,
    day,
    hour,
    month,
    murmur3_32,
    transform_strings,
    transform_value,
    truncate,
    wire_string,
    year,
)

# ---------------------------------------------------------------------------
# murmur3_32 algorithm sanity (independent of Iceberg: reference values of
# the canonical x86 32-bit Murmur3, seed 0)
# ---------------------------------------------------------------------------


def test_murmur3_reference_values():
    assert murmur3_32(b"") == 0
    # canonical murmur3_x86_32("hello", seed=0) = 0x248bfa47
    assert murmur3_32(b"hello") & 0xFFFFFFFF == 0x248BFA47


# ---------------------------------------------------------------------------
# Iceberg spec Appendix B hash vectors (bucket transform hash column)
# ---------------------------------------------------------------------------

_TSTZ = datetime.fromisoformat("2017-11-16T14:31:08-08:00")

APPENDIX_B_VECTORS = [
    # (col_type, value, type_params, expected signed 32-bit hash)
    ("int", 34, None, 2017239379),
    ("long", 34, None, 2017239379),  # int and long 34 hash identically
    ("decimal", Decimal("14.20"), {"precision": 4, "scale": 2}, -500754589),
    ("date", date(2017, 11, 16), None, -653330422),
    ("time", time(22, 31, 8), None, -662762989),
    ("timestamp", datetime(2017, 11, 16, 22, 31, 8), None, -2047944441),
    ("timestamptz", _TSTZ, None, -2047944441),
    ("string", "iceberg", None, 1210000089),
    ("uuid", uuid.UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7"), None, 1488055340),
    ("binary", b"\x00\x01\x02\x03", None, -188683207),  # fixed and binary alike
]


@pytest.mark.parametrize(
    ("col_type", "value", "type_params", "expected"), APPENDIX_B_VECTORS
)
def test_bucket_hash_spec_vectors(col_type, value, type_params, expected):
    assert bucket_hash(col_type, value, type_params) == expected


def test_bucket_hash_int_is_hashed_as_long():
    """Appendix B: hashInt(v) = hashLong(long(v)) — the hash input is the
    8-byte LE long form, NOT the 4-byte int the bounds codec stores."""
    assert transforms.bucket_encode("int", 34) == (34).to_bytes(8, "little")
    assert transforms.bucket_encode("date", date(2017, 11, 16)) == (17486).to_bytes(
        8, "little"
    )


def test_bucket_value_from_spec_hash():
    # bucket[N] = (hash & Integer.MAX_VALUE) % N, from the pinned hashes
    assert bucket("string", "iceberg", 16) == (1210000089 & 0x7FFFFFFF) % 16
    assert bucket("int", 34, 16) == (2017239379 & 0x7FFFFFFF) % 16
    # negative hash: Java's (h & MAX) % N, never Python's negative-mod
    assert bucket("date", date(2017, 11, 16), 16) == (-653330422 & 0x7FFFFFFF) % 16


def test_bucket_rejected_types():
    for bad in ("boolean", "float", "double", "json"):
        with pytest.raises(ValidationError, match="bucket cannot be applied"):
            bucket_hash(bad, 1)


# ---------------------------------------------------------------------------
# type gates: the exact vocabulary each transform accepts
# ---------------------------------------------------------------------------

ALL_COLTYPES = frozenset(
    {
        "boolean",
        "int8",
        "int16",
        "int",
        "long",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "float",
        "double",
        "decimal",
        "date",
        "time",
        "timestamp_s",
        "timestamp_ms",
        "timestamp",
        "timestamp_ns",
        "timestamptz",
        "string",
        "json",
        "uuid",
        "binary",
    }
)

EXPECTED_BUCKETABLE = frozenset(
    {
        "int8",
        "int16",
        "int",
        "long",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "date",
        "time",
        "timestamp_s",
        "timestamp_ms",
        "timestamp",
        "timestamp_ns",
        "timestamptz",
        "string",
        "uuid",
        "binary",
        "decimal",
    }
)
EXPECTED_TRUNCATABLE = frozenset(
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
EXPECTED_YMD = frozenset(
    {"date", "timestamp_s", "timestamp_ms", "timestamp", "timestamp_ns", "timestamptz"}
)
EXPECTED_HOUR = frozenset(
    {"timestamp_s", "timestamp_ms", "timestamp", "timestamp_ns", "timestamptz"}
)


def test_transform_type_gates_are_exactly_these_sets():
    """Pinned against the server's own gate (AlterService.BUCKETABLE_TYPES
    excludes boolean/float/double/json). Drift here means a client writes
    partition values the server would have refused."""
    assert transforms._BUCKETABLE == EXPECTED_BUCKETABLE
    assert transforms._TRUNCATABLE == EXPECTED_TRUNCATABLE
    assert transforms._YEAR_MONTH_DAY_TYPES == EXPECTED_YMD
    assert transforms._HOUR_TYPES == EXPECTED_HOUR
    # every gate is a subset of the closed vocabulary — no typos
    for gate in (EXPECTED_BUCKETABLE, EXPECTED_TRUNCATABLE, EXPECTED_YMD):
        assert gate <= ALL_COLTYPES


def test_json_takes_identity_only():
    """bucket hashes bytes and truncate slices them, but two documents
    equal as JSON (key order, whitespace, number spelling) have different
    bytes: either transform would scatter equal values across partitions
    and prune away files that do match."""
    assert "json" not in transforms._BUCKETABLE
    assert "json" not in transforms._TRUNCATABLE
    assert "json" not in transforms._YEAR_MONTH_DAY_TYPES
    with pytest.raises(ValidationError, match="bucket cannot be applied"):
        transform_value("bucket", 16, "json", "{}")
    with pytest.raises(ValidationError, match="truncate cannot be applied"):
        transform_value("truncate", 4, "json", "{}")
    with pytest.raises(ValidationError, match="day transform cannot be applied"):
        transform_value("day", None, "json", "{}")
    # identity passes the document through untouched
    assert transform_value("identity", None, "json", '{ "b":1 }') == '{ "b":1 }'
    assert wire_string("json", "identity", '{ "b":1 }') == '{ "b":1 }'


@pytest.mark.parametrize(
    "col_type", ["int8", "int16", "uint8", "uint16", "uint32", "uint64"]
)
def test_new_int_widths_hash_as_8_byte_longs(col_type):
    """Appendix B: hashInt(v) = hashLong(long(v)). Every integer width
    hashes through the same 8-byte LE long, so 34 buckets identically
    whatever width declared it."""
    assert transforms.bucket_encode(col_type, 34) == (34).to_bytes(8, "little")
    assert bucket_hash(col_type, 34) == bucket_hash("int", 34)


def test_uint64_above_2_pow_63_hashes_without_overflow():
    """struct.pack("<q") cannot hold [2^63, 2^64); the encoder takes the
    two's-complement 64 bits instead, which is the same byte string for
    everything that does fit."""
    assert transforms.bucket_encode("uint64", 2**64 - 1) == b"\xff" * 8
    assert transforms.bucket_encode("uint64", 34) == transforms.bucket_encode(
        "long", 34
    )
    assert 0 <= bucket("uint64", 2**64 - 1, 16) < 16


@pytest.mark.parametrize("col_type", ["timestamp_s", "timestamp_ms"])
def test_seconds_and_millis_timestamps_hash_as_micros(col_type):
    """One stored unit per mapped Iceberg type, or equal instants written
    at different declared precisions land in different buckets."""
    ts = datetime(2017, 11, 16, 22, 31, 8)
    assert bucket_hash(col_type, ts) == bucket_hash("timestamp", ts)
    assert bucket_hash(col_type, ts) == -2047944441  # the Appendix B vector


def test_nanos_timestamps_hash_as_nanos():
    nanos = 1788609600123456789
    assert transforms.bucket_encode("timestamp_ns", nanos) == nanos.to_bytes(
        8, "little"
    )
    # a datetime input scales exactly (it carries whole micros only)
    ts = datetime(2017, 11, 16, 22, 31, 8)
    seconds = 17486 * 86400 + 22 * 3600 + 31 * 60 + 8  # the spec's day 17486
    assert transforms.bucket_encode("timestamp_ns", ts) == (seconds * 10**9).to_bytes(
        8, "little"
    )


@pytest.mark.parametrize(
    "col_type", ["int8", "int16", "uint8", "uint16", "uint32", "uint64"]
)
def test_new_int_widths_truncate_like_int(col_type):
    assert truncate(col_type, 15, 10) == 10
    assert truncate(col_type, 10, 10) == 10


def test_signed_narrow_width_truncate_floors_negatives():
    assert truncate("int8", -1, 10) == -10  # floors, never toward zero
    assert truncate("int16", -11, 10) == -20


def test_bucket_bad_count():
    with pytest.raises(ValidationError, match="positive bucket count"):
        bucket("int", 1, 0)


# ---------------------------------------------------------------------------
# temporal transforms: spec "Partition Transforms" examples
# ---------------------------------------------------------------------------


def test_temporal_spec_examples():
    d = date(2017, 11, 16)
    assert year(d) == 47
    assert month(d) == 574  # (2017-1970)*12 + (11-1)
    assert day(d) == 17486
    ts = datetime(2017, 11, 16, 22, 31, 8)
    assert year(ts) == 47
    assert month(ts) == 574
    assert day(ts) == 17486
    assert hour(ts) == 17486 * 24 + 22


def test_temporal_pre_epoch_is_floored_negative():
    # the spec's pre-epoch examples: everything about 1969-12-31 is -1
    d = date(1969, 12, 31)
    assert year(d) == -1
    assert month(d) == -1
    assert day(d) == -1
    ts = datetime(1969, 12, 31, 23, 59, 58)
    assert day(ts) == -1
    assert hour(ts) == -1


def test_temporal_nulls():
    assert year(None) is None
    assert month(None) is None
    assert day(None) is None
    assert hour(None) is None


_EPOCH = datetime(1970, 1, 1)


@given(
    st.dates(min_value=date(1, 1, 1), max_value=date(9999, 12, 31)),
)
def test_date_transforms_vs_datetime_math(d):
    assert year(d) == d.year - 1970
    assert month(d) == (d.year - 1970) * 12 + (d.month - 1)
    assert day(d) == (d - date(1970, 1, 1)).days


@given(
    st.datetimes(min_value=datetime(1, 1, 1), max_value=datetime(9999, 12, 31)),
)
def test_timestamp_transforms_vs_datetime_math(ts):
    # timedelta floor-division floors toward -inf: the spec's semantics
    assert day(ts) == (ts - _EPOCH) // timedelta(days=1)
    assert hour(ts) == (ts - _EPOCH) // timedelta(hours=1)
    assert month(ts) == (ts.year - 1970) * 12 + (ts.month - 1)


def test_timestamptz_uses_utc_instant():
    # -08:00 wall time 14:31 is 22:31 UTC: same hour as the naive example
    assert hour(_TSTZ) == hour(datetime(2017, 11, 16, 22, 31, 8))
    assert day(_TSTZ) == 17486


# ---------------------------------------------------------------------------
# truncate: spec examples + properties
# ---------------------------------------------------------------------------


def test_truncate_spec_examples():
    assert truncate("int", 1, 10) == 0
    assert truncate("int", -1, 10) == -10  # floors, never rounds toward zero
    assert truncate("long", -1, 10) == -10
    assert truncate("string", "iceberg", 3) == "ice"
    assert truncate(
        "decimal", Decimal("10.65"), 50, {"precision": 4, "scale": 2}
    ) == Decimal("10.50")
    assert truncate("binary", b"\x01\x02\x03\x04", 2) == b"\x01\x02"


def test_truncate_string_counts_codepoints_not_bytes():
    assert truncate("string", "\N{PILE OF POO}abc", 2) == "\N{PILE OF POO}a"


def test_truncate_bad_width():
    with pytest.raises(ValidationError, match="positive width"):
        truncate("int", 5, 0)


def test_truncate_rejected_types():
    with pytest.raises(ValidationError, match="truncate cannot be applied"):
        truncate("date", date(2020, 1, 1), 5)


@given(st.integers(min_value=-(2**63), max_value=2**63 - 1), st.integers(1, 10**6))
def test_truncate_int_properties(v, w):
    t = truncate("long", v, w)
    assert t % w == 0
    assert t <= v < t + w
    assert truncate("long", t, w) == t  # idempotent


@given(
    st.integers(min_value=-(2**63), max_value=2**63 - 1),
    st.integers(min_value=-(2**63), max_value=2**63 - 1),
    st.integers(1, 10**6),
)
def test_truncate_int_preserves_order(a, b, w):
    if a > b:
        a, b = b, a
    assert truncate("long", a, w) <= truncate("long", b, w)


@given(st.text(max_size=40), st.text(max_size=40), st.integers(1, 20))
def test_truncate_string_properties(a, b, w):
    ta, tb = truncate("string", a, w), truncate("string", b, w)
    assert len(ta) <= w
    assert truncate("string", ta, w) == ta  # idempotent
    if a <= b:
        assert ta <= tb  # order-preserving


@given(
    st.decimals(
        min_value=Decimal(-10000),
        max_value=Decimal(10000),
        places=2,
        allow_nan=False,
        allow_infinity=False,
    ),
    st.integers(1, 1000),
)
def test_truncate_decimal_properties(v, w):
    params = {"precision": 9, "scale": 2}
    t = truncate("decimal", v, w, params)
    unscaled = int(t.scaleb(2))
    assert unscaled % w == 0
    assert t <= v
    assert (v - t) * 100 < w
    assert truncate("decimal", t, w, params) == t


# ---------------------------------------------------------------------------
# bucket properties
# ---------------------------------------------------------------------------


@given(
    st.one_of(
        st.integers(min_value=-(2**63), max_value=2**63 - 1),
        st.text(max_size=30),
        st.binary(max_size=30),
    ),
    st.integers(1, 4096),
)
def test_bucket_output_range(v, n):
    if isinstance(v, int):
        b = bucket("long", v, n)
    elif isinstance(v, str):
        b = bucket("string", v, n)
    else:
        b = bucket("binary", v, n)
    assert 0 <= b < n


def test_bucket_null_is_null():
    assert bucket("long", None, 8) is None


# ---------------------------------------------------------------------------
# transform_value dispatch + wire_string
# ---------------------------------------------------------------------------


def test_transform_value_dispatch():
    assert transform_value("identity", None, "long", 7) == 7
    assert transform_value("bucket", 16, "string", "iceberg") == bucket(
        "string", "iceberg", 16
    )
    assert transform_value("truncate", 10, "int", -1) == -10
    assert transform_value("month", None, "date", date(2017, 11, 16)) == 574
    assert transform_value("identity", None, "long", None) is None


def test_transform_value_errors():
    with pytest.raises(ValidationError, match="unknown partition transform"):
        transform_value("squiggle", None, "long", 1)
    with pytest.raises(ValidationError, match="requires transform_param"):
        transform_value("bucket", None, "long", 1)
    with pytest.raises(ValidationError, match="hour transform cannot be applied"):
        transform_value("hour", None, "date", date(2020, 1, 1))
    with pytest.raises(ValidationError, match="year transform cannot be applied"):
        transform_value("year", None, "long", 1)


def test_wire_strings():
    assert wire_string("long", "identity", 42) == "42"
    assert wire_string("long", "month", 574) == "574"
    assert wire_string("long", "bucket", 3) == "3"
    assert wire_string("string", "identity", "x") == "x"
    assert wire_string("date", "identity", date(2026, 1, 1)) == "2026-01-01"
    assert wire_string("boolean", "identity", True) == "true"
    assert wire_string("decimal", "identity", Decimal("10.50")) == "10.50"
    assert wire_string("binary", "identity", b"\x00\x01") == "AAE="
    u = uuid.uuid4()
    assert wire_string("uuid", "identity", u.bytes) == str(u)
    assert wire_string("long", "identity", None) is None


def test_wire_strings_new_types():
    for t in ("int8", "int16", "uint8", "uint16", "uint32", "uint64"):
        assert wire_string(t, "identity", 42) == "42"
    assert wire_string("uint64", "identity", 2**64 - 1) == "18446744073709551615"
    ts = datetime(2026, 1, 2, 3, 4, 5)
    assert wire_string("timestamp_s", "identity", ts) == "2026-01-02T03:04:05"
    assert wire_string("timestamp_ms", "identity", ts) == "2026-01-02T03:04:05"
    # timestamp_ns is nanos, NOT isoformat: a datetime cannot carry them,
    # and the array driver can only produce ints, so both paths must
    # agree on the integer or one value lands in two partitions
    assert wire_string("timestamp_ns", "identity", 1_000_000_001) == "1000000001"
    assert wire_string("timestamp_ns", "identity", ts) == str(
        int((ts - datetime(1970, 1, 1)).total_seconds()) * 10**9
    )
    assert wire_string("json", "identity", '{"a": 1}') == '{"a": 1}'
    for t in ("int8", "timestamp_ns", "json"):
        assert wire_string(t, "identity", None) is None


def test_transform_strings_json_identity_roundtrips_documents():
    """json arrays have no arrow dictionary encoding, so this exercises
    the per-value fallback; the document text must survive verbatim."""
    values = ["{}", '{"a": 1}', None, '{ "b" : 2 }']
    arr = pa.array(values, pa.json_()) if hasattr(pa, "json_") else None
    if arr is None:  # pragma: no cover - pyarrow < 19
        pytest.skip("pyarrow without pa.json_()")
    assert transform_strings("identity", None, arr, "json").to_pylist() == values


# ---------------------------------------------------------------------------
# array driver: arrow-native paths agree with the scalar transforms
# ---------------------------------------------------------------------------


def _scalar_strings(transform, param, values, col_type, type_params=None):
    return [
        wire_string(
            col_type,
            transform,
            transform_value(transform, param, col_type, v, type_params),
        )
        for v in values
    ]


@pytest.mark.parametrize("transform", ["year", "month", "day", "hour"])
def test_transform_strings_temporal_matches_scalars(transform):
    values = [
        datetime(2017, 11, 16, 22, 31, 8),
        datetime(1969, 12, 31, 23, 59, 58),  # pre-epoch flooring
        None,
        datetime(1970, 1, 1, 0, 0, 0),
        datetime(2026, 3, 1, 4, 5, 6),
    ]
    arr = pa.chunked_array([pa.array(values, pa.timestamp("us"))])
    got = transform_strings(transform, None, arr, "timestamp").to_pylist()
    assert got == _scalar_strings(transform, None, values, "timestamp")


@pytest.mark.parametrize("transform", ["year", "month", "day"])
def test_transform_strings_date_matches_scalars(transform):
    values = [date(2017, 11, 16), date(1969, 12, 31), None, date(1970, 1, 1)]
    arr = pa.array(values, pa.date32())
    got = transform_strings(transform, None, arr, "date").to_pylist()
    assert got == _scalar_strings(transform, None, values, "date")


@pytest.mark.parametrize("transform", ["year", "month", "day", "hour"])
@pytest.mark.parametrize(
    ("unit", "col_type"),
    [("s", "timestamp_s"), ("ms", "timestamp_ms"), ("us", "timestamp")],
)
def test_transform_strings_non_micro_units_match_scalars(transform, unit, col_type):
    """Regression: _temporal_ints used to cast the arrow array straight to
    int64 and divide by the MICROS-per-day/hour constants, which is wrong
    by 10^6 for a timestamp[s] column and 10^3 for timestamp[ms] — every
    day/hour partition landed on 1970-01-01. The conversion is now keyed
    on arr.type.unit."""
    values = [
        datetime(2017, 11, 16, 22, 31, 8),
        datetime(1969, 12, 31, 23, 59, 58),  # pre-epoch flooring
        None,
        datetime(1970, 1, 1, 0, 0, 0),
        datetime(2026, 3, 1, 4, 5, 6),
    ]
    arr = pa.array(values, pa.timestamp(unit))
    got = transform_strings(transform, None, arr, col_type).to_pylist()
    assert got == _scalar_strings(transform, None, values, col_type)
    # and the answers are unit-independent: same instants, same partitions
    us = pa.array(values, pa.timestamp("us"))
    assert got == transform_strings(transform, None, us, "timestamp").to_pylist()


@pytest.mark.parametrize("transform", ["day", "hour"])
def test_transform_strings_nanos_floor_pre_epoch(transform):
    """Nanos convert to micros by FLOOR division, not arrow's
    truncate-toward-zero divide: a pre-epoch instant with a sub-micro
    remainder must stay in the earlier micro, or day/hour round the wrong
    way across the epoch."""
    nanos = [-1, -1000, 0, 1000, 86_400_000_000_001]
    arr = pa.array(nanos, pa.timestamp("ns"))
    got = transform_strings(transform, None, arr, "timestamp_ns").to_pylist()
    per_micro_day = 86_400_000_000
    per_micro_hour = 3_600_000_000
    divisor = per_micro_day if transform == "day" else per_micro_hour
    expected = [str((n // 1000) // divisor) for n in nanos]
    assert got == expected
    assert got[0] == "-1"  # one nanosecond before the epoch is day/hour -1


def test_transform_strings_nanos_identity_and_bucket_use_raw_nanos():
    """Arrow refuses to render a sub-microsecond timestamp[ns] as a
    datetime at all (to_pylist raises), so the per-value path must see
    raw int64 nanos — which is also the codec's carrier for the type."""
    nanos = [1, 1_000_000_001, None, 1]
    arr = pa.array(nanos, pa.timestamp("ns"))
    with pytest.raises(ValueError, match="not safely convertible"):
        arr.to_pylist()  # pinned: the hazard this path routes around
    assert transform_strings("identity", None, arr, "timestamp_ns").to_pylist() == [
        "1",
        "1000000001",
        None,
        "1",
    ]
    assert transform_strings("bucket", 16, arr, "timestamp_ns").to_pylist() == (
        _scalar_strings("bucket", 16, nanos, "timestamp_ns")
    )


def test_transform_strings_timestamptz_utc():
    values = [datetime(2017, 11, 16, 22, 31, 8, tzinfo=UTC), None]
    arr = pa.array(values, pa.timestamp("us", tz="UTC"))
    got = transform_strings("hour", None, arr, "timestamptz").to_pylist()
    assert got == [str(17486 * 24 + 22), None]


def test_transform_strings_identity_bucket_truncate_per_unique():
    values = ["a", "b", None, "a", "iceberg"]
    arr = pa.array(values, pa.string())
    assert transform_strings("identity", None, arr, "string").to_pylist() == values
    assert transform_strings("bucket", 16, arr, "string").to_pylist() == (
        _scalar_strings("bucket", 16, values, "string")
    )
    assert transform_strings("truncate", 3, arr, "string").to_pylist() == [
        "a",
        "b",
        None,
        "a",
        "ice",
    ]


def test_transform_strings_decimal_fallback():
    # decimal128 has no arrow dictionary_encode: the per-value python
    # fallback must produce identical strings
    values = [Decimal("14.20"), None, Decimal("-0.01")]
    arr = pa.array(values, pa.decimal128(9, 2))
    params = {"precision": 9, "scale": 2}
    got = transform_strings("bucket", 32, arr, "decimal", params).to_pylist()
    assert got == _scalar_strings("bucket", 32, values, "decimal", params)


def test_transform_strings_all_null_column():
    arr = pa.array([None, None], pa.int64())
    assert transform_strings("bucket", 8, arr, "long").to_pylist() == [None, None]
    assert transform_strings("identity", None, arr, "long").to_pylist() == [None, None]


def test_transform_strings_unknown_transform():
    with pytest.raises(ValidationError, match="unknown partition transform"):
        transform_strings("squiggle", None, pa.array([1], pa.int64()), "long")


@given(st.lists(st.one_of(st.none(), st.integers(-(2**40), 2**40)), max_size=50))
def test_transform_strings_long_bucket_matches_scalars(values):
    arr = pa.array(values, pa.int64())
    got = transform_strings("bucket", 64, arr, "long").to_pylist()
    assert got == _scalar_strings("bucket", 64, values, "long")


@given(
    st.lists(
        st.one_of(
            st.none(),
            st.datetimes(
                min_value=datetime(1900, 1, 1), max_value=datetime(2200, 1, 1)
            ),
        ),
        max_size=50,
    )
)
def test_transform_strings_month_matches_scalars(values):
    values = [v.replace(microsecond=0) if v else None for v in values]
    arr = pa.array(values, pa.timestamp("us"))
    got = transform_strings("month", None, arr, "timestamp").to_pylist()
    assert got == _scalar_strings("month", None, values, "timestamp")
