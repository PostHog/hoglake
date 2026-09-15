"""Stats extraction from an in-memory parquet footer with known values."""

import io
import struct
from datetime import date, datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyhoglake import decode_bound, encode_bound
from pyhoglake.models import Column
from pyhoglake.stats import extract_column_stats
from pyhoglake.types import columns_to_arrow_schema

COLUMNS = (
    Column(name="id", type="long", field_id=1, ordinal=0, nullable=False),
    Column(name="name", type="string", field_id=2, ordinal=1),
    Column(name="score", type="double", field_id=3, ordinal=2),
    Column(name="day", type="date", field_id=4, ordinal=3),
    Column(name="ts", type="timestamp", field_id=5, ordinal=4),
    Column(
        name="amount",
        type="decimal",
        field_id=6,
        ordinal=5,
        type_params={"precision": 10, "scale": 2},
    ),
)


def _write(table: pa.Table, row_group_size: int) -> pq.FileMetaData:
    sink = io.BytesIO()
    pq.write_table(table, sink, row_group_size=row_group_size)
    return pq.read_metadata(io.BytesIO(sink.getvalue()))


def _make_table(n=100):
    ids = list(range(n))
    names = [f"name-{i:04d}" if i % 10 != 0 else None for i in ids]
    scores = [float(i) / 2 if i % 7 != 0 else None for i in ids]
    days = [date(2026, 1, 1 + (i % 28)) for i in ids]
    ts = [datetime(2026, 1, 1, 0, 0, i % 60) for i in ids]
    amounts = [Decimal(i).scaleb(-2) * 100 for i in ids]  # i.00 -> unscaled i*100
    schema = columns_to_arrow_schema(COLUMNS)
    return pa.table(
        {
            "id": ids,
            "name": names,
            "score": scores,
            "day": days,
            "ts": ts,
            "amount": amounts,
        },
        schema=schema,
    )


def test_stats_across_multiple_row_groups():
    n = 100
    table = _make_table(n)
    meta = _write(table, row_group_size=30)
    assert meta.num_row_groups == 4

    stats = {s.field_id: s for s in extract_column_stats(meta, COLUMNS)}
    assert set(stats) == {1, 2, 3, 4, 5, 6}

    s_id = stats[1]
    assert s_id.value_count == n
    assert s_id.null_count == 0
    assert s_id.lower_bound == struct.pack("<q", 0)
    assert s_id.upper_bound == struct.pack("<q", n - 1)
    assert s_id.size_bytes > 0

    s_name = stats[2]
    assert s_name.value_count == n
    assert s_name.null_count == 10  # every 10th
    assert s_name.lower_bound == b"name-0001"
    assert s_name.upper_bound == b"name-0099"

    s_score = stats[3]
    assert s_score.null_count == 15  # multiples of 7 in [0, 100)
    assert s_score.lower_bound == struct.pack("<d", 0.5)
    assert s_score.upper_bound == struct.pack("<d", 49.5)

    s_day = stats[4]
    assert s_day.lower_bound == encode_bound("date", date(2026, 1, 1))
    assert s_day.upper_bound == encode_bound("date", date(2026, 1, 28))

    s_ts = stats[5]
    assert s_ts.lower_bound == encode_bound("timestamp", datetime(2026, 1, 1, 0, 0, 0))
    assert s_ts.upper_bound == encode_bound("timestamp", datetime(2026, 1, 1, 0, 0, 59))

    s_amount = stats[6]
    assert s_amount.lower_bound == b"\x00"  # unscaled 0
    # unscaled 99*100 = 9900 -> 0x26AC
    assert s_amount.upper_bound == (9900).to_bytes(2, "big")


def test_all_null_column_has_no_bounds():
    columns = (Column(name="x", type="long", field_id=1, ordinal=0),)
    schema = columns_to_arrow_schema(columns)
    table = pa.table({"x": pa.array([None, None, None], pa.int64())}, schema=schema)
    meta = _write(table, row_group_size=2)
    (s,) = extract_column_stats(meta, columns)
    assert s.value_count == 3
    assert s.null_count == 3
    assert s.lower_bound is None
    assert s.upper_bound is None


def test_all_null_row_group_does_not_suppress_bounds():
    columns = (Column(name="x", type="long", field_id=1, ordinal=0),)
    schema = columns_to_arrow_schema(columns)
    # first row group all null, second has values
    table = pa.table({"x": pa.array([None, None, 5, 9], pa.int64())}, schema=schema)
    meta = _write(table, row_group_size=2)
    assert meta.num_row_groups == 2
    (s,) = extract_column_stats(meta, columns)
    assert s.value_count == 4
    assert s.null_count == 2
    assert s.lower_bound == struct.pack("<q", 5)
    assert s.upper_bound == struct.pack("<q", 9)


def test_unknown_file_column_ignored():
    columns = (Column(name="x", type="long", field_id=1, ordinal=0),)
    table = pa.table({"x": [1, 2], "y": ["a", "b"]})
    meta = _write(table, row_group_size=10)
    stats = extract_column_stats(meta, columns)
    assert [s.field_id for s in stats] == [1]


# -- the new scalar widths, end to end through a real footer -----------------


def _one_column_bounds(col_type, values, arrow_type=None, type_params=None):
    columns = (
        Column(name="x", type=col_type, field_id=1, ordinal=0, type_params=type_params),
    )
    schema = columns_to_arrow_schema(columns)
    arr = pa.array(values, arrow_type or schema.field("x").type)
    meta = _write(pa.table({"x": arr}, schema=schema), row_group_size=2)
    (s,) = extract_column_stats(meta, columns)
    return s


@pytest.mark.parametrize(
    ("col_type", "values"),
    [
        ("int8", [-128, 0, 127]),
        ("int16", [-32768, 0, 32767]),
        ("uint8", [0, 128, 255]),
        ("uint16", [0, 32768, 65535]),
        # uint32 writes as INT64 under the writer contract, so the whole
        # unsigned domain survives the footer as plain positive ints
        ("uint32", [0, 2**31, 2**32 - 1]),
        ("uint64", [0, 2**63, 2**64 - 1]),
    ],
)
def test_integer_width_bounds_from_footer(col_type, values):
    s = _one_column_bounds(col_type, values)
    assert s.lower_bound == encode_bound(col_type, min(values))
    assert s.upper_bound == encode_bound(col_type, max(values))


def test_uint64_footer_stats_are_unsigned_not_sign_wrapped():
    """Verified against pyarrow: a UINT64 column's min/max come back as
    non-negative Python ints all the way to 2^64-1. If they ever arrived
    signed, every bound above 2^63 would encode as a negative
    decimal(20,0) and prune wrong."""
    s = _one_column_bounds("uint64", [2**63, 2**64 - 1])
    assert s.upper_bound == encode_bound("uint64", 2**64 - 1)
    assert s.upper_bound == b"\x00" + b"\xff" * 8  # 9 bytes, 0x00 sign byte


@pytest.mark.parametrize("col_type", ["timestamp_s", "timestamp_ms"])
def test_timestamp_seconds_millis_bounds_are_micros(col_type):
    """pyarrow hands these back as datetimes (a timestamp[s] column is
    even written as parquet Timestamp(MILLIS) — parquet has no seconds
    unit), and encode_bound converts them to the stored micros."""
    values = [datetime(1969, 12, 31, 0, 0, 0), datetime(2026, 9, 5, 12, 0, 0)]
    s = _one_column_bounds(col_type, values)
    assert s.lower_bound == encode_bound("timestamp", values[0])
    assert s.upper_bound == encode_bound("timestamp", values[1])


def test_timestamp_nanos_bounds_come_from_raw_statistics():
    """pyarrow's st.min/st.max RAISE ValueError for a timestamp[ns]
    statistic that is not a whole microsecond ("not safely convertible
    to microseconds"), and round the ones that are. stats.py reads
    min_raw/max_raw instead, which are nanos — the stored unit."""
    nanos = [-1, 1, 1_788_609_600_123_456_789]
    meta = _write(
        pa.table({"x": pa.array(nanos, pa.timestamp("ns"))}), row_group_size=2
    )
    raw = meta.row_group(0).column(0).statistics
    with pytest.raises(ValueError, match="not safely convertible"):
        _ = raw.max  # pinned: the hazard the raw path routes around

    s = _one_column_bounds("timestamp_ns", nanos)
    assert s.lower_bound == struct.pack("<q", min(nanos))
    assert s.upper_bound == struct.pack("<q", max(nanos))
    assert decode_bound("timestamp_ns", s.upper_bound) == max(nanos)


@pytest.mark.parametrize(
    ("unit", "nanos_per_tick"),
    [("s", 1_000_000_000), ("ms", 1_000_000), ("us", 1_000)],
)
def test_timestamp_nanos_scales_a_foreign_unit_footer(unit, nanos_per_tick):
    """min_raw is the stored int64 in the FILE's unit, not necessarily
    nanos. pyhoglake's own writer emits NANOS, but a foreign client's
    file under a timestamp_ns column would otherwise be read 10^6 (or
    10^3) too small with no symptom but wrong pruning."""
    columns = (Column(name="x", type="timestamp_ns", field_id=1, ordinal=0),)
    ticks = [-3, 5]
    meta = _write(
        pa.table({"x": pa.array(ticks, pa.timestamp(unit))}), row_group_size=2
    )
    (s,) = extract_column_stats(meta, columns)
    # Scaling up is exact, so the bound is the TRUE nanosecond instant
    # whatever the file's unit turned out to be. The "s" case is the
    # sharp one: parquet has no seconds unit, so pyarrow rewrote those
    # ticks as millis, and only reading the footer's own unit (rather
    # than the declared one) still lands on the right instant.
    assert s.lower_bound == struct.pack("<q", min(ticks) * nanos_per_tick)
    assert s.upper_bound == struct.pack("<q", max(ticks) * nanos_per_tick)


def test_timestamp_nanos_on_a_non_timestamp_footer_drops_bounds():
    """A footer field that is not a timestamp gives no unit to scale by,
    so the raw int64s mean nothing. Bounds go NULL rather than being
    read as nanos — and st.min is never touched, since on a genuine ns
    column that is the call that raises."""
    columns = (Column(name="x", type="timestamp_ns", field_id=1, ordinal=0),)
    meta = _write(pa.table({"x": pa.array([1, 2], pa.int64())}), row_group_size=2)
    (s,) = extract_column_stats(meta, columns)
    assert s.value_count == 2  # the honest counts survive
    assert s.lower_bound is None
    assert s.upper_bound is None


def test_json_bounds_are_the_document_bytes():
    """Parquet reports a JSON column's stats as bytes; encode_bound's
    string branch passes them through untouched."""
    if not hasattr(pa, "json_"):
        pytest.skip("pyarrow without pa.json_()")
    docs = ['{"a": 1}', '{ "b" : "héllo" }', "{}"]
    s = _one_column_bounds("json", docs)
    assert s.lower_bound == min(docs).encode("utf-8")
    assert s.upper_bound == max(docs).encode("utf-8")


def test_uint32_column_written_as_uint32_still_reads():
    """The writer pins uint32 to INT64, but the READ path accepts a
    genuine parquet UINT32 column too — its stats are plain ints, so the
    hand-off to encode_bound is unchanged."""
    columns = (Column(name="x", type="uint32", field_id=1, ordinal=0),)
    values = [0, 2**32 - 1]
    meta = _write(pa.table({"x": pa.array(values, pa.uint32())}), row_group_size=2)
    (s,) = extract_column_stats(meta, columns)
    assert s.lower_bound == encode_bound("uint32", 0)
    assert s.upper_bound == encode_bound("uint32", 2**32 - 1)


# -- signed-zero bound determinism (bugs.md #20) -----------------------------


def test_float_zero_bounds_deterministic_across_row_group_orders():
    """bugs.md #20 regression: -0.0/0.0 bound bytes must not depend on
    row-group order. Python's min/max keep the FIRST of equal values
    (and -0.0 == 0.0), so mixed-sign zeros across row groups could flip
    the encoded sign bit with the write order. Bounds now use IEEE
    total-order semantics (java.lang.Double.compare: -0.0 < 0.0), so
    both orders produce identical bytes: min prefers -0.0, max prefers
    +0.0. (pyarrow's parquet writer already normalizes each row group's
    zero stats the same way, so this also pins us to its convention.)"""
    columns = (Column(name="x", type="double", field_id=1, ordinal=0),)
    schema = columns_to_arrow_schema(columns)

    def bounds(values):
        table = pa.table({"x": pa.array(values, pa.float64())}, schema=schema)
        meta = _write(table, row_group_size=1)  # one row group per value
        assert meta.num_row_groups == len(values)
        (s,) = extract_column_stats(meta, columns)
        return s.lower_bound, s.upper_bound

    neg_zero = struct.pack("<d", -0.0)
    pos_zero = struct.pack("<d", 0.0)
    assert neg_zero != pos_zero  # the sign bit is real on the wire

    lo_a, hi_a = bounds([0.0, -0.0])
    lo_b, hi_b = bounds([-0.0, 0.0])
    assert (lo_a, hi_a) == (lo_b, hi_b)  # order-independent bytes
    assert lo_a == neg_zero  # min prefers -0.0
    assert hi_a == pos_zero  # max prefers +0.0


def test_float_total_order_key_matches_double_compare():
    from pyhoglake.stats import _float_total_order_key as key

    # java.lang.Double.compare ordering on the interesting values
    ordered = [
        float("-inf"),
        -2.0,
        -1.0,
        -0.0,
        0.0,
        1.0,
        2.0,
        float("inf"),
    ]
    assert sorted(ordered, key=key) == ordered
    assert key(-0.0) < key(0.0)  # the pair Python's min/max cannot split
    # deterministic zero choice regardless of argument order
    assert str(min([0.0, -0.0], key=key)) == "-0.0"
    assert str(min([-0.0, 0.0], key=key)) == "-0.0"
    assert str(max([0.0, -0.0], key=key)) == "0.0"
    assert str(max([-0.0, 0.0], key=key)) == "0.0"
