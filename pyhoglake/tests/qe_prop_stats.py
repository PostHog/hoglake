"""Property-based fuzzing of footer stats extraction.

Generated tables (random types, random null placement, random row-group
splits) -> extract_column_stats must agree with ground truth computed
independently from the raw Python values.

Pinned policies (verified here):

* value_count is the row count (nulls included), null_count is exact,
  for every row-group split.
* Bounds decode back to exactly the true min/max over non-null values.
* NaN: parquet-cpp excludes NaN from min/max, so bounds never contain
  NaN; a column whose only non-null values are NaN gets NO bounds.
  nan_count is NEVER populated by pyhoglake (always absent from the
  wire) — the server can only ever see nan_count from other clients.
* All-null column -> null_count honest, no bounds. All-null row group
  does not suppress bounds from other groups.
* 0-row file -> no column_stats at all (pyarrow writes one empty row
  group without per-chunk statistics objects).
* Strings longer than parquet-cpp's 4096-byte statistics cap -> that
  row group has no min/max -> bounds omitted entirely (never a
  truncated — i.e. WRONG — upper bound).
* A file column absent from the catalog schema is skipped.
* timestamp_ns bounds come from Statistics.min_raw/max_raw, not
  min/max: pyarrow renders a timestamp[ns] statistic as a datetime and
  RAISES ValueError for any value that is not a whole microsecond. The
  raw int64 is nanos, which is the stored unit encode_bound wants.
* uint64 footer stats are non-negative Python ints up to 2^64-1 (never
  sign-wrapped), so no masking is needed on the hand-off.
* A statistic the bound path cannot read or represent yields NULL
  BOUNDS, never an exception out of the commit — the file's physical
  type and unit are foreign inputs, not givens. Asserted over the full
  catalog-type x footer-shape cross product, which is how the last two
  escaping paths were found.
"""

import io
import struct
from datetime import date, datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from pyhoglake import decode_bound
from pyhoglake.models import Column
from pyhoglake.stats import extract_column_stats
from pyhoglake.types import columns_to_arrow_schema

# The "qe" hypothesis profile (deadline=None) is loaded in conftest.py.
# Parquet round-trips are expensive; cap the main property explicitly.
STATS_SETTINGS = settings(
    max_examples=30,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)


# (coltype, type_params, value strategy) — value domains deliberately
# include the type boundaries and nasty values.
COLUMN_KINDS = {
    "long": (None, st.integers(-(2**63), 2**63 - 1)),
    "int": (None, st.integers(-(2**31), 2**31 - 1)),
    "double": (
        None,
        st.floats(allow_nan=False, allow_infinity=True, allow_subnormal=True),
    ),
    "float": (
        None,
        st.floats(allow_nan=False, allow_infinity=True, width=32),
    ),
    "boolean": (None, st.booleans()),
    "string": (None, st.text(max_size=40)),  # includes "" mins
    "binary": (None, st.binary(max_size=40)),
    "date": (None, st.dates(date(1, 1, 1), date(9999, 12, 31))),
    "timestamp": (
        None,
        st.datetimes(datetime(1677, 9, 22), datetime(2262, 4, 11)),
    ),
    "decimal": (
        {"precision": 18, "scale": 3},
        st.integers(-(10**18) + 1, 10**18 - 1).map(lambda n: Decimal(n).scaleb(-3)),
    ),
    "uuid": (None, st.binary(min_size=16, max_size=16)),
    "int8": (None, st.integers(-(2**7), 2**7 - 1)),
    "int16": (None, st.integers(-(2**15), 2**15 - 1)),
    "uint8": (None, st.integers(0, 2**8 - 1)),
    "uint16": (None, st.integers(0, 2**16 - 1)),
    # uint32 writes as parquet INT64 (the writer contract), so its stats
    # come back as plain ints across the whole unsigned domain
    "uint32": (None, st.integers(0, 2**32 - 1)),
    # the footer reports a UINT64 column's min/max as a non-negative
    # Python int all the way to 2^64-1 — never sign-wrapped (verified)
    "uint64": (None, st.integers(0, 2**64 - 1)),
    # seconds/millis columns: pyarrow reports these as datetimes, which
    # encode_bound converts to the stored micros
    "timestamp_s": (
        None,
        st.datetimes(datetime(1700, 1, 1), datetime(2400, 1, 1)).map(
            lambda d: d.replace(microsecond=0)
        ),
    ),
    "timestamp_ms": (
        None,
        st.datetimes(datetime(1700, 1, 1), datetime(2400, 1, 1)).map(
            lambda d: d.replace(microsecond=(d.microsecond // 1000) * 1000)
        ),
    ),
    # nanos: ground truth is the raw int, because arrow will not render
    # a sub-microsecond timestamp[ns] as a datetime at all
    "timestamp_ns": (None, st.integers(-(2**62), 2**62)),
    "json": (None, st.text(max_size=40)),
}


@st.composite
def stats_case(draw):
    kinds = draw(
        st.lists(st.sampled_from(sorted(COLUMN_KINDS)), min_size=1, max_size=4)
    )
    n_rows = draw(st.integers(0, 40))
    columns = []
    values = []
    for i, kind in enumerate(kinds):
        params, value_st = COLUMN_KINDS[kind]
        columns.append(
            Column(
                name=f"c{i}",
                type=kind,
                field_id=i + 1,
                ordinal=i,
                nullable=True,
                type_params=params,
            )
        )
        values.append(
            draw(
                st.lists(
                    st.one_of(st.none(), value_st),
                    min_size=n_rows,
                    max_size=n_rows,
                )
            )
        )
    row_group_size = draw(st.integers(1, 41))
    return tuple(columns), values, n_rows, row_group_size


def _ground_truth_minmax(kind, vals):
    nn = [v for v in vals if v is not None]
    if not nn:
        return None, None
    return min(nn), max(nn)


def _normalize(kind, decoded):
    if kind == "uuid":
        return decoded.bytes  # ground truth is raw 16-byte strings
    return decoded


def _write_meta(table, row_group_size):
    sink = io.BytesIO()
    pq.write_table(table, sink, row_group_size=row_group_size)
    return pq.read_metadata(io.BytesIO(sink.getvalue()))


@STATS_SETTINGS
@given(stats_case())
def test_extracted_stats_match_ground_truth(case):
    columns, values, n_rows, row_group_size = case
    schema = columns_to_arrow_schema(columns)
    table = pa.table(
        {
            c.name: pa.array(v, schema.field(c.name).type)
            for c, v in zip(columns, values)
        },
        schema=schema,
    )
    meta = _write_meta(table, row_group_size)
    stats = {s.field_id: s for s in extract_column_stats(meta, columns)}

    if n_rows == 0:
        # pinned: empty file ships no column stats at all
        assert stats == {}
        return

    for c, vals in zip(columns, values):
        s = stats[c.field_id]
        assert s.value_count == n_rows
        assert s.null_count == sum(1 for v in vals if v is None)
        assert s.nan_count is None  # pinned: pyhoglake never reports it
        assert s.size_bytes is not None and s.size_bytes > 0

        lo, hi = _ground_truth_minmax(c.type, vals)
        if lo is None:
            assert s.lower_bound is None and s.upper_bound is None
            continue
        assert s.lower_bound is not None and s.upper_bound is not None
        got_lo = _normalize(c.type, decode_bound(c.type, s.lower_bound, c.type_params))
        got_hi = _normalize(c.type, decode_bound(c.type, s.upper_bound, c.type_params))
        if c.type in ("double", "float"):
            # -0.0 == 0.0 makes plain == fine, but keep inf exact
            assert got_lo == lo and got_hi == hi
        else:
            assert got_lo == lo
            assert got_hi == hi


@STATS_SETTINGS
@given(st.data())
def test_every_column_kind_is_exercised_at_least_once(data):
    """The property above samples 4 kinds out of 21 per example, so a new
    type could ride along untested. This one walks the whole table."""
    for kind, (params, value_st) in sorted(COLUMN_KINDS.items()):
        col = Column(
            name="c",
            type=kind,
            field_id=1,
            ordinal=0,
            nullable=True,
            type_params=params,
        )
        vals = data.draw(
            st.lists(st.one_of(st.none(), value_st), min_size=1, max_size=6)
        )
        schema = columns_to_arrow_schema((col,))
        table = pa.table({"c": pa.array(vals, schema.field("c").type)}, schema=schema)
        (s,) = extract_column_stats(_write_meta(table, 2), (col,))
        assert s.value_count == len(vals)
        assert s.null_count == sum(1 for v in vals if v is None)
        lo, hi = _ground_truth_minmax(kind, vals)
        if lo is None:
            assert s.lower_bound is None and s.upper_bound is None
            continue
        assert _normalize(kind, decode_bound(kind, s.lower_bound, params)) == lo
        assert _normalize(kind, decode_bound(kind, s.upper_bound, params)) == hi


# -- foreign footers: a mismatched catalog type must degrade, not raise ----
#
# Everything above writes the arrow type the catalog column implies. The
# stats path is also handed footers OTHER writers produced, where the
# physical type and unit are inputs rather than givens; the whole cross
# product is walked here because the failure mode is an exception out of
# an otherwise-valid append, and each type pairing reaches the bound code
# by a different route.

_FOREIGN_FOOTERS = {
    "int64": (pa.int64(), [0, 2**40]),  # wider than a 4-byte Iceberg int
    "timestamp_us": (pa.timestamp("us"), [0, 253_402_214_400_000_000]),  # year 9999
    "timestamp_ms": (pa.timestamp("ms"), [-(2**40), 2**40]),
    "timestamp_ns": (pa.timestamp("ns"), [1, 1_000_000_001]),  # sub-micro: st.min dies
    "string": (pa.string(), ["a", "z"]),
    "double": (pa.float64(), [1.5, 2.5]),
    "binary": (pa.binary(), [b"\x00", b"\xff"]),
    "boolean": (pa.bool_(), [False, True]),
}

#: (footer, catalog type) cells where the two genuinely AGREE, so a bound
#: must actually come out. Without these the test above is satisfied by a
#: codec that returned None for everything — "did not raise" is a very
#: low bar, and nulling every bound in the lake clears it.
_MUST_PRODUCE = {
    ("int64", "long"),
    ("double", "double"),
    # NOT ("double", "float"): a float64 footer under a `float` column is
    # a NARROWING, and Kotlin's FLOAT arm takes only a FLOAT physical.
    # Python used to produce a bound here purely because it never looked
    # at the footer's type; it now refuses, like the hydrator. (The
    # reverse, float32 under `double`, is a legal widening and is
    # accepted by both — covered by the property test's own matrix.)
    ("string", "string"),
    ("string", "json"),
    ("binary", "binary"),
    ("boolean", "boolean"),
    # timestamptz is absent from COLUMN_KINDS, so there is no cell to
    # claim — caught by the seen == _MUST_PRODUCE check, which is what it
    # is for.
    #
    # Every timestamp precision reads a timestamp footer of ANY unit:
    # the decode is unit-driven (the file's annotation says what its
    # int64s mean) and scales to the type's own stored unit. So the
    # timestamp footers x timestamp catalog types form a full block, not
    # a diagonal.
    ("timestamp_us", "timestamp"),
    ("timestamp_us", "timestamp_s"),
    ("timestamp_us", "timestamp_ms"),
    ("timestamp_ms", "timestamp"),
    ("timestamp_ms", "timestamp_s"),
    ("timestamp_ms", "timestamp_ms"),
    # A millis footer scales UP to nanos exactly; a nanos footer is read
    # raw. Both are the paths a timestamp_ns column actually takes.
    ("timestamp_ms", "timestamp_ns"),
    ("timestamp_ns", "timestamp_ns"),
    # BYTE_ARRAY is BYTE_ARRAY: string, json and binary share a physical
    # form, and the catalog type decides only how the bytes are read.
    ("binary", "string"),
    ("binary", "json"),
    # uint32 maps to Iceberg long, and hoglake's own writer emits INT64
    # for it — so an int64 footer is the NATIVE shape, not a mismatch.
    ("int64", "uint32"),
}


def test_mismatched_catalog_type_over_a_foreign_footer_degrades():
    """EVERY catalog type over EVERY foreign footer shape degrades to
    absent bounds rather than raising.

    The cross product is the point: the two gaps this originally found
    (float/double's total-order min/max running outside the guard, and
    decimal's InvalidOperation being an ArithmeticError rather than a
    ValueError) were each reachable only from one cell of it, and
    neither was reachable from the paths anyone had thought to test.
    A foreign footer is an INPUT, not a given — the writer does not get
    to fail a commit because someone else's file had a statistic it
    could not read.

    The assertion is TWO-SIDED. _MUST_PRODUCE pins the cells where
    footer and catalog type correspond — a codec that degraded
    everything to null cannot pass — and every other cell must produce
    NO bound at all, so a codec that coerces mismatched values into
    plausible-looking bytes cannot pass either. A cell that legitimately
    starts producing belongs in _MUST_PRODUCE explicitly; it does not
    belong in a weakened else-branch.
    """
    seen: set[tuple[str, str]] = set()
    for footer, (arrow_type, values) in sorted(_FOREIGN_FOOTERS.items()):
        table = pa.table({"c": pa.array(values, arrow_type)})
        meta = _write_meta(table, 1)  # one row group per value
        for kind, (params, _) in sorted(COLUMN_KINDS.items()):
            col = Column(name="c", type=kind, field_id=1, ordinal=0, type_params=params)
            stats = extract_column_stats(meta, (col,))
            # counts never depend on whether a bound could be read
            assert len(stats) == 1, (kind, footer)
            assert stats[0].value_count == len(values), (kind, footer)
            assert stats[0].null_count == 0, (kind, footer)
            if (footer, kind) in _MUST_PRODUCE:
                assert stats[0].lower_bound is not None, (kind, footer)
                assert stats[0].upper_bound is not None, (kind, footer)
                seen.add((footer, kind))
            else:
                # Two-sided, and this half is the one that matters most:
                # a cell where footer and catalog type do NOT correspond
                # must produce NO bound, not an arbitrary one. Before the
                # physical-type check existed, a boolean footer under a
                # `long` column truthiness-coerced False/True into bounds
                # of 0/1 — well-formed bytes describing data that is not
                # there, which prunes real rows away.
                assert stats[0].lower_bound is None, (kind, footer)
                assert stats[0].upper_bound is None, (kind, footer)
    # Every declared cell was actually reachable: a typo in _MUST_PRODUCE
    # would otherwise make it a set of assertions nobody runs.
    assert seen == _MUST_PRODUCE, _MUST_PRODUCE - seen


# -- NaN policy (targeted; property above excludes NaN by construction) ----


def _double_col():
    return (Column(name="f", type="double", field_id=1, ordinal=0),)


def test_nan_values_never_reach_bounds():
    cols = _double_col()
    t = pa.table(
        {"f": pa.array([1.0, float("nan"), 3.0], pa.float64())},
        schema=columns_to_arrow_schema(cols),
    )
    (s,) = extract_column_stats(_write_meta(t, 10), cols)
    assert s.null_count == 0  # NaN is not null
    assert decode_bound("double", s.lower_bound) == 1.0
    assert decode_bound("double", s.upper_bound) == 3.0
    assert s.nan_count is None  # pinned: never computed client-side


def test_all_nan_column_has_no_bounds():
    cols = _double_col()
    t = pa.table(
        {"f": pa.array([float("nan"), float("nan")], pa.float64())},
        schema=columns_to_arrow_schema(cols),
    )
    (s,) = extract_column_stats(_write_meta(t, 10), cols)
    assert s.value_count == 2
    assert s.null_count == 0
    assert s.lower_bound is None and s.upper_bound is None


def test_negative_zero_bounds_bitpattern():
    cols = _double_col()
    t = pa.table(
        {"f": pa.array([-0.0, 0.0], pa.float64())},
        schema=columns_to_arrow_schema(cols),
    )
    (s,) = extract_column_stats(_write_meta(t, 10), cols)
    # parquet-cpp orders -0.0 < 0.0 for stats; the encoded bounds must
    # carry the sign bit
    assert s.lower_bound == struct.pack("<d", -0.0)
    assert s.lower_bound[-1] == 0x80
    assert s.upper_bound == struct.pack("<d", 0.0)


# -- oversized string stats -------------------------------------------------


def test_string_beyond_4096_stat_cap_omits_bounds_not_truncates():
    cols = (Column(name="s", type="string", field_id=1, ordinal=0),)
    big = "z" * 5000
    t = pa.table({"s": pa.array(["a", big])}, schema=columns_to_arrow_schema(cols))
    (s,) = extract_column_stats(_write_meta(t, 10), cols)
    assert s.value_count == 2 and s.null_count == 0
    # a truncated max ("zzz...z"[:4096]) would be an INVALID upper bound
    # (< the actual max); the honest answer is no bounds at all
    assert s.lower_bound is None and s.upper_bound is None


def test_string_at_4096_cap_keeps_exact_bounds():
    cols = (Column(name="s", type="string", field_id=1, ordinal=0),)
    lo, hi = "a" * 4096, "z" * 4096
    t = pa.table({"s": pa.array([lo, hi])}, schema=columns_to_arrow_schema(cols))
    (s,) = extract_column_stats(_write_meta(t, 10), cols)
    assert s.lower_bound == lo.encode()
    assert s.upper_bound == hi.encode()


# -- stats disabled entirely ------------------------------------------------


def test_write_statistics_false_yields_no_stats_rows():
    cols = (Column(name="x", type="long", field_id=1, ordinal=0),)
    t = pa.table({"x": [1, 2]}, schema=columns_to_arrow_schema(cols))
    sink = io.BytesIO()
    pq.write_table(t, sink, write_statistics=False)
    meta = pq.read_metadata(io.BytesIO(sink.getvalue()))
    # no null_count available -> the honest move is to omit the column
    assert extract_column_stats(meta, cols) == []


# -- the dot-name collision bug --------------------------------------------


def test_dot_named_column_not_misattributed_regression():
    cols = (
        Column(name="a", type="long", field_id=1, ordinal=0),
        Column(name="a.b", type="string", field_id=2, ordinal=1),
    )
    t = pa.table(
        {
            "a": pa.array([1, 2], pa.int64()),
            "a.b": pa.array(["x", "y"], pa.string()),
        }
    )
    stats = {s.field_id: s for s in extract_column_stats(_write_meta(t, 10), cols)}
    assert decode_bound("long", stats[1].lower_bound) == 1
    assert stats[2].lower_bound == b"x"


# -- misc edges -------------------------------------------------------------


def test_file_column_missing_from_catalog_is_skipped():
    cols = (Column(name="x", type="long", field_id=1, ordinal=0),)
    t = pa.table({"x": [3], "ghost": ["boo"]})
    stats = extract_column_stats(_write_meta(t, 10), cols)
    assert [s.field_id for s in stats] == [1]


def test_int64_extremes_roundtrip_through_footer():
    cols = (Column(name="x", type="long", field_id=1, ordinal=0),)
    t = pa.table(
        {"x": pa.array([-(2**63), 2**63 - 1], pa.int64())},
        schema=columns_to_arrow_schema(cols),
    )
    (s,) = extract_column_stats(_write_meta(t, 1), cols)  # 2 row groups
    assert decode_bound("long", s.lower_bound) == -(2**63)
    assert decode_bound("long", s.upper_bound) == 2**63 - 1


def test_every_row_group_all_null_still_reports_counts():
    cols = (Column(name="x", type="long", field_id=1, ordinal=0),)
    t = pa.table(
        {"x": pa.array([None] * 5, pa.int64())},
        schema=columns_to_arrow_schema(cols),
    )
    (s,) = extract_column_stats(_write_meta(t, 2), cols)
    assert s.value_count == 5
    assert s.null_count == 5
    assert s.lower_bound is None and s.upper_bound is None
