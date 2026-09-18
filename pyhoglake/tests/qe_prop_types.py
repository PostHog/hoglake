"""Property-based fuzzing of the arrow <-> hoglake ColType mapping.

Two totality claims under test:

1. Round-trip: every supported (coltype, type_params) maps to an arrow
   type and back to itself; every canonical arrow type is a fixed point
   after one round-trip (non-canonical spellings — large_string,
   tz-of-any-name, uuid extension — converge to a fixed point in one
   hop and never drift further). "uint32" is the ONE exception, carved
   out into its own test: its writer contract is pa.int64(), so it
   settles on "long" — see test_uint32_converges_to_long_in_one_hop.
2. Rejection completeness: every generated exotic arrow type (float16,
   date64, the time32/time64(ns) widths, tz-aware non-micros
   timestamps, durations, decimal256, dictionaries) raises
   UnsupportedTypeError — never a silent wrong mapping, never a
   different exception type — AND so does any nesting that contains
   one, however deep. list/struct/map over supported inner types are
   themselves supported since phase 2, so the exotic generator is
   rooted at unsupported LEAVES: every type it builds carries at least
   one, which is what keeps the property about rejection rather than
   about nesting.
"""

import pyarrow as pa
import pytest
from hypothesis import given
from hypothesis import strategies as st

from pyhoglake import UnsupportedTypeError, arrow_type_to_coltype, coltype_to_arrow
from pyhoglake.models import Column
from pyhoglake.types import (
    PARQUET_FIELD_ID_KEY,
    columns_to_arrow_schema,
    schema_to_column_defs,
)

# The "qe" hypothesis profile (deadline=None) is loaded in conftest.py.

# -- generators -------------------------------------------------------------

#: pa.json_() arrived in pyarrow 19; the project floor is 17. Without it
#: "json" maps to pa.string() and comes back as "string", so it is not a
#: round-trip type on that pyarrow and must leave the identity property.
HAS_JSON = hasattr(pa, "json_")

#: Round-trip coltypes: coltype -> arrow -> the SAME coltype. "uint32" is
#: deliberately absent (it maps to pa.int64(), i.e. "long" on the way
#: back) — see test_uint32_converges_to_long_in_one_hop.
SIMPLE_COLTYPES = [
    "boolean",
    "int8",
    "int16",
    "int",
    "long",
    "uint8",
    "uint16",
    "uint64",
    "float",
    "double",
    "string",
    "binary",
    "date",
    "time",
    "timestamp_s",
    "timestamp_ms",
    "timestamp",
    "timestamp_ns",
    "timestamptz",
    "uuid",
] + (["json"] if HAS_JSON else [])

coltype_with_params = st.one_of(
    st.tuples(st.sampled_from(SIMPLE_COLTYPES), st.none()),
    st.tuples(
        st.just("decimal"),
        st.integers(1, 38).flatmap(
            lambda p: st.integers(0, p).map(lambda s: {"precision": p, "scale": s})
        ),
    ),
)

canonical_arrow = st.one_of(
    st.sampled_from(
        [
            pa.bool_(),
            pa.int8(),
            pa.int16(),
            pa.int32(),
            pa.int64(),
            pa.uint8(),
            pa.uint16(),
            pa.uint64(),
            pa.float32(),
            pa.float64(),
            pa.string(),
            pa.binary(),
            pa.date32(),
            pa.time64("us"),
            pa.timestamp("s"),
            pa.timestamp("ms"),
            pa.timestamp("us"),
            pa.timestamp("ns"),
            pa.timestamp("us", tz="UTC"),
            pa.binary(16),
        ]
        + ([pa.json_()] if HAS_JSON else [])
    ),
    st.integers(1, 38).flatmap(
        lambda p: st.integers(0, p).map(lambda s: pa.decimal128(p, s))
    ),
)

noncanonical_arrow = st.one_of(
    st.sampled_from([pa.large_string(), pa.large_binary()]),
    st.sampled_from(["America/New_York", "Asia/Tokyo", "+05:30", "Europe/Berlin"]).map(
        lambda tz: pa.timestamp("us", tz=tz)
    ),
)

_unsupported_scalars = [
    pa.null(),
    pa.float16(),
    pa.date64(),
    pa.time32("s"),
    pa.time32("ms"),
    pa.time64("ns"),
    # tz-aware is micros-only: hoglake has no timestamptz_s/_ms/_ns, so a
    # tz-aware second/milli/nano column has nowhere to land.
    pa.timestamp("s", tz="UTC"),
    pa.timestamp("ms", tz="UTC"),
    pa.timestamp("ns", tz="UTC"),
    pa.timestamp("s", tz="America/New_York"),
    pa.duration("s"),
    pa.duration("us"),
    pa.duration("ns"),
    pa.month_day_nano_interval(),
    pa.decimal256(40, 2),
    pa.decimal256(10, 2),
]

unsupported_scalar = st.one_of(
    st.sampled_from(_unsupported_scalars),
    # fixed_size_binary of any width except the uuid-blessed 16
    st.integers(1, 64).filter(lambda w: w != 16).map(pa.binary),
    # dictionary-encoded: even dictionary<string> must be rejected. A
    # LEAF, not a nesting — hoglake has no dictionary column type, so
    # there is nothing to descend into.
    st.sampled_from([pa.string(), pa.int64()]).map(
        lambda v: pa.dictionary(pa.int32(), v)
    ),
)


def _nest(inner: st.SearchStrategy) -> st.SearchStrategy:
    """Wrap a strategy in every container spelling arrow offers."""
    return st.one_of(
        inner.map(pa.list_),
        inner.map(pa.large_list),
        st.tuples(inner, st.integers(1, 4)).map(lambda t: pa.list_(t[0], t[1])),
        inner.map(lambda t: pa.struct([("a", t)])),
        st.tuples(inner, inner).map(lambda t: pa.struct([("a", t[0]), ("b", t[1])])),
        inner.map(lambda t: pa.map_(pa.string(), t)),
    )


#: Rooted at UNSUPPORTED leaves, so every generated type contains one
#: however deeply it is wrapped. Nesting over supported leaves is the
#: other property below (test_nested_over_supported_inner_is_supported).
exotic_arrow = st.recursive(unsupported_scalar, _nest, max_leaves=6)

#: The mirror: containers over supported inner types, which ARE
#: supported. Without this the change that made list/struct/map real
#: would have been invisible to this module — every assertion here is
#: about refusal, and refusal got easier, not harder.
supported_nested = st.recursive(canonical_arrow, _nest, max_leaves=4)


# -- round-trip properties --------------------------------------------------


@given(coltype_with_params)
def test_coltype_to_arrow_to_coltype_is_identity(tp):
    coltype, params = tp
    arrow = coltype_to_arrow(coltype, params)
    back, back_params = arrow_type_to_coltype(arrow)
    assert back == coltype
    if coltype == "decimal":
        assert back_params == params
    else:
        assert back_params is None


@given(canonical_arrow)
def test_canonical_arrow_is_roundtrip_fixed_point(t):
    coltype, params = arrow_type_to_coltype(t)
    back = coltype_to_arrow(coltype, params)
    # uuid is the one canonical mapping that is not type-identical
    # (fixed_size_binary(16) -> "uuid" -> fixed_size_binary(16)); all
    # others must be exactly equal.
    assert back.equals(t)


@given(noncanonical_arrow)
def test_noncanonical_arrow_converges_in_one_hop(t):
    c1, p1 = arrow_type_to_coltype(t)
    a1 = coltype_to_arrow(c1, p1)
    c2, p2 = arrow_type_to_coltype(a1)
    assert (c1, p1) == (c2, p2)
    assert coltype_to_arrow(c2, p2).equals(a1)  # stable thereafter


def test_uint32_converges_to_long_in_one_hop():
    """The single mapping that is not a round trip in EITHER direction,
    and the only one excluded from the two identity properties above.

    ``coltype_to_arrow("uint32")`` is ``pa.int64()`` on purpose (the
    writer contract — pyarrow's pa.uint32() becomes parquet INT32 +
    Int(32, unsigned), which an Iceberg reader takes as SIGNED, so
    values above 2^31 would read back negative). So "uint32" settles on
    "long" after one hop, and ``pa.uint32()`` settles on ``pa.int64()``.
    Both then stay put — the mapping loses the unsigned NAME, never a
    value, and never drifts further.
    """
    assert coltype_to_arrow("uint32") == pa.int64()
    assert arrow_type_to_coltype(pa.int64()) == ("long", None)
    assert coltype_to_arrow("long") == pa.int64()  # fixed point reached

    assert arrow_type_to_coltype(pa.uint32()) == ("uint32", None)
    a1 = coltype_to_arrow("uint32")
    assert arrow_type_to_coltype(a1)[0] == "long"
    assert coltype_to_arrow("long").equals(a1)  # stable thereafter


# -- rejection completeness -------------------------------------------------


@given(exotic_arrow)
def test_exotic_types_always_raise_unsupported(t):
    with pytest.raises(UnsupportedTypeError):
        arrow_type_to_coltype(t)


@given(exotic_arrow.map(lambda t: pa.schema([pa.field("x", t)])))
def test_exotic_types_rejected_at_schema_level_too(schema):
    with pytest.raises(UnsupportedTypeError):
        schema_to_column_defs(schema)


@given(supported_nested)
def test_nested_over_supported_inner_is_supported(t):
    """A container over supported leaves maps, and maps to a container.

    The shape rules ride along: a list's child is always named `element`,
    a map's are `key` (non-nullable) then `value`, and a struct's keep
    their own names — the server enforces exactly this, so a client that
    emitted anything else would be shipping a guaranteed 422.
    """
    coltype, params = arrow_type_to_coltype(t)
    assert params is None or coltype == "decimal"
    defs = schema_to_column_defs(pa.schema([pa.field("x", t)]))
    assert len(defs) == 1
    _assert_nested_shape(defs[0])


def _assert_nested_shape(col):
    kids = col.get("children")
    if col["type"] not in ("list", "struct", "map"):
        assert kids is None
        return
    assert kids, f"{col['type']} must have children"
    if col["type"] == "list":
        assert [k["name"] for k in kids] == ["element"]
    elif col["type"] == "map":
        assert [k["name"] for k in kids] == ["key", "value"]
        assert kids[0]["nullable"] is False
    for k in kids:
        _assert_nested_shape(k)


def test_unsupported_type_error_is_typeerror_and_hoglake_error():
    from pyhoglake import HoglakeError

    try:
        arrow_type_to_coltype(pa.float16())  # int8 is a supported type now
    except UnsupportedTypeError as e:
        assert isinstance(e, TypeError)
        assert isinstance(e, HoglakeError)
    else:
        pytest.fail("no raise")


def test_decimal_missing_type_params_raise_unsupported_not_keyerror():
    with pytest.raises(UnsupportedTypeError):
        coltype_to_arrow("decimal", None)
    with pytest.raises(UnsupportedTypeError):
        coltype_to_arrow("decimal", {"precision": 10})  # scale missing
    with pytest.raises(UnsupportedTypeError):
        coltype_to_arrow("decimal", {"scale": 2})  # precision missing


# -- schema-level properties ------------------------------------------------

col_names = st.text(
    alphabet=st.characters(codec="utf-8", exclude_characters="\x00"),
    min_size=1,
    max_size=24,
)


@given(
    st.lists(
        st.tuples(col_names, coltype_with_params, st.booleans()),
        min_size=1,
        max_size=12,
        unique_by=lambda t: t[0],
    )
)
def test_schema_to_column_defs_exact_shape(cols):
    schema = pa.schema(
        [
            pa.field(name, coltype_to_arrow(ct, params), nullable=nullable)
            for name, (ct, params), nullable in cols
        ]
    )
    defs = schema_to_column_defs(schema)
    assert len(defs) == len(cols)
    for d, (name, (ct, params), nullable) in zip(defs, cols):
        expected_keys = {"name", "type", "nullable"} | (
            {"type_params"} if ct == "decimal" else set()
        )
        assert set(d) == expected_keys  # exact wire keys, nothing more
        assert d["name"] == name
        assert d["type"] == ct
        assert d["nullable"] is nullable
        if ct == "decimal":
            assert d["type_params"] == params


@given(
    st.lists(
        st.tuples(col_names, coltype_with_params, st.booleans()),
        min_size=1,
        max_size=12,
        unique_by=lambda t: t[0],
    ),
    st.randoms(use_true_random=False),
)
def test_columns_to_arrow_schema_sorts_by_ordinal_and_embeds_field_ids(cols, rnd):
    columns = [
        Column(
            name=name,
            type=ct,
            field_id=100 + i,
            ordinal=i,
            nullable=nullable,
            type_params=params,
        )
        for i, (name, (ct, params), nullable) in enumerate(cols)
    ]
    shuffled = list(columns)
    rnd.shuffle(shuffled)
    schema = columns_to_arrow_schema(shuffled)
    assert schema.names == [c.name for c in columns]  # ordinal order restored
    for c in columns:
        f = schema.field(c.name)
        assert f.nullable is c.nullable
        assert f.metadata[PARQUET_FIELD_ID_KEY] == str(c.field_id).encode()
        assert f.type.equals(coltype_to_arrow(c.type, c.type_params))
