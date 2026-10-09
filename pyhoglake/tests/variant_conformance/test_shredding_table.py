"""Suite S: where each value lands in the shredded storage, at the Arrow level.

One clause of VariantShredding.md (VS, apache/parquet-format at
bf0993925ccf41b1fb4b1ae241af4a66e8adf1fe, by line) or of the placement
policy (D3, D4, D10) per test, each run through encode_json and
encode_python. The placement rule is the Trino connector's
(VariantShredder.isShreddedPrimitive, and its TestShreddedVariantWriter rows
ported here): exact type classes, exact key names.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal

import pyarrow as pa
import pytest
from variant_helpers import same

from pyhoglake import variant
from pyhoglake.variant import VARIANT_NULL

NULL = "00"


def encode(rows: list[object], decl, source: str) -> pa.StructArray:
    """Rows as Python values; None is SQL NULL, VARIANT_NULL a Variant null.
    Through JSON text or as objects, which must land identically."""
    if source == "json":
        texts = [
            None if r is None else "null" if r is VARIANT_NULL else json.dumps(r)
            for r in rows
        ]
        array = variant.encode_json(texts, shredding=decl).array
    else:
        array = variant.encode_python(rows, shredding=decl).array
    variant.verify(array, shredding=decl)
    got = variant.to_python(array, shredding=decl)
    want = [VARIANT_NULL if r is VARIANT_NULL else r for r in rows]
    assert all(same(g, w) for g, w in zip(got, want, strict=True)), (got, want)
    return array


@pytest.fixture(params=["json", "python"])
def source(request):
    return request.param


def side(group: dict) -> tuple[object, object]:
    """``(value, typed_value)`` of a group, value as hex."""
    value = group["value"]
    return (None if value is None else value.hex(), group.get("typed_value"))


# -- S1, S2, S3, S9: the storage types (VS:40-111, 117-122, 166, 193) --------

OBJECT = {
    "type": "object",
    "fields": [
        {"name": "s", "type": "string"},
        {"name": "n", "type": "int64"},
        {"name": "v", "type": "variant"},
        {"name": "l", "type": "array", "element": {"type": "int32"}},
    ],
}


@pytest.mark.parametrize("decl", [None, {"type": "variant"}])
def test_s1_unshredded_is_required_metadata_and_required_value(decl):
    # The only unshredded shape the Trino connector reads (D11).
    assert variant.storage_type(decl) == pa.struct(
        [
            pa.field("metadata", pa.binary(), nullable=False),
            pa.field("value", pa.binary(), nullable=False),
        ]
    )
    assert variant.storage_type(decl, form="read") == variant.storage_type(decl)


def group(typed: pa.DataType) -> pa.StructType:
    return pa.struct([pa.field("value", pa.binary()), pa.field("typed_value", typed)])


def test_s1_s2_shredded_groups():
    element = pa.field("element", group(pa.int32()), nullable=False)
    assert variant.storage_type(OBJECT) == pa.struct(
        [
            pa.field("metadata", pa.binary(), nullable=False),
            pa.field("value", pa.binary()),
            pa.field(
                "typed_value",
                pa.struct(
                    [
                        # Field groups are REQUIRED; declared order and case.
                        pa.field("s", group(pa.string()), nullable=False),
                        pa.field("n", group(pa.int64()), nullable=False),
                        # A variant field has value only.
                        pa.field(
                            "v",
                            pa.struct([pa.field("value", pa.binary())]),
                            nullable=False,
                        ),
                        # Three-level lists of REQUIRED element groups.
                        pa.field("l", group(pa.list_(element)), nullable=False),
                    ]
                ),
            ),
        ]
    )


TYPED = [
    ({"type": "boolean"}, pa.bool_(), pa.bool_()),
    ({"type": "int8"}, pa.int8(), pa.int8()),
    ({"type": "int16"}, pa.int16(), pa.int16()),
    ({"type": "int32"}, pa.int32(), pa.int32()),
    ({"type": "int64"}, pa.int64(), pa.int64()),
    ({"type": "float"}, pa.float32(), pa.float32()),
    ({"type": "double"}, pa.float64(), pa.float64()),
    ({"type": "date"}, pa.date32(), pa.date32()),
    ({"type": "time"}, pa.time64("us"), pa.time64("us")),
    ({"type": "timestamp"}, pa.timestamp("us"), pa.timestamp("us")),
    ({"type": "timestamp_ns"}, pa.timestamp("ns"), pa.timestamp("ns")),
    ({"type": "timestamptz"}, pa.timestamp("us", "UTC"), pa.timestamp("us", "UTC")),
    ({"type": "timestamptz_ns"}, pa.timestamp("ns", "UTC"), pa.timestamp("ns", "UTC")),
    ({"type": "binary"}, pa.binary(), pa.binary()),
    ({"type": "string"}, pa.string(), pa.string()),
    ({"type": "uuid"}, pa.uuid(), pa.uuid()),
    # The width the file holds, unscaled, with DECIMAL(p, s) in the footer
    # (D15); a writer views them as int32/int64.
    (
        {"type": "decimal4", "precision": 9, "scale": 2},
        pa.decimal32(9, 2),
        pa.decimal128(9, 2),
    ),
    (
        {"type": "decimal8", "precision": 18, "scale": 0},
        pa.decimal64(18, 0),
        pa.decimal128(18, 0),
    ),
    (
        {"type": "decimal8", "precision": 3, "scale": 0},
        pa.decimal64(3, 0),
        pa.decimal128(3, 0),
    ),
    (
        {"type": "decimal16", "precision": 10, "scale": 2},
        pa.decimal128(10, 2),
        pa.decimal128(10, 2),
    ),
    (
        {"type": "decimal16", "precision": 38, "scale": 38},
        pa.decimal128(38, 38),
        pa.decimal128(38, 38),
    ),
]


@pytest.mark.parametrize(("decl", "write", "read"), TYPED, ids=lambda x: str(x))
def test_s3_typed_value_type_of_each_primitive(decl, write, read):
    for form, typed in (("write", write), ("read", read)):
        storage = variant.storage_type(decl, form=form)
        assert storage.field("typed_value").type == typed
        assert storage.field("value").nullable
        assert storage.field("typed_value").nullable


def test_s3_decimal_stamps_name_the_parquet_path_of_each_leaf():
    from pyhoglake._variant_codec import compile_plan

    decl = {
        "type": "object",
        "fields": [
            {"name": "a.b", "type": "decimal4", "precision": 5, "scale": 2},
            {"name": "w", "type": "decimal16", "precision": 20, "scale": 2},
            {
                "name": "xs",
                "type": "array",
                "element": {"type": "decimal8", "precision": 18, "scale": 4},
            },
        ],
    }
    assert compile_plan(decl).stamps == (
        (("typed_value", "a.b", "typed_value"), 5, 2),
        (("typed_value", "xs", "typed_value", "list", "element", "typed_value"), 18, 4),
    )
    assert compile_plan({"type": "decimal8", "precision": 9, "scale": 9}).stamps == (
        (("typed_value",), 9, 9),
    )


def test_s9_no_field_ids_below_the_group():
    # The PARQUET:field_id is the column's, on the group (D14); nothing in
    # the storage struct carries field metadata.
    def walk(kind):
        if pa.types.is_struct(kind):
            for field in kind:
                assert not field.metadata
                walk(field.type)
        elif pa.types.is_list(kind):
            assert not kind.value_field.metadata
            walk(kind.value_type)

    walk(variant.storage_type(OBJECT))
    walk(variant.encode_json(['{"s":"x","l":[1]}'], shredding=OBJECT).array.type)
    # Nor do the decimal4/decimal8 leaves: their types carry their scale.
    decimals = {
        "type": "object",
        "fields": [
            {"name": "d", "type": "decimal4", "precision": 9, "scale": 2},
            {
                "name": "l",
                "type": "array",
                "element": {"type": "decimal8", "precision": 18, "scale": 4},
            },
        ],
    }
    for form in ("write", "read"):
        walk(variant.storage_type(decimals, form=form))


# -- S5: a primitive has exactly one side (VS:76, 113) -------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (1, (None, 1)),
        (-(2**63), (None, -(2**63))),
        ("x", ("0578", None)),
        (VARIANT_NULL, (NULL, None)),
        (1.5, ("1c" + "000000000000f83f", None)),
        (True, ("04", None)),
        ([1], ("03010002" + "0c01", None)),
        (2**63, ("2800" + (2**63).to_bytes(16, "little").hex(), None)),
    ],
    ids=repr,
)
def test_s5_primitive_lands_on_exactly_one_side(source, value, expected):
    stored = encode([value], {"type": "int64"}, source).to_pylist()[0]
    assert side(stored) == expected


# -- S6: arrays (VS:117-147) -------------------------------------------------------

ARRAY = {"type": "array", "element": {"type": "string"}}


def test_s6_array_rows(source):
    # Inside an array, None is a Variant null.
    rows = [["comedy", "drama"], ["horror", None], [], "not an array", VARIANT_NULL]
    stored = encode(rows, ARRAY, source).to_pylist()
    # An array has value null, and each element exactly one side, a null
    # element being a value of Variant null: the table at VS:149-156.
    assert [side(e) for e in stored[0]["typed_value"]] == [
        (None, "comedy"),
        (None, "drama"),
    ]
    assert [side(e) for e in stored[1]["typed_value"]] == [
        (None, "horror"),
        (NULL, None),
    ]
    assert stored[0]["value"] is stored[1]["value"] is stored[2]["value"] is None
    assert stored[2]["typed_value"] == []
    assert side(stored[3]) == ("31" + b"not an array".hex(), None)
    assert side(stored[4]) == (NULL, None)


def test_s6_nested_arrays_and_objects_in_elements(source):
    decl = {
        "type": "array",
        "element": {
            "type": "object",
            "fields": [
                {"name": "type", "type": "string"},
                {"name": "ids", "type": "array", "element": {"type": "int64"}},
            ],
        },
    }
    rows = [
        [
            {"type": "click", "x": 1},
            "not an object",
            None,
            {},
            {"TYPE": "case", "ids": [1, None, "x"]},
        ],
        [[1, 2]],
    ]
    stored = encode(rows, decl, source).to_pylist()
    elements = stored[0]["typed_value"]
    assert [e["value"] is not None for e in elements] == [True, True, True, False, True]
    assert elements[3]["typed_value"]["type"] == {"value": None, "typed_value": None}
    ids = elements[4]["typed_value"]["ids"]["typed_value"]
    assert [side(e) for e in ids] == [(None, 1), (NULL, None), ("0578", None)]


# -- S4, S7, S8: objects and nulls (VS:158-223) -----------------------------------

EVENT = {
    "type": "object",
    "fields": [
        {"name": "event_type", "type": "string"},
        {"name": "event_ts", "type": "int64"},
    ],
}


def test_s7_the_spec_event_rows(source):
    # The table at VS:204-216, with event_ts an int64 (JSON has no
    # timestamps): fully shredded, partially, all fields missing, not an
    # object, a field missing, a field present and null, a field of another
    # type, empty, a Variant null, and SQL NULL ("missing").
    rows = [
        {"event_type": "noop", "event_ts": 1729794114937},
        {"event_type": "login", "event_ts": 1729794146402, "email": "user@example.com"},
        {"error_msg": "malformed: ..."},
        "malformed: not an object",
        {"event_ts": 1729794240241, "click": "_button"},
        {"event_type": None, "event_ts": 1729794954163},
        {"event_type": "noop", "event_ts": "2024-10-24"},
        {},
        VARIANT_NULL,
        None,
    ]
    array = encode(rows, EVENT, source)
    stored = array.to_pylist()
    residual = [
        None
        if r is None or r["value"] is None
        else variant.to_python(
            pa.StructArray.from_arrays(
                [pa.array([r["metadata"]]), pa.array([r["value"]])],
                fields=list(variant.storage_type()),
            )
        )[0]
        for r in stored
    ]
    typed = [None if r is None else r["typed_value"] for r in stored]
    assert residual[:3] == [
        None,
        {"email": "user@example.com"},
        {"error_msg": "malformed: ..."},
    ]
    assert residual[3] == "malformed: not an object" and typed[3] is None
    assert residual[4] == {"click": "_button"}
    assert residual[5] is None and residual[6] is None and residual[7] is None
    assert residual[8] is VARIANT_NULL and typed[8] is None
    assert stored[9] is None  # SQL NULL is a null group, never 00 (D4)
    event_type = [None if t is None else side(t["event_type"]) for t in typed]
    event_ts = [None if t is None else side(t["event_ts"]) for t in typed]
    assert event_type == [
        (None, "noop"),
        (None, "login"),
        (None, None),  # missing
        None,
        (None, None),
        (NULL, None),  # present and null
        (None, "noop"),
        (None, None),
        None,
        None,
    ]
    assert event_ts[6] == ("29" + b"2024-10-24".hex(), None)
    assert event_ts[7] == (None, None)


def test_s4_both_sides_null_only_in_field_groups(source):
    rows = [{"event_type": "x"}, {}, "s", VARIANT_NULL, [None], {"a": None}]
    array = encode(rows, EVENT, source)
    for stored in array.to_pylist():
        assert stored["value"] is not None or stored["typed_value"] is not None


def test_s7_residual_never_holds_a_declared_key(source):
    # Not even when the declared field's value is a mismatch: it goes to the
    # field's own value column (VS:171).
    rows = [{"event_type": 5, "event_ts": "x", "other": 1}]
    stored = encode(rows, EVENT, source).to_pylist()[0]
    names = variant.to_python(
        pa.StructArray.from_arrays(
            [pa.array([stored["metadata"]]), pa.array([stored["value"]])],
            fields=list(variant.storage_type()),
        )
    )[0]
    assert names == {"other": 1}
    assert side(stored["typed_value"]["event_type"]) == ("0c05", None)


TRINO_OBJECTS = {
    "type": "object",
    "fields": [
        {"name": "name", "type": "string"},
        {
            "name": "address",
            "type": "object",
            "fields": [
                {"name": "city", "type": "string"},
                {"name": "zip", "type": "int32"},
            ],
        },
        {"name": "payload", "type": "variant"},
        # Names of the layout's own columns are keys like any other.
        {"name": "value", "type": "int32"},
        {"name": "typed_value", "type": "variant"},
        {"name": "metadata", "type": "boolean"},
        {"name": "a.b", "type": "string"},
        {"name": "a", "type": "object", "fields": [{"name": "b", "type": "string"}]},
    ],
}


def test_s7_trino_writer_object_rows(source):
    # TestShreddedVariantWriter.testObjects.
    rows = [
        {
            "name": "alice",
            "address": {"city": "Paris", "zip": 75001, "country": "FR"},
            "payload": {"x": [1, "two"]},
            "extra": 1.5,
        },
        {"name": 42, "address": "not an object", "payload": None},
        {"address": {}, "other": {"name": "nested name is not shredded"}},
        {"value": 1, "typed_value": "t", "metadata": True},
        {"a.b": "dotted key", "a": {"b": "nested key"}},
        {"a.b": 1, "a": {"b": 2, "c": 3}},
    ]
    stored = encode(rows, TRINO_OBJECTS, source).to_pylist()
    assert [r["value"] is None for r in stored] == [
        False,
        True,
        False,
        True,
        True,
        True,
    ]
    t = [r["typed_value"] for r in stored]
    assert side(t[0]["address"]["typed_value"]["zip"]) == (None, 75001)
    assert t[0]["address"]["value"] is not None  # {"country": "FR"}
    assert side(t[1]["name"]) == ("0c2a", None)
    assert side(t[1]["payload"]) == (NULL, None)
    assert side(t[3]["value"]) == (None, 1)
    assert side(t[3]["metadata"]) == (None, True)
    assert t[3]["typed_value"]["value"] is not None
    assert side(t[4]["a.b"]) == (None, "dotted key")
    assert side(t[4]["a"]["typed_value"]["b"]) == (None, "nested key")
    assert side(t[5]["a.b"]) == ("0c01", None)
    assert t[5]["a"]["value"] is not None  # {"c": 3}


CASES = {
    "type": "object",
    "fields": [
        {"name": "$browser", "type": "string"},
        {
            "name": "props",
            "type": "object",
            "fields": [{"name": "plan", "type": "string"}],
        },
    ],
}


def test_s7_keys_that_differ_only_by_case_stay_in_the_residual(source):
    # TestShreddedVariantWriter.testKeysThatDifferOnlyByCase: only the exact
    # name is shredded (D6); DuckDB 1.5.5 drops the other spellings.
    rows = [
        {"$browser": "Chrome", "$os": "Mac", "props": {"Plan": "free", "plan": "pro"}},
        {"$browser": "Firefox", "props": {"plan": "team"}},
        {"$Browser": "Safari", "props": {"PLAN": "enterprise"}},
        {"$browser": "Edge", "$Browser": "Edge2", "$BROWSER": "Edge3"},
    ]
    stored = encode(rows, CASES, source).to_pylist()
    t = [r["typed_value"] for r in stored]
    assert side(t[0]["$browser"]) == (None, "Chrome")
    assert side(t[0]["props"]["typed_value"]["plan"]) == (None, "pro")
    assert t[0]["props"]["value"] is not None  # {"Plan": "free"}
    assert stored[1]["value"] is None
    assert side(t[2]["$browser"]) == (None, None)
    assert side(t[2]["props"]["typed_value"]["plan"]) == (None, None)
    assert side(t[3]["$browser"]) == (None, "Edge")
    assert stored[3]["value"] is not None  # {"$BROWSER", "$Browser"}


def test_s8_sql_null_is_a_null_group_and_variant_null_is_00(source):
    for decl in (None, EVENT, ARRAY, {"type": "int64"}):
        stored = encode([None, VARIANT_NULL], decl, source)
        assert stored.null_count == 1 and not stored.is_valid()[0].as_py()
        rows = stored.to_pylist()
        assert rows[0] is None
        assert rows[1]["value"] == b"\x00"
        assert rows[1].get("typed_value") is None
        # Under the null group, REQUIRED children hold an empty placeholder
        # rather than a null, so no non-nullable child holds a null at all.
        assert stored.field("metadata").to_pylist()[0] == b""
        if decl is None:
            assert stored.field("value").to_pylist()[0] == b""


def test_s8_a_not_null_target_has_no_null_groups():
    array = variant.encode_json(["1", "null"], shredding=EVENT, nullable=False).array
    assert array.null_count == 0
    assert array.to_pylist()[1]["value"] == b"\x00"


# -- D3: exact type classes (Trino's isShreddedPrimitive) ---------------------


@pytest.mark.parametrize(
    ("leaf", "typed", "untyped"),
    [
        # Integers of any width into an integer leaf that holds them.
        ("int8", [-128, 127, 0, -5], [128, -129, -(2**63), "1", True, 1.0]),
        ("int16", [-32768, 32767, 7], [32768, 1.5]),
        ("int32", [-(2**31), 2**31 - 1, 0], [2**31, -(2**31) - 1, True]),
        ("int64", [-(2**63), 2**63 - 1, -1], [2**63, 1.0, "2"]),
        # Never an integer into a double: D1 keeps JSON integers integers.
        ("double", [1.5, -0.0, 1e300], [1, 2**63, "1.5"]),
        ("boolean", [True, False], [1, 0, "true"]),
        ("string", ["", "a", "ü" * 100], [1, True, VARIANT_NULL]),
        # Nothing JSON or Python writes is a float or a nanosecond timestamp.
        ("float", [], [1.5, 1]),
        ("timestamp_ns", [], [1, "2024-01-01"]),
        ("timestamptz_ns", [], [1]),
        # JSON has no dates, times, uuids or binary; strings stay strings.
        ("date", [], ["2024-01-01", 19723]),
        ("time", [], ["12:00:00"]),
        ("uuid", [], ["0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0"]),
        ("binary", [], ["abc"]),
    ],
)
def test_d3_json_values(source, leaf, typed, untyped):
    rows = typed + untyped
    stored = encode(rows, {"type": leaf}, source).to_pylist()
    typed_rows = [r["typed_value"] is not None for r in stored]
    assert typed_rows == [True] * len(typed) + [False] * len(untyped)
    for r in stored:
        assert (r["value"] is None) != (r["typed_value"] is None)


@pytest.mark.parametrize(
    ("decl", "typed", "untyped"),
    [
        # A decimal of any width into a leaf of its scale whose precision
        # holds it; never across scales, never an int within int64.
        # Both bounds: a precision of p holds 10**p - 1 unscaled, either
        # sign, and not 10**p.
        (
            {"type": "decimal4", "precision": 5, "scale": 2},
            [Decimal("123.45"), Decimal("-999.99"), Decimal("999.99"), Decimal("0.00")],
            [
                Decimal("1000.00"),
                Decimal("-1000.00"),
                Decimal("1.230"),
                Decimal("1.2"),
                1,
                Decimal(12),
            ],
        ),
        (
            {"type": "decimal4", "precision": 9, "scale": 0},
            [Decimal(999_999_999), Decimal(-999_999_999)],
            [Decimal(1_000_000_000), Decimal(-1_000_000_000)],
        ),
        (
            {"type": "decimal8", "precision": 18, "scale": 4},
            [
                Decimal("99999999999999.9999"),
                Decimal("-99999999999999.9999"),
                Decimal("0.0001"),
            ],
            [
                Decimal("100000000000000.0000"),
                Decimal("-100000000000000.0000"),
                Decimal("1.000"),
                5,
            ],
        ),
        (
            {"type": "decimal8", "precision": 3, "scale": 0},
            [Decimal(999), Decimal(-999), Decimal("1E+2")],
            [Decimal(1000), 999],
        ),
        (
            {"type": "decimal16", "precision": 38, "scale": 10},
            [Decimal("1.0000000000"), Decimal("-" + "9" * 28 + "." + "9" * 10)],
            [Decimal("1.000000000"), Decimal(1)],
        ),
        # Integers beyond int64 are decimal16 with scale 0 (D1).
        (
            {"type": "decimal16", "precision": 20, "scale": 0},
            [
                2**63,
                -(2**63) - 1,
                10**20 - 1,
                -(10**20 - 1),
                Decimal(12345678901234567890),
            ],
            [10**20, -(10**20), 2**63 - 1, -(2**63), 5],
        ),
        # The ends of int64 are int values, never decimal16 (D1, D3).
        (
            {"type": "decimal16", "precision": 38, "scale": 0},
            [2**63, -(2**63) - 1],
            [-(2**63), 2**63 - 1],
        ),
        ({"type": "decimal16", "precision": 38, "scale": 2}, [], [2**63]),
        ({"type": "int64"}, [], [Decimal(1), Decimal(12345678901234567890)]),
        ({"type": "double"}, [float("nan"), float("inf")], [Decimal("1.5")]),
        ({"type": "date"}, [date(1969, 12, 31)], [datetime(2024, 1, 1)]),
        ({"type": "time"}, [time(23, 59, 59, 999999)], [datetime(2024, 1, 1)]),
        (
            {"type": "timestamp"},
            [datetime(2024, 1, 1, 12)],
            [datetime(2024, 1, 1, tzinfo=UTC), date(2024, 1, 1)],
        ),
        (
            {"type": "timestamptz"},
            [datetime(2024, 1, 1, tzinfo=UTC), datetime.min.replace(tzinfo=UTC)],
            [datetime(2024, 1, 1)],
        ),
        ({"type": "uuid"}, [uuid.UUID(int=0)], [str(uuid.UUID(int=0))]),
        ({"type": "binary"}, [b"", b"\x00\xff", bytearray(b"a")], ["a"]),
        ({"type": "timestamptz_ns"}, [], [datetime(2024, 1, 1, tzinfo=UTC)]),
    ],
    ids=lambda x: str(x)[:40],
)
def test_d3_python_values(decl, typed, untyped):
    stored = encode(typed + untyped, decl, "python").to_pylist()
    assert [r["typed_value"] is not None for r in stored] == [True] * len(typed) + [
        False
    ] * len(untyped)


@pytest.mark.parametrize(
    ("number", "typed"),
    [
        (10**20 - 1, True),
        (-(10**20 - 1), True),
        (10**20, False),
        (-(10**20), False),
        (-(2**63), False),
        (-(2**63) - 1, True),
    ],
)
def test_d3_json_integers_at_a_decimal16_leafs_bounds(number, typed):
    decl = {"type": "decimal16", "precision": 20, "scale": 0}
    (stored,) = encode([number], decl, "json").to_pylist()
    assert (stored["typed_value"] is not None) == typed
    assert (stored["value"] is None) == typed


def test_d3_python_datetimes_fit_trinos_timestamptz_bound():
    # Trino keeps a timestamptz's epoch milliseconds in 52 bits and leaves a
    # value beyond them in value; the matcher has no such guard because no
    # Python datetime reaches the bound.
    bound = 1 << 51
    for moment in (datetime.min.replace(tzinfo=UTC), datetime.max.replace(tzinfo=UTC)):
        millis = (moment - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(
            milliseconds=1
        )
        assert -bound <= millis < bound


def test_d3_typed_values_read_back_at_the_declared_width(source):
    stored = encode([1, -5, 100], {"type": "int8"}, source)
    assert stored.field("typed_value").type == pa.int8()
    assert stored.field("typed_value").to_pylist() == [1, -5, 100]
