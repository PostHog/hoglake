"""Nested and json column coercion (millrace/coerce.py) and the
batch build that turns the coerced values into Arrow (flush.py).

Pinned here:

- every leaf type at depth 2 and 3 inside a struct, a list and a map
  coerces, builds, writes parquet with a field id on every level, and
  reads back as the value the payload meant (the uuid/timestamptz/time/
  timestamp_s leaf conversions live in ONE place, flush._build_type, and
  hold at any depth);
- a missing child is null; a null into a non-nullable child is a
  DecodeError naming the dotted path; a non-object/non-array container
  value is a DecodeError;
- map keys: string keys as-is, a non-string key child coerces the key
  string (as-is, then as a strict JSON number/boolean); keys colliding
  after coercion are refused;
- the depth limit is pyhoglake's MAX_COLUMN_NESTING_DEPTH;
- json canonical text (unicode, nesting, scalars, big ints, floats,
  non-finite refused); JSON null is SQL NULL;
- a decimal is accepted exactly when its column holds it (no rounding,
  any spelling), and every accepted decimal builds;
- a ``variant`` below the top level is a startup refusal, and a
  top-level one is never coerced: its source is JSON text
  (:func:`variant_text`'s semantics table).
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, date, datetime
from datetime import time as dtime
from decimal import Decimal
from fractions import Fraction
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from hypothesis import given
from hypothesis import strategies as st
from pyhoglake import Column
from pyhoglake.parquet_schema import prepared_schema_matches
from pyhoglake.types import MAX_COLUMN_NESTING_DEPTH, PARQUET_FIELD_ID_KEY

from millrace import flush
from millrace.coerce import (
    REASON_UNCASTABLE_COLUMN,
    VARIANT_INVALID_UNICODE,
    VARIANT_NOT_TEXT,
    DecodeError,
    UnsupportedColumnError,
    canonical_json,
    coercer_for,
    variant_text,
)


def col(wire: dict[str, Any]) -> Column:
    return Column.from_wire(wire)


def columns(wires: list[dict[str, Any]]) -> tuple[Column, ...]:
    return tuple(Column.from_wire(w) for w in wires)


#: (catalog type, type_params, JSON input, the value Arrow reads back).
LEAVES: list[tuple[str, dict[str, Any] | None, Any, Any]] = [
    ("boolean", None, True, True),
    ("int8", None, -128, -128),
    ("int16", None, 32767, 32767),
    ("int", None, -(2**31), -(2**31)),
    ("long", None, 2**63 - 1, 2**63 - 1),
    ("uint8", None, 255, 255),
    ("uint16", None, 65535, 65535),
    ("uint32", None, 2**32 - 1, 2**32 - 1),
    ("uint64", None, 2**64 - 1, 2**64 - 1),
    ("float", None, 0.5, 0.5),
    ("double", None, 2.5e-7, 2.5e-7),
    ("string", None, "héllo", "héllo"),
    ("binary", None, "AAEC/w==", b"\x00\x01\x02\xff"),
    (
        "uuid",
        None,
        "12345678-1234-5678-1234-567812345678",
        uuid.UUID("12345678-1234-5678-1234-567812345678"),
    ),
    ("decimal", {"precision": 10, "scale": 2}, "1.5", Decimal("1.50")),
    ("date", None, "2026-01-31", date(2026, 1, 31)),
    ("time", None, "12:34:56.25", dtime(12, 34, 56, 250000)),
    ("timestamp_s", None, 1_700_000_000, datetime.fromisoformat("2023-11-14T22:13:20")),
    (
        "timestamp_ms",
        None,
        1_700_000_000_123,
        datetime.fromisoformat("2023-11-14T22:13:20.123"),
    ),
    (
        "timestamp",
        None,
        "2023-11-14T22:13:20.123456",
        datetime.fromisoformat("2023-11-14T22:13:20.123456"),
    ),
    (
        "timestamp_ns",
        None,
        1_700_000_000_123_456_000,
        datetime.fromisoformat("2023-11-14T22:13:20.123456"),
    ),
    (
        "timestamptz",
        None,
        "2026-10-12T03:04:05.5+02:00",
        datetime(2026, 10, 12, 1, 4, 5, 500000, tzinfo=UTC),
    ),
    ("json", None, {"b": [1, "x"], "a": None}, '{"b":[1,"x"],"a":null}'),
]
LEAF_IDS = [leaf[0] for leaf in LEAVES]


def _leaf(name: str, fid: int, t: str, params: dict[str, Any] | None) -> dict[str, Any]:
    wire: dict[str, Any] = {"name": name, "type": t, "field_id": fid, "ordinal": 0}
    if params is not None:
        wire["type_params"] = params
    return wire


def _key(fid: int) -> dict[str, Any]:
    return {
        "name": "key",
        "type": "string",
        "field_id": fid,
        "ordinal": 0,
        "nullable": False,
    }


def _shape(
    shape: str, t: str, params: dict[str, Any] | None, raw: Any, want: Any
) -> tuple[dict[str, Any], Any, Any]:
    """(the ``c`` column's wire, its JSON value, the value read back) for
    one container shape around one leaf."""
    if shape == "struct":
        wire = {
            "name": "c",
            "type": "struct",
            "field_id": 1,
            "ordinal": 0,
            "children": [_leaf("x", 2, t, params)],
        }
        return wire, {"x": raw}, {"x": want}
    if shape == "list":
        wire = {
            "name": "c",
            "type": "list",
            "field_id": 1,
            "ordinal": 0,
            "children": [_leaf("element", 2, t, params)],
        }
        return wire, [raw, None], [want, None]
    if shape == "map":
        value = _leaf("value", 3, t, params)
        value["ordinal"] = 1
        wire = {
            "name": "c",
            "type": "map",
            "field_id": 1,
            "ordinal": 0,
            "children": [_key(2), value],
        }
        return wire, {"k": raw, "n": None}, [("k", want), ("n", None)]
    if shape == "struct.struct":
        wire = {
            "name": "c",
            "type": "struct",
            "field_id": 1,
            "ordinal": 0,
            "children": [
                {
                    "name": "s",
                    "type": "struct",
                    "field_id": 2,
                    "ordinal": 0,
                    "children": [_leaf("x", 3, t, params)],
                }
            ],
        }
        return wire, {"s": {"x": raw}}, {"s": {"x": want}}
    if shape == "list.struct":
        wire = {
            "name": "c",
            "type": "list",
            "field_id": 1,
            "ordinal": 0,
            "children": [
                {
                    "name": "element",
                    "type": "struct",
                    "field_id": 2,
                    "ordinal": 0,
                    "children": [_leaf("x", 3, t, params)],
                }
            ],
        }
        return wire, [{"x": raw}, {}], [{"x": want}, {"x": None}]
    if shape == "map.list":
        wire = {
            "name": "c",
            "type": "map",
            "field_id": 1,
            "ordinal": 0,
            "children": [
                _key(2),
                {
                    "name": "value",
                    "type": "list",
                    "field_id": 3,
                    "ordinal": 1,
                    "children": [_leaf("element", 4, t, params)],
                },
            ],
        }
        return wire, {"k": [raw]}, [("k", [want])]
    raise AssertionError(shape)


def _roundtrip(cols: tuple[Column, ...], docs: list[dict[str, Any]]) -> pa.Table:
    """Decode → build → write parquet → read back, checking the file's
    schema (field ids at every level) is the encoding schema."""
    decoder = flush.JsonObjectDecoder(cols)
    records = [decoder.decode_record(json.dumps(d).encode()) for d in docs]
    _, encoded = flush._build_batch(records, cols)
    sink = pa.BufferOutputStream()
    pq.write_table(encoded, sink)
    with pq.ParquetFile(pa.BufferReader(sink.getvalue())) as parquet:
        assert prepared_schema_matches(parquet.schema_arrow, encoded.schema)
        return parquet.read()


def _field_ids(t: pa.DataType) -> list[bytes | None]:
    """Every nested field's id, depth first (containers included)."""
    out: list[bytes | None] = []
    if pa.types.is_struct(t):
        children = [t.field(i) for i in range(t.num_fields)]
    elif pa.types.is_map(t):
        children = [t.key_field, t.item_field]
    elif pa.types.is_list(t):
        children = [t.value_field]
    else:
        children = []
    for child in children:
        out.append((child.metadata or {}).get(PARQUET_FIELD_ID_KEY))
        out.extend(_field_ids(child.type))
    return out


@pytest.mark.parametrize(
    "shape", ["struct", "list", "map", "struct.struct", "list.struct", "map.list"]
)
@pytest.mark.parametrize(("t", "params", "raw", "want"), LEAVES, ids=LEAF_IDS)
def test_every_leaf_type_inside_every_container_round_trips(
    shape: str, t: str, params: dict[str, Any] | None, raw: Any, want: Any
) -> None:
    wire, value, expected = _shape(shape, t, params, raw, want)
    cols = columns([wire])
    table = _roundtrip(cols, [{"c": value}, {"c": None}, {}])
    assert table.column("c").to_pylist() == [expected, None, None]
    ids = _field_ids(table.schema.field("c").type)
    assert ids and all(i is not None for i in ids), ids


def test_a_missing_child_is_null_and_unknown_keys_are_ignored() -> None:
    wire = {
        "name": "p",
        "type": "struct",
        "field_id": 1,
        "ordinal": 0,
        "children": [
            {"name": "a", "type": "long", "field_id": 2, "ordinal": 0},
            {"name": "b", "type": "string", "field_id": 3, "ordinal": 1},
        ],
    }
    coerce = coercer_for(col(wire))
    assert coerce({"a": 1, "zzz": 2}) == {"a": 1, "b": None}
    assert coerce({}) == {"a": None, "b": None}


NOT_NULL_CHILDREN = {
    "name": "p",
    "type": "struct",
    "field_id": 1,
    "ordinal": 0,
    "children": [
        {
            "name": "inner",
            "type": "struct",
            "field_id": 2,
            "ordinal": 0,
            "children": [
                {
                    "name": "id",
                    "type": "long",
                    "field_id": 3,
                    "ordinal": 0,
                    "nullable": False,
                },
                {
                    "name": "tags",
                    "type": "list",
                    "field_id": 4,
                    "ordinal": 1,
                    "children": [
                        {
                            "name": "element",
                            "type": "string",
                            "field_id": 5,
                            "ordinal": 0,
                            "nullable": False,
                        }
                    ],
                },
                {
                    "name": "m",
                    "type": "map",
                    "field_id": 6,
                    "ordinal": 2,
                    "children": [
                        _key(7),
                        {
                            "name": "value",
                            "type": "long",
                            "field_id": 8,
                            "ordinal": 1,
                            "nullable": False,
                        },
                    ],
                },
            ],
        }
    ],
}


@pytest.mark.parametrize(
    ("value", "path"),
    [
        ({"inner": {}}, "p.inner.id"),
        ({"inner": {"id": None}}, "p.inner.id"),
        ({"inner": {"id": 1, "tags": ["a", None]}}, "p.inner.tags.element"),
        ({"inner": {"id": 1, "m": {"k": None}}}, "p.inner.m.value"),
    ],
)
def test_a_null_into_a_non_nullable_child_names_its_path(
    value: dict[str, Any], path: str
) -> None:
    with pytest.raises(DecodeError, match=f"^{path} is not nullable") as excinfo:
        coercer_for(col(NOT_NULL_CHILDREN))(value)
    assert excinfo.value.reason == REASON_UNCASTABLE_COLUMN


def test_a_null_parent_spares_its_non_nullable_children() -> None:
    """A null struct is one null value: its non-nullable children are not
    asked for (the catalog's NOT NULL binds only where the parent exists)."""
    coerce = coercer_for(col(NOT_NULL_CHILDREN))
    assert coerce({"inner": None}) == {"inner": None}
    table = _roundtrip(columns([NOT_NULL_CHILDREN]), [{"p": {"inner": None}}, {}])
    assert table.column("p").to_pylist() == [{"inner": None}, None]


@pytest.mark.parametrize(
    ("wire_type", "children", "value", "match"),
    [
        (
            "struct",
            [{"name": "a", "type": "long", "field_id": 2, "ordinal": 0}],
            [1],
            "expected a JSON object for a struct, got list",
        ),
        (
            "list",
            [{"name": "element", "type": "long", "field_id": 2, "ordinal": 0}],
            {"a": 1},
            "expected a JSON array for a list, got dict",
        ),
        (
            "map",
            [
                _key(2),
                {"name": "value", "type": "long", "field_id": 3, "ordinal": 1},
            ],
            [["a", 1]],
            "expected a JSON object for a map, got list",
        ),
        (
            "list",
            [{"name": "element", "type": "long", "field_id": 2, "ordinal": 0}],
            [1, "x"],
            "c.element: expected a JSON integer",
        ),
    ],
)
def test_a_container_value_of_the_wrong_shape_is_a_decode_error(
    wire_type: str, children: list[dict[str, Any]], value: Any, match: str
) -> None:
    wire = {
        "name": "c",
        "type": wire_type,
        "field_id": 1,
        "ordinal": 0,
        "children": children,
    }
    with pytest.raises(DecodeError, match=match) as excinfo:
        coercer_for(col(wire))(value)
    assert excinfo.value.reason == REASON_UNCASTABLE_COLUMN


def _map(key_type: str, key_params: dict[str, Any] | None = None) -> Column:
    key: dict[str, Any] = {
        "name": "key",
        "type": key_type,
        "field_id": 2,
        "ordinal": 0,
        "nullable": False,
    }
    if key_params is not None:
        key["type_params"] = key_params
    return col(
        {
            "name": "m",
            "type": "map",
            "field_id": 1,
            "ordinal": 0,
            "children": [
                key,
                {"name": "value", "type": "string", "field_id": 3, "ordinal": 1},
            ],
        }
    )


@pytest.mark.parametrize(
    ("key_type", "params", "text", "key"),
    [
        ("string", None, "42", "42"),
        ("long", None, "42", 42),
        ("int8", None, "-7", -7),
        ("boolean", None, "true", True),
        ("double", None, "1.5", 1.5),
        ("date", None, "2026-01-31", date(2026, 1, 31)),
        ("date", None, "3", date(1970, 1, 4)),
        ("timestamp", None, "1700000000000000", 1_700_000_000_000_000),
        ("timestamp", None, "1970-01-01T00:00:01", 1_000_000),
        ("decimal", {"precision": 5, "scale": 1}, "1.5", Decimal("1.5")),
        (
            "uuid",
            None,
            "12345678-1234-5678-1234-567812345678",
            uuid.UUID("12345678-1234-5678-1234-567812345678").bytes,
        ),
    ],
)
def test_a_map_key_string_coerces_through_the_key_child(
    key_type: str, params: dict[str, Any] | None, text: str, key: Any
) -> None:
    assert coercer_for(_map(key_type, params))({text: "v"}) == [(key, "v")]


def test_a_non_string_map_key_reads_back_typed() -> None:
    wire = {
        "name": "m",
        "type": "map",
        "field_id": 1,
        "ordinal": 0,
        "children": [
            {
                "name": "key",
                "type": "long",
                "field_id": 2,
                "ordinal": 0,
                "nullable": False,
            },
            {"name": "value", "type": "string", "field_id": 3, "ordinal": 1},
        ],
    }
    table = _roundtrip(columns([wire]), [{"m": {"1": "a", "-2": None}}])
    assert table.column("m").to_pylist() == [[(1, "a"), (-2, None)]]


@pytest.mark.parametrize(
    ("key_type", "text"),
    [("long", "x"), ("long", "1.5"), ("boolean", "1"), ("long", "NaN"), ("uuid", "x")],
)
def test_a_map_key_the_key_child_refuses_names_the_key_path(
    key_type: str, text: str
) -> None:
    with pytest.raises(DecodeError, match=r"^m\.key: ") as excinfo:
        coercer_for(_map(key_type))({text: "v"})
    assert excinfo.value.reason == REASON_UNCASTABLE_COLUMN


def test_map_keys_that_collide_after_coercion_are_refused() -> None:
    with pytest.raises(DecodeError, match="coerce to one map key"):
        coercer_for(_map("double"))({"1": "a", "1.0": "b"})


def test_float32_map_keys_collide_as_stored() -> None:
    """A catalog ``float`` key narrows to float32 in Arrow: two doubles
    that differ only below float32 precision are one stored key, and are
    refused as such (the double comparison alone would let both into the
    file)."""
    coerce = coercer_for(_map("float"))
    with pytest.raises(DecodeError, match="coerce to one map key") as excinfo:
        coerce({"0.1": "a", "0.10000000001": "b"})
    assert excinfo.value.path == "m.key"
    assert coerce({"0.1": "a", "0.2": "b"}) == [(0.1, "a"), (0.2, "b")]
    # a double key keeps double precision
    assert len(coercer_for(_map("double"))({"0.1": "a", "0.10000000001": "b"})) == 2


def test_a_temporal_key_reads_its_string_form_first() -> None:
    """``"20260101"`` is ISO-basic text: a date key reads it as a date,
    never as epoch day 20,260,101."""
    assert coercer_for(_map("date"))({"20260101": "v"}) == [(date(2026, 1, 1), "v")]


@pytest.mark.parametrize(
    ("value", "path"),
    [
        ({"inner": {}}, "p.inner.id"),
        ({"inner": {"id": "x"}}, "p.inner.id"),
        ({"inner": {"id": 1, "tags": [1]}}, "p.inner.tags.element"),
        ({"inner": {"id": 1, "m": {"k": "x"}}}, "p.inner.m.value"),
        ({"inner": []}, "p.inner"),
    ],
)
def test_a_nested_decode_error_carries_its_path(value: Any, path: str) -> None:
    with pytest.raises(DecodeError) as excinfo:
        coercer_for(col(NOT_NULL_CHILDREN))(value)
    assert excinfo.value.path == path


def _nest(depth: int) -> dict[str, Any]:
    """A column of ``depth`` levels: structs down to one long leaf."""
    wire: dict[str, Any] = {
        "name": "x",
        "type": "long",
        "field_id": depth,
        "ordinal": 0,
    }
    for level in range(depth - 1, 0, -1):
        wire = {
            "name": f"s{level}",
            "type": "struct",
            "field_id": level,
            "ordinal": 0,
            "children": [wire],
        }
    return wire


def test_nesting_is_bounded_by_pyhoglakes_depth_cap() -> None:
    assert MAX_COLUMN_NESTING_DEPTH == 8
    deepest = columns([_nest(MAX_COLUMN_NESTING_DEPTH)])
    value: Any = {"x": 5}
    for level in range(MAX_COLUMN_NESTING_DEPTH - 1, 1, -1):
        value = {f"s{level}": value}
    table = _roundtrip(deepest, [{"s1": value}])
    assert table.column("s1").to_pylist() == [value]
    with pytest.raises(UnsupportedColumnError, match="nests deeper than the maximum 8"):
        coercer_for(col(_nest(MAX_COLUMN_NESTING_DEPTH + 1)))


# -- json ----------------------------------------------------------------------

JSON_COL = {"name": "j", "type": "json", "field_id": 1, "ordinal": 0}


@pytest.mark.parametrize(
    ("value", "text"),
    [
        ({"é": "ü", "z": [1, {"a": None}]}, '{"é":"ü","z":[1,{"a":null}]}'),
        ({"b": 1, "a": 2}, '{"b":1,"a":2}'),  # payload key order kept
        ("x", '"x"'),
        (True, "true"),
        (0, "0"),
        (10**30, "1000000000000000000000000000000"),
        (-(10**40), "-" + "1" + "0" * 40),
        (0.1, "0.1"),
        (1e16, "1e+16"),
        (-0.0, "-0.0"),
        ([], "[]"),
    ],
)
def test_json_is_stored_as_canonical_text(value: Any, text: str) -> None:
    assert coercer_for(col(JSON_COL))(value) == text
    assert canonical_json(value) == text


def test_json_canonical_text_round_trips_from_the_payload() -> None:
    raw = '{"j": {"n": 12345678901234567890123, "f": 1.0, "s": "\\u00e9\\n"}}'
    record = flush.JsonObjectDecoder(columns([JSON_COL])).decode_record(raw.encode())
    assert record.values["j"] == '{"n":12345678901234567890123,"f":1.0,"s":"é\\n"}'
    table = _roundtrip(columns([JSON_COL]), [json.loads(raw), {"j": None}])
    assert table.column("j").to_pylist() == [record.values["j"], None]
    assert table.schema.field("j").type == pa.json_()


@pytest.mark.parametrize("value", [float("nan"), float("inf"), "\ud800", ["\udfff"]])
def test_json_without_valid_text_is_uncastable(value: Any) -> None:
    with pytest.raises(DecodeError) as excinfo:
        coercer_for(col(JSON_COL))(value)
    assert excinfo.value.reason == REASON_UNCASTABLE_COLUMN


def test_a_magnitude_overflow_literal_in_json_is_dropped() -> None:
    decoder = flush.JsonObjectDecoder(columns([JSON_COL]))
    with pytest.raises(DecodeError) as excinfo:
        decoder.decode_record(b'{"j": {"x": 1e400}}')
    assert excinfo.value.reason == REASON_UNCASTABLE_COLUMN


def test_json_null_is_sql_null_and_refused_when_not_nullable() -> None:
    nullable = flush.JsonObjectDecoder(columns([JSON_COL]))
    assert nullable.decode_record(b'{"j": null}').values["j"] is None
    strict = flush.JsonObjectDecoder(columns([{**JSON_COL, "nullable": False}]))
    with pytest.raises(DecodeError, match="not nullable"):
        strict.decode_record(b'{"j": null}')
    # A JSON null INSIDE a json value is part of its text.
    assert strict.decode_record(b'{"j": [null]}').values["j"] == "[null]"


# -- decimal -------------------------------------------------------------------


def _decimal_col(precision: int, scale: int) -> Column:
    return col(
        {
            "name": "d",
            "type": "decimal",
            "field_id": 1,
            "ordinal": 0,
            "type_params": {"precision": precision, "scale": scale},
        }
    )


@pytest.mark.parametrize(
    ("precision", "scale", "raw", "want"),
    [
        # The largest magnitude decimal(10, 2) holds: 10**8 - 10**-2.
        (10, 2, "99999999.99", "99999999.99"),
        (10, 2, "-99999999.99", "-99999999.99"),
        (10, 2, 99999999, "99999999"),
        (10, 2, "9.999999999E+7", "99999999.99"),
        (1, 1, "0.9", "0.9"),
        (38, 0, "9" * 38, "9" * 38),
        (38, 38, "0." + "9" * 38, "0." + "9" * 38),
        # Zeros the scale does not need are not digits the column holds,
        # however the payload spells them.
        (10, 2, "1.230", "1.23"),
        (10, 2, "1." + "0" * 70, "1"),
        (10, 2, "0E+50", "0"),
    ],
)
def test_a_decimal_that_fits_builds_exactly(
    precision: int, scale: int, raw: Any, want: str
) -> None:
    cols = (_decimal_col(precision, scale),)
    value = coercer_for(cols[0])(raw)
    assert value == Decimal(want)
    assert value.as_tuple().exponent == -scale
    table = _roundtrip(cols, [{"d": raw}])
    assert table.column("d").to_pylist() == [Decimal(want)]


@pytest.mark.parametrize(
    ("precision", "scale", "raw", "match"),
    [
        # One unit of the scale past the boundary, in every spelling. The
        # integer-valued and exponent forms carry fewer significant digits
        # than the column needs at its scale.
        (10, 2, "100000000.00", r"does not fit decimal\(10, 2\)"),
        (10, 2, 100000000, r"does not fit decimal\(10, 2\)"),
        (10, 2, -100000000, r"does not fit decimal\(10, 2\)"),
        (10, 2, "1E+8", r"does not fit decimal\(10, 2\)"),
        (10, 2, "1.0E+8", r"does not fit decimal\(10, 2\)"),
        (10, 2, 123456789, r"does not fit decimal\(10, 2\)"),
        (1, 1, "1", r"does not fit decimal\(1, 1\)"),
        (1, 1, 1, r"does not fit decimal\(1, 1\)"),
        (38, 0, "1E+38", r"does not fit decimal\(38, 0\)"),
        (38, 38, 1, r"does not fit decimal\(38, 38\)"),
        (10, 2, "1E+999999", r"does not fit decimal\(10, 2\)"),
        (10, 2, "99999999.995", r"does not fit decimal\(10, 2\)"),
        # A fractional digit past the scale is refused, never rounded,
        # however far past the scale it sits.
        (10, 2, "1.234", "more than 2 fractional digits; refusing to round"),
        (10, 0, "1." + "0" * 70 + "1", "more than 0 fractional digits"),
        (10, 2, "1E-1000000", "more than 2 fractional digits"),
        (10, 2, 1.5, "expected a JSON integer or decimal string, got float"),
        (10, 2, "abc", "invalid decimal 'abc'"),
        (10, 2, "-Infinity", "non-finite decimal '-Infinity'"),
    ],
)
def test_a_decimal_that_does_not_fit_is_uncastable(
    precision: int, scale: int, raw: Any, match: str
) -> None:
    with pytest.raises(DecodeError, match=match) as excinfo:
        coercer_for(_decimal_col(precision, scale))(raw)
    assert excinfo.value.reason == REASON_UNCASTABLE_COLUMN


@given(
    coefficient=st.integers(-(10**40), 10**40),
    exponent=st.integers(-45, 45),
    precision=st.integers(1, 38),
    data=st.data(),
)
def test_a_decimal_is_accepted_exactly_when_the_column_holds_it(
    coefficient: int, exponent: int, precision: int, data: st.DataObject
) -> None:
    """The oracle is exact rational arithmetic: the value fits when it
    times 10**scale is an integer below 10**precision in magnitude."""
    scale = data.draw(st.integers(0, precision))
    raw = f"{coefficient}E{exponent}"
    units = Fraction(Decimal(raw)) * 10**scale
    fits = units.denominator == 1 and abs(units) < 10**precision
    cols = (_decimal_col(precision, scale),)
    if not fits:
        with pytest.raises(DecodeError) as excinfo:
            coercer_for(cols[0])(raw)
        assert excinfo.value.reason == REASON_UNCASTABLE_COLUMN
        return
    table = _roundtrip(cols, [{"d": raw}])
    assert table.column("d").to_pylist() == [Decimal(raw)]


# -- variant -------------------------------------------------------------------


@pytest.mark.parametrize(
    ("wire", "message"),
    [
        (
            {"name": "v", "type": "variant", "field_id": 1, "ordinal": 0},
            (
                "column 'v' is a variant, which is filled from its "
                "MILLRACE_VARIANT_SOURCES payload key, not coerced"
            ),
        ),
        (
            {
                "name": "s",
                "type": "struct",
                "field_id": 1,
                "ordinal": 0,
                "children": [
                    {"name": "v", "type": "variant", "field_id": 2, "ordinal": 0}
                ],
            },
            (
                "column 's.v' is a variant below the top level: only a top-level "
                "variant column is written"
            ),
        ),
    ],
)
def test_a_variant_is_never_coerced(wire: dict[str, Any], message: str) -> None:
    """A nested variant has no writer (a startup refusal through the
    decoder); a top-level one is the decoder's, filled from its payload
    key's JSON text, so reaching the coercer is a wiring mistake."""
    with pytest.raises(UnsupportedColumnError) as e:
        coercer_for(col(wire))
    assert message in str(e.value)


@pytest.mark.parametrize(
    ("value", "want"),
    [
        # Absent or JSON null, and the empty string: SQL NULL, not invalid.
        (None, (None, None)),
        ("", (None, None)),
        # Text, unparsed: the encoder reads it (and "null" is a Variant null).
        ("null", ("null", None)),
        ('{"a":1,"a":2}', ('{"a":1,"a":2}', None)),
        ("not json", ("not json", None)),
        # An inner surrogate escape is ASCII text here; the encoder nulls it.
        ('{"k":"\\ud800"}', ('{"k":"\\ud800"}', None)),
        # Not text: SQL NULL, invalid, never encoded from the parsed value.
        (5, (None, VARIANT_NOT_TEXT)),
        (0, (None, VARIANT_NOT_TEXT)),
        (False, (None, VARIANT_NOT_TEXT)),
        ({"a": 1}, (None, VARIANT_NOT_TEXT)),
        ([], (None, VARIANT_NOT_TEXT)),
        # A lone surrogate in the OUTER payload cannot reach Arrow.
        ("\ud800", (None, VARIANT_INVALID_UNICODE)),
    ],
)
def test_variant_text_semantics(value: Any, want: tuple[Any, Any]) -> None:
    assert variant_text(value) == want


def test_the_variant_reasons_are_labels_beside_the_encoders() -> None:
    """Both share millrace_variant_invalid_total{reason} with pyhoglake's
    own reasons, read from its Literal: invalid_unicode must be the
    encoder's name for it (one label, not two), not_text none of them."""
    from typing import get_args

    from pyhoglake.variant import InvalidReason

    assert (VARIANT_NOT_TEXT, VARIANT_INVALID_UNICODE) == (
        "not_text",
        "invalid_unicode",
    )
    assert VARIANT_INVALID_UNICODE in get_args(InvalidReason)
    assert VARIANT_NOT_TEXT not in get_args(InvalidReason)


# -- timestamps outside years 1-9999 are clamped, never dropped ----------------

_MIN_US = -62_135_596_800_000_000  # 0001-01-01T00:00:00
_MAX_US = 253_402_300_799_999_999  # 9999-12-31T23:59:59.999999
_INT64 = (-(1 << 63), (1 << 63) - 1)

#: Per timestamp type: (lo, hi) in the column's unit.
_BOUNDS: dict[str, tuple[int, int]] = {
    "timestamp_s": (_MIN_US // 1_000_000, _MAX_US // 1_000_000),
    "timestamp_ms": (_MIN_US // 1_000, _MAX_US // 1_000),
    "timestamp": (_MIN_US, _MAX_US),
    "timestamptz": (_MIN_US, _MAX_US),
    "timestamp_ns": _INT64,  # int64 nanos span only 1677-2262
}


def _ts_coercer(t: str, notes: list[str]) -> Any:
    return coercer_for(
        col({"name": "ts", "type": t, "field_id": 1, "ordinal": 0}),
        on_clamp=notes.append,
    )


def test_timestamp_bounds_are_years_1_to_9999_narrowed_to_int64() -> None:
    from millrace.coerce import timestamp_bounds

    for t, bounds in _BOUNDS.items():
        assert timestamp_bounds(t) == bounds


@pytest.mark.parametrize("t", sorted(_BOUNDS))
def test_an_out_of_range_epoch_integer_clamps_and_is_reported(t: str) -> None:
    notes: list[str] = []
    coerce = _ts_coercer(t, notes)
    lo, hi = _BOUNDS[t]
    assert coerce(hi + 1) == hi
    assert coerce(lo - 1) == lo
    assert coerce(10**30) == hi
    assert coerce(-(10**30)) == lo
    assert notes == ["ts"] * 4
    # In range: untouched, not reported.
    assert coerce(hi) == hi
    assert coerce(lo) == lo
    assert coerce(0) == 0
    assert notes == ["ts"] * 4


@pytest.mark.parametrize("t", sorted(_BOUNDS))
@pytest.mark.parametrize(
    ("text", "side"),
    [
        ("10000-01-01T00:00:00", 1),
        ("+10000-01-01T00:00:00", 1),
        ("99999-06-15 12:00:00", 1),
        ("0000-01-01T00:00:00", -1),
        ("-0001-12-31T23:59:59", -1),
    ],
)
def test_an_iso_string_outside_years_1_to_9999_clamps(
    t: str, text: str, side: int
) -> None:
    notes: list[str] = []
    lo, hi = _BOUNDS[t]
    assert _ts_coercer(t, notes)(text) == (hi if side > 0 else lo)
    assert notes == ["ts"]


@pytest.mark.parametrize("t", sorted(_BOUNDS))
def test_an_iso_string_at_the_edges_is_exact(t: str) -> None:
    notes: list[str] = []
    coerce = _ts_coercer(t, notes)
    lo, hi = _BOUNDS[t]
    if t == "timestamp_ns":
        # Year 1 and 9999 are outside int64 nanos: clamped to int64.
        assert coerce("0001-01-01T00:00:00") == lo
        assert coerce("9999-12-31T23:59:59") == hi
        assert notes == ["ts", "ts"]
        return
    assert coerce("0001-01-01T00:00:00") == lo
    assert notes == []
    top = "9999-12-31T23:59:59" + (
        ".999999" if t in ("timestamp", "timestamptz") else ""
    )
    if t == "timestamp_ms":
        top = "9999-12-31T23:59:59.999"
    assert coerce(top) == hi
    assert notes == []


def test_tz_offsets_near_the_edges_never_overflow_and_clamp() -> None:
    notes: list[str] = []
    coerce = _ts_coercer("timestamptz", notes)
    # 9999-12-31T23:00 at -05:00 is 10000-01-01T04:00 UTC: past the top.
    assert coerce("9999-12-31T23:00:00-05:00") == _MAX_US
    # 0001-01-01T01:00 at +05:00 is 0000-12-31T20:00 UTC: below the bottom.
    assert coerce("0001-01-01T01:00:00+05:00") == _MIN_US
    assert notes == ["ts", "ts"]
    # In range with an offset: exact integer arithmetic.
    assert coerce("9999-12-31T18:59:59.999999-05:00") == _MAX_US
    assert coerce("0001-01-01T05:00:00+05:00") == _MIN_US
    assert notes == ["ts", "ts"]


def test_an_offset_out_of_range_string_into_a_naive_column_is_refused() -> None:
    notes: list[str] = []
    with pytest.raises(DecodeError, match="does not take an offset"):
        _ts_coercer("timestamp", notes)("10000-01-01T00:00:00+01:00")
    assert notes == []


@pytest.mark.parametrize(
    "text", ["10000-13-01T00:00:00", "10000-01-01Tnope", "1x000-01-01", "10000"]
)
def test_a_malformed_out_of_range_string_is_still_refused(text: str) -> None:
    notes: list[str] = []
    with pytest.raises(DecodeError, match="invalid ISO-8601"):
        _ts_coercer("timestamp", notes)(text)
    assert notes == []


def test_a_clamp_inside_a_container_reports_the_dotted_path() -> None:
    notes: list[str] = []
    coerce = coercer_for(
        col(
            {
                "name": "p",
                "type": "struct",
                "field_id": 1,
                "ordinal": 0,
                "children": [
                    {"name": "at", "type": "timestamptz", "field_id": 2, "ordinal": 0}
                ],
            }
        ),
        on_clamp=notes.append,
    )
    assert coerce({"at": 10**25}) == {"at": _MAX_US}
    assert notes == ["p.at"]


def test_the_flush_decoder_counts_clamps_per_column_and_builds_them() -> None:
    cols = columns(
        [
            {"name": "ts", "type": "timestamp", "field_id": 1, "ordinal": 0},
            {"name": "tz", "type": "timestamptz", "field_id": 2, "ordinal": 1},
        ]
    )
    decoder = flush.JsonObjectDecoder(cols)
    record = decoder.decode_record(
        json.dumps({"ts": 10**20, "tz": "0000-01-01T00:00:00"}).encode()
    )
    assert record.values == {"ts": _MAX_US, "tz": _MIN_US}
    assert decoder.clamped == {"ts": 1, "tz": 1}
    table, _ = flush._build_batch([record], cols)
    assert table.column("ts").to_pylist() == [
        datetime.fromisoformat("9999-12-31T23:59:59.999999")
    ]
    assert table.column("tz").to_pylist() == [datetime(1, 1, 1, tzinfo=UTC)]
