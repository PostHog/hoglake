"""Property-based checks of the Variant codec (pyhoglake.variant).

The oracle is the SOURCE value, never a second encoding: every row read
back must be the JSON or Python value it was encoded from (variant_helpers.
same: exact integers, bit-equal doubles, exact Decimals, key order ignored).
The decoder that reads it back is pinned to outside ground truth by the
apache/parquet-testing vectors (test_parquet_testing.py), so this is not an
agreement oracle between two halves of one codec.

Values draw keys from a small set that collides with the declarations' field
names, in and out of case, so rows shred, partly shred and miss. The
declarations are a fixed corpus (the Trino writer's layouts, array and
primitive roots, the bench's) and generated ones over the same names.

Properties:

- round trip: JSON text and Python objects, under every declaration;
- verify() passes: the invariants a round trip cannot see (a sorted and
  complete dictionary, residuals without declared keys, the smallest
  widths, is_large above 255 only, no missing element);
- determinism: the same rows encode to equal storage, chunked or not, and
  JSON text to the same storage as the objects it parses to;
- invalid rows: under on_invalid="null" exactly the bad rows are SQL NULL,
  counted by reason, and the rows around them are untouched;
- aware datetimes: those whose UTC time Python holds round-trip, the rest
  are refused (timestamp_out_of_range), never written one-way;
- robustness: bytes mutated from valid encodings read, or are refused with
  VariantEncodingError, never another exception (AGENT.md's fuzzing rule
  for a new decoder of bytes from anywhere).
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pyarrow as pa
from hypothesis import given, settings
from hypothesis import strategies as st
from variant_helpers import same, unshredded

from pyhoglake import VariantEncodingError, VariantReport, variant
from pyhoglake import _variant_codec as codec
from pyhoglake.variant import VARIANT_NULL

# The "qe" hypothesis profile (deadline=None) is loaded in conftest.py.

#: Field names a declaration may use, pairwise distinct case-insensitively.
NAMES = ["a", "b", "value", "a.b", "é", "\U0001f600", "$browser"]
#: Keys a value may use: the names, other spellings of them, and others.
KEYS = st.one_of(
    st.sampled_from([*NAMES, "A", "B", "É", "$Browser", "typed_value", "", "c"]),
    st.text(st.characters(exclude_categories=("Cs",)), max_size=4),
)

INTEGERS = st.one_of(
    st.integers(-(2**64), 2**64),
    st.integers(-(10**38) + 1, 10**38 - 1),
    st.sampled_from([127, 128, -129, 2**31, -(2**31) - 1, 2**63, -(2**63) - 1]),
)
TEXT = st.one_of(
    st.text(st.characters(exclude_categories=("Cs",)), max_size=8),
    st.text(st.characters(exclude_categories=("Cs",)), min_size=60, max_size=70),
)
JSON_SCALARS = st.one_of(
    st.none(),
    st.booleans(),
    INTEGERS,
    st.floats(allow_nan=False, allow_infinity=False),
    TEXT,
)


def _containers(children):
    return st.lists(children, max_size=4) | st.dictionaries(KEYS, children, max_size=5)


JSON_VALUES = st.recursive(JSON_SCALARS, _containers, max_leaves=25)


@st.composite
def _decimals(draw):
    digits = draw(st.integers(1, 38))
    unscaled = draw(st.integers(-(10**digits) + 1, 10**digits - 1))
    scale = draw(st.integers(0, digits))
    return Decimal(f"{unscaled}E-{scale}")


_OFFSETS = st.builds(
    timezone,
    st.timedeltas(min_value=timedelta(hours=-23), max_value=timedelta(hours=23)),
)


def _in_utc_range(value: datetime) -> bool:
    try:
        value.astimezone(UTC)
    except OverflowError:
        return False
    return True


#: Aware datetimes, the ends of Python's range among them, where an offset
#: can put the UTC time outside it.
AWARE = st.one_of(
    st.datetimes(timezones=_OFFSETS),
    st.builds(
        lambda moment, tz: moment.replace(tzinfo=tz),
        st.sampled_from(
            [datetime.min.replace(tzinfo=UTC), datetime.max.replace(tzinfo=UTC)]
        ),
        _OFFSETS,
    ),
)
PYTHON_SCALARS = st.one_of(
    JSON_SCALARS,
    st.floats(),  # NaN and the infinities are doubles in Python
    _decimals(),
    st.binary(max_size=12),
    st.dates(),
    st.times(),
    st.datetimes(),
    # The rest are refused, which test_aware_datetimes_round_trip_or_are_refused
    # pins; here every row is valid.
    AWARE.filter(_in_utc_range),
    st.uuids(),
)
PYTHON_VALUES = st.recursive(
    PYTHON_SCALARS,
    lambda children: (
        st.lists(children, max_size=4)
        | st.tuples(children, children)
        | st.dictionaries(KEYS, children, max_size=5)
    ),
    max_leaves=25,
)

LEAVES = [
    {"type": "string"},
    {"type": "int8"},
    {"type": "int64"},
    {"type": "double"},
    {"type": "boolean"},
    {"type": "variant"},
    {"type": "decimal16", "precision": 38, "scale": 0},
    {"type": "decimal8", "precision": 18, "scale": 2},
    {"type": "decimal4", "precision": 9, "scale": 4},
    {"type": "date"},
    {"type": "timestamptz"},
    {"type": "timestamp"},
    {"type": "time"},
    {"type": "uuid"},
    {"type": "binary"},
    {"type": "float"},
]


@st.composite
def _declaration(draw, depth: int = 0):
    kind = draw(st.sampled_from(["leaf", "object", "array"] if depth < 3 else ["leaf"]))
    if kind == "object":
        names = draw(
            st.lists(st.sampled_from(NAMES), min_size=1, max_size=4, unique=True)
        )
        return {
            "type": "object",
            "fields": [
                {"name": name, **draw(_declaration(depth + 1))} for name in names
            ],
        }
    if kind == "array":
        return {"type": "array", "element": draw(_declaration(depth + 1))}
    return dict(draw(st.sampled_from(LEAVES)))


CORPUS = [
    None,
    {"type": "variant"},
    {"type": "int64"},
    {"type": "array", "element": {"type": "string"}},
    {
        "type": "object",
        "fields": [
            {"name": "a", "type": "string"},
            {"name": "b", "type": "object", "fields": [{"name": "a", "type": "int32"}]},
            {"name": "value", "type": "int32"},
            {"name": "a.b", "type": "array", "element": {"type": "variant"}},
        ],
    },
    {
        "type": "array",
        "element": {
            "type": "object",
            "fields": [
                {"name": "a", "type": "array", "element": {"type": "double"}},
                {"name": "$browser", "type": "decimal16", "precision": 38, "scale": 0},
            ],
        },
    },
]
DECLARATIONS = st.one_of(st.sampled_from(CORPUS), _declaration())


def _json_expected(value: object) -> object:
    return VARIANT_NULL if value is None else value


def _check(array: pa.Array, decl, sources: list[object]) -> None:
    variant.verify(array, shredding=decl)
    got = variant.to_python(array, shredding=decl)
    assert len(got) == len(sources)
    for g, want in zip(got, sources, strict=True):
        assert same(g, want), (g, want, decl)


@given(st.lists(JSON_VALUES, min_size=1, max_size=5), DECLARATIONS)
def test_json_round_trips_exactly(values, decl):
    texts = [json.dumps(value) for value in values]
    array = variant.encode_json(texts, shredding=decl).array
    _check(array, decl, [_json_expected(value) for value in values])


@given(st.lists(PYTHON_VALUES, min_size=1, max_size=5), DECLARATIONS)
def test_python_round_trips_exactly(values, decl):
    rows = [VARIANT_NULL if value is None else value for value in values]
    array = variant.encode_python(rows, shredding=decl).array
    _check(array, decl, rows)


@given(st.lists(JSON_VALUES, min_size=1, max_size=6), DECLARATIONS, st.integers(1, 3))
def test_encoding_is_deterministic(values, decl, chunk_rows):
    texts = [None if i % 4 == 3 else json.dumps(v) for i, v in enumerate(values)]
    first = variant.encode_json(texts, shredding=decl).array
    again = variant.encode_json(pa.array(texts, pa.json_()), shredding=decl).array
    assert first.equals(again)
    # Chunks change where rows are cut, never what a row holds.
    chunks = codec.encode(
        texts,
        codec.compile_plan(decl),
        VariantReport(),
        json_text=True,
        column=None,
        nullable=True,
        on_invalid="raise",
        chunk_rows=chunk_rows,
    )
    assert pa.chunked_array(chunks, type=first.type).combine_chunks().equals(first)
    # JSON text lands exactly as the objects it parses to.
    objects = [
        None if t is None else VARIANT_NULL if v is None else v
        for t, v in zip(texts, values, strict=True)
    ]
    assert variant.encode_python(objects, shredding=decl).array.equals(first)


#: Rows each invalid for its reason, wherever the declaration puts them.
INVALID = {
    '{"a":1,"a":2}': "duplicate_key",
    '{"b":[NaN]}': "non_finite_number",
    '{"a":"\\ud800"}': "invalid_unicode",
    '[{"\\udc00":1}]': "invalid_unicode",
    "1" + "0" * 38: "integer_out_of_range",
    '{"a":' * 130 + "1" + "}" * 130: "nesting_too_deep",
    "{": "invalid_json",
}


@given(
    st.lists(
        st.one_of(JSON_VALUES.map(json.dumps), st.sampled_from(sorted(INVALID))),
        min_size=1,
        max_size=8,
    ),
    DECLARATIONS,
)
@settings(max_examples=60)
def test_invalid_rows_are_null_and_nothing_else_moves(texts, decl):
    encoded = variant.encode_json(texts, shredding=decl, on_invalid="null")
    expected = [None if t in INVALID else _json_expected(json.loads(t)) for t in texts]
    _check(encoded.array, decl, expected)
    reasons: dict[str, int] = {}
    for text in texts:
        if text in INVALID:
            reasons[INVALID[text]] = reasons.get(INVALID[text], 0) + 1
    assert encoded.report.invalid_by_reason == reasons
    assert encoded.report.invalid_rows == sum(reasons.values())


@given(st.uuids())
def test_uuids_are_big_endian(value: uuid.UUID):
    stored = variant.encode_python([value], shredding={"type": "uuid"}).array
    assert stored.field("typed_value").storage.to_pylist() == [value.bytes]
    assert variant.encode_python([value]).array.field("value").to_pylist() == [
        b"\x50" + value.bytes
    ]


@given(AWARE, st.sampled_from([None, {"type": "timestamptz"}, {"type": "variant"}]))
def test_aware_datetimes_round_trip_or_are_refused(moment, decl):
    if _in_utc_range(moment):
        array = variant.encode_python([moment], shredding=decl).array
        _check(array, decl, [moment])
        return
    encoded = variant.encode_python([moment, 1], shredding=decl, on_invalid="null")
    assert encoded.report.invalid_by_reason == {"timestamp_out_of_range": 1}
    _check(encoded.array, decl, [None, 1])


def _mutated(draw, data: bytes) -> bytes:
    """``data`` with a few bytes flipped, cut, or put in."""
    out = bytearray(data)
    for _ in range(draw(st.integers(1, 3))):
        action = draw(st.sampled_from(["flip", "cut", "insert"]))
        at = draw(st.integers(0, len(out)))
        if action == "flip" and at < len(out):
            out[at] ^= draw(st.integers(1, 255))
        elif action == "cut":
            del out[at : at + draw(st.integers(1, 4))]
        else:
            out[at:at] = draw(st.binary(min_size=1, max_size=4))
    return bytes(out)


@given(st.data(), JSON_VALUES, st.booleans())
@settings(max_examples=300)
def test_mutated_bytes_are_read_or_refused_never_crash(data, value, in_metadata):
    stored = variant.encode_json([json.dumps(value)]).array.to_pylist()[0]
    metadata, encoded = stored["metadata"], stored["value"]
    if in_metadata:
        metadata = _mutated(data.draw, metadata)
    else:
        encoded = _mutated(data.draw, encoded)
    array = unshredded(metadata, encoded)
    for read in (
        variant.to_python,
        variant.verify,
        lambda a: variant.verify(a, canonical=False),
    ):
        try:
            read(array)
        except VariantEncodingError:
            pass
