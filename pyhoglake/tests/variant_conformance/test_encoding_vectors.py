"""Suite E: the Variant bytes pyhoglake writes, as golden vectors.

Each test cites the spec clause it pins: VE = VariantEncoding.md and
VS = VariantShredding.md of apache/parquet-format at
bf0993925ccf41b1fb4b1ae241af4a66e8adf1fe, by line. Where the encoding is
pyhoglake's choice among legal ones (the smallest width, sorted
dictionaries: D8, D9), the README's policy table says so. Expected bytes
are written out by hand or built with int.to_bytes, never with the codec;
three of them are apache/parquet-testing's own (variant/primitive_*.value).
"""

from __future__ import annotations

import json
import struct
import uuid
from collections import OrderedDict, namedtuple
from datetime import UTC, date, datetime, time, timedelta, timezone
from decimal import Decimal

import pyarrow as pa
import pytest
from variant_helpers import (
    ident,
    json_bytes,
    json_row,
    row,
    same,
    unshredded,
    value_bytes,
)

from pyhoglake import VariantEncodingError, variant
from pyhoglake import _variant_codec as codec
from pyhoglake.models import Column
from pyhoglake.variant import VARIANT_NULL


def le(value: int, size: int) -> str:
    return value.to_bytes(size, "little", signed=True).hex()


# -- E1, E2: the metadata header (VE:74-146) ----------------------------------


def test_e1_metadata_of_two_keys_byte_for_byte():
    # 0x11: version 1, sorted_strings, 1-byte offsets; 2 strings; offsets
    # 0, 1, 2; "a" then "b", whatever order the object had them in.
    metadata, value = value_bytes({"b": 1, "a": 2})
    assert metadata.hex() == "1102000102" + "6162"
    # Field ids and offsets in key order (VE:456): a (id 0) = 2, b (id 1) = 1.
    assert value.hex() == "02" + "02" + "0001" + "000204" + "0c02" + "0c01"


@pytest.mark.parametrize(
    ("total", "header", "width"),
    [
        (255, 0x11, 1),
        (256, 0x51, 2),
        (65_535, 0x51, 2),
        (65_536, 0x91, 3),
        (2**24 - 1, 0x91, 3),
        (2**24, 0xD1, 4),
    ],
)
def test_e1_offset_width_follows_the_total_key_bytes(total, header, width):
    # The offset width is the smallest that holds the total length of the
    # strings and their count (they share it, VE:131-134).
    metadata, value = value_bytes({"k" * total: 1})
    assert metadata[0] == header
    assert int.from_bytes(metadata[1 : 1 + width], "little") == 1
    offsets = metadata[1 + width : 1 + 3 * width]
    assert offsets == (0).to_bytes(width, "little") + total.to_bytes(width, "little")
    assert len(metadata) == 1 + 3 * width + total
    # And the decoder's canonical check agrees on the smallest width.
    variant.verify(unshredded(metadata, value))


def test_e1_dictionary_size_of_256():
    keys = {f"{i:03d}": i for i in range(256)}
    metadata, _ = value_bytes(keys)
    # 256 strings, 768 bytes: two-byte size and offsets.
    assert metadata[0] == 0x51
    assert int.from_bytes(metadata[1:3], "little") == 256


@pytest.mark.parametrize(
    "value", [1, "x", [1, [2]], [], VARIANT_NULL, Decimal("1.5"), {}]
)
def test_e2_a_value_without_keys_has_the_three_byte_empty_metadata(value):
    # Version 1, unsorted (there is nothing to sort), size 0, one offset 0.
    # Not the two bytes of VS:60-63, which DuckDB rejects.
    metadata, _ = value_bytes(value)
    assert metadata == b"\x01\x00\x00"


def test_e2_empty_metadata_from_json():
    assert json_bytes("[]")[0] == b"\x01\x00\x00"
    assert json_bytes("null")[0] == b"\x01\x00\x00"


# -- E3, E4: one dictionary per row, with every key (VS:36-37) ----------------

_DECL = {
    "type": "object",
    "fields": [
        {"name": "a", "type": "string"},
        {
            "name": "b",
            "type": "object",
            "fields": [{"name": "c", "type": "int64"}],
        },
        {"name": "e", "type": "array", "element": {"type": "variant"}},
    ],
}
_ROW = {"a": "x", "b": {"c": 1, "d": 2}, "e": [{"f": 1}], "g": {"h": None}}


@pytest.mark.parametrize("source", ["json", "python"])
def test_e3_metadata_has_every_key_at_every_depth(source):
    # Shredded keys included (a, b, c, e land in typed columns only):
    # parquet-java and Iceberg fail on a dictionary without them.
    import json

    stored = json_row(json.dumps(_ROW), _DECL) if source == "json" else row(_ROW, _DECL)
    names = codec.decode_metadata(stored["metadata"])
    assert names == ("a", "b", "c", "d", "e", "f", "g", "h")
    array = variant.encode_python([_ROW], shredding=_DECL).array
    variant.verify(array, shredding=_DECL)


def test_e4_every_value_column_of_a_row_uses_its_metadata():
    stored = row(_ROW, _DECL)
    names = codec.decode_metadata(stored["metadata"])
    typed = stored["typed_value"]
    # The top-level residual, the residual of b, and the element of e are
    # three value columns; each decodes against the row's one dictionary.
    assert codec.decode_value(stored["value"], names) == {"g": {"h": None}}
    assert codec.decode_value(typed["b"]["value"], names) == {"d": 2}
    assert codec.decode_value(typed["e"]["typed_value"][0]["value"], names) == {"f": 1}
    assert typed["b"]["typed_value"]["c"] == {"value": None, "typed_value": 1}


# -- E5: primitive headers at their boundaries (VE:400-446) -------------------

_PLUS_5_30 = timezone(timedelta(hours=5, minutes=30))
_MINUS_4 = timezone(timedelta(hours=-4))

PRIMITIVES = [
    # Type 0 and 1, 2: no data.
    (VARIANT_NULL, "00"),
    (True, "04"),
    (False, "08"),
    # Integers take the smallest type that holds them (D1): 3 to 6.
    (0, "0c00"),
    (127, "0c7f"),
    (-128, "0c80"),
    (128, "108000"),
    (-129, "107fff"),
    (32_767, "10ff7f"),
    (-32_768, "100080"),
    (32_768, "1400800000"),
    (-32_769, "14ff7fffff"),
    (2**31 - 1, "14ffffff7f"),
    (-(2**31), "1400000080"),
    (2**31, "18" + le(2**31, 8)),
    (-(2**31) - 1, "18" + le(-(2**31) - 1, 8)),
    (2**63 - 1, "18ffffffffffffff7f"),
    (-(2**63), "180000000000000080"),
    # Beyond int64, decimal16 (type 10) with scale 0, both signs (D1).
    (2**63, "2800" + le(2**63, 16)),
    (-(2**63) - 1, "2800" + le(-(2**63) - 1, 16)),
    (2**64 - 1, "2800" + "ff" * 8 + "00" * 8),
    (10**38 - 1, "2800" + le(10**38 - 1, 16)),
    (-(10**38 - 1), "2800" + le(-(10**38 - 1), 16)),
    # Doubles (type 7), the sign of zero kept; NaN only from Python.
    (-0.0, "1c" + "00" * 7 + "80"),
    (1.5, "1c" + struct.pack("<d", 1.5).hex()),
    (float("nan"), "1c" + struct.pack("<d", float("nan")).hex()),
    (float("-inf"), "1c" + struct.pack("<d", float("-inf")).hex()),
    # Decimals (8, 9, 10) by precision, scale first, little-endian.
    (Decimal("1.23"), "2002" + le(123, 4)),
    (Decimal("-1.5"), "2001" + le(-15, 4)),
    (Decimal(999999999), "2000" + le(999_999_999, 4)),
    (Decimal(-999999999), "2000" + le(-999_999_999, 4)),
    (Decimal(9999999999), "2400" + le(9_999_999_999, 8)),
    (Decimal(999999999999999999), "2400" + le(10**18 - 1, 8)),
    (Decimal(9999999999999999999), "2800" + le(10**19 - 1, 16)),
    (Decimal(10**38 - 1), "2800" + le(10**38 - 1, 16)),
    (Decimal(-(10**38 - 1)), "2800" + le(-(10**38 - 1), 16)),
    (Decimal("0." + "0" * 37 + "1"), "2826" + le(1, 16)),
    (Decimal("-0." + "9" * 38), "2826" + le(-(10**38 - 1), 16)),
    # A positive exponent is rescaled to 0; the precision counts the scale.
    (Decimal("1E+2"), "2000" + le(100, 4)),
    (Decimal("0.001"), "2003" + le(1, 4)),
    (Decimal("1E-10"), "240a" + le(1, 8)),
    (Decimal("-0.00"), "2002" + le(0, 4)),
    # A zero's exponent is all it has: rescaled to scale 0, one digit.
    (Decimal("0E+38"), "2000" + le(0, 4)),
    (Decimal("0E+100"), "2000" + le(0, 4)),
    # Dates (11), before the epoch too.
    (date(1970, 1, 1), "2c00000000"),
    (date(1969, 12, 31), "2cffffffff"),
    (date(1, 1, 1), "2c" + le(-719_162, 4)),
    # Timestamps: aware ones in UTC (12), naive ones without a zone (13).
    (datetime(1970, 1, 1), "34" + "00" * 8),
    (datetime(1969, 12, 31, 23, 59, 59, 999_999), "34" + "ff" * 8),
    (datetime(1970, 1, 1, tzinfo=UTC), "30" + "00" * 8),
    (datetime(1970, 1, 1, 5, 30, tzinfo=_PLUS_5_30), "30" + "00" * 8),
    # The ends of Python's range, in UTC.
    (datetime(1, 1, 1, tzinfo=UTC), "30" + le(-62_135_596_800_000_000, 8)),
    (datetime.max.replace(tzinfo=UTC), "30" + le(253_402_300_799_999_999, 8)),
    (
        datetime(1, 1, 1, 5, tzinfo=timezone(timedelta(hours=5))),
        "30" + le(-62_135_596_800_000_000, 8),
    ),
    # apache/parquet-testing variant/primitive_timestamp.value:
    (datetime(2025, 4, 16, 12, 34, 56, 780_000, tzinfo=_MINUS_4), "30e05297dde7320600"),
    # Binary (15) from any bytes-like.
    (b"\x00\xff", "3c02000000" + "00ff"),
    (bytearray(b"\x00\xff"), "3c02000000" + "00ff"),
    (memoryview(b"\x00\xff"), "3c02000000" + "00ff"),
    (b"", "3c00000000"),
    # Strings of 64 bytes and more (16).
    ("x" * 64, "4040000000" + "78" * 64),
    # Times (17).
    (time(0, 0), "44" + "00" * 8),
    (time(23, 59, 59, 999_999), "44" + le(86_399_999_999, 8)),
    # apache/parquet-testing variant/primitive_time.value:
    (time(12, 33, 54, 123_456), "44c0f229880a000000"),
    # UUIDs (20), big-endian; parquet-testing variant/primitive_uuid.value:
    (
        uuid.UUID("f24f9b64-81fa-49d1-b74e-8c09a6e31c56"),
        "50f24f9b6481fa49d1b74e8c09a6e31c56",
    ),
]


@pytest.mark.parametrize(("value", "expected"), PRIMITIVES, ids=ident)
def test_e5_primitive_bytes(value, expected):
    assert value_bytes(value)[1].hex() == expected


@pytest.mark.parametrize(
    "text",
    ["0", "127", "-128", "128", "-129", "32768", "2147483648", "9223372036854775807"],
)
def test_e5_json_integers_take_the_smallest_type_too(text):
    assert json_bytes(text)[1] == value_bytes(int(text))[1]


@pytest.mark.parametrize(
    ("text", "number"),
    [
        ("9223372036854775808", 2**63),
        ("-9223372036854775809", -(2**63) - 1),
        ("18446744073709551615", 2**64 - 1),
        ("9" * 38, 10**38 - 1),
        ("-" + "9" * 38, -(10**38 - 1)),
    ],
)
def test_e5_json_integers_beyond_int64_are_decimal16(text, number):
    assert json_bytes(text)[1].hex() == "2800" + le(number, 16)


@pytest.mark.parametrize(
    ("text", "number"),
    [("1.5", 1.5), ("-0.0", -0.0), ("1e2", 100.0), ("1E-400", 0.0), ("2.5e-3", 0.0025)],
)
def test_e5_json_fractions_and_exponents_are_doubles(text, number):
    assert json_bytes(text)[1] == b"\x1c" + struct.pack("<d", number)


def test_e5_only_the_primitive_types_pyhoglake_writes():
    # Never float (14) or the nanosecond timestamps (18, 19): Python has
    # neither, and JSON numbers are ints, decimals or doubles.
    headers = {value_bytes(value)[1][0] for value, _ in PRIMITIVES}
    assert headers == {
        0x00, 0x04, 0x08, 0x0C, 0x10, 0x14, 0x18, 0x1C, 0x20, 0x24, 0x28,
        0x2C, 0x30, 0x34, 0x3C, 0x40, 0x44, 0x50,
    }  # fmt: skip
    assert not headers & {14 << 2, 18 << 2, 19 << 2}
    array = variant.encode_python([value for value, _ in PRIMITIVES]).array
    variant.verify(array)  # canonical: no 14, 18 or 19 anywhere


# -- E6: short strings (VE:182-191, 406) ---------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("", "01"),
        ("x" * 63, "fd" + "78" * 63),
        ("x" * 64, "4040000000" + "78" * 64),
        ("€" * 21, "fd" + "e282ac" * 21),  # 63 bytes of UTF-8
        ("é" * 32, "4040000000" + "c3a9" * 32),  # 64 bytes
    ],
)
def test_e6_short_strings_hold_at_most_63_bytes(text, expected):
    assert value_bytes(text)[1].hex() == expected
    assert json_bytes(f'"{text}"')[1].hex() == expected


# -- E7: key order is UTF-8 byte order (VE:456) ---------------------------------


def test_e7_keys_sort_by_utf8_bytes_not_utf16_units():
    # U+FFFF is EF BF BF and U+1F600 F0 9F 98 80, so U+FFFF comes first;
    # in UTF-16, U+1F600 (D83D DE00) would. Iceberg once sorted by UTF-16.
    metadata, value = value_bytes({"\U0001f600": 1, "￿": 2, "B": 3, "a": 4})
    assert metadata.hex() == "1104" + "0001020509" + "42" + "61" + "efbfbf" + "f09f9880"
    # The values follow the keys: B=3, a=4, U+FFFF=2, U+1F600=1.
    assert value.hex() == "02" + "04" + "00010203" + "0002040608" + "0c030c040c020c01"


# -- E8: is_large and the offset widths (VE:193-227, 385-389) ------------------


@pytest.mark.parametrize(
    ("count", "header", "count_bytes"),
    [(255, 0x06, "ff"), (256, 0x46, "00010000"), (257, 0x56, "01010000")],
)
def test_e8_objects_are_large_above_255_fields(count, header, count_bytes):
    obj = {f"{i:03d}": 1 for i in range(count)}
    metadata, value = value_bytes(obj)
    # Two-byte offsets (2 * count bytes of values); one-byte field ids up to
    # id 255, two-byte ids from 256 (0x10).
    assert value[0] == header
    assert value[1 : 1 + len(count_bytes) // 2].hex() == count_bytes
    # And the decoder's canonical check agrees where is_large begins.
    variant.verify(unshredded(metadata, value))


@pytest.mark.parametrize(
    ("count", "header", "count_bytes"), [(255, 0x07, "ff"), (256, 0x17, "00010000")]
)
def test_e8_arrays_are_large_above_255_elements(count, header, count_bytes):
    metadata, value = value_bytes([1] * count)
    assert value[0] == header
    assert value[1 : 1 + len(count_bytes) // 2].hex() == count_bytes
    variant.verify(unshredded(metadata, value))


@pytest.mark.parametrize(
    ("size", "header"),
    [
        (250, 0x03),
        (251, 0x07),
        (65_530, 0x07),
        (65_531, 0x0B),
        (2**24 - 6, 0x0B),
        (2**24 - 5, 0x0F),
    ],
)
def test_e8_offset_width_follows_the_data_size(size, header):
    # One long string of 5 + size bytes: 255 fits a 1-byte offset, 256 not;
    # 65,535 fits two bytes, 65,536 not; 16,777,215 three, 16,777,216 not.
    metadata, value = value_bytes(["x" * size])
    assert value[0] == header
    # And the decoder's canonical check agrees on the smallest width.
    variant.verify(unshredded(metadata, value))


@pytest.mark.parametrize("size", [2**24, 2**24 + 1])
def test_e8_a_string_or_binary_of_16_mib_keeps_a_four_byte_size(size):
    # A long string's or binary's size is four bytes whatever the offset
    # widths around it, and from 2**24 the fourth is not 0.
    for value, header in (("x" * size, 0x40), (b"y" * size, 0x3C)):
        metadata, raw = value_bytes(value)
        assert raw[:5].hex() == f"{header:02x}" + le(size, 4)
        array = unshredded(metadata, raw)
        assert variant.to_python(array) == [value]
        variant.verify(array)
    nested = {"k": "z" * size}
    array = variant.encode_python([nested]).array
    assert variant.to_python(array) == [nested]
    variant.verify(array)


@pytest.mark.parametrize(
    ("size", "header"),
    [(250, 0x02), (251, 0x06), (65_530, 0x06), (65_531, 0x0A)],
)
def test_e8_object_offset_width_follows_the_data_size(size, header):
    # The same for an object's field values: one field, one-byte ids.
    metadata, value = value_bytes({"a": "x" * size})
    assert value[0] == header
    width = ((header >> 2) & 0x03) + 1
    assert int.from_bytes(value[3 + width : 3 + 2 * width], "little") == size + 5
    variant.verify(unshredded(metadata, value))


# -- E9: decimal byte order -----------------------------------------------------


def test_e9_variant_decimals_are_little_endian_and_typed_decimal16_exact():
    # The Variant encoding is little-endian (VE:394-397). A decimal16 leaf
    # is decimal128 in Arrow, whose value must be the number itself:
    # Parquet's big-endian FLBA is pyarrow's to write from it.
    number = Decimal("-12345678901234567890.123")
    decl = {"type": "decimal16", "precision": 38, "scale": 3}
    stored = row(number, decl)
    assert stored["typed_value"] == number
    assert stored["value"] is None
    unscaled = -12_345_678_901_234_567_890_123
    assert value_bytes(number)[1].hex() == "2803" + le(unscaled, 16)
    typed = variant.encode_python([number], shredding=decl).array.field("typed_value")
    assert typed.buffers()[1].to_pybytes()[:16] == unscaled.to_bytes(
        16, "little", signed=True
    )


# -- E10: invalid rows ------------------------------------------------------------

DEEPEST = "[" * (codec.MAX_VARIANT_DEPTH + 1) + "]" * (codec.MAX_VARIANT_DEPTH + 1)
TOO_DEEP = "[" * (codec.MAX_VARIANT_DEPTH + 2) + "]" * (codec.MAX_VARIANT_DEPTH + 2)

INVALID_JSON = [
    ('{"a":1,"a":2}', "duplicate_key", None),
    ('{"x":[{"a":1,"a":1}]}', "duplicate_key", None),
    ("NaN", "non_finite_number", None),
    ('{"a":Infinity}', "non_finite_number", None),
    ("[-Infinity]", "non_finite_number", None),
    ("1e400", "non_finite_number", None),
    ('{"a":-1e400}', "non_finite_number", None),
    ("1" + "0" * 38, "integer_out_of_range", "$"),
    ("-1" + "0" * 38, "integer_out_of_range", "$"),
    ('{"a":[1,' + "9" * 39 + "]}", "integer_out_of_range", "$.a[1]"),
    # Beyond CPython's int-conversion limit, the parser refuses it.
    ("9" * 4301, "integer_out_of_range", None),
    # A key with a surrogate or a control character is quoted with
    # escapes, so the path is text any UTF-8 log can write.
    ('{"\\ud800":1}', "invalid_unicode", '$["\\ud800"]'),
    ('{"a":{"\\udfff":1}}', "invalid_unicode", '$.a["\\udfff"]'),
    ('{"a\\nb":["\\ud800"]}', "invalid_unicode", '$["a\\nb"][0]'),
    # A key found by its trail: in an array, and after a sibling.
    ('[1,{"\\ud800":1}]', "invalid_unicode", '$[1]["\\ud800"]'),
    ('{"a":1,"b":{"\\ud800":0}}', "invalid_unicode", '$.b["\\ud800"]'),
    ('{"a":[0,{"b":1,"\\udfff":2}]}', "invalid_unicode", '$.a[1]["\\udfff"]'),
    ('{"a":"\\ud800"}', "invalid_unicode", "$.a"),
    ('["x","\\ud800\\u0041"]', "invalid_unicode", "$[1]"),
    (TOO_DEEP, "nesting_too_deep", "$" + "[0]" * codec.MAX_VARIANT_DEPTH),
    # A number at depth 129, under 129 objects.
    (
        '{"k":' * 129 + "1" + "}" * 129,
        "nesting_too_deep",
        "$" + ".k" * codec.MAX_VARIANT_DEPTH,
    ),
    ("[" * 10_000 + "]" * 10_000, "nesting_too_deep", None),
    ('{"a":', "invalid_json", None),
    ("", "invalid_json", None),
    ("{'a':1}", "invalid_json", None),
    ("[1,]", "invalid_json", None),
    ("1 2", "invalid_json", None),
]

#: The declarations each invalid row is tried under: the faults are found
#: whether the value is shredded or not, in value or in typed_value.
DECLARATIONS = [
    None,
    {"type": "object", "fields": [{"name": "a", "type": "string"}]},
    {"type": "object", "fields": [{"name": "a", "type": "int64"}]},
    {"type": "array", "element": {"type": "string"}},
]


@pytest.mark.parametrize("decl", DECLARATIONS, ids=lambda d: str(d and d["type"]))
@pytest.mark.parametrize(("text", "reason", "path"), INVALID_JSON, ids=ident)
def test_e10_invalid_json_rows(text, reason, path, decl):
    with pytest.raises(VariantEncodingError) as raised:
        variant.encode_json(["1", text], shredding=decl)
    error = raised.value
    assert (error.reason, error.row, error.path) == (reason, 1, path)
    encoded = variant.encode_json(["1", text, "2"], shredding=decl, on_invalid="null")
    assert variant.to_python(encoded.array) == [1, None, 2]
    report = encoded.report
    assert (report.invalid_rows, report.invalid_by_reason) == (1, {reason: 1})
    assert report.first_invalid == (None, 1, reason, path)
    assert report.sql_null_rows == 0
    variant.verify(encoded.array, shredding=decl)


def test_e10_objects_nest_to_the_limit_too():
    text = '{"k":' * 128 + "1" + "}" * 128
    assert variant.to_python(variant.encode_json([text]).array)[0] is not None


def test_e10_nesting_to_the_limit_is_valid():
    # The innermost array is at depth 128, its parent's element.
    encoded = variant.encode_json([DEEPEST])
    value = variant.to_python(encoded.array)[0]
    for _ in range(codec.MAX_VARIANT_DEPTH):
        (value,) = value
    assert value == []
    nested: object = 1
    for _ in range(codec.MAX_VARIANT_DEPTH):
        nested = {"k": nested}
    assert variant.to_python(variant.encode_python([nested]).array) == [nested]
    with pytest.raises(VariantEncodingError, match="nested more than 128"):
        variant.encode_python([{"k": nested}])
    # 128 lists or tuples around a value, the innermost at depth 127.
    for kind in (list, tuple):
        encoded = variant.encode_python([_nested(codec.MAX_VARIANT_DEPTH, kind)])
        assert variant.to_python(encoded.array) == [_nested(codec.MAX_VARIANT_DEPTH)]


def test_e10_an_empty_container_may_sit_at_the_limit():
    # Nothing is below an empty object or array, so either kind may be the
    # value at depth 128, through either encoder.
    limit = codec.MAX_VARIANT_DEPTH
    for text in (
        "[" * limit + "{}" + "]" * limit,
        '{"k":' * limit + "[]" + "}" * limit,
    ):
        encoded = variant.encode_json([text])
        assert variant.to_python(encoded.array) == [json.loads(text)]
    in_lists: object = {}
    in_objects: object = []
    for _ in range(limit):
        in_lists, in_objects = [in_lists], {"k": in_objects}
    for value in (in_lists, in_objects):
        assert variant.to_python(variant.encode_python([value]).array) == [value]


def _nested(depth: int, kind: type = list) -> object:
    """1 inside ``depth`` lists (or tuples): the innermost at depth - 1."""
    value: object = 1
    for _ in range(depth):
        value = kind([value])
    return value


INVALID_PYTHON = [
    (time(1, tzinfo=UTC), "aware_time", "$"),
    # A list or tuple at depth 128 that holds anything, as for a dict; the
    # path is the container's, below a key and an index too.
    (_nested(129), "nesting_too_deep", "$" + "[0]" * codec.MAX_VARIANT_DEPTH),
    (_nested(129, tuple), "nesting_too_deep", "$" + "[0]" * codec.MAX_VARIANT_DEPTH),
    (
        {"a": [0, _nested(128)]},
        "nesting_too_deep",
        "$.a[1]" + "[0]" * (codec.MAX_VARIANT_DEPTH - 2),
    ),
    ({"t": time(1, tzinfo=_PLUS_5_30)}, "aware_time", "$.t"),
    (object(), "unsupported_python_type", "$"),
    ({1: "x"}, "unsupported_python_type", "$"),
    ([{"a": {2}}], "unsupported_python_type", "$[0].a"),
    (type("F", (float,), {})(1.5), "unsupported_python_type", "$"),
    (type("S", (str,), {})("x"), "unsupported_python_type", "$"),
    ({type("K", (str,), {})("a"): 1}, "unsupported_python_type", "$"),
    # Containers are matched exactly too: a namedtuple would lose its names.
    (OrderedDict(a=1), "unsupported_python_type", "$"),
    ({"p": namedtuple("P", "x y")(1, 2)}, "unsupported_python_type", "$.p"),
    (type("L", (list,), {})([1]), "unsupported_python_type", "$"),
    # An aware datetime whose UTC time no datetime holds, at either end.
    (
        datetime(1, 1, 1, tzinfo=timezone(timedelta(microseconds=1))),
        "timestamp_out_of_range",
        "$",
    ),
    (
        {"t": datetime(9999, 12, 31, 23, tzinfo=timezone(timedelta(hours=-5)))},
        "timestamp_out_of_range",
        "$.t",
    ),
    (Decimal("NaN"), "decimal_out_of_range", "$"),
    (Decimal("-Infinity"), "decimal_out_of_range", "$"),
    (Decimal("1E+38"), "decimal_out_of_range", "$"),
    (Decimal("1E-39"), "decimal_out_of_range", "$"),
    (Decimal("1." + "0" * 38), "decimal_out_of_range", "$"),
    # A scale beyond what the exact context can rescale: refused, not a
    # decimal.Rounded from inside the encoder.
    (Decimal("1." + "0" * 200), "decimal_out_of_range", "$"),
    (10**38, "integer_out_of_range", "$"),
    (-(10**38), "integer_out_of_range", "$"),
    (10**5000, "integer_out_of_range", "$"),
    ("\ud800", "invalid_unicode", "$"),
    ({"\udc00": 1}, "invalid_unicode", '$["\\udc00"]'),
    ([1, {"\udc00": 1}], "invalid_unicode", '$[1]["\\udc00"]'),
    ({"a": 1, "b": {"\udc00": 0}}, "invalid_unicode", '$.b["\\udc00"]'),
    ({"a": [0, {1: 2}]}, "unsupported_python_type", "$.a[1]"),
]


@pytest.mark.parametrize("decl", DECLARATIONS, ids=lambda d: str(d and d["type"]))
@pytest.mark.parametrize(("value", "reason", "path"), INVALID_PYTHON, ids=ident)
def test_e10_invalid_python_rows(value, reason, path, decl):
    with pytest.raises(VariantEncodingError) as raised:
        variant.encode_python([1, value], shredding=decl)
    assert (raised.value.reason, raised.value.row, raised.value.path) == (
        reason,
        1,
        path,
    )
    encoded = variant.encode_python([1, value, 2], shredding=decl, on_invalid="null")
    assert variant.to_python(encoded.array) == [1, None, 2]
    assert encoded.report.invalid_by_reason == {reason: 1}


def test_e10_a_decimal_refusal_says_how_big_not_what():
    # A Decimal has as many digits as its caller gave it; the message
    # stays a line.
    for value in (
        Decimal("1" * 100_000),
        Decimal("1E-100000"),
        Decimal("1" * 50 + "E-20"),
        Decimal("NaN" + "1" * 10_000),
    ):
        with pytest.raises(VariantEncodingError) as raised:
            variant.encode_python([value])
        assert raised.value.reason == "decimal_out_of_range"
        assert len(str(raised.value)) < 120


def test_e10_a_cyclic_value_is_too_deep_not_a_crash():
    cycle: list[object] = []
    cycle.append(cycle)
    with pytest.raises(VariantEncodingError) as raised:
        variant.encode_python([cycle])
    assert raised.value.reason == "nesting_too_deep"


def test_e10_bytes_input_must_be_utf8():
    encoded = variant.encode_json([b'"\xff"', b'"ok"'], on_invalid="null")
    assert variant.to_python(encoded.array) == [None, "ok"]
    assert encoded.report.invalid_by_reason == {"invalid_unicode": 1}
    # Found before any parse, so at no path (D18).
    assert encoded.report.first_invalid == (None, 0, "invalid_unicode", None)
    with pytest.raises(VariantEncodingError) as raised:
        variant.encode_json([b'{"a":"\xff"}'])
    assert (raised.value.reason, raised.value.path) == ("invalid_unicode", None)


def test_e10_a_duplicate_key_is_named():
    with pytest.raises(VariantEncodingError, match="key 'b' appears twice"):
        variant.encode_json(['{"a":1,"b":2,"c":3,"b":4}'])


def test_e10_arrow_text_that_is_not_utf8_fails_its_row_only():
    # Arrow does not check that text is UTF-8 on every path (a view, a
    # Parquet file, IPC): one bad row is that row's fault, as it is from
    # Python bytes, never the batch's (D6).
    raw = pa.array([b'{"a":1}', b'"\xff"', b"2"], pa.binary())
    text = raw.view(pa.string())
    for values in (
        text,
        pa.ExtensionArray.from_storage(pa.json_(), text),
        raw.cast(pa.large_binary()).view(pa.large_string()),
        raw.cast(pa.binary_view()).view(pa.string_view()),
        pa.chunked_array([text.slice(0, 1), text.slice(1)]),
    ):
        encoded = variant.encode_json(values, on_invalid="null")
        assert variant.to_python(encoded.array) == [{"a": 1}, None, 2]
        assert encoded.report.invalid_by_reason == {"invalid_unicode": 1}
    with pytest.raises(VariantEncodingError) as raised:
        variant.encode_json(text)
    assert (raised.value.reason, raised.value.row) == ("invalid_unicode", 1)


def test_e10_the_message_names_the_column_row_and_path():
    column = Column("props", "variant", 7, 0, True)
    with pytest.raises(VariantEncodingError) as raised:
        variant.encode_json(['{"a":1}', '{"a":["\\ud800"]}'], column)
    error = raised.value
    assert str(error) == (
        "variant column 'props' row 1 at $.a[0]: a string holds an unpaired surrogate"
    )
    assert (error.column, error.status_code) == ("props", None)


# -- E11: empty containers (VE:244-353) ------------------------------------------


def test_e11_empty_object_and_array():
    assert value_bytes({}) == (b"\x01\x00\x00", b"\x02\x00\x00")
    assert value_bytes([]) == (b"\x01\x00\x00", b"\x03\x00\x00")
    assert (
        json_bytes('{"a":{},"b":[]}')[1].hex() == "02020001000306" + "020000" + "030000"
    )


def test_round_trip_of_every_vector():
    values = [value for value, _ in PRIMITIVES]
    got = variant.to_python(variant.encode_python(values).array)
    assert all(same(g, w) for g, w in zip(got, values, strict=True))


def test_arrow_input_kinds():
    texts = ['{"a":1}', None, "null"]
    want = [{"a": 1}, None, VARIANT_NULL]
    for values in (
        pa.array(texts, pa.json_()),
        pa.array(texts, pa.string()),
        pa.array(texts, pa.large_string()),
        pa.array(texts, pa.string_view()),
        pa.chunked_array(
            [pa.array(texts[:1], pa.json_()), pa.array(texts[1:], pa.json_())]
        ),
        texts,
        [t.encode() if t else t for t in texts],
    ):
        assert variant.to_python(variant.encode_json(values).array) == want
    assert variant.to_python(variant.encode_json(pa.nulls(2)).array) == [None, None]
