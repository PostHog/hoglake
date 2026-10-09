"""Suite P: apache/parquet-testing's Variant vectors, the decoder's ground truth.

The files are vendored unmodified under tests/data/parquet-testing (see its
NOTICE and tests/data/README.md for the commit). Spark wrote ``variant/``
(Iceberg's test code the types Spark lacks) and Iceberg wrote
``shredded_variant/``, so neither shares a line of code with pyhoglake:

- ``variant/``: every metadata/value pair decodes to the value
  ``data_dictionary.json`` lists, re-encodes to a value that decodes the
  same, and, for the allowlist below, to the same bytes;
- ``shredded_variant/``: every case reassembles to the variant of its
  ``.variant.bin`` files (decoded on their own, unshredded), every error
  case raises, and so does every case its notes call INVALID that this
  reader refuses (the notes let a reader refuse or read those).
"""

from __future__ import annotations

import base64
import json
import re
import struct
import uuid
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from variant_helpers import same, unshredded

from pyhoglake import VariantEncodingError, variant
from pyhoglake.variant import VARIANT_NULL

ROOT = Path(__file__).parents[1] / "data" / "parquet-testing"
VARIANT = ROOT / "variant"
SHREDDED = ROOT / "shredded_variant"

#: data_dictionary.json ends its object with a trailing comma, which a
#: strict parser refuses; the file is vendored as is.
_DICTIONARY = json.loads(
    re.sub(r",\s*}\s*$", "}", (VARIANT / "data_dictionary.json").read_text())
)
NAMES = sorted(path.stem for path in VARIANT.glob("*.value"))

#: The vectors whose bytes pyhoglake's encoder reproduces exactly from the
#: decoded value. The others are legal Variant bytes pyhoglake writes
#: differently, each for a reason of its policy:
#: - object_primitive, object_nested, array_nested: Spark's dictionaries are
#:   in insertion order with sorted_strings unset; pyhoglake sorts (D8);
#: - primitive_float: Python has no float32, so a float reads back as a
#:   float and is written as a double (D2);
#: - primitive_timestamp_nanos, primitive_timestampntz_nanos: Python's
#:   datetime has no nanoseconds, so pyhoglake writes neither type (D2).
BYTE_EQUAL = set(NAMES) - {
    "object_primitive",
    "object_nested",
    "array_nested",
    "primitive_float",
    "primitive_timestamp_nanos",
    "primitive_timestampntz_nanos",
}


def test_the_vendored_directories_are_complete():
    # A silently shrunken copy must not pass: 29 pairs and 138 cases.
    assert len(NAMES) == 29
    assert set(_DICTIONARY) == set(NAMES) - {"long_string"}
    assert len(json.loads((SHREDDED / "cases.json").read_text())) == 138


def _stored(name: str) -> tuple[bytes, bytes]:
    return (VARIANT / f"{name}.metadata").read_bytes(), (
        VARIANT / f"{name}.value"
    ).read_bytes()


def _agrees(got: object, want: object, *, top: bool = True) -> bool:
    """Whether a decoded value is the one ``data_dictionary.json`` lists.

    The dictionary is lossy JSON: decimals as doubles, a float32 as the
    double nearest its decimal text, temporal values as text in several
    formats, binary as base64. Each entry is read as the type that was
    decoded, and compared exactly where the text allows it."""
    if want is None:
        return got is (VARIANT_NULL if top else None)
    if isinstance(want, dict):
        return (
            isinstance(got, dict)
            and set(got) == set(want)
            and all(_agrees(got[k], want[k], top=False) for k in want)
        )
    if isinstance(want, list):
        return (
            isinstance(got, list)
            and len(got) == len(want)
            and all(_agrees(g, w, top=False) for g, w in zip(got, want, strict=True))
        )
    if isinstance(got, Decimal):
        return float(got) == want
    if isinstance(got, float) and not isinstance(want, bool):
        # A float32 is printed with the fewest digits that read back to it.
        return got == want or got == struct.unpack("<f", struct.pack("<f", want))[0]
    if isinstance(got, bytes):
        return got == base64.b64decode(want)
    if isinstance(got, datetime):
        return got == datetime.fromisoformat(want)
    if isinstance(got, date):
        return got == date.fromisoformat(want)
    if isinstance(got, time):
        hours, minutes, seconds, micros = map(int, want.split(":"))
        return got == time(hours, minutes, seconds, micros)
    if isinstance(got, uuid.UUID):
        return got == uuid.UUID(want)
    if isinstance(got, pa.Scalar):
        # 2024-11-07T12:33:54.123456789, then +00:00 when adjusted to UTC.
        whole, fraction, offset = want[:19], want[20:29], want[29:]
        seconds = (datetime.fromisoformat(whole) - datetime(1970, 1, 1)) // timedelta(
            seconds=1
        )
        expected = pa.scalar(
            seconds * 10**9 + int(fraction),
            pa.timestamp("ns", tz="UTC" if offset == "+00:00" else None),
        )
        return got == expected and got.type == expected.type
    return type(got) is type(want) and got == want


#: The decimals exactly, which the dictionary's doubles cannot say.
_EXACT = {
    "primitive_decimal4": Decimal("12.34"),
    "primitive_decimal8": Decimal("12345678.90"),
    "primitive_decimal16": Decimal("12345678912345678.90"),
}


@pytest.mark.parametrize("name", NAMES)
def test_variant_vectors_decode_to_the_dictionary(name):
    metadata, value = _stored(name)
    (got,) = variant.to_python(unshredded(metadata, value))
    if name == "long_string":
        # The one pair the dictionary leaves out: a long string (type 16)
        # whose bytes follow a 4-byte length.
        assert (
            value[0] == 0x40 and int.from_bytes(value[1:5], "little") == len(value) - 5
        )
        assert got == value[5:].decode("utf-8")
    else:
        assert _agrees(got, _DICTIONARY[name]), (got, _DICTIONARY[name])
    if name in _EXACT:
        assert same(got, _EXACT[name])
    variant.verify(unshredded(metadata, value), canonical=False)


@pytest.mark.parametrize("name", NAMES)
def test_variant_vectors_round_trip_through_the_encoder(name):
    metadata, value = _stored(name)
    (decoded,) = variant.to_python(unshredded(metadata, value))
    if isinstance(decoded, pa.Scalar):
        # Nanosecond timestamps have no Python type the encoder takes.
        with pytest.raises(VariantEncodingError, match="TimestampScalar"):
            variant.encode_python([decoded])
        return
    encoded = variant.encode_python([decoded]).array
    variant.verify(encoded)
    (again,) = variant.to_python(encoded)
    assert same(again, decoded)
    stored = encoded.to_pylist()[0]
    if name in BYTE_EQUAL:
        assert (stored["metadata"], stored["value"]) == (metadata, value)
    else:
        assert (stored["metadata"], stored["value"]) != (metadata, value)


def test_float_and_nanosecond_vectors_are_not_canonical():
    # verify's canonical rules are pyhoglake's, not the spec's.
    for name in (
        "primitive_float",
        "primitive_timestamp_nanos",
        "primitive_timestampntz_nanos",
        "object_nested",
    ):
        with pytest.raises(VariantEncodingError) as raised:
            variant.verify(unshredded(*_stored(name)))
        assert raised.value.reason == "non_canonical"


# -- shredded_variant ------------------------------------------------------------

CASES = [
    case
    for case in json.loads((SHREDDED / "cases.json").read_text())
    if "parquet_file" in case
]

#: INVALID files (their notes say a reader may refuse or read them) this
#: reader reads: field groups that are OPTIONAL, which is DuckDB's layout,
#: with one of them null, read as a missing field. verify refuses it.
READ_INVALID = {84}

#: Valid files whose reading turns on the spec's rule for readers, that a
#: missing value where one is required is a Variant null (VS:78): an array
#: element with both columns null, and a row with both null. Writers must
#: not produce them (VS:145), so verify refuses them.
READER_RULE = {85: "an array element is missing", 129: "neither value nor typed_value"}


def _variant_bin(name: str | None) -> object:
    """A .variant.bin file: the metadata, then the value, unshredded."""
    if name is None:
        return None
    data = (SHREDDED / name).read_bytes()
    width = (data[0] >> 6) + 1
    count = int.from_bytes(data[1 : 1 + width], "little")
    start = 1 + width
    last = int.from_bytes(
        data[start + count * width : start + (count + 1) * width], "little"
    )
    split = start + (count + 1) * width + last
    (value,) = variant.to_python(unshredded(data[:split], data[split:]))
    return value


def _ids(case: dict) -> str:
    return f"{case['case_number']:03d}-{case['test']}"


#: pyarrow's ways of reading Parquet's binary and lists, each read alike:
#: string and binary leaves come back as views under binary_view.
READ_OPTIONS = {
    "default": {},
    "large-binary": {"binary_type": pa.large_binary()},
    "binary-view": {"binary_type": pa.binary_view()},
    "large-list": {"list_type": pa.LargeListType},
}


@pytest.mark.parametrize("options", READ_OPTIONS.values(), ids=READ_OPTIONS.keys())
@pytest.mark.parametrize("case", CASES, ids=_ids)
def test_shredded_cases(case, options):
    column = pq.read_table(SHREDDED / case["parquet_file"], **options).column("var")
    number = case["case_number"]
    if "error_message" in case or ("notes" in case and number not in READ_INVALID):
        with pytest.raises(VariantEncodingError) as raised:
            variant.to_python(column)
        assert raised.value.reason == "invalid_shredding"
        return
    files = case.get("variant_files") or [case["variant_file"]]
    expected = [_variant_bin(name) for name in files]
    got = variant.to_python(column)
    assert len(got) == len(expected)
    for g, w in zip(got, expected, strict=True):
        if isinstance(w, pa.Scalar):
            assert g == w and g.type == w.type
        else:
            assert same(g, w), (g, w)
    if number in READ_INVALID:
        with pytest.raises(VariantEncodingError, match="group is null"):
            variant.verify(column, canonical=False)
    elif number in READER_RULE:
        with pytest.raises(VariantEncodingError, match=READER_RULE[number]):
            variant.verify(column, canonical=False)
    else:
        variant.verify(column, canonical=False)


def test_every_error_case_is_covered():
    errors = {case["case_number"] for case in CASES if "error_message" in case}
    invalid = {case["case_number"] for case in CASES if "notes" in case}
    assert errors == {40, 42, 87, 127, 128, 137}
    assert invalid == {41, 43, 84, 125, 131, 132, 138}
    assert READ_INVALID <= invalid


def test_the_write_form_of_a_decimal_leaf_reads_by_its_type():
    # Parquet files carry DECIMAL(p, s) on a decimal8 leaf, so pyarrow reads
    # decimal128 (shredded_variant has such cases). The write form is a
    # decimal64 over the unscaled int64 the file holds, declaration or not.
    decl = {"type": "decimal8", "precision": 18, "scale": 9}
    encoded = variant.encode_python([Decimal("1.123456789")], shredding=decl).array
    leaf = encoded.field("typed_value")
    assert leaf.type == pa.decimal64(18, 9)
    assert leaf.view(pa.int64()).to_pylist() == [1_123_456_789]
    assert variant.to_python(encoded, shredding=decl) == [Decimal("1.123456789")]
    assert variant.to_python(encoded) == [Decimal("1.123456789")]
