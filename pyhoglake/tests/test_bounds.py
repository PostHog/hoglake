"""Byte-pattern tests for the Iceberg single-value binary codec."""

import struct
import uuid
from datetime import UTC, date, datetime, time, timedelta, timezone
from decimal import Decimal

import pytest

from pyhoglake import decode_bound, encode_bound


def test_boolean():
    assert encode_bound("boolean", True) == b"\x01"
    assert encode_bound("boolean", False) == b"\x00"
    assert decode_bound("boolean", b"\x01") is True
    assert decode_bound("boolean", b"\x00") is False


def test_int_le():
    assert encode_bound("int", 1) == b"\x01\x00\x00\x00"
    assert encode_bound("int", -1) == b"\xff\xff\xff\xff"
    assert encode_bound("int", 2**31 - 1) == b"\xff\xff\xff\x7f"
    assert decode_bound("int", b"\x01\x00\x00\x00") == 1
    assert decode_bound("int", b"\xff\xff\xff\xff") == -1


def test_long_le():
    assert encode_bound("long", 1) == b"\x01" + b"\x00" * 7
    assert encode_bound("long", -1) == b"\xff" * 8
    assert decode_bound("long", encode_bound("long", -(2**62))) == -(2**62)


# -- narrow integer widths: all four map to Iceberg int -> 4 bytes ---------


@pytest.mark.parametrize(
    "col_type,value",
    [
        ("int8", 127),
        ("int8", -128),
        ("int8", 0),
        ("int16", 32767),
        ("int16", -32768),
        ("uint8", 0),
        ("uint8", 255),
        ("uint16", 0),
        ("uint16", 65535),
    ],
)
def test_narrow_ints_encode_as_4_byte_iceberg_int(col_type, value):
    enc = encode_bound(col_type, value)
    assert enc == struct.pack("<i", value)
    assert len(enc) == 4
    assert decode_bound(col_type, enc) == value


def test_unsigned_narrow_ints_never_sign_wrap():
    # 0x80/0x8000 are negative as int8/int16 but positive as uint8/uint16;
    # the 4-byte Iceberg int holds both without ambiguity
    assert encode_bound("uint8", 128) == b"\x80\x00\x00\x00"
    assert encode_bound("int8", -128) == b"\x80\xff\xff\xff"
    assert encode_bound("uint16", 32768) == b"\x00\x80\x00\x00"
    assert encode_bound("int16", -32768) == b"\x00\x80\xff\xff"


def test_uint32_encodes_as_8_byte_iceberg_long():
    # 32 unsigned bits do not fit a signed int32, so uint32 maps to long
    for v in (0, 2**31, 2**32 - 1):
        enc = encode_bound("uint32", v)
        assert enc == struct.pack("<q", v)
        assert len(enc) == 8
        assert decode_bound("uint32", enc) == v


def test_uint64_encodes_as_decimal_20_0_unscaled():
    # maps to Iceberg decimal(20, 0): minimal big-endian two's complement
    assert encode_bound("uint64", 0) == b"\x00"
    assert encode_bound("uint64", 127) == b"\x7f"
    assert encode_bound("uint64", 128) == b"\x00\x80"  # sign byte appears
    # above 2^63 the minimal form is 9 bytes, never a wrapped negative
    assert encode_bound("uint64", 2**63) == b"\x00\x80" + b"\x00" * 7
    assert encode_bound("uint64", 2**64 - 1) == b"\x00" + b"\xff" * 8
    for v in (0, 1, 2**63 - 1, 2**63, 2**64 - 1):
        assert decode_bound("uint64", encode_bound("uint64", v)) == v


def test_float_double():
    assert encode_bound("float", 1.0) == struct.pack("<f", 1.0)
    assert encode_bound("double", -2.5) == struct.pack("<d", -2.5)
    assert decode_bound("float", struct.pack("<f", 1.5)) == 1.5
    assert decode_bound("double", struct.pack("<d", -2.5)) == -2.5


def test_date_days_since_epoch():
    assert encode_bound("date", date(1970, 1, 1)) == b"\x00\x00\x00\x00"
    assert encode_bound("date", date(1970, 1, 2)) == b"\x01\x00\x00\x00"
    assert encode_bound("date", date(1969, 12, 31)) == b"\xff\xff\xff\xff"
    assert encode_bound("date", 17532) == struct.pack("<i", 17532)
    assert decode_bound("date", struct.pack("<i", 17532)) == date(2018, 1, 1)


def test_time_micros():
    assert encode_bound("time", time(0, 0, 0)) == b"\x00" * 8
    one_hour = 3600 * 1_000_000
    assert encode_bound("time", time(1, 0, 0)) == struct.pack("<q", one_hour)
    t = time(13, 37, 42, 123456)
    assert decode_bound("time", encode_bound("time", t)) == t


def test_timestamp_micros():
    dt = datetime(2026, 9, 4, 12, 0, 0, 500)
    micros = int((dt - datetime(1970, 1, 1)) / timedelta(microseconds=1))
    assert encode_bound("timestamp", dt) == struct.pack("<q", micros)
    assert encode_bound("timestamp", micros) == struct.pack("<q", micros)
    assert decode_bound("timestamp", struct.pack("<q", micros)) == dt


@pytest.mark.parametrize("col_type", ["timestamp_s", "timestamp_ms"])
def test_timestamp_seconds_and_millis_bounds_are_micros(col_type):
    """Declared precision is catalog metadata; both map to Iceberg
    timestamp, whose single-value unit is MICROseconds. So these encode
    byte-for-byte like "timestamp" — an int is micros, not seconds."""
    dt = datetime(2026, 9, 4, 12, 0, 0, 500)
    micros = int((dt - datetime(1970, 1, 1)) / timedelta(microseconds=1))
    assert encode_bound(col_type, dt) == struct.pack("<q", micros)
    assert encode_bound(col_type, micros) == struct.pack("<q", micros)
    assert encode_bound(col_type, micros) == encode_bound("timestamp", micros)
    assert decode_bound(col_type, struct.pack("<q", micros)) == dt


def test_timestamp_nanos_bounds_are_nanos():
    """Iceberg V3 timestamp_ns: the stored unit is NANOseconds, the one
    temporal type that is not micros."""
    nanos = 1788609600123456789
    assert encode_bound("timestamp_ns", nanos) == struct.pack("<q", nanos)
    # a datetime carries at most micros, so scaling it is exact
    dt = datetime(2026, 9, 4, 12, 0, 0, 500)
    micros = int((dt - datetime(1970, 1, 1)) / timedelta(microseconds=1))
    assert encode_bound("timestamp_ns", dt) == struct.pack("<q", micros * 1000)


def test_timestamp_nanos_decodes_to_an_int_not_a_datetime():
    """datetime tops out at microseconds: returning one would round the
    sub-micro digits away and break encode(decode(b)) == b."""
    nanos = 1788609600123456789
    enc = struct.pack("<q", nanos)
    assert decode_bound("timestamp_ns", enc) == nanos
    assert encode_bound("timestamp_ns", decode_bound("timestamp_ns", enc)) == enc


def test_json_encodes_exactly_like_string():
    """json maps to Iceberg string: the document bytes verbatim, never
    re-canonicalized (no key sorting, no whitespace stripping)."""
    doc = '{ "b":1,  "a":"héllo" }'
    assert encode_bound("json", doc) == doc.encode("utf-8")
    assert encode_bound("json", doc) == encode_bound("string", doc)
    assert decode_bound("json", doc.encode("utf-8")) == doc
    # bytes pass through untouched, like the string branch
    assert encode_bound("json", b"{}") == b"{}"


def test_timestamptz_converts_to_utc():
    tz = timezone(timedelta(hours=-5))
    dt = datetime(2026, 9, 4, 7, 0, 0, tzinfo=tz)  # == 12:00 UTC
    utc = datetime(2026, 9, 4, 12, 0, 0, tzinfo=UTC)
    assert encode_bound("timestamptz", dt) == encode_bound("timestamptz", utc)
    assert decode_bound("timestamptz", encode_bound("timestamptz", dt)) == utc


def test_string_utf8():
    assert encode_bound("string", "ab") == b"\x61\x62"
    assert encode_bound("string", "héllo") == "héllo".encode()
    assert decode_bound("string", b"\x61\x62") == "ab"


def test_uuid_big_endian():
    u = uuid.UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7")
    enc = encode_bound("uuid", u)
    assert enc == u.bytes
    assert enc[0] == 0xF7  # big-endian: MSB of the hex form first
    assert encode_bound("uuid", str(u)) == u.bytes
    assert decode_bound("uuid", enc) == u


def test_binary_raw():
    assert encode_bound("binary", b"\x00\x01\xff") == b"\x00\x01\xff"
    assert decode_bound("binary", b"\x00\x01\xff") == b"\x00\x01\xff"


@pytest.mark.parametrize(
    "unscaled,expected",
    [
        (0, b"\x00"),
        (1, b"\x01"),
        (127, b"\x7f"),
        (128, b"\x00\x80"),  # needs the sign byte
        (255, b"\x00\xff"),
        (256, b"\x01\x00"),
        (-1, b"\xff"),
        (-128, b"\x80"),  # fits in one byte
        (-129, b"\xff\x7f"),
        (1234, b"\x04\xd2"),
        (-1234, b"\xfb\x2e"),
    ],
)
def test_decimal_minimal_twos_complement(unscaled, expected):
    assert encode_bound("decimal", unscaled, {"scale": 2}) == expected


def test_decimal_from_decimal_value():
    assert (
        encode_bound("decimal", Decimal("12.34"), {"precision": 10, "scale": 2})
        == b"\x04\xd2"
    )
    assert (
        encode_bound("decimal", Decimal("-1.00"), {"precision": 10, "scale": 2})
        == b"\x9c"
    )
    assert decode_bound("decimal", b"\x04\xd2", {"scale": 2}) == Decimal("12.34")


def test_decimal_scale_mismatch_raises():
    with pytest.raises(ValueError):
        encode_bound("decimal", Decimal("1.234"), {"scale": 2})
