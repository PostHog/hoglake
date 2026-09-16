"""Property-based fuzzing of the Iceberg single-value bounds codec.

Pinned policies (verified here, and load-bearing for the JVM port):

* NaN: the codec neither rejects nor canonicalizes NaN. float/double
  encode whatever IEEE-754 bits the value carries (payload preserved,
  including for float32) and decode returns a NaN with those bits.
  NaN-exclusion is the STATS layer's job (parquet-cpp already excludes
  NaN from min/max) — see qe_prop_stats.py.
* Negative zero: sign bit is preserved bit-exactly in both widths.
* float32: encode rounds the Python double to the nearest float32
  (struct semantics); decode returns exactly that float32 value.
* Strings: UTF-8; lone surrogates are rejected at encode time with
  UnicodeEncodeError (a ValueError subclass); decode of invalid UTF-8
  raises UnicodeDecodeError. Astral-plane text round-trips.
* Fixed widths: boolean=1, int/int8/int16/uint8/uint16/float/date=4,
  long/uint32/double/time/timestamp_s/timestamp_ms/timestamp/
  timestamp_ns/timestamptz=8, uuid=16 bytes — always. uint64 and json
  are variable-width by construction.
* Totality: the codec never range-checks. A uint8 bound holding 300
  encodes and decodes without complaint, because a decoder that threw
  would be un-invertible for hostile footers. Domain policing belongs to
  the writer, and the Kotlin codec makes the same choice.
* Decimal: minimal-length big-endian two's-complement unscaled value.
  Encoding is exact and canonical (no redundant sign-extension byte).
  Decode of unscaled values wider than the default decimal-context
  precision (28 digits) once silently rounded; encode and decode now
  both run under a widened localcontext, pinned by regressions below.
* Out-of-range ints (int/long) raise struct.error at encode.
"""

import math
import struct
import uuid as _uuid
from datetime import UTC, date, datetime, time, timedelta, timezone
from decimal import Decimal, localcontext

import pytest
from hypothesis import given
from hypothesis import strategies as st

from pyhoglake import decode_bound, encode_bound

# The "qe" hypothesis profile (deadline=None) is loaded in conftest.py.

INT32_MIN, INT32_MAX = -(2**31), 2**31 - 1
INT64_MIN, INT64_MAX = -(2**63), 2**63 - 1


def f32(x: float) -> float:
    return struct.unpack("<f", struct.pack("<f", x))[0]


def bits64(x: float) -> bytes:
    return struct.pack("<d", x)


# -- fixed-width integer types ---------------------------------------------


@given(st.integers(INT32_MIN, INT32_MAX))
def test_int_roundtrip_and_width(v):
    enc = encode_bound("int", v)
    assert len(enc) == 4
    assert decode_bound("int", enc) == v


@given(st.integers(INT64_MIN, INT64_MAX))
def test_long_roundtrip_and_width(v):
    enc = encode_bound("long", v)
    assert len(enc) == 8
    assert decode_bound("long", enc) == v


@given(
    st.one_of(
        st.integers(max_value=INT32_MIN - 1), st.integers(min_value=INT32_MAX + 1)
    )
)
def test_int_out_of_range_raises(v):
    with pytest.raises(struct.error):
        encode_bound("int", v)


@given(
    st.one_of(
        st.integers(max_value=INT64_MIN - 1), st.integers(min_value=INT64_MAX + 1)
    )
)
def test_long_out_of_range_raises(v):
    with pytest.raises((struct.error, OverflowError)):
        encode_bound("long", v)


# -- the narrow / unsigned integer widths ----------------------------------

#: (coltype, low, high) over each type's own domain.
_INT_DOMAINS = [
    ("int8", -(2**7), 2**7 - 1),
    ("int16", -(2**15), 2**15 - 1),
    ("uint8", 0, 2**8 - 1),
    ("uint16", 0, 2**16 - 1),
]


@given(st.data())
def test_narrow_int_roundtrip_and_width(data):
    col_type, lo, hi = data.draw(st.sampled_from(_INT_DOMAINS))
    v = data.draw(st.integers(lo, hi))
    enc = encode_bound(col_type, v)
    assert len(enc) == 4  # Iceberg int, whatever the declared width
    assert enc == encode_bound("int", v)  # identical bytes to plain int
    assert decode_bound(col_type, enc) == v


@given(st.sampled_from([d[0] for d in _INT_DOMAINS]), st.integers(INT32_MIN, INT32_MAX))
def test_narrow_int_codec_is_total_over_int32(col_type, v):
    """Pinned policy: NO domain validation. A uint8 bound holding 300
    encodes and decodes cleanly — range enforcement is the writer's job,
    and a throwing decoder could not invert a hostile footer."""
    assert decode_bound(col_type, encode_bound(col_type, v)) == v


@given(st.integers(0, 2**32 - 1))
def test_uint32_roundtrip_as_long(v):
    enc = encode_bound("uint32", v)
    assert len(enc) == 8  # maps to Iceberg long, not int
    assert enc == encode_bound("long", v)
    assert decode_bound("uint32", enc) == v


@given(st.integers(0, 2**64 - 1))
def test_uint64_roundtrip_as_decimal_unscaled(v):
    enc = encode_bound("uint64", v)
    # decimal(20, 0) minimal two's complement: 9 bytes only once the
    # value needs a 0x00 sign byte, i.e. from 2^63 up
    assert 1 <= len(enc) <= 9
    assert (len(enc) == 9) == (v >= 2**63)
    assert enc == encode_bound("decimal", v, {"scale": 0})
    assert decode_bound("uint64", enc) == v


# -- floats: full domain incl. nan / inf / subnormal / -0.0 ----------------


@given(st.floats(width=32, allow_nan=True, allow_infinity=True, allow_subnormal=True))
def test_float_roundtrip_bit_exact(v):
    enc = encode_bound("float", v)
    assert len(enc) == 4
    dec = decode_bound("float", enc)
    # decode must yield exactly the float32 rounding of v (bitwise:
    # covers nan payloads and -0.0, where == is the wrong question)
    assert struct.pack("<f", dec) == struct.pack("<f", f32(v))


@given(st.floats(allow_nan=True, allow_infinity=True, allow_subnormal=True))
def test_double_roundtrip_bit_exact(v):
    enc = encode_bound("double", v)
    assert len(enc) == 8
    dec = decode_bound("double", enc)
    assert bits64(dec) == bits64(v)


def test_nan_policy_passthrough():
    """NaN policy: encode accepts NaN, bits (incl. payload) preserved."""
    payload_nan = struct.unpack("<d", b"\x39\x05\x00\x00\x00\x00\xf8\x7f")[0]
    assert math.isnan(payload_nan)
    enc = encode_bound("double", payload_nan)
    assert enc == b"\x39\x05\x00\x00\x00\x00\xf8\x7f"
    assert math.isnan(decode_bound("double", enc))

    f_nan = struct.unpack("<f", b"\x01\x00\xc0\x7f")[0]
    assert encode_bound("float", f_nan) == b"\x01\x00\xc0\x7f"


def test_negative_zero_sign_preserved():
    assert encode_bound("double", -0.0) == b"\x00" * 7 + b"\x80"
    assert encode_bound("float", -0.0) == b"\x00\x00\x00\x80"
    assert (
        math.copysign(1.0, decode_bound("double", encode_bound("double", -0.0))) == -1.0
    )


def test_infinities():
    assert decode_bound("float", encode_bound("float", math.inf)) == math.inf
    assert decode_bound("double", encode_bound("double", -math.inf)) == -math.inf


def test_min_subnormals():
    tiny64 = 5e-324
    assert decode_bound("double", encode_bound("double", tiny64)) == tiny64
    tiny32 = struct.unpack("<f", b"\x01\x00\x00\x00")[0]
    assert decode_bound("float", encode_bound("float", tiny32)) == tiny32


# -- boolean ----------------------------------------------------------------


@given(st.booleans())
def test_boolean_roundtrip(v):
    enc = encode_bound("boolean", v)
    assert enc == (b"\x01" if v else b"\x00")
    assert decode_bound("boolean", enc) is v


def test_boolean_decode_of_garbage_is_lenient():
    # Pinned wart: decode treats anything != b"\x00" as True, including
    # the empty payload and multi-byte garbage. Encode never emits these,
    # so this is decode-of-invalid-input leniency, not a roundtrip hole.
    assert decode_bound("boolean", b"") is True
    assert decode_bound("boolean", b"\x02") is True
    assert decode_bound("boolean", b"\x00\x00") is True


# -- date / time / timestamp -----------------------------------------------


@given(st.dates())
def test_date_roundtrip_full_domain(v):
    enc = encode_bound("date", v)
    assert len(enc) == 4
    assert decode_bound("date", enc) == v


@given(
    st.integers((date.min - date(1970, 1, 1)).days, (date.max - date(1970, 1, 1)).days)
)
def test_date_int_passthrough(days):
    assert decode_bound("date", encode_bound("date", days)) == date(
        1970, 1, 1
    ) + timedelta(days=days)


def test_date_decode_beyond_pydate_overflows():
    # Pinned: 4-byte payloads beyond datetime.date's range raise
    # OverflowError at decode (encode of an int passes through).
    with pytest.raises(OverflowError):
        decode_bound("date", struct.pack("<i", INT32_MAX))


@given(st.times())
def test_time_roundtrip_full_domain(v):
    v = v.replace(tzinfo=None)
    enc = encode_bound("time", v)
    assert len(enc) == 8
    assert decode_bound("time", enc) == v


def test_time_decode_out_of_day_raises():
    with pytest.raises(ValueError):
        decode_bound("time", struct.pack("<q", 24 * 3600 * 1_000_000))
    with pytest.raises(ValueError):
        decode_bound("time", encode_bound("time", -1))


@given(
    st.datetimes(
        min_value=datetime(1, 1, 1),
        max_value=datetime(9999, 12, 31, 23, 59, 59, 999999),
    )
)
def test_timestamp_roundtrip_full_domain(v):
    enc = encode_bound("timestamp", v)
    assert len(enc) == 8
    assert decode_bound("timestamp", enc) == v


@given(
    st.datetimes(
        min_value=datetime(2, 1, 1),
        max_value=datetime(9998, 12, 31),
        timezones=st.sampled_from(
            [
                UTC,
                timezone(timedelta(hours=-12)),
                timezone(timedelta(hours=14)),
                timezone(timedelta(minutes=331)),  # weird +05:31 offset
            ]
        ),
    )
)
def test_timestamptz_roundtrip_normalizes_to_utc(v):
    enc = encode_bound("timestamptz", v)
    assert len(enc) == 8
    dec = decode_bound("timestamptz", enc)
    assert dec.tzinfo == UTC
    assert dec == v  # aware comparison: same instant


@given(st.integers(-(2**60), 2**60))
def test_timestamp_micros_passthrough_encoding(micros):
    assert encode_bound("timestamp", micros) == struct.pack("<q", micros)
    assert encode_bound("timestamptz", micros) == struct.pack("<q", micros)


def test_timestamp_decode_past_datetime_max_returns_raw_micros():
    # Pinned: encode accepts any int64 micros, and decode of an instant
    # beyond datetime.max falls back to the raw micros rather than
    # raising. Python's calendar stops at year 9999 (~2.5e17 micros)
    # while the bound domain is the whole int64 (~9.2e18) — refusing
    # there would make the top of our own domain undecodable in one
    # language only, since Kotlin decodes it to a plain Long.
    raw = struct.pack("<q", 2**62)
    assert decode_bound("timestamp", raw) == 2**62
    # And the round trip still closes, which is the property that matters.
    assert encode_bound("timestamp", decode_bound("timestamp", raw)) == raw


@given(
    st.sampled_from(["timestamp_s", "timestamp_ms"]),
    st.datetimes(
        min_value=datetime(1, 1, 1),
        max_value=datetime(9999, 12, 31, 23, 59, 59, 999999),
    ),
)
def test_timestamp_seconds_millis_encode_identically_to_timestamp(col_type, v):
    """The declared precision is metadata: all three map to Iceberg
    timestamp, so all three store MICROS and must be byte-identical.
    Anything else would make the same instant prune differently
    depending on which width the column was declared with."""
    enc = encode_bound(col_type, v)
    assert len(enc) == 8
    assert enc == encode_bound("timestamp", v)
    assert decode_bound(col_type, enc) == v


@given(st.sampled_from(["timestamp_s", "timestamp_ms"]), st.integers(-(2**60), 2**60))
def test_timestamp_seconds_millis_int_passthrough_is_micros(col_type, micros):
    # a passed int is ALWAYS the stored unit, never the declared one
    assert encode_bound(col_type, micros) == struct.pack("<q", micros)


@given(st.integers(INT64_MIN, INT64_MAX))
def test_timestamp_nanos_roundtrip_is_int_exact(nanos):
    enc = encode_bound("timestamp_ns", nanos)
    assert len(enc) == 8
    # decode returns nanos as an int: a datetime cannot hold them, and
    # rounding to micros would break encode(decode(b)) == b
    assert decode_bound("timestamp_ns", enc) == nanos
    assert encode_bound("timestamp_ns", decode_bound("timestamp_ns", enc)) == enc


@given(
    st.datetimes(
        min_value=datetime(1678, 1, 1),  # int64 nanos spans ~1678..2262
        max_value=datetime(2261, 12, 31, 23, 59, 59, 999999),
    )
)
def test_timestamp_nanos_from_datetime_scales_exactly(v):
    # a datetime carries whole micros only, so *1000 is exact, not a
    # guess. Floor-divide, never `/`: true division goes through a float
    # and loses digits past 2^53 micros (~year 2255).
    micros = (v - datetime(1970, 1, 1)) // timedelta(microseconds=1)
    assert encode_bound("timestamp_ns", v) == struct.pack("<q", micros * 1000)


# -- json -------------------------------------------------------------------


@given(st.text())
def test_json_is_byte_identical_to_string(v):
    """json maps to Iceberg string. Same bytes, no canonicalization — two
    documents equal as JSON but written differently stay different
    bounds, which is exactly why json takes no bucket/truncate."""
    enc = encode_bound("json", v)
    assert enc == encode_bound("string", v)
    assert enc == v.encode("utf-8")
    assert decode_bound("json", enc) == v


# -- string -----------------------------------------------------------------


@given(st.text())  # hypothesis text() excludes surrogates; includes astral
def test_string_roundtrip(v):
    enc = encode_bound("string", v)
    assert enc == v.encode("utf-8")
    assert decode_bound("string", enc) == v


@given(
    st.text(
        alphabet=st.characters(min_codepoint=0x10000, max_codepoint=0x10FFFF),
        min_size=1,
    )
)
def test_string_astral_planes_roundtrip(v):
    assert decode_bound("string", encode_bound("string", v)) == v


@given(st.integers(0xD800, 0xDFFF))
def test_string_lone_surrogates_rejected(cp):
    with pytest.raises(UnicodeEncodeError):
        encode_bound("string", chr(cp))


def test_string_decode_invalid_utf8_raises():
    with pytest.raises(UnicodeDecodeError):
        decode_bound("string", b"\xff\xfe")


def test_string_empty_is_empty_payload():
    assert encode_bound("string", "") == b""
    assert decode_bound("string", b"") == ""


def test_string_bytes_input_passes_through_unvalidated():
    # Pinned wart: encode accepts bytes verbatim without UTF-8
    # validation, so encode(b"\xff") produces a payload decode rejects.
    assert encode_bound("string", b"\xff") == b"\xff"
    with pytest.raises(UnicodeDecodeError):
        decode_bound("string", encode_bound("string", b"\xff"))


# -- uuid -------------------------------------------------------------------


@given(st.binary(min_size=16, max_size=16))
def test_uuid_roundtrip_all_bit_patterns(raw):
    u = _uuid.UUID(bytes=raw)
    enc = encode_bound("uuid", u)
    assert enc == raw and len(enc) == 16
    assert decode_bound("uuid", enc) == u
    # str and raw-bytes inputs agree with the UUID input
    assert encode_bound("uuid", str(u)) == enc
    assert encode_bound("uuid", raw) == enc


@given(st.binary(max_size=32).filter(lambda b: len(b) != 16))
def test_uuid_wrong_length_rejected(raw):
    with pytest.raises(ValueError):
        encode_bound("uuid", raw)


def test_uuid_edge_patterns():
    for raw in (b"\x00" * 16, b"\xff" * 16, b"\x80" + b"\x00" * 15):
        assert encode_bound("uuid", _uuid.UUID(bytes=raw)) == raw


# -- binary -----------------------------------------------------------------


@given(st.binary(max_size=256))
def test_binary_roundtrip(v):
    assert decode_bound("binary", encode_bound("binary", v)) == v


def test_binary_empty():
    assert encode_bound("binary", b"") == b""
    assert decode_bound("binary", b"") == b""


# -- decimal ----------------------------------------------------------------


def _minimal(b: bytes) -> bool:
    if len(b) <= 1:
        return True
    if b[0] == 0x00 and b[1] < 0x80:
        return False  # redundant leading zero
    # not a redundant sign extension
    return not (b[0] == 0xFF and b[1] >= 0x80)


@given(st.integers(-(10**38) + 1, 10**38 - 1), st.integers(0, 38))
def test_decimal_encode_exact_and_minimal(unscaled, scale):
    enc = encode_bound("decimal", unscaled, {"scale": scale})
    assert int.from_bytes(enc, "big", signed=True) == unscaled
    assert _minimal(enc)
    assert len(enc) <= 17  # precision-38 unscaled fits 16 bytes + sign slack


@given(st.integers(-(10**27), 10**27), st.integers(0, 10))
def test_decimal_roundtrip_within_default_context(unscaled, scale):
    enc = encode_bound("decimal", unscaled, {"scale": scale})
    with localcontext() as ctx:
        ctx.prec = 60
        expected = Decimal(unscaled).scaleb(-scale)
    assert decode_bound("decimal", enc, {"scale": scale}) == expected


# Regression (fixed 2026-09-05): >28-digit unscaled values were silently
# rounded by the ambient decimal context on decode.
@given(st.integers(10**28, 10**38 - 1), st.integers(0, 38))
def test_decimal_decode_wide_unscaled_is_exact_regression(unscaled, scale):
    enc = encode_bound("decimal", unscaled, {"scale": scale})
    with localcontext() as ctx:
        ctx.prec = 60
        expected = Decimal(unscaled).scaleb(-scale)
    assert decode_bound("decimal", enc, {"scale": scale}) == expected
    assert decode_bound("decimal", enc, {"scale": 0}) == unscaled


def test_decimal_precision38_boundary_values_encode():
    hi = 10**38 - 1
    assert len(encode_bound("decimal", hi, {"scale": 0})) == 16
    assert len(encode_bound("decimal", -hi, {"scale": 0})) == 16
    # canonical sign-byte boundary
    assert encode_bound("decimal", 128, {"scale": 0}) == b"\x00\x80"
    assert encode_bound("decimal", -128, {"scale": 0}) == b"\x80"
    assert encode_bound("decimal", -129, {"scale": 0}) == b"\xff\x7f"


@given(
    st.decimals(
        allow_nan=False,
        allow_infinity=False,
        places=2,
        min_value=Decimal("-1e20"),
        max_value=Decimal("1e20"),
    )
)
def test_decimal_from_decimal_value_roundtrip(v):
    enc = encode_bound("decimal", v, {"precision": 25, "scale": 2})
    assert decode_bound("decimal", enc, {"scale": 2}) == v


def test_decimal_scale_overflow_rejected():
    with pytest.raises(ValueError):
        encode_bound("decimal", Decimal("0.001"), {"scale": 2})


# -- canonical byte-level second-preimage sanity ---------------------------


_FIXED_WIDTHS = {
    "int8": 4,
    "int16": 4,
    "int": 4,
    "uint8": 4,
    "uint16": 4,
    "uint32": 8,
    "long": 8,
    "float": 4,
    "double": 8,
    "date": 4,
    "time": 8,
    "timestamp_s": 8,
    "timestamp_ms": 8,
    "timestamp": 8,
    "timestamp_ns": 8,
    "timestamptz": 8,
}


@given(st.sampled_from(sorted(_FIXED_WIDTHS)))
def test_fixed_width_table(col_type):
    sample = {
        "float": 1.0,
        "double": 1.0,
        "time": time(1, 2, 3),
    }.get(col_type, 1)
    assert len(encode_bound(col_type, sample)) == _FIXED_WIDTHS[col_type]


def test_unknown_coltype_rejected_both_directions():
    with pytest.raises(ValueError):
        encode_bound("varchar", "x")
    with pytest.raises(ValueError):
        decode_bound("varchar", b"x")
