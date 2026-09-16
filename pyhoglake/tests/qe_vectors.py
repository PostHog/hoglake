"""Cross-language differential seed: verify the Python bounds codec
against tests/vectors/bounds_vectors.json.

The vector file is the language-neutral contract (format documented in
its own header keys): (type, type_params, value, hex) triples where
`hex` is the exact Iceberg single-value binary encoding. A JVM (or any
other) implementation consumes the SAME file: parse `value` per the
header's value_conventions, then assert encode(value) == unhex(hex)
and — unless verify == "encode_only" — decode(unhex(hex)) == value and
re-encode(decode(...)) == unhex(hex).

Every "encode_only" vector documents a PYTHON decode_bound defect, not
a codec disagreement: the precision-38 decimals hit its decimal-context
rounding (see qe_prop_bounds.py), the two top-of-int64 timestamps hit
datetime's year-9999 ceiling. The JVM decoder shares neither limit
(BigInteger, Long) and SHOULD run all of them as full round-trips —
QeBoundsDecodeVectorsTest does, which is why the marker is read
per-type there rather than as a blanket skip.

Regeneration (only when the codec intentionally changes): feed the
exact value through pyhoglake.bounds.encode_bound and paste .hex() —
never hand-compute a byte. The file carried a "generated_by" provenance
string until it was dropped: nothing could falsify it, because every
run re-derives all 107 encodings from the INSTALLED codec anyway, and
pinning it to a version number would only have added a seventh string
to the release bump for a claim no regeneration backed.
"""

import base64
import json
import math
import struct
import uuid as _uuid
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, localcontext
from pathlib import Path

import pytest

from pyhoglake import decode_bound, encode_bound

VECTOR_PATH = Path(__file__).parent / "vectors" / "bounds_vectors.json"

with VECTOR_PATH.open(encoding="utf-8") as f:
    DOC = json.load(f)

VECTORS = DOC["vectors"]

_EPOCH_DATE = date(1970, 1, 1)
_EPOCH_NAIVE = datetime(1970, 1, 1)
_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=UTC)

#: hoglake's closed column-type vocabulary, in spec enum order. The file
#: must document a convention for every one of these AND carry vectors
#: for every one: a type with no vector is a type whose two codecs have
#: never been compared.
ALL_COLTYPES = {
    "boolean",
    "int8",
    "int16",
    "int",
    "long",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "float",
    "double",
    "decimal",
    "date",
    "time",
    "timestamp_s",
    "timestamp_ms",
    "timestamp",
    "timestamp_ns",
    "timestamptz",
    "string",
    "json",
    "uuid",
    "binary",
}


def _parse_value(vec):
    """value string -> the Python value fed to encode_bound (per the
    value_conventions header)."""
    t, v = vec["type"], vec["value"]
    if t == "boolean":
        return {"true": True, "false": False}[v]
    if t in (
        "int8",
        "int16",
        "int",
        "long",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "date",
        "time",
        # micros for these three, nanos for timestamp_ns: the STORED unit
        # in both cases, which is what the codec takes an int to mean
        "timestamp_s",
        "timestamp_ms",
        "timestamp",
        "timestamp_ns",
        "timestamptz",
    ):
        return int(v)  # integer-domain conventions; codec accepts ints
    if t in ("float", "double"):
        if v == "NaN":
            # hex is authoritative for NaN (payload bits not expressible
            # as a portable decimal string)
            fmt = "<f" if t == "float" else "<d"
            return struct.unpack(fmt, bytes.fromhex(vec["hex"]))[0]
        if v == "Infinity":
            return math.inf
        if v == "-Infinity":
            return -math.inf
        return float(v)
    if t in ("string", "json"):
        return v
    if t == "uuid":
        return _uuid.UUID(v)
    if t == "binary":
        return base64.b64decode(v)
    if t == "decimal":
        return int(v)  # unscaled integer convention
    raise AssertionError(f"unknown vector type {t!r}")


def _decoded_matches(vec, decoded):
    """decode_bound output -> equality with the vector's value."""
    t, v = vec["type"], vec["value"]
    if t == "boolean":
        return decoded is ({"true": True, "false": False}[v])
    if t in (
        "int8",
        "int16",
        "int",
        "long",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        # nanos as a plain int: decode_bound deliberately does NOT build a
        # datetime, which would round the sub-microsecond digits away
        "timestamp_ns",
    ):
        return decoded == int(v)
    if t in ("float", "double"):
        fmt = "<f" if t == "float" else "<d"
        return struct.pack(fmt, decoded) == bytes.fromhex(vec["hex"])
    if t == "date":
        return decoded == _EPOCH_DATE + timedelta(days=int(v))
    if t == "time":
        micros = int(v)
        return decoded == time(
            micros // 3_600_000_000,
            micros % 3_600_000_000 // 60_000_000,
            micros % 60_000_000 // 1_000_000,
            micros % 1_000_000,
        )
    if t in ("timestamp", "timestamp_s", "timestamp_ms", "timestamptz"):
        # All four decode to the same calendar object — micros is the
        # stored unit for every hoglake type mapped to Iceberg timestamp —
        # EXCEPT past year 9999, where datetime cannot go and the codec
        # falls back to raw micros (the same answer timestamp_ns always
        # gives). Accept whichever the value's magnitude implies.
        micros = int(v)
        base = _EPOCH_UTC if t == "timestamptz" else _EPOCH_NAIVE
        try:
            return decoded == base + timedelta(microseconds=micros)
        except OverflowError:
            return decoded == micros
    if t in ("string", "json"):
        return decoded == v
    if t == "uuid":
        return decoded == _uuid.UUID(v)
    if t == "binary":
        return decoded == base64.b64decode(v)
    if t == "decimal":
        scale = (vec["type_params"] or {}).get("scale", 0)
        with localcontext() as ctx:
            ctx.prec = 60
            expected = Decimal(int(v)).scaleb(-scale)
        return decoded == expected
    raise AssertionError(f"unknown vector type {t!r}")


def _vector_id(vec):
    val = vec["value"]
    tag = val if len(val) <= 24 else val[:21] + "..."
    return f"{vec['type']}:{tag}"


def test_vector_file_header_contract():
    assert DOC["format"] == "hoglake-bounds-vectors"
    assert DOC["version"] == 1
    assert set(DOC["value_conventions"]) >= ALL_COLTYPES
    # Exact, not >=: a vector deleted by a bad merge is otherwise a silent
    # loss of coverage. Bump deliberately when adding vectors, and keep
    # BoundsVectorFile.EXPECTED_COUNT on the Kotlin side in step.
    assert len(VECTORS) == 107
    for vec in VECTORS:
        assert set(vec) >= {"type", "type_params", "value", "hex", "note"}
        # hex must be lowercase and byte-aligned
        assert vec["hex"] == vec["hex"].lower()
        assert len(vec["hex"]) % 2 == 0


@pytest.mark.parametrize("vec", VECTORS, ids=_vector_id)
def test_python_codec_matches_vector(vec):
    raw = bytes.fromhex(vec["hex"])
    params = vec["type_params"]

    # encode: always verified
    assert encode_bound(vec["type"], _parse_value(vec), params) == raw

    if vec.get("verify") == "encode_only":
        return  # decode is knowingly broken for this case (BUG documented
        # in the vector's note and in qe_prop_bounds.py)

    # decode: value equality
    decoded = decode_bound(vec["type"], raw, params)
    assert _decoded_matches(vec, decoded), (
        f"decode mismatch for {vec['type']} {vec['value']!r}: got {decoded!r}"
    )
    # canonical re-encode (skip int-convention temporal types where
    # decode returns the rich Python object; re-encode those too — the
    # codec accepts them and must reproduce identical bytes)
    assert encode_bound(vec["type"], decoded, params) == raw


def test_all_coltypes_are_covered():
    covered = {v["type"] for v in VECTORS}
    assert covered == ALL_COLTYPES


def test_every_new_type_is_verified_both_ways():
    """No new type may hide behind `encode_only`. The only remaining
    member is a PYTHON decode defect the vector pins rather than papers
    over, and the JVM does not share it (BigInteger has no ambient
    precision), so the Kotlin decode test runs it as a full round-trip:

      decimal -- decode_bound rounds >28-digit unscaled values through
                 the ambient decimal context

    The two timestamp boundary vectors used to be here too: correcting
    them to true micros (~9.2e18) walked straight past datetime's year
    9999 ceiling (~2.5e17). That was a real codec gap, not a vector
    problem, and decode_bound now falls back to raw micros there — so
    they are two-way again.
    """
    encode_only = {v["type"] for v in VECTORS if v.get("verify") == "encode_only"}
    assert encode_only == {"decimal"}


def test_fixed_width_vectors_have_fixed_width_hex():
    # uint64 and json are absent on purpose: uint64's minimal
    # two's-complement form is 1-9 bytes wide, json's is the document.
    widths = {
        "boolean": 1,
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
        "uuid": 16,
    }
    for vec in VECTORS:
        w = widths.get(vec["type"])
        if w is not None:
            assert len(vec["hex"]) == 2 * w, vec
