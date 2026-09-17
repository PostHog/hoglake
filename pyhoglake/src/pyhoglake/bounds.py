"""Iceberg single-value binary serialization for column-stat bounds.

A bound is encoded in the serialization of the column type's MAPPED
ICEBERG type, never of the hoglake type name — that is what keeps
manifest generation for the Iceberg facade a mechanical copy, and it is
why several hoglake types share one encoding here.

Encodings (little-endian unless stated):

    boolean      1 byte, 0x00 / 0x01
    int8         4-byte LE signed  \\
    int16        4-byte LE signed   |  all map to Iceberg int; the small
    uint8        4-byte LE signed   |  widths fit int32 exactly, signed
    uint16       4-byte LE signed   |  or not
    int          4-byte LE signed  /
    uint32       8-byte LE signed  \\  map to Iceberg long (32 unsigned
    long         8-byte LE signed  /   bits do not fit a signed int32)
    uint64       minimal two's-complement big-endian value of the mapped
                 decimal(20,0) — 9 bytes with a 0x00 sign byte above 2^63
    float        4-byte LE IEEE-754
    double       8-byte LE IEEE-754
    date         days since 1970-01-01, 4-byte LE signed
    time         microseconds since midnight, 8-byte LE signed
    timestamp_s  MICROseconds since epoch, 8-byte LE signed  \\  all map to
    timestamp_ms MICROseconds since epoch, 8-byte LE signed   |  Iceberg
    timestamp    MICROseconds since epoch, 8-byte LE signed  /   timestamp
    timestamp_ns NANOseconds since epoch, 8-byte LE signed (Iceberg V3
                 timestamp_ns — the one temporal type not stored in micros)
    timestamptz  microseconds since epoch UTC, 8-byte LE signed
    string       UTF-8 bytes (see "Bytes in, bytes out" below)
    json         UTF-8 bytes (maps to Iceberg string; the document text
                 verbatim, never re-canonicalized)
    uuid         16 bytes, big-endian
    binary       raw bytes
    decimal      minimal two's-complement big-endian unscaled value

The declared precision of timestamp_s/timestamp_ms is catalog metadata,
not a bound unit: their bounds are micros because Iceberg ``timestamp``
single values are micros.

Domain enforcement is deliberately NOT here. The codec is total in both
directions (``encode_bound(t, decode_bound(t, b)) == b`` for every
well-formed ``b``), so a uint8 bound holding 300 encodes and decodes
without complaint. Keeping values inside their type's domain is the
writer's job; range checks here would make ``decode_bound``
un-invertible for hostile footers, which is the failure mode this design
refuses. The Kotlin ``IcebergSingleValue`` makes the same choice, byte
for byte.

**Bytes in, bytes out.** ``string`` and ``json`` bounds are BYTES on the
wire and the codec never changes them. ``encode_bound`` passes a
``bytes`` value through untouched, and ``decode_bound`` returns a ``str``
only when the bound really is UTF-8 — otherwise it returns the raw
``bytes``. Decoding with replacement characters would turn
``b"\xfe\x02"`` into four different bytes (``b"\xef\xbf\xbd\x02"``)
that sort elsewhere, breaking the round-trip above and putting this
codec at odds with the Kotlin hydrator, which copies a string bound
verbatim. A non-UTF-8 bound under a ``string`` column means the FILE is
mislabelled; the bytes say so, a mangled string does not.

One caveat this codec cannot fix: pyarrow decodes a UTF8-annotated
column's ``Statistics.min``/``max`` to ``str`` before pyhoglake sees
them, so stats read from such a FOOTER are already lossy at the source.
That is a property of the malformed file, not of this module.
"""

from __future__ import annotations

import struct
import uuid as _uuid
from datetime import UTC, date, datetime, time
from decimal import Decimal, localcontext
from typing import Any

_EPOCH_DATE = date(1970, 1, 1)
_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=UTC)
_EPOCH_NAIVE = datetime(1970, 1, 1)  # noqa: DTZ001  # naive epoch is the codec's intent for timestamp-without-tz


def _minimal_twos_complement(n: int) -> bytes:
    """Minimal-length big-endian two's-complement encoding of ``n``."""
    if n >= 0:
        length = n.bit_length() // 8 + 1
    else:
        length = (n + 1).bit_length() // 8 + 1
    return n.to_bytes(length, "big", signed=True)


def _unscaled(value: Any, scale: int) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return value  # already unscaled
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    # Ambient decimal context (prec=28) silently ROUNDS wide values in
    # scaleb; precision-38 unscaled values need up to 39 digits, plus
    # headroom for the shift (QE find, 2026-09-05).
    with localcontext() as ctx:
        ctx.prec = 60
        shifted = value.scaleb(scale)
    if shifted != shifted.to_integral_value():
        raise ValueError(f"decimal value {value} does not fit scale {scale} exactly")
    return int(shifted)


def _micros_since_epoch(value: Any) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            delta = value - _EPOCH_UTC
        else:
            delta = value - _EPOCH_NAIVE
        return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
    raise TypeError(f"cannot encode {type(value).__name__} as timestamp micros")


def _nanos_since_epoch(value: Any) -> int:
    """Nanos for a ``timestamp_ns`` bound, whose STORED unit is nanos.

    An int is therefore already the answer. A ``datetime`` tops out at
    microsecond resolution, so it can only ever contribute whole micros
    — scaling it is exact, not a widening guess.
    """
    if isinstance(value, int):
        return value
    return _micros_since_epoch(value) * 1000


def encode_bound(
    col_type: str, value: Any, type_params: dict[str, Any] | None = None
) -> bytes:
    """Encode a single value in Iceberg single-value binary for ``col_type``."""
    if col_type == "boolean":
        return b"\x01" if value else b"\x00"
    # Iceberg int: one 4-byte encoding for five hoglake types.
    if col_type in ("int", "int8", "int16", "uint8", "uint16"):
        return struct.pack("<i", int(value))
    # Iceberg long: uint32 joins it because 32 unsigned bits overflow int32.
    if col_type in ("long", "uint32"):
        return struct.pack("<q", int(value))
    # Iceberg decimal(20,0): the unsigned value as a minimal big-endian
    # two's-complement integer, so [2^63, 2^64) grows a 0x00 sign byte
    # instead of wrapping negative.
    if col_type == "uint64":
        return _minimal_twos_complement(int(value))
    if col_type == "float":
        return struct.pack("<f", float(value))
    if col_type == "double":
        return struct.pack("<d", float(value))
    if col_type == "date":
        if isinstance(value, date) and not isinstance(value, datetime):
            days = (value - _EPOCH_DATE).days
        else:
            days = int(value)
        return struct.pack("<i", days)
    if col_type == "time":
        if isinstance(value, time):
            micros = (
                value.hour * 3600 + value.minute * 60 + value.second
            ) * 1_000_000 + value.microsecond
        else:
            micros = int(value)
        return struct.pack("<q", micros)
    # A passed int is always the STORED unit, so the seconds and millis
    # variants share this arm: micros. Their declared precision is
    # catalog metadata; Iceberg timestamp single values are micros.
    if col_type in ("timestamp", "timestamptz", "timestamp_s", "timestamp_ms"):
        return struct.pack("<q", _micros_since_epoch(value))
    if col_type == "timestamp_ns":
        return struct.pack("<q", _nanos_since_epoch(value))
    # json maps to Iceberg string: the same bytes, no canonicalization.
    if col_type in ("string", "json"):
        if isinstance(value, bytes):
            return value
        return str(value).encode("utf-8")
    if col_type == "uuid":
        if isinstance(value, _uuid.UUID):
            return value.bytes
        if isinstance(value, str):
            return _uuid.UUID(value).bytes
        # bytes(int) ALLOCATES that many zero bytes — a numeric stat from a
        # mismatched footer must fail here, not as an OOM (bool is an int).
        if isinstance(value, int):
            raise ValueError(f"uuid bound must be 16 bytes, got int {value!r}")
        b = bytes(value)
        if len(b) != 16:
            raise ValueError(f"uuid bound must be 16 bytes, got {len(b)}")
        return b
    if col_type == "binary":
        if isinstance(value, int):
            raise ValueError(f"binary bound must be bytes-like, got int {value!r}")
        return bytes(value)
    if col_type == "decimal":
        scale = int((type_params or {}).get("scale", 0))
        return _minimal_twos_complement(_unscaled(value, scale))
    raise ValueError(f"cannot encode bound for column type {col_type!r}")


def decode_bound(
    col_type: str, data: bytes, type_params: dict[str, Any] | None = None
) -> Any:
    """Inverse of :func:`encode_bound` (returns naive-Python values)."""
    if col_type == "boolean":
        return data != b"\x00"
    if col_type in ("int", "int8", "int16", "uint8", "uint16"):
        return struct.unpack("<i", data)[0]
    if col_type in ("long", "uint32"):
        return struct.unpack("<q", data)[0]
    if col_type == "uint64":
        # Shares decimal's encoding at scale 0, so the unscaled integer
        # IS the value; signed=True because 0x00-prefixed nine-byte
        # forms above 2^63 must not be read as negative.
        return int.from_bytes(data, "big", signed=True)
    if col_type == "float":
        return struct.unpack("<f", data)[0]
    if col_type == "double":
        return struct.unpack("<d", data)[0]
    if col_type == "date":
        from datetime import timedelta

        return _EPOCH_DATE + timedelta(days=struct.unpack("<i", data)[0])
    if col_type == "time":
        micros = struct.unpack("<q", data)[0]
        return time(
            micros // 3_600_000_000,
            micros % 3_600_000_000 // 60_000_000,
            micros % 60_000_000 // 1_000_000,
            micros % 1_000_000,
        )
    if col_type in ("timestamp", "timestamptz", "timestamp_s", "timestamp_ms"):
        from datetime import timedelta

        micros = struct.unpack("<q", data)[0]
        base = _EPOCH_UTC if col_type == "timestamptz" else _EPOCH_NAIVE
        try:
            return base + timedelta(microseconds=micros)
        except OverflowError:
            # Python's datetime stops at year 9999 (~2.5e17 micros) while
            # the bound's domain is the whole int64 (~9.2e18) — a factor of
            # 36. Those bounds are legal, encodable, and the Kotlin codec
            # decodes them to a plain Long, so refusing here would make the
            # top of our own domain undecodable in one language only.
            # Fall back to raw micros, exactly as timestamp_ns does for the
            # same reason; encode_bound accepts ints, so the round-trip
            # still closes.
            return micros
    if col_type == "timestamp_ns":
        # Nanos as a plain int, NOT a datetime: datetime tops out at
        # microsecond resolution, so building one would round away the
        # sub-micro digits and break encode(decode(b)) == b. A caller
        # that wants a datetime divides by 1000 and owns the loss.
        return struct.unpack("<q", data)[0]
    if col_type in ("string", "json"):
        # BYTES IN, BYTES OUT. A str is returned when the bound really is
        # UTF-8, which is every honest string bound; when it is not, the
        # raw bytes are returned UNCHANGED rather than decoded with
        # replacement characters.
        #
        # The distinction is not academic. `b"\xfe\x02"` decoded with
        # errors="replace" becomes "\ufffd\x02", and re-encoding THAT
        # yields b"\xef\xbf\xbd\x02" — four bytes where there were
        # two, sorting differently, and silently. The Kotlin hydrator
        # copies a string bound's bytes verbatim, so a lossy decode here
        # would put the two implementations into disagreement about a
        # value neither of them chose. A non-UTF-8 bound under a string
        # column means the FILE is mislabelled; surfacing the bytes says
        # so, while mangling them hides it.
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            return data
    if col_type == "uuid":
        return _uuid.UUID(bytes=data)
    if col_type == "binary":
        return data
    if col_type == "decimal":
        scale = int((type_params or {}).get("scale", 0))
        unscaled = int.from_bytes(data, "big", signed=True)
        # Same context hazard as the encode path: prec=28 rounds >28-digit
        # unscaled values silently.
        with localcontext() as ctx:
            ctx.prec = 60
            return Decimal(unscaled).scaleb(-scale)
    raise ValueError(f"cannot decode bound for column type {col_type!r}")


#: The canonical stored encodings of zero, per Iceberg's single-value
#: rule: a float/double LOWER bound stores -0.0 and an UPPER bound
#: stores +0.0. Keyed (width, is_lower).
_CANONICAL_ZERO = {
    (4, True): struct.pack("<f", -0.0),
    (4, False): struct.pack("<f", 0.0),
    (8, True): struct.pack("<d", -0.0),
    (8, False): struct.pack("<d", 0.0),
}

_FLOAT_BOUND_WIDTH = {"float": 4, "double": 8}


def normalize_bound(col_type: str, raw: bytes | None, *, lower: bool) -> bytes | None:
    """The bytes to STORE for a bound of ``col_type`` in this role.

    Only the signed zeros move. +0.0 and -0.0 are IEEE-equal, so which
    one a writer emits is arbitrary — but Iceberg's evaluators compare
    float/double bounds in natural (total) order, where -0.0 < 0.0, so a
    stored pair of (lower=+0.0, upper=-0.0) reads as an EMPTY range and
    prunes away a file that holds 0.0. Iceberg resolves the arbitrariness
    by fixing the role: lower bounds store -0.0, upper bounds store +0.0.
    Rewriting one zero as the other widens nothing.

    The cases are pinned cross-language in
    ``tests/vectors/bounds_vectors.json`` under ``bound_normalization``;
    the Kotlin door (StatsSanity) answers the same file.

    Anything that is not a float/double bound of the right width comes
    back untouched: a bound of the wrong width is not a zero to canonicalize,
    it is a malformed bound, and the server drops those rather than
    rewriting them.
    """
    width = _FLOAT_BOUND_WIDTH.get(col_type)
    if raw is None or width is None or len(raw) != width:
        return raw
    fmt = "<f" if width == 4 else "<d"
    (value,) = struct.unpack(fmt, raw)
    if value != 0.0:  # NaN and every non-zero value included
        return raw
    return _CANONICAL_ZERO[(width, lower)]
