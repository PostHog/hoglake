"""The Variant codec behind :mod:`pyhoglake.variant`. Private.

It turns JSON text or Python objects into the Arrow storage of a VARIANT
column, shredded as a declaration lays it out, and reads any conformant
storage back. It writes no Parquet: the storage struct it builds is
the write form, field for field the Trino connector's
``VariantShreddingSchema.toParquetType`` layout. Whoever writes the file
puts the decimal4/decimal8 leaves, decimal32/decimal64 in Arrow, in the
file as INT32/INT64 with DECIMAL(p, s), and adds the VARIANT annotation,
which Arrow cannot carry, to the group; :attr:`Plan.stamps` lists those
leaves, and :meth:`Plan.expected_elements` the Parquet schema elements the
finished file holds (parquet_schema stamps the footer and checks it).

The encoder and the decoder share constants, never logic: the decoder is
the reference the encoder is tested against, and the apache/parquet-testing
vectors are the decoder's (tests/variant_conformance).

Spec references (``VE`` = VariantEncoding.md, ``VS`` = VariantShredding.md)
are to apache/parquet-format at bf0993925ccf41b1fb4b1ae241af4a66e8adf1fe.
"""

from __future__ import annotations

import json
import re
import struct
import time as _clock
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Context, Decimal, Inexact, Rounded
from functools import lru_cache
from itertools import accumulate, pairwise
from typing import TYPE_CHECKING, Any, Final, Literal, TypeAlias

import pyarrow as pa
import pyarrow.compute as pc

from .errors import UnsupportedShreddingError, VariantEncodingError

if TYPE_CHECKING:
    from .models import Column
    from .variant import Shredding, VariantReport

#: How deeply a value may nest: the root is at depth 0, and the elements
#: and fields of a value at depth d are at d + 1. Deeper is invalid
#: (``nesting_too_deep``). The bound is pyhoglake's, not a reader's (DuckDB
#: reads 400): it keeps the recursive encoder and decoder well inside
#: CPython's default recursion limit of 1000, and a refusal that depends on
#: no interpreter setting is the same on every machine.
MAX_VARIANT_DEPTH: Final = 128


class _VariantNull:
    """The type of :data:`VARIANT_NULL`. Copies and unpickled copies are the
    one instance, so ``is`` compares them."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "VARIANT_NULL"

    def __reduce__(self) -> str:
        return "VARIANT_NULL"


#: A Variant null (the value ``00``) where None would mean SQL NULL: as a
#: whole row. Inside an object or an array None is a Variant null too.
VARIANT_NULL: Final = _VariantNull()

#: Why a row is invalid (VariantEncodingError.reason, VariantReport keys).
#: The encoder's reasons, then the reader's.
Reason: TypeAlias = Literal[
    "invalid_json",
    "duplicate_key",
    "non_finite_number",
    "integer_out_of_range",
    "nesting_too_deep",
    "invalid_unicode",
    "unsupported_python_type",
    "decimal_out_of_range",
    "aware_time",
    "timestamp_out_of_range",
    "sql_null_in_not_null",
    "malformed_variant",
    "invalid_shredding",
    "non_canonical",
]

_INT64_MIN: Final = -(2**63)
_INT64_MAX: Final = 2**63 - 1

#: Integers beyond int64 are decimal16 with scale 0 up to 38 digits (D1).
_DECIMAL16_LIMIT: Final = 10**38

#: Decimal arithmetic that refuses to round. The default context has 28
#: digits, which silently rounds a 38-digit decimal16 (the hazard
#: bounds.py guards against too); traps make any rounding loud.
_EXACT: Final = Context(prec=100, traps=[Inexact, Rounded])

#: The metadata of a value without objects: version 1, no strings, one
#: offset (VE:118-121). The spec's own example (VS:60-63) shows two bytes,
#: which omits the offset; DuckDB rejects that form.
EMPTY_METADATA: Final = b"\x01\x00\x00"

#: A Variant null value: basic type 0, primitive type 0.
NULL_VALUE: Final = b"\x00"

_EPOCH_UTC: Final = datetime(1970, 1, 1, tzinfo=UTC)
#: The epoch of a timestamp without time zone, which is naive on purpose.
_EPOCH: Final = _EPOCH_UTC.replace(tzinfo=None)
_EPOCH_DAY: Final = date(1970, 1, 1)
_MICROS_PER_DAY: Final = 86_400_000_000


# -- row faults ---------------------------------------------------------------


class _Invalid(Exception):
    """A fault in one row, raised where it is found.

    Each container frame it passes on the way out appends its key or index
    to ``trail`` (a try block costs nothing until it catches, from CPython
    3.11), so the path costs nothing on rows that are fine. A fault found
    by the JSON parser has no position in the value and no path.
    """

    def __init__(self, reason: Reason, detail: str, *, located: bool = True) -> None:
        super().__init__(detail)
        self.reason: Reason = reason
        self.detail = detail
        self.located = located
        self.trail: list[str | int] = []
        #: The key an encoding fault in the metadata was found on; its path
        #: is looked up afterwards, since the dictionary has no position.
        self.key: str | None = None

    def path(self) -> str | None:
        if not self.located:
            return None
        return _cut(
            "$"
            + "".join(
                f"[{step}]" if isinstance(step, int) else member(step)
                for step in reversed(self.trail)
            ),
            PATH_CAP,
        )


#: The most characters of a row's path an error or a report carries: its
#: keys, or a storage's column names, are data, as long as the row may be,
#: and the path goes into a message, a log line and first_invalid. A
#: value's 128 levels of short keys stay whole.
PATH_CAP: Final = 1024


#: C0 and C1 controls, surrogates, and U+2028/U+2029, which end a line in
#: JavaScript and some log viewers.
_UNSAFE_IN_PATH: Final = re.compile("[\x00-\x1f\x7f-\x9f\ud800-\udfff\u2028\u2029]")


def _escape(match: re.Match[str]) -> str:
    return f"\\u{ord(match.group()):04x}"


def member(name: str) -> str:
    """The path step of an object key: ``.name``, or ``["a.b"]`` when the
    name would read as more than one step (a dot or a bracket in it, a quote
    or a backslash, or nothing at all). Trino's own tests declare a field
    ``a.b`` beside a field ``a`` holding ``b``, and the two must not share a
    path in a report.

    A name with a control character, a line separator or a surrogate (the
    unpaired one an ``invalid_unicode`` row is refused for) is quoted with
    ``\\u`` escapes, ``["\\ud800"]``: the path is then always one line of
    text any UTF-8 log can write. The set is fixed, not ``isprintable()``,
    whose answer follows the interpreter's Unicode version: a path is a
    report key and a metric label, and must not change with Python.
    """
    if _UNSAFE_IN_PATH.search(name):
        return "[" + json.dumps(name, ensure_ascii=True) + "]"
    if not name or any(c in name for c in '.[]"\\'):
        return "[" + json.dumps(name, ensure_ascii=False) + "]"
    return "." + name


# -- strict JSON --------------------------------------------------------------


def _object_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    # The stdlib keeps the last of two equal keys. A Variant object cannot
    # hold both (VE:465), and which one the writer meant is not ours to
    # guess, so the row is refused (D6).
    obj = dict(pairs)
    if len(obj) != len(pairs):
        seen: set[str] = set()
        for key, _ in pairs:
            if key in seen:
                raise _Invalid(
                    "duplicate_key",
                    f"key {_quoted(key)} appears twice in one object",
                    located=False,
                )
            seen.add(key)
    return obj


def _refuse_constant(name: str) -> float:
    raise _Invalid("non_finite_number", f"{name} is not a JSON number", located=False)


def _finite_float(text: str) -> float:
    # A literal beyond the double range parses to inf; a double from JSON is
    # always finite (D1). Underflow to 0.0 is an ordinary rounding.
    number = float(text)
    if number in (float("inf"), float("-inf")):
        raise _Invalid(
            "non_finite_number",
            f"{text[:40]} is beyond the range of a double",
            located=False,
        )
    return number


_JSON: Final = json.JSONDecoder(
    object_pairs_hook=_object_without_duplicates,
    parse_float=_finite_float,
    parse_constant=_refuse_constant,
)


def _parse_json(raw: Any) -> Any:
    """One row of JSON text, decoded; faults are ``_Invalid``."""
    if type(raw) is bytes:
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise _Invalid(
                "invalid_unicode",
                f"the text is not UTF-8 ({exc.reason})",
                located=False,
            ) from None
    elif type(raw) is not str:
        raise TypeError(
            f"encode_json takes JSON text (str or bytes), not {type(raw).__name__}; "
            "encode Python objects with encode_python"
        )
    try:
        return _JSON.decode(raw)
    except _Invalid:
        raise
    except json.JSONDecodeError as exc:
        raise _Invalid(
            "invalid_json", f"{exc.msg} at character {exc.pos}", located=False
        ) from None
    except RecursionError:
        raise _Invalid(
            "nesting_too_deep",
            "the text nests deeper than the JSON parser recurses",
            located=False,
        ) from None
    except ValueError as exc:
        # Not a JSONDecodeError: the only other ValueError the parser
        # raises is CPython's limit on the digits of an integer literal
        # (4300 by default), far beyond the 38 a Variant holds (D1).
        raise _Invalid("integer_out_of_range", str(exc), located=False) from None


def _collect_keys(
    container: dict[str, Any] | list[Any], keys: set[str], depth: int
) -> None:
    """Every key at every depth of decoded JSON, and the depth bound (D7, D8)."""
    if type(container) is dict:
        if depth >= MAX_VARIANT_DEPTH and container:
            raise _Invalid(
                "nesting_too_deep", f"nested more than {MAX_VARIANT_DEPTH} levels deep"
            )
        keys.update(container)
        for key, item in container.items():
            kind = type(item)
            if kind is dict or kind is list:
                try:
                    _collect_keys(item, keys, depth + 1)
                except _Invalid as exc:
                    exc.trail.append(key)
                    raise
    else:
        if depth >= MAX_VARIANT_DEPTH and container:
            raise _Invalid(
                "nesting_too_deep", f"nested more than {MAX_VARIANT_DEPTH} levels deep"
            )
        for index, item in enumerate(container):
            kind = type(item)
            if kind is dict or kind is list:
                try:
                    _collect_keys(item, keys, depth + 1)
                except _Invalid as exc:
                    exc.trail.append(index)
                    raise


def _prepare_json(raw: object) -> tuple[Any, set[str]]:
    value = _parse_json(raw)
    keys: set[str] = set()
    if type(value) is dict or type(value) is list:
        _collect_keys(value, keys, 0)
    return value, keys


def _normalise(value: object, keys: set[str], depth: int) -> object:
    """A Python object as the encoder takes it (D2): exact dicts and lists,
    bytes for any bytes-like, None for VARIANT_NULL. Collects the keys and
    checks the depth on the way, as _collect_keys does for JSON.

    Containers are matched exactly, as scalars are: a dict or list subclass
    (an OrderedDict, a namedtuple) is left as it is, for the encoder to
    refuse, rather than read as the plain container it may not mean.
    """
    if type(value) is dict:
        if depth >= MAX_VARIANT_DEPTH and value:
            raise _Invalid(
                "nesting_too_deep", f"nested more than {MAX_VARIANT_DEPTH} levels deep"
            )
        out: dict[str, object] = {}
        for key, item in value.items():
            if type(key) is not str:
                raise _Invalid(
                    "unsupported_python_type",
                    f"an object key is {type(key).__name__}, not str",
                )
            keys.add(key)
            try:
                out[key] = _normalise(item, keys, depth + 1)
            except _Invalid as exc:
                exc.trail.append(key)
                raise
        return out
    if type(value) is list or type(value) is tuple:
        if depth >= MAX_VARIANT_DEPTH and value:
            raise _Invalid(
                "nesting_too_deep", f"nested more than {MAX_VARIANT_DEPTH} levels deep"
            )
        items: list[object] = []
        for index, item in enumerate(value):
            try:
                items.append(_normalise(item, keys, depth + 1))
            except _Invalid as exc:
                exc.trail.append(index)
                raise
        return items
    if value is VARIANT_NULL:
        return None
    if type(value) is bytearray or type(value) is memoryview:
        return bytes(value)
    return value


def _prepare_python(raw: object) -> tuple[object, set[str]]:
    keys: set[str] = set()
    return _normalise(raw, keys, 0), keys


def _key_trail(value: object, key: str) -> list[str | int] | None:
    """The trail (innermost step first) to the first object holding ``key``:
    the path of a key the metadata could not encode."""
    if type(value) is dict:
        if key in value:
            return [key]
        for name, item in value.items():
            trail = _key_trail(item, key)
            if trail is not None:
                return [*trail, name]
    elif type(value) is list:
        for index, item in enumerate(value):
            trail = _key_trail(item, key)
            if trail is not None:
                return [*trail, index]
    return None


# -- encoding -----------------------------------------------------------------


def _width(n: int) -> int:
    """The fewest bytes, 1 to 4, of an unsigned field that holds ``n``."""
    return 1 if n < 0x100 else 2 if n < 0x10000 else 3 if n < 0x1000000 else 4


def _pack(width: int, values: list[int]) -> bytes:
    if width == 1:
        return bytes(values)
    if width == 2:
        return struct.pack(f"<{len(values)}H", *values)
    if width == 4:
        return struct.pack(f"<{len(values)}I", *values)
    return b"".join(value.to_bytes(3, "little") for value in values)


_NO_IDS: Final[dict[str, int]] = {}


def _metadata(keys: set[str]) -> tuple[bytes, dict[str, int]]:
    """The row's dictionary (D8) and the field id of each key.

    Every key at every depth, once, sorted by its UTF-8 bytes: for str
    without surrogates, code-point order is UTF-8 byte order (VE:456), and
    a str with one fails to encode below. ``sorted_strings`` is set
    whenever there are strings; the offset width is the smallest that
    holds both the count and the total length (they share it, VE:131-134).
    """
    if not keys:
        return EMPTY_METADATA, _NO_IDS
    names = sorted(keys)
    try:
        encoded = [name.encode("utf-8") for name in names]
    except UnicodeEncodeError as exc:
        fault = _Invalid("invalid_unicode", "an object key holds an unpaired surrogate")
        fault.key = exc.object
        raise fault from None
    offsets = [0, *accumulate(map(len, encoded))]
    width = _width(max(offsets[-1], len(names)))
    header = 0x01 | 0x10 | (width - 1) << 6
    body = _pack(width, [len(names), *offsets]) + b"".join(encoded)
    return bytes((header,)) + body, {name: i for i, name in enumerate(names)}


#: Short-string headers: basic type 1, the length in the upper six bits.
_SHORT_STRING: Final = [bytes(((n << 2) | 1,)) for n in range(64)]
_PRIMITIVE_INT8: Final = struct.Struct("<Bb").pack
_PRIMITIVE_INT16: Final = struct.Struct("<Bh").pack
_PRIMITIVE_INT32: Final = struct.Struct("<Bi").pack
_PRIMITIVE_INT64: Final = struct.Struct("<Bq").pack
_PRIMITIVE_DOUBLE: Final = struct.Struct("<Bd").pack
_PRIMITIVE_SIZED: Final = struct.Struct("<BI").pack

# Primitive headers: the type id (VE:414-434) shifted over basic type 0.
_INT8, _INT16, _INT32, _INT64 = 0x0C, 0x10, 0x14, 0x18
_DOUBLE, _DECIMAL4, _DECIMAL8, _DECIMAL16 = 0x1C, 0x20, 0x24, 0x28
_DATE, _TIMESTAMP_UTC, _TIMESTAMP_NTZ = 0x2C, 0x30, 0x34
_BINARY, _LONG_STRING, _TIME_NTZ, _UUID = 0x3C, 0x40, 0x44, 0x50


def _encode_string(text: str) -> bytes:
    data = _utf8(text)
    n = len(data)
    if n < 64:
        return _SHORT_STRING[n] + data
    return _PRIMITIVE_SIZED(_LONG_STRING, n) + data


def _utf8(text: str) -> bytes:
    """The one place a string value meets UTF-8 (D6): a lone surrogate,
    which the stdlib parser accepts from ``"\\ud800"``, fails the row here
    rather than the batch."""
    try:
        return text.encode("utf-8")
    except UnicodeEncodeError:
        raise _Invalid(
            "invalid_unicode", "a string holds an unpaired surrogate"
        ) from None


def _encode_int(value: int) -> bytes:
    # The smallest width that holds it (D1), then exact decimal16.
    if -0x80 <= value <= 0x7F:
        return _PRIMITIVE_INT8(_INT8, value)
    if -0x8000 <= value <= 0x7FFF:
        return _PRIMITIVE_INT16(_INT16, value)
    if -0x8000_0000 <= value <= 0x7FFF_FFFF:
        return _PRIMITIVE_INT32(_INT32, value)
    if _INT64_MIN <= value <= _INT64_MAX:
        return _PRIMITIVE_INT64(_INT64, value)
    if -_DECIMAL16_LIMIT < value < _DECIMAL16_LIMIT:
        return bytes((_DECIMAL16, 0)) + value.to_bytes(16, "little", signed=True)
    raise _Invalid(
        "integer_out_of_range",
        f"an integer of {value.bit_length()} bits has more than 38 digits",
    )


def _decimal_parts(value: Decimal) -> tuple[int, int, int]:
    """``(scale, unscaled, precision)`` of a Decimal as a Variant holds it (D2).

    The scale is the negated exponent; a positive exponent is rescaled to
    scale 0. The precision, which picks the width, counts the scale too,
    so 0.001 is precision 3: Spark and Iceberg read a decimal4 as
    DECIMAL(9, s) and refuse a scale above 9 there.
    """
    # The messages say how big the value is, never the value: a Decimal
    # has as many digits as its caller gave it.
    if not value.is_finite():
        raise _Invalid("decimal_out_of_range", "a NaN or infinite decimal")
    exponent = value.as_tuple().exponent
    assert isinstance(exponent, int)
    scale = -exponent if exponent < 0 else 0
    if scale > 38:
        raise _Invalid("decimal_out_of_range", f"a decimal of scale {scale}, beyond 38")
    # A zero's adjusted() is its exponent: 0E+50 is the one digit 0.
    if value and value.adjusted() >= 38:
        raise _Invalid(
            "decimal_out_of_range",
            f"a decimal of {value.adjusted() + 1} integer digits has more than 38",
        )
    unscaled = int(value.scaleb(scale, _EXACT))
    precision = max(len(str(abs(unscaled))), scale)
    if precision > 38:
        raise _Invalid(
            "decimal_out_of_range", f"a decimal of {precision} digits has more than 38"
        )
    return scale, unscaled, precision


def _encode_decimal(value: Decimal) -> bytes:
    scale, unscaled, precision = _decimal_parts(value)
    if precision <= 9:
        header, size = _DECIMAL4, 4
    elif precision <= 18:
        header, size = _DECIMAL8, 8
    else:
        header, size = _DECIMAL16, 16
    return bytes((header, scale)) + unscaled.to_bytes(size, "little", signed=True)


def _micros(delta: timedelta) -> int:
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


#: The days and microseconds since the epoch that Python's date and
#: datetime hold: the years 1 to 9999.
_MIN_DAY: Final = (date.min - _EPOCH_DAY).days
_MAX_DAY: Final = (date.max - _EPOCH_DAY).days
_MIN_MICROS: Final = _micros(datetime.min.replace(tzinfo=UTC) - _EPOCH_UTC)
_MAX_MICROS: Final = _micros(datetime.max.replace(tzinfo=UTC) - _EPOCH_UTC)


def _timestamp_micros(value: datetime) -> tuple[bool, int]:
    """Whether ``value`` is aware, and its microseconds since the epoch, in
    UTC when it is (D2: an aware datetime is normalised to UTC).

    An aware datetime near either end of Python's range can be an instant
    whose UTC time no datetime holds (0001-01-01 00:00+05:00 is in the
    year 0 in UTC). The bytes would be a valid Variant, but no Python
    reader, this module's included, could give the value back, so the row
    is refused rather than written one-way.
    """
    if value.utcoffset() is None:
        return False, _micros(value - _EPOCH)
    micros = _micros(value - _EPOCH_UTC)
    if not _MIN_MICROS <= micros <= _MAX_MICROS:
        raise _Invalid(
            "timestamp_out_of_range",
            f"{value} is outside the years 1 to 9999 in UTC, which a Python "
            "datetime holds",
        )
    return True, micros


def _time_micros(value: time) -> int:
    if value.utcoffset() is not None:
        raise _Invalid(
            "aware_time", f"{value} has a UTC offset, and a Variant time has none"
        )
    return ((value.hour * 60 + value.minute) * 60 + value.second) * 1_000_000 + (
        value.microsecond
    )


def _encode_other(value: Any) -> bytes:
    """The Python-only scalars of D2; JSON never produces them."""
    kind = type(value)
    if kind is Decimal:
        return _encode_decimal(value)
    if kind is bytes:
        return _PRIMITIVE_SIZED(_BINARY, len(value)) + value
    if kind is datetime:
        aware, micros = _timestamp_micros(value)
        return _PRIMITIVE_INT64(_TIMESTAMP_UTC if aware else _TIMESTAMP_NTZ, micros)
    if kind is date:
        return _PRIMITIVE_INT32(_DATE, (value - _EPOCH_DAY).days)
    if kind is time:
        return _PRIMITIVE_INT64(_TIME_NTZ, _time_micros(value))
    if kind is uuid.UUID:
        return bytes((_UUID,)) + value.bytes
    raise _Invalid(
        "unsupported_python_type",
        f"{kind.__module__}.{kind.__qualname__} has no Variant type",
    )


def _encode(value: Any, ids: dict[str, int]) -> bytes:
    """The canonical bytes of one value (D9), the row's field ids given."""
    kind = type(value)
    if kind is str:
        return _encode_string(value)
    if kind is int:
        return _encode_int(value)
    if kind is dict:
        return _encode_fields(value, list(value), ids)
    if kind is bool:
        return b"\x04" if value else b"\x08"
    if kind is float:
        return _PRIMITIVE_DOUBLE(_DOUBLE, value)
    if value is None:
        return NULL_VALUE
    if kind is list:
        return _encode_array(value, ids)
    return _encode_other(value)


def _encode_fields(
    obj: dict[str, object], names: list[str], ids: dict[str, int]
) -> bytes:
    """An object of ``names``, keys of ``obj``: the whole object, or the
    residual of a shredded one.

    Field ids and offsets go in key order, which is field-id order because
    the dictionary is sorted, and the values are laid out in that order
    too, so the offsets climb (VE:296-312, 454-465).
    """
    names.sort(key=ids.__getitem__)
    values: list[bytes] = []
    for name in names:
        try:
            values.append(_encode(obj[name], ids))
        except _Invalid as exc:
            exc.trail.append(name)
            raise
    field_ids = [ids[name] for name in names]
    n = len(values)
    offsets = [0, *accumulate(map(len, values))]
    offset_width = _width(offsets[-1])
    id_width = _width(field_ids[-1] if field_ids else 0)
    large = n > 255
    header = 0x02 | (offset_width - 1) << 2 | (id_width - 1) << 4 | large << 6
    return (
        bytes((header,))
        + n.to_bytes(4 if large else 1, "little")
        + _pack(id_width, field_ids)
        + _pack(offset_width, offsets)
        + b"".join(values)
    )


def _encode_array(items: list[object], ids: dict[str, int]) -> bytes:
    values: list[bytes] = []
    for index, item in enumerate(items):
        try:
            values.append(_encode(item, ids))
        except _Invalid as exc:
            exc.trail.append(index)
            raise
    n = len(values)
    offsets = [0, *accumulate(map(len, values))]
    offset_width = _width(offsets[-1])
    large = n > 255
    header = 0x03 | (offset_width - 1) << 2 | large << 4
    return (
        bytes((header,))
        + n.to_bytes(4 if large else 1, "little")
        + _pack(offset_width, offsets)
        + b"".join(values)
    )


# -- declarations -> plans ----------------------------------------------------

#: The Arrow type of each primitive leaf, written and read. decimal4 and
#: decimal8 are written as decimal32/decimal64, and read back as decimal128
#: from a file whose footer carries DECIMAL(p, s) and no Arrow schema
#: (VS:89-111).
_PRIMITIVE_ARROW: Final = {
    "boolean": pa.bool_(),
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "float": pa.float32(),
    "double": pa.float64(),
    "date": pa.date32(),
    "time": pa.time64("us"),
    "timestamp": pa.timestamp("us"),
    "timestamp_ns": pa.timestamp("ns"),
    "timestamptz": pa.timestamp("us", tz="UTC"),
    "timestamptz_ns": pa.timestamp("ns", tz="UTC"),
    "binary": pa.binary(),
    "string": pa.string(),
    "uuid": pa.uuid(),
}

_KNOWN_TYPES: Final = frozenset(
    {
        "object",
        "array",
        "variant",
        "decimal4",
        "decimal8",
        "decimal16",
        *_PRIMITIVE_ARROW,
    }
)


@dataclass(frozen=True, eq=False)
class Node:
    """One declared node: the ``value``/``typed_value`` group it becomes.

    ``path`` is the declared path (``$``, ``$.price``, ``$.tags[*]``,
    ``$["a.b"]``; see :func:`member`), uncapped, which VariantReport keys
    on. ``kind`` is the declared type.
    """

    path: str
    kind: str
    #: The group struct, written and read.
    write_type: pa.StructType
    read_type: pa.StructType
    #: decimal precision and scale; 0 otherwise.
    precision: int = 0
    scale: int = 0
    #: object: (name, node) in declared order.
    fields: tuple[tuple[str, Node], ...] = ()
    #: array: the element.
    element: Node | None = None


#: A decimal4/decimal8 leaf the footer must annotate: its path of Parquet
#: names below the variant group, its precision and its scale. Paths are
#: tuples because a declared name may hold a dot (Trino's ``a.b``).
DecimalStamp: TypeAlias = tuple[tuple[str, ...], int, int]


@dataclass(frozen=True, eq=False)
class Plan:
    """A compiled declaration: the storage it encodes to, and how.

    ``root`` is None for an unshredded column (no declaration, or a root
    ``variant``), whose storage is REQUIRED ``metadata`` and REQUIRED
    ``value``: the only unshredded shape the Trino connector reads (D11).
    The node tree is everything a later layout check needs to derive the
    Parquet elements a file must have.
    """

    shredding: Any
    root: Node | None
    write_type: pa.StructType
    read_type: pa.StructType
    stamps: tuple[DecimalStamp, ...]

    def form_of(self, kind: pa.DataType) -> Literal["write", "read"] | None:
        """Which form of this declaration's storage ``kind`` is, or None.

        Plain ``==``, which ignores field metadata: nothing in either form
        rests on it. A decimal4/decimal8 leaf is written as
        decimal32/decimal64, whose type names its precision and scale, so
        the storage of ``int64`` or of ``decimal8(18,6)`` is no form of
        ``decimal8(18,2)``, and Arrow will not put them in one column.
        """
        if kind == self.write_type:
            return "write"
        if kind == self.read_type:
            return "read"
        return None

    def expected_elements(
        self, column: Column
    ) -> list[tuple[tuple[str, ...], ParquetElement]]:
        """The Parquet schema elements a file holds for ``column`` written
        from this plan's storage, in schema order, each with its path of
        names (the column's name first).

        That is the group, REQUIRED or OPTIONAL as the column is NOT NULL or
        not, with the catalog field id and VARIANT(1), and below it the
        connector's layout (VS:83-122), with no field id: what pyarrow
        writes from the write form, decimal4/decimal8 leaves viewed as
        int32/int64 and no Arrow schema stored, once the footer is stamped.
        ``parquet_schema.variant_layout_fault`` holds pyhoglake's own files
        to it, element for element.
        """
        name = column.name
        group: ParquetElement = {
            3: _PQ_OPTIONAL if column.nullable else _PQ_REQUIRED,
            4: name,
            5: 2 if self.root is None else 3,
            9: column.field_id,
            10: {16: {1: 1}},
        }
        out = [
            ((name,), group),
            ((name, "metadata"), _binary("metadata", _PQ_REQUIRED)),
        ]
        if self.root is None:
            out.append(((name, "value"), _binary("value", _PQ_REQUIRED)))
        else:
            _node_elements(self.root, (name,), out)
        return out


def _difference(kind: pa.DataType, expected: pa.DataType, path: str) -> str | None:
    """The first column of ``kind`` that is not ``expected``'s, by name,
    order, nullability and type, as a path of column names spelt as
    :func:`member` spells keys (``.typed_value.a``, "" for the top), or
    None when there is none."""
    if pa.types.is_struct(expected) or pa.types.is_list(expected):
        if kind.id != expected.id:
            return path
        got = list(kind) if pa.types.is_struct(kind) else [kind.value_field]
        want = (
            list(expected) if pa.types.is_struct(expected) else [expected.value_field]
        )
        for mine, theirs in zip(got, want, strict=False):
            here = path + member(theirs.name)
            if mine.name != theirs.name or mine.nullable != theirs.nullable:
                return here
            found = _difference(mine.type, theirs.type, here)
            if found is not None:
                return found
        if len(got) != len(want):
            return path
        return None
    return None if kind == expected else path


_VALUE_FIELD: Final = pa.field("value", pa.binary())
_METADATA_FIELD: Final = pa.field("metadata", pa.binary(), nullable=False)
_UNSHREDDED: Final = pa.struct(
    [_METADATA_FIELD, pa.field("value", pa.binary(), nullable=False)]
)
UNSHREDDED_PLAN: Final = Plan(None, None, _UNSHREDDED, _UNSHREDDED, ())


def compile_plan(shredding: Shredding | None) -> Plan:
    """The plan of a declaration, cached on its canonical JSON.

    A node type this client does not know raises UnsupportedShreddingError
    (a newer server's grammar: fail closed, upgrade); any other fault is the
    ValidationError :func:`pyhoglake.variant.validate_shredding` raises.
    """
    if shredding is None:
        return UNSHREDDED_PLAN
    _refuse_unknown_types(shredding, "$", 0)
    # variant.py imports this module, and the grammar lives there.
    from .variant import _canonical_json, _plain, validate_shredding

    validate_shredding(shredding)
    # The grammar takes any Mapping and tuple, as variant_field does; json
    # serialises only dicts and lists, so a mappingproxy would fail here.
    return _compile_canonical(_canonical_json(_plain(shredding)))


def _refuse_unknown_types(node: object, path: str, depth: int) -> None:
    # Tolerant of every other fault, which validate_shredding reports; the
    # depth bound keeps a hostile declaration from recursing.
    if not isinstance(node, Mapping) or depth > 64:
        return
    kind = node.get("type")
    if isinstance(kind, str) and kind not in _KNOWN_TYPES:
        # The path is dotted as the grammar's refusals are, but this runs
        # first, on names it has not checked: one with a lone surrogate
        # would make text no UTF-8 log can write.
        where = _UNSAFE_IN_PATH.sub(_escape, _cut(path, PATH_CAP))
        raise UnsupportedShreddingError(
            f"type_params.shredding node {where} has type {_quoted(kind)}, "
            "which this version of pyhoglake does not know; upgrade pyhoglake",
            status_code=None,
        )
    fields = node.get("fields")
    if isinstance(fields, (list, tuple)):
        for field in fields:
            name = field.get("name") if isinstance(field, Mapping) else None
            _refuse_unknown_types(field, f"{path}.{name}", depth + 1)
    _refuse_unknown_types(node.get("element"), f"{path}[*]", depth + 1)


@lru_cache(maxsize=256)
def _compile_canonical(canonical: str) -> Plan:
    decl = json.loads(canonical)
    if decl["type"] == "variant":
        return Plan(decl, None, _UNSHREDDED, _UNSHREDDED, ())
    stamps: list[DecimalStamp] = []
    root = _compile_node(decl, "$", ("typed_value",), stamps)
    write = pa.struct([_METADATA_FIELD, *root.write_type])
    read = pa.struct([_METADATA_FIELD, *root.read_type])
    return Plan(decl, root, write, read, tuple(stamps))


def _compile_node(
    decl: Mapping[str, Any],
    path: str,
    typed_path: tuple[str, ...],
    stamps: list[DecimalStamp],
) -> Node:
    """The node for ``decl``. ``typed_path`` is the Parquet path of its
    ``typed_value`` below the variant group, for the stamps."""
    kind = decl["type"]
    if kind == "variant":
        group = pa.struct([_VALUE_FIELD])
        return Node(path, kind, group, group)
    if kind == "object":
        fields = tuple(
            (
                f["name"],
                _compile_node(
                    f,
                    path + member(f["name"]),
                    (*typed_path, f["name"], "typed_value"),
                    stamps,
                ),
            )
            for f in decl["fields"]
        )
        # Field groups are REQUIRED (VS:166, 193): a missing field is a group
        # whose value and typed_value are both null.
        write: pa.DataType = pa.struct(
            [pa.field(name, child.write_type, nullable=False) for name, child in fields]
        )
        read: pa.DataType = pa.struct(
            [pa.field(name, child.read_type, nullable=False) for name, child in fields]
        )
        return _group(path, kind, write, read, fields=fields)
    if kind == "array":
        element = _compile_node(
            decl["element"],
            f"{path}[*]",
            (*typed_path, "list", "element", "typed_value"),
            stamps,
        )
        # A three-level list of REQUIRED element groups (VS:117-122).
        write = pa.list_(pa.field("element", element.write_type, nullable=False))
        read = pa.list_(pa.field("element", element.read_type, nullable=False))
        return _group(path, kind, write, read, element=element)
    if kind in ("decimal4", "decimal8", "decimal16"):
        precision, scale = decl["precision"], decl["scale"]
        read = pa.decimal128(precision, scale)
        if kind == "decimal16":
            return _group(path, kind, read, read, precision=precision, scale=scale)
        stamps.append((typed_path, precision, scale))
        # Not int32/int64, though that is what the file holds: an integer
        # says nothing of its scale, and its storage would be the storage
        # of every declaration of that width. pyarrow writes a decimal32 or
        # decimal64 as FLBA by default, and not at all on 21. Its
        # store_decimal_as_integer is file-wide: it would also make INT32/
        # INT64 of a decimal16 leaf (FLBA in the connector's layout) and of
        # a top-level decimal column of precision 18 or less, so the writer
        # views these leaves as int32/int64 and stamps DECIMAL (Plan.stamps).
        width = pa.decimal32 if kind == "decimal4" else pa.decimal64
        return _group(
            path,
            kind,
            width(precision, scale),
            read,
            precision=precision,
            scale=scale,
        )
    arrow = _PRIMITIVE_ARROW[kind]
    return _group(path, kind, arrow, arrow)


def _group(
    path: str, kind: str, write: pa.DataType, read: pa.DataType, **rest: Any
) -> Node:
    return Node(
        path,
        kind,
        pa.struct([_VALUE_FIELD, pa.field("typed_value", write)]),
        pa.struct([_VALUE_FIELD, pa.field("typed_value", read)]),
        **rest,
    )


# -- plans -> Parquet schema elements -------------------------------------------

#: A Parquet schema element as parquet_schema reads one out of a footer:
#: parquet.thrift's ``SchemaElement`` by Thrift field id (1 type, 2
#: type_length, 3 repetition_type, 4 name, 5 num_children, 6
#: converted_type, 7 scale, 8 precision, 9 field_id, 10 logicalType), a
#: struct or a union inside it as a dict of the same kind.
ParquetElement: TypeAlias = dict[int, Any]

# parquet.thrift's FieldRepetitionType, Type and ConvertedType values.
_PQ_REQUIRED, _PQ_OPTIONAL, _PQ_REPEATED = 0, 1, 2
_PQ_BOOLEAN, _PQ_INT32, _PQ_INT64, _PQ_FLOAT, _PQ_DOUBLE = 0, 1, 2, 4, 5
_PQ_BYTE_ARRAY, _PQ_FIXED = 6, 7
_PQ_UTF8, _PQ_LIST, _PQ_DECIMAL, _PQ_DATE = 0, 3, 5, 6
_PQ_TIMESTAMP_MICROS, _PQ_INT_8, _PQ_INT_16 = 10, 15, 16

#: The ``typed_value`` leaf of each primitive node, but for its repetition
#: and name: the physical type and the logical type of the spec's table
#: (VS:83-105), which the connector's ``toParquetType`` writes, with the
#: legacy converted type pyarrow writes beside a logical type that has one.
#: A converted type is a legacy reader's spelling of the logical type, so
#: a writer may leave it out; the layout check accepts either, and the
#: pyarrow matrix pins which one each version writes. The timestamp leaves
#: carry TIMESTAMP_MICROS whether or not they are adjusted to UTC, as
#: pyarrow writes them; the logical type is what tells them apart.
_PARQUET_LEAF: Final[dict[str, ParquetElement]] = {
    "boolean": {1: _PQ_BOOLEAN},
    "int8": {1: _PQ_INT32, 6: _PQ_INT_8, 10: {10: {1: 8, 2: True}}},
    "int16": {1: _PQ_INT32, 6: _PQ_INT_16, 10: {10: {1: 16, 2: True}}},
    "int32": {1: _PQ_INT32},
    "int64": {1: _PQ_INT64},
    "float": {1: _PQ_FLOAT},
    "double": {1: _PQ_DOUBLE},
    "date": {1: _PQ_INT32, 6: _PQ_DATE, 10: {6: {}}},
    "time": {1: _PQ_INT64, 10: {7: {1: False, 2: {2: {}}}}},
    "timestamp": {
        1: _PQ_INT64,
        6: _PQ_TIMESTAMP_MICROS,
        10: {8: {1: False, 2: {2: {}}}},
    },
    "timestamp_ns": {1: _PQ_INT64, 10: {8: {1: False, 2: {3: {}}}}},
    "timestamptz": {
        1: _PQ_INT64,
        6: _PQ_TIMESTAMP_MICROS,
        10: {8: {1: True, 2: {2: {}}}},
    },
    "timestamptz_ns": {1: _PQ_INT64, 10: {8: {1: True, 2: {3: {}}}}},
    "binary": {1: _PQ_BYTE_ARRAY},
    "string": {1: _PQ_BYTE_ARRAY, 6: _PQ_UTF8, 10: {1: {}}},
    "uuid": {1: _PQ_FIXED, 2: 16, 10: {14: {}}},
}


def _binary(name: str, repetition: int) -> ParquetElement:
    return {1: _PQ_BYTE_ARRAY, 3: repetition, 4: name}


def _node_elements(
    node: Node,
    path: tuple[str, ...],
    out: list[tuple[tuple[str, ...], ParquetElement]],
) -> None:
    """The elements below the group at ``path`` that ``node`` lays out:
    its ``value``, then its ``typed_value`` and everything under it."""
    out.append(((*path, "value"), _binary("value", _PQ_OPTIONAL)))
    if node.kind == "variant":
        return
    typed = (*path, "typed_value")
    if node.kind == "object":
        out.append((typed, {3: _PQ_OPTIONAL, 4: "typed_value", 5: len(node.fields)}))
        for name, child in node.fields:
            _field_group((*typed, name), child, out)
    elif node.kind == "array":
        assert node.element is not None
        out.append(
            (typed, {3: _PQ_OPTIONAL, 4: "typed_value", 5: 1, 6: _PQ_LIST, 10: {3: {}}})
        )
        out.append(((*typed, "list"), {3: _PQ_REPEATED, 4: "list", 5: 1}))
        _field_group((*typed, "list", "element"), node.element, out)
    else:
        out.append((typed, {**_leaf_element(node), 3: _PQ_OPTIONAL, 4: "typed_value"}))


def _field_group(
    path: tuple[str, ...],
    node: Node,
    out: list[tuple[tuple[str, ...], ParquetElement]],
) -> None:
    # A field group and a list element are both REQUIRED (VS:122, 166); a
    # declared variant holds a value and nothing else.
    children = 1 if node.kind == "variant" else 2
    out.append((path, {3: _PQ_REQUIRED, 4: path[-1], 5: children}))
    _node_elements(node, path, out)


def _leaf_element(node: Node) -> ParquetElement:
    if node.kind not in ("decimal4", "decimal8", "decimal16"):
        return dict(_PARQUET_LEAF[node.kind])
    p, s = node.precision, node.scale
    decimal: ParquetElement = {6: _PQ_DECIMAL, 7: s, 8: p, 10: {5: {1: s, 2: p}}}
    if node.kind == "decimal16":
        # The fewest bytes that hold every unscaled value of the precision,
        # sign included: the connector's decimalByteLength, and pyarrow's.
        return {1: _PQ_FIXED, 2: (((10**p - 1).bit_length() + 1) + 7) // 8, **decimal}
    return {1: _PQ_INT32 if node.kind == "decimal4" else _PQ_INT64, **decimal}


# -- matching a value to a typed leaf (D3) ------------------------------------

#: A value the typed leaf does not take: it stays in ``value``.
_MISS: Final = object()

_INT_RANGES: Final = {
    "int8": (-0x80, 0x7F),
    "int16": (-0x8000, 0x7FFF),
    "int32": (-0x8000_0000, 0x7FFF_FFFF),
    "int64": (_INT64_MIN, _INT64_MAX),
}


def _matcher(node: Node) -> Callable[[object], object]:
    """The D3 rule for one primitive leaf: the typed value, or _MISS.

    A port of the Trino connector's ``VariantShredder.isShreddedPrimitive``
    over the Variant types D1 and D2 give Python values. A value goes to
    ``typed_value`` only in its own type class: an integer of any width
    into an integer leaf that holds it, a decimal into a decimal leaf of
    its scale whose precision holds it, and nothing across classes (an
    integer never into a double or a decimal). The encoder writes no
    float, nanosecond timestamp or long-encoded short string, so the
    float and nanosecond leaves take nothing it writes.
    """
    kind = node.kind
    if kind in _INT_RANGES:
        low, high = _INT_RANGES[kind]

        def match_int(value: object) -> object:
            if type(value) is int and low <= value <= high:
                return value
            return _MISS

        return match_int
    if kind in ("decimal4", "decimal8", "decimal16"):
        limit, scale = 10**node.precision, node.scale

        def match_decimal(value: object) -> object:
            # An int beyond int64 is decimal16 with scale 0 (D1).
            if type(value) is int:
                if _INT64_MIN <= value <= _INT64_MAX or scale:
                    return _MISS
                unscaled = value
            elif type(value) is Decimal:
                try:
                    value_scale, unscaled, _ = _decimal_parts(value)
                except _Invalid:
                    return _MISS  # invalid either way; the encoder says why
                if value_scale != scale:
                    return _MISS
            else:
                return _MISS
            return unscaled if -limit < unscaled < limit else _MISS

        return match_decimal
    exact = _EXACT_MATCH.get(kind)
    if exact is not None:
        return exact
    return _match_nothing


def _match_nothing(value: object) -> object:
    return _MISS


def _match_type(
    kind: type, convert: Callable[[Any], object]
) -> Callable[[object], object]:
    def match(value: object) -> object:
        return convert(value) if type(value) is kind else _MISS

    return match


def _match_timestamp(aware: bool) -> Callable[[object], object]:
    # Trino also refuses a timestamptz whose epoch milliseconds overflow
    # the 52 bits its TIMESTAMP WITH TIME ZONE keeps. A Python datetime
    # (years 1 to 9999) stays far inside them, which
    # test_shredding_table.py pins, so the guard has nothing to catch here.
    def match(value: object) -> object:
        if type(value) is datetime:
            value_aware, micros = _timestamp_micros(value)
            if value_aware == aware:
                return micros
        return _MISS

    return match


_EXACT_MATCH: Final[dict[str, Callable[[object], object]]] = {
    "boolean": _match_type(bool, lambda value: value),
    "double": _match_type(float, lambda value: value),
    "string": _match_type(str, _utf8),
    "binary": _match_type(bytes, lambda value: value),
    "uuid": _match_type(uuid.UUID, lambda value: value.bytes),
    "date": _match_type(date, lambda value: (value - _EPOCH_DAY).days),
    # An aware time is no Variant value at all: _time_micros refuses the
    # row (aware_time) here as the encoder would.
    "time": _match_type(time, _time_micros),
    "timestamp": _match_timestamp(aware=False),
    "timestamptz": _match_timestamp(aware=True),
}


# -- shredding into column builders (D3, D10) ---------------------------------


class _Budget:
    """The variable-length bytes of the chunk being built, every binary
    child together, so that none of them reaches int32 offsets."""

    __slots__ = ("used",)

    def __init__(self) -> None:
        self.used = 0


class _Builder:
    """The columns of one declared group, for one chunk of rows.

    ``add`` places a present value; ``absent`` writes a missing field, or
    the placeholder under a null parent that Parquet never writes because
    the definition levels stop at the parent. A row that fails part-way is
    cut back with ``truncate(rows)``: every list of a group is as long as
    its parent's count, so no per-row bookkeeping is needed.
    """

    __slots__ = ("budget", "node", "value")

    def __init__(self, node: Node | None, budget: _Budget) -> None:
        self.node = node
        self.budget = budget
        self.value: list[bytes | None] = []

    def add(self, value: object, ids: dict[str, int]) -> None:
        data = _encode(value, ids)
        self.value.append(data)
        self.budget.used += len(data)

    def absent(self) -> None:
        self.value.append(None)

    def truncate(self, rows: int) -> None:
        del self.value[rows:]

    def children(self) -> list[pa.Array]:
        return [pa.array(self.value, pa.binary())]

    def build(self) -> pa.StructArray:
        assert self.node is not None
        return pa.StructArray.from_arrays(
            self.children(), fields=list(self.node.write_type)
        )

    def count(
        self, report: VariantReport, column: str | None, group: pa.StructArray
    ) -> None:
        """Adds this group's typed, fallback and Variant-null counts (and its
        children's) to the report, from the built arrays."""


class _RequiredValue(_Builder):
    """The ``value`` of an unshredded column, which is REQUIRED (D11): a
    SQL NULL row holds an empty placeholder under its null group."""

    __slots__ = ()

    def absent(self) -> None:
        self.value.append(b"")


class _TypedBuilder(_Builder):
    __slots__ = ("typed",)

    def __init__(self, node: Node, budget: _Budget) -> None:
        super().__init__(node, budget)
        self.typed: list[Any] = []

    def absent(self) -> None:
        self.value.append(None)
        self.typed.append(None)

    def truncate(self, rows: int) -> None:
        del self.value[rows:]
        del self.typed[rows:]

    def children(self) -> list[pa.Array]:
        return [pa.array(self.value, pa.binary()), self.typed_array()]

    def typed_array(self) -> pa.Array:
        raise NotImplementedError

    def count(
        self, report: VariantReport, column: str | None, group: pa.StructArray
    ) -> None:
        assert self.node is not None
        value, typed = group.field(0), group.field(1)
        variant_nulls = (
            pc.sum(pc.equal(value, pa.scalar(NULL_VALUE, pa.binary()))).as_py() or 0
        )
        untyped = pc.sum(pc.and_(pc.is_valid(value), pc.is_null(typed))).as_py() or 0
        key = (column, self.node.path)
        report.typed[key] = report.typed.get(key, 0) + len(typed) - typed.null_count
        report.fallback[key] = report.fallback.get(key, 0) + untyped - variant_nulls
        report.variant_null[key] = report.variant_null.get(key, 0) + variant_nulls


class _PrimitiveBuilder(_TypedBuilder):
    __slots__ = ("match", "sized")

    def __init__(self, node: Node, budget: _Budget) -> None:
        super().__init__(node, budget)
        self.match = _matcher(node)
        self.sized = node.kind in ("string", "binary")

    def add(self, value: object, ids: dict[str, int]) -> None:
        typed: Any = self.match(value)
        if typed is _MISS:
            data = _encode(value, ids)
            self.value.append(data)
            self.typed.append(None)
            self.budget.used += len(data)
        else:
            # Exactly one of the two (VS:113): never both for a primitive.
            self.value.append(None)
            self.typed.append(typed)
            if self.sized:
                self.budget.used += len(typed)

    def typed_array(self) -> pa.Array:
        assert self.node is not None
        kind, values = self.node.kind, self.typed
        if kind == "string":
            # The UTF-8 bytes the match encoded once, viewed as text.
            return pa.array(values, pa.binary()).view(pa.string())
        if kind == "uuid":
            return pa.ExtensionArray.from_storage(
                pa.uuid(), pa.array(values, pa.binary(16))
            )
        if kind == "decimal16":
            return _decimal128(values, self.node.precision, self.node.scale)
        typed_type = self.node.write_type.field("typed_value").type
        if kind in ("decimal4", "decimal8"):
            # Unscaled, as decimal32/decimal64 hold them.
            width = pa.int32() if kind == "decimal4" else pa.int64()
            return pa.array(values, width).view(typed_type)
        return pa.array(values, typed_type)


def _decimal128(unscaled: list[int | None], precision: int, scale: int) -> pa.Array:
    """decimal128(p, s) from unscaled integers: Arrow's 16-byte little-endian
    two's complement, so no Decimal is built per value."""
    data = b"".join(
        (0 if value is None else value).to_bytes(16, "little", signed=True)
        for value in unscaled
    )
    validity = None
    if None in unscaled:
        validity = pa.array(
            [value is not None for value in unscaled], pa.bool_()
        ).buffers()[1]
    return pa.Array.from_buffers(
        pa.decimal128(precision, scale), len(unscaled), [validity, pa.py_buffer(data)]
    )


class _ObjectBuilder(_TypedBuilder):
    __slots__ = ("fields", "names")

    def __init__(self, node: Node, budget: _Budget) -> None:
        super().__init__(node, budget)
        self.fields = [(name, _builder(child, budget)) for name, child in node.fields]
        self.names = frozenset(name for name, _ in node.fields)

    def add(self, value: object, ids: dict[str, int]) -> None:
        if type(value) is not dict:
            data = _encode(value, ids)
            self.value.append(data)
            self.typed.append(False)
            self.budget.used += len(data)
            for _, child in self.fields:
                child.absent()
            return
        # An object always has a typed_value (D10). The residual holds the
        # keys not declared, by exact name, so a key that differs from a
        # declared one only by case stays here (D6); none when there are
        # none, so {} is a typed_value of missing fields.
        names = self.names
        rest = [name for name in value if name not in names]
        if rest:
            data = _encode_fields(value, rest, ids)
            self.value.append(data)
            self.budget.used += len(data)
        else:
            self.value.append(None)
        self.typed.append(True)
        for name, child in self.fields:
            if name in value:
                try:
                    child.add(value[name], ids)
                except _Invalid as exc:
                    exc.trail.append(name)
                    raise
            else:
                child.absent()

    def absent(self) -> None:
        self.value.append(None)
        self.typed.append(False)
        for _, child in self.fields:
            child.absent()

    def truncate(self, rows: int) -> None:
        super().truncate(rows)
        for _, child in self.fields:
            child.truncate(rows)

    def typed_array(self) -> pa.Array:
        assert self.node is not None
        typed_type = self.node.write_type.field("typed_value").type
        mask = (
            None
            if all(self.typed)
            else pa.array([not ok for ok in self.typed], pa.bool_())
        )
        return pa.StructArray.from_arrays(
            [child.build() for _, child in self.fields],
            fields=list(typed_type),
            mask=mask,
        )

    def count(
        self, report: VariantReport, column: str | None, group: pa.StructArray
    ) -> None:
        super().count(report, column, group)
        typed = group.field(1)
        for index, (_, child) in enumerate(self.fields):
            child.count(report, column, typed.field(index))


class _ArrayBuilder(_TypedBuilder):
    __slots__ = ("element", "offsets")

    def __init__(self, node: Node, budget: _Budget) -> None:
        super().__init__(node, budget)
        assert node.element is not None
        self.element = _builder(node.element, budget)
        self.offsets = [0]

    def add(self, value: object, ids: dict[str, int]) -> None:
        if type(value) is not list:
            data = _encode(value, ids)
            self.value.append(data)
            self.typed.append(False)
            self.offsets.append(self.offsets[-1])
            self.budget.used += len(data)
            return
        # An array is all in typed_value, every element with exactly one of
        # its two columns set: a null element is a value of Variant null
        # (VS:145-147), never a missing one.
        element = self.element
        for index, item in enumerate(value):
            try:
                element.add(item, ids)
            except _Invalid as exc:
                exc.trail.append(index)
                raise
        self.value.append(None)
        self.typed.append(True)
        self.offsets.append(self.offsets[-1] + len(value))
        # Elements count against the cap too: a list of typed numbers adds
        # no binary bytes, and list offsets are int32 as well.
        self.budget.used += len(value)

    def absent(self) -> None:
        self.value.append(None)
        self.typed.append(False)
        self.offsets.append(self.offsets[-1])

    def truncate(self, rows: int) -> None:
        super().truncate(rows)
        del self.offsets[rows + 1 :]
        self.element.truncate(self.offsets[-1])

    def typed_array(self) -> pa.Array:
        assert self.node is not None
        typed_type = self.node.write_type.field("typed_value").type
        mask = (
            None
            if all(self.typed)
            else pa.array([not ok for ok in self.typed], pa.bool_())
        )
        return pa.ListArray.from_arrays(
            pa.array(self.offsets, pa.int32()),
            self.element.build(),
            type=typed_type,
            mask=mask,
        )

    def count(
        self, report: VariantReport, column: str | None, group: pa.StructArray
    ) -> None:
        super().count(report, column, group)
        typed = group.field(1)
        offsets = typed.offsets
        start, stop = offsets[0].as_py(), offsets[-1].as_py()
        self.element.count(report, column, typed.values.slice(start, stop - start))


def _builder(node: Node, budget: _Budget) -> _Builder:
    if node.kind == "variant":
        return _Builder(node, budget)
    if node.kind == "object":
        return _ObjectBuilder(node, budget)
    if node.kind == "array":
        return _ArrayBuilder(node, budget)
    return _PrimitiveBuilder(node, budget)


class _Chunk:
    """One chunk of a column: the metadata and validity of each row, and the
    builder of the root group."""

    def __init__(self, plan: Plan) -> None:
        self.plan = plan
        self.budget = _Budget()
        self.root: _Builder = (
            _RequiredValue(None, self.budget)
            if plan.root is None
            else _builder(plan.root, self.budget)
        )
        self.metadata: list[bytes] = []
        self.valid: list[bool] = []

    def add(self, value: object, keys: set[str]) -> None:
        """One present row; on _Invalid nothing of it is kept."""
        try:
            metadata, ids = _metadata(keys)
            self.root.add(value, ids)
        except _Invalid as exc:
            self.root.truncate(len(self.metadata))
            if exc.key is not None:
                exc.trail = _key_trail(value, exc.key) or []
            raise
        self.metadata.append(metadata)
        self.valid.append(True)
        self.budget.used += len(metadata)

    def null(self) -> None:
        """A SQL NULL row: a null group over placeholders. The metadata
        placeholder is empty, because the child is REQUIRED (D4)."""
        self.root.absent()
        self.metadata.append(b"")
        self.valid.append(False)

    def __len__(self) -> int:
        return len(self.metadata)

    def build(self, report: VariantReport, column: str | None) -> pa.StructArray:
        mask = (
            None
            if all(self.valid)
            else pa.array([not ok for ok in self.valid], pa.bool_())
        )
        children = [pa.array(self.metadata, pa.binary()), *self.root.children()]
        array = pa.StructArray.from_arrays(
            children, fields=list(self.plan.write_type), mask=mask
        )
        fault = structure_fault(array)
        if fault is not None:
            raise VariantEncodingError(
                f"pyhoglake built malformed VARIANT storage ({fault}); this is a "
                "pyhoglake bug",
                column=column,
                reason="invalid_shredding",
            )
        if self.plan.root is not None:
            # The root group's columns are the storage struct's own, after
            # metadata: value and typed_value.
            group = pa.StructArray.from_arrays(
                [array.field(1), array.field(2)], fields=list(self.plan.root.write_type)
            )
            self.root.count(report, column, group)
        return array


#: Rows and encoded bytes per chunk. The rows bound the Python objects alive
#: at once (about 4.7 KB a row before the Arrow build, measured on 1 KB
#: PostHog properties); the bytes keep every binary child well under the
#: 2 GiB of int32 offsets, whatever the rows hold.
CHUNK_ROWS: Final = 8192
CHUNK_BYTES: Final = 512 * 1024 * 1024


def encode(
    values: Iterable[object],
    plan: Plan,
    report: VariantReport,
    *,
    json_text: bool,
    column: str | None,
    nullable: bool,
    on_invalid: Literal["raise", "null"],
    chunk_rows: int = CHUNK_ROWS,
    chunk_bytes: int = CHUNK_BYTES,
) -> list[pa.StructArray]:
    """Encodes ``values`` (JSON text, or Python objects) as storage chunks
    of ``plan.write_type`` and adds their counts to ``report``.

    None is SQL NULL. A row that cannot be encoded raises
    VariantEncodingError, or with ``on_invalid="null"`` is written as SQL
    NULL and counted by reason. SQL NULL into a NOT NULL target raises
    either way (the caller refuses ``"null"`` for one).
    """
    started = _clock.perf_counter()
    prepare = _prepare_json if json_text else _prepare_python
    chunks: list[pa.StructArray] = []
    chunk = _Chunk(plan)
    row = -1
    for row, raw in enumerate(values):
        if raw is None:
            if not nullable:
                raise VariantEncodingError(
                    _message(column, row, None, "SQL NULL into a NOT NULL column"),
                    column=column,
                    row=row,
                    reason="sql_null_in_not_null",
                )
            chunk.null()
            report.sql_null_rows += 1
        else:
            try:
                value, keys = prepare(raw)
                chunk.add(value, keys)
            except _Invalid as exc:
                path = exc.path()
                if on_invalid == "raise":
                    raise VariantEncodingError(
                        _message(column, row, path, exc.detail),
                        column=column,
                        row=row,
                        path=path,
                        reason=exc.reason,
                    ) from None
                chunk.null()
                report.invalid_rows += 1
                report.invalid_by_reason[exc.reason] = (
                    report.invalid_by_reason.get(exc.reason, 0) + 1
                )
                if report.first_invalid is None:
                    report.first_invalid = (column, row, exc.reason, path)
        if len(chunk) >= chunk_rows or chunk.budget.used >= chunk_bytes:
            chunks.append(chunk.build(report, column))
            chunk = _Chunk(plan)
    if len(chunk) or not chunks:
        chunks.append(chunk.build(report, column))
    report.rows += row + 1
    report.encode_seconds += _clock.perf_counter() - started
    return chunks


def _message(column: str | None, row: int | None, path: str | None, detail: str) -> str:
    where = f"variant column '{column}'" if column is not None else "variant value"
    if row is not None:
        where += f" row {row}"
    if path is not None:
        where += f" at {path}"
    return f"{where}: {detail}"


# -- structural invariants ----------------------------------------------------


def structure_fault(array: pa.StructArray) -> str | None:
    """The first null that a REQUIRED child holds under a present parent, or
    None.

    pyarrow writes such a null without complaint: a null row of a
    non-nullable struct becomes a present group of empty bytes. This is a
    vectorised backstop for this encoder's output and the structural guard
    for storage built elsewhere; :func:`pyhoglake.variant.verify` checks
    the bytes.
    """
    return _fault(array, pa.array([True] * len(array), pa.bool_()), "")


def _fault(array: pa.Array, present: pa.Array, path: str) -> str | None:
    kind = array.type
    if pa.types.is_struct(kind):
        own = pc.and_(present, pc.is_valid(array))
        for index, field in enumerate(kind):
            child = array.field(index)
            # Spelt as keys are (member), since a column may be named "a.b".
            name = path + member(field.name)
            if (
                not field.nullable
                and child.null_count
                and pc.any(pc.and_(pc.is_null(child), own)).as_py()
            ):
                return f"{_column_path(name)} is null under a present parent"
            fault = _fault(child, own, name)
            if fault is not None:
                return fault
        return None
    if pa.types.is_list(kind) or pa.types.is_large_list(kind):
        own = pc.and_(present, pc.is_valid(array))
        offsets = array.offsets
        start, stop = offsets[0].as_py(), offsets[-1].as_py()
        values = array.values.slice(start, stop - start)
        parents = pc.list_parent_indices(array)
        # A null list over a non-empty range holds elements no reader sees.
        element_present = pc.take(own, parents)
        name = path + member(kind.value_field.name)
        if (
            not kind.value_field.nullable
            and values.null_count
            and pc.any(pc.and_(pc.is_null(values), element_present)).as_py()
        ):
            return f"{_column_path(name)} is null under a present list"
        return _fault(values, element_present, name)
    return None


# -- decoding -----------------------------------------------------------------
#
# Written from the spec, apart from the encoder above: it is what the
# encoder is checked against. ``canonical`` adds the rules pyhoglake's encoder
# keeps (D8, D9) to the spec's own.


def _smallest(n: int) -> int:
    return 1 if n <= 0xFF else 2 if n <= 0xFFFF else 3 if n <= 0xFFFFFF else 4


def _bad(detail: str, reason: Reason = "malformed_variant") -> _Invalid:
    return _Invalid(reason, detail)


def _read_uint(data: bytes, pos: int, width: int, end: int) -> int:
    if pos + width > end:
        raise _bad("truncated")
    return int.from_bytes(data[pos : pos + width], "little")


def decode_metadata(
    data: bytes, *, strict: bool = False, canonical: bool = False
) -> tuple[str, ...]:
    """The dictionary of a metadata buffer (VE:74-146). ``strict`` holds it
    to the spec's writer rules, and ``canonical``, which implies it, to
    pyhoglake's encoding."""
    if not data:
        raise _bad("the metadata is empty")
    header = data[0]
    if header & 0x0F != 1:
        raise _bad(f"metadata version {header & 0x0F}, not 1")
    sorted_flag = bool(header & 0x10)
    width = (header >> 6) + 1
    end = len(data)
    count = _read_uint(data, 1, width, end)
    start = 1 + width
    if start + (count + 1) * width > end:
        raise _bad("the metadata is truncated")
    offsets = [
        _read_uint(data, start + i * width, width, end) for i in range(count + 1)
    ]
    base = start + (count + 1) * width
    if offsets[0] != 0 or any(a > b for a, b in pairwise(offsets)):
        raise _bad("the metadata offsets do not climb from 0")
    if base + offsets[-1] != end:
        raise _bad("the metadata length does not match its offsets")
    raw = [data[base + offsets[i] : base + offsets[i + 1]] for i in range(count)]
    try:
        names = tuple(name.decode("utf-8") for name in raw)
    except UnicodeDecodeError:
        raise _bad("a metadata string is not UTF-8") from None
    if (strict or canonical) and sorted_flag and any(a >= b for a, b in pairwise(raw)):
        # A writer's fault, and only a writer's: DuckDB 1.5.5 sets the flag
        # on every dictionary, and the Trino connector reads its files as
        # if the flag were unset (VariantRepairs.withVerifiedSortedFlag).
        # This reader looks names up by id, never by the flag.
        raise _bad("sorted_strings is set, but the strings are not sorted and unique")
    if canonical:
        # Bit 5 is reserved, and readers ignore it (VE:90): it does not
        # change the value, but it is a second spelling of the bytes.
        if header & 0x20:
            raise _bad("the metadata header sets a reserved bit", "non_canonical")
        if sorted_flag != bool(count):
            raise _bad(
                "sorted_strings is not set exactly when there are strings",
                "non_canonical",
            )
        if width != _smallest(max(offsets[-1], count)):
            raise _bad("the metadata offset width is not the smallest", "non_canonical")
    return names


def decode_value(
    data: bytes,
    names: tuple[str, ...],
    *,
    strict: bool = False,
    canonical: bool = False,
    depth: int = 0,
) -> object:
    """One value buffer, wholly. A Variant null is None. ``depth`` is where
    the buffer sits in the whole value: a shredded field's ``value`` is
    below its object. ``strict`` and ``canonical`` as for
    :func:`decode_metadata`."""
    value, end = _decode(
        data, 0, len(data), names, strict or canonical, canonical, depth
    )
    if end != len(data):
        raise _bad(f"{len(data) - end} bytes follow the value")
    return value


_FIXED: Final = {
    3: (1, "<b"),
    4: (2, "<h"),
    5: (4, "<i"),
    6: (8, "<q"),
    7: (8, "<d"),
    11: (4, "<i"),
    12: (8, "<q"),
    13: (8, "<q"),
    14: (4, "<f"),
    17: (8, "<q"),
    18: (8, "<q"),
    19: (8, "<q"),
}


def _decode(
    data: bytes,
    pos: int,
    end: int,
    names: tuple[str, ...],
    strict: bool,
    canonical: bool,
    depth: int,
) -> tuple[object, int]:
    if pos >= end:
        raise _bad("truncated")
    header = data[pos]
    basic, head = header & 0x03, header >> 2
    if basic == 1:
        stop = pos + 1 + head
        if stop > end:
            raise _bad("a short string is truncated")
        return _text(data[pos + 1 : stop]), stop
    if basic == 0:
        return _decode_primitive(data, pos, end, head, canonical)
    if basic == 2:
        return _decode_object(data, pos, end, head, names, strict, canonical, depth)
    return _decode_array(data, pos, end, head, names, strict, canonical, depth)


def _text(raw: bytes) -> str:
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        raise _bad("a string is not UTF-8") from None


def _decode_primitive(
    data: bytes, pos: int, end: int, type_id: int, canonical: bool
) -> tuple[object, int]:
    body = pos + 1
    if type_id == 0:
        return None, body
    if type_id in (1, 2):
        return type_id == 1, body
    if type_id in _FIXED:
        size, fmt = _FIXED[type_id]
        if body + size > end:
            raise _bad("a primitive is truncated")
        (raw,) = struct.unpack_from(fmt, data, body)
        stop = body + size
        if 3 <= type_id <= 6:
            if canonical and _int_type(raw) != type_id:
                raise _bad(
                    f"the integer {raw} is not in its smallest width", "non_canonical"
                )
            return raw, stop
        if canonical and type_id in (14, 18, 19):
            raise _bad(
                f"primitive type {type_id}, which pyhoglake never writes",
                "non_canonical",
            )
        return _temporal(type_id, raw), stop
    if type_id in (8, 9, 10):
        size, most = _DECIMAL_WIDTHS[type_id]
        if body + 1 + size > end:
            raise _bad("a decimal is truncated")
        # The scale is 0 to 38 at every width (VE:422-424), and each width
        # holds at most 9, 18 or 38 digits (VE:442-446): a reader that
        # enforces it (Spark's checkDecimal) refuses more, so more is
        # malformed, not merely wider than needed.
        scale = data[body]
        if scale > 38:
            raise _bad(f"a decimal has scale {scale}")
        unscaled = int.from_bytes(
            data[body + 1 : body + 1 + size], "little", signed=True
        )
        digits = len(str(abs(unscaled)))
        if digits > most:
            raise _bad(f"a decimal{size} holds {digits} digits, beyond its {most}")
        precision = max(digits, scale)
        if canonical and type_id != (
            8 if precision <= 9 else 9 if precision <= 18 else 10
        ):
            raise _bad(
                f"a decimal of precision {precision} is not in its smallest width",
                "non_canonical",
            )
        return Decimal(unscaled).scaleb(-scale, _EXACT), body + 1 + size
    if type_id in (15, 16):
        size = _read_uint(data, body, 4, end)
        stop = body + 4 + size
        if stop > end:
            raise _bad("a binary or string is truncated")
        if type_id == 15:
            return data[body + 4 : stop], stop
        if canonical and size < 64:
            raise _bad(
                "a string of under 64 bytes is not a short string", "non_canonical"
            )
        return _text(data[body + 4 : stop]), stop
    if type_id == 20:
        if body + 16 > end:
            raise _bad("a uuid is truncated")
        return uuid.UUID(bytes=bytes(data[body : body + 16])), body + 16
    raise _bad(f"unknown primitive type {type_id}")


#: Bytes and largest precision of decimal4, decimal8 and decimal16.
_DECIMAL_WIDTHS: Final = {8: (4, 9), 9: (8, 18), 10: (16, 38)}


def _int_type(value: int) -> int:
    if -0x80 <= value <= 0x7F:
        return 3
    if -0x8000 <= value <= 0x7FFF:
        return 4
    if -0x8000_0000 <= value <= 0x7FFF_FFFF:
        return 5
    return 6


def _temporal(type_id: int, raw: Any) -> object:
    if type_id in (7, 14):
        return raw
    if type_id == 11:
        return _date(raw)
    if type_id in (12, 13):
        return _timestamp(raw, aware=type_id == 12)
    if type_id == 17:
        return _time_of_day(raw)
    # 18, 19: nanoseconds, which no Python datetime holds.
    return pa.scalar(raw, pa.timestamp("ns", tz="UTC" if type_id == 18 else None))


# A date or timestamp beyond Python's years 1 to 9999 is valid all the
# same (Trino writes them), so it reads as the pyarrow scalar of its type,
# as a nanosecond timestamp always does, rather than as an error.


def _date(days: int) -> object:
    if _MIN_DAY <= days <= _MAX_DAY:
        return _EPOCH_DAY + timedelta(days=days)
    return pa.scalar(days, pa.date32())


def _timestamp(micros: int, *, aware: bool) -> object:
    if _MIN_MICROS <= micros <= _MAX_MICROS:
        return (_EPOCH_UTC if aware else _EPOCH) + timedelta(microseconds=micros)
    return pa.scalar(micros, pa.timestamp("us", tz="UTC" if aware else None))


def _time_of_day(micros: int) -> time:
    # A time of day (VE:431); Trino shreds only those, too.
    if not 0 <= micros < _MICROS_PER_DAY:
        raise _bad(f"a time of {micros} microseconds is not a time of day")
    seconds, micro = divmod(micros, 1_000_000)
    minutes, second = divmod(seconds, 60)
    return time(minutes // 60, minutes % 60, second, micro)


def _decode_object(
    data: bytes,
    pos: int,
    end: int,
    head: int,
    names: tuple[str, ...],
    strict: bool,
    canonical: bool,
    depth: int,
) -> tuple[object, int]:
    large = head >> 4 & 1
    id_width = (head >> 2 & 0x03) + 1
    offset_width = (head & 0x03) + 1
    count_width = 4 if large else 1
    count = _read_uint(data, pos + 1, count_width, end)
    _check_depth(count, depth)
    ids_at = pos + 1 + count_width
    offsets_at = ids_at + count * id_width
    base = offsets_at + (count + 1) * offset_width
    if base > end:
        raise _bad("an object header is truncated")
    ids = [_read_uint(data, ids_at + i * id_width, id_width, end) for i in range(count)]
    offsets = [
        _read_uint(data, offsets_at + i * offset_width, offset_width, end)
        for i in range(count + 1)
    ]
    stop = base + offsets[-1]
    if stop > end:
        raise _bad("an object is truncated")
    if any(field_id >= len(names) for field_id in ids):
        raise _bad("an object field id is beyond the metadata")
    keys = [names[field_id] for field_id in ids]
    encoded = [key.encode("utf-8") for key in keys]
    # Sorted and unique by UTF-8 bytes (VE:456, 465). DuckDB 1.5.5 lists
    # fields in field-id order over an unsorted dictionary, which the
    # Trino connector sorts on read (VariantRepairs.withSortedObjectFields):
    # a reader takes fields in any order, but never one name twice.
    in_order = all(a < b for a, b in pairwise(encoded))
    if not in_order:
        if strict:
            raise _bad("object keys are not sorted and unique")
        repeated = _first_repeat(keys)
        if repeated is not None:
            raise _bad(f"an object has two fields named {_quoted(repeated)}")
    # The values may lie in any order, but they are num_elements values,
    # offsets count from the first byte of the first of them, and the last
    # offset is the byte after the last (VE:293-296): the least starts at
    # 0, and each ends where the next one up begins. That holds in every
    # mode, as it does for an array's elements. Two fields that share a
    # value would let a few hundred bytes decode to 2**depth values, and a
    # gap before, between or after the values hides bytes no reader sees:
    # DuckDB 1.5.5 spells a string of exactly 64 bytes as an empty short
    # string followed by them.
    starts = sorted(offsets[:-1])
    if any(a == b for a, b in pairwise(starts)):
        raise _bad("two object fields start at one offset")
    if not starts and offsets[-1]:
        raise _bad("an empty object's last offset is not 0")
    if starts and starts[0]:
        raise _bad("no object field starts at offset 0")
    # zip(strict=False): an empty object has a last offset and no value,
    # so the two lists differ in length.
    ends = dict(zip(starts, [*starts[1:], offsets[-1]], strict=False))
    if canonical:
        # Bit 5 of the header is reserved (VE:210), like the metadata's.
        _canonical_container(head & 0x20, large, count, offset_width, offsets)
        if id_width != _smallest(max(ids, default=0)):
            raise _bad("the field id width is not the smallest", "non_canonical")
    out: dict[str, object] = {}
    for index, key in enumerate(keys):
        start = base + offsets[index]
        try:
            value, value_end = _decode(
                data, start, stop, names, strict, canonical, depth + 1
            )
            if value_end != base + ends[offsets[index]]:
                raise _bad("an object field does not end where the next value begins")
            if canonical and value_end != base + offsets[index + 1]:
                raise _bad(
                    "object values are not laid out in key order", "non_canonical"
                )
        except _Invalid as exc:
            exc.trail.append(key)
            raise
        out[key] = value
    # In the Variant's key order, which is code-point order for str.
    return (out if in_order else dict(sorted(out.items()))), stop


def _first_repeat(keys: list[str]) -> str | None:
    seen: set[str] = set()
    for key in keys:
        if key in seen:
            return key
        seen.add(key)
    return None


def _decode_array(
    data: bytes,
    pos: int,
    end: int,
    head: int,
    names: tuple[str, ...],
    strict: bool,
    canonical: bool,
    depth: int,
) -> tuple[object, int]:
    large = head >> 2 & 1
    offset_width = (head & 0x03) + 1
    count_width = 4 if large else 1
    count = _read_uint(data, pos + 1, count_width, end)
    _check_depth(count, depth)
    offsets_at = pos + 1 + count_width
    base = offsets_at + (count + 1) * offset_width
    if base > end:
        raise _bad("an array header is truncated")
    offsets = [
        _read_uint(data, offsets_at + i * offset_width, offset_width, end)
        for i in range(count + 1)
    ]
    stop = base + offsets[-1]
    if stop > end:
        raise _bad("an array is truncated")
    # Offsets count from the first byte of the first element (VE:349), so
    # bytes before it are hidden from every reader, as an object's are.
    if offsets[0]:
        raise _bad(
            "the first array element does not start at offset 0"
            if count
            else "an empty array's last offset is not 0"
        )
    if canonical:
        # Bits 3 to 5 of the header are reserved (VE:228).
        _canonical_container(head & 0x38, large, count, offset_width, offsets)
    out: list[object] = []
    for index in range(count):
        try:
            value, value_end = _decode(
                data, base + offsets[index], stop, names, strict, canonical, depth + 1
            )
            if value_end != base + offsets[index + 1]:
                raise _bad("an array element does not end at the next offset")
        except _Invalid as exc:
            exc.trail.append(index)
            raise
        out.append(value)
    return out, stop


def _check_depth(count: int, depth: int) -> None:
    # The encoder's bound (D7), over the whole value: an empty container
    # may sit at the deepest level, and nothing below it. The bytes may be
    # valid (the spec sets no bound, and DuckDB writes deeper), so this is
    # the reader's limit, nesting_too_deep, not malformed_variant: it keeps
    # this recursion bounded on bytes from anywhere.
    if count and depth >= MAX_VARIANT_DEPTH:
        raise _bad(
            f"nested more than {MAX_VARIANT_DEPTH} levels deep", "nesting_too_deep"
        )


def _canonical_container(
    reserved: int, large: int, count: int, offset_width: int, offsets: list[int]
) -> None:
    # No first-offset rule: a container's values start at 0 in every mode,
    # and an object's in key order then puts its first key there.
    if reserved:
        raise _bad("a container header sets a reserved bit", "non_canonical")
    if bool(large) != (count > 255):
        raise _bad("is_large is not set exactly above 255 elements", "non_canonical")
    if offset_width != _smallest(offsets[-1]):
        raise _bad("the offset width is not the smallest", "non_canonical")


# -- reassembling storage -----------------------------------------------------

#: A field or element both of whose columns are null: missing.
_MISSING: Final = object()

_Reader: TypeAlias = Callable[[int, tuple[str, ...]], object]


class _Reassembly:
    """Reads one storage chunk back into Python values (VS:286-334).

    Any conformant layout: pyhoglake's, Trino's, or DuckDB's, whose field
    and element groups are OPTIONAL. Columns are found by name. A null
    field group is a missing field; with ``strict`` (verify) the spec's
    writer rules hold too: no null groups, no missing element, no row
    with both columns null, every key in the metadata, a truthful sorted
    flag and fields in key order. ``canonical`` adds the encoding
    pyhoglake writes (D8, D9).
    """

    def __init__(self, array: pa.StructArray, *, strict: bool, canonical: bool) -> None:
        self.strict = strict
        self.canonical = canonical
        kind = array.type
        if not pa.types.is_struct(kind) or "metadata" not in _unique_names(kind):
            raise _bad(
                "VARIANT storage is a struct with a metadata column",
                "invalid_shredding",
            )
        _binary_column(kind.field("metadata"))
        self.metadata = array.field("metadata").to_pylist()
        self.valid = array.is_valid().to_pylist()
        self.root = self._group(array, (), 0, top=True)
        self._names: dict[bytes, tuple[str, ...]] = {}

    def row(self, index: int) -> object:
        if not self.valid[index]:
            return None
        raw = self.metadata[index]
        if raw is None:
            raise _bad("a present row has no metadata", "invalid_shredding")
        names = self._names.get(raw)
        if names is None:
            names = decode_metadata(raw, strict=self.strict, canonical=self.canonical)
            self._names[raw] = names
        value = self.root(index, names)
        if value is _MISSING:
            # A missing value where one is required reads as Variant null
            # (VS:78); a writer must write 00 instead.
            if self.strict:
                raise _bad(
                    "a present row has neither value nor typed_value",
                    "invalid_shredding",
                )
            return VARIANT_NULL
        if self.strict:
            used: set[str] = set()
            _keys_of(value, used)
            missing = used.difference(names)
            if missing:
                raise _bad(
                    f"key {_quoted(min(missing))} is not in the metadata",
                    "invalid_shredding",
                )
            if self.canonical and used != set(names):
                raise _bad(
                    "the metadata holds keys the value does not use", "non_canonical"
                )
        return VARIANT_NULL if value is None else value

    def _group(
        self,
        group: pa.Array,
        path: tuple[str, ...],
        depth: int,
        *,
        top: bool = False,
        element: bool = False,
    ) -> _Reader:
        """A reader of the group at ``path``, whose value is at ``depth``
        of the whole value."""
        kind = group.type
        if not pa.types.is_struct(kind):
            raise _bad(
                f"{_column_path(''.join(map(member, path)))} is "
                f"{_type_text(kind)}, not a group",
                "invalid_shredding",
            )
        names = _unique_names(kind)
        extra = names - {"value", "typed_value", *(["metadata"] if top else [])}
        if extra:
            raise _bad(
                f"a group has a column {_quoted(min(extra))}", "invalid_shredding"
            )
        if "value" not in names and not element:
            # Arrays may drop value when elements are typed (VS:126);
            # the variant group and object fields keep it.
            raise _bad("a group has no value column", "invalid_shredding")
        values = None
        if "value" in names:
            _binary_column(kind.field("value"))
            values = group.field("value").to_pylist()
        typed = None
        if "typed_value" in names:
            typed = self._typed(
                group.field("typed_value"), (*path, "typed_value"), depth
            )
        present = None if top else group.is_valid().to_pylist()
        strict, canonical = self.strict, self.canonical

        def read(index: int, metadata: tuple[str, ...]) -> object:
            if present is not None and not present[index]:
                if strict:
                    raise _bad("a field or element group is null", "invalid_shredding")
                return _MISSING
            raw = values[index] if values is not None else None
            if typed is not None:
                shredded = typed(index, metadata, raw)
                if shredded is not _MISSING:
                    return shredded
            if raw is not None:
                return decode_value(
                    raw, metadata, strict=strict, canonical=canonical, depth=depth
                )
            return _MISSING

        return read

    def _typed(
        self, typed: pa.Array, path: tuple[str, ...], depth: int
    ) -> Callable[[int, tuple[str, ...], bytes | None], object]:
        """A reader of ``typed_value`` given the group's ``value`` bytes;
        _MISSING when the row's typed_value is null."""
        kind = typed.type
        present = typed.is_valid().to_pylist()
        strict, canonical = self.strict, self.canonical
        container = (
            pa.types.is_struct(kind)
            or pa.types.is_list(kind)
            or pa.types.is_large_list(kind)
        )
        if container and depth > MAX_VARIANT_DEPTH:
            # A shredded object or array below the deepest level can hold
            # nothing this reader reads, and building its reader would
            # recurse once a level, as deep as the storage goes.
            raise _bad(
                f"the storage shreds values nested more than {MAX_VARIANT_DEPTH} "
                "levels deep",
                "nesting_too_deep",
            )
        if pa.types.is_struct(kind):
            # A name twice would be one key read twice, the later winning.
            _unique_names(kind)
            fields = [
                (
                    field.name,
                    self._group(typed.field(i), (*path, field.name), depth + 1),
                )
                for i, field in enumerate(kind)
            ]
            declared = frozenset(name for name, _ in fields)
            # Which rows hold each field, wanted only at the deepest level,
            # where an object may be empty and nothing more: the bound of
            # bytes (_check_depth), counted before any field is read.
            occupied = (
                [_occupied(typed.field(i)) for i in range(kind.num_fields)]
                if depth >= MAX_VARIANT_DEPTH
                else []
            )

            def read_object(
                index: int, metadata: tuple[str, ...], raw: bytes | None
            ) -> object:
                if not present[index]:
                    if raw is None:
                        return _MISSING
                    value = decode_value(
                        raw, metadata, strict=strict, canonical=canonical, depth=depth
                    )
                    if isinstance(value, dict):
                        # An object's typed_value is never null (VS:162,
                        # and the INVALID rows of the table at VS:218-220).
                        raise _bad(
                            "an object is in value while typed_value is null",
                            "invalid_shredding",
                        )
                    return value
                _check_depth(sum(rows[index] for rows in occupied), depth)
                out: dict[str, object] = {}
                for name, reader in fields:
                    try:
                        value = reader(index, metadata)
                    except _Invalid as exc:
                        exc.trail.append(name)
                        raise
                    if value is not _MISSING:
                        out[name] = value
                if raw is not None:
                    residual = decode_value(
                        raw, metadata, strict=strict, canonical=canonical, depth=depth
                    )
                    if not isinstance(residual, dict):
                        raise _bad(
                            "a non-object value has shredded fields",
                            "invalid_shredding",
                        )
                    if canonical and not residual:
                        raise _bad(
                            "a partially shredded object has an empty residual",
                            "non_canonical",
                        )
                    clash = declared.intersection(residual)
                    if clash:
                        raise _bad(
                            f"shredded field {_quoted(min(clash))} is in value too",
                            "invalid_shredding",
                        )
                    out.update(residual)
                return dict(sorted(out.items()))

            return read_object
        if pa.types.is_list(kind) or pa.types.is_large_list(kind):
            offsets = typed.offsets.to_pylist()
            elements = self._group(
                typed.values, (*path, "list", "element"), depth + 1, element=True
            )

            def read_array(
                index: int, metadata: tuple[str, ...], raw: bytes | None
            ) -> object:
                if not present[index]:
                    if raw is None:
                        return _MISSING
                    value = decode_value(
                        raw, metadata, strict=strict, canonical=canonical, depth=depth
                    )
                    if strict and isinstance(value, list):
                        # An array goes to typed_value (VS:119-120).
                        raise _bad(
                            "an array is in value while typed_value is null",
                            "invalid_shredding",
                        )
                    return value
                if raw is not None:
                    raise _bad(
                        "an array has a value as well as typed_value",
                        "invalid_shredding",
                    )
                _check_depth(offsets[index + 1] - offsets[index], depth)
                out: list[object] = []
                for position in range(offsets[index], offsets[index + 1]):
                    try:
                        value = elements(position, metadata)
                    except _Invalid as exc:
                        exc.trail.append(position - offsets[index])
                        raise
                    if value is _MISSING:
                        # Elements are never missing (VS:145); a reader
                        # returns Variant null for one.
                        if strict:
                            raise _bad(
                                "an array element is missing", "invalid_shredding"
                            )
                        value = None
                    out.append(value)
                return out

            return read_array
        leaves, read_leaf = _leaves(typed)
        if canonical and (
            kind == pa.float32() or (pa.types.is_timestamp(kind) and kind.unit == "ns")
        ):
            # Declared float and nanosecond leaves take nothing pyhoglake
            # writes (D3), as in value it writes no primitive 14, 18 or 19.
            read_leaf = _never_written(kind)
        if not canonical:
            return _primitive(present, leaves, read_leaf)
        return _placed(_primitive(present, leaves, read_leaf), kind, depth)


def _primitive(
    present: list[bool], leaves: list[Any], convert: Callable[[Any], object] | None
) -> Callable[[int, tuple[str, ...], bytes | None], object]:
    def read_primitive(
        index: int, metadata: tuple[str, ...], raw: bytes | None
    ) -> object:
        if not present[index]:
            return _MISSING
        if raw is not None:
            raise _bad(
                "a primitive has both value and typed_value", "invalid_shredding"
            )
        leaf = leaves[index]
        return leaf if convert is None else convert(leaf)

    return read_primitive


def _placed(
    read: Callable[[int, tuple[str, ...], bytes | None], object],
    kind: pa.DataType,
    depth: int,
) -> Callable[[int, tuple[str, ...], bytes | None], object]:
    """``read``, refusing a row whose ``value`` holds what the typed leaf
    takes: the same row with the value in ``typed_value``, which is where
    pyhoglake puts it (D3, D10), is a second spelling of it. Canonical
    only: other writers shred by rules of their own."""
    takes = _taken_by(kind)
    if takes is None:
        return read

    def read_placed(index: int, metadata: tuple[str, ...], raw: bytes | None) -> object:
        shredded = read(index, metadata, raw)
        if shredded is not _MISSING or raw is None:
            return shredded
        # Decoded here rather than by the group, which would do it next.
        value = decode_value(raw, metadata, canonical=True, depth=depth)
        if takes(value):
            raise _bad(
                f"value holds what its typed {_type_text(kind)} leaf takes",
                "non_canonical",
            )
        return value

    return read_placed


#: The range of each integer leaf, by its Arrow type.
_LEAF_INT_RANGES: Final = {
    pa.int8(): (-0x80, 0x7F),
    pa.int16(): (-0x8000, 0x7FFF),
    pa.int32(): (-0x8000_0000, 0x7FFF_FFFF),
    pa.int64(): (_INT64_MIN, _INT64_MAX),
}


def _taken_by(kind: pa.DataType) -> Callable[[object], bool] | None:
    """Which decoded values a typed leaf of ``kind`` takes (D3), or None
    for a leaf that takes nothing pyhoglake writes (float, nanoseconds).

    D3 over what the decoder gives, written apart from the encoder's
    _matcher, which works on the values a caller hands it: a Variant
    integer of any width is an int, a Variant decimal a Decimal whose
    exponent is minus its scale.
    """
    if kind in _LEAF_INT_RANGES:
        low, high = _LEAF_INT_RANGES[kind]
        return lambda v: type(v) is int and low <= v <= high
    if pa.types.is_decimal(kind):
        limit, scale = 10**kind.precision, kind.scale

        def takes_decimal(v: object) -> bool:
            if type(v) is not Decimal or v.as_tuple().exponent != -scale:
                return False
            return abs(int(v.scaleb(scale, _EXACT))) < limit

        return takes_decimal
    exact: type | None = None
    if pa.types.is_boolean(kind):
        exact = bool
    elif kind == pa.float64():
        exact = float
    elif kind in _STRING_AS_BINARY:
        exact = str
    elif kind in (pa.binary(), pa.large_binary(), pa.binary_view()):
        exact = bytes
    elif kind == pa.date32():
        exact = date
    elif kind == pa.time64("us"):
        exact = time
    elif isinstance(kind, pa.UuidType):
        exact = uuid.UUID
    elif pa.types.is_timestamp(kind) and kind.unit == "us":
        aware = kind.tz is not None
        return lambda v: type(v) is datetime and (v.tzinfo is not None) == aware
    if exact is None:
        return None
    return lambda v: type(v) is exact


def _never_written(kind: pa.DataType) -> Callable[[Any], object]:
    def refuse(_: Any) -> object:
        raise _bad(
            f"a typed {kind} leaf holds a value, which pyhoglake never writes",
            "non_canonical",
        )

    return refuse


def _occupied(group: pa.StructArray) -> list[bool]:
    """Which rows of a field group hold the field: the group is present
    and one of its columns is set."""
    held = [
        group.field(name).is_valid().to_pylist()
        for name in ("value", "typed_value")
        if group.type.get_field_index(name) >= 0
    ]
    return [
        present and any(columns)
        for present, *columns in zip(group.is_valid().to_pylist(), *held, strict=True)
    ]


def _unique_names(kind: pa.StructType) -> set[str]:
    """The column names of a group, refusing a name it repeats: storage is
    read by name (VS:54), and two shredded fields cannot share one."""
    names = {field.name for field in kind}
    if len(names) != kind.num_fields:
        repeated = min(n for n in names if kind.get_field_index(n) < 0)
        raise _bad(
            f"a group has two columns named {_quoted(repeated)}", "invalid_shredding"
        )
    return names


def _binary_column(field: pa.Field) -> None:
    kind = field.type
    if not (
        pa.types.is_binary(kind)
        or pa.types.is_large_binary(kind)
        or pa.types.is_binary_view(kind)
    ):
        raise _bad(
            f"{field.name} is {_type_text(kind)}, not binary", "invalid_shredding"
        )


#: The binary type each string type is read through. Views are what
#: pyarrow's Parquet reader gives with ``binary_type=pa.binary_view()``.
_STRING_AS_BINARY: Final = {
    pa.string(): pa.binary(),
    pa.large_string(): pa.large_binary(),
    pa.string_view(): pa.binary_view(),
}


def _leaves(typed: pa.Array) -> tuple[list[Any], Callable[[Any], object] | None]:
    """A typed leaf's raw values, and how to read one exactly, if not as it
    is. The conversion runs per row, so that a value no Python type holds
    is refused at its row, never as an exception from a whole column."""
    kind = typed.type
    if (
        pa.types.is_boolean(kind)
        or kind in (pa.int8(), pa.int16(), pa.int32(), pa.int64())
        or kind in (pa.float32(), pa.float64())
        or kind in (pa.binary(), pa.large_binary(), pa.binary_view())
    ):
        return typed.to_pylist(), None
    if pa.types.is_decimal(kind):
        # A Variant decimal has 38 digits at most, at a scale of 0 to 38
        # (VE:422-446): a decimal256 of more is no shredded type.
        precision, scale = kind.precision, kind.scale
        if not 0 <= scale <= precision <= 38:
            raise _bad(
                # A decimal type's text is short whatever its parameters.
                f"typed_value of type {kind} is not a Variant decimal",
                "invalid_shredding",
            )
        if pa.types.is_decimal32(kind) or pa.types.is_decimal64(kind):
            # The write form's: its unscaled integers, with no Decimal built
            # and taken apart again.
            width = pa.int32() if pa.types.is_decimal32(kind) else pa.int64()
            return typed.view(width).to_pylist(), lambda v: _scaled(v, precision, scale)
        return typed.to_pylist(), lambda v: _scaled(
            int(v.scaleb(scale, _EXACT)), precision, scale
        )
    if kind in _STRING_AS_BINARY:
        # As bytes: Arrow does not check that a string column is UTF-8.
        return typed.view(_STRING_AS_BINARY[kind]).to_pylist(), _text
    if kind == pa.date32():
        return typed.view(pa.int32()).to_pylist(), _date
    if kind == pa.time64("us"):
        # pyarrow's own conversion wraps a time past midnight round.
        return typed.view(pa.int64()).to_pylist(), _time_of_day
    if pa.types.is_timestamp(kind) and kind.unit in ("us", "ns"):
        raw_values = typed.view(pa.int64()).to_pylist()
        aware = kind.tz is not None
        if kind.unit == "ns":
            nanos = pa.timestamp("ns", tz="UTC" if aware else None)
            return raw_values, lambda v: pa.scalar(v, nanos)
        return raw_values, lambda v: _timestamp(v, aware=aware)
    if isinstance(kind, pa.UuidType):
        return typed.storage.to_pylist(), lambda v: uuid.UUID(bytes=v)
    raise _bad(
        f"typed_value of type {_type_text(kind)} is not a shredded Variant type",
        "invalid_shredding",
    )


def _scaled(unscaled: int, precision: int, scale: int) -> Decimal:
    """The value of a typed decimal leaf of DECIMAL(precision, scale).

    The precision is the most digits the leaf holds (Parquet's DECIMAL),
    as a width's is for a Variant decimal (VE:442-446), and more is
    malformed the same way. Nothing upstream checks it: Arrow builds a
    decimal from buffers or views one over integers, and pyarrow reads and
    writes one, without looking.
    """
    limit = 10**precision
    if not -limit < unscaled < limit:
        raise _bad(
            f"a DECIMAL({precision},{scale}) leaf holds "
            f"{len(str(abs(unscaled)))} digits, beyond its {precision}"
        )
    return Decimal(unscaled).scaleb(-scale, _EXACT)


def _keys_of(value: object, out: set[str]) -> None:
    if isinstance(value, dict):
        out.update(value)
        for item in value.values():
            _keys_of(item, out)
    elif isinstance(value, list):
        for item in value:
            _keys_of(item, out)


def _column_path(path: str | None) -> str:
    """A path from :func:`_difference`, for a message."""
    return _shorten(path.removeprefix(".")) if path else "the top"


def _shorten(text: str) -> str:
    return _cut(text, 64)


def _type_text(kind: pa.DataType) -> str:
    """A storage type in a message: escaped onto one line, since Arrow's
    text of a struct holds its field names, which are data, and cut short."""
    return _shorten(_UNSAFE_IN_PATH.sub(_escape, str(kind)))


def _quoted(name: str) -> str:
    """A key or a column name in a message: quoted, escaped onto one line
    by repr, and cut short, since a name is data."""
    return _shorten(repr(name))


def _cut(text: str, limit: int) -> str:
    # By code points, not the server's UTF-16 units (variant._cap), which
    # can cut a pair in two: a lone surrogate no UTF-8 log can write.
    return text if len(text) <= limit else text[: limit - 3] + "..."


#: What pyarrow's ``binary_type`` and ``list_type`` read options give in
#: place of each plain type: a reader's choice, not the storage's.
_PLAIN: Final = {
    pa.large_binary(): pa.binary(),
    pa.binary_view(): pa.binary(),
    pa.large_string(): pa.string(),
    pa.string_view(): pa.string(),
}

#: Deeper than any declaration's storage nests types: two a level (a group
#: and its typed struct or list), and a declaration nests 16 levels.
_FORM_DEPTH: Final = 64


def _plain(kind: pa.DataType, depth: int = 0) -> pa.DataType:
    """``kind`` with every large and view type spelt plain, so that storage
    pyarrow read with any of its options is a form of its declaration.
    Only the reader's check: an input a writer takes is held to the exact
    form (Plan.form_of)."""
    if depth > _FORM_DEPTH:
        # No declaration's storage nests this deep, so this is no form of
        # one whatever lies below, and the storage's type has no bound.
        return kind
    if pa.types.is_struct(kind):
        return pa.struct(
            [field.with_type(_plain(field.type, depth + 1)) for field in kind]
        )
    if pa.types.is_list(kind) or pa.types.is_large_list(kind):
        field = kind.value_field
        return pa.list_(field.with_type(_plain(field.type, depth + 1)))
    return _PLAIN.get(kind, kind)


def reassemble(
    array: pa.Array | pa.ChunkedArray,
    plan: Plan | None,
    *,
    strict: bool,
    canonical: bool = False,
    column: str | None = None,
    nullable: bool = True,
) -> list[object]:
    """Every row of a storage array as Python (to_python, verify).

    The storage says how to read itself, so no declaration is needed;
    ``plan`` checks that it is a form of that declaration, and
    ``nullable=False`` that no row is SQL NULL, as the encoders refuse it.
    """
    chunks = array.chunks if isinstance(array, pa.ChunkedArray) else [array]
    # A column of no chunks (Table.from_batches([], schema) gives one) is
    # still checked: its type is all the storage there is. pa.nulls, since
    # pa.array cannot build an empty array of a type that holds an
    # extension type, as a uuid leaf's storage does.
    chunks = chunks or [pa.nulls(0, array.type)]
    kind = array.type if plan is None else _plain(array.type)
    if plan is not None and plan.form_of(kind) is None:
        # Named by the first column that differs, not by the whole type,
        # which is as long as the declaration is wide. The two forms differ
        # only at decimal4/decimal8 leaves; where they agree, so does this.
        write = _column_path(_difference(kind, plan.write_type, ""))
        read = _column_path(_difference(kind, plan.read_type, ""))
        where = (
            f"it first differs at {write}"
            if write == read
            else f"it first differs from the write form at {write}, and from "
            f"the read form at {read}"
        )
        # A catalog column's files need not be: a writer that honours no
        # declaration (DuckDB, so hedgerow) writes its own layout.
        hint = (
            "; storage from a writer that honours no declaration, as DuckDB's, "
            "is read without the column"
            if column is not None
            else ""
        )
        raise VariantEncodingError(
            _message(
                column,
                None,
                None,
                "the storage is neither the write nor the read form of the "
                f"declaration ({where}){hint}",
            ),
            column=column,
            reason="invalid_shredding",
        )
    out: list[object] = []
    row = 0
    for chunk in chunks:
        try:
            reader = _Reassembly(chunk, strict=strict, canonical=canonical)
        except _Invalid as exc:
            raise VariantEncodingError(
                _message(column, None, None, exc.detail),
                column=column,
                reason=exc.reason,
            ) from None
        for index in range(len(chunk)):
            if not nullable and not reader.valid[index]:
                # D5: pyarrow would write this row of a REQUIRED group as a
                # present group of empty bytes.
                raise VariantEncodingError(
                    _message(column, row, None, "SQL NULL into a NOT NULL column"),
                    column=column,
                    row=row,
                    reason="sql_null_in_not_null",
                )
            try:
                out.append(reader.row(index))
            except _Invalid as exc:
                path = exc.path()
                raise VariantEncodingError(
                    _message(column, row, path, exc.detail),
                    column=column,
                    row=row,
                    path=path,
                    reason=exc.reason,
                ) from None
            row += 1
    return out
