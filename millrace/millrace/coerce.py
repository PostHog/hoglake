"""Per-column JSON value coercion, shared by the consume path (the
partition-tuple keyer, :mod:`millrace.grouping`) and the flush path's
payload decoder (:class:`millrace.flush.JsonObjectDecoder`).

One implementation on purpose: the consumer computes each record's
partition tuple from the SAME coerced value the flush later builds the
Arrow column from, so a record the consumer keys under a tuple is a
record the flush's fanout puts in that tuple's file (the fanout stays
the correctness backstop either way).

Every coercer is TOTAL over JSON values: it returns the column's Python
value or raises :class:`DecodeError` carrying the drop vocabulary —
never an ``OverflowError`` or a ``UnicodeEncodeError`` that would escape
per-record failure routing. Strict JSON (:func:`load_json_object`)
refuses the NaN/Infinity literals at parse time; magnitude-overflow
literals (``1e400`` parses to ``inf``) are refused at coercion.

Column types (:func:`coercer_for`):

- scalars: one coercer per catalog type (below);
- ``struct``: a JSON object → a dict with one entry per catalog child
  (an absent or null child → None; keys the catalog does not name are
  ignored); anything but an object is a :class:`DecodeError`;
- ``list``: a JSON array → a list of coerced elements;
- ``map``: a JSON object → a list of ``(key, value)`` pairs in the
  object's order; the key child coerces the key STRING (a key child
  that is not a string type first tries the string as-is, then the
  string parsed as a strict JSON number or boolean — ``"42"`` for a
  ``long`` key, ``"true"`` for a ``boolean`` one). The string form
  WINS: for a temporal key type (date, time, the timestamp family) a
  key that is valid ISO-8601 text is read as such — ``"20260101"`` is
  ISO-basic, so a ``date`` key reads it as 2026-01-01, never as epoch
  day 20,260,101 — and the numeric fallback applies only to a string
  that is not valid ISO text. Two keys that coerce to one value are
  refused, compared as the column stores them (a ``float`` key narrowed
  to float32 first). A key child that is a container or ``json`` is a
  startup refusal: a JSON object key is a string, which no such key
  coerces;
- ``json``: any JSON value → its canonical text (:func:`canonical_json`).

Inside a container a null element, child or value is SQL NULL, refused
(naming the dotted path) where the catalog says the child is not
nullable. Nesting is bounded by pyhoglake's
``MAX_COLUMN_NESTING_DEPTH``. A ``variant`` column, at any depth, is
refused (:class:`UnsupportedColumnError`): pyarrow cannot write the
native Parquet VARIANT annotation hoglake requires.
"""

from __future__ import annotations

import base64
import binascii
import json
import math
import re
import struct
import uuid as _uuid
from collections.abc import Callable
from datetime import date, datetime, timedelta
from datetime import time as dtime
from decimal import Decimal, localcontext
from typing import Any, Final

import pyarrow as pa
from pyhoglake import Column
from pyhoglake.types import MAX_COLUMN_NESTING_DEPTH

_MICROS_PER_SECOND: Final = 1_000_000
MICROS_PER_DAY: Final = 86_400_000_000
EPOCH_DATE: Final = date(1970, 1, 1)
# Naive-by-construction UTC wall time (ruff's DTZ001 refuses the bare
# constructor; the function that subtracts this takes naive-UTC inputs).
_EPOCH_DATETIME: Final = datetime.fromisoformat("1970-01-01T00:00:00")

# The decode half of the bounded drop vocabulary (the
# ``millrace_records_dropped_total`` reason label).
REASON_UNDECODABLE_PAYLOAD: Final = "undecodable_payload"
REASON_UNCASTABLE_COLUMN: Final = "uncastable_column"


class DecodeError(ValueError):
    """One staged payload cannot become a row: not a JSON document/object,
    or a value that cannot be coerced to its catalog column. Per-record —
    the flush drops the record and moves on. ``reason`` is the
    drop vocabulary (the metric's reason label). ``path`` is the dotted
    catalog path of the column or child that failed (``properties.ts``,
    ``tags.element``, ``attrs.key``) when one is known — catalog names
    only, never a payload value, so it is safe to log where the message
    (which may quote the value) is not."""

    def __init__(self, message: str, *, reason: str, path: str | None = None) -> None:
        super().__init__(message)
        self.reason = reason
        self.path = path


class UnsupportedColumnError(ValueError):
    """The destination table carries a column the JSON decoder cannot
    fill: an unknown type, a decimal whose type_params do not build, a
    container whose children do not match its type, a column nested past
    ``MAX_COLUMN_NESTING_DEPTH``, or a ``variant`` (pyarrow cannot write
    the native Parquet VARIANT annotation hoglake requires).
    Named with the column; the flusher's startup resolution re-raises it
    as a startup refusal."""


_INT_BOUNDS: Final = {
    "int8": (-(1 << 7), (1 << 7) - 1),
    "int16": (-(1 << 15), (1 << 15) - 1),
    "int": (-(1 << 31), (1 << 31) - 1),
    "long": (-(1 << 63), (1 << 63) - 1),
    "uint8": (0, (1 << 8) - 1),
    "uint16": (0, (1 << 16) - 1),
    "uint32": (0, (1 << 32) - 1),
    "uint64": (0, (1 << 64) - 1),
}

# Micros per one unit of the column type (and nanos-per-micro for ns).
TIMESTAMP_UNITS: Final = {
    "timestamp_s": 1_000_000,
    "timestamp_ms": 1_000,
    "timestamp": 1,
    "timestamptz": 1,
}

# int64 bounds for a timestamp column's value IN ITS OWN UNIT. Anything
# wider dies later and worse: ``pa.array`` raises ``OverflowError`` on an
# out-of-int64 value at BATCH build time (one bad record killing the
# flush), and the s→ms storage cast (``_encode_schema``) raises
# ``ArrowInvalid`` on seconds that overflow milliseconds — so
# ``timestamp_s`` is bounded by the millisecond-representable range, the
# others by int64. The junk ``event_time`` window only covers the live
# spec's temporal partition sources; every OTHER temporal column is
# covered by exactly this check.
_INT64_BOUNDS: Final = (-(1 << 63), (1 << 63) - 1)
_TIMESTAMP_VALUE_BOUNDS: Final = {
    "timestamp_s": (-((1 << 63) // 1000), ((1 << 63) - 1) // 1000),
    "timestamp_ms": _INT64_BOUNDS,
    "timestamp": _INT64_BOUNDS,
    "timestamp_ns": _INT64_BOUNDS,
    "timestamptz": _INT64_BOUNDS,
}

_ISO_FRACTION: Final = re.compile(r"\.(\d+)")


def reject_constant(raw: str) -> Any:
    """json.loads parse_constant hook: NaN/Infinity are not JSON."""
    raise DecodeError(
        f"{raw} is not valid JSON (strict mode: no NaN/Infinity)",
        reason=REASON_UNDECODABLE_PAYLOAD,
    )


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    raise DecodeError(
        f"expected a JSON boolean, got {type(value).__name__}",
        reason=REASON_UNCASTABLE_COLUMN,
    )


def _coerce_int(value: Any, type_name: str, lo: int, hi: int) -> int:
    # isinstance(bool) is isinstance(int) — exclude it explicitly.
    if isinstance(value, bool) or not isinstance(value, int):
        raise DecodeError(
            f"expected a JSON integer for {type_name}, got {type(value).__name__}",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    if not lo <= value <= hi:
        raise DecodeError(
            f"value {value} does not fit in {type_name}",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    return value


def _coerce_float(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DecodeError(
            f"expected a JSON number, got {type(value).__name__}",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    try:
        result = float(value)
    except OverflowError as e:
        # float(10**400): a JSON integer too large for a double.
        raise DecodeError(
            f"integer value does not fit in a double: {e}",
            reason=REASON_UNCASTABLE_COLUMN,
        ) from e
    if not math.isfinite(result):
        # json.loads("1e400") parses to inf WITHOUT tripping
        # parse_constant (that hook only sees the NaN/Infinity
        # literals): a magnitude-overflow literal is valid JSON syntax
        # carrying a value no double holds. Refuse it here, or inf
        # would land in the column and its stats.
        raise DecodeError(
            f"non-finite double ({result!r}): the JSON literal overflows "
            "a double (e.g. 1e400)",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    return result


#: The largest finite float32, exactly representable as a double
#: (``struct.unpack(">f", bytes.fromhex("7f7fffff"))[0]``). A catalog
#: ``float`` column narrows coerced doubles through Arrow's float32
#: conversion, which produces INF WITHOUT ERROR past this magnitude —
#: so the double-range check alone would publish infinity under the
#: non-finite guard's nose.
_FLOAT32_MAX: Final = 3.4028234663852886e38


def _coerce_float32(value: Any) -> float:
    """A catalog ``float`` (float32) column's coercion: the double
    checks PLUS float32 range. Arrow narrows the Python double with a
    native float cast that yields +/-inf for out-of-range magnitudes
    WITHOUT raising (probe-pinned on pyarrow 21–25), so a finite JSON
    double like ``1e100`` would publish infinity — defeating the
    non-finite guard — unless the magnitude is refused here. The check
    is deliberately the simple honest one: |v| > float32 max ⇒ refuse
    (the half-ulp band just above max that would round DOWN to a finite
    float32 is refused too — conservatism there is a rounding the
    producer cannot have meant); non-finite stays refused via
    :func:`_coerce_float`."""
    result = _coerce_float(value)
    if abs(result) > _FLOAT32_MAX:
        raise DecodeError(
            f"value {result!r} overflows a float32 (|v| > {_FLOAT32_MAX}); "
            "Arrow's narrowing cast would store infinity",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    return result


def _coerce_str(value: Any) -> str:
    if not isinstance(value, str):
        raise DecodeError(
            f"expected a JSON string, got {type(value).__name__}",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as e:
        # json.loads('{"event":"\\ud800"}') SUCCEEDS, producing a lone
        # surrogate Python's str happily holds — but Arrow's UTF-8
        # string conversion raises UnicodeEncodeError (neither
        # pa.ArrowException nor OverflowError — outside the batch
        # build's per-record isolation taxonomy), so one such record
        # would fail its whole flush window identically forever.
        # Refuse here instead: per-record drop, the batch
        # survives.
        raise DecodeError(
            f"string is not valid UTF-8 ({e}); refusing a surrogate-bearing "
            "value before the Arrow build",
            reason=REASON_UNCASTABLE_COLUMN,
        ) from e
    return value


def _coerce_b64(value: Any) -> bytes:
    if not isinstance(value, str):
        raise DecodeError(
            f"expected a base64 string for a binary column, got {type(value).__name__}",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    try:
        return base64.b64decode(value.encode("ascii"), validate=True)
    except (binascii.Error, UnicodeEncodeError) as e:
        raise DecodeError(
            f"invalid base64 for a binary column: {e}",
            reason=REASON_UNCASTABLE_COLUMN,
        ) from e


def _coerce_uuid_bytes(value: Any) -> bytes:
    if not isinstance(value, str):
        raise DecodeError(
            f"expected a UUID string, got {type(value).__name__}",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    try:
        return _uuid.UUID(value).bytes
    except ValueError as e:
        raise DecodeError(
            f"invalid UUID string {value!r}",
            reason=REASON_UNCASTABLE_COLUMN,
        ) from e


def _coerce_decimal(value: Any, type_params: dict[str, Any] | None) -> Decimal:
    """int or decimal string → Decimal, precision/scale checked (never
    rounded: a value that does not fit exactly is a decode failure, not a
    silent mutation). JSON floats are refused — a binary float is not the
    decimal the producer wrote."""
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise DecodeError(
            f"expected a JSON integer or decimal string, got {type(value).__name__}",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    params = type_params or {}
    precision = int(params["precision"])
    scale = int(params["scale"])
    try:
        d = Decimal(str(value))
    except ArithmeticError as e:
        raise DecodeError(
            f"invalid decimal {value!r}", reason=REASON_UNCASTABLE_COLUMN
        ) from e
    if not d.is_finite():
        raise DecodeError(
            f"non-finite decimal {value!r}", reason=REASON_UNCASTABLE_COLUMN
        )
    with localcontext() as ctx:
        ctx.prec = 60
        # value * 10^scale must be an integer, or accepting the value
        # would round it.
        scaled = d.scaleb(scale)
        if scaled != scaled.to_integral_value():
            raise DecodeError(
                f"decimal {value!r} has more than {scale} fractional digits; "
                "refusing to round",
                reason=REASON_UNCASTABLE_COLUMN,
            )
        digits = len(scaled.to_integral_value().as_tuple().digits)
    if digits > precision:
        raise DecodeError(
            f"decimal {value!r} exceeds precision {precision}",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    return d


def _coerce_date(value: Any) -> date:
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as e:
            raise DecodeError(
                f"invalid ISO-8601 date {value!r}",
                reason=REASON_UNCASTABLE_COLUMN,
            ) from e
    if isinstance(value, bool) or not isinstance(value, int):
        raise DecodeError(
            f"expected an ISO-8601 date string or epoch-days integer, got "
            f"{type(value).__name__}",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    try:
        return EPOCH_DATE + timedelta(days=value)
    except OverflowError as e:
        raise DecodeError(
            f"epoch-days value {value} is out of range",
            reason=REASON_UNCASTABLE_COLUMN,
        ) from e


def _time_to_micros(t: dtime) -> int:
    return (
        t.hour * 3600 + t.minute * 60 + t.second
    ) * _MICROS_PER_SECOND + t.microsecond


def _coerce_time_micros(value: Any) -> int:
    if isinstance(value, str):
        try:
            t = dtime.fromisoformat(value)
        except ValueError as e:
            raise DecodeError(
                f"invalid ISO-8601 time {value!r}",
                reason=REASON_UNCASTABLE_COLUMN,
            ) from e
        if t.tzinfo is not None:
            raise DecodeError(
                f"time column does not take a timezone: {value!r}",
                reason=REASON_UNCASTABLE_COLUMN,
            )
        return _time_to_micros(t)
    if isinstance(value, bool) or not isinstance(value, int):
        raise DecodeError(
            f"expected an ISO-8601 time string or micros-since-midnight "
            f"integer, got {type(value).__name__}",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    if not 0 <= value < MICROS_PER_DAY:
        raise DecodeError(
            f"micros-since-midnight {value} is out of range",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    return value


def _datetime_to_micros(dt: datetime) -> int:
    """Exact integer micros since the epoch (a naive datetime is read as
    UTC wall time — the tz-aware caller subtracts its offset first)."""
    delta = dt - _EPOCH_DATETIME
    return (
        delta.days * MICROS_PER_DAY
        + delta.seconds * _MICROS_PER_SECOND
        + delta.microseconds
    )


def _coerce_timestamp(value: Any, type_name: str, *, tz: bool) -> int:
    """An instant as an integer in the column's own unit (epoch seconds
    for ``timestamp_s``, millis, micros — ``timestamptz`` is micros — and
    nanos for ``timestamp_ns``), or a strict ISO-8601 string.

    Strings never silently lose precision: a sub-unit fraction is refused
    outright (a timestamp_s column takes only whole seconds from text),
    and sub-microsecond digits are refused even for timestamp_ns (the
    integer form is the exact one). Naive strings feed naive columns and
    offset-carrying strings feed ``timestamptz``; the mismatches are
    refused rather than reinterpreted. The result — from either form —
    must fit the column's unit in int64 (``timestamp_s`` in stored
    milliseconds): a magnitude outside it is a coercion failure, never
    a batch-killing ``OverflowError`` at Arrow build time."""
    if isinstance(value, bool):
        raise DecodeError(
            "expected a timestamp, got a JSON boolean",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    if isinstance(value, int):
        result = value
    elif not isinstance(value, str):
        raise DecodeError(
            f"expected an epoch integer or ISO-8601 string for {type_name}, "
            f"got {type(value).__name__}",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    else:
        fraction = _ISO_FRACTION.search(value)
        if (
            fraction is not None
            and len(fraction.group(1)) > 6
            and any(d != "0" for d in fraction.group(1)[6:])
        ):
            raise DecodeError(
                f"ISO-8601 timestamp {value!r} has sub-microsecond digits; "
                "use the integer form for nanosecond precision",
                reason=REASON_UNCASTABLE_COLUMN,
            )
        try:
            dt = datetime.fromisoformat(value)
        except ValueError as e:
            raise DecodeError(
                f"invalid ISO-8601 timestamp {value!r}",
                reason=REASON_UNCASTABLE_COLUMN,
            ) from e
        if tz:
            if dt.tzinfo is None:
                raise DecodeError(
                    f"timestamptz requires an offset ('Z' or '+hh:mm'): {value!r}",
                    reason=REASON_UNCASTABLE_COLUMN,
                )
            offset = dt.utcoffset()
            assert offset is not None  # tzinfo present ⇒ offset computable
            micros = _datetime_to_micros(dt.replace(tzinfo=None) - offset)
        else:
            if dt.tzinfo is not None:
                raise DecodeError(
                    f"naive {type_name} does not take an offset: {value!r}",
                    reason=REASON_UNCASTABLE_COLUMN,
                )
            micros = _datetime_to_micros(dt)
        if type_name == "timestamp_ns":
            result = micros * 1000
        else:
            unit_us = TIMESTAMP_UNITS[type_name]
            if micros % unit_us:
                raise DecodeError(
                    f"timestamp {value!r} is finer than {type_name}'s unit; "
                    "refusing to truncate",
                    reason=REASON_UNCASTABLE_COLUMN,
                )
            result = micros // unit_us
    lo, hi = _TIMESTAMP_VALUE_BOUNDS[type_name]
    if not lo <= result <= hi:
        raise DecodeError(
            f"{type_name} value {result} is outside the int64-representable "
            f"range for its unit [{lo}, {hi}]",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    return result


def canonical_json(value: Any) -> str:
    """The text a ``json`` value is stored as: compact separators,
    non-ASCII kept as UTF-8, object keys in PAYLOAD order (payload
    fidelity — so one logical object can have several texts:
    ``{"a":1,"b":2}`` and ``{"b":2,"a":1}`` are stored as written, not
    normalised), numbers as Python's ``json`` spells them (integers
    exactly, floats shortest round-trip). Raises ``ValueError`` for a non-finite float
    (``1e400`` parses to ``inf``) and ``RecursionError`` past the
    interpreter's depth."""
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _coerce_json_text(value: Any, *, reason: str) -> str:
    try:
        text = canonical_json(value)
    except (ValueError, RecursionError) as e:
        raise DecodeError(f"value has no JSON text: {e}", reason=reason) from e
    try:
        text.encode("utf-8")
    except UnicodeEncodeError as e:
        # A lone surrogate (legal JSON: "\ud800") — Arrow's UTF-8
        # conversion would raise outside the build isolation's taxonomy.
        raise DecodeError(f"JSON text is not valid UTF-8 ({e})", reason=reason) from e
    return text


def _scalar_coercer(column: Column) -> Callable[[Any], Any]:
    t = column.type
    if t == "boolean":
        return _coerce_bool
    if t in _INT_BOUNDS:
        lo, hi = _INT_BOUNDS[t]
        return lambda v: _coerce_int(v, t, lo, hi)
    if t == "float":
        # float32: Arrow's narrowing cast produces inf WITHOUT error on
        # out-of-range doubles — range-checked per record (the coercion
        # must stay total or one value wedges the batch build).
        return _coerce_float32
    if t == "double":
        return _coerce_float
    if t == "string":
        return _coerce_str
    if t == "binary":
        return _coerce_b64
    if t == "uuid":
        return _coerce_uuid_bytes
    if t == "decimal":
        # Eager shape validation: a decimal column whose type_params are
        # missing or unbuildable (precision outside decimal128's 1..38) is
        # a STARTUP refusal, not a per-record failure — otherwise the
        # per-record coercion KeyErrors (or the Arrow schema build raises)
        # at flush time, wedging the key on a destination defect.
        params = column.type_params or {}
        try:
            pa.decimal128(int(params["precision"]), int(params["scale"]))
        except (KeyError, TypeError, ValueError) as e:
            raise UnsupportedColumnError(
                f"column {column.name!r} is decimal but its type_params "
                f"({column.type_params!r}) do not build a decimal128: {e}"
            ) from e
        return lambda v: _coerce_decimal(v, column.type_params)
    if t == "date":
        return _coerce_date
    if t == "time":
        return _coerce_time_micros
    if t in ("timestamp_s", "timestamp_ms", "timestamp", "timestamp_ns"):
        return lambda v: _coerce_timestamp(v, t, tz=False)
    if t == "timestamptz":
        return lambda v: _coerce_timestamp(v, t, tz=True)
    if t == "json":
        return lambda v: _coerce_json_text(v, reason=REASON_UNCASTABLE_COLUMN)
    raise UnsupportedColumnError(
        f"column {column.name!r} has type {t!r}, which the JSON decoder does "
        "not know (supported: boolean, the int widths, float/double, string, "
        "binary, uuid, decimal, date, time, the timestamp family, json, "
        "struct, list and map)"
    )


_CONTAINER_TYPES: Final = frozenset({"struct", "list", "map"})
#: What a map key STRING may parse to (strict JSON) when the key child's
#: coercer refuses the string as-is: the number and boolean key types.
_KEY_RETRY: Final = (int, float, bool)


def _null_child(path: str) -> DecodeError:
    return DecodeError(
        f"{path} is not nullable and the payload has no usable value",
        reason=REASON_UNCASTABLE_COLUMN,
        path=path,
    )


def _float32(value: float) -> float:
    """The float32 a catalog ``float`` value is stored as."""
    return float(struct.unpack(">f", struct.pack(">f", value))[0])


def _children(column: Column, path: str) -> tuple[Column, ...]:
    """A container's children in ordinal order, shape-checked against
    its type (Iceberg's synthetic names for list/map)."""
    children = tuple(sorted(column.children or (), key=lambda c: c.ordinal))
    want = {"list": ("element",), "map": ("key", "value")}.get(column.type)
    if want is not None and tuple(c.name for c in children) != want:
        raise UnsupportedColumnError(
            f"column {path!r} is a {column.type} whose children are "
            f"{[c.name for c in children]}, not {list(want)}"
        )
    if not children:
        raise UnsupportedColumnError(f"column {path!r} is a struct with no children")
    return children


def _child_coercer(child: Column, path: str, depth: int) -> Callable[[Any], Any]:
    """A container child's coercer: null → None (refused when the child
    is not nullable), anything else coerced, a scalar failure re-raised
    naming the child's dotted path (container failures name their own)."""
    inner = _coercer(child, path, depth)
    nested = child.type in _CONTAINER_TYPES
    nullable = child.nullable

    def coerce(value: Any) -> Any:
        if value is None:
            if not nullable:
                raise _null_child(path)
            return None
        if nested:
            return inner(value)
        try:
            return inner(value)
        except DecodeError as e:
            raise DecodeError(f"{path}: {e}", reason=e.reason, path=path) from e

    return coerce


def _parse_key(text: str) -> Any:
    try:
        return json.loads(text, parse_constant=reject_constant)
    except (ValueError, RecursionError):
        return None


def _coercer(column: Column, path: str, depth: int) -> Callable[[Any], Any]:
    if depth > MAX_COLUMN_NESTING_DEPTH:
        raise UnsupportedColumnError(
            f"column {path!r} nests deeper than the maximum "
            f"{MAX_COLUMN_NESTING_DEPTH} (a top-level column is depth 1)"
        )
    t = column.type
    if t == "variant":
        raise UnsupportedColumnError(
            f"column {path!r} is a variant: hoglake takes a variant only as a "
            "native Parquet VARIANT group, and pyarrow cannot write that "
            "annotation (pyhoglake's Arrow writer refuses it for the same reason)"
        )
    if t == "struct":
        fields = tuple(
            (c.name, _child_coercer(c, f"{path}.{c.name}", depth + 1))
            for c in _children(column, path)
        )

        def coerce_struct(value: Any) -> dict[str, Any]:
            if not isinstance(value, dict):
                raise DecodeError(
                    f"{path}: expected a JSON object for a struct, got "
                    f"{type(value).__name__}",
                    reason=REASON_UNCASTABLE_COLUMN,
                    path=path,
                )
            return {name: coerce(value.get(name)) for name, coerce in fields}

        return coerce_struct
    if t == "list":
        (element,) = _children(column, path)
        coerce_element = _child_coercer(element, f"{path}.element", depth + 1)

        def coerce_list(value: Any) -> list[Any]:
            if not isinstance(value, list):
                raise DecodeError(
                    f"{path}: expected a JSON array for a list, got "
                    f"{type(value).__name__}",
                    reason=REASON_UNCASTABLE_COLUMN,
                    path=path,
                )
            return [coerce_element(v) for v in value]

        return coerce_list
    if t == "map":
        key, item = _children(column, path)
        key_path = f"{path}.key"
        if key.type in _CONTAINER_TYPES or key.type == "json":
            raise UnsupportedColumnError(
                f"column {key_path!r} is a {key.type!r} map key: a JSON object "
                "key is a string, which no such key coerces"
            )
        coerce_key = _coercer(key, key_path, depth + 1)
        narrow = _float32 if key.type == "float" else None
        coerce_item = _child_coercer(item, f"{path}.value", depth + 1)
        key_is_string = key.type == "string"

        def key_of(text: str) -> Any:
            try:
                return coerce_key(text)
            except DecodeError as first:
                parsed = None if key_is_string else _parse_key(text)
                if not isinstance(parsed, _KEY_RETRY):
                    raise DecodeError(
                        f"{key_path}: {first}", reason=first.reason, path=key_path
                    ) from first
            try:
                return coerce_key(parsed)
            except DecodeError as e:
                raise DecodeError(
                    f"{key_path}: {e}", reason=e.reason, path=key_path
                ) from e

        def coerce_map(value: Any) -> list[tuple[Any, Any]]:
            if not isinstance(value, dict):
                raise DecodeError(
                    f"{path}: expected a JSON object for a map, got "
                    f"{type(value).__name__}",
                    reason=REASON_UNCASTABLE_COLUMN,
                    path=path,
                )
            pairs = [(key_of(k), coerce_item(v)) for k, v in value.items()]
            # Compared as stored: a float32 key narrows in Arrow, so two
            # doubles that differ only below float32 precision collide.
            stored = {narrow(k) if narrow else k for k, _ in pairs}
            if len(stored) != len(pairs):
                raise DecodeError(
                    f"{key_path}: two keys of the object coerce to one map key",
                    reason=REASON_UNCASTABLE_COLUMN,
                    path=key_path,
                )
            return pairs

        return coerce_map
    return _scalar_coercer(column)


def coercer_for(column: Column) -> Callable[[Any], Any]:
    """The coercer of one catalog column (the module docstring's rules),
    applied to a NON-null JSON value — the caller decides what a null
    or absent top-level value means. Raises
    :class:`UnsupportedColumnError` for a column it cannot fill."""
    return _coercer(column, column.name, 1)


def load_json_object(payload: bytes) -> dict[str, Any]:
    """Parse one staged payload as a strict JSON OBJECT (no NaN/Infinity
    literals). Raises :class:`DecodeError` (``undecodable_payload``) for
    anything else — per record, never the caller's death."""
    try:
        doc = json.loads(payload, parse_constant=reject_constant)
    except DecodeError:
        raise
    except (ValueError, RecursionError) as e:
        # ValueError: not JSON (and the int-digit limit, and invalid
        # UTF-8 — both ValueError subclasses). RecursionError: a payload
        # nested past the interpreter limit — the C scanner reports it
        # that way, and it is just as per-record as a syntax error.
        raise DecodeError(
            f"payload is not a JSON document: {e}",
            reason=REASON_UNDECODABLE_PAYLOAD,
        ) from e
    if not isinstance(doc, dict):
        raise DecodeError(
            f"payload is a JSON {type(doc).__name__}, not an object",
            reason=REASON_UNDECODABLE_PAYLOAD,
        )
    return doc
