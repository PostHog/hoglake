"""pyarrow <-> hoglake column-type mapping.

Supported mappings (both directions):

    pa.bool_()               <-> boolean
    pa.int8()                <-> int8
    pa.int16()               <-> int16
    pa.int32()               <-> int
    pa.int64()               <-> long
    pa.uint8()               <-> uint8
    pa.uint16()              <-> uint16
    pa.uint32()               -> uint32 -> pa.int64()   (see below)
    pa.uint64()              <-> uint64
    pa.float32()             <-> float
    pa.float64()             <-> double
    pa.string()/large_string <-> string
    pa.json_()               <-> json   (pyarrow >= 19; else pa.string())
    pa.binary()/large_binary <-> binary
    pa.date32()              <-> date
    pa.time64("us")          <-> time
    pa.timestamp("s")        <-> timestamp_s
    pa.timestamp("ms")       <-> timestamp_ms
    pa.timestamp("us")       <-> timestamp
    pa.timestamp("ns")       <-> timestamp_ns
    pa.timestamp("us", tz)   <-> timestamptz
    pa.decimal128(p, s)      <-> decimal  (type_params: {"precision": p, "scale": s})
    pa.binary(16) (fixed)    <-> uuid

Note on uuid: hoglake's ``uuid`` column type maps from Arrow
``fixed_size_binary(16)`` (``pa.binary(16)``) holding the UUID's 16
big-endian bytes (``uuid.UUID(...).bytes``). pyarrow's canonical uuid
extension type (``pa.uuid()``, pyarrow >= 18) is accepted on input and
treated identically.

Note on uint32 — the one deliberately asymmetric mapping. ``uint32``
maps to Iceberg ``long``, but pyarrow writes ``pa.uint32()`` as parquet
INT32 + ``Int(32, isSigned=false)``, and an Iceberg reader takes that
annotation's physical INT32 as a SIGNED int32: every value above 2^31
would come back negative through the facade. So hoglake's WRITER
contract for uint32 is parquet INT64, i.e. ``coltype_to_arrow("uint32")``
returns ``pa.int64()``. The read path still accepts both physical forms
— only the writer is pinned — and a ``pa.uint32()`` array casts to
int64 losslessly, so this costs the caller nothing but one hop through
"long" if they round-trip the type name.

Note on timestamp_s: parquet has no seconds unit, so pyarrow writes a
``pa.timestamp("s")`` column as INT64 + ``Timestamp(MILLIS)`` and reads
it back as ``timestamp[ms]``. The catalog type stays ``timestamp_s`` —
declared precision is metadata — but do not expect the file's physical
annotation to say seconds.

Note on json: ``pa.string()``/``pa.large_string()`` always map to
``string``, NEVER to ``json``. Arrow's plain string carries no JSON
validity claim, so inferring json from it would attach a claim the data
never made; a json column must be declared explicitly in DDL (or carried
in ``pa.json_()``, which does make the claim). In the other direction
``coltype_to_arrow("json")`` prefers ``pa.json_()`` because that is what
makes pyarrow stamp the parquet JSON logical annotation; on pyarrow < 19
it falls back to ``pa.string()``, which loses that annotation but not
one byte of the document.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa

from .errors import UnsupportedTypeError
from .models import Column

PARQUET_FIELD_ID_KEY = b"PARQUET:field_id"

_SUPPORTED = (
    "bool, int8, int16, int32, int64, uint8, uint16, uint32, uint64, "
    "float32, float64, string, large_string, json, binary, large_binary, "
    "fixed_size_binary(16) [uuid], date32, time64(us), "
    "timestamp(s|ms|us|ns), timestamp(us, tz), decimal128"
)


def _is_uuid_extension(t: pa.DataType) -> bool:
    try:
        return t.equals(pa.uuid())  # pyarrow >= 18
    except AttributeError:  # pragma: no cover - old pyarrow
        return False


def _is_json_extension(t: pa.DataType) -> bool:
    try:
        return t.equals(pa.json_())  # pyarrow >= 19
    except AttributeError:  # pragma: no cover - old pyarrow
        return False


def arrow_type_to_coltype(t: pa.DataType) -> tuple[str, dict[str, Any] | None]:
    """Map an Arrow type to (hoglake column type, type_params)."""
    if pa.types.is_boolean(t):
        return "boolean", None
    if pa.types.is_int8(t):
        return "int8", None
    if pa.types.is_int16(t):
        return "int16", None
    if pa.types.is_int32(t):
        return "int", None
    if pa.types.is_int64(t):
        return "long", None
    if pa.types.is_uint8(t):
        return "uint8", None
    if pa.types.is_uint16(t):
        return "uint16", None
    if pa.types.is_uint32(t):
        return "uint32", None
    if pa.types.is_uint64(t):
        return "uint64", None
    if pa.types.is_float32(t):
        return "float", None
    if pa.types.is_float64(t):
        return "double", None
    # Extension checks come before the plain-string check they shadow:
    # json is an extension over utf8, and only the extension makes the
    # JSON validity claim. A bare string stays "string" (module docstring).
    if _is_json_extension(t):
        return "json", None
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "string", None
    if _is_uuid_extension(t):
        return "uuid", None
    if pa.types.is_fixed_size_binary(t):
        if t.byte_width == 16:
            return "uuid", None
        raise UnsupportedTypeError(
            f"unsupported Arrow type {t!r}: only fixed_size_binary(16) (uuid) "
            f"is supported; supported types: {_SUPPORTED}"
        )
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return "binary", None
    if pa.types.is_date32(t):
        return "date", None
    if pa.types.is_time64(t):
        if t.unit == "us":
            return "time", None
        raise UnsupportedTypeError(
            f"unsupported Arrow type {t!r}: time must be time64('us'); "
            f"supported types: {_SUPPORTED}"
        )
    if pa.types.is_timestamp(t):
        if t.tz:
            # timestamptz is micros-only and stays that way: hoglake has
            # no timestamptz_s/_ms/_ns to carry another unit, so mapping
            # one here would silently reinterpret the values as micros.
            if t.unit != "us":
                raise UnsupportedTypeError(
                    f"unsupported Arrow type {t!r}: a tz-aware timestamp must "
                    f"have microsecond unit (timestamptz is micros-only; the "
                    f"other units exist only tz-naive, as timestamp_s/_ms/_ns); "
                    f"supported types: {_SUPPORTED}"
                )
            return "timestamptz", None
        by_unit = {
            "s": "timestamp_s",
            "ms": "timestamp_ms",
            "us": "timestamp",
            "ns": "timestamp_ns",
        }
        if t.unit not in by_unit:  # pragma: no cover - arrow has no other unit
            raise UnsupportedTypeError(
                f"unsupported Arrow type {t!r}: unknown timestamp unit; "
                f"supported types: {_SUPPORTED}"
            )
        return by_unit[t.unit], None
    if pa.types.is_decimal128(t):
        return "decimal", {"precision": t.precision, "scale": t.scale}
    raise UnsupportedTypeError(
        f"unsupported Arrow type {t!r}; supported types: {_SUPPORTED}"
    )


def coltype_to_arrow(
    type_: str, type_params: dict[str, Any] | None = None
) -> pa.DataType:
    """Map a hoglake column type back to an Arrow type."""
    if type_ == "boolean":
        return pa.bool_()
    if type_ == "int8":
        return pa.int8()
    if type_ == "int16":
        return pa.int16()
    if type_ == "int":
        return pa.int32()
    if type_ == "long":
        return pa.int64()
    if type_ == "uint8":
        return pa.uint8()
    if type_ == "uint16":
        return pa.uint16()
    if type_ == "uint32":
        # NOT pa.uint32(): pyarrow writes that as parquet INT32 +
        # Int(32, unsigned), which an Iceberg reader takes as a SIGNED
        # int32, so everything above 2^31 reads back negative through
        # the facade. uint32 maps to Iceberg long, so the writer
        # contract is parquet INT64. A pa.uint32() array casts to int64
        # losslessly, so this costs the caller nothing.
        return pa.int64()
    if type_ == "uint64":
        return pa.uint64()
    if type_ == "float":
        return pa.float32()
    if type_ == "double":
        return pa.float64()
    if type_ == "string":
        return pa.string()
    if type_ == "json":
        # pa.json_() is what makes pyarrow stamp the parquet JSON logical
        # annotation. The pyarrow < 19 fallback loses that annotation,
        # not the bytes.
        try:
            return pa.json_()
        except AttributeError:  # pragma: no cover - old pyarrow
            return pa.string()
    if type_ == "binary":
        return pa.binary()
    if type_ == "date":
        return pa.date32()
    if type_ == "time":
        return pa.time64("us")
    if type_ == "timestamp_s":
        return pa.timestamp("s")
    if type_ == "timestamp_ms":
        return pa.timestamp("ms")
    if type_ == "timestamp":
        return pa.timestamp("us")
    if type_ == "timestamp_ns":
        return pa.timestamp("ns")
    if type_ == "timestamptz":
        return pa.timestamp("us", tz="UTC")
    if type_ == "uuid":
        return pa.binary(16)
    if type_ == "decimal":
        params = type_params or {}
        try:
            return pa.decimal128(int(params["precision"]), int(params["scale"]))
        except KeyError as e:
            raise UnsupportedTypeError(
                f"decimal column missing type_params key {e}"
            ) from None
    raise UnsupportedTypeError(f"unknown hoglake column type {type_!r}")


def schema_to_column_defs(schema: pa.Schema) -> list[dict[str, Any]]:
    """Convert a pyarrow schema into CreateTableRequest column defs."""
    out: list[dict[str, Any]] = []
    for f in schema:
        type_, params = arrow_type_to_coltype(f.type)
        col: dict[str, Any] = {"name": f.name, "type": type_, "nullable": f.nullable}
        if params:
            col["type_params"] = params
        out.append(col)
    return out


def columns_to_arrow_schema(columns: list[Column] | tuple[Column, ...]) -> pa.Schema:
    """Build the target Arrow schema for a table's columns, with parquet
    field ids embedded as ``PARQUET:field_id`` field metadata (pyarrow
    writes these into the parquet SchemaElement field_id slots)."""
    fields = []
    for c in sorted(columns, key=lambda c: c.ordinal):
        fields.append(
            pa.field(
                c.name,
                coltype_to_arrow(c.type, c.type_params),
                nullable=c.nullable,
                metadata={PARQUET_FIELD_ID_KEY: str(c.field_id).encode("ascii")},
            )
        )
    return pa.schema(fields)
