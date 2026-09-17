"""Prepared-file validation, including native Parquet VARIANT annotations.

Arrow 25 exposes VARIANT as its storage struct and drops its logical annotation.
Read only the schema list from the compact-Thrift footer to distinguish native
VARIANT from an ordinary struct. No payload pages are loaded or rewritten.
"""

from __future__ import annotations

import struct
from typing import Any

import pyarrow as pa
from thrift.protocol.TCompactProtocol import TCompactProtocol
from thrift.Thrift import TType
from thrift.transport.TTransport import TMemoryBuffer

from .errors import ValidationError
from .models import Column
from .types import PARQUET_FIELD_ID_KEY, coltype_to_arrow


def _struct(protocol: Any, depth: int = 0) -> dict[int, Any]:
    if depth > 8:
        raise ValueError("Parquet logical annotation nesting is too deep")
    result = {}
    protocol.readStructBegin()
    while True:
        _, kind, field = protocol.readFieldBegin()
        if kind == TType.STOP:
            break
        if kind == TType.I32:
            result[field] = protocol.readI32()
        elif kind == TType.BYTE:
            result[field] = protocol.readByte()
        elif kind == TType.STRING:
            result[field] = protocol.readString()
        elif kind == TType.STRUCT:
            result[field] = _struct(protocol, depth + 1)
        else:
            protocol.skip(kind)
        protocol.readFieldEnd()
    protocol.readStructEnd()
    return result


def _schema_elements(path: str) -> list[dict[int, Any]]:
    with open(path, "rb") as source:
        source.seek(0, 2)
        size = source.tell()
        source.seek(-8, 2)
        trailer = source.read(8)
        length = struct.unpack("<I", trailer[:4])[0]
        if trailer[4:] != b"PAR1" or length > min(size - 12, 64 * 1024 * 1024):
            raise ValueError("invalid or oversized Parquet footer")
        source.seek(-8 - length, 2)
        protocol = TCompactProtocol(TMemoryBuffer(source.read(length)))
    protocol.readStructBegin()
    while True:
        _, kind, field = protocol.readFieldBegin()
        if kind == TType.STOP:
            raise ValueError("Parquet footer has no schema")
        if field == 2 and kind == TType.LIST:
            item_type, count = protocol.readListBegin()
            if item_type != TType.STRUCT or not 0 < count <= 100000:
                raise ValueError("invalid Parquet schema list")
            return [_struct(protocol) for _ in range(count)]
        protocol.skip(kind)
        protocol.readFieldEnd()


def _top_level(elements: list[dict[int, Any]]) -> list[dict[int, Any]]:
    cursor = 1
    result = []
    for _ in range(elements[0].get(5, 0)):
        start = cursor
        pending = 1
        while pending:
            if cursor >= len(elements):
                raise ValueError("truncated Parquet schema")
            children = elements[cursor].get(5, 0)
            if children < 0:
                raise ValueError("negative schema child count")
            pending += children - 1
            cursor += 1
        result.append(elements[start])
    if cursor != len(elements):
        raise ValueError("invalid Parquet schema tree")
    return result


def validate_variant_file(path: str, parquet: Any, columns: tuple[Column, ...]) -> None:
    """Validate VARIANT files; the existing strict scalar-only path is unchanged."""

    def fail(message: str) -> None:
        raise ValidationError(message, status_code=None)

    try:
        physical = _top_level(_schema_elements(path))
    except (ValueError, EOFError, IndexError, struct.error) as error:
        fail(f"invalid prepared Parquet schema: {error}")
        return
    arrow = parquet.schema_arrow
    if arrow.names != [c.name for c in columns] or len(physical) != len(columns):
        fail("prepared Parquet columns differ from destination")
    for column, field, element in zip(columns, arrow, physical, strict=True):
        if (
            element.get(9) != column.field_id
            or field.metadata is None
            or field.metadata.get(PARQUET_FIELD_ID_KEY) != str(column.field_id).encode()
        ):
            fail(f"prepared field ID differs for {column.name}")
        if element.get(4) != column.name or element.get(3) not in (0, 1):
            fail(f"invalid prepared field {column.name}")
        if column.type == "variant":
            annotation = element.get(10, {})
            if (
                1 in element
                or set(annotation) != {16}
                or annotation[16].get(1, 1) != 1
                or not pa.types.is_struct(field.type)
            ):
                fail(f"{column.name} must be native Parquet VARIANT version 1")
            children = {child.name: child for child in field.type}
            metadata = children.get("metadata")
            value = children.get("value")
            if (
                len(children) != len(field.type)
                or not set(children).issubset({"metadata", "value", "typed_value"})
                or metadata is None
                or metadata.type != pa.binary()
                or metadata.nullable
                or not ({"value", "typed_value"} & children.keys())
                or (value is not None and value.type != pa.binary())
            ):
                fail(f"invalid native VARIANT storage for {column.name}")
            null_path = column.name + ".metadata"
        else:
            expected = coltype_to_arrow(column.type, column.type_params)
            if expected == pa.timestamp("s"):
                expected = pa.timestamp("ms")
            actual = field.type
            if column.type == "uuid" and isinstance(actual, pa.BaseExtensionType):
                actual = actual.storage_type
            if (
                pa.types.is_timestamp(actual)
                and actual.tz
                and pa.types.is_timestamp(expected)
                and expected.tz
            ):
                actual = pa.timestamp(actual.unit, "UTC")
                expected = pa.timestamp(expected.unit, "UTC")
            if 5 in element or actual != expected:
                fail(f"prepared Parquet type differs for {column.name}")
            null_path = column.name
        if not column.nullable and field.nullable:
            # DuckDB writes optional fields. Accept them for NOT NULL only when
            # every row group's required metadata/scalar leaf proves zero nulls.
            for group in range(parquet.metadata.num_row_groups):
                row_group = parquet.metadata.row_group(group)
                chunks = [
                    row_group.column(i)
                    for i in range(row_group.num_columns)
                    if row_group.column(i).path_in_schema == null_path
                ]
                if (
                    len(chunks) != 1
                    or chunks[0].statistics is None
                    or not chunks[0].statistics.has_null_count
                    or chunks[0].statistics.null_count != 0
                ):
                    fail(
                        f"prepared file cannot prove non-null values for {column.name}"
                    )
