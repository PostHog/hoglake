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

from .errors import UnsupportedTypeError, ValidationError
from .models import Column
from .types import (
    NESTED_TYPES,
    PARQUET_FIELD_ID_KEY,
    coltype_to_arrow,
    column_to_arrow_field,
    uuid_storage_form,
    uuid_storage_form_schema,
)


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


def _top_level(elements: list[dict[int, Any]]) -> list[tuple[dict[int, Any], range]]:
    """Each top-level schema element with the leaf-column range it owns.

    A row group's column chunks are its leaves, in schema order, so the
    range indexes ``row_group.column(...)`` directly. That is how a
    container's own leaves are found: by position in the tree, never by
    parsing a dotted ``path_in_schema``, whose synthetic level names
    (``list``/``element``/``item``) differ between writers and whose
    separator a column name may itself contain.
    """
    cursor = 1
    leaves = 0
    result = []
    for _ in range(elements[0].get(5, 0)):
        start, first_leaf = cursor, leaves
        pending = 1
        while pending:
            if cursor >= len(elements):
                raise ValueError("truncated Parquet schema")
            children = elements[cursor].get(5, 0)
            if children < 0:
                raise ValueError("negative schema child count")
            if children == 0:
                leaves += 1
            pending += children - 1
            cursor += 1
        result.append((elements[start], range(first_leaf, leaves)))
    if cursor != len(elements):
        raise ValueError("invalid Parquet schema tree")
    return result


def _arrow_children(kind: pa.DataType) -> list[pa.Field]:
    if pa.types.is_struct(kind):
        return list(kind)
    if pa.types.is_map(kind):
        return [kind.key_field, kind.item_field]
    if pa.types.is_list(kind) or pa.types.is_large_list(kind):
        return [kind.value_field]
    return []


def _leaf_count(kind: pa.DataType) -> int:
    children = _arrow_children(kind)
    return sum(_leaf_count(child.type) for child in children) if children else 1


def _field_id_fault(expected: pa.Field, actual: pa.Field, path: str) -> str | None:
    """Compare parquet field ids below the top level.

    Arrow type equality ignores field metadata and the synthetic element
    name, so it proves the shape and nothing about identity. hoglake binds
    a file to a schema by field id at every level, so the ids are checked
    here, against the same recursion the writer used.
    """
    children = zip(
        _arrow_children(expected.type), _arrow_children(actual.type), strict=True
    )
    for want, got in children:
        here = f"{path}.{want.name}"
        if (got.metadata or {}).get(PARQUET_FIELD_ID_KEY) != (want.metadata or {}).get(
            PARQUET_FIELD_ID_KEY
        ):
            return f"prepared field ID differs for {here}"
        deeper = _field_id_fault(want, got, here)
        if deeper is not None:
            return deeper
    return None


def _container_fault(column: Column, field: pa.Field) -> str | None:
    try:
        expected = column_to_arrow_field(column)
    except UnsupportedTypeError as error:
        # An unbuildable catalog container is the destination's problem,
        # not the prepared file's, but the caller still sees a refusal
        # rather than a stack trace about the wrong API.
        return f"cannot describe destination column {column.name}: {error}"
    if uuid_storage_form(field.type) != uuid_storage_form(expected.type):
        return f"prepared Parquet type differs for {column.name}"
    return _field_id_fault(expected, field, column.name)


def prepared_schema_matches(found: pa.Schema, want: pa.Schema) -> bool:
    """Whether a prepared file's Arrow schema is the destination's.

    Field for field, names, nullability and the ``PARQUET:field_id``
    metadata compared exactly — the identity check that binds a file to
    the catalog. The ONE licence is the uuid column's two legal
    spellings: hoglake's uuid wire form is
    ``FIXED_LEN_BYTE_ARRAY(16) + UUID``, which pyarrow produces only for
    ``pa.uuid()``, while every file registered before that contract (and
    any writer below the pyarrow floor) carries the bare fixed(16). The
    bytes are identical, so both are accepted, at any nesting depth
    (:func:`~pyhoglake.types.uuid_storage_form`).

    Nothing else is loosened: the field ids still have to match, so a
    file that annotates its uuid column but misnumbers it is refused
    exactly as before.
    """
    return uuid_storage_form_schema(found).equals(
        uuid_storage_form_schema(want), check_metadata=True
    )


def validate_variant_file(path: str, parquet: Any, columns: tuple[Column, ...]) -> None:
    """Validate native VARIANT or opt-in external files using physical schema and counts."""

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
    for column, field, (element, leaves) in zip(columns, arrow, physical, strict=True):
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
            # metadata is the variant's one REQUIRED leaf, so it stands for
            # the column: it is null exactly when the variant is. The
            # variant spec addresses these children by name, not position
            # (hoglake#70), so find it by name and convert to a leaf index
            # -- the shape check above does not pin the child order.
            offset = 0
            for child in field.type:
                if child.name == "metadata":
                    break
                offset += _leaf_count(child.type)
            proof, all_of = leaves[offset : offset + 1], True
        elif column.type in NESTED_TYPES:
            fault = _container_fault(column, field)
            if fault is not None:
                fail(fault)
            # Any one clean leaf proves the container: a leaf at full
            # definition level has every ancestor present. The converse
            # does not hold, so all the leaves are offered and one
            # suffices.
            proof, all_of = leaves, False
        else:
            expected = coltype_to_arrow(column.type, column.type_params)
            if expected == pa.timestamp("s"):
                expected = pa.timestamp("ms")
            # Both sides normalized, not just the file's: the catalog's
            # uuid type is itself the annotated extension now, and a file
            # carrying the bare fixed(16) is the same 16 bytes.
            expected = uuid_storage_form(expected)
            actual = uuid_storage_form(field.type)
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
            proof, all_of = leaves, True
        if not column.nullable and field.nullable:
            # DuckDB writes optional fields. Accept them for NOT NULL only
            # when every row group's leaves prove zero nulls.
            for group in range(parquet.metadata.num_row_groups):
                row_group = parquet.metadata.row_group(group)
                if not proof or max(proof) >= row_group.num_columns:
                    fail(f"prepared file has no columns for {column.name}")
                clean = [
                    chunk.statistics is not None
                    and chunk.statistics.has_null_count
                    and chunk.statistics.null_count == 0
                    for chunk in (row_group.column(i) for i in proof)
                ]
                if not (all(clean) if all_of else any(clean)):
                    fail(
                        f"prepared file cannot prove non-null values for {column.name}"
                    )
