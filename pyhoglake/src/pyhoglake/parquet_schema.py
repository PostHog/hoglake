"""Prepared-file validation, including native Parquet VARIANT annotations.

Arrow 25 exposes VARIANT as its storage struct and drops its logical annotation.
Read only the schema list from the compact-Thrift footer to distinguish native
VARIANT from an ordinary struct. No payload pages are loaded or rewritten.

The same footer reading serves pyhoglake's own VARIANT files, which Arrow
cannot annotate either: :func:`write_variant_parquet` writes the storage
with pyarrow, splices the annotations into the footer
(:func:`stamp_variant_footer`), and holds the result to the declaration's
layout (:func:`variant_layout_fault`).
"""

from __future__ import annotations

import os
import struct
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, NamedTuple, TypeAlias

import pyarrow as pa
import pyarrow.parquet as pq
from thrift.protocol.TCompactProtocol import TCompactProtocol
from thrift.Thrift import TType
from thrift.transport.TTransport import TMemoryBuffer

from ._variant_codec import Plan, member, structure_fault
from .errors import UnsupportedTypeError, ValidationError
from .models import Column
from .types import (
    NESTED_TYPES,
    PARQUET_FIELD_ID_KEY,
    coltype_to_arrow,
    column_to_arrow_field,
    map_names_form,
    uuid_storage_form,
    uuid_storage_form_schema,
)
from .variant import SHREDDING_KEY, _column_plan

#: A Parquet file: its path, or the whole file in memory.
Source: TypeAlias = str | os.PathLike[str] | bytes | bytearray | memoryview | pa.Buffer

#: The largest footer read: its length is the file's to state, and the
#: bytes are read whole before any of them is checked.
_MAX_FOOTER = 64 * 1024 * 1024

# parquet.thrift's SchemaElement field ids, and the Type values the stamps
# check, by name where the code below reads them.
_TYPE, _NAME, _CHILDREN = 1, 4, 5
_CONVERTED, _SCALE, _PRECISION, _FIELD_ID, _LOGICAL = 6, 7, 8, 9, 10
_INT32, _INT64 = 1, 2


def _struct(protocol: Any, depth: int = 0) -> dict[int, Any]:
    return _fields(protocol, depth)[0]


def _fields(protocol: Any, depth: int = 0) -> tuple[dict[int, Any], int]:
    """A struct's fields by Thrift id, and the id of its last field.

    BOOLs are kept: TIMESTAMP's and TIME's ``isAdjustedToUTC`` and
    INTEGER's ``isSigned`` are BOOLs, and without them TIMESTAMP(true)
    read as TIMESTAMP(false), and INT(8, true) as UINT_8. A list or any
    other type is skipped. The last id counts a skipped field too: the
    compact protocol writes each field id as a delta from the one before,
    so a splice that adds a field after it has to know it.
    """
    if depth > 8:
        raise ValueError("Parquet logical annotation nesting is too deep")
    result: dict[int, Any] = {}
    last = 0
    protocol.readStructBegin()
    while True:
        _, kind, field = protocol.readFieldBegin()
        if kind == TType.STOP:
            break
        last = field
        if kind == TType.I32:
            result[field] = protocol.readI32()
        elif kind == TType.BYTE:
            result[field] = protocol.readByte()
        elif kind == TType.BOOL:
            result[field] = protocol.readBool()
        elif kind == TType.STRING:
            result[field] = protocol.readString()
        elif kind == TType.STRUCT:
            result[field] = _struct(protocol, depth + 1)
        else:
            protocol.skip(kind)
        protocol.readFieldEnd()
    protocol.readStructEnd()
    return result, last


def _footer_bytes(source: Source) -> tuple[int, bytes]:
    """The offset of a Parquet file's footer, and the footer: the Thrift
    FileMetaData, without the length and the magic after it.

    ``source`` is a path, whose tail alone is read, or the whole file in
    memory. Only a plaintext footer is read: an encrypted one ends in
    ``PARE``, and its schema is ciphertext.
    """
    if isinstance(source, (str, os.PathLike)):
        with open(source, "rb") as file:
            file.seek(0, 2)
            size = file.tell()
            if size < 8:
                raise ValueError("invalid or oversized Parquet footer")
            file.seek(-8, 2)
            length = _footer_length(file.read(8), size)
            file.seek(-8 - length, 2)
            return size - 8 - length, file.read(length)
    view = memoryview(source).cast("B")
    size = len(view)
    if size < 8:
        raise ValueError("invalid or oversized Parquet footer")
    length = _footer_length(bytes(view[-8:]), size)
    start = size - 8 - length
    return start, bytes(view[start : size - 8])


def _footer_length(trailer: bytes, size: int) -> int:
    length: int = struct.unpack("<I", trailer[:4])[0]
    if trailer[4:] == b"PARE":
        raise ValueError("Parquet footer is encrypted (PARE)")
    if trailer[4:] != b"PAR1" or length > min(size - 12, _MAX_FOOTER):
        raise ValueError("invalid or oversized Parquet footer")
    return length


def _read_schema(footer: bytes) -> list[tuple[dict[int, Any], int, int]]:
    """Each element of a footer's schema list: its fields, the offset in
    the footer of its STOP byte, and the id of its last field."""
    transport = TMemoryBuffer(footer)
    protocol = TCompactProtocol(transport)
    protocol.readStructBegin()
    while True:
        _, kind, field = protocol.readFieldBegin()
        if kind == TType.STOP:
            raise ValueError("Parquet footer has no schema")
        if field == 2 and kind == TType.LIST:
            item_type, count = protocol.readListBegin()
            if item_type != TType.STRUCT or not 0 < count <= 100000:
                raise ValueError("invalid Parquet schema list")
            cursor = transport.cstringio_buf
            read = []
            for _ in range(count):
                fields, last = _fields(protocol)
                # The compact protocol's readStructEnd reads nothing, so the
                # byte just read is the element's STOP.
                read.append((fields, cursor.tell() - 1, last))
            return read
        protocol.skip(kind)
        protocol.readFieldEnd()


def _schema_elements_from_footer(footer: bytes) -> list[dict[int, Any]]:
    return [fields for fields, _, _ in _read_schema(footer)]


def _schema_elements(source: Source) -> list[dict[int, Any]]:
    return _schema_elements_from_footer(_footer_bytes(source)[1])


class SchemaElement(NamedTuple):
    """One schema element of a footer, with where it sits.

    ``stop`` is the offset in the footer of the element's STOP byte, before
    which a field can be added; ``last`` is the id of its last field.
    ``path`` is its names from the root's child down; a tuple, never a
    dotted string, because a declared name may hold a dot (Trino's ``a.b``).
    """

    fields: dict[int, Any]
    stop: int
    last: int
    path: tuple[str, ...]


def _positioned_elements(footer: bytes) -> list[SchemaElement]:
    """The schema list of a footer, each element a :class:`SchemaElement`.
    A tree that claims more elements than it holds, holds more than it
    claims, or counts children below zero is refused, with ValueError: the
    splice places its bytes by these paths."""
    out = []
    # [children still to come, path] of each group being read.
    open_groups: list[list[Any]] = []
    for index, (fields, stop, last) in enumerate(_read_schema(footer)):
        path: tuple[str, ...] = ()
        if index:
            while open_groups and not open_groups[-1][0]:
                open_groups.pop()
            name = fields.get(_NAME)
            if not open_groups or not isinstance(name, str):
                raise ValueError("invalid Parquet schema tree")
            open_groups[-1][0] -= 1
            path = (*open_groups[-1][1], name)
        children = fields.get(_CHILDREN, 0)
        if not isinstance(children, int) or children < 0:
            raise ValueError("negative schema child count")
        if children:
            open_groups.append([children, path])
        out.append(SchemaElement(fields, stop, last, path))
    if any(left for left, _ in open_groups):
        raise ValueError("truncated Parquet schema")
    return out


def _top_level(
    elements: list[dict[int, Any]],
) -> list[tuple[dict[int, Any], range, int]]:
    """Each top-level schema element with the leaf-column range it owns,
    and its index in ``elements``.

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
        result.append((elements[start], range(first_leaf, leaves), start))
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
    the catalog. There are TWO licences. The first is the uuid column's
    two legal spellings: hoglake's uuid wire form is
    ``FIXED_LEN_BYTE_ARRAY(16) + UUID``, which pyarrow produces only for
    ``pa.uuid()``, while every file registered before that contract (and
    any writer below the pyarrow floor) carries the bare fixed(16). The
    bytes are identical, so both are accepted, at any nesting depth
    (:func:`~pyhoglake.types.uuid_storage_form`).

    The second is a map's synthetic names. Every writer and reader names
    the entries struct, key and value differently, and pyarrow names the
    entries after the map's own field on read, so both sides are normalized
    (:func:`~pyhoglake.types.map_names_form`).

    Nothing else is loosened: the field ids still have to match, so a
    file that annotates its uuid column but misnumbers it, or that
    numbers a map's key or value differently, is refused exactly as
    before.
    """

    def comparable(schema: pa.Schema) -> pa.Schema:
        schema = uuid_storage_form_schema(schema)
        return pa.schema(
            [f.with_type(map_names_form(f.type)) for f in schema],
            metadata=schema.metadata,
        )

    return comparable(found).equals(comparable(want), check_metadata=True)


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
    for column, field, (element, leaves, _) in zip(
        columns, arrow, physical, strict=True
    ):
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


# -- stamping pyhoglake's own VARIANT files ------------------------------------


@dataclass(frozen=True)
class FooterStamp:
    """One annotation :func:`stamp_variant_footer` adds to a footer.

    ``variant`` is VARIANT(1) on a top-level group, which must carry
    ``field_id``; ``decimal4`` and ``decimal8`` are DECIMAL(precision,
    scale) on an INT32 or INT64 leaf. ``path`` is the element's names from
    the top-level column down.
    """

    path: tuple[str, ...]
    kind: Literal["variant", "decimal4", "decimal8"]
    field_id: int | None = None
    precision: int = 0
    scale: int = 0

    def __post_init__(self) -> None:
        # The root is never a stamp's target (the splice looks below it);
        # first, since every message below names the path.
        if not self.path:
            raise ValueError("a stamp needs a path below the root")
        # Any other would be stamped as a decimal8 (_annotation), typo or not.
        if self.kind not in ("variant", "decimal4", "decimal8"):
            raise ValueError(f"{_where(self.path)}: no stamp of kind {self.kind!r}")
        # A group's stamp is checked against the group's field id, so None
        # would match a group that has none; a leaf's would be ignored.
        has_id = isinstance(self.field_id, int) and not isinstance(self.field_id, bool)
        if has_id != (self.kind == "variant"):
            raise ValueError(
                f"{_where(self.path)}: a {self.kind} stamp with field id "
                f"{self.field_id!r}"
            )


def variant_stamps(column: Column, plan: Plan) -> list[FooterStamp]:
    """The stamps a file of ``column`` written from ``plan``'s storage
    needs: VARIANT(1) on its group and DECIMAL on each leaf of
    :attr:`Plan.stamps`, which the file holds as INT32 (decimal4) or INT64
    (decimal8) as :meth:`Plan.expected_elements` says."""
    expected = dict(plan.expected_elements(column))
    stamps = [FooterStamp((column.name,), "variant", field_id=column.field_id)]
    for path, precision, scale in plan.stamps:
        where = (column.name, *path)
        physical = expected[where][_TYPE]
        stamps.append(
            FooterStamp(
                where,
                "decimal4" if physical == _INT32 else "decimal8",
                precision=precision,
                scale=scale,
            )
        )
    return stamps


def stamp_variant_footer(
    raw: bytes | bytearray | memoryview | pa.Buffer, stamps: Sequence[FooterStamp]
) -> tuple[bytes, list[dict[int, Any]]]:
    """The file ``raw`` with ``stamps`` added to its footer, and the
    patched footer's schema elements.

    pyarrow writes a VARIANT group as the struct it is stored as, with no
    annotation (it reads one, but drops it on write), and a decimal4 or
    decimal8 leaf, given its unscaled integers, as a plain INT32 or INT64.
    Each annotation is spliced in as the bytes the compact protocol encodes
    it as, before the element's STOP byte: 7 bytes for VARIANT(1) on a
    group (``logicalType``), 14 for DECIMAL(p, s) on a leaf
    (``converted_type``, ``scale``, ``precision`` and ``logicalType``). A
    field inside the schema list changes neither the list's header (an
    element count) nor how any later field is encoded, and the body is
    untouched, so every offset into it (column chunks, page indexes, bloom
    filters) stays valid; only the footer's length in the trailer changes.

    The patched footer is not read again: the elements returned are the
    ones read before the splice with the stamps' fields added. The bytes
    depend on nothing but the stamp and the element's last field id, which
    the preconditions hold to the field id (a group) or to name or
    num_children (a leaf), so the tests' generic re-encoding pins every
    spelling there is; the exact size of the patch is asserted here.

    Each precondition refuses loudly, with ValueError (as a FooterStamp
    with another kind, a misplaced field id or no path does when it is
    made): a footer that is not plaintext ``PAR1``; a path that is not in
    the footer exactly once, or stamped twice; a group with a type, an
    annotation, a field id other than the stamp's, or a field after the
    field id; a leaf of another physical type, or with an annotation, a
    scale, a precision, a field id or any field after ``num_children``. A
    pyarrow that started writing any of these would stop pyhoglake's writes
    rather than have it publish a layout nobody checked.
    """
    start, footer = _footer_bytes(raw)
    elements = _positioned_elements(footer)
    found: dict[tuple[str, ...], list[SchemaElement]] = {}
    for element in elements[1:]:
        found.setdefault(element.path, []).append(element)
    inserts: dict[int, bytes] = {}
    stamped: dict[tuple[str, ...], FooterStamp] = {}
    for stamp in stamps:
        where = _where(stamp.path)
        if stamp.path in stamped:
            raise ValueError(f"{where} is stamped twice")
        stamped[stamp.path] = stamp
        hits = found.get(stamp.path, [])
        if len(hits) != 1:
            raise ValueError(f"{where} is in the footer {len(hits)} times, not once")
        inserts[hits[0].stop] = _annotation(stamp, hits[0])
    # In footer order, whatever order the stamps came in: spliced in the
    # order given, a later offset first, the pieces between them would
    # repeat a stretch of the footer.
    pieces = []
    previous = 0
    for offset in sorted(inserts):
        pieces += [footer[previous:offset], inserts[offset]]
        previous = offset
    pieces.append(footer[previous:])
    patched = b"".join(pieces)
    groups = sum(stamp.kind == "variant" for stamp in stamps)
    if len(patched) != len(footer) + 7 * groups + 14 * (len(stamps) - groups):
        raise ValueError("stamped footer has the wrong length")
    view = memoryview(raw).cast("B")
    body = b"".join((view[:start], patched, struct.pack("<I", len(patched)), b"PAR1"))
    return body, [_stamped(e.fields, stamped.get(e.path)) for e in elements]


def _annotation(stamp: FooterStamp, element: SchemaElement) -> bytes:
    """The compact-protocol bytes of ``stamp``'s fields, inserted before
    ``element``'s STOP, whose last field is ``element.last``."""
    fields, last, where = element.fields, element.last, _where(stamp.path)
    if stamp.kind == "variant":
        if _TYPE in fields or not fields.get(_CHILDREN):
            raise ValueError(f"{where} is not a group")
        if _CONVERTED in fields or _LOGICAL in fields:
            raise ValueError(f"{where} is already annotated")
        if fields.get(_FIELD_ID) != stamp.field_id:
            raise ValueError(
                f"{where} has field id {fields.get(_FIELD_ID)}, not {stamp.field_id}"
            )
        if last != _FIELD_ID:
            raise ValueError(f"{where} has field {last} after its field id")
        # Field 10 (a struct, a delta of 1 from the field id), the LogicalType
        # union's field 16 (VARIANT: a long-form header, the id as a zigzag
        # i16), its VariantType's field 1 (an i8, version 1), two STOPs.
        return bytes((0x1C, 0x0C, 0x20, 0x13, 0x01, 0x00, 0x00))
    physical, widest = (_INT32, 9) if stamp.kind == "decimal4" else (_INT64, 18)
    if fields.get(_TYPE) != physical:
        raise ValueError(
            f"{where} is physical type {fields.get(_TYPE)}, not {physical}"
        )
    if _FIELD_ID in fields:
        raise ValueError(f"{where} has a field id")
    held = sorted(set(fields) & {_CONVERTED, _SCALE, _PRECISION, _LOGICAL})
    if held:
        raise ValueError(f"{where} already has fields {held}")
    if last > _CHILDREN:
        raise ValueError(f"{where} has field {last} after num_children")
    if not 0 <= stamp.scale <= stamp.precision <= widest or not stamp.precision:
        raise ValueError(
            f"{where}: DECIMAL({stamp.precision}, {stamp.scale}) is not a {stamp.kind}"
        )
    # Precision and scale are at most 18, so each zigzag varint is one byte.
    scale, precision = stamp.scale << 1, stamp.precision << 1
    return bytes(
        (
            (_CONVERTED - last) << 4 | 0x05,  # converted_type, an i32:
            0x0A,  # DECIMAL (5)
            0x15,  # scale
            scale,
            0x15,  # precision
            precision,
            0x2C,  # logicalType, a struct (a delta of 2)
            0x5C,  # the LogicalType union's DECIMAL (field 5)
            0x15,  # DecimalType's scale
            scale,
            0x15,  # and precision
            precision,
            0x00,  # STOP DecimalType
            0x00,  # STOP LogicalType
        )
    )


def _stamped(fields: dict[int, Any], stamp: FooterStamp | None) -> dict[int, Any]:
    """An element's fields as they read once ``stamp`` is added."""
    if stamp is None:
        return fields
    if stamp.kind == "variant":
        return {**fields, _LOGICAL: {16: {1: 1}}}
    scale, precision = stamp.scale, stamp.precision
    return {
        **fields,
        _CONVERTED: 5,
        _SCALE: scale,
        _PRECISION: precision,
        _LOGICAL: {5: {1: scale, 2: precision}},
    }


def _where(path: Sequence[str]) -> str:
    """A path of Parquet names in a message: the column's name, then each
    name below it as :func:`member` spells a key, so ``a.b`` is one name."""
    return path[0] + "".join(member(name) for name in path[1:])


# -- holding pyhoglake's own VARIANT files to their declarations ---------------


def variant_layout_fault(
    elements: list[dict[int, Any]], start: int, column: Column, plan: Plan
) -> str | None:
    """What is wrong with the VARIANT layout pyhoglake wrote for
    ``column``, whose group is ``elements[start]``, or None.

    The subtree must be ``plan``'s :meth:`~Plan.expected_elements`,
    element for element: names, order, repetition, child counts, physical
    type and length, logical type with its BOOLs, scale and precision, the
    field id on the group and on nothing below it, and VARIANT(1) on the
    group. The one licence is a converted type, the legacy spelling of the
    logical type beside it (with a DECIMAL's scale and precision), which a
    writer may leave out whole, but not spell otherwise. Every element's
    child count is compared, so a subtree whose elements all match has the
    expected extent too.

    This is for pyhoglake's own files only, which the declaration says
    everything about; a file another writer produced may lay a declaration
    out otherwise, as the spec allows.
    """
    cursor, pending = start, 1
    while pending:
        if cursor >= len(elements):
            return "truncated Parquet schema"
        children = elements[cursor].get(_CHILDREN, 0)
        if not isinstance(children, int) or children < 0:
            return "negative schema child count"
        pending += children - 1
        cursor += 1
    for offset, (path, want) in enumerate(plan.expected_elements(column)):
        index = start + offset
        found = elements[index] if index < cursor else None
        if found is None or not _same_element(found, want):
            return f"{_where(path)} is {found!r}, not the declaration's {want!r}"
    return None


def _same_element(found: dict[int, Any], want: dict[int, Any]) -> bool:
    if found == want:
        return True
    # A converted type, with the scale and precision a DECIMAL one carries,
    # is the legacy spelling of the logical type beside it: a writer may
    # leave it out, but not write another.
    legacy = {_CONVERTED, _SCALE, _PRECISION}
    return _CONVERTED in want and found == {
        field: value for field, value in want.items() if field not in legacy
    }


@dataclass(frozen=True)
class VariantParquet:
    """A Parquet file :func:`write_variant_parquet` wrote, stamped and
    checked: the bytes to upload, and what the registration reads off
    them, the sizes measured AFTER the footer was patched."""

    body: bytes
    #: The footer's length from the patched trailer: the wire's
    #: ``footer_size`` (the Thrift FileMetaData, without the 8-byte suffix).
    footer_size: int
    #: pyarrow's FileMetaData of the file as written, before the stamps:
    #: the row count, the row groups, and every column chunk's offsets,
    #: sizes, null counts and raw statistics bytes, which the stamps do not
    #: touch. The stamped annotations are not the body's: no VARIANT on the
    #: groups, and no DECIMAL on their decimal4/decimal8 leaves, whose
    #: statistics therefore read as unscaled integers. Nor is
    #: ``serialized_size``, which is 0 here. ``extract_column_stats`` skips
    #: a variant's storage whole.
    metadata: Any

    @property
    def size(self) -> int:
        return len(self.body)


def write_variant_parquet(
    table: pa.Table,
    columns: Sequence[Column],
    plans: Mapping[str, Plan],
    *,
    row_group_size: int | None = None,
    write_options: Mapping[str, Any] | None = None,
) -> VariantParquet:
    """Write ``table``, whose variant columns are in the write form of
    their plans, as the Parquet file of their declarations.

    ``plans`` maps each variant column's name to its plan, the compiled
    ``type_params.shredding`` of that column in ``columns`` (another
    declaration's is refused, with ValueError), and every variant column of
    ``table`` must have one; ``columns`` are the catalog's, and ``table``
    carries the catalog's field ids. ``write_options`` comes from
    :func:`pyhoglake.parquet_write_options` (empty means pyarrow's default
    codec, SNAPPY), as for the other write paths. Each step is what the
    file must be for the Trino connector:

    1. Each decimal4/decimal8 leaf is viewed (zero-copy) as the int32/int64
       its decimal32/decimal64 already holds: pyarrow writes a decimal32 or
       decimal64 as a fixed-length array, and the file-wide
       ``store_decimal_as_integer`` would also change a decimal16 leaf and
       any top-level decimal column (D15).
    2. ``store_schema=False``: a stored Arrow schema would describe the
       group as the plain struct, with integer leaves, and pyarrow reads
       that back instead of the footer.
    3. The footer is stamped (:func:`stamp_variant_footer`).
    4. Each variant column's subtree is held to its declaration, element
       for element (:func:`variant_layout_fault`), on the elements the
       splice read: the file is not opened again.

    Storage with a null in a non-nullable field below the top is refused
    with ValueError: under a present row pyarrow would write it as other
    values, and under a SQL NULL row it cannot view it. The encoder's
    output holds neither.

    A failure at 3 or 4 is pyhoglake's, or a pyarrow it does not know, and
    raises ValidationError before anything is uploaded. How pyarrow reads
    the file back (a plain struct, never an extension type) is pinned by
    the tests' canaries, on every pyarrow CI runs.
    """
    by_name = {column.name: column for column in columns}
    if not plans:
        raise ValueError("no variant columns to write; use pq.write_table")
    if set(plans) - set(table.column_names):
        raise ValueError("a planned variant column is not in the table")
    fields, data, stamps = [], [], []
    for field in table.schema:
        plan = plans.get(field.name)
        column = by_name.get(field.name)
        if plan is None:
            if column is not None and column.type == "variant":
                # Written as it stands, it would be a plain struct with no
                # annotation, and nothing below would look at it.
                raise ValueError(f"variant column {field.name} has no plan")
            fields.append(field)
            data.append(table.column(field.name))
            continue
        if column is None or column.type != "variant":
            raise ValueError(f"{field.name} is not a variant column of the table")
        # The layout check holds the file to the plan, so a plan of another
        # declaration would pass it, in a layout that is not the column's.
        declared = _column_plan(column, (column.type_params or {}).get(SHREDDING_KEY))
        if plan.expected_elements(column) != declared.expected_elements(column):
            raise ValueError(f"the plan of {field.name} is not its declaration's")
        if plan.form_of(field.type) != "write":
            raise ValueError(f"{field.name} is not in its plan's write form")
        # pyarrow writes a null struct in a REQUIRED group as its children's
        # bytes, here an empty metadata: an invalid variant that both the
        # splice and the layout check pass, since the layout is right.
        if not column.nullable and table.column(field.name).null_count:
            raise ValueError(f"{field.name} is NOT NULL but has SQL NULL rows")
        kind = _integer_view(field.type)
        fields.append(field.with_type(kind))
        chunks = table.column(field.name).chunks
        # A null in a REQUIRED field under a present parent (metadata, an
        # object's field, a list's element) pyarrow writes as the masked
        # children's values or empty bytes; structure_fault names its path.
        for chunk in chunks:
            fault = structure_fault(chunk)
            if fault is not None:
                raise ValueError(f"{field.name}: {fault}")
        # Arrow's view refuses a null anywhere in a non-nullable child's
        # buffer, so what is left for it is a null under a SQL NULL row (as
        # take on a null index and pa.nulls build) or outside a slice's rows:
        # keep it when kind is the type unchanged.
        try:
            views = [chunk.view(kind) for chunk in chunks]
        except pa.ArrowInvalid as error:
            raise ValueError(
                f"{field.name} has a null in a non-nullable field under a SQL "
                "NULL row or outside its rows, which the writer cannot view: "
                "build the storage with the encoder"
            ) from error
        data.append(pa.chunked_array(views, kind))
        stamps += variant_stamps(column, plan)
    schema = pa.schema(fields, metadata=table.schema.metadata)
    sink = pa.BufferOutputStream()
    # The writer's own FileMetaData, collected as it closes: the stats need
    # no second parse of the footer.
    collected: list[Any] = []
    # store_schema last, so no option can put the Arrow schema back.
    options = {**(write_options or {}), "store_schema": False}
    pq.write_table(
        pa.table(data, schema=schema),
        sink,
        row_group_size=row_group_size,
        metadata_collector=collected,
        **options,
    )
    try:
        body, elements = stamp_variant_footer(sink.getvalue(), stamps)
    except ValueError as error:
        raise ValidationError(
            f"cannot annotate VARIANT footer: {error}", status_code=None
        ) from error
    starts = {element.get(_NAME): index for element, _, index in _top_level(elements)}
    for name, plan in plans.items():
        fault = variant_layout_fault(elements, starts[name], by_name[name], plan)
        if fault is not None:
            raise ValidationError(
                f"pyhoglake wrote VARIANT column {name} in a layout that is not its "
                f"declaration's: {fault}",
                status_code=None,
            )
    (metadata,) = collected
    return VariantParquet(body, _footer_size(body), metadata)


def _footer_size(raw: bytes) -> int:
    """Thrift footer-metadata length for the commit's ``footer_size``.

    Wire convention (bugs.md #7): ``footer_size`` is EXACTLY the 4-byte
    LE length stored in the parquet trailer — the serialized thrift
    FileMetaData size, EXCLUDING the trailing 8-byte suffix (4-byte
    length + ``PAR1`` magic). The server's hydrator tail-reads
    ``[file_size - footer_size - 8, file_size)`` and compaction stores
    the same value for its own outputs; shipping ``meta_len + 8`` here
    (the old behavior) made every client-written file 8 bytes off.
    """
    (meta_len,) = struct.unpack("<I", raw[-8:-4])
    return meta_len


def _integer_view(kind: pa.DataType) -> pa.DataType:
    """``kind`` with each decimal32/decimal64 leaf the int32/int64 its
    storage already is."""
    if pa.types.is_decimal32(kind):
        return pa.int32()
    if pa.types.is_decimal64(kind):
        return pa.int64()
    if pa.types.is_struct(kind):
        return pa.struct([field.with_type(_integer_view(field.type)) for field in kind])
    if pa.types.is_list(kind):
        return pa.list_(
            kind.value_field.with_type(_integer_view(kind.value_field.type))
        )
    return kind
