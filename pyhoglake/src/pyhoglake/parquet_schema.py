"""Prepared-file validation, including native Parquet VARIANT annotations.

The compact-Thrift footer is read as Thrift's generated readers (pyarrow's
C++, parquet-java) read it, and no payload page is loaded or rewritten. Its
schema list tells native VARIANT from an ordinary struct, which Arrow 25
cannot: it exposes VARIANT as its storage struct and drops the annotation
(:func:`validate_variant_file`). Its column chunks' metadata and statistics
are checked before pyarrow reads them, for every prepared file, since
pyarrow aborts the process on some it opens (:func:`validate_column_chunks`).

The same footer reading serves pyhoglake's own VARIANT files, which Arrow
cannot annotate either: :func:`write_variant_parquet` writes the storage
with pyarrow and splices the annotations into the footer
(:func:`stamp_variant_footer`), and :func:`variant_layout_fault` holds the
result to the declaration's layout, or a file another writer produced to
the layouts the spec and the Trino connector accept.
"""

from __future__ import annotations

import math
import os
import struct
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, NamedTuple, TypeAlias, overload

import pyarrow as pa
import pyarrow.parquet as pq
from thrift.protocol.TCompactProtocol import TCompactProtocol
from thrift.Thrift import TException, TType
from thrift.transport.TTransport import TMemoryBuffer

from ._variant_codec import MAX_VARIANT_DEPTH, UNSHREDDED_PLAN, Node, Plan, member
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
from .variant import SHREDDING_KEY, _column_plan

#: A Parquet file: its path, or the whole file in memory.
Source: TypeAlias = str | os.PathLike[str] | bytes | bytearray | memoryview | pa.Buffer

#: The largest footer read: its length is the file's to state, and the
#: bytes are read whole before any of them is checked.
_MAX_FOOTER = 64 * 1024 * 1024

# parquet.thrift's Type, FieldRepetitionType and ConvertedType values, and
# the field ids of SchemaElement, by name where the code below reads them.
_BOOLEAN, _INT32, _INT64, _INT96, _FLOAT, _DOUBLE, _BYTE_ARRAY, _FIXED = range(8)
_REQUIRED, _OPTIONAL, _REPEATED = 0, 1, 2
_TYPE, _LENGTH, _REPETITION, _NAME, _CHILDREN = 1, 2, 3, 4, 5
_CONVERTED, _SCALE, _PRECISION, _FIELD_ID, _LOGICAL = 6, 7, 8, 9, 10


class _Declared(dict[int, tuple[int, Any]]):
    """parquet.thrift's declaration of a struct on the way to what this
    module reads, ``name``: each field it declares, by field id, as its wire
    type and what is inside it (a struct's own declaration; a list's item,
    as the same pair; ``"text"`` for a binary read as UTF-8; an
    :class:`_Unread` for what no check reads). Thrift's generated readers,
    pyarrow's C++ and parquet-java's, skip a field whose wire type is not
    the declared one, so :func:`_fields` does too: a repetition_type
    spelled as an i8 is no repetition_type to pyarrow, which reads the
    element as REQUIRED, and it must be none here, or this module checks a
    schema pyarrow does not read.

    Every field parquet.thrift declares in these structs (arrow 21's copy)
    is declared here, read or not, so a field of another wire type is
    skipped as its header spells it, as they skip it, and a field not
    declared here is one parquet.thrift does not define. That one is skipped
    as its header spells it too, which reads the same bytes the generated
    readers read or skip, except in a list: they read a list's items as its
    declared item type whatever its header says, where a reader without the
    declaration follows the header. An encodings list whose header said its
    one item was a binary hid, inside that binary, a Statistics that pyarrow
    read as the chunk's own. So a list, a set or a map in a field
    parquet.thrift does not define is refused (:func:`_skip`): one a later
    parquet.thrift adds would hide fields from this module just so.

    A ``union`` (LogicalType, TimeUnit, ColumnOrder, ColumnCryptoMetaData,
    EncryptionAlgorithm) must spell exactly one field, of any id or wire
    type, which :func:`_fields` refuses otherwise. pyarrow's C++ reads a
    union as a struct, every field of it, where parquet-java's TUnion
    reads its first field and then takes the next field header for the
    union's end: a union of two fields is one annotation to pyarrow and a
    footer parquet-java cannot read (its fields run on into the next
    struct's), and one of none fails TUnion too ("Unrecognized type 0").
    """

    def __init__(
        self,
        name: str,
        fields: Mapping[int, tuple[int, Any]] | None = None,
        *,
        union: bool = False,
    ) -> None:
        super().__init__(fields or {})
        self.name = name
        self.union = union


class _Unread(NamedTuple):
    """A declared field no check reads: read as ``inside`` declares it (a
    list's item type held to the declaration, as everywhere), and kept as
    None, or a list as its length. A footer can spell a struct or a list
    item in one byte, which Python holds in a hundred."""

    inside: Any


class _Each(NamedTuple):
    """A declared list whose items are handed, each as it is read, to the
    reader's ``visit`` as ``(name, item)`` rather than kept, the list kept
    as its length: a footer's row groups and their column chunks, which are
    checked one at a time and never held together. Without a ``visit`` they
    are read past (:class:`_Unread`)."""

    inside: Any
    name: str


class _Enum(NamedTuple):
    """A required field of parquet.thrift's enum ``name``, whose defined
    ``values`` alone are read: parquet-java's generated reader (the server's
    hydrator's, and the Trino connector's) reads any other as null, and then
    refuses the whole footer for a required field that is absent ("Required
    field 'codec' was not present!"), where pyarrow's C++ keeps the number
    (and reads a codec it does not know as UNCOMPRESSED). So :func:`_fields`
    refuses one, with ValueError. An optional enum's unknown value is the
    field unset to parquet-java, which the checks that read one say
    (:func:`_converted`, :func:`_schema_fault`)."""

    name: str
    values: frozenset[int]


def _unread(kind: int, *fields: int) -> dict[int, tuple[int, Any]]:
    """Declared fields of Thrift type ``kind`` that no check reads."""
    return {field: (kind, _Unread(None)) for field in fields}


def _unread_list(item: _Declared) -> tuple[int, Any]:
    """A declared list of ``item`` structs that no check reads."""
    return TType.LIST, _Unread((TType.STRUCT, item))


_TIME_UNIT = _Declared(
    "TimeUnit",
    {
        unit: (TType.STRUCT, _Declared(name))
        for unit, name in ((1, "MilliSeconds"), (2, "MicroSeconds"), (3, "NanoSeconds"))
    },
    union=True,
)


def _temporal(name: str) -> _Declared:
    # isAdjustedToUTC of TIME and TIMESTAMP, isSigned of INTEGER, are BOOLs:
    # without them TIMESTAMP(true, MICROS) and TIMESTAMP(false, MICROS) read
    # the same, and so do INT(8, true) and UINT8.
    return _Declared(name, {1: (TType.BOOL, None), 2: (TType.STRUCT, _TIME_UNIT)})


_LOGICAL_TYPE = _Declared(
    "LogicalType",
    {
        **{
            member: (TType.STRUCT, _Declared(name))
            for member, name in (
                (1, "StringType"),
                (2, "MapType"),
                (3, "ListType"),
                (4, "EnumType"),
                (6, "DateType"),
                (11, "NullType"),
                (12, "JsonType"),
                (13, "BsonType"),
                (14, "UUIDType"),
                (15, "Float16Type"),
            )
        },
        5: (
            TType.STRUCT,
            _Declared("DecimalType", {1: (TType.I32, None), 2: (TType.I32, None)}),
        ),
        7: (TType.STRUCT, _temporal("TimeType")),
        8: (TType.STRUCT, _temporal("TimestampType")),
        10: (
            TType.STRUCT,
            _Declared("IntType", {1: (TType.BYTE, None), 2: (TType.BOOL, None)}),
        ),
        16: (TType.STRUCT, _Declared("VariantType", {1: (TType.BYTE, None)})),
        17: (TType.STRUCT, _Declared("GeometryType", _unread(TType.STRING, 1))),
        18: (
            TType.STRUCT,
            _Declared(
                "GeographyType", {**_unread(TType.STRING, 1), **_unread(TType.I32, 2)}
            ),
        ),
    },
    union=True,
)
_SCHEMA_ELEMENT = _Declared(
    "SchemaElement",
    {
        **{field: (TType.I32, None) for field in (1, 2, 3, 5, 6, 7, 8, 9)},
        4: (TType.STRING, "text"),
        10: (TType.STRUCT, _LOGICAL_TYPE),
    },
)
_STATISTICS = _Declared(
    "Statistics",
    {
        **{field: (TType.STRING, None) for field in (1, 2, 5, 6)},
        **_unread(TType.I64, 3, 4),
        **_unread(TType.BOOL, 7, 8),
    },
)
# The histograms are checked by length alone.
_SIZE_STATISTICS = _Declared(
    "SizeStatistics",
    {
        1: (TType.I64, None),
        2: (TType.LIST, _Unread((TType.I64, None))),
        3: (TType.LIST, _Unread((TType.I64, None))),
    },
)
_KEY_VALUE = _Declared("KeyValue", _unread(TType.STRING, 1, 2))
# GROUP_VAR_INT (1) is gone from parquet.thrift's Encoding, and from
# parquet-java's. An item of the encodings list (ColumnMetaData field 2) is
# no required field, and parquet-java reads an unknown one as a null item.
_ENCODING = _Enum("Encoding", frozenset({0, *range(2, 10)}))
_PAGE_ENCODING_STATS = _Declared(
    "PageEncodingStats",
    {
        1: (TType.I32, _Enum("PageType", frozenset(range(4)))),
        2: (TType.I32, _ENCODING),
        **_unread(TType.I32, 3),
    },
)
_COLUMN_METADATA = _Declared(
    "ColumnMetaData",
    {
        1: (TType.I32, None),
        2: (TType.LIST, _Unread((TType.I32, None))),  # encodings
        3: (TType.LIST, (TType.STRING, None)),
        4: (TType.I32, _Enum("CompressionCodec", frozenset(range(8)))),
        **_unread(TType.I32, 15),
        **_unread(TType.I64, 5, 6, 7, 9, 10, 11, 14),
        8: _unread_list(_KEY_VALUE),
        12: (TType.STRUCT, _STATISTICS),
        13: _unread_list(_PAGE_ENCODING_STATS),
        16: (TType.STRUCT, _SIZE_STATISTICS),
        17: (
            TType.STRUCT,
            _Unread(
                _Declared(
                    "GeospatialStatistics",
                    {
                        1: (
                            TType.STRUCT,
                            _Declared(
                                "BoundingBox", _unread(TType.DOUBLE, *range(1, 9))
                            ),
                        ),
                        2: (TType.LIST, (TType.I32, None)),
                    },
                )
            ),
        ),
    },
)
# Whose presence alone is read.
_COLUMN_CRYPTO = _Declared(
    "ColumnCryptoMetaData",
    {
        1: (TType.STRUCT, _Declared("EncryptionWithFooterKey")),
        2: (
            TType.STRUCT,
            _Declared(
                "EncryptionWithColumnKey",
                {1: (TType.LIST, (TType.STRING, None)), **_unread(TType.STRING, 2)},
            ),
        ),
    },
    union=True,
)
_COLUMN_CHUNK = _Declared(
    "ColumnChunk",
    {
        **_unread(TType.STRING, 1, 9),
        **_unread(TType.I64, 2, 4, 6),
        **_unread(TType.I32, 5, 7),
        3: (TType.STRUCT, _COLUMN_METADATA),
        8: (TType.STRUCT, _Unread(_COLUMN_CRYPTO)),
    },
)
_ROW_GROUP = _Declared(
    "RowGroup",
    {
        1: (TType.LIST, _Each((TType.STRUCT, _COLUMN_CHUNK), "chunk")),
        **_unread(TType.I64, 2, 3, 5, 6),
        4: _unread_list(
            _Declared(
                "SortingColumn",
                {**_unread(TType.I32, 1), **_unread(TType.BOOL, 2, 3)},
            )
        ),
        **_unread(TType.I16, 7),
    },
)


def _aes(name: str) -> _Declared:
    return _Declared(name, {**_unread(TType.STRING, 1, 2), **_unread(TType.BOOL, 3)})


_FILE_METADATA = _Declared(
    "FileMetaData",
    {
        **_unread(TType.I32, 1),
        2: (TType.LIST, (TType.STRUCT, _SCHEMA_ELEMENT)),
        **_unread(TType.I64, 3),
        4: (TType.LIST, _Each((TType.STRUCT, _ROW_GROUP), "row group")),
        5: _unread_list(_KEY_VALUE),
        **_unread(TType.STRING, 6, 9),
        7: (
            TType.LIST,
            _Each(
                (
                    TType.STRUCT,
                    _Declared(
                        "ColumnOrder",
                        {
                            1: (TType.STRUCT, _Declared("TypeDefinedOrder")),
                            2: (TType.STRUCT, _Declared("IEEE754TotalOrder")),
                        },
                        union=True,
                    ),
                ),
                "column order",
            ),
        ),
        8: (
            TType.STRUCT,
            _Unread(
                _Declared(
                    "EncryptionAlgorithm",
                    {
                        1: (TType.STRUCT, _aes("AesGcmV1")),
                        2: (TType.STRUCT, _aes("AesGcmCtrV1")),
                    },
                    union=True,
                )
            ),
        ),
    },
)

#: How deep the footer reader follows structs and lists into one another;
#: no Parquet footer nests a tenth as deep.
_MAX_THRIFT_DEPTH = 32


#: What the footer reader hands each item of an :class:`_Each` list to.
_Visit: TypeAlias = Callable[[str, dict[int, Any]], None]


def _fields(
    protocol: Any,
    declared: _Declared,
    depth: int = 0,
    *,
    keep: bool = True,
    visit: _Visit | None = None,
) -> tuple[dict[int, Any], int]:
    """A struct's fields by id, read as ``declared`` says, and the id of
    the last field it holds. Without ``keep`` they are read past and none
    is returned; ``visit`` is given the items of the lists that declare it
    (:class:`_Each`).

    A field that is not declared, or not of its declared wire type, is
    skipped (:func:`_skip`). The first undeclared one is kept, as None,
    so a check that asks whether a struct holds a field it does not
    expect (a logical type of a member parquet.thrift does not define, an
    element pyarrow writes a new field on) still sees one; never which, or
    how many, which would cost memory per field a footer spells in a byte.
    One of another wire type reads as absent, as it does to the generated
    readers.

    A union that does not spell exactly one field, counting those skipped,
    is refused, with ValueError (:class:`_Declared` says why), and so is a
    required enum field of a value its enum does not define (:class:`_Enum`).

    A struct that repeats a field id is refused, with ValueError. No
    writer repeats one, and the readers disagree on what a repeat means:
    pyarrow's C++ reads a repeated struct field into the one it read
    first, so a field only the first copy sets survives, where
    parquet-java keeps the last copy whole.

    So is a field id outside an i16, which is what the generated readers
    hold one in: they truncate a long-form id to 16 bits and wrap a delta
    sum at 16 bits, where Python's Thrift keeps either whole, so id 65552
    is SizeStatistics (16) to pyarrow and to parquet-java, and an
    undeclared field here, which would be neither checked nor seen to
    repeat. No writer spells one.

    The last id counts the fields this skips as well: the compact protocol
    writes each field id as a delta from the one before, so a splice that
    adds a field has to know it.
    """
    if depth > _MAX_THRIFT_DEPTH:
        raise ValueError("Parquet footer nesting is too deep")
    result: dict[int, Any] = {}
    # The ids read, as the bits of an int (a negative id's above the i16's
    # positive half): a set would hold some 70 bytes for each field, which
    # a footer spells in a byte.
    seen = 0
    last = 0
    count = 0
    undeclared = False
    protocol.readStructBegin()
    while True:
        _, kind, field = protocol.readFieldBegin()
        if kind == TType.STOP:
            break
        count += 1
        _within(field, TType.I16, "field id")
        bit = 1 << (field if field >= 0 else (1 << 15) - field)
        if seen & bit:
            raise ValueError(f"a Parquet footer struct repeats field {field}")
        seen |= bit
        last = field
        wire, inside = declared.get(field, (None, None))
        if kind != wire:
            # A field of another wire type is skipped by its header, by the
            # generated readers too, whatever it holds; an undeclared one may
            # be declared by a later parquet.thrift, so no list in it is
            # read here otherwise than pyarrow reads it.
            _skip(
                protocol,
                kind,
                depth + 1,
                containers=wire is not None,
                where=(field, declared.name),
            )
            if keep and wire is None and not undeclared:
                result[field] = None
                undeclared = True
        elif isinstance(inside, _Enum):
            value = _value(protocol, kind, None, depth + 1)
            if value not in inside.values:
                raise ValueError(
                    f"field {field} of a Parquet footer {declared.name} is {value}, "
                    f"which parquet.thrift's {inside.name} does not define: "
                    "parquet-java (the server's hydrator) and the Trino connector "
                    "read it as absent, and cannot read a footer without it"
                )
            if keep:
                result[field] = value
        elif isinstance(inside, _Unread):
            value = _value(protocol, kind, inside.inside, depth + 1, keep=False)
            if keep:
                result[field] = value
        elif isinstance(inside, _Each):
            value = _value(
                protocol,
                kind,
                inside.inside,
                depth + 1,
                keep=False,
                visit=visit,
                each=inside.name,
            )
            if keep:
                result[field] = value
        else:
            value = _value(protocol, kind, inside, depth + 1, keep=keep, visit=visit)
            if keep:
                result[field] = value
        protocol.readFieldEnd()
    protocol.readStructEnd()
    if declared.union and count != 1:
        raise ValueError(
            f"a Parquet footer {declared.name} union holds {count} fields, not one, "
            "which parquet-java (the server's hydrator) and the Trino connector "
            "cannot read a footer with"
        )
    return result, last


def _skip(
    protocol: Any,
    kind: int,
    depth: int,
    *,
    containers: bool,
    where: tuple[int, str] = (0, ""),
) -> None:
    """Read past a value of Thrift type ``kind`` as its headers spell it,
    the way the generated readers skip a field, keeping nothing.

    Without ``containers`` a list, a set or a map anywhere in it is refused,
    with ValueError: the value is field ``where`` (an id, and the struct it
    is in), which parquet.thrift as this module knows it does not define,
    and a later one that declares it would have pyarrow read the list's
    items as the declared type, whatever the header says, where this
    follows the header (the hiding :class:`_Declared` describes). So the
    refusal names it, and says what a writer that spells one is: newer
    than this pyhoglake, as pyarrow was when it began writing
    SizeStatistics, whose histograms are lists. Its size is held to an i32
    either way, which the generated readers read it as.
    """
    if depth > _MAX_THRIFT_DEPTH:
        raise ValueError("Parquet footer nesting is too deep")
    if kind == TType.STRUCT:
        protocol.readStructBegin()
        while True:
            _, field_kind, _ = protocol.readFieldBegin()
            if field_kind == TType.STOP:
                break
            _skip(protocol, field_kind, depth + 1, containers=containers, where=where)
            protocol.readFieldEnd()
        protocol.readStructEnd()
        return
    if kind in (TType.LIST, TType.SET, TType.MAP):
        name = {TType.LIST: "list", TType.SET: "set", TType.MAP: "map"}[kind]
        if not containers:
            field, struct_name = where
            raise ValueError(
                f"the footer holds a {name} in field {field} of a {struct_name}, "
                "which this pyhoglake's parquet.thrift does not define (a writer "
                "newer than this pyhoglake may write one: upgrade pyhoglake, or "
                "pin the writer)"
            )
        if kind == TType.MAP:
            key_kind, value_kind, count = protocol.readMapBegin()
            kinds: tuple[int, ...] = (key_kind, value_kind)
        else:
            item_kind, count = protocol.readListBegin()
            kinds = (item_kind,)
        _within(count, TType.I32, f"{name} size")
        for _ in range(count):
            for item_kind in kinds:
                _skip(protocol, item_kind, depth + 1, containers=True)
        (protocol.readMapEnd if kind == TType.MAP else protocol.readListEnd)()
        return
    if kind == TType.STRING:
        protocol.readBinary()
        return
    reader = _THRIFT_READERS.get(kind)
    if reader is None:
        raise ValueError(f"unknown Thrift type {kind}")
    getattr(protocol, reader)()


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
            file.seek(-8, 2)
            trailer = file.read(8)
            length = _footer_length(trailer, size)
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


@overload
def _schema_elements_from_footer(
    footer: bytes, *, positions: Literal[False] = False
) -> list[dict[int, Any]]: ...


@overload
def _schema_elements_from_footer(
    footer: bytes, *, positions: Literal[True]
) -> list[SchemaElement]: ...


def _schema_elements_from_footer(
    footer: bytes, *, positions: bool = False
) -> list[dict[int, Any]] | list[SchemaElement]:
    """The schema list of a footer: each element's fields by Thrift id, or
    with ``positions`` each as a :class:`SchemaElement`.

    Without ``positions`` the whole FileMetaData is read, as Thrift's
    generated readers read it (:func:`_file_metadata`), so a footer with
    two schema lists is refused. With ``positions`` the footer is read only
    as far as its first schema list: the splice reads a footer pyarrow has
    just written, once per file written, and pyarrow writes one.
    """
    if not positions:
        return _schema_list(_file_metadata(footer))
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
                fields, last = _fields(protocol, _SCHEMA_ELEMENT)
                # The compact protocol's readStructEnd reads nothing, so the
                # byte just read is the element's STOP.
                read.append((fields, cursor.tell() - 1, last))
            return _with_paths(read)
        _skip(protocol, kind, 1, containers=False, where=(field, "FileMetaData"))
        protocol.readFieldEnd()


def _file_metadata(footer: bytes, visit: _Visit | None = None) -> dict[int, Any]:
    """A footer's FileMetaData, its fields by Thrift id, read as Thrift's
    generated readers read the fields this module reads
    (:data:`_FILE_METADATA`), with no struct that repeats a field id
    (:func:`_fields`): two schema lists, say, of which pyarrow reads the
    last; with no union of more or fewer fields than one, which
    parquet-java reads otherwise than pyarrow (:class:`_Declared`); and
    with no field id or integer wider than they hold it in
    (:func:`_within`), which they would read as another.

    Its row groups, and their column chunks, are not kept (field 4 is their
    count), nor its column orders (field 7): each is handed to ``visit`` as
    it is read, if one is given (:class:`_ChunkCheck`). Nothing no check
    reads is kept either, so a footer is checked holding its schema list (as
    dicts, a few times the bytes it is spelled in) and one row group,
    whatever else it holds."""
    meta: dict[int, Any] = _value(
        TCompactProtocol(TMemoryBuffer(footer)),
        TType.STRUCT,
        _FILE_METADATA,
        0,
        visit=visit,
    )
    return meta


def _schema_list(meta: dict[int, Any]) -> list[dict[int, Any]]:
    elements = meta.get(2)
    if elements is None:
        raise ValueError("Parquet footer has no schema")
    if not 0 < len(elements) <= 100000:
        raise ValueError("invalid Parquet schema list")
    return list(elements)


def _with_paths(read: list[tuple[dict[int, Any], int, int]]) -> list[SchemaElement]:
    out = []
    # [children still to come, path] of each group being read.
    open_groups: list[list[Any]] = []
    for index, (fields, stop, last) in enumerate(read):
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


def _schema_elements(path: str) -> list[dict[int, Any]]:
    return _schema_elements_from_footer(_footer_bytes(path)[1])


def _is_leaf(element: dict[int, Any]) -> bool:
    """Whether a schema element is a leaf column as pyarrow reads it: one
    with a physical type and no children. One with neither is an empty
    group, which has no column chunk."""
    return not element.get(_CHILDREN) and _TYPE in element


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
            if _is_leaf(elements[cursor]):
                leaves += 1
            pending += children - 1
            cursor += 1
        result.append((elements[start], range(first_leaf, leaves), start))
    if cursor != len(elements):
        raise ValueError("invalid Parquet schema tree")
    return result


#: The bytes a statistic of each fixed-width physical type is plain-encoded
#: in; a FIXED_LEN_BYTE_ARRAY's is its type_length, a BYTE_ARRAY's any.
_STATISTIC_WIDTH = {
    _BOOLEAN: 1,
    _INT32: 4,
    _INT64: 8,
    _INT96: 12,
    _FLOAT: 4,
    _DOUBLE: 8,
}


def _value(
    protocol: Any,
    kind: int,
    inside: Any,
    depth: int,
    *,
    keep: bool = True,
    visit: _Visit | None = None,
    each: str | None = None,
) -> Any:
    """A declared compact-Thrift value: a struct as its fields by id (read
    as ``inside``, its declaration, says: :func:`_fields`), a list as a
    list, and a binary as bytes, or as text where ``inside`` says so. The
    library's own ``skip`` decodes a binary as UTF-8, which a statistic is
    not. Without ``keep`` it is read past, and None returned, or a list's
    length. ``visit`` is handed each item of a list that names them
    ``each`` (an :class:`_Each`), as ``visit(each, item)``, and passed on to
    the lists of a struct's fields.

    A list whose items are of another type than the declared one is
    refused, with ValueError: Thrift's generated readers read a declared
    list's items as the declared type, whatever its header says. So is an
    integer, or a list's size, wider than they hold it in
    (:data:`_WIDTHS`): they keep its low bits where Python's Thrift keeps
    the whole varint, and the two would read different values.
    """
    if depth > _MAX_THRIFT_DEPTH:
        raise ValueError("Parquet footer nesting is too deep")
    if kind == TType.STRUCT:
        fields = _fields(protocol, inside, depth, keep=keep, visit=visit)[0]
        return fields if keep else None
    if kind == TType.LIST:
        # Every item takes a byte at least, so a count the footer cannot
        # hold runs out of bytes rather than memory.
        item_kind, count = protocol.readListBegin()
        _within(count, TType.I32, "list size")
        declared_kind, item_inside = inside
        if item_kind != declared_kind:
            raise ValueError(
                f"a Parquet footer list holds Thrift type {item_kind}, "
                f"not the declared {declared_kind}"
            )
        items: list[Any] | int = count
        if keep:
            items = [
                _value(protocol, item_kind, item_inside, depth + 1)
                for _ in range(count)
            ]
        elif each is not None and visit is not None:
            for _ in range(count):
                item = _value(protocol, item_kind, item_inside, depth + 1, visit=visit)
                visit(each, item)
        else:
            for _ in range(count):
                _value(protocol, item_kind, item_inside, depth + 1, keep=False)
        protocol.readListEnd()
        return items
    if kind == TType.STRING:
        value = protocol.readBinary()
        if not keep:
            return None
        return value.decode() if inside == "text" else value
    reader = _THRIFT_READERS.get(kind)
    if reader is None:
        raise ValueError(f"unknown Thrift type {kind}")
    value = getattr(protocol, reader)()
    if kind in _WIDTHS:
        _within(value, kind, "integer")
    return value if keep else None


_THRIFT_READERS = {
    TType.BOOL: "readBool",
    TType.BYTE: "readByte",
    TType.I16: "readI16",
    TType.I32: "readI32",
    TType.I64: "readI64",
    TType.DOUBLE: "readDouble",
}

#: The bits Thrift's generated readers hold each kind of varint in. A
#: size is an i32 to them (C++ refuses a negative one); a field id an i16.
_WIDTHS = {TType.I16: 16, TType.I32: 32, TType.I64: 64}


def _within(value: int, kind: int, what: str) -> None:
    """Refuse, with ValueError, a varint the generated readers would read
    as another value: one outside the ``kind`` they hold it in."""
    bits = _WIDTHS[kind]
    if not -(1 << (bits - 1)) <= value < 1 << (bits - 1):
        raise ValueError(f"a Parquet footer {what} of {value} is not an i{bits}")


def _leaf_levels(
    elements: list[dict[int, Any]],
) -> list[tuple[dict[int, Any], int, int]]:
    """Each leaf of a schema list, in order, with its maximum definition and
    repetition levels: the OPTIONAL and REPEATED elements on its path, and
    the REPEATED ones, the leaf itself included and the root not, as
    pyarrow's ColumnDescriptor counts them, and its leaves as pyarrow
    tells them (:func:`_is_leaf`)."""
    return [leaf[:3] for leaf in _leaves(elements)]


def _leaves(
    elements: list[dict[int, Any]],
) -> Iterator[tuple[dict[int, Any], int, int, tuple[str, ...]]]:
    """:func:`_leaf_levels`, each leaf with its path too: its names from the
    root's child down, which its column chunks' ``path_in_schema`` spell."""
    # [children still to come, definition, repetition, path] of each open
    # group.
    open_groups: list[list[Any]] = []
    for element in elements[1:]:
        while open_groups and not open_groups[-1][0]:
            open_groups.pop()
        path: tuple[str, ...] = ()
        definition, repetition = 0, 0
        if open_groups:
            open_groups[-1][0] -= 1
            _, definition, repetition, path = open_groups[-1]
        path = (*path, element[_NAME])
        kind = element.get(_REPETITION)
        if kind == _OPTIONAL:
            definition += 1
        elif kind == _REPEATED:
            definition += 1
            repetition += 1
        children = element.get(_CHILDREN, 0)
        if children > 0:
            open_groups.append([children, definition, repetition, path])
        elif _is_leaf(element):
            yield element, definition, repetition, path


#: pyarrow's name of each physical type (``ColumnSchema.physical_type``).
_PHYSICAL_NAMES = {
    "BOOLEAN": _BOOLEAN,
    "INT32": _INT32,
    "INT64": _INT64,
    "INT96": _INT96,
    "FLOAT": _FLOAT,
    "DOUBLE": _DOUBLE,
    "BYTE_ARRAY": _BYTE_ARRAY,
    "FIXED_LEN_BYTE_ARRAY": _FIXED,
}


class _ChunkCheck:
    """Which column chunk pyarrow cannot read the metadata or statistics
    of, found as :func:`_file_metadata` reads the footer (it is the
    reader's ``visit``), and then :meth:`fault`. ``parquet`` is the
    ``pq.ParquetFile`` open on the file.

    pyarrow builds a chunk's metadata when ``row_group.column(i)`` is first
    read and decodes its statistics when ``.statistics`` is, and what its
    C++ throws in either place is not turned into a Python exception: the
    process aborts (std::terminate, on 23.0.0 to 26.0.0). Opening the file
    checks none of it, so every chunk is checked here, from the footer's
    bytes, before any of them is read: the NOT NULL proof reads them, and
    so does ``extract_column_stats`` after it, every chunk of every file.

    Each chunk is held to its leaf as pyarrow reads the leaf: the physical
    type, length and maximum levels of the ColumnDescriptor pyarrow built
    when it opened the file, which are what it checks the chunk against.
    The schema list read must be the same: one whose leaves differ from
    pyarrow's in any of those is refused, so the checks of this module that
    read the schema list read the one pyarrow does. What pyarrow throws on,
    and this refuses:

    - building the metadata: a chunk encrypted with a column key it has no
      key for (any encrypted chunk is refused; nothing here can read one),
      and SizeStatistics that do not fit the leaf, namely a level histogram
      neither empty nor one entry per level (0 to the leaf's maximum) or
      ``unencoded_byte_array_data_bytes`` on a leaf that is not
      BYTE_ARRAY (``SizeStatistics::Validate``);
    - decoding the statistics: a ColumnMetaData type that is not the
      leaf's, and a min or max too short for the physical type. The legacy
      min and max are held to the width too, though pyarrow decodes them
      for some writers only.

    It also refuses a chunk whose ``path_in_schema`` is not UTF-8, which
    pyarrow raises UnicodeDecodeError on rather than aborting, when
    ``extract_column_stats`` reads it, and one whose ``path_in_schema`` is
    not its leaf's path, name for name and case for case: pyarrow pairs a
    chunk with a leaf by position, but parquet-java (the server's
    hydrator) and the Trino connector find a chunk's column by that path,
    and cannot read a footer naming one the schema does not have, and
    ``extract_column_stats`` files a chunk's statistics under it, so one
    naming a sibling would bound the sibling with this leaf's values. The
    first row group's paths are kept until the schema list has been read
    (a footer may spell its row groups first), and each later row group's
    are held to them.

    The fault is the first row group's that has one: that it does not have
    a chunk per leaf, or else its first chunk's.

    The footer's column orders are handed to it too, as they are read, for
    :meth:`order_fault`.
    """

    def __init__(self, parquet: Any) -> None:
        schema = parquet.metadata.schema
        self.leaves = [
            _leaf(
                _PHYSICAL_NAMES[column.physical_type],
                column.length,
                column.max_definition_level,
                column.max_repetition_level,
            )
            for column in (schema.column(i) for i in range(len(schema)))
        ]
        self.group = 0
        self.chunks = 0  # of the row group being read
        # The first row group's chunk paths, by chunk, packed (None for a
        # chunk with no metadata, which is a fault of its own).
        self.paths: list[bytes | None] = []
        # The first fault, as (row group, chunk, what): the row group's own
        # (no chunk per leaf) as chunk -1, before its chunks'.
        self.found: tuple[int, int, str] | None = None
        self.orders = 0  # read so far
        # The leaves given IEEE_754_TOTAL_ORDER that are neither a FLOAT nor
        # a DOUBLE, by leaf (a FLOAT16 among them is told by its annotation,
        # in order_fault): one a leaf at most.
        self.ieee754: list[int] = []

    def _record(self, group: int, chunk: int, fault: str) -> None:
        if self.found is None or (group, chunk) < self.found[:2]:
            self.found = (group, chunk, fault)

    def __call__(self, name: str, fields: dict[int, Any]) -> None:
        if name == "column order":
            index = self.orders
            self.orders += 1
            # pyarrow opens no file whose column orders are not one a leaf.
            if 2 in fields and self.leaves[index][0] not in (_FLOAT, _DOUBLE):
                self.ieee754.append(index)
            return
        if name == "chunk":
            index = self.chunks
            self.chunks += 1
            column = fields.get(3)
            path = None if column is None else _packed(column.get(3) or ())
            if self.group == 0:
                self.paths.append(path)
            if self.found is not None or index >= len(self.leaves):
                # A fault found from here on comes after the one found (but
                # for the first row group's paths, kept for the end); and a
                # first row group short of a chunk has one.
                return
            fault = _chunk_fault(fields, *self.leaves[index])
            # Held to the first row group's path, which fault() holds to the
            # leaf's; where that one is not, or is missing, its fault comes
            # first. (A chunk without metadata here has a fault already.)
            first = self.paths[index] if self.group else None
            if (
                fault is None
                and path is not None
                and first is not None
                and path != first
            ):
                fault = _path_fault(path, first)
            if fault is not None:
                self._record(
                    self.group,
                    index,
                    f"column chunk {index} of row group {self.group} {fault}",
                )
            return
        # A row group, read after its chunks; field 1 is their count.
        if fields.get(1) != len(self.leaves):
            self._record(
                self.group,
                -1,
                f"row group {self.group} does not have a column chunk per leaf",
            )
        self.group += 1
        self.chunks = 0

    def order_fault(self, elements: list[dict[int, Any]]) -> str | None:
        """Which leaf has a column order parquet-java (the server's hydrator)
        cannot build it with, once the footer whose schema list is
        ``elements`` has been read, or None: IEEE_754_TOTAL_ORDER on anything
        but a FLOAT, a DOUBLE or a FLOAT16 ("The column order
        IEEE_754_TOTAL_ORDER is not supported by type INT64"). pyarrow reads
        the footer, and so does the Trino connector, which reads no column
        order. A member parquet.thrift does not define, alone, is an
        undefined order to parquet-java, and to pyarrow, and stands, as does
        TYPE_DEFINED_ORDER on an INT96, which parquet-java reads as
        undefined."""
        leaves = [
            (index, element)
            for index, element in enumerate(elements)
            if index and _is_leaf(element)
        ]
        for leaf in self.ieee754:
            index, element = leaves[leaf]
            if element.get(_TYPE) == _FIXED and _effective(element) == {15: {}}:
                continue  # FLOAT16
            return (
                f"schema element {index} ({element.get(_NAME)!r}) has the column "
                "order IEEE_754_TOTAL_ORDER, which parquet-java (the server's "
                "hydrator) takes on a FLOAT, a DOUBLE or a FLOAT16 alone, and "
                "cannot read a footer with otherwise"
            )
        return None

    def fault(self, elements: list[dict[int, Any]]) -> str | None:
        """The fault, once the footer whose schema list is ``elements`` has
        been read."""
        read = 0
        recorded = False
        for index, (element, definition, repetition, names) in enumerate(
            _leaves(elements)
        ):
            leaf = _leaf(
                element.get(_TYPE), element.get(_LENGTH), definition, repetition
            )
            if index >= len(self.leaves) or leaf != self.leaves[index]:
                return "the footer's schema does not read as pyarrow reads it"
            read += 1
            path = self.paths[index] if index < len(self.paths) else None
            want = _packed([name.encode() for name in names])
            # A chunk without metadata, which has no path, is refused as such.
            if path is not None and path != want and not recorded:
                fault = _path_fault(path, want)
                self._record(0, index, f"column chunk {index} of row group 0 {fault}")
                recorded = True
        if read != len(self.leaves):
            return "the footer's schema does not read as pyarrow reads it"
        return None if self.found is None else self.found[2]


def _packed(names: Sequence[bytes]) -> bytes:
    """A path of names as one bytes value, each name after its length, so
    that no two paths pack alike: one object a chunk of the first row group,
    where a tuple of names would be two or more."""
    return b"".join(len(name).to_bytes(4, "little") + name for name in names)


def _path_fault(path: bytes, want: bytes) -> str:
    def text(packed: bytes) -> str:
        names, cursor = [], 0
        while cursor < len(packed):
            size = int.from_bytes(packed[cursor : cursor + 4], "little")
            names.append(
                packed[cursor + 4 : cursor + 4 + size].decode(errors="replace")
            )
            cursor += 4 + size
        return repr(names)

    return (
        f"has the path_in_schema {text(path)}, not its leaf's {text(want)}, "
        "which parquet-java (the server's hydrator) and the Trino connector "
        "find the chunk's column by, and extract_column_stats files its "
        "statistics under"
    )


def _leaf(physical: Any, length: Any, definition: int, repetition: int) -> Any:
    # The length is a FIXED_LEN_BYTE_ARRAY's alone.
    return physical, length if physical == _FIXED else None, definition, repetition


def _chunk_fault(
    chunk: dict[int, Any],
    physical: int,
    length: Any,
    definition: int,
    repetition: int,
) -> str | None:
    """What of a column chunk pyarrow cannot read, held to its leaf
    (:class:`_ChunkCheck`), or None."""
    if 8 in chunk:
        return "is encrypted"
    column = chunk.get(3)
    if column is None:
        return "has no metadata"
    if column.get(1) != physical:
        return f"is of type {column.get(1)!r}, not its schema leaf's {physical!r}"
    if not _utf8(column.get(3)):
        return "has a path in schema that is not UTF-8"
    fault = _size_statistics_fault(column.get(16), physical, definition, repetition)
    if fault is not None:
        return fault
    statistics = column.get(12)
    if statistics is None:
        return None
    width = length if physical == _FIXED else _STATISTIC_WIDTH.get(physical, 0)
    for field in (1, 2, 5, 6):  # max, min, max_value, min_value
        bound = statistics.get(field)
        if bound is not None and len(bound) < width:
            return f"has a {len(bound)}-byte statistic of a {width}-byte type"
    return None


def _footer_faults(
    source: Source, parquet: Any
) -> tuple[list[dict[int, Any]], str | None, str | None]:
    """A prepared file's schema list, read as Thrift's generated readers
    read it, with what is wrong with the schema (:func:`_schema_fault`)
    and, when nothing is, with its column chunks (:class:`_ChunkCheck`).
    Raises what reading the footer can (:data:`_FOOTER_ERRORS`)."""
    check = _ChunkCheck(parquet)
    elements = _schema_list(_file_metadata(_footer_bytes(source)[1], check))
    schema_fault = _schema_fault(elements) or check.order_fault(elements)
    if schema_fault is not None:
        return elements, schema_fault, None
    return elements, None, check.fault(elements)


def _schema_fault(elements: list[dict[int, Any]]) -> str | None:
    """Which schema element below the root pyarrow reads and parquet-java
    cannot, or reads as another type, and why, or None: one with no
    repetition_type parquet.thrift defines (REQUIRED, OPTIONAL or
    REPEATED), a group with a physical type, or an annotation the two read
    apart (:func:`_annotation_fault`). parquet-java is the server's
    hydrator, and the Trino connector's ``ParquetMetadata.readTypeSchema``
    reads the schema as it does, so a footer neither can read fails every
    query that reaches the file, and one they read as another type serves
    other values than the ones pyarrow's statistics were taken from.

    pyarrow reads an element without a repetition_type as REQUIRED: an
    absent field, one spelled with another wire type (which the generated
    readers skip) or a value outside the enum. parquet-java calls
    ``name()`` on the field and throws NullPointerException. The root's is
    never read (parquet-java builds the message type from its name alone),
    and some writers leave it out.

    pyarrow reads an element with children as a group, whatever physical
    type it also carries; parquet-java reads any element with a type as a
    primitive, and then cannot place its children ("Arrived at primitive
    node"), or an annotation only a group takes ("LIST can not be applied
    to a primitive type"). A leaf, one with no children (num_children 0
    included), is a leaf to both.
    """
    for index, element in enumerate(elements[1:], 1):
        name = element.get(_NAME)
        repetition = element.get(_REPETITION)
        if repetition not in (_REQUIRED, _OPTIONAL, _REPEATED):
            spelled = (
                "no repetition_type"
                if repetition is None
                else f"repetition_type {repetition!r}"
            )
            return (
                f"schema element {index} ({name!r}) has {spelled}, which "
                "parquet-java (the server's hydrator) and the Trino connector "
                "cannot read a footer with"
            )
        if _TYPE in element and element.get(_CHILDREN, 0) > 0:
            return (
                f"schema element {index} ({name!r}) is a group with a physical "
                "type, which parquet-java (the server's hydrator) and the Trino "
                "connector read as a primitive, and cannot read a footer with"
            )
        fault = _annotation_fault(element)
        if fault is not None:
            return f"schema element {index} ({name!r}) {fault}"
    return None


#: parquet.thrift's ConvertedType names, by value.
_CONVERTED_NAMES = (
    "UTF8",
    "MAP",
    "MAP_KEY_VALUE",
    "LIST",
    "ENUM",
    "DECIMAL",
    "DATE",
    "TIME_MILLIS",
    "TIME_MICROS",
    "TIMESTAMP_MILLIS",
    "TIMESTAMP_MICROS",
    "UINT_8",
    "UINT_16",
    "UINT_32",
    "UINT_64",
    "INT_8",
    "INT_16",
    "INT_32",
    "INT_64",
    "JSON",
    "BSON",
    "INTERVAL",
)


def _annotation_fault(element: dict[int, Any]) -> str | None:
    """What of an element's annotation pyarrow and parquet-java read apart,
    or parquet-java cannot build, as the rest of a sentence about it, or
    None. pyarrow reads the logical type when there is one, and the
    converted type only when there is not; parquet-java (the server's
    hydrator), and the connector's ``ParquetMetadata`` after it, read the
    annotation :func:`_effective` gives and build it with parquet-java's
    ``Types`` builder, which throws on one that does not fit the element.
    So an element is refused:

    - whose logical type sets no member parquet.thrift defines (one spelled
      with another wire type, which the generated readers skip, or one it
      does not declare): pyarrow reads no annotation, and the connector's
      ``getLogicalTypeAnnotation`` switches over the member set, with no
      case for none, and throws NullPointerException; or whose TIME,
      TIMESTAMP or INTEGER lacks a field Thrift requires, or has a width
      parquet-java refuses (:data:`_MALFORMED`);
    - whose logical and converted types spell different types (parquet-java's
      ``toOriginalType``): the converted one wins there, so a timestamp in
      MICROS beside a TIMESTAMP_MILLIS is read off by a thousand, and a
      string beside a DATE is unreadable;
    - whose DECIMAL logical type its own scale or precision field
      contradicts ("Decimal scale should match with the scale of the
      logical type");
    - whose annotation parquet-java does not build on its physical type
      (:func:`_builds`). A group takes any.

    The rules are pinned against parquet-java itself by a vector file the
    server's suite reads too (``tests/vectors/schema_annotation_vectors
    .json``).
    """
    logical = element.get(_LOGICAL)
    converted = _converted(element)
    if logical is not None:
        if not logical or next(iter(logical)) not in _LOGICAL_TYPE:
            return (
                "has a logical type of no member parquet.thrift defines, which "
                "pyarrow reads as none, and the Trino connector cannot read a "
                "footer with"
            )
        original = _original(logical)
        if original == _MALFORMED:
            return (
                f"has a {_logical_name(logical)} logical type Thrift or "
                "parquet-java cannot read, which parquet-java (the server's "
                "hydrator) and the Trino connector cannot read a footer with"
            )
        if converted is not None and original != converted:
            return (
                f"has the logical type {_logical_name(logical)} beside the "
                f"converted type {_CONVERTED_NAMES[converted]}, which spell "
                "different types: pyarrow reads the first, and parquet-java "
                "(the server's hydrator) and the Trino connector the second"
            )
        ((kind, detail),) = logical.items()
        if (
            kind == 5
            and _TYPE in element
            and (
                element.get(_SCALE, detail.get(1)) != detail.get(1)
                or element.get(_PRECISION, detail.get(2)) != detail.get(2)
            )
        ):
            return (
                "has a DECIMAL logical type its scale or precision field "
                "contradicts, which parquet-java (the server's hydrator) and "
                "the Trino connector cannot read a footer with"
            )
    annotation = _effective(element)
    if _TYPE in element and annotation is not None and not _builds(annotation, element):
        named = _logical_name(annotation)
        if logical is None and converted is not None and converted != 5:
            named = _CONVERTED_NAMES[converted]  # MAP_KEY_VALUE, say: no MAP
        physical = element[_TYPE]
        spelled = _PHYSICAL_TEXT.get(physical, repr(physical))
        if physical == _FIXED:
            spelled += f"({element.get(_LENGTH)})"
        return (
            f"is annotated {named}, which parquet-java (the server's hydrator) "
            f"and the Trino connector cannot apply to {spelled}, and cannot "
            "read a footer with"
        )
    return None


def _logical_name(annotation: dict[int, Any]) -> str:
    """An annotation as a message names it: its member's struct name, a
    DECIMAL with its precision and scale."""
    ((kind, detail),) = annotation.items()
    if kind == 5:
        return f"DECIMAL({detail.get(2)}, {detail.get(1)})"
    declared = _LOGICAL_TYPE.get(kind)
    return "INTERVAL" if declared is None else declared[1].name


#: A physical type as messages name it, a FIXED_LEN_BYTE_ARRAY's length
#: aside.
_PHYSICAL_TEXT = {value: name for name, value in _PHYSICAL_NAMES.items()}


def _builds(annotation: dict[int, Any], element: dict[int, Any]) -> bool:
    """Whether parquet-java's ``Types.PrimitiveBuilder`` (parquet-column
    1.18.1, the server's and the connector's) builds the primitive
    ``element`` with ``annotation``, one :func:`_effective` gives: a string
    type on a BYTE_ARRAY, a DATE on an INT32, an INTEGER of 8, 16 or 32
    bits on an INT32 and of 64 on an INT64, a TIME in MILLIS on an INT32 and
    finer on an INT64, a TIMESTAMP on an INT64, a UUID, FLOAT16 and INTERVAL
    on a fixed-length array of 16, 2 and 12 bytes, and a DECIMAL whose
    precision the type holds, with a scale from 0 to it. No group type
    (LIST, MAP, VARIANT) is a primitive's; UNKNOWN fits any."""
    physical, length = element[_TYPE], element.get(_LENGTH)
    ((kind, detail),) = annotation.items()
    if kind in (1, 4, 12, 13, 17, 18):  # STRING ENUM JSON BSON GEOMETRY GEOGRAPHY
        return bool(physical == _BYTE_ARRAY)
    if kind == 5:
        precision, scale = detail.get(2), detail.get(1)
        widest: float | None = {_INT32: 9, _INT64: 18, _BYTE_ARRAY: math.inf}.get(
            physical
        )
        if physical == _FIXED:
            widest = _fixed_digits(length)
        return (
            widest is not None
            and isinstance(precision, int)
            and isinstance(scale, int)
            and 0 < precision <= widest
            and 0 <= scale <= precision
        )
    if kind == 6:  # DATE
        return bool(physical == _INT32)
    if kind == 7:  # TIME: MILLIS (1) in an INT32, MICROS and NANOS in an INT64
        return bool(physical == (_INT32 if 1 in detail[2] else _INT64))
    if kind == 8:  # TIMESTAMP
        return bool(physical == _INT64)
    if kind == 10:  # INTEGER
        return bool(
            physical == {8: _INT32, 16: _INT32, 32: _INT32, 64: _INT64}.get(detail[1])
        )
    if kind == 11:  # UNKNOWN, which the connector reads as no annotation
        return True
    fixed = {14: 16, 15: 2, -1: 12}.get(kind)  # UUID, FLOAT16, INTERVAL
    return fixed is not None and physical == _FIXED and length == fixed


def _utf8(path: list[bytes] | None) -> bool:
    """Whether a ColumnMetaData path_in_schema decodes as UTF-8, every name
    of it, as pyarrow's ``path_in_schema`` does (raising UnicodeDecodeError
    when it does not)."""
    try:
        for name in path or ():
            name.decode()
    except UnicodeDecodeError:
        return False
    return True


def _size_statistics_fault(
    statistics: dict[int, Any] | None,
    physical: int | None,
    definition: int,
    repetition: int,
) -> str | None:
    """What of a chunk's SizeStatistics (ColumnMetaData field 16) pyarrow
    throws on, or None. An ``unencoded_byte_array_data_bytes`` of 0 is
    one: pyarrow asks whether the field is set, not what it holds."""
    if statistics is None:
        return None
    if statistics.get(1) is not None and physical != _BYTE_ARRAY:
        return "has unencoded byte array data bytes, but is not BYTE_ARRAY"
    for field, name, levels in (
        (2, "repetition", repetition),
        (3, "definition", definition),
    ):
        entries = statistics.get(field)  # the histogram's length
        if entries and entries != levels + 1:
            return (
                f"has a {entries}-entry {name} level histogram, "
                f"for a maximum level of {levels}"
            )
    return None


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
    any writer on a pyarrow below 21) carries the bare fixed(16). The
    bytes are identical, so both are accepted, at any nesting depth
    (:func:`~pyhoglake.types.uuid_storage_form`).

    Nothing else is loosened: the field ids still have to match, so a
    file that annotates its uuid column but misnumbers it is refused
    exactly as before.
    """
    return uuid_storage_form_schema(found).equals(
        uuid_storage_form_schema(want), check_metadata=True
    )


#: What reading a footer this module did not write can raise: it can spell
#: anything Thrift can, such as a type nibble Thrift does not know, a
#: struct that repeats a field, or a declared list of another item type.
_FOOTER_ERRORS = (
    ValueError,
    EOFError,
    IndexError,
    KeyError,
    TypeError,
    struct.error,
    TException,
)


def _footer_error(error: BaseException) -> str:
    """What a footer read raised, as a reason: Thrift's transport raises a
    bare EOFError when a value runs past the footer's last byte (a binary
    whose size, a varint wider than the i32 Thrift's generated readers
    take it as, Python's Thrift reads whole, say)."""
    if isinstance(error, EOFError):
        return "a value runs past the end of the Parquet footer"
    return str(error)


def validate_column_chunks(source: Source, parquet: Any) -> None:
    """Refuse, with ValidationError, a Parquet file whose column chunks
    pyarrow aborts the process on when their metadata or statistics are
    read, or whose paths are not their leaves' (:class:`_ChunkCheck`); or
    whose schema parquet-java (the server's hydrator) or the Trino connector
    cannot read, or reads as another type than pyarrow does, for a missing
    repetition_type, a group with a physical type or an annotation the two
    read apart (:func:`_schema_fault`), or a column order parquet-java
    cannot build (:meth:`_ChunkCheck.order_fault`); or whose footer Thrift's
    generated readers read differently, or refuse (:func:`_file_metadata`).

    ``source`` is the file's path, whose footer alone is read, or the whole
    file in memory; ``parquet`` is the ``pq.ParquetFile`` open on it, whose
    reading of the schema the chunks are held to. :func:`validate_variant_file`
    runs the same check; this is the one for a prepared file it does not
    see, before ``extract_column_stats`` reads every chunk.
    """
    try:
        _, schema_fault, fault = _footer_faults(source, parquet)
    except _FOOTER_ERRORS as error:
        schema_fault = _footer_error(error)
    if schema_fault is not None:
        raise ValidationError(
            f"invalid prepared Parquet schema: {schema_fault}", status_code=None
        )
    if fault is not None:
        raise ValidationError(
            f"invalid prepared Parquet column chunk: {fault}", status_code=None
        )


def validate_variant_file(
    source: Source,
    parquet: Any,
    columns: tuple[Column, ...],
    *,
    strict: bool = False,
) -> None:
    """Validate native VARIANT or opt-in external files using physical schema and counts.

    ``source`` is the file's path, or the whole file in memory; ``parquet``
    is the ``pq.ParquetFile`` open on it. The footer is read as Thrift's
    generated readers read it (:func:`_file_metadata`), and every column
    chunk is checked first, from the footer's bytes, for metadata and
    statistics pyarrow can read (:class:`_ChunkCheck`): reading them
    aborts the process otherwise, here and in ``extract_column_stats``
    after this. That check also holds the schema read here to pyarrow's,
    so the checks below read the schema pyarrow does, and each chunk's path
    to its leaf's. A schema element without a repetition_type, which
    pyarrow reads as REQUIRED, a group with a physical type, which pyarrow
    reads as a group, and an annotation pyarrow and parquet-java read apart
    (a converted type that wins over the logical one there, one parquet-java
    cannot build) are refused before it, with every other footer
    :func:`validate_column_chunks` refuses: parquet-java and the connector
    read those as another type, or cannot read them at all
    (:func:`_schema_fault`).

    ``strict`` adds two checks of each variant column, which the default
    leaves out because a foreign file the Trino connector cannot read is
    still the server's to accept (it never opens files): its layout must be
    one the spec and the connector accept, and the column's declaration
    when it has one (:func:`variant_layout_fault`, ``mode="spec"``), and a
    NOT NULL column in a file DuckDB wrote must prove it holds no SQL NULL
    (:func:`_duckdb_sql_null_fault`).
    """

    def fail(message: str) -> None:
        raise ValidationError(message, status_code=None)

    try:
        elements, schema_fault, chunk_fault = _footer_faults(source, parquet)
        if schema_fault is None:
            physical = _top_level(elements)
    except _FOOTER_ERRORS as error:
        schema_fault = _footer_error(error)
    if schema_fault is not None:
        fail(f"invalid prepared Parquet schema: {schema_fault}")
        return
    if chunk_fault is not None:
        fail(f"invalid prepared Parquet column chunk: {chunk_fault}")
    arrow = parquet.schema_arrow
    if arrow.names != [c.name for c in columns] or len(physical) != len(columns):
        fail("prepared Parquet columns differ from destination")
    for column, field, (element, leaves, index) in zip(
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
            # The annotation as the server's hydrator reads it, through
            # parquet-java: an absent version is 0, not 1, which
            # FooterStats.variantFault warns of as "not a native parquet
            # VARIANT of spec version 1", which the openapi's VARIANT(1)
            # rule requires; pyarrow shows Variant(1). (A converted type
            # beside it, which wins there, a MAP say, and a physical type,
            # are refused above, as on any element: _schema_fault.)
            annotation = element.get(_LOGICAL) or {}
            if (
                set(annotation) != {16}
                or annotation[16].get(1) != 1
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
            if strict:
                fault = variant_layout_fault(
                    elements, index, column, _declared_plan(column), mode="spec"
                ) or _duckdb_sql_null_fault(parquet, column, field, leaves)
                if fault is not None:
                    fail(f"{column.name} fails the strict VARIANT check: {fault}")
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
        # Any other would be stamped as a decimal8 (_annotation), typo or not.
        if self.kind not in ("variant", "decimal4", "decimal8"):
            raise ValueError(f"{_where(self.path)}: no stamp of kind {self.kind!r}")


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

    Each precondition refuses loudly, with ValueError (as a FooterStamp of
    another kind does when it is made): a footer that is not plaintext
    ``PAR1``; a path that is not in the footer exactly once, or
    stamped twice; a group with a type, an annotation, a field id other
    than the stamp's, or a field after the field id; a leaf of another
    physical type, or with an annotation, a scale, a precision, a field id
    or any field after ``num_children``. A pyarrow that started writing any
    of these would stop pyhoglake's writes rather than have it publish a
    layout nobody checked.
    """
    start, tail, elements = _splice(raw, stamps)
    view = memoryview(raw).cast("B")
    return b"".join((view[:start], tail)), elements


def _splice(
    raw: bytes | bytearray | memoryview | pa.Buffer, stamps: Sequence[FooterStamp]
) -> tuple[int, bytes, list[dict[int, Any]]]:
    """:func:`stamp_variant_footer`, as the offset the footer starts at,
    the new footer and trailer that replace everything from there, and the
    new footer's schema elements."""
    start, footer = _footer_bytes(raw)
    elements = _schema_elements_from_footer(footer, positions=True)
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
    after = [
        element.fields
        for element in _schema_elements_from_footer(patched, positions=True)
    ]
    # The splice encodes each field id as a delta from the element's last:
    # one that read back as another field (a wrong last id) would land here.
    if len(after) != len(elements) or any(
        element != _stamped(before.fields, stamped.get(before.path))
        for element, before in zip(after, elements, strict=True)
    ):
        raise ValueError("stamped footer does not read back as stamped")
    return start, patched + struct.pack("<I", len(patched)) + b"PAR1", after


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
    decimal = {1: scale, 2: precision}
    return {
        **fields,
        _CONVERTED: 5,
        _SCALE: scale,
        _PRECISION: precision,
        _LOGICAL: {5: decimal},
    }


def _where(path: Sequence[str]) -> str:
    """A path of Parquet names in a message: the column's name, then each
    name below it as :func:`member` spells a key, so ``a.b`` is one name."""
    return path[0] + "".join(member(name) for name in path[1:]) if path else "the root"


@dataclass(frozen=True)
class VariantParquet:
    """A Parquet file :func:`write_variant_parquet` wrote, stamped and
    checked: the bytes to upload, and what the registration reads off
    them, all measured AFTER the footer was patched."""

    body: bytes
    #: The footer's length from the trailer: the wire's ``footer_size``.
    footer_size: int
    #: pyarrow's FileMetaData of ``body``, which the stats come from.
    metadata: Any

    @property
    def size(self) -> int:
        return len(self.body)


def _child_with_field_id(kind: pa.DataType, path: str) -> str | None:
    """The path of the first field below ``kind`` (at ``path``) that
    carries a ``PARQUET:field_id``, or None."""
    for child in _arrow_children(kind):
        here = f"{path}.{child.name}"
        if PARQUET_FIELD_ID_KEY in (child.metadata or {}):
            return here
        deeper = _child_with_field_id(child.type, here)
        if deeper is not None:
            return deeper
    return None


def write_variant_parquet(
    table: pa.Table,
    columns: Sequence[Column],
    plans: Mapping[str, Plan],
    *,
    row_group_size: int | None = None,
) -> VariantParquet:
    """Write ``table``, whose variant columns are in the write form of
    their plans, as the Parquet file of their declarations.

    ``plans`` maps each variant column's name to its plan, the compiled
    ``type_params.shredding`` of that column in ``columns`` (another
    declaration's is refused, with ValueError), and every variant column of
    ``table`` must have one; ``columns`` are the catalog's, and ``table``
    carries each planned column with its nullability and its field id, and
    no field id below it (another is refused, with ValueError). Each step is
    what the file must be for the connector:

    1. Each decimal4/decimal8 leaf is viewed (zero-copy) as the int32/int64
       its decimal32/decimal64 already holds: pyarrow 23 writes a decimal32
       or decimal64 as a fixed-length array, and the file-wide
       ``store_decimal_as_integer`` would also change a decimal16 leaf and
       any top-level decimal column (D15).
    2. ``pq.write_table(store_schema=False)``: a stored Arrow schema would
       describe the group as the plain struct, with integer leaves, and
       pyarrow reads that back instead of the footer.
    3. The footer is stamped (:func:`stamp_variant_footer`).
    4. pyarrow re-reads the patched bytes: each variant column must read
       back as its plan's read form, a plain struct (a pyarrow that surfaced
       an extension type would stop here), and its subtree must be the
       declaration's, element for element (``variant_layout_fault``,
       ``mode="exact"``).

    A failure at 3 or 4 is pyhoglake's, or a pyarrow it does not know, and
    raises ValidationError before anything is uploaded.
    """
    by_name = {column.name: column for column in columns}
    if not plans:
        raise ValueError("no variant columns to write; use pq.write_table")
    if set(plans) - set(table.column_names):
        raise ValueError("a planned variant column is not in the table")
    stored = []
    for field in table.schema:
        plan = plans.get(field.name)
        column = by_name.get(field.name)
        if plan is None:
            if column is not None and column.type == "variant":
                # Written as it stands, it would be a plain struct with no
                # annotation, which the read-back check below cannot see
                # (an undeclared column's write form IS its read form).
                raise ValueError(f"variant column {field.name} has no plan")
            stored.append((field, table.column(field.name)))
            continue
        if column is None or column.type != "variant":
            raise ValueError(f"{field.name} is not a variant column of the table")
        # Both would go out, and fail the read-back below as pyhoglake's
        # fault, when they are the caller's.
        if field.nullable != column.nullable:
            raise ValueError(
                f"{field.name} is {'' if field.nullable else 'not '}nullable in the "
                f"table, and {'' if column.nullable else 'not '}in the catalog"
            )
        field_id = (field.metadata or {}).get(PARQUET_FIELD_ID_KEY)
        if field_id != str(column.field_id).encode():
            raise ValueError(
                f"{field.name} has field id {field_id!r} in the table, "
                f"not the column's {column.field_id}"
            )
        # Arrow's type equality ignores a child's metadata, so the form
        # check below would let pyarrow write ids the layout has none of.
        below = _child_with_field_id(field.type, field.name)
        if below is not None:
            raise ValueError(
                f"{below} has a field id in the table; below a variant column "
                "there are none"
            )
        # The layout check below holds the file to the plan, so a plan of
        # another declaration would pass it, in a layout the column's own
        # strict check refuses.
        declared = _declared_plan(column)
        if plan.expected_elements(column) != declared.expected_elements(column):
            raise ValueError(f"the plan of {field.name} is not its declaration's")
        if plan.form_of(field.type) != "write":
            raise ValueError(f"{field.name} is not in its plan's write form")
        kind = _integer_view(field.type)
        data = table.column(field.name)
        stored.append(
            (
                field.with_type(kind),
                pa.chunked_array([chunk.view(kind) for chunk in data.chunks], kind),
            )
        )
    schema = pa.schema([field for field, _ in stored], metadata=table.schema.metadata)
    sink = pa.BufferOutputStream()
    options: dict[str, Any] = {"store_schema": False}
    if row_group_size is not None:
        options["row_group_size"] = row_group_size
    pq.write_table(
        pa.table([data for _, data in stored], schema=schema), sink, **options
    )
    stamps = [
        stamp
        for field in table.schema
        if field.name in plans
        for stamp in variant_stamps(by_name[field.name], plans[field.name])
    ]
    try:
        body, elements = stamp_variant_footer(sink.getvalue(), stamps)
    except ValueError as error:
        raise ValidationError(
            f"cannot annotate VARIANT footer: {error}", status_code=None
        ) from error
    with pq.ParquetFile(pa.BufferReader(body)) as parquet:
        metadata = parquet.metadata
        read = parquet.schema_arrow
    starts = {element.get(_NAME): index for element, _, index in _top_level(elements)}
    for name, plan in plans.items():
        column = by_name[name]
        if read.field(name).type != plan.read_type:
            raise ValidationError(
                f"pyhoglake wrote VARIANT column {name}, but pyarrow "
                f"{pa.__version__} reads it back as {read.field(name).type}, not "
                "its plan's read form",
                status_code=None,
            )
        fault = variant_layout_fault(elements, starts[name], column, plan, mode="exact")
        if fault is not None:
            raise ValidationError(
                f"pyhoglake wrote VARIANT column {name} in a layout that is not its "
                f"declaration's: {fault}",
                status_code=None,
            )
    return VariantParquet(body, _footer_size(body), metadata)


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


# -- VARIANT layouts -------------------------------------------------------------


class _LayoutFault(Exception):
    pass


def variant_layout_fault(
    elements: list[dict[int, Any]],
    start: int,
    column: Column,
    plan: Plan | None,
    *,
    mode: Literal["exact", "spec"] = "exact",
) -> str | None:
    """What is wrong with the VARIANT layout of ``column``, whose group is
    ``elements[start]``, or None.

    ``plan`` is the column's compiled declaration; None, or a plan of no
    declaration, is an undeclared column. Below the group neither mode
    allows a field id: hoglake binds a file's columns by field id, and the
    variant's are the group's alone.

    ``exact`` is for pyhoglake's own files: the subtree must be
    :meth:`Plan.expected_elements`, element for element (names, order,
    repetition, child counts, physical type and length, logical type with
    its flags, scale and precision), but that a converted type, the legacy
    spelling of the logical type beside it, may be absent.

    ``spec`` is for a file another writer produced: its layout must be one
    the spec allows and the Trino connector reads (``VariantShreddingSchema
    .fromParquet``, and ``HoglakeParquetFields`` for an unshredded group),
    and a declared column's must be the declaration's, fields compared as
    sets by name and type:

    - the group has a REQUIRED binary ``metadata``. Unshredded it has a
      REQUIRED binary ``value`` and nothing else, the only unshredded shape
      the connector reads; shredded, an OPTIONAL binary ``value`` and an
      OPTIONAL ``typed_value`` (VS:65; the ``value`` is required, as the
      parquet-testing vectors read the spec);
    - an object's field groups are REQUIRED (VS:166, 193) and each has an
      OPTIONAL binary ``value``, and their names differ, case aside (the
      connector finds columns by lowercase name);
    - an array is a 3-level list (a LIST group, a repeated group, a
      REQUIRED element group, VS:117-122) whose element has a ``value``,
      a ``typed_value`` or both, and whose repeated group is named neither
      ``array`` nor ``typed_value_tuple`` in any case (the connector
      lowercases names, and reads those as a 2-level list);
    - a leaf is an element with a physical type, as the connector reads
      one, even with num_children 0 (one that states children is refused),
      and a group one without; its annotation is the one the connector
      reads (:func:`_effective`: its logical type, unless a converted type
      beside it spells another, which then wins, or the converted type
      alone; one Thrift or parquet-java cannot read is refused), and it is
      one of the spec's table (VS:83-105): so an unsigned integer,
      MILLIS, TIME adjusted to UTC, INT96 and a fixed-length array that is
      neither a UUID nor a DECIMAL are refused. A DECIMAL is decimal4,
      decimal8 or decimal16 as its physical type is INT32, INT64 or a byte
      array, so a declared decimal4 written as a fixed-length array is not
      the declaration's, and it must be one parquet-java builds (a
      precision above 0 that the physical type holds, a scale from 0 to
      it);
    - nothing nests deeper than MAX_VARIANT_DEPTH levels.

    A fault is returned, never raised, for anything a footer can spell.
    """
    if mode not in ("exact", "spec"):
        raise ValueError(f"mode is 'exact' or 'spec', not {mode!r}")
    try:
        end = _subtree_end(elements, start)
        group = elements[start]
        path = (column.name,)
        if group.get(_NAME) != column.name:
            raise _LayoutFault(f"the group is named {group.get(_NAME)!r}")
        for element in elements[start + 1 : end]:
            if _FIELD_ID in element:
                raise _LayoutFault(
                    f"{element.get(_NAME)!r} below the group has a field id"
                )
        if mode == "exact":
            _exact(elements, start, end, column, plan or UNSHREDDED_PLAN)
        else:
            if group.get(_FIELD_ID) != column.field_id:
                raise _LayoutFault(
                    f"the group has field id {group.get(_FIELD_ID)}, "
                    f"not {column.field_id}"
                )
            annotation = _effective(group) or {}
            if (
                set(annotation) != {16}
                or annotation[16].get(1) != 1
                or group.get(_REPETITION) not in (_REQUIRED, _OPTIONAL)
                or _TYPE in group
            ):
                raise _LayoutFault("the group is not a VARIANT(1) group")
            found = _SpecWalk(elements).variant_group(start, path)
            if plan is not None and plan.shredding is not None:
                difference = _shape_difference(found, _declared(plan.root), path)
                if difference is not None:
                    raise _LayoutFault(difference)
    except _LayoutFault as fault:
        return str(fault)
    except (
        ValueError,
        IndexError,
        KeyError,
        AttributeError,
        TypeError,
        RecursionError,
    ) as error:
        # A foreign footer can hold anything Thrift can spell: a union
        # member that is not a struct, a count that is not a number. The
        # walk's depth bound keeps it inside the stack, but a caller already
        # deep in one is a fault here too, not an escape.
        return f"invalid schema below {_where((column.name,))}: {error!r}"
    return None


def _subtree_end(elements: list[dict[int, Any]], start: int) -> int:
    """The index after the last element of the subtree at ``start``."""
    cursor, pending = start, 1
    while pending:
        if cursor >= len(elements):
            raise ValueError("truncated Parquet schema")
        children = elements[cursor].get(_CHILDREN, 0)
        if not isinstance(children, int) or children < 0:
            raise ValueError("negative schema child count")
        pending += children - 1
        cursor += 1
    return cursor


def _exact(
    elements: list[dict[int, Any]], start: int, end: int, column: Column, plan: Plan
) -> None:
    # Every element's child count is compared, so a subtree whose elements
    # all match has the expected extent too.
    for offset, (path, want) in enumerate(plan.expected_elements(column)):
        index = start + offset
        found = elements[index] if index < end else None
        if found is None or not _same_element(found, want):
            raise _LayoutFault(
                f"{_where(path)} is {found!r}, not the declaration's {want!r}"
            )


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


#: The logical type each converted type stands for, where the logical type
#: is absent: parquet.thrift's backward-compatibility rules, as
#: parquet-java's reader (the connector's) applies them. A DECIMAL's comes
#: from the element's scale and precision.
_LEGACY: dict[int, dict[int, Any]] = {
    0: {1: {}},  # UTF8: STRING
    1: {2: {}},  # MAP
    2: {2: {}},  # MAP_KEY_VALUE
    3: {3: {}},  # LIST
    4: {4: {}},  # ENUM
    6: {6: {}},  # DATE
    7: {7: {1: True, 2: {1: {}}}},  # TIME_MILLIS
    8: {7: {1: True, 2: {2: {}}}},  # TIME_MICROS
    9: {8: {1: True, 2: {1: {}}}},  # TIMESTAMP_MILLIS
    10: {8: {1: True, 2: {2: {}}}},  # TIMESTAMP_MICROS
    11: {10: {1: 8, 2: False}},  # UINT_8
    12: {10: {1: 16, 2: False}},
    13: {10: {1: 32, 2: False}},
    14: {10: {1: 64, 2: False}},
    15: {10: {1: 8, 2: True}},  # INT_8
    16: {10: {1: 16, 2: True}},
    17: {10: {1: 32, 2: True}},
    18: {10: {1: 64, 2: True}},
    19: {12: {}},  # JSON
    20: {13: {}},  # BSON
}


def _converted(element: dict[int, Any]) -> int | None:
    """An element's converted_type as parquet-java reads it (the server's
    hydrator, and the connector's ``ParquetMetadata``): one of the values
    parquet.thrift's ConvertedType defines, 0 (UTF8) to 21 (INTERVAL), or
    None. Its ``ConvertedType.findByValue`` reads any other value as null,
    which is the field unset, so the logical type beside it stands."""
    converted = element.get(_CONVERTED)
    return converted if converted in range(22) else None


def _effective(element: dict[int, Any]) -> dict[int, Any] | None:
    """The annotation the connector reads off an element: its logical type,
    or the one its converted type stands for (an INTERVAL, which has none,
    as an unknown union member).

    With both, parquet-java (and the connector's ``ParquetMetadata``, which
    follows it) keeps the logical type only when the two spell the same
    converted type (``toOriginalType``), and otherwise reads the converted
    one: a logical INT(8, true) beside a converted UINT_8 is a UINT_8 to the
    connector, and a STRING beside an ENUM an ENUM.
    """
    converted = _converted(element)
    legacy = None
    if converted is not None:
        legacy = (
            {5: {1: element.get(_SCALE, 0), 2: element.get(_PRECISION)}}
            if converted == 5
            else _LEGACY.get(converted, {-1: {}})
        )
    if _LOGICAL not in element:
        return legacy
    logical: dict[int, Any] = element[_LOGICAL]
    # A union of no member, or of two, is malformed, and stays itself, as
    # does a member no reader parses.
    if legacy is not None and len(logical) == 1:
        original = _original(logical)
        if original != converted and original != _MALFORMED:
            return legacy
    return logical


#: The converted type each logical type spells where the spelling depends on
#: the union member alone (parquet-java's ``toOriginalType``).
_ORIGINAL: dict[int, int] = {1: 0, 2: 1, 3: 3, 4: 4, 5: 5, 6: 6, 12: 19, 13: 20}

#: What :func:`_original` answers for a logical type no reader parses: a
#: member parquet.thrift's union does not define (9, or 19 and up), which
#: Thrift's generated readers leave the union with no member set for; a
#: TIME, TIMESTAMP or INTEGER that lacks a required field (a bool
#: isAdjustedToUTC or isSigned, a one-member unit), which Thrift refuses;
#: or an INTEGER bitWidth parquet-java refuses (it takes 8, 16, 32 and
#: 64). The connector cannot read that footer at all, whatever converted
#: type is beside it (its getLogicalTypeAnnotation switches over the set
#: member, with no case for none), so the logical type stays, to be
#: refused. pyarrow reads an undefined member as no annotation, and the
#: server's parquet-java lets the converted type win. Every prepared-file
#: path refuses such an element (:func:`_annotation_fault`), before spec
#: mode reads it.
_MALFORMED = -1


def _original(logical: dict[int, Any]) -> int | None:
    """The converted type a one-member logical type spells, None for one
    that spells none (a UUID, a FLOAT16, VARIANT, a TIME or TIMESTAMP in
    NANOS), or :data:`_MALFORMED`."""
    ((kind, detail),) = logical.items()
    if kind not in _LOGICAL_TYPE:
        return _MALFORMED
    # Every member defined is declared a struct (_LOGICAL_TYPE), so the
    # footer reader skips one spelled as another Thrift type, as the
    # generated readers do, and ``detail`` is a struct's fields.
    if kind in _ORIGINAL:
        return _ORIGINAL[kind]
    if kind in (7, 8):  # TIME, TIMESTAMP: by unit alone (1 MILLIS, 2 MICROS)
        units = detail.get(2)
        if not isinstance(detail.get(1), bool) or not (
            isinstance(units, dict) and len(units) == 1
        ):
            return _MALFORMED
        spelt: dict[Any, int] = {(7, 1): 7, (7, 2): 8, (8, 1): 9, (8, 2): 10}
        return spelt.get((kind, next(iter(units))))
    if kind == 10:  # INTEGER: UINT_8..UINT_64 are 11..14, INT_8..INT_64 15..18
        widths: dict[Any, int] = {8: 0, 16: 1, 32: 2, 64: 3}
        width, signed = widths.get(detail.get(1)), detail.get(2)
        if width is None or not isinstance(signed, bool):
            return _MALFORMED
        return (15 if signed else 11) + width
    return None


#: A shredded value's type, compared with a declaration's: ``("variant",)``
#: for a value with no typed_value, ``("object", fields)`` with a frozenset
#: of (name, shape), ``("array", element)``, ``(decimal kind, precision,
#: scale)``, or ``(primitive kind,)``.
_Shape = tuple[Any, ...]


def _declared(node: Node | None) -> _Shape:
    if node is None or node.kind == "variant":
        return ("variant",)
    if node.kind == "object":
        return ("object", frozenset((n, _declared(child)) for n, child in node.fields))
    if node.kind == "array":
        return ("array", _declared(node.element))
    if node.kind in ("decimal4", "decimal8", "decimal16"):
        return (node.kind, node.precision, node.scale)
    return (node.kind,)


def _shape_difference(
    found: _Shape, declared: _Shape, path: tuple[str, ...]
) -> str | None:
    """Where ``found`` is not ``declared``, as a message, or None."""
    where = _where(path)
    if found[0] != declared[0] or found[0] not in ("object", "array"):
        if found == declared:
            return None
        return (
            f"{where} is {_shape_text(found)}, not the declared {_shape_text(declared)}"
        )
    if found[0] == "array":
        return _shape_difference(found[1], declared[1], (*path, "element"))
    have, want = dict(found[1]), dict(declared[1])
    missing, extra = (
        sorted(want.keys() - have.keys()),
        sorted(have.keys() - want.keys()),
    )
    if missing:
        return f"{where} has no field {missing[0]!r}, which is declared"
    if extra:
        return f"{where} has a field {extra[0]!r}, which is not declared"
    for name in sorted(want):
        difference = _shape_difference(have[name], want[name], (*path, name))
        if difference is not None:
            return difference
    return None


def _shape_text(shape: _Shape) -> str:
    if shape[0] == "variant":
        return "variant (no typed_value)"
    if len(shape) == 3:
        return f"{shape[0]}({shape[1]}, {shape[2]})"
    return str(shape[0])


class _SpecWalk:
    """The spec-mode reading of one VARIANT group (variant_layout_fault).

    ``depth`` is how deep a field group or an element nests: the variant's
    own value is 0, each object field and array element one more, as
    MAX_VARIANT_DEPTH counts a value's nesting. The walk recurses a few
    frames a level, so a foreign layout is refused past that depth rather
    than allowed to exhaust the interpreter's stack; no value pyhoglake
    encodes nests deeper.
    """

    def __init__(self, elements: list[dict[int, Any]]) -> None:
        self.elements = elements

    def children(self, index: int, path: tuple[str, ...]) -> dict[str, int]:
        """The children of the group at ``index``, by name."""
        out: dict[str, int] = {}
        cursor = index + 1
        for _ in range(self.elements[index].get(_CHILDREN, 0)):
            name = self.elements[cursor].get(_NAME)
            if not isinstance(name, str) or name in out:
                raise _LayoutFault(f"{_where(path)} has two children named {name!r}")
            out[name] = cursor
            cursor = _subtree_end(self.elements, cursor)
        return out

    def binary(self, index: int, path: tuple[str, ...], repetition: int) -> None:
        element = self.elements[index]
        if (
            element.get(_TYPE) != _BYTE_ARRAY
            or element.get(_CHILDREN)
            or element.get(_REPETITION) != repetition
        ):
            spelt = "a REQUIRED" if repetition == _REQUIRED else "an OPTIONAL"
            raise _LayoutFault(f"{_where(path)} is not {spelt} binary column")

    def variant_group(self, index: int, path: tuple[str, ...]) -> _Shape:
        kids = self.children(index, path)
        if "metadata" not in kids:
            raise _LayoutFault(f"{_where(path)} has no metadata")
        self.binary(kids["metadata"], (*path, "metadata"), _REQUIRED)
        if "typed_value" not in kids:
            if set(kids) != {"metadata", "value"}:
                raise _LayoutFault(
                    f"{_where(path)} is unshredded, and holds {sorted(kids)}, not "
                    "metadata and value"
                )
            # The connector reads an unshredded group only with both leaves
            # REQUIRED (HoglakeParquetFields.variantLeaf).
            self.binary(kids["value"], (*path, "value"), _REQUIRED)
            return ("variant",)
        return self.value_group(index, path, kids, 0, required_value=True)

    def value_group(
        self,
        index: int,
        path: tuple[str, ...],
        kids: dict[str, int],
        depth: int,
        *,
        required_value: bool,
    ) -> _Shape:
        """A group of ``value`` and ``typed_value``: the variant group,
        whose metadata is checked already, a field group or an element."""
        extra = set(kids) - {"metadata", "value", "typed_value"}
        if extra or ("metadata" in kids and len(path) > 1):
            raise _LayoutFault(f"{_where(path)} has children {sorted(kids)}")
        if "value" in kids:
            self.binary(kids["value"], (*path, "value"), _OPTIONAL)
        elif required_value or "typed_value" not in kids:
            raise _LayoutFault(f"{_where(path)} has no value")
        if "typed_value" not in kids:
            return ("variant",)
        return self.typed(kids["typed_value"], (*path, "typed_value"), depth)

    def typed(self, index: int, path: tuple[str, ...], depth: int) -> _Shape:
        element = self.elements[index]
        if element.get(_REPETITION) != _OPTIONAL:
            raise _LayoutFault(f"{_where(path)} is not OPTIONAL")
        annotation = _effective(element)
        # A leaf is an element with a physical type, as the connector reads
        # a footer (ParquetMetadata) and parquet.thrift has it, though a
        # writer may still set num_children 0 on one. A leaf with children
        # would leave them for the connector to read as its siblings.
        if _TYPE in element:
            if element.get(_CHILDREN):
                raise _LayoutFault(f"{_where(path)} is a group with a physical type")
            return _leaf_shape(element, annotation, path)
        if annotation == {3: {}}:
            return ("array", self.array(index, path, depth))
        if annotation is not None:
            raise _LayoutFault(f"{_where(path)} is a group annotated {annotation!r}")
        return self.object(index, path, depth)

    def object(self, index: int, path: tuple[str, ...], depth: int) -> _Shape:
        kids = self.children(index, path)
        if not kids:
            raise _LayoutFault(f"{_where(path)} has no fields")
        lowered: set[str] = set()
        fields = []
        for name, child in kids.items():
            here = (*path, name)
            if name.lower() in lowered:
                raise _LayoutFault(
                    f"{_where(here)} differs from another field only by case"
                )
            lowered.add(name.lower())
            fields.append((name, self.inner_group(child, here, depth + 1, field=True)))
        return ("object", frozenset(fields))

    def array(self, index: int, path: tuple[str, ...], depth: int) -> _Shape:
        kids = list(self.children(index, path).values())
        repeated = self.elements[kids[0]] if len(kids) == 1 else None
        if (
            repeated is None
            or repeated.get(_REPETITION) != _REPEATED
            or _TYPE in repeated
            or _effective(repeated) is not None
            # The connector lowercases every name it reads off a footer
            # (ParquetMetadata) before it looks for these, so "Array" is one.
            # The list's own name is typed_value, as found.
            or repeated[_NAME].lower() in ("array", f"{path[-1]}_tuple")
            or repeated.get(_CHILDREN) != 1
        ):
            # A 2-level list: the repeated field is itself the element.
            raise _LayoutFault(f"{_where(path)} is not a 3-level list")
        element = kids[0] + 1
        return self.inner_group(
            element,
            (*path, repeated[_NAME], self.elements[element][_NAME]),
            depth + 1,
            field=False,
        )

    def inner_group(
        self, index: int, path: tuple[str, ...], depth: int, *, field: bool
    ) -> _Shape:
        """A field group (``field``) or a list element: REQUIRED, holding a
        ``value``, which a field group must have, and a ``typed_value``."""
        if depth > MAX_VARIANT_DEPTH:
            # The path is a few names a level: its head says where.
            raise _LayoutFault(
                f"{_where(path[:9])}... is shredded more than "
                f"{MAX_VARIANT_DEPTH} levels deep"
            )
        element = self.elements[index]
        if _CHILDREN not in element or _TYPE in element:
            raise _LayoutFault(f"{_where(path)} is not a group")
        if element.get(_REPETITION) != _REQUIRED:
            raise _LayoutFault(f"{_where(path)} is not REQUIRED")
        return self.value_group(
            index, path, self.children(index, path), depth, required_value=field
        )


def _leaf_shape(
    element: dict[int, Any], annotation: dict[int, Any] | None, path: tuple[str, ...]
) -> _Shape:
    """The Variant type a typed_value leaf holds, as the connector's
    ``VariantShreddingSchema.primitiveValue`` maps it, or a fault."""
    physical = element.get(_TYPE)
    kind: int | None = None
    detail: dict[int, Any] = {}
    if annotation is not None and len(annotation) == 1:
        ((member_kind, member),) = annotation.items()
        if isinstance(member, dict):
            kind, detail = member_kind, member
    found: _Shape | None = None
    if annotation is not None and kind is None:
        # A union with no member, more than one, or one that is no struct,
        # which Thrift skips.
        found = None
    elif kind is None:
        plain: dict[Any, _Shape] = {
            _BOOLEAN: ("boolean",),
            _INT32: ("int32",),
            _INT64: ("int64",),
            _FLOAT: ("float",),
            _DOUBLE: ("double",),
            _BYTE_ARRAY: ("binary",),
        }
        found = plain.get(physical)
    elif kind == 5:  # DECIMAL: the physical type says which
        precision, scale = detail.get(2), detail.get(1, 0)
        widths: dict[Any, int] = {_INT32: 9, _INT64: 18, _BYTE_ARRAY: 38}
        widest = widths.get(physical)
        if physical == _FIXED:
            # Past 16 bytes parquet-java builds more digits, but the connector
            # reads no DECIMAL above 38 (VariantShreddingSchema.decimalValue).
            digits = _fixed_digits(element.get(_LENGTH))
            widest = None if digits is None else int(min(digits, 38))
        # parquet-java's schema builder, which the connector builds the
        # file's schema with, throws on any other: the file is unreadable.
        if (
            widest is not None
            and isinstance(precision, int)
            and isinstance(scale, int)
            and 0 <= scale <= precision
            and 0 < precision <= widest
            and element.get(_PRECISION, precision) == precision
            and element.get(_SCALE, scale) == scale
        ):
            name = "decimal4" if physical == _INT32 else "decimal16"
            name = "decimal8" if physical == _INT64 else name
            found = (name, precision, scale)
    elif kind == 10 and detail.get(2) is True:  # a signed INTEGER
        signed: dict[Any, _Shape] = {
            (_INT32, 8): ("int8",),
            (_INT32, 16): ("int16",),
            (_INT32, 32): ("int32",),
            (_INT64, 64): ("int64",),
        }
        found = signed.get((physical, detail.get(1)))
    elif kind == 6 and physical == _INT32:
        found = ("date",)
    elif kind in (7, 8) and physical == _INT64 and isinstance(detail.get(1), bool):
        # TimeUnit, a union of one: 1 MILLIS, 2 MICROS, 3 NANOS.
        units = detail.get(2)
        unit = (
            next(iter(units)) if isinstance(units, dict) and len(units) == 1 else None
        )
        utc = detail.get(1) is True
        if kind == 7 and unit == 2 and not utc:
            found = ("time",)
        elif kind == 8 and unit in (2, 3):
            found = (
                ("timestamptz" if utc else "timestamp") + ("_ns" if unit == 3 else ""),
            )
    elif kind == 1 and physical == _BYTE_ARRAY:
        found = ("string",)
    elif kind == 14 and physical == _FIXED and element.get(_LENGTH) == 16:
        found = ("uuid",)
    if found is None:
        raise _LayoutFault(
            f"{_where(path)} is physical type {physical} annotated {annotation!r}, "
            "which no Variant type is shredded as"
        )
    return found


def _fixed_digits(length: Any) -> float | None:
    """The most digits a DECIMAL in a fixed-length array of ``length`` bytes
    holds, as parquet-java's ``Types`` bounds it: the digits of ``2 ** (8 *
    length - 1) - 1``, less one. It computes that in doubles, whose power
    overflows from 129 bytes, and then takes any precision (measured)."""
    if not isinstance(length, int) or length < 1:
        return None
    if length > 128:
        return math.inf
    return len(str(2 ** (8 * length - 1) - 1)) - 1


def _declared_plan(column: Column) -> Plan:
    # The catalog's declaration, which the server accepted: one this client
    # cannot read raises UnsupportedShreddingError rather than be skipped.
    return _column_plan(column, (column.type_params or {}).get(SHREDDING_KEY))


def _duckdb_sql_null_fault(
    parquet: Any, column: Column, field: pa.Field, leaves: range
) -> str | None:
    """Whether a NOT NULL column of a file DuckDB wrote may hold SQL NULL.

    DuckDB writes a SQL NULL variant as a present group whose ``value`` is
    a Variant null (``00``), and both DuckDB and the connector (for a file
    whose created_by starts with DuckDB) read a top-level Variant null back
    as SQL NULL. So does the connector a row whose ``value`` and
    ``typed_value`` are both null, which it reads as a Variant null. The
    group is present, so the ``metadata`` proof finds no null, and each row
    group's statistics must prove there is neither, in one of two ways:

    - its ``value`` leaf has no null, and the first byte of its smallest
      value is above ``00`` (the connector reads the header byte alone, so
      ``00 01`` is a Variant null too);
    - its ``value`` leaf is all null, and its ``typed_value`` is a leaf
      with no null: every row is typed.

    A row group with nulls in some ``value`` rows only is refused. Its
    statistics count nulls, not rows: when ``value``'s and ``typed_value``'s
    add up to the rows, a row with neither side may be paired with one with
    both, which the connector reads (and returns, ahead of a row with both,
    which it fails on, since it reads a row at a time) as NULL. A
    ``typed_value`` group, whose leaves count the nulls below it too, and
    missing statistics prove nothing. Only under ``strict``: it also
    refuses a JSON null in a NOT NULL column, which those readers show as
    NULL, and a DuckDB file that shreds the column's values into a
    ``typed_value`` and leaves some in ``value``.
    """
    if column.nullable:
        return None
    try:
        created_by = (parquet.metadata.created_by or "").encode()
    except UnicodeDecodeError as error:
        # pyarrow decodes it as UTF-8, but a writer may put any bytes there,
        # and the connector reads them (startsWith("DuckDB")) all the same.
        created_by = bytes(error.object)
    if not created_by.startswith(b"DuckDB"):
        return None
    # Spec mode, which runs first, has found the group's value.
    first: dict[str, int] = {}
    offset = 0
    for child in field.type:
        first[child.name] = offset
        offset += _leaf_count(child.type)
    typed = field.type.get_field_index("typed_value")
    typed_leaf = None
    if typed >= 0 and not _arrow_children(field.type.field(typed).type):
        typed_leaf = leaves[first["typed_value"]]
    for group in range(parquet.metadata.num_row_groups):
        row_group = parquet.metadata.row_group(group)
        chunk = row_group.column(leaves[first["value"]])
        stats = chunk.statistics
        if stats is None or not stats.has_null_count:
            return f"row group {group} has no statistics for its value"
        if not chunk.num_values:
            continue  # an empty row group holds no row to prove
        if stats.null_count < chunk.num_values:
            # The connector reads a value's header byte alone: 00 is a
            # Variant null whatever bytes follow it, so the smallest value's
            # first byte is what must be above 00. A writer that truncates
            # its min keeps that byte.
            if not stats.has_min_max or not (
                isinstance(stats.min, bytes) and stats.min[:1] > b"\x00"
            ):
                return (
                    f"row group {group} may hold a Variant null as the whole "
                    "value, which DuckDB writes for SQL NULL"
                )
            if stats.null_count:
                return (
                    f"row group {group} has rows with a value and rows without, "
                    "and its statistics, which count nulls and not rows, cannot "
                    "prove that none has neither value nor typed_value, which "
                    "the connector reads as a Variant null"
                )
            continue
        typed_stats = (
            None if typed_leaf is None else row_group.column(typed_leaf).statistics
        )
        if typed_stats is None or not typed_stats.has_null_count:
            return (
                f"row group {group} has rows with no value, and no typed_value "
                "statistics to prove each has a typed_value"
            )
        if typed_stats.null_count:
            return (
                f"row group {group} may hold a row with neither value nor "
                "typed_value, which the connector reads as a Variant null"
            )
    return None
