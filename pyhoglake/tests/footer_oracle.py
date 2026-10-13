"""A generic compact-Thrift codec for Parquet footers: the test oracle of
``parquet_schema.stamp_variant_footer``. Tests only.

It reads a FileMetaData into a schema-less tree of ``(field id, type,
value)``, lets a test edit it, and writes it back. The production splice
never re-encodes anything, so the two share no code: a footer the splice
produces must equal, byte for byte, the one this codec writes after making
the same edit to the tree. It also forges what no writer at hand produces
(a ``created_by``, a footer without statistics, a field where pyarrow
writes none, a field of another wire type, a repeated field) for the
refusal tests.

Ported from the design probes' ``footer_patch.py``; a round trip of an
unedited footer is byte-identical (``test_variant_footer.
test_the_oracle_and_an_empty_splice_leave_foreign_footers_alone``), which
is what makes it an oracle.
"""

from __future__ import annotations

import struct
from collections.abc import Callable
from typing import Any

from thrift.protocol.TCompactProtocol import TCompactProtocol
from thrift.Thrift import TType
from thrift.transport.TTransport import TMemoryBuffer

#: A struct: its fields as [field id, Thrift type, value], in file order.
Tree = list[list[Any]]


def _read(protocol: TCompactProtocol, kind: int, depth: int = 0) -> Any:
    if depth > 64:
        raise ValueError("thrift nesting too deep")
    if kind == TType.STRUCT:
        fields = []
        protocol.readStructBegin()
        while True:
            _, field_kind, field = protocol.readFieldBegin()
            if field_kind == TType.STOP:
                break
            fields.append([field, field_kind, _read(protocol, field_kind, depth + 1)])
            protocol.readFieldEnd()
        protocol.readStructEnd()
        return fields
    if kind in (TType.LIST, TType.SET):
        begin = protocol.readListBegin if kind == TType.LIST else protocol.readSetBegin
        item_kind, count = begin()
        items = [_read(protocol, item_kind, depth + 1) for _ in range(count)]
        (protocol.readListEnd if kind == TType.LIST else protocol.readSetEnd)()
        return [item_kind, items]
    if kind == TType.MAP:
        key_kind, value_kind, count = protocol.readMapBegin()
        pairs = [
            (
                _read(protocol, key_kind, depth + 1),
                _read(protocol, value_kind, depth + 1),
            )
            for _ in range(count)
        ]
        protocol.readMapEnd()
        return [key_kind, value_kind, pairs]
    return {
        TType.BOOL: protocol.readBool,
        TType.BYTE: protocol.readByte,
        TType.I16: protocol.readI16,
        TType.I32: protocol.readI32,
        TType.I64: protocol.readI64,
        TType.DOUBLE: protocol.readDouble,
        # Bytes, never text: statistics hold raw bytes.
        TType.STRING: protocol.readBinary,
    }[kind]()


def _write(protocol: TCompactProtocol, kind: int, value: Any) -> None:
    if kind == TType.STRUCT:
        protocol.writeStructBegin(None)
        for field, field_kind, field_value in value:
            protocol.writeFieldBegin(None, field_kind, field)
            _write(protocol, field_kind, field_value)
            protocol.writeFieldEnd()
        protocol.writeFieldStop()
        protocol.writeStructEnd()
    elif kind in (TType.LIST, TType.SET):
        item_kind, items = value
        begin = (
            protocol.writeListBegin if kind == TType.LIST else protocol.writeSetBegin
        )
        begin(item_kind, len(items))
        for item in items:
            _write(protocol, item_kind, item)
        (protocol.writeListEnd if kind == TType.LIST else protocol.writeSetEnd)()
    elif kind == TType.MAP:
        key_kind, value_kind, pairs = value
        protocol.writeMapBegin(key_kind, value_kind, len(pairs))
        for key, item in pairs:
            _write(protocol, key_kind, key)
            _write(protocol, value_kind, item)
        protocol.writeMapEnd()
    else:
        {
            TType.BOOL: protocol.writeBool,
            TType.BYTE: protocol.writeByte,
            TType.I16: protocol.writeI16,
            TType.I32: protocol.writeI32,
            TType.I64: protocol.writeI64,
            TType.DOUBLE: protocol.writeDouble,
            TType.STRING: protocol.writeBinary,
        }[kind](value)


def decode(footer: bytes) -> Tree:
    buffer = TMemoryBuffer(footer)
    tree = _read(TCompactProtocol(buffer), TType.STRUCT)
    if buffer.cstringio_buf.tell() != len(footer):
        raise ValueError("bytes after the FileMetaData")
    return tree


def encode(tree: Tree) -> bytes:
    buffer = TMemoryBuffer()
    _write(TCompactProtocol(buffer), TType.STRUCT, tree)
    return buffer.getvalue()


class Spelled(bytes):
    """A value :func:`encode_unchecked` writes as these bytes, as they are:
    a list header whose size its items do not bear out, say."""


#: The compact protocol's type nibble of each Thrift type (BOOL is its
#: value's: 1 true, 2 false).
_COMPACT = {
    TType.BYTE: 3,
    TType.I16: 4,
    TType.I32: 5,
    TType.I64: 6,
    TType.DOUBLE: 7,
    TType.STRING: 8,
    TType.LIST: 9,
    TType.SET: 10,
    TType.MAP: 11,
    TType.STRUCT: 12,
}


def _varint(value: int) -> bytes:
    out = bytearray()
    while value > 0x7F:
        out.append(value & 0x7F | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def _zigzag(value: int) -> bytes:
    return _varint(value << 1 if value >= 0 else (-value << 1) - 1)


def _unchecked(kind: int, value: Any) -> bytes:
    if isinstance(value, Spelled):
        return bytes(value)
    if kind == TType.STRUCT:
        out, last = bytearray(), 0
        for field_id, field_kind, field_value in value:
            nibble = (
                (1 if field_value else 2)
                if field_kind == TType.BOOL
                else _COMPACT[field_kind]
            )
            # Thrift's writer's rule: a delta of 1 to 15 in the header,
            # otherwise the id after it; whatever the id, here.
            if 0 < field_id - last <= 15:
                out.append((field_id - last) << 4 | nibble)
            else:
                out += bytes([nibble]) + _zigzag(field_id)
            last = field_id
            if field_kind != TType.BOOL:
                out += _unchecked(field_kind, field_value)
        return bytes(out + b"\x00")
    if kind in (TType.LIST, TType.SET):
        item_kind, items = value
        nibble = 1 if item_kind == TType.BOOL else _COMPACT[item_kind]
        out = bytearray(
            [len(items) << 4 | nibble]
            if len(items) < 15
            else [0xF0 | nibble, *_varint(len(items))]
        )
        for item in items:
            out += (
                bytes([1 if item else 2])
                if item_kind == TType.BOOL
                else _unchecked(item_kind, item)
            )
        return bytes(out)
    if kind == TType.MAP:
        key_kind, value_kind, pairs = value
        if not pairs:
            return b"\x00"
        out = bytearray(_varint(len(pairs)))
        out.append(_COMPACT[key_kind] << 4 | _COMPACT[value_kind])
        for key, item in pairs:
            out += _unchecked(key_kind, key) + _unchecked(value_kind, item)
        return bytes(out)
    if kind == TType.BYTE:
        return struct.pack("b", value)
    if kind in (TType.I16, TType.I32, TType.I64):
        return _zigzag(value)
    if kind == TType.DOUBLE:
        return struct.pack("<d", value)
    if kind == TType.STRING:
        return _varint(len(value)) + value
    raise ValueError(f"no compact spelling of Thrift type {kind}")


def encode_unchecked(tree: Tree) -> bytes:
    """The compact encoding of ``tree``, written here rather than by
    Thrift's writer, which refuses to spell what this does: a field id
    outside an i16, an integer wider than its type, a :class:`Spelled`
    value. Python's Thrift reads either whole, where Thrift's generated
    readers keep the low bits; that difference is what such a footer tests.
    Of a tree Thrift's writer can spell, it writes the same bytes."""
    return _unchecked(TType.STRUCT, tree)


def long_form(fields: Tree) -> bytes:
    """A struct's fields, compact-encoded with every field id in long form
    (the type nibble, then the id) and no STOP: bytes that read as the same
    fields wherever they sit, whatever field was read before them."""
    out = bytearray()
    for field_id, kind, value in fields:
        if kind == TType.BOOL:
            out += bytes([1 if value else 2]) + _zigzag(field_id)
        else:
            out += bytes([_COMPACT[kind]]) + _zigzag(field_id)
            out += _unchecked(kind, value)
    return bytes(out)


def hide_in_encodings(raw: bytes, leaf: int, hidden: Tree) -> bytes:
    """``raw`` with ColumnMetaData fields ``hidden`` inside the encodings
    list of leaf column ``leaf``'s ColumnMetaData, in every row group, in
    place of any field of the same id there: the list's header says it
    holds one binary, whose bytes are ``hidden``. Python's Thrift follows
    the header and reads a binary; Thrift's generated readers read the item
    as the declared i32 (the binary's size), and then ``hidden`` as fields
    of the ColumnMetaData. Every field is spelled in long form, so both
    read the fields after it alike."""
    replaced = {field_id for field_id, _, _ in hidden}
    inner = long_form(hidden)
    header = bytes([1 << 4 | _COMPACT[TType.STRING]])
    encodings = Spelled(header + _varint(len(inner)) + inner)

    def edit(tree: Tree) -> None:
        for row_group in field(tree, 4)[1]:
            chunk = field(row_group, 1)[1][leaf]
            meta = [
                [2, TType.LIST, encodings] if entry[0] == 2 else entry
                for entry in field(chunk, 3)
                if entry[0] not in replaced
            ]
            put(chunk, 3, TType.STRUCT, Spelled(long_form(meta) + b"\x00"))

    return rebuild(raw, edit, encoder=encode_unchecked)


def split(raw: bytes) -> tuple[int, bytes]:
    """The footer's offset, and the footer."""
    if raw[-4:] != b"PAR1":
        raise ValueError("not a plaintext Parquet file")
    (length,) = struct.unpack("<I", raw[-8:-4])
    start = len(raw) - 8 - length
    return start, raw[start:-8]


def rebuild(
    raw: bytes,
    edit: Callable[[Tree], None],
    *,
    encoder: Callable[[Tree], bytes] = encode,
) -> bytes:
    """``raw`` with its footer decoded, edited in place and re-encoded, by
    ``encoder``: :func:`encode_unchecked` for what Thrift cannot spell."""
    start, footer = split(raw)
    tree = decode(footer)
    if encoder(tree) != footer:
        raise ValueError("the footer does not round-trip byte for byte")
    edit(tree)
    new = encoder(tree)
    return raw[:start] + new + struct.pack("<I", len(new)) + b"PAR1"


def field(fields: Tree, field_id: int, default: Any = None) -> Any:
    for found, _, value in fields:
        if found == field_id:
            return value
    return default


def put(fields: Tree, field_id: int, kind: int, value: Any) -> None:
    """Set a field, in field-id order, as a writer lays a struct out."""
    fields[:] = [entry for entry in fields if entry[0] != field_id]
    fields.append([field_id, kind, value])
    fields.sort(key=lambda entry: entry[0])


def repeat(fields: Tree, field_id: int, kind: int, value: Any) -> None:
    """Add a field after the struct's last, whatever ids it holds: a second
    field of an id it already has, which no writer emits and :func:`put`
    cannot spell."""
    fields.append([field_id, kind, value])


def drop(fields: Tree, field_id: int) -> None:
    fields[:] = [entry for entry in fields if entry[0] != field_id]


def schema(tree: Tree) -> list[Tree]:
    return field(tree, 2)[1]


def paths(elements: list[Tree]) -> list[tuple[str, ...]]:
    """Each schema element's path of names below the root, by recursion
    over num_children (the production code uses a stack)."""
    out: list[tuple[str, ...]] = [()]

    def walk(index: int, prefix: tuple[str, ...]) -> int:
        here = (*prefix, field(elements[index], 4).decode())
        out.append(here)
        index += 1
        for _ in range(field(elements[index - 1], 5, 0)):
            index = walk(index, here)
        return index

    index = 1
    for _ in range(field(elements[0], 5, 0)):
        index = walk(index, ())
    assert index == len(elements)
    return out


def stamp(raw: bytes, stamps: list[Any]) -> bytes:
    """The oracle of stamp_variant_footer: each stamp (anything with its
    ``path``, ``kind``, ``precision`` and ``scale``) added to the decoded
    tree as a field, and the footer re-encoded."""

    def edit(tree: Tree) -> None:
        elements = schema(tree)
        where = dict(zip(paths(elements), elements, strict=True))
        for each in stamps:
            element = where[tuple(each.path)]
            if each.kind == "variant":
                variant = [[1, TType.BYTE, 1]]
                put(element, 10, TType.STRUCT, [[16, TType.STRUCT, variant]])
                continue
            scale, precision = each.scale, each.precision
            put(element, 6, TType.I32, 5)
            put(element, 7, TType.I32, scale)
            put(element, 8, TType.I32, precision)
            decimal = [[1, TType.I32, scale], [2, TType.I32, precision]]
            put(element, 10, TType.STRUCT, [[5, TType.STRUCT, decimal]])

    return rebuild(raw, edit)


def unstamp(raw: bytes, stamps: list[Any]) -> bytes:
    """The inverse of :func:`stamp`: ``raw`` with each stamp's fields
    removed, which is the file as its writer wrote it."""

    def edit(tree: Tree) -> None:
        elements = schema(tree)
        where = dict(zip(paths(elements), elements, strict=True))
        for each in stamps:
            for field_id in (10,) if each.kind == "variant" else (6, 7, 8, 10):
                drop(where[tuple(each.path)], field_id)

    return rebuild(raw, edit)


def set_created_by(raw: bytes, text: str) -> bytes:
    return rebuild(raw, lambda tree: put(tree, 6, TType.STRING, text.encode()))


def drop_statistics(raw: bytes, leaf: int) -> bytes:
    """``raw`` with no statistics for leaf column ``leaf`` in any row group."""

    def edit(tree: Tree) -> None:
        for row_group in field(tree, 4)[1]:
            chunk = field(row_group, 1)[1][leaf]
            drop(field(chunk, 3), 12)

    return rebuild(raw, edit)


def edit_chunk(
    raw: bytes,
    leaf: int,
    edit: Callable[[Tree], None],
    *,
    encoder: Callable[[Tree], bytes] = encode,
) -> bytes:
    """``raw`` with ``edit`` applied to the ColumnMetaData of leaf column
    ``leaf`` in every row group."""

    def apply(tree: Tree) -> None:
        for row_group in field(tree, 4)[1]:
            edit(field(field(row_group, 1)[1][leaf], 3))

    return rebuild(raw, apply, encoder=encoder)


def drop_null_count(raw: bytes, leaf: int) -> bytes:
    """``raw`` with statistics for leaf column ``leaf`` that hold no null
    count (Statistics field 3)."""

    def edit(tree: Tree) -> None:
        for row_group in field(tree, 4)[1]:
            chunk = field(row_group, 1)[1][leaf]
            drop(field(field(chunk, 3), 12), 3)

    return rebuild(raw, edit)


def edit_element(
    raw: bytes,
    path: tuple[str, ...],
    edit: Callable[[Tree], None],
    *,
    encoder: Callable[[Tree], bytes] = encode,
) -> bytes:
    """``raw`` with ``edit`` applied to the fields of the element at ``path``."""

    def apply(tree: Tree) -> None:
        elements = schema(tree)
        edit(dict(zip(paths(elements), elements, strict=True))[path])

    return rebuild(raw, apply, encoder=encoder)
