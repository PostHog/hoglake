"""A generic compact-Thrift codec for Parquet footers: the test oracle of
``parquet_schema.stamp_variant_footer``. Tests only.

It reads a FileMetaData into a schema-less tree of ``[field id, type,
value]``, lets a test edit it, and writes it back with Thrift's own writer.
The production splice never re-encodes anything, so the two share no code:
a footer the splice produces must equal, byte for byte, the one this codec
writes after making the same edit to the tree. It also forges what pyarrow
does not write (a field after the field id, a second element of one name)
for the refusal tests.

A round trip of an unedited footer is byte-identical (:func:`rebuild`
checks it on every use), which is what makes it an oracle.
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

_READERS = {
    TType.BOOL: "readBool",
    TType.BYTE: "readByte",
    TType.I16: "readI16",
    TType.I32: "readI32",
    TType.I64: "readI64",
    TType.DOUBLE: "readDouble",
    # Bytes, never text: statistics hold raw bytes.
    TType.STRING: "readBinary",
}


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
        item_kind, count = protocol.readListBegin()
        items = [_read(protocol, item_kind, depth + 1) for _ in range(count)]
        protocol.readListEnd()
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
    return getattr(protocol, _READERS[kind])()


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
        protocol.writeListBegin(item_kind, len(items))
        for item in items:
            _write(protocol, item_kind, item)
        protocol.writeListEnd()
    elif kind == TType.MAP:
        key_kind, value_kind, pairs = value
        protocol.writeMapBegin(key_kind, value_kind, len(pairs))
        for key, item in pairs:
            _write(protocol, key_kind, key)
            _write(protocol, value_kind, item)
        protocol.writeMapEnd()
    else:
        getattr(protocol, "write" + _READERS[kind][4:])(value)


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


def split(raw: bytes) -> tuple[int, bytes]:
    """The footer's offset, and the footer."""
    if raw[-4:] != b"PAR1":
        raise ValueError("not a plaintext Parquet file")
    (length,) = struct.unpack("<I", raw[-8:-4])
    start = len(raw) - 8 - length
    return start, raw[start:-8]


def rebuild(raw: bytes, edit: Callable[[Tree], None]) -> bytes:
    """``raw`` with its footer decoded, edited in place and re-encoded."""
    start, footer = split(raw)
    tree = decode(footer)
    if encode(tree) != footer:
        raise ValueError("the footer does not round-trip byte for byte")
    edit(tree)
    new = encode(tree)
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


def edit_element(
    raw: bytes, path: tuple[str, ...], edit: Callable[[Tree], None]
) -> bytes:
    """``raw`` with ``edit`` applied to the fields of the element at ``path``."""

    def apply(tree: Tree) -> None:
        elements = schema(tree)
        edit(dict(zip(paths(elements), elements, strict=True))[path])

    return rebuild(raw, apply)
