"""Typed alter-op helpers for :meth:`pyhoglake.Table.alter`.

Usage::

    from pyhoglake import ops
    table.alter([
        ops.add_column("score", pa.float64()),
        ops.rename_column("old", "new"),
    ])
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa

from .models import PartitionField
from .types import arrow_type_to_coltype


@dataclass(frozen=True)
class AlterOp:
    op: str
    body: dict[str, Any] = field(default_factory=dict)

    def to_wire(self) -> dict[str, Any]:
        return {"op": self.op, **self.body}


def _column_def(
    name: str, type_: pa.DataType | str, nullable: bool = True
) -> dict[str, Any]:
    if isinstance(type_, pa.DataType):
        col_type, params = arrow_type_to_coltype(type_)
    else:
        col_type, params = type_, None
    col: dict[str, Any] = {"name": name, "type": col_type, "nullable": nullable}
    if params:
        col["type_params"] = params
    return col


def add_column(name: str, type_: pa.DataType | str, nullable: bool = True) -> AlterOp:
    """Add a column. ``type_`` is a pyarrow DataType or a hoglake type name."""
    return AlterOp("add_column", {"column": _column_def(name, type_, nullable)})


def drop_column(name: str) -> AlterOp:
    return AlterOp("drop_column", {"name": name})


def rename_column(from_: str, to: str) -> AlterOp:
    return AlterOp("rename_column", {"from": from_, "to": to})


def promote_column(name: str, to: str) -> AlterOp:
    """Widen a column's type (e.g. int -> long). ``to`` is a hoglake type name."""
    return AlterOp("promote_column", {"name": name, "to": to})


def rename_table(new_name: str) -> AlterOp:
    return AlterOp("rename_table", {"new_name": new_name})


def partition_field(
    source_field_id: int, transform: str, transform_param: int | None = None
) -> PartitionField:
    return PartitionField(
        source_field_id=source_field_id,
        transform=transform,
        transform_param=transform_param,
    )


def set_partition_spec(
    fields: list[PartitionField | dict[str, Any]],
) -> AlterOp:
    """Set the partition spec. An empty list makes the table unpartitioned."""
    wire = [f.to_wire() if isinstance(f, PartitionField) else dict(f) for f in fields]
    return AlterOp("set_partition_spec", {"fields": wire})


# Server-side bounds (server/.../service/TableMetadata.kt). Mirrored here
# so a bad comment or property set fails BEFORE the round trip, with the
# same message the server would raise. test_metadata_parity.py parses these
# out of TableMetadata.kt rather than restating them, so a drifted constant
# fails the suite instead of silently disagreeing with the server.
_COMMENT_MAX_CHARS = 16384
_PROPERTIES_MAX = 100
_PROPERTY_VALUE_MAX_CHARS = 4096
_PROPERTY_KEY_PATTERN = re.compile(r"[a-z][a-z0-9_.-]{0,127}")
_PROPERTY_KEY_RESERVED_PREFIXES = ("hoglake.", "trino.")
_PROPERTY_KEY_RESERVED = frozenset(
    {"partitioning", "sorted_by", "location", "format", "comment"}
)


def _validate_comment(comment: str) -> None:
    if len(comment) > _COMMENT_MAX_CHARS or "\x00" in comment:
        raise ValueError("comment must contain at most 16384 characters and no NUL")


def _validate_properties(properties: dict[str, str]) -> None:
    if len(properties) > _PROPERTIES_MAX:
        raise ValueError("at most 100 custom properties are allowed")
    for key, value in properties.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise TypeError("custom properties require string keys and values")
        if (
            not _PROPERTY_KEY_PATTERN.fullmatch(key)
            or key.startswith(_PROPERTY_KEY_RESERVED_PREFIXES)
            or key in _PROPERTY_KEY_RESERVED
        ):
            raise ValueError(f"invalid or reserved custom property key '{key}'")
        if len(value) > _PROPERTY_VALUE_MAX_CHARS or "\x00" in value:
            raise ValueError(
                "custom property values must contain at most 4096 characters and no NUL"
            )


def set_table_comment(comment: str | None) -> AlterOp:
    """Set the table comment; ``None`` removes it."""
    if comment is not None:
        _validate_comment(comment)
    return AlterOp("set_table_comment", {"comment": comment})


def set_column_comment(name: str, comment: str | None) -> AlterOp:
    """Set a column's comment; ``None`` removes it. ``name`` is a dotted path."""
    if comment is not None:
        _validate_comment(comment)
    return AlterOp("set_column_comment", {"name": name, "comment": comment})


def set_properties(properties: dict[str, str]) -> AlterOp:
    """Replace the table's properties as a whole; an empty dict clears them."""
    _validate_properties(properties)
    return AlterOp("set_properties", {"properties": dict(properties)})
