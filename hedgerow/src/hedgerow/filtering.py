"""Client-side row filter: keep rows where ``column == equals``.

The routing_value analog from the viaduck world, minus everything else:
one column, one equality, evaluated per record batch with Arrow compute.
NULLs never match. The filter value is validated (castable to the source
column's Arrow type) at startup — fail-fast, lesson #6.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc
from pyhoglake import Column, coltype_to_arrow

from .config import ConfigError, FilterConfig


def _comparable(t: pa.DataType) -> pa.DataType:
    """An extension type's storage; anything else unchanged.

    Arrow ships NO compute kernels for extension types — `pc.equal` over
    two `extension<arrow.uuid>` arrays raises ArrowNotImplementedError,
    and mixing an extension with its own storage raises the same way. A
    hoglake `uuid` column reaches this code annotated (what pyhoglake
    and compaction write) or bare (everything registered before that
    contract), and a `json` column as `extension<arrow.json>` or plain
    utf8, depending on the writer. Comparing storage is the one form
    both spellings share, and it is the same bytes.
    """
    return t.storage_type if isinstance(t, pa.BaseExtensionType) else t


@dataclass(frozen=True)
class RowFilter:
    column: str
    value: pa.Scalar

    def apply(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        column = batch.column(self.column)
        if isinstance(column.type, pa.BaseExtensionType):
            # The storage side of _comparable, which is the type
            # build_filter cast the value to.
            column = column.storage
        mask = pc.equal(column, self.value)
        mask = pc.fill_null(mask, False)  # NULL == x is NULL: drop, don't keep
        return batch.filter(mask)


def build_filter(
    cfg: FilterConfig | None, source_columns: Sequence[Column]
) -> RowFilter | None:
    if cfg is None:
        return None
    if cfg.equals is None:
        # BUG-5 regression: pa.scalar(None, t) validates and pc.equal
        # against a null scalar is null for EVERY row -> fill_null(False)
        # drops 100% of rows while the offset advances. Refuse at
        # startup; FilterConfig.parse refuses the same way for YAML/env
        # configs, this covers direct construction.
        raise ConfigError(
            "filter.equals filter value must be non-null; a null filter "
            "matches nothing (NULLs never match an equality filter)"
        )
    if isinstance(cfg.equals, float) and not math.isfinite(cfg.equals):
        # Same class as the null trap (bugs.md #4): pc.equal(col, nan) is
        # False for EVERY row (IEEE 754: NaN never equals anything, itself
        # included), so a NaN filter silently drops 100% of rows while the
        # offset still advances — permanent, unrecoverable skips at the
        # destination. FilterConfig.parse refuses YAML/env configs the
        # same way; this covers direct construction.
        raise ConfigError(
            f"filter.equals filter value must be finite, got {cfg.equals!r}; "
            "a NaN filter matches no row (NaN never equals anything), so it "
            "would drop 100% of rows while the offset advances"
        )
    col = next((c for c in source_columns if c.name == cfg.column), None)
    if col is None:
        # validate_projection reports this too; belt and braces for
        # direct construction paths.
        raise ConfigError(f"filter.column {cfg.column!r} is not a source column")
    # Storage form: the value is compared against a column read from
    # parquet, whose spelling depends on the writer, and no compute
    # kernel accepts an extension type at all (see _comparable).
    arrow_type = _comparable(coltype_to_arrow(col.type, col.type_params))
    try:
        value = pa.scalar(cfg.equals, type=arrow_type)
    except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError, TypeError) as e:
        raise ConfigError(
            f"filter.equals value {cfg.equals!r} is not valid for column "
            f"{cfg.column!r} of type {col.type} ({arrow_type}): {e}"
        ) from None
    return RowFilter(column=cfg.column, value=value)
