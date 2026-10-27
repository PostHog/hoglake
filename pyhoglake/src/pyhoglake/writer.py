"""The writer core: one partition group's Arrow table in, the Parquet file
the catalog registers out (:func:`write_part`), and its registration
(:meth:`PreparedPart.registration`). The partition split, uploads and the
commit are the caller's: ``Table.append`` and ``prepare_append_tables`` are
built on this, and an ingester that owns its own calls it directly.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .errors import ValidationError, VariantEncodingError
from .models import Column
from .parquet_schema import (
    VariantParquet,
    _footer_size,
    _RefusedTable,
    prepared_schema_matches,
    write_variant_parquet,
)
from .stats import extract_column_stats
from .types import columns_to_arrow_schema
from .variant import (
    SHREDDING_KEY,
    VariantReport,
    _column_plan,
    _is_json_type,
    encode_json,
)

_SCHEMA_DIFFERS = "prepared Parquet schema/field IDs differ from destination"


@dataclass(frozen=True)
class PreparedPart:
    """One written file, checked and measured, ready to upload and register.

    ``size`` and ``footer_size`` are the registration's ``file_size_bytes``
    and ``footer_size``, measured on ``body`` as uploaded. ``metadata`` is
    the writer's FileMetaData, which ``record_count`` and ``column_stats``
    come from; a variant file's is pyarrow's from before the footer stamp
    (see :class:`~pyhoglake.parquet_schema.VariantParquet`).
    """

    body: pa.Buffer
    size: int
    footer_size: int
    metadata: pq.FileMetaData

    def registration(
        self,
        uri: str,
        columns: Sequence[Column],
        *,
        stats: bool,
        partition_values: Sequence[str | None] | None = None,
    ) -> dict[str, Any]:
        """The FileRegistration wire dict for this file uploaded at ``uri``,
        with ``column_stats`` from its own footer when ``stats``."""
        return _registration(self, uri, columns, stats, partition_values)


def _registration(
    part: Any,
    uri: str,
    columns: Sequence[Column],
    stats: bool,
    partition_values: Sequence[str | None] | None,
) -> dict[str, Any]:
    # One builder for every path, a file the caller wrote included (``part``
    # is then the client's measurement of it): a registration that differs
    # between them is a wire-shape bug nothing else catches.
    reg: dict[str, Any] = {
        "path": uri,
        "record_count": part.metadata.num_rows,
        "file_size_bytes": part.size,
        "footer_size": part.footer_size,
    }
    # Omitted, not empty: an absent key is what the server registers
    # 'pending' for the hydrator; `[]` would be 'provided' with no stats,
    # forever.
    if stats:
        reg["column_stats"] = [
            stat.to_wire()
            for stat in extract_column_stats(part.metadata, tuple(columns))
        ]
    if partition_values is not None:
        reg["partition_values"] = list(partition_values)
    return reg


def write_part(
    table: pa.Table,
    columns: Sequence[Column],
    *,
    write_options: Mapping[str, Any],
    row_group_size: int | None = None,
    variant_on_invalid: Literal["raise", "null"] = "raise",
    variant_report: VariantReport | None = None,
) -> PreparedPart:
    """Write ``table``, one partition group, as the destination's Parquet
    file, in memory.

    ``table`` holds the catalog ``columns`` in their order, with their field
    ids and nullability (``columns_to_arrow_schema(columns, variant=...)``);
    a ``timestamp_s`` at any depth as seconds or milliseconds, stored as
    milliseconds either way. ``write_options`` comes from
    :func:`pyhoglake.parquet_write_options`. A variant column takes
    ``pa.json_()`` text, the null type, or its storage in its declaration's
    write form, encoded as ``Table.append`` documents, with
    ``variant_on_invalid`` and ``variant_report``, and is written by
    :func:`~pyhoglake.parquet_schema.write_variant_parquet`; a table with
    none, by ``pq.write_table``. Every file's footer is read back and held
    to the destination's schema as a prepared file is, so a refusal is
    raised before there is anything to upload.
    """
    _check_variant_on_invalid(variant_on_invalid)
    variants = _variant_columns(columns, variant_on_invalid)
    # Built before the write: a variant nested in a container is refused
    # here. A variant file's footer reads back as the read form, and so
    # the whole of it is compared, not only its other columns.
    expected = _stored_schema(columns_to_arrow_schema(tuple(columns), variant="read"))
    if table.schema.names != expected.names:
        raise ValidationError(_SCHEMA_DIFFERS, status_code=None)
    if variants:
        for column in variants:
            _check_variant_values(column, table.column(column.name))
        written = _write_variant_part(
            table,
            columns,
            variants,
            on_invalid=variant_on_invalid,
            report=variant_report,
            row_group_size=row_group_size,
            write_options=write_options,
        )
        body, footer_size = written.body, written.footer_size
        metadata = written.metadata
        with pq.ParquetFile(pa.BufferReader(body)) as parquet:
            found = parquet.schema_arrow
    else:
        options = dict(write_options)
        if row_group_size is not None:
            options["row_group_size"] = row_group_size
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink, **options)
        body = sink.getvalue()
        with pq.ParquetFile(pa.BufferReader(body)) as parquet:
            found, metadata = parquet.schema_arrow, parquet.metadata
        # Only the 8-byte trailer is copied out of the buffer.
        footer_size = _footer_size(bytes(body[-8:]))
    # On the ENCODED footer, not the caller's schema: conversions the
    # writer makes (timestamp_s as milliseconds) are compared as written.
    if not prepared_schema_matches(found, expected):
        raise ValidationError(_SCHEMA_DIFFERS, status_code=None)
    return PreparedPart(
        body=body, size=body.size, footer_size=footer_size, metadata=metadata
    )


def _stored_schema(schema: pa.Schema) -> pa.Schema:
    """``schema`` as its Parquet file reads back: every ``timestamp[s]``, at
    any depth, as milliseconds (Parquet has no seconds unit)."""
    return pa.schema(
        [field.with_type(_stored_type(field.type)) for field in schema],
        metadata=schema.metadata,
    )


def _stored_type(kind: pa.DataType) -> pa.DataType:
    if kind == pa.timestamp("s"):
        return pa.timestamp("ms")
    if pa.types.is_struct(kind):
        return pa.struct([field.with_type(_stored_type(field.type)) for field in kind])
    if pa.types.is_map(kind):
        return pa.map_(
            kind.key_field.with_type(_stored_type(kind.key_type)),
            kind.item_field.with_type(_stored_type(kind.item_type)),
            keys_sorted=kind.keys_sorted,
        )
    if pa.types.is_list(kind):
        return pa.list_(kind.value_field.with_type(_stored_type(kind.value_type)))
    return kind


# -- VARIANT columns ---------------------------------------------------------------


def _check_variant_on_invalid(on_invalid: object) -> None:
    # Pure, so it runs before any request, as the codec settings do.
    if on_invalid not in ("raise", "null"):
        raise ValidationError(
            f"variant_on_invalid is 'raise' or 'null', not {on_invalid!r}",
            status_code=None,
        )


def _variant_columns(
    columns: Sequence[Column], on_invalid: Literal["raise", "null"]
) -> list[Column]:
    """The destination's variant columns (top-level: a nested one fails
    building the schema), refusing ``"null"`` when one is NOT NULL, whatever
    form its values come in: an invalid row would have nowhere to go."""
    variants = [column for column in columns if column.type == "variant"]
    not_null = [column.name for column in variants if not column.nullable]
    if on_invalid == "null" and not_null:
        raise ValidationError(
            "variant_on_invalid='null' writes an invalid row as SQL NULL, and "
            f"variant column(s) {not_null} are NOT NULL",
            status_code=None,
        )
    return variants


def _check_variant_values(column: Column, values: pa.ChunkedArray) -> None:
    """Refuse a variant column's values before anything is written: a type
    they do not come in (JSON text as ``pa.json_()``, over any of its
    storage types; the null type; or the storage in the declaration's write
    form), and SQL NULL in a NOT NULL column, as the encoder refuses it
    (D5), whatever the form."""
    _check_variant_type(column, values.type)
    if not column.nullable and values.null_count:
        row = pc.index(values.is_null(), True).as_py()
        raise VariantEncodingError(
            f"variant column '{column.name}' row {row}: SQL NULL into a NOT NULL "
            "column",
            reason="sql_null_in_not_null",
            column=column.name,
            row=row,
        )


def _check_variant_type(column: Column, kind: pa.DataType) -> None:
    if _is_json_type(kind) or pa.types.is_null(kind):
        return
    plan = _column_plan(column, (column.type_params or {}).get(SHREDDING_KEY))
    if plan.form_of(kind) == "write":
        return
    if (
        pa.types.is_string(kind)
        or pa.types.is_large_string(kind)
        or pa.types.is_string_view(kind)
    ):
        # A plain string makes no claim to be JSON (types.py), so it is not
        # read as JSON here either: the caller says so by casting.
        raise ValidationError(
            f"variant column {column.name!r} takes JSON text as pa.json_(), not "
            f"{kind}: cast it to pa.json_() to append it as JSON",
            status_code=None,
        )
    raise ValidationError(
        f"variant column {column.name!r} takes pa.json_() text, the null type "
        "(all SQL NULL), or its storage in its declaration's write form "
        "(columns_to_arrow_schema(columns, variant='storage')), not "
        f"{kind}",
        status_code=None,
    )


def _write_variant_part(
    data: pa.Table,
    columns: Sequence[Column],
    variants: Sequence[Column],
    *,
    on_invalid: Literal["raise", "null"],
    report: VariantReport | None,
    row_group_size: int | None,
    write_options: Mapping[str, Any],
) -> VariantParquet:
    """Each variant column's JSON text (or nulls) encoded as its storage,
    then the table written, stamped and layout-checked by
    ``write_variant_parquet``. The values were checked by
    :func:`_check_variant_values`.

    Pre-encoded storage is checked by structure (write_variant_parquet's
    ``structure_fault`` and integer view: no null pyarrow would write as
    other bytes), not by :func:`pyhoglake.variant.verify`, which decodes
    every row in Python at the encoder's cost and would undo what encoding
    elsewhere saves. Its bytes are the caller's to verify.
    """
    for column in variants:
        index = data.schema.get_field_index(column.name)
        field, values = data.schema.field(index), data.column(index)
        if pa.types.is_struct(values.type):
            continue  # storage already
        encoded = encode_json(values, column, on_invalid=on_invalid)
        if report is not None:
            report.merge(encoded.report)
        data = data.set_column(
            index, field.with_type(encoded.array.type), encoded.array
        )
    try:
        return write_variant_parquet(
            data, columns, row_group_size=row_group_size, write_options=write_options
        )
    except _RefusedTable as error:
        # The caller's table: a field that is not its column's, or storage
        # with a null pyarrow would write as other values. An error of
        # pyarrow's write is raised as the other write paths raise it.
        raise ValidationError(
            f"cannot write VARIANT columns: {error}", status_code=None
        ) from error
