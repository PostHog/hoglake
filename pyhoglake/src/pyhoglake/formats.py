from __future__ import annotations

from collections.abc import Mapping

from .errors import ValidationError

TABLE_FORMAT_PROPERTY = "write.format.default"
PARQUET_FORMAT = "parquet"
CLICKHOUSE_MERGETREE_PACKED_FORMAT = "clickhouse-mergetree-packed"


def table_format(properties: Mapping[str, str] | None) -> str:
    return (properties or {}).get(TABLE_FORMAT_PROPERTY, PARQUET_FORMAT)


def require_parquet(properties: Mapping[str, str] | None, operation: str) -> None:
    file_format = table_format(properties)
    if file_format != PARQUET_FORMAT:
        raise ValidationError(
            f"{operation} supports Parquet tables only; table format is {file_format!r}",
            status_code=None,
        )
