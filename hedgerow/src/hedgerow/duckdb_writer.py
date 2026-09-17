"""Native VARIANT event-file writer, not yet wired into catalog publication.

Payloads stay in DuckDB from Parquet scan through COPY. Each call owns a separate
connection and disposable scratch; immutable raw inputs remain the backup. Memory
is a DuckDB budget, not a process RSS ceiling. Callers must bound worker count.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

from .halts import DataIntegrityError
from .sorted_writer import EVENT_SORT


def _identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


@dataclass(frozen=True)
class DuckDBFragment:
    """One immutable source file's row groups and expected selected row count."""

    path: str
    row_groups: tuple[int, ...]
    rows: int


@dataclass(frozen=True)
class DuckDBWriterOptions:
    target_file_bytes: int = 256 * 1024 * 1024
    memory_bytes: int = 512 * 1024 * 1024
    scratch_bytes: int = 8 * 1024 * 1024 * 1024
    row_group_rows: int = 8192

    def __post_init__(self):
        if min(self.target_file_bytes, self.memory_bytes, self.scratch_bytes) <= 0:
            raise ValueError("writer byte budgets must be positive")
        if self.row_group_rows < 2048:
            raise ValueError("DuckDB row groups require at least 2048 rows")


def write_duckdb_event_partition(
    fragments: Sequence[DuckDBFragment],
    output_directory: str,
    *,
    team_id: int,
    month: int,
    field_ids: Mapping[str, int],
    json_columns: Sequence[str] = (),
    options: DuckDBWriterOptions | None = None,
    configure_connection: Callable | None = None,
) -> list[Path]:
    """Write one identity(team_id)/month(timestamp) partition, UUID last.

    ``month`` is the Iceberg epoch-relative month, not month-of-year. Field IDs
    enumerate the exact output projection, including derived ``event_date``.
    Only explicitly selected JSON/VARCHAR columns become VARIANT. Existing native
    VARIANT columns pass through. Source UUID may be native UUID or 16-byte BLOB.

    Top-level JSON null is refused because DuckDB maps it to SQL NULL. SQL NULL
    and nested JSON nulls are accepted. Source schemas must agree exactly.
    No catalog mutation, upload, source deletion or Arrow payload conversion occurs.

    The optional connection initializer configures native DuckDB S3 credentials;
    it is trusted application code and must not override writer resource settings.
    Local tests do not establish S3 row-group pruning or cross-engine compatibility.
    """
    options = options or DuckDBWriterOptions()
    required = {*EVENT_SORT, "team_id"}
    if not required.issubset(field_ids):
        raise ValueError("output requires team_id, timestamp, event, uuid, event_date")
    if (
        any(not name or "\x00" in name for name in field_ids)
        or len({name.casefold() for name in field_ids}) != len(field_ids)
        or "file_row_number" in {name.casefold() for name in field_ids}
        or any(type(i) is not int or not 0 < i < 2**31 for i in field_ids.values())
        or len(set(field_ids.values())) != len(field_ids)
    ):
        raise ValueError("output names and positive int32 field IDs must be unique")
    if type(team_id) is not int or type(month) is not int:
        raise ValueError("team_id and month must be integers")
    if (
        len(set(json_columns)) != len(json_columns)
        or not set(json_columns).issubset(field_ids)
        or set(json_columns) & required
    ):
        raise ValueError("JSON conversion must name unique non-routing output columns")
    if not fragments or len({f.path for f in fragments}) != len(fragments):
        raise ValueError("provide unique source files for one frozen partition")
    for fragment in fragments:
        if (
            not fragment.path
            or any(c in fragment.path for c in "*?[]\x00")
            or not fragment.row_groups
            or len(set(fragment.row_groups)) != len(fragment.row_groups)
            or any(type(g) is not int or g < 0 for g in fragment.row_groups)
            or type(fragment.rows) is not int
            or fragment.rows <= 0
        ):
            raise ValueError(
                "source fragments require literal paths, row groups and positive counts"
            )

    root = Path(output_directory)
    root.mkdir(parents=True, exist_ok=True)
    if any(root.iterdir()):
        raise ValueError("output directory must be empty")
    with TemporaryDirectory(prefix="duckdb-flush-", dir=root) as temp:
        workspace = Path(temp)
        with duckdb.connect() as connection:
            if configure_connection is not None:
                configure_connection(connection)
            for name, value in (
                ("threads", "1"),
                ("memory_limit", f"{options.memory_bytes}B"),
                ("max_temp_directory_size", f"{options.scratch_bytes}B"),
                ("temp_directory", str(workspace / "spill")),
                ("TimeZone", "UTC"),
                ("preserve_insertion_order", "true"),
            ):
                connection.execute(f"SET {name} = {_literal(value)}")
            selects = []
            expected_schema = None
            for fragment in fragments:
                scan = f"read_parquet({_literal(fragment.path)}, hive_partitioning=false, file_row_number=true)"
                schema = {
                    row[0]: row[1]
                    for row in connection.execute(
                        f"DESCRIBE SELECT * FROM {scan}"
                    ).fetchall()
                }
                source_names = [name for name in field_ids if name != "event_date"]
                if not set(source_names).issubset(schema):
                    raise DataIntegrityError("raw file lacks a projected output column")
                projected_schema = {name: schema[name] for name in source_names}
                if expected_schema is not None and projected_schema != expected_schema:
                    raise DataIntegrityError(
                        "raw files disagree on projected column types"
                    )
                expected_schema = projected_schema
                if (
                    schema["team_id"] not in ("INTEGER", "BIGINT")
                    or schema["timestamp"]
                    not in ("TIMESTAMP", "TIMESTAMP WITH TIME ZONE")
                    or schema["event"] != "VARCHAR"
                    or schema["uuid"] not in ("UUID", "BLOB")
                ):
                    raise DataIntegrityError(
                        "unsupported raw event routing/identity types"
                    )
                if any(
                    schema[name] not in ("JSON", "VARCHAR") for name in json_columns
                ):
                    raise DataIntegrityError(
                        "JSON conversion requires JSON or VARCHAR input"
                    )
                groups = connection.execute(
                    "SELECT DISTINCT row_group_id, row_group_num_rows FROM parquet_metadata(?) ORDER BY row_group_id",
                    [fragment.path],
                ).fetchall()
                offset = 0
                ranges = []
                for group, rows in groups:
                    if group in fragment.row_groups:
                        ranges.append(
                            f"(file_row_number >= {offset} AND file_row_number < {offset + rows})"
                        )
                    offset += rows
                if not set(fragment.row_groups).issubset({g for g, _ in groups}):
                    raise DataIntegrityError("raw row group disappeared")
                group_filter = " OR ".join(ranges)
                routing = (
                    f"{_identifier('team_id')} = {team_id} AND "
                    f"(year({_identifier('timestamp')}) - 1970) * 12 + "
                    f"month({_identifier('timestamp')}) - 1 = {month}"
                )
                selected = f"SELECT * FROM {scan} WHERE ({group_filter}) AND {routing}"
                count = connection.execute(
                    f"SELECT count(*) FROM ({selected})"
                ).fetchone()[0]
                if count != fragment.rows:
                    raise DataIntegrityError(
                        "flush row count differs from discovered source fragment"
                    )
                for name in json_columns:
                    if connection.execute(
                        f"SELECT count(*) FROM ({selected}) WHERE json_type(CAST({_identifier(name)} AS JSON)) = 'NULL'"
                    ).fetchone()[0]:
                        raise DataIntegrityError(
                            "top-level JSON null cannot be preserved as native VARIANT"
                        )
                projection = []
                for name in field_ids:
                    expression = _identifier(name)
                    if name == "event_date":
                        expression = 'CAST("timestamp" AS DATE)'
                    elif name in json_columns:
                        expression = f"CAST(CAST({expression} AS JSON) AS VARIANT)"
                    elif name == "uuid" and schema[name] == "BLOB":
                        expression = f"CAST(hex({expression}) AS UUID)"
                    projection.append(f"{expression} AS {_identifier(name)}")
                selects.append(f"SELECT {', '.join(projection)} FROM ({selected})")
            order = ", ".join(f"{_identifier(n)} ASC NULLS FIRST" for n in EVENT_SORT)
            ids = ", ".join(f"{_literal(n)}: {i}" for n, i in field_ids.items())
            connection.execute(
                f"COPY ({' UNION ALL '.join(selects)} ORDER BY {order}) "
                f"TO {_literal(str(workspace / 'output'))} "
                f"(FORMAT PARQUET, COMPRESSION ZSTD, FIELD_IDS {{{ids}}}, "
                f"FILE_SIZE_BYTES '{options.target_file_bytes}B', ROW_GROUP_SIZE {options.row_group_rows})"
            )
            paths = sorted(
                (workspace / "output").glob("data_*.parquet"),
                key=lambda p: int(p.stem.removeprefix("data_")),
            )
            count = sum(
                connection.execute(
                    "SELECT num_rows FROM parquet_file_metadata(?)", [str(p)]
                ).fetchone()[0]
                for p in paths
            )
            if count != sum(f.rows for f in fragments):
                raise DataIntegrityError("output row count differs from frozen work")
        return [path.rename(root / path.name) for path in paths]
