"""ClickHouse-local adapter for restricted single-object packed MergeTree parts."""

from __future__ import annotations

import contextlib
import json
import re
import subprocess
import tempfile
import uuid
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa

from .client import Table, _align_table
from .errors import HoglakeError, ValidationError
from .formats import CLICKHOUSE_MERGETREE_PACKED_FORMAT, table_format
from .models import (
    AppendedFile,
    AppendResult,
    CommitResult,
    TableInfo,
)
from .upload import Upload, perform_upload, s3_key

_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]{0,127}$")

_SUPPORTED_TYPES = frozenset(
    {
        "boolean",
        "int8",
        "int16",
        "int",
        "long",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "float",
        "double",
        "date",
        "timestamp_s",
        "timestamp_ms",
        "timestamp",
        "timestamp_ns",
        "timestamptz",
        "string",
        "binary",
    }
)

_CLICKHOUSE_TYPES = {
    "boolean": "Bool",
    "int8": "Int8",
    "int16": "Int16",
    "int": "Int32",
    "long": "Int64",
    "uint8": "UInt8",
    "uint16": "UInt16",
    "uint32": "UInt32",
    "uint64": "UInt64",
    "float": "Float32",
    "double": "Float64",
    "date": "Date32",
    "timestamp_s": "DateTime64(0, 'UTC')",
    "timestamp_ms": "DateTime64(3, 'UTC')",
    "timestamp": "DateTime64(6, 'UTC')",
    "timestamp_ns": "DateTime64(9, 'UTC')",
    "timestamptz": "DateTime64(6, 'UTC')",
    "string": "String",
    "binary": "String",
}

# Coherent by construction: ClickHouse needs ~2.5x a block's size to parse an
# Arrow insert, and a read sorts one part at a time, so the memory limit
# covers the largest part; a snapshot within its byte limit is readable
# within the result limit (packed parts compress, results do not).
_DEFAULT_MAX_PART_BYTES = 1 * 1024**3
_DEFAULT_MAX_SNAPSHOT_BYTES = 2 * 1024**3
_DEFAULT_MAX_SNAPSHOT_PARTS = 500
_DEFAULT_MAX_MEMORY_BYTES = 4 * 1024**3
_DEFAULT_MAX_RESULT_BYTES = 2 * 1024**3

_ARROW_TYPES: dict[str, pa.DataType] = {
    "boolean": pa.bool_(),
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int": pa.int32(),
    "long": pa.int64(),
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
    "float": pa.float32(),
    "double": pa.float64(),
    "date": pa.date32(),
    "timestamp_s": pa.timestamp("s"),
    "timestamp_ms": pa.timestamp("ms"),
    "timestamp": pa.timestamp("us"),
    "timestamp_ns": pa.timestamp("ns"),
    "timestamptz": pa.timestamp("us", tz="UTC"),
    "string": pa.string(),
    "binary": pa.binary(),
}


def _quote_identifier(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def _quote_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _schema(info: TableInfo) -> pa.Schema:
    fields: list[pa.Field] = []
    for column in info.columns:
        if not _SAFE_IDENTIFIER.fullmatch(column.name):
            raise ValidationError(
                f"packed MergeTree requires a safe catalog column identifier, got {column.name!r}",
                status_code=None,
            )
        if column.name.startswith("_"):
            # ClickHouse resolves a real column before a virtual one, so a column
            # named _part or _part_offset would silently reorder every read.
            raise ValidationError(
                "packed MergeTree reserves column names starting with '_' for "
                f"ClickHouse virtual columns, got {column.name!r}",
                status_code=None,
            )
        if column.children or column.type not in _SUPPORTED_TYPES:
            raise ValidationError(
                f"packed MergeTree does not support column {column.name!r} "
                f"of type {column.type!r}",
                status_code=None,
            )
        fields.append(
            pa.field(column.name, _ARROW_TYPES[column.type], nullable=column.nullable)
        )
    return pa.schema(fields)


def _require_packed(info: TableInfo) -> None:
    actual = table_format(info.properties)
    if actual != CLICKHOUSE_MERGETREE_PACKED_FORMAT:
        raise ValidationError(
            "ClickHousePackedAdapter requires "
            f"{CLICKHOUSE_MERGETREE_PACKED_FORMAT!r}, got {actual!r}",
            status_code=None,
        )
    if info.partition_spec is not None and info.partition_spec.fields:
        raise ValidationError(
            "packed MergeTree tables do not support partition specs",
            status_code=None,
        )
    if info.sort_spec is not None and info.sort_spec.fields:
        raise ValidationError(
            "packed MergeTree tables do not support sort orders",
            status_code=None,
        )
    _schema(info)


def _arrow_stream(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


class ClickHousePackedAdapter:
    """Produce and read packed parts with a trusted ``clickhouse local`` executable.

    The adapter downloads only paths returned by the requested Hoglake scan plan.
    Each operation uses an isolated temporary ClickHouse data directory.
    """

    def __init__(
        self,
        executable: str | Path | Sequence[str] = "clickhouse",
        *,
        timeout: float = 120.0,
        max_threads: int = 2,
        max_part_bytes: int = _DEFAULT_MAX_PART_BYTES,
        max_snapshot_bytes: int = _DEFAULT_MAX_SNAPSHOT_BYTES,
        max_snapshot_parts: int = _DEFAULT_MAX_SNAPSHOT_PARTS,
        max_memory_bytes: int = _DEFAULT_MAX_MEMORY_BYTES,
        max_result_bytes: int = _DEFAULT_MAX_RESULT_BYTES,
    ) -> None:
        command: tuple[str, ...]
        if isinstance(executable, (str, Path)):
            command = (str(executable),)
        else:
            command = tuple(executable)
        if not command or any(not part for part in command):
            raise ValueError("executable must contain at least one non-empty argument")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if max_threads <= 0:
            raise ValueError("max_threads must be positive")
        limits = {
            "max_part_bytes": max_part_bytes,
            "max_snapshot_bytes": max_snapshot_bytes,
            "max_snapshot_parts": max_snapshot_parts,
            "max_memory_bytes": max_memory_bytes,
            "max_result_bytes": max_result_bytes,
        }
        invalid = [name for name, value in limits.items() if value <= 0]
        if invalid:
            raise ValueError(f"{', '.join(invalid)} must be positive")
        self._command = command
        self._timeout = timeout
        self._max_threads = max_threads
        self._max_part_bytes = max_part_bytes
        self._max_snapshot_bytes = max_snapshot_bytes
        self._max_snapshot_parts = max_snapshot_parts
        self._max_memory_bytes = max_memory_bytes
        self._max_result_bytes = max_result_bytes

    def prepare_append(
        self,
        table: Table,
        data: pa.Table,
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """Create, claim and upload one packed part, returning its exact commit payload."""
        if data.num_rows == 0:
            raise ValidationError(
                "packed MergeTree append requires at least one row",
                status_code=None,
            )
        operation = uuid.UUID(idempotency_key) if idempotency_key else uuid.uuid4()
        catalog = table._namespace._catalog
        expected_uuid = table.table_uuid
        info = table._check_incarnation(expected_uuid)
        _require_packed(info)
        if info.read_snapshot_id is None:
            raise HoglakeError(
                "packed appends require a server that reports Table.read_snapshot_id"
            )
        read_snapshot = info.read_snapshot_id
        aligned = _align_table(data, _schema(info))
        claim_path: str | None = None
        with tempfile.TemporaryDirectory(prefix="pyhoglake-packed-write-") as directory:
            root = Path(directory)
            self._run(root, self._create_sql(info, "packed_write"))
            # Squash the whole input into one block: by default ClickHouse cuts
            # inserts at min_insert_block_size_bytes (~256 MiB uncompressed),
            # which would produce several parts for one append.
            unbounded = str(2**62)
            self._run(
                root,
                f"INSERT INTO {_quote_identifier('packed_write')} FORMAT ArrowStream",
                input_bytes=_arrow_stream(aligned),
                settings={
                    "min_insert_block_size_rows": unbounded,
                    "min_insert_block_size_bytes": unbounded,
                    "max_insert_block_size": unbounded,
                },
            )
            part = self._single_part(root, "packed_write", aligned.num_rows)
            packed_file = part / "data.packed"
            packed_size = packed_file.stat().st_size
            if packed_size > self._max_part_bytes:
                raise ValidationError(
                    f"packed part is {packed_size} bytes, over max_part_bytes "
                    f"{self._max_part_bytes}",
                    status_code=None,
                )
            body = {
                "owner": str(operation),
                "prefix": (
                    f"{catalog.data_path.rstrip('/')}/data/{info.namespace}/"
                    f"{info.name}/{operation}"
                ),
                "file_kind": "data",
                "file_format": CLICKHOUSE_MERGETREE_PACKED_FORMAT,
            }
            claim = catalog._client._request(
                "PUT",
                catalog._path(f"/uploads/{uuid.uuid4()}"),
                json=body,
            )
            claim_path = str(claim["path"])
            try:
                self._upload(table, packed_file, claim_path)
            except BaseException:
                self._abandon(table, operation, [claim_path], best_effort=True)
                raise
            registration = {
                "path": claim_path,
                "file_format": CLICKHOUSE_MERGETREE_PACKED_FORMAT,
                "record_count": aligned.num_rows,
                "file_size_bytes": packed_size,
                "column_stats": [],
            }
        return {
            "idempotency_key": str(operation),
            "read_snapshot": read_snapshot,
            "appends": [
                {
                    "namespace": table.namespace,
                    "table": table.name,
                    "expected_table_uuid": expected_uuid,
                    "files": [registration],
                }
            ],
        }

    def commit_prepared(self, table: Table, payload: dict[str, Any]) -> CommitResult:
        """Publish the exact payload returned by :meth:`prepare_append`."""
        self._validate_payload(table, payload)
        return table._namespace._catalog._commit_uploads(payload, table=table)

    def append(
        self,
        table: Table,
        data: pa.Table,
        *,
        idempotency_key: str | None = None,
    ) -> AppendResult:
        """Prepare, upload and publish one packed part."""
        payload = self.prepare_append(table, data, idempotency_key=idempotency_key)
        try:
            result = self.commit_prepared(table, payload)
        except BaseException as error:
            with contextlib.suppress(AttributeError, TypeError):
                error.prepared_payload = payload  # type: ignore[attr-defined]
            raise
        file = payload["appends"][0]["files"][0]
        return AppendResult(
            snapshot_id=result.snapshot_id,
            schema_version=result.schema_version,
            files=(
                AppendedFile(
                    path=file["path"],
                    record_count=file["record_count"],
                    partition_values=None,
                ),
            ),
        )

    def abandon_prepared(self, table: Table, payload: Mapping[str, Any]) -> int:
        """Fence an uncommitted prepared upload so normal cleanup can reclaim it."""
        self._validate_payload(table, payload)
        owner = uuid.UUID(str(payload["idempotency_key"]))
        paths = [
            str(file["path"])
            for append in payload["appends"]
            for file in append["files"]
        ]
        return self._abandon(table, owner, paths)

    def read(
        self,
        table: Table,
        *,
        snapshot: int | None = None,
        at_timestamp: datetime | str | None = None,
    ) -> pa.Table:
        """Read exactly the packed parts in one Hoglake scan plan."""
        info = table.info(snapshot=snapshot, at_timestamp=at_timestamp, totals=False)
        _require_packed(info)
        schema = _schema(info)
        if info.read_snapshot_id is None:
            raise HoglakeError(
                "packed reads require a server that reports Table.read_snapshot_id"
            )
        plan = table.scan_plan(snapshot=info.read_snapshot_id)
        if len(plan) > self._max_snapshot_parts:
            raise ValidationError(
                f"packed snapshot has {len(plan)} parts, over max_snapshot_parts "
                f"{self._max_snapshot_parts}",
                status_code=None,
            )
        for item in plan:
            file = item.data_file
            if file.file_format != CLICKHOUSE_MERGETREE_PACKED_FORMAT:
                raise ValidationError(
                    f"snapshot contains unsupported data format {file.file_format!r}",
                    status_code=None,
                )
            if item.delete_file is not None:
                raise ValidationError(
                    "packed MergeTree reads do not support deletion vectors",
                    status_code=None,
                )
            if file.explicit_row_ids:
                raise ValidationError(
                    "packed MergeTree reads do not support explicit row-id files",
                    status_code=None,
                )
            if file.file_size_bytes > self._max_part_bytes:
                raise ValidationError(
                    f"packed part {file.path} is {file.file_size_bytes} bytes, over "
                    f"max_part_bytes {self._max_part_bytes}",
                    status_code=None,
                )
        snapshot_bytes = sum(item.data_file.file_size_bytes for item in plan)
        if snapshot_bytes > self._max_snapshot_bytes:
            raise ValidationError(
                f"packed snapshot is {snapshot_bytes} bytes, over max_snapshot_bytes "
                f"{self._max_snapshot_bytes}",
                status_code=None,
            )
        if not plan:
            return pa.Table.from_batches([], schema=schema)

        with tempfile.TemporaryDirectory(prefix="pyhoglake-packed-read-") as directory:
            root = Path(directory)
            self._run(root, self._create_sql(info, "packed_read"))
            data_path = (
                self._run(
                    root,
                    "SELECT arrayJoin(data_paths) FROM system.tables "
                    "WHERE name='packed_read' FORMAT TSVRaw",
                )
                .decode("utf-8")
                .strip()
            )
            table_path = Path(data_path).resolve()
            try:
                table_path.relative_to(root.resolve())
            except ValueError as error:
                raise HoglakeError(
                    "ClickHouse table path escaped its isolated directory"
                ) from error
            detached = table_path / "detached"
            detached.mkdir(parents=True, exist_ok=True)
            for index, item in enumerate(plan, start=1):
                destination = detached / f"all_{index}_{index}_0"
                destination.mkdir()
                self._download(
                    table,
                    item.data_file.path,
                    destination / "data.packed",
                    item.data_file.file_size_bytes,
                )
                actual = (destination / "data.packed").stat().st_size
                if actual != item.data_file.file_size_bytes:
                    raise HoglakeError(
                        f"downloaded packed part size {actual} does not match registered "
                        f"size {item.data_file.file_size_bytes} for {item.data_file.path}"
                    )
            attach = ";".join(
                f"ALTER TABLE {_quote_identifier('packed_read')} ATTACH PART "
                f"'all_{index}_{index}_0'"
                for index in range(1, len(plan) + 1)
            )
            raw = self._run(
                root,
                attach
                + ";ALTER TABLE "
                + _quote_identifier("packed_read")
                + " MODIFY SETTING table_readonly=1"
                + ";SELECT name, rows FROM system.parts WHERE active AND "
                + "table='packed_read' ORDER BY min_block_number FORMAT JSONEachRow",
            )
            # ATTACH numbers blocks in statement order, so block order is plan
            # order. Check it rather than assume it: every part must hold
            # exactly the rows its own registration records.
            parts = [json.loads(line) for line in raw.splitlines() if line]
            attached = [int(part["rows"]) for part in parts]
            registered = [item.data_file.record_count for item in plan]
            if attached != registered:
                raise HoglakeError(
                    f"attached packed parts hold {attached} rows, the scan plan "
                    f"registers {registered}"
                )
            # One output file per part, sorted within the part only. A single
            # ORDER BY across parts would materialize and sort the whole
            # snapshot under max_memory_usage; per part, the sort is bounded by
            # one part. Files rather than stdout, so the run goes through
            # _run's bounded path (both pipes drained, timeout enforced, exit
            # status checked before any output is trusted).
            columns = ", ".join(_quote_identifier(field.name) for field in schema)
            outputs = [root / f"result-{index}.arrow" for index in range(len(parts))]
            self._run(
                root,
                ";".join(
                    f"SELECT {columns} FROM {_quote_identifier('packed_read')} "
                    f"WHERE _part = {_quote_string(str(part['name']))} "
                    f"ORDER BY _part_offset INTO OUTFILE {_quote_string(str(output))} "
                    "FORMAT ArrowStream"
                    for part, output in zip(parts, outputs, strict=True)
                ),
            )
            result_bytes = sum(output.stat().st_size for output in outputs)
            if result_bytes > self._max_result_bytes:
                raise ValidationError(
                    f"packed snapshot result is {result_bytes} bytes, over "
                    f"max_result_bytes {self._max_result_bytes}",
                    status_code=None,
                )
            tables: list[pa.Table] = []
            for output in outputs:
                try:
                    with (
                        pa.OSFile(str(output)) as source,
                        pa.ipc.open_stream(source) as reader,
                    ):
                        tables.append(reader.read_all().cast(schema))
                except (pa.ArrowInvalid, pa.ArrowNotImplementedError, OSError) as error:
                    raise HoglakeError(
                        "ClickHouse returned an Arrow result incompatible with the "
                        f"packed table: {error}"
                    ) from error
            result = pa.concat_tables(tables)
        expected_rows = sum(item.data_file.record_count for item in plan)
        if result.num_rows != expected_rows:
            raise HoglakeError(
                f"ClickHouse returned {result.num_rows} rows for parts registered with "
                f"{expected_rows} rows"
            )
        return result

    def _create_sql(self, info: TableInfo, name: str) -> str:
        _require_packed(info)
        columns = []
        for column in info.columns:
            clickhouse_type = _CLICKHOUSE_TYPES[column.type]
            if column.nullable:
                clickhouse_type = f"Nullable({clickhouse_type})"
            columns.append(f"{_quote_identifier(column.name)} {clickhouse_type}")
        return (
            f"CREATE TABLE {_quote_identifier(name)} ({', '.join(columns)}) "
            "ENGINE=MergeTree ORDER BY tuple() SETTINGS "
            "min_bytes_for_wide_part=0, min_rows_for_wide_part=0, "
            "min_bytes_for_full_part_storage=1000000000000000000, "
            "min_rows_for_full_part_storage=1000000000000000000, "
            "max_bytes_to_merge_at_max_space_in_pool=0"
        )

    def _single_part(self, root: Path, table: str, expected_rows: int) -> Path:
        raw = self._run(
            root,
            "SELECT name, path, rows FROM system.parts "
            f"WHERE active AND table={_quote_string(table)} FORMAT JSONEachRow",
        )
        rows = [json.loads(line) for line in raw.splitlines() if line]
        if len(rows) != 1 or int(rows[0]["rows"]) != expected_rows:
            raise HoglakeError(
                f"ClickHouse produced {len(rows)} active parts for {expected_rows} rows"
            )
        part = Path(rows[0]["path"]).resolve()
        try:
            part.relative_to(root.resolve())
        except ValueError as error:
            raise HoglakeError(
                "ClickHouse part path escaped its isolated directory"
            ) from error
        entries = list(part.iterdir())
        if (
            len(entries) != 1
            or entries[0].name != "data.packed"
            or not entries[0].is_file()
            or entries[0].is_symlink()
        ):
            names = sorted(entry.name for entry in entries)
            raise HoglakeError(
                "ClickHouse did not produce the restricted single-file packed layout; "
                f"part entries were {names}"
            )
        return part

    def _run(
        self,
        root: Path,
        sql: str,
        *,
        input_bytes: bytes | None = None,
        settings: Mapping[str, str] | None = None,
    ) -> bytes:
        extra = [
            arg
            for key, value in (settings or {}).items()
            for arg in (f"--{key}", value)
        ]
        command = [
            *self._command,
            "local",
            "--path",
            str(root),
            "--background_schedule_pool_size",
            str(self._max_threads),
            "--max_threads",
            str(self._max_threads),
            "--max_memory_usage",
            str(self._max_memory_bytes),
            "--max_result_bytes",
            str(self._max_result_bytes),
            "--result_overflow_mode",
            "throw",
            "--output_format_arrow_string_as_string",
            "0",
            *extra,
            "--query",
            sql,
        ]
        try:
            completed = subprocess.run(
                command,
                input=input_bytes,
                capture_output=True,
                check=False,
                cwd=root,
                timeout=self._timeout,
            )
        except subprocess.TimeoutExpired as error:
            raise HoglakeError(
                f"clickhouse local exceeded the {self._timeout:g}s timeout"
            ) from error
        except OSError as error:
            raise HoglakeError(
                f"could not execute clickhouse local: {error}"
            ) from error
        if completed.returncode != 0:
            detail = completed.stderr.decode("utf-8", errors="replace").strip()
            raise HoglakeError(
                "clickhouse local failed" + (f": {detail[:2000]}" if detail else "")
            )
        return completed.stdout

    @staticmethod
    def _upload(table: Table, source: Path, uri: str) -> None:
        client = table._namespace._catalog._client
        perform_upload(
            client._filesystem(),
            client._put_client(1),
            Upload(uri=uri, size=source.stat().st_size, path=str(source)),
        )

    @staticmethod
    def _download(
        table: Table,
        uri: str,
        destination: Path,
        expected_size: int,
    ) -> None:
        filesystem = table._namespace._catalog._client._filesystem()
        copied = 0
        with (
            filesystem.open_input_stream(s3_key(uri)) as source,
            destination.open("wb") as sink,
        ):
            while copied <= expected_size:
                chunk = source.read(min(8 * 1024 * 1024, expected_size + 1 - copied))
                if not chunk:
                    break
                sink.write(chunk)
                copied += len(chunk)
        if copied > expected_size:
            destination.unlink(missing_ok=True)
            raise HoglakeError(
                f"packed object {uri} exceeds its registered size {expected_size}"
            )

    @staticmethod
    def _abandon(
        table: Table,
        owner: uuid.UUID,
        paths: list[str],
        *,
        best_effort: bool = False,
    ) -> int:
        if not paths:
            return 0
        catalog = table._namespace._catalog
        try:
            body = catalog._client._request(
                "POST",
                catalog._path("/uploads/abandon"),
                json={"owner": str(owner), "paths": paths},
            )
        except HoglakeError:
            if best_effort:
                return 0
            raise
        return int(body["abandoned"])

    @staticmethod
    def _validate_payload(table: Table, payload: Mapping[str, Any]) -> None:
        try:
            operation = uuid.UUID(str(payload["idempotency_key"]))
            appends = payload["appends"]
            append = appends[0]
            files = append["files"]
        except (KeyError, IndexError, TypeError, ValueError) as error:
            raise ValueError("invalid packed prepared payload") from error
        if (
            len(appends) != 1
            or len(files) != 1
            or append.get("namespace") != table.namespace
            or append.get("table") != table.name
            or append.get("expected_table_uuid") != table.table_uuid
            or files[0].get("file_format") != CLICKHOUSE_MERGETREE_PACKED_FORMAT
            or payload.get("read_snapshot") is None
            or str(operation) != str(payload["idempotency_key"])
        ):
            raise ValueError("invalid packed prepared payload")
