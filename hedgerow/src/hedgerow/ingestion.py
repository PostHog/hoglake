"""Buffered discovery and durable publication using the native DuckDB writer.

Discovery reads routing columns with Arrow. Flush payloads stay in DuckDB.
The buffered CLI owns service lifecycle; immutable raw files remain as backups.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from dataclasses import replace
from tempfile import TemporaryDirectory

import pyarrow.parquet as pq

from .buffering import BufferPolicy
from .discovery import discover_window
from .duckdb_writer import (
    DuckDBFragment,
    DuckDBWriterOptions,
    write_duckdb_event_partition,
)
from .events import EventTransform
from .halts import DataIntegrityError, IncarnationChangedError, SplitBrainError
from .pending import PendingStore, Work
from .scheduler import FlushScheduler
from .window import plan_window


def _table_identity(info) -> dict:
    """The job-defining shape of a table, as an EXPLICIT projection.

    Durable job identity is compared byte-for-byte at startup: a
    coordinator whose stored identity no longer matches refuses to open
    its pending store, and the claimed work has to be reconciled by hand.
    So the identity must change when the job really changes, and must NOT
    change for any other reason.

    This was `asdict(info)` with the three aggregate counts popped, which
    made identity a function of pyhoglake's dataclass SHAPE: adding a
    field to TableInfo, Column, PartitionField or SortField — or adopting
    a new server field into one — silently re-wrote the identity of every
    running job, and a routine library bump landed as a fleet-wide
    startup halt with no code change here at all.

    The fields below are exactly those `_guard` treats as job-defining
    (table_uuid, columns, partition spec, sort spec); the two must agree,
    or a change would either halt startup without tripping the guard or
    trip the guard without changing identity. Everything else about a
    table — its name, namespace, and the aggregate counts that move on
    every append — is deliberately absent.

    Adding a field here is a breaking change for every deployed
    coordinator, so it is a decision, not a consequence of an upstream
    edit. `test_job_identity` pins that by adding a synthetic field to
    the dataclasses and asserting the identity is unchanged.
    """
    return {
        "table_uuid": info.table_uuid,
        "columns": [
            {
                "field_id": c.field_id,
                "name": c.name,
                "type": c.type,
                "nullable": c.nullable,
                "type_params": c.type_params or {},
            }
            # Ordinal is the wire order; identity sorts by field id so a
            # pure reorder is not mistaken for a schema change.
            for c in sorted(info.columns, key=lambda c: c.field_id)
        ],
        "partition_spec": _partition_identity(info.partition_spec),
        "sort_spec": _sort_identity(info.sort_spec),
    }


def _partition_identity(spec) -> dict | None:
    if spec is None:
        return None
    return {
        "spec_id": spec.spec_id,
        # Partition field ORDER is semantic (it is the directory nesting),
        # so this list is not sorted.
        "fields": [
            {
                "source_field_id": f.source_field_id,
                "transform": f.transform,
                "transform_param": f.transform_param,
            }
            for f in spec.fields
        ],
    }


def _sort_identity(spec) -> dict | None:
    if spec is None:
        return None
    return {
        "sort_id": spec.sort_id,
        # Sort field order is semantic too: it is the sort key sequence.
        "fields": [
            {
                "source_field_id": f.source_field_id,
                "direction": f.direction,
                "null_order": f.null_order,
            }
            for f in spec.fields
        ],
    }


class BufferedIngestion:
    def __init__(
        self,
        source,
        destination,
        *,
        source_catalog,
        destination_catalog,
        consumer_id: str,
        state_path: str,
        filesystem,
        spill_directory: str,
        policy: BufferPolicy | None = None,
        start_snapshot: int = 0,
        max_snapshot_window: int = 1000,
        json_columns: Sequence[str] = (),
        writer_options: DuckDBWriterOptions | None = None,
        configure_duckdb: Callable | None = None,
    ):
        policy = policy or BufferPolicy()
        self.source = source
        self.destination = destination
        self.source_catalog = source_catalog
        self.destination_catalog = destination_catalog
        self.consumer_id = consumer_id
        self.filesystem = filesystem
        self.spill_directory = spill_directory
        self.policy = policy
        self.max_snapshot_window = max_snapshot_window
        self.source_info = source.info()
        self.destination_info = destination.info()
        self.transform = EventTransform(
            self.source_info.columns,
            self.destination_info,
            json_columns=tuple(json_columns),
        )
        self.writer_options = replace(
            writer_options or DuckDBWriterOptions(),
            target_file_bytes=policy.target_file_bytes,
        )
        self.configure_duckdb = configure_duckdb
        if not source_catalog.options().consumer_floor:
            raise DataIntegrityError(
                "buffered ingestion requires source consumer_floor retention protection"
            )
        identity = {
            "source": _table_identity(self.source_info),
            "destination": _table_identity(self.destination_info),
            "consumer": consumer_id,
            "writer": "duckdb-v1",
            "json_columns": sorted(self.transform.json_columns),
        }
        self.store = PendingStore(state_path, identity, start_snapshot)
        self.scheduler = FlushScheduler(
            self.store, policy, self._prepare, destination_catalog.commit_prepared
        )

    def _open_parquet(self, path):
        if not path.startswith("s3://"):
            raise DataIntegrityError("raw input must use S3")
        return pq.ParquetFile(path.removeprefix("s3://"), filesystem=self.filesystem)

    def _guard(self):
        for table, pinned in (
            (self.source, self.source_info),
            (self.destination, self.destination_info),
        ):
            current = table.info()
            if current.table_uuid != pinned.table_uuid:
                raise IncarnationChangedError("buffered ingestion table was recreated")
            if (current.columns, current.partition_spec, current.sort_spec) != (
                pinned.columns,
                pinned.partition_spec,
                pinned.sort_spec,
            ):
                raise DataIntegrityError(
                    "buffered ingestion schema/partition/sort changed; reconcile pending state before restarting"
                )
        if not self.source_catalog.options().consumer_floor:
            raise DataIntegrityError("source consumer_floor was disabled")

    def _offset(self):
        desired = self.store.published_through
        offset = self.source_catalog.offset(
            self.consumer_id, self.source_info.table_uuid
        )
        if offset is not None and offset.committed_snapshot > desired:
            raise SplitBrainError("source offset is ahead of durable publication state")
        if offset is None or offset.committed_snapshot < desired:
            self.source_catalog.commit_offset(
                self.consumer_id, self.source_info.table_uuid, desired
            )

    def run_once(self, now: float | None = None):
        self._guard()
        self._offset()  # establish retention pin BEFORE reading any source files
        self.scheduler.tick(time.time() if now is None else now)
        window = plan_window(
            self.store.discovered,
            self.source_catalog.refresh().head_snapshot_id,
            self.max_snapshot_window,
        )
        result = None
        if window is not None:
            plan = self.source.changes(window.from_snapshot, window.to_snapshot)
            if (plan.from_snapshot, plan.to_snapshot) != (
                window.from_snapshot,
                window.to_snapshot,
            ):
                raise DataIntegrityError("source returned a different discovery window")
            times = {}
            for snapshot in self.source_catalog.snapshots(after=window.from_snapshot):
                if snapshot.snapshot_id > window.to_snapshot:
                    break
                times[snapshot.snapshot_id] = snapshot.snapshot_time
            result = discover_window(
                self.store,
                plan,
                self.source_info.table_uuid,
                times,
                self.transform,
                self._open_parquet,
                max_fragments=self.policy.max_fragments_per_window,
            )
        self.scheduler.tick(time.time() if now is None else now)
        self._offset()
        return result

    def _prepare(self, work: Work):
        with TemporaryDirectory(
            prefix=f"flush-{work.work_id}-", dir=self.spill_directory
        ) as directory:
            by_id = {c.field_id: c.name for c in self.destination_info.columns}
            month_index = next(
                i
                for i, field in enumerate(self.destination_info.partition_spec.fields)
                if field.transform == "month"
            )
            team_index = next(
                i
                for i, field in enumerate(self.destination_info.partition_spec.fields)
                if by_id[field.source_field_id] == "team_id"
            )
            if work.partition[team_index] != str(work.team_id):
                raise DataIntegrityError(
                    "frozen partition disagrees with team identity"
                )
            paths = write_duckdb_event_partition(
                [
                    DuckDBFragment(f["path"], tuple(f["row_groups"]), f["rows"])
                    for f in work.fragments
                ],
                directory,
                team_id=work.team_id,
                month=int(work.partition[month_index]),
                field_ids={c.name: c.field_id for c in self.destination_info.columns},
                json_columns=self.transform.json_columns,
                options=self.writer_options,
                configure_connection=self.configure_duckdb,
            )
            return self.destination.prepare_append_files(
                [(str(path), work.partition) for path in paths],
                idempotency_key=work.work_id,
                expected_table_uuid=self.destination_info.table_uuid,
                expected_table_info=self.destination_info,
                allow_optional_fields=True,
            )

    def close(self):
        self.scheduler.close()
        self.store.close()
