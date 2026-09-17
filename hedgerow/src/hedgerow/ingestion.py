"""Buffered ingestion coordinator, exposed as a library while VARIANT is pending.

The CLI deliberately continues to run direct replication. This coordinator is
not a production raw_events mode yet: native VARIANT catalog/publication support
must be implemented before enabling it there. Raw files remain as backups.
"""

from __future__ import annotations

import time
from dataclasses import asdict
from tempfile import TemporaryDirectory

import pyarrow.compute as pc
import pyarrow.parquet as pq

from .buffering import BufferPolicy
from .discovery import discover_window
from .events import EventTransform
from .halts import DataIntegrityError, IncarnationChangedError, SplitBrainError
from .pending import PendingStore, Work
from .scheduler import FlushScheduler
from .sorted_writer import write_sorted_partition
from .window import plan_window


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
        self.transform = EventTransform(self.source_info.columns, self.destination_info)
        if not source_catalog.options().consumer_floor:
            raise DataIntegrityError(
                "buffered ingestion requires source consumer_floor retention protection"
            )
        identity = {
            "source": asdict(self.source_info),
            "destination": asdict(self.destination_info),
            "consumer": consumer_id,
        }
        # Aggregate counts change on every append and are not part of identity.
        for side in ("source", "destination"):
            for key in ("record_count", "file_count", "file_size_bytes"):
                identity[side].pop(key)
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

    def _batches(self, work: Work):
        for fragment in work.fragments:
            matched = 0
            with self._open_parquet(fragment["path"]) as parquet:
                for batch in parquet.iter_batches(
                    batch_size=8192,
                    row_groups=fragment["row_groups"],
                    columns=list(self.transform.read_columns),
                ):
                    output = self.transform.apply(batch)
                    mask = pc.equal(output.column("team_id"), work.team_id)
                    for array, value in zip(
                        self.transform.partition_arrays(output),
                        work.partition,
                        strict=True,
                    ):
                        comparison = (
                            pc.is_null(array)
                            if value is None
                            else pc.equal(array, value)
                        )
                        mask = pc.and_kleene(mask, comparison)
                    selected = output.filter(mask)
                    matched += selected.num_rows
                    if selected.num_rows:
                        yield selected
            if matched != fragment["rows"]:
                raise DataIntegrityError(
                    "flush row count differs from discovered source fragment"
                )

    def _prepare(self, work: Work):
        with TemporaryDirectory(
            prefix=f"flush-{work.work_id}-", dir=self.spill_directory
        ) as directory:
            paths = write_sorted_partition(
                self._batches(work),
                self.transform.schema,
                directory,
                target_bytes=self.policy.target_file_bytes,
                sort_columns=self.transform.sort_columns,
            )
            return self.destination.prepare_append_files(
                [(str(path), work.partition) for path in paths],
                idempotency_key=work.work_id,
                expected_table_uuid=self.destination_info.table_uuid,
                expected_table_info=self.destination_info,
            )

    def close(self):
        self.scheduler.close()
        self.store.close()
