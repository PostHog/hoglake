"""Discover a committed window once, indexing routing columns by file/partition.

A fragment packs all matching row-group numbers into one metadata record. We do
not create a consumer per team or duplicate event payloads in the pending store.
Shared row groups imply repeated S3 reads at flush time; ``read_amplification``
reports the estimated ratio, so layouts with sparse teams are not mistaken for
an efficient team index. A hard routing-entry bound prevents metadata explosion.
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyhoglake.models import ChangesPlan

from .events import EventTransform
from .halts import DataIntegrityError, DeletesPresentError, IncarnationChangedError
from .pending import Fragment, PendingStore, SourceFile


@dataclass(frozen=True)
class DiscoveryResult:
    files: int
    fragments: int
    source_bytes: int
    selected_bytes: int

    @property
    def read_amplification(self) -> float:
        return self.selected_bytes / self.source_bytes if self.source_bytes else 0.0


def discover_window(
    store: PendingStore,
    plan: ChangesPlan,
    source_uuid: str,
    commit_times: dict[int, datetime],
    transform: EventTransform,
    open_parquet: Callable[[str], pq.ParquetFile],
    *,
    batch_rows: int = 8192,
    max_fragments: int = 1_000_000,
) -> DiscoveryResult:
    if plan.table_uuid != source_uuid:
        raise IncarnationChangedError("source incarnation changed during discovery")
    if plan.delete_files:
        raise DeletesPresentError(
            "raw source has deletion vectors; refusing to skip or reinterpret pending input"
        )
    fragments = []
    source_files = []
    source_bytes = selected_bytes = 0
    for file in plan.files:
        if file.begin_snapshot not in commit_times:
            raise DataIntegrityError(
                "source commit time missing; cannot safely anchor pending age"
            )
        committed_at = commit_times[file.begin_snapshot]
        if committed_at.tzinfo is None:
            raise DataIntegrityError("source snapshot time must carry a timezone")
        # Only routing columns are read here. Transformation validation happens
        # independently before startup, and payload columns are read on flush.
        by_id = {c.field_id: c for c in transform.destination.columns}
        routing = {"team_id"} | {
            by_id[p.source_field_id].name
            for p in transform.destination.partition_spec.fields
        }
        if "event_date" in routing:
            routing.remove("event_date")
            routing.add("timestamp")
        routes = {}
        total = 0
        with open_parquet(file.path) as parquet:
            if parquet.metadata.num_rows != file.record_count:
                raise DataIntegrityError(
                    "source footer row count differs from committed registration"
                )
            for group in range(parquet.num_row_groups):
                metadata = parquet.metadata.row_group(group)
                compressed = sum(
                    metadata.column(i).total_compressed_size
                    for i in range(metadata.num_columns)
                )
                source_bytes += compressed
                counts = defaultdict(int)
                for batch in parquet.iter_batches(
                    batch_size=batch_rows, row_groups=[group], columns=sorted(routing)
                ):
                    # Partition transforms use destination field ids but only need
                    # source values (or the derived UTC date) for those fields.
                    if "event_date" in {
                        by_id[p.source_field_id].name
                        for p in transform.destination.partition_spec.fields
                    }:
                        stamp = batch.column(batch.schema.get_field_index("timestamp"))
                        if stamp.type.tz:
                            stamp = stamp.cast(pa.timestamp("us", "UTC"))
                        batch = batch.append_column(
                            "event_date", pc.cast(stamp, pa.date32())
                        )
                    team = batch.column(batch.schema.get_field_index("team_id"))
                    if team.null_count or (
                        "timestamp" in batch.schema.names
                        and batch.column(
                            batch.schema.get_field_index("timestamp")
                        ).null_count
                    ):
                        raise DataIntegrityError("null raw routing key")
                    partitions = [
                        a.to_pylist() for a in transform.partition_arrays(batch)
                    ]
                    for index, team_id in enumerate(team.to_pylist()):
                        key = (team_id, tuple(values[index] for values in partitions))
                        if key not in counts and len(counts) >= max_fragments:
                            raise DataIntegrityError(
                                "raw routing metadata limit exceeded; discovery not checkpointed"
                            )
                        counts[key] += 1
                    total += batch.num_rows
                if sum(counts.values()) != metadata.num_rows:
                    raise DataIntegrityError("short source row-group read")
                selected_bytes += compressed * len(counts)
                for key, rows in counts.items():
                    if key not in routes:
                        if len(fragments) + len(routes) >= max_fragments:
                            raise DataIntegrityError(
                                "raw routing metadata limit exceeded; discovery not checkpointed"
                            )
                        routes[key] = [[], 0, 0]
                    route = routes[key]
                    route[0].append(group)
                    route[1] += rows
                    route[2] += math.ceil(compressed * rows / metadata.num_rows)
        if total != file.record_count:
            raise DataIntegrityError("short source file read")
        source_files.append(
            SourceFile(
                file.data_file_id,
                file.begin_snapshot,
                committed_at.timestamp(),
                file.path,
                total,
            )
        )
        for (team, partition), (groups, rows, size) in routes.items():
            fragments.append(
                Fragment(file.data_file_id, team, partition, tuple(groups), rows, size)
            )
    store.discover(plan.from_snapshot, plan.to_snapshot, source_files, fragments)
    return DiscoveryResult(
        len(source_files), len(fragments), source_bytes, selected_bytes
    )
