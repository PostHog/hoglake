"""Raw event schema validation and UTC routing. No event identifiers are generated."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc
from pyhoglake import Column
from pyhoglake.models import TableInfo
from pyhoglake.transforms import transform_strings
from pyhoglake.types import columns_to_arrow_schema

from .halts import DataIntegrityError, SchemaMismatchError
from .projection import validate_projection
from .sorted_writer import EVENT_SORT


@dataclass(frozen=True)
class EventTransform:
    source_columns: tuple[Column, ...]
    destination: TableInfo
    sort_columns: tuple[str, ...] = EVENT_SORT

    def __post_init__(self):
        source = {c.name: c for c in self.source_columns}
        dest = {c.name: c for c in self.destination.columns}
        if any(c.type == "variant" for c in self.destination.columns):
            raise SchemaMismatchError(
                "native VARIANT requires a catalog/client/reader codec; JSON is not VARIANT. "
                "This runtime cannot yet publish native VARIANT event columns."
            )
        required = ("team_id", "timestamp", "event", "uuid")
        if any(name not in source or name not in dest for name in required):
            raise SchemaMismatchError(
                "raw/events schemas require team_id, timestamp, event, uuid"
            )
        if source["team_id"].type not in ("int", "long") or source["team_id"].nullable:
            raise SchemaMismatchError("raw team_id must be a non-null int or long")
        if (
            source["timestamp"].type not in ("timestamp", "timestamptz")
            or source["timestamp"].nullable
        ):
            raise SchemaMismatchError(
                "raw timestamp must be non-null microsecond timestamp (UTC)"
            )
        if source["event"].type != "string" or source["uuid"].type not in (
            "uuid",
            "string",
        ):
            raise SchemaMismatchError(
                "event must be string; uuid must preserve its source uuid/string type"
            )
        if "event_date" not in dest or dest["event_date"].type != "date":
            raise SchemaMismatchError(
                "events must contain a physical date column event_date"
            )
        derived = Column(
            "event_date",
            "date",
            dest["event_date"].field_id,
            dest["event_date"].ordinal,
            nullable=False,
        )
        validate_projection(
            [c for c in self.source_columns if c.name != "event_date"] + [derived],
            self.destination.columns,
        )
        spec = self.destination.partition_spec
        if not spec or not spec.fields:
            raise SchemaMismatchError(
                "events requires a destination partition specification"
            )
        team_field = dest["team_id"].field_id
        if not any(
            p.source_field_id == team_field and p.transform == "identity"
            for p in spec.fields
        ):
            raise SchemaMismatchError(
                "events partition specification must include identity(team_id)"
            )
        for p in spec.fields:
            if p.source_field_id not in {c.field_id for c in dest.values()}:
                raise SchemaMismatchError(
                    "destination partition references an absent column"
                )

        self.validate_sort(self.sort_columns)
        sort = self.destination.sort_spec
        expected = [
            (dest[name].field_id, "asc", "nulls_first") for name in self.sort_columns
        ]
        if (
            sort is None
            or [(f.source_field_id, f.direction, f.null_order) for f in sort.fields]
            != expected
        ):
            raise SchemaMismatchError(
                "destination sort spec must match the physical ascending event sort, uuid last"
            )

    @property
    def schema(self) -> pa.Schema:
        return columns_to_arrow_schema(self.destination.columns)

    @property
    def read_columns(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.destination.columns if c.name != "event_date")

    def apply(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        table = pa.Table.from_batches([batch])
        for name in ("team_id", "timestamp"):
            if table[name].null_count:
                raise DataIntegrityError(f"raw {name} contains nulls")
        timestamp = table["timestamp"]
        # Timestamp instant -> UTC calendar date; a non-UTC Arrow timezone must
        # not make a local midnight change the destination month/date.
        if timestamp.type.tz:
            timestamp = timestamp.cast(pa.timestamp("us", "UTC"))
        event_date = pc.cast(timestamp, pa.date32())
        if "event_date" in table.column_names:
            table = table.drop(["event_date"])
        table = table.append_column("event_date", event_date)
        table = table.select(self.schema.names).cast(self.schema)
        return table.combine_chunks().to_batches()[0]

    def partition_arrays(self, batch: pa.RecordBatch) -> list[pa.Array]:
        by_id = {c.field_id: c for c in self.destination.columns}
        return [
            transform_strings(
                p.transform,
                p.transform_param,
                batch.column(
                    batch.schema.get_field_index(by_id[p.source_field_id].name)
                ),
                by_id[p.source_field_id].type,
                by_id[p.source_field_id].type_params,
            )
            for p in self.destination.partition_spec.fields
        ]

    def validate_sort(self, columns: Sequence[str] = EVENT_SORT) -> None:
        if (
            not columns
            or columns[-1] != "uuid"
            or any(c not in self.schema.names for c in columns)
        ):
            raise SchemaMismatchError(
                "event sort must reference physical columns with uuid last"
            )
