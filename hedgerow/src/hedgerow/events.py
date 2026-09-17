"""Raw event schema validation and UTC routing. No event identifiers are generated."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace

import pyarrow as pa
from pyhoglake import Column
from pyhoglake.models import TableInfo
from pyhoglake.transforms import transform_strings

from .halts import SchemaMismatchError
from .projection import validate_projection

EVENT_SORT = ("event_date", "event", "timestamp", "uuid")


@dataclass(frozen=True)
class EventTransform:
    source_columns: tuple[Column, ...]
    destination: TableInfo
    sort_columns: tuple[str, ...] = EVENT_SORT
    json_columns: tuple[str, ...] = ()

    def __post_init__(self):
        source = {c.name: c for c in self.source_columns}
        dest = {c.name: c for c in self.destination.columns}
        if len(set(self.json_columns)) != len(self.json_columns) or any(
            name not in source
            or name not in dest
            or source[name].type not in ("json", "string")
            or dest[name].type != "variant"
            for name in self.json_columns
        ):
            raise SchemaMismatchError(
                "JSON mapping must name unique JSON/string sources and VARIANT destinations"
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
            [
                replace(c, type="variant", type_params=None)
                if c.name in self.json_columns
                else c
                for c in self.source_columns
                if c.name != "event_date"
            ]
            + [derived],
            self.destination.columns,
        )
        spec = self.destination.partition_spec
        if not spec or not spec.fields:
            raise SchemaMismatchError(
                "events requires a destination partition specification"
            )
        routes = [
            (
                next(
                    (c.name for c in dest.values() if c.field_id == p.source_field_id),
                    None,
                ),
                p.transform,
                p.transform_param,
            )
            for p in spec.fields
        ]
        if (
            len(routes) != 2
            or ("team_id", "identity", None) not in routes
            or not any(
                route in (("timestamp", "month", None), ("event_date", "month", None))
                for route in routes
            )
        ):
            raise SchemaMismatchError(
                "DuckDB events requires identity(team_id) and month(timestamp or event_date)"
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

    def partition_arrays(self, batch: pa.RecordBatch) -> list[pa.Array]:
        by_id = {c.field_id: c for c in self.destination.columns}
        arrays = []
        for field in self.destination.partition_spec.fields:
            column = by_id[field.source_field_id]
            array = batch.column(batch.schema.get_field_index(column.name))
            # Normalize routing timestamps before calendar transforms.
            if column.type == "timestamptz":
                array = array.cast(pa.timestamp("us", "UTC"))
            arrays.append(
                transform_strings(
                    field.transform,
                    field.transform_param,
                    array,
                    column.type,
                    column.type_params,
                )
            )
        return arrays

    def validate_sort(self, columns: Sequence[str] = EVENT_SORT) -> None:
        if (
            not columns
            or columns[-1] != "uuid"
            or tuple(columns) != EVENT_SORT
            or any(c not in {d.name for d in self.destination.columns} for c in columns)
        ):
            raise SchemaMismatchError(
                "event sort must reference physical columns with uuid last"
            )
