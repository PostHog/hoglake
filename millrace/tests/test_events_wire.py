"""The production payload, end to end through decode and grouping.

Every earlier fixture sent timestamps as integers or offset-carrying ISO
strings, so nothing exercised what PostHog's producers actually put on
``clickhouse_events_json``: the ``ClickHouseEvent`` struct
(posthog ``rust/common/types/src/event.rs``), whose DateTime64 fields
are ClickHouse strings with a space separator, 3 or 6 fractional
digits and NO offset (``formats.rs`` ``CH_FORMAT``), meaning UTC.
The first dev deployment dropped 100% of the topic on exactly that.

The columns are the dev chart's ``MILLRACE_TABLE_COLUMNS`` (PostHog/charts
``apps/millrace-events/values.yaml``: millpond's live events table, in its
order) and the spec is the chart's ``identity(team_id),day(timestamp)``.
A record shaped like the producer's must decode with zero drops and key
to the right partition tuple, and its ``properties`` text must fill the
operator-added VARIANT companion ``properties_sv`` with zero drops.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyhoglake import TableInfo
from pyhoglake.variant import to_python

from millrace import keyspace
from millrace.config import load_config
from millrace.consumer import classify_message
from millrace.flush import FlushContext, JsonObjectDecoder, build_flush_plan
from millrace.grouping import GroupKeyer
from millrace.stage import StagedRecord

#: Verbatim from the dev chart.
DEV_COLUMNS = (
    "uuid:uuid,event:string,properties:string,timestamp:timestamptz,team_id:long,"
    "project_id:long,distinct_id:string,elements_chain:string,created_at:timestamptz,"
    "captured_at:timestamptz,person_id:uuid,person_created_at:timestamptz,"
    "person_properties:string,person_mode:string,historical_migration:boolean,"
    "_inserted_at:timestamptz,group0_properties:string,group1_properties:string,"
    "group2_properties:string,group3_properties:string,group4_properties:string,"
    "group0_created_at:timestamptz,group1_created_at:timestamptz,"
    "group2_created_at:timestamptz,group3_created_at:timestamptz,"
    "group4_created_at:timestamptz"
)


def producer_event(**overrides: Any) -> dict[str, Any]:
    """One ``ClickHouseEvent`` as serde emits it: optional fields
    skipped when None, DateTime64 as naive ClickHouse strings."""
    event: dict[str, Any] = {
        "uuid": "01929f6a-3c5e-7b1a-9f00-2b1c3d4e5f60",
        "team_id": 2,
        "project_id": 2,
        "event": "$pageview",
        "distinct_id": "user-1",
        "properties": json.dumps({"$current_url": "https://posthog.com"}),
        "person_id": "01929f6a-0000-7000-8000-000000000001",
        "timestamp": "2026-10-13 23:34:10.123456",
        "created_at": "2026-10-13 23:34:11.456",
        "captured_at": "2026-10-13 23:34:10.200",
        "elements_chain": "",
        "person_created_at": "2026-01-01 00:00:00",
        "person_properties": "{}",
        "group0_properties": "{}",
        "group0_created_at": "2026-02-01 12:00:00.000",
        "person_mode": "full",
    }
    event.update(overrides)
    return {k: v for k, v in event.items() if v is not None}


def dev_info(*extra: dict[str, Any]) -> TableInfo:
    """The table the dev chart's config creates, parsed by the real
    config grammar (so a grammar change that alters the shape shows
    up here too), plus ``extra`` columns an operator added."""
    cfg = load_config(
        {
            "MILLRACE_KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
            "MILLRACE_KAFKA_TOPIC": "clickhouse_events_json",
            "MILLRACE_CATALOG": "millrace-dev",
            "MILLRACE_NAMESPACE": "ingest",
            "MILLRACE_TABLE": "events",
            "MILLRACE_STAGE_URL": "s3://stage-bucket/stage",
            "MILLRACE_REPLICA_COUNT": "1",
            "POD_NAME": "millrace-events-0",
            "MILLRACE_TABLE_COLUMNS": DEV_COLUMNS,
        }
    )
    specs = cfg.table_columns
    assert specs is not None and len(specs) == 26
    wire_columns = [
        {"name": spec.name, "type": spec.col_type, "field_id": i + 1, "ordinal": i}
        for i, spec in enumerate(specs)
    ]
    wire_columns += [
        {"field_id": 27 + i, "ordinal": 26 + i, **c} for i, c in enumerate(extra)
    ]
    ids = {c["name"]: c["field_id"] for c in wire_columns}
    return TableInfo.from_wire(
        {
            "name": "events",
            "namespace": "ingest",
            "table_uuid": "0b8ee9ba-79a1-4f3e-b7e5-6a0b6ab6f012",
            "columns": wire_columns,
            "read_snapshot_id": 2,
            "partition_spec": {
                "spec_id": 1,
                "fields": [
                    {"source_field_id": ids["team_id"], "transform": "identity"},
                    {"source_field_id": ids["timestamp"], "transform": "day"},
                ],
            },
        }
    )


#: 2026-10-13T23:34:10.123456Z as epoch micros.
TS_MICROS = 1_791_934_450_123_456


def stage_and_decode(event: dict[str, Any]) -> tuple[StagedRecord, dict[str, Any]]:
    info = dev_info()
    payload = json.dumps(event).encode()
    staged = classify_message(GroupKeyer(info), offset=7, value=payload)
    assert isinstance(staged, StagedRecord), staged
    return staged, JsonObjectDecoder(info.columns).decode_record(payload).values


def test_a_producer_shaped_event_stages_and_decodes() -> None:
    staged, values = stage_and_decode(producer_event())
    # day() renders as days since the epoch on the wire: 2026-10-13.
    assert staged.group == ("2", "20739")
    assert values["timestamp"] == TS_MICROS
    # Three fractional digits (CH_FORMAT) and none at all, read as UTC.
    assert values["created_at"] == 1_791_934_451_456_000
    assert values["person_created_at"] == 1_767_225_600_000_000
    assert values["group0_created_at"] == 1_769_947_200_000_000
    assert values["team_id"] == 2
    assert values["person_mode"] == "full"


def test_the_optional_fields_serde_skips_decode_as_null() -> None:
    _, values = stage_and_decode(
        producer_event(person_id=None, captured_at=None, group0_created_at=None)
    )
    for name in ("person_id", "captured_at", "group0_created_at", "_inserted_at"):
        assert values.get(name) is None, name


@pytest.mark.parametrize("person_mode", ["full", "propertyless", "force_upgrade"])
def test_every_person_mode_stages(person_mode: str) -> None:
    staged, values = stage_and_decode(producer_event(person_mode=person_mode))
    assert values["person_mode"] == person_mode
    assert staged.group[0] == "2"


def test_a_properties_sv_companion_takes_every_producer_event() -> None:
    """``properties_sv`` (a nullable variant an operator added) mapped from
    ``properties``, through the real build: no event is dropped, a valid
    text reads back as ``json.loads(properties)``, an absent one and an
    invalid one (a duplicate key) as SQL NULL."""
    info = dev_info(
        {
            "name": "properties_sv",
            "type": "variant",
            "type_params": {
                "shredding": {
                    "type": "object",
                    "fields": [{"name": "$current_url", "type": "string"}],
                }
            },
        }
    )
    events = [
        producer_event(),
        producer_event(properties=json.dumps({"$current_url": 1, "list": [None]})),
        producer_event(properties=None),
        producer_event(properties='{"a":1,"a":2}'),
    ]
    plan = build_flush_plan(
        [
            (keyspace.RowKey(("2", "20739"), i), json.dumps(e).encode())
            for i, e in enumerate(events)
        ],
        info,
        ctx=FlushContext(
            topic="clickhouse_events_json",
            slot=0,
            group=("2", "20739"),
            group_fields=("team_id", "timestamp_day"),
            trigger="size",
            read_snapshot=2,
            table_uuid=info.table_uuid,
            build="b0",
        ),
        data_path="s3://lake/prefix",
        max_files_per_commit=512,
        footer_stats_min_bytes=1,
        version="test",
        variant_sources={"properties_sv": "properties"},
    )
    assert plan.dropped_records == () and plan.records == len(events)
    (upload,) = plan.commits[0].uploads
    table = pq.read_table(pa.BufferReader(upload.body))
    (column,) = [c for c in info.columns if c.name == "properties_sv"]
    assert to_python(table.column("properties_sv"), column) == [
        json.loads(events[0]["properties"]),
        json.loads(events[1]["properties"]),
        None,
        None,
    ]
