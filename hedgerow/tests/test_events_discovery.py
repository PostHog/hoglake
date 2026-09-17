from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyhoglake.models import (
    ChangesPlan,
    Column,
    DataFile,
    PartitionField,
    PartitionSpec,
    SortField,
    SortSpec,
    TableInfo,
)

from hedgerow.discovery import discover_window
from hedgerow.events import EventTransform
from hedgerow.halts import DataIntegrityError, SchemaMismatchError
from hedgerow.pending import PendingStore


def layout():
    columns = tuple(
        Column(name, kind, i + 1, i, False)
        for i, (name, kind) in enumerate(
            [
                ("team_id", "long"),
                ("timestamp", "timestamptz"),
                ("event", "string"),
                ("uuid", "uuid"),
                ("event_date", "date"),
            ]
        )
    )
    info = TableInfo(
        "events",
        "ns",
        "dest",
        columns,
        0,
        0,
        0,
        PartitionSpec(1, (PartitionField(1, "identity"), PartitionField(2, "month"))),
        SortSpec(1, tuple(SortField(i, "asc", "nulls_first") for i in (5, 3, 2, 4))),
    )
    return EventTransform(columns[:-1], info)


def raw():
    return pa.table(
        {
            "team_id": pa.array([1, 2, 1], pa.int64()),
            "timestamp": pa.array(
                [
                    datetime(2000, 1, 1, tzinfo=UTC),
                    datetime(2026, 1, 1, tzinfo=UTC),
                    datetime(2000, 2, 1, tzinfo=UTC),
                ],
                pa.timestamp("us", "UTC"),
            ),
            "event": ["a", "b", "c"],
            "uuid": pa.array([i.to_bytes(16, "big") for i in range(3)], pa.binary(16)),
        }
    )


def test_utc_transform_preserves_event_identity_and_epoch_month():
    transform = layout()
    batch = raw().to_batches()[0]
    output = transform.apply(batch)
    assert output.column("uuid").equals(batch.column("uuid"))
    assert output.column("event_date").to_pylist()[0].isoformat() == "2000-01-01"
    assert transform.partition_arrays(output)[1].to_pylist() == ["360", "672", "361"]
    # Instant belongs to January in UTC even when rendered in a negative timezone.
    table = raw().set_column(
        1, "timestamp", raw()["timestamp"].cast(pa.timestamp("us", "America/Toronto"))
    )
    assert (
        transform.apply(table.to_batches()[0])
        .column("event_date")
        .to_pylist()[1]
        .isoformat()
        == "2026-01-01"
    )


def test_discovery_shared_file_late_old_events_and_integrity(tmp_path):
    transform = layout()
    path = tmp_path / "raw.parquet"
    pq.write_table(raw(), path, row_group_size=2)
    file = DataFile(7, str(path), "parquet", 3, path.stat().st_size, 0, "ready", 10)
    plan = ChangesPlan("source", 0, 10, (file,))
    store = PendingStore(str(tmp_path / "state.sqlite"), {"uuid": "source"})
    try:
        result = discover_window(
            store,
            plan,
            "source",
            {10: datetime(2026, 9, 1, tzinfo=UTC)},
            transform,
            pq.ParquetFile,
        )
        assert result.fragments == 3
        assert result.read_amplification > 1
        assert (
            store.ready(datetime(2026, 9, 1, 23, tzinfo=UTC).timestamp(), 2**30) == []
        )
        assert (
            len(store.ready(datetime(2026, 9, 2, tzinfo=UTC).timestamp(), 2**30)) == 2
        )
        assert store.published_through == 9
    finally:
        store.close()


def test_routing_cap_never_checkpoints_partial_window(tmp_path):
    path = tmp_path / "raw.parquet"
    pq.write_table(raw(), path)
    file = DataFile(7, str(path), "parquet", 3, path.stat().st_size, 0, "ready", 10)
    store = PendingStore(str(tmp_path / "state.sqlite"), {})
    try:
        with pytest.raises(DataIntegrityError, match="metadata limit"):
            discover_window(
                store,
                ChangesPlan("s", 0, 10, (file,)),
                "s",
                {10: datetime.now(UTC)},
                layout(),
                pq.ParquetFile,
                max_fragments=1,
            )
        assert store.discovered == 0
    finally:
        store.close()


def test_sort_spec_required_for_compaction():
    from dataclasses import replace

    transform = layout()
    with pytest.raises(SchemaMismatchError, match="sort spec"):
        EventTransform(
            transform.source_columns, replace(transform.destination, sort_spec=None)
        )
    with pytest.raises(SchemaMismatchError, match="native VARIANT"):
        EventTransform(
            transform.source_columns,
            replace(
                transform.destination,
                columns=transform.destination.columns
                + (Column("properties", "variant", 9, 6),),
            ),
        )


@pytest.mark.parametrize(
    "partition_column, transform_name",
    [("timestamp", "month"), ("timestamp", "identity"), ("event_date", "identity")],
)
def test_discovery_and_flush_share_utc_partition_keys(
    tmp_path, partition_column, transform_name
):
    import json
    from dataclasses import replace

    transform = layout()
    info = transform.destination
    field_id = next(c.field_id for c in info.columns if c.name == partition_column)
    transform = EventTransform(
        transform.source_columns,
        replace(
            info,
            partition_spec=PartitionSpec(
                1,
                (
                    PartitionField(1, "identity"),
                    PartitionField(field_id, transform_name),
                ),
            ),
        ),
    )
    table = raw()
    table = table.set_column(
        1, "timestamp", table["timestamp"].cast(pa.timestamp("us", "America/Toronto"))
    )
    path = tmp_path / "non-utc.parquet"
    pq.write_table(table, path)
    file = DataFile(1, str(path), "parquet", 3, path.stat().st_size, 0, "ready", 1)
    store = PendingStore(str(tmp_path / "pending.sqlite"), {})
    try:
        discover_window(
            store,
            ChangesPlan("source", 0, 1, (file,)),
            "source",
            {1: datetime.now(UTC)},
            transform,
            pq.ParquetFile,
        )
        discovered = {
            tuple(json.loads(row[0]))
            for row in store.db.execute("SELECT partition_key FROM pending")
        }
        output = transform.apply(table.to_batches()[0])
        flushed = set(
            zip(
                *(a.to_pylist() for a in transform.partition_arrays(output)),
                strict=True,
            )
        )
        assert discovered == flushed
    finally:
        store.close()
