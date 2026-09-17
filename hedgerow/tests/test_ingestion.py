import json
import time
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyhoglake.models import ChangesPlan, Column, DataFile
from pyhoglake.parquet_schema import validate_variant_file
from test_events_discovery import layout, raw

from hedgerow.buffering import BufferPolicy
from hedgerow.ingestion import BufferedIngestion


class Catalog:
    def __init__(self):
        self.head_snapshot_id = 1
        self.committed = None
        self.receipts = {}
        self.calls = []

    def refresh(self):
        return self

    def options(self):
        return SimpleNamespace(consumer_floor=True)

    def offset(self, *args):
        return (
            None
            if self.committed is None
            else SimpleNamespace(committed_snapshot=self.committed)
        )

    def commit_offset(self, consumer, uuid, snapshot):
        assert self.committed is None or snapshot >= self.committed
        self.committed = snapshot

    def snapshots(self, after):
        return iter(
            [
                SimpleNamespace(
                    snapshot_id=i, snapshot_time=datetime(2026, 9, 1, tzinfo=UTC)
                )
                for i in range(after + 1, self.head_snapshot_id + 1)
            ]
        )

    def commit_prepared(self, request):
        self.calls.append(request)
        key = request["idempotency_key"]
        if key not in self.receipts:
            self.receipts[key] = request
            raise TimeoutError("response lost after publication")
        assert self.receipts[key] == request


@pytest.mark.parametrize("payload", [None, "json", "variant"])
@pytest.mark.parametrize("date_month", [False, True])
def test_coordinator_accumulates_windows_and_recovers_ambiguous_publication(
    tmp_path, payload, date_month
):
    from dataclasses import replace

    transform = layout()
    if date_month:
        from pyhoglake.models import PartitionField, PartitionSpec

        transform = replace(
            transform,
            destination=replace(
                transform.destination,
                partition_spec=PartitionSpec(
                    1, (PartitionField(5, "month"), PartitionField(1, "identity"))
                ),
            ),
        )
    path = tmp_path / "raw.parquet"
    data = raw()
    if payload is None:
        # Existing scalar coordinators may preserve string UUIDs verbatim.
        data = data.set_column(
            3, "uuid", pa.array([str(uuid.UUID(int=i)) for i in range(3)])
        )
        transform = replace(
            transform,
            source_columns=tuple(
                replace(c, type="string") if c.name == "uuid" else c
                for c in transform.source_columns
            ),
            destination=replace(
                transform.destination,
                columns=tuple(
                    replace(c, type="string") if c.name == "uuid" else c
                    for c in transform.destination.columns
                ),
            ),
        )
    if payload:
        data = data.append_column(
            "properties", pa.array(['{"nested":[1,null],"large":9007199254740993}'] * 3)
        )
        transform = replace(
            transform,
            source_columns=transform.source_columns
            + (
                Column(
                    "properties",
                    "string" if payload == "json" else "variant",
                    6,
                    5,
                    False,
                ),
            ),
            destination=replace(
                transform.destination,
                columns=transform.destination.columns
                + (Column("properties", "variant", 6, 5, False),),
            ),
            json_columns=("properties",) if payload == "json" else (),
        )
    pq.write_table(data, path, row_group_size=2)
    if payload == "variant":
        native = tmp_path / "native.parquet"
        with duckdb.connect() as conn:
            conn.execute(
                "COPY (SELECT * REPLACE (properties::JSON::VARIANT AS properties) FROM read_parquet($src)) TO $dst (FORMAT PARQUET)",
                {"src": str(path), "dst": str(native)},
            )
        path = native
    paths = [tmp_path / f"raw-{i}.parquet" for i in (1, 2)]
    for output in paths:
        output.write_bytes(path.read_bytes())
    files = [
        DataFile(i, str(output), "parquet", 3, output.stat().st_size, 0, "ready", i)
        for i, output in enumerate(paths, 1)
    ]
    source_info = replace(
        transform.destination,
        name="raw_events",
        table_uuid="source",
        columns=transform.source_columns,
        partition_spec=None,
        sort_spec=None,
    )
    source = SimpleNamespace(
        info=lambda: source_info,
        changes=lambda start, end: ChangesPlan(
            "source",
            start,
            end,
            tuple(f for f in files if start < f.begin_snapshot <= end),
        ),
    )
    prepared_rows = []

    def prepare(files, **kwargs):
        rows = sum(pq.ParquetFile(path).metadata.num_rows for path, partition in files)
        for path, partition in files:
            with pq.ParquetFile(path) as parquet:
                validate_variant_file(path, parquet, transform.destination.columns)
            with duckdb.connect() as conn:
                conn.execute("SET TimeZone='UTC'")
                result = conn.execute(
                    "SELECT uuid::VARCHAR, event_date::VARCHAR, (year(timestamp)-1970)*12+month(timestamp)-1 FROM read_parquet(?)",
                    [path],
                ).fetchall()
                assert all(
                    str(row[2]) == partition[0 if date_month else 1] for row in result
                )
                assert all(
                    row[0] in {str(uuid.UUID(int=i)) for i in range(3)}
                    for row in result
                )
                if payload:
                    values = conn.execute(
                        "SELECT properties::JSON::VARCHAR FROM read_parquet(?)", [path]
                    ).fetchall()
                    assert [json.loads(v[0]) for v in values] == [
                        {"nested": [1, None], "large": 9007199254740993}
                    ] * len(result)
        prepared_rows.append(rows)
        return {
            "idempotency_key": kwargs["idempotency_key"],
            "rows": rows,
            "appends": [{"expected_table_uuid": kwargs["expected_table_uuid"]}],
        }

    dest = SimpleNamespace(
        info=lambda: transform.destination, prepare_append_files=prepare
    )
    source_cat, dest_cat = Catalog(), Catalog()

    configured = 0

    def configure(connection):
        nonlocal configured
        configured += 1
        if payload == "json" and configured == 1:
            raise ConnectionError("injected writer setup failure")
        connection.execute("SET TimeZone='America/Toronto'")

    def coordinator():
        daemon = BufferedIngestion(
            source,
            dest,
            source_catalog=source_cat,
            destination_catalog=dest_cat,
            consumer_id="job",
            state_path=str(tmp_path / "state.sqlite"),
            filesystem=None,
            spill_directory=str(tmp_path),
            policy=BufferPolicy(workers=1),
            json_columns=transform.json_columns,
            configure_duckdb=configure,
        )
        daemon._open_parquet = pq.ParquetFile
        return daemon

    now = datetime(2026, 9, 1, tzinfo=UTC).timestamp()
    daemon = coordinator()
    assert daemon.run_once(now).files == 1
    source_cat.head_snapshot_id = 2
    assert daemon.run_once(now + 10).files == 1
    assert source_cat.committed == 0  # discovery is not publication
    daemon.close()
    daemon = coordinator()
    try:
        assert daemon.store.discovered == 2
        restarted = False
        deadline = time.monotonic() + 15
        while source_cat.committed != 2:
            assert time.monotonic() < deadline
            try:
                daemon.run_once(now + 86400)
            except ConnectionError:
                before = [(w.work_id, w.fragments) for w in daemon.store.recover()]
                assert source_cat.committed == 0
                daemon.close()
                daemon = coordinator()
                assert [
                    (w.work_id, w.fragments) for w in daemon.store.recover()
                ] == before
            except TimeoutError:
                if not restarted:
                    daemon.close()
                    daemon = coordinator()
                    restarted = True
            time.sleep(0.001)
        assert prepared_rows == [
            2,
            2,
            2,
        ]  # consolidated across windows, separate team/month
        assert restarted
        assert all(p.exists() for p in paths)
        assert len(dest_cat.receipts) == 3
        assert len(dest_cat.calls) == 6
        assert len(daemon.store.cleanup_candidates()) == 2
    finally:
        daemon.close()
