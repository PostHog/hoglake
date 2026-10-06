import io
import json
import os
import re
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import pyarrow as pa
import pytest

from pyhoglake import (
    CLICKHOUSE_MERGETREE_PACKED_FORMAT,
    CatalogInfo,
    ClickHousePackedAdapter,
    HoglakeClient,
    TableInfo,
    ValidationError,
)
from pyhoglake.client import Catalog, Namespace, Table
from pyhoglake.models import Column

BASE = "http://hog.test"
CATALOG_WIRE = {
    "name": "cat",
    "data_path": "s3://bkt/lake",
    "head_snapshot_id": 5,
    "schema_version": 1,
}


class _Output(io.BytesIO):
    def __init__(self, files: dict[str, bytes], key: str):
        super().__init__()
        self._files = files
        self._key = key

    def close(self) -> None:
        if not self.closed:
            self._files[self._key] = self.getvalue()
        super().close()


class FakeS3:
    def __init__(self, *, fail_upload: bool = False) -> None:
        self.files: dict[str, bytes] = {}
        self.fail_upload = fail_upload

    def filesystem(self):
        return self

    def open_output_stream(self, key: str):
        if self.fail_upload:
            raise OSError(f"object store refused {key}")
        return _Output(self.files, key)

    def open_input_stream(self, key: str):
        return io.BytesIO(self.files[key])


def _columns() -> tuple[Column, ...]:
    definitions = [
        ("flag", "boolean", True),
        ("i8", "int8", False),
        ("i16", "int16", True),
        ("i32", "int", True),
        ("i64", "long", True),
        ("u8", "uint8", True),
        ("u16", "uint16", True),
        ("u32", "uint32", True),
        ("u64", "uint64", True),
        ("f32", "float", True),
        ("f64", "double", True),
        ("text", "string", True),
        ("payload", "binary", True),
        ("day", "date", True),
        ("ts_s", "timestamp_s", True),
        ("ts_ms", "timestamp_ms", True),
        ("ts_us", "timestamp", True),
        ("ts_ns", "timestamp_ns", True),
        ("ts_tz", "timestamptz", True),
    ]
    return tuple(
        Column(
            name=name,
            type=type_,
            field_id=index,
            ordinal=index - 1,
            nullable=nullable,
        )
        for index, (name, type_, nullable) in enumerate(definitions, start=1)
    )


def _table_wire(columns: tuple[Column, ...] | None = None) -> dict:
    cols = columns or _columns()
    return {
        "name": "events",
        "namespace": "ns1",
        "table_uuid": "0b8ee9ba-79a1-4f3e-b7e5-6a0b6ab6f012",
        "columns": [
            {
                "name": column.name,
                "type": column.type,
                "field_id": column.field_id,
                "ordinal": column.ordinal,
                "nullable": column.nullable,
            }
            for column in cols
        ],
        "properties": {"write.format.default": CLICKHOUSE_MERGETREE_PACKED_FORMAT},
        "record_count": 0,
        "file_count": 0,
        "file_size_bytes": 0,
        "read_snapshot_id": 5,
    }


def _table(fake_s3: FakeS3, columns: tuple[Column, ...] | None = None) -> Table:
    client = HoglakeClient(BASE, s3=fake_s3)
    catalog = Catalog(client, CatalogInfo.from_wire(CATALOG_WIRE))
    return Table(Namespace(catalog, "ns1"), TableInfo.from_wire(_table_wire(columns)))


def _data() -> pa.Table:
    columns = _columns()
    fields = []
    arrays = []
    values = {
        "flag": [True, None],
        "i8": [-8, 8],
        "i16": [-16, None],
        "i32": [-32, 32],
        "i64": [-64, 64],
        "u8": [8, None],
        "u16": [16, 17],
        "u32": [2**32 - 1, 1],
        "u64": [2**64 - 1, 1],
        "f32": [1.5, -2.5],
        "f64": [2.5, -0.0],
        "text": ["hé", None],
        "payload": [b"\x00\xff", b"x"],
        "day": [date(2026, 10, 3), None],
        "ts_s": [datetime(2026, 10, 3, 1, 2, 3), None],
        "ts_ms": [datetime(2026, 10, 3, 1, 2, 3, 456000), None],
        "ts_us": [datetime(2026, 10, 3, 1, 2, 3, 456789), None],
        "ts_tz": [datetime(2026, 10, 3, 1, 2, 3, 456789, tzinfo=UTC), None],
    }
    arrow_types = {
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
        "string": pa.string(),
        "binary": pa.binary(),
        "date": pa.date32(),
        "timestamp_s": pa.timestamp("s"),
        "timestamp_ms": pa.timestamp("ms"),
        "timestamp": pa.timestamp("us"),
        "timestamp_ns": pa.timestamp("ns"),
        "timestamptz": pa.timestamp("us", tz="UTC"),
    }
    for column in columns:
        type_ = arrow_types[column.type]
        fields.append(pa.field(column.name, type_, nullable=column.nullable))
        if column.name == "ts_ns":
            arrays.append(pa.array([1790992923456789123, None], type=type_))
        else:
            arrays.append(pa.array(values[column.name], type=type_))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def test_normal_parquet_append_refuses_a_packed_table_before_io() -> None:
    fake_s3 = FakeS3()
    table = _table(fake_s3, (_columns()[4],))
    with pytest.raises(ValidationError, match="Parquet tables only"):
        table.append(pa.table({"i64": [1]}))
    with pytest.raises(ValidationError, match="Parquet tables only"):
        table.prepare_append_files(
            [], idempotency_key="00000000-0000-0000-0000-000000000001"
        )
    with pytest.raises(ValidationError, match="Parquet tables only"):
        table.prepare_append_tables(
            [], idempotency_key="00000000-0000-0000-0000-000000000002"
        )
    assert fake_s3.files == {}
    table._namespace._catalog._client.close()


def test_create_table_sends_storage_properties(httpx_mock) -> None:
    client = HoglakeClient(BASE)
    catalog = Catalog(client, CatalogInfo.from_wire(CATALOG_WIRE))
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables",
        json=_table_wire((_columns()[4],)),
    )
    Namespace(catalog, "ns1").create_table(
        "events",
        pa.schema([pa.field("i64", pa.int64())]),
        properties={"write.format.default": CLICKHOUSE_MERGETREE_PACKED_FORMAT},
    )
    body = json.loads(httpx_mock.get_requests()[-1].content)
    assert body["properties"] == {
        "write.format.default": CLICKHOUSE_MERGETREE_PACKED_FORMAT
    }
    client.close()


def test_head_read_pins_scan_to_the_table_info_snapshot(httpx_mock) -> None:
    fake_s3 = FakeS3()
    table = _table(fake_s3, (_columns()[4],))
    table_url = f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events"
    httpx_mock.add_response(
        method="GET",
        url=f"{table_url}?totals=false",
        json=_table_wire((_columns()[4],)),
    )
    httpx_mock.add_response(
        method="GET",
        url=f"{table_url}/scan?snapshot=5",
        json=[],
    )

    result = ClickHousePackedAdapter("unused").read(table)
    assert result.num_rows == 0
    assert result.schema == pa.schema([pa.field("i64", pa.int64())])
    table._namespace._catalog._client.close()


def test_explicit_row_id_file_is_refused_from_wire(httpx_mock) -> None:
    fake_s3 = FakeS3()
    table = _table(fake_s3, (_columns()[4],))
    table_url = f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events"
    httpx_mock.add_response(
        method="GET",
        url=f"{table_url}?totals=false",
        json=_table_wire((_columns()[4],)),
    )
    httpx_mock.add_response(
        method="GET",
        url=f"{table_url}/scan?snapshot=5",
        json=[
            {
                "data_file": {
                    "data_file_id": 1,
                    "path": "s3://bkt/lake/data.packed",
                    "file_format": CLICKHOUSE_MERGETREE_PACKED_FORMAT,
                    "record_count": 1,
                    "file_size_bytes": 1,
                    "row_id_start": 0,
                    "stats_state": "provided",
                    "begin_snapshot": 5,
                    "explicit_row_ids": True,
                }
            }
        ],
    )

    with pytest.raises(ValidationError, match="explicit row-id"):
        ClickHousePackedAdapter("unused").read(table)
    table._namespace._catalog._client.close()


def test_snapshot_part_count_is_bounded_before_download(httpx_mock) -> None:
    fake_s3 = FakeS3()
    table = _table(fake_s3, (_columns()[4],))
    table_url = f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events"
    httpx_mock.add_response(
        method="GET",
        url=f"{table_url}?totals=false",
        json=_table_wire((_columns()[4],)),
    )
    files = []
    for file_id in (1, 2):
        files.append(
            {
                "data_file": {
                    "data_file_id": file_id,
                    "path": f"s3://bkt/lake/{file_id}.packed",
                    "file_format": CLICKHOUSE_MERGETREE_PACKED_FORMAT,
                    "record_count": 1,
                    "file_size_bytes": 1,
                    "row_id_start": file_id - 1,
                    "stats_state": "provided",
                    "begin_snapshot": 5,
                }
            }
        )
    httpx_mock.add_response(
        method="GET",
        url=f"{table_url}/scan?snapshot=5",
        json=files,
    )

    with pytest.raises(ValidationError, match="max_snapshot_parts"):
        ClickHousePackedAdapter("unused", max_snapshot_parts=1).read(table)
    assert fake_s3.files == {}
    table._namespace._catalog._client.close()


def test_failed_upload_is_abandoned(httpx_mock) -> None:
    class LocalPartAdapter(ClickHousePackedAdapter):
        def _run(self, root, sql, *, input_bytes=None, settings=None):
            if sql.startswith("INSERT INTO"):
                part = root / "part"
                part.mkdir()
                (part / "data.packed").write_bytes(b"packed")
            return b""

        def _single_part(self, root, table, expected_rows):
            return root / "part"

    fake_s3 = FakeS3(fail_upload=True)
    table = _table(fake_s3, (_columns()[4],))
    table_url = f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events"
    httpx_mock.add_response(
        method="GET",
        url=f"{table_url}?totals=false",
        json=_table_wire((_columns()[4],)),
    )
    httpx_mock.add_response(
        method="PUT",
        url=re.compile(f"{re.escape(BASE)}/v1/catalogs/cat/uploads/.*"),
        json={"path": "s3://bkt/lake/data/failed.packed"},
    )
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/uploads/abandon",
        json={"abandoned": 1},
    )

    with pytest.raises(OSError, match="object store refused"):
        LocalPartAdapter("unused").prepare_append(
            table, pa.table({"i64": pa.array([1], pa.int64())})
        )
    abandon = json.loads(httpx_mock.get_requests()[-1].content)
    assert abandon["paths"] == ["s3://bkt/lake/data/failed.packed"]
    table._namespace._catalog._client.close()


def test_prepared_payload_is_bound_to_its_table() -> None:
    fake_s3 = FakeS3()
    table = _table(fake_s3, (_columns()[4],))
    other_wire = _table_wire((_columns()[4],))
    other_wire["name"] = "other"
    other_wire["table_uuid"] = "11111111-1111-4111-8111-111111111111"
    other = Table(table._namespace, TableInfo.from_wire(other_wire))
    payload = {
        "idempotency_key": "00000000-0000-4000-8000-000000000001",
        "read_snapshot": 5,
        "appends": [
            {
                "namespace": table.namespace,
                "table": table.name,
                "expected_table_uuid": table.table_uuid,
                "files": [
                    {
                        "path": "s3://bkt/lake/a.packed",
                        "file_format": CLICKHOUSE_MERGETREE_PACKED_FORMAT,
                    }
                ],
            }
        ],
    }

    with pytest.raises(ValueError, match="invalid packed prepared payload"):
        ClickHousePackedAdapter("unused").commit_prepared(other, payload)
    table._namespace._catalog._client.close()


# Integration: needs a real ClickHouse (PYHOGLAKE_CLICKHOUSE); ci/live-python.sh
# provides the pinned one and fails the run on a skip.
@pytest.mark.integration
def test_real_clickhouse_packed_append_and_snapshot_read(httpx_mock) -> None:
    executable = os.environ.get("PYHOGLAKE_CLICKHOUSE")
    if not executable:
        pytest.skip("set PYHOGLAKE_CLICKHOUSE to a clickhouse executable or wrapper")
    fake_s3 = FakeS3()
    table = _table(fake_s3)
    adapter = ClickHousePackedAdapter(Path(executable), timeout=180)
    data = _data()
    table_url = f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events"
    claimed = "s3://bkt/lake/data/ns1/events/test/claimed.packed"

    httpx_mock.add_response(
        method="GET", url=f"{table_url}?totals=false", json=_table_wire()
    )

    def claim(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        assert body["file_format"] == CLICKHOUSE_MERGETREE_PACKED_FORMAT
        return httpx.Response(200, json={"path": claimed})

    httpx_mock.add_callback(
        claim,
        method="PUT",
        url=re.compile(f"{re.escape(BASE)}/v1/catalogs/cat/uploads/.*"),
    )

    payload = adapter.prepare_append(table, data)
    registration = payload["appends"][0]["files"][0]
    assert registration["path"] == claimed
    assert registration["file_format"] == CLICKHOUSE_MERGETREE_PACKED_FORMAT
    assert "footer_size" not in registration
    assert "split_offsets" not in registration
    assert registration["column_stats"] == []
    key = claimed.removeprefix("s3://")
    assert fake_s3.files[key]

    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/commit/uploads",
        json={"snapshot_id": 6, "schema_version": 1},
    )
    assert adapter.commit_prepared(table, payload).snapshot_id == 6
    commit_body = json.loads(httpx_mock.get_requests()[-1].content)
    assert commit_body == payload

    second_claimed = "s3://bkt/lake/data/ns1/events/test/second.packed"
    second_table = _table_wire()
    second_table["read_snapshot_id"] = 6
    httpx_mock.add_response(
        method="GET", url=f"{table_url}?totals=false", json=second_table
    )
    httpx_mock.add_response(
        method="PUT",
        url=re.compile(f"{re.escape(BASE)}/v1/catalogs/cat/uploads/.*"),
        json={"path": second_claimed},
    )
    second_payload = adapter.prepare_append(table, data)
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/commit/uploads",
        json={"snapshot_id": 7, "schema_version": 1},
    )
    assert adapter.commit_prepared(table, second_payload).snapshot_id == 7
    second_key = second_claimed.removeprefix("s3://")

    read_wire = _table_wire()
    read_wire["read_snapshot_id"] = 7
    httpx_mock.add_response(
        method="GET",
        url=f"{table_url}?snapshot=7&totals=false",
        json=read_wire,
    )
    httpx_mock.add_response(
        method="GET",
        url=f"{table_url}/scan?snapshot=7",
        json=[
            {
                "data_file": {
                    "data_file_id": 1,
                    "path": claimed,
                    "file_format": CLICKHOUSE_MERGETREE_PACKED_FORMAT,
                    "record_count": data.num_rows,
                    "file_size_bytes": len(fake_s3.files[key]),
                    "row_id_start": 0,
                    "stats_state": "provided",
                    "begin_snapshot": 6,
                }
            },
            {
                "data_file": {
                    "data_file_id": 2,
                    "path": second_claimed,
                    "file_format": CLICKHOUSE_MERGETREE_PACKED_FORMAT,
                    "record_count": data.num_rows,
                    "file_size_bytes": len(fake_s3.files[second_key]),
                    "row_id_start": data.num_rows,
                    "stats_state": "provided",
                    "begin_snapshot": 7,
                }
            },
        ],
    )
    expected = pa.concat_tables([data, data])
    assert adapter.read(table, snapshot=7).equals(expected)
    table._namespace._catalog._client.close()


# Integration: needs a real ClickHouse (PYHOGLAKE_CLICKHOUSE); ci/live-python.sh
# provides the pinned one and fails the run on a skip.
@pytest.mark.integration
def test_real_clickhouse_packed_multi_part_numeric_ordering(httpx_mock) -> None:
    executable = os.environ.get("PYHOGLAKE_CLICKHOUSE")
    if not executable:
        pytest.skip("set PYHOGLAKE_CLICKHOUSE to a clickhouse executable or wrapper")
    fake_s3 = FakeS3()
    table = _table(fake_s3)
    adapter = ClickHousePackedAdapter(Path(executable), timeout=180)
    table_url = f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events"

    schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
    table_wire = _table_wire()
    table_wire["columns"] = [
        {"field_id": 1, "name": "id", "type": "long", "nullable": False, "ordinal": 0}
    ]
    table_wire["read_snapshot_id"] = 12

    # Produce 12 independent single-row parts
    scan_entries = []
    expected_tables = []
    for i in range(1, 13):
        row_table = pa.table({"id": pa.array([i], pa.int64())}, schema=schema)
        expected_tables.append(row_table)
        claimed_uri = f"s3://bkt/lake/data/ns1/events/test/part_{i}.packed"

        httpx_mock.add_response(
            method="GET", url=f"{table_url}?totals=false", json=table_wire
        )
        httpx_mock.add_response(
            method="PUT",
            url=re.compile(f"{re.escape(BASE)}/v1/catalogs/cat/uploads/.*"),
            json={"path": claimed_uri},
        )
        _ = adapter.prepare_append(table, row_table)
        part_key = claimed_uri.removeprefix("s3://")
        scan_entries.append(
            {
                "data_file": {
                    "data_file_id": i,
                    "path": claimed_uri,
                    "file_format": CLICKHOUSE_MERGETREE_PACKED_FORMAT,
                    "record_count": 1,
                    "file_size_bytes": len(fake_s3.files[part_key]),
                    "row_id_start": i - 1,
                    "stats_state": "provided",
                    "begin_snapshot": i,
                }
            }
        )

    read_wire = dict(table_wire)
    read_wire["read_snapshot_id"] = 12
    httpx_mock.add_response(
        method="GET",
        url=f"{table_url}?snapshot=12&totals=false",
        json=read_wire,
    )
    httpx_mock.add_response(
        method="GET",
        url=f"{table_url}/scan?snapshot=12",
        json=scan_entries,
    )

    result = adapter.read(table, snapshot=12)
    expected = pa.concat_tables(expected_tables)
    assert result.column("id").to_pylist() == list(range(1, 13))
    assert result.equals(expected)
    table._namespace._catalog._client.close()


@pytest.mark.integration
def test_live_server_claim_commit_and_multi_part_snapshot_read(live_server_url) -> None:
    executable = os.environ.get("PYHOGLAKE_CLICKHOUSE")
    if not executable:
        pytest.skip("set PYHOGLAKE_CLICKHOUSE to a clickhouse executable or wrapper")
    from pyhoglake import S3Config

    suffix = os.urandom(6).hex()
    catalog_name = f"packed-it-{suffix}"
    bucket = "pyhoglake-packed-itest"
    s3 = S3Config(
        access_key=os.environ.get("HOGLAKE_S3_ACCESS_KEY", "hoglake"),
        secret_key=os.environ.get("HOGLAKE_S3_SECRET_KEY", "hoglake123"),
        endpoint_override=os.environ.get(
            "HOGLAKE_S3_ENDPOINT", "http://localhost:9000"
        ),
        region="us-east-1",
        allow_bucket_creation=True,
    )
    s3.filesystem().create_dir(bucket)
    with HoglakeClient(live_server_url, s3=s3) as client:
        catalog = client.create_catalog(catalog_name, f"s3://{bucket}/{catalog_name}/")
        namespace = catalog.create_namespace("ns")
        schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("name", pa.string()),
                pa.field("payload", pa.binary()),
                pa.field("at", pa.timestamp("ns")),
            ]
        )
        table = namespace.create_table(
            "events",
            schema,
            properties={"write.format.default": CLICKHOUSE_MERGETREE_PACKED_FORMAT},
        )
        adapter = ClickHousePackedAdapter(Path(executable), timeout=180)
        first = pa.table(
            {
                "id": pa.array([1, 2], pa.int64()),
                "name": ["a", None],
                "payload": pa.array([b"x", b"\x00\xff"], pa.binary()),
                "at": pa.array([1000000001, 1000000002], pa.timestamp("ns")),
            },
            schema=schema,
        )
        second = pa.table(
            {
                "id": pa.array([3], pa.int64()),
                "name": ["c"],
                "payload": pa.array([b"z"], pa.binary()),
                "at": pa.array([1000000003], pa.timestamp("ns")),
            },
            schema=schema,
        )
        snapshot_one = adapter.append(table, first).snapshot_id
        snapshot_two = adapter.append(table, second).snapshot_id

        assert adapter.read(table, snapshot=snapshot_one).equals(first)
        assert adapter.read(table, snapshot=snapshot_two).equals(
            pa.concat_tables([first, second])
        )
        files = table.files(snapshot=snapshot_two)
        assert [file.file_format for file in files] == [
            CLICKHOUSE_MERGETREE_PACKED_FORMAT,
            CLICKHOUSE_MERGETREE_PACKED_FORMAT,
        ]
