import uuid
from dataclasses import replace
from datetime import date

import duckdb
import pyarrow.parquet as pq
import pytest

from hedgerow.duckdb_writer import (
    DuckDBFragment,
    DuckDBWriterOptions,
    write_duckdb_event_partition,
)
from hedgerow.halts import DataIntegrityError

IDS = {
    "team_id": 11,
    "timestamp": 12,
    "event": 13,
    "uuid": 14,
    "properties": 15,
    "event_date": 16,
}


def raw_file(tmp_path, name="raw.parquet", count=10000):
    path = tmp_path / name
    with duckdb.connect() as c:
        c.execute(
            """COPY (SELECT (i%2)::BIGINT team_id,
            CASE WHEN i%3=0 THEN TIMESTAMPTZ '2026-02-01 00:30:00+02'
            ELSE TIMESTAMPTZ '2026-02-01 00:30:00+00' END AS timestamp,
            CASE WHEN i%7=0 THEN NULL ELSE md5(i::VARCHAR) END AS event,
            lpad(to_hex(i),32,'0')::UUID uuid,
            json_object('counter',i,'nested',{'flag':true,'null':NULL},'text','雪😀') properties
            FROM range(?) t(i)) TO ? (FORMAT PARQUET, ROW_GROUP_SIZE 2048)""",
            [str(path), count],
        )
    return path


def write(path, output, rows, groups=(0, 1, 2, 3, 4), **kwargs):
    return write_duckdb_event_partition(
        [DuckDBFragment(str(path), groups, rows)],
        str(output),
        team_id=1,
        month=672,
        field_ids=IDS,
        json_columns=["properties"],
        **kwargs,
    )


def test_read_transform_sort_and_roll_files(tmp_path):
    path = raw_file(tmp_path, count=60000)
    files = write(
        path,
        tmp_path / "output",
        10000,
        groups=tuple(range(30)),
        options=DuckDBWriterOptions(target_file_bytes=16384, row_group_rows=2048),
    )
    with duckdb.connect(config={"threads": "1"}) as c:
        c.execute("SET TimeZone='UTC'")
        # Fetch timestamps as strings; Python timezone adapters are not part of the writer.
        actual = []
        for f in files:
            actual.extend(
                c.execute(
                    "SELECT event, timestamp::VARCHAR, uuid::VARCHAR, event_date, properties::JSON::VARCHAR FROM read_parquet(?)",
                    [str(f)],
                ).fetchall()
            )
        expected = c.execute(
            """SELECT event,timestamp::VARCHAR,uuid::VARCHAR,timestamp::DATE,
            properties::VARIANT::JSON::VARCHAR FROM read_parquet(?) WHERE team_id=1 AND month(timestamp)=1
            ORDER BY timestamp::DATE NULLS FIRST,event NULLS FIRST,timestamp NULLS FIRST,uuid NULLS FIRST""",
            [str(path)],
        ).fetchall()
    assert len(files) > 1
    assert actual == expected
    assert all(row[3] == date(2026, 1, 31) for row in actual)
    schema = str(pq.ParquetFile(files[0]).schema)
    for name, field_id in IDS.items():
        assert f"field_id={field_id} {name}" in schema
    assert "properties (Variant(1))" in schema
    assert len(list((tmp_path / "output").iterdir())) == len(files)
    assert path.exists()


def test_subset_row_groups_and_multiple_files(tmp_path):
    paths = [raw_file(tmp_path, name=f"raw-{i}.parquet") for i in range(2)]
    # Odd multiples of three in [2048,4096).
    rows = sum(i % 2 == 1 and i % 3 == 0 for i in range(2048, 4096))
    files = write_duckdb_event_partition(
        [DuckDBFragment(str(p), (1,), rows) for p in paths],
        str(tmp_path / "out"),
        team_id=1,
        month=672,
        field_ids=IDS,
        json_columns=["properties"],
    )
    with duckdb.connect() as c:
        values = c.execute(
            "SELECT properties.counter::BIGINT FROM read_parquet(?)",
            [[str(f) for f in files]],
        ).fetchall()
    assert sorted(v[0] for v in values) == sorted(
        [i for i in range(2048, 4096) if i % 2 == 1 and i % 3 == 0] * 2
    )


@pytest.mark.parametrize(
    "groups,rows,error",
    [((99,), 1, "row group disappeared"), ((0,), 1, "row count differs")],
)
def test_invalid_frozen_work_leaves_no_output(tmp_path, groups, rows, error):
    path = raw_file(tmp_path)
    with pytest.raises(DataIntegrityError, match=error):
        write(path, tmp_path / "out", rows, groups)
    assert list((tmp_path / "out").iterdir()) == []
    assert path.exists()


def test_json_null_policy_and_blob_uuid(tmp_path):
    path = tmp_path / "raw.parquet"
    with duckdb.connect() as c:
        c.execute(
            "CREATE TABLE raw(team_id BIGINT, timestamp TIMESTAMP, event VARCHAR, uuid BLOB, properties JSON)"
        )
        c.execute(
            "INSERT INTO raw VALUES (1, TIMESTAMP '2026-01-01', 'e', ?, 'null')",
            [uuid.UUID(int=42).bytes],
        )
        c.execute("COPY raw TO ? (FORMAT PARQUET)", [str(path)])
    with pytest.raises(DataIntegrityError, match="top-level JSON null"):
        write(path, tmp_path / "out", 1, (0,))
    with duckdb.connect() as c:
        c.execute(
            "COPY (SELECT 1::BIGINT team_id, TIMESTAMP '2026-01-01' AS timestamp, 'e' AS event, ?::BLOB uuid, NULL::JSON properties) TO ? (FORMAT PARQUET)",
            [str(path), uuid.UUID(int=42).bytes],
        )
    files = write(path, tmp_path / "out", 1, (0,))
    with duckdb.connect() as c:
        assert c.execute(
            "SELECT uuid::VARCHAR, properties IS NULL FROM read_parquet(?)",
            [str(files[0])],
        ).fetchone() == (str(uuid.UUID(int=42)), True)


def test_escaped_paths_and_column_names(tmp_path):
    path = tmp_path / "raw'quote.parquet"
    ids = dict(IDS)
    ids['strange"payload'] = ids.pop("properties")
    with duckdb.connect() as c:
        c.execute(
            """COPY (SELECT 1::BIGINT team_id,TIMESTAMP '2026-01-01' AS timestamp,
        'e' AS event, UUID '00000000-0000-0000-0000-000000000001' uuid,
        '{"x":1}'::JSON AS "strange""payload") TO ? (FORMAT PARQUET)""",
            [str(path)],
        )
    files = write_duckdb_event_partition(
        [DuckDBFragment(str(path), (0,), 1)],
        str(tmp_path / "out'quote"),
        team_id=1,
        month=672,
        field_ids=ids,
        json_columns=['strange"payload'],
    )
    assert pq.ParquetFile(files[0]).metadata.num_rows == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"memory_bytes": 0},
        {"scratch_bytes": 0},
        {"target_file_bytes": 0},
        {"row_group_rows": 100},
    ],
)
def test_invalid_budgets(changes):
    with pytest.raises(ValueError):
        replace(DuckDBWriterOptions(), **changes)


def test_nonempty_output_is_not_overwritten(tmp_path):
    path = raw_file(tmp_path)
    output = tmp_path / "out"
    output.mkdir()
    marker = output / "existing"
    marker.write_text("keep")
    with pytest.raises(ValueError, match="empty"):
        write(path, output, 1667)
    assert marker.read_text() == "keep"


def test_native_variant_and_precise_payloads_pass_through(tmp_path):
    path = tmp_path / "native.parquet"
    with duckdb.connect() as c:
        c.execute(
            """COPY (SELECT 1::BIGINT team_id,TIMESTAMP '2026-01-01' AS timestamp,
        'e' AS event, UUID 'ffffffff-ffff-ffff-ffff-ffffffffffff' uuid,
        {'n':9007199254740993::BIGINT,'nested':[true,NULL],
         'precise':12345678901234567890.123456789012345678::DECIMAL(38,18)}::VARIANT properties,
        TIMESTAMP_NS '1969-12-31 23:59:59.999999999' nanos)
        TO ? (FORMAT PARQUET)""",
            [str(path)],
        )
    files = write_duckdb_event_partition(
        [DuckDBFragment(str(path), (0,), 1)],
        str(tmp_path / "out"),
        team_id=1,
        month=672,
        field_ids={**IDS, "nanos": 17},
    )
    with duckdb.connect() as c:
        sql = "SELECT uuid::VARCHAR,properties::VARCHAR,epoch_ns(nanos) FROM read_parquet(?)"
        assert (
            c.execute(sql, [str(files[0])]).fetchall()
            == c.execute(sql, [str(path)]).fetchall()
        )
        assert c.execute(
            "SELECT epoch_ns(nanos) FROM read_parquet(?)", [str(files[0])]
        ).fetchone() == (-1,)
    assert "properties (Variant(1))" in str(pq.ParquetFile(files[0]).schema)


def test_source_type_drift_is_not_coerced(tmp_path):
    path = raw_file(tmp_path)
    other = tmp_path / "other.parquet"
    with duckdb.connect() as c:
        c.execute(
            "COPY (SELECT * REPLACE (team_id::INTEGER AS team_id) FROM read_parquet($src)) TO $dst (FORMAT PARQUET)",
            {"src": str(path), "dst": str(other)},
        )
    with pytest.raises(DataIntegrityError, match="disagree"):
        write_duckdb_event_partition(
            [
                DuckDBFragment(str(path), (0,), 341),
                DuckDBFragment(str(other), (0,), 1667),
            ],
            str(tmp_path / "out"),
            team_id=1,
            month=672,
            field_ids=IDS,
            json_columns=["properties"],
        )
    assert list((tmp_path / "out").iterdir()) == []


@pytest.mark.parametrize(
    "field_ids,json_columns",
    [
        ({**IDS, "duplicate": 11}, ()),
        ({**IDS, "Event": 20}, ()),
        ({**IDS, "file_row_number": 20}, ()),
        (IDS, ("event",)),
        (IDS, ("missing",)),
    ],
)
def test_invalid_projection(tmp_path, field_ids, json_columns):
    with pytest.raises(ValueError):
        write_duckdb_event_partition(
            [DuckDBFragment("unused", (0,), 1)],
            str(tmp_path / "out"),
            team_id=1,
            month=672,
            field_ids=field_ids,
            json_columns=json_columns,
        )
