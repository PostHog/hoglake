"""Real CLI processes, S3 reads/uploads, VARIANT publication and restart."""

import json
import subprocess
import sys
import uuid
from datetime import UTC, datetime

import duckdb
import pyarrow as pa
import pytest
import yaml
from conftest import S3_ACCESS_KEY, S3_ENDPOINT, S3_SECRET_KEY
from pyhoglake import HoglakeClient, S3Config, ops

from hedgerow.buffered_service import configure_s3
from hedgerow.config import S3Settings

pytestmark = pytest.mark.integration


def test_buffered_cli_restart_publishes_native_variant(live_server_url, tmp_path):
    run = uuid.uuid4().hex[:10]
    s3 = S3Config(
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
        endpoint_override=S3_ENDPOINT,
        region="us-east-1",
        allow_bucket_creation=True,
    )
    filesystem = s3.filesystem()
    filesystem.create_dir("hedgerow-buffered-itest")
    prefix = f"s3://hedgerow-buffered-itest/{run}"
    with HoglakeClient(live_server_url, s3=s3) as client:
        source_catalog = client.create_catalog(f"hedgerow-{run}-src", prefix + "/src")
        source_catalog.set_retention(None, consumer_floor=True)
        destination_catalog = client.create_catalog(
            f"hedgerow-{run}-dst", prefix + "/dst"
        )
        source_ns = source_catalog.create_namespace("ns")
        destination_ns = destination_catalog.create_namespace("ns")
        schema = pa.schema(
            [
                pa.field("team_id", pa.int64(), nullable=False),
                pa.field("timestamp", pa.timestamp("us", "UTC"), nullable=False),
                pa.field("event", pa.string(), nullable=False),
                pa.field("uuid", pa.uuid(), nullable=False),
                pa.field("properties", pa.string(), nullable=False),
            ]
        )
        source = source_ns.create_table("raw_events", schema)
        destination = destination_ns.create_table(
            "events",
            pa.schema(
                list(schema)[:-1]
                + [pa.field("event_date", pa.date32(), nullable=False)]
            ),
        )
        destination.alter([ops.add_column("properties", "variant", nullable=False)])
        ids = {c.name: c.field_id for c in destination.info().columns}
        destination.alter(
            [
                ops.set_partition_spec(
                    [
                        {"source_field_id": ids["team_id"], "transform": "identity"},
                        {"source_field_id": ids["timestamp"], "transform": "month"},
                    ]
                ),
                ops.AlterOp(
                    "set_sort_order",
                    {
                        "sort_fields": [
                            {
                                "source_field_id": ids[n],
                                "direction": "asc",
                                "null_order": "nulls_first",
                            }
                            for n in ("event_date", "event", "timestamp", "uuid")
                        ]
                    },
                ),
            ]
        )
        rows = [
            {
                "team_id": team,
                "timestamp": datetime(2026, month, 1, tzinfo=UTC),
                "event": "event",
                "uuid": uuid.UUID(int=i).bytes,
                "properties": json.dumps(
                    {"large": 9007199254740993, "nested": [i, None]}
                ),
            }
            for i, (team, month) in enumerate([(1, 1), (1, 2), (2, 1)])
        ]
        source.append(pa.Table.from_pylist(rows, schema=schema))
        source_files = source.scan_plan()
        settings = {
            "endpoint": S3_ENDPOINT,
            "access_key": S3_ACCESS_KEY,
            "secret_key": S3_SECRET_KEY,
        }
        config = {
            "mode": "buffered",
            "source": {
                "url": live_server_url,
                "catalog": source_catalog.name,
                "namespace": "ns",
                "table": "raw_events",
                "consumer_id": "buffered-live",
                "s3": settings,
            },
            "destination": {
                "url": live_server_url,
                "catalog": destination_catalog.name,
                "namespace": "ns",
                "table": "events",
                "s3": settings,
            },
            "buffered": {
                "state_path": str(tmp_path / "state.sqlite"),
                "spill_directory": str(tmp_path / "scratch"),
                "policy": {"workers": 1},
            },
        }
        path = tmp_path / "buffered.yaml"

        def run_cli(lose_reply=False):
            path.write_text(yaml.safe_dump(config))
            command = [sys.executable, "-m", "hedgerow.cli"]
            if lose_reply:
                command = [
                    sys.executable,
                    "-c",
                    """
import sys
from pyhoglake.client import Catalog
from hedgerow.cli import main
original = Catalog.commit_prepared
def lose_response(self, request):
    original(self, request)
    raise TimeoutError('injected lost response after server commit')
Catalog.commit_prepared = lose_response
raise SystemExit(main(sys.argv[1:]))
""",
                ]
            result = subprocess.run(
                command + ["--config", str(path), "--once"],
                capture_output=True,
                text=True,
                timeout=120,
                check=False,
            )
            assert result.returncode == (9 if lose_reply else 0), result.stderr

        run_cli()  # discovery only: young data remains buffered on disk
        assert destination.info().record_count == 0
        config["buffered"]["policy"]["target_file_bytes"] = 1
        run_cli(
            lose_reply=True
        )  # real server committed; client did not receive success
        assert destination.info().record_count == 1
        run_cli()  # exact receipt retry, then remaining ready partitions
        assert destination.info().record_count == 3
        published = destination.scan_plan()
        run_cli()  # another restart cannot duplicate the publication
        assert destination.info().record_count == 3
        assert (
            source_catalog.offset("buffered-live", source.table_uuid).committed_snapshot
            == source_catalog.refresh().head_snapshot_id
        )
        with duckdb.connect() as connection:
            configure_s3(
                S3Settings(
                    endpoint=S3_ENDPOINT,
                    access_key=S3_ACCESS_KEY,
                    secret_key=S3_SECRET_KEY,
                )
            )(connection)
            connection.execute("SET TimeZone='UTC'")
            actual = connection.execute(
                "SELECT uuid::VARCHAR, properties::JSON::VARCHAR FROM read_parquet(?) ORDER BY uuid",
                [[f.data_file.path for f in published]],
            ).fetchall()
            assert [(u, json.loads(p)) for u, p in actual] == [
                (str(uuid.UUID(bytes=r["uuid"])), json.loads(r["properties"]))
                for r in rows
            ]
        for file in source_files:
            assert (
                filesystem.get_file_info(file.data_file.path.removeprefix("s3://")).size
                > 0
            )
