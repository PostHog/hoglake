"""Deterministic read-path fixture for the duckdb-client sqllogictests.

Creates (idempotently) catalog `duckext-read` with namespace ns1 and
table `points`, then drops and recreates the table and appends two
batches (two parquet files). Row ids restart at 0 for the new table
incarnation, so the sqllogictests can assert exact values.

Run with pyhoglake's environment against the live dev stack:

    cd ~/src/hoglake/pyhoglake && uv run python \
        <path-to>/duckdb-client/test/fixtures/read_fixture.py

Environment (pyhoglake conventions): HOGLAKE_URL (default
http://localhost:8080), HOGLAKE_S3_ENDPOINT (default
http://localhost:19000), HOGLAKE_S3_ACCESS_KEY / _SECRET_KEY
(default hoglake / hoglake123).
"""

import os
from datetime import UTC, datetime

import pyarrow as pa

from pyhoglake import AlreadyExistsError, HoglakeClient, NotFoundError, S3Config, ops

HOGLAKE_URL = os.environ.get("HOGLAKE_URL", "http://localhost:8080")
S3_ENDPOINT = os.environ.get("HOGLAKE_S3_ENDPOINT", "http://localhost:19000")
S3_ACCESS_KEY = os.environ.get("HOGLAKE_S3_ACCESS_KEY", "hoglake")
S3_SECRET_KEY = os.environ.get("HOGLAKE_S3_SECRET_KEY", "hoglake123")

CATALOG = "duckext-read"
BUCKET = "duckext-itest"
DATA_PATH = f"s3://{BUCKET}/read/"

SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("name", pa.string()),
        pa.field("val", pa.float64()),
        pa.field("ts", pa.timestamp("us")),
    ]
)

BATCH1 = pa.table(
    {
        "id": pa.array([1, 2, 3], pa.int64()),
        "name": ["alpha", "beta", None],
        "val": [1.5, -2.25, 42.0],
        "ts": [
            datetime(2026, 1, 1, 0, 0, 0),
            datetime(2026, 1, 2, 12, 30, 0),
            None,
        ],
    },
    schema=SCHEMA,
)

BATCH2 = pa.table(
    {
        "id": pa.array([4, 5], pa.int64()),
        "name": ["delta", "epsilon"],
        "val": [None, 0.125],
        "ts": [datetime(2026, 2, 1), datetime(2026, 2, 2)],
    },
    schema=SCHEMA,
)


def main() -> None:
    s3 = S3Config(
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
        endpoint_override=S3_ENDPOINT,
        region="us-east-1",
        allow_bucket_creation=True,
    )
    with HoglakeClient(HOGLAKE_URL, s3=s3) as client:
        fs = s3.filesystem()
        fs.create_dir(BUCKET)
        try:
            catalog = client.catalog(CATALOG)
        except NotFoundError:
            catalog = client.create_catalog(CATALOG, DATA_PATH)
        try:
            ns = catalog.create_namespace("ns1")
        except AlreadyExistsError:
            ns = catalog.namespace("ns1")
        try:
            ns.table("points").drop()
        except NotFoundError:
            pass
        table = ns.create_table("points", SCHEMA)
        r1 = table.append(BATCH1)
        r2 = table.append(BATCH2)
        print(f"fixture ready: {CATALOG}/ns1.points "
              f"snapshots {r1.snapshot_id},{r2.snapshot_id}")

        # partitioned table: identity(team) -> one file per team value
        try:
            ns.table("part_points").drop()
        except NotFoundError:
            pass
        part_schema = pa.schema(
            [
                pa.field("team", pa.string()),
                pa.field("n", pa.int64(), nullable=False),
            ]
        )
        part = ns.create_table("part_points", part_schema)
        team_field = next(c for c in part.columns if c.name == "team")
        part.alter([
            ops.set_partition_spec([ops.partition_field(team_field.field_id, "identity")])
        ])
        rp = part.append(
            pa.table(
                {
                    "team": ["a", "a", "b", "b", "c", None],
                    "n": pa.array([1, 2, 3, 4, 5, 6], pa.int64()),
                },
                schema=part_schema,
            )
        )
        print(f"fixture ready: {CATALOG}/ns1.part_points snapshot {rp.snapshot_id} "
              f"files {len(rp.files)}")


if __name__ == "__main__":
    main()
