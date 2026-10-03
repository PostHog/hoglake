"""Create the two packed-format tables used by hoglake_packed_format.test."""

import os
import uuid

import pyarrow as pa

from pyhoglake import AlreadyExistsError, HoglakeClient, NotFoundError, S3Config

HOGLAKE_URL = os.environ.get("HOGLAKE_URL", "http://localhost:8080")
S3_ENDPOINT = os.environ.get("HOGLAKE_S3_ENDPOINT", "http://localhost:19000")
S3_ACCESS_KEY = os.environ.get("HOGLAKE_S3_ACCESS_KEY", "hoglake")
S3_SECRET_KEY = os.environ.get("HOGLAKE_S3_SECRET_KEY", "hoglake123")
CATALOG = "duckext-packed"
DATA_PATH = "s3://duckext-itest/packed/"
FORMAT = "clickhouse-mergetree-packed"


def main() -> None:
    s3 = S3Config(
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
        endpoint_override=S3_ENDPOINT,
        region="us-east-1",
        allow_bucket_creation=True,
    )
    with HoglakeClient(HOGLAKE_URL, s3=s3) as client:
        s3.filesystem().create_dir("duckext-itest")
        try:
            catalog = client.catalog(CATALOG)
        except NotFoundError:
            catalog = client.create_catalog(CATALOG, DATA_PATH)
        try:
            namespace = catalog.create_namespace("ns1")
        except AlreadyExistsError:
            namespace = catalog.namespace("ns1")
        for name in ("packed_empty", "packed_registered"):
            try:
                namespace.table(name).drop()
            except NotFoundError:
                pass
            table = namespace.create_table(
                name,
                pa.schema([pa.field("id", pa.int64(), nullable=False)]),
                properties={"write.format.default": FORMAT},
            )
            if name == "packed_registered":
                catalog.commit_prepared(
                    {
                        "idempotency_key": str(uuid.uuid4()),
                        "read_snapshot": catalog.refresh().head_snapshot_id,
                        "appends": [
                            {
                                "namespace": "ns1",
                                "table": name,
                                "expected_table_uuid": table.table_uuid,
                                "files": [
                                    {
                                        "path": DATA_PATH + "data.packed",
                                        "file_format": FORMAT,
                                        "record_count": 1,
                                        "file_size_bytes": 1,
                                        "column_stats": [],
                                    }
                                ],
                            }
                        ],
                    }
                )


if __name__ == "__main__":
    main()
