"""Exercise DuckDB append guards against a live server and deterministic HTTP races."""

import json
import os
import subprocess
import sys
import threading
import time
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import httpx
import pyarrow as pa

from pyhoglake import HoglakeClient, S3Config, ops

ROOT = Path(__file__).resolve().parents[2]
BUILD = ROOT / "build" / "release"
URL = os.environ["HOGLAKE_URL"].rstrip("/")
S3_ENDPOINT = os.environ["DUCKEXT_S3_ENDPOINT"]
SCHEMA = pa.schema([("a", pa.int64()), ("b", pa.int64())])


class CommitProxy:
    def __init__(self):
        self.commits = []
        self.commit_times = []
        self.preparations = []
        self.responses = []
        self.on_commit = lambda: None
        self.injected = []
        self.failure = None
        self.poison_metadata = False
        self.wait_for_hydration = False
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def forward(self):
                try:
                    data = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                    path = urlsplit(self.path)
                    if self.command == "POST" and path.path.endswith("/commit"):
                        owner.commits.append(json.loads(data))
                        owner.commit_times.append(time.monotonic())
                        if len(owner.commits) == 1:
                            owner.on_commit()
                        if owner.injected:
                            status, body = owner.injected.pop(0)
                            self.send_response(status)
                            self.send_header("Content-Type", "application/json")
                            if status == 503:
                                self.send_header("Retry-After", "1")
                            self.end_headers()
                            self.wfile.write(json.dumps(body).encode())
                            return
                    with httpx.Client(trust_env=False, timeout=60) as client:
                        if (
                            owner.wait_for_hydration
                            and self.command == "POST"
                            and path.path.endswith("/alter")
                        ):
                            deadline = time.monotonic() + 15
                            scan_url = URL + path.path.removesuffix("/alter") + "/scan"
                            while True:
                                scan = client.get(scan_url)
                                scan.raise_for_status()
                                if all(
                                    entry["data_file"]["stats_state"] == "provided"
                                    for entry in scan.json()
                                ):
                                    break
                                if time.monotonic() >= deadline:
                                    raise TimeoutError(
                                        "DDL fixture files did not hydrate within 15 seconds"
                                    )
                                time.sleep(0.05)
                        response = client.request(
                            self.command,
                            URL + self.path,
                            content=data,
                            headers={"Content-Type": "application/json"},
                        )
                    body = response.json()
                    if response.is_success:
                        params = parse_qs(path.query)
                        if (
                            self.command == "GET"
                            and "/tables/" in path.path
                            and "snapshot" in params
                        ):
                            owner.preparations.append(int(params["snapshot"][0]))
                        if self.command == "POST" and (
                            path.path.endswith("/tables")
                            or path.path.endswith("/alter")
                        ):
                            owner.preparations.append(body["snapshot_id"])
                        if (
                            owner.poison_metadata
                            and self.command == "GET"
                            and path.path.startswith(
                                "/v1/catalogs/duckext-read/namespaces/ns1/tables/"
                            )
                        ):
                            name = path.path.rsplit("/", 1)[-1]
                            if name == "bad_dec100":
                                body["columns"][0]["type_params"]["precision"] = 100
                            elif name == "bad_dec_nop":
                                body["columns"][0].pop("type_params", None)
                            elif name == "bad_dec_neg":
                                body["columns"][0]["type_params"]["precision"] = -1
                            elif name == "bad_cols":
                                body["columns"][1]["name"] = "Team"
                    owner.responses.append((response.status_code, body))
                    self.send_response(response.status_code)
                    self.send_header("Content-Type", "application/json")
                    if "Retry-After" in response.headers:
                        self.send_header("Retry-After", response.headers["Retry-After"])
                    self.end_headers()
                    self.wfile.write(json.dumps(body).encode())
                except Exception as error:
                    owner.failure = error
                    self.send_response(500)
                    self.end_headers()
                    raise

            do_GET = forward
            do_POST = forward
            do_DELETE = forward

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


class AppendGuards(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        s3 = S3Config(
            endpoint_override=f"http://{S3_ENDPOINT}",
            access_key="hoglake",
            secret_key="hoglake123",
            region="us-east-1",
            allow_bucket_creation=True,
        )
        cls.client = HoglakeClient(URL, s3=s3)
        cls.addClassCleanup(cls.client.close)
        s3.filesystem().create_dir("duckext-itest")
        cls.catalog_name = "duckext-guards-" + uuid.uuid4().hex[:10]
        cls.catalog = cls.client.create_catalog(
            cls.catalog_name, f"s3://duckext-itest/{cls.catalog_name}/"
        )

    def setUp(self):
        self.ns = self.catalog.create_namespace(self._testMethodName)
        self.x = self.ns.create_table("x", SCHEMA)
        self.y = self.ns.create_table("y", SCHEMA)
        self.proxy = CommitProxy()
        self.addCleanup(self.proxy.close)
        self.prefix = f"lake.{self._testMethodName}."

    def run_sql(self, sql, succeeds=True):
        setup = f"""
            LOAD '{BUILD / "extension/hoglake/hoglake.duckdb_extension"}';
            LOAD '{BUILD / "extension/httpfs/httpfs.duckdb_extension"}';
            CREATE SECRET (TYPE s3, KEY_ID 'hoglake', SECRET 'hoglake123',
                ENDPOINT '{S3_ENDPOINT}', URL_STYLE 'path', USE_SSL false);
            ATTACH 'hoglake:{self.catalog_name}' AS lake (ENDPOINT '{self.proxy.url}');
            SET hoglake_max_retry_count=2;
            SET hoglake_retry_wait_ms=1;
        """
        result = subprocess.run(
            [
                str(BUILD / "duckdb"),
                "-unsigned",
                "-batch",
                "-bail",
                "-no-stdin",
                "-c",
                setup + sql,
            ],
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
        )
        self.assertIsNone(self.proxy.failure)
        self.assertEqual(
            result.returncode == 0, succeeds, result.stdout + result.stderr
        )
        return result

    def refused(self, sql, code="ddl_since_read_snapshot"):
        result = self.run_sql(sql, succeeds=False)
        self.assertIn(code, result.stderr)
        self.assertIn("new transaction", result.stderr)
        self.assertEqual(len(self.proxy.commits), 1)
        self.assertEqual(self.files("x"), [])
        self.assertEqual(self.files("y"), [])

    def files(self, name):
        return self.ns.table(name).files()

    def partition_x(self, field=1):
        self.x.alter([ops.set_partition_spec([ops.partition_field(field, "identity")])])

    def test_partitioned_insert_uses_preparation_snapshot(self):
        self.partition_x()
        self.run_sql(f"INSERT INTO {self.prefix}x VALUES (11, 22)")
        self.assertEqual(
            self.proxy.commits[0]["read_snapshot"], min(self.proxy.preparations)
        )
        self.assertEqual(self.files("x")[0].partition_values, ("11",))

    def test_same_arity_partition_change_refuses_stale_values(self):
        self.partition_x()
        self.proxy.on_commit = lambda: self.partition_x(2)
        self.refused(f"INSERT INTO {self.prefix}x VALUES (11, 22)")

    def test_schema_change_refuses_even_unpartitioned_append(self):
        self.proxy.on_commit = lambda: self.x.alter([ops.add_column("c", "long")])
        self.refused(f"INSERT INTO {self.prefix}x VALUES (11, 22)")

    def test_recreated_table_refuses_one_attempt(self):
        def recreate():
            self.x.drop()
            self.x = self.ns.create_table("x", SCHEMA)

        self.proxy.on_commit = recreate
        self.refused(f"INSERT INTO {self.prefix}x VALUES (11, 22)", "table_recreated")

    def test_multi_table_refusal_is_atomic(self):
        self.proxy.on_commit = lambda: self.y.alter([ops.add_column("c", "long")])
        self.refused(
            f"BEGIN; INSERT INTO {self.prefix}x VALUES (11, 22); "
            f"INSERT INTO {self.prefix}y VALUES (33, 44); COMMIT"
        )

    def test_same_table_multi_statement_retains_snapshot(self):
        self.run_sql(
            f"BEGIN; INSERT INTO {self.prefix}x VALUES (11, 22); "
            f"INSERT INTO {self.prefix}x VALUES (33, 44); COMMIT"
        )
        self.assertEqual(len(self.proxy.commits), 1)
        self.assertEqual(len(self.proxy.commits[0]["appends"][0]["files"]), 2)
        self.assertEqual(
            self.proxy.commits[0]["read_snapshot"], min(self.proxy.preparations)
        )

    def test_own_alter_then_insert_uses_post_ddl_snapshot(self):
        self.run_sql(
            f"BEGIN; ALTER TABLE {self.prefix}x ADD COLUMN c BIGINT; "
            f"INSERT INTO {self.prefix}x VALUES (11, 22, 33); COMMIT"
        )
        self.assertEqual(
            self.proxy.commits[0]["read_snapshot"], max(self.proxy.preparations)
        )
        self.assertEqual(len(self.files("x")), 1)

    def test_older_append_plus_own_alter_refuses(self):
        self.refused(
            f"BEGIN; INSERT INTO {self.prefix}x VALUES (11, 22); "
            f"ALTER TABLE {self.prefix}y ADD COLUMN c BIGINT; "
            f"INSERT INTO {self.prefix}y VALUES (33, 44, 55); COMMIT"
        )

    def test_later_ordinary_append_lowers_snapshot(self):
        self.refused(
            f"BEGIN; ALTER TABLE {self.prefix}x ADD COLUMN c BIGINT; "
            f"INSERT INTO {self.prefix}x VALUES (11, 22, 33); "
            f"INSERT INTO {self.prefix}y VALUES (44, 55); COMMIT"
        )

    def test_create_then_insert_uses_create_snapshot(self):
        self.run_sql(
            f"BEGIN; CREATE TABLE {self.prefix}z (a BIGINT); "
            f"INSERT INTO {self.prefix}z VALUES (11); COMMIT"
        )
        self.assertEqual(
            self.proxy.commits[0]["read_snapshot"], max(self.proxy.preparations)
        )
        self.assertEqual(len(self.ns.table("z").files()), 1)

    def test_ctas_uses_create_snapshot(self):
        self.run_sql(f"CREATE TABLE {self.prefix}z AS SELECT 11::BIGINT AS a")
        self.assertEqual(
            self.proxy.commits[0]["read_snapshot"], max(self.proxy.preparations)
        )
        self.assertEqual(len(self.ns.table("z").files()), 1)

    def test_external_alter_after_create_refuses(self):
        self.proxy.on_commit = lambda: self.ns.table("z").alter(
            [ops.add_column("b", "long")]
        )
        self.refused(
            f"BEGIN; CREATE TABLE {self.prefix}z (a BIGINT); "
            f"INSERT INTO {self.prefix}z VALUES (11); COMMIT"
        )
        self.assertEqual(self.ns.table("z").files(), [])

    def test_unrelated_own_ddl_keeps_append_legal(self):
        self.run_sql(
            f"BEGIN; ALTER TABLE {self.prefix}y ADD COLUMN c BIGINT; "
            f"INSERT INTO {self.prefix}x VALUES (11, 22); COMMIT"
        )
        self.assertEqual(len(self.files("x")), 1)

    def test_concurrent_append_keeps_insert_legal(self):
        self.proxy.on_commit = lambda: self.x.append(pa.table({"a": [33], "b": [44]}))
        self.run_sql(f"INSERT INTO {self.prefix}x VALUES (11, 22)")
        self.assertEqual(len(self.proxy.commits), 1)
        self.assertEqual(len(self.files("x")), 2)

    def test_append_and_delete_include_delete_snapshot(self):
        self.x.append(pa.table({"a": [11], "b": [22]}))
        self.run_sql(
            f"BEGIN; DELETE FROM {self.prefix}x WHERE a=11; "
            f"CREATE TABLE {self.prefix}z (a BIGINT); "
            f"INSERT INTO {self.prefix}z VALUES (33); COMMIT"
        )
        self.assertEqual(
            self.proxy.commits[0]["read_snapshot"], min(self.proxy.preparations)
        )
        self.assertTrue(self.proxy.commits[0]["deletes"])

    def test_empty_insert_does_not_lower_append_snapshot(self):
        self.run_sql(
            f"BEGIN; ALTER TABLE {self.prefix}x ADD COLUMN c BIGINT; "
            f"INSERT INTO {self.prefix}x VALUES (11, 22, 33); "
            f"INSERT INTO {self.prefix}y SELECT * FROM {self.prefix}y WHERE false; COMMIT"
        )
        self.assertEqual(len(self.files("x")), 1)
        self.assertEqual(self.files("y"), [])

    def test_delete_conflict_does_not_retry(self):
        self.x.append(pa.table({"a": [11], "b": [22]}))
        self.proxy.injected = [(409, {"error": "commit_conflict", "detail": "changed"})]
        result = self.run_sql(f"DELETE FROM {self.prefix}x WHERE a=11", succeeds=False)
        self.assertIn("deletion vectors", result.stderr)
        self.assertEqual(len(self.proxy.commits), 1)

    def test_empty_insert_before_alter_does_not_pin_append(self):
        self.run_sql(
            f"BEGIN; INSERT INTO {self.prefix}y SELECT * FROM {self.prefix}y WHERE false; "
            f"ALTER TABLE {self.prefix}x ADD COLUMN c BIGINT; "
            f"INSERT INTO {self.prefix}x VALUES (11, 22, 33); COMMIT"
        )
        self.assertEqual(len(self.proxy.commits), 1)
        self.assertEqual(len(self.files("x")), 1)

    def test_typed_ddl_refusal_does_not_depend_on_detail(self):
        self.proxy.injected = [
            (409, {"error": "ddl_since_read_snapshot", "detail": "changed"})
        ]
        self.refused(f"INSERT INTO {self.prefix}x VALUES (11, 22)")

    def test_typed_recreation_refusal_does_not_depend_on_detail(self):
        self.proxy.injected = [(409, {"error": "table_recreated", "detail": "changed"})]
        self.refused(f"INSERT INTO {self.prefix}x VALUES (11, 22)", "table_recreated")

    def test_backpressure_retries_identical_request(self):
        self.proxy.injected = [
            (503, {"error": "commit_queue_timeout", "detail": "busy"})
        ]
        self.run_sql(f"INSERT INTO {self.prefix}x VALUES (11, 22)")
        self.assertEqual(len(self.proxy.commits), 2)
        self.assertEqual(self.proxy.commits[0], self.proxy.commits[1])
        self.assertGreaterEqual(
            self.proxy.commit_times[1] - self.proxy.commit_times[0], 0.9
        )

    def test_transient_append_conflict_retries_identical_request(self):
        self.proxy.injected = [(409, {"error": "commit_conflict", "detail": "busy"})]
        self.run_sql(f"INSERT INTO {self.prefix}x VALUES (11, 22)")
        self.assertEqual(len(self.proxy.commits), 2)
        self.assertEqual(self.proxy.commits[0], self.proxy.commits[1])

    def test_legacy_recreation_refusal_does_not_retry(self):
        self.proxy.injected = [
            (409, {"error": "commit_conflict", "detail": "the table was recreated"})
        ]
        self.refused(f"INSERT INTO {self.prefix}x VALUES (11, 22)", "commit_conflict")

    def test_unknown_conflict_does_not_retry(self):
        self.proxy.injected = [(409, {"error": "future_guard", "detail": "changed"})]
        self.refused(f"INSERT INTO {self.prefix}x VALUES (11, 22)", "future_guard")


if __name__ == "__main__":
    if sys.argv[1:] == ["--sqllogictests"]:
        proxy = CommitProxy()
        proxy.poison_metadata = True
        proxy.wait_for_hydration = True
        try:
            result = subprocess.run(
                ["make", "test"],
                cwd=ROOT,
                env={**os.environ, "HOGLAKE_URL": proxy.url},
                check=False,
            )
            if proxy.failure is not None:
                raise proxy.failure
            sys.exit(result.returncode)
        finally:
            proxy.close()
    else:
        unittest.main(verbosity=2)
