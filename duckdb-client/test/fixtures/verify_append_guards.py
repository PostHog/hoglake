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
        self.ddl = []
        self.responses = []
        self.resolutions = []
        self.resolution_overrides = []
        self.on_resolve = lambda: None
        self.on_commit = lambda: None
        # (status, body) answers the proxy gives the next /commit attempts
        # itself, in order; a None entry forwards that attempt to the
        # server unchanged, so a later attempt can be the injected one
        self.injected = []
        # Catalog.capabilities the proxy hides from GET /v1/catalogs/{name}
        # (what an older server, or one without the feature, advertises)
        self.withheld_capabilities = set()
        # forwarded /commit answers as (status, body); a lost ack records
        # the server's real answer here and tells the client 503
        self.commit_results = []
        self.lost_acks = 0
        # the same loss in TRANSPORT: the server published and the
        # connection closes before any status reaches the client
        self.dropped_acks = 0
        # a hung server: every /commit is held (never forwarded) for this
        # many seconds, or until close(), then dropped without a status
        self.commit_hang_s = 0
        self.released = threading.Event()
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
                    if self.command == "DELETE" or (
                        self.command == "POST"
                        and path.path.endswith(
                            ("/catalogs", "/namespaces", "/tables", "/views", "/alter")
                        )
                    ):
                        metadata = (
                            {
                                key: values[0]
                                for key, values in parse_qs(path.query).items()
                            }
                            if self.command == "DELETE"
                            else json.loads(data)
                        )
                        owner.ddl.append((self.command, path.path, metadata))
                    if self.command == "POST" and path.path.endswith("/commit"):
                        owner.commits.append(json.loads(data))
                        owner.commit_times.append(time.monotonic())
                        if len(owner.commits) == 1:
                            owner.on_commit()
                        if owner.commit_hang_s:
                            owner.released.wait(owner.commit_hang_s)
                            self.close_connection = True
                            return
                        injected = owner.injected.pop(0) if owner.injected else None
                        if injected is not None:
                            status, body = injected
                            # a str body is a proxy's own answer (502/504
                            # HTML), not the API's JSON
                            raw = isinstance(body, str)
                            self.send_response(status)
                            self.send_header(
                                "Content-Type",
                                "text/html" if raw else "application/json",
                            )
                            if status == 503:
                                self.send_header("Retry-After", "1")
                            self.end_headers()
                            self.wfile.write(
                                (body if raw else json.dumps(body)).encode()
                            )
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
                        if (
                            owner.withheld_capabilities
                            and self.command == "GET"
                            and path.path.startswith("/v1/catalogs/")
                            and path.path.count("/") == 3
                        ):
                            body["capabilities"] = [
                                capability
                                for capability in body.get("capabilities", [])
                                if capability not in owner.withheld_capabilities
                            ]
                        if self.command == "GET" and path.path.endswith(
                            "/snapshots/resolve"
                        ):
                            owner.resolutions.append(body)
                            owner.on_resolve()
                            owner.on_resolve = lambda: None
                            if owner.resolution_overrides:
                                body = owner.resolution_overrides.pop(0)
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
                    if self.command == "POST" and path.path.endswith("/commit"):
                        owner.commit_results.append((response.status_code, body))
                        if owner.lost_acks and response.is_success:
                            # the server published; the client hears
                            # what Hoglite says for a lost acknowledgement
                            owner.lost_acks -= 1
                            self.send_response(503)
                            self.send_header("Content-Type", "application/json")
                            self.send_header("Retry-After", "1")
                            self.end_headers()
                            self.wfile.write(
                                json.dumps(
                                    {
                                        "error": "commit_outcome_unknown",
                                        "detail": "ack lost",
                                    }
                                ).encode()
                            )
                            return
                        if owner.dropped_acks and response.is_success:
                            # the server published; the connection dies
                            # without a status line (a reset, or what the
                            # client's read timeout amounts to)
                            owner.dropped_acks -= 1
                            self.close_connection = True
                            return
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
        self.released.set()
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

    def test_ddl_metadata_reaches_requests_and_snapshots(self):
        namespace = self._testMethodName
        before = self.catalog.refresh().head_snapshot_id
        self.run_sql(f"""
            CREATE SCHEMA lake.ddl_metadata;
            CREATE TABLE lake.{namespace}.ddl_table (id BIGINT);
            ALTER TABLE lake.{namespace}.ddl_table ADD COLUMN extra BIGINT;
            DROP TABLE lake.{namespace}.ddl_table;
        """)
        expected = [
            "CREATE SCHEMA ddl_metadata",
            f"CREATE TABLE {namespace}.ddl_table",
            f"ALTER TABLE {namespace}.ddl_table",
            f"DROP TABLE {namespace}.ddl_table",
        ]
        self.assertEqual(
            [metadata["message"] for _, _, metadata in self.proxy.ddl], expected
        )
        self.assertTrue(
            all(
                metadata["author"] == "duckdb-client"
                for _, _, metadata in self.proxy.ddl
            )
        )
        snapshots = [
            snapshot
            for snapshot in self.catalog.snapshots(after=before)
            if snapshot.author == "duckdb-client"
        ]
        self.assertEqual([snapshot.message for snapshot in snapshots], expected)

    def test_time_pin_resolves_empty_catalog(self):
        name = "duckext-time-empty-" + uuid.uuid4().hex[:10]
        empty = self.client.create_catalog(
            name,
            f"s3://duckext-itest/{name}/",
        )
        self.run_sql(f"""
            ATTACH 'hoglake:{empty.name}' AS pinned
                (ENDPOINT '{self.proxy.url}', SNAPSHOT_TIME '2999-01-01T00:00:00Z');
            SELECT CASE WHEN id = 0 THEN 1 ELSE error('wrong initial snapshot') END
                FROM hoglake_current_snapshot('pinned');
        """)

    def test_future_time_pin_survives_head_advance_until_commit(self):
        pin = self.catalog.resolve_snapshot("2999-01-01T00:00:00Z").snapshot_id
        self.proxy.on_resolve = lambda: self.x.append(
            pa.Table.from_pydict({"a": [11], "b": [22]}, schema=SCHEMA)
        )
        self.run_sql(f"""
            ATTACH 'hoglake:{self.catalog_name}' AS pinned
                (ENDPOINT '{self.proxy.url}', SNAPSHOT_TIME '2999-01-01T00:00:00Z');
            BEGIN;
            SELECT CASE WHEN id = {pin} THEN 1 ELSE error('wrong first pin') END
                FROM hoglake_current_snapshot('pinned');
            SELECT CASE WHEN count(*) = 0 THEN 1 ELSE error('time pin moved') END
                FROM pinned.{self._testMethodName}.x;
            SELECT CASE WHEN id = {pin} THEN 1 ELSE error('snapshot changed') END
                FROM hoglake_current_snapshot('pinned');
            COMMIT;
            BEGIN;
            SELECT CASE WHEN id = {pin + 1} THEN 1 ELSE error('pin did not refresh') END
                FROM hoglake_current_snapshot('pinned');
            SELECT CASE WHEN count(*) = 1 THEN 1 ELSE error('new head not visible') END
                FROM pinned.{self._testMethodName}.x;
            COMMIT;
        """)
        self.assertEqual(
            [r["snapshot_id"] for r in self.proxy.resolutions], [pin, pin + 1]
        )

    def test_time_pin_rejects_malformed_resolution(self):
        valid = {"snapshot_id": 1, "snapshot_time": "2026-02-01T00:00:00Z"}
        bodies = [
            {},
            {"snapshot_id": 1},
            {"snapshot_time": valid["snapshot_time"]},
            *[
                dict(valid, snapshot_id=value)
                for value in (None, True, 1.5, "1", -1, 2**63)
            ],
            *[dict(valid, snapshot_time=value) for value in (None, "", 1)],
        ]
        for body in bodies:
            with self.subTest(body=body):
                self.proxy.resolution_overrides = [body]
                result = self.run_sql(
                    f"""
                    ATTACH 'hoglake:{self.catalog_name}' AS pinned
                        (ENDPOINT '{self.proxy.url}', SNAPSHOT_TIME '2999-01-01T00:00:00Z');
                    SELECT * FROM hoglake_current_snapshot('pinned');
                """,
                    succeeds=False,
                )
                self.assertIn("snapshot resolution", result.stderr)

    def test_time_pin_protocol_error_fails_table_listing(self):
        self.proxy.resolution_overrides = [
            {"snapshot_id": 1.5, "snapshot_time": "2026-02-01T00:00:00Z"}
        ]
        result = self.run_sql(
            f"""
            ATTACH 'hoglake:{self.catalog_name}' AS pinned
                (ENDPOINT '{self.proxy.url}', SNAPSHOT_TIME '2999-01-01T00:00:00Z');
            SELECT * FROM hoglake_table_info('pinned');
        """,
            succeeds=False,
        )
        self.assertIn("snapshot resolution", result.stderr)

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
            self.x.drop(author="duckdb-client-tests", message="drop test fixture")
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

    def test_lost_commit_ack_replays_the_receipt(self):
        # The server published the commit and the client saw 503
        # commit_outcome_unknown instead of the result. The retry carries
        # the same idempotency_key, so the server answers from the
        # receipt: one file, one snapshot, no second registration.
        self.proxy.lost_acks = 1
        self.run_sql(f"INSERT INTO {self.prefix}x VALUES (11, 22)")
        self.assertEqual(len(self.proxy.commits), 2)
        self.assertEqual(self.proxy.commits[0], self.proxy.commits[1])
        # the duplicate-data symptom first: unkeyed, the retry registers
        # the same parquet again and the user still sees success
        self.assertEqual(len(self.files("x")), 1)
        self.assertEqual(
            [status for status, _ in self.proxy.commit_results], [200, 200]
        )
        self.assertEqual(
            self.proxy.commit_results[0][1]["snapshot_id"],
            self.proxy.commit_results[1][1]["snapshot_id"],
        )
        key = self.proxy.commits[0]["idempotency_key"]
        self.assertEqual(str(uuid.UUID(key)), key)

    def test_lost_commit_ack_in_transport_replays_the_receipt(self):
        # The same loss one layer down: the server published and the
        # connection closed before any status reached the client. Not an
        # IOException out of COMMIT: the loop retries under the same key
        # and the server answers from the receipt.
        self.proxy.dropped_acks = 1
        self.run_sql(f"INSERT INTO {self.prefix}x VALUES (11, 22)")
        self.assertEqual(len(self.proxy.commits), 2)
        self.assertEqual(self.proxy.commits[0], self.proxy.commits[1])
        self.assertEqual(len(self.files("x")), 1)
        self.assertEqual(
            [status for status, _ in self.proxy.commit_results], [200, 200]
        )
        self.assertEqual(
            self.proxy.commit_results[0][1]["snapshot_id"],
            self.proxy.commit_results[1][1]["snapshot_id"],
        )

    def test_gateway_error_retries_identical_request(self):
        # A proxy's 502/504 is not the API's JSON and says nothing about
        # whether the server saw the request: retried under the same key.
        for status, table in ((502, "x"), (504, "y")):
            with self.subTest(status=status):
                self.proxy.injected = [(status, "<html>gateway error</html>")]
                before = len(self.proxy.commits)
                self.run_sql(f"INSERT INTO {self.prefix}{table} VALUES (11, 22)")
                self.assertEqual(len(self.proxy.commits), before + 2)
                self.assertEqual(
                    self.proxy.commits[before], self.proxy.commits[before + 1]
                )
                self.assertEqual(len(self.files(table)), 1)

    def test_untyped_503_is_an_unknown_outcome(self):
        # A proxy's 503 with a non-API body can follow a commit it forwarded
        # and lost the answer to. Only commit_queue_timeout and
        # backend_unavailable are definite; this one retries only under the
        # key, and without the capability it stops with the advice.
        self.proxy.injected = [(503, "<html>upstream connect error</html>")]
        before = len(self.proxy.commits)
        self.run_sql(f"INSERT INTO {self.prefix}x VALUES (11, 22)")
        self.assertEqual(len(self.proxy.commits), before + 2)
        self.assertEqual(self.proxy.commits[before], self.proxy.commits[before + 1])
        self.assertIn("idempotency_key", self.proxy.commits[before])
        self.assertEqual(len(self.files("x")), 1)
        self.proxy.withheld_capabilities = {"idempotent-append-v1"}
        self.proxy.injected = [(503, "<html>upstream connect error</html>")]
        before = len(self.proxy.commits)
        result = self.run_sql(
            f"INSERT INTO {self.prefix}y VALUES (11, 22)", succeeds=False
        )
        self.assertEqual(len(self.proxy.commits), before + 1)
        self.assertIn("MAY be durable", result.stderr)
        self.assertIn("idempotent-append-v1", result.stderr)
        self.assertNotIn("retries exhausted", result.stderr)

    def assert_unknown_outcome_advice(self, stderr, key):
        # The terminal error for a commit the server may have published:
        # names the key and the receipt route, says what a 404 there
        # does NOT prove (an attempt may still be in flight; the server
        # guarantees no time after which one has stopped) and how long
        # the receipt is kept, points at the table's contents instead,
        # and never tells the user to re-run blind. One advice, once.
        receipt_url = (
            f"{self.proxy.url}/v1/catalogs/{self.catalog_name}/commit/receipts/{key}"
        )
        self.assertEqual(stderr.count("MAY be durable"), 1, stderr)
        self.assertIn(receipt_url, stderr)
        self.assertIn("cannot retry under the same idempotency_key", stderr)
        self.assertIn("A 404 is not proof either way", stderr)
        self.assertIn("an attempt may still be in flight", stderr)
        self.assertIn("HOGLAKE_RECEIPT_RETENTION_SECONDS", stderr)
        self.assertIn("check the table's contents or hoglake_snapshots", stderr)
        self.assertNotIn("HOGLAKE_COMMIT_LOCK_TIMEOUT_MS", stderr)
        self.assertNotIn("surely stopped", stderr)
        self.assertNotIn("404 if it did not", stderr)
        return receipt_url

    def test_exhausted_unknown_outcome_names_the_receipt(self):
        # Out of retries on an outcome the server never settled: the
        # error must not read as "nothing applied". It names the key and
        # the receipt route, and that route answers for the commit the
        # server did publish.
        cases = (
            ("lost_acks", "x", "commit_outcome_unknown"),
            ("dropped_acks", "y", "no_response"),
        )
        for attribute, table, code in cases:
            with self.subTest(attribute=attribute):
                setattr(self.proxy, attribute, 1)
                before = len(self.proxy.commits)
                result = self.run_sql(
                    f"SET hoglake_max_retry_count=0; "
                    f"INSERT INTO {self.prefix}{table} VALUES (11, 22)",
                    succeeds=False,
                )
                self.assertEqual(len(self.proxy.commits), before + 1)
                key = self.proxy.commits[before]["idempotency_key"]
                self.assertIn(code, result.stderr)
                receipt_url = self.assert_unknown_outcome_advice(result.stderr, key)
                self.assertEqual(len(self.files(table)), 1)
                receipt = httpx.get(receipt_url, trust_env=False)
                self.assertEqual(receipt.status_code, 200, receipt.text)
                self.assertEqual(
                    receipt.json()["snapshot_id"],
                    self.proxy.commit_results[before][1]["snapshot_id"],
                )

    def test_unknown_outcome_outlives_a_later_definite_failure(self):
        # The first attempt's ack is lost (the server published); the
        # retry gets a definite answer that is not success. The commit
        # is still MAY-be-durable: a definite failure on a retry does not
        # prove the earlier attempt dead, so the terminal error keeps the
        # receipt advice whether the retry's answer exhausted the budget
        # (503 commit_queue_timeout) or stopped the loop on its own
        # (409 ddl_since_read_snapshot). ONE message: the 409's own
        # "re-run the statement" suffix would contradict the advice not
        # to re-run blind, so the advice replaces it and carries the
        # reason (a new transaction is needed) itself.
        cases = (
            (
                "x",
                (503, {"error": "commit_queue_timeout", "detail": "busy"}),
                " (retries exhausted)",
                "retries exhausted; after 2 attempt(s) the commit MAY be durable",
            ),
            (
                "y",
                (409, {"error": "ddl_since_read_snapshot", "detail": "changed"}),
                " (start a new transaction and re-run the statement)",
                (
                    "this refusal needs a new transaction and ended the retries; "
                    "after 2 attempt(s) the commit MAY be durable"
                ),
            ),
        )
        for table, definite, definite_suffix, lead in cases:
            with self.subTest(code=definite[1]["error"]):
                self.proxy.lost_acks = 1
                self.proxy.injected = [None, definite]
                before = len(self.proxy.commits)
                result = self.run_sql(
                    f"SET hoglake_max_retry_count=1; "
                    f"INSERT INTO {self.prefix}{table} VALUES (11, 22)",
                    succeeds=False,
                )
                self.assertEqual(len(self.proxy.commits), before + 2)
                self.assertEqual(
                    self.proxy.commits[before], self.proxy.commits[before + 1]
                )
                # the last attempt's answer leads, the earlier unknown
                # outcome qualifies it
                self.assertIn(definite[1]["error"], result.stderr)
                key = self.proxy.commits[before]["idempotency_key"]
                receipt_url = self.assert_unknown_outcome_advice(result.stderr, key)
                self.assertIn(lead, result.stderr)
                self.assertNotIn(definite_suffix, result.stderr)
                if definite[0] == 409:
                    self.assertIn(
                        "before re-running the statement in a new transaction)",
                        result.stderr,
                    )
                else:
                    self.assertNotIn("new transaction", result.stderr)
                self.assertEqual(len(self.files(table)), 1)
                receipt = httpx.get(receipt_url, trust_env=False)
                self.assertEqual(receipt.status_code, 200, receipt.text)

    def test_internal_error_is_an_unknown_outcome(self):
        # A 500 on /commit is not a definite failure: on the PostgreSQL
        # backend a COMMIT whose acknowledgement the database connection
        # lost is durable yet surfaces as the catch-all internal_error
        # (only Hoglite raises commit_outcome_unknown). Keyed, the loop
        # retries it under the same key; unkeyed it stops with the advice.
        internal_error = (
            500,
            {"error": "internal_error", "detail": "internal server error"},
        )
        self.proxy.injected = [internal_error]
        self.run_sql(f"INSERT INTO {self.prefix}x VALUES (11, 22)")
        self.assertEqual(len(self.proxy.commits), 2)
        self.assertEqual(self.proxy.commits[0], self.proxy.commits[1])
        self.assertEqual(len(self.files("x")), 1)

        self.proxy.withheld_capabilities = {"idempotent-append-v1"}
        self.proxy.injected = [internal_error]
        result = self.run_sql(
            f"INSERT INTO {self.prefix}y VALUES (11, 22)", succeeds=False
        )
        self.assertEqual(len(self.proxy.commits), 3)
        self.assertNotIn("idempotency_key", self.proxy.commits[2])
        self.assertIn("internal_error", result.stderr)
        self.assertIn("MAY be durable", result.stderr)
        self.assertIn("idempotent-append-v1", result.stderr)
        self.assertNotIn("retries exhausted", result.stderr)
        self.assertEqual(len(self.files("y")), 0)

    def test_commit_deadline_bounds_the_retry_loop(self):
        # A hung server: every attempt times out (an unknown outcome,
        # retried under the key) and max_retry_count alone would hold
        # COMMIT for (retries + 1) × hoglake_commit_timeout_ms. The
        # deadline caps the loop's wall time: the last attempt's wait is
        # clamped to the time left, no attempt starts that could not end
        # in time, and the error names the deadline and gives the
        # unknown-outcome advice. The receipt is a 404 here (the proxy
        # never forwarded), which is exactly what the advice says a 404
        # does not prove.
        self.proxy.commit_hang_s = 30
        result = self.run_sql(
            f"SET hoglake_commit_timeout_ms=1000; "
            f"SET hoglake_commit_deadline_ms=2500; "
            f"SET hoglake_max_retry_count=5; "
            f"INSERT INTO {self.prefix}x VALUES (11, 22)",
            succeeds=False,
        )
        # 6 without the deadline; 3 with it (0-1 s, 1-2 s, 2-2.5 s)
        self.assertIn(len(self.proxy.commits), (2, 3), self.proxy.commit_times)
        self.assertLess(self.proxy.commit_times[-1] - self.proxy.commit_times[0], 2.5)
        self.assertEqual(len(set(map(json.dumps, self.proxy.commits))), 1)
        self.assertIn("no_response", result.stderr)
        self.assertIn(
            "the commit deadline (hoglake_commit_deadline_ms=2500) passed; "
            f"after {len(self.proxy.commits)} attempt(s) the commit MAY be durable",
            result.stderr,
        )
        key = self.proxy.commits[0]["idempotency_key"]
        receipt_url = self.assert_unknown_outcome_advice(result.stderr, key)
        self.assertEqual(httpx.get(receipt_url, trust_env=False).status_code, 404)
        self.assertEqual(len(self.files("x")), 0)

    def test_unknown_outcomes_do_not_retry_without_the_capability(self):
        # A server that does not advertise idempotent-append-v1 ignores
        # the key, so a retry of a commit that may have published is
        # blind. Against one the extension sends no key and stops at the
        # first unknown outcome, saying so; the definite 503 still retries.
        self.proxy.withheld_capabilities = {"idempotent-append-v1"}
        for attribute, table, code in (
            ("lost_acks", "x", "commit_outcome_unknown"),
            ("dropped_acks", "y", "no_response"),
        ):
            with self.subTest(attribute=attribute):
                setattr(self.proxy, attribute, 1)
                before = len(self.proxy.commits)
                result = self.run_sql(
                    f"INSERT INTO {self.prefix}{table} VALUES (11, 22)",
                    succeeds=False,
                )
                self.assertEqual(len(self.proxy.commits), before + 1)
                self.assertNotIn("idempotency_key", self.proxy.commits[before])
                self.assertIn(code, result.stderr)
                self.assertIn("MAY be durable", result.stderr)
                self.assertIn("idempotent-append-v1", result.stderr)
                self.assertNotIn("retries exhausted", result.stderr)
                self.assertNotIn("/commit/receipts/", result.stderr)
                self.assertEqual(len(self.files(table)), 1)
        self.proxy.injected = [
            (503, {"error": "commit_queue_timeout", "detail": "busy"})
        ]
        before = len(self.proxy.commits)
        self.run_sql(
            f"CREATE TABLE {self.prefix}z (a BIGINT); INSERT INTO {self.prefix}z VALUES (11)"
        )
        self.assertEqual(len(self.proxy.commits), before + 2)
        self.assertEqual(self.proxy.commits[before], self.proxy.commits[before + 1])
        self.assertNotIn("idempotency_key", self.proxy.commits[before])
        self.assertEqual(len(self.files("z")), 1)

    def test_each_commit_mints_its_own_idempotency_key(self):
        self.run_sql(
            f"INSERT INTO {self.prefix}x VALUES (11, 22); "
            f"BEGIN; INSERT INTO {self.prefix}x VALUES (33, 44); "
            f"INSERT INTO {self.prefix}y VALUES (55, 66); COMMIT"
        )
        self.assertEqual(len(self.proxy.commits), 2)
        keys = [commit["idempotency_key"] for commit in self.proxy.commits]
        self.assertEqual(len(set(keys)), 2)
        for key in keys:
            self.assertEqual(str(uuid.UUID(key)), key)
        self.assertEqual(len(self.files("x")), 2)
        self.assertEqual(len(self.files("y")), 1)

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
