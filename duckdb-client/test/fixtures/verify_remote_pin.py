"""Remote schemas: each remote table pins its own snapshot (DESIGN.md "Remote schemas").

A stdlib stub plays the catalog's server with two snapshot spaces: this
catalog's head is 5, and the remote schema team_42 answers its tables at
their own pins (events 9001, orders 7001; each unpinned fetch bumps the
pin, as a later transaction sees a later head). The stub records every
request line, and the built `duckdb` binary runs against it. No S3 and
no live Hoglake: every scan plan is empty.

    python3 test/fixtures/verify_remote_pin.py

DUCKEXT_BUILD_DIR points at another build (default build/release); the
local-request-lines case also passes against a build without this
change.
"""

import json
import os
import re
import subprocess
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parents[2]
BUILD = Path(os.environ.get("DUCKEXT_BUILD_DIR", ROOT / "build" / "release"))
CATALOG = "remote-pin"
PREFIX = f"/v1/catalogs/{CATALOG}"
HEAD = 5
CUSTOMER_SNAPSHOT = re.compile(rf"[?&]snapshot={HEAD}(&|$)")
COLUMNS = [
    {"name": "id", "type": "long", "nullable": True, "field_id": 1, "ordinal": 0}
]
UUIDS = {
    ("ns1", "t"): "00000000-0000-4000-8000-000000000001",
    ("team_42", "events"): "00000000-0000-4000-8000-000000000042",
    ("team_42", "orders"): "00000000-0000-4000-8000-000000000043",
}
REMOTE_ENTRY = {"name": "team_42", "remote": True}


class Stub:
    """mode: "listed" (the listing names team_42), "star" (a `*` token:
    it does not), "r7" (ns1.t answers remote), "nopin" (team_42.events
    answers without read_snapshot_id)."""

    def __init__(self, mode="listed"):
        self.mode = mode
        self.lines = []
        self.pins = {"events": 9001, "orders": 7001}
        self.lock = threading.Lock()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def reply(self, status, body):
                data = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def handle_any(self):
                self.rfile.read(int(self.headers.get("Content-Length", "0")))
                with owner.lock:
                    owner.lines.append(f"{self.command} {self.path}")
                status, body = owner.answer(self.command, urlsplit(self.path))
                self.reply(status, body)

            do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = handle_any

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=10)

    def table(self, ns, name, query):
        body = {
            "name": name,
            "namespace": ns,
            "table_uuid": UUIDS[(ns, name)],
            "columns": COLUMNS,
            "properties": {},
        }
        snapshot = query.get("snapshot")
        if ns == "ns1":
            body["read_snapshot_id"] = int(snapshot[0]) if snapshot else HEAD
            if self.mode == "r7":
                body["remote"] = True
            return body
        # the Host answers at the pin it is sent, else at its own head
        if snapshot:
            body["read_snapshot_id"] = int(snapshot[0])
        else:
            with self.lock:
                body["read_snapshot_id"] = self.pins[name]
                self.pins[name] += 1
        body["remote"] = True
        if self.mode == "nopin" and name == "events":
            del body["read_snapshot_id"]
        return body

    def answer(self, method, url):
        path, query = url.path, parse_qs(url.query)
        if method == "POST" and path == PREFIX + "/namespaces":
            return 201, {"name": "ns2", "namespace_id": 2}
        if method != "GET" or not path.startswith(PREFIX):
            return 403, {"error": "read_only", "detail": "the stub serves reads only"}
        parts = path[len(PREFIX) :].strip("/").split("/") if path != PREFIX else []
        namespaces = {"ns1": ["t"], "team_42": ["events", "orders"]}
        if parts == []:
            return 200, {
                "name": CATALOG,
                "data_path": "s3://stub/",
                "head_snapshot_id": HEAD,
                "schema_version": 1,
            }
        if parts == ["namespaces"]:
            listing = [{"name": "ns1", "namespace_id": 1}]
            return 200, listing + ([REMOTE_ENTRY] if self.mode != "star" else [])
        if parts[0] != "namespaces" or parts[1] not in namespaces:
            return 404, {"error": "not_found", "detail": "no such namespace"}
        ns = parts[1]
        if len(parts) == 2:
            return 200, REMOTE_ENTRY if ns == "team_42" else {
                "name": ns,
                "namespace_id": 1,
            }
        if parts[2:] == ["tables"]:
            return 200, [
                {"name": t, "table_uuid": UUIDS[(ns, t)]} for t in namespaces[ns]
            ]
        if len(parts) >= 4 and parts[3] in namespaces[ns]:
            if len(parts) == 4:
                return 200, self.table(ns, parts[3], query)
            if parts[4:] == ["scan"]:
                return 200, []
        return 404, {"error": "not_found", "detail": "no such object"}


class RemotePinTest(unittest.TestCase):
    mode = "listed"

    def setUp(self):
        self.stub = Stub(self.mode)
        self.addCleanup(self.stub.close)

    def tearDown(self):
        # case 4: no request under the remote schema ever carries this
        # catalog's snapshot, in any test
        leaked = [
            line
            for line in self.stub.lines
            if "/namespaces/team_42" in line and CUSTOMER_SNAPSHOT.search(line)
        ]
        self.assertEqual(leaked, [])

    def run_sql(self, sql, succeeds=True, attach="", bail=True):
        setup = f"""
            LOAD '{BUILD / "extension/hoglake/hoglake.duckdb_extension"}';
            ATTACH 'hoglake:{CATALOG}' AS lake (ENDPOINT '{self.stub.url}'{attach});
        """
        command = [str(BUILD / "duckdb"), "-unsigned", "-batch"]
        if bail:
            command += ["-bail", "-no-stdin", "-c", setup + sql]
        # without bail, one statement per line on stdin: the shell goes on
        # after an error, so a later statement proves the instance alive
        result = subprocess.run(
            command,
            input=None if bail else setup + sql,
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
        )
        if succeeds is not None:
            self.assertEqual(
                result.returncode == 0, succeeds, result.stdout + result.stderr
            )
        return result

    def requests(self, needle=""):
        """The recorded request lines under the catalog, prefix stripped."""
        return [line.replace(PREFIX, "") for line in self.stub.lines if needle in line]

    def refused(self, sql, *needles, attach=""):
        start = len(self.stub.lines)
        result = self.run_sql(sql, succeeds=False, attach=attach)
        for needle in needles:
            self.assertIn(needle, result.stderr)
        self.assertNotIn("INTERNAL", result.stderr)
        return self.stub.lines[start:]


T42 = "GET /namespaces/team_42/tables"
VERSION_3 = ", SNAPSHOT_VERSION 3"


class ListedRemoteSchema(RemotePinTest):
    def test_01_first_fetch_unpinned_scan_at_pin(self):
        self.run_sql("SELECT count(*) FROM lake.team_42.events;")
        self.assertEqual(
            self.requests("/team_42/"),
            [T42, f"{T42}/events", f"{T42}/events/scan?snapshot=9001"],
        )

    def test_02_self_join_one_fetch_second_table_own_pin(self):
        self.run_sql(
            "BEGIN; SELECT count(*) FROM lake.team_42.events a, lake.team_42.events b;"
            " SELECT count(*) FROM lake.team_42.orders; COMMIT;"
        )
        scan = f"{T42}/events/scan?snapshot=9001"
        self.assertEqual(self.requests("/events"), [f"{T42}/events", scan, scan])
        orders = [f"{T42}/orders", f"{T42}/orders/scan?snapshot=7001"]
        self.assertEqual(self.requests("/orders"), orders)

    def test_03_new_transaction_new_pin_local_pin_local_only(self):
        self.run_sql(
            "BEGIN; SELECT count(*) FROM lake.ns1.t;"
            " SELECT count(*) FROM lake.team_42.events; COMMIT;"
            " SELECT count(*) FROM lake.team_42.events;"
        )
        self.assertEqual(
            self.requests("/events"),
            [f"{T42}/events", f"{T42}/events/scan?snapshot=9001"]
            + [f"{T42}/events", f"{T42}/events/scan?snapshot=9002"],
        )
        pinned = [line for line in self.requests() if CUSTOMER_SNAPSHOT.search(line)]
        local = "GET /namespaces/ns1/tables/t"
        self.assertEqual(pinned, [f"{local}?snapshot=5", f"{local}/scan?snapshot=5"])

    def test_04_schema_scan_never_sends_customer_snapshot(self):
        # every table of the catalog through the schema scan; tearDown
        # asserts the whole log
        result = self.run_sql(
            "SELECT schema_name, table_name FROM duckdb_tables()"
            " WHERE database_name = 'lake' ORDER BY ALL;"
            " SELECT count(*) FROM lake.team_42.orders;"
        )
        self.assertIn("events", result.stdout)
        # the scan's transaction pinned orders at 7001, the next one at 7002
        self.assertEqual(
            self.requests("/orders"),
            [f"{T42}/orders", f"{T42}/orders", f"{T42}/orders/scan?snapshot=7002"],
        )
        self.assertIn("GET /namespaces/ns1/tables/t?snapshot=5", self.requests())

    def test_06_at_timestamp_first_fetch_only_version_refused(self):
        at = " AT (TIMESTAMP => TIMESTAMPTZ '2026-11-01 00:00:00+00');"
        read = "SELECT count(*) FROM lake.team_42.events" + at
        self.run_sql(f"BEGIN; {read} {read} COMMIT;")
        events = self.requests("/events")
        self.assertRegex(events[0], rf"^{re.escape(T42)}/events\?at_timestamp=[^&]+$")
        self.assertEqual(events[1:], [f"{T42}/events/scan?snapshot=9001"] * 2)
        sent = self.refused(
            "SELECT count(*) FROM lake.team_42.events AT (VERSION => 3);",
            "remote",
            "snapshot version",
        )
        self.assertEqual([line for line in sent if "/team_42" in line], [])

    def test_07_attach_snapshot_version_and_time(self):
        read = "SELECT count(*) FROM lake.team_42.events;"
        self.refused(read, "snapshot version", attach=VERSION_3)
        result = self.run_sql(
            "SELECT count(*) FROM lake.ns1.t; SELECT namespace || '.' || table_name"
            " AS row FROM hoglake_table_info('lake');",
            attach=VERSION_3,
        )
        self.assertIn("GET /namespaces/ns1/tables/t/scan?snapshot=3", self.requests())
        self.assertIn("ns1.t", result.stdout)
        self.assertNotIn("team_42", result.stdout)
        # listings, and DuckDB's "did you mean" scan of every schema, skip
        # the remote schema instead of replacing the error
        missing = "SELECT * FROM lake.ns1.missing;"
        self.refused(missing, "does not exist", attach=VERSION_3)
        self.assertEqual(self.requests("/team_42"), [])
        self.run_sql(read, attach=", SNAPSHOT_TIME '2026-11-01 00:00:00+00'")
        events = self.requests("/events")
        self.assertRegex(
            events[0], rf"^{re.escape(T42)}/events\?at_timestamp=2026-11-01"
        )
        self.assertEqual(events[1:], [f"{T42}/events/scan?snapshot=9001"])

    def test_08_writes_and_ddl_refused_before_any_write_request(self):
        for sql in [
            "INSERT INTO lake.team_42.events VALUES (1);",
            "DELETE FROM lake.team_42.events;",
            "UPDATE lake.team_42.events SET id = 2;",
            "ALTER TABLE lake.team_42.events ADD COLUMN c BIGINT;",
            "DROP TABLE lake.team_42.events;",
            "CREATE TABLE lake.team_42.x (a BIGINT);",
            "CREATE TABLE lake.team_42.x AS SELECT 1::BIGINT AS a;",
            "CREATE VIEW lake.team_42.v AS SELECT 1;",
        ]:
            with self.subTest(sql=sql):
                self.refused(sql, "remote schema", "team_42")
        writes = [line for line in self.stub.lines if not line.startswith("GET ")]
        self.assertEqual(writes, [])

    def test_09_table_info_reads_remote_rows_unpinned(self):
        result = self.run_sql(
            "SELECT namespace, table_name FROM hoglake_table_info('lake') ORDER BY ALL;"
        )
        for name in ["events", "orders", "t"]:
            self.assertIn(name, result.stdout)
        tables = self.requests("/team_42/tables/")
        self.assertEqual(tables, [f"{T42}/events", f"{T42}/orders"])
        self.assertIn("GET /namespaces/ns1/tables/t?snapshot=5", self.requests())

    def test_10_local_request_lines_unchanged(self):
        self.run_sql("SELECT count(*) FROM lake.ns1.t; CREATE SCHEMA lake.ns2;")
        self.assertEqual(
            self.requests(),
            ["GET ", "GET /namespaces", "GET /namespaces/ns1/tables", "GET "]
            + ["GET /namespaces/ns1/tables/t?snapshot=5"]
            + ["GET /namespaces/ns1/tables/t/scan?snapshot=5"]
            + ["GET /namespaces", "POST /namespaces"],
        )


class StarToken(RemotePinTest):
    mode = "star"

    def test_05_unlisted_schema_opens_through_get_namespace(self):
        self.run_sql(
            "BEGIN; SELECT count(*) FROM lake.team_42.events;"
            " SELECT count(*) FROM lake.team_42.orders; COMMIT;"
        )
        self.assertEqual(
            self.requests("/namespaces/team_42"),
            ["GET /namespaces/team_42", T42]
            + [f"{T42}/events", f"{T42}/events/scan?snapshot=9001"]
            + [f"{T42}/orders", f"{T42}/orders/scan?snapshot=7001"],
        )
        # today's error for a schema the server does not hold
        sent = self.refused(
            "SELECT count(*) FROM lake.team_99.events;",
            'schema "team_99" does not exist',
        )
        self.assertEqual(
            [line for line in sent if "/team_99" in line],
            [f"GET {PREFIX}/namespaces/team_99"],
        )


class CrossChecks(RemotePinTest):
    def alive_after(self, sql, needle):
        result = self.run_sql(sql + "\nSELECT 'alive';", succeeds=None, bail=False)
        self.assertIn(needle, result.stderr)
        self.assertNotIn("INTERNAL", result.stderr)
        self.assertIn("alive", result.stdout)
        self.assertEqual(self.requests("/scan"), [])

    def test_11_r7_remote_answer_in_local_schema(self):
        self.stub.mode = "r7"
        self.alive_after("SELECT count(*) FROM lake.ns1.t;", "answered as remote")

    def test_11_remote_answer_without_pin(self):
        self.stub.mode = "nopin"
        read = "SELECT count(*) FROM lake.team_42.events;"
        self.alive_after(read, "without read_snapshot_id")


if __name__ == "__main__":
    unittest.main()
