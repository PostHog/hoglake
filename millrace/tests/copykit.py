"""A stateful fake gigahog, object store and scratch bucket for the
millrace-copy suite (test_copy.py, test_copy_contract.py).

The fake answers exactly the routes the tool calls. Like the server, it
answers a used key from its receipt before it checks the read basis, and
refuses the key with another payload (422); a read basis below the expiry
floor is a 410, a DDL after it a 409. ``Store`` is both the pyarrow
filesystem and the boto3 client (conditional ``IfNoneMatch`` creates).
"""

from __future__ import annotations

import io
import itertools
import json
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyhoglake as hl
from botocore.exceptions import ClientError

from millrace import flush as fl
from millrace.copy import Ctx, load_config

BASE, CAT, DATA = "http://hog.test", "cat", "s3://lake/root"
SRC_UUID = "00000000-0000-4000-8000-00000000000a"
DEST_UUID = "00000000-0000-4000-8000-00000000000b"
DAY = 20454  # 2026-01-01
DAY_US = 86_400_000_000

COLS: list[dict[str, Any]] = [
    {"name": "team_id", "type": "long", "field_id": 1, "ordinal": 0},
    {"name": "timestamp", "type": "timestamptz", "field_id": 2, "ordinal": 1},
    {"name": "event", "type": "string", "field_id": 3, "ordinal": 2},
    {"name": "properties", "type": "string", "field_id": 4, "ordinal": 3},
]
SV = {"name": "properties_sv", "type": "variant", "field_id": 5, "ordinal": 4}
SPEC = {
    "spec_id": 1,
    "fields": [
        {"source_field_id": 1, "transform": "identity"},
        {"source_field_id": 2, "transform": "day"},
    ],
}
ENV = {
    "HOGLAKE_URL": BASE,
    "COPY_CATALOG": CAT,
    "COPY_SOURCE": "ingest.events",
    "COPY_SOURCE_UUID": SRC_UUID,
    "COPY_DEST": "ingest.events_v2",
    "COPY_DEST_UUID": DEST_UUID,
    "COPY_RUN": "r1",
    "COPY_SCRATCH_URL": "s3://scratch/copy/cat",
    "COPY_S_TIME": "2026-06-01T00:00:00+00:00",
}


class Store:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def open_input_file(self, key: str) -> pa.BufferReader:
        if key not in self.objects:
            raise FileNotFoundError(key)
        return pa.BufferReader(self.objects[key])

    def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:
        body = self.objects.get(f"{Bucket}/{Key}")
        if body is None:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(body)}

    def put_object(
        self, Bucket: str, Key: str, Body: Any, IfNoneMatch: str = ""
    ) -> None:
        key = f"{Bucket}/{Key}"
        if IfNoneMatch and key in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.objects[key] = Body.read() if hasattr(Body, "read") else bytes(Body)

    def record(self, unit: str, run: str = "r1") -> dict[str, Any]:
        return json.loads(self.objects[f"scratch/copy/cat/{run}/units/{unit}.json"])

    def put_record(self, rec: dict[str, Any], run: str = "r1") -> None:
        key = f"scratch/copy/cat/{run}/units/{rec['unit']}.json"
        self.objects[key] = json.dumps(rec).encode()


def _err(status: int, error: str) -> httpx.Response:
    return httpx.Response(status, json={"error": error, "detail": error})


class Lake:
    def __init__(self) -> None:
        self.store, self.snap, self.floor = Store(), 10, 0
        self.ids = itertools.count(1)
        dest_wire = {"columns": [*COLS, SV], "object_prefix_field": "team_id"}
        self.tables: dict[str, dict[str, Any]] = {
            "events": {"uuid": SRC_UUID, "columns": COLS, "files": []},
            "events_v2": {"uuid": DEST_UUID, "files": [], **dest_wire},
        }
        self.receipts: dict[str, tuple[str, int]] = {}
        self.ddl: dict[str, int] = {}
        self.fail: list[int] = []  # statuses answered before the next commits
        self.flaky: list[int] = []  # statuses answered before the next /files
        self.commits = 0
        self.debt: dict[str, Any] = {
            "sampled_at": "2999-01-01T00:00:00+00:00",
            "partitions": [],
        }

    # -- the wire ---------------------------------------------------------------

    def info(self, name: str) -> hl.TableInfo:
        return hl.TableInfo.from_wire(self._wire(name))

    def _wire(self, name: str) -> dict[str, Any]:
        t = self.tables[name]
        wire = {"name": name, "namespace": "ingest", "table_uuid": t["uuid"]}
        wire.update(
            columns=t["columns"], partition_spec=SPEC, read_snapshot_id=self.snap
        )
        if "object_prefix_field" in t:
            wire["object_prefix_field"] = t["object_prefix_field"]
        return wire

    def live(self, name: str, at: int | None = None) -> list[dict[str, Any]]:
        at = self.snap if at is None else at
        return [
            f
            for f in self.tables[name]["files"]
            if f["begin_snapshot"] <= at and (f["_end"] is None or f["_end"] > at)
        ]

    def handle(self, req: httpx.Request) -> httpx.Response:
        parts = req.url.path.split("/")[3:]  # after /v1/catalogs
        q = req.url.params
        if parts == [CAT]:
            body = {"name": CAT, "data_path": DATA, "schema_version": 1}
            return httpx.Response(200, json={**body, "head_snapshot_id": self.snap})
        if parts[1:] == ["stats", "partitions"]:
            return httpx.Response(200, json=self.debt)
        if parts[1:3] == ["commit", "receipts"]:
            got = self.receipts.get(parts[3])
            if got is None:
                return _err(404, "not_found")
            body = {
                "operation_id": parts[3],
                "snapshot_id": got[1],
                "schema_version": 1,
            }
            return httpx.Response(200, json=body)
        if parts[1:] == ["commit", "prepared"]:
            return self._commit(json.loads(req.content))
        name, route = parts[4], parts[5:]
        at = int(q["snapshot"]) if "snapshot" in q else None
        if at is not None and at < self.floor:
            return _err(410, "snapshot_expired")
        if not route:
            return httpx.Response(200, json=self._wire(name))
        files = self.live(name, at)
        if route == ["files"] and self.flaky:
            return _err(self.flaky.pop(0), "unavailable")
        if route == ["scan"]:
            rows = [{"data_file": _public(f), "delete_file": f["_dv"]} for f in files]
            return httpx.Response(200, json=rows)
        for want in q.get_list("partition"):
            i, _, v = want.partition(":")
            files = [f for f in files if f["partition_values"][int(i)] == v]
        offset, limit = int(q.get("offset", 0)), int(q.get("limit", 10**9))
        page = files[offset : offset + limit]
        return httpx.Response(200, json=[_public(f) for f in page])

    def _commit(self, body: dict[str, Any]) -> httpx.Response:
        key, fingerprint = body["idempotency_key"], json.dumps(body, sort_keys=True)
        if key in self.receipts:
            if self.receipts[key][0] != fingerprint:
                return _err(422, "validation")
            return httpx.Response(200, json={"snapshot_id": self.receipts[key][1]})
        if self.fail:
            return _err(self.fail.pop(0), "unavailable")
        if body["read_snapshot"] < self.floor:
            return _err(410, "read_snapshot_expired")
        (append,) = body["appends"]
        if self.ddl.get(append["table"], 0) > body["read_snapshot"]:
            return _err(409, "ddl_since_read_snapshot")
        self.snap += 1
        for reg in append["files"]:
            self.add(
                append["table"],
                reg["path"],
                reg["record_count"],
                reg["partition_values"],
            )
        self.receipts[key] = (fingerprint, self.snap)
        self.commits += 1
        return httpx.Response(200, json={"snapshot_id": self.snap})

    # -- test levers ------------------------------------------------------------

    def add(
        self, name: str, path: str, rows: int, values: list[Any], size: int = 100
    ) -> dict[str, Any]:
        f = {"data_file_id": next(self.ids), "path": path, "file_format": "parquet"}
        f.update(record_count=rows, file_size_bytes=size, row_id_start=0)
        f.update(stats_state="provided", begin_snapshot=self.snap, spec_id=1)
        f.update(partition_values=list(values), _end=None, _dv=None)
        self.tables[name]["files"].append(f)
        return f

    def write(
        self,
        team: int | None,
        rows: list[tuple[str, str | None]],
        *,
        day: int = DAY,
        drop: tuple[str, ...] = (),
        rename: dict[str, str] | None = None,
        field_ids: bool = True,
    ) -> dict[str, Any]:
        """One source file of ``(event, properties)`` rows for (team, day),
        committed at a new snapshot."""
        schema = fl.arrow_schema([hl.Column.from_wire(c) for c in COLS])
        data = pa.table(
            {
                "team_id": pa.array([team] * len(rows), pa.int64()),
                "timestamp": pa.array([day * DAY_US + i for i in range(len(rows))]),
                "event": [r[0] for r in rows],
                "properties": [r[1] for r in rows],
            }
        )
        data = data.cast(pa.schema([f for f in schema]))
        fields = [f if field_ids else f.remove_metadata() for f in data.schema]
        fields = [f.with_name((rename or {}).get(f.name, f.name)) for f in fields]
        data = pa.Table.from_arrays(data.columns, schema=pa.schema(fields))
        data = data.drop_columns([(rename or {}).get(n, n) for n in drop])
        path = f"{DATA}/data/ingest/events/{next(self.ids)}.parquet"
        sink = io.BytesIO()
        pq.write_table(data, sink)
        self.store.objects[path[5:]] = sink.getvalue()
        self.snap += 1
        values = [None if team is None else str(team), str(day)]
        return self.add("events", path, len(rows), values, size=len(sink.getvalue()))

    def end(self, file: dict[str, Any]) -> None:
        self.snap += 1
        file["_end"] = self.snap

    def merge(
        self, name: str, files: list[dict[str, Any]], path: str
    ) -> dict[str, Any]:
        """Compaction: the inputs leave head, one output holds their rows."""
        for f in files:
            self.end(f)
        rows = sum(f["record_count"] for f in files)
        return self.add(name, path, rows, files[0]["partition_values"])

    def ctx(self, **env: str) -> Ctx:
        client = hl.HoglakeClient(BASE)
        transport = httpx.MockTransport(self.handle)
        client._http = httpx.Client(base_url=BASE + "/v1", transport=transport)
        return Ctx(load_config({**ENV, **env}), client, self.store, self.store)

    def dest_rows(self) -> dict[tuple[str, str], int]:
        out: dict[tuple[str, str], int] = {}
        for f in self.live("events_v2"):
            key = (f["partition_values"][0], f["partition_values"][1])
            out[key] = out.get(key, 0) + f["record_count"]
        return out


def _public(f: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in f.items() if not k.startswith("_")}


#: A standard source: teams 2 and 3 over two days (two files for team 2's
#: first day), with properties as text, "" and null, and a lone surrogate.
PROPS = ['{"$browser":"Chrome"}', "", None, '{"$os":"\\ud800"}']


def seed(lake: Lake) -> None:
    lake.write(2, [("a", PROPS[0]), ("b", PROPS[1])])
    lake.write(2, [("c", PROPS[2]), ("d", PROPS[3])])
    lake.write(2, [("e", PROPS[0])], day=DAY + 1)
    lake.write(3, [("f", PROPS[3]), ("g", PROPS[0]), ("h", None)])
