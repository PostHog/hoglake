"""Scripted hoglake server + fake object store for the flush suite
(test_flush.py).

pyhoglake's own test pattern (pytest-httpx scripted responses + the
duck-typed fake object store of pyhoglake/tests/test_append_unit.py),
hedgerow's fakes.py shape: one module the whole flush suite builds its
server, store, config and payloads through, so wire-shape drift fails in
one place.

The scripted endpoints, exactly the set the flusher may call:

- ``GET /v1/catalogs/{c}`` / ``GET /v1/catalogs/{c}/namespaces`` —
  startup resolution;
- ``GET /v1/catalogs/{c}/namespaces/{n}/tables/{t}?totals=false`` — the
  identity read (startup + shape-cache refresh; ALWAYS totals=false);
- ``GET /v1/catalogs/{c}/options`` — the retention read behind the
  shape cache's TTL (once per flusher);
- ``POST /v1/catalogs/{c}/commit/prepared`` — the commit (scripted per
  call: a snapshot, a typed refusal, or a dropped connection);
- ``GET /v1/catalogs/{c}/commit/receipts/{key}`` — the receipt lookup
  (200 with the receipt, or 404).

pytest-httpx fails a test at teardown for registered-but-unconsumed
responses, so the scriptors below register exactly what a correct
flusher consumes — a superfluous request reddens the test as surely as
a missing one.
"""

from __future__ import annotations

import io
import json
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pyarrow as pa

from millrace import keyspace
from millrace.config import (
    AssignmentMode,
    BackpressureConfig,
    Config,
    EventTimePolicy,
    PoisonConfig,
    TeamKeyCodec,
)
from millrace.stage import PartitionStage

BASE = "http://hog.test"
CATALOG = "cat"
NAMESPACE = "ns1"
TABLE = "events"
TABLE_UUID = "0b8ee9ba-79a1-4f3e-b7e5-6a0b6ab6f012"
RECREATED_UUID = "11111111-2222-3333-4444-555555555555"
DATA_PATH = "s3://bkt/lake"
SCHEMA_VERSION = 2
RETENTION_S = 3600

CATALOG_URL = f"{BASE}/v1/catalogs/{CATALOG}"
NAMESPACES_URL = f"{CATALOG_URL}/namespaces"
TABLE_URL = f"{CATALOG_URL}/namespaces/{NAMESPACE}/tables/{TABLE}"
OPTIONS_URL = f"{CATALOG_URL}/options"
PREPARED_URL = f"{CATALOG_URL}/commit/prepared"
RECEIPTS_URL = f"{CATALOG_URL}/commit/receipts"

#: The destination table's columns: team_id (the identity partition
#: source), timestamp (the month partition source — event time, epoch
#: micros in the payload) and a free-text event name.
COLUMNS_WIRE: list[dict[str, Any]] = [
    {"name": "team_id", "type": "long", "field_id": 1, "ordinal": 0, "nullable": False},
    {
        "name": "timestamp",
        "type": "timestamp",
        "field_id": 2,
        "ordinal": 1,
        "nullable": False,
    },
    {"name": "event", "type": "string", "field_id": 3, "ordinal": 2, "nullable": True},
]

#: The default live spec: identity(team_id), month(timestamp).
SPEC_WIRE: dict[str, Any] = {
    "spec_id": 3,
    "fields": [
        {"source_field_id": 1, "transform": "identity"},
        {"source_field_id": 2, "transform": "month"},
    ],
}

#: A sort spec over the payload's event time — the catalog rule the
#: flusher's Arrow sort must honor (flush.py ``_spec_sort_keys``). The
#: default ``table_wire`` carries NO sort spec (the unsorted-table
#: behavior stays pinned by the happy path); sort tests opt in.
SORT_WIRE: dict[str, Any] = {
    "sort_id": 7,
    "fields": [
        {"source_field_id": 2, "direction": "asc", "null_order": "nulls_first"},
    ],
}


def catalog_wire() -> dict[str, Any]:
    return {
        "name": CATALOG,
        "data_path": DATA_PATH,
        "head_snapshot_id": 90,
        "schema_version": SCHEMA_VERSION,
    }


def options_wire(retention_s: int | None = RETENTION_S) -> dict[str, Any]:
    return {
        "consumer_floor": False,
        "earliest_snapshot_id": 1,
        "snapshot_retention_seconds": retention_s,
    }


def table_wire(
    *,
    uuid: str = TABLE_UUID,
    read_snapshot_id: int | None = 88,
    columns: list[dict[str, Any]] | None = None,
    spec: dict[str, Any] | None = None,
    sort: dict[str, Any] | None = None,
) -> dict[str, Any]:
    wire: dict[str, Any] = {
        "name": TABLE,
        "namespace": NAMESPACE,
        "table_uuid": uuid,
        "columns": COLUMNS_WIRE if columns is None else columns,
        "partition_spec": SPEC_WIRE if spec is None else spec,
    }
    if sort is not None:
        wire["sort_spec"] = sort
    if read_snapshot_id is not None:
        wire["read_snapshot_id"] = read_snapshot_id
    return wire


def script_bootstrap(
    httpx_mock: Any,
    *,
    table: dict[str, Any] | bool | None = None,
    namespaces: list[str] | None = None,
) -> None:
    """The startup-resolution reads, exactly what HoglakeFlusher.resolve
    issues (the table read is the totals=false identity read).
    ``table=False`` registers no table read (resolve is expected to fail
    before it)."""
    httpx_mock.add_response(method="GET", url=CATALOG_URL, json=catalog_wire())
    httpx_mock.add_response(
        method="GET",
        url=NAMESPACES_URL,
        json=[
            {"name": n} for n in (namespaces if namespaces is not None else [NAMESPACE])
        ],
    )
    if table is not False:
        httpx_mock.add_response(
            method="GET",
            url=f"{TABLE_URL}?totals=false",
            json=table_wire() if table is None else table,
        )


def script_options(httpx_mock: Any, *, retention_s: int | None = RETENTION_S) -> None:
    """The retention read behind the shape cache's TTL (once per flusher)."""
    httpx_mock.add_response(
        method="GET", url=OPTIONS_URL, json=options_wire(retention_s)
    )


def script_table_refresh(
    httpx_mock: Any, *, table: dict[str, Any] | None = None
) -> None:
    """A shape-cache refresh (an identity read past the TTL)."""
    httpx_mock.add_response(
        method="GET",
        url=f"{TABLE_URL}?totals=false",
        json=table_wire() if table is None else table,
    )


def script_receipt(
    httpx_mock: Any, idempotency_key: str, *, snapshot_id: int | None
) -> None:
    """A receipt lookup: 404 (never landed) when ``snapshot_id`` is None,
    else the durable 200 receipt."""
    if snapshot_id is None:
        httpx_mock.add_response(
            method="GET",
            url=f"{RECEIPTS_URL}/{idempotency_key}",
            status_code=404,
            json={"error": "not_found", "detail": "no receipt for that key"},
        )
    else:
        httpx_mock.add_response(
            method="GET",
            url=f"{RECEIPTS_URL}/{idempotency_key}",
            json={
                "operation_id": idempotency_key,
                "snapshot_id": snapshot_id,
                "schema_version": SCHEMA_VERSION,
            },
        )


class CommitCapture:
    """The scripted ``POST .../commit/prepared``: one scripted step per
    request, and every request's raw bytes captured.

    A step is a ``httpx.Response`` to answer or an ``httpx.HTTPError`` to
    raise (the dropped-connection / timeout case — the ambiguous one the
    persisted request exists for). Over-running the script is a test
    bug, not a 500.

    ONE capture per test (``:meth:`extend`` to append steps between
    flushes): pytest-httpx prefers the first never-matched registration,
    so two registrations of the same URL shadow each other.
    """

    def __init__(self, httpx_mock: Any, steps: list[httpx.Response | httpx.HTTPError]):
        self._steps = list(steps)
        self.bodies: list[bytes] = []
        self._register(httpx_mock)

    def extend(self, steps: list[httpx.Response | httpx.HTTPError]) -> None:
        """Append scripted steps (e.g. the second flush's responses)."""
        self._steps.extend(steps)

    def _register(self, httpx_mock: Any) -> None:
        def callback(request: httpx.Request) -> httpx.Response:
            index = len(self.bodies)
            self.bodies.append(request.content)
            if index >= len(self._steps):
                raise AssertionError(
                    f"the scripted server received commit #{index + 1} but the "
                    f"script has {len(self._steps)} step(s)"
                )
            step = self._steps[index]
            if isinstance(step, httpx.HTTPError):
                raise step
            return step

        # Reusable: one registration serves the whole script (in order).
        # Always optional: "the script was fully consumed" is asserted by
        # the tests' body counts, and an over-run fails loudly above.
        httpx_mock.add_callback(
            callback,
            method="POST",
            url=PREPARED_URL,
            is_reusable=True,
            is_optional=True,
        )

    @property
    def payloads(self) -> list[dict[str, Any]]:
        return [json.loads(body) for body in self.bodies]


def commit_ok(snapshot_id: int) -> httpx.Response:
    return httpx.Response(
        200, json={"snapshot_id": snapshot_id, "schema_version": SCHEMA_VERSION}
    )


def commit_refusal(
    status: int, error: str, detail: str, *, headers: dict[str, str] | None = None
) -> httpx.Response:
    return httpx.Response(
        status, json={"error": error, "detail": detail}, headers=headers
    )


class _FakeStream(io.BytesIO):
    def __init__(self, store: dict[str, bytes], key: str, *, fail_on_close: bool):
        super().__init__()
        self._store = store
        self._key = key
        self._fail_on_close = fail_on_close

    def close(self) -> None:
        if not self.closed:
            # A close that fails still leaves whatever bytes landed: the
            # truncated-object case the caller cannot distinguish.
            self._store[self._key] = self.getvalue()
            if self._fail_on_close:
                super().close()
                raise OSError(f"object store lost the tail of {self._key}")
        super().close()


class FakeObjectStore:
    """Duck-typed stand-in for ``S3Config`` + pyarrow's S3FileSystem
    (pyhoglake/tests/test_append_unit.py's FakeS3 shape): pyhoglake reads
    ``.filesystem()`` and uploads with ``open_output_stream(key)``.

    ``fail_at`` injects an object-store fault on the Nth (0-based)
    ``open_output_stream`` call — on the open, or on the stream's close
    with ``fail_on_close`` (the truncated-object case)."""

    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.opened: list[str] = []
        self.fail_at: int | None = None
        self.fail_on_close: bool = False

    def filesystem(self) -> FakeObjectStore:
        return self

    def open_output_stream(self, key: str) -> _FakeStream:
        failing = self.fail_at == len(self.opened)
        self.opened.append(key)
        if failing and not self.fail_on_close:
            raise OSError(f"object store refused {key}")
        return _FakeStream(self.files, key, fail_on_close=failing)


class Sleeper:
    """The injected retry/sweep sleep: records, never waits."""

    def __init__(self) -> None:
        self.calls: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)


class FakeMonotonic:
    """The injected monotonic clock for the shape-cache TTL (seconds)."""

    def __init__(self, now: float = 10_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, delta_s: float) -> None:
        self.now += delta_s


def make_config(**overrides: Any) -> Config:
    base: dict[str, Any] = {
        "kafka_bootstrap_servers": "broker:9092",
        "kafka_topic": "events",
        "kafka_group_id": "millrace-events",
        "assignment_mode": AssignmentMode.STATIC,
        "static_partitions": (0,),
        "team_key_codec": TeamKeyCodec.UTF8_DECIMAL,
        "catalog": CATALOG,
        "namespace": NAMESPACE,
        "table": TABLE,
        "stage_store_url": "memory:///",
        "stage_base_path": "millrace",
        "target_output_bytes": 128 * 1024 * 1024,
        "flush_deadline_s": 900,
        "slow_lane_deadline_s": 21600,
        "min_flush_bytes": 1024 * 1024,
        "max_files_per_commit": 512,
        "backpressure": BackpressureConfig(
            pause_staged_bytes=10**9,
            resume_staged_bytes=5 * 10**8,
            pause_oldest_age_s=3600,
            resume_oldest_age_s=1800,
        ),
        "poison": PoisonConfig(max_records_per_run=1000, value_max_bytes=1 << 20),
        "consume_batch_size": 100,
        "poll_timeout_ms": 50,
        "event_time_policy": EventTimePolicy.QUARANTINE,
        "metrics_port": 8000,
    }
    return Config(**{**base, **overrides})


def event_payload(
    team_id: int, ts_us: int, event: str = "pageview", **extra: Any
) -> bytes:
    """One staged event payload: a JSON object keyed by column name (the
    v1 decoder's input). ``timestamp`` is epoch micros."""
    doc: dict[str, Any] = {"team_id": team_id, "timestamp": ts_us, "event": event}
    doc.update(extra)
    return json.dumps(doc, separators=(",", ":")).encode()


def month_wire(ts_us: int) -> str:
    """The epoch-month partition value for a micros timestamp, computed
    INDEPENDENTLY of the code under test (docs/iceberg-federation.md's
    epoch-month rule: floored months since 1970-01)."""
    day = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=ts_us)
    return str((day.year - 1970) * 12 + (day.month - 1))


def decision(
    team_id: int,
    *,
    trigger: str = "size",
    staged_bytes: int = 1,
    first_staged_ts: int,
    age_us: int = 0,
) -> Any:
    from millrace.planner import FlushDecision

    return FlushDecision(
        team_id=team_id,
        trigger=trigger,  # type: ignore[arg-type]
        staged_bytes=staged_bytes,
        first_staged_ts=first_staged_ts,
        age_us=age_us,
    )


async def read_markers(stage: PartitionStage) -> list[tuple[bytes, bytes]]:
    """Every ``flushed/`` key, raw and white-box. Nothing writes this
    prefix any more (settlement is one atomic transaction with the
    receipt in hand — a marker could only restate both), so every
    assertion against this helper is an emptiness pin or a legacy
    collection check."""
    return [
        (kv.key, kv.value) for kv in await stage._scan_prefix(keyspace.FLUSHED_PREFIX)
    ]


def parquet_rows(store: FakeObjectStore, uri: str) -> pa.Table:
    """Read back one uploaded parquet object from the fake store."""
    import pyarrow.parquet as pq

    key = uri.removeprefix("s3://")
    return pq.read_table(pa.BufferReader(store.files[key]))
