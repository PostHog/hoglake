"""Remote schemas: each remote table reads at its own catalog's snapshot.

The stub plays a Hoglake whose own catalog is at head 5 and which relays a
remote schema, ``team_42``. Its table ``events`` loads at snapshot 9001 of
the catalog that holds it; the local ``ns1.t`` loads at 5. The listing
entry and the namespace answer are exactly ``{"name": "team_42",
"remote": true}``, as the server sends them. httpx_mock fails on any
request it was not given, so a refused call is proved to send nothing.
"""

import uuid

import httpx
import pyarrow as pa
import pytest

from pyhoglake import (
    HoglakeClient,
    HoglakeError,
    MalformedResponseError,
    NotFoundError,
    ops,
)
from pyhoglake.client import Namespace

BASE = "http://hog.test"
CAT = f"{BASE}/v1/catalogs/cat"
CATALOG_WIRE = {
    "name": "cat",
    "data_path": "s3://bkt/lake/",
    "head_snapshot_id": 5,
    "schema_version": 2,
}
LOCAL_ENTRY = {"name": "ns1", "namespace_id": 1}
REMOTE_ENTRY = {"name": "team_42", "remote": True}
REMOTE_NAMESPACE = {"name": "team_42", "remote": True}
COLUMNS = [
    {"name": "id", "type": "long", "field_id": 1, "ordinal": 0, "nullable": False}
]


def _table_wire(namespace, name, read_snapshot_id, **extra):
    return {
        "name": name,
        "namespace": namespace,
        "table_uuid": "0b8ee9ba-79a1-4f3e-b7e5-6a0b6ab6f012",
        "columns": COLUMNS,
        "read_snapshot_id": read_snapshot_id,
        **extra,
    }


REMOTE_TABLE = _table_wire("team_42", "events", 9001, remote=True)
LOCAL_TABLE = _table_wire("ns1", "t", 5)
DATA_FILE = {
    "data_file_id": 10,
    "path": "s3://host/data/events/x.parquet",
    "file_format": "parquet",
    "record_count": 1,
    "file_size_bytes": 10,
    "row_id_start": 0,
    "stats_state": "provided",
    "begin_snapshot": 9000,
}
SCAN = [{"data_file": DATA_FILE}]
EVENTS = "/namespaces/team_42/tables/events"
TS = "2026-11-17T12:00:00Z"


class _NoUploads:
    """An S3 config that fails the test on any use: no write may reach it."""

    def __getattr__(self, name):
        raise AssertionError(f"upload attempted ({name})")


@pytest.fixture
def cat(httpx_mock):
    with HoglakeClient(BASE) as client:
        client.s3 = _NoUploads()
        httpx_mock.add_response(method="GET", url=CAT, json=CATALOG_WIRE)
        yield client.catalog("cat")


def _get(httpx_mock, path, body, status=200, params=None):
    url = httpx.URL(CAT + path, params=params) if params else CAT + path
    httpx_mock.add_response(method="GET", url=url, json=body, status_code=status)


def _lines(httpx_mock):
    """Every request after the catalog GET, as ``METHOD path?query``."""
    prefix = "/v1/catalogs/cat"
    return [
        f"{r.method} {r.url.raw_path.decode().removeprefix(prefix)}"
        for r in httpx_mock.get_requests()[1:]
    ]


def _remote_table(cat, httpx_mock, body=REMOTE_TABLE):
    _get(httpx_mock, "/namespaces", [LOCAL_ENTRY, REMOTE_ENTRY])
    ns = cat.namespace("team_42")
    _get(httpx_mock, EVENTS, body)
    return ns, ns.table("events")


# -- R1: the class, from the server ------------------------------------------


def test_the_listing_entry_classes_a_namespace(cat, httpx_mock):
    for _ in range(3):
        _get(httpx_mock, "/namespaces", [LOCAL_ENTRY, REMOTE_ENTRY])
    assert cat.namespace("team_42").remote is True
    assert cat.namespace("ns1").remote is False
    # an entry without namespace_id parses
    assert cat.list_namespaces() == ["ns1", "team_42"]
    assert _lines(httpx_mock) == ["GET /namespaces"] * 3


def test_a_name_the_listing_omits_is_asked_of_the_namespace(cat, httpx_mock):
    _get(httpx_mock, "/namespaces", [LOCAL_ENTRY])
    _get(httpx_mock, "/namespaces/team_42", REMOTE_NAMESPACE)
    assert cat.namespace("team_42").remote is True
    assert _lines(httpx_mock) == ["GET /namespaces", "GET /namespaces/team_42"]


def test_a_404_from_the_namespace_keeps_the_old_message(cat, httpx_mock):
    _get(httpx_mock, "/namespaces", [LOCAL_ENTRY])
    _get(httpx_mock, "/namespaces/team_9", {"error": "not_found"}, status=404)
    with pytest.raises(NotFoundError) as refused:
        cat.namespace("team_9")
    assert refused.value.message == "namespace 'team_9' not found in catalog 'cat'"
    assert refused.value.status_code == 404


def test_a_namespace_built_directly_asks_once(cat, httpx_mock):
    _get(httpx_mock, "/namespaces/team_42", REMOTE_NAMESPACE)
    ns = Namespace(cat, "team_42")
    assert ns.remote is True
    assert ns.remote is True
    assert _lines(httpx_mock) == ["GET /namespaces/team_42"]


def test_the_table_flag_classes_a_namespace_built_directly(cat, httpx_mock):
    ns = Namespace(cat, "team_42")
    _get(httpx_mock, EVENTS, REMOTE_TABLE)
    table = ns.table("events")
    assert table.remote is True
    assert ns.remote is True  # learned from the answer: no namespace GET
    _get(httpx_mock, EVENTS + "/scan", SCAN, params={"snapshot": 9001})
    table.scan_plan()
    assert _lines(httpx_mock) == [f"GET {EVENTS}", f"GET {EVENTS}/scan?snapshot=9001"]


# -- R2, R3: one pin per Table -------------------------------------------------


@pytest.mark.parametrize(
    "body",
    [REMOTE_TABLE, {k: v for k, v in REMOTE_TABLE.items() if k != "remote"}],
    ids=["flagged", "host-without-flag"],
)
def test_every_read_sends_the_pin(cat, httpx_mock, body):
    _, table = _remote_table(cat, httpx_mock, body)
    assert table.remote is True
    _get(httpx_mock, EVENTS + "/scan", SCAN, params={"snapshot": 9001})
    _get(httpx_mock, EVENTS, body, params={"snapshot": 9001})
    _get(httpx_mock, EVENTS + "/files", [DATA_FILE], params={"snapshot": 9001})
    _get(httpx_mock, EVENTS + "/scan", SCAN, params={"snapshot": 9001})
    table.scan_plan()
    table.info()
    table.files()
    table.scan_plan()  # a second read: the same pin
    assert _lines(httpx_mock) == [
        "GET /namespaces",
        f"GET {EVENTS}",  # the load: no snapshot
        f"GET {EVENTS}/scan?snapshot=9001",
        f"GET {EVENTS}?snapshot=9001",
        f"GET {EVENTS}/files?snapshot=9001",
        f"GET {EVENTS}/scan?snapshot=9001",
    ]


def test_a_second_table_call_takes_a_new_pin(cat, httpx_mock):
    ns, first = _remote_table(cat, httpx_mock)
    _get(httpx_mock, EVENTS, {**REMOTE_TABLE, "read_snapshot_id": 9002})
    second = ns.table("events")
    _get(httpx_mock, EVENTS + "/scan", SCAN, params={"snapshot": 9001})
    _get(httpx_mock, EVENTS + "/scan", SCAN, params={"snapshot": 9002})
    first.scan_plan()
    second.scan_plan()
    assert _lines(httpx_mock)[-2:] == [
        f"GET {EVENTS}/scan?snapshot=9001",
        f"GET {EVENTS}/scan?snapshot=9002",
    ]


# -- R4, R5: no customer snapshot; timestamps pass ------------------------------


@pytest.mark.parametrize(
    ("call", "names"),
    [
        (lambda ns, t: t.scan_plan(snapshot=5), "at_timestamp"),
        (lambda ns, t: t.files(snapshot=5), "at_timestamp"),
        (lambda ns, t: t.info(snapshot=5), "at_timestamp"),
        (lambda ns, t: ns.list_tables(snapshot=5), "at_timestamp"),
        (lambda ns, t: t.changes(from_snapshot=0), "changes"),
    ],
    ids=["scan_plan", "files", "info", "list_tables", "changes"],
)
def test_snapshot_ids_of_this_catalog_are_refused_with_no_request(
    cat, httpx_mock, call, names
):
    ns, table = _remote_table(cat, httpx_mock)
    sent = len(httpx_mock.get_requests())
    with pytest.raises(ValueError, match=names):
        call(ns, table)
    assert len(httpx_mock.get_requests()) == sent


def test_at_timestamp_passes_through_alone(cat, httpx_mock):
    ns, table = _remote_table(cat, httpx_mock)
    at = {"at_timestamp": TS}
    _get(httpx_mock, EVENTS + "/scan", SCAN, params=at)
    _get(httpx_mock, EVENTS + "/files", [DATA_FILE], params=at)
    _get(httpx_mock, EVENTS, {**REMOTE_TABLE, "read_snapshot_id": 8000}, params=at)
    _get(httpx_mock, "/namespaces/team_42/tables", [], params=at)
    table.scan_plan(at_timestamp=TS)
    table.files(at_timestamp=TS)
    table.info(at_timestamp=TS)
    ns.list_tables(at_timestamp=TS)
    assert [dict(r.url.params) for r in httpx_mock.get_requests()[3:]] == [at] * 4
    _get(httpx_mock, EVENTS + "/scan", SCAN, params={"snapshot": 9001})
    table.scan_plan()  # the pin is untouched
    _get(httpx_mock, "/namespaces/team_42/tables", [])
    ns.list_tables()  # a remote listing at head carries nothing
    assert _lines(httpx_mock)[-2:] == [
        f"GET {EVENTS}/scan?snapshot=9001",
        "GET /namespaces/team_42/tables",
    ]


# -- R6: read-only -----------------------------------------------------------------


def _key():
    return str(uuid.uuid4())


TABLE_WRITES = {
    "alter": lambda t: t.alter([ops.add_column("x", pa.int64())]),
    "drop": lambda t: t.drop(author="a", message="m"),
    "truncate": lambda t: t.truncate(author="a", message="m"),
    "append": lambda t: t.append(pa.table({"id": [1]})),
    "prepare_append_files": lambda t: t.prepare_append_files(
        [("/no/such.parquet", None)], idempotency_key=_key()
    ),
    "prepare_append_tables": lambda t: t.prepare_append_tables(
        [(pa.table({"id": [1]}), None)], idempotency_key=_key()
    ),
    "commit_prepared": lambda t: t.commit_prepared({"idempotency_key": _key()}),
    "catalog_commit_prepared": lambda t: t._namespace._catalog.commit_prepared(
        {"idempotency_key": _key()}, table=t
    ),
}
NAMESPACE_WRITES = {
    "create_table": lambda ns: ns.create_table("x", pa.schema([("id", pa.int64())])),
    "create_view": lambda ns: ns.create_view("v", "SELECT 1"),
    "drop": lambda ns: ns.drop(author="a", message="m"),
}


@pytest.mark.parametrize("write", TABLE_WRITES, ids=list(TABLE_WRITES))
def test_every_table_write_is_refused_with_no_request(cat, httpx_mock, write):
    _, table = _remote_table(cat, httpx_mock)
    sent = len(httpx_mock.get_requests())
    with pytest.raises(HoglakeError, match="remote schema is read-only"):
        TABLE_WRITES[write](table)
    assert len(httpx_mock.get_requests()) == sent


@pytest.mark.parametrize("write", NAMESPACE_WRITES, ids=list(NAMESPACE_WRITES))
def test_every_namespace_write_is_refused_with_no_request(cat, httpx_mock, write):
    _get(httpx_mock, "/namespaces", [REMOTE_ENTRY])
    ns = cat.namespace("team_42")
    with pytest.raises(HoglakeError, match="remote schema is read-only"):
        NAMESPACE_WRITES[write](ns)
    assert _lines(httpx_mock) == ["GET /namespaces"]


def test_a_drop_whose_identity_read_says_remote_sends_no_delete(cat, httpx_mock):
    _get(httpx_mock, "/namespaces/team_42", REMOTE_NAMESPACE)
    with pytest.raises(HoglakeError, match="remote schema is read-only"):
        Namespace(cat, "team_42").drop(author="a", message="m")
    assert _lines(httpx_mock) == ["GET /namespaces/team_42"]


def test_a_local_drop_without_namespace_id_raises_clearly(cat, httpx_mock):
    _get(httpx_mock, "/namespaces/ns1", {"name": "ns1"})
    with pytest.raises(MalformedResponseError, match="namespace_id"):
        Namespace(cat, "ns1", remote=False).drop(author="a", message="m")
    assert _lines(httpx_mock) == ["GET /namespaces/ns1"]


# -- R7 and the pin's presence ---------------------------------------------------


def test_a_remote_answer_in_a_local_namespace_fails(cat, httpx_mock):
    _get(httpx_mock, "/namespaces", [LOCAL_ENTRY, REMOTE_ENTRY])
    ns = cat.namespace("ns1")
    _get(httpx_mock, "/namespaces/ns1/tables/t", {**LOCAL_TABLE, "remote": True})
    with pytest.raises(MalformedResponseError, match="classed as local"):
        ns.table("t")


def test_a_remote_answer_without_read_snapshot_id_fails(cat, httpx_mock):
    body = {k: v for k, v in REMOTE_TABLE.items() if k != "read_snapshot_id"}
    with pytest.raises(MalformedResponseError, match="read_snapshot_id"):
        _remote_table(cat, httpx_mock, body)


# -- local tables: today's request lines ------------------------------------------


def test_a_local_table_then_scan_sends_todays_request_lines(cat, httpx_mock):
    _get(httpx_mock, "/namespaces", [LOCAL_ENTRY, REMOTE_ENTRY])
    ns = cat.namespace("ns1")
    _get(httpx_mock, "/namespaces/ns1/tables/t", LOCAL_TABLE)
    table = ns.table("t")
    _get(httpx_mock, "/namespaces/ns1/tables/t/scan", SCAN)
    _get(httpx_mock, "/namespaces/ns1/tables/t/scan", SCAN, params={"snapshot": 3})
    table.scan_plan()
    table.scan_plan(snapshot=3)
    assert table.remote is False
    assert _lines(httpx_mock) == [
        "GET /namespaces",
        "GET /namespaces/ns1/tables/t",
        "GET /namespaces/ns1/tables/t/scan",
        "GET /namespaces/ns1/tables/t/scan?snapshot=3",
    ]
