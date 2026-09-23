"""Endpoint-wrapper unit tests against a mocked transport (pytest-httpx):
URL, method, body shape, error mapping, pagination."""

import json

import pyarrow as pa
import pytest

from pyhoglake import (
    AlreadyExistsError,
    CommitConflictError,
    ExpiredError,
    HoglakeClient,
    NotFoundError,
    OffsetRegressionError,
    ReconciliationRequiredError,
    ValidationError,
    ops,
)

BASE = "http://hog.test"


@pytest.fixture
def client():
    with HoglakeClient(BASE) as c:
        yield c


TABLE_WIRE = {
    "name": "events",
    "namespace": "ns1",
    "table_uuid": "0b8ee9ba-79a1-4f3e-b7e5-6a0b6ab6f012",
    "columns": [
        {"name": "id", "type": "long", "field_id": 1, "ordinal": 0, "nullable": False},
        {
            "name": "name",
            "type": "string",
            "field_id": 2,
            "ordinal": 1,
            "nullable": True,
        },
    ],
    "record_count": 0,
    "file_count": 0,
    "file_size_bytes": 0,
}

CATALOG_WIRE = {
    "name": "cat",
    "data_path": "s3://bkt/lake/",
    "head_snapshot_id": 5,
    "schema_version": 2,
}

DATA_FILE_WIRE = {
    "data_file_id": 10,
    "path": "s3://bkt/lake/data/ns1/events/x.parquet",
    "file_format": "parquet",
    "record_count": 100,
    "file_size_bytes": 1234,
    "row_id_start": 0,
    "stats_state": "provided",
    "begin_snapshot": 6,
}


def _catalog(client, httpx_mock):
    httpx_mock.add_response(
        method="GET", url=f"{BASE}/v1/catalogs/cat", json=CATALOG_WIRE
    )
    return client.catalog("cat")


def _table(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=TABLE_WIRE,
    )
    from pyhoglake.client import Namespace

    return Namespace(cat, "ns1").table("events")


# -- catalogs ---------------------------------------------------------------


def test_create_catalog(client, httpx_mock):
    httpx_mock.add_response(
        method="POST", url=f"{BASE}/v1/catalogs", json=CATALOG_WIRE, status_code=201
    )
    cat = client.create_catalog("cat", "s3://bkt/lake/")
    req = httpx_mock.get_requests()[0]
    assert json.loads(req.content) == {"name": "cat", "data_path": "s3://bkt/lake/"}
    assert cat.name == "cat"
    assert cat.info.head_snapshot_id == 5


def test_create_catalog_conflict(client, httpx_mock):
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs",
        json={"error": "catalog exists", "detail": "cat"},
        status_code=409,
    )
    with pytest.raises(AlreadyExistsError) as ei:
        client.create_catalog("cat", "s3://x/")
    assert ei.value.status_code == 409
    assert ei.value.detail == "cat"
    assert not ei.value.retryable


def test_get_catalog_not_found(client, httpx_mock):
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/nope",
        json={"error": "no such catalog"},
        status_code=404,
    )
    with pytest.raises(NotFoundError):
        client.catalog("nope")


def test_list_catalogs(client, httpx_mock):
    httpx_mock.add_response(
        method="GET", url=f"{BASE}/v1/catalogs", json=[CATALOG_WIRE]
    )
    cats = client.list_catalogs()
    assert len(cats) == 1
    assert cats[0].data_path == "s3://bkt/lake/"


# -- namespaces -------------------------------------------------------------


def test_create_namespace(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/namespaces",
        json={"name": "ns1"},
        status_code=201,
    )
    ns = cat.create_namespace("ns1")
    assert json.loads(httpx_mock.get_requests()[-1].content) == {"name": "ns1"}
    assert ns.name == "ns1"


def test_namespace_lookup(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces",
        json=[{"name": "ns1"}, {"name": "ns2"}],
    )
    assert cat.namespace("ns1").name == "ns1"
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces",
        json=[{"name": "ns1"}],
    )
    with pytest.raises(NotFoundError):
        cat.namespace("missing")


def test_list_namespaces(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces",
        json=[{"name": "a"}, {"name": "b"}],
    )
    assert cat.list_namespaces() == ["a", "b"]


# -- tables -----------------------------------------------------------------


def test_create_table_body_shape(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    from pyhoglake.client import Namespace

    # A create is a DDL commit: the response names the snapshot it made
    # (#35), so a post-create read can pin to it.
    created = dict(TABLE_WIRE, snapshot_id=7)
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables",
        json=created,
        status_code=201,
    )
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string()),
            pa.field("amount", pa.decimal128(10, 2)),
        ]
    )
    t = Namespace(cat, "ns1").create_table("events", schema)
    assert t.snapshot_id == 7  # the create's own snapshot
    body = json.loads(httpx_mock.get_requests()[-1].content)
    assert body == {
        "name": "events",
        "columns": [
            {"name": "id", "type": "long", "nullable": False},
            {"name": "name", "type": "string", "nullable": True},
            {
                "name": "amount",
                "type": "decimal",
                "nullable": True,
                "type_params": {"precision": 10, "scale": 2},
            },
        ],
    }
    assert t.table_uuid == TABLE_WIRE["table_uuid"]
    assert t.columns[0].field_id == 1


def test_create_table_body_shape_for_nested_columns(client, httpx_mock):
    """The exact JSON a nested create-table sends.

    Pinned as a literal because this body is a CONTRACT with the server's
    ColumnDefDto: the key is `children`, the list child is named
    `element`, the map's are `key` (nullable false) and `value`, and a
    scalar carries no `children` key at all. Any of those wrong is a 400
    or a 422 from a server this client cannot see in a unit test.
    """
    cat = _catalog(client, httpx_mock)
    from pyhoglake.client import Namespace

    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables",
        json=TABLE_WIRE,
        status_code=201,
    )
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("tags", pa.list_(pa.string())),
            pa.field("addr", pa.struct([("city", pa.string()), ("zip", pa.int32())])),
            pa.field("props", pa.map_(pa.string(), pa.int64())),
        ]
    )
    Namespace(cat, "ns1").create_table("nested", schema)
    body = json.loads(httpx_mock.get_requests()[-1].content)
    assert body == {
        "name": "nested",
        "columns": [
            {"name": "id", "type": "long", "nullable": False},
            {
                "name": "tags",
                "type": "list",
                "nullable": True,
                "children": [{"name": "element", "type": "string", "nullable": True}],
            },
            {
                "name": "addr",
                "type": "struct",
                "nullable": True,
                "children": [
                    {"name": "city", "type": "string", "nullable": True},
                    {"name": "zip", "type": "int", "nullable": True},
                ],
            },
            {
                "name": "props",
                "type": "map",
                "nullable": True,
                "children": [
                    {"name": "key", "type": "string", "nullable": False},
                    {"name": "value", "type": "long", "nullable": True},
                ],
            },
        ],
    }


def test_reserved_prefix_message_matches_the_server_phrase():
    """The client's refusal uses the SERVER's phrase.

    Identifiers.validateColumn raises "column name '<path>' uses the
    reserved prefix '_hog': names starting with it belong to hoglake's
    own physical columns (compaction's _hog_row_id)". A user who trips
    the local check and a user who gets the 422 should be able to search
    for the same string, so the shared span is pinned here — the local
    one differs only in reporting every path at once.
    """
    from pyhoglake.client import _check_reserved_columns

    with pytest.raises(ValidationError) as ei:
        _check_reserved_columns(pa.schema([pa.field("_hog_row_id", pa.int64())]))
    msg = str(ei.value)
    # Plural here, singular on the server ('name(s) ... use' vs
    # 'name ... uses'); the searchable span is the same.
    assert "the reserved prefix '_hog':" in msg
    assert "hoglake's own physical columns (compaction's _hog_row_id)" in msg
    assert "_hog_row_id" in msg


def test_create_table_reserved_hog_column_fast_fails(client, httpx_mock):
    """`_hog*` column names are reserved for hoglake internals
    (_hog_row_id is compaction's row-id carrier). The server enforces
    the prefix too now, at every nesting level (hoglake#36); this check
    fast-fails BEFORE the POST leaves the building, and reports every
    offending path at once instead of one per round trip."""
    cat = _catalog(client, httpx_mock)
    from pyhoglake.client import Namespace

    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("_hog_row_id", pa.int64()),
        ]
    )
    with pytest.raises(ValidationError, match=r"_hog.*reserved|reserved.*_hog"):
        Namespace(cat, "ns1").create_table("events", schema)
    # Only the fixture's catalog GET happened — no create POST.
    assert [r.method for r in httpx_mock.get_requests()] == ["GET"]

    # Prefix match, not just the exact carrier name; plain leading
    # underscores stay valid (checked via the same helper).
    with pytest.raises(ValidationError, match="reserved"):
        Namespace(cat, "ns1").create_table(
            "events", pa.schema([pa.field("_hogx", pa.string())])
        )
    from pyhoglake.client import _check_reserved_columns

    _check_reserved_columns(pa.schema([pa.field("_leading", pa.string())]))


def test_get_table_time_travel_params(client, httpx_mock):
    t = _table(client, httpx_mock)
    # TABLE_WIRE carries no snapshot_id: a READ parses it to None, so a
    # client can tell a plain read apart from a fresh DDL pin (#35).
    assert t.snapshot_id is None
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events?snapshot=3",
        json=TABLE_WIRE,
    )
    assert t.info(snapshot=3).snapshot_id is None
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events?at_timestamp=2026-09-04T12%3A00%3A00",
        json=TABLE_WIRE,
    )
    t.info(at_timestamp="2026-09-04T12:00:00")
    with pytest.raises(ValueError):
        t.info(snapshot=3, at_timestamp="2026-09-04T12:00:00")


def test_list_tables(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    from pyhoglake.client import Namespace

    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables",
        json=[{"name": "events", "table_uuid": TABLE_WIRE["table_uuid"]}],
    )
    tables = Namespace(cat, "ns1").list_tables()
    assert tables[0].name == "events"


def test_files_and_scan_plan(client, httpx_mock):
    t = _table(client, httpx_mock)
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events/files?snapshot=7",
        json=[DATA_FILE_WIRE],
    )
    files = t.files(snapshot=7)
    assert files[0].stats_state == "provided"
    assert files[0].row_id_start == 0

    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events/scan",
        json=[{"data_file": DATA_FILE_WIRE}],
    )
    plan = t.scan_plan()
    assert plan[0].data_file.data_file_id == 10
    assert plan[0].delete_file is None


def test_changes_params_and_expired(client, httpx_mock):
    t = _table(client, httpx_mock)
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events/changes?from_snapshot=3&to_snapshot=9",
        json={
            "table_uuid": TABLE_WIRE["table_uuid"],
            "from_snapshot": 3,
            "to_snapshot": 9,
            "files": [DATA_FILE_WIRE],
            "delete_files": [],
        },
    )
    plan = t.changes(3, 9)
    assert plan.from_snapshot == 3
    assert plan.files[0].path.endswith("x.parquet")

    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events/changes?from_snapshot=1",
        json={"error": "range expired", "detail": "earliest is 4"},
        status_code=410,
    )
    with pytest.raises(ExpiredError) as ei:
        t.changes(1)
    assert ei.value.status_code == 410


def test_alter_wire_shape_and_errors(client, httpx_mock):
    t = _table(client, httpx_mock)
    url = f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events/alter"
    altered = dict(TABLE_WIRE)
    altered["columns"] = TABLE_WIRE["columns"] + [
        {
            "name": "score",
            "type": "double",
            "field_id": 3,
            "ordinal": 2,
            "nullable": True,
        }
    ]
    # An alter is a DDL commit: the response names the snapshot it made
    # (#35), so a post-alter read can pin to it.
    altered["snapshot_id"] = 11
    httpx_mock.add_response(method="POST", url=url, json=altered)
    info = t.alter(
        [
            ops.add_column("score", pa.float64()),
            ops.rename_column("old", "new"),
            ops.promote_column("id", "long"),
            ops.rename_table("events2"),
            ops.drop_column("junk"),
            ops.set_partition_spec([ops.partition_field(1, "bucket", 16)]),
        ]
    )
    body = json.loads(httpx_mock.get_requests()[-1].content)
    assert body == {
        "ops": [
            {
                "op": "add_column",
                "column": {"name": "score", "type": "double", "nullable": True},
            },
            {"op": "rename_column", "from": "old", "to": "new"},
            {"op": "promote_column", "name": "id", "to": "long"},
            {"op": "rename_table", "new_name": "events2"},
            {"op": "drop_column", "name": "junk"},
            {
                "op": "set_partition_spec",
                "fields": [
                    {"source_field_id": 1, "transform": "bucket", "transform_param": 16}
                ],
            },
        ]
    }
    assert len(info.columns) == 3
    assert t.columns[2].name == "score"  # cached info updated
    assert info.snapshot_id == 11  # the alter's own snapshot
    assert t.snapshot_id == 11  # cached TableInfo refreshed by alter()

    # A later info() refresh carries no snapshot_id, but the pin is sticky:
    # a pin to an earlier snapshot stays valid after a read, so the refresh
    # must not silently drop it back to a racy head read.
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=TABLE_WIRE,  # no snapshot_id — a plain read
    )
    t.info()
    assert t.snapshot_id == 11  # still pinned, not None

    httpx_mock.add_response(
        method="POST", url=url, json={"error": "conflict"}, status_code=409
    )
    with pytest.raises(CommitConflictError) as ei:
        t.alter([ops.drop_column("id")])
    assert ei.value.retryable is True

    httpx_mock.add_response(
        method="POST", url=url, json={"error": "unknown column"}, status_code=422
    )
    with pytest.raises(ValidationError):
        t.alter([ops.drop_column("ghost")])


def test_drop_table(client, httpx_mock):
    t = _table(client, httpx_mock)
    # Give the table a DDL pin (via alter), so drop() has something to clear.
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events/alter",
        json=dict(TABLE_WIRE, snapshot_id=8),
    )
    t.alter([ops.rename_column("name", "name2")])
    assert t.snapshot_id == 8

    httpx_mock.add_response(
        method="DELETE",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json={"snapshot_id": 9, "schema_version": 3},
    )
    res = t.drop()
    assert res.snapshot_id == 9
    # The table is gone; a pin to one of its snapshots would resolve a dead
    # incarnation. drop() clears it.
    assert t.snapshot_id is None


def test_reads_auto_pin_to_the_ddl_snapshot(client, httpx_mock):
    t = _table(client, httpx_mock)
    # A DDL pin from an alter.
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events/alter",
        json=dict(TABLE_WIRE, snapshot_id=8),
    )
    t.alter([ops.rename_column("name", "name2")])

    # files() with no explicit snapshot pins to the DDL snapshot (#35), so
    # a create/alter followed by a read sees that DDL, not a racing head.
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events/files?snapshot=8",
        json=[DATA_FILE_WIRE],
    )
    assert t.files()[0].data_file_id == 10

    # scan_plan() pins the same way.
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events/scan?snapshot=8",
        json=[{"data_file": DATA_FILE_WIRE}],
    )
    assert t.scan_plan()[0].data_file.data_file_id == 10

    # An explicit snapshot still wins over the pin.
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events/files?snapshot=3",
        json=[DATA_FILE_WIRE],
    )
    assert t.files(snapshot=3)[0].data_file_id == 10


# -- views ------------------------------------------------------------------


def test_views(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    from pyhoglake.client import Namespace

    ns = Namespace(cat, "ns1")
    view_wire = {
        "name": "v1",
        "namespace": "ns1",
        "view_uuid": "9d2a2c40-58c8-45f2-8b39-8a8f5a4d9a01",
        "dialect": "trino",
        "sql": "SELECT 1",
    }
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/views",
        json=view_wire,
        status_code=201,
    )
    v = ns.create_view("v1", "SELECT 1")
    assert json.loads(httpx_mock.get_requests()[-1].content) == {
        "name": "v1",
        "sql": "SELECT 1",
        "dialect": "trino",
    }
    assert v.sql == "SELECT 1"

    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/views/v1",
        json=view_wire,
    )
    assert ns.view("v1").view_uuid == view_wire["view_uuid"]

    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/views",
        json=[view_wire],
    )
    assert [x.name for x in ns.list_views()] == ["v1"]

    httpx_mock.add_response(
        method="DELETE",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/views/v1",
        json={"snapshot_id": 11},
    )
    assert v.drop().snapshot_id == 11


# -- snapshots pagination ---------------------------------------------------


def test_snapshots_pagination(client, httpx_mock):
    cat = _catalog(client, httpx_mock)

    def snap(i):
        return {
            "snapshot_id": i,
            "snapshot_time": "2026-09-04T12:00:00Z",
            "schema_version": 1,
        }

    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/snapshots?after=0&limit=2",
        json={"snapshots": [snap(1), snap(2)], "has_more": True},
    )
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/snapshots?after=2&limit=2",
        json={"snapshots": [snap(3)], "has_more": False},
    )
    got = list(cat.snapshots(limit=2))
    assert [s.snapshot_id for s in got] == [1, 2, 3]
    assert got[0].snapshot_time.year == 2026


def test_snapshots_before_pagination_descending(client, httpx_mock):
    cat = _catalog(client, httpx_mock)

    def snap(i):
        return {
            "snapshot_id": i,
            "snapshot_time": "2026-09-04T12:00:00Z",
            "schema_version": 1,
        }

    # descending walk from head+1: newest first, cursor = last (lowest)
    # id of each page
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/snapshots?before=6&limit=2",
        json={"snapshots": [snap(5), snap(4)], "has_more": True},
    )
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/snapshots?before=4&limit=2",
        json={"snapshots": [snap(3)], "has_more": False},
    )
    got = list(cat.snapshots(before=6, limit=2))
    assert [s.snapshot_id for s in got] == [5, 4, 3]
    # descending requests never carry the 'after' cursor
    for r in httpx_mock.get_requests()[-2:]:
        assert "after" not in dict(r.url.params)


def test_snapshots_before_and_after_mutually_exclusive(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    n_before = len(httpx_mock.get_requests())
    with pytest.raises(ValueError, match="mutually exclusive"):
        cat.snapshots(after=3, before=9)  # eager: no iteration needed
    # enforced client-side: no request ever left the building
    assert len(httpx_mock.get_requests()) == n_before
    # after=0 (the ascending default) is NOT a cursor: before alone is fine
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/snapshots?before=1&limit=1000",
        json={"snapshots": [], "has_more": False},
    )
    assert list(cat.snapshots(before=1)) == []


# -- options / maintenance --------------------------------------------------


def test_options_and_set_retention(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    url = f"{BASE}/v1/catalogs/cat/options"
    httpx_mock.add_response(
        method="GET",
        url=url,
        json={
            "snapshot_retention_seconds": 3600,
            "consumer_floor": True,
            "earliest_snapshot_id": 2,
        },
    )
    opts = cat.options()
    assert opts.snapshot_retention_seconds == 3600
    assert opts.earliest_snapshot_id == 2

    httpx_mock.add_response(
        method="PATCH",
        url=url,
        json={
            "snapshot_retention_seconds": None,
            "consumer_floor": False,
            "earliest_snapshot_id": 2,
        },
    )
    opts = cat.set_retention(None, consumer_floor=False)
    body = json.loads(httpx_mock.get_requests()[-1].content)
    assert body == {"snapshot_retention_seconds": None, "consumer_floor": False}
    assert opts.snapshot_retention_seconds is None


def test_expire_and_cleanup(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/maintenance/expire?batch=10",
        json={
            "snapshots_expired": 3,
            "data_files_queued": 1,
            "delete_files_queued": 0,
            "new_earliest_snapshot_id": 4,
            "floored_by_consumer": "cdc-1",
        },
    )
    res = cat.expire(batch=10)
    assert res.snapshots_expired == 3
    assert res.floored_by_consumer == "cdc-1"

    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/maintenance/cleanup",
        json={"removed": 1, "missing": 0, "still_referenced": 0},
    )
    res = cat.cleanup()
    assert res.removed == 1


# -- consumer offsets -------------------------------------------------------


def test_offsets(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    uuid = TABLE_WIRE["table_uuid"]
    offset_wire = {
        "consumer_id": "cdc-1",
        "table_uuid": uuid,
        "committed_snapshot": 7,
        "updated_at": "2026-09-04T12:00:00Z",
    }
    httpx_mock.add_response(
        method="PUT",
        url=f"{BASE}/v1/catalogs/cat/consumers/cdc-1/offsets/{uuid}",
        json=offset_wire,
    )
    off = cat.commit_offset("cdc-1", uuid, 7)
    assert json.loads(httpx_mock.get_requests()[-1].content) == {"snapshot_id": 7}
    assert off.committed_snapshot == 7

    httpx_mock.add_response(
        method="PUT",
        url=f"{BASE}/v1/catalogs/cat/consumers/cdc-1/offsets/{uuid}",
        json={"error": "offset regression", "detail": "stored 7 > 3"},
        status_code=409,
    )
    with pytest.raises(OffsetRegressionError):
        cat.commit_offset("cdc-1", uuid, 3)

    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/consumers/cdc-1/offsets",
        json=[offset_wire],
    )
    assert cat.offsets("cdc-1")[0].table_uuid == uuid


def test_single_offset_get(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    uuid = TABLE_WIRE["table_uuid"]
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/consumers/cdc-1/offsets/{uuid}",
        json={
            "consumer_id": "cdc-1",
            "table_uuid": uuid,
            "committed_snapshot": 7,
            "updated_at": "2026-09-04T12:00:00Z",
        },
    )
    off = cat.offset("cdc-1", uuid)
    assert off is not None
    assert off.committed_snapshot == 7
    assert off.table_uuid == uuid


def test_offset_routes_percent_encode_table_uuid(client, httpx_mock):
    # bugs.md #23 regression: table_uuid was the ONE path segment not
    # routed through _seg. A hostile/corrupt uuid-shaped value must stay
    # inside its segment (the server will 4xx it, but the URL itself has
    # to be well-formed) — assert the exact encoded request target.
    cat = _catalog(client, httpx_mock)
    evil = "0b8ee9ba-79a1-4f3e-b7e5-6a0b6ab6f012?x=1"
    encoded = "0b8ee9ba-79a1-4f3e-b7e5-6a0b6ab6f012%3Fx%3D1"

    httpx_mock.add_response(
        method="PUT",
        url=f"{BASE}/v1/catalogs/cat/consumers/cdc-1/offsets/{encoded}",
        json={
            "consumer_id": "cdc-1",
            "table_uuid": evil,
            "committed_snapshot": 7,
            "updated_at": "2026-09-04T12:00:00Z",
        },
    )
    cat.commit_offset("cdc-1", evil, 7)
    req = httpx_mock.get_requests()[-1]
    assert dict(req.url.params) == {}  # nothing leaked into the query
    assert req.url.raw_path == (
        f"/v1/catalogs/cat/consumers/cdc-1/offsets/{encoded}".encode()
    )

    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/consumers/cdc-1/offsets/{encoded}",
        json={"error": "no such table"},
        status_code=404,
    )
    assert cat.offset("cdc-1", evil) is None
    req = httpx_mock.get_requests()[-1]
    assert dict(req.url.params) == {}
    assert req.url.raw_path == (
        f"/v1/catalogs/cat/consumers/cdc-1/offsets/{encoded}".encode()
    )


def test_single_offset_get_absent_returns_none(client, httpx_mock):
    # absence is a routine consumer state: None, not NotFoundError (a
    # deliberate divergence from the raise-on-404 idiom)
    cat = _catalog(client, httpx_mock)
    uuid = TABLE_WIRE["table_uuid"]
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/consumers/cdc-1/offsets/{uuid}",
        json={"error": "no offset stored"},
        status_code=404,
    )
    assert cat.offset("cdc-1", uuid) is None


# -- commit conflict on the append path ------------------------------------


def test_commit_conflict_is_retryable(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/commit",
        json={"error": "commit conflict", "detail": "table altered"},
        status_code=409,
    )
    with pytest.raises(CommitConflictError) as ei:
        cat._commit({"appends": []})
    assert ei.value.retryable is True


@pytest.mark.parametrize(
    "body",
    [
        {"error": "commit_conflict: the table was recreated"},
        {"error": "commit_conflict", "detail": "The Table Was Recreated (uuid x != y)"},
    ],
    ids=["marker-in-error", "marker-in-detail-case-insensitive"],
)
def test_commit_409_recreation_maps_to_incarnation_changed(client, httpx_mock, body):
    # the 409 discriminator: "the table was recreated" in message or
    # detail (case-insensitive) -> IncarnationChangedError (never
    # retryable); any other 409 stays CommitConflictError (see above)
    from pyhoglake import IncarnationChangedError

    cat = _catalog(client, httpx_mock)
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/commit",
        json=body,
        status_code=409,
    )
    with pytest.raises(IncarnationChangedError) as ei:
        cat._commit({"appends": []})
    assert ei.value.status_code == 409
    assert ei.value.retryable is False


def test_non_json_error_body(client, httpx_mock):
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat",
        content=b"gateway exploded",
        status_code=502,
    )
    from pyhoglake import HoglakeError

    with pytest.raises(HoglakeError) as ei:
        client.catalog("cat")
    assert ei.value.status_code == 502


def test_3xx_is_a_typed_error_never_success(client, httpx_mock):
    # bugs.md #19 regression: redirects are not followed (httpx default)
    # and a 3xx used to fall into the success branch, leaking a raw
    # JSONDecodeError from resp.json() outside the error taxonomy. It
    # must surface as a typed HoglakeError naming the unexpected status.
    from pyhoglake import HoglakeError

    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat",
        status_code=302,
        headers={"Location": "http://elsewhere.test/v1/catalogs/cat"},
        content=b"",
    )
    with pytest.raises(HoglakeError) as ei:
        client.catalog("cat")
    assert type(ei.value) is HoglakeError  # base type: not a 4xx mapping
    assert ei.value.status_code == 302
    assert "302" in str(ei.value)
    assert "redirect" in ei.value.message
    assert "elsewhere.test" in (ei.value.detail or "")


def test_changes_truncate_requires_reconciliation(client, httpx_mock):
    table = _table(client, httpx_mock)
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events/changes?from_snapshot=3",
        status_code=409,
        json={"error": "reconciliation_required", "detail": "Reconcile after truncate"},
    )
    with pytest.raises(ReconciliationRequiredError, match="Reconcile") as failure:
        table.changes(3)
    assert failure.value.retryable is False


# -- table/column comments and properties (#169) ----------------------------


def test_comment_and_properties_ops_wire_shape(client, httpx_mock):
    t = _table(client, httpx_mock)
    url = f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events/alter"
    httpx_mock.add_response(
        method="POST", url=url, json=dict(TABLE_WIRE, snapshot_id=12)
    )
    t.alter(
        [
            ops.set_table_comment("page views"),
            ops.set_column_comment("id", "surrogate key"),
            ops.set_column_comment("name", None),  # None removes
            ops.set_properties({"owner": "web-analytics", "quality.tier": "gold"}),
        ]
    )
    body = json.loads(httpx_mock.get_requests()[-1].content)
    assert body == {
        "ops": [
            {"op": "set_table_comment", "comment": "page views"},
            {"op": "set_column_comment", "name": "id", "comment": "surrogate key"},
            {"op": "set_column_comment", "name": "name", "comment": None},
            {
                "op": "set_properties",
                "properties": {"owner": "web-analytics", "quality.tier": "gold"},
            },
        ]
    }


def test_table_comment_and_properties_parse_from_wire(client, httpx_mock):
    commented = dict(
        TABLE_WIRE,
        comment="page views",
        properties={"owner": "web-analytics"},
        columns=[
            dict(TABLE_WIRE["columns"][0], comment="surrogate key"),
            TABLE_WIRE["columns"][1],  # no comment
        ],
    )
    cat = _catalog(client, httpx_mock)
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=commented,
    )
    from pyhoglake.client import Namespace

    t = Namespace(cat, "ns1").table("events")
    assert t.comment == "page views"
    assert t.properties == {"owner": "web-analytics"}
    assert t.columns[0].comment == "surrogate key"
    assert t.columns[1].comment is None  # absent, not ""


def test_table_without_metadata_parses_to_none(client, httpx_mock):
    t = _table(client, httpx_mock)  # TABLE_WIRE has no comment/properties
    assert t.comment is None
    assert t.properties is None
    assert all(c.comment is None for c in t.columns)


def test_comment_ops_client_side_validation():
    # Over the length cap or containing NUL fails before the round trip.
    with pytest.raises(ValueError, match="16384"):
        ops.set_table_comment("x" * 16385)
    with pytest.raises(ValueError, match="no NUL"):
        ops.set_table_comment("a\x00b")
    with pytest.raises(ValueError, match="16384"):
        ops.set_column_comment("c", "x" * 16385)
    # Exactly at the cap is fine, and None (remove) skips validation.
    ops.set_table_comment("x" * 16384)
    ops.set_table_comment(None)
    ops.set_column_comment("c", None)


def test_set_properties_client_side_validation():
    with pytest.raises(ValueError, match="at most 100"):
        ops.set_properties({f"k{i}": "v" for i in range(101)})
    with pytest.raises(ValueError, match="reserved"):
        ops.set_properties({"hoglake.internal": "v"})
    with pytest.raises(ValueError, match="reserved"):
        ops.set_properties({"comment": "v"})
    with pytest.raises(ValueError, match="invalid or reserved"):
        ops.set_properties({"Bad Key": "v"})
    with pytest.raises(ValueError, match="4096"):
        ops.set_properties({"k": "v" * 4097})
    with pytest.raises(ValueError, match="no NUL"):
        ops.set_properties({"k": "a\x00b"})
    # A valid set passes, and an empty dict (clear) is valid.
    ops.set_properties({"owner": "web-analytics", "quality.tier": "gold"})
    ops.set_properties({})
