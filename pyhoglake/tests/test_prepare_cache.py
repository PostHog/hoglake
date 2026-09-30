"""The writer path's cached TableInfo: zero table GETs per flush, and the
refusals that invalidate it.

The mechanism is the server's OCC, with no new wire field. An append
prepared against a `TableInfo` whose `read_snapshot_id` is S and
committed with
`read_snapshot = S` is accepted exactly when nothing has altered,
dropped or recreated the table since S — which is the entire question
the writer used to re-read the table to answer. So the info and the
conflict basis come from the SAME read, and the flush makes no table GET
at all.

The old shape was two GETs per flush: a catalog GET for head and a table
GET whose live-totals scan (a count and two sums over every live file
row, ~10M rows on prod-us) is what #232 is about.

Three things make it safe rather than merely cheap, and each has a test
below: a stale read_snapshot is REFUSED, never silently accepted; the
refusal is typed non-retryable so a retry loop re-prepares instead of
replaying; and the cache refreshes itself before the snapshot can sink
below the catalog's expiry floor, because a refusal after the parquet is
uploaded costs a re-encode and a set of orphaned objects.
"""

import json
import uuid

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from test_append_unit import FakeS3

from pyhoglake import (
    DdlSinceReadSnapshotError,
    HoglakeClient,
    IncarnationChangedError,
    ReadSnapshotExpiredError,
    TableInfo,
)
from pyhoglake.client import Namespace, Table

BASE = "http://hog.test"

CATALOG_URL = f"{BASE}/v1/catalogs/cat"
OPTIONS_URL = f"{BASE}/v1/catalogs/cat/options"
TABLE_URL = f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events"
WRITER_TABLE_URL = f"{TABLE_URL}?totals=false"
PREPARED_URL = f"{BASE}/v1/catalogs/cat/commit/prepared"

CATALOG_WIRE = {
    "name": "cat",
    "data_path": "s3://bkt/lake",
    "head_snapshot_id": 90,
    "schema_version": 2,
}

# prod-us: one hour, so the floor sits about an hour behind head.
OPTIONS_WIRE = {
    "consumer_floor": False,
    "earliest_snapshot_id": 40,
    "snapshot_retention_seconds": 3600,
}

# A server that reports the snapshot a READ resolved at
# (`read_snapshot_id` — NOT `snapshot_id`, which is a DDL receipt and
# which reads pin to). That field is what makes the cache usable;
# without it the writer path falls back to what it always did.
TABLE_WIRE = {
    "name": "events",
    "namespace": "ns1",
    "table_uuid": "0b8ee9ba-79a1-4f3e-b7e5-6a0b6ab6f012",
    "columns": [
        {"name": "id", "type": "long", "field_id": 1, "ordinal": 0, "nullable": False},
        {
            "name": "team",
            "type": "long",
            "field_id": 2,
            "ordinal": 1,
            "nullable": True,
        },
    ],
    "record_count": 0,
    "file_count": 0,
    "file_size_bytes": 0,
    "read_snapshot_id": 88,
    "partition_spec": {
        "spec_id": 3,
        "fields": [{"source_field_id": 2, "transform": "identity"}],
    },
}

# The same table, read again after DDL.
REREAD_WIRE = {
    **TABLE_WIRE,
    "read_snapshot_id": 95,
    "partition_spec": {
        "spec_id": 4,
        "fields": [{"source_field_id": 2, "transform": "bucket", "transform_param": 8}],
    },
}

# An older server: no read_snapshot_id at all.
LEGACY_TABLE_WIRE = {k: v for k, v in TABLE_WIRE.items() if k != "read_snapshot_id"}

# The same, still partitioned — the shape `Table.append` runs against a
# server that does not report the resolved snapshot, i.e. every server
# until TableDto.read_snapshot_id lands.
LEGACY_PARTITIONED_WIRE = LEGACY_TABLE_WIRE

DDL_409 = {
    "error": "ddl_since_read_snapshot",
    "detail": "concurrent DDL since snapshot 88 on table(s): ns1.events",
    "tables": ["ns1.events"],
    "read_snapshot": 88,
    "retry": "re-prepare",
}

RECREATED_409 = {
    "error": "table_recreated",
    "detail": "table 'ns1.events' no longer has the expected UUID",
    "tables": ["ns1.events"],
    "retry": "re-prepare",
}

EXPIRED_410 = {
    "error": "expired",
    "detail": "read_snapshot 88 is below the expiry floor 120",
}


@pytest.fixture
def fake_s3():
    return FakeS3()


def _client(httpx_mock, fake_s3):
    client = HoglakeClient(BASE)
    client.s3 = fake_s3
    httpx_mock.add_response(method="GET", url=CATALOG_URL, json=CATALOG_WIRE)
    return client, client.catalog("cat")


@pytest.fixture
def table(httpx_mock, fake_s3):
    client, cat = _client(httpx_mock, fake_s3)
    httpx_mock.add_response(method="GET", url=TABLE_URL, json=TABLE_WIRE)
    t = Namespace(cat, "ns1").table("events")
    # The retention read is one GET per Catalog object, not per flush.
    httpx_mock.add_response(
        method="GET",
        url=OPTIONS_URL,
        json=OPTIONS_WIRE,
        is_optional=True,
        is_reusable=True,
    )
    yield t
    client.close()


def _parquet(table, tmp_path, name):
    from pyhoglake.types import columns_to_arrow_schema

    schema = columns_to_arrow_schema(table.columns)
    path = tmp_path / name
    pq.write_table(pa.Table.from_pylist([{"id": 1, "team": 7}], schema=schema), path)
    return str(path)


def _gets(httpx_mock, suffix):
    return [
        r
        for r in httpx_mock.get_requests()
        if r.method == "GET" and r.url.path.endswith(suffix)
    ]


def _prepare(table, tmp_path, name):
    return table.prepare_append_files(
        [(_parquet(table, tmp_path, name), ("7",))],
        idempotency_key=str(uuid.uuid4()),
    )


def test_repeated_prepares_and_commits_make_no_reads_at_all(
    table, httpx_mock, fake_s3, tmp_path
):
    """The steady state, and the whole point: N flushes, zero GETs.

    Not just zero TABLE GETs — zero catalog GETs too, because the
    read_snapshot comes off the cached info rather than from a head read.
    One POST per flush and nothing else.
    """
    httpx_mock.add_response(
        method="POST",
        url=PREPARED_URL,
        json={"snapshot_id": 91, "schema_version": 2},
        is_reusable=True,
    )
    catalog = table._namespace._catalog
    before = len(httpx_mock.get_requests())
    for i in range(4):
        request = _prepare(table, tmp_path, f"f{i}.parquet")
        # The cached read's own snapshot is the conflict basis.
        assert request["read_snapshot"] == TABLE_WIRE["read_snapshot_id"]
        catalog.commit_prepared(request, table=table)
    after = httpx_mock.get_requests()[before:]
    assert [r for r in after if r.method == "GET" and "options" not in r.url.path] == []
    assert len([r for r in after if r.method == "POST"]) == 4
    assert len(fake_s3.files) == 4


def test_the_payload_pairs_the_cached_shape_with_its_own_snapshot(
    table, httpx_mock, tmp_path
):
    request = _prepare(table, tmp_path, "f.parquet")
    (append,) = request["appends"]
    assert request["read_snapshot"] == 88
    assert append["expected_table_uuid"] == TABLE_WIRE["table_uuid"]
    # No new wire fields: the guard is read_snapshot + expected_table_uuid,
    # both of which the contract already had.
    assert set(append) == {"namespace", "table", "expected_table_uuid", "files"}


@pytest.mark.parametrize(
    "body,status,error",
    [
        (DDL_409, 409, DdlSinceReadSnapshotError),
        (RECREATED_409, 409, IncarnationChangedError),
        (EXPIRED_410, 410, ReadSnapshotExpiredError),
    ],
    ids=["ddl", "recreated", "below-floor"],
)
def test_each_re_prepare_refusal_invalidates_the_cache_and_the_next_prepare_re_reads(
    table, httpx_mock, fake_s3, tmp_path, body, status, error
):
    request = _prepare(table, tmp_path, "a.parquet")
    httpx_mock.add_response(
        method="POST", url=PREPARED_URL, json=body, status_code=status
    )
    catalog = table._namespace._catalog
    with pytest.raises(error) as excinfo:
        catalog.commit_prepared(request, table=table)
    assert not excinfo.value.retryable
    assert excinfo.value.re_prepare
    if error is ReadSnapshotExpiredError:
        # A below-floor refusal is the one observation that proves the
        # cached RETENTION wrong too — retention can be shortened live,
        # and a writer holding the old longer threshold would otherwise
        # take this 410 once per new-retention period forever.
        assert catalog._retention_cache is None

    # The cache is gone, so the next prepare re-reads: ONE identity GET
    # (totals=false) and ONE catalog GET, in that order — head must not
    # be newer than the identity read or DDL between them would fall
    # outside the conflict window. The re-read's own snapshot is what the
    # new payload carries.
    before = len(httpx_mock.get_requests())
    httpx_mock.add_response(method="GET", url=CATALOG_URL, json=CATALOG_WIRE)
    httpx_mock.add_response(method="GET", url=WRITER_TABLE_URL, json=REREAD_WIRE)
    reprepared = _prepare(table, tmp_path, "b.parquet")
    reads = [
        r
        for r in httpx_mock.get_requests()[before:]
        if r.method == "GET" and "options" not in r.url.path
    ]
    assert [r.url.path for r in reads] == [
        "/v1/catalogs/cat",
        "/v1/catalogs/cat/namespaces/ns1/tables/events",
    ]
    assert reads[1].url.params.get("totals") == "false"
    assert reprepared["read_snapshot"] == REREAD_WIRE["read_snapshot_id"]

    # And the re-read refilled the cache: no further reads. (Options is
    # excluded because the below-floor case deliberately dropped the
    # cached retention too, so the next check re-reads it once.)
    def data_gets():
        return [
            r
            for r in httpx_mock.get_requests()
            if r.method == "GET" and "options" not in r.url.path
        ]

    after = len(data_gets())
    _prepare(table, tmp_path, "c.parquet")
    assert len(data_gets()) == after


def test_an_ordinary_commit_conflict_leaves_the_cache_alone(
    table, httpx_mock, fake_s3, tmp_path
):
    """A retryable conflict is replayed with the SAME payload, so the
    cached info it was built from must survive. Invalidating here would
    buy a table read on every OCC retry, which is the cost #232
    removes."""
    from pyhoglake import CommitConflictError

    request = _prepare(table, tmp_path, "a.parquet")
    httpx_mock.add_response(
        method="POST",
        url=PREPARED_URL,
        json={
            "error": "commit_conflict",
            "detail": "a concurrent insert moved the read set",
        },
        status_code=409,
    )
    catalog = table._namespace._catalog
    with pytest.raises(CommitConflictError) as ei:
        catalog.commit_prepared(request, table=table)
    # A plain commit_conflict, not the DDL subclass — the retryable half
    # of the split, and the flags are what tell them apart now that the
    # class does not.
    assert not isinstance(ei.value, DdlSinceReadSnapshotError)
    assert ei.value.retryable
    assert not ei.value.re_prepare
    before = len(httpx_mock.get_requests())
    _prepare(table, tmp_path, "b.parquet")
    assert len(httpx_mock.get_requests()) == before


def test_a_cache_older_than_half_the_retention_refreshes_before_preparing(
    table, httpx_mock, fake_s3, tmp_path, monkeypatch
):
    """The prod fact this exists for: `snapshot_retention_seconds = 3600`,
    so the expiry floor sits about an hour behind head and a cached
    read_snapshot older than that is refused on EVERY commit — after the
    flush's parquet is already uploaded, so each refusal costs a
    re-encode and a set of orphaned objects. Refreshing at half the
    retention means it never happens.
    """
    import pyhoglake.client as client_module

    # Age the cache past 1800s. monotonic(), not the wall clock: elapsed
    # time is the question and a clock step must not answer it.
    base = client_module.time.monotonic()
    monkeypatch.setattr(client_module.time, "monotonic", lambda: base + 1801.0)

    before = len(httpx_mock.get_requests())
    httpx_mock.add_response(method="GET", url=CATALOG_URL, json=CATALOG_WIRE)
    httpx_mock.add_response(method="GET", url=WRITER_TABLE_URL, json=REREAD_WIRE)
    request = _prepare(table, tmp_path, "f.parquet")
    reads = [r for r in httpx_mock.get_requests()[before:] if r.method == "GET"]
    # The options GET may be paid here too; the table read is the point.
    assert any(r.url.path.endswith("/tables/events") for r in reads)
    assert request["read_snapshot"] == REREAD_WIRE["read_snapshot_id"]


def test_a_cache_younger_than_half_the_retention_is_used(
    table, httpx_mock, fake_s3, tmp_path, monkeypatch
):
    import pyhoglake.client as client_module

    base = client_module.time.monotonic()
    monkeypatch.setattr(client_module.time, "monotonic", lambda: base + 1799.0)
    before = len(httpx_mock.get_requests())
    request = _prepare(table, tmp_path, "f.parquet")
    reads = [
        r
        for r in httpx_mock.get_requests()[before:]
        if r.method == "GET" and r.url.path.endswith("/tables/events")
    ]
    assert reads == []
    assert request["read_snapshot"] == TABLE_WIRE["read_snapshot_id"]


def test_retention_is_read_once_per_catalog_not_once_per_flush(
    table, httpx_mock, fake_s3, tmp_path
):
    for i in range(3):
        _prepare(table, tmp_path, f"f{i}.parquet")
    assert len(_gets(httpx_mock, "/options")) <= 1


def test_disabled_retention_never_ages_the_cache_out(httpx_mock, fake_s3, tmp_path):
    # With retention off the expiry floor does not advance on its own, so
    # a cached snapshot cannot age out and no refresh is due however old
    # it gets.
    import pyhoglake.client as client_module

    client, cat = _client(httpx_mock, fake_s3)
    httpx_mock.add_response(method="GET", url=TABLE_URL, json=TABLE_WIRE)
    t = Namespace(cat, "ns1").table("events")
    httpx_mock.add_response(
        method="GET",
        url=OPTIONS_URL,
        json={**OPTIONS_WIRE, "snapshot_retention_seconds": None},
    )
    assert cat._retention_seconds() == float("inf")
    before = len(httpx_mock.get_requests())
    _prepare(t, tmp_path, "f.parquet")
    assert [
        r
        for r in httpx_mock.get_requests()[before:]
        if r.method == "GET" and r.url.path.endswith("/tables/events")
    ] == []
    client.close()
    assert client_module._ASSUMED_RETENTION_SECONDS == 1800.0


def test_an_unreadable_options_endpoint_falls_back_to_the_assumed_retention(
    httpx_mock, fake_s3
):
    # An unknown retention must cost a read, never an expired commit, so
    # the fallback is deliberately shorter than any retention worth
    # configuring.
    from pyhoglake.client import _ASSUMED_RETENTION_SECONDS

    client, cat = _client(httpx_mock, fake_s3)
    httpx_mock.add_response(method="GET", url=OPTIONS_URL, status_code=404, json={})
    assert cat._retention_seconds() == _ASSUMED_RETENTION_SECONDS
    client.close()


def test_a_server_without_read_snapshot_id_behaves_exactly_as_before(
    httpx_mock, fake_s3, tmp_path
):
    """The compatibility floor: an older server sends no
    `read_snapshot_id`, so there is nothing to use as a read_snapshot and
    the writer path falls back to a catalog GET plus an identity GET per
    flush — what main does, minus the totals scan."""
    client, cat = _client(httpx_mock, fake_s3)
    httpx_mock.add_response(method="GET", url=TABLE_URL, json=LEGACY_TABLE_WIRE)
    t = Namespace(cat, "ns1").table("events")
    httpx_mock.add_response(
        method="GET",
        url=OPTIONS_URL,
        json=OPTIONS_WIRE,
        is_optional=True,
        is_reusable=True,
    )
    for i in range(2):
        httpx_mock.add_response(method="GET", url=CATALOG_URL, json=CATALOG_WIRE)
        httpx_mock.add_response(
            method="GET", url=WRITER_TABLE_URL, json=LEGACY_TABLE_WIRE
        )
        request = _prepare(t, tmp_path, f"f{i}.parquet")
        assert request["read_snapshot"] == CATALOG_WIRE["head_snapshot_id"]
    assert len(_gets(httpx_mock, "/tables/events")) == 3  # the resolve + one per flush
    client.close()


def test_a_cached_uuid_the_caller_did_not_ask_for_refuses_before_any_upload(
    table, httpx_mock, fake_s3, tmp_path
):
    with pytest.raises(IncarnationChangedError, match="recreated"):
        table.prepare_append_files(
            [(_parquet(table, tmp_path, "a.parquet"), ("7",))],
            idempotency_key=str(uuid.uuid4()),
            expected_table_uuid="11111111-2222-3333-4444-555555555555",
        )
    assert fake_s3.files == {}


def test_expected_table_info_opts_out_of_the_cache_and_reads(
    table, httpx_mock, fake_s3, tmp_path
):
    """`expected_table_info` keeps its ORIGINAL semantics: it refuses
    when the DESTINATION's layout is not the one the caller planned
    against, which means comparing against a fresh SERVER read.
    Comparing it with this client's own cache would be a comparison of
    two client-side values, so passing it opts out."""
    from pyhoglake import ValidationError

    before = len(httpx_mock.get_requests())
    httpx_mock.add_response(method="GET", url=CATALOG_URL, json=CATALOG_WIRE)
    httpx_mock.add_response(method="GET", url=WRITER_TABLE_URL, json=REREAD_WIRE)
    # The caller planned against the OLD spec; the server now has a new
    # one, and only a fresh read can see that.
    with pytest.raises(ValidationError, match="destination layout changed"):
        table.prepare_append_files(
            [(_parquet(table, tmp_path, "a.parquet"), ("7",))],
            idempotency_key=str(uuid.uuid4()),
            expected_table_info=TableInfo.from_wire(TABLE_WIRE),
        )
    reads = [r for r in httpx_mock.get_requests()[before:] if r.method == "GET"]
    assert any(r.url.path.endswith("/tables/events") for r in reads)


def test_expected_table_info_matching_the_fresh_read_prepares(
    table, httpx_mock, fake_s3, tmp_path
):
    httpx_mock.add_response(method="GET", url=CATALOG_URL, json=CATALOG_WIRE)
    httpx_mock.add_response(method="GET", url=WRITER_TABLE_URL, json=TABLE_WIRE)
    request = table.prepare_append_files(
        [(_parquet(table, tmp_path, "a.parquet"), ("7",))],
        idempotency_key=str(uuid.uuid4()),
        expected_table_info=TableInfo.from_wire(TABLE_WIRE),
    )
    # head, not the cached snapshot: this path took the reads.
    assert request["read_snapshot"] == CATALOG_WIRE["head_snapshot_id"]


def test_invalidate_is_public_and_forces_one_re_read(
    table, httpx_mock, fake_s3, tmp_path
):
    # The lever for a caller that publishes some other way than
    # Catalog.commit_prepared(table=...) — the contract is explicitly
    # cross-process, so the committer may not hold this Table at all.
    table.invalidate()
    httpx_mock.add_response(method="GET", url=CATALOG_URL, json=CATALOG_WIRE)
    httpx_mock.add_response(method="GET", url=WRITER_TABLE_URL, json=REREAD_WIRE)
    request = _prepare(table, tmp_path, "f.parquet")
    assert request["read_snapshot"] == REREAD_WIRE["read_snapshot_id"]


def test_two_table_handles_each_keep_their_own_cache(httpx_mock, fake_s3, tmp_path):
    """The cache lives on the Table instance, with no registry, so two
    handles to one table are independent: a refusal taken through one
    does not invalidate the other, and that other pays ONE extra refusal
    before it re-reads. That is fine — the refusal is atomic and writes
    nothing, the cost is one flush's uploads, and it is bounded at one
    per handle per DDL. A registry would trade that for keeping every
    Table object a Catalog ever handed out reachable from it."""
    client, cat = _client(httpx_mock, fake_s3)
    httpx_mock.add_response(
        method="GET", url=TABLE_URL, json=TABLE_WIRE, is_reusable=True
    )
    httpx_mock.add_response(
        method="GET",
        url=OPTIONS_URL,
        json=OPTIONS_WIRE,
        is_optional=True,
        is_reusable=True,
    )
    ns = Namespace(cat, "ns1")
    a, b = ns.table("events"), ns.table("events")
    assert a is not b

    request = _prepare(a, tmp_path, "a.parquet")
    httpx_mock.add_response(
        method="POST", url=PREPARED_URL, json=DDL_409, status_code=409
    )
    with pytest.raises(DdlSinceReadSnapshotError):
        cat.commit_prepared(request, table=a)

    # `a` re-reads; `b` still holds the stale snapshot and will take its
    # own refusal.
    assert not a._cache_is_usable()
    assert b._cache_is_usable()
    assert _prepare(b, tmp_path, "b.parquet")["read_snapshot"] == 88
    client.close()


def test_an_alter_clears_the_cache_so_the_next_prepare_re_reads(
    table, httpx_mock, fake_s3, tmp_path
):
    """`Table.alter` is a second cache setter and has to go through the
    same one function every read does.

    Assigning `_info` directly left the PRE-alter snapshot in the cache
    while the shape became post-alter, so the next flush was a guaranteed
    409 with its parquet already uploaded — a doomed flush per in-process
    alter. An alter receipt carries no `read_snapshot_id`, so adopting it
    correctly clears the cache instead.
    """
    from pyhoglake import ops

    httpx_mock.add_response(
        method="POST",
        url=f"{TABLE_URL}/alter",
        json={**REREAD_WIRE, "snapshot_id": 95, "read_snapshot_id": None},
    )
    table.alter([ops.add_column("extra", "string")])
    assert table._cache is None

    httpx_mock.add_response(method="GET", url=CATALOG_URL, json=CATALOG_WIRE)
    httpx_mock.add_response(method="GET", url=WRITER_TABLE_URL, json=REREAD_WIRE)
    assert _prepare(table, tmp_path, "f.parquet")["read_snapshot"] == 95


def test_a_drop_clears_the_cache(table, httpx_mock):
    # The table is gone, so a prepare against the cache could only be
    # refused by the incarnation guard.
    httpx_mock.add_response(
        method="DELETE", url=TABLE_URL, json={"snapshot_id": 99, "schema_version": 3}
    )
    table.drop()
    assert table._cache is None


def test_table_commit_prepared_invalidates_without_an_opt_in(
    table, httpx_mock, fake_s3, tmp_path
):
    """The form that cannot forget: `Table.commit_prepared` passes itself,
    so the cache invalidation a re_prepare refusal needs does not depend
    on a caller remembering a keyword argument."""
    request = _prepare(table, tmp_path, "a.parquet")
    httpx_mock.add_response(
        method="POST", url=PREPARED_URL, json=DDL_409, status_code=409
    )
    with pytest.raises(DdlSinceReadSnapshotError):
        table.commit_prepared(request)
    assert table._cache is None


def test_the_catalog_form_without_table_leaves_invalidation_to_the_caller(
    table, httpx_mock, fake_s3, tmp_path
):
    # Documented, not accidental: the contract is cross-process, so a
    # committer may not hold this Table at all. millpond takes this path
    # and drops its whole cached Table itself.
    request = _prepare(table, tmp_path, "a.parquet")
    httpx_mock.add_response(
        method="POST", url=PREPARED_URL, json=DDL_409, status_code=409
    )
    with pytest.raises(DdlSinceReadSnapshotError):
        table._namespace._catalog.commit_prepared(request)
    assert table._cache is not None
    # ...and the lever is public.
    table.invalidate()
    assert table._cache is None


def test_a_non_retryable_422_on_a_cached_basis_also_invalidates(
    table, httpx_mock, fake_s3, tmp_path
):
    """The safety net behind the server's check ordering.

    A DDL change and the file-level symptom it produces arrive together;
    if a 422 ever answers first, the client must still learn its basis is
    stale, or it rebuilds the identical doomed payload every flush until
    the cache ages out (measured at half the retention).
    """
    from pyhoglake import ValidationError

    request = _prepare(table, tmp_path, "a.parquet")
    httpx_mock.add_response(
        method="POST",
        url=PREPARED_URL,
        json={"error": "validation", "detail": "unknown field_id 2 in stats"},
        status_code=422,
    )
    with pytest.raises(ValidationError) as ei:
        table.commit_prepared(request)
    assert not ei.value.re_prepare  # no server hint; the client inferred it
    assert table._cache is None


def test_a_422_on_a_freshly_read_basis_keeps_the_cache(
    table, httpx_mock, fake_s3, tmp_path
):
    # The other side of the same rule: a payload built from a FRESH read
    # already has the best basis there is, so a malformed-request refusal
    # must not buy a table read.
    from pyhoglake import ValidationError

    httpx_mock.add_response(method="GET", url=CATALOG_URL, json=CATALOG_WIRE)
    httpx_mock.add_response(method="GET", url=WRITER_TABLE_URL, json=TABLE_WIRE)
    request = table.prepare_append_files(
        [(_parquet(table, tmp_path, "a.parquet"), ("7",))],
        idempotency_key=str(uuid.uuid4()),
        expected_table_info=TableInfo.from_wire(TABLE_WIRE),
    )
    # head, not the cached snapshot: this payload's basis is not the cache's.
    assert request["read_snapshot"] == CATALOG_WIRE["head_snapshot_id"]
    httpx_mock.add_response(
        method="POST",
        url=PREPARED_URL,
        json={"error": "validation", "detail": "blank file path"},
        status_code=422,
    )
    with pytest.raises(ValidationError):
        table.commit_prepared(request)
    assert table._cache is not None


def test_set_retention_drops_the_cached_threshold(table, httpx_mock):
    """Shortening retention is a live operational lever, and the staleness
    threshold is derived from it: a writer keeping the old longer value
    would hold a basis past the new floor and take a post-upload 410 once
    per new-retention period, silently, forever."""
    catalog = table._namespace._catalog
    assert catalog._retention_seconds() == 3600.0
    httpx_mock.add_response(
        method="PATCH",
        url=OPTIONS_URL,
        json={**OPTIONS_WIRE, "snapshot_retention_seconds": 600},
    )
    catalog.set_retention(600)
    assert catalog._retention_cache is None
    httpx_mock.add_response(
        method="GET",
        url=OPTIONS_URL,
        json={**OPTIONS_WIRE, "snapshot_retention_seconds": 600},
    )
    assert catalog._retention_seconds() == 600.0


def test_a_failed_options_read_is_not_cached_permanently(httpx_mock, fake_s3):
    # One transient blip must not pin the process to 30-minute refreshes
    # for its life.
    from pyhoglake.client import _ASSUMED_RETENTION_SECONDS

    client, cat = _client(httpx_mock, fake_s3)
    httpx_mock.add_response(method="GET", url=OPTIONS_URL, status_code=503, json={})
    assert cat._retention_seconds() == _ASSUMED_RETENTION_SECONDS
    assert cat._retention_cache is None
    httpx_mock.add_response(method="GET", url=OPTIONS_URL, json=OPTIONS_WIRE)
    assert cat._retention_seconds() == 3600.0
    client.close()


def test_a_response_without_totals_parses_as_none_not_zero(table, httpx_mock):
    """`totals=false` omits the three fields, and absent must not read
    back as 0 — an unmeasured table and an empty one are different facts
    and only one of them is a number."""
    stripped = {
        k: v
        for k, v in TABLE_WIRE.items()
        if k not in ("record_count", "file_count", "file_size_bytes")
    }
    httpx_mock.add_response(method="GET", url=WRITER_TABLE_URL, json=stripped)
    info = table.info(totals=False)
    assert info.record_count is None
    assert info.file_count is None
    assert info.file_size_bytes is None
    # Everything the writer path uses is still there.
    assert info.table_uuid == TABLE_WIRE["table_uuid"]
    assert info.read_snapshot_id == 88
    assert info.partition_spec is not None
    assert info.partition_spec.spec_id == 3
    assert TableInfo.from_wire(stripped).record_count is None


def test_an_explicit_info_read_keeps_the_totals(table, httpx_mock):
    # Only the writer path opts out: a caller who asked for the table's
    # size gets it.
    httpx_mock.add_response(method="GET", url=TABLE_URL, json=TABLE_WIRE)
    info = table.info()
    assert info.record_count == 0
    assert _gets(httpx_mock, "/tables/events")[-1].url.params.get("totals") is None


def test_the_commit_body_is_what_the_prepare_recorded(
    table, httpx_mock, fake_s3, tmp_path
):
    # The prepared request is the durable payload, so what prepare put in
    # it is what the publish sends.
    request = _prepare(table, tmp_path, "f.parquet")
    httpx_mock.add_response(
        method="POST", url=PREPARED_URL, json={"snapshot_id": 91, "schema_version": 2}
    )
    table._namespace._catalog.commit_prepared(request, table=table)
    body = json.loads(httpx_mock.get_requests()[-1].content)
    assert body["read_snapshot"] == 88
    assert body["appends"] == request["appends"]


def test_a_partitioned_blind_append_takes_a_read_snapshot(table, httpx_mock, fake_s3):
    """`Table.append` (the non-prepared path) defaults to a blind commit,
    which the server now refuses for a partitioned table. The client
    supplies the basis itself so the caller never sees that 422."""
    httpx_mock.add_response(method="GET", url=CATALOG_URL, json=CATALOG_WIRE)
    httpx_mock.add_response(method="GET", url=WRITER_TABLE_URL, json=TABLE_WIRE)
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/commit",
        json={"snapshot_id": 91, "schema_version": 2},
    )
    table.append(pa.table({"id": pa.array([1], pa.int64()), "team": [7]}))
    body = json.loads(httpx_mock.get_requests()[-1].content)
    # The resolve's own snapshot, so the spec the values were computed
    # under and the conflict window are the same instant.
    assert body["read_snapshot"] == TABLE_WIRE["read_snapshot_id"]
    assert body["appends"][0]["files"][0]["partition_values"] == ["7"]


def test_a_partitioned_append_reads_head_BEFORE_the_resolve(table, httpx_mock, fake_s3):
    """The ordering, on the branch that actually runs against a server
    with no `read_snapshot_id`.

    The server's window is `snapshot_id > read_snapshot`, so a basis taken
    AFTER the resolve sits above any DDL that landed in between — putting
    the respec this guard exists to catch OUTSIDE the window, for the
    whole duration of the parquet write and the S3 upload. Asserted on the
    recorded request ORDER, because that is the property: catalog first,
    table second, commit last.
    """
    httpx_mock.add_response(method="GET", url=CATALOG_URL, json=CATALOG_WIRE)
    httpx_mock.add_response(
        method="GET", url=WRITER_TABLE_URL, json=LEGACY_PARTITIONED_WIRE
    )
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/commit",
        json={"snapshot_id": 91, "schema_version": 2},
    )
    before = len(httpx_mock.get_requests())
    table.append(pa.table({"id": pa.array([1], pa.int64()), "team": [7]}))
    seen = [
        (r.method, r.url.path.rsplit("/", 1)[-1])
        for r in httpx_mock.get_requests()[before:]
        if "options" not in r.url.path
    ]
    assert seen == [("GET", "cat"), ("GET", "events"), ("POST", "commit")]
    body = json.loads(httpx_mock.get_requests()[-1].content)
    # Head, read first, so the window covers the resolve and the upload.
    assert body["read_snapshot"] == CATALOG_WIRE["head_snapshot_id"]


def test_a_spec_installed_since_the_cache_fill_still_settles_the_basis_pre_upload(
    httpx_mock, fake_s3
):
    """The sub-case: the CACHED shape looked unpartitioned, so no head was
    read before the resolve, and the fresh resolve says partitioned.

    The basis then has to be read where the resolve is, not after the
    upload — otherwise the window excludes the whole parquet write and S3
    write, which is the defect this function has now had three rounds of.
    Asserted on the request ORDER: the head read sits between the resolve
    and the commit, and nothing was uploaded before it.
    """
    plain_wire = {k: v for k, v in TABLE_WIRE.items() if k != "partition_spec"}
    client, cat = _client(httpx_mock, fake_s3)
    # Cached as unpartitioned...
    stale = Table(Namespace(cat, "ns1"), TableInfo.from_wire(plain_wire))
    # ...but the resolve says partitioned, and reports no read_snapshot_id,
    # so the third source (a head read here) is the one that runs.
    httpx_mock.add_response(
        method="GET", url=WRITER_TABLE_URL, json=LEGACY_PARTITIONED_WIRE
    )
    # A CALLBACK, not a static response: the request order alone cannot
    # show this, because the parquet upload goes to the object store and
    # never through httpx. Recording how many objects exist AT THE MOMENT
    # the head is read is the actual property.
    uploads_when_head_was_read = []

    def head(request):
        uploads_when_head_was_read.append(len(fake_s3.files))
        return httpx.Response(200, json=CATALOG_WIRE)

    httpx_mock.add_callback(head, method="GET", url=CATALOG_URL)
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/commit",
        json={"snapshot_id": 91, "schema_version": 2},
    )
    before = len(httpx_mock.get_requests())
    stale.append(pa.table({"id": pa.array([1], pa.int64()), "team": [7]}))
    seen = [
        (r.method, r.url.path.rsplit("/", 1)[-1])
        for r in httpx_mock.get_requests()[before:]
        if "options" not in r.url.path
    ]
    assert seen == [("GET", "events"), ("GET", "cat"), ("POST", "commit")]
    # Nothing was in the store yet: the whole upload is inside the window.
    assert uploads_when_head_was_read == [0]
    assert len(fake_s3.files) == 1
    body = json.loads(httpx_mock.get_requests()[-1].content)
    assert body["read_snapshot"] == CATALOG_WIRE["head_snapshot_id"]
    assert body["appends"][0]["files"][0]["partition_values"] == ["7"]
    client.close()


def test_an_unpartitioned_append_reads_no_head(httpx_mock, fake_s3):
    # The head read is paid only where it is needed: an unpartitioned
    # append stays at one GET and one POST, exactly as before.
    plain_wire = {k: v for k, v in TABLE_WIRE.items() if k != "partition_spec"}
    client, cat = _client(httpx_mock, fake_s3)
    plain = Table(Namespace(cat, "ns1"), TableInfo.from_wire(plain_wire))
    httpx_mock.add_response(method="GET", url=WRITER_TABLE_URL, json=plain_wire)
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/commit",
        json={"snapshot_id": 91, "schema_version": 2},
    )
    before = len(httpx_mock.get_requests())
    plain.append(pa.table({"id": pa.array([1], pa.int64()), "team": [7]}))
    seen = [
        (r.method, r.url.path.rsplit("/", 1)[-1])
        for r in httpx_mock.get_requests()[before:]
    ]
    assert seen == [("GET", "events"), ("POST", "commit")]
    client.close()


def test_an_unpartitioned_append_stays_blind(httpx_mock, fake_s3):
    # The legacy shape, still legal and still cheap: nothing is bound to
    # a spec, so there is nothing a concurrent alter can invalidate that
    # the field-id content checks do not already catch.
    plain_wire = {k: v for k, v in TABLE_WIRE.items() if k != "partition_spec"}
    client, cat = _client(httpx_mock, fake_s3)
    plain = Table(Namespace(cat, "ns1"), TableInfo.from_wire(plain_wire))
    httpx_mock.add_response(method="GET", url=WRITER_TABLE_URL, json=plain_wire)
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/commit",
        json={"snapshot_id": 91, "schema_version": 2},
    )
    plain.append(pa.table({"id": pa.array([1], pa.int64()), "team": [7]}))
    body = json.loads(httpx_mock.get_requests()[-1].content)
    assert "read_snapshot" not in body
    client.close()
