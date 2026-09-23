"""Unit tests for the append writer path: parquet bytes written through a
fake filesystem, commit body shape, field ids, deferred stats, and the
name-rebind incarnation guard (BUG-1, 2026-09-05 adversarial review; now
atomic server-side — the commit ships ``expected_table_uuid`` and the
server 409s a mismatch with zero writes, the client keeping only one
cheap pre-flight re-resolve as an upload-saving fast-fail)."""

import base64
import io
import json
import struct
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyhoglake import (
    HoglakeClient,
    HoglakeError,
    IncarnationChangedError,
    ValidationError,
)
from pyhoglake.client import Namespace

BASE = "http://hog.test"

CATALOG_WIRE = {
    "name": "cat",
    "data_path": "s3://bkt/lake",  # note: no trailing slash; client must add it
    "head_snapshot_id": 5,
    "schema_version": 2,
}

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


class _FakeStream(io.BytesIO):
    def __init__(self, store: dict, key: str, *, fail_on_close: bool = False):
        super().__init__()
        self._store = store
        self._key = key
        self._fail_on_close = fail_on_close

    def close(self):
        if not self.closed:
            # A close that fails still leaves whatever bytes landed: the
            # truncated-object case the caller cannot distinguish.
            self._store[self._key] = self.getvalue()
            if self._fail_on_close:
                super().close()
                raise OSError(f"object store lost the tail of {self._key}")
        super().close()


class FakeS3:
    """Duck-typed stand-in for S3Config + pyarrow S3FileSystem.

    ``fail_at`` injects an object-store fault on the Nth (0-based)
    ``open_output_stream`` call — on the open itself, or on the stream's
    close with ``fail_on_close``, which is the case where a truncated
    object exists.
    """

    def __init__(self):
        self.files: dict[str, bytes] = {}
        self.opened: list[str] = []
        self.fail_at: int | None = None
        self.fail_on_close: bool = False

    def filesystem(self):
        return self

    def open_output_stream(self, key: str):
        failing = self.fail_at == len(self.opened)
        self.opened.append(key)
        if failing and not self.fail_on_close:
            raise OSError(f"object store refused {key}")
        return _FakeStream(self.files, key, fail_on_close=failing)


@pytest.fixture
def fake_s3():
    return FakeS3()


@pytest.fixture
def table(httpx_mock, fake_s3):
    client = HoglakeClient(BASE)
    client.s3 = fake_s3
    httpx_mock.add_response(
        method="GET", url=f"{BASE}/v1/catalogs/cat", json=CATALOG_WIRE
    )
    cat = client.catalog("cat")
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=TABLE_WIRE,
    )
    t = Namespace(cat, "ns1").table("events")
    yield t
    client.close()


def _mock_refresh_and_commit(httpx_mock):
    # append re-resolves the table by name exactly ONCE (the pre-flight
    # fast-fail before the upload; the server-side expected_table_uuid
    # commit guard replaced the old second, post-upload re-resolve).
    # Non-reusable on purpose: a second GET would fail the mock.
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=TABLE_WIRE,
    )
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/commit",
        json={"snapshot_id": 6, "schema_version": 2},
    )


def test_append_full(table, httpx_mock, fake_s3):
    _mock_refresh_and_commit(httpx_mock)
    data = pa.table({"id": [1, 2, 3], "name": ["a", "b", None]})
    res = table.append(data, read_snapshot=5, author="tester", message="first")
    assert res.snapshot_id == 6

    body = json.loads(httpx_mock.get_requests()[-1].content)
    assert body["read_snapshot"] == 5
    assert body["author"] == "tester"
    assert body["message"] == "first"
    (append,) = body["appends"]
    assert append["namespace"] == "ns1"
    assert append["table"] == "events"
    # the atomic guard rides the wire: default = the pinned table_uuid
    assert append["expected_table_uuid"] == TABLE_WIRE["table_uuid"]
    (file_reg,) = append["files"]

    # exactly one file written, under the data path with the missing '/' fixed
    (key,) = fake_s3.files.keys()
    assert key.startswith("bkt/lake/data/ns1/events/")
    assert key.endswith(".parquet")
    assert file_reg["path"] == "s3://" + key

    raw = fake_s3.files[key]
    assert file_reg["record_count"] == 3
    assert file_reg["file_size_bytes"] == len(raw)
    # wire convention (bugs.md #7): footer_size == the trailer's 4-byte LE
    # thrift length EXACTLY — it EXCLUDES the 8-byte length+magic suffix
    expected_footer = struct.unpack("<I", raw[-8:-4])[0]
    assert file_reg["footer_size"] == expected_footer

    # stats: base64 Iceberg single-value bounds
    stats = {s["field_id"]: s for s in file_reg["column_stats"]}
    assert stats[1]["value_count"] == 3
    assert stats[1]["null_count"] == 0
    assert base64.b64decode(stats[1]["lower_bound"]) == struct.pack("<q", 1)
    assert base64.b64decode(stats[1]["upper_bound"]) == struct.pack("<q", 3)
    assert stats[2]["null_count"] == 1
    assert base64.b64decode(stats[2]["lower_bound"]) == b"a"
    assert base64.b64decode(stats[2]["upper_bound"]) == b"b"

    # the written parquet round-trips and carries the catalog field ids
    pf = pq.ParquetFile(io.BytesIO(raw))
    assert "field_id=1" in str(pf.schema)
    assert "field_id=2" in str(pf.schema)
    got = pf.read()
    assert got.column("id").to_pylist() == [1, 2, 3]
    assert got.column("name").to_pylist() == ["a", "b", None]
    meta = pf.schema_arrow
    assert meta.field("id").metadata[b"PARQUET:field_id"] == b"1"

    # single pre-flight shape: exactly ONE re-resolve during the append
    # (plus the fixture's initial resolve) — the old post-upload second
    # re-resolve is gone, superseded by the server-side commit guard
    table_gets = [
        r
        for r in httpx_mock.get_requests()
        if r.method == "GET" and str(r.url).endswith("/tables/events")
    ]
    assert len(table_gets) == 2


def test_append_reserved_hog_column_fast_fails(table, httpx_mock, fake_s3):
    """A user ``_hog*`` field in the append batch fast-fails client-side
    BEFORE the pre-flight resolve and the parquet upload. The prefix is
    reserved for hoglake internals (``_hog_row_id`` is compaction's row-id
    carrier); the server refuses it as well (hoglake#36), so failing here
    saves a round trip and the S3 write rather than being the only
    barrier."""
    data = pa.table({"id": [1], "name": ["a"], "_hog_row_id": [7]})
    with pytest.raises(ValidationError, match="reserved"):
        table.append(data)
    # No request beyond the fixture's catalog + table resolves, no upload.
    assert len(httpx_mock.get_requests()) == 2
    assert fake_s3.files == {}


def test_footer_size_wire_convention():
    """bugs.md #7 regression: ``footer_size`` is the serialized thrift
    FileMetaData length — the DB convention proven by the server's
    hydrator tail math (``[file_size - footer_size - 8, file_size)``) and
    by compaction's stored value — EXCLUDING the trailing 8-byte suffix
    (4-byte LE length + b"PAR1"). pyhoglake used to ship ``meta_len + 8``;
    the hydrator's tail read absorbed the 8-byte over-read, so files
    registered with the old value need no repair — but any consumer
    treating footer_size as exact would mis-slice every client-written
    file. Pinned against a real pyarrow-written file with the trailer
    parsed by hand."""
    from pyhoglake.client import _footer_size

    sink = io.BytesIO()
    pq.write_table(pa.table({"id": [1, 2, 3], "name": ["a", "b", None]}), sink)
    raw = sink.getvalue()

    # parse the parquet trailer ourselves: ... [metadata][len LE32]["PAR1"]
    assert raw[-4:] == b"PAR1"
    (meta_len,) = struct.unpack("<I", raw[-8:-4])

    assert _footer_size(raw) == meta_len  # NOT meta_len + 8
    # and the value really delimits the serialized footer: the region the
    # server's tail read covers, [file_size - footer_size - 8, file_size),
    # starts exactly at the thrift metadata
    footer_region = raw[len(raw) - meta_len - 8 : len(raw) - 8]
    assert len(footer_region) == meta_len


def test_append_deferred_stats(table, httpx_mock, fake_s3):
    _mock_refresh_and_commit(httpx_mock)
    data = pa.table({"id": [1], "name": ["x"]})
    table.append(data, deferred_stats=True)
    body = json.loads(httpx_mock.get_requests()[-1].content)
    (file_reg,) = body["appends"][0]["files"]
    assert "column_stats" not in file_reg
    assert file_reg["record_count"] == 1
    assert "read_snapshot" not in body  # blind append


def test_append_reorders_and_casts(table, httpx_mock, fake_s3):
    _mock_refresh_and_commit(httpx_mock)
    # columns out of order; ints as int32 needing an upcast to long
    data = pa.table(
        {
            "name": pa.array(["z"], pa.string()),
            "id": pa.array([7], pa.int32()),
        }
    )
    table.append(data)
    (key,) = fake_s3.files.keys()
    pf = pq.ParquetFile(io.BytesIO(fake_s3.files[key]))
    assert pf.schema_arrow.names == ["id", "name"]
    assert pf.schema_arrow.field("id").type == pa.int64()


def test_append_missing_column_rejected(table, httpx_mock):
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=TABLE_WIRE,
    )
    with pytest.raises(ValidationError, match="missing table columns"):
        table.append(pa.table({"id": [1]}))


def test_append_extra_column_rejected(table, httpx_mock):
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=TABLE_WIRE,
    )
    with pytest.raises(ValidationError, match="not in the table schema"):
        table.append(pa.table({"id": [1], "name": ["a"], "ghost": [1]}))


# -- incarnation guard (BUG-1 regression: name-rebind race; the guard is
# -- now ATOMIC at commit — expected_table_uuid on the commit body, 409
# -- with zero writes on mismatch — with one pre-flight kept as an
# -- upload-saving fast-fail) ------------------------------------------------

REBOUND_WIRE = dict(TABLE_WIRE, table_uuid="9d1c2f34-0000-4000-8000-000000000bad")
_TABLES_URL = f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events"
_COMMIT_URL = f"{BASE}/v1/catalogs/cat/commit"

RECREATED_409 = {
    "error": "commit_conflict",
    "detail": (
        "expected_table_uuid mismatch: the table was recreated "
        f"(expected {TABLE_WIRE['table_uuid']})"
    ),
}


def test_append_rebind_detected_before_upload(table, httpx_mock, fake_s3):
    # the pre-flight fast-fail sees the new incarnation: no S3 write
    httpx_mock.add_response(method="GET", url=_TABLES_URL, json=REBOUND_WIRE)
    with pytest.raises(IncarnationChangedError, match="recreated"):
        table.append(pa.table({"id": [1], "name": ["a"]}))
    assert fake_s3.files == {}  # nothing uploaded
    assert not [r for r in httpx_mock.get_requests() if r.method == "POST"]


def test_append_rebind_after_preflight_is_409d_by_the_server(
    table, httpx_mock, fake_s3
):
    # THE previously-racy half: the pre-flight passes, the table is
    # recreated before the commit lands, and the SERVER's atomic
    # expected_table_uuid guard 409s the commit ("the table was
    # recreated") with zero writes. The client maps it to
    # IncarnationChangedError.
    httpx_mock.add_response(method="GET", url=_TABLES_URL, json=TABLE_WIRE)
    httpx_mock.add_response(
        method="POST", url=_COMMIT_URL, json=RECREATED_409, status_code=409
    )
    with pytest.raises(IncarnationChangedError, match="commit_conflict") as ei:
        table.append(pa.table({"id": [1], "name": ["a"]}))
    assert ei.value.status_code == 409
    assert "the table was recreated" in (ei.value.detail or "")
    assert not ei.value.retryable
    assert len(fake_s3.files) == 1  # parquet orphaned in the bucket


def test_append_ordinary_commit_conflict_stays_retryable(table, httpx_mock, fake_s3):
    # a 409 that does NOT indicate recreation keeps the existing
    # taxonomy: CommitConflictError, retryable
    from pyhoglake import CommitConflictError

    httpx_mock.add_response(method="GET", url=_TABLES_URL, json=TABLE_WIRE)
    httpx_mock.add_response(
        method="POST",
        url=_COMMIT_URL,
        json={"error": "commit conflict", "detail": "concurrent DDL"},
        status_code=409,
    )
    with pytest.raises(CommitConflictError) as ei:
        table.append(pa.table({"id": [1], "name": ["a"]}))
    assert ei.value.retryable


def test_append_server_refusal_does_not_rebase_pinned_identity(
    table, httpx_mock, fake_s3
):
    # a server-side guard refusal must leave the Table pinned to the
    # ORIGINAL uuid (the pre-flight saw the old incarnation and adopted
    # nothing new)
    httpx_mock.add_response(method="GET", url=_TABLES_URL, json=TABLE_WIRE)
    httpx_mock.add_response(
        method="POST", url=_COMMIT_URL, json=RECREATED_409, status_code=409
    )
    with pytest.raises(IncarnationChangedError):
        table.append(pa.table({"id": [1], "name": ["a"]}))
    assert table.table_uuid == TABLE_WIRE["table_uuid"]


def test_append_rebind_does_not_rebase_pinned_identity(table, httpx_mock, fake_s3):
    # after a refused append, the Table object still pins the ORIGINAL
    # uuid — a naive retry must trip the guard again, not silently adopt
    # the new incarnation.
    httpx_mock.add_response(method="GET", url=_TABLES_URL, json=REBOUND_WIRE)
    with pytest.raises(IncarnationChangedError):
        table.append(pa.table({"id": [1], "name": ["a"]}))
    assert table.table_uuid == TABLE_WIRE["table_uuid"]
    httpx_mock.add_response(method="GET", url=_TABLES_URL, json=REBOUND_WIRE)
    with pytest.raises(IncarnationChangedError):
        table.append(pa.table({"id": [1], "name": ["a"]}))


def test_append_explicit_expected_table_uuid(table, httpx_mock, fake_s3):
    # an explicit pin (uuid.UUID accepted) overrides the object's own
    httpx_mock.add_response(method="GET", url=_TABLES_URL, json=TABLE_WIRE)
    with pytest.raises(IncarnationChangedError, match="recreated"):
        table.append(
            pa.table({"id": [1], "name": ["a"]}),
            expected_table_uuid=uuid.UUID("9d1c2f34-0000-4000-8000-000000000bad"),
        )
    assert fake_s3.files == {}


def test_append_explicit_expected_table_uuid_match_commits(table, httpx_mock, fake_s3):
    _mock_refresh_and_commit(httpx_mock)
    res = table.append(
        pa.table({"id": [1], "name": ["a"]}),
        expected_table_uuid=uuid.UUID(TABLE_WIRE["table_uuid"]),
    )
    assert res.snapshot_id == 6
    assert len(fake_s3.files) == 1
    body = json.loads(httpx_mock.get_requests()[-1].content)
    (append,) = body["appends"]
    # an explicit pin is what goes on the wire
    assert append["expected_table_uuid"] == TABLE_WIRE["table_uuid"]


def test_append_unguarded_sends_no_field_and_skips_the_guard(
    table, httpx_mock, fake_s3
):
    # opt-out: name-only resolution. Even a rebound name commits (the
    # caller asked for exactly that), and no expected_table_uuid field
    # rides the commit body.
    from pyhoglake import UNGUARDED

    httpx_mock.add_response(method="GET", url=_TABLES_URL, json=REBOUND_WIRE)
    httpx_mock.add_response(method="POST", url=_COMMIT_URL, json={"snapshot_id": 6})
    res = table.append(
        pa.table({"id": [1], "name": ["a"]}), expected_table_uuid=UNGUARDED
    )
    assert res.snapshot_id == 6
    body = json.loads(httpx_mock.get_requests()[-1].content)
    (append,) = body["appends"]
    assert "expected_table_uuid" not in append


def test_append_without_s3_config(httpx_mock):
    client = HoglakeClient(BASE)  # no s3
    httpx_mock.add_response(
        method="GET", url=f"{BASE}/v1/catalogs/cat", json=CATALOG_WIRE
    )
    cat = client.catalog("cat")
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=TABLE_WIRE,
    )
    t = Namespace(cat, "ns1").table("events")
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=TABLE_WIRE,
    )
    with pytest.raises(HoglakeError, match="S3 configuration"):
        t.append(pa.table({"id": [1], "name": ["a"]}))
    client.close()


def test_nested_name_check_covers_the_whole_list_family():
    """large_list and fixed_size_list normalize to catalog ``list``, so
    the recursive nested-name check must know all three.

    It knew only the canonical member, so appending either of the other
    two walked into a branch that answered "no mismatch" without
    looking — and the typo'd inner struct field the check exists to
    catch reached the cast and appended as an all-NULL column whose own
    stats said ``null_count == record_count``.
    """
    from pyhoglake.client import _nested_field_mismatch

    inner_ok = pa.struct([pa.field("a", pa.int64())])
    inner_typo = pa.struct([pa.field("aa", pa.int64())])
    for maker in (
        lambda t: pa.list_(t),
        lambda t: pa.large_list(t),
        lambda t: pa.list_(t, 3),
    ):
        missing, extra = _nested_field_mismatch(maker(inner_typo), maker(inner_ok), "l")
        assert missing == ["l.element.a"], maker
        assert extra == ["l.element.aa"], maker
        # The matching shape stays quiet.
        assert _nested_field_mismatch(maker(inner_ok), maker(inner_ok), "l") == ([], [])


def test_list_family_predicate_puts_map_first():
    """Arrow's map is physically a list of structs and answers yes to
    ``is_list``; every caller of the family predicate must test
    ``is_map`` first, so the predicate documents that rather than
    pretending otherwise."""
    from pyhoglake.types import is_list_family

    assert is_list_family(pa.list_(pa.int64()))
    assert is_list_family(pa.large_list(pa.int64()))
    assert is_list_family(pa.list_(pa.int64(), 4))
    assert not is_list_family(pa.struct([pa.field("a", pa.int64())]))
    # Pinned as an OBSERVATION, not a dependency: pyarrow 25 reports a
    # map as not-a-list, earlier comments in this package claimed the
    # opposite, and every caller dispatches on is_map first so neither
    # answer can change behaviour. If this flips, the ordering already
    # covers it and only this assertion needs updating.
    assert not is_list_family(pa.map_(pa.string(), pa.int64()))


def test_prepared_files_upload_then_commit_exact_request(
    table, httpx_mock, fake_s3, tmp_path
):
    from pyhoglake.types import columns_to_arrow_schema

    schema = columns_to_arrow_schema(table.columns)
    path = tmp_path / "sorted.parquet"
    data = pa.Table.from_pylist([{"id": 1, "name": "a"}], schema=schema)
    pq.write_table(data, path)
    httpx_mock.add_response(
        method="GET", url=f"{BASE}/v1/catalogs/cat", json=CATALOG_WIRE
    )
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=TABLE_WIRE,
    )
    key = str(uuid.uuid4())
    request = table.prepare_append_files([(str(path), None)], idempotency_key=key)
    assert request["idempotency_key"] == key
    assert request["read_snapshot"] == 5
    assert request["appends"][0]["expected_table_uuid"] == TABLE_WIRE["table_uuid"]
    assert len(fake_s3.files) == 1
    assert next(iter(fake_s3.files.values())) == path.read_bytes()
    # Publication has not happened as part of prepare. Persisting this request
    # is the caller's responsibility; publication uses a capability-safe route.
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/commit/prepared",
        json={"snapshot_id": 6, "schema_version": 2},
        is_reusable=True,
    )
    catalog = table._namespace._catalog
    assert catalog.commit_prepared(request) == catalog.commit_prepared(request)
    posts = [r for r in httpx_mock.get_requests() if r.method == "POST"]
    assert len(posts) == 2
    assert posts[0].content == posts[1].content


@pytest.mark.parametrize("defect", [None, "field_id", "nullability", "unit"])
def test_prepared_seconds_timestamp_preserves_schema_guards(
    table, httpx_mock, fake_s3, tmp_path, defect
):
    from datetime import datetime

    from pyhoglake.models import TableInfo
    from pyhoglake.types import columns_to_arrow_schema

    wire = {
        **TABLE_WIRE,
        "columns": [
            *TABLE_WIRE["columns"],
            {
                "name": "created_at",
                "type": "timestamp_s",
                "field_id": 3,
                "ordinal": 2,
                "nullable": False,
            },
        ],
    }
    schema = columns_to_arrow_schema(TableInfo.from_wire(wire).columns)
    field = schema.field("created_at")
    if defect == "field_id":
        field = field.with_metadata({b"PARQUET:field_id": b"99"})
    elif defect == "nullability":
        field = field.with_nullable(True)
    elif defect == "unit":
        field = field.with_type(pa.timestamp("us"))
    schema = schema.set(2, field)
    path = tmp_path / "seconds.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [{"id": 1, "name": "a", "created_at": datetime(2026, 1, 1)}], schema=schema
        ),
        path,
    )
    httpx_mock.add_response(
        method="GET", url=f"{BASE}/v1/catalogs/cat", json=CATALOG_WIRE
    )
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=wire,
    )
    if defect:
        with pytest.raises(ValidationError, match="schema/field IDs"):
            table.prepare_append_files(
                [(str(path), None)], idempotency_key=str(uuid.uuid4())
            )
        assert not fake_s3.files
    else:
        request = table.prepare_append_files(
            [(str(path), None)], idempotency_key=str(uuid.uuid4())
        )
        assert request["appends"][0]["files"][0]["record_count"] == 1
        uploaded = pq.read_table(io.BytesIO(next(iter(fake_s3.files.values()))))
        assert uploaded["created_at"].to_pylist() == [datetime(2026, 1, 1)]


# --- uuid: the annotated wire form, and the two forms on prepare ---------
#
# hoglake's uuid column is FIXED_LEN_BYTE_ARRAY(16) + the UUID logical
# annotation (docs/iceberg-federation.md), which is what compaction's
# ParquetRewriter and the Trino connector write. pyarrow stamps that
# annotation only for pa.uuid(), so the writer emits the extension type —
# but every file already registered carries the bare fixed(16), and a
# foreign writer may too, so PREPARE accepts either. The bytes are
# identical; only the annotation differs.

UUID_TABLE_WIRE = {
    **TABLE_WIRE,
    "columns": [
        *TABLE_WIRE["columns"],
        {
            "name": "event_id",
            "type": "uuid",
            "field_id": 3,
            "ordinal": 2,
            "nullable": False,
        },
    ],
}


@pytest.mark.parametrize(
    "form,accepted",
    [
        ("annotated", True),
        ("bare", True),
        ("narrow", False),
        ("string", False),
        ("wrong_field_id", False),
    ],
)
def test_prepared_uuid_column_accepts_both_wire_forms(
    table, httpx_mock, fake_s3, tmp_path, form, accepted
):
    """Both uuid spellings pass the prepared-file schema check; nothing
    else does. ``narrow``/``string`` are the neighbouring types the
    equivalence must not swallow, and ``wrong_field_id`` pins that
    accepting the second spelling did not stop comparing field ids —
    the check the old ``equals(..., check_metadata=True)`` carried."""
    from pyhoglake.models import TableInfo
    from pyhoglake.types import columns_to_arrow_schema

    schema = columns_to_arrow_schema(TableInfo.from_wire(UUID_TABLE_WIRE).columns)
    field = schema.field("event_id")
    storage, value = pa.binary(16), uuid.uuid4().bytes
    # Both spellings are written EXPLICITLY, so the case does not quietly
    # become a test of whatever coltype_to_arrow happens to return.
    if form == "annotated":
        field = field.with_type(pa.uuid())
    elif form == "bare":
        field = field.with_type(pa.binary(16))
    elif form == "narrow":
        storage, value = pa.binary(15), uuid.uuid4().bytes[:15]
        field = field.with_type(storage)
    elif form == "string":
        storage, value = pa.string(), str(uuid.uuid4())
        field = field.with_type(storage)
    elif form == "wrong_field_id":
        field = field.with_metadata({b"PARQUET:field_id": b"99"})
    schema = schema.set(2, field)
    path = tmp_path / f"{form}.parquet"
    raw = pa.table(
        {
            "id": pa.array([1], pa.int64()),
            "name": pa.array(["a"], pa.string()),
            "event_id": pa.array([value], storage),
        }
    )
    pq.write_table(raw.cast(schema), path)
    httpx_mock.add_response(
        method="GET", url=f"{BASE}/v1/catalogs/cat", json=CATALOG_WIRE
    )
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=UUID_TABLE_WIRE,
    )
    if accepted:
        request = table.prepare_append_files(
            [(str(path), None)], idempotency_key=str(uuid.uuid4())
        )
        assert request["appends"][0]["files"][0]["record_count"] == 1
        assert next(iter(fake_s3.files.values())) == path.read_bytes()
        # The uuid bound rides the same 16 big-endian bytes either way.
        stats = {
            stat["field_id"]: stat
            for stat in request["appends"][0]["files"][0]["column_stats"]
        }
        assert base64.b64decode(stats[3]["lower_bound"]) == value
    else:
        with pytest.raises(ValidationError, match="schema/field IDs"):
            table.prepare_append_files(
                [(str(path), None)], idempotency_key=str(uuid.uuid4())
            )
        assert not fake_s3.files


def test_prepared_json_column_still_requires_the_json_extension(
    table, httpx_mock, fake_s3, tmp_path
):
    """The uuid licence is the uuid column's alone. A bare utf8 file
    under a `json` catalog column stays a refusal: arrow's plain string
    makes no JSON validity claim, so accepting it would attach one the
    data never made (types.py module docstring). Pinned here because
    the obvious over-generalization of the uuid rule — unwrap ANY
    extension type before comparing — would silently allow it."""
    from pyhoglake.models import TableInfo
    from pyhoglake.types import columns_to_arrow_schema

    wire = {
        **TABLE_WIRE,
        "columns": [
            *TABLE_WIRE["columns"],
            {
                "name": "props",
                "type": "json",
                "field_id": 3,
                "ordinal": 2,
                "nullable": False,
            },
        ],
    }
    schema = columns_to_arrow_schema(TableInfo.from_wire(wire).columns)
    assert schema.field("props").type == pa.json_()
    bare = schema.set(2, schema.field("props").with_type(pa.string()))
    path = tmp_path / "bare-json.parquet"
    pq.write_table(
        pa.table(
            {
                "id": pa.array([1], pa.int64()),
                "name": pa.array(["a"], pa.string()),
                "props": pa.array(['{"a":1}'], pa.string()),
            }
        ).cast(bare),
        path,
    )
    httpx_mock.add_response(
        method="GET", url=f"{BASE}/v1/catalogs/cat", json=CATALOG_WIRE
    )
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=wire,
    )
    with pytest.raises(ValidationError, match="schema/field IDs"):
        table.prepare_append_files(
            [(str(path), None)], idempotency_key=str(uuid.uuid4())
        )
    assert not fake_s3.files


def test_appended_uuid_file_carries_the_parquet_uuid_annotation(
    table, httpx_mock, fake_s3
):
    """The defect this change closes, read off the object the client
    uploaded: a uuid column written through Table.append must reach the
    store as FIXED_LEN_BYTE_ARRAY(16) annotated UUID, not bare fixed
    binary an Iceberg reader takes for opaque bytes."""
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=UUID_TABLE_WIRE,
    )
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/commit",
        json={"snapshot_id": 6, "schema_version": 2},
    )
    value = uuid.uuid4().bytes
    # The caller hands over plain 16-byte storage — the client's own
    # target schema is what adds the annotation.
    table.append(
        pa.table(
            {
                "id": pa.array([1], pa.int64()),
                "name": pa.array(["a"], pa.string()),
                "event_id": pa.array([value], pa.binary(16)),
            }
        )
    )
    (raw,) = fake_s3.files.values()
    parquet = pq.ParquetFile(io.BytesIO(raw))
    column = parquet.schema.column(2)
    assert column.physical_type == "FIXED_LEN_BYTE_ARRAY"
    assert column.length == 16
    assert column.logical_type.type == "UUID"
    assert parquet.schema_arrow.field(2).metadata[b"PARQUET:field_id"] == b"3"
    assert parquet.read().column("event_id").to_pylist() == [uuid.UUID(bytes=value)]


def test_prepared_native_variant_uploads_original_bytes(table, httpx_mock, fake_s3):
    from pathlib import Path

    from pyhoglake.models import TableInfo

    path = Path(__file__).parent / "data" / "native_variant.parquet"
    wire = {
        **TABLE_WIRE,
        "columns": [
            TABLE_WIRE["columns"][0],
            {
                "name": "properties",
                "type": "variant",
                "field_id": 2,
                "ordinal": 1,
                "nullable": False,
            },
        ],
    }
    table._info = TableInfo.from_wire(wire)
    httpx_mock.add_response(
        method="GET", url=f"{BASE}/v1/catalogs/cat", json=CATALOG_WIRE
    )
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=wire,
    )
    request = table.prepare_append_files(
        [(str(path), None)], idempotency_key=str(uuid.uuid4())
    )
    assert next(iter(fake_s3.files.values())) == path.read_bytes()
    stats = request["appends"][0]["files"][0]["column_stats"]
    assert [stat["field_id"] for stat in stats] == [1]


@pytest.mark.parametrize("null_id", [False, True])
def test_prepared_external_optional_fields_require_zero_nulls(
    table, httpx_mock, fake_s3, tmp_path, null_id
):
    from pyhoglake.types import columns_to_arrow_schema

    schema = columns_to_arrow_schema(table.columns)
    schema = schema.set(0, schema.field(0).with_nullable(True))
    path = tmp_path / "external.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [{"id": None if null_id else 1, "name": "a"}], schema=schema
        ),
        path,
    )
    httpx_mock.add_response(
        method="GET", url=f"{BASE}/v1/catalogs/cat", json=CATALOG_WIRE
    )
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=TABLE_WIRE,
    )
    if null_id:
        with pytest.raises(ValidationError, match="non-null"):
            table.prepare_append_files(
                [(str(path), None)],
                idempotency_key=str(uuid.uuid4()),
                allow_optional_fields=True,
            )
        assert not fake_s3.files
    else:
        table.prepare_append_files(
            [(str(path), None)],
            idempotency_key=str(uuid.uuid4()),
            allow_optional_fields=True,
        )
        assert next(iter(fake_s3.files.values())) == path.read_bytes()


# --- orphan accounting on a failed prepare -------------------------------
#
# `prepare_append_files` fans out uploads before it returns anything, so a
# mid-fanout object-store fault used to leave the caller unable to say how
# many objects it had orphaned. Every exception out of the call now carries
# `uploaded_files` / `uploaded_uris`, counting only uploads whose output
# stream CLOSED successfully.


def _prepare_mocks(httpx_mock, table_wire=TABLE_WIRE):
    # prepare refreshes the catalog (read_snapshot) then re-resolves the
    # table once (the incarnation pre-flight).
    httpx_mock.add_response(
        method="GET", url=f"{BASE}/v1/catalogs/cat", json=CATALOG_WIRE
    )
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=table_wire,
    )


def _prepared_file(table, tmp_path, name, *, rows=1):
    from pyhoglake.types import columns_to_arrow_schema

    schema = columns_to_arrow_schema(table.columns)
    path = tmp_path / name
    pq.write_table(
        pa.Table.from_pylist(
            [{"id": i, "name": "a"} for i in range(rows)], schema=schema
        ),
        path,
    )
    return str(path)


def _assert_nothing_uploaded(error, fake_s3):
    assert error.uploaded_files == 0
    assert error.uploaded_uris == ()
    # The consumer-side read of the same facts.
    assert getattr(error, "uploaded_files", 0) == 0
    assert getattr(error, "uploaded_uris", ()) == ()
    assert not fake_s3.files


@pytest.mark.parametrize("defect", ["schema", "arity", "empty", "no_files"])
def test_prepare_per_file_refusals_report_zero_uploads(
    table, httpx_mock, fake_s3, tmp_path, defect
):
    """A refusal raised BEFORE any upload orphans nothing, and says so."""
    if defect == "schema":
        # No field ids, so the destination-schema comparison refuses it.
        path = str(tmp_path / "idless.parquet")
        pq.write_table(pa.table({"id": [1], "name": ["a"]}), path)
        files = [(path, None)]
        expected = "schema/field IDs"
    elif defect == "arity":
        files = [(_prepared_file(table, tmp_path, "a.parquet"), ("2026-01",))]
        expected = "partition arity"
    elif defect == "empty":
        files = [(_prepared_file(table, tmp_path, "empty.parquet", rows=0), None)]
        expected = "must contain rows"
    else:
        files = []
        expected = "must contain files"
    _prepare_mocks(httpx_mock)
    with pytest.raises(ValidationError, match=expected) as excinfo:
        table.prepare_append_files(files, idempotency_key=str(uuid.uuid4()))
    _assert_nothing_uploaded(excinfo.value, fake_s3)


def test_prepare_bad_idempotency_key_reports_zero_uploads(table, fake_s3):
    """Not a HoglakeError at all — the uniform attribute is on whatever
    leaves the call, so a caller never needs to know the type."""
    with pytest.raises(ValueError) as excinfo:
        table.prepare_append_files([], idempotency_key="not-a-uuid")
    _assert_nothing_uploaded(excinfo.value, fake_s3)


def test_prepare_incarnation_refusal_reports_zero_uploads(
    table, httpx_mock, fake_s3, tmp_path
):
    files = [(_prepared_file(table, tmp_path, "a.parquet"), None)]
    _prepare_mocks(
        httpx_mock,
        table_wire={**TABLE_WIRE, "table_uuid": "11111111-2222-3333-4444-555555555555"},
    )
    with pytest.raises(IncarnationChangedError) as excinfo:
        table.prepare_append_files(files, idempotency_key=str(uuid.uuid4()))
    _assert_nothing_uploaded(excinfo.value, fake_s3)


def test_prepare_layout_change_refusal_reports_zero_uploads(
    table, httpx_mock, fake_s3, tmp_path
):
    from pyhoglake.models import TableInfo

    files = [(_prepared_file(table, tmp_path, "a.parquet"), None)]
    stale = TableInfo.from_wire(
        {
            **TABLE_WIRE,
            "columns": [
                *TABLE_WIRE["columns"],
                {
                    "name": "extra",
                    "type": "long",
                    "field_id": 3,
                    "ordinal": 2,
                    "nullable": True,
                },
            ],
        }
    )
    _prepare_mocks(httpx_mock)
    with pytest.raises(ValidationError, match="layout changed") as excinfo:
        table.prepare_append_files(
            files, idempotency_key=str(uuid.uuid4()), expected_table_info=stale
        )
    _assert_nothing_uploaded(excinfo.value, fake_s3)


@pytest.mark.parametrize("fail_on_close", [False, True])
@pytest.mark.parametrize("completed", [0, 1, 3])
def test_prepare_counts_completed_uploads_on_object_store_failure(
    table, httpx_mock, fake_s3, tmp_path, completed, fail_on_close
):
    """Failing while uploading the (k+1)-th of n files reports exactly k.

    The failing file is NOT completed in either flavour: an open that
    never succeeded wrote nothing, and a close that failed may have left
    a truncated object the caller cannot tell apart from a whole one.
    """
    total = 4
    files = [
        (_prepared_file(table, tmp_path, f"part{i}.parquet"), None)
        for i in range(total)
    ]
    fake_s3.fail_at = completed
    fake_s3.fail_on_close = fail_on_close
    key = str(uuid.uuid4())
    _prepare_mocks(httpx_mock)
    # The object store's own OSError, unwrapped: an existing
    # `except OSError` in a published consumer must keep catching it.
    with pytest.raises(OSError) as excinfo:
        table.prepare_append_files(files, idempotency_key=key)
    error = excinfo.value
    assert error.uploaded_files == completed
    assert len(error.uploaded_uris) == completed
    # The uris name the objects that exist, not the ones that were tried.
    assert [u.removeprefix("s3://") for u in error.uploaded_uris] == fake_s3.opened[
        :completed
    ]
    for uri in error.uploaded_uris:
        assert uri.startswith(f"{CATALOG_WIRE['data_path']}/data/ns1/events/{key}/")
        assert uri.endswith(".parquet")
    if fail_on_close:
        # The truncated object landed in the store and is deliberately
        # NOT counted; the prefix alone cannot tell the caller that.
        assert len(fake_s3.files) == completed + 1
        assert fake_s3.opened[completed].removeprefix("s3://") not in [
            u.removeprefix("s3://") for u in error.uploaded_uris
        ]
    else:
        assert len(fake_s3.files) == completed


def test_prepare_success_is_unaffected(table, httpx_mock, fake_s3, tmp_path):
    total = 3
    files = [
        (_prepared_file(table, tmp_path, f"part{i}.parquet"), None)
        for i in range(total)
    ]
    key = str(uuid.uuid4())
    _prepare_mocks(httpx_mock)
    request = table.prepare_append_files(files, idempotency_key=key)
    assert len(request["appends"][0]["files"]) == total
    assert len(fake_s3.files) == total
    assert request["idempotency_key"] == key
    assert [reg["record_count"] for reg in request["appends"][0]["files"]] == [
        1
    ] * total


def test_prepare_consumer_style_getattr_reads_every_case(
    table, httpx_mock, fake_s3, tmp_path
):
    """How a downstream sink actually reads this: one getattr, no
    knowledge of pyhoglake's exception hierarchy, correct in all three
    shapes (refusal, mid-fanout fault, success)."""

    def sweep(files, **kwargs):
        try:
            table.prepare_append_files(files, **kwargs)
        except Exception as error:  # the consumer shape: one catch-all
            return (
                getattr(error, "uploaded_files", 0),
                tuple(getattr(error, "uploaded_uris", ())),
            )
        return None

    good = [
        (_prepared_file(table, tmp_path, f"part{i}.parquet"), None) for i in range(3)
    ]
    empty = [(_prepared_file(table, tmp_path, "empty.parquet", rows=0), None)]

    _prepare_mocks(httpx_mock)
    assert sweep(empty, idempotency_key=str(uuid.uuid4())) == (0, ())

    fake_s3.fail_at = 2
    _prepare_mocks(httpx_mock)
    orphans = sweep(good, idempotency_key=str(uuid.uuid4()))
    assert orphans is not None
    assert orphans[0] == 2
    assert len(orphans[1]) == 2

    fake_s3.fail_at = None
    fake_s3.files.clear()
    _prepare_mocks(httpx_mock)
    assert sweep(good, idempotency_key=str(uuid.uuid4())) is None
