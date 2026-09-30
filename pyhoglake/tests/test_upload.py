"""Unit tests for the upload half of the prepared-append path: parquet
encoded in memory (``prepare_append_tables``), small objects written in
ONE request, and every object of a flush in flight at once.

The premise these pin is measured, not assumed: pyarrow's S3 output
stream always opens a multipart upload, so a 12 KiB parquet file — the
median for a partition-fanned events flush — cost three round trips and
~320 ms, and a serial loop over ~271 of them was 85 s p50 of the writer's
flush against 18 ms of server commit.
"""

import builtins
import contextlib
import logging
import re
import struct
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from test_append_unit import BASE, CATALOG_WIRE, TABLE_WIRE

from pyhoglake import HoglakeClient, HoglakeError, S3Config, ValidationError
from pyhoglake.client import Namespace
from pyhoglake.types import columns_to_arrow_schema
from pyhoglake.upload import (
    CONCURRENCY_ENV,
    DEFAULT_MAX_CONCURRENCY,
    SINGLE_REQUEST_MAX_BYTES,
    Upload,
    perform_upload,
    resolve_concurrency,
    run_uploads,
    widen_io_threads,
    widen_io_threads_for,
)

# The index the prepared-append uri ends with (``<uuid4>-<index>.parquet``),
# which is how these doubles tie an object back to its input group without
# the writer having to tell them.
_INDEX_IN_URI = re.compile(r"-(\d+)\.parquet$")


def _group_index(key: str) -> int:
    match = _INDEX_IN_URI.search(key)
    assert match, key
    return int(match.group(1))


class RecordingS3:
    """Duck-typed ``S3Config`` stand-in implementing BOTH halves of the
    writer path — ``filesystem()`` for the streaming multipart write and
    ``put_client()`` for the single-request one — and recording which one
    each object took.

    ``hook`` runs at the start of every upload with the object's group
    index, which is how the tests inject latency and faults at a specific
    group without depending on how the pool schedules them.
    """

    def __init__(self, *, hook=None, single_request=True):
        self.files: dict[str, bytes] = {}
        self.puts: list[str] = []
        self.streams: list[str] = []
        self.pool: int | None = None
        self._hook = hook
        self._single_request = single_request
        self._lock = threading.Lock()

    # -- as an S3Config ---------------------------------------------------

    def filesystem(self):
        return self

    def put_client(self, connections: int):
        self.pool = connections
        return self if self._single_request else None

    # -- as the pyarrow filesystem ---------------------------------------

    def open_output_stream(self, key: str):
        self._enter(key)
        with self._lock:
            self.streams.append(key)
        return _Sink(self, key)

    # -- as the boto3 client ----------------------------------------------

    def put_object(self, *, Bucket, Key, Body):  # boto3's own parameter names
        key = f"{Bucket}/{Key}"
        self._enter(key)
        with self._lock:
            self.puts.append(key)
            self.files[key] = Body.read()

    def _enter(self, key: str) -> None:
        if self._hook is not None:
            self._hook(_group_index(key))

    def _landed(self, key: str, raw: bytes) -> None:
        with self._lock:
            self.files[key] = raw


class _Sink:
    def __init__(self, store: RecordingS3, key: str):
        self._store = store
        self._key = key
        self._chunks: list[bytes] = []

    def write(self, chunk) -> None:
        self._chunks.append(bytes(chunk))

    def close(self) -> None:
        self._store._landed(self._key, b"".join(self._chunks))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def _mocks(httpx_mock, table_wire=TABLE_WIRE):
    """The GETs a prepare does on THIS wire — the catalog refresh for
    ``read_snapshot`` and the incarnation pre-flight — reusable, so a test
    may prepare more than once.

    Two URLs for the table, both reusable and both optional, because how
    many of these a prepare makes depends on the wire. `TABLE_WIRE` here
    carries no ``read_snapshot_id``, so the writer cache is unusable and
    every prepare pays the pair (the catalog first, then the identity read
    with ``totals=false`` — that order is load-bearing, see
    ``Table._prepared_read``). The query-less one is the initial
    ``Namespace.table()`` resolve, which is a CALLER's read and keeps the
    totals.
    """
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat",
        json=CATALOG_WIRE,
        is_reusable=True,
    )
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=table_wire,
        is_reusable=True,
    )
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events?totals=false",
        json=table_wire,
        is_reusable=True,
        is_optional=True,
    )


@pytest.fixture
def recording_s3():
    return RecordingS3()


def resolved_table(httpx_mock, store, table_wire=TABLE_WIRE):
    """A resolved ``Table`` over ``store``, its prepare GETs mocked.

    Called once per test — the mocks are registered here, and
    pytest-httpx wants every registered response used — so a test that
    needs a different table wire or a differently behaving store builds
    its own rather than taking the ``prepared`` fixture.
    """
    client = HoglakeClient(BASE)
    client.s3 = store
    _mocks(httpx_mock, table_wire)
    return Namespace(client.catalog("cat"), "ns1").table("events")


@pytest.fixture
def prepared(httpx_mock, recording_s3):
    """The shape most tests here need: a table over the recording store,
    on the default (unpartitioned, two-column) wire."""
    table = resolved_table(httpx_mock, recording_s3)
    yield table
    table._namespace._catalog._client.close()


def _rows(count: int, schema: pa.Schema) -> pa.Table:
    return pa.Table.from_pylist(
        [{"id": i, "name": f"r{i}"} for i in range(count)], schema=schema
    )


def _groups(table, counts):
    schema = columns_to_arrow_schema(table.columns)
    return [(_rows(n, schema), None) for n in counts]


# --- the two paths produce the same registration ------------------------


def test_prepare_tables_registrations_match_prepare_files(
    prepared, recording_s3, tmp_path
):
    """Same rows, both entry points, identical registrations.

    This is the contract that lets a writer drop its temp files: the
    commit request the server sees must not be able to tell which path
    produced it. Compared field for field except ``path``, which carries
    a fresh uuid by design.
    """
    schema = columns_to_arrow_schema(prepared.columns)
    data = _rows(7, schema)
    path = tmp_path / "group.parquet"
    pq.write_table(data, path)

    from_files = prepared.prepare_append_files(
        [(str(path), None)], idempotency_key=str(uuid.uuid4())
    )
    from_tables = prepared.prepare_append_tables(
        [(data, None)], idempotency_key=str(uuid.uuid4())
    )
    file_reg = from_files["appends"][0]["files"][0]
    table_reg = from_tables["appends"][0]["files"][0]
    assert {k: v for k, v in table_reg.items() if k != "path"} == {
        k: v for k, v in file_reg.items() if k != "path"
    }
    # Every wire field the server reads, named rather than left to the
    # dict comparison: a field silently missing from BOTH would pass.
    assert table_reg["record_count"] == 7
    assert table_reg["file_size_bytes"] == path.stat().st_size
    assert table_reg["footer_size"] == struct.unpack("<I", path.read_bytes()[-8:-4])[0]
    assert table_reg["column_stats"] == file_reg["column_stats"]
    # Same uri shape (the path pattern the sweeper matches on) and, since
    # the same encoder wrote both, the same bytes.
    assert re.fullmatch(
        rf"{re.escape(CATALOG_WIRE['data_path'])}/data/ns1/events/"
        rf"{from_tables['idempotency_key']}/[0-9a-f-]+-0\.parquet",
        table_reg["path"],
    )
    assert recording_s3.files[table_reg["path"].removeprefix("s3://")] == (
        path.read_bytes()
    )


def test_prepare_tables_carries_partition_values_and_input_order(httpx_mock):
    """Registrations follow the INPUT order, not the order the uploads
    finished — the server assigns row ids in registration order, so the
    fan-out must not be visible in the request. The delay is staggered so
    the completion order is genuinely not the input order."""
    store = RecordingS3(hook=lambda index: time.sleep(0.02 * ((index + 1) % 3)))
    partitioned = {
        **TABLE_WIRE,
        "partition_spec": {
            "spec_id": 1,
            "fields": [{"source_field_id": 1, "transform": "identity"}],
        },
    }
    table = resolved_table(httpx_mock, store, partitioned)
    schema = columns_to_arrow_schema(table.columns)
    groups = [(_rows(n, schema), (str(n),)) for n in (3, 1, 2, 9, 4)]
    request = table.prepare_append_tables(groups, idempotency_key=str(uuid.uuid4()))
    files = request["appends"][0]["files"]
    assert [f["record_count"] for f in files] == [3, 1, 2, 9, 4]
    assert [f["partition_values"] for f in files] == [
        ["3"],
        ["1"],
        ["2"],
        ["9"],
        ["4"],
    ]


def test_prepare_tables_refuses_a_variant_destination(httpx_mock, recording_s3):
    """An Arrow rewrite drops the native VARIANT annotation, so there is
    no honest way to serve a variant destination from an Arrow table."""
    with_variant = {
        **TABLE_WIRE,
        "columns": [
            *TABLE_WIRE["columns"],
            {
                "name": "props",
                "type": "variant",
                "field_id": 3,
                "ordinal": 2,
                "nullable": True,
            },
        ],
    }
    table = resolved_table(httpx_mock, recording_s3, with_variant)
    schema = columns_to_arrow_schema(table.columns[:2])
    with pytest.raises(ValidationError, match="native Parquet VARIANT") as excinfo:
        table.prepare_append_tables(
            [(_rows(1, schema), None)], idempotency_key=str(uuid.uuid4())
        )
    assert excinfo.value.uploaded_uris == ()
    assert not recording_s3.files


def test_prepare_tables_refuses_a_foreign_schema(prepared, recording_s3):
    """The field-id check is the file check, run on the ENCODED footer:
    an Arrow table without the destination's field ids is refused with
    the same error a prepared file would get, before any upload."""
    with pytest.raises(ValidationError, match="schema/field IDs") as excinfo:
        prepared.prepare_append_tables(
            [(pa.table({"id": [1], "name": ["a"]}), None)],
            idempotency_key=str(uuid.uuid4()),
        )
    assert excinfo.value.uploaded_files == 0
    assert not recording_s3.files


def test_prepare_tables_never_touches_disk(
    prepared, recording_s3, tmp_path, monkeypatch
):
    """No temp file, anywhere: the point of the method is to skip the
    write/fsync/re-read/unlink round trip, and an accidental
    ``NamedTemporaryFile`` in the path would be invisible otherwise."""
    import tempfile

    def refuse(*args, **kwargs):
        raise AssertionError("prepare_append_tables opened a temp file")

    monkeypatch.setattr(tempfile, "NamedTemporaryFile", refuse)
    monkeypatch.setattr(tempfile, "mkstemp", refuse)
    schema = columns_to_arrow_schema(prepared.columns)
    request = prepared.prepare_append_tables(
        [(_rows(2, schema), None)], idempotency_key=str(uuid.uuid4())
    )
    assert request["appends"][0]["files"][0]["record_count"] == 2


# --- one request for a small object -------------------------------------


class _CountingEndpoint(BaseHTTPRequestHandler):
    """An S3 endpoint that answers just enough to be written to, and
    records every request line it is given."""

    protocol_version = "HTTP/1.1"
    log = None  # set on the server's handler class per test

    def _drain(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        while length > 0:
            length -= len(self.rfile.read(min(length, 1 << 20)))

    def _reply(self, body: bytes = b"") -> None:
        self.send_response(200)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("ETag", '"0a0b"')
        self.end_headers()
        if body:
            self.wfile.write(body)

    def do_PUT(self):  # http.server's own dispatch names
        self._drain()
        type(self).log.append(f"PUT {self.path}")
        self._reply()

    def do_POST(self):
        self._drain()
        type(self).log.append(f"POST {self.path}")
        if "uploads" in self.path:
            self._reply(
                b"<InitiateMultipartUploadResult><Bucket>bkt</Bucket>"
                b"<Key>k</Key><UploadId>u1</UploadId>"
                b"</InitiateMultipartUploadResult>"
            )
        else:
            self._reply(
                b"<CompleteMultipartUploadResult><Bucket>bkt</Bucket>"
                b'<Key>k</Key><ETag>"0a0b"</ETag>'
                b"</CompleteMultipartUploadResult>"
            )

    def log_message(self, *args):
        pass


@pytest.fixture
def s3_endpoint():
    """A real HTTP endpoint, so request COUNTS are counted rather than
    asserted about a double."""
    log: list[str] = []
    handler = type("_Handler", (_CountingEndpoint,), {"log": log})
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}", log
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


@pytest.mark.parametrize("single_request", [True, False])
def test_small_object_costs_one_request_or_three(s3_endpoint, single_request):
    """The measurement the whole change rests on, as a test.

    A 12 KiB object — the median for a partition-fanned events flush —
    takes ONE PutObject on the single-request path, and the three
    pyarrow's output stream always takes (CreateMultipartUpload,
    UploadPart, CompleteMultipartUpload) when that path is turned off.
    pyarrow has no way to write a whole buffer in one request, which is
    why the fast path needs a client of its own.
    """
    endpoint, log = s3_endpoint
    s3 = S3Config(
        access_key="a",
        secret_key="b",
        region="us-east-1",
        endpoint_override=endpoint,
        single_request_uploads=single_request,
    )
    try:
        filesystem = s3.filesystem()
    except ImportError as unavailable:  # pragma: no cover
        pytest.skip(f"pyarrow built without S3 support: {unavailable}")
    put_client = s3.put_client(1)
    assert (put_client is not None) == single_request
    del log[:]
    try:
        perform_upload(
            filesystem,
            put_client,
            Upload(uri="s3://bkt/lake/small.parquet", size=12288, body=b"x" * 12288),
        )
    finally:
        # A real boto3 client owns a connection pool; the test that built
        # it is the only thing that can hand it back.
        if put_client is not None:
            put_client.close()
    if single_request:
        assert log == ["PUT /bkt/lake/small.parquet"]
    else:
        assert log == [
            "POST /bkt/lake/small.parquet?uploads",
            "PUT /bkt/lake/small.parquet?partNumber=1&uploadId=u1",
            "POST /bkt/lake/small.parquet?uploadId=u1",
        ]


def _hide_boto3(monkeypatch):
    real_import = builtins.__import__

    def no_boto(name, *args, **kwargs):
        if name.startswith("boto"):
            raise ImportError(f"no module named {name}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", no_boto)


def test_without_boto3_the_fast_path_declines_instead_of_failing(monkeypatch):
    """boto3 is an optional extra, so an install without it has to degrade
    to the streaming path — which is also what the published wheel's
    smoke test installs."""
    _hide_boto3(monkeypatch)
    s3 = S3Config(access_key="a", secret_key="b", region="us-east-1")
    assert s3.put_client(8) is None


def test_a_missing_extra_is_warned_about_once_per_client(monkeypatch, caplog):
    """A silent decline is the whole change no-opping in production:
    ``single_request_uploads`` defaults to True, so an install without the
    extra is asking for a path it cannot have and has to hear about it.
    Once per client, because the answer cannot change under us and a line
    per flush is a line nobody reads.
    """
    _hide_boto3(monkeypatch)
    client = HoglakeClient(
        BASE, s3=S3Config(access_key="a", secret_key="b", region="us-east-1")
    )
    with caplog.at_level(logging.WARNING, logger="pyhoglake"):
        assert client._put_client(8) is None
        assert client._put_client(64) is None  # a wider flush asks again
    records = [r for r in caplog.records if r.name == "pyhoglake"]
    assert len(records) == 1
    message = records[0].getMessage()
    assert "pyhoglake[fast-upload]" in message
    assert "three S3 requests per object" in message
    client.close()


def test_turning_single_request_uploads_off_declines_silently(caplog):
    """An operator who chose the streaming path does not need to be told
    about it on every client."""
    client = HoglakeClient(
        BASE,
        s3=S3Config(
            access_key="a",
            secret_key="b",
            region="us-east-1",
            single_request_uploads=False,
        ),
    )
    with caplog.at_level(logging.WARNING, logger="pyhoglake"):
        assert client._put_client(8) is None
    assert [r for r in caplog.records if r.name == "pyhoglake"] == []
    client.close()


def test_the_put_client_ignores_ambient_endpoint_configuration(monkeypatch):
    """botocore honours AWS_ENDPOINT_URL / AWS_ENDPOINT_URL_S3 and the
    shared config's endpoint_url; ``pyarrow.fs.S3FileSystem`` reads none
    of them.

    Left alone, a pod carrying either variable would put every object
    under the threshold in one store and everything above it in another,
    register all of them under one set of uris, and succeed. So the put
    client refuses ambient endpoints outright, and an explicit override
    still wins — which is what MinIO deployments use.
    """
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://rogue.invalid:1234")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://rogue2.invalid:1234")
    ambient = S3Config(access_key="a", secret_key="b", region="eu-central-1")
    explicit = S3Config(
        access_key="a",
        secret_key="b",
        region="eu-central-1",
        endpoint_override="http://minio:9000",
    )
    # closing(): these are real boto3 clients, each with its own
    # connection pool, and nothing else will hand them back.
    with (
        contextlib.closing(ambient.put_client(4)) as from_env,
        contextlib.closing(explicit.put_client(4)) as from_config,
    ):
        assert from_env.meta.endpoint_url == "https://s3.eu-central-1.amazonaws.com"
        assert from_config.meta.endpoint_url == "http://minio:9000"
        assert from_config.meta.config.s3["addressing_style"] == "path"


def test_the_put_client_fails_fast_rather_than_stalling_a_flush():
    """botocore's 60 s/60 s defaults against pyarrow's ~1 s/3 s, on a path
    whose fan-out waits for every in-flight upload before it re-raises:
    with the defaults a brownout turns a failing flush into minutes of
    stall instead of a failure. Pinned on a real client, along with the
    pool size the fan-out needs and the no-endpoint addressing style.
    """
    s3 = S3Config(access_key="a", secret_key="b", region="us-east-1")
    with contextlib.closing(s3.put_client(64)) as put:
        config = put.meta.config
        assert (config.connect_timeout, config.read_timeout) == (3, 30)
        # botocore counts max_attempts as RETRIES, hence four tries.
        assert config.retries["total_max_attempts"] == 4
        assert config.max_pool_connections == 64
        assert config.s3["addressing_style"] == "auto"


def test_a_close_fault_after_a_successful_put_keeps_the_uri(
    monkeypatch, tmp_path, recording_s3
):
    """The object exists once ``put_object`` returns. If closing the
    source file then fails — a network filesystem losing the handle —
    dropping the uri would hand the caller an orphan nobody sweeps."""
    path = tmp_path / "small.parquet"
    path.write_bytes(b"parquet-ish")
    real_open = builtins.open

    class Fragile:
        def __init__(self, inner):
            self._inner = inner

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def close(self):
            self._inner.close()
            raise OSError("the network filesystem lost the handle")

    def fragile_open(file, *args, **kwargs):
        handle = real_open(file, *args, **kwargs)
        return Fragile(handle) if str(file) == str(path) else handle

    monkeypatch.setattr(builtins, "open", fragile_open)
    completed: list[str] = []
    run_uploads(
        lambda upload: perform_upload(recording_s3, recording_s3, upload),
        [
            Upload(
                uri="s3://bkt/lake/x-0.parquet",
                size=path.stat().st_size,
                path=str(path),
            )
        ],
        concurrency=1,
        completed=completed,
    )
    assert completed == ["s3://bkt/lake/x-0.parquet"]
    assert recording_s3.files["bkt/lake/x-0.parquet"] == b"parquet-ish"


@pytest.mark.parametrize("source", ["buffer", "file"])
def test_a_boto3_fault_arrives_as_the_oserror_consumers_already_catch(
    recording_s3, tmp_path, source
):
    """Both upload paths must fail the same way. pyarrow raises OSError
    for an object-store fault and published consumers catch that; whether
    the boto3 fast path is installed is not their business, so botocore's
    own taxonomy is translated rather than leaked.

    Both BODY kinds, because they are separate branches: the file one
    opens its handle outside a ``with`` so that a close fault can be
    tolerated, and a put fault has to keep travelling through that
    ``finally`` as itself.
    """
    from botocore.exceptions import ClientError

    class Failing:
        def put_object(self, **kwargs):
            raise ClientError({"Error": {"Code": "SlowDown"}}, "PutObject")

    if source == "buffer":
        upload = Upload(uri="s3://bkt/lake/a-0.parquet", size=1, body=b"a")
    else:
        path = tmp_path / "a.parquet"
        path.write_bytes(b"parquet-ish")
        upload = Upload(
            uri="s3://bkt/lake/a-0.parquet",
            size=path.stat().st_size,
            path=str(path),
        )

    with pytest.raises(OSError, match="SlowDown") as excinfo:
        perform_upload(recording_s3, Failing(), upload)
    assert isinstance(excinfo.value.__cause__, ClientError)
    assert not recording_s3.files


def test_an_object_over_the_threshold_keeps_the_streaming_path(recording_s3):
    """The threshold is a memory bound (the whole object is held while
    its request is in flight), so anything above it streams even with a
    single-request client available."""
    big = SINGLE_REQUEST_MAX_BYTES + 1
    perform_upload(
        recording_s3,
        recording_s3,
        Upload(uri="s3://bkt/lake/big-0.parquet", size=big, body=b"y" * big),
    )
    perform_upload(
        recording_s3,
        recording_s3,
        Upload(
            uri="s3://bkt/lake/small-0.parquet",
            size=SINGLE_REQUEST_MAX_BYTES,
            body=b"y" * 16,
        ),
    )
    assert recording_s3.streams == ["bkt/lake/big-0.parquet"]
    assert recording_s3.puts == ["bkt/lake/small-0.parquet"]


def test_a_small_prepared_file_is_read_by_its_own_upload(
    prepared, recording_s3, tmp_path
):
    """The files path shares the single-request upload, and the file is
    read whole by the thread uploading it — nothing holds the flush's
    files in memory at once."""
    schema = columns_to_arrow_schema(prepared.columns)
    path = tmp_path / "one.parquet"
    pq.write_table(_rows(3, schema), path)
    prepared.prepare_append_files(
        [(str(path), None)], idempotency_key=str(uuid.uuid4())
    )
    assert len(recording_s3.puts) == 1
    assert not recording_s3.streams
    assert recording_s3.files[recording_s3.puts[0]] == path.read_bytes()


def test_append_stays_on_the_streaming_path(httpx_mock, recording_s3):
    """``Table.append`` is deliberately NOT rerouted through the
    single-request upload.

    It would change the transport of the library's most-used method for
    every install that happens to have boto3, while buying nothing but
    3 requests -> 1 per file (append writes its per-partition files
    serially, so there is no concurrency to gain). The store here offers
    a put client and append must still not take it.
    """
    table = resolved_table(httpx_mock, recording_s3)
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/commit",
        json={"snapshot_id": 6, "schema_version": 2},
    )
    schema = columns_to_arrow_schema(table.columns)
    result = table.append(_rows(4, schema))
    assert result.snapshot_id == 6
    assert len(recording_s3.streams) == 1
    assert not recording_s3.puts


# --- the fan-out ---------------------------------------------------------

_DELAY = 0.05


def test_fifty_groups_upload_concurrently(httpx_mock):
    """50 groups under 50 ms of injected per-upload latency finish in
    well under 5 x one upload — serially they would take 2.5 s."""
    store = RecordingS3(hook=lambda index: time.sleep(_DELAY))
    table = resolved_table(httpx_mock, store)
    groups = _groups(table, [1] * 50)

    started = time.monotonic()
    request = table.prepare_append_tables(groups, idempotency_key=str(uuid.uuid4()))
    elapsed = time.monotonic() - started

    assert len(request["appends"][0]["files"]) == 50
    assert len(store.puts) == 50
    assert elapsed < 5 * _DELAY, f"{elapsed:.3f}s for 50 x {_DELAY}s uploads"
    # The connection pool is sized to the fan-out that actually ran:
    # botocore's default 10 would have made a 50-wide fan-out behave like
    # a 10-wide one.
    assert store.pool == 50


def test_a_fault_reports_every_upload_that_landed_and_no_cancelled_one(httpx_mock):
    """Group 6 fails while group 7 is in flight, with 20 groups two at a
    time.

    What the caller is owed is the EXACT set of objects now in the store:
    group 7 lands AFTER the failure and must be reported (the fan-out
    waits for a request that is already going to create an object rather
    than abandoning it), and the groups that never started must not be,
    or a sweep of the reported uris deletes files nobody wrote.

    Group 7's event is what makes that deterministic — group 6 refuses
    only once group 7 is under way. The 20 ms every other group takes is
    what makes the cancellation deterministic: a worker freed by the
    failure can still claim the next queued group before the cancel
    lands (hence ``<= 8``), but not two.
    """
    in_flight = threading.Event()

    def hook(index: int) -> None:
        if index == 7:
            in_flight.set()
            time.sleep(0.2)  # still going when group 6 fails
        elif index == 6:
            assert in_flight.wait(5), "group 7 never started"
            raise OSError("object store refused group 6")
        else:
            time.sleep(0.02)

    store = RecordingS3(hook=hook)
    table = resolved_table(httpx_mock, store)
    groups = _groups(table, [1] * 20)
    key = str(uuid.uuid4())

    # The object store's own OSError, unwrapped: a published consumer's
    # `except OSError` has to keep catching it.
    with pytest.raises(OSError, match="refused group 6") as excinfo:
        table.prepare_append_tables(groups, idempotency_key=key, concurrency=2)
    error = excinfo.value

    landed = sorted(_group_index(stored) for stored in store.files)
    reported = [_group_index(uri.removeprefix("s3://")) for uri in error.uploaded_uris]
    # Exactly what is in the store, no more and no less.
    assert reported == landed
    assert error.uploaded_files == len(reported)
    # The one that finished AFTER the failure is in there; the one that
    # failed is not; the ones that were cancelled are not.
    assert set(reported) >= {0, 1, 2, 3, 4, 5, 7}
    assert 6 not in reported
    assert max(reported) <= 8, "a cancelled group was uploaded anyway"
    # Input order, not completion order.
    assert reported == sorted(reported)
    for uri in error.uploaded_uris:
        assert uri.startswith(f"{CATALOG_WIRE['data_path']}/data/ns1/events/{key}/")


def test_a_fault_cancels_the_rest_rather_than_uploading_them(httpx_mock):
    """The first error stops the fan-out: with one worker and the first
    group failing, nothing else is even attempted."""
    store = RecordingS3(hook=lambda index: _raise_on(index, 0))
    table = resolved_table(httpx_mock, store)
    with pytest.raises(OSError) as excinfo:
        table.prepare_append_tables(
            _groups(table, [1] * 10),
            idempotency_key=str(uuid.uuid4()),
            concurrency=1,
        )
    assert excinfo.value.uploaded_uris == ()
    assert not store.files


def _raise_on(index: int, target: int) -> None:
    if index == target:
        raise OSError(f"object store refused group {index}")


def test_an_invalid_prepare_names_the_callers_mistake(httpx_mock):
    """The object-store clients are built after every group is validated,
    so a client with no S3 configuration at all still answers an empty
    prepare with what the CALLER got wrong rather than with "no S3
    configuration"."""
    table = resolved_table(httpx_mock, None)
    with pytest.raises(ValidationError, match="must contain files") as excinfo:
        table.prepare_append_files([], idempotency_key=str(uuid.uuid4()))
    assert excinfo.value.uploaded_uris == ()
    table._namespace._catalog._client.close()


# --- concurrency settings ------------------------------------------------


def test_resolve_concurrency_argument_beats_env_beats_default(monkeypatch):
    monkeypatch.delenv(CONCURRENCY_ENV, raising=False)
    assert resolve_concurrency(1000, None) == DEFAULT_MAX_CONCURRENCY
    # Never wider than there is work to do.
    assert resolve_concurrency(3, None) == 3
    monkeypatch.setenv(CONCURRENCY_ENV, "16")
    assert resolve_concurrency(1000, None) == 16
    # The argument wins over the environment.
    assert resolve_concurrency(1000, 4) == 4
    monkeypatch.setenv(CONCURRENCY_ENV, " 200 ")
    assert resolve_concurrency(1000, None) == 200


@pytest.mark.parametrize("bad", ["nope", "8x", "0", "-4"])
def test_a_broken_concurrency_setting_is_an_error_not_a_silent_clamp(monkeypatch, bad):
    """Running eight-wide when you asked for 256 is the exact failure
    this module exists to remove, and it is invisible from outside."""
    monkeypatch.setenv(CONCURRENCY_ENV, bad)
    # ValidationError, not a bare ValueError: the README promises the
    # whole client surface is catchable as HoglakeError, and every other
    # refusal on this path already is.
    with pytest.raises(ValidationError, match=CONCURRENCY_ENV) as excinfo:
        resolve_concurrency(10, None)
    assert isinstance(excinfo.value, HoglakeError)


def test_a_broken_concurrency_argument_reports_zero_uploads(prepared, recording_s3):
    """And it is refused before anything is encoded or uploaded, so the
    orphan accounting on the exception reads correctly."""
    with pytest.raises(ValidationError, match="concurrency") as excinfo:
        prepared.prepare_append_tables(
            _groups(prepared, [1, 1]),
            idempotency_key=str(uuid.uuid4()),
            concurrency=0,
        )
    assert excinfo.value.uploaded_uris == ()
    assert not recording_s3.files


@pytest.fixture(autouse=True)
def io_threads():
    """Arrow's IO thread count is PROCESS-GLOBAL, so a test that widens
    it leaks a wide pool into every module that runs after this one.
    Autouse rather than opt-in: the tests that widen it are not only the
    ones that say so — any prepare that reaches the streaming path
    does — and remembering which is exactly the kind of bookkeeping that
    goes stale."""
    original = pa.io_thread_count()
    yield
    pa.set_io_thread_count(original)


def test_arrow_io_threads_are_raised_never_lowered(io_threads):
    """Every Arrow S3 request runs on that one pool (eight threads by
    default), so a 64-wide fan-out over the streaming path would still
    queue eight at a time unless it is widened. Widening only upwards:
    another part of the host process may have set it higher on purpose.
    """
    pa.set_io_thread_count(4)
    widen_io_threads(16)
    assert pa.io_thread_count() == 16
    widen_io_threads(2)
    assert pa.io_thread_count() == 16
    widen_io_threads(16)
    assert pa.io_thread_count() == 16


def test_a_wide_streaming_prepare_widens_the_arrow_pool(httpx_mock):
    """The store here has no put client, so all twelve objects go through
    Arrow and the pool has to be as wide as the fan-out."""
    store = RecordingS3(single_request=False)
    table = resolved_table(httpx_mock, store)
    pa.set_io_thread_count(2)
    table.prepare_append_tables(
        _groups(table, [1] * 12), idempotency_key=str(uuid.uuid4())
    )
    assert len(store.streams) == 12
    assert pa.io_thread_count() == 12


def test_a_boto3_only_prepare_leaves_the_arrow_pool_alone(prepared, recording_s3):
    """The intended production case: every object goes up as one
    PutObject, Arrow's IO pool serves none of them, so widening a
    process-global that the host process shares would be a side effect
    with nothing on the other side of it."""
    pa.set_io_thread_count(2)
    prepared.prepare_append_tables(
        _groups(prepared, [1] * 12), idempotency_key=str(uuid.uuid4())
    )
    assert len(recording_s3.puts) == 12
    assert pa.io_thread_count() == 2


def test_widen_io_threads_for_follows_the_transport(recording_s3):
    """The rule, in isolation: widen when — and only when — some object
    in THIS batch will be written through Arrow."""
    small = [
        Upload(uri=f"s3://bkt/s-{i}.parquet", size=16, body=b"x") for i in range(4)
    ]
    big = [
        *small,
        Upload(
            uri="s3://bkt/b-4.parquet",
            size=SINGLE_REQUEST_MAX_BYTES + 1,
            body=b"x",
        ),
    ]
    pa.set_io_thread_count(2)
    widen_io_threads_for(small, recording_s3, 8)
    assert pa.io_thread_count() == 2  # every object takes one PutObject
    widen_io_threads_for(small, None, 1)
    assert pa.io_thread_count() == 2  # serial: one request at a time
    widen_io_threads_for(big, recording_s3, 8)
    assert pa.io_thread_count() == 8  # one object streams
    pa.set_io_thread_count(2)
    widen_io_threads_for(small, None, 8)
    assert pa.io_thread_count() == 8  # no put client: all of them stream


def test_a_single_object_takes_no_thread_pool(recording_s3):
    """Table.append's one-file case and concurrency=1 keep the serial
    semantics exactly: the pool is not even created."""
    pool_used = []

    def perform(upload):
        pool_used.append(threading.current_thread() is threading.main_thread())

    completed: list[str] = []
    run_uploads(
        perform,
        [Upload(uri="s3://bkt/a-0.parquet", size=1, body=b"a")],
        concurrency=64,
        completed=completed,
    )
    assert pool_used == [True]
    assert completed == ["s3://bkt/a-0.parquet"]
