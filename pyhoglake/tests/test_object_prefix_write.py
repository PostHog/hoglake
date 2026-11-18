"""pyhoglake's writers on an object-prefix table: ``Table.append``,
``prepare_append_tables`` and ``prepare_append_files`` place each file
under ``<data_path>/data/<ns>/<table>/<field>=<value>/``, prove the value
on the file's own footer before the upload, refuse what the server would
refuse at commit before anything is written, and with deferred stats
register the prefix field's stats alone so the file is ``provided``.

Placement is held to ``tests/vectors/object_prefix_vectors.json`` as the
server and ``object_prefix`` are. A table without the option gets the
registrations and uris it always got.
"""

import base64
import json
import os
import re
import struct
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from test_append_unit import FakeS3
from thrift.protocol.TCompactProtocol import TCompactProtocol
from thrift.Thrift import TType
from thrift.transport.TTransport import TMemoryBuffer

from pyhoglake import (
    DEFAULT_FOOTER_STATS_MIN_BYTES,
    HoglakeClient,
    ValidationError,
    object_prefix,
    ops,
    parquet_write_options,
)
from pyhoglake import client as client_module
from pyhoglake.client import Namespace
from pyhoglake.models import TableInfo
from pyhoglake.types import columns_to_arrow_schema
from pyhoglake.writer import write_part

BASE = "http://hog.test"
VECTORS = json.loads(
    (Path(__file__).parent / "vectors" / "object_prefix_vectors.json").read_text()
)
# months since 1970 for 2026-01 and 2026-02
M_JAN, M_FEB = "672", "673"
UUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"


def _catalog_wire(data_path="s3://bkt/lake"):
    return {
        "name": "cat",
        "data_path": data_path,
        "head_snapshot_id": 90,
        "schema_version": 2,
    }


def _table_wire(
    *,
    namespace="ns1",
    table="events",
    field="team_id",
    col_type="long",
    prefix=True,
    spec="identity",
    variant=False,
    extra_columns=0,
):
    columns = [
        {
            "name": "team_id",
            "type": col_type,
            "field_id": 1,
            "ordinal": 0,
            "nullable": True,
        },
        {
            "name": "ts",
            "type": "timestamp",
            "field_id": 2,
            "ordinal": 1,
            "nullable": True,
        },
        {
            "name": "val",
            "type": "string",
            "field_id": 3,
            "ordinal": 2,
            "nullable": True,
        },
    ]
    if variant:
        columns.append(
            {
                "name": "props",
                "type": "variant",
                "field_id": 4,
                "ordinal": 3,
                "nullable": True,
            }
        )
    for i in range(extra_columns):
        columns.append(
            {
                "name": f"c{i}",
                "type": "long" if i % 2 else "string",
                "field_id": 10 + i,
                "ordinal": len(columns),
                "nullable": True,
            }
        )
    wire = {
        "name": table,
        "namespace": namespace,
        "table_uuid": "0b8ee9ba-79a1-4f3e-b7e5-6a0b6ab6f012",
        "columns": columns,
        "record_count": 0,
        "file_count": 0,
        "file_size_bytes": 0,
        "read_snapshot_id": 88,
    }
    fields = {
        "identity": [
            {"source_field_id": 1, "transform": "identity"},
            {"source_field_id": 2, "transform": "month"},
        ],
        "bucket": [
            {"source_field_id": 1, "transform": "bucket", "transform_param": 8},
            {"source_field_id": 2, "transform": "month"},
        ],
        "twice": [
            {"source_field_id": 1, "transform": "identity"},
            {"source_field_id": 1, "transform": "identity"},
        ],
        None: None,
    }[spec]
    if fields is not None:
        wire["partition_spec"] = {"spec_id": 3, "fields": fields}
    if prefix:
        wire["object_prefix_field"] = field
    return wire


@pytest.fixture
def fake_s3():
    return FakeS3()


def _make(httpx_mock, fake_s3, *, catalog_wire=None, table_wire=None):
    catalog_wire = catalog_wire or _catalog_wire()
    table_wire = table_wire or _table_wire()
    client = HoglakeClient(BASE)
    client.s3 = fake_s3
    httpx_mock.add_response(
        method="GET", url=f"{BASE}/v1/catalogs/cat", json=catalog_wire
    )
    cat = client.catalog("cat")
    ns, name = table_wire["namespace"], table_wire["name"]
    table_url = f"{BASE}/v1/catalogs/cat/namespaces/{ns}/tables/{name}"
    httpx_mock.add_response(method="GET", url=table_url, json=table_wire)
    table = Namespace(cat, ns).table(name)
    # The prepared path's one retention read per Catalog; append's
    # pre-flight re-resolve; neither is what these tests are about.
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/options",
        json={
            "consumer_floor": False,
            "earliest_snapshot_id": 40,
            "snapshot_retention_seconds": 3600,
        },
        is_optional=True,
        is_reusable=True,
    )
    httpx_mock.add_response(
        method="GET",
        url=f"{table_url}?totals=false",
        json=table_wire,
        is_optional=True,
        is_reusable=True,
    )
    return client, table


def _rows(table, team_ids, month=1, val="v", props=None):
    schema = columns_to_arrow_schema(table.columns, variant="json")
    rows = [
        {
            "team_id": t,
            "ts": datetime(2026, month, 1 + i, 12, 0, 0),
            "val": f"{val}-{i}",
        }
        for i, t in enumerate(team_ids)
    ]
    if props is not None:
        for row in rows:
            row["props"] = props
    return pa.Table.from_pylist(rows, schema=schema)


def _many_rows(table, team_id, n, val_bytes=None):
    """``n`` rows of one team, a second apart, for the scale checks; every
    extra column (``_table_wire(extra_columns=...)``) filled. ``val_bytes``
    makes ``val`` that many random hex bytes per row."""
    schema = columns_to_arrow_schema(table.columns, variant="json")
    start = datetime(2026, 1, 1, 12, 0, 0)
    arrays = []
    for field in schema:
        if field.name == "team_id":
            arrays.append(pa.array([team_id] * n, field.type))
        elif field.name == "ts":
            arrays.append(
                pa.array([start + timedelta(seconds=i) for i in range(n)], field.type)
            )
        elif field.name == "val" and val_bytes:
            arrays.append(
                pa.array([os.urandom(val_bytes // 2).hex() for _ in range(n)])
            )
        elif pa.types.is_string(field.type):
            arrays.append(pa.array([f"{field.name}-{i}" for i in range(n)]))
        else:
            arrays.append(pa.array(list(range(n)), field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def _prepare(table, groups, tmp_path, via, **kwargs):
    """``prepare_append_tables`` or, via a file per group, ``prepare_append_files``."""
    key = kwargs.pop("idempotency_key", str(uuid.uuid4()))
    if via == "tables":
        return key, table.prepare_append_tables(groups, idempotency_key=key, **kwargs)
    files = []
    for i, (data, partition) in enumerate(groups):
        path = tmp_path / f"g{i}.parquet"
        pq.write_table(data, path)
        files.append((str(path), partition))
    return key, table.prepare_append_files(files, idempotency_key=key, **kwargs)


def _posted(httpx_mock):
    """The body of the LAST commit POSTed."""
    return json.loads(
        [r for r in httpx_mock.get_requests() if r.method == "POST"][-1].content
    )


def _counting_writes(monkeypatch, log=None):
    """``write_part`` as the client calls it, logging each call as
    ``"write"`` in ``log`` (returned)."""
    log = [] if log is None else log
    original = client_module.write_part

    def counting(*args, **kwargs):
        log.append("write")
        return original(*args, **kwargs)

    monkeypatch.setattr(client_module, "write_part", counting)
    return log


class _LoggingS3(FakeS3):
    """``FakeS3`` logging each upload as ``"upload"`` in ``log``."""

    def __init__(self, log):
        super().__init__()
        self.log = log

    def open_output_stream(self, key):
        self.log.append("upload")
        return super().open_output_stream(key)


def _registered(httpx_mock, fake_s3, table, groups, tmp_path, via, **kwargs):
    """The file registrations one of the three writers produces for
    ``groups``: ``append`` (the body it POSTs) or a prepared call (the
    request it returns)."""
    if via == "append":
        # Optional: a refusal under test never reaches the commit.
        _mock_commit(httpx_mock, optional=True)
        table.append(pa.concat_tables([rows for rows, _ in groups]), **kwargs)
        return _posted(httpx_mock)["appends"][0]["files"]
    _, request = _prepare(table, groups, tmp_path, via, **kwargs)
    return request["appends"][0]["files"]


def _mock_commit(httpx_mock, *, optional=False):
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/commit",
        json={"snapshot_id": 91, "schema_version": 2},
        is_optional=optional,
    )


# -- placement ---------------------------------------------------------------------


@pytest.mark.parametrize("via", ["tables", "files"])
def test_prepared_files_go_under_their_values_prefix(
    httpx_mock, fake_s3, tmp_path, via
):
    client, table = _make(httpx_mock, fake_s3)
    groups = [
        (_rows(table, [7, 7]), ("7", M_JAN)),
        (_rows(table, [8]), ("8", M_JAN)),
        (_rows(table, [7], month=2), ("7", M_FEB)),
    ]
    key, request = _prepare(table, groups, tmp_path, via)
    (append,) = request["appends"]
    assert request["read_snapshot"] == 88
    assert len(append["files"]) == 3
    for index, (reg, (_, partition)) in enumerate(
        zip(append["files"], groups, strict=True)
    ):
        # The idempotency-key directory sits under the prefix, as before.
        assert re.fullmatch(
            rf"s3://bkt/lake/data/ns1/events/team_id={partition[0]}/{key}/{UUID}-{index}\.parquet",
            reg["path"],
        )
        assert reg["path"][len("s3://") :] in fake_s3.files
        assert reg["partition_values"] == list(partition)
        # deferred_stats=False: every stat, as on any table
        assert {s["field_id"] for s in reg["column_stats"]} == {1, 2, 3}
    client.close()


@pytest.mark.parametrize("via", ["tables", "files"])
def test_deferred_stats_register_the_prefix_fields_stats_alone(
    httpx_mock, fake_s3, tmp_path, via
):
    """D4: not pending for the hydrator; provided, with the one stat the
    server checks at commit."""
    client, table = _make(httpx_mock, fake_s3)
    groups = [(_rows(table, [7, 7]), ("7", M_JAN)), (_rows(table, [8]), ("8", M_JAN))]
    _, request = _prepare(table, groups, tmp_path, via, deferred_stats=True)
    for reg in request["appends"][0]["files"]:
        (stat,) = reg["column_stats"]
        assert stat["field_id"] == 1
        assert stat["null_count"] == 0
        assert stat["lower_bound"] == stat["upper_bound"]
    client.close()


def test_append_places_every_file_and_commits_them_in_one_request(httpx_mock, fake_s3):
    client, table = _make(httpx_mock, fake_s3)
    _mock_commit(httpx_mock)
    data = pa.concat_tables(
        [_rows(table, [7, 8, 7]), _rows(table, [8, 7], month=2, val="w")]
    )
    result = table.append(data)
    assert result.snapshot_id == 91
    body = _posted(httpx_mock)
    assert body["read_snapshot"] == 88
    (append,) = body["appends"]
    assert len(append["files"]) == 4
    by_partition = {tuple(f["partition_values"]): f for f in append["files"]}
    assert set(by_partition) == {("7", M_JAN), ("8", M_JAN), ("8", M_FEB), ("7", M_FEB)}
    for (team, _), reg in by_partition.items():
        assert re.fullmatch(
            rf"s3://bkt/lake/data/ns1/events/team_id={team}/{UUID}\.parquet",
            reg["path"],
        )
        assert reg["path"][len("s3://") :] in fake_s3.files
        assert {s["field_id"] for s in reg["column_stats"]} == {1, 2, 3}
    assert [f.path for f in result.files] == [f["path"] for f in append["files"]]
    client.close()


def test_append_with_deferred_stats_registers_the_prefix_fields_stats_alone(
    httpx_mock, fake_s3
):
    client, table = _make(httpx_mock, fake_s3)
    _mock_commit(httpx_mock)
    table.append(_rows(table, [7, 8]), deferred_stats=True)
    for reg in _posted(httpx_mock)["appends"][0]["files"]:
        assert [s["field_id"] for s in reg["column_stats"]] == [1]
    client.close()


def test_a_variant_column_beside_the_prefix_field_is_placed_and_unstatted(
    httpx_mock, fake_s3
):
    client, table = _make(httpx_mock, fake_s3, table_wire=_table_wire(variant=True))
    groups = [
        (_rows(table, [7], props='{"a": 1}'), ("7", M_JAN)),
        (_rows(table, [8], props='{"b": [true]}'), ("8", M_JAN)),
    ]
    request = table.prepare_append_tables(groups, idempotency_key=str(uuid.uuid4()))
    full = request["appends"][0]["files"]
    assert all("/team_id=" in reg["path"] for reg in full)
    assert all(
        {s["field_id"] for s in reg["column_stats"]} == {1, 2, 3} for reg in full
    )
    deferred = table.prepare_append_tables(
        groups, idempotency_key=str(uuid.uuid4()), deferred_stats=True
    )
    for reg in deferred["appends"][0]["files"]:
        assert [s["field_id"] for s in reg["column_stats"]] == [1]
    client.close()


# -- the shared vectors --------------------------------------------------------------


def _vector_id(case):
    return f"{case['col_type']}:{case['value']!r}"


@pytest.mark.parametrize("case", VECTORS["prefix_cases"], ids=_vector_id)
def test_prefix_cases_place_or_refuse_as_the_server_does(
    httpx_mock, fake_s3, monkeypatch, case
):
    """A valid value's file lands under exactly the vector's prefix; an
    invalid one (not canonical, out of range, null, a type that cannot be a
    prefix field) is refused before any encode or upload."""
    encoded = []
    original = client_module.write_part

    def counting(*args, **kwargs):
        encoded.append(1)
        return original(*args, **kwargs)

    monkeypatch.setattr(client_module, "write_part", counting)
    client, table = _make(
        httpx_mock,
        fake_s3,
        catalog_wire=_catalog_wire(case["data_path"]),
        table_wire=_table_wire(
            namespace=case["namespace"],
            table=case["table"],
            field=case["field"],
            col_type=case["col_type"],
        ),
    )
    key = str(uuid.uuid4())
    if "expected" in case:
        rows = _rows(table, [int(case["value"])])
        request = table.prepare_append_tables(
            [(rows, (case["value"], M_JAN))], idempotency_key=key
        )
        (reg,) = request["appends"][0]["files"]
        assert reg["path"].startswith(f"{case['expected']}{key}/")
        assert reg["path"][len("s3://") :] in fake_s3.files
    else:
        placeholder = "0" if case["col_type"] == "string" else 0
        rows = _rows(table, [placeholder])
        with pytest.raises(ValidationError) as ei:
            table.prepare_append_tables(
                [(rows, (case["value"], M_JAN))], idempotency_key=key
            )
        assert f"{case['namespace']}.{case['table']}" in str(ei.value)
        assert case["field"] in str(ei.value)
        assert not encoded
        assert not fake_s3.files
    client.close()


# -- refusals before anything is written -----------------------------------------------


@pytest.mark.parametrize("via", ["tables", "files"])
def test_a_null_value_is_refused_before_any_encode_or_upload(
    httpx_mock, fake_s3, tmp_path, monkeypatch, via
):
    """D3: a row with no value in the field names no prefix. Refused with
    the table and the field named, before the first group is encoded."""
    encoded = []
    original = client_module.write_part

    def counting(*args, **kwargs):
        encoded.append(1)
        return original(*args, **kwargs)

    monkeypatch.setattr(client_module, "write_part", counting)
    client, table = _make(httpx_mock, fake_s3)
    groups = [(_rows(table, [7]), ("7", M_JAN)), (_rows(table, [None]), (None, M_JAN))]
    with pytest.raises(ValidationError, match=r"ns1\.events has a null 'team_id'"):
        _prepare(table, groups, tmp_path, via)
    assert not encoded
    assert not fake_s3.files
    client.close()


def test_append_refuses_a_null_group_before_any_write(httpx_mock, fake_s3, monkeypatch):
    encoded = []
    original = client_module.write_part

    def counting(*args, **kwargs):
        encoded.append(1)
        return original(*args, **kwargs)

    monkeypatch.setattr(client_module, "write_part", counting)
    client, table = _make(httpx_mock, fake_s3)
    with pytest.raises(ValidationError, match=r"ns1\.events has a null 'team_id'"):
        table.append(_rows(table, [7, None, 8]))
    assert not encoded
    assert not fake_s3.files
    assert not [r for r in httpx_mock.get_requests() if r.method == "POST"]
    client.close()


@pytest.mark.parametrize(
    ("wire", "partition", "match"),
    [
        (_table_wire(field="val"), ("7", M_JAN), "is a 'string' column, not one of"),
        (_table_wire(field="nope"), ("7", M_JAN), "is no top-level column, not one of"),
        (_table_wire(spec="bucket"), ("7", M_JAN), "no single identity field"),
        (_table_wire(spec=None), None, "no single identity field"),
        (_table_wire(spec="twice"), ("7", "7"), "no single identity field"),
    ],
    ids=[
        "not-an-integer",
        "no-such-column",
        "bucket-not-identity",
        "no-spec",
        "two-identities",
    ],
)
def test_a_layout_no_file_can_be_placed_under_is_refused_before_any_encode(
    httpx_mock, fake_s3, monkeypatch, wire, partition, match
):
    encoded = []
    monkeypatch.setattr(client_module, "write_part", lambda *a, **k: encoded.append(1))
    client, table = _make(httpx_mock, fake_s3, table_wire=wire)
    with pytest.raises(ValidationError, match=match):
        table.prepare_append_tables(
            [(_rows(table, [7]), partition)], idempotency_key=str(uuid.uuid4())
        )
    assert not encoded
    assert not fake_s3.files
    client.close()


@pytest.mark.parametrize(
    ("team_ids", "write_statistics", "verdict"),
    [
        ([7, 8], True, "BOUND_MISMATCH"),
        ([7, None], True, "NULLS_PRESENT"),
        ([7], False, "MISSING_STATS"),
    ],
    ids=["another-values-rows", "nulls", "no-stats"],
)
def test_a_file_whose_footer_does_not_prove_its_value_is_refused_before_upload(
    httpx_mock, fake_s3, tmp_path, team_ids, write_statistics, verdict
):
    """The caller's partition value says 7; the footer, read for the
    registration anyway, must agree, as the server's check at commit would."""
    client, table = _make(httpx_mock, fake_s3)
    path = tmp_path / "claims-7.parquet"
    pq.write_table(_rows(table, team_ids), path, write_statistics=write_statistics)
    with pytest.raises(
        ValidationError, match=rf"team_id=7 of ns1\.events .*\({verdict}\)"
    ):
        table.prepare_append_files(
            [(str(path), ("7", M_JAN))], idempotency_key=str(uuid.uuid4())
        )
    assert not fake_s3.files
    client.close()


def test_prepare_tables_refuses_a_group_holding_another_values_rows(
    httpx_mock, fake_s3
):
    client, table = _make(httpx_mock, fake_s3)
    with pytest.raises(ValidationError, match=r"\(BOUND_MISMATCH\)"):
        table.prepare_append_tables(
            [(_rows(table, [7, 8]), ("7", M_JAN))], idempotency_key=str(uuid.uuid4())
        )
    assert not fake_s3.files
    client.close()


# -- append streams: each file written as its upload comes --------------------------------


@pytest.mark.parametrize("prefix", [True, False], ids=["prefix-table", "plain-table"])
def test_append_writes_each_file_as_its_upload_comes(httpx_mock, monkeypatch, prefix):
    """No variant column, so no reason to hold every group's bytes: write,
    upload, write, upload — on an object-prefix table too, where the proof
    is made on the footer just written, inside the loop."""
    log = []
    fake_s3 = _LoggingS3(log)
    _counting_writes(monkeypatch, log)
    client, table = _make(httpx_mock, fake_s3, table_wire=_table_wire(prefix=prefix))
    _mock_commit(httpx_mock)
    table.append(_rows(table, [7, 8, 9]))
    assert log == ["write", "upload"] * 3
    client.close()


def test_append_with_a_variant_column_still_writes_every_group_first(
    httpx_mock, monkeypatch
):
    """The eager order stays where it always was: an invalid variant row in
    the last group must not orphan the files of the ones before it."""
    log = []
    fake_s3 = _LoggingS3(log)
    _counting_writes(monkeypatch, log)
    client, table = _make(httpx_mock, fake_s3, table_wire=_table_wire(variant=True))
    _mock_commit(httpx_mock)
    table.append(_rows(table, [7, 8, 9], props='{"a": 1}'))
    assert log == ["write"] * 3 + ["upload"] * 3
    client.close()


def test_append_proves_each_file_before_its_own_upload(
    httpx_mock, fake_s3, monkeypatch
):
    """A proof that fails on the third group refuses the append with the
    first two groups' objects in the store and no commit: the same orphans
    a failed third upload has always left, and what the orphan sweep is
    for. Nothing of the failing group is uploaded."""
    checked = []
    original = client_module.extract_column_stats

    def failing_last(metadata, columns):
        checked.append(1)
        return [] if len(checked) == 3 else original(metadata, columns)

    monkeypatch.setattr(client_module, "extract_column_stats", failing_last)
    client, table = _make(httpx_mock, fake_s3)
    with pytest.raises(
        ValidationError, match=r"team_id=9 of ns1\.events .*\(MISSING_STATS\)"
    ):
        table.append(_rows(table, [7, 8, 9]))
    assert len(checked) == 3
    assert sorted(k.split("/")[-2] for k in fake_s3.files) == ["team_id=7", "team_id=8"]
    assert not [r for r in httpx_mock.get_requests() if r.method == "POST"]
    client.close()


# -- without the option: unchanged ---------------------------------------------------------


@pytest.mark.parametrize("deferred", [False, True])
def test_without_the_option_prepared_registrations_and_uris_are_what_they_were(
    httpx_mock, fake_s3, deferred
):
    """Golden: the registration is exactly PreparedPart.registration's for
    the same rows at the same uri, and the uri keeps its shape."""
    client, table = _make(httpx_mock, fake_s3, table_wire=_table_wire(prefix=False))
    groups = [(_rows(table, [7, 8]), ("7", M_JAN)), (_rows(table, [9]), ("9", M_JAN))]
    key = str(uuid.uuid4())
    request = table.prepare_append_tables(
        groups, idempotency_key=key, deferred_stats=deferred
    )
    for index, (reg, (rows, partition)) in enumerate(
        zip(request["appends"][0]["files"], groups, strict=True)
    ):
        assert re.fullmatch(
            rf"s3://bkt/lake/data/ns1/events/{key}/{UUID}-{index}\.parquet", reg["path"]
        )
        expected = write_part(
            rows, table.columns, write_options=parquet_write_options(None, None)
        ).registration(
            reg["path"], table.columns, stats=not deferred, partition_values=partition
        )
        assert reg == expected
        assert ("column_stats" in reg) is not deferred
    client.close()


def test_without_the_option_append_writes_under_the_table_root(httpx_mock, fake_s3):
    client, table = _make(httpx_mock, fake_s3, table_wire=_table_wire(prefix=False))
    _mock_commit(httpx_mock)
    result = table.append(_rows(table, [7, 8]), deferred_stats=True)
    for f in result.files:
        assert re.fullmatch(rf"s3://bkt/lake/data/ns1/events/{UUID}\.parquet", f.path)
    for reg in _posted(httpx_mock)["appends"][0]["files"]:
        assert "column_stats" not in reg
    client.close()


# -- the op ------------------------------------------------------------------------------


def test_set_object_prefix_field_is_the_servers_op():
    assert ops.set_object_prefix_field("team_id").to_wire() == {
        "op": "set_object_prefix_field",
        "name": "team_id",
    }


# -- D4: deferred stats and the size threshold -----------------------------------------


VIAS = ["append", "tables", "files"]


@pytest.mark.parametrize("via", VIAS)
def test_deferred_stats_under_the_threshold_register_the_prefix_fields_stats_alone(
    httpx_mock, fake_s3, tmp_path, via
):
    """A small file (these are ~1 KiB, far under the 1 MiB default) is
    registered provided with the one stat the server checks at commit,
    as millrace registers a file below its footer_stats_min_bytes."""
    client, table = _make(httpx_mock, fake_s3)
    groups = [(_rows(table, [7, 7]), ("7", M_JAN)), (_rows(table, [8]), ("8", M_JAN))]
    regs = _registered(
        httpx_mock, fake_s3, table, groups, tmp_path, via, deferred_stats=True
    )
    assert len(regs) == 2
    for reg in regs:
        assert reg["file_size_bytes"] < DEFAULT_FOOTER_STATS_MIN_BYTES
        assert [s["field_id"] for s in reg["column_stats"]] == [1]
    client.close()


@pytest.mark.parametrize("via", VIAS)
def test_deferred_stats_at_or_over_the_threshold_register_every_stat(
    httpx_mock, fake_s3, tmp_path, via
):
    """Both sides of the threshold, on the file's own size: at it, every
    stat (the hydrator would have written them for a pending file of that
    size, and a provided file is never hydrated); one byte under, the
    prefix field's alone. 0 is every stat for every file."""
    client, table = _make(httpx_mock, fake_s3)
    groups = [(_rows(table, [7, 7]), ("7", M_JAN))]
    (reg,) = _registered(
        httpx_mock, fake_s3, table, groups, tmp_path, via, deferred_stats=True
    )
    size = reg["file_size_bytes"]
    assert size == len(fake_s3.files[reg["path"][len("s3://") :]])

    (at,) = _registered(
        httpx_mock,
        fake_s3,
        table,
        groups,
        tmp_path,
        via,
        deferred_stats=True,
        footer_stats_min_bytes=size,
    )
    assert at["file_size_bytes"] == size
    assert {s["field_id"] for s in at["column_stats"]} == {1, 2, 3}

    (under,) = _registered(
        httpx_mock,
        fake_s3,
        table,
        groups,
        tmp_path,
        via,
        deferred_stats=True,
        footer_stats_min_bytes=size + 1,
    )
    assert [s["field_id"] for s in under["column_stats"]] == [1]

    (zero,) = _registered(
        httpx_mock,
        fake_s3,
        table,
        groups,
        tmp_path,
        via,
        deferred_stats=True,
        footer_stats_min_bytes=0,
    )
    assert {s["field_id"] for s in zero["column_stats"]} == {1, 2, 3}
    client.close()


@pytest.mark.parametrize("via", VIAS)
def test_the_threshold_means_nothing_without_deferred_stats(
    httpx_mock, fake_s3, tmp_path, via
):
    client, table = _make(httpx_mock, fake_s3)
    groups = [(_rows(table, [7]), ("7", M_JAN))]
    (reg,) = _registered(
        httpx_mock,
        fake_s3,
        table,
        groups,
        tmp_path,
        via,
        footer_stats_min_bytes=1 << 40,
    )
    assert {s["field_id"] for s in reg["column_stats"]} == {1, 2, 3}
    client.close()


@pytest.mark.parametrize("via", VIAS)
def test_the_threshold_means_nothing_on_a_table_without_the_option(
    httpx_mock, fake_s3, tmp_path, via
):
    """Deferred stats on a plain table are deferred in full (pending for the
    hydrator), whatever the threshold says."""
    client, table = _make(httpx_mock, fake_s3, table_wire=_table_wire(prefix=False))
    groups = [(_rows(table, [7]), ("7", M_JAN))]
    (reg,) = _registered(
        httpx_mock,
        fake_s3,
        table,
        groups,
        tmp_path,
        via,
        deferred_stats=True,
        footer_stats_min_bytes=0,
    )
    assert "column_stats" not in reg
    client.close()


@pytest.mark.parametrize("bad", [-1, True, 1.5, "1", None], ids=repr)
@pytest.mark.parametrize("via", VIAS)
def test_a_bad_threshold_is_refused_before_any_encode_or_request(
    httpx_mock, fake_s3, tmp_path, monkeypatch, via, bad
):
    encoded = _counting_writes(monkeypatch)
    client, table = _make(httpx_mock, fake_s3)
    groups = [(_rows(table, [7]), ("7", M_JAN))]
    with pytest.raises(ValidationError, match="footer_stats_min_bytes must be"):
        _registered(
            httpx_mock,
            fake_s3,
            table,
            groups,
            tmp_path,
            via,
            deferred_stats=True,
            footer_stats_min_bytes=bad,
        )
    assert not encoded
    assert not fake_s3.files
    assert not [r for r in httpx_mock.get_requests() if r.method == "POST"]
    client.close()


@pytest.mark.parametrize("via", VIAS)
def test_the_deferred_prefix_stat_is_the_expected_bound_on_the_wire(
    httpx_mock, fake_s3, tmp_path, via
):
    """Not just lower == upper: both are ``object_prefix.expected_bound``
    for the value, base64 as ColumnStats.to_wire writes a bound, with no
    nulls and every row counted — the stat the server's proof reads."""
    client, table = _make(httpx_mock, fake_s3)
    groups = [
        (_rows(table, [7, 7, 7]), ("7", M_JAN)),
        (_rows(table, [42]), ("42", M_JAN)),
    ]
    regs = _registered(
        httpx_mock, fake_s3, table, groups, tmp_path, via, deferred_stats=True
    )
    for reg, (rows, (value, _)) in zip(regs, groups, strict=True):
        (stat,) = reg["column_stats"]
        expected = base64.b64encode(
            object_prefix.expected_bound(TableInfo.from_wire(_table_wire()), int(value))
        ).decode("ascii")
        assert stat["lower_bound"] == expected
        assert stat["upper_bound"] == expected
        assert (stat["null_count"], stat["value_count"]) == (0, rows.num_rows)
    client.close()


# -- more refusals, and the types ---------------------------------------------------------


def test_append_refuses_a_negative_value_before_any_encode(
    httpx_mock, fake_s3, monkeypatch
):
    """``-4`` is a long, and its identity partition value is ``"-4"``, which
    names no prefix (the vectors' invalid_value): refused with the field
    and the table named, before the first group is encoded."""
    encoded = _counting_writes(monkeypatch)
    client, table = _make(httpx_mock, fake_s3)
    with pytest.raises(
        ValidationError,
        match=r"'-4' of 'team_id' names no object prefix of ns1\.events",
    ):
        table.append(_rows(table, [7, -4]))
    assert not encoded
    assert not fake_s3.files
    assert not [r for r in httpx_mock.get_requests() if r.method == "POST"]
    client.close()


_MAX_CASES = [
    case
    for case in VECTORS["prefix_cases"]
    if "expected" in case
    and int(case["value"]) == object_prefix.MAX_VALUE[case["col_type"]]
]


@pytest.mark.parametrize("case", _MAX_CASES, ids=lambda c: c["col_type"])
def test_append_places_each_type_at_its_largest_value(httpx_mock, fake_s3, case):
    """Every type a prefix field can have, at its MAX_VALUE: the identity
    transform's string form of the column's own Arrow type is the
    canonical decimal, and the file lands under the vector's prefix."""
    client, table = _make(
        httpx_mock,
        fake_s3,
        catalog_wire=_catalog_wire(case["data_path"]),
        table_wire=_table_wire(
            namespace=case["namespace"],
            table=case["table"],
            field=case["field"],
            col_type=case["col_type"],
        ),
    )
    _mock_commit(httpx_mock)
    result = table.append(_rows(table, [int(case["value"])]))
    (appended,) = result.files
    assert appended.path.startswith(case["expected"])
    assert appended.partition_values == (case["value"], M_JAN)
    (reg,) = _posted(httpx_mock)["appends"][0]["files"]
    assert reg["path"] == appended.path
    client.close()


def test_a_wrong_arity_on_a_prefix_table_is_the_arity_refusal(httpx_mock, fake_s3):
    """The spec has two fields; a one-value tuple is the loop's refusal in
    its own words, not a prefix complaint, and nothing is uploaded."""
    client, table = _make(httpx_mock, fake_s3)
    with pytest.raises(ValidationError, match="partition arity differs"):
        table.prepare_append_tables(
            [(_rows(table, [7]), ("7",))], idempotency_key=str(uuid.uuid4())
        )
    assert not fake_s3.files
    client.close()


def test_a_non_string_partition_value_is_refused(httpx_mock, fake_s3):
    """Partition values are the transforms' wire strings; an int in the
    tuple is the caller's bug, named before any encode or upload."""
    client, table = _make(httpx_mock, fake_s3)
    with pytest.raises(
        ValidationError, match=r"partition value 7 of 'team_id' is not a wire string"
    ):
        table.prepare_append_tables(
            [(_rows(table, [7]), (7, M_JAN))], idempotency_key=str(uuid.uuid4())
        )
    assert not fake_s3.files
    client.close()


def test_a_plan_made_without_the_option_is_a_layout_change(httpx_mock, fake_s3):
    """expected_table_info from a read before the option was set, columns
    and specs identical: a plan against it would write under the table
    root, so it is the layout-changed refusal."""
    client, table = _make(httpx_mock, fake_s3)
    planned = TableInfo.from_wire(_table_wire(prefix=False))
    assert planned.object_prefix_field is None
    live = TableInfo.from_wire(_table_wire())
    assert live.object_prefix_field == "team_id"
    assert (planned.columns, planned.partition_spec, planned.sort_spec) == (
        live.columns,
        live.partition_spec,
        live.sort_spec,
    )
    httpx_mock.add_response(
        method="GET", url=f"{BASE}/v1/catalogs/cat", json=_catalog_wire()
    )
    with pytest.raises(ValidationError, match="destination layout changed"):
        table.prepare_append_tables(
            [(_rows(table, [7]), ("7", M_JAN))],
            idempotency_key=str(uuid.uuid4()),
            expected_table_info=planned,
        )
    assert not fake_s3.files
    client.close()


def test_a_variant_column_beside_the_prefix_field_through_prepared_files(
    httpx_mock, fake_s3, tmp_path
):
    """A caller's file with a variant column is validated by
    validate_variant_file and placed like any other; its deferred
    registration carries the prefix field's stat alone."""
    client, table = _make(httpx_mock, fake_s3, table_wire=_table_wire(variant=True))
    files = []
    for i, (team, props) in enumerate([(7, '{"a": 1}'), (8, '{"b": [true]}')]):
        written = write_part(
            _rows(table, [team], props=props),
            table.columns,
            write_options=parquet_write_options(None, None),
        )
        path = tmp_path / f"v{i}.parquet"
        path.write_bytes(written.body.to_pybytes())
        files.append((str(path), (str(team), M_JAN)))
    key = str(uuid.uuid4())
    full = table.prepare_append_files(files, idempotency_key=key)["appends"][0]["files"]
    for reg, (_, (team, _)) in zip(full, files, strict=True):
        assert reg["path"].startswith(
            f"s3://bkt/lake/data/ns1/events/team_id={team}/{key}/"
        )
        assert {s["field_id"] for s in reg["column_stats"]} == {1, 2, 3}
    deferred = table.prepare_append_files(
        files, idempotency_key=str(uuid.uuid4()), deferred_stats=True
    )
    for reg in deferred["appends"][0]["files"]:
        assert [s["field_id"] for s in reg["column_stats"]] == [1]
    client.close()


def test_a_variant_column_beside_the_prefix_field_through_append(httpx_mock, fake_s3):
    client, table = _make(httpx_mock, fake_s3, table_wire=_table_wire(variant=True))
    _mock_commit(httpx_mock)
    result = table.append(_rows(table, [7, 8, 7], props='{"a": [1, null]}'))
    assert {f.partition_values[0] for f in result.files} == {"7", "8"}
    for reg in _posted(httpx_mock)["appends"][0]["files"]:
        assert f"/team_id={reg['partition_values'][0]}/" in reg["path"]
        assert {s["field_id"] for s in reg["column_stats"]} == {1, 2, 3}
    _mock_commit(httpx_mock)
    table.append(_rows(table, [9], props="null"), deferred_stats=True)
    (reg,) = _posted(httpx_mock)["appends"][0]["files"]
    assert [s["field_id"] for s in reg["column_stats"]] == [1]
    client.close()


# -- multi-row-group footers through prepare_append_files ------------------------------------


def _write_row_groups(path, table, *row_groups):
    """One file, one row group per table given."""
    with pq.ParquetWriter(path, row_groups[0].schema) as writer:
        for rows in row_groups:
            writer.write_table(rows)
    return str(path)


def _read_any(protocol, kind):
    """A thrift value of ``kind`` from the compact protocol, as a plain tree
    (a struct is ``{field_id: (kind, value)}``), so it can be written back
    unchanged apart from the edit a test makes."""
    if kind == TType.STRUCT:
        out = {}
        protocol.readStructBegin()
        while True:
            _, field_kind, field = protocol.readFieldBegin()
            if field_kind == TType.STOP:
                break
            out[field] = (field_kind, _read_any(protocol, field_kind))
            protocol.readFieldEnd()
        protocol.readStructEnd()
        return out
    if kind in (TType.LIST, TType.SET):
        element_kind, size = protocol.readListBegin()
        items = [_read_any(protocol, element_kind) for _ in range(size)]
        protocol.readListEnd()
        return (element_kind, items)
    if kind == TType.MAP:
        key_kind, value_kind, size = protocol.readMapBegin()
        items = [
            (_read_any(protocol, key_kind), _read_any(protocol, value_kind))
            for _ in range(size)
        ]
        protocol.readMapEnd()
        return (key_kind, value_kind, items)
    reader = {
        TType.BOOL: protocol.readBool,
        TType.BYTE: protocol.readByte,
        TType.I16: protocol.readI16,
        TType.I32: protocol.readI32,
        TType.I64: protocol.readI64,
        TType.DOUBLE: protocol.readDouble,
        TType.STRING: protocol.readBinary,
    }[kind]
    return reader()


def _write_any(protocol, kind, value):
    if kind == TType.STRUCT:
        protocol.writeStructBegin("")
        for field, (field_kind, field_value) in value.items():
            protocol.writeFieldBegin("", field_kind, field)
            _write_any(protocol, field_kind, field_value)
            protocol.writeFieldEnd()
        protocol.writeFieldStop()
        protocol.writeStructEnd()
        return
    if kind in (TType.LIST, TType.SET):
        element_kind, items = value
        protocol.writeListBegin(element_kind, len(items))
        for item in items:
            _write_any(protocol, element_kind, item)
        protocol.writeListEnd()
        return
    if kind == TType.MAP:
        key_kind, value_kind, items = value
        protocol.writeMapBegin(key_kind, value_kind, len(items))
        for key, item in items:
            _write_any(protocol, key_kind, key)
            _write_any(protocol, value_kind, item)
        protocol.writeMapEnd()
        return
    writer = {
        TType.BOOL: protocol.writeBool,
        TType.BYTE: protocol.writeByte,
        TType.I16: protocol.writeI16,
        TType.I32: protocol.writeI32,
        TType.I64: protocol.writeI64,
        TType.DOUBLE: protocol.writeDouble,
        TType.STRING: protocol.writeBinary,
    }[kind]
    writer(value)


# parquet.thrift field ids: FileMetaData.row_groups, RowGroup.columns,
# ColumnChunk.meta_data, ColumnMetaData.statistics.
_ROW_GROUPS, _COLUMNS, _META_DATA, _STATISTICS = 4, 1, 3, 12


def _drop_statistics(path, row_group, column):
    """Rewrite ``path``'s footer with the Statistics struct removed from one
    column chunk of one row group: a file whose other row groups carry
    stats for the column and that one does not, which no pyarrow option
    writes (``write_statistics`` is per file and per column)."""
    raw = Path(path).read_bytes()
    assert raw[-4:] == b"PAR1"
    (length,) = struct.unpack("<I", raw[-8:-4])
    start = len(raw) - 8 - length
    protocol = TCompactProtocol(TMemoryBuffer(raw[start:-8]))
    footer = _read_any(protocol, TType.STRUCT)
    _, (_, groups) = footer[_ROW_GROUPS]
    _, (_, chunks) = groups[row_group][_COLUMNS]
    _, meta = chunks[column][_META_DATA]
    del meta[_STATISTICS]
    out = TMemoryBuffer()
    _write_any(TCompactProtocol(out), TType.STRUCT, footer)
    patched = out.getvalue()
    Path(path).write_bytes(
        raw[:start] + patched + struct.pack("<I", len(patched)) + b"PAR1"
    )
    metadata = pq.read_metadata(path)
    assert metadata.row_group(row_group).column(column).statistics is None
    return str(path)


def test_a_multi_row_group_file_of_one_value_is_accepted(httpx_mock, fake_s3, tmp_path):
    client, table = _make(httpx_mock, fake_s3)
    path = _write_row_groups(
        tmp_path / "rg.parquet", table, _rows(table, [7, 7]), _rows(table, [7])
    )
    assert pq.read_metadata(path).num_row_groups == 2
    key = str(uuid.uuid4())
    request = table.prepare_append_files(
        [(path, ("7", M_JAN))], idempotency_key=key, deferred_stats=True
    )
    (reg,) = request["appends"][0]["files"]
    assert reg["path"].startswith(f"s3://bkt/lake/data/ns1/events/team_id=7/{key}/")
    assert reg["record_count"] == 3
    (stat,) = reg["column_stats"]
    assert (stat["value_count"], stat["null_count"]) == (3, 0)
    expected = base64.b64encode(
        object_prefix.expected_bound(TableInfo.from_wire(_table_wire()), 7)
    ).decode("ascii")
    assert stat["lower_bound"] == stat["upper_bound"] == expected
    client.close()


@pytest.mark.parametrize(
    ("second", "edit", "verdict"),
    [
        ([8], None, "BOUND_MISMATCH"),
        ([None], None, "NULLS_PRESENT"),
        ([7], (1, 0), "MISSING_STATS"),
    ],
    ids=[
        "another-value-in-one-row-group",
        "an-all-null-row-group",
        "one-row-group-without-stats",
    ],
)
def test_a_multi_row_group_footer_that_does_not_prove_the_value_is_refused(
    httpx_mock, fake_s3, tmp_path, second, edit, verdict
):
    """The proof reads the whole footer as the server does: every row
    group's bounds and null counts, and a column chunk without statistics
    in ANY row group leaves the column with no stats at all (the module
    never guesses a bound), so the file is refused before its upload."""
    client, table = _make(httpx_mock, fake_s3)
    path = _write_row_groups(
        tmp_path / "rg.parquet", table, _rows(table, [7, 7]), _rows(table, second)
    )
    if edit is not None:
        _drop_statistics(path, *edit)
    with pytest.raises(
        ValidationError, match=rf"team_id=7 of ns1\.events .*\({verdict}\)"
    ):
        table.prepare_append_files(
            [(path, ("7", M_JAN))], idempotency_key=str(uuid.uuid4())
        )
    assert not fake_s3.files
    client.close()


# -- scale: not CI-gating ------------------------------------------------------------------


_BENCH = pytest.mark.skipif(
    os.environ.get("PYHOGLAKE_BENCH") != "1",
    reason="set PYHOGLAKE_BENCH=1 to run the scale checks (not CI-gating)",
)


@_BENCH
@pytest.mark.bench
@pytest.mark.parametrize("n_cols", [3, 120])
@pytest.mark.parametrize("n_groups", [50, 200, 500])
def test_bench_prepared_groups_cost_about_the_same_each(
    httpx_mock, fake_s3, monkeypatch, n_groups, n_cols
):
    """prepare_append_tables over N one-team groups, on a 3-column and a
    120-column table: per-group wall time roughly constant in N (no
    quadratic step in the placement), and the proof a small fraction of
    the encode — on the wide table the proof walks one column where the
    registration walks them all. Numbers are printed; the assertions are
    loose ceilings."""
    encode, proof = [], []
    original_write, original_proof = (
        client_module.write_part,
        client_module._placed_prefix,
    )

    def timed_write(*args, **kwargs):
        started = time.perf_counter()
        try:
            return original_write(*args, **kwargs)
        finally:
            encode.append(time.perf_counter() - started)

    def timed_proof(*args, **kwargs):
        started = time.perf_counter()
        try:
            return original_proof(*args, **kwargs)
        finally:
            proof.append(time.perf_counter() - started)

    monkeypatch.setattr(client_module, "write_part", timed_write)
    monkeypatch.setattr(client_module, "_placed_prefix", timed_proof)
    client, table = _make(
        httpx_mock, fake_s3, table_wire=_table_wire(extra_columns=n_cols - 3)
    )
    assert len(table.columns) == n_cols
    groups = [
        (_many_rows(table, t, 20), (str(t), M_JAN)) for t in range(1, n_groups + 1)
    ]
    started = time.perf_counter()
    table.prepare_append_tables(
        groups, idempotency_key=str(uuid.uuid4()), deferred_stats=True, concurrency=1
    )
    total = time.perf_counter() - started
    per_group = total / n_groups
    print(
        f"\n{n_groups} groups x {n_cols} cols: {total * 1e3:.1f} ms total, "
        f"{per_group * 1e6:.0f} us/group, "
        f"encode {sum(encode) / n_groups * 1e6:.0f} us, proof {sum(proof) / n_groups * 1e6:.0f} us "
        f"({sum(proof) / sum(encode) * 100:.1f}% of encode)"
    )
    assert len(encode) == len(proof) == n_groups
    assert sum(proof) < 0.25 * sum(encode)
    assert per_group < 0.02  # 20 ms; a laptop does ~1 ms
    client.close()


@_BENCH
@pytest.mark.bench
def test_bench_append_peak_memory_on_a_prefix_table_is_bounded(httpx_mock, fake_s3):
    """append over ~256 MiB of input in 170 groups, uncompressed parquet:
    with the files written as their uploads come, Arrow's pool peaks at
    the input's partition copies plus a file or so in flight — not every
    group's encoded bytes as well, which the eager form held (and which
    would be about the input's size again here). Measured on Arrow's own
    pool (tracemalloc does not see Arrow's allocations); the fake store
    keeps the uploaded bodies as Python bytes, outside the pool. Baseline
    printed."""
    client, table = _make(httpx_mock, fake_s3)
    _mock_commit(httpx_mock)
    n_groups, rows_per_group, val_bytes = 170, 1500, 1024
    data = pa.concat_tables(
        [
            _many_rows(table, t, rows_per_group, val_bytes)
            for t in range(1, n_groups + 1)
        ]
    )
    assert data.nbytes > 200 * 2**20
    pool = pa.default_memory_pool()
    before_peak, before_now = pool.max_memory(), pa.total_allocated_bytes()
    table.append(data, compression="none")
    peak = pool.max_memory() - before_peak
    total_files = sum(len(b) for b in fake_s3.files.values())
    print(
        f"\n{n_groups} groups, {data.nbytes / 2**20:.0f} MiB input, "
        f"{total_files / 2**20:.0f} MiB of files: Arrow pool peak {peak / 2**20:.0f} MiB "
        f"over the input (live before: {before_now / 2**20:.0f} MiB)"
    )
    # The partition split copies the input once (the groups are alive
    # together); the encoded files must not be alive together too.
    assert total_files > 0.8 * data.nbytes  # uncompressed: eager would fail below
    assert peak < data.nbytes * 1.25 + 32 * 2**20
    client.close()
