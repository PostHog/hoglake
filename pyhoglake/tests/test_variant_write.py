"""Suite W: VARIANT columns through the Arrow write paths, ``Table.append``
and ``Table.prepare_append_tables``.

A variant column's values come as JSON text (``pa.json_()``), the null
type, or storage in its declaration's write form; each partition group is
encoded on its own and written by ``write_variant_parquet``. These tests
read what reached the (recording) object store: the footer's VARIANT
annotation, the registration's sizes, the stats, and the values.
"""

from __future__ import annotations

import io
import json
import re
import struct
import uuid
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from test_append_unit import BASE, CATALOG_WIRE, FakeS3
from test_upload import RecordingS3, resolved_table

from pyhoglake import (
    VARIANT_NULL,
    HoglakeClient,
    ValidationError,
    VariantEncodingError,
    VariantReport,
    variant,
)
from pyhoglake import writer as writer_module
from pyhoglake.client import Namespace
from pyhoglake.errors import UnsupportedTypeError
from pyhoglake.models import Column
from pyhoglake.parquet_schema import _schema_elements, _top_level
from pyhoglake.types import columns_to_arrow_schema

DECL = {
    "type": "object",
    "fields": [
        {"name": "$browser", "type": "string"},
        {"name": "price", "type": "decimal8", "precision": 18, "scale": 2},
        # A declared key is a value's key, user data: not a reserved name.
        {"name": "_hog_x", "type": "int64"},
    ],
}

COLUMNS_WIRE = [
    {"name": "id", "type": "long", "field_id": 1, "ordinal": 0, "nullable": False},
    {"name": "name", "type": "string", "field_id": 2, "ordinal": 1, "nullable": True},
    {
        "name": "props",
        "type": "variant",
        "field_id": 3,
        "ordinal": 2,
        "nullable": True,
        "type_params": {"shredding": DECL},
    },
    {"name": "raw", "type": "variant", "field_id": 4, "ordinal": 3, "nullable": False},
]

WIRE = {
    "name": "events",
    "namespace": "ns1",
    "table_uuid": "0b8ee9ba-79a1-4f3e-b7e5-6a0b6ab6f012",
    "columns": COLUMNS_WIRE,
    "record_count": 0,
    "file_count": 0,
    "file_size_bytes": 0,
}

PARTITIONED = {
    **WIRE,
    "partition_spec": {
        "spec_id": 1,
        "fields": [{"source_field_id": 1, "transform": "identity"}],
    },
}

_TABLES_URL = f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events"

PROPS = [
    '{"$browser": "Chrome", "price": 12.5, "_hog_x": 7, "n": [1, "a"]}',
    '"text"',
    "null",
    None,
]
PROPS_BACK = [
    {"$browser": "Chrome", "price": 12.5, "_hog_x": 7, "n": [1, "a"]},
    "text",
    VARIANT_NULL,
    None,
]
RAW = ["1", '{"k": true}', "18446744073709551616", '"x"']
RAW_BACK = [1, {"k": True}, Decimal(18446744073709551616), "x"]


def _columns():
    return tuple(Column.from_wire(c) for c in COLUMNS_WIRE)


def _json_table(props=PROPS, raw=RAW, *, ids=None, schema=None):
    ids = list(range(len(props))) if ids is None else ids
    schema = schema or columns_to_arrow_schema(_columns(), variant="json")
    return pa.table(
        {
            "id": ids,
            "name": [f"n{i}" for i in ids],
            "props": pa.array(props, pa.string()).cast(pa.json_()),
            "raw": pa.array(raw, pa.string()).cast(pa.json_()),
        },
        schema=schema,
    )


def _check_file(raw: bytes, reg: dict, rows: int) -> pa.Table:
    """What every variant file must be: VARIANT(1) on each variant group,
    sizes measured on the stamped bytes, no stats for a variant field id."""
    elements = _schema_elements(raw)
    groups = {e.get(4): e for e, _, _ in _top_level(elements)}
    for name, field_id in (("props", 3), ("raw", 4)):
        assert groups[name][10] == {16: {1: 1}}
        assert groups[name][9] == field_id
    assert groups["id"].get(10) is None
    assert reg["record_count"] == rows
    assert reg["file_size_bytes"] == len(raw)
    assert reg["footer_size"] == struct.unpack("<I", raw[-8:-4])[0]
    assert {s["field_id"] for s in reg["column_stats"]} == {1, 2}
    return pq.read_table(pa.BufferReader(raw))


def _values(table: pa.Table, name: str):
    column = next(c for c in _columns() if c.name == name)
    return variant.to_python(table.column(name), column)


# -- Table.append ----------------------------------------------------------------


@pytest.fixture
def store():
    return FakeS3()


def _append_table(httpx_mock, store, wire=WIRE, *, commit=True):
    client = HoglakeClient(BASE)
    client.s3 = store
    httpx_mock.add_response(
        method="GET", url=f"{BASE}/v1/catalogs/cat", json=CATALOG_WIRE
    )
    httpx_mock.add_response(method="GET", url=_TABLES_URL, json=wire)
    table = Namespace(client.catalog("cat"), "ns1").table("events")
    # The pre-flight re-resolve of each append, then the commit; both
    # optional, so a refusal that comes first leaves them unused.
    httpx_mock.add_response(
        method="GET",
        url=f"{_TABLES_URL}?totals=false",
        json=wire,
        is_optional=True,
        is_reusable=True,
    )
    if commit:
        httpx_mock.add_response(
            method="POST",
            url=f"{BASE}/v1/catalogs/cat/commit",
            json={"snapshot_id": 6, "schema_version": 2},
            is_optional=True,
        )
    return table


def _committed(httpx_mock):
    posts = [r for r in httpx_mock.get_requests() if r.method == "POST"]
    (post,) = posts
    return json.loads(post.content)["appends"][0]["files"]


def test_append_writes_json_text_as_variant_files(httpx_mock, store):
    table = _append_table(httpx_mock, store)
    report = VariantReport()
    table.append(_json_table(), read_snapshot=5, variant_report=report)
    (reg,) = _committed(httpx_mock)
    (raw,) = store.files.values()
    back = _check_file(raw, reg, 4)
    assert back.column("id").to_pylist() == [0, 1, 2, 3]
    assert _values(back, "props") == PROPS_BACK
    assert _values(back, "raw") == RAW_BACK
    # Both columns' counts, in the one report.
    assert report.rows == 8
    assert report.sql_null_rows == 1
    assert report.typed[("props", "$.$browser")] == 1
    assert report.fallback[("props", "$.price")] == 1  # a double, never a decimal


def test_append_takes_the_null_type_as_all_sql_null(httpx_mock, store):
    table = _append_table(httpx_mock, store)
    data = _json_table()
    data = data.set_column(2, pa.field("props", pa.null()), pa.nulls(4))
    table.append(data, read_snapshot=5)
    (reg,) = _committed(httpx_mock)
    (raw,) = store.files.values()
    back = _check_file(raw, reg, 4)
    assert _values(back, "props") == [None] * 4
    assert _values(back, "raw") == RAW_BACK


def test_append_writes_pre_encoded_storage_as_given(httpx_mock, store):
    columns = _columns()
    props = variant.encode_python(
        [{"price": Decimal("1.25"), "_hog_x": 1}, VARIANT_NULL], columns[2]
    ).array
    raw_storage = variant.encode_python(["a", 2], columns[3]).array
    data = pa.table(
        {"id": [1, 2], "name": ["a", None], "props": props, "raw": raw_storage}
    )
    table = _append_table(httpx_mock, store)
    table.append(data, read_snapshot=5)
    (reg,) = _committed(httpx_mock)
    (raw,) = store.files.values()
    back = _check_file(raw, reg, 2)
    assert _values(back, "props") == [
        {"price": Decimal("1.25"), "_hog_x": 1},
        VARIANT_NULL,
    ]
    assert _values(back, "raw") == ["a", 2]


def test_append_encodes_each_partition_group_on_its_own(httpx_mock, store, monkeypatch):
    """The JSON is encoded after the split, a group at a time: the encoder
    sees each group's rows, never the batch's, and each group is a file."""
    seen = []
    real = writer_module.encode_json

    def spy(values, column, **kwargs):
        seen.append((column.name, len(values)))
        return real(values, column, **kwargs)

    monkeypatch.setattr(writer_module, "encode_json", spy)
    table = _append_table(httpx_mock, store, PARTITIONED)
    report = VariantReport()
    data = _json_table(ids=[7, 8, 7, 7])
    table.append(data, read_snapshot=5, variant_report=report, row_group_size=1)
    regs = _committed(httpx_mock)
    assert [r["partition_values"] for r in regs] == [["7"], ["8"]]
    assert sorted(seen) == [("props", 1), ("props", 3), ("raw", 1), ("raw", 3)]
    assert report.rows == 8
    by_path = {key.removeprefix("bkt/lake/"): raw for key, raw in store.files.items()}
    for reg, rows, props in zip(
        regs,
        (3, 1),
        ([PROPS_BACK[0], PROPS_BACK[2], PROPS_BACK[3]], [PROPS_BACK[1]]),
        strict=True,
    ):
        raw = by_path[reg["path"].removeprefix("s3://bkt/lake/")]
        back = _check_file(raw, reg, rows)
        assert _values(back, "props") == props
        assert pq.read_metadata(pa.BufferReader(raw)).num_row_groups == rows


def test_append_writes_variant_files_in_the_requested_codec(httpx_mock, store):
    table = _append_table(httpx_mock, store)
    table.append(
        _json_table(), read_snapshot=5, compression="zstd", compression_level=3
    )
    (raw,) = store.files.values()
    metadata = pq.read_metadata(pa.BufferReader(raw))
    group = metadata.row_group(0)
    codecs = {group.column(i).compression for i in range(group.num_columns)}
    assert codecs == {"ZSTD"}


def test_append_invalid_rows_raise_before_any_upload(httpx_mock, store):
    """An invalid row in the LAST partition: every group is encoded before
    the first upload, so nothing is left behind and nothing committed."""
    table = _append_table(httpx_mock, store, PARTITIONED)
    props = [*PROPS[:3], '{"a": 1, "a": 2}']
    with pytest.raises(VariantEncodingError) as refused:
        table.append(_json_table(props, ids=[1, 1, 1, 2]), read_snapshot=5)
    assert refused.value.reason == "duplicate_key"
    assert refused.value.column == "props"
    assert refused.value.row == 0  # its row in its partition group
    assert not store.files


def test_append_on_invalid_null_writes_sql_null_and_counts(httpx_mock, store):
    columns_wire = [{**c, "nullable": True} for c in COLUMNS_WIRE]
    table = _append_table(httpx_mock, store, {**WIRE, "columns": columns_wire})
    report = VariantReport()
    props = ['{"a": 1, "a": 2}', '{"x": NaN}', '"ok"', None]
    table.append(
        _json_table(props),
        read_snapshot=5,
        variant_on_invalid="null",
        variant_report=report,
    )
    (raw,) = store.files.values()
    back = pq.read_table(pa.BufferReader(raw))
    assert variant.to_python(back.column("props")) == [None, None, "ok", None]
    assert report.invalid_rows == 2
    assert report.invalid_by_reason == {"duplicate_key": 1, "non_finite_number": 1}
    assert report.sql_null_rows == 1
    assert report.first_invalid[:3] == ("props", 0, "duplicate_key")


@pytest.mark.parametrize("value", ["sometimes", None, 1])
def test_append_refuses_an_unknown_on_invalid_before_any_request(
    httpx_mock, store, value
):
    table = _append_table(httpx_mock, store, commit=False)
    before = len(httpx_mock.get_requests())
    with pytest.raises(
        ValidationError, match="variant_on_invalid is 'raise' or 'null'"
    ):
        table.append(_json_table(), variant_on_invalid=value)
    assert len(httpx_mock.get_requests()) == before


def test_on_invalid_null_is_refused_for_a_not_null_variant(httpx_mock, store):
    """Whatever form raw's values come in: an invalid row would have
    nowhere to go."""
    table = _append_table(httpx_mock, store, commit=False)
    columns = _columns()
    data = _json_table().set_column(
        3,
        columns_to_arrow_schema(columns, variant="storage").field("raw"),
        variant.encode_python([1, 2, 3, 4], columns[3]).array,
    )
    for given in (_json_table(), data):
        with pytest.raises(ValidationError, match=r"\['raw'\] are NOT NULL"):
            table.append(given, read_snapshot=5, variant_on_invalid="null")
    assert not store.files


@pytest.mark.parametrize(
    ("kind", "message"),
    [
        (pa.string(), "takes JSON text as pa.json_(), not string: cast it"),
        (pa.large_string(), "takes JSON text as pa.json_(), not large_string"),
        (pa.string_view(), "takes JSON text as pa.json_(), not string_view"),
        (pa.binary(), "takes pa.json_() text, the null type"),
        (pa.int64(), "takes pa.json_() text, the null type"),
        ("read", "takes pa.json_() text, the null type"),
        ("other", "takes pa.json_() text, the null type"),
    ],
)
def test_append_refuses_what_a_variant_is_not_given_as(
    httpx_mock, store, kind, message
):
    table = _append_table(httpx_mock, store, commit=False)
    if kind == "read":
        values = pa.nulls(4, variant.storage_type(DECL, form="read"))
    elif kind == "other":
        values = variant.encode_python([1, 2, 3, 4], shredding={"type": "int64"}).array
    elif pa.types.is_integer(kind):
        values = pa.array([1, 2, 3, 4], kind)
    else:
        values = pa.array(["{}"] * 4, kind)
    data = _json_table().set_column(2, pa.field("props", values.type), values)
    with pytest.raises(
        ValidationError, match=re.escape(f"variant column 'props' {message}")
    ):
        table.append(data, read_snapshot=5)
    assert not store.files


def test_append_refuses_data_missing_a_variant_column(httpx_mock, store):
    table = _append_table(httpx_mock, store, commit=False)
    with pytest.raises(ValidationError, match=r"missing table columns: \['props'\]"):
        table.append(_json_table().drop_columns(["props"]), read_snapshot=5)
    assert not store.files


@pytest.mark.parametrize(
    ("wire", "order"),
    [(WIRE, [0, 1, 2, 3]), (PARTITIONED, [0, 2, 3, 1])],
    ids=["unpartitioned", "partitioned"],
)
@pytest.mark.parametrize("storage", [pa.large_string(), pa.string_view()])
def test_append_takes_json_over_every_storage_type(
    httpx_mock, store, wire, order, storage
):
    """Partitioned too: the split has no take kernel for string_view."""
    table = _append_table(httpx_mock, store, wire)
    data = _json_table(ids=[7, 8, 7, 7])
    values = pa.array(PROPS, storage).cast(pa.json_(storage))
    table.append(
        data.set_column(2, pa.field("props", values.type), values), read_snapshot=5
    )
    by_path = {key.removeprefix("bkt/lake/"): raw for key, raw in store.files.items()}
    back = [
        value
        for reg in _committed(httpx_mock)
        for value in _values(
            pq.read_table(
                pa.BufferReader(by_path[reg["path"].removeprefix("s3://bkt/lake/")])
            ),
            "props",
        )
    ]
    assert back == [PROPS_BACK[i] for i in order]


@pytest.mark.parametrize("form", ["json", "null", "storage"])
def test_append_refuses_sql_null_into_a_not_null_variant(httpx_mock, store, form):
    table = _append_table(httpx_mock, store, commit=False)
    rows = ["1", None, "2", "3"]
    if form == "json":
        values = pa.array(rows).cast(pa.json_())
    elif form == "null":
        values = pa.nulls(4)
    else:
        nullable = Column("raw", "variant", 4, 3, True)
        values = variant.encode_json(rows, nullable).array
    data = _json_table().set_column(3, pa.field("raw", values.type), values)
    with pytest.raises(
        VariantEncodingError, match="SQL NULL into a NOT NULL"
    ) as refused:
        table.append(data, read_snapshot=5)
    assert refused.value.reason == "sql_null_in_not_null"
    assert refused.value.column == "raw"
    assert refused.value.row == (0 if form == "null" else 1)
    assert not store.files


def test_append_names_the_not_null_row_in_the_table_given(httpx_mock, store):
    """Before the partition split: input row 3 is row 1 of its group."""
    table = _append_table(httpx_mock, store, PARTITIONED, commit=False)
    data = _json_table(raw=["1", "2", "3", None], ids=[1, 2, 1, 2])
    with pytest.raises(VariantEncodingError, match="row 3: SQL NULL") as refused:
        table.append(data, read_snapshot=5)
    assert refused.value.row == 3
    assert not store.files


def test_append_refuses_a_variant_nested_in_a_struct(httpx_mock, store):
    nested = {
        "name": "s",
        "type": "struct",
        "field_id": 5,
        "ordinal": 4,
        "nullable": True,
        "children": [{"name": "v", "type": "variant", "field_id": 6, "ordinal": 0}],
    }
    wire = {**WIRE, "columns": [*COLUMNS_WIRE, nested]}
    table = _append_table(httpx_mock, store, wire, commit=False)
    with pytest.raises(UnsupportedTypeError, match="'v' is nested in a struct"):
        table.append(_json_table(), read_snapshot=5)
    assert not store.files


@pytest.mark.parametrize(
    ("broken_as", "refusal"),
    [
        ("structure", "raw: metadata is null"),
        ("view", "props has a null in a non-nullable field under a SQL NULL row"),
    ],
)
def test_append_refuses_broken_pre_encoded_storage_as_a_validation_error(
    httpx_mock, store, broken_as, refusal
):
    """Storage checked by structure: a null in a REQUIRED field of a present
    row, which pyarrow would write as other bytes, or under a SQL NULL row
    (pa.nulls), which the writer cannot view."""
    table = _append_table(httpx_mock, store, commit=False)
    columns = _columns()
    if broken_as == "structure":
        storage = variant.encode_python([1, "x", 2, 3], columns[3]).array
        metadata = storage.field("metadata").to_pylist()
        broken = pa.StructArray.from_arrays(
            [pa.array([None, *metadata[1:]], pa.binary()), storage.field("value")],
            fields=list(storage.type),
        )
        data = _json_table().set_column(3, pa.field("raw", broken.type), broken)
    else:
        kind = columns_to_arrow_schema(columns, variant="storage").field("props").type
        broken = pa.nulls(4, kind)
        data = _json_table().set_column(2, pa.field("props", kind), broken)
    with pytest.raises(
        ValidationError, match=f"cannot write VARIANT columns: {refusal}"
    ):
        table.append(data, read_snapshot=5)
    assert not store.files


def test_append_reserved_check_skips_variant_storage_but_not_its_name(
    httpx_mock, store
):
    """A declared key `_hog_x` is in the storage's typed_value: user data,
    not a column name. A top-level `_hog` column is still refused, before
    any request."""
    columns = _columns()
    props = variant.encode_python([{"_hog_x": 1}] * 4, columns[2]).array
    assert "_hog_x" in str(props.type)
    data = _json_table().set_column(2, pa.field("props", props.type), props)
    table = _append_table(httpx_mock, store)
    table.append(data, read_snapshot=5)
    assert len(store.files) == 1
    with pytest.raises(ValidationError, match=r"\['_hog_props'\] use the reserved"):
        table.append(data.rename_columns(["id", "name", "_hog_props", "raw"]))


# -- Table.prepare_append_tables -------------------------------------------------


@pytest.fixture
def prepared(httpx_mock):
    store = RecordingS3()
    table = resolved_table(httpx_mock, store, WIRE)
    yield table, store
    table._namespace._catalog._client.close()


def test_prepare_tables_writes_json_null_and_pre_encoded_groups(prepared):
    table, store = prepared
    columns = _columns()
    schema = columns_to_arrow_schema(columns, variant="json")
    storage = columns_to_arrow_schema(columns, variant="storage")
    pre_encoded = pa.table(
        [
            pa.array([5], pa.int64()),
            pa.array(["e"]),
            variant.encode_python([{"$browser": "x"}], columns[2]).array,
            variant.encode_python([[1, 2]], columns[3]).array,
        ],
        schema=storage,
    )
    null_typed = pa.table(
        [
            pa.array([6], pa.int64()),
            pa.array([None], pa.string()),
            pa.nulls(1),
            pa.array(["false"]).cast(pa.json_()),
        ],
        schema=schema.set(
            2, pa.field("props", pa.null(), metadata=schema.field(2).metadata)
        ),
    )
    report = VariantReport()
    request = table.prepare_append_tables(
        [(_json_table(schema=schema), None), (pre_encoded, None), (null_typed, None)],
        idempotency_key=str(uuid.uuid4()),
        variant_report=report,
    )
    regs = request["appends"][0]["files"]
    expected = [
        (PROPS_BACK, RAW_BACK),
        ([{"$browser": "x"}], [[1, 2]]),
        ([None], [False]),
    ]
    for reg, (props, raw) in zip(regs, expected, strict=True):
        body = store.files[reg["path"].removeprefix("s3://")]
        back = _check_file(body, reg, len(props))
        assert _values(back, "props") == props
        assert _values(back, "raw") == raw
    # The JSON groups' counts: four rows of each column, then the null
    # group's two columns' one row each.
    assert report.rows == 10
    assert report.sql_null_rows == 2


def test_prepare_tables_codec_reaches_the_variant_file(prepared):
    table, store = prepared
    request = table.prepare_append_tables(
        [(_json_table(), None)],
        idempotency_key=str(uuid.uuid4()),
        compression="zstd",
    )
    (reg,) = request["appends"][0]["files"]
    group = pq.read_metadata(
        pa.BufferReader(store.files[reg["path"].removeprefix("s3://")])
    ).row_group(0)
    assert {group.column(i).compression for i in range(group.num_columns)} == {"ZSTD"}


@pytest.mark.parametrize("value", ["sometimes", None, 1])
@pytest.mark.parametrize("variants", [True, False])
def test_prepare_tables_refuse_an_unknown_on_invalid_before_any_request(
    httpx_mock, variants, value
):
    """With or without a variant column: the setting is checked before the
    destination is read, as append checks it."""
    store = RecordingS3()
    table = resolved_table(httpx_mock, store, WIRE if variants else PLAIN_WIRE)
    data = _json_table() if variants else _plain_table()
    before = len(httpx_mock.get_requests())
    with pytest.raises(
        ValidationError, match="variant_on_invalid is 'raise' or 'null'"
    ):
        table.prepare_append_tables(
            [(data, None)],
            idempotency_key=str(uuid.uuid4()),
            variant_on_invalid=value,
        )
    assert len(httpx_mock.get_requests()) == before
    assert not store.files
    table._namespace._catalog._client.close()


def test_prepare_tables_on_invalid_modes(prepared):
    table, store = prepared
    data = _json_table([*PROPS[:3], "[1, 2"])
    with pytest.raises(VariantEncodingError) as refused:
        table.prepare_append_tables([(data, None)], idempotency_key=str(uuid.uuid4()))
    assert refused.value.reason == "invalid_json"
    assert refused.value.uploaded_files == 0
    with pytest.raises(ValidationError, match=r"\['raw'\] are NOT NULL"):
        table.prepare_append_tables(
            [(data, None)],
            idempotency_key=str(uuid.uuid4()),
            variant_on_invalid="null",
        )
    assert not store.files


def test_prepare_tables_on_invalid_null_counts(httpx_mock):
    store = RecordingS3()
    wire = {**WIRE, "columns": [{**c, "nullable": True} for c in COLUMNS_WIRE]}
    table = resolved_table(httpx_mock, store, wire)
    report = VariantReport()
    data = _json_table(
        ['"a"', "[1, 2", None, "1e400"],
        schema=columns_to_arrow_schema(table.columns, variant="json"),
    )
    request = table.prepare_append_tables(
        [(data, None)],
        idempotency_key=str(uuid.uuid4()),
        variant_on_invalid="null",
        variant_report=report,
    )
    (reg,) = request["appends"][0]["files"]
    back = pq.read_table(
        pa.BufferReader(store.files[reg["path"].removeprefix("s3://")])
    )
    assert variant.to_python(back.column("props")) == ["a", None, None, None]
    assert report.invalid_by_reason == {"invalid_json": 1, "non_finite_number": 1}
    assert report.sql_null_rows == 1


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ("field_id", "props carries field id 9, not its column's 3"),
        ("nullable", "raw is nullable, and its column NOT NULL"),
    ],
)
def test_prepare_tables_holds_a_variant_field_to_its_column(prepared, change, message):
    """A variant field carries the destination's field id and nullability,
    as every field does; the caller's table is refused as such, before any
    upload, not blamed on pyhoglake's layout."""
    table, store = prepared
    schema = columns_to_arrow_schema(_columns(), variant="json")
    if change == "field_id":
        schema = schema.set(
            2, schema.field(2).with_metadata({b"PARQUET:field_id": b"9"})
        )
    else:
        schema = schema.set(3, schema.field(3).with_nullable(True))
    with pytest.raises(
        ValidationError, match=f"cannot write VARIANT columns: {message}"
    ):
        table.prepare_append_tables(
            [(_json_table(schema=schema), None)], idempotency_key=str(uuid.uuid4())
        )
    assert not store.files


def test_prepare_tables_still_compares_the_other_columns(prepared):
    """The prepared-file comparison runs over the non-variant columns of the
    written file: a scalar without its field id is refused as before."""
    table, store = prepared
    schema = columns_to_arrow_schema(_columns(), variant="json")
    schema = schema.set(1, schema.field(1).remove_metadata())
    with pytest.raises(ValidationError, match="schema/field IDs differ"):
        table.prepare_append_tables(
            [(_json_table(schema=schema), None)], idempotency_key=str(uuid.uuid4())
        )
    assert not store.files


@pytest.mark.parametrize(
    "names", [["id", "name", "raw", "props"], ["id", "name", "props"]]
)
def test_prepare_tables_wants_every_column_in_order(prepared, names):
    table, store = prepared
    data = _json_table().select(names)
    with pytest.raises(ValidationError, match="schema/field IDs differ"):
        table.prepare_append_tables([(data, None)], idempotency_key=str(uuid.uuid4()))
    assert not store.files


def test_prepare_tables_refuses_a_plain_string_variant(prepared):
    table, _ = prepared
    data = _json_table()
    values = pa.array(PROPS)
    data = data.set_column(2, data.schema.field(2).with_type(pa.string()), values)
    with pytest.raises(ValidationError, match=re.escape("cast it to pa.json_()")):
        table.prepare_append_tables([(data, None)], idempotency_key=str(uuid.uuid4()))


def test_prepare_tables_registrations_match_append(httpx_mock, prepared):
    """The same rows through both entry points register the same file but
    for its path: the same writer, the same footer."""
    table, prepared_store = prepared
    request = table.prepare_append_tables(
        [(_json_table(), None)], idempotency_key=str(uuid.uuid4())
    )
    (from_tables,) = request["appends"][0]["files"]
    store = FakeS3()
    appender = _append_table(httpx_mock, store)
    appender.append(_json_table(), read_snapshot=5)
    (from_append,) = _committed(httpx_mock)
    assert {k: v for k, v in from_tables.items() if k != "path"} == {
        k: v for k, v in from_append.items() if k != "path"
    }
    (appended,) = store.files.values()
    assert prepared_store.files[from_tables["path"].removeprefix("s3://")] == appended


# -- tables without a variant column: unchanged ------------------------------------


PLAIN_WIRE = {**WIRE, "columns": COLUMNS_WIRE[:2]}


def _plain_table():
    schema = columns_to_arrow_schema(_columns()[:2])
    return pa.table({"id": [3, 1, 2], "name": ["c", None, "b"]}, schema=schema)


@pytest.mark.parametrize("variants", [True, False])
def test_prepare_tables_raise_a_write_error_as_without_variants(httpx_mock, variants):
    """pyarrow's own error from the write, here SQL NULL in the NOT NULL id,
    is raised as on a table with no variant column: not hidden behind the
    failed writer's close, nor relabelled a VARIANT failure."""
    store = RecordingS3()
    table = resolved_table(httpx_mock, store, WIRE if variants else PLAIN_WIRE)
    data = _json_table() if variants else _plain_table()
    ids = pa.array([None, *range(1, data.num_rows)], pa.int64())
    data = data.set_column(0, data.schema.field(0), ids)
    with pytest.raises(
        pa.ArrowInvalid, match="'id' is declared non-nullable but contains nulls"
    ):
        table.prepare_append_tables([(data, None)], idempotency_key=str(uuid.uuid4()))
    assert not store.files
    table._namespace._catalog._client.close()


@pytest.mark.parametrize("variants", [True, False])
def test_append_raises_a_write_option_error_as_without_variants(
    httpx_mock, store, variants
):
    table = _append_table(
        httpx_mock, store, WIRE if variants else PLAIN_WIRE, commit=False
    )
    data = _json_table() if variants else _plain_table()
    with pytest.raises(ValueError, match="^Row group size cannot be 0$"):
        table.append(data, read_snapshot=5, row_group_size=0)
    assert not store.files


def _refuse_variant_writer(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("a table without variant columns reached the writer")

    monkeypatch.setattr(writer_module, "write_variant_parquet", refuse)
    monkeypatch.setattr(writer_module, "encode_json", refuse)


def test_append_without_variant_columns_is_byte_identical(
    httpx_mock, store, monkeypatch
):
    """The bytes of a table with no variant column are pq.write_table's, with
    the codec and row-group options, exactly as before VARIANT was wired."""
    _refuse_variant_writer(monkeypatch)
    table = _append_table(httpx_mock, store, PLAIN_WIRE)
    table.append(_plain_table(), read_snapshot=5, compression="zstd", row_group_size=2)
    (raw,) = store.files.values()
    sink = io.BytesIO()
    pq.write_table(_plain_table(), sink, compression="zstd", row_group_size=2)
    assert raw == sink.getvalue()


def test_prepare_tables_without_variant_columns_is_byte_identical(
    httpx_mock, monkeypatch
):
    _refuse_variant_writer(monkeypatch)
    store = RecordingS3()
    table = resolved_table(httpx_mock, store, PLAIN_WIRE)
    request = table.prepare_append_tables(
        [(_plain_table(), None)], idempotency_key=str(uuid.uuid4()), compression="zstd"
    )
    (reg,) = request["appends"][0]["files"]
    sink = pa.BufferOutputStream()
    pq.write_table(_plain_table(), sink, compression="zstd")
    assert (
        store.files[reg["path"].removeprefix("s3://")] == sink.getvalue().to_pybytes()
    )
