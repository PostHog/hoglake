"""The writer core, ``pyhoglake.writer``: one partition group's Arrow table
in, one ready-to-register Parquet file out.

The golden cases pin the bytes of a table with no variant column to what
``Table.append`` and ``prepare_append_tables`` wrote before they were
rebuilt on the core: ``data/writer_golden.json`` was captured from those
paths, and they and the core must keep writing it.
"""

from __future__ import annotations

import base64
import dataclasses
import json
import os
import uuid
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from test_append_unit import FakeS3
from test_transforms import _kotlin_coltypes
from test_upload import RecordingS3, resolved_table
from test_variant_write import (
    PARTITIONED,
    PROPS,
    PROPS_BACK,
    RAW,
    RAW_BACK,
    WIRE,
    _append_table,
    _check_file,
    _columns,
    _committed,
    _json_table,
    _values,
)

from pyhoglake import (
    PreparedPart,
    ValidationError,
    VariantReport,
    object_prefix,
    parquet_write_options,
    variant,
    write_part,
)
from pyhoglake import client as client_module
from pyhoglake import writer as writer_module
from pyhoglake.models import Column
from pyhoglake.stats import extract_column_stats
from pyhoglake.types import columns_to_arrow_schema

GOLDEN = Path(__file__).parent / "data" / "writer_golden.json"
# Set to rewrite the fixture from the current paths instead of comparing.
RECAPTURE = os.environ.get("PYHOGLAKE_RECAPTURE_WRITER_GOLDEN") == "1"


def _col(name, type_, field_id, ordinal, *, nullable=True, **extra):
    return {
        "name": name,
        "type": type_,
        "field_id": field_id,
        "ordinal": ordinal,
        "nullable": nullable,
        **extra,
    }


GOLDEN_COLUMNS = [
    _col("id", "long", 1, 0, nullable=False),
    _col("name", "string", 2, 1),
    _col("score", "double", 3, 2),
    _col("ok", "boolean", 4, 3),
    _col("day", "date", 5, 4),
    _col("at", "timestamp_s", 6, 5),
    _col("amount", "decimal", 7, 6, type_params={"precision": 10, "scale": 2}),
    _col("uid", "uuid", 8, 7),
    _col("tags", "list", 9, 8, children=[_col("element", "string", 10, 0)]),
    _col(
        "attrs",
        "map",
        11,
        9,
        children=[
            _col("key", "string", 12, 0, nullable=False),
            _col("value", "long", 13, 1),
        ],
    ),
    _col(
        "ctx",
        "struct",
        14,
        10,
        children=[_col("seen", "timestamptz", 15, 0), _col("n", "int", 16, 1)],
    ),
]
GOLDEN_WIRE = {**WIRE, "columns": GOLDEN_COLUMNS}


def _golden_columns() -> tuple[Column, ...]:
    return tuple(Column.from_wire(c) for c in GOLDEN_COLUMNS)


def _golden_rows() -> pa.Table:
    rows = [
        {
            "id": i,
            "name": None if i == 2 else f"n{i}",
            "score": i / 3,
            "ok": i % 2 == 0,
            "day": date(2026, 1, 1 + i),
            "at": datetime(2026, 1, 1, 0, 0, i),
            "amount": Decimal(f"{i}.25"),
            "uid": uuid.UUID(int=i).bytes,
            "tags": [f"t{i}", None],
            "attrs": [("k", i)],
            "ctx": {"seen": datetime(2026, 2, 1, i, tzinfo=UTC), "n": -i},
        }
        for i in range(5)
    ]
    return pa.Table.from_pylist(rows, schema=columns_to_arrow_schema(_golden_columns()))


# The pre-refactor paths' arguments, by case.
GOLDEN_CASES = {
    "append-zstd-row-groups": (
        "append",
        {"compression": "zstd", "compression_level": 3, "row_group_size": 2},
    ),
    "append-default-codec": ("append", {}),
    "prepare-zstd": ("prepare", {"compression": "zstd", "compression_level": 3}),
}


def _write_through(path: str, httpx_mock, kwargs) -> tuple[bytes, dict]:
    if path == "append":
        store = FakeS3()
        table = _append_table(httpx_mock, store, GOLDEN_WIRE)
        table.append(_golden_rows(), read_snapshot=5, **kwargs)
        (reg,) = _committed(httpx_mock)
        (body,) = store.files.values()
    else:
        recording = RecordingS3()
        table = resolved_table(httpx_mock, recording, GOLDEN_WIRE)
        request = table.prepare_append_tables(
            [(_golden_rows(), None)], idempotency_key=str(uuid.uuid4()), **kwargs
        )
        (reg,) = request["appends"][0]["files"]
        body = recording.files[reg["path"].removeprefix("s3://")]
    table._namespace._catalog._client.close()
    return body, reg


def _recorded(body: bytes, golden: dict) -> bytes:
    """``body`` with this pyarrow's ``created_by`` put back to the one the
    fixture was captured under, so a pyarrow upgrade that writes the same
    bytes is not a failure. A version string of another length moves the
    footer: recapture then."""
    now = pq.read_metadata(pa.BufferReader(body)).created_by
    then = golden["created_by"]
    assert len(now) == len(then), (
        f"pyarrow's created_by {now!r} is not {then!r}'s length: recapture "
        "with PYHOGLAKE_RECAPTURE_WRITER_GOLDEN=1, after checking that the "
        "writer did not change"
    )
    return body.replace(now.encode(), then.encode())


def _golden_entry(body: bytes, reg: dict) -> dict:
    return {
        "body": base64.b64encode(body).decode(),
        "record_count": reg["record_count"],
        "file_size_bytes": reg["file_size_bytes"],
        "footer_size": reg["footer_size"],
        "column_stats": reg["column_stats"],
    }


def _golden() -> dict:
    return json.loads(GOLDEN.read_text())


@pytest.mark.parametrize("case", sorted(GOLDEN_CASES))
def test_convenience_paths_write_the_golden_bytes(httpx_mock, case):
    path, kwargs = GOLDEN_CASES[case]
    body, reg = _write_through(path, httpx_mock, kwargs)
    if RECAPTURE:
        golden = _golden() if GOLDEN.exists() else {}
        golden["created_by"] = pq.read_metadata(pa.BufferReader(body)).created_by
        golden.setdefault("cases", {})[case] = _golden_entry(body, reg)
        GOLDEN.write_text(json.dumps(golden, indent=1, sort_keys=True) + "\n")
        return
    golden = _golden()
    want = golden["cases"][case]
    assert _golden_entry(_recorded(body, golden), reg) == want
    # The fixture is not one the test rebuilds: it holds every case, and
    # the cases are not trivially the same file.
    assert sorted(golden["cases"]) == sorted(GOLDEN_CASES)
    assert len({c["body"] for c in golden["cases"].values()}) == 3


@pytest.mark.parametrize("case", sorted(GOLDEN_CASES))
def test_write_part_writes_the_golden_bytes(case):
    """The core itself, called as an ingester calls it, writes what the
    pre-refactor paths wrote: the body, its sizes and the stats."""
    _, kwargs = GOLDEN_CASES[case]
    kwargs = dict(kwargs)
    options = parquet_write_options(
        kwargs.pop("compression", None), kwargs.pop("compression_level", None)
    )
    part = write_part(
        _golden_rows(), _golden_columns(), write_options=options, **kwargs
    )
    reg = part.registration("s3://b/f.parquet", _golden_columns(), stats=True)
    golden = _golden()
    assert (
        _golden_entry(_recorded(part.body.to_pybytes(), golden), reg)
        == golden["cases"][case]
    )


# -- registration -------------------------------------------------------------------


def test_registration_omits_column_stats_without_stats():
    """Omitted, not empty: `[]` would register the file 'provided' with no
    stats, forever, where an absent key leaves it to the hydrator."""
    part = write_part(_golden_rows(), _golden_columns(), write_options={})
    raw = part.body.to_pybytes()
    common = {
        "path": "s3://b/f.parquet",
        "record_count": 5,
        "file_size_bytes": len(raw),
        "footer_size": int.from_bytes(raw[-8:-4], "little"),
    }
    assert part.size == len(raw) and part.footer_size == common["footer_size"]
    without = part.registration("s3://b/f.parquet", _golden_columns(), stats=False)
    assert without == common
    with_stats = part.registration(
        "s3://b/f.parquet",
        _golden_columns(),
        stats=True,
        partition_values=("7", None),
    )
    assert list(with_stats) == [*common, "column_stats", "partition_values"]
    assert len(with_stats["column_stats"]) == 13
    assert with_stats["partition_values"] == ["7", None]


def test_registration_keeps_the_stats_of_the_field_ids_asked_for():
    """An object-prefix writer's small file, one row of id 4: its id stat
    alone, which proves the value from the footer. Any iterable of ids is
    read once, and `stats=False` leaves the key out, ids or no ids."""
    columns = _golden_columns()
    part = write_part(_golden_rows().slice(4, 1), columns, write_options={})

    def stats(ids, on=True):
        reg = part.registration("s3://b/f", columns, stats=on, stats_field_ids=ids)
        return reg.get("column_stats")

    full = stats(None)
    assert len(full) == 13
    (id_stat,) = [
        s for s in extract_column_stats(part.metadata, columns) if s.field_id == 1
    ]
    assert stats({1}) == [id_stat.to_wire()]
    assert object_prefix.check_field_stats(1, "long", 4, id_stat) == "OK"
    assert stats([16, 1]) == [s for s in full if s["field_id"] in (1, 16)]
    assert stats(iter([1])) == [id_stat.to_wire()]
    assert stats({1}, on=False) is None
    assert stats(frozenset(), on=False) is None


@pytest.mark.parametrize(
    "ids,match",
    [({1, 9, 99}, r"\[9, 99\] name no stats"), (frozenset(), "is empty")],
)
def test_registration_refuses_ids_that_leave_a_stat_out(ids, match):
    """A container's id, one the table does not have, or none at all: the
    file would register provided without the stat, and never get it."""
    part = write_part(_golden_rows(), _golden_columns(), write_options={})
    with pytest.raises(ValidationError, match=match):
        part.registration(
            "s3://b/f.parquet", _golden_columns(), stats=True, stats_field_ids=ids
        )


# -- timestamp_s at every depth ---------------------------------------------------

NESTED_TS_COLUMNS = [
    _col("at", "timestamp_s", 1, 0),
    _col("s", "struct", 2, 1, children=[_col("at", "timestamp_s", 3, 0)]),
    _col("l", "list", 4, 2, children=[_col("element", "timestamp_s", 5, 0)]),
    _col(
        "m",
        "map",
        6,
        3,
        children=[
            _col("key", "string", 7, 0, nullable=False),
            _col("value", "timestamp_s", 8, 1),
        ],
    ),
    _col(
        "mk",
        "map",
        9,
        4,
        children=[
            _col("key", "timestamp_s", 10, 0, nullable=False),
            _col("value", "long", 11, 1),
        ],
    ),
]


def _nested_ts_rows(unit: str) -> pa.Table:
    columns = tuple(Column.from_wire(c) for c in NESTED_TS_COLUMNS)
    at = datetime(2026, 3, 1, 12, 0, 7)
    rows = pa.Table.from_pylist(
        [{"at": at, "s": {"at": at}, "l": [at], "m": [("k", at)], "mk": [(at, 1)]}],
        schema=columns_to_arrow_schema(columns),
    )
    return rows if unit == "s" else rows.cast(writer_module._stored_schema(rows.schema))


@pytest.mark.parametrize("unit", ["s", "ms"])
def test_timestamp_s_is_milliseconds_at_every_depth(unit):
    """Seconds (the catalog's Arrow type) or milliseconds (an ingester's
    pre-cast table) at any depth: the file stores milliseconds, and the
    footer check expects them in a struct, a list, a map value and a map
    key too."""
    columns = tuple(Column.from_wire(c) for c in NESTED_TS_COLUMNS)
    part = write_part(_nested_ts_rows(unit), columns, write_options={})
    back = pq.read_table(pa.BufferReader(part.body))
    for name in ("at", "s", "l", "m", "mk"):
        assert "timestamp[ms]" in str(back.schema.field(name).type)
    assert back.column("s").to_pylist() == [{"at": datetime(2026, 3, 1, 12, 0, 7)}]


def test_prepare_tables_takes_a_nested_timestamp_s(httpx_mock):
    """The prepared path compared nested timestamp_s as seconds and refused
    every such table; it is the core's check now."""
    recording = RecordingS3()
    wire = {**WIRE, "columns": NESTED_TS_COLUMNS}
    table = resolved_table(httpx_mock, recording, wire)
    request = table.prepare_append_tables(
        [(_nested_ts_rows("s"), None)], idempotency_key=str(uuid.uuid4())
    )
    assert request["appends"][0]["files"][0]["record_count"] == 1
    table._namespace._catalog._client.close()


def test_prepared_file_takes_a_nested_timestamp_s(httpx_mock, tmp_path):
    """prepare_append_files shares the conversion: a file another writer
    stored with milliseconds below the top is the destination's."""
    path = tmp_path / "part.parquet"
    pq.write_table(_nested_ts_rows("s"), path)
    recording = RecordingS3()
    table = resolved_table(
        httpx_mock, recording, {**WIRE, "columns": NESTED_TS_COLUMNS}
    )
    request = table.prepare_append_files(
        [(str(path), None)], idempotency_key=str(uuid.uuid4())
    )
    assert request["appends"][0]["files"][0]["record_count"] == 1
    table._namespace._catalog._client.close()


# -- VARIANT input -------------------------------------------------------------------


def test_write_part_encodes_json_and_counts_it():
    report = VariantReport()
    part = write_part(
        _json_table(), _columns(), write_options={}, variant_report=report
    )
    reg = part.registration("s3://b/f.parquet", _columns(), stats=True)
    back = _check_file(part.body.to_pybytes(), reg, 4)
    assert _values(back, "props") == PROPS_BACK
    assert _values(back, "raw") == RAW_BACK
    assert report.rows == 8


def test_write_part_takes_the_null_type():
    data = _json_table()
    data = data.set_column(2, data.schema.field(2).with_type(pa.null()), pa.nulls(4))
    part = write_part(data, _columns(), write_options={})
    reg = part.registration("s3://b/f.parquet", _columns(), stats=True)
    assert _values(_check_file(part.body.to_pybytes(), reg, 4), "props") == [None] * 4


@pytest.mark.parametrize("select", ["take", "filter"])
def test_write_part_takes_storage_after_take_and_filter(select):
    """Storage encoded once for a batch, then cut as a sort and a partition
    split cut it, is written as its rows."""
    columns = _columns()
    storage = columns_to_arrow_schema(columns, variant="storage")
    batch = pa.table(
        [
            pa.array([0, 1, 2, 3], pa.int64()),
            pa.array(["a", "b", "c", "d"]),
            variant.encode_json(PROPS, columns[2]).array,
            variant.encode_json(RAW, columns[3]).array,
        ],
        schema=storage,
    )
    rows = [3, 0, 2]
    if select == "take":
        part_table = batch.take(rows)
    else:
        part_table = batch.filter(pa.array([True, False, True, True])).take([2, 0, 1])
    part = write_part(part_table, columns, write_options={})
    reg = part.registration("s3://b/f.parquet", columns, stats=True)
    back = _check_file(part.body.to_pybytes(), reg, 3)
    assert back.column("id").to_pylist() == rows
    assert _values(back, "props") == [PROPS_BACK[i] for i in rows]
    assert _values(back, "raw") == [RAW_BACK[i] for i in rows]


# -- refusals -----------------------------------------------------------------------


def _with_field(data: pa.Table, name: str, change) -> pa.Table:
    index = data.schema.get_field_index(name)
    schema = data.schema.set(index, change(data.schema.field(index)))
    return pa.Table.from_arrays(data.columns, schema=schema)


def _no_id(field: pa.Field) -> pa.Field:
    return field.remove_metadata()


def _nullable(field: pa.Field) -> pa.Field:
    return field.with_nullable(True)


@pytest.mark.parametrize(
    ("variants", "name", "change", "message"),
    [
        (False, "name", _no_id, "schema/field IDs differ"),
        (False, "id", _nullable, "schema/field IDs differ"),
        (True, "name", _no_id, "schema/field IDs differ"),
        (True, "id", _nullable, "schema/field IDs differ"),
        (True, "props", _no_id, "props carries no field id, not its column's 3"),
        (True, "raw", _nullable, "raw is nullable, and its column NOT NULL"),
    ],
)
def test_write_part_refuses_a_field_that_is_not_its_column(
    variants, name, change, message
):
    """Field ids and nullability: on the footer for every column, and before
    the write for a variant field."""
    if variants:
        data, columns = _json_table(), _columns()
    else:
        data, columns = _golden_rows(), _golden_columns()
    with pytest.raises(ValidationError, match=message):
        write_part(_with_field(data, name, change), columns, write_options={})


@pytest.mark.parametrize("variants", [False, True], ids=["plain", "variant"])
def test_write_part_refuses_another_column_order(variants):
    data, columns = (
        (_json_table(), _columns()) if variants else (_golden_rows(), _golden_columns())
    )
    names = data.schema.names
    with pytest.raises(ValidationError, match="schema/field IDs differ"):
        write_part(
            data.select([names[1], names[0], *names[2:]]), columns, write_options={}
        )


def test_write_part_refuses_a_plain_string_variant():
    data = _json_table()
    data = data.set_column(
        2, data.schema.field(2).with_type(pa.string()), pa.array(PROPS)
    )
    with pytest.raises(ValidationError, match="cast it to pa.json_"):
        write_part(data, _columns(), write_options={})


def test_write_part_refuses_on_invalid_null_for_a_not_null_variant():
    with pytest.raises(ValidationError, match=r"\['raw'\] are NOT NULL"):
        write_part(
            _json_table(), _columns(), write_options={}, variant_on_invalid="null"
        )


@pytest.mark.parametrize("variants", [False, True], ids=["plain", "variant"])
def test_write_part_refuses_an_unknown_on_invalid(variants):
    """write_part's own check: an ingester calls it without append's."""
    data, columns = (
        (_json_table(), _columns()) if variants else (_golden_rows(), _golden_columns())
    )
    with pytest.raises(ValidationError, match="is 'raise' or 'null', not 'mark'"):
        write_part(data, columns, write_options={}, variant_on_invalid="mark")


def test_write_part_compares_the_variant_fields_of_the_footer(monkeypatch):
    """The whole footer is the destination's, its variant fields too: a
    variant writer that put props under another field id, consistently
    enough to pass its own layout check, is refused."""
    original = writer_module.write_variant_parquet

    def renumbering(data, columns, **kwargs):
        props = data.schema.get_field_index("props")
        field = data.schema.field(props).with_metadata({b"PARQUET:field_id": b"99"})
        data = pa.Table.from_arrays(data.columns, schema=data.schema.set(props, field))
        columns = [
            dataclasses.replace(c, field_id=99) if c.name == "props" else c
            for c in columns
        ]
        return original(data, columns, **kwargs)

    monkeypatch.setattr(writer_module, "write_variant_parquet", renumbering)
    with pytest.raises(ValidationError, match="schema/field IDs differ"):
        write_part(_json_table(), _columns(), write_options={})


# -- Table.append's footer check ----------------------------------------------------


def test_append_checks_the_footer_it_wrote(httpx_mock, monkeypatch):
    """append gained the core's footer check; after _align_table it always
    passes (every type at every depth, with a variant column and without,
    runs through it below), so a
    failing comparison here is a writer bug, refused before the upload."""
    monkeypatch.setattr(writer_module, "prepared_schema_matches", lambda *_: False)
    store = FakeS3()
    table = _append_table(httpx_mock, store, GOLDEN_WIRE, commit=False)
    with pytest.raises(ValidationError, match="schema/field IDs differ"):
        table.append(_golden_rows(), read_snapshot=5)
    assert not store.files
    table._namespace._catalog._client.close()


def test_append_takes_stats_before_the_upload(httpx_mock, monkeypatch):
    """As before the core: a file whose stats fail leaves no object."""

    def failing(*_args):
        raise RuntimeError("stats failed")

    monkeypatch.setattr(writer_module, "extract_column_stats", failing)
    store = FakeS3()
    table = _append_table(httpx_mock, store, GOLDEN_WIRE, commit=False)
    with pytest.raises(RuntimeError, match="stats failed"):
        table.append(_golden_rows(), read_snapshot=5)
    assert not store.files
    table._namespace._catalog._client.close()


def test_append_without_variants_writes_each_file_as_it_uploads(
    httpx_mock, monkeypatch
):
    """Without a variant column a partitioned append holds one file at a
    time, as before the core: a failed first upload has written one file,
    not every group's."""
    written = []

    def counting(*args, **kwargs):
        written.append(1)
        return writer_module.write_part(*args, **kwargs)

    monkeypatch.setattr(client_module, "write_part", counting)
    store = FakeS3()
    store.fail_at = 0
    wire = {**GOLDEN_WIRE, "partition_spec": PARTITIONED["partition_spec"]}
    table = _append_table(httpx_mock, store, wire, commit=False)
    with pytest.raises(OSError, match="object store refused"):
        table.append(_golden_rows(), read_snapshot=5)
    assert written == [1]
    table._namespace._catalog._client.close()


# One value of every scalar column type, as Table.append takes it.
_AT = datetime(2026, 1, 2, 3, 4, 5, 6000)
SCALAR_VALUES = {
    "boolean": True,
    "int8": -8,
    "int16": -16,
    "int": -32,
    "long": -64,
    "uint8": 255,
    "uint16": 65535,
    "uint32": 2**32 - 1,
    "uint64": 2**64 - 1,
    "float": 1.5,
    "double": 2.5,
    "string": "s",
    "json": '{"k": [1]}',
    "binary": b"\x00b",
    "date": date(2026, 1, 2),
    "time": _AT.time(),
    "timestamp_s": _AT.replace(microsecond=0),
    "timestamp_ms": _AT,
    "timestamp": _AT,
    "timestamp_ns": _AT,
    "timestamptz": _AT.replace(tzinfo=UTC),
    "uuid": uuid.UUID(int=7).bytes,
    "decimal": Decimal("-12345678901234567890123456789012.345678"),
}


def test_every_scalar_type_has_a_value():
    """Read from the server's ColType: a new scalar type is appended below
    as soon as it exists."""
    containers = {"variant", "list", "struct", "map"}
    assert set(SCALAR_VALUES) == _kotlin_coltypes() - containers


def _every_type_wire(shape: str, with_variant: bool) -> list[dict]:
    ids = iter(range(1, 1000))

    def scalar(name, type_, ordinal):
        params = {"precision": 38, "scale": 6} if type_ == "decimal" else None
        extra = {"type_params": params} if params else {}
        return _col(name, type_, next(ids), ordinal, **extra)

    def column(type_, ordinal):
        if shape == "top":
            return scalar(f"c_{type_}", type_, ordinal)
        if shape == "list":
            children = [scalar("element", type_, 0)]
        else:
            key = _col("key", "string", next(ids), 0, nullable=False)
            children = [key, scalar("value", type_, 1)]
        return _col(f"c_{type_}", shape, next(ids), ordinal, children=children)

    if shape == "struct":
        children = [scalar(t, t, i) for i, t in enumerate(SCALAR_VALUES)]
        wire = [_col("c", "struct", next(ids), 0, children=children)]
    else:
        wire = [column(t, i) for i, t in enumerate(SCALAR_VALUES)]
    if with_variant:
        wire.append(_col("v", "variant", next(ids), len(wire)))
    return wire


def _every_type_row(shape: str, with_variant: bool) -> dict:
    shaped = {
        "top": lambda value: value,
        "list": lambda value: [value, None],
        "map": lambda value: [("k", value)],
    }
    if shape == "struct":
        row = {"c": SCALAR_VALUES}
    else:
        row = {f"c_{t}": shaped[shape](v) for t, v in SCALAR_VALUES.items()}
    return {**row, "v": '{"a": 1}'} if with_variant else row


def _storage(kind: pa.DataType) -> pa.DataType:
    if isinstance(kind, pa.BaseExtensionType):
        return kind.storage_type
    if pa.types.is_struct(kind):
        return pa.struct([f.with_type(_storage(f.type)) for f in kind])
    if pa.types.is_map(kind):
        return pa.map_(
            kind.key_field, kind.item_field.with_type(_storage(kind.item_type))
        )
    if pa.types.is_list(kind):
        return pa.list_(kind.value_field.with_type(_storage(kind.value_type)))
    return kind


@pytest.mark.parametrize("with_variant", [False, True], ids=["plain", "variant"])
@pytest.mark.parametrize("shape", ["top", "struct", "list", "map"])
def test_append_takes_every_type_beside_a_variant_or_not(
    httpx_mock, shape, with_variant
):
    """Table.append's footer check passes for every catalog type at every
    depth. Beside a variant column the file is written without its
    ARROW:schema, so json and uuid read back from their Parquet logical
    types alone, and the whole footer is compared."""
    wire = _every_type_wire(shape, with_variant)
    columns = tuple(Column.from_wire(c) for c in wire)
    schema = columns_to_arrow_schema(columns, variant="json")
    # pyarrow builds no nested extension value from Python: built over the
    # storage types, then cast.
    data = pa.Table.from_pylist(
        [_every_type_row(shape, with_variant), {}],
        schema=pa.schema([f.with_type(_storage(f.type)) for f in schema]),
    ).cast(schema)
    store = FakeS3()
    table = _append_table(httpx_mock, store, {**WIRE, "columns": wire})
    table.append(data, read_snapshot=5)
    (reg,) = _committed(httpx_mock)
    (body,) = store.files.values()
    assert reg["record_count"] == 2
    back = pq.read_table(pa.BufferReader(body))
    plain = [c.name for c in columns if c.type != "variant"]
    assert back.select(plain).to_pylist() == data.select(plain).to_pylist()
    table._namespace._catalog._client.close()


def test_append_takes_a_nested_timestamp_s(httpx_mock):
    store = FakeS3()
    table = _append_table(httpx_mock, store, {**WIRE, "columns": NESTED_TS_COLUMNS})
    table.append(_nested_ts_rows("s"), read_snapshot=5)
    (body,) = store.files.values()
    assert pq.read_table(pa.BufferReader(body)).num_rows == 1
    table._namespace._catalog._client.close()


def test_write_part_returns_a_prepared_part():
    part = write_part(_golden_rows(), _golden_columns(), write_options={})
    assert isinstance(part, PreparedPart)
    assert isinstance(part.body, pa.Buffer)
    assert part.metadata.num_rows == 5
