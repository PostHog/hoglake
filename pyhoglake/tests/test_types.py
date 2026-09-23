"""Arrow <-> hoglake ColType mapping, both directions, plus rejection."""

import pyarrow as pa
import pytest

from pyhoglake import UnsupportedTypeError, arrow_type_to_coltype, coltype_to_arrow
from pyhoglake.models import Column
from pyhoglake.types import (
    PARQUET_FIELD_ID_KEY,
    columns_to_arrow_schema,
    schema_to_column_defs,
)

FORWARD = [
    (pa.bool_(), "boolean", None),
    (pa.int8(), "int8", None),
    (pa.int16(), "int16", None),
    (pa.int32(), "int", None),
    (pa.int64(), "long", None),
    (pa.uint8(), "uint8", None),
    (pa.uint16(), "uint16", None),
    (pa.uint32(), "uint32", None),
    (pa.uint64(), "uint64", None),
    (pa.float32(), "float", None),
    (pa.float64(), "double", None),
    (pa.string(), "string", None),
    (pa.large_string(), "string", None),
    (pa.binary(), "binary", None),
    (pa.large_binary(), "binary", None),
    (pa.date32(), "date", None),
    (pa.time64("us"), "time", None),
    (pa.timestamp("s"), "timestamp_s", None),
    (pa.timestamp("ms"), "timestamp_ms", None),
    (pa.timestamp("us"), "timestamp", None),
    (pa.timestamp("ns"), "timestamp_ns", None),
    (pa.timestamp("us", tz="UTC"), "timestamptz", None),
    (pa.timestamp("us", tz="America/New_York"), "timestamptz", None),
    (pa.decimal128(10, 2), "decimal", {"precision": 10, "scale": 2}),
    (pa.binary(16), "uuid", None),
]


@pytest.mark.parametrize("arrow_type,coltype,params", FORWARD)
def test_arrow_to_coltype(arrow_type, coltype, params):
    assert arrow_type_to_coltype(arrow_type) == (coltype, params)


REVERSE = [
    ("boolean", None, pa.bool_()),
    ("int8", None, pa.int8()),
    ("int16", None, pa.int16()),
    ("int", None, pa.int32()),
    ("long", None, pa.int64()),
    ("uint8", None, pa.uint8()),
    ("uint16", None, pa.uint16()),
    # uint32 -> pa.int64(), NOT pa.uint32(): see the types.py docstring.
    ("uint32", None, pa.int64()),
    ("uint64", None, pa.uint64()),
    ("float", None, pa.float32()),
    ("double", None, pa.float64()),
    ("string", None, pa.string()),
    ("binary", None, pa.binary()),
    ("date", None, pa.date32()),
    ("time", None, pa.time64("us")),
    ("timestamp_s", None, pa.timestamp("s")),
    ("timestamp_ms", None, pa.timestamp("ms")),
    ("timestamp", None, pa.timestamp("us")),
    ("timestamp_ns", None, pa.timestamp("ns")),
    ("timestamptz", None, pa.timestamp("us", tz="UTC")),
    ("decimal", {"precision": 10, "scale": 2}, pa.decimal128(10, 2)),
    # pa.uuid(), NOT pa.binary(16): only the extension type makes pyarrow
    # stamp the parquet UUID annotation hoglake's wire form requires.
    ("uuid", None, pa.uuid()),
]


@pytest.mark.parametrize("coltype,params,arrow_type", REVERSE)
def test_coltype_to_arrow(coltype, params, arrow_type):
    assert coltype_to_arrow(coltype, params) == arrow_type


def test_uuid_extension_type_maps_if_available():
    if not hasattr(pa, "uuid"):
        pytest.skip("pyarrow without pa.uuid()")
    assert arrow_type_to_coltype(pa.uuid()) == ("uuid", None)


def test_uuid_writer_contract_is_the_annotating_extension_type():
    """hoglake's uuid wire form is FIXED_LEN_BYTE_ARRAY(16) + the UUID
    logical annotation — documented in docs/iceberg-federation.md and
    written by compaction (server ParquetRewriter.kt, the `uuidType()`
    arm) — and pyarrow stamps that annotation ONLY for pa.uuid().
    pa.binary(16)
    writes the bytes with no annotation, which an Iceberg-conformant
    reader sees as fixed binary until compaction rewrites the file."""
    assert coltype_to_arrow("uuid") == pa.uuid()
    # The bytes are the same 16 big-endian bytes either way: the
    # extension only adds the annotation.
    assert coltype_to_arrow("uuid").storage_type == pa.binary(16)
    # Both forms stay readable — files written before this contract, and
    # by writers that cannot produce the extension, still map to uuid.
    assert arrow_type_to_coltype(pa.binary(16)) == ("uuid", None)


def test_uuid_column_writes_the_parquet_uuid_annotation(tmp_path):
    """The point of the extension type, at the parquet level."""
    import uuid as _uuid

    import pyarrow.parquet as pq

    schema = columns_to_arrow_schema(
        [Column(name="u", type="uuid", field_id=7, ordinal=0, nullable=False)]
    )
    path = tmp_path / "uuid.parquet"
    pq.write_table(
        pa.table(
            {"u": pa.array([_uuid.uuid4().bytes], pa.binary(16))}, schema=None
        ).cast(schema),
        path,
    )
    parquet = pq.ParquetFile(path)
    column = parquet.schema.column(0)
    assert column.physical_type == "FIXED_LEN_BYTE_ARRAY"
    assert column.logical_type.type == "UUID"
    # The field id rides along unchanged.
    assert parquet.schema_arrow.field(0).metadata == {PARQUET_FIELD_ID_KEY: b"7"}


def test_uuid_falls_back_to_fixed_binary_without_the_extension(monkeypatch):
    """Below the pyarrow floor there is no pa.uuid(): the column still
    writes its 16 bytes, it just loses the annotation — the state every
    file written before this contract is already in."""
    monkeypatch.delattr(pa, "uuid", raising=False)
    assert coltype_to_arrow("uuid") == pa.binary(16)


def test_uuid_column_round_trips_through_column_defs():
    """columns_to_arrow_schema -> schema_to_column_defs is how a caller
    copies a table's shape; the extension type must survive it as uuid
    rather than come back as an unsupported type."""
    columns = [Column(name="u", type="uuid", field_id=3, ordinal=0, nullable=False)]
    schema = columns_to_arrow_schema(columns)
    assert schema_to_column_defs(schema) == [
        {"name": "u", "type": "uuid", "nullable": False}
    ]


def test_json_extension_type_maps_if_available():
    if not hasattr(pa, "json_"):
        pytest.skip("pyarrow without pa.json_()")
    assert arrow_type_to_coltype(pa.json_()) == ("json", None)
    assert coltype_to_arrow("json") == pa.json_()


def test_json_falls_back_to_string_without_the_extension(monkeypatch):
    """pyarrow < 19 has no pa.json_(): the column still writes, it just
    loses the parquet JSON annotation — never the bytes."""
    monkeypatch.delattr(pa, "json_", raising=False)
    assert coltype_to_arrow("json") == pa.string()


@pytest.mark.parametrize("t", [pa.string(), pa.large_string()], ids=str)
def test_json_is_never_inferred_from_a_plain_string(t):
    """Arrow's plain string makes no JSON validity claim, so inferring
    json from it would invent one. A json column is declared, not
    guessed."""
    assert arrow_type_to_coltype(t) == ("string", None)


def test_uint32_writer_contract_is_int64():
    """pyarrow writes pa.uint32() as parquet INT32 + Int(32, unsigned),
    which an Iceberg reader reads as a SIGNED int32 — everything above
    2^31 would come back negative. uint32 maps to Iceberg long, so the
    writer emits INT64."""
    assert coltype_to_arrow("uint32") == pa.int64()
    # the read direction still recognises a genuine uint32 column
    assert arrow_type_to_coltype(pa.uint32()) == ("uint32", None)
    # and the cast the caller pays for is lossless across the boundary
    values = [0, 2**31, 2**32 - 1]
    assert pa.array(values, pa.uint32()).cast(pa.int64()).to_pylist() == values


REJECTED = [
    pa.float16(),
    pa.date64(),
    pa.time32("s"),
    pa.time64("ns"),
    # tz-aware timestamps are micros-only: timestamptz has no s/ms/ns twin
    pa.timestamp("s", tz="UTC"),
    pa.timestamp("ms", tz="UTC"),
    pa.timestamp("ns", tz="UTC"),
    pa.binary(8),  # fixed-size but not 16
    pa.duration("us"),
    # Containers are supported since phase 2 — but only over supported
    # inner types. A container carrying an unsupported leaf is still
    # rejected, at any depth.
    pa.list_(pa.duration("us")),
    pa.struct([("a", pa.float16())]),
    pa.map_(pa.string(), pa.date64()),
    pa.list_(pa.struct([("a", pa.list_(pa.time32("s")))])),
    # An empty struct has no parquet or Iceberg representation.
    pa.struct([]),
]


@pytest.mark.parametrize("arrow_type", REJECTED, ids=str)
def test_rejects_unsupported(arrow_type):
    with pytest.raises(UnsupportedTypeError) as ei:
        arrow_type_to_coltype(arrow_type)
    assert "supported types" in str(ei.value)


def test_unknown_coltype_rejected():
    with pytest.raises(UnsupportedTypeError):
        coltype_to_arrow("varchar", None)


def test_schema_to_column_defs():
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string()),
            pa.field("amount", pa.decimal128(18, 4)),
        ]
    )
    assert schema_to_column_defs(schema) == [
        {"name": "id", "type": "long", "nullable": False},
        {"name": "name", "type": "string", "nullable": True},
        {
            "name": "amount",
            "type": "decimal",
            "nullable": True,
            "type_params": {"precision": 18, "scale": 4},
        },
    ]


def test_columns_to_arrow_schema_embeds_field_ids_and_orders_by_ordinal():
    cols = (
        Column(name="b", type="string", field_id=12, ordinal=1),
        Column(name="a", type="long", field_id=11, ordinal=0, nullable=False),
    )
    schema = columns_to_arrow_schema(cols)
    assert schema.names == ["a", "b"]
    assert schema.field("a").type == pa.int64()
    assert not schema.field("a").nullable
    assert schema.field("a").metadata[PARQUET_FIELD_ID_KEY] == b"11"
    assert schema.field("b").metadata[PARQUET_FIELD_ID_KEY] == b"12"
