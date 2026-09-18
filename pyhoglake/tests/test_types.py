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
    ("uuid", None, pa.binary(16)),
]


@pytest.mark.parametrize("coltype,params,arrow_type", REVERSE)
def test_coltype_to_arrow(coltype, params, arrow_type):
    assert coltype_to_arrow(coltype, params) == arrow_type


def test_uuid_extension_type_maps_if_available():
    if not hasattr(pa, "uuid"):
        pytest.skip("pyarrow without pa.uuid()")
    assert arrow_type_to_coltype(pa.uuid()) == ("uuid", None)


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
