"""Nested column types on the client: list, struct and map.

Three surfaces, one shape rule each:

* :mod:`pyhoglake.types` builds the Arrow schema, with the catalog's
  field id on EVERY level (containers included) and Iceberg's synthetic
  child names (``element``; ``key``/``value``) regardless of what the
  local Arrow field happened to be called.
* :mod:`pyhoglake.stats` extracts bounds per LEAF, through the nesting.
  A container never gets a stats row — it has no values — while a list's
  element and a map's key/value do, which is Iceberg's own rule.
* :mod:`pyhoglake.transforms` extracts a partition source through struct
  levels, and refuses a container or anything under a list or a map.

Bounds are asserted BYTE for byte, not "is not None": the codec is
unchanged by this phase, and the whole claim is that a leaf under a
container encodes exactly as the same scalar would at top level.
"""

import io
import struct

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyhoglake import UnsupportedTypeError
from pyhoglake.client import _align_table, _field_id_chains, _partition_groups
from pyhoglake.errors import ValidationError
from pyhoglake.models import Column, PartitionField, PartitionSpec, TableInfo
from pyhoglake.stats import extract_column_stats
from pyhoglake.transforms import partition_source_array
from pyhoglake.types import (
    MAX_COLUMN_NESTING_DEPTH,
    PARQUET_FIELD_ID_KEY,
    coltype_to_arrow,
    columns_to_arrow_schema,
    schema_to_column_defs,
)


def _col(
    name, type_, field_id, ordinal=0, *, nullable=True, children=None, params=None
):
    return Column(
        name=name,
        type=type_,
        field_id=field_id,
        ordinal=ordinal,
        nullable=nullable,
        type_params=params,
        children=children,
    )


#: list<int>, map<string,long>, struct{a:int, b:string} and a three-level
#: combination, with the field ids a server would have assigned
#: depth-first.
NESTED_COLUMNS = (
    _col("l", "list", 1, 0, children=(_col("element", "int", 2),)),
    _col(
        "m",
        "map",
        3,
        1,
        children=(
            _col("key", "string", 4, 0, nullable=False),
            _col("value", "long", 5, 1),
        ),
    ),
    _col(
        "s",
        "struct",
        6,
        2,
        children=(_col("a", "int", 7, 0), _col("b", "string", 8, 1)),
    ),
    _col(
        "deep",
        "struct",
        9,
        3,
        children=(
            _col(
                "inner",
                "list",
                10,
                0,
                children=(
                    _col(
                        "element",
                        "struct",
                        11,
                        0,
                        children=(_col("x", "long", 12, 0),),
                    ),
                ),
            ),
        ),
    ),
)


def _nested_table():
    schema = columns_to_arrow_schema(NESTED_COLUMNS)
    return pa.table(
        {
            "l": [[1, 2], [3], None, []],
            "m": [[("a", 10)], [("b", -1)], [], [("c", 7)]],
            "s": [
                {"a": 5, "b": "x"},
                {"a": -2, "b": "y"},
                {"a": 0, "b": "z"},
                {"a": 9, "b": "w"},
            ],
            "deep": [
                {"inner": [{"x": 100}, {"x": -100}]},
                {"inner": []},
                {"inner": [{"x": 7}]},
                {"inner": None},
            ],
        },
        schema=schema,
    )


def _footer(table: pa.Table) -> pq.FileMetaData:
    sink = io.BytesIO()
    pq.write_table(table, sink)
    return pq.read_metadata(io.BytesIO(sink.getvalue()))


# -- schema construction -----------------------------------------------------


def test_field_ids_land_on_every_nesting_level_in_the_parquet_footer():
    """Every container AND every leaf carries its catalog field id.

    Asserted through a real parquet round trip, not on the Arrow schema:
    arrow metadata is only a request, and what binds a column to the
    catalog is the field id parquet actually stored. The synthetic
    repetition groups (``list``, ``key_value``) deliberately carry none —
    Iceberg has nothing to match one against.
    """
    footer = _footer(_nested_table())
    arrow = footer.schema.to_arrow_schema()

    def ids(field, out):
        meta = field.metadata or {}
        if PARQUET_FIELD_ID_KEY in meta:
            out[field.name] = int(meta[PARQUET_FIELD_ID_KEY])
        t = field.type
        if pa.types.is_struct(t):
            for i in range(t.num_fields):
                ids(t.field(i), out)
        elif pa.types.is_map(t):
            ids(t.key_field, out)
            ids(t.item_field, out)
        elif pa.types.is_list(t):
            ids(t.value_field, out)

    found: dict[str, int] = {}
    for f in arrow:
        ids(f, found)
    assert found == {
        "l": 1,
        "element": 11,  # the DEEP element wins the name collision, see below
        "m": 3,
        "key": 4,
        "value": 5,
        "s": 6,
        "a": 7,
        "b": 8,
        "deep": 9,
        "inner": 10,
        "x": 12,
    }
    # Two different `element` fields exist (field 2 and field 11); the
    # flat dict above keeps one. Assert the shallow one separately so the
    # collision cannot hide a missing id.
    assert int(arrow.field("l").type.value_field.metadata[PARQUET_FIELD_ID_KEY]) == 2

    # The parquet leaf paths are the ones stats.py predicts.
    rg = footer.row_group(0)
    paths = {rg.column(j).path_in_schema for j in range(footer.num_columns)}
    assert paths == {
        "l.list.element",
        "m.key_value.key",
        "m.key_value.value",
        "s.a",
        "s.b",
        "deep.inner.list.element.x",
    }


def test_map_key_is_written_required():
    """Iceberg map keys are non-nullable and the server refuses a
    nullable one; arrow enforces it too, so this pins the agreement."""
    schema = columns_to_arrow_schema(NESTED_COLUMNS)
    assert schema.field("m").type.key_field.nullable is False


def test_schema_to_column_defs_uses_icebergs_synthetic_child_names():
    """Whatever the local Arrow field is called, the DDL says `element`,
    `key` and `value` — otherwise the schema a client declares would
    depend on which library built its arrays."""
    schema = pa.schema(
        [
            # arrow's default list value field is called "item"
            pa.field("l", pa.list_(pa.int32())),
            pa.field("m", pa.map_(pa.string(), pa.int64())),
            pa.field("s", pa.struct([("a", pa.int32())])),
        ]
    )
    defs = schema_to_column_defs(schema)
    by_name = {d["name"]: d for d in defs}
    assert [c["name"] for c in by_name["l"]["children"]] == ["element"]
    assert by_name["l"]["children"][0]["type"] == "int"
    assert [c["name"] for c in by_name["m"]["children"]] == ["key", "value"]
    assert by_name["m"]["children"][0]["nullable"] is False
    # struct children keep the user's names
    assert [c["name"] for c in by_name["s"]["children"]] == ["a"]


def test_list_element_nullability_is_declarable():
    """Iceberg's element-required is a real distinction; a required
    element must not silently become optional in the DDL."""
    schema = pa.schema(
        [pa.field("l", pa.list_(pa.field("item", pa.int32(), nullable=False)))]
    )
    (col,) = schema_to_column_defs(schema)
    assert col["children"][0]["nullable"] is False


def test_depth_cap_is_mirrored_from_the_server():
    """The cap exists so a pathological schema fails BEFORE the upload.
    Checked at the boundary in both directions: the deepest legal schema
    is accepted and one level more is refused, naming the cap."""

    def nest(depth):
        t: pa.DataType = pa.int32()
        for _ in range(depth - 1):
            t = pa.struct([("c", t)])
        return pa.schema([pa.field("x", t)])

    assert schema_to_column_defs(nest(MAX_COLUMN_NESTING_DEPTH))
    with pytest.raises(UnsupportedTypeError) as ei:
        schema_to_column_defs(nest(MAX_COLUMN_NESTING_DEPTH + 1))
    assert str(MAX_COLUMN_NESTING_DEPTH) in str(ei.value)
    assert "nesting depth" in str(ei.value)


def test_coltype_to_arrow_refuses_containers_by_name():
    """The scalar-only signature cannot know the children, so it says so
    instead of inventing an element type."""
    for t in ("list", "struct", "map"):
        with pytest.raises(UnsupportedTypeError) as ei:
            coltype_to_arrow(t)
        assert "nested container" in str(ei.value)
        assert "columns_to_arrow_schema" in str(ei.value)


@pytest.mark.parametrize(
    ("column", "fragment"),
    [
        (_col("l", "list", 1, children=()), "exactly 1 child"),
        (
            _col("l", "list", 1, children=(_col("a", "int", 2), _col("b", "int", 3))),
            "exactly 1 child",
        ),
        (_col("m", "map", 1, children=(_col("key", "string", 2),)), "exactly 2 child"),
        (_col("s", "struct", 1, children=()), "at least one child"),
    ],
)
def test_malformed_containers_are_refused_by_name(column, fragment):
    with pytest.raises(UnsupportedTypeError) as ei:
        columns_to_arrow_schema((column,))
    assert fragment in str(ei.value)


# -- stats -------------------------------------------------------------------


def test_leaf_bounds_are_extracted_through_every_nesting_level():
    footer = _footer(_nested_table())
    stats = {s.field_id: s for s in extract_column_stats(footer, NESTED_COLUMNS)}

    # Containers carry no stats row at all: nothing to count, nothing to
    # bound, and the server refuses a row addressed to one.
    for container in (1, 3, 6, 9, 10, 11):
        assert container not in stats, f"field {container} is a container"

    # Every LEAF does, bounds byte-for-byte identical to the same scalar
    # at top level.
    assert stats[2].lower_bound == struct.pack("<i", 1)  # list<int> element
    assert stats[2].upper_bound == struct.pack("<i", 3)
    assert stats[4].lower_bound == b"a"  # map key (string)
    assert stats[4].upper_bound == b"c"
    assert stats[5].lower_bound == struct.pack("<q", -1)  # map value (long)
    assert stats[5].upper_bound == struct.pack("<q", 10)
    assert stats[7].lower_bound == struct.pack("<i", -2)  # struct leaf (int)
    assert stats[7].upper_bound == struct.pack("<i", 9)
    assert stats[8].lower_bound == b"w"  # struct leaf (string)
    assert stats[8].upper_bound == b"z"
    assert stats[12].lower_bound == struct.pack("<q", -100)  # struct>list>struct>long
    assert stats[12].upper_bound == struct.pack("<q", 100)

    assert set(stats) == {2, 4, 5, 7, 8, 12}


def test_value_counts_under_a_list_count_values_not_rows():
    """Iceberg's value_counts is the number of VALUES, and a row under a
    list contributes many (or none). The Kotlin hydrator sums the same
    parquet num_values, and the two must not disagree the moment a
    repeated leaf appears."""
    columns = (_col("l", "list", 1, children=(_col("element", "int", 2),)),)
    table = pa.table(
        {"l": [[1, 2, 3], [4], None, []]}, schema=columns_to_arrow_schema(columns)
    )
    (s,) = extract_column_stats(_footer(table), columns)
    assert s.field_id == 2
    # 4 real values + one definition-level slot each for the null list
    # and the empty one.
    assert s.value_count == 6
    assert s.null_count == 2


def test_a_shape_mismatch_leaves_the_leaf_without_bounds():
    """The catalog says struct, the file says int32. The per-arm rule is
    unchanged one level down: no honest bound, so none at all — never a
    bound read off the wrong physical column."""
    columns = (_col("s", "struct", 1, children=(_col("a", "int", 2),)),)
    flat = pa.table({"s": pa.array([1, 2, 3], pa.int32())})
    assert extract_column_stats(_footer(flat), columns) == []


def test_a_struct_field_the_file_predates_simply_has_no_stats():
    """An ALTER added `b` after this file was written. `a` still bounds;
    `b` is absent, which is not an error."""
    written = (_col("s", "struct", 1, children=(_col("a", "int", 2),)),)
    table = pa.table(
        {"s": [{"a": 4}, {"a": 8}]}, schema=columns_to_arrow_schema(written)
    )
    live = (
        _col(
            "s",
            "struct",
            1,
            children=(_col("a", "int", 2), _col("b", "string", 3, ordinal=1)),
        ),
    )
    stats = {s.field_id: s for s in extract_column_stats(_footer(table), live)}
    assert set(stats) == {2}
    assert stats[2].lower_bound == struct.pack("<i", 4)


def test_two_structs_with_a_same_named_leaf_do_not_pool_their_stats():
    """Leaves are matched on the FULL chunk path. A name-only match would
    have summed `a.id` and `b.id` into one impossible row."""
    columns = (
        _col("a", "struct", 1, 0, children=(_col("id", "long", 2),)),
        _col("b", "struct", 3, 1, children=(_col("id", "long", 4),)),
    )
    table = pa.table(
        {"a": [{"id": 1}, {"id": 2}], "b": [{"id": 100}, {"id": 200}]},
        schema=columns_to_arrow_schema(columns),
    )
    stats = {s.field_id: s for s in extract_column_stats(_footer(table), columns)}
    assert stats[2].upper_bound == struct.pack("<q", 2)
    assert stats[4].lower_bound == struct.pack("<q", 100)


# -- partition sources -------------------------------------------------------


def test_struct_leaf_is_a_legal_partition_source():
    """Iceberg's source-id may point at a struct's leaf field, so the
    client must be able to reach one."""
    table = _nested_table()
    chains = _field_id_chains(NESTED_COLUMNS)
    values = partition_source_array(table, chains[7]).to_pylist()
    assert values == [5, -2, 0, 9]


@pytest.mark.parametrize(
    ("field_id", "fragment"),
    [
        (1, "no single value per row"),  # the list itself
        (6, "no single value per row"),  # the struct itself
        (2, "a row has many such values"),  # list element
        (4, "a row has many such values"),  # map key
        (5, "a row has many such values"),  # map value
        (12, "a row has many such values"),  # a leaf under a list, two levels down
    ],
)
def test_container_and_repeated_sources_are_refused_by_name(field_id, fragment):
    table = _nested_table()
    chains = _field_id_chains(NESTED_COLUMNS)
    with pytest.raises(ValidationError) as ei:
        partition_source_array(table, chains[field_id])
    assert fragment in str(ei.value)


def test_partition_fanout_over_a_struct_leaf():
    """The whole append path, minus the upload: a partitioned table whose
    spec points into a struct must group by that leaf's values."""
    columns = (
        _col("s", "struct", 1, 0, children=(_col("region", "string", 2),)),
        _col("v", "long", 3, 1),
    )
    info = TableInfo(
        name="t",
        namespace="ns",
        table_uuid="00000000-0000-0000-0000-000000000000",
        columns=columns,
        record_count=0,
        file_count=0,
        file_size_bytes=0,
        partition_spec=PartitionSpec(
            spec_id=1,
            fields=(PartitionField(source_field_id=2, transform="identity"),),
        ),
    )
    table = pa.table(
        {
            "s": [{"region": "eu"}, {"region": "us"}, {"region": "eu"}],
            "v": [1, 2, 3],
        },
        schema=columns_to_arrow_schema(columns),
    )
    groups = _partition_groups(table, info, info.partition_spec)
    assert [(values, part.num_rows) for values, part in groups] == [
        (("eu",), 2),
        (("us",), 1),
    ]
    assert groups[0][1].column("v").to_pylist() == [1, 3]


def test_align_table_carries_nested_field_ids_onto_a_foreign_shaped_batch():
    """`append` casts the caller's batch to the catalog's schema. The
    cast must carry the field-id metadata down EVERY level: without it a
    struct field or a list element would land in parquet with no id, and
    the file would bind by name — the exact hazard the rename guard
    exists for."""
    columns = (
        _col("s", "struct", 1, 0, children=(_col("a", "int", 2),)),
        _col("l", "list", 3, 1, children=(_col("element", "long", 4),)),
        _col(
            "m",
            "map",
            5,
            2,
            children=(
                _col("key", "string", 6, 0, nullable=False),
                _col("value", "int", 7, 1),
            ),
        ),
    )
    target = columns_to_arrow_schema(columns)
    # Caller order and inner widths differ from the catalog's on purpose.
    raw = pa.table(
        {
            "l": pa.array([[1, 2]], pa.list_(pa.int64())),
            "m": pa.array([[("a", 1)]], pa.map_(pa.string(), pa.int32())),
            "s": pa.array([{"a": 3}], pa.struct([("a", pa.int32())])),
        }
    )
    out = _align_table(raw, target)
    footer = _footer(out)
    arrow = footer.schema.to_arrow_schema()
    assert int(arrow.field("s").type.field(0).metadata[PARQUET_FIELD_ID_KEY]) == 2
    assert int(arrow.field("l").type.value_field.metadata[PARQUET_FIELD_ID_KEY]) == 4
    assert int(arrow.field("m").type.key_field.metadata[PARQUET_FIELD_ID_KEY]) == 6
    assert int(arrow.field("m").type.item_field.metadata[PARQUET_FIELD_ID_KEY]) == 7
    # And the stats still resolve, which is the end-to-end proof that the
    # ids and the leaf paths agree after the cast.
    assert {s.field_id for s in extract_column_stats(footer, columns)} == {2, 4, 6, 7}


def test_arrow_itself_refuses_a_nullable_map_key():
    """The premise schema_to_column_defs leans on.

    It emits the key's own ``nullable`` verbatim rather than forcing it,
    because arrow will not build a map with a nullable key and Iceberg
    would not accept one either. If arrow ever relaxes that, this test
    fails and the emitted DDL becomes a server-side 422 instead of a
    silent wrong claim — which is the point of pinning a premise.
    """
    with pytest.raises(TypeError, match="non-nullable"):
        pa.map_(
            pa.field("key", pa.string(), nullable=True), pa.field("value", pa.int64())
        )
    # ...and the honest consequence: the def always says False.
    (col,) = schema_to_column_defs(
        pa.schema([pa.field("m", pa.map_(pa.string(), pa.int64()))])
    )
    assert col["children"][0]["nullable"] is False
