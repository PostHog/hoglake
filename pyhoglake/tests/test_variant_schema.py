import copy
import json
import re
import sys
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import footer_oracle
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from thrift.Thrift import TType

from pyhoglake import variant
from pyhoglake._variant_codec import compile_plan
from pyhoglake.errors import UnsupportedShreddingError, ValidationError
from pyhoglake.models import Column
from pyhoglake.parquet_schema import (
    _footer_bytes,
    _leaf_levels,
    _schema_elements_from_footer,
    _top_level,
    prepared_schema_matches,
    stamp_variant_footer,
    validate_column_chunks,
    validate_variant_file,
    variant_layout_fault,
    variant_stamps,
    write_variant_parquet,
)
from pyhoglake.stats import extract_column_stats
from pyhoglake.types import arrow_type_to_coltype, columns_to_arrow_schema

sys.path.insert(0, str(Path(__file__).parent / "variant_conformance"))
from variant_corpus import DECLARATIONS as CORPUS

FIXTURE = Path(__file__).parent / "data" / "native_variant.parquet"
COLUMNS = (
    Column("id", "long", 1, 0, False),
    Column("properties", "variant", 2, 1, False),
)


def test_native_fixture_is_accepted_with_required_catalog_columns():
    with pq.ParquetFile(FIXTURE) as p:
        validate_variant_file(str(FIXTURE), p, COLUMNS)


@pytest.mark.parametrize("defect", ["annotation", "id", "scalar_type", "nullable"])
def test_invalid_variant_prepared_file_is_rejected(tmp_path, defect):
    table = pq.read_table(FIXTURE)
    columns = COLUMNS
    if defect == "id":
        columns = (COLUMNS[0], replace(COLUMNS[1], field_id=3))
        path = FIXTURE
    elif defect == "scalar_type":
        columns = (replace(COLUMNS[0], type="int"), COLUMNS[1])
        path = FIXTURE
    elif defect == "nullable":
        path = FIXTURE.with_name("native_variant_null_id.parquet")
    else:
        path = tmp_path / "struct.parquet"
        pq.write_table(table, path)
    with pq.ParquetFile(path) as p, pytest.raises(ValidationError):
        validate_variant_file(str(path), p, columns)


# -- uuid columns on the variant/optional-fields path -------------------------
#
# validate_variant_file is the OTHER prepared-file check: it runs for a
# table holding any variant column, and for every caller that passes
# allow_optional_fields (hedgerow's DuckDB writer does). It compares
# types itself rather than through prepared_schema_matches, so the uuid
# equivalence has to be stated here too — in BOTH directions, since the
# catalog's own uuid type is now the annotated extension.

UUID_FIELD_ID = {b"PARQUET:field_id": b"2"}
ID_FIELD_ID = {b"PARQUET:field_id": b"1"}
UUID_COLUMNS = (
    Column("id", "long", 1, 0, False),
    Column("event_id", "uuid", 2, 1, True),
)
STRUCT_UUID_COLUMNS = (
    Column("id", "long", 1, 0, False),
    Column("s", "struct", 2, 1, True, children=(Column("u", "uuid", 3, 0, True),)),
)


def _write(tmp_path, name, schema, row, storage=None):
    """Write one row. ``storage`` is the same schema with plain
    fixed(16) leaves — arrow cannot build an extension value from
    python inside a struct, so the row is built on the storage schema
    and cast. Both schemas are hand-built by the helpers below; nothing
    here goes through the normalizer under test."""
    path = tmp_path / name
    table = pa.Table.from_pylist([row], schema=storage if storage else schema)
    pq.write_table(table.cast(schema), path)
    return path


def _uuid_schema(leaf: pa.DataType) -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False, metadata=ID_FIELD_ID),
            pa.field("event_id", leaf, metadata=UUID_FIELD_ID),
        ]
    )


def _struct_uuid_schema(leaf: pa.DataType) -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False, metadata=ID_FIELD_ID),
            pa.field(
                "s",
                pa.struct([pa.field("u", leaf, metadata={b"PARQUET:field_id": b"3"})]),
                metadata=UUID_FIELD_ID,
            ),
        ]
    )


@pytest.mark.parametrize("leaf", [pa.uuid(), pa.binary(16)], ids=["annotated", "bare"])
def test_uuid_column_validates_in_both_spellings(tmp_path, leaf):
    """The scalar arm. An annotated file is what this client and
    compaction now write; a bare one is what every file registered before
    the contract carries, and what a writer on a pyarrow below 21 still
    produces. Both are the same 16 bytes."""
    path = _write(
        tmp_path,
        f"uuid-{leaf}.parquet",
        _uuid_schema(leaf),
        {"id": 1, "event_id": b"\x01" * 16},
        storage=_uuid_schema(pa.binary(16)),
    )
    with pq.ParquetFile(path) as p:
        validate_variant_file(str(path), p, UUID_COLUMNS)


@pytest.mark.parametrize("leaf", [pa.binary(15), pa.string()], ids=["narrow", "string"])
def test_a_uuid_column_of_the_wrong_type_is_still_rejected(tmp_path, leaf):
    """The equivalence is between two spellings of sixteen bytes, not a
    licence for the neighbouring types."""
    value = b"\x01" * 15 if leaf == pa.binary(15) else "not-sixteen-bytes"
    path = _write(
        tmp_path,
        f"bad-uuid-{leaf}.parquet",
        _uuid_schema(leaf),
        {"id": 1, "event_id": value},
    )
    with (
        pq.ParquetFile(path) as p,
        pytest.raises(ValidationError, match="type differs"),
    ):
        validate_variant_file(str(path), p, UUID_COLUMNS)


@pytest.mark.parametrize("leaf", [pa.uuid(), pa.binary(16)], ids=["annotated", "bare"])
def test_uuid_under_a_container_validates_in_both_spellings(tmp_path, leaf):
    """The container arm, which compares against column_to_arrow_field's
    whole type in one go — so the normalization has to reach into it."""
    path = _write(
        tmp_path,
        f"struct-uuid-{leaf}.parquet",
        _struct_uuid_schema(leaf),
        {"id": 1, "s": {"u": b"\x02" * 16}},
        storage=_struct_uuid_schema(pa.binary(16)),
    )
    with pq.ParquetFile(path) as p:
        validate_variant_file(str(path), p, STRUCT_UUID_COLUMNS)


def test_a_uuid_under_a_container_of_the_wrong_type_is_rejected(tmp_path):
    path = _write(
        tmp_path,
        "struct-bad-uuid.parquet",
        _struct_uuid_schema(pa.binary(15)),
        {"id": 1, "s": {"u": b"\x02" * 15}},
    )
    with (
        pq.ParquetFile(path) as p,
        pytest.raises(ValidationError, match="type differs"),
    ):
        validate_variant_file(str(path), p, STRUCT_UUID_COLUMNS)


MIXED = FIXTURE.with_name("native_variant_struct.parquet")
NESTED = Column("s", "struct", 3, 2, False, children=(Column("x", "int", 4, 0, True),))
MIXED_COLUMNS = (*COLUMNS, NESTED)


def test_a_container_column_beside_a_variant_validates():
    """A table may hold both a variant and a container.

    ``validate_variant_file`` runs over the whole file whenever ANY column
    is a variant, and whenever the caller opts into optional fields, so it
    has to describe every catalog type -- not only the ones
    ``coltype_to_arrow`` can name on its own. A container is compared
    against ``column_to_arrow_field``, the same shape the writer builds.
    Before the merge this raised ``UnsupportedTypeError`` -- a developer
    message about the wrong API, on a path a caller can reach.
    """
    with pq.ParquetFile(MIXED) as p:
        validate_variant_file(str(MIXED), p, MIXED_COLUMNS)


@pytest.mark.parametrize(
    "defect", ["child_type", "child_id", "child_name", "child_nullable", "arity"]
)
def test_a_container_whose_shape_differs_is_rejected(defect):
    child = NESTED.children[0]
    if defect == "child_type":
        children = (replace(child, type="long"),)
    elif defect == "child_id":
        children = (replace(child, field_id=9),)
    elif defect == "child_name":
        children = (replace(child, name="y"),)
    elif defect == "child_nullable":
        children = (replace(child, nullable=False),)
    else:
        children = (child, replace(child, name="y", field_id=9, ordinal=1))
    columns = (*COLUMNS, replace(NESTED, children=children))
    with pq.ParquetFile(MIXED) as p, pytest.raises(ValidationError):
        validate_variant_file(str(MIXED), p, columns)


def test_a_required_container_cannot_be_proven_by_null_leaves(tmp_path):
    """DuckDB writes everything optional, so a required catalog container
    is accepted only when SOME leaf beneath it has a zero null count.

    That direction is the sound one: a leaf value at full definition level
    proves every ancestor of it is present. The converse does not hold --
    a null leaf says nothing about its parent -- so a container whose
    leaves are all null is refused rather than guessed at.

    This file has no variant, which is the other door into the same
    function: ``allow_optional_fields=True`` on a purely nested table.
    """
    columns = (Column("s", "struct", 3, 0, False, children=NESTED.children),)
    schema = columns_to_arrow_schema(columns)
    optional = pa.schema([schema.field(0).with_nullable(True)])
    path = tmp_path / "null-leaf.parquet"
    pq.write_table(
        pa.table({"s": pa.array([{"x": None}], optional.field(0).type)}, optional), path
    )
    with pq.ParquetFile(path) as p, pytest.raises(ValidationError):
        validate_variant_file(str(path), p, columns)

    present = tmp_path / "live-leaf.parquet"
    pq.write_table(
        pa.table({"s": pa.array([{"x": 7}], optional.field(0).type)}, optional), present
    )
    with pq.ParquetFile(present) as p:
        validate_variant_file(str(present), p, columns)


def test_nested_variant_stats_do_not_alias_dotted_scalar_name(tmp_path):
    """A variant's payload chunk and a scalar spelled the same way must
    not be confused -- and when they cannot be told apart, NEITHER is
    bound.

    #77 separated them by physical leaf POSITION, which is exact, and
    gave every nested top-level column an empty name so none of its
    leaves could bind. That was right while variant was the only nested
    column type -- the blanked columns were all variants. Phase 2 makes
    struct/list/map leaves the ones that MUST bind, so the merge keeps
    path binding and narrows the exclusion to variant storage instead.

    So variant storage is excluded by path PREFIX, which is
    order-independent, and a catalog column whose own name collides with
    that prefix gets no stats rather than a guessed one. The same rule
    the Kotlin side applies to duplicate names: a binding with two
    candidates is not a binding.

    The lost case is unreachable through the server anyway -- a column
    named ``properties.value`` cannot be created, because the identifier
    pattern forbids ``.`` -- so this costs a column that cannot exist and
    buys correctness for files hoglake did not write.
    """
    table = pq.read_table(FIXTURE).append_column(
        "properties.value", pa.array([b"scalar"])
    )
    path = tmp_path / "collision.parquet"
    pq.write_table(table, path)
    columns = (*COLUMNS, Column("properties.value", "binary", 3, 2, True))
    stats = extract_column_stats(pq.read_metadata(path), columns)
    assert [s.field_id for s in stats] == [1]

    # Without the collision the same scalar binds normally, so the
    # exclusion is scoped to the ambiguity and not to the name.
    plain = pq.read_table(FIXTURE).append_column("scalar_col", pa.array([b"scalar"]))
    plain_path = tmp_path / "no-collision.parquet"
    pq.write_table(plain, plain_path)
    plain_stats = extract_column_stats(
        pq.read_metadata(plain_path),
        (*COLUMNS, Column("scalar_col", "binary", 3, 2, True)),
    )
    assert [s.field_id for s in plain_stats] == [1, 3]
    assert plain_stats[1].lower_bound == b"scalar"


# == Suite L: VARIANT layouts ====================================================
#
# variant_layout_fault holds a VARIANT group to a layout: exactly the
# declaration's for pyhoglake's own files (mode="exact"), or one the spec
# allows and the Trino connector reads for anyone's (mode="spec", which
# validate_variant_file(strict=True) and prepare_append_files(
# strict_variant=True) apply). Each mutation below is refused by both modes;
# the ones only a declaration can catch are refused by spec mode only when
# the column has one.

DATA = FIXTURE.parent
LAYOUT = {
    "type": "object",
    "fields": [
        {"name": "ts", "type": "timestamp"},
        {"name": "tz", "type": "timestamptz"},
        {"name": "t", "type": "time"},
        {"name": "n", "type": "int32"},
        {"name": "d4", "type": "decimal4", "precision": 9, "scale": 2},
        {"name": "tags", "type": "array", "element": {"type": "string"}},
        {"name": "any", "type": "variant"},
        {"name": "u", "type": "uuid"},
    ],
}


def _variant_column(decl, *, name="v", field_id=1, nullable=True):
    params = None if decl is None else {"shredding": decl}
    return Column(name, "variant", field_id, 0, nullable, type_params=params)


def _plan_of(column):
    return compile_plan((column.type_params or {}).get("shredding"))


def _written(column, rows):
    """pyhoglake's own file of ``rows``, and its schema elements."""
    storage = variant.encode_python(rows, column).array
    field = pa.field(
        column.name,
        storage.type,
        nullable=column.nullable,
        metadata={b"PARQUET:field_id": str(column.field_id).encode()},
    )
    table = pa.table([storage], schema=pa.schema([field]))
    written = write_variant_parquet(table, [column], {column.name: _plan_of(column)})
    return written.body, _elements_of(written.body)


def _elements_of(raw):
    return _schema_elements_from_footer(_footer_bytes(raw)[1])


def _start(elements, name):
    (index,) = [i for e, _, i in _top_level(elements) if e.get(4) == name]
    return index


def _element_paths(elements):
    """Each element's path, as the positions reader computes it."""
    out, stack = [], []
    for index, element in enumerate(elements):
        path = ()
        if index:
            while not stack[-1][0]:
                stack.pop()
            stack[-1][0] -= 1
            path = (*stack[-1][1], element[4])
        if element.get(5):
            stack.append([element[5], path])
        out.append(path)
    return out


def _at(elements, *path):
    return elements[_element_paths(elements).index(path)]


def _remove(elements, path):
    """Remove the subtree at ``path`` and count one child fewer above it."""
    paths = _element_paths(elements)
    index = paths.index(path)
    end = index + 1
    while end < len(paths) and paths[end][: len(path)] == path:
        end += 1
    del elements[index:end]
    _at(elements, *path[:-1])[5] -= 1


LAYOUT_ROWS = [
    {"ts": "x", "n": 1, "tags": ["a", None], "any": {"k": 1}},
    {"d4": Decimal("1.25"), "tz": None},
    None,
]


def _layout_elements():
    column = _variant_column(LAYOUT)
    _, elements = _written(column, LAYOUT_ROWS)
    return column, elements


TV = ("v", "typed_value")


def _optional_field_group(e):
    _at(e, *TV, "ts")[3] = 1


def _optional_element(e):
    _at(e, *TV, "tags", "typed_value", "list", "element")[3] = 1


def _timestamp_millis(e):
    leaf = _at(e, *TV, "ts", "typed_value")
    leaf[10] = {8: {1: False, 2: {1: {}}}}
    leaf[6] = 9


def _unsigned(e):
    leaf = _at(e, *TV, "n", "typed_value")
    leaf[10] = {10: {1: 32, 2: False}}
    leaf[6] = 13


def _time_utc(e):
    _at(e, *TV, "t", "typed_value")[10] = {7: {1: True, 2: {2: {}}}}


def _timestamptz_for_timestamp(e):
    _at(e, *TV, "ts", "typed_value")[10] = {8: {1: True, 2: {2: {}}}}


def _flba_decimal4(e):
    leaf = _at(e, *TV, "d4", "typed_value")
    leaf[1] = 7
    leaf[2] = 4


def _decimal_precision(e):
    leaf = _at(e, *TV, "d4", "typed_value")
    leaf[8] = 8
    leaf[10] = {5: {1: 2, 2: 8}}


def _required_typed_value(e):
    _at(e, *TV, "n", "typed_value")[3] = 0


def _case_collision(e):
    _at(e, *TV, "n")[4] = "Ts"


def _list_not_repeated(e):
    _at(e, *TV, "tags", "typed_value", "list")[3] = 1


def _list_two_elements(e):
    paths = _element_paths(e)
    element = (*TV, "tags", "typed_value", "list", "element")
    start = paths.index(element)
    end = start + 1
    while end < len(paths) and paths[end][: len(element)] == element:
        end += 1
    _at(e, *TV, "tags", "typed_value", "list")[5] = 2
    e[end:end] = copy.deepcopy(e[start:end])


def _int32_decimal_too_wide(e):
    leaf = _at(e, *TV, "d4", "typed_value")
    leaf[8] = 10
    leaf[10] = {5: {1: 2, 2: 10}}


def _uuid_length(e):
    _at(e, *TV, "u", "typed_value")[2] = 15


def _no_metadata(e):
    _remove(e, ("v", "metadata"))


def _metadata_not_binary(e):
    _at(e, "v", "metadata")[1] = 1


def _required_value(e):
    _at(e, "v", "value")[3] = 0


def _typed_group_with_a_type(e):
    _at(e, *TV)[1] = 6


def _annotated_object(e):
    _at(e, *TV)[10] = {2: {}}


def _empty_object(e):
    for field in ("ts", "tz", "t", "n", "d4", "tags", "any", "u"):
        _remove(e, (*TV, field))


def _field_group_not_a_group(e):
    _remove(e, (*TV, "any", "value"))
    del _at(e, *TV, "any")[5]
    _at(e, *TV, "any")[1] = 6


def _field_group_with_a_type(e):
    _at(e, *TV, "n")[1] = 6


def _list_named_array(e):
    _at(e, *TV, "tags", "typed_value", "list")[4] = "array"


def _list_annotated_repeated(e):
    _at(e, *TV, "tags", "typed_value", "list")[10] = {3: {}}


def _list_primitive_element(e):
    element = (*TV, "tags", "typed_value", "list", "element")
    _remove(e, (*element, "typed_value"))
    _remove(e, (*element, "value"))
    leaf = _at(e, *element)
    del leaf[5]
    leaf[1] = 6


def _extra_child(e):
    index = _element_paths(e).index(("v", "typed_value"))
    _at(e, "v")[5] += 1
    e[index:index] = [{1: 6, 3: 1, 4: "extra"}]


def _missing_value(e):
    _remove(e, (*TV, "n", "value"))


def _child_field_id(e):
    _at(e, *TV, "n", "typed_value")[9] = 1


def _missing_field(e):
    _remove(e, (*TV, "n"))


def _extra_field(e):
    index = _element_paths(e).index((*TV, "any"))
    _at(e, *TV)[5] += 1
    e[index:index] = [{3: 0, 4: "extra", 5: 1}, {1: 6, 3: 1, 4: "value"}]


def _renamed_field(e):
    _at(e, *TV, "n")[4] = "m"


def _other_leaf_type(e):
    _at(e, *TV, "n", "typed_value")[1] = 2


def _two_level_list(e):
    paths = _element_paths(e)
    element = _at(e, *TV, "tags", "typed_value", "list", "element")
    element[3] = 2
    del e[paths.index((*TV, "tags", "typed_value", "list"))]


def _insert_before(e, path, element, parent):
    """Insert ``element`` as a child of ``parent``, just before ``path``."""
    index = _element_paths(e).index(path)
    _at(e, *parent)[5] += 1
    e[index:index] = [element]


def _metadata_in_field_group(e):
    _insert_before(e, (*TV, "n", "value"), {1: 6, 3: 0, 4: "metadata"}, (*TV, "n"))


def _metadata_in_element(e):
    element = (*TV, "tags", "typed_value", "list", "element")
    _insert_before(e, (*element, "value"), {1: 6, 3: 0, 4: "metadata"}, element)


def _two_values_in_field_group(e):
    _insert_before(e, (*TV, "n", "value"), {1: 6, 3: 1, 4: "value"}, (*TV, "n"))


def _duplicate_field(e):
    _at(e, *TV, "ts")[4] = "n"


def _empty_element(e):
    element = (*TV, "tags", "typed_value", "list", "element")
    _remove(e, (*element, "typed_value"))
    _remove(e, (*element, "value"))


def _list_extra_child(e):
    # After the repeated group, which a check of the first child alone sees.
    after = _element_paths(e).index((*TV, "any"))
    _at(e, *TV, "tags", "typed_value")[5] += 1
    e[after:after] = [{1: 6, 3: 1, 4: "extra"}]


def _list_repeated_primitive(e):
    # A repeated field with a physical type is a primitive to parquet-java,
    # whatever children it counts.
    _at(e, *TV, "tags", "typed_value", "list")[1] = 6


def _list_tuple_name(e):
    # parquet-java reads a repeated group named <list>_tuple (or array) as
    # the element of a 2-level list, whatever it holds.
    _at(e, *TV, "tags", "typed_value", "list")[4] = "typed_value_tuple"


def _list_named_array_in_capitals(e):
    # The connector lowercases every name it reads off a footer before it
    # looks for array and <list>_tuple.
    _at(e, *TV, "tags", "typed_value", "list")[4] = "Array"


def _list_tuple_name_in_capitals(e):
    _at(e, *TV, "tags", "typed_value", "list")[4] = "TYPED_VALUE_tuple"


def _decimal_zero_precision(e):
    leaf = _at(e, *TV, "d4", "typed_value")
    leaf.update({7: 0, 8: 0, 10: {5: {1: 0, 2: 0}}})


def _decimal_scale_above_precision(e):
    leaf = _at(e, *TV, "d4", "typed_value")
    leaf.update({7: 5, 8: 4, 10: {5: {1: 5, 2: 4}}})


def _decimal_fields_disagree(e):
    # The legacy precision beside DECIMAL(9, 2): parquet-java's builder
    # throws on the pair, so the file is unreadable.
    _at(e, *TV, "d4", "typed_value")[8] = 8


def _decimal_scale_field_disagrees(e):
    _at(e, *TV, "d4", "typed_value")[7] = 1


def _flba_decimal_too_narrow(e):
    leaf = _at(e, *TV, "d4", "typed_value")
    leaf[1] = 7
    leaf[2] = 3  # 6 digits at most, not 9


def _logical_signed_converted_unsigned(e):
    # The connector keeps the converted type when the two disagree.
    leaf = _at(e, *TV, "n", "typed_value")
    leaf.update({6: 13, 10: {10: {1: 32, 2: True}}})


def _logical_string_converted_enum(e):
    _at(e, *TV, "tags", "typed_value", "list", "element", "typed_value")[6] = 4


def _other_element_type(e):
    leaf = _at(e, *TV, "tags", "typed_value", "list", "element", "typed_value")
    leaf[1] = 2
    del leaf[6], leaf[10]


def _metadata_field_id(e):
    _at(e, "v", "metadata")[9] = 2


def _with_a_child(path):
    """The leaf at ``path`` counting one child, the element after it: a
    primitive to the connector (it has a physical type), which reads that
    element as the leaf's sibling, and a group to pyarrow."""

    def mutate(e):
        index = _element_paths(e).index(path)
        e[index][5] = 1
        e[index + 1 : index + 1] = [{1: 6, 3: 1, 4: "x"}]

    return mutate


def _list_converted_map_key_value(e):
    # parquet-java reads MAP_KEY_VALUE alone as its own annotation, and the
    # connector walks only a LIST-annotated group as an array.
    group = _at(e, *TV, "tags", "typed_value")
    del group[10]
    group[6] = 2


#: Each mutation, and whether a spec-mode check refuses it with no
#: declaration to compare: a layout the spec or the connector refuses
#: (True), or one only the declaration rules out (False).
MUTATIONS = {
    "optional_field_group": (_optional_field_group, True),
    "optional_element": (_optional_element, True),
    "timestamp_millis": (_timestamp_millis, True),
    "unsigned_int": (_unsigned, True),
    "time_utc": (_time_utc, True),
    "timestamptz_for_timestamp": (_timestamptz_for_timestamp, False),
    "flba_decimal4": (_flba_decimal4, False),
    "decimal_precision": (_decimal_precision, False),
    "int32_decimal_too_wide": (_int32_decimal_too_wide, True),
    "uuid_length": (_uuid_length, True),
    "required_typed_value": (_required_typed_value, True),
    "case_collision": (_case_collision, True),
    "list_not_repeated": (_list_not_repeated, True),
    "list_two_elements": (_list_two_elements, True),
    "no_metadata": (_no_metadata, True),
    "metadata_not_binary": (_metadata_not_binary, True),
    "required_value": (_required_value, True),
    "typed_group_with_a_type": (_typed_group_with_a_type, True),
    "annotated_object": (_annotated_object, True),
    "empty_object": (_empty_object, True),
    "field_group_not_a_group": (_field_group_not_a_group, True),
    "field_group_with_a_type": (_field_group_with_a_type, True),
    "list_named_array": (_list_named_array, True),
    "list_annotated_repeated": (_list_annotated_repeated, True),
    "list_primitive_element": (_list_primitive_element, True),
    "extra_child": (_extra_child, True),
    "missing_value_in_field_group": (_missing_value, True),
    "child_field_id": (_child_field_id, True),
    "missing_field": (_missing_field, False),
    "extra_field": (_extra_field, False),
    "renamed_field": (_renamed_field, False),
    "other_leaf_type": (_other_leaf_type, False),
    "two_level_list": (_two_level_list, True),
    "metadata_in_field_group": (_metadata_in_field_group, True),
    "metadata_in_element": (_metadata_in_element, True),
    "two_values_in_field_group": (_two_values_in_field_group, True),
    "duplicate_field": (_duplicate_field, True),
    "empty_element": (_empty_element, True),
    "list_extra_child": (_list_extra_child, True),
    "list_tuple_name": (_list_tuple_name, True),
    "list_repeated_primitive": (_list_repeated_primitive, True),
    "list_named_array_in_capitals": (_list_named_array_in_capitals, True),
    "list_tuple_name_in_capitals": (_list_tuple_name_in_capitals, True),
    "decimal_zero_precision": (_decimal_zero_precision, True),
    "decimal_scale_above_precision": (_decimal_scale_above_precision, True),
    "decimal_fields_disagree": (_decimal_fields_disagree, True),
    "decimal_scale_field_disagrees": (_decimal_scale_field_disagrees, True),
    "flba_decimal_too_narrow": (_flba_decimal_too_narrow, True),
    "logical_signed_converted_unsigned": (_logical_signed_converted_unsigned, True),
    "logical_string_converted_enum": (_logical_string_converted_enum, True),
    "other_element_type": (_other_element_type, False),
    "metadata_field_id": (_metadata_field_id, True),
    "metadata_with_a_child": (_with_a_child(("v", "metadata")), True),
    "value_with_a_child": (_with_a_child((*TV, "n", "value")), True),
    "typed_leaf_with_a_child": (_with_a_child((*TV, "n", "typed_value")), True),
    "list_converted_map_key_value": (_list_converted_map_key_value, True),
}


#: What spec mode, with the declaration, says of each mutation: the rule
#: that refused it, so a refusal that comes from somewhere else (a KeyError
#: the catch-all turns into a fault, say) does not pass for it.
SPEC_FAULTS = {
    "optional_field_group": "v.typed_value.ts is not REQUIRED",
    "optional_element": "list.element is not REQUIRED",
    "timestamp_millis": "annotated {8: {1: False, 2: {1: {}}}}, which no Variant",
    "unsigned_int": "annotated {10: {1: 32, 2: False}}, which no Variant",
    "time_utc": "annotated {7: {1: True, 2: {2: {}}}}, which no Variant",
    "timestamptz_for_timestamp": "v.ts is timestamptz, not the declared timestamp",
    "flba_decimal4": "v.d4 is decimal16(9, 2), not the declared decimal4(9, 2)",
    "decimal_precision": "v.d4 is decimal4(8, 2), not the declared decimal4(9, 2)",
    "int32_decimal_too_wide": "annotated {5: {1: 2, 2: 10}}, which no Variant",
    "uuid_length": "physical type 7 annotated {14: {}}, which no Variant",
    "required_typed_value": "v.typed_value.n.typed_value is not OPTIONAL",
    "case_collision": "v.typed_value.Ts differs from another field only by case",
    "list_not_repeated": "tags.typed_value is not a 3-level list",
    "list_two_elements": "tags.typed_value is not a 3-level list",
    "no_metadata": "v has no metadata",
    "metadata_not_binary": "v.metadata is not a REQUIRED binary column",
    "required_value": "v.value is not an OPTIONAL binary column",
    "typed_group_with_a_type": "v.typed_value is a group with a physical type",
    "annotated_object": "v.typed_value is a group annotated {2: {}}",
    "empty_object": "v.typed_value has no fields",
    "field_group_not_a_group": "v.typed_value.any is not a group",
    "field_group_with_a_type": "v.typed_value.n is not a group",
    "list_named_array": "tags.typed_value is not a 3-level list",
    "list_annotated_repeated": "tags.typed_value is not a 3-level list",
    "list_primitive_element": "list.element is not a group",
    "extra_child": "v has children ['extra', 'metadata', 'typed_value', 'value']",
    "missing_value_in_field_group": "v.typed_value.n has no value",
    "child_field_id": "'typed_value' below the group has a field id",
    "missing_field": "v has no field 'n', which is declared",
    "extra_field": "v has a field 'extra', which is not declared",
    "renamed_field": "v has no field 'n', which is declared",
    "other_leaf_type": "v.n is int64, not the declared int32",
    "two_level_list": "tags.typed_value is not a 3-level list",
    "metadata_in_field_group": "v.typed_value.n has children ['metadata',",
    "metadata_in_element": "list.element has children ['metadata',",
    "two_values_in_field_group": "v.typed_value.n has two children named 'value'",
    "duplicate_field": "v.typed_value has two children named 'n'",
    "empty_element": "list.element has no value",
    "list_extra_child": "tags.typed_value is not a 3-level list",
    "list_tuple_name": "tags.typed_value is not a 3-level list",
    "list_repeated_primitive": "tags.typed_value is not a 3-level list",
    "list_named_array_in_capitals": "tags.typed_value is not a 3-level list",
    "list_tuple_name_in_capitals": "tags.typed_value is not a 3-level list",
    "decimal_zero_precision": "annotated {5: {1: 0, 2: 0}}, which no Variant",
    "decimal_scale_above_precision": "annotated {5: {1: 5, 2: 4}}, which no Variant",
    "decimal_fields_disagree": "type 1 annotated {5: {1: 2, 2: 9}}, which no Variant",
    "decimal_scale_field_disagrees": "type 1 annotated {5: {1: 2, 2: 9}}, which no",
    "flba_decimal_too_narrow": "type 7 annotated {5: {1: 2, 2: 9}}, which no Variant",
    "logical_signed_converted_unsigned": "annotated {10: {1: 32, 2: False}}, which",
    "logical_string_converted_enum": "type 6 annotated {4: {}}, which no Variant",
    "other_element_type": "v.tags.element is int64, not the declared string",
    "metadata_field_id": "'metadata' below the group has a field id",
    "metadata_with_a_child": "v.metadata is not a REQUIRED binary column",
    "value_with_a_child": "v.typed_value.n.value is not an OPTIONAL binary column",
    "typed_leaf_with_a_child": "n.typed_value is a group with a physical type",
    "list_converted_map_key_value": "tags.typed_value is a group annotated {2: {}}",
}


def test_every_mutation_names_its_spec_fault():
    assert SPEC_FAULTS.keys() == MUTATIONS.keys()


def test_both_modes_accept_pyhoglakes_own_layout():
    column, elements = _layout_elements()
    plan = _plan_of(column)
    assert variant_layout_fault(elements, 1, column, plan, mode="exact") is None
    assert variant_layout_fault(elements, 1, column, plan, mode="spec") is None
    assert variant_layout_fault(elements, 1, column, None, mode="spec") is None


@pytest.mark.parametrize("mutation", MUTATIONS)
def test_both_modes_refuse_each_mutation(mutation):
    mutate, is_layout = MUTATIONS[mutation]
    column, elements = _layout_elements()
    mutate(elements)
    plan = _plan_of(column)
    assert variant_layout_fault(elements, 1, column, plan, mode="exact") is not None
    fault = variant_layout_fault(elements, 1, column, plan, mode="spec")
    assert SPEC_FAULTS[mutation] in (fault or ""), fault
    undeclared = variant_layout_fault(elements, 1, column, None, mode="spec")
    assert (undeclared is not None) == is_layout, undeclared


@pytest.mark.parametrize(
    ("physical", "length", "logical", "kind"),
    [
        (0, None, None, ("boolean",)),
        (0, None, {1: {}}, None),
        (1, None, None, ("int32",)),
        (1, None, {10: {1: 8, 2: True}}, ("int8",)),
        (1, None, {10: {1: 16, 2: True}}, ("int16",)),
        (1, None, {10: {1: 32, 2: True}}, ("int32",)),
        (1, None, {10: {1: 64, 2: True}}, None),
        (1, None, {10: {1: 8, 2: False}}, None),
        (1, None, {6: {}}, ("date",)),
        (1, None, {5: {1: 2, 2: 9}}, ("decimal4", 9, 2)),
        (1, None, {5: {1: 2, 2: 10}}, None),
        (1, None, {7: {1: False, 2: {1: {}}}}, None),
        (1, None, {7: {1: False, 2: {2: {}}}}, None),
        (1, None, {8: {1: True, 2: {2: {}}}}, None),
        (2, None, None, ("int64",)),
        (2, None, {10: {1: 64, 2: True}}, ("int64",)),
        (2, None, {10: {1: 32, 2: True}}, None),
        (2, None, {10: {1: 64, 2: False}}, None),
        (2, None, {5: {1: 0, 2: 18}}, ("decimal8", 18, 0)),
        (2, None, {5: {1: 0, 2: 19}}, None),
        (2, None, {7: {1: False, 2: {2: {}}}}, ("time",)),
        (2, None, {7: {1: True, 2: {2: {}}}}, None),
        (2, None, {7: {1: False, 2: {3: {}}}}, None),
        (2, None, {7: {1: False, 2: {1: {}}}}, None),
        (2, None, {8: {1: True, 2: {2: {}}}}, ("timestamptz",)),
        (2, None, {8: {1: True, 2: {3: {}}}}, ("timestamptz_ns",)),
        (2, None, {8: {1: False, 2: {2: {}}}}, ("timestamp",)),
        (2, None, {8: {1: False, 2: {3: {}}}}, ("timestamp_ns",)),
        (2, None, {8: {1: True, 2: {1: {}}}}, None),
        (2, None, {6: {}}, None),
        (3, None, None, None),
        (4, None, None, ("float",)),
        (4, None, {15: {}}, None),
        (5, None, None, ("double",)),
        (5, None, {5: {1: 0, 2: 9}}, None),
        (6, None, None, ("binary",)),
        (6, None, {1: {}}, ("string",)),
        (6, None, {5: {1: 0, 2: 38}}, ("decimal16", 38, 0)),
        (6, None, {5: {1: 0, 2: 39}}, None),
        (6, None, {12: {}}, None),
        (6, None, {4: {}}, None),
        (7, 16, {14: {}}, ("uuid",)),
        (7, 15, {14: {}}, None),
        (7, 4, {5: {1: 2, 2: 9}}, ("decimal16", 9, 2)),
        (7, 4, None, None),
        (7, 16, {1: {}}, None),
        (6, None, {1: {}, 4: {}}, None),
        (6, None, {}, None),
        # A union member that is no struct, or a TIMESTAMP with no
        # isAdjustedToUTC (a required bool), is a footer Thrift refuses.
        (6, None, {1: 5}, None),
        (2, None, {8: {2: {2: {}}}}, None),
        (2, None, {8: {1: 1, 2: {2: {}}}}, None),
        (2, None, {8: {1: False, 2: {2: {}, 1: {}}}}, None),
        # A DECIMAL parquet-java's schema builder refuses makes the file
        # unreadable: precision 0, a scale above the precision or below 0,
        # and more digits than a fixed-length array holds.
        (1, None, {5: {1: 0, 2: 0}}, None),
        (1, None, {5: {1: 5, 2: 4}}, None),
        (1, None, {5: {1: -1, 2: 4}}, None),
        (6, None, {5: {1: 0, 2: 1}}, ("decimal16", 1, 0)),
        (7, None, {5: {1: 0, 2: 1}}, None),
        (7, 0, {5: {1: 0, 2: 1}}, None),
    ],
)
def test_the_leaf_mapping_is_the_connectors(physical, length, logical, kind):
    """VariantShreddingSchema.primitiveValue, case by case: the Variant
    type each Parquet leaf holds, or none (a refusal)."""
    from pyhoglake.parquet_schema import _LayoutFault, _leaf_shape

    element = {1: physical, 3: 1, 4: "typed_value"}
    if length is not None:
        element[2] = length
    if kind is None:
        with pytest.raises(_LayoutFault, match="no Variant type"):
            _leaf_shape(element, logical, ("v", "typed_value"))
    else:
        assert _leaf_shape(element, logical, ("v", "typed_value")) == kind


def test_an_unshredded_value_must_be_required():
    """The connector reads an unshredded group only when
    metadata and value are both REQUIRED."""
    column = _variant_column(None)
    _, elements = _written(column, [1, None])
    assert variant_layout_fault(elements, 1, column, None, mode="exact") is None
    assert variant_layout_fault(elements, 1, column, None, mode="spec") is None
    _at(elements, "v", "value")[3] = 1
    assert "value" in variant_layout_fault(elements, 1, column, None, mode="exact")
    assert "REQUIRED" in variant_layout_fault(elements, 1, column, None, mode="spec")
    # Nor holds anything but the two.
    _at(elements, "v", "value")[3] = 0
    elements[1][5] = 3
    elements.append({1: 6, 3: 1, 4: "extra"})
    fault = variant_layout_fault(elements, 1, column, None, mode="spec")
    assert "is unshredded, and holds" in fault


@pytest.mark.parametrize("name", CORPUS)
@pytest.mark.parametrize("nullable", [True, False])
def test_exact_accepts_pyhoglakes_file_for_every_corpus_declaration(name, nullable):
    """Every corpus layout, as this pyarrow writes it (the CI matrix runs
    each version), REQUIRED or OPTIONAL as the column is."""
    column = _variant_column(CORPUS[name], nullable=nullable)
    rows = [{"a": 1}, [1, "x"], "s", Decimal("1.5")]
    _, elements = _written(column, rows if nullable else rows[:2])
    plan = _plan_of(column)
    assert elements[1][3] == (1 if nullable else 0)
    assert variant_layout_fault(elements, 1, column, plan, mode="exact") is None
    assert variant_layout_fault(elements, 1, column, plan, mode="spec") is None


def test_exact_accepts_every_decimal16_precision():
    """decimal16 is a fixed-length array of the fewest bytes its precision
    needs, the connector's decimalByteLength, which pyarrow's width must
    equal at every precision (7 is where a bit more makes a byte more)."""
    fields = [
        {"name": f"p{p}", "type": "decimal16", "precision": p, "scale": min(p, 3)}
        for p in range(1, 39)
    ]
    column = _variant_column({"type": "object", "fields": fields})
    _, elements = _written(column, [{"p7": Decimal("1234.567")}])
    plan = _plan_of(column)
    assert variant_layout_fault(elements, 1, column, plan) is None
    assert _at(elements, *TV, "p7", "typed_value")[2] == 4


def test_exact_holds_the_group_to_the_columns_field_id_and_repetition():
    column, elements = _layout_elements()
    plan = _plan_of(column)
    other = replace(column, field_id=2)
    assert "field" not in (variant_layout_fault(elements, 1, column, plan) or "")
    assert variant_layout_fault(elements, 1, other, plan) is not None
    assert variant_layout_fault(elements, 1, other, plan, mode="spec") is not None
    required = replace(column, nullable=False)
    assert variant_layout_fault(elements, 1, required, plan) is not None
    renamed = replace(column, name="w")
    assert "named 'v'" in variant_layout_fault(elements, 1, renamed, plan)


def test_exact_refuses_a_group_without_the_variant_annotation():
    column, elements = _layout_elements()
    del elements[1][10]
    plan = _plan_of(column)
    assert variant_layout_fault(elements, 1, column, plan) is not None
    assert "VARIANT(1)" in variant_layout_fault(elements, 1, column, plan, mode="spec")


def test_exact_lets_a_writer_leave_out_a_legacy_converted_type_and_no_more():
    """A converted type is the legacy spelling of the logical type beside
    it. pyarrow writes one; a later one may not, and the file is the same
    to every reader that knows logical types. A different one, or half of
    a DECIMAL's, is not that."""
    column, elements = _layout_elements()
    plan = _plan_of(column)
    string = _at(elements, *TV, "tags", "typed_value", "list", "element", "typed_value")
    decimal = _at(elements, *TV, "d4", "typed_value")
    gone = copy.deepcopy(elements)
    del _at(gone, *TV, "tags", "typed_value", "list", "element", "typed_value")[6]
    for field in (6, 7, 8):
        del _at(gone, *TV, "d4", "typed_value")[field]
    assert variant_layout_fault(gone, 1, column, plan) is None
    assert string[6] == 0 and decimal[6] == 5
    half = copy.deepcopy(elements)
    del _at(half, *TV, "d4", "typed_value")[6]
    assert variant_layout_fault(half, 1, column, plan) is not None
    other = copy.deepcopy(elements)
    _at(other, *TV, "tags", "typed_value", "list", "element", "typed_value")[6] = 19
    assert variant_layout_fault(other, 1, column, plan) is not None


def test_exact_refuses_extra_fields_on_an_element():
    column, elements = _layout_elements()
    _at(elements, *TV, "n", "typed_value")[10] = {10: {1: 32, 2: True}}
    assert variant_layout_fault(elements, 1, column, _plan_of(column)) is not None


def test_a_truncated_subtree_is_a_fault_not_a_crash():
    column, elements = _layout_elements()
    plan = _plan_of(column)
    for mode in ("exact", "spec"):
        fault = variant_layout_fault(elements[:6], 1, column, plan, mode=mode)
        assert "truncated" in fault


def test_a_negative_child_count_is_a_fault_not_a_crash():
    column, elements = _layout_elements()
    plan = _plan_of(column)
    _at(elements, "v", "metadata")[5] = -1
    for mode in ("exact", "spec"):
        fault = variant_layout_fault(elements, 1, column, plan, mode=mode)
        assert "negative schema child count" in (fault or ""), fault


def test_a_field_group_or_element_that_is_a_leaf_is_named_as_one():
    column, elements = _layout_elements()
    _field_group_not_a_group(elements)
    fault = variant_layout_fault(elements, 1, column, None, mode="spec")
    assert fault == "v.typed_value.any is not a group"
    column, elements = _layout_elements()
    _list_primitive_element(elements)
    fault = variant_layout_fault(elements, 1, column, None, mode="spec")
    assert fault == "v.typed_value.tags.typed_value.list.element is not a group"
    # With no physical type either, it is still no group.
    del _at(elements, *TV, "tags", "typed_value", "list", "element")[1]
    fault = variant_layout_fault(elements, 1, column, None, mode="spec")
    assert fault == "v.typed_value.tags.typed_value.list.element is not a group"


def test_a_malformed_foreign_annotation_is_a_fault_not_a_crash():
    """A footer can spell a union member that is not a struct; spec mode,
    which reads other writers' footers, answers with a fault."""
    column, elements = _layout_elements()
    elements[1][10] = {16: 5}
    assert "invalid schema" in variant_layout_fault(
        elements, 1, column, None, mode="spec"
    )
    _at(elements, *TV, "n", "typed_value")[10] = {10: 7}
    elements[1][10] = {16: {1: 1}}
    assert variant_layout_fault(elements, 1, column, None, mode="spec") is not None


@pytest.mark.parametrize(
    ("leaf", "declared", "converted", "accepted"),
    [
        # UTF8 alone is STRING to parquet-java, and to spec mode.
        (("tags", "typed_value", "list", "element", "typed_value"), None, 0, True),
        # DECIMAL alone, with its scale and precision.
        (("d4", "typed_value"), None, 5, True),
        # TIMESTAMP_MICROS alone is TIMESTAMP(true, MICROS): a timestamptz,
        # which a declared timestamp is not.
        (("ts", "typed_value"), None, 10, False),
        (("tz", "typed_value"), None, 10, True),
        # TIMESTAMP_MILLIS alone is MILLIS, which no Variant type is.
        (("tz", "typed_value"), None, 9, False),
        # UINT_32 alone is unsigned.
        (("n", "typed_value"), None, 13, False),
    ],
)
def test_spec_mode_reads_a_converted_type_where_there_is_no_logical_type(
    leaf, declared, converted, accepted
):
    """DuckDB writes some leaves with the legacy converted type alone (its
    INT32 carries INT_32, its lists LIST); spec mode reads the logical type
    it stands for, as parquet-java does."""
    column, elements = _layout_elements()
    element = _at(elements, *TV, *leaf)
    element.pop(10, None)
    element[6] = converted
    fault = variant_layout_fault(elements, 1, column, _plan_of(column), mode="spec")
    assert (fault is None) == accepted, fault


def test_spec_mode_reads_a_converted_decimal_without_a_scale_as_scale_0():
    """parquet-java reads a converted DECIMAL's scale off the element, and
    an absent one is Thrift's default, 0."""
    column = _variant_column({"type": "decimal4", "precision": 9, "scale": 0})
    _, elements = _written(column, [Decimal(5)])
    leaf = _at(elements, "v", "typed_value")
    assert leaf.pop(10) == {5: {1: 0, 2: 9}}
    del leaf[7]
    assert (
        variant_layout_fault(elements, 1, column, _plan_of(column), mode="spec") is None
    )


def _variant_annotation(variant_type):
    """An edit setting an element's logicalType to VARIANT with the
    VariantType fields ``variant_type``."""
    return lambda element: footer_oracle.put(
        element, 10, TType.STRUCT, [[16, TType.STRUCT, variant_type]]
    )


_NOT_VARIANT_1 = "native Parquet VARIANT version 1"
_BESIDE = "has the logical type VariantType beside the converted type"


@pytest.mark.parametrize(
    ("edit", "spelled", "fault"),
    [
        (_variant_annotation([]), {16: {}}, _NOT_VARIANT_1),
        # An i32 version is no version to Thrift's generated readers.
        (_variant_annotation([[1, TType.I32, 1]]), {16: {}}, _NOT_VARIANT_1),
        (
            lambda element: footer_oracle.put(element, 6, TType.I32, 1),
            {16: {1: 1}},
            f"{_BESIDE} MAP",
        ),
        (
            lambda element: footer_oracle.put(element, 6, TType.I32, 0),
            {16: {1: 1}},
            f"{_BESIDE} UTF8",
        ),
    ],
    ids=["unversioned", "i32_version", "converted_map", "converted_utf8"],
)
def test_a_group_the_server_reads_as_another_type_is_not_variant_1(
    edit, spelled, fault
):
    """The server's hydrator reads the group through parquet-java, which
    reads an absent specification_version as 0 (it takes the field's value
    without asking whether it is set), and lets a converted type beside
    the logical one win, as the connector's ParquetMetadata does: a MAP or
    a UTF8 here. FooterStats.variantFault warns of both as "not a native
    parquet VARIANT of spec version 1", and the openapi's variant requires
    VARIANT(1). pyarrow shows Variant(1) for every one of these files. So
    neither path accepts one: the first two as no VARIANT(1), the others as
    an element whose logical and converted types the readers read apart,
    which any element is refused for. The server's reading is asserted in
    the server's suite, on two files it shares with this one
    (:data:`SERVER_READ_AS_ANOTHER_TYPE`); a group with a physical type,
    which it cannot read at all, is refused as any element is
    (:data:`SERVER_CANNOT_READ`)."""
    column = _variant_column(LAYOUT)
    raw, _ = _written(column, LAYOUT_ROWS)
    raw = footer_oracle.edit_element(raw, ("v",), edit)
    assert _elements_of(raw)[1][10] == spelled
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        assert "Variant(1)" in str(parquet.schema)
    for strict in (False, True):
        with pytest.raises(ValidationError, match=fault):
            _validate(raw, (column,), strict=strict)


#: Two of the groups above, made from native_variant.parquet as
#: data/README.md says, and shared byte for byte with the server's suite,
#: whose FooterStatsTest asserts that its hydrator warns of both as "not a
#: native parquet VARIANT of spec version 1": so the server's reading of
#: them is pinned there, from the same bytes, and not restated here. With
#: what pyhoglake refuses each for.
SERVER_READ_AS_ANOTHER_TYPE = {
    "native_variant_unversioned.parquet": _NOT_VARIANT_1,
    "native_variant_converted_map.parquet": f"{_BESIDE} MAP",
}

#: Shared fixtures the server's hydrator cannot parse at all, which
#: FooterStatsTest asserts on the same bytes, with what pyhoglake refuses
#: each for: its group given a physical type, its metadata without a
#: repetition_type, its typed_value given a physical type, its group's
#: LogicalType union spelled with two fields, and a leaf whose converted
#: UTF8 parquet-java lets win over its logical INT(32, true), and cannot
#: put on an INT32.
SERVER_CANNOT_READ = {
    "native_variant_typed_group.parquet": (
        r"schema element 2 \('properties'\) is a group with a physical type"
    ),
    "native_variant_no_repetition.parquet": (
        r"schema element 3 \('metadata'\) has no repetition_type"
    ),
    "native_variant_typed_subgroup.parquet": (
        r"schema element 5 \('typed_value'\) is a group with a physical type"
    ),
    "native_variant_union_two_fields.parquet": "LogicalType union holds 2 fields",
    "native_variant_converted_unbuildable.parquet": (
        r"schema element 15 \('typed_value'\) has the logical type IntType beside "
        "the converted type UTF8"
    ),
}

#: Shared fixtures whose id the server's hydrator reads as the INTEGER(64,
#: true) of its converted INT_64, which FooterStatsTest asserts on the same
#: bytes, where pyarrow reads it otherwise, with what pyhoglake refuses each
#: for: its logical type a union of no member (which the Trino connector
#: cannot read at all), and a TIMESTAMP.
SERVER_READS_APART = {
    "native_variant_lone_member.parquet": (
        pa.int64(),
        r"schema element 1 \('id'\) has a logical type of no member",
    ),
    "native_variant_logical_beside_converted.parquet": (
        pa.timestamp("us", "UTC"),
        (
            r"schema element 1 \('id'\) has the logical type TimestampType beside "
            "the converted type INT_64"
        ),
    ),
}


def test_the_file_the_server_reads_as_variant_1_beside_an_undefined_converted_type():
    """The server's hydrator reads ``native_variant_converted_undefined``
    (converted_type 22 on the group) as VARIANT(1), with no warning:
    FooterStatsTest asserts it on the same bytes. So pyhoglake accepts it on
    both paths, and the rule below rests on that reading, not on a claim."""
    raw = (DATA / "native_variant_converted_undefined.parquet").read_bytes()
    assert _elements_of(raw)[2][6] == 22  # converted_type
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        assert "properties (Variant(1))" in str(parquet.schema)
    _validate(raw, COLUMNS, strict=False)
    # Strict holds DuckDB's OPTIONAL field groups against it, past the group.
    with pytest.raises(ValidationError, match="is not REQUIRED"):
        _validate(raw, COLUMNS, strict=True)


@pytest.mark.parametrize("name", sorted(SERVER_CANNOT_READ))
def test_the_files_the_server_cannot_read_are_refused(name):
    """parquet-java throws reading each footer, which FooterStatsTest
    asserts on the same bytes, where pyarrow opens it and shows the group
    as Variant(1); pyhoglake refuses it on every path."""
    raw = (DATA / name).read_bytes()
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        assert "properties (Variant(1))" in str(parquet.schema)
    for strict in (False, True):
        with pytest.raises(ValidationError, match=SERVER_CANNOT_READ[name]):
            _validate(raw, COLUMNS, strict=strict)
    with pytest.raises(ValidationError, match=SERVER_CANNOT_READ[name]):
        _check_chunks(raw)


@pytest.mark.parametrize("name", sorted(SERVER_READS_APART))
def test_the_files_the_server_reads_apart_from_pyarrow_are_refused(name):
    """The server's hydrator reads id as an INTEGER(64, true), and pyarrow
    as the type here; pyhoglake refuses each file on every path."""
    reads, fault = SERVER_READS_APART[name]
    raw = (DATA / name).read_bytes()
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        assert "properties (Variant(1))" in str(parquet.schema)
        assert parquet.schema_arrow.field("id").type == reads
    for strict in (False, True):
        with pytest.raises(ValidationError, match=fault):
            _validate(raw, COLUMNS, strict=strict)
    with pytest.raises(ValidationError, match=fault):
        _check_chunks(raw)


@pytest.mark.parametrize("converted", [22, 99, -1])
def test_a_converted_type_parquet_thrift_does_not_define_is_none(converted):
    """parquet-java (the server's hydrator, and the connector's
    ParquetMetadata) reads a converted_type outside ConvertedType's 0 to 21
    as unset (findByValue gives null), so the logical type beside it
    stands: the group is VARIANT(1) on both paths, and a string leaf is a
    string to strict. One of those alone is no annotation. The server's
    reading of 22 on the group is pinned on a shared fixture (above)."""
    column = _variant_column({"type": "string"})
    raw, _ = _written(column, ["a", "b"])
    for path in (("v",), ("v", "typed_value")):
        raw = footer_oracle.edit_element(
            raw,
            path,
            lambda element: footer_oracle.put(element, 6, TType.I32, converted),
        )
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        assert "Variant(1)" in str(parquet.schema)
    for strict in (False, True):
        _validate(raw, (column,), strict=strict)
    elements = _elements_of(raw)
    del _at(elements, "v", "typed_value")[10]
    fault = variant_layout_fault(elements, 1, column, _plan_of(column), mode="spec")
    assert fault == "v is binary, not the declared string"


@pytest.mark.parametrize("name", sorted(SERVER_READ_AS_ANOTHER_TYPE))
def test_the_files_the_server_reads_as_another_type_are_not_variant_1(name):
    raw = (DATA / name).read_bytes()
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        assert "properties (Variant(1))" in str(parquet.schema)
    for strict in (False, True):
        with pytest.raises(ValidationError, match=SERVER_READ_AS_ANOTHER_TYPE[name]):
            _validate(raw, COLUMNS, strict=strict)


@pytest.mark.parametrize(
    "name",
    [
        "native_variant.parquet",
        *SERVER_READ_AS_ANOTHER_TYPE,
        "native_variant_converted_undefined.parquet",
        *SERVER_CANNOT_READ,
        *SERVER_READS_APART,
    ],
)
def test_the_server_reads_the_same_variant_files(name):
    server = Path(__file__).parents[2] / "server" / "src" / "test" / "resources"
    if not server.exists():
        pytest.skip("not a repository checkout")
    assert (server / "variant" / name).read_bytes() == (DATA / name).read_bytes()


#: The most digits a FIXED_LEN_BYTE_ARRAY(n) DECIMAL holds, n = 1..16, as
#: parquet-column 1.18.1's Types builder (the connector's) accepts them:
#: measured, one more digit at each length throws "FIXED(n) cannot store".
FIXED_DIGITS = [2, 4, 6, 9, 11, 14, 16, 18, 21, 23, 26, 28, 31, 33, 35, 38]


@pytest.mark.parametrize("length", [*range(1, 17), 17, 20])
def test_a_fixed_length_decimal_holds_the_digits_parquet_java_allows(length):
    """Past 16 bytes parquet-java builds more digits, but the connector
    reads no DECIMAL above 38 (VariantShreddingSchema.decimalValue)."""
    from pyhoglake.parquet_schema import _LayoutFault, _leaf_shape

    element = {1: 7, 2: length, 3: 1, 4: "typed_value"}
    digits = FIXED_DIGITS[length - 1] if length <= 16 else 38
    path = ("v", "typed_value")
    assert _leaf_shape(element, {5: {1: 0, 2: digits}}, path) == (
        "decimal16",
        digits,
        0,
    )
    with pytest.raises(_LayoutFault, match="no Variant type"):
        _leaf_shape(element, {5: {1: 0, 2: digits + 1}}, path)


def _primitive_root(leaf):
    """pyhoglake's file of an int32 root, its typed_value leaf replaced by
    ``leaf``, and an undeclared column to read it as."""
    column = _variant_column({"type": "int32"})
    _, elements = _written(column, [1])
    assert elements[-1][4] == "typed_value"
    elements[-1] = {3: 1, 4: "typed_value", **leaf}
    return elements, _variant_column(None)


#: Each converted type alone, on a typed_value leaf of each physical type,
#: and the Variant type the connector reads there; every other pairing it
#: refuses. The connector maps a converted type as parquet-java does
#: (ParquetMetadataConverter.getLogicalTypeAnnotation(ConvertedType)): the
#: TIME_* and TIMESTAMP_* types adjusted to UTC, INT_* signed, UINT_*
#: unsigned, and then VariantShreddingSchema.primitiveValue reads the
#: result. So TIME_MICROS is TIME(true, MICROS), which no Variant type is.
CONVERTED_ALONE = {
    (0, 6): ("string",),  # UTF8
    (5, 1): ("decimal4", 9, 2),  # DECIMAL, with scale 2 and precision 9
    (5, 2): ("decimal8", 9, 2),
    (5, 6): ("decimal16", 9, 2),
    (5, 7): ("decimal16", 9, 2),
    (6, 1): ("date",),  # DATE
    (10, 2): ("timestamptz",),  # TIMESTAMP_MICROS
    (15, 1): ("int8",),  # INT_8
    (16, 1): ("int16",),  # INT_16
    (17, 1): ("int32",),  # INT_32
    (18, 2): ("int64",),  # INT_64, as DuckDB writes a BIGINT typed_value
}


@pytest.mark.parametrize("physical", range(8))
@pytest.mark.parametrize("converted", range(22))
def test_spec_mode_reads_each_converted_type_alone_as_the_connector_does(
    converted, physical
):
    leaf = {1: physical, 6: converted}
    if physical == 7:
        leaf[2] = 16
    if converted == 5:
        leaf.update({7: 2, 8: 9})
    elements, column = _primitive_root(leaf)
    fault = variant_layout_fault(elements, 1, column, None, mode="spec")
    kind = CONVERTED_ALONE.get((converted, physical))
    if kind is None:
        assert "which no Variant type is shredded as" in (fault or ""), fault
        return
    assert fault is None, fault
    # Read as exactly that type: the declaration of it is satisfied.
    decl = {"type": kind[0]}
    if len(kind) == 3:
        decl.update(precision=kind[1], scale=kind[2])
    declared = _variant_column(decl)
    assert (
        variant_layout_fault(elements, 1, declared, _plan_of(declared), mode="spec")
        is None
    )


@pytest.mark.parametrize(
    ("physical", "logical", "converted", "kind"),
    [
        # The two spell the same converted type: the logical type stands,
        # with whatever more it says (a local TIMESTAMP, a DECIMAL's scale).
        (1, {10: {1: 8, 2: True}}, 15, ("int8",)),
        (6, {1: {}}, 0, ("string",)),
        (2, {8: {1: False, 2: {2: {}}}}, 10, ("timestamp",)),
        (2, {7: {1: False, 2: {2: {}}}}, 8, ("time",)),
        # They differ: the converted type is read. A NANOS unit, a UUID and
        # UNKNOWN spell no converted type, which differs from any.
        (1, {10: {1: 8, 2: True}}, 11, None),
        (6, {1: {}}, 4, None),
        (6, {1: {}}, 19, None),
        (2, {8: {1: False, 2: {3: {}}}}, 10, ("timestamptz",)),
        (7, {14: {}}, 0, None),
        (1, {5: {1: 2, 2: 9}}, 17, ("int32",)),
        (1, {11: {}}, 15, ("int8",)),
        (2, {10: {1: 64, 2: True}}, 14, None),
        (1, {10: {1: 8, 2: False}}, 15, ("int8",)),
        (1, {10: {1: 16, 2: True}}, 15, ("int8",)),
        # One row for each spelling toOriginalType has, so that a wrong one
        # reads the pair as agreeing.
        (1, {6: {}}, 17, ("int32",)),
        (6, {4: {}}, 0, ("string",)),
        (6, {12: {}}, 0, ("string",)),
        (6, {13: {}}, 0, ("string",)),
        (1, {10: {1: 8, 2: True}}, 16, ("int16",)),
        (1, {10: {1: 32, 2: True}}, 18, None),
        (2, {10: {1: 64, 2: True}}, 17, None),
        # A malformed union is no logical type to defer to: still refused.
        (6, {}, 0, None),
        (6, {1: {}, 4: {}}, 0, None),
        # Nor is one Thrift or parquet-java cannot read, whatever converted
        # type is beside it: the connector cannot open the footer. An
        # INTEGER needs a bool isSigned and a width of 8, 16, 32 or 64; a
        # TIMESTAMP a bool isAdjustedToUTC and one unit.
        (1, {10: {1: 8}}, 15, None),
        (1, {10: {1: 8, 2: 1}}, 15, None),
        (1, {10: {1: 7, 2: True}}, 15, None),
        (2, {8: {2: {2: {}}}}, 10, None),
        (2, {8: {2: {1: {}}}}, 10, None),
        (2, {8: {1: False}}, 10, None),
        (2, {8: {1: False, 2: {1: {}, 2: {}}}}, 10, None),
        # Nor is a member parquet.thrift does not define: the generated
        # readers leave the union with no member set, which the connector's
        # getLogicalTypeAnnotation has no case for. UNKNOWN (11), which it
        # does read, is above.
        (6, {9: {}}, 0, None),
        (6, {19: {}}, 0, None),
    ],
)
def test_spec_mode_reads_the_converted_type_when_the_two_disagree(
    physical, logical, converted, kind
):
    """parquet-java, and the connector's ParquetMetadata after it, keep the
    logical type only when it spells the same converted type
    (toOriginalType); otherwise the converted type is what the leaf is."""
    leaf = {1: physical, 6: converted, 10: logical}
    if physical == 7:
        leaf[2] = 16
    elements, column = _primitive_root(leaf)
    fault = variant_layout_fault(elements, 1, column, None, mode="spec")
    if kind is None:
        assert "which no Variant type is shredded as" in (fault or ""), fault
        return
    assert fault is None, fault
    declared = _variant_column({"type": kind[0]})
    assert (
        variant_layout_fault(elements, 1, declared, _plan_of(declared), mode="spec")
        is None
    )


@pytest.mark.parametrize("member", [9, 19])
def test_a_logical_type_member_parquet_thrift_does_not_define_is_refused(member):
    """A string typed_value whose logicalType is a member parquet.thrift
    does not define, beside a converted UTF8. The three readers part ways:
    pyarrow reads a plain binary, the server's parquet-java a UTF8 string
    (an unset union is no annotation to it, so the converted type wins),
    and the connector cannot read the file at all. Every path refuses it,
    as any element whose logical type sets no member; spec mode, given the
    schema list itself, refuses it too, where letting the converted type
    win made it the declared string."""
    column = _variant_column({"type": "string"})
    raw, _ = _written(column, ["a", "b"])

    def edit(element):
        footer_oracle.put(element, 6, TType.I32, 0)
        footer_oracle.put(element, 10, TType.STRUCT, [[member, TType.STRUCT, []]])

    forged = footer_oracle.edit_element(raw, ("v", "typed_value"), edit)
    with pq.ParquetFile(pa.BufferReader(forged)) as parquet:
        typed = parquet.schema_arrow.field("v").type.field("typed_value")
        assert typed.type == pa.binary()
    for strict in (False, True):
        with pytest.raises(ValidationError, match="has a logical type of no member"):
            _validate(forged, (column,), strict=strict)
    fault = variant_layout_fault(
        _elements_of(forged), 1, column, _plan_of(column), mode="spec"
    )
    assert f"annotated {{{member}: None}}, which no Variant type" in (fault or "")


@pytest.mark.parametrize(
    ("logical", "converted", "fault"),
    [
        ({3: {}}, 3, None),
        # Logical LIST, converted MAP: toOriginalType says LIST, so the
        # converted MAP is what the group is.
        ({3: {}}, 1, "v.typed_value.tags.typed_value is a group annotated {2: {}}"),
        # And a logical MAP beside a converted LIST is a list.
        ({2: {}}, 3, None),
        ({2: {}}, 1, "v.typed_value.tags.typed_value is a group annotated {2: {}}"),
    ],
)
def test_spec_mode_reads_a_list_groups_converted_type_when_the_two_disagree(
    logical, converted, fault
):
    column, elements = _layout_elements()
    _at(elements, *TV, "tags", "typed_value").update({6: converted, 10: logical})
    for plan in (None, _plan_of(column)):
        found = variant_layout_fault(elements, 1, column, plan, mode="spec")
        if fault is None:
            assert found is None, found
        else:
            assert found == fault


def test_spec_mode_reads_a_leaf_that_counts_no_children_as_a_leaf():
    """parquet.thrift leaves num_children unset on a primitive, but a writer
    that sets it to 0 still writes a leaf: the connector tells one by its
    physical type, and pyarrow reads the file. Spec mode, which took any
    count for a group, refused it at its metadata."""
    column, elements = _layout_elements()
    for element in elements[1:]:
        if 1 in element:
            element[5] = 0
    plan = _plan_of(column)
    assert variant_layout_fault(elements, 1, column, None, mode="spec") is None
    assert variant_layout_fault(elements, 1, column, plan, mode="spec") is None
    # pyhoglake's writer sets none, so exact mode holds its files to that.
    assert variant_layout_fault(elements, 1, column, plan, mode="exact") is not None


def test_strict_accepts_a_file_whose_leaves_count_no_children():
    column = _variant_column({"type": "int64"})
    raw, _ = _written(column, [1, None, "x"])

    def count_none(tree):
        for element in footer_oracle.schema(tree):
            if footer_oracle.field(element, 1) is not None:
                footer_oracle.put(element, 5, TType.I32, 0)

    zeroed = footer_oracle.rebuild(raw, count_none)
    assert _elements_of(zeroed)[2][5] == 0
    _validate(zeroed, (column,), strict=True)


@pytest.mark.parametrize(
    "edit",
    [
        lambda group: group.update({10: {16: {1: 1}, 1: {}}}),
        lambda group: group.update({10: {16: {1: 2}}}),
        lambda group: group.update({3: 2}),
        lambda group: group.update({1: 6}),
        # A converted type beside VARIANT, which spells none: the converted
        # type is what the group is.
        lambda group: group.update({6: 1}),
        # parquet-java reads an absent version as 0.
        lambda group: group.update({10: {16: {}}}),
    ],
    ids=[
        "two_members",
        "version_2",
        "repeated",
        "physical_type",
        "converted",
        "unversioned",
    ],
)
def test_spec_mode_holds_the_group_itself_to_variant_1(edit):
    """validate_variant_file checks the group before spec mode does, so
    these are held here, against variant_layout_fault alone."""
    column, elements = _layout_elements()
    edit(elements[1])
    fault = variant_layout_fault(elements, 1, column, None, mode="spec")
    assert fault == "the group is not a VARIANT(1) group"
    assert variant_layout_fault(elements, 1, column, _plan_of(column)) is not None


def _deep(depth, kind="object"):
    """A stamped file whose typed_value shreds an object (a field ``a`` in a
    field ``a`` ...) or an array of arrays ``depth`` levels deep, as pyarrow
    writes it."""
    inner = pa.struct([pa.field("value", pa.binary())])
    for _ in range(depth):
        if kind == "object":
            typed = pa.struct([pa.field("a", inner, nullable=False)])
        else:
            typed = pa.list_(pa.field("element", inner, nullable=False))
        inner = pa.struct(
            [pa.field("value", pa.binary()), pa.field("typed_value", typed)]
        )
    storage = pa.struct(
        [
            pa.field("metadata", pa.binary(), nullable=False),
            pa.field("value", pa.binary()),
            pa.field("typed_value", inner.field("typed_value").type),
        ]
    )
    field = pa.field("v", storage, metadata={b"PARQUET:field_id": b"1"})
    sink = pa.BufferOutputStream()
    table = pa.table([pa.array([None], storage)], schema=pa.schema([field]))
    pq.write_table(table, sink, store_schema=False)
    stamp = variant_stamps(_variant_column(None), _plan_of(_variant_column(None)))
    raw, _ = stamp_variant_footer(sink.getvalue(), stamp)
    return raw


@pytest.mark.parametrize("kind", ["object", "array"])
@pytest.mark.parametrize("depth", [128, 129, 300])
def test_strict_refuses_a_layout_nested_deeper_than_a_value_may_be(depth, kind):
    """The spec walk recurses a few frames a level. A layout deeper than
    MAX_VARIANT_DEPTH (128, as a value's nesting is counted) is a named
    fault, not a RecursionError out of validate_variant_file: at 300 levels
    the unbounded walk overflowed the stack. pyarrow writes such a file on
    every version; up to 25 it opens it too, and the default path accepts
    it."""
    raw = _deep(depth, kind)
    column = _variant_column(None)
    fault = variant_layout_fault(_elements_of(raw), 1, column, None, mode="spec")
    if depth <= 128:
        assert fault is None, fault
    else:
        assert "shredded more than 128 levels deep" in (fault or ""), fault
    try:
        parquet = pq.ParquetFile(pa.BufferReader(raw))
    except OSError as error:
        # pyarrow 26 refuses a schema more than 100 levels deep (two a
        # shredded level) itself, so there the file never reaches the check.
        assert "too deeply nested" in str(error)
        return
    with parquet:
        validate_variant_file(raw, parquet, (column,))
        if fault is None:
            validate_variant_file(raw, parquet, (column,), strict=True)
            return
        with pytest.raises(ValidationError, match="more than 128 levels deep"):
            validate_variant_file(raw, parquet, (column,), strict=True)


def test_a_stack_too_shallow_for_the_walk_is_a_fault_not_a_crash():
    """The depth bound keeps the walk inside the interpreter's default
    stack; a caller already deep in its own gets a fault, never the
    RecursionError (a hedgerow catching ValidationError would crash)."""
    import inspect

    elements = _elements_of(_deep(128))
    column = _variant_column(None)
    limit = sys.getrecursionlimit()
    sys.setrecursionlimit(len(inspect.stack()) + 100)
    try:
        fault = variant_layout_fault(elements, 1, column, None, mode="spec")
    finally:
        sys.setrecursionlimit(limit)
    assert "RecursionError" in (fault or ""), fault


def test_mode_is_exact_or_spec():
    column, elements = _layout_elements()
    with pytest.raises(ValueError, match="mode"):
        variant_layout_fault(elements, 1, column, None, mode="loose")


def test_a_utc_timestamp_where_a_local_one_is_declared_is_refused_from_its_bytes():
    """The isAdjustedToUTC flag is a Thrift BOOL, which the footer reader
    used to skip: TIMESTAMP(true) read as TIMESTAMP(false), and both modes
    accepted a file the declaration does not describe."""
    aware = {"type": "object", "fields": [{"name": "ts", "type": "timestamptz"}]}
    local = {"type": "object", "fields": [{"name": "ts", "type": "timestamp"}]}
    _, elements = _written(
        _variant_column(aware), [{"ts": datetime(2024, 1, 1, tzinfo=UTC)}]
    )
    declared = _variant_column(local)
    plan = _plan_of(declared)
    assert variant_layout_fault(elements, 1, declared, plan, mode="exact") is not None
    fault = variant_layout_fault(elements, 1, declared, plan, mode="spec")
    assert "timestamptz, not the declared timestamp" in fault


def test_an_unsigned_leaf_is_refused_from_its_bytes():
    """pyarrow writes a uint32 as INT(32, false), which the connector does
    not read as any Variant type; isSigned is a BOOL too."""
    storage = pa.struct(
        [
            pa.field("metadata", pa.binary(), nullable=False),
            pa.field("value", pa.binary()),
            pa.field("typed_value", pa.uint32()),
        ]
    )
    field = pa.field("v", storage, metadata={b"PARQUET:field_id": b"1"})
    sink = pa.BufferOutputStream()
    table = pa.table(
        [
            pa.array(
                [{"metadata": b"\x01\x00\x00", "value": None, "typed_value": 7}],
                storage,
            )
        ],
        schema=pa.schema([field]),
    )
    pq.write_table(table, sink, store_schema=False)
    column = _variant_column({"type": "int32"})
    raw, _ = stamp_variant_footer(
        sink.getvalue(), variant_stamps(column, _plan_of(column))
    )
    elements = _elements_of(raw)
    assert "no Variant type" in variant_layout_fault(
        elements, 1, column, None, mode="spec"
    )
    assert variant_layout_fault(elements, 1, column, _plan_of(column)) is not None


# -- spec mode against apache/parquet-testing ----------------------------------

SHREDDED = DATA / "parquet-testing" / "shredded_variant"

#: The cases whose LAYOUT the spec forbids, as their notes and error
#: messages say: a variant group or a field group with no value (41, 131,
#: 132, 138), OPTIONAL field groups (84), an unsigned integer (127), and a
#: fixed-length array that is neither UUID nor DECIMAL (137). The other
#: INVALID and error cases are faults in values, which no layout check can
#: see.
LAYOUT_INVALID = {41, 84, 127, 131, 132, 137, 138}


def test_spec_mode_refuses_exactly_the_parquet_testing_layouts_the_spec_forbids():
    """Ground truth from outside: Iceberg's writer and the spec's authors
    classify each case, and spec mode agrees on every one (it reads none of
    their values)."""
    cases = json.loads((SHREDDED / "cases.json").read_text())
    column = Column("var", "variant", 2, 1, True)
    refused = set()
    read = 0
    for case in cases:
        if "parquet_file" not in case:
            continue
        read += 1
        elements = _elements_of((SHREDDED / case["parquet_file"]).read_bytes())
        start = _start(elements, "var")
        if variant_layout_fault(elements, start, column, None, mode="spec"):
            refused.add(case["case_number"])
    assert read == 137
    assert refused == LAYOUT_INVALID


# -- validate_variant_file(strict=...) on foreign files -------------------------

DUCKDB_COLUMNS = (
    Column("id", "int", 1, 0, True),
    Column("v", "variant", 2, 1, True),
)


@pytest.mark.parametrize(
    ("name", "columns"),
    [
        ("native_variant.parquet", COLUMNS),
        ("native_variant_struct.parquet", MIXED_COLUMNS),
        ("duckdb_unsorted_variant.parquet", DUCKDB_COLUMNS),
        ("duckdb_64_byte_strings.parquet", DUCKDB_COLUMNS),
    ],
)
def test_duckdb_files_are_accepted_by_default_and_refused_strictly(name, columns):
    """DuckDB writes every field and element group OPTIONAL (the spec says
    REQUIRED, VS:122, 166), which only strict refuses: the connector
    tolerates it, and the default is hedgerow's path, unchanged."""
    path = DATA / name
    with pq.ParquetFile(path) as parquet:
        validate_variant_file(str(path), parquet, columns)
        validate_variant_file(path.read_bytes(), parquet, columns)
        with pytest.raises(ValidationError, match="is not REQUIRED"):
            validate_variant_file(str(path), parquet, columns, strict=True)


SQL_NULL = DATA / "duckdb_variant_sql_null.parquet"
NO_NULL = DATA / "duckdb_variant_int.parquet"
NOT_NULL_COLUMNS = (
    Column("id", "int", 1, 0, True),
    Column("v", "variant", 2, 1, False),
)


def _validate(source, columns, *, strict):
    raw = source if isinstance(source, bytes) else source.read_bytes()
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        validate_variant_file(raw, parquet, columns, strict=strict)


def _check_chunks(raw):
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        validate_column_chunks(raw, parquet)


def test_a_duckdb_sql_null_in_a_not_null_column_passes_by_default():
    """The hole the DuckDB rule closes: DuckDB writes SQL NULL as a present
    group holding a Variant null, so the metadata leaf proves nothing."""
    _validate(SQL_NULL, NOT_NULL_COLUMNS, strict=False)


def test_strict_refuses_a_duckdb_sql_null_in_a_not_null_column():
    with pytest.raises(ValidationError, match="Variant null as the whole value"):
        _validate(SQL_NULL, NOT_NULL_COLUMNS, strict=True)


def test_strict_accepts_a_duckdb_file_whose_value_statistics_prove_no_sql_null():
    """Every value of the file is typed: its value column is all null and
    its typed_value has no null, so each row has exactly one side, the
    proof the rule asks for."""
    _validate(NO_NULL, NOT_NULL_COLUMNS, strict=True)


def test_the_duckdb_rule_is_for_not_null_columns_only():
    _validate(SQL_NULL, DUCKDB_COLUMNS, strict=True)


def test_the_duckdb_rule_is_for_files_duckdb_wrote_only():
    """Another writer's 00 is a Variant null, which a NOT NULL column holds
    legitimately; the connector reads it as one."""
    forged = footer_oracle.set_created_by(SQL_NULL.read_bytes(), "parquet-cpp-arrow")
    _validate(forged, NOT_NULL_COLUMNS, strict=True)
    duckdb = footer_oracle.set_created_by(NO_NULL.read_bytes(), "DuckDB other")
    _validate(duckdb, NOT_NULL_COLUMNS, strict=True)


def test_the_duckdb_rule_reads_created_by_as_the_connector_does():
    """The connector's test is startsWith("DuckDB") on the bytes it decodes
    with replacement; pyarrow decodes them strictly, and raised a
    UnicodeDecodeError out of strict validation for a created_by that is not
    UTF-8. Now that is read as its bytes, and a writer that names DuckDB
    elsewhere is not DuckDB."""
    for created_by in ("parquet-cpp-arrow (via DuckDB)", "duckdb"):
        _validate(
            footer_oracle.set_created_by(SQL_NULL.read_bytes(), created_by),
            NOT_NULL_COLUMNS,
            strict=True,
        )

    def forged(created_by):
        return footer_oracle.rebuild(
            SQL_NULL.read_bytes(),
            lambda tree: footer_oracle.put(tree, 6, TType.STRING, created_by),
        )

    for columns in (NOT_NULL_COLUMNS, DUCKDB_COLUMNS):
        _validate(forged(b"\xffDuckDB"), columns, strict=True)
    with pytest.raises(ValidationError, match="Variant null as the whole value"):
        _validate(forged(b"DuckDB \xff"), NOT_NULL_COLUMNS, strict=True)
    _validate(forged(b"DuckDB \xff"), DUCKDB_COLUMNS, strict=True)


DUCKDB_CREATED_BY = "DuckDB version v1.5.5 (build d8cdaa33fd)"


def _duckdb_int32_file(values, typed, *, row_group_size=None):
    """A file DuckDB is named as the writer of: a NOT NULL variant shredded
    as int32, each row's top-level ``value`` and ``typed_value`` given."""
    storage = pa.StructArray.from_arrays(
        [
            pa.array([b"\x01\x00\x00"] * len(values), pa.binary()),
            pa.array(values, pa.binary()),
            pa.array(typed, pa.int32()),
        ],
        fields=[
            pa.field("metadata", pa.binary(), nullable=False),
            pa.field("value", pa.binary()),
            pa.field("typed_value", pa.int32()),
        ],
    )
    column = _variant_column(None, nullable=False)
    field = pa.field("v", storage.type, nullable=False, metadata=ID_FIELD_ID)
    sink = pa.BufferOutputStream()
    table = pa.table([storage], schema=pa.schema([field]))
    pq.write_table(table, sink, store_schema=False, row_group_size=row_group_size)
    stamps = variant_stamps(column, _plan_of(column))
    raw, _ = stamp_variant_footer(sink.getvalue(), stamps)
    return footer_oracle.set_created_by(raw, DUCKDB_CREATED_BY), column


def _duckdb_unshredded_file(values):
    """A file DuckDB is named as the writer of: a NOT NULL unshredded
    variant, its ``value`` REQUIRED as the connector reads one, so its
    statistics count no null."""
    column = _variant_column(None, nullable=False)
    storage = pa.StructArray.from_arrays(
        [pa.array([b"\x01\x00\x00"] * len(values)), pa.array(values)],
        fields=[
            pa.field("metadata", pa.binary(), nullable=False),
            pa.field("value", pa.binary(), nullable=False),
        ],
    )
    field = pa.field("v", storage.type, nullable=False, metadata=ID_FIELD_ID)
    written = write_variant_parquet(
        pa.table([storage], schema=pa.schema([field])),
        [column],
        {"v": _plan_of(column)},
    )
    return footer_oracle.set_created_by(written.body, DUCKDB_CREATED_BY), column


@pytest.mark.parametrize(
    ("values", "typed"),
    [
        # The connector reads the header byte alone: 00 is a Variant null
        # whatever follows it, and a min of 00 ff is above 00.
        ([b"\x00\xff", None], [None, 7]),
        ([b"\x00\x01", b"\x0c\x05"], [None, None]),
        ([b"\x00", None], [None, 7]),
    ],
)
def test_strict_refuses_a_duckdb_value_whose_header_is_a_variant_null(values, typed):
    raw, column = _duckdb_int32_file(values, typed)
    with pytest.raises(ValidationError, match="Variant null as the whole value"):
        _validate(raw, (column,), strict=True)
    _validate(raw, (column,), strict=False)


def test_strict_reads_the_duckdb_rule_off_an_unshredded_value():
    """An unshredded group's value is REQUIRED, so it has no null for a
    typed_value to account for, and its minimum is the proof."""
    raw, column = _duckdb_unshredded_file([b"\x0c\x01", b"\x0c\x02"])
    _validate(raw, (column,), strict=True)
    for values in ([b"\x0c\x01", b"\x00"], [b"\x0c\x01", b"\x00\x01"]):
        raw, column = _duckdb_unshredded_file(values)
        with pytest.raises(ValidationError, match="Variant null as the whole value"):
            _validate(raw, (column,), strict=True)
        _validate(raw, (column,), strict=False)


def test_strict_refuses_a_duckdb_row_with_neither_value_nor_typed_value():
    """The connector reads a top-level row with both null as a Variant
    null, and a DuckDB file's Variant null as SQL NULL; the value leaf's
    statistics (all null) cannot see it, the typed_value's must."""
    raw, column = _duckdb_int32_file([None, None, None], [7, None, 9])
    with pytest.raises(ValidationError, match="neither value nor typed_value"):
        _validate(raw, (column,), strict=True)
    _validate(raw, (column,), strict=False)


def test_strict_accepts_a_duckdb_file_whose_row_groups_are_each_all_value_or_typed():
    """A row group whose value has no null (and no 00), and one whose value
    is all null and whose typed_value has none: every row of each has
    exactly one side, which the statistics prove."""
    raw, column = _duckdb_int32_file(
        [b"\x0c\x05", b"\x0c\x06", None, None], [None, None, 7, 9], row_group_size=2
    )
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        assert parquet.metadata.num_row_groups == 2
    _validate(raw, (column,), strict=True)


@pytest.mark.parametrize(
    ("values", "typed"),
    [
        # Nulls adding up to the rows, each row with one side.
        ([None, b"\x0c\x05", None], [7, None, 9]),
        # The same counts, with row 0 holding neither side and row 1 both:
        # the connector reads row 0 as NULL before it reaches row 1.
        ([None, b"\x0c\x05", b"\x0c\x06"], [None, 5, 6]),
        ([None, b"\x0c\x05", None], [None, 5, 6]),
        # Fewer nulls than rows: a row with both sides.
        ([b"\x0c\x04", b"\x0c\x05", None], [None, 5, 6]),
    ],
    ids=["one_side_each", "neither_then_both", "neither_then_both_typed", "both"],
)
def test_strict_refuses_a_duckdb_row_group_with_some_rows_of_no_value(values, typed):
    """Statistics count nulls, not rows, so a row group with nulls in only
    some of its values cannot prove that none of those rows has neither
    side, which pairs with a row with both in the same counts. The first
    file is sound and refused all the same; the next two are the review's,
    whose row 0 the connector returns as NULL from a NOT NULL column."""
    raw, column = _duckdb_int32_file(values, typed)
    with pytest.raises(ValidationError, match="has rows with a value and rows without"):
        _validate(raw, (column,), strict=True)
    _validate(raw, (column,), strict=False)


def test_the_duckdb_rule_reads_each_row_group_on_its_own():
    """A row group that proves each row typed, and a second that mixes: the
    second is refused, by its index."""
    raw, column = _duckdb_int32_file(
        [None, None, None, b"\x0c\x05"], [7, 9, 1, None], row_group_size=2
    )
    with pytest.raises(ValidationError, match="row group 1 has rows with a value"):
        _validate(raw, (column,), strict=True)


def test_the_duckdb_rule_passes_an_empty_row_group():
    """A row group of no rows holds no SQL NULL. Its value counts no null
    and has no min, which proves nothing of a row group with rows, and an
    unshredded group has no typed_value to prove it by either."""

    def empty_second(tree):
        groups = footer_oracle.field(tree, 4)[1]
        groups.append(copy.deepcopy(groups[0]))
        footer_oracle.put(groups[1], 3, TType.I64, 0)  # num_rows
        for chunk in footer_oracle.field(groups[1], 1)[1]:
            meta = footer_oracle.field(chunk, 3)
            footer_oracle.put(meta, 5, TType.I64, 0)  # num_values
            footer_oracle.put(meta, 12, TType.STRUCT, [[3, TType.I64, 0]])

    raw, column = _duckdb_unshredded_file([b"\x0c\x01", b"\x0c\x02"])
    raw = footer_oracle.rebuild(raw, empty_second)
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        empty = parquet.metadata.row_group(1).column(1)
        assert (empty.num_values, empty.statistics.has_min_max) == (0, False)
    _validate(raw, (column,), strict=True)


@pytest.mark.parametrize(
    "strip", [footer_oracle.drop_statistics, footer_oracle.drop_null_count]
)
def test_strict_refuses_a_duckdb_file_that_cannot_prove_each_row_has_a_side(strip):
    # The typed_value leaf (leaf 2) without statistics, or a null count.
    raw, column = _duckdb_int32_file([None, None], [7, 8])
    with pytest.raises(ValidationError, match="no typed_value statistics"):
        _validate(strip(raw, 2), (column,), strict=True)
    # A typed_value group: its leaves count the nulls below it as well.
    declared = _variant_column(
        {"type": "object", "fields": [{"name": "a", "type": "int64"}]},
        nullable=False,
    )
    written, _ = _written(declared, [{"a": 1}, {"a": 2}])
    forged = footer_oracle.set_created_by(written, DUCKDB_CREATED_BY)
    with pytest.raises(ValidationError, match="no typed_value statistics"):
        _validate(forged, (declared,), strict=True)


@pytest.mark.parametrize(
    "strip", [footer_oracle.drop_statistics, footer_oracle.drop_null_count]
)
def test_strict_refuses_a_duckdb_file_without_value_statistics(strip):
    # Leaves: id, v.metadata, v.value, v.typed_value.
    stripped = strip(NO_NULL.read_bytes(), 2)
    with pytest.raises(ValidationError, match="no statistics"):
        _validate(stripped, NOT_NULL_COLUMNS, strict=True)
    _validate(stripped, NOT_NULL_COLUMNS, strict=False)


def _set_statistic(length, fields=(5, 6)):
    """An edit setting each of ``fields`` of a chunk's Statistics (1 and 2
    the legacy max and min, 5 and 6 max_value and min_value) to ``length``
    bytes, and leaving the others as they are."""

    def edit(meta):
        statistics = footer_oracle.field(meta, 12)
        if statistics is None:
            statistics = []
            footer_oracle.put(meta, 12, TType.STRUCT, statistics)
        for field_id in fields:
            footer_oracle.put(statistics, field_id, TType.STRING, b"\x01" * length)

    return edit


def _refused_everywhere(raw, fault):
    """``raw`` is refused with ``fault`` by validate_variant_file, strict or
    not, its variant column NOT NULL or not, and by the check prepared
    files without a variant column get."""
    for columns in (NOT_NULL_COLUMNS, DUCKDB_COLUMNS):
        for strict in (False, True):
            with pytest.raises(ValidationError, match=fault):
                _validate(raw, columns, strict=strict)
    with pytest.raises(ValidationError, match=fault):
        _check_chunks(raw)


@pytest.mark.parametrize(
    ("leaf", "edit", "fault"),
    [
        # Leaves: id, v.metadata, v.value, v.typed_value.
        (2, lambda meta: footer_oracle.put(meta, 1, TType.I32, 3), "is of type 3, not"),
        (3, _set_statistic(3), "3-byte statistic of a 4-byte type"),
        # Each bound alone, the others left full width: pyarrow aborts on
        # a short max_value or min_value alone, and on a short legacy one
        # when it reads the legacy fields (the writer's column orders and
        # created_by decide).
        (0, _set_statistic(1, (5,)), "1-byte statistic of a 4-byte type"),
        (0, _set_statistic(2, (6,)), "2-byte statistic of a 4-byte type"),
        (0, _set_statistic(3, (1,)), "3-byte statistic of a 4-byte type"),
        (0, _set_statistic(0, (2,)), "0-byte statistic of a 4-byte type"),
    ],
    ids=["type", "both", "max_value", "min_value", "legacy_max", "legacy_min"],
)
def test_a_chunk_pyarrow_cannot_decode_statistics_of_is_refused(leaf, edit, fault):
    """pyarrow decodes a chunk's statistics when they are first read, and
    aborts the process (an uncaught C++ exception) for a chunk whose type
    is not its leaf's or whose min is too short: a footer-overwrite
    property drew both, and killed the test run. Every chunk is checked
    before any statistic is read, strict or not, NOT NULL or not, since
    extract_column_stats reads them all after the check."""
    raw = footer_oracle.edit_chunk(NO_NULL.read_bytes(), leaf, edit)
    _refused_everywhere(raw, fault)


def test_a_chunk_without_statistics_does_not_end_the_check():
    """The chunks after one with no statistics are checked too: here only
    the second column has statistics, and its min is short."""
    columns = (Column("a", "int", 1, 0, True), Column("b", "int", 2, 1, True))
    table = pa.table(
        [pa.array([1, 2], pa.int32()), pa.array([3, 4], pa.int32())],
        schema=pa.schema(
            [
                pa.field(c.name, pa.int32(), metadata={b"PARQUET:field_id": b"%d" % i})
                for i, c in enumerate(columns, 1)
            ]
        ),
    )
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, write_statistics=["b"])
    raw = sink.getvalue().to_pybytes()
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        assert parquet.metadata.row_group(0).column(0).statistics is None
    _validate(raw, columns, strict=False)
    short = footer_oracle.edit_chunk(raw, 1, _set_statistic(3, (6,)))
    with pytest.raises(ValidationError, match="column chunk 1 .* 3-byte statistic"):
        _validate(short, columns, strict=False)
    with pytest.raises(ValidationError, match="column chunk 1 .* 3-byte statistic"):
        _check_chunks(short)


def _size_statistics(**fields):
    """An edit giving a chunk SizeStatistics (ColumnMetaData field 16) of
    ``unencoded`` byte array data bytes and ``repetition`` and
    ``definition`` level histograms, each left out when not given."""
    spelled = []
    if "unencoded" in fields:
        spelled.append([1, TType.I64, fields["unencoded"]])
    for field_id, name in ((2, "repetition"), (3, "definition")):
        if name in fields:
            spelled.append([field_id, TType.LIST, [TType.I64, fields[name]]])
    return lambda meta: footer_oracle.put(meta, 16, TType.STRUCT, spelled)


@pytest.mark.parametrize(
    ("leaf", "edit", "fault"),
    [
        # Leaves: id (OPTIONAL INT32, levels 0 to 1), v.metadata (REQUIRED
        # in an OPTIONAL group), v.value and v.typed_value (OPTIONAL in it,
        # levels 0 to 2); nothing is repeated.
        (
            0,
            _size_statistics(definition=[1]),
            "1-entry definition level histogram, for a maximum level of 1",
        ),
        (
            3,
            _size_statistics(definition=[1, 2, 3, 4, 5, 6]),
            "6-entry definition level histogram, for a maximum level of 2",
        ),
        (
            3,
            _size_statistics(repetition=[1, 2, 3]),
            "3-entry repetition level histogram, for a maximum level of 0",
        ),
        (0, _size_statistics(unencoded=5), "unencoded byte array data bytes"),
        (3, _size_statistics(unencoded=5), "unencoded byte array data bytes"),
        # pyarrow asks whether the field is set, not what it holds.
        (0, _size_statistics(unencoded=0), "unencoded byte array data bytes"),
    ],
    ids=[
        "id_definition",
        "value_definition",
        "repetition",
        "int32",
        "typed_int32",
        "int32_zero",
    ],
)
def test_size_statistics_pyarrow_cannot_build_the_chunk_of_are_refused(
    leaf, edit, fault
):
    """pyarrow validates a chunk's SizeStatistics when it builds the
    chunk's metadata (``row_group.column(i)``, which the NOT NULL proof and
    extract_column_stats both read), and aborts the process when a level
    histogram is neither empty nor one entry per level, or a chunk that is
    not BYTE_ARRAY counts unencoded byte array bytes."""
    raw = footer_oracle.edit_chunk(NO_NULL.read_bytes(), leaf, edit)
    _refused_everywhere(raw, fault)


@pytest.mark.parametrize(
    ("leaf", "edit"),
    [
        (0, _size_statistics(repetition=[], definition=[])),
        (0, _size_statistics(repetition=[2], definition=[0, 2])),
        (3, _size_statistics(repetition=[2], definition=[0, 1, 1])),
        (1, _size_statistics(unencoded=6, definition=[0, 2])),
    ],
    ids=["empty", "id", "value", "binary"],
)
def test_size_statistics_pyarrow_reads_are_accepted(leaf, edit):
    raw = footer_oracle.edit_chunk(NO_NULL.read_bytes(), leaf, edit)
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        statistics = parquet.metadata.row_group(0).column(leaf).statistics
        assert statistics is not None
    for columns in (NOT_NULL_COLUMNS, DUCKDB_COLUMNS):
        _validate(raw, columns, strict=True)
    _check_chunks(raw)


def test_a_corpus_histogram_one_entry_too_long_is_refused():
    """pyarrow 25 writes level histograms on every chunk; one entry added
    to the first's is enough to abort the process."""
    raw = (DATA / "variant" / "corpus_stamped.parquet").read_bytes()
    columns = tuple(
        Column(element[4], "variant", element[9], ordinal, True)
        for ordinal, (element, _, _) in enumerate(
            _top_level(_schema_elements_from_footer(_footer_bytes(raw)[1]))
        )
    )
    for strict in (False, True):
        _validate(raw, columns, strict=strict)

    def append(meta):
        histogram = footer_oracle.field(footer_oracle.field(meta, 16), 3)[1]
        histogram.append(1)

    longer = footer_oracle.edit_chunk(raw, 0, append)
    for strict in (False, True):
        with pytest.raises(ValidationError, match="definition level histogram"):
            _validate(longer, columns, strict=strict)


def _parquet_files():
    """Every file at hand whose footer the reader reads: all but the union
    of two fields among SERVER_CANNOT_READ."""
    return [
        *sorted(
            path
            for path in DATA.glob("*.parquet")
            if path.name != "native_variant_union_two_fields.parquet"
        ),
        DATA / "variant" / "corpus_stamped.parquet",
        *sorted((DATA / "parquet-testing" / "shredded_variant").glob("*.parquet")),
    ]


def test_leaf_levels_are_pyarrows():
    """The levels each leaf's histograms are held to, against the maximum
    levels pyarrow's own schema gives every leaf of every file at hand
    (lists and REQUIRED leaves among them)."""
    for path in _parquet_files():
        raw = path.read_bytes()
        leaves = _leaf_levels(_schema_elements_from_footer(_footer_bytes(raw)[1]))
        with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
            schema = parquet.metadata.schema
            assert [(d, r) for _, d, r in leaves] == [
                (
                    schema.column(i).max_definition_level,
                    schema.column(i).max_repetition_level,
                )
                for i in range(len(schema))
            ], path.name


def _edit_column_chunk(raw, leaf, edit, *, encoder=footer_oracle.encode):
    """``raw`` with ``edit`` applied to the ColumnChunk (not its metadata)
    of leaf column ``leaf`` in every row group."""

    def apply(tree):
        for row_group in footer_oracle.field(tree, 4)[1]:
            edit(footer_oracle.field(row_group, 1)[1][leaf])

    return footer_oracle.rebuild(raw, apply, encoder=encoder)


#: ColumnCryptoMetaData's union: ENCRYPTION_WITH_FOOTER_KEY (an empty
#: struct), and ENCRYPTION_WITH_COLUMN_KEY (its path_in_schema).
_FOOTER_KEY = [[1, TType.STRUCT, []]]
_COLUMN_KEY = [[2, TType.STRUCT, [[1, TType.LIST, [TType.STRING, [b"v"]]]]]]


@pytest.mark.parametrize(
    ("edit", "fault"),
    [
        (
            lambda chunk: footer_oracle.put(chunk, 8, TType.STRUCT, _COLUMN_KEY),
            "is encrypted",
        ),
        (
            lambda chunk: footer_oracle.put(chunk, 8, TType.STRUCT, _FOOTER_KEY),
            "is encrypted",
        ),
        (lambda chunk: footer_oracle.drop(chunk, 3), "has no metadata"),
    ],
    ids=["column_key", "footer_key", "no_metadata"],
)
def test_a_column_chunk_pyarrow_cannot_read_is_refused(edit, fault):
    """pyarrow opens a plaintext footer whose chunk is encrypted with a
    column key, and aborts the process building that chunk's metadata
    (it has no key). One encrypted with the footer key it builds, but its
    pages are ciphertext nothing here can read. A chunk with no metadata
    has nothing to check, and pyarrow opens that too."""
    raw = _edit_column_chunk(NO_NULL.read_bytes(), 1, edit)
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        assert parquet.schema_arrow.names == ["id", "v"]
    _refused_everywhere(raw, f"column chunk 1 of row group 0 {fault}")


@pytest.mark.parametrize(
    "path", [[b"\x80"], [b"v", b"\x80"]], ids=["first_name", "later_name"]
)
def test_a_path_in_schema_that_is_not_utf8_is_refused(path):
    """pyarrow opens it, and raises UnicodeDecodeError (not a
    ValidationError) when extract_column_stats reads the chunk's path,
    whichever of its names is not UTF-8; the footer-overwrite property drew
    one in a parquet-testing file."""
    raw = footer_oracle.edit_chunk(
        NO_NULL.read_bytes(),
        0,
        lambda meta: footer_oracle.put(meta, 3, TType.LIST, [TType.STRING, path]),
    )
    with (
        pq.ParquetFile(pa.BufferReader(raw)) as parquet,
        pytest.raises(UnicodeDecodeError),
    ):
        parquet.metadata.row_group(0).column(0).path_in_schema  # noqa: B018
    _refused_everywhere(raw, "path in schema that is not UTF-8")


def _path_of(leaf, names):
    """An edit of a file giving leaf column ``leaf``'s chunks, in every row
    group, the path_in_schema ``names``."""
    return lambda raw: footer_oracle.edit_chunk(
        raw,
        leaf,
        lambda meta: footer_oracle.put(meta, 3, TType.LIST, [TType.STRING, names]),
    )


def _later_path(names):
    """NO_NULL with a second row group whose id chunk's path is ``names``."""
    return lambda raw: footer_oracle.rebuild(
        raw,
        _second_row_group(
            lambda meta: footer_oracle.put(meta, 3, TType.LIST, [TType.STRING, names])
        ),
    )


#: NO_NULL with a chunk's path_in_schema not its leaf's, and the refusal.
PATHS_NOT_THE_LEAFS = {
    "foreign": (
        _path_of(0, [b"zzz"]),
        (
            "column chunk 0 of row group 0 has the path_in_schema ['zzz'], not its "
            "leaf's ['id']"
        ),
    ),
    "sibling": (
        _path_of(2, [b"v", b"metadata"]),
        (
            "column chunk 2 of row group 0 has the path_in_schema ['v', 'metadata'], "
            "not its leaf's ['v', 'value']"
        ),
    ),
    # parquet-java compares names case and all.
    "case": (_path_of(0, [b"ID"]), "path_in_schema ['ID'], not its leaf's ['id']"),
    # The same dotted string, other names.
    "dotted": (
        _path_of(1, [b"v.metadata"]),
        "path_in_schema ['v.metadata'], not its leaf's ['v', 'metadata']",
    ),
    "short": (_path_of(1, [b"v"]), "path_in_schema ['v'], not its leaf's"),
    # The same names run together, other names.
    "split": (
        _path_of(1, [b"vm", b"etadata"]),
        "path_in_schema ['vm', 'etadata'], not its leaf's ['v', 'metadata']",
    ),
    "later_row_group": (
        _later_path([b"zzz"]),
        (
            "column chunk 0 of row group 1 has the path_in_schema ['zzz'], not its "
            "leaf's ['id']"
        ),
    ),
}


@pytest.mark.parametrize("case", sorted(PATHS_NOT_THE_LEAFS))
def test_a_chunk_whose_path_is_not_its_leafs_is_refused(case):
    """pyarrow pairs a chunk with its leaf by position, and reads the file;
    parquet-java (the server's hydrator) and the Trino connector find a
    chunk's column by its path_in_schema, and cannot read a footer naming a
    column the schema does not have ("zzz not found in message schema"),
    and extract_column_stats files a chunk's statistics under it (below).
    So every chunk's path, in every row group, must be its leaf's, name for
    name."""
    edit, fault = PATHS_NOT_THE_LEAFS[case]
    raw = edit(NO_NULL.read_bytes())
    _read_every_chunk(raw)
    _refused_everywhere(raw, re.escape(fault))


def test_a_chunk_with_a_siblings_path_would_bound_the_sibling():
    """A chunk of ``n`` given ``m``'s path: extract_column_stats files n's
    statistics as m's, so m is registered with bounds that exclude every
    value it holds, which pruning skips the file for."""
    columns = (Column("n", "long", 1, 0, False), Column("m", "long", 2, 1, False))
    table = pa.Table.from_pylist(
        [{"n": 1, "m": 3}, {"n": 2, "m": 4}], schema=columns_to_arrow_schema(columns)
    )
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    raw = _path_of(0, [b"m"])(sink.getvalue().to_pybytes())
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        stats = extract_column_stats(parquet.metadata, columns)
        assert [stat.field_id for stat in stats] == [2]
        assert stats[0].lower_bound == (1).to_bytes(8, "little")
        assert pq.read_table(pa.BufferReader(raw)).column("m").to_pylist() == [3, 4]
    with pytest.raises(ValidationError, match=re.escape("not its leaf's ['n']")):
        _check_chunks(raw)


def _encoding_stats(page_type, encoding):
    """A chunk edit giving it one PageEncodingStats (ColumnMetaData field 13)
    of ``page_type`` and ``encoding``."""
    stats = [[1, TType.I32, page_type], [2, TType.I32, encoding], [3, TType.I32, 1]]
    return lambda meta: footer_oracle.put(meta, 13, TType.LIST, [TType.STRUCT, [stats]])


def _codec(value):
    return lambda meta: footer_oracle.put(meta, 4, TType.I32, value)


@pytest.mark.parametrize(
    ("edit", "fault"),
    [
        (_codec(99), "field 4 of a Parquet footer ColumnMetaData is 99, which "),
        (_codec(-1), "ColumnMetaData is -1, which parquet.thrift's CompressionCodec"),
        (_codec(8), "ColumnMetaData is 8, which parquet.thrift's CompressionCodec"),
        (_encoding_stats(99, 0), "PageEncodingStats is 99, which parquet.thrift's "),
        (_encoding_stats(4, 0), "field 1 of a Parquet footer PageEncodingStats is 4"),
        (_encoding_stats(0, 99), "PageEncodingStats is 99, which parquet.thrift's E"),
        # GROUP_VAR_INT, gone from parquet.thrift.
        (_encoding_stats(0, 1), "field 2 of a Parquet footer PageEncodingStats is 1"),
        (_encoding_stats(0, 10), "field 2 of a Parquet footer PageEncodingStats is 10"),
    ],
    ids=["codec", "codec_negative", "codec_past", "page_type", "page_type_past"]
    + ["encoding", "encoding_removed", "encoding_past"],
)
def test_a_required_enum_of_a_value_its_enum_does_not_define_is_refused(edit, fault):
    """parquet-java's generated reader (the server's hydrator's, and the
    connector's) reads an enum value its enum does not define as null, and
    then refuses the footer for a required field that is absent ("Required
    field 'codec' was not present!"). pyarrow keeps the number, reads a codec
    it does not know as UNCOMPRESSED, and builds every chunk."""
    raw = footer_oracle.edit_chunk(NO_NULL.read_bytes(), 0, edit)
    _read_every_chunk(raw)
    _refused_everywhere(raw, re.escape(fault))


def test_the_enum_values_parquet_thrift_defines_are_accepted():
    """The last codec (LZ4_RAW), page type (DATA_PAGE_V2) and encoding
    (BYTE_STREAM_SPLIT) are read, and so is an encodings list item no
    Encoding is: that list's items are no required field, and parquet-java
    reads one it does not know as a null item."""

    def edit(meta):
        _codec(7)(meta)
        _encoding_stats(3, 9)(meta)
        footer_oracle.put(meta, 2, TType.LIST, [TType.I32, [0, 99]])

    raw = footer_oracle.edit_chunk(NO_NULL.read_bytes(), 0, edit)
    _read_every_chunk(raw)
    for strict in (False, True):
        _validate(raw, DUCKDB_COLUMNS, strict=strict)
    _check_chunks(raw)


def _read_every_chunk(raw):
    """Build every chunk's metadata and decode its statistics, as the NOT
    NULL proof and extract_column_stats do: what the guard accepts must
    survive this (a breach aborts the process)."""
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        metadata = parquet.metadata
        for group in range(metadata.num_row_groups):
            row_group = metadata.row_group(group)
            for index in range(row_group.num_columns):
                chunk = row_group.column(index)
                _ = chunk.statistics, chunk.path_in_schema


def test_a_field_of_another_wire_type_is_read_as_pyarrow_reads_it():
    """Thrift's generated readers skip a field whose wire type is not the
    one parquet.thrift declares, so pyarrow reads a num_children spelled
    as a string as no num_children, and v.metadata as the leaf it is. The
    footer reader here skips it too, and agrees: the file is accepted, and
    its chunks read."""
    raw = footer_oracle.edit_element(
        NO_NULL.read_bytes(),
        ("v", "metadata"),
        lambda element: footer_oracle.put(element, 5, TType.STRING, b"0"),
    )
    assert 5 not in _at(_elements_of(raw), "v", "metadata")
    for columns in (NOT_NULL_COLUMNS, DUCKDB_COLUMNS):
        for strict in (False, True):
            _validate(raw, columns, strict=strict)
    _check_chunks(raw)
    _read_every_chunk(raw)


#: A repetition_type pyarrow reads as REQUIRED: absent, OPTIONAL (1) spelled
#: with a wire type that is not parquet.thrift's i32, which Thrift's
#: generated readers skip, or a value FieldRepetitionType does not define.
_REQUIRED_SPELLINGS = {
    "absent": lambda element: footer_oracle.drop(element, 3),
    "i8": lambda element: footer_oracle.put(element, 3, TType.BYTE, 1),
    "bool": lambda element: footer_oracle.put(element, 3, TType.BOOL, True),
    "3": lambda element: footer_oracle.put(element, 3, TType.I32, 3),
    "negative": lambda element: footer_oracle.put(element, 3, TType.I32, -1),
}


@pytest.mark.parametrize("spelling", sorted(_REQUIRED_SPELLINGS))
@pytest.mark.parametrize(
    ("path", "index"),
    [(("v", "typed_value"), 5), (("v", "metadata"), 3), (("v",), 2), (("id",), 1)],
    ids=["leaf", "variant_leaf", "variant_group", "column"],
)
def test_a_schema_element_without_a_repetition_type_is_refused(spelling, path, index):
    """pyarrow reads each as REQUIRED, but parquet-java (the server's
    hydrator) and the Trino connector's ``ParquetMetadata.readTypeSchema``
    call ``repetition_type.name()`` and throw NullPointerException: they
    cannot read the footer, so a published file fails every query. pyarrow
    opens each, and reads the element REQUIRED; the guard refuses it on
    every path, before any chunk is read. A typed_value pyarrow reads as
    REQUIRED was held to the levels pyarrow read it at, and published when
    its chunk fitted them (``native_variant_no_repetition.parquet`` pins
    parquet-java's reading)."""
    spelled = footer_oracle.edit_element(
        NO_NULL.read_bytes(), path, _REQUIRED_SPELLINGS[spelling]
    )
    with pq.ParquetFile(pa.BufferReader(spelled)) as parquet:
        assert parquet.schema_arrow.names == ["id", "v"]
        if path == ("v", "typed_value"):
            leaf = parquet.metadata.schema.column(3)
            assert (leaf.max_definition_level, leaf.max_repetition_level) == (1, 0)
    fault = f"schema element {index} \\({path[-1]!r}\\) has "
    _refused_everywhere(spelled, fault)
    for histogram in ([0, 1, 1], [0, 2]):
        sized = footer_oracle.edit_chunk(
            spelled, 3, _size_statistics(definition=histogram)
        )
        _refused_everywhere(sized, fault)


def test_the_repetition_refusal_reads_the_field_as_the_generated_readers_do():
    """The refusal's message names what Thrift's generated readers read:
    nothing for a field of another wire type, which they skip, and the
    value for one parquet.thrift does not define."""
    for spelling, read in (("i8", "no repetition_type"), ("3", "repetition_type 3")):
        spelled = footer_oracle.edit_element(
            NO_NULL.read_bytes(), ("v", "typed_value"), _REQUIRED_SPELLINGS[spelling]
        )
        with pytest.raises(
            ValidationError, match=f"'typed_value'\\) has {read}, which"
        ):
            _check_chunks(spelled)


def test_a_physical_type_of_another_wire_type_leaves_an_empty_group():
    """A typed_value whose type is spelled as an i8 has no type to pyarrow,
    and no children: an empty group (``struct<>``), not a leaf, so the
    file has a column chunk more than pyarrow's schema has leaves (its
    column orders, one per leaf, and its annotation are dropped so that
    pyarrow opens it). It aborted the process reading that chunk."""

    def edit(tree):
        footer_oracle.drop(tree, 7)
        elements = footer_oracle.schema(tree)
        where = dict(zip(footer_oracle.paths(elements), elements, strict=True))
        typed = where[("v", "typed_value")]
        footer_oracle.put(typed, 1, TType.BYTE, 1)
        # Its INT(32, true), which pyarrow refuses on a group.
        footer_oracle.drop(typed, 6)
        footer_oracle.drop(typed, 10)

    raw = footer_oracle.rebuild(NO_NULL.read_bytes(), edit)
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        assert parquet.schema_arrow.field("v").type.field("typed_value").type == (
            pa.struct([])
        )
        assert len(parquet.metadata.schema) == 3
    _refused_everywhere(raw, "row group 0 does not have a column chunk per leaf")


def test_an_empty_group_has_no_leaf_for_the_columns_after_it_to_count():
    """The same empty group with its chunk removed too, so the file has a
    chunk per leaf pyarrow reads, and a NOT NULL column after it: that
    column's null-count proof is its own chunk, the third, not a fourth
    that counting the empty group as a leaf would ask for."""
    column = _variant_column({"type": "int64"})
    storage = variant.encode_python([1, "x"], column).array
    table = pa.table(
        [storage, pa.array([1, 2], pa.int32())],
        schema=pa.schema(
            [
                pa.field("v", storage.type, metadata={b"PARQUET:field_id": b"1"}),
                pa.field("id", pa.int32(), metadata={b"PARQUET:field_id": b"2"}),
            ]
        ),
    )
    columns = (column, Column("id", "int", 2, 1, False))
    raw = write_variant_parquet(table, columns, {"v": _plan_of(column)}).body

    def edit(tree):
        footer_oracle.drop(tree, 7)
        elements = footer_oracle.schema(tree)
        where = dict(zip(footer_oracle.paths(elements), elements, strict=True))
        footer_oracle.drop(where[("v", "typed_value")], 1)
        for row_group in footer_oracle.field(tree, 4)[1]:
            del footer_oracle.field(row_group, 1)[1][2]

    emptied = footer_oracle.rebuild(raw, edit)
    with pq.ParquetFile(pa.BufferReader(emptied)) as parquet:
        assert len(parquet.metadata.schema) == 3
    _validate(emptied, columns, strict=False)
    _check_chunks(emptied)
    _read_every_chunk(emptied)


def _repeat_in_chunk(field_id, first, second):
    """An edit giving each chunk's ColumnMetaData ``field_id`` twice, the
    struct ``first`` and then ``second``."""

    def edit(meta):
        footer_oracle.drop(meta, field_id)
        footer_oracle.repeat(meta, field_id, TType.STRUCT, first)
        footer_oracle.repeat(meta, field_id, TType.STRUCT, second)

    return edit


def _repeat_schema(tree):
    """A second schema list after the first, in which v.typed_value is
    REQUIRED: pyarrow reads the last list."""
    elements = copy.deepcopy(footer_oracle.schema(tree))
    where = dict(zip(footer_oracle.paths(elements), elements, strict=True))
    footer_oracle.put(where[("v", "typed_value")], 3, TType.I32, 0)
    footer_oracle.repeat(tree, 2, TType.LIST, [TType.STRUCT, elements])


def _repeat_chunk_metadata(tree):
    """Each first chunk's metadata twice: the first copy with a 1-byte
    min_value, the second with no statistics."""
    for row_group in footer_oracle.field(tree, 4)[1]:
        chunk = footer_oracle.field(row_group, 1)[1][0]
        meta = footer_oracle.field(chunk, 3)
        first = copy.deepcopy(meta)
        footer_oracle.put(footer_oracle.field(first, 12), 6, TType.STRING, b"\x01")
        second = copy.deepcopy(meta)
        footer_oracle.drop(second, 12)
        footer_oracle.drop(chunk, 3)
        footer_oracle.repeat(chunk, 3, TType.STRUCT, first)
        footer_oracle.repeat(chunk, 3, TType.STRUCT, second)


@pytest.mark.parametrize(
    ("edit", "repeated"),
    [
        # pyarrow's C++ reads a repeated struct field into the first copy,
        # so what only the first sets survives: here unencoded bytes on an
        # INT32 chunk, and a 2-byte min_value, each of which aborted the
        # process; the second copy is all a last-wins reader saw.
        (
            lambda tree: _edit_chunk_tree(
                tree, 0, _repeat_in_chunk(16, [[1, TType.I64, 5]], [])
            ),
            16,
        ),
        (
            lambda tree: _edit_chunk_tree(
                tree,
                0,
                _repeat_in_chunk(
                    12,
                    [[5, TType.STRING, b"\x01\x00"], [6, TType.STRING, b"\x01\x00"]],
                    [[3, TType.I64, 0]],
                ),
            ),
            12,
        ),
        (_repeat_chunk_metadata, 3),
        # Of two schema lists pyarrow reads the last, and the first was
        # the one checked here.
        (_repeat_schema, 2),
    ],
    ids=["size_statistics", "statistics", "chunk_metadata", "schema"],
)
def test_a_footer_struct_that_repeats_a_field_is_refused(edit, repeated):
    """No writer repeats a field id, and the readers disagree on what a
    repeat means, so whichever copy were checked here, pyarrow could read
    another. Each of these pyarrow opens, and aborted the process when it
    read a chunk."""
    forged = footer_oracle.rebuild(NO_NULL.read_bytes(), edit)
    with pq.ParquetFile(pa.BufferReader(forged)) as parquet:
        assert parquet.schema_arrow.names == ["id", "v"]
    _refused_everywhere(forged, f"a Parquet footer struct repeats field {repeated}$")


def _edit_chunk_tree(tree, leaf, edit):
    for row_group in footer_oracle.field(tree, 4)[1]:
        edit(footer_oracle.field(footer_oracle.field(row_group, 1)[1][leaf], 3))


def test_strict_reads_a_converted_type_of_another_wire_type_as_none():
    """A string typed_value whose only annotation is a converted UTF8
    spelled as an i8: pyarrow and parquet-java skip it, and read a plain
    binary, which is not the declared string."""
    column = _variant_column({"type": "string"})
    raw, _ = _written(column, ["a", "b"])

    def edit(element):
        footer_oracle.drop(element, 10)
        footer_oracle.put(element, 6, TType.BYTE, 0)

    forged = footer_oracle.edit_element(raw, ("v", "typed_value"), edit)
    with pq.ParquetFile(pa.BufferReader(forged)) as parquet:
        typed = parquet.schema_arrow.field("v").type.field("typed_value")
        assert typed.type == pa.binary()
    with pytest.raises(ValidationError, match="v is binary, not the declared string"):
        _validate(forged, (column,), strict=True)
    _validate(forged, (column,), strict=False)


@pytest.mark.parametrize(
    "edit",
    [
        lambda chunk: footer_oracle.put(
            footer_oracle.field(chunk, 3), 12, TType.I32, 1
        ),
        lambda chunk: footer_oracle.put(chunk, 8, TType.I32, 1),
        lambda chunk: footer_oracle.put(
            footer_oracle.field(chunk, 3), 16, TType.STRUCT, [[1, TType.STRING, b"x"]]
        ),
    ],
    ids=["statistics", "crypto_metadata", "unencoded_byte_array_data_bytes"],
)
def test_a_chunk_field_of_another_wire_type_reads_as_absent(edit):
    """pyarrow skips each of these, spelled with a wire type parquet.thrift
    does not declare: the INT32 id chunk then has no statistics, is not
    encrypted, and counts no unencoded byte array bytes (which, counted,
    abort the process on an INT32 chunk). The guard skips them as well, so
    it accepts the file, and every chunk reads."""
    raw = _edit_column_chunk(NO_NULL.read_bytes(), 0, edit)
    _read_every_chunk(raw)
    for columns in (NOT_NULL_COLUMNS, DUCKDB_COLUMNS):
        for strict in (False, True):
            _validate(raw, columns, strict=strict)
    _check_chunks(raw)


def test_a_path_in_schema_of_another_item_type_is_refused():
    """pyarrow opens a path_in_schema whose list header says i32 (its
    generated reader reads the item as a string anyway, here an empty one),
    and Python's Thrift would read an int, which no UTF-8 check can take."""
    raw = footer_oracle.edit_chunk(
        NO_NULL.read_bytes(),
        0,
        lambda meta: footer_oracle.put(meta, 3, TType.LIST, [TType.I32, [0]]),
    )
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        assert parquet.schema_arrow.names == ["id", "v"]
    _refused_everywhere(raw, "list holds Thrift type 8, not the declared 11")


#: Every list parquet.thrift declares (as pyarrow's C++ and parquet-java
#: compile it) on the way from FileMetaData to what the footer reader
#: reads, by its path of field ids, with its item type and its name.
PARQUET_THRIFT_LISTS = {
    (2,): (TType.STRUCT, "FileMetaData.schema"),
    (4,): (TType.STRUCT, "FileMetaData.row_groups"),
    (5,): (TType.STRUCT, "FileMetaData.key_value_metadata"),
    (7,): (TType.STRUCT, "FileMetaData.column_orders"),
    (4, 1): (TType.STRUCT, "RowGroup.columns"),
    (4, 4): (TType.STRUCT, "RowGroup.sorting_columns"),
    (4, 1, 3, 2): (TType.I32, "ColumnMetaData.encodings"),
    (4, 1, 3, 3): (TType.STRING, "ColumnMetaData.path_in_schema"),
    (4, 1, 3, 8): (TType.STRUCT, "ColumnMetaData.key_value_metadata"),
    (4, 1, 3, 13): (TType.STRUCT, "ColumnMetaData.encoding_stats"),
    (4, 1, 3, 16, 2): (TType.I64, "SizeStatistics.repetition_level_histogram"),
    (4, 1, 3, 16, 3): (TType.I64, "SizeStatistics.definition_level_histogram"),
    (4, 1, 3, 17, 2): (TType.I32, "GeospatialStatistics.geospatial_types"),
    (4, 1, 8, 2, 1): (TType.STRING, "EncryptionWithColumnKey.path_in_schema"),
}


def _declared_lists(declared, path=()):
    from pyhoglake.parquet_schema import _Each, _Unread

    def read(inside):
        return inside.inside if isinstance(inside, (_Each, _Unread)) else inside

    for field_id, (kind, inside) in declared.items():
        here = (*path, field_id)
        if kind == TType.LIST:
            item_kind, item_inside = read(inside)
            yield here, item_kind
            if item_kind == TType.STRUCT:
                yield from _declared_lists(read(item_inside), here)
        elif kind == TType.STRUCT:
            yield from _declared_lists(read(inside), here)


def test_every_list_parquet_thrift_declares_is_declared():
    """Thrift's generated readers read a list's items as the declared type,
    whatever its header says, so a list the reader does not declare is
    one it reads otherwise than they do (the next test)."""
    from pyhoglake.parquet_schema import _FILE_METADATA

    assert dict(_declared_lists(_FILE_METADATA)) == {
        path: item for path, (item, _) in PARQUET_THRIFT_LISTS.items()
    }


def _spell_list(fields, path, spelled):
    """Set the list at ``path`` (field ids, through every item of a list)
    to the bytes ``spelled``, adding any struct on the way."""
    head, *rest = path
    if not rest:
        footer_oracle.put(fields, head, TType.LIST, footer_oracle.Spelled(spelled))
        return
    entry = next((entry for entry in fields if entry[0] == head), None)
    if entry is None:
        entry = [head, TType.STRUCT, []]
        footer_oracle.put(fields, *entry)
    for struct in entry[2][1] if entry[1] == TType.LIST else [entry[2]]:
        _spell_list(struct, rest, spelled)


#: A list of one item whose header names another type than the declared
#: one, spelled so either reading takes one byte: an empty binary where an
#: integer is declared (its size, 0, is the integer to the generated
#: readers), a 0 where a binary is (its size, an empty one), and a byte 0
#: where a struct is (its STOP).
_ANOTHER_ITEM = {
    TType.I32: bytes([0x18, 0]),
    TType.I64: bytes([0x18, 0]),
    TType.STRING: bytes([0x15, 0]),
    TType.STRUCT: bytes([0x13, 0]),
}


@pytest.mark.parametrize(
    "path",
    PARQUET_THRIFT_LISTS,
    ids=[name for _, name in PARQUET_THRIFT_LISTS.values()],
)
def test_a_list_of_another_item_type_is_refused_wherever_it_is(path):
    from pyhoglake.parquet_schema import _file_metadata

    spelled = _ANOTHER_ITEM[PARQUET_THRIFT_LISTS[path][0]]
    raw = footer_oracle.rebuild(
        NO_NULL.read_bytes(),
        lambda tree: _spell_list(tree, path, spelled),
        encoder=footer_oracle.encode_unchecked,
    )
    with pytest.raises(ValueError, match="list holds Thrift type .*, not the declared"):
        _file_metadata(footer_oracle.split(raw)[1])


#: An empty list of i32s, set or map, each as its compact header.
_EMPTY_CONTAINERS = {
    "list": (TType.LIST, b"\x05"),
    "set": (TType.SET, b"\x05"),
    "map": (TType.MAP, b"\x00"),
}


def _element(tree, path):
    elements = footer_oracle.schema(tree)
    return dict(zip(footer_oracle.paths(elements), elements, strict=True))[path]


def _first_chunk(tree):
    return footer_oracle.field(footer_oracle.field(tree, 4)[1][0], 1)[1][0]


def _key_value(tree):
    footer_oracle.put(tree, 5, TType.LIST, [TType.STRUCT, [[[1, TType.STRING, b"k"]]]])
    return footer_oracle.field(tree, 5)[1][0]


def _undeclared_struct(tree):
    footer_oracle.put(tree, 100, TType.STRUCT, [])
    return footer_oracle.field(tree, 100)


#: A struct of each kind the footer reader reads, or reads past, on the way
#: to what it checks, found in a footer's tree, and a field id
#: parquet.thrift does not define in it.
UNDECLARED_SITES = {
    "FileMetaData": (lambda tree: tree, 100),
    "SchemaElement": (lambda tree: _element(tree, ("v",)), 11),
    "VariantType": (
        lambda tree: footer_oracle.field(
            footer_oracle.field(_element(tree, ("v",)), 10), 16
        ),
        2,
    ),
    "RowGroup": (lambda tree: footer_oracle.field(tree, 4)[1][0], 9),
    "ColumnChunk": (_first_chunk, 10),
    "ColumnMetaData": (lambda tree: footer_oracle.field(_first_chunk(tree), 3), 18),
    "Statistics": (
        lambda tree: footer_oracle.field(
            footer_oracle.field(_first_chunk(tree), 3), 12
        ),
        10,
    ),
    "KeyValue": (_key_value, 3),
    "an undeclared struct": (_undeclared_struct, 1),
}


def _undeclared(site, kind, value):
    """A footer edit adding to the struct at ``site`` its undeclared field."""
    locate, field_id = UNDECLARED_SITES[site]
    return lambda tree: footer_oracle.put(locate(tree), field_id, kind, value)


@pytest.mark.parametrize("container", sorted(_EMPTY_CONTAINERS))
@pytest.mark.parametrize("site", sorted(UNDECLARED_SITES))
def test_a_container_parquet_thrift_does_not_declare_is_refused(site, container):
    """A field parquet.thrift does not define is skipped as its header spells
    it, as the generated readers skip it today. But if a later
    parquet.thrift declares it a list, pyarrow reads the items as the
    declared type whatever the header says, and a header of another item
    type hides fields from the guard (as an encodings list hid Statistics,
    above). So a list, a set or a map that is not declared is refused
    wherever it is, which no writer's footer holds today (the fixtures all
    pass). The cost falls on users: once a writer spells one by default,
    as pyarrow came to spell SizeStatistics (whose histograms are lists,
    on every chunk), every file it writes is refused by a pyhoglake older
    than the field, valid as it is, until pyhoglake is upgraded or the
    writer pinned; the 'latest' pyarrow canary only warns of it, a week
    after the release. So the refusal names where the container is and
    says that, rather than call the file malformed."""
    kind, spelled = _EMPTY_CONTAINERS[container]
    raw = footer_oracle.rebuild(
        NO_NULL.read_bytes(),
        _undeclared(site, kind, footer_oracle.Spelled(spelled)),
        encoder=footer_oracle.encode_unchecked,
    )
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        assert parquet.schema_arrow.names == ["id", "v"]
    field_id = UNDECLARED_SITES[site][1]
    where = (
        "field 100 of a FileMetaData"
        if site == "an undeclared struct"
        else f"field {field_id} of a {site}"
    )
    _refused_everywhere(
        raw,
        f"holds a {container} in {where}, which this pyhoglake's parquet.thrift "
        r"does not define \(a writer newer than this pyhoglake may write one: "
        "upgrade pyhoglake",
    )


def test_a_struct_parquet_thrift_does_not_declare_is_read_past():
    """The same sites with an undeclared struct of scalars and a binary,
    which no later declaration can make a list: skipped, and every chunk
    reads."""
    scalars = [[1, TType.I32, 7], [2, TType.STRING, b"\xff"], [3, TType.STRUCT, []]]
    for site in UNDECLARED_SITES:
        raw = footer_oracle.rebuild(
            NO_NULL.read_bytes(), _undeclared(site, TType.STRUCT, scalars)
        )
        _read_every_chunk(raw)
        for strict in (False, True):
            _validate(raw, DUCKDB_COLUMNS, strict=strict)
        _check_chunks(raw)


#: Every struct parquet.thrift declares on the way from FileMetaData to what
#: the footer reader reads, as arrow 21's copy declares it, by name, with
#: the wire type of each field it declares; and the unions among them. A
#: later parquet.thrift adds fields (Statistics' nan_count, ColumnOrder's
#: IEEE754TotalOrder), of which a list, a set or a map is then refused as
#: a newer writer's, and anything else read past as undefined.
PARQUET_THRIFT_STRUCTS = {
    "FileMetaData": {
        1: TType.I32,
        2: TType.LIST,
        3: TType.I64,
        4: TType.LIST,
        5: TType.LIST,
        6: TType.STRING,
        7: TType.LIST,
        8: TType.STRUCT,
        9: TType.STRING,
    },
    "SchemaElement": {
        **dict.fromkeys((1, 2, 3, 5, 6, 7, 8, 9), TType.I32),
        4: TType.STRING,
        10: TType.STRUCT,
    },
    "LogicalType": dict.fromkeys((*range(1, 9), *range(10, 19)), TType.STRUCT),
    **{
        name: {}
        for name in (
            "StringType",
            "MapType",
            "ListType",
            "EnumType",
            "DateType",
            "NullType",
            "JsonType",
            "BsonType",
            "UUIDType",
            "Float16Type",
            "MilliSeconds",
            "MicroSeconds",
            "NanoSeconds",
            "TypeDefinedOrder",
            "IEEE754TotalOrder",
            "EncryptionWithFooterKey",
        )
    },
    "DecimalType": {1: TType.I32, 2: TType.I32},
    "TimeType": {1: TType.BOOL, 2: TType.STRUCT},
    "TimestampType": {1: TType.BOOL, 2: TType.STRUCT},
    "TimeUnit": dict.fromkeys((1, 2, 3), TType.STRUCT),
    "IntType": {1: TType.BYTE, 2: TType.BOOL},
    "VariantType": {1: TType.BYTE},
    "GeometryType": {1: TType.STRING},
    "GeographyType": {1: TType.STRING, 2: TType.I32},
    "RowGroup": {
        1: TType.LIST,
        2: TType.I64,
        3: TType.I64,
        4: TType.LIST,
        5: TType.I64,
        6: TType.I64,
        7: TType.I16,
    },
    "ColumnChunk": {
        1: TType.STRING,
        2: TType.I64,
        3: TType.STRUCT,
        4: TType.I64,
        5: TType.I32,
        6: TType.I64,
        7: TType.I32,
        8: TType.STRUCT,
        9: TType.STRING,
    },
    "ColumnMetaData": {
        1: TType.I32,
        2: TType.LIST,
        3: TType.LIST,
        4: TType.I32,
        **dict.fromkeys((5, 6, 7, 9, 10, 11, 14), TType.I64),
        8: TType.LIST,
        12: TType.STRUCT,
        13: TType.LIST,
        15: TType.I32,
        16: TType.STRUCT,
        17: TType.STRUCT,
    },
    "Statistics": {
        **dict.fromkeys((1, 2, 5, 6), TType.STRING),
        3: TType.I64,
        4: TType.I64,
        7: TType.BOOL,
        8: TType.BOOL,
    },
    "SizeStatistics": {1: TType.I64, 2: TType.LIST, 3: TType.LIST},
    "GeospatialStatistics": {1: TType.STRUCT, 2: TType.LIST},
    "BoundingBox": dict.fromkeys(range(1, 9), TType.DOUBLE),
    "KeyValue": {1: TType.STRING, 2: TType.STRING},
    "SortingColumn": {1: TType.I32, 2: TType.BOOL, 3: TType.BOOL},
    "PageEncodingStats": {1: TType.I32, 2: TType.I32, 3: TType.I32},
    # IEEE754TotalOrder (2) is parquet-format 2.12's, after arrow 21's copy:
    # parquet-java 1.18.1 reads it, and refuses it on an integer column.
    "ColumnOrder": {1: TType.STRUCT, 2: TType.STRUCT},
    "ColumnCryptoMetaData": {1: TType.STRUCT, 2: TType.STRUCT},
    "EncryptionWithColumnKey": {1: TType.LIST, 2: TType.STRING},
    "EncryptionAlgorithm": {1: TType.STRUCT, 2: TType.STRUCT},
    "AesGcmV1": {1: TType.STRING, 2: TType.STRING, 3: TType.BOOL},
    "AesGcmCtrV1": {1: TType.STRING, 2: TType.STRING, 3: TType.BOOL},
}
PARQUET_THRIFT_UNIONS = {
    "LogicalType",
    "TimeUnit",
    "ColumnOrder",
    "ColumnCryptoMetaData",
    "EncryptionAlgorithm",
}


def _declared_structs(declared, found):
    from pyhoglake.parquet_schema import _Declared, _Each, _Unread

    def read(inside):
        return inside.inside if isinstance(inside, (_Each, _Unread)) else inside

    found[declared.name] = (
        {k: kind for k, (kind, _) in declared.items()},
        declared.union,
    )
    for kind, inside in declared.values():
        inside = read(inside)
        if kind == TType.LIST:
            inside = read(inside[1])
        if isinstance(inside, _Declared):
            _declared_structs(inside, found)
    return found


def test_every_struct_field_parquet_thrift_declares_is_declared():
    """A field the reader does not declare is one it reads as
    parquet.thrift leaving it undefined: a list there is refused as one a
    newer writer adds. So every field parquet.thrift does declare, read or
    not, is declared with its wire type, and a mistyped one is skipped by
    its header, as Thrift's generated readers skip it (the next test); and
    every union is marked one, which must spell one field."""
    from pyhoglake.parquet_schema import _FILE_METADATA

    found = _declared_structs(_FILE_METADATA, {})
    assert {name: fields for name, (fields, _) in found.items()} == (
        PARQUET_THRIFT_STRUCTS
    )
    assert {name for name, (_, union) in found.items() if union} == (
        PARQUET_THRIFT_UNIONS
    )


def _statistics(tree):
    meta = footer_oracle.field(_first_chunk(tree), 3)
    if footer_oracle.field(meta, 12) is None:
        footer_oracle.put(meta, 12, TType.STRUCT, [])
    return footer_oracle.field(meta, 12)


#: Fields parquet.thrift declares and no check reads, each where it is found.
DECLARED_UNREAD_SITES = {
    "Statistics.null_count": (_statistics, 3),
    "Statistics.is_max_value_exact": (_statistics, 7),
    "ColumnMetaData.bloom_filter_offset": (
        lambda tree: footer_oracle.field(_first_chunk(tree), 3),
        14,
    ),
    "ColumnChunk.offset_index_offset": (_first_chunk, 4),
    "RowGroup.ordinal": (lambda tree: footer_oracle.field(tree, 4)[1][0], 7),
    "KeyValue.value": (_key_value, 2),
    "FileMetaData.footer_signing_key_metadata": (lambda tree: tree, 9),
}


@pytest.mark.parametrize("site", sorted(DECLARED_UNREAD_SITES))
def test_a_declared_field_spelled_as_a_list_is_skipped(site):
    """Each spelled as a list of one i64, a wire type parquet.thrift does
    not declare for it: pyarrow skips it by its header (a null count so
    spelled is no null count to it), and so does the guard, which accepts
    the file, every chunk of which reads. It is not a field parquet.thrift
    leaves undefined, which a list would be refused in."""
    locate, field_id = DECLARED_UNREAD_SITES[site]
    raw = footer_oracle.rebuild(
        NO_NULL.read_bytes(),
        lambda tree: footer_oracle.put(
            locate(tree), field_id, TType.LIST, [TType.I64, [1]]
        ),
    )
    if site == "Statistics.null_count":
        with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
            assert not parquet.metadata.row_group(0).column(0).statistics.has_null_count
    _read_every_chunk(raw)
    for strict in (False, True):
        _validate(raw, DUCKDB_COLUMNS, strict=strict)
    _check_chunks(raw)


def _logical_type(path, members):
    """An edit of NO_NULL setting the logicalType of the element at
    ``path`` to the union fields ``members``."""
    return lambda raw: footer_oracle.edit_element(
        raw, path, lambda element: footer_oracle.put(element, 10, TType.STRUCT, members)
    )


_VARIANT_1 = [16, TType.STRUCT, [[1, TType.BYTE, 1]]]
_INT_32 = [10, TType.STRUCT, [[1, TType.BYTE, 32], [2, TType.BOOL, True]]]


def _two_column_orders(raw):
    def edit(tree):
        footer_oracle.field(tree, 7)[1][0].append([2, TType.STRUCT, []])

    return footer_oracle.rebuild(raw, edit)


#: A union of more fields, or fewer, than one, each in a footer pyarrow
#: opens, and the count the refusal names.
UNIONS_NOT_OF_ONE = {
    # A STRING spelled as a bool, then VARIANT(1): pyarrow skips the bool,
    # and shows Variant(1).
    "group_mistyped_first": (
        _logical_type(("v",), [[1, TType.BOOL, True], _VARIANT_1]),
        "LogicalType union holds 2 fields",
    ),
    "group_mistyped_after": (
        _logical_type(("v",), [_VARIANT_1, [17, TType.I32, 0]]),
        "LogicalType union holds 2 fields",
    ),
    "leaf_mistyped_first": (
        _logical_type(("v", "typed_value"), [[1, TType.BOOL, True], _INT_32]),
        "LogicalType union holds 2 fields",
    ),
    "column_of_none": (
        _logical_type(("id",), []),
        "LogicalType union holds 0 fields",
    ),
    "column_order_of_two": (_two_column_orders, "ColumnOrder union holds 2 fields"),
}


@pytest.mark.parametrize("case", sorted(UNIONS_NOT_OF_ONE))
def test_a_union_not_of_one_field_is_refused(case):
    """pyarrow's C++ reads a Thrift union as a struct: every field of it,
    skipping one of another wire type. parquet-java's TUnion (libthrift,
    the server's hydrator through parquet-hadoop, and the Trino connector's
    MetadataReader) reads the first field, a mistyped one as none, and then
    takes the next field header for the union's end, so the rest of the
    footer is read out of step, and an empty union throws ("Unrecognized
    type 0"). The file is one annotation to pyarrow and one parquet-java
    cannot read at all (``native_variant_union_two_fields.parquet`` pins
    that, in FooterStatsTest), so it is refused on every path."""
    edit, fault = UNIONS_NOT_OF_ONE[case]
    raw = edit(NO_NULL.read_bytes())
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        assert parquet.schema_arrow.names == ["id", "v"]
        if case.startswith("group"):
            assert "v (Variant(1))" in str(parquet.schema)
    _refused_everywhere(raw, fault)


def test_a_union_the_footer_reader_does_not_reach_a_check_through_is_held_too():
    """The unions no check reads inside: a TimeUnit (a TIMESTAMP leaf's,
    whose bool pyarrow skips), a ColumnCryptoMetaData and an
    EncryptionAlgorithm, refused as the footer is read."""
    from pyhoglake.parquet_schema import _file_metadata

    sink = pa.BufferOutputStream()
    pq.write_table(pa.table({"t": pa.array([1], pa.timestamp("us", "UTC"))}), sink)

    def unit(element):
        timestamp = footer_oracle.field(footer_oracle.field(element, 10), 8)
        footer_oracle.put(
            timestamp, 2, TType.STRUCT, [[1, TType.BOOL, True], [2, TType.STRUCT, []]]
        )

    raw = footer_oracle.edit_element(sink.getvalue().to_pybytes(), ("t",), unit)
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        assert parquet.schema_arrow.field("t").type == pa.timestamp("us", "UTC")
    with pytest.raises(ValidationError, match="TimeUnit union holds 2 fields"):
        _check_chunks(raw)
    for edit, fault in (
        (
            lambda tree: footer_oracle.put(
                _first_chunk(tree),
                8,
                TType.STRUCT,
                [[1, TType.STRUCT, []], [2, TType.STRUCT, []]],
            ),
            "ColumnCryptoMetaData union holds 2 fields",
        ),
        (
            lambda tree: footer_oracle.put(tree, 8, TType.STRUCT, []),
            "EncryptionAlgorithm union holds 0 fields",
        ),
    ):
        forged = footer_oracle.rebuild(NO_NULL.read_bytes(), edit)
        with pytest.raises(ValueError, match=fault):
            _file_metadata(footer_oracle.split(forged)[1])


@pytest.mark.parametrize(
    "members",
    [[[10, TType.BOOL, True]], [[19, TType.STRUCT, []]]],
    ids=["mistyped", "undefined"],
)
def test_a_logical_type_of_no_member_a_reader_parses_is_refused(members):
    """An INTEGER spelled as a bool, or a member parquet.thrift does not
    define, alone: pyarrow reads no logical type, the plain int32 the
    column is, and parquet-java's TUnion reads a union with no member set,
    which the server's hydrator reads past to the converted type beside it
    (``native_variant_lone_member.parquet`` pins that in FooterStatsTest).
    The Trino connector cannot read the footer at all: its
    getLogicalTypeAnnotation switches over the member set, with no case for
    none, and throws NullPointerException, for every query that reaches the
    file. So it is refused on every path, as an annotation the readers
    read apart is (below)."""
    raw = _logical_type(("id",), members)(NO_NULL.read_bytes())
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        assert parquet.schema_arrow.field("id").type == pa.int32()
    _read_every_chunk(raw)
    _refused_everywhere(
        raw, r"schema element 1 \('id'\) has a logical type of no member"
    )


def _column_order(leaf, member):
    """A footer edit giving leaf column ``leaf`` the ColumnOrder union of
    ``member`` alone."""

    def edit(tree):
        footer_oracle.field(tree, 7)[1][leaf][:] = [[member, TType.STRUCT, []]]

    return edit


def test_a_column_order_of_a_member_parquet_thrift_does_not_define_is_accepted():
    """A ColumnOrder of a member neither parquet.thrift defines (3), alone:
    pyarrow and parquet-java read an order they do not know as undefined,
    and the Trino connector does not read column orders, so unlike a
    LogicalType of no member it is accepted."""
    forged = footer_oracle.rebuild(NO_NULL.read_bytes(), _column_order(0, 3))
    _read_every_chunk(forged)
    for strict in (False, True):
        _validate(forged, DUCKDB_COLUMNS, strict=strict)
    _check_chunks(forged)


@pytest.mark.parametrize(
    ("leaf", "fault"),
    [(0, r"schema element 1 \('id'\)"), (3, r"schema element 5 \('typed_value'\)")],
    ids=["column", "variant_leaf"],
)
def test_an_ieee754_order_on_a_column_parquet_java_takes_it_for_none_is_refused(
    leaf, fault
):
    """IEEE754TotalOrder (ColumnOrder member 2), which parquet-format 2.12
    added: pyarrow reads the footer, and the Trino connector reads no column
    order, but parquet-java's schema builder (the server's hydrator's) takes
    it on a FLOAT, a DOUBLE or a FLOAT16 alone, and throws on an INT32 ("The
    column order IEEE_754_TOTAL_ORDER is not supported by type INT32")."""
    raw = footer_oracle.rebuild(NO_NULL.read_bytes(), _column_order(leaf, 2))
    _read_every_chunk(raw)
    _refused_everywhere(raw, f"{fault} has the column order IEEE_754_TOTAL_ORDER")


def test_an_ieee754_order_on_a_floating_point_column_is_accepted():
    """On a FLOAT16, a FLOAT and a DOUBLE, the types IEEE754TotalOrder is
    for, every reader takes it."""
    # 1.5 and 2.0 as IEEE half floats, little-endian.
    half = pa.Array.from_buffers(
        pa.float16(), 2, [None, pa.py_buffer(b"\x00\x3e\x00\x40")]
    )
    table = pa.table(
        {"h": half, "f": pa.array([1.0, 2.0], pa.float32()), "d": pa.array([1.0, 2.0])}
    )
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    raw = sink.getvalue().to_pybytes()
    for leaf in range(3):
        raw = footer_oracle.rebuild(raw, _column_order(leaf, 2))
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        assert parquet.schema_arrow.field("h").type == pa.float16()
    _read_every_chunk(raw)
    _check_chunks(raw)
    # The same order on the FLOAT16's fixed-length array, unannotated.
    bare = footer_oracle.edit_element(
        raw, ("h",), lambda element: footer_oracle.drop(element, 10)
    )
    with pytest.raises(ValidationError, match=r"\('h'\) has the column order"):
        _check_chunks(bare)


def _element_fields(path, **fields):
    """An edit of NO_NULL setting the given fields of the element at
    ``path``: ``converted`` (6), ``scale`` (7), ``precision`` (8) as i32s
    and ``logical`` (10) as the union's members."""
    ids = {"converted": 6, "scale": 7, "precision": 8, "logical": 10}

    def edit(element):
        for name, value in fields.items():
            kind = TType.STRUCT if name == "logical" else TType.I32
            footer_oracle.put(element, ids[name], kind, value)

    return lambda raw: footer_oracle.edit_element(raw, path, edit)


_DECIMAL_9_2 = [[5, TType.STRUCT, [[1, TType.I32, 2], [2, TType.I32, 9]]]]

#: Annotations pyarrow reads and parquet-java (the server's hydrator, and
#: the connector's ParquetMetadata) reads as another type or cannot build,
#: on NO_NULL's leaves (id an INT32 with the converted INT_32 alone, v.value
#: a plain BYTE_ARRAY, v.typed_value an INT32 with INT_32), with what
#: pyarrow reads and the refusal.
ANNOTATIONS_READ_APART = {
    # parquet-java reads INT(32, true), pyarrow a date.
    "logical_beside_converted": (
        _element_fields(("id",), logical=[[6, TType.STRUCT, []]]),
        ("id", pa.date32()),
        (
            r"schema element 1 \('id'\) has the logical type DateType beside the "
            "converted type INT_32, which spell different types"
        ),
    ),
    # A converted DATE beside a STRING: parquet-java lets the DATE win, and
    # then cannot put it on a BYTE_ARRAY.
    "converted_date_beside_string": (
        _element_fields(("v", "value"), converted=6, logical=[[1, TType.STRUCT, []]]),
        None,
        r"\('value'\) has the logical type StringType beside the converted type DATE",
    ),
    "decimal_scale_field": (
        _element_fields(
            ("v", "typed_value"),
            converted=5,
            scale=3,
            precision=9,
            logical=_DECIMAL_9_2,
        ),
        None,
        (
            r"\('typed_value'\) has a DECIMAL logical type its scale or precision "
            "field contradicts"
        ),
    ),
    "decimal_precision_field": (
        _element_fields(
            ("v", "typed_value"), converted=5, precision=8, logical=_DECIMAL_9_2
        ),
        None,
        "has a DECIMAL logical type its scale or precision field contradicts",
    ),
    # A converted DECIMAL alone, of 10 digits, which pyarrow reads from an
    # INT32 and parquet-java will not build ("INT32 cannot store 10 digits").
    "converted_decimal_too_wide": (
        _element_fields(("v", "typed_value"), converted=5, precision=10),
        None,
        (
            r"\('typed_value'\) is annotated DECIMAL\(10, 0\), which parquet-java .* "
            "cannot apply to INT32"
        ),
    ),
}


@pytest.mark.parametrize("case", sorted(ANNOTATIONS_READ_APART))
def test_an_annotation_the_readers_read_apart_is_refused_everywhere(case):
    """pyarrow reads an element's logical type where it has one, and its
    converted type only where it has not. parquet-java reads the annotation
    the connector's ``ParquetMetadata.readTypeSchema`` does: it lets a
    converted type that spells another type win over the logical one, and
    its schema builder throws on an annotation that does not fit the
    element. Each of these files was published on every path, strict
    included, to be read as another type or not at all."""
    edit, reads, fault = ANNOTATIONS_READ_APART[case]
    raw = edit(NO_NULL.read_bytes())
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        if reads is not None:
            assert parquet.schema_arrow.field(reads[0]).type == reads[1]
    _read_every_chunk(raw)
    _refused_everywhere(raw, fault)


@pytest.mark.parametrize(
    "logical",
    [
        {8: {2: {2: {}}}},  # a TIMESTAMP without isAdjustedToUTC
        {7: {1: True, 2: {}}},  # a TIME of no unit
        {10: {1: 7, 2: True}},  # an INTEGER of 7 bits
    ],
    ids=["no_flag", "no_unit", "width"],
)
def test_a_logical_type_thrift_or_parquet_java_cannot_read_is_refused(logical):
    """Thrift's generated readers refuse a TIME or TIMESTAMP without a field
    it requires, and parquet-java an INTEGER of a width it does not build;
    pyarrow refuses each file too, so this schema list is given directly."""
    from pyhoglake.parquet_schema import _schema_fault

    elements = [{4: "schema", 5: 1}, {1: 2, 3: 1, 4: "c", 10: logical}]
    assert "logical type Thrift or parquet-java cannot read" in (
        _schema_fault(elements) or ""
    )


def test_a_fixed_length_decimal_past_128_bytes_holds_any_precision():
    """parquet-java bounds a fixed-length DECIMAL's precision by
    floor(log10(2 ** (8 * length - 1) - 1)) computed in doubles, which
    overflow from 129 bytes: there it builds any precision, up to the i32's
    largest (measured against parquet-column 1.18.1 for 1 to 200 bytes),
    and the bound is not computed at all, however long the array says."""
    from pyhoglake.parquet_schema import _schema_fault

    for length, precision, fault in (
        (128, 307, False),
        (128, 308, True),
        (129, 2**31 - 1, False),
        (2**31 - 1, 2**31 - 1, False),
    ):
        decimal = {5: {1: 0, 2: precision}}
        leaf = {1: 7, 2: length, 3: 1, 4: "c", 10: decimal}
        assert (_schema_fault([{4: "schema", 5: 1}, leaf]) is not None) == fault


def test_a_timestamp_parquet_java_reads_in_another_unit_is_refused():
    """A pyarrow timestamp[us, UTC] column whose converted type is made
    TIMESTAMP_MILLIS: pyarrow reads the logical TIMESTAMP(true, MICROS),
    and so its statistics, while parquet-java and the connector read the
    values as milliseconds, a thousand times too late, without a word."""
    table = pa.table(
        {"t": pa.array([datetime(2024, 1, 1, tzinfo=UTC)], pa.timestamp("us", "UTC"))}
    )
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    raw = footer_oracle.edit_element(
        sink.getvalue().to_pybytes(),
        ("t",),
        lambda element: footer_oracle.put(element, 6, TType.I32, 9),
    )
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        assert parquet.schema_arrow.field("t").type == pa.timestamp("us", "UTC")
    assert pq.read_table(pa.BufferReader(raw)).equals(table)
    with pytest.raises(
        ValidationError,
        match="TimestampType beside the converted type TIMESTAMP_MILLIS",
    ):
        _check_chunks(raw)


#: Pinned exactly, with SchemaAnnotationVectorFile.EXPECTED_COUNT in the
#: server's suite, so a vector lost to a bad merge fails on both sides.
ANNOTATION_VECTOR_COUNT = 1643

_PHYSICAL = {
    "BOOLEAN": 0,
    "INT32": 1,
    "INT64": 2,
    "INT96": 3,
    "FLOAT": 4,
    "DOUBLE": 5,
    "BYTE_ARRAY": 6,
    "FIXED_LEN_BYTE_ARRAY": 7,
}
_MEMBERS = {
    "STRING": 1,
    "MAP": 2,
    "LIST": 3,
    "ENUM": 4,
    "DECIMAL": 5,
    "DATE": 6,
    "TIME": 7,
    "TIMESTAMP": 8,
    "INTEGER": 10,
    "UNKNOWN": 11,
    "JSON": 12,
    "BSON": 13,
    "UUID": 14,
    "FLOAT16": 15,
    "VARIANT": 16,
    "GEOMETRY": 17,
    "GEOGRAPHY": 18,
}


def _vector_footer(vector):
    """The footer an annotation vector describes, spelled as the server's
    SchemaAnnotationVectorFileTest builds it from the same fields."""
    from pyhoglake.parquet_schema import _CONVERTED_NAMES

    ((name, params),) = (vector.get("logical") or {"": {}}).items()
    units = {"MILLIS": 1, "MICROS": 2, "NANOS": 3}
    member = []
    if name == "DECIMAL":
        member = [[1, TType.I32, params["scale"]], [2, TType.I32, params["precision"]]]
    elif name in ("TIME", "TIMESTAMP"):
        unit = [[units[params["unit"]], TType.STRUCT, []]]
        member = [[1, TType.BOOL, params["isAdjustedToUTC"]], [2, TType.STRUCT, unit]]
    elif name == "INTEGER":
        member = [
            [1, TType.BYTE, params["bitWidth"]],
            [2, TType.BOOL, params["isSigned"]],
        ]
    elif name == "VARIANT":
        member = [[1, TType.BYTE, params["specification_version"]]]
    group = vector["physical"] == "GROUP"
    element = [] if group else [[1, TType.I32, _PHYSICAL[vector["physical"]]]]
    if "length" in vector:
        element.append([2, TType.I32, vector["length"]])
    element += [[3, TType.I32, 1], [4, TType.STRING, b"c"]]
    if group:
        element.append([5, TType.I32, 1])
    if "converted" in vector:
        converted = _CONVERTED_NAMES.index(vector["converted"])
        element.append([6, TType.I32, converted])
    for field_id, key in ((7, "scale"), (8, "precision")):
        if key in vector:
            element.append([field_id, TType.I32, vector[key]])
    if name:
        element.append([10, TType.STRUCT, [[_MEMBERS[name], TType.STRUCT, member]]])
    schema = [[[4, TType.STRING, b"schema"], [5, TType.I32, 1]], element]
    if group:
        schema.append([[1, TType.I32, 6], [3, TType.I32, 1], [4, TType.STRING, b"x"]])
    return footer_oracle.encode(
        [
            [1, TType.I32, 1],
            [2, TType.LIST, [TType.STRUCT, schema]],
            [3, TType.I64, 0],
            [4, TType.LIST, [TType.STRUCT, []]],
        ]
    )


def test_an_annotation_is_refused_exactly_where_parquet_java_reads_it_otherwise():
    """The shared vector file (tests/vectors/schema_annotation_vectors.json)
    gives, for one element of each pairing of physical type (or group),
    logical type, converted type and DECIMAL fields, how parquet-java reads
    it, which the server's SchemaAnnotationVectorFileTest asserts against
    parquet-java itself: as pyarrow does (``reads``), with the converted
    type winning over the logical one (``reads_converted``), or not at all
    (``refuses``). Every prepared-file path refuses the element exactly
    when parquet-java does not read it as pyarrow does."""
    from pyhoglake.parquet_schema import _schema_fault

    document = json.loads(
        (
            Path(__file__).parent / "vectors" / "schema_annotation_vectors.json"
        ).read_text()
    )
    assert document["format"] == "hoglake-schema-annotation-vectors"
    assert document["version"] == 1
    vectors = document["vectors"]
    assert len(vectors) == ANNOTATION_VECTOR_COUNT
    assert len({vector["id"] for vector in vectors}) == len(vectors)
    assert {vector["parquet_java"] for vector in vectors} == {
        "reads",
        "reads_converted",
        "refuses",
    }
    wrong = []
    for vector in vectors:
        elements = _schema_elements_from_footer(_vector_footer(vector))
        fault = _schema_fault(elements)
        if (fault is None) != (vector["parquet_java"] == "reads"):
            wrong.append((vector["id"], vector["parquet_java"], fault))
    assert not wrong


def _typed(raw, path):
    """``raw`` with the element at ``path`` given the physical type INT32."""
    return footer_oracle.edit_element(
        raw, path, lambda element: footer_oracle.put(element, 1, TType.I32, 1)
    )


def _struct_and_list():
    """A file of ``s`` struct<x int>, ``l`` list<int>, and ``n``, and the
    catalog columns it is written from."""
    columns = (
        Column("s", "struct", 1, 0, True, children=(Column("x", "int", 2, 0, True),)),
        Column(
            "l", "list", 3, 1, True, children=(Column("element", "int", 4, 0, True),)
        ),
        Column("n", "int", 5, 2, True),
    )
    rows = [{"s": {"x": 1}, "l": [1, 2], "n": 5}, {"s": None, "l": None, "n": 6}]
    sink = pa.BufferOutputStream()
    pq.write_table(
        pa.Table.from_pylist(rows, schema=columns_to_arrow_schema(columns)), sink
    )
    return sink.getvalue().to_pybytes(), columns


@pytest.mark.parametrize(
    ("path", "index"),
    [(("s",), 1), (("l",), 3), (("l", "list"), 4)],
    ids=["struct", "list", "repeated_group"],
)
def test_a_group_with_a_physical_type_is_refused(path, index):
    """pyarrow reads an element with children as a group whatever physical
    type it also carries; parquet-java (the server's hydrator, through
    FooterParse) and the Trino connector's ``ParquetMetadata`` read any
    element with a type as a primitive, and cannot read the footer
    ("Arrived at primitive node", "LIST can not be applied to a primitive
    type"). Every one of these files matched its schema and was published,
    to fail every query that reached it. So a group with a type is refused
    on every path, as a VARIANT group was (and one below a VARIANT group
    is: ``native_variant_typed_subgroup.parquet``)."""
    raw, columns = _struct_and_list()
    raw = _typed(raw, path)
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        assert parquet.schema_arrow.names == ["s", "l", "n"]
        assert prepared_schema_matches(
            parquet.schema_arrow, columns_to_arrow_schema(columns)
        )
    fault = f"schema element {index} \\({path[-1]!r}\\) is a group with a physical"
    for strict in (False, True):
        with pytest.raises(ValidationError, match=fault):
            _validate(raw, columns, strict=strict)
    with pytest.raises(ValidationError, match=fault):
        _check_chunks(raw)


def test_a_leaf_with_no_children_and_a_type_is_no_group():
    """num_children 0 on a leaf is a leaf to pyarrow and to parquet-java
    alike, so it is not refused as a group with a type."""
    raw, columns = _struct_and_list()
    raw = footer_oracle.edit_element(
        raw, ("s", "x"), lambda element: footer_oracle.put(element, 5, TType.I32, 0)
    )
    _read_every_chunk(raw)
    for strict in (False, True):
        _validate(raw, columns, strict=strict)
    _check_chunks(raw)


def test_a_group_with_a_physical_type_is_a_group_to_the_leaf_count():
    """pyarrow reads an element with children as a group, so the leaves
    after one with a type too are counted as it counts them (the file is
    refused above, but the count must not depend on that)."""
    raw, _ = _struct_and_list()
    elements = _elements_of(_typed(raw, ("s",)))
    assert [leaves for _, leaves, _ in _top_level(elements)] == [
        range(1),
        range(1, 2),
        range(2, 3),
    ]
    assert [element[4] for element, _, _ in _leaf_levels(elements)] == [
        "x",
        "element",
        "n",
    ]


def _swallowed_by_a_misread_map(bounds):
    """NO_NULL with the id chunk's ColumnMetaData.encoding_stats (declared
    a list, so Thrift's generated readers skip it by its header) spelled
    as a map of one i32 key and a binary value, followed by Statistics with
    ``bounds``, then a 9-byte binary of an undefined field. The key is the
    Statistics' length plus one: a skip that read it as the value's type,
    a binary, would take that many bytes, the Statistics among them, and
    the field after them reads alike either way."""
    statistics = footer_oracle.long_form(
        [[12, TType.STRUCT, [[5, TType.STRING, bounds], [6, TType.STRING, bounds]]]]
    )
    key = bytes([1 + len(statistics)])
    spelled_map = (
        bytes([0x0B]) + footer_oracle._zigzag(13) + b"\x01\x58" + key + b"\x00"
    )
    realign = bytes([0x08]) + footer_oracle._zigzag(100) + b"\x05abcde"

    def edit(tree):
        chunk = _first_chunk(tree)
        meta = [
            entry for entry in footer_oracle.field(chunk, 3) if entry[0] not in (12, 13)
        ]
        footer_oracle.put(
            chunk,
            3,
            TType.STRUCT,
            footer_oracle.Spelled(
                footer_oracle.long_form(meta)
                + spelled_map
                + statistics
                + realign
                + b"\x00"
            ),
        )

    return footer_oracle.rebuild(
        NO_NULL.read_bytes(), edit, encoder=footer_oracle.encode_unchecked
    )


def _swallowed_by_a_misread_double(bounds):
    """NO_NULL with the id chunk's ColumnMetaData given an undefined struct
    field holding a DOUBLE, 0.5, then Statistics with ``bounds``. Read as
    the varint of an i64, the double's first byte would end it, and its
    other seven be read as fields."""
    statistics = [[5, TType.STRING, bounds], [6, TType.STRING, bounds]]

    def edit(meta):
        footer_oracle.drop(meta, 12)
        footer_oracle.put(meta, 100, TType.STRUCT, [[1, TType.DOUBLE, 0.5]])
        footer_oracle.repeat(meta, 12, TType.STRUCT, statistics)

    return footer_oracle.edit_chunk(
        NO_NULL.read_bytes(), 0, edit, encoder=footer_oracle.encode_unchecked
    )


@pytest.mark.parametrize(
    "forge",
    [_swallowed_by_a_misread_map, _swallowed_by_a_misread_double],
    ids=["map", "double"],
)
def test_a_value_skipped_by_its_header_is_read_as_its_types(forge):
    """A field skipped by its header is read as the Thrift types its header
    names, each item of a map as its key's or its value's, a double as its
    eight bytes: a skip that read one as another would read the bytes after
    it out of step with pyarrow, which here would hide from the guard the
    1-byte bounds that pyarrow aborts on reading the INT32 chunk's
    statistics. With full-width bounds the file is accepted, and every
    chunk reads."""
    sound = forge(b"\x01\x00\x00\x00")
    _read_every_chunk(sound)
    for strict in (False, True):
        _validate(sound, DUCKDB_COLUMNS, strict=strict)
    _check_chunks(sound)
    with pq.ParquetFile(pa.BufferReader(sound)) as parquet:
        assert parquet.metadata.row_group(0).column(0).statistics.min == 1
    _refused_everywhere(forge(b"\x01"), "1-byte statistic of a 4-byte type")


def test_a_footer_is_checked_in_memory_linear_in_its_bytes():
    """A footer of 4,000 column chunks in 50 row groups, a key_value_metadata
    of 20,000 KeyValues, an undeclared struct of 50 structs of 4,000 bools,
    and 30,000 undeclared bools: each a field or an item spelled in a byte
    or a few, which the reader held as a dict entry or a dict, some 70
    bytes of Python heap a footer byte (a 60 MB footer of empty structs ran
    a caller out of memory, where pyarrow opened it in 105 MB). Now nothing
    no check reads is kept, and the row groups are checked as they are
    read, so such a footer peaks at about its own bytes. What is kept is
    the schema list, as dicts, and a few lists of a tuple per leaf, so a
    footer that is mostly schema, a wide file's as pyarrow writes it,
    peaks at a few times its bytes (about 6 for 20,000 columns), still
    linear in them."""
    import tracemalloc

    table = pa.table({f"c{i}": pa.array(range(50), pa.int64()) for i in range(80)})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, row_group_size=1, store_schema=False)
    bools = b"\x11" * 4000 + b"\x00"  # field ids 1 to 4,000, each true
    count = 20000
    key_values = b"\xfc" + footer_oracle._varint(count) + b"\x18\x01k\x00" * count

    def edit(tree):
        footer_oracle.put(tree, 5, TType.LIST, footer_oracle.Spelled(key_values))
        footer_oracle.repeat(
            tree,
            1000,
            TType.STRUCT,
            footer_oracle.Spelled(b"".join([b"\x1c" + bools] * 50) + b"\x00"),
        )
        # And 30,000 undeclared fields of FileMetaData itself, a byte each.
        for field_id in range(1001, 31001):
            footer_oracle.repeat(tree, field_id, TType.BOOL, True)

    raw = footer_oracle.rebuild(
        sink.getvalue().to_pybytes(), edit, encoder=footer_oracle.encode_unchecked
    )
    footer = len(footer_oracle.split(raw)[1])
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        assert parquet.metadata.num_row_groups == 50
        tracemalloc.start()
        try:
            validate_column_chunks(raw, parquet)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
    assert footer > 400_000
    assert peak < 3 * footer, (peak, footer)
    wide = pa.table({f"c{i}": pa.array([1], pa.int64()) for i in range(4000)})
    sink = pa.BufferOutputStream()
    pq.write_table(wide, sink, store_schema=False, write_statistics=False)
    raw = sink.getvalue().to_pybytes()
    footer = len(footer_oracle.split(raw)[1])
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        tracemalloc.start()
        try:
            validate_column_chunks(raw, parquet)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
    assert peak < 8 * footer, (peak, footer)


#: A Statistics struct (ColumnMetaData field 12) holding only a null count
#: of 7, and one whose 1-byte bounds pyarrow aborts on for an INT32 chunk.
_NULL_COUNT_7 = [[12, TType.STRUCT, [[3, TType.I64, 7]]]]
_SHORT_BOUNDS = [
    [12, TType.STRUCT, [[5, TType.STRING, b"\x01"], [6, TType.STRING, b"\x01"]]]
]


def test_pyarrow_reads_a_list_item_as_its_declared_type():
    """The refusal below, seen on a field pyarrow reads without aborting:
    the id chunk's encodings list says it holds a binary, whose bytes are
    Statistics with a null count of 7, and pyarrow, which reads the item as
    the declared i32 and the bytes after it as the chunk's own fields,
    reads that null count."""
    raw = footer_oracle.hide_in_encodings(NO_NULL.read_bytes(), 0, _NULL_COUNT_7)
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        assert parquet.metadata.row_group(0).column(0).statistics.null_count == 7
    _refused_everywhere(raw, "list holds Thrift type 11, not the declared 8")


def test_a_list_header_of_another_item_type_hides_nothing_from_the_guard():
    """Statistics with 1-byte bounds on the INT32 id chunk, which pyarrow
    aborts the process reading, hidden from Python's Thrift inside an
    encodings list whose header says it holds a binary: the guard read
    no statistics there, and accepted the file. Its reader now declares
    every list parquet.thrift does (above), and refuses the header."""
    raw = footer_oracle.hide_in_encodings(NO_NULL.read_bytes(), 0, _SHORT_BOUNDS)
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        assert parquet.schema_arrow.names == ["id", "v"]
    _refused_everywhere(raw, "list holds Thrift type 11, not the declared 8")


def test_a_negative_field_id_repeats_only_itself():
    """Field ids are i16s, negative ones too, which no writer spells: -1
    beside FileMetaData's version (1) is two fields, skipped, and -1 twice
    is a repeat."""
    once = footer_oracle.rebuild(
        NO_NULL.read_bytes(),
        lambda tree: footer_oracle.repeat(tree, -1, TType.I32, 5),
        encoder=footer_oracle.encode_unchecked,
    )
    _read_every_chunk(once)
    _check_chunks(once)
    twice = footer_oracle.rebuild(
        NO_NULL.read_bytes(),
        lambda tree: [footer_oracle.repeat(tree, -1, TType.I32, 5) for _ in range(2)],
        encoder=footer_oracle.encode_unchecked,
    )
    _refused_everywhere(twice, "repeats field -1")


def _faults_in_two_row_groups(tree):
    """The id chunk's type in row group 0, a short statistic beside it, and
    a histogram in row group 1."""
    groups = footer_oracle.field(tree, 4)[1]
    groups.append(copy.deepcopy(groups[0]))
    first, second = (footer_oracle.field(group, 1)[1] for group in groups)
    footer_oracle.put(footer_oracle.field(first[0], 3), 1, TType.I32, 2)
    _set_statistic(3)(footer_oracle.field(first[3], 3))
    _size_statistics(definition=[1])(footer_oracle.field(second[0], 3))


def _path_then_type(tree):
    """Row group 0's id chunk given another path, and its next chunk another
    type: the path is held to the leaf's only once the schema is read, and
    is still the first fault."""
    chunks = footer_oracle.field(footer_oracle.field(tree, 4)[1][0], 1)[1]
    footer_oracle.put(
        footer_oracle.field(chunks[0], 3), 3, TType.LIST, [TType.STRING, [b"zzz"]]
    )
    footer_oracle.put(footer_oracle.field(chunks[1], 3), 1, TType.I32, 1)


def _type_then_path(tree):
    chunks = footer_oracle.field(footer_oracle.field(tree, 4)[1][0], 1)[1]
    footer_oracle.put(footer_oracle.field(chunks[0], 3), 1, TType.I32, 2)
    footer_oracle.put(
        footer_oracle.field(chunks[2], 3), 3, TType.LIST, [TType.STRING, [b"zzz"]]
    )


def _a_later_fault_then_a_first_row_group_path(tree):
    """Row group 0's last chunk given another path, and row group 1's first
    chunk a histogram that does not fit."""
    groups = footer_oracle.field(tree, 4)[1]
    groups.append(copy.deepcopy(groups[0]))
    first, second = (footer_oracle.field(group, 1)[1] for group in groups)
    footer_oracle.put(
        footer_oracle.field(first[3], 3), 3, TType.LIST, [TType.STRING, [b"zzz"]]
    )
    _size_statistics(definition=[1])(footer_oracle.field(second[0], 3))


def _a_chunk_short_and_another_type(tree):
    """Row group 0 without its last chunk, and its first of another type: a
    row group's own fault comes before its chunks'."""
    chunks = footer_oracle.field(footer_oracle.field(tree, 4)[1][0], 1)[1]
    footer_oracle.put(footer_oracle.field(chunks[0], 3), 1, TType.I32, 2)
    chunks.pop()


def _a_chunk_short_then_a_sound_row_group(tree):
    """Row group 0 without its last chunk, and row group 1 whole."""
    groups = footer_oracle.field(tree, 4)[1]
    groups.append(copy.deepcopy(groups[0]))
    footer_oracle.field(groups[0], 1)[1].pop()


@pytest.mark.parametrize(
    ("edit", "fault"),
    [
        (_faults_in_two_row_groups, "column chunk 0 of row group 0 is of type 2, not"),
        (
            _a_chunk_short_then_a_sound_row_group,
            "row group 0 does not have a column chunk per leaf",
        ),
        (
            _a_chunk_short_and_another_type,
            "row group 0 does not have a column chunk per leaf",
        ),
        (_path_then_type, "column chunk 0 of row group 0 has the path_in_schema"),
        (_type_then_path, "column chunk 0 of row group 0 is of type 2, not"),
        (
            _a_later_fault_then_a_first_row_group_path,
            "column chunk 3 of row group 0 has the path_in_schema",
        ),
    ],
    ids=[
        "chunks",
        "chunk_count",
        "chunk_count_first",
        "path_first",
        "path_after",
        "path_first_group",
    ],
)
def test_the_first_fault_of_a_footer_is_the_one_reported(edit, fault):
    """The row groups are checked as the footer is read, one at a time, and
    the fault reported is still the first row group's, and its first
    chunk's, and a sound row group after it does not clear it."""
    raw = footer_oracle.rebuild(NO_NULL.read_bytes(), edit)
    _refused_everywhere(raw, fault)


def _second_row_group(edit):
    """A footer edit adding a second row group, a copy of the first (pyarrow
    opens the file: nothing it reads checks a row group against the pages),
    with ``edit`` applied to the copy's first chunk's metadata alone."""

    def apply(tree):
        groups = footer_oracle.field(tree, 4)[1]
        groups.append(copy.deepcopy(groups[0]))
        edit(footer_oracle.field(footer_oracle.field(groups[1], 1)[1][0], 3))

    return apply


@pytest.mark.parametrize(
    ("edit", "fault"),
    [
        (
            lambda meta: footer_oracle.put(meta, 1, TType.I32, 2),
            "is of type 2, not its schema leaf's 1",
        ),
        (_set_statistic(3), "has a 3-byte statistic of a 4-byte type"),
        (
            _size_statistics(definition=[1]),
            "has a 1-entry definition level histogram",
        ),
    ],
    ids=["type", "statistic", "histogram"],
)
def test_a_chunk_of_a_later_row_group_is_checked_too(edit, fault):
    """pyarrow reads every row group's chunks, and aborts the process on any
    of these in the second (rc -6 for the type)."""
    sound = footer_oracle.rebuild(
        NO_NULL.read_bytes(), _second_row_group(lambda meta: None)
    )
    _read_every_chunk(sound)
    _check_chunks(sound)
    raw = footer_oracle.rebuild(NO_NULL.read_bytes(), _second_row_group(edit))
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        assert parquet.metadata.num_row_groups == 2
    _refused_everywhere(raw, f"column chunk 0 of row group 1 {fault}")


#: 65536 past a field id: the same field to Thrift's generated readers,
#: which hold a field id in an i16, and an undeclared one to Python's
#: Thrift, which holds it whole.
_WRAP = 1 << 16


def _hidden_in_metadata(field_id, kind, value):
    """An edit replacing a ColumnChunk's metadata field ``field_id`` with
    one spelled 65536 past it."""

    def edit(chunk):
        meta = footer_oracle.field(chunk, 3)
        footer_oracle.drop(meta, field_id)
        footer_oracle.repeat(meta, field_id + _WRAP, kind, value)

    return edit


def _hidden_max_value(chunk):
    statistics = footer_oracle.field(footer_oracle.field(chunk, 3), 12)
    footer_oracle.repeat(statistics, 5 + _WRAP, TType.STRING, b"\x01")


def _hidden_type(chunk):
    footer_oracle.repeat(footer_oracle.field(chunk, 3), 1 + _WRAP, TType.I32, 2)


def _hidden_crypto(chunk):
    footer_oracle.repeat(chunk, 8 + _WRAP, TType.STRUCT, _COLUMN_KEY)


@pytest.mark.parametrize(
    ("edit", "spelled"),
    [
        (_hidden_in_metadata(16, TType.STRUCT, [[1, TType.I64, 5]]), 65552),
        (
            _hidden_in_metadata(
                16, TType.STRUCT, [[3, TType.LIST, [TType.I64, [1, 2, 3, 4]]]]
            ),
            65552,
        ),
        (_hidden_type, 65537),
        (_hidden_max_value, 65541),
        (_hidden_crypto, 65544),
        # 65536 below, the same field to the generated readers.
        (
            lambda chunk: footer_oracle.repeat(
                footer_oracle.field(chunk, 3),
                16 - _WRAP,
                TType.STRUCT,
                [[1, TType.I64, 5]],
            ),
            -65520,
        ),
    ],
    ids=[
        "unencoded_bytes",
        "histogram",
        "type",
        "max_value",
        "crypto_metadata",
        "negative",
    ],
)
def test_a_field_id_wider_than_an_i16_is_refused(edit, spelled):
    """Thrift's generated readers keep a long-form field id's low 16 bits,
    so to pyarrow 65536 + k is field k of the struct, where Python's
    Thrift reads an undeclared field: each of these hid from the guard a
    field it checks, and pyarrow aborted the process building or reading
    the INT32 id chunk (unencoded byte array bytes, a 4-entry definition
    histogram, an INT64 type, a 1-byte max_value, a column-key encryption).
    No writer spells such an id."""
    raw = _edit_column_chunk(
        NO_NULL.read_bytes(), 0, edit, encoder=footer_oracle.encode_unchecked
    )
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        assert parquet.schema_arrow.names == ["id", "v"]
    _refused_everywhere(raw, f"field id of {spelled} is not an i16$")


def _statistics_after(filler):
    """An edit replacing a ColumnChunk's Statistics with one holding only a
    null count of 7, spelled as field 65548, after a BYTE field of each id
    in ``filler``."""

    def edit(chunk):
        meta = footer_oracle.field(chunk, 3)
        footer_oracle.drop(meta, 12)
        for field_id in filler:
            footer_oracle.repeat(meta, field_id, TType.BYTE, 1)
        footer_oracle.repeat(meta, 12 + _WRAP, TType.STRUCT, [[3, TType.I64, 7]])

    return edit


@pytest.mark.parametrize(
    ("filler", "refused"),
    [([], 65548), ([32767 + 15 * step for step in range(2186)], 32782)],
    ids=["long_form", "delta"],
)
def test_pyarrow_reads_a_field_id_by_its_low_16_bits(filler, refused):
    """The refusal above, seen on a field pyarrow reads without aborting:
    Statistics spelled as field 65548 are the chunk's Statistics (12) to
    pyarrow, which reads their null count. The delta form gets there too,
    so refusing long-form ids alone would not do: after a long-form 32767,
    deltas of 15 and then 6 sum to 65548 for Python's Thrift and wrap at 16
    bits to 12 for the generated readers."""
    raw = _edit_column_chunk(
        NO_NULL.read_bytes(),
        0,
        _statistics_after(filler),
        encoder=footer_oracle.encode_unchecked,
    )
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        assert parquet.metadata.row_group(0).column(0).statistics.null_count == 7
    _refused_everywhere(raw, f"field id of {refused} is not an i16$")


@pytest.mark.parametrize(
    ("hidden", "shown"),
    [
        ((6, TType.I32, 1), "(Variant(1))"),
        (
            (10, TType.STRUCT, [[16, TType.STRUCT, [[1, TType.BYTE, 0]]]]),
            "(Variant(0))",
        ),
    ],
    ids=["converted_map", "logical_type_version_0"],
)
def test_a_variant_annotation_spelled_past_an_i16_is_refused(hidden, shown):
    """A converted MAP, or a second logicalType of VARIANT version 0, given
    the group as a field id 65536 past its own: the generated readers read
    the MAP (parquet-java's hydrator then reads a MAP), and merge the second
    logicalType into the first, which pyarrow shows as Variant(0). Python's
    Thrift read neither, and both paths accepted the group as native
    Parquet VARIANT version 1."""
    field_id, kind, value = hidden
    column = _variant_column(LAYOUT)
    raw, _ = _written(column, LAYOUT_ROWS)
    forged = footer_oracle.edit_element(
        raw,
        ("v",),
        lambda element: footer_oracle.repeat(element, field_id + _WRAP, kind, value),
        encoder=footer_oracle.encode_unchecked,
    )
    with pq.ParquetFile(pa.BufferReader(forged)) as parquet:
        assert f"v {shown}" in str(parquet.schema)
    for strict in (False, True):
        with pytest.raises(
            ValidationError, match=f"field id of {field_id + _WRAP} is not an i16$"
        ):
            _validate(forged, (column,), strict=strict)


@pytest.mark.parametrize(
    ("kind", "bits"), [(TType.I16, 16), (TType.I32, 32), (TType.I64, 64)]
)
def test_a_varint_is_held_to_the_width_the_generated_readers_hold_it_in(kind, bits):
    """The bounds themselves, of a field id (an i16), an i32 or a size, and
    an i64: the files above reach only values far outside them."""
    from pyhoglake.parquet_schema import _within

    for value in (-(2 ** (bits - 1)), 2 ** (bits - 1) - 1, 0):
        _within(value, kind, "value")
    for value in (-(2 ** (bits - 1)) - 1, 2 ** (bits - 1)):
        with pytest.raises(ValueError, match=f"value of {value} is not an i{bits}$"):
            _within(value, kind, "value")


#: 2**32 + 1 as a varint: 1 to Thrift's generated readers, which take a
#: size from its low 32 bits, and the whole of it to Python's Thrift.
_WIDE_SIZE = b"\x81\x80\x80\x80\x10"


def _unknown_field(kind, spelled):
    """A footer edit adding FileMetaData field 100, which parquet.thrift
    does not define, of type ``kind``, as the bytes ``spelled``."""
    return lambda tree: footer_oracle.repeat(
        tree, 100, kind, footer_oracle.Spelled(spelled)
    )


@pytest.mark.parametrize(
    ("edit", "fault"),
    [
        (
            lambda tree: _edit_chunk_tree(
                tree, 0, lambda meta: footer_oracle.put(meta, 1, TType.I32, 2**31 + 1)
            ),
            "integer of 2147483649 is not an i32",
        ),
        # The id chunk's encodings, a list of one i32 (0xF5: a long size,
        # i32 items), PLAIN; the key_value_metadata, a list, spelled as a
        # map of one i32 to i32 (0x55), which the generated readers skip
        # by its header; and a binary of one byte.
        (
            lambda tree: _edit_chunk_tree(
                tree,
                0,
                lambda meta: footer_oracle.put(
                    meta,
                    2,
                    TType.LIST,
                    footer_oracle.Spelled(b"\xf5" + _WIDE_SIZE + b"\x00"),
                ),
            ),
            "list size of 4294967297 is not an i32",
        ),
        (
            lambda tree: footer_oracle.put(
                tree, 5, TType.MAP, footer_oracle.Spelled(_WIDE_SIZE + b"\x55\x0e\x0e")
            ),
            "map size of 4294967297 is not an i32",
        ),
        (
            _unknown_field(TType.STRING, _WIDE_SIZE + b"x"),
            "a value runs past the end of the Parquet footer",
        ),
    ],
    ids=["integer", "list_size", "map_size", "binary_size"],
)
def test_a_varint_wider_than_thrift_reads_it_is_refused(edit, fault):
    """Thrift's generated readers take an i32, or a size, from the low 32
    bits of its varint, where Python's Thrift reads the whole varint. So
    pyarrow reads the id chunk's type, spelled as 2**31 + 1, as INT32 (1),
    and a list, a map or a binary of 2**32 + 1 items as one item, and reads
    every chunk of each of these files. The guard reads none of them as
    pyarrow does, so it refuses them, each with its reason."""
    raw = footer_oracle.rebuild(
        NO_NULL.read_bytes(), edit, encoder=footer_oracle.encode_unchecked
    )
    _read_every_chunk(raw)
    _refused_everywhere(raw, fault)


def _plain_file(*, nullable=True, length=16, integer="int64"):
    """A file of a fixed-length and an integer column, neither BYTE_ARRAY."""
    schema = pa.schema(
        [
            pa.field("f", pa.binary(length), nullable=nullable),
            pa.field("n", pa.type_for_alias(integer), nullable=nullable),
        ]
    )
    sink = pa.BufferOutputStream()
    pq.write_table(pa.table([[b"0" * length], [1]], schema=schema), sink)
    return sink.getvalue().to_pybytes()


@pytest.mark.parametrize("leaf", [0, 1], ids=["fixed_len_byte_array", "int64"])
def test_unencoded_byte_array_bytes_are_refused_on_any_other_type(leaf):
    """pyarrow aborts building the chunk's metadata ("Unencoded byte array
    data bytes does not support FIXED_LEN_BYTE_ARRAY"), and so for every
    type but BYTE_ARRAY."""
    raw = footer_oracle.edit_chunk(_plain_file(), leaf, _size_statistics(unencoded=5))
    with pytest.raises(ValidationError, match="unencoded byte array data bytes"):
        _check_chunks(raw)


@pytest.mark.parametrize(
    "differences",
    [
        {"nullable": False},
        {"length": 8},
        {"integer": "int32"},
    ],
    ids=["levels", "length", "physical_type"],
)
def test_the_chunks_are_held_to_the_parquet_file_given(differences):
    """The leaves the chunks are checked against are the ParquetFile's own
    (its ColumnDescriptors), and the footer must read as it does, in each
    leaf's levels, physical type and fixed length: a ParquetFile of a file
    that differs in any is refused."""
    raw = _plain_file()
    with (
        pq.ParquetFile(pa.BufferReader(_plain_file(**differences))) as other,
        pytest.raises(ValidationError, match="does not read as pyarrow reads"),
    ):
        validate_column_chunks(raw, other)
    _check_chunks(raw)


@pytest.mark.parametrize("extra", [True, False], ids=["more_leaves", "fewer_leaves"])
def test_the_chunks_are_held_to_a_parquet_file_of_as_many_leaves(extra):
    """A ParquetFile of a file with a leaf more, or a leaf fewer, than the
    footer read: its schema is not the footer's either."""
    fields = [pa.field("f", pa.binary(16)), pa.field("n", pa.int64())]
    fields = [*fields, pa.field("x", pa.int64())] if extra else fields[:1]
    sink = pa.BufferOutputStream()
    pq.write_table(
        pa.table([[b"0" * 16], [1], [2]][: len(fields)], schema=pa.schema(fields)), sink
    )
    with (
        pq.ParquetFile(pa.BufferReader(sink.getvalue().to_pybytes())) as other,
        pytest.raises(ValidationError, match="does not read as pyarrow reads"),
    ):
        validate_column_chunks(_plain_file(), other)


@pytest.mark.parametrize(
    "spelled", [(TType.I32, 7), (TType.BOOL, True)], ids=["i32", "bool"]
)
def test_a_logical_type_that_is_not_a_struct_is_not_variant(spelled):
    """pyarrow skips a logicalType that is not a struct, and opens the file
    with the group as a plain struct; it is not VARIANT, strict or not."""
    kind, value = spelled
    raw = footer_oracle.edit_element(
        FIXTURE.read_bytes(),
        ("properties",),
        lambda element: footer_oracle.put(element, 10, kind, value),
    )
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        assert parquet.schema_arrow.names == ["id", "properties"]
    for strict in (False, True):
        with pytest.raises(ValidationError, match="native Parquet VARIANT version 1"):
            _validate(raw, COLUMNS, strict=strict)


@pytest.mark.parametrize(
    "spelled", [(TType.I32, 1), (TType.BOOL, True)], ids=["i32", "bool"]
)
def test_a_variant_member_that_is_not_a_struct_is_not_variant(spelled):
    """pyarrow itself refuses to open a group whose logicalType's VARIANT
    member is not a struct (Thrift skips it, and an empty LogicalType
    cannot annotate a group). Should it ever open one, the check still
    refuses it rather than raise, as a logical type of no member: here it
    is given the forged footer with the original file's Arrow schema."""
    kind, value = spelled
    raw = FIXTURE.read_bytes()
    forged = footer_oracle.edit_element(
        raw,
        ("properties",),
        lambda element: footer_oracle.put(
            element, 10, TType.STRUCT, [[16, kind, value]]
        ),
    )
    with pytest.raises(OSError, match="Logical type Undefined"):
        pq.ParquetFile(pa.BufferReader(forged)).schema_arrow  # noqa: B018
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        for strict in (False, True):
            with pytest.raises(
                ValidationError, match="'properties'.* logical type of no member"
            ):
                validate_variant_file(forged, parquet, COLUMNS, strict=strict)


def _catalog_columns(raw, *, nullable):
    """The catalog columns a file is a file of: a top-level group annotated
    VARIANT is a variant column, any other column the type its Arrow type
    maps to."""
    elements = _schema_elements_from_footer(_footer_bytes(raw)[1])
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        arrow = parquet.schema_arrow
    columns = []
    for ordinal, ((element, _, _), field) in enumerate(
        zip(_top_level(elements), arrow, strict=True)
    ):
        if 16 in element.get(10, {}):
            kind, params = "variant", None
        else:
            kind, params = arrow_type_to_coltype(field.type)
        columns.append(
            Column(field.name, kind, element[9], ordinal, nullable, type_params=params)
        )
    return tuple(columns)


@pytest.mark.parametrize(
    ("offset", "byte", "fault"),
    [
        (26, 0x00, "3-entry definition level histogram"),
        (368, 0x01, "3-entry repetition level histogram"),
        # A list header now naming structs: Thrift's generated reader
        # reads its one item as the declared i64 anyway, a 1-entry
        # histogram, so a list of another item type is refused.
        (371, 0x1C, "list holds Thrift type 12, not the declared 10"),
    ],
)
def test_one_footer_byte_that_breaks_a_histogram_is_refused(offset, byte, fault):
    """The footer-overwrite property's own domain: a parquet-testing file
    forged as DuckDB's, every column NOT NULL, and one footer byte
    overwritten (a reviewer's draws). pyarrow opens each; the default check
    accepted them, and the strict one aborted the process building a chunk
    whose level histogram no longer fits its leaf."""
    path = DATA / "parquet-testing" / "shredded_variant" / "case-001.parquet"
    raw = footer_oracle.set_created_by(path.read_bytes(), DUCKDB_CREATED_BY)
    start, _ = _footer_bytes(raw)
    mutated = bytearray(raw)
    mutated[start + offset] = byte
    mutated = bytes(mutated)
    with pq.ParquetFile(pa.BufferReader(mutated)) as parquet:
        assert parquet.schema_arrow.names == ["id", "var"]
    for nullable in (False, True):
        columns = _catalog_columns(raw, nullable=nullable)
        for strict in (False, True):
            with pytest.raises(ValidationError, match=fault):
                _validate(mutated, columns, strict=strict)


@pytest.mark.parametrize(
    ("decl", "width"),
    [
        ({"type": "boolean"}, 1),
        ({"type": "int32"}, 4),
        ({"type": "int64"}, 8),
        ({"type": "float"}, 4),
        ({"type": "double"}, 8),
        ({"type": "uuid"}, 16),
    ],
)
def test_a_statistic_is_held_to_its_physical_types_width(decl, width):
    column = _variant_column(decl)
    raw, _ = _written(column, [None])
    exact = footer_oracle.edit_chunk(raw, 2, _set_statistic(width))
    _validate(exact, (column,), strict=True)
    short = footer_oracle.edit_chunk(raw, 2, _set_statistic(width - 1))
    with pytest.raises(ValidationError, match=f"of a {width}-byte type"):
        _validate(short, (column,), strict=False)


def test_an_int96_statistic_is_held_to_12_bytes():
    """pyarrow does not decode an INT96 chunk's statistics (their order is
    undefined) today, but a short one is no less malformed."""
    sink = pa.BufferOutputStream()
    times = pa.array([datetime(2026, 1, 1)], pa.timestamp("ns"))
    field = pa.field("t", times.type, metadata=ID_FIELD_ID)
    pq.write_table(
        pa.table([times], schema=pa.schema([field])),
        sink,
        use_deprecated_int96_timestamps=True,
    )
    raw = footer_oracle.edit_chunk(sink.getvalue().to_pybytes(), 0, _set_statistic(11))
    with pytest.raises(ValidationError, match="11-byte statistic of a 12-byte type"):
        _validate(raw, (Column("t", "timestamp_ns", 1, 0, True),), strict=False)


def test_a_row_group_must_have_a_chunk_per_leaf():
    def edit(tree):
        chunks = footer_oracle.field(footer_oracle.field(tree, 4)[1][0], 1)[1]
        del chunks[-1]

    raw = footer_oracle.rebuild(NO_NULL.read_bytes(), edit)
    with pytest.raises(ValidationError, match="column chunk per leaf"):
        _validate(raw, DUCKDB_COLUMNS, strict=False)


@pytest.mark.parametrize(
    ("decl", "fault"),
    [
        ({"type": "int32"}, None),
        ({"type": "int64"}, "int32, not the declared int64"),
        ({"type": "variant"}, "v is int32, not the declared variant (no typed_value)"),
        (None, None),
    ],
)
def test_strict_holds_a_foreign_file_to_the_columns_declaration(decl, fault):
    """DuckDB shreds this file's top level as int32 (it writes INT32 with
    only the legacy INT_32 converted type, which spec mode reads as the
    logical type it stands for). A declaration is checked; no declaration
    is not."""
    params = None if decl is None else {"shredding": decl}
    columns = (DUCKDB_COLUMNS[0], replace(DUCKDB_COLUMNS[1], type_params=params))
    if fault is None:
        _validate(NO_NULL, columns, strict=True)
        return
    with pytest.raises(ValidationError, match=re.escape(fault)):
        _validate(NO_NULL, columns, strict=True)
    _validate(NO_NULL, columns, strict=False)


def test_strict_refuses_a_declaration_this_client_cannot_read():
    columns = (
        DUCKDB_COLUMNS[0],
        replace(DUCKDB_COLUMNS[1], type_params={"shredding": {"type": "int128"}}),
    )
    with pytest.raises(UnsupportedShreddingError, match="upgrade pyhoglake"):
        _validate(NO_NULL, columns, strict=True)


def test_strict_accepts_pyhoglakes_own_files():
    for name, decl in CORPUS.items():
        column = _variant_column(decl, nullable=False)
        raw, _ = _written(column, [{"a": 1}, 2])
        _validate(raw, (column,), strict=True)
        assert name


def test_validate_variant_file_reads_a_path_and_bytes_alike():
    with pq.ParquetFile(FIXTURE) as p:
        validate_variant_file(FIXTURE.read_bytes(), p, COLUMNS)
        validate_variant_file(pa.py_buffer(FIXTURE.read_bytes()), p, COLUMNS)
        with pytest.raises(ValidationError, match="invalid prepared Parquet schema"):
            validate_variant_file(FIXTURE.read_bytes()[:-1] + b"E", p, COLUMNS)


def test_expected_elements_name_the_decimal_leaves_plan_stamps_lists():
    """Two derivations of one layout: the plan's stamps, and the decimal4/
    decimal8 leaves of its expected elements."""
    for decl in [*CORPUS.values(), LAYOUT]:
        column = _variant_column(decl)
        plan = _plan_of(column)
        leaves = [
            path[1:]
            for path, element in plan.expected_elements(column)
            if element.get(6) == 5 and element[1] in (1, 2)
        ]
        assert leaves == [path for path, _, _ in plan.stamps]
