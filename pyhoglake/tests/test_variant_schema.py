from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyhoglake.errors import ValidationError
from pyhoglake.models import Column
from pyhoglake.parquet_schema import validate_variant_file
from pyhoglake.stats import extract_column_stats
from pyhoglake.types import columns_to_arrow_schema

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
