from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyhoglake.errors import ValidationError
from pyhoglake.models import Column
from pyhoglake.parquet_schema import validate_variant_file
from pyhoglake.stats import extract_column_stats

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


def test_nested_variant_stats_do_not_alias_dotted_scalar_name(tmp_path):
    """A variant's payload chunk and a scalar spelled the same way must
    not be confused -- and when they cannot be told apart, NEITHER is
    bound.

    #77 separated them by physical leaf POSITION, which is exact. The
    merge cannot keep that: #73 added a second production caller
    (``prepare_append_files``) that reads a parquet file the CALLER
    wrote, so footer column order is no longer guaranteed to match
    catalog order, and cross-column position arithmetic mis-attributes
    on a foreign file -- a silent wrong-column binding, which is worse
    than the aliasing it fixes.

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
