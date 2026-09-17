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
    # Stats binding must use physical leaf positions, even when two paths have
    # the same spelling. Arrow's storage struct suffices to exercise this case.
    table = pq.read_table(FIXTURE).append_column(
        "properties.value", pa.array([b"scalar"])
    )
    path = tmp_path / "collision.parquet"
    pq.write_table(table, path)
    columns = (*COLUMNS, Column("properties.value", "binary", 3, 2, True))
    stats = extract_column_stats(pq.read_metadata(path), columns)
    assert [s.field_id for s in stats] == [1, 3]
    assert stats[1].lower_bound == b"scalar"
    assert stats[1].upper_bound == b"scalar"
