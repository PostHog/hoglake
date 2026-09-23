"""Row-filter construction and application."""

import uuid

import pyarrow as pa
import pytest
from fakes import col

from hedgerow.config import ConfigError, FilterConfig
from hedgerow.filtering import build_filter

SRC = (
    col("id", "long", 1, 0, nullable=False),
    col("team_id", "long", 2, 1),
    col("name", "string", 3, 2),
    col("active", "boolean", 4, 3),
    col("event_id", "uuid", 5, 4),
    col("props", "json", 6, 5),
)


def _batch(**cols) -> pa.RecordBatch:
    return pa.record_batch(cols)


def test_no_filter_config_is_none():
    assert build_filter(None, SRC) is None


def test_equals_int_filter():
    f = build_filter(FilterConfig(column="team_id", equals=42), SRC)
    batch = _batch(
        id=pa.array([1, 2, 3, 4], pa.int64()),
        team_id=pa.array([42, 7, 42, None], pa.int64()),
    )
    out = f.apply(batch)
    assert out.column("id").to_pylist() == [1, 3]


def test_nulls_never_match():
    f = build_filter(FilterConfig(column="team_id", equals=42), SRC)
    batch = _batch(
        id=pa.array([1, 2], pa.int64()),
        team_id=pa.array([None, None], pa.int64()),
    )
    assert f.apply(batch).num_rows == 0


def test_string_filter():
    f = build_filter(FilterConfig(column="name", equals="a"), SRC)
    batch = _batch(
        id=pa.array([1, 2, 3], pa.int64()),
        name=pa.array(["a", "b", None], pa.string()),
    )
    assert f.apply(batch).column("id").to_pylist() == [1]


def test_uuid_filter_matches_either_wire_form():
    """A hoglake uuid column reaches the reader in either spelling: the
    annotated pa.uuid() (what pyhoglake, compaction and the Trino
    connector write) or the bare fixed_size_binary(16) of everything
    registered before that contract. Arrow has NO compute kernel for the
    extension type — pc.equal over extension<arrow.uuid> raises
    ArrowNotImplementedError — so the filter compares the 16 storage
    bytes both forms share."""
    value = uuid.UUID(int=7).bytes
    other = uuid.UUID(int=8).bytes
    f = build_filter(FilterConfig(column="event_id", equals=value), SRC)
    for spelling in (pa.binary(16), pa.uuid()):
        batch = _batch(
            id=pa.array([1, 2, 3], pa.int64()),
            event_id=pa.array([value, other, None], pa.binary(16)).cast(spelling),
        )
        assert f.apply(batch).column("id").to_pylist() == [1], spelling


def test_json_filter_matches_either_wire_form():
    """json has the same two spellings as uuid — pa.json_() from a
    writer that annotates, plain utf8 from one that does not — and the
    same no-kernel problem: every arrow compute function refuses an
    extension type. Filtering on a json column raised
    ArrowNotImplementedError in both spellings before the filter started
    comparing storage."""
    doc = '{"a": 1}'
    f = build_filter(FilterConfig(column="props", equals=doc), SRC)
    for spelling in (pa.string(), pa.json_()):
        batch = _batch(
            id=pa.array([1, 2, 3], pa.int64()),
            props=pa.array([doc, '{"a": 2}', None], pa.string()).cast(spelling),
        )
        assert f.apply(batch).column("id").to_pylist() == [1], spelling


def test_boolean_filter():
    f = build_filter(FilterConfig(column="active", equals=False), SRC)
    batch = _batch(
        id=pa.array([1, 2], pa.int64()),
        active=pa.array([True, False]),
    )
    assert f.apply(batch).column("id").to_pylist() == [2]


def test_value_cast_to_column_type():
    # YAML "42" for a long column: int scalar carries the column type
    f = build_filter(FilterConfig(column="team_id", equals=42), SRC)
    assert f.value.type == pa.int64()


def test_unknown_filter_column_rejected():
    with pytest.raises(ConfigError, match="'ghost' is not a source column"):
        build_filter(FilterConfig(column="ghost", equals=1), SRC)


def test_uncastable_filter_value_rejected():
    with pytest.raises(ConfigError, match="not valid for column 'team_id'"):
        build_filter(FilterConfig(column="team_id", equals="not-a-number"), SRC)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_nan_inf_filter_value_rejected_on_direct_construction(value):
    # bugs.md #4 regression, direct-construction path: pc.equal(col, nan)
    # is False for every row, so the filter silently drops 100% of rows
    # while the offset advances. Refused next to the null guard.
    with pytest.raises(ConfigError, match="finite"):
        build_filter(FilterConfig(column="team_id", equals=value), SRC)


def test_filter_preserves_schema():
    f = build_filter(FilterConfig(column="team_id", equals=1), SRC)
    batch = _batch(
        id=pa.array([1], pa.int64()),
        team_id=pa.array([1], pa.int64()),
    )
    assert f.apply(batch).schema == batch.schema
