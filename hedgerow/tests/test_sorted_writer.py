from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from hedgerow.halts import DataIntegrityError
from hedgerow.sorted_writer import EVENT_SORT, write_sorted_partition


def events():
    return pa.table(
        {
            "event_date": pa.array([datetime(2026, 1, 1).date()] * 200, pa.date32()),
            "event": [f"event-{i % 3}" for i in range(200)],
            "timestamp": pa.array(
                [datetime(2026, 1, 1, tzinfo=UTC)] * 200, pa.timestamp("us", "UTC")
            ),
            "uuid": pa.array(
                [i.to_bytes(16, "big") for i in reversed(range(200))], pa.binary(16)
            ),
        }
    )


def test_sort_spans_batches_and_output_boundaries(tmp_path):
    table = events()
    paths = write_sorted_partition(
        table.to_batches(max_chunksize=7),
        table.schema,
        str(tmp_path),
        target_bytes=500,
        batch_rows=9,
        fan_in=3,
    )
    assert len(paths) > 1
    actual = pa.concat_tables([pq.read_table(p) for p in paths])
    assert actual.equals(table.sort_by([(name, "ascending") for name in EVENT_SORT]))
    assert (
        actual["uuid"].to_pylist()
        == table.sort_by([(name, "ascending") for name in EVENT_SORT])[
            "uuid"
        ].to_pylist()
    )


def test_input_batches_do_not_become_files(tmp_path):
    table = events()
    paths = write_sorted_partition(
        table.to_batches(max_chunksize=2), table.schema, str(tmp_path), fan_in=3
    )
    assert len(paths) == 1
    assert pq.read_table(paths[0]).num_rows == 200


def test_memory_budget_refuses_oversize_batch(tmp_path):
    table = events()
    with pytest.raises(DataIntegrityError, match="memory budget"):
        write_sorted_partition(
            table.to_batches(), table.schema, str(tmp_path), max_batch_bytes=1
        )
