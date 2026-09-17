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


def test_merge_passes_bound_reader_and_output_bytes(tmp_path, monkeypatch):
    import hedgerow.sorted_writer as writer_module

    budget = 1024
    table = events().slice(0, 64).append_column("payload", pa.array([b"x" * 900] * 64))
    batches = table.to_batches(max_chunksize=1)
    assert max(b.nbytes for b in batches) < budget
    original_merge = writer_module._merge
    original_file = pq.ParquetFile
    sizes = []

    def merge(*args, **kwargs):
        for batch in original_merge(*args, **kwargs):
            sizes.append(batch.nbytes)
            assert batch.nbytes <= budget
            yield batch

    class CheckedFile:
        def __init__(self, path):
            self.file = original_file(path)
            self.num_row_groups = self.file.num_row_groups

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.file.close()

        def iter_batches(self, **kwargs):
            assert len(kwargs["row_groups"]) == 1
            for batch in self.file.iter_batches(**kwargs):
                assert batch.nbytes <= budget
                yield batch

    with monkeypatch.context() as patch:
        patch.setattr(writer_module, "_merge", merge)
        patch.setattr(writer_module.pq, "ParquetFile", CheckedFile)
        paths = write_sorted_partition(
            batches, table.schema, str(tmp_path), max_batch_bytes=budget, fan_in=4
        )
    assert len(sizes) > len(batches)  # exercised multiple merge levels
    assert len(paths) == 1  # byte-bounded merge batches still share output files
    actual = pq.read_table(paths[0])
    assert actual.equals(table.sort_by([(c, "ascending") for c in EVENT_SORT]))


@pytest.mark.parametrize(
    "unit,values", [("ns", [-1001, -1, 0, 1, 1001]), ("s", [-1001, -1, 0, 1, 1001])]
)
def test_timestamp_payload_and_sort_keys_keep_exact_units(tmp_path, unit, values):
    table = (
        events()
        .slice(0, len(values))
        .append_column("extra_time", pa.array(values, pa.timestamp(unit)))
    )
    paths = write_sorted_partition(
        table.to_batches(max_chunksize=1),
        table.schema,
        str(tmp_path),
        sort_columns=("extra_time", "uuid"),
        fan_in=2,
    )
    output = pa.concat_tables([pq.read_table(p) for p in paths])
    assert (
        output["extra_time"].cast(pa.timestamp(unit)).cast(pa.int64()).to_pylist()
        == values
    )
    assert output["uuid"].to_pylist() == table["uuid"].to_pylist()


def test_nan_null_and_signed_zero_match_compaction_across_passes(tmp_path):
    import math

    # UUIDs deliberately reverse the required order within signed zeros so the
    # test detects treating -0 and +0 as equal before the UUID tiebreaker.
    values = [
        float("nan"),
        -0.0,
        1.0,
        None,
        0.0,
        float("-inf"),
        float("inf"),
        float("nan"),
    ]
    table = events().slice(0, len(values)).append_column("score", pa.array(values))
    expected_indexes = [3, 5, 1, 4, 2, 6, 7, 0]
    for chunk_size in (1, 3, len(values)):
        paths = write_sorted_partition(
            table.to_batches(max_chunksize=chunk_size),
            table.schema,
            str(tmp_path / str(chunk_size)),
            sort_columns=("score", "uuid"),
            fan_in=2,
        )
        output = pa.concat_tables([pq.read_table(p) for p in paths])
        assert (
            output["uuid"].to_pylist()
            == table.take(expected_indexes)["uuid"].to_pylist()
        )
        scores = output["score"].to_pylist()
        assert scores[0] is None
        assert math.copysign(1, scores[2]) == -1
        assert math.copysign(1, scores[3]) == 1
        assert all(math.isnan(x) for x in scores[-2:])


def test_nanosecond_payload_nulls_with_default_event_sort(tmp_path):
    table = (
        events()
        .slice(0, 4)
        .append_column("payload_time", pa.array([-1, None, 0, 1], pa.timestamp("ns")))
    )
    paths = write_sorted_partition(
        table.to_batches(max_chunksize=1), table.schema, str(tmp_path), fan_in=2
    )
    actual = pa.concat_tables([pq.read_table(p) for p in paths])
    expected = table.sort_by([(column, "ascending") for column in EVENT_SORT])
    assert actual.equals(expected)
