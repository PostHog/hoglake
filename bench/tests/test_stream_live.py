"""Live integration for the ``stream`` task (skips cleanly without a server).

These runs are bounded by ``--max-events`` so the suite terminates; the
task itself runs until it is interrupted.
"""

from __future__ import annotations

import io
import os
import uuid

import pyarrow.parquet as pq
import pytest

from hoglake_bench.cli import build_parser
from hoglake_bench.context import Bench, BenchConfig
from hoglake_bench.stream import events as E
from hoglake_bench.stream import pacing, streamer

pytestmark = pytest.mark.integration


def _args(url: str, **overrides: object):
    argv = ["stream", "--url", url]
    for key, value in overrides.items():
        argv += [f"--{key.replace('_', '-')}", str(value)]
    return build_parser().parse_args(argv)


@pytest.fixture
def bench(live_server_url: str, tmp_path):
    cfg = BenchConfig(
        url=live_server_url,
        s3_endpoint=os.environ.get("HOGLAKE_S3_ENDPOINT", "http://localhost:9000"),
        results_path=str(tmp_path / "bench-results.jsonl"),
    )
    b = Bench(cfg)
    try:
        yield b
    finally:
        b.close()


def _run(bench: Bench, **overrides: object):
    catalog = overrides.pop("catalog", f"stream-it-{uuid.uuid4().hex[:8]}")
    args = _args(bench.cfg.url, catalog=catalog, **overrides)
    streamer.run(bench, args)
    table = (
        bench.client.catalog(args.catalog).namespace(args.namespace).table(args.table)
    )
    return table


def test_table_is_created_with_the_hour_spec_and_the_declared_sort_order(
    bench: Bench,
) -> None:
    table = _run(bench, max_events=4_000, rate=0, flush_mb=0.05, progress_seconds=999)
    info = table.info()
    field_id = {c.name: c.field_id for c in info.columns}

    assert info.partition_spec is not None
    assert [(f.source_field_id, f.transform) for f in info.partition_spec.fields] == [
        (field_id["ts"], "hour")
    ]
    # ...and team_id is deliberately NOT in it: production's events table
    # is not partitioned per tenant.
    assert field_id["team_id"] not in {
        f.source_field_id for f in info.partition_spec.fields
    }

    assert info.sort_spec is not None
    assert [f.source_field_id for f in info.sort_spec.fields] == [
        field_id[c] for c in E.SORT_COLUMNS
    ]
    assert all(f.direction == "asc" for f in info.sort_spec.fields)


def test_a_bounded_realtime_run_writes_about_one_file_per_flush(
    bench: Bench,
) -> None:
    """With one open hour, a flush fans out to ONE file (two only when it
    straddles the boundary) — the spec has a single field."""
    table = _run(bench, max_events=6_000, rate=0, flush_mb=0.05, progress_seconds=999)
    files = table.files()
    assert files, "a bounded run must leave files behind"
    # every file carries exactly one partition value: the hour
    assert all(
        f.partition_values is not None and len(f.partition_values) == 1 for f in files
    )
    hours = {f.partition_values[0] for f in files}
    assert len(hours) <= 2  # a real-time run touches one hour, two if it rolls
    assert table.info().record_count == 6_000


def test_time_compression_rolls_the_hour_partition_over(bench: Bench) -> None:
    table = _run(
        bench,
        max_events=20_000,
        rate=2_000,
        flush_mb=0.02,
        flush_seconds=0.5,
        hours_per_minute=600,  # 10 simulated hours per real second
        progress_seconds=999,
    )
    files = table.files()
    hours = {f.partition_values[0] for f in files}
    assert len(hours) > 1, f"expected several hour cells, got {hours}"
    # consecutive hour cells, as a wall-clock-following stream produces
    values = sorted(int(h) for h in hours)
    assert values == list(range(values[0], values[0] + len(values)))
    # and the cells really are HOURS: every row of a file falls in the
    # epoch hour the partition value names (a `day` spec would not).
    fs = bench.client._filesystem()
    for data_file in files:
        with fs.open_input_file(data_file.path[len("s3://") :]) as handle:
            written = pq.read_table(io.BytesIO(handle.read()))
        cell = int(data_file.partition_values[0])
        micros = [int(v.value) for v in written.column("ts")]
        assert {pacing.epoch_hour(m) for m in micros} == {cell}


def test_every_written_file_honours_the_declared_sort_order(
    bench: Bench,
) -> None:
    """team_id then ts, non-decreasing inside each file.

    Sort order is advisory for writers, so nothing server-side checks
    this — but it is the only thing giving a reader team locality on a
    table that is not partitioned by team, so the writer must hold it.
    """
    table = _run(bench, max_events=8_000, rate=0, flush_mb=0.05, progress_seconds=999)
    fs = bench.client._filesystem()
    for data_file in table.files():
        with fs.open_input_file(data_file.path[len("s3://") :]) as handle:
            payload = handle.read()
        written = pq.read_table(io.BytesIO(payload))
        keys = list(
            zip(
                written.column("team_id").to_pylist(),
                written.column("ts").to_pylist(),
            )
        )
        assert keys == sorted(keys), f"{data_file.path} is not sorted"


def test_the_skew_lands_inside_the_files_not_across_them(bench: Bench) -> None:
    """No team partitioning, so a whale shows up as most of the ROWS of
    every file, with the tail scattered through — and a file's team_id
    bounds therefore span the whole active team range."""
    table = _run(
        bench,
        max_events=30_000,
        rate=0,
        flush_mb=0.05,
        teams=50,
        whale_share=0.6,
        progress_seconds=999,
    )
    fs = bench.client._filesystem()
    files = table.files()
    assert files
    for data_file in files:
        with fs.open_input_file(data_file.path[len("s3://") :]) as handle:
            payload = handle.read()
        teams = pq.read_table(io.BytesIO(payload)).column("team_id").to_pylist()
        whale = max(set(teams), key=teams.count)
        assert teams.count(whale) / len(teams) > 0.4
        # the bounds are wide: the tail is in here too
        assert min(teams) < max(teams)


def test_a_run_journals_one_end_to_end_line(bench: Bench) -> None:
    import json

    _run(bench, max_events=2_000, rate=0, flush_mb=0.05, progress_seconds=999)
    with open(bench.cfg.results_path, encoding="utf-8") as handle:
        records = [json.loads(line) for line in handle]
    assert len(records) == 1
    record = records[0]
    assert record["scenario"] == "stream"
    assert record["io_mode"] == "end-to-end"
    assert record["status"] == "ok"
    assert record["metrics"][0]["events"] == 2_000
    assert record["metrics"][0]["hour_partitions"] >= 1
