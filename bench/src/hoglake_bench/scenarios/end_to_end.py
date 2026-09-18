"""Scenario 7: the realistic writer — real parquet through Table.append.

pyarrow tables go through the full client path: schema alignment,
parquet encode, S3 upload to MinIO, footer-stats extraction, commit.
Reports rows/s end to end and per-append latency.

Two table shapes (``--table-schema``):

- ``simple`` (default): the four-column id/name/value/ts table — the
  historical headline, comparable across runs.
- ``typed``: the full 1.1.0 matrix from :mod:`hoglake_bench.datagen` —
  every writable scalar plus nested list/struct/map — so footer-stats
  extraction, bounds encoding and registration run against every type
  the write path can express. Batches are deterministic per (seed,
  batch index). Reported as ``append.real_parquet_typed`` so the two
  shapes never blur.
"""

from __future__ import annotations

import argparse

import numpy as np
import pyarrow as pa

from ..context import Bench
from ..datagen import full_coverage_columns, make_table, to_arrow_schema
from ..runner import FailureGuard, run_loop
from ..stats import Metric
from .common import ScenarioReport, assert_row_tiling, check

#: End-to-end: every append encodes real parquet, uploads it to the
#: object store, and ships stats extracted from the writer's own footer.
IO_MODE = "end-to-end"

E2E_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("name", pa.string()),
        pa.field("value", pa.float64()),
        pa.field("ts", pa.timestamp("us", tz="UTC")),
    ]
)


#: Fixed generation seed for typed batches (the catalog-lock magic
#: number, same convention as the seed task): typed runs are
#: reproducible without a new CLI knob, batch index folded in.
TYPED_SEED = 4740871


def typed_batch(index: int, rows: int) -> pa.Table:
    """Batch ``index`` of the typed run — a pure function of (TYPED_SEED,
    index), so a rerun writes byte-identical data."""
    rng = np.random.default_rng([TYPED_SEED, index])
    return make_table(rng, full_coverage_columns(), rows)


def _batch(start: int, rows: int) -> pa.Table:
    ids = list(range(start, start + rows))
    return pa.table(
        {
            "id": pa.array(ids, pa.int64()),
            "name": pa.array([f"event-{i % 97}" for i in ids], pa.string()),
            "value": pa.array([(i % 1000) / 7.0 for i in ids], pa.float64()),
            "ts": pa.array(
                [1_700_000_000_000_000 + i for i in ids],
                pa.timestamp("us", tz="UTC"),
            ),
        }
    )


def run(bench: Bench, args: argparse.Namespace) -> ScenarioReport:
    batches = max(1, args.rows // args.batch_rows)
    warmup = 1 if batches > 1 else 0
    report = ScenarioReport(
        scenario="end-to-end-writer",
        params={
            "rows": args.rows,
            "batch_rows": args.batch_rows,
            "table_schema": args.table_schema,
            "batches": batches,  # measured appends; warmup appends on top
            "warmup_batches": warmup,
            "warmup": warmup,
            "duration": args.duration,
            "url": args.url,
        },
    )
    bench.ensure_bucket()
    catalog = bench.new_catalog("e2e")
    ns = catalog.create_namespace("bench")
    typed = args.table_schema == "typed"
    if typed:
        schema = to_arrow_schema(full_coverage_columns())
        metric_name = "append.real_parquet_typed"
    else:
        schema = E2E_SCHEMA
        metric_name = "append.real_parquet"
    table = ns.create_table("events", schema)
    guard = FailureGuard()

    payload_bytes = [0]

    def op(i: int) -> None:
        if typed:
            data = typed_batch(i, args.batch_rows)
        else:
            data = _batch(i * args.batch_rows, args.batch_rows)
        table.append(data, author="hoglake-bench", message=f"batch {i}")
        if i >= warmup:
            # arrow_mb_s: numerator and denominator must cover the SAME
            # measured window — warmup bytes stay out of both
            payload_bytes[0] += data.nbytes

    loop = run_loop(
        op, ops=batches, warmup=warmup, duration_s=args.duration, guard=guard
    )
    check(loop.errors == 0, f"{loop.errors} failed appends")
    appended = loop.recorder.count + loop.warmup.count
    measured_rows = loop.recorder.count * args.batch_rows
    report.add(
        Metric.from_recorder(
            metric_name,
            loop.recorder,
            loop.wall_s,
            rows_s=measured_rows / loop.wall_s if loop.wall_s else 0.0,
            rows=measured_rows,
            arrow_mb_s=(payload_bytes[0] / 1e6 / loop.wall_s) if loop.wall_s else 0.0,
        )
    )

    # correctness: server-side accounting matches what we wrote
    info = table.info()
    check(
        info.record_count == appended * args.batch_rows,
        f"table record_count={info.record_count}, wrote "
        f"{appended * args.batch_rows} rows",
    )
    check(
        info.file_count == appended,
        f"file_count={info.file_count}, appended {appended} files",
    )
    files = table.files()
    check(
        all(f.stats_state == "provided" for f in files),
        "footer-shipped stats did not register as provided",
    )
    assert_row_tiling(table, expected_rows=appended * args.batch_rows)
    return report


def add_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--rows", type=int, default=100_000, help="total rows to write")
    p.add_argument("--batch-rows", type=int, default=10_000, help="rows per append")
    p.add_argument(
        "--table-schema",
        choices=("simple", "typed"),
        default="simple",
        help="table shape: 'simple' (the comparable historical headline) "
        "or 'typed' (the full 1.1.0 type matrix, nested types included)",
    )
