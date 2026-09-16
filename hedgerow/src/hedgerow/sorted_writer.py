"""External merge sort with bounded fan-in and compressed-size output rolling.

Spill files exist only during a flush attempt. The durable source of unpublished
rows remains raw_events. Read batches determine sort runs/row groups, never output
file boundaries. Each input iterator must contain exactly one destination partition.
"""

from __future__ import annotations

import heapq
import itertools
from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .halts import DataIntegrityError

EVENT_SORT = ("event_date", "event", "timestamp", "uuid")


def _key(row: dict, columns: Sequence[str]) -> tuple:
    # Explicit NULLS FIRST, matching the declared catalog sort order.
    return tuple((row[name] is not None, row[name]) for name in columns)


def _rows(path: Path, batch_rows: int) -> Iterator[dict]:
    with pq.ParquetFile(path) as file:
        for batch in file.iter_batches(batch_size=batch_rows):
            yield from batch.to_pylist()


def _merge(
    paths: Sequence[Path], schema: pa.Schema, columns: Sequence[str], batch_rows: int
) -> Iterator[pa.RecordBatch]:
    rows = heapq.merge(
        *(_rows(p, max(1, batch_rows // len(paths))) for p in paths),
        key=lambda row: _key(row, columns),
    )
    while chunk := list(itertools.islice(rows, batch_rows)):
        yield pa.RecordBatch.from_pylist(chunk, schema=schema)


def write_sorted_partition(
    batches: Iterable[pa.RecordBatch],
    schema: pa.Schema,
    output_dir: str,
    *,
    target_bytes: int = 256 * 1024 * 1024,
    sort_columns: Sequence[str] = EVENT_SORT,
    batch_rows: int = 8192,
    fan_in: int = 16,
    max_batch_bytes: int = 64 * 1024 * 1024,
) -> list[Path]:
    """Return local Parquet paths, sorted across files as well as within them.

    File size is approximate: one row group and the footer can overshoot the target.
    An oversized input batch fails explicitly; callers must bound input reads too.
    """
    if target_bytes <= 0 or batch_rows <= 0 or fan_in < 2 or max_batch_bytes <= 0:
        raise ValueError("invalid sorted-writer bounds")
    if not sort_columns or any(name not in schema.names for name in sort_columns):
        raise ValueError("sort columns must exist in the physical output schema")
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    output = []
    counter = itertools.count()
    with TemporaryDirectory(prefix="sort-", dir=root) as temp:
        levels: list[list[Path]] = []

        def merge_run(group: list[Path]) -> Path:
            path = Path(temp) / f"{next(counter)}.parquet"
            with pq.ParquetWriter(path, schema, compression="zstd") as writer:
                for merged_batch in _merge(group, schema, sort_columns, batch_rows):
                    writer.write_batch(merged_batch)
            for old in group:
                old.unlink()
            return path

        for batch in batches:
            if batch.nbytes > max_batch_bytes:
                raise DataIntegrityError(
                    "input batch exceeds sorted-writer memory budget"
                )
            if not batch.num_rows:
                continue
            table = pa.Table.from_batches([batch]).cast(schema)
            order = pc.sort_indices(
                table,
                sort_keys=[(c, "ascending") for c in sort_columns],
                null_placement="at_start",
            )
            run = Path(temp) / f"{next(counter)}.parquet"
            pq.write_table(
                table.take(order), run, compression="zstd", row_group_size=batch_rows
            )
            level = 0
            while True:
                if level == len(levels):
                    levels.append([])
                levels[level].append(run)
                if len(levels[level]) < fan_in:
                    break
                run = merge_run(levels[level])
                levels[level] = []
                level += 1
        runs = [path for level in levels for path in level]
        while len(runs) > fan_in:
            merged = []
            for start in range(0, len(runs), fan_in):
                group = runs[start : start + fan_in]
                path = Path(temp) / f"{next(counter)}.parquet"
                with pq.ParquetWriter(path, schema, compression="zstd") as writer:
                    for batch in _merge(group, schema, sort_columns, batch_rows):
                        writer.write_batch(batch)
                for old in group:
                    old.unlink()
                merged.append(path)
            runs = merged
        if not runs:
            return []
        writer = sink = None
        try:
            for batch in _merge(runs, schema, sort_columns, batch_rows):
                if writer is None:
                    path = root / f"part-{len(output):06d}.parquet"
                    if path.exists():
                        raise FileExistsError(path)
                    sink = pa.OSFile(str(path), "wb")
                    writer = pq.ParquetWriter(sink, schema, compression="zstd")
                    output.append(path)
                writer.write_batch(batch)
                if sink.tell() >= target_bytes:
                    writer.close()
                    sink.close()
                    writer = sink = None
        finally:
            if writer is not None:
                writer.close()
            if sink is not None:
                sink.close()
    return output
