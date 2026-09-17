# DuckDB event-file writer

`hedgerow.duckdb_writer.write_duckdb_event_partition` implements the file-writing
slice of buffered ingestion. It reads frozen source fragments directly with
DuckDB 1.5.5, selects one team/calendar-month partition, derives UTC `event_date`,
converts explicitly named JSON columns to native VARIANT, sorts by
`(event_date, event, timestamp, uuid)` ascending/nulls-first, and writes Zstandard
Parquet with explicit top-level field IDs. UUIDs are preserved, never generated.

This is a standalone library component. It does not upload or publish files and
is not wired into `BufferedIngestion` or the CLI. The catalog accepts VARIANT and
pyhoglake can publish these files through `prepare_append_files` without rewriting
them. VARIANT tables are excluded from scalar compaction; query-engine
interoperability and coordinator wiring remain separate work.
The existing coordinator and CLI retain their current behavior.

```python
from hedgerow.duckdb_writer import (
    DuckDBFragment,
    DuckDBWriterOptions,
    write_duckdb_event_partition,
)

paths = write_duckdb_event_partition(
    # rows is the expected count for this team/month within these row groups.
    [DuckDBFragment("/data/raw.parquet", row_groups=(0, 1), rows=1200)],
    "/scratch/flush-output",  # must be empty; returned paths are in physical order
    team_id=42,
    month=672,  # January 2026, months since January 1970
    field_ids={
        "team_id": 1,
        "timestamp": 2,
        "event": 3,
        "uuid": 4,
        "properties": 5,
        "event_date": 6,
    },
    json_columns=("properties",),  # example mapping, not an assumed event schema
    options=DuckDBWriterOptions(),
)
```

## Data contract

- Each path is a literal immutable file, not a glob. Row-group IDs must exist.
  Each file appears once per call. Selected counts must match frozen discovery
  before writing; total output count is checked again afterwards.
- Fields named by `field_ids` are the exact output projection; `event_date` is
  derived rather than copied. Projected source types must agree across files.
- `team_id` is INTEGER/BIGINT; `timestamp` is microsecond TIMESTAMP/TIMESTAMPTZ;
  naive timestamps mean UTC. `event` is VARCHAR; `uuid` is UUID or a 16-byte BLOB.
- Only explicitly named JSON/VARCHAR columns are converted. Existing VARIANT
  payloads pass through DuckDB, including native nested values. Other projected
  payloads retain their DuckDB types. This is not catalog schema validation.
- Top-level nulls in VARIANT output are rejected: DuckDB conflates SQL NULL
  and VARIANT null, and its writer encodes SQL NULL as a non-null VARIANT group
  containing null. This applies to converted JSON/VARCHAR and existing VARIANT
  columns. Nested nulls are supported; nulls in other output types are unchanged.
  Invalid JSON and malformed UUIDs
  fail rather than being replaced with nulls. Conversion follows DuckDB JSON
  numeric semantics; arbitrary-precision JSON numbers are not promised.
- Row-group selection uses file-row-number ranges. Correct selection is tested;
  efficient row-group I/O pruning and production S3 read amplification are not.
- Raw files remain indefinitely as backups. This component never deletes them.

## Resources and failure handling

Each call owns one DuckDB connection and one thread. The default memory setting
is 512 MiB, scratch setting is 8 GiB, and output target is 256 MiB with 8,192-row
row groups. File size is approximate, with row-group/footer overshoot. Scratch
limits do not include completed output files. The caller must bound concurrent
workers and provision enough space for both scratch and output.

DuckDB memory settings are not hard process RSS limits. A local 400,000-row
compatibility probe failed at 64 MB and completed with disk spilling at 128 MB,
but reported about 223 MB peak buffer memory. Use process/container limits and
measure realistic workloads before choosing production concurrency.

Private scratch and unvalidated output are removed on failure. If moving validated
files into the output directory fails, already moved files are removed so the
caller can retry with an empty directory. Validated output
files are returned in numeric file sequence, which preserves the physical sort
across file rolling. Consumers must use that order, not a lexical glob order.

An optional `configure_connection(connection)` initializer allows trusted
application code to configure native DuckDB S3 access. Writer settings are applied
after the initializer. Local regression tests do not exercise S3 or live catalog
publication. Payloads never pass through Arrow: ordinary PyArrow rewrites lose
the Parquet VARIANT annotation.

## Verification

The regression suite writes and reads real Parquet. It covers UTC month routing,
multiple source files and selected row groups, output rolling/order, field IDs,
JSON conversion and native VARIANT pass-through, precise nested decimal/integer
values, negative nanosecond timestamps, UUID preservation, null policy, schema
drift, literal quoting, invalid budgets and cleanup after integrity failures.
