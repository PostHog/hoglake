# Buffered raw event ingestion: implementation status

This branch adds the buffering/publication building blocks and an assembled
`hedgerow.ingestion.BufferedIngestion` library coordinator. **It does not yet
provide the requested production raw_events → events CLI mode.** Existing CLI
configurations continue to run direct replication with their documented
at-least-once behavior.

Native VARIANT catalog and prepared-file publication are supported. The Arrow-based
coordinator still refuses VARIANT destination schemas. The separate [DuckDB event-file writer](DUCKDB_WRITER.md)
now reads, transforms, sorts and writes native VARIANT Parquet, but is not yet
wired into the coordinator or CLI. Do not deploy this library coordinator as the
finished ingestion service.

Raw files remain indefinitely as data backups. Raw-file retirement is out of scope
and is not a prerequisite for enabling ingestion.

## Implemented behavior

Discovery reads each committed change window once. It indexes routing columns
by source file, team and actual destination partition; row-group numbers are
packed into each reference. Events stay in shared raw Parquet. A SQLite WAL
stores only identities, scheduling metadata and prepared commit requests, with
FULL synchronous durability. Discovery and publication checkpoints are separate.
The single registered source consumer stays behind the oldest unpublished source
snapshot and requires `consumer_floor`; there are no per-team consumers.

`BufferPolicy.parse` accepts this policy mapping (this is **not** a new CLI YAML
section yet):

```yaml
target_file_bytes: 268435456
max_age_s: 86400
team_max_age_s:
  42: 3600
workers: 4
max_fragments_per_window: 1000000
```

Readiness uses the bytes allocated to an individual destination partition and
the oldest pending source **commit time**. Arrivals never reset that time. The
compressed-byte readiness estimate allocates source row-group compressed sizes
proportionally by row count; transformation/compression can change output size.
Output files roll after reaching the configured compressed-byte target, with up
to a row group plus footer of overshoot. Input batch boundaries do not create
output files.

The default event sort is physical `(event_date, event, timestamp, uuid)`, all
ascending with nulls first. `event_date` is derived in UTC; event UUIDs are
preserved. The catalog sort specification must match, so later compaction has
the same order. `EventTransform.sort_columns` permits another physical order
with UUID last. The table must include an identity partition on team_id; routing
uses the actual destination specification. The recommended team/calendar-month
layout is **identity(team_id), month(timestamp)**. Iceberg month is a single
epoch-relative month number, e.g. January 2026 is 672, not month-of-year 1. It
already distinguishes years. A separate `year(timestamp)` is redundant.

The bounded worker pool owns at most one frozen input set per team. Sorted
output is built with bounded-fan-in external merge passes in temporary files;
these are disposable flush scratch, not durable pending-event storage. Arrow
batches are capped and oversized input batches are refused. Limits are per
worker; provision memory and local scratch for the configured worker count.

A complete uploaded-file commit request is persisted before publication. The
new `POST /catalogs/{catalog}/commit/prepared` endpoint requires a UUID
`idempotency_key` and atomically stores a receipt with the commit under the
catalog lock. Exact retries return the original result. Different payloads with
the same key fail. Receipts survive snapshot expiry. This deduplicates a work
publication, not equal event UUIDs. An older server returns 404 on this dedicated
endpoint rather than silently ignoring an unfamiliar request field.

## Operational constraints and remaining work

- A coordinator holds an OS lock until its workers stop. Reassignment requires
  moving the same durable volume after the previous process has exited. This is
  not multi-host lease/fencing support; NFS/shared filesystems are unsupported.
  Do not run the same consumer with a different state database. Losing the state
  database requires reconciliation; do not initialize a replacement at head.
- Source files must be immutable and their record counts must match the catalog.
  Routing keys are non-null integer team_id and microsecond timestamp; naive
  timestamps mean UTC. Source deletes halt. Schema, partition/sort specification
  and table-incarnation changes halt rather than reinterpreting pending input.
- Team-sorted source row groups reduce read amplification but do not eliminate
  it. Discovery reports estimated selected/raw compressed bytes. Flushes can
  reread shared row groups; there is no shared row-group cache/coalescing yet.
  Routing expansion fails at its configured cap without checkpointing a partial
  window. Production sizing at 140K teams and indexed scheduling improvements
  are still needed; readiness currently aggregates pending metadata.
- `cleanup_candidates()` is unused legacy bookkeeping, not deletion authorization.
  Raw files are retained indefinitely as backups; there is no retirement worker.
- A failure before a commit request is persisted can orphan uploaded output
  files. Pending input remains intact; an upload reservation/reclamation path is
  still needed. Prepared requests must never be regenerated after an ambiguous
  commit. A persistent DDL conflict requires operator reconciliation.
- Receipts currently retain the complete request indefinitely. A receipt GC
  protocol needs an explicit replay horizon before any deletion is safe.
- Native VARIANT is not JSON. The standalone DuckDB writer pins stable 1.5.5 and
  keeps payloads inside DuckDB. Catalog types and prepared-file physical validation are implemented; VARIANT
  statistics are omitted. VARIANT compaction, reader interoperability and
  coordinator wiring still need implementation. No Arrow payload rewrite may be inserted: ordinary
  PyArrow read/write drops the native VARIANT annotation. Source-to-destination
  JSON column mappings must be explicit; no property names are assumed.

## Verification

The unit regressions exercise shared files across source windows, separate
months, the 24-hour default and team overrides, preserved deadlines after
restart, frozen work ownership, ambiguous publication retries, retention floors,
late-arriving old events, UTC partition values, physical sorting with UUID last,
compressed-size rolling, routing caps and schema/type refusal. The assembled
coordinator test uses real local Parquet plus fake catalogs; it is not a live
S3/Hoglake end-to-end test. Server tests exercise concurrent receipt creation,
REST DTO propagation, request mismatch and receipt survival after snapshot
history removal, in addition to migration/mapper consistency gates.
