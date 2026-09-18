# Buffered raw event ingestion: implementation status

`hedgerow --config buffered.example.yaml` runs the coordinator with
`mode: buffered`. Existing configurations default to direct replication.
See [README.md](README.md#buffered-event-ingestion) for configuration, credential
setup, `--once`, and shutdown behavior.

Every flush uses the [DuckDB event-file writer](DUCKDB_WRITER.md): source payload
reads, transformation, sorting and Parquet output stay in DuckDB. PyArrow reads
routing columns during discovery and footer metadata during publication.

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

Configure `buffered.policy` using [buffered.example.yaml](buffered.example.yaml).

Readiness uses the bytes allocated to an individual destination partition and
the oldest pending source **commit time**. Arrivals never reset that time. The
compressed-byte readiness estimate allocates source row-group compressed sizes
proportionally by row count; transformation/compression can change output size.
DuckDB rolls output files around the configured compressed-byte target; this is
an approximate target, not a hard maximum. Input batch boundaries do not create
output files.

The default event sort is physical `(event_date, event, timestamp, uuid)`, all
ascending with nulls first. `event_date` is derived in UTC; event UUIDs are
preserved. The catalog sort specification must match, so later compaction has
the same order. The required partition layout is **identity(team_id), month(timestamp)**
(or month(event_date)), in either order; other layouts and sort orders fail at
startup. Iceberg month is a single
epoch-relative month number, e.g. January 2026 is 672, not month-of-year 1. It
already distinguishes years. A separate `year(timestamp)` is redundant.

The bounded worker pool owns at most one frozen input set per team. DuckDB sorts
using disposable local scratch. `writer_options` supplies per-worker memory and
scratch budgets; `BufferPolicy.target_file_bytes` controls output size. DuckDB's
memory budget is not an RSS ceiling. Provision for the configured worker count.

Pass explicit `json_columns=("properties", ...)` to `BufferedIngestion` only for
JSON/string source columns destined for VARIANT. Native VARIANT passes through.
The mapping and writer version are part of durable job identity: incompatible
state fails closed at startup. Existing Arrow coordinator state requires explicit
reconciliation; do not discard pending state to bypass that guard.

Job identity is an **explicit projection** of what defines the job — table uuid,
columns (field id, name, type, nullability, variant mapping), partition spec,
sort spec, consumer id, writer version and JSON-column mapping — and nothing
else. It is deliberately not derived from the pyhoglake dataclasses' shape: that
made a field added upstream re-write the identity of every running job and halt
the fleet at startup, with no change here at all. Adding a field to the
projection is therefore a decision about what makes a job different, and it
breaks every deployed coordinator, so it belongs in a change that says so.
`tests/test_job_identity.py` pins both directions.

`configure_duckdb(connection)` configures each worker connection's S3 access
(e.g. a DuckDB secret). It is separate from the Arrow filesystem used by discovery
and the destination client's upload filesystem. It must be thread-safe and must
not override writer resource limits. Credentials are not stored in pending state.

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
- Native VARIANT is not JSON. The DuckDB writer pins stable 1.5.5 and
  keeps payloads inside DuckDB. Catalog types and prepared-file physical validation are implemented; VARIANT
  statistics are omitted. VARIANT compaction, reader interoperability still need implementation. No Arrow payload rewrite may be inserted: ordinary
  PyArrow read/write drops the native VARIANT annotation. Source-to-destination
  JSON column mappings are explicit at the library boundary; the CLI defaults
  only `properties` as described in the README.

## Verification

The unit regressions exercise shared files across source windows, separate
months, the 24-hour default and team overrides, preserved deadlines after
restart, frozen work ownership, ambiguous publication retries, retention floors,
late-arriving old events, UTC partition values, physical sorting with UUID last,
compressed-size rolling, routing caps and schema/type refusal. The assembled
coordinator test uses real local Parquet plus fake catalogs; it is not a live
S3/Hoglake end-to-end test. `test_buffered_integration.py` separately exercises
the CLI against a live catalog and S3, including process restarts, a lost commit
response, exact VARIANT readback, and raw-file retention. Server tests exercise concurrent receipt creation,
REST DTO propagation, request mismatch and receipt survival after snapshot
history removal, in addition to migration/mapper consistency gates.
