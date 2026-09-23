# Trino integration — reads now, writes without a redesign later

Compute is moving out to Trino, so Trino is the facade's first and most
important consumer — and eventually a producer. This doc covers what
the v1 design does to make reads excellent and to keep the write door
open without reversing the read-only-facade decision.

Companions: [README.md](../README.md) (decisions),
[iceberg-federation.md](iceberg-federation.md) (the facade's design
obligations — all of which are prerequisites here).

## 0. Native connector read contract (interim, until the facade lands)

The native connector lives in `PostHog/trino` (`plugin/trino-hoglake`);
`server/trino/` retains the server integration tests. Its read contract is:

- **One query, one snapshot.** `getTableHandle` resolves the catalog
  head once and pins snapshot id, `table_uuid`, and the column list
  into the handle; later metadata calls serve from the handle and
  split planning scans at the pinned snapshot. Concurrent commits —
  including DROP+CREATE incarnation changes — can never rebind an
  in-flight query: it reads the analyzed incarnation's consistent data
  or fails typed.
- **Typed failures.** Control-plane unreachability/5xx are EXTERNAL
  (`HOGLAKE_CATALOG_UNAVAILABLE`); a 410 below the expiry floor is
  `HOGLAKE_SNAPSHOT_EXPIRED` ("snapshot expired during query" — the
  expiry invariant's engine-side face: reconcile by re-running, never
  silently skip); vanished tables/schemas are the SPI's typed
  not-founds; a missing configured catalog is a USER_ERROR; malformed
  responses are coded, never bare exceptions.
- **DVs are applied, not refused.** A scan pairing a data file with a
  live deletion vector produces a split carrying that vector's path;
  the page source reads the puffin `deletion-vector-v1` object and
  drops the positions it marks, so deleted rows reach no result,
  aggregate, filter, or join. Unfiltered `count(*)` answers from
  catalog metadata (`record_count` minus `delete_count`) but still
  reads and validates the vector first. Planning checks only that the
  scan's data-file/DV pairing is internally consistent. A vector that
  is missing, corrupt, in another format, inconsistent with the
  catalog's `delete_count`, or naming a different data file fails the
  query — never "no deleted rows". Of those, `server/trino/` asserts
  the last two, the ones that depend on hoglake's own wire fields; the
  rest are the connector's suite to cover.
- **Config fails at load.** `hoglake.uri` validated and
  slash-normalized, empty `hoglake.catalog` rejected, request timeout
  configurable (`hoglake.client.request-timeout`, default 2m).
- **Id-authoritative column binding stays.** The connector binds by
  `PARQUET:field_id`, falling back to name only for files that carry no
  ids; catalog columns absent from the file read as nulls. (The
  fallback is "exact, then case-insensitive" per the class javadoc on
  `HoglakePageSourceProvider` in PostHog/trino — read there, not
  verified here; this harness only exercises the exact-name path.) The
  rename-vs-id-less-files
  hazard that binding creates is closed catalog-side: field ids are a
  registration contract and the server refuses renames while id-less
  files are live.
- **That guard's blind spot is the open work**, not the guard itself.
  `missing_field_ids` is written only by the hydrator's footer read,
  and files registered with inline stats never reach the hydrator, so
  a rename over them is allowed and the renamed column then reads
  NULL. `renameColumnEvadesTheFieldIdGuardViaInlineStats` in the Trino
  harness is that blind spot end to end.

## 1. Reads: everything rides the Iceberg connector

No custom Trino plugin. Trino's stock Iceberg connector pointed at the
hoglake REST facade gets: snapshot isolation, time travel
(`FOR VERSION AS OF` / `FOR TIMESTAMP AS OF` map to our snapshots),
partition + min/max pruning (as good as our manifests — hence the
typed-bounds obligation), and credential vending (the REST spec's
vended-credentials flow, which the AuthN/Z section already plans for —
Trino workers get scoped, short-lived object-store credentials per
table instead of bucket-wide keys).

Read-quality obligations beyond the federation doc:

- **Manifest quality is query performance.** Trino's planning cost and
  split pruning depend directly on manifest granularity and bound
  tightness. Deferred-stats (`pending`) files appear with null bounds
  and are never pruned — fine for correctness, but a table that lives
  mostly-pending will plan poorly in Trino; the hydrator's SLO is
  effectively a Trino-performance SLO.
- **Predictable snapshot pointers.** Trino caches table metadata;
  the facade must serve stable `metadata.json` locations per snapshot
  and a cheap current-pointer lookup, or planning latency eats the
  gains.
- **Type semantics parity.** TIMESTAMPTZ vs TIMESTAMP behavior,
  decimal scale/precision, and UTF-8 collation expectations should be
  validated against Trino's Iceberg type mapping in the phase-4
  milestone ("Trino reads a hoglake table") with a conformance test
  table containing every supported type.

## 2. Writes: design the commit endpoint so the adapter is a translation

The decision stands: v1 facade is read-only, all writes through
hoglake's API. But "Trino as a read/write source" stays reachable
because of two v1 choices:

**2a. The commit shapes are deliberately isomorphic.** An Iceberg REST
commit is *requirements + updates*: assert the snapshot state the
writer built against, append a new snapshot registering new files. The
hoglake commit is: declare the read snapshot, register files with
stats, server runs OCC. Same shape. Keep it that way — specifically:

- The registration API's stats payload is **Iceberg-`Metrics`-shaped**
  (value/null/nan counts, typed bounds, split offsets). Then a
  manifest entry from a Trino write *is* a valid hoglake registration
  with zero transformation.
- Map hoglake OCC conflicts to the Iceberg REST error contract
  (409/`CommitFailedException`) — Trino already retries those
  correctly. Our typed conflicts (README, commit-serialization
  refinements) make this mapping honest rather than string-matched.

**2b. The universal-producer invariant.** Adopt as a design rule:

> Any Iceberg-conformant parquet file is a valid hoglake data file.

Trino's Iceberg writer already embeds field IDs and produces
footer stats. If hoglake requires nothing beyond conformance — no
bespoke footer fields, encryption optional, no writer-side catalog
calls during the write — then Trino, Spark, and anything else that
writes correct Iceberg parquet is a valid *producer*, and enabling
writes is flipping on an adapter, not teaching engines about hoglake.

**2c. Scope external writes to append-only.** Server-side row-id range
assignment works identically for Trino-written files (they register
like any others and get ranges at commit). What does NOT translate:
Trino's delete/update strategies — copy-on-write rewrites and its own
position-delete emission — collide with the row-lineage guarantee
(rewritten files would need server-side rowid remapping Trino knows
nothing about). So the reachable end-state is:

| Operation from Trino | Status |
|---|---|
| SELECT (incl. time travel) | v1, via facade |
| INSERT / CTAS | reachable via REST adapter — append commit, no lineage hazard |
| DELETE / UPDATE / MERGE | not planned — hoglake-native only, lineage-preserving |

Append-only is also the actual requirement for moved-out compute:
transformation jobs write new tables/partitions; row-level mutation
stays with the CDC machinery that understands lineage.

## 3. Operational seams

- **Admission applies to adapter writes too.** A Trino CTAS
  registering 10K files is a maintenance-sized commit; it goes through
  the same commit-admission and fair-queuing as everything else, and
  gets the same explicit backpressure (which Trino surfaces as
  retryable commit failure).
- **Audit**: adapter commits carry the Trino principal through
  credential vending, so the audit log attributes engine writes to a
  real actor, not "the trino service account did something."
- **Conformance testing**: phase 4 grows a two-way suite — hoglake
  writes / Trino reads (v1 gate), and Trino writes-via-adapter /
  hoglake + Trino read-back (gate for enabling 2b in anger).

### Guarded table lifecycle

The `guarded-table-lifecycle-v1` catalog capability advertises an
`expected_table_uuid` query parameter on table DELETE and POST `/alter`, and
POST `/truncate` (where the UUID is required). The guard is checked after name
resolution under the catalog commit lock; a mismatch returns 409. Existing
clients that omit the optional guard on DELETE/alter retain their old behavior.
DuckDB and the console need no lifecycle-request changes; they do not
acquire the guarded guarantee until they send the UUID. Truncate is available
through REST and the paired Trino connector change.

Same-namespace rename preserves identity, files, schema and retained history.
Truncate ends live data-file and deletion-vector visibility in one snapshot,
without recreating the table or changing its UUID, schema, partition/sort specs,
properties or row-id allocator. DROP and TRUNCATE perform no object-storage
removal; normal retention/expiry/cleanup policy still applies later.

Truncate records `table_altered` as the existing DDL conflict barrier and
`table_deleted_from` for change tracking. An INSERT committed before truncate
is cleared. A snapshot-based INSERT planned before truncate but committed after
it conflicts; a fresh INSERT can commit normally. Legacy blind appends follow
catalog commit order. A committed append's durable receipt can still be replayed
after rename, truncate or drop without appending again or resurrecting a table.

Lifecycle requests have no durable operation receipt. A lost response, malformed
success response or server failure can leave the outcome unknown. Do not retry
these operations automatically or infer their outcome from a reused name.
In particular, replaying TRUNCATE could erase intervening INSERTs. Inspect table
identity and snapshot history before deciding on a new SQL operation. Append
receipt recovery does not confer lifecycle idempotency.

Deploy the server capability before enabling the paired Trino connector, which
refuses lifecycle SQL on servers without it. Cross-schema rename remains unsupported.


Changefeed windows `(from_snapshot, to_snapshot]` crossing TRUNCATE return
HTTP 409 `reconciliation_required`: ending old files creates no new deletion
vector, so returning an empty append plan would silently lose the deletion.
The guard uses the paired `table_altered`/`table_deleted_from` snapshot records
for the resolved table identity. Historical windows ending before truncate and
windows starting at or after it remain available. A consumer must reconcile its
destination from a full snapshot before explicitly advancing to that snapshot;
this is not an instruction to skip the rejected window. Pyhoglake exposes a
non-retryable `ReconciliationRequiredError`; both Hedgerow modes halt without
checkpointing the rejected window. Older clients receive an HTTP error rather
than an apparently successful empty plan. DuckDB and the console do not gain
new changefeed behavior; any caller of `/changes` must honor this refusal.
Ship the updated pyhoglake package with the updated Hedgerow package; the server
refusal also protects older consumers, which receive a permanent HTTP error.

### Atomic table replacement

`atomic-table-replacement-v1` extends prepared table creation with an optional
`replacement` guard: `expected_table_uuid` (null for an absent target) and
`read_snapshot`. Normal prepared creation remains unchanged. Deploy the server
capability before enabling connector replacement; finish the server rollout
before using it, since replacement receipts use durable definition version 3
and older servers refuse that version.

The connector supports empty `CREATE OR REPLACE TABLE` and replacement CTAS
for its supported definitions, including CTAS reading the old target. It captures
the target identity and snapshot during planning. Preparation and upload leave
the original visible; publication checks the guard under the catalog lock and
retires the old incarnation while publishing the new definition, UUID, and files
at one snapshot. Retained snapshots can still read the old incarnation.

Any recorded target change after the guarded snapshot (including INSERT, rename,
drop, truncate, and replacement) rejects publication. Compaction is the one
exception: it rewrites which files back a table and never which rows are visible
in it, so it cannot invalidate anything a replacement read, and the incarnation
being retired has all its files end-snapshotted anyway. Counting it made a
CREATE OR REPLACE of a continuously compacted table impossible to land, because
the rejection is durable. Unrelated table changes do not conflict. An absent target uses normal creation semantics and must
still be absent at publication. A guard overtaken by retention rejects publication.
Stale identified writers cannot append into the replacement. Replaying a committed
old INSERT or creation operation returns its original receipt without republishing.

Abort or a failed publication transaction leaves the old table intact. Lost
publication responses use the existing durable creation receipt; uncertain outcomes
never authorize upload deletion. Objects from retired incarnations remain subject
to normal retention and cleanup, with no immediate storage deletion.

Changefeed windows crossing replacement return `reconciliation_required`, including
at the retention boundary. Consumers must reconcile against a full snapshot and
start from the new UUID/snapshot; the transition is not an append-only change set.
The REST addition is opt-in: Python and DuckDB clients keep their existing creation
behavior, while Hedgerow must reconcile identity changes. The console's existing
history and dropped-incarnation views remain applicable.

## Guarded SQL schema evolution

The Trino connector's schema-evolution slice requires `guarded-schema-evolution-v1`;
roll out the server to every replica first. Namespace responses now include the
stable `namespace_id`. Optional `expected_namespace_id` on namespace deletion
rejects name reuse under the catalog lock. Optional `read_snapshot` on `/alter`,
paired with `expected_table_uuid`, rejects intervening table DDL and expired
conflict history. Ordinary appends and changes to other tables do not conflict.
Prepared replacement already checks target changes and namespace identity: an
alteration committed first rejects replacement, and replacement committed first
rejects an alteration using the old UUID.

ADD COLUMN now shares RENAME COLUMN's refusal of live id-less files and files
with pending or failed hydration.
Without IDs, an old file can contain a name being added (including DROP + ADD),
and name-based readers could expose those old values under the new field ID.
Hydrate, rewrite, or retire blocking files first. All clients using `/alter`
(including Python, DuckDB and the console) receive `idless_files_present` for
this unsafe case. Existing requests otherwise retain their semantics; the new
guards are optional for legacy callers. Namespace identity is additive response
metadata, which existing consumers may ignore. No mutation replay is introduced.

Trino exposes nullable top-level ADD COLUMN, RENAME COLUMN, DROP COLUMN, CREATE
SCHEMA and empty DROP SCHEMA. SQL type changes expose the existing signed integer
widening and float-to-double promotion policy, preserving field IDs. No cascade or
SQL nested evolution is added by those operations.

### Recursive Trino writes

`recursive-write-schema-v1` advertises the existing recursive column-definition,
materialized child-ID, and native scalar contracts to the Trino writer. Deploy
this capability to every server replica before the new connector. Older clients
may ignore the additive capability; no existing request or durable receipt changes.
Python remains the reference for these types and uses the same file encodings.
Hedgerow and the console retain their existing type support. DuckDB clients keep
their existing named refusals for scalar/variant types they do not recognize;
this change does not make those clients capable of reading them.

Trino writes ARRAY/MAP/named ROW with IDs on every catalog node, required map keys,
and preserved nested nullability. It writes signed narrow integers, nanosecond
timestamps within int64 range, and unshredded native VARIANT. Existing unsigned
columns use wider signed SQL types or DECIMAL(20,0) for uint64, while preserving
native physical encodings. uint64 retains the documented Iceberg-facade file
limitation; Trino's native connector converts the unsigned bits explicitly.
Existing JSON columns are exposed as validated, unchanged VARCHAR text, and
second/millisecond timestamp columns reject finer values. New SQL declarations
use canonical signed/string types. TIME and zoned timestamp precision above six,
CHAR, and SQL nested type evolution remain refused. Field-ID-keyed statistics are
populated by the existing hydrator, with no new metadata-store representation.

### Partitioned Trino creation and writes

`atomic-partitioned-table-creation-v1` adds the guarded preparation endpoint
`PUT /table-creations/{operation}/partitioned` with nonempty `partition_fields`.
Sources use the deterministic depth-first initial field IDs. The definition,
partition spec and initial files publish in one snapshot; mismatched values
roll back publication. Status/commit/abort keep their ordinary paths. The
ordinary preparation endpoint refuses nonempty partition fields. Deploy all
server replicas before the connector: older replicas return 404 for preparation
or refuse version-4 durable receipts, never silently create an unpartitioned table.
Unpartitioned receipts retain their existing lowest-capable format.

Existing Python, DuckDB, webui and hedgerow requests are unchanged. Other writers
continue using the same partition-value strings and transform allowlists. The
Trino connector supports scalar and struct-leaf partition sources, emits null
partition values as JSON null, and refuses unsupported transforms (including
truncate and legacy hour(date)). No background production job is activated.

### Sorted Trino creation and writes

`atomic-sorted-table-creation-v1` adds `PUT /table-creations/{operation}/sorted`.
It requires nonempty `sort_fields` and also accepts initial `partition_fields`.
Both specs use the initial depth-first field IDs and publish with initial files
in one snapshot. Ordinary and partition-only preparation refuse sort fields;
old replicas return 404 for the sorted endpoint. Durable sorted definitions use
version 5; other definitions retain their prior lowest-capable format.
All definition validation precedes publication mutations, including replacement.

The connector uses Trino's PageSorter on logical values before unsigned physical
conversion. Each bounded sorted run closes its files. Sorting is per file and
composes with partition routing and row-changing SQL. This adds writer CPU and
bounded buffering; it does not promise global table ordering or sorting of
historical files. Existing consumers' wire fields and requests are unchanged.

### Comments and custom properties

`COMMENT ON TABLE`, `COMMENT ON COLUMN`, CREATE column/table comments, and
`extra_properties = MAP(ARRAY['owner.team'], ARRAY['analytics'])` persist in the
catalog. `ALTER TABLE ... SET PROPERTIES extra_properties = ...` replaces the
whole custom map; `DEFAULT` clears it. These are inert annotations, not storage
configuration. Other table properties cannot be altered through this SQL surface.

Metadata edits use the existing guarded DDL transaction and mint a new snapshot;
concurrent stale schema-based writers conflict even when only an annotation changed.
No Parquet rewrite is needed. Comments and properties follow snapshot visibility. Rename and type promotion
preserve comments and stable field IDs; replacement receives only its new
metadata. SQL metadata and SHOW CREATE return the persisted values. Comment NULL
removes the value; an empty string is a distinct comment. Comments allow at most
16,384 UTF-16 code units. Custom maps allow 100 string pairs, keys matching
`[a-z][a-z0-9_.-]{0,127}`, and values of at most 4,096 UTF-16 code units. NUL is
refused. `hoglake.`/`trino.` prefixes and `partitioning`, `sorted_by`, `location`,
`format`, `comment` keys are reserved to avoid implied configuration behavior.

Deploy the additive V11 migration and upgrade **all server replicas** before
enabling metadata writes. Old server DDL can rewrite a version without preserving
new metadata fields; a mixed-version fleet or downgrade after metadata use is not
supported. The connector
requires `versioned-table-metadata-v1`; atomic creation uses `/metadata` and
receipt version 6 only when metadata is present. Old replicas reject that endpoint
or receipt. Metadata ALTER operation names (including `add_column_with_metadata`)
are distinct so old replicas cannot acknowledge and silently drop annotations.
Existing no-metadata receipts retain their earlier encodings. Python, DuckDB,
hedgerow and console consumers can ignore the additive response fields; this change
does not add metadata editing to their UIs or synthesize Iceberg properties.


### Reclaiming abandoned Trino uploads

With `claimed-uploads-v1`, Trino claims each Parquet or deletion-vector path before
PUT. The server creates unique paths, records the statement owner, and leases them
for 24 hours. Writers renew the owner's active leases while writing (at five-minute
intervals) and before handing off/publishing. Active claims, committed files, and
all retained historical data/DV references protect objects. Publication settles
claims in the same catalog transaction as files and the permanent commit receipt.
A lost response therefore cannot authorize deletion of a successful publication.
Existing immutable objects with retained references can still be referenced by
another commit; registered paths without retained references cannot be revived.

An explicit `POST /v1/catalogs/{catalog}/uploads/schedule-expired?limit=1000` fences
expired leases and queues abandoned paths, and returns how many it fenced. It does
not run in a new background job and does not enable production cleanup. The
existing cleanup drain still checks retained references under the commit lock
before physical deletion.

Claim transitions do NOT take that lock. A writer claims one upload per output
file, so taking the catalog's write-throughput bottleneck once per file bought
nothing: paths are server-generated random UUIDs and the claim insert is
idempotent. Renewal, abort and the expiry sweep serialize on the claim ROW
instead — each re-checks the claim's state in its own `WHERE`, which Postgres
re-evaluates after any row-lock wait, and publication takes the claim rows
`FOR UPDATE` inside the commit transaction. The ordering guarantees are
unchanged: a renewal that commits first beats the sweep, a publication that
commits first cannot be fenced afterwards, and once expiry wins a late commit
fails even after removal-ledger pruning. Each fence is its own short transaction,
so a publication never waits on a whole sweep while holding the commit lock.

Abandoned tombstones are retained permanently, and later explicit sweeps revisit
them because an in-flight PUT may finish after an earlier DELETE — but a
tombstone rests for the drained-ledger retention between offers rather than being
re-offered on every call, so sweep cost does not grow with the catalog's whole
upload history. A path that any data or deletion-vector file row still references
is never queued at all. Selection rotates through bounded batches. Unclaimed
historical or foreign objects are never inferred to be garbage from their age or
a bucket listing.

This adds one durable row and claim request per output file, occasional owner-wide
renewals, and permanent fencing metadata. A paused writer exceeding its lease may
fail if an operator reclaims it; it must retry with fresh paths. Unfinished worker
aborts fence only their own paths; workers never directly delete claimed files.
Uploads handed to the coordinator remain protected until publication or lease
expiry. Claim support requires V12 and all server replicas upgraded before use.
Claimed publication uses dedicated endpoints so old replicas refuse it. Older
servers retain legacy writes without this reclamation guarantee. Python, DuckDB,
hedgerow and the console can continue their existing paths; they do not claim or
schedule uploads in this feature. No object-store listing permissions are added.
