# hoglake server

A lakehouse catalog as a service. Hoglake owns the metadata for tables
whose data lives as parquet in object storage: what tables exist, which
files back them at every point in time, per-file statistics for query
pruning, row-level deletes, and where every consumer of a table's
change stream is. Clients talk REST; nobody talks to the backing
Postgres but the service itself.

Kotlin/JVM, Ktor, Postgres via JDBI + Flyway, S3-compatible object
storage, Prometheus metrics, structured audit log. The OpenAPI contract
lives at `src/main/resources/openapi/hoglake.yaml` and is served at
`/openapi.yaml`.

```
 writers ──► POST /commit ──┐            ┌──► GET /files /scan /changes
 (parquet → S3, then        │            │    (read planning: metadata only)
  register w/ footer stats) │            │
                      ┌─────▼────────────┴─────┐
                      │      hoglake server     │
                      │  DDL · commits (OCC) ·  │──► Postgres (the catalog)
                      │  changefeed · offsets · │
                      │  retention · hydrator · │──► S3 (footer reads +
                      │  metrics · audit        │       physical cleanup only)
                      └─────────────────────────┘
```

The server never opens a data file to admit it, and touches object
storage in exactly three places: the hydrator's footer reads, cleanup's
physical deletes, and compaction's rewrite (the one background job that
reads and writes data files — always outside the commit path).

## The data model

Everything hangs off five ideas (`db/migration/V1__init.sql`, mirrored
by the canonical `schema.sql`; an integration test asserts the two
stay structurally identical):

**Snapshots are the clock.** Every mutation — DDL, append, delete —
commits exactly one `hog_snapshot` row with a **dense, per-catalog
monotonic id**. A snapshot carries its schema version, timestamp,
author/message, and a set of **typed change rows**
(`hog_snapshot_change`: `table_created`, `table_inserted_into`,
`table_deleted_from`, …, each with an object id). Those change rows are
simultaneously the commit history humans read and the machine
vocabulary conflict detection joins against.

**Versioned rows give time travel.** Mutable metadata (table
name/namespace, columns, partition specs, data files, deletion vectors,
views) is stored as rows with `[begin_snapshot, end_snapshot)` validity.
One predicate defines all reads:
`begin_snapshot <= S AND (end_snapshot IS NULL OR S < end_snapshot)`.
Reading the past is reading the present with a different `S`.

**Identity is split from versions.** `hog_table` is immutable identity:
`table_id` plus a `table_uuid` that survives rename and *changes* on
drop+recreate — the contract external consumers key their cursors to,
so a recreated table is visibly a different thing. `hog_table_version`
and `hog_column` carry the mutable, versioned parts. Column `field_id`s
are stable for the life of the table and are embedded by writers into
the parquet schema itself (`PARQUET:field_id`), so file columns bind to
catalog columns by id, never by name.

**Allocators are rows, advanced under the commit lock.** Snapshot ids,
table/file/view ids, per-table field ids, and per-table row-id ranges
all come from counter columns (`hog_catalog.last_snapshot_id`,
`next_file_id`, `hog_table_stats.next_row_id`, …) advanced with
`UPDATE … RETURNING` inside the serialized commit tail. No sequences,
no gaps, ids ordered exactly like commits.

**Integrity is declared.** Foreign keys with `ON DELETE CASCADE`,
`NOT NULL` where code assumes it, partial unique indexes enforcing
things like "one live table of this name per namespace" and "one live
deletion vector per data file", CHECK-constrained vocabularies. The
database rejects states the code shouldn't have to defend against.

## Feature walkthroughs

### Commits and concurrency control

`commit/CommitService.kt`. A commit is one SQL transaction that opens
by taking a **per-catalog advisory transaction lock**
(`persistence/Locks.kt`:
`pg_advisory_xact_lock((4740871::bigint << 32) | (catalog_id::bigint &
4294967295))` — the single-bigint form, and the key must be computed
identically everywhere or serialization silently breaks).
Every DDL and data commit tail serializes on it, which is what makes
snapshot ids dense and allocator math trivial; everything expensive a
client does (writing parquet) happened before the request, so the
serialized section is milliseconds of metadata writes.

Conflicts are optimistic and typed. A writer says which snapshot it
planned against (`read_snapshot`); the service runs one indexed query
over `hog_snapshot_change` for changes after that point that actually
matter to the write set — for appends, only `table_dropped` /
`table_altered` on the touched tables (**appends never conflict with
appends**); for deletes, additionally a per-file check that the
deletion vector being superseded wasn't itself replaced after the read
snapshot. A hit is HTTP 409 with the offending table named; the client
refreshes and retries. Omitting `read_snapshot` is a blind append with
no conflict window — except when the files carry `partition_values`,
which `HOGLAKE_REFUSE_BLIND_PARTITIONED_APPENDS` (default false) will
refuse with 422; today the server WARNs once per resolved table and
counts `hoglake_blind_partitioned_appends_total`. A *stated*
`read_snapshot` below the expiry
floor is **410 Gone** (the change rows that would prove the window
clean were expired with their snapshots; the message names the floor
and when it was reached), never a silently truncated conflict check.
Validation failures (unknown table, bad field id, malformed stats) are
422 and roll back the entire commit — a multi-table commit is atomic.

Two more admission gates run under the lock. A commit may carry an
`expected_table_uuid` per table — the incarnation guard: if the live
table's uuid differs (drop+recreate raced the writer), the whole
commit 409s with zero writes, under the typed code `table_recreated`. A
conflict that is DDL on a touched table after the request's
`read_snapshot` is 409 `ddl_since_read_snapshot`. Neither is retryable,
and both carry `tables` and `retry: re-prepare` in the body: the
recovery is to re-read the table and prepare a new request, which is
also the recovery for the below-floor 410 above. And every registered path (data file or
DV) is checked against **undrained `hog_file_removal` rows**: a path
still scheduled for physical deletion is refused with 409 — otherwise
cleanup's drain could delete an object a newer commit just made live
(path reuse). Time itself is honest too: `snapshot_time` is stamped
`clock_timestamp()` — statement time inside the serialized tail, after
the lock — so recorded times order exactly like snapshot ids even when
commits queue.

Admission is backpressured, not implicit: the commit transaction sets
a `lock_timeout` (`HOGLAKE_COMMIT_LOCK_TIMEOUT_MS`, default 30s), and
a writer that can't get the tail in time receives **503
`commit_queue_timeout` + Retry-After** — an explicit, retryable "the
catalog is convoyed" signal instead of unbounded queueing. Lock waits
are measured (`hoglake_commit_lock_wait_seconds`) on every commit, DDL,
and maintenance tail, so a forming convoy is visible before it's an
incident.

### File registration: footer-shipping and deferred stats

The writer just wrote the parquet file, so it holds the footer in
memory: it ships `record_count`, sizes, and per-column stats
(value/null/nan counts and typed min/max bounds) in the commit body,
and the server registers the file without ever opening it
(`hog_data_file` + `hog_file_column_stats`). Bounds are stored in
Iceberg single-value binary form (`stats/IcebergSingleValue.kt`) —
typed bytes, not text — so downstream metadata consumers re-encode
mechanically.

Stats may also be **deferred**: a file registers with only
`record_count` (mandatory — row-id assignment needs it) and
`stats_state='pending'`. The **hydrator** (`hydrator/Hydrator.kt`,
parquet-java) sweeps pending files in the background: a ranged S3 read
fetches the parquet footer (never data pages), stats aggregate across
row groups, columns map by embedded field id (name fallback with a
warning — resolved against the schema **at the file's
`begin_snapshot`**, never live-at-hydration, so a drop+add-same-name
between commit and sweep can't write the old incarnation's stats under
a new field id), bounds encode by catalog type, and the file flips to
`provided` — or `failed`, loudly, if the footer contradicts the
registration (a lying `record_count` is fraud, not a discrepancy). One
bad file never wedges a sweep, and a bound the codec can't represent
safely (e.g. a timestamp whose unit conversion would overflow) stores
as NULL — bounds are never guessed. Until hydrated, a pending file
simply matches every scan: correctness holds, pruning quality lags.

`null_count` is mandatory in `hog_file_column_stats`, so a leaf whose
footer omits it in any row group gets **no row** — unless the leaf's
max definition level is 0 (it and every ancestor REQUIRED), where the
parquet format guarantees no nulls and the omitted count is taken as 0.
That case is not hypothetical: ClickHouse writes its non-`Nullable`
columns REQUIRED and omits their `null_count` (it does set it on
`Nullable` ones), and before this rule such files hydrated to
`provided` with no rows for those columns (for the benchmark dataset,
no rows at all). A nullable leaf (OPTIONAL,
or under an OPTIONAL/repeated ancestor) with no count still gets no row,
with a warning naming it. Files hydrated before the rule are `provided`
with those rows missing and are never swept again; `POST
/maintenance/rehydrate` requeues only `failed` files, so backfilling them
means flipping the affected `provided` files back to `pending` (the
upsert makes a re-hydration idempotent) — not yet an endpoint.

Hydration failure has a **two-class taxonomy**. Transient fetch errors
(S3 5xx/SlowDown, timeouts, connection resets) leave the file
`pending` — logged and counted
(`hoglake_hydrator_transient_errors_total`), retried on the next sweep.
Structural errors (object missing, unparseable footer, footer
contradicting the registration) go to `failed` — terminal to the sweep
but operator-recoverable: `POST /maintenance/rehydrate` flips
`failed` → `pending` (catalog-wide or scoped to one namespace+table),
for when the cause was fixed (object re-uploaded, cap raised). Two
guard rails on the fetch itself: files without a usable `footer_size`
fall back to a whole-object read **capped** at
`HOGLAKE_HYDRATOR_MAX_WHOLE_OBJECT_BYTES` (default 256 MiB — a larger
file fails structurally with a fix-then-rehydrate message instead of
becoming an OOM vector), and the sweep claims its batch
`FOR UPDATE SKIP LOCKED`, so concurrent replicas hydrate disjoint sets
instead of amplifying S3 GETs against the same head.

The footer read doubles as the **field-id contract check**: any leaf
without a `PARQUET:field_id` flags the row
(`hog_data_file.missing_field_ids`, gauged as
`hoglake_missing_field_id_files{catalog}`; the reserved `_hog_row_id`
id on compacted files is fine). Id-less files bind columns by name, so
while one is LIVE the table's `rename_column` fails with 409
`idless_files_present` — renaming would silently NULL that column's
history in readers. The guard blocks on live **`pending`** files too:
their id state is unknown until the footer is read, so a rename can't
slip through the commit-to-hydration window. `rename_table` is
unaffected, and the flag clears from relevance when the file is
compacted away, dropped, or expired.

The same footer read also records the file's **row-group start
offsets** (`hog_data_file.split_offsets`, served on
`GET /scan?include=split_offsets`): per row group, where its first
column chunk starts (the dictionary page when one precedes the first
data page, else the first data page). They are written in the statement
that flips the file to `provided`, and the footer is authoritative — it
replaces any list the registration shipped. A file with more than
100,000 row groups, or whose footer gives no strictly increasing
in-range list, stores none. Writers that hold the footer can ship the
list themselves as `split_offsets` on the registration; it is validated
(non-empty, strictly increasing, within `[0, file_size_bytes)`, at most
100,000 entries) and a bad list is a 422, never stored or repaired. A
provided-stats registration that ships no list never passes through the
hydrator, so it simply has none — acceptable, because row-group
alignment is an optimization and even cuts are always correct.
Compaction registers the list for every output from the footer its
writer just produced, at no extra IO.

One client-side note with a server cost: pyhoglake's
`prepare_append_tables` uploads a commit's objects concurrently (the
`fast-upload` extra, bounded by `concurrency=` or
`PYHOGLAKE_UPLOAD_CONCURRENCY`, hoglake#234), so a faster client raises
files-per-commit rather than commits per second. Every per-file cost
here scales with that number — the hydrator's sweep, the expiry and
retirement arithmetic, and the commit receipt, whose stored request body
is the one that grows fastest (hoglake#240).

### Row lineage

Every append gets a contiguous row-id range per file
(`row_id_start`, width `record_count`) allocated server-side from
`hog_table_stats.next_row_id` under the commit lock. Ranges tile
`[0, total-rows-ever)` per table with no overlap and **no reuse, ever**
— a row's id is stable for the life of the table incarnation, and the
incarnation itself is pinned by `table_uuid`. This is what lets change
consumers and (future) compaction reason about identity without
guessing.

### Row-level deletes: deletion vectors

Deletes never rewrite data files. A client writes a **deletion vector**
file (a bitmap of deleted positions) to object storage and registers it
against a specific data file in a commit (`hog_delete_file`). Rules,
enforced in `CommitService` and by a unique partial index:

- At most **one live DV per data file**. A new DV supersedes the old
  (end-snapshots it) and must **cover** it — `delete_count` only grows.
- Registering a DV requires `read_snapshot`, and if the live DV was
  itself replaced after that snapshot, the commit 409s: you built on a
  stale vector, and merging bitmaps is the client's job. Lost updates
  are structurally impossible.
- A DV must target a live file of the right table, can't shrink, can't
  exceed the file's `record_count`, and ordinary numeric references cannot
  target a file created in the same commit. The explicit transaction endpoint
  additionally permits private same-commit append references by path (below).

Read planning pairs each data file with its DV *as of the requested
snapshot* (`service/ScanService.kt`, `GET /scan`) — historical scans
see historical vectors, so time travel is delete-correct. Lifecycle is
airtight at the edges too: `DROP TABLE` end-snapshots live DVs along
with the data files (so expiry reclaims both row and object — nothing
`end_snapshot IS NULL` survives a drop), and compaction end-snapshots
a group's DVs together with the inputs they mask.

A plan can also carry each `provided` file's stored **column
statistics**, so an engine prunes files before it plans splits for
them: `GET /scan?include=column_stats`, narrowed by `stats_fields` to
the field ids a pushed-down predicate names. Opt-in, because most scan
consumers cannot prune and should pay neither the join nor the
payload. Five things about it are worth knowing before you ask for
it:

- **it is the one response whose size is a PRODUCT** — provided files
  x requested visible leaf columns, at a measured 106 bytes an entry,
  which is 66 MB for a 20,000-file 30-column table. It is capped at
  `ScanService.SCAN_COLUMN_STATS_MAX_ENTRIES` (1,000,000 entries,
  ~100 MB) and a request past that is a 422 naming `stats_fields` as
  the way back under it, decided from file and column metadata before
  a statistics row is read. **`stats_fields` divides only the second
  factor**, so a table with more than a million files carrying
  statistics is past the cap at every narrowing — one column still
  costs one entry per file — and `include=column_stats` is unreachable
  for it: the engine plans without bounds. That is reachable at the top
  of the fleet, where 3M files x 15 columns is 45x the cap and one
  column of it is still 3x. The fix when a table needs it is a paged or
  filtered statistics surface, not a bigger number here, because the
  number bounds a response the server builds in memory;
- **the cap bounds the PAYLOAD, not the read.** A narrowed request
  returns fewer entries; the statement still reads the catalog's whole
  statistics relation to find them, because `hog_file_column_stats`
  carries no table id and one column's rows sit one per file across
  every page. Measured at 45M stored rows: ~1.6 s warm, ~17 s cold,
  narrowed or not. It is a planning-time cost per scan, and
  `stats_fields` does not make it cheap;
- **the plan's file list is uncapped**, deliberately and unchanged: a
  scan returning some of a table's files would be a wrong answer, where
  one returning all of them without bounds is a slow one. Only the
  statistics are bounded;
- **the counts and bounds are pre-deletion-vector.** The plan hands
  you the file's live DV alongside and nothing subtracts it. Pruning
  stays sound (deletions only shrink the live set); answering
  `count(*)`/`min()`/`max()` from these numbers does not;
- **the narrowing is in the SQL, not applied afterwards** — the field
  ids reach the statement as one array parameter, so a one-column
  request ships one row per file out of Postgres rather than thirty.
  See `ScanColumnStatsQueryPlanIntegrationTest` for the plans in all
  three shapes, for the four index candidates that were measured and
  rejected, and for the one alternative (CLUSTER) that does change the
  answer and why it is not taken.

A plan can also carry each file's **row-group start offsets**, so an
engine cuts byte-range splits on row-group boundaries instead of at
even offsets that land mid-row-group: `GET /scan?include=split_offsets`
(combinable: `include=column_stats,split_offsets`). Entry i is where row
group i's FIRST column chunk starts — parquet-java's
`ColumnChunkMetaData.getStartingPos()`, never the thrift
`RowGroup.file_offset` — and a list is only ever served whole, strictly
increasing and inside `[0, file_size_bytes)`; a file without one simply
carries no property and is cut evenly. The offsets live on the file row
(`hog_data_file.split_offsets`), so asking adds no statement to the
plan. Like `column_stats`, it is a scan-plan-only property: the files
listing and the changefeed never carry it. Where the lists come from is
under "File registration" above.

### Partitioning

A table carries a versioned **partition spec** (`hog_partition_spec` /
`hog_partition_field`): ordered fields of (source `field_id`,
transform, optional param), where transforms are the closed set
`identity | bucket(n) | year | month | day | hour`. `bucket` uses a
32-bit Murmur3 with pinned semantics so every writer hashes
identically. Writers compute transformed values themselves and ship
one opaque string per spec field with each file registration; the
server validates arity against the live spec, stamps the file with its
`spec_id`, and stores values in `hog_file_partition_value`. Files
remember the spec they were written under, so spec evolution never
rewrites history — pruning just knows which vintage each file is.

**Trust boundary**: partition-value *correctness* is writer-trusted —
the server validates structure (spec arity, key indexes) but never
opens data files at commit time, so it cannot verify that a shipped
value is the true transform of the file's rows (e.g. that a bucket
value lies in `[0, n)`). A wrong value mis-prunes reads of that file.
This is the one deliberate waiver of the server-validates-structure
rule; registration *paths*, by contrast, are validated against the
catalog's `data_path` prefix at commit.

### Schema evolution

`service/AlterService.kt`, `POST /alter`: a list of typed operations —
`add_column`, `drop_column`, `rename_column`, `promote_column`,
`rename_table`, `set_partition_spec`, `set_sort_order`, and the V11
metadata ops `add_column_with_metadata`, `set_table_comment`,
`set_column_comment`, `set_properties` (`versioned-table-metadata-v1`)
— applied **in
order, atomically, as one DDL commit** (one snapshot, one
`table_altered` change row, one schema-version bump). Renames keep the
`field_id` (end the old row, begin a new one with the same id), so
files written before a rename still bind correctly. Type promotion is
a strict widening lattice (`int→long`, `float→double`) chosen so
existing files remain readable under the new schema — and a promote
**re-encodes the table's existing stats bounds** (4-byte → 8-byte
Iceberg encoding) in the same transaction, so nothing downstream ever
decodes old-width bounds under the new type. Validation runs against the state produced by
earlier ops in the same request, so `[add tmp, rename tmp→final]` works
and `[drop x, rename x→y]` fails precisely. You can't drop the last
column, or a column the live partition spec depends on.

### Time travel

Every point-in-time read (`getTable`, `/files`, `/scan`) takes
`?snapshot=` or `?at_timestamp=` (mutually exclusive). Timestamp
resolution is one indexed lookup: the largest snapshot with
`snapshot_time <= t`; after head resolves to head; before the earliest
*retained* snapshot is 410.

`Table.read_snapshot_id` names the snapshot a read resolved at — the
catalog head when the request named no `snapshot`/`at_timestamp`, the
resolved snapshot otherwise. It is required and always present, on every
path including `totals=false`, because it describes the READ and not the
totals; a writer caches the response and sends it back as a commit's
`read_snapshot`, which the existing OCC then validates.

The three table-level aggregates (`record_count`, `file_count`,
`file_size_bytes`) are three-way, and OPTIONAL on the wire:

- On a TIME-TRAVEL read (`snapshot` or `at_timestamp`), and on a
  createTable/alterTable receipt, they are aggregated from the manifest
  and exact at that snapshot, with no freshness fields — a scan is the
  only correct answer for a past snapshot, and an exact number has no
  age.
- On a HEAD read they come from the maintenance sampler's published
  generation (the same source `/maintenance/status` and
  `/stats/partitions` read, summed per table — `persistence/TierTotalsRepo.kt`),
  so the request reads one indexed row set and never the manifest.
  `totals_snapshot_id` names the snapshot they are exact at and
  `totals_as_of` when it was captured; expect minutes to tens of minutes
  behind head. They are ABSENT when the published generation is not an
  answer for the table, and absent means NOT SAMPLED, never zero.
- Under `totals=false` all five are absent. That is the identity read,
  and what a per-flush writer should send.

The namespace listing is the asymmetry: `TableSummary`'s three totals
are still required and still aggregated from the manifest, exact at the
catalog head, so the two endpoints can disagree by the sample's age for
the same table (#235).

### The changefeed and consumer offsets

`GET /changes?from_snapshot&to_snapshot` returns the metadata plan for
what happened in `(from, to]`: data files appended and deletion vectors
registered, in snapshot order, with row-id ranges. The client reads the
parquet itself — the feed is planning, not data. Paired with it,
**consumer offsets are catalog state**
(`PUT /consumers/{id}/offsets/{table_uuid}`): monotonic (regression is
a 409), never below the expiry floor (410 — an expired offset would
otherwise wedge the consumer-floor sweep at zero work forever), keyed
by `table_uuid` so incarnation changes are visible, and — critically —
**respected by retention** (below). Together these are the
log primitives: a replicator's entire state machine is
"changes → apply → commit offset."

Offsets survive a drop by design, so a consumer can finish reading a
dropped table — but an incarnation that **atomic replacement** retired
is a different case: the consumer reconciles onto the new `table_uuid`
and never looks at the old one again, so its row there would pin the
consumer-floor sweep at a pre-replacement snapshot forever, with nothing
left that could ever advance it. Such rows are **released** — deleted, not
advanced — in two places, both in `persistence/OffsetRepo.kt`. On the
commit path, `releaseAncestorsOf` walks **backward** from the row just
committed, following `replaced_table_id`: a primary-key lookup per hop
and, on the overwhelming majority of commits, zero hops. (Walking
forward there would cost O(that consumer's offset count) on every
offset commit, including on the catalogs — most of them — that have
never had a replacement.) The expiry sweep has no row in hand, so it
uses the **forward** `releaseSupersededOffsets` across every consumer,
clearing rows stranded by replicas predating this so its floor is honest
rather than merely filtered. A row releases when a replacement
descendant of its incarnation carries an offset for the same consumer at
or past that descendant's `created_snapshot`; both walks are recursive,
because a consumer down across two replacements reconciles straight to
the newest and both ancestors must go. Same-consumer is load-bearing:
another consumer that has not reconciled keeps its position and keeps
pinning.

Two scope notes. The sweep's release is **not** gated on
`consumer_floor` — the rows are dead either way, and `GET /consumers`
should not show them — but it *is* gated on
`snapshot_retention_seconds` being set, because it runs inside the
sweep; a catalog with retention disabled releases only through
`commitOffset`. And a released row **disappears from `GET /consumers`**,
which is a visible contract change from "offsets outlive drops".

The lineage is **recorded, not derived**: V14 adds
`hog_table.replaced_table_id`, written by `CatalogService.createTable`
when it publishes a replacement, so the hop is one indexed equality
(`hog_table_replacement_lineage`, partial on `replaced_table_id IS NOT
NULL`). The derivation `old.dropped_snapshot = new.created_snapshot` is used
**exactly once**, in V14's one-time backfill, which has run in all three
environments. The derivation is **not** used
at runtime, for two reasons. Nothing enforces it, and the cost of it
being wrong is deleting a consumer's position on a table it is still
draining.
And it is unindexable in the direction the walk needs: the hop seeks the
*successor*, whose own `dropped_snapshot` is NULL at the end of every
chain, so a partial index on `dropped_snapshot` cannot serve it and the
walk degrades to a sequential scan of `hog_table` per hop — in the
commit tail, on catalogs holding tens of thousands of dropped tables.
The walk terminates on `created_snapshot` strictly increasing rather
than a hop cap, because a cap made an origin more than N replacements
behind permanently unreleasable, which is the bug being fixed.

### Retention, expiry, and safe file removal

Retention is a **catalog property** (`PATCH /options`:
`snapshot_retention_seconds`, `consumer_floor`), enforced continuously
by background sweeps (`service/ExpiryService.kt`), not by an external
scheduler. A sweep is **two phases**, and the split is the fix for the
2026-10-01 prod-us incident (see below).

**Phase A — the floor advance, under the commit lock, one bounded
transaction** carrying its own `statement_timeout` (derived:
`min(session/4, admission/2) / 11` statements, 1,363 ms each at the
defaults, so the whole hold is at most half the 30 s admission window).
Compute the new floor as the minimum of (age cutoff, head−1, current
floor + batch, and — when `consumer_floor` — the **minimum consumer
offset**, so expiry can never outrun a lagging consumer; the pinning
consumer is named in the result and the audit line). Then snapshots are
removed with **range deletes**, never id lists, and
`earliest_snapshot_id` advances — capturing
the new floor snapshot's `snapshot_time` into
`hog_catalog.earliest_snapshot_time` in the same update, so once the
rows below the floor are gone the catalog can still say WHEN the floor
was reached; requests reaching below it get **410 Gone** with
reconcile instructions citing that time — a consumer is told its feed
has a hole (and since when) rather than silently skipping one. A fifth
step applies the same reachability rule to the accumulating versioned
DDL tables (`hog_table_version`, `hog_column`, `hog_partition_spec`,
`hog_sort_spec`, `hog_view`): rows with
`end_snapshot <= earliest_snapshot_id` are invisible at every retained
snapshot and are deleted (spec fields cascade), so DDL churn cannot
grow the metadata without bound. Those five tables are bounded by DDL
volume rather than by file volume — thousands of rows on prod-us, not
millions — which is why they stay in the locked transaction while the
file rows do not.

**Phase B — the file purge, outside every lock, paged.** A
`hog_data_file` or `hog_delete_file` row whose `end_snapshot` is at or
below the committed floor is unreadable (a read below the floor is
refused 410) and its *eligibility* is immutable: every writer of
`end_snapshot` either carries `end_snapshot IS NULL` or takes only ids it
re-read as live under the commit lock, and a commit tail writes the NEW
HEAD there, which is above the floor by definition. So the rows need no
lock to delete — and the page's data-file `DELETE` repeats the
eligibility predicate beside its `ctid` array anyway, because a `ctid`
array is not a qual a concurrently-updated row gets re-checked against.
What that buys is a SKIPPED ROW: the page purges one fewer than it
examined, the walk carries on, and the row waits for a later sweep. It is
a complete defence only because the vector delete keys off what the
data-file delete actually removed, so a skipped file keeps its vectors
instead of losing them to a statement that assumed it was going.

**Each page is one statement in one transaction** under its own 5 s
`statement_timeout`: it picks up to `HOGLAKE_EXPIRY_PURGE_PAGE` eligible
data files, deletes them, deletes the delete vectors of the files it
actually removed (live or superseded — the FK cascade would otherwise
take them with no `hog_file_removal` row, which is a permanently leaked
puffin object), and inserts every path into the cleanup queue, all in the
same statement. So
a path is queued iff its row is gone, the vector ordering is a fact about
that page rather than a claim about a whole table, and a crash between
the phases can orphan nothing. A second, separate arm pages the
*superseded* vectors — the ones whose own `end_snapshot` is below the
floor and whose data file is still live — and is probed with an `EXISTS`
first, because it has no index yet (the partial
`(catalog_id, end_snapshot)` index that the arm split now makes choosable
is ticketed with the migration it needs).

**A page is `HOGLAKE_EXPIRY_PURGE_PAGE` data files, not that many ROWS.**
Each file brings its cascade (~26 `hog_file_column_stats` rows and its
partition values) and its vectors, and while
`hog_delete_file_one_live_per_data_file` bounds *live* vectors to one per
file, nothing bounds *superseded* ones — vectors only grow, and each
supersession leaves an ended row. So one page deletes `page × (1 +
supersession depth)` vector rows and queues that many paths. On every
table any writer in the fleet produces today that depth is 0–1 (millpond
appends and never deletes; a delete commit supersedes at most once per
flush), which is why the default page is sized on the stats cascade — but
a table with deep vector history is the shape that makes a page expensive
in a way the row count does not show, and the 5 s statement bound plus
the halving ladder are what bound it. The vector delete probes
`hog_delete_file_data_lookup` once per deleted file (measured: a nested
loop, ~2.2 buffers a probe against a 44,000-row vector relation), so the
*lookup* cost is O(page) whatever the depth; it is the rows it finds that
are not.

**A floor of 0 skips phase B entirely** — nothing can satisfy
`end_snapshot <= 0` — so a catalog that has NEVER expired pays nothing at
all. One that expired before its retention was switched off keeps its
floor, and still pays an index-only data-file page plus the superseded
probe every sweep; that is deliberate, because its backlog still has to
drain.

The rate, done properly — `BackgroundLoops` is fixed **delay**, not fixed
period, and `runOnceAllCatalogs` sweeps serially:

    rows/sweep      = HOGLAKE_EXPIRY_PURGE_BUDGET_MS / page cost
    period          = HOGLAKE_EXPIRY_INTERVAL_MS + Σ (every catalog's sweep)
    rows/min (hot)  = rows/sweep × 60000 / period

At the compiled defaults (page 1,000, budget 10 s) and production's
measured 700–870 µs per file that is **10,000–14,000 rows a sweep**. With
K catalogs behind on one maintenance pod and prod-us's 15 s interval the
hot catalog gets `12,000 × 60/(15 + 10K)` rows a minute: **~29k at K=1,
~16k at K=3, ~11k at K=5** — not the 40–56k a fixed-period reading would
suggest. Compaction retires ~12,288 files per **~110 s wave** (six groups
of 2,048), i.e. ~6,700 a minute, so the margin is ~4× at K=1 and is gone
somewhere around K=8. When it goes, the symptom is a standing
`purge_truncated` and a rising `hoglake_expiry_purge_remaining`, and the
levers are fewer catalogs per maintenance pod or a larger budget — not a
larger page.

The per-row cost is the row plus its cascade, not a constant of the code:
`ExpiryPurgeCostMeasurement` measures a 100,000-row fixture with all
three cascades a production row carries (26 `hog_file_column_stats` rows,
one `hog_file_partition_value`, the vector probe) and prints the figure
and the ratio on every run — production's stats relation is 66 GiB and
does not fit cache, which is the distance from tens of microseconds to
870. A page that cannot finish inside its 5 s bound is **halved and
retried inside the same run**, down to a page of one, with the halvings
counted in `hoglake_expiry_halvings_total{phase="purge"}`; without that,
one poison page would be retried first on every sweep forever and nothing
would drain. `HOGLAKE_EXPIRY_PURGE_PAGE` (default 1,000, refused above
50,000 at boot) and `HOGLAKE_EXPIRY_PURGE_BUDGET_MS` (default 10,000; 0
with the loop ON is refused at boot, because it would advance the floor
forever and purge nothing while every ledger row read healthy) are the
knobs. Neither has a chart template today, so changing one in an
environment needs chart work first. `HOGLAKE_EXPIRY_BATCH` is unchanged
in meaning: it bounds SNAPSHOTS, and therefore phase A's
`hog_snapshot_change` cascade — and it is halved and retried the same way
when phase A's own statement bound fires
(`hoglake_expiry_halvings_total{phase="advance"}`).

**What to alert on.** A sweep that spends its budget, hits a failed page,
or cannot read the floor reports `purge_truncated` with `purge_remaining`
(saturating at 100,000) and `purge_failures` on its ledger row, and the
same facts are series: `hoglake_expiry_purge_truncated_total` (the one to
alert on — a chronically budget-starved purge never *fails*, so
`…_purge_failures_total` stays at zero and `…_purge_rows_total` looks
healthy forever), `hoglake_expiry_purge_remaining` (a gauge: how far
behind), `hoglake_expiry_purge_failures_total` (a page that could not
finish even at one row), `hoglake_expiry_advance_failures_total` (the
floor itself is stuck — retention is not being enforced at all) and
`hoglake_expiry_halvings_total{phase}` (a knob is wrong for the work).
The halvings are also on the ledger row (`purge_halvings`,
`advance_halvings`) and badged in the console, because for a sweep whose
every page hit the 5 s bound they are the only count that moves: the rows
are zero and the pages are the rungs. A reduced page size is carried to
the next sweep, so that ladder converges in a few sweeps instead of
restarting from the configured page every time. **Phase B
runs even when phase A throws**, and even when phase A advanced nothing,
so a failed advance does not starve the purge; the ledger row is `failed`
and still carries what the purge did.

**The consequence for observers:** ended rows below the floor can now
exist for a few sweeps. #262 taught `/verify`'s `expiry_floor` to assert
its `hog_data_file` / `hog_delete_file` arm only against a sweep whose
own ledger row reported the purge DRAINED (and, after that PR's review
round, only when that row's floor covered the catalog's current one), so
that a drained purge which still left rows below the floor — the purge's
predicate and the floor advance disagreeing — stayed a violation while
the designed lag did not. #261 removed the check, so that invariant is
**unwatched until the paged scrubber lands**. What still holds is the
BACKLOG signal, which never went through verify: `purge_truncated`,
`purge_remaining` and `purge_failures` on every ledger row, plus
`hoglake_expiry_purge_truncated_total` and
`hoglake_expiry_purge_remaining`.
`ExpiryPurgeIntegrationTest.a drained purge's ledger row names the floor
it drained AT, not the catalog's current one` pins the ledger fact the
comparison stands on, so the scrubber has something to build against.
And one counter can now fire
from normal operation: two file rows may legitimately share one path
(`V16`'s index is non-unique for exactly this reason), and purging one
while the other survives queues a path the catalog still claims, which is
what `removal_queue` and the drain's `still_referenced` report — a
counter AGENT.md's invariant 4 calls alerted. Where the surviving row is
LIVE that was already true before this change; what is new is that two
rows both *eligible* can land in different pages, so the window can also
last a page (a sweep, if the budget stops between them). Nothing is
deleted in either case — refusing is the drain's whole purpose — and
closing it means expiry taking each page's whole path-closure, which is a
separate change.

**Why it is two phases.** It used to be one, and the file GC rode it:
one unbounded `DELETE FROM hog_data_file WHERE end_snapshot <= floor`
under the commit lock, plus its cascade into `hog_file_column_stats`
(26 rows per file, 66 GiB), `hog_file_partition_value` and
`hog_delete_file`. Compaction 1.3.7 commits six groups of up to 2,048
files, so each sweep met one whole compaction wave: 12,288 rows, 17–25 s
of lock hold per minute, ten commits queued behind it when sampled, the
commit-lock wait p99 pinned at the 30 s admission bound for sixteen
hours, and API p99 of 20–60 s across routes because parked commits held
the API pods' pool connections — with RDS idle at 8% CPU the whole time.
Shortening the sweep interval to 15 s shortens each hold and leaves the
duty cycle where it was.

Physical deletion is decoupled and paranoid
(`service/CleanupService.kt`): the queue is a *suggestion*. At drain
time every path is re-checked against live references — a
still-referenced path is skipped and counted as an **invariant
violation** (alertable), never deleted. Missing objects count as done.
That re-check is ONE statement per sub-batch and it is INDEXED: it
probes `hog_data_file`, `hog_delete_file` (V17's `(catalog_id, path)`)
and `hog_upload` (V12's unique key) by path, so its cost sizes with the
sub-batch and not with the catalog's manifest — before V17 it read
every file row the catalog has ever registered, once per sub-batch
(190,884 buffers and 692 ms at 5M rows, against 8,728 and 33 ms after).

**The drain is a claimed work queue, and it takes no catalog lock
anywhere** (V21). Each sub-batch is three steps with the transaction
boundaries as the design:

1. **CLAIM** — `UPDATE hog_file_removal SET claimed_at = now(),
   claimed_by = :worker WHERE removal_id IN (SELECT ... FOR UPDATE SKIP
   LOCKED) RETURNING`, one statement in its own transaction, committed
   **before the first object-store call**. So no transaction is open
   while a worker waits on S3, and `idle_in_transaction_session_timeout`
   bounds nothing here.
2. **WORK** — the reference check, then the deletes. No transaction, no
   lock, nobody waiting.
3. **SETTLE** — one short transaction, fenced on `drained_at IS NULL AND
   claimed_by = :worker` and `RETURNING` the rows it actually took. The
   ones it did not are `settled_elsewhere`.

The lock it used to hold protected nothing, and cost ~19 s per 1,000-row
sub-batch on gigahog-prod-us (2026-09-24), during which every commit on
the catalog waited — which is why cleanup was off in production until
the lock came out. Cleanup is ON in prod-us since 2026-09-29, at 60 s /
batch 10,000 / 1 worker; dev and prod-eu carry no `maintenance:` block
at all and so run the chart defaults (30 min / batch 2,000). The queue
stood at ~2.6M undrained rows growing ~190k/h on 2026-09-29, about 9.4k
of them orphaned `compaction_staging` tickets past their grace, and the
drain-down cleared it that day.

The reason the lock was unnecessary is the pair it was half of: a commit
passes the path-reuse guard only when no UNDRAINED row names the path, and
the drain reads only undrained rows — a claimed row is still undrained, so
the refusal covers the whole time a path is somebody's work. What remains
is a queue INSERT racing a registration, and there are **four inserters
with three serializers**, which is worth stating because two of them take
no commit lock:

- `ExpiryService` and `RetirementService` insert under the commit lock,
  and only for paths whose file rows they have just deleted;
- `CompactionService.stageOutputPath`'s initial stage takes **no** lock
  (autocommit, before the rewrite): safe because the path is a UUID that
  sweep just minted and nobody else knows. Its re-stage runs inside
  `commitGroup` and so under the lock;
- `UploadService.fenceAndQueue` takes **no** commit lock either, and
  deliberately — taking one would convoy the catalog. It is serialized
  against a concurrent registration by the `hog_upload` **row lock**:
  `UploadService.register` takes `SELECT ... FOR UPDATE` on the claim rows
  inside the commit transaction and raises `CommitConflict` unless the
  claim is still active, while the sweep re-evaluates its candidate
  predicate after waiting on that lock. This is the arm the removed commit
  lock was actually standing in for.

The standing constraint that falls out: a commit that registers a path
with **no `hog_upload` claim** has no serializer against a concurrent
queue insert for that path. It is unreachable today because paths are
never reused and every externally-supplied path arrives through an upload
claim — a future path that is neither must bring its own serializer.

`FOR UPDATE SKIP LOCKED` is what lets `HOGLAKE_CLEANUP_WORKERS`
(default **1**) workers per run — and any number of replicas —
partition the queue with no coordination and no single-flight lock: a row
another claim holds is skipped rather than waited for.
`HOGLAKE_CLEANUP_BATCH` is a budget **per worker**, so a run asks for
`workers x batch` rows and the standing rate is

    rows/h = workers x batch x 3600000 / HOGLAKE_CLEANUP_INTERVAL_MS

which is **4,000 rows/h at the compiled defaults** — a deliberately
conservative floor, and nowhere near a busy catalog's arrivals. The
defaults do not clear a backlog; the values that do live in the chart, and
they are a pair (a batch large enough to keep the workers busy, an
interval near a run's own duration, which the reference check sets at ~20 s
cold for 4 x 1,000 rows). A worker holds a pooled connection across that
check, so `HOGLAKE_COMPACTION_PARALLEL_GROUPS + HOGLAKE_CLEANUP_WORKERS`
is refused at boot above `HOGLAKE_DB_POOL_SIZE -
FOREGROUND_CONNECTION_RESERVE`: at the default pool of 10 with reserve 4
and 6 groups, no worker count is legal, which is why prod-us raised
`dbPoolSize` to 16 — that is what makes 1 worker (and up to 6) legal. A claim is a
**lease**, not a lock (`HOGLAKE_CLEANUP_CLAIM_LEASE_SECONDS`, 900 s): a
worker killed between its claim and its settle leaves rows claimed until
the lease lapses, and `DeleteObjects` is idempotent so the re-drain
settles exactly as the first attempt would have. Note that
`HOGLAKE_CLEANUP_SUB_BATCH`, `HOGLAKE_CLEANUP_CLAIM_LEASE_SECONDS` and
`HOGLAKE_CLEANUP_STAGING_GRACE_SECONDS` are server-default-only today:
the chart has no template for any of them, so they cannot be set in any
environment, and changing one needs chart template work first. The
`claimed_by` fence
is the other half of that: a worker whose lease lapsed while it was
talking to S3 can no longer write the ledger, so it cannot stamp its
outcome over the worker that took the row from it. The fence is also
where `still_referenced` is COUNTED from — the attempts bump returns the
rows it really touched, and a compaction group that commits between a
claim and its reference check makes the path live without being a
violation.

Two writers can settle a `hog_file_removal` row: this drain, and a
compaction group's commit settling its own staging ticket
`'registered'`. `CompactionService.commitGroup` re-reads the ticket `FOR
UPDATE` and takes it **only if nobody has touched it** — `drained_at IS
NULL AND claimed_at IS NULL AND attempts = 0`, the same three terms in its
settle — otherwise it re-stages the path and skips the group. A LAPSED
claim counts as touched, and that is the point: an expired claim does not
mean the object survived, it means nobody knows, and the row lock cannot
stand between a DELETE no transaction holds and a registration. The three
routes to a touched-but-unsettled ticket are a worker past its lease, a
worker killed after its DELETE, and a lost `DeleteObjects` response (which
bumps `attempts` and releases the claim); each costs one group's CPU and a
fresh UUID path next sweep, rather than a live file row pointing at a
deleted object. Compaction therefore never reads the lease — it is
cleanup's alone. The re-read's wait is at most one claim transaction (a
single UPDATE), because the claim skips locked rows and never waits on
compaction.

The drain also **reads back the rows its settle did not match** and
classifies them: another writer's `'registered'` over a path this
sub-batch had not deleted is ordinary contention (`settled_elsewhere`), a
claim that moved is a lapsed lease (a WARN naming both workers), and a
`'registered'` over a path this sub-batch **had** deleted is an invariant
violation — an ERROR, a `cleanup_violation` audit event and a counted
`still_referenced`. That last case is unreachable given the re-read above;
it is instrumented because nothing else in the system could see it (every
`staging_tickets` arm passes once the file row exists).

A bulk sub-batch's deletes are one `DeleteObjects` call **per bucket
chunk** — 1,000 keys is S3's own ceiling on one request, which is where
the default sub-batch (and therefore the size of one claim) comes from —
rather than the `25 x (HeadObject + DeleteObject)` it used to be. That
old shape held the commit lock ~3.2 s per sub-batch and ~255 s per
2,000-row run, which took commit latency on gigahog-prod-us from
200-400 ms to 12-22 s against a 30 s admission bound and got both API
pods liveness-killed. What scales the work is the number of REQUESTS,
not the sub-batch: keys past 1,000, or spread across buckets, chunk into
more requests, so raising the knob past the ceiling buys nothing.

`reason = 'compaction_staging'` rows are the carve-out, on three
counts, and they are **claimed by their own statement** so that the lease
bounds the work one claim carries: 25 tickets is 50 round trips (500 s at
the call bound, inside the 900 s lease), where a single claim of 1,000
tickets would have been 20,000 s of calls under one lease. They keep HeadObject + DeleteObject, because a `DeleteObjects`
response cannot distinguish a key it removed from one that was never
there, and that distinction is recorded as `'absent'`. Its reader was
`/verify`'s `staging_tickets` check, removed in #261, so the value is
written and unread today — kept because it cannot be reconstructed
after the delete. Because that costs two round trips per row,
they are claimed and settled 25 at a time — a unit of progress (each
sub-batch settles in its own transaction) and the unit the lease bounds.
And the drain leaves them alone until they are past
`HOGLAKE_CLEANUP_STAGING_GRACE_SECONDS` (1 h), because the ticket is
inserted before the rewrite starts and settling a fresh one while its
group is still uploading leaves the object with no ticket naming it;
that grace used to have a ceiling — it plus the cleanup interval had to
stay well under `/verify`'s 6 h staging-ticket age, or the check alerted
on tickets the drain was deliberately leaving alone — and #261 removed
the check, so nothing bounds it today. Every other reason settles
`'deleted'`, whether or not the key was there.

The object-store call bound (`RemovalStore.apiCallTimeout`, 10 s at the
defaults, derived as `min(idle bound, admission bound) / 3` so an
operator who lowers `HOGLAKE_COMMIT_LOCK_TIMEOUT_MS` gets shorter calls
with it) is no longer an inequality against a lock hold: nothing is open
across a call. What a hung call costs is one parked worker holding a
claim until its lease expires. The `deadline_skipped` counter and the
`HOLD_BUDGET` arithmetic it belonged to bounded that hold and are gone;
the counter stays on the wire, always 0, because the maintenance ledger
holds rows that carry it.

`hoglake_files_removed_total` counts `objects_removed` — DISTINCT paths
physically deleted — and not `removed`, which counts ledger rows: two
undrained rows over one path are legitimate state and one batched
delete settles both.

The ledger's own retention purge is a **loop of bounded deletes** that
pages the primary key from the bottom, where an append-only queue keeps
its oldest rows: up to `LEDGER_PURGE_PAGE` ids per page through the key,
delete the ones past the cutoff, stop at the first page with nothing to
purge or when the wall budget expires. It used to be one
`DELETE ... WHERE catalog_id = :c AND drained_at < cutoff`, which **no
index on this table can serve** — both are partial on `drained_at IS
NULL`, the complement of the rows it deletes — so it was a sequential scan
with an unbounded row count, once per catalog per run, against a ledger
heading for ~137M rows at 190k/h drained and the 30-day default. That is
the shape of the 2026-09-28 expiry-lock outage, and it is why this is
bounded before cleanup is re-enabled rather than after. The cost of the
early stop is stated rather than hidden: a page with nothing to purge ends
the walk, so rows above a long-undrained row wait for the run after it
settles.

Draining **soft-deletes**: a settled `hog_file_removal` row keeps its
place with `drained_at` + `drained_outcome` (`'deleted'` for a
physical removal, `'absent'` for verified-already-gone), so "what did
cleanup touch, when, after how many attempts" is queryable instead of
dying with the row; skips and transient failures bump
`attempts`/`last_attempt_at` and stay queued. The drain reads only
undrained rows (partial index), and each sweep purges drained rows
older than `HOGLAKE_REMOVAL_LEDGER_RETENTION_SECONDS` (default 30
days) so the ledger never becomes its own unbounded-growth problem.

### Sort orders and compaction

A table may carry a versioned **sort order** (`hog_sort_spec` /
`hog_sort_field`, set via the `set_sort_order` alter op) — advisory for
writers (the server never verifies file sortedness), binding for
compaction. **Compaction** (`compaction/CompactionService.kt`,
`POST /maintenance/compact` + an off-by-default loop) merges small live
files: candidates share (spec_id, partition_values) and pack to a byte
target — row-id adjacency is NOT required, which is why
outputs materialize their row ids as an explicit `_hog_row_id` int64
column (reserved field id 2147483646, flagged by
`data_file.explicit_row_ids`) instead of relying on position. That
makes sorting on rewrite safe — the predecessor's sorted-compaction
rowid remap is structurally impossible. The rewrite (parquet-java —
the project's one parquet library, shared with the hydrator's footer
reads) happens entirely before the commit transaction; the commit
re-verifies each input is still live under the exact planned identity
(plan-to-commit races skip the group), end-snapshots inputs (time
travel keeps them; expiry reclaims them later), registers the output's
stats from the footer the rewrite just wrote — the same
`FooterStats.aggregate` the hydrator runs, so the counts and bounds are
exactly the rows the writer emitted and no input stats row is read at
all — and the changefeed excludes compacted outputs so
consumers never see merged rows re-appear as fresh appends.

Grouping is ONE PASS of ordinary **bin packing**, the shape Iceberg's
`rewrite_data_files` uses. For each partition/spec bucket, sort the
candidates by size and close a group as soon as its input bytes reach
`HOGLAKE_COMPACTION_TARGET_BYTES` (512 MiB) **or** it holds
`HOGLAKE_COMPACTION_MAX_INPUT_FILES` (default **64**); carry on with the
rest, and emit the trailing remainder as a group as well.

**By size, not by row id**, and that is load-bearing. A compaction
output takes `row_id_start = min surviving id` of its inputs, so it
sorts in front of the newer, smaller files. Packed in row-id order, a
large output ate most of the group's quota, the group closed a file or
two later, and the whole group was then discarded for holding too few
files — permanently, because row-id order never changes. Simulated over
300 ticks of steady ingest and a full drain, 100 MiB ingest files left
373 small files stranded forever; size-ordered, the same workload drains
to none. Row-id adjacency is not required (outputs carry explicit row
ids), so nothing else depends on the old order.

A group is then dropped unless it holds
`min(HOGLAKE_COMPACTION_MIN_INPUT_FILES, target / its largest file)`
files, floored at 2. **The minimum scales with the files it judges**,
because a fixed file count cannot judge a byte target: no group can hold
five files that are each over a fifth of the target, so a fixed 5
silently means "never compact this bucket" for any bucket with files
that big — including every SORTED table, whose effective target is
derated to fit its sort buffer in heap. Silently, because no group forms,
so nothing is refused and nothing is logged.

The minimum is a **write-amplification** knob, not a termination
condition: compaction terminates at any value >= 2, since a group turns
N >= 2 files into exactly one and the bucket's file count strictly
decreases. What a low value costs is repeated rewriting — compression
puts every output back under the target, so at 2 it converges on the
target from below one rewrite at a time, roughly 4x the bytes moved.
That is the ladder by another name.

Measured at the default of 5: draining a static backlog costs about
**2x** the input bytes, because ingest-sized files reach the target band
in one rewrite and the outputs consolidate pairwise from there. So the
claim is one rewrite to reach the target band, not one rewrite per file
for all time — and without the dominance split described above, steady
state is a 5.6x LOSS. This replaced a geometric ladder of size tiers
(#151, V10), which rewrote the same bytes once per rung while every rung
above the first saved nothing.

`HOGLAKE_COMPACTION_MAX_GROUPS_PER_RUN` (default 1) caps executed
attempts per catalog, including failures and skips.

### Concurrency: the fixed per-group cost, and the two knobs for it

A compaction group costs a **fixed** amount of wall time almost
regardless of what it holds, because the cost is object-store LATENCY
rather than bytes: measured on gigahog-prod-us (2026-09-24,
`main.events_raw`) at about **8.5 s per group** — the serialized input
opens, the plan, the commit. A sweep that runs those groups one at a
time drains files at a rate set by that constant and by nothing else.

`HOGLAKE_COMPACTION_PARALLEL_GROUPS` (default **1**, which is the
sequential sweep this server has always run: no executor, groups on the
calling thread) rewrites and commits that many of a sweep's planned
groups at once. Groups share no input file by construction — one pass
of bin packing over disjoint buckets — so the only serialization point
between them is the per-catalog commit lock, and the commit transaction
takes that lock ALONE: never across the rewrite or the upload.

The sweep plans and executes **per table**, in name order, and that
ordering is deliberate: a plan is metadata-only and cheap, and its value
decays fast, so it is taken as late as it can be. Planning every table
up front would leave the last table's plan as old as every rewrite
before it — at 64 groups a sweep and ~8.5 s a group, minutes of ingest
and of a sibling replica's commits between the read and the attempt.

A wave is joined WHOLE before the next one starts, so the slowest group
in a wave bounds it, and a wave is drawn from ONE table's queue — a
table holding fewer groups than `parallelGroups` runs at its own group
count, not at the knob's. Both follow from planning per table, and both
mean the knob is a ceiling rather than a promise.

Three things to raise with it: the maintenance pod's CPU (the rewrite is
CPU-bound on zstd), the sorted-path heap, which this knob DIVIDES
(below), and **`HOGLAKE_DB_POOL_SIZE`** (default **10**).

The pool is not optional. Every in-flight group holds a pooled
connection across its commit-lock wait, and taking too much of a pool
the foreground shares turns a writer's commit backpressure from the
typed 503 the admission contract promises into a connection-pool
timeout served as a 500 — with nothing in the 500 naming the knob that
caused it. So the server **refuses to boot** when

```
HOGLAKE_COMPACTION_PARALLEL_GROUPS > HOGLAKE_DB_POOL_SIZE - 4
```

naming both variables in the failure. The four are a FLOOR, not a model
of demand: the hydrator, expiry, cleanup, retirement and the metrics
sampler draw on the same pool, and a busy instance's foreground wants
more than four of its own. Raise the pool with the knob — at the
default pool of 10 the ceiling is 6.

**`HOGLAKE_REQUEST_THREADS`** (default **`HOGLAKE_DB_POOL_SIZE`**) is
the pool's other customer, and it is sized against the same arithmetic
(#218). It is the width of the blocking dispatcher every route handler
runs on, so it is the ceiling on concurrent requests inside a blocking
call — and nearly every handler's first act is to borrow a connection.
Handler threads ABOVE the pool therefore buy no concurrency for those
routes: they queue inside `HikariPool.getConnection`, which gives up
after 5 s and throws a 500, instead of queueing in the dispatcher where
the wait is FIFO, measured (`hoglake_request_queue_wait_seconds`) and
shed with the typed 503 once it passes the admission bound. The server
**refuses to boot** on

```
HOGLAKE_REQUEST_THREADS > HOGLAKE_DB_POOL_SIZE
```

for the same reason it refuses the compaction inequality: raising it is
a pair of knobs, and forgetting the second one converts dispatcher
queueing into Hikari timeouts fleet-wide.

What the default does NOT promise is a reserve for the loops. On the
API workload the chart turns every background loop off, so the
foreground is the pool's only customer and `requestThreads ==
dbPoolSize` is exact. On a pod that runs loops AND serves traffic —
the maintenance Deployment, a dev stack, anything on the defaults —
the foreground alone can take every connection, and a hydrator or
expiry iteration then waits Hikari's 5 s and fails (logged, counted as
`hoglake_background_loop_failures_total`, retried next interval: a
delay, not a loss). `FOREGROUND_CONNECTION_RESERVE` bounds
COMPACTION's draw, not the foreground's. Such a workload should set
`HOGLAKE_REQUEST_THREADS` below the pool by the number of loops it
runs.

Compaction's own commits now pass `HOGLAKE_COMMIT_LOCK_TIMEOUT_MS` like
every other acquirer of that lock. It is transaction-local, so it bounds
the whole commit tail rather than only the advisory lock: a group whose
commit — or whose row locks inside that tail — times out is counted with
the plan-to-commit races, and its staged output is left undrained for
the cleanup drain, exactly as a lost race leaves it.

`HOGLAKE_COMPACTION_PARALLEL_INPUT_OPENS` (default **8**) opens that
many of ONE group's input files at a time. Opening a parquet input costs
at least one round trip before a row can be read, and a 64-file group
used to pay 64 of them end to end — twice over, since the rewriter
opened each input once for its schema and again for its rows. It now
opens each once, with a bounded window ahead of the consumer. Merge
order is preserved (the rewrite still consumes inputs in order, on one
thread; only the `open` overlaps) and so is the streaming memory bound:
an input that is open but not yet being read holds its parsed footer,
not a readahead buffer. Measured on a synthetic 64-file group against a
MinIO container, where per-open latency is a fraction of what a real
object store charges: **435 ms sequential, 74 ms at 8**.

Its footprint is worth stating because it is the knob that is ON by
default. The worst case is `parallelGroups x inputOpenParallelism`
readers each holding a parsed footer, and a footer read is capped at
`S3InputFile.DEFAULT_MAX_PREFETCH_BYTES` (64 MiB) — so at the highest
`parallelGroups` a default pool allows, 6 x 8 x 64 MiB is the
arithmetic bound, against kilobytes per footer for every real file. The
window is per GROUP and is not divided by `parallelGroups`; an operator
running both knobs high on a small pod should lower one of them.

`HOGLAKE_COMPACTION_CLAIMS_ENABLED` (default **on**) enables group claims.
Each execution plan holds a per-catalog advisory lock while it reads candidates,
packs groups, and claims the selected queue. The queue is limited to the remaining
run budget. Planning and claims use one connection and separate transactions;
packing has no open transaction. The lock is separate from the commit and
retirement locks. Its wait is limited to 15 seconds. File I/O starts after unlock.
Metadata-only plan previews do not lock or claim work.

`HOGLAKE_COMPACTION_CLAIM_TTL_SECONDS` defaults to **3600** seconds. Set it above
the longest full run, including queue time. Each group refreshes its live claim
before execution. An expired or replaced claim skips execution. Each run has
its own claimant ID, including concurrent runs on one server. Cancellation and
heap refusal release claims for groups that did not start.

The next planner sees all queued claims and excludes groups with overlapping
inputs. Claim skips increment `claimed_elsewhere` and do not consume the run
budget. Existing explicit TTL settings override the new default. Update shorter
settings before adding concurrent compactors. All compactors must use the new
planning lock before the full queue protection applies.

**A claim is an optimization and never authorization.** Correctness
against a concurrent rewrite is, and stays, the plan-to-commit
re-verification under the commit lock; turning claims off costs work and
nothing else. The claim key is a hash of the group's spec, partition
values and sorted input file ids — the identity of the WORK, so two
replicas compute it with no coordination — while the planner's skip is
by input-file OVERLAP, because a replica planning a moment later packs
the same files into differently-keyed groups. A claim is released when
its group does not commit and deliberately kept when it does: the
inputs are dead, and the other maintainer's older plan still names them.
A kept claim goes on its own lease
(`HOGLAKE_COMPACTION_COMMITTED_CLAIM_TTL_SECONDS`, default **600**),
sized for the thing it has to cover: the age of a SIBLING'S PLAN, which
is one whole sweep. At the measured 8.5 s a group, 64 groups is about
**544 s** — so the lease has to clear that, and a shorter one (the
first draft used 120 s) leaves most of the sibling's plans arriving at
an expired claim and rewriting anyway. The cost is rows whose
`input_file_ids` the planner reads on every pass: `committed groups per
sweep x lease / sweep duration`, about **70 per table** at those
settings.

`expires_at` is the only liveness protocol — no heartbeat, so a killed
maintainer costs one lease on those files and nothing else — and a bulk
purge at the head of each sweep clears what the releases did not. That
purge is not gated on `HOGLAKE_COMPACTION_CLAIMS_ENABLED`, so turning
the CLAIMS off still clears the rows they left. It does live at the head
of a SWEEP, though, so turning compaction itself off
(`HOGLAKE_COMPACTION_INTERVAL_MS=0`) stops it like everything else in a
sweep — which used to be one of the two things
`/maintenance/verify`'s `compaction_claims` check redded on (the other
being an expired claim outliving its table). #261 removed that check,
so a purge that stops running is unobserved.

`HOGLAKE_COMPACTION_CODEC` (default **zstd**, with
`HOGLAKE_COMPACTION_ZSTD_LEVEL` default **3**) is the compression the
rewrite writes its OUTPUT with; the legal set is zstd, snappy, gzip,
lz4_raw and uncompressed, case-insensitive, and an unknown name is
refused at boot. It is not a per-file detail. Compaction rewrites a
table's rows into target-sized files and then leaves them alone, so
this is the codec a compacted table is stored and scanned under —
permanently. The writer used to take
`ExampleParquetWriter`'s UNCOMPRESSED default, which made every merge a
one-way decompression of clients that all write snappy (pyarrow's and
DuckDB's default, so pyhoglake and the duckdb-client) or zstd
(hedgerow): one dev-catalog run merged 67.6 MiB of inputs into 80.1 MiB
of output.

zstd over snappy because the cost falls on the side paid once.
Measured on event-shaped data (`CodecMeasurement`, four files of 200k
pageview rows, inputs written snappy): high-cardinality strings
compact to **1.41x** the input bytes uncompressed, 1.03x under snappy
and **0.60x** under zstd; low-cardinality enums to 1.84x, 1.27x and
0.84x. Against the uncompressed output, zstd is 0.43-0.45x and snappy
0.69-0.74x. The rewrite's wall time for that group was 0.85 s
uncompressed, 0.86 s snappy, 1.25 s zstd and 3.5 s gzip — so zstd costs
roughly 25-30% more CPU than writing raw, for less than half the bytes,
while gzip pays 4x the CPU for a worse ratio than zstd. The level is
pinned rather than inherited because the sweep is CPU-bound on a shared
maintenance pod and a parquet-java bump must not move that budget
without a diff. parquet-java's zstd workers stay at 0 (in-thread).

`HOGLAKE_COMPACTION_SORTED_HEAP_BYTES` (default **1 GiB**) bounds the
materialized object graph on the SORTED path, which is the only
path that materializes one: the unsorted path streams a record at a time
and its heap is flat in group size. `HOGLAKE_COMPACTION_TARGET_BYTES`
used to stand in for this, on the stated assumption that a flat row's
object graph is near its byte size. Measured, it is not: a flat
11-column event row costs about **1.7 KiB** of heap against **119
bytes** of snappy input — 14x — and since compaction started writing
zstd its own outputs are **1.70x** denser again, so the same byte budget
was admitting 24x the rows a heap could hold. A 512 MiB group of zstd
event data would want roughly 13 GiB.

So the planner works in ROWS and converts. The heap budget divides by
the live schema's node count (~192 B per materialized node, measured) to
give a row ceiling; the table's own registered bytes-per-row — from
`hog_data_file.file_size_bytes` and `record_count`, metadata only, no
footer reads — converts that ceiling back into the byte budget grouping
runs under, capped at the target. Denser inputs therefore
buy fewer bytes per group, automatically and per table, with no guessed
compression ratio anywhere. One consequence is worth stating plainly: a
file that alone holds more rows than the ceiling stops being a compaction
candidate, because merging it could not fit the sort buffer.

That budget is the PROCESS's heap, not one group's, and
`HOGLAKE_COMPACTION_PARALLEL_GROUPS` is what makes the difference
visible: the planner divides it by that value — both the density bound
and the nested one, since they estimate the same heap by different
routes and the tightest wins — so N concurrent sorted groups cannot
exceed what one group was allowed. The division is
enforced in metadata at planning time — the same place, and by the same
arithmetic, as the exact `record_count` ceiling — so a group that could
not fit is never formed and never spends a byte of IO finding out. The
price is proportionally smaller sorted groups, on every sorted table,
whether or not a sweep ever runs two at once; at the default of 1 the
arithmetic is bit-identical to the single-group one. At a HIGH N the
price compounds with the scaling file minimum described above — a
target derated far enough stops admitting the files a group needs, and
the table quietly stops compacting rather than compacting slowly. That
interaction is the reason the default is 1, and the reason to raise it
in steps. The alternative
(admit one full-size sorted group at a time through a semaphore)
preserves group size but serializes exactly the slowest tables and
makes the heap bound a runtime invariant holding a permit across
object-store IO. Either way the real fix is the external merge sort
described above, which removes the ceiling and the division together.

The ladder's scaling is an average, so the exact check happens per
group: a planned group whose registered survivor count is above the
ceiling is refused in METADATA, counted as `heap_budget_exceeded` and
exported as `hoglake_compaction_skipped_total{reason="heap_budget"}`. It
costs no object-store IO, unlike the `java.lang.OutOfMemoryError` ninety
seconds into a rewrite that it replaces (hoglake#118), and it does not
consume the run's group budget — a table that cannot compact must not
starve the ones that can.

### The sorted cap is temporary

Capping a 512 MiB target at tens of megabytes is a real cost, and no
value of the knob fixes it — it only moves it. The cap exists for one
reason: `ParquetRewriter` reads the whole group into an
`ArrayList<Group>` and calls `sortedWith`. An in-memory sort needs the
group in memory.

The replacement is an **external merge sort**, tracked as hoglake#134.
It would remove the heap bound on group size entirely, and with it this
knob, the row ceiling and the `heap_budget` skip; it is the only thing
that lets a SORTED table reach the target in one rewrite. Until it
lands, the levers are the pod ladder below and dropping a table's sort
order (which moves the table to the streaming path, where group size
costs no heap at all).

### Sizing it against the pod

The default is the largest value that is safe on the maintenance pod
**as it exists today** — 4 GiB, so ~2.8 GiB of heap at the image's
`MaxRAMPercentage=70`. Worst-case peak at 1 GiB is ~1130 MiB (the sort
buffer's measured 0.79x of the declared budget, plus parquet-java's
128 MiB row-group block, plus the hydrator's 256 MiB whole-object
ceiling if it fires in the same tick) — 39% of that heap. Raising the
knob without raising the pod turns a counted refusal back into the OOM
it replaced.

This figure used to include the group's input and output byte arrays.
Compaction streams both ends now (`S3InputFile` / `S3OutputFile`), so
its transport costs one 8 MiB readahead buffer and one 16 MiB part
buffer — flat, whatever the group holds. The table below still assumes
the old peak, so every row is conservative by roughly 130 MiB; nobody
has re-derived it or claimed the headroom. Gigahog runs the last row:
the chart sets `sortedHeapBytes: 17179869184` (16 GiB) on the
`gigahog-maintenance` pod and cites this table's 31% figure for it.

Bigger pods buy proportionally bigger groups. Holding peak at ~45% of a
heap that is 70% of the pod, for a ten-column event table:

| pod | heap | `SORTED_HEAP_BYTES` | peak | group (snappy) | group (zstd) |
|---|---|---|---|---|---|
| 4 GiB (today) | 2.8 GiB | **1 GiB** (default) | 1260 MiB, 44% | 57.7 MiB | 33.9 MiB |
| 8 GiB | 5.6 GiB | 2 GiB | 2137 MiB, 37% | 115.4 MiB | 67.9 MiB |
| 16 GiB | 11.2 GiB | 4 GiB | 3890 MiB, 34% | 230.8 MiB | 135.8 MiB |
| 32 GiB | 22.4 GiB | 8 GiB | 7395 MiB, 32% | 461.6 MiB | 271.5 MiB |
| 64 GiB | 44.8 GiB | 16 GiB | 14407 MiB, 31% | 512 MiB (cap stops binding) | 512 MiB |

### The multipart upload needs a lifecycle rule

Compaction's output goes up as a multipart upload. Parts that are
neither completed nor aborted are **billed and invisible**: they are not
objects, so `hog_file_removal` cannot address them and the cleanup drain
cannot see them. Nothing in this repo reclaims them.

The server aborts on every failure path it controls, and counts the
aborts that themselves fail
(`hoglake_multipart_abort_failures_total` — a sustained nonzero value
means storage is growing silently, usually IAM missing
`s3:AbortMultipartUpload`). What it cannot cover is a process that dies
between starting an upload and aborting it: a kill, an OOM-kill, a node
eviction. Under a crash loop the ceiling is one dangling upload per
process lifetime, bounded by the compaction target.

**Every bucket hoglake compacts into wants an
`AbortIncompleteMultipartUpload` lifecycle rule** — a few days is
plenty, since a live upload finishes in minutes. Without one the leak is
permanent and unobservable.

Group bytes are close to column-count independent: the budget divides by
node count and multiplies by bytes-per-row, and both scale with the
number of columns. A 20-column table whose rows really do carry twice
the bytes lands in the same 34-58 MiB band. The exception is a wide
table whose extra columns are nearly free (low-cardinality, dictionary
encoded): those add nodes without adding bytes, and the same 1 GiB
budget yields 17.8 MiB (zstd) / 30.2 MiB (snappy).

`HOGLAKE_COMPACTION_MAX_NODES_PER_ROW` (default **1,000,000**) bounds
one ROW's materialized object graph, which no group-level budget can:
both rewrite paths materialize a row whole, so a single row holding a
hundred-million-element list is an OOM, and an OOM in a background loop
takes the request path down with it. A row past the budget is refused
as `invalid_data` — one counted skip instead of a process kill. The
allowance is spent inside the parquet record materializer as the row is
decoded, and again — from a FRESH allowance — by the copy; counting it
after `read()` returned would only have reported the allocation that
already happened, and sharing one allowance across both phases charged
the same graph twice and silently halved the ceiling.

Calibrate in NODES, not elements: a scalar column costs 1 per row, a
list element costs 2 (its synthetic entry group plus the value), a map
entry 3. The default therefore admits roughly half a million list
elements in a single row — far above any honest row, and far below what
a heap holds at ~50-100 bytes a node.

Per-phase allowances mean peak live heap is up to **2x** the budget: the
decoded row is still reachable while the copy builds its own. That is
the price of the advertised ceiling being the real one, and it is
measured, not assumed — a 999,999-node row rewrites under `-Xmx192m`.

Three non-success outcomes carry a counter, all under
`hoglake_compaction_skipped_total{catalog, reason}`:
`unconvertible_schema` rising means a table has stopped compacting,
`invalid_data` rising means a writer produced something its own
registration or schema forbids — bad values, or a file whose schema
contradicts its `explicit_row_ids` registration — and `failed` is the
outright failure that gets retried next run. The first two never show up as failures — a sweep with either can
look perfectly healthy — which is why they get a line rather than only a
log and a ledger row. The self-healing skips (commit conflicts, DV
supersession) stay uncounted: they re-plan on the next run.

**`reason="failed"` is a FAILURE filed under a metric named
`skipped`.** That is deliberate — one series for "groups that did not
compact, by reason" beats three — but it means
`sum(rate(hoglake_compaction_skipped_total[5m]))` now includes failures,
so an alert written against the two-reason version has silently changed
meaning. Alert on the label: `{reason="failed"}` is the page-worthy one,
`{reason="unconvertible_schema"}` is a backlog that will not clear on
its own, and `{reason="invalid_data"}` is a bug report against whoever
wrote the file. The axis separating the last two is DURABILITY AND
FAULT, not values-versus-schema: an `invalid_data` group is re-planned
and re-refused every sweep, because nothing about it will change.

Each table's candidate list is fixed before rewriting starts, so an
output cannot feed another group in the **same run**. Input bytes only
estimate the output size: encoding, schema changes and DV removal all
move it. Files at or above the target are excluded. Zero-byte inputs are
candidates like any other — they can never close a group on bytes, so
they ride along until the fan-in cap closes one. Unsorted rewrites
stream; sorted rewrites still materialize survivors in memory.

Both debt endpoints read persisted asynchronous summaries of the files
the planner would group (before the execution budget), trailing
remainders included. A group under its scaled minimum contributes zero
debt; raw small-file counts remain visible on the partition page. Neither endpoint scans the manifest. `sampled_at` exposes freshness;
before the first sample, counts are unknown. `MaintenanceSummarySampler`
checkpoints indexed keyset pages and per-bucket accumulators in Postgres, so a
restart resumes progress. It scans files at a captured catalog snapshot;
if expiry overtakes that snapshot it restarts without publishing partial
counts. Hydration and removal counts are observations over the sampling
window. Defaults: 10,000 metadata rows per 1s tick, 60s between completed
refreshes, controlled by `HOGLAKE_MAINTENANCE_SUMMARY_BATCH`,
`HOGLAKE_MAINTENANCE_SUMMARY_INTERVAL_MS` (0 disables), and
`HOGLAKE_MAINTENANCE_SUMMARY_REFRESH_SECONDS`.

Coverage is 100% of live layouts, not just the easy ones:

- **DV-bearing inputs compact.** The rewrite reads each input's live
  deletion vector (Iceberg-v3 puffin `deletion-vector-v1` blobs —
  roaring bitmap, magic and CRC verified; `PuffinDeletionVector` is
  the one server-side reader of DV content) and drops the deleted
  positions: survivors keep their original ids in `_hog_row_id`,
  deleted ids are gone forever, and the inputs AND their DVs
  end-snapshot together. The commit re-verifies the exact DV identity
  it planned against — a vector that grew or appeared since planning
  skips the group (`dv_superseded` in the result), so a post-plan
  delete is never dropped. A DV'd group's output needs no hydrator
  sweep: its stats come from the footer the rewrite wrote, which
  counted the survivors because the survivors are what it wrote, so the
  output registers `stats_state='provided'` like any other.
- **Heterogeneous-schema groups compact.** Inputs written under
  different schema versions rewrite under the LIVE schema, mapped by
  field id: a live column absent from an input null-fills, promoted
  columns up-cast (int32→int64, float→double), dropped field ids drop
  their data. Only a live column *unproducible* from an input's
  physical type skips the group (`unconvertible_schema` — detected
  before any bytes are staged); the unproducible set is narrowings,
  physical mismatches, decimal-scale changes, non-micros time(stamp)
  units, and INT96. Nested containers are NOT in it: list/struct/map are
  copied through recursively, so a nested table compacts like any other,
  and an input whose nested SHAPE disagrees with the live column is
  `unconvertible_schema` rather than a guess.
- **Aborted uploads can't orphan objects.** Before uploading, the
  output path pre-registers as an undrained `hog_file_removal` row
  (reason `compaction_staging`) — a claim ticket. A successful group
  commit settles it (`drained_outcome='registered'`) in the same
  transaction that makes the path live; an aborted group leaves it for
  the normal cleanup drain to reclaim. The commit re-claims the ticket
  first, so a drain that won the race just aborts the group — cleanup
  can reclaim aborted outputs and can never reclaim committed ones.

### Views

Versioned name + SQL text + dialect (`hog_view`), stored verbatim —
the catalog never parses view SQL. Create/drop are DDL commits with
their own change kinds, so views appear in snapshot history and time
travel like everything else.

### Self-knowledge: partition debt, consumers, identity

The catalog reports on itself instead of waiting for ops SQL:

- **`POST /maintenance/verify` — REMOVED (#261)**. It ran the catalog's
  twelve global-invariant checks in one read-only REPEATABLE READ
  transaction, every one of them an UNBOUNDED full-table aggregate. At
  gigahog-prod-us's shape (14M live `hog_data_file` rows, 371M
  `hog_file_column_stats` rows) it hit the 60 s `statement_timeout`
  every run and was disabled there, so the one deployment that needed a
  scrubber had none and the ones that ran it had nothing worth
  scrubbing. `VerifyService`, the loop
  (`HOGLAKE_VERIFY_INTERVAL_MS` — now `Config.REMOVED_INTERVAL_ENV`: a
  positive value is refused at boot, `0` and absence pass in silence),
  `hoglake_verify_violations`,
  `hoglake_verify_errors_total` and the console's panel went with it.
  What is NOT watched any more, in any environment: row-id tiling, DV
  uniqueness and bounds, orphaned rows on dropped tables,
  still-referenced removal-queue entries, snapshot density,
  `next_row_id`, the expiry floor, versioned-row visibility bounds,
  superseded-offset release, staging tickets, upload claims and
  compaction claims — though three of those twelve have another
  enforcer (`removal_queue` at drain time via
  `CleanupService.referencedPaths`, `next_row_id` via the commit-path
  allocator, `orphans`' retirement arm via the eligibility stamp), and
  four are now asserted in the test suite by
  `testing/CatalogInvariants.kt` (`visibility_bounds`, `removal_queue` at
  the compaction boundary, `snapshot_density`, `staging_tickets` arm (c)).
  ONE OF THOSE TWELVE HAD JUST BEEN SHARPENED AND IS NOW UNWATCHED
  TOO. #262 split expiry into a floor advance under the lock and a paged
  file-row purge off it, so a below-floor ended `hog_data_file` /
  `hog_delete_file` row became the DESIGNED intermediate state of a
  two-phase sweep rather than a violation. `expiry_floor`'s file arm was
  gated on the ledger row reporting `purge_truncated: false` (and, after
  that PR's own review, on the drained row's floor covering the current
  one) precisely so it kept asserting the invariant without alerting on
  the backlog. That gate is gone with the check: a drained purge that
  still leaves rows below the floor — its predicate and the floor
  advance disagreeing, which is a bug no counter shows — is **unwatched
  until #261**. What still works is the BACKLOG half, which never needed
  verify: `purge_truncated`, `purge_remaining` and `purge_failures` on
  every ledger row, plus `hoglake_expiry_purge_truncated_total` and
  `hoglake_expiry_purge_remaining` (see §Retention).
  The replacement is a PAGED, RESUMABLE scrubber
  (one bounded page per transaction, a cursor, a run budget, a measured
  per-row cost) — issue #261. Until then, `MaintenanceTask.VERIFY`, the
  spec's `VerifyReport`/`VerifyCheck` schemas and the console's ledger
  renderer stay as HISTORICAL shapes so the ledger's existing `verify`
  rows still decode for their retention window.
- **`POST /maintenance/rehydrate`** — the operator requeue for the
  hydrator's structural failures (above): flips `failed` → `pending`,
  catalog-wide or scoped to one namespace+table.
- **`GET /stats/partitions`** (`service/PartitionStatsService.kt`) —
  leaf partitions ranked by compaction debt: `debt_score` is the
  small-file count under exactly the threshold the compactor plans
  with (so the ranking predicts what a sweep would do), plus
  stale-spec-group counts. The webui's compaction-debt page renders
  this.
- **`GET .../tables/{t}/partitions`** — one row per (spec vintage,
  partition value tuple) with the file, byte, deletion-vector, row and
  compaction-debt measures **as the maintenance sampler last measured
  them**. It reads `hog_maintenance_summary_tier`, so it walks no
  manifest, takes no lock and adds nothing to a commit — which is also
  why it accepts no `snapshot`/`at_timestamp`: there is exactly one
  snapshot it can answer at, and the response names it as
  `sampled_snapshot_id`. A catalog with no published sample answers 200
  with `sampled_at: null`, `total: 0` and no partitions — the table
  exists, the measurement does not. Sorting, `limit`/`offset` and a
  repeatable `filter` of `key_index:text` (matched against the DECODED
  value, so `0:2026-09` is a month) are query parameters; a table with
  more sampled groups than the listing will materialise is 422, because
  it decodes, sorts and pages in memory. The webui's partitions tab
  renders this.
- **`GET .../tables/{t}/partitions/values`** — the partition fields at
  the read snapshot, each with the distinct STORED, transformed values
  it takes across the live files (most-frequent first, capped at 500;
  `truncated` marks a field past the cap). This one is a live read, not
  a sample: it takes `snapshot`/`at_timestamp`, and the values are
  returned verbatim for a client to offer as a filter and echo back as
  `partition=key_index:value` on `GET .../files`.
- **`GET .../maintenance/status`** and **`GET .../maintenance/runs`**,
  with instance-wide twins at **`GET /maintenance/status`** and
  **`GET /maintenance/runs`** — the read side of `hog_maintenance_run`
  (the ledger every background sweep and manual trigger writes) plus the
  persisted summaries. `status` is per-task: loop cadence, the task's own
  backlog keys (hydrator `pending_files`/`failed_files`, expiry's
  retention and floors, cleanup `queued_removals` and
  `oldest_queued_age_seconds`, compaction `small_files`/`target_bytes`,
  retirement nothing) and its most recent recorded run. Counts are absent
  until a sample exists — unknown, not zero. `runs` pages the ledger
  newest-first on an exclusive `before` run_id cursor, optionally
  filtered by `task` (422 on an unknown one), 50 per page and capped at
  500. Neither queries the manifest. The instance-wide pair is the
  ops feed: same conventions, run rows carry their catalog name, and
  `/maintenance/status` pages catalogs on an exclusive `after` name
  cursor.
- **`GET /consumers`** — every consumer in the catalog with per-table
  offsets, table names resolved, dropped tables flagged (offsets
  outlive drops by design — an offset on a dropped table is data, not
  garbage).
- **`GET /v1/info`** — instance identity: the operator-configured
  display name (`HOGLAKE_INSTANCE_NAME`, e.g. "GigaHog"), shown in the
  webui topbar so nobody mistakes prod for dev.
- **`GET /export`** — the DR manifest (snapshot range + live-file
  manifest + consumer offsets, consistent at head) is specified, not yet
  implemented — see that section below.

### Observability

Three surfaces, none of which ever writes to the catalog or rides a
transaction (`observability/`):

- **`/metrics`** (Prometheus): per-catalog health gauges sampled by a
  background loop in one batched query pass — head snapshot and age,
  expiry floor, removal-queue depth (undrained entries only),
  pending-stats AND failed-stats counts
  (`hoglake_stats_failed_files`), live id-less-file count
  (`hoglake_missing_field_id_files`), live table
  count, per-consumer lag (cardinality-capped). #261 removed
  `hoglake_verify_violations{catalog, check}` and
  `hoglake_verify_errors_total{catalog}` with the verify subsystem —
  a dashboard or alert still keyed on either would now have a series
  that never arrives rather than one that reads zero, and nothing behind
  it to alert on. NOTHING IS: checked on 2026-10-01 against live
  `PostHog/grafana-dashboards` at `master` (the default branch is
  `master`, not `main`) commit `1ea38c48`, pushed that day —
  `managed-warehouse/gigahog.json` carries 21 distinct `hoglake_*`
  series and zero `verify` matches, and a repo-wide code search for
  `hoglake_verify` returns 0. `charts/` has no `hoglake_*` reference in
  `alerts/` either, so no alerting or dashboard change is owed.
  THE ONE PUSHED GAUGE LEFT is
  `hoglake_retirement_consecutive_timeouts{catalog, table}` (#263):
  CONSECUTIVE retirement runs that ended one dropped table on its own
  `statement_timeout`. Retirement does NOT resize its batch in response
  to a timeout — the per-row cost is flat in the batch size
  (`RetirementCostIntegrationTest`), so a cancelled batch is a COLD one,
  and the table is left for the next run at the same size — so
  `hoglake_retirement_timeouts_total` is a rate that is MEANT to tick
  occasionally and is deliberately NOT the alert.
  **Alert on this gauge at `>= 3`, and it needs no `for` clause**,
  because the streak IS the duration: 3 means three runs in a row, which
  no cache miss survives. A `MultiGauge` (the shape `ExpiryGauges` uses,
  and the one the removed verify gauge used), so a row exists ONLY
  between a table's first timeout and its next non-timeout outcome — a
  committed batch, a drain, a stuck table — and **ABSENCE IS THE HEALTHY
  STATE**: the series RETIRES on recovery rather than freezing at its
  last value, which a counter could not do (it would leave an
  unclosable alert on a table that is fine). A standing value means
  `HOGLAKE_RETIREMENT_BATCH` is too large for that table's per-row
  cascade; the knob is process-wide, so it is lowered on the INSTANCE
  that retires that catalog (the maintenance workload) and applies to
  everything that pod retires. The streak is per PROCESS and in memory:
  a restart republishes it on the first run that still times out. Plus
  source-side counters: commits by outcome, snapshots expired (and superseded
  consumer offsets released, in the sweep's result and audit event),
  files removed,
  hydrations by result (transient errors counted separately), and the
  `hoglake_commit_lock_wait_seconds` histogram on every commit/DDL/
  maintenance tail (the convoy early-warning). HTTP server metrics
  come with the Ktor Micrometer plugin. The webui renders a `/metrics`
  snapshot visually on its metrics page.
  Plus the two POOL gauge families (#218), read straight off the pools
  rather than sampled by a loop:
  `hoglake_db_pool_active`/`_idle`/`_pending`/`_max` (Hikari's request
  pool — `pending` is the one to alert on: `active == max` is a busy
  instance, `pending > 0` is a caller queued inside `getConnection`
  with 5 s before it becomes a 500) and
  `hoglake_request_pool_active`/`_queued`/`_max` (the blocking request
  dispatcher). Read them together: handlers waiting in the dispatcher
  hold no catalog connection, handlers pending on Hikari do. `active`
  is an approximation and counts THREADS (a handler parsing its body
  runs on `Dispatchers.IO` and is not one); `queued` counts calls
  waiting for their FIRST thread, not the executor's queue depth, which
  also holds the re-dispatch of every coroutine that resumed. Their
  companion is the `hoglake_request_queue_wait_seconds` histogram —
  what the saturation COST, recorded once per request at handler entry.
  Its p99 against `HOGLAKE_COMMIT_LOCK_TIMEOUT_MS` is the reading that
  matters: a wait that reaches the bound is shed as a typed 503
  (`hoglake_requests_shed_total`), so a rising tail is the warning that
  precedes visible refusals. **The histogram is conditioned on
  survival** — it records at handler entry, so a wait that ended in the
  caller giving up contributes nothing, and under a convoy bad enough
  that most callers time out first the surviving waits are the SHORT
  ones and the p99 can FALL while the instance gets worse. Read it with
  `hoglake_request_queue_abandoned_total`, which counts exactly those.
  Finally `hoglake_health_probe_attempts_hung` is the probe's own
  outstanding attempts: normally 0, and **2 (the cap) against a
  database that is demonstrably answering means the pod is stuck and
  must be restarted** — both probe threads are parked on connection
  attempts nothing can interrupt, so `/healthz` will report 503 for the
  rest of that pod's life. The refusal is logged at ERROR.
- **Audit log**: every consequential action (DDL, commits with
  outcome, options changes, expiry/cleanup runs, offset commits) emits
  one structured JSON line on the `hoglake.audit` logger — actor,
  action, object, outcome, request id — strictly *after* its
  transaction resolves. Request ids ride `X-Request-Id` in and out.
- **`GET /v1/database/health`** (`persistence/DatabaseHealthRepo.kt`,
  `service/DatabaseHealthService.kt`): the health of the Postgres
  INSTANCE behind the catalog, read from the statistics views, plus
  findings that interpret those numbers for this schema. It reads
  catalog and `pg_stat_*` relations only — never a `hog_*` table — so
  its cost does not scale with the manifest, and it carries NO query
  text, because `pg_stat_activity.query` holds literal values (object
  paths, identifiers, author strings) and only durations and counts may
  cross the wire. Each finding is a `severity` (`INFO`, `WARN` or
  `CRITICAL`, sorted worst-first) plus a stable `code`, a `detail` of
  what was measured and a `hoglake_impact` saying why it matters HERE —
  the second half is the reason this endpoint exists rather than a link
  to a generic Postgres dashboard. The CRITICAL-capable codes are
  `invalid_indexes`, `inactive_replication_slot`,
  `prepared_transactions`, `commit_lock_held`, `autovacuum_disabled`,
  `xid_wraparound` and `idle_in_transaction` (which escalates from WARN
  to CRITICAL with the age of the oldest idle transaction, because an
  open transaction pins the vacuum horizon for the whole database and
  every expiry delete and compaction supersession stays unreclaimable
  until it ends). The WARN set is `long_transaction`, `lock_wait`,
  `dead_tuples`, `never_analyzed`, `sequential_scans`, `unused_indexes`,
  `cache_hit_ratio`, `connection_saturation` and
  `checkpoints_requested`; `temp_files` is INFO.
- **Health**: `/healthz` proves the catalog is reachable (`SELECT 1`
  on the probe's OWN one-connection pool, bounded four ways by
  `HOGLAKE_HEALTH_PROBE_TIMEOUT_MS` — Hikari `connectionTimeout`,
  pgjdbc `connectTimeout`/`socketTimeout`, session `statement_timeout`
  — with a wall-clock deadline of twice it); `/livez` is process
  liveness only. The probe shares nothing with the request path, so a
  full pool reads 200 and only a database that does not answer reads
  503 (#218). Before that it borrowed a request connection, and a
  commit convoy that filled the pool read as "database dead" and got
  both API pods liveness-killed.

### Background assembly

`App.kt` wires services into Ktor and `startBackground()` runs the
loops — hydrator, expiry, cleanup, compaction (default off:
`HOGLAKE_COMPACTION_INTERVAL_MS=0` — flipping it on is an ops
decision), retirement (also default off), metrics sampler — as
**coroutines under one supervisor scope** (`BackgroundLoops`), each
with its own interval knob
(`Config.kt`, all env-sourced, `<= 0` disables), per-catalog failure
isolation (a failed iteration is logged + counted and the loop keeps
running), and structured, bounded shutdown (cancel + join, 5s cap).
`Main.kt` = migrate (under an advisory lock, so replicas don't race
DDL) → assemble → start loops → serve.

**Every route handler's blocking work runs off the Netty event loop**
(`api/BlockingDispatch.kt`, #218): one interceptor in its own phase,
inserted after `Plugins` so it never sheds ahead of `RequestId`,
installed once by `App.module`, runs the rest of the pipeline
under a bounded dispatcher of `HOGLAKE_REQUEST_THREADS` threads
(default `HOGLAKE_DB_POOL_SIZE`). `/healthz`, `/livez` and `/metrics`
bypass it, matched on a path normalised for trailing slashes, so a full
dispatcher is still observable and still probes green. Measured, not
assumed: `/healthz/` is a **404** today, because this server does not
install Ktor 3's `IgnoreTrailingSlash` — configure the probe without
the slash. Normalising makes that a *fast* 404 rather than a queued
timeout, and removes the trap where installing that one-line plugin
later would route a real probe through the saturated pool.

Excess requests QUEUE, and the queue is bounded by AGE rather than by
depth. The interceptor stamps `System.nanoTime()` before the hand-off,
records the wait as `hoglake_request_queue_wait_seconds` at handler
entry, sheds a request whose wait already passed
`HOGLAKE_COMMIT_LOCK_TIMEOUT_MS` with the typed 503 + Retry-After
*before* it borrows a connection, and charges the rest of the wait
against the advisory-lock bound inside `Locks.acquireCatalogCommitLock`
— so a request cannot spend the admission budget twice (25 s queued
plus a fresh 30 s lock wait was 55 s of server time for a caller that
had gone). The shed is judged at ADMISSION, when a call reaches a
thread: a transient convoy drains and discards its stale head, while a
pod whose pool never frees a thread queues rather than refuses, which
is why readiness-on-sustained-saturation is still a follow-up.

`Main.kt` also floors Netty's CALL group at
`HOGLAKE_NETTY_CALL_GROUP_SIZE` (default `max(4, availableProcessors)`),
which is defence in depth rather than the fix: Ktor sizes that group at
the CPU count exactly, and the production pod has one CPU, so a probe's
dispatch — or a `/metrics` scrape, which shares the bypass and is real
CPU work on that thread — would otherwise queue behind another call.
The WORKER group keeps Ktor's own default (`parallelism / 2 + 1`): it
does socket IO, it is not where blocking work or the probe handler
runs, and applying this floor there would RAISE it on every pod with
more than two CPUs.

There is no **verify loop** any more. It ran
`VerifyService.runOnceAllCatalogs()` — one twelve-check report per
catalog, recorded in the ledger with trigger `loop` — behind
`HOGLAKE_VERIFY_INTERVAL_MS`, default `0` and set to `3600000` only on
the Gigahog maintenance workload. #261 removed it: every check was an
unbounded full-table aggregate, and at production scale the sweep
timed out instead of reporting, so the knob was `"0"` there. The env
var is now in `Config.REMOVED_INTERVAL_ENV`, which splits on the VALUE:
`0` and absence pass in silence, a positive value is refused at boot
naming #261. `0` is what the chart renders — unconditionally, from a
REQUIRED `verifyIntervalMs`, with no override in any environment — so a
blanket `REMOVED_ENV` refusal would reject every pod over a value that
means what is now true. The cost of that would be a stalled rollout
rather than an outage (`maxUnavailable: 0`, so the old ReplicaSet keeps
serving; the signal is a Degraded Application, in dev only, with prod
behind the promotion gate) — but a refusal nobody needs is still a
refusal. A POSITIVE value is refused because it asks for checks that no
longer run. `RemovedEnvConfigTest` pins both tiers. The entry graduates
to `REMOVED_ENV` once the chart stops rendering the key.

### Specified, not yet implemented

**CDC publications** — the WAL tap: a catalog-managed publication tails
a table's changefeed and produces rows to Kafka, its progress tracked
as a first-class consumer offset (so the retention floor protects
unpublished ranges automatically). Fully specified in the OpenAPI
(endpoints return 501) so clients can build against the shape.

**`GET /catalogs/{c}/export`** — the DR manifest (snapshot range +
live-file manifest + consumer offsets, consistent at head), likewise
fully specified in the OpenAPI and answering 501 until built.

## Operational endpoints and probe contracts

Served at the root (not under `/v1`), documented in
`openapi/hoglake.yaml`:

| Endpoint | What | Kubernetes probe |
|---|---|---|
| `GET /livez` | Process liveness only — never touches the database | **liveness** (a database outage must not restart pods) |
| `GET /healthz` | Readiness: the catalog must answer (`SELECT 1` on the probe's own connection, bounded by `HOGLAKE_HEALTH_PROBE_TIMEOUT_MS`; the zombie-server incident is why it touches the database at all, #218 is why it no longer touches the request pool) | **readiness** (an unready pod leaves the Service) |
| `GET /metrics` | Prometheus text exposition | scrape target, never a probe |

One more read-only operational endpoint is served under `/v1`, not the
root:

| Endpoint | What | Kubernetes probe |
|---|---|---|
| `GET /v1/database/health` | Postgres-instance health from the statistics views, with interpreted findings (see Observability) — instance-level, never a `hog_*` read, and carries no query text | never a probe |

The webui container serves its own static `GET /health` (nginx `return
200`, independent of the SPA fallback and the API proxy) as its probe
target, and proxies `/healthz`, `/metrics` and `/openapi.yaml` through
to the server. Its upstream is resolved at request time (nginx
`resolver` + variable `proxy_pass`), so the console boots and probes
green whether or not the server exists yet; proxied paths return
502 until it does, then heal without a restart.

## Dev environment

Toolchain via [flox](https://flox.dev) (JDK 25); Gradle via the
**checked-in wrapper** (`./gradlew`, version pinned in
`gradle/wrapper/gradle-wrapper.properties` — never a system gradle);
recipes via `just` (see `justfile`, composed into `../justfile`):

```sh
just            # list recipes
just test       # full suite (Docker required)
just unit       # no Docker
just one 'com.posthog.hoglake.commit.*'
just schema-check
just compose-up # Postgres 18 + MinIO (ports overridable via HOGLAKE_*_PORT)
just run        # server on :8080
just docs       # OpenAPI spec in Swagger UI on :8090 (HOGLAKE_SWAGGER_PORT)
```

The compose MinIO needs the server's S3 env pointed at it — the
defaults are blank/ambient (AWS default chain, for IRSA in deploys), so
without these the first S3-touching path (hydrator footer reads,
cleanup deletes, compaction rewrites) dies with the SDK's
"Unable to load credentials" chain error before any request is made:

```sh
export HOGLAKE_S3_ENDPOINT=http://localhost:9000   # MinIO from compose-up
export HOGLAKE_S3_ACCESS_KEY=hoglake               # MINIO_ROOT_USER
export HOGLAKE_S3_SECRET_KEY=hoglake123            # MINIO_ROOT_PASSWORD
just run
```

(`HOGLAKE_S3_PATH_STYLE` already defaults to `true`, which MinIO
requires.) When the server runs in a container instead, use
`http://host.docker.internal:9000` — or `http://minio:9000` with the
container attached to the compose network — as the endpoint.


## Layout

- `src/main/resources/db/migration/` — Flyway. `V1__init.sql` is frozen
  as of v1.0.0 and the chain is append-only from there; V22 is the
  latest. `schema.sql` is the canonical twin, equivalence-tested.
- `src/main/kotlin/com/posthog/hoglake/`
  - `model/` — domain types (mirror the OpenAPI schemas).
  - `persistence/` — JDBI repositories; all SQL parameterized.
  - `service/` — DDL, alter, scan, views, options, expiry, cleanup.
  - `commit/` — the commit path (OCC, lock tail, row-id assignment).
  - `compaction/` — small-file merging (the only parquet-java user).
  - `hydrator/` — deferred-stats hydration.
  - `stats/` — Iceberg single-value bounds codec.
  - `observability/` — metrics, audit, request ids.
  - `api/` — Ktor routes implementing the spec.

## Testing

JUnit 5 + AssertJ; property tests via kotest-property (strategy:
[../fuzzing.md](../docs/fuzzing.md)). Integration tests use Testcontainers
(Postgres 18, MinIO), tagged `integration`, and need Docker
(`docker-java.properties` in test resources pins the Docker API version
for recent daemons). The schema-equivalence test and an OCC concurrency
torture suite run with everything else in `just test`.


## Atomic CREATE / CTAS

Catalog responses advertise `atomic-table-creation-v1`. Prepare a definition with
`PUT /v1/catalogs/{catalog}/table-creations/{operation-uuid}`, write Parquet using
the returned field IDs and `write_path`, then POST the ordered file registrations
to that operation's `/commit`. Preparation creates no visible table or snapshot
and does not reserve the target name. Publication installs the definition, files,
and receipt in one database transaction and one snapshot. Empty registrations
create an empty table. INSERT continues to use the existing append API.

Repeat preparation with the same definition, or publication with the identical
ordered file list, to recover lost responses. Different payloads under the same
operation ID conflict. GET the operation to inspect its durable state. A committed
receipt remains valid even after subsequent table deletion or replacement.
POST `/abort` to atomically fence publication; if publication already won, abort
returns `committed` and never deletes the table. HTTP 200 reports operation state,
including `rejected` and `aborted`; clients must inspect the response state.

Only publication takes the catalog-wide commit lock, followed by the operation
row lock. Status, abort, and expiry serialize on the operation row; preparation
uses the unique operation key to resolve concurrent retries. No operation takes
the catalog lock while holding an operation row lock. Both row and catalog waits
honor `HOGLAKE_COMMIT_LOCK_TIMEOUT_MS` (default 30000; 0 disables the bound),
returning 503 with `Retry-After` when the bound is exceeded.

Prepared operations expire after 24 hours, checked under the operation row lock on
status, abort, preparation retry, or publication. Terminal receipts are retained
for the lifetime of the catalog in this version. No automatic object deletion is added: failed or
aborted writes can leave objects. A `prepared` status or an unavailable receipt is
not permission to delete data. Only terminal aborted/rejected operations are
eligible for operator cleanup, after their writers have stopped. Never remove
files from committed operations based on operation age; normal catalog retention
owns their lifecycle. There is no automatic fallback to staging-table rename.

The API supports unpartitioned creation, at most 10000 columns and 10000 files per
operation, and validates registration metadata using the existing append checks.
It does not open Parquet objects at publication time. Field IDs are table-local,
start at one, and are installed with the prepared UUID at publication. Expanding
receipt retention, automated orphan cleanup, or idempotent INSERT is separate work.

Atomic preparation, publication, and abort emit `table_creation_*` audit events
after their transactions finish. `hoglake_table_creation_total` labels attempts
by catalog, action, and outcome; terminal retries are `replayed`. Only first-time
successful publication increments `hoglake_commits_total{result="committed"}`.
Stored definitions carry a format version and stable wire type names; existing
unversioned receipts remain readable and retryable.

A supplied `footer_size` must be nonnegative and fit within `file_size_bytes - 8`
for both initial publication and INSERT. Atomic publication requires this field;
ordinary INSERT retains support for omitted footer metadata.

### Recovering an INSERT response

`idempotent-append-v1` advertises durable commit replay and
`GET /v1/catalogs/{catalog}/commit/receipts/{operation}`. The operation is the
existing optional `idempotency_key` UUID on `/commit`. Both the append and its
full canonical request receipt commit in one PostgreSQL transaction under the
catalog lock. Reusing a key with another payload returns 422. Receipts have no
expiry or snapshot/table foreign key, so later writes, rename, drop and snapshot
expiry cannot cause publication again. Existing receipts remain replayable.

Canonical comparison orders append groups, their files and column statistics;
partition value order remains significant. The read snapshot, expected table
UUID, all registration fields, and remaining request metadata are included.
A missing receipt does not fence an in-flight request. Retry only the identical
payload with the same key; never infer permission to delete uploaded files.

Deploy this server before enabling connector recovery. Older clients (Python,
DuckDB, hedgerow and the console) retain their existing behavior and need no wire
changes. `/commit/prepared` remains supported. Manual SQL reruns, query/task
retries and orphan collection are outside this guarantee.

The standalone server has no application authentication. Protect this lookup
with the same authenticated deployment boundary and catalog authorization as
`/commit`; catalog scoping alone is not authentication. This change does not
introduce an authentication framework or authorize any deployment.

### Atomic DML transaction publication

`atomic-dml-transactions-v1` adds `POST /v1/catalogs/{catalog}/commit/transaction`.
The request uses the existing guarded, idempotent commit envelope. The
unchanged-since-`read_snapshot` requirement binds the tables the transaction
DELETES from — those conflict with inserts, deletes and DDL — because a delete is
published against a read of the target's rows. A table the transaction only
appends to keeps the ordinary DDL-only rule: appends never conflict with appends,
inside a transaction as much as outside one, so an append-only transaction does
not 409 on a concurrent INSERT. Every table publishes in one snapshot and one
durable receipt. This is snapshot isolation with write-write conflicts; read-only
tables and other catalogs are not part of the commit read set.

Because "unchanged" binds only the delete targets, a statement that **read** a
table but produced no deletion vector for it — a MERGE or UPDATE whose predicate
matched nothing — must still send a delete group for that table with an empty
`files` list, or it keeps no read-set protection. That is the same anchor
`/commit/mutations/prepared` already documents, and it is now load-bearing for
`/commit/transaction` too.

**Compaction is never a conflict**, on any guarded path. A `table_compacted`
change rewrites which files back a table, never which rows are visible in it, so
it cannot invalidate a read set — and treating it as one livelocked every
mutation on a continuously compacted table, where a group publishes every couple
of minutes and any read-to-publish window longer than that retried into the next
one forever. The same exclusion applies to the atomic-replacement guard in
`service/TableCreationService.kt`, where the rejection is *durable*
(`rejected`/`target_changed`), so a `CREATE OR REPLACE` whose upload outlived one
compaction interval burned its receipt and the retry raced the next group.

What compaction *can* invalidate is a specific deletion-vector target, and that
is caught per file: a DV against a data file that was live at `read_snapshot` and
has since been retired — by compaction or another commit — is a **409**
(retryable: re-read at the compaction snapshot and re-publish against the
output), while one against a file that was already dead at `read_snapshot` stays
a 422. Expiry is deliberately not on that list: for expiry to have removed a file
that was live at `read_snapshot`, `read_snapshot` must be below the floor, and
the floor guard returns **410** before any per-file check runs — so a connector
never needs a retry path for it.

A transaction may delete rows from files it has staged privately. A DV registration
can specify `data_file_id: 0` and `data_file_path` referencing exactly one append in
the same table and same commit. The server resolves that reference after allocating
and inserting the new files, inside the same transaction. Ambiguous, missing and
cross-table references roll everything back. Numeric IDs retain their existing
snapshot checks. Other commit endpoints reject this new field; old replicas lack
the transaction endpoint, so clients must never fall back. New fields are omitted
from ordinary receipt fingerprints, preserving existing receipts.

**Upload claims do not take the commit lock.** A writer claims one upload per
output file, so `claim`/`renew`/`abandon`/`schedule-expired` taking the per-catalog
commit lock — unbounded, with no admission timeout — meant paying the catalog's
whole write-throughput bottleneck once per file, for nothing: claim paths are
server-generated random UUIDs and the claim INSERT is `ON CONFLICT DO NOTHING`.
Row-level semantics replace it. Every UPDATE re-checks the claim's state in its
WHERE clause, so under READ COMMITTED an update that waited on a publication's
row lock re-evaluates against the committed row and cannot clobber a settled
claim, and `UploadService.register` — which runs inside the commit transaction —
takes the claim rows `FOR UPDATE`. The expired-upload sweep's fence re-checks the
*whole* candidate predicate, not just "not registered", so a renewal that landed
between candidate selection and the fence wins, as `renewUploads` promises; it
fences each candidate in **its own short transaction**, because a publication
registering one of those paths waits on that row lock while holding the catalog
commit lock, and one transaction over a 10,000-row batch would convoy the
catalog behind the sweep. It also refuses to queue a path any data or
deletion-vector file row still claims, and rests an abandoned tombstone for the
drained-ledger retention between offers.

Trino stages DML on its coordinator and uses this endpoint at explicit COMMIT.
DDL remains autocommit-only. Failed/coordinator-lost transactions leave only leased
uploads until publication; an unknown publication outcome must be resolved from
the receipt, not by deleting objects or blindly resubmitting SQL. Python, DuckDB,
hedgerow and the web UI retain their ordinary commit behavior. They read the usual
positive file IDs, row lineage and Puffin vectors after publication; private IDs
and paths add no new committed storage or scan representation. No migration is
needed beyond the claimed-upload ledger required by this capability.
