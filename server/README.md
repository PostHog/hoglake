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
(`persistence/Locks.kt`: `pg_advisory_xact_lock(4740871, catalog_id)`).
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
no conflict window — but a *stated* `read_snapshot` below the expiry
floor is **410 Gone** (the change rows that would prove the window
clean were expired with their snapshots; the message names the floor
and when it was reached), never a silently truncated conflict check.
Validation failures (unknown table, bad field id, malformed stats) are
422 and roll back the entire commit — a multi-table commit is atomic.

Two more admission gates run under the lock. A commit may carry an
`expected_table_uuid` per table — the incarnation guard: if the live
table's uuid differs (drop+recreate raced the writer), the whole
commit 409s with zero writes. And every registered path (data file or
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
`rename_table`, `set_partition_spec`, `set_sort_order` — applied **in
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
*retained* snapshot is 410. Table-level aggregates (`record_count`,
`file_count`, `file_size_bytes`) are always computed from the files
visible at the requested snapshot — never from head-scoped counters.

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
**exactly once**, in V14's one-time backfill, where it is exact rather
than probable: `dropped_snapshot` has exactly two writers (the
replacement branch and `dropTable`, both via `TableRepo.markDropped`),
`hog_table` rows are inserted from exactly one place
(`TableRepo.insertTable`, called only by `createTable`), every DDL
transaction mints its own snapshot under the commit lock, and views,
compaction and data commits never write `hog_table` at all — so a
snapshot that both retires a table and creates one is a replacement and
can be nothing else, in any environment. What the backfill repairs is
the **rolling-deploy window**: a 1.2.0 replica still publishes
replacements without writing `replaced_table_id`, and each one strands
a retired incarnation whose consumer offset pins expiry until the edge
exists. Re-running the `UPDATE` is the repair; no automated job is wired
up because production carries no replacements yet, and an operator can
run it by hand if that stops being true. The derivation is **not** used
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
scheduler. Each sweep is one bounded transaction under the commit lock:
compute the new floor as the minimum of (age cutoff, head−1, current
floor + batch, and — when `consumer_floor` — the **minimum consumer
offset**, so expiry can never outrun a lagging consumer; the pinning
consumer is named in the result and the audit line). Then: unreachable
file rows are deleted (FK cascades take their stats and partition
values) with their paths queued, and snapshots are removed with **range
deletes**, never id lists. `earliest_snapshot_id` advances — capturing
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
grow the metadata without bound.

Physical deletion is decoupled and paranoid
(`service/CleanupService.kt`): the queue is a *suggestion*. At drain
time every path is re-checked against live references — a
still-referenced path is skipped and counted as an **invariant
violation** (alertable), never deleted. Missing objects count as done.
S3 deletes run in sub-batches (25 paths) whose ledger updates commit
independently, so a mid-drain failure never rolls back completed work
— and each sub-batch's check-then-delete pair runs **under the
per-catalog commit lock**, paired with commit's refusal to register a
path that has an undrained removal row: the two sides together make
delete-under-path-reuse structurally impossible (the liveness answer
can't go stale between check and delete, and a new commit can't slip a
live file under a queued path). The lock hold is bounded — sub-batch
size × one S3 round-trip — which is why the sub-batch is kept small.

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
travel keeps them; expiry reclaims them later), aggregates stats from
typed decoded bounds — treating an undecodable bound as absent, never
wedging on it — and the changefeed excludes compacted outputs so
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

Measured at the default of 5, two ways, because they disagree:
*draining* a static backlog costs about **2x** the input bytes against
the ladder's **4x** — ingest-sized files reach the target band in one
rewrite, and the outputs consolidate pairwise from there. But *steady
state*, a trickle arriving on an already-compacted partition, is about
**2.4x** against the ladder's **2.5x** — barely a win, and a 5.6x LOSS
without the dominance split described above. So the claim is one rewrite
to reach the target band, not one rewrite per file for all time.

This replaced a geometric ladder of size tiers, which packed each file
against its own tier's floor and promoted it a rung at a time. The
consequence was that the same bytes were rewritten once per rung — four
passes to reach 512 MiB from ~53 MiB ingest — and every rung above the
first saved nothing, because the compression had already happened on
the first pass. In production that read as `2 -> 1 files, 121 MiB ->
121 MiB`, fifteen seconds of decompress-and-recompress, repeating on a
catalog with no ingest at all.

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
of demand: the hydrator, expiry, cleanup, verify and the metrics
sampler draw on the same pool, and a busy instance's foreground wants
more than four of its own. Raise the pool with the knob — at the
default pool of 10 the ceiling is 6.

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

`HOGLAKE_COMPACTION_CLAIMS_ENABLED` (default **on**) and
`HOGLAKE_COMPACTION_CLAIM_TTL_SECONDS` (default **900**) stop two
maintenance replicas rewriting the same group. Both plan from the same
metadata and so form the same groups; without claims both rewrite and
upload every one of them and the loser is discarded at commit — a 547 s
production sweep committed 34 groups and lost 30 that way, each loss a
full rewrite and upload thrown away and its staged object left for the
cleanup drain. A maintainer now claims a group before the IO, and the
other's planner skips it (counted `claimed_elsewhere` in the run
result, and — like the heap refusals — NOT charged to
`HOGLAKE_COMPACTION_MAX_GROUPS_PER_RUN`, since it spends no IO: the
sweep refunds the slot and pulls the next candidate).

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
sweep — and that is one of the two things `/maintenance/verify`'s
`compaction_claims` check reds on, the other being an expired claim
outliving its table.

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

The replacement is an **external merge sort**, and compaction is
unusually well placed for one:

- **A compaction OUTPUT needs no sort at all.** This rewriter sorts
  what it writes — the sort spec is *binding for compaction rewrites*
  (`schema.sql`) — so such an input is an already-sorted RUN, and
  merging k runs needs one row per run in a priority queue:
  **O(files)** live rows, not O(group).
- **A CLIENT-WRITTEN file cannot assume it.** A client's sort order is
  **advisory** — `schema.sql` says so, and the server never verifies
  file sortedness. But such a file is bounded by the ingest flush size,
  so sorting one is bounded by that one file: sort each alone, spill it
  as a temp run, and stream-merge the runs like any other.

  Note which case carries the bytes now. Under the ladder most input
  was a previous output being carried up a rung, so most groups were
  free merges; one-pass compaction consumes each file once, so nearly
  every input is client-written and the spill path is the ordinary one.
  The external sort is more work to build than it was, and worth more:
  it is the only thing that lets a SORTED table reach the target in one
  rewrite. Note also that the spill
  needs a scratch directory of its own — compaction streams both ends
  now (`S3InputFile` / `S3OutputFile`) and touches no local disk, so the
  per-group temp dir this plan was written to borrow no longer exists.

That removes the heap bound on group size entirely, and with it this
knob, the row ceiling and the `heap_budget` skip. Until it lands, the
levers are the pod ladder above and dropping a table's sort order (which
moves it to the streaming path, where group size costs no heap at all).

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
has re-derived it or claimed the headroom.

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
  delete is never dropped. Because the registered stats of a DV'd
  input describe pre-delete data, outputs with DVs applied register as
  `stats_state='pending'` and the hydrator re-derives honest stats.
- **Heterogeneous-schema groups compact.** Inputs written under
  different schema versions rewrite under the LIVE schema, mapped by
  field id: a live column absent from an input null-fills, promoted
  columns up-cast (int32→int64, float→double), dropped field ids drop
  their data. Only a live column *unproducible* from an input's
  physical type skips the group (`unconvertible_schema` — detected
  before any bytes are staged); the unproducible set is narrowings,
  physical mismatches, decimal-scale changes, non-micros time(stamp)
  units, nested inputs, and INT96.
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

### Self-knowledge: verify, partition debt, consumers, identity

The catalog reports on itself instead of waiting for ops SQL:

- **`POST /maintenance/verify`** (`service/VerifyService.kt`) — the
  catalog's global-invariant SQL as a read-only, metadata-only
  endpoint (one REPEATABLE READ MVCC snapshot, no catalog lock), and
  the same code the **verify loop** runs on a cadence
  (`HOGLAKE_VERIFY_INTERVAL_MS`, below). Twelve checks:
  - `row_id_tiling` — positional overlap; `explicit_row_ids`
    compaction outputs exempt by design (invariant 2).
  - `delete_vectors` — one live DV per file, monotone supersession
    chains, `delete_count <= record_count` (invariant 3).
  - `orphans` — live `hog_data_file` / `hog_column` /
    `hog_table_version` rows on a dropped table.
  - `removal_queue` — undrained queue entries whose path a file row
    still claims: cleanup's `still_referenced` alert at rest
    (invariant 4).
  - `snapshot_density` — `count(*)` equals the dense
    `[earliest, head]` range (invariant 1).
  - `next_row_id` — the allocator is never behind a range it handed
    out (invariant 2).
  - `expiry_floor` — the floor is at or below head and (under
    `consumer_floor`) at or below every live consumer offset, using
    ExpiryService's own floor query with the superseded-offset release
    applied; and no versioned row with
    `end_snapshot <= earliest_snapshot_id` survives the sweep that
    advanced the floor (invariant 5).
  - `visibility_bounds` — every versioned row's `begin`/`end` pair is
    inside the catalog's snapshot range and correctly ordered, and
    `hog_table.created_snapshot <= dropped_snapshot` when dropped
    (invariant 6).
  - `offset_release` — no consumer offset survives on an incarnation
    whose lineage successor that same consumer has already reconciled
    past; such a row can never be advanced and pins the floor forever.
  - `staging_tickets` — compaction's `compaction_staging` claim-ticket
    lifecycle: settled `registered` with nothing registered, an
    undrained ticket older than the staleness bound with no file row,
    or a ticket drained `absent` whose path IS a file row.
  - `upload_claims` — the upload-claim state machine: a `registered`
    claim queued for `trino_upload` reclamation, or an
    `active`/`abandoned` claim whose path the catalog has registered.
  - `compaction_claims` — the group-claim lease's lifecycle. Both arms
    require the claim to be well past its expiry, because a claim
    expires between sweeps and the NEXT sweep's purge removes it, so
    the window in between is a correct system: one still present past
    that grace (nothing purged it — the sweep is no longer reaching its
    head, or compaction was turned off after having run), and one on a
    table that is dropped or gone. A LIVE claim is never flagged, whatever its files or its
    table are doing — a committed group keeps its claim on a short
    lease, so a live claim over end-snapshotted files, or on a table
    dropped inside that lease, is the ordinary state, and flagging it
    turned `/verify` red on healthy catalogs. Because a claim is an
    optimization and never authorization, a violation here means
    redundant work or a leaked row and never a wrong commit.

  The JSON report carries per-check status, true violation counts
  (`count(*)`, never the sample length), samples capped at 20, and
  each check's own one-paragraph `description` of the invariant it
  enforces.
- **`POST /maintenance/rehydrate`** — the operator requeue for the
  hydrator's structural failures (above): flips `failed` → `pending`,
  catalog-wide or scoped to one namespace+table.
- **`GET /stats/partitions`** (`service/PartitionStatsService.kt`) —
  leaf partitions ranked by compaction debt: `debt_score` is the
  small-file count under exactly the threshold the compactor plans
  with (so the ranking predicts what a sweep would do), plus
  stale-spec-group counts. The webui's compaction-debt page renders
  this.
- **`GET /consumers`** — every consumer in the catalog with per-table
  offsets, table names resolved, dropped tables flagged (offsets
  outlive drops by design — an offset on a dropped table is data, not
  garbage).
- **`GET /v1/info`** — instance identity: the operator-configured
  display name (`HOGLAKE_INSTANCE_NAME`, e.g. "GigaHog"), shown in the
  webui topbar so nobody mistakes prod for dev.
- **`GET /export`** — the DR manifest (snapshot range + live-file
  manifest + consumer offsets, consistent at head) is fully specified
  in the OpenAPI and answers **501** until built (gaps.md B5).

### Observability

Three surfaces, none of which ever writes to the catalog or rides a
transaction (`observability/`):

- **`/metrics`** (Prometheus): per-catalog health gauges sampled by a
  background loop in one batched query pass — head snapshot and age,
  expiry floor, removal-queue depth (undrained entries only),
  pending-stats AND failed-stats counts
  (`hoglake_stats_failed_files`), live id-less-file count
  (`hoglake_missing_field_id_files`), live table
  count, per-consumer lag (cardinality-capped) — plus
  `hoglake_verify_violations{catalog, check}`, which is NOT sampled:
  the verify SWEEP pushes each check's true violation count at the end
  of every pass (0 meaning the check passed) and it stands until the
  next sweep. A `MultiGauge`, so one sweep replaces the whole row set
  and a deleted catalog's series retire; and the LOOP alone publishes —
  a manual trigger on a replica whose loop is off would mint an
  alerting series nothing ever refreshes. Its companion is
  `hoglake_verify_errors_total{catalog}`: a catalog whose scan THREW is
  absent from the gauge (the sweep has no answer for it) and invisible
  to `hoglake_background_loop_failures_total` (the per-catalog catch
  means the iteration succeeded), so this counter is the only thing
  that says a catalog is not being checked at all — alert on
  `increase(...) > 0` alongside `hoglake_verify_violations > 0`. Plus
  source-side counters: commits by outcome, snapshots expired (and superseded
  consumer offsets released, in the sweep's result and audit event),
  files removed,
  hydrations by result (transient errors counted separately), and the
  `hoglake_commit_lock_wait_seconds` histogram on every commit/DDL/
  maintenance tail (the convoy early-warning). HTTP server metrics
  come with the Ktor Micrometer plugin. The webui renders a `/metrics`
  snapshot visually on its metrics page.
- **Audit log**: every consequential action (DDL, commits with
  outcome, options changes, expiry/cleanup runs, offset commits) emits
  one structured JSON line on the `hoglake.audit` logger — actor,
  action, object, outcome, request id — strictly *after* its
  transaction resolves. Request ids ride `X-Request-Id` in and out.
- **Health**: `/healthz` proves the catalog is reachable (`SELECT 1`,
  503 within the pool's fail-fast timeout when it isn't); `/livez` is
  process liveness only.

### Background assembly

`App.kt` wires services into Ktor and `startBackground()` runs the
loops — hydrator, expiry, cleanup, compaction (default off:
`HOGLAKE_COMPACTION_INTERVAL_MS=0` — flipping it on is an ops
decision), **verify** (`HOGLAKE_VERIFY_INTERVAL_MS`, also default
off: `0`; `<= 0` disables), metrics sampler — as
**coroutines under one supervisor scope** (`BackgroundLoops`), each
with its own interval knob
(`Config.kt`, all env-sourced, `<= 0` disables), per-catalog failure
isolation (a failed iteration is logged + counted and the loop keeps
running), and structured, bounded shutdown (cancel + join, 5s cap).
`Main.kt` = migrate (under an advisory lock, so replicas don't race
DDL) → assemble → start loops → serve.

The **verify loop** runs `VerifyService.runOnceAllCatalogs()`: one
report per catalog, recorded in the run ledger with trigger `loop`,
with per-catalog isolation (a catalog that throws is logged and the
rest proceed — the loop never throws out of an iteration). A catalog
whose report FAILS is logged at WARN **once per distinct failing-check
set**, not once an interval: a violation is a standing state, and the
unchanged-condition log flood is the lesson compaction's heap-refusal
warning already learned. Every LOOP sweep publishes
`hoglake_verify_violations{catalog, check}`, set to the true violation
count and to 0 on a pass, so an alert keys on `> 0` and a healthy
catalog is a published zero rather than an absent series. A MANUAL run
deliberately publishes nothing: the trigger works on every replica,
including the ones with the loop off, and a one-off run there would
mint an alerting series that nothing ever refreshes. Series for
catalogs that vanish retire with the next sweep (`MultiGauge`, whole
row set replaced), like every other per-catalog gauge.

Like compaction, the interval defaults to **0 — off** and turning it
on is a per-workload ops decision: in Gigahog the server workload is
expected to leave it at `0` while the maintenance workload sets
`3600000`, so the aggregate pass never runs on the pods serving the
commit tail. That split lives in PostHog/charts and has not landed
yet; until it does, every workload inherits the default and the sweep
runs nowhere — the manual trigger still works everywhere. A default of
an hour here would have run it on every replica instead.

### Specified, not yet implemented

**CDC publications** — the WAL tap: a catalog-managed publication tails
a table's changefeed and produces rows to Kafka, its progress tracked
as a first-class consumer offset (so the retention floor protects
unpublished ranges automatically). Fully specified in the OpenAPI
(endpoints return 501) so clients can build against the shape.

## Operational endpoints and probe contracts

Served at the root (not under `/v1`), documented in
`openapi/hoglake.yaml`:

| Endpoint | What | Kubernetes probe |
|---|---|---|
| `GET /livez` | Process liveness only — never touches the database | **liveness** (a database outage must not restart pods) |
| `GET /healthz` | Readiness: the catalog must answer (`SELECT 1` through the pool, fail-fast on a dead pool; the zombie-server incident is why) | **readiness** (an unready pod leaves the Service) |
| `GET /metrics` | Prometheus text exposition | scrape target, never a probe |

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

- `src/main/resources/db/migration/` — Flyway (single squashed V1
  pre-release; `schema.sql` is the canonical twin, equivalence-tested).
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
