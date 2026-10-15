# Kafka ingestion via staged keying — design obligations for v1

Status: implemented in `millrace/` (code map: millrace/architecture.md).
The component is **millrace** — the channel (historically, the sluice)
that conducts the current to the water wheel: narrow where the mill pond
is broad, swift where it is still. Accumulate, then release — that is
the whole idea.

Companions: [README.md](../README.md) (architecture),
[../hedgerow/BUFFERED_INGESTION.md](../hedgerow/BUFFERED_INGESTION.md)
(the staging precedent — SQLite WAL),
[../hedgerow/README.md](../hedgerow/README.md) (the daemon idiom and the
viaduck lessons this component inherits),
[iceberg-federation.md](iceberg-federation.md) (field IDs and type
discipline — the writer contract below is pyhoglake's, unchanged),
[kafka-ingestion-plan.md](kafka-ingestion-plan.md) (the build-and-test
sequence for this design).

## The problem

The events topic's tenant distribution is brutally skewed: of order
10⁹ events/day across of order 10⁵ distinct tenants/day, with a
Zipf-like tail — the median tenant contributes sub-row counts per
flush window while the top tenant is ~10⁶× the median. (Exact figures
live in internal metadata exports; the shape is what matters here, not
the numbers.)

Any **arrival-window** flush policy fails this distribution in both
directions at once: a cadence sized for the tail (~1 row per window for
the median tenant) produces a small file per tenant per window and a
commit storm; a cadence sized for the whales buffers gigabytes of the
top tenants' rows in memory per window and concentrates them on the few
pods that own the hot partitions. Today's mitigation — many pods, the
topic re-keyed by `team_id`, a 10-minute flush — works, and is exactly
as fragile as that description sounds. Per-tenant readiness spans six
orders of magnitude; a single global flush clock cannot.

## The idea

Decouple *when data arrives* from *when it is flushed* by staging it in
an embedded, object-storage-native LSM ([SlateDB](https://slatedb.io),
via its official Python bindings), keyed by **partition value** rather
than by arrival order — precisely, by the destination table's
**partition tuple**: the `partition_values` a hoglake file of the row
would carry under the table's live partition spec, computed per record
at consume time:

```
consume loop:   poll → stage rows under (partition tuple, …) → commit Kafka offsets
flush loop:     per-key readiness scan → staged range → Arrow → parquet
                → commit → delete exactly the published row keys
```

Staging is the buffer, so Kafka offsets commit at stage time and neither
loop waits on the other. Accumulation is bottomless (the state lives on
object storage, not in pod memory or on a PVC), so a whale's rows can
accumulate to a full target file size while a median tenant's accumulate
for fifteen minutes — **per-key** readiness (a key is one partition
tuple's staged rows), size-triggered for whales, age-triggered for the
tail, one mechanism. With an `identity(team_id)`-led spec the key is
the tenant (and the month, under `month(timestamp)`); a steady-state
flush of one key writes exactly one file.

The LSM's ordering is load-bearing, not incidental: keying staged rows
by `(partition tuple, offset)` means a flush reads one contiguous
range, a capped read is an exact offset prefix of the tuple's staged
rows, and a flushed window is an exact set of keys to delete. Readiness
is decided from an in-memory index of the staged groups (sizes and ages
are never persisted; the startup drain below is what makes that sound),
so a sweep costs no I/O.

## Why a new component

- **Not millpond.** Conceptually right pipeline, wrong durability model
  (in-memory buffer, offsets withheld until flush — the coupling this
  design exists to break), and its cruft is structural: destination
  duality, DuckDB coupling, VARIANT dual-write. We take two ideas — the
  sink seam and staging gauges as alert inputs — and no code.
- **Not hedgerow's buffered mode.** Its SQLite WAL ties a coordinator
  to a durable volume and its scheduling is known not to scale to this
  tenant cardinality ("production sizing at 140K teams … still
  needed"). SlateDB removes the volume. Hedgerow's persisted
  prepared-commit discipline (exactly-once across restarts) is
  deliberately NOT adopted: the contract here is at-least-once.
- **pyhoglake is reused wholesale.** Field-ID embedding, footer stats,
  the Iceberg bounds codec, transforms, prepared commits — one writer
  implementation, integration-tested against the server. This component
  does not re-derive any of it.

## Staging layout

**One SlateDB instance per replica, its home stage**, at
`s3://…/millrace/{topic}/{slot}`, the slot of the smallest partition the
replica claims. A replica claims partitions the way millpond does (its
StatefulSet ordinal's share of the topic, derived once at start; the
assignment *is* the claim) and stages every claimed partition's records
in its home, so a group is one key range per replica, not one per
partition. At startup it also drains every other existing slot `j` with
`j % R == ordinal` (the home of a replica a scale-down removed) and closes each once empty; the slot sets
are disjoint for a fixed `R`, and replica-count changes go through
zero replicas. SlateDB is single-writer per path; a contested open
fences the older writer lazily, at its next durable write, and a fenced
process exits 4. The consumer writes durably on every poll before it
commits offsets (one staging batch for all partitions, or a one-put
`meta/fence` probe when the poll's records all dropped), so a fenced
owner never commits offsets against the new one. SSTs are zstd. No
volumes anywhere.

```
rows/<group><partition_u32><offset_ob>   → event payload bytes
meta/fence                               → the consumer's fence probe (not a row)
```

`<group>` is the partition tuple, prefix-free: `arity u8`, then per
component a tag byte (null / present) and, when present, `u16 length +
UTF-8` wire string; an unpartitioned table's tuple is empty. `_ob` is
offset binary, so byte order is numeric order. The Kafka record
timestamp is never read.

There is no persisted index. Each stage keeps its groups' first staged
time, staged bytes and a write generation in memory. That is sound
because **staged state never outlives a process**: at startup each
replica opens its stages and flushes everything staged before it
consumes anything new (the startup drain: one sequential read-ahead
pass per stage, stages concurrently, flushed in batches of windows;
a group that fails is held in the index and its rows stay staged). After that, every write updates the in-memory index
before it reaches disk, and a group leaves the index only when a
complete scan of it was flushed and no write touched it since; so a
group with staged rows is always in the index, and every group becomes
due within the slow-lane deadline.

There is no quarantine: a record that can never become a row (not a
JSON object, a malformed partition source, a value no column can hold)
is dropped, logged and metered (`millrace_records_dropped_total{reason}`),
at consume time (nothing is staged for it) or at flush time (it is left
out of the payload and deleted with its window). Event times are never
judged: ancient and far-future events are written, and an instant
outside years 1–9999 is clamped to the nearest representable one
(counted in `millrace_values_clamped_total{column}`).

Which group a staged record lands under is computed from its PAYLOAD,
never from the Kafka key: the consumer parses the value as a JSON object
once and, per field of the table's live partition spec, reads the source
scalar by path (struct leaves included), coerces it with the same
column coercer the flush decoder uses, applies the transform and
renders the wire string pyhoglake's commit carries
(`grouping.py:GroupKeyer`). The consumer re-reads the table shape on the
flusher's TTL; a spec change just starts new groups. The tuple is a
layout optimization, never the truth: the flush fans every window out
by the LIVE spec, so a group keyed under an older spec flushes
correctly, as several files.

Any partition's instance can hold any group, so a group spread over K
partitions is up to K files per flush window, and a medium group split
thin can fall under the age lane's minimum per instance and move to the
slow lane.

Staging order is Kafka offset order; the catalog's sort spec is event
time, which late and skewed events invert. The flush sorts each batch
by the live sort spec before the partition fanout, so every file is
key-nondecreasing under the spec compaction enforces.

## The flush planner

One predicate per group, first match wins:

| Trigger | Condition | Catches |
|---|---|---|
| size | `staged_bytes ≥ MILLRACE_FLUSH_BYTES` (256 MiB) | whales: early, often |
| slow | `age ≥ slow_lane_deadline` (6 h; ≥ `flush_deadline`) | pathological low-volume groups, churned groups, groups keyed under a retired spec |
| age | `age ≥ flush_deadline` (15 min) **and** `staged_bytes ≥ 1 MiB` | medium tenants: the freshness SLA |

`age` is measured from the group's first staged row (lifted to the
start of the last complete flush when rows remain, so a group under
steady inflow is due once per deadline). The age lane's minimum keeps
tiny groups from producing a file per deadline; the slow lane drops the
tail from ~96 files/day to ~4/day per group.

A flush's unit of work is bounded: one scan reads an offset prefix of
at most `MILLRACE_FLUSH_BYTES` of staged payload, and always at least
one row, so a record larger than the budget never wedges its group.
Files come out around a fifth of the budget at typical compression;
**compaction owns final file sizing**. The pod runs at most 4
decisions at once, so the flush's memory envelope is about 4 × (budget
+ decoded batch + output).

Due groups below the size lane are flushed in **batches**: every due
group of a stage, packed up to `MILLRACE_FLUSH_BYTES` of staged bytes
and 512 groups (`max_files_per_commit`), built on one shape read and
published in one commit (more if the groups fan out past 512 files);
the batch's rows are deleted in one write batch as the commit lands. A
group whose build fails leaves the batch; a refused batch (4xx) splits,
each group retrying alone, which isolates a poisoned group; any other
failure retries the batch. A size-lane group flushes alone.

A flush of one group: scan → decode → Arrow → sort by the live sort
spec → one parquet file per partition tuple under the table's LIVE
spec → chunk into commits of ≤ 512 files → per commit, upload,
`commit/prepared`, then one durable write batch that deletes exactly
the scanned row keys that commit published (so a split decision that
fails partway never republishes the parts that landed). A row that entered the scanned
range after the scan is not among them and flushes later. No scan and
no payload lowering ever runs under a stage's write lock.

## The wire contract — from day one, no flags

- Every commit carries the `read_snapshot` of the shape it was built
  from and `expected_table_uuid`, the incarnation the startup resolve
  READ and pinned. A later read or commit naming another incarnation
  halts the process (exit 3): a human decides whether the staged rows
  belong in the new table.
- Every build mints a fresh uuid4 `idempotency_key` per commit. An
  ambiguous answer (5xx, 408, 429, a transport error) or a plain 409
  `commit_conflict` resends the IDENTICAL bytes under the same key, and
  the server's receipt makes that exactly-once within the process. A
  409 `ddl_since_read_snapshot` or a 410 `read_snapshot_expired`
  rebuilds against a fresh shape read with a fresh key.
- Any other refusal (a 400/422 payload refusal, 401, 403, 404, 413,
  `table_dropped`) fails the decision: nothing is deleted, the group
  retries with backoff, and after three consecutive failures it is
  reported stuck (ERROR log, counter, gauge) while every other group
  keeps flushing. Rows are never dropped for a refusal.
- Object names are `…/{build}/{index}.parquet`, `build` a fresh token
  per build, so a rebuild never touches objects a landed commit may
  name. Every object exists before a commit names it.
- Table identity reads use `totals=false`; the shape cache's TTL is a
  quarter of the catalog's snapshot retention, so the commit's
  conflict window always covers a spec change.

Stats policy is keyed on the realized file size: a file of at least
32 MiB ships full footer stats; smaller files ship
`stats_mode=deferred` and the hydrator backfills bounds off the commit
path.

## Delivery semantics

**At-least-once**: a Kafka offset commits only after every record at or
below it is durable in SlateDB or was dropped as unrepresentable and
counted; a staged row is deleted only after every commit naming it
landed. Duplicates are accepted:

- a crash, or a failed delete batch, after a commit landed: the window
  re-flushes (on restart, by the drain) under a new key;
- a Kafka rewind below already-flushed offsets (a changed group id,
  offsets aged out of `__consumer_offsets`): the re-consumed records
  stage and publish again.

`MILLRACE_KAFKA_AUTO_OFFSET_RESET` defaults to **`latest`**, the fleet
policy (a fresh group under `earliest` replays the whole retention,
the 7-day replay in the millpond chart's comments). The mirror risk:
with `latest`, a partition whose committed offsets were LOST starts at
the head and its backlog is never consumed. The operator levers are
group-id stability and `offsets.retention.minutes` outliving any dark
period; with both, the knob only governs genuinely new partitions.

The startup drain makes restart simple: there is no recovery step and
nothing persisted to reconcile. It costs startup time proportional to
the staged backlog, during which Kafka lag grows (durably) and the pod
reports not-ready; the startup probe must allow for it. A pod that
crashes mid-drain drains again on restart.

## What the server owns (and what this design asks of it)

- **Compaction** owns file sizing *and* sorted-structure maintenance:
  converting overlapping small runs into large, disjoint, sorted units
  is what keeps catalog-level stats pruning discriminating, and it is
  the binding consumer of the declared sort spec — the ingest sort and
  the catalog sort spec must match or sorted rewrites never converge.
  This design's file-arrival rate is its sizing input; see below.
- **The hydrator** owns deferred-stats backfill.
- **Expiry/cleanup** are unchanged; ingest produces only ordinary
  commits.

**Growth arithmetic** (per AGENT.md's doctrine; baseline = prod-us
2026-09-30: ~3,600 files added/min, 10M live file rows on the events
table, 96% under 60 KiB): at a 15-minute deadline and ~10⁵ active
tenants/day, worst-case arrival is of order 10⁷ small files/day —
single-digit multiples of the current baseline, not a new regime.
Per-commit rows grow only by the bounded flush window (≤
`MILLRACE_FLUSH_BYTES` of staged input, ≤ 512 files per commit);
receipts expire server-side (`HOGLAKE_RECEIPT_RETENTION_SECONDS`, V24);
stats rows of files under 32 MiB arrive deferred via the hydrator
rather than in the commit body.
The open question is compaction throughput at that arrival rate, which
is validation item 1, not an assumption.

## Deployment and operations

One process = one topic → one table (no central scheduler, no fleet
coupling). The deployment is a StatefulSet of `MILLRACE_REPLICA_COUNT`
replicas with `POD_NAME` from the downward API; assignment is static
and derived (`p % replica_count == ordinal`, the partition count read
once at start); there is no consumer group membership and no
rebalance. An ordinal with no partitions or an unknown topic refuses to
start. A replica's home stage is keyed by its smallest claimed
partition, so it stays with the ordinal across deploys.

Startup: create the destination from configuration when it does not
exist (`millrace/destination.py`: catalog, namespace, and the table in
ONE atomic request with its partition spec and sort order; an existing
table is compared and never altered), resolve and pin the incarnation,
open the home stage and the slots to drain, **drain everything staged**,
then assign and
consume. `/readyz` is 503 until then; the chart's startup probe must
cover the worst expected drain. Recommended dev shape:
`MILLRACE_PARTITION_BY=identity(team_id),day(timestamp)`,
`MILLRACE_SORT_BY=timestamp`.

Metrics are passive (never control flow), read from memory on each
pod's ops server: consume and flush counters,
`millrace_records_dropped_total{reason}`, commit refusals by status,
stuck groups (the alert), staged bytes/rows/groups since the last drain,
the oldest due group's age, `millrace_draining`, and a whitelist of
SlateDB internals bridged from `DefaultMetricsRecorder` and summed over
the pod's instances (per-instance series would be a cardinality trap):
`millrace_slatedb_l0_sst_files`, `…_l0_ssts_per_segment_max`, the WAL
flush series, `…_write_stalls_total`, `…_l0_stalls_total{type}`,
`…_memtable_bytes`, `…_block_cache_accesses_total{entry_kind,result}`.
Per-DB compaction debt comes from the maintenance service
(`millrace_maintenance_compaction_l0_ssts{topic,partition}` plus the
fleet max). `/healthz` 503s once a halt or fence has latched.

## Maintenance services: external compaction and GC

Compaction and GC for the staging SlateDB instances run as
**separate services** deployed beside a millrace deployment, looping
over every replica's instances across topics. The point is traffic
placement: compaction reads and rewrites the whole staged dataset over
time, and the replicas — already hosting many instances each — should
spend their network and CPU on ingestion, not on LSM housekeeping.

Verified against the installed binding (slatedb 0.17.0) and the
upstream tutorials (slatedb.io/docs/tutorials/standalone-compactor,
…/standalone-garbage-collector), not recalled:

- **Writers can shed both loops.** `Settings` clears
  `compactor_options` / `garbage_collector_options` (probe-verified via
  the binding's JSON settings). Upstream documents exactly this for the
  standalone topology. Ordering rule: a writer's embedded loops are
  disabled only when the external service is live and proven — with no
  compactor anywhere, L0 grows to `l0_max_ssts` and writes stall — an
  error condition surfaced by SlateDB's own stall series and the
  maintenance service's per-DB L0 debt gauges (there is no consumer
  pause to surface it; §Deployment). The stalled-compaction shape is
  pinned by a parity test.
- **External GC is fully supported from Python today.**
  `Admin.run_gc_once(options)` runs against a live writer
  (probe-verified), needs no fencing — multiple collectors may run in
  parallel — and deletes only files past `min_age` unreferenced by the
  latest state *and active checkpoints*. Upstream cautions we inherit
  verbatim: WAL-fence deletion stays dry-run unless `min_age` outlives
  any stale writer, and `boundary_files_enabled=false` requires
  `min_age` longer than any stale process's lifetime.
- **External compaction is NOT available from the Python binding** —
  verified empirically on 0.17.0 (parity tests + FFI symbol inspection):
  `submit_compaction` only *enqueues* — the job is executed by the
  **writer's own embedded compactor** on its poll tick, so submission
  buys zero offload (and risks double scheduling). Submission against a
  writer whose compactor is disabled *fails outright* ("invalid DB
  state": the compactions store is created by a running compactor).
  The execution engine (`CompactorBuilder` / `run_compactor`) exists in
  Rust core and the `slatedb` CLI but not in the binding, and the wheel
  ships no CLI. Worse for the naive form of this idea: a writer with
  `compactor_options=None` **stalls** — memtable→L0 flushes block
  indefinitely at `l0_max_ssts` with nothing to drain them (verified).
  So writers MUST keep their embedded compactors regardless.
- **External GC is fully supported and safe**, verified by permanent
  parity tests: `Admin.run_gc_once` executes synchronously in the
  caller, runs concurrently with an active writer without a single
  error across a 265-cycle churn test (incl. checkpoint pin/release and
  compactor-epoch bumps — no false fencing; `writer_epoch` untouched),
  respects checkpoints precisely (a checkpoint pins its manifest's WAL
  range and SSTs; `delete_checkpoint` + a later GC reclaims them), and
  never raises on a missing/foreign path (so the service probes
  `read_manifest` first). Writers always shed the embedded GC
  (`stage.build_slatedb_settings`; there is no knob) because this
  service owns collection: with dozens of pipelines planned, one GC
  service keeps LIST/DELETE work and its scheduling off the ingest
  pods. GC is idempotent and overlap-safe.
- **Decision (2026-10-09): the maintenance service is GC +
  debt-monitoring only.** Compaction offload waits on the binding:
  route 1 is upstreaming a `CompactorBuilder` exposure (small, well-
  scoped; the binding already tracks core closely — `Admin` is there);
  route 2 is shipping the `slatedb` CLI in the maintenance image and
  supervising per-instance `run-compactor` processes under a budget.
  The debt half is implemented: every swept DB's compactor state view
  is read and exported (`millrace_maintenance_compaction_l0_ssts
  {topic,partition}` + the fleet max), so a compactor losing to its
  writer is visible per DB before it stalls.
- The compactor side coordinates (upstream's "compactor coordinator"
  phrasing) — the interaction between an external compactor's manifest
  updates and the writer's lazy fencing (Phase 2's finding) is a
  required parity test before any writer disables its embedded loop.

The service itself: discovers instances by prefix listing under the
staging base (a full listing at most once per
`MILLRACE_MAINTENANCE_DISCOVERY_INTERVAL_S`, default 1 h, cached in
between — a listing of the whole root per sweep is the cost the cache
exists to avoid), sweeps every DB at an equal cadence (rotating budget
pages over the sorted path set — debt-ORDERED scheduling is a later
refinement, not implemented), reads each swept DB's compactor state
view for the debt series, and does bounded work per sweep with a
ledgered truncation — the repo's scale doctrine applies to this loop
exactly as it does to the server's own maintenance loops. One
maintenance deployment per millrace deployment.

## Prior art: Apache Paimon

This design is Paimon's write path with the LSM moved out of the table
format: per-bucket write buffers → per-key SlateDB readiness; checkpoint
interval → the age deadline; L0 sorted runs + leveled compaction → small
files + server-side compaction; dedicated compaction job →
server-owned maintenance. Two deliberate divergences: no buckets
(identity partitioning absorbs skew at *staging* time and keeps
per-tenant zone maps exact for per-tenant readers), and no LSM in the
read path (hoglake readers see plain parquet snapshots; merge-on-read
complexity is never a query engine's problem). SlateDB is the durable
write buffer Paimon gets from Flink's state backend — without the Flink
cluster.

## Validation plan, in order

1. **Compaction throughput at the design's arrival rate** — the number
   the whole design stands on. Bench the planner and rewrite loop
   against a production-shaped fixture (10⁷ file rows, skewed
   partition sizes) before any ingest code ships.
2. **SlateDB rates**: put / scan / range-delete at the per-pod slice of
   the event rate, with the real record size; confirm binding-level
   parity for the knobs this design uses (write batches, transactions,
   scans, range deletes) — plus the two the staging topology adds:
   **writer fencing on a contested open**, and **per-instance
   housekeeping cost** at the K-instances-per-pod count a real
   assignment implies.
3. **Commit rate + deferred-stats mix** against a live catalog: sweeps
   of ~10⁵ tiny registrations batched up to 512 files per commit;
   hydrator keeping up.
4. **Startup drain time** against the worst staged backlog a pod may
   restart with; it sizes the startup probe.

## Open questions

- Decoder surface: JSON values at launch; schema-registry (Avro/Proto)
  decoding is a pluggable front, not a v1 requirement.
- Whether a dead-letter store for unrepresentable records is worth its
  cost (today they are dropped and counted).
- Multi-topic fan-in per process vs. strict one-pipeline-per-pod at
  higher topic counts.
- SlateDB checkpoint/clone as the disaster-recovery story for staged
  state, vs. treating Kafka replay below the committed offset as the
  recovery path.
