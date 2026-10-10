# Kafka ingestion via staged keying — design obligations for v1

Status: design. Implementation in progress on `jakob/sluice-ingest`.
The component is **millrace** — the channel (historically, the sluice)
that conducts the current to the water wheel: narrow where the mill pond
is broad, swift where it is still. Accumulate, then release — that is
the whole idea.

Companions: [README.md](../README.md) (architecture),
[../hedgerow/BUFFERED_INGESTION.md](../hedgerow/BUFFERED_INGESTION.md)
(the staging precedent — SQLite WAL, persisted prepared commits),
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
than by arrival order:

```
consume loop:   poll → stage rows under (team_id, …) → commit Kafka offsets
flush loop:     per-key readiness scan → staged range → Arrow → parquet
                → prepared commit → delete staged range
```

Staging is the buffer, so Kafka offsets commit at stage time and neither
loop waits on the other. Accumulation is bottomless (the state lives on
object storage, not in pod memory or on a PVC), so a whale's rows can
accumulate to a full target file size while a median tenant's accumulate
for fifteen minutes — **per-key** readiness, size-triggered for whales,
age-triggered for the tail, one mechanism.

The LSM's ordering is load-bearing, not incidental: keying staged rows
by `(team_id, timestamp)` means a flush reads one contiguous, nearly
sorted range. The readiness scheduler's AGE/SLOW lanes are the ordered
index scan the staging layout promises (`sched_age/` prefix scan to the
deadline cutoff, then a point-read per candidate for the exact decision
input); the SIZE lane has no size-ordered index (`sched_size` is the
deferred bench question below) and evaluates one `stats/` scan per
partition per sweep — and that scan is shared with the published
gauges, so a sweep costs one stats scan per partition, not an
O(tenants) pass per reader per poll.

## Why a new component

- **Not millpond.** Conceptually right pipeline, wrong durability model
  (in-memory buffer, offsets withheld until flush — the coupling this
  design exists to break), and its cruft is structural: destination
  duality, DuckDB coupling, VARIANT dual-write. We take three ideas —
  the sink seam, offset-derived flush identity, staging gauges as
  alert inputs — and no code.
- **Not hedgerow's buffered mode.** The staging discipline (persist the
  complete prepared commit request before publication; separate
  discovery and publication checkpoints) is adopted verbatim, but its
  SQLite WAL ties a coordinator to a durable volume and its scheduling
  is known not to scale to this tenant cardinality
  ("production sizing at 140K teams … still needed"). SlateDB removes
  the volume and makes the scheduler a range scan.
- **pyhoglake is reused wholesale.** Field-ID embedding, footer stats,
  the Iceberg bounds codec, transforms, prepared commits — one writer
  implementation, integration-tested against the server. This component
  does not re-derive any of it.

## Staging layout

**One SlateDB instance per claimed Kafka partition**, at an
object-storage path keyed by the partition, not the pod
(`s3://…/millrace/{topic}/{partition}`). A replica claims partitions the
way it always has — the consumer assignment *is* the claim — and opening
a partition's DB is part of claiming it. The set of partitions assigned
to a replica may change between deploys: staged state follows the
partition to its new owner, never the pod. SlateDB is single-writer per
path, which is exactly the ownership fence assignment already provides;
the fencing behavior on a contested open is pinned by a binding-parity
test, not assumed. When the fence fires (a newer writer opened the
path), the flush loop treats the partition's `Error.Closed` as
terminal: the partition leaves flush scheduling with one log line
(never a stack trace per sweep) until the assignment revokes it, and
the runner's stats surface the fenced set; the consume side's handling
is the consumer's own. A pod holding K partitions runs K instances; per-DB
housekeeping overhead (memtable, WAL, manifest) is a bench item, because
it bounds how small a partition slice may be before S3 chatter
dominates. No volumes anywhere.

```
rows/<team_id>/<timestamp_be><offset_be>  → event payload bytes
stats/<team_id>                           → {staged_bytes, first_staged_ts, …}
sched_age/<first_staged_ts><team_id>      → ∅
poison/<offset_be>                        → quarantine envelope (stamped; purged at
                                            MILLRACE_POISON_RETENTION_S, default 7 d)
```

The keyspace carries no partition component: the instance *is* the
partition. `poison/` is the quarantine — forensic state with a bounded
lifetime: entries carry the staging-clock stamp of their write and a
per-pod sweeper deletes expired ones in bounded batches (nothing
depends on poison presence; a pod down at expiry purges later);
unstamped (v1) entries are kept forever — unknown age is never
deleted.

Which team a staged record lands under is the configured extraction
codec (`MILLRACE_TEAM_KEY_CODEC`): `utf8-decimal`/`be64` decode the
message key (the topic is keyed by team — the repartitioned-topology
case), while `value-json:<field>` reads a top-level field of the JSON
payload and never consults the key — which is what lets millrace
consume a topic that is NOT keyed by team (`clickhouse_events_json`)
and drop the repartition hop entirely. The consequence is stated, not
dodged: with an unkeyed topic a team's records land on whatever
partition the producer's partitioner chose, so **every instance can
hold every team**, and a team spread over K partitions is up to K files
per flush window — one per instance. The per-partition topology above
is unchanged; what changes is which teams a partition's instance holds
— and the readiness lanes now evaluate a team's PER-PARTITION slice:
the whale pays K full-size files per window (the size lane fires per
instance, as before), while a medium tenant split thin across K
partitions can fall under `min_flush_bytes` per instance and drift from
the age lane to the slow lane — exactly the freshness-for-files trade
the repartition hop used to buy back by concentrating the team. Sizing
`min_flush_bytes` against K is the operator lever.
Idempotency is unaffected: the flush identity scopes a window by
`(topic, partition, team, offset range)`, so the same team's windows on
different partitions are distinct flushes whose idempotency keys never
collide, and a partition's replay only ever re-puts its own staged
keys.

- `rows` keys are ordered so one tenant's staged slice is a contiguous
  range scan, approximately time-ordered on arrival — approximately,
  because the row key's timestamp is the Kafka RECORD time while the
  catalog's sort spec is the payload's event time, which late and skewed
  events invert constantly. The flush therefore does not trust staging
  order: `build_prepared_plan` sorts the Arrow batch by the live
  `sort_spec` (fields, directions, null placement) before the partition
  fanout, so every registered file is key-nondecreasing under the spec
  compaction's sortedness checks enforce — the bounded scan stays
  contiguous, and the sort is what reconciles record time with event
  time.
- `stats` and `sched_age` are maintained in the same write batch /
  transaction as the row puts, so readiness accounting cannot drift
  from the data. `sched_age` is the age-ordered readiness index: the
  sweep for expired keys is a prefix range scan.
- A size-ordered view (`sched_size`) is either a second index or
  derived by scanning `stats` — decided by the benchmark, since it is
  the only prefix whose maintenance cost is per-put.
- Kafka metadata (`topic, partition, first_offset, last_offset`) is
  recorded per staged range in a side prefix; it is the flush's
  identity material (below), not the row key.
- `event_time` outliers are clamped or quarantined **at flush time**
  (Arrow compute over the staged range), never by mutating staged rows.

## The flush planner

Three lanes, evaluated per key; arrivals never reset a key's
`first_staged_ts`:

| Trigger | Condition | Catches |
|---|---|---|
| size | `staged_bytes × compression_ratio ≥ target_output_bytes` (default 500 MB) | whales — early, often, full-size files |
| age | `age ≥ flush_deadline` (15 min) **and** `staged_bytes ≥ min_flush_bytes` | medium tenants — the freshness SLA |
| slow | `age ≥ slow_lane_deadline` (6–24 h) | pathological low-volume keys |

The **size lane sizes the output, not the input**: the trigger estimates
the parquet file size, taking compression roughly into account. The
ratio is *observed*, not guessed — an exponentially-weighted average of
(parquet bytes ÷ staged bytes) from this table's completed flushes,
fed back into the planner as an input (the planner stays pure; a
configured fallback ratio applies until the first flush lands).
Implemented as the flusher's in-memory EWMA
(`MILLRACE_COMPRESSION_RATIO_HALFLIFE`, default 20 flushes), reset when
the table's shape or incarnation changes — a schema change makes the
old observations a different quantity. It is deliberately NOT
persisted: a restart falls back to `DEFAULT_COMPRESSION_RATIO` (0.2),
which errs toward over-staging (a bigger file is a sizing miss; the
reverse would be a new small-file regime).

A flush's unit of work is BOUNDED, so a post-outage catch-up slice is
not one unbounded scan → Arrow build → settlement transaction: the
staged-range scan is capped in rows and bytes
(`ceil(target_output_bytes ÷ ratio × 1.25)` staged bytes, floored at
`MILLRACE_FLUSH_SCAN_MIN_BYTES`, default 64 MiB — the floor keeps toy
targets from fragmenting flushes; rows capped at
`MILLRACE_FLUSH_SCAN_MAX_ROWS`). The flush window is derived from the
SCANNED rows: their own offset span when the scan covered the team's
whole staged slice (the steady state — caps do not bind, and staged
offsets only ever grow past a scan snapshot, so the span's settlement
deletes exactly the published rows). Only a TRUNCATED scan narrows to
`[first_scanned_offset, K]`, the covered offset prefix: the `offsets/`
record holds one `[first, last]` span per staged batch (arrival
structure — a sound over-approximation of the team's staged offsets,
since a span's interior can name another team's offsets), so a
record-timestamp inversion straddling the cap still shrinks the window
rather than settling a row the flush never saw — conservatively, never
past the first span offset the scan missed. Leftover rows simply
re-decide on the next sweep; the window/prepared machinery below
already handles arbitrary sub-windows. A single record larger than the
budget still flushes alone (one staged record is bounded by the
broker's message limit; the alternative is a wedged key).

The **age lane carries a minimum size** so that genuinely tiny keys skip
it: a tenant with a handful of staged rows at the 15-minute mark does
not earn a file. The **slow lane** is where those keys live: one flush
per 6–24 hours, which drops the pathological tail from ~96 files/day to
~4/day per key — the cardinality that used to gum up the works now costs
almost nothing, and the small-file floor of identity partitioning all
but disappears as a compaction-arrival rate.

(The lane boundary is deliberately low — `min_flush_bytes` is sized so
only pathological keys defer; "medium" tenants keep their 15-minute
SLA. The policy itself is a seam: the planner takes a policy object, and
this three-lane policy is the default — pluggable policies are a later,
small step precisely because the planner is pure.)

A flush of one key: scan the staged range → Arrow → filter/quarantine →
one parquet file per `(team_id, month(timestamp))` partition tuple →
upload → persist the complete prepared request under `prepared/` →
`commit/prepared` → on receipt, one staging transaction that deletes
the staged range and sched entries and drops the persisted request.
That settlement is atomic and runs only with the receipt in hand, so
there is no `flushed/` marker to repair: recovery's ambiguity set is
exactly the surviving `prepared/` entries — a crash between commit and
delete leaves the entry, and recovery receipt-checks it (a 200 settles
the range locally with zero republication; a 404 replays the persisted
bytes byte-identically, and the server's receipt dedupes a landing
replay). `prepared/` envelopes are versioned: v3 carries the
idempotency key's derivation inputs (`topic`, `partition`) beside the
key and body, so a recovered entry is self-describing and recovery
cross-checks it against the stage's own identity; they also carry
`persisted_at` (since v2) so the replay is bounded by the receipt
horizon (§Delivery semantics): a 404 past the horizon is ambiguous with
a purged receipt and halts the pipeline for operator reconciliation
rather than risking a duplicate. Recovery also
collects any `flushed/` markers left by pre-lifecycle builds — settled
garbage by construction — in bounded pages, one delete batch per page,
so a partition's recovery never stalls on marker volume. Sweeps across
keys (and across the pod's partition instances) each become their own
commit per `(team, window)` — bounded in file count by
`max_files_per_commit` (a spec that fans one decision past the bound is
a deployment bug the flusher halts on); a sweep is never one unbounded
commit body.

Team churn: a tenant that stops sending leaves residue below the size
and age triggers — which is exactly what the slow lane absorbs: any key
past `slow_lane_deadline` flushes regardless of size, so churned tenants
leave no residue and the stats keyspace reflects live tenants.

## The wire contract — from day one, no flags

This writer is born correct; it does not repeat millpond's blind-append
shape (AGENT.md invariant 12 exists because of that shape):

- Every commit carries a `read_snapshot` taken before the staged range
  was read, and `expected_table_uuid` pinned at pipeline startup.
  A 409 `commit_conflict` refreshes and retries; `ddl_since_read_snapshot`,
  `table_recreated` and 410 are *re-prepare* signals — re-read the table,
  rebuild the request, never replay the refused payload. 503
  `commit_queue_timeout` backs off per `Retry-After`.
- The complete prepared commit request is persisted (SlateDB side
  prefix) **before** publication and is never regenerated after an
  ambiguous response; the idempotency key derives from
  `(table_uuid, topic, partition, team_id, first_offset, last_offset)`
  of the staged range — topic and partition are derivation inputs
  because offsets are per partition: two partitions flushing one team
  over the same offset range must never share a key or the object URIs
  derived from it. The same rows re-flushed produce the same request,
  and the server's receipt dedupes the replay.
- An answered refusal is judged, not retried — and judged precisely:
  only a 422 whose error code names a per-record validation quarantines
  the window (whole payloads, never truncated — the settlement deletes
  the staged rows, so the poison entry is the only surviving copy).
  Every other answered 4xx — a proxy's 401/403, an ingress's 413, a
  request-shape `validation` 422 — halts the pipeline with the rows
  staged: nothing about it says the rows can never publish, and the
  staged rows plus the persisted entry are the operator's
  reconciliation material.
- Partition values are computed under the spec the `read_snapshot`
  describes; on spec change, the refusal path above is the only
  recovery, per invariant 12.
- Table identity reads use `totals=false`; the manifest is never
  aggregated on a request path.

Stats policy is asymmetric, keyed on the REALIZED file size rather than
the deciding trigger: a file at or above half of `target_output_bytes`
ships **full footer stats** (large files are where pruning pays — and
the size lane's steady state), smaller files ship
**`stats_mode=deferred`** (a sub-row-count file prunes trivially; the
hydrator fills bounds in asynchronously, off the commit path). Keying
on the trigger instead would invert exactly on the bad case: the
biggest files this pipeline writes are post-outage catch-up flushes,
which arrive on the age/slow lanes.

## Delivery semantics

At-least-once end to end, hedgerow's contract restated for a staging
area: Kafka offsets commit strictly after durable staging; staged ranges
are deleted strictly after a commit receipt. Crash between commit and
delete → the range re-flushes → the persisted prepared request replays
byte-identically → the receipt dedupes it. Duplicates require a flush
boundary shift across restart, which the persisted request prevents.
410 below the retention floor is a stop sign, never a skip.

That absorption has a precise boundary: replay is idempotent only for
rows still **staged**. A re-consumed record re-puts the same `rows/`
key, so replays of not-yet-flushed ranges rewrite staged state in place.
Below the settled floor — a range already flushed, receipt-confirmed
and deleted from staging — a replay re-stages those rows as new keys
and re-flushes them under **new** idempotency keys: the destination
sees duplicate rows that nothing dedupes. That replay happens exactly
when a partition restarts from `earliest` across a settlement boundary:
a changed `MILLRACE_KAFKA_GROUP_ID`, or offsets aged out of
`__consumer_offsets`.

Where consumption starts when no committed offset exists is therefore a
deliberate knob, `MILLRACE_KAFKA_AUTO_OFFSET_RESET`, and the default is
**`latest`** — the fleet policy: the same setting pinned to `earliest`
caused the 7-day whole-retention replay recorded in the millpond
chart's comments, and a replay storm is the worse operational failure
of the two. The price is the mirror-image risk millpond's AGENT.md
names: with `latest`, a partition whose offsets were LOST (a changed
group.id, aged-out `__consumer_offsets`) starts at the head and any
unconsumed backlog is silently never consumed — removal-by-omission.
(A genuinely new partition has no backlog and loses nothing under
either setting.) `earliest` remains available for a deployment that
would rather replay than skip. Either setting faces the same
settled-floor boundary above — millrace's per-partition staged state
makes a replay idempotent above it, and the receipt horizon below
bounds the ambiguity — so the choice is operational, not semantic. The
operators' levers are therefore **group-id stability** (a pipeline's
group never changes) and **offset retention**
(`offsets.retention.minutes` must outlive any window in which a dark
partition could be rediscovered) — with both in place, consumption
never restarts across a settled range at all, and the knob only ever
governs genuinely new partitions.

One boundary is load-bearing and honest: the dedupe depends on the
server's commit receipts, and receipts are PURGED
(`HOGLAKE_RECEIPT_RETENTION_SECONDS`, default 7 days, V24 — floored per
catalog at twice its snapshot retention, capped at 30 days, and refused
below one hour server-side). A partition dark past that retention can
hold a `prepared/` entry whose receipt is gone whether or not its
commit landed; replaying blindly could duplicate an executed commit. So
a receipt 404 is answered with a replay only while the persisted entry
is younger than the **receipt horizon** — `MILLRACE_RECEIPT_HORIZON_S`,
default 6 days, deliberately below the server's default. The dependency
is NOT on the wire — the server exposes snapshot retention but never
receipt retention (verified against the OpenAPI: `CatalogOptions`
carries `snapshot_retention_seconds` only; `GET /v1/info` is instance
identity) — so the knob CARRIES the assumption that it stays below the
server's effective purge floor (`max(HOGLAKE_RECEIPT_RETENTION_SECONDS,
2 × snapshot retention)`, capped 30 d), and an operator who changes the
server's retention posture must re-check this knob against it. If
pyhoglake grows a `Catalog._receipt_retention_seconds` accessor the
flusher honors it over the knob (the guarded seam,
`flush.py:_receipt_horizon_s`; "receipts kept forever" means no
horizon). Past the horizon — or on a pre-`persisted_at` (v1) envelope,
whose age is unknowable — the pipeline HALTS loudly for operator
reconciliation instead of replaying.

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
Per-commit rows grow only by the bounded flush window (≤ the size-lane
cap); receipts are digests; stats
rows arrive deferred via the hydrator rather than in the commit body.
The open question is compaction throughput at that arrival rate, which
is validation item 1, not an assumption.

## Deployment and operations

One process = one topic → one table (hedgerow idiom: no central
scheduler, no fleet coupling). Replicas claim partitions the way the
consumer assignment hands them out — and claim each partition's SlateDB
instance with it, so rebalancing between deploys moves staged state
with the partition rather than stranding it on an ordinal.
Parallelism by replica count, and bounded-parallel flush within one:
the sweep runs up to `MILLRACE_FLUSH_CONCURRENCY` (default 4) decisions
in flight per partition (4× that process-wide — each in-flight decision
holds its bounded scan in memory). Evidence for the tail-SLA claim
(M5): the sweep bench (`tests/test_flush_bench.py`, non-gating:
`uv run pytest --bench tests/test_flush_bench.py -s`) measures 1024
ready keys × 4 partitions end-to-end at **72 keys/s serial** and **286
keys/s at the default concurrency** on in-memory stages with a 5 ms WAL
flush tick — the serial loop sits within 2× of the ~110 ready keys/s
the design arithmetic wants, so the bound-parallel loop shipped; the
tick dominates the rig (at a 1 ms interval the same rig reads 189/253
keys/s), and production WAL latencies (object-store round trips, not an
in-process tick) are exactly what the parallelism overlaps. The
consume loop stops only on error. The staged gauges (bytes/rows per
partition, oldest ELIGIBLE staged age — a key is aged only once any
flush lane will take it, so pathological slow-lane tenants don't page)
are alert inputs; the loop never reads them. SlateDB's own write stall
(L0 at `l0_max_ssts` with no compactor draining) stalls the staging
write itself and surfaces through the `slatedb.db` stall series.
On the consume side, one poll's per-partition stage
batches are in flight CONCURRENTLY (each partition's WAL write and
durability wait overlap; the single offset commit still follows every
ack), and the librdkafka fetch knobs are config
(`MILLRACE_KAFKA_MAX_PARTITION_FETCH_BYTES` / `…_FETCH_MAX_BYTES` /
`…_QUEUED_MAX_MESSAGES_KBYTES`, defaults sized for the whale shape —
the 1 MiB `max.partition.fetch.bytes` library default capped millpond's
hot partition at ~50 MB/s), with the per-poll batch bounded by a record
cap and an observed-average byte cap. Metrics remain passive
observability, never control flow. Fail-fast config at
startup: resolve catalog, table, incarnation; refuse on mismatch. Junk
`event_time` policy (clamp vs quarantine) is config, validated at boot.

Metrics are passive — they never feed control flow — and scraped from
each pod's ops server: millrace's own counters and gauges (staged
bytes/rows, oldest-eligible staged age, flush counters including
orphans and quarantines) plus a whitelist of SlateDB internals, bridged via
SlateDB's `DefaultMetricsRecorder` polled at scrape time
(`snapshot()` is in-process atomics — no loop hop) and **aggregated
across the pod's partition instances** — per-partition series at K
instances per pod is a cardinality trap. One DELIBERATE exception: a
stuck partition must be findable, so `millrace_partition_staged_bytes`,
`millrace_partition_oldest_staged_age_seconds` and
`millrace_flush_partition_fenced` carry `{topic,partition}` labels —
cardinality bounded by the pod's ASSIGNED partitions (tens), never by
teams. The whitelisted series
(verified against slatedb 0.17.0's actual emission):
`millrace_slatedb_l0_sst_files`, `millrace_slatedb_l0_ssts_per_segment_max`
(L0 count/depth), `millrace_slatedb_wal_buffer_flushes_total`,
`millrace_slatedb_wal_buffer_flush_requests_total`,
`millrace_slatedb_wal_flush_bytes_total`, `millrace_slatedb_wal_buffer_bytes`
(the WAL arm), `millrace_slatedb_write_stalls_total` and
`millrace_slatedb_l0_stalls_total{type=...}` (write-stall state),
`millrace_slatedb_memtable_bytes`, and
`millrace_slatedb_block_cache_accesses_total{entry_kind,result}` (the
hit rate is hit/(hit+miss) in PromQL). 0.17.0 emits NO WAL
flush-latency histogram (the design's "ack-latency driver" is therefore
read through the WAL flush rate/bytes and the consumer-visible staging
latency instead; the one latency histogram SlateDB does emit,
`object_store.request_duration_seconds`, cannot separate WAL puts from
SST uploads by label and is deliberately not bridged). Unknown or new
SlateDB metric names are ignored by the bridge — a SlateDB upgrade can
never break a scrape. Per-instance compaction debt is not a pod metric
at all: the maintenance service reads each instance's compactor state
view on its sweep and exports it as
`millrace_maintenance_compaction_l0_ssts{topic,partition}` (the fleet
authority on debt, so the per-DB labels are affordable there) plus the
fleet-wide max — a writer's embedded compactor falling behind shows
there first (L0 approaching `l0_max_ssts` is the write-stall
precursor). The probes follow the same honesty rule: `/healthz` 503s
with the reason on a latched terminal state (a halted flush runner, a
fenced partition) rather than always answering 200, and `/readyz` fails
whenever liveness does.

## Maintenance services: external compaction and GC

Compaction and GC for the per-partition SlateDB instances run as
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
  `read_manifest` first). Writers shed the embedded GC's LIST/DELETE
  traffic by DEFAULT (`MILLRACE_SLATEDB_GC_ENABLED=false`, wired
  through `stage.build_slatedb_settings`) because this service owns
  collection — GC is idempotent and overlap-safe, so a misconfigured
  overlap is wasteful, never corrupt; boot warns loudly that the
  setting assumes this service exists.
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
   scans, range deletes) — plus the two the per-partition topology adds:
   **writer fencing on a contested open**, and **per-instance
   housekeeping cost** at the K-instances-per-pod count a real
   assignment implies.
3. **Commit rate + deferred-stats mix** against a live catalog: sweeps
   of ~10⁵ tiny registrations at one commit per (team, window), bounded
   per commit by the fanout guard; hydrator keeping up.
4. **Planner policy** as pure functions over `stats` scans, unit-tested
   against a replayed skewed-distribution sample — before Kafka or
   hoglake wiring exists.

## Open questions

- Decoder surface: JSON values at launch; schema-registry (Avro/Proto)
  decoding is a pluggable front, not a v1 requirement.
- Whether `sched_size` earns its per-put maintenance cost or falls out
  of the `stats` scan.
- Multi-topic fan-in per process vs. strict one-pipeline-per-pod at
  higher topic counts.
- SlateDB checkpoint/clone as the disaster-recovery story for staged
  state, vs. treating Kafka replay below the committed offset as the
  recovery path.
