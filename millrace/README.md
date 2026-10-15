# millrace

Kafka → hoglake ingestion with durable SlateDB staging. One process
consumes one topic into one table: each replica stages every record
it consumes in ONE SlateDB instance on object storage (its home stage)
under its destination partition tuple, and a flush loop turns due
tuples' staged rows into parquet and hoglake commits, batching many
small groups into one commit.

**Contract: at-least-once.** A Kafka offset commits only after its
record is durable in staging (or was dropped as unrepresentable, and
counted). A staged row is deleted only after every commit naming it
succeeded. Duplicates are accepted (a crash between a commit and the
staged-row delete, a Kafka rewind); loss is not.

Code map and invariants: [architecture.md](architecture.md). Design:
[docs/kafka-ingestion.md](../docs/kafka-ingestion.md).

- `just millrace unit` — unit + component tests (`memory:///`)
- `just millrace test` — the whole non-live suite, including `file:///`
  integration (durability across hard kill, contested-open fencing)
- `just millrace lint` — ruff check/format + mypy
- `just millrace mutants` — the mutation gate over keyspace.py and
  planner.py (tests/MUTATION_TESTING.md)
- `just millrace live` — the live docker suite (`.artifacts/live-millrace/`)

The live soak (`tests/live/test_live_soak.py`) runs only when
`MILLRACE_SOAK_S` names a duration:
`MILLRACE_SOAK_S=1800 MILLRACE_LIVE_PYTEST_ARGS="-k soak -s" just millrace live`.

## Image

`millrace/Dockerfile` builds `ghcr.io/posthog/hoglake-millrace`: one
image, two entrypoints. The default (`millrace`) is the pipeline; the
SlateDB GC service runs as `python -m millrace.maintenance`. Both refuse
an empty environment with exit 2 (the image smoke test). Non-root
(uid 10001), nothing written outside `/work` and `/tmp`.
`.github/workflows/millrace-image.yml` builds on every PR and publishes
on every main push, tagged `r<UTC build minute>-<sha6>`, by sha and
`latest`.

## Configuration

The knob table is in [config.py](millrace/config.py)'s module docstring
and [architecture.md §7](architecture.md#7-configuration). The ones that
shape behavior:

| Knob | Default | Meaning |
|---|---|---|
| `MILLRACE_FLUSH_BYTES` | 256 MiB | staged bytes per flush decision: the size lane fires at it and one scan reads at most it (always ≥ 1 row). Compaction owns final file sizing |
| `MILLRACE_FLUSH_DEADLINE_S` | 900 | a group at least this old with ≥ 1 MiB staged flushes |
| `MILLRACE_SLOW_LANE_DEADLINE_S` | 21600 | any group this old flushes |
| `MILLRACE_KAFKA_AUTO_OFFSET_RESET` | `latest` | where a partition with no committed offset starts |

## Destination

Created from configuration at startup when missing
(`destination.py:ensure_destination`), never inferred from data: the
partition tuple is the staging key and unknown payload keys are dropped
(no schema evolution), so the spec and every column must exist before
the first record is staged.

| Knob | Used when | Value |
|---|---|---|
| `MILLRACE_CATALOG_DATA_PATH` | the catalog does not exist | `s3://bucket/prefix/`; a different bucket from `MILLRACE_STAGE_URL` (same bucket is refused) |
| `MILLRACE_TABLE_COLUMNS` | the table does not exist | the complete column list `name:type,…` (all nullable; nested, `variant`, duplicates and `_hog*` refused) |
| `MILLRACE_PARTITION_BY` | creation; compared with an existing table | `col`, `identity(col)`, `year/month/day/hour(col)`, `bucket(col, N)` |
| `MILLRACE_SORT_BY` | same | `col [asc|desc] [nulls_first|nulls_last]`, … |

Catalog, namespace and table are each created if missing; the table in
one atomic request (columns, spec and sort together; millrace requires
the server's atomic-creation capabilities rather than create + alter).
An existing object is never altered: a difference from the
configuration is one WARNING carrying both sides. The replica race is
tolerated (the loser re-reads). Recommended dev shape:

```
MILLRACE_CATALOG_DATA_PATH=s3://posthog-gigahog-mw-dev/millrace/millrace-dev/
MILLRACE_TABLE_COLUMNS=team_id:long,timestamp:timestamptz,distinct_id:string,event:string,properties:json
MILLRACE_PARTITION_BY=identity(team_id),day(timestamp)
MILLRACE_SORT_BY=timestamp
```

## Startup drain and probes

On start each replica opens its home stage (the slot of its smallest
claimed partition) plus every other existing slot `j` with `j % R ==
ordinal` (the home of a replica removed by a scale-down), and **flushes everything staged before it
consumes anything new**. Drained foreign slots are closed and left in
place. Change the replica count with the fleet scaled to zero: a
mixed-`R` fleet is unsupported. Staged state therefore never outlives
a process; that is what lets group sizes and ages live in memory only.
A crash mid-drain drains again on restart (windows that already landed
land again: duplicates, accepted).

**The stage format changed** with the home stage (row keys carry the
Kafka partition): the drain refuses a stage written by an earlier build
(`StageFormatError`, "wipe MILLRACE_STAGE_URL"). Wipe the stage before
deploying this build, and before rolling back past it.

The drain is a stream: per stage, one sequential read-ahead pass
over the staged rows, cut into one-group windows (at the group's end or
at `MILLRACE_FLUSH_BYTES`), packed into batches of up to
`MILLRACE_FLUSH_BYTES` and 512 windows (`max_files_per_commit`), each
batch one commit (more if its groups fan out past 512 files), flushed
and settled as the pass goes; stages drain concurrently with at most 4
batches (the flush concurrency) in memory. Progress is logged per stage (`startup drain of
<topic>[<p>]: …` at start, every 1,000 windows or 30 s, and at the end)
and counted in `millrace_drain_rows_total` / `millrace_drain_bytes_total`.
Small groups share commits, so drain time is mostly the stage reads
and the parquet builds. The stream
reopens its iterator past the last key every 60 s and after a read
error, so an SST the maintenance GC deletes under it costs a reopen,
not a restart.

- `/readyz` answers 503 for the whole drain, then 200 once the consumer
  is assigned. `millrace_draining` is 1 meanwhile.
- **The startup probe must allow for the drain.** Drain time grows with
  the staged backlog (roughly staged bytes ÷ flush throughput; minutes
  after a long hoglake outage). The chart sets the startup probe; size
  its `failureThreshold × periodSeconds` to the worst backlog you are
  willing to drain, or the kubelet restarts the pod mid-drain (safe,
  but it starts over). Liveness stays 200 during the drain.
- A StatefulSet rollout waits on readiness pod by pod, so a long drain
  slows the rollout; Kafka lag grows meanwhile (durable, at-least-once).
- A group that keeps failing during the drain (see "Stuck groups")
  does not block it: the stream skips the rest of its rows (they stay
  staged), the drain retries it after the stream and finishes when
  every group is flushed or stuck, and stuck groups keep retrying while
  the pod consumes.
- `/healthz` 503s once a terminal condition latches: the destination
  was recreated (exit 3) or a stage was fenced (exit 4).

## Stuck groups

Any flush failure leaves the rows staged and retries the group with
backoff (2 s doubling to 120 s). Due groups that are not at the size
lane are flushed in batches, one commit per batch (more past 512
files); a refused batch
(400/422) splits: each of its groups retries alone, so one bad group
never holds the others back. Other batch failures retry the batch. After 3 consecutive failures the group is
logged at ERROR (`flush STUCK: …`), `millrace_flush_groups_stuck_total`
increments and `millrace_flush_groups_stuck` counts it until it
succeeds; other groups keep flushing. Alert on the gauge. Refusals are
counted by status in `millrace_flush_commit_refusals_total{status}`.

## Assignment, rollouts and fencing

Static assignment (millpond's model): replica `ordinal` (the trailing
`-<n>` of `POD_NAME`) claims every partition `p` with
`p % MILLRACE_REPLICA_COUNT == ordinal`, the partition count read once
at start. An empty share or an unknown topic exits 2. `spec.replicas`
and `MILLRACE_REPLICA_COUNT` must change together with the fleet fully
down. **Adding partitions to the topic** requires restarting the fleet
promptly: assignment is read once at start, and a new partition has no
committed offset, so under `auto.offset.reset=latest` (the policy) the
records produced to it before its ordinal restarts are skipped.

SlateDB is single-writer per path and fences lazily: a newer writer's
open succeeds and the old one learns at its next write. A fenced
process exits **4**; do not restart it onto the same assignment (it
would fence the new owner back). The rollout needs a barrier so an
ordinal's new pod starts only after the old one is gone. A fenced
writer can never commit Kafka offsets past unstaged data (every poll
writes durably first, a fence probe when all its records were dropped)
nor delete staged rows; at worst it publishes a duplicate.

## Dropped records

Timestamps outside years 1–9999 are clamped to the nearest
representable instant (counted in
`millrace_values_clamped_total{column}`, rate-limited WARNING), not
dropped. Only records that cannot be represented at all are dropped; each is
counted in `millrace_records_dropped_total{reason}` and logged (consume
side: at most one line per partition and reason per 10 s). Event times
are never judged: ancient and far-future events are written.

| Where | `reason` | Cause |
|---|---|---|
| Consume | `invalid_offset` | offset < 0 or > int64 max |
| Consume | `missing_value` | tombstone |
| Consume | `malformed_value` | not a strict JSON object |
| Consume | `malformed_partition_source` | a partition source value of the wrong type or uncoercible (the log names the path) |
| Flush | `undecodable_payload` / `uncastable_column` | a value no column can hold (the log names the first refusing column path, never the value) |

## SlateDB and pod memory

Every instance opens with explicit settings; the pod shares one SST
cache. These are constants (`config.SlateDbConfig`): unflushed bytes
256 MiB per instance, L0 stall depth 8, block and meta cache 128 MiB
each per pod. The writer's embedded GC is always off: the maintenance
service (`python -m millrace.maintenance`, one per staging root) owns
collection for every pipeline, which keeps LIST/DELETE traffic off the
ingest pods; it also exports L0 depth per DB. The embedded compactor is
never disabled (that stalls memtable flushes, parity-pinned).

Pod memory, in terms:

```
  K partitions × 256 MiB unflushed
+ 256 MiB shared cache × ~1.05
+ 256 MiB librdkafka queue + 256 MiB fetch
+ 256 MiB consume batch
+ 4 in-flight decisions × ~1.1 GiB at MILLRACE_FLUSH_BYTES=256 MiB (measured)
+ process baseline
```

During the startup drain the 4 decisions are the drain's in-flight
windows, and each partition also holds one row stream:

```
  K partitions × ( (L0 SSTs + sorted runs) × 2 fetches × 1 MiB read-ahead
                   + ≤ 16 MiB page )
```

Measured: 4 partitions × 8 overlapping L0 SSTs streamed +157 MiB RSS at
221 MiB/s (60 ms store latency); the window-scan options (4 × 4 MiB)
took +316 MiB for 230 MiB/s, so the stream uses the smaller ones
(`stage._drain_scan_options`).

At 16 partitions per pod that is ~4 GiB unflushed + ~0.8 GiB queues and
cache + ~4.4 GiB of decisions + baseline: a 1-replica, 16-partition
pod does **not** fit 10 GiB; 4 partitions per pod (~7 GiB) does. The
catch-up profile peaked at 6.6 GiB with 4 partitions.

Row scans read ahead in 4 MiB ranged GETs with 4 concurrent fetches and
do not cache their blocks (`stage._bulk_scan_options`; slatedb's
default reads one ~4 KiB block per GET). No scan and no payload
lowering ever runs under a stage's write lock.

**Binding patch.** slatedb 0.17's generated uniffi binding copies every
lowered bytes value into the Rust buffer one byte per Python iteration
(90% of GIL time during a catch-up). `millrace/slatedb_patch.py`
replaces that copy with one `ctypes.memmove` at import of `stage.py`,
after checking the binding's shape and a self-test round trip; if
either fails it leaves the binding alone and logs a WARNING.

## Test layers

Unmarked = unit (pure), `component` = SlateDB on `memory:///`,
`integration` = `file:///`, `live` = the docker stack (deselected unless
`-m live`). The rate benches are deselected by default:
`uv run pytest --bench -m bench -s`.
