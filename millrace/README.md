# millrace

Kafka → hoglake ingestion daemon with SlateDB staging: consume a topic,
stage rows in an embedded, object-storage-native LSM keyed by the
destination table's partition tuple (computed per record from the JSON
payload under the table's live partition spec), and flush per tuple on
size/age readiness into parquet + prepared commits — one file per tuple
in the steady state. At-least-once end to end — Kafka offsets commit strictly after
durable staging; staged ranges delete strictly after a commit receipt.
The receipt dedupe rests on server-side receipts, which are purged
(`HOGLAKE_RECEIPT_RETENTION_SECONDS`, default 7 d — floored per catalog
at twice its snapshot retention, capped at 30 d): a persisted request
whose receipt 404s is replayed verbatim whatever its age. Past the
receipt retention the replay's read snapshot is normally below the
catalog's expiry floor, so it is answered 410 and re-prepared from the
staged rows; if the first attempt had landed, the window lands a second
time (duplicate rows). That is accepted at-least-once behaviour and is
counted (`millrace_prepared_reprepared_total`,
`millrace_prepared_replays_total`; the dropped entry's objects, possibly
live, in `millrace_flush_unresolved_uploads_total`). Rows are staged
under `(partition tuple, offset)`. The idempotency key includes a
SHA-256 of the window's rows, and a fresh flush looks it up before it
builds: offsets re-consumed below the settled floor settle from a
published window's receipt only when the rebuilt window has the same
group, bounds and rows and the receipt still exists
(`millrace_flush_deduped_total` — nonzero means a rewind re-staged
identical data); otherwise they publish as a new commit and the shared
rows land twice. Details:
[architecture.md](architecture.md) §1 and §6.

Design: [docs/kafka-ingestion.md](../docs/kafka-ingestion.md).
Build/test sequence: [docs/kafka-ingestion-plan.md](../docs/kafka-ingestion-plan.md).

Status: Phase 5 — `keyspace.py` (staging key codecs, side prefixes,
idempotency-key derivation),
`planner.py` (pure per-key flush decisions: the size/age/slow
three-lane policy behind a policy seam), `grouping.py` (the
partition-tuple keyer: payload → the destination's partition tuple; it
reads struct-leaf sources such as `properties.team_id` by path),
`coerce.py` (the per-column JSON coercers the keyer and the flush decoder
share: scalars, `json` and `struct`/`list`/`map` at any depth; a
`variant` column is a startup refusal, as pyarrow cannot write the
native Parquet VARIANT annotation), `stage.py`
(the per-partition SlateDB staging layer:
`PartitionStage` + `StageManager`), `consumer.py` (the consume loop:
static and cooperative assignment claiming via `StageManager.sync_assignment`,
poll → stage → commit with offsets strictly after durable staging and
the poll's per-partition stage batches overlapped (one WAL durability
wait per partition, in flight together), staging under the partition
tuple with the keyer re-read on the flusher's shape TTL (the Kafka key
is never read), the FENCED halt below, malformed-record drops), `flush.py`
(the flush
loop: planner-driven sweeps, staged range → Arrow → live-spec partition
fanout (a fanout past `MILLRACE_MAX_FILES_PER_COMMIT` halts
`fanout_exceeds_commit_bound`; there is no commit chunking) →
parquet → persisted prepared commit → settle, the day-one wire contract
(`read_snapshot` + `expected_table_uuid` pinned at the startup
resolve — a later uuid change halts `table_recreated`, `totals=false`
identity reads, retention/4 shape cache), the full refusal taxonomy
(a 409 `commit_conflict` resends the identical bytes up to 5 attempts,
then `FlushPublishExhausted`) with pinned
recoveries, byte-identical replay of persisted requests, receipt-based
recovery handoff, junk `event_time` clamp/drop, orphan
accounting), `maintenance.py` (the external GC + compaction-debt
service, `python -m millrace.maintenance`, deployed beside the
replicas — writers shed the embedded GC by default
(`MILLRACE_SLATEDB_GC_ENABLED=false`) because this service owns
collection, and must never disable `compactor_options`;
docs/kafka-ingestion.md §Maintenance services),
`main.py` + `server.py` (the process entry point: env →
config → StageManager → consumer + flusher + FlushRunner on one asyncio
loop, plus the `/metrics` `/healthz` `/readyz` surface; halt exit code
3, fenced exit code 4)
are implemented and tested. The live docker suite (`tests/live/`,
marker `live`, run by `ci/live-millrace.sh`) covers the ten Phase 5
scenarios: end-to-end truth with exact per-tenant reconciliation,
kill -9 mid-flush, the lost commit response (an in-test TCP proxy drops
the response AFTER the server commits), static and cooperative
reassignment, junk `event_time` drop, whale overload + tail
freshness, the freshness SLA measurement, the DDL race, drop+recreate
incarnation halt, and a compaction observation. Code-level map:
[architecture.md](architecture.md).

- `just millrace unit` — unit + component tests (pytest, `memory:///`)
- `just millrace test` — the whole non-live suite, including `file:///`
  integration (durability across hard kill, contested-open fencing)
- `just millrace lint` — ruff check/format + mypy
- `just millrace live` — the live docker stack suite
  (`.artifacts/live-millrace/`); fails on a missing/empty/skipped/failed
  report, wired into CI as the `live-millrace` job of the reusable
  `python-live.yml` workflow

## Staging format

The staging keyspace is keyed by the destination's partition tuple
(prepared envelope v5); it shares no key or value shape with the
#331-era team-keyed layout. millrace requires a FRESH staging root — a
new `MILLRACE_STAGE_URL` prefix. There is no code guard: pointed at an
old root it fails at open or at the first sweep with the `ValueError`
from `keyspace.decode_prepared_value` / `decode_stats_key`.

## Rollouts and fencing (static assignment)

SlateDB is single-writer per partition path and fences LAZILY: a second
writer's open succeeds, and the older writer learns it lost the path at
its next write's durability wait (`Error.Closed(FENCED)`, pinned by
`tests/test_slatedb_parity.py`). Under static assignment no rebalance
event announces the loss — an EKS one-at-a-time StatefulSet rollout
double-assigns the ordinal while the old pod drains, and the new pod
opens the same SlateDB paths. The consume loop therefore catches FENCED
at the staging write (and the flush loop's sweep surfaces an idle
partition's fence at its next planning read), closes the fenced stage
locally, and halts
with `ConsumerFencedError` — deliberately DISTINCT from a crash or a
flush halt: a blind restart would re-open the path and fence the NEW
owner, and the two pods would fence each other until the rollout
replaces one, offsets never advancing.

So: **static mode on Kubernetes needs a rollout barrier** — a PreSync
hook that keeps the new ordinal's pod from starting until the old one
is fully gone (the compaction work's rollout research) — or run
`MILLRACE_KAFKA_ASSIGNMENT=cooperative`, where revocation events make
the ownership handoff explicit and the fence becomes a backstop rather
than the signal. Cooperative is the recommended mode for new
deployments without a barrier. The process-level wiring: a fenced
consumer exits with code **4** (`EXIT_FENCED`, distinct from a generic
failure's 1, a config error's 2 and a flush halt's 3) so a supervisor
can refuse the restart-into-the-same-path, and while the fenced process
drains, `/healthz` and `/readyz` both 503 with the fence named.

## SlateDB settings and pod memory

Every partition's SlateDB instance opens with explicit `Settings` wired
from `MILLRACE_SLATEDB_*` (config.py's table; the binding's defaults
are per-instance and there are K instances per pod, so defaults need
arithmetic, not trust):

- `MILLRACE_SLATEDB_GC_ENABLED` (default **false**) — the
  writer-internal GC loop is off because the external maintenance
  service (below) is part of every millrace deployment and owns
  collection; doubled GC is wasted LIST/DELETE traffic on the replicas
  (idempotent and overlap-safe, so a misconfigured doubling is
  wasteful, never corrupt). Boot logs a loud warning when off: the
  setting ASSUMES the service exists. There is deliberately no knob for
  `compactor_options` — disabling the writer's embedded compactor
  stalls memtable→L0 flushes at `l0_max_ssts` with nothing to drain
  them (parity-pinned in `tests/test_slatedb_parity.py`).
- `MILLRACE_SLATEDB_MAX_UNFLUSHED_BYTES` (default 256 MiB) caps one
  instance's unflushed memtable+WAL backlog. **Per-pod worst case:
  K × this.** At 64 assigned partitions that is 16 GiB against the
  library default's 64 GiB — and the flush sweep's in-flight scans
  (`4 × MILLRACE_FLUSH_CONCURRENCY` bounded scans of at most
  ~`MILLRACE_TARGET_OUTPUT_BYTES ÷ compression ratio` staged bytes
  each) sit on top. Size the pod's memory request against the sum, not
  the library default.
- `MILLRACE_SLATEDB_L0_MAX_SSTS` (default 8, SlateDB's own) — the L0
  depth at which writes stall when the embedded compactor falls behind.
  The stall precursor is observable before it bites: the maintenance
  service exports `millrace_maintenance_compaction_l0_ssts
  {topic,partition}` per DB and `..._max` fleet-wide, and the pod's own
  `millrace_slatedb_l0_stalls_total` / `millrace_slatedb_write_stalls_total`
  count stalls after the fact. The stage-rate bench at the whale shape
  (`uv run pytest --bench -m bench -s`, `tests/test_stage_bench.py`)
  measures ~7.3K msg/s per partition sustained with zero write stalls
  in every arm (the shape is latency-bound at the 100 ms WAL flush
  interval, ~5x the whale's 1.5K msg/s requirement) — the 55 MB/s
  single-group shape (an L0 SST per ~1.2 s against the compactor's 5 s
  poll) is the one pilot question the local bench cannot answer, and
  the debt series is its tripwire.

## Dropped records

Malformed records are dropped, logged and metered; nothing is
quarantined or retained. Each drop increments
`millrace_records_dropped_total{reason}` and logs at WARNING. The Kafka
record timestamp is never read (rows are keyed `(partition tuple,
offset)`; staging stats use the staging clock), so no record is dropped
for its timestamp.

| Where | `reason` label | Cause |
|---|---|---|
| Consume | `invalid_offset` | Offset < 0 or > int64 max |
| Consume | `missing_value` | Tombstone |
| Consume | `malformed_value` | Not a strict JSON object |
| Consume | `malformed_partition_source` | A partition source scalar cannot be keyed; the log line names the path (`malformed_partition_source:<path>`), the label does not |
| Flush | `undecodable_payload` / `uncastable_column` | The payload does not decode or cast against the table's columns (the drop log line names the first refusing column's dotted path, `first_path=properties.inner.id`, never the value), or defeats the Arrow build |
| Flush | `event_time_out_of_window` | Junk `event_time` under `MILLRACE_EVENT_TIME_POLICY=drop` (the default) |

Consume-time drop lines name topic, partition, offset and reason, at
most one per (partition, reason) per 10 s (the first drop of a reason
always logs; later lines carry the suppressed count); the offset commit
covers a dropped record like a staged one. Flush-time drops log one
line per reason per decision with the group and offsets; the dropped
rows are deleted by their window's ordinary settlement.

## Probes and per-partition metrics

`/healthz` (liveness) is no longer a static 200: it 503s — with the
reason in the body — when the pipeline is in a latched terminal state
(the flush runner halted, the consumer fenced, a partition fenced out
of flush scheduling — e.g. the quiet-partition fence nothing else
surfaces). Latched states only, so the probe cannot flap. `/readyz`
503s until the consumer has started and whenever liveness fails: a pod
that lost a partition's ownership is not ready to serve it.
`/metrics` carries bounded per-partition series —
`millrace_partition_staged_bytes{topic,partition}`,
`millrace_partition_oldest_staged_age_seconds{topic,partition}`,
`millrace_flush_partition_fenced{topic,partition}` — so a stuck
partition is findable; cardinality is bounded by the pod's assigned
partition count (tens), never by partition tuples. The staged gauges —
`millrace_staged_bytes`, `millrace_staged_rows`, `millrace_staged_groups`
(partition tuples holding staged rows),
`millrace_oldest_eligible_staged_age_seconds` (the oldest key that is
eligible under ANY flush lane, so pathological slow-lane tenants don't
trip it) — plus SlateDB's own stall series are alert inputs; nothing in
the consume loop reads them.

The maintenance service (`python -m millrace.maintenance`) does GC plus
compaction-debt monitoring: per swept DB it reads the compactor state
view and exports the L0 SST count per `{topic,partition}` (it is the
fleet authority on debt, so the labels are affordable here) plus the
fleet max. Its discovery listing of the staging root runs at most once
per `MILLRACE_MAINTENANCE_DISCOVERY_INTERVAL_S` (default 1 h) and is
cached between listings — a new partition's DB waits at most one
cadence for its first sweep, and a listing failure with a warm cache
degrades to the cached set.

Test layers (docs/kafka-ingestion-plan.md): unmarked = unit (pure, no
I/O), `component` = SlateDB on `memory:///`, `integration` = SlateDB on
`file:///` (no docker), `live` = the docker stack (deselected unless
`-m live` is passed — see tests/conftest.py). The benches — the SlateDB
rate bench (validation item 2) and the flush sweep throughput bench
(M5's measurement: 1024 ready keys total, 256 per partition stage × 4
stages) — are non-gating and deselected by default:

```
uv run pytest --bench -m bench -s
```
