# millrace

Kafka → hoglake ingestion daemon with SlateDB staging: consume a topic,
stage rows in an embedded, object-storage-native LSM keyed by partition
value, and flush per key on size/age readiness into parquet + prepared
commits. At-least-once end to end — Kafka offsets commit strictly after
durable staging; staged ranges delete strictly after a commit receipt.
The receipt dedupe rests on server-side receipts, which are purged
(`HOGLAKE_RECEIPT_RETENTION_SECONDS`, default 7 d — floored per catalog
at twice its snapshot retention, capped at 30 d): a persisted request
whose receipt 404s is replayed only inside the receipt horizon
(`MILLRACE_RECEIPT_HORIZON_S`, default 6 d — kept deliberately below the
server's), and the pipeline halts loudly past it rather than risking a
duplicate. The dependency is **not on the wire** — the server exposes
snapshot retention but never receipt retention (verified against the
OpenAPI: `CatalogOptions` carries `snapshot_retention_seconds` only;
`GET /v1/info` is instance identity) — so the horizon knob CARRIES the
assumption, and an operator who lowers the server's receipt retention
(or raises a catalog's snapshot retention past ~3.5 d, which lifts the
floor) must lower this knob with it. A pyhoglake that grows
`Catalog._receipt_retention_seconds` is honored instead (the guarded
seam, `flush.py:_receipt_horizon_s`).

Design: [docs/kafka-ingestion.md](../docs/kafka-ingestion.md).
Build/test sequence: [docs/kafka-ingestion-plan.md](../docs/kafka-ingestion-plan.md).

Status: Phase 5 — `keyspace.py` (staging key codecs, side prefixes,
idempotency-key derivation, the `poison/` quarantine codec),
`planner.py` (pure per-key flush decisions: the size/age/slow
three-lane policy behind a policy seam, commit chunking), `stage.py`
(the per-partition SlateDB staging layer:
`PartitionStage` + `StageManager`), `consumer.py` (the consume loop:
static and cooperative assignment claiming via `StageManager.sync_assignment`,
poll → stage → commit with offsets strictly after durable staging and
the poll's per-partition stage batches overlapped (one WAL durability
wait per partition, in flight together), the `value-json:<field>` team
codec for topics not keyed by team, the FENCED halt below, poison
quarantine), `flush.py` (the flush
loop: planner-driven sweeps, staged range → Arrow → partition fanout →
parquet → persisted prepared commit → settle, the day-one wire contract
(`read_snapshot` + `expected_table_uuid`, `totals=false` identity reads,
retention/4 shape cache), the full refusal taxonomy with pinned
recoveries, byte-identical replay of persisted requests, receipt-based
recovery handoff, junk `event_time` clamp/quarantine, orphan
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
reassignment, junk `event_time` quarantine, whale overload + tail
freshness, the freshness SLA measurement, the DDL race, drop+recreate
incarnation halt, and a compaction observation.

- `just millrace unit` — unit + component tests (pytest, `memory:///`)
- `just millrace test` — the whole non-live suite, including `file:///`
  integration (durability across hard kill, contested-open fencing)
- `just millrace lint` — ruff check/format + mypy
- `just millrace live` — the live docker stack suite
  (`.artifacts/live-millrace/`); fails on a missing/empty/skipped/failed
  report, wired into CI as the `live-millrace` job of the reusable
  `python-live.yml` workflow

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
  single-team shape (an L0 SST per ~1.2 s against the compactor's 5 s
  poll) is the one pilot question the local bench cannot answer, and
  the debt series is its tripwire.

## Quarantine retention (`poison/`)

Quarantined records are forensic state with a bounded lifetime, not an
unbounded prefix: every entry carries the staging-clock stamp of its
write, and a per-process sweeper (off the consume/flush path) deletes
entries older than `MILLRACE_POISON_RETENTION_S` (default 7 d, matching
the server's default receipt retention — the incidents poison matters
to live inside that window) per partition every
`MILLRACE_POISON_SWEEP_S` (default 1 h), in bounded batches.
`millrace_poison_entries_purged_total` counts deletions. Entries
without a stamp (written before this policy, or by the flush path until
it passes its clock) are kept forever — unknown age is never deleted.
Nothing depends on poison presence, so a pod that is down at expiry
simply purges at its next sweep.

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
partition count (tens), never by teams. The staged gauges —
`millrace_staged_bytes`, `millrace_staged_rows`,
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
(M5's measurement) — are non-gating and deselected by default:

```
uv run pytest --bench -m bench -s
```
