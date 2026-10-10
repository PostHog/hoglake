# Kafka ingestion — implementation plan

Companion to [kafka-ingestion.md](kafka-ingestion.md) (the design). This
is the build-and-test sequence. The plan is test-led because the design's
risks are behavioural (durability ordering, retry taxonomy, per-key
scheduling under skew), not algorithmic: every phase lands its tests with
its code, and no phase is done while its tests are local-only — *a suite
that only runs locally rots* (AGENT.md).

Sources of discipline, and where each is applied below:
**hoglake/AGENT.md** (CI gates, no-skip live suites, mutation-tested
predicates, growth arithmetic), **hedgerow** (scripted fakes +
adversarial layers, persisted prepared requests, viaduck lessons),
**millpond/AGENT.md** (deterministic commit messages, injected clocks,
probe patterns, image pinning, the `_liveness_status` pure-function
pattern).

## Test architecture (applies to every phase)

Four layers, each with a distinct job; a failure mode caught by exactly
the cheapest layer that can see it:

| Layer | Stack | Sees |
|---|---|---|
| unit | pytest + hypothesis; injected clock/HTTP | pure decisions: planner, keyspace, formatters, retry classification |
| component | scripted fakes (hedgerow `fakes.py` pattern) + SlateDB on `memory:///` | loop logic, ordering guarantees, crash/replay without docker |
| integration | real SlateDB on `file://`, pytest-httpx scripted hoglake | persistence across restart, wire-shape conformance |
| live | docker: hoglake server (built from checkout) + Postgres + MinIO + apache/kafka | the truth: end-to-end, kill -9, DDL races, rebalance |

Hard rules, inherited verbatim:

- **No skips, no xfail, no empty reports, anywhere.** The live harness
  fails on a skipped test the way `ci/live-python.sh` +
  `check_live_results.py` do today. A green run without the stack up is
  not a verification.
- **Mutation-test every load-bearing predicate** (planner triggers,
  deadline arithmetic, range bounds, idempotency-key inputs): flip it,
  the narrowest test must red. For Python: `mutmut` on
  `millrace/planner.py` and `millrace/keyspace.py`, run on PRs touching
  those files.
- **Clocks and monotonic time are injected** everywhere (millpond's
  `self._monotonic` lesson: a test crosses a TTL by advancing the fake,
  never by sleeping).
- **Deterministic commit text**: no wall-clock, randomness or
  per-attempt state in the commit message or idempotency key; a rebuild
  in a new process must produce identical bytes (millpond's formatter
  rules, including *formatted before the upload* and a source-order
  test pinning it).
- **Test stacks pin images by digest** (millpond's `:latest`
  substitution lesson).
- **Skew-shaped fixtures**, never uniform toys: the generator emits a
  Zipf-α≈1 tenant distribution, because every interesting failure lives
  in the skew.
- PR bodies carry **growth arithmetic** for anything written per commit
  or per file (AGENT.md scale doctrine), even though this component
  writes no server SQL — it *drives* the tables the doctrine protects.

## Phase 0 — scaffold, CI, and the gates

**Scope.** `millrace/` tree: `pyproject.toml` (uv; Python 3.12; flox),
version `0.1.0.dev0`; deps pyhoglake (path), slatedb, confluent-kafka,
pyarrow, prometheus-client, httpx; dev group with **pinned ruff**, mypy,
pytest, hypothesis, pytest-httpx. Module skeleton: `config.py`,
`consumer.py`, `stage.py`, `keyspace.py`, `planner.py`, `flush.py`,
`server.py` (health/metrics), `main.py`.

**CI wiring (the phase's real content).**
- `just millrace unit`, `just millrace lint`, `just millrace live`; `lint-all`
  picks up the tree.
- `ci-python.yml`: `changes` job gains `millrace/**`; the millrace checks
  fan into the **`python-checks` gate** — require the gate, never a leaf
  (AGENT.md: #304's lesson).
- `ci/live-millrace.sh` modeled on `ci/live-python.sh`: isolated PG /
  server-from-checkout / MinIO / kafka on high ports, JUnit + logs to
  `.artifacts/live-millrace/`, services torn down on exit.
- AGENT.md gains the component row (name, stack, test story).

**Tests.** A trivial unit suite plus one meta-test that fails if the
millrace suite is absent from the gate's inputs — the "workflow that
path-filters itself out leaves the check expected forever" failure mode,
pinned.

**Exit:** a docs-only PR touching `millrace/**` produces green
`python-checks`; the live harness runs and reports zero tests as a
*failure* (proving the gate works before it matters).

## Phase 1 — keyspace and planner (the pure core)

**Scope.** `keyspace.py`: `(team_id, timestamp, offset)` row-key codec,
`stats`/`sched_age` prefixes, side prefixes for Kafka offset ranges,
persisted prepared requests (`prepared/`, v3 envelopes carrying the
idempotency key's `topic`/`partition` derivation inputs, with
`persisted_at` for the receipt-horizon guard), and the legacy `flushed/`
marker prefix (never written — settlement is one atomic transaction
with the receipt in hand, so a marker could only restate both;
recovery collects leftovers in bounded pages). The keyspace carries no
partition component: one SlateDB instance per claimed Kafka partition,
so the instance is the partition (design doc §Staging layout). `planner.py`:
pure functions `stats snapshot + now → [FlushDecision]`; trigger policy
(size for whales, 10–15 min age for the tail); churn reaper; commit
chunking.

**Tests.**
- hypothesis: codec round-trips and ordering (key bytes sort =
  (team, ts, offset) sort); a staged range is contiguous under scans;
  planner determinism; size and age triggers are mutually exclusive per
  decision; **arrivals never reset `first_staged_ts`** (the property the
  tail SLA stands on); idempotency-key stability across processes.
- Golden vectors for key encoding.
- Planner replay: a recorded skewed arrival stream drives the pure
  planner; assertions on file-size and latency distributions *per tier*
  (this is design-doc validation item 4, landed as a unit suite).
- `mutmut` on both modules; every surviving mutant is either killed or
  argued in the PR body.

**Exit:** property suite green, mutation-clean, replay numbers attached
to the PR.

## Phase 2 — staging (SlateDB)

**Scope.** `stage.py`: microbatched `WriteBatch` puts, transaction
covering row puts + stats + sched maintenance, range scans, range
deletes, staged-bytes accounting for backpressure.

**Tests.**
- Component, `memory:///`: put/scan/delete correctness; the
  offset-commit invariant as a *call-order* test (offset commit is never
  observed before the stage write returns — hedgerow's rows-then-offset,
  restated).
- Integration, `file:///`: **durability across hard kill** — a
  subprocess stages, is SIGKILLed, reopens, and every acknowledged row
  is present; staged stats reconcile with row scans.
- **Binding-parity probes as permanent tests**: transactions with
  isolation, scan `seek`, range delete, checkpoint — each exercised so a
  UniFFI binding regression fails loudly at upgrade time, not in prod
  (validation item 2, encoded).
- SlateDB rate bench (validation item 2's number): put/scan/delete at
  the per-pod event-rate slice with the real record size; results in the
  PR body, thresholds as a non-gating trend test.

**Exit:** crash-durability suite green; rate numbers recorded.

## Phase 3 — consume

**Scope.** `consumer.py`: confluent-kafka, partition claiming that
follows the consumer assignment (a replica claims a partition *and* its
SlateDB instance together; assignments may change between deploys),
`auto.offset.reset=earliest` (millpond AGENT.md:
`latest` silently drops existing data — a removal-by-omission),
pause/resume from staged-bytes and oldest-staged-age gauges, poison-pill
quarantine.

**Tests.**
- Scripted fake consumer (hedgerow `fakes.py` shape): loop logic,
  pause/resume thresholds (pure decision function, `_liveness_status`
  pattern), quarantine routing, rebalance listener (revoked partitions
  flush nothing, abandon nothing).
- Backpressure: gauge thresholds crossed → consumer paused exactly once,
  resumed exactly once; no flapping (hypothesis over gauge sequences).

**Exit:** fake-driven suite covers every loop branch.

## Phase 4 — flush and the wire contract

**Scope.** `flush.py`: scan staged range → Arrow → junk
quarantine/clamp → partition fanout `(team_id, month(timestamp))` →
parquet via pyhoglake → persist prepared request → `commit/prepared` →
delete staged range. Wire correctness from day one: `read_snapshot`,
`expected_table_uuid`, `totals=False` identity reads, table-shape cache
with TTL = retention/4 (millpond's cache-TTL lesson), deferred stats on
age flushes / footer stats on size flushes.

**Tests (pytest-httpx scripted server — pyhoglake's own pattern).**
- The full refusal taxonomy, each with its recovery pinned:
  `commit_conflict` → refresh + retry; `ddl_since_read_snapshot` →
  re-read + re-prepare (never replay); `table_recreated` → halt loudly;
  410 → halt with reconcile instructions; 503 + `Retry-After` →
  identical-retry backoff; 422 → quarantine the decision, alert.
- **Persisted-request discipline**: after a simulated crash, the
  re-publication is *byte-identical* to the persisted request; a test
  asserts no code path regenerates one after an ambiguous response.
- Receipt dedupe: second publish of an idempotency key returns the
  original result and does not double-count `records_written`.
- Stats asymmetry: age-triggered flushes ship `record_count` only,
  size-triggered ship full footer stats.
- Orphan accounting: failure between upload and persist counts and logs
  the abandoned objects (hedgerow buffered's known gap, made visible).

**Exit:** taxonomy suite green against the scripted server.

## Phase 5 — live (docker)

**Scope.** `tests/live/` + compose stack: hoglake server built from the
checkout, Postgres, MinIO, apache/kafka (stack shape and digest pinning
from millpond's `tests/hoglake_stack`); skewed-distribution producer
fixture.

**Scenarios** (each also has a fake-layer twin where feasible —
hedgerow's dual-layer rule):
1. End-to-end truth: produce → every committed offset queryable via
   pyhoglake scan; per-tenant row counts reconcile exactly.
2. `kill -9` mid-flush → restart → no loss, receipt dedupe, no
   double-count.
3. Lost commit response (connection dropped after the server commits)
   → replay resolves via receipt.
4. Rebalance storm → no range is flushed by two owners.
5. Junk `event_time` dates quarantined; no future-dated partitions.
6. Whale overload: one tenant at ~10³× the median → full-size files on
   the size trigger; the tail still meets the age deadline in the same
   run.
7. **Freshness SLA assertion**: p99 produce→queryable ≤ deadline +
   margin, measured, under the skewed load.
8. DDL race: concurrent `add_column` mid-flush →
   `ddl_since_read_snapshot` → re-prepare succeeds (millpond's
   `test_concurrent_writers_evolving_and_appending` equivalent).
9. Drop+recreate mid-flight → incarnation guard halts the pipeline,
   loudly (viaduck lesson 3).
10. Compaction observation: the run leaves compaction-debt metrics
    no worse than the arrival-rate model predicts — the first real
    reading for validation item 1.

**Exit:** all ten green in CI via `ci/live-millrace.sh`, wired into the
live gate; the harness fails on skips.

## Phase 5.5 — external maintenance services

Per docs/kafka-ingestion.md §Maintenance services: a separate
deployment looping all per-partition SlateDB instances (across topics
and replicas) for GC now and compaction when the binding route lands.

- **GC + debt-monitoring service** (the decided scope, 2026-10-09 —
  route 3 in the design doc): pure Python over `Admin.run_gc_once` for
  GC, plus compaction-debt monitoring per instance
  (`read_compactor_state_view` → metrics/alerting). Discovery by prefix
  listing, bounded budgeted sweeps, jittered cadence, per-DB failure
  containment. Parity tests first: GC concurrent with an active writer
  (integrity reconciles throughout, garbage actually reclaimed), GC vs.
  checkpoints, and the stalled-compaction backpressure shape
  (`l0_max_ssts` → writer stalls → consumer pause).
- **Compaction service**: deferred (route 3 decision). The binding gap
  stands (no `CompactorBuilder` in UniFFI; `submit_compaction` is
  queue-only — probe-verified 2026-10-09). Revisit when replica
  compaction traffic justifies the upstream contribution (route 1) or
  the CLI shim (route 2); the external-compactor vs. lazy-fencing
  parity test is a precondition either way. Writers keep embedded
  compactors.
- The `slatedb` CLI is not in the Python wheel (checked 2026-10-09);
  route 2 means packaging the Rust binary in the maintenance image.

## Phase 5.6 — review follow-ups (adversarial review 2026-10-09)

Full findings: `millrace/ADVERSARIAL-REVIEW-2026-10-09.md`. Verdict was
fix-before-review; work proceeds in batches because the files overlap.

- **Correctness batch** (in flight): C1 decoder overflow boundary,
  C2 `flushed/` marker lifecycle + bounded `recover()`,
  C3 replay-horizon guard (`persisted_at` in the `prepared/` envelope).
- **Maintenance batch** (in flight): external GC + debt-monitoring
  service per §Phase 5.5, parity probes.
- **Behavioral majors** (landed 2026-10-09): M4 bounded per-decision
  flush windows (scan capped by the size-lane-derived budget, window
  narrowed to the covered offset prefix, per-team outstanding-entry
  gate); M5 flush-runner throughput measured (1024 ready keys × 4
  partitions: 72 keys/s serial, 286 at the default concurrency 4 on the
  in-memory rig — serial within 2× of the ~110/s the design wants, so
  bound-parallel flush shipped: per-partition in-flight cap, the
  flusher's I/O pool sized to match) and evidence noted in
  docs/kafka-ingestion.md §Deployment; M6 `sched_age` honesty (age/slow
  readiness reads the index: prefix scan + candidate point-reads; one
  stats scan per partition per sweep shared with the gauges); m10
  stats_mode keyed on realized file size. Minors m9–m16 and nits
  n17–n23 folded in as sized.
- **SlateDB metrics bridge** (landed with the majors):
  `DefaultMetricsRecorder` per partition stage; the `/metrics`
  collector polls `snapshot()` at scrape time — SlateDB aggregates
  Rust-side, FFI only on scrape. Whitelisted series only (L0
  count/depth, WAL flush counters, write-stall counters, memtable
  bytes, block-cache accesses — 0.17.0 emits no WAL flush-latency
  histogram; the doc's metrics paragraph names the exact series) and
  **aggregated across the pod's instances** — per-partition label
  series are a cardinality trap. Per-DB compaction debt stays with the
  maintenance service's state-view reads (fleet-wide coverage, no
  per-writer plumbing).
- **Queued majors** (after these — same files): M7 earliest-replay
  boundary documented, consider `flushed/`-marker suppression; M8
  mutmut into the millrace CI job with a survivors check against the
  ledger.

## Phase 6 — benches (the design's validation items, closed out)

1. **Compaction throughput** at the modeled arrival rate on a
   production-shaped fixture (~10⁷ file rows, skewed partitions) —
   owned server-side; this plan's only cross-component item, sequenced
   first because the design stands on it.
2. SlateDB rates (phase 2's bench, promoted to a repeatable target).
3. Commit chunking + hydrator keep-up under ~10⁵-key sweeps.
4. Planner replay (phase 1) re-run against the *measured* phase-5
   distributions, closing the model/measurement loop.

## Phase 7 — shadow and cutover

- Run millrace beside millpond against the same topic into a separate
  table; **daily reconciliation** of per-tenant row counts and checksums
  — the deterministic offset ranges in every commit message (millpond's
  rule) are what make "is offset X in the lake" answerable.
- Cutover criteria: 14 days zero divergence; p99 freshness inside SLA;
  compaction debt flat; orphan count zero.
- Rollback is millpond, untouched, until the criteria hold.

## What is deliberately not here

No multi-writer/shared SlateDB topology (single-writer engine
constraint); no Rust or C (orchestration isn't the bottleneck — see the
design doc); no Arrow-native wire format (producers can't pre-batch);
no server changes except the phase-6 compaction bench.
