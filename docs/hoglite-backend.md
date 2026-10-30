# The hoglite backend (HOGLAKE_DATABASE=hoglite)

hoglake has two storage backends behind one interface. `HOGLAKE_DATABASE=postgres`
(the default) is what the existing shared deployment runs: the catalog lives in
one Postgres database, one row set per catalog, serialised by advisory locks.
`HOGLAKE_DATABASE=hoglite` is the warehouse deployment: each catalog is a
**hoglite tenant** — a SQLite file the engine replicates — and every catalog
operation is a guarded hoglite transaction or read-only query against it.

The route table is one definition shared by both backends. There are no 501
carve-outs for the hoglite backend: every endpoint hoglake serves works on it
(the only 501s are the ones the Postgres path already serves — publications,
export). This document is the map: what the seam is, how each surface is
implemented, and the few places where "the same" is deliberately a different
computation.

## The seam

`server/.../store/CatalogStore.kt` is the service surface the routes call —
catalogs, namespaces, tables, files, snapshots, changes, offsets, scan plans,
commits, receipts. `PostgresCatalogStore` is a pure delegate over the existing
services (CommitService, CatalogService, ScanService); `HogliteCatalogStore`
implements the same surface over `HogliteClient`.

The route groups the services own directly (table-creations, uploads, alters,
views, maintenance, partition listings) are behind small backend interfaces in
`service/` (`UploadBackend`, `TableCreationBackend`, `AlterBackend`,
`ViewBackend`, `MaintenanceBackends.kt`). The Postgres services implement
them unchanged; the hoglite ports live in `server/.../hoglite/`.

## The mapping

- **Catalog == tenant, incarnation-scoped.** The tenant id is the
  deployment prefix (`HOGLAKE_HOGLITE_TENANT_PREFIX`, default `hoglake-`)
  plus the catalog name plus a per-create random suffix — an INCARNATION id,
  recorded in the registry row. Creating a catalog provisions the tenant
  FIRST (hoglite's `PUT /v1/tenants/{tid}` is idempotent) and bootstraps the
  schema in one fixed-txid transaction, so a replay is a read; the registry
  row (catalog name + data_path prefix disjointness, in a dedicated registry
  tenant) publishes LAST — a crash mid-create leaves an empty orphan tenant,
  never a ghost registry row. Dropping resolves the row's tenant id, fences
  the catalog (one transaction: the emptiness preconditions as guards plus
  `meta.dropping = 1`, which every mutating transaction guards on), then
  releases and deletes THAT tenant id — hoglite routes the release to the
  owning pod, so a drop works through any replica — and deletes the registry
  row conditionally on the incarnation. A drop paused mid-flight and resumed
  after a drop+recreate therefore finishes the old incarnation's (already
  complete) deletion and cannot touch the new catalog.
- **Expiry re-reads its policy in the transaction.** The sweep's planning
  reads (floor, retention, consumer-floor flag) are separate requests; the
  advance transaction guards on all three, so a concurrent sweep, a
  retention patch, or a consumer-floor toggle between plan and publish is a
  typed conflict, and the next sweep re-plans. `commitOffset` guards the
  floor in its transaction the same way.
- **Every write is one hoglite transaction** (`POST /v1/t/{tid}/tx`) carrying
  its OCC guards in the request: `expect_scalar_int`/`expect_affected` per
  statement, evaluated inside the tenant's serial transaction. hoglite answers
  a guard miss with 409 + `guard_index`; `HogliteTx` records, per guarded
  statement, which hoglake exception that index decodes to, so the flat 409
  maps back to hoglake's typed refusals (404 unknown table, 409
  TableRecreated / OffsetRegression / CommitConflict, 422 stale read
  snapshot, 410 expired, …).
- **Reads are `POST /v1/t/{tid}/query`** — read-only SELECTs. One consequence
  worth knowing: the engine scans the SQL *text* for DML words, so query
  strings must not contain words like `delete` even in string literals (the
  commit claims-probe uses numeric kind markers for this reason).
- **Idempotency**: commit txids are `ik:<idempotency-key>` when the request
  carries one; an UNKEYED commit gets a fresh random txid per store call —
  publication idempotency is the explicit key's alone (hoglake's Postgres
  contract), and a content-derived txid would silently replay an identical
  later request (truncate + reload) without applying it. A keyed replay is
  hoglite's dedup hit with the recorded result; a same-key/different-body
  replay is the typed 422. DDL operations are *not* idempotent in hoglake's
  contract, so they use fresh txids and let their guards answer duplicates
  (409/422).
- **Snapshots**: `meta.next_snapshot` is the allocator; commit allocations
  bump it alone, DDL allocations bump `schema_version` with it — the same
  distinction as `CatalogRepo.allocateSnapshot` vs the commit tail.
- **Deletion vectors**: supersession is epoch-ordered per data file
  (`PRIMARY KEY(data_file_id, epoch)`), with the staleness-before-monotonicity
  guard order from the commit path. Supersession ENDS the predecessor epoch
  in the same transaction (Postgres's `ended_dvs`), so exactly one live DV
  per data file is the invariant compaction, expiry, and storage accounting
  rely on — and a DV may only target a LIVE data file (retired after the
  read is the retryable 409; retired before it, the 422), which is what
  keeps a delete racing compaction from being silently lost.

## What is computed differently, deliberately

- **No summary sampler.** Postgres's MaintenanceSummarySampler exists because
  scanning a 10M-row manifest per request is what a shared database cannot
  pay. A hoglite tenant *is* one catalog, so table-GET totals, partition
  stats/listings and maintenance backlogs are computed **exactly** at request
  time. The wire's `sampledAt`/`totalsAsOf` fields go null, which the API
  contract already defines as "no sample".
- **Debt is the planner's arithmetic.** `GET .../stats/partitions` computes
  actionable debt by running the same `CompactionGrouping` pack the compaction
  sweep will run, instead of reading the sampler's simulated pool tallies.
- **Compaction plans simply.** One candidate read per table (live files under
  the target size), grouped by (spec, partition), packed greedily; groups run
  sequentially (one tenant has one writer). The claims stay — cross-replica
  dedup is real — one transaction per claim. The five commit guards are
  CompactionService's verbatim: staging ticket untouched, table alive, live
  sort spec unchanged, all inputs live, DVs not superseded. The empty-rewrite
  retirement (snapshot + change + end inputs, no output row) is kept.
- **No per-catalog advisory locks.** The tenant's single writer is the
  serialisation. `convoyed`/`skippedLocked`/`timeouts` counters exist in the
  results and are always 0.
- **Retirement/expiry/cleanup keep the ordering rules.** DVs are deleted
  before their files in the same transaction (bug hunt #16), and a removal
  row is queued in the same transaction as the manifest row it names — a path
  is queued iff its row is gone.

## Genuinely Postgres-intrinsic

- **ReindexService** — B-tree bloat remediation. A hoglite deployment has no
  such loop; the task reports as absent in maintenance status.
- **`GET /v1/database/health`** — the DTO is pg_stat-shaped. The hoglite port
  answers 200 with the engine's reachability as findings, zeroed Postgres
  instruments, and explicit `blind_spots` entries saying which instruments do
  not exist rather than faking their values.
- **Flyway migrations** — the tenant schema is `HogliteSchema.DDL`, applied
  idempotently at catalog bootstrap.

## Configuration

| Env var | Default | Meaning |
| --- | --- | --- |
| `HOGLAKE_DATABASE` | `postgres` | `postgres` or `hoglite` |
| `HOGLAKE_HOGLITE_URL` | — | single endpoint… |
| `HOGLAKE_HOGLITE_WHALE_URL` / `HOGLAKE_HOGLITE_OWNER_URL_PATTERN` | — | …or whale + owner-pattern routing |
| `HOGLAKE_HOGLITE_CONNECT_TIMEOUT_MS` | 2000 | TCP connect |
| `HOGLAKE_HOGLITE_READ_TIMEOUT_MS` | 5000 | queries and tenant ops |
| `HOGLAKE_HOGLITE_COMMIT_TIMEOUT_MS` | 10000 | transactions |
| `HOGLAKE_HOGLITE_RETRY_BUDGET_MS` | 8000 | total per-call retry envelope |
| `HOGLAKE_HOGLITE_CIRCUIT_FAILURE_THRESHOLD` | 10 | consecutive failures before the pool opens |
| `HOGLAKE_HOGLITE_CIRCUIT_OPEN_MS` | 5000 | open duration before the half-open probe |

Retry taxonomy: 421 (owner moved — repin and retry), 503, 504 and transport
errors retry, inside the budget, with 200ms backoff, and a `/tx` retry
reuses its txid, so it is exactly-once. A commit has three outcomes, and the
engine's `code` (not its status) tells them apart:

- **committed**: 200.
- **not committed**, final: 409 guard failure, 422 `refused`, and the query
  guard refusals (504 `query_timeout`, 413 `result_too_large`, 507
  `out_of_memory`), which rolled back. They surface at once as hoglake's
  typed exceptions.
- **unknown**: 504 `outcome_unknown` (applied, then the owner failed before
  the commit was durable), a 503 not labelled `not_applied`, any other
  unlabelled error, or a transport failure after the request was sent.
  These retry; if the budget runs out after one, the caller gets 503
  `commit_outcome_unknown` with `Retry-After`: the commit may have landed.

When the budget is exhausted on attempts that cannot have applied, the
caller gets 503 `backend_unavailable` with `Retry-After` — the honest
"transient, retry" answer, not a 500.

## Failure modes

- **hoglite down**: `/healthz` fails (the probe is the client's own healthz);
  requests get 503 `backend_unavailable`.
- **Owner moves**: the client's 421 handling repins; callers never see it
  inside the retry budget.
- **A maintenance run fails mid-catalog**: the run ledger records the partial
  result (`store.recorded`, mirroring MaintenanceRunStore); the next sweep
  re-runs from state, not from the failure.
- **Compaction loses its staging ticket**: the output path is re-staged before
  the group aborts, so the object can never outlive a ticket naming it
  (CompactionService's rule, kept).

## Testing

`server/.../hoglite/HogliteTxTest.kt` is the guard-mapping unit suite.
`HogliteBackendIntegrationTest` runs the real engine: it needs a `hoglited`
binary (`HOGLITE_BINARY`, or on `PATH`) plus the MinIO image the rest of the
suite uses, and covers the catalog/namespace/table lifecycle, commit OCC and
idempotent replay, stale-read conflicts, drop+recreate incarnation guards,
offset monotonicity, upload claims, atomic table-creation, alters, views,
options, truncate, partition debt, and the maintenance status surface.

CI builds that binary from a pinned PostHog/hoglite commit with
`ci/build-hoglited.sh` (bit-reproducible; the script refuses any digest
other than the pinned one) and fails `server-test` if any hoglite test
class skips. The pins (`HOGLITE_COMMIT`, `HOGLITE_GO_VERSION`,
`HOGLITED_SHA256`) and the reproduction recipe are on the `server-test`
job in `.github/workflows/server.yml`. To run the live suites locally
with the same bytes:

```sh
git clone git@github.com:PostHog/hoglite.git /tmp/hoglite
git -C /tmp/hoglite checkout <HOGLITE_COMMIT>
HOGLITE_GO_VERSION=1.27.1 ci/build-hoglited.sh /tmp/hoglite <HOGLITE_COMMIT> <HOGLITED_SHA256> /tmp/hoglited
cd server && HOGLITE_BINARY=/tmp/hoglited ./gradlew :test --tests 'com.posthog.hoglake.hoglite.*'
```
