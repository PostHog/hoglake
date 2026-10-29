# The hoglite backend (HOGLAKE_DATABASE=hoglite)

hoglake has two storage backends behind one interface. `HOGLAKE_DATABASE=postgres`
(the default) is the deployment gigahog runs: the catalog lives in one Postgres
database, one row set per catalog, serialised by advisory locks.
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

- **Catalog == tenant.** Creating a catalog provisions the tenant lazily
  (hoglite's `PUT /v1/tenants/{tid}` is idempotent) and bootstraps the schema
  in one transaction whose txid is fixed (`hoglake-bootstrap`), so a replay is
  a read. The registry of catalog names + data_path prefix disjointness lives
  in a dedicated `__hoglake_registry` tenant, written under a guard in the
  same transaction as the name claim.
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
  carries one, else `ch:<catalog>:<fingerprint16>` over the request content
  (hoglake's own commit fingerprint), so a retried commit is hoglite's dedup
  hit with the recorded result, and a same-key/different-body replay is the
  typed 422. DDL operations are *not* idempotent in hoglake's contract, so
  they use fresh txids and let their guards answer duplicates (409/422).
- **Snapshots**: `meta.next_snapshot` is the allocator; commit allocations
  bump it alone, DDL allocations bump `schema_version` with it — the same
  distinction as `CatalogRepo.allocateSnapshot` vs the commit tail.
- **Deletion vectors**: supersession is epoch-ordered per data file
  (`PRIMARY KEY(data_file_id, epoch)`), with the staleness-before-monotonicity
  guard order from the commit path.

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

Retry taxonomy: only 421 (owner moved — repin and retry), 503, and transport
errors retry, inside the budget, with 200ms backoff. 409/422 surface
immediately as hoglake's typed exceptions. When the budget is exhausted the
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
