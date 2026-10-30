# Backend caller-contract gate

Run with a real release engine and Docker:

```sh
HOGLITE_BINARY=/path/to/hoglited ./gradlew :test --tests '*Hoglite*'
```

The gate adds 68 test invocations: 33 shared scenarios × two backends, one schema
equivalence test, and one acknowledged-recovery test.

`HogliteBackendConformanceIntegrationTest` has one parameterized scenario per
contract, each invoked for POSTGRES and HOGLITE. Only fixture construction varies;
assertions do not choose a backend. PostgreSQL uses `PgTestSupport.freshDatabase()`
(the normal migrated Testcontainers harness). Hoglite runs the release binary
against a real object store. No mocked catalog or transaction transport is used.
The engine stdout goes to a file, not an undrained pipe.

Without `HOGLITE_BINARY` or a `hoglited` executable on PATH, 30 PostgreSQL-only
invocations still run. The 33 hoglite arms, three PostgreSQL Parquet/compaction
arms sharing the live fixture, schema equivalence and recovery explicitly abort
with a missing-engine reason (38 invocations). JUnit represents assumption aborts
as skipped in XML; CI rejects them and requires all six live suite reports by
name. These are not silently successful tests.

## Covered guarantees

- Catalog/namespace/table lifecycle and historical identity visibility.
- Serial and concurrent commits allocate a dense total snapshot order; refusals
  and keyed replay consume no snapshot; an acknowledged commit is immediately
  readable at its snapshot. Row lineage is dense and server-assigned.
- Idempotency payload mismatch; unkeyed commits remain distinct after truncate.
- Exact refusal **classes**, not superclass matching: Validation, AlreadyExists,
  NotFound, CommitConflict, DdlSinceReadSnapshot, TableRecreated,
  OffsetRegression, Expired, ObjectPrefixViolation, PartitionLookupInvalidRequest,
  NamespaceNotEmpty, CatalogNotEmpty, TableDropped, ReconciliationRequired.
  In particular truncate/stale-delete is DdlSinceReadSnapshot, not merely any
  CommitConflict. Fresh-read retired-file deletion is Validation.
- Deletion-vector supersession, historical epochs, stale-epoch conflicts, and
  cross-table staged-target refusal/transaction atomicity.
- Retention opt-in, consumer pinning and explicit disabling of the consumer floor;
  expired snapshot reads/writes fail with Expired and live files survive expiry.
- Object-prefix proof, canonical values, deferred stats, option-off behavior,
  immutable layout DDL refusals, replacement refusal, durable and staged DV paths.
- Retired partition specs still resolve live files after expiry; allocator and
  same-alter coalescing semantics; exact lookup tuples, aggregates, stable paging,
  cursor binding, time travel, and invalid requests.
- Atomic prepare/publish/replay/abort, durable target-exists rejection,
  existing-target replacement, and absent-target create-or-replace.
- Views, rename/add-column, guarded alters, sort order lifecycle, retention option
  validation and attribution.
- Real Parquet hydration, provided stats, pending-file exclusion, prefix
  quarantine, compaction proof refusal, row preservation, explicit row IDs,
  historical input visibility, exact snapshot totals and output-prefix layout.
- Separately, `HogliteDurabilityConformanceIntegrationTest` kills the owner after
  acknowledged commits and recovers with an empty local directory. Snapshot
  answers and receipts must survive S3 recovery. This tests acknowledged
  durability, not a controlled unacknowledged-read durability barrier.

### Read results must wait for their own durable basis

The stronger invariant (a read cannot expose a locally applied commit that later
vanishes) is fault-injected at the engine boundary by
`TestReadSnapshotReleasesAtOwnDurableBasis` in the hoglite repository
(`internal/hoglite/reads_test.go`). It gates two
segment PUTs, releases only the first snapshot's segment, and asserts the read
returns that snapshot's data, never the later row. This is an engine-level proof,
not a claim that the acknowledged-recovery smoke test parks durability. The
PostgreSQL arm is explicitly argued from synchronous WAL commit/visibility
ordering, not fault-injected by this shared application harness.

## Explicit named exclusions / asymmetries

These are **not silently skipped shared contracts**:

| Name | Why it is outside this shared suite | Existing responsibility |
| --- | --- | --- |
| `POSTGRES_FLYWAY` | Flyway migrations and PostgreSQL system catalogs have no SQLite equivalent. | `SchemaEquivalenceIntegrationTest`; hoglite has the independent equivalence gate below. |
| `POSTGRES_REINDEX` | ReindexService rebuilds PostgreSQL indexes and uses PostgreSQL locking/maintenance facilities. Hoglite does not implement that operation. | PostgreSQL reindex service tests. |
| `POSTGRES_HEAD_SUMMARY_SAMPLER` | PostgreSQL unpinned head totals are nullable/asynchronous sampled values; hoglite currently supplies exact totals. Treating a missing sample as zero would be wrong. | The shared contract requires exact pinned totals, and checks undated head totals when supplied. Sampling-specific freshness is covered by PostgreSQL maintenance-summary tests. |
| `POSTGRES_CRASH_INJECTION` | PgTestSupport owns a JVM-shared container with no restart hook; killing it would corrupt unrelated tests. PostgreSQL synchronous commit supplies WAL durability. | PostgreSQL WAL/transaction engine; no crash-injection claim is made here. |
| `POSTGRES_READ_DURABILITY_BARRIER` | The shared harness cannot park PostgreSQL WAL fsync. There is no equivalent application-level fault hook. | Synchronous PostgreSQL commit/visibility ordering; argued from the engine contract, not experimentally tested here. |

## Schema evolution gate

`HogliteSchemaEquivalenceIntegrationTest` creates two actual SQLite tenants:

1. `src/test/resources/hoglite/tenant-v0.sql`, an **independent, frozen** historical
   pre-layout schema, then the production `ensureTenantSchema` migration path.
2. Current `HogliteSchema.DDL` directly.

It compares sorted SQLite-owned table/index definitions, including columns,
nullability, defaults, keys/checks and index predicates. Whitespace, SQL comments,
`IF NOT EXISTS`, and object creation order are immaterial. A fresh store replays
all migration probes to check upgrade idempotence. Column order remains strict
(the historical upgrade appends columns, as does canonical DDL).

**Never regenerate the historical baseline when adding a column or index.** Add
it to both canonical DDL and a tenant migration. Otherwise deriving the old side
from the new side would hide the exact missing-upgrade bug this gate prevents.

## Maintaining the contract

A new behavior cannot be inferred automatically from a changed service body.
Whenever adding a guard/invariant to CommitService, ExpiryService or another
backend seam, add a shared scenario here with its exact typed refusal and atomic
postcondition. Do not add only a PostgreSQL test plus an independently maintained
hoglite twin. A change in either backend then has to satisfy the same assertion.

## Defects found by this suite

- **Sort-spec allocator and same-alter coalescing:** PostgreSQL allocates MAX + 1
  across retired specs and deletes a never-visible intermediate spec. Hoglite's
  receipt used live + 1 (returning 1 after retirement when the persisted id was 2)
  and burned ids for intermediate specs. The shared lifecycle contract fails on
  the old hoglite implementation; the port also reports the actual begin snapshot.
- **Staged-delete admission:** ordinary `/commit` accepted `dataFilePath` targets
  on hoglite although PostgreSQL requires the transaction publication path,
  idempotency key, read snapshot, unchanged-tables guard, and UUIDs on every target.
  The existing hoglite prefix test was exercising a request PostgreSQL rejects.
  Its staged scenario now supplies honest transaction metadata; the shared
  admission contract rejects all six missing-guard shapes before any mutation.

## Reproduced drift checks

The targeted shared options, sort-lifecycle and staged-admission run produced six
invocations: all three PostgreSQL cases passed and all three hoglite cases failed
with the options attribution guard removed and the two ports absent. Restoring
only the hoglite counterparts makes those same assertions pass. The options
source was restored byte-for-byte (empty diff against HEAD).

## Deliberately not covered in this pass

This is not a replacement for backend-specific race/scale suites. It does not
fault-inject every transaction guard window, ambiguous/failed commit recovery,
lease fencing, network partitions, cleanup object-deletion retries, upload-claim
settlement, retention-attribution races, scheduler coordination, metrics,
authentication, or HTTP serialization. Those remain separate integration/engine
responsibilities. Large-catalog paging/lock latency and circuit-breaker behavior
are not inferred from these small semantic fixtures. Exact partition lookup's
candidate-budget exhaustion is covered by the service-specific tests rather than
this shared suite. New guards still require new shared scenarios: interfaces and
this finite suite cannot automatically infer an arbitrary future invariant.

The schema negative check removed `files.missing_field_ids` only from the frozen
baseline: the real upgrade/fresh-DDL comparison failed, then passed after restoring
the column. Separately reverting the two production ports after a green run
reproduced exactly two hoglite failures while both PostgreSQL cases stayed green.
