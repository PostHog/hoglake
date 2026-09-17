# Hoglake — Postgres-Native Lakehouse Catalog Control Plane

Agent guidance for this repo (github.com/PostHog/hoglake — the
standalone hoglake monorepo; the codebase began life as a subtree of
the PostHog DuckLake fork, now PostHog/ducklake, whose history holds
the pre-split commits).

## Pre-push checklist

**Never push broken code.** Before every commit and push:

```bash
cd server && flox activate -- ./gradlew :test         # server suite via the wrapper (Docker required)
just lint-all        # ktlint + ruff (check & format) across both Python trees, mypy on pyhoglake
just pyhoglake test  # pyhoglake suite
just webui test      # vitest (no server needed)
just hedgerow test   # unit; integration needs a live server
```

`ruff` is **pinned** in both Python trees' dev groups and run through
`uv run` — never `uvx ruff`, which resolves a different rule set per
machine and silently ignores the projects' `per-file-ignores`.

The server builds through the **checked-in Gradle wrapper**
(`server/gradlew`, pinned in
`server/gradle/wrapper/gradle-wrapper.properties`) — never a
system-installed gradle. The `just server ...` recipes route through it
too, so `just test-all` (server + pyhoglake) stays equivalent.

The DuckDB extension is built and tested separately (it is not in
`just`): `cd duckdb-client && ./test/run-live-tests.sh`, which needs
the dev stack and a pyhoglake checkout for its fixtures. Its
sqllogictests SKIP without `HOGLAKE_URL`, so a green `make test`
without a server up verifies nothing — see
[duckdb-client/README.md](duckdb-client/README.md) for the build flags
(omitting `BUILD_EXTENSION_TEST_DEPS=full` makes vcpkg delete
curl/openssl/zlib from the build tree).

For the full end-to-end pass (client/hedgerow integration tests against
a real server): `just server compose-up && just server run` in another
terminal first — integration tests skip cleanly when no server is up,
so a green run without one is NOT a full verification. Say which you
ran. The server needs the compose MinIO's credentials in its env
(`HOGLAKE_S3_ENDPOINT`/`HOGLAKE_S3_ACCESS_KEY`/`HOGLAKE_S3_SECRET_KEY`
— see server/README.md §Dev environment) or the hydrator/cleanup/
compaction paths fail the SDK credentials chain. (`just up` / `just
down` instead brings up/tears down the whole stack in containers —
server + webui images built from the working tree, webui on :5173 —
with the S3 env wired by the compose file.)

The server suite includes the **schema equivalence gate**
(`just server schema-check`): fold(migrations) must equal `schema.sql`.
If you touch a migration, update `schema.sql` in the same change or
this fails. It also includes the **mapper-coverage gate**
(`MapperCoverageGateIntegrationTest`): every hog_* table's live columns
must equal the set declared in `persistence/HogSchemaColumns.kt` — a
new column means updating the row mapper(s) named there AND the
declaration, or the gate fails naming the table and column.

Prefer fixup commits over amending and force-pushing.

## What this is

The catalog is a **service, not a client library** (see
[README.md](README.md) — the design doc — and the defect ledger for
why). Clients never see the backing Postgres. Writers write parquet to
object storage themselves and register it via footer-shipping commits;
readers plan from metadata. Kotlin/Ktor/JDBI server, Python client,
React console, Python replication daemon:

| Component | What | Stack | Tests |
|---|---|---|---|
| `server/` | The control plane: DDL, commits (OCC + admission backpressure), scans, changefeed, offsets, retention/expiry/cleanup, hydrator, compaction, verify, metrics, audit | Kotlin 2.2 / JDK 21 (flox) / Ktor / JDBI / Flyway / parquet-java (footer reads + compaction writes) | JUnit5 + Testcontainers (PG16, MinIO) + kotest-property |
| `pyhoglake/` | Thin API client; owns the Python writer path (parquet with field IDs, footer stats, Iceberg bounds codec) | Python 3.12 (flox) / uv / httpx / pyarrow | pytest + pytest-httpx + hypothesis |
| `webui/` | Lakekeeper-style management console: catalog browser (namespaces/tables/files/scan with time travel), newest-first snapshot timeline (`before` paging), consumers (grouped, names resolved, dropped badges), compaction-debt page, maintenance pages (central catalog×task matrix + per-catalog task panels over the run ledger), `/metrics` visualizer, instance-name badge; int64 wire fields carried as strings (lossless above 2^53) | Vite / React / TS | vitest (mocked fetch) |
| `hedgerow/` | viaduck's successor: source table → destination table replication, append-only, single-destination | Python / uv / pyhoglake | pytest; scripted-fake unit + live integration |
| `duckdb-client/` | DuckDB extension: ATTACH over REST, scan (partition pruning + deletion vectors), INSERT/UPDATE/DELETE via footer-shipping commits, DDL, time travel, metadata/maintenance functions | C++ / DuckDB (pinned) / cpp-httplib + yyjson (both duckdb-vendored) / roaring via vcpkg | sqllogictests against the live dev stack + cross-client wire vectors |

The REST contract is `server/src/main/resources/openapi/hoglake.yaml`
— it is the single source of truth for wire shapes; server routes,
pyhoglake, webui fixtures, and hedgerow all conform to it. Change the
spec and the implementations together.

## CI/CD and deploys (Gigahog)

The production deployment of hoglake is **Gigahog** (deploy targets
`gigahog-server` / `gigahog-webui` in PostHog/charts). CI is
path-scoped per component, posthog-monorepo style:

| Workflow | Paths | Runs |
|---|---|---|
| `server.yml` | `server/**`, codec vectors | test + ktlint (Docker/Testcontainers; schema-equivalence gate included), PR image boot-smoke, the gated `deploy` job, and `trino-test` — the integration harness against the fork's public image (`ghcr.io/posthog/trino`, newest ordered release tag; pin via the `HOGLAKE_TRINO_IMAGE` repo variable). trino-test runs on an ARM runner (the image is arm64-only), is NOT in deploy's `needs`, and must stay non-required: a broken fork build must never wedge hoglake CD |
| `webui.yml` | `webui/**`, OpenAPI spec | `npm run build` (tsc gate) + vitest + PR image boot-smoke + gated `deploy` job |
| `ci-python.yml` | `pyhoglake/**` `hedgerow/**` `bench/**` | pyhoglake: `pyhoglake-checks.yml` (ruff, mypy, pytest on 3.11–3.13, build, wheel smoke test). hedgerow and bench: uv sync, ruff (pinned; bench exempt until its format backlog lands), pytest. All unit/mocked layer — live integration is local, per the pre-push checklist |
| `publish-pyhoglake.yml` | `pyhoglake-v*` tags; PRs touching the workflow | `pyhoglake-checks.yml`; tags also publish to PyPI (trusted publishing, `pypi` environment) and create a non-latest GitHub release |
| `fuzz.yml` | nightly cron + dispatch | `./gradlew fuzz` over every Jazzer target (600s each by default), with the generated corpus accumulated across nights through the actions cache. Not a PR check: PR CI only REPLAYS the committed seed corpus, inside `:test` |
| `semgrep.yml` | all | python / kotlin+java / general packs, pinned container |
| `dependency-review.yml` | PRs | vulnerability gate (license allow-list deferred until the three-ecosystem atom set settles) |

CD is the charts state-file mechanism (same as duckgres, millpond,
viaduck): a push to main touching `server/**` or `webui/**` runs the
`deploy` job of `server.yml` / `webui.yml`, which builds a
**multi-arch** image (build stages are pinned to `$BUILDPLATFORM`; the
mw fleet runs Graviton), push `ghcr.io/posthog/hoglake-server|-webui`,
and dispatch `commit_state_update` with the multi-arch MANIFEST digest
(never a per-arch digest) to `state/gigahog-server.yaml` /
`state/gigahog-webui.yaml`. Dev auto-promotes; prod goes through the
charts `promote-to-prod.yml` workflow behind the
`prod-promote-managed-warehouse` approval gate. Unlike millpond and
duckgres (where CD runs in parallel with CI), the deploy job is GATED:
it `needs` the test jobs, so a red main push never builds, publishes,
or dispatches (decided 2026-09-11). A flaky-failure rerun that goes
green deploys on the rerun — the procedure in
millpond's AGENT.md applies verbatim with `app=gigahog-server` /
`app=gigahog-webui`.

Bootstrap state (until all are done, CD dispatches fail or no-op):

- [ ] repo secrets `GH_APP_CHARTS_DEPLOYER_APP_ID` /
      `GH_APP_CHARTS_DEPLOYER_PRIVATE_KEY` exposed to this repo
- [ ] charts side: golden-chart apps + `state/gigahog-*.yaml` seeded
      AFTER the first image push (the seed digest must postdate the
      code — docs/claude/new-app-image-releases.md in charts)
- [ ] first images published (needs the repo visible to GHCR consumers)
- [ ] Dependency Graph (+ GHAS for internal repos) enabled — the
      dependency-review workflow errors on every PR without it

### Versioning

Images deploy per-commit by SHA+digest (above); semver is release-only.
Between releases, main carries the NEXT patch version with a dev
suffix — `X.Y.Z-dev` in gradle/npm/the OpenAPI spec, `X.Y.Z.dev0`
(PEP 440) in the Python trees — so an unreleased build never claims a
released number. Cutting a release = one PR that strips the suffix
(bumping minor/major if warranted) across all six version strings
(server + trino build.gradle.kts, webui package.json, pyhoglake +
hedgerow pyproject.toml, spec `info.version`), tag it (`vX.Y.Z`;
`pyhoglake-vX.Y.Z` additionally publishes to PyPI), then a follow-up
commit restores the next `-dev`. The `:checkOpenapiVersion` gradle task
(in CI) keeps the spec's `info.version` locked to the server version;
the pyhoglake publish workflow refuses a tag that mismatches its
pyproject. The Python packages' `__version__` attributes read the
installed metadata, so they are not version strings to bump.

Because that version is constant between releases, it cannot tell two
deploys apart. The image build therefore also carries a BUILD STAMP: the
CD job passes `HOGLAKE_BUILD_STAMP` (a UTC `date -u +%Y%m%dT%H%MZ`) as a
Docker build arg, the Dockerfile forwards it to gradle as `-PbuildStamp`,
`generateVersionResource` writes it beside the version, and GET /v1/info
reports it as `build` for the webui badge. It is SUPPLIED, never
generated by gradle: an ordinary local build passes nothing and reports
no stamp, so a stamp in the webui always means "this came off the
pipeline". Keep it out of `project.version` — that is the contract
version `:checkOpenapiVersion` gates against, and a per-build suffix
there would break that gate on every build.

## Invariants (violating any of these is a bug, full stop)

1. **Snapshot ids are dense per catalog** and ordered with commit order
   (assigned inside the per-catalog advisory-lock commit tail:
   `pg_advisory_xact_lock((4740871::bigint << 32) | (catalog_id & 4294967295))`
   — every DDL/commit tail uses `persistence/Locks.kt`; the key must be
   computed identically everywhere or serialization silently breaks).
2. **Row-id ranges** are assigned server-side at commit, contiguous per
   file, tile `[0, total)` per table, never reused — the lineage
   guarantee. `table_uuid` changes on drop+recreate; consumers key on
   it and must SEE incarnation changes (hedgerow halts on them).
   Compaction preserves ids by materializing them: outputs carry an
   explicit physical `_hog_row_id` int64 column under the **reserved
   parquet field id 2147483646** (never allocatable to a real column),
   flagged by `hog_data_file.explicit_row_ids` — when set,
   `row_id_start` is only min(input ids), not positional. Sorting on
   rewrite is safe *only* because of this; positional reassignment of
   merged rows is the predecessor's rowid-remap bug and must never
   return. The flag — never a file's own field ids — decides which
   source a reader uses, and a reader must refuse a file that
   disagrees with its registration in EITHER direction, because the
   server cannot detect the disagreement (registration never opens the
   parquet; `/verify` is metadata-only). Both refusals are implemented
   and tested in `duckdb-client/`; the server does not yet enforce the
   reserved `_hog` prefix that protects the carrier (finding 9 in
   duckdb-client/DESIGN.md).
3. **One live deletion vector per data file** (unique partial index);
   supersessions only grow (`delete_count` monotone); a DV newer than
   your `read_snapshot` is a 409, never a lost update.
4. **Physical deletion is never authorized by the queue**: cleanup
   liveness-checks every path against live references at drain time;
   `still_referenced > 0` is an invariant violation, alerted, not
   deleted. Draining soft-deletes: settled `hog_file_removal` rows keep
   `drained_at`/`drained_outcome` (the queryable forensics ledger,
   purged past `HOGLAKE_REMOVAL_LEDGER_RETENTION_SECONDS`, default 30d);
   undrained rows accumulate `attempts`/`last_attempt_at`. The ledger
   is also commit-integrated both ways: commits 409 any registered path
   with an undrained row (path-reuse guard), drain sub-batches run
   under the per-catalog commit lock, and compaction pre-registers its
   output path as a `compaction_staging` claim settled
   `'registered'` on group commit.
5. **Expiry never passes head or (when `consumer_floor`) the min
   consumer offset**, and names the pinning consumer. Ranges below
   `earliest_snapshot_id` are 410 Gone — consumers reconcile, never
   silently skip; the floor advance captures
   `hog_catalog.earliest_snapshot_time` so 410s can say WHEN the floor
   was reached. A fifth sweep step deletes versioned DDL rows
   (`hog_table_version`/`hog_column`/`hog_partition_spec`/`hog_sort_spec`/
   `hog_view`) whose `end_snapshot <= earliest_snapshot_id` — invisible
   at every retained snapshot, so DDL churn cannot grow them unbounded.
6. **Versioned-row visibility**: a row is visible at S iff
   `begin_snapshot <= S AND (end_snapshot IS NULL OR S < end_snapshot)`.
   Every read path uses exactly this predicate.
7. **TableInfo aggregates come from files visible at the requested
   snapshot**, never from `hog_table_stats` (that row is the gross
   append counter / row-id allocator anchor — head-scoped by nature).
8. **Audit/observability never rides a transaction** and never writes
   to any database. Audit emits after commit/rollback; metrics are
   passive. Maintenance run history and asynchronous summaries are
   durable operational state; run-history writes are best-effort after
   task transactions resolve. The removal ledger remains transactionally
   integrated with commits and cleanup, as invariant 4 requires.
9. **All SQL is parameterized.** No string-built values, anywhere.
10. **Rows-then-offset everywhere** (server offset API is monotonic;
    hedgerow commits offsets only after destination durability).

## Working conventions

- **Dependency versions are looked up, never recalled.** When adding or
  pinning a dependency, verify the latest stable version against its
  registry (Maven Central, npm, PyPI) at that moment — a version
  written from memory is stale by your knowledge horizon, silently
  (the original dependency set arrived up to 17 months old this way,
  and security scanning never notices staleness without a CVE).
  Dependabot version updates (.github/dependabot.yml) backstop this
  weekly; grouped minor/patch PRs get a normal review, majors get
  their own.
- **Migrations**: plain SQL in `server/src/main/resources/db/migration/`.
  **The chain is append-only as of v1.0.0** (2026-09-11, the Gigahog
  deploy): `V1__init.sql` is FROZEN — never edit it; schema changes are
  new `V<n>__` migrations, and `schema.sql` must equal the fold of the
  whole chain (the equivalence test enforces it). FKs with CASCADE,
  partial indexes for hot predicates, CHECK-constrained vocabularies
  (deliberate choice over PG enums while the vocabulary churns). No
  migration ledger hacks — Flyway owns it.
- **Change kinds** (`hog_snapshot_change.kind`) are the typed OCC
  vocabulary; adding one = migration + schema.sql + `ChangeKind` enum +
  conflict-rule review in `CommitService`.
- **Errors**: services throw `HoglakeException.*`; the API maps them
  (404/409/410/422; commit admission timeout 503 + Retry-After; parse
  failures 400). New failure modes get a typed exception, not a status
  code sprinkled in a route.
- **Background loops are coroutines**: every periodic job (hydrator,
  expiry, cleanup, compaction, metrics sampler) registers with
  `BackgroundLoops` (one supervisor scope owned by
  `App.startBackground()`) — never a raw daemon thread. Contracts:
  `intervalMs <= 0` = disabled; a failed iteration is logged + counted
  (`hoglake_background_loop_failures_total{loop}`) and the loop (and
  its siblings) keeps running; shutdown is structured and bounded
  (cancel + join, 5s hard cap). Tests drive the services'
  `runOnce`/`sampleOnce` entry points directly, not the scheduler.
  Every maintenance-task run — loop sweep or manual `/maintenance/`
  trigger — is recorded in `hog_maintenance_run` via
  `MaintenanceRunStore.recorded` (the per-catalog `runOnce` is the one
  funnel; the hydrator's instance-wide sweep fans out one row per
  claimed catalog). Recording is post-run and best-effort (never fails
  the task); the cleanup sweep purges rows past
  `HOGLAKE_MAINTENANCE_LEDGER_RETENTION_SECONDS` (7d default). Read
  side: per-catalog `GET .../maintenance/status` + `/runs` and the
  instance-wide `GET /v1/maintenance/status` + `/runs` twins
  (MaintenanceStatusService; batched reads independent of catalog count).
  Dashboard and partition-debt requests read persisted asynchronous
  summaries, NEVER the manifest. `MaintenanceSummarySampler` checkpoints
  keyset pages in `(catalog, table, row_id_start, file_id)` order, carrying
  tier quotas across pages. Default row budget 10,000/tick, interval 1s,
  refresh delay 60s after a completed scan (`HOGLAKE_MAINTENANCE_SUMMARY_*`).
  Incomplete generations are never published; old samples remain visible
  with freshness timestamps. Expiry overtaking a scan restarts it. The
  central status endpoint pages catalogs (50 default, max 100).
- **Multi-agent work**: partition by package/file ownership; frozen
  shared files (build files, Model.kt, migrations, spec) change only
  through the integrating session; agents report needed changes rather
  than making them. Concurrent gradle runs contend on the build dir —
  EOFException in `:test` results is contention, rerun.
- **Every feature and its tests consider ALL consumers.** The wire
  contract has more implementations than the server suite runs, and the
  ones that drift are the ones nothing reds. A type-system,
  wire, or file-format change states its effect on each consumer —
  implemented, refused with a named error, or an issue filed — and never
  leaves one silent.
  - **pyhoglake** is the reference client. It shares the Iceberg
    single-value codec through
    `pyhoglake/tests/vectors/bounds_vectors.json` (107 vectors, count
    pinned on both sides — `qe_vectors.test_vector_file_header_contract`
    and `BoundsVectorFile.EXPECTED_COUNT` — so a silently shrunken file
    cannot pass), and it mirrors the server's type mapping and
    validation gates, which therefore move with the server. A parity
    test must PARSE the other side's artifact or share a fixture:
    `test_transforms.py` regexes `ColType` out of `Model.kt` and
    `BUCKETABLE_TYPES` out of `AlterService.kt`, and
    `ScalarTypeParityTest` reads the migration, `schema.sql`, and the
    OpenAPI enum off disk, for exactly this reason. A test that restates
    the constant it claims to mirror asserts only that the file
    compiles, and one that reconstructs its own expectation cannot fail
    at all — both shapes are in the tree today.
  - **duckdb-client** is NOT in server CI, so nothing reds when the
    server outgrows it: the ten scalar types of #66 shipped with the
    extension unable to read them (`Unknown hoglake column type` at
    bind — a clean refusal, tracked separately, but nobody chose it),
    and the same is true of `variant` (#77). Its
    [DESIGN.md](duckdb-client/DESIGN.md) carries invariants the server
    has contradicted when nobody looked — the reserved row-id field id,
    the two `explicit_row_ids` disagreement refusals — as well as the
    numbered findings listed under Known deferrals.
  - **The Trino connector** lives in the PostHog/trino fork; the in-repo
    harness (`server/trino/`) resolves the newest fork image at run time
    and is the cross-repo drift alarm. A server feature that changes
    what files or scans look like adds a harness case where feasible —
    "to the extent possible" is the standard, since the connector code
    is not here. Harness assertions must never pin fork prose;
    discriminate on object paths hoglake registered and on values only a
    correct implementation can know. Replay every new predicate offline
    against inputs it MUST reject before believing a green container
    run: five successive generations of one fail-open matcher were each
    invisible in a green suite and visible in seconds of replay (#75).
  - **hedgerow** and the **webui** consume the same wire and restate the
    same vocabularies, and neither is exercised by the server suite
    either — `webui/src/api/types.ts` still has no `variant`.
- **Look the invariant up before you write it down.** Before
  implementing a rule about row ids, field ids, or column binding, READ
  the ones already stated: this file, the
  [duckdb-client design doc](duckdb-client/DESIGN.md),
  [iceberg-federation.md](iceberg-federation.md), and the comments in
  `server/schema.sql`. Follow the document over an instruction or an
  intuition, and say that you are doing so. Two regressions shipped
  because a rule was invented while the correct one was already on disk.
- **API changes get the fuzzing treatment.** The Jazzer targets under
  `server/src/test/kotlin/com/posthog/hoglake/fuzz/` have reached
  defects nothing else did, so they are part of the change, not a
  follow-up (see [fuzzing.md](fuzzing.md)). New or changed wire surface
  — DTOs, OpenAPI shapes, validation — extends the wire corpus; new
  parse or validation logic gets a target or joins an existing one.
  Cross-surface machinery (a reader/rewriter pair, a codec's
  encode/decode) gets an agreement- or round-trip-style fuzzer whose
  oracle checks GROUND TRUTH, never mutual agreement: an agreement
  oracle went blind to a real bug for more than a million executions
  because unifying the two surfaces had correlated their errors. Every
  campaign finding becomes a deterministic regression test in `:test` —
  a corpus seed is NOT regression cover, because `fuzz` and `:test` are
  different Gradle tasks and PR CI replays only `:test`. Verify the red
  before the green by reading the `<testcase name=...>` entries in
  `server/build/test-results/test/TEST-*.xml`, never an exit code: a
  `--tests` filter that matches nothing FAILS the build, and that
  failure has been misread as a failing assertion. And `ColType`
  ordinals are baked into the corpora — the decode target reads its
  first byte as a type index modulo the vocabulary size — so the enum is
  append-only, and any membership change means rerunning
  `./gradlew generateFuzzSeeds` and recommitting the seeds — otherwise
  the seeds silently start exercising different types than their names
  claim.
- **QE culture**: substantive changes get an adversarial review or QE
  agent pass before merge; bugs found by tests/fuzzing become pinned
  regression tests + (design-class ones) defect-ledger entries.

## Known deferrals / open items

- **Compaction (M4) — 100% implemented**: `server/compaction/` —
  planning is metadata-only (live, same spec + partition values + size
  tier; adjacency NOT required). `HOGLAKE_COMPACTION_TIER_TARGET` (T=8,
  minimum 2) sets both geometric tier spacing and max fan-in. Divide
  the final target downward by T (ceiling-rounded integer bytes), consume
  minimal row-id-ordered prefixes reaching each tier's quota, and repeat
  until the remainder is short. A table's plan is fixed before execution:
  outputs are never re-compacted within that run. Input bytes estimate
  promotion; actual output size determines the next-run tier. The existing
  max-groups-per-run budget still caps executed attempts. Rewrite via **parquet-java**
  (the project's one parquet library — decision 2026-09-05: Hardwood is
  out of main code entirely (the trino test fixtures still use
  hardwood-core to produce id-less parquet — deliberately); parquet-java handles footer reads in the hydrator AND
  the compaction writer), commit under the catalog lock with input
  re-verification. **DV-bearing files compact — LANDED**: the rewrite
  APPLIES each input's live DV (puffin `deletion-vector-v1`, read via
  `PuffinDeletionVector`) — survivors keep their ids in `_hog_row_id`,
  deleted ids are gone forever, the DV rows end-snapshot with their
  files, and the commit re-verifies the exact planned DV identity (a
  vector that grew/appeared since planning skips the group:
  `dv_superseded` — a post-plan delete is never dropped).
  **Heterogeneous-schema groups — LANDED**: inputs map to the LIVE
  schema by field id (missing columns null-fill, int→long/float→double
  up-cast, unsigned int32→long ZERO-extension, dropped field ids drop
  their data); only a live column unproducible from an input's physical
  type skips the group (`unconvertible_schema`). The rewriter applies
  the same unsigned-annotation domain rule as the hydrator's footer
  decode (`ColType.maxUnsignedParquetWidth`) so compaction cannot
  launder an annotation the hydrator refuses. **Aborted-upload orphans — LANDED**: the
  output path pre-registers as an undrained `hog_file_removal` row
  (reason `compaction_staging`) before upload; a successful group
  commit settles it (`drained_outcome='registered'`) in the same
  transaction, an aborted group leaves it for the normal cleanup drain
  to reclaim, and the commit re-claims the ticket first so a drain that
  won the race just aborts the group. Still deliberate: the background
  loop defaults OFF (`HOGLAKE_COMPACTION_INTERVAL_MS=0`) — flipping it
  on is an ops decision, not a code gap. Remaining rewrite deferrals
  (all surface as `unconvertible_schema` skips, never wrong bytes):
  nested schemas, INT96, decimal-scale changes, and non-native
  time(stamp) units — each timestamp type accepts only the unit its own
  files carry (millis for `timestamp_s`/`timestamp_ms`, micros for
  `timestamp`/`timestamptz`, nanos for `timestamp_ns`), and `time`
  stays micros-only. No unit CONVERSION exists, because no legal
  promotion produces a unit mismatch: `PROMOTIONS` follows DuckLake's
  documented table, which has no timestamp rungs.
- **Field ids are a contract**: the hydrator's footer read flags files
  whose parquet schema has any leaf without `PARQUET:field_id`
  (`hog_data_file.missing_field_ids`; gauge
  `hoglake_missing_field_id_files{catalog}`; the reserved `_hog_row_id`
  id 2147483646 is fine). While a flagged file — or a still-`pending`
  file, whose id state is unknown until the footer is read — is LIVE,
  `rename_column` is refused with 409 `idless_files_present` (id-less
  files bind columns by name; renaming would silently NULL their
  history in readers). `rename_table` is unaffected. Files registered
  with inline stats never pass through the hydrator, so only
  deferred-stats files get checked — still a known gap: the verify
  endpoint is metadata-only by design and can't do the S3 footer reads
  this check needs.
- **Maintenance verify — LANDED**: `POST
  /v1/catalogs/{c}/maintenance/verify` (schema-gaps review item B3, absorbing B4) — the
  QE suite's global-invariant SQL as a metadata-only, read-only
  endpoint (REPEATABLE READ MVCC snapshot, no catalog lock): row-id
  tiling (explicit_row_ids-aware), DV uniqueness/monotonicity/bounds,
  orphaned live rows on dropped tables, still-referenced removal-queue
  entries, true snapshot density, next_row_id consistency.
- **DuckDB client (`duckdb-client/`)**: complete through time travel
  and maintenance functions, verified against the live dev stack, but
  NOT yet in CI and not yet released — no path-scoped workflow, and its
  `duckdb` / `extension-ci-tools` trees are gitignored pinned clones
  rather than submodules (the repo root was out of the branch's write
  scope; they become submodules on extraction to a standalone
  community-extension repo). Its DESIGN.md ends with 11 numbered
  **findings for the server** — wire gaps (no namespace drop, no
  `explicit_row_ids` on `FileRegistration`, no snapshot id on
  create/alter responses, head-only listings, no timestamp→snapshot
  resolution) that each cap a parity item. No server changes were made
  for them.
- **CDC publications to Kafka (the WAL tap)**: fully specified in the
  OpenAPI (501s) + README; not implemented.
- **DR/export**: `GET /v1/catalogs/{c}/export` specified in the OpenAPI
  (snapshot range + live-file manifest + consumer offsets, consistent
  at head); 501 stub until built (schema-gaps review item B5).
- **Iceberg REST facade + Trino**: design obligations in
  [iceberg-federation.md](iceberg-federation.md) /
  [trino-integration.md](trino-integration.md); v1 schema already
  conforms (typed bounds, Iceberg transforms, DV-only deletes).
- **Auth**: out of scope for v1; audit actor is `anonymous` until it
  lands. Decision space in README §AuthN/Z.
- pyhoglake implements `truncate[W]` partition transforms; the server's
  Transform vocabulary doesn't include truncate yet (client-ready,
  server gap).
- No catalog delete API (QE/integration runs leave disposable
  `qe-*`/`pyhog-*`/`hedgerow-*` catalogs behind on dev stacks).
- `bench/` is outside `just lint-all`: it passes `ruff check` but 12
  files fail `ruff format --check`. Format it and wire it in (pin ruff
  in its dev group, add the `lint` recipe) as its own change, so the
  reformat lands as a reviewable diff rather than noise inside another.

## Doc index

**Design and decisions** — [README.md](README.md) (the design doc) ·
[metadata-schema.md](metadata-schema.md) (the schema, table by table) ·
[iceberg-federation.md](iceberg-federation.md) /
[trino-integration.md](trino-integration.md) (engine surfaces) ·
[split-validation-commit.md](split-validation-commit.md) (the commit
tail's escape hatch — designed, not scheduled) ·
[duckdb-read-extension.md](duckdb-read-extension.md) (DuckDB back as a
client, sketch).

**Assessments** — [operational-notes.md](operational-notes.md) (what
changes, and what honestly doesn't, at 2PB/1T) ·
[sql-suggestions.md](sql-suggestions.md) (schema review) ·
[suggestions.md](suggestions.md) (language/stack retrospective) ·
[paimon-compare.md](paimon-compare.md) (the closest comparable).

**Predecessor and process** —
[ducklake-defect-ledger.md](ducklake-defect-ledger.md) (the bugs this
architecture answers) · [ducklake-api-map.md](ducklake-api-map.md) /
[pyducklake-api-map.md](pyducklake-api-map.md) (predecessor surfaces) ·
[fuzzing.md](fuzzing.md) · [source-inventory.md](source-inventory.md).

**Per-component** — [server](server/README.md) ·
[duckdb-client](duckdb-client/README.md)
([design](duckdb-client/DESIGN.md) ·
[parity](duckdb-client/PARITY.md)) ·
[trino connector](server/trino/README.md) ·
[pyhoglake](pyhoglake/README.md) · [webui](webui/README.md) ·
[hedgerow](hedgerow/README.md) · [bench](bench/README.md).

Bug-hunt writeups and in-flight worklists stay local-only (they are
snapshots of a moving tree, not documentation).
