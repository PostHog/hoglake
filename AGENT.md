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

Before the PR opens, **mutation-test every load-bearing predicate**
you added or changed: flip it (a conflict kind in or out, `>` to `>=`,
a `NOT EXISTS` guard removed, `FOR UPDATE` dropped, a lock put back),
run the narrowest class, and the test you wrote for it must red. Three
review rounds on one change found three, four and three uncaught
mutations in a row; each was a rule the diff claimed to enforce and
nothing pinned. A chain/depth test must not seed the intermediate state
that lets depth 1 pass.

`./gradlew :test -PunitOnly` runs without Docker. It excludes the JUnit
tag `integration` (plus `*IntegrationTest*` by name), so every class that
opens a container MUST carry `@Tag("integration")`;
`IntegrationTagGateTest` reads the test sources and reds when one does
not. The earlier `junit.jupiter.tags.exclude` system property was not a
JUnit parameter and filtered nothing (#181), so twenty tagged classes
without the name ran and failed on any machine without Docker.

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
| `server/` | The control plane: DDL, commits (OCC + admission backpressure), scans, changefeed, offsets, retention/expiry/cleanup, hydrator, compaction, verify, metrics, audit | Kotlin 2.4 / JDK 25 (flox) / Ktor / JDBI / Flyway / parquet-java (footer reads + compaction writes) | JUnit 6 + Testcontainers (PG18, MinIO) + kotest-property |
| `pyhoglake/` | Thin API client; owns the Python writer path (parquet with field IDs, footer stats, Iceberg bounds codec) | Python 3.12 (flox) / uv / httpx / pyarrow | pytest + pytest-httpx + hypothesis |
| `webui/` | Lakekeeper-style management console: catalog browser (namespaces/tables/files/scan with time travel; the namespace listing is NAME, RECORD_COUNT, FILE_COUNT, FILE_SIZE, SNAPSHOTS, EARLIEST_SNAPSHOT, COMMENT — `table_uuid` stays on the wire but is shown on the table page, not as a column), newest-first snapshot timeline (`before` paging), consumers (grouped, names resolved, dropped badges), compaction-debt page, maintenance pages (central catalog×task matrix + per-catalog task panels over the run ledger), `/metrics` visualizer, instance-name badge; int64 wire fields carried as strings (lossless above 2^53) | Vite / React / TS | vitest (mocked fetch) |
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
| `bench-image.yml` | `bench/**` `pyhoglake/**` | builds `bench/Dockerfile` (context = REPO ROOT: bench installs pyhoglake from the sibling tree, so both must be in the context) and smokes it — CLI runs, non-root, no `.venv`, and the installed pyhoglake is this commit's and not PyPI's. A main push then publishes multi-arch `ghcr.io/posthog/hoglake-bench` (sha + `latest`), gated on the smoke. NOT CD: no charts dispatch, no chart references it, it is pulled by hand into `bench/deploy/bench-pod.yaml`. Its build stage is deliberately NOT `$BUILDPLATFORM`-pinned — a virtualenv holds native wheels, so each arch installs its own |
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
   and tested in `duckdb-client/`, and COMPACTION enforces them
   server-side as well — it is the one server surface that does open
   the parquet, so `ParquetRewriter.rowIdCarrier` refuses both
   directions before a rewrite can launder a file that disagrees with
   its registration. The server also enforces the reserved `_hog`
   prefix that protects the carrier now
   (`Identifiers.RESERVED_COLUMN_PREFIX`, at every nesting level), so
   duckdb-client/DESIGN.md finding 9 is closed for names created
   through the DDL; files registered before the reservation, or written
   by a foreign writer, are still what the refusals above are for.
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
  - **Every heavy-lock statement runs inside the `lock_timeout` window.**
    `ALTER TABLE` (ADD COLUMN included — it is ACCESS EXCLUSIVE even
    when metadata-only), `ADD/DROP CONSTRAINT`, `DROP INDEX` and any
    backfill `UPDATE` sit after the V9 save+`SET lock_timeout = '5s'`
    and before the restore; only `CREATE INDEX CONCURRENTLY` sits
    outside it, because that build waits out older transactions by
    design. A migration session has no `lock_timeout` of its own, so an
    unguarded ALTER queues behind one in-flight commit for up to the
    statement timeout and every reader queues behind IT, while every
    other booting pod waits on the Flyway advisory lock. V10 put its
    ADD COLUMN after the restore, V11 and V12 had no guard at all, and
    V14's first draft repeated it. A CHECK over a table that is not
    known-tiny is `NOT VALID` then `VALIDATE CONSTRAINT`.
  - **An index proves itself against the query it serves.** The
    migration test runs the migration FILE (`Database.migrate()` after
    `PgTestSupport.freshDatabaseAt(<previous version>)`, or after
    deleting the history row) against rows inserted BEFORE it, then
    `ANALYZE`s and `EXPLAIN`s the production statement — the repo
    function's own SQL, exposed `internal` — asserting the index name
    appears and `Seq Scan` on the table does not. V14's first index was
    on the wrong column of a recursive join, and its test was green
    because it EXPLAINed a predicate no code path issues. A partial
    predicate is asserted through `pg_index.indpred`, not by counting
    table rows.
- **Change kinds** (`hog_snapshot_change.kind`) are the typed OCC
  vocabulary; adding one = migration + schema.sql + `ChangeKind` enum +
  conflict-rule review in `CommitService`. `object_id` is ONE column
  over three disjoint id spaces — table, namespace, view — and only the
  kind says which, so any read that goes BY object id filters on kind or
  silently counts a namespace's history against a table that shares its
  id. `ChangeKind.TABLE_SCOPED` is that filter, and it is an EXPLICIT
  list, not `startsWith("TABLE_")`: a derived set cannot be checked,
  because any test would have to apply the same rule and agree with
  itself (the first version did, and a hypothetical
  `TABLE_NAMESPACE_MOVED` would have joined the set and passed). Adding
  a kind therefore means classifying it in `TABLE_SCOPED` or
  `NON_TABLE_SCOPED`; `TableSummaryVocabularyTest` asserts the two
  PARTITION schema.sql's vocabulary, so an unclassified kind reds and
  the author has to answer the question rather than inherit an answer.
- **A table's snapshots are defined through the change log**, because
  snapshots are CATALOG-wide. `TableSummary.snapshot_count` is the
  number of distinct `hog_snapshot_change.snapshot_id` values at or
  above the catalog's `earliest_snapshot_id` whose row is
  `TABLE_SCOPED` and names the table; `earliest_snapshot_id` is the
  smallest of them, null when none. Both are RETAINED-only by
  construction, so they shrink as expiry advances the floor — they
  answer "how far back can this table still be read", never "how many
  commits has it taken". One commit can write several change rows for
  one table in one snapshot (truncate writes two, an atomic replacement
  three), so the count is `DISTINCT` on the snapshot id and nothing
  else. The floor bound is DEFENCE rather than arithmetic: today's
  `ExpiryService` advances the floor and deletes the snapshots below it
  in one transaction, and `hog_snapshot_change` cascades, so after a
  sweep there is nothing below the floor left to exclude and the bound
  is provably redundant. It stays because the alternative is a query
  whose correctness depends on that cascade with nothing saying so, and
  it is tested against the state it defends against (a floor moved
  ahead of its reclamation), not against a state the server produces.
- **`GET .../tables` is the one unpaged listing that scales with data.**
  One row per live table, one statement, per-table work index-driven —
  linear in the namespace. Measured on a Portola-shaped namespace (54k
  tables / 270k files / 270k change rows, PG18, warm, serial):
  **389-402 ms, 595,488 shared buffers, 9.16 MiB of JSON**, with the
  final `ORDER BY` spilling 595 temp blocks. The FILE rollup is 73% of
  the buffer traffic and the change-log lateral 27% — so the tempting
  optimisation is the wrong one: the change-log lateral's per-loop Sort
  (`count(DISTINCT)` cannot use `hog_snapshot_change_conflict`'s
  snapshot_id ordering with the kind filter between `object_id` and
  `snapshot_id`) would cost a second index on the table every commit's
  OCC check writes, to save a quarter of a read path's buffers. The
  9 MiB unpaged body is the real limit, and it is a shape problem, not
  a plan problem: PAGING is the follow-up. Numbers re-measured for this
  entry rather than carried over from the review that raised it — the
  review's differed (444 ms / 813k / 5.4 MiB), which is what happens to
  a number nobody re-runs.
- **Compaction is never a conflict, and only a race gets a retryable
  status.** Any guard that compares a writer's read set against
  `hog_snapshot_change` excludes `table_compacted` (both the commit
  path and the guarded replacement publish): compaction changes no
  visible row, and a deletion vector whose target compaction retired is
  caught per file by the liveness check. On a table under continuous
  compaction (one group every ~2.6 min in production) anything that
  treats it as a conflict livelocks every statement longer than one
  interval — and did, for UPDATE/MERGE, from #161 until the post-merge
  review of #156–#162 caught it. A plan-to-commit
  race answers 409; 422 is for a request that was already wrong at its
  own `read_snapshot`, and `end_snapshot == read_snapshot` is the 422
  side of that line. A conflict test drives the real service between
  read and publish (`CompactionService.compactPlannedGroup`,
  `ExpiryService.runOnce`); seeding change rows by SQL asserts the rule
  you wrote, not the system, and passed a livelock.
- **Whatever mints a new identity releases the old one's consumers.**
  Consumer offsets pin expiry on purpose and survive drops on purpose;
  atomic replacement mints a new `table_uuid`, so a consumer's row on
  the retired uuid pinned the whole catalog forever with no delete API.
  Lineage between incarnations is RECORDED (`hog_table.replaced_table_id`,
  written by the replacement path) and walked from that column; it is
  never derived at run time from a convention such as "dropped in the
  snapshot the successor was created in", because a convention nothing
  enforces has a failure mode of deleting a consumer's position on a
  table it is still draining. The commit path walks backward from the
  table just committed (primary-key hops, zero in the normal case); the
  expiry sweep walks forward for every consumer as the catch-all for
  rows committed before the rule existed.
- **Stored payloads outlive the code that wrote them.** JSON the server
  writes and later reads back — commit receipts, table-creation
  definitions, the maintenance ledger — is decoded with the lenient
  stored-payload mapper (`WireJson.storedPayloadObjectMapper`), never
  the strict API mapper, and every new defaulted field on a stored
  model is `@JsonInclude(NON_DEFAULT)`. A rolling deploy has both
  versions replaying each other's rows; a receipt carrying
  `"require_unchanged_tables": false` made the older replica throw
  under the catalog lock and 500. The mixed-fleet case is a test with an
  unknown property at the top level AND nested inside a file entry.
  The API mapper stays strict: an unknown request field is a 400.
- **The per-catalog commit lock is for commits.** Nothing acquires it
  unless it allocates a snapshot or settles state inside a commit
  transaction, and every acquirer passes `commitLockTimeoutMs` — the
  default argument is an unbounded wait. Upload claims took it once per
  output file with no bound. Row-level state re-checks replace it:
  under READ COMMITTED an `UPDATE ... WHERE state = 'active'` re-evaluates
  its predicate after waiting on the row lock, so a settle that landed
  first wins without any global lock; the predicate that selects
  candidates is the predicate that fences them, verbatim, or a renewal
  in the window is clobbered. Sweeps settle one row per transaction so a
  commit never queues behind a sweep while holding the commit lock.
- **A new guard never edits an existing test out of its way.** When a
  new refusal reds a test, that test either asserts the refusal or has
  its fixture changed to satisfy the guard legitimately, with the reason
  in the test. Swapping `add_column` for `set_sort_order` in two tests to
  keep them green (#159) hid that the guard fires on any table with
  hydration in flight.
- **PR-body claims are not evidence.** Test counts, "adversarial review
  found no blocking issues" and "verified against Trino" are read as
  claims to verify from the code and the tests; a review that trusts
  them reviews nothing. Eight PRs each carried the clean-review line;
  three of them held the livelock, the 500 and the expiry pin above.
- **Errors**: services throw `HoglakeException.*`; the API maps them
  (404/409/410/422; commit admission timeout 503 + Retry-After; parse
  failures 400). New failure modes get a typed exception, not a status
  code sprinkled in a route.
- **Background loops are coroutines**: every periodic job (hydrator,
  expiry, cleanup, compaction, verify, metrics sampler) registers with
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
  keyset pages in `(catalog, table, file_size_bytes, file_id)` order —
  the order compaction bin-packs in (V10 indexes it) — carrying each
  bucket's partial group across pages in `pending_max_bytes`. Default row budget 10,000/tick, interval 1s,
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
    `pyhoglake/tests/vectors/bounds_vectors.json` (109 vectors, count
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
  - Worked example, `TableSummary` growing five fields + `comment`
    (#189): pyhoglake and the webui implemented them; the **DuckDB
    extension**'s `HoglakeApiClient::ListTables`
    (duckdb-client/src/rest/hoglake_api_client.cpp:552) reads only
    `name` and `table_uuid` off each array member and ignores the rest,
    so an additive change is safe there and it was left alone;
    **hedgerow** never calls the endpoint at all. Saying
    which of the four it is — implemented, safe-by-construction, or not
    a consumer — is the point; "additive, so fine" without naming them
    is the shape that lets one drift.
- **Look the invariant up before you write it down.** Before
  implementing a rule about row ids, field ids, or column binding, READ
  the ones already stated: this file, the
  [duckdb-client design doc](duckdb-client/DESIGN.md),
  [iceberg-federation.md](docs/iceberg-federation.md), and the comments in
  `server/schema.sql`. Follow the document over an instruction or an
  intuition, and say that you are doing so. Two regressions shipped
  because a rule was invented while the correct one was already on disk.
- **API changes get the fuzzing treatment.** The Jazzer targets under
  `server/src/test/kotlin/com/posthog/hoglake/fuzz/` have reached
  defects nothing else did, so they are part of the change, not a
  follow-up (see [fuzzing.md](docs/fuzzing.md)). New or changed wire surface
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
  - A target whose coverage is flat from `INITED` to `DONE` is
    saturated: its budget is a regression sweep, and it belongs in the
    sweep class (`fuzzSweepTargets`). A target under ~1,000 exec/s is
    starved and finds nothing at any budget: profile it with JFR before
    giving it minutes — the nightly ran NestedAgreement at 11 exec/s
    (7k executions a night) for a week and ParquetFooter at 580/s, both
    dominated by Hadoop `Configuration` construction (#165), while five
    saturated targets burned 600 s each. Read the nightly's
    `INITED`/`DONE` lines when a run is suspiciously green.
- **QE culture**: substantive changes get an adversarial review or QE
  agent pass before merge; bugs found by tests/fuzzing become pinned
  regression tests + (design-class ones) defect-ledger entries.

## Known deferrals / open items

- **Compaction (M4) — 100% implemented**: `server/compaction/` —
  planning is metadata-only (live, same spec + partition values;
  adjacency NOT required) and ONE-PASS BIN PACKING. Sort a bucket's
  candidates by SIZE, pack until their bytes reach
  `HOGLAKE_COMPACTION_TARGET_BYTES` or the group holds
  `HOGLAKE_COMPACTION_MAX_INPUT_FILES` (64), close, carry on; the
  trailing remainder is a group too. A group is dropped unless it holds
  `min(HOGLAKE_COMPACTION_MIN_INPUT_FILES, target / its largest file)`
  files, floored at 2 — the minimum SCALES, because a fixed file count
  cannot judge a byte target and a fixed 5 silently means "never
  compact" for any bucket whose files exceed a fifth of the target
  (every sorted table, whose target is derated for heap). Size order is
  load-bearing: outputs inherit `min` row id, so in row-id order a big
  output blocked the small files behind it forever. Output compression is
  `HOGLAKE_COMPACTION_CODEC` (default **zstd** at
  `HOGLAKE_COMPACTION_ZSTD_LEVEL` 3; snappy/gzip/lz4_raw/uncompressed
  also legal, an unknown name refused at boot). Not a per-file detail:
  compaction rewrites a table's rows into target-sized files and then
  leaves them alone, so it is the codec a compacted table is stored and
  scanned under from then on. It was UNCOMPRESSED — inherited from
  `ExampleParquetWriter`'s default, never chosen — which made every
  merge a permanent decompression of clients that write snappy (pyarrow
  and DuckDB defaults) or zstd (hedgerow), measured at 1.4-1.8x the
  input bytes. An input's codec is never an instruction; the rewrite
  decodes and re-encodes.
  A table's plan is fixed before execution:
  outputs are never re-compacted within that run, and the file minimum
  keeps them from being re-compacted in later runs either. Input bytes
  estimate the output size; encoding and DV removal change it. The
  existing max-groups-per-run budget still caps executed attempts. Rewrite via **parquet-java**
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
  on is an ops decision, not a code gap.
  **A GROUP COSTS A FIXED AMOUNT OF WALL TIME, and that is the shape
  every throughput knob here answers.** Measured on gigahog-prod-us
  (2026-09-24, catalog millpond-prod-us, `main.events_raw`): ~8.5 s per
  group whatever the group holds, because the cost is object-store
  LATENCY — the serialized opens, the plan, the commit — and not bytes.
  Three knobs overlap it, and only the middle one is on by default.
  `HOGLAKE_COMPACTION_PARALLEL_GROUPS` (**default 1**, which is the
  sequential sweep exactly: no executor, groups on the calling thread,
  tables planned and executed one at a time as before) runs that many of
  a sweep's planned groups at once. The sweep plans and executes PER
  TABLE, deliberately — planning every table up front would make the
  last table's plan as old as every rewrite before it, which at 64
  groups and ~8.5 s each is minutes of staleness arriving at a commit.
  A wave is joined whole (the slowest group bounds it) and drawn from
  ONE table's queue, so a table with fewer groups than the knob runs at
  its own group count: the knob is a ceiling, not a promise. Groups
  share no input file by construction, so the only serialization between
  them is the per-catalog commit lock, which the commit transaction
  takes ALONE — never across the rewrite or the upload, and there is a
  test that blocks a rewrite mid-read and takes the real lock to prove
  it. Compaction's commit now passes `HOGLAKE_COMMIT_LOCK_TIMEOUT_MS`
  like every other acquirer (a timeout anywhere in the commit
  transaction — the advisory lock or a row lock in the tail, since the
  setting is transaction-local — is a counted race, not a failure),
  because N workers queued on an untimed lock hold N pooled connections
  and turn foreground commit backpressure from a typed 503 into a
  connection-pool 500. That is also why boot REFUSES
  `parallelGroups > HOGLAKE_DB_POOL_SIZE - 4` (default pool 10, so the
  ceiling is 6); the four are a floor, not a model — the other
  background loops draw on the same pool. Raising the knob wants the
  pool and the pod's CPU raised with it, and it DIVIDES
  `HOGLAKE_COMPACTION_SORTED_HEAP_BYTES` (see below).
  **Cancellation stops the sweep on BOTH paths.** The table loop and the
  wave loop check this thread's interrupt flag, `executeGroup` restores
  it when a caught `Throwable`'s cause chain holds an interrupt (the
  object-store client translates it and sometimes clears the flag), and
  `runOnceAllCatalogs` rethrows rather than treating cancellation as a
  per-catalog failure and sweeping the rest of the fleet. The sweep
  throws `SweepInterrupted` — an `InterruptedException` carrying the
  PARTIAL tally — so the ledger row for a cancelled sweep still counts
  the groups that committed; those commits are durable, and a row saying
  otherwise is the same lie an uncounted swallow tells.
  `HOGLAKE_COMPACTION_PARALLEL_INPUT_OPENS` (**default 8**) opens that
  many of one group's inputs at a time; merge order and the streaming
  memory bound are both preserved — only the `open` round trips overlap,
  an open-but-unread input holds its parsed footer rather than a
  readahead buffer, and the rewrite still consumes inputs in order on
  one thread. Measured on a synthetic 64-file group against a MinIO
  container: 435 ms sequential, 74 ms at 8 (and the double open per
  input — once for the schema, once for the rows — is gone). It is the
  one of the three that is ON by default, so its footprint is worth
  stating: the worst case is `parallelGroups x inputOpenParallelism`
  readers holding a parsed footer each, capped at
  `S3InputFile.DEFAULT_MAX_PREFETCH_BYTES` (64 MiB) — 6 x 8 x 64 MiB at
  the highest `parallelGroups` a default pool allows, against kilobytes
  per footer for every real file. The window is per GROUP and is not
  divided by `parallelGroups`.
  **The sorted-path heap budget is DIVIDED by the group concurrency,
  not gated.** `sortedHeapBytes / parallelGroups` is what
  `CompactionConfig.sortedRowCeiling` converts to a row ceiling, so N
  concurrent sorted groups cannot exceed what one group was allowed, and
  the bound is arithmetic evaluated at planning time rather than a
  runtime invariant holding a permit across object-store IO. The price
  is proportionally smaller sorted groups whether or not a sweep ever
  runs two at once; at the default of 1 the arithmetic is bit-identical
  to what it was. BOTH arms are divided — the density arm (a statement
  about rows) and the nested arm (a statement about bytes) — because
  they estimate the same heap by different routes and the tightest wins,
  so leaving either undivided lets N groups take N heaps. At a high N
  the divided ceiling plus the SCALING file minimum can stop a sorted
  table forming groups at all; that is the cost, and it is why the
  default is 1. A gate would preserve sorted group SIZE and is the
  alternative if that ever bites — but the real fix is the external
  merge sort `CompactionConfig.sortedHeapBytes` already describes,
  which removes the ceiling and the division together.
  **Two maintainers do not rewrite the same group: they CLAIM it**
  (`hog_compaction_claim`, V15, `HOGLAKE_COMPACTION_CLAIMS_ENABLED`
  default on). **A claim is an optimization, never authorization** —
  correctness against a concurrent rewrite is, and stays, the
  plan-to-commit re-verification under the commit lock, and a change
  that makes the commit path trust a claim turns an advisory lease into
  a correctness dependency on two clocks. What it removes is waste: two
  replicas planning the same candidate set both rewrote and uploaded
  every group and one of the two was discarded at commit (a 547 s
  production sweep committed 34 groups and lost 30 that way, counted
  `skipped_conflicts`). The key is a hash of the group's spec, partition
  values and sorted input file ids — the identity of the WORK, so two
  replicas compute it with no coordination — while the planner's skip is
  by input-file OVERLAP, because a replica planning a moment later packs
  the same files under a different key. A claim is released when its
  group does NOT commit and deliberately KEPT when it does: the inputs
  are dead, and the other maintainer's stale plan still names them, so
  the row is what turns its arrival into a counted `claimed_elsewhere`
  instead of a wasted rewrite. A kept claim gets its own lease
  (`HOGLAKE_COMPACTION_COMMITTED_CLAIM_TTL_SECONDS`, 600 s), sized for
  what it has to cover: the age of a sibling's PLAN, which is one whole
  sweep — ~544 s for 64 groups at the measured 8.5 s each, not one sweep
  INTERVAL. The cost is `committed groups per sweep x lease / sweep
  duration` rows per table, about 70 at production settings, whose
  `input_file_ids` the planner reads on every pass.
  `expires_at` is the only liveness
  protocol (`HOGLAKE_COMPACTION_CLAIM_TTL_SECONDS`, 900 s) — there is no
  heartbeat, so a killed maintainer costs one lease and nothing else —
  and the bulk purge at the head of each sweep clears the rest. That
  purge is not gated on `HOGLAKE_COMPACTION_CLAIMS_ENABLED`, so turning
  the CLAIMS off still clears what they left; it does live at the head
  of a SWEEP, so turning compaction off stops it like everything else.
  `/verify`'s `compaction_claims` check is what reds when that purge
  stops running or an expired claim outlives its table; both arms
  require the claim to be well PAST its expiry, because a live claim on
  a dropped table is the normal state of a group that just committed,
  and the gap between expiry and the next sweep's purge is a correct
  system. **Nested schemas rewrite** —
  list/struct/map are copied through recursively (the plan is a tree of
  steps; parquet-java's Group API already is one), so a nested table is
  compactable like any other; an input whose nested SHAPE disagrees with
  the live column is `unconvertible_schema`, never a guess. A fault that is DURABLE and
  the WRITER's is the OTHER typed skip, `invalid_data`: a value that
  cannot exist under the type its own file declares (an empty blob under
  a decimal, an unscaled value past the destination precision, a row
  past `HOGLAKE_COMPACTION_MAX_NODES_PER_ROW`), a file whose schema
  contradicts its own `explicit_row_ids` registration (invariant 2's
  reserved field id present or absent), or a registered `.dv` object
  whose bytes do not decode — the decoder's refusals, including the
  containment around the roaring library, about bytes it already holds;
  the object-store FETCH stays retryable. Durability and fault are the
  axis, not values-versus-schema — a schema skip clears when the schema
  or the file set moves, and this one never does, so it is re-planned
  and re-refused every sweep and a nonzero count is a writer bug rather
  than a backlog. Remaining
  rewrite deferrals (all surface as `unconvertible_schema` skips, never
  wrong bytes): INT96, decimal-scale changes, and non-native
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
  endpoint (REPEATABLE READ MVCC snapshot, no catalog lock), and the
  same code a background sweep runs. **Twelve** checks: row-id tiling
  (explicit_row_ids-aware), DV uniqueness/monotonicity/bounds,
  orphaned live rows on dropped tables, still-referenced removal-queue
  entries, true snapshot density, next_row_id consistency,
  expiry-floor (invariant 5, sharing ExpiryService's own floor clause),
  versioned-row visibility bounds (invariant 6, over every versioned
  table), superseded-offset release (#167, asking OffsetRepo's own
  predicate), compaction staging tickets (#174), upload claims
  (#162/#167) and compaction group claims (V15: a claim is an
  optimization, never authorization, so a violation is redundant work
  or a leaked row and never a wrong commit). Each check carries a
  one-paragraph `description` of the invariant it enforces; counts are `count(*)` and samples are capped
  at 20, trimmed round-robin so a composite check's noisiest class
  cannot crowd out its siblings.
  The LOOP is `HOGLAKE_VERIFY_INTERVAL_MS`, **default 0 = off** — like
  compaction, turning it on is a per-workload ops decision, because an
  aggregate pass over every catalog must not run on the replicas
  serving the commit tail. A sweep publishes
  `hoglake_verify_violations{catalog, check}` (MultiGauge, whole row
  set, so a vanished catalog's series retire) and counts a catalog it
  could not read in `hoglake_verify_errors_total{catalog}`; a manual
  trigger publishes neither. A standing violation warns once per
  distinct failing-check set, not once per sweep.
  Every path-equality sub-query is registered in
  `VerifyService.PATH_EQUALITY_QUERIES` and EXPLAINed against a
  50k-file manifest (`VerifyQueryPlanIntegrationTest`): none of those
  tables is indexed on `path`, so a query the planner cannot flatten is
  quadratic and invisible on any fixture-sized catalog.
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
  [docs/iceberg-federation.md](docs/iceberg-federation.md) /
  [docs/trino-integration.md](docs/trino-integration.md); v1 schema already
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

Reference docs live in [docs/](docs/); [README.md](README.md) is the
front door. The predecessor-analysis set (the DuckLake and pyducklake
API maps, the C++ source inventory, the Paimon comparison, the schema
review and the language retrospective) was retired in the 2026-09-17
docs pass: each had done its job informing the as-built system, and git
history holds them.

**Design** — [docs/metadata-schema.md](docs/metadata-schema.md) (the
schema, table by table) ·
[docs/iceberg-federation.md](docs/iceberg-federation.md) /
[docs/trino-integration.md](docs/trino-integration.md) (engine
surfaces).

Rehearsing a Postgres major-version move: the integration harness pins
the version production runs, overridable for a dry run of the whole
suite — `./gradlew :test -PpgImage=postgres:19` (or
`HOGLAKE_TEST_PG_IMAGE`). A version move rarely breaks application code;
it breaks statistics views that moved columns, which only an integration
run can see. The suite, the dev compose stack and the CI image-smoke
service all run **Postgres 18** as of 2026-09-18 (moved from 16); the
override runs backwards too (`-PpgImage=postgres:16`), which is how a
failure gets attributed to the version rather than to the environment.
Two things the test suite cannot see, and a future move must check by
hand: the docker-library image's PGDATA path (18 moved it to
`/var/lib/postgresql/18/docker` and the VOLUME to the parent, so a
compose mount of the old `/var/lib/postgresql/data` silently stops
persisting), and Flyway's supported-version table — a Flyway that
predates the server warns "support has not been tested" on every
migration and is the thing to bump first.

**Operating** — [docs/operational-notes.md](docs/operational-notes.md)
(what changes, and what honestly doesn't, at 2PB/1T) ·
[docs/fuzzing.md](docs/fuzzing.md) (property testing and fuzzing).

**Why this architecture** —
[docs/ducklake-defect-ledger.md](docs/ducklake-defect-ledger.md) (the
predecessor's production bugs, and where hoglake answers each).

**Per-component** — [server](server/README.md) ·
[duckdb-client](duckdb-client/README.md)
([design](duckdb-client/DESIGN.md) ·
[parity](duckdb-client/PARITY.md)) ·
[trino connector](server/trino/README.md) ·
[pyhoglake](pyhoglake/README.md) · [webui](webui/README.md) ·
[hedgerow](hedgerow/README.md) · [bench](bench/README.md).

Bug-hunt writeups and in-flight worklists stay local-only (they are
snapshots of a moving tree, not documentation). Name one
`<whatever>-not-for-commit.md` and `.gitignore` will refuse to stage it
— no edit to `.gitignore`, and the filename never lands in a tracked
file, which matters when the name itself would disclose something.
