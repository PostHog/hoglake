# Hoglake — Postgres-Native Lakehouse Catalog Control Plane

Agent guidance for this repo (github.com/PostHog/hoglake — the
standalone hoglake monorepo; the codebase began life as a subtree of
the PostHog DuckLake fork, now PostHog/ducklake, whose history holds
the pre-split commits).

## Pre-push checklist

**Never push broken code.** Before every commit and push:

(And before a PR that touches a maintenance loop, the commit path or a
statement over the manifest tables: the six scale questions at the end
of [Scale doctrine](#scale-doctrine-read-before-touching-a-query-a-loop-or-a-lock)
are answered in the PR body, the sixth being what ran on the local
stack and what it showed.)

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
| `server/` | The control plane: DDL, commits (OCC + admission backpressure), scans, changefeed, offsets, retention/expiry/cleanup, hydrator, compaction, retirement, metrics, audit | Kotlin 2.4 / JDK 25 (flox) / Ktor / JDBI / Flyway / parquet-java (footer reads + compaction writes) | JUnit 6 + Testcontainers (PG18, MinIO) + kotest-property |
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

**Pending note for the 1.3.8 release body**: pyhoglake loses the public
`Catalog.verify()` method and the `VerifyReport` / `VerifyCheck` models
with #261, in a patch bump and with no changelog in the tree. The
endpoint it called is gone, so the method could only 404 against a 1.3.8
server — but it still works against an older one, which is what makes it
a breaking client change worth a line rather than a silent deletion. Say
so in the release notes when 1.3.8 is cut.

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
   parquet, and the removed `/verify` was metadata-only). Both refusals are implemented
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
   with an undrained row (path-reuse guard) and compaction
   pre-registers its output path as a `compaction_staging` claim
   settled `'registered'` on group commit — and only if cleanup has not
   TOUCHED that ticket.
   **THE DRAIN IS A CLAIMED WORK QUEUE AND TAKES NO CATALOG LOCK
   ANYWHERE** (V21): CLAIM in its own transaction committed before any
   object-store call, WORK with nothing open, SETTLE fenced on
   `claimed_by`. A claim is a **LEASE**
   (`HOGLAKE_CLEANUP_CLAIM_LEASE_SECONDS`, 900), not a lock, and the
   `claimed_by` fence is what stops a lapsed worker writing the ledger
   over the worker that took its row. THE LEASE IS CLEANUP'S ALONE:
   `commitGroup` re-reads its staging ticket `FOR UPDATE` and takes it
   only if NOBODY HAS TOUCHED IT, because an expired claim does not mean
   the object survived — it means nobody knows.
   The proof — the four-inserters-three-serializers argument, the
   `hog_upload` row-lock arm, the staging carve-out, the `DeleteObjects`
   ceiling, the ledger-purge shape and the production measurements — is
   in server/README.md §Retention, expiry, and safe file removal, which
   is the single home for it.
5. **Expiry never passes head or (when `consumer_floor`) the min
   consumer offset**, and names the pinning consumer. Ranges below
   `earliest_snapshot_id` are 410 Gone — consumers reconcile, never
   silently skip; the floor advance captures
   `hog_catalog.earliest_snapshot_time` so 410s can say WHEN the floor
   was reached. A fifth sweep step deletes versioned DDL rows
   (`hog_table_version`/`hog_column`/`hog_partition_spec`/`hog_sort_spec`/
   `hog_view`) whose `end_snapshot <= earliest_snapshot_id` — invisible
   at every retained snapshot, so DDL churn cannot grow them unbounded.
   The FILE rows below the floor are the exception, and deliberately:
   a sweep advances the floor under the commit lock and then purges
   `hog_data_file`/`hog_delete_file` in bounded pages OUTSIDE it, so an
   ended row below the floor is legitimate state for a few sweeps. It is
   unreadable (410) and immutable while it sits there, and each page
   queues its paths in the same statement that deletes its rows.
   NOTHING ASSERTS THE INVARIANT HALF OF THIS ANY MORE: #262 gated
   `/verify`'s `expiry_floor` file arm on the ledger row reporting the
   purge drained, precisely so it kept catching a drained purge that
   still left rows below the floor (its predicate and the floor advance
   disagreeing) without alerting on the designed lag — and #261 removed
   the check. UNWATCHED UNTIL #261 ships the paged scrubber. The BACKLOG
   half still works and never needed verify: `purge_truncated`,
   `purge_remaining` and `purge_failures` on the ledger row,
   `hoglake_expiry_purge_truncated_total` and
   `hoglake_expiry_purge_remaining`. server/README.md §Retention has the
   rate arithmetic and the 2026-10-01 incident it comes from.
6. **Versioned-row visibility**: a row is visible at S iff
   `begin_snapshot <= S AND (end_snapshot IS NULL OR S < end_snapshot)`.
   Every read path uses exactly this predicate.
7. **TableInfo aggregates never come from `hog_table_stats`** (that row
   is the gross append counter / row-id allocator anchor — head-scoped by
   nature). Where they DO come from is now the request's own shape
   (#232), and the response says which:
   - `snapshot` / `at_timestamp` given: the files visible AT THAT
     SNAPSHOT, aggregated from the manifest (`FileRepo.aggregateAt`).
     Exact, and the only correct answer for a past snapshot. Same for a
     createTable/alterTable receipt, whose snapshot its own transaction
     just made.
   - neither: the MAINTENANCE SAMPLER's published generation, summed per
     table (`TierTotalsRepo`), because the maintenance section's rule
     below — persisted summaries, never the manifest — applies to this
     endpoint too: the writer fleet calls it three times per flush and
     the aggregate it replaced was ~10M manifest rows per call. The
     response carries `totals_snapshot_id` (the snapshot the numbers are
     exact at) and `totals_as_of`; all three totals are ABSENT, never 0,
     when the published generation does not cover the table.
   - `totals=false`: absent, and no file read of any kind. The identity
     read. `read_snapshot_id` is present on every one of these paths —
     it describes the read, not the totals, and a writer sends it back
     as a commit's `read_snapshot`.

   THE NAMESPACE LISTING (`TableSummary`) IS NOT ON THIS RULE YET: it
   still aggregates the manifest per table through `LIVE_SUMMARIES_SQL`'s
   LATERAL, and its three totals stay required and exact at head. So the
   table GET and the listing can disagree by the sample's age for the
   same table; the OpenAPI `Table` schema states the asymmetry, and
   moving the listing onto `TierTotalsRepo.PER_TABLE_SQL` is its own
   change.
8. **Audit/observability never rides a transaction** and never writes
   to any database. Audit emits after commit/rollback; metrics are
   passive. Maintenance run history and asynchronous summaries are
   durable operational state; run-history writes are best-effort after
   task transactions resolve. The removal ledger remains transactionally
   integrated with commits and cleanup, as invariant 4 requires.
9. **All SQL is parameterized.** No string-built values, anywhere.
10. **Rows-then-offset everywhere** (server offset API is monotonic;
    hedgerow commits offsets only after destination durability).
11. **The table is the authority: no code path may treat a dropped
    table's file rows as reachable data.** Table state is established
    BEFORE file rows are read, written or counted. `DROP TABLE` is
    O(columns) — `TableRepo.markDropped` sets
    `hog_table.dropped_snapshot` and end-snapshots the one live version
    row and the column rows, and touches NO `hog_data_file` or
    `hog_delete_file` row. It used to end-snapshot all of them in the
    same transaction, under the per-catalog commit lock: 3,008,849 rows
    measured at 44.6 s and 3.9 GB of WAL warm and uncontended, which on
    gigahog-prod-us was a drop that could not complete at all (#193).
    The rows leave later, paced, through `RetirementService`, and only
    once `dropped_snapshot <= hog_catalog.earliest_snapshot_id` —
    above the floor a read at S < drop is still legal and still needs
    them. One predicate expresses the rule (`t.dropped_snapshot IS
    NULL`, or `:snapshot < t.dropped_snapshot` for a time-travel read)
    and every resolver, loop, gauge and check below uses it:
    `TableRepo.findLive`/`findAt`, `CommitService.resolveLiveTable`,
    `CompactionService`'s planner AND its commit re-verification,
    `Hydrator.claimPending`/`rehydrateFailed`, `CatalogMetrics`'s
    sample, `MaintenanceSummarySampler`'s scan.
    THREE CARVE-OUTS, and they are the whole list:
    - **path-keyed liveness** (`CleanupService.referencedPaths`,
      `UploadService`'s reclaim/register probes) asks "does ANY row name
      this path", live or not, and MUST keep seeing dropped tables'
      rows: that is what fences their objects against physical deletion
      until retirement queues them;
    - **the retirement loop itself**, whose entire job is those rows;
    - **maintenance surfaces that count rows at NO snapshot** (the
      hydrator, the gauges, the debt sampler) filter on
      `hog_table` rather than resolving visibility at a snapshot,
      because there is no snapshot to resolve at.
    TRUNCATE IS NOT COVERED. `CatalogService.truncateTable` is the one
    remaining caller of `FileRepo.endLiveFiles`/`endLiveDeleteFiles` and
    keeps the O(rows) pass under the commit lock; `POST .../truncate` on
    a multi-million-row table has today's hazard, and redesigning it is
    its own change.
12. **A file's partition values are valid only under the spec they were
    computed with**, and the server cannot check them: it never opens
    the parquet (footer-shipping), so it can neither verify nor
    recompute a transform, and `CommitService.validateFiles`' arity
    check cannot tell two same-arity specs apart. `read_snapshot` is
    therefore the whole guard, and an append whose files carry
    `partition_values` NEEDS one — a blind append has no conflict window
    at all, so a spec change between the writer's read and its commit
    would register the old transform's values under the new `spec_id`
    with nothing afterwards able to detect it. With one, the change is
    `checkConflicts`' DDL arm.
    The REFUSAL is staged, not immediate:
    `HOGLAKE_REFUSE_BLIND_PARTITIONED_APPENDS` (default **false**) makes
    it the 422 the contract describes, and until then the shape is
    accepted and logged at WARN once per RESOLVED table per pod, naming
    the client and quoting the refusal it will get. (Once per resolved
    table, not per requested name: the dedupe key is the server's
    `tableId`, because a key holding anything a client supplies — a
    User-Agent, a `namespace.table` string — lets one client fill the cap
    and silence the signal for the whole fleet.)
    The flag exists because **every production flush is that shape
    today**. millpond is the primary blind writer: `prepare_append_files`
    puts a `read_snapshot` on the payload and millpond deletes it
    (`millpond/hoglake.py`, `payload.pop("read_snapshot", None)`), because
    a prepared payload's basis is frozen and the 409 it earned when
    another pod added a column was permanent. `duckdb-client` is the
    second — append-only commits carry no `read_snapshot`
    (`storage/hoglake_transaction.cpp`) and partition values ride whenever
    the table has a live spec (`storage/hoglake_insert.cpp`) — so
    enforcing this in the same change as the server would break both
    millpond's flush and `INSERT INTO <partitioned table>` through the
    extension.
    THE FLIP PRECONDITION is therefore two-part: millpond keeps the field
    (safe now, and not before — `ddl_since_read_snapshot` subclasses
    `CommitConflictError`, which millpond's `is_retryable` ladder already
    recovers from by `reset_caches` + dropping the refused payload, so the
    re-upload it was avoiding costs one flush rather than wedging), and
    duckdb-client starts sending one. The rule is not optional; the
    rollout order is. WHEN to flip is read off
    `hoglake_blind_partitioned_appends_total{catalog,namespace,table}`,
    which counts every occurrence and is deliberately outside the WARN's
    dedupe: a line that fires once per pod cannot distinguish a fixed
    client from a pod that already logged it, and the counter keeps
    counting after the flip.
    `checkConflicts` also runs BEFORE `validateFiles`, deliberately: a
    DDL change and the file-level symptom it produces (stats naming a
    dropped field, the wrong arity, values on a now-unpartitioned table)
    arrive together, and the CAUSE has to answer before the SYMPTOM — a
    422 tells a caching writer nothing about its stale basis, so it
    rebuilds the same doomed payload until the cache ages out.
    That arm is TYPED: when every conflicted row is DDL it is
    `HoglakeException.DdlSinceReadSnapshot` -> 409
    `ddl_since_read_snapshot` carrying `tables`, `read_snapshot` and
    `retry: re-prepare`, never the retryable `commit_conflict`. The
    distinction is the point: a prepared request's `read_snapshot` is
    part of a durable payload that must be replayed byte-identically,
    so the refusal is permanent for that payload and a retry loop on it
    is a livelock. Row-content conflicts keep `commit_conflict`, and so
    does a mixed refusal (`all { ddl }`, not `any`) — a plain retry
    clears those. The `expected_table_uuid` guard joins the same family:
    `HoglakeException.TableRecreated` -> 409 `table_recreated`, because
    the incarnation the caller named is gone and its history does not
    carry over (it was a `CommitConflict`, and pyhoglake could only tell
    it apart by grepping the detail string for "the table was
    recreated"). So does the below-floor `Expired` 410. Three codes, one
    recovery: re-read the table and prepare a new request. And
    `table_recreated` is the ONLY guard against a drop+recreate —
    `checkConflicts` keys on the RESOLVED table id and counts
    `table_created` only for a guarded request's delete targets, so a
    recreate is outside an append's window however fresh its
    `read_snapshot` is.
    `DdlSinceReadSnapshot` is a SUBCLASS of `CommitConflict`, and that is
    load-bearing rather than tidy: clients discriminate with `isinstance`
    ladders (millpond's retry budget does, and it already recovers
    correctly by resetting its caches on a `CommitConflict`), so
    un-subclassing would turn a working recovery into a hard re-raise.
    The wire CODE is what changed. Every `when` over the hierarchy must
    therefore put the subclass arm FIRST — `ErrorMapping`,
    `Metrics.commitFailureResult` and `Audit.failureOutcome` all do, and
    each says so.

## Scale doctrine (read before touching a query, a loop or a lock)

Three outages in one week came from the same mistake, and none of them
was a wrong query. Each was a correct query, a correct loop or a
correct lock written for a catalog of thousands of files and run
against ten million:

- **2026-09-28, expiry.** One sweep deleted every ended file row below
  the new floor in one statement, under the per-catalog commit lock,
  in compaction-commit order (random heap reads). ~50k files per sweep
  at 700-870 µs each; the 60 s statement timeout fired 195 times in
  200 sweeps, each failure holding the lock the full 60 s. Mean commit
  wait 13.5 s, API pods liveness-killed all day.
- **2026-09-29, cleanup.** The drain re-checked every path's liveness
  under the commit lock: ~19 s per 1,000 paths on a table whose hot
  set does not fit the cache, every commit queued behind it. The lock
  protected nothing (V21). Cleanup was off in production until it came
  out, and is ON in prod-us since 2026-09-29 at 60 s / batch 10,000 /
  1 worker.
- **2026-09-30, compaction (#247, shipped in 1.3.6; the other two
  shipped as #220, #225).** The planner loaded every small file of a
  table (9.9M rows, a correlated `array_agg` per row, an `ORDER BY`)
  into a Kotlin list inside one transaction, then bin-packed in memory
  for minutes. `idle_in_transaction_session_timeout` killed the
  connection at 30 s; every sweep failed for eleven hours. The groups
  it planned were then refused anyway: packed by bytes, checked by
  rows, 2.9M rows against a ceiling of 552k.
- **2026-09-30 again, 55 minutes after #247 rolled (#255).** The fix
  raised fan-in from 64 to 2,048 files per group and measured one
  thing in the per-group path: the commit-lock hold, on a cache-hot
  fixture. The same path read the inputs' `hog_file_column_stats` rows
  to build the output's stats: 2,048 index descents, ~53k heap rows on
  a 66 GiB table, and it crossed the 60 s `statement_timeout` once the
  rotation cursor reached colder buckets. A `throw` inside a catch arm
  then bypassed the sibling arm that counts a failed group, so one
  group's timeout aborted the whole sweep with an empty ledger row,
  seven runs in a row. The full suite was green at 2,149 tests. Both
  defects showed in under a minute on the compose stack with four
  tables and a trigger that failed one output insert: `main` died with
  zero outputs, the branch recorded one failed group and committed the
  rest.

The numbers to design against are production's, not the fixture's.
gigahog-prod-us on 2026-09-30: one catalog, ~500k snapshots, retention
3,600 s, ~14M live `hog_data_file` rows, 10M of them on one table
across ~2,800 partitions, 96% of them under 60 KiB; ~50 commits/min of
~270 files × 25 columns each, rising 4-5× with pyhoglake's concurrent
uploads; ~3,600 files added per minute; compaction retiring 64 files
per 4-minute run. The pod it all runs on: one maintenance replica with
`dbPoolSize` raised to 16, which is what makes 6 compaction groups plus
a cleanup worker legal against the reserve of 4. Any code path that is
O(files of a table), O(snapshots) or O(commits) is O(ten million) here,
and the growth rates are as important as the counts. Note also that
these are PROD-US numbers and prod-us settings: dev and prod-eu carry no
`maintenance:` block at all, so they run the chart defaults, and every
tuning figure in this section and in server/README §Retention is
prod-us-only.

The rules. Every one of them was violated by the code above.

- **Bound every fetch.** No statement on a maintenance or request path
  may read an unbounded row set into memory. Every candidate, victim or
  work-queue read must carry a `LIMIT` sized from the work the run can
  actually do (compaction: `maxGroupsPerRun × maxInputFiles`; cleanup:
  the sub-batch; expiry: a file-count batch), and the ledger must record
  when the limit truncated. Compaction's candidate read does not yet —
  it is the 2026-09-30 shape above, and the branch named there is where
  it gets one. Selecting from a per-partition summary
  (the sampler tier) and then fetching rows for the chosen buckets is
  the shape; "select everything and filter in Kotlin" is not.
- **Bound every unit of work under a lock or inside a transaction.**
  A statement under the per-catalog commit lock, or inside any
  transaction, has a row bound and a `SET LOCAL statement_timeout`
  smaller than the lock's fair share. Loops of bounded steps, each its
  own transaction, with a run budget and a pause (the retirement
  shape), never one statement that is "done when it is done".
- **Never hold a transaction across non-database work.** Parquet I/O,
  object-store calls, in-memory sorting and packing, HTTP calls: all
  of it happens between transactions, never inside one. Read what you
  need, commit, compute, open a new transaction for the write, and
  re-check under it what could have changed (liveness, claims). The
  test suite is where the rule gets enforced, and today it is opt-in:
  `PgTestSupport.freshDatabase(productionSession = true)` wires the pool
  with `Database.SESSION_INIT_SQL`, so the test connection carries
  production's own bounds — a 60 s `statement_timeout` and a 30 s
  `idle_in_transaction_session_timeout` — and it is default OFF on
  purpose, because a suite-wide bound would sit under the six-figure bulk
  seeds and the drain tests that hold a transaction open across MinIO
  round trips. A suite-wide guard at
  `idle_in_transaction_session_timeout = '2s'`, so that a transaction
  left open across slow work fails the test the way production fails the
  pod, lands with the compaction planner fix on branch
  `jakob/compaction-plan-bounded` (PR pending). Either way the rule is
  the rule: do not raise the value to make a test pass; split the
  transaction.
- **Measure per-row cost, not per-statement cost.** A statement's
  plan says which index it uses; the per-row cost on production's
  access pattern (random heap reads on a 10M-row table versus PK-order
  reads) is what decides whether a 50k-row batch takes 1 s or 50 s.
  Expiry's 700 µs/file versus retirement's 20 µs/row for the same CTE
  was the whole 2026-09-28 incident. State the per-row figure and the
  batch that fits the budget in the KDoc.
- **Prove the plan on a production-shaped fixture.** "An index proves
  itself against the query it serves" (below) is necessary, not
  sufficient: a fixture of a few thousand rows proves nothing about a
  skip scan, a generic-plan flip after pgjdbc's fifth execution, or a
  correlated subquery. Plan tests for a hot statement seed at least
  100k rows in the shape production has (the events table's
  partition skew, mostly-live files, retained history) and assert
  rows examined and buffers, not only the index name. Record the
  measured figure in the test and the KDoc, with the fixture size.
- **Run it on the local stack before it ships.** The suite proves
  statements and functions; it does not prove the path. A change to a
  migration, a maintenance loop, the commit path, or any statement
  over the manifest tables is run end to end on the compose stack
  (`just up` builds the server image from the working tree; the
  harness in #255's PR body is the shape) before the PR is opened:
  the migration applied to a database that already holds data, the
  loop driven through its API trigger with the knobs the PR changes
  set to their production values, a fault injected where production
  will inject one (a trigger that raises on an insert, a
  `statement_timeout` of 1 ms, an object missing from the store), and
  the ledger, the file rows and the output footers read back. When
  the change fixes an incident, run `main` through the same harness
  first so the PR shows the before and the after. The compose stack's
  data is small, so it will not reproduce a cost that only exists at
  ten million rows; that is what the plan test above is for. What it
  does reproduce is everything a unit fixture mocks away: boot,
  Flyway, the pool and its session bounds, the real object store, a
  sweep's accounting when one unit of work fails, and what the
  console shows for it. The PR body says what ran and what it showed;
  "the suite is green" is not an answer to this question.
- **Do the growth arithmetic for anything written per commit or per
  file.** Rows per file per commit × files per commit × commits per
  minute × retention, in bytes on disk, before the change ships.
  `hog_commit_receipt` stored 566 KB per 270-file commit and nothing
  purged it — ~40 GB/day, 58.2 GiB standing when #240 found it. It now
  stores a 33-byte digest instead of the body and expires at
  `HOGLAKE_RECEIPT_RETENTION_SECONDS` (7 days), purged in bounded pages
  by the cleanup sweep (V24). Column stats are 25 rows per file and are
  still unbounded. Every such table needs a retention and a bounded
  purge from the day it is created — **bounded, not necessarily
  PK-walking**: the removal-ledger purge walks its primary key because
  its predicate (`drained_at`) is uncorrelated with that key, so pages
  can purge nothing and the walk needs a cursor and a skip cap. The
  receipt purge walks `(catalog_id, created_at)` instead, because the
  predicate IS the indexed column and the eligible rows are therefore a
  dense prefix — no cursor, no skip, and `purged < page` means the end.
  Pick whichever makes the eligible set an index range; what is not
  negotiable is that a page bounds the rows EXAMINED and that the
  statement carries its own `statement_timeout`, because a page's cost
  is not always its row count (a legacy receipt's ~160 KiB body is ~80
  synchronous TOAST-chunk deletes on top of its heap delete).
- **Size groups by every dimension a downstream bound checks.** If a
  later stage refuses on rows, pack by rows and bytes; if it refuses
  on heap, pack by the estimate it will apply. A planner that packs by
  one dimension and refuses on another does nothing forever and says
  so in a WARN nobody reads.
- **Stage every refusal that a live client could hit.** A new 4xx on a
  path a deployed writer uses ships behind a flag (default off) with a
  counter that shows the fleet has stopped sending the shape, and only
  then flips. `HOGLAKE_REFUSE_BLIND_PARTITIONED_APPENDS` is the model;
  millpond stripping `read_snapshot` from every payload was found by
  a review, not by the flag.
- **Fanout is the multiplier.** Files per commit is set by partitions
  touched, not rows per flush, so a faster writer produces more files,
  more stats rows, more change rows and smaller files, not bigger
  ones. When a change raises the write rate, re-run the arithmetic
  for expiry (`HOGLAKE_EXPIRY_BATCH × 60 / interval` must exceed the
  commit rate), compaction throughput, receipt growth and lock duty
  cycle (hold time per commit × commits per minute).

Before opening a PR that touches a maintenance loop, the commit path,
or any statement over `hog_data_file`, `hog_snapshot_change`,
`hog_file_partition_value` or a stats table, the PR body answers:
what bounds this per run, what bounds it per transaction, what the
per-row cost is on production's access pattern, what the plan test's
fixture size is, what grows per commit and how it is purged, and what
ran on the local stack and what it showed.

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
  whole chain (the equivalence test enforces it). There is no `V5__` —
  the number is skipped, which Flyway allows and which nothing in the
  repo explains; it is not a missing file. FKs with CASCADE,
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
    `MigrationLockWindowTest` now READS the files (V14 and up; V9-V13
    are grandfathered, V10-V12 being the recorded violations) and reds
    on a heavy statement outside the window, on a missing window, and on
    `SET LOCAL` in a file Flyway runs outside a transaction — where it
    applies to nothing. Deleting the save/restore pair from a migration
    used to leave the whole suite green, which is why the rule was
    broken four times and caught by review four times.
  - **`CREATE INDEX CONCURRENTLY` is not the safe default; it is a
    trade, and it is measured PER MIGRATION.** The exemption above says
    CIC MAY sit outside the window, not that it should be preferred.
    Both halves are table-size questions and neither answer transfers:
    on a small table a plain build can be no slower than CIC and one
    pass instead of two (V16 blocks), while on the manifest a plain
    build blocks every commit in the fleet past the admission bound
    (V17 does not). State the measurement in the file, from a run
    rather than from the neighbouring migration; each migration carries
    its own figures.
    A blocking build's cost is seconds of blocked writes inside the 5 s
    window (V13's/V16's transactional shape, where a failure rolls back
    and writes no history row). A CIC's cost is that its phases WAIT
    OUT every transaction older than themselves, and a parked build is
    a pod that never binds: `Main.kt` migrates before the listener
    opens, and the chart's startup probe SIGKILLs at 30 x 5 s. Hoglake's
    own parker is gone — `Database.awaitMigrationLock` polls
    `pg_try_advisory_lock` instead of blocking inside it, because a
    blocked lock wait is an open statement with a live xmin and CIC
    parked behind a replica waiting on the very migration CIC belongs
    to (a CHAIN, not a cycle, so the deadlock detector never fires).
    What remains are FOREIGN snapshots — an operator's
    psql in a transaction, `pg_dump`, an RDS export, a logical-decoding
    reader — none of which carry hoglake's idle bound. So a CIC on a
    big table is announced in its file as an OUT-OF-BAND step
    (`CREATE INDEX CONCURRENTLY IF NOT EXISTS`, run before promoting,
    after checking `pg_stat_activity` for old transactions), which
    makes the migration a no-op; V17's header carries the statements.
    Every cancelled build leaves an INVALID index, so a file that
    builds concurrently clears one first — `CREATE INDEX CONCURRENTLY
    IF NOT EXISTS` matches on NAME and would skip it forever.
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
    table rows, and the KEY through `pg_index.indkey` resolved to
    attribute names — a `contains("catalog_id", "path")` over
    `pg_get_indexdef` is satisfied by the index's own NAME.
    **The index name and the absence of `Seq Scan` are the weakest
    assertions available**, and V16 has the proof: rewriting its guard
    to `path LIKE ANY(...)` — which no btree under a non-C collation can
    drive — still produced `Index Only Scan using
    hog_file_removal_undrained_path`, because the leading `catalog_id`
    was enough, with `path` demoted to a Filter. What separates the two
    is what the scan DID: `Index Searches` (one descent per probe key,
    not one for the whole statement) and the ABSENCE of `Rows Removed by
    Filter`. Take buffers from the SCAN NODE, not the plan's maximum —
    EXPLAIN's `Planning:` section carries a `Buffers:` line that dwarfed
    a well-indexed scan's — and derive the budget from the probe rather
    than writing a constant: the constant V16 started with sat 1.27x
    below the degraded plan it had to exclude. The probe set itself must
    be SCATTERED, as a commit's output paths are; sixty-four keys
    sharing a prefix let the btree descend twice and walk, which made
    the measurement four times kinder than the real statement.
  - **`hog_file_removal` is indexed BOTH ways** — `..._drain
    (catalog_id, removal_id)` for the cleanup drain's ordered LIMIT and
    `..._undrained_path (catalog_id, path)` for the commit path's
    path-reuse guard and `UploadService`'s reclaim probe (V16, #199:
    without it every commit read the catalog's whole queue). Both are
    partial on `drained_at IS NULL`, and the path one is deliberately
    NOT unique: nothing makes a file path unique, so expiry can queue
    one path twice from one `DELETE ... RETURNING`, and with no writer
    carrying `ON CONFLICT` a unique violation would abort the sweep that
    advances the retention floor. The OTHER half of that statement pair
    is `hog_data_file_path` / `hog_delete_file_path` (catalog_id, path)
    (V17): the drain's liveness check asks those two tables the same
    question once per sub-batch, and until V17 both legs were a full
    scan of the manifest (the V17 file carries the buffer counts). NOT
    partial, because the check covers "any file row, live or not".
    `hog_upload`'s leg needs nothing: `UNIQUE (catalog_id, path)` (V12)
    is already that key, and its fallback is bounded by the catalog's
    ACTIVE claims rather than by an append-only history. The index is
    paid on the hottest write in the system, and it is built
    `CONCURRENTLY` because at that size a plain build blocks every
    commit past the admission bound — which reverses V16's trade on
    measurement rather than on analogy. What makes a concurrent build
    safe to deploy, and what still cannot make it safe on its own, is
    the CIC rule above.
  - **`hog_data_file_ended (catalog_id, end_snapshot) WHERE end_snapshot
    IS NOT NULL`** (V19, #193) is the third entry, and the one that
    INVERTS V17's partial-index decision for a reason rather than a
    preference. It serves `ExpiryService.DATA_FILE_EXPIRY_SQL` — the
    sweep's data-file DELETE, which carries `end_snapshot IS NOT NULL
    AND end_snapshot <= floor` and, before V19, matched NO index's
    leading columns at all: `hog_data_file_live` is partial on the
    COMPLEMENT of those rows, and everything else leads on ids or
    paths. So the statement was a sequential scan of the whole
    manifest, inside the sweep transaction, under the per-catalog
    commit lock. Measured on a fixture with production's mostly-live
    ratio (200,000 rows / 4,445 heap pages, 4,000 of them ended — one
    row in every hundred, the fraction the test's `ENDED_EVERY` fixes;
    `V19DataFileEndedIndexMigrationIntegrationTest` runs the migration
    against rows seeded BEFORE it and EXPLAINs the repo's own
    `internal` constant) with the ended rows SCATTERED through the
    manifest, scan
    node only: **4,445 buffers as a `Seq Scan` with `Rows Removed by
    Filter: 198,000` -> 2,003 buffers as an `Index Scan using
    hog_data_file_ended` with `Index Searches: 1` and nothing
    filtered** — 2.2x at a 1.0% ended fraction.
    THE SCATTER IS THE MEASUREMENT. An earlier draft clustered the
    ended rows and read 91x, which was an artifact of insert order:
    contiguous rows share heap pages. Production's ended rows are
    wherever expiry and compaction left them, so the index path costs
    roughly ONE HEAP BUFFER PER ENDED ROW and the win is
    `seq pages / ended rows` — **2-6x at prod-us's standing 30-90k of
    ~5.0M**, and a WASH right after a large batch of rows ends (measured at
    300k scattered ended rows: 300,003 buffers with the index against
    112,655 without, i.e. marginally slower). It is still the right
    index, because the steady state is the low fraction and because a
    sequential scan grows with the CATALOG while this grows with the
    WORK. It also does NOT bound the row work: the RI triggers for the
    three cascading children (~4 s per 300k rows against an EMPTY child
    table) and the CTE's tuplestore spilling at `work_mem` 4 MB are the
    dominant costs of that DELETE, and neither moves. PARTIAL here where V17's is not, because V17's
    statement covers "any file row, live or not" while this one carries
    `end_snapshot IS NOT NULL` in its own text — and the rows the
    predicate excludes are the overwhelming majority (4.94M of
    gigahog-prod-us's ~5.0M are live), so the index holds only the
    ~30-90k ended rows at a measured 12 bytes each and AN APPENDED ROW
    NEVER ENTERS IT. The hottest write in the system pays nothing,
    which is the whole argument. Built `CONCURRENTLY` in V17's shape:
    the build is a full heap scan whatever the index holds, so at
    production size it is V17's 26-35 s and a plain build's SHARE lock
    would block every commit for it — and the cold estimate is a FLOOR:
    two heap passes over 1,491 MiB is ~24 s at an idle volume's
    125 MB/s and 30-60 s on a busy one, against a 60 s session
    statement bound. NO MATCHING INDEX ON
    `hog_delete_file` — expiry's DV arm is an `OR` whose second arm is
    a correlated `EXISTS` and the planner never chooses one for it, so
    an index there would be paid on every write and used by nothing;
    splitting that statement into two arms is the fix, and it is
    ticketed.
    V19 and V20 are both applied in all three environments; the general
    rule their split recorded stands. An `ALTER TABLE` migration against
    `hog_table` MUST be timed, because an EXPIRY SWEEP HOLDS ACCESS
    SHARE ON `hog_table` for its whole transaction (step 5 deletes from
    `hog_table_version`, whose FK references it), so a long sweep makes
    the `SET LOCAL lock_timeout = '5s'` fire, which rolls the migration
    back cleanly and crash-loops the pod until the sweep finishes — the
    designed behaviour, and a deploy that looks hung. Check
    `pg_stat_activity` for an in-flight sweep before promoting one, and
    do NOT raise retention to make the sweep easier: that makes it
    zero-work and the floor stops moving at all. A TRANSACTIONAL
    migration that fails rolls back and writes no history row and the
    next pod retries; a non-transactional one half-applied needs
    `flyway repair` before any replica can validate again.
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
  linear in the namespace. Measured on a 54k-table namespace (270k
  files / 270k change rows, PG18, warm, serial):
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
- **The request path never blocks the event loop, and the probe never
  borrows a request connection** (#218). `App.module` installs ONE
  interceptor (`api/BlockingDispatch.kt`) that runs the rest of the
  pipeline — routing, the handler, serialization — under a bounded
  dispatcher of `HOGLAKE_REQUEST_THREADS` threads, default
  `HOGLAKE_DB_POOL_SIZE`. One seam, not ~90 `withContext` calls, because
  the failure mode of the per-handler form is a new route that looks
  like its neighbours and reintroduces the incident with nothing
  redding. The seam lives in its OWN PHASE inserted after `Plugins`, not
  in `Plugins`: same-phase interceptors run in registration order, so at
  `Plugins` it shed AHEAD of `RequestId` and the 503 went out with no
  `X-Request-Id` — the response class most likely to be investigated
  being the one that could not be. `/healthz`, `/livez` and `/metrics`
  BYPASS the dispatcher (`PROBE_PATHS`, matched on a path normalised for
  trailing slashes): they are the surfaces an operator reaches for
  exactly when the queue is longest.
  Excess requests QUEUE, and THE QUEUE IS BOUNDED BY AGE, NOT DEPTH. The
  wait is FROZEN at admission (`RequestAdmission.admit`), not read live,
  and the difference is not cosmetic: elapsed-since-dispatch grows with
  THE HANDLER'S OWN EXECUTION, so `POST .../maintenance/compact` — a
  synchronous sweep on the handler thread — would take the lock with the
  1 s floor for every group after the first 30 s of the run and 503
  blaming a queue wait that never happened. One budget, charged once:
  the shed happens before a connection is borrowed, and the remainder is
  charged against the advisory-lock bound
  (`RequestAdmission.remainingLockTimeoutMs`, floored so a request never
  arrives with a useless remainder and never raised above a bound an
  operator set lower).
  TWO BOOT REFUSALS. `HOGLAKE_REQUEST_THREADS > HOGLAKE_DB_POOL_SIZE` is
  refused naming both knobs — handler threads above the pool queue
  inside `getConnection` and 500 after Hikari's 5 s bound instead of
  queueing in the dispatcher, which is the same argument
  `HOGLAKE_COMPACTION_PARALLEL_GROUPS`' boot check makes — as are a
  dispatcher of zero threads, a probe timeout under Hikari's own 250 ms
  floor, and a call group of zero. The default reserves NOTHING for the
  background loops: on the API workload the chart turns them all off,
  but a pod that runs loops and serves traffic should set
  `HOGLAKE_REQUEST_THREADS` below the pool by the number of loops it
  runs. SHUTDOWN ORDER IS PART OF THE FIX: `Main.kt` stops the ENGINE
  first and only then closes the loops and the pools, and both new pools
  close with `shutdown()` + a bounded wait rather than `shutdownNow()` —
  interrupting an in-flight commit to save a second of shutdown is the
  behaviour #218 complains about.
  The incident narrative, the Netty call-group floor, the probe's own
  pool and threads, and the full pool/dispatcher metric roster are in
  server/README.md §Background assembly and §Observability, which is the
  single home for them.
  The Ktor TEST ENGINE cannot see any of this: it has no call group and
  no event loop, so a blocked handler starves nothing and `/healthz`
  answers whether or not the fix exists. The property is pinned against
  the REAL Netty engine on a real port
  (`api/RequestDispatchIntegrationTest`), and any future change to
  request dispatch belongs there too.
- **Background loops are coroutines**: every periodic job (hydrator,
  expiry, cleanup, compaction, retirement, metrics sampler)
  registers with
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
  `HOGLAKE_MAINTENANCE_LEDGER_RETENTION_SECONDS` (7d default), and
  commit receipts past `HOGLAKE_RECEIPT_RETENTION_SECONDS` (7d, #240/V24
  — floored per catalog at twice its snapshot retention and capped at
  30 days, because a receipt purged inside a client's replay window
  turns that replay into a duplicate publication with nothing reporting
  it). Read
  side: per-catalog `GET .../maintenance/status` + `/runs` and the
  instance-wide `GET /v1/maintenance/status` + `/runs` twins
  (MaintenanceStatusService; batched reads independent of catalog count).
  Dashboard and partition-debt requests read persisted asynchronous
  summaries, NEVER the manifest — and since #232 that includes the TABLE
  GET's three totals, which sum the published generation's tier rows
  (`TierTotalsRepo.PER_TABLE_SQL`, the one per-table sum; the two
  per-partition-group readers share only its published-generation
  predicate, `TierTotalsRepo.PUBLISHED_GENERATION_JOIN`). Invariant 7 has
  the full contract, including what the response says about freshness and
  which endpoint is still exempt. `MaintenanceSummarySampler` checkpoints
  keyset pages in `(catalog, table, file_size_bytes, file_id)` order —
  the order compaction bin-packs in (V10 indexes it) — carrying each
  bucket's partial group across pages in `pending_max_bytes`. Default row budget 10,000/tick, interval 1s,
  refresh delay 60s after a completed scan (`HOGLAKE_MAINTENANCE_SUMMARY_*`).
  Incomplete generations are never published; old samples remain visible
  with freshness timestamps. Expiry overtaking a scan restarts it. The
  central status endpoint pages catalogs (50 default, max 100).
  - **Retirement** (`MaintenanceTask.RETIREMENT`, loop `retirement`,
    invariant 11) is the loop that deletes a dropped table's file rows
    and queues their objects.
    `HOGLAKE_RETIREMENT_INTERVAL_MS` is **0 = off** by default, the
    position compaction takes and for a sharper reason: a
    batch TAKES THE PER-CATALOG COMMIT LOCK, hundreds of times per run,
    so on an API replica it would tax the commit tail it serves. The
    chart turns it on for the maintenance Deployment alone. There is no
    manual trigger and no `POST /maintenance/retire`: the loop is the
    only driver, deliberately, because a trigger reaches every replica.
    The knobs, and every one of them is a TIME knob:
    `HOGLAKE_RETIREMENT_BATCH` (**8,000** rows per transaction, a
    ~160 ms lock hold at the measured 19.6 us/row and 2,902 B of WAL per
    row — `RetirementCostIntegrationTest`, on a fixture with the
    production 9:1 stats ratio; the time moves with the machine and the
    WAL does not, and what the test asserts is that both are FLAT in the
    batch size), `HOGLAKE_RETIREMENT_PAUSE_MS` (**750**,
    so an 18-25% duty cycle on the lock — a foreground commit's p99 tax
    is about one hold), `HOGLAKE_RETIREMENT_RUN_BUDGET_MS`
    (**60,000** of wall clock per run per catalog, so a 50M-row table
    is paced by the interval rather than by one continuously-held run
    — there is no cursor to lose, the victim select is just "what is
    still live on this dropped table"), and
    `HOGLAKE_RETIREMENT_QUEUE_CEILING` (**500,000** undrained
    `hog_file_removal` rows, above which a run declines to start;
    ~125 hours of drain at the STANDING cleanup defaults, which makes
    it a circuit breaker there, and ~50 minutes at the runbook's event
    settings of 10,000 per 60 s, which makes it a throttle). IT IS
    CHECKED ONCE PER RUN, so the EFFECTIVE CAP IS ABOUT TWICE IT: a run
    that starts just under the ceiling commits ~528,000 more rows
    before the next one can stop it, so 500,000 admits a peak of
    ~1,030,000 undrained rows — about **631 MB** of `hog_file_removal`
    at 631 bytes per undrained row, not the ~315 MB the number alone
    suggests. Size storage against the doubled figure. A run the
    ceiling stops STILL STAMPS `retirement_eligible_at` first, so a
    wedged retirement is dateable rather than silent — the reader was
    `/verify`'s orphans arm and #261 removed it, so the column is
    write-only until a scrubber replaces that check; and `HOGLAKE_RETIREMENT_QUEUE_CEILING=0` with the
    loop ON is REFUSED AT BOOT, because every run would skip forever
    with nothing saying so.
    THE ELAPSED TIME OF A LARGE RETIREMENT IS SET BY THE CLEANUP
    DRAIN, not by this loop's knobs, and the arithmetic is worth doing
    before touching either. One 60 s run is ~66 batches (a ~160 ms hold
    plus a 750 ms pause each, and the pause is charged against the
    budget because the budget is wall clock), so at
    `HOGLAKE_RETIREMENT_BATCH` 8,000 a run retires **~528,000 rows**
    and, at a 60 s interval plus a 60 s budget, lands every ~120 s —
    **~15.8M rows/hour of CAPACITY**. The drain is 600,000 paths/hour
    at the event settings and 4,000/h at the standing ones, so
    retirement outruns it by more than an order of magnitude and
    `HOGLAKE_RETIREMENT_QUEUE_CEILING` is what actually paces it:
    bursts of ~60 s of holds, once per ~50 minutes of drain. Elapsed
    time for a 3M-row table is `queued paths / cleanup rate` — **~5
    hours** at the event settings — and shortening the interval or
    lengthening the budget does not move it. The only lever is
    `HOGLAKE_CLEANUP_BATCH` / `HOGLAKE_CLEANUP_INTERVAL_MS`. The candidate
    read and the eligibility stamp are bounded at
    `RetirementService.MAX_TABLES_PER_RUN` (1,000) tables per run,
    ordered by `table_id` so the bound is a stable PREFIX: a mass drop
    on a 54,000-table catalog must not make a run that the budget stops
    after a handful of tables read and stamp all of them first.
    SINGLE FLIGHT per catalog through a SESSION advisory lock on its own
    lock class (`Locks.tryAcquireCatalogRetirementLock`): a second
    maintainer SKIPS rather than queues, because Postgres's lock queue
    is FIFO so W maintainers tax every foreground commit by
    `(W - 0.5) x hold` and the work is idempotent. A batch sets its own
    transaction-local `statement_timeout` —
    `min(session statement_timeout / 4, admission / 2)`. A batch it
    CANCELS IS COUNTED AND NOT RESIZED (#263): every batch of every
    table of every run runs at `HOGLAKE_RETIREMENT_BATCH`, the cancelled
    one rolls back whole, the table is left for the next run, and that
    run asks for the same rows at the same size. THE PER-ROW COST IS
    FLAT IN THE BATCH SIZE (`RetirementCostIntegrationTest`: 19.5 µs/row
    at 2,000 and 19.6 at 8,000), so a batch that crossed the bound
    crossed it COLD rather than big — and the cancelled statement warmed
    exactly the pages it touched, which makes the retry the cheap case.
    This loop used to halve instead, and that locked in a size chosen by
    the worst page of the night: on millpond-prod-us a 13.9M-row
    retirement walked from 8,000-row batches to 62 in its first cold
    hour and stayed there for a day (~4,700 rows per 60 s run, ~280k
    rows/hour against the ~15.8M/hour the defaults size for, the commit
    lock idle between ~0.1 s holds), and when the pod was restarted at
    18:04 UTC on 2026-10-02 at a configured 1,000 it retired 36,000 rows
    in its first run and was back at 62 by 18:19. THE ONE CASE THAT
    NEEDS A HUMAN is a table that times out run after run — its per-row
    cascade does not fit the bound at this batch, and no retry will
    change that — so alert on
    `hoglake_retirement_consecutive_timeouts{catalog,table}`
    (`RetirementGauges`, a MultiGauge whose row exists only between a
    table's first timeout and its next NON-TIMEOUT outcome, so the
    series RETIRES on recovery and `>= 3` needs no `for` clause) and
    lower `HOGLAKE_RETIREMENT_BATCH` on the instance that retires that
    catalog — the knob is process-wide.
    `hoglake_retirement_timeouts_total` is the per-run rate and is
    deliberately NOT the alert: an occasional tick is a cache miss. The
    cost of not retrying in-run is stated rather than hidden: an
    intermittently cold table forfeits the rest of ONE run's budget per
    timeout, which on a one-table catalog is the whole run, and that is
    accepted — re-attempting on a busy RDS to exploit a maybe-warm page
    needs its own backoff and attempt cap for a hope rather than a plan.
    CleanupService's hold-budget
    arithmetic does not apply and no longer exists at all: a retirement
    connection makes no object-store calls, so it is never idle in
    transaction, and the drain those bounds were written for holds no
    lock any more (invariant 4). A batch deletes DELETION VECTORS
    FIRST, by `data_file_id`, with NO `end_snapshot` clause — the
    data-file delete cascades `hog_delete_file`, so any DV left when it
    runs is taken away UN-QUEUED and its puffin leaks forever (bug hunt
    #16's exact failure, carried forward).
  - **Retirement feeds cleanup, and the two are coupled by the queue
    ceiling.** Every row a retirement batch deletes queues one path, and
    the drain is much the slower of the two (`HOGLAKE_CLEANUP_BATCH` per
    `HOGLAKE_CLEANUP_INTERVAL_MS`, against a sub-batch that has to pay
    one `DeleteObjects` round trip — times `HOGLAKE_CLEANUP_WORKERS`,
    since the batch is a PER-WORKER budget and a worker's rate no longer
    costs anyone the commit lock). Unpaced, a 3M-row table converts a
    bounded metadata problem into a 3M-row queue — ~1.9 GB of ledger at
    the measured 631 bytes per undrained row — so a run whose catalog is
    already over `HOGLAKE_RETIREMENT_QUEUE_CEILING` declines to start.
    That count is ONE count per RUN, never one per batch: at 3M queued
    rows it is ~100k buffers, and per batch it would be that times 376.
    The other direction of the coupling is expiry's: retirement DELETES
    rows rather than end-snapshotting them, so it adds nothing to
    expiry's below-floor sweep — but a dropped table's rows sit in
    `hog_data_file` until it runs, which is what V19's partial index and
    the gauges' `hog_table` join both exist to survive.
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

- **Compaction (M4) — 100% implemented**, so what belongs here is only
  what is still deferred. The design — one-pass bin packing, the scaling
  minimum, the two knobs, the claim, the sorted-heap bound, the typed
  skips and their measurements — is in server/README.md §Sort orders and
  compaction, the single home for it.
  Remaining rewrite deferrals (all surface as `unconvertible_schema`
  skips, never wrong bytes): INT96, decimal-scale changes, and
  non-native time(stamp) units — each timestamp type accepts only the
  unit its own files carry (millis for `timestamp_s`/`timestamp_ms`,
  micros for `timestamp`/`timestamptz`, nanos for `timestamp_ns`), and
  `time` stays micros-only. No unit CONVERSION exists, because no legal
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
  deferred-stats files get checked — still a known gap, and nothing
  else covers it: the verify endpoint was metadata-only by design and
  could not do the S3 footer reads this check needs, and #261 removed it
  anyway.
- **Maintenance verify — REMOVED (#261, 2026-10-01)**: `POST
  /v1/catalogs/{c}/maintenance/verify`, its background loop
  (`HOGLAKE_VERIFY_INTERVAL_MS`), its `hoglake_verify_violations` /
  `hoglake_verify_errors_total` metrics, its console panel and its
  twelve checks are gone, along with `VerifyService`,
  `VerifyQueryPlanIntegrationTest` and `VerifySpecParityTest`.
  WHY, and it is the Scale doctrine's own argument: a run was twelve
  UNBOUNDED full-table checks in ONE REPEATABLE READ transaction. On
  gigahog-prod-us (14M live `hog_data_file` rows, 371M
  `hog_file_column_stats` rows) it crossed the 60 s `statement_timeout`
  every run, so it was disabled there (`verifyIntervalMs: "0"`) and
  proved nothing at the only scale that mattered — a checker that is
  green because it never finished is worse than no checker. Every rule
  in [Scale doctrine](#scale-doctrine-read-before-touching-a-query-a-loop-or-a-lock)
  applied to it: no bound per run, no bound per transaction, no
  per-row figure, a fixture of thousands against a manifest of
  millions.
  WHAT CAME WITH IT. `MaintenanceTask.VERIFY` STAYS in the enum
  (`hasLoop = false`) and in the spec's `MaintenanceTask`, because
  `hog_maintenance_run.task`'s CHECK (V2, frozen chain) still admits
  `'verify'` and `MaintenanceRunStore.runMapper` errors on an unknown
  task — deleting the value would 500 every unfiltered
  `GET /maintenance/runs` for as long as one historical row survives
  `HOGLAKE_MAINTENANCE_LEDGER_RETENTION_SECONDS`. The spec's
  `VerifyReport`/`VerifyCheck` schemas and the console's renderer for a
  `verify` ledger row stay for the same reason, marked HISTORICAL; they
  go once the retention window has passed everywhere.
  `HOGLAKE_VERIFY_INTERVAL_MS` is in `Config.REMOVED_INTERVAL_ENV`, a
  middle tier between `REMOVED_ENV` (any value refused) and ignoring a
  knob: a POSITIVE value is refused at boot naming #261, while `0` and
  absence are accepted in silence. `0` is what the gigahog chart renders
  — unconditionally, from a REQUIRED `verifyIntervalMs`, with no override
  anywhere under `argocd/gigahog/values/` — so a `REMOVED_ENV`-style
  refusal would reject every pod over a value that means exactly what is
  now true. (It would not take production down: the chart sets
  `maxUnavailable: 0`, so the old ReplicaSet keeps serving and the
  failure is a STALLED ROLLOUT plus a Degraded Application paging
  `#alerts-managed-warehouse`, in dev only — prod is behind
  `require_prod_approval` and the `prod-promote-managed-warehouse`
  reviewer gate.) A positive value is refused because it is an operator
  asking for invariant checks that no longer exist, and silence there
  would leave them believing a catalog is being scrubbed.
  `RemovedEnvConfigTest` pins both tiers; neither had a test before.
  The entry graduates into `REMOVED_ENV` once the chart stops rendering
  the key at all.
  WHAT IS NOW UNWATCHED. All twelve checks are gone, and the list is
  exhaustive rather than a sample: `row_id_tiling`, `delete_vectors`,
  `orphans`, `removal_queue`, `snapshot_density`, `next_row_id`,
  `expiry_floor`, `visibility_bounds`, `offset_release`,
  `staging_tickets`, `upload_claims`, `compaction_claims`. THREE of those
  twelve have another enforcer and their absence here is defensible:
  `removal_queue` is enforced at drain time by
  `CleanupService.referencedPaths` and its `still_referenced` counter;
  `next_row_id` by the commit-path allocator that owns the column; and
  `orphans`' retirement arm by the stamp + ledger counters in the
  retirement bullet above. The other nine have no at-rest enforcer in any
  environment. Four are now asserted in the TEST suite only, by
  `testing/CatalogInvariants.kt`: `visibility_bounds`, `removal_queue` at
  the compaction boundary, `snapshot_density` (all 8 concurrent-commit
  sites in `CompactionParallelIntegrationTest`), and `staging_tickets`
  arm (c) — rebuilt because they were the oracle's only falsifiable arms
  at those call sites and nothing else covers them. The versioned-table
  schema-drift guard that rode in the deleted verify test moved to
  `ExpiryServiceIntegrationTest` (`VERSIONED_RETENTION_TABLES is every
  versioned table the schema has`), where it never needed a verify symbol.
  Three artefacts are now written and read only by tests, kept because
  none can be reconstructed after the fact:
  `hog_table.retirement_eligible_at` (no reader at all), and
  `hog_file_removal`'s `'absent'` outcome plus the HEAD-before-DELETE
  carve-out that produces it — read only by `CatalogInvariants`.
  FOLLOW-UP TO DECIDE, not a standing property: that carve-out costs two
  round trips per staging path, which is what forces
  `CleanupService.STAGING_SUB_BATCH = 25` instead of the full sub-batch,
  and at the ~9.4k orphaned tickets / 50 per hour that file records it is
  an eight-day drain. Keep it for the scrubber or drop it and lose the
  outcome forever — somebody has to choose. `HOGLAKE_CLEANUP_STAGING_GRACE
  _SECONDS` also lost its only ceiling (verify's 6 h ticket age) and has
  none today; a `require` bounding it against the cleanup interval would
  restore it as code rather than prose.
  The replacement is a PAGED, RESUMABLE SCRUBBER — one bounded page per
  transaction, a cursor, a run budget, a per-row figure measured on a
  production-shaped fixture — tracked as issue #261.
- **DuckDB client (`duckdb-client/`)**: complete through time travel
  and maintenance functions, verified against the live dev stack, but
  NOT yet in CI and not yet released as of 2026-09-30 (four server
  releases into that state) — no path-scoped workflow, and its
  `duckdb` / `extension-ci-tools` trees are gitignored pinned clones
  rather than submodules (the repo root was out of the branch's write
  scope; they become submodules on extraction to a standalone
  community-extension repo). Its DESIGN.md ends with numbered
  **findings for the server**; the two still open are no
  `explicit_row_ids` on `FileRegistration` (#30) and head-only listings
  (#28), each capping a parity item.
- **CDC publications to Kafka (the WAL tap)**: fully specified in the
  OpenAPI (501s) + README; not implemented.
- **DR/export**: `GET /v1/catalogs/{c}/export` specified in the OpenAPI
  (snapshot range + live-file manifest + consumer offsets, consistent
  at head); 501 stub until built.
- **Iceberg REST facade + Trino**: design obligations in
  [docs/iceberg-federation.md](docs/iceberg-federation.md) /
  [docs/trino-integration.md](docs/trino-integration.md); v1 schema already
  conforms (typed bounds, Iceberg transforms, DV-only deletes).
- **Auth**: out of scope for v1; audit actor is `anonymous` until it
  lands. README §Status says the same and there is no decision record
  beyond it.
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

**Design** — [server/schema.sql](server/schema.sql) (the schema, table
by table, with each index's rationale beside it) ·
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
