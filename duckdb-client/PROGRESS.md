# duckdb-client progress

Working tree: git worktree `~/src/hoglake-duckdb-client`, branch
`jakob/duckdb-client` (from origin/main). Never pushed.

## M0 — research memo + build plan: DONE
- Read: ducklake-api-map.md, duckdb-read-extension.md, hoglake.yaml,
  schema.sql, pyhoglake (client/types/stats/bounds), server
  PuffinDeletionVector.kt, ducklake-fork build layout.
- DESIGN.md written: architecture, module map, REST client decision
  (duckdb-vendored cpp-httplib + yyjson), transaction/pinning model,
  full DuckLake-parity table with wire gaps, test strategy, server
  findings.
- Key pins: duckdb ac5c6d11c3b0915fcc0fa43080d31893269d6448,
  extension-ci-tools 795096d04b009c0d087468439ebb526a5460dfac
  (both = ducklake-fork's pins). vcpkg at ~/.vcpkg.

## M1 — scaffold + ATTACH + catalog listing: DONE
- [x] Makefile/CMake/extension_config/vcpkg.json + scripts/fetch-deps.sh
      (deps are gitignored pinned clones, not submodules — repo-root
      .gitmodules is out of write scope; becomes submodules on
      extraction to a standalone repo)
- [x] builds against pinned duckdb ac5c6d11c3 (make release GEN=ninja;
      no vcpkg needed until M2's roaring)
- [x] ATTACH 'hoglake:<catalog>' (ENDPOINT '...') — catalog NAME, not
      URI (URI payloads trip duckdb's remote-file/httpfs detection);
      SET hoglake_default_endpoint fallback; CREATE_IF_NOT_EXISTS +
      DATA_PATH create-on-attach; SNAPSHOT_VERSION/SNAPSHOT_TIME parsed
      (enforced read-only; full time-travel semantics in M5)
- [x] SHOW TABLES / DESCRIBE work against the live dev server
      (snapshot-pinned transactions; lazy per-schema/table entry cache;
      NOT NULL round-trips — DESCRIBE needed get_bind_info on the stub
      scan to trace constraints)
- [x] eager DDL: CREATE SCHEMA [IF NOT EXISTS], CREATE TABLE
      [IF NOT EXISTS], DROP TABLE (each is its own server snapshot;
      documented divergence: not rolled back by transaction ROLLBACK)
- [x] sqllogictests under `make test` (skip cleanly without
      HOGLAKE_URL; full run needs the live dev stack)
- Scan is a bind-only stub that throws on execute (M2).
- Verified: `HOGLAKE_URL=http://localhost:8080 make test` → all pass
  (37 assertions) on 2026-09-11 against the live dev server.

## M2 — read path: DONE
- [x] SELECT over tables: parquet_scan cloned with a
      HoglakeMultiFileReader (ducklake's pattern); file list from
      GET /scan at the pinned snapshot; field-id column mapping
      (BY_FIELD_ID; BY_NAME fallback for id-less registered files)
- [x] snapshot pinning per transaction (M1's pin feeds the scan)
- [x] partition pruning: ComplexFilterPushdown evaluates pushed
      filters against identity-transform partition_values via
      TableFilter::ToExpression + constant folding; files under a
      non-live spec never pruned; conservative on any doubt.
      Verified: filter on a partitioned column changes the plan's
      file list (~0 rows when every partition prunes)
- [x] deletion vectors: puffin deletion-vector-v1 reader (roaring via
      vcpkg; hoglake requires a real container with exactly ONE DV
      blob — bare-blob and multi-blob forms are ducklake-isms) +
      HoglakeDeleteFilter positional mask. NOT yet exercised by tests:
      nothing writes DVs until M4 (pyhoglake is append-only) — M4's
      DELETE round-trips it
- [x] rowid virtual column: row_id_start + file_row_number, or the
      physical _hog_row_id column (field id 2147483646) for
      explicit_row_ids compaction outputs (untested until a compacted
      fixture exists); filename/snapshot_id/file_row_number virtual
      columns
- Build now needs vcpkg (roaring) and, for the integration tests,
  httpfs: `VCPKG_OVERLAY_TRIPLETS=$PWD/vcpkg-triplets
  VCPKG_TOOLCHAIN_PATH=~/.vcpkg/scripts/buildsystems/vcpkg.cmake
  BUILD_EXTENSION_TEST_DEPS=full make release GEN=ninja`.
  The overlay triplet pins HAVE_PIPE2=0 (macOS SDK declares pipe2 as
  macOS-27; curl's detection otherwise breaks the vcpkg build).
- Fixtures: test/fixtures/read_fixture.py (run via pyhoglake:
  `cd ~/src/hoglake/pyhoglake && HOGLAKE_S3_ENDPOINT=http://localhost:9000
  uv run python .../read_fixture.py`) builds duckext-read/ns1.points
  (2 batches) and ns1.part_points (identity(team), 4 files incl. null
  partition).
- Verified: `HOGLAKE_URL=http://localhost:8080
  DUCKEXT_S3_ENDPOINT=localhost:9000 make test` → 101 assertions pass
  (2026-09-11, live dev stack; MinIO is on port 9000, not 19000).
## M3 — write path: DONE
- [x] INSERT: child PhysicalCopyToFile (parquet with hoglake field ids
      via the field_ids option, hive-partitioned under the live spec,
      rotating at 512MB) -> HoglakeInsert sink consumes
      WRITTEN_FILE_STATISTICS rows into buffered FileRegistrations;
      footer_size is the copy's footer_size_bytes (== thrift length,
      the bugs.md #7 convention)
- [x] footer-shipping commit at COMMIT: one CommitRequest for all
      buffered (multi-table) appends -> atomic snapshot; blind append
      (no read_snapshot); expected_table_uuid incarnation guard rides
      every append; ROLLBACK drops registrations (uploaded parquet
      orphaned by design)
- [x] OCC retry loop: 409 conflict retries with backoff
      (hoglake_max_retry_count/_retry_wait_ms/_retry_backoff), the
      "the table was recreated" 409 never retries, 503
      commit_queue_timeout honors Retry-After
- [x] stats_mode=deferred: commits ship no column_stats; the server
      hydrator reads footers async (stats_state pending->provided).
      Client-side NOT NULL enforcement from written null counts
      (note: parquet stats keys are QUOTED column paths — unquote)
- [x] CTAS (eager create at plan time; documented caveat: re-executing
      a cached prepared plan re-runs the create)
- [x] partitioned INSERT: identity transforms only; partition_values
      re-encoded from hive partition keys to pyhoglake's wire_string
      conventions (boolean/int/long/string/date/timestamp; other
      identity types and bucket/year/month/day/hour transforms throw
      NotImplemented — port of transforms + murmur3 is the follow-up);
      verified manually against part_points incl. the NULL group
- Read-your-own-writes within a transaction is NOT implemented:
  uncommitted inserts are invisible to the transaction's own scans
  (ducklake shows transaction-local files; documented divergence,
  candidate for M5+)
- Verified: full suite 143 assertions pass (3 test files) against the
  live dev stack 2026-09-11.
## M4 — update/delete/alter/drop: DONE
- [x] DELETE via deletion vectors: sink groups (filename,
      file_row_number) per data file, merges with the file's live DV
      (vectors only grow), writes a superseding puffin container
      (HoglakePuffin::WritePuffinFile — server-compatible: one
      uncompressed deletion-vector-v1 blob), buffers
      DeleteFileRegistrations; commit carries read_snapshot (mandatory
      with deletes). A delete-commit 409 is NOT auto-retried (the DV
      must be rebuilt) — the error says re-run. Round-trips the M2 DV
      read path (surviving rowids stay positionally stable)
- [x] UPDATE = delete + insert (HoglakeUpdate streams updated values
      into the insert copy while sinking old row ids into an embedded
      HoglakeDelete; BindUpdateConstraints sets
      update_is_del_and_insert and projects all columns). DIVERGENCE:
      updated rows get NEW row ids — FileRegistration has no
      explicit_row_ids, so clients cannot preserve row identity
      (ducklake preserves via _ducklake_internal_row_id; wire finding)
- [x] ALTER: ADD/DROP COLUMN, RENAME COLUMN/TABLE, ALTER COLUMN TYPE
      (server promotion lattice), SET PARTITIONED BY (identity +
      year/month/day/hour + bucket parsed; writes still identity-only),
      SET SORTED BY (DDL only; sort-on-insert not implemented).
      Eager one-op /alter commits; altered entries swap in the cache
      (old entry retired, not freed). SET/DROP NOT NULL, SET DEFAULT,
      nested-field ops -> NotImplemented (wire gaps)
- [x] DROP TABLE was M1
- Environment finding: the dev stack's hydrator is not hydrating ANY
  deferred-stats files (pyhoglake deferred probe also stays 'pending'
  forever) — so RENAME COLUMN on tables with live extension-written
  files 409s (idless_files_present names not-yet-hydrated files).
  Surfaced cleanly; the sqllogictest renames on an empty table.
- Verified: full suite 214 assertions pass (4 test files) 2026-09-11.
## M5 — time travel + metadata functions + parity checklist: DONE
- [x] AT (VERSION => n) / AT (TIMESTAMP => t): per-lookup table
      entries fetched at the historical schema (cached per travel),
      scans plan at the entry's travel, writes refused; out-of-range /
      below-floor errors surface cleanly. Attach-level
      SNAPSHOT_VERSION/SNAPSHOT_TIME verified
- [x] hoglake_snapshots (paginated — never the full catalog in one
      response), hoglake_table_info, hoglake_current_snapshot
- [x] maintenance passthroughs: hoglake_expire/compact/cleanup
      (batch => n) + hoglake_verify, returning the server's JSON
      report (verify: status pass asserted in tests)
- [x] PARITY.md: full DuckLake-capability checklist with
      DONE/PARTIAL/WIRE GAP/N-A/TODO statuses
- Compaction on the dev stack returns 0 groups (pending-stats files;
  same hydrator env gap) — the explicit _hog_row_id read path remains
  untested against real compaction output.
- Verified: full suite 237 assertions pass (5 test files) 2026-09-11.

## Review round 1 — all 14 confirmed findings fixed, 7 overflow items assessed: DONE
- Dispositions with tests: REVIEW-FINDINGS.md ("Round-1 fix
  dispositions"). Highlights: per-data-file DV merging across
  statements (multi-statement DML transactions commit), recursive
  mutexes + retire-not-destroy on the catalog caches (SHOW ALL TABLES
  race repro now 6/6 green), listing-based CI identifier resolution
  (DuckDB case semantics over the case-sensitive server), CTAS cast
  projection to wire types (silent-corruption repro fixed), genuine
  time-travel tests via fixture-exported snapshot ids, eager-DDL vs
  buffered-writes refusal policy + post-DDL read travel (DESIGN.md
  "Transactions and eager DDL"), pyhoglake-byte-identical
  date/timestamp partition wire strings verified cross-client,
  yyjson/puffin/retry-clamp hardening, maintenance mutations moved
  from bind to execution, and a PARITY.md/DESIGN.md audit so no claim
  exceeds what tests prove.
- New test surface: hoglake_txn.test, hoglake_case.test,
  hoglake_types.test, test/run-live-tests.sh (fixtures -> suite ->
  verify_partition_wire.py cross-client check), fixture-exported
  live-env.sh for time travel.
- Verified 2026-09-11: ./test/run-live-tests.sh fully green — 8
  sqllogictest files / 406 assertions + 4 cross-client partition
  groups byte-shared.

## Review round 2 — all 13 confirmed findings fixed, 5 overflow lows assessed: DONE
- Dispositions: REVIEW-FINDINGS-R2.md ("Round-2 fix dispositions").
  Headlines: read-your-own-DELETES via scan-side buffered-DV merge
  (both live corruption repros — resurrect and double — now commit
  correct data), instant-semantics timestamp parsing on AT()/
  SNAPSHOT_TIME (explicit offsets never dropped; tested at +02:00 on
  both paths), symmetric DML-after-DDL refusal rules (ALTER-then-
  DELETE self-conflict closed; ALTER-then-INSERT still commits),
  CI-conflict checks on every ALTER target + unrepresentable-table
  containment (listings survive), namespace ambiguity policy,
  read-only refusal for mutating maintenance (pin-forced included),
  failed-commit transaction-map leak fix, puffin range-check UB fix,
  base64 BLOB partition decode, GetSnapshot-under-time-pin honesty,
  and a permanent 8-round concurrency regression test for the round-1
  UAF fix. Round-1 disposition #12's false claim corrected in place.
- Verified 2026-09-11: ./test/run-live-tests.sh fully green — 9
  sqllogictest files / 515 assertions + 4 cross-client partition
  groups byte-shared.

## Review round 3 — all 13 confirmed + 1 unverified finding fixed: DONE
- Theme: InternalException = instance death. Full 45-site NumericCast/
  LogicalType sweep dispatched (parse-time wire bounds via
  GetBoundedInt, decimal params validated naming the table, typed
  errors for every user-supplied number); listing containment widened
  (both throw layers, table_info too) with remembered
  unrepresentable-table errors (order-independent lookups, loud DROP
  IF EXISTS); session-TimeZone semantics actually implemented
  (context-bearing TIMESTAMPTZ cast); partial-overlap DML silent
  wrongness closed by refusing DELETE/UPDATE after own inserts;
  reserved _hog enforced on ALTER targets; create-catalog refused on
  read-only/pinned attaches; duplicate-path DELETE registers the DV
  for every live data_file_id (unverified finding confirmed+fixed).
  Dispositions + R2 corrections: REVIEW-FINDINGS-R3.md / -R2.md.
- Environment: the dev stack's hydrator/compactor revived — fixture
  force-compacts points and hoglake_compacted_read.test now reads a
  REAL compaction output (explicit _hog_row_id, row ids preserved);
  compaction-fragile layout asserts pinned via time travel.
- Verified 2026-09-12: ./test/run-live-tests.sh fully green — 11
  sqllogictest files / 592 assertions + cross-client wire check.

## Review round 4 — all 11 confirmed findings fixed: DONE
- Corrected sweep rule (engine-internal only if BOTH sides engine-
  derived) re-run: typed refusals for out-of-range external DV/
  registration data in the delete sink (refuse loudly, never clamp),
  Table-schema parse bounds (the omitted struct), puffin encode size
  guard. Exception taxonomy unified (transport IOException propagates;
  wire-data InvalidInput/CatalogException containable) so the
  containment pair covers every data-shaped throw. Per-copy DV
  attribution via rowid base for duplicate-path registrations (rowid
  predicates grow only the matching copy; ambiguous shapes refuse
  typed); server duplicate-target 422 verified compatible (one
  registration per data_file_id). Discriminating explicit-rowid test
  via sort-permuted compaction (points_sorted); stale _hog header
  comment fixed. Dispositions: REVIEW-FINDINGS-R4.md.
- Verified 2026-09-14: ./test/run-live-tests.sh fully green — 11
  sqllogictest files / 630 assertions + cross-client wire check.
