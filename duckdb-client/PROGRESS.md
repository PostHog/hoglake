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
## M3 — write path: NOT STARTED
## M4 — update/delete/alter/drop: NOT STARTED
## M5 — time travel + metadata functions + parity checklist: NOT STARTED
