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

## M2 — read path: NOT STARTED
## M3 — write path: NOT STARTED
## M4 — update/delete/alter/drop: NOT STARTED
## M5 — time travel + metadata functions + parity checklist: NOT STARTED
