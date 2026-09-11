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

## M1 — scaffold + ATTACH + catalog listing: IN PROGRESS
- [ ] Makefile/CMake/extension_config/vcpkg.json + fetch-deps.sh
- [ ] builds against pinned duckdb
- [ ] ATTACH 'hoglake:http://localhost:8080/<catalog>'
- [ ] SHOW TABLES / DESCRIBE against live dev server
- [ ] sqllogictests under `make test`

## M2 — read path: NOT STARTED
## M3 — write path: NOT STARTED
## M4 — update/delete/alter/drop: NOT STARTED
## M5 — time travel + metadata functions + parity checklist: NOT STARTED
