# DuckLake-extension parity checklist

Status of every DuckLake SQL-surface capability (per
[ducklake-api-map.md](../ducklake-api-map.md)) in the hoglake DuckDB
client. **DONE** = implemented and tested against the live dev stack;
**PARTIAL** = implemented with named limits; **WIRE GAP** = blocked on
the hoglake REST contract (see DESIGN.md server findings); **N/A** =
deliberately not carried over (the fleet-liability list); **TODO** =
client work not yet done.

## Attach / catalog

| Capability | Status | Notes |
|---|---|---|
| `ATTACH` | DONE | `ATTACH 'hoglake:<catalog>' (ENDPOINT ...)`; `SET hoglake_default_endpoint` fallback; `CREATE_IF_NOT_EXISTS` + `DATA_PATH` |
| `SNAPSHOT_VERSION` / `SNAPSHOT_TIME` attach pins | DONE | forced read-only, as ducklake |
| Secrets (`TYPE ducklake`) | TODO | endpoint+token secret type; low-cost once auth exists |
| Multi-catalog attach | DONE | independent catalogs per ATTACH |
| `DETACH` / re-attach | DONE | |
| Identifier case semantics | DONE | DuckDB CI identifiers over the case-sensitive server: listing-based resolution for tables and columns, case-preserving creates, CI duplicate conflicts on CREATE and on ALTER targets (add/rename column, rename table; self case-change legal), ambiguity errors for case-colliding server-side tables, columns, and namespaces with listings surviving. Fixture-created colliding TABLE and NAMESPACE pairs (`hoglake_case.test`) AND a colliding-COLUMN table (`hoglake_wire_hardening.test`: targeted error, order-independent, listings survive) |
| `metadata_*` options, `busy_timeout`, `automatic_migration`, `ducklake_version`, `override_data_path` | N/A | no client-visible metadata backend |
| `encrypted`, `data_inlining_row_limit` | N/A | encryption/inlining dropped by hoglake's design |

## DDL

| Capability | Status | Notes |
|---|---|---|
| `CREATE SCHEMA` | DONE | eager DDL (own server snapshot; not rolled back) |
| `DROP SCHEMA` | WIRE GAP | no namespace-drop endpoint |
| `CREATE TABLE` (+ IF NOT EXISTS) | DONE | flat hoglake type set only; `_hog` column prefix fast-fail |
| `CREATE TABLE AS` | DONE | eager create at plan time (prepared-statement re-exec caveat) |
| `DROP TABLE` | DONE | no CASCADE (as ducklake) |
| `ALTER TABLE ADD/DROP COLUMN` | DONE | ADD with DEFAULT refused (no wire defaults) |
| `ALTER TABLE RENAME COLUMN/TABLE` | DONE | rename-column 409s while id-less/not-yet-hydrated files are live (server invariant, surfaced verbatim) |
| `ALTER COLUMN TYPE` | DONE | server promotion lattice (int→long, float→double) |
| `SET PARTITIONED BY` | DONE | identity/year/month/day/hour/bucket parsed to the wire spec |
| `SET SORTED BY` | PARTIAL | DDL committed; sort-on-insert NOT applied by the writer yet |
| `SET/DROP NOT NULL`, `SET DEFAULT` | WIRE GAP | not in AlterOp |
| Nested-field ALTERs (`ADD/DROP/RENAME FIELD`) | N/A | flat wire schema |
| `COMMENT ON` | WIRE GAP | no comment/tag storage |
| `CREATE VIEW` / views | TODO/WIRE-SHAPED | wire has views (dialect-tagged); binding duckdb-dialect views is client work not done |
| `CREATE MACRO` / sequences / indexes / constraints beyond NOT NULL | N/A | as ducklake (unsupported there too) except macros |

## DML

| Capability | Status | Notes |
|---|---|---|
| `INSERT` | DONE | parquet via PhysicalCopyToFile (field ids, rotation), footer-shipping commit, OCC retry loop, incarnation guard. Type coverage: every wire type round-trips write-then-read in `hoglake_types.test` (unicode, int64 > 2^53, boolean/date/time/timestamp/timestamptz/decimal/uuid/blob, NOT NULL on VARCHAR) |
| Partitioned INSERT | PARTIAL | identity transforms only; identity wire encodings for boolean/int/long/string/date/timestamp are byte-verified against pyhoglake's wire_string cross-client (verify_partition_wire.py, incl. fractional-second timestamps); float/double/decimal/uuid/binary identity partitions and bucket/year/month/day/hour transforms throw NotImplemented (transforms + murmur3 port is the follow-up) |
| `DELETE` | DONE | puffin DV merge + superseding registration; multi-statement transactions merge per data_file_id (one live DV per file per commit) AND same-transaction scans mask buffered deletes (read-your-own-deletes — no resurrect/double corruption; `hoglake_txn.test`); conflict = re-run (no blind retry) |
| `UPDATE` | DONE | delete + insert; **updated rows get NEW row ids** (wire gap: FileRegistration cannot register explicit-row-id files) |
| Wire/user-input hardening | DONE | no wire- or user-supplied value can reach an InternalException (= DuckDB instance invalidation): parse-time range validation for every numeric wire field, decimal params validated with the table named, typed errors for negative SNAPSHOT_VERSION / AT VERSION / batch / bucket counts, unrepresentable tables skipped from listings with remembered targeted errors (`hoglake_wire_hardening.test`) |
| DML after own INSERT/UPDATE | DONE (refused) | DELETE/UPDATE on a table this transaction already appended rows to is refused (uncommitted inserts are invisible to scans; a partial-overlap predicate would commit sequentially-wrong data with a plausible count — R3-6). DELETE-then-DELETE/UPDATE stay legal (`hoglake_txn.test`) |
| DDL vs buffered writes (both orders) | DONE | eager DDL on a table with uncommitted writes is refused, and the mirror order too: DELETE/UPDATE after an ALTER of the same table (or with buffered appends to an altered table) is refused before it can self-conflict at commit; ALTER-then-INSERT commits (blind); created/altered tables read at the post-DDL snapshot (`hoglake_txn.test`, DESIGN.md "Transactions and eager DDL") |
| `MERGE INTO` | TODO | lowers to the same primitives; not wired |
| `INSERT ... RETURNING` / `ON CONFLICT` | TODO | refused with clear errors |
| NOT NULL enforcement | DONE | client-side from written null counts |
| sort_on_insert | TODO | needs PhysicalOrder in the insert plan from the live sort spec |
| Read-your-own-writes in a transaction | TODO | uncommitted inserts invisible to own scans (ducklake has transaction-local files) |
| Data inlining | N/A | dropped by design |

## Read path

| Capability | Status | Notes |
|---|---|---|
| SELECT / parquet scan | DONE | parquet_scan clone + hoglake MultiFileReader; field-id mapping, by-name fallback for id-less files |
| Deletion-vector application | DONE | puffin deletion-vector-v1 reader (roaring) |
| `rowid` virtual column | DONE | positional path tested (stable across DVs) AND the explicit `_hog_row_id` (2147483646) branch tested against a real fixture-forced server compaction output (`hoglake_compacted_read.test`: one merged file, row ids preserved 0..4) |
| `filename` / `file_row_number` / `snapshot_id` virtual columns | DONE | snapshot_id = file begin_snapshot |
| Partition pruning | DONE | identity transforms, filter constant-folded per partition value; binary partition wire values base64-decoded (pyhoglake encoding; tested on a fixture-written binary partition); non-live-spec files never pruned |
| File-level min/max (zone-map) pruning | WIRE GAP | `/scan` carries no column bounds |
| Global column stats for the optimizer | WIRE GAP | no global stats endpoint |
| Encryption keys | N/A | |

## Time travel / CDC

| Capability | Status | Notes |
|---|---|---|
| `AT (VERSION => n)` / `AT (TIMESTAMP => t)` | DONE | per-lookup entries at the historical schema; genuinely-historical reads tested (fixture-exported snapshot ids: V1 shows 3 of head's 5 rows; a pre-ADD-COLUMN read shows the 1-column schema) |
| Attach-level pins | DONE | SNAPSHOT_VERSION and SNAPSHOT_TIME both tested, including the pin FORCING read-only without an explicit flag (INSERT refused; READ_WRITE+pin refused at attach) and explicit-UTC-offset timestamps parsed with instant semantics on both the AT() and SNAPSHOT_TIME paths; `hoglake_current_snapshot` under a SNAPSHOT_TIME pin errors (no client-resolvable snapshot id — server finding 9) |
| `ducklake_snapshots` | DONE | `hoglake_snapshots(cat)` — paginated (fixes ducklake's full-list OOM; `page_size` param, page-boundary crossing tested); typed change rows (content asserted) |
| `ducklake_table_info` | DONE | `hoglake_table_info(cat)` |
| `ducklake_current_snapshot` | DONE | `hoglake_current_snapshot(cat)` |
| `ducklake_last_committed_snapshot` | TODO | trivial (commit result is already parsed) |
| `ducklake_table_insertions/deletions/table_changes` | TODO/PARTIAL WIRE | `/changes` covers append ranges + DV diffs (insertions + deletions); no update pre/post images on the wire; client function not yet written |
| `ducklake_list_files` | TODO | trivial over `/files` |
| Consumer offsets (no ducklake analogue) | TODO | wire exists; natural companion to a changes function |

## Maintenance / options

| Capability | Status | Notes |
|---|---|---|
| `merge_adjacent_files` | DONE (server-side passthrough) | `hoglake_compact(cat, batch)` — POST /maintenance/compact; refused on read-only attaches. Real compaction exercised: the fixture force-compacts `points` via this endpoint and the suite reads the output (`hoglake_compacted_read.test`) |
| `expire_snapshots` | PARTIAL (server-side passthrough) | `hoglake_expire(cat, batch)`; shape-tested only (no retention configured on the dev catalog, so nothing ever expires in tests) |
| `cleanup_old_files` | DONE (server-side) | `hoglake_cleanup(cat, batch)` — liveness-checked server-side; shape-tested |
| verify (no ducklake analogue) | DONE | `hoglake_verify(cat)` |
| `rewrite_data_files`, `delete_orphaned_files`, `flush_inlined_data` | N/A | server policy / inlining dropped |
| `ducklake_set_option` / `options` / `settings` | TODO | retention PATCH + settings echo are trivial; most ducklake options have no hoglake meaning |
| `set_commit_message` | TODO | author/message fields already ride CommitRequest |
| `CHECKPOINT` composite | N/A | server loops exist |
| `add_data_files` | TODO | client-side footer parse + commit; hoglake assigns row ids so ducklake's NULL row_id_start defect cannot occur |
| `ducklake_commit` (server-side commit) | N/A | every hoglake commit is server-side by construction |
| `murmur3_32` | TODO | needed for bucket-transform writes/pruning |
| Per-catalog table macros | TODO | sugar |
| Plan serialization (`ducklake_scan` serialize) | TODO | serialize callbacks cleared; dependent-context plans will fail loudly |
