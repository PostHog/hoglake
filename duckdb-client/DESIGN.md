# hoglake DuckDB client extension — design

Target: feature parity with the DuckLake DuckDB extension, speaking
hoglake's REST wire contract (`server/src/main/resources/openapi/
hoglake.yaml`) instead of DuckLake's SQL-catalog protocol. Long-term:
submission as an official DuckDB community extension.

This design began as a read-only sketch and grew to cover writes; the
DuckLake SQL-surface survey it was originally checked against has since
been retired (see the doc index in [AGENT.md](../AGENT.md)).

## Architecture

The DuckLake extension's shape with the metadata layer swapped: its
`storage/` (catalog, table entries, transaction, scan, insert, delete)
maps over conceptually; its `metadata_manager/` (SQL against a metadata
database) is replaced by one typed REST client. Everything downstream
of the file list is stock DuckDB machinery — parquet reader/writer,
httpfs S3 filesystem, filter pushdown.

```
duckdb-client/
├── Makefile                    # extension-ci-tools duckdb_extension.Makefile
├── CMakeLists.txt
├── extension_config.cmake      # loads hoglake + parquet/httpfs/json for tests
├── vcpkg.json                  # roaring (deletion vectors) — same dep set as ducklake
├── scripts/fetch-deps.sh       # pins duckdb + extension-ci-tools (see "Deps")
├── src/
│   ├── hoglake_extension.cpp   # entry: storage extension + functions + settings
│   ├── include/…               # headers mirror src/ layout
│   ├── common/
│   │   ├── hoglake_types.cpp   # hoglake column types <-> LogicalType
│   │   ├── hoglake_bounds.cpp  # Iceberg single-value binary codec (stats bounds)
│   │   └── hoglake_wire.cpp    # DTOs for the OpenAPI schemas + yyjson codec
│   ├── rest/
│   │   └── hoglake_api_client.cpp  # HTTP transport + error taxonomy
│   └── storage/
│       ├── hoglake_storage.cpp       # StorageExtension: ATTACH parsing/options
│       ├── hoglake_catalog.cpp       # duckdb::Catalog impl
│       ├── hoglake_schema_entry.cpp  # namespace -> SchemaCatalogEntry
│       ├── hoglake_table_entry.cpp   # table at pinned snapshot; ALTER dispatch
│       ├── hoglake_transaction.cpp   # snapshot pinning; buffered writes; commit
│       ├── hoglake_transaction_manager.cpp
│       ├── hoglake_scan.cpp          # parquet_scan clone + hoglake MultiFileList
│       ├── hoglake_multi_file_list.cpp
│       ├── hoglake_insert.cpp        # parquet write w/ field ids + footer stats
│       ├── hoglake_delete.cpp        # M4: DV build + registration
│       └── hoglake_puffin.cpp        # puffin deletion-vector-v1 read/write (roaring)
│   └── functions/
│       ├── hoglake_snapshots.cpp, hoglake_table_info.cpp,
│       ├── hoglake_table_changes.cpp, hoglake_current_snapshot.cpp,
│       └── hoglake_maintenance.cpp   # compact/expire/cleanup/verify passthroughs
└── test/sql/…                  # sqllogictests (live dev server + MinIO)
```

## Wire client

**Decision: DuckDB's vendored cpp-httplib (`duckdb/third_party/httplib`)
+ DuckDB's vendored yyjson (`duckdb_yyjson`).** Zero new dependencies;
both already ship in every DuckDB build and are what other in-tree
consumers use. Rejected alternatives:

- *Reuse httpfs's HTTP stack*: not a public cross-extension API;
  couples us to httpfs internals and load order.
- *vcpkg curl/cpr*: real dependency weight and TLS bootstrapping pain
  in the community-extension build matrix, for what is a handful of
  small JSON calls.

TLS: cpp-httplib supports `https://` when compiled with
`CPPHTTPLIB_OPENSSL_SUPPORT`; v1 targets the plaintext dev/in-cluster
endpoint, and the OpenSSL vcpkg feature is a flip documented as a
follow-up (community CI builds openssl for httpfs already). Auth is out
of scope for the server's v1; the client carries an optional bearer
token setting so the slot exists.

Error taxonomy (mirrors pyhoglake `errors.py`): 404 not-found, 409
conflict (commit conflicts retryable; `"the table was recreated"` in
the ApiError text marks the incarnation-guard refusal — never retried),
410 expired (below the expiry floor; surfaced with the floor detail),
422 validation (never retried), 503 `commit_queue_timeout` (retryable
backpressure; honors `Retry-After`). Retries: commit-loop only, default
max 10 / 100 ms / backoff 1.5 with jitter (DuckLake's numbers), settings
`hoglake_max_retry_count` / `hoglake_retry_wait_ms` /
`hoglake_retry_backoff`.

## ATTACH

```sql
ATTACH 'hoglake:analytics' AS lake (ENDPOINT 'http://localhost:8080');
-- or, with a session default:
SET hoglake_default_endpoint = 'http://localhost:8080';
ATTACH 'hoglake:analytics' AS lake;
```

The attach payload is the **catalog name, never a URI** (an early
design call, confirmed the hard way: a URI
payload trips DuckDB's remote-file detection in
`DatabaseManager::AttachDatabase`, which demands httpfs and bumps the
attach to READ_ONLY before the storage extension ever sees it). The
endpoint comes from the `ENDPOINT` option or the
`hoglake_default_endpoint` setting (`/v1` appended by the client, as in
pyhoglake). Options:

| Option | Meaning |
|---|---|
| `ENDPOINT` | server base URL (alternative to embedding it in the path) |
| `SNAPSHOT_VERSION` / `SNAPSHOT_TIME` | pin the whole attach; forces READ_ONLY; mutually exclusive (DuckLake semantics) |
| `CREATE_IF_NOT_EXISTS` + `DATA_PATH` | create the catalog on attach (maps to POST /catalogs; DATA_PATH must be s3://) |

A `SECRET` of `TYPE hoglake` (endpoint, token) resolves when the
payload omits the endpoint — planned in M5, matching DuckLake's secret
flow. Object-store credentials are DuckDB's ordinary S3 secrets
(httpfs); two credential domains by design.

## Transactions and snapshot pinning

DuckLake's model, reproduced: a `HoglakeTransaction` pins one snapshot
id at first catalog touch (GET `/catalogs/{c}` head; explicit
`snapshot=`/`at_timestamp=` pins override). Every metadata read in the
transaction carries `?snapshot=<pinned>`, giving multi-table-consistent
reads — the property DuckLake got from being inside one Postgres
transaction. Catalog entries (schemas/tables) are cached per
transaction; the caches are guarded by recursive mutexes (parallel
pipeline inits hit them concurrently — DuckDB does not serialize
catalog scans for extensions), entries are retired rather than
destroyed while the transaction lives, and the lock order is
schema-entry lock before transaction lock (ScanSchemas snapshots its
entry list and runs callbacks unlocked to keep the order acyclic).

Writes buffer in the transaction: INSERTs write parquet immediately
(data plane), and file registrations accumulate. On DuckDB COMMIT, one
`POST /catalogs/{c}/commit` ships every buffered `TableAppend` +
`TableDeletes` — multi-statement, multi-table atomicity comes from the
wire contract itself (one CommitRequest = one snapshot). Append-only
commits are blind (no `read_snapshot`); commits with deletes carry
`read_snapshot = pinned`. A 409 on an append-only commit retries with
backoff; a 409 with deletes is NOT auto-retried (the superseded
deletion vectors would have to be rebuilt against the new state — the
statement must be re-run), and the "table was recreated" refusal never
retries. ROLLBACK drops the registrations; uploaded parquet/puffin is
orphaned (cleanup's problem, never the catalog's — the pyhoglake
position).

**One deletion vector per data file per commit.** Deletes buffer
per-(table, data_file_id): each DELETE/UPDATE statement merges the
server-live DV, the positions already buffered by earlier statements
of the same transaction, and its own new positions into one superseding
puffin file, and the registration REPLACES the earlier buffered one.
The server's one-live-DV-per-data-file invariant therefore holds for
any multi-statement DML transaction (`hoglake_txn.test`).

**Read-your-own-DELETES; refuse-after-own-INSERTS.** Every
same-transaction scan merges the transaction's buffered delete
positions into its delete mask (AT-clause reads stay historical), so
a later statement never re-reads a row this transaction already
deleted — without this, DELETE-then-UPDATE resurrected the deleted
row and UPDATE-then-UPDATE doubled the table, silently, at commit.
Uncommitted INSERTS (from INSERT or an earlier UPDATE's rewrites)
remain invisible — and because a later predicate could then silently
miss rewritten rows while reporting a plausible nonzero count
(partial predicate overlap commits sequentially-wrong data), any
DELETE/UPDATE on a table this transaction already inserted or updated
rows into is REFUSED with a clear error. DELETE-then-DELETE and
DELETE-then-UPDATE stay legal (deletes are masked); everything after
an append-producing statement on the same table requires COMMIT
first. Tested in `hoglake_txn.test` (both corruption shapes refused,
the legal orders verified).

## Transactions and eager DDL

Every hoglake DDL endpoint (create/drop/alter table, create namespace)
is its own server-side commit producing its own snapshot. The extension
executes DDL eagerly at statement time; a subsequent ROLLBACK does not
undo it (DuckLake could hold DDL inside the metadata transaction;
hoglake's service contract cannot, short of a wire change — server
finding 3). Two rules keep this honest instead of quietly broken:

1. **DDL on a table with buffered writes is refused** with a clear
   TransactionException ("COMMIT or ROLLBACK first"). Without the
   refusal, INSERT t; DROP t; COMMIT would abort at commit (append for
   a nonexistent table) with the drop already persisted, and
   DELETE t; ALTER t; COMMIT would always 409 against the
   transaction's own alter. DDL on OTHER tables stays allowed — the
   server's conflict check is table-scoped.
1b. **The mirror order is refused too.** The server's conflict check
   runs iff the commit carries deletes and scans
   `table_dropped`/`table_altered` changes after `read_snapshot` over
   every touched table (appends and deletes alike). So after an eager
   ALTER on table x in this transaction: DELETE/UPDATE on x is
   refused; DELETE/UPDATE anywhere is refused while buffered appends
   target x; INSERT into x is refused while buffered deletes exist.
   ALTER x; INSERT x; COMMIT stays legal (append-only commits are
   blind). `table_created` is NOT conflict-scanned, so tables created
   in the transaction are freely writable.
2. **Tables created or altered inside the transaction read at the
   post-DDL snapshot**, not the (older) transaction pin: their entry
   carries a fixed `read_travel` of the snapshot the DDL response
   returned. Without this, SELECT after CREATE in the same transaction
   404s (the pin predates the table). The rest of the transaction
   keeps the original pin. Because the create/alter responses carry
   `snapshot_id` (hoglake#35 — drop always did), the pin is the DDL
   commit's own snapshot: deterministic, not a separate GET
   /catalogs/{c} head read that a foreign commit could land inside.
   (Fallback for a pre-#35 server with no `snapshot_id`: the old head
   read, accepting that race.) The entry's travel already includes
   every foreign commit between the pin and the DDL by design. One
   behavior the head read did not have: a pinned DDL snapshot can fall
   BELOW the expiry floor if the retention sweep advances it between
   the DDL commit and this transaction's first read, and then the read
   410s (Expired). The old head read was always >= the floor by
   construction. That is the intended trade for determinism — a table
   whose creating snapshot has already expired is not meaningfully
   readable — but it is a change worth naming here, not just in code.

CREATE TABLE AS works: eager create, then buffered insert committing
on COMMIT (the CTAS child plan is cast to the wire types first —
TINYINT/SMALLINT widen to the wire "int"; feeding narrower vectors
into the copy corrupts data silently).

Uncommitted INSERTS are invisible to the transaction's own scans (a
created-in-txn table reads as empty until COMMIT); buffered DELETES
ARE masked (see "Read-your-own-DELETES" above), and DML that would
trip over the insert invisibility is refused rather than allowed to
commit wrong data.

## Identifier case

DuckDB identifiers are case-insensitive and case-preserving; the
hoglake server matches names case-sensitively (`tv.name = :name`). The
bridge: table identifiers are resolved through the server listing —
every lookup and every DDL/DML wire call CI-resolves the typed
identifier to the server's exact name and sends THAT (never the typed
case). Creates send the typed case (case-preserving); CI-equal names
are duplicates, DuckDB-style. Two server tables whose names differ
only by case are unaddressable from DuckDB and reported as ambiguous.
Column identifiers resolve the same way against the wire columns, and
DDL TARGETS are CI-conflict-checked before the eager server commit
(ADD/RENAME COLUMN, RENAME TABLE — the server checks exact case only,
and a committed CI column collision is unrepresentable in DuckDB's
ColumnList; self case-change renames stay legal). Case-colliding
pairs created by other clients error as ambiguous on lookup and are
skipped in listings — for tables, columns (a targeted per-table
error; the rest of the catalog stays browsable), and namespaces
alike. `hoglake_case.test` covers cold-cache lookups, DDL, DML, the
conflict checks, and the fixture-created ambiguous pairs.

## Read path

1. **Bind**: table entry from GET `…/tables/{t}?snapshot=` — columns
   (name, hoglake type, field_id, ordinal, nullable), partition spec,
   sort spec, table_uuid.
2. **Plan**: GET `…/tables/{t}/scan?snapshot=` → `ScanFile[]`
   (data_file + optional live delete_file). A `HoglakeMultiFileList`
   feeds a clone of `parquet_scan` through a `HoglakeMultiFileReader`
   (DuckLake's exact pattern), with field-id-based column mapping so
   renames/adds/drops bind correctly against old files.
3. **Prune**: partition pruning client-side from `spec_id` +
   `partition_values` on each file — identity transforms prune with
   full fidelity; `bucket/year/month/day/hour` prune via
   transform-aware comparison (Iceberg semantics, mirroring
   pyhoglake `transforms.py`). Files with `stats_state != provided`
   are never pruned (correctness by construction).
   **Gap (client side; server finding 1 addressed)**: the extension
   plans without per-file column bounds, so DuckLake-style zone-map file
   skipping on arbitrary predicates is not implemented; parquet
   row-group pruning still applies after open. The server now serves
   them on request — `/scan?include=column_stats&stats_fields=<ids>`
   attaches `data_file.column_stats` (decoded bounds, the
   `ScanColumnStats` schema) to every `provided` file — so this is a
   client TODO: request the predicate's field ids and skip files whose
   bounds exclude it.
4. **Deletes**: a `delete_file` (puffin `deletion-vector-v1`, exactly
   the encoding in `PuffinDeletionVector.kt`) is fetched, decoded
   (roaring via vcpkg — DuckLake's dependency), and applied as a
   positional delete filter on the scan (DuckLake's
   `DuckLakeDeleteFilter` machinery).
5. **Virtual columns**: `rowid` = `row_id_start + ordinal` for
   positional files; for `explicit_row_ids` files (compaction outputs)
   it reads the physical `_hog_row_id` column (reserved parquet field
   id 2147483646). The CATALOG's flag decides which source is used —
   never the file's own field ids — and the client then checks that
   the file AGREES with its registration, because the server cannot
   (registration never opens the parquet; /verify excludes the
   field-id contract). Both disagreements are refused with a typed
   error naming the file, each by its own explicit check (never a
   fall-through): flag false + reserved field id present violates the
   reserved-id invariant (AGENT.md invariant 2) and its row ids cannot
   be trusted; flag true + reserved column absent cannot produce row
   ids at all. Both directions are covered by tests
   (`hoglake_wire_hardening.test`). `filename` and `snapshot_id`
   (file's `begin_snapshot`) as in DuckLake. `_hog_row_id` is
   projected out of ordinary `SELECT *`.

## Write path

INSERT plans a `HoglakeInsert` physical operator (DuckLake's insert
shape): rows are partitioned under the table's live spec (fanout, one
file per distinct transformed tuple — null groups per Iceberg), sorted
by the live sort spec when `sort_on_insert`-equivalent applies, and
written with DuckDB's parquet writer with **field ids stamped** and
target file size rotation. Per file the operator collects footer stats
into `FileRegistration`: `record_count`, `file_size_bytes`,
`footer_size` (**exactly the 4-byte LE thrift length, excluding the
8-byte trailer** — the bugs.md #7 convention), `column_stats` with
Iceberg single-value binary bounds (base64; codec ported from
pyhoglake `bounds.py`, validated against `tests/vectors/
bounds_vectors.json`), and `partition_values` (wire strings by
key_index). Paths: `<data_path>data/<namespace>/<table>/<uuid>.parquet`
(pyhoglake's layout); the server enforces the data_path prefix and
segment hygiene. `expected_table_uuid` rides every append (the
incarnation guard) with the uuid the table entry was bound at.

DELETE (M4): scan produces (file, position) sets, attributed to the
exact LOGICAL registration via the rowid base (rowid −
file_row_number == the registration's row_id_start): the catalog
legally holds several live registrations of one physical path (writer
retries), and a rowid/snapshot-scoped predicate must grow only the
matching copy's DV. Ambiguous shapes (explicit-row-id duplicates,
shared row_id_start) refuse typed. Out-of-range positions — an
under-declared duplicate registration or another client's committed
DV holding positions ≥ record_count (the server validates neither) —
are the OTHER party's corruption: the statement refuses with a typed
error naming file, position, and registration; never an
instance-invalidating InternalException, never a silent clamp.
Otherwise: scan produces (file, position) sets; per touched data file
the transaction merges positions into that file's existing DV (vectors
only grow — superseding DV must contain the old one), writes a new
puffin file, buffers a `DeleteFileRegistration`
(`delete_count = total`, bounded by `record_count`), and commits with
mandatory `read_snapshot`. UPDATE = DELETE of old positions + INSERT of
new rows in the same commit (DuckLake's rewrite semantics). MERGE
lowers to the same two primitives.

Column types: hoglake's closed flat set (`boolean,int,long,float,
double,decimal,date,time,timestamp,timestamptz,string,uuid,binary`);
mapping identical to pyhoglake `types.py` (`uuid` <-> DuckDB UUID via
16-byte big-endian; `timestamp[tz]` micros). Nested/other DuckDB types
are rejected at CREATE/ALTER with a clear error. Column names beginning
`_hog` are rejected client-side (fast fail before upload).

## DDL mapping

| SQL | Wire |
|---|---|
| `CREATE SCHEMA` | POST `/namespaces` |
| `DROP SCHEMA` | **gap**: no wire op (finding: DELETE `/namespaces/{ns}`) |
| `CREATE TABLE` | POST `/tables` |
| `CREATE TABLE ... PARTITIONED BY / SORTED BY` | create + one `/alter` (`set_partition_spec` / `set_sort_order`) |
| `DROP TABLE` | DELETE `/tables/{t}` (no CASCADE, as DuckLake) |
| `ALTER TABLE ADD/DROP/RENAME COLUMN` | `/alter` ops `add_column`/`drop_column`/`rename_column` (409 `idless_files_present` surfaced verbatim) |
| `ALTER TABLE ALTER COLUMN TYPE` | `promote_column` (server's promotion lattice: int→long, float→double; others 422) |
| `ALTER TABLE RENAME TO` | `rename_table` |
| `SET PARTITIONED BY / SORTED BY` | `set_partition_spec` / `set_sort_order` |
| `CREATE/DROP VIEW` | POST/DELETE `/views` with `dialect: duckdb`; only duckdb-dialect views bind on read, others listed but error on use |
| `COMMENT ON` | **gap**: no wire support |
| `SET/DROP NOT NULL`, `SET DEFAULT`, nested-field ops | **gap**: not in `AlterOp` (flat schema, no defaults on the wire) |

Multiple ALTER clauses batch into one `/alter` call (atomic, ordered —
better than DuckLake's per-statement alters).

## Metadata / maintenance functions (parity table)

DuckLake function → hoglake equivalent:

| DuckLake | hoglake extension | Notes |
|---|---|---|
| `ducklake_snapshots` | `hoglake_snapshots(cat)` | GET `/snapshots`, **paginated** (fixes the 15.3M-snapshot OOM); typed change rows surfaced as MAP |
| `ducklake_table_info` | `hoglake_table_info(cat)` | per-table GET; delete-file counts derived from `/scan` |
| `ducklake_table_insertions` | `hoglake_table_changes(cat, ns, t, from, to)` | GET `/changes`; append-only v1 — insertions + DV diffs; no update pre/post images (**wire gap**) |
| `ducklake_table_deletions` | `delete_files` half of `/changes` | positions only; row materialization = read data file + DV diff |
| `ducklake_merge_adjacent_files` | `hoglake_compact(cat)` | POST `/maintenance/compact` (server-side — the whole point) |
| `ducklake_rewrite_data_files` | server compaction policy | no client knob; N/A |
| `ducklake_expire_snapshots` | `hoglake_expire(cat)` | POST `/maintenance/expire`; all mutating maintenance functions refuse read-only attaches (incl. pin-forced read-only) |
| `ducklake_cleanup_old_files` | `hoglake_cleanup(cat)` | POST `/maintenance/cleanup` (liveness-checked server-side) |
| `ducklake_delete_orphaned_files` | — | server concern; not exposed |
| `ducklake_flush_inlined_data` | — | N/A (no inlining in hoglake, by design) |
| `ducklake_set_option` / `options` | `hoglake_options(cat)` / `hoglake_set_retention(...)` | GET/PATCH `/options` (retention + consumer_floor only — hoglake options are server config) |
| `ducklake_settings` | `hoglake_settings(cat)` | endpoint, extension version, data_path |
| `ducklake_set_commit_message` | `hoglake_set_commit_message(cat, author, msg)` | rides CommitRequest `author`/`message` |
| `ducklake_current_snapshot` | `hoglake_current_snapshot(cat)` | the pinned snapshot |
| `ducklake_last_committed_snapshot` | `hoglake_last_committed_snapshot(cat)` | in-process cache, as DuckLake |
| `ducklake_list_files` | `hoglake_list_files(cat, t)` | GET `/files` (+ snapshot/time params); no encryption keys (N/A) |
| `ducklake_add_data_files` | deferred | wire supports it in principle (client parses footers, commits); DuckLake's NULL `row_id_start` defect does not apply — hoglake assigns row ids at commit |
| `ducklake_commit` | — | N/A: every hoglake commit is server-side by construction |
| `CHECKPOINT` composite | maybe `expire→compact→cleanup` passthrough | low value; server loops exist |
| `murmur3_32` | ported | needed for bucket-transform pruning anyway |
| time travel `AT (VERSION/TIMESTAMP =>)` | ✔ | `?snapshot=` / `?at_timestamp=` (410 below floor → clear error) |
| consumer offsets | `hoglake_offsets(...)`, `hoglake_commit_offset(...)` | new surface (no DuckLake analogue); monotonic 409 surfaced |

Not carried over (fleet-liability list, deliberate): data inlining,
encryption, `metadata_*` options, multi-backend managers,
`automatic_migration`, per-catalog maintenance with client-side commit
loops.

## Deps & build

- **duckdb** pinned to the ducklake-fork's submodule commit
  `ac5c6d11c3b0915fcc0fa43080d31893269d6448` (fork describes it
  v1.5.5-dev); **extension-ci-tools** pinned to
  `795096d04b009c0d087468439ebb526a5460dfac` (v1.4.3-6). Same makefile
  pattern as ducklake (`EXT_NAME=hoglake`, include
  `extension-ci-tools/makefiles/duckdb_extension.Makefile`).
- Because `duckdb-client/` lives inside the hoglake monorepo and this
  branch may not touch the repo root, the two trees are **fetched by
  `scripts/fetch-deps.sh`** (clones from the local ducklake-fork
  checkouts when present, else from GitHub; checks out the pinned
  SHAs) and are gitignored. On extraction to a standalone
  community-extension repo they become ordinary submodules —
  documented, mechanical.
- `vcpkg.json`: `roaring` only (deletion vectors) — identical to
  ducklake. Local toolchain: `~/.vcpkg/scripts/buildsystems/vcpkg.cmake`.
- Test extension deps: `parquet` is in-tree; `httpfs` needed for s3
  reads in integration tests (`FULL_TEST_EXTENSION_DEPS` pattern).

## Test strategy

1. **sqllogictests** (`test/sql/`): the primary suite, run via
   `make test` — or `test/run-live-tests.sh`, which chains fixtures,
   the suite (with the fixture-exported time-travel snapshot ids in
   the environment), and the cross-client wire check. `require-env` is
   a WHOLE-FILE skip disposition wherever it appears, and the runner
   exits 0 on a skip, so: every conditional fixture gate owns its own
   file, and `run-live-tests.sh` fails when a gate is unexported, when
   any file was skipped, when fewer files ran than exist, or below an
   assertion floor — a green run states what it ran. Integration tests `require-env HOGLAKE_URL` (+ MinIO
   creds env) and run against the live dev stack
   (`http://localhost:8080` + MinIO `http://localhost:19000`,
   creds hoglake/hoglake123 — pyhoglake's conventions); they
   create disposable `duckext-<runid>` catalogs in a
   `duckext-itest` bucket. Skip cleanly when no server is up — a green
   run without the env is NOT a full verification (repo rule; report
   which ran).
2. **Cross-client wire vectors**: `test/run-live-tests.sh` runs
   `test/fixtures/verify_partition_wire.py` after the suite — it
   asserts extension-written and pyhoglake-written files with the same
   logical partition value land in byte-identical partition string
   groups (int/date/timestamp-with-fractional-seconds/boolean). The
   bounds codec (`tests/vectors/bounds_vectors.json`) has NO C++
   counterpart yet: stats ship deferred, so no bounds are encoded
   client-side; the vector harness lands with the bounds codec.
3. **Semantics cross-check**: pyhoglake's integration tests are the
   executable spec; each milestone ports the relevant behaviors
   (append/commit shapes, alter flows, expiry 410s, incarnation guard).
4. CI (later, when adopted): compose-up server + MinIO, then
   `make test` — path-scoped workflow like the other components.

## Milestone status

M0–M5 all landed; the milestone log itself is a local-only working
document (repo convention for in-flight worklists), as are the
review-round findings files. [README.md](README.md) is the component's
front door (what it is, how to build it, how to run the live suite).

## Parity status

See [PARITY.md](PARITY.md) for the per-capability checklist
(implemented / partial / wire gap / not-carried-over / todo).

## Findings for the server (no server changes made)

1. ~~`/scan` (`ScanFile`) carries no per-file column bounds → no
   file-level zone-map pruning for external engines.~~ **Addressed**
   server-side: opt-in `include=column_stats` (narrowed by
   `stats_fields`) adds per-file bounds to the scan plan. The extension
   does not request them yet (see step 3 above).
2. No `DELETE /namespaces/{ns}` → `DROP SCHEMA` unimplementable.
3. No staged/transactional DDL commit → DuckDB DDL cannot participate
   in transaction rollback (documented divergence).
4. `/changes` has no update pre/post-image semantics (append-only v1)
   → `ducklake_table_changes` parity is partial by wire design.
5. No comment/tag storage for tables/columns → `COMMENT ON` gap.
6. `/namespaces` and `.../tables` listings are head-only (no
   `snapshot=` param) → catalog listings inside a pinned transaction
   can drift from the pinned snapshot (per-table fetches are pinned).
7. `FileRegistration` has no `explicit_row_ids` → clients cannot
   register explicit-row-id files, so client-side UPDATE cannot
   preserve row identity (ducklake preserves; hoglake UPDATE assigns
   new row ids). If preserved-rowid updates matter, the commit needs
   the flag (plus the row-id-tiling exemption compaction outputs get).
8. ~~`POST .../tables/{t}` (create) and `.../alter` responses carry no
   snapshot_id (drop's CommitResult does): the client cannot learn its
   own DDL commit's snapshot and papers over it with a racy
   GET /catalogs — return CommitResult-style snapshot info on
   create/alter for a deterministic post-DDL read pin.~~
   **CLOSED** (hoglake#35). Both responses now carry `snapshot_id` —
   the snapshot the DDL commit just allocated — so the client pins
   post-DDL reads to it directly instead of racing a GET /catalogs
   head read. `PostDDLTravel` uses the returned snapshot and keeps the
   head read only as a fallback for a server old enough not to send it.
9. ~~The server does NOT enforce the `_hog` reserved column prefix~~
   **CLOSED.** `Identifiers.validateColumn` reserves the prefix at
   every nesting level, on create, add_column and rename_column, and
   `ParquetRewriter.outputSchema` refuses a live column of that name
   for tables that predate the reservation. Compaction also enforces
   both directions of the reserved-FIELD-ID contract that the carrier
   depends on (§ "Virtual columns"): it is the one server surface that
   opens a registered parquet, so it can check what registration
   cannot. What remains outside the server's reach is a file whose
   SCHEMA carries the reserved id without the catalog saying so —
   caught at compaction, and by this client at read time, but never at
   registration.
10. No timestamp→snapshot resolution on the wire: a SNAPSHOT_TIME
   attach can never produce a client-side snapshot id (reads stay
   consistent because the server re-resolves the same timestamp to the
   same snapshot; `hoglake_current_snapshot` errors instead of lying).
   Returning the resolved snapshot id on time-travel responses (or a
   resolve endpoint) would close this.
11. Environment note (historical): earlier rounds found the dev
   stack's hydrator hydrating nothing (files stuck `pending`,
   compaction starved). As of 2026-09-12 the stack hydrates and
   compacts again; the fixture force-compacts `points` and the
   suite reads real compaction output (explicit `_hog_row_id`).