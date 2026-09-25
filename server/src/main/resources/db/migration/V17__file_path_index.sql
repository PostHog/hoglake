-- The cleanup drain's LIVENESS CHECK (invariant 4) reads every file row
-- in the catalog, once per sub-batch, while it holds the per-catalog
-- commit lock. `CleanupService.referencedPaths` asks
--
--   SELECT path FROM hog_data_file   WHERE catalog_id = ? AND path = ANY(?)
--   UNION
--   SELECT path FROM hog_delete_file WHERE catalog_id = ? AND path = ANY(?)
--   UNION
--   SELECT path FROM hog_upload      WHERE catalog_id = ? AND path = ANY(?)
--                                      AND state = 'active'
--
-- and before this file NOTHING on hog_data_file or hog_delete_file has
-- `path` anywhere in its leading columns: hog_data_file carries
-- `hog_data_file_live`, `_pending`, `_changefeed`, `_maintenance_scan`,
-- `_maintenance_size_scan` and its primary key, every one of them keyed
-- on ids and snapshots. V16 indexed hog_file_removal, which is a
-- different table and a different statement. So two of the three legs
-- were a SEQUENTIAL SCAN of the whole relation, per sub-batch, under
-- the lock every commit and every DDL tail needs.
--
-- The third leg needs nothing: hog_upload already carries
-- `UNIQUE (catalog_id, path)` (V12), and the planner drives that leg
-- from `hog_upload_catalog_id_path_key` with `state` as a cheap filter
-- on the rows it fetches. Measured below with the rest; no index is
-- added for it, because an index nobody chooses must not exist.
--
-- MEASURED, on a production-shaped fixture (PG 18.6, 5,000,000
-- hog_data_file rows over two catalogs — one dropped table's 3,000,000
-- ended rows plus a live set — 175-byte hash-scattered paths, 1,491 MiB
-- heap / 190,884 pages, 100,000 hog_delete_file rows, 200,000
-- hog_upload rows). THE FIXTURE IS THE SIZE OF THE REAL THING: counted
-- through the API on 2026-09-25 02:40 UTC, catalog millpond-prod-us
-- holds 4,941,469 LIVE data files (ingest.events_raw 1,917,940;
-- main.events_raw 3,008,849 — the dropped-but-undroppable table; the
-- rest ~10k), and hog_data_file also carries the historical rows inside
-- the 3 h retention, roughly 10-30k per hour of expiry churn, so the
-- table is about 5.0-5.1M rows. Its relation size was NOT measured (the
-- deploy tooling has no database access); at the fixture's 243 bytes of
-- index per row that is ~1.2 GiB of new index. EXPLAIN (ANALYZE,
-- BUFFERS) of the exact statement with a 1,000-path probe:
--
--   before   197,752 buffers, 692 ms  (Seq Scan on hog_data_file:
--            190,884 buffers, `Rows Removed by Filter: 4,999,998`;
--            Seq Scan on hog_delete_file: 3,125 buffers)
--   after      8,728 buffers,  33 ms  (Index Only Scan x2, `Index
--            Searches: 995` and `Heap Fetches: 0`)
--
-- 23x fewer buffers, and what remains sizes with the PROBE rather than
-- with the catalog. The production corroboration (gigahog-prod-us,
-- 2026-09-25, catalog millpond-prod-us, right after 1.3.1 shipped
-- #205's 1,000-row sub-batch): a warm 2,000-row drain took 6 s across
-- two replicas interleaving their holds — ~3 s per hold, of which the
-- one `DeleteObjects` call is a fraction of a second, so the rest is
-- this scan. The same drain during the rollout itself, against a cold
-- cache and an old replica still holding the lock, took ~30 s per hold
-- and commit latency went from ~0.3 s to 5.7 s average / 36 s maximum
-- for that minute, with one API pod liveness-killed and a compaction
-- group timed out waiting for the lock. #205 made the hold cheap in
-- object-store calls; this file makes it cheap in the database.
--
-- NOT PARTIAL, and the check's own KDoc is why: it covers "any file
-- row, LIVE OR NOT". A historical row still claims its object at every
-- retained snapshot, so `WHERE end_snapshot IS NULL` would hide exactly
-- the rows whose absence authorizes a physical delete — a partial index
-- here would not be a cheaper index, it would be a correctness change
-- wearing an index's clothes. There is no other clause to restrict on:
-- the statement carries `catalog_id` and `path` and nothing else.
--
-- `CREATE INDEX CONCURRENTLY`, AND THAT REVERSES V16. V16 chose a
-- plain, blocking build after measuring one, and the reasoning was
-- sound for a 164,307-row table where the build takes 1.37 s. It does
-- not transfer to a 5,000,000-row table with 175-byte keys. Timed on
-- the fixture above, with a writer committing single-row inserts
-- throughout:
--
--                        maintenance_work_mem 256 MB   64 MB
--   plain CREATE INDEX          build           27.4 s   31.1 s
--                               worst INSERT    27.4 s   31.1 s
--   CREATE INDEX CONCURRENTLY   build           26.0 s   35.0 s
--                               worst INSERT     156 ms    519 ms
--
-- The plain build BLOCKS a writer for its whole duration — one blocked
-- write is every commit in the fleet, it is past the 30 s admission
-- bound (HOGLAKE_COMMIT_LOCK_TIMEOUT_MS), and it reproduces the
-- incident this file exists to remove: queued commits each hold a
-- pooled connection, `/healthz` needs one too, and the pods are
-- liveness-killed. The concurrent build is two table passes and no
-- faster — slower, at the smaller `maintenance_work_mem` — and it never
-- blocks a writer, which at this size is the whole decision. Both
-- numbers are stated with the memory they were measured at because
-- that setting, not the code, is what decides whether the build fits
-- the deploy budget below; 64 MB is the conservative end of what an
-- RDS parameter group gives a mid-sized instance.
--
-- The lock_timeout window is not what makes a plain build safe here:
-- `lock_timeout` bounds how long a statement WAITS for a lock, never
-- how long it HOLDS one. A plain build that acquires SHARE instantly
-- holds it for its full 27 s inside a 5 s window.
--
-- THE HAZARD V16 RECORDED AGAINST CIC IS REAL, AND IT IS FIXED AT ITS
-- ROOT RATHER THAN AVOIDED. CIC's phases wait out every transaction
-- older than themselves; a second replica booting into
-- `Database.migrate` used to sit in a BLOCKING `pg_advisory_lock`,
-- which is an open statement with a published snapshot, so the build
-- parked behind a session that was itself waiting on the migration the
-- build belongs to. Measured on the fixture, with writers running:
--
--   * no second replica:                     CIC 26 s
--   * second replica blocking (old code):    CIC 58 s — 32 s of it
--     parked, observed directly as `wait_event = virtualxid` in
--     pg_stat_activity against the waiting replica's virtual
--     transaction. That replica's own boot then FAILED: its blocked
--     `pg_advisory_lock` was killed by the 60 s statement_timeout its
--     session carries. Which of the two ended the park is not claimed
--     here — the durations are what were measured, on separate session
--     clocks, and the build was demonstrably waiting on the waiter;
--   * second replica POLLING (this change):  CIC 26 s, no park, and the
--     replica takes the lock as soon as the first one is done.
--
-- `Database.awaitMigrationLock` now polls `pg_try_advisory_lock` with a
-- client-side sleep, so a waiting replica holds a snapshot for the
-- microseconds each attempt takes instead of for the whole migration.
-- `MigrationLockIntegrationTest` pins it with the property that
-- matters: a CIC in a third session completes while a replica waits.
--
-- THE DEPLOY BUDGET, AND WHY THE BUILD SHOULD BE PRE-APPLIED ON A LARGE
-- CATALOG. `Main.kt` migrates BEFORE Netty binds :8080, and the chart's
-- TCP startup probe allows 30 x 5 s = 150 s before the kubelet SIGKILLs
-- the container. Every pod in the fleet is inside that window at the
-- same time. A 26-35 s build fits; a build that PARKS does not, and the
-- polling fix only removed hoglake's own parker. CIC waits out every
-- older snapshot in the cluster, and the rest of them are foreign:
-- an operator's psql sitting in a transaction, `pg_dump`, an RDS
-- snapshot export, a logical-decoding slot's reader. None of those
-- carry hoglake's 30 s idle-in-transaction bound, so none of them is
-- bounded by anything this repo controls, and a build parked behind one
-- takes every pod to a 150 s SIGKILL with no listener ever open.
--
-- So for a catalog of this size the RECOMMENDED deploy step is to build
-- the indexes OUT OF BAND first, at a time of your choosing, and let
-- the migration be the no-op it then is:
--
--   -- 1. nothing older than the build may be running:
--   SELECT pid, state, now() - xact_start AS xact_age, left(query, 80)
--   FROM pg_stat_activity
--   WHERE datname = current_database() AND xact_start IS NOT NULL
--     AND now() - xact_start > interval '1 minute'
--   ORDER BY xact_start;
--
--   -- 2. the builds themselves (each can be re-run; an interrupted one
--   --    leaves an INVALID index that the DO block below clears):
--   SET statement_timeout = 0;
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS hog_data_file_path
--       ON hog_data_file (catalog_id, path);
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS hog_delete_file_path
--       ON hog_delete_file (catalog_id, path);
--
--   -- 3. both valid before promoting:
--   SELECT c.relname, i.indisvalid FROM pg_class c
--   JOIN pg_index i ON i.indexrelid = c.oid
--   WHERE c.relname IN ('hog_data_file_path', 'hog_delete_file_path');
--
-- The migration then finds them and does nothing, and the deploy has no
-- build in it at all. On a small catalog (dev, a fresh install) none of
-- this matters: the build is milliseconds and the migration is the
-- simplest place for it.
--
-- WRITE COST, because hog_data_file's INSERT is the hottest write in
-- the system and this index is paid on every one of them. On the same
-- fixture, 20,000 rows inserted as 100 transactions of 200 files (a
-- commit's shape), each run after a CHECKPOINT:
--
--   without the index   345 ms,  16 MiB WAL    (836 bytes/row)
--   with the index      955 ms, 156 MiB WAL  (8,179 bytes/row)
--
-- +31 us and +7,343 BYTES of WAL per file row: a 200-file commit pays
-- ~6 ms and ~1.5 MiB more, and at the production churn of ~200k file
-- rows/hour that is 1.47 GB/hour, ~35 GB/day, and ~250 GB of extra
-- retained WAL at a 7-day PITR window (plus the same bytes on every
-- replication stream). The WAL is full-page images, not tuples — a
-- random 175-byte key lands on its own leaf of a 1,157 MiB /
-- 148,158-page index, so the first insert into each leaf after a
-- checkpoint images the page. The IOPS are not the problem: ~56 random
-- 8 KiB page writes per second at that insert rate. An operator who
-- wants the bytes back can turn on `wal_compression` (measured lz4:
-- 5,033 bytes/row, -39%); this file does not, because that is a
-- cluster-wide setting and not a schema decision.
--
-- The index is 1,157 MiB against a 1,491 MiB heap at 5M rows (243
-- bytes/row, the key plus tuple overhead), taking hog_data_file's total
-- relation size from 2,264 MiB to 3,421 MiB. hog_delete_file's is
-- 19 MiB at 100,000 rows and its build takes 473 ms; it is the same
-- shape for the same statement, and its inserts are a small fraction of
-- the data files'.
--
-- WHAT ELSE THE INDEX SERVES (all measured on the fixture, all the same
-- `(catalog_id, path)` shape):
--
--   * `UploadService.register`'s retained-path probe, which runs on the
--     FOREGROUND commit path — every commit, inside the commit
--     transaction — and UNIONs the same two tables by path;
--   * `UploadService.QUEUE_ABANDONED_UPLOAD_SQL`'s second and third
--     `NOT EXISTS`, which V16's file recorded as deliberately
--     sequential. That trade is what this file re-decides: the reason
--     given there was "an hourly read", and the same scan turned out to
--     be on the commit-lock hold of every cleanup sub-batch.
--   * `CommitService`'s pending-delete resolution, which filters
--     hog_data_file by (catalog_id, table_id, begin_snapshot, path) and
--     can now be driven by whichever of the two is more selective;
--   * `VerifyService.PATH_EQUALITY_QUERIES`, the metadata-only /verify
--     checks that join these tables on `path`.
--
-- Every heavy-lock statement below sits inside the lock_timeout window
-- (AGENT.md's migration rule; `MigrationLockWindowTest` reads this file
-- off disk and enforces it). The builds themselves are the documented
-- exemption: CONCURRENTLY waits out older transactions by design, and a
-- 5 s bound would abort exactly that wait. This file therefore runs
-- with `executeInTransaction=false` (see the `.conf`), so the window is
-- a SAVE-and-RESTORE pair rather than `SET LOCAL`, and every statement
-- is idempotent — a partial failure leaves a `success = false` history
-- row that fails Flyway's validate on every replica until an operator
-- runs `flyway repair`, and re-running has to be free.
--
-- NEITHER RESTORE RUNS ON THE FAILURE PATH, and that is survivable for
-- one reason only: a failed migration takes the process with it.
-- `Main.kt` migrates before it binds, so a throw here is a dead pod and
-- the pooled connection that carried the settings dies with the JVM —
-- nothing later ever sees a session left at `lock_timeout = 5s` or
-- `statement_timeout = 0`. That is a property of the CALLER, not of
-- this file, and a future caller that migrates on a live pool (an admin
-- endpoint, a test harness reusing the pool) would leak them. V14 and
-- V15 have the same shape.
SELECT set_config('hoglake.migration_lock_timeout', current_setting('lock_timeout'), false);
SET lock_timeout = '5s';

-- INVALID remnants are cleared before the builds. `CREATE INDEX
-- CONCURRENTLY IF NOT EXISTS` matches on NAME alone: it would find a
-- half-built index from a cancelled build, skip, and leave one that
-- every INSERT maintains and no query may use. A cancelled CIC is not
-- hypothetical here — it is what a killed pod, an operator's Ctrl-C, or
-- a statement timeout leaves behind, and this file's own build is long
-- enough to be interrupted. Plain `DROP INDEX` takes ACCESS EXCLUSIVE
-- on the TABLE and cannot be CONCURRENTLY inside a DO block, so it runs
-- under the 5 s bound above: fail fast and retryably rather than
-- convoy the catalog.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_index i ON i.indexrelid = c.oid
        WHERE c.relname = 'hog_data_file_path' AND NOT i.indisvalid
    ) THEN
        EXECUTE 'DROP INDEX hog_data_file_path';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_index i ON i.indexrelid = c.oid
        WHERE c.relname = 'hog_delete_file_path' AND NOT i.indisvalid
    ) THEN
        EXECUTE 'DROP INDEX hog_delete_file_path';
    END IF;
END $$;

-- Restore BEFORE the concurrent builds: they wait out every transaction
-- older than themselves, which is exactly the wait a 5 s lock_timeout
-- would abort.
SELECT set_config('lock_timeout', current_setting('hoglake.migration_lock_timeout'), false);

-- `statement_timeout = 0` for the builds and nothing else. A pod's
-- session carries 60 s (Database.SESSION_INIT_SQL) and the measured
-- build is 26 s at 5M rows with 256 MB of `maintenance_work_mem` and
-- 35 s with 64 MB, so the bound is not far away, it grows with the
-- table, and it shrinks with that setting. A build killed by it leaves
-- an INVALID index and a failed history row; the DO block above is the
-- other half of that story. Pre-building the indexes out of band is the
-- RECOMMENDED path at this size — the statements, the pre-flight check
-- and the reason are above, under "THE DEPLOY BUDGET" — after which
-- these two statements find them and are no-ops.
SELECT set_config('hoglake.migration_statement_timeout', current_setting('statement_timeout'), false);
SET statement_timeout = 0;

CREATE INDEX CONCURRENTLY IF NOT EXISTS hog_data_file_path
    ON hog_data_file (catalog_id, path);

CREATE INDEX CONCURRENTLY IF NOT EXISTS hog_delete_file_path
    ON hog_delete_file (catalog_id, path);

SELECT set_config('statement_timeout', current_setting('hoglake.migration_statement_timeout'), false);
