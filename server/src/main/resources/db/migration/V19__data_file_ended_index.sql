-- THE INDEX EXPIRY'S DATA-FILE DELETE HAS NEVER HAD (#193).
--
-- `ExpiryService`'s step 2 is
--
--   DELETE FROM hog_data_file
--    WHERE catalog_id = ? AND end_snapshot IS NOT NULL
--      AND end_snapshot <= ?
--   RETURNING path
--
-- and before this file NOTHING on hog_data_file has `end_snapshot`
-- anywhere in its leading columns. Its indexes are `hog_data_file_live`
-- (catalog_id, table_id, begin_snapshot) WHERE end_snapshot IS NULL —
-- the COMPLEMENT of the rows this statement wants — `_pending`,
-- `_changefeed`, `_maintenance_scan`, `_maintenance_size_scan`,
-- `hog_data_file_path` (V17) and its primary key. So the statement is a
-- SEQUENTIAL SCAN of the whole manifest, inside the expiry sweep's one
-- transaction, under the per-catalog commit lock, on every sweep.
--
-- IT SHIPS ON ITS OWN, AHEAD OF THE REST OF #193, and that is why it is
-- its own migration. It helps the sweep that exists TODAY, it touches
-- one relation, and — the part that matters for a deploy — it takes no
-- ACCESS EXCLUSIVE lock on anything, so it needs no window in which no
-- expiry sweep is running. The two `ALTER TABLE`s #193 also needs
-- (`hog_table.retirement_eligible_at` and the `hog_maintenance_run.task`
-- CHECK) are V19, which does need that window.
--
-- MEASURED, WITH THE ENDED ROWS SCATTERED, which is the only honest
-- way to measure this. `V19DataFileEndedIndexMigrationIntegrationTest`
-- runs the migration against rows seeded BEFORE it and EXPLAINs
-- `ExpiryService.DATA_FILE_EXPIRY_SQL` — the repo function's own SQL,
-- exposed `internal` — on both sides. Fixture: PG 18.6, 200,000
-- hog_data_file rows over two catalogs, 4,445 heap pages, one row in
-- every hundred ENDED (the production fraction: 30-90k ended of ~5.0M
-- is 0.6-1.8%), INTERLEAVED with the live ones. Scan node, not the
-- plan's maximum:
--
--   before   4,445 buffers  (Seq Scan on hog_data_file,
--            `Rows Removed by Filter: 198,000` — the whole live
--            manifest read and discarded to find 2,000 rows)
--   after    2,003 buffers  (Index Scan using hog_data_file_ended,
--            `Index Searches: 1`, no `Rows Removed by Filter`)
--
-- 2.2x at a 1.0% ended fraction. AN EARLIER DRAFT OF THIS FILE CLAIMED
-- 91x, AND THAT NUMBER WAS AN ARTIFACT of its fixture: it inserted the
-- ended rows as one contiguous block, so they shared heap pages and the
-- index path fetched a handful of them. Production's ended rows are
-- whatever expiry and compaction happened to end, spread across the
-- manifest, so the index path costs ROUGHLY ONE HEAP BUFFER PER ENDED
-- ROW and the win is `seq pages / ended rows` rather than a constant.
--
-- WHAT THAT MEANS, stated so nobody has to rediscover it:
--
--   * the benefit is INVERSELY PROPORTIONAL TO THE ENDED FRACTION. At
--     gigahog-prod-us's standing shape (30-90k ended of ~5.0M rows,
--     190,884 heap pages) expect 2-6x. Right after a large retirement,
--     before expiry has collected what it queued, the fraction spikes
--     and the index approaches a WASH — measured at 300k scattered
--     ended rows: 300,003 buffers with the index against 112,655 for
--     the sequential scan, i.e. marginally SLOWER. It is still the
--     right index, because the steady state is the low fraction and
--     because a sequential scan's cost grows with the CATALOG while
--     this one grows with the WORK;
--   * and it DOES NOT BOUND THE ROW WORK. The scan is not the dominant
--     cost of this DELETE. The RI triggers for the three cascading
--     children are (~4 s per 300k rows even against an EMPTY child
--     table), and so is the CTE's tuplestore, which spills to disk at
--     the default `work_mem` of 4 MB. This index removes a term that
--     grows with the manifest; it does not make the statement cheap,
--     and an operator who needs the sweep itself bounded wants
--     `HOGLAKE_EXPIRY_BATCH` instead.
--
-- PARTIAL, on `end_snapshot IS NOT NULL`, and that is the opposite of
-- V17's decision for a different reason rather than a change of mind.
-- V17's index is not partial because the statement it serves covers
-- "any file row, live or not". This one serves a statement that carries
-- `end_snapshot IS NOT NULL` in its own text, and the rows it excludes
-- are the overwhelming majority — 4.94M of 5.0M on
-- gigahog-prod-us/millpond-prod-us are LIVE. So the index holds only
-- the ended rows, which is:
--
--   * ~10 bytes per ended row after deduplication, against V17's 243:
--     two bigints and tuple overhead, with no 175-byte path in the key;
--   * a standing population on prod-us of roughly 30-90k rows (the
--     historical rows inside the 3 h retention window at ~10-30k/hour
--     of expiry churn), so under a megabyte;
--   * ZERO on the insert path. An appended row has `end_snapshot NULL`
--     and does not enter the index at all. The hottest write in the
--     system pays nothing for this, which is the whole argument for the
--     partial predicate and is why this index costs a fraction of what
--     V17's does.
--
-- NO INDEX FOR THE DELETION-VECTOR ARM, and this is a decision, not an
-- omission. Expiry's step 1 is an `OR` of a range predicate on
-- `hog_delete_file.end_snapshot` and a correlated `EXISTS` over
-- hog_data_file, and the planner never chooses a `(catalog_id,
-- end_snapshot)` index for it: an OR whose second arm is a
-- semi-join is read as one pass, and the arm this index would serve
-- cannot be separated out without rewriting the statement into a UNION.
-- Adding an index nothing chooses would be paying for a plan that never
-- happens (V17's file says the same thing about hog_upload). SPLITTING
-- the statement into its two arms is the fix, it is a behaviour change
-- to the sweep, and it is ticketed rather than smuggled in here.
--
-- THE BUILD, MEASURED, WITH ITS CACHE STATE NAMED. On the fixture
-- above (200,000 rows / 4,445 heap pages, 4,000 of them ended), warm —
-- the relation was VACUUM ANALYZEd immediately before, so its heap is
-- in shared_buffers and these are the BEST case, not the deploy case:
--
--   CREATE INDEX CONCURRENTLY   14 ms
--   plain CREATE INDEX           8 ms
--   whole V19 migration         46 ms (colder: first touch after the
--                                      bulk insert)
--   index size              49,152 bytes = 12 B per ENDED row
--   heap size           36,413,440 bytes
--
-- THE COLD ESTIMATE AT PRODUCTION SIZE IS A FLOOR, and it is labelled
-- an estimate because nothing here has a production-sized volume to
-- measure on. The build is a full heap scan whatever the index holds —
-- twice over, CONCURRENTLY — so it is bounded by the RELATION, not by
-- the 30-90k rows that enter the index. At gigahog-prod-us's 1,491 MiB
-- / 190,884 pages that is two passes of ~1.5 GiB: **~24 s at an idle
-- volume's 125 MB/s, and 30-60 s on a busy one**, which is the number
-- to plan against. V17 measured a CONCURRENT build over exactly this
-- relation at 26 s (256 MB maintenance_work_mem) and 35 s (64 MB)
-- while ALSO writing a 1,157 MiB index; V19's index is about a
-- megabyte at the same row count, so the write is noise and the two
-- estimates agree. Against a 60 s session `statement_timeout`, that is
-- a margin rather than a guarantee — which is the whole argument for
-- the out-of-band pre-build below.
--
-- CONCURRENTLY, IN V17'S SHAPE, for V17's reason rather than by
-- analogy: a PLAIN build takes SHARE on hog_data_file for its whole
-- duration, and 6-30 s of blocked INSERTs is every commit in the fleet
-- blocked, past the 30 s admission bound
-- (HOGLAKE_COMMIT_LOCK_TIMEOUT_MS). On the fixture above the plain
-- build is the faster of the two (9 ms against 15 ms — one table pass
-- instead of two), exactly as V16 measured at ITS table's size, and
-- exactly as V17 found that trade reversing at this table's size. The
-- reversal is about what the build BLOCKS, not about what it costs.
--
-- SO ON A LARGE CATALOG, PRE-BUILD IT OUT OF BAND and let the migration
-- be the no-op it then is. `Main.kt` migrates before Netty binds and
-- the chart's startup probe SIGKILLs at 30 x 5 s, and a CIC waits out
-- every transaction older than itself — including FOREIGN ones this
-- repo does not bound (an operator's psql in a transaction, pg_dump, an
-- RDS export, a logical-decoding reader). The steps:
--
--   -- 1. nothing older than the build may be running:
--   SELECT pid, state, now() - xact_start AS xact_age, left(query, 80)
--   FROM pg_stat_activity
--   WHERE datname = current_database() AND xact_start IS NOT NULL
--     AND now() - xact_start > interval '1 minute'
--   ORDER BY xact_start;
--
--   -- 2. the build (re-runnable; an interrupted one leaves an INVALID
--   --    index that the DO block below clears):
--   SET statement_timeout = 0;
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS hog_data_file_ended
--       ON hog_data_file (catalog_id, end_snapshot) WHERE end_snapshot IS NOT NULL;
--
--   -- 3. valid before promoting:
--   SELECT c.relname, i.indisvalid FROM pg_class c
--   JOIN pg_index i ON i.indexrelid = c.oid
--   WHERE c.relname = 'hog_data_file_ended';
--
-- On a small catalog (dev, a fresh install) none of this matters: the
-- build is milliseconds and the migration is the simplest place for it.
--
-- ============================================================
-- WHY THIS FILE IS NOT TRANSACTIONAL
-- ============================================================
--
-- The index builds CONCURRENTLY, which cannot run inside a transaction.
-- That forces V17's shape: the lock_timeout window is a SAVE-and-
-- RESTORE pair rather than `SET LOCAL`, every statement is idempotent,
-- and a partial failure leaves a `success = false` history row that
-- fails Flyway's validate on every replica until an operator runs
-- `flyway repair`.
--
-- NEITHER RESTORE RUNS ON THE FAILURE PATH, and that is survivable for
-- one reason only, which is a property of the CALLER: `Main.kt`
-- migrates before it binds, so a throw here is a dead pod and the
-- pooled connection carrying the settings dies with the JVM. A future
-- caller that migrates on a live pool would leak them. V14, V15 and V17
-- have the same shape. V19 — which carries this change's two ALTERs —
-- deliberately does NOT: it builds nothing, so it takes V16's
-- transactional shape and rolls back cleanly.
SELECT set_config('hoglake.migration_lock_timeout', current_setting('lock_timeout'), false);
SET lock_timeout = '5s';

-- An INVALID remnant is cleared before the build. `CREATE INDEX
-- CONCURRENTLY IF NOT EXISTS` matches on NAME alone: it would find a
-- half-built index from a cancelled build, skip, and leave one that
-- every ended row's UPDATE maintains and no query may use. A cancelled
-- build is what a killed pod, an operator's Ctrl-C or a statement
-- timeout leaves behind. Plain `DROP INDEX` takes ACCESS EXCLUSIVE on
-- the TABLE and cannot be CONCURRENTLY inside a DO block, so it runs
-- under the 5 s bound: fail fast and retryably rather than convoy the
-- catalog.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_index i ON i.indexrelid = c.oid
        WHERE c.relname = 'hog_data_file_ended' AND NOT i.indisvalid
    ) THEN
        EXECUTE 'DROP INDEX hog_data_file_ended';
    END IF;
END $$;

-- Restore BEFORE the concurrent build: it waits out every transaction
-- older than itself, which is exactly the wait a 5 s lock_timeout would
-- abort.
SELECT set_config('lock_timeout', current_setting('hoglake.migration_lock_timeout'), false);

-- `statement_timeout = 0` for the build and nothing else. A pod's
-- session carries 60 s (Database.SESSION_INIT_SQL); a build the bound
-- kills leaves an INVALID index and a failed history row, and the DO
-- block above is the other half of that story.
SELECT set_config('hoglake.migration_statement_timeout', current_setting('statement_timeout'), false);
SET statement_timeout = 0;

CREATE INDEX CONCURRENTLY IF NOT EXISTS hog_data_file_ended
    ON hog_data_file (catalog_id, end_snapshot) WHERE end_snapshot IS NOT NULL;

SELECT set_config('statement_timeout', current_setting('hoglake.migration_statement_timeout'), false);
