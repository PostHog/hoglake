-- THE CATALOG DROP'S CASCADE PROBE INTO THE REMOVAL LEDGER (#412).
--
-- `DELETE /v1/catalogs/{catalog}` deletes the hog_catalog row and lets
-- the ON DELETE CASCADE foreign keys remove the rest. Each cascade is an
-- RI trigger running `DELETE FROM ONLY <child> WHERE catalog_id = $1`,
-- and every child has a catalog_id-leading index for it EXCEPT
-- hog_file_removal: its two indexes (hog_file_removal_drain, V1;
-- hog_file_removal_undrained_path, V16) are both partial on
-- `drained_at IS NULL`, and the rows the cascade has to find are
-- exactly the SETTLED ones (the drop refuses while any row is
-- undrained). So without this index every drop is a sequential scan of
-- the fleet-wide ledger — the statement shape of the 2026-09-28 outage
-- (server/README.md §Retention: the ledger purge's unindexed
-- `DELETE ... WHERE catalog_id = :c AND drained_at < cutoff`).
--
-- AND THE LEDGER DOES NOT SHRINK. ~137M rows is its steady state at
-- 190k drained/h and the 30-day default retention, not a backlog. The
-- drop runs under the session's 60 s statement_timeout, holding the
-- catalog's commit lock, its FOR UPDATE row and ROW EXCLUSIVE on the
-- children while it scans; past the timeout it rolls back, every time,
-- on exactly the environment the route exists for.
--
-- MEASURED (PG 18, 5,000,000 rows with production-length paths across
-- 40 catalogs, warm, serial):
--   without the index  151,427 buffers, 176 ms (Seq Scan; linear in the
--                      ledger: ~4.1M buffers, ~32 GiB, at 137M rows)
--   with it            6 buffers, 0.03 ms (Index Scan)
--   index size         6.9 bytes/row (B-tree deduplication: catalog_id
--                      has a handful of distinct values), so ~950 MB at
--                      137M rows
-- THE PRICE is one 8-byte-key index entry per removal-queue INSERT —
-- ~53/s at the 190k/h production rate, on a table whose inserts already
-- maintain two indexes — and the space above. Not partial: the settled
-- rows are what it exists to find.
--
-- CONCURRENTLY, IN V19'S SHAPE AND FOR V19'S REASON: a plain CREATE INDEX
-- would hold SHARE on the ledger for the whole build and block every
-- commit's queue insert and every cleanup settle. That needs
-- executeInTransaction=false (the .conf beside this file); the
-- lock_timeout window covers only the invalid-index repair, and the
-- build itself runs with statement_timeout = 0.
SELECT set_config('hoglake.migration_lock_timeout', current_setting('lock_timeout'), false);
SET lock_timeout = '5s';

-- A cancelled CONCURRENTLY build leaves an INVALID index under the name;
-- `IF NOT EXISTS` would then skip the rebuild and leave it unusable.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_index i ON i.indexrelid = c.oid
        WHERE c.relname = 'hog_file_removal_catalog' AND NOT i.indisvalid
    ) THEN
        EXECUTE 'DROP INDEX hog_file_removal_catalog';
    END IF;
END $$;

SELECT set_config('lock_timeout', current_setting('hoglake.migration_lock_timeout'), false);

SELECT set_config('hoglake.migration_statement_timeout', current_setting('statement_timeout'), false);
SET statement_timeout = 0;

CREATE INDEX CONCURRENTLY IF NOT EXISTS hog_file_removal_catalog
    ON hog_file_removal (catalog_id);

SELECT set_config('statement_timeout', current_setting('hoglake.migration_statement_timeout'), false);
