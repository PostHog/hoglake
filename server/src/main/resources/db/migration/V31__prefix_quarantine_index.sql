-- THE FOURTH ATTENTION INDEX: hog_data_file_prefix_quarantined (PR 2.1c).
--
-- Serves `CatalogMetrics.SAMPLE_SQL`'s `prefix_quarantined` count, the
-- gauge `hoglake_object_prefix_quarantined_files{catalog}`: LIVE files
-- the hydrator flipped to provided with `prefix_quarantined` set (V30).
-- V26's shape and reason exactly: partial on the rare state AND on
-- liveness, so an ordinary append (`prefix_quarantined = false`) never
-- enters it and the hottest write in the system pays nothing; `table_id`
-- in the key, so the count is an index-only range per catalog plus one
-- hog_table probe per distinct table, which is how it excludes dropped
-- tables. Without it the subselect is a scan of the catalog's manifest
-- every metrics tick.
--
-- MEASURED (PG 18, 5,000,000 hog_data_file rows, 1,347 MiB of heap,
-- warm, serial, no row qualifying):
--   V30's ADD COLUMN   0.7 ms, heap size unchanged (no rewrite)
--   plain build        447-466 ms
--   CONCURRENTLY       742-746 ms
--   index size         8 KiB (one page)
-- Both builds are a full heap scan (CONCURRENTLY two), so the cost
-- grows with the RELATION, not with the few rows that enter it: ~2.8x
-- these at prod-us's ~14M rows warm, and cold it is read throughput,
-- ~11 s a pass over this heap at 125 MB/s (computed, not measured).
--
-- CONCURRENTLY, THEREFORE, IN V28'S SHAPE: a plain build holds SHARE on
-- hog_data_file for that whole scan, blocking every commit in the fleet
-- past the admission bound. The file is NOT transactional (the .conf
-- beside it); the lock_timeout window covers only the invalid-index
-- repair, and the build runs with statement_timeout = 0. Every
-- statement is idempotent. On a large catalog pre-build it out of band
-- (V17's header), USING THIS EXACT NAME AND DEFINITION, so the
-- `IF NOT EXISTS` below is the no-op it then is.
SELECT set_config('hoglake.migration_lock_timeout', current_setting('lock_timeout'), false);
SET lock_timeout = '5s';

-- A cancelled CONCURRENTLY build leaves an INVALID index under the name;
-- `IF NOT EXISTS` would then skip the rebuild and leave it unusable.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_index i ON i.indexrelid = c.oid
        WHERE c.relname = 'hog_data_file_prefix_quarantined' AND NOT i.indisvalid
    ) THEN
        EXECUTE 'DROP INDEX hog_data_file_prefix_quarantined';
    END IF;
END $$;

SELECT set_config('lock_timeout', current_setting('hoglake.migration_lock_timeout'), false);

SELECT set_config('hoglake.migration_statement_timeout', current_setting('statement_timeout'), false);
SET statement_timeout = 0;

CREATE INDEX CONCURRENTLY IF NOT EXISTS hog_data_file_prefix_quarantined
    ON hog_data_file (catalog_id, table_id)
    WHERE prefix_quarantined AND end_snapshot IS NULL;

SELECT set_config('statement_timeout', current_setting('hoglake.migration_statement_timeout'), false);
