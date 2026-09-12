-- Changefeed access-path indexes. FileRepo.changedIn and
-- DeleteFileReadRepo.changedIn range on (catalog_id, table_id,
-- begin_snapshot) over ALL files — dead ones included, by design: a
-- consumer replaying across a compaction must see the original files,
-- never the compacted output. The only (catalog_id, table_id,
-- begin_snapshot) index (hog_data_file_live) is partial on
-- end_snapshot IS NULL, so the planner cannot use it here and falls
-- back to scanning the table's whole file history: a fixed-width
-- changefeed window costs O(total catalog files), not O(window).
-- Found by the bench quick-smoke regression flag (2.57x slower at
-- 2,000 snapshots than at 500 for the same 100-snapshot window).
--
-- Existing manifests may be large. Build without blocking their writers
-- and without the request connection's 60s statement timeout, restoring
-- the timeout before the connection returns to the pool (the V2
-- pattern). Dropping first recovers INVALID remnants of interrupted
-- concurrent builds when the script is retried after Flyway repair.
SELECT set_config('hoglake.migration_statement_timeout', current_setting('statement_timeout'), false);
SET statement_timeout = 0;
DROP INDEX CONCURRENTLY IF EXISTS hog_data_file_changefeed;
CREATE INDEX CONCURRENTLY hog_data_file_changefeed
    ON hog_data_file (catalog_id, table_id, begin_snapshot);
DROP INDEX CONCURRENTLY IF EXISTS hog_delete_file_changefeed;
CREATE INDEX CONCURRENTLY hog_delete_file_changefeed
    ON hog_delete_file (catalog_id, table_id, begin_snapshot);
SELECT set_config('statement_timeout', current_setting('hoglake.migration_statement_timeout'), false);
