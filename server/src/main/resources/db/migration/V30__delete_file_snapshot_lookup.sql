-- Exact partition inspection seeks the newest DV version at or before a
-- retained snapshot. The existing (catalog_id, data_file_id) index cannot
-- order a file's retained history: LIMIT above a sort still reads it all.
-- Keep end_snapshot filtering outside the ordered LIMIT so a gap does not
-- turn the seek back into a history scan.
--
-- This index is paid only on DV registration, not ordinary data-file
-- appends. It grows by one entry per retained DV version; the existing
-- snapshot-expiry purge removes those entries with their owning rows.
-- V30DeleteFileSnapshotLookupMigrationIntegrationTest measures the build,
-- index bytes and before/after production query on interleaved histories.
-- PG18 fixture: 200,000 versions, two interleaved catalogs, 100,000 per
-- file. At the middle snapshot the old changefeed/bitmap path examined
-- 50,000 versions; the new seek examined one (4 shared buffers, 0.021 ms,
-- warm/serial). The full aggregate fell from 2,097 buffers / 11.934 ms to
-- 14 buffers / 0.110 ms. Concurrent build: 94 ms; index 9,953,280 bytes,
-- 49.8 bytes per retained version (about 50 MB per million versions).
-- These are fixture measurements, not a deployment-time guarantee.
--
-- Build CONCURRENTLY because a blocking build holds SHARE on the DV
-- manifest, blocking registrations and supersessions for its full scan.
-- On a large installation, pre-build out of band before promotion:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS hog_delete_file_snapshot_lookup
--     ON hog_delete_file
--       (catalog_id, data_file_id, begin_snapshot DESC, delete_file_id DESC);
-- Check for old transactions first: a concurrent build can wait beyond the
-- pod's startup budget. A cancelled build leaves an invalid index; the
-- repair below removes it before retrying.
SELECT set_config('hoglake.migration_lock_timeout', current_setting('lock_timeout'), false);
SET lock_timeout = '5s';

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_index i ON i.indexrelid = c.oid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = current_schema()
          AND c.relname = 'hog_delete_file_snapshot_lookup' AND NOT i.indisvalid
    ) THEN
        EXECUTE 'DROP INDEX hog_delete_file_snapshot_lookup';
    END IF;
END $$;

SELECT set_config('lock_timeout', current_setting('hoglake.migration_lock_timeout'), false);
SELECT set_config('hoglake.migration_statement_timeout', current_setting('statement_timeout'), false);
SET statement_timeout = 0;

CREATE INDEX CONCURRENTLY IF NOT EXISTS hog_delete_file_snapshot_lookup
    ON hog_delete_file (catalog_id, data_file_id, begin_snapshot DESC, delete_file_id DESC);

SELECT set_config('statement_timeout', current_setting('hoglake.migration_statement_timeout'), false);
