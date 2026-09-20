-- Compaction groups files by BIN PACKING on size, not by row-id order.
--
-- Why the order changed: a compaction output takes `row_id_start = min
-- surviving id` of its inputs, so it sorts IN FRONT of the newer, smaller
-- files. Packed in row-id order it ate most of the group's byte quota, the
-- group closed a file or two later, and the whole group was then discarded
-- for holding too few files -- permanently, because row-id order never
-- changes. Sorting by size puts the small files beside each other where
-- they can fill a group between them. See CompactionGrouping.groups.
--
-- MaintenanceSummarySampler reports the debt that planner produces, and it
-- is a bounded keyset scan with no whole-bucket list to sort, so it has to
-- WALK the files in size order. That is this index.

-- The maintenance scan's keyset moved from (table, row_id_start, file) to
-- (table, file_size_bytes, file). The old index still serves the planner's
-- candidate read and the changefeed, so it stays.
--
-- UNLIKE V2 and V3, this does NOT drop-then-recreate unconditionally.
-- hog_data_file is the largest table here, Database.migrate holds a
-- pg_advisory_lock for the whole Flyway run, and CREATE INDEX
-- CONCURRENTLY waits out every transaction older than itself — so on a
-- production-sized manifest this statement is the long pole, and every
-- other booting pod blocks behind it. Written this way the index can be
-- built out of band, in a quiet window, BEFORE the deploy:
--
--   CREATE INDEX CONCURRENTLY hog_data_file_maintenance_size_scan
--       ON hog_data_file (catalog_id, table_id, file_size_bytes, data_file_id);
--
-- after which this migration finds it and is a no-op. An INVALID remnant
-- of an interrupted build is still cleared first, which is what the
-- unconditional drop in V2/V3 was for; a plain DROP is right there,
-- because an invalid index serves no query and nothing can be waiting on
-- it. Leaving it would be worse than the brief lock: IF NOT EXISTS below
-- would see it and skip, and the index would never become valid.
--
-- The lock is bounded, though. A plain DROP INDEX takes AccessExclusive
-- on the TABLE, not just the index, and inside a DO block it cannot be
-- CONCURRENTLY. On a busy hog_data_file an unbounded wait would queue
-- every new query behind it. Fail fast and retryably instead -- the
-- save-and-restore pattern and the 5s value are V4's, V8's and V9's, and
-- restoring matters here because executeInTransaction=false means a
-- clobbered setting would persist for the rest of the Flyway run.
SELECT set_config('hoglake.migration_lock_timeout', current_setting('lock_timeout'), false);
SET lock_timeout = '5s';
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_index i ON i.indexrelid = c.oid
        WHERE c.relname = 'hog_data_file_maintenance_size_scan' AND NOT i.indisvalid
    ) THEN
        EXECUTE 'DROP INDEX hog_data_file_maintenance_size_scan';
    END IF;
END $$;
SELECT set_config('lock_timeout', current_setting('hoglake.migration_lock_timeout'), false);
SELECT set_config('hoglake.migration_statement_timeout', current_setting('statement_timeout'), false);
SET statement_timeout = 0;
CREATE INDEX CONCURRENTLY IF NOT EXISTS hog_data_file_maintenance_size_scan
    ON hog_data_file (catalog_id, table_id, file_size_bytes, data_file_id);
SELECT set_config('statement_timeout', current_setting('hoglake.migration_statement_timeout'), false);

-- A group is judged against ITS OWN largest file: it needs
-- `min(min_input_files, target / largest)` files, never fewer than 2. A
-- fixed minimum is a file count judging a byte target, and no group can
-- hold five files that are each over a fifth of the target -- so a fixed 5
-- silently means "never compact" for any bucket with files that big,
-- including every sorted table, whose effective target is derated to fit
-- its sort buffer in heap.
--
-- Because the scan walks a bucket smallest-first, the largest file in a
-- partial group is simply the last one seen. This column carries it across
-- the page boundaries the scan checkpoints on, so the trailing remainder
-- can be judged by the same rule as a group that closed mid-page.
ALTER TABLE hog_maintenance_summary_tier
    ADD COLUMN IF NOT EXISTS pending_max_bytes bigint NOT NULL DEFAULT 0;
