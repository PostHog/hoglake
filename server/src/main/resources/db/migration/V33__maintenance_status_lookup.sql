-- Mixed metrics recover each status from durable history after a restart.
-- Existing ledger indexes cannot bound a status-filtered read during a long failure streak.
-- The new index serves three equality predicates followed by LIMIT 1 in run order.
-- It adds one entry per maintenance run. The existing ledger purge removes those entries.
-- A PostgreSQL 18 fixture holds 100,001 runs, including 100,000 newer failures.
-- The success probe changes from 100,000 filtered rows to one indexed row with four buffers.
-- The concurrent migration takes 152-216 ms. The index uses 4,997,120 bytes, about 50 bytes per run.
-- These measurements use a warm, serial local fixture. They do not predict a deployment duration.
-- A concurrent build avoids blocking maintenance ledger inserts during its scan.
-- Before retrying a cancelled build on a busy installation, remove the invalid index:
--   DROP INDEX CONCURRENTLY IF EXISTS hog_maintenance_run_status;
-- Then pre-build the same index before promotion if startup cannot wait for its scan.
-- The existing migration repair pattern bounds the fallback table-lock wait to five seconds.
SELECT set_config('hoglake.migration_lock_timeout', current_setting('lock_timeout'), false);
SET lock_timeout = '5s';

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_index i ON i.indexrelid = c.oid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = current_schema()
          AND c.relname = 'hog_maintenance_run_status' AND NOT i.indisvalid
    ) THEN
        EXECUTE 'DROP INDEX hog_maintenance_run_status';
    END IF;
END $$;

SELECT set_config('lock_timeout', current_setting('hoglake.migration_lock_timeout'), false);
SELECT set_config('hoglake.migration_statement_timeout', current_setting('statement_timeout'), false);
SET statement_timeout = 0;

CREATE INDEX CONCURRENTLY IF NOT EXISTS hog_maintenance_run_status
    ON hog_maintenance_run (catalog_id, task, status, run_id DESC);

SELECT set_config('statement_timeout', current_setting('hoglake.migration_statement_timeout'), false);
