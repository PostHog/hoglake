-- The maintenance run ledger: one row per maintenance-task run, whether
-- the BackgroundLoops sweep or the manual /maintenance/* trigger drove
-- it. The queryable answer to "is the loop alive, when did it last run,
-- and what did it do" — the same forensics-ledger position as
-- hog_file_removal's drained rows. Recording never rides the sweep's
-- transaction: the row is inserted after the run resolves, and a
-- recording failure never fails the run.
--
-- Hydrator sweeps are instance-wide, so they record one row per catalog
-- they claimed files for (a no-claim sweep writes nothing — a catalog's
-- waiting hydrator work is the stats_state backlog, not a run). The
-- cleanup sweep purges rows past HOGLAKE_MAINTENANCE_LEDGER_RETENTION_SECONDS
-- (default 7d). The script is nontransactional for concurrent index
-- builds. Table creation is idempotent for recovery of a partial run.
CREATE TABLE IF NOT EXISTS hog_maintenance_run (
    run_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    catalog_id  bigint NOT NULL REFERENCES hog_catalog ON DELETE CASCADE,
    task        text   NOT NULL CHECK (task IN ('hydrator', 'expiry', 'cleanup',
                                                'compaction', 'verify')),
    -- 'loop' = a BackgroundLoops sweep; 'manual' = a /maintenance/*
    -- trigger (for task 'hydrator', manual rows are rehydrate calls).
    run_trigger text   NOT NULL CHECK (run_trigger IN ('loop', 'manual')),
    started_at  timestamptz NOT NULL,
    finished_at timestamptz NOT NULL,
    -- 'failed' = the run itself threw (error carries the detail).
    -- Per-FILE hydration failures are not run failures: they live in the
    -- result counts and the stats_state backlog.
    status      text   NOT NULL CHECK (status IN ('ok', 'failed')),
    error       text,
    -- The task's result payload, serialized exactly as the matching POST
    -- /maintenance/* response body (expiry -> ExpiryResult, cleanup ->
    -- CleanupResult, compaction -> CompactionResult, verify ->
    -- VerifyReport; hydrator loop rows carry per-catalog sweep counts,
    -- manual hydrator rows carry RehydrateResult).
    result      jsonb,
    CHECK (finished_at >= started_at),
    CHECK ((status = 'failed') = (error IS NOT NULL))
);
-- Latest run per task and filtered/unfiltered history access paths.
CREATE INDEX IF NOT EXISTS hog_maintenance_run_recent
    ON hog_maintenance_run (catalog_id, task, run_id DESC);
CREATE INDEX IF NOT EXISTS hog_maintenance_run_catalog_history
    ON hog_maintenance_run (catalog_id, run_id DESC);
CREATE INDEX IF NOT EXISTS hog_maintenance_run_task_history
    ON hog_maintenance_run (task, run_id DESC);

CREATE TABLE IF NOT EXISTS hog_maintenance_summary (
    catalog_id bigint PRIMARY KEY REFERENCES hog_catalog ON DELETE CASCADE,
    generation bigint NOT NULL DEFAULT 0,
    published_generation bigint NOT NULL DEFAULT 0,
    sampled_at timestamptz,
    sample jsonb,
    scan_state jsonb,
    next_batch_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((sampled_at IS NULL) = (sample IS NULL))
);
CREATE INDEX IF NOT EXISTS hog_maintenance_summary_due
    ON hog_maintenance_summary (next_batch_at, catalog_id);

-- Checkpointed per-partition/spec/tier accumulators. The bounded hash
-- is solely an observability key, never an authorization for maintenance.
CREATE TABLE IF NOT EXISTS hog_maintenance_summary_tier (
    catalog_id bigint NOT NULL REFERENCES hog_catalog ON DELETE CASCADE,
    generation bigint NOT NULL,
    bucket_key text NOT NULL,
    table_id bigint NOT NULL,
    spec_id bigint,
    partition_values text[],
    quota bigint NOT NULL CHECK (quota >= 0),
    remaining bigint NOT NULL CHECK (remaining > 0),
    pending integer NOT NULL CHECK (pending >= 0),
    selected bigint NOT NULL DEFAULT 0,
    file_count bigint NOT NULL DEFAULT 0,
    small_count bigint NOT NULL DEFAULT 0,
    total_bytes bigint NOT NULL DEFAULT 0,
    small_bytes bigint NOT NULL DEFAULT 0,
    dv_count bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (catalog_id, generation, bucket_key)
);

-- Existing manifests may be large. Build without blocking their writers
-- and without the request connection's 60s statement timeout. Restore the
-- original timeout before returning the connection to the pool. Dropping
-- these new indexes first also recovers INVALID remnants of interrupted
-- concurrent builds when the script is retried after Flyway repair.
SELECT set_config('hoglake.migration_statement_timeout', current_setting('statement_timeout'), false);
SET statement_timeout = 0;
DROP INDEX CONCURRENTLY IF EXISTS hog_data_file_maintenance_scan;
CREATE INDEX CONCURRENTLY hog_data_file_maintenance_scan
    ON hog_data_file (catalog_id, table_id, row_id_start, data_file_id);
DROP INDEX CONCURRENTLY IF EXISTS hog_delete_file_data_lookup;
CREATE INDEX CONCURRENTLY hog_delete_file_data_lookup
    ON hog_delete_file (catalog_id, data_file_id);
SELECT set_config('statement_timeout', current_setting('hoglake.migration_statement_timeout'), false);
