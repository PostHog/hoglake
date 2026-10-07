-- THE MANIFEST'S ATTENTION INDEXES: three partial indexes, one per
-- rare state the hydrator and the metrics sampler ask about, so neither
-- ever walks hog_data_file (#269).
--
-- `CatalogMetrics.SAMPLE_SQL` was one full pass over hog_data_file
-- every 15 s on every pod — ~1.5 GiB of buffer traffic per tick on
-- gigahog-prod-us (Config.kt, `metricsIntervalMs`) — because its live
-- totals had no index to answer them and its three state counts had
-- one between them. The live totals now come from the maintenance
-- summary (V27: stamped per catalog at publish, one row per catalog
-- per tick). The three counts come from these:
--
--   hog_data_file_pending            LIVE rows awaiting hydration — the
--                                    hydrator's queue and the backlog
--                                    gauge. REBUILT here (it existed
--                                    since V1) to add `end_snapshot IS
--                                    NULL` to its predicate and
--                                    `table_id` to its payload.
--   hog_data_file_failed             LIVE rows whose hydration failed
--                                    structurally, the operator's
--                                    requeue backlog.
--   hog_data_file_missing_field_ids  LIVE rows whose parquet schema
--                                    binds columns by name, the
--                                    rename-refusal population.
--
-- LIVE IN EVERY PREDICATE. The hydrator's claim skips ended rows
-- (#269: a file compaction or an overwrite ended before the sweep
-- reached it has no reader left to serve, so its footer GET buys
-- nothing), and a row the claim skips must not sit in the index the
-- claim walks: a `pending` index without the liveness term would keep
-- every such row forever, and each sweep would heap-fetch and discard
-- the lot before reaching its first live row. With the term in the
-- predicate, the UPDATE that ends a row removes it from the index at
-- that moment, and the index holds exactly the queue. The same term
-- keeps the failed and missing-field-id populations at the live rows
-- the gauges count.
--
-- `table_id` IN EVERY INDEX. Each count joins hog_table to exclude a
-- dropped table's rows (the drop touches no file row, so the join is
-- the only way — CatalogMetrics.SAMPLE_SQL's KDoc), and the join needs
-- the row's table_id. In the key for the two counting-only indexes,
-- and as an INCLUDE payload on the pending index (whose key order,
-- `(catalog_id, data_file_id)`, IS the claim's order), so each count
-- is an index-only scan plus one memoized hog_table probe per distinct
-- table, with no heap fetch per counted row. "Index-only" is what the
-- visibility map allows: these rows are by definition recently
-- updated (a state just flipped), so expect heap fetches for the
-- freshest of them until the next vacuum. Either way the cost is the
-- rare population, never the manifest.
--
-- Each index holds only its state's rows (partial), so an ordinary
-- append — `stats_state = 'provided'`, `missing_field_ids = false` —
-- enters only the pending index when it is pending, which it did
-- before; the hottest write in the system pays nothing new.
--
-- SIZE: the pending index is what it was plus eight bytes a row; the
-- other two are a handful of pages each on any instance. At the
-- (unhealthy) extreme of 100,000 failed rows, ~3 MB.
--
-- CONCURRENTLY, IN V19'S SHAPE AND FOR V19'S REASON: a plain build
-- takes SHARE on hog_data_file for the length of a full heap scan,
-- which at prod-us's size is every commit in the fleet blocked past
-- the admission bound. The file is therefore NOT transactional (the
-- .conf beside it), the lock_timeout window is a save-and-restore
-- pair, every statement is idempotent, and an invalid remnant of a
-- cancelled build is dropped before each build. Each build is a full
-- heap scan twice over, bounded by the RELATION, not by the few rows
-- that enter the index: plan on V19's ~24-60 s per index on prod-us
-- (minutes to hours on a 10^9-row manifest, during which the pod is
-- still booting — a startup probe shorter than three builds kills the
-- pod mid-build and the next boot drops the remnant and starts over),
-- and pre-build out of band on a large catalog the way V19's header
-- describes, USING THESE EXACT NAMES AND DEFINITIONS, so each
-- `IF NOT EXISTS` below is the no-op it then is.
--
-- THE PENDING REBUILD keeps the name `hog_data_file_pending` (the plan
-- tests and the hydrator's KDoc know it) without a moment in which no
-- pending index exists: the old index is RENAMED aside (a catalog-only
-- change under the 5 s lock window; the planner serves the claim from
-- it by definition, not by name), the new one is built beside it, and
-- the old one is dropped CONCURRENTLY once the new one is valid. The
-- rename fires only on an index whose predicate lacks the liveness
-- term, so a re-run after success touches nothing.
SELECT set_config('hoglake.migration_lock_timeout', current_setting('lock_timeout'), false);
SET lock_timeout = '5s';

DO $$
DECLARE
    remnant text;
BEGIN
    -- A cancelled CONCURRENTLY build leaves an INVALID index under the
    -- target name; IF NOT EXISTS would keep it. Drop before building.
    FOR remnant IN
        SELECT c.relname FROM pg_class c
        JOIN pg_index i ON i.indexrelid = c.oid
        WHERE c.relname IN ('hog_data_file_failed', 'hog_data_file_missing_field_ids', 'hog_data_file_pending')
          AND NOT i.indisvalid
    LOOP
        EXECUTE format('DROP INDEX %I', remnant);
    END LOOP;
    -- The pre-V26 pending index: same name, no liveness term. Move it
    -- aside so the build below can take the name.
    IF EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_index i ON i.indexrelid = c.oid
        WHERE c.relname = 'hog_data_file_pending' AND i.indisvalid
          AND pg_get_expr(i.indpred, i.indrelid) NOT LIKE '%end_snapshot%'
    ) THEN
        EXECUTE 'ALTER INDEX hog_data_file_pending RENAME TO hog_data_file_pending_old';
    END IF;
END $$;

-- Restore BEFORE the concurrent builds: each waits out every
-- transaction older than itself, which is exactly the wait a 5 s
-- lock_timeout would abort.
SELECT set_config('lock_timeout', current_setting('hoglake.migration_lock_timeout'), false);

-- `statement_timeout = 0` for the builds and nothing else (V19).
SELECT set_config('hoglake.migration_statement_timeout', current_setting('statement_timeout'), false);
SET statement_timeout = 0;

CREATE INDEX CONCURRENTLY IF NOT EXISTS hog_data_file_failed
    ON hog_data_file (catalog_id, table_id)
    WHERE stats_state = 'failed' AND end_snapshot IS NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS hog_data_file_missing_field_ids
    ON hog_data_file (catalog_id, table_id)
    WHERE missing_field_ids AND end_snapshot IS NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS hog_data_file_pending
    ON hog_data_file (catalog_id, data_file_id) INCLUDE (table_id)
    WHERE stats_state = 'pending' AND end_snapshot IS NULL;

-- Waits out every transaction still using the old index (a hydrator
-- sweep in flight), then drops it. Idempotent: gone on a re-run.
DROP INDEX CONCURRENTLY IF EXISTS hog_data_file_pending_old;

SELECT set_config('statement_timeout', current_setting('hoglake.migration_statement_timeout'), false);
