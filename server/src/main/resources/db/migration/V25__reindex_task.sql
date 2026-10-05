-- THE DAILY INDEX-BLOAT CHECK GETS A LEDGER TASK (#268): `ReindexService`
-- records its runs in hog_maintenance_run under task 'reindex', so the
-- vocabulary CHECK gains the value. Nothing else changes: the task reads
-- pg_class / pg_index / pg_stats and runs REINDEX INDEX CONCURRENTLY,
-- neither of which needs a column or an index of its own.
--
-- V20's shape, verbatim: a CHECK-constrained vocabulary is a
-- constraint swap, `NOT VALID` then `VALIDATE CONSTRAINT`. V20's comment
-- says the VALIDATE then runs under SHARE UPDATE EXCLUSIVE; IN ONE
-- TRANSACTION IT DOES NOT — the DROP and the ADD have already taken
-- ACCESS EXCLUSIVE on hog_maintenance_run, held to commit, so the
-- VALIDATE's scan runs under it too. Harmless here: the ledger is seven
-- days of runs (tens of thousands of rows per catalog) and its writers
-- are post-run, best-effort inserts. Every statement sits inside the
-- lock_timeout window (`MigrationLockWindowTest`).
--
-- If another V25 lands first, renumber this file and its test (freshDatabaseAt, the >= 25 assertion) and the AGENT.md reference.
--
-- TRANSACTIONAL (no `.conf`): a failure rolls the whole file back and
-- writes no history row, so the next pod simply runs it again. The
-- DROP is `IF EXISTS` and the ADD is guarded, so a re-run is free.
--
-- DEPLOY NOTE: the only table touched is the run ledger, whose writers
-- are post-run best-effort inserts (`MaintenanceRunStore.record`); one
-- that queues behind this file's lock waits at most the 5 s window, and
-- one that fails is logged and swallowed. No sweep holds a lock on it.
SET LOCAL lock_timeout = '5s';

ALTER TABLE hog_maintenance_run DROP CONSTRAINT IF EXISTS hog_maintenance_run_task_check;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'hog_maintenance_run_task_check'
          AND conrelid = 'hog_maintenance_run'::regclass
    ) THEN
        EXECUTE $ddl$
            ALTER TABLE hog_maintenance_run
                ADD CONSTRAINT hog_maintenance_run_task_check
                CHECK (task IN ('hydrator', 'expiry', 'cleanup',
                                'compaction', 'verify', 'retirement',
                                'reindex'))
                NOT VALID
        $ddl$;
    END IF;
END $$;
ALTER TABLE hog_maintenance_run VALIDATE CONSTRAINT hog_maintenance_run_task_check;
