-- THE PACED RETIREMENT OF DROPPED TABLES (#193): the two schema
-- changes the loop needs on `hog_table` and on the run ledger.
--
-- Drop is now O(columns): `TableRepo.markDropped` sets
-- `hog_table.dropped_snapshot`, ends the one live version row and the
-- column rows, and touches NO file row. A table's rows are unreachable
-- because the TABLE says so. `RetirementService` deletes those rows
-- later, in paced batches, once the drop snapshot has sunk to or below
-- the catalog's expiry floor, queueing every object path for the
-- cleanup drain.
--
-- SPLIT FROM V18 DELIBERATELY. V18 adds `hog_data_file_ended`, which is
-- a pure read-path win for the EXISTING expiry sweep, ships on its own,
-- and touches no table this file touches. This one carries the two
-- statements that take ACCESS EXCLUSIVE on `hog_table` and
-- `hog_maintenance_run`, so it is the half an operator has to time
-- against in-flight sweeps — see the deploy note at the bottom.
--
-- ============================================================
-- 1. hog_table.retirement_eligible_at
-- ============================================================
--
-- When a retirement sweep FIRST observed this dropped table at or below
-- the floor. NULL default, no rewrite, metadata-only — but still
-- ACCESS EXCLUSIVE, so it sits inside the lock_timeout window like
-- every other ALTER TABLE (AGENT.md's migration rule;
-- `MigrationLockWindowTest` reads this file off disk and enforces it).
--
-- Nothing on the request path reads it. `/verify`'s orphans check does:
-- it calls a dropped table's surviving rows a LEAK only once the table
-- has been ELIGIBLE for several retirement intervals. Dating that from
-- the DROP instead would fire on a catalog whose floor simply has not
-- reached the drop yet — which is a system working exactly as designed,
-- and an alert that fires on a healthy system is an alert nobody reads.
-- It is stamped ONCE (`WHERE retirement_eligible_at IS NULL`), so a
-- table that takes fifty runs to retire keeps its first observation.
--
-- ============================================================
-- 2. hog_maintenance_run.task gains 'retirement'
-- ============================================================
--
-- A CHECK-constrained vocabulary (AGENT.md's deliberate choice over PG
-- enums while the vocabulary churns), so a new task is a constraint
-- swap. `NOT VALID` then `VALIDATE CONSTRAINT`, per the rule for a
-- CHECK over a table that is not known-tiny: the ledger holds seven
-- days of runs at a few per minute per catalog, which is tens of
-- thousands of rows per catalog and grows with the fleet. The ADD is
-- then a catalog write rather than a scan, and the VALIDATE takes SHARE
-- UPDATE EXCLUSIVE instead of ACCESS EXCLUSIVE — it does not block
-- readers or writers, only other schema changes. Both still sit inside
-- the window: `MigrationLockWindowTest` treats every `ALTER TABLE` as
-- heavy, on purpose, because the modifier that makes one cheap is easy
-- to lose in an edit.
--
-- The DROP names the constraint Postgres generated for V2's inline
-- CHECK (`<table>_<column>_check`), and `IF EXISTS` so a re-run is
-- free. `DO $$` around the ADD for the same reason: `ADD CONSTRAINT`
-- has no `IF NOT EXISTS`.
--
-- ============================================================
-- WHY THIS FILE IS TRANSACTIONAL AND V18 IS NOT
-- ============================================================
--
-- V18 builds an index CONCURRENTLY, which cannot run inside a
-- transaction, so it pays V17's price: `executeInTransaction=false`, a
-- save-and-restore `lock_timeout` pair, and a partial failure that
-- leaves a `success = false` history row failing Flyway's validate on
-- every replica until an operator runs `flyway repair`.
--
-- Nothing here builds an index, so this file takes V16's shape instead:
-- no `.conf` at all, Flyway's default `executeInTransaction=true`,
-- `SET LOCAL` that expires with the transaction and needs no restore,
-- and — the part that matters — A FAILURE ROLLS THE WHOLE FILE BACK AND
-- WRITES NO HISTORY ROW, so the next pod simply runs V19 again. The
-- statements are still idempotent, because being re-runnable costs
-- nothing and being wrong about which shape you are in costs a manual
-- repair on every replica.
--
-- DEPLOY NOTE, and it is the reason this file is worth timing. Both
-- ALTERs need ACCESS EXCLUSIVE on their table, and an EXPIRY SWEEP
-- holds ACCESS SHARE on `hog_table` for its whole transaction (step 5
-- deletes from `hog_table_version`, whose FK references it). A sweep
-- that runs long — the first sweep after a deploy on a catalog whose
-- floor is far behind can — will make this file's `SET LOCAL
-- lock_timeout = '5s'` fire, which rolls the migration back cleanly and
-- crash-loops the pod until the sweep finishes. That is the DESIGNED
-- behaviour (fail fast and retryably rather than convoy the catalog),
-- but it is a deploy that appears to hang, so: check
-- `pg_stat_activity` for an in-flight expiry sweep before promoting.
-- The PR body's runbook has the query and the `HOGLAKE_EXPIRY_BATCH`
-- lever that keeps sweeps short.
SET LOCAL lock_timeout = '5s';

-- Metadata-only (NULL default, no rewrite) and still ACCESS EXCLUSIVE.
ALTER TABLE hog_table ADD COLUMN IF NOT EXISTS retirement_eligible_at timestamptz;

-- The task vocabulary gains 'retirement'.
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
                                'compaction', 'verify', 'retirement'))
                NOT VALID
        $ddl$;
    END IF;
END $$;
ALTER TABLE hog_maintenance_run VALIDATE CONSTRAINT hog_maintenance_run_task_check;
