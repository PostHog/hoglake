-- DuckLake scalar-type parity, phase 1: ten new flat column types join
-- the closed vocabulary on hog_column.col_type — int8, int16, uint8,
-- uint16, uint32, uint64, timestamp_s, timestamp_ms, timestamp_ns and
-- json. Each one has a defined Iceberg facade mapping recorded in
-- iceberg-federation.md §2, which is the membership rule for this set.
--
-- The vocabulary is a CHECK constraint, so extending it is a drop and a
-- recreate under the SAME auto-generated name the inline CHECK in
-- V1__init.sql produced (hog_column_col_type_check) — schema.sql keeps
-- the constraint inline, and the schema-equivalence gate compares
-- pg_get_constraintdef output, so the member ORDER below must match
-- schema.sql's list exactly.
--
-- Cost: ADD CONSTRAINT validates with a full scan under ACCESS
-- EXCLUSIVE. hog_column holds one row per column version, not per data
-- row, so this is DDL-sized work even on the largest catalogs — but the
-- LOCK is the risk, not the scan. ACCESS EXCLUSIVE on hog_column queues
-- behind every in-flight DDL/commit and then blocks every one that
-- arrives after it, so an unbounded wait would convoy the whole
-- instance. lock_timeout makes the migration fail fast and retryably
-- instead (the V2/V3 save-and-restore pattern, applied to lock_timeout
-- rather than statement_timeout because here it is acquisition, not
-- execution, that can hang).
--
-- Names DuckLake has that hoglake refuses permanently (int128, uint128,
-- timetz, interval, the geometry family) are deliberately absent: they
-- have no Iceberg mapping, so they are rejected at the API with a named
-- 422 (ColType.REFUSALS) and can never reach this constraint.
SELECT set_config('hoglake.migration_lock_timeout', current_setting('lock_timeout'), false);
SET lock_timeout = '5s';

-- The DROP hardcodes V1's auto-generated constraint name. If a catalog
-- diverged (hand-patched constraint, restored from a doctored dump), a
-- bare DROP fails with "constraint does not exist" and no hint about
-- what the migration expected. Check first and say so.
DO $$
DECLARE
    found_def text;
BEGIN
    SELECT pg_get_constraintdef(oid) INTO found_def
    FROM pg_constraint
    WHERE conrelid = 'hog_column'::regclass AND conname = 'hog_column_col_type_check';

    IF found_def IS NULL THEN
        RAISE EXCEPTION
            'V4 expected the constraint hog_column_col_type_check on hog_column (created '
            'by V1__init.sql as an inline CHECK) but it is absent. This catalog has '
            'diverged from the migration chain; reconcile hog_column''s col_type CHECK '
            'with V1 before re-running.';
    END IF;
    IF found_def NOT LIKE '%col_type%' THEN
        RAISE EXCEPTION
            'V4 found hog_column_col_type_check but it does not constrain col_type (%). '
            'Refusing to replace a constraint this migration does not recognise.', found_def;
    END IF;
END
$$;

ALTER TABLE hog_column DROP CONSTRAINT hog_column_col_type_check;
ALTER TABLE hog_column ADD CONSTRAINT hog_column_col_type_check CHECK (col_type IN (
    'boolean', 'int8', 'int16', 'int', 'long', 'uint8', 'uint16',
    'uint32', 'uint64', 'float', 'double', 'decimal', 'date', 'time',
    'timestamp_s', 'timestamp_ms', 'timestamp', 'timestamp_ns',
    'timestamptz', 'string', 'json', 'uuid', 'binary'));

SELECT set_config('lock_timeout', current_setting('hoglake.migration_lock_timeout'), false);
