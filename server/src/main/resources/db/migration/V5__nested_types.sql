-- DuckLake type parity, phase 2: the three CONTAINER types — list,
-- struct and map — join the closed vocabulary on hog_column.col_type,
-- and hog_column gains the self-reference that makes a column a TREE.
--
-- Each of the three maps natively to the Iceberg V2 type of the same
-- name, with the element/key/value field ids round-tripping unchanged
-- (iceberg-federation.md §2.8) — which is the membership rule for this
-- set, exactly as it was for phase 1's scalars.
--
-- Three changes, one lock window:
--
--  1. parent_field_id: the tree edge. It is a same-table reference by
--     (catalog_id, table_id, field_id) IDENTITY, deliberately NOT a
--     foreign key — hog_column rows are VERSIONED (begin_snapshot is
--     part of the primary key), so a real FK would have to name a
--     specific version of the parent and would then break the moment
--     the parent is renamed or promoted, which end-snapshots that
--     version and inserts another. Field ids are stable across
--     versions; the versions are not. Parent linkage therefore rides
--     the id, and the service layer (ColumnTrees / TableRepo.columnsAt)
--     assembles the tree per snapshot under the ordinary visibility
--     predicate.
--
--     NULL means "top-level column", which is exactly what every
--     pre-phase-2 row is — so the column is added NULLable with no
--     default and no backfill: adding a NULLable column with no default
--     is catalog-only in PG11+, no table rewrite.
--
--  2. The col_type CHECK is recreated with 'list', 'struct' and 'map'
--     APPENDED. Order is load-bearing (the schema-equivalence gate
--     compares pg_get_constraintdef text, and ScalarTypeParityTest
--     compares this list against the ColType enum and the OpenAPI enum),
--     and appending rather than inserting also keeps the committed fuzz
--     seed corpus pointed at the types its file names claim.
--
--  3. hog_column_live_ordinal is rebuilt to be per-PARENT. Ordinals
--     order SIBLINGS now, so two struct fields in different structs may
--     both be ordinal 0; the old table-wide unique index would have
--     refused the second one. NULLS NOT DISTINCT (PG15+) is what keeps
--     the old guarantee intact for top-level columns, whose
--     parent_field_id is NULL: without it PostgreSQL treats every NULL
--     as distinct and the duplicate-ordinal corruption vector the index
--     exists to stop would quietly come back for exactly the rows it
--     used to cover.
--
-- Cost and lock: ADD COLUMN is catalog-only; ADD CONSTRAINT validates
-- with a full scan; the index rebuild scans too. hog_column holds one
-- row per column VERSION, not per data row, so the scans are DDL-sized
-- even on the largest catalogs — but the LOCK is the risk, not the
-- scan. ACCESS EXCLUSIVE on hog_column queues behind every in-flight
-- DDL/commit and then blocks every one that arrives after it, so an
-- unbounded wait would convoy the whole instance. lock_timeout makes
-- the migration fail fast and retryably instead (V4's pattern).
SELECT set_config('hoglake.migration_lock_timeout', current_setting('lock_timeout'), false);
SET lock_timeout = '5s';

-- The DROP hardcodes V1's auto-generated constraint name (V4 recreated
-- it under the same name). If a catalog diverged — hand-patched
-- constraint, restored from a doctored dump — a bare DROP fails with
-- "constraint does not exist" and no hint about what the migration
-- expected. Check first and say so.
DO $$
DECLARE
    found_def text;
BEGIN
    SELECT pg_get_constraintdef(oid) INTO found_def
    FROM pg_constraint
    WHERE conrelid = 'hog_column'::regclass AND conname = 'hog_column_col_type_check';

    IF found_def IS NULL THEN
        RAISE EXCEPTION
            'V5 expected the constraint hog_column_col_type_check on hog_column (created '
            'by V1__init.sql and recreated by V4__scalar_types.sql) but it is absent. This '
            'catalog has diverged from the migration chain; reconcile hog_column''s col_type '
            'CHECK with V4 before re-running.';
    END IF;
    IF found_def NOT LIKE '%col_type%' THEN
        RAISE EXCEPTION
            'V5 found hog_column_col_type_check but it does not constrain col_type (%). '
            'Refusing to replace a constraint this migration does not recognise.', found_def;
    END IF;
    IF found_def NOT LIKE '%timestamp_ns%' THEN
        RAISE EXCEPTION
            'V5 found hog_column_col_type_check without V4''s scalar types (%). This catalog '
            'is at a pre-V4 vocabulary; run the chain in order.', found_def;
    END IF;
END
$$;

-- Same guard for the ordinal index: V5 replaces it, and replacing an
-- index this migration does not recognise would silently drop whatever
-- guarantee the divergent one was carrying.
DO $$
BEGIN
    IF to_regclass('hog_column_live_ordinal') IS NULL THEN
        RAISE EXCEPTION
            'V5 expected the index hog_column_live_ordinal on hog_column (created by '
            'V1__init.sql) but it is absent. This catalog has diverged from the migration '
            'chain; reconcile it with V1 before re-running.';
    END IF;
END
$$;

ALTER TABLE hog_column ADD COLUMN parent_field_id bigint;

ALTER TABLE hog_column DROP CONSTRAINT hog_column_col_type_check;
ALTER TABLE hog_column ADD CONSTRAINT hog_column_col_type_check CHECK (col_type IN (
    'boolean', 'int8', 'int16', 'int', 'long', 'uint8', 'uint16',
    'uint32', 'uint64', 'float', 'double', 'decimal', 'date', 'time',
    'timestamp_s', 'timestamp_ms', 'timestamp', 'timestamp_ns',
    'timestamptz', 'string', 'json', 'uuid', 'binary',
    'list', 'struct', 'map'));

-- A column is never its own parent. Cheap, and it is the one cycle a
-- single row can express on its own; deeper cycles are impossible by
-- construction (ids are allocated strictly increasing, parents before
-- children) and are not worth a recursive CHECK.
ALTER TABLE hog_column ADD CONSTRAINT hog_column_parent_not_self
    CHECK (parent_field_id IS NULL OR parent_field_id <> field_id);

DROP INDEX hog_column_live_ordinal;
CREATE UNIQUE INDEX hog_column_live_ordinal
    ON hog_column (catalog_id, table_id, parent_field_id, ordinal) NULLS NOT DISTINCT
    WHERE end_snapshot IS NULL;

SELECT set_config('lock_timeout', current_setting('hoglake.migration_lock_timeout'), false);
