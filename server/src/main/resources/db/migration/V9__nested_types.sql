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
    found_def      text;
    found_types    text[];
    found_skeleton text;
    -- V4's vocabulary, in V4's order. The order is load-bearing: the
    -- schema-equivalence gate compares the normalized constraint text,
    -- so a catalog whose CHECK lists the same 23 names in a different
    -- order is NOT the state this migration appends to.
    expected_types CONSTANT text[] := ARRAY[
        'boolean', 'int8', 'int16', 'int', 'long', 'uint8', 'uint16',
        'uint32', 'uint64', 'float', 'double', 'decimal', 'date', 'time',
        'timestamp_s', 'timestamp_ms', 'timestamp', 'timestamp_ns',
        'timestamptz', 'string', 'json', 'uuid', 'binary'];
BEGIN
    SELECT pg_get_constraintdef(oid) INTO found_def
    FROM pg_constraint
    WHERE conrelid = 'hog_column'::regclass AND conname = 'hog_column_col_type_check';

    IF found_def IS NULL THEN
        RAISE EXCEPTION
            'V9 expected the constraint hog_column_col_type_check on hog_column (created '
            'by V1__init.sql and recreated by V4__scalar_types.sql) but it is absent. This '
            'catalog has diverged from the migration chain; reconcile hog_column''s col_type '
            'CHECK with V4 before re-running.';
    END IF;
    IF found_def NOT LIKE '%col_type%' THEN
        RAISE EXCEPTION
            'V9 found hog_column_col_type_check but it does not constrain col_type (%). '
            'Refusing to replace a constraint this migration does not recognise.', found_def;
    END IF;

    -- The DEFINITION, not a substring of it. V9 DROPs this constraint
    -- and recreates it, so one that merely MENTIONS a V4 type while
    -- permitting a different vocabulary — a hand-patched catalog that
    -- allows 'variant', say — would be discarded silently, which is the
    -- divergence this guard claims to catch.
    --
    -- Compared as the extracted MEMBER LIST rather than as one exact
    -- string: pg_get_constraintdef renders `IN (...)` as
    -- `= ANY (ARRAY['boolean'::text, ...])`, and the casts and spacing
    -- in that rendering are Postgres's business and have changed
    -- before. The vocabulary and its order are the guarantee V4 made;
    -- pin those and nothing else.
    SELECT array_agg(m[1] ORDER BY ord) INTO found_types
    FROM regexp_matches(found_def, '''([a-z0-9_]+)''', 'g') WITH ORDINALITY AS t(m, ord);

    IF found_types IS DISTINCT FROM expected_types THEN
        RAISE EXCEPTION
            'V9 expected hog_column_col_type_check to permit exactly V4''s vocabulary, in V4''s '
            'order. Expected: %. Found: % (from %). V9 replaces this constraint and will not '
            'silently discard a vocabulary it does not recognise; reconcile with V4 before '
            're-running.', expected_types, found_types, found_def;
    END IF;

    -- The member list is only half the constraint. The other half is
    -- what it DOES with them, and a list comparison cannot see that:
    --   a NOT IN over the same 23 names        -- inverted
    --   the same membership test, OR'd with true -- vacuous
    --   a membership test on a DIFFERENT column, AND'd with a
    --     col_type IS NOT NULL that mentions this one
    -- all yield identical found_types and all passed. V9 then dropped
    -- them and installed its own, discarding exactly the divergence this
    -- guard exists to refuse.
    --
    -- So compare the SHAPE too, by skeletonising: strip the quoted
    -- members and their ::text casts, drop the separators they leave
    -- behind, and collapse whitespace. What remains is the operator and
    -- the parenthesisation, which is the part being asserted. Casts and
    -- spacing stay excluded on purpose -- those are Postgres's rendering
    -- and have changed between versions; `= ANY (ARRAY[...])` is how
    -- every supported version renders an IN-list.
    found_skeleton := regexp_replace(found_def, '''[a-z0-9_]+''(::text)?', '', 'g');
    found_skeleton := regexp_replace(found_skeleton, '[ ,]+', ' ', 'g');
    found_skeleton := btrim(regexp_replace(found_skeleton, '\[ \]', '[]', 'g'));

    IF found_skeleton NOT IN (
        'CHECK ((col_type = ANY (ARRAY[])))',
        'CHECK (col_type = ANY (ARRAY[]))'
    ) THEN
        RAISE EXCEPTION
            'V9 found hog_column_col_type_check listing V4''s vocabulary, but its SHAPE is not '
            'a plain membership test: expected a skeleton of CHECK ((col_type = ANY (ARRAY[]))), '
            'found % (from %). A NOT IN, an OR, or an extra conjunct permits a different '
            'vocabulary while listing the same names, and V9 will not silently discard one; '
            'reconcile with V4 before re-running.', found_skeleton, found_def;
    END IF;
END
$$;

-- Same guard for the ordinal index: V9 replaces it, and replacing an
-- index this migration does not recognise would silently drop whatever
-- guarantee the divergent one was carrying.
DO $$
DECLARE
    found_def text;
BEGIN
    IF to_regclass('hog_column_live_ordinal') IS NULL THEN
        RAISE EXCEPTION
            'V9 expected the index hog_column_live_ordinal on hog_column (created by '
            'V1__init.sql) but it is absent. This catalog has diverged from the migration '
            'chain; reconcile it with V1 before re-running.';
    END IF;

    -- EXISTENCE is not the check. The next statement DROPs this index
    -- and replaces it with a per-parent one, so an index that merely
    -- shares V1's NAME while guarding something else would be discarded
    -- silently — which is precisely the divergence this guard claims to
    -- catch. Compare the DEFINITION.
    --
    -- Matched by shape rather than by one exact string: pg_get_indexdef
    -- schema-qualifies the table and normalises spacing, and both vary
    -- with search_path and server version. The three clauses below are
    -- the guarantee V1 actually made — unique, keyed on
    -- (catalog_id, table_id, ordinal), partial on the live rows — and
    -- losing any one of them is what would matter.
    -- ON THE RIGHT TABLE. Index names are unique per schema, not per
    -- table, so a divergent catalog can carry V1's index NAME on some
    -- other relation entirely -- and the definition checks below would
    -- all pass while the guarantee they describe protects nothing here.
    IF (SELECT indrelid FROM pg_index WHERE indexrelid = 'hog_column_live_ordinal'::regclass)
       IS DISTINCT FROM 'hog_column'::regclass THEN
        RAISE EXCEPTION
            'V9 found an index named hog_column_live_ordinal, but it is not on hog_column. '
            'This catalog has diverged from the migration chain; reconcile it with V1 before '
            're-running.';
    END IF;

    SELECT pg_get_indexdef(indexrelid) INTO found_def
    FROM pg_index
    WHERE indexrelid = 'hog_column_live_ordinal'::regclass;

    IF found_def NOT LIKE 'CREATE UNIQUE INDEX%'
       OR found_def NOT LIKE '%(catalog_id, table_id, ordinal)%'
       OR found_def NOT LIKE '%WHERE (end_snapshot IS NULL)%' THEN
        RAISE EXCEPTION
            'V9 found an index named hog_column_live_ordinal whose definition is not the one '
            'V1__init.sql created. Expected a UNIQUE index on '
            '(catalog_id, table_id, ordinal) WHERE end_snapshot IS NULL; found: %. '
            'V9 replaces this index with a per-parent one and will not silently discard a '
            'guarantee it does not recognise; reconcile it with V1 before re-running.', found_def;
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
-- Ids are allocated depth-first, parents before children, so a parent's
-- id is always BELOW its children's. That ordering is what makes
-- "deeper cycles are impossible by construction" true, and until now it
-- was only a comment. As a row-local CHECK it costs nothing, it
-- subsumes the self-reference above (kept separately because its
-- message is the one a confused client needs), and it makes a cycle
-- unrepresentable rather than merely unlikely.
ALTER TABLE hog_column ADD CONSTRAINT hog_column_parent_precedes_child
    CHECK (parent_field_id IS NULL OR parent_field_id < field_id);

DROP INDEX hog_column_live_ordinal;
CREATE UNIQUE INDEX hog_column_live_ordinal
    ON hog_column (catalog_id, table_id, parent_field_id, ordinal) NULLS NOT DISTINCT
    WHERE end_snapshot IS NULL;

SELECT set_config('lock_timeout', current_setting('hoglake.migration_lock_timeout'), false);
