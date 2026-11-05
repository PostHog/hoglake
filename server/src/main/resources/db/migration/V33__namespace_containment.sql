-- NAMESPACE CONTAINMENT: a namespace marked contained for `field = value`.
--
-- A table in a contained namespace `n` may register files only under
-- `<data_path>/data/n/`, and no table outside `n` may register files
-- there (CommitService.checkContainment). That is what lets a caller
-- narrowed to `field = value` (a `ten: scoped` token's `prd`) be given
-- the namespace and its prefix and nothing else. Only an operator marks,
-- clears or drops a contained namespace.
--
-- Both columns or neither. `value` is a positive id, the shape a scoped
-- token's predicate values take. At most one LIVE namespace per
-- (catalog, field, value), so "the namespace for team 42" is one row.
--
-- hog_namespace is one row per namespace ever created in a catalog
-- (thousands at most), so the ADD COLUMNs are catalog-only changes and
-- the unique index is a plain build over a small table: no CONCURRENTLY.
SET LOCAL lock_timeout = '5s';

ALTER TABLE hog_namespace
    ADD COLUMN IF NOT EXISTS contained_field text,
    ADD COLUMN IF NOT EXISTS contained_value bigint;

-- The populated arm says IS NOT NULL outright: a CHECK passes on NULL, so
-- without it `(field, NULL)` would satisfy `field ~ ... AND NULL > 0`.
-- `ADD CONSTRAINT` has no `IF NOT EXISTS`, so `DO $$` keeps a re-run free
-- (V20's shape). Every existing row has both columns NULL, so validating
-- inline is a scan of a small table.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'hog_namespace_containment_shape'
          AND conrelid = 'hog_namespace'::regclass
    ) THEN
        EXECUTE $ddl$
            ALTER TABLE hog_namespace
                ADD CONSTRAINT hog_namespace_containment_shape CHECK (
                    (contained_field IS NULL AND contained_value IS NULL)
                    OR (contained_field IS NOT NULL AND contained_value IS NOT NULL
                        AND contained_field ~ '^[a-z][a-z0-9_]{0,62}$' AND contained_value > 0)
                )
        $ddl$;
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS hog_namespace_live_containment
    ON hog_namespace (catalog_id, contained_field, contained_value)
    WHERE NOT dropped AND contained_field IS NOT NULL;
