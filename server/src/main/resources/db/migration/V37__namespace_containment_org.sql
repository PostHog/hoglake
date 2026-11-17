-- ORG-MARKED CONTAINMENT: a contained namespace may also carry the org that
-- owns it. The org mark moves the namespace's prefix from
-- `<data_path>/data/<name>/` to `<data_path>/org_id=<org>/<field>=<value>/`
-- (Containment.namespaceRoot), and is permanent once set: the service refuses
-- to change or clear it.
--
-- The org is the shape a scoped token's org claim takes. NULL on every
-- existing row, so the ADD COLUMN is a catalog-only change.
SET LOCAL lock_timeout = '5s';

ALTER TABLE hog_namespace
    ADD COLUMN IF NOT EXISTS contained_org text;

-- `ADD CONSTRAINT` has no `IF NOT EXISTS`, so `DO $$` keeps a re-run free
-- (V33's shape). An org only ever accompanies a field and a value.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'hog_namespace_containment_org_shape'
          AND conrelid = 'hog_namespace'::regclass
    ) THEN
        EXECUTE $ddl$
            ALTER TABLE hog_namespace
                ADD CONSTRAINT hog_namespace_containment_org_shape CHECK (
                    contained_org IS NULL
                    OR (contained_field IS NOT NULL AND contained_org ~ '^[a-z0-9][a-z0-9-]{0,63}$')
                )
        $ddl$;
    END IF;
END $$;
