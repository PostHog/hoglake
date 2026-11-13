-- SEVERAL CONTAINED NAMESPACES PER VALUE: V33 allowed one live namespace
-- per (catalog, field, value). A team that keeps its backfilled events in
-- `team_42` and its data imports in `imports_42` needs both contained for
-- `team_id = 42`, so the rule is relaxed: any number of live namespaces
-- may be contained for the same value.
--
-- That is safe because authorization only ever maps a namespace to its
-- containment (a scoped token is held to the namespaces whose containment
-- matches its predicate), never a value to "the" namespace, and each
-- namespace still owns its own prefix `<data_path>/data/<name>/` and its
-- own name (CommitService.checkContainment, V33's marking checks).
--
-- The index stays, now non-unique, for the lookups that read a catalog's
-- contained namespaces by (field, value). Same name, columns and
-- predicate. hog_namespace is one row per namespace ever created in a
-- catalog (thousands at most), so the drop and rebuild are a plain
-- statement over a small table: no CONCURRENTLY.
SET LOCAL lock_timeout = '5s';

DROP INDEX IF EXISTS hog_namespace_live_containment;

CREATE INDEX IF NOT EXISTS hog_namespace_live_containment
    ON hog_namespace (catalog_id, contained_field, contained_value)
    WHERE NOT dropped AND contained_field IS NOT NULL;
