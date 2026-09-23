-- Atomic table replacement leaves a consumer's offset on the RETIRED
-- incarnation: the consumer reconciles onto the new table_uuid and never
-- looks at the old one again, so on a consumer_floor catalog that row
-- pins expiry at a pre-replacement snapshot forever. Releasing it needs
-- the replacement LINEAGE, and hoglake stored none.
--
-- RECORD it, do not derive it. The derivation
-- `old.dropped_snapshot = new.created_snapshot` is sound (see the
-- backfill below for why), but nothing ENFORCES it, it is stated as an
-- invariant nowhere, and the failure mode if a future change breaks it
-- is DELETING a consumer's position on a table it is still draining. A
-- recorded edge cannot be wrong about what replaced what. It is also
-- unindexable in the direction the walk needs: the hop seeks the
-- SUCCESSOR, whose own dropped_snapshot is NULL at the end of every
-- chain, so a partial index on dropped_snapshot cannot serve it and the
-- walk degrades to a sequential scan of hog_table per hop.
--
-- LOCK BOUND FIRST, for every statement that takes a heavy lock: the
-- ADD COLUMN and ADD CONSTRAINT below take ACCESS EXCLUSIVE on
-- hog_table, and Database.migrate holds a pg_advisory_lock across the
-- whole Flyway run, so an unbounded wait here queues every other booting
-- pod behind this one. Database.kt sets only statement_timeout at
-- connect, so without this the wait is the server default (unbounded).
-- The save-and-restore is V9's and V10's, and the 5s value is theirs;
-- what is NOT theirs is the placement. V10 put its own ADD COLUMN AFTER
-- the restore, which is the same defect this file previously had —
-- executeInTransaction=false means the setting is per-session, so where
-- it is set relative to the DDL is the whole of its effect.
SELECT set_config('hoglake.migration_lock_timeout', current_setting('lock_timeout'), false);
SET lock_timeout = '5s';

ALTER TABLE hog_table ADD COLUMN IF NOT EXISTS replaced_table_id bigint;

-- No FK (the referenced row is in this same table and is retired, never
-- deleted; a cascade is exactly the behaviour we do not want), so this
-- CHECK is the only structural defence the edge has. A self-edge would
-- make the UNION ALL in OffsetRepo.releaseSupersededOffsets a 2-cycle if
-- its `created_snapshot >` termination guard were ever loosened. Named
-- explicitly on both sides (here and in schema.sql) so the equivalence
-- gate compares equal; drop-then-add for idempotency, since Postgres has
-- no ADD CONSTRAINT IF NOT EXISTS.
ALTER TABLE hog_table DROP CONSTRAINT IF EXISTS hog_table_no_self_replacement;
ALTER TABLE hog_table ADD CONSTRAINT hog_table_no_self_replacement
    CHECK (replaced_table_id <> table_id);

-- One-time backfill. It uses the snapshot derivation EXACTLY ONCE, here,
-- and it is exact rather than probable:
--
--   * hog_table.dropped_snapshot has exactly TWO writers —
--     CatalogService.createTable's replacement branch and dropTable —
--     both through TableRepo.markDropped;
--   * hog_table rows are INSERTed from exactly one place,
--     TableRepo.insertTable, called only by createTable;
--   * every DDL transaction mints its own snapshot under the per-catalog
--     commit lock, so a snapshot that both retires a table and creates
--     one is a replacement and can be nothing else (dropTable's snapshot
--     creates nothing; a plain createTable's retires nothing);
--   * views, compaction and data commits never write hog_table at all.
--
-- So `o.dropped_snapshot = n.created_snapshot AND o.table_id <> n.table_id`
-- matches replacement pairs and nothing else, in ANY environment, past
-- or present. The `<>` is not decorative: without it a row would match
-- itself whenever created_snapshot = dropped_snapshot.
--
-- What this repairs is the rolling-deploy window: a 1.2.0 replica still
-- publishes replacements without writing replaced_table_id, and each one
-- leaves a retired incarnation whose consumer offset pins expiry until
-- the edge exists. Re-running this UPDATE is the repair. It is idempotent
-- (`IS NULL` guard), so a Flyway retry cannot overwrite an edge
-- createTable has since recorded properly. No automated repair job is
-- wired up, deliberately: production carries no replacements yet, so the
-- window is empty in practice and an operator can re-run this statement
-- by hand if that ever stops being true.
UPDATE hog_table n
   SET replaced_table_id = o.table_id
  FROM hog_table o
 WHERE o.catalog_id = n.catalog_id
   AND o.dropped_snapshot = n.created_snapshot
   AND o.table_id <> n.table_id
   AND n.replaced_table_id IS NULL;

-- An INVALID remnant of an interrupted concurrent build is cleared
-- before the build below: CREATE INDEX CONCURRENTLY IF NOT EXISTS would
-- otherwise see it, skip, and leave it invalid forever. A plain DROP
-- INDEX takes ACCESS EXCLUSIVE on the TABLE and cannot be CONCURRENTLY
-- inside a DO block, so it runs under the same 5s bound as the DDL above
-- — fail fast and retryably rather than convoy the catalog.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_index i ON i.indexrelid = c.oid
        WHERE c.relname = 'hog_table_replacement_lineage' AND NOT i.indisvalid
    ) THEN
        EXECUTE 'DROP INDEX hog_table_replacement_lineage';
    END IF;
END $$;

-- Restore BEFORE the concurrent build: CREATE INDEX CONCURRENTLY waits
-- out every transaction older than itself, which is exactly the wait a
-- 5s lock_timeout would abort.
SELECT set_config('lock_timeout', current_setting('hoglake.migration_lock_timeout'), false);

-- The walk's access path: the expiry sweep's all-consumer pass in
-- OffsetRepo.releaseSupersededOffsets follows
-- `n.replaced_table_id = l.table_id` forward, under the catalog commit
-- lock, and a catalog can hold tens of thousands of dropped tables.
-- Partial, because only a replacement row carries the edge and those are
-- the minority. (The per-commit BACKWARD walk needs no index: it follows
-- replaced_table_id from a known row, which is a primary-key lookup per
-- hop.) The index can also be pre-built out of band --
--
--   CREATE INDEX CONCURRENTLY hog_table_replacement_lineage
--       ON hog_table (catalog_id, replaced_table_id) WHERE replaced_table_id IS NOT NULL;
--
-- after which this statement finds it and is a no-op.
SELECT set_config('hoglake.migration_statement_timeout', current_setting('statement_timeout'), false);
SET statement_timeout = 0;
CREATE INDEX CONCURRENTLY IF NOT EXISTS hog_table_replacement_lineage
    ON hog_table (catalog_id, replaced_table_id) WHERE replaced_table_id IS NOT NULL;
SELECT set_config('statement_timeout', current_setting('hoglake.migration_statement_timeout'), false);
