-- ATOMIC PARTITION REPLACEMENT: THE `table_replaced` CHANGE KIND AND THE
-- RECEIPT'S ENDED-FILE COUNT.
--
-- 1. `hog_snapshot_change.kind` gains 'table_replaced'. A replacement
--    (POST /commit/replacements/prepared) that ended at least one data
--    file of a table records it beside 'table_deleted_from'. The
--    changefeed raises its reconciliation barrier on THIS row rather than
--    on "a data file was ended at snapshot s": expiry purges the ended
--    file rows once the floor reaches s, so the inference lost the
--    barrier exactly at the retention floor, while a change row of the
--    snapshot at the floor is retained with it.
--
-- 2. `hog_commit_receipt.ended_files` stores the data files a replacement
--    ended, so a replay (and GET /commit/receipts/{operation}) answers
--    with the same count as the first response. NULL on every receipt of a
--    commit that is not a replacement, and on every receipt written
--    before this migration: the field is then omitted, as it was.
--
-- Transactional, in V20's shape: `SET LOCAL` expires with the transaction
-- and a failure rolls the whole file back. The CHECK swap is NOT VALID
-- then VALIDATE (V20's reason: the change table has a row per change of
-- every commit, so the ADD must be a catalog write and the VALIDATE must
-- not block writers). The DROP names the constraint Postgres generated
-- for V1's inline CHECK (`<table>_<column>_check`).
SET LOCAL lock_timeout = '5s';

ALTER TABLE hog_snapshot_change DROP CONSTRAINT IF EXISTS hog_snapshot_change_kind_check;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'hog_snapshot_change_kind_check'
          AND conrelid = 'hog_snapshot_change'::regclass
    ) THEN
        EXECUTE $ddl$
            ALTER TABLE hog_snapshot_change
                ADD CONSTRAINT hog_snapshot_change_kind_check
                CHECK (kind IN (
                    'namespace_created', 'namespace_dropped',
                    'table_created', 'table_dropped', 'table_altered',
                    'table_inserted_into', 'table_deleted_from',
                    'table_compacted', 'table_replaced',
                    'view_created', 'view_dropped'))
                NOT VALID
        $ddl$;
    END IF;
END $$;
ALTER TABLE hog_snapshot_change VALIDATE CONSTRAINT hog_snapshot_change_kind_check;

-- Metadata-only (NULL default, no rewrite) and still ACCESS EXCLUSIVE.
ALTER TABLE hog_commit_receipt ADD COLUMN IF NOT EXISTS ended_files bigint;
