-- Admit the restricted single-object ClickHouse packed-part format.
-- Format is stored on the immutable hog_table row as the single source of
-- truth. Enforcement is performed at the service layer on creation, alter,
-- and commit, gated by HOGLAKE_PACKED_MERGETREE_ENABLED.
SET LOCAL lock_timeout = '5s';

ALTER TABLE hog_table
    ADD COLUMN IF NOT EXISTS file_format text NOT NULL DEFAULT 'parquet';

ALTER TABLE hog_table DROP CONSTRAINT IF EXISTS hog_table_file_format_check;
ALTER TABLE hog_table
    ADD CONSTRAINT hog_table_file_format_check
    CHECK (file_format IN ('parquet', 'clickhouse-mergetree-packed'))
    NOT VALID;
ALTER TABLE hog_table VALIDATE CONSTRAINT hog_table_file_format_check;

-- Existing data-file rows remain valid without a full table scan under
-- ACCESS EXCLUSIVE lock: V1 only admitted Parquet, and new rows are validated
-- on insert. The constraint stays NOT VALID to avoid a multi-minute lock on
-- large production catalogs.
ALTER TABLE hog_data_file DROP CONSTRAINT IF EXISTS hog_data_file_file_format_check;
ALTER TABLE hog_data_file
    ADD CONSTRAINT hog_data_file_file_format_check
    CHECK (file_format IN ('parquet', 'clickhouse-mergetree-packed'))
    NOT VALID;

-- Format-aware claims are nullable for rolling deploys. A claim minted by an
-- older replica carries NULL and may settle a Parquet data registration only.
ALTER TABLE hog_upload ADD COLUMN IF NOT EXISTS file_format text;

-- Reserve the table property safely (case-insensitively). Earlier binaries
-- accepted arbitrary properties, so stop the migration rather than reinterpret
-- retained metadata that already used this key for another purpose.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM hog_table_version
         WHERE properties ? 'write.format.default'
           AND lower(properties ->> 'write.format.default') <> 'parquet'
    ) THEN
        RAISE EXCEPTION
            'retained table metadata already uses reserved property write.format.default; remove it before V25';
    END IF;
END $$;
