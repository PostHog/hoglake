-- Admit the restricted single-object ClickHouse packed-part format.
-- The immutable table row carries the format so a database foreign key,
-- not only the current server binary, prevents mixed-format publication.
SET LOCAL lock_timeout = '5s';

ALTER TABLE hog_table
    ADD COLUMN IF NOT EXISTS file_format text NOT NULL DEFAULT 'parquet';

ALTER TABLE hog_table DROP CONSTRAINT IF EXISTS hog_table_file_format_check;
ALTER TABLE hog_table
    ADD CONSTRAINT hog_table_file_format_check
    CHECK (file_format IN ('parquet', 'clickhouse-mergetree-packed'))
    NOT VALID;
ALTER TABLE hog_table VALIDATE CONSTRAINT hog_table_file_format_check;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'hog_table_format_identity'
          AND conrelid = 'hog_table'::regclass
    ) THEN
        ALTER TABLE hog_table
            ADD CONSTRAINT hog_table_format_identity
            UNIQUE (catalog_id, table_id, file_format);
    END IF;
END $$;

ALTER TABLE hog_data_file DROP CONSTRAINT IF EXISTS hog_data_file_file_format_check;
ALTER TABLE hog_data_file
    ADD CONSTRAINT hog_data_file_file_format_check
    CHECK (file_format IN ('parquet', 'clickhouse-mergetree-packed'))
    NOT VALID;
ALTER TABLE hog_data_file VALIDATE CONSTRAINT hog_data_file_file_format_check;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'hog_data_file_table_format_fk'
          AND conrelid = 'hog_data_file'::regclass
    ) THEN
        -- Existing rows can stay unscanned: before this migration the data-file
        -- CHECK admitted only Parquet, and every existing table receives the
        -- Parquet default above. PostgreSQL still enforces a NOT VALID foreign
        -- key for every row inserted after the constraint is installed.
        ALTER TABLE hog_data_file
            ADD CONSTRAINT hog_data_file_table_format_fk
            FOREIGN KEY (catalog_id, table_id, file_format)
            REFERENCES hog_table (catalog_id, table_id, file_format)
            NOT VALID;
    END IF;
END $$;

-- Format-aware claims are nullable for rolling deploys. A claim minted by an
-- older replica carries NULL and may settle a Parquet data registration only.
ALTER TABLE hog_upload ADD COLUMN IF NOT EXISTS file_format text;

-- Reserve the table property safely. Earlier binaries accepted arbitrary
-- properties, so stop the migration rather than reinterpret retained metadata
-- that already used this key for another purpose.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM hog_table_version
         WHERE properties ? 'write.format.default'
           AND properties ->> 'write.format.default' <> 'parquet'
    ) THEN
        RAISE EXCEPTION
            'retained table metadata already uses reserved property write.format.default; remove it before V25';
    END IF;
END $$;

-- A version row must repeat the immutable identity-row format. This trigger
-- also fences an older replica that tries to create a packed property while
-- its old INSERT still lets hog_table default to Parquet.
CREATE OR REPLACE FUNCTION hog_enforce_table_version_format()
RETURNS trigger LANGUAGE plpgsql AS '
DECLARE
    expected_format text;
    actual_format text;
BEGIN
    SELECT file_format INTO expected_format
      FROM hog_table
     WHERE catalog_id = NEW.catalog_id AND table_id = NEW.table_id;
    actual_format := COALESCE(NEW.properties ->> ''write.format.default'', ''parquet'');
    IF expected_format IS NOT NULL AND actual_format <> expected_format THEN
        RAISE EXCEPTION ''table property format % does not match immutable table format %'',
            actual_format, expected_format
            USING ERRCODE = ''check_violation'';
    END IF;
    RETURN NEW;
END';

DROP TRIGGER IF EXISTS hog_table_version_format_guard ON hog_table_version;
CREATE TRIGGER hog_table_version_format_guard
BEFORE INSERT OR UPDATE OF properties ON hog_table_version
FOR EACH ROW EXECUTE FUNCTION hog_enforce_table_version_format();

-- Old binaries do not know the fixed-layout restrictions. Fence every
-- unsupported catalog mutation at the database boundary during a rolling
-- deploy. Initial column inserts at the table's creation snapshot and all
-- cleanup after a table is marked dropped remain legal.
CREATE OR REPLACE FUNCTION hog_reject_packed_table_mutation()
RETURNS trigger LANGUAGE plpgsql AS '
DECLARE
    row_catalog_id bigint;
    row_table_id bigint;
    table_format text;
    table_created_snapshot bigint;
    table_dropped_snapshot bigint;
BEGIN
    IF TG_OP = ''DELETE'' THEN
        row_catalog_id := OLD.catalog_id;
        row_table_id := OLD.table_id;
    ELSE
        row_catalog_id := NEW.catalog_id;
        row_table_id := NEW.table_id;
    END IF;

    SELECT file_format, created_snapshot, hog_table.dropped_snapshot
      INTO table_format, table_created_snapshot, table_dropped_snapshot
      FROM hog_table
     WHERE catalog_id = row_catalog_id AND table_id = row_table_id;

    IF table_format IS DISTINCT FROM ''clickhouse-mergetree-packed''
       OR table_dropped_snapshot IS NOT NULL THEN
        IF TG_OP = ''DELETE'' THEN
            RETURN OLD;
        END IF;
        RETURN NEW;
    END IF;

    IF TG_TABLE_NAME = ''hog_column'' AND TG_OP = ''INSERT''
       AND NEW.begin_snapshot = table_created_snapshot THEN
        RETURN NEW;
    END IF;

    RAISE EXCEPTION ''packed MergeTree table does not support mutation through %'', TG_TABLE_NAME
        USING ERRCODE = ''check_violation'';
END';

DROP TRIGGER IF EXISTS hog_packed_column_guard ON hog_column;
CREATE TRIGGER hog_packed_column_guard
BEFORE INSERT OR UPDATE OR DELETE ON hog_column
FOR EACH ROW EXECUTE FUNCTION hog_reject_packed_table_mutation();

DROP TRIGGER IF EXISTS hog_packed_partition_guard ON hog_partition_spec;
CREATE TRIGGER hog_packed_partition_guard
BEFORE INSERT OR UPDATE OR DELETE ON hog_partition_spec
FOR EACH ROW EXECUTE FUNCTION hog_reject_packed_table_mutation();

DROP TRIGGER IF EXISTS hog_packed_sort_guard ON hog_sort_spec;
CREATE TRIGGER hog_packed_sort_guard
BEFORE INSERT OR UPDATE OR DELETE ON hog_sort_spec
FOR EACH ROW EXECUTE FUNCTION hog_reject_packed_table_mutation();

DROP TRIGGER IF EXISTS hog_packed_delete_file_guard ON hog_delete_file;
CREATE TRIGGER hog_packed_delete_file_guard
BEFORE INSERT OR UPDATE ON hog_delete_file
FOR EACH ROW EXECUTE FUNCTION hog_reject_packed_table_mutation();

DROP TRIGGER IF EXISTS hog_packed_data_file_end_guard ON hog_data_file;
CREATE TRIGGER hog_packed_data_file_end_guard
BEFORE UPDATE OF end_snapshot ON hog_data_file
FOR EACH ROW EXECUTE FUNCTION hog_reject_packed_table_mutation();

-- Old replicas do not read hog_upload.file_format. Enforce the claim format
-- on the data-row INSERT itself so they cannot settle a packed claim with a
-- legacy registration that defaults to Parquet. Unclaimed writer paths skip
-- the indexed probe entirely.
CREATE OR REPLACE FUNCTION hog_enforce_claimed_data_file_format()
RETURNS trigger LANGUAGE plpgsql AS '
DECLARE
    claimed_format text;
BEGIN
    IF strpos(NEW.path, ''/trino-upload/'') = 0 THEN
        RETURN NEW;
    END IF;

    SELECT COALESCE(file_format, ''parquet'') INTO claimed_format
      FROM hog_upload
     WHERE catalog_id = NEW.catalog_id AND path = NEW.path AND file_kind = ''data'';
    IF claimed_format IS NOT NULL AND claimed_format <> NEW.file_format THEN
        RAISE EXCEPTION ''data file format % does not match upload claim format %'',
            NEW.file_format, claimed_format
            USING ERRCODE = ''check_violation'';
    END IF;
    RETURN NEW;
END';

DROP TRIGGER IF EXISTS hog_claimed_data_file_format_guard ON hog_data_file;
CREATE TRIGGER hog_claimed_data_file_format_guard
BEFORE INSERT ON hog_data_file
FOR EACH ROW EXECUTE FUNCTION hog_enforce_claimed_data_file_format();
