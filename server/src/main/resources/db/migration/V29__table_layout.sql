-- PER-TABLE OBJECT LAYOUT: the object-prefix option (`set_object_prefix_field`).
--
-- A table with the option on keeps every data file and deletion vector
-- under `<data_path>/data/<ns>/<table>/<field>=<value>/`, where value is
-- the file's identity partition value for the prefix field, so one IAM
-- statement on that prefix grants exactly one value's objects
-- (model/ObjectPrefix.kt). The commit path enforces the layout; this
-- table records which tables have it, and on which field.
--
-- VERSIONED, in the hog_sort_spec shape: a row is visible at S iff
-- begin_snapshot <= S AND (end_snapshot IS NULL OR S < end_snapshot), so
-- a time-travel read of the table reports the option as it was then,
-- and expiry's versioned-row step clears an ended row once no retained
-- snapshot can see it (ExpiryService.VERSIONED_RETENTION_TABLES). Not
-- custom properties (inert by contract, and `set_properties` replaces
-- them wholesale), and not a column on hog_table_version (every DDL
-- re-inserts that row, and an older binary would drop the field).
--
-- object_prefix_field_id has NO foreign key: hog_column's key includes
-- begin_snapshot, so a field id alone references nothing. The DDL refuses
-- dropping or renaming the field while the option is on, and the commit
-- path reads the column with a LEFT JOIN so a missing one is a 500, never
-- a silent "off".
--
-- hog_table_layout_live is UNIQUE: at most one live layout per table.
-- The commit read and LayoutRepo both assume it, and a second live row
-- would otherwise be a layout chosen by whichever row came first.
--
-- TRANSACTIONAL (no `.conf`): the foreign key takes SHARE ROW EXCLUSIVE
-- on hog_table, which waits for and then blocks every write to it (table
-- creation, drops, field-id allocation by add_column), so everything sits
-- inside the lock_timeout window (MigrationLockWindowTest); a 5 s timeout
-- rolls the file back with no history row and the next pod retries. The
-- table is new and empty, so the index build is instantaneous.
SET LOCAL lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS hog_table_layout (
    catalog_id             bigint NOT NULL,
    table_id               bigint NOT NULL,
    begin_snapshot         bigint NOT NULL,
    end_snapshot           bigint,
    object_prefix_field_id bigint NOT NULL,
    PRIMARY KEY (catalog_id, table_id, begin_snapshot),
    FOREIGN KEY (catalog_id, table_id) REFERENCES hog_table ON DELETE CASCADE,
    CHECK (end_snapshot IS NULL OR end_snapshot > begin_snapshot)
);
CREATE UNIQUE INDEX IF NOT EXISTS hog_table_layout_live
    ON hog_table_layout (catalog_id, table_id) WHERE end_snapshot IS NULL;
