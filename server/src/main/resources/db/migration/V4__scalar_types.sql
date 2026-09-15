-- DuckLake scalar-type parity, phase 1: ten new flat column types join
-- the closed vocabulary on hog_column.col_type — int8, int16, uint8,
-- uint16, uint32, uint64, timestamp_s, timestamp_ms, timestamp_ns and
-- json. Each one has a defined Iceberg facade mapping recorded in
-- iceberg-federation.md §2, which is the membership rule for this set.
--
-- The vocabulary is a CHECK constraint, so extending it is a drop and a
-- recreate under the SAME auto-generated name the inline CHECK in
-- V1__init.sql produced (hog_column_col_type_check) — schema.sql keeps
-- the constraint inline, and the schema-equivalence gate compares
-- pg_get_constraintdef output, so the member ORDER below must match
-- schema.sql's list exactly.
--
-- Cost: ADD CONSTRAINT validates with a full scan under ACCESS
-- EXCLUSIVE. hog_column holds one row per column version, not per data
-- row, so this is DDL-sized work even on the largest catalogs.
--
-- Names DuckLake has that hoglake refuses permanently (int128, uint128,
-- timetz, interval, the geometry family) are deliberately absent: they
-- have no Iceberg mapping, so they are rejected at the API with a named
-- 422 (ColType.REFUSALS) and can never reach this constraint.
ALTER TABLE hog_column DROP CONSTRAINT hog_column_col_type_check;
ALTER TABLE hog_column ADD CONSTRAINT hog_column_col_type_check CHECK (col_type IN (
    'boolean', 'int8', 'int16', 'int', 'long', 'uint8', 'uint16',
    'uint32', 'uint64', 'float', 'double', 'decimal', 'date', 'time',
    'timestamp_s', 'timestamp_ms', 'timestamp', 'timestamp_ns',
    'timestamptz', 'string', 'json', 'uuid', 'binary'));
