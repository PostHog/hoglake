-- Catalog metadata only; VARIANT uses the Iceberg v3 variant type.
SELECT set_config('hoglake.migration_lock_timeout', current_setting('lock_timeout'), false);
SET lock_timeout = '5s';
ALTER TABLE hog_column DROP CONSTRAINT hog_column_col_type_check;
ALTER TABLE hog_column ADD CONSTRAINT hog_column_col_type_check CHECK (col_type IN (
    'boolean', 'int8', 'int16', 'int', 'long', 'uint8', 'uint16',
    'uint32', 'uint64', 'float', 'double', 'decimal', 'date', 'time',
    'timestamp_s', 'timestamp_ms', 'timestamp', 'timestamp_ns',
    'timestamptz', 'string', 'json', 'uuid', 'binary', 'variant'));
SELECT set_config('lock_timeout', current_setting('hoglake.migration_lock_timeout'), false);
