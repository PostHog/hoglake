ALTER TABLE hog_table_version ADD COLUMN comment text;
ALTER TABLE hog_table_version ADD COLUMN properties jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE hog_column ADD COLUMN comment text;
