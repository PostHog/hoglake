CREATE TABLE hog_upload (
    catalog_id bigint NOT NULL REFERENCES hog_catalog ON DELETE CASCADE,
    upload_id uuid NOT NULL,
    owner uuid NOT NULL,
    prefix text NOT NULL,
    path text NOT NULL,
    file_kind text NOT NULL CHECK (file_kind IN ('data', 'delete')),
    state text NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'registered', 'abandoned')),
    expires_at timestamptz NOT NULL DEFAULT now() + interval '24 hours',
    last_scheduled_at timestamptz,
    PRIMARY KEY (catalog_id, upload_id),
    UNIQUE (catalog_id, path)
);
CREATE INDEX hog_upload_owner ON hog_upload (catalog_id, owner) WHERE state = 'active';
CREATE INDEX hog_upload_cleanup ON hog_upload (catalog_id, last_scheduled_at, upload_id) WHERE state <> 'registered';
ALTER TABLE hog_file_removal DROP CONSTRAINT hog_file_removal_reason_check;
ALTER TABLE hog_file_removal ADD CONSTRAINT hog_file_removal_reason_check
    CHECK (reason IN ('snapshot_expiry', 'table_drop_gc', 'compaction_staging', 'trino_upload'));
