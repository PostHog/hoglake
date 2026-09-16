-- Durable receipts are deliberately retained; expiry only closes prepared operations.
CREATE TABLE hog_table_creation (
    catalog_id bigint NOT NULL REFERENCES hog_catalog ON DELETE CASCADE,
    operation_id uuid NOT NULL,
    namespace_id bigint NOT NULL,
    definition jsonb NOT NULL,
    table_uuid uuid NOT NULL,
    write_path text NOT NULL,
    state text NOT NULL DEFAULT 'prepared' CHECK (state IN ('prepared', 'committed', 'rejected', 'aborted')),
    expires_at timestamptz NOT NULL DEFAULT clock_timestamp() + interval '24 hours',
    files jsonb,
    snapshot_id bigint,
    schema_version bigint,
    reason text,
    PRIMARY KEY (catalog_id, operation_id),
    UNIQUE (catalog_id, table_uuid),
    CHECK ((state = 'committed') = (snapshot_id IS NOT NULL AND schema_version IS NOT NULL))
);
