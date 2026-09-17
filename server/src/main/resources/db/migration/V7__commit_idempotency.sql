-- Publication receipts outlive snapshot expiry: retrying an old request must
-- never publish it again. Keys are scoped to a catalog and immutable payload.
CREATE TABLE hog_commit_receipt (
    catalog_id BIGINT NOT NULL REFERENCES hog_catalog(catalog_id) ON DELETE CASCADE,
    idempotency_key UUID NOT NULL,
    request JSONB NOT NULL,
    snapshot_id BIGINT NOT NULL,
    schema_version BIGINT NOT NULL,
    PRIMARY KEY (catalog_id, idempotency_key)
);
