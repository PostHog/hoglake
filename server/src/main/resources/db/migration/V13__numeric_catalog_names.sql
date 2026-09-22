-- Allow UUID catalog names without changing existing catalog identities.
SET LOCAL lock_timeout = '5s';
ALTER TABLE hog_catalog DROP CONSTRAINT hog_catalog_name_check;
ALTER TABLE hog_catalog ADD CONSTRAINT hog_catalog_name_check
    CHECK (name ~ '^[a-z0-9][a-z0-9_-]{0,62}$');
