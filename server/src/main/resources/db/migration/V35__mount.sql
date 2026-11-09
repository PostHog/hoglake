-- MOUNTS: one row per mount name (`<orgID>-events`), the values it admits.
--
-- Written only through PUT /v1/mounts/{name}, by the one operator token
-- HOGLAKE_MOUNT_WRITER_SUBJECT names (hogtower's events mount loop), each
-- write CAS on the revision the writer read. A row is never deleted (a
-- mount is disabled and emptied instead), so a revision never resets.
-- Every write is an audit line (`mount_write`), not a row here.
--
-- A mount serves while `enabled`, its org is in HOGLAKE_MOUNT_ALLOWLIST,
-- and hog_mount_switch is on (V34).
SET LOCAL lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS hog_mount (
    name         text    PRIMARY KEY
        CHECK (name ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-events$'),
    revision     bigint  NOT NULL CHECK (revision > 0),
    enabled      boolean NOT NULL,
    -- Ascending and distinct, at most MountStore.MAX_VALUES.
    mount_values bigint[] NOT NULL
);
