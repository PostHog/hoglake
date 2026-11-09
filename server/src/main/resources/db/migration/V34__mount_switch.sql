-- MOUNT SWITCH: the instance-wide switch for mount dispatch.
--
-- A mount is a read-only catalog name (`<orgID>-events`) over a table in
-- this instance's PostgreSQL catalogs. While `enabled` is false, no name
-- resolves to a mount, and every request but a create of a reserved name
-- (refused whatever this says) is served as before.
-- Off by default. One row; an operator turns it on (or off, with false) with
--   UPDATE hog_mount_switch SET enabled = true;
-- and it applies from the next request, on every replica. It is read only
-- for a name the mount registry holds. A missing row reads as off.
SET LOCAL lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS hog_mount_switch (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    enabled   boolean NOT NULL DEFAULT false
);

INSERT INTO hog_mount_switch DEFAULT VALUES ON CONFLICT DO NOTHING;
