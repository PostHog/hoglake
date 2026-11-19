---
hoglake: minor (Server)
---

The events mount path is removed: the `/v1/mounts` API, the `<orgID>-events` catalog relay and the `read_only` catalog field. Remote schemas replace it. A server that sets `HOGLAKE_MOUNT_ALLOWLIST`, `HOGLAKE_MOUNT_WRITER_SUBJECT`, `HOGLAKE_MOUNT_SOURCE_CATALOG`, `HOGLAKE_MOUNT_SOURCE_TABLE_UUID` or `HOGLAKE_MOUNT_SOURCE_FIELD_ID`, or whose `HOGLAKE_MOUNT_HOSTS_FILE` has an `events` or `catalog` entry, now refuses to start: remove them before deploying. The `hog_mount` tables stay, and nothing reads them; no migration runs.
