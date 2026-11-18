---
hoglake: minor (pyhoglake)
---

pyhoglake pins each remote table to the snapshot it was loaded at (#656). `Catalog.namespace` classes a namespace from the server's `remote: true` (listing entry, or get-namespace for a name the listing omits); `Namespace.remote` and `Table.remote` expose it. A remote table sends its pin on `info()`, `files()` and `scan_plan()`; `at_timestamp` is passed through; `snapshot=`, `list_tables(snapshot=)` on a remote namespace and `changes()` raise `ValueError`. Remote schemas are read-only on the client.
