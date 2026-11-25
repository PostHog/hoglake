---
hoglake: patch (Server)
---

Add three local-only GET routes for dedicated read sessions under `/v1/local-only/catalogs/...`: the namespace, the table and the pinned scan. They read only the bound namespace, table or pinned scan from the local Hoglite owner. They never reach a remote schema relay. A name outside the binding returns 404 without a backend call. Each answer except 405, 422 and 501 carries `X-Hoglake-Local-Only: true`. A `read_session` binding can now name a namespace that a remote schema relays by that exact name. Only the local-only routes select that binding. Every ordinary request keeps the relay.
