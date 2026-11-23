---
hoglake: patch (Server)
---

Tests and docs for a Hoglite catalog drop that fails after its fence (#683). Retrying `DELETE /v1/catalogs/{catalog}` already completes such a drop (the fence transaction skips the `dropping` guard, so the retry re-proves the catalog empty, releases and deletes the tenant); a unit test and a live test now pin that. docs/auth.md, docs/auth-tenancy.md and the OpenAPI `dropCatalog` (which now lists the 403 and the Hoglite fenced state) state that dropping a Hoglite catalog needs a catalog-bound `operator` token, because hoglite deletes a tenant only for an operator; a `tenant` token with `admin` gets 403 before anything is fenced. Tenant-admin self-service drop stays PostgreSQL-only.
