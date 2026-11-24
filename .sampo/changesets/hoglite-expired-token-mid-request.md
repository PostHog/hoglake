---
hoglake: patch (Server)
---

Hoglite: a token that lapses partway through a long operation no longer looks to clients like a request that never ran (fixes #680). hoglite verifies the forwarded token on every call, where PostgreSQL checks it once when the request starts, so a token that expired partway through a long compaction, cleanup or drop failed the request after earlier transactions had committed with 401 `invalid_token`, and a client that re-minted its token replayed the request as if nothing ran. Two changes:

- **Refused up front.** On a Hoglite catalog, the long-running routes (`POST .../maintenance/expire`, `cleanup`, `compact` and `rehydrate`, `DELETE /v1/catalogs/{catalog}`, `POST .../uploads/schedule-expired`) now answer 401 `token_expired`, before any backend call, for a token with less than `HOGLAKE_HOGLITE_MIN_TOKEN_LIFETIME_SECONDS` left at hoglite (`exp` plus hoglite's 60 s leeway). The default is 300; 0 disables the check. Nothing ran, so pyhoglake's `ClientCredentials` and the DuckDB extension, which re-mint on any 401, retry with a fresh token safely. An issuer whose tokens live less than the minimum minus 60 s makes these routes always refuse: lower the minimum or mint longer-lived tokens for them.
- **Failed midway anyway.** A backend 401 that arrives after one of the request's transactions applied (or may have) now answers 503 `commit_outcome_unknown`, naming that transaction, instead of 401 `invalid_token`. When that transaction is known to have applied, the message says the request partly applied and, without an idempotency key, to re-read the catalog's state before a retry. A 401 that arrives mid-way through a namespace containment scan keeps that answer rather than `containment_unverified`. A 403 stays 403: hoglite answers it only for authority that never covers the call, which a retry cannot change. A 401 before anything applied is unchanged.

PostgreSQL is unaffected by both.
