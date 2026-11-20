---
hoglake: minor (Operators)
---

Hoglite tenants have no migrations and `CREATE TABLE IF NOT EXISTS` leaves an existing table as it was, so a tenant bootstrapped before this release keeps its unconstrained schema: drop and recreate every development Hoglite tenant (catalog) after deploying. No customer tenant exists yet, so nothing else moves. A `hoglited` that does not run tenant connections with `PRAGMA foreign_keys = ON` (every released build does) now refuses catalog creation with 503 `backend_unavailable` naming the tenant.
