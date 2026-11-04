# Catalog backend selection

One deployment can serve catalogs on PostgreSQL or hoglite.
Mixed routing requires `HOGLAKE_DATABASE=mixed`.
Existing clients keep the same catalog names, URLs, authentication rules, and response shapes.
Only catalog creation accepts new optional fields.

## Deployment modes

| `HOGLAKE_DATABASE` | New catalog default | Directory | PostgreSQL initialization |
| --- | --- | --- | --- |
| `postgres` or unset | PostgreSQL | Existing PostgreSQL metadata | Existing startup migration |
| `hoglite` | hoglite | Existing hoglite registry | None |
| `mixed` | hoglite | Extended hoglite registry | Lazy initialization before PostgreSQL work |

Both legacy modes keep their existing defaults.
In configured PostgreSQL mode, the presence of `backend` or `connection_ref` on
create is an unknown-field `400 bad_request`, including an explicit JSON `null`.
Legacy hoglite keeps its existing behavior: null selection values act as absent;
any non-null explicit selection is refused with `422 validation`.
Mixed mode requires the existing PostgreSQL configuration.
It also requires the existing `HOGLAKE_HOGLITE_*` configuration.
Boot validates both service configurations without requiring PostgreSQL availability.
PostgreSQL schema initialization runs before the first PostgreSQL operation.

Mixed mode does not discover or adopt existing PostgreSQL catalogs.
An unregistered PostgreSQL name blocks creation with that name.
An overlapping PostgreSQL data path also blocks creation.
A missing directory entry never authorizes catalog deletion.
Do not run legacy writers against the same metadata stores as a mixed deployment.
Changing an existing deployment's mode does not migrate its catalogs.

## Creation and connection references

`POST /v1/catalogs` keeps its existing operator authorization.
Omitting both optional fields selects hoglite in mixed mode.

```json
{
  "name": "warehouse",
  "data_path": "s3://example-bucket/warehouse",
  "backend": "postgres",
  "connection_ref": "postgres"
}
```

The allowlist contains exactly two connection references:

| Reference | Backend | Configuration target |
| --- | --- | --- |
| `postgres` | PostgreSQL | The single pool configured by `HOGLAKE_JDBC_URL`, `HOGLAKE_DB_USER`, and `HOGLAKE_DB_PASSWORD` |
| `hoglite` | hoglite | The cluster configured by `HOGLAKE_HOGLITE_URL` and its existing routing options |

The connection reference can select its backend when `backend` is absent.
In mixed mode, null selection values act as absent; supplying both non-null
fields requires a matching pair.
Unknown references fail before a name reservation.
References never contain client-provided URLs or credentials.
The directory stores the selected reference.
The backend remains immutable for that catalog incarnation.

The PostgreSQL target can be the configured shared database or a configured dedicated database.
This implementation supports only one PostgreSQL pool per deployment.
Additional RDS instances require future configuration for additional pools.
Adding an alias does not create a pool.
Arbitrary endpoints do not work dynamically.
Changing a reference's configuration target does not move a catalog.

## Identity and lifecycle

The existing hoglite registry owns catalog names across both backends.
Each operation reads the directory once.
Its synchronous scope carries the selected backend and durable identity.
Hoglite uses the existing tenant incarnation.
PostgreSQL uses its durable `catalog_id`.
Nested PostgreSQL lookups reject a replacement identity before mutation.

Hoglite creation keeps its existing tenant-first publication.
The final registry transaction publishes the name.
An interrupted unpublished creation can leave an orphan tenant.
No background task deletes orphan tenants.

PostgreSQL creation uses these transitions:

1. Allocate a PostgreSQL identity.
2. Reserve the name as `creating` with a random operation identity.
3. Insert the catalog with that PostgreSQL identity.
4. Publish the same reservation as `ready`.

The insertion and publication use separate transactions.
A publication failure can leave an inserted catalog behind a `creating` reservation.
The request does not report success in that case.
Ordinary operations return `503 backend_unavailable` for that reservation.
Another create returns `409 already_exists`.
`DELETE /v1/catalogs/{catalog}` cancels the pinned reservation through the existing emptiness checks.
Cancellation requires PostgreSQL availability, even when insertion never succeeded.
A PostgreSQL outage leaves the reservation unchanged and cancellation returns 503.
There is no offline cancellation or automatic timeout.

Ordinary catalog lists, instance status pages, and instance run feeds include only ready entries.
A retained reservation does not block unrelated ready catalogs.
Direct operations on its known name still return 503 with its state and cancellation guidance.
Operators retain that name from the failed create request or its logs.
No additional reservation-listing API is introduced.

PostgreSQL create and drop use a per-name session advisory lock.
Acquisition makes one nonblocking attempt.
Contention returns `503 backend_unavailable`.
The same Jdbi handle owns the lock and performs PostgreSQL mutations.
Registry HTTP calls retain the configured hoglite deadlines.
No PostgreSQL transaction spans a registry HTTP call.

Drop rereads the directory under that lock.
It refuses a replacement reservation.
It deletes only the bound PostgreSQL identity.
It removes the matching directory entry after backend deletion.
A failure between those steps leaves an old directory entry.
A repeated DELETE removes that entry after confirming that its bound PostgreSQL identity is absent.

Ordinary reads can return 404 during an interrupted drop.
A repeated DELETE completes the drop without adopting another catalog.
No reservation expires automatically.
No timer steals a reservation.
No background task performs destructive recovery.
Dropping a catalog permits a later creation with a different backend.

## Availability and maintenance

The hoglite registry is an explicit shared dependency in mixed mode.
A directory failure fails catalog operations.
No lookup error selects another backend.
A missing tenant never redirects an operation to PostgreSQL.
A PostgreSQL failure never redirects an operation to hoglite.

Existing hoglite catalogs remain available during a PostgreSQL outage.
New creates require PostgreSQL for collision checks, including new hoglite catalogs.
Those creates fail during a PostgreSQL outage.
Instance lists fail if a selected backend cannot answer.
Backend outages do not become successful partial lists.
Mixed readiness checks the directory, not PostgreSQL.

Directory reads after initialization fail closed when the registry is missing.
After restart, a missing registry is indistinguishable from a fresh deployment.
Startup can therefore bootstrap an empty registry and return an empty roster.
The registry's durability is the directory's durability.
Lost directory contents are not reconstructed from PostgreSQL or orphan hoglite tenants.
No automatic adoption or recovery is provided.

Catalog lists read the directory once in bounded pages.
They group ready entries by backend without resolving their names again.
Hoglite metadata uses the existing 250-item batch mechanism.
PostgreSQL metadata uses 250-ID queries.
A concurrent drop can omit the old identity; the list never substitutes its replacement.
Older engines without query-batch still require one metadata query per selected hoglite tenant.
That fallback does not add per-catalog registry or migration queries.
The live count fixture uses 260 catalogs on each backend.
A cold process reads the existing registry schema once and probes four directory columns.
Its list then uses one directory page, two hoglite batches, and two PostgreSQL ID queries.
A warm list needs only that page, those two batches, and those two ID queries.
Neither list issues individual tenant metadata queries or migration probes.

Catalog maintenance uses the same directory entries and identity scopes as requests.
The existing lease mechanism elects one process per loop.
Each catalog retains its existing maintenance limits.
Hydration shares its existing file budget across the selected catalogs.
The PostgreSQL summary sampler receives only registered, ready PostgreSQL identities.
Reindex remains a PostgreSQL database task.
Mixed compaction retains the existing seeded jitter, including when leases are disabled.

All modes collect catalog metrics through the configured metrics loop.
Mixed mode samples only registered, ready directory entries.
The sampler pins each entry's backend and immutable identity for that sample.
The `/v1/info` totals use successful sampler observations.
The database health endpoint reports PostgreSQL statistics plus hoglite findings.
That endpoint fails if PostgreSQL cannot answer.
Per-catalog maintenance endpoints remain the catalog-specific diagnostic surface.
The instance maintenance run feed retains hoglite's newest-page behavior.
Its `before` parameter is advisory because ledger IDs are local to their catalog.
The page uses descending start time and run ID, then ascending catalog name.
Per-catalog run pagination retains its existing cursor behavior.

### Metrics scope and failure behavior

All 36 backend-neutral sampler series work in PostgreSQL-only, hoglite-only, and mixed modes.
Existing catalog, namespace, table, consumer, task, status, and group labels retain their meanings.
The PostgreSQL-only physical series are:

- `hoglake_relation_bytes{relation,kind}`
- `hoglake_relation_live_tuples{relation}`
- `hoglake_relation_dead_tuples{relation}`

These series describe only the configured PostgreSQL relations.
They never describe hoglite storage or total fleet storage.
Hoglite-only mode publishes none of these relation series.
Mixed mode retains the PostgreSQL reindex gauge refresh.

PostgreSQL totals retain their maintained-summary semantics.
A missing summary produces `live_totals_sampled=0` with absent totals and age.
Hoglite uses exact durable SQL aggregates over live tables and files.
For hoglite, `live_totals_sampled=1` means an exact aggregate exists for that catalog incarnation.
Its age measures time since that successful aggregate read begins.
The age continues to increase after failed refreshes.
A scrape never resets that age.
Before the first successful read, hoglite totals and age remain absent.
Every numeric zero represents an observed empty count.
Hoglite table metrics use exact aggregates rather than a persisted summary generation.

Core reads share one durable snapshot within each catalog.
There is no atomic snapshot across catalogs or backends.
Each catalog/group keeps its last successful observation after a read failure.
Healthy catalogs continue to refresh.
The sampler increments the error counter once per failed sample.
It increments each affected group counter once per sample.
A failed directory read preserves the previous observations without selecting another backend.
An authoritative roster removes observations for dropped or replaced identities.
A replacement never inherits another incarnation's maintenance epochs.
A drop after roster selection can leave the selected incarnation's stale observation until the next roster.
Incomplete reservations never enter the sample.

`hoglake_metrics_last_sample_epoch` advances only when every selected core read succeeds.
Extended-group failures do not prevent that core epoch from advancing.
A core failure preserves the previous complete `/v1/info` totals for an unchanged roster.
A roster change invalidates those fleet totals until the next complete core success.
Per-catalog values can remain stale when their own read fails.
The group counters expose that condition.
No partial successful fleet sum replaces a complete fleet sum.

Maintenance epochs come from persisted ledger rows across replicas.
Hoglite and mixed PostgreSQL use bounded indexed probes for each task and status.
They recover older successful runs after a sampler restart, including after long failure streaks.
Ledger retention still limits recoverable history.
Legacy PostgreSQL-only mode retains its existing window-plus-memory behavior.

Collection walks the roster serially in bounded pages.
It retains the existing 100-consumer cap and 500-table cap per catalog.
The maximum consumer lag remains available above the consumer cap.
The retirement gauge probes only the newest 10,000 dropped tables per catalog.
That retirement value remains a lower bound.
Queries retain the configured hoglite deadlines and the PostgreSQL statement timeout.
Returned aggregate rows remain bounded, but exact hoglite queries can scan a tenant's live manifest.
Collection time and retained observations grow with catalog count.
This implementation does not optimize deployments with very many tenants.
The extended groups retain the existing maintenance-summary interval gate.
Sampling failures never become request failures.

The live contract tests use the workflow-pinned hoglited executable plus PostgreSQL.
They cover exported values, persisted history, missing summaries, partial failures, and incarnation changes.
The schema tests compare historical tenant upgrades with new tenant creation.

## Scope

This feature does not migrate metadata or data.
It does not switch a live catalog's backend.
It does not provision RDS instances.
It does not add export, import, or automatic promotion.
The directory remains part of hoglake's existing registry infrastructure.
