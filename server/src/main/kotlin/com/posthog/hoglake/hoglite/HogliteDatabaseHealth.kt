package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.DatabaseActivity
import com.posthog.hoglake.model.DatabaseFinding
import com.posthog.hoglake.model.DatabaseHealth
import com.posthog.hoglake.model.DatabaseServer
import com.posthog.hoglake.model.FindingResolution
import com.posthog.hoglake.model.FindingSeverity
import com.posthog.hoglake.model.ResolutionKind
import com.posthog.hoglake.service.DatabaseHealthBackend
import java.time.Instant

/**
 * `GET /v1/database/health` on hoglite — the ONE genuinely
 * Postgres-shaped surface this backend answers, and it answers it
 * honestly rather than by faking pg_stat numbers.
 *
 * The Postgres page's whole thesis (DatabaseHealth's KDoc) is reading
 * the statistics views against THIS schema's write patterns: dead
 * tuples in the partial indexes the commit path scans, advisory-lock
 * convoys, the vacuum horizon, xid age. None of those concepts exist
 * on hoglite — a tenant's store is a SQLite file with one writer, no
 * vacuum, no wraparound, no WAL slots, no shared buffer cache — so
 * there is nothing to MISREAD, and the actionable page is small:
 *
 *  - the engine's reachability (this process's HogliteClient, with its
 *    retry budget and circuit state — the failure a request would see),
 *  - the fleet identity (which backend, which endpoint),
 *  - and the zeroed Postgres instruments, labelled in [DatabaseHealth.blindSpots]
 *    so a green page is never read as a claim about statistics that
 *    were never collected.
 *
 * The DTO stays [DatabaseHealth] — the wire contract is the console's,
 * and the console renders the same shape on both backends; what changes
 * is which fields can carry signal.
 */
internal class HogliteDatabaseHealth(
    private val client: HogliteClient,
) : DatabaseHealthBackend {
    override fun report(): DatabaseHealth {
        val reachable = client.reachable()
        val findings = mutableListOf<DatabaseFinding>()
        if (!reachable) {
            findings +=
                DatabaseFinding(
                    severity = FindingSeverity.CRITICAL,
                    code = "hoglite_unreachable",
                    title = "The hoglite endpoint is not answering",
                    detail =
                        "A reachability probe (the same call /healthz makes) failed. " +
                            "Every catalog operation on this deployment goes through the engine, " +
                            "so this is a full outage, not a degraded statistic.",
                    hoglakeImpact =
                        "All reads and writes fail with 503 backend_unavailable until the engine " +
                            "or the route to it recovers.",
                    resolution =
                        FindingResolution(
                            kind = ResolutionKind.OPERATOR,
                            text =
                                "Check the hoglited process and this pod's network path to it. " +
                                    "The owner's address is the configured endpoint; a 421-bearing " +
                                    "proxy redirects are followed by the client, so an unreachable " +
                                    "answer here means no replica answered at all.",
                        ),
                )
        } else {
            findings +=
                DatabaseFinding(
                    severity = FindingSeverity.INFO,
                    code = "backend_is_hoglite",
                    title = "This deployment's backing store is hoglite",
                    detail =
                        "The instruments this page fills on Postgres — dead tuples, xid age, " +
                            "replication slots, checkpoint mix, advisory-lock waits — measure a " +
                            "storage engine this deployment does not have. They are reported as " +
                            "zero because they are absent, not because they were measured.",
                    hoglakeImpact =
                        "None. Tenant-level state lives on the per-catalog maintenance pages; " +
                            "the engine's own health is the reachability finding above.",
                    resolution =
                        FindingResolution(
                            kind = ResolutionKind.WATCH,
                            text =
                                "Operator telemetry for hoglite comes from the engine's own " +
                                    "metrics and the maintenance run ledger, not from this page.",
                        ),
                )
        }
        return DatabaseHealth(
            server =
                DatabaseServer(
                    version = "hoglite",
                    database = "hoglite",
                    // No server-side size answer exists on the engine's
                    // API; the S3 manifest is the durable state and the
                    // local tenant files are cache. 0, and blindSpots says so.
                    sizeBytes = 0,
                    startedAt = null,
                    connectionsUsed = 0,
                    connectionsMax = 0,
                    cacheHitRatio = null,
                    deadlocks = 0,
                    rolledBack = 0,
                    committed = 0,
                    xidAge = 0,
                    xidFreezeMaxAge = 0,
                    autovacuumEnabled = false,
                    tempFiles = 0,
                    tempBytes = 0,
                    checkpointsTimed = null,
                    checkpointsRequested = null,
                ),
            activity =
                DatabaseActivity(
                    active = 0,
                    idle = 0,
                    idleInTransaction = 0,
                    waiting = 0,
                    longestTransactionSeconds = null,
                    longestIdleInTransactionSeconds = null,
                    longestWaitSeconds = null,
                ),
            commitLocks = emptyList(),
            replicationSlots = emptyList(),
            tables = emptyList(),
            indexes = emptyList(),
            findings = findings,
            blindSpots =
                listOf(
                    "Postgres statistics (vacuum, xid, buffer cache, checkpoints, " +
                        "replication slots, lock waits) do not exist on hoglite; the zeroed " +
                        "counters above are ABSENT instruments, not measurements.",
                    "Tenant storage size is not reported: the durable bytes are the S3 " +
                        "manifest, and the engine's API exposes no per-tenant size.",
                    "The engine's failure modes (WAL shipping lag, replica ownership moves) " +
                        "surface as request latency and the circuit breaker's state, which " +
                        "this page does not read.",
                ),
        )
    }
}
