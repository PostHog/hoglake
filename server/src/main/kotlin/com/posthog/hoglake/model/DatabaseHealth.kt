package com.posthog.hoglake.model

import java.time.Instant

/**
 * Health of the Postgres instance the catalog lives in, read from the
 * statistics views.
 *
 * Deliberately NOT a general Postgres console. Every field here is one a
 * hoglake operator can act on, and the [findings] are the point: raw
 * `pg_stat_*` numbers need interpreting against how this schema is
 * written — the manifest tables take UPDATEs that leave dead tuples in
 * exactly the partial indexes the commit path scans, and the advisory
 * commit tail turns a stalled transaction into catalog-wide latency.
 *
 * No query text crosses this boundary. `pg_stat_activity.query` carries
 * literal values — object paths, table and column names, author strings
 * — and this endpoint serves an unauthenticated console today; durations
 * and counts say what an operator needs without shipping the payload.
 */
data class DatabaseHealth(
    val server: DatabaseServer,
    val activity: DatabaseActivity,
    val tables: List<DatabaseTable>,
    val indexes: List<DatabaseIndex>,
    val findings: List<DatabaseFinding>,
)

data class DatabaseServer(
    /** The server's version string, trimmed to the release (no build flags). */
    val version: String,
    val database: String,
    val sizeBytes: Long,
    val startedAt: Instant?,
    val connectionsUsed: Int,
    val connectionsMax: Int,
    /**
     * Fraction of block reads served from the buffer cache, for the
     * lifetime of the stats (since the last `pg_stat_reset`). The
     * manifest tables are the hot read set; when this drops the commit
     * tail's index probes are hitting disk.
     */
    val cacheHitRatio: Double?,
    val deadlocks: Long,
    val rolledBack: Long,
    val committed: Long,
    /**
     * Transaction-id age of the database against
     * `autovacuum_freeze_max_age`. A catalog that commits at hundreds of
     * transactions a second burns through the id space, and a wraparound
     * freeze is a whole-table read this schema cannot afford quietly.
     */
    val xidAge: Long,
    val xidFreezeMaxAge: Long,
    val autovacuumEnabled: Boolean,
)

data class DatabaseActivity(
    val active: Int,
    val idle: Int,
    val idleInTransaction: Int,
    val waiting: Int,
    /** Age of the oldest open transaction, whatever it is doing. */
    val longestTransactionSeconds: Double?,
    /**
     * Age of the oldest transaction that is open but running nothing.
     * This is the one that silently costs: it pins the vacuum horizon,
     * so expiry's deleted snapshots and compaction's superseded files
     * stay unreclaimable for as long as it sits there.
     */
    val longestIdleInTransactionSeconds: Double?,
    /** Age of the oldest session blocked waiting for a lock. */
    val longestWaitSeconds: Double?,
)

data class DatabaseTable(
    val name: String,
    val liveTuples: Long,
    val deadTuples: Long,
    val tableBytes: Long,
    val indexBytes: Long,
    val toastBytes: Long,
    val seqScans: Long,
    val indexScans: Long,
    val lastVacuum: Instant?,
    val lastAutovacuum: Instant?,
    val lastAnalyze: Instant?,
    val lastAutoanalyze: Instant?,
    val autovacuumCount: Long,
) {
    val totalBytes: Long get() = tableBytes + indexBytes + toastBytes

    /**
     * Dead tuples as a fraction of all tuples. Postgres' own autovacuum
     * threshold is scale_factor (0.2 by default) over this ratio, so it
     * is the number that decides whether a vacuum is due.
     */
    val deadRatio: Double?
        get() = (liveTuples + deadTuples).takeIf { it > 0 }?.let { deadTuples.toDouble() / it }
}

data class DatabaseIndex(
    val table: String,
    val name: String,
    val sizeBytes: Long,
    val scans: Long,
    /**
     * Backs a unique or primary-key constraint. An unused one of these
     * is still doing a job, so it must never be reported as droppable —
     * the distinction is the difference between useful advice and advice
     * that breaks the schema.
     */
    val constraintBacking: Boolean,
)

enum class FindingSeverity { INFO, WARN, CRITICAL }

/**
 * One interpreted observation. [detail] says what was measured and
 * [hoglakeImpact] why it matters for THIS schema — a generic Postgres
 * dashboard can produce the first half; the second is the reason this
 * endpoint exists rather than a link to one.
 */
data class DatabaseFinding(
    val severity: FindingSeverity,
    val code: String,
    val title: String,
    val detail: String,
    val hoglakeImpact: String,
)
