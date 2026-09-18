package com.posthog.hoglake.api

import com.posthog.hoglake.model.DatabaseActivity
import com.posthog.hoglake.model.DatabaseFinding
import com.posthog.hoglake.model.DatabaseHealth
import com.posthog.hoglake.model.DatabaseIndex
import com.posthog.hoglake.model.DatabaseServer
import com.posthog.hoglake.model.DatabaseTable
import java.time.Instant

/**
 * Wire DTOs for GET /v1/database/health (openapi/hoglake.yaml:
 * DatabaseHealth). snake_case rides the app-wide naming strategy.
 *
 * Derived values — dead ratio, total bytes — are computed here and sent,
 * rather than left to the console to recompute. The findings already
 * quote them, so a client that derived its own would be able to disagree
 * with the text beside it.
 */
data class DatabaseHealthDto(
    val server: DatabaseServerDto,
    val activity: DatabaseActivityDto,
    val tables: List<DatabaseTableDto>,
    val indexes: List<DatabaseIndexDto>,
    val findings: List<DatabaseFindingDto>,
)

data class DatabaseServerDto(
    val version: String,
    val database: String,
    val sizeBytes: Long,
    val startedAt: Instant?,
    val connectionsUsed: Int,
    val connectionsMax: Int,
    val cacheHitRatio: Double?,
    val deadlocks: Long,
    val committed: Long,
    val rolledBack: Long,
    val xidAge: Long,
    val xidFreezeMaxAge: Long,
    val autovacuumEnabled: Boolean,
)

data class DatabaseActivityDto(
    val active: Int,
    val idle: Int,
    val idleInTransaction: Int,
    val waiting: Int,
    val longestTransactionSeconds: Double?,
    val longestIdleInTransactionSeconds: Double?,
    val longestWaitSeconds: Double?,
)

data class DatabaseTableDto(
    val name: String,
    val liveTuples: Long,
    val deadTuples: Long,
    val deadRatio: Double?,
    val tableBytes: Long,
    val indexBytes: Long,
    val toastBytes: Long,
    val totalBytes: Long,
    val seqScans: Long,
    val indexScans: Long,
    val lastVacuum: Instant?,
    val lastAutovacuum: Instant?,
    val lastAnalyze: Instant?,
    val lastAutoanalyze: Instant?,
    val autovacuumCount: Long,
)

data class DatabaseIndexDto(
    val table: String,
    val name: String,
    val sizeBytes: Long,
    val scans: Long,
    val constraintBacking: Boolean,
)

data class DatabaseFindingDto(
    val severity: String,
    val code: String,
    val title: String,
    val detail: String,
    val hoglakeImpact: String,
)

fun DatabaseHealth.toDto() =
    DatabaseHealthDto(
        server = server.toDto(),
        activity = activity.toDto(),
        tables = tables.map { it.toDto() },
        indexes = indexes.map { it.toDto() },
        findings = findings.map { it.toDto() },
    )

fun DatabaseServer.toDto() =
    DatabaseServerDto(
        version = version,
        database = database,
        sizeBytes = sizeBytes,
        startedAt = startedAt,
        connectionsUsed = connectionsUsed,
        connectionsMax = connectionsMax,
        cacheHitRatio = cacheHitRatio,
        deadlocks = deadlocks,
        committed = committed,
        rolledBack = rolledBack,
        xidAge = xidAge,
        xidFreezeMaxAge = xidFreezeMaxAge,
        autovacuumEnabled = autovacuumEnabled,
    )

fun DatabaseActivity.toDto() =
    DatabaseActivityDto(
        active = active,
        idle = idle,
        idleInTransaction = idleInTransaction,
        waiting = waiting,
        longestTransactionSeconds = longestTransactionSeconds,
        longestIdleInTransactionSeconds = longestIdleInTransactionSeconds,
        longestWaitSeconds = longestWaitSeconds,
    )

fun DatabaseTable.toDto() =
    DatabaseTableDto(
        name = name,
        liveTuples = liveTuples,
        deadTuples = deadTuples,
        deadRatio = deadRatio,
        tableBytes = tableBytes,
        indexBytes = indexBytes,
        toastBytes = toastBytes,
        totalBytes = totalBytes,
        seqScans = seqScans,
        indexScans = indexScans,
        lastVacuum = lastVacuum,
        lastAutovacuum = lastAutovacuum,
        lastAnalyze = lastAnalyze,
        lastAutoanalyze = lastAutoanalyze,
        autovacuumCount = autovacuumCount,
    )

fun DatabaseIndex.toDto() =
    DatabaseIndexDto(
        table = table,
        name = name,
        sizeBytes = sizeBytes,
        scans = scans,
        constraintBacking = constraintBacking,
    )

fun DatabaseFinding.toDto() =
    DatabaseFindingDto(
        severity = severity.name.lowercase(),
        code = code,
        title = title,
        detail = detail,
        hoglakeImpact = hoglakeImpact,
    )
