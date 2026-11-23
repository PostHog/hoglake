package com.posthog.hoglake.compaction

import com.posthog.hoglake.compaction.CompactionService.GroupOutcome
import com.posthog.hoglake.compaction.CompactionWork.CandidateFetch
import com.posthog.hoglake.compaction.CompactionWork.PlanWithContext
import com.posthog.hoglake.compaction.CompactionWork.PlannedWork
import com.posthog.hoglake.compaction.CompactionWork.TableContext
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CompactionResult
import com.posthog.hoglake.model.MaintenanceTrigger
import java.util.UUID

/** Storage operations for the compaction executor. No transaction crosses object-store work. */
internal interface CompactionStorage {
    /** Retain the PostgreSQL planning connection, not a transaction, across packing and reservation. */
    fun <T> withPlanningSession(body: (PlanningSession) -> T): T

    interface PlanningSession {
        /** Fetch candidates with their table context at one pinned head. */
        fun fetch(
            catalog: String,
            namespace: String,
            table: String,
            cfg: CompactionConfig,
        ): CandidateFetch

        /** Read sibling claims after packing. */
        fun liveClaimedFileIds(ctx: TableContext): Set<Long>

        /** Acquire the planning lock in its own transaction. Reserve the queue in a second transaction. */
        fun reserve(
            planned: PlanWithContext,
            cfg: CompactionConfig,
            room: Int,
            claimant: UUID,
        ): PlannedWork
    }

    fun liveTables(catalog: String): List<Pair<String, String>>

    fun purgeExpired(catalog: String)

    fun refreshClaim(
        ctx: TableContext,
        claimKey: String,
        bucketKeys: List<String>,
        claimant: UUID,
        cfg: CompactionConfig,
    ): Boolean

    fun releaseClaim(
        ctx: TableContext,
        claimKey: String,
        claimant: UUID,
    )

    fun settleClaim(
        ctx: TableContext,
        claimKey: String,
        claimant: UUID,
        cfg: CompactionConfig,
        committed: Boolean,
        inputFileIds: List<Long>?,
    )

    /** Commit the output reservation before the rewrite starts. */
    fun stageOutputPath(
        catalogId: Long,
        outputPath: String,
    ): Long

    /** Publish one rewrite through the existing guarded transaction. */
    fun publish(
        ctx: TableContext,
        group: CompactionGroup,
        outputPath: String,
        outputBytes: Long,
        footerSize: Long,
        stats: List<ColumnStats>?,
        splitOffsets: List<Long>?,
        survivors: Long,
        rowIdStart: Long,
        stagingId: Long,
    ): GroupOutcome

    fun recorded(
        catalog: String,
        trigger: MaintenanceTrigger,
        body: () -> CompactionResult,
    ): CompactionResult

    /** Classify a driver statement failure. Preserve the original exception for logging and cancellation checks. */
    fun isStatementLockTimeout(error: Throwable): Boolean

    /** Stop the sweep for a terminal backend failure. PostgreSQL retains group isolation. */
    fun isTerminalFailure(error: Throwable): Boolean = false

    /** Skip a corrupt table plan. PostgreSQL retains its existing planning failure behavior. */
    fun isRecoverablePlanningFailure(error: Throwable): Boolean = false

    /**
     * Run a sweep's group workers under the sweep thread's request
     * admission (`RequestAdmission.carrying`). Hoglite: a manual sweep's
     * thread blocks on its workers, so their calls are the request's —
     * under its bulkhead permit and budget cap (#679). PostgreSQL keeps
     * them background work, as the `RequestAdmission` KDoc describes.
     */
    val carriesRequestAdmission: Boolean get() = false
}
