package com.posthog.hoglake.api

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.posthog.hoglake.model.CatalogOptions
import com.posthog.hoglake.model.CleanupResult
import com.posthog.hoglake.model.CompactionResult
import com.posthog.hoglake.model.ExpiryResult
import com.posthog.hoglake.model.InstanceMaintenanceStatus
import com.posthog.hoglake.model.MaintenanceBacklog
import com.posthog.hoglake.model.MaintenanceRun
import com.posthog.hoglake.model.MaintenanceRunPage
import com.posthog.hoglake.model.MaintenanceStatus
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTaskStatus
import com.posthog.hoglake.model.RehydrateResult
import com.posthog.hoglake.service.PatchField
import io.ktor.server.plugins.BadRequestException
import java.time.Instant

/**
 * Wire DTOs for the options/maintenance surface (openapi/hoglake.yaml:
 * CatalogOptions, ExpiryResult, CleanupResult). snake_case rides the
 * app-wide Jackson naming strategy; NON_NULL inclusion means a null
 * snapshot_retention_seconds / floored_by_consumer is omitted from the
 * body (both optional in the spec).
 */

data class CatalogOptionsDto(
    val snapshotRetentionSeconds: Long?,
    val consumerFloor: Boolean,
    val earliestSnapshotId: Long,
)

fun CatalogOptions.toDto() =
    CatalogOptionsDto(
        snapshotRetentionSeconds = snapshotRetentionSeconds,
        consumerFloor = consumerFloor,
        earliestSnapshotId = earliestSnapshotId,
    )

data class ExpiryResultDto(
    val snapshotsExpired: Long,
    val dataFilesQueued: Long,
    val deleteFilesQueued: Long,
    val newEarliestSnapshotId: Long,
    val flooredByConsumer: String? = null,
    /**
     * Superseded consumer offsets the sweep deleted. Optional in the
     * spec: a ledger row written before the counter existed replays
     * without it, the same position invalid_data took on
     * CompactionResult — and it is filled in on read by
     * [normalizeLedgerResult] so a client generated from the spec does
     * not meet an absent required field.
     */
    val offsetsReleased: Long,
    /**
     * Data-file rows the sweep's phase-B purge deleted below the floor —
     * equal to [dataFilesQueued] by construction, because the delete and
     * the `hog_file_removal` insert are one statement. See
     * `ExpiryResult.dataFilesPurged` for why both are reported.
     */
    val dataFilesPurged: Long,
    /** Pages that purge ran, across both arms; empty pages are not counted. */
    val purgePages: Long,
    /**
     * Purge pages that threw. Separate from the row counters because 0
     * purged is otherwise the same number for an idle sweep and a
     * failing one — see `ExpiryResult.purgeFailures`.
     */
    val purgeFailures: Long,
    /**
     * The purge stopped with work possibly left: the budget, a page that
     * failed even at one row, or a floor that could not be read.
     * Serialized unconditionally like the counters: a response that omits
     * a flag it declares makes every client's `false` a guess.
     */
    val purgeTruncated: Boolean,
    /**
     * File rows still eligible when a truncated purge stopped — both
     * tables — saturating at `ExpiryService.PURGE_REMAINING_CAP` each.
     * 0 when the purge drained, and ABSENT when the count itself could
     * not finish inside its 5 s bound: see
     * `ExpiryResult.purgeRemaining` for why an unknown is not reported
     * as a zero. The one nullable field in this DTO, and the only one
     * whose absence means something other than "a zero from an older
     * build".
     */
    @get:JsonInclude(JsonInclude.Include.NON_NULL)
    val purgeRemaining: Long?,
    /** Phase-A batch halvings: the floor advance's statement bound firing. */
    val advanceHalvings: Long,
    /** Phase-B page halvings, across both arms. */
    val purgeHalvings: Long,
)

fun ExpiryResult.toDto() =
    ExpiryResultDto(
        snapshotsExpired = snapshotsExpired,
        dataFilesQueued = dataFilesQueued,
        deleteFilesQueued = deleteFilesQueued,
        newEarliestSnapshotId = newEarliestSnapshotId,
        flooredByConsumer = flooredByConsumer,
        offsetsReleased = offsetsReleased,
        dataFilesPurged = dataFilesPurged,
        purgePages = purgePages,
        purgeFailures = purgeFailures,
        purgeTruncated = purgeTruncated,
        purgeRemaining = purgeRemaining,
        advanceHalvings = advanceHalvings,
        purgeHalvings = purgeHalvings,
    )

data class CleanupResultDto(
    /** Queue rows settled 'deleted' — see [objectsRemoved] for objects. */
    val removed: Long,
    val missing: Long,
    val stillReferenced: Long,
    /**
     * DISTINCT paths whose object was physically deleted. Serialized
     * unconditionally here, unlike on the stored model: this DTO is the
     * RESPONSE, and a response that omits a counter it declares makes
     * every client's zero a guess. The ledger's read path is what fills
     * 0 for rows an older server wrote — see CLEANUP_COUNTERS_ADDED_LATER.
     */
    val objectsRemoved: Long,
    /** Rows a sub-batch could not settle because the row was no longer its own. */
    val settledElsewhere: Long,
    /** Always 0: the hold budget it counted bounded a lock the drain no longer takes. */
    val deadlineSkipped: Long,
    /**
     * Commit receipts past their retention that this run deleted (#240).
     *
     * Serialized unconditionally here, unlike on the stored model, for
     * objectsRemoved's reason: a response that omits a counter it
     * declares makes every client's zero a guess.
     */
    val receiptsPurged: Long,
    /**
     * Purge pages that threw. Separate from receipts_purged because 0
     * purged is otherwise the same number for an idle run and a failing
     * one — see CleanupResult.receiptsPurgeFailures.
     */
    val receiptsPurgeFailures: Long,
)

fun CleanupResult.toDto() =
    CleanupResultDto(
        removed = removed,
        missing = missing,
        stillReferenced = stillReferenced,
        objectsRemoved = objectsRemoved,
        settledElsewhere = settledElsewhere,
        deadlineSkipped = deadlineSkipped,
        receiptsPurged = receiptsPurged,
        receiptsPurgeFailures = receiptsPurgeFailures,
    )

data class CompactionResultDto(
    val groupsCompacted: Long,
    val filesIn: Long,
    val filesOut: Long,
    val bytesIn: Long,
    val bytesOut: Long,
    val skippedConflicts: Long,
    val dvSuperseded: Long,
    val unconvertibleSchema: Long,
    val invalidData: Long,
    /** HISTORICAL: always 0 since hoglake#134; see CompactionResult.heapBudgetExceeded. */
    val heapBudgetExceeded: Long,
    val failedGroups: Long,
    /**
     * Groups another maintenance replica's live claim covered, so this
     * sweep never spent their IO. See CompactionResult.claimedElsewhere.
     *
     * Serialized unconditionally here, unlike on the stored model: this
     * DTO is the RESPONSE, and a response that omits a counter it
     * declares makes every client's zero a guess. The ledger's read path
     * is what fills 0 for rows an older server wrote — see
     * COMPACTION_COUNTERS_ADDED_LATER.
     */
    val claimedElsewhere: Long,
    /**
     * The PLAN measures — what the sweep's planners read, and how long
     * it took — rather than what any rewrite did. See
     * CompactionResult.candidatesFetched for the unbounded read they
     * replaced.
     *
     * Serialized unconditionally, like every counter on this DTO and
     * for the same reason: a response that omits a field it declares
     * makes the client's zero a guess.
     */
    val candidatesFetched: Long,
    val bucketsConsidered: Long,
    val bucketsAvailable: Long,
    val candidatesTruncated: Long,
    val planMs: Long,
    /**
     * The sorted rewrite's external merge sort (hoglake#134): runs read
     * in place, spilled and demoted, bytes spilled, the two budget
     * refusals, and spill directories left behind. See the matching
     * CompactionResult fields. Serialized unconditionally, like every
     * counter on this DTO; COMPACTION_COUNTERS_ADDED_LATER fills 0 for
     * ledger rows written before they existed.
     */
    val runsTrusted: Long,
    val runsSpilled: Long,
    val runsDemoted: Long,
    val spillBytes: Long,
    val spillBudgetExceeded: Long,
    val mergeBudgetExceeded: Long,
    val spillCleanupFailures: Long,
    /**
     * The sortedness pre-pass's verdicts: verified; out of order, or not
     * checkable (id-less columns, a key under a container, an unsortable
     * physical type, a null row-id carrier); under the size floor. See the
     * matching CompactionResult fields.
     */
    val filesVerified: Long,
    val filesUnsorted: Long,
    val filesUnchecked: Long,
    /**
     * Input row groups appended byte for byte instead of re-encoded, and
     * their compressed bytes. See CompactionResult.rowGroupsAppended.
     */
    val rowGroupsAppended: Long,
    val bytesAppended: Long,
    /**
     * Groups retired with no output because every input was fully
     * deleted. See CompactionResult.groupsRetired.
     */
    val groupsRetired: Long,
    /**
     * Sorted-rewrite runs with no usable first-key range, in groups where
     * that cost an append. See CompactionResult.filesUnranged.
     */
    val filesUnranged: Long,
)

fun CompactionResult.toDto() =
    CompactionResultDto(
        groupsCompacted = groupsCompacted,
        filesIn = filesIn,
        filesOut = filesOut,
        bytesIn = bytesIn,
        bytesOut = bytesOut,
        skippedConflicts = skippedConflicts,
        dvSuperseded = dvSuperseded,
        unconvertibleSchema = unconvertibleSchema,
        invalidData = invalidData,
        heapBudgetExceeded = heapBudgetExceeded,
        failedGroups = failedGroups,
        claimedElsewhere = claimedElsewhere,
        candidatesFetched = candidatesFetched,
        bucketsConsidered = bucketsConsidered,
        bucketsAvailable = bucketsAvailable,
        candidatesTruncated = candidatesTruncated,
        planMs = planMs,
        runsTrusted = runsTrusted,
        runsSpilled = runsSpilled,
        runsDemoted = runsDemoted,
        spillBytes = spillBytes,
        spillBudgetExceeded = spillBudgetExceeded,
        mergeBudgetExceeded = mergeBudgetExceeded,
        spillCleanupFailures = spillCleanupFailures,
        filesVerified = filesVerified,
        filesUnsorted = filesUnsorted,
        filesUnchecked = filesUnchecked,
        rowGroupsAppended = rowGroupsAppended,
        bytesAppended = bytesAppended,
        groupsRetired = groupsRetired,
        filesUnranged = filesUnranged,
    )

data class RehydrateResultDto(
    val requeued: Long,
)

fun RehydrateResult.toDto() = RehydrateResultDto(requeued = requeued)

// ---- maintenance status + run ledger (openapi: MaintenanceStatus, ----
// ---- MaintenanceRun, MaintenanceRunPage) ------------------------------

/**
 * run_id stays a Long on the wire (bare JSON number, like every int64 —
 * the webui carries it as a decimal string past its reviver). `result`
 * is ALWAYS in the body: a failed run has none, and an absent-vs-null
 * shape difference would push null-handling onto every reader (the
 * PartitionValue.value precedent).
 */
data class MaintenanceRunDto(
    val runId: Long,
    val catalog: String,
    val task: String,
    val trigger: String,
    val startedAt: java.time.Instant,
    val finishedAt: java.time.Instant,
    val status: String,
    val error: String?,
    @get:JsonInclude(JsonInclude.Include.ALWAYS)
    val result: JsonNode?,
)

fun MaintenanceRun.toDto(): MaintenanceRunDto =
    MaintenanceRunDto(
        runId = runId,
        catalog = catalog,
        task = task.wire,
        trigger = trigger.wire,
        startedAt = startedAt,
        finishedAt = finishedAt,
        status = status.wire,
        error = error,
        result = resultJson?.let { normalizeLedgerResult(task, RAW_JSON.readTree(it)) },
    )

/**
 * Fill in counters a ledger row predates.
 *
 * A run's result is stored as the RAW JSON the API returned at the time
 * and handed back verbatim, so every row written before a counter
 * existed is missing it — while the OpenAPI schema lists it as
 * required. A client generated from the spec then meets a
 * CompactionResult with no `invalid_data` and a webui guard written as
 * `!== "0"` is TRUE for `undefined`, which is why every historical run
 * grew an "invalid-data —" badge.
 *
 * Normalizing on READ rather than migrating: the stored row is an
 * accurate record of what that run reported, and rewriting history to
 * make a later schema fit is the worse of the two. Zero is not a guess
 * here — the counter did not exist, so nothing it counts could have
 * happened.
 */
private fun normalizeLedgerResult(
    task: MaintenanceTask,
    node: JsonNode,
): JsonNode {
    if (!node.isObject) return node
    val added =
        when (task) {
            MaintenanceTask.COMPACTION -> COMPACTION_COUNTERS_ADDED_LATER
            MaintenanceTask.EXPIRY -> EXPIRY_COUNTERS_ADDED_LATER
            MaintenanceTask.CLEANUP -> CLEANUP_COUNTERS_ADDED_LATER
            else -> return node
        }
    val obj = node as ObjectNode
    for (field in added) {
        if (!obj.has(field)) obj.put(field, 0L)
    }
    return obj
}

/**
 * Compaction result counters added after the ledger started recording.
 * Append-only: a counter joins this list in the same change that adds
 * it to CompactionResult, and never leaves.
 */
private val COMPACTION_COUNTERS_ADDED_LATER =
    listOf(
        "invalid_data",
        "heap_budget_exceeded",
        "claimed_elsewhere",
        "candidates_fetched",
        "buckets_considered",
        "buckets_available",
        "candidates_truncated",
        "plan_ms",
        "runs_trusted",
        "runs_spilled",
        "runs_demoted",
        "spill_bytes",
        "spill_budget_exceeded",
        "merge_budget_exceeded",
        "spill_cleanup_failures",
        "files_verified",
        "files_unsorted",
        "files_unchecked",
        "row_groups_appended",
        "bytes_appended",
        "groups_retired",
        "files_unranged",
    )

/**
 * The same, for ExpiryResult. Append-only for the same reason.
 *
 * TWO FIELDS ARE DELIBERATELY NOT HERE, for two different reasons.
 * `purge_truncated` is a BOOLEAN and this normalizer fills a missing
 * field with the integer 0, so listing it would write a value its own
 * schema does not allow. `purge_remaining` is an integer but its
 * ABSENCE MEANS SOMETHING: the count is best-effort, and a sweep whose
 * count could not finish omits it to say "unknown" rather than claim a
 * zero that would contradict `purge_truncated` (see
 * `ExpiryResult.purgeRemaining`). Filling that with 0 on read would
 * re-introduce exactly the lie the nullability exists to prevent, and a
 * pre-upgrade row's absence is honest for the same reason — it had no
 * purge. Every console guard on both is undefined-safe.
 */
private val EXPIRY_COUNTERS_ADDED_LATER =
    listOf(
        "offsets_released",
        "data_files_purged",
        "purge_pages",
        "purge_failures",
        "advance_halvings",
        "purge_halvings",
    )

/** The same, for CleanupResult. Append-only for the same reason. */
private val CLEANUP_COUNTERS_ADDED_LATER =
    listOf(
        "objects_removed",
        "settled_elsewhere",
        "deadline_skipped",
        "receipts_purged",
        "receipts_purge_failures",
    )

/**
 * What the run ledger observed about a task's loop — fleet-wide, unlike
 * [MaintenanceTaskStatusDto.loopIntervalMs].
 */
data class LoopObservationDto(
    /** Absent under NON_NULL when the ledger cannot state a cadence. */
    val observedIntervalMs: Long?,
    /** Absent under NON_NULL when no loop run is inside the retention window. */
    val lastRunAt: Instant?,
    /** How to read silence here — see the spec's LoopObservation. */
    val recordsEverySweep: Boolean,
)

data class MaintenanceTaskStatusDto(
    val task: String,
    /**
     * The RESPONDING PROCESS's configured cadence (absent under NON_NULL
     * for a task with no loop — `verify`, whose subsystem #261
     * removed, is the only one and it is not in the task list at all;
     * 0 = the loop is off IN THIS PROCESS, which is how the API
     * workload runs compaction and retirement). A
     * deployment may run a task's loop in a different pod from the one
     * serving the API, so a reader must never turn this into "the task
     * is not running" — that is [loop]'s job.
     */
    val loopIntervalMs: Long?,
    /** ALWAYS included: null = the task has no recorded run. */
    @get:JsonInclude(JsonInclude.Include.ALWAYS)
    val lastRun: MaintenanceRunDto?,
    /**
     * The sealed backlog payload rides the app-wide snake_case Jackson
     * config (pendingFiles -> pending_files; null fields like
     * ExpiryBacklog.snapshotRetentionSeconds drop out under NON_NULL,
     * matching the spec's "absent when disabled" contract).
     */
    val backlog: MaintenanceBacklog,
    /**
     * ALWAYS included, so a client can tell "this task has no loop"
     * (null) from "an older server did not report" (the key is missing)
     * — the two must not collapse into the same silence during a
     * rollout, where the webui runs ahead of the server for a few
     * minutes.
     */
    @get:JsonInclude(JsonInclude.Include.ALWAYS)
    val loop: LoopObservationDto?,
)

fun MaintenanceTaskStatus.toDto() =
    MaintenanceTaskStatusDto(
        task = task.wire,
        loopIntervalMs = loopIntervalMs,
        lastRun = lastRun?.toDto(),
        backlog = backlog,
        loop = loop?.let { LoopObservationDto(it.intervalMs, it.lastRunAt, it.recordsEverySweep) },
    )

data class MaintenanceStatusDto(
    val catalog: String,
    val tasks: List<MaintenanceTaskStatusDto>,
    val sampledAt: Instant?,
    val sampleStartedAt: Instant?,
    val sampledSnapshotId: Long?,
)

fun MaintenanceStatus.toDto() =
    MaintenanceStatusDto(
        catalog,
        tasks.map {
            it.toDto()
        },
        sampledAt,
        sampleStartedAt,
        sampledSnapshotId,
    )

data class InstanceMaintenanceStatusDto(
    val catalogs: List<MaintenanceStatusDto>,
    val hasMore: Boolean,
    val nextAfter: String?,
)

fun InstanceMaintenanceStatus.toDto() = InstanceMaintenanceStatusDto(catalogs.map { it.toDto() }, hasMore, nextAfter)

data class MaintenanceRunPageDto(
    val runs: List<MaintenanceRunDto>,
    val hasMore: Boolean,
)

fun MaintenanceRunPage.toDto() = MaintenanceRunPageDto(runs = runs.map { it.toDto() }, hasMore = hasMore)

/** Raw-JSON parse for ledger result payloads (they are already wire-shaped JSON text). */
private val RAW_JSON = ObjectMapper()

/**
 * PATCH /options body, parsed from raw JSON because absent-vs-null is
 * semantic here: an absent snapshot_retention_seconds leaves retention
 * unchanged, an explicit null disables expiry. (A typed DTO cannot see
 * the difference once Jackson has bound it.)
 *
 * Shape errors — non-object body, non-integral retention, non-boolean
 * or null consumer_floor — are [BadRequestException] (-> 400); value
 * errors (retention <= 0) flow out of OptionsService as Validation
 * (-> 422), per the AlterDto precedent.
 */
data class OptionsPatchRequest(
    val snapshotRetentionSeconds: PatchField<Long>,
    val consumerFloor: Boolean?,
) {
    companion object {
        private const val RETENTION = "snapshot_retention_seconds"
        private const val FLOOR = "consumer_floor"

        fun from(node: JsonNode): OptionsPatchRequest {
            if (!node.isObject) throw BadRequestException("request body must be a JSON object")
            val retention: PatchField<Long> =
                when {
                    !node.has(RETENTION) -> PatchField.Absent
                    node.get(RETENTION).isNull -> PatchField.Set(null)
                    node.get(RETENTION).canConvertToLong() && node.get(RETENTION).isIntegralNumber ->
                        PatchField.Set(node.get(RETENTION).longValue())
                    else -> throw BadRequestException(
                        "'$RETENTION' must be an integer or null, got ${node.get(RETENTION)}",
                    )
                }
            val floor: Boolean? =
                when {
                    !node.has(FLOOR) -> null
                    node.get(FLOOR).isBoolean -> node.get(FLOOR).booleanValue()
                    else -> throw BadRequestException(
                        "'$FLOOR' must be a boolean, got ${node.get(FLOOR)}",
                    )
                }
            return OptionsPatchRequest(retention, floor)
        }
    }
}
