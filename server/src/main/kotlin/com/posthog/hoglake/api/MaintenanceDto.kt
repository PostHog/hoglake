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
import com.posthog.hoglake.model.VerifyCheck
import com.posthog.hoglake.model.VerifyReport
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
)

fun ExpiryResult.toDto() =
    ExpiryResultDto(
        snapshotsExpired = snapshotsExpired,
        dataFilesQueued = dataFilesQueued,
        deleteFilesQueued = deleteFilesQueued,
        newEarliestSnapshotId = newEarliestSnapshotId,
        flooredByConsumer = flooredByConsumer,
        offsetsReleased = offsetsReleased,
    )

data class CleanupResultDto(
    val removed: Long,
    val missing: Long,
    val stillReferenced: Long,
)

fun CleanupResult.toDto() =
    CleanupResultDto(
        removed = removed,
        missing = missing,
        stillReferenced = stillReferenced,
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
    )

data class RehydrateResultDto(
    val requeued: Long,
)

fun RehydrateResult.toDto() = RehydrateResultDto(requeued = requeued)

data class VerifyCheckDto(
    val check: String,
    val status: String,
    val violations: Long,
    val samples: List<String>,
    /**
     * The invariant this check enforces, one paragraph, in AGENT.md's
     * own words. Additive: existing consumers ignore it, and a report
     * read by somebody who has never seen the code still says what was
     * violated rather than only that something was.
     *
     * NULLABLE, and optional in the spec, for one reason: the same
     * `VerifyCheck` schema describes this response AND the `result`
     * payload of a `verify` row in the maintenance run ledger, which is
     * stored raw and replayed verbatim. Ledger rows deliberately carry
     * no descriptions ([VerifyReport.forLedger] — identical constant
     * prose in every row), and rows written before the field existed
     * carry none either. Marking it required would make the spec
     * contradict every historical row. A LIVE response always fills it.
     */
    @get:JsonInclude(JsonInclude.Include.NON_NULL)
    val description: String?,
)

data class VerifyReportDto(
    val catalog: String,
    val status: String,
    val checks: List<VerifyCheckDto>,
)

fun VerifyCheck.toDto() =
    VerifyCheckDto(
        check = check,
        status = status,
        violations = violations,
        samples = samples,
        description = description,
    )

fun VerifyReport.toDto() =
    VerifyReportDto(
        catalog = catalog,
        status = status,
        checks = checks.map { it.toDto() },
    )

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
    listOf("invalid_data", "heap_budget_exceeded", "claimed_elsewhere")

/** The same, for ExpiryResult. Append-only for the same reason. */
private val EXPIRY_COUNTERS_ADDED_LATER = listOf("offsets_released")

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
     * for a task with no loop, of which there are none today; 0 = the
     * loop is off IN THIS PROCESS, which is how the API workload runs
     * verify). A
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
