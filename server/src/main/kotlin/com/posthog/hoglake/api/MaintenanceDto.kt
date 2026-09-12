package com.posthog.hoglake.api

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.model.CatalogOptions
import com.posthog.hoglake.model.CleanupResult
import com.posthog.hoglake.model.CompactionResult
import com.posthog.hoglake.model.ExpiryResult
import com.posthog.hoglake.model.InstanceMaintenanceStatus
import com.posthog.hoglake.model.MaintenanceBacklog
import com.posthog.hoglake.model.MaintenanceRun
import com.posthog.hoglake.model.MaintenanceRunPage
import com.posthog.hoglake.model.MaintenanceStatus
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
)

fun ExpiryResult.toDto() =
    ExpiryResultDto(
        snapshotsExpired = snapshotsExpired,
        dataFilesQueued = dataFilesQueued,
        deleteFilesQueued = deleteFilesQueued,
        newEarliestSnapshotId = newEarliestSnapshotId,
        flooredByConsumer = flooredByConsumer,
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
    val failedGroups: Long,
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
        failedGroups = failedGroups,
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
        result = resultJson?.let { RAW_JSON.readTree(it) },
    )

data class MaintenanceTaskStatusDto(
    val task: String,
    /** Absent under NON_NULL for manual-only tasks (verify); 0 = loop disabled. */
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
)

fun MaintenanceTaskStatus.toDto() =
    MaintenanceTaskStatusDto(
        task = task.wire,
        loopIntervalMs = loopIntervalMs,
        lastRun = lastRun?.toDto(),
        backlog = backlog,
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
