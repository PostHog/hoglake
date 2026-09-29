package com.posthog.hoglake.api

import com.fasterxml.jackson.annotation.JsonInclude
import com.posthog.hoglake.model.PartitionGroup
import com.posthog.hoglake.model.PartitionListing
import com.posthog.hoglake.model.PartitionListingValue
import com.posthog.hoglake.model.PartitionSpecField
import com.posthog.hoglake.model.PartitionSpecSummary
import com.posthog.hoglake.service.PartitionListingService
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.plugins.BadRequestException
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import java.time.Instant

/**
 * `GET /v1/catalogs/{c}/namespaces/{n}/tables/{t}/partitions` (openapi
 * `listTablePartitions`): the console's partitions tab. Handler
 * translates wire <-> model only (Routes.kt division of labor);
 * semantics live in [PartitionListingService], whose HoglakeExceptions
 * are mapped by ErrorMapping (404 unknown catalog/namespace/table, 422
 * an out-of-range filter key).
 *
 * Installed separately from `installApiRoutes` for the same reason the
 * scan and partition-stats routes are: the service is built from the
 * compaction policy, not from `CatalogService`, and threading it
 * through the main installer would put a maintenance dependency on
 * every call site that mounts the API.
 *
 * Parameter conventions and error texts are the FILES listing's, on
 * purpose — the two tabs sit beside each other and a client that can
 * page one can page the other. The differences are stated where they
 * are: no `snapshot`/`at_timestamp` (there is one sampled snapshot and
 * the body names it), `filter` instead of `partition` because it
 * matches the DECODED value by prefix rather than the stored value
 * exactly, and a default `limit` because a sample listing has no
 * historical unbounded caller to keep working.
 */
fun Application.installPartitionListingRoutes(listings: PartitionListingService) {
    routing {
        get("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/partitions") {
            call.respond(
                listings.listPartitions(
                    call.catalog(),
                    call.namespace(),
                    call.table(),
                    sort = call.partitionSort(),
                    desc = call.partitionOrder(),
                    limit = call.partitionLimit(),
                    offset = call.partitionOffset(),
                    filters = call.partitionFilters(),
                ).toDto(),
            )
        }
    }
}

private fun ApplicationCall.partitionSort(): PartitionListingService.SortColumn {
    val raw = request.queryParameters["sort"] ?: return PartitionListingService.SortColumn.PARTITION
    return PartitionListingService.SortColumn.fromWire(raw)
        ?: throw BadRequestException(
            "query parameter 'sort' must be one of " +
                PartitionListingService.SortColumn.entries.joinToString(", ") { it.wire } +
                ", got '$raw'",
        )
}

/**
 * null = "the sort column's own default direction" — ascending for
 * `partition`, descending for every measure. An explicit `order` always
 * wins, so a caller can ask for the least-debt partitions.
 */
private fun ApplicationCall.partitionOrder(): Boolean? =
    when (val o = request.queryParameters["order"]) {
        null -> null
        "asc" -> false
        "desc" -> true
        else -> throw BadRequestException("query parameter 'order' must be 'asc' or 'desc', got '$o'")
    }

private fun ApplicationCall.partitionLimit(): Int =
    intQuery("limit")?.also {
        if (it < 1) throw BadRequestException("query parameter 'limit' must be positive, got '$it'")
    } ?: PartitionListingService.DEFAULT_LIMIT

private fun ApplicationCall.partitionOffset(): Int =
    intQuery("offset")?.also {
        if (it < 0) throw BadRequestException("query parameter 'offset' must be >= 0, got '$it'")
    } ?: 0

/**
 * `filter=key_index:text`, repeatable, ANDed.
 *
 * The text is matched against the DECODED value the console shows, as a
 * case-insensitive prefix — `0:2026-09` is every day of that month
 * under a day transform. An EMPTY text (`0:`) is the NULL value, which
 * no prefix could otherwise name; the files listing has no equivalent
 * because it matches stored values exactly.
 */
private fun ApplicationCall.partitionFilters(): List<PartitionListingService.Filter> =
    request.queryParameters.getAll("filter")?.map { parsePartitionFilter(it) } ?: emptyList()

/**
 * One `filter=` clause, as a pure function of its text.
 *
 * Top-level and `internal` rather than a method on the call, so
 * `WireDtoParseFuzzTest` can drive it over arbitrary bytes the way it
 * drives `parseLongQuery` and `parseScanRequest` — AGENT.md asks new
 * parse surface to join an existing target, and a parser that needs an
 * `ApplicationCall` cannot. Everything it can throw is a
 * [BadRequestException], which ErrorMapping turns into a 400.
 */
internal fun parsePartitionFilter(param: String): PartitionListingService.Filter {
    val parts = param.split(":", limit = 2)
    if (parts.size != 2) {
        throw BadRequestException("query parameter 'filter' must be key_index:text, got '$param'")
    }
    val keyIndex =
        parts[0].toIntOrNull()
            ?: throw BadRequestException(
                "filter key_index must be a non-negative integer, got '${parts[0]}'",
            )
    if (keyIndex < 0) {
        throw BadRequestException(
            "filter key_index must be a non-negative integer, got '${parts[0]}'",
        )
    }
    return PartitionListingService.Filter(keyIndex, parts[1])
}

// ---- wire DTOs (openapi/hoglake.yaml: PartitionListing) ------------------

data class PartitionListingValueDto(
    val field: String,
    /**
     * ALWAYS included, both of them: a null partition value must
     * serialize as `"raw": null` / `"decoded": null` rather than vanish
     * under the app-wide NON_NULL inclusion. The triple's shape is the
     * contract, and a missing key would read as a decoder that dropped
     * the element.
     */
    @get:JsonInclude(JsonInclude.Include.ALWAYS)
    val raw: String?,
    @get:JsonInclude(JsonInclude.Include.ALWAYS)
    val decoded: String?,
)

data class PartitionSpecFieldDto(
    val field: String,
    val transform: String,
    /** Omitted (NON_NULL) for every transform but bucket(n). */
    val transformParam: Int?,
    val sourceFieldId: Long,
)

data class PartitionSpecSummaryDto(
    val specId: Long,
    val fields: List<PartitionSpecFieldDto>,
)

data class PartitionGroupDto(
    /** Omitted (NON_NULL) for an unpartitioned vintage. */
    val specId: Long?,
    val values: List<PartitionListingValueDto>,
    val fileCount: Long,
    val smallFileCount: Long,
    val totalBytes: Long,
    val smallFileBytes: Long,
    val avgFileBytes: Long,
    val dvCount: Long,
    val debtScore: Long,
    /**
     * ALWAYS included: null here means "this generation was sampled
     * before the sampler measured rows", which a client must be able to
     * tell from zero — an omitted key reads as the former on a client
     * that defaults it and as the latter on one that does not.
     */
    @get:JsonInclude(JsonInclude.Include.ALWAYS)
    val recordCount: Long?,
    @get:JsonInclude(JsonInclude.Include.ALWAYS)
    val lastWrittenSnapshot: Long?,
)

data class PartitionListingDto(
    /** ALWAYS included: null is the "no sample yet" state the console renders. */
    @get:JsonInclude(JsonInclude.Include.ALWAYS)
    val sampledAt: Instant?,
    /**
     * When the scan that produced these numbers STARTED — the age a
     * freshness display must use. A generation runs for ~30 minutes on
     * a production catalog, so `sampled_at` (publish time) understates
     * the numbers' age by that much.
     */
    @get:JsonInclude(JsonInclude.Include.ALWAYS)
    val sampleStarted: Instant?,
    @get:JsonInclude(JsonInclude.Include.ALWAYS)
    val sampledSnapshotId: Long?,
    /** Omitted (NON_NULL) when the table is unpartitioned at head. */
    val spec: PartitionSpecSummaryDto?,
    val total: Int,
    val staleSpecGroups: Int,
    val partitions: List<PartitionGroupDto>,
)

fun PartitionListingValue.toDto() = PartitionListingValueDto(field = field, raw = raw, decoded = decoded)

fun PartitionSpecField.toDto() =
    PartitionSpecFieldDto(
        field = field,
        transform = transform,
        transformParam = transformParam,
        sourceFieldId = sourceFieldId,
    )

fun PartitionSpecSummary.toDto() = PartitionSpecSummaryDto(specId = specId, fields = fields.map { it.toDto() })

fun PartitionGroup.toDto() =
    PartitionGroupDto(
        specId = specId,
        values = values.map { it.toDto() },
        fileCount = fileCount,
        smallFileCount = smallFileCount,
        totalBytes = totalBytes,
        smallFileBytes = smallFileBytes,
        avgFileBytes = avgFileBytes,
        dvCount = dvCount,
        debtScore = debtScore,
        recordCount = recordCount,
        lastWrittenSnapshot = lastWrittenSnapshot,
    )

fun PartitionListing.toDto() =
    PartitionListingDto(
        sampledAt = sampledAt,
        sampleStarted = sampleStarted,
        sampledSnapshotId = sampledSnapshotId,
        spec = spec?.toDto(),
        total = total,
        staleSpecGroups = staleSpecGroups,
        partitions = partitions.map { it.toDto() },
    )
