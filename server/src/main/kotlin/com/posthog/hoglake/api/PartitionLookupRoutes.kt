package com.posthog.hoglake.api

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.JacksonException
import com.fasterxml.jackson.databind.DeserializationFeature
import com.posthog.hoglake.model.PartitionLookup
import com.posthog.hoglake.model.PartitionLookupInvalidRequest
import com.posthog.hoglake.model.PartitionLookupStatsState
import com.posthog.hoglake.model.validatePartitionLookupValues
import com.posthog.hoglake.service.PartitionLookupService
import com.posthog.hoglake.wireObjectMapper
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import java.util.UUID

fun Application.installPartitionLookupRoutes(lookups: PartitionLookupService) {
    routing {
        get("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/partitions/lookup") {
            if (call.request.queryParameters.contains("at_timestamp")) {
                throw PartitionLookupInvalidRequest("at_timestamp is unsupported; use snapshot")
            }
            val specId =
                call.lookupScalar("spec_id")?.toLongOrNull()
                    ?: throw PartitionLookupInvalidRequest("spec_id is required and must be an int64")
            val values = call.lookupScalar("values") ?: throw PartitionLookupInvalidRequest("values is required")
            val snapshot =
                call.lookupScalar("snapshot")?.let {
                    it.toLongOrNull() ?: throw PartitionLookupInvalidRequest("snapshot must be an int64")
                }
            val limit =
                call.lookupScalar("limit")?.let {
                    it.toIntOrNull() ?: throw PartitionLookupInvalidRequest("limit must be an integer")
                } ?: 100
            call.respond(
                lookups.lookup(
                    call.catalog(),
                    call.namespace(),
                    call.table(),
                    specId,
                    parsePartitionLookupValues(values),
                    snapshot,
                    limit,
                    call.lookupScalar("cursor"),
                ).toDto(),
            )
        }
    }
}

private fun ApplicationCall.lookupScalar(name: String): String? {
    val values = request.queryParameters.getAll(name) ?: return null
    if (values.size != 1) throw PartitionLookupInvalidRequest("query parameter '$name' must appear once")
    return values.single()
}

internal fun parsePartitionLookupValues(raw: String): List<String?> {
    if (raw.length > 65_536) throw PartitionLookupInvalidRequest("values exceeds 65536 characters")
    val node =
        try {
            wireObjectMapper().reader().with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).readTree(raw)
        } catch (_: JacksonException) {
            throw PartitionLookupInvalidRequest("values must be a JSON array of strings or nulls")
        }
    if (node == null || !node.isArray || node.size() !in 1..64) {
        throw PartitionLookupInvalidRequest("values must be a JSON array containing 1..64 strings or nulls")
    }
    return node.map { value ->
        when {
            value.isNull -> null
            value.isTextual && value.textValue().length <= 8192 -> value.textValue()
            else -> throw PartitionLookupInvalidRequest(
                "each partition value must be a string of at most 8192 characters or null",
            )
        }
    }.also { validatePartitionLookupValues(it) }
}

data class PartitionLookupDto(
    val snapshotId: Long,
    val tableUuid: UUID,
    val specId: Long,
    val values: List<String?>,
    val aggregates: PartitionLookupAggregatesDto,
    val files: List<DataFileDto>,
    @get:JsonInclude(JsonInclude.Include.ALWAYS)
    val nextCursor: String?,
)

data class PartitionLookupAggregatesDto(
    val status: String,
    @get:JsonInclude(JsonInclude.Include.ALWAYS)
    val reason: String?,
    @get:JsonInclude(JsonInclude.Include.ALWAYS)
    val totals: PartitionLookupTotalsDto?,
)

data class PartitionLookupTotalsDto(
    val fileCount: Long,
    val physicalRows: Long,
    val totalBytes: Long,
    val dvCount: Long,
    val statsState: PartitionLookupStatsState,
    @get:JsonInclude(JsonInclude.Include.ALWAYS)
    val lastFileWriteSnapshot: Long?,
)

fun PartitionLookup.toDto() =
    PartitionLookupDto(
        snapshotId,
        tableUuid,
        specId,
        values,
        PartitionLookupAggregatesDto(
            aggregates.status,
            aggregates.reason,
            aggregates.totals?.let {
                PartitionLookupTotalsDto(
                    it.fileCount,
                    it.physicalRows,
                    it.totalBytes,
                    it.dvCount,
                    it.statsState,
                    it.lastFileWriteSnapshot,
                )
            },
        ),
        files.map { it.toDto() },
        nextCursor,
    )
