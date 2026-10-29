package com.posthog.hoglake.model

import java.util.UUID

class PartitionLookupInvalidRequest(detail: String) : RuntimeException(detail)

class PartitionLookupUnavailable(val reason: String, detail: String) : RuntimeException(detail)

internal fun validatePartitionLookupValues(values: List<String?>) {
    if (values.isEmpty() || values.size > 64) {
        throw PartitionLookupInvalidRequest(
            "values must contain 1..64 strings or nulls",
        )
    }
    for (value in values) {
        if (value == null) continue
        if (value.length > 8192) throw PartitionLookupInvalidRequest("partition values are limited to 8192 characters")
        var i = 0
        while (i < value.length) {
            val c = value[i]
            if (c == '\u0000' || c.isLowSurrogate() ||
                (c.isHighSurrogate() && (i + 1 == value.length || !value[i + 1].isLowSurrogate()))
            ) {
                throw PartitionLookupInvalidRequest("partition values must be valid Unicode without NUL")
            }
            i += if (c.isHighSurrogate()) 2 else 1
        }
    }
}

data class PartitionLookup(
    val snapshotId: Long,
    val tableUuid: UUID,
    val specId: Long,
    val values: List<String?>,
    val aggregates: PartitionLookupAggregates,
    val files: List<DataFile>,
    val nextCursor: String?,
)

data class PartitionLookupAggregates(
    val status: String,
    val reason: String?,
    val totals: PartitionLookupTotals?,
)

data class PartitionLookupTotals(
    val fileCount: Long,
    val physicalRows: Long,
    val totalBytes: Long,
    val dvCount: Long,
    val statsState: PartitionLookupStatsState,
    val lastFileWriteSnapshot: Long?,
)

data class PartitionLookupStatsState(val provided: Long, val pending: Long, val failed: Long)
