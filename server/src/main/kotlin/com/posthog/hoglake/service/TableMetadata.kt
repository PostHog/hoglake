package com.posthog.hoglake.service

import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.FileFormats
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.SortFieldDef

/** Table metadata validation, including the storage-format property that affects writer behavior. */
internal object TableMetadata {
    fun validateComment(comment: String?) {
        if (comment != null && (comment.length > 16384 || '\u0000' in comment)) {
            throw HoglakeException.Validation("comment must contain at most 16384 characters and no NUL")
        }
    }

    fun validateProperties(properties: Map<String, String>) {
        if (properties.size > 100) throw HoglakeException.Validation("at most 100 custom properties are allowed")
        for ((key, value) in (properties as Map<*, *>)) {
            if (key !is String || value !is String) {
                throw HoglakeException.Validation(
                    "custom properties require string keys and values",
                )
            }
            if (!key.matches(Regex("[a-z][a-z0-9_.-]{0,127}")) ||
                key.startsWith("hoglake.") || key.startsWith("trino.") ||
                key in setOf("partitioning", "sorted_by", "location", "format", "comment")
            ) {
                throw HoglakeException.Validation("invalid or reserved custom property key '$key'")
            }
            if (value.length > 4096 || '\u0000' in value) {
                throw HoglakeException.Validation(
                    "custom property values must contain at most 4096 characters and no NUL",
                )
            }
        }
        val format = FileFormats.tableFormat(properties)
        if (format !in FileFormats.allowed) {
            throw HoglakeException.Validation(
                "property '${FileFormats.TABLE_PROPERTY}' must be one of ${FileFormats.allowed.sorted()}",
            )
        }
    }

    fun validateDefinitionForFormat(
        properties: Map<String, String>,
        columns: List<ColumnDef>,
        partitionFields: List<PartitionFieldDef>,
        sortFields: List<SortFieldDef>,
    ) {
        if (!FileFormats.isPacked(properties)) return
        if (partitionFields.isNotEmpty()) {
            throw HoglakeException.Validation("packed MergeTree tables do not support partition specs")
        }
        if (sortFields.isNotEmpty()) {
            throw HoglakeException.Validation("packed MergeTree tables do not support sort orders")
        }

        fun nodes(defs: List<ColumnDef>): Sequence<ColumnDef> =
            defs.asSequence().flatMap { def -> sequenceOf(def) + nodes(def.children.orEmpty()) }
        val unsupported = nodes(columns).firstOrNull { it.type !in FileFormats.packedColumnTypes }
        if (unsupported != null) {
            throw HoglakeException.Validation(
                "packed MergeTree tables do not support column '${unsupported.name}' " +
                    "of type '${unsupported.type.wire}'; supported types are " +
                    FileFormats.packedColumnTypes.map { it.wire }.sorted().joinToString(", "),
            )
        }
    }

    fun requireFormatUnchanged(
        before: Map<String, String>,
        after: Map<String, String>,
    ) {
        val old = FileFormats.tableFormat(before)
        val new = FileFormats.tableFormat(after)
        if (old != new) {
            throw HoglakeException.Validation(
                "property '${FileFormats.TABLE_PROPERTY}' is immutable (current '$old', requested '$new')",
            )
        }
    }
}
