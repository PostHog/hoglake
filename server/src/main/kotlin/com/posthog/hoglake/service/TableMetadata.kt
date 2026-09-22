package com.posthog.hoglake.service

import com.posthog.hoglake.model.HoglakeException

/** User metadata has no effect on storage or writer behavior. */
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
    }
}
