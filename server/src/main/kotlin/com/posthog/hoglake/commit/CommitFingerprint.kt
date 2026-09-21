package com.posthog.hoglake.commit

import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.wireObjectMapper

/** Canonical full payload, not a lossy subset or hash. Partition value order is significant. */
internal fun commitFingerprint(request: CommitRequest): String {
    val mapper = wireObjectMapper()
    val appends =
        request.appends.map { append ->
            append.copy(
                files =
                    append.files.map { file ->
                        val canonical = file.copy(columnStats = file.columnStats?.sortedBy { it.fieldId })
                        mapper.writeValueAsString(canonical) to canonical
                    }.sortedBy { it.first }.map { it.second },
            ).let { mapper.writeValueAsString(it) to it }
        }.sortedBy { it.first }.map { it.second }
    return mapper.writeValueAsString(request.copy(appends = appends))
}
