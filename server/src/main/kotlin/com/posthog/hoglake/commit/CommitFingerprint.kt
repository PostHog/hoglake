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
                        file.copy(columnStats = file.columnStats?.sortedBy { it.fieldId })
                    }.sortedBy { mapper.writeValueAsString(it) },
            )
        }.sortedBy { mapper.writeValueAsString(it) }
    return mapper.writeValueAsString(request.copy(appends = appends))
}
