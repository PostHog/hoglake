package com.posthog.hoglake.api

import com.posthog.hoglake.model.StorageUsage

/** Decimal int64 strings: exact even for clients whose JSON numbers are IEEE-754 doubles. */
data class StorageUsageDto(val trackedBytes: String, val pendingDeleteFiles: String)

fun StorageUsage.toDto(): StorageUsageDto =
    StorageUsageDto(
        trackedBytes = trackedBytes.toString(),
        pendingDeleteFiles = pendingDeleteFiles.toString(),
    )
