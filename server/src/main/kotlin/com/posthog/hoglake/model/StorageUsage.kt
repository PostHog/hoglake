package com.posthog.hoglake.model

/** Exact catalog-tracked storage, not snapshot-visible data or an object-store inventory. */
data class StorageUsage(val trackedBytes: Long, val pendingDeleteFiles: Long)
