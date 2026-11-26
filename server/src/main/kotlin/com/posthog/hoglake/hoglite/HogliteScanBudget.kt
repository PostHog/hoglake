package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.HoglakeException

/** The scan wire contract is one list: refuse oversized plans, never truncate it. */
internal class HogliteScanBudget {
    private var files = 0
    private var bytes = 0L
    private var metadata = 0L

    fun add(
        rowBytes: Long,
        metadataBytes: Long = 0,
    ) {
        chargeMetadata(metadataBytes)
        files++
        bytes += rowBytes
        if (files > HogliteCatalogStore.SCAN_MAX_FILES) {
            throw HoglakeException.Validation("scan plan exceeds maximum ${HogliteCatalogStore.SCAN_MAX_FILES} files")
        }
        checkBytes()
    }

    /**
     * Charge more bytes to a file already counted: a payload read alone
     * that came back longer than its page measured it, as a concurrent
     * type promotion rewriting `files.stats_json` in place can make it.
     */
    fun grow(
        rowBytes: Long,
        metadataBytes: Long,
    ) {
        chargeMetadata(metadataBytes)
        bytes += rowBytes
        checkBytes()
    }

    private fun chargeMetadata(metadataBytes: Long) {
        metadata += metadataBytes
        if (metadata > HogliteCatalogStore.SCAN_MAX_METADATA_BYTES) {
            throw HoglakeException.Validation(
                "scan plan requested metadata exceeds maximum ${HogliteCatalogStore.SCAN_MAX_METADATA_BYTES} bytes",
            )
        }
    }

    private fun checkBytes() {
        if (bytes > HogliteCatalogStore.SCAN_MAX_BYTES) {
            throw HoglakeException.Validation("scan plan exceeds maximum ${HogliteCatalogStore.SCAN_MAX_BYTES} bytes")
        }
    }
}
