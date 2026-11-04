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
        metadata += metadataBytes
        if (metadata > HogliteCatalogStore.SCAN_MAX_METADATA_BYTES) {
            throw HoglakeException.Validation(
                "scan plan requested metadata exceeds maximum ${HogliteCatalogStore.SCAN_MAX_METADATA_BYTES} bytes",
            )
        }
        files++
        bytes += rowBytes
        if (files > HogliteCatalogStore.SCAN_MAX_FILES) {
            throw HoglakeException.Validation("scan plan exceeds maximum ${HogliteCatalogStore.SCAN_MAX_FILES} files")
        }
        if (bytes > HogliteCatalogStore.SCAN_MAX_BYTES) {
            throw HoglakeException.Validation("scan plan exceeds maximum ${HogliteCatalogStore.SCAN_MAX_BYTES} bytes")
        }
    }
}
