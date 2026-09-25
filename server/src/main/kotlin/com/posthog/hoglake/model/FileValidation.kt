package com.posthog.hoglake.model

/** Validate footer metadata without subtraction overflow on malformed file sizes. */
fun FileRegistration.validateFooterSize(required: Boolean = false) {
    val footer = footerSize
    if (footer == null) {
        if (required) throw HoglakeException.Validation("missing footer_size")
        return
    }
    if (fileSizeBytes < 8 || footer < 0 || footer > fileSizeBytes - 8) {
        throw HoglakeException.Validation("invalid footer_size")
    }
}

/**
 * Validate a footer-shipping registration's optional `split_offsets`
 * against [SplitOffsets.violation]: a list that breaks the contract is
 * refused (422), never stored and never silently dropped — a writer
 * whose footer walk is wrong should hear about it, not find its files
 * cut evenly with no explanation. Absent is always fine.
 */
fun FileRegistration.validateSplitOffsets() {
    val offsets = splitOffsets ?: return
    SplitOffsets.violation(offsets, fileSizeBytes)?.let { reason ->
        throw HoglakeException.Validation("invalid split_offsets for $path: $reason")
    }
}
