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
