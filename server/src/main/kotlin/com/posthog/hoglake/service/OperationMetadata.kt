package com.posthog.hoglake.service

import com.posthog.hoglake.model.HoglakeException

internal fun requireOperationMetadata(
    author: String?,
    message: String?,
) {
    if (author.isBlankMetadata()) throw HoglakeException.Validation("author is required and must not be blank")
    if (message.isBlankMetadata()) throw HoglakeException.Validation("message is required and must not be blank")
}

// Include NEXT LINE so the wire and Python's whitespace definition agree.
private fun String?.isBlankMetadata(): Boolean = this == null || all { it.isWhitespace() || it == '\u0085' }
