package com.posthog.hoglake.compaction

/** Claim cleanup is best effort. Cancellation stops the sweep before another read. */
internal fun bestEffortCompactionPurge(
    purge: () -> Unit,
    terminalFailure: (Throwable) -> Boolean = { false },
    onFailure: (Throwable) -> Unit,
) {
    try {
        purge()
    } catch (e: Throwable) {
        if (e is InterruptedException || (Thread.currentThread().isInterrupted && terminalFailure(e))) {
            Thread.currentThread().interrupt()
            throw e
        }
        onFailure(e)
    }
}
