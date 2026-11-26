package com.posthog.hoglake.hoglite

import java.util.UUID

/** One configured engine endpoint. Routing claims carry only [id], never these URLs. */
data class HogliteBackend(
    val id: UUID,
    val url: String,
    val whaleUrl: String = "",
    val ownerUrlPattern: String = "",
    val connectTimeoutMs: Long = 2_000,
    val readTimeoutMs: Long = 5_000,
    val commitTimeoutMs: Long = 10_000,
    val retryBudgetMs: Long = 8_000,
    val circuitFailureThreshold: Int = 10,
    val circuitOpenMs: Long = 5_000,
    /**
     * The most request threads this backend may hold at once
     * (`max_concurrent_calls`; a request holds one for each operation,
     * however many calls that makes, and one request more may wait for
     * a permit): the bulkhead between it and the other backends sharing
     * the dispatcher (#679). `Config` derives the
     * default from `HOGLAKE_REQUEST_THREADS`; see `HogliteClient`'s
     * "Bulkhead" KDoc for what it does and does not bound.
     */
    val maxConcurrentCalls: Int = DEFAULT_MAX_CONCURRENT_CALLS,
) {
    companion object {
        /** Half the default `HOGLAKE_REQUEST_THREADS`, for a backend built outside `Config`. */
        const val DEFAULT_MAX_CONCURRENT_CALLS = 5
    }
}
