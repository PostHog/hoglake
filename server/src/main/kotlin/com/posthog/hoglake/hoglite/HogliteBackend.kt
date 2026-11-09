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
)
