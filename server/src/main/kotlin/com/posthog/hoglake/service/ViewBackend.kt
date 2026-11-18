package com.posthog.hoglake.service

import com.posthog.hoglake.model.CommitResult
import com.posthog.hoglake.model.ViewInfo

/**
 * The views seam. The Postgres implementation is [ViewService]; the
 * hoglite backend's is [com.posthog.hoglake.hoglite.HogliteViews].
 */
interface ViewBackend {
    fun create(
        catalog: String,
        namespace: String,
        name: String,
        sql: String,
        dialect: String = "trino",
        author: String? = null,
        message: String? = null,
    ): ViewInfo

    fun get(
        catalog: String,
        namespace: String,
        name: String,
    ): ViewInfo

    fun list(
        catalog: String,
        namespace: String,
    ): List<ViewInfo>

    fun drop(
        catalog: String,
        namespace: String,
        name: String,
        author: String? = null,
        message: String? = null,
    ): CommitResult
}
