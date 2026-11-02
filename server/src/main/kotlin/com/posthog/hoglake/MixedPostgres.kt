package com.posthog.hoglake

import com.posthog.hoglake.model.HoglakeException
import javax.sql.DataSource

/** PostgreSQL initialization cannot block the healthy hoglite routes in mixed mode. */
internal class MixedPostgres(private val dataSource: DataSource) {
    @Volatile
    private var initialized = false

    @Synchronized
    fun ensureReady() {
        if (initialized) return
        try {
            Database.migrate(dataSource)
            initialized = true
        } catch (e: Exception) {
            throw HoglakeException.BackendUnavailable("configured postgres connection is not ready")
                .apply { initCause(e) }
        }
    }
}
