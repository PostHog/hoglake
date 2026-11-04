package com.posthog.hoglake

import com.posthog.hoglake.model.HoglakeException
import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicBoolean
import javax.sql.DataSource

/** A single cold PostgreSQL initializer; other callers refuse without waiting for it. */
internal class MixedPostgres(private val dataSource: DataSource) {
    @Volatile
    private var initialized = false
    private val initializing = AtomicBoolean()

    fun ensureReady() {
        if (initialized) return
        if (!initializing.compareAndSet(false, true)) {
            if (initialized) return
            throw HoglakeException.BackendUnavailable("configured postgres connection is not ready")
        }
        try {
            if (initialized) return
            Database.migrate(dataSource)
            initialized = true
        } catch (e: CancellationException) {
            throw e
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw e
        } catch (e: Exception) {
            throw HoglakeException.BackendUnavailable("configured postgres connection is not ready")
                .apply { initCause(e) }
        } finally {
            initializing.set(false)
        }
    }
}
