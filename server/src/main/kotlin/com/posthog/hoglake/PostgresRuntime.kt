package com.posthog.hoglake

import com.posthog.hoglake.model.HoglakeException
import com.zaxxer.hikari.HikariDataSource
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jdbi.v3.core.Jdbi

/** One pool and migration per process. Failed attempts close their pool and can be retried. */
internal class PostgresRuntime(
    private val cfg: Config,
    private val open: () -> HikariDataSource = { Database.dataSource(cfg) },
    private val migrate: (HikariDataSource) -> Unit = Database::migrate,
) : AutoCloseable {
    private val log = KotlinLogging.logger {}
    private var dataSource: HikariDataSource? = null
    private var closed = false

    @Volatile
    private var initialized: Jdbi? = null

    /** Installed before a lazy request can arrive. Called under the initialization lock. */
    var onInitialized: (HikariDataSource) -> Unit = {}

    val isInitialized: Boolean get() = initialized != null

    @Synchronized
    fun get(): Jdbi {
        check(!closed) { "PostgreSQL runtime is closed" }
        initialized?.let { return it }
        var pool: HikariDataSource? = null
        val jdbi =
            try {
                pool = open()
                migrate(pool)
                Database.jdbi(pool)
            } catch (e: Exception) {
                pool?.close()
                if (cfg.defaultBackend == "postgres") throw e
                log.error(e) { "lazy PostgreSQL initialization failed" }
                throw HoglakeException.BackendUnavailable("PostgreSQL backend is unavailable")
            }
        dataSource = pool
        initialized = jdbi
        // A hook failure must not discard a migrated pool that services can already use.
        // Hooks may register gauges and launch loop coroutines, but must not resolve services here.
        onInitialized(checkNotNull(pool))
        return jdbi
    }

    @Synchronized
    fun notifyInitialized() {
        dataSource?.let(onInitialized)
    }

    @Synchronized
    override fun close() {
        closed = true
        dataSource?.close()
        dataSource = null
    }
}
