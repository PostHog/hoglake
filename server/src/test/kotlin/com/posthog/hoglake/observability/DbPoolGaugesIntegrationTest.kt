package com.posthog.hoglake.observability

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.Database
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.sql.Connection
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * `hoglake_db_pool_*` (#218): the saturation the commit convoy passes
 * through on its way to an incident, as numbers.
 *
 * Until #218 the request pool filling had no series at all — the only
 * evidence was the 5 s `connectionTimeout` 500s that arrive after it is
 * already too late, and a `/healthz` that had started failing for a
 * reason nobody could distinguish from a dead database.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DbPoolGaugesIntegrationTest {
    private val db = PgTestSupport.freshDatabase()

    /**
     * The smallest pool `Config`'s own boot check allows: it refuses
     * anything under `compactionParallelGroups +
     * FOREGROUND_CONNECTION_RESERVE`, so "every connection is out" is
     * five connections rather than an arbitrary small number.
     */
    private val poolSize = Config().compactionParallelGroups + Config.FOREGROUND_CONNECTION_RESERVE

    private val cfg =
        Config(
            hydratorIntervalMs = 0,
            metricsIntervalMs = 0,
            maintenanceSummaryIntervalMs = 0,
            jdbcUrl = db.jdbcUrl,
            dbUser = PgTestSupport.USER,
            dbPassword = PgTestSupport.PASSWORD,
            dbPoolSize = poolSize,
        )

    // The production pool builder, not a copy of its settings: the
    // 5 s connectionTimeout the `pending` assertion races against is
    // Database.dataSource's, and a local HikariConfig would assert
    // against a number production does not use.
    private val ds = Database.dataSource(cfg)
    private val app = App.build(cfg, Database.jdbi(ds), dataSource = ds)
    private val borrowers = Executors.newCachedThreadPool()

    @AfterAll
    fun tearDown() {
        borrowers.shutdownNow()
        app.close()
        ds.close()
        db.close()
    }

    private fun gauge(name: String): Double = app.meterRegistry.get(name).gauge().value()

    /**
     * MUTATION: register `idleConnections` where `activeConnections` is
     * meant (the two are complements at a fixed pool size, so a test
     * that only asserted one of them would pass) — the active/idle pair
     * reds. MUTATION: drop the `threadsAwaitingConnection` gauge, or
     * read `totalConnections` for it — the pending assertion reds, and
     * pending is the one worth alerting on. MUTATION: remove the
     * `if (dataSource != null)` registration from `App.build` — every
     * lookup here reds with a missing meter.
     */
    @Test
    fun `active idle pending and max track the request pool`() {
        assertThat(gauge("hoglake_db_pool_max")).isEqualTo(poolSize.toDouble())

        val held = mutableListOf<Connection>()
        try {
            held += ds.connection
            assertThat(gauge("hoglake_db_pool_active")).isEqualTo(1.0)
            assertThat(gauge("hoglake_db_pool_pending")).isEqualTo(0.0)

            while (held.size < poolSize) held += ds.connection
            assertThat(gauge("hoglake_db_pool_active")).isEqualTo(poolSize.toDouble())
            assertThat(gauge("hoglake_db_pool_idle")).isEqualTo(0.0)
            assertThat(gauge("hoglake_db_pool_pending")).isEqualTo(0.0)

            // One more borrower than the pool can serve: it waits inside
            // HikariPool.getConnection, which is precisely the state that
            // has 5 s to resolve before it becomes a 500.
            val queued = borrowers.submit { ds.connection.use { it.isValid(1) } }
            await().atMost(Duration.ofSeconds(3)).until { gauge("hoglake_db_pool_pending") >= 1.0 }

            held.removeLast().close()
            queued.get(10, TimeUnit.SECONDS)
            await().atMost(Duration.ofSeconds(3)).until { gauge("hoglake_db_pool_pending") == 0.0 }
        } finally {
            held.forEach { runCatching { it.close() } }
        }

        await().atMost(Duration.ofSeconds(3)).until { gauge("hoglake_db_pool_active") == 0.0 }
        assertThat(gauge("hoglake_db_pool_idle")).isEqualTo(poolSize.toDouble())
    }
}
