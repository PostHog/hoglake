package com.posthog.hoglake.hoglite

import com.posthog.hoglake.App
import com.posthog.hoglake.DatabaseBackend
import com.posthog.hoglake.HealthProbe
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.sql.SQLException
import java.time.Duration
import java.util.UUID

/** These public wiring checks also run unchanged against the pre-port tree. */
@Tag("integration")
class CatalogMetricsLoopIntegrationTest {
    @Test
    fun `hoglite background loop actually collects catalog metrics`() = checkLoop(DatabaseBackend.HOGLITE)

    @Test
    fun `mixed background loop collects hoglite metrics without PostgreSQL availability`() =
        checkLoop(
            DatabaseBackend.MIXED,
        )

    private fun checkLoop(mode: DatabaseBackend) {
        HogliteConformanceEngine().use { engine ->
            PgTestSupport.freshDatabase().use { db ->
                val store = HogliteCatalogStore(engine.client, tenantPrefix = "loop-${UUID.randomUUID()}-")
                store.createCatalog("loop", "s3://hoglake-it/loop")
                val cfg =
                    engine.config().copy(
                        database = mode, hogliteUrl = engine.url(), metricsIntervalMs = 20,
                        maintenanceSummaryIntervalMs = 0, hydratorIntervalMs = 0, expiryIntervalMs = 0,
                        cleanupIntervalMs = 0, compactionIntervalMs = 0, retirementIntervalMs = 0,
                        reindexIntervalMs = 0,
                    )
                val probe = HealthProbe.forProbe(1_000) { true }
                val app =
                    if (mode == DatabaseBackend.MIXED) {
                        App.buildMixed(
                            cfg,
                            db.jdbi,
                            store,
                            probe,
                            ensurePostgres = { throw SQLException("fixture outage") },
                        )
                    } else {
                        App.buildHoglite(cfg, store, probe)
                    }
                app.use {
                    app.startBackground().use {
                        await().atMost(Duration.ofSeconds(3)).untilAsserted {
                            assertThat(
                                app.meterRegistry.find("hoglake_head_snapshot_id").tag("catalog", "loop").gauge(),
                            )
                                .isNotNull()
                        }
                        assertThat(
                            app.meterRegistry.find("hoglake_metrics_last_sample_epoch").gauge()!!.value(),
                        ).isPositive()
                        assertThat(
                            app.meterRegistry.find("hoglake_metrics_sample_errors_total").counter()!!.count(),
                        ).isZero()
                    }
                }
                Metrics.clear()
            }
        }
    }
}
