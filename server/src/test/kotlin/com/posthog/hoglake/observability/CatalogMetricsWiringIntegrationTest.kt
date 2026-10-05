package com.posthog.hoglake.observability

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.testing.publishMaintenanceSample
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * `App`'s wiring of the extended catalog-metrics groups: they run only
 * where `HOGLAKE_MAINTENANCE_SUMMARY_INTERVAL_MS > 0` (the maintenance
 * Deployment), so an API replica's `/metrics` carries the core gauges and
 * none of the instance-wide extended families that every replica would
 * otherwise publish again.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CatalogMetricsWiringIntegrationTest {
    private val db = PgTestSupport.freshDatabase()

    @AfterAll
    fun tearDown() = db.close()

    @BeforeAll
    fun seed() {
        val catalogs = CatalogService(db.jdbi)
        catalogs.createCatalog("wiring", "s3://wiring")
        catalogs.createNamespace("wiring", "ns")
        catalogs.createTable("wiring", "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        CommitService(db.jdbi).commit(
            "wiring",
            CommitRequest(
                appends =
                    listOf(
                        TableAppend("ns", "t", listOf(FileRegistration("s3://wiring/data/a.parquet", 10, 100))),
                    ),
            ),
        )
        publishMaintenanceSample(db.jdbi)
    }

    private fun scrape(maintenanceSummaryIntervalMs: Long): String {
        val app =
            App.build(
                Config(
                    hydratorIntervalMs = 0,
                    metricsIntervalMs = 0,
                    maintenanceSummaryIntervalMs = maintenanceSummaryIntervalMs,
                ),
                db.jdbi,
            )
        app.catalogMetrics.sampleOnce()
        var body = ""
        testApplication {
            application { app.module(this) }
            body = client.get("/metrics").bodyAsText()
        }
        return body
    }

    private fun families(scrape: String): Set<String> =
        scrape.lineSequence()
            .filter { it.isNotBlank() && !it.startsWith("#") }
            .map { it.substringBefore('{').substringBefore(' ') }
            .toSet()

    @Test
    fun `an API replica (no summary loop) publishes only the core group`() {
        val names = families(scrape(maintenanceSummaryIntervalMs = 0))
        // MUTATION: wire extendedGroups = true unconditionally in App and
        // these appear.
        assertThat(names.filter { it.startsWith("hoglake_table_") && it != "hoglake_table_count" }).isEmpty()
        assertThat(names.filter { it.startsWith("hoglake_relation_") }).isEmpty()
        assertThat(names).doesNotContain("hoglake_maintenance_last_run_duration_seconds")
        // The core group is there.
        assertThat(names).contains("hoglake_live_files", "hoglake_table_count", "hoglake_metrics_last_sample_epoch")
    }

    @Test
    fun `the maintenance pod (summary loop on) publishes the extended groups too`() {
        val names = families(scrape(maintenanceSummaryIntervalMs = 1_000))
        // MUTATION: wire extendedGroups = false and these disappear.
        assertThat(names).contains(
            "hoglake_table_files",
            "hoglake_table_series_truncated",
            "hoglake_relation_bytes",
            "hoglake_live_delete_files",
        )
        assertThat(names).contains("hoglake_live_files")
    }
}
