package com.posthog.hoglake.service

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.observability.VerifyGauges
import com.posthog.hoglake.testing.PgTestSupport
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList

/**
 * The verify SWEEP: `runOnceAllCatalogs`, driven directly rather than
 * through the scheduler (AGENT.md — tests drive the services' runOnce
 * entry points, not BackgroundLoops, so a sequence can be asserted
 * instead of waited for).
 *
 * What it pins:
 *  - every catalog gets a report, recorded with trigger 'loop';
 *  - `hoglake_verify_violations{catalog, check}` carries the true count
 *    for a failing catalog and an explicit 0 for a passing one, is
 *    published by the SWEEP and never by a manual run, and RETIRES the
 *    rows of a catalog the sweep no longer covers (MultiGauge overwrite);
 *  - a STANDING violation warns once, warns again when the failing-check
 *    set changes, and warns again after it clears and returns;
 *  - one catalog throwing does not stop the others.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VerifyLoopIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)

    @AfterAll
    fun tearDown() = db.close()

    private fun catalogId(name: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = ?")
                .bind(0, name).mapTo(Long::class.java).one()
        }

    /** Catalog with ns.t and one live data file; nothing broken yet. */
    private fun seed(catalog: String): Long {
        catalogs.createCatalog(catalog, "s3://vfy/$catalog")
        catalogs.createNamespace(catalog, "ns")
        catalogs.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        commits.commit(
            catalog,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "t",
                            listOf(FileRegistration("s3://vfy/$catalog/f0.parquet", 4, 32)),
                        ),
                    ),
            ),
        )
        return catalogId(catalog)
    }

    /** Invariant 4's alert condition: a queued path a live file row claims. */
    private fun breakRemovalQueue(catalog: String) =
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
                VALUES (:c, 's3://vfy/$catalog/f0.parquet', 'data', 'snapshot_expiry')
                """,
            ).bind("c", catalogId(catalog)).execute()
        }

    private fun clearRemovalQueue(catalog: String) =
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate("DELETE FROM hog_file_removal WHERE catalog_id = :c")
                .bind("c", catalogId(catalog)).execute()
        }

    /** Invariant 1: a hole in the dense snapshot range, a DIFFERENT check. */
    private fun breakSnapshotDensity(catalog: String) =
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate("DELETE FROM hog_snapshot WHERE catalog_id = :c AND snapshot_id = 1")
                .bind("c", catalogId(catalog)).execute()
        }

    private class Capture : AutoCloseable {
        val events = CopyOnWriteArrayList<ILoggingEvent>()
        private val appender =
            object : AppenderBase<ILoggingEvent>() {
                override fun append(event: ILoggingEvent) {
                    events += event
                }
            }
        private val logger = LoggerFactory.getLogger(VerifyService::class.java) as Logger

        init {
            appender.context = LoggerFactory.getILoggerFactory() as LoggerContext
            appender.start()
            logger.addAppender(appender)
        }

        /**
         * WARN-level events emitted since the last call, oldest first.
         *
         * Filtered by LEVEL, not by message text: a test that greps for
         * a phrase passes when the line is downgraded to debug, which
         * is the change that would actually lose the signal.
         */
        fun drainWarnings(): List<String> {
            val lines = events.filter { it.level == Level.WARN }.map { it.formattedMessage }
            events.clear()
            return lines
        }

        override fun close() {
            logger.detachAppender(appender)
            appender.stop()
        }
    }

    @Test
    fun `the sweep reports every catalog, publishes the gauge, and warns once per failing-check set`() {
        seed("loop-broken")
        seed("loop-healthy")
        breakRemovalQueue("loop-broken")
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        Metrics.bind(registry)
        VerifyGauges.clear()
        // ONE service: the warn-once memory is per instance, exactly as
        // App.startBackground wires it.
        val verify = VerifyService(db.jdbi)

        // The list seam, like the other two cases: this class shares one
        // database, so sweeping EVERY catalog would drag other cases'
        // fixtures in and make the gauge row set a moving target.
        val swept = listOf("loop-broken", "loop-healthy")
        try {
            Capture().use { capture ->
                // -- sweep 1: broken catalog fails, healthy one passes.
                val first = verify.runOnceAllCatalogs(swept)
                assertThat(first.map { it.first }).isEqualTo(swept)
                assertThat(first.single { it.first == "loop-broken" }.second.status).isEqualTo("fail")
                assertThat(first.single { it.first == "loop-healthy" }.second.status).isEqualTo("pass")
                assertThat(loopRuns("loop-broken")).isEqualTo(1)
                assertThat(loopRuns("loop-healthy")).isEqualTo(1)
                val warned = capture.drainWarnings()
                assertThat(warned).hasSize(1)
                assertThat(warned.single())
                    .contains("catalog 'loop-broken'")
                    .contains("removal_queue=1")
                // The true count, and an explicit zero everywhere else — an
                // alert keys on > 0, so a pass must be a published zero.
                assertThat(gauge(registry, "loop-broken", "removal_queue")).isEqualTo(1.0)
                assertThat(gauge(registry, "loop-broken", "row_id_tiling")).isEqualTo(0.0)
                assertThat(gauge(registry, "loop-healthy", "removal_queue")).isEqualTo(0.0)

                // -- sweep 2: nothing changed. The violation is a STATE, so
                //    it must not produce a second line.
                verify.runOnceAllCatalogs(swept)
                assertThat(capture.drainWarnings())
                    .describedAs("an unchanged failing-check set logs once, not once per sweep")
                    .isEmpty()

                // -- sweep 3: a SECOND check starts failing on the same
                //    catalog. The set changed, so the operator hears about it.
                breakSnapshotDensity("loop-broken")
                verify.runOnceAllCatalogs(swept)
                assertThat(capture.drainWarnings().single())
                    .contains("removal_queue=1")
                    .contains("snapshot_density=")

                // -- sweep 4: cleared. No warning, and the memory is dropped.
                clearRemovalQueue("loop-broken")
                restoreSnapshotDensity("loop-broken")
                verify.runOnceAllCatalogs(swept)
                assertThat(capture.drainWarnings()).isEmpty()
                assertThat(gauge(registry, "loop-broken", "removal_queue")).isEqualTo(0.0)

                // -- sweep 5: the failure returns with the SAME check set as
                //    sweep 3. Identical to what the memory last held, so the
                //    line only appears if sweep 4 really dropped it — a
                //    recurrence an operator must see, not one deduplicated
                //    against history. (A recurrence with a DIFFERENT set
                //    would warn either way and would prove nothing.)
                breakRemovalQueue("loop-broken")
                breakSnapshotDensity("loop-broken")
                verify.runOnceAllCatalogs(swept)
                assertThat(capture.drainWarnings())
                    .describedAs("a failure that cleared and came back must warn again")
                    .hasSize(1)
            }
        } finally {
            Metrics.clear()
            VerifyGauges.clear()
        }
    }

    @Test
    fun `a catalog the sweep cannot read is skipped, the rest still run, and its series retire`() {
        seed("loop-isolated-a")
        seed("loop-isolated-b")
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        Metrics.bind(registry)
        VerifyGauges.clear()
        try {
            val verify = VerifyService(db.jdbi)
            verify.runOnceAllCatalogs(listOf("loop-isolated-a", "loop-isolated-b"))
            assertThat(gauge(registry, "loop-isolated-a", "orphans")).isEqualTo(0.0)
            assertThat(gauge(registry, "loop-isolated-b", "orphans")).isEqualTo(0.0)

            // A catalog whose row is gone by the time its own transaction
            // resolves it: CatalogRepo.require throws inside the
            // per-catalog try. That is the ONLY way a scan fails, and it
            // is a race no test can schedule — hence the list seam, which
            // reaches the same path deterministically. The assertion is
            // direct: the catalog AFTER the failing one was still swept.
            val results = verify.runOnceAllCatalogs(listOf("loop-gone", "loop-isolated-b"))
            assertThat(results.map { it.first })
                .describedAs("one catalog throwing must not stop the sweep")
                .containsExactly("loop-isolated-b")
            assertThat(loopRuns("loop-isolated-b")).isEqualTo(2)
            assertThat(loopRuns("loop-isolated-a")).isEqualTo(1)

            // The catalogs this sweep did NOT cover lose their series,
            // they do not freeze at their last value: that is what
            // MultiGauge's whole-row-set refresh buys, and a per-key
            // gauge map could not do it. A catalog nobody is verifying
            // must not keep publishing a reassuring zero.
            assertThat(
                registry.find("hoglake_verify_violations").tag("catalog", "loop-isolated-a").gauges(),
            ).isEmpty()
            assertThat(gauge(registry, "loop-isolated-b", "orphans")).isEqualTo(0.0)

            // The catalog the sweep could not read is COUNTED. Without
            // this it would be invisible twice over: the per-catalog
            // catch keeps hoglake_background_loop_failures_total silent
            // (the iteration succeeded), and the gauge cannot carry it
            // either, because a sweep with no answer must not publish a
            // reassuring zero.
            assertThat(
                registry.get("hoglake_verify_errors_total").tag("catalog", "loop-gone").counter().count(),
            ).isEqualTo(1.0)
        } finally {
            Metrics.clear()
            VerifyGauges.clear()
        }
    }

    @Test
    fun `a manual run records a ledger row but publishes no gauge`() {
        seed("loop-manual")
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        Metrics.bind(registry)
        VerifyGauges.clear()
        try {
            VerifyService(db.jdbi).runOnce("loop-manual")

            // The trigger works on every replica, including the ones
            // running HOGLAKE_VERIFY_INTERVAL_MS=0. A series minted
            // there is never refreshed again: a stale zero that says
            // "healthy" about a catalog nobody checks, or a stale
            // nonzero that pages forever.
            assertThat(registry.find("hoglake_verify_violations").gauges())
                .describedAs("a manual run must not mint an alerting series")
                .isEmpty()
            assertThat(manualRuns("loop-manual")).isEqualTo(1)
        } finally {
            Metrics.clear()
            VerifyGauges.clear()
        }
    }

    @Test
    fun `startBackground registers the verify loop only when the interval is positive`() {
        // The default is 0 — OFF — and the chart turns it on for the
        // maintenance workload alone. A default that silently ran an
        // aggregate pass over every catalog on every API replica is the
        // thing this pins, and the `<= 0 = disabled` contract is the
        // only thing standing between the two.
        seed("loop-wiring")
        val off = Config(hydratorIntervalMs = 0, metricsIntervalMs = 0, maintenanceSummaryIntervalMs = 0)
        assertThat(off.verifyIntervalMs).describedAs("the default is OFF").isZero()
        App.build(off, db.jdbi).startBackground().use {
            Thread.sleep(300)
        }
        assertThat(loopRuns("loop-wiring"))
            .describedAs("a disabled interval must register nothing at all")
            .isZero()

        val on =
            Config(
                hydratorIntervalMs = 0,
                metricsIntervalMs = 0,
                maintenanceSummaryIntervalMs = 0,
                verifyIntervalMs = 50,
            )
        App.build(on, db.jdbi).startBackground().use {
            await().atMost(Duration.ofSeconds(20)).untilAsserted {
                assertThat(loopRuns("loop-wiring")).isGreaterThanOrEqualTo(1)
            }
        }
    }

    private fun restoreSnapshotDensity(catalog: String) =
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_snapshot (catalog_id, snapshot_id, schema_version)
                VALUES (:c, 1, 0) ON CONFLICT DO NOTHING
                """,
            ).bind("c", catalogId(catalog)).execute()
        }

    private fun runs(
        catalog: String,
        trigger: String,
    ): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT count(*) FROM hog_maintenance_run r
                JOIN hog_catalog c ON c.catalog_id = r.catalog_id
                WHERE c.name = :n AND r.task = :task AND r.run_trigger = :trigger
                """,
            ).bind("n", catalog).bind("task", MaintenanceTask.VERIFY.wire).bind("trigger", trigger)
                .mapTo(Long::class.java).one()
        }

    private fun loopRuns(catalog: String): Long = runs(catalog, "loop")

    private fun manualRuns(catalog: String): Long = runs(catalog, "manual")

    private fun gauge(
        registry: PrometheusMeterRegistry,
        catalog: String,
        check: String,
    ): Double =
        registry.get("hoglake_verify_violations")
            .tag("catalog", catalog)
            .tag("check", check)
            .gauge()
            .value()
}
