package com.posthog.hoglake.hoglite

import com.posthog.hoglake.App
import com.posthog.hoglake.DatabaseBackend
import com.posthog.hoglake.HealthProbe
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.observability.CatalogMetrics
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.testing.publishMaintenanceSample
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.within
import org.awaitility.Awaitility.await
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.sql.SQLException
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/** Shared export contracts run against PostgreSQL plus the workflow-pinned hoglited executable. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CatalogMetricsBackendIntegrationTest {
    private lateinit var engine: HogliteConformanceEngine
    private lateinit var db: PgTestSupport.TestDb
    private lateinit var store: HogliteCatalogStore
    private val serial = AtomicInteger()

    @BeforeAll
    fun start() {
        engine = HogliteConformanceEngine()
        db = PgTestSupport.freshDatabase()
    }

    @BeforeEach
    fun isolate() {
        store = HogliteCatalogStore(engine.client, tenantPrefix = "metric-${UUID.randomUUID()}-")
    }

    @AfterAll
    fun stop() {
        if (::db.isInitialized) db.close()
        if (::engine.isInitialized) engine.close()
        Metrics.clear()
    }

    private fun name() = "metrics${serial.incrementAndGet()}"

    private fun app(postgresAvailable: AtomicBoolean = AtomicBoolean(true)): App =
        App.buildMixed(
            engine.config().copy(
                database = DatabaseBackend.MIXED,
                hogliteUrl = engine.url(),
                maintenanceSummaryIntervalMs = 1_000,
            ),
            db.jdbi,
            store,
            HealthProbe.forProbe(1_000) { true },
            ensurePostgres = { if (!postgresAvailable.get()) throw SQLException("fixture PostgreSQL outage") },
        )

    private fun create(
        app: App,
        backend: String,
    ): String {
        val name = name()
        app.store.createCatalog(name, "s3://hoglake-it/$name", backend = backend)
        app.store.createNamespace(name, "ns")
        app.store.createTable(name, "ns", "events", listOf(ColumnDef("id", ColType.LONG)))
        return name
    }

    private fun gauge(
        registry: MeterRegistry,
        metric: String,
        catalog: String? = null,
        vararg labels: String,
    ): Double? {
        val search = registry.find(metric)
        if (catalog != null) search.tag("catalog", catalog)
        return search.tags(*labels).gauge()?.value()
    }

    private fun write(
        catalog: String,
        vararg statements: HogliteStatement,
    ) = store.catalogOperation(catalog) {
        engine.client.tx(store.resolveTenant(catalog), UUID.randomUUID().toString(), null, statements.toList())
    }

    private fun seedHoglite(catalog: String) {
        write(
            catalog,
            HogliteStatement("UPDATE meta SET next_snapshot = 11, earliest_snapshot_id = 2 WHERE id = 1"),
            HogliteStatement(
                "INSERT INTO snapshots(id,ts,schema_version) VALUES (10,?,0)",
                listOf("2026-01-01T00:00:00.000Z"),
            ),
            HogliteStatement(
                """
                    INSERT INTO
                    files(id,table_id,path,record_count,file_size_bytes,row_id_start,stats_state,missing_field_ids,prefix_quarantined,begin_snapshot)
                    """ +
                    """
                        VALUES
                        (100,1,'s3://fixture/a',7,100,0,'pending',1,1,1),(101,1,'s3://fixture/b',11,2000,7,'failed',0,0,1)
                        """,
            ),
            HogliteStatement(
                """
                    INSERT INTO
                    dvs(data_file_id,table_id,delete_file_id,path,delete_count,file_size_bytes,begin_snapshot,epoch)
                    """ +
                    "VALUES (101,1,102,'s3://fixture/dv',2,55,1,1)",
            ),
            HogliteStatement(
                """
                    INSERT INTO offsets(consumer_id,table_uuid,committed_snapshot,updated_at) SELECT
                    'consumer',table_uuid,3,? FROM tables WHERE id=1
                    """,
                listOf(HogliteSchema.now()),
            ),
            HogliteStatement(
                """
                    INSERT INTO removals(path,file_kind,reason,queued_snapshot,queued_at) VALUES
                    ('s3://fixture/removed','data','expired',1,?)
                    """,
                listOf(HogliteSchema.now()),
            ),
            HogliteStatement(
                """
                    INSERT INTO maintenance_runs(task,run_trigger,started_at,finished_at,status) VALUES
                    ('expiry','loop',?,?,'ok')
                    """,
                listOf("2026-01-01T00:00:00.000Z", "2026-01-01T00:00:02.000Z"),
            ),
        )
    }

    @Test
    fun `hoglite exports the 36 neutral series with observed values and no PostgreSQL relations`() {
        val cfg =
            engine.config().copy(
                database = DatabaseBackend.HOGLITE,
                hogliteUrl = engine.url(),
                maintenanceSummaryIntervalMs = 1_000,
                compactionTargetBytes = 1_000,
            )
        App.buildHoglite(cfg, store, HealthProbe.forProbe(1_000) { true }).use { app ->
            val cat = name()
            app.store.createCatalog(cat, "s3://hoglake-it/$cat")
            app.store.createNamespace(cat, "ns")
            app.store.createTable(cat, "ns", "events", listOf(ColumnDef("id", ColType.LONG)))
            seedHoglite(cat)
            app.catalogMetrics.sampleOnce()
            val expected =
                mapOf(
                    "hoglake_head_snapshot_id" to 10.0, "hoglake_earliest_snapshot_id" to 2.0,
                    "hoglake_snapshot_count" to 9.0, "hoglake_removal_queue_depth" to 1.0,
                    "hoglake_stats_pending_files" to 1.0, "hoglake_stats_pending_head_file_id" to 100.0,
                    "hoglake_stats_failed_files" to 1.0, "hoglake_missing_field_id_files" to 1.0,
                    "hoglake_object_prefix_quarantined_files" to 1.0, "hoglake_live_totals_sampled" to 1.0,
                    "hoglake_live_rows" to 18.0, "hoglake_live_bytes" to 2100.0, "hoglake_live_files" to 2.0,
                    "hoglake_table_count" to 1.0, "hoglake_consumer_lag_snapshots" to 7.0,
                    "hoglake_consumer_lag_max" to 7.0, "hoglake_table_files" to 2.0,
                    "hoglake_table_small_files" to 1.0, "hoglake_table_bytes" to 2100.0,
                    "hoglake_table_small_bytes" to 100.0, "hoglake_table_rows" to 18.0,
                    "hoglake_table_delete_files" to 1.0, "hoglake_table_partitions" to 1.0,
                    "hoglake_table_largest_partition_files" to 2.0, "hoglake_table_series_truncated" to 0.0,
                    "hoglake_live_delete_files" to 1.0, "hoglake_live_delete_bytes" to 55.0,
                    "hoglake_dropped_tables_pending_retirement" to 0.0,
                    "hoglake_maintenance_last_run_epoch" to
                        Instant.parse("2026-01-01T00:00:02Z").epochSecond.toDouble(),
                    "hoglake_maintenance_last_run_duration_seconds" to 2.0,
                )
            expected.forEach {
                    (metric, value) ->
                assertThat(gauge(app.meterRegistry, metric, cat)).describedAs(metric).isEqualTo(value)
            }
            assertThat(gauge(app.meterRegistry, "hoglake_head_age_seconds", cat)).isPositive()
            assertThat(gauge(app.meterRegistry, "hoglake_earliest_snapshot_age_seconds", cat)).isPositive()
            assertThat(gauge(app.meterRegistry, "hoglake_live_totals_age_seconds", cat)).isGreaterThanOrEqualTo(0.0)
            assertThat(gauge(app.meterRegistry, "hoglake_metrics_last_sample_epoch")).isPositive()
            assertThat(app.meterRegistry.find("hoglake_metrics_sample_errors_total").counter()!!.count()).isZero()
            assertThat(app.meterRegistry.find("hoglake_metrics_group_failures_total").counters()).hasSize(6)
            assertThat(app.meterRegistry.find("hoglake_relation_bytes").gauges()).isEmpty()
            assertThat(app.meterRegistry.find("hoglake_relation_live_tuples").gauges()).isEmpty()
            assertThat(app.meterRegistry.find("hoglake_relation_dead_tuples").gauges()).isEmpty()
            assertThat(expected.size + 6).isEqualTo(36)
        }
    }

    @Test
    fun `mixed selection excludes unregistered rows and distinguishes a missing summary from observed zero`() {
        app().use { app ->
            val pg = create(app, "postgres")
            val hg = create(app, "hoglite")
            val stranger = name()
            CatalogService(db.jdbi).createCatalog(stranger, "s3://hoglake-it/$stranger")
            app.catalogMetrics.sampleOnce()
            assertThat(gauge(app.meterRegistry, "hoglake_head_snapshot_id", stranger)).isNull()
            assertThat(gauge(app.meterRegistry, "hoglake_live_totals_sampled", pg)).isZero()
            assertThat(gauge(app.meterRegistry, "hoglake_live_rows", pg)).isNull()
            assertThat(gauge(app.meterRegistry, "hoglake_live_totals_age_seconds", pg)).isNull()
            assertThat(gauge(app.meterRegistry, "hoglake_live_rows", hg)).isZero()
            assertThat(gauge(app.meterRegistry, "hoglake_live_totals_sampled", hg)).isEqualTo(1.0)
            assertThat(app.meterRegistry.find("hoglake_relation_bytes").gauges()).isNotEmpty()
            assertThat(app.meterRegistry.find("hoglake_relation_live_tuples").gauges()).isNotEmpty()
            assertThat(app.meterRegistry.find("hoglake_relation_dead_tuples").gauges()).isNotEmpty()
            publishMaintenanceSample(db.jdbi)
            app.catalogMetrics.sampleOnce()
            assertThat(gauge(app.meterRegistry, "hoglake_live_rows", pg)).isZero()
        }
    }

    @Test
    fun `a backend outage retains stale values while healthy catalogs refresh without advancing the epoch`() {
        val available = AtomicBoolean(true)
        app(available).use { app ->
            val pg = create(app, "postgres")
            val hg = create(app, "hoglite")
            app.catalogMetrics.sampleOnce()
            val epoch = gauge(app.meterRegistry, "hoglake_metrics_last_sample_epoch")
            val before = app.catalogMetrics.latestTotals
            available.set(false)
            await().atMost(Duration.ofSeconds(3)).until { Instant.now().epochSecond.toDouble() > epoch!! }
            write(hg, HogliteStatement("UPDATE meta SET next_snapshot = 101 WHERE id = 1"))
            assertThatThrownBy { app.catalogMetrics.sampleOnce() }.isInstanceOf(SQLException::class.java)
            assertThat(gauge(app.meterRegistry, "hoglake_head_snapshot_id", hg)).isEqualTo(100.0)
            assertThat(gauge(app.meterRegistry, "hoglake_head_snapshot_id", pg)).isEqualTo(2.0)
            assertThat(gauge(app.meterRegistry, "hoglake_metrics_last_sample_epoch")).isEqualTo(epoch)
            assertThat(app.catalogMetrics.latestTotals).isEqualTo(before)
            assertThat(app.meterRegistry.find("hoglake_metrics_sample_errors_total").counter()!!.count()).isEqualTo(1.0)
            val freshRegistry = SimpleMeterRegistry()
            val freshSampler =
                CatalogMetrics(
                    db.jdbi,
                    freshRegistry,
                    source = RoutedCatalogMetricSource(store, 1_000, app.mixedRouter, db.jdbi),
                )
            assertThatThrownBy { freshSampler.sampleOnce() }.isInstanceOf(SQLException::class.java)
            assertThat(gauge(freshRegistry, "hoglake_live_rows", pg)).isNull()
            assertThat(gauge(freshRegistry, "hoglake_live_totals_age_seconds", pg)).isNull()
            assertThat(gauge(freshRegistry, "hoglake_metrics_last_sample_epoch")).isZero()
            assertThat(freshSampler.latestTotals).isNull()
            available.set(true)
            create(app, "hoglite")
            available.set(false)
            assertThatThrownBy { app.catalogMetrics.sampleOnce() }.isInstanceOf(SQLException::class.java)
            assertThat(app.catalogMetrics.latestTotals).isNull()
            available.set(true)
        }
    }

    @Test
    fun `a failed directory read preserves observations without backend fallback`() {
        app().use { app ->
            val cat = create(app, "hoglite")
            app.catalogMetrics.sampleOnce()
            val before = app.catalogMetrics.latestTotals
            val epoch = gauge(app.meterRegistry, "hoglake_metrics_last_sample_epoch")
            val selected = AtomicInteger()
            app.mixedRouter.selected = { selected.incrementAndGet() }

            fun rename(sql: String) {
                engine.client.tx(
                    store.registryTenantId,
                    UUID.randomUUID().toString(),
                    null,
                    listOf(HogliteStatement(sql)),
                )
            }
            rename("ALTER TABLE catalogs RENAME TO unavailable_catalogs")
            try {
                assertThatThrownBy { app.catalogMetrics.sampleOnce() }.isInstanceOf(Exception::class.java)
                assertThat(gauge(app.meterRegistry, "hoglake_head_snapshot_id", cat)).isEqualTo(2.0)
                assertThat(gauge(app.meterRegistry, "hoglake_metrics_last_sample_epoch")).isEqualTo(epoch)
                assertThat(app.catalogMetrics.latestTotals).isEqualTo(before)
                assertThat(selected.get()).isZero()
            } finally {
                rename("ALTER TABLE unavailable_catalogs RENAME TO catalogs")
            }
        }
    }

    @Test
    fun `failed exact refresh ages its old totals without fabricating zero`() {
        app().use { app ->
            val hg = create(app, "hoglite")
            seedHoglite(hg)
            app.catalogMetrics.sampleOnce()
            val age = gauge(app.meterRegistry, "hoglake_live_totals_age_seconds", hg)!!
            write(hg, HogliteStatement("ALTER TABLE files RENAME TO unavailable_files"))
            try {
                assertThatThrownBy { app.catalogMetrics.sampleOnce() }.isInstanceOf(Exception::class.java)
                assertThat(gauge(app.meterRegistry, "hoglake_live_rows", hg)).isEqualTo(18.0)
                await().atMost(Duration.ofSeconds(2)).untilAsserted {
                    assertThat(
                        gauge(app.meterRegistry, "hoglake_live_totals_age_seconds", hg),
                    ).isGreaterThan(age + 0.05)
                }
            } finally {
                write(hg, HogliteStatement("ALTER TABLE unavailable_files RENAME TO files"))
            }
        }
    }

    @Test
    fun `new samplers recover old ok history after 1001 failures on both backends with indexed reads`() {
        app().use { app ->
            val hg = create(app, "hoglite")
            val pg = create(app, "postgres")
            val ok = "2026-01-01T00:00:02.000Z"
            val failed = "2026-01-02T00:00:02.000Z"
            write(
                hg,
                HogliteStatement(
                    """
                        INSERT INTO maintenance_runs(task,run_trigger,started_at,finished_at,status) VALUES
                        ('expiry','loop',?,?,'ok')
                        """,
                    listOf("2026-01-01T00:00:00.000Z", ok),
                ),
                HogliteStatement(
                    """
                        INSERT INTO maintenance_runs(task,run_trigger,started_at,finished_at,status,error) WITH
                        RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<1001) SELECT
                        'expiry','loop',?,?, 'failed','fixture' FROM n
                        """,
                    listOf("2026-01-02T00:00:00.000Z", failed),
                ),
            )
            db.jdbi.withHandleUnchecked { h ->
                h.createUpdate(
                    """
                        INSERT INTO hog_maintenance_run(catalog_id,task,run_trigger,started_at,finished_at,status)
                        SELECT catalog_id,'expiry','loop',?::timestamptz,?::timestamptz,'ok' FROM hog_catalog WHERE
                        name=?
                        """,
                )
                    .bind(0, "2026-01-01T00:00:00Z").bind(1, ok).bind(2, pg).execute()
                h.createUpdate(
                    """
                        INSERT INTO
                        hog_maintenance_run(catalog_id,task,run_trigger,started_at,finished_at,status,error) SELECT
                        c.catalog_id,'expiry','loop',?::timestamptz,?::timestamptz,'failed','fixture' FROM hog_catalog
                        c CROSS JOIN generate_series(1,1001) n WHERE c.name=?
                        """,
                )
                    .bind(0, "2026-01-02T00:00:00Z").bind(1, failed).bind(2, pg).execute()
                h.execute("ANALYZE hog_maintenance_run")
                val plan =
                    h.createQuery(
                        """
                            EXPLAIN SELECT finished_at FROM hog_maintenance_run WHERE catalog_id=(SELECT catalog_id FROM
                            hog_catalog WHERE name=:name) AND task='expiry' AND status='ok' ORDER BY run_id DESC LIMIT 1
                            """,
                    ).bind("name", pg).mapTo(String::class.java).list().joinToString("\n")
                assertThat(plan).contains("hog_maintenance_run_status")
            }
            store.catalogOperation(hg) {
                val plan =
                    store.queryCatalog(
                        hg,
                        "EXPLAIN QUERY PLAN ${HogliteCatalogMetrics.LAST_STATUS_SQL}",
                        "expiry",
                        "ok",
                    ).rows.toString()
                assertThat(plan).contains("maintenance_runs_status")
            }
            repeat(2) {
                val registry = SimpleMeterRegistry()
                val restarted =
                    CatalogMetrics(
                        db.jdbi,
                        registry,
                        source = RoutedCatalogMetricSource(store, 1_000, app.mixedRouter, db.jdbi),
                    )
                restarted.sampleOnce()
                for (catalog in listOf(pg, hg)) {
                    assertThat(
                        gauge(
                            registry,
                            "hoglake_maintenance_last_run_epoch",
                            catalog,
                            "task",
                            "expiry",
                            "status",
                            "ok",
                        ),
                    ).isEqualTo(Instant.parse(ok).epochSecond.toDouble())
                    assertThat(
                        gauge(
                            registry,
                            "hoglake_maintenance_last_run_epoch",
                            catalog,
                            "task",
                            "expiry",
                            "status",
                            "failed",
                        ),
                    ).isEqualTo(Instant.parse(failed).epochSecond.toDouble())
                    assertThat(
                        gauge(registry, "hoglake_maintenance_last_run_duration_seconds", catalog, "task", "expiry"),
                    ).isCloseTo(2.0, within(0.001))
                }
            }
        }
    }

    @Test
    fun `extended ledger failure preserves its last values while a successful core advances the epoch`() {
        app().use { app ->
            val cat = create(app, "hoglite")
            seedHoglite(cat)
            app.catalogMetrics.sampleOnce()
            val epoch = gauge(app.meterRegistry, "hoglake_metrics_last_sample_epoch")!!
            write(cat, HogliteStatement("ALTER TABLE maintenance_runs RENAME TO unavailable_runs"))
            try {
                await().atMost(Duration.ofSeconds(3)).untilAsserted {
                    assertThatThrownBy { app.catalogMetrics.sampleOnce() }.isInstanceOf(Exception::class.java)
                    assertThat(gauge(app.meterRegistry, "hoglake_metrics_last_sample_epoch")).isGreaterThan(epoch)
                }
                assertThat(
                    gauge(app.meterRegistry, "hoglake_maintenance_last_run_duration_seconds", cat),
                ).isEqualTo(2.0)
                assertThat(
                    app.meterRegistry.find(
                        "hoglake_metrics_group_failures_total",
                    ).tag("group", "maintenance_runs").counter()!!.count(),
                ).isPositive()
                assertThat(
                    app.meterRegistry.find(
                        "hoglake_metrics_group_failures_total",
                    ).tag("group", "catalogs").counter()!!.count(),
                ).isZero()
            } finally {
                write(cat, HogliteStatement("ALTER TABLE unavailable_runs RENAME TO maintenance_runs"))
            }
        }
    }

    @Test
    fun `caps preserve worst consumer lag and report omitted tables on exact reads`() {
        app().use { app ->
            val cat = create(app, "hoglite")
            seedHoglite(cat)
            app.store.createTable(cat, "ns", "empty", listOf(ColumnDef("id", ColType.LONG)))
            write(
                cat,
                HogliteStatement(
                    "INSERT INTO offsets(consumer_id,table_uuid,committed_snapshot,updated_at) " +
                        "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<101) " +
                        "SELECT 'c' || i,table_uuid,0,? FROM n CROSS JOIN tables WHERE id=1",
                    listOf(HogliteSchema.now()),
                ),
            )
            val registry = SimpleMeterRegistry()
            val metrics =
                CatalogMetrics(
                    db.jdbi,
                    registry,
                    maxTableSeries = 1,
                    source = RoutedCatalogMetricSource(store, 1_000, app.mixedRouter, db.jdbi),
                )
            metrics.sampleOnce()
            assertThat(registry.find("hoglake_consumer_lag_snapshots").tag("catalog", cat).gauges()).isEmpty()
            assertThat(gauge(registry, "hoglake_consumer_lag_max", cat)).isEqualTo(11.0)
            assertThat(gauge(registry, "hoglake_table_series_truncated", cat)).isEqualTo(1.0)
            assertThat(gauge(registry, "hoglake_table_files", cat, "table", "events")).isEqualTo(2.0)
            assertThat(gauge(registry, "hoglake_table_files", cat, "table", "empty")).isNull()
        }
    }

    @Test
    fun `incomplete reservations do not fail core collection or publish PostgreSQL rows`() {
        app().use { app ->
            val cat = name()
            val id =
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery("SELECT nextval(pg_get_serial_sequence('hog_catalog','catalog_id'))")
                        .mapTo(Long::class.javaObjectType).one()
                }
            app.mixedRouter.directory.reserve(cat, "s3://hoglake-it/$cat", id)
            app.catalogMetrics.sampleOnce()
            assertThat(gauge(app.meterRegistry, "hoglake_head_snapshot_id", cat)).isNull()
            assertThat(gauge(app.meterRegistry, "hoglake_metrics_last_sample_epoch")).isPositive()
            assertThat(app.meterRegistry.find("hoglake_metrics_sample_errors_total").counter()!!.count()).isZero()
        }
    }

    @Test
    fun `drop and recreation during a sample never adopts replacement identities or maintenance epochs`() {
        app().use { app ->
            val cat = name()
            app.store.createCatalog(cat, "s3://hoglake-it/$cat", backend = "hoglite")
            write(
                cat,
                HogliteStatement(
                    """
                        INSERT INTO maintenance_runs(task,run_trigger,started_at,finished_at,status) VALUES
                        ('expiry','loop',?,?,'ok')
                        """,
                    listOf("2026-01-01T00:00:00.000Z", "2026-01-01T00:00:02.000Z"),
                ),
            )
            app.catalogMetrics.sampleOnce()
            val epoch = gauge(app.meterRegistry, "hoglake_metrics_last_sample_epoch")
            app.mixedRouter.selected = { entry ->
                if (entry.name == cat) {
                    app.mixedRouter.selected = {}
                    app.store.dropCatalog(cat, "fixture", "drop")
                    app.store.createCatalog(cat, "s3://hoglake-it/$cat", backend = "postgres")
                }
            }
            assertThatThrownBy { app.catalogMetrics.sampleOnce() }.isInstanceOf(Exception::class.java)
            assertThat(gauge(app.meterRegistry, "hoglake_metrics_last_sample_epoch")).isEqualTo(epoch)
            // The failed old incarnation can remain stale until the next authoritative roster.
            app.catalogMetrics.sampleOnce()
            assertThat(gauge(app.meterRegistry, "hoglake_head_snapshot_id", cat)).isZero()
            assertThat(gauge(app.meterRegistry, "hoglake_maintenance_last_run_epoch", cat)).isNull()
            app.store.dropCatalog(cat, "fixture", "drop")
            app.catalogMetrics.sampleOnce()
            assertThat(gauge(app.meterRegistry, "hoglake_head_snapshot_id", cat)).isNull()
            assertThat(gauge(app.meterRegistry, "hoglake_live_totals_sampled", cat)).isNull()
        }
    }
}
