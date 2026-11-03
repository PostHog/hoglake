package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.BackgroundLoops
import com.posthog.hoglake.Config
import com.posthog.hoglake.Database
import com.posthog.hoglake.DatabaseBackend
import com.posthog.hoglake.HealthProbe
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.persistence.CatalogBinding
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.ktor.client.request.delete
import io.ktor.client.request.get
import io.ktor.client.request.patch
import io.ktor.client.request.post
import io.ktor.client.request.put
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.testApplication
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.sql.SQLException
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource

/** Real HTTP handlers over a live PostgreSQL database and the pinned hoglited binary. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MixedCatalogIntegrationTest {
    private lateinit var engine: HogliteConformanceEngine
    private lateinit var db: PgTestSupport.TestDb
    private val names = AtomicInteger()
    private val json = jacksonObjectMapper()

    @BeforeAll
    fun start() {
        engine = HogliteConformanceEngine()
        db = PgTestSupport.freshDatabase()
    }

    @AfterAll
    fun stop() {
        if (::db.isInitialized) db.close()
        if (::engine.isInitialized) engine.close()
        Metrics.clear()
    }

    private fun name() = "mixed${names.incrementAndGet()}"

    private fun app(jdbi: Jdbi = db.jdbi): App =
        App.buildMixed(
            engine.config().copy(database = DatabaseBackend.MIXED, hogliteUrl = "http://127.0.0.1:1"),
            jdbi,
            engine.store,
            HealthProbe.forProbe(1_000) { true },
        )

    private fun create(
        app: App,
        name: String,
        backend: String? = null,
    ) = app.store.createCatalog(name, "s3://hoglake-it/$name", backend = backend, connectionRef = backend)

    @Test
    fun `concurrent catalogs use every catalog HTTP service port without changing identities`() {
        app().use { app ->
            testApplication {
                application { app.module(this) }
                val hg = name()
                val pg = name()
                coroutineScope {
                    val jobs =
                        listOf(hg to null, pg to "postgres").map { (name, backend) ->
                            async(Dispatchers.IO) {
                                val payload = mutableMapOf("name" to name, "data_path" to "s3://hoglake-it/$name")
                                if (backend != null) payload["backend"] = backend
                                val created =
                                    client.post("/v1/catalogs") {
                                        contentType(ContentType.Application.Json)
                                        setBody(json.writeValueAsString(payload))
                                    }
                                assertThat(created.status).isEqualTo(HttpStatusCode.Created)
                                assertThat(json.readTree(created.bodyAsText())["name"].asText()).isEqualTo(name)
                                val root = "/v1/catalogs/$name"
                                assertThat(
                                    client.post("$root/namespaces") {
                                        contentType(ContentType.Application.Json)
                                        setBody("""{"name":"ns"}""")
                                    }.status,
                                ).isEqualTo(HttpStatusCode.Created)
                                assertThat(
                                    client.post("$root/namespaces/ns/tables") {
                                        contentType(ContentType.Application.Json)
                                        setBody("""{"name":"t","columns":[{"name":"id","type":"long"}]}""")
                                    }.status,
                                ).isEqualTo(HttpStatusCode.Created)
                                assertThat(
                                    client.patch("$root/options") {
                                        contentType(ContentType.Application.Json)
                                        setBody(
                                            """{"snapshot_retention_seconds":3600,"author":"test","message":"set"}""",
                                        )
                                    }.status,
                                ).isEqualTo(HttpStatusCode.OK)
                                for (path in listOf(
                                    "",
                                    "/storage-usage",
                                    "/namespaces",
                                    "/namespaces/ns/tables",
                                    "/namespaces/ns/tables/t",
                                    "/namespaces/ns/tables/t/partitions",
                                    "/stats/partitions",
                                    "/namespaces/ns/views",
                                    "/options",
                                    "/maintenance/status",
                                    "/maintenance/runs",
                                )) {
                                    val response = client.get(root + path)
                                    assertThat(
                                        response.status,
                                    ).describedAs(root + path + response.bodyAsText()).isEqualTo(HttpStatusCode.OK)
                                }
                                val operationId = UUID.randomUUID()
                                val prepared =
                                    client.put("$root/table-creations/$operationId") {
                                        contentType(ContentType.Application.Json)
                                        setBody(
                                            """{"namespace":"ns","name":"staged","author":"test","message":"prepare",
                                              "columns":[{"name":"id","type":"long"}]}""",
                                        )
                                    }
                                assertThat(prepared.status).describedAs(prepared.bodyAsText())
                                    .isEqualTo(HttpStatusCode.OK)
                                assertThat(client.get("$root/table-creations/$operationId").status)
                                    .isEqualTo(HttpStatusCode.OK)
                                assertThat(client.post("$root/table-creations/$operationId/abort").status)
                                    .isEqualTo(HttpStatusCode.OK)
                                assertThat(
                                    client.post("$root/uploads/renew") {
                                        contentType(ContentType.Application.Json)
                                        setBody("""{"owner":"${UUID.randomUUID()}"}""")
                                    }.status,
                                ).isEqualTo(HttpStatusCode.OK)
                                val fieldId = app.store.getTable(name, "ns", "t").columns.first().fieldId
                                assertThat(
                                    client.post("$root/namespaces/ns/tables/t/alter") {
                                        contentType(ContentType.Application.Json)
                                        setBody(
                                            """{"author":"test","message":"partition","ops":[{"op":"set_partition_spec",
                                            "fields":[{"source_field_id":$fieldId,"transform":"identity"}]}]}""",
                                        )
                                    }.status,
                                ).isEqualTo(HttpStatusCode.OK)
                                assertThat(
                                    client.post("$root/namespaces/ns/tables/t/partitions/lookup?spec_id=1") {
                                        contentType(ContentType.Application.Json)
                                        setBody("""["1"]""")
                                    }.status,
                                ).isEqualTo(HttpStatusCode.OK)
                                assertThat(
                                    client.post("$root/namespaces/ns/views") {
                                        contentType(ContentType.Application.Json)
                                        setBody(
                                            """{"name":"v","sql":"select 1","author":"test","message":"create view"}""",
                                        )
                                    }.status,
                                ).isEqualTo(HttpStatusCode.Created)
                                for (task in listOf("expire", "cleanup", "compact", "rehydrate")) {
                                    val response = client.post("$root/maintenance/$task")
                                    assertThat(
                                        response.status,
                                    ).describedAs(task + response.bodyAsText()).isEqualTo(HttpStatusCode.OK)
                                }
                                assertThat(
                                    client.post("$root/namespaces/ns/tables/t/alter") {
                                        contentType(ContentType.Application.Json)
                                        setBody(
                                            """{"author":"test","message":"alter",
"ops":[{"op":"add_column","column":{"name":"extra","type":"string"}}]}""",
                                        )
                                    }.status,
                                ).isEqualTo(HttpStatusCode.OK)
                            }
                        }
                    jobs.forEach { it.await() }
                }
                assertThat(app.mixedRouter.directory.find(hg)!!.backend).isEqualTo(DatabaseBackend.HOGLITE)
                assertThat(app.mixedRouter.directory.find(pg)!!.backend).isEqualTo(DatabaseBackend.POSTGRES)
                val listed = client.get("/v1/catalogs")
                assertThat(listed.status).isEqualTo(HttpStatusCode.OK)
                assertThat(client.get("/v1/maintenance/status").status).isEqualTo(HttpStatusCode.OK)
                assertThat(client.get("/v1/maintenance/runs").status).isEqualTo(HttpStatusCode.OK)
                assertThat(client.get("/v1/database/health").status).isEqualTo(HttpStatusCode.OK)
            }
        }
    }

    @Test
    fun `one name racing across backends publishes exactly one identity`() {
        app().use { app ->
            val name = name()
            val gate = CountDownLatch(1)
            val pool = Executors.newFixedThreadPool(2)
            try {
                val attempts =
                    listOf("postgres", "hoglite").map { backend ->
                        pool.submit<Result<Any>> {
                            gate.await()
                            runCatching { create(app, name, backend) }
                        }
                    }
                gate.countDown()
                val results = attempts.map { it.get(30, TimeUnit.SECONDS) }
                assertThat(results.count { it.isSuccess }).isEqualTo(1)
                assertThat(results.single { it.isFailure }.exceptionOrNull())
                    .isInstanceOfAny(
                        HoglakeException.AlreadyExists::class.java,
                        HoglakeException.BackendUnavailable::class.java,
                    )
                val row = app.mixedRouter.directory.find(name)!!
                assertThat(app.store.getCatalog(name).name).isEqualTo(name)
                if (row.backend == DatabaseBackend.HOGLITE) {
                    assertThat(db.jdbi.withHandleUnchecked { CatalogRepo.findByName(it, name) }).isNull()
                }
            } finally {
                pool.shutdownNow()
            }
        }
    }

    @Test
    fun `mixed instance feed keeps overlapping ledger ids as distinct ordered rows`() {
        app().use { app ->
            val pg = name()
            val hg = name()
            val pgId = create(app, pg, "postgres").catalogId
            create(app, hg)
            val started = Instant.now().plusSeconds(60)
            val id =
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery(
                        """
                        INSERT INTO hog_maintenance_run
                            (catalog_id, task, run_trigger, started_at, finished_at, status)
                        VALUES (?, 'cleanup', 'manual', ?, ?, 'ok') RETURNING run_id
                        """,
                    ).bind(0, pgId).bind(1, started).bind(2, started).mapTo(Long::class.java).one()
                }
            val timestamp = HogliteSchema.fmt(started.minusSeconds(1))
            engine.client.tx(
                app.mixedRouter.directory.find(hg)!!.identity,
                UUID.randomUUID().toString(),
                null,
                listOf(
                    HogliteStatement(
                        "INSERT INTO maintenance_runs(id, task, run_trigger, started_at, finished_at, status) " +
                            "VALUES (?, 'cleanup', 'manual', ?, ?, 'ok')",
                        listOf(id, timestamp, timestamp),
                    ),
                ),
            )
            testApplication {
                application { app.module(this) }
                for (suffix in listOf("", "?before=$id")) {
                    val response = client.get("/v1/maintenance/runs$suffix")
                    assertThat(response.status).isEqualTo(HttpStatusCode.OK)
                    val rows =
                        json.readTree(response.bodyAsText())["runs"].filter { it["catalog"].asText() in setOf(pg, hg) }
                    assertThat(rows.map { it["catalog"].asText() }).containsExactly(pg, hg)
                    assertThat(rows.map { it["run_id"].asLong() }).containsExactly(id, id)
                }
            }
        }
    }

    @Test
    fun `a selected cleanup never purges another PostgreSQL catalogs drained ledger`() {
        app().use { app ->
            val selected = name()
            val other = name()
            val selectedId = create(app, selected, "postgres").catalogId
            val otherId = create(app, other, "postgres").catalogId
            db.jdbi.withHandleUnchecked { h ->
                for (id in listOf(selectedId, otherId)) {
                    h.createUpdate(
                        """
                        INSERT INTO hog_file_removal
                            (catalog_id, path, file_kind, reason, drained_at, drained_outcome)
                        VALUES (?, ?, 'data', 'snapshot_expiry', now() - interval '40 days', 'deleted')
                        """,
                    ).bind(0, id).bind(1, "s3://hoglake-it/$id/drained").execute()
                }
            }
            app.mixedCatalogTask(app.mixedRouter.directory.find(selected)!!, MaintenanceTask.CLEANUP)
            val remaining =
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery("SELECT catalog_id FROM hog_file_removal WHERE catalog_id IN (?, ?)")
                        .bind(0, selectedId).bind(1, otherId).mapTo(Long::class.java).list()
                }
            assertThat(remaining).containsExactly(otherId)
        }
    }

    @Test
    fun `invalid aliases fail before reservation or backend work`() {
        app(Jdbi.create { throw SQLException("offline") }).use { app ->
            for ((backend, reference) in listOf(
                "postgres" to "hoglite",
                "hoglite" to "postgres",
                "mixed" to "mixed",
                "postgres" to "unknown",
                "unknown" to null,
                null to "https://example.invalid",
            )) {
                val name = name()
                assertThatThrownBy {
                    app.store.createCatalog(name, "s3://hoglake-it/$name", backend = backend, connectionRef = reference)
                }.isInstanceOf(HoglakeException.Validation::class.java)
                assertThat(app.mixedRouter.directory.find(name)).isNull()
            }
        }
    }

    @Test
    fun `existing hoglite routes survive PostgreSQL failure while new creates fail closed`() {
        val hg = name()
        val pg = name()
        app().use {
            create(it, hg)
            create(it, pg, "postgres")
        }
        app(Jdbi.create { throw SQLException("offline") }).use { app ->
            testApplication {
                application { app.module(this) }
                assertThat(client.get("/v1/catalogs/$hg").status).isEqualTo(HttpStatusCode.OK)
                assertThat(
                    client.post("/v1/catalogs/$hg/namespaces") {
                        contentType(ContentType.Application.Json)
                        setBody("""{"name":"healthy"}""")
                    }.status,
                ).isEqualTo(HttpStatusCode.Created)
                assertThat(client.get("/v1/catalogs/$pg").status).isEqualTo(HttpStatusCode.ServiceUnavailable)
                val fresh = name()
                assertThat(
                    client.post("/v1/catalogs") {
                        contentType(ContentType.Application.Json)
                        setBody("""{"name":"$fresh","data_path":"s3://hoglake-it/$fresh"}""")
                    }.status,
                ).isEqualTo(HttpStatusCode.ServiceUnavailable)
                assertThat(app.mixedRouter.directory.find(fresh)).isNull()
                assertThat(app.mixedRouter.directory.find(pg)!!.backend).isEqualTo(DatabaseBackend.POSTGRES)
            }
        }
    }

    @Test
    fun `a registry outage never probes or redirects to PostgreSQL`() {
        // A refused connection opens the breaker. The outage must not query PostgreSQL.
        val dead =
            HogliteClient(
                standardUrl = "http://127.0.0.1:1",
                retryBudgetMs = 2_000,
                connectTimeoutMs = 100,
                circuitFailureThreshold = 1,
                sleeper = {},
            )
        val pgCalls = AtomicInteger()
        val noDb =
            Jdbi.create {
                pgCalls.incrementAndGet()
                throw SQLException("must not access PostgreSQL")
            }
        App.buildMixed(
            Config(database = DatabaseBackend.MIXED, hogliteUrl = "http://127.0.0.1:1"),
            noDb,
            HogliteCatalogStore(dead),
            HealthProbe.forProbe(1_000) { false },
        ).use { app ->
            assertThatThrownBy {
                app.store.getCatalog(
                    "missing",
                )
            }.isInstanceOf(HoglakeException.BackendUnavailable::class.java)
            assertThat(pgCalls.get()).isZero()
        }
        dead.close()
    }

    @Test
    fun `a missing initialized directory fails closed instead of becoming an empty roster`() {
        val isolated = HogliteCatalogStore(engine.client, tenantPrefix = "missing-${UUID.randomUUID()}-")
        App.buildMixed(
            engine.config().copy(database = DatabaseBackend.MIXED, hogliteUrl = "http://127.0.0.1:1"),
            db.jdbi,
            isolated,
            HealthProbe.forProbe(1_000) { true },
        ).use { app ->
            val name = name()
            create(app, name)
            val entry = app.mixedRouter.directory.find(name)!!
            engine.client.release(isolated.registryTenantId)
            engine.client.deleteTenant(isolated.registryTenantId)
            try {
                assertThatThrownBy { app.store.getCatalog(name) }
                    .isInstanceOf(HoglakeException.BackendUnavailable::class.java)
                assertThatThrownBy { app.store.listCatalogs() }
                    .isInstanceOf(HoglakeException.BackendUnavailable::class.java)
                assertThatThrownBy { create(app, name, "postgres") }
                    .isInstanceOf(HoglakeException.BackendUnavailable::class.java)
            } finally {
                engine.client.release(entry.identity)
                engine.client.deleteTenant(entry.identity)
            }
        }
    }

    @Test
    fun `publication failure retains the inserted identity across restart until pinned DELETE`() {
        val name = name()
        val identity: String
        app().use { app ->
            app.mixedRouter.directory.beforePublish = {
                throw HoglakeException.BackendUnavailable("publication failed")
            }
            assertThatThrownBy {
                create(
                    app,
                    name,
                    "postgres",
                )
            }.isInstanceOf(HoglakeException.BackendUnavailable::class.java)
            val entry = app.mixedRouter.directory.find(name)!!
            assertThat(entry.state).isEqualTo("creating")
            identity = entry.identity
            assertThat(
                db.jdbi.withHandleUnchecked { CatalogRepo.require(it, name).catalogId },
            ).isEqualTo(identity.toLong())
            assertThatThrownBy {
                app.store.getCatalog(
                    name,
                )
            }.isInstanceOf(HoglakeException.BackendUnavailable::class.java)
            assertThatThrownBy {
                create(
                    app,
                    name,
                    "postgres",
                )
            }.isInstanceOf(HoglakeException.AlreadyExists::class.java)
        }
        app().use { app ->
            assertThat(app.mixedRouter.directory.find(name)!!.identity).isEqualTo(identity)
            app.store.dropCatalog(name, "test", "drop empty catalog")
            assertThat(app.mixedRouter.directory.find(name)).isNull()
            assertThat(db.jdbi.withHandleUnchecked { CatalogRepo.findByName(it, name) }).isNull()
            create(app, name, "postgres")
            assertThat(app.mixedRouter.directory.find(name)!!.identity).isNotEqualTo(identity)
        }
    }

    @Test
    fun `a retained reservation is directly observable without breaking ready rosters or feeds`() {
        app().use { app ->
            val prefix = "reservation${names.incrementAndGet()}"
            val pending = "$prefix-a"
            val hg = "$prefix-b"
            val pg = "$prefix-c"
            create(app, hg)
            create(app, pg, "postgres")
            app.mixedCatalogTask(app.mixedRouter.directory.find(hg)!!, MaintenanceTask.CLEANUP)
            app.mixedCatalogTask(app.mixedRouter.directory.find(pg)!!, MaintenanceTask.CLEANUP)
            app.mixedRouter.directory.beforePublish = {
                throw HoglakeException.BackendUnavailable("publication failed")
            }
            assertThatThrownBy { create(app, pending, "postgres") }
                .isInstanceOf(HoglakeException.BackendUnavailable::class.java)
            testApplication {
                application { app.module(this) }
                val listed = client.get("/v1/catalogs")
                assertThat(listed.status).isEqualTo(HttpStatusCode.OK)
                assertThat(json.readTree(listed.bodyAsText()).map { it["name"].asText() })
                    .contains(hg, pg).doesNotContain(pending)
                val status = client.get("/v1/maintenance/status?after=$prefix&limit=1")
                assertThat(status.status).isEqualTo(HttpStatusCode.OK)
                assertThat(json.readTree(status.bodyAsText())["catalogs"].map { it["catalog"].asText() })
                    .containsExactly(hg)
                val next = client.get("/v1/maintenance/status?after=$hg&limit=1")
                assertThat(next.status).isEqualTo(HttpStatusCode.OK)
                assertThat(json.readTree(next.bodyAsText())["catalogs"].map { it["catalog"].asText() })
                    .containsExactly(pg)
                val feed = client.get("/v1/maintenance/runs?limit=1000")
                assertThat(feed.status).isEqualTo(HttpStatusCode.OK)
                assertThat(json.readTree(feed.bodyAsText())["runs"].map { it["catalog"].asText() })
                    .contains(hg, pg).doesNotContain(pending)
                val direct = client.get("/v1/catalogs/$pending")
                assertThat(direct.status).isEqualTo(HttpStatusCode.ServiceUnavailable)
                assertThat(direct.bodyAsText()).contains("creating", pending)
            }
            val entry = app.mixedRouter.directory.find(pending)
            app(Jdbi.create { throw SQLException("offline") }).use { offline ->
                assertThatThrownBy { offline.store.dropCatalog(pending, "test", "cancel") }
                    .isInstanceOf(HoglakeException.BackendUnavailable::class.java)
            }
            assertThat(app.mixedRouter.directory.find(pending)).isEqualTo(entry)
            app.store.dropCatalog(pending, "test", "cancel with PostgreSQL available")
        }
    }

    @Test
    fun `mixed compaction retains the seeded jitter when leases are disabled`() {
        val cfg =
            engine.config().copy(
                database = DatabaseBackend.MIXED,
                hogliteUrl = engine.url(),
                hogliteLoopLeases = false,
                compactionIntervalMs = 100,
                hydratorIntervalMs = 0,
                expiryIntervalMs = 0,
                cleanupIntervalMs = 0,
                retirementIntervalMs = 0,
                reindexIntervalMs = 0,
                metricsIntervalMs = 0,
                maintenanceSummaryIntervalMs = 0,
            )
        val isolated = HogliteCatalogStore(engine.client, tenantPrefix = "jitter-${UUID.randomUUID()}-")
        App.buildMixed(cfg, db.jdbi, isolated, HealthProbe.forProbe(1_000) { true }).use { app ->
            val identity =
                App::class.java.getDeclaredField("plannerIdentity").let {
                    it.isAccessible = true
                    it.get(app) as String
                }
            val random = java.util.Random(com.posthog.hoglake.compaction.CompactionService.identitySeed(identity))
            val expected =
                (1..3).map {
                    BackgroundLoops.jitteredDelayMs(
                        100,
                        BackgroundLoops.COMPACTION_JITTER_FRACTION,
                        random.nextDouble(),
                    )
                }
            val delays = java.util.Collections.synchronizedList(mutableListOf<Long>())
            val collected = CountDownLatch(3)
            val loops = BackgroundLoops()
            loops.onDelay = { loop, delay ->
                if (loop == "compaction" && delays.size < 3) {
                    delays += delay
                    collected.countDown()
                }
            }
            app.startMixedBackground(loops).use {
                assertThat(collected.await(10, TimeUnit.SECONDS)).isTrue()
            }
            assertThat(delays).containsExactlyElementsOf(expected)
        }
    }

    @Test
    fun `unregistered PostgreSQL catalogs are neither adopted nor deleted`() {
        val name = name()
        val original = CatalogService(db.jdbi).createCatalog(name, "s3://hoglake-it/$name")
        app().use { app ->
            for (backend in listOf("postgres", "hoglite")) {
                assertThatThrownBy {
                    create(
                        app,
                        name,
                        backend,
                    )
                }.isInstanceOf(HoglakeException.AlreadyExists::class.java)
            }
            assertThatThrownBy {
                app.store.dropCatalog(name, "test", "drop empty catalog")
            }.isInstanceOf(HoglakeException.NotFound::class.java)
            assertThat(CatalogService(db.jdbi).getCatalog(name).catalogId).isEqualTo(original.catalogId)
        }
    }

    @Test
    fun `paused commit DDL and maintenance cannot reach a recreated catalog`() {
        app().use { app ->
            testApplication {
                application { app.module(this) }
                coroutineScope {
                    for ((backend, replacement) in listOf(
                        "postgres" to "postgres",
                        "postgres" to "hoglite",
                        "hoglite" to "postgres",
                        "hoglite" to "hoglite",
                    )) {
                        for (operation in listOf("commit", "ddl", "cleanup", "expire", "compact")) {
                            val name = name()
                            create(app, name, backend)
                            val selected = CountDownLatch(1)
                            val resume = CountDownLatch(1)
                            val once = AtomicBoolean()
                            app.mixedRouter.selected = { e ->
                                if (e.name == name && once.compareAndSet(false, true)) {
                                    selected.countDown()
                                    check(resume.await(20, TimeUnit.SECONDS))
                                }
                            }
                            val stale =
                                async(Dispatchers.IO) {
                                    val path =
                                        when (operation) {
                                            "commit" -> "/v1/catalogs/$name/commit"
                                            "ddl" -> "/v1/catalogs/$name/namespaces"
                                            else -> "/v1/catalogs/$name/maintenance/$operation"
                                        }
                                    val payload =
                                        when (operation) {
                                            "ddl" -> """{"name":"stale"}"""
                                            "commit" ->
                                                """{"appends":[{"namespace":"ns","table":"t","files":[{
                                                "path":"s3://hoglake-it/$name/data.parquet",
                                                "record_count":1,"file_size_bytes":100,"footer_size":10}]}]}"""
                                            else -> "{}"
                                        }
                                    client.post(path) {
                                        contentType(ContentType.Application.Json)
                                        setBody(payload)
                                    }
                                }
                            try {
                                assertThat(
                                    withContext(Dispatchers.IO) { selected.await(10, TimeUnit.SECONDS) },
                                ).isTrue()
                                assertThat(
                                    client.delete("/v1/catalogs/$name?author=test&message=drop").status,
                                ).isEqualTo(HttpStatusCode.OK)
                                create(app, name, replacement)
                                if (operation == "commit") {
                                    app.store.createNamespace(name, "ns")
                                    app.store.createTable(name, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
                                }
                            } finally {
                                resume.countDown()
                            }
                            val answer = stale.await()
                            assertThat(
                                answer.status,
                            ).describedAs(operation + answer.bodyAsText()).isEqualTo(HttpStatusCode.NotFound)
                            assertThat(app.store.getCatalog(name).headSnapshotId)
                                .isEqualTo(if (operation == "commit") 2L else 0L)
                            assertThat(app.store.listNamespaces(name).map { it.name })
                                .containsExactlyElementsOf(if (operation == "commit") listOf("ns") else emptyList())
                            val newId = app.store.getCatalog(name).catalogId
                            assertThat(
                                if (replacement == "postgres") {
                                    db.jdbi.withHandleUnchecked { h ->
                                        h.createQuery("SELECT count(*) FROM hog_maintenance_run WHERE catalog_id = ?")
                                            .bind(0, newId).mapTo(Int::class.java).one()
                                    }
                                } else {
                                    engine.store.catalogOperation(name) {
                                        engine.store.queryCatalog(name, "SELECT count(*) FROM maintenance_runs")
                                            .firstRow()!![0].asLong().toInt()
                                    }
                                },
                            ).isZero()
                            app.mixedRouter.selected = {}
                        }
                    }
                }
            }
        }
    }

    @Test
    fun `nested PostgreSQL lookups keep the bound identity after recreation`() {
        val name = name()
        app().use { app ->
            val old = create(app, name, "postgres").catalogId
            app.store.dropCatalog(name, "test", "drop empty catalog")
            create(app, name, "postgres")
            CatalogBinding.withIdentity(name, old) {
                assertThatThrownBy { CatalogService(db.jdbi).createNamespace(name, "stale") }
                    .isInstanceOf(HoglakeException.NotFound::class.java)
                assertThatThrownBy {
                    com.posthog.hoglake.commit.CommitService(db.jdbi).commit(name, CommitRequest())
                }.isInstanceOf(HoglakeException.NotFound::class.java)
            }
            assertThat(app.store.listNamespaces(name)).isEmpty()
            assertThat(app.store.getCatalog(name).headSnapshotId).isZero()
        }
    }

    @Test
    fun `a paused PostgreSQL receipt request cannot read the replacement incarnation receipt`() {
        app().use { app ->
            testApplication {
                application { app.module(this) }
                val name = name()
                val old = create(app, name, "postgres")
                val operation = UUID.randomUUID()
                val selected = CountDownLatch(1)
                val resume = CountDownLatch(1)
                val once = AtomicBoolean()
                app.mixedRouter.selected = { entry ->
                    if (entry.name == name && once.compareAndSet(false, true)) {
                        check(entry.identity.toLong() == old.catalogId)
                        selected.countDown()
                        check(resume.await(20, TimeUnit.SECONDS))
                    }
                }
                coroutineScope {
                    val path = "/v1/catalogs/$name/commit/receipts/$operation"
                    val stale = async(Dispatchers.IO) { client.get(path) }
                    try {
                        assertThat(withContext(Dispatchers.IO) { selected.await(10, TimeUnit.SECONDS) }).isTrue()
                        app.store.dropCatalog(name, "test", "replace")
                        val fresh = create(app, name, "postgres")
                        assertThat(fresh.catalogId).isNotEqualTo(old.catalogId)
                        app.store.createNamespace(name, "ns")
                        app.store.createTable(name, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
                        val committed =
                            app.store.commit(
                                name,
                                CommitRequest(
                                    idempotencyKey = operation,
                                    appends =
                                        listOf(
                                            TableAppend(
                                                "ns",
                                                "t",
                                                listOf(
                                                    FileRegistration("s3://hoglake-it/$name/data.parquet", 1, 100, 10),
                                                ),
                                            ),
                                        ),
                                ),
                            )
                        val current = client.get(path)
                        assertThat(current.status).isEqualTo(HttpStatusCode.OK)
                        assertThat(json.readTree(current.bodyAsText())["snapshot_id"].asLong())
                            .isEqualTo(committed.snapshotId)
                        // Unbound legacy callers still resolve the current catalog by name.
                        assertThat(CommitService(db.jdbi).receipt(name, operation)).isEqualTo(committed)
                    } finally {
                        resume.countDown()
                    }
                    val answer = stale.await()
                    assertThat(answer.status).describedAs(answer.bodyAsText()).isEqualTo(HttpStatusCode.NotFound)
                }
            }
        }
    }

    @Test
    fun `all catalog sweep tasks use the directory incarnation for both backends`() {
        app().use { app ->
            val pg = name()
            val hg = name()
            create(app, pg, "postgres")
            create(app, hg)
            val entries = listOf(pg, hg).map { app.mixedRouter.directory.find(it)!! }
            val tasks =
                listOf(
                    MaintenanceTask.EXPIRY,
                    MaintenanceTask.CLEANUP,
                    MaintenanceTask.COMPACTION,
                    MaintenanceTask.RETIREMENT,
                    MaintenanceTask.HYDRATOR,
                )
            for (entry in entries) {
                for (task in tasks) app.mixedCatalogTask(entry, task)
                app.store.dropCatalog(entry.name, "test", "recreate")
                create(app, entry.name, entry.backend.wire)
                for (task in tasks.filter { it != MaintenanceTask.HYDRATOR }) {
                    assertThatThrownBy { app.mixedCatalogTask(entry, task) }
                        .describedAs("${entry.backend} $task")
                        .isInstanceOf(HoglakeException.NotFound::class.java)
                }
                // A PostgreSQL hydrator sweep uses explicit ids. A missing id produces no work.
                if (entry.backend == DatabaseBackend.POSTGRES) app.mixedCatalogTask(entry, MaintenanceTask.HYDRATOR)
                assertThat(app.store.getCatalog(entry.name).headSnapshotId).isZero()
            }
        }
    }

    @Test
    fun `summary sampling excludes unregistered and pending PostgreSQL identities`() {
        val unmanaged = name()
        val original = CatalogService(db.jdbi).createCatalog(unmanaged, "s3://hoglake-it/$unmanaged")
        app().use { app ->
            val pg = name()
            val id = create(app, pg, "postgres").catalogId
            val pending = name()
            app.mixedRouter.directory.beforePublish = {
                throw HoglakeException.BackendUnavailable("publication failed")
            }
            assertThatThrownBy {
                create(
                    app,
                    pending,
                    "postgres",
                )
            }.isInstanceOf(HoglakeException.BackendUnavailable::class.java)
            val pendingId = app.mixedRouter.directory.find(pending)!!.identity.toLong()
            app.mixedMaintenanceSummary()
            val sampled =
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery("SELECT catalog_id FROM hog_maintenance_summary").mapTo(Long::class.java).set()
                }
            assertThat(sampled).contains(id).doesNotContain(original.catalogId, pendingId)
            app.store.dropCatalog(pending, "test", "cancel reservation")
        }
    }

    @Test
    fun `a failed registry deletion keeps the old identity for a safe drop retry`() {
        val name = name()
        app().use { app ->
            val old = create(app, name, "postgres").catalogId
            app.mixedRouter.afterPostgresDrop = {
                throw HoglakeException.BackendUnavailable("registry deletion failed")
            }
            assertThatThrownBy { app.store.dropCatalog(name, "test", "drop") }
                .isInstanceOf(HoglakeException.BackendUnavailable::class.java)
            assertThat(app.mixedRouter.directory.find(name)!!.identity).isEqualTo(old.toString())
            assertThatThrownBy { create(app, name, "hoglite") }.isInstanceOf(HoglakeException.AlreadyExists::class.java)
        }
        app().use { app ->
            app.store.dropCatalog(name, "test", "retry drop")
            assertThat(app.mixedRouter.directory.find(name)).isNull()
            create(app, name, "hoglite")
            assertThat(app.mixedRouter.directory.find(name)!!.backend).isEqualTo(DatabaseBackend.HOGLITE)
        }
    }

    @Test
    fun `losing the lock connection cannot insert through another pooled connection`() {
        app().use { app ->
            val name = name()
            val independent = com.posthog.hoglake.Database.jdbi(db.dataSource)
            app.mixedRouter.beforePostgresInsert = {
                val pid =
                    db.jdbi.withHandleUnchecked {
                            h ->
                        h.createQuery("SELECT pg_backend_pid()").mapTo(Int::class.java).one()
                    }
                independent.withHandleUnchecked { h ->
                    h.createQuery("SELECT pg_terminate_backend(?)").bind(0, pid).mapTo(Boolean::class.java).one()
                }
            }
            assertThatThrownBy {
                create(
                    app,
                    name,
                    "postgres",
                )
            }.isInstanceOf(HoglakeException.BackendUnavailable::class.java)
            assertThat(app.mixedRouter.directory.find(name)!!.state).isEqualTo("creating")
            assertThat(db.jdbi.withHandleUnchecked { CatalogRepo.findByName(it, name) }).isNull()
            app.mixedRouter.beforePostgresInsert = {}
            app.store.dropCatalog(name, "test", "cancel failed create")
            create(app, name, "postgres")
            app.store.dropCatalog(name, "test", "prove lock release")
        }
    }

    @Test
    fun `lifecycle callbacks reuse the lock session in a one connection pool`() {
        val config =
            HikariConfig().apply {
                jdbcUrl = db.jdbcUrl
                username = db.dataSource.username
                password = db.dataSource.password
                maximumPoolSize = 1
                minimumIdle = 0
                connectionTimeout = 250
                initializationFailTimeout = -1
                connectionInitSql = Database.SESSION_INIT_SQL
            }
        HikariDataSource(config).use { pool ->
            val checkouts = AtomicInteger()
            val counted =
                object : DataSource by pool {
                    override fun getConnection(): java.sql.Connection {
                        checkouts.incrementAndGet()
                        return pool.connection
                    }
                }
            val scoped = Database.jdbi(counted)
            val cfg =
                engine.config().copy(
                    database = DatabaseBackend.MIXED,
                    hogliteUrl = engine.url(),
                    // Keep production's four-slot safety floor; inject a one-slot pool to prove handle reuse.
                    dbPoolSize = 4,
                    requestThreads = 1,
                    cleanupIntervalMs = 0,
                    compactionIntervalMs = 0,
                    reindexIntervalMs = 0,
                )
            App.buildMixed(cfg, scoped, engine.store, HealthProbe.forProbe(1_000) { true }).use { app ->
                app.mixedRouter.beforePostgresInsert = {
                    scoped.withHandleUnchecked { h ->
                        val held =
                            h.createQuery(
                                "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid = pg_backend_pid() " +
                                    "AND locktype = 'advisory' AND granted)",
                            ).mapTo(Boolean::class.java).one()
                        assertThat(held).isTrue()
                    }
                }
                for (backend in listOf("postgres", "hoglite")) {
                    val name = name()
                    var before = checkouts.get()
                    create(app, name, backend)
                    assertThat(checkouts.get() - before).isEqualTo(1)
                    before = checkouts.get()
                    app.store.dropCatalog(name, "test", "drop")
                    assertThat(checkouts.get() - before).isEqualTo(if (backend == "postgres") 1 else 0)
                }
            }
        }
    }

    @Test
    fun `a paused creator cannot mutate after lock loss cancellation and a replacement reservation`() {
        for (replacement in listOf("postgres", "hoglite")) {
            app().use { creator ->
                app().use { canceller ->
                    val name = name()
                    val pids = ArrayBlockingQueue<Int>(1)
                    val resume = CountDownLatch(1)
                    creator.mixedRouter.beforePostgresInsert = {
                        val pid =
                            db.jdbi.withHandleUnchecked { h ->
                                h.createQuery(
                                    "SELECT pg_backend_pid(), EXISTS(SELECT 1 FROM pg_locks " +
                                        "WHERE pid = pg_backend_pid() AND locktype = 'advisory' AND granted)",
                                ).map { rs, _ ->
                                    assertThat(rs.getBoolean(2)).isTrue()
                                    rs.getInt(1)
                                }.one()
                            }
                        pids.add(pid)
                        check(resume.await(20, TimeUnit.SECONDS))
                    }
                    val worker = Executors.newSingleThreadExecutor()
                    try {
                        val stale =
                            worker.submit<Result<Unit>> {
                                runCatching {
                                    create(creator, name, "postgres")
                                    Unit
                                }
                            }
                        val pid = pids.poll(10, TimeUnit.SECONDS)
                        assertThat(pid).isNotNull()
                        val old = creator.mixedRouter.directory.find(name)!!
                        Database.jdbi(db.dataSource).withHandleUnchecked { h ->
                            assertThat(
                                h.createQuery("SELECT pg_terminate_backend(?)").bind(0, pid)
                                    .mapTo(Boolean::class.java).one(),
                            ).isTrue()
                        }
                        canceller.store.dropCatalog(name, "test", "cancel terminated creator")
                        create(canceller, name, replacement)
                        val fresh = canceller.mixedRouter.directory.find(name)!!
                        assertThat(fresh.identity).isNotEqualTo(old.identity)
                        resume.countDown()
                        val result = stale.get(10, TimeUnit.SECONDS)
                        assertThat(
                            result.exceptionOrNull(),
                        ).isInstanceOf(HoglakeException.BackendUnavailable::class.java)
                        assertThat(canceller.mixedRouter.directory.find(name)).isEqualTo(fresh)
                        val row = db.jdbi.withHandleUnchecked { CatalogRepo.findByName(it, name) }
                        if (replacement == "postgres") {
                            assertThat(row!!.catalogId).isEqualTo(fresh.identity.toLong())
                        } else {
                            assertThat(row).isNull()
                        }
                        assertThat(canceller.store.getCatalog(name).headSnapshotId).isZero()
                    } finally {
                        resume.countDown()
                        worker.shutdownNow()
                    }
                }
            }
        }
    }

    @Test
    fun `session lock contention fails before reservation and releases after exceptions`() {
        app().use { app ->
            app.mixedRouter.directory.initialize()
            val name = name()
            val independent = com.posthog.hoglake.Database.jdbi(db.dataSource)
            independent.withHandleUnchecked { h ->
                val key =
                    h.createQuery(
                        "SELECT hashtextextended(?, 7249831)",
                    ).bind(0, name).mapTo(Long::class.java).one()
                h.createQuery("SELECT pg_advisory_lock(?)").bind(0, key).mapTo(String::class.java).one()
                try {
                    val start = System.nanoTime()
                    assertThatThrownBy {
                        create(
                            app,
                            name,
                            "postgres",
                        )
                    }.isInstanceOf(HoglakeException.BackendUnavailable::class.java)
                    assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)).isLessThan(3_000)
                    assertThat(app.mixedRouter.directory.find(name)).isNull()
                } finally {
                    h.createQuery("SELECT pg_advisory_unlock(?)").bind(0, key).mapTo(Boolean::class.java).one()
                }
            }
            create(app, name, "postgres")
            // A duplicate fails inside the lock. The next drop still acquires it.
            assertThatThrownBy { create(app, name, "hoglite") }.isInstanceOf(HoglakeException.AlreadyExists::class.java)
            app.store.dropCatalog(name, "test", "prove lock release")
        }
    }

    @Test
    fun `options HTTP refusal preserves the floor and uses the existing validation response`() {
        app().use { app ->
            testApplication {
                application { app.module(this) }
                for (backend in listOf("hoglite", "postgres")) {
                    val n = name()
                    create(app, n, backend)
                    val root = "/v1/catalogs/$n/options"
                    val initial =
                        client.patch(root) {
                            contentType(ContentType.Application.Json)
                            setBody("""{"snapshot_retention_seconds":100,"author":"test","message":"initial"}""")
                        }
                    assertThat(initial.status).isEqualTo(HttpStatusCode.OK)
                    val refused =
                        client.patch(root) {
                            contentType(ContentType.Application.Json)
                            setBody(
                                json.writeValueAsString(
                                    mapOf(
                                        "snapshot_retention_seconds" to 20,
                                        "consumer_floor" to false,
                                        "author" to "\u0085",
                                        "message" to "refuse",
                                    ),
                                ),
                            )
                        }
                    assertThat(refused.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
                    assertThat(json.readTree(refused.bodyAsText())["error"].asText()).isEqualTo("validation")
                    assertThat(json.readTree(refused.bodyAsText())["detail"].asText())
                        .isEqualTo("author is required and must not be blank")
                    val after = json.readTree(client.get(root).bodyAsText())
                    assertThat(after["snapshot_retention_seconds"].asLong()).isEqualTo(100)
                    assertThat(after["consumer_floor"].asBoolean()).isTrue()
                }
            }
        }
    }

    /** Test-local transport barriers preserve the real engine's SQL and response bodies. */
    private class DropProbe(private val upstream: String) : AutoCloseable {
        val seen = java.util.concurrent.CopyOnWriteArrayList<String>()
        var before: (String, String, String) -> Pair<Int, String>? = { _, _, _ -> null }
        var after: (String, String, String, Int, String) -> Unit = { _, _, _, _, _ -> }
        private val http = java.net.http.HttpClient.newHttpClient()
        private val pool = Executors.newCachedThreadPool()
        private val server = com.sun.net.httpserver.HttpServer.create(java.net.InetSocketAddress("127.0.0.1", 0), 0)

        init {
            server.executor = pool
            server.createContext("/") { ex ->
                try {
                    val method = ex.requestMethod
                    val path = ex.requestURI.toString()
                    val body = ex.requestBody.readBytes().decodeToString()
                    seen += "$method $path $body"
                    val injected = before(method, path, body)
                    val response =
                        if (injected != null) {
                            injected
                        } else {
                            val request =
                                java.net.http.HttpRequest.newBuilder(java.net.URI.create(upstream + path))
                                    .header("Content-Type", "application/json")
                                    .method(
                                        method,
                                        if (body.isEmpty()) {
                                            java.net.http.HttpRequest.BodyPublishers.noBody()
                                        } else {
                                            java.net.http.HttpRequest.BodyPublishers.ofString(body)
                                        },
                                    ).build()
                            val r = http.send(request, java.net.http.HttpResponse.BodyHandlers.ofString())
                            after(method, path, body, r.statusCode(), r.body())
                            r.statusCode() to r.body()
                        }
                    val bytes = response.second.toByteArray()
                    ex.responseHeaders.add("Content-Type", "application/json")
                    ex.sendResponseHeaders(response.first, if (bytes.isEmpty()) -1 else bytes.size.toLong())
                    if (bytes.isNotEmpty()) ex.responseBody.use { it.write(bytes) } else ex.close()
                } catch (_: Throwable) {
                    ex.sendResponseHeaders(599, -1)
                    ex.close()
                }
            }
            server.start()
        }

        fun client() = HogliteClient(standardUrl = "http://127.0.0.1:${server.address.port}")

        override fun close() {
            server.stop(0)
            pool.shutdownNow()
            http.close()
        }
    }

    private fun proxyApp(store: HogliteCatalogStore): App =
        App.buildMixed(
            engine.config().copy(database = DatabaseBackend.MIXED, hogliteUrl = engine.url()),
            db.jdbi,
            store,
            HealthProbe.forProbe(1_000) { true },
        )

    @Test
    fun `captured drop cleanup preserves hoglite and PostgreSQL replacements`() {
        app().use { current ->
            for ((backend, legacy, ghost) in listOf(
                Triple("hoglite", false, true),
                Triple("postgres", false, true),
                Triple("hoglite", true, true),
                Triple("hoglite", false, false),
                Triple("postgres", false, false),
            )) {
                DropProbe(engine.url()).use { probe ->
                    probe.client().use { client ->
                        val pinnedStore = HogliteCatalogStore(client)
                        proxyApp(pinnedStore).use { stale ->
                            val n = name()
                            create(current, n)
                            val created = current.mixedRouter.directory.find(n)!!
                            if (ghost) {
                                engine.client.release(created.identity)
                                engine.client.deleteTenant(created.identity)
                            }
                            if (legacy) {
                                engine.store.runTx(
                                    HogliteCatalogStore.REGISTRY_TENANT,
                                    UUID.randomUUID().toString(),
                                    null,
                                    HogliteTx().apply { add("UPDATE catalogs SET tenant = NULL WHERE name = ?", n) },
                                    fenced = false,
                                )
                            }
                            val old = current.mixedRouter.directory.find(n)!!
                            val fired = AtomicBoolean()
                            var replacement: CatalogDirectory.Entry? = null
                            probe.before = { method, path, body ->
                                if (method == "POST" && path.endsWith("/tx") && "DELETE FROM catalogs" in body &&
                                    n in body && fired.compareAndSet(false, true)
                                ) {
                                    current.store.dropCatalog(n, "test", "remove ghost")
                                    current.store.createCatalog(
                                        n, old.dataPath + "-replacement", backend = backend, connectionRef = backend,
                                    )
                                    replacement = current.mixedRouter.directory.find(n)
                                    current.store.createNamespace(n, "victim")
                                    current.store.createTable(n, "victim", "t", listOf(ColumnDef("id", ColType.LONG)))
                                    if (backend == "hoglite") {
                                        HogliteOptions(engine.store).patch(
                                            n, com.posthog.hoglake.service.PatchField.Absent, false,
                                        )
                                        pinnedStore.getCatalog(n)
                                    }
                                }
                                null
                            }
                            val result = stale.store.dropCatalog(n, "test", "captured cleanup")
                            assertThat(fired.get()).isTrue()
                            assertThat(result.dataPath).isEqualTo(old.dataPath)
                            assertThat(current.mixedRouter.directory.find(n)).isEqualTo(replacement)
                            val cleanupSql =
                                "DELETE FROM catalogs WHERE name = ? AND tenant IS ? AND data_path = ? " +
                                    "AND backend = ? AND connection_ref = ? AND state = ? AND operation = ?"
                            assertThat(probe.seen.any { cleanupSql in it }).isTrue()
                            if (backend == "hoglite") {
                                val beforeCachedLookup = probe.seen.size
                                assertThat(pinnedStore.resolveTenant(n)).isEqualTo(replacement!!.identity)
                                assertThat(probe.seen.size).isEqualTo(beforeCachedLookup)
                            }
                            assertThat(current.store.listNamespaces(n).map { it.name }).containsExactly("victim")
                            if (backend == "hoglite") {
                                assertThat(
                                    HogliteOptions(engine.store).get(n).consumerFloor,
                                ).isFalse()
                            }
                            assertThat(current.store.listTables(n, "victim").map { it.name }).containsExactly("t")
                            assertThat(
                                probe.seen.filter {
                                    ("/v1/t/${replacement!!.identity}/" in it && ("/release" in it || "/tx" in it)) ||
                                        it.startsWith("DELETE /v1/tenants/${replacement!!.identity} ")
                                },
                            ).isEmpty()
                        }
                    }
                }
            }
        }
    }

    @Test
    fun `a selected hoglite drop refuses a replacement before its metadata probe`() {
        app().use { current ->
            DropProbe(engine.url()).use { probe ->
                probe.client().use { transport ->
                    proxyApp(HogliteCatalogStore(transport)).use { stale ->
                        val n = name()
                        create(current, n)
                        val fired = AtomicBoolean()
                        var replacement: CatalogDirectory.Entry? = null
                        stale.mixedRouter.selected = { e ->
                            if (e.name == n && fired.compareAndSet(false, true)) {
                                current.store.dropCatalog(n, "test", "replace")
                                create(current, n)
                                replacement = current.mixedRouter.directory.find(n)
                                current.store.createNamespace(n, "victim")
                            }
                        }
                        assertThatThrownBy { stale.store.dropCatalog(n, "test", "stale") }
                            .isExactlyInstanceOf(HoglakeException.NotFound::class.java)
                        assertThat(current.mixedRouter.directory.find(n)).isEqualTo(replacement)
                        assertThat(current.store.listNamespaces(n).map { it.name }).containsExactly("victim")
                        assertThat(probe.seen.filter { "/v1/t/${replacement!!.identity}/" in it }).isEmpty()
                    }
                }
            }
        }
    }

    @Test
    fun `drop probe refuses unproven absence and a cold migration proxy404`() {
        app().use { current ->
            val n = name()
            create(current, n)
            val entry = current.mixedRouter.directory.find(n)!!
            DropProbe(engine.url()).use { probe ->
                probe.client().use { client ->
                    val store = HogliteCatalogStore(client)
                    proxyApp(store).use { candidate ->
                        // Warm this exact process before faults on the drop-private metadata probe.
                        store.getCatalog(n)
                        for ((status, body) in listOf(
                            404 to "{\"error\":\"proxy route missing\"}",
                            409 to "{\"error\":\"SQL refused\"}",
                            200 to "{\"cols\":[],\"rows\":[]}",
                            200 to "not-json",
                            503 to "{\"error\":\"unavailable\"}",
                        )) {
                            val count = AtomicInteger()
                            probe.before = { _, path, payload ->
                                if (path.endsWith("/${entry.identity}/query") && "FROM meta" in payload) {
                                    count.incrementAndGet()
                                    status to body
                                } else {
                                    null
                                }
                            }
                            val result = runCatching { candidate.store.dropCatalog(n, "test", "refuse") }
                            val expected =
                                when {
                                    status == 404 -> HoglakeException.NotFound::class.java
                                    body == "not-json" -> java.lang.reflect.UndeclaredThrowableException::class.java
                                    else -> HoglakeException.BackendUnavailable::class.java
                                }
                            assertThat(result.exceptionOrNull()).describedAs("status=$status body=$body")
                                .isExactlyInstanceOf(expected)
                            if (body == "not-json") {
                                assertThat(result.exceptionOrNull()!!.cause)
                                    .isExactlyInstanceOf(com.fasterxml.jackson.core.JsonParseException::class.java)
                            }
                            assertThat(count.get()).isGreaterThan(0)
                            assertThat(current.mixedRouter.directory.find(n)).isEqualTo(entry)
                        }
                    }
                    // A different store has no migration cache entry.
                    proxyApp(HogliteCatalogStore(client)).use { cold ->
                        val count = AtomicInteger()
                        probe.before = { _, path, body ->
                            if (path.endsWith("/${entry.identity}/query") && "sqlite_master" in body) {
                                count.incrementAndGet()
                                404 to "{\"error\":\"proxy route missing\"}"
                            } else {
                                null
                            }
                        }
                        assertThatThrownBy { cold.store.dropCatalog(n, "test", "cold") }
                            .isExactlyInstanceOf(HogliteRejected::class.java)
                        assertThat(count.get()).isEqualTo(1)
                        assertThat(current.mixedRouter.directory.find(n)).isEqualTo(entry)
                    }
                }
            }
        }
    }

    @Test
    fun `a failed registry cleanup leaves a recoverable captured ghost`() {
        app().use { current ->
            val n = name()
            create(current, n)
            val entry = current.mixedRouter.directory.find(n)!!
            DropProbe(engine.url()).use { probe ->
                probe.client().use { client ->
                    proxyApp(HogliteCatalogStore(client)).use { interrupted ->
                        probe.before = { _, path, body ->
                            if (path.endsWith("/tx") && "DELETE FROM catalogs" in body && n in body) {
                                422 to "{\"code\":\"not_applied\",\"error\":\"registry refusal\"}"
                            } else {
                                null
                            }
                        }
                        assertThatThrownBy { interrupted.store.dropCatalog(n, "test", "partial") }
                            .isExactlyInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
                        assertThat(current.mixedRouter.directory.find(n)).isEqualTo(entry)
                        val missing = runCatching { engine.client.query(entry.identity, "SELECT 1") }.exceptionOrNull()
                        assertThat(missing).isExactlyInstanceOf(HogliteRejected::class.java)
                        assertThat((missing as HogliteRejected).status).isEqualTo(404)
                        assertThat(missing.body).contains("tenant does not exist")
                        probe.before = { _, _, _ -> null }
                        val result = interrupted.store.dropCatalog(n, "test", "retry")
                        assertThat(result.dataPath).isEqualTo(entry.dataPath)
                        assertThat(current.mixedRouter.directory.find(n)).isNull()
                    }
                }
            }
        }
    }

    @Test
    fun `mixed ghost drop recovers with a warm schema cache`() = recoverGhost(false)

    @Test
    fun `mixed ghost drop recovers with a cold schema cache`() = recoverGhost(true)

    private fun recoverGhost(cold: Boolean) {
        app().use { creator ->
            DropProbe(engine.url()).use { probe ->
                probe.client().use { transport ->
                    val store = HogliteCatalogStore(transport)
                    proxyApp(store).use { recovered ->
                        val n = name()
                        create(creator, n)
                        val entry = creator.mixedRouter.directory.find(n)!!
                        if (!cold) store.getCatalog(n)
                        engine.client.release(entry.identity)
                        engine.client.deleteTenant(entry.identity)
                        probe.seen.clear()
                        var response: Pair<HttpStatusCode, String>? = null
                        var repeated: HttpStatusCode? = null
                        testApplication {
                            application { recovered.module(this) }
                            val first = client.delete("/v1/catalogs/$n?author=test&message=retry")
                            response = first.status to first.bodyAsText()
                            repeated = client.delete("/v1/catalogs/$n?author=test&message=again").status
                        }
                        val (status, text) = checkNotNull(response)
                        assertThat(status).describedAs("cold=$cold $text").isEqualTo(HttpStatusCode.OK)
                        val body = json.readTree(text)
                        assertThat(body["name"].asText()).isEqualTo(n)
                        assertThat(body["data_path"].asText()).isEqualTo(entry.dataPath)
                        assertThat(recovered.mixedRouter.directory.find(n)).isNull()
                        assertThat(probe.seen.count { "/${entry.identity}/query" in it && "sqlite_master" in it })
                            .isEqualTo(if (cold) 1 else 0)
                        assertThat(repeated).isEqualTo(HttpStatusCode.NotFound)
                    }
                }
            }
        }
    }

    @Test
    fun `a stale PostgreSQL drop cannot delete a replacement directory entry`() {
        app().use { app ->
            val name = name()
            create(app, name, "postgres")
            val selected = CountDownLatch(1)
            val resume = CountDownLatch(1)
            val once = AtomicBoolean()
            app.mixedRouter.selected = { e ->
                if (e.name == name && once.compareAndSet(false, true)) {
                    selected.countDown()
                    check(resume.await(20, TimeUnit.SECONDS))
                }
            }
            val pool = Executors.newSingleThreadExecutor()
            try {
                val stale =
                    pool.submit<Result<Any>> {
                        runCatching {
                            app.store.dropCatalog(
                                name,
                                "test",
                                "stale drop",
                            )
                        }
                    }
                assertThat(selected.await(10, TimeUnit.SECONDS)).isTrue()
                app.store.dropCatalog(name, "test", "current drop")
                create(app, name, "hoglite")
                resume.countDown()
                assertThat(
                    stale.get(20, TimeUnit.SECONDS).exceptionOrNull(),
                ).isInstanceOf(HoglakeException.NotFound::class.java)
                assertThat(app.mixedRouter.directory.find(name)!!.backend).isEqualTo(DatabaseBackend.HOGLITE)
                assertThat(app.store.getCatalog(name).name).isEqualTo(name)
            } finally {
                resume.countDown()
                pool.shutdownNow()
            }
        }
    }
}
