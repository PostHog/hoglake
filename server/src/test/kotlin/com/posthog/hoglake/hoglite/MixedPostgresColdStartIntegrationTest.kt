package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Database
import com.posthog.hoglake.DatabaseBackend
import com.posthog.hoglake.HealthProbe
import com.posthog.hoglake.MixedPostgres
import com.posthog.hoglake.configureApiListener
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.testing.PgTestSupport
import com.sun.net.httpserver.HttpServer
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.sql.Connection
import java.sql.SQLException
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource

/** Precise cold-initialization fault; not a warmed-PG bulkhead or unbounded-load guarantee. */
@Tag("integration")
class MixedPostgresColdStartIntegrationTest {
    private class BlockedSource(private val delegate: DataSource) : DataSource by delegate {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val checkouts = AtomicInteger()
        val failure = SQLException("test cold initializer checkout failure")

        override fun getConnection(): Connection {
            if (checkouts.incrementAndGet() == 1) {
                entered.countDown()
                check(release.await(30, TimeUnit.SECONDS)) { "test leader not released" }
                throw failure
            }
            return delegate.connection
        }

        override fun getConnection(
            user: String?,
            password: String?,
        ): Connection = connection
    }

    private fun <T> progress(future: Future<T>): T =
        try {
            future.get(5, TimeUnit.SECONDS)
        } catch (e: TimeoutException) {
            throw AssertionError("follower or healthy hoglite must finish BEFORE cold leader release", e)
        }

    private fun caught(body: () -> Unit): Throwable? =
        try {
            body()
            null
        } catch (e: Throwable) {
            e
        }

    private fun fixture(wire: Boolean) {
        HogliteConformanceEngine().use { engine ->
            PgTestSupport.freshDatabase().use { db ->
                val id = UUID.randomUUID().toString().replace("-", "")
                val hg = "coldh$id"
                val pg = "coldp$id"
                val fresh = "coldn$id"
                val cfg =
                    engine.config().copy(
                        database = DatabaseBackend.MIXED,
                        hogliteUrl = engine.url(),
                        dbPoolSize = 8,
                        requestThreads = 2,
                        port = 0,
                    )
                App.buildMixed(cfg, db.jdbi, engine.store, HealthProbe.forProbe(1_000) { true }).use { setup ->
                    setup.store.createCatalog(hg, "s3://hoglake-it/$hg", backend = "hoglite")
                    setup.store.createCatalog(pg, "s3://hoglake-it/$pg", backend = "postgres")
                }
                val source = BlockedSource(db.dataSource)
                val initializer = MixedPostgres(source)
                val clients = Executors.newFixedThreadPool(if (wire) 3 else 2)
                val proxyWorkers = Executors.newFixedThreadPool(2)
                val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
                val tenantCreates = AtomicInteger()
                val proxy =
                    HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
                        executor = proxyWorkers
                        createContext("/") { ex ->
                            val raw = ex.requestBody.readBytes()
                            if (ex.requestMethod == "PUT" && ex.requestURI.path.startsWith("/v1/tenants/")) {
                                tenantCreates.incrementAndGet()
                            }
                            val answer =
                                http.send(
                                    HttpRequest.newBuilder(URI.create(engine.url() + ex.requestURI))
                                        .header("Content-Type", "application/json")
                                        .method(ex.requestMethod, HttpRequest.BodyPublishers.ofByteArray(raw)).build(),
                                    HttpResponse.BodyHandlers.ofByteArray(),
                                )
                            ex.sendResponseHeaders(answer.statusCode(), answer.body().size.toLong())
                            ex.responseBody.use { it.write(answer.body()) }
                        }
                        start()
                    }
                val engineClient = HogliteClient(standardUrl = "http://127.0.0.1:${proxy.address.port}")
                try {
                    App.buildMixed(
                        cfg,
                        Database.jdbi(source),
                        HogliteCatalogStore(engineClient),
                        HealthProbe.forProbe(1_000) { true },
                        initializer::ensureReady,
                    ).use { app ->
                        val entries = app.mixedRouter.directory.all()
                        val entry = entries.single { it.name == hg }
                        assertThat(entry.backend).isEqualTo(DatabaseBackend.HOGLITE)
                        // Logical->physical identity is read verbatim from the actual directory, never inferred.
                        val originalHead =
                            engine.client.query(
                                entry.identity,
                                "SELECT next_snapshot - 1 FROM meta",
                            ).firstRow()!![0].asLong()
                        app.store.getCatalog(hg)
                        assertThat(source.checkouts.get()).isZero()
                        val beforeCreates = tenantCreates.get()

                        fun pgRows() =
                            db.jdbi.withHandle<List<Map<String, Any?>>, RuntimeException> { h ->
                                h.createQuery(
                                    "SELECT catalog_id, name, last_snapshot_id FROM hog_catalog ORDER BY catalog_id",
                                )
                                    .mapToMap().list()
                            }
                        val beforePg = pgRows()
                        if (!wire) {
                            val leader = clients.submit<Throwable?> { caught { app.store.getCatalog(pg) } }
                            try {
                                assertThat(source.entered.await(10, TimeUnit.SECONDS)).isTrue()
                                val follower =
                                    progress(clients.submit<Throwable?> { caught { app.store.getCatalog(pg) } })
                                assertThat(follower).isInstanceOf(HoglakeException.BackendUnavailable::class.java)
                                val names =
                                    progress(
                                        clients.submit<List<String>> {
                                            app.store.createNamespace(hg, "healthy")
                                            assertThat(app.store.getCatalog(hg).name).isEqualTo(hg)
                                            app.store.listCatalogs(setOf(hg)).map { it.name }
                                        },
                                    )
                                assertThat(names).containsExactly(hg)
                                assertThat(source.release.count).isEqualTo(1)
                                assertThat(source.checkouts.get()).isEqualTo(1)
                            } finally {
                                source.release.countDown()
                                val error = leader.get(30, TimeUnit.SECONDS)
                                assertThat(error).isInstanceOf(HoglakeException.BackendUnavailable::class.java)
                                assertThat(error!!.cause).isSameAs(source.failure)
                            }
                        } else {
                            val server =
                                embeddedServer(Netty, configure = { configureApiListener(cfg) }) { app.module(this) }
                            server.start(wait = false)
                            val port = runBlocking { server.engine.resolvedConnectors().single().port }

                            fun request(
                                path: String,
                                body: String? = null,
                            ): HttpResponse<String> =
                                http.send(
                                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port$path"))
                                        .timeout(Duration.ofSeconds(20))
                                        .header("Content-Type", "application/json")
                                        .apply {
                                            if (body == null) GET() else POST(HttpRequest.BodyPublishers.ofString(body))
                                        }.build(),
                                    HttpResponse.BodyHandlers.ofString(),
                                )

                            fun refused(answer: HttpResponse<String>) {
                                assertThat(answer.statusCode()).isEqualTo(503)
                                assertThat(
                                    jacksonObjectMapper().readTree(answer.body())["error"].asText(),
                                ).isEqualTo("backend_unavailable")
                                assertThat(answer.headers().firstValue("Retry-After")).isPresent()
                                assertThat(answer.headers().firstValue("X-Request-Id")).isPresent()
                            }
                            val pending = clients.submit<HttpResponse<String>> { request("/v1/catalogs/$pg") }
                            try {
                                assertThat(source.entered.await(10, TimeUnit.SECONDS)).isTrue()
                                refused(progress(clients.submit<HttpResponse<String>> { request("/v1/catalogs/$pg") }))
                                val healthy =
                                    progress(clients.submit<HttpResponse<String>> { request("/v1/catalogs/$hg") })
                                assertThat(healthy.statusCode()).isEqualTo(200)
                                val mutation =
                                    progress(
                                        clients.submit<HttpResponse<String>> {
                                            request("/v1/catalogs/$hg/namespaces", """{"name":"healthy"}""")
                                        },
                                    )
                                assertThat(mutation.statusCode()).isEqualTo(201)
                                refused(progress(clients.submit<HttpResponse<String>> { request("/v1/catalogs") }))
                                refused(
                                    progress(
                                        clients.submit<HttpResponse<String>> {
                                            request(
                                                "/v1/catalogs",
                                                """{"name":"$fresh","data_path":"s3://hoglake-it/$fresh",""" +
                                                    """"backend":"hoglite"}""",
                                            )
                                        },
                                    ),
                                )
                                assertThat(app.store.listCatalogs(setOf(hg)).map { it.name }).containsExactly(hg)
                                assertThat(source.release.count).isEqualTo(1)
                                assertThat(source.checkouts.get()).isEqualTo(1)
                                assertThat(tenantCreates.get()).isEqualTo(beforeCreates)
                                assertThat(app.mixedRouter.directory.find(fresh)).isNull()
                                assertThat(pgRows()).isEqualTo(beforePg)
                            } finally {
                                source.release.countDown()
                                try {
                                    refused(pending.get(30, TimeUnit.SECONDS))
                                } finally {
                                    server.stop(0, 5, TimeUnit.SECONDS)
                                }
                            }
                        }
                        assertThat(app.mixedRouter.directory.all()).isEqualTo(entries)
                        assertThat(tenantCreates.get()).isEqualTo(beforeCreates)
                        assertThat(pgRows()).isEqualTo(beforePg)
                        assertThat(
                            engine.client.query(
                                entry.identity,
                                "SELECT next_snapshot - 1 FROM meta",
                            ).firstRow()!![0].asLong(),
                        )
                            .isEqualTo(originalHead + 1)
                        assertThat(app.store.listNamespaces(hg).map { it.name }).contains("healthy")
                        // Recover on the same source/initializer: admission reuse, not partial migration repair.
                        assertThat(app.store.getCatalog(pg).name).isEqualTo(pg)
                        val afterRecovery = source.checkouts.get()
                        initializer.ensureReady()
                        assertThat(source.checkouts.get()).isEqualTo(afterRecovery)
                    }
                } finally {
                    source.release.countDown()
                    clients.shutdownNow()
                    assertThat(clients.awaitTermination(10, TimeUnit.SECONDS)).isTrue()
                    engineClient.close()
                    proxy.stop(0)
                    proxyWorkers.shutdownNow()
                    http.close()
                    Metrics.clear()
                }
            }
        }
    }

    @Test
    fun `constrained router workers serve existing hoglite before cold leader release`() = fixture(false)

    @Test
    fun `Netty cold postgres followers refuse while existing hoglite completes before release`() = fixture(true)
}
