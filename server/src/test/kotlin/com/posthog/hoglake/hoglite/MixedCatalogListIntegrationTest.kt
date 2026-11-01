package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.DatabaseBackend
import com.posthog.hoglake.HealthProbe
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.testing.PgTestSupport
import com.sun.net.httpserver.HttpServer
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.jdbi.v3.core.statement.SqlLogger
import org.jdbi.v3.core.statement.StatementContext
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/** Counts actual engine HTTP requests and PostgreSQL statements for a roster larger than either batch. */
@Tag("integration")
class MixedCatalogListIntegrationTest {
    private val json = jacksonObjectMapper()

    private class Counts {
        val registry = AtomicInteger()
        val batches = AtomicInteger()
        val items = AtomicInteger()
        val tenantReads = AtomicInteger()
        val other = AtomicInteger()
        val postgres = AtomicInteger()

        fun reset() {
            listOf(registry, batches, items, tenantReads, other, postgres).forEach { it.set(0) }
        }
    }

    @Test
    fun `cold and warm mixed HTTP lists batch 260 catalogs per backend and retain selected incarnations`() {
        HogliteConformanceEngine().use { engine ->
            PgTestSupport.freshDatabase().use { db ->
                val cfg = engine.config().copy(database = DatabaseBackend.MIXED, hogliteUrl = engine.url())
                App.buildMixed(cfg, db.jdbi, engine.store, HealthProbe.forProbe(1_000) { true }).use { setup ->
                    setup.mixedRouter.directory.initialize()
                    for (i in 0 until 260) {
                        val name = "h%04d".format(i)
                        engine.store.createCatalog(name, "s3://hoglake-it/$name")
                    }
                    val pg =
                        db.jdbi.withHandleUnchecked { h ->
                            h.createQuery(
                                """
                                INSERT INTO hog_catalog(name, data_path)
                                SELECT 'p' || to_char(g, 'FM0000'), 's3://hoglake-it/p' || to_char(g, 'FM0000')
                                FROM generate_series(0, 259) g
                                RETURNING catalog_id, name, data_path
                                """,
                            ).map { rs, _ ->
                                Triple(rs.getLong("catalog_id"), rs.getString("name"), rs.getString("data_path"))
                            }.list()
                        }
                    engine.client.tx(
                        engine.store.registryTenantId,
                        UUID.randomUUID().toString(),
                        null,
                        pg.map { (id, name, path) ->
                            HogliteStatement(
                                "INSERT INTO catalogs(name, data_path, tenant, backend, connection_ref, " +
                                    "state, operation) " +
                                    "VALUES (?, ?, ?, 'postgres', 'postgres', 'ready', ?)",
                                listOf(name, path, id.toString(), "fixture-$id"),
                            )
                        },
                    )
                    val counts = Counts()
                    db.jdbi.setSqlLogger(
                        object : SqlLogger {
                            override fun logBeforeExecution(context: StatementContext) {
                                check(context.rawSql.contains("WHERE catalog_id = ANY(:ids)"))
                                counts.postgres.incrementAndGet()
                            }
                        },
                    )
                    val proxy = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
                    val executor = Executors.newFixedThreadPool(4)
                    proxy.executor = executor
                    val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()
                    val afterRoster = AtomicReference<() -> Unit>({})
                    proxy.createContext("/") { ex ->
                        val raw = ex.requestBody.readBytes()
                        val path = ex.requestURI.path
                        when {
                            path == "/v1/t/${engine.store.registryTenantId}/query" -> counts.registry.incrementAndGet()
                            path == "/v1/query-batch" -> {
                                counts.batches.incrementAndGet()
                                counts.items.addAndGet(json.readTree(raw)["queries"].size())
                            }
                            path.endsWith("/query") -> counts.tenantReads.incrementAndGet()
                            else -> counts.other.incrementAndGet()
                        }
                        val answer =
                            http.send(
                                HttpRequest.newBuilder(URI.create(engine.url() + ex.requestURI))
                                    .timeout(Duration.ofSeconds(20))
                                    .header("Content-Type", "application/json")
                                    .method(ex.requestMethod, HttpRequest.BodyPublishers.ofByteArray(raw))
                                    .build(),
                                HttpResponse.BodyHandlers.ofByteArray(),
                            )
                        if (path == "/v1/t/${engine.store.registryTenantId}/query" &&
                            json.readTree(raw)["sql"].asText().startsWith("SELECT name, data_path, tenant, backend")
                        ) {
                            afterRoster.get().invoke()
                        }
                        ex.sendResponseHeaders(answer.statusCode(), answer.body().size.toLong())
                        ex.responseBody.use { it.write(answer.body()) }
                    }
                    proxy.start()
                    val routedClient =
                        HogliteClient(
                            standardUrl = "http://127.0.0.1:${proxy.address.port}",
                            retryBudgetMs = 2_000,
                        )
                    try {
                        val coldStore =
                            HogliteCatalogStore(
                                routedClient,
                            ).apply { requireOperationBindingForTests = true }
                        App.buildMixed(cfg, db.jdbi, coldStore, HealthProbe.forProbe(1_000) { true }).use { app ->
                            testApplication {
                                application { app.module(this) }
                                for (cold in listOf(true, false)) {
                                    counts.reset()
                                    val response = client.get("/v1/catalogs")
                                    assertThat(response.status).isEqualTo(HttpStatusCode.OK)
                                    assertThat(json.readTree(response.bodyAsText())).hasSize(520)
                                    // Cold: one registry schema read plus four directory-column probes.
                                    // Both lists: one directory page, two metadata batches, two PG ID batches.
                                    assertThat(counts.registry.get()).isEqualTo(if (cold) 6 else 1)
                                    assertThat(counts.batches.get()).isEqualTo(2)
                                    assertThat(counts.items.get()).isEqualTo(260)
                                    assertThat(counts.postgres.get()).isEqualTo(2)
                                    assertThat(counts.tenantReads.get()).isZero()
                                    assertThat(counts.other.get()).isZero()
                                }
                                // Return the old directory snapshot, then replace both backend incarnations.
                                db.jdbi.setSqlLogger(SqlLogger.NOP_SQL_LOGGER)
                                afterRoster.set {
                                    afterRoster.set {}
                                    for ((name, backend) in listOf("h0000" to "hoglite", "p0000" to "postgres")) {
                                        setup.store.dropCatalog(name, "test", "replace")
                                        setup.store.createCatalog(
                                            name,
                                            "s3://hoglake-it/recreated-$name",
                                            backend = backend,
                                        )
                                    }
                                }
                                val stale = json.readTree(client.get("/v1/catalogs").bodyAsText())
                                assertThat(stale.map { it["name"].asText() }).doesNotContain("h0000", "p0000")
                                val fresh = json.readTree(client.get("/v1/catalogs").bodyAsText())
                                for (name in listOf("h0000", "p0000")) {
                                    assertThat(fresh.single { it["name"].asText() == name }["data_path"].asText())
                                        .isEqualTo("s3://hoglake-it/recreated-$name")
                                }
                            }
                        }
                    } finally {
                        routedClient.close()
                        proxy.stop(0)
                        executor.shutdownNow()
                        http.close()
                    }
                }
            }
        }
        Metrics.clear()
    }
}
