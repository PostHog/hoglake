package com.posthog.hoglake.api

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.auth.installAuth
import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.observability.CatalogTotals
import com.posthog.hoglake.store.CatalogStore
import com.posthog.hoglake.store.CatalogStoreSelector
import com.sun.net.httpserver.HttpServer
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.lang.reflect.Proxy
import java.net.InetSocketAddress
import java.nio.file.Files
import java.nio.file.Path
import java.security.interfaces.ECPublicKey
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

class CatalogListingBackendTotalsTest {
    @TempDir
    lateinit var dir: Path

    @Test
    fun `a routed hoglite listing never inherits sampler totals from a same-name PostgreSQL catalog`() {
        val json = jacksonObjectMapper()
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        val queries = AtomicInteger()
        val result =
            mapOf(
                "cols" to List(8) { "c$it" },
                "rows" to listOf(listOf("lake", "s3://bucket/hoglite", 7, 2, 0, null, null, 1)),
                "affected" to 0,
            )
        server.createContext("/v1/query-batch") { exchange ->
            exchange.requestBody.use { it.readBytes() }
            queries.incrementAndGet()
            val answer = json.writeValueAsBytes(mapOf("results" to listOf(mapOf("status" to 200, "body" to result))))
            exchange.sendResponseHeaders(200, answer.size.toLong())
            exchange.responseBody.use { it.write(answer) }
        }
        server.start()
        try {
            testApplication {
                val id = UUID.fromString("5b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e")
                val file = dir.resolve("backends.json")
                Files.writeString(
                    file,
                    """{"backends":[{"id":"$id","kind":"hoglite","url":"http://127.0.0.1:${server.address.port}"}]}""",
                )
                val postgresId = "6b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e"
                val cfg =
                    Config(
                        backendsFile = file.toString(),
                        defaultBackend = "postgres",
                        authMode = "enforce",
                        postgresBackendId = postgresId,
                    )
                val pair = TestTokens.keyPair()
                val postgresCatalog = CatalogInfo(1, "lake", "s3://bucket/postgres", 11, 3)
                val postgres =
                    Proxy.newProxyInstance(
                        CatalogStore::class.java.classLoader,
                        arrayOf(CatalogStore::class.java),
                    ) { _, method, _ ->
                        check(method.name == "listCatalogs") { "unexpected PostgreSQL operation: ${method.name}" }
                        listOf(postgresCatalog)
                    } as CatalogStore
                val sampled = CatalogTotals(13, 1234, 5678, Instant.parse("2026-10-01T00:00:00Z"))
                val samplesRead = AtomicInteger()
                application {
                    installAuth(
                        AuthService(
                            AuthMode.ENFORCE,
                            JwtVerifier(
                                TestTokens.ISSUER,
                                TestTokens.AUDIENCE,
                                StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey),
                                backends = cfg.backendKinds,
                                clock = { 1_700_000_001 },
                            ),
                        ),
                    )
                    install(ContentNegotiation) { jackson { configureHoglakeWire() } }
                    install(StatusPages) { installErrorMapping() }
                    installApiRoutes(CatalogStoreSelector(postgres, cfg), catalogTotals = {
                        samplesRead.incrementAndGet()
                        mapOf(postgresCatalog.name to sampled)
                    })
                }
                val legacy = TestTokens.mint(pair, TestTokens.operatorClaims())
                val postgresClaims =
                    TestTokens.operatorClaims().dropLast(1) +
                        """, "cat":{"lake":["admin"]}, "rtg":{"b":"postgres","be":"$postgresId"}}"""
                val postgresRouted = TestTokens.mint(pair, postgresClaims)
                for (token in listOf(legacy, postgresRouted)) {
                    val pgResponse = client.get("/v1/catalogs") { header(HttpHeaders.Authorization, "Bearer $token") }
                    assertThat(pgResponse.status).isEqualTo(HttpStatusCode.OK)
                    val pg = json.readTree(pgResponse.bodyAsText()).single()
                    assertThat(pg["data_path"].asText()).isEqualTo(postgresCatalog.dataPath)
                    assertThat(pg["table_count"].asLong()).isEqualTo(sampled.tableCount)
                    assertThat(pg["live_rows"].asLong()).isEqualTo(sampled.liveRows)
                    assertThat(pg["live_size_bytes"].asLong()).isEqualTo(sampled.liveBytes)
                    assertThat(pg["oldest_snapshot_time"].asText()).isEqualTo(sampled.oldestSnapshotTime.toString())
                }
                assertThat(samplesRead.get()).isEqualTo(2)
                val claims =
                    TestTokens.operatorClaims().dropLast(1) +
                        """, "cat":{"lake":["admin"]}, "rtg":{"b":"hoglite","be":"$id","t":"opaque_1"}}"""
                val routed = TestTokens.mint(pair, claims)
                val hogliteResponse = client.get("/v1/catalogs") { header(HttpHeaders.Authorization, "Bearer $routed") }
                assertThat(hogliteResponse.status).isEqualTo(HttpStatusCode.OK)
                val hoglite = json.readTree(hogliteResponse.bodyAsText()).single()
                assertThat(hoglite["name"].asText()).isEqualTo(postgresCatalog.name)
                assertThat(hoglite["data_path"].asText()).isEqualTo("s3://bucket/hoglite")
                for (field in listOf("table_count", "live_rows", "live_size_bytes", "oldest_snapshot_time")) {
                    assertThat(hoglite.has(field)).describedAs("hoglite must omit PostgreSQL $field").isFalse()
                }
                assertThat(samplesRead.get()).isEqualTo(2)
                assertThat(queries.get()).isEqualTo(1)
            }
        } finally {
            server.stop(0)
        }
    }
}
