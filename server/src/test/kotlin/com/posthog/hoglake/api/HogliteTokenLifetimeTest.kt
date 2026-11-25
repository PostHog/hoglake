package com.posthog.hoglake.api

import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.auth.installAuth
import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.service.CleanupBackend
import com.posthog.hoglake.service.CompactionBackend
import com.posthog.hoglake.service.ExpiryBackend
import com.posthog.hoglake.service.HydratorMaintenanceBackend
import com.posthog.hoglake.service.OptionsBackend
import com.posthog.hoglake.service.RemovalStore
import com.posthog.hoglake.service.UploadBackend
import com.posthog.hoglake.store.CatalogStore
import com.posthog.hoglake.store.CatalogStoreSelector
import com.sun.net.httpserver.HttpServer
import io.ktor.client.request.header
import io.ktor.client.request.request
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.lang.reflect.Proxy
import java.net.InetSocketAddress
import java.nio.file.Files
import java.nio.file.Path
import java.security.interfaces.ECPublicKey
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

/**
 * hoglite verifies the forwarded token on every call, so a long-running
 * route refuses, before any backend call, a token with too little lifetime
 * left at hoglite for the operation (#680). PostgreSQL and the other routes
 * are unaffected.
 */
class HogliteTokenLifetimeTest {
    @TempDir
    lateinit var dir: Path

    private val id = UUID.fromString("5b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e")
    private val pair = TestTokens.keyPair()
    private val engineCalls = AtomicInteger()

    /** Answers every hoglite call with a refusal, counting it. */
    private val engine =
        HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/") { exchange ->
                engineCalls.incrementAndGet()
                exchange.requestBody.close()
                val bytes = """{"code":"refused","error":"engine-dispatch-probe"}""".toByteArray()
                exchange.sendResponseHeaders(422, bytes.size.toLong())
                exchange.responseBody.use { it.write(bytes) }
            }
            start()
        }

    @AfterEach
    fun stopEngine() = engine.stop(0)

    private inline fun <reified T> postgres(): T =
        Proxy.newProxyInstance(T::class.java.classLoader, arrayOf(T::class.java)) { _, _, _ ->
            throw HoglakeException.NotFound("postgres-dispatch-probe")
        } as T

    private val longRunning =
        listOf(
            HttpMethod.Post to "/v1/catalogs/lake/maintenance/expire",
            HttpMethod.Post to "/v1/catalogs/lake/maintenance/cleanup",
            HttpMethod.Post to "/v1/catalogs/lake/maintenance/compact",
            HttpMethod.Post to "/v1/catalogs/lake/maintenance/rehydrate",
            HttpMethod.Delete to "/v1/catalogs/lake",
            HttpMethod.Post to "/v1/catalogs/lake/uploads/schedule-expired",
        )

    /** A routed operator token whose `exp` leaves [left] seconds at hoglite, its leeway included. */
    private fun routed(left: Long): String =
        TestTokens.mint(
            pair,
            TestTokens.operatorClaims(exp = NOW + left - LEEWAY).dropLast(1) +
                """, "cat":{"lake":["admin"]}, "rtg":{"b":"hoglite","be":"$id","t":"opaque_1"}}""",
        )

    private fun app(
        minimum: Long = 300,
        mode: AuthMode = AuthMode.ENFORCE,
        body: suspend ApplicationTestBuilder.() -> Unit,
    ) = testApplication {
        val file = dir.resolve("backends.json")
        Files.writeString(
            file,
            """{"backends":[{"id":"$id","kind":"hoglite","url":"http://127.0.0.1:${engine.address.port}"}]}""",
        )
        val cfg =
            Config(
                backendsFile = file.toString(),
                authMode = mode.wire,
                hogliteMinTokenLifetimeSeconds = minimum,
            )
        // Never asked: cleanup and compaction read hoglite first.
        val objects = ObjectStore("http://127.0.0.1:1", "us-east-1", "key", "secret", pathStyle = true)
        val removals = RemovalStore("http://127.0.0.1:1", "us-east-1", "key", "secret", pathStyle = true)
        val selector =
            CatalogStoreSelector(
                postgres<CatalogStore>(),
                cfg,
                objectStore = { objects },
                removalStore = { removals },
            )
        application {
            installAuth(
                AuthService(
                    mode,
                    JwtVerifier(
                        TestTokens.ISSUER,
                        TestTokens.AUDIENCE,
                        StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey),
                        backends = cfg.backendKinds,
                        clock = { NOW },
                    ),
                ),
            )
            install(ContentNegotiation) { jackson { configureHoglakeWire() } }
            install(StatusPages) { installErrorMapping() }
            installApiRoutes(selector)
            installMaintenanceRoutes(
                { postgres<OptionsBackend>() },
                { postgres<ExpiryBackend>() },
                { postgres<CleanupBackend>() },
                { postgres<CompactionBackend>() },
                { postgres<HydratorMaintenanceBackend>() },
                { error("maintenance status is not under test") },
                { error("database health is not under test") },
                selector,
            )
            installUploadRoutes(postgres<UploadBackend>(), selector)
        }
        body()
    }

    private suspend fun ApplicationTestBuilder.send(
        route: Pair<HttpMethod, String>,
        token: String,
    ): HttpResponse =
        client.request(route.second) {
            method = route.first
            header(HttpHeaders.Authorization, "Bearer $token")
        }

    @Test
    fun `a long-running hoglite route refuses a token under the minimum before any backend call`() =
        app {
            val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
            Metrics.bind(registry)
            for (route in longRunning) {
                val response = send(route, routed(left = 299))
                assertThat(response.status).describedAs(route.second).isEqualTo(HttpStatusCode.Unauthorized)
                assertThat(response.headers[HttpHeaders.WWWAuthenticate])
                    .describedAs(route.second)
                    .isEqualTo("Bearer error=\"token_expired\"")
                assertThat(response.bodyAsText()).contains("\"token_expired\"").contains("nothing ran")
            }
            assertThat(engineCalls.get()).isZero()
            // Counted as the refusal it is, though the plugin let the request through.
            val refused = registry.find("hoglake_auth_requests_total").tag("outcome", "invalid").counter()
            assertThat(refused?.count()).isEqualTo(longRunning.size.toDouble())
        }

    @Test
    fun `warn counts a token under the minimum and lets it through, as it does every refusal`() =
        app(mode = AuthMode.WARN) {
            val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
            Metrics.bind(registry)
            for (route in longRunning) {
                val before = engineCalls.get()
                val response = send(route, routed(left = 299))
                assertThat(response.status).describedAs(route.second).isNotEqualTo(HttpStatusCode.Unauthorized)
                assertThat(engineCalls.get()).describedAs(route.second).isGreaterThan(before)
            }
            val refused = registry.find("hoglake_auth_requests_total").tag("outcome", "invalid").counter()
            assertThat(refused?.count()).isEqualTo(longRunning.size.toDouble())
        }

    @Test
    fun `a token with exactly the minimum left at hoglite reaches the backend`() =
        app {
            for (route in longRunning) {
                val before = engineCalls.get()
                val response = send(route, routed(left = 300))
                assertThat(response.status).describedAs(route.second).isNotEqualTo(HttpStatusCode.Unauthorized)
                assertThat(engineCalls.get()).describedAs(route.second).isGreaterThan(before)
            }
        }

    @Test
    fun `a short-lived token still reaches the backend on a route that runs one transaction`() =
        app {
            val response =
                client.request("/v1/catalogs/lake/uploads/renew") {
                    method = HttpMethod.Post
                    header(HttpHeaders.Authorization, "Bearer ${routed(left = 1)}")
                    contentType(ContentType.Application.Json)
                    setBody("""{"owner":"00000000-0000-0000-0000-000000000001"}""")
                }
            assertThat(response.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
            assertThat(engineCalls.get()).isEqualTo(1)
            // The drop's pattern under another method: the route key is both.
            val read = send(HttpMethod.Get to "/v1/catalogs/lake", routed(left = 1))
            assertThat(read.status).isNotEqualTo(HttpStatusCode.Unauthorized)
            assertThat(engineCalls.get()).isEqualTo(2)
        }

    @Test
    fun `a minimum of 0 disables the check`() =
        app(minimum = 0) {
            for (route in longRunning) {
                val before = engineCalls.get()
                val response = send(route, routed(left = 1))
                assertThat(response.status).describedAs(route.second).isNotEqualTo(HttpStatusCode.Unauthorized)
                assertThat(engineCalls.get()).describedAs(route.second).isGreaterThan(before)
            }
        }

    @Test
    fun `PostgreSQL checks the token once and is unaffected`() =
        app {
            // No `rtg`: the default PostgreSQL backend serves the catalog.
            val legacy = TestTokens.mint(pair, TestTokens.operatorClaims(exp = NOW + 1))
            for (route in longRunning) {
                val response = send(route, legacy)
                assertThat(response.status).describedAs(route.second).isEqualTo(HttpStatusCode.NotFound)
                assertThat(response.bodyAsText()).contains("postgres-dispatch-probe")
            }
            assertThat(engineCalls.get()).isZero()
        }

    private companion object {
        const val NOW = 1_700_000_001L

        /** hoglite's leeway past `exp`, which the check counts as lifetime left. */
        const val LEEWAY = 60L
    }
}
