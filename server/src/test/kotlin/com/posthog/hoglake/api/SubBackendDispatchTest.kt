package com.posthog.hoglake.api

import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.auth.installAuth
import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.service.AlterBackend
import com.posthog.hoglake.service.PartitionListingBackend
import com.posthog.hoglake.service.PartitionLookupBackend
import com.posthog.hoglake.service.PartitionStatsBackend
import com.posthog.hoglake.service.TableCreationBackend
import com.posthog.hoglake.service.UploadBackend
import com.posthog.hoglake.service.ViewBackend
import com.posthog.hoglake.store.CatalogStore
import com.posthog.hoglake.store.CatalogStoreSelector
import com.sun.net.httpserver.HttpServer
import io.ktor.client.request.header
import io.ktor.client.request.request
import io.ktor.client.request.setBody
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
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.lang.reflect.Proxy
import java.net.InetSocketAddress
import java.nio.file.Files
import java.nio.file.Path
import java.security.interfaces.ECPublicKey
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

/** Each route family must use verified placement before it calls a PostgreSQL service. */
class SubBackendDispatchTest {
    @TempDir
    lateinit var dir: Path

    private val id = UUID.fromString("5b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e")
    private val pair = TestTokens.keyPair()

    private inline fun <reified T> postgres(): T =
        Proxy.newProxyInstance(T::class.java.classLoader, arrayOf(T::class.java)) { _, _, _ ->
            throw HoglakeException.NotFound("postgres-dispatch-probe")
        } as T

    private data class Probe(val method: HttpMethod, val path: String, val body: String? = null)

    private val operation = "00000000-0000-0000-0000-000000000001"
    private val owner = """{"owner":"$operation"}"""
    private val probes =
        listOf(
            Probe(HttpMethod.Post, "/uploads/renew", owner),
            Probe(HttpMethod.Get, "/table-creations/$operation"),
            Probe(
                HttpMethod.Post,
                "/namespaces/ns/tables/t/alter",
                """{"ops":[{"op":"add_column","column":{"name":"extra","type":"long"}}]}""",
            ),
            Probe(HttpMethod.Get, "/namespaces/ns/views"),
            Probe(HttpMethod.Get, "/stats/partitions"),
            Probe(HttpMethod.Get, "/namespaces/ns/tables/t/partitions"),
            Probe(HttpMethod.Post, "/namespaces/ns/tables/t/partitions/lookup?spec_id=1", "[null]"),
        )

    @Test
    fun `all route families dispatch routed calls to the engine and retain the PostgreSQL fallback`() =
        dispatch(
            "postgres",
        )

    @Test
    fun `all route families refuse other catalogs without a default backend`() = dispatch("none")

    private fun dispatch(default: String) {
        val engineCalls = AtomicInteger()
        val server =
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
        try {
            testApplication {
                val file = dir.resolve("backends.json")
                Files.writeString(
                    file,
                    """{"backends":[{"id":"$id","kind":"hoglite","url":"http://127.0.0.1:${server.address.port}"}]}""",
                )
                val cfg = Config(backendsFile = file.toString(), defaultBackend = default, authMode = "enforce")
                val selector = CatalogStoreSelector(postgres<CatalogStore>(), cfg)
                val claims =
                    TestTokens.operatorClaims().dropLast(1) +
                        """, "cat":{"lake":["admin"]}, "rtg":{"b":"hoglite","be":"$id","t":"opaque_1"}}"""
                val routedToken = TestTokens.mint(pair, claims)
                val legacyToken = TestTokens.mint(pair, TestTokens.operatorClaims())
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
                    installUploadRoutes(postgres<UploadBackend>(), selector)
                    installTableCreationRoutes(postgres<TableCreationBackend>(), selector)
                    installAlterRoutes(postgres<AlterBackend>(), selector)
                    installViewRoutes(postgres<ViewBackend>(), selector)
                    installPartitionStatsRoutes(postgres<PartitionStatsBackend>(), selector)
                    installPartitionListingRoutes(postgres<PartitionListingBackend>(), selector)
                    installPartitionLookupRoutes(postgres<PartitionLookupBackend>(), selector)
                }
                for (probe in probes) {
                    val response =
                        client.request("/v1/catalogs/lake${probe.path}") {
                            method = probe.method
                            header(HttpHeaders.Authorization, "Bearer $routedToken")
                            probe.body?.let {
                                contentType(ContentType.Application.Json)
                                setBody(it)
                            }
                        }
                    assertThat(response.status).describedAs(probe.path).isEqualTo(HttpStatusCode.UnprocessableEntity)
                    assertThat(response.bodyAsText()).contains("engine-dispatch-probe")
                    val before = engineCalls.get()
                    for ((catalog, token) in listOf("other" to routedToken, "lake" to legacyToken)) {
                        val fallback =
                            client.request("/v1/catalogs/$catalog${probe.path}") {
                                method = probe.method
                                header(HttpHeaders.Authorization, "Bearer $token")
                                probe.body?.let {
                                    contentType(ContentType.Application.Json)
                                    setBody(it)
                                }
                            }
                        assertThat(fallback.status).describedAs(probe.path).isEqualTo(HttpStatusCode.NotFound)
                        if (default == "postgres") {
                            assertThat(fallback.bodyAsText()).contains("postgres-dispatch-probe")
                        } else {
                            assertThat(fallback.bodyAsText()).doesNotContain("postgres-dispatch-probe")
                        }
                        assertThat(engineCalls.get()).isEqualTo(before)
                    }
                }
                assertThat(engineCalls.get()).isEqualTo(probes.size)
            }
        } finally {
            server.stop(0)
        }
    }
}
