package com.posthog.hoglake.auth

import com.posthog.hoglake.Config
import com.posthog.hoglake.api.installApiRoutes
import com.posthog.hoglake.api.installErrorMapping
import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.model.NamespaceInfo
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.store.CatalogStore
import com.posthog.hoglake.store.CatalogStoreSelector
import com.sun.net.httpserver.HttpServer
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
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
import java.util.concurrent.CopyOnWriteArrayList

/** Owned HTTP fixtures exercise the real verifier, gate, selector, and existing API route. */
class ScopedNamespaceDispatchTest {
    @TempDir
    lateinit var dir: Path

    private val id = UUID.fromString("5b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e")
    private val otherId = UUID.fromString("6b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e")
    private val postgresId = UUID.fromString("7b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e")
    private val pair = TestTokens.keyPair()
    private val path = "/v1/catalogs/lake/namespaces/reports"
    private val success = """{"namespace_id":17,"name":"reports","containment":{"field":"team_id","value":42}}"""
    private val requests = CopyOnWriteArrayList<Pair<String, String?>>()
    private val legacy = CopyOnWriteArrayList<String>()
    private var server: HttpServer? = null

    @Volatile
    private var status = 200

    @Volatile
    private var body = success

    @AfterEach
    fun stop() {
        server?.stop(0)
        Metrics.clear()
    }

    private fun config(enabled: Boolean = true): Config {
        val peer = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server = peer
        peer.createContext("/") { exchange ->
            exchange.requestBody.readBytes()
            val request = "${exchange.requestMethod} ${exchange.requestURI}"
            requests.add(request to exchange.requestHeaders.getFirst("Authorization"))
            exchange.responseHeaders.set("Content-Type", "application/json")
            val bytes = body.toByteArray()
            exchange.sendResponseHeaders(status, bytes.size.toLong())
            exchange.responseBody.use { it.write(bytes) }
        }
        peer.start()
        val backends =
            listOf(id, otherId).joinToString(",") {
                """{"id":"$it","kind":"hoglite","url":"http://127.0.0.1:${peer.address.port}"}"""
            }
        val backendsFile = Files.writeString(dir.resolve("backends.json"), """{"backends":[$backends]}""")
        val binding =
            """{"catalog":"lake","backend":"hoglite","backend_id":"$id","tenant":"opaque_1","namespace":"reports"}"""
        val bindingFile = Files.writeString(dir.resolve("binding.json"), binding)
        return Config(
            backendsFile = backendsFile.toString(),
            scopedNamespaceFile = if (enabled) bindingFile.toString() else "",
            postgresBackendId = postgresId.toString(),
            authMode = "enforce",
            authJwksFile = "mounted-public-jwks.json",
            authPredicateFields = "team_id",
        )
    }

    private fun Application.harness(
        cfg: Config,
        guardLegacy: Boolean = true,
        audience: String = TestTokens.AUDIENCE,
    ) {
        val postgres =
            Proxy.newProxyInstance(
                CatalogStore::class.java.classLoader,
                arrayOf(CatalogStore::class.java),
            ) { _, method, args ->
                legacy.add(method.name)
                check(!guardLegacy) { "legacy store must not run: ${method.name}" }
                when (method.name) {
                    "liveContainment" -> NamespaceContainment("team_id", 42)
                    "getNamespace" -> NamespaceInfo(99, args!![1] as String, NamespaceContainment("team_id", 42))
                    else -> error("unexpected legacy method: ${method.name}")
                }
            } as CatalogStore
        val selector = CatalogStoreSelector(postgres, cfg)
        installAuth(
            AuthService(
                AuthMode.ENFORCE,
                JwtVerifier(
                    TestTokens.ISSUER,
                    audience,
                    StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey),
                    predicateFields = setOf("team_id"),
                    backends = cfg.backendKinds,
                ) { 1_700_000_100L },
            ),
        )
        installNamespaceGate(selector)
        install(ContentNegotiation) { jackson { configureHoglakeWire() } }
        install(StatusPages) { installErrorMapping() }
        installApiRoutes(selector)
    }

    private fun claims(
        backend: UUID = id,
        tenant: String = "opaque_1",
        catalog: String = "lake",
        prd: String = """{"team_id":{"42":"read"}}""",
    ): String =
        TestTokens.scopedClaims(
            catalogs = """{"$catalog":["read"]}""",
            prd = prd,
            extra = ""","rtg":{"b":"hoglite","be":"$backend","t":"$tenant"}""",
        )

    private fun token(claims: String = claims()): String = TestTokens.mint(pair, claims)

    @Test
    fun `the exact route forwards the exact verified JWT without legacy reads`() =
        testApplication {
            val cfg = config()
            val jwt = token(claims().replace(""""aud":"hoglake"""", """"aud":["hoglake","hoglite"]"""))
            application { harness(cfg) }
            val response = client.get(path) { header(HttpHeaders.Authorization, "Bearer $jwt") }
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            assertThat(response.bodyAsText()).contains("\"namespace_id\":17", "\"name\":\"reports\"")
            assertThat(requests).containsExactly(
                "GET /v1/t/opaque_1/metadata/v1/catalogs/lake/namespaces/reports" to "Bearer $jwt",
            )
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `the selected route rejects other authority without fallback`() =
        testApplication {
            val cfg = config()
            application { harness(cfg) }
            val noPlacement = TestTokens.scopedClaims(catalogs = """{"lake":["read"]}""")
            val postgresPlacement = noPlacement.dropLast(1) + ""","rtg":{"b":"postgres","be":"$postgresId"}}"""
            for (claims in listOf(
                claims(backend = otherId),
                claims(tenant = "other_tenant"),
                claims(catalog = "other"),
                claims().dropLast(1) + """, "storage": {"fixture": true}}""",
                noPlacement,
                postgresPlacement,
            )) {
                val response = client.get(path) { header(HttpHeaders.Authorization, "Bearer ${token(claims)}") }
                assertThat(response.status).isEqualTo(HttpStatusCode.NotFound)
            }
            assertThat(requests).isEmpty()
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `JWT failures stop before typed or legacy dispatch`() =
        testApplication {
            val cfg = config()
            application { harness(cfg) }
            val invalid = TestTokens.mint(TestTokens.keyPair(), claims())
            val expired = token(claims().replace("\"exp\":1700000900", "\"exp\":1700000001"))
            val wrongAudience = token(claims().replace(""""aud":"hoglake"""", """"aud":"elsewhere""""))
            for (jwt in listOf("", invalid, expired, wrongAudience)) {
                val response =
                    client.get(
                        path,
                    ) { if (jwt.isNotEmpty()) header(HttpHeaders.Authorization, "Bearer $jwt") }
                assertThat(response.status).isEqualTo(HttpStatusCode.Unauthorized)
            }
            assertThat(requests).isEmpty()
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `a supported miss or rejected mark returns one refusal without legacy reads`() =
        testApplication {
            val cfg = config()
            val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
            Metrics.bind(registry)
            application { harness(cfg) }
            status = 404
            body = """{"code":"not_found"}"""
            val missing = client.get(path) { header(HttpHeaders.Authorization, "Bearer ${token()}") }
            status = 200
            body = success.replace("\"value\":42", "\"value\":43")
            val rejected = client.get(path) { header(HttpHeaders.Authorization, "Bearer ${token()}") }
            assertThat(missing.status).isEqualTo(HttpStatusCode.NotFound)
            assertThat(rejected.status).isEqualTo(HttpStatusCode.NotFound)
            assertThat(rejected.bodyAsText()).isEqualTo(missing.bodyAsText()).doesNotContain("43")
            val count = registry.find("hoglake_auth_requests_total").tag("outcome", "not_entitled").counter()
            assertThat(count?.count()).isEqualTo(2.0)
            assertThat(requests).hasSize(2)
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `unsupported outcomes cannot trigger a legacy retry or disclose peer bodies`() =
        testApplication {
            val cfg = config()
            application { harness(cfg) }
            for ((code, peerBody) in listOf(
                404 to """{"code":"different","secret":"peer-secret"}""",
                501 to """{"code":"unsupported","secret":"peer-secret"}""",
                503 to """{"code":"unavailable","secret":"peer-secret"}""",
                200 to success.replace("\"reports\"", "\"wrong_name\""),
                200 to success.replace("\"team_id\"", "\"unknown_field\""),
                200 to success.replace("\"value\":42", "\"value\":0"),
                200 to success.replace("\"namespace_id\":17", "\"namespace_id\":0"),
                200 to """{"namespace_id":17,"name":"reports"}""",
            )) {
                status = code
                body = peerBody
                val response = client.get(path) { header(HttpHeaders.Authorization, "Bearer ${token()}") }
                assertThat(response.status).isEqualTo(HttpStatusCode.ServiceUnavailable)
                assertThat(response.bodyAsText()).doesNotContain("peer-secret", "wrong_name", "unknown_field")
            }
            assertThat(requests).hasSize(8)
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `the typed route requires a canonical path without a body or query`() =
        testApplication {
            val cfg = config()
            application { harness(cfg) }
            for (target in listOf("$path?snapshot=1", "$path?", path.replace("reports", "%72eports"))) {
                val response = client.get(target) { header(HttpHeaders.Authorization, "Bearer ${token()}") }
                assertThat(response.status).isEqualTo(HttpStatusCode.BadRequest)
            }
            val withBody =
                client.get(path) {
                    header(HttpHeaders.Authorization, "Bearer ${token()}")
                    setBody("x")
                }
            assertThat(withBody.status).isEqualTo(HttpStatusCode.BadRequest)
            assertThat(requests).isEmpty()
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `a different namespace keeps scoped legacy behavior`() =
        testApplication {
            val cfg = config()
            application { harness(cfg, guardLegacy = false) }
            val jwt = token(TestTokens.scopedClaims(catalogs = """{"lake":["read"]}"""))
            val response =
                client.get(
                    "/v1/catalogs/lake/namespaces/other",
                ) { header(HttpHeaders.Authorization, "Bearer $jwt") }
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            assertThat(response.bodyAsText()).contains("\"namespace_id\":99")
            assertThat(legacy).contains("liveContainment", "getNamespace")
            assertThat(requests).isEmpty()
        }

    @Test
    fun `an absent binding leaves the exact legacy route intact`() =
        testApplication {
            val cfg = config(enabled = false)
            application { harness(cfg, guardLegacy = false) }
            val jwt = token(TestTokens.scopedClaims(catalogs = """{"lake":["read"]}"""))
            val response = client.get(path) { header(HttpHeaders.Authorization, "Bearer $jwt") }
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            assertThat(response.bodyAsText()).contains("\"namespace_id\":99")
            assertThat(legacy).contains("liveContainment", "getNamespace")
            assertThat(requests).isEmpty()
        }

    @Test
    fun `tenant and operator callers retain the legacy namespace handler`() =
        testApplication {
            val cfg = config()
            application { harness(cfg, guardLegacy = false) }
            for (claims in listOf(
                TestTokens.tenantClaims(),
                TestTokens.operatorClaims(),
            )) {
                val response = client.get(path) { header(HttpHeaders.Authorization, "Bearer ${token(claims)}") }
                assertThat(response.status).isEqualTo(HttpStatusCode.OK)
                assertThat(response.bodyAsText()).contains("\"namespace_id\":99")
            }
            assertThat(legacy).containsExactly("getNamespace", "getNamespace")
            assertThat(requests).isEmpty()
        }

    @Test
    fun `typed routing preserves the configured audience membership rule`() =
        testApplication {
            val cfg = config()
            application { harness(cfg, audience = "custom-metadata") }
            val jwt =
                token(claims().replace(""""aud":"hoglake"""", """"aud":["other","custom-metadata","hoglite"]"""))
            val response = client.get(path) { header(HttpHeaders.Authorization, "Bearer $jwt") }
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            assertThat(requests.single().second).isEqualTo("Bearer $jwt")
            assertThat(legacy).isEmpty()
        }
}
