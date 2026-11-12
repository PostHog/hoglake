package com.posthog.hoglake.api

import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.auth.installAuth
import com.posthog.hoglake.auth.installNamespaceGate
import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.model.NamespaceInfo
import com.posthog.hoglake.service.ReservedNamespaces
import com.posthog.hoglake.store.CatalogStore
import com.posthog.hoglake.store.CatalogStoreSelector
import com.sun.net.httpserver.HttpServer
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.contentType
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.IOException
import java.lang.reflect.Proxy
import java.net.InetSocketAddress
import java.nio.file.Files
import java.nio.file.Path
import java.security.interfaces.ECPublicKey
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.readText
import kotlin.io.path.walk

/**
 * The reserved-name guard on `POST .../namespaces`, through the real auth
 * plugin, namespace gate and routes, for both backends: a PostgreSQL store
 * that counts every call, and a hoglite placement whose engine is a local
 * stub that counts every request. A refusal reaches neither.
 */
class ReservedNamespaceRoutesTest {
    @TempDir
    lateinit var dir: Path
    private val pair = TestTokens.keyPair()
    private val cat = "lake"
    private val hogliteId = UUID.fromString("5b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e")
    private val postgresId = UUID.fromString("7b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e")

    /** PostgreSQL: answers createNamespace, counts every call. */
    private val pgCalls = AtomicInteger()
    private val postgres =
        Proxy.newProxyInstance(CatalogStore::class.java.classLoader, arrayOf(CatalogStore::class.java)) { _, m, a ->
            pgCalls.incrementAndGet()
            check(m.name == "createNamespace") { "unexpected store call ${m.name}" }
            NamespaceInfo(1, a!![1] as String)
        } as CatalogStore

    /** The hoglite engine: a stub in this task's port range that counts and refuses every request. */
    private val engineRequests = AtomicInteger()
    private val engine: HttpServer = bindEngine()

    private fun bindEngine(): HttpServer {
        for (port in (51000..51999).shuffled().take(50)) {
            try {
                return HttpServer.create(InetSocketAddress("127.0.0.1", port), 0).apply {
                    createContext("/") { ex ->
                        engineRequests.incrementAndGet()
                        ex.sendResponseHeaders(503, -1)
                        ex.close()
                    }
                    start()
                }
            } catch (_: IOException) {
                continue
            }
        }
        error("no free port in 51000-51999")
    }

    @AfterEach
    fun stop() = engine.stop(0)

    private fun config(): Config {
        val file = dir.resolve("backends.json")
        val url = "http://127.0.0.1:${engine.address.port}"
        Files.writeString(
            file,
            """{"backends":[{"id":"$hogliteId","kind":"hoglite","url":"$url","retry_budget_ms":300}]}""",
        )
        return Config(
            backendsFile = file.toString(),
            defaultBackend = "postgres",
            postgresBackendId = postgresId.toString(),
            authMode = "enforce",
        )
    }

    private fun operator() = TestTokens.mint(pair, TestTokens.operatorClaims())

    private fun tenant() = TestTokens.mint(pair, TestTokens.tenantClaims(catalogs = mapOf(cat to listOf("admin"))))

    private fun scoped() =
        TestTokens.mint(pair, TestTokens.scopedClaims(catalogs = """{"$cat":["write"]}""", iat = 1_700_000_000))

    private val toHoglite = """"rtg":{"b":"hoglite","be":"$hogliteId","t":"t_1"}"""

    private fun routedOperator() =
        TestTokens.mint(
            pair,
            TestTokens.operatorClaims().dropLast(1) + ""","cat":{"$cat":["admin"]},$toHoglite}""",
        )

    private fun routedTenant() =
        TestTokens.mint(
            pair,
            TestTokens.tenantClaims(catalogs = mapOf(cat to listOf("admin"))).dropLast(1) + ",$toHoglite}",
        )

    private fun routes(
        mode: AuthMode,
        reserved: ReservedNamespaces,
        block: suspend ApplicationTestBuilder.() -> Unit,
    ) = testApplication {
        val cfg = config()
        val selector = CatalogStoreSelector(postgres, cfg)
        application {
            val verifier =
                if (mode == AuthMode.OFF) {
                    null
                } else {
                    JwtVerifier(
                        TestTokens.ISSUER,
                        TestTokens.AUDIENCE,
                        StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey),
                        predicateFields = setOf("team_id"),
                        backends = cfg.backendKinds,
                        clock = { 1_700_000_001 },
                    )
                }
            installAuth(AuthService(mode, verifier))
            installNamespaceGate(selector)
            install(ContentNegotiation) { jackson { configureHoglakeWire() } }
            install(StatusPages) { installErrorMapping() }
            installApiRoutes(selector, reservedNamespaces = reserved)
        }
        block()
    }

    private suspend fun ApplicationTestBuilder.create(
        token: String?,
        name: String,
    ): HttpResponse =
        client.post("/v1/catalogs/$cat/namespaces") {
            token?.let { header(HttpHeaders.Authorization, "Bearer $it") }
            contentType(ContentType.Application.Json)
            setBody("""{"name":"$name"}""")
        }

    private suspend fun assertReserved(
        response: HttpResponse,
        what: String,
    ) {
        val body = response.bodyAsText()
        assertThat(response.status.value).describedAs("%s: %s", what, body).isEqualTo(422)
        assertThat(body).describedAs(what)
            .contains("\"error\":\"validation\"")
            .contains("is reserved on this server")
    }

    private val posthog = ReservedNamespaces(listOf("posthog"))
    private val variants = listOf("posthog", "PostHog", "POSTHOG", "pOsThOg")

    @Test
    fun `every caller kind is refused on PostgreSQL before the store is called`() {
        routes(AuthMode.ENFORCE, posthog) {
            for ((who, token) in listOf("operator" to operator(), "tenant" to tenant())) {
                for (name in variants) assertReserved(create(token, name), "$who $name")
            }
            // A scoped token never reaches the handler: POST .../namespaces is not a scoped route.
            assertThat(create(scoped(), "posthog").status.value).isEqualTo(403)
            assertThat(pgCalls.get()).isZero()
            // Other names reach the store, for the same callers.
            for (name in listOf("posthog_raw", "posthog-events", "my_posthog", "posthog2", "Post_Hog")) {
                assertThat(create(operator(), name).status.value).describedAs(name).isEqualTo(201)
                assertThat(create(tenant(), name).status.value).describedAs(name).isEqualTo(201)
            }
            assertThat(pgCalls.get()).isEqualTo(10)
        }
        routes(AuthMode.OFF, posthog) {
            for (name in variants) assertReserved(create(null, name), "auth off $name")
            assertThat(pgCalls.get()).isEqualTo(10)
        }
    }

    @Test
    fun `every caller kind is refused on a hoglite placement before the engine is contacted`() {
        routes(AuthMode.ENFORCE, posthog) {
            for ((who, token) in listOf("operator" to routedOperator(), "tenant" to routedTenant())) {
                for (name in variants) assertReserved(create(token, name), "$who $name")
            }
            assertThat(engineRequests.get()).isZero()
            // The same placement with another name does reach the engine (its stub answers 503).
            val allowed = create(routedOperator(), "posthog_raw")
            assertThat(allowed.status.value).describedAs(allowed.bodyAsText()).isEqualTo(503)
            assertThat(engineRequests.get()).describedAs(allowed.bodyAsText()).isPositive()
            assertThat(pgCalls.get()).isZero()
        }
    }

    @Test
    fun `unset reserves nothing`() {
        routes(AuthMode.ENFORCE, ReservedNamespaces.NONE) {
            for (name in variants) assertThat(create(tenant(), name).status.value).describedAs(name).isEqualTo(201)
            assertThat(pgCalls.get()).isEqualTo(variants.size)
        }
        routes(AuthMode.OFF, ReservedNamespaces.NONE) {
            assertThat(create(null, "posthog").status.value).isEqualTo(201)
        }
    }

    @Test
    fun `the namespace route is the only caller of a store's createNamespace`() {
        // A textual tripwire, not a proof: it catches the usual ways a second
        // creation path appears (a new call of a store's createNamespace, a new
        // writer of the namespace rows on either backend). Gradle runs tests in server/.
        val main = Path.of("src/main/kotlin")
        assertThat(main.resolve("com/posthog/hoglake/api/Routes.kt")).exists()

        fun filesMatching(pattern: String) =
            main.walk().filter { it.toString().endsWith(".kt") }
                .filter { Regex(pattern).containsMatchIn(it.readText()) }
                .map { main.relativize(it).toString() }
                .toSortedSet()
        assertThat(filesMatching("""(\.|::)createNamespace\b""")).containsExactly(
            "com/posthog/hoglake/api/Routes.kt",
            "com/posthog/hoglake/store/PostgresCatalogStore.kt",
        )
        // The row writers: PostgreSQL's from CatalogService.createNamespace only, hoglite's in its store only.
        assertThat(filesMatching("""NamespaceRepo\.insert\(""")).containsExactly(
            "com/posthog/hoglake/service/CatalogService.kt",
        )
        assertThat(filesMatching("""(?i)INSERT\s+INTO\s+(hog_namespace|namespaces)\b""")).containsExactly(
            "com/posthog/hoglake/hoglite/HogliteCatalogStore.kt",
            "com/posthog/hoglake/persistence/NamespaceRepo.kt",
        )
        val routes = main.resolve("com/posthog/hoglake/api/Routes.kt").readText()
        assertThat(Regex("""\.createNamespace\(""").findAll(routes).count()).isEqualTo(1)
        val guard = routes.indexOf("reservedNamespaces.refuse(req.name)")
        assertThat(guard).isPositive().isLessThan(routes.indexOf("catalogs.createNamespace("))
    }
}
