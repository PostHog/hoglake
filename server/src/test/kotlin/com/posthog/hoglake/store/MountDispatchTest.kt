package com.posthog.hoglake.store

import com.posthog.hoglake.Config
import com.posthog.hoglake.api.installApiRoutes
import com.posthog.hoglake.api.installErrorMapping
import com.posthog.hoglake.api.installScanRoutes
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.Caller
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.Routing
import com.posthog.hoglake.auth.Scope
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.auth.installAuth
import com.posthog.hoglake.auth.installNamespaceGate
import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.observability.Metrics
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.lang.reflect.Proxy
import java.nio.file.Files
import java.nio.file.Path
import java.security.interfaces.ECPublicKey
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

/**
 * Mount dispatch through the real auth plugin, namespace gate and catalog
 * routes, over a PostgreSQL store that fails the test if any route reaches it.
 */
class MountDispatchTest {
    @TempDir
    lateinit var dir: Path
    private val pair = TestTokens.keyPair()
    private val mount = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f0-events"
    private val hogliteId = UUID.fromString("5b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e")
    private val postgresId = UUID.fromString("7b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e")
    private val reached = AtomicInteger()
    private val postgres =
        Proxy.newProxyInstance(CatalogStore::class.java.classLoader, arrayOf(CatalogStore::class.java)) { _, m, _ ->
            reached.incrementAndGet()
            throw IllegalStateException("a route reached PostgreSQL: ${m.name}")
        } as CatalogStore

    private fun config(default: String = "postgres"): Config {
        val file = dir.resolve("backends.json")
        val backend = """"id":"$hogliteId","kind":"hoglite","url":"http://127.0.0.1:9","retry_budget_ms":1"""
        Files.writeString(file, """{"backends":[{$backend}]}""")
        return Config(
            backendsFile = file.toString(),
            defaultBackend = default,
            postgresBackendId = if (default == "postgres") postgresId.toString() else "",
            authMode = "enforce",
        )
    }

    private val oneMount = MountRegistry { if (it == mount) MountRecord(it) else null }

    @AfterEach
    fun unbindMetrics() = Metrics.clear()

    /** `name`'s counts by the values of [tag]; one series per value. */
    private fun SimpleMeterRegistry.counts(
        name: String,
        tag: String,
    ): Map<String, Double> =
        find(name).counters().groupBy { it.id.getTag(tag)!! }.mapValues { (_, c) -> c.sumOf { it.count() } }

    private fun mounts(
        switchOn: () -> Boolean = { true },
        registry: MountRegistry = oneMount,
    ) = MountRegistry.behindSwitch(switchOn, registry)

    private fun scoped(
        scope: String = "read",
        catalog: String = mount,
        rtg: String = "",
    ): String =
        TestTokens.mint(
            pair,
            TestTokens.scopedClaims(
                catalogs = """{"$catalog":["$scope"]}""",
                extra = if (rtg.isEmpty()) "" else ""","rtg":$rtg""",
            ),
        )

    private fun tenant() = TestTokens.mint(pair, TestTokens.tenantClaims(catalogs = mapOf(mount to listOf("admin"))))

    private fun operator() = TestTokens.mint(pair, TestTokens.operatorClaims())

    /** A catalog operator token that places [catalog] on the hoglite backend. */
    private fun routedOperator(catalog: String) =
        TestTokens.mint(
            pair,
            TestTokens.operatorClaims().dropLast(1) +
                ""","cat":{"$catalog":["admin"]},"rtg":{"b":"hoglite","be":"$hogliteId","t":"t_1"}}""",
        )

    private fun dispatch(
        mode: AuthMode,
        mounts: MountRegistry = mounts(),
        default: String = "postgres",
        block: suspend ApplicationTestBuilder.() -> Unit,
    ) = testApplication {
        val cfg = config(default)
        val selector = CatalogStoreSelector(postgres, cfg, mounts)
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
            installApiRoutes(selector)
            installScanRoutes(selector)
        }
        block()
    }

    private suspend fun ApplicationTestBuilder.get(
        path: String,
        token: String? = null,
    ): HttpResponse = client.get(path) { token?.let { header(HttpHeaders.Authorization, "Bearer $it") } }

    private suspend fun ApplicationTestBuilder.commit(token: String?): HttpResponse =
        client.post("/v1/catalogs/$mount/commit") {
            token?.let { header(HttpHeaders.Authorization, "Bearer $it") }
            contentType(ContentType.Application.Json)
            setBody("{}")
        }

    private fun assertRefused(
        response: HttpResponse,
        status: HttpStatusCode,
        error: String,
    ) {
        assertThat(response.status).isEqualTo(status)
        assertThat(kotlinx.coroutines.runBlocking { response.bodyAsText() }).contains("\"error\":\"$error\"")
    }

    @Test
    fun `the reserved shape is a canonical lowercase org uuid followed by -events`() {
        assertThat(MountNames.isReserved(mount)).isTrue()
        for (name in listOf(
            "0190F3C4-1B2A-4C3D-8E4F-A5B6C7D8E9F0-events",
            "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f0-events2",
            "x0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f0-events",
            "0190f3c41b2a4c3d8e4fa5b6c7d8e9f0-events",
            "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f-events",
            "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f0",
            "millrace-dev-events",
            "millrace-dev",
        )) {
            assertThat(MountNames.isReserved(name)).describedAs(name).isFalse()
        }
    }

    @Test
    fun `only a verified scoped caller whose cat names the mount may use it`() {
        fun caller(
            kind: Caller.Kind,
            cat: Map<String, Set<Scope>> = mapOf(mount to setOf(Scope.READ)),
            routing: Routing? = null,
        ) = Caller("sub", null, kind, cat, "jti", null, routing = routing)
        val record = MountRecord(mount)

        requireMountReader(caller(Caller.Kind.SCOPED), record)
        requireMountReader(caller(Caller.Kind.SCOPED, mapOf(mount to setOf(Scope.WRITE))), record)
        requireMountReader(
            caller(Caller.Kind.SCOPED, routing = Routing(mount, BackendKind.POSTGRES, postgresId, null)),
            record,
        )
        for (refused in listOf(
            null,
            caller(Caller.Kind.TENANT, mapOf(mount to setOf(Scope.ADMIN))),
            caller(Caller.Kind.OPERATOR, emptyMap()),
            caller(Caller.Kind.SCOPED, mapOf("lake" to setOf(Scope.READ))),
        )) {
            assertThatThrownBy { requireMountReader(refused, record) }
                .describedAs("%s", refused?.kind)
                .isInstanceOf(HoglakeException.NotFound::class.java)
                .hasMessage("catalog '$mount'")
        }
        assertThatThrownBy {
            requireMountReader(
                caller(Caller.Kind.SCOPED, routing = Routing(mount, BackendKind.HOGLITE, hogliteId, "t_1")),
                record,
            )
        }.isInstanceOf(HoglakeException.NotSupported::class.java)
    }

    @Test
    fun `a scoped caller naming the mount is refused before any service call in warn and enforce`() {
        for (mode in listOf(AuthMode.WARN, AuthMode.ENFORCE)) {
            dispatch(mode) {
                // The namespace listing: a scoped route the plugin allows outright.
                assertRefused(
                    get("/v1/catalogs/$mount/namespaces", scoped()),
                    HttpStatusCode.NotImplemented,
                    "mount_operation_unavailable",
                )
                // A namespaced read: the gate resolves the catalog and gets the refusal.
                assertRefused(
                    get("/v1/catalogs/$mount/namespaces/ingest/tables/events/scan", scoped()),
                    HttpStatusCode.NotImplemented,
                    "mount_operation_unavailable",
                )
                // A body-scoped write: the handler resolves the catalog first.
                assertRefused(commit(scoped("write")), HttpStatusCode.Forbidden, "read_only_catalog")
                // A token that places the mount on hoglite.
                val routed = """{"b":"hoglite","be":"$hogliteId","t":"t_1"}"""
                assertRefused(
                    get("/v1/catalogs/$mount/namespaces", scoped(rtg = routed)),
                    HttpStatusCode.NotImplemented,
                    "not_supported_on_backend",
                )
                // A PostgreSQL placement is this process's PostgreSQL: still a mount.
                val local = """{"b":"postgres","be":"$postgresId"}"""
                assertRefused(
                    get("/v1/catalogs/$mount/namespaces", scoped(rtg = local)),
                    HttpStatusCode.NotImplemented,
                    "mount_operation_unavailable",
                )
            }
        }
        assertThat(reached.get()).isZero()
    }

    @Test
    fun `everyone else is told the mount does not exist in every auth mode`() {
        for (mode in listOf(AuthMode.OFF, AuthMode.WARN, AuthMode.ENFORCE)) {
            dispatch(mode) {
                // Anonymous gets through the plugin under `off` and `warn` only.
                val tokens = listOf(tenant(), operator()) + if (mode == AuthMode.ENFORCE) emptyList() else listOf(null)
                for (token in tokens) {
                    val response = get("/v1/catalogs/$mount", token)
                    assertRefused(response, HttpStatusCode.NotFound, "not_found")
                    assertThat(response.bodyAsText()).describedAs("%s", mode).contains("catalog '$mount'")
                    assertRefused(commit(token), HttpStatusCode.NotFound, "not_found")
                }
            }
        }
        assertThat(reached.get()).isZero()
    }

    @Test
    fun `only a held reserved name is a mount, and a non-reader or a create never reaches hoglite`() {
        val lookups = AtomicInteger()
        val switchReads = AtomicInteger()
        val counting =
            mounts(
                switchOn = {
                    switchReads.incrementAndGet()
                    false
                },
                registry = {
                    lookups.incrementAndGet()
                    oneMount.find(it)
                },
            )
        dispatch(AuthMode.ENFORCE, counting) {
            // Not the reserved shape: neither the registry nor the switch is read.
            assertThat(get("/v1/catalogs/lake", operator()).status).isEqualTo(HttpStatusCode.InternalServerError)
            assertThat(lookups.get() to switchReads.get()).isEqualTo(0 to 0)
            // Reserved but not a mount: the switch is not read.
            val other = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f1-events"
            assertThat(get("/v1/catalogs/$other", operator()).status).isEqualTo(HttpStatusCode.InternalServerError)
            assertThat(lookups.get() to switchReads.get()).isEqualTo(1 to 0)
            // A mount with the switch off: served as any catalog, by PostgreSQL.
            assertThat(get("/v1/catalogs/$mount", operator()).status).isEqualTo(HttpStatusCode.InternalServerError)
            assertThat(lookups.get() to switchReads.get()).isEqualTo(2 to 1)
        }
        assertThat(reached.get()).isEqualTo(3)
        val unread = mounts(switchOn = { error("switch read") }, registry = { error("registry read") })
        dispatch(AuthMode.ENFORCE, unread, default = "none") {
            // No PostgreSQL default: no mounts, and nothing read to find out.
            assertRefused(get("/v1/catalogs/$mount", operator()), HttpStatusCode.NotFound, "not_found")
        }
        dispatch(AuthMode.ENFORCE) {
            // A non-reader routed to hoglite learns nothing: the caller check comes first.
            assertRefused(get("/v1/catalogs/$mount", routedOperator(mount)), HttpStatusCode.NotFound, "not_found")
            // A reserved name is refused on create before placement picks hoglite.
            val create =
                client.post("/v1/catalogs") {
                    header(HttpHeaders.Authorization, "Bearer ${routedOperator(mount)}")
                    contentType(ContentType.Application.Json)
                    setBody("""{"name":"$mount","data_path":"s3://b/$mount"}""")
                }
            assertRefused(create, HttpStatusCode.UnprocessableEntity, "validation")
        }
        dispatch(AuthMode.ENFORCE, mounts(switchOn = { error("switch read") }, registry = MountRegistry.NONE)) {
            // A reserved name the token places on hoglite goes to hoglite, unread
            // by PostgreSQL; the configured engine is down, hence 503.
            assertRefused(
                get("/v1/catalogs/$mount/namespaces", routedOperator(mount)),
                HttpStatusCode.ServiceUnavailable,
                "backend_unavailable",
            )
        }
        assertThat(reached.get()).isEqualTo(3)
    }

    @Test
    fun `each mount refusal is counted once by its error code, for mounts the registry holds only`() {
        val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
        val routed = """{"b":"hoglite","be":"$hogliteId","t":"t_1"}"""
        val other = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f1-events"
        dispatch(AuthMode.ENFORCE) {
            assertRefused(
                get("/v1/catalogs/$mount/namespaces", scoped()),
                HttpStatusCode.NotImplemented,
                "mount_operation_unavailable",
            )
            assertRefused(commit(scoped("write")), HttpStatusCode.Forbidden, "read_only_catalog")
            assertRefused(
                get("/v1/catalogs/$mount/namespaces", scoped(rtg = routed)),
                HttpStatusCode.NotImplemented,
                "not_supported_on_backend",
            )
            assertRefused(get("/v1/catalogs/$mount", tenant()), HttpStatusCode.NotFound, "not_found")
            // MUTATION: count before the registry holds the name and these
            // add series: a reserved name that is not a mount, and a held
            // one reached through the store with the switch off, below.
            get("/v1/catalogs/$other", operator())
        }
        dispatch(AuthMode.ENFORCE, mounts(switchOn = { false })) { get("/v1/catalogs/$mount", tenant()) }
        assertThat(registry.counts("hoglake_mount_refusals_total", "reason")).isEqualTo(
            mapOf(
                "mount_operation_unavailable" to 1.0,
                "read_only_catalog" to 1.0,
                "not_supported_on_backend" to 1.0,
                "not_found" to 1.0,
            ),
        )
        val ids = registry.find("hoglake_mount_refusals_total").counters().map { it.id }
        assertThat(ids.map { it.getTag("catalog") }.toSet()).containsExactly(mount)
        assertThat(ids.flatMap { id -> id.tags.map { it.key } }.toSet()).containsExactlyInAnyOrder("catalog", "reason")
    }

    @Test
    fun `the gate counts the request's one auth outcome when its lookup answers, and none for an outage`() {
        val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
        val table = "namespaces/ingest/tables/events"
        val scan = "$table/scan"
        val routed = """{"b":"hoglite","be":"$hogliteId","t":"t_1"}"""
        dispatch(AuthMode.ENFORCE) {
            assertRefused(
                get("/v1/catalogs/$mount/$scan", scoped()),
                HttpStatusCode.NotImplemented,
                "mount_operation_unavailable",
            )
            assertRefused(
                get("/v1/catalogs/$mount/$scan", scoped(rtg = routed)),
                HttpStatusCode.NotImplemented,
                "not_supported_on_backend",
            )
            // A scoped writer on a namespaced write route.
            val truncate =
                client.post("/v1/catalogs/$mount/$table/truncate") {
                    header(HttpHeaders.Authorization, "Bearer ${scoped("write")}")
                }
            assertRefused(truncate, HttpStatusCode.Forbidden, "read_only_catalog")
            // MUTATION: count every exception and this outage (the routed
            // engine is down) is counted as a refusal.
            assertRefused(
                get("/v1/catalogs/lake/$scan", scoped(catalog = "lake", rtg = routed)),
                HttpStatusCode.ServiceUnavailable,
                "backend_unavailable",
            )
        }
        dispatch(AuthMode.ENFORCE, default = "none") {
            // No backend holds the catalog: the lookup's 404 is the refusal.
            assertRefused(
                get("/v1/catalogs/lake/$scan", scoped(catalog = "lake")),
                HttpStatusCode.NotFound,
                "not_found",
            )
        }
        // A mount reader on a route the mount does not serve is authorized,
        // as the plugin counts it on the routes it allows outright.
        // MUTATION: drop the gate's catch and none of these has an outcome.
        assertThat(registry.counts("hoglake_auth_requests_total", "outcome"))
            .isEqualTo(mapOf("ok" to 2.0, "other" to 1.0, "not_entitled" to 1.0))
        // Each refused mount request also counts once against the mount.
        assertThat(registry.counts("hoglake_mount_refusals_total", "reason")).isEqualTo(
            mapOf("mount_operation_unavailable" to 1.0, "not_supported_on_backend" to 1.0, "read_only_catalog" to 1.0),
        )
        assertThat(reached.get()).isZero()
    }
}
