package com.posthog.hoglake.store

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.api.installApiRoutes
import com.posthog.hoglake.api.installErrorMapping
import com.posthog.hoglake.api.installScanRoutes
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.auth.installAuth
import com.posthog.hoglake.auth.installNamespaceGate
import com.posthog.hoglake.configureApiListener
import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.observability.RequestId
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import io.ktor.client.request.get
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
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.lang.reflect.Proxy
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import java.security.interfaces.ECPublicKey
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

/**
 * A mount Client (plan row 1.3c) through the real auth plugin, namespace gate
 * and catalog routes, against a stub Host that records every request it gets.
 * The Client has no PostgreSQL default, as the customer hoglake has none.
 */
class MountHostClientTest {
    @TempDir
    lateinit var dir: Path
    private val pair = TestTokens.keyPair()
    private val mount = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f0-events"
    private val other = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f1-events"
    private val hogliteId = UUID.fromString("5b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e")
    private val scan = "/v1/catalogs/$mount/namespaces/ingest/tables/events_v2/scan"

    /** One request as the stub Host received it. */
    private data class Seen(
        val method: String,
        val rawPath: String,
        val rawQuery: String?,
        val headers: Map<String, List<String>>,
    )

    /** A Host that records every request and answers with [answer]. */
    private inner class StubHost : AutoCloseable {
        val seen = CopyOnWriteArrayList<Seen>()
        private val workers = Executors.newCachedThreadPool()

        @Volatile
        var answer: (HttpExchange) -> Unit = { it.json(200, "[]") }
        private val server =
            HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).also { server ->
                server.executor = workers
                server.createContext("/") { exchange ->
                    val uri = exchange.requestURI
                    seen +=
                        Seen(
                            exchange.requestMethod,
                            uri.rawPath,
                            uri.rawQuery,
                            exchange.requestHeaders.mapKeys { it.key.lowercase() },
                        )
                    try {
                        answer(exchange)
                    } finally {
                        exchange.close()
                    }
                }
                server.start()
            }
        val port: Int get() = server.address.port
        val origin: URI get() = URI("http://127.0.0.1:$port")

        override fun close() {
            server.stop(0)
            workers.shutdownNow()
        }
    }

    private fun HttpExchange.json(
        status: Int,
        body: String,
        contentType: String = "application/json",
        headers: Map<String, String> = emptyMap(),
    ) {
        val bytes = body.toByteArray()
        responseHeaders.add("Content-Type", contentType)
        headers.forEach { (name, value) -> responseHeaders.add(name, value) }
        sendResponseHeaders(status, if (bytes.isEmpty()) -1 else bytes.size.toLong())
        if (bytes.isNotEmpty()) responseBody.use { it.write(bytes) }
    }

    private val reached = AtomicInteger()

    /** A PostgreSQL store holding one catalog, `lake`; it counts every call. */
    private val postgres =
        Proxy.newProxyInstance(CatalogStore::class.java.classLoader, arrayOf(CatalogStore::class.java)) { _, m, _ ->
            reached.incrementAndGet()
            val lake =
                CatalogInfo(
                    catalogId = 1,
                    name = "lake",
                    dataPath = "s3://bucket/lake",
                    headSnapshotId = 3,
                    schemaVersion = 2,
                )
            when (m.name) {
                "getCatalog" -> lake
                "listCatalogs" -> listOf(lake)
                else -> throw IllegalStateException("a route reached PostgreSQL: ${m.name}")
            }
        } as CatalogStore

    private fun config(default: String): Config {
        val file = dir.resolve("backends.json")
        val backend = """"id":"$hogliteId","kind":"hoglite","url":"http://127.0.0.1:9","retry_budget_ms":1"""
        Files.writeString(file, """{"backends":[{$backend}]}""")
        return Config(backendsFile = file.toString(), defaultBackend = default, authMode = "enforce")
    }

    @AfterEach
    fun unbindMetrics() = Metrics.clear()

    private fun scoped(
        catalogs: String = """{"$mount":["read"]}""",
        rtg: String = "",
    ): String =
        TestTokens.mint(
            pair,
            TestTokens.scopedClaims(catalogs = catalogs, extra = if (rtg.isEmpty()) "" else ""","rtg":$rtg"""),
        )

    private fun tenant() = TestTokens.mint(pair, TestTokens.tenantClaims(catalogs = mapOf(mount to listOf("admin"))))

    private fun operator() = TestTokens.mint(pair, TestTokens.operatorClaims())

    private fun Application.clientModule(
        selector: CatalogStoreSelector,
        mode: AuthMode,
        cfg: Config,
    ) {
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
        install(RequestId)
        installAuth(AuthService(mode, verifier))
        installNamespaceGate(selector)
        install(ContentNegotiation) { jackson { configureHoglakeWire() } }
        install(StatusPages) { installErrorMapping() }
        installApiRoutes(selector)
        installScanRoutes(selector)
    }

    private fun hostClient(
        origin: URI,
        timeoutMs: Long = 5_000,
        maxResponseBytes: Long = 1 shl 20,
        maxConcurrent: Int = 4,
        prebufferBytes: Int = MountHostClient.PREBUFFER_BYTES,
        match: MountMatch = MountMatch.Events,
    ) = MountHostClient(
        MountHost(
            id = UUID.randomUUID(),
            match = match,
            kind = MountHost.Kind.HOGLAKE,
            origin = origin,
            auth = MountHost.Auth.FORWARD,
            timeoutMs = timeoutMs,
            maxResponseBytes = maxResponseBytes,
            maxConcurrent = maxConcurrent,
        ),
        prebufferBytes = prebufferBytes,
    )

    /** The resolver over one reader, by its host's matcher; null is a server with no mount hosts. */
    private fun resolver(client: MountHostClient?) =
        if (client == null) {
            MountHostResolver.NONE
        } else {
            MountHostResolver { _, name -> client.takeIf { it.host.match.matches(name) } }
        }

    /** A Client of [host] under [mode]; [client] null is a server with no mount hosts. */
    private fun client(
        host: StubHost,
        mode: AuthMode = AuthMode.ENFORCE,
        default: String = "none",
        client: MountHostClient? = hostClient(host.origin),
        block: suspend ApplicationTestBuilder.(MountHostClient?) -> Unit,
    ) = testApplication {
        val cfg = config(default)
        application {
            clientModule(
                CatalogStoreSelector(postgres, cfg, MountRegistry.NONE, resolver(client)),
                mode,
                cfg,
            )
        }
        try {
            block(client)
        } finally {
            client?.close()
        }
    }

    private suspend fun ApplicationTestBuilder.get(
        path: String,
        token: String? = null,
        requestId: String? = null,
    ) = client.get(path) {
        token?.let { header(HttpHeaders.Authorization, "Bearer $it") }
        requestId?.let { header("X-Request-Id", it) }
    }

    private fun error(body: String): String = body.substringAfter("\"error\":\"").substringBefore('"')

    @Test
    fun `a reader's request goes to the host with its own token and request id, and the answer comes back unchanged`() {
        StubHost().use { host ->
            // Odd spacing and a field this server does not know: a re-serialised body would differ.
            val body = """[ {"name":"ingest",  "namespace_id":7, "future_field":{"x":[1,2]}} ]"""
            host.answer = { it.json(200, body) }
            client(host) {
                val token = scoped()
                val routes =
                    listOf(
                        "/v1/catalogs/$mount",
                        "/v1/catalogs/$mount/namespaces",
                        "/v1/catalogs/$mount/namespaces/ingest",
                        "/v1/catalogs/$mount/namespaces/ingest/tables",
                        "/v1/catalogs/$mount/namespaces/ingest/tables/events_v2",
                        scan,
                    )
                for (route in routes) {
                    val response = get(route, token, requestId = "req-1.3c")
                    assertThat(response.status).describedAs(route).isEqualTo(HttpStatusCode.OK)
                    assertThat(response.bodyAsText()).describedAs(route).isEqualTo(body)
                    assertThat(response.headers[HttpHeaders.ContentType]).startsWith("application/json")
                }
                assertThat(host.seen.map { it.method }.toSet()).containsExactly("GET")
                assertThat(host.seen.map { it.rawPath }).isEqualTo(routes)
                for (seen in host.seen) {
                    assertThat(seen.rawQuery).isNull()
                    // The caller's own credential, unchanged, and nothing else of its headers.
                    assertThat(seen.headers["authorization"]).containsExactly("Bearer $token")
                    assertThat(seen.headers["x-request-id"]).containsExactly("req-1.3c")
                    assertThat(seen.headers["user-agent"]!!.single()).startsWith("hoglake-server/")
                }
            }
        }
    }

    @Test
    fun `query parameters are forwarded re-encoded, and none can leave the path or the host`() {
        StubHost().use { host ->
            client(host) {
                val query =
                    "snapshot=5&include=column_stats&include=split_offsets&stats_fields=1,2" +
                        "&at_timestamp=2026-09-05T00:00:00Z&x=a%20b%26c%3Dd%23e%2F..%2F%3Fq&%2Fy=1"
                assertThat(get("$scan?$query", scoped()).status).isEqualTo(HttpStatusCode.OK)
                val seen = host.seen.single()
                assertThat(seen.rawPath).isEqualTo(scan)
                assertThat(seen.rawQuery).isEqualTo(
                    "snapshot=5&include=column_stats&include=split_offsets&stats_fields=1,2" +
                        "&at_timestamp=2026-09-05T00:00:00Z&x=a%20b%26c%3Dd%23e%2F..%2F%3Fq&%2Fy=1",
                )
            }
        }
    }

    @Test
    fun `a path segment is one segment whatever it holds`() {
        assertThat(MountHostClient.pathSegment("events_v2")).isEqualTo("events_v2")
        assertThat(MountHostClient.pathSegment("a/b")).isEqualTo("a%2Fb")
        assertThat(MountHostClient.pathSegment("a?b#c%d e")).isEqualTo("a%3Fb%23c%25d%20e")
        assertThat(MountHostClient.pathSegment("..%2f")).isEqualTo("..%252f")
        assertThat(MountHostClient.pathSegment("é")).isEqualTo("%C3%A9")
        // The path is built from segments only, so a name can never add one, even unvalidated.
        assertThat(MountHostClient.path(mount, listOf("namespaces", "a/b", "tables", "c?d#e", "scan")))
            .isEqualTo("/v1/catalogs/$mount/namespaces/a%2Fb/tables/c%3Fd%23e/scan")
        assertThat(MountHostClient.path(mount, emptyList())).isEqualTo("/v1/catalogs/$mount")
        for (dots in listOf("", ".", "..", "...")) {
            assertThatThrownBy { MountHostClient.pathSegment(dots) }.isInstanceOf(IllegalArgumentException::class.java)
        }
    }

    @Test
    fun `a namespace or table name no catalog could hold is a local 404 and is never sent`() {
        StubHost().use { host ->
            client(host) {
                val token = scoped()
                val names = listOf("..", ".", "a%2Fb", "a%2F..%2F..%2Fx", "a%20b", "a%3Fb", "a%23b", "9lives", "%C3%A9")
                for (name in names) {
                    for (path in listOf(
                        "/v1/catalogs/$mount/namespaces/$name",
                        "/v1/catalogs/$mount/namespaces/$name/tables",
                        "/v1/catalogs/$mount/namespaces/$name/tables/events_v2",
                        "/v1/catalogs/$mount/namespaces/ingest/tables/$name",
                        "/v1/catalogs/$mount/namespaces/ingest/tables/$name/scan",
                    )) {
                        val response = get(path, token)
                        assertThat(response.status).describedAs(path).isEqualTo(HttpStatusCode.NotFound)
                        assertThat(error(response.bodyAsText())).describedAs(path).isEqualTo("not_found")
                    }
                }
                assertThat(host.seen).isEmpty()
            }
        }
    }

    @Test
    fun `a caller that is not the mount's scoped reader is refused here and the host is never called`() {
        StubHost().use { host ->
            val routed = """{"b":"hoglite","be":"$hogliteId","t":"t_1"}"""
            val reads =
                listOf(
                    "/v1/catalogs/$mount",
                    "/v1/catalogs/$mount/namespaces",
                    "/v1/catalogs/$mount/namespaces/ingest",
                    "/v1/catalogs/$mount/namespaces/ingest/tables",
                    "/v1/catalogs/$mount/namespaces/ingest/tables/events_v2",
                    scan,
                )
            for (mode in listOf(AuthMode.OFF, AuthMode.WARN, AuthMode.ENFORCE)) {
                client(host, mode) {
                    for (path in reads) {
                        // A scoped token whose `cat` names another catalog, or another mount.
                        for (cat in listOf("""{"lake":["read"]}""", """{"$other":["read"]}""")) {
                            val response = get(path, scoped(cat))
                            assertThat(
                                response.status,
                            ).describedAs("%s %s", mode, path).isEqualTo(HttpStatusCode.NotFound)
                        }
                        // A tenant and an operator pass the plugin; the binding refuses them.
                        for (token in listOf(tenant(), operator())) {
                            val response = get(path, token)
                            assertThat(
                                response.status,
                            ).describedAs("%s %s", mode, path).isEqualTo(HttpStatusCode.NotFound)
                            assertThat(response.bodyAsText()).contains("catalog '$mount'")
                        }
                        // No token: 401 under enforce, the binding's 404 where the plugin lets it through.
                        val anonymous = get(path)
                        assertThat(anonymous.status).describedAs("%s %s", mode, path)
                            .isEqualTo(
                                if (mode == AuthMode.ENFORCE) HttpStatusCode.Unauthorized else HttpStatusCode.NotFound,
                            )
                        if (mode != AuthMode.OFF) {
                            // A reader whose `rtg` places the mount on hoglite, as the Host answers it.
                            val placed = get(path, scoped(rtg = routed))
                            assertThat(
                                placed.status,
                            ).describedAs("%s %s", mode, path).isEqualTo(HttpStatusCode.NotImplemented)
                            assertThat(error(placed.bodyAsText())).isEqualTo("not_supported_on_backend")
                        }
                    }
                }
            }
            assertThat(host.seen).isEmpty()
            assertThat(reached.get()).isZero()
        }
    }

    @Test
    fun `under off even the mount's own reader is served nothing`() {
        StubHost().use { host ->
            client(host, AuthMode.OFF) {
                assertThat(get("/v1/catalogs/$mount", scoped()).status).isEqualTo(HttpStatusCode.NotFound)
                assertThat(get(scan, scoped()).status).isEqualTo(HttpStatusCode.NotFound)
            }
            assertThat(host.seen).isEmpty()
        }
    }

    @Test
    fun `every other route on the mount is refused with the host's codes and the host is never called`() {
        StubHost().use { host ->
            for (mode in listOf(AuthMode.WARN, AuthMode.ENFORCE)) {
                client(host, mode) {
                    val reader = scoped()
                    val writer = scoped("""{"$mount":["write"]}""")
                    // A scoped read route a mount does not serve.
                    val files = get("/v1/catalogs/$mount/namespaces/ingest/tables/events_v2/files", reader)
                    assertThat(files.status).isEqualTo(HttpStatusCode.NotImplemented)
                    assertThat(error(files.bodyAsText())).isEqualTo("mount_operation_unavailable")

                    suspend fun send(
                        method: HttpMethod,
                        path: String,
                        token: String,
                    ) = client.request(path) {
                        this.method = method
                        header(HttpHeaders.Authorization, "Bearer $token")
                        contentType(ContentType.Application.Json)
                        setBody("{}")
                    }
                    // Writes a scoped token may attempt: the binding refuses them as read-only.
                    for ((method, path) in listOf(
                        HttpMethod.Post to "/v1/catalogs/$mount/commit",
                        HttpMethod.Post to "/v1/catalogs/$mount/namespaces/ingest/tables",
                        HttpMethod.Delete to "/v1/catalogs/$mount/namespaces/ingest/tables/events_v2",
                    )) {
                        val response = send(method, path, writer)
                        assertThat(response.status).describedAs(path).isEqualTo(HttpStatusCode.Forbidden)
                        assertThat(error(response.bodyAsText())).describedAs(path).isEqualTo("read_only_catalog")
                    }
                    // The same writes with a read-only `cat`, and routes outside the scoped set: the plugin's 403.
                    for ((method, path, token) in listOf(
                        Triple(HttpMethod.Post, "/v1/catalogs/$mount/commit", reader),
                        Triple(HttpMethod.Delete, "/v1/catalogs/$mount", writer),
                        Triple(HttpMethod.Post, "/v1/catalogs/$mount/namespaces", writer),
                        Triple(HttpMethod.Get, "/v1/catalogs/$mount/storage-usage", reader),
                    )) {
                        val response = send(method, path, token)
                        assertThat(response.status).describedAs(path).isEqualTo(HttpStatusCode.Forbidden)
                        assertThat(error(response.bodyAsText())).describedAs(path).isEqualTo("forbidden")
                    }
                }
            }
            assertThat(host.seen).isEmpty()
            assertThat(reached.get()).isZero()
        }
    }

    @Test
    fun `a catalog that is not a mount name never involves the host`() {
        StubHost().use { host ->
            client(host, default = "postgres") {
                val response = get("/v1/catalogs/lake", operator())
                assertThat(response.status).isEqualTo(HttpStatusCode.OK)
                assertThat(response.bodyAsText()).contains(""""data_path":"s3://bucket/lake"""")
                // Names that only resemble a mount's are catalogs too.
                for (name in listOf("${mount}2", mount.uppercase(), "x$mount")) {
                    assertThat(
                        get("/v1/catalogs/$name", operator()).status,
                    ).describedAs(name).isEqualTo(HttpStatusCode.OK)
                }
                assertThat(get("/v1/catalogs", operator()).bodyAsText()).contains(""""name":"lake"""")
            }
            assertThat(host.seen).isEmpty()
            assertThat(reached.get()).isEqualTo(5)
        }
    }

    @Test
    fun `with no host configured a mount name is served as before`() {
        StubHost().use { host ->
            // PostgreSQL default and no mount record: the reserved name is the catalog it is.
            client(host, default = "postgres", client = null) {
                assertThat(get("/v1/catalogs/$mount", scoped()).status).isEqualTo(HttpStatusCode.OK)
            }
            assertThat(reached.get()).isEqualTo(1)
            // No default backend: nothing holds it.
            client(host, client = null) {
                assertThat(get("/v1/catalogs/$mount", scoped()).status).isEqualTo(HttpStatusCode.NotFound)
                assertThat(get("/v1/catalogs", scoped()).status).isEqualTo(HttpStatusCode.NotFound)
            }
            assertThat(host.seen).isEmpty()
        }
    }

    @Test
    fun `the host's typed refusals pass through with their status, code and headers`() {
        StubHost().use { host ->
            client(host) {
                host.answer = { it.json(404, """{"error":"not_found","detail":"table 'ingest.nope'"}""") }
                val missing = get(scan, scoped())
                assertThat(missing.status).isEqualTo(HttpStatusCode.NotFound)
                assertThat(missing.bodyAsText()).isEqualTo("""{"error":"not_found","detail":"table 'ingest.nope'"}""")

                host.answer = {
                    val retry = mapOf("Retry-After" to "1")
                    it.json(503, """{"error":"mount_scan_busy","detail":"busy"}""", headers = retry)
                }
                val busy = get(scan, scoped())
                assertThat(busy.status).isEqualTo(HttpStatusCode.ServiceUnavailable)
                assertThat(error(busy.bodyAsText())).isEqualTo("mount_scan_busy")
                assertThat(busy.headers[HttpHeaders.RetryAfter]).isEqualTo("1")

                host.answer = { it.json(422, """{"error":"mount_scan_too_many_files","detail":"over budget"}""") }
                val budget = get(scan, scoped())
                assertThat(budget.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
                assertThat(error(budget.bodyAsText())).isEqualTo("mount_scan_too_many_files")
                assertThat(budget.headers[HttpHeaders.RetryAfter]).isNull()

                host.answer = { it.json(401, """{"error":"token_expired","detail":"token refused"}""") }
                val expired = get(scan, scoped())
                assertThat(expired.status).isEqualTo(HttpStatusCode.Unauthorized)
                assertThat(error(expired.bodyAsText())).isEqualTo("token_expired")
                assertThat(expired.headers[HttpHeaders.WWWAuthenticate]).isEqualTo("""Bearer error="token_expired"""")

                host.answer = { it.json(500, """{"error":"internal_error","detail":"internal server error"}""") }
                assertThat(get("/v1/catalogs/$mount", scoped()).status).isEqualTo(HttpStatusCode.InternalServerError)
            }
        }
    }

    @Test
    fun `an answer that is not the host's contract is a typed 502 that shows none of it`() {
        StubHost().use { host ->
            StubHost().use { elsewhere ->
                client(host) {
                    val answers =
                        listOf<(HttpExchange) -> Unit>(
                            { it.json(500, "<html>secret-upstream-page</html>", "text/html") },
                            { it.json(200, "<html>secret-upstream-page</html>", "text/html") },
                            { it.json(500, """{"detail":"secret-upstream-page"}""") },
                            { it.json(500, """{"error":"Not A Code secret-upstream-page","detail":"x"}""") },
                            { it.json(500, """["secret-upstream-page"]""") },
                            { it.json(418, """{"error":"teapot","detail":"secret-upstream-page"}""") },
                            { it.json(204, "") },
                            { it.json(500, """{"error":"e","detail":"${"secret-upstream-page".repeat(8_000)}"}""") },
                            // A redirect is not followed: only the configured host is ever contacted.
                            {
                                it.json(
                                    302,
                                    "",
                                    headers = mapOf("Location" to "${elsewhere.origin}/v1/catalogs/$mount"),
                                )
                            },
                            {
                                it.json(
                                    307,
                                    "",
                                    headers = mapOf("Location" to "${elsewhere.origin}/v1/catalogs/$mount"),
                                )
                            },
                        )
                    for ((i, answer) in answers.withIndex()) {
                        host.answer = answer
                        for (path in listOf("/v1/catalogs/$mount", "/v1/catalogs")) {
                            val response = get(path, scoped())
                            val text = response.bodyAsText()
                            assertThat(response.status).describedAs("answer %d %s: %s", i, path, text)
                                .isEqualTo(HttpStatusCode.BadGateway)
                            assertThat(error(text)).isEqualTo("mount_host_invalid_response")
                            assertThat(text).doesNotContain("secret-upstream-page", "127.0.0.1", "${host.port}")
                        }
                    }
                }
                assertThat(elsewhere.seen).isEmpty()
            }
        }
    }

    @Test
    fun `a host that is down is a typed 503 that names no address`() {
        val host = StubHost()
        val origin = host.origin
        host.close()
        run {
            client(host, client = hostClient(origin)) {
                for (path in listOf("/v1/catalogs/$mount", scan, "/v1/catalogs")) {
                    val response = get(path, scoped())
                    val text = response.bodyAsText()
                    assertThat(response.status).describedAs(path).isEqualTo(HttpStatusCode.ServiceUnavailable)
                    assertThat(error(text)).isEqualTo("mount_host_unavailable")
                    assertThat(response.headers[HttpHeaders.RetryAfter]).isEqualTo("1")
                    assertThat(text).doesNotContain("127.0.0.1", "${origin.port}", "Connect")
                }
            }
        }
    }

    @Test
    fun `a host slower than the deadline is a typed 503, before its headers or in its body`() {
        StubHost().use { host ->
            val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
            client(host, client = hostClient(host.origin, timeoutMs = 300)) {
                // No answer at all inside the deadline.
                host.answer = {
                    Thread.sleep(1_500)
                    it.json(200, "[]")
                }
                val silent = get(scan, scoped())
                assertThat(silent.status).isEqualTo(HttpStatusCode.ServiceUnavailable)
                assertThat(error(silent.bodyAsText())).isEqualTo("mount_host_unavailable")
                // Headers at once, then a body that stalls.
                host.answer = {
                    it.responseHeaders.add("Content-Type", "application/json")
                    it.sendResponseHeaders(200, 0)
                    it.responseBody.write("[".toByteArray())
                    it.responseBody.flush()
                    Thread.sleep(1_500)
                }
                val stalled = get(scan, scoped())
                assertThat(stalled.status).isEqualTo(HttpStatusCode.ServiceUnavailable)
                assertThat(error(stalled.bodyAsText())).isEqualTo("mount_host_unavailable")
                // Both failed requests gave their permits back.
                assertThat(it!!.cap.availablePermits()).isEqualTo(4)
            }
            val errors = registry.find("hoglake_mount_proxy_upstream_errors_total").counters()
            assertThat(errors.associate { it.id.getTag("reason") to it.count() }).isEqualTo(mapOf("timeout" to 2.0))
        }
    }

    @Test
    fun `the cap sheds a request with a typed 503 and sends nothing`() {
        StubHost().use { host ->
            val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
            client(host, client = hostClient(host.origin, maxConcurrent = 1)) { proxy ->
                proxy!!.cap.acquire()
                try {
                    for (path in listOf(scan, "/v1/catalogs")) {
                        val response = get(path, scoped())
                        assertThat(response.status).isEqualTo(HttpStatusCode.ServiceUnavailable)
                        assertThat(error(response.bodyAsText())).isEqualTo("mount_proxy_busy")
                        assertThat(response.headers[HttpHeaders.RetryAfter]).isEqualTo("1")
                    }
                    assertThat(host.seen).isEmpty()
                } finally {
                    proxy.cap.release()
                }
                // Each request returned its permit: the next one is served.
                repeat(3) { assertThat(get(scan, scoped()).status).isEqualTo(HttpStatusCode.OK) }
                assertThat(proxy.cap.availablePermits()).isEqualTo(1)
            }
            val refusals = registry.find("hoglake_mount_refusals_total").counters()
            assertThat(refusals.associate { it.id.getTag("reason") to it.count() })
                .isEqualTo(mapOf("mount_proxy_busy" to 2.0))
        }
    }

    @Test
    fun `a body over the cap is refused whole when it is small enough to hold`() {
        StubHost().use { host ->
            host.answer = { it.json(200, "[" + "1,".repeat(2_000) + "1]") }
            client(host, client = hostClient(host.origin, maxResponseBytes = 1_000)) {
                val response = get(scan, scoped())
                assertThat(response.status).isEqualTo(HttpStatusCode.BadGateway)
                assertThat(error(response.bodyAsText())).isEqualTo("mount_host_invalid_response")
            }
        }
    }

    /** A real listener: how a response ends mid-body is the engine's behaviour, not the test host's. */
    private fun <T> onNetty(
        proxy: MountHostClient,
        block: (port: Int) -> T,
    ): T {
        val cfg = config("none")
        val selector = CatalogStoreSelector(postgres, cfg, MountRegistry.NONE, resolver(proxy))
        val server =
            embeddedServer(Netty, port = 0, host = "127.0.0.1") { clientModule(selector, AuthMode.ENFORCE, cfg) }
        try {
            server.start(wait = false)
            return block(runBlocking { server.engine.resolvedConnectors().first().port })
        } finally {
            server.stop(0, 1000)
            proxy.close()
        }
    }

    private fun fetch(
        port: Int,
        path: String,
        token: String,
    ): HttpResponse<ByteArray> =
        HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build().use { http ->
            val request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port$path")).GET()
            http.send(request.header("Authorization", "Bearer $token").build(), HttpResponse.BodyHandlers.ofByteArray())
        }

    @Test
    fun `a large body is streamed through byte for byte`() {
        StubHost().use { host ->
            val body = ("[" + (1..60_000).joinToString(",") { """{"data_file_id":$it}""" } + "]").toByteArray()
            assertThat(body.size).isGreaterThan(1_000_000)
            host.answer = {
                it.responseHeaders.add("Content-Type", "application/json")
                // Chunked, as a Ktor Host streams a scan.
                it.sendResponseHeaders(200, 0)
                it.responseBody.use { out -> out.write(body) }
            }
            onNetty(hostClient(host.origin, maxResponseBytes = 8L shl 20, prebufferBytes = 1_024)) { port ->
                val response = fetch(port, scan, scoped())
                assertThat(response.statusCode()).isEqualTo(200)
                assertThat(response.body()).isEqualTo(body)
            }
        }
    }

    @Test
    fun `a body that passes the cap or breaks mid-stream never reaches the reader as a complete answer`() {
        val body = ("[" + (1..60_000).joinToString(",") { """{"data_file_id":$it}""" } + "]").toByteArray()
        val failures =
            mapOf<String, (HttpExchange) -> Unit>(
                // The whole body, over the Client's byte cap.
                "response_too_large" to {
                    it.responseHeaders.add("Content-Type", "application/json")
                    it.sendResponseHeaders(200, 0)
                    it.responseBody.use { out -> out.write(body) }
                },
                // A declared length the Host never delivers: the connection drops mid-body.
                "unavailable" to {
                    it.responseHeaders.add("Content-Type", "application/json")
                    it.sendResponseHeaders(200, body.size.toLong())
                    it.responseBody.write(body, 0, 200_000)
                    it.responseBody.flush()
                },
            )
        for ((reason, answer) in failures) {
            StubHost().use { host ->
                val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
                host.answer = answer
                val proxy = hostClient(host.origin, maxResponseBytes = 400_000, prebufferBytes = 1_024)
                onNetty(proxy) { port ->
                    // The connection is dropped mid-body: the transfer fails, it does not end short.
                    assertThatThrownBy { fetch(port, scan, scoped()) }.describedAs(reason)
                        .isInstanceOf(java.io.IOException::class.java)
                    // The failed request gave its permit back.
                    await().atMost(Duration.ofSeconds(5)).untilAsserted {
                        assertThat(proxy.cap.availablePermits()).isEqualTo(4)
                    }
                }
                // At least once: a client whose connection closed before any byte arrived may ask again.
                val errors = registry.find("hoglake_mount_proxy_upstream_errors_total").counters()
                assertThat(errors.map { it.id.getTag("reason") }).containsExactly(reason)
                assertThat(errors.single().count()).isGreaterThanOrEqualTo(1.0)
                Metrics.clear()
            }
        }
    }

    @Test
    fun `the listing asks the host once and shows only the mounts the caller's cat names`() {
        StubHost().use { host ->
            val entry =
                """{"name":"$mount","data_path":"s3://bucket/millrace/millrace-dev","head_snapshot_id":41,""" +
                    """"schema_version":7,"capabilities":[],"read_only":true,"future_field":1}"""
            host.answer = {
                it.json(
                    200,
                    // The caller's mount, a catalog of the Host's, a mount the caller's `cat` does not name, and noise.
                    """[{"name":"millrace-dev","data_path":"s3://bucket/millrace/millrace-dev"},$entry,""" +
                        """{"name":"$other","data_path":"s3://x","read_only":true},$entry,7,{"name":7}]""",
                )
            }
            client(host) {
                val listed = get("/v1/catalogs", scoped())
                assertThat(listed.status).isEqualTo(HttpStatusCode.OK)
                assertThat(listed.bodyAsText()).isEqualTo("[$entry]")
                val seen = host.seen.single()
                assertThat(seen.rawPath).isEqualTo("/v1/catalogs")
                assertThat(seen.rawQuery).isNull()
                assertThat(seen.headers["authorization"]!!.single()).startsWith("Bearer ")
                // A mount the Host does not list is not listed here either.
                host.answer = { it.json(200, "[]") }
                assertThat(get("/v1/catalogs", scoped()).bodyAsText()).isEqualTo("[]")
            }
            host.seen.clear()
            // A caller with no mount to see makes no upstream call: its listing does not depend on the Host.
            client(host, default = "postgres") {
                assertThat(get("/v1/catalogs", operator()).status).isEqualTo(HttpStatusCode.OK)
                assertThat(get("/v1/catalogs", scoped("""{"lake":["read"]}""")).bodyAsText()).contains("lake")
                assertThat(host.seen).isEmpty()
                // With local catalogs too, the Host's mounts follow them.
                host.answer = { it.json(200, "[$entry]") }
                val both = get("/v1/catalogs", scoped("""{"lake":["read"],"$mount":["read"]}""")).bodyAsText()
                assertThat(both).startsWith("""[{"name":"lake"""").endsWith(",$entry]")
                assertThat(host.seen).hasSize(1)
            }
        }
    }

    @Test
    fun `proxied requests are counted and timed per mount, and a refused name adds no series`() {
        StubHost().use { host ->
            val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
            client(host, AuthMode.WARN) {
                repeat(2) { get(scan, scoped()) }
                host.answer = { it.json(404, """{"error":"not_found","detail":"x"}""") }
                get(scan, scoped())
                // Refused locally, under names the caller chose.
                for (i in 0..9) get("/v1/catalogs/0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9${"%02d".format(i)}-events")
                get("/v1/catalogs/$other", operator())
            }
            val requests = registry.find("hoglake_mount_proxy_requests_total").counters()
            assertThat(requests.associate { it.id.getTag("catalog") to it.count() }).isEqualTo(mapOf(mount to 3.0))
            val timer = registry.find("hoglake_mount_proxy_requests").timers().single()
            assertThat(timer.id.getTag("catalog")).isEqualTo(mount)
            assertThat(timer.count()).isEqualTo(3)
            val errors = registry.find("hoglake_mount_proxy_upstream_errors_total").counters()
            assertThat(errors.associate { it.id.getTag("reason") to it.count() }).isEqualTo(mapOf("http_404" to 1.0))
            // Every series is the verified mount's: no label came from a refused request's path.
            val catalogs = registry.meters.mapNotNull { it.id.getTag("catalog") }.toSet()
            assertThat(catalogs).containsExactly(mount)
        }
    }

    @Test
    fun `two hosts each get only the names they match, and a name no host matches stays local`() {
        StubHost().use { first ->
            StubHost().use { second ->
                fun entry(name: String) = """{"name":"$name","data_path":"s3://bucket/$name","read_only":true}"""
                first.answer = { it.json(200, "[${entry(mount)},${entry(other)}]") }
                second.answer = { it.json(200, "[${entry(other)},${entry(mount)}]") }
                val firstId = UUID.fromString("11111111-1111-4111-8111-111111111111")
                val secondId = UUID.fromString("22222222-2222-4222-8222-222222222222")

                fun host(
                    id: UUID,
                    name: String,
                    origin: URI,
                ) = MountHost(id, MountMatch.Catalog(name), MountHost.Kind.HOGLAKE, origin, MountHost.Auth.FORWARD)
                val third = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f2-events"
                StaticMountHosts(listOf(host(firstId, mount, first.origin), host(secondId, other, second.origin))).use {
                        hosts ->
                    // By id, as a verified claim could name a host later; an unknown id names nothing.
                    assertThat(hosts.host(firstId)!!.host.origin).isEqualTo(first.origin)
                    assertThat(hosts.host(secondId)!!.host.origin).isEqualTo(second.origin)
                    assertThat(hosts.host(UUID.randomUUID())).isNull()
                    testApplication {
                        val cfg = config("none")
                        application {
                            clientModule(
                                CatalogStoreSelector(postgres, cfg, MountRegistry.NONE, hosts),
                                AuthMode.ENFORCE,
                                cfg,
                            )
                        }
                        val token = scoped("""{"$mount":["read"],"$other":["read"],"$third":["read"]}""")
                        assertThat(get("/v1/catalogs/$mount/namespaces", token).status).isEqualTo(HttpStatusCode.OK)
                        assertThat(first.seen.map { it.rawPath }).containsExactly("/v1/catalogs/$mount/namespaces")
                        assertThat(second.seen).isEmpty()
                        assertThat(get("/v1/catalogs/$other/namespaces", token).status).isEqualTo(HttpStatusCode.OK)
                        assertThat(second.seen.map { it.rawPath }).containsExactly("/v1/catalogs/$other/namespaces")
                        assertThat(first.seen).hasSize(1)
                        // A mount name neither host matches is not remote: nothing here holds it.
                        assertThat(
                            get("/v1/catalogs/$third/namespaces", token).status,
                        ).isEqualTo(HttpStatusCode.NotFound)
                        assertThat(first.seen.size + second.seen.size).isEqualTo(2)
                        // The listing asks each host once and keeps from each only the names it matches.
                        val listed = get("/v1/catalogs", token)
                        assertThat(listed.bodyAsText()).isEqualTo("[${entry(mount)},${entry(other)}]")
                        assertThat(first.seen.map { it.rawPath }.last()).isEqualTo("/v1/catalogs")
                        assertThat(second.seen.map { it.rawPath }.last()).isEqualTo("/v1/catalogs")
                        assertThat(first.seen.size + second.seen.size).isEqualTo(4)
                        // A caller that names only one mount reaches only its host.
                        get("/v1/catalogs", scoped("""{"$other":["read"]}"""))
                        assertThat(first.seen).hasSize(2)
                        assertThat(second.seen).hasSize(3)
                    }
                }
            }
        }
    }

    @Test
    fun `hosts whose matchers overlap or share an id are refused`() {
        fun host(
            match: MountMatch,
            id: UUID = UUID.randomUUID(),
        ) = MountHost(id, match, MountHost.Kind.HOGLAKE, URI("http://127.0.0.1:9"), MountHost.Auth.FORWARD)
        for (overlapping in listOf(
            listOf(host(MountMatch.Events), host(MountMatch.Events)),
            listOf(host(MountMatch.Events), host(MountMatch.Catalog(mount))),
            listOf(host(MountMatch.Catalog(mount)), host(MountMatch.Events)),
            listOf(host(MountMatch.Catalog(mount)), host(MountMatch.Catalog(other)), host(MountMatch.Catalog(mount))),
        )) {
            assertThatThrownBy { StaticMountHosts(overlapping) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("match the same catalog name")
        }
        val id = UUID.randomUUID()
        assertThatThrownBy {
            StaticMountHosts(listOf(host(MountMatch.Catalog(mount), id), host(MountMatch.Catalog(other), id)))
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("duplicate mount host id")
        // A name that is not a mount's cannot be matched: it could be another tenant's catalog.
        for (name in listOf("lake", "millrace-dev", mount.uppercase(), "${mount}2")) {
            assertThatThrownBy { MountMatch.Catalog(name) }.isInstanceOf(IllegalArgumentException::class.java)
        }
        StaticMountHosts(listOf(host(MountMatch.Catalog(mount)), host(MountMatch.Catalog(other)))).close()
    }

    @Test
    fun `a parameter with no value reaches the host as an empty one, which a hoglake reads the same`() {
        StubHost().use { host ->
            onNetty(hostClient(host.origin)) { port ->
                assertThat(fetch(port, "$scan?include&snapshot=5&x=", scoped()).statusCode()).isEqualTo(200)
                assertThat(host.seen.single().rawQuery).isEqualTo("include=&snapshot=5&x=")
            }
        }
    }

    @Test
    fun `a 200 whose end cannot be told from a cut connection is refused`() {
        // No Content-Length and no chunking: the body ends when the connection does.
        ServerSocket(0, 50, InetAddress.getByName("127.0.0.1")).use { server ->
            val accepting =
                Thread {
                    runCatching {
                        while (true) {
                            server.accept().use { socket ->
                                val input = socket.getInputStream().bufferedReader()
                                while (input.readLine().orEmpty().isNotEmpty()) Unit
                                socket.getOutputStream().write(
                                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n[1,2".toByteArray(),
                                )
                            }
                        }
                    }
                }.apply {
                    isDaemon = true
                    start()
                }
            StubHost().use { unused ->
                client(unused, client = hostClient(URI("http://127.0.0.1:${server.localPort}"))) {
                    val response = get(scan, scoped())
                    assertThat(response.status).isEqualTo(HttpStatusCode.BadGateway)
                    assertThat(error(response.bodyAsText())).isEqualTo("mount_host_invalid_response")
                }
            }
            accepting.interrupt()
        }
        // An encoding nobody asked for would be relayed as if it were JSON.
        StubHost().use { host ->
            host.answer = { it.json(200, "[]", headers = mapOf("Content-Encoding" to "gzip")) }
            client(host) {
                assertThat(get(scan, scoped()).status).isEqualTo(HttpStatusCode.BadGateway)
            }
        }
    }

    @Test
    fun `a listing larger than a listing can be is refused, not parsed`() {
        StubHost().use { host ->
            val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
            host.answer = { it.json(200, "[" + "1,".repeat(200_000) + "1]") }
            client(host) {
                val response = get("/v1/catalogs", scoped())
                assertThat(response.status).isEqualTo(HttpStatusCode.BadGateway)
                assertThat(error(response.bodyAsText())).isEqualTo("mount_host_invalid_response")
            }
            val errors = registry.find("hoglake_mount_proxy_upstream_errors_total").counters()
            assertThat(errors.map { it.id.getTag("reason") }).containsExactly("response_too_large")
        }
    }

    @Test
    fun `the whole application cuts a body that fails mid-stream, and answers its typed errors`() {
        // The real App and its full pipeline (blocking dispatch, logging, metrics), with no PostgreSQL.
        val body = ("[" + (1..60_000).joinToString(",") { """{"data_file_id":$it}""" } + "]").toByteArray()
        StubHost().use { host ->
            val hosts =
                Files.writeString(
                    dir.resolve("hosts.json"),
                    """{"hosts":[{"id":"3c0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e","match":{"type":"events"},""" +
                        """"kind":"hoglake","url":"${host.origin}","auth":"forward","max_response_bytes":400000}]}""",
                )
            val keys = Files.writeString(dir.resolve("jwks.json"), TestTokens.jwks(pair))
            val cfg =
                Config(
                    port = 0,
                    defaultBackend = "none",
                    jdbcUrl = "jdbc:no-such-driver:must-not-open",
                    dbUser = "",
                    dbPassword = "",
                    authMode = "enforce",
                    authIssuer = TestTokens.ISSUER,
                    authJwksFile = keys.toString(),
                    authPredicateFields = "team_id",
                    mountHostsFile = hosts.toString(),
                )
            val iat = System.currentTimeMillis() / 1000 - 5
            val token =
                TestTokens.mint(pair, TestTokens.scopedClaims(catalogs = """{"$mount":["read"]}""", iat = iat))
            App.build(cfg).use { app ->
                val server = embeddedServer(Netty, configure = { configureApiListener(cfg) }) { app.module(this) }
                try {
                    server.start(wait = false)
                    val port = runBlocking { server.engine.resolvedConnectors().first().port }
                    // Served whole.
                    host.answer = { it.json(200, "[]") }
                    assertThat(String(fetch(port, scan, token).body())).isEqualTo("[]")
                    // Over the cap after the response began: the connection is cut.
                    host.answer = {
                        it.responseHeaders.add("Content-Type", "application/json")
                        it.sendResponseHeaders(200, 0)
                        it.responseBody.use { out -> out.write(body) }
                    }
                    assertThatThrownBy { fetch(port, scan, token) }.isInstanceOf(java.io.IOException::class.java)
                    // And the server is still serving.
                    host.answer = { it.json(404, """{"error":"not_found","detail":"table 'ingest.nope'"}""") }
                    val missing = fetch(port, scan, token)
                    assertThat(missing.statusCode()).isEqualTo(404)
                    assertThat(
                        String(missing.body()),
                    ).isEqualTo("""{"error":"not_found","detail":"table 'ingest.nope'"}""")
                } finally {
                    server.stop(0, 1000)
                }
            }
        }
    }
}
