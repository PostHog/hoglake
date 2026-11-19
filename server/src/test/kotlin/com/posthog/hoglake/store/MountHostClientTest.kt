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
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
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
 * The relay transport of a remote schema's Client (plan row 1.3c,
 * docs/auth-tenancy.md §6.5b) through the real auth plugin, namespace gate
 * and routes, against a stub Host that records every request it gets. The
 * Client holds `lake` and relays its namespace `posthog`.
 */
class MountHostClientTest {
    @TempDir
    lateinit var dir: Path
    private val pair = TestTokens.keyPair()
    private val hogliteId = UUID.fromString("5b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e")
    private val schema = "/v1/catalogs/lake/namespaces/posthog"
    private val scan = "$schema/tables/events_v2/scan"

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

    /** The schema's reader: the session profile and its `remote_schemas` claim; [rtg] places `lake`. */
    private fun scoped(rtg: String = ""): String =
        TestTokens.mint(
            pair,
            TestTokens.scopedClaims(
                catalogs = """{"lake":["read"]}""",
                extra =
                    TestTokens.sessionProfile("lake", """{"posthog":["read"]}""") +
                        if (rtg.isEmpty()) "" else ""","rtg":$rtg""",
            ),
        )

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
                    sessionProfile = true,
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
        match: MountMatch = MountMatch.Namespace("posthog"),
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
            // The stub Hosts speak http, which a remote schema's entry must opt in to.
            allowInsecureHttp = true,
        ),
        prebufferBytes = prebufferBytes,
    )

    /** The resolver over one reader, as its remote schema; null is a server with no mount hosts. */
    private fun resolver(client: MountHostClient?) =
        if (client == null) {
            MountHostResolver.NONE
        } else {
            MountHostResolver { listOf(RemoteSchemaHost(client.host.match as MountMatch.RemoteSchemaMatch, client)) }
        }

    /** A Client of [host] under [mode]; [client] null is a server with no mount hosts. */
    private fun client(
        host: StubHost,
        mode: AuthMode = AuthMode.ENFORCE,
        default: String = "postgres",
        client: MountHostClient? = hostClient(host.origin),
        block: suspend ApplicationTestBuilder.(MountHostClient?) -> Unit,
    ) = testApplication {
        val cfg = config(default)
        application {
            clientModule(
                CatalogStoreSelector(postgres, cfg, resolver(client)),
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
                val routes = listOf(schema, "$schema/tables", "$schema/tables/events_v2", scan)
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
    fun `a query parameter named like a route value is not forwarded`() {
        StubHost().use { host ->
            client(host) {
                // Route values come from the path only; every other parameter still goes.
                // Parameter names are matched without regard to case, so each spelling is left out.
                val query = "Catalog=other&snapshot=5&NAMESPACE=elsewhere&table=other_table&include=column_stats"
                assertThat(get("$scan?$query", scoped()).status).isEqualTo(HttpStatusCode.OK)
                assertThat(get("$schema/tables?catalog=other", scoped()).status)
                    .isEqualTo(HttpStatusCode.OK)
                assertThat(host.seen.map { it.rawPath }).containsExactly(scan, "$schema/tables")
                assertThat(host.seen.map { it.rawQuery }).containsExactly("snapshot=5&include=column_stats", null)
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
        assertThat(MountHostClient.path("lake", listOf("namespaces", "a/b", "tables", "c?d#e", "scan")))
            .isEqualTo("/v1/catalogs/lake/namespaces/a%2Fb/tables/c%3Fd%23e/scan")
        assertThat(MountHostClient.path("lake", emptyList())).isEqualTo("/v1/catalogs/lake")
        for (dots in listOf("", ".", "..", "...")) {
            assertThatThrownBy { MountHostClient.pathSegment(dots) }.isInstanceOf(IllegalArgumentException::class.java)
        }
    }

    @Test
    fun `a table name no catalog could hold is a local 404 and is never sent`() {
        StubHost().use { host ->
            client(host) {
                val token = scoped()
                val names = listOf("..", ".", "a%2Fb", "a%2F..%2F..%2Fx", "a%20b", "a%3Fb", "a%23b", "9lives", "%C3%A9")
                for (name in names) {
                    for (path in listOf("$schema/tables/$name", "$schema/tables/$name/scan")) {
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
                assertThat(get(schema, scoped()).status).isEqualTo(HttpStatusCode.InternalServerError)
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
                                    headers = mapOf("Location" to "${elsewhere.origin}$schema"),
                                )
                            },
                            {
                                it.json(
                                    307,
                                    "",
                                    headers = mapOf("Location" to "${elsewhere.origin}$schema"),
                                )
                            },
                        )
                    for ((i, answer) in answers.withIndex()) {
                        host.answer = answer
                        for (path in listOf(schema, scan)) {
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
                for (path in listOf(schema, scan)) {
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
                    for (path in listOf(scan, schema)) {
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
        val cfg = config("postgres")
        val selector = CatalogStoreSelector(postgres, cfg, resolver(proxy))
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
    fun `hosts whose matchers overlap or share an id are refused, and a host is found by its id`() {
        fun host(
            match: MountMatch,
            id: UUID = UUID.randomUUID(),
        ) = MountHost(id, match, MountHost.Kind.HOGLAKE, URI("https://127.0.0.1:9"), MountHost.Auth.FORWARD)
        val posthog = MountMatch.Namespace("posthog")
        val family = MountMatch.NamespaceFamily("team")
        for (overlapping in listOf(
            listOf(host(posthog), host(posthog)),
            listOf(host(posthog), host(MountMatch.Namespace("PostHog"))),
            listOf(host(family), host(MountMatch.Namespace("team_5"))),
            listOf(host(posthog), host(family), host(family)),
        )) {
            assertThatThrownBy { StaticMountHosts(overlapping) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("match the same namespace name")
        }
        val id = UUID.randomUUID()
        assertThatThrownBy { StaticMountHosts(listOf(host(posthog, id), host(family, id))) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("duplicate mount host id")
        // By id, as a verified claim could name a host later; an unknown id names nothing.
        val other = UUID.randomUUID()
        StaticMountHosts(listOf(host(posthog, id), host(family, other))).use { hosts ->
            assertThat(hosts.host(id)!!.host.match).isEqualTo(posthog)
            assertThat(hosts.host(other)!!.host.match).isEqualTo(family)
            assertThat(hosts.host(UUID.randomUUID())).isNull()
        }
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
    fun `the whole application cuts a body that fails mid-stream, and answers its typed errors`() {
        // The real App and its full pipeline (blocking dispatch, logging, metrics), with no PostgreSQL:
        // the token places `lake` on a hoglite engine that answers the one catalog read the schema needs.
        val body = ("[" + (1..60_000).joinToString(",") { """{"data_file_id":$it}""" } + "]").toByteArray()
        val engine =
            HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
                createContext("/") { exchange ->
                    val row = """["lake","s3://bucket/lake",3,2,0,null,null,0]"""
                    exchange.json(200, """{"cols":[],"rows":[$row],"affected":0}""")
                }
                start()
            }
        StubHost().use { host ->
            val hosts =
                Files.writeString(
                    dir.resolve("hosts.json"),
                    """{"hosts":[{"id":"3c0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e","match":{"type":"namespace",""" +
                        """"name":"posthog"},"kind":"hoglake","url":"${host.origin}","auth":"forward",""" +
                        """"allow_insecure_http":true,"max_response_bytes":400000}]}""",
                )
            val keys = Files.writeString(dir.resolve("jwks.json"), TestTokens.jwks(pair))
            val backend = """"id":"$hogliteId","kind":"hoglite","url":"http://127.0.0.1:${engine.address.port}""""
            val backends = Files.writeString(dir.resolve("engine.json"), """{"backends":[{$backend}]}""")
            val cfg =
                Config(
                    port = 0,
                    backendsFile = backends.toString(),
                    defaultBackend = "none",
                    jdbcUrl = "jdbc:no-such-driver:must-not-open",
                    dbUser = "",
                    dbPassword = "",
                    authMode = "enforce",
                    authIssuer = TestTokens.ISSUER,
                    authJwksFile = keys.toString(),
                    authPredicateFields = "team_id",
                    authSessionProfile = "on",
                    mountHostsFile = hosts.toString(),
                )
            val iat = System.currentTimeMillis() / 1000 - 5
            val profile = TestTokens.sessionProfile("lake", """{"posthog":["read"]}""")
            val routed = ""","rtg":{"b":"hoglite","be":"$hogliteId","t":"t_1"}"""
            val claims =
                TestTokens.scopedClaims(catalogs = """{"lake":["read"]}""", iat = iat, extra = profile + routed)
            val token = TestTokens.mint(pair, claims)
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
                    engine.stop(0)
                }
            }
        }
    }
}
