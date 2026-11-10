package com.posthog.hoglake.store

import com.posthog.hoglake.Config
import com.posthog.hoglake.api.installApiRoutes
import com.posthog.hoglake.api.installErrorMapping
import com.posthog.hoglake.api.installScanRoutes
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthPolicy
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.Caller
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.auth.installAuth
import com.posthog.hoglake.auth.installNamespaceGate
import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.observability.RequestId
import com.posthog.hoglake.store.FakeTokenEndpoint.Companion.error
import com.posthog.hoglake.store.FakeTokenEndpoint.Companion.json
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import io.ktor.client.request.header
import io.ktor.client.request.request
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.lang.reflect.Proxy
import java.net.InetSocketAddress
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.security.interfaces.ECPublicKey
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors

/**
 * A mount Client whose host is reached by token exchange (`auth:
 * exchange`), through the real auth plugin, namespace gate and catalog
 * routes: who is served, what the Host is given, and what a reader is
 * answered when the issuer says no. The Host is a stub that records every
 * request; the issuer is [FakeTokenEndpoint].
 */
class MountExchangeProxyTest {
    @TempDir
    lateinit var dir: Path
    private val pair = TestTokens.keyPair()
    private val org = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f0"
    private val otherOrg = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f1"
    private val unfederated = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f2"
    private val mount = "$org-events"
    private val otherMount = "$otherOrg-events"
    private val hogliteId = UUID.fromString("5b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e")
    private val scan = "/v1/catalogs/$mount/namespaces/ingest/tables/events_v2/scan"
    private val reads =
        listOf(
            "/v1/catalogs/$mount",
            "/v1/catalogs/$mount/namespaces",
            "/v1/catalogs/$mount/namespaces/ingest",
            "/v1/catalogs/$mount/namespaces/ingest/tables",
            "/v1/catalogs/$mount/namespaces/ingest/tables/events_v2",
            scan,
        )

    /** One request as the stub Host received it. */
    private data class Seen(
        val method: String,
        val rawPath: String,
        val headers: Map<String, List<String>>,
    )

    private class StubHost : AutoCloseable {
        val seen = CopyOnWriteArrayList<Seen>()
        private val workers = Executors.newCachedThreadPool()

        @Volatile
        var answer: (HttpExchange) -> Unit = { it.json(200, "[]") }
        private val server =
            HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).also { server ->
                server.executor = workers
                server.createContext("/") { exchange ->
                    seen +=
                        Seen(
                            exchange.requestMethod,
                            exchange.requestURI.rawPath,
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
        val origin: URI get() = URI("http://127.0.0.1:${server.address.port}")

        override fun close() {
            server.stop(0)
            workers.shutdownNow()
        }
    }

    /** A PostgreSQL store holding one catalog, `lake`. */
    private val postgres =
        Proxy.newProxyInstance(CatalogStore::class.java.classLoader, arrayOf(CatalogStore::class.java)) { _, m, _ ->
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

    @AfterEach
    fun unbindMetrics() = Metrics.clear()

    private fun endpoint() = FakeTokenEndpoint(pair, federated = mapOf(org to setOf(7L, 42L), otherOrg to setOf(7L)))

    private fun config(default: String): Config {
        val file = dir.resolve("backends-${UUID.randomUUID()}.json")
        val backend = """"id":"$hogliteId","kind":"hoglite","url":"http://127.0.0.1:9","retry_budget_ms":1"""
        Files.writeString(file, """{"backends":[{$backend}]}""")
        return Config(backendsFile = file.toString(), defaultBackend = default, authMode = "enforce")
    }

    /** A reader's token: scoped, for [org]'s catalog unless [catalogs] says otherwise, with [life] seconds left. */
    private fun reader(
        org: String = this.org,
        catalogs: String = """{"$org":["read"]}""",
        life: Long = 900,
        routed: Boolean = false,
        prd: String = """{"team_id":{"7":"read","42":"read"}}""",
    ): String {
        val now = System.currentTimeMillis() / 1000
        val rtg = if (routed) ""","rtg":{"b":"hoglite","be":"$hogliteId","t":"t_1"}""" else ""
        return TestTokens.mint(
            pair,
            """{"iss":"${TestTokens.ISSUER}","aud":"${TestTokens.AUDIENCE}","sub":"hlc_team",""" +
                """"jti":"jti-${UUID.randomUUID()}","ten":"scoped","org":"$org","cli":"laptop","usr":"user-1",""" +
                """"iat":${maxOf(now - 5, now + life - 900)},"exp":${now + life},"cat":$catalogs,"prd":$prd$rtg}""",
        )
    }

    private fun tenant(catalog: String) =
        TestTokens.mint(pair, TestTokens.tenantClaims(org = org, catalogs = mapOf(catalog to listOf("admin"))))

    private fun operator() = TestTokens.mint(pair, TestTokens.operatorClaims())

    private fun exchangeHost(
        host: StubHost,
        endpoint: FakeTokenEndpoint,
        match: MountMatch = MountMatch.Events,
    ) = MountHost(
        id = UUID.randomUUID(),
        match = match,
        kind = MountHost.Kind.HOGLAKE,
        origin = host.origin,
        auth = MountHost.Auth.EXCHANGE,
        timeoutMs = 5_000,
        exchange = endpoint.exchangeConfig(timeoutMs = 2_000),
    )

    private fun forwardHost(
        host: StubHost,
        match: MountMatch,
    ) = MountHost(
        UUID.randomUUID(),
        match,
        MountHost.Kind.HOGLAKE,
        host.origin,
        MountHost.Auth.FORWARD,
        timeoutMs = 5_000,
    )

    /** A Client with [hosts], as `App` wires one: the same host list feeds the resolver and the auth service. */
    private fun client(
        hosts: List<MountHost>,
        default: String = "none",
        exchangeMount: (Caller) -> String? = AuthService.exchangeMounts(hosts),
        routes: Application.(CatalogStoreSelector, StaticMountHosts) -> Unit = { selector, _ ->
            installApiRoutes(selector)
            installScanRoutes(selector)
        },
        block: suspend ApplicationTestBuilder.() -> Unit,
    ) = testApplication {
        val cfg = config(default)
        val resolver = StaticMountHosts(hosts)
        application {
            val verifier =
                JwtVerifier(
                    TestTokens.ISSUER,
                    TestTokens.AUDIENCE,
                    StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey),
                    predicateFields = setOf("team_id"),
                    backends = cfg.backendKinds,
                )
            val selector = CatalogStoreSelector(postgres, cfg, MountRegistry.NONE, resolver)
            install(RequestId)
            installAuth(AuthService(AuthMode.ENFORCE, verifier, exchangeMount = exchangeMount))
            installNamespaceGate(selector)
            install(ContentNegotiation) { jackson { configureHoglakeWire() } }
            install(StatusPages) { installErrorMapping() }
            routes(selector, resolver)
        }
        try {
            block()
        } finally {
            resolver.close()
        }
    }

    private suspend fun ApplicationTestBuilder.send(
        method: HttpMethod,
        path: String,
        token: String?,
        requestId: String? = null,
    ): HttpResponse =
        client.request(path) {
            this.method = method
            token?.let { header(HttpHeaders.Authorization, "Bearer $it") }
            requestId?.let { header("X-Request-Id", it) }
            if (method != HttpMethod.Get && method != HttpMethod.Delete) {
                header(HttpHeaders.ContentType, "application/json")
                setBody("{}")
            }
        }

    private suspend fun ApplicationTestBuilder.get(
        path: String,
        token: String?,
        requestId: String? = null,
    ) = send(HttpMethod.Get, path, token, requestId)

    private fun error(body: String): String = body.substringAfter("\"error\":\"", "").substringBefore('"')

    /** Every catalog route in the route table, with its parameters filled for [catalog]. */
    private fun catalogRoutes(catalog: String): List<Pair<AuthPolicy.Entry, String>> =
        AuthPolicy.rules.filter { "{catalog}" in it.pattern }.map { rule ->
            rule to
                rule.pattern
                    .replace("{catalog}", catalog)
                    .replace("{namespace}", "ingest")
                    .replace("{table}", "events_v2")
                    .replace(Regex("\\{[a-zA-Z]+}"), "00000000-0000-0000-0000-000000000001")
        }

    @Test
    fun `an org-token reader is served with the token minted for it, and the host never sees the reader's`() {
        StubHost().use { host ->
            endpoint().use { endpoint ->
                val body = """[ {"name":"ingest",  "future_field":{"x":[1,2]}} ]"""
                host.answer = { it.json(200, body) }
                client(listOf(exchangeHost(host, endpoint))) {
                    // A real reader's token places its org catalog on hoglite; one without a placement works too.
                    for (token in listOf(reader(routed = true), reader())) {
                        host.seen.clear()
                        endpoint.seen.clear()
                        for (route in reads) {
                            val response = get(route, token, requestId = "req-exchange")
                            assertThat(response.status).describedAs(route).isEqualTo(HttpStatusCode.OK)
                            assertThat(response.bodyAsText()).describedAs(route).isEqualTo(body)
                        }
                        // Six requests, one exchange, for this reader's own token and its own org's mount.
                        val exchanged = endpoint.seen.single()
                        assertThat(exchanged.field("subject_token")).isEqualTo(token)
                        assertThat(exchanged.field("resource")).isEqualTo("urn:posthog:hoglake:catalog:$mount")
                        val minted = endpoint.minted.last()
                        assertThat(host.seen.map { it.rawPath }).isEqualTo(reads)
                        for (seen in host.seen) {
                            assertThat(seen.method).isEqualTo("GET")
                            assertThat(seen.headers["authorization"]).containsExactly("Bearer $minted")
                            assertThat(seen.headers["x-request-id"]).containsExactly("req-exchange")
                            assertThat(
                                seen.headers.values.flatten(),
                            ).noneMatch { token in it || endpoint.clientSecret in it }
                        }
                    }
                    assertThat(endpoint.minted).hasSize(2).doesNotHaveDuplicates()
                }
            }
        }
    }

    @Test
    fun `anyone but the org's own scoped reader is refused here, with no exchange and nothing sent to the host`() {
        StubHost().use { host ->
            endpoint().use { endpoint ->
                client(listOf(exchangeHost(host, endpoint))) {
                    val callers =
                        mapOf(
                            "another org's reader" to reader(org = otherOrg),
                            "a reader whose org is another's but whose cat names this org's catalog" to
                                reader(org = otherOrg, catalogs = """{"$org":["read"]}"""),
                            "a scoped token for another catalog" to reader(catalogs = """{"lake":["read"]}"""),
                            "a token that names the mount itself" to reader(catalogs = """{"$mount":["read"]}"""),
                            "a token that names another org's mount" to
                                reader(catalogs = """{"$otherMount":["read"]}"""),
                            "a tenant of the org catalog" to tenant(org),
                            "a tenant naming the mount" to tenant(mount),
                            "an operator" to operator(),
                            "nobody" to null,
                        )
                    for ((who, token) in callers) {
                        for ((rule, path) in catalogRoutes(mount)) {
                            val response = send(rule.method, path, token)
                            assertThat(response.status.value).describedAs("%s %s %s", who, rule.method.value, path)
                                .isIn(401, 403, 404)
                        }
                        val read = get(scan, token)
                        assertThat(read.status.value).describedAs(who).isEqualTo(if (token == null) 401 else 404)
                        if (token != null) {
                            assertThat(read.bodyAsText()).describedAs(who)
                                .isEqualTo("""{"error":"not_found","detail":"catalog '$mount' not found"}""")
                            // (Another org's reader has a mount of its own to list: checked below.)
                            if (who != "another org's reader") {
                                val listed = get("/v1/catalogs", token).bodyAsText()
                                assertThat(listed).describedAs(who).doesNotContain("events")
                            }
                        }
                    }
                    // The reader itself on the routes a mount does not serve: answered here too. (The
                    // exact codes are pinned against a whole server in MountClientIntegrationTest; this
                    // application installs only some of the routes, and one it lacks is a plain 404.)
                    for (cat in listOf("read", "write")) {
                        val own = reader(catalogs = """{"$org":["$cat"]}""")
                        for ((rule, path) in catalogRoutes(mount)) {
                            if (rule.method == HttpMethod.Get && path in reads) continue
                            val response = send(rule.method, path, own)
                            assertThat(response.status.value).describedAs("%s %s (%s)", rule.method.value, path, cat)
                                .isIn(403, 404, 501)
                        }
                        val write = send(HttpMethod.Post, "/v1/catalogs/$mount/commit", own)
                        assertThat(write.status.value to error(write.bodyAsText())).isEqualTo(403 to "forbidden")
                    }
                    assertThat(endpoint.seen).isEmpty()
                    assertThat(host.seen).isEmpty()
                    // Another org's reader listing its catalogs is asked about, and shown, its own mount only.
                    host.answer = { it.json(200, """[{"name":"$mount"},{"name":"$otherMount"}]""") }
                    assertThat(get("/v1/catalogs", callers.getValue("another org's reader")).bodyAsText())
                        .isEqualTo("""[{"name":"$otherMount"}]""")
                    assertThat(endpoint.seen.single().field("resource"))
                        .isEqualTo("urn:posthog:hoglake:catalog:$otherMount")
                }
            }
        }
    }

    @Test
    fun `the issuer's answers reach the reader as typed errors that name nothing`() {
        StubHost().use { host ->
            endpoint().use { endpoint ->
                client(listOf(exchangeHost(host, endpoint))) {
                    val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
                    val secrets = mutableListOf(endpoint.clientSecret, endpoint.clientId, "127.0.0.1", "oauth", "hls_")

                    suspend fun answered(
                        token: String,
                        status: Int,
                        code: String,
                    ): HttpResponse {
                        secrets += token
                        val response = get(scan, token)
                        val body = response.bodyAsText()
                        assertThat(response.status.value to error(body)).isEqualTo(status to code)
                        assertThat(secrets).noneMatch { it in body || it in response.headers.toString() }
                        return response
                    }
                    // Another org's mount is the plugin's 404; these two are the same words for this org's.
                    val elsewhere = get("/v1/catalogs/$otherMount", reader()).bodyAsText()
                    val unknown = """{"error":"not_found","detail":"catalog '$mount' not found"}"""
                    assertThat(elsewhere).isEqualTo(unknown.replace(mount, otherMount))
                    endpoint.answer = { e, _ -> e.error(400, "invalid_target") }
                    assertThat(answered(reader(), 404, "not_found").bodyAsText()).isEqualTo(unknown)
                    endpoint.answer = { e, _ -> e.error(400, "invalid_grant") }
                    assertThat(answered(reader(), 404, "not_found").bodyAsText()).isEqualTo(unknown)
                    // The contract's own refusals: an org with no federated team, a reader with no federated team.
                    endpoint.answer = null
                    assertThat(
                        get("/v1/catalogs/$unfederated-events", reader(org = unfederated)).status.value,
                    ).isEqualTo(404)
                    answered(reader(prd = """{"team_id":{"9":"read"}}"""), 404, "not_found")

                    // A token about to expire is the reader's to renew, and is not sent anywhere.
                    val before = endpoint.seen.size
                    val expiring = answered(reader(life = 20), 401, "token_expired")
                    assertThat(
                        expiring.headers[HttpHeaders.WWWAuthenticate],
                    ).isEqualTo("Bearer error=\"token_expired\"")
                    assertThat(endpoint.seen).hasSize(before)

                    endpoint.answer = { e, _ -> e.json(200, """{"access_token":"${endpoint.clientSecret}"}""") }
                    answered(reader(), 502, "mount_exchange_invalid_response")
                    endpoint.answer = { e, _ -> e.error(503, "temporarily_unavailable") }
                    val unavailable = answered(reader(), 503, "mount_exchange_unavailable")
                    assertThat(unavailable.headers[HttpHeaders.RetryAfter]).isEqualTo("1")
                    // The issuer refuses this server's client: a 500 of its own, not the reader's fault.
                    endpoint.answer = { e, _ -> e.error(401, "invalid_client") }
                    val misconfigured = answered(reader(), 500, "mount_exchange_misconfigured")
                    assertThat(misconfigured.headers[HttpHeaders.WWWAuthenticate]).isNull()
                    assertThat(misconfigured.headers[HttpHeaders.RetryAfter]).isNull()

                    assertThat(host.seen).isEmpty()
                    // Counted by reason; and an org the issuer minted nothing for has no series of its own.
                    val reasons =
                        registry.find("hoglake_mount_exchange_errors_total").counters()
                            .associate { it.id.getTag("reason") to it.count() }
                    assertThat(reasons).isEqualTo(
                        mapOf(
                            "invalid_target" to 2.0,
                            "invalid_grant" to 2.0,
                            "near_expiry" to 1.0,
                            "invalid_response" to 1.0,
                            "temporarily_unavailable" to 1.0,
                            "invalid_client" to 1.0,
                        ),
                    )
                    val labels = registry.meters.flatMap { meter -> meter.id.tags.map { it.key to it.value } }
                    assertThat(labels.filter { it.first == "catalog" }).isEmpty()
                    assertThat(labels.map { it.second }).noneMatch { org in it || "hls_" in it || "hlc_" in it }
                }
            }
        }
    }

    @Test
    fun `an endpoint that is down is a typed 503 and the host is not asked`() {
        StubHost().use { host ->
            val endpoint = endpoint()
            val hosts = listOf(exchangeHost(host, endpoint))
            endpoint.close()
            client(hosts) {
                val response = get(scan, reader())
                assertThat(
                    response.status.value to error(response.bodyAsText()),
                ).isEqualTo(503 to "mount_exchange_unavailable")
                assertThat(response.headers[HttpHeaders.RetryAfter]).isEqualTo("1")
                assertThat(response.bodyAsText()).doesNotContain("127.0.0.1", "${endpoint.port}", "oauth")
                assertThat(host.seen).isEmpty()
            }
        }
    }

    @Test
    fun `the listing shows the reader's mount as the host lists it, and leaves it out when there is none`() {
        StubHost().use { host ->
            endpoint().use { endpoint ->
                val entry = """{"name":"$mount","data_path":"s3://bucket/millrace","read_only":true}"""
                val others = """{"name":"$otherMount"},{"name":"lake","data_path":"s3://host"}"""
                host.answer = { it.json(200, "[$entry,$others]") }
                client(listOf(exchangeHost(host, endpoint)), default = "postgres") {
                    val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
                    val token = reader()
                    val listed = get("/v1/catalogs", token).bodyAsText()
                    // The Client's own catalog, then the Host's entry for this reader's mount and nothing else of the Host's.
                    assertThat(
                        listed,
                    ).startsWith("""[{"name":"lake","data_path":"s3://bucket/lake"""").endsWith(",$entry]")
                    assertThat(listed).doesNotContain(otherMount, "s3://host")
                    assertThat(host.seen.single().rawPath).isEqualTo("/v1/catalogs")
                    assertThat(
                        host.seen.single().headers["authorization"],
                    ).containsExactly("Bearer ${endpoint.minted.single()}")
                    // The same token serves the listing and the reads: still one exchange.
                    assertThat(get(scan, token).status).isEqualTo(HttpStatusCode.OK)
                    assertThat(endpoint.seen).hasSize(1)

                    // No mount for this caller: the listing is its catalogs, and the Host is not asked.
                    host.seen.clear()
                    for (none in listOf(
                        reader(org = unfederated),
                        reader(prd = """{"team_id":{"9":"read"}}"""),
                        reader(life = 20),
                    )) {
                        val response = get("/v1/catalogs", none)
                        assertThat(response.status).isEqualTo(HttpStatusCode.OK)
                        assertThat(response.bodyAsText()).startsWith("""[{"name":"lake"""").doesNotContain("events")
                    }
                    assertThat(host.seen).isEmpty()
                    assertThat(registry.counter("hoglake_mount_exchange_listing_omitted_total").count()).isZero()

                    // The exchange cannot be made: whether there is a mount is unknown. The listing
                    // still answers, without it, and says so in a counter; a read of the mount is the typed error.
                    for ((code, status) in listOf("temporarily_unavailable" to 503, "invalid_client" to 401)) {
                        endpoint.answer = { e, _ -> e.error(status, code) }
                        val unknown = reader()
                        val response = get("/v1/catalogs", unknown)
                        assertThat(response.status).describedAs(code).isEqualTo(HttpStatusCode.OK)
                        assertThat(response.bodyAsText()).startsWith("""[{"name":"lake"""").doesNotContain("events")
                    }
                    assertThat(registry.counter("hoglake_mount_exchange_listing_omitted_total").count()).isEqualTo(2.0)
                    assertThat(get(scan, reader()).status.value).isEqualTo(500)
                    assertThat(host.seen).isEmpty()

                    // With a token in hand the mount is known to exist: the Host failing is the listing failing.
                    host.answer = { it.json(503, """{"error":"mount_scan_busy","detail":"busy"}""") }
                    val known = get("/v1/catalogs", token)
                    assertThat(known.status.value to error(known.bodyAsText())).isEqualTo(503 to "mount_scan_busy")
                }
            }
        }
    }

    @Test
    fun `a host that refuses the minted token is this server's error, and the token is not presented again`() {
        StubHost().use { host ->
            endpoint().use { endpoint ->
                host.answer = {
                    val refusal = """{"error":"token_expired","detail":"token refused"}"""
                    it.json(401, refusal, headers = mapOf("WWW-Authenticate" to "Bearer"))
                }
                client(listOf(exchangeHost(host, endpoint))) {
                    val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
                    val token = reader()
                    for (attempt in 1..2) {
                        val response = get(scan, token)
                        // Not the reader's 401: it never held that token, and renewing its own changes nothing.
                        assertThat(response.status.value to error(response.bodyAsText()))
                            .isEqualTo(502 to "mount_host_invalid_response")
                        assertThat(response.headers[HttpHeaders.WWWAuthenticate]).isNull()
                        assertThat(response.bodyAsText()).doesNotContain(endpoint.minted.last())
                        // Each attempt exchanged afresh: the refused token was dropped.
                        assertThat(endpoint.seen).hasSize(attempt)
                    }
                    assertThat(host.seen.map { it.headers["authorization"]!!.single() })
                        .containsExactly("Bearer ${endpoint.minted[0]}", "Bearer ${endpoint.minted[1]}")
                    assertThat(
                        registry.counter(
                            "hoglake_mount_proxy_upstream_errors_total",
                            "catalog",
                            mount,
                            "reason",
                            "http_401",
                        )
                            .count(),
                    ).isEqualTo(2.0)
                    // Any other refusal of the Host's is relayed as before.
                    host.answer = { it.json(404, """{"error":"not_found","detail":"table 'x'"}""") }
                    val relayed = get(scan, token)
                    assertThat(relayed.status.value to relayed.bodyAsText())
                        .isEqualTo(404 to """{"error":"not_found","detail":"table 'x'"}""")
                }
            }
        }
    }

    @Test
    fun `a forwarding host beside an exchanging one behaves as it always did`() {
        StubHost().use { forwarding ->
            StubHost().use { exchanging ->
                endpoint().use { endpoint ->
                    val hosts =
                        listOf(
                            forwardHost(forwarding, MountMatch.Catalog(otherMount)),
                            exchangeHost(exchanging, endpoint, MountMatch.Catalog(mount)),
                        )
                    client(hosts) {
                        val otherScan = scan.replace(mount, otherMount)
                        // Its own reader holds a token that names the mount, and that token is what the host gets.
                        val named = reader(org = otherOrg, catalogs = """{"$otherMount":["read"]}""")
                        assertThat(get(otherScan, named).status).isEqualTo(HttpStatusCode.OK)
                        assertThat(forwarding.seen.single().headers["authorization"]).containsExactly("Bearer $named")
                        assertThat(get("/v1/catalogs", named).status).isEqualTo(HttpStatusCode.OK)
                        assertThat(forwarding.seen.last().rawPath).isEqualTo("/v1/catalogs")
                        // An org-catalog token is nobody there, exactly as before exchange mode existed.
                        forwarding.seen.clear()
                        val orgToken = reader(org = otherOrg)
                        val refused = get(otherScan, orgToken)
                        assertThat(refused.status.value to error(refused.bodyAsText())).isEqualTo(404 to "not_found")
                        assertThat(get("/v1/catalogs", orgToken).bodyAsText()).doesNotContain("events")
                        // And a mount-named token placed on hoglite is still the 501 it was.
                        val placed = reader(org = otherOrg, catalogs = """{"$otherMount":["read"]}""", routed = true)
                        assertThat(error(get(otherScan, placed).bodyAsText())).isEqualTo("not_supported_on_backend")
                        assertThat(forwarding.seen).isEmpty()
                        assertThat(endpoint.seen).isEmpty()
                        assertThat(exchanging.seen).isEmpty()
                        // The exchanging host still serves its own org's reader.
                        assertThat(get(scan, reader()).status).isEqualTo(HttpStatusCode.OK)
                        assertThat(exchanging.seen).hasSize(1)
                        assertThat(forwarding.seen).isEmpty()
                    }
                    // With forwarding hosts only, no caller is given an exchange mount at all.
                    assertThat(AuthService.exchangeMounts(hosts.take(1))(verified(reader()))).isNull()
                    assertThat(AuthService.exchangeMounts(hosts)(verified(reader()))).isEqualTo(mount)
                    // The other org's mount is served by forwarding, so its org-token reader has none either.
                    assertThat(AuthService.exchangeMounts(hosts)(verified(reader(org = otherOrg)))).isNull()
                }
            }
        }
    }

    @Test
    fun `the proxy itself exchanges for nobody but the mount's reader, whoever calls it`() {
        StubHost().use { host ->
            endpoint().use { endpoint ->
                // A route that hands every caller the route table lets through straight to the proxy,
                // with no binding in between: what a later caller of the proxy could get wrong.
                client(
                    listOf(exchangeHost(host, endpoint)),
                    routes = { _, hosts ->
                        routing {
                            get("/v1/catalogs/{catalog}") {
                                val name = call.parameters["catalog"]!!
                                hosts.resolve(null, name)!!.catalog(call, name)
                            }
                        }
                    },
                ) {
                    val callers =
                        mapOf(
                            "a token that names the mount itself" to reader(catalogs = """{"$mount":["read"]}"""),
                            "a reader whose org is another's" to
                                reader(org = otherOrg, catalogs = """{"$mount":["read"],"$org":["read"]}"""),
                            "a tenant naming the mount" to tenant(mount),
                            "an operator" to operator(),
                        )
                    for ((who, token) in callers) {
                        val response = get("/v1/catalogs/$mount", token)
                        assertThat(response.status.value).describedAs(who).isEqualTo(404)
                        assertThat(response.bodyAsText()).describedAs(who)
                            .isEqualTo("""{"error":"not_found","detail":"catalog '$mount' not found"}""")
                    }
                    assertThat(endpoint.seen).isEmpty()
                    assertThat(host.seen).isEmpty()
                    assertThat(get("/v1/catalogs/$mount", reader()).status).isEqualTo(HttpStatusCode.OK)
                    assertThat(endpoint.seen).hasSize(1)
                }
            }
        }
    }

    @Test
    fun `an exchange mount with no host behind it is nothing, never the catalog of that name`() {
        // The route table would open the name; the binding must not fall through to a backend,
        // where this test's PostgreSQL answers for any name it is asked about.
        client(emptyList(), default = "postgres", exchangeMount = { mount }) {
            val token = reader()
            for (path in listOf("/v1/catalogs/$mount", "/v1/catalogs/$mount/namespaces", scan)) {
                val response = get(path, token)
                assertThat(response.status.value).describedAs(path).isEqualTo(404)
                assertThat(response.bodyAsText()).describedAs(path)
                    .isEqualTo("""{"error":"not_found","detail":"catalog '$mount' not found"}""")
            }
            // Its own catalogs are as ever.
            assertThat(get("/v1/catalogs/lake", operator()).bodyAsText()).contains("s3://bucket/lake")
        }
    }

    private fun verified(token: String) =
        JwtVerifier(
            TestTokens.ISSUER,
            TestTokens.AUDIENCE,
            StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey),
            predicateFields = setOf("team_id"),
        ).verify(token)

    @Test
    fun `the entitlement is the org's own catalog at read, for a scoped caller, and nothing else`() {
        fun mountOf(token: String) = MountEntitlement.mountOf(verified(token))
        assertThat(mountOf(reader())).isEqualTo(mount)
        assertThat(mountOf(reader(catalogs = """{"$org":["write"]}"""))).isEqualTo(mount)
        assertThat(mountOf(reader(catalogs = """{"$org":["read"],"lake":["read"]}"""))).isEqualTo(mount)
        assertThat(mountOf(reader(org = otherOrg))).isEqualTo(otherMount)
        // The org claim and the catalog must be the same org.
        assertThat(mountOf(reader(org = otherOrg, catalogs = """{"$org":["read"]}"""))).isNull()
        assertThat(mountOf(reader(catalogs = """{"$mount":["read"]}"""))).isNull()
        assertThat(mountOf(reader(catalogs = """{"lake":["read"]}"""))).isNull()
        // An org id that is not a canonical UUID has no mount name.
        assertThat(mountOf(reader(org = "acme", catalogs = """{"acme":["read"]}"""))).isNull()
        assertThat(mountOf(tenant(org))).isNull()
        assertThat(mountOf(operator())).isNull()
        assertThat(MountEntitlement.mountOf(null)).isNull()
        assertThat(MountEntitlement.mayRead(verified(reader()), mount)).isTrue()
        assertThat(MountEntitlement.mayRead(verified(reader()), otherMount)).isFalse()
        assertThat(MountEntitlement.mayRead(null, mount)).isFalse()
    }
}
