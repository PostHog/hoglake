package com.posthog.hoglake.store

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.posthog.hoglake.Config
import com.posthog.hoglake.api.installAlterRoutes
import com.posthog.hoglake.api.installApiRoutes
import com.posthog.hoglake.api.installErrorMapping
import com.posthog.hoglake.api.installMaintenanceRoutes
import com.posthog.hoglake.api.installPartitionListingRoutes
import com.posthog.hoglake.api.installPartitionLookupRoutes
import com.posthog.hoglake.api.installPartitionStatsRoutes
import com.posthog.hoglake.api.installScanRoutes
import com.posthog.hoglake.api.installViewRoutes
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthPolicy
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.Requirement
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.auth.installAuth
import com.posthog.hoglake.auth.installNamespaceGate
import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.model.ConsumerOffset
import com.posthog.hoglake.model.ConsumerTableOffset
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.model.NamespaceInfo
import com.posthog.hoglake.model.PartitionDebt
import com.posthog.hoglake.model.PartitionStatsReport
import com.posthog.hoglake.model.RehydrateResult
import com.posthog.hoglake.model.TableSummaryInfo
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.observability.RequestId
import com.posthog.hoglake.service.HydratorMaintenanceBackend
import com.posthog.hoglake.service.PartitionStatsBackend
import com.posthog.hoglake.wireObjectMapper
import com.sun.net.httpserver.HttpExchange
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
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.slf4j.LoggerFactory
import java.lang.reflect.Proxy
import java.net.InetSocketAddress
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.security.interfaces.ECPublicKey
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors

/**
 * A remote schema on its Client (docs/auth-tenancy.md §6.5c) through the
 * real auth plugin, namespace gate and routes, against a stub Host that
 * records every request it gets. The Client's catalog `lake` is local and
 * already holds namespaces named `posthog` and `PostHog`; the store records
 * every call, so "never read or written" is "the store was never asked".
 */
class RemoteSchemaClientTest {
    @TempDir
    lateinit var dir: Path
    private val pair = TestTokens.keyPair()
    private val mapper = wireObjectMapper()
    private val hogliteId = UUID.fromString("5b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e")
    private val mount = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f0-events"
    private val schema = "/v1/catalogs/lake/namespaces/posthog"
    private val relayed = listOf(schema, "$schema/tables", "$schema/tables/events", "$schema/tables/events/scan")

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
        val origin: URI get() = URI("http://127.0.0.1:${server.address.port}")

        override fun close() {
            server.stop(0)
            workers.shutdownNow()
        }
    }

    private fun HttpExchange.json(
        status: Int,
        body: String,
    ) {
        val bytes = body.toByteArray()
        responseHeaders.add("Content-Type", "application/json")
        sendResponseHeaders(status, bytes.size.toLong())
        responseBody.use { it.write(bytes) }
    }

    /** Every call the local store received: its method and arguments. */
    private val calls = CopyOnWriteArrayList<Pair<String, List<Any?>>>()

    /** The calls that name the remote schema or any case variant of it, plus every commit. */
    private fun touched() =
        calls.filter { (name, args) -> name == "commit" || args.any { (it as? String).equals("posthog", true) } }

    private val team = NamespaceContainment("team_id", 42)
    private val ownTable = UUID.fromString("00000000-0000-0000-0000-0000000000a1")
    private val hiddenTable = UUID.fromString("00000000-0000-0000-0000-0000000000b1")
    private val droppedTable = UUID.fromString("00000000-0000-0000-0000-0000000000b2")

    /** The PostgreSQL store behind `lake`: it lists a local `posthog`, contained for the reader's own team. */
    private val postgres =
        Proxy.newProxyInstance(CatalogStore::class.java.classLoader, arrayOf(CatalogStore::class.java)) { _, m, args ->
            calls += m.name to (args?.toList() ?: emptyList())
            // The store holds `lake`; a catalog named `ghost` does not exist in it.
            if (args?.firstOrNull() == "ghost") throw HoglakeException.NotFound("catalog 'ghost'")
            when (m.name) {
                "getCatalog" -> CatalogInfo(1, "lake", "s3://bucket/lake", headSnapshotId = 3, schemaVersion = 2)
                "listNamespaces" ->
                    listOf(
                        NamespaceInfo(1, "team_7", team),
                        NamespaceInfo(2, "posthog", team),
                        NamespaceInfo(3, "PostHog", team),
                        NamespaceInfo(4, "other"),
                    )
                "liveContainment" -> team.takeUnless { args!![1] == "nope" }
                "getNamespace" -> NamespaceInfo(2, args!![1] as String, team)
                // The local `posthog` holds one live table; consumers hold offsets on it and on others.
                "listTables" ->
                    if (args!![1] == "posthog") {
                        listOf(TableSummaryInfo(9, hiddenTable, "events", null, 0, 0, 0, 0, null))
                    } else {
                        emptyList()
                    }
                "listConsumers" ->
                    listOf(
                        ConsumerTableOffset("c1", ownTable, 3, Instant.EPOCH, "team_7", "t", false),
                        ConsumerTableOffset("c1", hiddenTable, 3, Instant.EPOCH, "posthog", "events", false),
                        ConsumerTableOffset("c2", droppedTable, 2, Instant.EPOCH, "PostHog", "old", true),
                    )
                "listOffsets" ->
                    listOf(
                        ConsumerOffset("c1", ownTable, 3, Instant.EPOCH),
                        ConsumerOffset("c1", hiddenTable, 3, Instant.EPOCH),
                    )
                "getOffset", "commitOffset" -> ConsumerOffset(args!![1] as String, args[2] as UUID, 3, Instant.EPOCH)
                else -> throw IllegalStateException("a route reached the local store: ${m.name}")
            }
        } as CatalogStore

    private val stats =
        object : PartitionStatsBackend {
            override fun partitionStats(
                catalog: String,
                namespace: String?,
                table: String?,
                limit: Int,
            ): PartitionStatsReport {
                fun debt(ns: String) =
                    PartitionDebt(ns, "events", UUID.randomUUID(), emptyList(), null, 1, 1, 1, 1, 1, 0, 1)
                return PartitionStatsReport(listOf("team_7", "posthog", "PostHog").map(::debt), false, 0, 1, null)
            }
        }

    private val hydrator =
        object : HydratorMaintenanceBackend {
            override fun rehydrateFailed(
                catalog: String,
                namespace: String?,
                table: String?,
            ) = RehydrateResult(0)
        }

    private fun config(
        engine: String = "http://127.0.0.1:9",
        defaultBackend: String = "postgres",
    ): Config {
        val file = dir.resolve("backends.json")
        // The default engine refuses every connection, so one attempt is all it gets; a stub engine answers.
        val budget = if (engine.endsWith(":9")) 1 else 5_000
        val backend = """"id":"$hogliteId","kind":"hoglite","url":"$engine","retry_budget_ms":$budget"""
        Files.writeString(file, """{"backends":[{$backend}]}""")
        return Config(backendsFile = file.toString(), defaultBackend = defaultBackend, authMode = "enforce")
    }

    /**
     * A hoglite engine that answers the one read a remote schema needs of it,
     * the catalog's metadata row, and counts every request it gets. With
     * [holds] false it has no such row: the catalog does not exist there.
     */
    private inner class StubEngine(
        private val holds: Boolean = true,
    ) : AutoCloseable {
        val requests = java.util.concurrent.atomic.AtomicInteger()
        private val server =
            HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
                createContext("/") { exchange ->
                    requests.incrementAndGet()
                    val sql = mapper.readTree(exchange.requestBody.readBytes()).path("sql").asText()
                    if (sql.startsWith("SELECT name, data_path,")) {
                        val row = """["lake","s3://bucket/lake",3,2,0,null,null,0]"""
                        val rows = if (holds) "[$row]" else "[]"
                        exchange.json(200, """{"cols":[],"rows":$rows,"affected":0}""")
                    } else {
                        exchange.sendResponseHeaders(503, -1)
                    }
                    exchange.close()
                }
                start()
            }
        val url: String get() = "http://127.0.0.1:${server.address.port}"

        override fun close() = server.stop(0)
    }

    @AfterEach
    fun unbindMetrics() = Metrics.clear()

    /**
     * A scoped token on one catalog. [schemas] is its `remote_schemas` claim,
     * carried in the session profile; null is a plain scoped token with no
     * profile at all.
     */
    private fun scoped(
        cat: String = """{"lake":["read"]}""",
        schemas: String? = """{"posthog":["read"]}""",
        rtg: String? = null,
    ): String =
        TestTokens.mint(
            pair,
            TestTokens.scopedClaims(
                catalogs = cat,
                // The profile's primary catalog is the one `cat` names.
                extra =
                    (schemas?.let { TestTokens.sessionProfile(cat.split('"')[1], it) } ?: "") +
                        (rtg?.let { ""","rtg":$it""" } ?: ""),
            ),
        )

    private fun tenant() = TestTokens.mint(pair, TestTokens.tenantClaims(catalogs = mapOf("lake" to listOf("admin"))))

    /** A tenant token that carries the claim: the verifier accepts it, and the schema is still not its to read. */
    private fun tenantSession(): String =
        TestTokens.mint(
            pair,
            TestTokens.tenantClaims(catalogs = mapOf("lake" to listOf("admin")), exp = 1_700_000_900).dropLast(1) +
                TestTokens.sessionProfile("lake", """{"posthog":["read"]}""") + "}",
        )

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
        // A backend none of these routes may reach for a remote schema: reaching one is a 500.
        installViewRoutes({ error("views reached") }, selector)
        installAlterRoutes({ error("alter reached") }, selector)
        installPartitionListingRoutes({ error("partition listing reached") }, selector)
        installPartitionLookupRoutes({ error("partition lookup reached") }, selector)
        installPartitionStatsRoutes({ stats }, selector)
        installMaintenanceRoutes(
            { error("options reached") },
            { error("expiry reached") },
            { error("cleanup reached") },
            { error("compaction reached") },
            { hydrator },
            { error("status reached") },
            { error("database health reached") },
            selector,
        )
    }

    /**
     * A Client under [mode] whose hosts are [matches], each at [host]; no
     * matches is a server that configures no remote schema.
     */
    private fun client(
        host: StubHost,
        mode: AuthMode = AuthMode.ENFORCE,
        matches: List<MountMatch> = listOf(MountMatch.Namespace("posthog")),
        origin: URI = host.origin,
        /** A remote schema served by a host of its own, by name; every other match is at [origin]. */
        origins: Map<String, URI> = emptyMap(),
        /** The hoglite backend's engine; the default refuses every connection. */
        engine: String = "http://127.0.0.1:9",
        /** `none` is a server with no PostgreSQL catalogs: only a routed token has a backend. */
        defaultBackend: String = "postgres",
        block: suspend ApplicationTestBuilder.() -> Unit,
    ) = testApplication {
        val cfg = config(engine, defaultBackend)
        val hosts =
            StaticMountHosts(
                matches.map {
                    val at = (it as? MountMatch.Namespace)?.let { schema -> origins[schema.name] } ?: origin
                    // The stub Hosts speak http, which a remote schema's entry must opt in to.
                    val http = it is MountMatch.Namespace
                    MountHost(
                        UUID.randomUUID(),
                        it,
                        MountHost.Kind.HOGLAKE,
                        at,
                        MountHost.Auth.FORWARD,
                        5_000,
                        allowInsecureHttp = http,
                    )
                },
            )
        application { clientModule(CatalogStoreSelector(postgres, cfg, MountRegistry.NONE, hosts), mode, cfg) }
        try {
            block()
        } finally {
            hosts.close()
        }
    }

    private suspend fun ApplicationTestBuilder.send(
        method: HttpMethod,
        path: String,
        token: String?,
        body: String = "{}",
    ): HttpResponse =
        client.request(path) {
            this.method = method
            token?.let { header(HttpHeaders.Authorization, "Bearer $it") }
            header("X-Request-Id", "req-remote-schema")
            if (method != HttpMethod.Get && method != HttpMethod.Delete) {
                contentType(ContentType.Application.Json)
                setBody(body)
            }
        }

    private suspend fun ApplicationTestBuilder.get(
        path: String,
        token: String?,
    ) = send(HttpMethod.Get, path, token)

    private fun code(body: String): String = body.substringAfter("\"error\":\"", "").substringBefore('"')

    private fun namespaceRoutes() = RemoteSchemaFixtures.namespaceRoutes("lake")

    private fun isRelayed(rule: AuthPolicy.Entry) = RemoteSchemaFixtures.isRelayed(rule)

    @Test
    fun `the four routes go to the host with the caller's path, query and token, and the answer is unchanged`() {
        StubHost().use { host ->
            // Odd spacing and fields this server does not know: a re-serialised body would differ.
            val body = """{ "name":"posthog",  "remote":true, "snapshot_id":41, "future_field":{"x":[1,2]} }"""
            host.answer = { it.json(200, body) }
            client(host) {
                val token = scoped()
                for (path in relayed) {
                    val response = get(path, token)
                    assertThat(response.status).describedAs(path).isEqualTo(HttpStatusCode.OK)
                    assertThat(response.bodyAsText()).describedAs(path).isEqualTo(body)
                }
                // Relayed as it came: a value this server's own scan would refuse is the Host's to judge.
                val query = "snapshot=41&include=column_stats&include=nonsense&stats_fields=1,2&x=a%20b%26c"
                assertThat(get("${relayed[3]}?$query", token).status).isEqualTo(HttpStatusCode.OK)
                // The path is the caller's own: its catalog and the schema, not a name the Client holds.
                assertThat(host.seen.map { it.rawPath }).isEqualTo(relayed + relayed[3])
                assertThat(host.seen.map { it.rawQuery }).isEqualTo(listOf(null, null, null, null, query))
                for (seen in host.seen) {
                    assertThat(seen.method).isEqualTo("GET")
                    assertThat(seen.headers["authorization"]).containsExactly("Bearer $token")
                    assertThat(seen.headers["x-request-id"]).containsExactly("req-remote-schema")
                }
            }
            assertThat(touched()).isEmpty()
        }
    }

    @Test
    fun `a write scope on the catalog and a hoglite placement do not change what is relayed`() {
        StubHost().use { host ->
            StubEngine().use { engine ->
                client(host, engine = engine.url) {
                    val routed = """{"b":"hoglite","be":"$hogliteId","t":"t_1"}"""
                    for (token in listOf(scoped("""{"lake":["write"]}"""), scoped(rtg = routed))) {
                        for (path in relayed) {
                            assertThat(get(path, token).status).describedAs(path).isEqualTo(HttpStatusCode.OK)
                        }
                    }
                    assertThat(host.seen.map { it.rawPath }).isEqualTo(relayed + relayed)
                }
                // Each relayed request asked its catalog's own backend one thing: that the catalog exists.
                assertThat(calls.map { it.first to it.second }).isEqualTo(List(4) { "getCatalog" to listOf("lake") })
                assertThat(engine.requests.get()).isEqualTo(4)
            }
        }
    }

    @Test
    fun `a remote schema is relayed only in a catalog this server holds`() {
        val routed = """{"b":"hoglite","be":"$hogliteId","t":"t_1"}"""
        StubHost().use { host ->
            // PostgreSQL: the token names a catalog that does not exist, with the claim.
            client(host) {
                val ghost = scoped(cat = """{"ghost":["write"]}""")
                val listing = get("/v1/catalogs/ghost/namespaces", ghost)
                assertThat(listing.status).isEqualTo(HttpStatusCode.NotFound)
                for (path in relayed.map { it.replace("/lake/", "/ghost/") }) {
                    val response = get(path, ghost)
                    assertThat(response.status).describedAs(path).isEqualTo(HttpStatusCode.NotFound)
                    assertThat(response.bodyAsText()).describedAs(path).isEqualTo(listing.bodyAsText())
                }
                // A write there is the missing catalog too, not the schema's read-only answer.
                val write = send(HttpMethod.Post, "/v1/catalogs/ghost/namespaces/posthog/tables", ghost)
                assertThat(write.status).isEqualTo(HttpStatusCode.NotFound)
                // A caller the token check refuses costs no read: its answer is the missing namespace.
                calls.clear()
                val tenant =
                    TestTokens.mint(
                        pair,
                        TestTokens.tenantClaims(catalogs = mapOf("ghost" to listOf("admin"))),
                    )
                val refused = get("/v1/catalogs/ghost/namespaces/posthog/tables", tenant)
                assertThat(refused.bodyAsText()).contains("namespace 'posthog' in catalog 'ghost'")
                assertThat(calls).isEmpty()
                // A request that names no remote schema is not asked whether its catalog exists.
                get("/v1/catalogs/lake/namespaces/team_7/tables", operator())
                assertThat(calls.map { it.first }).containsExactly("listTables")
            }
            // hoglite: the engine the token routes to has no such catalog.
            StubEngine(holds = false).use { engine ->
                client(host, engine = engine.url) {
                    for (path in relayed) {
                        val response = get(path, scoped(rtg = routed))
                        assertThat(response.status).describedAs(path).isEqualTo(HttpStatusCode.NotFound)
                        assertThat(response.bodyAsText()).describedAs(path).contains("catalog 'lake'")
                    }
                }
                assertThat(engine.requests.get()).isEqualTo(relayed.size)
            }
            // A server with no PostgreSQL catalogs, and a token that routes nowhere: no backend holds it.
            client(host, defaultBackend = "none") {
                val listing = get("/v1/catalogs/lake/namespaces", scoped())
                assertThat(listing.status).isEqualTo(HttpStatusCode.NotFound)
                for (path in relayed) {
                    val response = get(path, scoped())
                    assertThat(response.status).describedAs(path).isEqualTo(HttpStatusCode.NotFound)
                    assertThat(response.bodyAsText()).describedAs(path).isEqualTo(listing.bodyAsText())
                }
            }
            assertThat(host.seen).isEmpty()
            assertThat(touched()).isEmpty()
        }
    }

    @Test
    fun `a HEAD request on a relayed route is not relayed and reads nothing`() {
        StubHost().use { host ->
            client(host) {
                for (path in relayed) {
                    for (token in listOf(scoped(), operator())) {
                        // The route table reads HEAD as GET; no route serves it, and none relays it.
                        val response = send(HttpMethod.Head, path, token)
                        assertThat(response.status).describedAs(path).isEqualTo(HttpStatusCode.NotFound)
                    }
                }
            }
            assertThat(host.seen).isEmpty()
            assertThat(touched()).isEmpty()
        }
    }

    @Test
    fun `a body is answered for its first namespace that fails, whether or not a schema is configured`() {
        // A scoped writer without the claim; `nope` is no namespace of the catalog.
        val first =
            """{"appends":[{"namespace":"nope","table":"t","files":[]},""" +
                """{"namespace":"posthog","table":"events","files":[]}]}"""
        val second =
            """{"appends":[{"namespace":"team_7","table":"t","files":[]},""" +
                """{"namespace":"posthog","table":"events","files":[]},{"namespace":"nope","table":"t","files":[]}]}"""
        StubHost().use { host ->
            val answers =
                listOf(listOf(MountMatch.Namespace("posthog")), emptyList()).map { matches ->
                    var body = ""
                    client(host, matches = matches) {
                        val writer = scoped("""{"lake":["write"]}""", schemas = null)
                        val response = send(HttpMethod.Post, "/v1/catalogs/lake/commit", writer, first)
                        assertThat(response.status).isEqualTo(HttpStatusCode.NotFound)
                        body = response.bodyAsText()
                        if (matches.isNotEmpty()) {
                            // The schema is the first that fails here, for the reader (read-only) and for others.
                            val others = send(HttpMethod.Post, "/v1/catalogs/lake/commit", writer, second)
                            assertThat(others.bodyAsText()).contains("namespace 'posthog' in catalog 'lake'")
                            val reader =
                                send(
                                    HttpMethod.Post,
                                    "/v1/catalogs/lake/commit",
                                    scoped("""{"lake":["write"]}"""),
                                    second,
                                )
                            assertThat(code(reader.bodyAsText())).isEqualTo("read_only_catalog")
                        }
                    }
                    body
                }
            // MUTATION: scan the whole body for a remote schema first and the configured server names `posthog`.
            assertThat(answers[0]).contains("namespace 'nope' in catalog 'lake'").isEqualTo(answers[1])
            assertThat(host.seen).isEmpty()
        }
    }

    @Test
    fun `a caller the rule refuses gets the 404 of a missing namespace and nothing is sent`() {
        StubHost().use { host ->
            for (mode in listOf(AuthMode.OFF, AuthMode.WARN, AuthMode.ENFORCE)) {
                client(host, mode) {
                    val refused =
                        mapOf(
                            "tenant" to tenant(),
                            "tenant with the claim" to tenantSession(),
                            "operator" to operator(),
                            "scoped without the claim" to scoped(schemas = null),
                            "scoped with an empty claim" to scoped(schemas = "{}"),
                            "scoped with another schema" to scoped(schemas = """{"imports":["read"]}"""),
                            "scoped with a case variant of the schema" to scoped(schemas = """{"PostHog":["read"]}"""),
                        )
                    for (path in relayed) {
                        for ((who, token) in refused) {
                            val response = get(path, token)
                            assertThat(response.status).describedAs("%s %s %s", mode, who, path)
                                .isEqualTo(HttpStatusCode.NotFound)
                            assertThat(code(response.bodyAsText())).isEqualTo("not_found")
                            assertThat(response.bodyAsText()).contains("namespace 'posthog' in catalog 'lake'")
                        }
                        // The claim without the catalog: the 404 of a catalog the token does not name,
                        // which is decided before the schema is looked at.
                        val other = get(path, scoped(cat = """{"pond":["read"]}"""))
                        assertThat(other.status).describedAs("%s %s", mode, path).isEqualTo(HttpStatusCode.NotFound)
                        if (mode != AuthMode.OFF) assertThat(other.bodyAsText()).contains("catalog 'lake' not found")
                        // No token: 401 under enforce, the rule's 404 where the plugin lets it through.
                        assertThat(get(path, null).status).describedAs("%s %s", mode, path)
                            .isEqualTo(
                                if (mode == AuthMode.ENFORCE) HttpStatusCode.Unauthorized else HttpStatusCode.NotFound,
                            )
                    }
                }
            }
            assertThat(host.seen).isEmpty()
            assertThat(touched()).isEmpty()
        }
    }

    @Test
    fun `a refusal is counted and audited once per request, where the schema is handed out`() {
        val commit = """{"appends":[{"namespace":"posthog","table":"events","files":[]}]}"""
        val capture = ListAppender<ILoggingEvent>().apply { start() }
        val audit = LoggerFactory.getLogger(Audit.LOGGER_NAME) as Logger
        audit.addAppender(capture)
        try {
            StubHost().use { host ->
                val registry = SimpleMeterRegistry().also { Metrics.bind(it) }

                fun outcome(name: String) =
                    registry.find("hoglake_auth_requests_total").tag("outcome", name).counters().sumOf { it.count() }
                client(host) {
                    // A tenant is refused in the handler: the plugin had counted its request as allowed.
                    get(schema, tenant())
                    assertThat(outcome("not_entitled") to outcome("ok")).isEqualTo(1.0 to 1.0)
                    // A scoped caller is refused in the namespace gate, which counts a request's one outcome.
                    get(schema, scoped(schemas = null))
                    assertThat(outcome("not_entitled") to outcome("ok")).isEqualTo(2.0 to 1.0)
                    // A body that names the schema is refused by the same check.
                    send(HttpMethod.Post, "/v1/catalogs/lake/commit", operator(), commit)
                    assertThat(outcome("not_entitled")).isEqualTo(3.0)
                    // The schema's reader adds no refusal, relayed or read-only: it was authorized, and
                    // each request has its one `ok`, from the gate on a path and from the body check.
                    get(schema, scoped())
                    send(HttpMethod.Post, "$schema/tables", scoped("""{"lake":["write"]}"""))
                    assertThat(outcome("not_entitled") to outcome("ok")).isEqualTo(3.0 to 4.0)
                    val readOnly =
                        send(HttpMethod.Post, "/v1/catalogs/lake/commit", scoped("""{"lake":["write"]}"""), commit)
                    assertThat(code(readOnly.bodyAsText())).isEqualTo("read_only_catalog")
                    assertThat(outcome("not_entitled") to outcome("ok")).isEqualTo(3.0 to 5.0)
                    // A token that carries the claim and does not name the catalog is refused for the
                    // catalog: the schema is never looked at, so nothing is counted against it.
                    val other = get(schema, scoped(cat = """{"pond":["read"]}"""))
                    assertThat(other.bodyAsText()).contains("catalog 'lake' not found")
                    assertThat(outcome("not_entitled")).isEqualTo(4.0)
                }
                val onSchema = registry.find("hoglake_mount_refusals_total").tag("reason", "not_found").counters()
                assertThat(onSchema.sumOf { it.count() }).isEqualTo(3.0)
                assertThat(host.seen.map { it.rawPath }).containsExactly(schema)
            }
        } finally {
            audit.detachAppender(capture)
        }
        val lines =
            capture.list.filter { it.formattedMessage.startsWith("auth_refused ") }
                .map { it.formattedMessage + " " + it.argumentArray.joinToString(" ") }
        assertThat(lines).hasSize(4).allSatisfy { assertThat(it).startsWith("auth_refused refused") }
        assertThat(lines.take(3)).allSatisfy { line ->
            assertThat(line).contains("catalog=lake", "namespace 'posthog' in catalog 'lake'", "at=remote_schema")
        }
        assertThat(lines.last()).contains("catalog 'lake' not found").doesNotContain("at=")
    }

    @Test
    fun `under off even a token that carries the claim is served nothing`() {
        StubHost().use { host ->
            client(host, AuthMode.OFF) {
                for (path in relayed) {
                    assertThat(get(path, scoped()).status).describedAs(path).isEqualTo(HttpStatusCode.NotFound)
                }
                // Nothing is listed either, and the local namespace of that name stays hidden.
                val names =
                    mapper.readTree(
                        get("/v1/catalogs/lake/namespaces", scoped()).bodyAsText(),
                    ).map { it["name"].asText() }
                assertThat(names).containsExactly("team_7", "other")
            }
            assertThat(host.seen).isEmpty()
            assertThat(touched()).isEmpty()
        }
    }

    @Test
    fun `a case variant of the name is neither relayed nor local, for every caller`() {
        StubHost().use { host ->
            client(host) {
                for (token in listOf(scoped(), scoped("""{"lake":["write"]}"""), tenant(), operator())) {
                    for (path in relayed) {
                        val variant = path.replace("posthog", "PostHog")
                        val response = get(variant, token)
                        assertThat(response.status).describedAs(variant).isEqualTo(HttpStatusCode.NotFound)
                        assertThat(response.bodyAsText()).contains("namespace 'PostHog' in catalog 'lake'")
                    }
                }
                // A name that only resembles it is an ordinary local namespace.
                assertThat(get("/v1/catalogs/lake/namespaces/posthog_raw", operator()).status)
                    .isEqualTo(HttpStatusCode.OK)
            }
            assertThat(host.seen).isEmpty()
            assertThat(touched()).isEmpty()
            assertThat(calls.map { it.first }).contains("getNamespace")
        }
    }

    @Test
    fun `the namespace of a request is the one in its path, whatever its query says`() {
        StubHost().use { host ->
            client(host) {
                // Route values come from the path only: a `namespace` query parameter names nothing.
                for (name in listOf("posthog", "PostHog")) {
                    for (token in listOf(operator(), tenant())) {
                        val read = get("/v1/catalogs/lake/namespaces/zzz?namespace=$name", token)
                        assertThat(read.status).isEqualTo(HttpStatusCode.OK)
                        assertThat(mapper.readTree(read.bodyAsText())["name"].asText()).isEqualTo("zzz")
                        get("/v1/catalogs/lake/namespaces/zzz/tables?namespace=$name", token)
                        get("/v1/catalogs/lake/namespaces/zzz/tables/events?namespace=$name", token)
                        get("/v1/catalogs/lake/namespaces/zzz/tables/events/scan?namespace=$name", token)
                        send(HttpMethod.Delete, "/v1/catalogs/lake/namespaces/zzz?namespace=$name&message=m", token)
                        send(HttpMethod.Post, "/v1/catalogs/lake/namespaces/zzz/tables?namespace=$name", token)
                    }
                }
                // The store was asked about `zzz`, and never about the remote schema's name.
                assertThat(calls.map { it.first }).contains("getNamespace", "listTables", "dropNamespace")
                assertThat(touched()).isEmpty()
                assertThat(host.seen).isEmpty()

                // The other way round: the schema in the path is relayed as the path says, and a
                // `catalog`, `namespace` or `table` in the query is not sent on.
                val relayedOnce =
                    get("$schema/tables/events?namespace=zzz&catalog=pond&table=other&snapshot=4", scoped())
                assertThat(relayedOnce.status).isEqualTo(HttpStatusCode.OK)
                assertThat(host.seen.single().rawPath).isEqualTo("$schema/tables/events")
                assertThat(host.seen.single().rawQuery).isEqualTo("snapshot=4")
            }
            assertThat(touched()).isEmpty()
        }
    }

    @Test
    fun `the listing hides a local namespace of the name and shows the schema only to a token that allows it`() {
        StubHost().use { host ->
            client(host) {
                suspend fun listing(token: String) =
                    mapper.readTree(get("/v1/catalogs/lake/namespaces", token).bodyAsText())

                val allowed = listing(scoped())
                assertThat(allowed.map { it["name"].asText() }).containsExactly("team_7", "posthog")
                // A local entry is what it was; the remote one is a name and the flag, with no invented id.
                assertThat(allowed[0].fieldNames().asSequence().toList())
                    .containsExactly("name", "namespace_id", "containment")
                assertThat(mapper.writeValueAsString(allowed[1])).isEqualTo("""{"name":"posthog","remote":true}""")

                for (token in listOf(scoped(schemas = null), scoped(schemas = """{"imports":["read"]}"""))) {
                    assertThat(listing(token).map { it["name"].asText() }).containsExactly("team_7")
                }
                // A tenant and an operator see every local namespace but that one, and no remote entry.
                for (token in listOf(tenant(), tenantSession(), operator())) {
                    val entries = listing(token)
                    assertThat(entries.map { it["name"].asText() }).containsExactly("team_7", "other")
                    assertThat(entries.none { it.has("remote") }).isTrue()
                }
            }
            // The listing asks no Host: its outage cannot fail it.
            assertThat(host.seen).isEmpty()
        }
    }

    @Test
    fun `every other route that names the schema refuses, and neither the host nor the store is asked`() {
        StubHost().use { host ->
            for (mode in listOf(AuthMode.WARN, AuthMode.ENFORCE)) {
                client(host, mode) {
                    val writer = scoped("""{"lake":["write"]}""")
                    for ((rule, path) in namespaceRoutes().filterNot { isRelayed(it.first) }) {
                        val what = "$mode ${rule.method.value} ${rule.pattern}"
                        // The schema's reader: read-only for a write, unavailable for a read it is not served.
                        val entitled = send(rule.method, path, writer)
                        val (status, code) =
                            when {
                                (rule.method to rule.pattern) !in AuthPolicy.SCOPED_ROUTES ->
                                    HttpStatusCode.Forbidden to "forbidden"
                                rule.requirement == Requirement.READ ->
                                    HttpStatusCode.NotImplemented to "mount_operation_unavailable"
                                else -> HttpStatusCode.Forbidden to "read_only_catalog"
                            }
                        assertThat(entitled.status).describedAs(what).isEqualTo(status)
                        assertThat(code(entitled.bodyAsText())).describedAs(what).isEqualTo(code)

                        // Everyone else: the namespace does not exist, whatever the route.
                        val asOperator = send(rule.method, path, operator())
                        assertThat(asOperator.status).describedAs(what).isEqualTo(HttpStatusCode.NotFound)
                        assertThat(asOperator.bodyAsText()).contains("namespace 'posthog' in catalog 'lake'")
                        val asTenant = send(rule.method, path, tenant())
                        if (rule.requirement == Requirement.OPERATOR && mode == AuthMode.ENFORCE) {
                            assertThat(asTenant.status).describedAs(what).isEqualTo(HttpStatusCode.Forbidden)
                        } else {
                            assertThat(asTenant.status).describedAs(what).isEqualTo(HttpStatusCode.NotFound)
                        }
                        val unclaimed = send(rule.method, path, scoped("""{"lake":["write"]}""", schemas = null))
                        assertThat(unclaimed.status).describedAs(what)
                            .isIn(HttpStatusCode.NotFound, HttpStatusCode.Forbidden)
                        if ((rule.method to rule.pattern) in AuthPolicy.SCOPED_ROUTES) {
                            assertThat(unclaimed.status).describedAs(what).isEqualTo(HttpStatusCode.NotFound)
                        }
                    }
                }
            }
            assertThat(namespaceRoutes().filterNot { isRelayed(it.first) }).hasSizeGreaterThan(15)
            assertThat(host.seen).isEmpty()
            assertThat(touched()).isEmpty()
        }
    }

    @Test
    fun `a read-only token's write is the policy's 403, before the schema is looked at`() {
        StubHost().use { host ->
            client(host) {
                val response = send(HttpMethod.Post, "$schema/tables", scoped())
                assertThat(response.status).isEqualTo(HttpStatusCode.Forbidden)
                assertThat(code(response.bodyAsText())).isEqualTo("forbidden")
            }
            assertThat(host.seen).isEmpty()
            assertThat(touched()).isEmpty()
        }
    }

    @Test
    fun `a commit that names the schema in its body is refused before the store commits`() {
        StubHost().use { host ->
            val commits =
                listOf(
                    "commit",
                    "commit/prepared",
                    "commit/deletes/prepared",
                    "commit/mutations/prepared",
                    "commit/transaction",
                    "commit/uploads",
                )
            val uuid = "00000000-0000-0000-0000-000000000001"
            val bodies =
                listOf("posthog", "PostHog").flatMap { name ->
                    listOf(
                        """{"appends":[{"namespace":"team_7","table":"t","files":[]},""" +
                            """{"namespace":"$name","table":"events","files":[]}],""" +
                            """"idempotency_key":"$uuid","read_snapshot":1}""",
                        """{"deletes":[{"namespace":"$name","table":"events","files":[],""" +
                            """"expected_table_uuid":"$uuid"}],""" +
                            """"idempotency_key":"$uuid","read_snapshot":1}""",
                    )
                }
            client(host) {
                for (route in commits) {
                    val path = "/v1/catalogs/lake/$route"
                    for (body in bodies) {
                        val variant = "PostHog" in body
                        val entitled = send(HttpMethod.Post, path, scoped("""{"lake":["write"]}"""), body)
                        if (variant) {
                            assertThat(entitled.status).describedAs(path).isEqualTo(HttpStatusCode.NotFound)
                        } else {
                            assertThat(entitled.status).describedAs(path).isEqualTo(HttpStatusCode.Forbidden)
                            assertThat(code(entitled.bodyAsText())).describedAs(path).isEqualTo("read_only_catalog")
                        }
                        for (token in listOf(tenant(), operator(), scoped("""{"lake":["write"]}""", schemas = null))) {
                            val response = send(HttpMethod.Post, path, token, body)
                            assertThat(response.status).describedAs(path).isEqualTo(HttpStatusCode.NotFound)
                            assertThat(response.bodyAsText()).contains("in catalog 'lake'")
                        }
                    }
                }
            }
            assertThat(host.seen).isEmpty()
            assertThat(touched()).isEmpty()
        }
    }

    @Test
    fun `a namespace parameter that names the schema is refused, and the catalog-wide report leaves it out`() {
        StubHost().use { host ->
            client(host) {
                for (token in listOf(tenant(), operator())) {
                    for (name in listOf("posthog", "PostHog")) {
                        val stats = get("/v1/catalogs/lake/stats/partitions?namespace=$name", token)
                        assertThat(stats.status).describedAs(name).isEqualTo(HttpStatusCode.NotFound)
                        val rehydrate =
                            send(
                                HttpMethod.Post,
                                "/v1/catalogs/lake/maintenance/rehydrate?namespace=$name&table=events",
                                token,
                            )
                        assertThat(rehydrate.status).describedAs(name).isEqualTo(HttpStatusCode.NotFound)
                    }
                    val report = mapper.readTree(get("/v1/catalogs/lake/stats/partitions", token).bodyAsText())
                    assertThat(report["partitions"].map { it["namespace"].asText() }).containsExactly("team_7")
                    assertThat(get("/v1/catalogs/lake/stats/partitions?namespace=team_7", token).status)
                        .isEqualTo(HttpStatusCode.OK)
                    assertThat(send(HttpMethod.Post, "/v1/catalogs/lake/maintenance/rehydrate", token).status)
                        .isEqualTo(HttpStatusCode.OK)
                }
            }
            assertThat(host.seen).isEmpty()
        }
    }

    @Test
    fun `a consumer offset on a table of the hidden namespace is not listed, read or committed`() {
        val offsets = "/v1/catalogs/lake/consumers/c1/offsets"
        val commit = """{"snapshot_id":1}"""

        fun onHidden() = calls.filter { (name, args) -> name.endsWith("Offset") && hiddenTable in args }
        StubHost().use { host ->
            client(host) {
                for (token in listOf(operator(), tenant())) {
                    // `c2` holds offsets only on a table of `PostHog`, so it is not listed at all.
                    val consumers = mapper.readTree(get("/v1/catalogs/lake/consumers", token).bodyAsText())["consumers"]
                    assertThat(consumers.map { it["consumer_id"].asText() }).containsExactly("c1")
                    assertThat(consumers[0]["offsets"].map { it["namespace"].asText() }).containsExactly("team_7")
                    val listed = mapper.readTree(get(offsets, token).bodyAsText())
                    assertThat(listed.map { it["table_uuid"].asText() }).containsExactly(ownTable.toString())

                    // Answered as for a table UUID the catalog does not have, in the stores' words.
                    val read = get("$offsets/$hiddenTable", token)
                    assertThat(read.status).isEqualTo(HttpStatusCode.NotFound)
                    assertThat(read.bodyAsText())
                        .contains("no offset for consumer 'c1' on table $hiddenTable in catalog 'lake'")
                    val committed = send(HttpMethod.Put, "$offsets/$hiddenTable", token, commit)
                    assertThat(committed.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
                    assertThat(committed.bodyAsText()).contains("unknown table_uuid $hiddenTable in catalog 'lake'")
                    assertThat(read.bodyAsText() + committed.bodyAsText()).doesNotContain("posthog")
                    assertThat(get("$offsets/$ownTable", token).status).isEqualTo(HttpStatusCode.OK)
                    assertThat(
                        send(HttpMethod.Put, "$offsets/$ownTable", token, commit).status,
                    ).isEqualTo(HttpStatusCode.OK)
                }
            }
            assertThat(onHidden()).isEmpty()
            assertThat(calls.count { it.first == "commitOffset" }).isEqualTo(2)
            assertThat(host.seen).isEmpty()

            // With no remote schema configured every row is served, and no table list is read for it.
            calls.clear()
            client(host, matches = emptyList()) {
                val consumers =
                    mapper.readTree(
                        get("/v1/catalogs/lake/consumers", operator()).bodyAsText(),
                    )["consumers"]
                assertThat(consumers.map { it["consumer_id"].asText() }).containsExactly("c1", "c2")
                assertThat(mapper.readTree(get(offsets, operator()).bodyAsText())).hasSize(2)
                assertThat(get("$offsets/$hiddenTable", operator()).status).isEqualTo(HttpStatusCode.OK)
                assertThat(send(HttpMethod.Put, "$offsets/$hiddenTable", operator(), commit).status)
                    .isEqualTo(HttpStatusCode.OK)
            }
            assertThat(onHidden()).hasSize(2)
            assertThat(calls.map { it.first }).doesNotContain("listTables", "listNamespaces")
        }
    }

    @Test
    fun `two remote schemas are told apart in the listing, the relay and the refusals`() {
        StubHost().use { posthogHost ->
            StubHost().use { importsHost ->
                val both = listOf(MountMatch.Namespace("posthog"), MountMatch.Namespace("imports"))
                val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
                client(posthogHost, matches = both, origins = mapOf("imports" to importsHost.origin)) {
                    suspend fun listed(token: String) =
                        mapper.readTree(
                            get("/v1/catalogs/lake/namespaces", token).bodyAsText(),
                        ).map { it["name"].asText() }

                    val all = """{"posthog":["read"],"imports":["read"]}"""
                    val onlyImports = """{"imports":["read"]}"""
                    // Local namespaces first, then the allowed schemas by name, whatever the file's order.
                    assertThat(listed(scoped(schemas = all))).containsExactly("team_7", "imports", "posthog")
                    assertThat(listed(scoped(schemas = onlyImports))).containsExactly("team_7", "imports")
                    assertThat(listed(scoped())).containsExactly("team_7", "posthog")
                    assertThat(listed(scoped(schemas = null))).containsExactly("team_7")

                    // Each schema goes to its own host, and a token allowed one of the two reads only that one.
                    val imports = "/v1/catalogs/lake/namespaces/imports"
                    assertThat(
                        get("$imports/tables", scoped(schemas = onlyImports)).status,
                    ).isEqualTo(HttpStatusCode.OK)
                    assertThat(get("$schema/tables", scoped(schemas = onlyImports)).status)
                        .isEqualTo(HttpStatusCode.NotFound)
                    assertThat(get("$schema/tables", scoped()).status).isEqualTo(HttpStatusCode.OK)
                    assertThat(get("$imports/tables", scoped()).status).isEqualTo(HttpStatusCode.NotFound)
                    assertThat(importsHost.seen.map { it.rawPath }).containsExactly("$imports/tables")
                    assertThat(posthogHost.seen.map { it.rawPath }).containsExactly("$schema/tables")

                    // A body is refused for the schema it names, by that schema's own rule.
                    val body =
                        """{"appends":[{"namespace":"team_7","table":"t","files":[]},""" +
                            """{"namespace":"imports","table":"x","files":[]}]}"""
                    val writer = """{"lake":["write"]}"""
                    val reader = send(HttpMethod.Post, "/v1/catalogs/lake/commit", scoped(writer, onlyImports), body)
                    assertThat(reader.status).isEqualTo(HttpStatusCode.Forbidden)
                    assertThat(code(reader.bodyAsText())).isEqualTo("read_only_catalog")
                    val other = send(HttpMethod.Post, "/v1/catalogs/lake/commit", scoped(writer), body)
                    assertThat(other.status).isEqualTo(HttpStatusCode.NotFound)
                    assertThat(other.bodyAsText()).contains("namespace 'imports' in catalog 'lake'")
                }
                assertThat(touched()).isEmpty()
                val refusals = registry.find("hoglake_mount_refusals_total").counters()
                assertThat(refusals.associate { (it.id.getTag("catalog") to it.id.getTag("reason")) to it.count() })
                    .isEqualTo(
                        mapOf(
                            ("posthog" to "not_found") to 1.0,
                            ("imports" to "not_found") to 2.0,
                            ("imports" to "read_only_catalog") to 1.0,
                        ),
                    )
            }
        }
    }

    @Test
    fun `a token routed to hoglite is refused the same way, and the engine is never asked`() {
        // The catalog's own backend is hoglite here: a stub engine that counts every request it gets.
        val engine = StubEngine()
        try {
            StubHost().use { host ->
                client(host, engine = engine.url) {
                    val rtg = """"rtg":{"b":"hoglite","be":"$hogliteId","t":"t_1"}"""
                    val routed =
                        TestTokens.mint(
                            pair,
                            TestTokens.tenantClaims(catalogs = mapOf("lake" to listOf("admin"))).dropLast(1) + ",$rtg}",
                        )
                    val commit = """{"appends":[{"namespace":"posthog","table":"events","files":[]}]}"""
                    val refused =
                        listOf(
                            get("$schema/tables", routed),
                            send(HttpMethod.Post, "$schema/tables", routed),
                            send(HttpMethod.Delete, "$schema?message=m", routed),
                            send(HttpMethod.Post, "/v1/catalogs/lake/commit", routed, commit),
                            get("/v1/catalogs/lake/stats/partitions?namespace=posthog", routed),
                            send(
                                HttpMethod.Post,
                                "/v1/catalogs/lake/maintenance/rehydrate?namespace=posthog&table=x",
                                routed,
                            ),
                        )
                    for (response in refused) {
                        assertThat(response.status).isEqualTo(HttpStatusCode.NotFound)
                        assertThat(response.bodyAsText()).contains("namespace 'posthog' in catalog 'lake'")
                    }
                    assertThat(engine.requests.get()).isZero()
                    // The schema's reader, routed the same way, is relayed and refused as on PostgreSQL.
                    val reader =
                        scoped("""{"lake":["write"]}""", rtg = """{"b":"hoglite","be":"$hogliteId","t":"t_1"}""")
                    assertThat(get("$schema/tables", reader).status).isEqualTo(HttpStatusCode.OK)
                    assertThat(code(send(HttpMethod.Post, "$schema/tables", reader).bodyAsText()))
                        .isEqualTo("read_only_catalog")
                    assertThat(code(send(HttpMethod.Post, "/v1/catalogs/lake/commit", reader, commit).bodyAsText()))
                        .isEqualTo("read_only_catalog")
                }
                assertThat(host.seen.map { it.rawPath }).containsExactly("$schema/tables")
            }
            // The engine was asked once for each of the reader's two requests whose path names the
            // schema, whether the catalog exists, and nothing else: no namespace, table or commit.
            assertThat(engine.requests.get()).isEqualTo(2)
            assertThat(calls).isEmpty()
        } finally {
            engine.close()
        }
    }

    @Test
    fun `a host outage is an error on the schema, never the local namespace`() {
        StubHost().use { host ->
            RemoteSchemaFixtures.DeadHost().use { dead ->
                client(host, origin = dead.origin) {
                    for (path in relayed) {
                        val response = get(path, scoped())
                        assertThat(response.status).describedAs(path).isEqualTo(HttpStatusCode.ServiceUnavailable)
                        assertThat(code(response.bodyAsText())).describedAs(path).isEqualTo("mount_host_unavailable")
                        assertThat(response.bodyAsText()).doesNotContain("127.0.0.1")
                    }
                    // The listing still answers, and still from configuration.
                    val names =
                        mapper.readTree(
                            get("/v1/catalogs/lake/namespaces", scoped()).bodyAsText(),
                        ).map { it["name"].asText() }
                    assertThat(names).containsExactly("team_7", "posthog")
                }
            }
            assertThat(touched()).isEmpty()
        }
    }

    @Test
    fun `the host's own refusal and an answer outside its contract come back as a mount host's do`() {
        StubHost().use { host ->
            client(host) {
                host.answer = { it.json(404, """{"error":"not_found","detail":"table 'posthog.nope'"}""") }
                val missing = get("$schema/tables/nope", scoped())
                assertThat(missing.status).isEqualTo(HttpStatusCode.NotFound)
                assertThat(missing.bodyAsText()).contains("table 'posthog.nope'")
                host.answer = { exchange ->
                    exchange.responseHeaders.add("Content-Type", "text/html")
                    exchange.sendResponseHeaders(200, 6)
                    exchange.responseBody.use { it.write("<html>".toByteArray()) }
                }
                val invalid = get(schema, scoped())
                assertThat(invalid.status).isEqualTo(HttpStatusCode.BadGateway)
                assertThat(code(invalid.bodyAsText())).isEqualTo("mount_host_invalid_response")
            }
            assertThat(touched()).isEmpty()
        }
    }

    @Test
    fun `a table name no catalog could hold is a local 404 and is never sent`() {
        StubHost().use { host ->
            client(host) {
                for (name in listOf("..", "a%2Fb", "a%20b", "9lives")) {
                    for (path in listOf("$schema/tables/$name", "$schema/tables/$name/scan")) {
                        assertThat(get(path, scoped()).status).describedAs(path).isEqualTo(HttpStatusCode.NotFound)
                    }
                }
            }
            assertThat(host.seen).isEmpty()
        }
    }

    @Test
    fun `a catalog name no catalog could have is a local 404 and is never sent`() {
        StubHost().use { host ->
            client(host) {
                // The token names it, so the caller rule passes; the path the Host would get is refused here.
                val response = get("/v1/catalogs/La%20ke/namespaces/posthog", scoped(cat = """{"La ke":["read"]}"""))
                assertThat(response.status).isEqualTo(HttpStatusCode.NotFound)
                assertThat(response.bodyAsText()).contains("catalog 'La ke'")
            }
            assertThat(host.seen).isEmpty()
        }
    }

    @Test
    fun `proxied requests and refusals are labelled by the schema name, never by the catalog or the org`() {
        StubHost().use { host ->
            val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
            client(host) {
                get(schema, scoped())
                get(schema, tenant())
                send(HttpMethod.Post, "$schema/tables", scoped("""{"lake":["write"]}"""))
                get("$schema/tables/events/files", scoped())
                get(schema.replace("posthog", "PostHog"), scoped())
            }
            // The one label value on every mount series is the configured name: not `lake`, not `PostHog`.
            val mountMeters = registry.meters.filter { it.id.name.startsWith("hoglake_mount_") }
            assertThat(mountMeters.map { it.id.getTag("catalog") }.toSet()).containsExactly("posthog")
            val requests = registry.find("hoglake_mount_proxy_requests_total").counters()
            assertThat(requests.single().count()).isEqualTo(1.0)
            assertThat(registry.find("hoglake_mount_proxy_requests").timers().single().count()).isEqualTo(1)
            val refusals = registry.find("hoglake_mount_refusals_total").counters()
            assertThat(refusals.associate { it.id.getTag("reason") to it.count() }).isEqualTo(
                mapOf("not_found" to 2.0, "read_only_catalog" to 1.0, "mount_operation_unavailable" to 1.0),
            )
        }
    }

    @Test
    fun `with no namespace host the name is an ordinary local namespace`() {
        StubHost().use { host ->
            // No hosts at all, and a hosts file with only the old match types.
            for (matches in listOf<List<MountMatch>>(emptyList(), listOf(MountMatch.Events))) {
                calls.clear()
                client(host, matches = matches) {
                    assertThat(get(schema, operator()).status).isEqualTo(HttpStatusCode.OK)
                    assertThat(get(schema, scoped()).status).isEqualTo(HttpStatusCode.OK)
                    val names =
                        mapper.readTree(
                            get("/v1/catalogs/lake/namespaces", operator()).bodyAsText(),
                        ).map { it["name"].asText() }
                    assertThat(names).containsExactly("team_7", "posthog", "PostHog", "other")
                    val report = mapper.readTree(get("/v1/catalogs/lake/stats/partitions", operator()).bodyAsText())
                    assertThat(report["partitions"]).hasSize(3)
                }
                assertThat(calls.filter { it.first == "getNamespace" }).hasSize(2)
            }
            assertThat(host.seen).isEmpty()
        }
    }

    @Test
    fun `a mount keeps its own namespaces, whatever they are called`() {
        StubHost().use { host ->
            client(host, matches = listOf(MountMatch.Events, MountMatch.Namespace("posthog"))) {
                val reader = TestTokens.mint(pair, TestTokens.scopedClaims(catalogs = """{"$mount":["read"]}"""))
                val path = "/v1/catalogs/$mount/namespaces/posthog/tables"
                // A mount reader needs no remote_schemas claim: the mount's host serves the mount's namespaces.
                assertThat(get(path, reader).status).isEqualTo(HttpStatusCode.OK)
                assertThat(host.seen.single().rawPath).isEqualTo(path)
            }
            assertThat(calls).isEmpty()
        }
    }
}
