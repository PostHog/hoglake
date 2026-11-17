package com.posthog.hoglake.store

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.AuthPolicy
import com.posthog.hoglake.auth.Requirement
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.configureApiListener
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.wireObjectMapper
import com.sun.net.httpserver.HttpServer
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList

/**
 * A remote schema end to end on its Client (docs/auth-tenancy.md §6.5c): the
 * real application over PostgreSQL, with a hosts file naming the remote
 * schema `posthog`, and a stub Host that records every request it receives.
 *
 * The catalog `lake` already holds a namespace `posthog` with a table, made
 * before the name was configured and contained for a team the reader holds,
 * so without the remote schema every caller here could read or write it. Each
 * test checks what the routes answer, that the Host saw only what was
 * relayed, and that the catalog's head snapshot did not move.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RemoteSchemaClientIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val pair = TestTokens.keyPair()
    private val keyFile =
        Files.createTempFile("hoglake-remote-schema-", ".json").also { Files.writeString(it, TestTokens.jwks(pair)) }
    private val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
    private val mapper = wireObjectMapper()
    private val cat = "lake"
    private val dataPath = "s3://bucket/$cat"
    private val schema = "/v1/catalogs/$cat/namespaces/posthog"
    private val relayed = listOf(schema, "$schema/tables", "$schema/tables/events", "$schema/tables/events/scan")
    private val hostAnswer = """{ "served_by":"host",  "remote":true }"""

    init {
        catalogs.createCatalog(cat, dataPath)
        // One namespace per contained value: the reader holds both 7 and 8.
        for ((ns, team) in listOf("team_7" to 7L, "posthog" to 8L)) {
            catalogs.createNamespace(cat, ns)
            catalogs.setNamespaceContainment(cat, ns, NamespaceContainment("team_id", team))
        }
        catalogs.createNamespace(cat, "PostHog")
        catalogs.createTable(cat, "team_7", "t", listOf(ColumnDef("v", ColType.LONG)))
        catalogs.createTable(cat, "posthog", "events", listOf(ColumnDef("v", ColType.LONG)))
        catalogs.createTable(cat, "PostHog", "events", listOf(ColumnDef("v", ColType.LONG)))
    }

    /** A Host that records each request's path and answers every one with [hostAnswer]. */
    private inner class StubHost : AutoCloseable {
        val seen = CopyOnWriteArrayList<String>()
        private val server =
            HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).also { server ->
                server.createContext("/") { exchange ->
                    seen += exchange.requestURI.rawPath
                    val bytes = hostAnswer.toByteArray()
                    exchange.responseHeaders.add("Content-Type", "application/json")
                    exchange.sendResponseHeaders(200, bytes.size.toLong())
                    exchange.responseBody.use { it.write(bytes) }
                    exchange.close()
                }
                server.start()
            }
        val port: Int get() = server.address.port

        override fun close() = server.stop(0)
    }

    @AfterAll
    fun close() {
        http.close()
        db.close()
        Files.deleteIfExists(keyFile)
    }

    private fun hostsFile(hostPort: Int): String =
        Files.createTempFile("hoglake-remote-schema-hosts-", ".json").also {
            it.toFile().deleteOnExit()
            Files.writeString(
                it,
                """{"hosts":[{"id":"3c0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e","match":{"type":"namespace",""" +
                    """"name":"posthog"},"kind":"hoglake","url":"http://127.0.0.1:$hostPort","auth":"forward",""" +
                    // The stub Host speaks http, which a remote schema's entry must opt in to.
                    """"allow_insecure_http":true}]}""",
            )
        }.toString()

    /** The Client with the Host at [hostPort]; null is the same server with no hosts file. */
    private fun <T> withClient(
        hostPort: Int?,
        block: (port: Int) -> T,
    ): T {
        val cfg =
            Config(port = 0, hydratorIntervalMs = 0).copy(
                authMode = "enforce",
                authIssuer = TestTokens.ISSUER,
                authJwksFile = keyFile.toString(),
                authPredicateFields = "team_id",
                authSessionProfile = "on",
                mountHostsFile = hostPort?.let(::hostsFile) ?: "",
            )
        val app = App.build(cfg, db.jdbi)
        val server = embeddedServer(Netty, configure = { configureApiListener(cfg) }) { app.module(this) }
        try {
            server.start(wait = false)
            return block(runBlocking { server.engine.resolvedConnectors().first().port })
        } finally {
            server.stop(0, 1000)
            app.close()
        }
    }

    /**
     * A scoped token for teams 7 and 8 with `write` on the catalog. [schemas] is its `remote_schemas`
     * claim, carried in the session profile; null is a plain scoped token.
     */
    private fun scoped(
        schemas: String? = """{"posthog":["read"]}""",
        catalog: String = cat,
    ): String {
        val iat = System.currentTimeMillis() / 1000 - 5
        return TestTokens.mint(
            pair,
            TestTokens.scopedClaims(
                prd = """{"team_id":{"7":"write","8":"write"}}""",
                catalogs = """{"$catalog":["write"]}""",
                iat = iat,
                extra = schemas?.let { TestTokens.sessionProfile(catalog, it) } ?: "",
            ),
        )
    }

    private fun tenant() = TestTokens.mint(pair, TestTokens.tenantClaims(catalogs = mapOf(cat to listOf("admin"))))

    private fun operator() = TestTokens.mint(pair, TestTokens.operatorClaims())

    private fun send(
        port: Int,
        method: String,
        path: String,
        token: String,
        body: String? = null,
    ): HttpResponse<String> {
        val payload = body ?: if (method == "GET" || method == "DELETE") null else "{}"
        return http.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port$path"))
                .header("Authorization", "Bearer $token")
                .header("Content-Type", "application/json")
                .method(
                    method,
                    payload?.let { HttpRequest.BodyPublishers.ofString(it) } ?: HttpRequest.BodyPublishers.noBody(),
                )
                .build(),
            HttpResponse.BodyHandlers.ofString(),
        )
    }

    private fun code(response: HttpResponse<String>): String =
        response.body().substringAfter("\"error\":\"", "").substringBefore('"')

    private fun head(): Long = catalogs.getCatalog(cat).headSnapshotId

    private fun names(response: HttpResponse<String>) = mapper.readTree(response.body()).map { it["name"].asText() }

    private fun assertMissing(
        response: HttpResponse<String>,
        what: String,
    ) {
        assertThat(response.statusCode()).describedAs("%s: %s", what, response.body()).isEqualTo(404)
        assertThat(code(response)).describedAs(what).isEqualTo("not_found")
    }

    private fun assertReadOnly(
        response: HttpResponse<String>,
        what: String,
    ) {
        assertThat(response.statusCode()).describedAs("%s: %s", what, response.body()).isEqualTo(403)
        assertThat(code(response)).describedAs(what).isEqualTo("read_only_catalog")
    }

    @Test
    fun `without the hosts file the namespace is the local one it was`() {
        withClient(null) { port ->
            assertThat(names(send(port, "GET", "/v1/catalogs/$cat/namespaces", operator())))
                .containsExactlyInAnyOrder("team_7", "posthog", "PostHog")
            for (token in listOf(operator(), tenant(), scoped(), scoped(schemas = null))) {
                val tables = send(port, "GET", "$schema/tables", token)
                assertThat(tables.statusCode()).describedAs(tables.body()).isEqualTo(200)
                assertThat(names(tables)).containsExactly("events")
            }
        }
    }

    @Test
    fun `the four routes are relayed for the schema's reader and are a missing namespace for everyone else`() {
        StubHost().use { host ->
            val before = head()
            withClient(host.port) { port ->
                for (path in relayed) {
                    val response = send(port, "GET", path, scoped())
                    assertThat(response.statusCode()).describedAs(path).isEqualTo(200)
                    // The Host's bytes, not the local namespace's and not a re-serialisation.
                    assertThat(response.body()).describedAs(path).isEqualTo(hostAnswer)
                }
                assertThat(host.seen).isEqualTo(relayed)
                host.seen.clear()
                for (path in relayed) {
                    for (token in listOf(
                        tenant(),
                        operator(),
                        scoped(schemas = null),
                        scoped("""{"imports":["read"]}"""),
                    )) {
                        assertMissing(send(port, "GET", path, token), path)
                    }
                    assertMissing(send(port, "GET", path.replace("posthog", "PostHog"), scoped()), "variant of $path")
                    assertMissing(send(port, "GET", path.replace("posthog", "PostHog"), operator()), "variant of $path")
                }
                assertThat(host.seen).isEmpty()
                // The rest of the catalog is served as before.
                assertThat(send(port, "GET", "/v1/catalogs/$cat", tenant()).statusCode()).isEqualTo(200)
                assertThat(send(port, "GET", "/v1/catalogs/$cat/namespaces/team_7/tables", scoped()).statusCode())
                    .isEqualTo(200)
                assertThat(send(port, "GET", "/v1/catalogs/$cat/storage-usage", tenant()).statusCode()).isEqualTo(200)
            }
            assertThat(head()).isEqualTo(before)
        }
    }

    @Test
    fun `every route that names the schema in its path leaves the local namespace unread and unwritten`() {
        StubHost().use { host ->
            val before = head()
            val routes =
                RemoteSchemaFixtures.namespaceRoutes(
                    cat,
                ).filterNot { RemoteSchemaFixtures.isRelayed(it.first) }
            assertThat(routes).hasSizeGreaterThan(15)
            withClient(host.port) { port ->
                for ((rule, path) in routes) {
                    val method = rule.method.value
                    val what = "$method ${rule.pattern}"
                    val entitled = send(port, method, path, scoped())
                    when {
                        (rule.method to rule.pattern) !in AuthPolicy.SCOPED_ROUTES -> {
                            assertThat(entitled.statusCode()).describedAs(what).isEqualTo(403)
                            assertThat(code(entitled)).describedAs(what).isEqualTo("forbidden")
                        }
                        rule.requirement == Requirement.READ -> {
                            assertThat(
                                entitled.statusCode(),
                            ).describedAs("%s: %s", what, entitled.body()).isEqualTo(501)
                            assertThat(code(entitled)).describedAs(what).isEqualTo("mount_operation_unavailable")
                        }
                        else -> assertReadOnly(entitled, what)
                    }
                    assertMissing(send(port, method, path, operator()), "operator $what")
                    val asTenant = send(port, method, path, tenant())
                    if (rule.requirement == Requirement.OPERATOR) {
                        assertThat(asTenant.statusCode()).describedAs(what).isEqualTo(403)
                    } else {
                        assertMissing(asTenant, "tenant $what")
                    }
                    if ((rule.method to rule.pattern) in AuthPolicy.SCOPED_ROUTES) {
                        assertMissing(send(port, method, path, scoped(schemas = null)), "unclaimed $what")
                    }
                }
            }
            assertThat(host.seen).isEmpty()
            // Nothing was dropped, truncated, created or re-contained.
            assertThat(head()).isEqualTo(before)
            assertThat(catalogs.getTable(cat, "posthog", "events").name).isEqualTo("events")
            assertThat(catalogs.getNamespace(cat, "posthog").containment).isEqualTo(NamespaceContainment("team_id", 8))
        }
    }

    @Test
    fun `the namespace of a request is the one in its path, whatever its query says`() {
        StubHost().use { host ->
            val before = head()
            withClient(host.port) { port ->
                val own = "/v1/catalogs/$cat/namespaces/team_7"
                // Route values come from the path only: a `namespace` query parameter names nothing.
                for (token in listOf(operator(), tenant(), scoped(), scoped(schemas = null))) {
                    for (name in listOf("posthog", "PostHog")) {
                        val tables = send(port, "GET", "$own/tables?namespace=$name", token)
                        assertThat(tables.statusCode()).describedAs(tables.body()).isEqualTo(200)
                        assertThat(names(tables)).doesNotContain("events").contains("t")
                        val namespace = send(port, "GET", "$own?namespace=$name", token)
                        assertThat(mapper.readTree(namespace.body())["name"].asText()).isEqualTo("team_7")
                        assertMissing(send(port, "GET", "$own/tables/events?namespace=$name", token), "table $name")
                        assertMissing(send(port, "GET", "$own/tables/events/scan?namespace=$name", token), "scan $name")
                        val drop = send(port, "DELETE", "$own/tables/events?namespace=$name&message=drop", token)
                        assertMissing(drop, "drop $name")
                    }
                }
                val create = """{"name":"made_by_path","columns":[{"name":"v","type":"long"}]}"""
                val made = send(port, "POST", "$own/tables?namespace=posthog", operator(), create)
                assertThat(made.statusCode()).describedAs(made.body()).isEqualTo(201)
            }
            assertThat(host.seen).isEmpty()
            // One table was created, in the namespace of the path; the hidden namespace is as it was.
            assertThat(head()).isEqualTo(before + 1)
            assertThat(catalogs.listTables(cat, "team_7").map { it.name }).contains("made_by_path")
            assertThat(catalogs.listTables(cat, "posthog").map { it.name }).containsExactly("events")
            assertThat(catalogs.listTables(cat, "PostHog").map { it.name }).containsExactly("events")
        }
    }

    private fun file(ns: String) =
        """{"path":"$dataPath/data/$ns/${UUID.randomUUID()}.parquet","record_count":1,"file_size_bytes":10}"""

    @Test
    fun `a commit or a table creation that names the schema in its body writes nothing`() {
        StubHost().use { host ->
            val before = head()
            val uuid = catalogs.getTable(cat, "posthog", "events").tableUuid
            withClient(host.port) { port ->
                val commits =
                    listOf("commit", "commit/prepared", "commit/deletes/prepared", "commit/mutations/prepared")
                        .plus(listOf("commit/transaction", "commit/uploads"))
                for (route in commits) {
                    val path = "/v1/catalogs/$cat/$route"
                    val appends =
                        """{"appends":[{"namespace":"posthog","table":"events","files":[${file("posthog")}],""" +
                            """"expected_table_uuid":"$uuid"}],"read_snapshot":$before,""" +
                            """"idempotency_key":"${UUID.randomUUID()}"}"""
                    val deletes =
                        """{"deletes":[{"namespace":"posthog","table":"events","files":[],""" +
                            """"expected_table_uuid":"$uuid"}],"read_snapshot":$before,""" +
                            """"idempotency_key":"${UUID.randomUUID()}"}"""
                    for (body in listOf(appends, deletes)) {
                        assertReadOnly(send(port, "POST", path, scoped(), body), "scoped $route")
                        for (token in listOf(tenant(), operator(), scoped(schemas = null))) {
                            assertMissing(send(port, "POST", path, token, body), route)
                        }
                    }
                }
                for (suffix in listOf("", "/partitioned", "/sorted", "/metadata")) {
                    val path = "/v1/catalogs/$cat/table-creations/${UUID.randomUUID()}$suffix"
                    val body = """{"namespace":"posthog","name":"made","columns":[{"name":"v","type":"long"}]}"""
                    assertReadOnly(send(port, "PUT", path, scoped(), body), "scoped prepare$suffix")
                    for (token in listOf(tenant(), operator(), scoped(schemas = null))) {
                        assertMissing(send(port, "PUT", path, token, body), "prepare$suffix")
                    }
                }
                // A scoped upload claim spells its namespace in the prefix.
                val claim =
                    """{"owner":"${UUID.randomUUID()}","prefix":"$dataPath/data/posthog/events","file_kind":"data"}"""
                assertReadOnly(
                    send(port, "PUT", "/v1/catalogs/$cat/uploads/${UUID.randomUUID()}", scoped(), claim),
                    "scoped claim",
                )
                // The same writes to a namespace that is not a remote schema still land.
                val own =
                    """{"appends":[{"namespace":"team_7","table":"t","files":[${file("team_7")}]}]}"""
                val landed = send(port, "POST", "/v1/catalogs/$cat/commit", scoped(), own)
                assertThat(landed.statusCode()).describedAs(landed.body()).isEqualTo(200)
            }
            assertThat(host.seen).isEmpty()
            assertThat(head()).isEqualTo(before + 1)
            assertThat(catalogs.listTables(cat, "posthog").map { it.name }).containsExactly("events")
        }
    }

    @Test
    fun `a creation prepared for the namespace before it was a remote schema cannot be read or finished after`() {
        StubHost().use { host ->
            val operations = List(4) { UUID.randomUUID() }
            withClient(null) { port ->
                for (operation in operations) {
                    val body =
                        """{"namespace":"posthog","name":"late_$operation","columns":[{"name":"v","type":"long"}]}"""
                            .replace("-", "_")
                    val prepared = send(port, "PUT", "/v1/catalogs/$cat/table-creations/$operation", tenant(), body)
                    assertThat(prepared.statusCode()).describedAs(prepared.body()).isEqualTo(200)
                }
            }
            val before = head()
            withClient(host.port) { port ->
                val steps = listOf("GET" to "", "POST" to "/commit", "POST" to "/commit/uploads", "POST" to "/abort")
                for ((operation, step) in operations.zip(steps)) {
                    val (method, suffix) = step
                    val path = "/v1/catalogs/$cat/table-creations/$operation$suffix"
                    val body = if (method == "GET") null else """{"files":[]}"""
                    // The request named an operation and no namespace: the answer is the one an
                    // operation that does not exist gets, word for word.
                    val unknown = UUID.randomUUID()
                    for (token in listOf(tenant(), operator(), scoped(schemas = null))) {
                        val refused = send(port, method, path, token, body)
                        assertMissing(refused, "$method creation$suffix")
                        val missing = send(port, method, path.replace("$operation", "$unknown"), token, body)
                        assertThat(refused.body().replace("$operation", "$unknown")).isEqualTo(missing.body())
                        assertThat(refused.body()).doesNotContain("posthog")
                    }
                    val entitled = send(port, method, path, scoped(), body)
                    if (method == "GET") {
                        assertThat(entitled.statusCode()).describedAs(entitled.body()).isEqualTo(501)
                    } else {
                        assertReadOnly(entitled, "scoped creation$suffix")
                    }
                }
                // A creation for another namespace is still the tenant's to prepare, read and abort.
                val mine = "/v1/catalogs/$cat/table-creations/${UUID.randomUUID()}"
                val body = """{"namespace":"team_7","name":"own_made","columns":[{"name":"v","type":"long"}]}"""
                assertThat(send(port, "PUT", mine, tenant(), body).statusCode()).isEqualTo(200)
                assertThat(send(port, "GET", mine, tenant()).statusCode()).isEqualTo(200)
                assertThat(send(port, "POST", "$mine/abort", tenant()).statusCode()).isEqualTo(200)
            }
            assertThat(head()).isEqualTo(before)
            assertThat(host.seen).isEmpty()
            // Each is still prepared: nothing published or aborted it.
            withClient(null) { port ->
                for (operation in operations) {
                    val status = send(port, "GET", "/v1/catalogs/$cat/table-creations/$operation", tenant())
                    assertThat(mapper.readTree(status.body()).path("state").asText()).isEqualTo("prepared")
                }
            }
        }
    }

    @Test
    fun `a consumer offset on a table of the hidden namespace is not listed, read or committed`() {
        val hidden = catalogs.getTable(cat, "posthog", "events").tableUuid
        val own = catalogs.getTable(cat, "team_7", "t").tableUuid
        val at = head()
        // Committed before the name was configured.
        catalogs.commitOffset(cat, "reader", hidden, at)
        catalogs.commitOffset(cat, "reader", own, at)
        val offsets = "/v1/catalogs/$cat/consumers/reader/offsets"
        val commit = """{"snapshot_id":$at}"""
        StubHost().use { host ->
            withClient(host.port) { port ->
                for (token in listOf(operator(), tenant())) {
                    val consumers = mapper.readTree(send(port, "GET", "/v1/catalogs/$cat/consumers", token).body())
                    val rows = consumers["consumers"].single { it["consumer_id"].asText() == "reader" }["offsets"]
                    assertThat(rows.map { it["namespace"].asText() }).containsExactly("team_7")
                    val listed = mapper.readTree(send(port, "GET", offsets, token).body())
                    assertThat(listed.map { it["table_uuid"].asText() }).containsExactly(own.toString())
                    // Each answers as it does for a table UUID the catalog does not have, word for word.
                    val unknown = UUID.randomUUID()
                    val read = send(port, "GET", "$offsets/$hidden", token)
                    assertMissing(read, "read by uuid")
                    assertThat(read.body().replace("$hidden", "$unknown"))
                        .isEqualTo(send(port, "GET", "$offsets/$unknown", token).body())
                    val committed = send(port, "PUT", "$offsets/$hidden", token, commit)
                    assertThat(committed.statusCode()).describedAs(committed.body()).isEqualTo(422)
                    assertThat(committed.body().replace("$hidden", "$unknown"))
                        .isEqualTo(send(port, "PUT", "$offsets/$unknown", token, commit).body())
                    assertThat(read.body() + committed.body()).doesNotContain("posthog")
                    assertThat(send(port, "GET", "$offsets/$own", token).statusCode()).isEqualTo(200)
                    assertThat(send(port, "PUT", "$offsets/$own", token, commit).statusCode()).isEqualTo(200)
                }
            }
            assertThat(host.seen).isEmpty()
        }
        // The offset is as it was, and a server without the hosts file lists it again.
        assertThat(catalogs.getOffset(cat, "reader", hidden).committedSnapshot).isEqualTo(at)
        withClient(null) { port ->
            val listed = mapper.readTree(send(port, "GET", offsets, operator()).body())
            assertThat(
                listed.map { it["table_uuid"].asText() },
            ).containsExactlyInAnyOrder(own.toString(), hidden.toString())
        }
    }

    @Test
    fun `the listing hides the local namespace, shows the schema to its reader, and the name cannot be created`() {
        StubHost().use { host ->
            val before = head()
            withClient(host.port) { port ->
                val listing = "/v1/catalogs/$cat/namespaces"
                val allowed = mapper.readTree(send(port, "GET", listing, scoped()).body())
                assertThat(allowed.map { it["name"].asText() }).containsExactly("team_7", "posthog")
                assertThat(mapper.writeValueAsString(allowed[1])).isEqualTo("""{"name":"posthog","remote":true}""")
                assertThat(allowed[0].has("namespace_id")).isTrue()
                assertThat(allowed[0].has("remote")).isFalse()
                assertThat(names(send(port, "GET", listing, scoped(schemas = null)))).containsExactly("team_7")
                for (token in listOf(tenant(), operator())) {
                    assertThat(names(send(port, "GET", listing, token))).containsExactly("team_7")
                }
                // The name is reserved without being listed in HOGLAKE_RESERVED_NAMESPACES.
                for (token in listOf(tenant(), operator())) {
                    for (name in listOf("posthog", "PostHog", "POSTHOG")) {
                        val refused = send(port, "POST", listing, token, """{"name":"$name"}""")
                        assertThat(refused.statusCode()).describedAs(name).isEqualTo(422)
                        assertThat(refused.body()).contains("is reserved on this server")
                    }
                }
            }
            assertThat(host.seen).isEmpty()
            assertThat(head()).isEqualTo(before)
        }
    }

    @Test
    fun `a host that is down is an error on the schema and never the local namespace`() {
        RemoteSchemaFixtures.DeadHost().use { dead ->
            withClient(dead.port) { port ->
                for (path in relayed) {
                    val response = send(port, "GET", path, scoped())
                    assertThat(response.statusCode()).describedAs(path).isEqualTo(503)
                    assertThat(code(response)).describedAs(path).isEqualTo("mount_host_unavailable")
                    assertThat(response.body()).doesNotContain("events").doesNotContain("127.0.0.1")
                }
                // Everyone else still sees no namespace, and the listing still answers.
                assertMissing(send(port, "GET", "$schema/tables", operator()), "operator")
                assertThat(names(send(port, "GET", "/v1/catalogs/$cat/namespaces", scoped())))
                    .containsExactly("team_7", "posthog")
            }
        }
    }

    @Test
    fun `a remote schema is relayed only in a catalog this server holds`() {
        StubHost().use { host ->
            withClient(host.port) { port ->
                // The token names a catalog that does not exist, with the claim: nothing of it is here.
                val ghost = scoped(catalog = "ghost")
                val listing = send(port, "GET", "/v1/catalogs/ghost/namespaces", ghost)
                assertThat(listing.statusCode()).describedAs(listing.body()).isEqualTo(404)
                for (path in relayed) {
                    val response = send(port, "GET", path.replace("/$cat/", "/ghost/"), ghost)
                    assertThat(response.statusCode()).describedAs("%s: %s", path, response.body()).isEqualTo(404)
                    assertThat(
                        response.body(),
                    ).describedAs(path).contains("catalog 'ghost'").doesNotContain("served_by")
                }
                // A write there is the missing catalog too, not the schema's read-only answer.
                val write = send(port, "POST", "/v1/catalogs/ghost/namespaces/posthog/tables", ghost)
                assertThat(write.statusCode()).describedAs(write.body()).isEqualTo(404)
                assertThat(host.seen).isEmpty()
                // The same token shape on the catalog that exists is relayed.
                assertThat(send(port, "GET", "$schema/tables", scoped()).statusCode()).isEqualTo(200)
                assertThat(host.seen).containsExactly("$schema/tables")
            }
        }
    }

    @Test
    fun `a HEAD request on a relayed route is not relayed and reads nothing`() {
        StubHost().use { host ->
            withClient(host.port) { port ->
                for (path in relayed) {
                    for (token in listOf(scoped(), operator())) {
                        // The route table reads HEAD as GET; no route serves it, and none relays it.
                        val response = send(port, "HEAD", path, token)
                        assertThat(response.statusCode()).describedAs(path).isEqualTo(404)
                        assertThat(response.body()).describedAs(path).isEmpty()
                    }
                }
            }
            assertThat(host.seen).isEmpty()
        }
    }

    @Test
    fun `an upload claim under the hidden namespace's directory is refused for every caller`() {
        StubHost().use { host ->
            withClient(host.port) { port ->
                fun claim(
                    token: String,
                    prefix: String,
                ) = send(
                    port,
                    "PUT",
                    "/v1/catalogs/$cat/uploads/${UUID.randomUUID()}",
                    token,
                    """{"owner":"${UUID.randomUUID()}","prefix":"$prefix","file_kind":"data"}""",
                )
                // The directory spelled as it is, percent-encoded in either segment, and as a case variant.
                val spellings =
                    listOf("data/posthog", "data/posthog/events", "d%61ta/posthog", "data/post%68og", "data/PostHog")
                for (token in listOf(tenant(), operator())) {
                    for (spelling in spellings) {
                        assertMissing(claim(token, "$dataPath/$spelling"), "claim under $spelling")
                    }
                    // Any other directory of the catalog is theirs to claim, as before.
                    for (other in listOf("data/team_7/t", "data/posthog_raw", "staging/posthog")) {
                        val claimed = claim(token, "$dataPath/$other")
                        assertThat(claimed.statusCode()).describedAs("%s: %s", other, claimed.body()).isEqualTo(200)
                    }
                }
                assertReadOnly(claim(scoped(), "$dataPath/data/posthog"), "the schema's reader")
                assertMissing(claim(scoped(schemas = null), "$dataPath/data/posthog"), "scoped without the claim")
            }
            assertThat(host.seen).isEmpty()
        }
        // Without the hosts file the same claim is an ordinary one.
        withClient(null) { port ->
            val body = """{"owner":"${UUID.randomUUID()}","prefix":"$dataPath/data/posthog","file_kind":"data"}"""
            val claimed = send(port, "PUT", "/v1/catalogs/$cat/uploads/${UUID.randomUUID()}", tenant(), body)
            assertThat(claimed.statusCode()).describedAs(claimed.body()).isEqualTo(200)
        }
    }
}
