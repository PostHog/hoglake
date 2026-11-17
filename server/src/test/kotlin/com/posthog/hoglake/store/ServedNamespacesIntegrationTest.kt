package com.posthog.hoglake.store

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.configureApiListener
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.wireObjectMapper
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files

/**
 * Served namespaces on the Host end to end (docs/auth-tenancy.md §6.5d): the
 * real application over PostgreSQL, with the source catalog `imports` holding
 * namespaces marked for org A and a served-namespaces file naming some of
 * them. The callers' catalog, `cust`, does not exist on this server.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ServedNamespacesIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val pair = TestTokens.keyPair()
    private val keyFile =
        Files.createTempFile("hoglake-served-", ".json").also { Files.writeString(it, TestTokens.jwks(pair)) }
    private val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
    private val mapper = wireObjectMapper()
    private val orgA = "01a1e5de-1ac5-0000-e53e-d2f9a77deae4"
    private val orgB = "02b2f6ef-2bd6-0000-f64f-e3a0b88efbf5"
    private val source = "imports"
    private val cust = "cust"

    // The customer's own Hoglite backend: this server's backends file does not list it.
    private val hogliteId = "5b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e"
    private val ns = "/v1/catalogs/$cust/namespaces/team_42"
    private val relayed = listOf(ns, "$ns/tables", "$ns/tables/events", "$ns/tables/events/scan")
    private val filePath = "s3://bucket/$source/org_id=$orgA/team_id=42/events/a.parquet"
    private val server: io.ktor.server.engine.EmbeddedServer<*, *>
    private val app: App
    private val port: Int

    init {
        catalogs.createCatalog(source, "s3://bucket/$source")
        for (name in listOf(
            "team_42",
            "team_50",
            "team_51",
            "team_60",
            "Team_42",
        )) catalogs.createNamespace(source, name)
        catalogs.setNamespaceContainment(source, "team_42", NamespaceContainment("team_id", 42, orgA))
        // team_50 is not contained, team_51 is contained without an org, team_60 is marked but not configured.
        catalogs.setNamespaceContainment(source, "team_51", NamespaceContainment("team_id", 51))
        catalogs.setNamespaceContainment(source, "team_60", NamespaceContainment("team_id", 60, orgA))
        for (name in listOf("team_42", "team_50", "team_51", "team_60")) {
            catalogs.createTable(source, name, "events", listOf(ColumnDef("v", ColType.LONG)))
        }
        commits.commit(
            source,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend("team_42", "events", listOf(FileRegistration(filePath, 10, 1_000, 20, null, null))),
                    ),
            ),
        )
        val served =
            Files.createTempFile("hoglake-served-ns-", ".json").also {
                it.toFile().deleteOnExit()
                Files.writeString(
                    it,
                    """{"namespaces":[""" +
                        listOf("team_42", "team_50", "team_51", "team_70").joinToString(",") { n ->
                            """{"name":"$n","source_catalog":"$source"}"""
                        } + "]}",
                )
            }
        val cfg =
            Config(port = 0, hydratorIntervalMs = 0).copy(
                authMode = "enforce",
                authIssuer = TestTokens.ISSUER,
                authJwksFile = keyFile.toString(),
                authPredicateFields = "team_id",
                authSessionProfile = "on",
                servedNamespacesFile = served.toString(),
            )
        app = App.build(cfg, db.jdbi)
        server = embeddedServer(Netty, configure = { configureApiListener(cfg) }) { app.module(this) }
        server.start(wait = false)
        port = runBlocking { server.engine.resolvedConnectors().first().port }
    }

    @AfterAll
    fun close() {
        server.stop(0, 1000)
        app.close()
        http.close()
        db.close()
        Files.deleteIfExists(keyFile)
    }

    /**
     * A session-profile scoped token as the real issuer shapes it: [catalog] is the one `cat` entry and the
     * primary catalog. A null [schemas] leaves `remote_schemas` out; [rtg] is spliced in verbatim.
     */
    private fun scoped(
        org: String = orgA,
        prd: String = """{"team_id":{"42":"read"}}""",
        schemas: String? = """{"team_42":["read"]}""",
        catalog: String = cust,
        scope: String = "read",
        rtg: String? = null,
    ): String {
        val iat = System.currentTimeMillis() / 1000 - 5
        val claims =
            TestTokens.scopedClaims(
                prd = prd,
                catalogs = """{"$catalog":["$scope"]}""",
                iat = iat,
                extra = TestTokens.sessionProfile(catalog, schemas) + (rtg?.let { ""","rtg":$it""" } ?: ""),
            ).replace("\"org\":\"11111111-1111-1111-1111-111111111111\"", "\"org\":\"$org\"")
        return TestTokens.mint(pair, claims)
    }

    private fun tenant() =
        TestTokens.mint(
            pair,
            TestTokens.tenantClaims(org = orgA, catalogs = mapOf(cust to listOf("admin"))).dropLast(1) +
                TestTokens.sessionProfile(cust, """{"team_42":["read"]}""") + "}",
        )

    private fun operator() = TestTokens.mint(pair, TestTokens.operatorClaims())

    private fun send(
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

    private fun head(): Long = catalogs.getCatalog(source).headSnapshotId

    /** The 404 of a missing namespace, with nothing of the source in it. */
    private fun assertMissing(
        response: HttpResponse<String>,
        what: String,
    ) {
        assertThat(response.statusCode()).describedAs("%s: %s", what, response.body()).isEqualTo(404)
        assertThat(code(response)).describedAs(what).isEqualTo("not_found")
        assertThat(response.body()).describedAs(what).doesNotContain("events", "a.parquet", orgA, "imports")
    }

    private fun assertMissingNamespace(
        response: HttpResponse<String>,
        what: String,
        path: String = ns,
    ) {
        assertMissing(response, what)
        val name = path.substringAfter("/namespaces/").substringBefore('/')
        assertThat(response.body()).describedAs(what).contains("namespace '$name' in catalog '$cust'")
    }

    @Test
    fun `the four relayed routes serve the source namespace to its org's reader`() {
        val before = head()
        val namespace = send("GET", ns, scoped())
        assertThat(namespace.statusCode()).describedAs(namespace.body()).isEqualTo(200)
        val body = mapper.readTree(namespace.body())
        assertThat(body["name"].asText()).isEqualTo("team_42")
        // The org mark and field are the Host's own.
        assertThat(namespace.body()).doesNotContain("containment", orgA)

        val tables = send("GET", "$ns/tables", scoped())
        assertThat(tables.statusCode()).describedAs(tables.body()).isEqualTo(200)
        assertThat(mapper.readTree(tables.body()).map { it["name"].asText() }).containsExactly("events")

        val table = send("GET", "$ns/tables/events", scoped())
        assertThat(table.statusCode()).describedAs(table.body()).isEqualTo(200)
        assertThat(mapper.readTree(table.body())["name"].asText()).isEqualTo("events")

        val scan = send("GET", "$ns/tables/events/scan", scoped())
        assertThat(scan.statusCode()).describedAs(scan.body()).isEqualTo(200)
        assertThat(scan.body()).contains(filePath)
        assertThat(head()).isEqualTo(before)
    }

    @Test
    fun `a token shaped as the issuer shapes it, naming the org's own catalog, is served`() {
        val own = "/v1/catalogs/$orgA/namespaces/team_42"
        val token = scoped(catalog = orgA)
        for (path in listOf(own, "$own/tables", "$own/tables/events", "$own/tables/events/scan")) {
            assertThat(send("GET", path, token).statusCode()).describedAs(path).isEqualTo(200)
        }
    }

    @Test
    fun `a wildcard prd with the namespace's org is served`() {
        for (path in relayed) {
            val response = send("GET", path, scoped(prd = """{"team_id":{"*":"read"}}"""))
            assertThat(response.statusCode()).describedAs("%s: %s", path, response.body()).isEqualTo(200)
        }
    }

    @Test
    fun `a token whose rtg names a backend this server does not list is served from PostgreSQL`() {
        val rtg = """{"b":"hoglite","be":"$hogliteId","t":"opaque_1"}"""
        for (path in relayed) {
            val response = send("GET", path, scoped(rtg = rtg))
            assertThat(response.statusCode()).describedAs("%s: %s", path, response.body()).isEqualTo(200)
        }
    }

    @Test
    fun `every caller the rule does not admit gets a missing namespace and nothing of the source`() {
        val refused =
            mapOf(
                "another org" to scoped(org = orgB),
                "another org with a wildcard prd" to scoped(org = orgB, prd = """{"team_id":{"*":"read"}}"""),
                "prd without the value" to scoped(prd = """{"team_id":{"43":"read"}}"""),
                "no remote_schemas claim" to scoped(schemas = null),
                "remote_schemas naming another namespace" to scoped(schemas = """{"team_43":["read"]}"""),
            )
        for ((what, token) in refused) {
            for (path in relayed) {
                assertMissingNamespace(send("GET", path, token), "$what: $path")
            }
        }
    }

    @Test
    fun `a tenant and an operator are not entered in the served rule and get the answer of a missing catalog`() {
        for (token in listOf(tenant(), operator())) {
            for (path in relayed) {
                val configured = send("GET", path, token)
                assertThat(configured.statusCode()).describedAs(path).isEqualTo(404)
                assertThat(configured.body()).doesNotContain("events", "a.parquet", orgA, "imports")
                // The same words for a name that is not configured: no refusal says whether a name is served.
                val other = send("GET", path.replace("team_42", "team_99"), token)
                assertThat(other.statusCode()).isEqualTo(404)
                assertThat(other.body()).isEqualTo(configured.body())
            }
        }
    }

    @Test
    fun `a refused scoped caller gets the same words for a configured and an unconfigured name`() {
        val refused = scoped(org = orgB, schemas = """{"team_42":["read"],"team_99":["read"]}""")
        for (suffix in listOf("", "/tables", "/tables/events", "/tables/events/scan")) {
            val configured = send("GET", "$ns$suffix", refused)
            val other = send("GET", "${ns.replace("team_42", "team_99")}$suffix", refused)
            assertThat(configured.statusCode()).describedAs(suffix).isEqualTo(404)
            assertThat(configured.body().replace("team_42", "team_99")).describedAs(suffix).isEqualTo(other.body())
        }
    }

    @Test
    fun `the served namespace carries no id, and a pinned read works`() {
        val head = head()
        assertThat(send("GET", ns, scoped()).body()).doesNotContain("namespace_id")
        for (pin in listOf("snapshot=$head", "at_timestamp=${java.time.Instant.now().plusSeconds(3600)}")) {
            for (path in relayed) {
                val response = send("GET", "$path?$pin", scoped())
                assertThat(response.statusCode()).describedAs("%s?%s: %s", path, pin, response.body()).isEqualTo(200)
            }
        }
    }

    @Test
    fun `errors for an admitted caller speak of the caller's catalog and reveal no source detail`() {
        val head = head()
        val token = scoped()
        val leaks = listOf("imports", head.toString(), "earliest", "[0,")
        val missingTable = send("GET", "$ns/tables/nope", token)
        assertThat(missingTable.statusCode()).isEqualTo(404)
        assertThat(missingTable.body()).contains("table 'team_42.nope' in catalog 'cust'")
        val missingScan = send("GET", "$ns/tables/nope/scan", token)
        assertThat(missingScan.statusCode()).isEqualTo(404)
        assertThat(missingScan.body()).contains("table 'team_42.nope' in catalog 'cust'")
        // Before the table existed: the table is missing at that snapshot.
        val early = send("GET", "$ns/tables/events?snapshot=0", token)
        assertThat(early.statusCode()).describedAs(early.body()).isEqualTo(404)
        assertThat(early.body()).contains("table 'team_42.events' in catalog 'cust'")
        val beyond = send("GET", "$ns/tables/events?snapshot=${head + 1000}", token)
        val beyondScan = send("GET", "$ns/tables/events/scan?snapshot=${head + 1000}", token)
        val beyondTables = send("GET", "$ns/tables?snapshot=${head + 1000}", token)
        val beyondNamespace = send("GET", "$ns?snapshot=${head + 1000}", token)
        val floor = send("GET", "$ns/tables/events?at_timestamp=2000-01-01T00:00:00Z", token)
        val floorScan = send("GET", "$ns/tables/events/scan?at_timestamp=2000-01-01T00:00:00Z", token)
        for (response in listOf(beyond, beyondScan, beyondTables, beyondNamespace)) {
            assertThat(response.statusCode()).describedAs(response.body()).isEqualTo(422)
        }
        for (response in listOf(floor, floorScan)) {
            assertThat(response.statusCode()).describedAs(response.body()).isEqualTo(410)
        }
        val all = listOf(missingTable, missingScan, early, beyond, beyondScan, beyondTables, beyondNamespace, floor)
        for (response in all + floorScan) {
            assertThat(response.body()).describedAs(response.body()).doesNotContain(*leaks.toTypedArray())
        }
    }

    @Test
    fun `a mount's reserved name is never a served catalog`() {
        val mount = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f0-events"
        for (path in relayed) {
            val response = send("GET", path.replace("/$cust/", "/$mount/"), scoped(catalog = mount))
            assertThat(response.statusCode()).describedAs(path).isEqualTo(404)
            assertThat(response.body()).describedAs(path).doesNotContain("events\"", "a.parquet", orgA, "imports")
        }
    }

    @Test
    fun `a dropped namespace stops being served, pinned reads included`() {
        val catalogName = source
        catalogs.createNamespace(catalogName, "team_70")
        catalogs.setNamespaceContainment(catalogName, "team_70", NamespaceContainment("team_id", 70, orgA))
        catalogs.createTable(catalogName, "team_70", "events", listOf(ColumnDef("v", ColType.LONG)))
        val live = head()
        val path = "/v1/catalogs/$cust/namespaces/team_70/tables"
        val token = scoped(prd = """{"team_id":{"70":"read"}}""", schemas = """{"team_70":["read"]}""")
        assertThat(send("GET", path, token).statusCode()).isEqualTo(200)
        catalogs.dropTable(catalogName, "team_70", "events", author = "test", message = "test")
        catalogs.dropNamespace(catalogName, "team_70", author = "test", message = "test")
        assertThat(send("GET", path, token).statusCode()).isEqualTo(404)
        val pinned = send("GET", "$path?snapshot=$live", token)
        assertThat(pinned.statusCode()).describedAs(pinned.body()).isEqualTo(404)
        assertThat(pinned.body()).doesNotContain("events", "imports")
    }

    @Test
    fun `a token whose cat does not name the catalog is refused as a missing catalog`() {
        for (path in relayed) {
            val response = send("GET", path, scoped(catalog = "other"))
            assertThat(response.statusCode()).describedAs(path).isEqualTo(404)
            assertThat(response.body()).describedAs(path).doesNotContain("events", "a.parquet", orgA, "imports")
        }
    }

    @Test
    fun `a stored namespace that is not contained for the caller's org is not served`() {
        // Not contained at all, and contained without an org: the same refusal as a missing namespace.
        for (name in listOf("team_50", "team_51")) {
            val path = "/v1/catalogs/$cust/namespaces/$name"
            val token =
                scoped(prd = """{"team_id":{"*":"read"}}""", schemas = """{"$name":["read"]}""")
            for (relay in listOf(path, "$path/tables", "$path/tables/events", "$path/tables/events/scan")) {
                assertMissingNamespace(send("GET", relay, token), "$name: $relay", path)
            }
        }
    }

    @Test
    fun `a case variant and an unconfigured name are not served`() {
        // `Team_42` exists in the source but is no served name; `team_60` is org-marked but not configured.
        for (name in listOf("Team_42", "team_60", "team_99")) {
            val token = scoped(prd = """{"team_id":{"*":"read"}}""", schemas = """{"$name":["read"]}""")
            val response = send("GET", "/v1/catalogs/$cust/namespaces/$name/tables", token)
            assertThat(response.statusCode()).describedAs(name).isEqualTo(404)
            assertThat(response.body()).describedAs(name).doesNotContain("events", "a.parquet", orgA)
        }
    }

    @Test
    fun `writes and unrelayed reads are refused and leave the source untouched`() {
        val before = head()
        val writer = scoped(prd = """{"team_id":{"42":"write"}}""", scope = "write")
        val create = """{"name":"made","columns":[{"name":"v","type":"long"}]}"""
        val made = send("POST", "$ns/tables", writer, create)
        assertThat(made.statusCode()).describedAs(made.body()).isEqualTo(403)
        assertThat(code(made)).isEqualTo("read_only_catalog")
        val dropNs = send("DELETE", ns, writer)
        assertThat(dropNs.statusCode()).describedAs(dropNs.body()).isEqualTo(403)
        // A namespace drop is outside a scoped token's route set: the route table answers first.
        assertThat(code(dropNs)).isEqualTo("forbidden")
        val dropTable = send("DELETE", "$ns/tables/events", writer)
        assertThat(dropTable.statusCode()).describedAs(dropTable.body()).isEqualTo(403)
        assertThat(code(dropTable)).isEqualTo("read_only_catalog")
        // The read-only token meets the route table's own refusal first.
        val readOnly = send("POST", "$ns/tables", scoped(), create)
        assertThat(readOnly.statusCode()).describedAs(readOnly.body()).isEqualTo(403)

        // A commit naming the namespace goes to the caller's catalog, which this server does not hold.
        val commit =
            """{"appends":[{"namespace":"team_42","table":"events","files":[{"path":"$filePath","record_count":1,""" +
                """"file_size_bytes":1}]}]}"""
        val committed = send("POST", "/v1/catalogs/$cust/commit", writer, commit)
        assertThat(committed.statusCode()).describedAs(committed.body()).isIn(403, 404)

        val files = send("GET", "$ns/tables/events/files", scoped())
        assertThat(files.statusCode()).describedAs(files.body()).isEqualTo(501)
        assertThat(code(files)).isEqualTo("mount_operation_unavailable")

        assertThat(head()).isEqualTo(before)
        assertThat(catalogs.listTables(source, "team_42").map { it.name }).containsExactly("events")
        assertThat(catalogs.getNamespace(source, "team_42").containment)
            .isEqualTo(NamespaceContainment("team_id", 42, orgA))
    }

    @Test
    fun `an operator reading the source catalog directly keeps the local path`() {
        val tables = send("GET", "/v1/catalogs/$source/namespaces/team_42/tables", operator())
        assertThat(tables.statusCode()).describedAs(tables.body()).isEqualTo(200)
        assertThat(mapper.readTree(tables.body()).map { it["name"].asText() }).containsExactly("events")
        val namespace = send("GET", "/v1/catalogs/$source/namespaces/team_42", operator())
        // Unlike the served view, the source's own answer carries its containment.
        assertThat(namespace.body()).contains("containment")
    }
}
