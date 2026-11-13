package com.posthog.hoglake.auth

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.configureApiListener
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.NamespaceContainment
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
import java.util.UUID

/**
 * A scoped caller on the write routes, through the real application. It
 * writes to the contained namespaces whose value it holds `write` on, and to
 * no other: not a read-only value's, not another team's, not an uncontained
 * one, whether the namespace is in the path, in the body, or behind a
 * creation's operation id. Every [AuthPolicy.BODY_SCOPED_ROUTES] entry is
 * driven here ([bodyRoutesCovered]).
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ScopedWritesIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val pair = TestTokens.keyPair()
    private val keyFile =
        Files.createTempFile("hoglake-scoped-writes-", ".json").also {
            Files.writeString(it, TestTokens.jwks(pair))
        }
    private val json = wireObjectMapper()
    private val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
    private val cat = "scopedw"
    private val dataPath = "s3://bucket/$cat"
    private val columns = listOf(ColumnDef("v", ColType.LONG))
    private val driven = mutableSetOf<Pair<String, String>>()
    private var serial = 0

    /** A fresh table name: [bodyRoutesCovered] runs the route tests a second time. */
    private fun fresh(prefix: String) = "${prefix}_${++serial}"

    init {
        catalogs.createCatalog(cat, dataPath)
        for ((ns, value) in listOf("team_42" to 42L, "imports_42" to 42L, "team_7" to 7L, "team_9" to 9L)) {
            catalogs.createNamespace(cat, ns)
            catalogs.setNamespaceContainment(cat, ns, NamespaceContainment("team_id", value))
        }
        catalogs.createNamespace(cat, "main")
        for (ns in listOf(
            "team_42",
            "imports_42",
            "team_7",
            "team_9",
            "main",
        )) catalogs.createTable(cat, ns, "t", columns)
    }

    @AfterAll
    fun close() {
        http.close()
        db.close()
        Files.deleteIfExists(keyFile)
    }

    private fun scoped(prd: String = """{"team_id":{"42":"write","7":"read"}}"""): String {
        val iat = System.currentTimeMillis() / 1000 - 5
        return TestTokens.mint(
            pair,
            TestTokens.scopedClaims(prd = prd, catalogs = """{"$cat":["write"]}""", iat = iat),
        )
    }

    private fun tenant() = TestTokens.mint(pair, TestTokens.tenantClaims(catalogs = mapOf(cat to listOf("write"))))

    private fun <T> withServer(
        mode: AuthMode,
        block: (port: Int) -> T,
    ): T {
        val cfg =
            Config(port = 0, hydratorIntervalMs = 0).copy(
                authMode = mode.wire,
                authIssuer = TestTokens.ISSUER,
                authJwksFile = keyFile.toString(),
                authPredicateFields = "team_id",
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

    private fun call(
        port: Int,
        token: String,
        method: String,
        path: String,
        body: String? = null,
        pattern: String? = null,
    ): HttpResponse<String> {
        pattern?.let { driven += method to "/v1/catalogs/{catalog}$it" }
        return http.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port/v1/catalogs/$cat$path"))
                .header("Authorization", "Bearer $token")
                .header("Content-Type", "application/json")
                .method(
                    method,
                    body?.let { HttpRequest.BodyPublishers.ofString(it) } ?: HttpRequest.BodyPublishers.noBody(),
                )
                .build(),
            HttpResponse.BodyHandlers.ofString(),
        )
    }

    private fun assertMissing(
        response: HttpResponse<String>,
        what: String,
    ) {
        assertThat(response.statusCode()).describedAs("%s: %s", what, response.body()).isEqualTo(404)
        assertThat(response.body()).describedAs(what).doesNotContain("team_id").doesNotContain("scoped credential")
    }

    private fun ok(
        response: HttpResponse<String>,
        what: String,
    ) {
        assertThat(response.statusCode()).describedAs("%s: %s", what, response.body()).isBetween(200, 201)
    }

    private fun file(ns: String) =
        """{"path":"$dataPath/data/$ns/${UUID.randomUUID()}.parquet","record_count":1,"file_size_bytes":10}"""

    private fun append(
        ns: String,
        uuid: UUID? = null,
    ) = """{"namespace":"$ns","table":"t","files":[${file(ns)}]""" +
        (uuid?.let { ""","expected_table_uuid":"$it"""" } ?: "") + "}"

    private fun tableUuid(ns: String): UUID = catalogs.getTable(cat, ns, "t").tableUuid

    private val commitRoutes =
        listOf("/commit", "/commit/prepared", "/commit/deletes/prepared", "/commit/mutations/prepared")
            .plus(listOf("/commit/transaction", "/commit/uploads"))

    @Test
    fun `path-named writes follow the namespace's value`() {
        for (mode in listOf(AuthMode.WARN, AuthMode.ENFORCE)) {
            withServer(mode) { port ->
                val token = scoped()
                val name = "c_${mode.wire}"
                val create = """{"name":"$name","columns":[{"name":"v","type":"long"}]}"""
                val made = call(port, token, "POST", "/namespaces/team_42/tables", create)
                assertThat(made.statusCode()).describedAs(made.body()).isBetween(200, 201)
                val view = """{"name":"v_${mode.wire}","sql":"SELECT 1"}"""
                val base = "/namespaces/team_42"
                ok(call(port, token, "POST", "$base/views", view), "$mode create view")
                ok(call(port, token, "DELETE", "$base/views/v_${mode.wire}?message=drop"), "$mode drop view")
                val uuid = catalogs.getTable(cat, "team_42", name).tableUuid
                val truncate = "$base/tables/$name/truncate?expected_table_uuid=$uuid"
                ok(call(port, token, "POST", truncate, """{"message":"t"}"""), "$mode truncate")
                ok(call(port, token, "DELETE", "$base/tables/$name?message=drop"), "$mode drop")

                // A read-only value, another team's, an uncontained one.
                for (ns in listOf("team_7", "team_9", "main")) {
                    val what = "$mode $ns"
                    assertMissing(call(port, token, "POST", "/namespaces/$ns/tables", create), "$what create")
                    assertMissing(call(port, token, "DELETE", "/namespaces/$ns/tables/t"), "$what drop")
                    assertMissing(call(port, token, "POST", "/namespaces/$ns/tables/t/truncate", "{}"), "$what trunc")
                    assertMissing(call(port, token, "POST", "/namespaces/$ns/tables/t/alter", "{}"), "$what alter")
                    assertMissing(call(port, token, "POST", "/namespaces/$ns/views", view), "$what view")
                    assertMissing(call(port, token, "DELETE", "/namespaces/$ns/views/x"), "$what drop view")
                }
                // Namespace DDL stays the operator's.
                assertThat(call(port, token, "POST", "/namespaces", """{"name":"team_99"}""").statusCode())
                    .isEqualTo(403)
                assertThat(call(port, token, "DELETE", "/namespaces/team_42").statusCode()).isEqualTo(403)
            }
        }
    }

    @Test
    fun `a commit is held to every namespace its body names, on every commit route`() {
        withServer(AuthMode.ENFORCE) { port ->
            val token = scoped()
            for (route in commitRoutes) {
                for (ns in listOf("team_7", "team_9", "main")) {
                    val body =
                        """{"appends":[${append(ns, tableUuid(ns))}],"read_snapshot":1,""" +
                            """"idempotency_key":"${UUID.randomUUID()}"}"""
                    assertMissing(call(port, token, "POST", route, body, pattern = route), "$route $ns")
                    // One allowed table does not carry another.
                    val mixed =
                        """{"appends":[${append("team_42", tableUuid("team_42"))},""" +
                            """${append(ns, tableUuid(ns))}],"read_snapshot":1,""" +
                            """"idempotency_key":"${UUID.randomUUID()}"}"""
                    assertMissing(call(port, token, "POST", route, mixed), "$route team_42+$ns")
                }
                // Deletes name namespaces too.
                val deletes =
                    """{"deletes":[{"namespace":"team_9","table":"t","files":[],""" +
                        """"expected_table_uuid":"${tableUuid("team_9")}"}],"read_snapshot":1,""" +
                        """"idempotency_key":"${UUID.randomUUID()}"}"""
                assertMissing(call(port, token, "POST", route, deletes), "$route deletes")
            }
            // Its own namespace commits, and containment still holds the paths.
            val ok = call(port, token, "POST", "/commit", """{"appends":[${append("team_42")}]}""")
            assertThat(ok.statusCode()).describedAs(ok.body()).isEqualTo(200)
            val stray =
                """{"appends":[{"namespace":"team_42","table":"t","files":[""" +
                    """{"path":"$dataPath/data/team_9/x.parquet","record_count":1,"file_size_bytes":10}]}]}"""
            assertThat(call(port, token, "POST", "/commit", stray).statusCode()).isEqualTo(422)
            // A tenant is unaffected.
            assertThat(call(port, tenant(), "POST", "/commit", """{"appends":[${append("main")}]}""").statusCode())
                .isEqualTo(200)
        }
    }

    @Test
    fun `a token for a value commits into every namespace contained for it, each held to its own prefix`() {
        withServer(AuthMode.ENFORCE) { port ->
            val token = scoped()
            for (ns in listOf("team_42", "imports_42")) {
                val ok = call(port, token, "POST", "/commit", """{"appends":[${append(ns)}]}""")
                assertThat(ok.statusCode()).describedAs("%s: %s", ns, ok.body()).isEqualTo(200)
            }
            // A file of one namespace under the other's prefix is refused,
            // both ways: the token reaches both, containment still holds the paths.
            for ((into, under) in listOf("team_42" to "imports_42", "imports_42" to "team_42")) {
                val stray =
                    """{"appends":[{"namespace":"$into","table":"t","files":[""" +
                        """{"path":"$dataPath/data/$under/x.parquet","record_count":1,"file_size_bytes":10}]}]}"""
                val r = call(port, token, "POST", "/commit", stray)
                assertThat(r.statusCode()).describedAs("%s under %s: %s", into, under, r.body()).isEqualTo(422)
            }
            // A token for another value reaches neither.
            val team7 = scoped(prd = """{"team_id":{"7":"write"}}""")
            for (ns in listOf("team_42", "imports_42")) {
                assertMissing(call(port, team7, "POST", "/commit", """{"appends":[${append(ns)}]}"""), "team 7 $ns")
            }
        }
    }

    @Test
    fun `a receipt does not replay another namespace's commit to a scoped caller`() {
        withServer(AuthMode.ENFORCE) { port ->
            val key = UUID.randomUUID()
            val body = """{"appends":[${append("team_9")}],"idempotency_key":"$key"}"""
            assertThat(call(port, tenant(), "POST", "/commit/prepared", body).statusCode()).isEqualTo(200)
            assertMissing(call(port, scoped(), "POST", "/commit/prepared", body), "replay")
        }
    }

    @Test
    fun `a table creation is held to its namespace at prepare and after`() {
        withServer(AuthMode.ENFORCE) { port ->
            val token = scoped()
            val prepares = listOf("", "/partitioned", "/sorted", "/metadata")
            for (suffix in prepares) {
                val pattern = "/table-creations/{operation}$suffix"
                for (ns in listOf("team_7", "team_9", "main")) {
                    val body = """{"namespace":"$ns","name":"n","columns":[{"name":"v","type":"long"}]}"""
                    val r = call(port, token, "PUT", "/table-creations/${UUID.randomUUID()}$suffix", body, pattern)
                    assertMissing(r, "prepare$suffix $ns")
                }
            }
            // Its own: prepare, read, publish.
            val own = "/table-creations/${UUID.randomUUID()}"
            val body = """{"namespace":"team_42","name":"${fresh("made")}","columns":[{"name":"v","type":"long"}]}"""
            val prepared = call(port, token, "PUT", own, body)
            assertThat(prepared.statusCode()).describedAs(prepared.body()).isEqualTo(200)
            assertThat(json.readTree(prepared.body()).path("write_path").asText())
                .startsWith("$dataPath/data/team_42/")
            assertThat(call(port, token, "GET", own).statusCode()).isEqualTo(200)
            val published = call(port, token, "POST", "$own/commit", """{"files":[]}""")
            assertThat(published.statusCode()).describedAs(published.body()).isEqualTo(200)

            // Another team's creation, prepared by a tenant: invisible and
            // untouchable by id.
            for (suffix in listOf("", "/commit", "/commit/uploads", "/abort")) {
                val theirs = "/table-creations/${UUID.randomUUID()}"
                val other = """{"namespace":"team_9","name":"${fresh("n")}","columns":[{"name":"v","type":"long"}]}"""
                assertThat(call(port, tenant(), "PUT", theirs, other).statusCode()).isEqualTo(200)
                val method = if (suffix.isEmpty()) "GET" else "POST"
                val r =
                    call(
                        port,
                        token,
                        method,
                        "$theirs$suffix",
                        """{"files":[]}""".takeIf { suffix.isNotEmpty() },
                        "/table-creations/{operation}$suffix",
                    )
                assertMissing(r, "$method creation$suffix")
                // Still the tenant's to finish.
                assertThat(call(port, tenant(), "GET", theirs).path("state")).isEqualTo("prepared")
            }
            // Abort its own.
            val mine = "/table-creations/${UUID.randomUUID()}"
            val second = """{"namespace":"team_42","name":"${fresh(
                "aborted",
            )}","columns":[{"name":"v","type":"long"}]}"""
            assertThat(call(port, token, "PUT", mine, second).statusCode()).isEqualTo(200)
            assertThat(call(port, token, "POST", "$mine/abort").statusCode()).isEqualTo(200)
        }
    }

    private fun HttpResponse<String>.path(field: String) = json.readTree(body()).path(field).asText()

    @Test
    fun `an upload claim is held to a namespace spelled literally`() {
        withServer(AuthMode.ENFORCE) { port ->
            val token = scoped()
            val owner = UUID.randomUUID()

            fun claim(prefix: String) =
                call(
                    port,
                    token,
                    "PUT",
                    "/uploads/${UUID.randomUUID()}",
                    """{"owner":"$owner","prefix":"$prefix","file_kind":"data"}""",
                    "/uploads/{upload}",
                )
            val ok = claim("$dataPath/data/team_42/t")
            assertThat(ok.statusCode()).describedAs(ok.body()).isEqualTo(200)
            assertThat(claim("$dataPath/data/team_42").statusCode()).isEqualTo(200)
            for (ns in listOf("team_7", "team_9", "main", "team%5F42")) {
                assertMissing(claim("$dataPath/data/$ns/t"), "claim $ns")
            }
            for (prefix in listOf("$dataPath/elsewhere", "$dataPath/data", "$dataPath/%64ata/team_42", dataPath)) {
                val r = claim(prefix)
                assertThat(r.statusCode()).describedAs("%s: %s", prefix, r.body()).isIn(403, 404)
            }
            // Renew and abandon name no namespace.
            assertThat(call(port, token, "POST", "/uploads/renew", """{"owner":"$owner"}""").statusCode())
                .isEqualTo(200)
            assertThat(
                call(port, token, "POST", "/uploads/abandon", """{"owner":"$owner","paths":[]}""").statusCode(),
            ).isEqualTo(200)
            assertThat(call(port, token, "POST", "/uploads/schedule-expired").statusCode()).isEqualTo(403)
        }
    }

    @Test
    fun bodyRoutesCovered() {
        // The other tests may run in any order, or alone, so drive the body
        // routes again here and compare.
        `a commit is held to every namespace its body names, on every commit route`()
        `a table creation is held to its namespace at prepare and after`()
        `an upload claim is held to a namespace spelled literally`()
        assertThat(driven).containsExactlyInAnyOrderElementsOf(
            AuthPolicy.BODY_SCOPED_ROUTES.map { (method, pattern) -> method.value to pattern },
        )
    }
}
