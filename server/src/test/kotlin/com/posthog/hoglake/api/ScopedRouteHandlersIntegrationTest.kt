package com.posthog.hoglake.api

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.AuthPolicy
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.configureApiListener
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.ViewService
import com.posthog.hoglake.store.auditRefusals
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
 * A scoped caller on every route the route table opens to it, through the
 * real application and its real handlers over PostgreSQL. Each request is
 * consistent: the namespace of its path, of its body and behind its id is
 * one the token holds for write. The reads are sent again at a snapshot and
 * at a timestamp.
 *
 * No answer is a 403 or a 404, and no refusal is audited at the store
 * (`at=store`): no handler asks its view of the store (auth/StoreGrant.kt)
 * for anything the checks before it did not allow, such as an operation the
 * view closes to a scoped caller. RoutePathValuesTest drives the same
 * handlers with recording backends, for an operator and a tenant.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ScopedRouteHandlersIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val pair = TestTokens.keyPair()
    private val keyFile =
        Files.createTempFile("hoglake-scoped-handlers-", ".json").also { Files.writeString(it, TestTokens.jwks(pair)) }
    private val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
    private val mapper = wireObjectMapper()
    private val cat = "alpha"
    private val dataPath = "s3://bucket/$cat"
    private val ns = "team_7"

    /** Another team's namespace, which the token does not hold. */
    private val theirs = "team_9"
    private val columns = """[{"name":"v","type":"long"}]"""

    init {
        catalogs.createCatalog(cat, dataPath)
        catalogs.createNamespace(cat, ns)
        catalogs.setNamespaceContainment(cat, ns, NamespaceContainment("team_id", 7))
        catalogs.createTable(cat, ns, "t", listOf(ColumnDef("v", ColType.LONG)))
        catalogs.createNamespace(cat, theirs)
        catalogs.setNamespaceContainment(cat, theirs, NamespaceContainment("team_id", 9))
        catalogs.createTable(cat, theirs, "t", listOf(ColumnDef("v", ColType.LONG)))
        ViewService(db.jdbi).create(cat, ns, "v", "SELECT 1")
    }

    @AfterAll
    fun close() {
        http.close()
        db.close()
        Files.deleteIfExists(keyFile)
    }

    /** A scoped token for team 7 of `alpha`, for write. */
    private fun scoped(): String {
        val iat = System.currentTimeMillis() / 1000 - 5
        val claims =
            TestTokens.scopedClaims(
                prd = """{"team_id":{"7":"write"}}""",
                catalogs = """{"$cat":["write"]}""",
                iat = iat,
            )
        return TestTokens.mint(pair, claims)
    }

    private fun <T> withServer(block: (port: Int) -> T): T {
        val cfg =
            Config(port = 0, hydratorIntervalMs = 0).copy(
                authMode = "enforce",
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

    private fun tableUuid(
        table: String,
        namespace: String = ns,
    ): UUID = catalogs.getTable(cat, namespace, table).tableUuid

    private fun exchange(
        port: Int,
        token: String,
        method: String,
        path: String,
        body: String? = null,
    ): HttpResponse<String> =
        http.send(
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

    private fun file(prefix: String = "$dataPath/data/$ns") =
        """{"path":"$prefix/${UUID.randomUUID()}.parquet","record_count":1,"file_size_bytes":10}"""

    @Test
    fun `a consistent request from a scoped caller is served on every route open to it`() {
        val answers = mutableListOf<String>()
        val driven = mutableSetOf<Pair<String, String>>()
        val (_, lines) = auditRefusals { withServer { port -> driveEveryRoute(port, answers, driven) } }
        // MUTATION: give a view method a rule that refuses what its route allows
        // (close it, or check a read as a write) and that route answers 403 or 404 here.
        assertThat(answers.filterNot { it.startsWith("2") }).isEmpty()
        assertThat(lines.filter { "at=store" in it }).isEmpty()
        // Not vacuous: every route a scoped caller may use.
        val scopedRoutes = AuthPolicy.SCOPED_ROUTES + AuthPolicy.BODY_SCOPED_ROUTES
        assertThat(driven).containsExactlyInAnyOrderElementsOf(scopedRoutes.map { (m, pattern) -> m.value to pattern })
    }

    @Test
    fun `a replacement that ends another namespace's files is refused by the route's own check`() {
        val (response, lines) =
            auditRefusals {
                withServer { port ->
                    fun table(namespace: String) =
                        """"namespace":"$namespace","table":"t","expected_table_uuid":"${tableUuid("t", namespace)}""""
                    val own = """{${table(ns)},"files":[${file()}]}"""
                    val ends = """{${table(theirs)},"all_files":true}"""
                    val head = catalogs.getCatalog(cat).headSnapshotId
                    val body =
                        """{"appends":[$own],"file_ends":[$ends],"message":"r",""" +
                            """"read_snapshot":$head,"idempotency_key":"${UUID.randomUUID()}"}"""
                    exchange(port, scoped(), "POST", "/commit/replacements/prepared", body)
                }
            }
        assertThat(response.statusCode()).describedAs(response.body()).isEqualTo(404)
        assertThat(response.body()).contains("namespace '$theirs' in catalog '$cat'")
        // MUTATION: leave the ended files out of the route's own check and the
        // view refuses them instead, audited `at=store`.
        assertThat(lines).hasSize(1)
        assertThat(lines.single()).contains("namespace '$theirs'").doesNotContain("at=store")
    }

    private fun driveEveryRoute(
        port: Int,
        answers: MutableList<String>,
        driven: MutableSet<Pair<String, String>>,
    ) {
        val token = scoped()

        fun send(
            method: String,
            pattern: String,
            path: String,
            body: String? = null,
        ): HttpResponse<String> {
            driven += method to "/v1/catalogs/{catalog}$pattern"
            val response = exchange(port, token, method, path, body)
            answers += "${response.statusCode()} $method $path ${response.body().take(300)}"
            return response
        }
        val table = "/namespaces/{namespace}/tables/{table}"
        val views = "/namespaces/{namespace}/views"

        // A file in the table, so that its stats and its partition values have something to show.
        send("POST", "/commit", "/commit", """{"appends":[{"namespace":"$ns","table":"t","files":[${file()}]}]}""")
        val head = catalogs.getCatalog(cat).headSnapshotId
        // The head's own time, from the database's clock: this process's clock may be behind it.
        val now = catalogs.listSnapshots(cat, head - 1, 1).first.single().snapshotTime
        val files = send("GET", "$table/files", "/namespaces/$ns/tables/t/files")
        val fileId = mapper.readTree(files.body()).first()["data_file_id"].asLong()

        // Every read as it is, at a snapshot and at a timestamp: a handler may take another path for each.
        val reads =
            listOf(
                "/namespaces" to "/namespaces",
                "/namespaces/{namespace}" to "/namespaces/$ns",
                "/namespaces/{namespace}/tables" to "/namespaces/$ns/tables",
                table to "/namespaces/$ns/tables/t",
                "$table/scan" to "/namespaces/$ns/tables/t/scan",
                "$table/files" to "/namespaces/$ns/tables/t/files",
                "$table/files/{fileId}/stats" to "/namespaces/$ns/tables/t/files/$fileId/stats",
                "$table/partitions" to "/namespaces/$ns/tables/t/partitions",
                "$table/partitions/values" to "/namespaces/$ns/tables/t/partitions/values",
                "$table/changes" to "/namespaces/$ns/tables/t/changes?from_snapshot=0",
                views to "/namespaces/$ns/views",
                "$views/{view}" to "/namespaces/$ns/views/v",
            )
        for ((pattern, path) in reads) {
            val joiner = if ('?' in path) "&" else "?"
            for (variant in listOf("", "${joiner}snapshot=$head", "${joiner}at_timestamp=$now")) {
                send("GET", pattern, path + variant)
            }
        }

        // Path-named writes.
        val made = """{"name":"made","columns":$columns}"""
        send("POST", "/namespaces/{namespace}/tables", "/namespaces/$ns/tables", made)
        val alter = """{"ops":[{"op":"add_column","column":{"name":"w","type":"long"}}]}"""
        send("POST", "$table/alter", "/namespaces/$ns/tables/made/alter", alter)
        val truncate = "/namespaces/$ns/tables/made/truncate?expected_table_uuid=${tableUuid("made")}"
        send("POST", "$table/truncate", truncate, """{"message":"t"}""")
        send("DELETE", table, "/namespaces/$ns/tables/made?message=drop")
        send("POST", views, "/namespaces/$ns/views", """{"name":"v2","sql":"SELECT 1"}""")
        send("DELETE", "$views/{view}", "/namespaces/$ns/views/v2?message=drop")

        // The body-named writes. An upload is claimed for the commit whose key owns it.
        val uuid = tableUuid("t")
        val owner = UUID.randomUUID()
        val claim = """{"owner":"$owner","prefix":"$dataPath/data/$ns/t","file_kind":"data"}"""
        val uploaded = mapper.readTree(send("PUT", "/uploads/{upload}", "/uploads/$owner", claim).body())["path"]

        fun append(path: String? = null) =
            """{"namespace":"$ns","table":"t","expected_table_uuid":"$uuid","files":[""" +
                (path?.let { """{"path":"$it","record_count":1,"file_size_bytes":10}""" } ?: file()) + "]}"
        val delete = """{"namespace":"$ns","table":"t","expected_table_uuid":"$uuid","files":[]}"""

        fun guarded(
            body: String,
            key: UUID = UUID.randomUUID(),
        ) = """{$body,"read_snapshot":${catalogs.getCatalog(cat).headSnapshotId},"idempotency_key":"$key"}"""
        send("POST", "/commit/prepared", "/commit/prepared", guarded(""""appends":[${append()}]"""))
        send("POST", "/commit/deletes/prepared", "/commit/deletes/prepared", guarded(""""deletes":[$delete]"""))
        val mutation = """"appends":[${append()}],"deletes":[$delete]"""
        send("POST", "/commit/mutations/prepared", "/commit/mutations/prepared", guarded(mutation))
        send("POST", "/commit/transaction", "/commit/transaction", guarded(""""appends":[${append()}]"""))
        val ends = """"file_ends":[{"namespace":"$ns","table":"t","expected_table_uuid":"$uuid","all_files":true}]"""
        val replacement = guarded(""""message":"r","appends":[${append()}],$ends""")
        send("POST", "/commit/replacements/prepared", "/commit/replacements/prepared", replacement)
        val claimed = guarded(""""appends":[${append(uploaded.asText())}]""", owner)
        send("POST", "/commit/uploads", "/commit/uploads", claimed)
        send("POST", "/uploads/renew", "/uploads/renew", """{"owner":"$owner"}""")
        send("POST", "/uploads/abandon", "/uploads/abandon", """{"owner":"$owner","paths":[]}""")

        // Table creations, prepared on each endpoint, then read, published and aborted by id.
        val creation = "/table-creations/{operation}"
        val definitions =
            mapOf(
                "" to "",
                "/partitioned" to ""","partition_fields":[{"source_field_id":1,"transform":"identity"}]""",
                "/sorted" to ""","sort_fields":[{"source_field_id":1,"direction":"asc","null_order":"nulls_first"}]""",
                "/metadata" to ""","comment":"made here"""",
            )
        val prepared =
            definitions.map { (suffix, extra) ->
                val id = UUID.randomUUID()
                val name = "c${definitions.keys.indexOf(suffix)}"
                val body = """{"namespace":"$ns","name":"$name","columns":$columns$extra}"""
                send("PUT", "$creation$suffix", "/table-creations/$id$suffix", body)
                id
            }
        send("GET", creation, "/table-creations/${prepared[0]}")
        send("POST", "$creation/commit", "/table-creations/${prepared[0]}/commit", """{"files":[]}""")
        send("POST", "$creation/commit/uploads", "/table-creations/${prepared[1]}/commit/uploads", """{"files":[]}""")
        send("POST", "$creation/abort", "/table-creations/${prepared[2]}/abort")

        // A lookup in the partitioned table just published, as it is and at a snapshot (it takes no timestamp).
        val spec = mapper.readTree(send("GET", table, "/namespaces/$ns/tables/c1").body())["partition_spec"]["spec_id"]
        val lookup = "/namespaces/$ns/tables/c1/partitions/lookup?spec_id=$spec"
        send("POST", "$table/partitions/lookup", lookup, """["1"]""")
        val at = catalogs.getCatalog(cat).headSnapshotId
        send("POST", "$table/partitions/lookup", "$lookup&snapshot=$at", """["1"]""")
    }
}
