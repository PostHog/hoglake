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
import io.ktor.server.engine.connector
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
 * A dedicated read session on the Host (docs/auth.md, "Read sessions on a
 * remote schema"): the real application over PostgreSQL, serving the `team`
 * family from `imports`, with the read session profile on and no Hoglite
 * backend. Both listeners are started; the public one is where a Client
 * relays to. The callers' catalog, `cust`, does not exist on this server.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ServedReadSessionIntegrationTest : ServedReadSessionCases("enforce")

/** The same Host with a warning API listener: only its public listener, which always enforces, admits a read session. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ServedReadSessionWarnIntegrationTest : ServedReadSessionCases("warn") {
    @Test
    fun `the warning API listener does not admit a read session`() {
        for (path in listOf(ns, "$ns/tables", "$ns/tables/events", scanPath())) {
            val response = send(apiPort, path, session())
            assertThat(response.statusCode()).describedAs("%s: %s", path, response.body()).isEqualTo(404)
            assertThat(response.body()).doesNotContain("a.parquet", "team_id=42")
        }
    }
}

abstract class ServedReadSessionCases(authMode: String) {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val pair = TestTokens.keyPair()
    private val keyFile =
        Files.createTempFile("hoglake-served-read-", ".json").also { Files.writeString(it, TestTokens.jwks(pair)) }
    private val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
    private val mapper = wireObjectMapper()
    private val orgA = "01a1e5de-1ac5-0000-e53e-d2f9a77deae4"
    private val orgB = "02b2f6ef-2bd6-0000-f64f-e3a0b88efbf5"
    private val source = "imports"
    private val cust = "cust"

    // The customer's own Hoglite backend: this server has no backends file.
    private val hogliteId = "5b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e"
    protected val ns = "/v1/catalogs/$cust/namespaces/team_42"
    private val importsPrefix = "$source/org_id=$orgA/team_id=42/"
    private val filePath = "s3://bucket/${importsPrefix}events/a.parquet"
    private val api: io.ktor.server.engine.EmbeddedServer<*, *>
    private val public: io.ktor.server.engine.EmbeddedServer<*, *>
    private val app: App
    protected val apiPort: Int
    private val publicPort: Int

    /** The listeners that admit a read session. */
    private val ports: List<Int>

    init {
        catalogs.createCatalog(source, "s3://bucket/$source")
        for (name in listOf("team_42", "team_43", "team_46")) catalogs.createNamespace(source, name)
        catalogs.setNamespaceContainment(source, "team_42", NamespaceContainment("team_id", 42, orgA))
        catalogs.setNamespaceContainment(source, "team_43", NamespaceContainment("team_id", 43, orgA))
        catalogs.setNamespaceContainment(source, "team_46", NamespaceContainment("team_id", 46, orgB))
        for (name in listOf("team_42", "team_43", "team_46")) {
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
            Files.createTempFile("hoglake-served-read-ns-", ".json").also {
                it.toFile().deleteOnExit()
                Files.writeString(
                    it,
                    """{"families":[{"family":"team","source_catalog":"$source","predicate_field":"team_id"}]}""",
                )
            }
        val cfg =
            Config(port = 0, hydratorIntervalMs = 0).copy(
                authMode = authMode,
                publicPort = 8443,
                authIssuer = TestTokens.ISSUER,
                authJwksFile = keyFile.toString(),
                authPredicateFields = "team_id",
                authSessionProfile = "on",
                servedNamespacesFile = served.toString(),
                readSessionProfile = "example.aws-session",
                readSessionRemoteTargets = "remote:events,remote:imports",
            )
        app = App.build(cfg, db.jdbi)
        api = embeddedServer(Netty, configure = { configureApiListener(cfg) }) { app.module(this) }
        api.start(wait = false)
        apiPort = runBlocking { api.engine.resolvedConnectors().first().port }
        public =
            embeddedServer(Netty, configure = { connector { port = 0 } }) {
                app.module(this, app.auth.forPublicListener())
            }
        public.start(wait = false)
        publicPort = runBlocking { public.engine.resolvedConnectors().first().port }
        ports = if (authMode == "enforce") listOf(apiPort, publicPort) else listOf(publicPort)
    }

    @AfterAll
    fun close() {
        public.stop(0, 1000)
        api.stop(0, 1000)
        app.close()
        http.close()
        db.close()
        Files.deleteIfExists(keyFile)
    }

    /** A dedicated read session as the issuer shapes it, valid now. The customer's backend is unknown here. */
    protected fun session(
        org: String = orgA,
        team: Int = 42,
        catalog: String = cust,
        imports: String = importsPrefix,
        audiences: String = """["trino-session","hoglake-session","hoglite-session"]""",
    ): String {
        val iat = System.currentTimeMillis() / 1000 - 5
        val exp = iat + 120
        val grant = """"catalog":"$catalog","region":"us-east-1","actions":["s3:GetObject"]"""
        return TestTokens.mint(
            pair,
            """{"iss":"${TestTokens.ISSUER}","aud":$audiences,"sub":"alice","usr":"alice","jti":"jti-r",""" +
                """"profile":"example.aws-session","version":1,"purpose":"user-session","ten":"scoped",""" +
                """"org":"$org","primary_catalog":"$catalog","sid":"sid-1","generation":1,""" +
                """"session_deadline":${exp + 60},"iat":$iat,"nbf":$iat,"exp":$exp,""" +
                """"cat":{"$catalog":["read"]},"prd":{"team_id":{"$team":"read"}},""" +
                """"rtg":{"b":"hoglite","be":"$hogliteId","t":"fixture_3"},"remote_schemas":{"team":["read"]},""" +
                """"aws":{"access_key_id":"FICTIONAL_KEY","secret_access_key":"FICTIONAL_SECRET",""" +
                """"session_token":"FICTIONAL_TOKEN","region":"us-east-1","expires_at":$exp},"grants":[""" +
                """{"target":"remote:imports","bucket":"bucket","prefix":"$imports",$grant},""" +
                """{"target":"remote:events","bucket":"bucket","prefix":"stream/team_id=$team/",$grant},""" +
                """{"target":"$catalog","bucket":"bucket-cust","prefix":"tables/",$grant},""" +
                """{"target":"$hogliteId","bucket":"bucket-owner",""" +
                """"object_key":"root/2c/t/fixture_3/manifest",$grant}""" +
                "]}",
        )
    }

    protected fun send(
        port: Int,
        path: String,
        token: String,
        method: String = "GET",
    ): HttpResponse<String> =
        http.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port$path"))
                .header("Authorization", "Bearer $token")
                .method(method, HttpRequest.BodyPublishers.noBody())
                .build(),
            HttpResponse.BodyHandlers.ofString(),
        )

    private fun head(): Long = catalogs.getCatalog(source).headSnapshotId

    private fun tableUuid(): String = catalogs.getTable(source, "team_42", "events").tableUuid.toString()

    protected fun scanPath(
        snapshot: Long = head(),
        uuid: String = tableUuid(),
    ) = "$ns/tables/events/scan?snapshot=$snapshot&table_uuid=$uuid"

    private fun assertMissing(
        response: HttpResponse<String>,
        what: String,
    ) {
        assertThat(response.statusCode()).describedAs("%s: %s", what, response.body()).isEqualTo(404)
        assertThat(response.body()).describedAs(what).doesNotContain("a.parquet", orgA, source)
    }

    @Test
    fun `both listeners serve the four routes to the session's own team, without totals`() {
        for (port in ports) {
            val namespace = send(port, ns, session())
            assertThat(namespace.statusCode()).describedAs(namespace.body()).isEqualTo(200)
            assertThat(mapper.readTree(namespace.body())["name"].asText()).isEqualTo("team_42")

            val tables = send(port, "$ns/tables", session())
            assertThat(tables.statusCode()).describedAs(tables.body()).isEqualTo(200)
            val listed = mapper.readTree(tables.body())
            assertThat(listed.map { it["name"].asText() }).containsExactly("events")
            assertThat(listed[0]["table_uuid"].asText()).isEqualTo(tableUuid())
            assertThat(listed[0].fieldNames().asSequence().toList())
                .doesNotContain("record_count", "file_count", "file_size_bytes", "snapshot_count")
            assertThat(tables.headers().firstValue("Cache-Control")).hasValue("no-store")

            for (query in listOf("", "?totals=false")) {
                val table = send(port, "$ns/tables/events$query", session())
                assertThat(table.statusCode()).describedAs(table.body()).isEqualTo(200)
                val described = mapper.readTree(table.body())
                assertThat(described["table_uuid"].asText()).isEqualTo(tableUuid())
                assertThat(described["remote"].asBoolean()).isTrue()
                assertThat(described.fieldNames().asSequence().toList())
                    .doesNotContain(
                        "record_count",
                        "file_count",
                        "file_size_bytes",
                        "totals_snapshot_id",
                        "totals_as_of",
                    )
                assertThat(table.headers().firstValue("Cache-Control")).hasValue("no-store")
            }

            val scan = send(port, scanPath(), session())
            assertThat(scan.statusCode()).describedAs(scan.body()).isEqualTo(200)
            val files = mapper.readTree(scan.body())
            assertThat(files.map { it["data_file"]["path"].asText() }).containsExactly(filePath)
            assertThat(scan.headers().firstValue("Cache-Control")).hasValue("no-store")
        }
    }

    @Test
    fun `the scan needs the pinned snapshot and the table's own uuid`() {
        for (port in ports) {
            for (bad in listOf(
                "$ns/tables/events/scan",
                "$ns/tables/events/scan?snapshot=${head()}",
                "$ns/tables/events/scan?table_uuid=${tableUuid()}",
                "${scanPath()}&include=column_stats",
                "$ns/tables/events/scan?snapshot=0${head()}&table_uuid=${tableUuid()}",
            )) {
                assertThat(send(port, bad, session()).statusCode()).describedAs(bad).isEqualTo(400)
            }
            // Another table's UUID, of the same organization.
            val other = catalogs.getTable(source, "team_43", "events").tableUuid.toString()
            assertMissing(send(port, scanPath(uuid = other), session()), "another table's uuid")
            assertMissing(
                send(port, scanPath(uuid = "00000000-0000-4000-8000-000000000000"), session()),
                "an unknown uuid",
            )
            assertThat(send(port, "$ns/tables/events?totals=true", session()).statusCode()).isEqualTo(400)
            assertThat(send(port, "$ns/tables/events?snapshot=${head()}", session()).statusCode()).isEqualTo(400)
        }
    }

    @Test
    fun `a file outside the two remote grants refuses the whole scan without its path`() {
        for (port in ports) {
            for (imports in listOf("$source/org_id=$orgA/team_id=420/", "$source/org_id=$orgB/team_id=42/")) {
                val scan = send(port, scanPath(), session(imports = imports))
                assertThat(scan.statusCode()).describedAs(scan.body()).isEqualTo(403)
                assertThat(scan.body()).doesNotContain("a.parquet", "bucket", source, orgA)
            }
        }
    }

    @Test
    fun `the served caller rule is unchanged for a read session`() {
        for (port in ports) {
            for (suffix in listOf(
                "",
                "/tables",
                "/tables/events",
                "/tables/events/scan?snapshot=1&table_uuid=${tableUuid()}",
            )) {
                // Another team of the same organization, and a team of another organization.
                assertMissing(send(port, "/v1/catalogs/$cust/namespaces/team_43$suffix", session()), "team_43$suffix")
                assertMissing(send(port, "/v1/catalogs/$cust/namespaces/team_46$suffix", session()), "team_46$suffix")
                // The mark's organization is not the caller's, whatever team the caller holds.
                assertMissing(send(port, "$ns$suffix", session(org = orgB)), "another org$suffix")
                assertMissing(send(port, "$ns$suffix", session(team = 43)), "another prd$suffix")
                // The session names another catalog than the path.
                assertMissing(send(port, "$ns$suffix", session(catalog = "other")), "another catalog$suffix")
                // The source catalog itself is never a read session's.
                assertMissing(
                    send(port, "/v1/catalogs/$source/namespaces/team_42$suffix", session(catalog = source)),
                    "the source catalog$suffix",
                )
            }
        }
    }

    @Test
    fun `no other route selects the read verifier`() {
        for (port in ports) {
            for ((method, path) in listOf(
                "GET" to "/v1/catalogs/$cust/namespaces",
                "GET" to "/v1/catalogs/$cust",
                "GET" to "$ns/tables/events/files",
                "DELETE" to "$ns/tables/events",
                "POST" to "$ns/tables",
                "GET" to "/v1/catalogs/$cust/namespaces/team_042",
                "GET" to "/v1/catalogs/$cust/namespaces/TEAM_42",
            )) {
                val response = send(port, path, session(), method)
                assertThat(response.statusCode()).describedAs("%s %s: %s", method, path, response.body()).isIn(401, 404)
                assertThat(response.body()).doesNotContain("a.parquet")
            }
            // A token that mixes the ordinary audience in is no read session and no ordinary token.
            val mixed = session(audiences = """["trino-session","hoglake-session","hoglite-session","hoglake"]""")
            assertThat(send(port, ns, mixed).statusCode()).isEqualTo(401)
        }
        // The public listener has no local-only route, with or without the profile.
        val localOnly = send(publicPort, "/v1/local-only/catalogs/$cust/namespaces/team_42", session())
        assertThat(localOnly.statusCode()).describedAs(localOnly.body()).isEqualTo(501)
    }
}
