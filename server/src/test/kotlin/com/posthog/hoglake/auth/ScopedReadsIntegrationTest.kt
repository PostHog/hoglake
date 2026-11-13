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

/**
 * A scoped caller on the read routes, through the real application: the
 * namespace gate decides per namespace, the listing filters, and everything
 * else stays refused. Run under `warn` and `enforce`, which must agree: a
 * scoped token is enforced in both.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ScopedReadsIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val pair = TestTokens.keyPair()
    private val keyFile =
        Files.createTempFile("hoglake-scoped-reads-", ".json").also {
            Files.writeString(it, TestTokens.jwks(pair))
        }
    private val json = wireObjectMapper()
    private val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
    private val cat = "scoped"
    private val columns = listOf(ColumnDef("v", ColType.LONG))

    init {
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        for ((ns, value) in listOf("team_42" to 42L, "imports_42" to 42L, "team_7" to 7L, "team_9" to 9L)) {
            catalogs.createNamespace(cat, ns)
            catalogs.setNamespaceContainment(cat, ns, NamespaceContainment("team_id", value))
        }
        catalogs.createNamespace(cat, "main")
        for (ns in listOf("team_42", "imports_42", "team_9", "main")) catalogs.createTable(cat, ns, "t", columns)
        com.posthog.hoglake.service.ViewService(db.jdbi).create(cat, "team_42", "v", "SELECT 1")
    }

    @AfterAll
    fun close() {
        http.close()
        db.close()
        Files.deleteIfExists(keyFile)
    }

    private fun scoped(
        prd: String = """{"team_id":{"42":"write","7":"read"}}""",
        catalogScope: String = "write",
    ): String {
        val iat = System.currentTimeMillis() / 1000 - 5
        return TestTokens.mint(
            pair,
            TestTokens.scopedClaims(prd = prd, catalogs = """{"$cat":["$catalogScope"]}""", iat = iat),
        )
    }

    private fun tenant() = TestTokens.mint(pair, TestTokens.tenantClaims(catalogs = mapOf(cat to listOf("read"))))

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
        path: String,
        method: String = "GET",
    ): HttpResponse<String> =
        http.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port/v1/catalogs/$cat$path"))
                .header("Authorization", "Bearer $token")
                .method(method, HttpRequest.BodyPublishers.noBody())
                .build(),
            HttpResponse.BodyHandlers.ofString(),
        )

    private fun names(response: HttpResponse<String>): List<String> {
        assertThat(response.statusCode()).describedAs(response.body()).isEqualTo(200)
        return json.readTree(response.body()).map { it.path("name").asText() }.sorted()
    }

    @Test
    fun `a scoped caller reads its own contained namespaces and nothing else`() {
        for (mode in listOf(AuthMode.WARN, AuthMode.ENFORCE)) {
            withServer(mode) { port ->
                val token = scoped()
                // The listing holds only what it may use: team_9 is another
                // team's, main is not contained.
                assertThat(names(call(port, token, "/namespaces"))).describedAs("%s", mode)
                    .containsExactly("imports_42", "team_42", "team_7")
                // Its own namespace's reads, read-only value included.
                for (path in listOf(
                    "/namespaces/team_42",
                    "/namespaces/team_42/tables",
                    "/namespaces/team_42/tables/t",
                    "/namespaces/team_42/tables/t/files",
                    "/namespaces/team_42/views",
                    "/namespaces/team_42/views/v",
                    "/namespaces/team_7",
                    "/namespaces/team_7/tables",
                )) {
                    assertThat(call(port, token, path).statusCode()).describedAs("%s %s", mode, path).isEqualTo(200)
                }
                // Another team's, an uncontained one, and a missing one all
                // answer the same 404 a missing namespace does.
                val missing = call(port, token, "/namespaces/nope")
                assertThat(missing.statusCode()).isEqualTo(404)
                val missingCode = json.readTree(missing.body()).path("error").asText()
                for (path in listOf(
                    "/namespaces/team_9",
                    "/namespaces/team_9/tables/t",
                    "/namespaces/main",
                    "/namespaces/main/tables/t/files",
                )) {
                    val r = call(port, token, path)
                    assertThat(r.statusCode()).describedAs("%s %s", mode, path).isEqualTo(404)
                    assertThat(json.readTree(r.body()).path("error").asText()).isEqualTo(missingCode)
                    assertThat(r.body()).doesNotContain("team_id").doesNotContain("scoped credential")
                }
                // Catalog-wide reads and namespace DDL stay refused. Writes
                // are ScopedWritesIntegrationTest's.
                assertThat(call(port, token, "").statusCode()).isEqualTo(403)
                assertThat(call(port, token, "/snapshots").statusCode()).isEqualTo(403)
                assertThat(call(port, token, "/namespaces/team_42", "DELETE").statusCode()).isEqualTo(403)
                // A read-only value's table is not writable.
                assertThat(call(port, token, "/namespaces/team_7/tables/x", "DELETE").statusCode())
                    .isEqualTo(404)
            }
        }
    }

    @Test
    fun `several namespaces contained for one value are all visible to that value and to nobody else`() {
        withServer(AuthMode.ENFORCE) { port ->
            val team42 = scoped(prd = """{"team_id":{"42":"read"}}""")
            assertThat(names(call(port, team42, "/namespaces"))).containsExactly("imports_42", "team_42")
            for (ns in listOf("team_42", "imports_42")) {
                for (path in listOf("", "/tables", "/tables/t", "/tables/t/files")) {
                    val r = call(port, team42, "/namespaces/$ns$path")
                    assertThat(r.statusCode()).describedAs("%s%s", ns, path).isEqualTo(200)
                }
            }
            val team7 = scoped(prd = """{"team_id":{"7":"read"}}""")
            assertThat(names(call(port, team7, "/namespaces"))).containsExactly("team_7")
            for (ns in listOf("team_42", "imports_42")) {
                assertThat(call(port, team7, "/namespaces/$ns/tables/t").statusCode()).describedAs(ns).isEqualTo(404)
            }
        }
    }

    @Test
    fun `every value, a read-only catalog, and a tenant beside it`() {
        withServer(AuthMode.ENFORCE) { port ->
            // `*`: every contained namespace for the field, still not main.
            val all = scoped(prd = """{"team_id":{"*":"read"}}""")
            assertThat(
                names(call(port, all, "/namespaces")),
            ).containsExactly("imports_42", "team_42", "team_7", "team_9")
            assertThat(call(port, all, "/namespaces/team_9/tables/t").statusCode()).isEqualTo(200)
            assertThat(call(port, all, "/namespaces/main").statusCode()).isEqualTo(404)
            // `cat: read` is enough for reads.
            val reader = scoped(catalogScope = "read")
            assertThat(call(port, reader, "/namespaces/team_42/tables/t").statusCode()).isEqualTo(200)
            // A tenant sees the whole catalog, as before.
            assertThat(names(call(port, tenant(), "/namespaces")))
                .contains("main", "team_42", "team_7", "team_9")
            assertThat(call(port, tenant(), "/namespaces/main/tables/t").statusCode()).isEqualTo(200)
        }
    }

    @Test
    fun `a namespace stops answering the moment containment is cleared`() {
        catalogs.createNamespace(cat, "team_11")
        catalogs.setNamespaceContainment(cat, "team_11", NamespaceContainment("team_id", 11))
        withServer(AuthMode.ENFORCE) { port ->
            val token = scoped(prd = """{"team_id":{"11":"read"}}""")
            assertThat(call(port, token, "/namespaces/team_11").statusCode()).isEqualTo(200)
            catalogs.setNamespaceContainment(cat, "team_11", null)
            assertThat(call(port, token, "/namespaces/team_11").statusCode()).isEqualTo(404)
            assertThat(names(call(port, token, "/namespaces"))).isEmpty()
        }
    }
}
