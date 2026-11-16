package com.posthog.hoglake.api

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.TestTokens
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
 * Route values come from the path only, through the real application over
 * PostgreSQL. Two catalogs, `alpha` and `beta`, each hold a namespace `ns`
 * with one table named after its catalog, and `alpha` holds two contained
 * namespaces. A request for `alpha` that also carries `?catalog=beta`, or
 * for one namespace that also carries `?namespace=` another, is served the
 * catalog and namespace of its path.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RoutePathValuesIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val pair = TestTokens.keyPair()
    private val keyFile =
        Files.createTempFile("hoglake-route-values-", ".json").also { Files.writeString(it, TestTokens.jwks(pair)) }
    private val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
    private val mapper = wireObjectMapper()
    private val columns = listOf(ColumnDef("v", ColType.LONG))

    init {
        for (catalog in listOf("alpha", "beta")) {
            catalogs.createCatalog(catalog, "s3://bucket/$catalog")
            catalogs.createNamespace(catalog, "ns")
            catalogs.createTable(catalog, "ns", "only_$catalog", columns)
        }
        for ((namespace, team) in listOf("team_7" to 7L, "team_9" to 9L)) {
            catalogs.createNamespace("alpha", namespace)
            catalogs.setNamespaceContainment("alpha", namespace, NamespaceContainment("team_id", team))
            catalogs.createTable("alpha", namespace, "of_$namespace", columns)
        }
    }

    @AfterAll
    fun close() {
        http.close()
        db.close()
        Files.deleteIfExists(keyFile)
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

    /** A tenant token for `alpha` alone. */
    private fun alpha() = TestTokens.mint(pair, TestTokens.tenantClaims(catalogs = mapOf("alpha" to listOf("admin"))))

    /** A scoped token for team 7 of `alpha` alone. */
    private fun team7(): String {
        val iat = System.currentTimeMillis() / 1000 - 5
        val claims =
            TestTokens.scopedClaims(
                prd = """{"team_id":{"7":"write"}}""",
                catalogs = """{"alpha":["write"]}""",
                iat = iat,
            )
        return TestTokens.mint(pair, claims)
    }

    private fun operator() = TestTokens.mint(pair, TestTokens.operatorClaims())

    private fun send(
        port: Int,
        method: String,
        path: String,
        token: String,
        body: String? = null,
    ): HttpResponse<String> =
        http.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port$path"))
                .header("Authorization", "Bearer $token")
                .header("Content-Type", "application/json")
                .method(
                    method,
                    body?.let { HttpRequest.BodyPublishers.ofString(it) } ?: HttpRequest.BodyPublishers.noBody(),
                )
                .build(),
            HttpResponse.BodyHandlers.ofString(),
        )

    private fun names(response: HttpResponse<String>): List<String> {
        assertThat(response.statusCode()).describedAs(response.body()).isEqualTo(200)
        return mapper.readTree(response.body()).map { it["name"].asText() }
    }

    @Test
    fun `a catalog query parameter does not change the catalog a read is served from`() {
        withServer { port ->
            val token = alpha()
            // The token's own catalog, and the other one refused by name: the baseline.
            assertThat(names(send(port, "GET", "/v1/catalogs/alpha/namespaces/ns/tables", token)))
                .containsExactly("only_alpha")
            assertThat(send(port, "GET", "/v1/catalogs/beta/namespaces/ns/tables", token).statusCode()).isEqualTo(404)

            assertThat(names(send(port, "GET", "/v1/catalogs/alpha/namespaces/ns/tables?catalog=beta", token)))
                .containsExactly("only_alpha")
            val catalog = send(port, "GET", "/v1/catalogs/alpha?catalog=beta", token)
            assertThat(mapper.readTree(catalog.body())["name"].asText()).isEqualTo("alpha")
            val table = send(port, "GET", "/v1/catalogs/alpha/namespaces/ns/tables/only_beta?catalog=beta", token)
            assertThat(table.statusCode()).describedAs(table.body()).isEqualTo(404)
            val scan = send(port, "GET", "/v1/catalogs/alpha/namespaces/ns/tables/only_beta/scan?catalog=beta", token)
            assertThat(scan.statusCode()).describedAs(scan.body()).isEqualTo(404)
        }
    }

    @Test
    fun `a catalog query parameter does not change the catalog a write lands in`() {
        withServer { port ->
            val before = catalogs.getCatalog("beta").headSnapshotId
            val made = send(port, "POST", "/v1/catalogs/alpha/namespaces?catalog=beta", alpha(), """{"name":"made"}""")
            assertThat(made.statusCode()).describedAs(made.body()).isEqualTo(201)
            assertThat(catalogs.listNamespaces("alpha").map { it.name }).contains("made")
            assertThat(catalogs.listNamespaces("beta").map { it.name }).containsExactly("ns")
            val drop = "/v1/catalogs/alpha/namespaces/ns/tables/only_beta?catalog=beta&message=drop"
            val dropped = send(port, "DELETE", drop, alpha())
            assertThat(dropped.statusCode()).describedAs(dropped.body()).isEqualTo(404)
            assertThat(catalogs.listTables("beta", "ns").map { it.name }).containsExactly("only_beta")
            assertThat(catalogs.getCatalog("beta").headSnapshotId).isEqualTo(before)
        }
    }

    @Test
    fun `a namespace or table query parameter does not change the namespace or table of the path`() {
        withServer { port ->
            val token = team7()
            val own = "/v1/catalogs/alpha/namespaces/team_7"
            assertThat(names(send(port, "GET", "$own/tables", token))).containsExactly("of_team_7")
            assertThat(send(port, "GET", "/v1/catalogs/alpha/namespaces/team_9/tables", token).statusCode())
                .isEqualTo(404)

            assertThat(names(send(port, "GET", "$own/tables?namespace=team_9", token))).containsExactly("of_team_7")
            val namespace = send(port, "GET", "$own?namespace=team_9", token)
            assertThat(mapper.readTree(namespace.body())["name"].asText()).isEqualTo("team_7")
            val table = send(port, "GET", "$own/tables/of_team_7?namespace=team_9&table=of_team_9", token)
            assertThat(mapper.readTree(table.body())["name"].asText()).isEqualTo("of_team_7")
            val create = """{"name":"made_here","columns":[{"name":"v","type":"long"}]}"""
            val made = send(port, "POST", "$own/tables?namespace=team_9", token, create)
            assertThat(made.statusCode()).describedAs(made.body()).isEqualTo(201)
            assertThat(catalogs.listTables("alpha", "team_9").map { it.name }).containsExactly("of_team_9")
            assertThat(catalogs.listTables("alpha", "team_7").map { it.name }).contains("made_here")
        }
    }

    @Test
    fun `a mount query parameter does not change the mount of the path`() {
        withServer { port ->
            val asked = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f0-events"
            val other = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f1-events"
            val mount = send(port, "GET", "/v1/mounts/$asked?mount=$other", operator())
            assertThat(mount.statusCode()).describedAs(mount.body()).isEqualTo(200)
            assertThat(mapper.readTree(mount.body())["name"].asText()).isEqualTo(asked)
        }
    }
}
