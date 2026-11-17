package com.posthog.hoglake.store

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.configureApiListener
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
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
 * Served namespaces (docs/auth-tenancy.md §6.5d) under `warn` auth, and the token tolerance that only a server
 * that serves namespaces has.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ServedNamespacesModesIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val pair = TestTokens.keyPair()
    private val keyFile =
        Files.createTempFile("hoglake-served-modes-", ".json").also { Files.writeString(it, TestTokens.jwks(pair)) }
    private val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
    private val orgA = "01a1e5de-1ac5-0000-e53e-d2f9a77deae4"
    private val ns = "/v1/catalogs/cust/namespaces/team_42"
    private val paths = listOf(ns, "$ns/tables", "$ns/tables/events", "$ns/tables/events/scan")
    private val unknownBackend = """{"b":"hoglite","be":"5b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e","t":"opaque_1"}"""

    init {
        val catalogs = CatalogService(db.jdbi)
        catalogs.createCatalog("imports", "s3://bucket/imports")
        catalogs.createNamespace("imports", "team_42")
        catalogs.setNamespaceContainment("imports", "team_42", NamespaceContainment("team_id", 42, orgA))
        catalogs.createTable("imports", "team_42", "events", listOf(ColumnDef("v", ColType.LONG)))
    }

    @AfterAll
    fun close() {
        http.close()
        db.close()
        Files.deleteIfExists(keyFile)
    }

    private fun <T> withServer(
        serve: Boolean,
        authMode: String,
        block: (port: Int) -> T,
    ): T {
        val served =
            Files.createTempFile("hoglake-served-modes-ns-", ".json").also {
                it.toFile().deleteOnExit()
                Files.writeString(it, """{"namespaces":[{"name":"team_42","source_catalog":"imports"}]}""")
            }
        val cfg =
            Config(port = 0, hydratorIntervalMs = 0).copy(
                authMode = authMode,
                authIssuer = TestTokens.ISSUER,
                authJwksFile = keyFile.toString(),
                authPredicateFields = "team_id",
                authSessionProfile = "on",
                servedNamespacesFile = if (serve) served.toString() else "",
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

    private fun scoped(rtg: String? = null): String {
        val claims =
            TestTokens.scopedClaims(
                prd = """{"team_id":{"42":"read"}}""",
                catalogs = """{"cust":["read"]}""",
                iat = System.currentTimeMillis() / 1000 - 5,
                extra =
                    TestTokens.sessionProfile("cust", """{"team_42":["read"]}""") +
                        (rtg?.let { ""","rtg":$it""" } ?: ""),
            ).replace("\"org\":\"11111111-1111-1111-1111-111111111111\"", "\"org\":\"$orgA\"")
        return TestTokens.mint(pair, claims)
    }

    private fun tenant() =
        TestTokens.mint(
            pair,
            TestTokens.tenantClaims(org = orgA, catalogs = mapOf("cust" to listOf("admin"))).dropLast(1) +
                TestTokens.sessionProfile("cust", """{"team_42":["read"]}""") + "}",
        )

    private fun get(
        port: Int,
        path: String,
        token: String?,
    ): HttpResponse<String> =
        http.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port$path")).apply {
                if (token != null) header("Authorization", "Bearer $token")
            }.GET().build(),
            HttpResponse.BodyHandlers.ofString(),
        )

    @Test
    fun `under warn a caller without a token and a tenant are never served, and a scoped reader is`() {
        withServer(serve = true, authMode = "warn") { port ->
            for (path in paths) {
                for (token in listOf(null, tenant())) {
                    val response = get(port, path, token)
                    assertThat(response.statusCode()).describedAs("%s %s", path, token != null).isIn(401, 404)
                    assertThat(response.body()).doesNotContain("events\"", "a.parquet", orgA, "imports")
                }
                assertThat(get(port, path, scoped()).statusCode()).describedAs(path).isEqualTo(200)
            }
        }
    }

    @Test
    fun `an rtg naming an unlisted backend is ignored on a server that serves namespaces and refused on any other`() {
        val token = scoped(unknownBackend)
        withServer(serve = true, authMode = "enforce") { port ->
            for (path in paths) assertThat(get(port, path, token).statusCode()).describedAs(path).isEqualTo(200)
        }
        // Only a scoped token's placement is ignored.
        val tenantWithBackend =
            TestTokens.tenantClaims(org = orgA, catalogs = mapOf("cust" to listOf("admin"))).dropLast(1) +
                ""","rtg":$unknownBackend}"""
        withServer(serve = true, authMode = "enforce") { port ->
            val response = get(port, ns, TestTokens.mint(pair, tenantWithBackend))
            assertThat(response.statusCode()).describedAs(response.body()).isEqualTo(401)
            assertThat(response.body()).contains("unknown_backend")
        }
        withServer(serve = false, authMode = "enforce") { port ->
            val response = get(port, ns, token)
            assertThat(response.statusCode()).describedAs(response.body()).isEqualTo(401)
            assertThat(response.body()).contains("unknown_backend")
        }
    }
}
