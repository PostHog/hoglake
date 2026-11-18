package com.posthog.hoglake.api

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.configureApiListener
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files

/**
 * `HOGLAKE_RESERVED_NAMESPACES` through the real application on PostgreSQL:
 * Config -> App -> the namespace route -> CatalogService. A refusal writes
 * nothing (no snapshot, no namespace row, no change row).
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReservedNamespaceIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val pair = TestTokens.keyPair()
    private val keyFile =
        Files.createTempFile("hoglake-reserved-ns-", ".json").also { Files.writeString(it, TestTokens.jwks(pair)) }
    private val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
    private var serial = 0
    private var cat = ""

    /** A catalog per test: the database is shared by the class. */
    @BeforeEach
    fun freshCatalog() {
        cat = "reservedns-${++serial}"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
    }

    @AfterAll
    fun close() {
        http.close()
        db.close()
        Files.deleteIfExists(keyFile)
    }

    private fun <T> withServer(
        reserved: Set<String>,
        mode: AuthMode = AuthMode.ENFORCE,
        block: (port: Int) -> T,
    ): T {
        val cfg =
            Config(port = 0, hydratorIntervalMs = 0).copy(
                authMode = mode.wire,
                authIssuer = TestTokens.ISSUER,
                authJwksFile = keyFile.toString(),
                authPredicateFields = "team_id",
                reservedNamespaceNames = reserved,
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

    private fun operator() = TestTokens.mint(pair, TestTokens.operatorClaims())

    private fun tenant() = TestTokens.mint(pair, TestTokens.tenantClaims(catalogs = mapOf(cat to listOf("admin"))))

    private fun scoped(): String {
        val iat = System.currentTimeMillis() / 1000 - 5
        return TestTokens.mint(pair, TestTokens.scopedClaims(catalogs = """{"$cat":["write"]}""", iat = iat))
    }

    private fun send(
        port: Int,
        token: String?,
        method: String,
        path: String,
        body: String? = null,
    ): HttpResponse<String> =
        http.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port/v1/catalogs/$cat$path"))
                .apply { token?.let { header("Authorization", "Bearer $it") } }
                .header("Content-Type", "application/json")
                .method(
                    method,
                    body?.let { HttpRequest.BodyPublishers.ofString(it) } ?: HttpRequest.BodyPublishers.noBody(),
                )
                .build(),
            HttpResponse.BodyHandlers.ofString(),
        )

    private fun create(
        port: Int,
        token: String?,
        name: String,
    ) = send(port, token, "POST", "/namespaces", """{"name":"$name"}""")

    /** Every row a namespace create writes, catalog-wide. */
    private fun footprint(): List<Long> =
        db.jdbi.withHandleUnchecked { h ->
            listOf("hog_snapshot", "hog_namespace", "hog_snapshot_change").map {
                h.createQuery("SELECT count(*) FROM $it").mapTo(Long::class.java).one()
            } + catalogs.getCatalog(cat).headSnapshotId
        }

    private fun expect(
        response: HttpResponse<String>,
        status: Int,
    ) = assertThat(response.statusCode()).describedAs(response.body()).isEqualTo(status)

    private fun assertReserved(
        response: HttpResponse<String>,
        what: String,
    ) {
        assertThat(response.statusCode()).describedAs("%s: %s", what, response.body()).isEqualTo(422)
        assertThat(response.body()).describedAs(what).contains("\"error\":\"validation\"").contains("is reserved")
    }

    @Test
    fun `a reserved name is refused for every caller kind and case, with no write`() {
        withServer(setOf("posthog")) { port ->
            val before = footprint()
            for ((who, token) in listOf("operator" to operator(), "tenant" to tenant())) {
                for (name in listOf("posthog", "PostHog", "POSTHOG", "posTHOG")) {
                    assertReserved(create(port, token, name), "$who $name")
                }
            }
            // A scoped token cannot create namespaces at all: refused before the handler.
            assertThat(create(port, scoped(), "posthog").statusCode()).isEqualTo(403)
            assertThat(footprint()).isEqualTo(before)
            assertThat(catalogs.listNamespaces(cat).map { it.name.lowercase() }).doesNotContain("posthog")
            // Other names are unaffected, for the same callers.
            for (name in listOf("posthog_raw", "posthog-events", "my_posthog", "posthog2", "_posthog")) {
                assertThat(create(port, operator(), name).statusCode()).describedAs(name).isEqualTo(201)
            }
        }
        withServer(setOf("posthog"), AuthMode.OFF) { port ->
            val before = footprint()
            assertReserved(create(port, null, "posthog"), "auth off")
            assertThat(footprint()).isEqualTo(before)
        }
    }

    @Test
    fun `unset reserves nothing`() {
        withServer(emptySet()) { port ->
            assertThat(create(port, tenant(), "Posthog").statusCode()).isEqualTo(201)
            expect(send(port, operator(), "DELETE", "/namespaces/Posthog?message=drop"), 200)
        }
    }

    @Test
    fun `a namespace that existed before its name was reserved keeps working but cannot come back`() {
        catalogs.createNamespace(cat, "early")
        catalogs.createTable(cat, "early", "t", listOf(ColumnDef("id", ColType.LONG)))
        withServer(setOf("early", "other")) { port ->
            val token = tenant()
            assertThat(send(port, token, "GET", "/namespaces").body()).contains("\"name\":\"early\"")
            expect(send(port, token, "GET", "/namespaces/early"), 200)
            expect(send(port, token, "GET", "/namespaces/early/tables/t"), 200)
            expect(send(port, token, "DELETE", "/namespaces/early/tables/t?message=drop"), 200)
            expect(send(port, token, "DELETE", "/namespaces/early?message=drop"), 200)
            for (name in listOf("early", "EARLY")) assertReserved(create(port, token, name), name)
        }
    }
}
