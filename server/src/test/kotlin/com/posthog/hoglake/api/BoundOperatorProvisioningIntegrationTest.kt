package com.posthog.hoglake.api

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.configureApiListener
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
 * `POST /v1/catalogs` on PostgreSQL for a catalog-bound operator (#667): what
 * the provisioning route tells it about catalogs outside its binding. The
 * body's name is held to the binding before the name's own rules, and the
 * data_path overlap refusal names another catalog only to a caller whose
 * token names that catalog. A refusal creates nothing.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BoundOperatorProvisioningIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val pair = TestTokens.keyPair()
    private val keyFile =
        Files.createTempFile("hoglake-bound-op-", ".json").also { Files.writeString(it, TestTokens.jwks(pair)) }
    private val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()

    /** Outside the bound operator's binding: neither its name nor its prefix may reach that operator. */
    private val hidden = "hidden-catalog"
    private val hiddenPath = "s3://bucket/hidden-prefix"

    init {
        catalogs.createCatalog(hidden, hiddenPath)
    }

    @AfterAll
    fun close() {
        http.close()
        db.close()
        Files.deleteIfExists(keyFile)
    }

    private fun withServer(block: (port: Int) -> Unit) {
        val cfg =
            Config(port = 0, hydratorIntervalMs = 0).copy(
                authMode = AuthMode.ENFORCE.wire,
                authIssuer = TestTokens.ISSUER,
                authJwksFile = keyFile.toString(),
            )
        val app = App.build(cfg, db.jdbi)
        val server = embeddedServer(Netty, configure = { configureApiListener(cfg) }) { app.module(this) }
        try {
            server.start(wait = false)
            block(runBlocking { server.engine.resolvedConnectors().first().port })
        } finally {
            server.stop(0, 1000)
            app.close()
        }
    }

    private fun fleetWide() = TestTokens.mint(pair, TestTokens.operatorClaims())

    /** An operator bound to [names], each with `admin`. */
    private fun bound(vararg names: String) =
        TestTokens.mint(
            pair,
            TestTokens.operatorClaims().dropLast(1) +
                ""","org":"org-1","cat":{${names.joinToString(",") { """"$it":["admin"]""" }}}}""",
        )

    private fun create(
        port: Int,
        token: String,
        name: String,
        dataPath: String,
    ): HttpResponse<String> =
        http.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port/v1/catalogs"))
                .header("Authorization", "Bearer $token")
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("""{"name":"$name","data_path":"$dataPath"}"""))
                .build(),
            HttpResponse.BodyHandlers.ofString(),
        )

    private fun exists(name: String) = catalogs.listCatalogs(setOf(name)).isNotEmpty()

    @Test
    fun `an overlap with a catalog outside the binding does not name that catalog or its data_path`() =
        withServer { port ->
            // MUTATION: hand the overlap refusal through unredacted
            // (BoundCatalogStore.createCatalog) and the bound body names both.
            val refused = create(port, bound("mine"), "mine", "s3://bucket/")
            assertThat(refused.statusCode()).isEqualTo(422)
            assertThat(refused.body())
                .contains("overlaps another catalog")
                .doesNotContain(hidden)
                .doesNotContain("hidden-prefix")
            assertThat(exists("mine")).isFalse()

            // A binding that names the other catalog too may read it, as a
            // fleet-wide operator, which names every catalog, does.
            for (token in listOf(bound("mine", hidden), fleetWide())) {
                val detailed = create(port, token, "mine", "s3://bucket/")
                assertThat(detailed.statusCode()).isEqualTo(422)
                assertThat(detailed.body()).contains("overlaps catalog '$hidden'").contains(hiddenPath)
            }
            assertThat(exists("mine")).isFalse()
        }

    @Test
    fun `a reserved name outside the binding is the 404 of any other name, not the reserved name's 422`() =
        withServer { port ->
            val reserved = "01a1e5de-1ac5-0000-e53e-d2f9a77deae4-events"
            // MUTATION: check the reserved name before the binding (Routes.kt)
            // and this answers 422.
            val other = create(port, bound("mine"), reserved, "s3://bucket/reserved")
            assertThat(other.statusCode()).isEqualTo(404)
            assertThat(other.body()).contains("catalog '$reserved' not found")
            // A caller entitled to the name still gets the reserved name's refusal.
            for (token in listOf(bound(reserved), fleetWide())) {
                val refused = create(port, token, reserved, "s3://bucket/reserved")
                assertThat(refused.statusCode()).isEqualTo(422)
                assertThat(refused.body()).contains("is reserved")
            }
            assertThat(exists(reserved)).isFalse()
        }
}
