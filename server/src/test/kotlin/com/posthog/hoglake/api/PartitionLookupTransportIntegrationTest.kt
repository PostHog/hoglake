package com.posthog.hoglake.api

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.configureApiListener
import com.posthog.hoglake.testing.PartitionLookupFixture
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.wireObjectMapper
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.time.Duration

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PartitionLookupTransportIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val cfg = Config(port = 0, hydratorIntervalMs = 0)
    private val app = App.build(cfg, db.jdbi)
    private val f = PartitionLookupFixture(db)
    private val json = wireObjectMapper()
    private val server = embeddedServer(Netty, configure = { configureApiListener(cfg) }) { app.module(this) }
    private val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
    private var port = 0

    @BeforeAll
    fun boot() {
        server.start(wait = false)
        port = runBlocking { server.engine.resolvedConnectors().first().port }
    }

    @AfterAll
    fun close() {
        http.close()
        server.stop(0, 1000)
        app.close()
        db.close()
    }

    private fun post(
        raw: String,
        listenerPort: Int = port,
        path: String = "${f.url}?spec_id=${f.specId}",
        token: String? = null,
        chunked: Boolean = false,
    ): HttpResponse<String> {
        val request =
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:$listenerPort$path"))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
        if (token != null) request.header("Authorization", "Bearer $token")
        val publisher =
            if (chunked) {
                HttpRequest.BodyPublishers.ofInputStream { raw.byteInputStream() }
            } else {
                HttpRequest.BodyPublishers.ofString(raw)
            }
        return http.send(request.POST(publisher).build(), HttpResponse.BodyHandlers.ofString())
    }

    private fun lookup(values: List<String?>): HttpResponse<String> = post(json.writeValueAsString(values))

    @Test
    fun `maximum length Unicode partition values reach the production POST route over Netty`() {
        val tuple = listOf("東".repeat(8192), "京".repeat(8192))
        val response = lookup(tuple)
        assertThat(response.statusCode()).isEqualTo(200)
        assertThat(json.readTree(response.body())["values"].map { it.textValue() }).isEqualTo(tuple)
    }

    @Test
    fun `near maximum tuple reaches semantic validation instead of transport rejection`() {
        val tuple = List(8) { "東".repeat(8188) }
        assertThat(json.writeValueAsString(tuple).length).isBetween(65_500, 65_536)
        val response = lookup(tuple)
        assertThat(response.statusCode()).isEqualTo(422)
        assertThat(json.readTree(response.body()).toString()).contains("complete 2-field tuple")
    }

    @Test
    fun `oversized values still receive a structured validation error`() {
        val response = lookup(listOf("東".repeat(8193), "京"))
        assertThat(response.statusCode()).isEqualTo(400)
        assertThat(json.readTree(response.body()).isObject).isTrue()
        assertThat(response.body()).contains("8192")
    }

    @Test
    fun `malformed tuple types are refused without coercion over Netty`() {
        for (raw in listOf("", "null", "[1, true]", "[{}, null]", "[null, null] {}")) {
            val response = post(raw)
            assertThat(response.statusCode()).describedAs(raw).isEqualTo(400)
            assertThat(json.readTree(response.body())["error"].textValue()).isEqualTo("bad_request")
        }
    }

    @Test
    fun `body bytes are bounded before JSON parsing for fixed and chunked bodies`() {
        for (chunked in listOf(false, true)) {
            val response = post(" ".repeat(262_145), chunked = chunked)
            assertThat(response.statusCode()).describedAs("chunked=%s", chunked).isEqualTo(400)
            assertThat(response.body()).contains("262144 bytes")
        }
    }

    @Test
    fun `unrelated GET retains the default request line cap`() {
        val request =
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port/healthz?ignored=${"x".repeat(5000)}"))
                .timeout(Duration.ofSeconds(10)).GET().build()
        val response = http.send(request, HttpResponse.BodyHandlers.ofString())
        assertThat(response.statusCode()).isEqualTo(400)
        assertThat(response.body()).contains("4096")
    }

    @Test
    fun `POST lookup is readable while mutation remains forbidden in enforce and modes keep their semantics`() {
        val pair = TestTokens.keyPair()
        val keyFile = Files.createTempFile("hoglake-lookup-auth-", ".json")
        Files.writeString(keyFile, TestTokens.jwks(pair))
        val reader = TestTokens.mint(pair, TestTokens.tenantClaims(catalogs = mapOf(f.name to listOf("read"))))
        val foreign = TestTokens.mint(pair, TestTokens.tenantClaims(catalogs = mapOf("other" to listOf("read"))))
        try {
            for (mode in AuthMode.entries) {
                val authCfg =
                    cfg.copy(authMode = mode.wire, authIssuer = TestTokens.ISSUER, authJwksFile = keyFile.toString())
                val authApp = App.build(authCfg, db.jdbi)
                val authServer =
                    embeddedServer(Netty, configure = { configureApiListener(authCfg) }) { authApp.module(this) }
                try {
                    authServer.start(wait = false)
                    val authPort = runBlocking { authServer.engine.resolvedConnectors().first().port }
                    assertThat(post("[null, null]", authPort, token = reader).statusCode())
                        .describedAs("%s reader lookup", mode).isEqualTo(200)
                    assertThat(post("[null, null]", authPort).statusCode())
                        .describedAs("%s missing token", mode).isEqualTo(if (mode == AuthMode.ENFORCE) 401 else 200)
                    assertThat(post("[null, null]", authPort, token = foreign).statusCode())
                        .describedAs("%s foreign catalog", mode).isEqualTo(if (mode == AuthMode.ENFORCE) 404 else 200)
                    val alter = f.url.removeSuffix("/partitions/lookup") + "/alter"
                    assertThat(post("{}", authPort, alter, reader).statusCode())
                        .describedAs("%s reader mutation", mode).isEqualTo(if (mode == AuthMode.ENFORCE) 403 else 400)
                } finally {
                    authServer.stop(0, 1000)
                    authApp.close()
                }
            }
        } finally {
            Files.deleteIfExists(keyFile)
        }
    }
}
