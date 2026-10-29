package com.posthog.hoglake.api

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
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
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets.UTF_8
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

    private fun lookup(values: List<String?>): HttpResponse<String> {
        val encoded = URLEncoder.encode(json.writeValueAsString(values), UTF_8)
        assertThat(encoded.length).isGreaterThan(4096)
        val uri = URI.create("http://127.0.0.1:$port${f.url}?spec_id=${f.specId}&values=$encoded")
        return http.send(
            HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(10)).GET().build(),
            HttpResponse.BodyHandlers.ofString(),
        )
    }

    @Test
    fun `maximum length Unicode partition values reach the production route over Netty`() {
        val tuple = listOf("東".repeat(8192), "京".repeat(8192))
        val response = lookup(tuple)
        assertThat(response.statusCode()).isEqualTo(200)
        assertThat(json.readTree(response.body())["values"].map { it.textValue() }).isEqualTo(tuple)
    }

    @Test
    fun `near maximum encoded tuple reaches semantic validation instead of transport rejection`() {
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
}
