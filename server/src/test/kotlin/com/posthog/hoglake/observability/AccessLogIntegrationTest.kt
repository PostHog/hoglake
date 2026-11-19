package com.posthog.hoglake.observability

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import ch.qos.logback.core.read.ListAppender
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.engine.connector
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import kotlinx.coroutines.runBlocking
import net.logstash.logback.encoder.LogstashEncoder
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.slf4j.LoggerFactory
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.security.interfaces.ECPublicKey
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * The access log end to end, through the real `App.module`: lines on
 * "hoglake.access" (and "hoglake.audit") are encoded by the real
 * LogstashEncoder, so the assertions are on the JSON stdout carries.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AccessLogIntegrationTest {
    private val db = PgTestSupport.freshDatabase()

    // One request thread and a short admission bound, so the shed test can
    // queue a request past it. Every other test sends one request at a time.
    private val cfg =
        Config(
            hydratorIntervalMs = 0,
            metricsIntervalMs = 0,
            maintenanceSummaryIntervalMs = 0,
            requestThreads = 1,
            commitLockTimeoutMs = ADMISSION_MS,
        )
    private val app = App.build(cfg, db.jdbi)
    private val json = ObjectMapper()
    private val pair = TestTokens.keyPair()
    private val enforce =
        AuthService(
            AuthMode.ENFORCE,
            JwtVerifier(
                TestTokens.ISSUER,
                TestTokens.AUDIENCE,
                StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey),
                predicateFields = setOf("team_id"),
            ) { 1_700_000_100L },
        )
    private val off = AuthService(AuthMode.OFF, null)

    private class CapturingAppender(private val encoder: LogstashEncoder) : AppenderBase<ILoggingEvent>() {
        val lines = CopyOnWriteArrayList<String>()

        override fun append(event: ILoggingEvent) {
            lines += String(encoder.encode(event))
        }
    }

    private val ctx = LoggerFactory.getILoggerFactory() as LoggerContext
    private val access = capture(ACCESS_LOGGER_NAME)
    private val audit = capture(Audit.LOGGER_NAME)

    private fun capture(logger: String): CapturingAppender {
        val encoder =
            LogstashEncoder().apply {
                context = ctx
                setIncludeMdc(false)
                start()
            }
        return CapturingAppender(encoder).apply {
            context = ctx
            start()
            (LoggerFactory.getLogger(logger) as Logger).addAppender(this)
        }
    }

    @BeforeAll
    fun seed() {
        val catalogs = CatalogService(db.jdbi)
        catalogs.createCatalog("lake", "s3://bucket/lake")
        catalogs.createNamespace("lake", "team_42")
    }

    @BeforeEach
    fun clear() {
        access.lines.clear()
        audit.lines.clear()
    }

    @AfterAll
    fun close() {
        (LoggerFactory.getLogger(ACCESS_LOGGER_NAME) as Logger).detachAppender(access)
        (LoggerFactory.getLogger(Audit.LOGGER_NAME) as Logger).detachAppender(audit)
        app.close()
        db.close()
    }

    private fun api(
        auth: AuthService,
        block: suspend ApplicationTestBuilder.() -> Unit,
    ) = testApplication {
        application { app.module(this, auth) }
        block()
    }

    private fun lines(): List<JsonNode> = access.lines.map(json::readTree)

    /** The one line for [requestId], raw and parsed. Written after the response, so awaited. */
    private fun lineFor(requestId: String?): Pair<String, JsonNode> {
        await().atMost(Duration.ofSeconds(10)).until { lines().any { it["request_id"]?.asText() == requestId } }
        val raw = access.lines.single { json.readTree(it)["request_id"]?.asText() == requestId }
        return raw to json.readTree(raw)
    }

    @Test
    fun `a tenant request names its org, team, token and route, and never the token or the query`() =
        api(enforce) {
            val token = TestTokens.mint(pair, TestTokens.tenantClaims())
            val r =
                client.get("/v1/catalogs/lake/namespaces/team_42?probe=query-secret") {
                    header(HttpHeaders.Authorization, "Bearer $token")
                    header(HttpHeaders.UserAgent, "pyhoglake/1.0")
                }
            assertThat(r.status).isEqualTo(HttpStatusCode.OK)
            val (raw, line) = lineFor(r.headers[REQUEST_ID_HEADER])
            assertThat(line["listener"].asText()).isEqualTo("api")
            assertThat(line["method"].asText()).isEqualTo("GET")
            assertThat(line["status"].asInt()).isEqualTo(200)
            assertThat(line["route"].asText()).isEqualTo("/v1/catalogs/{catalog}/namespaces/{namespace}")
            assertThat(line["catalog"].asText()).isEqualTo("lake")
            assertThat(line["namespace"].asText()).isEqualTo("team_42")
            assertThat(line["team"].asLong()).isEqualTo(42)
            assertThat(line["auth"].asText()).isEqualTo("authenticated")
            assertThat(line["kind"].asText()).isEqualTo("tenant")
            assertThat(line["org"].asText()).isEqualTo(ORG)
            assertThat(line["jti"].asText()).isEqualTo("jti-1")
            assertThat(line["client"].asText()).isEqualTo("hedgerow-prod")
            assertThat(line["client_class"].asText()).isEqualTo("pyhoglake")
            assertThat(line["duration_ms"].isIntegralNumber).isTrue()
            assertThat(line["queue_wait_ms"].isIntegralNumber).isTrue()
            assertThat(line.has("outcome")).isFalse()
            assertThat(raw).doesNotContain(token, "Bearer", "query-secret")
        }

    @Test
    fun `a scoped refusal names the user and the one team its grant holds`() =
        api(enforce) {
            val token = TestTokens.mint(pair, TestTokens.scopedClaims(prd = """{"team_id":{"42":"read"}}"""))
            val r = client.get("/v1/catalogs/lake") { header(HttpHeaders.Authorization, "Bearer $token") }
            assertThat(r.status).isEqualTo(HttpStatusCode.Forbidden)
            val (_, line) = lineFor(r.headers[REQUEST_ID_HEADER])
            assertThat(line["kind"].asText()).isEqualTo("scoped")
            assertThat(line["usr"].asText()).isEqualTo("user-1")
            assertThat(line["client"].asText()).isEqualTo("laptop")
            assertThat(line["jti"].asText()).isEqualTo("jti-s")
            assertThat(line["team"].asLong()).isEqualTo(42)
            assertThat(line["outcome"].asText()).isEqualTo("forbidden")
            assertThat(line.has("queue_wait_ms")).isFalse()
        }

    @Test
    fun `a bad token is an anonymous 401 with the response's request id, on both listeners`() {
        for ((auth, listener) in listOf(enforce to "api", enforce.forPublicListener() to "public")) {
            access.lines.clear()
            api(auth) {
                val r = client.get("/v1/catalogs/lake") { header(HttpHeaders.Authorization, "Bearer not-a-jwt") }
                assertThat(r.status).isEqualTo(HttpStatusCode.Unauthorized)
                val (raw, line) = lineFor(r.headers[REQUEST_ID_HEADER])
                assertThat(line["listener"].asText()).describedAs(listener).isEqualTo(listener)
                assertThat(line["status"].asInt()).isEqualTo(401)
                assertThat(line["auth"].asText()).isEqualTo("anonymous")
                assertThat(line["route"].asText()).isEqualTo("/v1/catalogs/{catalog}")
                assertThat(line["outcome"].asText()).isEqualTo(json.readTree(r.bodyAsText())["error"].asText())
                assertThat(line.has("kind")).isFalse()
                // Refused before the dispatch seam: it never queued.
                assertThat(line.has("queue_wait_ms")).isFalse()
                assertThat(raw).doesNotContain("not-a-jwt")
            }
        }
    }

    @Test
    fun `a request shed by admission is a 503 with its outcome and its queue wait`() {
        val entered = CountDownLatch(1)
        val server =
            embeddedServer(Netty, configure = { connector { port = 0 } }) {
                app.module(this, off)
                routing {
                    get("/test/block") {
                        entered.countDown()
                        Thread.sleep(ADMISSION_MS * 3)
                        call.respondText("done")
                    }
                }
            }.start(wait = false)
        try {
            val port = runBlocking { server.engine.resolvedConnectors().first().port }
            val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()

            fun send(path: String) =
                http.send(
                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port$path"))
                        .timeout(Duration.ofSeconds(30))
                        .build(),
                    HttpResponse.BodyHandlers.ofString(),
                )
            // The one request thread is busy, so the next request queues past the bound.
            val blocker = CompletableFuture.supplyAsync { send("/test/block") }
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue()
            val shed = send("/v1/catalogs/lake/namespaces/team_42")
            assertThat(shed.statusCode()).isEqualTo(503)
            assertThat(blocker.get(30, TimeUnit.SECONDS).statusCode()).isEqualTo(200)
            val (_, line) = lineFor(shed.headers().firstValue(REQUEST_ID_HEADER).orElse(null))
            assertThat(line["status"].asInt()).isEqualTo(503)
            assertThat(line["outcome"].asText()).isEqualTo("commit_queue_timeout")
            assertThat(line["queue_wait_ms"].asLong()).isGreaterThanOrEqualTo(ADMISSION_MS)
            assertThat(line["route"].asText()).isEqualTo("/v1/catalogs/{catalog}/namespaces/{namespace}")
        } finally {
            server.stop(0, 1, TimeUnit.SECONDS)
        }
    }

    @Test
    fun `the probes write no line`() =
        api(off) {
            // Answered or not (this fixture's /healthz probes no database), none is logged.
            for (probe in listOf("/healthz", "/livez", "/metrics")) client.get(probe)
            lineFor(client.get("/v1/info").headers[REQUEST_ID_HEADER])
            assertThat(lines().map { it["route"].asText() }).containsExactly("/v1/info")
        }

    @Test
    fun `with auth off the line carries no caller`() =
        api(off) {
            val r = client.get("/v1/catalogs/lake/namespaces/team_42")
            assertThat(r.status).isEqualTo(HttpStatusCode.OK)
            val (_, line) = lineFor(r.headers[REQUEST_ID_HEADER])
            assertThat(line["auth"].asText()).isEqualTo("off")
            assertThat(line["listener"].asText()).isEqualTo("api")
            for (field in listOf("kind", "org", "jti", "usr", "client")) {
                assertThat(line.has(field)).describedAs(field).isFalse()
            }
            // From the namespace, not from a caller.
            assertThat(line["team"].asLong()).isEqualTo(42)
        }

    @Test
    fun `an audited write in a request still carries the request id`() =
        api(enforce) {
            val token = TestTokens.mint(pair, TestTokens.tenantClaims())
            val r =
                client.post("/v1/catalogs/lake/namespaces") {
                    header(HttpHeaders.Authorization, "Bearer $token")
                    contentType(ContentType.Application.Json)
                    setBody("""{"name": "written"}""")
                }
            assertThat(r.status).isEqualTo(HttpStatusCode.Created)
            val id = r.headers[REQUEST_ID_HEADER]
            val created = audit.lines.map(json::readTree).single { it["action"].asText() == "namespace_create" }
            assertThat(created["request_id"].asText()).isEqualTo(id)
            assertThat(lineFor(id).second["status"].asInt()).isEqualTo(201)
        }

    @Test
    fun `CallLogging writes no plain-text line for a request`() {
        val root = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) as Logger
        val plain = ListAppender<ILoggingEvent>().apply { start() }
        root.addAppender(plain)
        try {
            api(off) {
                val r = client.get("/v1/catalogs/lake/namespaces/team_42")
                lineFor(r.headers[REQUEST_ID_HEADER])
            }
            assertThat(plain.list.map { it.formattedMessage }).noneMatch { "/v1/catalogs/lake/namespaces" in it }
        } finally {
            root.detachAppender(plain)
        }
    }

    private companion object {
        const val ADMISSION_MS = 400L
        const val ORG = "11111111-1111-1111-1111-111111111111"
    }
}
