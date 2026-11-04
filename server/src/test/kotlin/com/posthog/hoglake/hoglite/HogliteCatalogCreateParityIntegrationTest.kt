package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.DatabaseBackend
import com.posthog.hoglake.HealthProbe
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.testing.PgTestSupport
import com.sun.net.httpserver.HttpServer
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.statement.SqlLogger
import org.jdbi.v3.core.statement.StatementContext
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

/** Configured App routes, not backend inference. POSTGRES never constructs an optional engine helper. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteCatalogCreateParityIntegrationTest {
    enum class Mode { POSTGRES, HOGLITE, MIXED }

    private val json = jacksonObjectMapper()
    private var pg: PgTestSupport.TestDb? = null
    private var live: HogliteConformanceEngine? = null
    private val pair = TestTokens.keyPair()
    private val io = AtomicInteger()

    @AfterAll
    fun close() {
        pg?.close()
        live?.close()
    }

    private fun fixture(
        mode: Mode,
        enforce: Boolean = false,
        body: (App) -> Unit,
    ) {
        var proxy: HttpServer? = null
        var transport: HogliteClient? = null
        val executor = Executors.newFixedThreadPool(4)
        val jwks = Files.createTempFile("catalog-create-jwks", ".json")
        Files.writeString(jwks, TestTokens.jwks(pair))
        val db = if (mode != Mode.HOGLITE) pg ?: PgTestSupport.freshDatabase().also { pg = it } else null
        db?.jdbi?.setSqlLogger(
            object : SqlLogger {
                override fun logBeforeExecution(context: StatementContext) {
                    io.incrementAndGet()
                }
            },
        )
        val base: Config
        val hg: HogliteCatalogStore?
        if (mode != Mode.POSTGRES) {
            val engine = live ?: HogliteConformanceEngine().also { live = it }
            val http = HttpClient.newHttpClient()
            proxy =
                HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
                    this.executor = executor
                    createContext("/") { ex ->
                        io.incrementAndGet()
                        val answer =
                            http.send(
                                HttpRequest.newBuilder(URI.create(engine.url() + ex.requestURI))
                                    .header("Content-Type", "application/json")
                                    .method(
                                        ex.requestMethod,
                                        HttpRequest.BodyPublishers.ofByteArray(ex.requestBody.readBytes()),
                                    )
                                    .build(),
                                HttpResponse.BodyHandlers.ofByteArray(),
                            )
                        ex.sendResponseHeaders(answer.statusCode(), answer.body().size.toLong())
                        ex.responseBody.use { it.write(answer.body()) }
                    }
                    start()
                }
            transport = HogliteClient(standardUrl = "http://127.0.0.1:${proxy.address.port}", sleeper = {})
            hg = HogliteCatalogStore(transport)
            base = engine.config().copy(hogliteUrl = "http://127.0.0.1:${proxy.address.port}")
        } else {
            base = Config()
            hg = null
        }
        val cfg =
            base.copy(
                database = DatabaseBackend.valueOf(mode.name),
                hydratorIntervalMs = 0,
                authMode = if (enforce) "enforce" else "off",
                authIssuer = if (enforce) TestTokens.ISSUER else "",
                authAudience = TestTokens.AUDIENCE,
                authJwksFile = if (enforce) jwks.toString() else "",
            )
        try {
            val app =
                when (mode) {
                    Mode.POSTGRES -> App.build(cfg, db!!.jdbi)
                    Mode.HOGLITE -> App.buildHoglite(cfg, hg!!, HealthProbe.forProbe(1000) { true })
                    Mode.MIXED -> App.buildMixed(cfg, db!!.jdbi, hg!!, HealthProbe.forProbe(1000) { true })
                }
            app.use(body)
        } finally {
            db?.jdbi?.setSqlLogger(SqlLogger.NOP_SQL_LOGGER)
            transport?.close()
            proxy?.stop(0)
            executor.shutdownNow()
            Files.delete(jwks)
        }
    }

    private fun name() = "create" + UUID.randomUUID().toString().replace("-", "")

    @ParameterizedTest(name = "{displayName} [{index}] {0}")
    @EnumSource(Mode::class)
    fun `configured create selection presence null scalar and conflict matrix`(mode: Mode) =
        fixture(mode) { app ->
            testApplication {
                application { app.module(this) }
                client.get("/livez") // Start before observing backend I/O.
                val selections =
                    buildList {
                        add(emptyMap<String, Any?>())
                        for (field in listOf("backend", "connection_ref")) {
                            for (value in listOf(
                                null, "postgres", "hoglite", "mixed", "invalid", 7, true,
                                listOf(
                                    "postgres",
                                ),
                                mapOf("x" to 1),
                            )) {
                                add(mapOf(field to value))
                            }
                        }
                        add(mapOf("backend" to null, "connection_ref" to null))
                        add(mapOf("backend" to "postgres", "connection_ref" to "postgres"))
                        add(mapOf("backend" to "hoglite", "connection_ref" to "hoglite"))
                        add(mapOf("backend" to "postgres", "connection_ref" to "hoglite"))
                        add(mapOf("backend" to null, "connection_ref" to "postgres"))
                        add(mapOf("backend" to "hoglite", "connection_ref" to null))
                    }
                for (selection in selections) {
                    val cat = name()
                    val malformedType = selection.values.any { it is List<*> || it is Map<*, *> }
                    val expected =
                        when {
                            mode == Mode.POSTGRES && selection.isNotEmpty() -> HttpStatusCode.BadRequest
                            malformedType -> HttpStatusCode.BadRequest
                            mode == Mode.HOGLITE && selection.values.any { it != null } ->
                                HttpStatusCode.UnprocessableEntity
                            mode == Mode.MIXED -> {
                                val selected = selection["backend"] ?: selection["connection_ref"] ?: "hoglite"
                                val connection = selection["connection_ref"]
                                if (selected !in listOf("hoglite", "postgres") ||
                                    (connection != null && connection != selected)
                                ) {
                                    HttpStatusCode.UnprocessableEntity
                                } else {
                                    HttpStatusCode.Created
                                }
                            }
                            else -> HttpStatusCode.Created
                        }
                    val before = io.get()
                    val response =
                        client.post("/v1/catalogs") {
                            contentType(ContentType.Application.Json)
                            setBody(
                                json.writeValueAsString(
                                    mapOf("name" to cat, "data_path" to "s3://hoglake-it/$cat") + selection,
                                ),
                            )
                        }
                    val raw = response.bodyAsText()
                    assertThat(
                        response.status,
                    ).describedAs("%s selection=%s body=%s", mode, selection, raw).isEqualTo(expected)
                    if (expected == HttpStatusCode.BadRequest) {
                        assertThat(json.readTree(raw)["error"].asText()).isEqualTo("bad_request")
                        assertThat(
                            io.get(),
                        ).describedAs("400 must not touch SQL, directory or engine").isEqualTo(before)
                    }
                    if (expected == HttpStatusCode.Created) {
                        assertThat(json.readTree(raw)["name"].asText()).isEqualTo(cat)
                        if (mode == Mode.MIXED) {
                            val selected = selection["backend"] ?: selection["connection_ref"] ?: "hoglite"
                            val placement =
                                live!!.store.queryRegistry("SELECT backend FROM catalogs WHERE name = ?", cat)
                                    .firstRow()!![0].asString()
                            assertThat(placement).isEqualTo(selected)
                        }
                    }
                }
            }
        }

    @ParameterizedTest(name = "{displayName} [{index}] {0}")
    @EnumSource(Mode::class)
    fun `configured create malformed unknown strict attribution and content type do no backend IO`(mode: Mode) =
        fixture(mode) { app ->
            testApplication {
                application { app.module(this) }
                client.get("/livez")
                val cat = name()
                val good = """{"name":"$cat","data_path":"s3://hoglake-it/$cat"}"""
                val invalid =
                    listOf("{", "null", "[]", "{}", "", good.dropLast(1) + ",\"unknown\":null}") +
                        listOf("author", "message").flatMap { field ->
                            listOf("7", "true", "{}", "[]").map { good.dropLast(1) + ",\"$field\":$it}" }
                        }
                val requests = invalid.map { it to ContentType.Application.Json } + (good to ContentType.Text.Plain)
                for ((body, type) in requests) {
                    val before = io.get()
                    val response =
                        client.post("/v1/catalogs") {
                            contentType(type)
                            setBody(body)
                        }
                    assertThat(
                        response.status,
                    ).describedAs("%s body=%s type=%s: %s", mode, body, type, response.bodyAsText())
                        .isEqualTo(HttpStatusCode.BadRequest)
                    assertThat(json.readTree(response.bodyAsText())["error"].asText()).isEqualTo("bad_request")
                    assertThat(io.get()).isEqualTo(before)
                }
                for (metadata in listOf(
                    "\"author\":null,\"message\":null",
                    "\"author\":\"writer\",\"message\":\"created\"",
                )) {
                    val named = name()
                    val response =
                        client.post("/v1/catalogs") {
                            contentType(ContentType.Application.Json)
                            setBody("""{"name":"$named","data_path":"s3://hoglake-it/$named",$metadata}""")
                        }
                    assertThat(response.status).isEqualTo(HttpStatusCode.Created)
                }
            }
        }

    @ParameterizedTest(name = "{displayName} [{index}] {0}")
    @EnumSource(Mode::class)
    fun `configured create authenticates before decoding and keeps operator attribution`(mode: Mode) =
        fixture(mode, true) { app ->
            testApplication {
                application { app.module(this) }
                client.get("/livez")
                val tenant = TestTokens.mint(pair, TestTokens.tenantClaims())
                val operator = TestTokens.mint(pair, TestTokens.operatorClaims())
                for ((token, expected) in listOf(
                    null to HttpStatusCode.Unauthorized,
                    tenant to HttpStatusCode.Forbidden,
                )) {
                    for (body in listOf("{", """{"backend":null}""")) {
                        val before = io.get()
                        val response =
                            client.post("/v1/catalogs") {
                                if (token != null) header(HttpHeaders.Authorization, "Bearer $token")
                                contentType(ContentType.Application.Json)
                                setBody(body)
                            }
                        assertThat(response.status).isEqualTo(expected)
                        assertThat(io.get()).isEqualTo(before)
                    }
                }
                val before = io.get()
                val malformed =
                    client.post("/v1/catalogs") {
                        header(HttpHeaders.Authorization, "Bearer $operator")
                        contentType(ContentType.Application.Json)
                        setBody("""{"author":7}""")
                    }
                assertThat(malformed.status).isEqualTo(HttpStatusCode.BadRequest)
                assertThat(io.get()).isEqualTo(before)
                val cat = name()
                val created =
                    client.post("/v1/catalogs") {
                        header(HttpHeaders.Authorization, "Bearer $operator")
                        contentType(ContentType.Application.Json)
                        setBody(
                            """{"name":"$cat","data_path":"s3://hoglake-it/$cat",
 "author":"delegated","message":"audit"}""",
                        )
                    }
                assertThat(created.status).isEqualTo(HttpStatusCode.Created)
                // Snapshot listing excludes snapshot0; inspect the actual initial snapshot explicitly.
                val attribution =
                    if (mode == Mode.POSTGRES) {
                        pg!!.jdbi.withHandle<List<String>, Exception> { h ->
                            h.createQuery(
                                "SELECT author || ':' || commit_message FROM hog_snapshot " +
                                    "WHERE catalog_id = ? AND snapshot_id = 0",
                            )
                                .bind(0, app.store.getCatalog(cat).catalogId).mapTo(String::class.java).list()
                        }
                    } else {
                        live!!.store.catalogOperation(cat) {
                            live!!.store.queryCatalog(
                                cat,
                                "SELECT author || ':' || message FROM snapshots WHERE id = 0",
                            )
                                .rows.map { it[0].asString() }
                        }
                    }
                assertThat(attribution).containsExactly("delegated:audit")
                val own = TestTokens.mint(pair, TestTokens.tenantClaims(catalogs = mapOf(cat to listOf("admin"))))
                val refused =
                    client.post("/v1/catalogs/$cat/namespaces") {
                        header(HttpHeaders.Authorization, "Bearer $own")
                        contentType(ContentType.Application.Json)
                        setBody("""{"name":"ns","author":"forged"}""")
                    }
                assertThat(refused.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
            }
        }
}
