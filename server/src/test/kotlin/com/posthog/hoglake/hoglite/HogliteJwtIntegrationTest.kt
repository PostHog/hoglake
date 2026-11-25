package com.posthog.hoglake.hoglite

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.model.HoglakeException
import com.sun.net.httpserver.HttpServer
import io.ktor.client.request.delete
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.put
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import java.security.interfaces.ECPublicKey
import java.time.Instant
import java.util.UUID

/** The real API, request selector, adapter, and locally built enforcing engine. */
@Tag("integration")
class HogliteJwtIntegrationTest {
    @TempDir
    lateinit var dir: Path

    @Test
    fun `verified authority reaches operator provisioning and tenant metadata requests`() {
        val pair = TestTokens.keyPair()
        val id = "7c9e2a14-3f51-4b8d-9e60-2d5a1f8c44aa"
        val jwks = dir.resolve("public.json")
        Files.writeString(jwks, TestTokens.jwks(pair))
        HogliteConformanceEngine(
            listOf(
                "-auth-mode",
                "enforce",
                "-auth-issuer",
                TestTokens.ISSUER,
                "-auth-backend-id",
                id,
                "-auth-jwks-file",
                jwks.toString(),
            ),
        ).use { engine ->
            val backends = dir.resolve("backends.json")
            Files.writeString(
                backends,
                """{"backends":[{"id":"$id","kind":"hoglite","url":"${engine.url()}"}]}""",
            )
            val cfg =
                Config(
                    defaultBackend = "none",
                    backendsFile = backends.toString(),
                    authMode = "enforce",
                    jdbcUrl = "jdbc:no-driver:must-not-open",
                    dbUser = "",
                    dbPassword = "",
                )
            val auth =
                AuthService(
                    AuthMode.ENFORCE,
                    JwtVerifier(
                        TestTokens.ISSUER,
                        "hoglake",
                        StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey),
                        backends = cfg.backendKinds,
                    ),
                )
            val now = Instant.now().epochSecond

            fun token(
                kind: String,
                scope: String,
                tenant: String = "opaque_1",
                catalog: String = "lake",
            ): String =
                TestTokens.mint(
                    pair,
                    """{"iss":"${TestTokens.ISSUER}","aud":["hoglake","hoglite"],"sub":"test-client",""" +
                        """"jti":"test-$kind-$scope","ten":"$kind","org":"org-not-the-tid",""" +
                        """"cat":{"$catalog":["$scope"]},"iat":$now,"nbf":$now,"exp":${now + 900},""" +
                        """"rtg":{"b":"hoglite","be":"$id","t":"$tenant"}}""",
                )
            val operator = token("operator", "admin")
            val reader = token("tenant", "read")
            val writer = token("tenant", "write")
            testApplication {
                App.build(cfg).use { app ->
                    application { app.module(this, auth) }
                    val created =
                        client.post("/v1/catalogs") {
                            header(HttpHeaders.Authorization, "Bearer $operator")
                            contentType(ContentType.Application.Json)
                            setBody("""{"name":"lake","data_path":"s3://hoglake-it/lake"}""")
                        }
                    assertThat(created.status).isEqualTo(HttpStatusCode.Created)
                    // ADMIN satisfies generic policy, but cannot release this backend's tenant.
                    assertThat(
                        client.delete("/v1/catalogs/lake?message=drop") {
                            header(HttpHeaders.Authorization, "Bearer ${token("tenant", "admin")}")
                        }.status,
                    ).isEqualTo(HttpStatusCode.Forbidden)
                    assertThat(
                        client.get("/v1/catalogs/lake") {
                            header(HttpHeaders.Authorization, "Bearer $reader")
                        }.status,
                    ).isEqualTo(HttpStatusCode.OK)
                    val written =
                        client.post("/v1/catalogs/lake/namespaces") {
                            header(HttpHeaders.Authorization, "Bearer $writer")
                            contentType(ContentType.Application.Json)
                            setBody("""{"name":"ns"}""")
                        }
                    assertThat(written.status).isEqualTo(HttpStatusCode.Created)
                    val namespaces =
                        client.get("/v1/catalogs/lake/namespaces") {
                            header(HttpHeaders.Authorization, "Bearer $reader")
                        }
                    assertThat(namespaces.status).isEqualTo(HttpStatusCode.OK)
                    assertThat(namespaces.bodyAsText()).contains("ns")
                    val operation = UUID.randomUUID()
                    val statusPath = "/v1/catalogs/lake/table-creations/$operation"
                    assertThat(
                        client.put("$statusPath/metadata") {
                            header(HttpHeaders.Authorization, "Bearer $writer")
                            contentType(ContentType.Application.Json)
                            setBody("""{"namespace":"ns","name":"pending","columns":[{"name":"id","type":"long"}]}""")
                        }.status,
                    ).isEqualTo(HttpStatusCode.OK)
                    val json = com.fasterxml.jackson.module.kotlin.jacksonObjectMapper()
                    val existing = client.get(statusPath) { header(HttpHeaders.Authorization, "Bearer $reader") }
                    assertThat(existing.status).isEqualTo(HttpStatusCode.OK)
                    assertThat(json.readTree(existing.bodyAsText())["state"].asText()).isEqualTo("prepared")
                    HogliteClient(engine.url()).use { shared ->
                        val selected = shared.forRequest(writer)
                        selected.tx(
                            "opaque_1",
                            "expire-$operation",
                            null,
                            listOf(
                                HogliteStatement(
                                    "UPDATE table_creations SET expires_at = ? WHERE operation = ?",
                                    listOf("2000-01-01T00:00:00.000000Z", operation.toString()),
                                ),
                            ),
                        )
                        val expired = client.get(statusPath) { header(HttpHeaders.Authorization, "Bearer $reader") }
                        assertThat(expired.status).isEqualTo(HttpStatusCode.OK)
                        val receipt = json.readTree(expired.bodyAsText())
                        assertThat(receipt["state"].asText()).isEqualTo("aborted")
                        assertThat(receipt["reason"].asText()).isEqualTo("expired")

                        fun persistedState(): Any? =
                            selected.query(
                                "opaque_1",
                                "SELECT state FROM table_creations WHERE operation = ?",
                                listOf(operation.toString()),
                            ).rows.single().single()
                        assertThat(persistedState()).isEqualTo("prepared")
                        val store = HogliteCatalogStore(selected, { "opaque_1" })
                        store.catalogOperation("lake") { store.loadCreationOrThrow("lake", operation) }
                        assertThat(persistedState()).isEqualTo("aborted")
                    }
                    assertThat(
                        client.get("/v1/catalogs") {
                            header(HttpHeaders.Authorization, "Bearer $reader")
                        }.status,
                    ).isEqualTo(HttpStatusCode.OK)
                    assertThat(
                        client.post("/v1/catalogs/lake/namespaces") {
                            header(HttpHeaders.Authorization, "Bearer $reader")
                            contentType(ContentType.Application.Json)
                            setBody("""{"name":"forbidden"}""")
                        }.status,
                    ).isEqualTo(HttpStatusCode.Forbidden)
                    assertThat(
                        client.delete("/v1/catalogs/lake") {
                            header(HttpHeaders.Authorization, "Bearer $writer")
                        }.status,
                    ).isEqualTo(HttpStatusCode.Forbidden)
                    assertThat(
                        client.get("/v1/catalogs/lake") {
                            header(HttpHeaders.Authorization, "Bearer ${token("tenant", "read", "other_1")}")
                        }.status,
                    ).isEqualTo(HttpStatusCode.NotFound)
                    assertThat(
                        client.get("/v1/catalogs/lake") {
                            header(HttpHeaders.Authorization, "Bearer not-verified")
                        }.status,
                    ).isEqualTo(HttpStatusCode.Unauthorized)
                }
            }
            // Warn mode bypasses generic policy refusals, not backend lifecycle capabilities.
            testApplication {
                App.build(cfg).use { app ->
                    val warn =
                        AuthService(
                            AuthMode.WARN,
                            JwtVerifier(
                                TestTokens.ISSUER,
                                "hoglake",
                                StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey),
                                backends = cfg.backendKinds,
                            ),
                        )
                    application { app.module(this, warn) }
                    val tenant = "opaque_warn_1"
                    val catalog = "warnlake"
                    val warnOperator = token("operator", "admin", tenant, catalog)
                    val warnAdmin = token("tenant", "admin", tenant, catalog)
                    val warnWriter = token("tenant", "write", tenant, catalog)
                    assertThat(
                        client.post("/v1/catalogs") {
                            header(HttpHeaders.Authorization, "Bearer $warnOperator")
                            contentType(ContentType.Application.Json)
                            setBody("""{"name":"$catalog","data_path":"s3://hoglake-it/warnlake"}""")
                        }.status,
                    ).isEqualTo(HttpStatusCode.Created)
                    for (credential in listOf(warnAdmin, warnWriter)) {
                        assertThat(
                            client.delete("/v1/catalogs/$catalog?message=drop") {
                                header(HttpHeaders.Authorization, "Bearer $credential")
                            }.status,
                        ).isEqualTo(HttpStatusCode.Forbidden)
                    }
                    assertThat(
                        client.post("/v1/catalogs/$catalog/namespaces") {
                            header(HttpHeaders.Authorization, "Bearer $warnWriter")
                            contentType(ContentType.Application.Json)
                            setBody("""{"name":"still_usable"}""")
                        }.status,
                    ).isEqualTo(HttpStatusCode.Created)
                }
            }
        }
    }

    /**
     * hoglite verifies the forwarded token on every hop, PostgreSQL once
     * when the request starts. A token that expires between a request's
     * applied write and its next hop must not answer invalid_token, which
     * a client re-mints and replays the whole request on as if nothing ran.
     */
    @Test
    fun `a token that expires after a request's write leaves an unknown outcome, not invalid_token`() {
        val pair = TestTokens.keyPair()
        val id = "7c9e2a14-3f51-4b8d-9e60-2d5a1f8c44aa"
        val jwks = dir.resolve("public.json")
        Files.writeString(jwks, TestTokens.jwks(pair))
        HogliteConformanceEngine(
            listOf(
                "-auth-mode",
                "enforce",
                "-auth-issuer",
                TestTokens.ISSUER,
                "-auth-backend-id",
                id,
                "-auth-jwks-file",
                jwks.toString(),
            ),
        ).use { engine ->
            val now = Instant.now().epochSecond

            fun token(
                kind: String,
                scope: String,
                exp: Long,
            ): String =
                TestTokens.mint(
                    pair,
                    """{"iss":"${TestTokens.ISSUER}","aud":["hoglake","hoglite"],"sub":"test-client",""" +
                        """"jti":"test-$kind-$scope-$exp","ten":"$kind","org":"org-not-the-tid",""" +
                        """"cat":{"lake":["$scope"]},"iat":${now - 120},"nbf":${now - 120},"exp":$exp,""" +
                        """"rtg":{"b":"hoglite","be":"$id","t":"opaque_1"}}""",
                )
            HogliteClient(engine.url(), sleeper = {}).use { shared ->
                val operator =
                    HogliteCatalogStore(shared.forRequest(token("operator", "admin", now + 900)), { "opaque_1" })
                operator.createCatalog("lake", "s3://hoglake-it/lake", null, null)

                // hoglited accepts a token up to HOGLITED_LEEWAY_SECONDS past its exp. Minted
                // after the cold createCatalog, so the write alone races the lifetime.
                val exp = Instant.now().epochSecond - HOGLITED_LEEWAY_SECONDS + EXPIRING_LIFETIME_SECONDS
                val expiring = token("tenant", "write", exp)
                val request = HogliteCatalogStore(shared.forRequest(expiring), { "opaque_1" })
                assertThat(catchThrowable { request.createNamespace("lake", "ns") })
                    .describedAs("the write must land within EXPIRING_LIFETIME_SECONDS; raise it on a slow runner")
                    .isNull()
                // Past the leeway by CLOCK_SKEW_SECONDS, in case hoglited's clock lags the JVM's.
                while (Instant.now().epochSecond < exp + HOGLITED_LEEWAY_SECONDS + CLOCK_SKEW_SECONDS) Thread.sleep(100)

                val failure = catchThrowable { request.listNamespaces("lake") }
                assertThat(failure).isExactlyInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
                assertThat((failure as HoglakeException.CommitOutcomeUnknown).applied).isTrue()
                // A request that applied nothing keeps the refusal a client re-mints for.
                val fresh = HogliteCatalogStore(shared.forRequest(expiring), { "opaque_1" })
                val refused = catchThrowable { fresh.listNamespaces("lake") }
                assertThat(refused).isExactlyInstanceOf(HoglakeException.Unauthenticated::class.java)
                assertThat((refused as HoglakeException.Unauthenticated).reason).isEqualTo("invalid_token")
                // The write stands.
                assertThat(operator.listNamespaces("lake").map { it.name }).contains("ns")
            }
        }
    }

    /**
     * The other half of #680: a long-running route refuses up front, as the
     * 401 a client re-mints for, a token that could lapse midway at hoglite.
     * Nothing runs: no run row, the catalog still there. A fresh token runs.
     */
    @Test
    fun `a long-running route refuses a token too short-lived for it before anything runs`() {
        val pair = TestTokens.keyPair()
        val id = "7c9e2a14-3f51-4b8d-9e60-2d5a1f8c44aa"
        val jwks = dir.resolve("public.json")
        Files.writeString(jwks, TestTokens.jwks(pair))
        HogliteConformanceEngine(
            listOf(
                "-auth-mode",
                "enforce",
                "-auth-issuer",
                TestTokens.ISSUER,
                "-auth-backend-id",
                id,
                "-auth-jwks-file",
                jwks.toString(),
            ),
        ).use { engine ->
            val backends = dir.resolve("backends.json")
            Files.writeString(
                backends,
                """{"backends":[{"id":"$id","kind":"hoglite","url":"${engine.url()}"}]}""",
            )
            val cfg =
                Config(
                    defaultBackend = "none",
                    backendsFile = backends.toString(),
                    authMode = "enforce",
                    jdbcUrl = "jdbc:no-driver:must-not-open",
                    dbUser = "",
                    dbPassword = "",
                    hogliteMinTokenLifetimeSeconds = 300,
                )
            val auth =
                AuthService(
                    AuthMode.ENFORCE,
                    JwtVerifier(
                        TestTokens.ISSUER,
                        "hoglake",
                        StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey),
                        backends = cfg.backendKinds,
                    ),
                )
            val now = Instant.now().epochSecond

            fun operator(exp: Long): String =
                TestTokens.mint(
                    pair,
                    """{"iss":"${TestTokens.ISSUER}","aud":["hoglake","hoglite"],"sub":"test-client",""" +
                        """"jti":"test-operator-$exp","ten":"operator","org":"org-not-the-tid",""" +
                        """"cat":{"lake":["admin"]},"iat":$now,"nbf":$now,"exp":$exp,""" +
                        """"rtg":{"b":"hoglite","be":"$id","t":"opaque_1"}}""",
                )
            val fresh = operator(now + 900)
            // Valid at hoglite for 120 + its leeway = 180 s, under the 300 s minimum.
            val short = operator(now + 120)
            testApplication {
                App.build(cfg).use { app ->
                    application { app.module(this, auth) }
                    assertThat(
                        client.post("/v1/catalogs") {
                            header(HttpHeaders.Authorization, "Bearer $fresh")
                            contentType(ContentType.Application.Json)
                            setBody("""{"name":"lake","data_path":"s3://hoglake-it/lake"}""")
                        }.status,
                    ).isEqualTo(HttpStatusCode.Created)

                    val json = com.fasterxml.jackson.module.kotlin.jacksonObjectMapper()

                    suspend fun expiryRuns(): Int {
                        val runs =
                            client.get("/v1/catalogs/lake/maintenance/runs?task=expiry") {
                                header(HttpHeaders.Authorization, "Bearer $fresh")
                            }
                        assertThat(runs.status).isEqualTo(HttpStatusCode.OK)
                        return json.readTree(runs.bodyAsText())["runs"].size()
                    }

                    val refused =
                        client.post("/v1/catalogs/lake/maintenance/expire") {
                            header(HttpHeaders.Authorization, "Bearer $short")
                        }
                    assertThat(refused.status).isEqualTo(HttpStatusCode.Unauthorized)
                    assertThat(refused.headers[HttpHeaders.WWWAuthenticate]).isEqualTo("Bearer error=\"token_expired\"")
                    assertThat(expiryRuns()).isZero()
                    // A short-lived token still serves a route that runs one transaction.
                    assertThat(
                        client.get("/v1/catalogs/lake") { header(HttpHeaders.Authorization, "Bearer $short") }.status,
                    ).isEqualTo(HttpStatusCode.OK)

                    assertThat(
                        client.post("/v1/catalogs/lake/maintenance/expire") {
                            header(HttpHeaders.Authorization, "Bearer $fresh")
                        }.status,
                    ).isEqualTo(HttpStatusCode.OK)
                    assertThat(expiryRuns()).isEqualTo(1)

                    val drop =
                        client.delete("/v1/catalogs/lake?message=drop") {
                            header(HttpHeaders.Authorization, "Bearer $short")
                        }
                    assertThat(drop.status).isEqualTo(HttpStatusCode.Unauthorized)
                    assertThat(drop.bodyAsText()).contains("\"token_expired\"")
                    assertThat(
                        client.get("/v1/catalogs/lake") { header(HttpHeaders.Authorization, "Bearer $fresh") }.status,
                    ).isEqualTo(HttpStatusCode.OK)
                    assertThat(
                        client.delete("/v1/catalogs/lake?message=drop") {
                            header(HttpHeaders.Authorization, "Bearer $fresh")
                        }.status,
                    ).isEqualTo(HttpStatusCode.OK)
                }
            }
        }
    }

    /**
     * A maintenance run swallows a failed best-effort step into a failure
     * count and answers 200. A token that lapses at hoglite after one of
     * the run's writes is not such a failure: the run partly applied, and
     * a 200 would hide that and its transaction (#680). The engine here
     * verifies nothing; the proxy in front answers 401, as an enforcing
     * one would, once the named write applied.
     */
    @Test
    fun `a maintenance run whose token lapses after a write answers an unknown outcome, never 200`() {
        val pair = TestTokens.keyPair()
        val id = "7c9e2a14-3f51-4b8d-9e60-2d5a1f8c44aa"
        HogliteConformanceEngine().use { engine ->
            LapsingProxy(engine.url()).use { proxy ->
                val backends = dir.resolve("backends.json")
                Files.writeString(
                    backends,
                    """{"backends":[{"id":"$id","kind":"hoglite","url":"${proxy.url()}"}]}""",
                )
                val cfg =
                    Config(
                        defaultBackend = "none",
                        backendsFile = backends.toString(),
                        authMode = "enforce",
                        jdbcUrl = "jdbc:no-driver:must-not-open",
                        dbUser = "",
                        dbPassword = "",
                    )
                val auth =
                    AuthService(
                        AuthMode.ENFORCE,
                        JwtVerifier(
                            TestTokens.ISSUER,
                            "hoglake",
                            StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey),
                            backends = cfg.backendKinds,
                        ),
                    )
                val now = Instant.now().epochSecond
                val operator =
                    TestTokens.mint(
                        pair,
                        """{"iss":"${TestTokens.ISSUER}","aud":["hoglake","hoglite"],"sub":"test-client",""" +
                            """"jti":"test-operator","ten":"operator","org":"org-not-the-tid",""" +
                            """"cat":{"lake":["admin"]},"iat":$now,"nbf":$now,"exp":${now + 900},""" +
                            """"rtg":{"b":"hoglite","be":"$id","t":"opaque_1"}}""",
                    )
                testApplication {
                    App.build(cfg).use { app ->
                        application { app.module(this, auth) }
                        assertThat(
                            client.post("/v1/catalogs") {
                                header(HttpHeaders.Authorization, "Bearer $operator")
                                contentType(ContentType.Application.Json)
                                setBody("""{"name":"lake","data_path":"s3://hoglake-it/lake"}""")
                            }.status,
                        ).isEqualTo(HttpStatusCode.Created)

                        // Expiry: phase A's first write, then the purge. Cleanup: the
                        // run-ledger purge, then the receipt and drained-ledger purges.
                        for ((task, marker) in listOf("expire" to "expiry-release", "cleanup" to "cleanup-ledger")) {
                            proxy.refuseAfter(marker)
                            val run =
                                client.post("/v1/catalogs/lake/maintenance/$task") {
                                    header(HttpHeaders.Authorization, "Bearer $operator")
                                }
                            assertThat(proxy.refused()).describedAs(task).isTrue()
                            assertThat(run.status).describedAs(task).isEqualTo(HttpStatusCode.ServiceUnavailable)
                            assertThat(run.bodyAsText()).describedAs(task)
                                .contains("\"commit_outcome_unknown\"", "request partly applied", "'$marker:")
                            proxy.pass()
                        }
                    }
                }
            }
        }
    }

    /**
     * A pass-through to [upstream] that answers every call 401 once a `/tx`
     * whose txid starts with the armed prefix applied: a token that lapsed
     * at hoglite right after that write.
     */
    private class LapsingProxy(private val upstream: String) : AutoCloseable {
        @Volatile private var prefix: String? = null

        @Volatile private var refusing = false

        private val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()

        private val server =
            HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
                createContext("/") { exchange ->
                    val body = exchange.requestBody.readBytes()
                    val (status, bytes) =
                        if (refusing) {
                            401 to """{"code":"refused","error":"request not authorized"}""".toByteArray()
                        } else {
                            val request =
                                HttpRequest.newBuilder(URI.create(upstream + exchange.requestURI)).method(
                                    exchange.requestMethod,
                                    HttpRequest.BodyPublishers.ofByteArray(body),
                                )
                            for (name in listOf("Content-Type", "Authorization")) {
                                exchange.requestHeaders.getFirst(name)?.let { request.header(name, it) }
                            }
                            val response = http.send(request.build(), HttpResponse.BodyHandlers.ofByteArray())
                            val armed = prefix
                            if (armed != null && response.statusCode() == 200 &&
                                exchange.requestURI.path.endsWith("/tx") &&
                                String(body).contains("\"txid\":\"$armed:")
                            ) {
                                refusing = true
                            }
                            response.statusCode() to response.body()
                        }
                    exchange.sendResponseHeaders(status, if (bytes.isEmpty()) -1 else bytes.size.toLong())
                    exchange.responseBody.use { it.write(bytes) }
                }
                start()
            }

        fun url(): String = "http://127.0.0.1:${server.address.port}"

        fun refuseAfter(txidPrefix: String) {
            refusing = false
            prefix = txidPrefix
        }

        fun refused(): Boolean = refusing

        fun pass() {
            prefix = null
            refusing = false
        }

        override fun close() {
            server.stop(0)
            http.close()
        }
    }

    private companion object {
        const val HOGLITED_LEEWAY_SECONDS = 60L

        /** Long enough for the write to land before the token lapses, short enough to wait out. */
        const val EXPIRING_LIFETIME_SECONDS = 15L

        /** Margin past the leeway for a hoglited clock behind the JVM's. */
        const val CLOCK_SKEW_SECONDS = 2L
    }
}
