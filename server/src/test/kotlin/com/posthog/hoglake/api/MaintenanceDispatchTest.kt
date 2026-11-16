package com.posthog.hoglake.api

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.request
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.security.interfaces.ECPublicKey

/** Backend refusal must precede body parsing, query parsing, and PostgreSQL construction. */
class MaintenanceDispatchTest {
    @TempDir
    lateinit var dir: Path

    @Test
    fun `retirement trigger remains absent without auth or PostgreSQL`() {
        App.build(
            Config(defaultBackend = "none", authMode = "off", jdbcUrl = "jdbc:no-such-driver:must-not-open"),
        ).use {
                app ->
            testApplication {
                application { app.module(this) }
                val response = client.request("/v1/catalogs/lake/maintenance/retire") { method = HttpMethod.Post }
                assertThat(response.status).isEqualTo(HttpStatusCode.NotFound)
            }
        }
    }

    @Test
    fun `fleet routes reject a deployment without PostgreSQL before initialization or query parsing`() {
        App.build(
            Config(defaultBackend = "none", authMode = "off", jdbcUrl = "jdbc:no-such-driver:must-not-open"),
        ).use { app ->
            testApplication {
                application { app.module(this) }
                for (path in listOf(
                    "/v1/maintenance/status?limit=bad",
                    "/v1/maintenance/runs?task=bad",
                    "/v1/database/health",
                )) {
                    val response = client.get(path)
                    assertThat(response.status).describedAs(path).isEqualTo(HttpStatusCode.NotImplemented)
                    assertThat(response.bodyAsText()).contains("not_supported_on_backend")
                    assertThat(response.headers[HttpHeaders.RetryAfter]).isNull()
                }
            }
        }
    }

    @Test
    fun `fleet routes retain unavailable errors for configured PostgreSQL`() {
        App.build(
            Config(
                defaultBackend = "none",
                authMode = "off",
                postgresBackendId = "5b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e",
                jdbcUrl = "jdbc:no-such-driver:configured",
                dbUser = "test",
                dbPassword = "test",
            ),
        ).use { app ->
            testApplication {
                application { app.module(this) }
                for (path in listOf("/v1/maintenance/status", "/v1/maintenance/runs", "/v1/database/health")) {
                    val response = client.get(path)
                    assertThat(response.status).describedAs(path).isEqualTo(HttpStatusCode.ServiceUnavailable)
                    assertThat(response.bodyAsText()).contains("backend_unavailable")
                }
            }
        }
    }

    @Test
    fun `maintenance routes resolve placement before malformed input or PostgreSQL`() {
        val id = "5b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e"
        val backends = dir.resolve("backends.json")
        Files.writeString(backends, """{"backends":[{"id":"$id","kind":"hoglite","url":"http://localhost:1"}]}""")
        val cfg =
            Config(
                defaultBackend = "none",
                backendsFile = backends.toString(),
                authMode = "enforce",
                jdbcUrl = "jdbc:no-such-driver:must-not-open",
                dbUser = "",
                dbPassword = "",
            )
        val pair = TestTokens.keyPair()
        val auth =
            AuthService(
                AuthMode.ENFORCE,
                JwtVerifier(
                    TestTokens.ISSUER,
                    TestTokens.AUDIENCE,
                    StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey),
                    backends = cfg.backendKinds,
                ),
            )
        val token =
            TestTokens.mint(
                pair,
                TestTokens.operatorClaims().dropLast(1) +
                    """, "cat":{"lake":["admin"]}, "rtg":{"b":"hoglite","be":"$id","t":"opaque_1"}}""",
            )
        val probes =
            listOf(
                HttpMethod.Get to "/maintenance/runs?before=bad",
            )
        App.build(cfg).use { app ->
            testApplication {
                application { app.module(this, auth) }
                for ((catalog, status, code) in listOf(
                    Triple("lake", HttpStatusCode.BadRequest, "bad_request"),
                    Triple("other", HttpStatusCode.NotFound, "not_found"),
                )) {
                    for ((method, suffix) in probes) {
                        val response =
                            client.request("/v1/catalogs/$catalog$suffix") {
                                this.method = method
                                header(HttpHeaders.Authorization, "Bearer $token")
                                if (method == HttpMethod.Patch) {
                                    contentType(ContentType.Application.Json)
                                    setBody("{")
                                }
                            }
                        assertThat(response.status).describedAs("$method $catalog$suffix").isEqualTo(status)
                        assertThat(response.bodyAsText()).contains("\"error\":\"$code\"")
                        assertThat(response.headers[HttpHeaders.RetryAfter]).isNull()
                    }
                }
                val otherStatus =
                    client.request("/v1/catalogs/other/maintenance/status") {
                        header(HttpHeaders.Authorization, "Bearer $token")
                    }
                assertThat(otherStatus.status).isEqualTo(HttpStatusCode.NotFound)
                val export =
                    client.request("/v1/catalogs/lake/export") {
                        header(HttpHeaders.Authorization, "Bearer $token")
                    }
                assertThat(export.status).isEqualTo(HttpStatusCode.NotImplemented)
                assertThat(export.bodyAsText()).contains("not_supported_on_backend")
                for (operation in listOf("compact", "expire", "cleanup")) {
                    for ((catalog, status) in listOf(
                        "lake" to HttpStatusCode.BadRequest,
                        "other" to HttpStatusCode.NotFound,
                    )) {
                        val response =
                            client.request("/v1/catalogs/$catalog/maintenance/$operation?batch=bad") {
                                method = HttpMethod.Post
                                header(HttpHeaders.Authorization, "Bearer $token")
                            }
                        assertThat(response.status).describedAs("$catalog $operation").isEqualTo(status)
                        assertThat(response.headers[HttpHeaders.RetryAfter]).isNull()
                    }
                }
                val malformedOptions =
                    client.request("/v1/catalogs/lake/options") {
                        method = HttpMethod.Patch
                        header(HttpHeaders.Authorization, "Bearer $token")
                        contentType(ContentType.Application.Json)
                        setBody("{")
                    }
                assertThat(malformedOptions.status).isEqualTo(HttpStatusCode.BadRequest)
                val otherOptions =
                    client.request("/v1/catalogs/other/options") {
                        method = HttpMethod.Patch
                        header(HttpHeaders.Authorization, "Bearer $token")
                        contentType(ContentType.Application.Json)
                        setBody("{")
                    }
                assertThat(otherOptions.status).isEqualTo(HttpStatusCode.NotFound)
            }
        }
    }
}
