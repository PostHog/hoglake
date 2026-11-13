package com.posthog.hoglake.api

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
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
    fun `unsupported maintenance routes refuse hoglite before PostgreSQL or malformed input`() {
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
                HttpMethod.Post to "/maintenance/expire?batch=bad",
                HttpMethod.Post to "/maintenance/cleanup?batch=bad",
                HttpMethod.Post to "/maintenance/compact?batch=bad",
                HttpMethod.Post to "/maintenance/rehydrate?namespace=ns",
                HttpMethod.Get to "/maintenance/status",
                HttpMethod.Get to "/maintenance/runs?before=bad&task=bad&limit=bad",
            )
        App.build(cfg).use { app ->
            testApplication {
                application { app.module(this, auth) }
                for ((catalog, status, code) in listOf(
                    Triple("lake", HttpStatusCode.NotImplemented, "not_supported_on_backend"),
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
