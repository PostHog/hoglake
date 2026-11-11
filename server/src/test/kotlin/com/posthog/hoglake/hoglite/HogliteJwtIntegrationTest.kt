package com.posthog.hoglake.hoglite

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
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
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
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
}
