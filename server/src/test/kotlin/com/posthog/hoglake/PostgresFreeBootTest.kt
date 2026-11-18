package com.posthog.hoglake

import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.security.interfaces.ECPublicKey

class PostgresFreeBootTest {
    private val pair = TestTokens.keyPair()
    private val postgresId = "6b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e"

    private fun auth(cfg: Config) =
        AuthService(
            AuthMode.ENFORCE,
            JwtVerifier(
                TestTokens.ISSUER,
                TestTokens.AUDIENCE,
                StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey),
                backends = cfg.backendKinds,
            ),
        )

    private fun token() =
        TestTokens.mint(
            pair,
            TestTokens.operatorClaims().dropLast(1) +
                """, "cat":{"lake":["admin"]}, "rtg":{"b":"postgres","be":"$postgresId"}}""",
        )

    @Test
    fun `environment backed boot needs no PG settings`() {
        val javaExecutable = System.getProperty("java.home") + "/bin/java"
        val builder =
            ProcessBuilder(
                javaExecutable,
                "-cp",
                System.getProperty("java.class.path"),
                PostgresFreeBootProbe::class.java.name,
            ).redirectErrorStream(true)
        builder.environment().keys.removeIf { it.startsWith("HOGLAKE_") }
        builder.environment()["HOGLAKE_DEFAULT_BACKEND"] = "none"
        val process = builder.start()
        assertThat(process.waitFor(20, java.util.concurrent.TimeUnit.SECONDS)).isTrue()
        val output = process.inputStream.bufferedReader().readText()
        assertThat(process.exitValue()).describedAs(output).isZero()
        assertThat(output).contains("BOOTED_WITHOUT_POSTGRES").doesNotContain("HikariPool", "Migrating schema")
    }

    @Test
    fun `boot and probes without PG settings create no pool or database samplers`() {
        for (url in listOf("", "jdbc:no-such-driver:must-not-open")) {
            testApplication {
                val cfg =
                    Config(
                        defaultBackend = "none",
                        jdbcUrl = url,
                        dbUser = "",
                        dbPassword = "",
                        // Positive intervals must not start a sampler or a pool.
                        metricsIntervalMs = 1,
                        expiryIntervalMs = 1,
                        hydratorIntervalMs = 1,
                    )
                App.build(cfg).use { app ->
                    app.startBackground().use {
                        application { app.module(this, auth(cfg)) }
                        repeat(2) {
                            assertThat(client.get("/livez").status).isEqualTo(HttpStatusCode.OK)
                            assertThat(client.get("/healthz").status).isEqualTo(HttpStatusCode.OK)
                        }
                        assertThat(app.healthProbe.outstandingAttempts).isZero()
                        assertThat(app.meterRegistry.scrape()).doesNotContain("hoglake_db_pool", "hoglake_catalog_")
                        val refused =
                            client.get("/v1/catalogs/lake") {
                                header(HttpHeaders.Authorization, "Bearer ${token()}")
                            }
                        assertThat(refused.status).isEqualTo(HttpStatusCode.Unauthorized)
                        assertThat(refused.bodyAsText()).contains("unknown_backend")
                    }
                }
            }
        }
    }

    @Test
    fun `configured PG requires connection settings at boot`() {
        val base = Config(defaultBackend = "none", jdbcUrl = "", dbUser = "", dbPassword = "")
        assertThatThrownBy { base.copy(postgresBackendId = postgresId) }.hasMessageContaining("HOGLAKE_JDBC_URL")
        assertThatThrownBy { base.copy(postgresBackendId = postgresId, jdbcUrl = "jdbc:postgresql://localhost/lake") }
            .hasMessageContaining("HOGLAKE_DB_USER")
        assertThatThrownBy {
            base.copy(postgresBackendId = postgresId, jdbcUrl = "jdbc:postgresql://localhost/lake", dbUser = "hoglake")
        }.hasMessageContaining("HOGLAKE_DB_PASSWORD")
    }

    @Test
    fun `an unavailable lazy PG backend returns 503 but health does not open it`() =
        testApplication {
            val cfg =
                Config(
                    defaultBackend = "none",
                    postgresBackendId = postgresId,
                    jdbcUrl = "jdbc:postgresql://127.0.0.1:1/unavailable?connectTimeout=1",
                    dbUser = "user",
                    dbPassword = "password",
                )
            App.build(cfg).use { app ->
                app.startBackground().use {
                    application { app.module(this, auth(cfg)) }
                    assertThat(client.get("/healthz").status).isEqualTo(HttpStatusCode.OK)
                    val response =
                        client.get("/v1/catalogs/lake") {
                            header(HttpHeaders.Authorization, "Bearer ${token()}")
                        }
                    assertThat(response.status).isEqualTo(HttpStatusCode.ServiceUnavailable)
                    assertThat(response.bodyAsText()).contains("backend_unavailable")
                    assertThat(client.get("/healthz").status).isEqualTo(HttpStatusCode.OK)
                    assertThat(app.healthProbe.outstandingAttempts).isZero()
                    assertThat(app.meterRegistry.scrape()).doesNotContain("hoglake_db_pool")
                }
            }
        }
}
