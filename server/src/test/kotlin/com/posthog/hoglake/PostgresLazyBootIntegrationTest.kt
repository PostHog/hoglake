package com.posthog.hoglake

import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
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
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.PostgreSQLContainer
import java.security.interfaces.ECPublicKey
import java.sql.DriverManager

@Tag("integration")
class PostgresLazyBootIntegrationTest {
    @Test
    fun `the PostgreSQL default still migrates before serving requests`() {
        PostgreSQLContainer(PgTestSupport.image)
            .withUsername(PgTestSupport.USER)
            .withPassword(PgTestSupport.PASSWORD)
            .use { postgres ->
                postgres.start()
                val cfg =
                    Config(
                        jdbcUrl = postgres.jdbcUrl,
                        dbUser = postgres.username,
                        dbPassword = postgres.password,
                        hydratorIntervalMs = 0,
                        expiryIntervalMs = 0,
                    )
                App.build(cfg).use { app ->
                    val migrated =
                        app.jdbi.withHandle<Boolean, Exception> { handle ->
                            handle.createQuery("SELECT to_regclass('public.flyway_schema_history') IS NOT NULL")
                                .mapTo(Boolean::class.java).one()
                        }
                    assertThat(migrated).isTrue()
                    assertThat(app.meterRegistry.find("hoglake_db_pool_active").gauge()).isNotNull()
                    testApplication {
                        app.startBackground().use {
                            application { app.module(this) }
                            assertThat(client.get("/healthz").status).isEqualTo(HttpStatusCode.OK)
                        }
                    }
                }
            }
    }

    @Test
    fun `first routed request migrates PG and starts samplers while later requests reuse the pool`() {
        PostgreSQLContainer(PgTestSupport.image)
            .withUsername(PgTestSupport.USER)
            .withPassword(PgTestSupport.PASSWORD)
            .use { postgres ->
                postgres.start()
                val postgresId = "6b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e"
                val cfg =
                    Config(
                        defaultBackend = "none",
                        postgresBackendId = postgresId,
                        jdbcUrl = postgres.jdbcUrl,
                        dbUser = postgres.username,
                        dbPassword = postgres.password,
                        metricsIntervalMs = 1_000,
                        expiryIntervalMs = 0,
                        hydratorIntervalMs = 0,
                    )

                fun hasMigrations(): Boolean =
                    DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { conn ->
                        conn.createStatement().use { stmt ->
                            stmt.executeQuery("SELECT to_regclass('public.flyway_schema_history') IS NOT NULL").use {
                                it.next()
                                it.getBoolean(1)
                            }
                        }
                    }
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
                val claims =
                    TestTokens.operatorClaims().dropLast(1) +
                        """, "cat":{"lake":["admin"]}, "rtg":{"b":"postgres","be":"$postgresId"}}"""
                val token = TestTokens.mint(pair, claims)
                testApplication {
                    App.build(cfg).use { app ->
                        app.startBackground().use { background ->
                            application { app.module(this, auth) }
                            assertThat(hasMigrations()).isFalse()
                            assertThat(client.get("/healthz").status).isEqualTo(HttpStatusCode.OK)
                            assertThat(hasMigrations()).isFalse()
                            assertThat(app.meterRegistry.scrape()).doesNotContain("hoglake_db_pool")
                            val created =
                                client.post("/v1/catalogs") {
                                    header(HttpHeaders.Authorization, "Bearer $token")
                                    contentType(ContentType.Application.Json)
                                    setBody("""{"name":"lake","data_path":"s3://bucket/lake"}""")
                                }
                            assertThat(created.status).isEqualTo(HttpStatusCode.Created)
                            assertThat(hasMigrations()).isTrue()
                            assertThat(app.meterRegistry.find("hoglake_db_pool_active").gauge()).isNotNull()
                            val jdbi = app.jdbi
                            val found =
                                client.get("/v1/catalogs/lake") {
                                    header(HttpHeaders.Authorization, "Bearer $token")
                                }
                            assertThat(found.status).isEqualTo(HttpStatusCode.OK)
                            assertThat(found.bodyAsText()).contains("s3://bucket/lake")
                            assertThat(app.jdbi).isSameAs(jdbi)
                            kotlinx.coroutines.withTimeout(10_000) {
                                while (app.catalogMetrics.latestTotals == null) kotlinx.coroutines.delay(10)
                            }
                            val namespace =
                                client.post("/v1/catalogs/lake/namespaces") {
                                    header(HttpHeaders.Authorization, "Bearer $token")
                                    contentType(ContentType.Application.Json)
                                    setBody("""{"name":"default"}""")
                                }
                            assertThat(namespace.status).isEqualTo(HttpStatusCode.Created)
                            // A sub-service starts on this request, not at module installation.
                            val namespaces =
                                client.get("/v1/catalogs/lake/namespaces/default/views") {
                                    header(HttpHeaders.Authorization, "Bearer $token")
                                }
                            assertThat(namespaces.status).isEqualTo(HttpStatusCode.OK)
                            assertThat(client.get("/healthz").status).isEqualTo(HttpStatusCode.OK)
                            background.close()
                            postgres.stop()
                            assertThat(client.get("/healthz").status).isEqualTo(HttpStatusCode.ServiceUnavailable)
                        }
                    }
                }
            }
    }
}
