package com.posthog.hoglake

import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import com.sun.net.httpserver.HttpServer
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.net.InetSocketAddress
import java.nio.file.Files
import java.nio.file.Path
import java.security.interfaces.ECPublicKey
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class PostgresFreeHogliteBootTest {
    @TempDir
    lateinit var dir: Path

    @Test
    fun `hoglite routes and readiness do not open PG or start its maintenance loops`() {
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        val healthy = AtomicInteger(200)
        val probes = AtomicInteger()
        val forwardedAuthority = AtomicReference<String?>()
        server.createContext("/v1/healthz") { exchange ->
            probes.incrementAndGet()
            exchange.sendResponseHeaders(healthy.get(), -1)
            exchange.close()
        }
        server.createContext("/v1/query-batch") { exchange ->
            forwardedAuthority.set(exchange.requestHeaders.getFirst("Authorization"))
            exchange.requestBody.use { it.readBytes() }
            val answer =
                (
                    """{"results":[{"status":200,"body":{"cols":[],"rows":[""" +
                        """["lake","s3://bucket/lake",7,2,0,null,null,1]],"affected":0}}]}"""
                ).toByteArray()
            exchange.sendResponseHeaders(200, answer.size.toLong())
            exchange.responseBody.use { it.write(answer) }
        }
        server.start()
        try {
            val id = "5b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e"
            val backends = dir.resolve("backends.json")
            Files.writeString(
                backends,
                """{"backends":[{"id":"$id","kind":"hoglite","url":"http://127.0.0.1:${server.address.port}"}]}""",
            )
            val leftover = Files.createDirectory(dir.resolve("hoglake-compaction-spill-old"))
            val cfg =
                Config(
                    defaultBackend = "none", backendsFile = backends.toString(), authMode = "enforce",
                    jdbcUrl = "jdbc:no-such-driver:must-not-open", dbUser = "", dbPassword = "",
                    metricsIntervalMs = 1, compactionIntervalMs = 60_000, compactionSpillDir = dir.toString(),
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
            val claims =
                TestTokens.operatorClaims().dropLast(1) +
                    """, "cat":{"lake":["admin"]}, "rtg":{"b":"hoglite","be":"$id","t":"opaque_1"}}"""
            val token = TestTokens.mint(pair, claims)
            testApplication {
                App.build(cfg).use { app ->
                    app.startBackground().use {
                        application { app.module(this, auth) }
                        assertThat(client.get("/healthz").status).isEqualTo(HttpStatusCode.OK)
                        assertThat(probes.get()).isEqualTo(1)
                        val listed =
                            client.get("/v1/catalogs") {
                                header(HttpHeaders.Authorization, "Bearer $token")
                            }
                        assertThat(listed.status).isEqualTo(HttpStatusCode.OK)
                        assertThat(forwardedAuthority.get() == "Bearer $token").isTrue()
                        assertThat(listed.bodyAsText()).contains("s3://bucket/lake")
                        val maintenance =
                            client.post("/v1/catalogs/lake/maintenance/cleanup") {
                                header(HttpHeaders.Authorization, "Bearer $token")
                            }
                        assertThat(maintenance.status).isEqualTo(HttpStatusCode.NotImplemented)
                        assertThat(maintenance.bodyAsText()).contains("not_supported_on_backend")
                        assertThat(leftover).exists()
                        assertThat(app.healthProbe.outstandingAttempts).isZero()
                        assertThat(app.meterRegistry.scrape()).doesNotContain("hoglake_db_pool", "hoglake_catalog_")
                        healthy.set(503)
                        assertThat(client.get("/healthz").status).isEqualTo(HttpStatusCode.ServiceUnavailable)
                        assertThat(probes.get()).isEqualTo(2)
                    }
                }
            }
        } finally {
            server.stop(0)
        }
    }
}
