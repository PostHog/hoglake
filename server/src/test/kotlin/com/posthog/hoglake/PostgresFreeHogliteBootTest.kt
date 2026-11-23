package com.posthog.hoglake

import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import com.sun.net.httpserver.HttpServer
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.net.InetSocketAddress
import java.nio.file.Files
import java.nio.file.Path
import java.security.interfaces.ECPublicKey
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class PostgresFreeHogliteBootTest {
    @TempDir
    lateinit var dir: Path

    @Test
    fun `hoglite routes and readiness do not open PG or start its maintenance loops`() {
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        val healthy = AtomicInteger(200)
        val stallMs = AtomicLong()
        val probes = AtomicInteger()
        val forwardedAuthority = AtomicReference<String?>()
        server.createContext("/v1/healthz") { exchange ->
            probes.incrementAndGet()
            Thread.sleep(stallMs.get())
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
                    hogliteHealthIntervalMs = 100,
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

                        fun series(name: String): Double? =
                            app.meterRegistry.scrape().lineSequence()
                                .firstOrNull { it.startsWith("$name{backend=\"$id\"}") }
                                ?.substringAfterLast(' ')?.toDouble()

                        fun reachable() = series("hoglake_hoglite_backend_reachable")

                        assertThat(client.get("/healthz").status).isEqualTo(HttpStatusCode.OK)
                        // The hoglite_health loop, not the readiness call, probes the
                        // backend: startBackground started it without PostgreSQL.
                        await().atMost(Duration.ofSeconds(10)).until { probes.get() >= 1 && reachable() == 1.0 }
                        assertThat(series("hoglake_hoglite_health_probe_attempts_hung")).isEqualTo(0.0)
                        val listed =
                            client.get("/v1/catalogs") {
                                header(HttpHeaders.Authorization, "Bearer $token")
                            }
                        assertThat(listed.status).isEqualTo(HttpStatusCode.OK)
                        assertThat(forwardedAuthority.get() == "Bearer $token").isTrue()
                        assertThat(listed.bodyAsText()).contains("s3://bucket/lake")
                        val maintenance =
                            client.get("/v1/maintenance/status") {
                                header(HttpHeaders.Authorization, "Bearer $token")
                            }
                        assertThat(maintenance.status).isEqualTo(HttpStatusCode.NotImplemented)
                        assertThat(maintenance.bodyAsText()).contains("not_supported_on_backend")
                        assertThat(leftover).exists()
                        assertThat(app.healthProbe.outstandingAttempts).isZero()
                        assertThat(app.meterRegistry.scrape()).doesNotContain("hoglake_db_pool", "hoglake_catalog_")
                        // #678: an unhealthy hoglite backend (hoglited answers 503
                        // through a lease handover) is a per-backend series, not
                        // pod unreadiness — one backend every pod shares would
                        // otherwise make the whole fleet unready at once.
                        //
                        // MUTATION: restore the hoglite term to `/healthz`'s
                        // condition in `App.module` and this is a 503.
                        healthy.set(503)
                        await().atMost(Duration.ofSeconds(10)).until { reachable() == 0.0 }
                        assertThat(client.get("/healthz").status).isEqualTo(HttpStatusCode.OK)
                        // And a backend that stalls does not stall the probe: the
                        // kubelet's readiness timeout (3 s in the chart) is below one
                        // backend's bound, so waiting would be unreadiness by timeout.
                        //
                        // MUTATION: have `/healthz` await `catalogSelector.hogliteReachable`
                        // and the elapsed assertion reds by the stall.
                        stallMs.set(3_000)
                        val sampler = app.hogliteBackendHealth
                        await().atMost(Duration.ofSeconds(10)).until { sampler.outstandingAttempts == 1 }
                        val started = System.nanoTime()
                        assertThat(client.get("/healthz").status).isEqualTo(HttpStatusCode.OK)
                        assertThat((System.nanoTime() - started) / 1_000_000).isLessThan(1_000)
                        // Single flight: the loop ticks at 100 ms, the stalled attempt is
                        // inside its 4 s deadline, so no tick launched a second one.
                        assertThat(sampler.outstandingAttempts).isEqualTo(1)
                    }
                }
            }
        } finally {
            server.stop(0)
        }
    }
}
