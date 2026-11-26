package com.posthog.hoglake.hoglite

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import io.ktor.client.request.get
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.security.interfaces.ECPublicKey
import java.time.Duration

/**
 * Readiness across a real hoglited's outage (#678): a hoglite-only
 * process stays ready while its backend is up, stays ready once the
 * backend is gone, and says which backend is gone in
 * `hoglake_hoglite_backend_reachable` instead. The live counterpart of
 * `PostgresFreeHogliteBootTest`, whose backend is a stub.
 */
@Tag("integration")
class HogliteBackendHealthIntegrationTest {
    @TempDir
    lateinit var dir: Path

    /**
     * MUTATION: restore the hoglite term to `/healthz`'s condition in
     * `App.module` and the second status assertion reds with a 503 — the
     * fleet-wide unreadiness the issue describes.
     */
    @Test
    fun `readiness stays 200 across a hoglited outage and the backend series reports it`() {
        val engine = HogliteConformanceEngine()
        var engineClosed = false
        try {
            val id = "6c1d7e1f-2a3b-4c4d-8e5f-6a7b8c9d0e1f"
            val backends = dir.resolve("backends.json")
            Files.writeString(backends, """{"backends":[{"id":"$id","kind":"hoglite","url":"${engine.url()}"}]}""")
            val cfg =
                Config(
                    defaultBackend = "none", backendsFile = backends.toString(), authMode = "enforce",
                    jdbcUrl = "jdbc:no-such-driver:must-not-open", dbUser = "", dbPassword = "",
                    metricsIntervalMs = 1, compactionIntervalMs = 60_000, compactionSpillDir = dir.toString(),
                    hogliteHealthIntervalMs = 100,
                )
            val auth =
                AuthService(
                    AuthMode.ENFORCE,
                    JwtVerifier(
                        TestTokens.ISSUER,
                        TestTokens.AUDIENCE,
                        StaticSigningKeys(TestTokens.KID, TestTokens.keyPair().public as ECPublicKey),
                        backends = cfg.backendKinds,
                    ),
                )
            testApplication {
                App.build(cfg).use { app ->
                    app.startBackground().use {
                        application { app.module(this, auth) }

                        fun series(): Double? =
                            app.meterRegistry.scrape().lineSequence()
                                .firstOrNull { it.startsWith("hoglake_hoglite_backend_reachable{backend=\"$id\"}") }
                                ?.substringAfterLast(' ')?.toDouble()

                        assertThat(client.get("/healthz").status).isEqualTo(HttpStatusCode.OK)
                        // The hoglite_health loop probes; the readiness call above did not.
                        await().atMost(Duration.ofSeconds(10)).until { series() == 1.0 }

                        engine.close()
                        engineClosed = true

                        await().atMost(Duration.ofSeconds(10)).until { series() == 0.0 }
                        val started = System.nanoTime()
                        val probe = client.get("/healthz")
                        val elapsedMs = (System.nanoTime() - started) / 1_000_000
                        assertThat(probe.status).isEqualTo(HttpStatusCode.OK)
                        assertThat(elapsedMs).isLessThan(1_000)
                        assertThat(client.get("/livez").status).isEqualTo(HttpStatusCode.OK)
                    }
                }
            }
        } finally {
            if (!engineClosed) engine.close()
        }
    }
}
