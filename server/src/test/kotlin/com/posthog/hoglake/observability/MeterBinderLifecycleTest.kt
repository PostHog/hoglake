package com.posthog.hoglake.observability

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import io.ktor.client.request.get
import io.ktor.server.application.Application
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.lang.ref.WeakReference

/** The API module's JVM meter binders end with its application. */
class MeterBinderLifecycleTest {
    @Test
    fun `a stopped application is not kept reachable by its GC metrics listener`() {
        val stopped = serveOnce()
        // Metrics.bind holds the LATEST registry, and with it that application: a second one releases the first.
        serveOnce()
        // MUTATION: drop the ApplicationStopped close in App.module and JvmGcMetrics'
        // listener on the GC MXBeans keeps the application, and everything it holds.
        val collected =
            (1..20).any {
                System.gc()
                Thread.sleep(50)
                stopped.get() == null
            }
        assertThat(collected).isTrue()
    }

    /** One request through the API module, then a stopped application, held only weakly. */
    private fun serveOnce(): WeakReference<Application> {
        var ref: WeakReference<Application>? = null
        App.build(
            Config(defaultBackend = "none", authMode = "off", jdbcUrl = "jdbc:no-such-driver:must-not-open"),
        ).use { app ->
            testApplication {
                application {
                    ref = WeakReference(this)
                    app.module(this)
                }
                client.get("/healthz")
            }
        }
        return ref!!
    }
}
