package com.posthog.hoglake.observability

import com.posthog.hoglake.Config
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.testApplication
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * The infra listener (docs/auth-tenancy.md §6.7): the scrape with no token,
 * and nothing else.
 */
class MetricsListenerTest {
    @Test
    fun `the scrape needs no token and serves the shared registry`() =
        testApplication {
            val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
            registry.counter("hoglake_listener_probe_total").increment()
            application { metricsListener(registry) }

            val response = client.get("/metrics")

            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            assertThat(response.bodyAsText()).contains("hoglake_listener_probe_total")
        }

    @Test
    fun `nothing but the scrape is mounted`() =
        testApplication {
            // MUTATION: mount App.module here and these stop 404ing; the
            // API would then be reachable around the auth plugin.
            application { metricsListener(PrometheusMeterRegistry(PrometheusConfig.DEFAULT)) }

            for (path in listOf("/v1/catalogs", "/healthz", "/v1/debug/threads", "/")) {
                assertThat(client.get(path).status).describedAs(path).isEqualTo(HttpStatusCode.NotFound)
            }
        }

    @Test
    fun `the listener is off by default and cannot share the API port`() {
        assertThat(Config().metricsPort).isZero()
        assertThatThrownBy { Config(port = 8080, metricsPort = 8080) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_METRICS_PORT")
        assertThatThrownBy { Config(metricsPort = 70000) }
            .isInstanceOf(IllegalArgumentException::class.java)
        assertThat(Config(port = 8080, metricsPort = 8081).metricsPort).isEqualTo(8081)
    }
}
