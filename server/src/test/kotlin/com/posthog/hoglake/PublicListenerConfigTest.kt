package com.posthog.hoglake

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class PublicListenerConfigTest {
    @Test
    fun `the public listener is off by default`() {
        assertThat(Config().publicPort).isZero()
    }

    @Test
    fun `it shares no port with the API or the infra listener`() {
        for (cfg in listOf(
            { Config(port = 8080, publicPort = 8080, authMode = "warn") },
            { Config(port = 8080, metricsPort = 9090, publicPort = 9090, authMode = "warn") },
            { Config(publicPort = 70000, authMode = "warn") },
        )) {
            assertThatThrownBy { cfg() }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("HOGLAKE_PUBLIC_PORT")
        }
    }

    @Test
    fun `it needs an auth mode that has a verifier`() {
        assertThatThrownBy { Config(port = 8080, publicPort = 8443, authMode = "off") }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_AUTH_MODE")
        assertThat(
            Config(port = 8080, metricsPort = 9090, publicPort = 8443, authMode = "warn").publicPort,
        ).isEqualTo(8443)
    }
}
