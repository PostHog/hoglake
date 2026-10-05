package com.posthog.hoglake

import com.posthog.hoglake.observability.Metrics
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.sql.SQLException

/**
 * The bloat gauge's refresh rides the metrics loop's tick on the reindex
 * pod. Unit, with a database that refuses every connection: a failed
 * sample must not skip the refresh, and a failed refresh must count on
 * its own series rather than throw into the loop.
 */
class ReindexGaugeTickTest {
    @AfterEach
    fun unbind() = Metrics.clear()

    private fun app(reindexIntervalMs: Long): App =
        App.build(
            Config(reindexIntervalMs = reindexIntervalMs),
            Jdbi.create { throw SQLException("no database in this test") },
        )

    private fun refreshFailures(app: App): Double =
        app.meterRegistry.find("hoglake_reindex_gauge_refresh_failures_total").counter()?.count() ?: 0.0

    @Test
    fun `a failed sample still refreshes the gauge, and the refresh's failure is its own`() {
        val app = app(reindexIntervalMs = 300_000)
        // The SAMPLE's failure is the tick's (the loop counts it); the
        // refresh's is not — it would otherwise replace the sample's.
        assertThatThrownBy { app.metricsTick() }.hasMessageContaining("no database")
        // MUTATION: call refreshGauge after sampleOnce instead of in a
        // `finally` and it never runs here; drop its runCatching and the
        // refresh's exception replaces the sample's. Both red this.
        assertThat(refreshFailures(app)).isEqualTo(1.0)
        app.close()
    }

    @Test
    fun `an API pod, where the loop is off, never refreshes`() {
        val app = app(reindexIntervalMs = 0)
        assertThatThrownBy { app.metricsTick() }.hasMessageContaining("no database")
        assertThat(refreshFailures(app)).isZero()
        app.close()
    }
}
