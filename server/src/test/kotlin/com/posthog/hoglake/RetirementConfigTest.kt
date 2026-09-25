package com.posthog.hoglake

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * The two ways retirement can be configured into SILENCE, refused where
 * an operator is already reading logs.
 *
 * A background loop that is OFF is fine and says so — the status
 * endpoint reports `loop_interval_ms = 0` and the ledger holds no run
 * rows. A loop that is ON and can never do anything is the failure this
 * guards: the run ledger fills with rows, the metrics stay flat, and
 * every check is green, because nothing in the system distinguishes
 * "swept and found nothing" from "cannot sweep".
 *
 * A unit test, deliberately: this is a fact about constants and a
 * `require` block, so it reds in the fast lane with no Docker.
 */
class RetirementConfigTest {
    @Test
    fun `a zero queue ceiling with the loop ON is refused at boot, naming both knobs`() {
        // `count(*) > 0` is true of any catalog holding a single
        // undrained removal row, which a live catalog always does — so
        // a ceiling of 0 means every run skips on the cleanup-queue
        // check, forever. It is not "no pacing", it is "never run".
        //
        // MUTATION: delete the `require` from Config's init and this
        // reds — the Config constructs happily and retirement is
        // silently dead.
        assertThatThrownBy { Config(retirementIntervalMs = 60_000, retirementQueueCeiling = 0) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_RETIREMENT_QUEUE_CEILING")
            .hasMessageContaining("HOGLAKE_RETIREMENT_INTERVAL_MS")

        // THE MISTAKE IS A PAIR, which is why the refusal is
        // conditional rather than a blanket `> 0`. A ceiling of 0 while
        // the loop is off is harmless — the loop never runs — and
        // refusing it would fail the boot of every API replica, which
        // runs exactly that configuration.
        Config(retirementIntervalMs = 0, retirementQueueCeiling = 0)
        // And the legal combinations stay legal.
        Config(retirementIntervalMs = 60_000, retirementQueueCeiling = 1)
        Config(retirementIntervalMs = 60_000)
    }

    @Test
    fun `the refusal quotes the default rather than restating it`() {
        // The message tells an operator what to set. If it carried a
        // literal, the literal and the property would drift and the
        // advice would start being wrong.
        val message =
            runCatching { Config(retirementIntervalMs = 1, retirementQueueCeiling = 0) }
                .exceptionOrNull()!!
                .message!!
        assertThat(message).contains(Config.DEFAULT_RETIREMENT_QUEUE_CEILING.toString())
        assertThat(Config().retirementQueueCeiling)
            .describedAs("the property and the constant the message quotes must be the same number")
            .isEqualTo(Config.DEFAULT_RETIREMENT_QUEUE_CEILING)
    }

    @Test
    fun `the API replicas' own configuration boots`() {
        // The shape the chart gives every pod that is NOT the
        // maintenance workload: every heavy loop off, retirement
        // included. A guard that broke this would take the whole fleet
        // down at boot, which is worse than what it guards against.
        val api =
            Config(
                retirementIntervalMs = 0,
                compactionIntervalMs = 0,
                verifyIntervalMs = 0,
            )
        assertThat(api.retirementIntervalMs).isZero()
        assertThat(api.retirementQueueCeiling)
            .describedAs("the ceiling keeps its default even where the loop is off")
            .isEqualTo(Config.DEFAULT_RETIREMENT_QUEUE_CEILING)
    }
}
