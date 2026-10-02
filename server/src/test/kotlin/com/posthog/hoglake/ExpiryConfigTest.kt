package com.posthog.hoglake

import com.posthog.hoglake.service.ExpiryService
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * The three ways expiry's phase-B purge can be configured into something
 * that looks healthy and reclaims nothing, refused at boot where the knob
 * names are still in scope.
 *
 * A unit test, like `CleanupConfigTest` and `RetirementConfigTest`: these
 * are facts about a `require` block, so they red in the fast lane with no
 * Docker, and `Config` is a data class, so each case is one construction.
 *
 * ALL THREE FAILURES ARE SILENT ONES, which is why they are boot
 * refusals rather than documentation. A page of zero purges nothing
 * forever; a page of a million cannot finish inside the purge's own
 * statement bound, so every page fails and the only trace is a counter
 * nobody has a reason to look at yet; a budget of zero with the loop ON
 * advances the floor on every sweep and drains nothing behind it, which
 * is the retirement-queue-ceiling mistake in another costume — every
 * ledger row reads healthy while the rows below the floor accumulate.
 */
class ExpiryConfigTest {
    private companion object {
        /** A loop interval that turns expiry ON, stated rather than inherited. */
        const val LOOP_ON = 15_000L
    }

    @Test
    fun `the compiled defaults boot, and are the values the KDoc quotes`() {
        val cfg = Config()
        assertThat(cfg.expiryPurgePage).isEqualTo(ExpiryService.PURGE_PAGE)
        assertThat(cfg.expiryPurgeBudgetMs).isEqualTo(ExpiryService.PURGE_BUDGET_MS)
        assertThat(cfg.expiryIntervalMs)
            .describedAs("the loop is ON by default, which is what makes the budget refusal reachable")
            .isGreaterThan(0)
    }

    @Test
    fun `a page of zero or past the ceiling is refused, naming the arithmetic`() {
        // Zero: a purge that deletes nothing, forever, with every other
        // surface reporting a healthy sweep.
        assertThatThrownBy { Config(expiryPurgePage = 0) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_EXPIRY_PURGE_PAGE=0")
            .hasMessageContaining("${Config.MAX_EXPIRY_PURGE_PAGE}")

        // Past the ceiling: at the measured 700-870 us per file a page of
        // 50,000 is 35-43 s against a 5 s statement bound, so every page
        // would be cancelled. The refusal has to name the bound, because
        // "too big" without the number is not actionable.
        assertThatThrownBy { Config(expiryPurgePage = Config.MAX_EXPIRY_PURGE_PAGE + 1) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_EXPIRY_PURGE_PAGE=${Config.MAX_EXPIRY_PURGE_PAGE + 1}")
            .hasMessageContaining(ExpiryService.PURGE_STATEMENT_TIMEOUT)

        // The ceiling itself is legal: a refusal at the boundary would be
        // a different rule from the one the message states.
        assertThatCode { Config(expiryPurgePage = Config.MAX_EXPIRY_PURGE_PAGE) }
            .doesNotThrowAnyException()
    }

    @Test
    fun `a budget of zero is refused with the loop on and allowed with it off`() {
        assertThatThrownBy { Config(expiryPurgeBudgetMs = 0, expiryIntervalMs = LOOP_ON) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_EXPIRY_PURGE_BUDGET_MS=0")
            .hasMessageContaining("HOGLAKE_EXPIRY_INTERVAL_MS=$LOOP_ON")

        // With the loop off there is no sweep to starve, and 0 is then a
        // legitimate "purge nothing from the manual endpoint either".
        assertThatCode { Config(expiryPurgeBudgetMs = 0, expiryIntervalMs = 0) }
            .doesNotThrowAnyException()
    }

    @Test
    fun `a NEGATIVE budget reports its own value, not zero`() {
        // THE ORDERING BUG THIS PINS. The combined "0 with the loop on"
        // refusal used to be checked FIRST, and its message hardcodes
        // `=0`, so an operator who set -1 was told their value was zero —
        // on every production configuration, since the loop is on by
        // default. MUTATION: move the sign check back below the combined
        // one and this reds on the message.
        assertThatThrownBy { Config(expiryPurgeBudgetMs = -1, expiryIntervalMs = LOOP_ON) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_EXPIRY_PURGE_BUDGET_MS=-1")
            .hasMessageContaining("must not be negative")

        // And with the loop off, where the combined check cannot fire at
        // all, the sign check is still the thing that refuses it.
        assertThatThrownBy { Config(expiryPurgeBudgetMs = -1, expiryIntervalMs = 0) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_EXPIRY_PURGE_BUDGET_MS=-1")
    }
}
