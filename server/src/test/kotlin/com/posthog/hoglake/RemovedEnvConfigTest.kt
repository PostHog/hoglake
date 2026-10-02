package com.posthog.hoglake

import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * The two removed-knob tiers, which had no test at all before #261 added
 * the second one.
 *
 * Both are `require`s inside `Config.init` reading the process
 * environment, and no test can set an environment variable on a modern
 * JVM — so deleting either refusal left the whole suite green, which is
 * exactly the shape AGENT.md's mutation rule exists to catch.
 * `Config.checkRemovedEnv` takes the lookup as a parameter for that
 * reason and `init` passes `System::getenv`; these cases drive it with a
 * map.
 *
 * A unit test, deliberately: facts about a `require` block, red in the
 * fast lane with no Docker.
 */
class RemovedEnvConfigTest {
    private fun check(vararg env: Pair<String, String>) = Config.checkRemovedEnv(mapOf(*env)::get)

    // ---- tier 1: REMOVED_ENV — any value is refused -------------------------

    @Test
    fun `a knob that was removed outright is refused whatever its value, and names its replacements`() {
        // MUTATION: delete the REMOVED_ENV loop and both of these pass.
        for (value in listOf("8", "0", "")) {
            assertThatThrownBy { check("HOGLAKE_COMPACTION_TIER_TARGET" to value) }
                .describedAs("value %s", value)
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("HOGLAKE_COMPACTION_TIER_TARGET was removed")
                // The refusal's whole job: name what replaced it, because
                // the old value maps to neither of the two new knobs.
                .hasMessageContaining("HOGLAKE_COMPACTION_MIN_INPUT_FILES")
                .hasMessageContaining("HOGLAKE_COMPACTION_MAX_INPUT_FILES")
        }
    }

    // ---- tier 2: REMOVED_INTERVAL_ENV — 0 passes, positive is refused -------

    @Test
    fun `a removed subsystem's interval knob is accepted at 0, which is what every chart renders`() {
        // This is the case that forbids a REMOVED_ENV-style refusal:
        // charts/gigahog renders HOGLAKE_VERIFY_INTERVAL_MS
        // unconditionally from a REQUIRED values key, set to "0" in every
        // environment. Refusing it would stall every rollout over a value
        // that means exactly what is true — the loop is off.
        assertThatCode { check("HOGLAKE_VERIFY_INTERVAL_MS" to "0") }.doesNotThrowAnyException()
        assertThatCode { check("HOGLAKE_VERIFY_INTERVAL_MS" to "-1") }.doesNotThrowAnyException()
        // Blank is absence: `Config.env` treats it that way too.
        assertThatCode { check("HOGLAKE_VERIFY_INTERVAL_MS" to "") }.doesNotThrowAnyException()
        assertThatCode { check() }.doesNotThrowAnyException()
    }

    @Test
    fun `a POSITIVE interval for a removed subsystem is refused, naming the knob and the issue`() {
        // The hole a pure ignore leaves: an operator who sets an hour is
        // asking for invariant checks that no longer exist, and silence
        // would leave them believing a catalog is being scrubbed.
        //
        // MUTATION: relax the predicate to `raw == null` (ignore any
        // value) and this reds while the 0-case above stays green.
        assertThatThrownBy { check("HOGLAKE_VERIFY_INTERVAL_MS" to "3600000") }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_VERIFY_INTERVAL_MS=3600000 is not supported")
            .hasMessageContaining("#261")
            .hasMessageContaining("scrubber")
    }

    @Test
    fun `an unparseable interval is refused rather than read as off`() {
        // "off" is a claim about a number. A value that is not one cannot
        // be read as 0 without guessing on the operator's behalf, and the
        // guess that is wrong in the dangerous direction is the one that
        // boots.
        //
        // MUTATION: `raw.toLongOrNull() ?: 0L` and this reds.
        assertThatThrownBy { check("HOGLAKE_VERIFY_INTERVAL_MS" to "1h") }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_VERIFY_INTERVAL_MS=1h is not supported")
    }

    @Test
    fun `the real process environment boots, which is what every pod does`() {
        // The seam above would be worth nothing if `init` had stopped
        // calling it, so construct a Config for real: this is the
        // assertion that the wiring survives, and it is why the test does
        // not only exercise `checkRemovedEnv` directly.
        assertThatCode { Config() }.doesNotThrowAnyException()
    }
}
