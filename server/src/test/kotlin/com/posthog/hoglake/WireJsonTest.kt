package com.posthog.hoglake

import com.fasterxml.jackson.databind.exc.InvalidNullException
import com.fasterxml.jackson.module.kotlin.readValue
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.time.Instant

/**
 * The shared wire configuration, pinned directly.
 *
 * Every property here was previously asserted only in comments, on five
 * separate mappers, and two of those had drifted without anything
 * noticing — the maintenance ledger had silently lost the ISO-8601
 * setting it claimed to have. A config that several layers depend on
 * being identical needs a test, not a promise.
 */
class WireJsonTest {
    private data class Probe(
        val startedAt: Instant,
        val someName: String,
        val absent: String? = null,
    )

    private data class Holder(val items: List<Probe>)

    @Test
    fun `times are ISO-8601, never numeric epochs`() {
        // The property the ledger's copy had lost. No result payload
        // carries a temporal field today, which is the only reason that
        // drift was never visible; this fails if it is lost again.
        val json = wireObjectMapper().writeValueAsString(Probe(Instant.parse("2026-09-15T12:00:00Z"), "x"))
        assertThat(json).contains(""""started_at":"2026-09-15T12:00:00Z"""")
        assertThat(json).doesNotContain("1789473600")
    }

    @Test
    fun `properties are snake_case and nulls are omitted`() {
        val json = wireObjectMapper().writeValueAsString(Probe(Instant.EPOCH, "x"))
        assertThat(json).contains(""""some_name":"x"""")
        assertThat(json).doesNotContain("absent")
    }

    @Test
    fun `a null element in a non-null collection is refused at binding`() {
        // The #61 fix, at the configuration level rather than through a
        // route: this is what makes every request list a 400 and not a 500.
        assertThatThrownBy { wireObjectMapper().readValue<Holder>("""{"items":[null]}""") }
            .isInstanceOf(InvalidNullException::class.java)
            .hasMessageContaining("items")
    }
}
