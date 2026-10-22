package com.posthog.hoglake.compaction

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import kotlin.math.abs

/**
 * Where each pod's FIRST sweep of a table starts in the bucket list
 * (`CompactionService.initialCursorOffset`).
 *
 * Before it, every pod's cursor began empty, so every pod started at the
 * head of the same list and advanced by the same amount: nine pods on
 * gigahog-prod-us walked the same 123 buckets in lockstep on 2026-10-21.
 * The seed spreads the starts by pod identity; the server does not know
 * how many pods there are, so a hash stands in for `k x size / N`.
 *
 * Unit: pure functions, no database.
 */
class CompactionCursorSeedTest {
    private val size = 5_036 // millpond's events_raw partitions on 2026-10-21

    @Test
    fun `the same identity and table always start at the same offset`() {
        val a = CompactionService.initialCursorOffset("gigahog-maintenance-compaction-7d9f8-x2k4q", 1, 42, size)
        repeat(5) {
            assertThat(
                CompactionService.initialCursorOffset("gigahog-maintenance-compaction-7d9f8-x2k4q", 1, 42, size),
            ).isEqualTo(a)
        }
        assertThat(a).isBetween(0, size - 1)
    }

    @Test
    fun `two pod identities start at different offsets`() {
        // Two names that differ ONLY in their last character, as a
        // Deployment's pod names do.
        val a = CompactionService.initialCursorOffset("gigahog-maintenance-compaction-7d9f8-x2k4q", 1, 42, size)
        val b = CompactionService.initialCursorOffset("gigahog-maintenance-compaction-7d9f8-x2k4r", 1, 42, size)
        // MUTATION: return 0 from `initialCursorOffset` (the old empty-
        // cursor start) and this reds.
        assertThat(a).isNotEqualTo(b)
    }

    @Test
    fun `nine pods spread over the list instead of sharing a start`() {
        val pods = (0 until 9).map { "gigahog-maintenance-compaction-7d9f8-pod$it" }
        val starts = pods.map { CompactionService.initialCursorOffset(it, 1, 42, size) }
        assertThat(starts).doesNotHaveDuplicates()
        // And not one slice: 123 buckets is what one prod-us sweep
        // probed, so nine starts inside a window that wide would be the
        // lockstep walk with a small offset. (Pairwise overlap is
        // EXPECTED of a hash at this density; the bucket claims cover it.)
        assertThat(starts.max() - starts.min()).describedAs("starts %s", starts.sorted()).isGreaterThan(123)
    }

    @Test
    fun `the seed mixes near-equal names, which String hashCode does not`() {
        // MUTATION: implement `identitySeed` as `identity.hashCode()` and
        // two names one character apart seed one apart — adjacent starts
        // modulo any size, the herd again.
        val a = CompactionService.identitySeed("maintenance-a")
        val b = CompactionService.identitySeed("maintenance-b")
        assertThat(abs(a - b)).isGreaterThan(1_000_000L)
    }

    @Test
    fun `the table is part of the seed, and degenerate sizes are safe`() {
        val t1 = CompactionService.initialCursorOffset("pod", 1, 1, size)
        val t2 = CompactionService.initialCursorOffset("pod", 1, 2, size)
        assertThat(t1).isNotEqualTo(t2)
        assertThat(CompactionService.initialCursorOffset("pod", 1, 1, 0)).isZero()
        assertThat(CompactionService.initialCursorOffset("pod", 1, 1, 1)).isZero()
    }

    @Test
    fun `the default identity is never blank`() {
        assertThat(CompactionService.defaultPlannerIdentity()).isNotBlank()
    }
}
