package com.posthog.hoglake.service

import com.posthog.hoglake.model.IndexBloatEstimate
import com.posthog.hoglake.service.ReindexService.Companion.EXCESS_BYTES_THRESHOLD
import com.posthog.hoglake.service.ReindexService.Companion.MIN_RATIO_INDEX_BYTES
import com.posthog.hoglake.service.ReindexService.Companion.RATIO_THRESHOLD
import com.posthog.hoglake.service.ReindexService.Companion.isDue
import com.posthog.hoglake.service.ReindexService.Companion.overThreshold
import com.posthog.hoglake.service.ReindexService.Companion.pickVictim
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.within
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.LocalTime

/**
 * The reindex task's three pure decisions — is today's run due, is this
 * index over threshold, which one goes first — against literal inputs.
 * Unit, not integration: the integration suite proves the SQL feeds
 * these; this proves the decisions at their edges, which a database
 * fixture cannot place precisely (a clock a minute either side of 03:00,
 * a ratio a hair under 3.0).
 */
class ReindexDecisionTest {
    private fun t(iso: String): Instant = Instant.parse(iso)

    // ---- the daily gate ----------------------------------------------------

    @Test
    fun `the run is hardcoded to 03 00 UTC`() {
        assertThat(ReindexService.RUN_AT_UTC).isEqualTo(LocalTime.of(3, 0))
    }

    @Test
    fun `never due before 03 00 UTC, whatever the ledger says`() {
        // Midnight to 03:00 is "today" already, and today's window has not
        // opened. MUTATION: drop the `now.isBefore(window)` return and the
        // never-run case below reds — it would run at 00:00.
        assertThat(isDue(t("2026-10-05T00:00:00Z"), null)).isFalse()
        assertThat(isDue(t("2026-10-05T02:59:59.999Z"), null)).isFalse()
        assertThat(isDue(t("2026-10-05T02:59:59Z"), t("2026-10-03T03:00:00Z"))).isFalse()
    }

    @Test
    fun `due at 03 00 exactly when nothing ran today`() {
        assertThat(isDue(t("2026-10-05T03:00:00Z"), null)).isTrue()
        assertThat(isDue(t("2026-10-05T03:00:00Z"), t("2026-10-04T03:00:05Z"))).isTrue()
    }

    @Test
    fun `a restart after today's run does not run again`() {
        // The gate reads the LEDGER, so a pod restarted at 07:00 after the
        // 03:00 run sees that run. MUTATION: compare `<=` instead of
        // `isBefore` (a run that started AT the window re-runs) and the
        // first assertion reds.
        assertThat(isDue(t("2026-10-05T07:00:00Z"), t("2026-10-05T03:00:00Z"))).isFalse()
        assertThat(isDue(t("2026-10-05T23:59:59Z"), t("2026-10-05T03:04:12Z"))).isFalse()
    }

    @Test
    fun `a missed window runs on the next poll the same day`() {
        // The pod was down at 03:00 and came back at 14:00: yesterday's run
        // is the newest row, so today's is still owed.
        assertThat(isDue(t("2026-10-05T14:00:00Z"), t("2026-10-04T03:01:00Z"))).isTrue()
        // A run from BEFORE today's window — yesterday evening's catch-up —
        // does not count as today's.
        assertThat(isDue(t("2026-10-05T03:01:00Z"), t("2026-10-04T22:00:00Z"))).isTrue()
    }

    @Test
    fun `a missed day is not caught up after midnight but at the next 03 00`() {
        // Missed all of the 4th: at 00:30 on the 5th it is not due (the
        // 5th's window is not open), at 03:00 it is — once, not twice.
        val last = t("2026-10-03T03:00:00Z")
        assertThat(isDue(t("2026-10-05T00:30:00Z"), last)).isFalse()
        assertThat(isDue(t("2026-10-05T03:00:00Z"), last)).isTrue()
    }

    @Test
    fun `the day is the UTC day, not the pod's`() {
        // 2026-10-05T01:30+02:00 is 23:30Z on the 4th: past the 4th's
        // window, so due if the 4th has not run. A local-date reading
        // would call it the 5th before 03:00 and say no.
        val now = java.time.OffsetDateTime.parse("2026-10-05T01:30:00+02:00").toInstant()
        assertThat(isDue(now, t("2026-10-03T03:00:00Z"))).isTrue()
        assertThat(isDue(now, t("2026-10-04T03:00:00Z"))).isFalse()
    }

    @Test
    fun `a run that stepped aside for a migration or another build is retried until 06 00`() {
        val skipped = t("2026-10-05T03:00:30Z")
        // MUTATION: ignore `lastRetryable` and both of these red.
        assertThat(isDue(t("2026-10-05T03:05:00Z"), skipped, lastRetryable = true)).isTrue()
        assertThat(isDue(t("2026-10-05T05:59:59Z"), skipped, lastRetryable = true)).isTrue()
        // Past the cut-off the day is closed. MUTATION: drop the cut-off
        // (retry all day) and this reds.
        assertThat(isDue(t("2026-10-05T06:00:00Z"), skipped, lastRetryable = true)).isFalse()
        // A non-transient outcome closes the day at once.
        assertThat(isDue(t("2026-10-05T03:05:00Z"), skipped, lastRetryable = false)).isFalse()
        assertThat(ReindexService.RETRY_UNTIL_UTC).isEqualTo(LocalTime.of(6, 0))
        assertThat(ReindexService.RETRYABLE_SKIPS.map { it.wire })
            .containsExactlyInAnyOrder("migration_pending", "reindex_in_progress")
    }

    // ---- the bloat arithmetic ----------------------------------------------

    private fun est(
        size: Long,
        expected: Long?,
        name: String = "hog_x_idx",
        constraint: Boolean = false,
    ) = IndexBloatEstimate("hog_x", name, size, expected, constraint)

    @Test
    fun `excess and ratio are size against expected, and never negative`() {
        val e = est(size = 30L shl 20, expected = 10L shl 20)
        assertThat(e.excessBytes).isEqualTo(20L shl 20)
        assertThat(e.ratio).isCloseTo(3.0, within(1e-9))
        // Deduplication can make an index SMALLER than the estimate; that
        // is no bloat, not negative bloat.
        val dedup = est(size = 4L shl 20, expected = 10L shl 20)
        assertThat(dedup.excessBytes).isZero()
        assertThat(dedup.ratio).isCloseTo(0.4, within(1e-9))
        // No estimate: neither number is invented.
        assertThat(est(size = 1L shl 30, expected = null).excessBytes).isNull()
        assertThat(est(size = 1L shl 30, expected = null).ratio).isNull()
    }

    @Test
    fun `the ratio arm fires at exactly 3x and not a hair under`() {
        val floor = MIN_RATIO_INDEX_BYTES
        // MUTATION: `ratio > RATIO_THRESHOLD` reds the first assertion.
        assertThat(overThreshold(est(size = floor * 3, expected = floor))).isTrue()
        // MUTATION: drop the ratio arm entirely, or raise the constant, and
        // this second shape (3x, small excess) reds.
        assertThat(RATIO_THRESHOLD).isEqualTo(3.0)
        assertThat(overThreshold(est(size = floor * 3 - 1, expected = floor))).isFalse()
    }

    @Test
    fun `the ratio arm is floored by size, and the excess arm is not`() {
        // A 40 KiB index at 5x: the metapage term alone makes small indexes
        // read like this. MUTATION: drop the `sizeBytes >= minRatioIndexBytes`
        // term and this reds.
        assertThat(overThreshold(est(size = 40L shl 10, expected = 8L shl 10))).isFalse()
        assertThat(overThreshold(est(size = MIN_RATIO_INDEX_BYTES - 1, expected = 1L shl 20))).isFalse()
        assertThat(overThreshold(est(size = MIN_RATIO_INDEX_BYTES, expected = 1L shl 20))).isTrue()
    }

    @Test
    fun `a gigabyte of excess fires whatever the ratio`() {
        // 14.8 GB of indexes over 1.48M rows is ratio-heavy; this is the
        // other shape: a big healthy-looking index at 1.5x still carrying
        // a gigabyte of dead pages.
        val big = 2L shl 30
        // MUTATION: `excess > EXCESS_BYTES_THRESHOLD` reds the first; the
        // arm removed reds both of the first two.
        assertThat(overThreshold(est(size = big + EXCESS_BYTES_THRESHOLD, expected = big))).isTrue()
        assertThat(overThreshold(est(size = 3L shl 30, expected = 2L shl 30))).isTrue()
        assertThat(overThreshold(est(size = big + EXCESS_BYTES_THRESHOLD - 1, expected = big))).isFalse()
        assertThat(EXCESS_BYTES_THRESHOLD).isEqualTo(1L shl 30)
    }

    @Test
    fun `the excess arm needs a ratio past a random-key btree's steady state`() {
        // A 6 GiB md5-path index at its natural ~1.35x: 1.6 GiB of
        // "excess", forever. Without the ratio floor it is rebuilt daily.
        // MUTATION: drop `ratio >= EXCESS_MIN_RATIO` and this reds.
        val steady = est(size = (6L shl 30) * 135 / 100, expected = 6L shl 30)
        assertThat(steady.excessBytes!!).isGreaterThan(EXCESS_BYTES_THRESHOLD)
        assertThat(overThreshold(steady)).isFalse()
        // At 1.5x it fires. MUTATION: `ratio > EXCESS_MIN_RATIO` reds this.
        assertThat(overThreshold(est(size = 3L shl 30, expected = 2L shl 30))).isTrue()
        assertThat(ReindexService.EXCESS_MIN_RATIO).isEqualTo(1.5)
    }

    @Test
    fun `rebuilds are capped at 16 GiB expected`() {
        assertThat(ReindexService.MAX_REBUILD_EXPECTED_BYTES).isEqualTo(16L shl 30)
    }

    @Test
    fun `a victim expected past the cap is skipped as too_large, and the next under it goes`() {
        val cap = ReindexService.MAX_REBUILD_EXPECTED_BYTES
        // Sizes RELATIVE to the cap, so the constant can move.
        val huge = est(size = cap * 4, expected = cap + 1, name = "a_huge")
        val atCap = est(size = cap * 2, expected = cap, name = "b_at_cap")
        // MUTATION: `<= cap` to `< cap` reds the first; dropping the cap
        // reds the second.
        assertThat(ReindexService.select(listOf(huge, atCap), emptySet()).first).isEqualTo(atCap)
        assertThat(ReindexService.select(listOf(huge), emptySet()))
            .isEqualTo(null to com.posthog.hoglake.model.ReindexSkip.TOO_LARGE)
    }

    @Test
    fun `a victim whose last attempt failed is passed over, and named when it was the only one`() {
        val first = est(size = 300L shl 20, expected = 10L shl 20, name = "a_first")
        val second = est(size = 200L shl 20, expected = 10L shl 20, name = "b_second")
        // MUTATION: drop the failedLast filter and both red.
        assertThat(ReindexService.select(listOf(first, second), setOf("a_first")).first).isEqualTo(second)
        assertThat(ReindexService.select(listOf(first), setOf("a_first")))
            .isEqualTo(null to com.posthog.hoglake.model.ReindexSkip.LAST_ATTEMPT_FAILED)
        assertThat(ReindexService.select(emptyList(), emptySet())).isEqualTo(null to null)
    }

    @Test
    fun `no estimate is never over threshold`() {
        assertThat(overThreshold(est(size = 100L shl 30, expected = null))).isFalse()
    }

    // ---- which index goes first --------------------------------------------

    @Test
    fun `the largest excess goes first`() {
        val small = est(size = 30L shl 20, expected = 10L shl 20, name = "a_small")
        val large = est(size = 300L shl 20, expected = 10L shl 20, name = "z_large")
        // MUTATION: `compareBy` instead of `compareByDescending` reds this.
        assertThat(pickVictim(listOf(small, large))).isEqualTo(large)
        assertThat(pickVictim(emptyList())).isNull()
    }

    @Test
    fun `on equal excess a non-constraint index goes before a primary key`() {
        val pkey = est(size = 300L shl 20, expected = 10L shl 20, name = "a_pkey", constraint = true)
        val plain = est(size = 300L shl 20, expected = 10L shl 20, name = "z_plain", constraint = false)
        // The pkey sorts first by NAME, so only the constraint key can put
        // `plain` ahead. MUTATION: drop `thenBy { constraintBacking }` (or
        // invert it) and this reds.
        assertThat(pickVictim(listOf(pkey, plain))).isEqualTo(plain)
        // A constraint-backing index is still ELIGIBLE: alone, it is chosen.
        assertThat(pickVictim(listOf(pkey))).isEqualTo(pkey)
        // And a strictly larger primary key still beats a plain index.
        val biggerPkey = pkey.copy(sizeBytes = 400L shl 20)
        assertThat(pickVictim(listOf(biggerPkey, plain))).isEqualTo(biggerPkey)
    }
}
