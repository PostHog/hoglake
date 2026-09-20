package com.posthog.hoglake.compaction

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import kotlin.random.Random

/**
 * Grouping policy: pack to the target, bounded by fan-in, and only
 * rewrite a group worth rewriting.
 *
 * These used to assert a ladder — each size tier packing to its own
 * floor, so a group closed at the next rung and its output landed back
 * on that rung to be rewritten again. The tests that encoded it are
 * gone with it; what replaces them is the property that made the change
 * worth making, which the ladder never had: **compaction rewrites a
 * file once and then leaves it alone.**
 */
class CompactionGroupingTest {
    private val mib = 1024L * 1024

    private fun grouping(targetBytes: Long = 512 * mib) = CompactionGrouping.of(targetBytes)

    private fun group(
        sizes: List<Long>,
        min: Int = CompactionGrouping.DEFAULT_MIN_INPUT_FILES,
        max: Int = CompactionGrouping.DEFAULT_MAX_INPUT_FILES,
        targetBytes: Long = 512 * mib,
    ) = grouping(targetBytes).groups(sizes, min, max) { it }

    @Test
    fun `files pack to the target rather than to an intermediate rung`() {
        // Ten 53 MiB files reach the 512 MiB target, so forty of them
        // are four groups. The ladder cut each group at TWO files,
        // because two crossed the 64 MiB rung — then rewrote the result
        // up the rungs above it.
        val groups = group(List(40) { 53 * mib })
        assertThat(groups).hasSize(4)
        assertThat(groups.map { it.size }).containsOnly(10)
    }

    /**
     * Rewrite a population until nothing groups, returning
     * (passes, write amplification). 0.6 is the compression ratio
     * measured on the dev fleet; the exact figure does not matter, only
     * that an output is SMALLER than its inputs and therefore under the
     * target again — which is what makes it a candidate a second time,
     * and what the minimum exists to stop being acted on.
     */
    private fun settle(start: List<Long>): Pair<Int, Double> {
        var files = start
        var passes = 0
        var bytesRewritten = 0L
        while (true) {
            val groups = group(files)
            if (groups.isEmpty()) break
            passes++
            check(passes <= 20) { "did not converge after $passes passes" }
            bytesRewritten += groups.sumOf { g -> g.sum() }
            val merged = groups.map { g -> (g.sum() * 0.6).toLong() }
            val untouched = files.toMutableList()
            groups.flatten().forEach { untouched.remove(it) }
            files = untouched + merged
        }
        return passes to bytesRewritten.toDouble() / start.sum()
    }

    @Test
    fun `ingest-sized files reach the target in one rewrite`() {
        // The pass that matters. Ten 53 MiB ingest files fill the
        // 512 MiB target, five fit comfortably, so the minimum does not
        // scale down and the whole population is consumed in ONE rewrite
        // at amplification exactly 1.0. The ladder took these same bytes
        // up four rungs to get here.
        val first = group(List(40) { 53 * mib })
        assertThat(first.map { it.size }).containsOnly(10)
        assertThat(first.sumOf { g -> g.sum() }).isEqualTo(List(40) { 53 * mib }.sum())
    }

    @Test
    fun `the outputs then consolidate pairwise, and stop`() {
        // Honest about the cost. Those 318 MiB outputs are each over a
        // fifth of the target, so only one fits and the minimum scales
        // to its floor of two — they pair up, and pair again. Feeding
        // the whole population back until nothing groups costs about 2x
        // the input bytes, against the ladder's ~4x, and it terminates:
        // every group turns N >= 2 files into exactly one, so the file
        // count strictly decreases.
        //
        // Not one rewrite per file, then — one rewrite to REACH the
        // target band, and a bounded tail of merges to finish inside it.
        val (passes, amplification) = settle(List(40) { 53 * mib })
        assertThat(passes).describedAs("bounded, not a ladder").isLessThanOrEqualTo(3)
        assertThat(amplification).describedAs("write amplification").isLessThan(2.0)
    }

    @Test
    fun `files too large for the full minimum converge in a few passes`() {
        // 300 MiB files against a 512 MiB target: only ONE fits, so the
        // minimum scales to its floor of 2 and consolidation is pairwise
        // — these files ARE touched more than once. That is the honest
        // cost of not stranding them, and what matters is the bound: it
        // settles in a handful of passes at well under the ladder's ~4x,
        // and it does settle, because every group turns N files into one
        // and the file count strictly decreases.
        val (passes, amplification) = settle(List(16) { 300 * mib })
        assertThat(passes).isLessThanOrEqualTo(4)
        assertThat(amplification).isLessThan(2.5)
        assertThat(group(List(16) { 300 * mib })).isNotEmpty()
    }

    @Test
    fun `a near-target file is not rewritten to absorb a trickle`() {
        // The regression this clause exists for. `need` FALLS as the
        // largest file grows — a 460 MiB file against a 512 MiB target
        // needs only 2 — so on the file count alone a bucket holding one
        // near-target file and one small arrival is a valid group. That
        // rewrites 461 MiB to retire a single file, and because the
        // output is still under the target it happens again on the next
        // arrival: `2 -> 1 files, 121 MiB -> 121 MiB` in a bigger band.
        assertThat(group(listOf(460 * mib, 1 * mib))).isEmpty()
        assertThat(group(listOf(460 * mib, 1 * mib, 1 * mib, 1 * mib))).isEmpty()
        assertThat(group(listOf(300 * mib, 1 * mib))).isEmpty()
        // But a group that genuinely fills the target is still taken,
        // near-target member and all.
        assertThat(group(listOf(460 * mib) + List(5) { 53 * mib })).hasSize(1)
        // And the rule is "the rest is worth as much as the largest",
        // so an even pair merges however big it is.
        assertThat(group(List(2) { 300 * mib })).hasSize(1)
    }

    @Test
    fun `the split strands leftovers that cannot clear the minimum, and that is the trade`() {
        // The split's own cost, pinned so a future change to
        // DOMINANCE_FACTOR cannot move the band in silence. Four 10 MiB
        // files hold 40 MiB; 100 MiB is more than twice that, so it
        // splits — and the four that break off need five. Nothing
        // merges, where packing them together would have made one group
        // of five.
        assertThat(group(List(4) { 10 * mib } + listOf(100 * mib))).isEmpty()
        assertThat(group(listOf(1, 2, 5, 11, 40).map { it * mib })).isEmpty()
        // It self-heals: one more small file clears the minimum, and the
        // stranded bytes are bounded by targetBytes / DOMINANCE_FACTOR.
        assertThat(group(List(5) { 10 * mib } + listOf(100 * mib))).isNotEmpty()
        // And the non-splitting side of the same threshold still packs
        // together, which is what stops this being size tiers again.
        assertThat(group(List(8) { 20 * mib } + listOf(300 * mib))).hasSize(1)
    }

    @Test
    fun `trickle ingest onto a compacted partition does not amplify`() {
        // Steady state, which every one-shot drain measurement is blind
        // to: a partition that has already been compacted, receiving a
        // small file at a time. Without the byte clause this rewrites the
        // accumulator on every arrival.
        var live = listOf(400 * mib)
        var ingested = 0L
        var rewritten = 0L
        repeat(120) {
            live = live + (1 * mib)
            ingested += 1 * mib
            val g = group(live).maxByOrNull { it.size }
            if (g != null) {
                rewritten += g.sum()
                // zstd on event data, measured
                live = live - g.toSet() + (g.sum() * 10 / 17)
            }
        }
        assertThat(rewritten.toDouble() / ingested)
            .describedAs("write amplification under trickle ingest")
            .isLessThan(4.0)
    }

    @Test
    fun `a group too small to be worth rewriting is left alone`() {
        // The observed pathology: two already-compacted files rewritten
        // into one, over and over, on a catalog with no ingest at all.
        // 121 MiB files need four, because four is what fits under a
        // 512 MiB target — min(5, 512/121) — so a pair is refused.
        assertThat(group(List(2) { 121 * mib })).isEmpty()
        assertThat(group(List(3) { 121 * mib })).isEmpty()
        // Four fills the target and is a real consolidation.
        assertThat(group(List(4) { 121 * mib })).hasSize(1)
        // Where five DO fit, five is what it asks for.
        assertThat(group(List(4) { 90 * mib })).isEmpty()
        assertThat(group(List(5) { 90 * mib })).hasSize(1)
    }

    @Test
    fun `the minimum scales down only as far as the target forces it`() {
        // A file count cannot judge a byte target on its own: no group
        // holds five files that are each over a fifth of the target, so
        // a FIXED five would mean "never compact" for these buckets —
        // silently, since no group forms and so nothing is refused.
        // Every case here is a bucket a fixed minimum would strand.
        assertThat(group(List(2) { 300 * mib })).hasSize(1) // 1 fits -> floor of 2
        assertThat(group(List(3) { 200 * mib })).hasSize(1) // 2 fit -> 2
        assertThat(group(List(3) { 150 * mib })).hasSize(1) // 3 fit -> 3
        // But it never scales below two: one file is a copy.
        assertThat(group(listOf(400 * mib))).isEmpty()
    }

    @Test
    fun `small files group even when a large one arrives first in caller order`() {
        // Packing is by SIZE, and this is the case that makes it matter.
        // A compaction output takes `rowIdStart = min surviving id`, so it
        // sorts in FRONT of the newer, smaller files the planner hands
        // over. Packed in THAT order the big file joins the group; packed
        // by size it is split off and the smalls merge without it.
        //
        // The sizes are load-bearing. An earlier version used 20 MiB
        // smalls, where sorted and unsorted both yield one 9-file group
        // -- 8 x 20 holds 160 MiB and 160 * 2 >= 300, so the dominance
        // split never fires and the test could not tell the orders apart.
        // At 10 MiB the smalls hold 80 MiB, 80 * 2 < 300 fires the split,
        // and the two orders genuinely diverge.
        val bigFirst = listOf(300 * mib) + List(8) { 10 * mib }
        val groups = group(bigFirst)
        assertThat(groups).describedAs("the small files must still find each other").hasSize(1)
        assertThat(groups.single())
            .describedAs("size order splits the big file off; caller order would absorb it")
            .containsExactlyElementsOf(List(8) { 10L * mib })
        assertThat(groups.flatten()).doesNotContain(300L * mib)
        // Caller order is preserved inside each group.
        for (g in groups) {
            val positions = g.map { bigFirst.indexOf(it) }
            assertThat(positions).isSorted()
        }
    }

    @Test
    fun `the dominance factor is pinned from ABOVE, not just below`() {
        // DOMINANCE_FACTOR is a write-amplification bound, so a
        // regression that LOOSENS it is the dangerous direction and the
        // one the other tests miss: they all still pass at 4.
        //
        // 115 + 323 + 486 MiB against a 512 MiB target. At 2, 115 is too
        // light to carry 323 (115 * 2 < 323), so it splits off and is
        // dropped as a singleton, leaving one pair. At 4 it would ride
        // along and 924 MiB would be rewritten to absorb 115 MiB — the
        // exact shape the split exists to refuse.
        val groups = group(listOf(115, 323, 486).map { it * mib })
        assertThat(groups).hasSize(1)
        assertThat(groups.single())
            .describedAs("a factor above 2 would pull the 115 MiB file in")
            .containsExactly(323L * mib, 486L * mib)
    }

    @Test
    fun `a file at or over the target is not a candidate at all`() {
        // Callers filter these out, but this function's KDoc presents it
        // as the authority: left in, `targetBytes / largest` floors to 0
        // and `need` collapses to 2, making an oversized file EASIER to
        // group. Verify it is dropped rather than trusted.
        assertThat(group(listOf(512 * mib) + List(8) { 20 * mib }).flatten())
            .doesNotContain(512L * mib)
        assertThat(group(listOf(900 * mib) + List(8) { 20 * mib }).flatten())
            .doesNotContain(900L * mib)
    }

    @Test
    fun `fan-in closes a group that bytes never would`() {
        // A partition of tiny files never approaches the target, and
        // packing on bytes alone would leave it uncompacted forever.
        val groups = group(List(200) { 1024L }, max = 64)
        // Three full groups and a remainder of 8, which clears the
        // minimum and so is taken too.
        assertThat(groups.map { it.size }).containsExactly(64, 64, 64, 8)
    }

    @Test
    fun `the trailing remainder is compacted when it clears the minimum`() {
        // A partition that stopped receiving writes never reaches the
        // target again; holding its tail back would strand it.
        assertThat(group(List(6) { 10 * mib })).hasSize(1)
        assertThat(group(List(6) { 10 * mib }).single()).hasSize(6)
        // ...but not when it does not clear the minimum.
        assertThat(group(List(3) { 10 * mib })).isEmpty()
    }

    @Test
    fun `a file already at the target is never rewritten`() {
        // A file at or above the target closes a group by itself, and a
        // group of one is a copy — so the minimum refuses it. That is
        // the terminal state: nothing rewrites a finished file.
        assertThat(group(listOf(512 * mib))).isEmpty()
        assertThat(group(List(4) { 512 * mib }, min = 2)).isEmpty()
        assertThat(group(List(4) { 600 * mib }, min = 2)).isEmpty()
    }

    @Test
    fun `zero-byte files still group, on fan-in, and long sums do not wrap`() {
        assertThat(group(List(10) { 0L }, max = 5)).hasSize(2)
        // Each of these is far above the target, so each closes a group
        // alone and the minimum refuses it — no overflow, no rewrite.
        val huge = List(6) { Long.MAX_VALUE / 4 }
        assertThat(group(huge, min = 2)).isEmpty()
    }

    @Test
    fun `invalid policy parameters fail before planning`() {
        assertThatThrownBy { CompactionGrouping.of(1) }
            .isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy { group(listOf(1L), min = 1) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("a copy, not a compaction")
        assertThatThrownBy { group(listOf(1L), min = 5, max = 4) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("no group could form")
    }

    @Test
    fun `randomized inputs preserve identity, ordering and both bounds`() {
        val rng = Random(20260919)
        val target = 512 * mib
        repeat(300) {
            val sizes = List(rng.nextInt(0, 80)) { rng.nextLong(0, 200 * mib) }
            val min = rng.nextInt(2, 6)
            val max = rng.nextInt(min, min + 40)
            val groups = grouping().groups(sizes, min, max) { it }

            // Groups are disjoint, ordered prefixes of the input.
            val flat = groups.flatten()
            assertThat(flat).isSubsetOf(sizes)
            assertThat(flat.size).isLessThanOrEqualTo(sizes.size)
            // Packing is by size, but each group comes back in the
            // CALLER's order — that is what lets the planner hand the
            // rewriter its inputs in row-id order.
            for (g in groups) {
                val positions = g.map { sizes.indexOf(it) }
                assertThat(positions).describedAs("group holds caller order").isSorted()
            }

            for (g in groups) {
                val need =
                    maxOf(2L, minOf(min.toLong(), if (g.max() <= 0) min.toLong() else target / g.max()))
                assertThat(g.size.toLong()).describedAs("scaled minimum").isGreaterThanOrEqualTo(need)
                assertThat(g.size).describedAs("fan-in").isLessThanOrEqualTo(max)
                // The amplification bound the split buys: no group's
                // largest file outweighs DOMINANCE_FACTOR times the
                // rest, so no merge moves more than (1 + FACTOR) times
                // the bytes it retires. This is the property, as opposed
                // to the handful of sizes the fixtures pin.
                val largest = g.max()
                val rest = g.sum() - largest
                assertThat(largest)
                    .describedAs("largest %d vs %d x rest %d", largest, CompactionGrouping.DOMINANCE_FACTOR, rest)
                    .isLessThanOrEqualTo(CompactionGrouping.DOMINANCE_FACTOR * rest)
            }
        }
    }
}
