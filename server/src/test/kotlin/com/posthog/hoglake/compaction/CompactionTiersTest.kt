package com.posthog.hoglake.compaction

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.math.BigInteger
import kotlin.random.Random

class CompactionTiersTest {
    private val mib = 1024L * 1024

    @Test
    fun `default ladder includes small-file tiers and exact boundaries go up a tier`() {
        val tiers = CompactionTiers.of(512 * mib)
        assertThat(tiers.floors.takeLast(5)).containsExactly(mib / 8, mib, 8 * mib, 64 * mib, 512 * mib)
        assertThat(tiers.tierTarget).isEqualTo(8)
        for ((index, floor) in tiers.floors.withIndex()) {
            assertThat(tiers.tierOf(floor - 1)).isEqualTo(index)
            assertThat(tiers.tierOf(floor)).isEqualTo((index + 1).takeIf { it < tiers.floors.size })
        }
        assertThat(tiers.groups(listOf(512 * mib, 513 * mib)) { it }).isEmpty()
    }

    @Test
    fun `seven files wait eight promote and seventeen make two groups plus a remainder`() {
        val tiers = CompactionTiers.of(512 * mib)
        assertThat(tiers.groups(List(7) { mib }) { it }).isEmpty()
        assertThat(tiers.groups(List(8) { mib }) { it }.single()).hasSize(8)
        assertThat(tiers.groups(List(17) { mib }) { it }.map { it.size }).containsExactly(8, 8)
    }

    @Test
    fun `mixed sizes stop at the first byte crossing with no cross-tier rescue`() {
        val tiers = CompactionTiers.of(512 * mib)
        val sizes = listOf(3 * mib, 4 * mib, mib, 7 * mib, 2 * mib, 2 * mib, 9 * mib)
        // [3,4,1] exactly 8; [7,2] overshoots; 2 is the short remainder.
        // The 9 MiB file is in the next tier and cannot rescue it.
        assertThat(tiers.groups(sizes) { it }).containsExactly(
            listOf(3 * mib, 4 * mib, mib),
            listOf(7 * mib, 2 * mib),
        )
    }

    @Test
    fun `non-power targets round boundaries up so the fan-in always suffices`() {
        val tiers = CompactionTiers.of(1001, 8)
        assertThat(tiers.floors).containsExactly(1, 2, 16, 126, 1001)
        for ((lower, upper) in tiers.floors.zipWithNext()) {
            val groups = tiers.groups(List(8) { lower }) { it }
            assertThat(groups).isNotEmpty()
            for (group in groups) {
                assertThat(group.sum()).isGreaterThanOrEqualTo(upper)
                assertThat(group).hasSizeLessThanOrEqualTo(8)
            }
        }
    }

    @Test
    fun `zero files do not stall other tiers and long sums do not wrap`() {
        val tiers = CompactionTiers.of(Long.MAX_VALUE, 8)
        val big = Long.MAX_VALUE / 2 + 1
        assertThat(tiers.groups(List(100) { 0L } + listOf(big, big)) { it })
            .containsExactly(listOf(big, big))
        assertThat(tiers.groups(emptyList<Long>()) { it }).isEmpty()
    }

    @Test
    fun `invalid policy parameters fail before planning`() {
        for (target in listOf(Long.MIN_VALUE, -1L, 0L, 1L)) {
            assertThatThrownBy { CompactionTiers.of(target) }.isInstanceOf(IllegalArgumentException::class.java)
        }
        for (t in listOf(Int.MIN_VALUE, -1, 0, 1)) {
            assertThatThrownBy { CompactionTiers.of(512 * mib, t) }.isInstanceOf(IllegalArgumentException::class.java)
        }
        assertThatThrownBy { CompactionTiers.of(1000).tierOf(-1) }.isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `randomized selections preserve identity ordering quota minimality and fan-in`() {
        val random = Random(4740871)
        repeat(2000) {
            val target = if (it % 20 == 0) Long.MAX_VALUE else random.nextLong(2, 1_000_000)
            val t = random.nextInt(2, 17)
            val policy = CompactionTiers.of(target, t)
            val files =
                List(random.nextInt(0, 250)) { id ->
                    val floor = policy.floors.random(random)
                    id to random.nextLong(0, floor)
                }
            val groups = policy.groups(files) { f -> f.second }
            val selected = groups.flatten()
            assertThat(selected.map { f -> f.first }).doesNotHaveDuplicates()
            for (group in groups) {
                assertThat(group.size).isBetween(2, t)
                val tier = policy.tierOf(group.first().second)!!
                assertThat(group.map { f -> policy.tierOf(f.second) }).containsOnly(tier)
                assertThat(group.map { f -> f.first }).isSorted()
                val quota = BigInteger.valueOf(policy.reachFloor(tier))
                val sum = group.fold(BigInteger.ZERO) { n, f -> n + BigInteger.valueOf(f.second) }
                assertThat(sum).isGreaterThanOrEqualTo(quota)
                assertThat(sum - BigInteger.valueOf(group.last().second)).isLessThan(quota)
            }
            for (tier in policy.floors.indices) {
                val original = files.filter { f -> policy.tierOf(f.second) == tier }
                val taken = selected.filter { f -> policy.tierOf(f.second) == tier }
                assertThat(taken).isEqualTo(original.take(taken.size))
                val left = original.drop(taken.size).fold(BigInteger.ZERO) { n, f -> n + BigInteger.valueOf(f.second) }
                assertThat(left).isLessThan(BigInteger.valueOf(policy.reachFloor(tier)))
            }
        }
    }
}
