package com.posthog.hoglake.compaction

/**
 * Geometric size tiers anchored at the final target. T controls both
 * spacing and maximum fan-in. Boundaries are generated DOWNWARD using
 * integer ceiling division, so even at non-power-of-T targets, T files
 * at a tier's lower bound can reach its upper bound. No floating point.
 *
 * Groups use registered input bytes as the promotion estimate. Actual
 * output bytes can differ (DV removal, compression, schema changes);
 * classify that measured size afresh on the NEXT run.
 */
class CompactionTiers private constructor(
    val floors: List<Long>,
    val tierTarget: Int,
) {
    /** Null for finalized files. Zero-byte files cannot satisfy any quota. */
    fun tierOf(fileBytes: Long): Int? {
        require(fileBytes >= 0) { "file size must be non-negative" }
        return floors.indexOfFirst { fileBytes < it }.takeIf { it >= 0 }
    }

    fun reachFloor(tier: Int): Long = floors[tier]

    /**
     * Input is a frozen, deterministically ordered candidate list for ONE
     * partition/spec bucket. Partition by tier, consume the shortest
     * prefix reaching its quota, then repeat on the remainder. A suffix
     * below quota waits. Outputs cannot enter this candidate list.
     */
    fun <T> groups(
        files: List<T>,
        size: (T) -> Long,
    ): List<List<T>> {
        val byTier = files.groupBy { tierOf(size(it)) }
        val result = mutableListOf<List<T>>()
        for (tier in floors.indices) {
            val rows = byTier[tier] ?: continue
            val floor = reachFloor(tier)
            var start = 0
            var remaining = floor
            for (index in rows.indices) {
                val bytes = size(rows[index])
                // Subtract remaining quota rather than summing: a prefix
                // can exceed Long.MAX_VALUE with extreme registered sizes.
                if (bytes >= remaining) {
                    result += rows.subList(start, index + 1).toList()
                    start = index + 1
                    remaining = floor
                } else {
                    remaining -= bytes
                    // With ceiling-rounded geometric tiers this is only
                    // reachable for zero-byte files (quota 1, no progress).
                    if (index - start + 1 == tierTarget) break
                }
            }
        }
        return result
    }

    companion object {
        const val DEFAULT_TIER_TARGET = 8

        fun of(
            targetBytes: Long,
            tierTarget: Int = DEFAULT_TIER_TARGET,
        ): CompactionTiers {
            require(targetBytes >= 2) { "compaction target bytes must be at least 2" }
            require(tierTarget >= 2) { "compaction tier target must be at least 2" }
            val descending = mutableListOf(targetBytes)
            var floor = targetBytes
            while (floor > 1) {
                floor = floor / tierTarget + if (floor % tierTarget == 0L) 0 else 1
                descending += floor
            }
            return CompactionTiers(descending.asReversed().toList(), tierTarget)
        }
    }
}
