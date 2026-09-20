package com.posthog.hoglake.compaction

import kotlin.math.max
import kotlin.math.min

/**
 * Which files compaction merges together, and which it leaves alone.
 *
 * This used to be a geometric ladder of size tiers: a file was
 * classified by size, packed against its own tier's floor, and promoted
 * a rung at a time. The consequence was that the same bytes were
 * rewritten once per rung — four passes to reach a 512 MiB target from
 * ~53 MiB ingest — and the upper rungs saved nothing at all, because
 * the compression had already happened on the first pass. In production
 * that read as `2 -> 1 files, 121 MiB -> 121 MiB`, fifteen seconds of
 * decompress-and-recompress, repeating on a catalog with no ingest.
 *
 * What replaces it is ordinary BIN PACKING, the same shape Iceberg's
 * `rewrite_data_files` uses: sort a bucket's candidates by size, pack
 * them to the target, cap the fan-in, and refuse a group too small to
 * be worth the rewrite.
 *
 * What that buys, measured TWO ways, because they do not agree and only
 * quoting the flattering one would be a lie:
 *
 *  - DRAINING a static backlog: ingest-sized files reach the target band
 *    in ONE rewrite. Files already over a fifth of the target then
 *    consolidate pairwise, since only one of them fits under the target,
 *    so the whole settle costs roughly 2x the input bytes against the
 *    ladder's 4x.
 *  - STEADY STATE, trickle ingest onto an already-compacted partition:
 *    about 2.4x, against ~2.5x for the ladder. Barely a win, and it is
 *    a LOSS (5.6x) without the dominance split below.
 *
 * So: one rewrite to REACH the target band, a bounded tail inside it,
 * and no regression against the ladder on a partition that has stopped
 * growing. Not "one rewrite per file" in any regime.
 */
class CompactionGrouping private constructor(
    /**
     * The size a group aims at. Packing to THIS, rather than to the
     * next rung of a geometric ladder, is what takes ingest-sized files
     * to the target band in one rewrite instead of four.
     */
    val targetBytes: Long,
) {
    /**
     * Bin-pack one partition/spec bucket's candidate files into the
     * groups compaction should rewrite. Outputs cannot enter this
     * candidate list.
     *
     * ## Packed by SIZE, returned in input order
     *
     * Candidates are packed smallest-first. The caller may pass them in
     * any order and gets its own order back within each group; only
     * group MEMBERSHIP is decided on size.
     *
     * This is not a refinement, it is load-bearing. Compaction outputs
     * take `rowIdStart = min surviving id` of their inputs, so an output
     * sorts IN FRONT of the newer, smaller files in row-id order. Packed
     * in that order, a large output eats most of the quota, the group
     * closes two or three files later, and the whole group is then
     * discarded for holding too few files — permanently, because row-id
     * order never changes. Simulated over 300 ticks of steady ingest and
     * a full drain, 100 MiB ingest files left 373 small files stranded
     * forever; size-ordered, the same workload drains to none. Sorting
     * by size puts the small files next to each other, where they can
     * fill a group between them.
     *
     * ## One pass, not a ladder
     *
     * A group closes when its bytes reach the final target, not when
     * they reach the next rung of a ladder. With 53 MiB inputs and a
     * 64 MiB rung that was two files, and the ~60 MiB output landed back
     * on the same rung — so the same bytes were rewritten roughly four
     * times on the way to 512 MiB, and every rewrite after the first
     * saved nothing, the compression having already happened.
     *
     * ## Why a minimum, and why it SCALES with the files it judges
     *
     * Packing to the target alone is not enough. Compression means a
     * target's worth of input yields a smaller output, which is still
     * below the target and therefore still a candidate — so groups of
     * outputs would merge again, and again, converging on the target
     * from below one slow rewrite at a time. That terminates (every
     * group turns N >= 2 files into exactly one, so the bucket's file
     * count strictly decreases), but it is the ladder rebuilt by another
     * name, and it costs about 4x the write amplification of one pass.
     *
     * [minInputFiles] is what buys the first pass outright: a group of
     * fewer than that many files is not worth a rewrite, whatever its
     * bytes.
     *
     * A FIXED minimum cannot be right, though, because it is a file
     * count being asked to judge a byte target. No group can hold five
     * files when each file is larger than a fifth of the target, so a
     * fixed 5 silently means "never compact this bucket" for any bucket
     * whose files are that big — including every SORTED table, whose
     * effective target is derated to fit the sort buffer in heap
     * (CompactionConfig.effectiveTargetBytes) and can be tens of
     * megabytes. Silently: no group forms, so nothing is refused and
     * nothing is logged.
     *
     * So each group is judged against ITS OWN largest file: a group
     * needs `min(minInputFiles, targetBytes / largest)` files, never
     * fewer than 2. A bucket of 10 MiB files against a 512 MiB target
     * still needs all 5; a bucket of 200 MiB files needs 2, because 2 is
     * all that can fit. The floor of 2 is what makes this terminate at
     * all — a one-file group is a copy, not a compaction.
     *
     * The trailing remainder is emitted too, under the same rule. A
     * partition that has stopped receiving writes never reaches the
     * target again, so holding its tail back would leave it uncompacted
     * permanently — and on a table partitioned finely enough that
     * partitions close, that is most of the data.
     *
     * MaintenanceSummarySampler reports debt under this same rule, and
     * cannot call this function: it is a bounded streaming scan and has
     * no whole-bucket list to sort. It mirrors it instead, which is why
     * the rule is expressed so that a streaming scan CAN mirror it —
     * size-ascending order comes from its SQL, and "the group's largest
     * file" is its last one, known exactly when the group closes.
     */
    fun <T> groups(
        files: List<T>,
        minInputFiles: Int = DEFAULT_MIN_INPUT_FILES,
        maxInputFiles: Int = DEFAULT_MAX_INPUT_FILES,
        size: (T) -> Long,
    ): List<List<T>> {
        require(minInputFiles >= 2) { "a group of one file is a copy, not a compaction" }
        require(maxInputFiles >= minInputFiles) {
            "maxInputFiles $maxInputFiles is below minInputFiles $minInputFiles: no group could form"
        }
        // A file at or over the target is not a candidate. Both callers
        // filter it out already (CompactionService's SQL, and the
        // sampler's mirroring skip), but this function's KDoc presents it
        // as the authority on what merges, so it must not depend on that.
        // Left in, `targetBytes / largest` floors to 0 and `need` falls to
        // its floor of 2 — an oversized file would make a group EASIER to
        // accept, which is backwards.
        val candidates = files.filter { size(it) < targetBytes }
        // Smallest first, ties by input position so the packing is
        // deterministic for equal-sized files. The index rides along so
        // each group can be handed back in the caller's own order.
        val packed = candidates.withIndex().sortedWith(compareBy({ size(it.value) }, { it.index }))
        val result = mutableListOf<List<T>>()
        var start = 0

        fun close(endExclusive: Int) {
            val group = packed.subList(start, endExclusive)
            // The group's largest file is its last, because the pack is
            // size-ascending — so this is decidable here, with no
            // lookahead, which is what lets the sampler apply the
            // identical rule while streaming.
            val largest = size(group.last().value)
            val fit = if (largest <= 0) minInputFiles.toLong() else targetBytes / largest
            val need = max(2L, min(minInputFiles.toLong(), fit)).toInt()
            if (group.size >= need) {
                result += group.sortedBy { it.index }.map { it.value }
            }
            start = endExclusive
        }

        var remaining = targetBytes
        for (index in packed.indices) {
            val bytes = size(packed[index].value)
            val held = index - start + 1
            // Close on EITHER bound. Bytes alone would never compact a
            // bucket whose whole contents sit under the target — a
            // low-traffic partition with a thousand tiny files would stay
            // a thousand files forever. Count alone would let one group
            // swallow a partition.
            //
            // Subtract remaining quota rather than summing: a prefix can
            // exceed Long.MAX_VALUE with extreme registered sizes.
            //
            // FIRST, though: close before a file that outweighs
            // everything held so far. `need` FALLS as the largest file
            // grows (a 460 MiB file against a 512 MiB target needs only
            // 2), so without this a bucket holding one near-target file
            // and one small arrival is a valid group — 461 MiB rewritten
            // to retire a single file, repeating on every arrival because
            // the output is still under the target. That is the
            // `2 -> 1 files, 121 MiB -> 121 MiB` pathology this class
            // exists to remove, in a larger size band.
            //
            // Splitting rather than REFUSING matters: refusing the whole
            // group would strand the small files behind the big one,
            // which is the other bug this class exists to remove. Here
            // the smalls close as their own group and the big file starts
            // a fresh one.
            //
            // [DOMINANCE_FACTOR] of 2, not 1, because "bigger than
            // everything held" is too eager: it splits three 100-byte
            // files away from two 600-byte ones, turning one group into
            // two for the same bytes moved and one more file left over.
            // Requiring the arrival to be more than TWICE what is held
            // catches the carrying case and leaves comparable sizes
            // together. Measured over 300 sweeps of trickle ingest onto
            // an already-compacted partition: 2.36x write amplification
            // against 5.62x with no split at all, and ~2.47x for the
            // size-tier ladder this replaced.
            //
            // Streamable by construction — it reads only the bytes held
            // and the arriving file, both of which the sampler has.
            if (held > 1 && (targetBytes - remaining) * DOMINANCE_FACTOR < bytes) {
                close(index)
                remaining = targetBytes
            }
            if (bytes >= remaining || (index - start + 1) >= maxInputFiles) {
                close(index + 1)
                remaining = targetBytes
            } else {
                remaining -= bytes
            }
        }
        if (start < packed.size) close(packed.size)
        return result
    }

    companion object {
        /**
         * The most files a group is asked to hold before it is worth
         * rewriting. 5, matching Iceberg's `min-input-files`.
         *
         * A CEILING on the requirement, not a fixed one — see [groups].
         * A group whose files are too big for five of them to fit under
         * the target is judged against what does fit, floored at 2.
         *
         * This is a WRITE-AMPLIFICATION knob. Compaction terminates at
         * any value >= 2 (a group turns N >= 2 files into one, so the
         * file count strictly decreases); what a low value costs is
         * repeated rewriting of the same bytes on the way to the target,
         * measured at roughly 4x for 2 against 1x for 5.
         */
        const val DEFAULT_MIN_INPUT_FILES = 5

        /**
         * Fan-in cap. A group of this many files closes even if their
         * bytes are nowhere near the target, which is what lets a
         * partition full of tiny files consolidate at all.
         *
         * 64 rather than the old ladder's fan-in of 8: the point of
         * packing to the target is to reach it in ONE rewrite, and at a
         * 512 MiB target with small inputs that takes more than eight.
         * It also bounds concurrently open readers once the merge lands.
         */
        const val DEFAULT_MAX_INPUT_FILES = 64

        /**
         * How far an arriving file must outweigh the bytes already held
         * before it starts a group of its own instead of joining theirs.
         * See the split in [groups].
         */
        const val DOMINANCE_FACTOR = 2L

        fun of(targetBytes: Long): CompactionGrouping {
            require(targetBytes >= 2) { "compaction target bytes must be at least 2" }
            return CompactionGrouping(targetBytes)
        }
    }
}
