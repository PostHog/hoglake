package com.posthog.hoglake.model

/**
 * The contract of `hog_data_file.split_offsets` (V18) and of the
 * `split_offsets` property on the wire: one entry per parquet row group,
 * the byte offset at which that row group's FIRST column chunk starts
 * (parquet-java `ColumnChunkMetaData.getStartingPos()` — the dictionary
 * page offset when a dictionary page precedes the first data page, else
 * the first data page offset). The Iceberg `split_offsets` convention.
 *
 * A reader uses the list to place byte-range split boundaries on
 * row-group starts. The Trino connector ignores any list that is empty,
 * not strictly increasing, starts below 0 or reaches `file_size_bytes`,
 * and falls back to even cuts — so a list that breaks the contract is
 * not a degraded answer, it is a wasted one, and a list that is PARTIAL
 * or unsorted but happens to pass would misplace cuts silently. Every
 * writer (the commit path, the hydrator, compaction) therefore runs the
 * same [violation] check, and stores NOTHING rather than a list that
 * fails it.
 */
object SplitOffsets {
    /**
     * Most row groups a stored list may name.
     *
     * A real file has tens to low thousands of row groups; a list past
     * this is either a pathological writer (one row per group) or
     * hostile, and at 8 bytes an entry plus JSON rendering it would put
     * megabytes into every scan plan that includes the file. Past the cap
     * the hydrator and compaction store nothing (the reader cuts evenly,
     * which is always correct) and the commit path refuses the list.
     */
    const val MAX_ROW_GROUPS = 100_000

    /**
     * Why [offsets] cannot be stored for a file of [fileSizeBytes], or
     * null when it can. The reason names the first offending entry, so a
     * writer reading a 422 knows which row group its footer walk got
     * wrong.
     */
    fun violation(
        offsets: List<Long>,
        fileSizeBytes: Long,
    ): String? {
        if (offsets.isEmpty()) return "must name at least one row group; omit the field instead"
        if (offsets.size > MAX_ROW_GROUPS) {
            return "names ${offsets.size} row groups, over the maximum $MAX_ROW_GROUPS"
        }
        if (offsets[0] < 0) return "entry 0 is negative (${offsets[0]})"
        for (i in 1 until offsets.size) {
            if (offsets[i] <= offsets[i - 1]) {
                return "is not strictly increasing: entry $i (${offsets[i]}) <= entry ${i - 1} (${offsets[i - 1]})"
            }
        }
        val last = offsets.last()
        if (last >= fileSizeBytes) {
            return "entry ${offsets.size - 1} ($last) is not below file_size_bytes ($fileSizeBytes)"
        }
        return null
    }
}
