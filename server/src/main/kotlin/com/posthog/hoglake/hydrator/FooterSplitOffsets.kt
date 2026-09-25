package com.posthog.hoglake.hydrator

import com.posthog.hoglake.model.SplitOffsets
import org.apache.parquet.hadoop.metadata.ParquetMetadata

/**
 * A parsed footer's row-group start offsets, in the [SplitOffsets]
 * contract, or null when the footer cannot give a list that honours it.
 *
 * Each entry is the row group's FIRST column chunk's
 * `ColumnChunkMetaData.getStartingPos()`: the dictionary page offset when
 * the chunk has a dictionary page that precedes its first data page, else
 * the first data page offset. That is the position parquet readers
 * (parquet-java, and Trino's reader through the same rule) use to decide
 * which split a row group belongs to, so a cut placed there never splits
 * one. `RowGroup.file_offset` is NOT used: some writers have set it
 * wrongly, and Trino's own reader stopped trusting it for the same reason.
 *
 * NULL, never partial. The answer is null when the footer has no row
 * groups, when any row group has no column chunks (there is no start to
 * name), when there are more than [maxRowGroups], and when the computed
 * list fails [SplitOffsets.violation] against [fileSizeBytes] (a writer
 * that laid its row groups out of order, or a footer whose offsets point
 * past the registered size). A reader handed null cuts evenly, which is
 * always correct; a reader handed half a list, or an unsorted one, would
 * place its cuts on a guess.
 *
 * TOTAL over any footer parquet-java managed to parse: the offsets are
 * optional metadata and must never be the reason a hydration or a
 * compaction fails, so anything parquet-java throws on the way (an
 * encrypted chunk it cannot decrypt, say) is also null. The footer fuzz
 * target holds it to that.
 */
object FooterSplitOffsets {
    fun of(
        footer: ParquetMetadata,
        fileSizeBytes: Long,
        maxRowGroups: Int = SplitOffsets.MAX_ROW_GROUPS,
    ): List<Long>? {
        val blocks = footer.blocks ?: return null
        if (blocks.isEmpty() || blocks.size > maxRowGroups) return null
        val offsets =
            try {
                blocks.map { block ->
                    val first = block.columns?.firstOrNull() ?: return null
                    first.startingPos
                }
            } catch (_: RuntimeException) {
                return null
            }
        return offsets.takeIf { SplitOffsets.violation(it, fileSizeBytes) == null }
    }
}
