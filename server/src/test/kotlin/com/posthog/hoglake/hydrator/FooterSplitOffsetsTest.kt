package com.posthog.hoglake.hydrator

import com.posthog.hoglake.model.SplitOffsets
import org.apache.parquet.column.Encoding
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData
import org.apache.parquet.hadoop.metadata.ColumnPath
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * [FooterSplitOffsets] over CONSTRUCTED footer metadata: the paths a real
 * file cannot reach cheaply (100,001 row groups, a row group with no
 * column chunks, a writer that laid row groups out of order) and the
 * starting-position rule's three arms, each with offsets chosen so that
 * the wrong arm gives a different answer. The end-to-end half — real
 * files, read back through the hydrator — is HydratorIntegrationTest.
 */
class FooterSplitOffsetsTest {
    private val column: PrimitiveType =
        Types.required(PrimitiveType.PrimitiveTypeName.INT64).id(1).named("id")
    private val schema = MessageType("t", column)

    /** One column chunk with the given page offsets; 0 = "no dictionary page". */
    private fun chunk(
        dataPage: Long,
        dictionaryPage: Long = 0,
    ): ColumnChunkMetaData =
        ColumnChunkMetaData.get(
            ColumnPath.get("id"),
            column,
            CompressionCodecName.UNCOMPRESSED,
            null,
            setOf(Encoding.PLAIN),
            null,
            dataPage,
            dictionaryPage,
            1,
            10,
            10,
        )

    private fun block(vararg chunks: ColumnChunkMetaData): BlockMetaData =
        BlockMetaData().apply {
            rowCount = 1
            chunks.forEach { addColumn(it) }
        }

    private fun footer(blocks: List<BlockMetaData>): ParquetMetadata =
        ParquetMetadata(FileMetaData(schema, emptyMap(), "test"), blocks)

    @Test
    fun `each entry is the first column chunk's starting position, dictionary page first when it precedes`() {
        val offsets =
            FooterSplitOffsets.of(
                footer(
                    listOf(
                        // Dictionary page before the data page: the chunk
                        // starts at the dictionary page.
                        block(chunk(dataPage = 40, dictionaryPage = 4)),
                        // No dictionary page (offset 0): the data page.
                        block(chunk(dataPage = 100)),
                        // A "dictionary" offset AFTER the data page is not
                        // where the chunk starts; getStartingPos ignores it.
                        block(chunk(dataPage = 200, dictionaryPage = 250)),
                        // Only the FIRST chunk counts; a second chunk that
                        // starts earlier in the row group is irrelevant.
                        block(chunk(dataPage = 400, dictionaryPage = 350), chunk(dataPage = 300)),
                    ),
                ),
                fileSizeBytes = 1_000,
            )
        assertThat(offsets).containsExactly(4L, 100L, 200L, 350L)
    }

    @Test
    fun `a single row group gives a one-element list`() {
        assertThat(FooterSplitOffsets.of(footer(listOf(block(chunk(dataPage = 4)))), fileSizeBytes = 100))
            .containsExactly(4L)
    }

    @Test
    fun `over the row-group cap stores nothing, at the cap stores every entry`() {
        fun rowGroups(n: Int) = footer(List(n) { i -> block(chunk(dataPage = 4L + i * 10L)) })
        val fileSize = 4L + (SplitOffsets.MAX_ROW_GROUPS + 1) * 10L + 100
        val atCap = FooterSplitOffsets.of(rowGroups(SplitOffsets.MAX_ROW_GROUPS), fileSize)
        assertThat(atCap).hasSize(SplitOffsets.MAX_ROW_GROUPS)
        assertThat(FooterSplitOffsets.of(rowGroups(SplitOffsets.MAX_ROW_GROUPS + 1), fileSize)).isNull()
    }

    @Test
    fun `an explicit smaller cap is honoured`() {
        val three = footer(listOf(block(chunk(4)), block(chunk(20)), block(chunk(40))))
        assertThat(FooterSplitOffsets.of(three, 100, maxRowGroups = 3)).containsExactly(4L, 20L, 40L)
        assertThat(FooterSplitOffsets.of(three, 100, maxRowGroups = 2)).isNull()
    }

    @Test
    fun `never a partial or unsorted list - null instead`() {
        // No row groups at all: an empty list would break the contract.
        assertThat(FooterSplitOffsets.of(footer(emptyList()), 100)).isNull()
        // A row group with no column chunk has no start to name, so the
        // WHOLE list goes, not just that entry.
        assertThat(FooterSplitOffsets.of(footer(listOf(block(chunk(4)), block())), 100)).isNull()
        // Row groups laid out of order, or two starting at one offset.
        assertThat(FooterSplitOffsets.of(footer(listOf(block(chunk(40)), block(chunk(4)))), 100)).isNull()
        assertThat(FooterSplitOffsets.of(footer(listOf(block(chunk(4)), block(chunk(4)))), 100)).isNull()
        // An offset at or past the registered size.
        assertThat(FooterSplitOffsets.of(footer(listOf(block(chunk(4)), block(chunk(100)))), 100)).isNull()
        assertThat(FooterSplitOffsets.of(footer(listOf(block(chunk(4)), block(chunk(99)))), 100))
            .containsExactly(4L, 99L)
    }
}
