package com.posthog.hoglake.compaction

import com.posthog.hoglake.hydrator.CatalogColumn
import com.posthog.hoglake.hydrator.FooterStats
import com.posthog.hoglake.model.ColType
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.LocalInputFile
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

class BoundedWriterFooterStatsTest {
    @TempDir
    lateinit var directory: Path

    private val catalog = listOf(CatalogColumn(1, "value", ColType.BINARY, null))

    @Test
    fun `nullable omitted counts never become a false zero`() {
        val fixture = BoundedWriterPageStoreTest().also { it.directory = directory }
        val result =
            fixture.write(
                "nullable-huge.parquet",
                true,
                ParquetProperties.WriterVersion.PARQUET_1_0,
                listOf(listOf(ByteArray(24 shl 20) { 109 }, null)),
            )
        val inMemory = FooterStats.aggregate(result.footer, catalog, result.path.toString()).single()
        assertThat(inMemory.nullCount).isEqualTo(1)
        assertThat(inMemory.lowerBound).isNull()
        assertThat(inMemory.upperBound).isNull()
        ParquetFileReader.open(LocalInputFile(result.path)).use { reader ->
            assertThat(FooterStats.aggregate(reader.footer, catalog, result.path.toString())).isEmpty()
        }
    }

    @Test
    fun `nullable small bounds preserve accurate pruning statistics`() {
        val fixture = BoundedWriterPageStoreTest().also { it.directory = directory }
        val result =
            fixture.write(
                "nullable-small.parquet",
                true,
                ParquetProperties.WriterVersion.PARQUET_2_0,
                listOf(listOf("a".toByteArray(), null, "z".toByteArray())),
            )
        ParquetFileReader.open(LocalInputFile(result.path)).use { reader ->
            val statistics = FooterStats.aggregate(reader.footer, catalog, result.path.toString()).single()
            assertThat(statistics.nullCount).isEqualTo(1)
            assertThat(statistics.lowerBound).isEqualTo("a".toByteArray())
            assertThat(statistics.upperBound).isEqualTo("z".toByteArray())
        }
    }
}
