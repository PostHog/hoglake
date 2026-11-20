package com.posthog.hoglake.compaction

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.CodecFactory
import org.apache.parquet.hadoop.ColumnChunkPageWriteStore
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.MessageTypeParser
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

class BoundedWriterPageStoreTest {
    @TempDir
    lateinit var directory: Path

    private val schema = MessageTypeParser.parseMessageType("message m { optional binary value = 1; }")

    internal data class Written(
        val path: Path,
        val footer: ParquetMetadata,
        val retained: Long,
        val suppressed: Int,
    )

    internal fun write(
        name: String,
        bounded: Boolean,
        version: ParquetProperties.WriterVersion,
        groups: List<List<ByteArray?>>,
    ): Written {
        val path = directory.resolve(name)
        val props =
            ParquetProperties.builder()
                .withWriterVersion(version)
                .withPageSize(64 shl 10)
                .withMinRowCountForPageSizeCheck(1)
                .withMaxRowCountForPageSizeCheck(10)
                .build()
        val codecs = CodecFactory(Configuration(false), props.pageSizeThreshold)
        val compressor = codecs.getCompressor(CompressionCodecName.ZSTD)
        val file =
            ParquetFileWriter(
                LocalOutputFile(path),
                schema,
                ParquetFileWriter.Mode.OVERWRITE,
                16L shl 20,
                0,
                null,
                props,
            )
        var retained = 0L
        var suppressed = 0
        try {
            file.start()
            for (rows in groups) {
                val pages =
                    ColumnChunkPageWriteStore.builder()
                        .withCompressorProvider { compressor }
                        .withSchema(schema)
                        .withAllocator(props.allocator)
                        .withColumnIndexTruncateLength(props.columnIndexTruncateLength)
                        .withPageWriteChecksumEnabled(props.pageWriteChecksumEnabled)
                        .build()
                val wrapper = if (bounded) BoundedWriterPageStore(pages) else null
                val columns = props.newColumnWriteStore(schema, wrapper ?: pages, pages)
                try {
                    val consumer = ColumnIOFactory(false).getColumnIO(schema).getRecordWriter(columns)
                    for (value in rows) {
                        consumer.startMessage()
                        if (value != null) {
                            consumer.startField("value", 0)
                            consumer.addBinary(Binary.fromConstantByteArray(value))
                            consumer.endField("value", 0)
                        }
                        consumer.endMessage()
                    }
                    consumer.flush()
                    file.startBlock(rows.size.toLong())
                    columns.flush()
                    wrapper?.finishStatistics()
                    retained += wrapper?.retainedExtremaBytes ?: 0
                    suppressed += wrapper?.suppressedColumns ?: 0
                    pages.flushToFileWriter(file)
                    file.endBlock()
                } finally {
                    columns.close()
                    pages.close()
                }
            }
            file.end(emptyMap())
            return Written(path, file.footer, retained, suppressed)
        } finally {
            file.close()
            codecs.release()
        }
    }

    @Test
    fun `both writer versions preserve the complete serialized file`() {
        val huge = ByteArray(24 shl 20) { 109 }
        val groups = listOf(listOf("small".toByteArray(), null), listOf(huge, null), listOf(huge.copyOf(), null))
        for (version in ParquetProperties.WriterVersion.values()) {
            val stock = write("stock-$version.parquet", false, version, groups)
            val bounded = write("bounded-$version.parquet", true, version, groups)
            assertThat(Files.mismatch(stock.path, bounded.path)).isEqualTo(-1L)
            assertThat(bounded.suppressed).isEqualTo(2)
            assertThat(bounded.retained).isEqualTo(10)
            val hugeStats = bounded.footer.blocks[1].columns.single().statistics
            assertThat(hugeStats.hasNonNullValue()).isFalse()
            assertThat(hugeStats.isEmpty).isFalse()
            assertThat(hugeStats.numNulls).isEqualTo(1)
            val copied = hugeStats.copy()
            assertThat(copied.hasNonNullValue()).isFalse()
            assertThat(copied.isSmallerThan(4096)).isFalse()
            assertThat(copied.isEmpty).isFalse()
            assertThat(copied.numNulls).isEqualTo(1)
            ParquetFileReader.open(LocalInputFile(bounded.path)).use { reader ->
                val decoded = reader.footer.blocks[1].columns.single().statistics
                assertThat(decoded.isNumNullsSet).isFalse()
                assertThat(decoded.hasNonNullValue()).isFalse()
                val small = reader.footer.blocks[0].columns.single().statistics
                assertThat(small.numNulls).isEqualTo(1)
                assertThat(small.minBytes).isEqualTo("small".toByteArray())
                val columns = ColumnIOFactory().getColumnIO(schema)
                var values = 0
                while (true) {
                    val pages = reader.readNextRowGroup() ?: break
                    pages.use {
                        val records = columns.getRecordReader(pages, GroupRecordConverter(schema))
                        repeat(pages.rowCount.toInt()) {
                            val row = records.read()
                            if (row.getFieldRepetitionCount(0) > 0) values++
                        }
                    }
                }
                assertThat(values).isEqualTo(3)
            }
        }
    }

    @Test
    fun `the converter cutoff preserves every legal small bound`() {
        for (length in listOf(0, 1, 2047, 2048)) {
            val groups = listOf(listOf(ByteArray(length) { 97 }, null))
            val stock =
                write("cutoff-stock-$length.parquet", false, ParquetProperties.WriterVersion.PARQUET_1_0, groups)
            val bounded =
                write("cutoff-bounded-$length.parquet", true, ParquetProperties.WriterVersion.PARQUET_1_0, groups)
            assertThat(Files.mismatch(stock.path, bounded.path)).isEqualTo(-1L)
            assertThat(bounded.suppressed).isEqualTo(if (length < 2048) 0 else 1)
            if (length < 2048) {
                val statistics = bounded.footer.blocks.single().columns.single().statistics
                assertThat(statistics.minBytes).hasSize(length)
                assertThat(statistics.maxBytes).hasSize(length)
                assertThat(statistics.numNulls).isEqualTo(1)
            }
        }
    }

    @Test
    fun `small final extrema survive an oversized intermediate page`() {
        val values = listOf(ByteArray(24 shl 20) { 109 }, "a".toByteArray(), "z".toByteArray(), null)
        val stock = write("mixed-stock.parquet", false, ParquetProperties.WriterVersion.PARQUET_1_0, listOf(values))
        val bounded = write("mixed-bounded.parquet", true, ParquetProperties.WriterVersion.PARQUET_1_0, listOf(values))
        assertThat(Files.mismatch(stock.path, bounded.path)).isEqualTo(-1L)
        assertThat(bounded.suppressed).isZero()
        val statistics = bounded.footer.blocks.single().columns.single().statistics
        assertThat(statistics.minBytes).isEqualTo("a".toByteArray())
        assertThat(statistics.maxBytes).isEqualTo("z".toByteArray())
        assertThat(statistics.numNulls).isEqualTo(1)
    }
}
