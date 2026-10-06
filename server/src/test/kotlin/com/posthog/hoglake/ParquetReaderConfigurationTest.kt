package com.posthog.hoglake

import com.posthog.hoglake.compaction.ParquetRewriter
import com.posthog.hoglake.hydrator.FooterParse
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CodecPool
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.codec.SnappyCodec
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.ValueSource
import java.net.URL
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class ParquetReaderConfigurationTest {
    private val schema =
        Types.buildMessage()
            .addField(Types.required(PrimitiveTypeName.INT64).id(1).named("id"))
            .named("input")
    private val columns = listOf(Column(1, 0, ColumnDef("id", ColType.LONG, nullable = false)))

    @Test
    fun `footer reads do not reload Hadoop defaults for each file`() {
        val bytes = parquet(CompressionCodecName.UNCOMPRESSED, 0)
        FooterParse.parse(memoryInput(bytes))
        withoutReaderXmlLoads {
            repeat(12) {
                val footer = FooterParse.parse(memoryInput(bytes))
                assertThat(footer.blocks.sumOf { block -> block.rowCount }).isEqualTo(ROWS.toLong())
                assertThat(footer.fileMetaData.schema).isEqualTo(schema)
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [1, 4])
    fun `rewrite readers do not reload Hadoop defaults for each input`(parallelism: Int) {
        val bytes = parquet(CompressionCodecName.GZIP, 0)
        FooterParse.parse(memoryInput(bytes))
        val inputs =
            List(4) { index ->
                ParquetRewriter.Input(memoryInput(bytes), "input-$index", (index * ROWS).toLong())
            }
        val output = MemoryOutputFile()
        withoutReaderXmlLoads {
            val result =
                ParquetRewriter.rewrite(
                    inputs,
                    columns,
                    emptyList(),
                    output,
                    inputOpenParallelism = parallelism,
                )
            assertThat(result.rowsWritten).isEqualTo((4 * ROWS).toLong())
        }
        val rows = readRows(output.bytes())
        val expectedIds = List(4) { (0 until ROWS).map(Int::toLong) }.flatten()
        assertThat(rows.map { it.first }).containsExactlyElementsOf(expectedIds)
        assertThat(rows.map { it.second }).containsExactlyElementsOf((0 until 4 * ROWS).map(Int::toLong))
    }

    @Test
    fun `closing one reader releases only its own decompressor`() {
        val bytes = parquet(CompressionCodecName.SNAPPY, 0)
        val codec = SnappyCodec().apply { conf = Configuration(false) }
        val initialLeases = CodecPool.getLeasedDecompressorsCount(codec)
        ParquetReaders.open(memoryInput(bytes)).use { first ->
            val pages = first.readNextRowGroup()
            assertThat(CodecPool.getLeasedDecompressorsCount(codec)).isEqualTo(initialLeases + 1)
            ParquetReaders.open(memoryInput(bytes)).use { second ->
                second.readNextRowGroup()
                assertThat(CodecPool.getLeasedDecompressorsCount(codec)).isEqualTo(initialLeases + 2)
            }
            assertThat(CodecPool.getLeasedDecompressorsCount(codec)).isEqualTo(initialLeases + 1)
            val records = ColumnIOFactory().getColumnIO(schema).getRecordReader(pages, GroupRecordConverter(schema))
            val rows = List(ROWS) { records.read().getLong("id", 0) }
            assertThat(rows).containsExactlyElementsOf((0 until ROWS).map(Int::toLong))
        }
        assertThat(CodecPool.getLeasedDecompressorsCount(codec)).isEqualTo(initialLeases)
    }

    @ParameterizedTest
    @EnumSource(value = CompressionCodecName::class, names = ["GZIP", "SNAPPY", "ZSTD"])
    fun `compressed rewrites retain independent codec state`(codec: CompressionCodecName) {
        val bytes = List(3) { index -> parquet(codec, index * ROWS) }
        val inputs =
            bytes.mapIndexed { index, content ->
                ParquetRewriter.Input(memoryInput(content), "input-$index", (index * ROWS).toLong())
            }
        val expected = (0 until 3 * ROWS).map { it.toLong() to it.toLong() }
        Executors.newFixedThreadPool(4).use { workers ->
            val tasks =
                List(16) {
                    Callable {
                        val output = MemoryOutputFile()
                        val result =
                            ParquetRewriter.rewrite(
                                inputs,
                                columns,
                                emptyList(),
                                output,
                                inputOpenParallelism = 3,
                            )
                        assertThat(result.rowsWritten).isEqualTo(expected.size.toLong())
                        assertThat(readRows(output.bytes())).containsExactlyElementsOf(expected)
                    }
                }
            val results = workers.invokeAll(tasks, 60, TimeUnit.SECONDS)
            results.forEach { it.get() }
        }
    }

    private fun parquet(
        codec: CompressionCodecName,
        start: Int,
    ): ByteArray {
        val output = MemoryOutputFile()
        val groups = SimpleGroupFactory(schema)
        ExampleParquetWriter.builder(output)
            .withType(schema)
            .withCompressionCodec(codec)
            .build()
            .use { writer ->
                repeat(ROWS) { index ->
                    writer.write(groups.newGroup().append("id", (start + index).toLong()))
                }
            }
        return output.bytes()
    }

    private fun readRows(bytes: ByteArray): List<Pair<Long, Long>> =
        ParquetFileReader.open(memoryInput(bytes)).use { reader ->
            val outputSchema = reader.footer.fileMetaData.schema
            val columnIO = ColumnIOFactory().getColumnIO(outputSchema)
            buildList {
                var pages = reader.readNextRowGroup()
                while (pages != null) {
                    val records = columnIO.getRecordReader(pages, GroupRecordConverter(outputSchema))
                    repeat(Math.toIntExact(pages.rowCount)) {
                        val row = records.read()
                        add(row.getLong("id", 0) to row.getLong(ParquetRewriter.ROW_ID_COLUMN, 0))
                    }
                    pages = reader.readNextRowGroup()
                }
            }
        }

    private fun withoutReaderXmlLoads(body: () -> Unit) {
        val thread = Thread.currentThread()
        val originalLoader = thread.contextClassLoader
        val xmlLoads = AtomicInteger()
        thread.contextClassLoader =
            object : ClassLoader(originalLoader) {
                override fun getResource(name: String): URL? {
                    if (name == "core-default.xml" &&
                        Thread.currentThread().stackTrace.any {
                            it.className == "org.apache.parquet.ParquetReadOptions\$Builder"
                        }
                    ) {
                        xmlLoads.incrementAndGet()
                    }
                    return super.getResource(name)
                }
            }
        try {
            body()
            assertThat(xmlLoads.get())
                .describedAs("reader opens must not load core-default.xml after initialization")
                .isZero()
        } finally {
            thread.contextClassLoader = originalLoader
        }
    }

    private companion object {
        const val ROWS = 256
    }
}
