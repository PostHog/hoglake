package com.posthog.hoglake.compaction

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import org.apache.parquet.column.Encoding
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData
import org.apache.parquet.hadoop.metadata.ColumnPath
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit

class SortMemoryTest {
    @TempDir
    lateinit var tmp: Path

    @Test
    fun `footer charges saturate instead of admitting overflowed sizes`() {
        assertThat(SortMemory.add(-1)).isEqualTo(Long.MAX_VALUE)
        assertThat(SortMemory.add(Long.MAX_VALUE, 1)).isEqualTo(Long.MAX_VALUE)
        assertThat(SortMemory.multiply(2, -1)).isEqualTo(Long.MAX_VALUE)
        assertThat(SortMemory.multiply(Long.MAX_VALUE, 2)).isEqualTo(Long.MAX_VALUE)
        val block =
            BlockMetaData().apply {
                addColumn(
                    ColumnChunkMetaData.get(
                        ColumnPath.get("payload"),
                        PrimitiveTypeName.BINARY,
                        CompressionCodecName.SNAPPY,
                        emptySet(),
                        0,
                        0,
                        Long.MAX_VALUE,
                        Long.MAX_VALUE,
                        Long.MAX_VALUE,
                    ),
                )
            }
        assertThat(ExternalMergeSort.footerRunBytes(ParquetMetadata(null, listOf(block)))).isEqualTo(Long.MAX_VALUE)
        val schema = Types.buildMessage().required(PrimitiveTypeName.BINARY).named("payload").named("t")
        assertThatThrownBy { SortMemory(64L * 1024 * 1024).checkReader(listOf(block), schema, "overflow") }
            .isInstanceOf(MergeBudgetExceededException::class.java)
    }

    @Test
    fun `writer charges include fixed live leaves and bounded buffers`() {
        val memory = SortMemory(64L shl 20)
        val leaves = 27
        val tuning = memory.writerTuning(ExternalMergeSort.OUTPUT_BLOCK_BYTES, leaves)
        val charge =
            SortMemory.add(
                SortMemory.multiply(leaves.toLong(), SortMemory.WRITER_FIXED_BYTES_PER_LEAF),
                SortMemory.multiply(2, tuning.rowGroupBytes),
                SortMemory.multiply(
                    SortMemory.WRITER_BUFFER_MULTIPLIER,
                    SortMemory.multiply(leaves.toLong(), tuning.pageBytes.toLong()),
                ),
            )
        assertThat(charge).isLessThanOrEqualTo(memory.writerBytes)
        assertThat(tuning.pageBytes).isGreaterThanOrEqualTo(SortMemory.MIN_WRITER_PAGE_BYTES.toInt())
        assertThatThrownBy { memory.writerTuning(ExternalMergeSort.OUTPUT_BLOCK_BYTES, Int.MAX_VALUE) }
            .isInstanceOf(MergeBudgetExceededException::class.java)
        assertThatThrownBy { memory.writerTuning(-1, 1) }.isInstanceOf(MergeBudgetExceededException::class.java)
        assertThatThrownBy { memory.writerTuning(ExternalMergeSort.OUTPUT_BLOCK_BYTES, -1) }
            .isInstanceOf(MergeBudgetExceededException::class.java)
    }

    @Test
    fun `writer admission includes the minimum slab boundary`() {
        // This allowance funds exactly one 64-byte page after fixed and row-group reserves.
        val tuning = SortMemory(159152).writerTuning(ExternalMergeSort.OUTPUT_BLOCK_BYTES, 1)
        assertThat(tuning.pageBytes).isEqualTo(SortMemory.MIN_WRITER_PAGE_BYTES.toInt())
        assertThatThrownBy { SortMemory(159148).writerTuning(ExternalMergeSort.OUTPUT_BLOCK_BYTES, 1) }
            .isInstanceOf(MergeBudgetExceededException::class.java)
    }

    @Test
    fun `null-filled output leaves refuse before writer construction`() {
        val schema = Types.buildMessage().required(PrimitiveTypeName.INT64).id(1).named("c0").named("t")
        val live = List(1000) { Column(it + 1L, it, ColumnDef("c$it", ColType.LONG)) }
        for (rows in 1..2) {
            val path = tmp.resolve("wide-source-$rows.parquet")
            ExampleParquetWriter.builder(LocalOutputFile(path)).withType(schema).build().use { writer ->
                repeat(rows) { n -> writer.write(SimpleGroupFactory(schema).newGroup().apply { add(0, n.toLong()) }) }
            }
            val output = tmp.resolve("wide-output-$rows.parquet")
            val error =
                catchThrowable {
                    rewriteToLocal(
                        listOf(localInput(path, 0).copy(survivingRecords = rows.toLong())),
                        live,
                        listOf(SortFieldDef(1, SortDirection.ASC, NullOrder.NULLS_LAST)),
                        output,
                        spill =
                            SortSpill(
                                1,
                                1L shl 20,
                                1L shl 30,
                                tmp,
                                readaheadBytes = 0,
                                verifyMinBytes = Long.MAX_VALUE,
                            ),
                    )
                }
            assertThat(error).isInstanceOf(MergeBudgetExceededException::class.java)
            assertThat((error as MergeBudgetExceededException).runsSpilled).isZero()
            assertThat(error.spillBytes).isZero()
            assertThat(output).doesNotExist()
            assertThat(
                Files.list(tmp).use { paths ->
                    paths.noneMatch { it.fileName.toString().startsWith(SpillDirectory.PREFIX) }
                },
            ).isTrue()
        }
    }

    @Test
    fun `the decoder charges binary bytes before copying`() {
        val schema = Types.buildMessage().required(PrimitiveTypeName.BINARY).named("payload").named("t")
        val budget = ParquetRewriter.NodeBudget(100, "payload", 4096)
        val materializer = ParquetRewriter.budgetedMaterializer(schema, budget)
        val root = materializer.rootConverter
        root.start()
        // Accessing this deliberately short buffer would fail before the expected typed refusal.
        val value = Binary.fromConstantByteBuffer(java.nio.ByteBuffer.allocateDirect(1), 0, 8192)
        assertThatThrownBy { root.getConverter(0).asPrimitiveConverter().addBinary(value) }
            .isInstanceOf(MergeBudgetExceededException::class.java)
    }

    @Test
    fun `the decoder renews its byte charge for each row`() {
        val schema = Types.buildMessage().required(PrimitiveTypeName.BINARY).named("payload").named("t")
        val budget = ParquetRewriter.NodeBudget(100, "payload", 65536)
        val materializer = ParquetRewriter.budgetedMaterializer(schema, budget)
        val root = materializer.rootConverter

        fun decode(size: Int): Long {
            root.start()
            root.getConverter(0).asPrimitiveConverter().addBinary(Binary.fromConstantByteArray(ByteArray(size)))
            root.end()
            return budget.retainedBytes
        }
        val small = decode(0)
        assertThat(decode(4096)).isEqualTo(small + 3 * 4096)
        assertThat(decode(0)).isEqualTo(small)
    }

    @Test
    fun `dictionary repetitions cannot undercharge a materialized head`() {
        val schema =
            Types.buildMessage().addField(
                Types.optionalList().optionalElement(PrimitiveTypeName.BINARY).named("values"),
            ).named("t")
        val path = tmp.resolve("dictionary.parquet")
        val row = SimpleGroupFactory(schema).newGroup()
        val list = row.addGroup(0)
        val binary = Binary.fromConstantByteArray(ByteArray(8192) { 42 })
        repeat(50) { list.addGroup(0).add(0, binary) }
        ExampleParquetWriter.builder(LocalOutputFile(path))
            .withType(schema).withDictionaryEncoding(true).build().use { it.write(row) }
        ParquetFileReader.open(LocalInputFile(path)).use { reader ->
            assertThat(reader.footer.blocks.single().columns.single().encodings)
                .containsAnyOf(
                    Encoding.RLE_DICTIONARY,
                    Encoding.PLAIN_DICTIONARY,
                )
            val memory = SortMemory(64L * 1024 * 1024)
            val budget = ParquetRewriter.NodeBudget(1000, "dictionary", memory.rowBytes)
            val records =
                ColumnIOFactory().getColumnIO(schema).getRecordReader(
                    reader.readNextRowGroup(),
                    ParquetRewriter.budgetedMaterializer(schema, budget),
                )
            records.read()
            assertThat(budget.retainedBytes).isGreaterThan(3L * 50 * 8192)
            assertThat(memory.headBytes(reader.footer)).isGreaterThanOrEqualTo(budget.retainedBytes)
        }
    }

    @Test
    fun `merge admission charges legal pages larger than their writer target`() {
        val live =
            listOf(
                Column(
                    1,
                    0,
                    ColumnDef("payload", ColType.BINARY),
                ),
            )
        val schema = ParquetRewriter.outputSchema(live)
        val path = tmp.resolve("large-merge-page.parquet")
        val row = SimpleGroupFactory(schema).newGroup()
        row.add(0, Binary.fromConstantByteArray(ByteArray(8 * 1024 * 1024) { 42 }))
        row.add(1, 0L)
        ExampleParquetWriter.builder(LocalOutputFile(path))
            .withType(schema).withPageSize(64 * 1024).withDictionaryEncoding(false)
            .withCompressionCodec(CompressionCodecName.SNAPPY).build().use { it.write(row) }
        val memory = SortMemory(64L * 1024 * 1024)
        ParquetFileReader.open(LocalInputFile(path)).use { reader ->
            assertThat(reader.footer.blocks.single().columns.first().totalUncompressedSize)
                .isGreaterThan(8L * 1024 * 1024)
            assertThat(2 * memory.runBytes(reader.footer)).isGreaterThan(memory.mergeBytes)
        }
        val output = tmp.resolve("large-merge-output.parquet")
        val sort =
            listOf(
                SortFieldDef(
                    1,
                    SortDirection.ASC,
                    NullOrder.NULLS_LAST,
                ),
            )
        val error =
            catchThrowable {
                rewriteToLocal(
                    List(
                        2,
                    ) { localInput(path, 0, explicitRowIds = true, trustedSorted = true).copy(survivingRecords = 1) },
                    live,
                    sort,
                    output,
                    spill = SortSpill(1000, memory.bytes, 1L shl 30, tmp, readaheadBytes = 0),
                )
            }
        assertThat(error).isInstanceOf(MergeBudgetExceededException::class.java)
        assertThat((error as MergeBudgetExceededException).runsSpilled).isZero()
        assertThat(error.cause!!.stackTrace).anyMatch { it.methodName == "confirmFooters" }
        assertThat(output).doesNotExist()
    }

    @Test
    fun `an indivisible input page refuses in a bounded child JVM`() {
        child("generate-page", "512m")
        val log = child("page", "192m")
        assertThat(log).contains("REFUSED_BEFORE_DECODE")
        assertThat(tmp.resolve("output.parquet")).doesNotExist()
        assertThat(
            Files.list(tmp).use {
                    paths ->
                paths.noneMatch { it.fileName.toString().startsWith(SpillDirectory.PREFIX) }
            },
        )
            .isTrue()
    }

    @Test
    fun `unique payloads refuse a one-pass merge safely in a bounded child JVM`() {
        child("generate", "192m")
        val log = child("sort", "192m")
        assertThat(log).contains("REFUSED_MERGE")
        assertThat(tmp.resolve("output.parquet")).doesNotExist()
    }

    @Test
    fun `repeated payloads spill early and succeed in a bounded child JVM`() {
        child("generate-repeated", "192m")
        val log = child("repeated", "192m")
        assertThat(log).contains("ROWS=600")
    }

    private fun child(
        mode: String,
        heap: String,
    ): String {
        val log = tmp.resolve("$mode.log")
        val process =
            ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx$heap",
                "-cp",
                System.getProperty("java.class.path"),
                SortMemoryProbe::class.java.name,
                mode,
                tmp.toString(),
            ).redirectErrorStream(true).redirectOutput(log.toFile()).start()
        val finished = process.waitFor(180, TimeUnit.SECONDS)
        if (!finished) process.destroyForcibly().waitFor()
        val text = Files.readString(log)
        assertThat(finished).describedAs(text).isTrue()
        assertThat(process.exitValue()).describedAs(text).isZero()
        assertThat(text).doesNotContain("OutOfMemoryError")
        return text
    }
}
