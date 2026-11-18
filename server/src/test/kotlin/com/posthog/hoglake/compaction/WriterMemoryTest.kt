package com.posthog.hoglake.compaction

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.io.api.Binary
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.util.Random

class WriterMemoryTest {
    @TempDir
    lateinit var tmp: Path

    private val columns = listOf(Column(1, 0, ColumnDef("payload", ColType.BINARY)))
    private val schema = ParquetRewriter.outputSchema(columns)
    private val shape = ParquetRewriter.OutputShape(columns, schema, ParquetRewriter.DEFAULT_MAX_NODES_PER_ROW)
    private val codec = ParquetRewriter.OutputCodec(CompressionCodecName.SNAPPY)

    private fun row(
        size: Int,
        id: Long = 0,
    ): ParquetRewriter.Row {
        val payload = ByteArray(size)
        Random(id).nextBytes(payload)
        val group = SimpleGroupFactory(schema).newGroup()
        group.add(0, Binary.fromConstantByteArray(payload))
        group.add(1, id)
        return ParquetRewriter.Row(
            group,
            id,
            plan = shape.identityPlan,
            memoryBytes = size + 1024L,
        )
    }

    private fun guard(
        memory: SortMemory,
        base: Long = 32L shl 20,
        retainFooter: Boolean = false,
        reserve: (Long) -> Unit = {},
    ): WriterMemory =
        WriterMemory(
            memory,
            schema,
            codec,
            base,
            16L * 1024 * 1024 / 10,
            64L * 1024,
            retainFooter = retainFooter,
            reserve = reserve,
        )

    @Test
    fun `ordinary flat rows avoid dry emission and writer overshoot`() {
        val memory = SortMemory(1L shl 30)
        val row = row(100)
        val writer = guard(memory)
        assertThat(writer.extraBytes(row)).isZero()
        assertThat(row.writerBytes).isNegative()
    }

    @Test
    fun `large-row reserve is separate from its emitted readback charge`() {
        val memory = SortMemory(512L shl 20)
        val row = row(24 shl 20)
        val writer = guard(memory)
        assertThat(writer.extraBytes(row)).isGreaterThan(32L shl 20)
        assertThat(row.writerBytes).isBetween(24L shl 20, 25L shl 20)
        assertThat(row.memoryBytes).isLessThan(25L shl 20)
        assertThat(writer.extraBytes(row) + row.memoryBytes).isLessThan(memory.bytes)
    }

    @Test
    fun `clearing codec reservations cannot erase live dictionary arrays`() {
        val writer =
            guard(SortMemory(900), base = 0) {
                if (!SortMemory.fits(it, 900)) throw MergeBudgetExceededException("test dictionary", 0, 0, 0)
            }
        writer.replaceDictionaryReservation(800)
        writer.replaceExternalReservation(0)
        assertThrows<MergeBudgetExceededException> { writer.allocator.allocate(64) }
    }

    @Test
    fun `a refused dictionary reservation leaves its prior state intact`() {
        val writer =
            guard(SortMemory(900), base = 0) {
                if (!SortMemory.fits(it, 900)) throw MergeBudgetExceededException("test dictionary rollback", 0, 0, 0)
            }
        assertThrows<MergeBudgetExceededException> { writer.replaceDictionaryReservation(1000) }
        writer.replaceExternalReservation(900)
    }

    @Test
    fun `repeated dictionary forecasts count emitted values only when needed`() {
        val live =
            listOf(
                Column(
                    1,
                    0,
                    ColumnDef("items", ColType.LIST),
                    listOf(Column(2, 0, ColumnDef("element", ColType.INT))),
                ),
            )
        val repeated = ParquetRewriter.outputSchema(live)
        val shape = ParquetRewriter.OutputShape(live, repeated, ParquetRewriter.DEFAULT_MAX_NODES_PER_ROW)
        val group = SimpleGroupFactory(repeated).newGroup()
        val list = group.addGroup(0)
        repeat(3) { list.addGroup(0).add(0, it) }
        group.add(1, 0L)
        val row = ParquetRewriter.Row(group, 0, plan = shape.identityPlan)
        val writer =
            WriterMemory(
                SortMemory(1L shl 20),
                repeated,
                codec,
                32L shl 20,
                (16L shl 20) / 10,
                64L shl 10,
                reserve = {},
            )
        assertThat(writer.hasRepeatedValues).isTrue()
        assertThat(writer.dictionaryGrowthBound(row) { _, count -> count }).isEqualTo(Long.MAX_VALUE)
        assertThat(row.writerBytes).isZero()
        val counts = mutableMapOf<String, Long>()
        assertThat(
            writer.dictionaryGrowthMeasured(row) { column, count ->
                counts[column.path.joinToString(".")] = count
                count
            },
        ).isEqualTo(4)
        assertThat(counts).isEqualTo(mapOf("items.list.element" to 3L, ParquetRewriter.ROW_ID_COLUMN to 1L))
        assertThat(row.writerBytes).isPositive()
    }

    @Test
    fun `a refused later dictionary allocation closes the whole writer scope`() {
        val live = (1..151).map { Column(it.toLong(), it - 1, ColumnDef("i$it", ColType.INT)) }
        val wide = ParquetRewriter.outputSchema(live)
        val shape = ParquetRewriter.OutputShape(live, wide, ParquetRewriter.DEFAULT_MAX_NODES_PER_ROW)
        val group = SimpleGroupFactory(wide).newGroup()
        repeat(live.size) { group.add(it, it) }
        group.add(live.size, 0L)
        val writer =
            WriterMemory(
                SortMemory(1L shl 20),
                wide,
                codec,
                8192,
                (16L shl 20) / 10,
                64L shl 10,
                availableExtra = { 0 },
                reserve = { if (it > 0) throw MergeBudgetExceededException("test dictionary cleanup", 0, 0, 0) },
            )
        val path = tmp.resolve("refused-dictionary.parquet")
        val tuning = ParquetRewriter.WriterTuning(16L shl 20, 64 shl 10, 10, writerMemory = writer)
        assertThrows<MergeBudgetExceededException> {
            ParquetRewriter.writingTo(LocalDiscardableOutput(path), wide, codec, tuning) { output ->
                output.write(
                    ParquetRewriter.Row(
                        group,
                        0,
                        plan = shape.identityPlan,
                        memoryBytes = 128L + 160L * wide.fieldCount,
                    ),
                )
            }
        }
        assertThat(Files.exists(path)).isFalse()
        writer.replaceExternalReservation(8192)
    }

    @Test
    fun `retained fallback maps do not shrink ordinary binary output below the append floor`() {
        val live =
            listOf(
                Column(1, 0, ColumnDef("id", ColType.LONG)),
                Column(2, 1, ColumnDef("payload", ColType.BINARY)),
            )
        val flat = ParquetRewriter.outputSchema(live)
        val shape = ParquetRewriter.OutputShape(live, flat, ParquetRewriter.DEFAULT_MAX_NODES_PER_ROW)
        val rows = SimpleGroupFactory(flat)
        val outputCodec = ParquetRewriter.OutputCodec(CompressionCodecName.ZSTD)
        val memory = SortMemory(1L shl 30)
        val writer =
            WriterMemory(
                memory,
                flat,
                outputCodec,
                144L shl 20,
                (128L shl 20) / 10_000,
                1L shl 20,
                availableExtra = { 0 },
                reserve = {
                    if (it > 0) throw MergeBudgetExceededException("test append layout", 0, 0, 0)
                },
            )
        val tuning =
            ParquetRewriter.WriterTuning(
                128L shl 20,
                1 shl 20,
                10_000,
                writerMemory = writer,
                tuneChecks = false,
            )
        val path = tmp.resolve("ordinary-append-layout.parquet")
        val random = Random(553)
        ParquetRewriter.writingTo(LocalDiscardableOutput(path), flat, outputCodec, tuning) { output ->
            repeat(200_000) { index ->
                val payload = ByteArray(1024)
                random.nextBytes(payload)
                val group = rows.newGroup()
                group.add(0, index.toLong())
                group.add(1, Binary.fromConstantByteArray(payload))
                group.add(2, index.toLong())
                output.write(ParquetRewriter.Row(group, index.toLong(), plan = shape.identityPlan, memoryBytes = 1664))
            }
        }
        ParquetFileReader.open(LocalInputFile(path)).use { reader ->
            assertThat(reader.footer.blocks.sumOf { it.rowCount }).isEqualTo(200_000)
            assertThat(reader.footer.blocks).hasSize(2)
            assertThat(reader.footer.blocks)
                .allMatch { it.compressedSize >= ParquetRewriter.APPEND_MIN_ROW_GROUP_BYTES }
            val expected = Random(553)
            val payload = ByteArray(1024)
            val io = ColumnIOFactory().getColumnIO(flat)
            var index = 0
            for (block in reader.footer.blocks.indices) {
                reader.readRowGroup(block).use { pages ->
                    val records = io.getRecordReader(pages, GroupRecordConverter(flat))
                    repeat(Math.toIntExact(pages.rowCount)) {
                        expected.nextBytes(payload)
                        val row = records.read()
                        assertThat(row.getLong(0, 0)).isEqualTo(index.toLong())
                        assertThat(row.getBinary(1, 0).bytes).isEqualTo(payload)
                        assertThat(row.getLong(2, 0)).isEqualTo(index.toLong())
                        index++
                    }
                }
            }
            assertThat(index).isEqualTo(200_000)
        }
    }

    @Test
    fun `wide dictionaries use extra allowance or flush whole pending groups`() {
        val live = (1..151).map { Column(it.toLong(), it - 1, ColumnDef("i$it", ColType.INT)) }
        val wide = ParquetRewriter.outputSchema(live)
        val shape = ParquetRewriter.OutputShape(live, wide, ParquetRewriter.DEFAULT_MAX_NODES_PER_ROW)
        val rows = SimpleGroupFactory(wide)
        for (allowExtra in listOf(true, false)) {
            val available = if (allowExtra) 1L shl 30 else 0
            var extra = 0L
            val memory = SortMemory(1L shl 30)
            val writer =
                WriterMemory(
                    memory,
                    wide,
                    codec,
                    32L shl 20,
                    (16L shl 20) / 10,
                    64L shl 10,
                    availableExtra = { available },
                    reserve = {
                        extra = maxOf(extra, it)
                        if (!SortMemory.fits(it, available)) {
                            throw MergeBudgetExceededException("test wide dictionary", 0, 0, 0)
                        }
                    },
                )
            val tuning = ParquetRewriter.WriterTuning(16L shl 20, 64 shl 10, 10, writerMemory = writer)
            val path = tmp.resolve("wide-dictionary-$allowExtra.parquet")
            ParquetRewriter.writingTo(LocalDiscardableOutput(path), wide, codec, tuning) { output ->
                repeat(20_000) { index ->
                    val group = rows.newGroup()
                    repeat(live.size) { group.add(it, index) }
                    group.add(live.size, index.toLong())
                    output.write(
                        ParquetRewriter.Row(
                            group,
                            index.toLong(),
                            plan = shape.identityPlan,
                            memoryBytes = 128L + 160L * wide.fieldCount,
                        ),
                    )
                }
            }
            if (allowExtra) assertThat(extra).isPositive() else assertThat(extra).isZero()
            ParquetFileReader.open(LocalInputFile(path)).use { reader ->
                if (!allowExtra) assertThat(reader.footer.blocks.size).isGreaterThan(1)
                val io = ColumnIOFactory().getColumnIO(wide)
                var index = 0
                for (block in reader.footer.blocks.indices) {
                    reader.readRowGroup(block).use { pages ->
                        val records = io.getRecordReader(pages, GroupRecordConverter(wide))
                        repeat(Math.toIntExact(pages.rowCount)) {
                            val group = records.read()
                            assertThat(group.getInteger(0, 0)).isEqualTo(index)
                            assertThat(group.getInteger(150, 0)).isEqualTo(index)
                            assertThat(group.getLong(151, 0)).isEqualTo(index.toLong())
                            index++
                        }
                    }
                }
                assertThat(index).isEqualTo(20_000)
            }
        }
    }

    @Test
    fun `writer overflow stays unaffordable after removing its fixed reserve`() {
        var actual = 0L
        val writer = guard(SortMemory(Long.MAX_VALUE), base = 128) { actual = it }
        writer.replaceExternalReservation(Long.MAX_VALUE)
        assertThat(actual).isEqualTo(Long.MAX_VALUE)
        val row = row(16).also { it.writerBytes = Long.MAX_VALUE }
        assertThat(writer.extraBytes(row)).isEqualTo(Long.MAX_VALUE)
        assertThat(writer.beforeWrite(row)).isFalse()
    }

    @Test
    fun `actual slabs reserve before allocation and release their live capacity`() {
        val memory = SortMemory(1024)
        var charged = 0L
        val writer =
            guard(memory, base = 128) {
                charged = it
                if (it > 1024) throw MergeBudgetExceededException("test writer", 0, 0, 0)
            }
        val first = writer.allocator.allocate(400)
        assertThat(charged).isGreaterThanOrEqualTo(400)
        writer.allocator.release(first)
        writer.allocator.allocate(400)
        assertThrows<MergeBudgetExceededException> { writer.allocator.allocate(2048) }
    }

    @Test
    fun `finished groups retain only their bounded footer charge`() {
        var charged = 0L
        val writer = guard(SortMemory(1L shl 30), base = 64, retainFooter = true) { charged = it }
        writer.observeBinary(listOf(0), Binary.fromConstantByteArray(ByteArray(1024)))
        writer.finishRowGroup(16)
        writer.observeBinary(listOf(0), Binary.fromConstantByteArray(ByteArray(2048)))
        assertThat(charged).isGreaterThanOrEqualTo(16 + 2048 - 64L)
        assertThat(writer.largestBinary).isEqualTo(2048)
    }

    @Test
    fun `large values use per-row page checks without forcing one-row output groups`() {
        val memory = SortMemory(512L shl 20)
        var extra = 0L
        val writer = guard(memory, base = 144L shl 20, retainFooter = true) { extra = maxOf(extra, it) }
        val tuning =
            ParquetRewriter.WriterTuning(
                128L shl 20,
                1 shl 20,
                10_000,
                measureRows = memory,
                writerMemory = writer,
                tuneChecks = false,
            )
        val path = tmp.resolve("large-values.parquet")
        val (_, returned) =
            ParquetRewriter.writingTo(LocalOutputFile(path), schema, codec, tuning) { output ->
                repeat(4) { output.write(row(8 shl 20, it.toLong())) }
                assertThat(output.maxRowBytes).isLessThan(9L shl 20)
            }
        ParquetFileReader.open(LocalInputFile(path)).use { reader ->
            assertThat(reader.footer.blocks.map { it.rowCount }).containsExactly(4L)
            val actual = reader.footer.blocks.single().columns.first().statistics
            val normalized = returned!!.blocks.single().columns.first().statistics
            assertThat(normalized.hasNonNullValue()).isEqualTo(actual.hasNonNullValue())
            assertThat(normalized.minBytes).isEqualTo(actual.minBytes)
            assertThat(normalized.maxBytes).isEqualTo(actual.maxBytes)
            assertThat(normalized.minBytes).isNull()
            assertThat(normalized.maxBytes).isNull()
            assertThat(normalized.numNulls).isEqualTo(actual.numNulls)
        }
        assertThat(extra).isLessThan(memory.bytes)
    }

    @Test
    fun `large legal values rewrite with room and refuse cleanly without writer room`() {
        val source = tmp.resolve("source.parquet")
        ExampleParquetWriter.builder(LocalOutputFile(source))
            .withType(schema)
            .withDictionaryEncoding(false)
            .withMinRowCountForPageSizeCheck(1)
            .withMaxRowCountForPageSizeCheck(1)
            .build().use { output -> repeat(3) { output.write(row(24 shl 20, it.toLong()).group) } }
        val spills = Files.createDirectory(tmp.resolve("spills"))
        val input = localInput(source, 0, explicitRowIds = true).copy(fileSizeBytes = 0, survivingRecords = 3)
        val sorting = listOf(SortFieldDef(1, SortDirection.ASC, NullOrder.NULLS_LAST))

        fun tuning(budget: Long): SortSpill =
            SortSpill(1, budget, 1L shl 30, spills, readaheadBytes = 0, readerBudgetBytes = 512L shl 20)
        val failed = tmp.resolve("refused.parquet")
        val refusal =
            assertThrows<MergeBudgetExceededException> {
                ParquetRewriter.rewrite(
                    listOf(input),
                    columns,
                    sorting,
                    LocalDiscardableOutput(failed),
                    spill = tuning(32L shl 20).copy(chunkRows = Long.MAX_VALUE),
                )
            }
        assertThat(refusal.oversizedRow).isTrue()
        assertThat(Files.exists(failed)).isFalse()
        Files.list(spills).use { assertThat(it.toList()).isEmpty() }

        val output = tmp.resolve("accepted.parquet")
        val observed = mutableListOf<Map<String, Long>>()
        val accepted =
            tuning(1L shl 30).apply {
                memoryObserver = { event, values -> if (event == "spill") observed += values }
            }
        val result =
            ParquetRewriter.rewrite(
                listOf(input),
                columns,
                sorting,
                LocalDiscardableOutput(output),
                spill = accepted,
            )
        assertThat(result.rowsWritten).isEqualTo(3)
        assertThat(observed).hasSize(3)
        assertThat(observed.map { it.getValue("maxRowBytes") }).allMatch { it < 73L shl 20 }
        ParquetFileReader.open(LocalInputFile(output)).use { reader ->
            val io = ColumnIOFactory().getColumnIO(reader.footer.fileMetaData.schema)
            var records = 0L
            for (index in reader.footer.blocks.indices) {
                reader.readRowGroup(index).use { pages ->
                    val materializer = GroupRecordConverter(reader.footer.fileMetaData.schema)
                    val rows = io.getRecordReader(pages, materializer)
                    repeat(Math.toIntExact(pages.rowCount)) {
                        assertThat(rows.read().getBinary(0, 0).length()).isEqualTo(24 shl 20)
                        records++
                    }
                }
            }
            assertThat(records).isEqualTo(3)
        }
        Files.list(spills).use { assertThat(it.toList()).isEmpty() }
    }
}
