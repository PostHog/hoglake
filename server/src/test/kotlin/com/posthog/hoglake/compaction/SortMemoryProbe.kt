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
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import java.nio.file.Files
import java.nio.file.Path
import java.util.Random

/** Exercise byte-accounted payload rewrites in bounded child JVMs. */
object SortMemoryProbe {
    private const val ROWS = 15000
    private const val PAYLOAD = 16384
    private const val BUDGET = 64L * 1024 * 1024
    private val live =
        (0 until 26).map { i ->
            Column(i + 1L, i, ColumnDef("c$i", if (i == 25) ColType.BINARY else ColType.LONG))
        }
    private val schema =
        Types.buildMessage().apply {
            repeat(26) { i ->
                addField(
                    Types.required(
                        if (i == 25) PrimitiveTypeName.BINARY else PrimitiveTypeName.INT64,
                    ).id(i + 1).named("c$i"),
                )
            }
        }.named("payload")
    private val sort = listOf(SortFieldDef(1, SortDirection.ASC, NullOrder.NULLS_LAST))

    @JvmStatic
    fun main(args: Array<String>) {
        val dir = Path.of(args[1])
        val page = args[0].endsWith("page")
        val repeated = args[0].endsWith("repeated")
        val budget = if (repeated) 96L * 1024 * 1024 else BUDGET
        val rows = if (repeated) 600 else ROWS
        val source = dir.resolve(if (page) "page.parquet" else "input.parquet")
        when (args[0]) {
            "generate", "generate-page", "generate-repeated" -> {
                val random = Random(19)
                val factory = SimpleGroupFactory(schema)
                ExampleParquetWriter.builder(LocalOutputFile(source))
                    .withType(schema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withDictionaryEncoding(repeated)
                    .withRowGroupSize(256 * 1024)
                    .withPageSize(64 * 1024)
                    .withMinRowCountForPageSizeCheck(1)
                    .withMaxRowCountForPageSizeCheck(1)
                    .build().use { writer ->
                        repeat(if (page) 1 else rows) { row ->
                            val group = factory.newGroup()
                            repeat(25) { group.add(it, (rows - row).toLong()) }
                            val bytes = ByteArray(if (page) 32 * 1024 * 1024 else PAYLOAD)
                            if (repeated) bytes.fill(42) else random.nextBytes(bytes)
                            group.add(25, Binary.fromConstantByteArray(bytes))
                            writer.write(group)
                        }
                    }
            }
            "sort", "page", "repeated" -> {
                try {
                    val result =
                        rewriteToLocal(
                            listOf(localInput(source, 100)),
                            live,
                            sort,
                            dir.resolve("output.parquet"),
                            spill =
                                SortSpill(
                                    chunkRows =
                                        CompactionConfig(
                                            targetBytes = 512L * 1024 * 1024,
                                            maxGroupsPerRun = 1,
                                            sortedHeapBytes = budget,
                                        ).spillChunkRows(live),
                                    mergeBudgetBytes = budget,
                                    spillBudgetBytes = 4L * 1024 * 1024 * 1024,
                                    spillDir = dir,
                                    readaheadBytes = 0,
                                    verifyMinBytes = Long.MAX_VALUE,
                                ),
                        )
                    check(!page)
                    check(repeated)
                    check(result.rowsWritten == rows.toLong())
                    check(result.runsSpilled > 1)
                    verify(dir.resolve("output.parquet"), rows, repeated)
                    println("ROWS=${result.rowsWritten}")
                } catch (e: MergeBudgetExceededException) {
                    check(!repeated) { e.message ?: e.toString() }
                    if (page) {
                        check(e.message!!.contains("before materialization"))
                        println("REFUSED_BEFORE_DECODE")
                    } else {
                        check(e.runsSpilled > 0)
                        check(e.spillBytes > 0)
                        println("REFUSED_MERGE")
                    }
                }
                check(
                    Files.list(dir).use {
                            paths ->
                        paths.noneMatch { it.fileName.toString().startsWith(SpillDirectory.PREFIX) }
                    },
                )
            }
            else -> error("unknown probe mode")
        }
    }

    private fun verify(
        path: Path,
        rows: Int,
        repeated: Boolean,
    ) {
        ParquetFileReader.open(LocalInputFile(path)).use { reader ->
            val schema = reader.footer.fileMetaData.schema
            val io = ColumnIOFactory().getColumnIO(schema)
            val id = schema.getFieldIndex(ParquetRewriter.ROW_ID_COLUMN)
            val random = Random(19)
            val expected = IntArray(rows)
            repeat(rows) { i ->
                val bytes = ByteArray(PAYLOAD)
                if (repeated) bytes.fill(42) else random.nextBytes(bytes)
                expected[i] = bytes.contentHashCode()
            }
            var count = 0
            while (true) {
                val pages = reader.readNextRowGroup() ?: break
                val records = io.getRecordReader(pages, GroupRecordConverter(schema))
                repeat(pages.rowCount.toInt()) {
                    val row = records.read()
                    val position = rows - count - 1
                    check(row.getLong(0, 0) == count + 1L)
                    check(row.getLong(id, 0) == position + 100L)
                    check(row.getBinary(25, 0).bytes.contentHashCode() == expected[position])
                    count++
                }
            }
            check(count == rows)
        }
    }
}
