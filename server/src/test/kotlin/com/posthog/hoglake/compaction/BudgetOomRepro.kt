package com.posthog.hoglake.compaction

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types
import java.nio.file.Path

/**
 * Two-stage OOM repro for the per-row node budget.
 *
 *   write <path> <elements>   -- big heap, builds a one-row file
 *   read-old <path>           -- small heap, plain GroupRecordConverter
 *   read-new <path> <budget>  -- small heap, through ParquetRewriter
 */
object BudgetOomRepro {
    private val schema: MessageType =
        MessageType(
            "m",
            listOf<Type>(
                Types.optionalList()
                    .setElementType(
                        Types.optional(PrimitiveType.PrimitiveTypeName.INT64).id(2).named("element"),
                    )
                    .id(1)
                    .named("l"),
            ),
        )

    private val live =
        listOf(
            Column(
                1,
                0,
                ColumnDef("l", ColType.LIST, children = listOf(ColumnDef("element", ColType.LONG))),
                children = listOf(Column(2, 0, ColumnDef("element", ColType.LONG))),
            ),
        )

    @JvmStatic
    fun main(args: Array<String>) {
        when (args[0]) {
            "write" -> write(Path.of(args[1]), args[2].toInt())
            "read-old" -> readOld(Path.of(args[1]))
            "read-new" -> readNew(Path.of(args[1]), args[2].toInt())
            else -> error("usage: write|read-old|read-new")
        }
    }

    private fun write(
        path: Path,
        elements: Int,
    ) {
        val f = SimpleGroupFactory(schema)
        ExampleParquetWriter.builder(LocalOutputFile(path))
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .build()
            .use { w ->
                val g = f.newGroup()
                val list = g.addGroup(0)
                repeat(elements) { i -> list.addGroup(0).add(0, i.toLong()) }
                w.write(g)
            }
        println("WROTE one row of $elements elements to $path (${path.toFile().length()} bytes)")
    }

    /** What the read path did before the budget moved into the decode. */
    private fun readOld(path: Path) {
        val t =
            probe("read-old (plain GroupRecordConverter)") {
                ParquetFileReader.open(LocalInputFile(path)).use { reader ->
                    val columnIO = ColumnIOFactory().getColumnIO(schema)
                    var pages = reader.readNextRowGroup()
                    while (pages != null) {
                        val rr = columnIO.getRecordReader(pages, GroupRecordConverter(schema))
                        repeat(Math.toIntExact(pages.rowCount)) { rr.read() }
                        pages = reader.readNextRowGroup()
                    }
                }
            }
        println(t)
    }

    private fun readNew(
        path: Path,
        budget: Int,
    ) {
        val out = path.resolveSibling("budget-out.parquet")
        val t =
            probe("read-new (budget=$budget)") {
                rewriteToLocal(
                    listOf(localInput(path, 0L, null)),
                    live,
                    emptyList(),
                    out,
                    maxNodesPerRow = budget,
                )
            }
        println(t)
        println("partial output on disk = ${out.toFile().exists()}")
    }

    private fun probe(
        what: String,
        body: () -> Unit,
    ): String =
        try {
            body()
            "$what -> COMPLETED (no refusal)"
        } catch (e: OutOfMemoryError) {
            "$what -> OutOfMemoryError: ${e.message}"
        } catch (e: Throwable) {
            "$what -> ${e.javaClass.simpleName}: ${e.message?.take(160)}"
        }
}
