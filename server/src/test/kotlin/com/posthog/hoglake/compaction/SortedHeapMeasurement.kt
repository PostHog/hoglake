package com.posthog.hoglake.compaction

import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types
import java.nio.file.Files
import java.nio.file.Path
import java.util.Random
import kotlin.io.path.fileSize

/**
 * The measurement behind [CompactionConfig.SORTED_HEAP_BYTES_PER_NODE]
 * and the density derate — manual, like [CodecMeasurement] and
 * [BudgetOomRepro], because it is evidence rather than a gate.
 * `CompactionConfigTest` is the gate.
 *
 *   ./gradlew writeTestClasspath
 *   java -Xmx6g -cp "$(cat build/test-classpath.txt)" \
 *     com.posthog.hoglake.compaction.SortedHeapMeasurement [rows]
 *
 * What it answers, for the flat event shape this catalog actually
 * holds (the bench seed's PAGEVIEWS columns, as in [CodecMeasurement]):
 *
 *  1. **Heap per materialized row**, which is what the SORTED rewrite
 *     path holds — it reads every survivor of a group into an
 *     `ArrayList<Group>` so it can sort them. Measured as RETAINED
 *     bytes (used-heap delta across a settled GC, with the list still
 *     strongly reachable), divided by rows, divided by nodes per row.
 *     A `SimpleGroup` is not a compact record: it is one object, one
 *     `List<Object>[]` field array, and then one `ArrayList` PLUS its
 *     backing `Object[]` PLUS a boxed value per populated field.
 *
 *  2. **Compressed bytes per row** under SNAPPY (what every client
 *     writer here produces) and under ZSTD (what compaction itself
 *     writes since #115, and therefore what every tier-2-and-above
 *     input is). The RATIO between them is the amount by which the same
 *     byte budget started admitting more rows — i.e. exactly how much
 *     #115 degraded `targetBytes` as a heap proxy.
 *
 * Together those two numbers say how many bytes of input the sorted
 * path may plan per group for a given heap allowance, which is what
 * [CompactionConfig.effectiveTargetBytes] computes.
 */
object SortedHeapMeasurement {
    @JvmStatic
    fun main(args: Array<String>) {
        val rows = args.getOrNull(0)?.toInt() ?: 200_000
        val dir = Files.createTempDirectory("sorted-heap-measurement")
        try {
            println("== flat PAGEVIEWS shape, $rows rows, ${schema.fieldCount} parquet fields")
            val densities =
                listOf(CompressionCodecName.SNAPPY, CompressionCodecName.ZSTD).associateWith { codec ->
                    val path = dir.resolve("in-${codec.name}.parquet")
                    write(path, rows, codec)
                    val bytesPerRow = path.fileSize().toDouble() / rows
                    println(
                        String.format(
                            "   %-10s %12d B  %8.2f B/row",
                            codec.name.lowercase(),
                            path.fileSize(),
                            bytesPerRow,
                        ),
                    )
                    path to bytesPerRow
                }
            val snappy = densities.getValue(CompressionCodecName.SNAPPY).second
            val zstd = densities.getValue(CompressionCodecName.ZSTD).second
            println(String.format("   zstd is %.2fx denser than snappy (rows per byte)", snappy / zstd))

            // Heap is measured off the SNAPPY file; the decoded object
            // graph does not depend on the codec it arrived under.
            val source = densities.getValue(CompressionCodecName.SNAPPY).first
            val nodesPerRow = schema.fieldCount // one populated leaf per field, flat shape
            for (attempt in 1..3) {
                val retained = retainedHeapOfMaterializing(source)
                println(
                    String.format(
                        "   materialize attempt %d: %,d rows retained %,d B = %.1f B/row = %.1f B/node",
                        attempt,
                        rows,
                        retained,
                        retained.toDouble() / rows,
                        retained.toDouble() / rows / nodesPerRow,
                    ),
                )
            }
            println()
            println("   heap/byte ratio at snappy density: " + String.format("%.1fx", heapPerRowHint / snappy))
            println("   heap/byte ratio at zstd   density: " + String.format("%.1fx", heapPerRowHint / zstd))
        } finally {
            dir.toFile().deleteRecursively()
        }
    }

    /** Filled in by the run above; only used for the closing ratio lines. */
    private var heapPerRowHint = 0.0

    /**
     * Read every row of [path] into a list — the sorted path's
     * `ArrayList<Row>` — and report the RETAINED bytes.
     *
     * Settle the heap, sample, materialize, settle again with the list
     * still reachable, sample again. `Runtime.totalMemory - freeMemory`
     * after a settled GC is a coarse instrument, which is why the caller
     * runs it repeatedly and why the constant it feeds is rounded UP.
     */
    private fun retainedHeapOfMaterializing(path: Path): Long {
        val held = ArrayList<Group>()
        val before = settledUsedHeap()
        ParquetFileReader.open(LocalInputFile(path)).use { reader ->
            val columnIO = ColumnIOFactory().getColumnIO(schema)
            var pages = reader.readNextRowGroup()
            while (pages != null) {
                val rr = columnIO.getRecordReader(pages, GroupRecordConverter(schema))
                repeat(Math.toIntExact(pages.rowCount)) { held.add(rr.read()) }
                pages = reader.readNextRowGroup()
            }
        }
        val after = settledUsedHeap()
        // Keep the list strongly reachable across the second sample.
        check(held.isNotEmpty())
        val retained = after - before
        heapPerRowHint = retained.toDouble() / held.size
        return retained
    }

    private fun settledUsedHeap(): Long {
        repeat(4) {
            System.gc()
            Thread.sleep(120)
        }
        val rt = Runtime.getRuntime()
        return rt.totalMemory() - rt.freeMemory()
    }

    // ---- fixture ----------------------------------------------------------

    /**
     * The bench seed's pageview columns plus the `_hog_row_id` carrier
     * every compaction output holds — the shape the sorted path actually
     * materializes.
     */
    private val schema: MessageType =
        MessageType(
            "pageviews",
            buildList<Type> {
                var id = 1
                for (n in listOf("event_id", "distinct_id", "session_id", "path", "country", "browser", "os")) {
                    add(
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                            .`as`(LogicalTypeAnnotation.stringType())
                            .id(id++)
                            .named(n),
                    )
                }
                add(
                    Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .`as`(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
                        .id(id++)
                        .named("ts"),
                )
                add(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).id(id++).named("duration_ms"))
                add(Types.optional(PrimitiveType.PrimitiveTypeName.INT32).id(id).named("viewport_width"))
                add(
                    Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                        .id(ParquetRewriter.ROW_ID_FIELD_ID)
                        .named(ParquetRewriter.ROW_ID_COLUMN),
                )
            },
        )

    private val countries = (0 until 60).map { "C%02d".format(it) }
    private val browsers = listOf("Chrome", "Safari", "Firefox", "Edge", "Opera", "Chrome Mobile")
    private val operatingSystems = listOf("macOS", "Windows", "Linux", "iOS", "Android", "ChromeOS")
    private val paths = (0 until 240).map { "/app/section$it/page" }

    private fun write(
        path: Path,
        rows: Int,
        codec: CompressionCodecName,
    ) {
        val rng = Random(1234)
        val factory = SimpleGroupFactory(schema)
        ExampleParquetWriter.builder(LocalOutputFile(path))
            .withType(schema)
            .withCompressionCodec(codec)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .build()
            .use { writer ->
                repeat(rows) { i ->
                    val g = factory.newGroup()
                    g.add(0, java.util.UUID(rng.nextLong(), rng.nextLong()).toString())
                    g.add(1, "u_" + java.lang.Long.toHexString(rng.nextLong()))
                    g.add(2, java.util.UUID(rng.nextLong(), rng.nextLong()).toString())
                    g.add(3, paths[rng.nextInt(paths.size)] + "?q=" + rng.nextInt(1_000_000))
                    g.add(4, countries[rng.nextInt(countries.size)])
                    g.add(5, browsers[rng.nextInt(browsers.size)])
                    g.add(6, operatingSystems[rng.nextInt(operatingSystems.size)])
                    g.add(7, 1_700_000_000_000_000L + rng.nextInt(86_400_000).toLong() * 1000)
                    g.add(8, rng.nextInt(120_000).toLong())
                    g.add(9, 320 + rng.nextInt(2240))
                    g.add(10, i.toLong())
                    writer.write(g)
                }
            }
    }
}
