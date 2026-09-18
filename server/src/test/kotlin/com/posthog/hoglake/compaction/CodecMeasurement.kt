package com.posthog.hoglake.compaction

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
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
 * The measurement behind [ParquetRewriter.OutputCodec]'s default —
 * manual, like [BudgetOomRepro], because it is evidence rather than a
 * gate. `CompactionCodecTest` is the gate.
 *
 *   ./gradlew writeTestClasspath
 *   java -cp "$(cat build/test-classpath.txt)" \
 *     com.posthog.hoglake.compaction.CodecMeasurement [rowsPerFile] [files]
 *
 * What it does: fabricate multi-file event-shaped inputs in the bench
 * seed's PAGEVIEWS shape (uuid-ish event ids, session ids, paths,
 * country/browser/os enums, numerics, timestamps), write them SNAPPY —
 * which is what every client writer here actually produces (pyarrow's
 * default, and DuckDB's, so pyhoglake and the duckdb-client both) — then
 * merge them through the REAL [ParquetRewriter] once per candidate
 * codec and print input bytes, output bytes and wall time.
 *
 * Two column shapes are measured separately because they do not behave
 * alike: HIGH-CARDINALITY strings (an id per row, near-unique, where
 * dictionary encoding gives up and the codec is the only compression
 * there is) and LOW-CARDINALITY enums (a handful of distinct values,
 * where parquet's own dictionary has already done most of the work and
 * a codec can only squeeze the remainder). A codec argued from one of
 * them alone is an argument about half the table.
 */
object CodecMeasurement {
    private const val TS_FIELD_ID = 5

    /** The bench seed's pageview columns, split by how they compress. */
    private enum class Shape { HIGH_CARDINALITY, LOW_CARDINALITY, MIXED }

    @JvmStatic
    fun main(args: Array<String>) {
        val rowsPerFile = args.getOrNull(0)?.toInt() ?: 200_000
        val files = args.getOrNull(1)?.toInt() ?: 4
        val dir = Files.createTempDirectory("codec-measurement")
        try {
            for (shape in Shape.entries) {
                measure(dir, shape, rowsPerFile, files)
            }
        } finally {
            dir.toFile().deleteRecursively()
        }
    }

    private fun measure(
        dir: Path,
        shape: Shape,
        rowsPerFile: Int,
        files: Int,
    ) {
        val schema = schemaOf(shape)
        val live = liveColumns(shape)
        val inputs =
            (0 until files).map { f ->
                val path = dir.resolve("$shape-in-$f.parquet")
                writeInput(path, schema, shape, rowsPerFile, seed = 1000L + f)
                ParquetRewriter.Input(path, f.toLong() * rowsPerFile)
            }
        val inputBytes = inputs.sumOf { it.localPath.fileSize() }

        println()
        println("== $shape: $files files x $rowsPerFile rows, inputs written SNAPPY (client default)")
        println("   inputs: ${fmt(inputBytes)}")
        println(
            String.format(
                "   %-14s %14s %10s %10s %10s",
                "output codec",
                "output bytes",
                "vs input",
                "vs uncomp",
                "rewrite s",
            ),
        )
        var uncompressed = 0L
        // UNCOMPRESSED first: it is the baseline the other rows divide by,
        // and it is the behaviour this measurement exists to price.
        val order =
            listOf(CompressionCodecName.UNCOMPRESSED) +
                ParquetRewriter.SUPPORTED.filter { it != CompressionCodecName.UNCOMPRESSED }.sortedBy { it.name }
        for (codec in order) {
            val out = dir.resolve("$shape-out-${codec.name}.parquet")
            val started = System.nanoTime()
            ParquetRewriter.rewrite(
                inputs,
                live,
                emptyList(),
                out,
                codec = ParquetRewriter.OutputCodec(codec),
            )
            val seconds = (System.nanoTime() - started) / 1e9
            val bytes = out.fileSize()
            if (codec == CompressionCodecName.UNCOMPRESSED) uncompressed = bytes
            println(
                String.format(
                    "   %-14s %14s %9.2fx %9s %10.2f",
                    codec.name.lowercase(),
                    fmt(bytes),
                    bytes.toDouble() / inputBytes,
                    if (uncompressed == 0L) "-" else String.format("%.2fx", bytes.toDouble() / uncompressed),
                    seconds,
                ),
            )
            // Assert-by-print: a codec that silently did not apply is the
            // failure this measurement is most likely to hide.
            println("                  footer says: ${footerCodecs(out)}")
            out.toFile().delete()
        }
        inputs.forEach { it.localPath.toFile().delete() }
    }

    /** Every column chunk's recorded codec, as the footer holds it. */
    private fun footerCodecs(path: Path): String =
        ParquetFileReader.open(LocalInputFile(path)).use { reader ->
            reader.footer.blocks
                .flatMap { it.columns }
                .map { "${it.path.toDotString()}=${it.codec}" }
                .distinctBy { it.substringAfter('=') }
                .joinToString(", ") { it.substringAfter('=') }
        }

    // ---- fabrication ------------------------------------------------------

    private fun schemaOf(shape: Shape): MessageType {
        val fields = mutableListOf<Type>()
        var id = 1

        fun string(name: String) {
            fields +=
                Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                    .`as`(LogicalTypeAnnotation.stringType())
                    .id(id++)
                    .named(name)
        }
        if (shape != Shape.LOW_CARDINALITY) {
            string("event_id")
            string("distinct_id")
            string("session_id")
            string("path")
        } else {
            id = 5
        }
        if (shape != Shape.HIGH_CARDINALITY) {
            string("country")
            string("browser")
            string("os")
            string("device_type")
        }
        fields +=
            Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                .`as`(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
                .id(TS_FIELD_ID + 20)
                .named("ts")
        fields += Types.optional(PrimitiveType.PrimitiveTypeName.INT64).id(TS_FIELD_ID + 21).named("duration_ms")
        fields += Types.optional(PrimitiveType.PrimitiveTypeName.INT32).id(TS_FIELD_ID + 22).named("viewport_width")
        return MessageType("pageviews", fields)
    }

    private fun liveColumns(shape: Shape): List<Column> {
        val cols = mutableListOf<Column>()
        var id = 1
        var ordinal = 0

        fun add(
            name: String,
            type: ColType,
            fieldId: Int,
        ) {
            cols += Column(fieldId.toLong(), ordinal++, ColumnDef(name, type))
        }
        if (shape != Shape.LOW_CARDINALITY) {
            for (n in listOf("event_id", "distinct_id", "session_id", "path")) add(n, ColType.STRING, id++)
        } else {
            id = 5
        }
        if (shape != Shape.HIGH_CARDINALITY) {
            for (n in listOf("country", "browser", "os", "device_type")) add(n, ColType.STRING, id++)
        }
        add("ts", ColType.TIMESTAMPTZ, TS_FIELD_ID + 20)
        add("duration_ms", ColType.LONG, TS_FIELD_ID + 21)
        add("viewport_width", ColType.INT, TS_FIELD_ID + 22)
        return cols
    }

    private val countries = (0 until 60).map { "C%02d".format(it) }
    private val browsers = listOf("Chrome", "Safari", "Firefox", "Edge", "Opera", "Chrome Mobile", "Safari Mobile")
    private val operatingSystems = listOf("macOS", "Windows", "Linux", "iOS", "Android", "ChromeOS")
    private val deviceTypes = listOf("Desktop", "Mobile", "Tablet")
    private val paths = (0 until 240).map { "/app/section$it/page" }

    private fun writeInput(
        path: Path,
        schema: MessageType,
        shape: Shape,
        rows: Int,
        seed: Long,
    ) {
        val rng = Random(seed)
        val factory = SimpleGroupFactory(schema)
        // SNAPPY on purpose: this is what the clients write, so "input
        // bytes" here is the number production actually pays today.
        ExampleParquetWriter.builder(LocalOutputFile(path))
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .build()
            .use { writer ->
                repeat(rows) {
                    val g = factory.newGroup()
                    var i = 0
                    if (shape != Shape.LOW_CARDINALITY) {
                        g.add(i++, uuidish(rng))
                        g.add(i++, "u_" + java.lang.Long.toHexString(rng.nextLong()))
                        g.add(i++, uuidish(rng))
                        g.add(i++, paths[rng.nextInt(paths.size)] + "?q=" + rng.nextInt(1_000_000))
                    }
                    if (shape != Shape.HIGH_CARDINALITY) {
                        g.add(i++, countries[rng.nextInt(countries.size)])
                        g.add(i++, browsers[rng.nextInt(browsers.size)])
                        g.add(i++, operatingSystems[rng.nextInt(operatingSystems.size)])
                        g.add(i++, deviceTypes[rng.nextInt(deviceTypes.size)])
                    }
                    g.add(i++, 1_700_000_000_000_000L + rng.nextInt(86_400_000).toLong() * 1000)
                    g.add(i++, rng.nextInt(120_000).toLong())
                    g.add(i, 320 + rng.nextInt(2240))
                    writer.write(g)
                }
            }
    }

    private fun uuidish(rng: Random): String = java.util.UUID(rng.nextLong(), rng.nextLong()).toString()

    private fun fmt(bytes: Long): String =
        when {
            bytes >= 1L shl 20 -> String.format("%.1f MiB", bytes / 1048576.0)
            else -> String.format("%.1f KiB", bytes / 1024.0)
        }
}
