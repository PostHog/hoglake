package com.posthog.hoglake.compaction

import com.posthog.hoglake.ParquetReaders
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import java.nio.file.Files
import java.nio.file.Path
import java.util.Random

/**
 * CPU of a sorted rewrite whose merge has many runs — manual, like
 * [SortedRewriteHeapMeasurement], because it is evidence rather than a
 * gate. Three modes:
 *
 *   ./gradlew writeTestClasspath
 *   java -Xmx3g -cp "$(cat build/test-classpath.txt)" \
 *     com.posthog.hoglake.compaction.SortedRewriteCpuMeasurement spill [runs] [rowsPerRun] [repeats]
 *     ... SortedRewriteCpuMeasurement presorted [files] [rowsPerFile] [repeats]
 *     ... SortedRewriteCpuMeasurement tiny [files] [rowsPerFile] [repeats]
 *     ... SortedRewriteCpuMeasurement unsorted [files] [rowsPerFile] [repeats]
 *     ... SortedRewriteCpuMeasurement dropped [files] [rowsPerFile] [repeats]
 *     ... SortedRewriteCpuMeasurement append-unsorted [files] [rowsPerFile] [repeats] [floor]
 *     ... SortedRewriteCpuMeasurement append-sorted [files] [rowsPerFile] [repeats] [floor]
 *
 *  - `spill`: [runs] UNSORTED inputs of [rowsPerRun] rows, `chunkRows` =
 *    [rowsPerRun], so every input is one chunk and one spilled run — the
 *    cost of a key comparison, which the merge pays ~log2(k) times a row
 *    and each chunk sort n·log2(n).
 *  - `presorted`: the inputs already sorted by the spec (millpond's
 *    flushes). Timed with the sortedness pre-pass on (floor 0: each file
 *    verified and merged in place) and off (floor = MAX: every file
 *    spilled, the behaviour before the pre-pass); also prints the bytes
 *    the pre-pass read against the files' bytes.
 *  - `tiny`: many small pre-sorted inputs (default 2,000 of ~12 KiB):
 *    floor 0 (verify and reopen every file, a run each) against the
 *    default 16 MiB floor (the chunk path).
 *  - `unsorted`: the same inputs rewritten with NO sort order — the
 *    streaming path, decode and encode with nothing in between.
 *  - `dropped`: as `unsorted`, but every input also carries ten string
 *    columns (field ids 101-110) the live schema has DROPPED — the cost
 *    of reading columns the output does not keep.
 *  - `append-unsorted`: [files] PRIOR OUTPUTS (each a client file
 *    rewritten through the rewriter first) merged with no sort order:
 *    appended byte for byte (append floor [floor], default 0) against
 *    decoded and re-encoded (floor MAX, the path before package D1).
 *  - `append-sorted`: [files] prior outputs written under the spec, each
 *    with its own band of countries and no null country, so their
 *    first-key ranges are disjoint: appended in key order against merged.
 *
 * The spec is three keys: country ASC NULLS LAST (string, many ties,
 * ~5% null), duration_ms DESC NULLS FIRST (long, ~10% null), ts ASC.
 * One warm-up, then [repeats] timed rewrites; prints each and the median.
 */
object SortedRewriteCpuMeasurement {
    @JvmStatic
    fun main(args: Array<String>) {
        val mode = args.getOrNull(0) ?: "spill"
        val tiny = mode == "tiny"
        val files = args.getOrNull(1)?.toInt() ?: if (tiny) 2_000 else 16
        val rowsPerFile = args.getOrNull(2)?.toInt() ?: if (tiny) 100 else 50_000
        val repeats = args.getOrNull(3)?.toInt() ?: 5
        val dir = Files.createTempDirectory("sorted-rewrite-cpu")
        try {
            if (mode.startsWith("append-")) {
                appendMode(dir, mode == "append-sorted", files, rowsPerFile, repeats, args.getOrNull(4)?.toLong() ?: 0)
                return
            }
            val sorted = mode != "spill" && mode != "unsorted" && mode != "dropped"
            val extra = if (mode == "dropped") DROPPED_COLUMNS else 0
            val paths =
                (0 until files).map { i ->
                    dir.resolve("in-$i.parquet").also { write(it, i, rowsPerFile, sorted, extra = extra) }
                }
            val spillDir = Files.createDirectories(dir.resolve("spill"))
            val fileBytes = paths.sumOf { Files.size(it) }

            fun once(floor: Long): ParquetRewriter.RewriteResult {
                val out = dir.resolve("out.parquet")
                val result =
                    ParquetRewriter.rewrite(
                        paths.mapIndexed { i, path ->
                            ParquetRewriter.Input(
                                source = LocalInputFile(path),
                                label = path.fileName.toString(),
                                rowIdStart = i.toLong() * rowsPerFile,
                                fileSizeBytes = Files.size(path),
                                survivingRecords = rowsPerFile.toLong(),
                            )
                        },
                        live,
                        if (mode == "unsorted" || mode == "dropped") emptyList() else sort,
                        LocalOutputFile(out),
                        spill =
                            SortSpill(
                                chunkRows = if (tiny) 50_000L else rowsPerFile.toLong(),
                                mergeBudgetBytes = Long.MAX_VALUE / 4,
                                spillBudgetBytes = Long.MAX_VALUE / 4,
                                spillDir = spillDir,
                                verifyMinBytes = floor,
                            ),
                    )
                Files.deleteIfExists(out)
                return result
            }

            fun timed(
                label: String,
                floor: Long,
            ) {
                once(floor)
                var last: ParquetRewriter.RewriteResult? = null
                val times =
                    (1..repeats).map {
                        val started = System.nanoTime()
                        last = once(floor)
                        (System.nanoTime() - started) / 1_000_000
                    }
                val r = last!!
                println(
                    "   $label: median ${times.sorted()[times.size / 2]} ms (${times.joinToString(", ")}); " +
                        "verified=${r.filesVerified} unchecked=${r.filesUnchecked} trusted=${r.runsTrusted} " +
                        "spilled=${r.runsSpilled} spill_bytes=${r.spillBytes} sort_check_bytes=${r.sortCheckBytes}",
                )
            }
            println("== $mode: $files inputs x $rowsPerFile rows ($fileBytes file bytes), 3-key sort")
            when (mode) {
                "spill" -> timed("spilled (no pre-pass)", Long.MAX_VALUE)
                "unsorted" -> timed("streamed, no sort order", Long.MAX_VALUE)
                "dropped" -> timed("streamed, $DROPPED_COLUMNS dropped columns per input", Long.MAX_VALUE)
                "presorted" -> {
                    timed("pre-pass, merged in place (floor 0)", 0)
                    timed("no pre-pass, all spilled (floor MAX)", Long.MAX_VALUE)
                }
                "tiny" -> {
                    timed("verify + reopen every file (floor 0)", 0)
                    timed("chunk path (floor 16 MiB)", CompactionConfig.DEFAULT_VERIFY_MIN_BYTES)
                }
                else -> error("unknown mode $mode")
            }
        } finally {
            dir.toFile().deleteRecursively()
        }
    }

    /** See the class doc's `append-*` modes. */
    private fun appendMode(
        dir: Path,
        sorted: Boolean,
        files: Int,
        rowsPerFile: Int,
        repeats: Int,
        floor: Long,
    ) {
        val spec = if (sorted) sort else emptyList()
        val priors =
            (0 until files).map { i ->
                val client =
                    dir.resolve(
                        "client-$i.parquet",
                    ).also { write(it, i, rowsPerFile, sorted, band = if (sorted) i else null) }
                val prior = dir.resolve("prior-$i.parquet")
                ParquetRewriter.rewrite(
                    listOf(
                        ParquetRewriter.Input(
                            LocalInputFile(client),
                            client.fileName.toString(),
                            i.toLong() * rowsPerFile,
                        ),
                    ),
                    live,
                    spec,
                    LocalOutputFile(prior),
                    spill = if (sorted) roomySpill(Files.createDirectories(dir.resolve("prior-spill"))) else null,
                )
                Files.delete(client)
                prior
            }
        val spillDir = Files.createDirectories(dir.resolve("spill"))
        val priorBytes = priors.sumOf { Files.size(it) }
        val rowGroups =
            priors.map {
                    p ->
                ParquetReaders.open(LocalInputFile(p)).use { r -> r.footer.blocks.map { it.compressedSize } }
            }

        fun once(appendFloor: Long): ParquetRewriter.RewriteResult {
            val out = dir.resolve("out.parquet")
            val result =
                ParquetRewriter.rewrite(
                    priors.map { path ->
                        ParquetRewriter.Input(
                            source = LocalInputFile(path),
                            label = path.fileName.toString(),
                            rowIdStart = 0,
                            explicitRowIds = true,
                            trustedSorted = sorted,
                            fileSizeBytes = Files.size(path),
                            survivingRecords = rowsPerFile.toLong(),
                        )
                    },
                    live,
                    spec,
                    LocalOutputFile(out),
                    spill =
                        SortSpill(
                            chunkRows = rowsPerFile.toLong(),
                            mergeBudgetBytes = Long.MAX_VALUE / 4,
                            spillBudgetBytes = Long.MAX_VALUE / 4,
                            spillDir = spillDir,
                        ),
                    appendFloorBytes = appendFloor,
                )
            Files.deleteIfExists(out)
            return result
        }

        fun timed(
            label: String,
            appendFloor: Long,
        ) {
            once(appendFloor)
            var last: ParquetRewriter.RewriteResult? = null
            val times =
                (1..repeats).map {
                    val started = System.nanoTime()
                    last = once(appendFloor)
                    (System.nanoTime() - started) / 1_000_000
                }
            val r = last!!
            println(
                "   $label: median ${times.sorted()[times.size / 2]} ms (${times.joinToString(", ")}); " +
                    "row_groups_appended=${r.rowGroupsAppended} bytes_appended=${r.bytesAppended} " +
                    "trusted=${r.runsTrusted} spilled=${r.runsSpilled} rows=${r.rowsWritten}",
            )
        }
        val sizes = rowGroups.flatten()
        println(
            "== append-${if (sorted) "sorted" else "unsorted"}: $files prior outputs x $rowsPerFile rows " +
                "($priorBytes bytes; row groups ${sizes.minOrNull()}..${sizes.maxOrNull()} B), " +
                "append floor $floor",
        )
        timed("appended (floor $floor)", floor)
        timed("decoded and re-encoded (floor MAX)", Long.MAX_VALUE)
    }

    private val sort =
        listOf(
            SortFieldDef(5, SortDirection.ASC, NullOrder.NULLS_LAST),
            SortFieldDef(9, SortDirection.DESC, NullOrder.NULLS_FIRST),
            SortFieldDef(8, SortDirection.ASC, NullOrder.NULLS_LAST),
        )

    private val names = listOf("event_id", "distinct_id", "session_id", "path", "country", "browser", "os")

    private val live: List<Column> =
        names.mapIndexed { i, n -> Column(i + 1L, i, ColumnDef(n, ColType.STRING)) } +
            listOf(
                Column(8, 7, ColumnDef("ts", ColType.TIMESTAMPTZ)),
                Column(9, 8, ColumnDef("duration_ms", ColType.LONG)),
                Column(10, 9, ColumnDef("viewport_width", ColType.INT)),
            )

    private const val DROPPED_COLUMNS = 10

    private val schema: MessageType = schemaWith(0)

    /** The pageview schema plus [extra] string columns of field ids 101.. the live schema does not have. */
    private fun schemaWith(extra: Int): MessageType =
        Types.buildMessage()
            .apply {
                names.forEachIndexed { i, n ->
                    addField(
                        Types.optional(
                            PrimitiveTypeName.BINARY,
                        ).`as`(LogicalTypeAnnotation.stringType()).id(i + 1).named(n),
                    )
                }
            }
            .addField(
                Types.optional(PrimitiveTypeName.INT64)
                    .`as`(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
                    .id(8).named("ts"),
            )
            .addField(Types.optional(PrimitiveTypeName.INT64).id(9).named("duration_ms"))
            .addField(Types.optional(PrimitiveTypeName.INT32).id(10).named("viewport_width"))
            .apply {
                for (i in 0 until extra) {
                    addField(
                        Types.optional(PrimitiveTypeName.BINARY).`as`(LogicalTypeAnnotation.stringType())
                            .id(101 + i).named("dropped_$i"),
                    )
                }
            }
            .named("pageviews")

    private class Event(
        val eventId: String,
        val distinctId: String,
        val sessionId: String,
        val path: String,
        val country: String?,
        val browser: String,
        val os: String,
        val ts: Long,
        val duration: Long?,
        val viewport: Int,
    )

    /** The spec's order, written out (country ASC NULLS LAST, duration DESC NULLS FIRST, ts ASC). */
    private val specOrder: Comparator<Event> =
        compareBy<Event, String?>(nullsLast()) { it.country }
            .then(compareByDescending<Event, Long?>(nullsLast()) { it.duration })
            .thenBy { it.ts }

    private fun write(
        path: Path,
        seed: Int,
        rows: Int,
        sorted: Boolean,
        /** A band of three countries of this file's own, never null; null = all 60, ~5% null. */
        band: Int? = null,
        /** Dropped columns to add (see [schemaWith]). */
        extra: Int = 0,
    ) {
        val schema = if (extra == 0) schema else schemaWith(extra)
        val rng = Random(seed.toLong())
        val events =
            (0 until rows).map {
                Event(
                    java.util.UUID(rng.nextLong(), rng.nextLong()).toString(),
                    "u_" + java.lang.Long.toHexString(rng.nextLong()),
                    java.util.UUID(rng.nextLong(), rng.nextLong()).toString(),
                    "/app/section${rng.nextInt(240)}/page?q=${rng.nextInt(1_000_000)}",
                    when {
                        band != null -> "C%03d".format(band * 3 + rng.nextInt(3))
                        rng.nextInt(20) != 0 -> "C%02d".format(rng.nextInt(60))
                        else -> null
                    },
                    listOf("Chrome", "Safari", "Firefox", "Edge")[rng.nextInt(4)],
                    listOf("macOS", "Windows", "Linux", "iOS")[rng.nextInt(4)],
                    1_700_000_000_000_000L + rng.nextInt(86_400_000).toLong() * 1000,
                    if (rng.nextInt(10) != 0) rng.nextInt(2_000).toLong() else null,
                    320 + rng.nextInt(2240),
                )
            }.let { if (sorted) it.sortedWith(specOrder) else it }
        val factory = SimpleGroupFactory(schema)
        ExampleParquetWriter.builder(LocalOutputFile(path))
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .build()
            .use { writer ->
                for (e in events) {
                    val g = factory.newGroup()
                    g.add(0, e.eventId)
                    g.add(1, e.distinctId)
                    g.add(2, e.sessionId)
                    g.add(3, e.path)
                    e.country?.let { g.add(4, it) }
                    g.add(5, e.browser)
                    g.add(6, e.os)
                    g.add(7, e.ts)
                    e.duration?.let { g.add(8, it) }
                    g.add(9, e.viewport)
                    for (i in 0 until extra) g.add(10 + i, "${e.eventId}-$i-${e.path}")
                    writer.write(g)
                }
            }
    }
}
