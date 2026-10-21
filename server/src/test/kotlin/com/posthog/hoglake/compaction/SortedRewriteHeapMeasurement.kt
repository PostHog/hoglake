package com.posthog.hoglake.compaction

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
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.io.OutputFile
import org.apache.parquet.io.PositionOutputStream
import org.apache.parquet.io.SeekableInputStream
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.util.Random

/**
 * Whether a sorted rewrite's PEAK retained heap is flat in group size at
 * a fixed chunk — the property the external merge sort exists for —
 * manual, like [SortedHeapMeasurement], because it is evidence rather
 * than a gate.
 *
 *   ./gradlew writeTestClasspath
 *   java -Xmx3g -cp "$(cat build/test-classpath.txt)" \
 *     com.posthog.hoglake.compaction.SortedRewriteHeapMeasurement [chunkRows] [maxFiles] [files,files,...]
 *
 * Groups of 2, 4, 8 ... [maxFiles] inputs of 100k flat event rows each,
 * sorted by a random timestamp so every chunk interleaves with every
 * other, rewritten three ways:
 *
 *  - UNSORTED: the streaming path, the floor — the input row group being
 *    read plus the output writer's buffered row group (up to the 128 MiB
 *    output block), which every rewrite pays.
 *  - EXTERNAL: the sorted path at a fixed `chunkRows`.
 *  - ONE CHUNK: the sorted path with a chunk large enough for the whole
 *    group, i.e. the in-memory whole-group sort #134 removed (stopped at
 *    800k rows, where it needs ~1.3 GB).
 *
 * Retained heap is sampled at a full GC from inside the rewrite: at
 * every input row-group read (the chunk filling) and at output writes
 * (throttled; the merge flushing a row group), so samples land where
 * the peaks are. The first is the CHUNK-phase peak, the second the
 * MERGE-phase one.
 */
object SortedRewriteHeapMeasurement {
    private const val ROWS_PER_FILE = 100_000

    @JvmStatic
    fun main(args: Array<String>) {
        val chunkRows = args.getOrNull(0)?.toLong() ?: 50_000L
        // Optional third argument: exact group sizes in FILES, comma
        // separated (e.g. `32,64,80` for 3.2M/6.4M/8M rows), instead of
        // doubling from 2 — a full 512 MiB zstd group is ~7.7M rows,
        // which no power of two lands on.
        val counts = args.getOrNull(2)?.split(",")?.map { it.trim().toInt() }
        val maxFiles = counts?.max() ?: args.getOrNull(1)?.toInt() ?: 16
        val dir = Files.createTempDirectory("sorted-rewrite-heap")
        try {
            val files = (0 until maxFiles).map { i -> dir.resolve("in-$i.parquet").also { write(it, i) } }
            println("== flat PAGEVIEWS shape, $ROWS_PER_FILE rows per input, chunkRows=$chunkRows")
            println(
                String.format(
                    "   %10s %6s %14s %14s %14s %14s",
                    "rows",
                    "runs",
                    "unsorted",
                    "ext. chunk",
                    "ext. merge",
                    "one chunk",
                ),
            )
            for (n in counts ?: generateSequence(2) { it * 2 }.takeWhile { it <= maxFiles }.toList()) {
                val group = files.take(n)
                val unsorted = peak(group, dir, sort = false, chunkRows = chunkRows)
                val external = peak(group, dir, sort = true, chunkRows = chunkRows)
                val oneChunk =
                    if (n * ROWS_PER_FILE <= 800_000) {
                        peak(
                            group,
                            dir,
                            sort = true,
                            chunkRows = Long.MAX_VALUE,
                        )
                    } else {
                        null
                    }
                println(
                    String.format(
                        "   %,10d %6d %,12d K %,12d K %,12d K %14s",
                        n.toLong() * ROWS_PER_FILE,
                        external.runs,
                        unsorted.overall / 1024,
                        external.reading / 1024,
                        external.writing / 1024,
                        oneChunk?.let { String.format("%,12d K", it.overall / 1024) } ?: "-",
                    ),
                )
            }
        } finally {
            dir.toFile().deleteRecursively()
        }
    }

    /**
     * Peaks above the pre-run baseline: [reading] sampled at input reads
     * (the chunk phase, on the sorted path), [writing] at output writes
     * (the merge, which is the only phase that writes the output).
     */
    private class Peaks(val reading: Long, val writing: Long, val runs: Int) {
        val overall: Long get() = maxOf(reading, writing)
    }

    private fun peak(
        group: List<Path>,
        dir: Path,
        sort: Boolean,
        chunkRows: Long,
    ): Peaks {
        val sampler = Sampler()
        val base = sampler.settled()
        val out = dir.resolve("out.parquet")
        val spillDir = Files.createDirectories(dir.resolve("spill"))
        val result =
            ParquetRewriter.rewrite(
                group.mapIndexed { i, path ->
                    ParquetRewriter.Input(
                        source = SampledInput(path, sampler),
                        label = path.fileName.toString(),
                        rowIdStart = i.toLong() * ROWS_PER_FILE,
                        fileSizeBytes = Files.size(path),
                        survivingRecords = ROWS_PER_FILE.toLong(),
                    )
                },
                live,
                if (sort) listOf(SortFieldDef(8, SortDirection.ASC, NullOrder.NULLS_LAST)) else emptyList(),
                SampledOutput(out, sampler),
                spill =
                    if (sort) {
                        SortSpill(
                            chunkRows = chunkRows,
                            mergeBudgetBytes = Long.MAX_VALUE / 4,
                            spillBudgetBytes = Long.MAX_VALUE / 4,
                            spillDir = spillDir,
                        )
                    } else {
                        null
                    },
            )
        Files.deleteIfExists(out)
        return Peaks(sampler.readPeak - base, sampler.writePeak - base, result.runsSpilled)
    }

    private class Sampler {
        var readPeak = 0L
            private set
        var writePeak = 0L
            private set
        private var last = 0L

        fun sample(writing: Boolean) {
            // Reads are sampled EVERY time: they are one per input row
            // group, and a time throttle phase-locks onto the spill (the
            // first read after a slow spill always samples, and always
            // sees an empty chunk). Writes are many small page writes per
            // output flush, so those are throttled.
            val now = System.nanoTime()
            if (writing && now - last < 150_000_000L) return
            val used = settled()
            if (writing) writePeak = maxOf(writePeak, used) else readPeak = maxOf(readPeak, used)
            last = System.nanoTime()
        }

        fun settled(): Long {
            System.gc()
            val rt = Runtime.getRuntime()
            return rt.totalMemory() - rt.freeMemory()
        }
    }

    private class SampledInput(path: Path, private val sampler: Sampler) : InputFile {
        private val delegate = LocalInputFile(path)

        override fun getLength(): Long = delegate.length

        override fun newStream(): SeekableInputStream {
            val d = delegate.newStream()
            return object : SeekableInputStream() {
                override fun getPos(): Long = d.pos

                override fun seek(newPos: Long) = d.seek(newPos)

                override fun read(): Int = d.read()

                override fun read(
                    b: ByteArray,
                    off: Int,
                    len: Int,
                ): Int = d.read(b, off, len).also { sampler.sample(writing = false) }

                override fun readFully(bytes: ByteArray) = d.readFully(bytes).also { sampler.sample(writing = false) }

                override fun readFully(
                    bytes: ByteArray,
                    start: Int,
                    len: Int,
                ) = d.readFully(bytes, start, len).also { sampler.sample(writing = false) }

                override fun read(buf: ByteBuffer): Int = d.read(buf).also { sampler.sample(writing = false) }

                override fun readFully(buf: ByteBuffer) = d.readFully(buf).also { sampler.sample(writing = false) }

                override fun close() = d.close()
            }
        }
    }

    private class SampledOutput(private val path: Path, private val sampler: Sampler) : OutputFile {
        private val delegate = LocalOutputFile(path)

        override fun create(blockSizeHint: Long): PositionOutputStream = wrap(delegate.create(blockSizeHint))

        override fun createOrOverwrite(blockSizeHint: Long): PositionOutputStream =
            wrap(delegate.createOrOverwrite(blockSizeHint))

        override fun supportsBlockSize(): Boolean = delegate.supportsBlockSize()

        override fun defaultBlockSize(): Long = delegate.defaultBlockSize()

        override fun getPath(): String = path.toString()

        private fun wrap(d: PositionOutputStream) =
            object : PositionOutputStream() {
                override fun getPos(): Long = d.pos

                override fun write(b: Int) = d.write(b)

                override fun write(
                    b: ByteArray,
                    off: Int,
                    len: Int,
                ) {
                    // Before the bytes leave: the writer still holds the
                    // row group it is flushing.
                    sampler.sample(writing = true)
                    d.write(b, off, len)
                }

                override fun flush() = d.flush()

                override fun close() = d.close()
            }
    }

    // ---- fixture: SortedHeapMeasurement's pageview shape ----------------------

    private val names = listOf("event_id", "distinct_id", "session_id", "path", "country", "browser", "os")

    private val live: List<Column> =
        names.mapIndexed { i, n -> Column(i + 1L, i, ColumnDef(n, ColType.STRING)) } +
            listOf(
                Column(8, 7, ColumnDef("ts", ColType.TIMESTAMPTZ)),
                Column(9, 8, ColumnDef("duration_ms", ColType.LONG)),
                Column(10, 9, ColumnDef("viewport_width", ColType.INT)),
            )

    private val schema: MessageType =
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
            .named("pageviews")

    private fun write(
        path: Path,
        seed: Int,
    ) {
        val rng = Random(seed.toLong())
        val factory = SimpleGroupFactory(schema)
        ExampleParquetWriter.builder(LocalOutputFile(path))
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            // Small row groups, as client writers produce, so the
            // sampler sees reads all through the chunk phase.
            .withRowGroupSize(1L shl 20)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .build()
            .use { writer ->
                repeat(ROWS_PER_FILE) {
                    val g = factory.newGroup()
                    g.add(0, java.util.UUID(rng.nextLong(), rng.nextLong()).toString())
                    g.add(1, "u_" + java.lang.Long.toHexString(rng.nextLong()))
                    g.add(2, java.util.UUID(rng.nextLong(), rng.nextLong()).toString())
                    g.add(3, "/app/section${rng.nextInt(240)}/page?q=${rng.nextInt(1_000_000)}")
                    g.add(4, "C%02d".format(rng.nextInt(60)))
                    g.add(5, listOf("Chrome", "Safari", "Firefox", "Edge")[rng.nextInt(4)])
                    g.add(6, listOf("macOS", "Windows", "Linux", "iOS")[rng.nextInt(4)])
                    g.add(7, 1_700_000_000_000_000L + rng.nextInt(86_400_000).toLong() * 1000)
                    g.add(8, rng.nextInt(120_000).toLong())
                    g.add(9, 320 + rng.nextInt(2240))
                    writer.write(g)
                }
            }
    }
}
