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
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import kotlin.io.path.fileSize

/**
 * What codec a compaction output is ACTUALLY written with, read back out
 * of its parquet footer, per column chunk.
 *
 * The bug this pins: [ParquetRewriter] wrote UNCOMPRESSED, which is also
 * `ExampleParquetWriter`'s default, so the value was inherited rather
 * than chosen and no comment said otherwise. Because compaction is a
 * ratchet — the tier ladder rewrites a table's hot rows once per tier
 * and every output is the next tier's input — that made a fully
 * compacted table uncompressed end to end, permanently, against clients
 * that all write snappy or zstd. A dev-catalog run merged 67.6 MiB of
 * inputs into 80.1 MiB of output.
 *
 * The assertions are on the FOOTER, not on the config value, and per
 * COLUMN CHUNK, not per file. A test that reads back
 * `CompactionConfig.codec` proves the data class has a field; a test
 * that checks one chunk misses the row-id carrier, which parquet writes
 * like any other column and which is the one column the rewriter
 * synthesizes rather than copies. Both shapes would have stayed green
 * through the whole bug.
 *
 * Sizes are asserted too, because a codec NAME in a footer is cheap: the
 * failure mode that survives a name check is a knob that is read,
 * recorded and then not applied to the pages.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CompactionCodecTest {
    private val tmp: Path = Files.createTempDirectory("codec-test")

    @AfterAll
    fun tearDown() {
        tmp.toFile().deleteRecursively()
    }

    private val liveColumns =
        listOf(
            Column(1, 0, ColumnDef("event_id", ColType.STRING)),
            Column(2, 1, ColumnDef("country", ColType.STRING)),
            Column(3, 2, ColumnDef("duration_ms", ColType.LONG)),
        )

    @Test
    fun `the DEFAULT rewrite compresses, and the footer says which codec`() {
        // The gate. Nothing else in the suite takes the default path and
        // looks at the result: every other rewrite test asserts values,
        // ids or schema, all of which are identical under any codec. With
        // the rewriter back on UNCOMPRESSED this is the test that reds.
        val out = rewriteDefault("default.parquet")

        assertThat(codecsInFooter(out))
            .describedAs("every column chunk's recorded codec, default rewrite")
            .containsOnly(ParquetRewriter.DEFAULT_CODEC)
        assertThat(ParquetRewriter.DEFAULT_CODEC)
            .describedAs("the default must be a real codec; UNCOMPRESSED is the bug")
            .isNotEqualTo(CompressionCodecName.UNCOMPRESSED)
    }

    @Test
    fun `the row-id carrier is compressed like every other column`() {
        // _hog_row_id is the one column the rewriter synthesizes instead
        // of copying, so it is the one a per-file or first-chunk
        // assertion would skip. It is also int64 and monotone — the most
        // compressible column in the file.
        val out = rewriteDefault("carrier.parquet")
        val byPath = codecByColumnPath(out)

        assertThat(byPath)
            .describedAs("the row-id carrier must be in the footer at all")
            .containsKey(ParquetRewriter.ROW_ID_COLUMN)
        assertThat(byPath[ParquetRewriter.ROW_ID_COLUMN])
            .isEqualTo(ParquetRewriter.DEFAULT_CODEC)
    }

    @Test
    fun `every supported codec reaches the footer, and only it`() {
        // Each name the operator may set, end to end through the real
        // rewriter. UNCOMPRESSED is on the list deliberately — it is the
        // documented escape hatch, so it must keep working, and it is
        // what makes the sizes below comparable.
        for (codec in ParquetRewriter.SUPPORTED) {
            val out = rewrite("supported-${codec.name}.parquet", ParquetRewriter.OutputCodec(codec))
            assertThat(codecsInFooter(out))
                .describedAs("footer codecs for a rewrite configured %s", codec)
                .containsOnly(codec)
        }
    }

    @Test
    fun `the configured codec is applied to the pages, not just recorded`() {
        // The name check above passes on a writer that stamps a footer
        // and stores raw pages. Bytes are the only thing that does not.
        // Low-cardinality strings on purpose: parquet's own dictionary
        // has already taken the easy win, so what is left is the codec's.
        val uncompressed =
            rewrite("bytes-none.parquet", ParquetRewriter.OutputCodec(CompressionCodecName.UNCOMPRESSED)).fileSize()
        val defaulted = rewriteDefault("bytes-default.parquet").fileSize()

        assertThat(defaulted)
            .describedAs("default-codec output (%d B) vs uncompressed (%d B)", defaulted, uncompressed)
            .isLessThan(uncompressed)
        // Measured on event-shaped data (CodecMeasurement): zstd lands at
        // 0.43-0.45x of uncompressed on both column shapes. Half is a
        // floor loose enough for any codec on SUPPORTED that is not
        // UNCOMPRESSED, and tight enough that a codec applied to nothing
        // but the footer cannot reach it.
        assertThat(defaulted.toDouble() / uncompressed)
            .describedAs("compression ratio against uncompressed")
            .isLessThan(0.75)
    }

    @Test
    fun `an input's codec does not decide its output's`() {
        // Inputs may be any mix — pyarrow snappy, hedgerow zstd, an older
        // uncompressed compaction output — and none of them is an
        // instruction. The rewrite decodes and re-encodes, so the output
        // carries the configured codec whatever arrived.
        val snappyIn = writeInput("mixed-snappy.parquet", 0, CompressionCodecName.SNAPPY)
        val rawIn = writeInput("mixed-raw.parquet", 500, CompressionCodecName.UNCOMPRESSED)
        val gzipIn = writeInput("mixed-gzip.parquet", 1000, CompressionCodecName.GZIP)
        val out = tmp.resolve("mixed-out.parquet")

        rewriteToLocal(
            listOf(
                localInput(snappyIn, 0),
                localInput(rawIn, 500),
                localInput(gzipIn, 1000),
            ),
            liveColumns,
            emptyList(),
            out,
        )

        assertThat(codecsInFooter(out))
            .describedAs("three differently-compressed inputs, one output codec")
            .containsOnly(ParquetRewriter.DEFAULT_CODEC)
    }

    // ---- the knob's refusals ---------------------------------------------

    @Test
    fun `an unknown codec name is refused by name, listing the legal set`() {
        // The operator surface is a string. `valueOf` alone answers a typo
        // with a bare IllegalArgumentException naming the enum, which does
        // not tell an operator what to type instead — and LZO/BROTLI are
        // real CompressionCodecName values whose implementations are NOT
        // on this server's classpath, so `valueOf` would accept them at
        // boot and fail inside a rewrite, per group, forever.
        for (bad in listOf("lzo", "brotli", "zstandard", "", "snappy!")) {
            assertThatThrownBy { ParquetRewriter.OutputCodec.parse(bad) }
                .describedAs("codec name %s", bad)
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("zstd")
                .hasMessageContaining("snappy")
        }
    }

    @Test
    fun `codec names parse case-insensitively and tolerate surrounding space`() {
        // Env vars arrive from charts values and human hands.
        for (name in listOf("zstd", "ZSTD", "Zstd", " zstd ")) {
            assertThat(ParquetRewriter.OutputCodec.parse(name).name)
                .describedAs("codec name %s", name)
                .isEqualTo(CompressionCodecName.ZSTD)
        }
    }

    @Test
    fun `a zstd level outside zstd's own range is refused at construction`() {
        // Refused at boot rather than inside a rewrite: zstd-jni's answer
        // to an out-of-range level is a throw from deep inside the writer,
        // which the sweep would count as failed_groups and retry forever.
        for (bad in listOf(0, -1, 23, 100)) {
            assertThatThrownBy { ParquetRewriter.OutputCodec(CompressionCodecName.ZSTD, bad) }
                .describedAs("zstd level %d", bad)
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("zstd level")
        }
        for (ok in listOf(ParquetRewriter.MIN_ZSTD_LEVEL, 3, ParquetRewriter.MAX_ZSTD_LEVEL)) {
            ParquetRewriter.OutputCodec(CompressionCodecName.ZSTD, ok)
        }
    }

    @Test
    fun `a non-default zstd level still produces a readable zstd file`() {
        // The level travels through the writer's untyped config map, so a
        // wrong key name is silent: the file is still valid zstd at the
        // library default and nothing says the knob did nothing. Size is
        // what separates them — level 1 and level 19 cannot tie on this
        // data.
        val fast = rewrite("zstd-1.parquet", ParquetRewriter.OutputCodec(CompressionCodecName.ZSTD, 1))
        val slow = rewrite("zstd-19.parquet", ParquetRewriter.OutputCodec(CompressionCodecName.ZSTD, 19))

        assertThat(codecsInFooter(fast)).containsOnly(CompressionCodecName.ZSTD)
        assertThat(codecsInFooter(slow)).containsOnly(CompressionCodecName.ZSTD)
        assertThat(slow.fileSize())
            .describedAs("level 19 (%d B) must beat level 1 (%d B), or the level key is not reaching the codec")
            .isLessThan(fast.fileSize())
    }

    // ---- helpers ----------------------------------------------------------

    /** Rewrite two inputs with the rewriter's own default codec. */
    private fun rewriteDefault(name: String): Path = rewrite(name, null)

    private fun rewrite(
        name: String,
        codec: ParquetRewriter.OutputCodec?,
    ): Path {
        val a = writeInput("in-a-$name", 0, CompressionCodecName.SNAPPY)
        val b = writeInput("in-b-$name", ROWS.toLong(), CompressionCodecName.SNAPPY)
        val out = tmp.resolve("out-$name")
        val inputs = listOf(localInput(a, 0), localInput(b, ROWS.toLong()))
        if (codec == null) {
            rewriteToLocal(inputs, liveColumns, emptyList(), out)
        } else {
            rewriteToLocal(inputs, liveColumns, emptyList(), out, codec = codec)
        }
        return out
    }

    /** Every column chunk's recorded codec, across every row group. */
    private fun codecsInFooter(path: Path): List<CompressionCodecName> =
        ParquetFileReader.open(LocalInputFile(path)).use { reader ->
            reader.footer.blocks.flatMap { block -> block.columns.map { it.codec } }
        }

    private fun codecByColumnPath(path: Path): Map<String, CompressionCodecName> =
        ParquetFileReader.open(LocalInputFile(path)).use { reader ->
            reader.footer.blocks
                .flatMap { block -> block.columns }
                .associate { it.path.toDotString() to it.codec }
        }

    private val inputSchema: MessageType =
        Types.buildMessage()
            .addField(
                Types.optional(PrimitiveTypeName.BINARY)
                    .`as`(LogicalTypeAnnotation.stringType()).id(1).named("event_id"),
            )
            .addField(
                Types.optional(PrimitiveTypeName.BINARY)
                    .`as`(LogicalTypeAnnotation.stringType()).id(2).named("country"),
            )
            .addField(Types.optional(PrimitiveTypeName.INT64).id(3).named("duration_ms"))
            .named("events")

    /**
     * Event-shaped rows: a near-unique id per row (where the codec is the
     * only compression available) alongside a 60-value enum (where the
     * dictionary has already won). Deterministic — seeded from [offset] —
     * so a size assertion is not a coin flip.
     */
    private fun writeInput(
        name: String,
        offset: Long,
        codec: CompressionCodecName,
    ): Path {
        val path = tmp.resolve(name)
        val factory = SimpleGroupFactory(inputSchema)
        val rng = java.util.Random(offset)
        ExampleParquetWriter.builder(LocalOutputFile(path))
            .withType(inputSchema)
            .withCompressionCodec(codec)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .build()
            .use { w ->
                repeat(ROWS) { i ->
                    val g = factory.newGroup()
                    g.add("event_id", UUID(rng.nextLong(), rng.nextLong()).toString())
                    g.add("country", "C%02d".format(rng.nextInt(60)))
                    g.add("duration_ms", offset + i)
                    w.write(g)
                }
            }
        return path
    }

    private companion object {
        /** Enough rows that page/dictionary overheads do not dominate the sizes asserted above. */
        const val ROWS = 20_000
    }
}
