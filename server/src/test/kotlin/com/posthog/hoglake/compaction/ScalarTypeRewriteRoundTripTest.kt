package com.posthog.hoglake.compaction

import com.posthog.hoglake.hydrator.CatalogColumn
import com.posthog.hoglake.hydrator.FooterStats
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.stats.IcebergSingleValue
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.math.BigInteger
import java.nio.file.Files
import java.nio.file.Path

/**
 * The scalar-parity types through the WHOLE loop a compacted file makes:
 * writer footer -> [FooterStats] bounds -> [ParquetRewriter] -> footer
 * again -> bounds again, and once more so re-compaction is proven
 * idempotent rather than merely plausible.
 *
 * Each half of that loop already has unit coverage (FooterStatsTest for
 * the decode, ParquetRewriterTest for the rewrite), and each is correct
 * on its own. What neither can see is DISAGREEMENT: the bound encoding
 * follows the type's MAPPED Iceberg type, the physical parquet form
 * follows what a parquet reader can represent, and the two move
 * independently. Compaction is the one place both are chosen at once, so
 * it is the one place a divergence turns into permanently wrong metadata
 * on a file nobody will re-read.
 *
 * The bounds are therefore asserted IDENTICAL byte-for-byte across the
 * loop, never merely "still decodable": a file's stats row is rewritten
 * from the compacted footer at commit, so any drift silently replaces a
 * correct bound with a different one. Every decoded pair also gets a
 * lower <= upper check, because an inverted range is this area's
 * recurring failure (sign-extension and unit confusion both produce it)
 * and a pruner reading an inverted range drops the file's data whole.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ScalarTypeRewriteRoundTripTest {
    private val tmp: Path = Files.createTempDirectory("scalar-roundtrip")

    @AfterAll
    fun tearDown() {
        tmp.toFile().deleteRecursively()
    }

    // ---- the five that round-trip without changing shape ----------------

    @Test
    fun `the small integer widths keep both their bounds and their annotation`() {
        // int8/int16/uint8/uint16 all ride INT32 and all map to Iceberg
        // int, so compaction should be a pure copy: same 4-byte bound,
        // same INT(width, signed) annotation, twice over. Covered
        // explicitly because "nothing happens" is exactly the claim a
        // rewrite is most likely to break quietly.
        val widths =
            listOf(
                Triple(ColType.INT8, 8, true),
                Triple(ColType.INT16, 16, true),
                Triple(ColType.UINT8, 8, false),
                Triple(ColType.UINT16, 16, false),
            )
        for ((type, width, signed) in widths) {
            val annotation = LogicalTypeAnnotation.intType(width, signed)
            val input = oneColumn(PrimitiveTypeName.INT32, annotation)
            // The domain edges for this width, in the leaf's own order.
            val lo = if (signed) -(1 shl (width - 1)) else 0
            val hi = if (signed) (1 shl (width - 1)) - 1 else (1 shl width) - 1
            val rows =
                listOf<(Group) -> Unit>(
                    { g -> g.add("v", lo) },
                    { g -> g.add("v", 0) },
                    { g -> g.add("v", hi) },
                )
            val trip = roundTrip("i$width-$signed", input, type, rows)
            assertThat(trip.lower).describedAs(type.wire).isEqualTo(IcebergSingleValue.encodeInt(lo))
            assertThat(trip.upper).describedAs(type.wire).isEqualTo(IcebergSingleValue.encodeInt(hi))
            for (out in listOf(trip.first, trip.second)) {
                assertThat(physical(out).primitiveTypeName)
                    .describedAs("%s stays INT32", type.wire)
                    .isEqualTo(PrimitiveTypeName.INT32)
                assertThat(physical(out).logicalTypeAnnotation)
                    .describedAs("%s keeps its INT(%d, %s) annotation", type.wire, width, signed)
                    .isEqualTo(annotation)
            }
        }
    }

    @Test
    fun `timestamp_ms bounds stay micros while its files stay millis`() {
        // The same declared-unit/stored-unit split as timestamp_s, and it
        // needs its own case: the two share a physical form but are
        // different catalog types, so a rewrite could plausibly get one
        // right and the other wrong.
        val input =
            oneColumn(
                PrimitiveTypeName.INT64,
                LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS),
            )
        val rows =
            listOf<(Group) -> Unit>(
                { g -> g.add("v", -1_500L) },
                { g -> g.add("v", 0L) },
                { g -> g.add("v", 1_788_609_600_123L) },
            )
        val trip = roundTrip("ts-ms", input, ColType.TIMESTAMP_MS, rows)
        assertThat(trip.lower).isEqualTo(IcebergSingleValue.encodeTimestampMicros(-1_500_000L))
        assertThat(trip.upper).isEqualTo(IcebergSingleValue.encodeTimestampMicros(1_788_609_600_123_000L))
        for (out in listOf(trip.first, trip.second)) {
            assertThat(
                (physical(out).logicalTypeAnnotation as LogicalTypeAnnotation.TimestampLogicalTypeAnnotation).unit,
            ).describedAs("output file stays MILLIS").isEqualTo(LogicalTypeAnnotation.TimeUnit.MILLIS)
        }
    }

    // ---- uint32 --------------------------------------------------------

    @Test
    fun `uint32 keeps its bounds while deliberately changing physical type`() {
        // An arrow writer emits uint32 as INT32 + INT(32, unsigned);
        // hoglake writes a PLAIN INT64, because uint32 maps to Iceberg
        // long and an Iceberg reader takes an INT32 column as signed —
        // 0xFFFFFFFF would read back as -1. Both forms carry the same
        // 8-byte LE long bound, which is exactly what makes the shape
        // change safe to perform silently.
        val input = oneColumn(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(32, false))
        val rows =
            listOf<(Group) -> Unit>(
                { g -> g.add("v", 0) },
                // 2^31 and 2^32-1: the two values a signed reading breaks on.
                { g -> g.add("v", Int.MIN_VALUE) },
                { g -> g.add("v", -1) },
            )
        val trip = roundTrip("u32", input, ColType.UINT32, rows)
        assertThat(trip.lower).isEqualTo(IcebergSingleValue.encodeLong(0L))
        assertThat(trip.upper).isEqualTo(IcebergSingleValue.encodeLong(4_294_967_295L))

        // The two facts that must hold together: bounds unchanged (above,
        // through roundTrip), physical form changed (here).
        assertThat(physical(trip.input).primitiveTypeName).isEqualTo(PrimitiveTypeName.INT32)
        for (out in listOf(trip.first, trip.second)) {
            assertThat(physical(out).primitiveTypeName)
                .describedAs("compaction output %s", out.fileName)
                .isEqualTo(PrimitiveTypeName.INT64)
            assertThat(physical(out).logicalTypeAnnotation)
                .describedAs("a plain int64, not an annotated unsigned one")
                .isNull()
        }
    }

    // ---- uint64 --------------------------------------------------------

    @Test
    fun `uint64 keeps its decimal bounds across compaction`() {
        // uint64 maps to decimal(20,0), so the bound is the unscaled value
        // in minimal two's-complement BIG-endian bytes — the one scalar
        // whose bound is neither fixed-width nor little-endian. Both edges
        // need the leading 0x00 sign byte, which is the part a hand-rolled
        // encoder gets wrong.
        val input = oneColumn(PrimitiveTypeName.INT64, LogicalTypeAnnotation.intType(64, false))
        val rows =
            listOf<(Group) -> Unit>(
                { g -> g.add("v", 0L) },
                // 2^63 and 2^64-1, both above the signed int64 ceiling.
                { g -> g.add("v", Long.MIN_VALUE) },
                { g -> g.add("v", -1L) },
            )
        val trip = roundTrip("u64", input, ColType.UINT64, rows)
        assertThat(trip.lower).isEqualTo(BigInteger.ZERO.toByteArray())
        assertThat(trip.upper)
            .isEqualTo(BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE).toByteArray())
        // The native form is preserved: nothing maps uint64 onto anything
        // narrower, so a rewrite that changed the annotation would make
        // the next footer read drop bounds entirely.
        for (out in listOf(trip.first, trip.second)) {
            assertThat(physical(out).logicalTypeAnnotation)
                .describedAs("compaction output %s", out.fileName)
                .isEqualTo(LogicalTypeAnnotation.intType(64, false))
        }
    }

    // ---- timestamps ----------------------------------------------------

    @Test
    fun `timestamp_s bounds stay micros while its files stay millis`() {
        // Parquet has no seconds unit, so a timestamp_s column's files are
        // physically MILLIS — the declared catalog type and the file's unit
        // disagree by construction, at every vintage including compaction
        // output. The bound is micros regardless, because the mapped
        // Iceberg type is timestamp. Asserting the millis annotation here
        // pins the disagreement as intended rather than as a bug someone
        // later "fixes" into unreadable files.
        val millis = LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS)
        val input = oneColumn(PrimitiveTypeName.INT64, millis)
        val rows =
            listOf<(Group) -> Unit>(
                // Pre-epoch (the sign must survive), the epoch, and
                // 9999-12-31 — which still fits int64 micros after scaling.
                { g -> g.add("v", -1_500L) },
                { g -> g.add("v", 0L) },
                { g -> g.add("v", 253_402_300_799_000L) },
            )
        val trip = roundTrip("ts-s", input, ColType.TIMESTAMP_S, rows)
        assertThat(trip.lower).isEqualTo(IcebergSingleValue.encodeTimestampMicros(-1_500_000L))
        assertThat(trip.upper).isEqualTo(IcebergSingleValue.encodeTimestampMicros(253_402_300_799_000_000L))
        for (out in listOf(trip.first, trip.second)) {
            assertThat(physical(out).logicalTypeAnnotation)
                .describedAs("compaction output %s is millis under a timestamp_s column", out.fileName)
                .isEqualTo(millis)
        }
    }

    @Test
    fun `timestamp_ns bounds stay nanos across compaction`() {
        // The one timestamp precision whose bound is NOT micros: Iceberg
        // V3 timestamp_ns stores nanos, so a rewrite that normalized the
        // file to micros would keep every bound legal-looking and off by
        // 1000x.
        val nanos = LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.NANOS)
        val input = oneColumn(PrimitiveTypeName.INT64, nanos)
        val rows =
            listOf<(Group) -> Unit>(
                { g -> g.add("v", -1_500L) },
                { g -> g.add("v", 0L) },
                // 2100-01-01 in nanos: near the top of what nanos can say.
                { g -> g.add("v", 4_102_444_800_000_000_000L) },
            )
        val trip = roundTrip("ts-ns", input, ColType.TIMESTAMP_NS, rows)
        assertThat(trip.lower).isEqualTo(IcebergSingleValue.encodeTimestampNanos(-1_500L))
        assertThat(trip.upper).isEqualTo(IcebergSingleValue.encodeTimestampNanos(4_102_444_800_000_000_000L))
        for (out in listOf(trip.first, trip.second)) {
            assertThat(physical(out).logicalTypeAnnotation)
                .describedAs("compaction output %s", out.fileName)
                .isEqualTo(nanos)
        }
    }

    // ---- json ----------------------------------------------------------

    @Test
    fun `json bounds are the document bytes, unchanged by compaction`() {
        // json maps to Iceberg string: the bound is the document's UTF-8
        // bytes verbatim, ordered unsigned. The non-ASCII document is the
        // point — 0xC2 0xB5 sorts ABOVE every ASCII document only under an
        // unsigned compare, so a signed one inverts this file's range.
        val input = oneColumn(PrimitiveTypeName.BINARY, LogicalTypeAnnotation.jsonType())
        val documents = listOf("""{"a":1}""", """{"b":[2,3]}""", """{"µ":"ü"}""")
        val rows = documents.map { doc -> { g: Group -> g.add("v", doc) } }
        val trip = roundTrip("json", input, ColType.JSON, rows)
        assertThat(trip.lower).isEqualTo("""{"a":1}""".toByteArray(Charsets.UTF_8))
        assertThat(trip.upper).isEqualTo("""{"µ":"ü"}""".toByteArray(Charsets.UTF_8))
        for (out in listOf(trip.first, trip.second)) {
            assertThat(physical(out).logicalTypeAnnotation)
                .describedAs("compaction output %s", out.fileName)
                .isEqualTo(LogicalTypeAnnotation.jsonType())
        }
    }

    // ---- the loop ------------------------------------------------------

    private class Trip(
        val lower: ByteArray,
        val upper: ByteArray,
        val input: Path,
        val first: Path,
        val second: Path,
    )

    /**
     * Writes [rows] under [input], compacts, compacts the output again,
     * and asserts all three footers yield byte-identical bounds for
     * [type]. Returns those bounds plus the paths, so a caller can pin
     * the exact encoding and whatever shape change it expects.
     */
    private fun roundTrip(
        name: String,
        input: MessageType,
        type: ColType,
        rows: List<(Group) -> Unit>,
    ): Trip {
        val inPath = writeCustom("$name-in.parquet", input, rows)
        val pre = bounds(inPath, type)
        val first = tmp.resolve("$name-out1.parquet")
        rewrite(inPath, type, first)
        assertBounds(bounds(first, type), pre, "$name after one compaction")
        // Feeding the output back in is the real production sequence: a
        // tier-1 output becomes a tier-2 input, and the bounds must not
        // drift a little further on each pass.
        val second = tmp.resolve("$name-out2.parquet")
        rewrite(first, type, second)
        assertBounds(bounds(second, type), pre, "$name after re-compaction")
        return Trip(pre.first, pre.second, inPath, first, second)
    }

    private fun assertBounds(
        actual: Pair<ByteArray, ByteArray>,
        expected: Pair<ByteArray, ByteArray>,
        what: String,
    ) {
        assertThat(actual.first).describedAs("lower bound, %s", what).isEqualTo(expected.first)
        assertThat(actual.second).describedAs("upper bound, %s", what).isEqualTo(expected.second)
    }

    /**
     * The bounds the hydrator would store for a [type] column read off
     * this file's footer — the same call the commit path makes, so the
     * test cannot be right about an encoding the server does not produce.
     * Fails if either bound is absent: a dropped bound is a silent loss of
     * pruning, and "identical to the previous step" would pass trivially
     * if both steps dropped them.
     */
    private fun bounds(
        path: Path,
        type: ColType,
    ): Pair<ByteArray, ByteArray> {
        val column = CatalogColumn(1, "v", type, null)
        val agg =
            ParquetFileReader.open(LocalInputFile(path)).use {
                FooterStats.aggregate(it.footer, listOf(column), path.toString())
            }
        assertThat(agg).describedAs("one stats row for %s in %s", type.wire, path.fileName).hasSize(1)
        val lower = agg.single().lowerBound
        val upper = agg.single().upperBound
        assertThat(lower).describedAs("lower bound present in %s", path.fileName).isNotNull()
        assertThat(upper).describedAs("upper bound present in %s", path.fileName).isNotNull()
        assertThat(IcebergSingleValue.compareValues(type, decode(type, lower!!), decode(type, upper!!)))
            .describedAs("decoded lower <= upper for %s in %s", type.wire, path.fileName)
            .isLessThanOrEqualTo(0)
        return lower to upper
    }

    private fun decode(
        type: ColType,
        data: ByteArray,
    ): Any = IcebergSingleValue.decode(type, data)

    /** Compaction of one input under a one-column live schema of [type]. */
    private fun rewrite(
        input: Path,
        type: ColType,
        out: Path,
    ) {
        ParquetRewriter.rewrite(
            listOf(ParquetRewriter.Input(input, 0)),
            listOf(Column(1, 0, ColumnDef("v", type))),
            emptyList(),
            out,
        )
    }

    // ---- parquet fixtures ----------------------------------------------

    private fun physical(path: Path): PrimitiveType =
        ParquetFileReader.open(LocalInputFile(path)).use {
            it.footer.fileMetaData.schema.getType("v").asPrimitiveType()
        }

    private fun oneColumn(
        physical: PrimitiveTypeName,
        logical: LogicalTypeAnnotation,
    ): MessageType =
        Types.buildMessage()
            .addField(Types.optional(physical).`as`(logical).id(1).named("v"))
            .named("t")

    private fun writeCustom(
        fileName: String,
        schema: MessageType,
        rows: List<(Group) -> Unit>,
    ): Path {
        val path = tmp.resolve(fileName)
        val factory = SimpleGroupFactory(schema)
        ExampleParquetWriter.builder(LocalOutputFile(path))
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build()
            .use { w ->
                for (fill in rows) {
                    val g = factory.newGroup()
                    fill(g)
                    w.write(g)
                }
            }
        return path
    }
}
