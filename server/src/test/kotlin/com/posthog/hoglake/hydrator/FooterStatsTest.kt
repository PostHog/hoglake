package com.posthog.hoglake.hydrator

import com.posthog.hoglake.model.ColType
import org.apache.parquet.column.Encoding
import org.apache.parquet.column.statistics.Statistics
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData
import org.apache.parquet.hadoop.metadata.ColumnPath
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Unit tests for footer aggregation against hand-built [ParquetMetadata]
 * (parquet-java's metadata classes are constructible records). This is
 * also where the field-id mapping and [FooterStats.missingFieldIds]
 * contract check are exercised without touching an object store.
 */
class FooterStatsTest {
    // ---- fixture helpers ---------------------------------------------------

    private fun leaf(
        name: String,
        physical: PrimitiveType.PrimitiveTypeName,
        fieldId: Int? = null,
        logical: LogicalTypeAnnotation? = null,
        typeLength: Int? = null,
    ): PrimitiveType {
        var b = Types.optional(physical)
        if (typeLength != null) b = b.length(typeLength)
        if (logical != null) b = b.`as`(logical)
        if (fieldId != null) b = b.id(fieldId)
        return b.named(name)
    }

    private fun schema(vararg fields: Type): MessageType = MessageType("root", fields.toList())

    /** Statistics with the given raw min/max bytes and null count (null = unknown). */
    private fun stats(
        type: PrimitiveType,
        min: ByteArray?,
        max: ByteArray?,
        nulls: Long? = 0L,
    ): Statistics<*> {
        val b = Statistics.getBuilderForReading(type)
        if (min != null && max != null) {
            b.withMin(min)
            b.withMax(max)
        }
        if (nulls != null) b.withNumNulls(nulls)
        return b.build()
    }

    private fun chunk(
        leaf: PrimitiveType,
        numValues: Long,
        st: Statistics<*>,
        compressedSize: Long = 100L,
    ): ColumnChunkMetaData =
        ColumnChunkMetaData.get(
            ColumnPath.get(leaf.name),
            leaf,
            CompressionCodecName.UNCOMPRESSED,
            null,
            setOf(Encoding.PLAIN),
            st,
            4L,
            0L,
            numValues,
            compressedSize,
            compressedSize * 2,
        )

    /** A nested chunk (multi-element path) that top-level aggregation must ignore. */
    private fun nestedChunk(
        leaf: PrimitiveType,
        parent: String,
        numValues: Long,
        st: Statistics<*>,
    ): ColumnChunkMetaData =
        ColumnChunkMetaData.get(
            ColumnPath.get(parent, leaf.name),
            leaf,
            CompressionCodecName.UNCOMPRESSED,
            null,
            setOf(Encoding.PLAIN),
            st,
            4L,
            0L,
            numValues,
            10L,
            20L,
        )

    private fun meta(
        schema: MessageType,
        numRows: Long,
        vararg groups: List<ColumnChunkMetaData>,
    ): ParquetMetadata {
        val blocks =
            groups.map { chunks ->
                BlockMetaData().apply {
                    rowCount = numRows
                    totalByteSize = chunks.sumOf { it.totalSize }
                    chunks.forEach { addColumn(it) }
                }
            }
        return ParquetMetadata(FileMetaData(schema, emptyMap(), "test"), blocks)
    }

    private fun le(v: Int): ByteArray = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(v).array()

    private fun le(v: Long): ByteArray = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(v).array()

    private fun le(v: Double): ByteArray = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(v).array()

    private fun agg(
        meta: ParquetMetadata,
        vararg cols: CatalogColumn,
    ): Map<Long, FooterStats.ColumnAgg> =
        FooterStats.aggregate(meta, cols.toList(), "s3://t/f.parquet").associateBy { it.fieldId }

    // ---- mapping -----------------------------------------------------------

    @Test
    fun `parquet field ids beat name matching`() {
        // Parquet column is named "a_old"; the catalog renamed it to "a".
        // Field id 7 must carry the mapping.
        val a = leaf("a_old", PrimitiveType.PrimitiveTypeName.INT64, fieldId = 7)
        val b = leaf("b", PrimitiveType.PrimitiveTypeName.INT64, fieldId = 8)
        val m =
            meta(
                schema(a, b),
                10,
                listOf(
                    chunk(a, 10, stats(a, le(5L), le(9L))),
                    chunk(b, 10, stats(b, le(1L), le(2L))),
                ),
            )
        // Catalog: field 7 named "a" (renamed), and field 9 named "b" —
        // the name collision with parquet "b" (field 8) must NOT match.
        val out =
            agg(
                m,
                CatalogColumn(7, "a", ColType.LONG, null),
                CatalogColumn(9, "b", ColType.LONG, null),
            )
        assertThat(out).containsOnlyKeys(7L)
        assertThat(out[7L]!!.lowerBound).isEqualTo(le(5L))
        assertThat(out[7L]!!.upperBound).isEqualTo(le(9L))
    }

    @Test
    fun `falls back to names when the file has no field ids`() {
        val a = leaf("a", PrimitiveType.PrimitiveTypeName.INT64)
        val m = meta(schema(a), 10, listOf(chunk(a, 10, stats(a, le(-2L), le(4L), nulls = 3))))
        val out = agg(m, CatalogColumn(1, "a", ColType.LONG, null))
        assertThat(out).containsOnlyKeys(1L)
        with(out[1L]!!) {
            assertThat(valueCount).isEqualTo(10)
            assertThat(nullCount).isEqualTo(3)
            assertThat(lowerBound).isEqualTo(le(-2L))
            assertThat(upperBound).isEqualTo(le(4L))
        }
    }

    @Test
    fun `catalog columns absent from the file get no stats row`() {
        val a = leaf("a", PrimitiveType.PrimitiveTypeName.INT64)
        val m = meta(schema(a), 5, listOf(chunk(a, 5, stats(a, le(0L), le(1L)))))
        val out =
            agg(
                m,
                CatalogColumn(1, "a", ColType.LONG, null),
                CatalogColumn(2, "added_later", ColType.STRING, null),
            )
        assertThat(out).containsOnlyKeys(1L)
    }

    // ---- the field-id contract check ---------------------------------------

    @Test
    fun `missingFieldIds is false when every leaf has an id and true when any lacks one`() {
        val withIds =
            schema(
                leaf("a", PrimitiveType.PrimitiveTypeName.INT64, fieldId = 1),
                leaf("b", PrimitiveType.PrimitiveTypeName.INT64, fieldId = 2),
            )
        assertThat(FooterStats.missingFieldIds(withIds)).isFalse()

        val oneMissing =
            schema(
                leaf("a", PrimitiveType.PrimitiveTypeName.INT64, fieldId = 1),
                leaf("b", PrimitiveType.PrimitiveTypeName.INT64),
            )
        assertThat(FooterStats.missingFieldIds(oneMissing)).isTrue()
    }

    @Test
    fun `the reserved _hog_row_id id counts as an id like any other`() {
        val compacted =
            schema(
                leaf("a", PrimitiveType.PrimitiveTypeName.INT64, fieldId = 1),
                Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                    .id(2147483646)
                    .named("_hog_row_id"),
            )
        assertThat(FooterStats.missingFieldIds(compacted)).isFalse()
    }

    @Test
    fun `missingFieldIds inspects nested leaves too`() {
        val nested =
            schema(
                leaf("a", PrimitiveType.PrimitiveTypeName.INT64, fieldId = 1),
                Types.optionalGroup()
                    .addField(leaf("x", PrimitiveType.PrimitiveTypeName.INT64))
                    .id(2)
                    .named("g"),
            )
        assertThat(FooterStats.missingFieldIds(nested)).isTrue()
    }

    // ---- multi row-group aggregation ---------------------------------------

    @Test
    fun `merges counts and bounds across row groups`() {
        val n = leaf("n", PrimitiveType.PrimitiveTypeName.INT64)
        val s =
            leaf(
                "s",
                PrimitiveType.PrimitiveTypeName.BINARY,
                logical = LogicalTypeAnnotation.stringType(),
            )
        val m =
            meta(
                schema(n, s),
                25,
                listOf(
                    chunk(n, 10, stats(n, le(5L), le(10L), nulls = 1), compressedSize = 40),
                    chunk(s, 10, stats(s, "banana".toByteArray(), "cherry".toByteArray())),
                ),
                listOf(
                    chunk(n, 15, stats(n, le(-3L), le(7L), nulls = 2), compressedSize = 60),
                    chunk(s, 15, stats(s, "apple".toByteArray(), "candy".toByteArray())),
                ),
            )
        val out =
            agg(
                m,
                CatalogColumn(1, "n", ColType.LONG, null),
                CatalogColumn(2, "s", ColType.STRING, null),
            )
        with(out[1L]!!) {
            assertThat(valueCount).isEqualTo(25)
            assertThat(nullCount).isEqualTo(3)
            assertThat(sizeBytes).isEqualTo(100)
            assertThat(lowerBound).isEqualTo(le(-3L))
            assertThat(upperBound).isEqualTo(le(10L))
        }
        with(out[2L]!!) {
            assertThat(lowerBound).isEqualTo("apple".toByteArray())
            assertThat(upperBound).isEqualTo("cherry".toByteArray())
        }
    }

    // ---- reliability gates -------------------------------------------------

    @Test
    fun `stats without min-max keep counts but drop bounds`() {
        // The shape parquet-java hands back for footers whose deprecated
        // min/max it refused (pre-TYPE_DEFINED_ORDER unreliable order):
        // null count present, no bounds.
        val a = leaf("a", PrimitiveType.PrimitiveTypeName.INT64)
        val m = meta(schema(a), 10, listOf(chunk(a, 10, stats(a, null, null, nulls = 4))))
        val out = agg(m, CatalogColumn(1, "a", ColType.LONG, null))
        with(out[1L]!!) {
            assertThat(valueCount).isEqualTo(10)
            assertThat(nullCount).isEqualTo(4)
            assertThat(lowerBound).isNull()
            assertThat(upperBound).isNull()
        }
    }

    @Test
    fun `one chunk without min-max poisons bounds for the whole file`() {
        val a = leaf("a", PrimitiveType.PrimitiveTypeName.INT64)
        val m =
            meta(
                schema(a),
                20,
                listOf(chunk(a, 10, stats(a, le(1L), le(2L), nulls = 0))),
                listOf(chunk(a, 10, stats(a, null, null, nulls = 0))),
            )
        val out = agg(m, CatalogColumn(1, "a", ColType.LONG, null))
        with(out[1L]!!) {
            assertThat(valueCount).isEqualTo(20)
            assertThat(lowerBound).isNull()
            assertThat(upperBound).isNull()
        }
    }

    @Test
    fun `missing null count drops the whole stats row`() {
        val a = leaf("a", PrimitiveType.PrimitiveTypeName.INT64)
        val m = meta(schema(a), 10, listOf(chunk(a, 10, stats(a, le(1L), le(2L), nulls = null))))
        assertThat(agg(m, CatalogColumn(1, "a", ColType.LONG, null))).isEmpty()
    }

    @Test
    fun `NaN bounds are never written`() {
        val d = leaf("d", PrimitiveType.PrimitiveTypeName.DOUBLE)
        val m = meta(schema(d), 10, listOf(chunk(d, 10, stats(d, le(Double.NaN), le(5.0)))))
        val out = agg(m, CatalogColumn(1, "d", ColType.DOUBLE, null))
        with(out[1L]!!) {
            assertThat(lowerBound).isNull()
            assertThat(upperBound).isNull()
        }
    }

    @Test
    fun `physical type mismatch keeps counts but drops bounds`() {
        val a = leaf("a", PrimitiveType.PrimitiveTypeName.BINARY)
        val m = meta(schema(a), 10, listOf(chunk(a, 10, stats(a, le(1L), le(2L)))))
        val out = agg(m, CatalogColumn(1, "a", ColType.LONG, null))
        with(out[1L]!!) {
            assertThat(valueCount).isEqualTo(10)
            assertThat(lowerBound).isNull()
            assertThat(upperBound).isNull()
        }
    }

    // ---- typed decoding ----------------------------------------------------

    @Test
    fun `int32 widens to catalog long`() {
        val a = leaf("a", PrimitiveType.PrimitiveTypeName.INT32)
        val m = meta(schema(a), 10, listOf(chunk(a, 10, stats(a, le(-7), le(9)))))
        val out = agg(m, CatalogColumn(1, "a", ColType.LONG, null))
        assertThat(out[1L]!!.lowerBound).isEqualTo(le(-7L))
        assertThat(out[1L]!!.upperBound).isEqualTo(le(9L))
    }

    @Test
    fun `timestamp millis convert exactly to micros`() {
        val ts =
            leaf(
                "ts",
                PrimitiveType.PrimitiveTypeName.INT64,
                logical = LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS),
            )
        val m = meta(schema(ts), 10, listOf(chunk(ts, 10, stats(ts, le(1_000L), le(2_500L)))))
        val out = agg(m, CatalogColumn(1, "ts", ColType.TIMESTAMPTZ, null))
        assertThat(out[1L]!!.lowerBound).isEqualTo(le(1_000_000L))
        assertThat(out[1L]!!.upperBound).isEqualTo(le(2_500_000L))
    }

    @Test
    fun `timestamp nanos floor the lower bound and ceil the upper`() {
        val ts =
            leaf(
                "ts",
                PrimitiveType.PrimitiveTypeName.INT64,
                logical = LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS),
            )
        val m = meta(schema(ts), 10, listOf(chunk(ts, 10, stats(ts, le(1_500L), le(2_500L)))))
        val out = agg(m, CatalogColumn(1, "ts", ColType.TIMESTAMPTZ, null))
        assertThat(out[1L]!!.lowerBound).isEqualTo(le(1L))
        assertThat(out[1L]!!.upperBound).isEqualTo(le(3L))
    }

    @Test
    fun `timestamp millis near Long MAX overflows to null bounds, never an exception`() {
        // Pinned regression (bug hunt #10): millis -> micros uses
        // multiplyExact; a bound near Long.MAX_VALUE (hostile-writer
        // craftable) used to throw ArithmeticException out of decode and
        // fail the WHOLE file. The "bounds NULL, never guessed" contract
        // demands a null bound and an otherwise-honest stats row.
        val ts =
            leaf(
                "ts",
                PrimitiveType.PrimitiveTypeName.INT64,
                logical = LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS),
            )
        val m = meta(schema(ts), 10, listOf(chunk(ts, 10, stats(ts, le(Long.MAX_VALUE - 1), le(Long.MAX_VALUE)))))
        val out = agg(m, CatalogColumn(1, "ts", ColType.TIMESTAMPTZ, null))
        assertThat(out).containsOnlyKeys(1L) // the row survives
        assertThat(out[1L]!!.valueCount).isEqualTo(10)
        assertThat(out[1L]!!.lowerBound).isNull()
        assertThat(out[1L]!!.upperBound).isNull()
    }

    @Test
    fun `timestamp nanos near Long MAX overflows the upper-bound ceil to null bounds`() {
        // The NANOS upper bound ceils via addExact(v, 999): craftable
        // overflow on the upper side specifically.
        val ts =
            leaf(
                "ts",
                PrimitiveType.PrimitiveTypeName.INT64,
                logical = LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS),
            )
        val m = meta(schema(ts), 10, listOf(chunk(ts, 10, stats(ts, le(0L), le(Long.MAX_VALUE)))))
        val out = agg(m, CatalogColumn(1, "ts", ColType.TIMESTAMPTZ, null))
        assertThat(out).containsOnlyKeys(1L)
        assertThat(out[1L]!!.lowerBound).isNull()
        assertThat(out[1L]!!.upperBound).isNull()
    }

    @Test
    fun `timestamp with no logical annotation drops bounds`() {
        val ts = leaf("ts", PrimitiveType.PrimitiveTypeName.INT64)
        val m = meta(schema(ts), 10, listOf(chunk(ts, 10, stats(ts, le(1L), le(2L)))))
        val out = agg(m, CatalogColumn(1, "ts", ColType.TIMESTAMPTZ, null))
        assertThat(out[1L]!!.lowerBound).isNull()
    }

    @Test
    fun `decimal byte-array bounds pass through at matching scale`() {
        val d =
            leaf(
                "d",
                PrimitiveType.PrimitiveTypeName.BINARY,
                logical = LogicalTypeAnnotation.decimalType(2, 10),
            )
        val m =
            meta(
                schema(d),
                10,
                // -0.01 .. 14.20
                listOf(chunk(d, 10, stats(d, byteArrayOf(-1), byteArrayOf(0x05, 0x8C.toByte())))),
            )
        val out = agg(m, CatalogColumn(1, "d", ColType.DECIMAL, decimalScale = 2))
        assertThat(out[1L]!!.lowerBound).isEqualTo(byteArrayOf(-1))
        assertThat(out[1L]!!.upperBound).isEqualTo(byteArrayOf(0x05, 0x8C.toByte()))
    }

    @Test
    fun `decimal scale mismatch drops bounds`() {
        val d =
            leaf(
                "d",
                PrimitiveType.PrimitiveTypeName.BINARY,
                logical = LogicalTypeAnnotation.decimalType(2, 10),
            )
        val m = meta(schema(d), 10, listOf(chunk(d, 10, stats(d, byteArrayOf(1), byteArrayOf(2)))))
        val out = agg(m, CatalogColumn(1, "d", ColType.DECIMAL, decimalScale = 3))
        assertThat(out[1L]!!.lowerBound).isNull()
        assertThat(out[1L]!!.upperBound).isNull()
    }

    @Test
    fun `uuid fixed16 bounds pass through`() {
        val lo = ByteArray(16) { 0x00 }
        val hi = ByteArray(16) { 0xAB.toByte() }
        val u =
            leaf(
                "u",
                PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
                logical = LogicalTypeAnnotation.uuidType(),
                typeLength = 16,
            )
        val m = meta(schema(u), 10, listOf(chunk(u, 10, stats(u, lo, hi))))
        val out = agg(m, CatalogColumn(1, "u", ColType.UUID_T, null))
        assertThat(out[1L]!!.lowerBound).isEqualTo(lo)
        assertThat(out[1L]!!.upperBound).isEqualTo(hi)
    }

    @Test
    fun `string bounds compare as unsigned bytes`() {
        // 0xC2 0xB5 (µ) must sort above ASCII despite the negative signed byte.
        val s =
            leaf(
                "s",
                PrimitiveType.PrimitiveTypeName.BINARY,
                logical = LogicalTypeAnnotation.stringType(),
            )
        val m =
            meta(
                schema(s),
                10,
                listOf(chunk(s, 5, stats(s, "a".toByteArray(), "µ".toByteArray()))),
                listOf(chunk(s, 5, stats(s, "b".toByteArray(), "z".toByteArray()))),
            )
        val out = agg(m, CatalogColumn(1, "s", ColType.STRING, null))
        assertThat(out[1L]!!.lowerBound).isEqualTo("a".toByteArray())
        assertThat(out[1L]!!.upperBound).isEqualTo("µ".toByteArray())
    }

    @Test
    fun `nested leaves are ignored, top-level ones still map`() {
        // root { a: int64, g: group { x: int64 } }
        val a = leaf("a", PrimitiveType.PrimitiveTypeName.INT64)
        val x = leaf("x", PrimitiveType.PrimitiveTypeName.INT64)
        val g = Types.optionalGroup().addField(x).named("g")
        val m =
            meta(
                schema(a, g),
                10,
                listOf(
                    chunk(a, 10, stats(a, le(1L), le(2L))),
                    nestedChunk(x, "g", 10, stats(x, le(9L), le(9L))),
                ),
            )
        val out =
            agg(
                m,
                CatalogColumn(1, "a", ColType.LONG, null),
                CatalogColumn(2, "x", ColType.LONG, null),
            )
        assertThat(out).containsOnlyKeys(1L)
        assertThat(out[1L]!!.upperBound).isEqualTo(le(2L))
    }

    // ---- the DuckLake scalar-parity types ----------------------------------
    //
    // Bounds are always the MAPPED Iceberg type's encoding, so the
    // assertions below are really assertions about iceberg-federation.md
    // §2's table: 4-byte ints for everything int-mapped, 8-byte longs for
    // uint32, decimal bytes for uint64, micros for the timestamp
    // precisions and nanos for timestamp_ns.

    @Test
    fun `small int widths ride int32 and keep the 4-byte int bound`() {
        val widths =
            listOf(
                Triple(ColType.INT8, 8, true),
                Triple(ColType.INT16, 16, true),
                Triple(ColType.UINT8, 8, false),
                Triple(ColType.UINT16, 16, false),
            )
        for ((type, width, signed) in widths) {
            val a =
                leaf(
                    "a",
                    PrimitiveType.PrimitiveTypeName.INT32,
                    logical = LogicalTypeAnnotation.intType(width, signed),
                )
            val lo = if (signed) -5 else 0
            val m = meta(schema(a), 10, listOf(chunk(a, 10, stats(a, le(lo), le(9)))))
            val out = agg(m, CatalogColumn(1, "a", type, null))
            assertThat(out[1L]!!.lowerBound).describedAs(type.wire).isEqualTo(le(lo))
            assertThat(out[1L]!!.upperBound).describedAs(type.wire).isEqualTo(le(9))
        }
    }

    @Test
    fun `uint32 from the hoglake-written int64 form is a plain long bound`() {
        val a = leaf("a", PrimitiveType.PrimitiveTypeName.INT64)
        val m = meta(schema(a), 10, listOf(chunk(a, 10, stats(a, le(0L), le(4_294_967_295L)))))
        val out = agg(m, CatalogColumn(1, "a", ColType.UINT32, null))
        assertThat(out[1L]!!.lowerBound).isEqualTo(le(0L))
        assertThat(out[1L]!!.upperBound).isEqualTo(le(4_294_967_295L))
    }

    @Test
    fun `uint32 from an arrow-written unsigned int32 zero-extends past 2 to the 31`() {
        // The bug this pins: sign-extending 0xFFFFFFFF gives -1, which is
        // not a uint32 bound and inverts the range.
        val a =
            leaf(
                "a",
                PrimitiveType.PrimitiveTypeName.INT32,
                logical = LogicalTypeAnnotation.intType(32, false),
            )
        val m = meta(schema(a), 10, listOf(chunk(a, 10, stats(a, le(0), le(-1)))))
        val out = agg(m, CatalogColumn(1, "a", ColType.UINT32, null))
        assertThat(out[1L]!!.lowerBound).isEqualTo(le(0L))
        assertThat(out[1L]!!.upperBound).isEqualTo(le(4_294_967_295L))
    }

    @Test
    fun `uint32 from an int32 WITHOUT the unsigned annotation drops bounds`() {
        // Without the annotation parquet computed min/max in SIGNED order,
        // so reinterpreting them as unsigned would invert the range. Null,
        // never guessed.
        val a = leaf("a", PrimitiveType.PrimitiveTypeName.INT32)
        val m = meta(schema(a), 10, listOf(chunk(a, 10, stats(a, le(0), le(7)))))
        val out = agg(m, CatalogColumn(1, "a", ColType.UINT32, null))
        assertThat(out[1L]!!.valueCount).isEqualTo(10)
        assertThat(out[1L]!!.lowerBound).isNull()
        assertThat(out[1L]!!.upperBound).isNull()
    }

    @Test
    fun `an unsigned int32 under a catalog long zero-extends at every width`() {
        // The promotion uint8/uint16/uint32 -> long is legal and leaves the
        // stats bytes alone (same mapped Iceberg type at 4 bytes for the
        // small widths, already 8 for uint32), so files written BEFORE the
        // promotion keep arriving at the hydrator afterwards under the new
        // catalog type. Sign-extending them is the ParquetRewriter hazard in
        // its read-path twin: 0xFFFFFFFF becomes -1, the upper bound lands
        // BELOW the lower one, and a pruner silently drops the whole file.
        val widths = listOf(8 to 255, 16 to 65_535, 32 to -1)
        for ((width, maxBits) in widths) {
            val a =
                leaf(
                    "a",
                    PrimitiveType.PrimitiveTypeName.INT32,
                    logical = LogicalTypeAnnotation.intType(width, false),
                )
            val m = meta(schema(a), 10, listOf(chunk(a, 10, stats(a, le(0), le(maxBits)))))
            val out = agg(m, CatalogColumn(1, "a", ColType.LONG, null))
            val expectedMax = maxBits.toLong() and 0xFFFFFFFFL
            assertThat(out[1L]!!.lowerBound).describedAs("uint%d lower", width).isEqualTo(le(0L))
            assertThat(out[1L]!!.upperBound)
                .describedAs("uint%d upper (must zero-extend, not sign-extend)", width)
                .isEqualTo(le(expectedMax))
        }
    }

    @Test
    fun `a signed int32 under a catalog long still sign-extends`() {
        // The control: the pre-existing int -> long promotion must keep
        // meaning what it meant.
        val a = leaf("a", PrimitiveType.PrimitiveTypeName.INT32)
        val m = meta(schema(a), 10, listOf(chunk(a, 10, stats(a, le(-7), le(9)))))
        val out = agg(m, CatalogColumn(1, "a", ColType.LONG, null))
        assertThat(out[1L]!!.lowerBound).isEqualTo(le(-7L))
        assertThat(out[1L]!!.upperBound).isEqualTo(le(9L))
    }

    @Test
    fun `an unsigned int64 under any catalog type but uint64 drops bounds`() {
        // Found by QeFooterStatsBoundsPropertyTest's lower <= upper
        // invariant. Parquet ordered this chunk UNSIGNED, so a long
        // column reading the same bits signed inherits an ordering it
        // disagrees with — and the values above 2^63 are not longs at
        // all. uint64 is the one reader that zero-extends into a
        // decimal(20,0) bound, so it is the one reader allowed.
        val a =
            leaf(
                "a",
                PrimitiveType.PrimitiveTypeName.INT64,
                logical = LogicalTypeAnnotation.intType(64, false),
            )
        // Unsigned-ordered min/max whose signed reading inverts.
        val m = meta(schema(a), 10, listOf(chunk(a, 10, stats(a, le(1L), le(-1L)))))
        for (type in listOf(ColType.LONG, ColType.TIMESTAMP, ColType.TIMESTAMPTZ, ColType.DECIMAL)) {
            val out = agg(m, CatalogColumn(1, "a", type, 0))
            assertThat(out[1L]!!.lowerBound).describedAs(type.wire).isNull()
            assertThat(out[1L]!!.upperBound).describedAs(type.wire).isNull()
        }
        // The allowed reader still gets its bounds, right way up.
        val ok = agg(m, CatalogColumn(1, "a", ColType.UINT64, null))
        assertThat(ok[1L]!!.lowerBound).isEqualTo(java.math.BigInteger.ONE.toByteArray())
        assertThat(ok[1L]!!.upperBound)
            .isEqualTo(java.math.BigInteger.ONE.shiftLeft(64).subtract(java.math.BigInteger.ONE).toByteArray())
    }

    @Test
    fun `an unsigned int32 under a date column drops bounds`() {
        // Same find, same shape: date maps to Iceberg int and reads the
        // int32 signed, so an unsigned-ordered chunk inverts under it.
        val a =
            leaf(
                "a",
                PrimitiveType.PrimitiveTypeName.INT32,
                logical = LogicalTypeAnnotation.intType(32, false),
            )
        val m = meta(schema(a), 10, listOf(chunk(a, 10, stats(a, le(1), le(-1)))))
        val out = agg(m, CatalogColumn(1, "a", ColType.DATE, null))
        assertThat(out[1L]!!.lowerBound).isNull()
        assertThat(out[1L]!!.upperBound).isNull()
    }

    @Test
    fun `an unsigned int32 under an int-mapped catalog type drops bounds`() {
        // No legal promotion produces this pairing (nothing promotes INTO
        // int from uint32), so it can only arrive from a foreign writer
        // disagreeing with the declared type. The values do not fit a
        // 4-byte signed Iceberg int bound and parquet ordered the chunk's
        // min/max unsigned, so there is nothing honest to store.
        val a =
            leaf(
                "a",
                PrimitiveType.PrimitiveTypeName.INT32,
                logical = LogicalTypeAnnotation.intType(32, false),
            )
        val m = meta(schema(a), 10, listOf(chunk(a, 10, stats(a, le(0), le(-1)))))
        for (type in listOf(ColType.INT, ColType.INT8, ColType.INT16, ColType.UINT8, ColType.UINT16)) {
            val out = agg(m, CatalogColumn(1, "a", type, null))
            assertThat(out[1L]!!.valueCount).describedAs(type.wire).isEqualTo(10)
            assertThat(out[1L]!!.lowerBound).describedAs(type.wire).isNull()
            assertThat(out[1L]!!.upperBound).describedAs(type.wire).isNull()
        }
    }

    @Test
    fun `uint64 bounds are the decimal encoding of the unsigned value`() {
        val a =
            leaf(
                "a",
                PrimitiveType.PrimitiveTypeName.INT64,
                logical = LogicalTypeAnnotation.intType(64, false),
            )
        // max is the bit pattern of 2^64-1; min is 2^63 (Long.MIN_VALUE's bits).
        val m = meta(schema(a), 10, listOf(chunk(a, 10, stats(a, le(Long.MIN_VALUE), le(-1L)))))
        val out = agg(m, CatalogColumn(1, "a", ColType.UINT64, null))
        // 2^63 and 2^64-1 both need the leading 0x00 sign byte.
        assertThat(out[1L]!!.lowerBound)
            .isEqualTo(java.math.BigInteger.ONE.shiftLeft(63).toByteArray())
        assertThat(out[1L]!!.upperBound)
            .isEqualTo(java.math.BigInteger.ONE.shiftLeft(64).subtract(java.math.BigInteger.ONE).toByteArray())
    }

    @Test
    fun `uint64 without the unsigned annotation drops bounds`() {
        val a = leaf("a", PrimitiveType.PrimitiveTypeName.INT64)
        val m = meta(schema(a), 10, listOf(chunk(a, 10, stats(a, le(1L), le(2L)))))
        val out = agg(m, CatalogColumn(1, "a", ColType.UINT64, null))
        assertThat(out[1L]!!.lowerBound).isNull()
    }

    @Test
    fun `timestamp_s files are physically millis and still bound in micros`() {
        // Parquet has no seconds unit, so this IS the shape a timestamp_s
        // column's files have (pyarrow coerces timestamp[s] to MILLIS).
        val ts =
            leaf(
                "ts",
                PrimitiveType.PrimitiveTypeName.INT64,
                logical = LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS),
            )
        val m = meta(schema(ts), 10, listOf(chunk(ts, 10, stats(ts, le(-1_000L), le(2_000L)))))
        for (type in listOf(ColType.TIMESTAMP_S, ColType.TIMESTAMP_MS)) {
            val out = agg(m, CatalogColumn(1, "ts", type, null))
            assertThat(out[1L]!!.lowerBound).describedAs(type.wire).isEqualTo(le(-1_000_000L))
            assertThat(out[1L]!!.upperBound).describedAs(type.wire).isEqualTo(le(2_000_000L))
        }
    }

    @Test
    fun `timestamp_ns bounds are nanos, not micros`() {
        val ts =
            leaf(
                "ts",
                PrimitiveType.PrimitiveTypeName.INT64,
                logical = LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.NANOS),
            )
        val m = meta(schema(ts), 10, listOf(chunk(ts, 10, stats(ts, le(-1_500L), le(2_500L)))))
        val out = agg(m, CatalogColumn(1, "ts", ColType.TIMESTAMP_NS, null))
        // Verbatim: no flooring, no ceiling, no unit change.
        assertThat(out[1L]!!.lowerBound).isEqualTo(le(-1_500L))
        assertThat(out[1L]!!.upperBound).isEqualTo(le(2_500L))
    }

    @Test
    fun `timestamp_ns scales a millis or micros file UP exactly`() {
        val scales =
            listOf(
                LogicalTypeAnnotation.TimeUnit.MILLIS to 1_000_000L,
                LogicalTypeAnnotation.TimeUnit.MICROS to 1_000L,
            )
        for ((unit, factor) in scales) {
            val ts =
                leaf(
                    "ts",
                    PrimitiveType.PrimitiveTypeName.INT64,
                    logical = LogicalTypeAnnotation.timestampType(false, unit),
                )
            val m = meta(schema(ts), 10, listOf(chunk(ts, 10, stats(ts, le(3L), le(4L)))))
            val out = agg(m, CatalogColumn(1, "ts", ColType.TIMESTAMP_NS, null))
            assertThat(out[1L]!!.lowerBound).describedAs("$unit").isEqualTo(le(3L * factor))
        }
    }

    @Test
    fun `timestamp_ns scaling overflow drops bounds, never throws`() {
        val ts =
            leaf(
                "ts",
                PrimitiveType.PrimitiveTypeName.INT64,
                logical = LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS),
            )
        val m = meta(schema(ts), 10, listOf(chunk(ts, 10, stats(ts, le(1L), le(Long.MAX_VALUE)))))
        val out = agg(m, CatalogColumn(1, "ts", ColType.TIMESTAMP_NS, null))
        assertThat(out).containsOnlyKeys(1L)
        assertThat(out[1L]!!.lowerBound).isNull()
        assertThat(out[1L]!!.upperBound).isNull()
    }

    @Test
    fun `json bounds are the document bytes, compared unsigned like string`() {
        val j =
            leaf(
                "j",
                PrimitiveType.PrimitiveTypeName.BINARY,
                logical = LogicalTypeAnnotation.jsonType(),
            )
        val m =
            meta(
                schema(j),
                20,
                listOf(chunk(j, 10, stats(j, "{\"a\":1}".toByteArray(), "{\"µ\":2}".toByteArray()))),
                listOf(chunk(j, 10, stats(j, "{\"b\":1}".toByteArray(), "{\"c\":2}".toByteArray()))),
            )
        val out = agg(m, CatalogColumn(1, "j", ColType.JSON, null))
        assertThat(out[1L]!!.lowerBound).isEqualTo("{\"a\":1}".toByteArray())
        // 'µ' is 0xC2 0xB5 in UTF-8 — above 'c' only under an UNSIGNED
        // byte compare, which is the one that matches Iceberg's ordering.
        assertThat(out[1L]!!.upperBound).isEqualTo("{\"µ\":2}".toByteArray())
    }

    @Test
    fun `json also reads a plain BYTE_ARRAY with no JSON annotation`() {
        // The annotation changes neither the bytes nor their sort order,
        // so requiring it would only lose bounds on files from writers
        // that do not stamp it.
        val j = leaf("j", PrimitiveType.PrimitiveTypeName.BINARY)
        val m = meta(schema(j), 10, listOf(chunk(j, 10, stats(j, "[]".toByteArray(), "{}".toByteArray()))))
        val out = agg(m, CatalogColumn(1, "j", ColType.JSON, null))
        assertThat(out[1L]!!.lowerBound).isEqualTo("[]".toByteArray())
    }
}
