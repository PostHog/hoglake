package com.posthog.hoglake.hydrator

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.IcebergType
import com.posthog.hoglake.model.icebergType
import com.posthog.hoglake.stats.IcebergSingleValue
import io.kotest.property.Arb
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.long
import io.kotest.property.checkAll
import kotlinx.coroutines.runBlocking
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
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Property assault on FooterStats' decode matrix: every catalog type
 * crossed with every physical/logical shape a writer might hand us.
 *
 * Two invariants, and the SECOND one is the reason this class exists:
 *
 *  1. A produced bound is encoded at the MAPPED Iceberg type's width
 *     (ColType.icebergType, iceberg-federation.md §2) — a 4-byte bound
 *     under a long column is the stale-width poison that wedges
 *     compaction's bound-merge.
 *  2. lower <= upper, compared under the catalog type.
 *
 * Invariant 2 catches an entire bug CLASS generically rather than one
 * instance of it. The unsigned-int32-under-a-long-column defect
 * (0xFFFFFFFF sign-extending to -1) produced exactly an inverted range,
 * and an inverted range is worse than a wrong one: a pruner reads it as
 * "no rows here" and drops the file from every scan while all the
 * counts still look healthy. Any future decode arm that reinterprets
 * bits without honouring the annotation that ordered them will land
 * here the same way.
 *
 * The generated min/max pair is always ordered in the LEAF's own
 * parquet sort order, because that is what a real writer's footer
 * contains — an unsigned annotation means parquet ordered the chunk
 * unsigned, and reading those bytes signed is precisely the mistake.
 * (Bounds are allowed to come back NULL for any shape: "NULL, never
 * guessed" is the standing contract, so refusing is always correct.)
 */
class QeFooterStatsBoundsPropertyTest {
    /** One physical/logical leaf shape, with the ordering parquet gives it. */
    private class Shape(
        val name: String,
        val leaf: PrimitiveType,
        /** A sorted (min, max) pair of raw statistics bytes in this leaf's order. */
        val sortedPair: (Arb.Companion, kotlin.random.Random) -> Pair<ByteArray, ByteArray>,
    )

    private fun leafOf(
        physical: PrimitiveTypeName,
        logical: LogicalTypeAnnotation? = null,
        length: Int? = null,
    ): PrimitiveType {
        var b = Types.optional(physical)
        if (length != null) b = b.length(length)
        if (logical != null) b = b.`as`(logical)
        return b.id(1).named("v")
    }

    private fun le(v: Int): ByteArray = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(v).array()

    private fun le(v: Long): ByteArray = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(v).array()

    /** Two ints ordered by [cmp], as 4-byte LE stat bytes. */
    private fun intPair(
        rnd: kotlin.random.Random,
        bound: Int,
        cmp: Comparator<Int>,
    ): Pair<ByteArray, ByteArray> {
        val a = rnd.nextInt(0, bound)
        val b = rnd.nextInt(0, bound)
        val (lo, hi) = if (cmp.compare(a, b) <= 0) a to b else b to a
        return le(lo) to le(hi)
    }

    private fun longPair(
        rnd: kotlin.random.Random,
        cmp: Comparator<Long>,
    ): Pair<ByteArray, ByteArray> {
        val a = rnd.nextLong()
        val b = rnd.nextLong()
        val (lo, hi) = if (cmp.compare(a, b) <= 0) a to b else b to a
        return le(lo) to le(hi)
    }

    private val shapes: List<Shape> by lazy {
        buildList {
            // Signed int32, annotated and bare. Parquet orders these signed.
            val signedInt32 =
                listOf(
                    "int32" to null,
                    "int32/int8s" to LogicalTypeAnnotation.intType(8, true),
                    "int32/int16s" to LogicalTypeAnnotation.intType(16, true),
                    "int32/int32s" to LogicalTypeAnnotation.intType(32, true),
                )
            for ((label, ann) in signedInt32) {
                add(
                    Shape(label, leafOf(PrimitiveTypeName.INT32, ann)) { _, rnd ->
                        // Full signed domain, shifted so negatives appear.
                        val cmp = Comparator<Int> { x, y -> x.compareTo(y) }
                        val (a, b) = intPair(rnd, Int.MAX_VALUE, cmp)
                        val sa = ByteBuffer.wrap(a).order(ByteOrder.LITTLE_ENDIAN).int - 1_000_000
                        val sb = ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN).int - 1_000_000
                        le(minOf(sa, sb)) to le(maxOf(sa, sb))
                    },
                )
            }
            // UNSIGNED int32 at each width. Parquet orders these UNSIGNED,
            // which is the whole point: the raw bits of the max can have
            // the high bit set.
            for (width in listOf(8, 16, 32)) {
                add(
                    Shape(
                        "int32/uint$width",
                        leafOf(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(width, false)),
                    ) { _, rnd ->
                        val span = if (width == 32) 0xFFFFFFFFL else (1L shl width) - 1
                        val a = (rnd.nextDouble() * span).toLong()
                        val b = (rnd.nextDouble() * span).toLong()
                        le(minOf(a, b).toInt()) to le(maxOf(a, b).toInt())
                    },
                )
            }
            add(
                Shape("int64", leafOf(PrimitiveTypeName.INT64)) { _, rnd ->
                    longPair(rnd) { x, y -> x.compareTo(y) }
                },
            )
            add(
                Shape(
                    "int64/uint64",
                    leafOf(PrimitiveTypeName.INT64, LogicalTypeAnnotation.intType(64, false)),
                ) { _, rnd ->
                    longPair(rnd) { x, y -> java.lang.Long.compareUnsigned(x, y) }
                },
            )
            for (unit in LogicalTypeAnnotation.TimeUnit.entries) {
                add(
                    Shape(
                        "int64/ts-$unit",
                        leafOf(
                            PrimitiveTypeName.INT64,
                            LogicalTypeAnnotation.timestampType(false, unit),
                        ),
                    ) { _, rnd ->
                        // Bounded so unit conversion cannot overflow into the
                        // null-bound path on every single sample.
                        val a = rnd.nextLong(-1_000_000_000_000L, 1_000_000_000_000L)
                        val b = rnd.nextLong(-1_000_000_000_000L, 1_000_000_000_000L)
                        le(minOf(a, b)) to le(maxOf(a, b))
                    },
                )
            }
            add(
                Shape(
                    "int64/time-us",
                    leafOf(
                        PrimitiveTypeName.INT64,
                        LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MICROS),
                    ),
                ) { _, rnd ->
                    val a = rnd.nextLong(0, 86_400_000_000L)
                    val b = rnd.nextLong(0, 86_400_000_000L)
                    le(minOf(a, b)) to le(maxOf(a, b))
                },
            )
            add(
                Shape("int32/date", leafOf(PrimitiveTypeName.INT32, LogicalTypeAnnotation.dateType())) { _, rnd ->
                    val a = rnd.nextInt(-50_000, 50_000)
                    val b = rnd.nextInt(-50_000, 50_000)
                    le(minOf(a, b)) to le(maxOf(a, b))
                },
            )
            add(
                Shape("float", leafOf(PrimitiveTypeName.FLOAT)) { _, rnd ->
                    val a = rnd.nextFloat() * 1000f
                    val b = rnd.nextFloat() * 1000f
                    le(java.lang.Float.floatToRawIntBits(minOf(a, b))) to
                        le(java.lang.Float.floatToRawIntBits(maxOf(a, b)))
                },
            )
            add(
                Shape("double", leafOf(PrimitiveTypeName.DOUBLE)) { _, rnd ->
                    val a = rnd.nextDouble() * 1000.0
                    val b = rnd.nextDouble() * 1000.0
                    le(java.lang.Double.doubleToRawLongBits(minOf(a, b))) to
                        le(java.lang.Double.doubleToRawLongBits(maxOf(a, b)))
                },
            )
            // BYTE_ARRAY flavours: parquet orders these unsigned lexicographic.
            val byteArrays =
                listOf(
                    "binary" to null,
                    "binary/string" to LogicalTypeAnnotation.stringType(),
                    "binary/json" to LogicalTypeAnnotation.jsonType(),
                )
            for ((label, ann) in byteArrays) {
                add(
                    Shape(label, leafOf(PrimitiveTypeName.BINARY, ann)) { _, rnd ->
                        val a = ByteArray(rnd.nextInt(1, 8)) { rnd.nextInt(256).toByte() }
                        val b = ByteArray(rnd.nextInt(1, 8)) { rnd.nextInt(256).toByte() }
                        if (java.util.Arrays.compareUnsigned(a, b) <= 0) a to b else b to a
                    },
                )
            }
            add(
                Shape(
                    "binary/decimal",
                    leafOf(PrimitiveTypeName.BINARY, LogicalTypeAnnotation.decimalType(0, 20)),
                ) { _, rnd ->
                    // Non-negative, fixed 8-byte big-endian: with the high
                    // bit clear and both operands the same length, numeric
                    // order and unsigned byte order coincide. That keeps the
                    // pair correctly ordered under BOTH readings, so a
                    // decimal-annotated leaf read as a string/binary column
                    // (a type mismatch, but one the matrix generates) does
                    // not fail the invariant for a reason that is really an
                    // ordering disagreement rather than a decode bug.
                    val a = rnd.nextLong(0, Long.MAX_VALUE)
                    val b = rnd.nextLong(0, Long.MAX_VALUE)

                    fun be(v: Long) = ByteBuffer.allocate(8).putLong(v).array()
                    be(minOf(a, b)) to be(maxOf(a, b))
                },
            )
            add(
                Shape(
                    "fixed16/uuid",
                    leafOf(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, LogicalTypeAnnotation.uuidType(), 16),
                ) { _, rnd ->
                    val a = ByteArray(16) { rnd.nextInt(256).toByte() }
                    val b = ByteArray(16) { rnd.nextInt(256).toByte() }
                    if (java.util.Arrays.compareUnsigned(a, b) <= 0) a to b else b to a
                },
            )
            add(
                Shape("boolean", leafOf(PrimitiveTypeName.BOOLEAN)) { _, _ ->
                    byteArrayOf(0) to byteArrayOf(1)
                },
            )
        }
    }

    /** The bound width the mapped Iceberg type demands; null = variable. */
    private fun expectedWidth(type: ColType): Int? =
        when (type.icebergType) {
            IcebergType.BOOLEAN -> 1
            IcebergType.INT, IcebergType.FLOAT, IcebergType.DATE -> 4
            IcebergType.LONG, IcebergType.DOUBLE, IcebergType.TIME,
            IcebergType.TIMESTAMP, IcebergType.TIMESTAMP_NS, IcebergType.TIMESTAMPTZ,
            -> 8
            IcebergType.UUID -> 16
            // decimal/string/binary are legitimately variable-length.
            IcebergType.DECIMAL, IcebergType.STRING, IcebergType.BINARY -> null
        }

    private fun footerFor(
        shape: Shape,
        min: ByteArray,
        max: ByteArray,
    ): ParquetMetadata {
        val st =
            Statistics.getBuilderForReading(shape.leaf)
                .withMin(min)
                .withMax(max)
                .withNumNulls(0L)
                .build()
        val chunk =
            ColumnChunkMetaData.get(
                ColumnPath.get(shape.leaf.name),
                shape.leaf,
                CompressionCodecName.UNCOMPRESSED,
                null,
                setOf(Encoding.PLAIN),
                st,
                4L,
                0L,
                10L,
                100L,
                200L,
            )
        val block =
            BlockMetaData().apply {
                rowCount = 10
                totalByteSize = 100
                addColumn(chunk)
            }
        return ParquetMetadata(
            FileMetaData(MessageType("root", listOf(shape.leaf)), emptyMap(), "qe"),
            listOf(block),
        )
    }

    @Test
    fun `a produced bound is mapped-type-wide and never inverted`() =
        runBlocking<Unit> {
            val failures = mutableListOf<String>()
            var produced = 0
            var refused = 0

            checkAll(
                Arb.int(0, ColType.entries.size - 1),
                Arb.int(0, shapes.size - 1),
                Arb.long(),
            ) { typeIdx, shapeIdx, seed ->
                val type = ColType.entries[typeIdx]
                val shape = shapes[shapeIdx]
                val rnd = kotlin.random.Random(seed)
                val (min, max) = shape.sortedPair(Arb, rnd)

                val agg =
                    FooterStats.aggregate(
                        footerFor(shape, min, max),
                        listOf(
                            CatalogColumn(
                                fieldId = 1,
                                name = "v",
                                type = type,
                                // Matches the decimal shape's annotation so the
                                // decimal arm is exercised rather than always
                                // skipped on a scale mismatch.
                                decimalScale = 20,
                            ),
                        ),
                        "s3://qe/f.parquet",
                    ).singleOrNull() ?: return@checkAll

                val lower = agg.lowerBound
                val upper = agg.upperBound
                if (lower == null || upper == null) {
                    // Refusing is always a correct answer.
                    refused++
                    return@checkAll
                }
                produced++

                val want = expectedWidth(type)
                if (want != null && (lower.size != want || upper.size != want)) {
                    failures +=
                        "${type.wire} on ${shape.name}: bound widths ${lower.size}/${upper.size}, " +
                        "expected $want for mapped ${type.icebergType.wire}"
                    return@checkAll
                }

                // String/json/binary/uuid bounds are compared as STORED
                // BYTES, unsigned — that is what Iceberg does, and it also
                // sidesteps a decode artifact: invalid UTF-8 decodes to
                // U+FFFD replacements, so comparing the decoded Strings
                // would report an inversion that the stored bytes do not
                // have. Everything else compares as its decoded value,
                // since signed little-endian ints do not sort bytewise.
                val byteOrdered =
                    type.icebergType in
                        setOf(IcebergType.STRING, IcebergType.BINARY, IcebergType.UUID)
                val inverted =
                    if (byteOrdered) {
                        java.util.Arrays.compareUnsigned(lower, upper) > 0
                    } else {
                        val lo =
                            try {
                                IcebergSingleValue.decode(type, lower)
                            } catch (e: IllegalArgumentException) {
                                failures +=
                                    "${type.wire} on ${shape.name}: stored bound will not decode: ${e.message}"
                                return@checkAll
                            }
                        val hi = IcebergSingleValue.decode(type, upper)
                        // NaN is unordered; FooterStats already refuses NaN
                        // bounds, so anything reaching here must compare.
                        IcebergSingleValue.compareValues(type, lo, hi) > 0
                    }
                if (inverted) {
                    failures +=
                        "${type.wire} on ${shape.name}: INVERTED range " +
                        "lower=${lower.toHex()} upper=${upper.toHex()} " +
                        "(raw min=${min.toHex()} max=${max.toHex()})"
                }
            }

            assertThat(failures).describedAs("decode-matrix violations").isEmpty()
            // Guard against the property passing vacuously: the matrix must
            // actually be producing bounds, not refusing everything.
            assertThat(produced)
                .describedAs("samples that produced a bound (refused %d)", refused)
                .isGreaterThan(100)
        }

    private fun ByteArray.toHex(): String = joinToString("") { "%02x".format(it) }
}
