package com.posthog.hoglake.hydrator

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.IcebergType
import com.posthog.hoglake.model.icebergType
import com.posthog.hoglake.stats.IcebergSingleValue
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
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.random.Random

/**
 * Assault on FooterStats' decode matrix: every catalog type crossed with
 * every physical/logical shape a writer might hand us.
 *
 * The matrix is ENUMERATED, not sampled: every catalog type against
 * every leaf shape, asserted against the full cross product rather than
 * a counted total. Each cell costs a handful of pure function calls, so
 * a random walk over it buys
 * nothing and costs coverage: at the default property-test budget a
 * typical run left dozens of cells untouched, which made a reintroduced
 * sign-extension bug a coin flip rather than a failure. Every cell now
 * runs, with a seed derived from the cell's own identity, so a failure
 * reproduces exactly from its name alone.
 *
 * Three claims, per cell:
 *
 *  1. A produced bound is encoded at the MAPPED Iceberg type's width
 *     (ColType.icebergType, docs/iceberg-federation.md §2) — a 4-byte bound
 *     under a long column is the stale-width poison that wedges
 *     compaction's bound-merge.
 *  2. lower <= upper, compared under the catalog type.
 *  3. The cells in [mustProduce] produce a bound at all.
 *  4. The cells in [mustRefuse] produce NO bound at all.
 *
 * Claim 2 catches an entire bug CLASS generically rather than one
 * instance of it. The unsigned-int32-under-a-long-column defect
 * (0xFFFFFFFF sign-extending to -1) produced exactly an inverted range,
 * and an inverted range is worse than a wrong one: a pruner reads it as
 * "no rows here" and drops the file from every scan while all the counts
 * still look healthy. Any future decode arm that reinterprets bits
 * without honouring the annotation that ordered them lands here the same
 * way — but only if the offending bits are actually generated, so every
 * fixed-width shape hard-codes its high-bit-set patterns (0xFFFFFFFF for
 * a 32-bit leaf, Long.MIN_VALUE's pattern for a 64-bit one) instead of
 * hoping a sampler stumbles onto them. Those patterns are ~2^-31 of the
 * domain; they are 100% of the bugs.
 *
 * Claim 3 is what stops the whole thing from passing vacuously. "Bounds
 * NULL, never guessed" is the standing contract, so a refusal is always
 * a *safe* answer — which means a decode arm could go dark and claims 1
 * and 2 would still be perfectly satisfied by the resulting silence.
 * [mustProduce] names the pairings a writer in the wild actually emits,
 * and refusing one of those is a pruning regression, not caution.
 *
 * Claim 4 is claim 3's mirror, and it exists because claims 1-3 are all
 * satisfied by a gate that is too PERMISSIVE as long as the values it
 * lets through happen to be small. The per-width unsigned rule is
 * exactly such a gate: widening it back to "only full-width unsigned is
 * refused" keeps every other assertion green, because an INT(16,
 * unsigned) leaf under an int8 column still decodes to a positive int
 * that fits four bytes and still sorts the right way round — it is just
 * 65535 written as an int8 bound. [mustRefuse] names the cells where a
 * bound must not appear at all.
 *
 * Generated (min, max) pairs are always ordered in the LEAF'S OWN
 * parquet sort order, because that is what a real writer's footer
 * contains — an unsigned annotation means parquet ordered the chunk
 * unsigned, and reading those bytes signed is precisely the mistake.
 */
class QeFooterStatsBoundsPropertyTest {
    private companion object {
        /** Extra generated pairs per cell, on top of the pinned boundary ones. */
        const val RANDOM_PAIRS_PER_CELL = 4
    }

    /** One physical/logical leaf shape, with the ordering parquet gives it. */
    private class Shape(
        val name: String,
        val leaf: PrimitiveType,
        /**
         * Pairs every cell of this shape's column gets, pinned in source:
         * the domain edges and the bit patterns that expose reinterpretation
         * bugs. Ordered in the leaf's own parquet sort order.
         */
        val boundary: List<Pair<ByteArray, ByteArray>>,
        /** One more same-ordering pair, from the cell's pinned Random. */
        val random: (Random) -> Pair<ByteArray, ByteArray>,
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

    /** Big-endian, the byte order parquet uses for BINARY-backed decimals. */
    private fun be(v: Long): ByteArray = ByteBuffer.allocate(8).putLong(v).array()

    private fun intPairs(vararg pairs: Pair<Int, Int>): List<Pair<ByteArray, ByteArray>> =
        pairs.map { le(it.first) to le(it.second) }

    private fun longPairs(vararg pairs: Pair<Long, Long>): List<Pair<ByteArray, ByteArray>> =
        pairs.map { le(it.first) to le(it.second) }

    private fun bytePairs(vararg pairs: Pair<ByteArray, ByteArray>): List<Pair<ByteArray, ByteArray>> = pairs.toList()

    private fun bytes(vararg v: Int): ByteArray = ByteArray(v.size) { v[it].toByte() }

    private fun filled(
        size: Int,
        v: Int,
    ): ByteArray = ByteArray(size) { v.toByte() }

    /** Signed-int32 leaves, annotated and bare; parquet orders them signed. */
    private val signedInt32Shapes =
        listOf(
            "int32" to null,
            "int32/int8s" to LogicalTypeAnnotation.intType(8, true),
            "int32/int16s" to LogicalTypeAnnotation.intType(16, true),
            "int32/int32s" to LogicalTypeAnnotation.intType(32, true),
        )

    /** BYTE_ARRAY leaves; parquet orders all three unsigned lexicographic. */
    private val byteArrayShapes =
        listOf(
            "binary" to null,
            "binary/string" to LogicalTypeAnnotation.stringType(),
            "binary/json" to LogicalTypeAnnotation.jsonType(),
        )

    private val shapes: List<Shape> by lazy {
        buildList {
            for ((label, ann) in signedInt32Shapes) {
                add(
                    Shape(
                        label,
                        leafOf(PrimitiveTypeName.INT32, ann),
                        // Both int32 extremes and the all-ones pattern, which is
                        // -1 signed: an arm that reads these bits unsigned puts
                        // the max below the min.
                        intPairs(
                            Int.MIN_VALUE to Int.MAX_VALUE,
                            -1 to 0,
                            Int.MIN_VALUE to Int.MIN_VALUE,
                            Int.MAX_VALUE to Int.MAX_VALUE,
                        ),
                    ) { rnd ->
                        val a = rnd.nextInt()
                        val b = rnd.nextInt()
                        le(minOf(a, b)) to le(maxOf(a, b))
                    },
                )
            }
            // UNSIGNED int32 at each width. Parquet orders these UNSIGNED,
            // which is the whole point: the raw bits of the max can have the
            // high bit set while still being the larger value.
            for (width in listOf(8, 16, 32)) {
                val span = if (width == 32) 0xFFFFFFFFL else (1L shl width) - 1
                val top = span.toInt()
                val mid = (span / 2 + 1).toInt()
                add(
                    Shape(
                        "int32/uint$width",
                        leafOf(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(width, false)),
                        // (0, top) is the pair that kills a sign-extending
                        // reader at width 32: signed it reads (0, -1), which is
                        // the inverted range a pruner obeys. (mid, top) stays
                        // ordered under both readings, so it is the control.
                        intPairs(0 to top, mid to top, top to top, 0 to 0),
                    ) { rnd ->
                        val a = rnd.nextLong(0, span + 1)
                        val b = rnd.nextLong(0, span + 1)
                        le(minOf(a, b).toInt()) to le(maxOf(a, b).toInt())
                    },
                )
            }
            add(
                Shape(
                    "int64",
                    leafOf(PrimitiveTypeName.INT64),
                    longPairs(
                        Long.MIN_VALUE to Long.MAX_VALUE,
                        -1L to 0L,
                        Long.MIN_VALUE to Long.MIN_VALUE,
                        Long.MAX_VALUE to Long.MAX_VALUE,
                    ),
                ) { rnd ->
                    val a = rnd.nextLong()
                    val b = rnd.nextLong()
                    le(minOf(a, b)) to le(maxOf(a, b))
                },
            )
            add(
                Shape(
                    "int64/uint64",
                    leafOf(PrimitiveTypeName.INT64, LogicalTypeAnnotation.intType(64, false)),
                    // (MAX, MIN) is unsigned-ordered (2^63-1 < 2^63) and
                    // signed-INVERTED, so it is the 64-bit twin of the uint32
                    // trap; (0, -1) spans the whole unsigned domain.
                    longPairs(
                        0L to -1L,
                        Long.MAX_VALUE to Long.MIN_VALUE,
                        Long.MIN_VALUE to -1L,
                        -1L to -1L,
                    ),
                ) { rnd ->
                    val a = rnd.nextLong()
                    val b = rnd.nextLong()
                    if (java.lang.Long.compareUnsigned(a, b) <= 0) le(a) to le(b) else le(b) to le(a)
                },
            )
            for (unit in LogicalTypeAnnotation.TimeUnit.entries) {
                // A millis leaf's int64 extremes are NOT reachable bounds: the
                // read scales them to micros, which overflows, and the contract
                // says a null bound rather than a wrapped one. Cap them so the
                // edge still exercises the arm instead of only its overflow
                // guard (that guard has its own coverage in the unit tests).
                val edge = if (unit == LogicalTypeAnnotation.TimeUnit.MILLIS) Long.MAX_VALUE / 1_000 else Long.MAX_VALUE
                add(
                    Shape(
                        "int64/ts-$unit",
                        leafOf(PrimitiveTypeName.INT64, LogicalTypeAnnotation.timestampType(false, unit)),
                        longPairs(-edge to edge, -1L to 0L, -edge to -edge, edge to edge),
                    ) { rnd ->
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
                    // Micros are the stored unit, so no scaling can overflow and
                    // the int64 extremes are legitimate bound bits here.
                    longPairs(
                        Long.MIN_VALUE to Long.MAX_VALUE,
                        -1L to 0L,
                        0L to 86_399_999_999L,
                    ),
                ) { rnd ->
                    val a = rnd.nextLong(0, 86_400_000_000L)
                    val b = rnd.nextLong(0, 86_400_000_000L)
                    le(minOf(a, b)) to le(maxOf(a, b))
                },
            )
            add(
                Shape(
                    "int32/date",
                    leafOf(PrimitiveTypeName.INT32, LogicalTypeAnnotation.dateType()),
                    intPairs(Int.MIN_VALUE to Int.MAX_VALUE, -1 to 0, 0 to 0),
                ) { rnd ->
                    val a = rnd.nextInt(-50_000, 50_000)
                    val b = rnd.nextInt(-50_000, 50_000)
                    le(minOf(a, b)) to le(maxOf(a, b))
                },
            )
            add(
                Shape(
                    "float",
                    leafOf(PrimitiveTypeName.FLOAT),
                    // Negative floats have the sign bit set, so the all-negative
                    // pair is this shape's high-bit case; -0.0/0.0 pins the one
                    // place IEEE order and bit order disagree. No NaN: FooterStats
                    // refuses NaN bounds by contract, and this shape's cells are
                    // must-produce.
                    listOf(
                        le((-Float.MAX_VALUE).toRawBits()) to le(Float.MAX_VALUE.toRawBits()),
                        le((-1000f).toRawBits()) to le((-0.5f).toRawBits()),
                        le((-0.0f).toRawBits()) to le(0.0f.toRawBits()),
                    ),
                ) { rnd ->
                    val a = (rnd.nextFloat() - 0.5f) * 1000f
                    val b = (rnd.nextFloat() - 0.5f) * 1000f
                    le(minOf(a, b).toRawBits()) to le(maxOf(a, b).toRawBits())
                },
            )
            add(
                Shape(
                    "double",
                    leafOf(PrimitiveTypeName.DOUBLE),
                    listOf(
                        le((-Double.MAX_VALUE).toRawBits()) to le(Double.MAX_VALUE.toRawBits()),
                        le((-1000.0).toRawBits()) to le((-0.5).toRawBits()),
                        le((-0.0).toRawBits()) to le(0.0.toRawBits()),
                    ),
                ) { rnd ->
                    val a = (rnd.nextDouble() - 0.5) * 1000.0
                    val b = (rnd.nextDouble() - 0.5) * 1000.0
                    le(minOf(a, b).toRawBits()) to le(maxOf(a, b).toRawBits())
                },
            )
            for ((label, ann) in byteArrayShapes) {
                add(
                    Shape(
                        label,
                        leafOf(PrimitiveTypeName.BINARY, ann),
                        // 0x7F/0x80 is the byte-array analogue of the integer
                        // sign trap: ordered unsigned, inverted signed.
                        bytePairs(
                            bytes(0x00) to bytes(0xFF),
                            bytes(0x7F) to bytes(0x80),
                            filled(4, 0x00) to filled(4, 0xFF),
                            filled(4, 0xFF) to filled(4, 0xFF),
                        ),
                    ) { rnd ->
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
                    // The one shape that deliberately keeps the high bit CLEAR.
                    // A BINARY decimal reads as a signed two's-complement
                    // BigInteger but as unsigned bytes under a string/binary
                    // column, and the matrix generates both: with the high bit
                    // clear and equal lengths the two orderings coincide, so an
                    // inversion here means a decode bug rather than a
                    // disagreement about what the bytes are.
                    bytePairs(be(0L) to be(Long.MAX_VALUE), be(0L) to be(0L), be(Long.MAX_VALUE) to be(Long.MAX_VALUE)),
                ) { rnd ->
                    val a = rnd.nextLong(0, Long.MAX_VALUE)
                    val b = rnd.nextLong(0, Long.MAX_VALUE)
                    be(minOf(a, b)) to be(maxOf(a, b))
                },
            )
            add(
                Shape(
                    "fixed16/uuid",
                    leafOf(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, LogicalTypeAnnotation.uuidType(), 16),
                    bytePairs(
                        filled(16, 0x00) to filled(16, 0xFF),
                        bytes(0x7F) + filled(15, 0xFF) to bytes(0x80) + filled(15, 0x00),
                        filled(16, 0xFF) to filled(16, 0xFF),
                    ),
                ) { rnd ->
                    val a = ByteArray(16) { rnd.nextInt(256).toByte() }
                    val b = ByteArray(16) { rnd.nextInt(256).toByte() }
                    if (java.util.Arrays.compareUnsigned(a, b) <= 0) a to b else b to a
                },
            )
            add(
                Shape(
                    "boolean",
                    leafOf(PrimitiveTypeName.BOOLEAN),
                    bytePairs(bytes(0) to bytes(1), bytes(0) to bytes(0), bytes(1) to bytes(1)),
                ) { _ -> bytes(0) to bytes(1) },
            )
        }
    }

    /**
     * The (catalog type, shape) cells that MUST come back with a bound.
     *
     * Everything here is a pairing a real writer emits — pyarrow, DuckDB,
     * or hoglake's own writer — so a NULL bound is lost pruning on live
     * data, not the decode path being careful. The set is deliberately
     * narrower than "every cell that happens to work today": it names the
     * arms whose loss would be a regression, and leaves the incidental
     * pairings (a date leaf under an int column, say) to claims 1 and 2.
     */
    private val mustProduce: Set<Pair<ColType, String>> by lazy {
        buildSet {
            // The five types that ride parquet INT32 with either no
            // annotation or a signed one.
            val int32Types = listOf(ColType.INT8, ColType.INT16, ColType.UINT8, ColType.UINT16, ColType.INT)
            for ((label, _) in signedInt32Shapes) {
                for (t in int32Types) {
                    add(t to label)
                }
            }
            // Small unsigned widths: signed and unsigned parquet order agree
            // below 2^16, which is what lets these types read them at all.
            add(ColType.UINT8 to "int32/uint8")
            add(ColType.UINT16 to "int32/uint8")
            add(ColType.UINT16 to "int32/uint16")
            add(ColType.INT to "int32/uint8")
            add(ColType.INT to "int32/uint16")
            // uint32's two physical spellings: hoglake writes INT64,
            // pyarrow/DuckDB write INT32 + INT(32, unsigned).
            add(ColType.UINT32 to "int32/uint32")
            add(ColType.UINT32 to "int64")
            // long: the plain signed case, plus the zero-extension cases
            // a foreign writer produces — arrow and DuckDB emit unsigned
            // data as INT32 + INT(w, unsigned), and long's domain holds
            // all of them up to 32 bits.
            add(ColType.LONG to "int64")
            for (w in listOf(8, 16, 32)) {
                add(ColType.LONG to "int32/uint$w")
            }
            add(ColType.UINT64 to "int64/uint64")
            // Each timestamp type against the unit its own files carry.
            // timestamp_s has no parquet unit of its own: pyarrow coerces
            // timestamp[s] to TIMESTAMP(MILLIS) on write.
            add(ColType.TIMESTAMP_S to "int64/ts-MILLIS")
            add(ColType.TIMESTAMP_MS to "int64/ts-MILLIS")
            add(ColType.TIMESTAMP to "int64/ts-MICROS")
            add(ColType.TIMESTAMPTZ to "int64/ts-MICROS")
            add(ColType.TIMESTAMP_NS to "int64/ts-NANOS")
            add(ColType.TIME to "int64/time-us")
            add(ColType.DATE to "int32/date")
            add(ColType.FLOAT to "float")
            add(ColType.DOUBLE to "double")
            // float under a double column: the legal FLOAT -> DOUBLE promotion
            // leaves 4-byte stats bytes behind a double column forever.
            add(ColType.DOUBLE to "float")
            for ((label, _) in byteArrayShapes) {
                add(ColType.STRING to label)
                add(ColType.JSON to label)
                add(ColType.BINARY to label)
            }
            // binary accepts FIXED_LEN too, so a uuid-shaped file under a
            // binary column still bounds.
            add(ColType.BINARY to "fixed16/uuid")
            add(ColType.UUID_T to "fixed16/uuid")
            add(ColType.DECIMAL to "binary/decimal")
            add(ColType.BOOLEAN to "boolean")
        }
    }

    /**
     * Cells that must yield NO bound: an unsigned leaf whose domain
     * [0, 2^w) does not fit the catalog type's own
     * (ColType.maxUnsignedParquetWidth).
     *
     * Every entry here is a NARROW width, on purpose. A rule that only
     * refuses full-width unsigned annotations passes claims 1-3 — the
     * values still fit four bytes and still sort correctly — while
     * quietly storing 65535 as an int8 column's upper bound. These are
     * the cells that tell the two rules apart.
     */
    private val mustRefuse: Set<Pair<ColType, String>> by lazy {
        buildSet {
            // int8 holds 127: no unsigned width fits, not even 8.
            add(ColType.INT8 to "int32/uint8")
            add(ColType.INT8 to "int32/uint16")
            // int16 holds 32767: takes INT(8,u), not INT(16,u).
            add(ColType.INT16 to "int32/uint16")
            // uint8 holds 255.
            add(ColType.UINT8 to "int32/uint16")
            // uint16 holds 65535.
            add(ColType.UINT16 to "int32/uint32")
            // int and date are int32-domain: 16 bits fit, 32 do not.
            add(ColType.INT to "int32/uint32")
            add(ColType.DATE to "int32/uint32")
            // 64-bit unsigned belongs to uint64 alone.
            add(ColType.LONG to "int64/uint64")
            add(ColType.UINT32 to "int64/uint64")
            add(ColType.TIMESTAMP to "int64/uint64")
        }
    }

    /** The bound width the mapped Iceberg type demands; null = variable. */
    private fun expectedWidth(type: ColType): Int? =
        when (type.icebergType) {
            IcebergType.VARIANT -> null
            IcebergType.BOOLEAN -> 1
            IcebergType.INT, IcebergType.FLOAT, IcebergType.DATE -> 4
            IcebergType.LONG, IcebergType.DOUBLE, IcebergType.TIME,
            IcebergType.TIMESTAMP, IcebergType.TIMESTAMP_NS, IcebergType.TIMESTAMPTZ,
            -> 8
            IcebergType.UUID -> 16
            // decimal/string/binary are legitimately variable-length.
            IcebergType.DECIMAL, IcebergType.STRING, IcebergType.BINARY -> null
            // Containers have no bound at all; no catalog column in this
            // matrix is one (the shapes are scalar leaves).
            IcebergType.LIST, IcebergType.STRUCT, IcebergType.MAP -> null
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

    private fun columnOf(type: ColType): CatalogColumn =
        CatalogColumn(
            fieldId = 1,
            name = "v",
            type = type,
            // Must equal the decimal shape's annotated scale — decimalType(0, 20)
            // is decimal(precision 20, scale 0). A mismatch is not a failure, it
            // is a silent skip, so getting this wrong switches the entire decimal
            // arm off while the test still passes.
            decimalScale = 0,
        )

    @Test
    fun `every catalog type x leaf shape cell is mapped-type-wide and never inverted`() {
        val failures = mutableListOf<String>()
        val visited = mutableSetOf<Pair<ColType, String>>()
        var produced = 0
        var refused = 0

        for (type in ColType.entries.filter { it != ColType.VARIANT }) {
            for (shape in shapes) {
                visited += type to shape.name
                // Pinned to the cell's identity, not to a run: a failure names
                // the cell, and rerunning that cell replays the same bytes.
                val rnd = Random("${type.wire}/${shape.name}".hashCode().toLong())
                val pairs = shape.boundary + List(RANDOM_PAIRS_PER_CELL) { shape.random(rnd) }
                val required = (type to shape.name) in mustProduce
                val forbidden = (type to shape.name) in mustRefuse

                for ((min, max) in pairs) {
                    val aggs =
                        FooterStats.aggregate(
                            footerFor(shape, min, max),
                            listOf(columnOf(type)),
                            "s3://qe/f.parquet",
                        )
                    if (type.isNested) {
                        // A container column over a SCALAR leaf is a shape
                        // disagreement, and the whole subtree drops — no
                        // stats row for the container (it has no values)
                        // and none for a child that is not in the file.
                        // This is the mustRefuse claim for the three
                        // container types, over every leaf shape at once.
                        if (aggs.isNotEmpty()) {
                            failures +=
                                "${type.wire} on ${shape.name}: produced ${aggs.size} stats row(s); " +
                                "a nested container over a scalar leaf must produce none"
                        }
                        continue
                    }
                    val agg = aggs.singleOrNull()
                    if (agg == null) {
                        // Counts are unconditional: dropping the whole stats row
                        // loses null_count too, not just the bounds.
                        failures += "${type.wire} on ${shape.name}: no stats row at all"
                        continue
                    }

                    val lower = agg.lowerBound
                    val upper = agg.upperBound
                    if (lower == null || upper == null) {
                        refused++
                        if (required) {
                            failures +=
                                "${type.wire} on ${shape.name}: NULL bound for a pairing writers " +
                                "emit (raw min=${min.toHex()} max=${max.toHex()})"
                        }
                        continue
                    }
                    produced++
                    if (forbidden) {
                        failures +=
                            "${type.wire} on ${shape.name}: produced a bound from an unsigned leaf " +
                            "wider than its domain (raw min=${min.toHex()} max=${max.toHex()}, " +
                            "lower=${lower.toHex()} upper=${upper.toHex()})"
                        continue
                    }
                    failures += violations(type, shape, min, max, lower, upper)
                }
            }
        }

        assertThat(failures).describedAs("decode-matrix violations").isEmpty()
        // The SET of cells actually visited, compared against the full
        // cross product. A counter incremented inside the loop only ever
        // proves the loop ran as many times as it ran.
        assertThat(visited)
            .describedAs("the full matrix ran, not a sample of it")
            .isEqualTo(
                ColType.entries.filter { it != ColType.VARIANT }.flatMap { t -> shapes.map { t to it.name } }.toSet(),
            )
        assertThat(mustRefuse).describedAs("claim 4 is not vacuous").isNotEmpty()
        assertThat(mustProduce).describedAs("claim 3 is not vacuous").isNotEmpty()
        // Backstop for claim 3: even if mustProduce were gutted, a matrix that
        // stopped decoding wholesale would show up here. Most of the matrix is
        // genuinely mismatched (a boolean leaf under a uuid column, say), so the
        // floor sits below the ~710 the current arms produce, not near the total.
        assertThat(produced)
            .describedAs("cell values that produced a bound (refused %d)", refused)
            .isGreaterThan(600)
    }

    // ---- the nested half of the matrix ------------------------------------

    /**
     * One nested cell: a catalog column tree, the parquet schema a
     * writer produced for it, and which LEAF field ids must come back
     * with a bound.
     *
     * The mustProduce/mustRefuse split is the same discipline as the
     * scalar matrix above, and it exists for the same reason: every
     * structural refusal here is a SAFE answer, so a walk that only
     * asserted "no wrong bounds" would pass just as happily against a
     * FooterStats that had stopped descending into nested schemas at
     * all. Naming the ids that must appear is what keeps it honest.
     */
    private class NestedCell(
        val name: String,
        val column: CatalogColumn,
        val schema: MessageType,
        /** (chunk path, leaf type, min, max) per parquet leaf with statistics. */
        val leaves: List<NestedLeaf>,
        /** Field ids that must come back WITH bounds. */
        val mustProduce: Set<Long>,
        /** Field ids that must not appear in the results at all. */
        val mustRefuse: Set<Long>,
    )

    private class NestedLeaf(
        val path: List<String>,
        val type: PrimitiveType,
        val min: ByteArray,
        val max: ByteArray,
    )

    private fun nestedFooter(cell: NestedCell): ParquetMetadata {
        val block =
            BlockMetaData().apply {
                rowCount = 10
                totalByteSize = 100
            }
        for (leaf in cell.leaves) {
            val st =
                Statistics.getBuilderForReading(leaf.type)
                    .withMin(leaf.min)
                    .withMax(leaf.max)
                    .withNumNulls(0L)
                    .build()
            block.addColumn(
                ColumnChunkMetaData.get(
                    ColumnPath.get(*leaf.path.toTypedArray()),
                    leaf.type,
                    CompressionCodecName.UNCOMPRESSED,
                    null,
                    setOf(Encoding.PLAIN),
                    st,
                    4L,
                    0L,
                    10L,
                    100L,
                    200L,
                ),
            )
        }
        return ParquetMetadata(FileMetaData(cell.schema, emptyMap(), "qe"), listOf(block))
    }

    private fun scalarChild(
        fieldId: Long,
        name: String,
        type: ColType,
    ) = CatalogColumn(fieldId = fieldId, name = name, type = type, decimalScale = null)

    private fun container(
        fieldId: Long,
        name: String,
        type: ColType,
        vararg children: CatalogColumn,
    ) = CatalogColumn(fieldId, name, type, null, children.toList())

    private fun optInt(
        id: Int,
        name: String,
    ): PrimitiveType = Types.optional(PrimitiveTypeName.INT32).id(id).named(name)

    private fun optLong(
        id: Int,
        name: String,
    ): PrimitiveType = Types.optional(PrimitiveTypeName.INT64).id(id).named(name)

    private fun reqString(
        id: Int,
        name: String,
    ): PrimitiveType =
        Types.required(PrimitiveTypeName.BINARY)
            .`as`(LogicalTypeAnnotation.stringType()).id(id).named(name)

    private fun listGroup(
        id: Int,
        name: String,
        element: org.apache.parquet.schema.Type,
    ) = Types.optionalGroup()
        .addField(Types.repeatedGroup().addField(element).named("list"))
        .`as`(LogicalTypeAnnotation.listType())
        .id(id).named(name)

    private fun mapGroup(
        id: Int,
        name: String,
        vararg entryFields: org.apache.parquet.schema.Type,
    ) = Types.optionalGroup()
        .addField(Types.repeatedGroup().addFields(*entryFields).named("key_value"))
        .`as`(LogicalTypeAnnotation.mapType())
        .id(id).named(name)

    private val nestedCells: List<NestedCell> by lazy {
        buildList {
            add(
                NestedCell(
                    name = "list<int> in the 3-level encoding",
                    column = container(1, "l", ColType.LIST, scalarChild(2, "element", ColType.INT)),
                    schema = MessageType("root", listOf(listGroup(1, "l", optInt(2, "element")))),
                    leaves =
                        listOf(
                            NestedLeaf(listOf("l", "list", "element"), optInt(2, "element"), le(-7), le(9)),
                        ),
                    // Iceberg records value_counts and bounds for list
                    // ELEMENTS; only the container itself gets none.
                    mustProduce = setOf(2L),
                    mustRefuse = setOf(1L),
                ),
            )
            add(
                NestedCell(
                    name = "map<string,long>",
                    column =
                        container(
                            1,
                            "m",
                            ColType.MAP,
                            scalarChild(2, "key", ColType.STRING),
                            scalarChild(3, "value", ColType.LONG),
                        ),
                    schema =
                        MessageType(
                            "root",
                            listOf(mapGroup(1, "m", reqString(2, "key"), optLong(3, "value"))),
                        ),
                    leaves =
                        listOf(
                            NestedLeaf(
                                listOf("m", "key_value", "key"),
                                reqString(2, "key"),
                                bytes(0x41),
                                bytes(0x7A),
                            ),
                            NestedLeaf(
                                listOf("m", "key_value", "value"),
                                optLong(3, "value"),
                                le(-1L),
                                le(1L shl 40),
                            ),
                        ),
                    mustProduce = setOf(2L, 3L),
                    mustRefuse = setOf(1L),
                ),
            )
            add(
                NestedCell(
                    name = "struct{a:int, b:string}",
                    column =
                        container(
                            1,
                            "s",
                            ColType.STRUCT,
                            scalarChild(2, "a", ColType.INT),
                            scalarChild(3, "b", ColType.STRING),
                        ),
                    schema =
                        MessageType(
                            "root",
                            listOf(
                                Types.optionalGroup()
                                    .addFields(optInt(2, "a"), reqString(3, "b"))
                                    .id(1).named("s"),
                            ),
                        ),
                    leaves =
                        listOf(
                            NestedLeaf(listOf("s", "a"), optInt(2, "a"), le(3), le(300)),
                            NestedLeaf(listOf("s", "b"), reqString(3, "b"), bytes(0x00), bytes(0xFF)),
                        ),
                    mustProduce = setOf(2L, 3L),
                    mustRefuse = setOf(1L),
                ),
            )
            add(
                NestedCell(
                    name = "struct{l: list<struct{x: long}>} (three levels)",
                    column =
                        container(
                            1,
                            "s",
                            ColType.STRUCT,
                            container(
                                2,
                                "l",
                                ColType.LIST,
                                container(3, "element", ColType.STRUCT, scalarChild(4, "x", ColType.LONG)),
                            ),
                        ),
                    schema =
                        MessageType(
                            "root",
                            listOf(
                                Types.optionalGroup()
                                    .addField(
                                        listGroup(
                                            2,
                                            "l",
                                            Types.optionalGroup().addField(optLong(4, "x")).id(3).named("element"),
                                        ),
                                    )
                                    .id(1).named("s"),
                            ),
                        ),
                    leaves =
                        listOf(
                            NestedLeaf(
                                listOf("s", "l", "list", "element", "x"),
                                optLong(4, "x"),
                                le(Long.MIN_VALUE),
                                le(Long.MAX_VALUE),
                            ),
                        ),
                    mustProduce = setOf(4L),
                    mustRefuse = setOf(1L, 2L, 3L),
                ),
            )
            add(
                NestedCell(
                    name = "list<int> over the LEGACY 2-level encoding",
                    column = container(1, "l", ColType.LIST, scalarChild(2, "element", ColType.INT)),
                    // repeated PRIMITIVE, not repeated group: the pre-2.x
                    // shape. Refused rather than guessed — the element's
                    // field id is not where the 3-level rule says it is.
                    schema =
                        MessageType(
                            "root",
                            listOf(
                                Types.optionalGroup()
                                    .addField(Types.repeated(PrimitiveTypeName.INT32).id(2).named("element"))
                                    .`as`(LogicalTypeAnnotation.listType())
                                    .id(1).named("l"),
                            ),
                        ),
                    leaves =
                        listOf(
                            NestedLeaf(
                                listOf("l", "element"),
                                Types.repeated(PrimitiveTypeName.INT32).id(2).named("element"),
                                le(1),
                                le(2),
                            ),
                        ),
                    mustProduce = emptySet(),
                    mustRefuse = setOf(1L, 2L),
                ),
            )
            add(
                NestedCell(
                    name = "list whose middle group is OPTIONAL, not repeated",
                    column = container(1, "l", ColType.LIST, scalarChild(2, "element", ColType.INT)),
                    // The repetition layer is what makes a LIST a list.
                    // A wrapper group holding a non-repeated group is not
                    // the 3-level encoding, whatever its annotation says,
                    // and reading its leaf as the element would report
                    // one value per row for a column that has many.
                    schema =
                        MessageType(
                            "root",
                            listOf(
                                Types.optionalGroup()
                                    .addField(
                                        Types.optionalGroup()
                                            .addField(optInt(2, "element"))
                                            .named("list"),
                                    )
                                    .`as`(LogicalTypeAnnotation.listType())
                                    .id(1).named("l"),
                            ),
                        ),
                    leaves =
                        listOf(
                            NestedLeaf(listOf("l", "list", "element"), optInt(2, "element"), le(1), le(2)),
                        ),
                    mustProduce = emptySet(),
                    mustRefuse = setOf(1L, 2L),
                ),
            )
            add(
                NestedCell(
                    name = "map whose key_value group has three fields",
                    column =
                        container(
                            1,
                            "m",
                            ColType.MAP,
                            scalarChild(2, "key", ColType.STRING),
                            scalarChild(3, "value", ColType.LONG),
                        ),
                    schema =
                        MessageType(
                            "root",
                            listOf(
                                mapGroup(
                                    1,
                                    "m",
                                    reqString(2, "key"),
                                    optLong(3, "value"),
                                    optInt(9, "extra"),
                                ),
                            ),
                        ),
                    leaves =
                        listOf(
                            NestedLeaf(
                                listOf("m", "key_value", "key"),
                                reqString(2, "key"),
                                bytes(0x41),
                                bytes(0x7A),
                            ),
                        ),
                    mustProduce = emptySet(),
                    mustRefuse = setOf(1L, 2L, 3L),
                ),
            )
            add(
                NestedCell(
                    name = "struct declared over a primitive leaf",
                    column =
                        container(1, "s", ColType.STRUCT, scalarChild(2, "a", ColType.INT)),
                    schema = MessageType("root", listOf(optInt(1, "s"))),
                    leaves = listOf(NestedLeaf(listOf("s"), optInt(1, "s"), le(1), le(2))),
                    mustProduce = emptySet(),
                    mustRefuse = setOf(1L, 2L),
                ),
            )
            add(
                NestedCell(
                    name = "list whose element carries NO field id (positional fallback)",
                    column = container(2, "l", ColType.LIST, scalarChild(3, "element", ColType.INT)),
                    // The identity check must not become "ids required".
                    // An id-less child keeps the positional binding —
                    // the same exemption missingFieldIds grants the
                    // repetition layer — and such a file is already
                    // flagged, so renames on its table are blocked and
                    // position cannot drift out from under it. Tighten
                    // this to "must have an id" and every element in a
                    // partially-id'd file loses its bounds.
                    //
                    // The sibling `k` is what makes the file id-BEARING
                    // (usesFieldIds walks leaves): without it the whole
                    // file would take the name-binding path and this
                    // cell would be testing something else entirely.
                    schema =
                        MessageType(
                            "root",
                            listOf(
                                optInt(1, "k"),
                                listGroup(
                                    2,
                                    "l",
                                    Types.optional(PrimitiveTypeName.INT32).named("element"),
                                ),
                            ),
                        ),
                    leaves =
                        listOf(
                            NestedLeaf(
                                listOf("l", "list", "element"),
                                Types.optional(PrimitiveTypeName.INT32).named("element"),
                                le(5),
                                le(9),
                            ),
                        ),
                    mustProduce = setOf(3L),
                    mustRefuse = setOf(2L),
                ),
            )
            add(
                NestedCell(
                    name = "list whose element carries a DIFFERENT field id",
                    column = container(1, "l", ColType.LIST, scalarChild(2, "element", ColType.INT)),
                    // The wrapper matches by id; the element does not.
                    // Bound by POSITION alone, the file's leaf 99 would
                    // have its counts and bounds recorded under catalog
                    // field 2 — a range describing other data, on a field
                    // id this file never claimed, which is what a pruner
                    // then skips files on.
                    schema = MessageType("root", listOf(listGroup(1, "l", optInt(99, "element")))),
                    leaves =
                        listOf(
                            NestedLeaf(listOf("l", "list", "element"), optInt(99, "element"), le(1000), le(1002)),
                        ),
                    mustProduce = emptySet(),
                    mustRefuse = setOf(1L, 2L, 99L),
                ),
            )
            add(
                NestedCell(
                    name = "map whose value carries an id the catalog does not know",
                    column =
                        container(
                            1,
                            "m",
                            ColType.MAP,
                            scalarChild(2, "key", ColType.STRING),
                            scalarChild(3, "value", ColType.LONG),
                        ),
                    schema =
                        MessageType(
                            "root",
                            listOf(mapGroup(1, "m", reqString(2, "key"), optLong(99, "value"))),
                        ),
                    leaves =
                        listOf(
                            NestedLeaf(listOf("m", "key_value", "key"), reqString(2, "key"), bytes(0x61), bytes(0x7A)),
                            NestedLeaf(listOf("m", "key_value", "value"), optLong(99, "value"), le(500L), le(501L)),
                        ),
                    // The KEY matches and the VALUE does not, and the
                    // whole entry is refused rather than half-recorded:
                    // a map whose members disagree with the catalog is
                    // not a map the catalog can describe.
                    mustProduce = emptySet(),
                    mustRefuse = setOf(1L, 2L, 3L, 99L),
                ),
            )
            add(
                NestedCell(
                    name = "map whose key is OPTIONAL",
                    column =
                        container(
                            1,
                            "m",
                            ColType.MAP,
                            scalarChild(2, "key", ColType.STRING),
                            scalarChild(3, "value", ColType.LONG),
                        ),
                    // The rewriter refuses this file outright (the output
                    // key is REQUIRED, so a row with none would fail the
                    // write mid-group). The reader used to accept it, so
                    // the two surfaces disagreed about which files they
                    // handle — the drift maxUnsignedParquetWidth is
                    // shared to prevent. A file compaction will never
                    // rewrite should not accumulate stats as though it
                    // will.
                    schema =
                        MessageType(
                            "root",
                            listOf(
                                mapGroup(
                                    1,
                                    "m",
                                    Types.optional(PrimitiveTypeName.BINARY)
                                        .`as`(LogicalTypeAnnotation.stringType()).id(2).named("key"),
                                    optLong(3, "value"),
                                ),
                            ),
                        ),
                    leaves =
                        listOf(
                            NestedLeaf(
                                listOf("m", "key_value", "key"),
                                Types.optional(PrimitiveTypeName.BINARY)
                                    .`as`(LogicalTypeAnnotation.stringType()).id(2).named("key"),
                                bytes(0x61),
                                bytes(0x7A),
                            ),
                        ),
                    mustProduce = emptySet(),
                    mustRefuse = setOf(1L, 2L, 3L),
                ),
            )
            add(
                NestedCell(
                    name = "struct declared over a LIST-annotated group with its field id",
                    column =
                        container(1, "s", ColType.STRUCT, scalarChild(2, "a", ColType.INT)),
                    // The reader's half of the rewriter's refusal. A
                    // container wearing a struct's id is a type mismatch,
                    // and saying so beats every child quietly missing.
                    schema = MessageType("root", listOf(listGroup(1, "s", optInt(2, "element")))),
                    leaves =
                        listOf(
                            NestedLeaf(listOf("s", "list", "element"), optInt(2, "element"), le(1), le(2)),
                        ),
                    mustProduce = emptySet(),
                    mustRefuse = setOf(1L, 2L),
                ),
            )
            add(
                NestedCell(
                    name = "struct over a REPEATED plain group",
                    column =
                        container(1, "s", ColType.STRUCT, scalarChild(2, "a", ColType.INT)),
                    // "many per row" where the catalog says "one". This
                    // side counted every repetition as a value (vc=6 for
                    // 3 rows) while the rewriter copied repetition 0 and
                    // dropped the rest — two wrong answers to one file.
                    schema =
                        MessageType(
                            "root",
                            listOf(Types.repeatedGroup().addField(optInt(2, "a")).id(1).named("s")),
                        ),
                    leaves = listOf(NestedLeaf(listOf("s", "a"), optInt(2, "a"), le(1), le(2))),
                    mustProduce = emptySet(),
                    mustRefuse = setOf(1L, 2L),
                ),
            )
            add(
                NestedCell(
                    name = "scalar over a REPEATED primitive",
                    column = scalarChild(1, "x", ColType.INT),
                    schema =
                        MessageType(
                            "root",
                            listOf(Types.repeated(PrimitiveTypeName.INT32).id(1).named("x")),
                        ),
                    leaves =
                        listOf(
                            NestedLeaf(
                                listOf("x"),
                                Types.repeated(PrimitiveTypeName.INT32).id(1).named("x"),
                                le(1),
                                le(9),
                            ),
                        ),
                    mustProduce = emptySet(),
                    mustRefuse = setOf(1L),
                ),
            )
            add(
                NestedCell(
                    name = "struct over a MAP-annotated group",
                    // The reader's half of the rewriter's refusal, for
                    // MAP as well as LIST — the LIST cell alone left the
                    // map door untested.
                    column = container(1, "s", ColType.STRUCT, scalarChild(2, "a", ColType.INT)),
                    schema =
                        MessageType(
                            "root",
                            listOf(mapGroup(1, "s", reqString(2, "key"), optLong(3, "value"))),
                        ),
                    leaves =
                        listOf(
                            NestedLeaf(listOf("s", "key_value", "key"), reqString(2, "key"), bytes(0x61), bytes(0x7A)),
                        ),
                    mustProduce = emptySet(),
                    mustRefuse = setOf(1L, 2L, 3L),
                ),
            )
            add(
                NestedCell(
                    name = "map whose KEY carries a different field id",
                    // The key twin of the value cell above: an id
                    // mismatch on either member is a map the catalog
                    // cannot describe, and only one of the two was pinned.
                    column =
                        container(
                            1,
                            "m",
                            ColType.MAP,
                            scalarChild(2, "key", ColType.STRING),
                            scalarChild(3, "value", ColType.LONG),
                        ),
                    schema =
                        MessageType(
                            "root",
                            listOf(mapGroup(1, "m", reqString(98, "key"), optLong(3, "value"))),
                        ),
                    leaves =
                        listOf(
                            NestedLeaf(listOf("m", "key_value", "key"), reqString(98, "key"), bytes(0x61), bytes(0x7A)),
                            NestedLeaf(listOf("m", "key_value", "value"), optLong(3, "value"), le(1L), le(2L)),
                        ),
                    mustProduce = emptySet(),
                    mustRefuse = setOf(1L, 2L, 3L, 98L),
                ),
            )
            add(
                NestedCell(
                    name = "struct-shaped group with a NON-container annotation",
                    // The overreach control. ENUM says nothing about
                    // shape, so this must still bound — refusing it made
                    // the table uncompactable AND unbounded at once.
                    column =
                        container(1, "s", ColType.STRUCT, scalarChild(2, "a", ColType.INT)),
                    schema =
                        MessageType(
                            "root",
                            listOf(
                                Types.optionalGroup()
                                    .addField(optInt(2, "a"))
                                    .`as`(LogicalTypeAnnotation.enumType())
                                    .id(1).named("s"),
                            ),
                        ),
                    leaves = listOf(NestedLeaf(listOf("s", "a"), optInt(2, "a"), le(3), le(300))),
                    mustProduce = setOf(2L),
                    mustRefuse = setOf(1L),
                ),
            )
            add(
                NestedCell(
                    name = "list<int> whose element leaf is physically BOOLEAN",
                    column = container(1, "l", ColType.LIST, scalarChild(2, "element", ColType.INT)),
                    schema =
                        MessageType(
                            "root",
                            listOf(
                                listGroup(
                                    1,
                                    "l",
                                    Types.optional(PrimitiveTypeName.BOOLEAN).id(2).named("element"),
                                ),
                            ),
                        ),
                    leaves =
                        listOf(
                            NestedLeaf(
                                listOf("l", "list", "element"),
                                Types.optional(PrimitiveTypeName.BOOLEAN).id(2).named("element"),
                                bytes(0),
                                bytes(1),
                            ),
                        ),
                    // The element MATCHES structurally, so its counts are
                    // honest; only the per-arm decode refuses, leaving the
                    // bounds NULL. This is the phase-1 discipline reaching
                    // one level down, and the reason mustRefuse is checked
                    // separately from "no row at all".
                    mustProduce = emptySet(),
                    mustRefuse = setOf(1L),
                ),
            )
        }
    }

    @Test
    fun `nested shapes produce leaf bounds and refuse container and shape-mismatched ones`() {
        val failures = mutableListOf<String>()
        for (cell in nestedCells) {
            val aggs =
                FooterStats.aggregate(
                    nestedFooter(cell),
                    listOf(cell.column),
                    "s3://qe/nested.parquet",
                )
            val byField = aggs.associateBy { it.fieldId }
            for (id in cell.mustProduce) {
                val agg = byField[id]
                when {
                    agg == null -> failures += "${cell.name}: field $id produced no stats row"
                    agg.lowerBound == null || agg.upperBound == null ->
                        failures += "${cell.name}: field $id produced a stats row with NULL bounds"
                }
            }
            for (id in cell.mustRefuse) {
                if (byField.containsKey(id)) {
                    failures += "${cell.name}: field $id produced a stats row and must not have"
                }
            }
            // Nothing outside the declared tree may appear at all.
            val declared = declaredFieldIds(cell.column)
            for (agg in aggs) {
                if (agg.fieldId !in declared) {
                    failures += "${cell.name}: stats row for field ${agg.fieldId}, which is not in the column tree"
                }
            }
        }
        assertThat(failures).describedAs("nested decode-matrix violations").isEmpty()
        // Non-vacuity, both directions: the cells above must actually
        // exercise production AND refusal.
        assertThat(nestedCells.flatMap { it.mustProduce }).isNotEmpty()
        assertThat(nestedCells.flatMap { it.mustRefuse }).isNotEmpty()
    }

    @Test
    fun `a file declaring one field id twice produces NO stats at all`() {
        // Every lookup elects the FIRST match, so one id on two fields
        // had whichever came first silently elected — measured, bounds
        // from `first` recorded for a column the file also declares as
        // `second`. There is no rule saying which is right, so there is
        // no binding to make: the file keeps whatever stats it already
        // had rather than gaining something invented.
        val cell =
            NestedCell(
                name = "duplicate ids",
                column = scalarChild(1, "x", ColType.INT),
                schema =
                    MessageType(
                        "root",
                        listOf(optInt(1, "first"), optInt(1, "second")),
                    ),
                leaves =
                    listOf(
                        NestedLeaf(listOf("first"), optInt(1, "first"), le(0), le(2)),
                        NestedLeaf(listOf("second"), optInt(1, "second"), le(1000), le(1002)),
                    ),
                mustProduce = emptySet(),
                mustRefuse = setOf(1L),
            )
        assertThat(FooterStats.aggregate(nestedFooter(cell), listOf(cell.column), "s3://qe/dup.parquet"))
            .isEmpty()

        // Deep, too: a duplicate inside a struct is the same hazard, and
        // a sweep that only looked at the top level would miss it.
        val deep =
            NestedCell(
                name = "duplicate ids inside a struct",
                column = container(1, "s", ColType.STRUCT, scalarChild(2, "a", ColType.INT)),
                schema =
                    MessageType(
                        "root",
                        listOf(
                            optInt(2, "top"),
                            Types.optionalGroup().addField(optInt(2, "a")).id(1).named("s"),
                        ),
                    ),
                leaves = listOf(NestedLeaf(listOf("s", "a"), optInt(2, "a"), le(1), le(2))),
                mustProduce = emptySet(),
                mustRefuse = setOf(1L, 2L),
            )
        assertThat(FooterStats.aggregate(nestedFooter(deep), listOf(deep.column), "s3://qe/dupdeep.parquet"))
            .isEmpty()
    }

    @Test
    fun `a container CATALOG row with the wrong child count degrades, never throws`() {
        // A corrupt or hand-edited catalog: a map column with three
        // children indexed past the end and threw IndexOutOfBounds
        // straight out of the hydrator sweep, which fails the file AND
        // burns a retry every pass.
        val cell =
            NestedCell(
                name = "3-child map",
                column =
                    container(
                        1,
                        "m",
                        ColType.MAP,
                        scalarChild(2, "key", ColType.STRING),
                        scalarChild(3, "value", ColType.LONG),
                        scalarChild(4, "extra", ColType.INT),
                    ),
                schema =
                    MessageType("root", listOf(mapGroup(1, "m", reqString(2, "key"), optLong(3, "value")))),
                leaves =
                    listOf(
                        NestedLeaf(listOf("m", "key_value", "key"), reqString(2, "key"), bytes(0x61), bytes(0x7A)),
                    ),
                mustProduce = emptySet(),
                mustRefuse = setOf(1L, 2L, 3L, 4L),
            )
        assertThat(FooterStats.aggregate(nestedFooter(cell), listOf(cell.column), "s3://qe/arity.parquet"))
            .isEmpty()

        val listCell =
            NestedCell(
                name = "childless list",
                column = container(1, "l", ColType.LIST),
                schema = MessageType("root", listOf(listGroup(1, "l", optInt(2, "element")))),
                leaves =
                    listOf(NestedLeaf(listOf("l", "list", "element"), optInt(2, "element"), le(1), le(2))),
                mustProduce = emptySet(),
                mustRefuse = setOf(1L, 2L),
            )
        assertThat(
            FooterStats.aggregate(nestedFooter(listCell), listOf(listCell.column), "s3://qe/arity2.parquet"),
        ).isEmpty()
    }

    @Test
    fun `two structs with a same-named leaf do not pool their chunks`() {
        // Leaves are matched on the FULL chunk path. A name-only match
        // sums `a.id` and `b.id` into one row whose bounds describe
        // neither column — and both still look perfectly well-formed.
        val columns =
            listOf(
                container(1, "a", ColType.STRUCT, scalarChild(2, "id", ColType.INT)),
                container(3, "b", ColType.STRUCT, scalarChild(4, "id", ColType.INT)),
            )
        val schema =
            MessageType(
                "root",
                listOf(
                    Types.optionalGroup().addField(optInt(2, "id")).id(1).named("a"),
                    Types.optionalGroup().addField(optInt(4, "id")).id(3).named("b"),
                ),
            )
        val cell =
            NestedCell(
                name = "two structs, one leaf name",
                column = columns[0],
                schema = schema,
                leaves =
                    listOf(
                        NestedLeaf(listOf("a", "id"), optInt(2, "id"), le(1), le(2)),
                        NestedLeaf(listOf("b", "id"), optInt(4, "id"), le(100), le(200)),
                    ),
                mustProduce = emptySet(),
                mustRefuse = emptySet(),
            )
        val aggs = FooterStats.aggregate(nestedFooter(cell), columns, "s3://qe/two.parquet")
        val byField = aggs.associateBy { it.fieldId }
        assertThat(byField.keys).containsExactlyInAnyOrder(2L, 4L)
        assertThat(byField.getValue(2L).upperBound).isEqualTo(IcebergSingleValue.encodeInt(2))
        assertThat(byField.getValue(4L).lowerBound).isEqualTo(IcebergSingleValue.encodeInt(100))
    }

    // ---- the field-id contract over nested schemas -------------------------

    /**
     * A foreign file with ids on every LEAF but none on the struct group
     * holding them. Iceberg puts an id on the struct too; a writer that
     * does not has produced a file whose struct binds by NAME.
     */
    private fun leafIdsOnlySchema(structName: String): MessageType =
        MessageType(
            "foreign",
            listOf(
                optInt(1, "k"),
                Types.optionalGroup()
                    .addFields(optInt(4, "a"), reqString(5, "b"))
                    // deliberately NO .id(...) on the group
                    .named(structName),
            ),
        )

    @Test
    fun `a container group without a field id is flagged, so the rename guard fires`() {
        // THE data-loss scenario, at its first link. Unflagged, the
        // rename guard does not fire; after `rename_column addr ->
        // location` the compaction rewriter can match the subtree
        // neither by id (the group has none) nor by name (it changed),
        // null-fills it, and end-snapshots the input — which expiry then
        // deletes. Silent, permanent, uncounted.
        assertThat(FooterStats.missingFieldIds(leafIdsOnlySchema("addr")))
            .describedAs("a struct group with no field id binds by NAME and must be flagged")
            .isTrue()
        // The same file still USES field ids for its leaves, so stats
        // still bind by id — the two questions are different, and only
        // one of them is about renames.
        assertThat(FooterStats.usesFieldIds(leafIdsOnlySchema("addr"))).isTrue()
    }

    @Test
    fun `a fully id-bearing nested schema is not flagged`() {
        // The control. Every binding node — leaves AND container
        // wrappers — carries an id, which is what pyarrow and hoglake's
        // own writer both emit.
        val schema =
            MessageType(
                "ok",
                listOf(
                    optInt(1, "k"),
                    Types.optionalGroup()
                        .addFields(optInt(4, "a"), reqString(5, "b"))
                        .id(3).named("addr"),
                    listGroup(6, "tags", reqString(7, "element")),
                    mapGroup(8, "props", reqString(9, "key"), optLong(10, "value")),
                ),
            )
        assertThat(FooterStats.missingFieldIds(schema)).isFalse()
    }

    @Test
    fun `the synthetic repetition groups are exempt, or every rewrite would self-flag`() {
        // THE trap. parquet inserts `repeated group list` inside a LIST
        // and `repeated group key_value` inside a MAP; neither is a
        // column, Iceberg has no id to match against one, and neither
        // pyarrow nor hoglake's own compaction output writes one.
        // Flagging them would make every rewrite produce a file that
        // instantly fails its own contract check and blocks renames on
        // its own table forever.
        val listSchema = MessageType("l", listOf(listGroup(1, "tags", reqString(2, "element"))))
        val mapSchema =
            MessageType("m", listOf(mapGroup(1, "props", reqString(2, "key"), optLong(3, "value"))))
        for (schema in listOf(listSchema, mapSchema)) {
            assertThat(schema.getFields()[0].asGroupType().getType(0).id)
                .describedAs("the fixture really does omit the repetition layer's id")
                .isNull()
            assertThat(FooterStats.missingFieldIds(schema))
                .describedAs("%s", schema.getFields()[0].name)
                .isFalse()
        }
    }

    @Test
    fun `the exemption is by SHAPE, so a foreign-named repetition layer is still exempt`() {
        // The name-regression fence. `list` and `key_value` are parquet's
        // CONVENTIONS, not its rules — the spec says the repetition
        // layer's name is insignificant, and writers in the wild use
        // `bag`, `array`, `map`, `entries`. An exemption that matched
        // those two literals would pass every other test in this file
        // (they all use the canonical names) and then flag every
        // foreign-written nested file as id-less, blocking renames on
        // its table for a reason that is not true.
        //
        // Every binding node below HAS its id; only the repetition
        // layer's name is exotic. The answer must be false.
        val exoticMap =
            MessageType(
                "m",
                listOf(
                    Types.optionalGroup()
                        .addField(
                            Types.repeatedGroup()
                                .addFields(reqString(2, "k"), optLong(3, "v"))
                                .named("zzz_entries"),
                        )
                        .`as`(LogicalTypeAnnotation.mapType())
                        .id(1).named("m"),
                ),
            )
        assertThat(FooterStats.missingFieldIds(exoticMap))
            .describedAs("a MAP repetition layer named 'zzz_entries' is exempt by shape")
            .isFalse()

        val exoticList =
            MessageType(
                "l",
                listOf(
                    Types.optionalGroup()
                        .addField(Types.repeatedGroup().addField(optInt(2, "el")).named("bag"))
                        .`as`(LogicalTypeAnnotation.listType())
                        .id(1).named("l"),
                ),
            )
        assertThat(FooterStats.missingFieldIds(exoticList))
            .describedAs("a LIST repetition layer named 'bag' is exempt by shape")
            .isFalse()

        // ...and the exemption does not become a blanket one: the same
        // exotic shapes with an id-less ELEMENT are still flagged, so
        // "by shape" is not "by wishful thinking".
        val exoticListIdlessElement =
            MessageType(
                "l",
                listOf(
                    Types.optionalGroup()
                        .addField(
                            Types.repeatedGroup()
                                .addField(Types.optional(PrimitiveTypeName.INT32).named("el"))
                                .named("bag"),
                        )
                        .`as`(LogicalTypeAnnotation.listType())
                        .id(1).named("l"),
                ),
            )
        assertThat(FooterStats.missingFieldIds(exoticListIdlessElement)).isTrue()
    }

    @Test
    fun `a LIST wrapper without its own field id is still flagged`() {
        // The wrapper binds; only the repetition layer under it is
        // exempt. An exemption written per-annotation rather than
        // per-shape would have swallowed this one too.
        val schema =
            MessageType(
                "l",
                listOf(
                    // .named without .id: the wrapper carries no field id.
                    Types.optionalGroup()
                        .addField(
                            Types.repeatedGroup().addField(reqString(2, "element")).named("list"),
                        )
                        .`as`(LogicalTypeAnnotation.listType())
                        .named("tags"),
                ),
            )
        assertThat(FooterStats.missingFieldIds(schema)).isTrue()
    }

    @Test
    fun `a LIST wrapper holding a non-repeated group exempts nothing`() {
        // The exemption's SHAPE clause, isolated. `repeated` is what
        // makes a group the repetition layer; a LIST-annotated wrapper
        // whose single child is an OPTIONAL group is a foreign shape
        // (FooterStats.matchInto refuses it for stats too), and that
        // inner group binds like any other — id or flag. Dropping the
        // repetition clause would exempt it and let an id-less group
        // through under a LIST annotation.
        val schema =
            MessageType(
                "l",
                listOf(
                    Types.optionalGroup()
                        .addField(
                            // OPTIONAL, not repeated — and no id.
                            Types.optionalGroup().addField(reqString(3, "element")).named("inner"),
                        )
                        .`as`(LogicalTypeAnnotation.listType())
                        .id(1).named("tags"),
                ),
            )
        assertThat(FooterStats.missingFieldIds(schema))
            .describedAs("an id-less non-repeated group under a LIST wrapper still binds")
            .isTrue()
    }

    @Test
    fun `an unmatched CONTAINER logs at warn, an unmatched scalar does not`() {
        // The level is the finding. A scalar the file predates is
        // ordinary schema evolution and belongs at debug; a container
        // that cannot be matched silently drops every leaf beneath it —
        // the same unmatchability that made the compaction rewriter
        // null-fill a whole subtree — and has to be findable in a log.
        val events = CopyOnWriteArrayList<ILoggingEvent>()
        val appender =
            object : AppenderBase<ILoggingEvent>() {
                override fun append(event: ILoggingEvent) {
                    events += event
                }
            }
        val ctx = LoggerFactory.getILoggerFactory() as LoggerContext
        appender.context = ctx
        appender.start()
        val logger = LoggerFactory.getLogger(FooterStats::class.java.name) as Logger
        logger.addAppender(appender)
        try {
            // A file holding only `k`; the catalog also knows a struct
            // and a scalar the file has never seen.
            val cell =
                NestedCell(
                    name = "absent",
                    column = scalarChild(1, "k", ColType.INT),
                    schema = MessageType("root", listOf(optInt(1, "k"))),
                    leaves = listOf(NestedLeaf(listOf("k"), optInt(1, "k"), le(1), le(2))),
                    mustProduce = emptySet(),
                    mustRefuse = emptySet(),
                )
            FooterStats.aggregate(
                nestedFooter(cell),
                listOf(
                    cell.column,
                    scalarChild(7, "added_later", ColType.STRING),
                    container(8, "addr", ColType.STRUCT, scalarChild(9, "city", ColType.STRING)),
                ),
                "s3://qe/absent.parquet",
            )
            val warns = events.filter { it.level == Level.WARN }.map { it.formattedMessage }
            // Anchored on the FACTS an operator greps for — the file and
            // the column — not on the wording. A prose edit that left the
            // guard intact used to kill this test, which trains people
            // to loosen the assertion rather than read it.
            assertThat(warns)
                .describedAs("the missing container names its file and column")
                .anySatisfy({ m ->
                    assertThat(m).contains("s3://qe/absent.parquet")
                    assertThat(m).contains("addr")
                    assertThat(m).contains("8") // its field id
                })
            assertThat(warns)
                .describedAs("the missing scalar is not loud")
                .noneSatisfy({ m -> assertThat(m).contains("added_later") })
        } finally {
            logger.detachAppender(appender)
            appender.stop()
        }
    }

    @Test
    fun `a struct over a LIST-annotated group SAYS SO rather than going quiet`() {
        // The reader's half of the rewriter's refusal, and the only
        // thing that half can be asserted on. findField would never have
        // matched the struct's children inside a LIST wrapper anyway —
        // its one child is the unnamed, id-less repetition layer — so
        // the outcome was already "no stats". The guard's whole value is
        // that the outcome stops being SILENT: the rewriter refuses this
        // file outright, and an operator whose table quietly lost its
        // struct bounds needs the two surfaces saying the same thing.
        val events = CopyOnWriteArrayList<ILoggingEvent>()
        val appender =
            object : AppenderBase<ILoggingEvent>() {
                override fun append(event: ILoggingEvent) {
                    events += event
                }
            }
        val ctx = LoggerFactory.getILoggerFactory() as LoggerContext
        appender.context = ctx
        appender.start()
        val logger = LoggerFactory.getLogger(FooterStats::class.java.name) as Logger
        logger.addAppender(appender)
        try {
            val cell =
                NestedCell(
                    name = "struct over a list",
                    column = container(1, "s", ColType.STRUCT, scalarChild(2, "a", ColType.INT)),
                    schema = MessageType("root", listOf(listGroup(1, "s", optInt(2, "element")))),
                    leaves =
                        listOf(
                            NestedLeaf(listOf("s", "list", "element"), optInt(2, "element"), le(1), le(2)),
                        ),
                    mustProduce = emptySet(),
                    mustRefuse = setOf(1L, 2L),
                )
            val aggs = FooterStats.aggregate(nestedFooter(cell), listOf(cell.column), "s3://qe/sol.parquet")
            assertThat(aggs).describedAs("no stats, with or without the guard").isEmpty()
            assertThat(events.filter { it.level == Level.WARN }.map { it.formattedMessage })
                .describedAs("...but the file, the column and the reason are in the log")
                .anySatisfy({ m ->
                    assertThat(m).contains("s3://qe/sol.parquet")
                    assertThat(m).contains("field 1")
                    assertThat(m).contains("not a struct")
                })
        } finally {
            logger.detachAppender(appender)
            appender.stop()
        }
    }

    @Test
    fun `a legacy 2-level list's repeated element is NOT exempt`() {
        // In the 2-level encoding the repeated node IS the element — a
        // real column that needs its id. The exemption is by SHAPE (a
        // repeated GROUP under the wrapper), never by "it sits under a
        // LIST annotation".
        val schema =
            MessageType(
                "l",
                listOf(
                    Types.optionalGroup()
                        .addField(Types.repeated(PrimitiveTypeName.INT32).named("element"))
                        .`as`(LogicalTypeAnnotation.listType())
                        .id(1).named("tags"),
                ),
            )
        assertThat(FooterStats.missingFieldIds(schema)).isTrue()
    }

    @Test
    fun `a schema whose only columns are nested still binds by field id`() {
        // usesFieldIds decides whether the hydrator binds by id (the LIVE
        // column set) or by NAME (the set at the file's begin_snapshot).
        // A nested-only file has no top-level primitive leaf at all, so a
        // top-level-only check answers FALSE for a perfectly id-bearing
        // file and sends it down the name-binding path — where a
        // synthetic `element` or `key` matches nothing and every bound
        // disappears.
        val schema =
            MessageType(
                "root",
                listOf(
                    Types.optionalGroup()
                        .addField(
                            Types.repeatedGroup()
                                .addField(optInt(2, "element"))
                                .named("list"),
                        )
                        .`as`(LogicalTypeAnnotation.listType())
                        .id(1).named("l"),
                ),
            )
        assertThat(FooterStats.usesFieldIds(schema)).isTrue()
        assertThat(FooterStats.missingFieldIds(schema))
            .describedAs("the synthetic repetition group carries no id, and that is not a gap")
            .isFalse()
    }

    private fun declaredFieldIds(col: CatalogColumn): Set<Long> =
        setOf(col.fieldId) + col.children.flatMap { declaredFieldIds(it) }

    /** Claims 1 and 2 for one produced bound pair. */
    private fun violations(
        type: ColType,
        shape: Shape,
        min: ByteArray,
        max: ByteArray,
        lower: ByteArray,
        upper: ByteArray,
    ): List<String> {
        val want = expectedWidth(type)
        if (want != null && (lower.size != want || upper.size != want)) {
            return listOf(
                "${type.wire} on ${shape.name}: bound widths ${lower.size}/${upper.size}, " +
                    "expected $want for mapped ${type.icebergType.wire}",
            )
        }

        // String/json/binary/uuid bounds are compared as STORED BYTES,
        // unsigned — that is what Iceberg does, and it also sidesteps a decode
        // artifact: invalid UTF-8 decodes to U+FFFD replacements, so comparing
        // the decoded Strings would report an inversion that the stored bytes
        // do not have. Everything else compares as its decoded value, since
        // signed little-endian ints do not sort bytewise.
        val byteOrdered =
            type.icebergType in setOf(IcebergType.STRING, IcebergType.BINARY, IcebergType.UUID)
        val inverted =
            if (byteOrdered) {
                java.util.Arrays.compareUnsigned(lower, upper) > 0
            } else {
                val lo =
                    try {
                        IcebergSingleValue.decode(type, lower)
                    } catch (e: IllegalArgumentException) {
                        return listOf("${type.wire} on ${shape.name}: stored bound will not decode: ${e.message}")
                    }
                val hi = IcebergSingleValue.decode(type, upper)
                // NaN is unordered; FooterStats already refuses NaN bounds, so
                // anything reaching here must compare.
                IcebergSingleValue.compareValues(type, lo, hi) > 0
            }
        if (inverted) {
            return listOf(
                "${type.wire} on ${shape.name}: INVERTED range " +
                    "lower=${lower.toHex()} upper=${upper.toHex()} " +
                    "(raw min=${min.toHex()} max=${max.toHex()})",
            )
        }
        return emptyList()
    }

    private fun ByteArray.toHex(): String = joinToString("") { "%02x".format(it) }
}
