package com.posthog.hoglake.hydrator

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
import java.nio.ByteBuffer
import java.nio.ByteOrder
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
 *     (ColType.icebergType, iceberg-federation.md §2) — a 4-byte bound
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

        for (type in ColType.entries) {
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
                ColType.entries.flatMap { t -> shapes.map { t to it.name } }.toSet(),
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
