package com.posthog.hoglake.hydrator

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
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

    /**
     * Statistics with the given raw min/max bytes, null count (null =
     * unknown) and NaN count (null = unset, which is what every footer
     * written before parquet-format added the field says and what
     * `Statistics.Builder` defaults to).
     */
    private fun stats(
        type: PrimitiveType,
        min: ByteArray?,
        max: ByteArray?,
        nulls: Long? = 0L,
        nans: Long? = null,
    ): Statistics<*> {
        val b = Statistics.getBuilderForReading(type)
        if (min != null && max != null) {
            b.withMin(min)
            b.withMax(max)
        }
        if (nulls != null) b.withNumNulls(nulls)
        if (nans != null) b.withNanCount(nans)
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
    fun `an ALL-NULL row group contributes nothing and leaves the valued chunks' bounds alone`() {
        // THE SPARSE-COLUMN BUG. parquet-java reports
        // `hasNonNullValue == false` for two different chunks: one whose
        // min/max it refused, and one that simply holds no non-null
        // value. Treating them alike nulled a whole column's bounds the
        // first time ANY row group was all-null — and a compaction
        // output is many 128 MiB row groups, so for a sparse column that
        // was every output, every time. An all-null chunk has no values,
        // so it has nothing to say about the range: skip it.
        val a = leaf("a", PrimitiveType.PrimitiveTypeName.INT64)
        val m =
            meta(
                schema(a),
                10,
                // All ten values null: numNulls == the chunk's valueCount.
                listOf(chunk(a, 10, stats(a, null, null, nulls = 10), compressedSize = 40)),
                listOf(chunk(a, 10, stats(a, le(5L), le(9L), nulls = 2), compressedSize = 60)),
            )
        with(agg(m, CatalogColumn(1, "a", ColType.LONG, null))[1L]!!) {
            assertThat(valueCount).describedAs("both chunks' values are counted").isEqualTo(20)
            assertThat(nullCount).describedAs("both chunks' nulls are counted").isEqualTo(12)
            assertThat(sizeBytes).isEqualTo(100)
            assertThat(lowerBound)
                .describedAs("the valued chunk's minimum, not NULL")
                .isEqualTo(le(5L))
            assertThat(upperBound).isEqualTo(le(9L))
        }
    }

    @Test
    fun `a column whose EVERY chunk is all-null has no bounds, because it has no values`() {
        val a = leaf("a", PrimitiveType.PrimitiveTypeName.INT64)
        val m =
            meta(
                schema(a),
                10,
                listOf(chunk(a, 10, stats(a, null, null, nulls = 10))),
                listOf(chunk(a, 5, stats(a, null, null, nulls = 5))),
            )
        with(agg(m, CatalogColumn(1, "a", ColType.LONG, null))[1L]!!) {
            assertThat(valueCount).isEqualTo(15)
            assertThat(nullCount).isEqualTo(15)
            assertThat(lowerBound).describedAs("no value, so no bound — not a guess").isNull()
            assertThat(upperBound).isNull()
        }
    }

    @Test
    fun `one chunk without min-max poisons bounds for the whole file`() {
        // The OTHER side of the all-null rule above: this chunk's ten
        // values are all non-null (null_count 0) and it still states no
        // min/max, so it holds values this footer does not bound — and a
        // bound that covers only part of a file would make a pruner drop
        // rows that are there.
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
    fun `missing null count on a nullable leaf drops the whole stats row`() {
        val a = leaf("a", PrimitiveType.PrimitiveTypeName.INT64)
        val m = meta(schema(a), 10, listOf(chunk(a, 10, stats(a, le(1L), le(2L), nulls = null))))
        assertThat(agg(m, CatalogColumn(1, "a", ColType.LONG, null))).isEmpty()
    }

    // ---- writers that omit null_count (ClickHouse) ------------------------
    //
    // ClickHouse writes non-Nullable columns REQUIRED and omits their
    // null_count from the chunk statistics, so parquet-java hands back
    // min/max with isNumNullsSet false. A leaf whose max definition level
    // is 0 cannot hold a null, so its count is 0 whatever the writer
    // recorded; a leaf that CAN hold one (optional itself, or under an
    // optional or repeated ancestor) keeps the old refusal.

    private fun requiredLeaf(
        name: String,
        physical: PrimitiveType.PrimitiveTypeName,
        fieldId: Int,
        logical: LogicalTypeAnnotation? = null,
    ): PrimitiveType {
        var b = Types.required(physical).id(fieldId)
        if (logical != null) b = b.`as`(logical)
        return b.named(name)
    }

    /** WARN lines FooterStats logs while [block] runs. */
    private fun warningsDuring(block: () -> Unit): List<String> {
        val events = java.util.concurrent.CopyOnWriteArrayList<ch.qos.logback.classic.spi.ILoggingEvent>()
        val appender =
            object : ch.qos.logback.core.AppenderBase<ch.qos.logback.classic.spi.ILoggingEvent>() {
                override fun append(event: ch.qos.logback.classic.spi.ILoggingEvent) {
                    events += event
                }
            }
        appender.context = org.slf4j.LoggerFactory.getILoggerFactory() as ch.qos.logback.classic.LoggerContext
        appender.start()
        val logger =
            org.slf4j.LoggerFactory.getLogger(FooterStats::class.java.name) as ch.qos.logback.classic.Logger
        logger.addAppender(appender)
        try {
            block()
        } finally {
            logger.detachAppender(appender)
            appender.stop()
        }
        return events.filter { it.level == ch.qos.logback.classic.Level.WARN }.map { it.formattedMessage }
    }

    @Test
    fun `a required leaf whose writer omitted null_count counts zero nulls and keeps its bounds`() {
        val id = requiredLeaf("id", PrimitiveType.PrimitiveTypeName.INT64, 1)
        val name = requiredLeaf("name", PrimitiveType.PrimitiveTypeName.BINARY, 2, LogicalTypeAnnotation.stringType())
        // Two row groups, neither carrying a null count: the shape of a
        // ClickHouse footer.
        val m =
            meta(
                schema(id, name),
                10,
                listOf(
                    chunk(id, 10, stats(id, le(5L), le(9L), nulls = null)),
                    chunk(name, 10, stats(name, "b".toByteArray(), "k".toByteArray(), nulls = null)),
                ),
                listOf(
                    chunk(id, 10, stats(id, le(-3L), le(4L), nulls = null)),
                    chunk(name, 10, stats(name, "a".toByteArray(), "c".toByteArray(), nulls = null)),
                ),
            )
        lateinit var got: Map<Long, FooterStats.ColumnAgg>
        val warnings =
            warningsDuring {
                got =
                    agg(
                        m,
                        CatalogColumn(1, "id", ColType.LONG, null),
                        CatalogColumn(2, "name", ColType.STRING, null),
                    )
            }
        assertThat(warnings).isEmpty()
        assertThat(got).containsOnlyKeys(1L, 2L)
        with(got[1L]!!) {
            // value_count comes from the chunk, not the statistics, and
            // for a required top-level leaf is the row count.
            assertThat(valueCount).isEqualTo(20)
            assertThat(nullCount).isEqualTo(0)
            assertThat(lowerBound).isEqualTo(le(-3L))
            assertThat(upperBound).isEqualTo(le(9L))
        }
        with(got[2L]!!) {
            assertThat(valueCount).isEqualTo(20)
            assertThat(nullCount).isEqualTo(0)
            assertThat(lowerBound).isEqualTo("a".toByteArray())
            assertThat(upperBound).isEqualTo("k".toByteArray())
        }
    }

    @Test
    fun `a required leaf mixing recorded and omitted null counts sums the recorded ones`() {
        // One chunk says 0, the other says nothing; a required leaf's
        // unrecorded count is 0, so the file's is the recorded sum.
        val a = requiredLeaf("a", PrimitiveType.PrimitiveTypeName.INT64, 1)
        val m =
            meta(
                schema(a),
                10,
                listOf(chunk(a, 10, stats(a, le(1L), le(2L), nulls = 0))),
                listOf(chunk(a, 10, stats(a, le(3L), le(4L), nulls = null))),
            )
        with(agg(m, CatalogColumn(1, "a", ColType.LONG, null))[1L]!!) {
            assertThat(valueCount).isEqualTo(20)
            assertThat(nullCount).isEqualTo(0)
            assertThat(lowerBound).isEqualTo(le(1L))
            assertThat(upperBound).isEqualTo(le(4L))
        }
    }

    @Test
    fun `an optional leaf whose writer omitted null_count still gets no row, and the warning says nullable`() {
        val a = leaf("a", PrimitiveType.PrimitiveTypeName.INT64, fieldId = 1)
        val b = requiredLeaf("b", PrimitiveType.PrimitiveTypeName.INT64, 2)
        val m =
            meta(
                schema(a, b),
                10,
                listOf(
                    chunk(a, 10, stats(a, le(1L), le(2L), nulls = null)),
                    chunk(b, 10, stats(b, le(1L), le(2L), nulls = null)),
                ),
            )
        lateinit var out: Map<Long, FooterStats.ColumnAgg>
        val warnings =
            warningsDuring {
                out =
                    agg(
                        m,
                        CatalogColumn(1, "a", ColType.LONG, null),
                        CatalogColumn(2, "b", ColType.LONG, null),
                    )
            }
        // The optional column is refused; the required one beside it is not.
        assertThat(out).containsOnlyKeys(2L)
        assertThat(warnings).singleElement().asString()
            .contains("column a (field 1)")
            .contains("is nullable (max definition level 1)")
            .contains("writer omitted null_count")
            .contains("skipping its stats row")
    }

    @Test
    fun `a required leaf under an OPTIONAL struct gets no row, since the struct itself can be null`() {
        // root { g: optional group { x: required int64 } } -- x's max
        // definition level is 1, and a null g makes x null.
        val x = requiredLeaf("x", PrimitiveType.PrimitiveTypeName.INT64, 11)
        val g = Types.optionalGroup().addField(x).id(10).named("g")
        val m = meta(schema(g), 10, listOf(nestedChunk(x, "g", 10, stats(x, le(1L), le(2L), nulls = null))))
        val struct =
            CatalogColumn(10, "g", ColType.STRUCT, null, children = listOf(CatalogColumn(11, "x", ColType.LONG, null)))
        lateinit var out: Map<Long, FooterStats.ColumnAgg>
        val warnings = warningsDuring { out = agg(m, struct) }
        assertThat(out).isEmpty()
        assertThat(warnings).singleElement().asString().contains("column x (field 11)").contains("is nullable")
    }

    @Test
    fun `a required leaf under a REQUIRED struct counts zero nulls, since nothing on its path can be null`() {
        // The contrast to the optional-struct case: every level required,
        // max definition level 0.
        val x = requiredLeaf("x", PrimitiveType.PrimitiveTypeName.INT64, 11)
        val g = Types.requiredGroup().addField(x).id(10).named("g")
        val m = meta(schema(g), 10, listOf(nestedChunk(x, "g", 10, stats(x, le(1L), le(2L), nulls = null))))
        val struct =
            CatalogColumn(10, "g", ColType.STRUCT, null, children = listOf(CatalogColumn(11, "x", ColType.LONG, null)))
        with(agg(m, struct)[11L]!!) {
            assertThat(valueCount).isEqualTo(10)
            assertThat(nullCount).isEqualTo(0)
            assertThat(lowerBound).isEqualTo(le(1L))
            assertThat(upperBound).isEqualTo(le(2L))
        }
    }

    @Test
    fun `a required leaf with no statistics at all keeps its counts, without bounds`() {
        // Nothing but the chunk's value count: the null count is still
        // provably 0, and bounds stay NULL as always.
        val a = requiredLeaf("a", PrimitiveType.PrimitiveTypeName.INT64, 1)
        val m = meta(schema(a), 10, listOf(chunk(a, 10, stats(a, null, null, nulls = null))))
        with(agg(m, CatalogColumn(1, "a", ColType.LONG, null))[1L]!!) {
            assertThat(valueCount).isEqualTo(10)
            assertThat(nullCount).isEqualTo(0)
            assertThat(lowerBound).isNull()
            assertThat(upperBound).isNull()
        }
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
    fun `a decimal column with NO declared scale reads a scale-0 leaf`() {
        // `type_params` carrying only a precision is ordinary — it is
        // pyhoglake's shape for an integral decimal, it is what
        // BoundWire.scaleOf reads as 0, and it is what ParquetRewriter
        // STAMPS as `decimalType(0, precision)` on a compaction output's
        // leaf. Reading a null catalog scale as "unknown" dropped the
        // bounds of every such column on both this path and
        // compaction's, for a scale the file and the catalog agreed on.
        val d =
            leaf(
                "d",
                PrimitiveType.PrimitiveTypeName.BINARY,
                logical = LogicalTypeAnnotation.decimalType(0, 10),
            )
        val m = meta(schema(d), 10, listOf(chunk(d, 10, stats(d, byteArrayOf(-3), byteArrayOf(11)))))
        val out = agg(m, CatalogColumn(1, "d", ColType.DECIMAL, decimalScale = null))
        assertThat(out[1L]!!.lowerBound).isEqualTo(byteArrayOf(-3))
        assertThat(out[1L]!!.upperBound).isEqualTo(byteArrayOf(11))
    }

    @Test
    fun `a decimal column with no declared scale still refuses a SCALED leaf`() {
        // The default is 0, not "whatever the file says": a leaf
        // declaring scale 2 under a column the catalog says is scale 0
        // is a real mismatch, and its bounds mean something else.
        val d =
            leaf(
                "d",
                PrimitiveType.PrimitiveTypeName.BINARY,
                logical = LogicalTypeAnnotation.decimalType(2, 10),
            )
        val m = meta(schema(d), 10, listOf(chunk(d, 10, stats(d, byteArrayOf(1), byteArrayOf(2)))))
        val out = agg(m, CatalogColumn(1, "d", ColType.DECIMAL, decimalScale = null))
        assertThat(out[1L]!!.lowerBound).isNull()
        assertThat(out[1L]!!.upperBound).isNull()
    }

    // ---- parquet's own serialization limit ---------------------------------

    @Test
    fun `a chunk whose min and max are too big for parquet to WRITE drops bounds, keeps counts`() {
        // parquet-java keeps the full min/max in the in-memory
        // ParquetMetadata but refuses to serialize a chunk's statistics
        // at all once `minBytes.length + maxBytes.length` reaches
        // ParquetMetadataConverter.MAX_STATS_SIZE (4096). The hydrator
        // reads files, so it never SEES such a pair; compaction reads
        // the writer's in-memory footer, so it did — and would have
        // stored bounds the file itself does not carry, which a later
        // rehydrate would replace with NULL.
        val s =
            leaf(
                "s",
                PrimitiveType.PrimitiveTypeName.BINARY,
                logical = LogicalTypeAnnotation.stringType(),
            )
        val lo = ByteArray(2_100) { 'a'.code.toByte() }
        val hi = ByteArray(2_100) { 'b'.code.toByte() }
        val m = meta(schema(s), 10, listOf(chunk(s, 10, stats(s, lo, hi, nulls = 1))))
        with(agg(m, CatalogColumn(1, "s", ColType.STRING, null))[1L]!!) {
            assertThat(valueCount).isEqualTo(10)
            assertThat(nullCount).isEqualTo(1)
            assertThat(lowerBound).isNull()
            assertThat(upperBound).isNull()
        }
    }

    @Test
    fun `a chunk one byte UNDER parquet's limit keeps its bounds`() {
        // 2047 + 2048 = 4095, which `Statistics.isSmallerThan(4096)`
        // accepts — so the footer will carry this pair and so does the
        // stats row. The gate is the limit, not a conservative guess.
        val s =
            leaf(
                "s",
                PrimitiveType.PrimitiveTypeName.BINARY,
                logical = LogicalTypeAnnotation.stringType(),
            )
        val lo = ByteArray(2_047) { 'a'.code.toByte() }
        val hi = ByteArray(2_048) { 'b'.code.toByte() }
        val m = meta(schema(s), 10, listOf(chunk(s, 10, stats(s, lo, hi, nulls = 0))))
        with(agg(m, CatalogColumn(1, "s", ColType.STRING, null))[1L]!!) {
            assertThat(lowerBound).isEqualTo(lo)
            assertThat(upperBound).isEqualTo(hi)
        }
    }

    // ---- nan_count ---------------------------------------------------------

    @Test
    fun `nan_count sums across chunks when every chunk carries one`() {
        // `Statistics.nan_count` is real (parquet-java 1.18.1,
        // isNanCountSet/getNanCount) and only float/double statistics
        // ever set it. This surface used to hard-code null, which cost
        // the one count Iceberg defines for floating-point columns on
        // every file either door produced.
        val d = leaf("d", PrimitiveType.PrimitiveTypeName.DOUBLE)
        val m =
            meta(
                schema(d),
                20,
                listOf(chunk(d, 10, stats(d, le(1.0), le(2.0), nulls = 0, nans = 3))),
                listOf(chunk(d, 10, stats(d, le(0.5), le(4.0), nulls = 1, nans = 2))),
            )
        with(agg(m, CatalogColumn(1, "d", ColType.DOUBLE, null))[1L]!!) {
            assertThat(nanCount).isEqualTo(5)
            assertThat(nullCount).isEqualTo(1)
            assertThat(lowerBound).isEqualTo(le(0.5))
            assertThat(upperBound).isEqualTo(le(4.0))
        }
    }

    @Test
    fun `one chunk with no nan_count drops the whole column's`() {
        // A partial sum of a count is worse than no count: it would
        // understate, and nothing downstream could tell. pyarrow and
        // ClickHouse write no nan_count at all, so this is the ordinary
        // shape for a foreign file.
        val d = leaf("d", PrimitiveType.PrimitiveTypeName.DOUBLE)
        val m =
            meta(
                schema(d),
                20,
                listOf(chunk(d, 10, stats(d, le(1.0), le(2.0), nulls = 0, nans = 3))),
                listOf(chunk(d, 10, stats(d, le(0.5), le(4.0), nulls = 0, nans = null))),
            )
        assertThat(agg(m, CatalogColumn(1, "d", ColType.DOUBLE, null))[1L]!!.nanCount).isNull()
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
    // assertions below are really assertions about docs/iceberg-federation.md
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
        // A `long` column's domain contains every unsigned value up to
        // 32 bits, and arrow and DuckDB both emit unsigned data as INT32
        // + INT(w, unsigned), so a client declaring the column `long`
        // produces exactly this pairing with no ALTER involved.
        // Sign-extending it is the ParquetRewriter hazard in its
        // read-path twin: 0xFFFFFFFF becomes -1, the upper bound lands
        // BELOW the lower one, and a pruner silently drops the file.
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
    fun `a NARROW unsigned annotation is refused by types too small to hold it`() {
        // The per-width half of the rule, which a full-width-only gate
        // gets wrong while looking fine: INT(16, unsigned) holding 65535
        // decodes to a positive int, fits four bytes, and sorts the right
        // way round — it is simply not an int8 bound. Every row here is a
        // narrow width, because the wide ones cannot tell the two rules
        // apart.
        val cases =
            listOf(
                // (catalog type, annotation width, the value that overflows it)
                Triple(ColType.INT8, 8, 255),
                Triple(ColType.INT8, 16, 65_535),
                Triple(ColType.INT16, 16, 65_535),
                Triple(ColType.UINT8, 16, 65_535),
            )
        for ((type, width, max) in cases) {
            val a =
                leaf(
                    "a",
                    PrimitiveType.PrimitiveTypeName.INT32,
                    logical = LogicalTypeAnnotation.intType(width, false),
                )
            val m = meta(schema(a), 10, listOf(chunk(a, 10, stats(a, le(0), le(max)))))
            val out = agg(m, CatalogColumn(1, "a", type, null))
            assertThat(out[1L]!!.valueCount)
                .describedAs("%s keeps its counts", type.wire)
                .isEqualTo(10)
            assertThat(out[1L]!!.lowerBound)
                .describedAs("%s must not bound an INT(%d, unsigned) leaf", type.wire, width)
                .isNull()
            assertThat(out[1L]!!.upperBound).describedAs("%s upper", type.wire).isNull()
        }
    }

    @Test
    fun `a narrow unsigned annotation IS read by types that contain it`() {
        // The control for the rule's other side: int16 holds 255, int
        // holds 65535, and refusing those would lose bounds on files
        // nothing is wrong with.
        val cases =
            listOf(
                Triple(ColType.INT16, 8, 255),
                Triple(ColType.INT, 8, 255),
                Triple(ColType.INT, 16, 65_535),
                Triple(ColType.UINT8, 8, 255),
                Triple(ColType.UINT16, 16, 65_535),
            )
        for ((type, width, max) in cases) {
            val a =
                leaf(
                    "a",
                    PrimitiveType.PrimitiveTypeName.INT32,
                    logical = LogicalTypeAnnotation.intType(width, false),
                )
            val m = meta(schema(a), 10, listOf(chunk(a, 10, stats(a, le(0), le(max)))))
            val out = agg(m, CatalogColumn(1, "a", type, null))
            assertThat(out[1L]!!.lowerBound)
                .describedAs("%s reads an INT(%d, unsigned) leaf", type.wire, width)
                .isEqualTo(le(0))
            assertThat(out[1L]!!.upperBound).describedAs("%s upper", type.wire).isEqualTo(le(max))
        }
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
    fun `uint64 bounds merge across row groups by MAGNITUDE, not as signed longs`() {
        // The cross-chunk merge is where a uint64's representation
        // earns its keep: the decode produces a BigInteger in
        // [0, 2^64) precisely so that the min/max reduction over row
        // groups compares magnitudes. Read as signed longs, everything
        // at or above 2^63 is negative, so the LOWER bound here would
        // come back as 2^63+10 — above half the file's values, which is
        // the shape a pruner reads as "these rows are not here".
        //
        // (This replaces a `compareValues`-level test; see
        // IcebergSingleValueTest for the comparator's own contract.)
        val a =
            leaf(
                "a",
                PrimitiveType.PrimitiveTypeName.INT64,
                logical = LogicalTypeAnnotation.intType(64, false),
            )
        val m =
            meta(
                schema(a),
                20,
                // 7 .. 2^63 (Long.MIN_VALUE's bit pattern)
                listOf(chunk(a, 10, stats(a, le(7L), le(Long.MIN_VALUE)))),
                // 2^63+10 .. 2^64-1
                listOf(chunk(a, 10, stats(a, le(Long.MIN_VALUE + 10), le(-1L)))),
            )
        val out = agg(m, CatalogColumn(1, "a", ColType.UINT64, null))
        assertThat(out[1L]!!.lowerBound)
            .describedAs("7, not the signed minimum 2^63+10")
            .isEqualTo(java.math.BigInteger.valueOf(7).toByteArray())
        assertThat(out[1L]!!.upperBound)
            .isEqualTo(java.math.BigInteger.ONE.shiftLeft(64).subtract(java.math.BigInteger.ONE).toByteArray())
        // The counterexample, so neither anchor can be read as a
        // tautology: both of the second chunk's bounds are negative
        // longs.
        assertThat(Long.MIN_VALUE + 10).isNegative()
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

    @Test
    fun `variant group field id governs renames and child stats are omitted`() {
        val variant =
            Types.optionalGroup().`as`(LogicalTypeAnnotation.variantType(1.toByte())).id(7)
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("metadata")
                .optional(PrimitiveType.PrimitiveTypeName.BINARY).named("value")
                .named("old_properties")
        val schema = schema(variant)
        assertThat(FooterStats.usesFieldIds(schema)).isTrue()
        assertThat(FooterStats.missingFieldIds(schema)).isFalse()
        assertThat(agg(meta(schema, 1), CatalogColumn(7, "renamed", ColType.VARIANT, null))).isEmpty()

        // DEGRADES, where #77 threw. `aggregate` is documented total and
        // every other shape disagreement in this object takes the
        // offending subtree out of the results with a warning; throwing
        // cost the whole FILE its stats — every other column included —
        // for a column that produces none either way. The observable
        // contract is the same where it matters: no stats row.
        assertThat(agg(meta(schema, 1), CatalogColumn(7, "renamed", ColType.STRING, null)))
            .describedAs("a variant group bound to a scalar column yields nothing")
            .isEmpty()
        val plain =
            Types.optionalGroup().id(7)
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("metadata")
                .optional(PrimitiveType.PrimitiveTypeName.BINARY).named("value").named("properties")
        assertThat(agg(meta(schema(plain), 1), CatalogColumn(7, "properties", ColType.VARIANT, null)))
            .describedAs("a group without the variant annotation is not a variant")
            .isEmpty()
    }

    @Test
    fun `a CONTAINER column bound to a variant group fabricates nothing`(
        @org.junit.jupiter.api.io.TempDir tmp: java.nio.file.Path,
    ) {
        // #77's aggregate prologue had a second check — "native VARIANT
        // cannot bind to scalar column" — and the merge dropped it,
        // because for a SCALAR the group-vs-primitive fallthrough covers
        // it. It does not cover a CONTAINER: isContainerAnnotation knows
        // only LIST/MAP/MAP_KEY_VALUE, so a catalog struct descended
        // into metadata/value/typed_value, and the name fallback (those
        // children carry no ids, so it applies even under useFieldIds)
        // bound a struct field literally named `value` to the variant's
        // binary payload. Measured before the fix: bounds AAA..zzz for a
        // column that has no such values.
        //
        // A REAL file with real chunk statistics: a synthesized footer
        // with no chunks produces no stats whatever the binding does, so
        // the assertion would have held with the guard deleted.
        fun fileWith(id: Int?): java.nio.file.Path {
            val b =
                Types.optionalGroup().`as`(LogicalTypeAnnotation.variantType(1.toByte()))
                    .required(PrimitiveType.PrimitiveTypeName.BINARY).named("metadata")
                    .optional(PrimitiveType.PrimitiveTypeName.BINARY).named("value")
            val schema = MessageType("m", listOf<Type>(if (id != null) b.id(id).named("p") else b.named("p")))
            val path = tmp.resolve("variant-${id ?: "noid"}.parquet")
            val factory = org.apache.parquet.example.data.simple.SimpleGroupFactory(schema)
            org.apache.parquet.hadoop.example.ExampleParquetWriter
                .builder(org.apache.parquet.io.LocalOutputFile(path))
                .withType(schema)
                .withCompressionCodec(org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED)
                .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE)
                .build()
                .use { w ->
                    for (v in listOf("AAA", "mmm", "zzz")) {
                        val g = factory.newGroup()
                        g.addGroup(0).also {
                            it.add(0, org.apache.parquet.io.api.Binary.fromString("meta"))
                            it.add(1, org.apache.parquet.io.api.Binary.fromString(v))
                        }
                        w.write(g)
                    }
                }
            return path
        }

        fun statsFor(
            path: java.nio.file.Path,
            col: CatalogColumn,
        ) = FooterStats.aggregate(
            FooterParse.parse(org.apache.parquet.io.LocalInputFile(path)),
            listOf(col),
            path.toString(),
        )

        val struct =
            CatalogColumn(
                1,
                "p",
                ColType.STRUCT,
                null,
                children = listOf(CatalogColumn(2, "value", ColType.STRING, null)),
            )
        // The sanity check the vacuous version lacked: this file really
        // does carry bounds, so an empty result means the BINDING
        // refused, not that there was nothing to find.
        assertThat(statsFor(fileWith(1), CatalogColumn(1, "p", ColType.VARIANT, null)))
            .describedAs("a variant column yields no stats either, by design")
            .isEmpty()
        assertThat(statsFor(fileWith(1), struct)).describedAs("id-bearing file").isEmpty()
        assertThat(statsFor(fileWith(null), struct)).describedAs("name-fallback file").isEmpty()
        assertThat(
            statsFor(
                fileWith(1),
                CatalogColumn(
                    1,
                    "p",
                    ColType.LIST,
                    null,
                    children = listOf(CatalogColumn(2, "element", ColType.STRING, null)),
                ),
            ),
        ).describedAs("list column").isEmpty()
    }

    @Test
    fun `an invalid variant is reported, with the fault named`(
        @org.junit.jupiter.api.io.TempDir tmp: java.nio.file.Path,
    ) {
        // The merge turned #77's throw into a degrade, which is right —
        // `aggregate` is total — but a degrade nobody can see is a
        // silent drop. #77's test asserted the THROW's message; the
        // rewrite asserted only `.isEmpty()`, which the generic
        // shape-mismatch arm satisfies, so `variantFault`'s six messages
        // became unreachable-by-test. Assert the warn.
        val events = java.util.concurrent.CopyOnWriteArrayList<ch.qos.logback.classic.spi.ILoggingEvent>()
        val appender =
            object : ch.qos.logback.core.AppenderBase<ch.qos.logback.classic.spi.ILoggingEvent>() {
                override fun append(event: ch.qos.logback.classic.spi.ILoggingEvent) {
                    events += event
                }
            }
        val ctx = org.slf4j.LoggerFactory.getILoggerFactory() as ch.qos.logback.classic.LoggerContext
        appender.context = ctx
        appender.start()
        val logger =
            org.slf4j.LoggerFactory.getLogger(FooterStats::class.java.name) as ch.qos.logback.classic.Logger
        logger.addAppender(appender)
        try {
            fun faultOf(
                label: String,
                group: Type,
            ): String {
                events.clear()
                val schema = MessageType("m", listOf(group))
                val path = tmp.resolve("$label.parquet")
                val factory = org.apache.parquet.example.data.simple.SimpleGroupFactory(schema)
                org.apache.parquet.hadoop.example.ExampleParquetWriter
                    .builder(org.apache.parquet.io.LocalOutputFile(path))
                    .withType(schema)
                    .withCompressionCodec(org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED)
                    .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE)
                    .build()
                    .use { w -> w.write(factory.newGroup()) }
                FooterStats.aggregate(
                    FooterParse.parse(org.apache.parquet.io.LocalInputFile(path)),
                    listOf(CatalogColumn(1, "p", ColType.VARIANT, null)),
                    path.toString(),
                )
                return events.filter { it.level == ch.qos.logback.classic.Level.WARN }
                    .joinToString(" | ") { it.formattedMessage }
            }

            // No variant annotation at all.
            assertThat(
                faultOf(
                    "plain",
                    Types.optionalGroup().id(1)
                        .required(PrimitiveType.PrimitiveTypeName.BINARY).named("metadata")
                        .optional(PrimitiveType.PrimitiveTypeName.BINARY).named("value").named("p"),
                ),
            ).contains("is not a native parquet VARIANT of spec version 1")

            // Annotated, but `metadata` is optional where the spec says required.
            assertThat(
                faultOf(
                    "optional-metadata",
                    Types.optionalGroup().`as`(LogicalTypeAnnotation.variantType(1.toByte())).id(1)
                        .optional(PrimitiveType.PrimitiveTypeName.BINARY).named("metadata")
                        .optional(PrimitiveType.PrimitiveTypeName.BINARY).named("value").named("p"),
                ),
            ).contains("has no REQUIRED binary 'metadata'")

            // A child the variant spec does not define.
            assertThat(
                faultOf(
                    "stray-child",
                    Types.optionalGroup().`as`(LogicalTypeAnnotation.variantType(1.toByte())).id(1)
                        .required(PrimitiveType.PrimitiveTypeName.BINARY).named("metadata")
                        .optional(PrimitiveType.PrimitiveTypeName.BINARY).named("value")
                        .optional(PrimitiveType.PrimitiveTypeName.BINARY).named("stray").named("p"),
                ),
            ).contains("outside metadata/value/typed_value")

            // Neither payload child.
            assertThat(
                faultOf(
                    "no-payload",
                    Types.optionalGroup().`as`(LogicalTypeAnnotation.variantType(1.toByte())).id(1)
                        .required(PrimitiveType.PrimitiveTypeName.BINARY).named("metadata").named("p"),
                ),
            ).contains("has neither 'value' nor 'typed_value'")

            // And a WELL-FORMED variant logs nothing: the degrade must
            // not fire on the shape it is meant to accept.
            assertThat(
                faultOf(
                    "good",
                    Types.optionalGroup().`as`(LogicalTypeAnnotation.variantType(1.toByte())).id(1)
                        .required(PrimitiveType.PrimitiveTypeName.BINARY).named("metadata")
                        .optional(PrimitiveType.PrimitiveTypeName.BINARY).named("value").named("p"),
                ),
            ).describedAs("a valid variant is silent").isEmpty()
        } finally {
            logger.detachAppender(appender)
        }
    }

    @Test
    fun `DuckDB native fixture hydrates scalar stats and preserves variant field identity`() {
        val path = java.nio.file.Path.of(javaClass.getResource("/variant/native_variant.parquet")!!.toURI())
        org.apache.parquet.hadoop.ParquetFileReader.open(org.apache.parquet.io.LocalInputFile(path)).use { reader ->
            assertThat(FooterStats.missingFieldIds(reader.footer.fileMetaData.schema)).isFalse()
            val stats =
                agg(
                    reader.footer,
                    CatalogColumn(1, "id", ColType.LONG, null),
                    CatalogColumn(2, "properties", ColType.VARIANT, null),
                )
            assertThat(stats.keys).containsExactly(1L)
        }
    }

    @Test
    fun `a nested group's own field id DOES gate the file, now that groups are columns`() {
        // #77 asserted FALSE here, and that was right in a world where a
        // parquet group was never a catalog column: an id on one meant
        // nothing, so ignoring it was free. Containers changed the
        // premise — a struct/list/map wrapper IS a catalog column with
        // its own field id — and ignoring it was the round-1 data
        // substitution: a file with ids on its wrappers and none on its
        // leaves read as id-less, so the reader produced no stats while
        // the rewriter copied it by name.
        val nested =
            Types.optionalGroup().id(9)
                .optional(PrimitiveType.PrimitiveTypeName.BINARY).named("child").named("nested")
        assertThat(FooterStats.usesFieldIds(schema(nested)))
            .describedAs("a group carrying an id is a binding node")
            .isTrue()
        // And the child that carries none still flags the contract.
        assertThat(FooterStats.missingFieldIds(schema(nested))).isTrue()
    }

    // ---- Column -> CatalogColumn -------------------------------------------
    //
    // Compaction already HOLDS the live columns as `Column` (its rewrite
    // shape is bound to them), so it converts rather than re-reading
    // them from hog_column per group. The conversion is what decides
    // which stats rows a compaction output gets, so it is tested
    // directly: a dropped recursion step would silently cost every
    // nested leaf its row, which is exactly the bug the hydrator's own
    // `columnTypes`-over-top-level-only had.

    @Test
    fun `asCatalogColumns recurses, so a leaf inside a struct is a column of its own`() {
        val cols =
            listOf(
                Column(1, 0, ColumnDef("id", ColType.LONG, nullable = false)),
                Column(
                    2,
                    1,
                    ColumnDef("addr", ColType.STRUCT),
                    listOf(
                        Column(3, 0, ColumnDef("city", ColType.STRING)),
                        Column(
                            4,
                            1,
                            ColumnDef("geo", ColType.STRUCT),
                            listOf(Column(5, 0, ColumnDef("lat", ColType.DOUBLE))),
                        ),
                    ),
                ),
            )
        val out = cols.asCatalogColumns()
        assertThat(out.map { it.fieldId to it.name }).containsExactly(1L to "id", 2L to "addr")
        assertThat(out[0].children).isEmpty()
        val addr = out[1]
        assertThat(addr.type).isEqualTo(ColType.STRUCT)
        assertThat(addr.children.map { it.fieldId to it.name }).containsExactly(3L to "city", 4L to "geo")
        // Two levels down, which is where a single non-recursive map
        // would have stopped.
        assertThat(addr.children[1].children.single())
            .isEqualTo(CatalogColumn(5, "lat", ColType.DOUBLE, null))
    }

    @Test
    fun `asCatalogColumns reads a declared decimal scale and leaves an absent one null`() {
        val out =
            listOf(
                Column(1, 0, ColumnDef("cents", ColType.DECIMAL, typeParams = mapOf("precision" to 10, "scale" to 2))),
                Column(2, 1, ColumnDef("bare", ColType.DECIMAL, typeParams = mapOf("precision" to 10))),
                Column(3, 2, ColumnDef("none", ColType.DECIMAL)),
            ).asCatalogColumns()
        assertThat(out.map { it.decimalScale })
            // Null, not 0: the absent-means-0 default lives in
            // FooterStats.decodeDecimal, which is the one place both
            // this producer and the hydrator's own pass through — see
            // the scale-0-leaf tests above.
            .containsExactly(2, null, null)
    }
}
