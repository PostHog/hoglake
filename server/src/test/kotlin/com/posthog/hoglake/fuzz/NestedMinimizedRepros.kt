package com.posthog.hoglake.fuzz

import com.posthog.hoglake.compaction.ParquetRewriter
import com.posthog.hoglake.compaction.UnconvertibleSchemaException
import com.posthog.hoglake.hydrator.CatalogColumn
import com.posthog.hoglake.hydrator.FooterParse
import com.posthog.hoglake.hydrator.FooterStats
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types
import java.nio.file.Files
import java.nio.file.Path

/**
 * Hand-minimized reproductions of every campaign finding. Each `repro*`
 * prints PASS/FAIL so the whole set replays in one run, with no fuzzing
 * and no randomness.
 */
object NestedMinimizedRepros {
    @JvmStatic
    fun main(args: Array<String>) {
        for (check in run(Files.createTempDirectory("hoglake-repro"))) {
            println(
                "${if (check.reproduced) "REPRODUCED" else "not reproduced"}  " +
                    "${check.name} :: ${check.detail}",
            )
        }
    }

    /** Every A/C finding replayed once. See [ReproCheck]. */
    fun run(tmp: Path): List<ReproCheck> {
        checks.clear()
        reproA1EmptyDecimalBinary(tmp)
        reproA1bEmptyDecimalBinarySorted(tmp)
        reproA2ContainerIdsWithoutLeafIds(tmp)
        reproA3DuplicateFieldNames(tmp)
        reproA4DecimalPrecisionMidWrite(tmp)
        reproA5NonUtf8StringBound(tmp)
        reproC1InvertedFooterStats()
        reproC2NullCountAboveValueCount()
        return checks.toList()
    }

    private val checks = mutableListOf<ReproCheck>()

    // ---- C1/C2: the hydrator trusts a writer-supplied footer's numbers ----
    //
    // Commits are FOOTER-SHIPPING: the bytes the hydrator parses came
    // from the client. FooterStats copies min/max and numNulls straight
    // through with no final sanity check, so a writer that ships an
    // inverted bound pair gets one stored — the exact shape a pruner
    // reads as "no rows here", silently dropping the file from every
    // scan (FooterStats' own KDoc names that outcome).

    private fun syntheticFooter(
        schema: MessageType,
        stats: org.apache.parquet.column.statistics.Statistics<*>,
        path: String,
        valueCount: Long,
    ): org.apache.parquet.hadoop.metadata.ParquetMetadata {
        val block = org.apache.parquet.hadoop.metadata.BlockMetaData()
        block.rowCount = valueCount
        block.addColumn(
            org.apache.parquet.hadoop.metadata.ColumnChunkMetaData.get(
                org.apache.parquet.hadoop.metadata.ColumnPath.fromDotString(path),
                schema.getType(path).asPrimitiveType(),
                org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED,
                null,
                setOf(org.apache.parquet.column.Encoding.PLAIN),
                stats,
                4L,
                4L,
                valueCount,
                64L,
                64L,
            ),
        )
        return org.apache.parquet.hadoop.metadata.ParquetMetadata(
            org.apache.parquet.hadoop.metadata.FileMetaData(schema, emptyMap(), "fuzz"),
            listOf(block),
        )
    }

    private fun reproC1InvertedFooterStats() {
        val schema =
            MessageType(
                "m",
                listOf<Type>(
                    Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                        .`as`(LogicalTypeAnnotation.dateType()).id(1).named("d"),
                ),
            )
        val st =
            org.apache.parquet.column.statistics.Statistics
                .getBuilderForReading(schema.getType("d").asPrimitiveType())
                .withMin(intLE(500))
                .withMax(intLE(100)) // MAX < MIN, as a hostile writer may ship
                .withNumNulls(0L)
                .build()
        val aggs =
            FooterStats.aggregate(
                syntheticFooter(schema, st, "d", 3L),
                listOf(CatalogColumn(1, "d", ColType.DATE, null)),
                "fuzz://c1",
            )
        val a = aggs.singleOrNull()
        val lo = a?.lowerBound?.let { com.posthog.hoglake.stats.IcebergSingleValue.decode(ColType.DATE, it) }
        val hi = a?.upperBound?.let { com.posthog.hoglake.stats.IcebergSingleValue.decode(ColType.DATE, it) }
        say(
            "C1 inverted bound pair copied through from the footer",
            a != null && lo != null && hi != null && (lo as Int) > (hi as Int),
            "stored lower=$lo upper=$hi (a pruner reads lower>upper as 'no rows here')",
        )
    }

    private fun reproC2NullCountAboveValueCount() {
        val schema =
            MessageType(
                "m",
                listOf<Type>(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).id(1).named("n")),
            )
        val st =
            org.apache.parquet.column.statistics.Statistics
                .getBuilderForReading(schema.getType("n").asPrimitiveType())
                .withNumNulls(9L) // more nulls than the chunk has values
                .build()
        val aggs =
            FooterStats.aggregate(
                syntheticFooter(schema, st, "n", 2L),
                listOf(CatalogColumn(1, "n", ColType.LONG, null)),
                "fuzz://c2",
            )
        val a = aggs.singleOrNull()
        say(
            "C2 null_count > value_count copied through from the footer",
            a != null && a.nullCount > a.valueCount,
            "stored value_count=${a?.valueCount} null_count=${a?.nullCount} " +
                "(hog_file_column_stats has no CHECK for this)",
        )
    }

    private fun intLE(v: Int): ByteArray =
        java.nio.ByteBuffer.allocate(4).order(java.nio.ByteOrder.LITTLE_ENDIAN).putInt(v).array()

    private fun say(
        name: String,
        holds: Boolean,
        detail: String,
    ) {
        checks += ReproCheck(name, holds, detail)
    }

    // ---- A1: empty BINARY under a DECIMAL column -------------------------

    private fun reproA1EmptyDecimalBinary(tmp: Path) {
        val live =
            listOf(
                Column(
                    fieldId = 1,
                    ordinal = 0,
                    def = ColumnDef("d", ColType.DECIMAL, mapOf("precision" to 10, "scale" to 2)),
                ),
            )
        val schema =
            MessageType(
                "m",
                listOf<Type>(
                    Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .`as`(LogicalTypeAnnotation.decimalType(2, 10))
                        .id(1).named("d"),
                ),
            )
        val src = tmp.resolve("a1.parquet")
        write(schema, src) { f ->
            val g = f.newGroup()
            g.add(0, Binary.fromConstantByteArray(ByteArray(0)))
            listOf(g)
        }

        // Reader: total, degrades to null bounds.
        val footer = FooterParse.parse(LocalInputFile(src))
        val aggs =
            FooterStats.aggregate(
                footer,
                listOf(CatalogColumn(1, "d", ColType.DECIMAL, 2)),
                src.toString(),
            )
        val readerOk = aggs.size == 1 && aggs[0].lowerBound == null

        // Rewriter: raw NumberFormatException.
        val thrown =
            try {
                ParquetRewriter.rewrite(
                    listOf(ParquetRewriter.Input(src, 0L, null)),
                    live,
                    emptyList(),
                    tmp.resolve("a1-out.parquet"),
                )
                null
            } catch (t: Throwable) {
                t
            }
        say(
            "A1 empty-decimal-binary (streaming path)",
            thrown is NumberFormatException,
            "reader=${if (readerOk) "clean null bounds" else "unexpected"} rewriter=${thrown?.javaClass?.name}: " +
                "${thrown?.message} @ ${thrown?.stackTrace?.firstOrNull { it.className.contains("hoglake") }}",
        )
    }

    private fun reproA1bEmptyDecimalBinarySorted(tmp: Path) {
        val live =
            listOf(
                Column(1, 0, ColumnDef("d", ColType.DECIMAL, mapOf("precision" to 10, "scale" to 2))),
                Column(2, 1, ColumnDef("k", ColType.LONG)),
            )
        val schema =
            MessageType(
                "m",
                listOf<Type>(
                    Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .`as`(LogicalTypeAnnotation.decimalType(2, 10)).id(1).named("d"),
                    Types.optional(PrimitiveType.PrimitiveTypeName.INT64).id(2).named("k"),
                ),
            )
        val src = tmp.resolve("a1b.parquet")
        write(schema, src) { f ->
            // Two rows so the comparator actually compares.
            val g1 = f.newGroup()
            g1.add(0, Binary.fromConstantByteArray(ByteArray(0)))
            g1.add(1, 7L)
            val g2 = f.newGroup()
            g2.add(0, Binary.fromConstantByteArray(byteArrayOf(1)))
            g2.add(1, 3L)
            listOf(g1, g2)
        }
        val thrown =
            try {
                ParquetRewriter.rewrite(
                    listOf(ParquetRewriter.Input(src, 0L, null)),
                    live,
                    listOf(
                        com.posthog.hoglake.model.SortFieldDef(
                            1L,
                            com.posthog.hoglake.model.SortDirection.ASC,
                            com.posthog.hoglake.model.NullOrder.NULLS_LAST,
                        ),
                    ),
                    tmp.resolve("a1b-out.parquet"),
                )
                null
            } catch (t: Throwable) {
                t
            }
        say(
            "A1b empty-decimal-binary (sorted path, compareNonNull)",
            thrown is NumberFormatException,
            "${thrown?.javaClass?.name}: ${thrown?.message} @ " +
                "${thrown?.stackTrace?.firstOrNull { it.className.contains("hoglake") }}",
        )
    }

    // ---- A2: ids on container groups, none on leaves ---------------------

    private fun reproA2ContainerIdsWithoutLeafIds(tmp: Path) {
        val live =
            listOf(
                Column(
                    fieldId = 1,
                    ordinal = 0,
                    def = ColumnDef("s", ColType.STRUCT),
                    children = listOf(Column(2, 0, ColumnDef("f0", ColType.LONG))),
                ),
            )
        val catalog =
            listOf(
                CatalogColumn(
                    1,
                    "s",
                    ColType.STRUCT,
                    null,
                    listOf(CatalogColumn(2, "f0", ColType.LONG, null)),
                ),
            )
        val schema =
            MessageType(
                "m",
                listOf<Type>(
                    Types.buildGroup(Type.Repetition.OPTIONAL)
                        .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("f0"))
                        .id(1).named("s"),
                ),
            )
        val src = tmp.resolve("a2.parquet")
        write(schema, src) { f ->
            val g = f.newGroup()
            g.addGroup(0).add(0, 42L)
            listOf(g)
        }

        val footer = FooterParse.parse(LocalInputFile(src))
        val aggs = FooterStats.aggregate(footer, catalog, src.toString())
        val flagged = FooterStats.missingFieldIds(schema)
        val usesIds = FooterStats.usesFieldIds(schema)

        val out = tmp.resolve("a2-out.parquet")
        val result = ParquetRewriter.rewrite(listOf(ParquetRewriter.Input(src, 0L, null)), live, emptyList(), out)
        val outLeaves = NestedFuzz.leafStats(out).second
        val copied =
            outLeaves.firstOrNull { it.fieldId == 2 }
                ?.let { it.valueCount - (it.nullCount ?: 0) } ?: 0L

        say(
            "A2 container-ids-without-leaf-ids",
            aggs.isEmpty() && copied > 0,
            "reader stats=${aggs.size} rows (expected 1), rewriter copied $copied non-null value(s), " +
                "rowsWritten=${result.rowsWritten}; usesFieldIds=$usesIds missingFieldIds=$flagged",
        )
    }

    // ---- A3: duplicate field names -------------------------------------

    private fun reproA3DuplicateFieldNames(tmp: Path) {
        val live = listOf(Column(1, 0, ColumnDef("a", ColType.LONG)))
        val schema =
            MessageType(
                "m",
                listOf<Type>(
                    Types.optional(PrimitiveType.PrimitiveTypeName.INT64).id(1).named("dup"),
                    Types.optional(PrimitiveType.PrimitiveTypeName.INT64).id(2).named("dup"),
                ),
            )
        val src = tmp.resolve("a3.parquet")
        write(schema, src) { f ->
            val g = f.newGroup()
            g.add(0, 1L)
            g.add(1, 2L)
            listOf(g)
        }
        val readerThrew =
            try {
                val footer = FooterParse.parse(LocalInputFile(src))
                FooterStats.aggregate(footer, listOf(CatalogColumn(1, "a", ColType.LONG, null)), src.toString())
                null
            } catch (t: Throwable) {
                t
            }
        val thrown =
            try {
                ParquetRewriter.rewrite(
                    listOf(ParquetRewriter.Input(src, 0L, null)),
                    live,
                    emptyList(),
                    tmp.resolve("a3-out.parquet"),
                )
                null
            } catch (t: Throwable) {
                t
            }
        say(
            "A3 duplicate-field-names",
            thrown != null && thrown !is UnconvertibleSchemaException,
            "reader=${readerThrew?.javaClass?.name ?: "clean"} rewriter=${thrown?.javaClass?.name}: ${thrown?.message}",
        )
    }

    // ---- A4: decimal value over precision, refused MID-WRITE -------------

    private fun reproA4DecimalPrecisionMidWrite(tmp: Path) {
        val live = listOf(Column(1, 0, ColumnDef("d", ColType.DECIMAL, mapOf("precision" to 4, "scale" to 0))))
        val schema =
            MessageType(
                "m",
                listOf<Type>(
                    Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .`as`(LogicalTypeAnnotation.decimalType(0, 4)).id(1).named("d"),
                ),
            )
        val src = tmp.resolve("a4.parquet")
        write(schema, src) { f ->
            val ok = f.newGroup()
            ok.add(0, Binary.fromConstantByteArray(java.math.BigInteger.valueOf(12).toByteArray()))
            val bad = f.newGroup()
            bad.add(0, Binary.fromConstantByteArray(java.math.BigInteger.valueOf(999_999).toByteArray()))
            listOf(ok, bad)
        }
        val out = tmp.resolve("a4-out.parquet")
        val thrown =
            try {
                ParquetRewriter.rewrite(listOf(ParquetRewriter.Input(src, 0L, null)), live, emptyList(), out)
                null
            } catch (t: Throwable) {
                t
            }
        val partial = Files.exists(out) && Files.size(out) > 0
        say(
            "A4 decimal-over-precision refused mid-write",
            thrown is UnconvertibleSchemaException,
            "${thrown?.javaClass?.simpleName}: ${thrown?.message}; " +
                "frame=${thrown?.stackTrace?.firstOrNull { it.className.contains("hoglake") }}; " +
                "partial output left on disk=$partial (${if (partial) Files.size(out) else 0} bytes)",
        )
    }

    // ---- A5: non-UTF-8 bytes accepted as a `string` bound ----------------

    private fun reproA5NonUtf8StringBound(tmp: Path) {
        val schema =
            MessageType(
                "m",
                listOf<Type>(
                    Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .`as`(LogicalTypeAnnotation.stringType()).id(1).named("s"),
                ),
            )
        val src = tmp.resolve("a5.parquet")
        write(schema, src) { f ->
            listOf(
                byteArrayOf(0xFF.toByte(), 0x01),
                byteArrayOf(0xFE.toByte(), 0x02),
            ).map { b ->
                f.newGroup().also { it.add(0, Binary.fromConstantByteArray(b)) }
            }
        }
        val footer = FooterParse.parse(LocalInputFile(src))
        val agg =
            FooterStats.aggregate(footer, listOf(CatalogColumn(1, "s", ColType.STRING, null)), src.toString())
                .single()
        val lo = agg.lowerBound!!
        val hi = agg.upperBound!!
        val rt =
            com.posthog.hoglake.stats.IcebergSingleValue.encode(
                ColType.STRING,
                com.posthog.hoglake.stats.IcebergSingleValue.decode(ColType.STRING, lo),
            )
        say(
            "A5 non-utf8 string bound (codec round-trip is lossy)",
            !rt.contentEquals(lo),
            "stored lo=${lo.joinToString("") { "%02x".format(it) }} hi=${hi.joinToString("") { "%02x".format(it) }}; " +
                "encode(decode(lo))=${rt.joinToString("") { "%02x".format(it) }}",
        )
    }

    // ---- helper ----------------------------------------------------------

    private fun write(
        schema: MessageType,
        path: Path,
        rows: (SimpleGroupFactory) -> List<org.apache.parquet.example.data.Group>,
    ) {
        val f = SimpleGroupFactory(schema)
        ExampleParquetWriter.builder(LocalOutputFile(path))
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .build()
            .use { w -> rows(f).forEach { w.write(it) } }
    }
}

/**
 * One campaign finding replayed: [reproduced] true means the defect is
 * still present. The regression test asserts every one is false.
 */
data class ReproCheck(val name: String, val reproduced: Boolean, val detail: String)
