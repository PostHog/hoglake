package com.posthog.hoglake.fuzz

import com.posthog.hoglake.compaction.ParquetRewriter
import com.posthog.hoglake.compaction.UnconvertibleSchemaException
import com.posthog.hoglake.hydrator.CatalogColumn
import com.posthog.hoglake.hydrator.FooterParse
import com.posthog.hoglake.hydrator.FooterStats
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import org.apache.parquet.example.data.Group
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
import java.math.BigInteger
import java.nio.file.Files
import java.nio.file.Path

/** Minimized repros for the fleet's second-generation findings. */
object NestedReprosD {
    @JvmStatic
    fun main(args: Array<String>) {
        for (check in run(Files.createTempDirectory("hoglake-repro-d"))) {
            println(
                "${if (check.reproduced) "REPRODUCED" else "not reproduced"}  " +
                    "${check.name} :: ${check.detail}",
            )
        }
    }

    /** Every D finding replayed once. See [ReproCheck]. */
    fun run(tmp: Path): List<ReproCheck> {
        checks.clear()
        d1BinaryOverFixedLenByteArray(tmp)
        d2StringOverDecimalAnnotatedBinary(tmp)
        d2bBinaryOverDecimalAnnotatedBinary(tmp)
        d3ListElementIdlessUnderIdBearingWrapper(tmp)
        return checks.toList()
    }

    private val checks = mutableListOf<ReproCheck>()

    private fun say(
        name: String,
        holds: Boolean,
        detail: String,
    ) {
        checks += ReproCheck(name, holds, detail)
    }

    /**
     * D1 — catalog `binary` over a FIXED_LEN_BYTE_ARRAY leaf. The reader
     * accepts both physical forms (`BINARY, FIXED_LEN_BYTE_ARRAY -> raw`)
     * and produces bounds; the rewriter accepts only BINARY and refuses
     * the file forever. FooterStats' own KDoc says the two surfaces have
     * to agree about which files they accept.
     */
    private fun d1BinaryOverFixedLenByteArray(tmp: Path) {
        val live = listOf(Column(1, 0, ColumnDef("b", ColType.BINARY)))
        val schema =
            MessageType(
                "m",
                listOf<Type>(
                    Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                        .length(4).id(1).named("b"),
                ),
            )
        val src = tmp.resolve("d1.parquet")
        write(schema, src) { f ->
            listOf(byteArrayOf(1, 2, 3, 4), byteArrayOf(9, 9, 9, 9)).map { b ->
                f.newGroup().also { it.add(0, Binary.fromConstantByteArray(b)) }
            }
        }
        val agg =
            FooterStats.aggregate(
                FooterParse.parse(LocalInputFile(src)),
                listOf(CatalogColumn(1, "b", ColType.BINARY, null)),
                src.toString(),
            ).singleOrNull()
        val refusal =
            try {
                ParquetRewriter.rewrite(
                    listOf(ParquetRewriter.Input(src, 0L, null)),
                    live,
                    emptyList(),
                    tmp.resolve("d1-out.parquet"),
                )
                null
            } catch (t: Throwable) {
                t
            }
        say(
            "D1 binary column over FIXED_LEN_BYTE_ARRAY: reader bounds, rewriter refuses",
            agg?.lowerBound != null && refusal is UnconvertibleSchemaException,
            "reader lower=${agg?.lowerBound?.hex()} upper=${agg?.upperBound?.hex()} " +
                "valueCount=${agg?.valueCount}; rewriter=${refusal?.javaClass?.simpleName}: ${refusal?.message}",
        )
    }

    /**
     * D2 — catalog `json` (or `string`) over a DECIMAL-annotated BINARY
     * leaf. The reader takes the bytes verbatim ("the JSON logical
     * annotation... changes neither the bytes nor their sort order"), but
     * parquet ordered THAT chunk's min/max as a signed two's-complement
     * DECIMAL. Read back as unsigned bytes the pair comes out INVERTED —
     * lower > upper, the shape a pruner reads as "no rows here", silently
     * dropping the file from every scan. Exactly the sort-order trap the
     * unsigned-INT rule (`maxUnsignedParquetWidth`) exists to stop, one
     * annotation family over.
     */
    private fun d2StringOverDecimalAnnotatedBinary(tmp: Path) {
        val schema =
            MessageType(
                "m",
                listOf<Type>(
                    Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .`as`(LogicalTypeAnnotation.decimalType(0, 10)).id(1).named("j"),
                ),
            )
        val src = tmp.resolve("d2.parquet")
        // -1 encodes as 0xFF (decimal min), +1 as 0x01 (decimal max).
        // Unsigned byte order says 0xFF > 0x01, so the pair inverts.
        write(schema, src) { f ->
            listOf(BigInteger.valueOf(-1), BigInteger.ONE).map { v ->
                f.newGroup().also { it.add(0, Binary.fromConstantByteArray(v.toByteArray())) }
            }
        }
        val footer = FooterParse.parse(LocalInputFile(src))
        for (t in listOf(ColType.JSON, ColType.STRING)) {
            val agg =
                FooterStats.aggregate(footer, listOf(CatalogColumn(1, "j", t, null)), src.toString())
                    .singleOrNull()
            val lo = agg?.lowerBound
            val hi = agg?.upperBound
            val inverted = lo != null && hi != null && java.util.Arrays.compareUnsigned(lo, hi) > 0
            // And the REWRITER happily copies it, re-stamping the bytes
            // with a STRING/JSON annotation: annotation laundering.
            val live = listOf(Column(1, 0, ColumnDef("j", t)))
            val rewrite =
                try {
                    ParquetRewriter.rewrite(
                        listOf(ParquetRewriter.Input(src, 0L, null)),
                        live,
                        emptyList(),
                        tmp.resolve("d2-out-${t.wire}.parquet"),
                    ).rowsWritten.toString()
                } catch (e: Throwable) {
                    "refused: ${e.javaClass.simpleName}"
                }
            say(
                "D2 ${t.wire} column over DECIMAL-annotated BINARY: inverted bound pair",
                inverted,
                "lower=${lo?.hex()} upper=${hi?.hex()} (unsigned compare says lower > upper); " +
                    "rewriter rowsWritten=$rewrite",
            )
        }
    }

    /**
     * D2b — the same trap for a catalog `binary` column, which also takes
     * the bytes verbatim.
     */
    private fun d2bBinaryOverDecimalAnnotatedBinary(tmp: Path) {
        val schema =
            MessageType(
                "m",
                listOf<Type>(
                    Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .`as`(LogicalTypeAnnotation.decimalType(0, 10)).id(1).named("b"),
                ),
            )
        val src = tmp.resolve("d2b.parquet")
        write(schema, src) { f ->
            listOf(BigInteger.valueOf(-1), BigInteger.ONE).map { v ->
                f.newGroup().also { it.add(0, Binary.fromConstantByteArray(v.toByteArray())) }
            }
        }
        val agg =
            FooterStats.aggregate(
                FooterParse.parse(LocalInputFile(src)),
                listOf(CatalogColumn(1, "b", ColType.BINARY, null)),
                src.toString(),
            ).singleOrNull()
        val lo = agg?.lowerBound
        val hi = agg?.upperBound
        say(
            "D2b binary column over DECIMAL-annotated BINARY: inverted bound pair",
            lo != null && hi != null && java.util.Arrays.compareUnsigned(lo, hi) > 0,
            "lower=${lo?.hex()} upper=${hi?.hex()}",
        )
    }

    /**
     * D3 — the LIST shape of the A2 class: the wrapper carries the live
     * list's field id, the element carries none. `usesFieldIds` scans
     * LEAVES only, so it answers false; `findField`'s name fallback then
     * requires `id == null`, which the id-BEARING wrapper is not. The
     * reader loses the whole subtree's stats while the rewriter binds by
     * id and copies every value.
     */
    private fun d3ListElementIdlessUnderIdBearingWrapper(tmp: Path) {
        val live =
            listOf(
                Column(
                    4,
                    0,
                    ColumnDef("c1", ColType.LIST),
                    children = listOf(Column(5, 0, ColumnDef("element", ColType.UUID_T))),
                ),
            )
        val catalog =
            listOf(
                CatalogColumn(
                    4,
                    "c1",
                    ColType.LIST,
                    null,
                    listOf(CatalogColumn(5, "element", ColType.UUID_T, null)),
                ),
            )
        val schema =
            MessageType(
                "m",
                listOf<Type>(
                    Types.buildGroup(Type.Repetition.OPTIONAL)
                        .addField(
                            Types.repeatedGroup()
                                .addField(
                                    Types.primitive(
                                        PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
                                        Type.Repetition.REQUIRED,
                                    ).length(16).`as`(LogicalTypeAnnotation.uuidType()).named("element"),
                                )
                                .named("list"),
                        )
                        .`as`(LogicalTypeAnnotation.listType())
                        .id(4).named("c1"),
                ),
            )
        val src = tmp.resolve("d3.parquet")
        write(schema, src) { f ->
            val g = f.newGroup()
            val l = g.addGroup(0)
            l.addGroup(0).add(0, Binary.fromConstantByteArray(ByteArray(16) { 1 }))
            l.addGroup(0).add(0, Binary.fromConstantByteArray(ByteArray(16) { 2 }))
            listOf(g)
        }
        val aggs =
            FooterStats.aggregate(FooterParse.parse(LocalInputFile(src)), catalog, src.toString())
        val out = tmp.resolve("d3-out.parquet")
        val r = ParquetRewriter.rewrite(listOf(ParquetRewriter.Input(src, 0L, null)), live, emptyList(), out)
        val copied =
            NestedFuzz.leafStats(out).second.firstOrNull { it.fieldId == 5 }
                ?.let { it.valueCount - (it.nullCount ?: 0) } ?: 0L
        say(
            "D3 list wrapper has the id, element does not (A2 class, list shape)",
            aggs.isEmpty() && copied > 0,
            "usesFieldIds=${FooterStats.usesFieldIds(schema)} missingFieldIds=" +
                "${FooterStats.missingFieldIds(schema)}; reader stats rows=${aggs.size} (expected 1); " +
                "rewriter copied $copied element value(s), rowsWritten=${r.rowsWritten}",
        )
    }

    private fun ByteArray.hex(): String = joinToString("") { "%02x".format(it) }

    private fun write(
        schema: MessageType,
        path: Path,
        rows: (SimpleGroupFactory) -> List<Group>,
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
