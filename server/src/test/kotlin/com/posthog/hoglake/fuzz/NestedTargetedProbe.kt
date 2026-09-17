package com.posthog.hoglake.fuzz

import com.posthog.hoglake.compaction.ParquetRewriter
import com.posthog.hoglake.compaction.UnconvertibleSchemaException
import com.posthog.hoglake.hydrator.CatalogColumn
import com.posthog.hoglake.hydrator.FooterParse
import com.posthog.hoglake.hydrator.FooterStats
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.service.ColumnTrees
import com.posthog.hoglake.service.Identifiers
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
import java.nio.file.Files
import java.nio.file.Path

/**
 * Targeted probes for classes the generative campaigns under-sample:
 * the `_hog_row_id` carrier, heterogeneous multi-input rewrites, and the
 * nested-map positional/id binding asymmetry.
 */
object NestedTargetedProbe {
    @JvmStatic
    fun main(args: Array<String>) {
        for (p in run(Files.createTempDirectory("hoglake-probe"))) println("${p.name} :: ${p.verdict}")
    }

    /**
     * Every B probe, replayed once, as VERDICT STRINGS.
     *
     * Returned rather than printed so `:test` can assert on them. These
     * were a `main` nobody ran automatically: four of the seven findings
     * got hand-written tests and B4, B5 and B7 were pinned only by a
     * program no CI job invokes. A finding whose only regression cover
     * is a fuzz corpus or a manual main reaches CI as a green build —
     * the `fuzz` task is separate from `:test`, and `:test` replays
     * seeds, not campaigns.
     */
    fun run(tmp: Path): List<ProbeOutcome> {
        outcomes.clear()
        probeRowIdCarrierWrongType(tmp)
        probeRowIdCarrierNull(tmp)
        probeRowIdCarrierAsCatalogColumn(tmp)
        probeHeterogeneousInputs(tmp)
        probeNestedMapKeyIdOnGroupOnly(tmp)
        probeRowIdCarrierHijack(tmp)
        probeRowIdCarrierInsideStruct(tmp)
        return outcomes.toList()
    }

    private val outcomes = mutableListOf<ProbeOutcome>()

    /**
     * B6 — the sharp one. A client declares a top-level `long` column
     * NAMED `_hog_row_id` (DDL accepts it: B3). Its files carry that
     * column. The rewriter locates the row-id carrier by NAME ONLY, so
     * it reads the USER's values as hoglake row ids — and stamps them
     * into the output's real carrier. Row ids are the lineage guarantee
     * (AGENT.md invariant 2: assigned server-side, never reused); here
     * a client chooses them.
     */
    private fun probeRowIdCarrierHijack(tmp: Path) {
        val live =
            listOf(
                Column(1, 0, ColumnDef("a", ColType.LONG)),
                Column(2, 1, ColumnDef(ParquetRewriter.ROW_ID_COLUMN, ColType.LONG)),
            )
        val schema =
            MessageType(
                "m",
                listOf<Type>(
                    Types.optional(PrimitiveType.PrimitiveTypeName.INT64).id(1).named("a"),
                    Types.optional(PrimitiveType.PrimitiveTypeName.INT64).id(2)
                        .named(ParquetRewriter.ROW_ID_COLUMN),
                ),
            )
        val src = tmp.resolve("b6.parquet")
        write(schema, src) { f ->
            (0 until 3).map { i ->
                f.newGroup().also {
                    it.add(0, i.toLong())
                    // The client's own values, nothing to do with row ids.
                    it.add(1, -999_000L - i)
                }
            }
        }
        val out = tmp.resolve("b6-out.parquet")
        val r =
            try {
                ParquetRewriter.rewrite(
                    // rowIdStart = 5000: the row ids the SERVER assigned.
                    listOf(ParquetRewriter.Input(src, 5000L, null)),
                    live,
                    emptyList(),
                    out,
                )
            } catch (t: Throwable) {
                say("B6 _hog_row_id hijack", "threw ${t.javaClass.simpleName}: ${t.message?.take(160)}")
                return
            }
        say(
            "B6 _hog_row_id hijack (client column named _hog_row_id, type long)",
            "server-assigned row ids were 5000..5002; rewrite reports minRowId=${r.minRowId} " +
                "rowsWritten=${r.rowsWritten} — the client's own values were taken as row ids",
        )
    }

    /**
     * B7 — the same name one level down. `_hog_row_id` as a STRUCT field
     * is not a top-level field of the input schema, so `indexOfFirst`
     * does not see it; the output schema is well-formed. Included to
     * bound the blast radius of B3/B6 precisely.
     */
    private fun probeRowIdCarrierInsideStruct(tmp: Path) {
        val live =
            listOf(
                Column(
                    1,
                    0,
                    ColumnDef("s", ColType.STRUCT),
                    children = listOf(Column(2, 0, ColumnDef(ParquetRewriter.ROW_ID_COLUMN, ColType.LONG))),
                ),
            )
        val schema =
            MessageType(
                "m",
                listOf<Type>(
                    Types.buildGroup(Type.Repetition.OPTIONAL)
                        .addField(
                            Types.optional(PrimitiveType.PrimitiveTypeName.INT64).id(2)
                                .named(ParquetRewriter.ROW_ID_COLUMN),
                        )
                        .id(1).named("s"),
                ),
            )
        val src = tmp.resolve("b7.parquet")
        write(schema, src) { f ->
            val g = f.newGroup()
            g.addGroup(0).add(0, 42L)
            listOf(g)
        }
        val t = rewriteCatching(src, live, tmp.resolve("b7-out.parquet"))
        say(
            "B7 _hog_row_id as a STRUCT field",
            "${t?.javaClass?.simpleName ?: "ACCEPTED"}: ${t?.message?.take(160) ?: "no carrier confusion"}",
        )
    }

    private fun say(
        name: String,
        verdict: String,
    ) {
        outcomes += ProbeOutcome(name, verdict)
    }

    // ---- B1: a foreign file with a STRING column called _hog_row_id ------

    private fun probeRowIdCarrierWrongType(tmp: Path) {
        val live = listOf(Column(1, 0, ColumnDef("a", ColType.LONG)))
        val schema =
            MessageType(
                "m",
                listOf<Type>(
                    Types.optional(PrimitiveType.PrimitiveTypeName.INT64).id(1).named("a"),
                    Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .`as`(LogicalTypeAnnotation.stringType()).id(77).named(ParquetRewriter.ROW_ID_COLUMN),
                ),
            )
        val src = tmp.resolve("b1.parquet")
        write(schema, src) { f ->
            val g = f.newGroup()
            g.add(0, 5L)
            g.add(1, Binary.fromString("not-a-row-id"))
            listOf(g)
        }
        val t = rewriteCatching(src, live, tmp.resolve("b1-out.parquet"))
        say(
            "B1 foreign _hog_row_id column of the wrong physical type",
            "${t?.javaClass?.name ?: "ACCEPTED"}: ${t?.message} @ " +
                "${t?.stackTrace?.firstOrNull { it.className.contains("hoglake") }}",
        )
    }

    // ---- B2: an OPTIONAL _hog_row_id that is null in a row ---------------

    private fun probeRowIdCarrierNull(tmp: Path) {
        val live = listOf(Column(1, 0, ColumnDef("a", ColType.LONG)))
        val schema =
            MessageType(
                "m",
                listOf<Type>(
                    Types.optional(PrimitiveType.PrimitiveTypeName.INT64).id(1).named("a"),
                    Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .id(ParquetRewriter.ROW_ID_FIELD_ID).named(ParquetRewriter.ROW_ID_COLUMN),
                ),
            )
        val src = tmp.resolve("b2.parquet")
        write(schema, src) { f ->
            val g = f.newGroup()
            g.add(0, 5L) // _hog_row_id left NULL
            listOf(g)
        }
        val t = rewriteCatching(src, live, tmp.resolve("b2-out.parquet"))
        say(
            "B2 NULL _hog_row_id carrier value",
            "${t?.javaClass?.name ?: "ACCEPTED"}: ${t?.message} @ " +
                "${t?.stackTrace?.firstOrNull { it.className.contains("hoglake") }}",
        )
    }

    // ---- B3: can a CLIENT declare a column called _hog_row_id? -----------

    private fun probeRowIdCarrierAsCatalogColumn(tmp: Path) {
        val def = ColumnDef(ParquetRewriter.ROW_ID_COLUMN, ColType.STRING)
        val ident =
            try {
                Identifiers.validate("column", def.name)
                "ACCEPTED by Identifiers.validate"
            } catch (t: Throwable) {
                "refused: ${t.javaClass.simpleName}"
            }
        val trees =
            try {
                ColumnTrees.validate(listOf(def))
                "ACCEPTED by ColumnTrees.validate"
            } catch (t: Throwable) {
                "refused: ${t.javaClass.simpleName}"
            }
        // And what does the rewriter's OUTPUT schema then look like?
        val live = listOf(Column(1, 0, def))
        val schema =
            MessageType(
                "m",
                listOf<Type>(
                    Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .`as`(LogicalTypeAnnotation.stringType()).id(1)
                        .named(ParquetRewriter.ROW_ID_COLUMN),
                ),
            )
        val src = tmp.resolve("b3.parquet")
        write(schema, src) { f ->
            val g = f.newGroup()
            g.add(0, Binary.fromString("hello"))
            listOf(g)
        }
        val t = rewriteCatching(src, live, tmp.resolve("b3-out.parquet"))
        say(
            "B3 catalog column literally named _hog_row_id",
            "$ident / $trees / rewrite -> ${t?.javaClass?.name ?: "ACCEPTED"}: ${t?.message} @ " +
                "${t?.stackTrace?.firstOrNull { it.className.contains("hoglake") }}",
        )
    }

    // ---- B4: heterogeneous inputs (one canonical, one mutated) -----------

    private fun probeHeterogeneousInputs(tmp: Path) {
        val live =
            listOf(
                Column(
                    1,
                    0,
                    ColumnDef("l", ColType.LIST),
                    children = listOf(Column(2, 0, ColumnDef("element", ColType.LONG))),
                ),
            )

        fun listSchema(
            elementName: String,
            elementId: Int?,
        ): MessageType {
            var el = Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
            val elType = if (elementId != null) el.id(elementId).named(elementName) else el.named(elementName)
            return MessageType(
                "m",
                listOf<Type>(
                    Types.buildGroup(Type.Repetition.OPTIONAL)
                        .addField(Types.repeatedGroup().addField(elType).named("list"))
                        .`as`(LogicalTypeAnnotation.listType())
                        .id(1).named("l"),
                ),
            )
        }
        val a = tmp.resolve("b4a.parquet")
        val b = tmp.resolve("b4b.parquet")
        write(listSchema("element", 2), a) { f ->
            val g = f.newGroup()
            val lst = g.addGroup(0)
            lst.addGroup(0).add(0, 10L)
            lst.addGroup(0).add(0, 11L)
            listOf(g)
        }
        // Same data, but the element is named "item" and carries no id —
        // the shape pyarrow and many foreign writers actually produce.
        write(listSchema("item", null), b) { f ->
            val g = f.newGroup()
            val lst = g.addGroup(0)
            lst.addGroup(0).add(0, 20L)
            listOf(g)
        }
        val out = tmp.resolve("b4-out.parquet")
        val t =
            try {
                val r =
                    ParquetRewriter.rewrite(
                        listOf(
                            ParquetRewriter.Input(a, 0L, null),
                            ParquetRewriter.Input(b, 100L, null),
                        ),
                        live,
                        emptyList(),
                        out,
                    )
                val leaves = NestedFuzz.leafStats(out).second
                val el = leaves.firstOrNull { it.fieldId == 2 }
                say(
                    "B4 heterogeneous list inputs (ids vs id-less 'item')",
                    "rowsWritten=${r.rowsWritten} minRowId=${r.minRowId} element values=" +
                        "${el?.valueCount} nulls=${el?.nullCount} (expected 3 non-null)",
                )
                null
            } catch (t: Throwable) {
                t
            }
        if (t != null) {
            say("B4 heterogeneous list inputs", "${t.javaClass.name}: ${t.message}")
        }
    }

    // ---- B5: nested map key whose GROUP carries an id, leaves id-less ----

    private fun probeNestedMapKeyIdOnGroupOnly(tmp: Path) {
        // catalog: map<struct<k:long>, long>
        val live =
            listOf(
                Column(
                    1,
                    0,
                    ColumnDef("m", ColType.MAP),
                    children =
                        listOf(
                            Column(
                                2,
                                0,
                                ColumnDef("key", ColType.STRUCT, nullable = false),
                                children = listOf(Column(3, 0, ColumnDef("k", ColType.LONG))),
                            ),
                            Column(4, 1, ColumnDef("value", ColType.LONG)),
                        ),
                ),
            )
        val catalog =
            listOf(
                CatalogColumn(
                    1,
                    "m",
                    ColType.MAP,
                    null,
                    listOf(
                        CatalogColumn(
                            2,
                            "key",
                            ColType.STRUCT,
                            null,
                            listOf(CatalogColumn(3, "k", ColType.LONG, null)),
                        ),
                        CatalogColumn(4, "value", ColType.LONG, null),
                    ),
                ),
            )
        // The key GROUP carries a WRONG id (99); every primitive leaf is
        // id-less, so FooterStats.usesFieldIds is false.
        val keyGroup =
            Types.buildGroup(Type.Repetition.REQUIRED)
                .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("k"))
                .id(99).named("key")
        val schema =
            MessageType(
                "m",
                listOf<Type>(
                    Types.buildGroup(Type.Repetition.OPTIONAL)
                        .addField(
                            Types.repeatedGroup()
                                .addFields(
                                    keyGroup,
                                    Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("value"),
                                )
                                .named("key_value"),
                        )
                        .`as`(LogicalTypeAnnotation.mapType())
                        .id(1).named("m"),
                ),
            )
        val src = tmp.resolve("b5.parquet")
        write(schema, src) { f ->
            val g = f.newGroup()
            val map = g.addGroup(0)
            val e = map.addGroup(0)
            e.addGroup(0).add(0, 7L)
            e.add(1, 8L)
            listOf(g)
        }
        val footer = FooterParse.parse(LocalInputFile(src))
        val aggs = FooterStats.aggregate(footer, catalog, src.toString())
        val t = rewriteCatching(src, live, tmp.resolve("b5-out.parquet"))
        say(
            "B5 nested map key: id on the GROUP only, leaves id-less",
            "usesFieldIds=${FooterStats.usesFieldIds(schema)} readerStats=${aggs.map { it.fieldId }} " +
                "rewriter=${if (t == null) "ACCEPTED" else "${t.javaClass.simpleName}: ${t.message?.take(140)}"}",
        )
    }

    // ---- helpers ---------------------------------------------------------

    private fun rewriteCatching(
        src: Path,
        live: List<Column>,
        out: Path,
    ): Throwable? =
        try {
            ParquetRewriter.rewrite(listOf(ParquetRewriter.Input(src, 0L, null)), live, emptyList(), out)
            null
        } catch (t: Throwable) {
            t
        }

    private fun unusedMarker(t: UnconvertibleSchemaException) = t

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

/** One probe's name and the one-line verdict it produced. */
data class ProbeOutcome(val name: String, val verdict: String)
