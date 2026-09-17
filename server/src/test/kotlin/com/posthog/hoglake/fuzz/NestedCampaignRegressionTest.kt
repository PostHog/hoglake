package com.posthog.hoglake.fuzz

import com.posthog.hoglake.compaction.InvalidDataException
import com.posthog.hoglake.compaction.MixedIdBindingRepro
import com.posthog.hoglake.compaction.ParquetRewriter
import com.posthog.hoglake.compaction.UnconvertibleSchemaException
import com.posthog.hoglake.hydrator.CatalogColumn
import com.posthog.hoglake.hydrator.FooterParse
import com.posthog.hoglake.hydrator.FooterStats
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MAX_COLUMN_NESTING_DEPTH
import com.posthog.hoglake.model.StatsSanity
import com.posthog.hoglake.model.columnDefDepth
import com.posthog.hoglake.model.nodeCount
import com.posthog.hoglake.service.ColumnTrees
import com.posthog.hoglake.service.CorruptDefinitionException
import com.posthog.hoglake.service.Identifiers
import com.posthog.hoglake.service.TableCreationDefinitionCodec
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
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import java.nio.file.Path

/**
 * Every finding of the phase-2 nested fuzz campaign, pinned.
 *
 * The A/C/D sets replay through the minimized repro objects themselves
 * ([NestedMinimizedRepros], [NestedReprosD]) so the assertion here and
 * the artifact the campaign produced cannot drift apart — each `run`
 * reports whether the defect is still reachable, and the answer must be
 * no, forever. The rest of the file pins the findings whose fix lives
 * on a surface those two do not drive (DDL validation, the receipt
 * codec, the stats gate, the row budget).
 *
 * These are REGRESSION tests, not the fuzzing itself: the campaign runs
 * through [NestedAgreementFuzzTest] (jazzer, seeded corpus) and
 * [NestedFuzzSoak].
 */
class NestedCampaignRegressionTest {
    @Test
    fun `every minimized A and C finding stays fixed`(
        @TempDir tmp: Path,
    ) {
        val still = NestedMinimizedRepros.run(tmp).filter { it.reproduced }
        assertThat(still)
            .describedAs("campaign findings that came back")
            .isEmpty()
    }

    @Test
    fun `every minimized D finding stays fixed`(
        @TempDir tmp: Path,
    ) {
        val still = NestedReprosD.run(tmp).filter { it.reproduced }
        assertThat(still)
            .describedAs("campaign findings that came back")
            .isEmpty()
    }

    // ---- decimal parameters, which nothing validated ---------------------

    @Test
    fun `a decimal parameter that does not survive toInt is refused`() {
        // The sweep the `version` narrowing prompted, one layer out from
        // the codec. Every consumer reads these as
        // `(as? Number)?.toInt()`, which TRUNCATES: a precision stored
        // faithfully as 4294967297 came back as 1, and every value in
        // the column then failed the rewriter's over-precision check.
        for (bad in listOf(4294967297L, -4294967295L)) {
            assertThatThrownBy {
                ColumnTrees.validate(
                    listOf(ColumnDef("d", ColType.DECIMAL, mapOf("precision" to bad))),
                )
            }.describedAs("precision %d", bad)
                .isInstanceOf(HoglakeException.Validation::class.java)
                .hasMessageContaining("outside the int range")
        }
    }

    @Test
    fun `a decimal precision outside parquet's range is refused`() {
        for (bad in listOf(0, -1, 39, 99)) {
            assertThatThrownBy {
                ColumnTrees.validate(
                    listOf(ColumnDef("d", ColType.DECIMAL, mapOf("precision" to bad))),
                )
            }.describedAs("precision %d", bad)
                .isInstanceOf(HoglakeException.Validation::class.java)
                .hasMessageContaining("outside 1..38")
        }
        // A scale above the precision describes a number with more
        // fractional digits than digits.
        assertThatThrownBy {
            ColumnTrees.validate(
                listOf(ColumnDef("d", ColType.DECIMAL, mapOf("precision" to 5, "scale" to 6))),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("above its precision")
        assertThatThrownBy {
            ColumnTrees.validate(
                listOf(ColumnDef("d", ColType.DECIMAL, mapOf("precision" to 5, "scale" to -1))),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("negative scale")
        assertThatThrownBy {
            ColumnTrees.validate(
                listOf(ColumnDef("d", ColType.DECIMAL, mapOf("precision" to "five"))),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("non-integer")
    }

    @Test
    fun `sound decimal parameters, and absent ones, are accepted`() {
        // Absent means the documented default (38, 0), which the
        // rewriter already assumes — refusing it would break every
        // existing plain `decimal` column.
        ColumnTrees.validate(listOf(ColumnDef("d", ColType.DECIMAL)))
        ColumnTrees.validate(listOf(ColumnDef("d", ColType.DECIMAL, mapOf("precision" to 1))))
        ColumnTrees.validate(listOf(ColumnDef("d", ColType.DECIMAL, mapOf("precision" to 38, "scale" to 38))))
        ColumnTrees.validate(listOf(ColumnDef("d", ColType.DECIMAL, mapOf("precision" to 10, "scale" to 0))))
        // And inside a container, since validation runs at every level.
        ColumnTrees.validate(
            listOf(
                ColumnDef(
                    "l",
                    ColType.LIST,
                    children = listOf(ColumnDef("element", ColType.DECIMAL, mapOf("precision" to 9, "scale" to 2))),
                ),
            ),
        )
    }

    @Test
    fun `every targeted B probe still reaches its fixed verdict`(
        @TempDir tmp: Path,
    ) {
        // The B-class campaign findings, replayed in `:test`. They were
        // a `main` nobody ran: B1/B2/B3/B6 acquired hand-written tests
        // and B4, B5 and B7 had none at all, so three fixed findings
        // were regression-covered by a program CI never invokes.
        //
        // Asserted on the VERDICT STRING, which is what the probe
        // produces. Coarse on purpose — the point is that each shape
        // still lands where it was fixed to land, not to restate the
        // messages a dedicated test already pins.
        val byName = NestedTargetedProbe.run(tmp).associate { it.name.substringBefore(" ") to it.verdict }
        assertThat(byName.keys).containsExactlyInAnyOrder("B1", "B2", "B3", "B4", "B5", "B6", "B7")

        // A foreign field wearing the reserved name: refusal, both for
        // the wrong TYPE and the foreign ID.
        assertThat(byName["B1"]).contains("UnconvertibleSchemaException")
        // A null in a real carrier is invalid DATA, not a schema fault.
        assertThat(byName["B2"]).contains("InvalidDataException")
        // A catalog column called _hog_row_id: refused by the DDL, and
        // refused again by the rewriter for tables that predate it.
        assertThat(byName["B3"]).contains("refused: Validation").contains("collides with compaction")
        assertThat(byName["B6"]).contains("collides with compaction")
        // Heterogeneous inputs still rewrite, losing nothing.
        assertThat(byName["B4"]).contains("element values=3 nulls=0")
        // A map whose key id sits on the group only: no stats, and a
        // typed refusal — the two surfaces agreeing to decline.
        assertThat(byName["B5"]).contains("readerStats=[]").contains("UnconvertibleSchemaException")
        // And the name one level down is nobody's carrier.
        assertThat(byName["B7"]).contains("no carrier confusion")
    }

    // ---- binding: ids outrank names, at every level ----------------------

    @Test
    fun `an id-less field sharing a name never outranks the field carrying the id`(
        @TempDir tmp: Path,
    ) {
        // Field ids are the binding contract. A file that stamps ids on
        // only SOME columns — foreign writers, mostly — can hold an
        // id-less field whose name matches a catalog column while a
        // different field carries that column's id. The id must win.
        //
        // It did, until both surfaces were unified behind a single-pass
        // scan of the shared PREDICATE, which is first-match-wins by
        // position: the reader bounded the wrong column and compaction
        // copied its values into the output, then end-snapshotted the
        // input. Silent substitution, and identical on both surfaces —
        // which is exactly what an agreement oracle cannot see.
        assertThat(MixedIdBindingRepro.run(tmp)).startsWith("CORRECT")
    }

    @Test
    fun `the id-before-name search holds inside a struct too`() {
        val fields =
            listOf<Type>(
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("b"),
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64).id(1).named("x"),
            )
        // By id: the SECOND field, whatever the order.
        assertThat(FooterStats.bindIndex(fields, 1, "b", useFieldIds = true)).isEqualTo(1)
        // With no id-bearing candidate, the id-less name match stands.
        assertThat(FooterStats.bindIndex(fields, 7, "b", useFieldIds = true)).isEqualTo(0)
        // An id-BEARING field never answers to a name — that is what
        // makes a rename safe on an id-bearing file.
        assertThat(FooterStats.bindIndex(fields, 7, "x", useFieldIds = true)).isEqualTo(-1)
        // File-level gate off: names only, and still only id-less ones.
        assertThat(FooterStats.bindIndex(fields, 1, "b", useFieldIds = false)).isEqualTo(0)
        assertThat(FooterStats.bindIndex(fields, 1, "x", useFieldIds = false)).isEqualTo(-1)
    }

    @Test
    fun `a decimal-annotated FLBA under a uuid column is refused by both surfaces`(
        @TempDir tmp: Path,
    ) {
        // Sixteen bytes annotated DECIMAL are not a uuid. Parquet
        // ordered them SIGNED two's-complement; a uuid bound is
        // unsigned-byte. HIGH-2 gated string/json/binary on that and
        // left this arm, so the reader took the bytes verbatim (the
        // inverted pair was only caught afterwards by StatsSanity, which
        // then reported a repaired row and blamed the writer) and the
        // rewriter copied them under a UUID stamp.
        val schema =
            MessageType(
                "m",
                listOf<Type>(
                    Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                        .length(16)
                        .`as`(LogicalTypeAnnotation.decimalType(0, 38))
                        .id(1)
                        .named("u"),
                ),
            )
        // Values chosen so the pair is ASCENDING under unsigned byte
        // order: 0x01.. below 0x02... A mutation test showed the obvious
        // choice (0xFF vs 0x01) proves nothing here — that pair is
        // INVERTED unsigned, so StatsSanity's backstop deletes it
        // whether or not this arm has the annotation gate, and the
        // assertion passed with the gate removed. Only a pair the
        // backstop would happily keep can show that the ANNOTATION rule
        // is what refused it.
        val src = tmp.resolve("uuid-decimal.parquet")
        write(schema, src) { f ->
            listOf(
                f.newGroup().also { it.add(0, Binary.fromConstantByteArray(ByteArray(16) { 1 })) },
                f.newGroup().also { it.add(0, Binary.fromConstantByteArray(ByteArray(16) { 2 })) },
            )
        }
        val footer = FooterParse.parse(LocalInputFile(src))
        val agg =
            FooterStats.aggregate(footer, listOf(CatalogColumn(1, "u", ColType.UUID_T, null)), src.toString())
                .single()
        assertThat(agg.lowerBound).isNull()
        assertThat(agg.upperBound).isNull()
        assertThat(agg.valueCount).describedAs("counts are unaffected").isEqualTo(2)

        // Rewriter: typed refusal rather than an annotation re-stamp.
        assertThatThrownBy {
            rewrite(src, listOf(Column(1, 0, ColumnDef("u", ColType.UUID_T))), tmp.resolve("uuid-out.parquet"))
        }.isInstanceOf(UnconvertibleSchemaException::class.java)
    }

    @Test
    fun `the reader refuses duplicate sibling NAMES, as it does duplicate ids`(
        @TempDir tmp: Path,
    ) {
        // Two id-less siblings sharing a name have no correct binding,
        // exactly like two fields sharing an id. The consequence was
        // worse than an arbitrary pick: the chunk walk matches by PATH,
        // both columns have the same path, so their statistics were
        // SUMMED — a one-row two-column file reported valueCount=4 for
        // field 1 and stored it. The rewriter refuses such a file, so
        // the table was permanently uncompactable with permanently wrong
        // stats, and a value count above the row count is the kind of
        // number a planner divides by.
        val schema =
            MessageType(
                "m",
                listOf<Type>(
                    Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("b"),
                    Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("b"),
                ),
            )
        val src = tmp.resolve("dupe-names.parquet")
        write(schema, src) { f ->
            listOf(
                f.newGroup().also {
                    it.add(0, 1L)
                    it.add(1, 2L)
                },
            )
        }
        val footer = FooterParse.parse(LocalInputFile(src))
        assertThat(FooterStats.aggregate(footer, listOf(CatalogColumn(1, "b", ColType.LONG, null)), src.toString()))
            .describedAs("no stats beat summed stats from two different columns")
            .isEmpty()
        // And the rewriter still refuses it, so the two surfaces agree.
        assertThatThrownBy {
            rewrite(src, listOf(Column(1, 0, ColumnDef("b", ColType.LONG))), tmp.resolve("dupe-out.parquet"))
        }.isInstanceOf(UnconvertibleSchemaException::class.java)
    }

    // ---- the reserved column prefix, at every nesting level --------------

    @Test
    fun `a reserved-prefix column name is refused at the top level`() {
        assertThatThrownBy { ColumnTrees.validate(listOf(ColumnDef("_hog_row_id", ColType.STRING))) }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("reserved prefix")
            .hasMessageContaining("_hog")
    }

    @Test
    fun `a reserved-prefix column name is refused inside a struct`() {
        val def =
            ColumnDef(
                "outer",
                ColType.STRUCT,
                children = listOf(ColumnDef("_hoglet", ColType.LONG)),
            )
        assertThatThrownBy { ColumnTrees.validate(listOf(def)) }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("outer._hoglet")
            .hasMessageContaining("reserved prefix")
    }

    @Test
    fun `a reserved-prefix column name is refused under a list element`() {
        // The element itself is synthetically named, so the refusal has
        // to come from the struct field one level further down — the
        // level a top-level-only check never reached.
        val def =
            ColumnDef(
                "l",
                ColType.LIST,
                children =
                    listOf(
                        ColumnDef(
                            "element",
                            ColType.STRUCT,
                            children = listOf(ColumnDef("_hog_x", ColType.LONG)),
                        ),
                    ),
            )
        assertThatThrownBy { ColumnTrees.validate(listOf(def)) }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("l.element._hog_x")
    }

    @Test
    fun `the synthetic child names are still accepted`() {
        // element/key/value are this code's own names and must not be
        // caught by the identifier sweep that now runs at every level.
        ColumnTrees.validate(
            listOf(
                ColumnDef("l", ColType.LIST, children = listOf(ColumnDef("element", ColType.LONG))),
                ColumnDef(
                    "m",
                    ColType.MAP,
                    children =
                        listOf(
                            ColumnDef("key", ColType.STRING, nullable = false),
                            ColumnDef("value", ColType.LONG),
                        ),
                ),
            ),
        )
    }

    @Test
    fun `renaming a column onto the reserved prefix is refused`() {
        assertThatThrownBy { Identifiers.validateColumn("_hog_row_id") }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("reserved prefix")
    }

    // ---- the row-id carrier binds by reserved id AND type ----------------

    @Test
    fun `a carrier of the wrong physical type is a typed refusal`(
        @TempDir tmp: Path,
    ) {
        val live = listOf(Column(1, 0, ColumnDef("a", ColType.LONG)))
        // BOTH ways a field can BE the carrier: by the reserved id, and
        // by the name while declaring no id of its own. A field wearing
        // the name with a DIFFERENT id is not a candidate at all — that
        // is its own test below.
        val candidates =
            listOf(
                "by-id" to
                    Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .`as`(LogicalTypeAnnotation.stringType())
                        .id(ParquetRewriter.ROW_ID_FIELD_ID)
                        .named(ParquetRewriter.ROW_ID_COLUMN),
                "by-name" to
                    Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .`as`(LogicalTypeAnnotation.stringType())
                        .named(ParquetRewriter.ROW_ID_COLUMN),
            )
        for ((label, carrier) in candidates) {
            val schema =
                MessageType(
                    "m",
                    listOf<Type>(
                        Types.optional(PrimitiveType.PrimitiveTypeName.INT64).id(1).named("a"),
                        carrier,
                    ),
                )
            val src = tmp.resolve("wrong-type-$label.parquet")
            write(schema, src) { f ->
                val g = f.newGroup()
                g.add(0, 5L)
                g.add(1, Binary.fromString("not-a-row-id"))
                listOf(g)
            }
            assertThatThrownBy { rewrite(src, live, tmp.resolve("o1-$label.parquet")) }
                .describedAs("carrier %s", label)
                .isInstanceOf(UnconvertibleSchemaException::class.java)
                .hasMessageContaining("reserved row-id position")
        }
    }

    @Test
    fun `a null row id in the carrier is a typed refusal, never a renumber`(
        @TempDir tmp: Path,
    ) {
        val live = listOf(Column(1, 0, ColumnDef("a", ColType.LONG)))
        val schema =
            MessageType(
                "m",
                listOf<Type>(
                    Types.optional(PrimitiveType.PrimitiveTypeName.INT64).id(1).named("a"),
                    Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .id(ParquetRewriter.ROW_ID_FIELD_ID)
                        .named(ParquetRewriter.ROW_ID_COLUMN),
                ),
            )
        val src = tmp.resolve("null-carrier.parquet")
        write(schema, src) { f ->
            val g = f.newGroup()
            g.add(0, 5L) // carrier left null
            listOf(g)
        }
        assertThatThrownBy { rewrite(src, live, tmp.resolve("o2.parquet")) }
            .isInstanceOf(InvalidDataException::class.java)
            .hasMessageContaining("null ${ParquetRewriter.ROW_ID_COLUMN}")
    }

    @Test
    fun `a live column colliding with the reserved row-id column is a typed refusal`(
        @TempDir tmp: Path,
    ) {
        // Reachable only for a table created before the prefix was
        // reserved; the rewriter must still say so rather than hand
        // parquet-java a schema naming one field twice.
        val live =
            listOf(
                Column(1, 0, ColumnDef("a", ColType.LONG)),
                Column(2, 1, ColumnDef(ParquetRewriter.ROW_ID_COLUMN, ColType.LONG)),
            )
        val schema =
            MessageType(
                "m",
                listOf<Type>(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).id(1).named("a")),
            )
        val src = tmp.resolve("collide.parquet")
        write(schema, src) { f ->
            val g = f.newGroup()
            g.add(0, 1L)
            listOf(g)
        }
        assertThatThrownBy { rewrite(src, live, tmp.resolve("o3.parquet")) }
            .isInstanceOf(UnconvertibleSchemaException::class.java)
            .hasMessageContaining("collides with compaction's reserved row-id column")
    }

    @Test
    fun `a field wearing the reserved name with a foreign id is a refusal`(
        @TempDir tmp: Path,
    ) {
        // Two wrong answers and no right one, so the only honest move is
        // to refuse. Taking it as the carrier reads a user column's
        // values as row IDENTITIES; ignoring it renumbers every row in
        // the file. An earlier version of this test asserted the second
        // — it checked that the rows took positional ids — which pinned
        // the silent renumbering the KDoc two files over forbids in so
        // many words.
        val schema =
            MessageType(
                "m",
                listOf<Type>(
                    Types.optional(PrimitiveType.PrimitiveTypeName.INT64).id(1).named("a"),
                    Types.optional(PrimitiveType.PrimitiveTypeName.INT64).id(2)
                        .named(ParquetRewriter.ROW_ID_COLUMN),
                ),
            )
        val src = tmp.resolve("named-carrier.parquet")
        write(schema, src) { f ->
            (0 until 3).map { i ->
                f.newGroup().also {
                    it.add(0, i.toLong())
                    it.add(1, -999L - i)
                }
            }
        }
        assertThatThrownBy {
            ParquetRewriter.rewrite(
                listOf(ParquetRewriter.Input(src, 5000L, null)),
                listOf(Column(1, 0, ColumnDef("a", ColType.LONG))),
                emptyList(),
                tmp.resolve("named-carrier-out.parquet"),
            )
        }
            .isInstanceOf(UnconvertibleSchemaException::class.java)
            .hasMessageContaining("not the reserved ${ParquetRewriter.ROW_ID_FIELD_ID}")
    }

    @Test
    fun `a real carrier still works, by id and by name`(
        @TempDir tmp: Path,
    ) {
        // The refusal above must not swallow the two legal shapes: the
        // reserved id (every compaction output this project writes), and
        // the bare name with no id at all (what the hydrator tolerates).
        for ((label, carrier) in listOf(
            "by-id" to
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                    .id(ParquetRewriter.ROW_ID_FIELD_ID)
                    .named(ParquetRewriter.ROW_ID_COLUMN),
            "by-name" to
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                    .named(ParquetRewriter.ROW_ID_COLUMN),
        )) {
            val schema =
                MessageType(
                    "m",
                    listOf<Type>(
                        Types.optional(PrimitiveType.PrimitiveTypeName.INT64).id(1).named("a"),
                        carrier,
                    ),
                )
            val src = tmp.resolve("carrier-$label.parquet")
            write(schema, src) { f ->
                (0 until 3).map { i ->
                    f.newGroup().also {
                        it.add(0, i.toLong())
                        it.add(1, 700L + i)
                    }
                }
            }
            val out =
                ParquetRewriter.rewrite(
                    listOf(ParquetRewriter.Input(src, 5000L, null)),
                    listOf(Column(1, 0, ColumnDef("a", ColType.LONG))),
                    emptyList(),
                    tmp.resolve("carrier-$label-out.parquet"),
                )
            assertThat(out.minRowId)
                .describedAs("%s: the carrier's ids are kept, not the positional ones", label)
                .isEqualTo(700L)
        }
    }

    // ---- the per-row node budget ----------------------------------------

    @Test
    fun `a row past the node budget is refused rather than materialized`(
        @TempDir tmp: Path,
    ) {
        val live =
            listOf(
                Column(
                    1,
                    0,
                    ColumnDef("l", ColType.LIST, children = listOf(ColumnDef("element", ColType.LONG))),
                    children = listOf(Column(2, 0, ColumnDef("element", ColType.LONG))),
                ),
            )
        val schema =
            MessageType(
                "m",
                listOf<Type>(
                    Types.optionalList()
                        .setElementType(
                            Types.optional(PrimitiveType.PrimitiveTypeName.INT64).id(2).named("element"),
                        )
                        .id(1)
                        .named("l"),
                ),
            )
        val src = tmp.resolve("fat-row.parquet")
        write(schema, src) { f ->
            val g = f.newGroup()
            val list = g.addGroup(0)
            repeat(64) { i -> list.addGroup(0).add(0, i.toLong()) }
            listOf(g)
        }
        // A budget of 8 stands in for the production million: the
        // behaviour under test is the refusal, not the constant.
        assertThatThrownBy {
            ParquetRewriter.rewrite(
                listOf(ParquetRewriter.Input(src, 0L, null)),
                live,
                emptyList(),
                tmp.resolve("o4.parquet"),
                maxNodesPerRow = 8,
            )
        }
            .isInstanceOf(InvalidDataException::class.java)
            .hasMessageContaining("more than 8 nodes")
    }

    @Test
    fun `an ordinary row passes the default node budget`(
        @TempDir tmp: Path,
    ) {
        val live = listOf(Column(1, 0, ColumnDef("a", ColType.LONG)))
        val schema =
            MessageType(
                "m",
                listOf<Type>(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).id(1).named("a")),
            )
        val src = tmp.resolve("thin-row.parquet")
        write(schema, src) { f ->
            (0 until 3).map { i -> f.newGroup().also { it.add(0, i.toLong()) } }
        }
        val out = rewrite(src, live, tmp.resolve("o5.parquet"))
        assertThat(out.rowsWritten).isEqualTo(3)
    }

    // ---- the node cap, on every DDL path --------------------------------

    @Test
    fun `the node cap counts nodes, not roots`() {
        // One root, MAX_COLUMN_NODES fields: a cap counted by roots would
        // read this as "1 column" and wave it through.
        val fat =
            ColumnDef(
                "s",
                ColType.STRUCT,
                children = (0 until ColumnTrees.MAX_COLUMN_NODES).map { ColumnDef("f$it", ColType.LONG) },
            )
        assertThatThrownBy { ColumnTrees.validate(listOf(fat)) }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("too many columns")
        assertThat(nodeCount(listOf(fat))).isEqualTo(ColumnTrees.MAX_COLUMN_NODES + 1)
    }

    @Test
    fun `the node cap is exact at the boundary`() {
        val exactly =
            ColumnDef(
                "s",
                ColType.STRUCT,
                children =
                    (0 until ColumnTrees.MAX_COLUMN_NODES - 1).map { ColumnDef("f$it", ColType.LONG) },
            )
        ColumnTrees.validate(listOf(exactly)) // MAX_COLUMN_NODES exactly: legal
    }

    @Test
    fun `an alter caps the POST-GRAFT total, not the addition alone`() {
        // The addition is one node. Under a cap on the addition alone
        // this is legal no matter how full the table already is.
        val one = listOf(ColumnDef("added", ColType.LONG))
        ColumnTrees.validate(one, existingNodes = ColumnTrees.MAX_COLUMN_NODES - 1)
        assertThatThrownBy { ColumnTrees.validate(one, existingNodes = ColumnTrees.MAX_COLUMN_NODES) }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("too many columns")
    }

    // ---- the depth check survives what it exists to refuse ---------------

    @Test
    fun `a pathologically deep request is a named refusal, not a stack overflow`() {
        var def = ColumnDef("leaf", ColType.LONG)
        repeat(20_000) { def = ColumnDef("s", ColType.STRUCT, children = listOf(def)) }
        assertThatThrownBy { ColumnTrees.validate(listOf(def)) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        // The node cap fires first on this shape, which is the point:
        // both walks are iterative, so neither dies on the way to the
        // refusal.
        // The cap is an EARLY BAIL, not a correctness guard: the walk is
        // iterative either way, so raising it changes only the work
        // done. Mutation testing confirmed as much — replacing
        // ColumnTrees' cap with Int.MAX_VALUE kills nothing, and that is
        // an equivalent mutant rather than a missing assertion. What is
        // asserted is the bail itself.
        assertThat(columnDefDepth(listOf(def), cap = 12)).isEqualTo(12)
        // And the message says "or more" when the walk BAILED, rather
        // than quoting a depth it stopped short of measuring.
        var deep: ColumnDef = ColumnDef("leaf", ColType.LONG)
        repeat(MAX_COLUMN_NESTING_DEPTH + 4) { deep = ColumnDef("s", ColType.STRUCT, children = listOf(deep)) }
        assertThatThrownBy { ColumnTrees.validate(listOf(deep)) }.hasMessageContaining("or more")
        // Exactly one past the cap: the exact depth IS known there, so
        // it is reported rather than approximated.
        var justOver: ColumnDef = ColumnDef("leaf", ColType.LONG)
        repeat(MAX_COLUMN_NESTING_DEPTH) { justOver = ColumnDef("s", ColType.STRUCT, children = listOf(justOver)) }
        assertThatThrownBy { ColumnTrees.validate(listOf(justOver)) }
            .hasMessageContaining("depth ${MAX_COLUMN_NESTING_DEPTH + 1} exceeds")
        assertThat(nodeCount(listOf(def))).isEqualTo(20_001)
    }

    @Test
    fun `columnDefDepth reports the exact depth below its cap`() {
        var def = ColumnDef("leaf", ColType.LONG)
        repeat(4) { def = ColumnDef("s", ColType.STRUCT, children = listOf(def)) }
        assertThat(columnDefDepth(listOf(def))).isEqualTo(5)
        assertThat(columnDefDepth(listOf(def), cap = 99)).isEqualTo(5)
    }

    // ---- the receipt codec is total over garbage -------------------------

    @Test
    fun `an unreadable receipt is one typed error, never a raw NPE`() {
        val corpus =
            listOf(
                "",
                "not json at all",
                "[]",
                "null",
                """{"version":1}""",
                """{"version":1,"namespace":"n","name":"t"}""",
                """{"version":1,"namespace":"n","name":"t","columns":{}}""",
                """{"version":1,"namespace":"n","name":"t","columns":[{}]}""",
                """{"version":1,"namespace":"n","name":"t","columns":[{"name":"c"}]}""",
                """{"version":1,"namespace":"n","name":"t","columns":[{"name":"c","type":"nope"}]}""",
                """{"version":1,"name":"t","columns":[]}""",
                """{"version":99,"namespace":"n","name":"t","columns":[]}""",
                // Integral, in range for a LONG, and `asInt()` narrows
                // it to 1 — a valid-looking version that walks straight
                // past the range check.
                """{"version":4294967297,"namespace":"n","name":"t","columns":[]}""",
                """{"version":-4294967295,"namespace":"n","name":"t","columns":[]}""",
                // `asInt()` answers 0 for a string — the same 0 that
                // means "the pre-versioned shape" — so this used to
                // decode silently with version-0 spellings.
                """{"version":"2","namespace":"n","name":"t","columns":[]}""",
                """{"version":true,"namespace":"n","name":"t","columns":[]}""",
                """{"version":{},"namespace":"n","name":"t","columns":[]}""",
                """{"version":2,"namespace":"n","name":"t",""" +
                    """"columns":[{"name":"c","type":"long","children":7}]}""",
                """{"version":1,"namespace":"n","name":"t",""" +
                    """"columns":[{"name":"c","type":"decimal","type_params":"nope"}]}""",
                """{"version":1,"namespace":"n","name":"t",""" +
                    """"columns":[{"name":"c","type":"decimal","type_params":[1,2]}]}""",
                """{"version":1,"namespace":"n","name":"t",""" +
                    """"columns":[{"name":"c","type":"decimal","type_params":7}]}""",
                """{"version":1,"namespace":"n","name":"t",""" +
                    """"columns":[{"name":"c","type":"decimal","type_params":true}]}""",
                // `asBoolean()` coerces every one of these to FALSE,
                // which for `nullable` meant a corrupt receipt silently
                // became a REQUIRED-column definition and published.
                """{"version":1,"namespace":"n","name":"t",""" +
                    """"columns":[{"name":"c","type":"long","nullable":"yes"}]}""",
                """{"version":1,"namespace":"n","name":"t",""" +
                    """"columns":[{"name":"c","type":"long","nullable":1}]}""",
                """{"version":1,"namespace":"n","name":"t",""" +
                    """"columns":[{"name":"c","type":"long","nullable":null}]}""",
                """{"version":1,"namespace":"n","name":"t",""" +
                    """"columns":[{"name":"c","type":"long","nullable":{}}]}""",
                // ...and one level down, where a nested definition lives.
                """{"version":2,"namespace":"n","name":"t","columns":[{"name":"s","type":"struct",""" +
                    """"children":[{"name":"f","type":"long","nullable":"no"}]}]}""",
            )
        for (blob in corpus) {
            assertThatThrownBy { TableCreationDefinitionCodec.decode(blob, "operation deadbeef") }
                .describedAs("decoding %s", blob)
                .isInstanceOf(CorruptDefinitionException::class.java)
                // The KDoc promises it says WHICH receipt; a stack trace
                // pointing at an `.asText()` call does not help an
                // operator find one row among millions.
                .hasMessageContaining("operation deadbeef")
        }
    }

    @Test
    fun `a non-object type_params is named, not left to Jackson`() {
        // The guard's whole value is the DIAGNOSTIC: every non-object
        // node already throws inside convertValue, so removing it still
        // produces a refusal — just a generic one. Asserting the
        // refusal therefore proves nothing about the guard, which is why
        // this asserts the message.
        for (blob in listOf("[1,2]", "7", "true", "\"nope\"", "[]")) {
            assertThatThrownBy {
                TableCreationDefinitionCodec.decode(
                    """{"version":1,"namespace":"n","name":"t",""" +
                        """"columns":[{"name":"c","type":"decimal","type_params":$blob}]}""",
                    "operation deadbeef",
                )
            }
                .describedAs("type_params = %s", blob)
                .isInstanceOf(CorruptDefinitionException::class.java)
                .hasMessageContaining("non-object 'type_params'")
                .hasMessageContaining("column 'c'")
        }
    }

    @Test
    fun `every stored echo in a corrupt-receipt message is capped`() {
        // The message goes into a 500 body and every log line that
        // records one. A stored column name is Identifiers-shaped in a
        // healthy row — but this codec exists for rows that are NOT what
        // they should be, so "bounded" has to mean bounded everywhere.
        val huge = "x".repeat(5_000)
        val blobs =
            listOf(
                """{"version":1,"namespace":"n","name":"t",""" +
                    """"columns":[{"name":"$huge","type":"nope"}]}""",
                """{"version":1,"namespace":"n","name":"t",""" +
                    """"columns":[{"name":"c","type":"$huge"}]}""",
                """{"version":1,"namespace":"n","name":"t",""" +
                    """"columns":[{"name":"$huge","type":"long","nullable":"$huge"}]}""",
                """{"version":1,"namespace":"n","name":"t",""" +
                    """"columns":[{"name":"$huge","type":"decimal","type_params":"$huge"}]}""",
                """{"version":"$huge","namespace":"n","name":"t","columns":[]}""",
            )
        for (blob in blobs) {
            val thrown = catchThrowable { TableCreationDefinitionCodec.decode(blob, "operation deadbeef") }
            assertThat(thrown).isInstanceOf(CorruptDefinitionException::class.java)
            assertThat(thrown.message!!.length)
                .describedAs("message for %s...", blob.take(60))
                .isLessThan(400)
        }
    }

    @Test
    fun `a 422 for a decimal parameter does not echo the caller's blob`() {
        val huge = "y".repeat(5_000)
        val thrown =
            catchThrowable {
                ColumnTrees.validate(listOf(ColumnDef("d", ColType.DECIMAL, mapOf("precision" to huge))))
            }
        assertThat(thrown).isInstanceOf(HoglakeException.Validation::class.java)
        assertThat(thrown.message!!.length).isLessThan(200)
    }

    @Test
    fun `a sound receipt still decodes, nullable and all`() {
        // The guard must not turn the legal shapes into refusals: absent
        // means nullable, and both booleans are readable.
        val ok =
            TableCreationDefinitionCodec.decode(
                """{"version":2,"namespace":"n","name":"t","columns":[""" +
                    """{"name":"a","type":"long"},""" +
                    """{"name":"b","type":"long","nullable":false},""" +
                    """{"name":"s","type":"struct","children":[{"name":"f","type":"long","nullable":true}]}]}""",
                "operation deadbeef",
            )
        assertThat(ok.columns.map { it.name to it.nullable })
            .containsExactly("a" to true, "b" to false, "s" to true)
        assertThat(ok.columns[2].children!!.single().nullable).isTrue()
    }

    // ---- stats that cannot be true never reach storage -------------------

    @Test
    fun `an inverted bound pair is dropped, in the type's own order`() {
        val checked =
            StatsSanity.check(
                ColumnStats(
                    1,
                    valueCount = 3,
                    nullCount = 0,
                    nanCount = null,
                    sizeBytes = 8,
                    lowerBound = intLE(500),
                    upperBound = intLE(100),
                ),
                ColType.DATE,
            )
        assertThat(checked.stats.lowerBound).isNull()
        assertThat(checked.stats.upperBound).isNull()
        assertThat(checked.repairs).anyMatch { it.contains("sorts above") }
    }

    @Test
    fun `a negative int bound below a positive one is NOT called inverted`() {
        // -1 is little-endian ff ff ff ff, which byte-compares ABOVE
        // 00 00 00 01. A bytewise inversion check would have deleted
        // this perfectly good pair.
        val checked =
            StatsSanity.check(
                ColumnStats(1, 3, 0, null, 8, intLE(-1), intLE(1)),
                ColType.INT,
            )
        assertThat(checked.repairs).isEmpty()
        assertThat(checked.stats.lowerBound).isEqualTo(intLE(-1))
    }

    @Test
    fun `a bound the catalog type cannot decode is dropped`() {
        val checked =
            StatsSanity.check(
                ColumnStats(1, 3, 0, null, 8, byteArrayOf(1, 2, 3, 4), byteArrayOf(9)),
                ColType.LONG,
            )
        assertThat(checked.stats.lowerBound).isNull()
        assertThat(checked.stats.upperBound).isNull()
        assertThat(checked.repairs).hasSize(2)
    }

    @Test
    fun `null_count above value_count is clamped`() {
        val checked =
            StatsSanity.check(
                ColumnStats(
                    1,
                    valueCount = 2,
                    nullCount = 9,
                    nanCount = null,
                    sizeBytes = null,
                    lowerBound = null,
                    upperBound = null,
                ),
                ColType.LONG,
            )
        assertThat(checked.stats.nullCount).isEqualTo(2)
        assertThat(checked.stats.valueCount).isEqualTo(2)
        assertThat(checked.repairs).anyMatch { it.contains("exceeds value_count") }
    }

    @Test
    fun `an inverted boolean pair is caught in the codec's own order`() {
        // The codec reads ANY nonzero byte as true, so 0xff is true and
        // 0x00 is false. A signed BYTE compare put -1 below 0 and waved
        // the pair through: lower=true, upper=false, stored.
        val checked =
            StatsSanity.check(
                ColumnStats(1, 2, 0, null, null, byteArrayOf(0xFF.toByte()), byteArrayOf(0)),
                ColType.BOOLEAN,
            )
        assertThat(checked.stats.lowerBound).isNull()
        assertThat(checked.stats.upperBound).isNull()
        // ...and the sound direction survives, in both spellings of true.
        for (t in listOf<Byte>(1, 0x7F, 0xFF.toByte())) {
            assertThat(
                StatsSanity.check(
                    ColumnStats(1, 2, 0, null, null, byteArrayOf(0), byteArrayOf(t)),
                    ColType.BOOLEAN,
                ).repairs,
            )
                .describedAs("false..%s", t)
                .isEmpty()
        }
    }

    @Test
    fun `a float pair that only disagrees about the sign of zero is not inverted`() {
        // Kotlin's compareTo is the TOTAL order, which ranks -0.0 below
        // +0.0; IEEE says they are equal. Under the total order this
        // pair read as inverted and both bounds were deleted.
        val plus = intLE(java.lang.Float.floatToRawIntBits(0.0f))
        val minus = intLE(java.lang.Float.floatToRawIntBits(-0.0f))
        assertThat(StatsSanity.check(ColumnStats(1, 2, 0, null, null, plus, minus), ColType.FLOAT).repairs)
            .isEmpty()
        // A genuine inversion is still caught.
        val two = intLE(java.lang.Float.floatToRawIntBits(2.0f))
        val one = intLE(java.lang.Float.floatToRawIntBits(1.0f))
        assertThat(StatsSanity.check(ColumnStats(1, 2, 0, null, null, two, one), ColType.FLOAT).repairs)
            .anyMatch { it.contains("sorts above") }
    }

    @Test
    fun `nan_count cannot exceed the NON-NULL value count`() {
        // NaNs are non-null floating values, so the ceiling is
        // value_count - null_count. The old clamp accepted 10 values /
        // 9 nulls / 10 NaNs.
        val checked =
            StatsSanity.check(ColumnStats(1, 10, 9, 10, null, null, null), ColType.DOUBLE)
        assertThat(checked.stats.nanCount).isEqualTo(1)
        assertThat(checked.repairs).anyMatch { it.contains("non-null value count 1") }
        // At the ceiling exactly: sound.
        assertThat(StatsSanity.check(ColumnStats(1, 10, 9, 1, null, null, null), ColType.DOUBLE).repairs)
            .isEmpty()
    }

    @Test
    fun `nan_count on a type that has no NaN is dropped`() {
        // Iceberg's nan_value_counts is float/double only.
        val checked = StatsSanity.check(ColumnStats(1, 5, 0, 2, null, null, null), ColType.LONG)
        assertThat(checked.stats.nanCount).isNull()
        assertThat(checked.repairs).anyMatch { it.contains("has no NaN") }
    }

    @Test
    fun `a NaN bound is refused at the commit door, as the footer door refuses it`() {
        // Length alone passes a NaN: it encodes in four or eight bytes
        // like any other value. And compare() treats NaN as equal to
        // everything (it is unordered), so an inverted pair containing
        // one is not caught either. The hydrator's footer path drops any
        // pair with a NaN, so without this a client could publish
        // through the commit door exactly what a footer could never
        // produce — pruning metadata no reader can use.
        val nanF = intLE(java.lang.Float.floatToRawIntBits(Float.NaN))
        val oneF = intLE(java.lang.Float.floatToRawIntBits(1.0f))
        val nanChecked = StatsSanity.check(ColumnStats(1, 2, 0, null, null, nanF, oneF), ColType.FLOAT)
        assertThat(nanChecked.stats.lowerBound).isNull()
        assertThat(nanChecked.repairs).anyMatch { it.contains("is NaN") }

        val nanD =
            java.nio.ByteBuffer.allocate(8).order(java.nio.ByteOrder.LITTLE_ENDIAN)
                .putLong(java.lang.Double.doubleToRawLongBits(Double.NaN)).array()
        val oneD =
            java.nio.ByteBuffer.allocate(8).order(java.nio.ByteOrder.LITTLE_ENDIAN)
                .putLong(java.lang.Double.doubleToRawLongBits(1.0)).array()
        assertThat(StatsSanity.check(ColumnStats(1, 2, 0, null, null, oneD, nanD), ColType.DOUBLE).stats.upperBound)
            .isNull()

        // Infinities and -0.0 are ORDINARY values and must survive: they
        // are orderable, unlike NaN.
        val negZero = intLE(java.lang.Float.floatToRawIntBits(-0.0f))
        val inf = intLE(java.lang.Float.floatToRawIntBits(Float.POSITIVE_INFINITY))
        assertThat(StatsSanity.check(ColumnStats(1, 2, 0, null, null, negZero, inf), ColType.FLOAT).repairs)
            .isEmpty()
    }

    @Test
    fun `bounds on an all-null column are dropped, as the footer path drops them`() {
        // A column with no non-null values has nothing to bound.
        // FooterStats.chunkBounds refuses exactly this — it returns null
        // unless hasNonNullValue() — so without the same rule here the
        // commit door accepted pruning metadata a footer could never
        // produce.
        val checked =
            StatsSanity.check(ColumnStats(1, 10, 10, null, null, intLE(5), intLE(9)), ColType.INT)
        assertThat(checked.stats.lowerBound).isNull()
        assertThat(checked.stats.upperBound).isNull()
        assertThat(checked.repairs).anyMatch { it.contains("10 values and 10 nulls") }
        // One non-null value is enough to bound.
        assertThat(StatsSanity.check(ColumnStats(1, 10, 9, null, null, intLE(5), intLE(9)), ColType.INT).repairs)
            .isEmpty()
        // And a bound-less all-null row is perfectly ordinary.
        assertThat(StatsSanity.check(ColumnStats(1, 10, 10, null, null, null, null), ColType.INT).repairs)
            .isEmpty()
    }

    @Test
    fun `the undecodable diagnostic does not blame the length of a NaN`() {
        // The one line an operator gets has to be true. "not decodable
        // as 'float' (4 bytes)" names the property that was CORRECT and
        // sends them hunting a length bug that does not exist.
        val nan = intLE(java.lang.Float.floatToRawIntBits(Float.NaN))
        val repairs = StatsSanity.check(ColumnStats(1, 2, 0, null, null, nan, intLE(1)), ColType.FLOAT).repairs
        assertThat(repairs).anyMatch { it.contains("is NaN") }
        assertThat(repairs).noneMatch { it.contains("4 bytes") }
        // A genuine length fault still says so.
        assertThat(StatsSanity.check(ColumnStats(1, 2, 0, null, null, byteArrayOf(1), intLE(1)), ColType.FLOAT).repairs)
            .anyMatch { it.contains("1 bytes") }
    }

    @Test
    fun `a sound stats row passes through untouched`() {
        val ok = ColumnStats(1, 10, 2, null, 64, intLE(1), intLE(9))
        val checked = StatsSanity.check(ok, ColType.INT)
        assertThat(checked.repairs).isEmpty()
        assertThat(checked.stats).isSameAs(ok)
    }

    @Test
    fun `an empty decimal bound is refused as undecodable`() {
        val checked =
            StatsSanity.check(ColumnStats(1, 1, 0, null, null, ByteArray(0), byteArrayOf(5)), ColType.DECIMAL)
        assertThat(checked.stats.lowerBound).isNull()
        assertThat(checked.stats.upperBound).isEqualTo(byteArrayOf(5))
    }

    @Test
    fun `an empty string bound is a legal value, not a repair`() {
        val ok = ColumnStats(1, 1, 0, null, null, ByteArray(0), byteArrayOf(0x7a))
        assertThat(StatsSanity.check(ok, ColType.STRING).repairs).isEmpty()
    }

    // ---- helpers ---------------------------------------------------------

    private fun intLE(v: Int): ByteArray = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(v).array()

    private fun rewrite(
        src: Path,
        live: List<Column>,
        out: Path,
    ) = ParquetRewriter.rewrite(listOf(ParquetRewriter.Input(src, 0L, null)), live, emptyList(), out)

    private fun write(
        schema: MessageType,
        path: Path,
        rows: (SimpleGroupFactory) -> List<Group>,
    ) {
        Files.createDirectories(path.parent)
        val f = SimpleGroupFactory(schema)
        ExampleParquetWriter.builder(LocalOutputFile(path))
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .build()
            .use { w -> rows(f).forEach { w.write(it) } }
    }
}
