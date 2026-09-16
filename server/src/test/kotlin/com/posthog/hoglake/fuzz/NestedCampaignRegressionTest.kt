package com.posthog.hoglake.fuzz

import com.posthog.hoglake.compaction.InvalidDataException
import com.posthog.hoglake.compaction.ParquetRewriter
import com.posthog.hoglake.compaction.UnconvertibleSchemaException
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.HoglakeException
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
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
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
        val schema =
            MessageType(
                "m",
                listOf<Type>(
                    Types.optional(PrimitiveType.PrimitiveTypeName.INT64).id(1).named("a"),
                    Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .`as`(LogicalTypeAnnotation.stringType())
                        .id(77)
                        .named(ParquetRewriter.ROW_ID_COLUMN),
                ),
            )
        val src = tmp.resolve("wrong-type.parquet")
        write(schema, src) { f ->
            val g = f.newGroup()
            g.add(0, 5L)
            g.add(1, Binary.fromString("not-a-row-id"))
            listOf(g)
        }
        assertThatThrownBy { rewrite(src, live, tmp.resolve("o1.parquet")) }
            .isInstanceOf(UnconvertibleSchemaException::class.java)
            .hasMessageContaining("reserved row-id position")
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
        assertThat(columnDefDepth(listOf(def), cap = 12)).isEqualTo(12)
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
                """{"version":2,"namespace":"n","name":"t",""" +
                    """"columns":[{"name":"c","type":"long","children":7}]}""",
                """{"version":1,"namespace":"n","name":"t",""" +
                    """"columns":[{"name":"c","type":"decimal","type_params":"nope"}]}""",
            )
        for (blob in corpus) {
            assertThatThrownBy { TableCreationDefinitionCodec.decode(blob) }
                .describedAs("decoding %s", blob)
                .isInstanceOf(CorruptDefinitionException::class.java)
        }
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
