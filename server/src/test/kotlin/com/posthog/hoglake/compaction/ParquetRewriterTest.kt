package com.posthog.hoglake.compaction

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.nio.file.Files
import java.nio.file.Path

/**
 * ParquetRewriter unit coverage against real local parquet files (no
 * containers): explicit row-id materialization across NON-ADJACENT
 * inputs, deletion-vector application (mid-file holes, fully deleted
 * inputs, empty survivor sets, DV/file mismatch refusal), sort-order
 * application with null placement, the live-schema mapping
 * (add null-fills, promote up-casts, drop drops, id-less name
 * fallback), unconvertible refusal, and the re-compaction path (an
 * input that already carries _hog_row_id keeps its ids).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ParquetRewriterTest {
    private val tmp: Path = Files.createTempDirectory("rewriter-test")

    @AfterAll
    fun tearDown() {
        tmp.toFile().deleteRecursively()
    }

    // The live schema most tests rewrite under:
    // id long (field 1), name string (2), score double (3).
    private val liveColumns =
        listOf(
            Column(1, 0, ColumnDef("id", ColType.LONG, nullable = false)),
            Column(2, 1, ColumnDef("name", ColType.STRING)),
            Column(3, 2, ColumnDef("score", ColType.DOUBLE)),
        )

    private fun schema(withIds: Boolean): MessageType {
        fun <T : Types.Builder<T, out org.apache.parquet.schema.Type>> T.maybeId(id: Int): T =
            if (withIds) this.id(id) else this
        return Types.buildMessage()
            .addField(Types.required(PrimitiveTypeName.INT64).maybeId(1).named("id"))
            .addField(
                Types.optional(PrimitiveTypeName.BINARY)
                    .`as`(LogicalTypeAnnotation.stringType()).maybeId(2).named("name"),
            )
            .addField(Types.optional(PrimitiveTypeName.DOUBLE).maybeId(3).named("score"))
            .named("t")
    }

    private data class TestRow(val id: Long, val name: String?, val score: Double?)

    private fun writeInput(
        fileName: String,
        rows: List<TestRow>,
        withIds: Boolean = true,
    ): Path {
        val path = tmp.resolve(fileName)
        val schema = schema(withIds)
        val factory = SimpleGroupFactory(schema)
        ExampleParquetWriter.builder(LocalOutputFile(path))
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build()
            .use { w ->
                for (r in rows) {
                    val g = factory.newGroup()
                    g.add("id", r.id)
                    r.name?.let { g.add("name", it) }
                    r.score?.let { g.add("score", it) }
                    w.write(g)
                }
            }
        return path
    }

    /** Arbitrary-schema writer for the heterogeneous/unconvertible cases. */
    private fun writeCustom(
        fileName: String,
        schema: MessageType,
        rows: List<(Group) -> Unit>,
    ): Path {
        val path = tmp.resolve(fileName)
        val factory = SimpleGroupFactory(schema)
        ExampleParquetWriter.builder(LocalOutputFile(path))
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build()
            .use { w ->
                for (fill in rows) {
                    val g = factory.newGroup()
                    fill(g)
                    w.write(g)
                }
            }
        return path
    }

    private fun dv(vararg positions: Long): DeletionVector =
        PuffinDeletionVector.read(PuffinTestFiles.deletionVector(positions.toList()))

    private data class OutRow(val id: Long, val name: String?, val score: Double?, val rowId: Long)

    private fun readOutput(path: Path): Pair<MessageType, List<OutRow>> {
        val out = mutableListOf<OutRow>()
        lateinit var schema: MessageType
        ParquetFileReader.open(LocalInputFile(path)).use { reader ->
            schema = reader.footer.fileMetaData.schema
            val columnIO = ColumnIOFactory().getColumnIO(schema)
            var pages = reader.readNextRowGroup()
            while (pages != null) {
                val rr = columnIO.getRecordReader(pages, GroupRecordConverter(schema))
                repeat(Math.toIntExact(pages.rowCount)) {
                    val g: Group = rr.read()
                    out +=
                        OutRow(
                            id = g.getLong(schema.getFieldIndex("id"), 0),
                            name =
                                if (g.getFieldRepetitionCount(schema.getFieldIndex("name")) == 0) {
                                    null
                                } else {
                                    g.getString(schema.getFieldIndex("name"), 0)
                                },
                            score =
                                if (g.getFieldRepetitionCount(schema.getFieldIndex("score")) == 0) {
                                    null
                                } else {
                                    g.getDouble(schema.getFieldIndex("score"), 0)
                                },
                            rowId = g.getLong(schema.getFieldIndex(ParquetRewriter.ROW_ID_COLUMN), 0),
                        )
                }
                pages = reader.readNextRowGroup()
            }
        }
        return schema to out
    }

    @Test
    fun `merges non-adjacent inputs preserving input row ids in the explicit column`() {
        // Row-id ranges 0..2 and 10..12 — deliberately NOT adjacent.
        val a =
            writeInput("a.parquet", listOf(TestRow(100, "x", 1.0), TestRow(101, null, 2.0), TestRow(102, "z", null)))
        val b = writeInput("b.parquet", listOf(TestRow(200, "m", 3.0), TestRow(201, "n", 4.0), TestRow(202, "o", 5.0)))
        val out = tmp.resolve("out1.parquet")
        val result =
            ParquetRewriter.rewrite(
                listOf(ParquetRewriter.Input(a, 0), ParquetRewriter.Input(b, 10)),
                liveColumns,
                emptyList(),
                out,
            )
        assertThat(result.rowsWritten).isEqualTo(6)
        assertThat(result.minRowId).isEqualTo(0)
        val (schema, rows) = readOutput(out)
        assertThat(rows.map { it.rowId }).containsExactly(0L, 1L, 2L, 10L, 11L, 12L)
        assertThat(rows.map { it.id }).containsExactly(100L, 101L, 102L, 200L, 201L, 202L)
        assertThat(rows[1].name).isNull()
        assertThat(rows[2].score).isNull()
        // Every column carries its field id; the row-id column the reserved one.
        assertThat(schema.getType("id").id.intValue()).isEqualTo(1)
        assertThat(schema.getType("name").id.intValue()).isEqualTo(2)
        assertThat(schema.getType("score").id.intValue()).isEqualTo(3)
        assertThat(schema.getType(ParquetRewriter.ROW_ID_COLUMN).id.intValue())
            .isEqualTo(ParquetRewriter.ROW_ID_FIELD_ID)
    }

    @Test
    fun `applies a mid-file deletion vector - survivors keep their original ids`() {
        val a =
            writeInput(
                "dv-a.parquet",
                listOf(TestRow(100, "a", 1.0), TestRow(101, "b", 2.0), TestRow(102, "c", 3.0), TestRow(103, "d", 4.0)),
            )
        val b = writeInput("dv-b.parquet", listOf(TestRow(200, "m", 5.0), TestRow(201, "n", 6.0)))
        val out = tmp.resolve("dv-out.parquet")
        val result =
            ParquetRewriter.rewrite(
                // Positions 0 and 2 of `a` die: row ids 7 and 9 vanish forever.
                listOf(ParquetRewriter.Input(a, 7, dv(0, 2)), ParquetRewriter.Input(b, 20)),
                liveColumns,
                emptyList(),
                out,
            )
        assertThat(result.rowsWritten).isEqualTo(4)
        assertThat(result.minRowId).isEqualTo(8) // 7 was deleted; min survivor is 8
        val (_, rows) = readOutput(out)
        assertThat(rows.map { it.rowId }).containsExactly(8L, 10L, 20L, 21L)
        assertThat(rows.map { it.id }).containsExactly(101L, 103L, 200L, 201L)
    }

    @Test
    fun `a fully deleted input contributes nothing - all inputs deleted means an empty output`() {
        val a = writeInput("full-a.parquet", listOf(TestRow(1, "a", 1.0), TestRow(2, "b", 2.0)))
        val b = writeInput("full-b.parquet", listOf(TestRow(3, "c", 3.0)))
        val out = tmp.resolve("full-out.parquet")
        val partial =
            ParquetRewriter.rewrite(
                listOf(ParquetRewriter.Input(a, 0, dv(0, 1)), ParquetRewriter.Input(b, 5)),
                liveColumns,
                emptyList(),
                out,
            )
        assertThat(partial.rowsWritten).isEqualTo(1)
        assertThat(partial.minRowId).isEqualTo(5)

        val empty =
            ParquetRewriter.rewrite(
                listOf(ParquetRewriter.Input(a, 0, dv(0, 1)), ParquetRewriter.Input(b, 5, dv(0))),
                liveColumns,
                emptyList(),
                tmp.resolve("empty-out.parquet"),
            )
        assertThat(empty.rowsWritten).isZero()
        assertThat(empty.minRowId).isNull()
        val (schema, rows) = readOutput(tmp.resolve("empty-out.parquet"))
        assertThat(rows).isEmpty()
        assertThat(schema.getType(ParquetRewriter.ROW_ID_COLUMN).id.intValue())
            .isEqualTo(ParquetRewriter.ROW_ID_FIELD_ID)
    }

    @Test
    fun `a DV position beyond the file refuses the rewrite instead of losing the delete`() {
        val a = writeInput("oob-a.parquet", listOf(TestRow(1, "a", 1.0), TestRow(2, "b", 2.0)))
        assertThatThrownBy {
            ParquetRewriter.rewrite(
                listOf(ParquetRewriter.Input(a, 0, dv(1, 17))),
                liveColumns,
                emptyList(),
                tmp.resolve("oob-out.parquet"),
            )
        }.isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("refusing a lossy compaction")
    }

    @Test
    fun `sorts merged rows by the sort spec with null placement, ids ride along`() {
        val a = writeInput("s-a.parquet", listOf(TestRow(1, "e", 5.0), TestRow(2, "d", null), TestRow(3, "c", 1.0)))
        val b = writeInput("s-b.parquet", listOf(TestRow(4, "b", 4.0), TestRow(5, "a", null), TestRow(6, "f", 0.5)))
        val out = tmp.resolve("out2.parquet")
        ParquetRewriter.rewrite(
            listOf(ParquetRewriter.Input(a, 0), ParquetRewriter.Input(b, 100)),
            liveColumns,
            listOf(SortFieldDef(3, SortDirection.ASC, NullOrder.NULLS_LAST)),
            out,
        )
        val (_, rows) = readOutput(out)
        assertThat(rows.map { it.score }).containsExactly(0.5, 1.0, 4.0, 5.0, null, null)
        // Nulls keep row-id order among themselves (stable sort): 1 then 101.
        assertThat(rows.map { it.rowId }).containsExactly(102L, 2L, 100L, 0L, 1L, 101L)
    }

    @Test
    fun `descending with nulls first`() {
        val a = writeInput("d-a.parquet", listOf(TestRow(1, "a", 1.0), TestRow(2, "b", null), TestRow(3, "c", 9.0)))
        val out = tmp.resolve("out3.parquet")
        ParquetRewriter.rewrite(
            listOf(ParquetRewriter.Input(a, 0)),
            liveColumns,
            listOf(SortFieldDef(3, SortDirection.DESC, NullOrder.NULLS_FIRST)),
            out,
        )
        val (_, rows) = readOutput(out)
        assertThat(rows.map { it.score }).containsExactly(null, 9.0, 1.0)
    }

    @Test
    fun `inputs without embedded field ids bind to live columns by name`() {
        val a = writeInput("no-ids.parquet", listOf(TestRow(1, "a", 1.0)), withIds = false)
        val out = tmp.resolve("out4.parquet")
        ParquetRewriter.rewrite(listOf(ParquetRewriter.Input(a, 0)), liveColumns, emptyList(), out)
        val (schema, rows) = readOutput(out)
        assertThat(schema.getType("id").id.intValue()).isEqualTo(1)
        assertThat(schema.getType("score").id.intValue()).isEqualTo(3)
        assertThat(rows.single().id).isEqualTo(1L)
        assertThat(rows.single().name).isEqualTo("a")
    }

    @Test
    fun `heterogeneous inputs map by field id - add null-fills, promote up-casts, drop drops`() {
        // Vintage 1: id was INT (int32), plus a since-dropped column
        // 'old' (field 9). Vintage 2: id promoted to long, 'score'
        // (field 3) added. Live schema: id long (1), name string (2),
        // score double (3).
        val v1Schema =
            Types.buildMessage()
                .addField(Types.optional(PrimitiveTypeName.INT32).id(1).named("id"))
                .addField(
                    Types.optional(PrimitiveTypeName.BINARY)
                        .`as`(LogicalTypeAnnotation.stringType()).id(2).named("name"),
                )
                .addField(
                    Types.optional(PrimitiveTypeName.BINARY)
                        .`as`(LogicalTypeAnnotation.stringType()).id(9).named("old"),
                )
                .named("t")
        val v1 =
            writeCustom(
                "het-v1.parquet",
                v1Schema,
                listOf(
                    { g ->
                        g.add("id", 10)
                        g.add("name", "x")
                        g.add("old", "dead-data")
                    },
                    { g ->
                        g.add("id", 20)
                        g.add("old", "more-dead-data")
                    },
                ),
            )
        val v2 = writeInput("het-v2.parquet", listOf(TestRow(30, "z", 1.5)))

        val out = tmp.resolve("het-out.parquet")
        val result =
            ParquetRewriter.rewrite(
                listOf(ParquetRewriter.Input(v1, 0), ParquetRewriter.Input(v2, 2)),
                liveColumns,
                emptyList(),
                out,
            )
        assertThat(result.rowsWritten).isEqualTo(3)
        val (schema, rows) = readOutput(out)
        // The output is the LIVE schema: dropped 'old' is gone entirely.
        assertThat(schema.fields.map { it.name })
            .containsExactly("id", "name", "score", ParquetRewriter.ROW_ID_COLUMN)
        assertThat(schema.getType("id").asPrimitiveType().primitiveTypeName)
            .isEqualTo(PrimitiveTypeName.INT64)
        assertThat(schema.getType("id").id.intValue()).isEqualTo(1)
        assertThat(schema.getType("score").id.intValue()).isEqualTo(3)
        // int32 values up-cast; the added column null-fills for old rows.
        assertThat(rows.map { it.id }).containsExactly(10L, 20L, 30L)
        assertThat(rows.map { it.name }).containsExactly("x", null, "z")
        assertThat(rows.map { it.score }).containsExactly(null, null, 1.5)
        assertThat(rows.map { it.rowId }).containsExactly(0L, 1L, 2L)
    }

    @Test
    fun `float promotes to double on rewrite`() {
        val floatSchema =
            Types.buildMessage()
                .addField(Types.optional(PrimitiveTypeName.INT64).id(1).named("id"))
                .addField(Types.optional(PrimitiveTypeName.FLOAT).id(3).named("score"))
                .named("t")
        val f =
            writeCustom(
                "float.parquet",
                floatSchema,
                listOf({ g ->
                    g.add("id", 1L)
                    g.add("score", 2.5f)
                }),
            )
        val out = tmp.resolve("float-out.parquet")
        ParquetRewriter.rewrite(listOf(ParquetRewriter.Input(f, 0)), liveColumns, emptyList(), out)
        val (schema, rows) = readOutput(out)
        assertThat(schema.getType("score").asPrimitiveType().primitiveTypeName)
            .isEqualTo(PrimitiveTypeName.DOUBLE)
        assertThat(rows.single().score).isEqualTo(2.5)
    }

    @Test
    fun `a live column unproducible from the input type is refused as unconvertible`() {
        // Live 'id' is long; the file stores field 1 as a string — no
        // promotion covers that.
        val badSchema =
            Types.buildMessage()
                .addField(
                    Types.optional(PrimitiveTypeName.BINARY)
                        .`as`(LogicalTypeAnnotation.stringType()).id(1).named("id"),
                )
                .named("t")
        val bad = writeCustom("bad.parquet", badSchema, listOf({ g -> g.add("id", "not-a-long") }))
        assertThatThrownBy {
            ParquetRewriter.rewrite(
                listOf(ParquetRewriter.Input(bad, 0)),
                liveColumns,
                emptyList(),
                tmp.resolve("bad-out.parquet"),
            )
        }.isInstanceOf(UnconvertibleSchemaException::class.java)
            .hasMessageContaining("cannot be produced")
    }

    @Test
    fun `an id-less input column with no live name match is dropped, not fatal`() {
        val straySchema =
            Types.buildMessage()
                .addField(Types.optional(PrimitiveTypeName.INT64).id(1).named("id"))
                .addField(Types.optional(PrimitiveTypeName.INT64).named("stray"))
                .named("t")
        val stray =
            writeCustom(
                "stray.parquet",
                straySchema,
                listOf({ g ->
                    g.add("id", 7L)
                    g.add("stray", 99L)
                }),
            )
        val out = tmp.resolve("stray-out.parquet")
        val result =
            ParquetRewriter.rewrite(listOf(ParquetRewriter.Input(stray, 0)), liveColumns, emptyList(), out)
        assertThat(result.rowsWritten).isEqualTo(1)
        val (schema, rows) = readOutput(out)
        assertThat(schema.fields.map { it.name }).doesNotContain("stray")
        assertThat(rows.single().id).isEqualTo(7L)
    }

    @Test
    fun `re-compacting an explicit-row-id input keeps its ids, never positional`() {
        val a = writeInput("r-a.parquet", listOf(TestRow(1, "a", 1.0), TestRow(2, "b", 2.0)))
        val out1 = tmp.resolve("first.parquet")
        ParquetRewriter.rewrite(listOf(ParquetRewriter.Input(a, 40)), liveColumns, emptyList(), out1)
        // Second pass: rowIdStart deliberately WRONG (0) — the ids must come
        // from the file's own _hog_row_id column, not position.
        val out2 = tmp.resolve("second.parquet")
        ParquetRewriter.rewrite(listOf(ParquetRewriter.Input(out1, 0)), liveColumns, emptyList(), out2)
        val (schema, rows) = readOutput(out2)
        assertThat(rows.map { it.rowId }).containsExactly(40L, 41L)
        // And the schema still has exactly one row-id column.
        assertThat(schema.fields.count { it.name == ParquetRewriter.ROW_ID_COLUMN }).isEqualTo(1)
    }

    @Test
    fun `re-compacting an explicit-row-id input with a DV drops by position but keeps ids`() {
        val a = writeInput("rd-a.parquet", listOf(TestRow(1, "a", 3.0), TestRow(2, "b", 1.0), TestRow(3, "c", 2.0)))
        val out1 = tmp.resolve("rd-first.parquet")
        // Sorted first pass: physical order becomes score ASC -> ids 51, 52, 50.
        ParquetRewriter.rewrite(
            listOf(ParquetRewriter.Input(a, 50)),
            liveColumns,
            listOf(SortFieldDef(3, SortDirection.ASC, NullOrder.NULLS_LAST)),
            out1,
        )
        // DV position 0 targets the PHYSICAL first row of the sorted file,
        // which carries id 51 — that is the row that must die.
        val out2 = tmp.resolve("rd-second.parquet")
        val result =
            ParquetRewriter.rewrite(
                listOf(ParquetRewriter.Input(out1, 0, dv(0))),
                liveColumns,
                emptyList(),
                out2,
            )
        assertThat(result.rowsWritten).isEqualTo(2)
        assertThat(result.minRowId).isEqualTo(50)
        val (_, rows) = readOutput(out2)
        assertThat(rows.map { it.rowId }).containsExactly(52L, 50L)
        assertThat(rows.map { it.id }).containsExactly(3L, 1L)
    }

    // ---- the DuckLake scalar-parity types ----------------------------------

    /** Reads the single data column of a one-column output, plus its schema. */
    private fun readSingle(path: Path): Pair<org.apache.parquet.schema.PrimitiveType, List<Group>> {
        val groups = mutableListOf<Group>()
        lateinit var schema: MessageType
        ParquetFileReader.open(LocalInputFile(path)).use { reader ->
            schema = reader.footer.fileMetaData.schema
            val columnIO = ColumnIOFactory().getColumnIO(schema)
            var pages = reader.readNextRowGroup()
            while (pages != null) {
                val rr = columnIO.getRecordReader(pages, GroupRecordConverter(schema))
                repeat(Math.toIntExact(pages.rowCount)) { groups += rr.read() }
                pages = reader.readNextRowGroup()
            }
        }
        return schema.getType(0).asPrimitiveType() to groups
    }

    /** One column named "v" with field id 1, plus the rows to write into it. */
    private fun rewriteOne(
        name: String,
        input: MessageType,
        live: ColType,
        rows: List<(Group) -> Unit>,
        sort: List<SortFieldDef> = emptyList(),
    ): Pair<org.apache.parquet.schema.PrimitiveType, List<Group>> {
        val f = writeCustom("$name-in.parquet", input, rows)
        val out = tmp.resolve("$name-out.parquet")
        ParquetRewriter.rewrite(
            listOf(ParquetRewriter.Input(f, 0)),
            listOf(Column(1, 0, ColumnDef("v", live))),
            sort,
            out,
        )
        return readSingle(out)
    }

    private fun oneColumn(
        physical: PrimitiveTypeName,
        logical: LogicalTypeAnnotation? = null,
    ): MessageType {
        var b = Types.optional(physical)
        if (logical != null) b = b.`as`(logical)
        return Types.buildMessage().addField(b.id(1).named("v")).named("t")
    }

    @Test
    fun `the small int widths rewrite as int32 carrying their INT annotation`() {
        val widths =
            listOf(
                Triple(ColType.INT8, 8, true),
                Triple(ColType.INT16, 16, true),
                Triple(ColType.UINT8, 8, false),
                Triple(ColType.UINT16, 16, false),
            )
        for ((type, width, signed) in widths) {
            val input = oneColumn(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(width, signed))
            val (prim, rows) = rewriteOne("i$width-$signed", input, type, listOf({ g -> g.add("v", 7) }))
            assertThat(prim.primitiveTypeName).describedAs(type.wire).isEqualTo(PrimitiveTypeName.INT32)
            assertThat(prim.logicalTypeAnnotation)
                .describedAs(type.wire)
                .isEqualTo(LogicalTypeAnnotation.intType(width, signed))
            assertThat(rows.single().getInteger(0, 0)).isEqualTo(7)
        }
    }

    @Test
    fun `an int8 file rewrites unchanged under a promoted int16 column`() {
        // The int8 -> int16 promotion is a physical no-op: both are int32.
        val input = oneColumn(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8, true))
        val (prim, rows) = rewriteOne("i8-to-i16", input, ColType.INT16, listOf({ g -> g.add("v", -128) }))
        assertThat(prim.primitiveTypeName).isEqualTo(PrimitiveTypeName.INT32)
        assertThat(rows.single().getInteger(0, 0)).isEqualTo(-128)
    }

    @Test
    fun `uint32 rewrites to a plain INT64, converting an arrow unsigned int32 without sign-extending`() {
        // The whole reason uint32's writer contract is INT64: 0xFFFFFFFF
        // must come out as 4294967295, not -1.
        val input = oneColumn(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(32, false))
        val (prim, rows) = rewriteOne("u32", input, ColType.UINT32, listOf({ g -> g.add("v", -1) }))
        assertThat(prim.primitiveTypeName).isEqualTo(PrimitiveTypeName.INT64)
        assertThat(prim.logicalTypeAnnotation).isNull()
        assertThat(rows.single().getLong(0, 0)).isEqualTo(4_294_967_295L)
    }

    @Test
    fun `an unsigned int32 promoted into a long column zero-extends too`() {
        val input = oneColumn(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(32, false))
        val (prim, rows) = rewriteOne("u32-long", input, ColType.LONG, listOf({ g -> g.add("v", -1) }))
        assertThat(prim.primitiveTypeName).isEqualTo(PrimitiveTypeName.INT64)
        assertThat(rows.single().getLong(0, 0)).isEqualTo(4_294_967_295L)
    }

    @Test
    fun `a SIGNED int32 promoted into a long column still sign-extends`() {
        // The control for the test above: the pre-existing int -> long
        // promotion must keep its meaning.
        val input = oneColumn(PrimitiveTypeName.INT32)
        val (_, rows) = rewriteOne("i32-long", input, ColType.LONG, listOf({ g -> g.add("v", -1) }))
        assertThat(rows.single().getLong(0, 0)).isEqualTo(-1L)
    }

    @Test
    fun `uint64 keeps its native unsigned int64 form`() {
        val input = oneColumn(PrimitiveTypeName.INT64, LogicalTypeAnnotation.intType(64, false))
        val (prim, rows) =
            rewriteOne("u64", input, ColType.UINT64, listOf({ g -> g.add("v", Long.MIN_VALUE) }))
        assertThat(prim.logicalTypeAnnotation).isEqualTo(LogicalTypeAnnotation.intType(64, false))
        // The bit pattern survives; reading it as unsigned is the catalog's job.
        assertThat(rows.single().getLong(0, 0)).isEqualTo(Long.MIN_VALUE)
    }

    @Test
    fun `unsigned sort keys sort unsigned, not signed`() {
        // 2^63 (Long.MIN_VALUE's bits) is the LARGEST uint64, so a signed
        // comparator would sort it first. Pinned because the merged output
        // order is what a sorted table's readers prune against.
        val input = oneColumn(PrimitiveTypeName.INT64, LogicalTypeAnnotation.intType(64, false))
        val (_, rows) =
            rewriteOne(
                "u64-sort",
                input,
                ColType.UINT64,
                listOf(
                    { g -> g.add("v", Long.MIN_VALUE) },
                    { g -> g.add("v", 1L) },
                ),
                sort = listOf(SortFieldDef(1, SortDirection.ASC, NullOrder.NULLS_LAST)),
            )
        assertThat(rows.map { it.getLong(0, 0) }).containsExactly(1L, Long.MIN_VALUE)
    }

    @Test
    fun `timestamp_s and timestamp_ms rewrite as millis, verbatim`() {
        for (type in listOf(ColType.TIMESTAMP_S, ColType.TIMESTAMP_MS)) {
            val input =
                oneColumn(
                    PrimitiveTypeName.INT64,
                    LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS),
                )
            val (prim, rows) = rewriteOne("${type.wire}", input, type, listOf({ g -> g.add("v", -1_500L) }))
            assertThat(prim.logicalTypeAnnotation)
                .describedAs(type.wire)
                .isEqualTo(
                    LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS),
                )
            assertThat(rows.single().getLong(0, 0)).describedAs(type.wire).isEqualTo(-1_500L)
        }
    }

    @Test
    fun `a millis file under a micros timestamp column is refused, not silently scaled`() {
        // There is no legal path to this pairing: PROMOTIONS follows
        // DuckLake's documented table, which has no timestamp rungs, so a
        // millis file can only sit under a micros column if a writer
        // disagreed with its own DDL. Refusing keeps compaction from
        // inventing a unit conversion nobody asked for.
        val input =
            oneColumn(
                PrimitiveTypeName.INT64,
                LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS),
            )
        assertThatThrownBy {
            rewriteOne("ms-under-us", input, ColType.TIMESTAMP, listOf({ g -> g.add("v", 1L) }))
        }.isInstanceOf(UnconvertibleSchemaException::class.java)
            .hasMessageContaining("cannot be produced")
    }

    @Test
    fun `timestamp_ns rewrites as nanos and refuses any other unit`() {
        val nanos =
            oneColumn(
                PrimitiveTypeName.INT64,
                LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.NANOS),
            )
        val (prim, rows) = rewriteOne("ts-ns", nanos, ColType.TIMESTAMP_NS, listOf({ g -> g.add("v", 42L) }))
        assertThat(prim.logicalTypeAnnotation)
            .isEqualTo(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.NANOS))
        assertThat(rows.single().getLong(0, 0)).isEqualTo(42L)

        val micros =
            oneColumn(
                PrimitiveTypeName.INT64,
                LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS),
            )
        assertThatThrownBy {
            rewriteOne("ts-ns-bad", micros, ColType.TIMESTAMP_NS, listOf({ g -> g.add("v", 42L) }))
        }.isInstanceOf(UnconvertibleSchemaException::class.java)
    }

    @Test
    fun `an unsigned source wider than the live type is refused, per width`() {
        // The rewriter's domain gate, which had no coverage at all:
        // deleting it left every other test green, because every OTHER
        // test pairs an unsigned source with a type that can hold it.
        //
        // Each row is (live type, source annotation width): an unsigned
        // leaf of width w is convertible only under a type whose domain
        // contains [0, 2^w). Without the gate these copy through IDENTITY
        // and get re-stamped with the LIVE column's annotation —
        // compaction laundering a file the hydrator refuses to read, and
        // turning "no bounds" into "wrong bounds".
        val refused =
            listOf(
                // int8 holds 127; it cannot take any unsigned width.
                Triple(ColType.INT8, 32, PrimitiveTypeName.INT32),
                Triple(ColType.INT8, 16, PrimitiveTypeName.INT32),
                Triple(ColType.INT8, 8, PrimitiveTypeName.INT32),
                // int16 takes INT(8,u) but not INT(16,u).
                Triple(ColType.INT16, 16, PrimitiveTypeName.INT32),
                // int and date are int32-domain: 16 bits yes, 32 no.
                Triple(ColType.INT, 32, PrimitiveTypeName.INT32),
                Triple(ColType.DATE, 32, PrimitiveTypeName.INT32),
                // uint16 holds 65535; INT(32,u) overflows it.
                Triple(ColType.UINT16, 32, PrimitiveTypeName.INT32),
                // 64-bit unsigned belongs to uint64 alone.
                Triple(ColType.LONG, 64, PrimitiveTypeName.INT64),
                Triple(ColType.TIME, 64, PrimitiveTypeName.INT64),
                Triple(ColType.TIMESTAMP, 64, PrimitiveTypeName.INT64),
                Triple(ColType.UINT32, 64, PrimitiveTypeName.INT64),
            )
        for ((live, width, physical) in refused) {
            val input = oneColumn(physical, LogicalTypeAnnotation.intType(width, false))
            val rows =
                if (physical == PrimitiveTypeName.INT32) {
                    listOf<(Group) -> Unit>({ g -> g.add("v", 1) })
                } else {
                    listOf<(Group) -> Unit>({ g -> g.add("v", 1L) })
                }
            assertThatThrownBy { rewriteOne("refuse-${live.wire}-u$width", input, live, rows) }
                .describedAs("%s must refuse an INT(%d, unsigned) source", live.wire, width)
                .isInstanceOf(UnconvertibleSchemaException::class.java)
                .hasMessageContaining("cannot be produced")
        }
    }

    @Test
    fun `an unsigned source the live type CAN hold still converts`() {
        // The control: the gate must bite on width, not on the mere
        // presence of an unsigned annotation.
        val allowed =
            listOf(
                Triple(ColType.INT16, 8, PrimitiveTypeName.INT32),
                Triple(ColType.INT, 16, PrimitiveTypeName.INT32),
                Triple(ColType.UINT8, 8, PrimitiveTypeName.INT32),
                Triple(ColType.UINT16, 16, PrimitiveTypeName.INT32),
                Triple(ColType.UINT32, 32, PrimitiveTypeName.INT32),
                Triple(ColType.LONG, 32, PrimitiveTypeName.INT32),
                Triple(ColType.UINT64, 64, PrimitiveTypeName.INT64),
            )
        for ((live, width, physical) in allowed) {
            val input = oneColumn(physical, LogicalTypeAnnotation.intType(width, false))
            val rows =
                if (physical == PrimitiveTypeName.INT32) {
                    listOf<(Group) -> Unit>({ g -> g.add("v", 1) })
                } else {
                    listOf<(Group) -> Unit>({ g -> g.add("v", 1L) })
                }
            val (_, out) = rewriteOne("allow-${live.wire}-u$width", input, live, rows)
            assertThat(out).describedAs("%s accepts INT(%d, unsigned)", live.wire, width).hasSize(1)
        }
    }

    @Test
    fun `json rewrites as BYTE_ARRAY with the JSON annotation, bytes untouched`() {
        val input = oneColumn(PrimitiveTypeName.BINARY, LogicalTypeAnnotation.jsonType())
        val document = """{ "b":1,  "a":[2] }"""
        val (prim, rows) = rewriteOne("json", input, ColType.JSON, listOf({ g -> g.add("v", document) }))
        assertThat(prim.logicalTypeAnnotation).isEqualTo(LogicalTypeAnnotation.jsonType())
        // Verbatim: compaction never re-canonicalizes a document (which is
        // also why json is identity-partition-only).
        assertThat(rows.single().getBinary(0, 0).bytes).isEqualTo(document.toByteArray())
    }

    @Test
    fun `a string file rewrites into a json column and vice versa - both are BYTE_ARRAY`() {
        // Not a promotion (the ALTER path refuses string <-> json), but a
        // group whose live column changed type out from under it must not
        // fail here for a PHYSICAL reason; the type gate lives in ALTER.
        val stringInput = oneColumn(PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType())
        val (prim, _) = rewriteOne("str-json", stringInput, ColType.JSON, listOf({ g -> g.add("v", "{}") }))
        assertThat(prim.logicalTypeAnnotation).isEqualTo(LogicalTypeAnnotation.jsonType())
    }
}
