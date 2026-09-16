package com.posthog.hoglake.compaction

import com.posthog.hoglake.hydrator.CatalogColumn
import com.posthog.hoglake.hydrator.FooterStats
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.stats.IcebergSingleValue
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
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.nio.file.Files
import java.nio.file.Path

/**
 * Nested columns through the whole compaction loop: writer footer ->
 * [ParquetRewriter] -> footer -> rewrite again, with the VALUES and the
 * per-leaf bounds asserted identical at every step.
 *
 * The decision this file pins is that compaction REWRITES nested
 * columns rather than refusing them. The alternative — making a nested
 * schema `unconvertible_schema`, the way heterogeneous types that
 * cannot be produced are — would have been cheap, and permanently
 * wrong: a table with one `map` column could then never be compacted,
 * so its small-file debt would grow forever with no operator lever. The
 * parquet-java Group API is already a tree (`addGroup`/`getGroup`), so
 * the existing plan-and-copy pipeline extends one level at a time.
 *
 * Idempotence is asserted by feeding the output back in, because that
 * IS the production sequence (a tier-1 output is a tier-2 input) and a
 * per-pass drift — an entry duplicated, an empty list turning into a
 * null, a bound widening — would be invisible in a single pass.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NestedTypeRewriteRoundTripTest {
    private val tmp: Path = Files.createTempDirectory("nested-roundtrip")

    @AfterAll
    fun tearDown() {
        tmp.toFile().deleteRecursively()
    }

    // ---- the live schemas under test --------------------------------------

    /** `l list<int>` — field 1 with element 2. */
    private val listColumn =
        Column(1, 0, ColumnDef("l", ColType.LIST), listOf(Column(2, 0, ColumnDef("element", ColType.INT))))

    /** `s struct{a int, b string}` — field 1 with leaves 2 and 3. */
    private val structColumn =
        Column(
            1,
            0,
            ColumnDef("s", ColType.STRUCT),
            listOf(
                Column(2, 0, ColumnDef("a", ColType.INT)),
                Column(3, 1, ColumnDef("b", ColType.STRING)),
            ),
        )

    /** `m map<string, long>` — field 1 with key 2 and value 3. */
    private val mapColumn =
        Column(
            1,
            0,
            ColumnDef("m", ColType.MAP),
            listOf(
                Column(2, 0, ColumnDef("key", ColType.STRING, nullable = false)),
                Column(3, 1, ColumnDef("value", ColType.LONG)),
            ),
        )

    /** `d struct{ runs list<struct{ score double }> }` — leaves: 5. */
    private val deepColumn =
        Column(
            1,
            0,
            ColumnDef("d", ColType.STRUCT),
            listOf(
                Column(
                    2,
                    0,
                    ColumnDef("runs", ColType.LIST),
                    listOf(
                        Column(
                            3,
                            0,
                            ColumnDef("element", ColType.STRUCT),
                            listOf(Column(5, 0, ColumnDef("score", ColType.DOUBLE))),
                        ),
                    ),
                ),
            ),
        )

    // ---- round trips -------------------------------------------------------

    @Test
    fun `a list survives compaction with its values, its empties and its nulls`() {
        // Empty list vs. NULL list is a real distinction in parquet (and
        // in every reader), and it is the one a naive copy loses: an
        // `addGroup` for a null column, or a skipped one for an empty
        // list, and both collapse to the same thing.
        val rows =
            listOf(
                listOf(1, 2, 3),
                emptyList(),
                null,
                listOf(-7),
            )
        val schema = listSchema()
        val path =
            write("list-in", schema) { g, i ->
                val value = rows[i]
                if (value != null) {
                    val outer = g.addGroup(0)
                    for (v in value) outer.addGroup(0).add(0, v)
                }
            }
        val trip = roundTrip("list", path, listOf(listColumn))
        for (out in trip) {
            assertThat(readLists(out)).describedAs("values in %s", out.fileName).isEqualTo(rows)
        }
    }

    @Test
    fun `a struct survives compaction, with a null struct staying null`() {
        val rows = listOf(5 to "x", -2 to "y", null, 9 to "w")
        val schema = structSchema()
        val path =
            write("struct-in", schema) { g, i ->
                val value = rows[i]
                if (value != null) {
                    val inner = g.addGroup(0)
                    inner.add(0, value.first)
                    inner.add(1, value.second)
                }
            }
        val trip = roundTrip("struct", path, listOf(structColumn))
        for (out in trip) {
            assertThat(readStructs(out)).describedAs("values in %s", out.fileName).isEqualTo(rows)
        }
    }

    @Test
    fun `a map survives compaction, entries and order intact`() {
        val rows =
            listOf(
                listOf("a" to 10L, "b" to -1L),
                emptyList(),
                null,
                listOf("c" to (1L shl 40)),
            )
        val schema = mapSchema()
        val path =
            write("map-in", schema) { g, i ->
                val value = rows[i]
                if (value != null) {
                    val outer = g.addGroup(0)
                    for ((k, v) in value) {
                        val entry = outer.addGroup(0)
                        entry.add(0, k)
                        entry.add(1, v)
                    }
                }
            }
        val trip = roundTrip("map", path, listOf(mapColumn))
        for (out in trip) {
            assertThat(readMaps(out)).describedAs("values in %s", out.fileName).isEqualTo(rows)
        }
    }

    @Test
    fun `a struct of a list of structs survives compaction`() {
        // Three container levels, which is where an index confusion
        // between the output schema and the plan would finally show.
        val rows = listOf(listOf(1.5, -2.5), emptyList(), null, listOf(0.0))
        val schema = deepSchema()
        val path =
            write("deep-in", schema) { g, i ->
                val value = rows[i]
                if (value != null) {
                    val outer = g.addGroup(0) // d
                    val list = outer.addGroup(0) // runs
                    for (v in value) list.addGroup(0).addGroup(0).add(0, v)
                }
            }
        val trip = roundTrip("deep", path, listOf(deepColumn))
        for (out in trip) {
            assertThat(readDeep(out)).describedAs("values in %s", out.fileName).isEqualTo(rows)
        }
    }

    // ---- the output's SHAPE ------------------------------------------------

    @Test
    fun `the output carries field ids on every level and none on the repetition groups`() {
        val path =
            write("shape-in", mapSchema()) { g, _ ->
                val entry = g.addGroup(0).addGroup(0)
                entry.add(0, "k")
                entry.add(1, 1L)
            }
        val out = tmp.resolve("shape-out.parquet")
        rewrite(path, listOf(mapColumn), out)
        val schema = schemaOf(out)
        val map = schema.getType("m").asGroupType()
        assertThat(map.id.intValue()).isEqualTo(1)
        assertThat(map.logicalTypeAnnotation).isEqualTo(LogicalTypeAnnotation.mapType())
        val entry = map.getType(0).asGroupType()
        // The repetition layer is synthetic: Iceberg has nothing to match
        // an id on it against, so it must not carry one.
        assertThat(entry.id).describedAs("the key_value group carries no field id").isNull()
        assertThat(entry.isRepetition(Type.Repetition.REPEATED)).isTrue()
        assertThat(entry.getType(0).id.intValue()).isEqualTo(2)
        assertThat(entry.getType(1).id.intValue()).isEqualTo(3)
        // Iceberg map keys are non-nullable, and so is the parquet shape.
        assertThat(entry.getType(0).isRepetition(Type.Repetition.REQUIRED))
            .describedAs("the map key is REQUIRED")
            .isTrue()
        // The row-id carrier still rides alongside, unchanged.
        assertThat(schema.getType(ParquetRewriter.ROW_ID_COLUMN).id.intValue())
            .isEqualTo(ParquetRewriter.ROW_ID_FIELD_ID)

        // And the whole schema binds by id: no leaf is id-less, so the
        // hydrator's field-id contract check stays clean.
        assertThat(FooterStats.missingFieldIds(schema))
            .describedAs("a compacted nested file is never flagged id-less")
            .isFalse()
    }

    @Test
    fun `explicit row ids survive a nested rewrite and its re-compaction`() {
        // The lineage guarantee does not get a pass because the schema is
        // complicated: every survivor still carries its own id, and a
        // second pass reads the ids out of the file rather than assigning
        // them positionally.
        val path =
            write("rowid-in", listSchema(), rows = 4) { g, i ->
                g.addGroup(0).addGroup(0).add(0, i)
            }
        val first = tmp.resolve("rowid-out1.parquet")
        val result =
            ParquetRewriter.rewrite(
                listOf(ParquetRewriter.Input(path, 100)),
                listOf(listColumn),
                emptyList(),
                first,
            )
        assertThat(result.rowsWritten).isEqualTo(4)
        assertThat(result.minRowId).isEqualTo(100)
        assertThat(readRowIds(first)).containsExactly(100, 101, 102, 103)

        val second = tmp.resolve("rowid-out2.parquet")
        // rowIdStart 0 on purpose: a rewriter that reassigned positionally
        // would renumber these 0..3 and the assertion would catch it.
        ParquetRewriter.rewrite(listOf(ParquetRewriter.Input(first, 0)), listOf(listColumn), emptyList(), second)
        assertThat(readRowIds(second)).containsExactly(100, 101, 102, 103)
    }

    // ---- bounds through the loop -------------------------------------------

    @Test
    fun `per-leaf bounds are byte-identical across two compactions`() {
        // The hydrator reads the compacted footer at commit and REPLACES
        // the file's stats rows, so any drift here silently substitutes a
        // different bound for a correct one — on a file nobody will
        // re-read.
        val path =
            write("bounds-in", mapSchema(), rows = 3) { g, i ->
                val entry = g.addGroup(0).addGroup(0)
                entry.add(0, listOf("a", "m", "z")[i])
                entry.add(1, listOf(-1L, 0L, 1L shl 40)[i])
            }
        val catalog =
            listOf(
                CatalogColumn(
                    1,
                    "m",
                    ColType.MAP,
                    null,
                    listOf(
                        CatalogColumn(2, "key", ColType.STRING, null),
                        CatalogColumn(3, "value", ColType.LONG, null),
                    ),
                ),
            )
        val before = boundsOf(path, catalog)
        assertThat(before.keys).describedAs("only LEAVES get bounds").containsExactlyInAnyOrder(2L, 3L)
        assertThat(before.getValue(2L).first).isEqualTo("a".toByteArray())
        assertThat(before.getValue(2L).second).isEqualTo("z".toByteArray())
        assertThat(before.getValue(3L).first).isEqualTo(IcebergSingleValue.encodeLong(-1L))
        assertThat(before.getValue(3L).second).isEqualTo(IcebergSingleValue.encodeLong(1L shl 40))

        for (out in roundTrip("bounds", path, listOf(mapColumn))) {
            val after = boundsOf(out, catalog)
            assertThat(after.keys).describedAs("leaf set, %s", out.fileName).isEqualTo(before.keys)
            for ((fieldId, pair) in before) {
                assertThat(after.getValue(fieldId).first)
                    .describedAs("lower bound of field %d in %s", fieldId, out.fileName)
                    .isEqualTo(pair.first)
                assertThat(after.getValue(fieldId).second)
                    .describedAs("upper bound of field %d in %s", fieldId, out.fileName)
                    .isEqualTo(pair.second)
            }
        }
    }

    // ---- sorting -----------------------------------------------------------

    @Test
    fun `a struct leaf is a legal sort key and actually orders the output`() {
        val path =
            write("sort-in", structSchema(), rows = 4) { g, i ->
                val inner = g.addGroup(0)
                inner.add(0, listOf(3, 1, 4, 2)[i])
                inner.add(1, "r$i")
            }
        val out = tmp.resolve("sort-out.parquet")
        ParquetRewriter.rewrite(
            listOf(ParquetRewriter.Input(path, 0)),
            listOf(structColumn),
            listOf(SortFieldDef(2, SortDirection.ASC, NullOrder.NULLS_LAST)),
            out,
        )
        assertThat(readStructs(out).map { it!!.first }).containsExactly(1, 2, 3, 4)
        // Sorting is only safe because the ids are explicit data, not
        // position — so they must have travelled with their rows.
        assertThat(readRowIds(out)).containsExactly(1, 3, 0, 2)
    }

    @Test
    fun `a nested container is refused as a sort key, by name`() {
        val path =
            write("sortbad-in", structSchema(), rows = 1) { g, _ ->
                val inner = g.addGroup(0)
                inner.add(0, 1)
                inner.add(1, "x")
            }
        assertThatThrownBy {
            ParquetRewriter.rewrite(
                listOf(ParquetRewriter.Input(path, 0)),
                listOf(structColumn),
                // field 1 is the struct itself
                listOf(SortFieldDef(1, SortDirection.ASC, NullOrder.NULLS_LAST)),
                tmp.resolve("sortbad-out.parquet"),
            )
        }
            .isInstanceOf(UnconvertibleSchemaException::class.java)
            .hasMessageContaining("nested container")
    }

    @Test
    fun `a leaf under a list is refused as a sort key, by name`() {
        val path =
            write("sortlist-in", listSchema(), rows = 1) { g, _ ->
                g.addGroup(0).addGroup(0).add(0, 1)
            }
        assertThatThrownBy {
            ParquetRewriter.rewrite(
                listOf(ParquetRewriter.Input(path, 0)),
                listOf(listColumn),
                // field 2 is the list ELEMENT: many values per row.
                listOf(SortFieldDef(2, SortDirection.ASC, NullOrder.NULLS_LAST)),
                tmp.resolve("sortlist-out.parquet"),
            )
        }
            .isInstanceOf(UnconvertibleSchemaException::class.java)
            .hasMessageContaining("not sortable")
    }

    // ---- schema disagreements ----------------------------------------------

    @Test
    fun `an input whose nested shape disagrees with the live column is unconvertible`() {
        // Skip-with-reason, never wrong bytes: the group stays
        // uncompacted and the sweep reports unconvertible_schema.
        val flat =
            Types.buildMessage()
                .addField(Types.optional(PrimitiveTypeName.INT32).id(1).named("s"))
                .named("t")
        val path = write("mismatch-in", flat, rows = 1) { g, _ -> g.add(0, 1) }
        assertThatThrownBy { rewrite(path, listOf(structColumn), tmp.resolve("mismatch-out.parquet")) }
            .isInstanceOf(UnconvertibleSchemaException::class.java)
            .hasMessageContaining("primitive leaf, not a group")
    }

    @Test
    fun `an input map with an optional key is refused rather than half-written`() {
        // The output key is REQUIRED (the parquet MAP shape says so), so
        // a row whose key is absent would fail the write halfway through
        // the group and leave a partial file. Refusing at plan time is
        // the whole point of unconvertible_schema.
        val optionalKey =
            Types.buildMessage()
                .addField(
                    Types.optionalGroup()
                        .addField(
                            Types.repeatedGroup()
                                .addFields(
                                    Types.optional(PrimitiveTypeName.BINARY)
                                        .`as`(LogicalTypeAnnotation.stringType()).id(2).named("key"),
                                    Types.optional(PrimitiveTypeName.INT64).id(3).named("value"),
                                )
                                .named("key_value"),
                        )
                        .`as`(LogicalTypeAnnotation.mapType())
                        .id(1).named("m"),
                )
                .named("t")
        val path =
            write("optkey-in", optionalKey, rows = 1) { g, _ ->
                val entry = g.addGroup(0).addGroup(0)
                entry.add(0, "k")
                entry.add(1, 1L)
            }
        assertThatThrownBy { rewrite(path, listOf(mapColumn), tmp.resolve("optkey-out.parquet")) }
            .isInstanceOf(UnconvertibleSchemaException::class.java)
            .hasMessageContaining("Iceberg map keys are non-nullable")
    }

    @Test
    fun `a struct field the input predates null-fills, and one it lost drops`() {
        // The heterogeneous-schema rule, one level down: inputs map to
        // the LIVE schema by field id, so a field added later null-fills
        // and a dropped field id's data disappears.
        val older =
            Types.buildMessage()
                .addField(
                    Types.optionalGroup()
                        .addFields(
                            Types.optional(PrimitiveTypeName.INT32).id(2).named("a"),
                            // field 9 is not in the live schema any more
                            Types.optional(PrimitiveTypeName.INT32).id(9).named("gone"),
                        )
                        .id(1).named("s"),
                )
                .named("t")
        val path =
            write("hetero-in", older, rows = 2) { g, i ->
                val inner = g.addGroup(0)
                inner.add(0, i)
                inner.add(1, 100 + i)
            }
        val out = tmp.resolve("hetero-out.parquet")
        rewrite(path, listOf(structColumn), out)
        // `b` (field 3) null-fills; `gone` (field 9) is not in the output.
        assertThat(readStructs(out)).containsExactly(0 to null, 1 to null)
        assertThat(schemaOf(out).getType("s").asGroupType().fields.map { it.name })
            .containsExactly("a", "b")
    }

    // ---- harness -----------------------------------------------------------

    /** Compact [input], then compact THAT, returning both outputs. */
    private fun roundTrip(
        name: String,
        input: Path,
        live: List<Column>,
    ): List<Path> {
        val first = tmp.resolve("$name-out1.parquet")
        rewrite(input, live, first)
        val second = tmp.resolve("$name-out2.parquet")
        rewrite(first, live, second)
        return listOf(first, second)
    }

    private fun rewrite(
        input: Path,
        live: List<Column>,
        out: Path,
    ) {
        ParquetRewriter.rewrite(listOf(ParquetRewriter.Input(input, 0)), live, emptyList(), out)
    }

    /** Per-leaf (lower, upper) the hydrator would store for this file. */
    private fun boundsOf(
        path: Path,
        catalog: List<CatalogColumn>,
    ): Map<Long, Pair<ByteArray, ByteArray>> =
        ParquetFileReader.open(LocalInputFile(path)).use { reader ->
            FooterStats.aggregate(reader.footer, catalog, path.toString())
                .mapNotNull { agg ->
                    val lower = agg.lowerBound
                    val upper = agg.upperBound
                    if (lower == null || upper == null) null else agg.fieldId to (lower to upper)
                }
                .toMap()
        }

    // ---- parquet fixtures ---------------------------------------------------

    private fun listSchema(): MessageType =
        Types.buildMessage()
            .addField(
                Types.optionalGroup()
                    .addField(
                        Types.repeatedGroup()
                            .addField(Types.optional(PrimitiveTypeName.INT32).id(2).named("element"))
                            .named("list"),
                    )
                    .`as`(LogicalTypeAnnotation.listType())
                    .id(1).named("l"),
            )
            .named("t")

    private fun structSchema(): MessageType =
        Types.buildMessage()
            .addField(
                Types.optionalGroup()
                    .addFields(
                        Types.optional(PrimitiveTypeName.INT32).id(2).named("a"),
                        Types.optional(PrimitiveTypeName.BINARY)
                            .`as`(LogicalTypeAnnotation.stringType()).id(3).named("b"),
                    )
                    .id(1).named("s"),
            )
            .named("t")

    private fun mapSchema(): MessageType =
        Types.buildMessage()
            .addField(
                Types.optionalGroup()
                    .addField(
                        Types.repeatedGroup()
                            .addFields(
                                Types.required(PrimitiveTypeName.BINARY)
                                    .`as`(LogicalTypeAnnotation.stringType()).id(2).named("key"),
                                Types.optional(PrimitiveTypeName.INT64).id(3).named("value"),
                            )
                            .named("key_value"),
                    )
                    .`as`(LogicalTypeAnnotation.mapType())
                    .id(1).named("m"),
            )
            .named("t")

    private fun deepSchema(): MessageType =
        Types.buildMessage()
            .addField(
                Types.optionalGroup()
                    .addField(
                        Types.optionalGroup()
                            .addField(
                                Types.repeatedGroup()
                                    .addField(
                                        Types.optionalGroup()
                                            .addField(
                                                Types.optional(PrimitiveTypeName.DOUBLE).id(5).named("score"),
                                            )
                                            .id(3).named("element"),
                                    )
                                    .named("list"),
                            )
                            .`as`(LogicalTypeAnnotation.listType())
                            .id(2).named("runs"),
                    )
                    .id(1).named("d"),
            )
            .named("t")

    private fun write(
        fileName: String,
        schema: MessageType,
        rows: Int = 4,
        fill: (Group, Int) -> Unit,
    ): Path {
        val path = tmp.resolve("$fileName.parquet")
        val factory = SimpleGroupFactory(schema)
        ExampleParquetWriter.builder(LocalOutputFile(path))
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build()
            .use { w ->
                for (i in 0 until rows) {
                    val g = factory.newGroup()
                    fill(g, i)
                    w.write(g)
                }
            }
        return path
    }

    // ---- readers ------------------------------------------------------------

    private fun schemaOf(path: Path): MessageType =
        ParquetFileReader.open(LocalInputFile(path)).use { it.footer.fileMetaData.schema }

    private fun <T> readRows(
        path: Path,
        read: (Group) -> T,
    ): List<T> {
        val schema = schemaOf(path)
        val out = mutableListOf<T>()
        ParquetFileReader.open(LocalInputFile(path)).use { reader ->
            val columnIO = ColumnIOFactory().getColumnIO(schema)
            var pages = reader.readNextRowGroup()
            while (pages != null) {
                val records = columnIO.getRecordReader(pages, GroupRecordConverter(schema))
                repeat(Math.toIntExact(pages.rowCount)) { out.add(read(records.read())) }
                pages = reader.readNextRowGroup()
            }
        }
        return out
    }

    private fun readLists(path: Path): List<List<Int>?> =
        readRows(path) { g ->
            if (g.getFieldRepetitionCount(0) == 0) {
                null
            } else {
                val outer = g.getGroup(0, 0)
                (0 until outer.getFieldRepetitionCount(0)).map { outer.getGroup(0, it).getInteger(0, 0) }
            }
        }

    private fun readStructs(path: Path): List<Pair<Int, String?>?> =
        readRows(path) { g ->
            if (g.getFieldRepetitionCount(0) == 0) {
                null
            } else {
                val inner = g.getGroup(0, 0)
                val b = if (inner.getFieldRepetitionCount(1) == 0) null else inner.getString(1, 0)
                inner.getInteger(0, 0) to b
            }
        }

    private fun readMaps(path: Path): List<List<Pair<String, Long>>?> =
        readRows(path) { g ->
            if (g.getFieldRepetitionCount(0) == 0) {
                null
            } else {
                val outer = g.getGroup(0, 0)
                (0 until outer.getFieldRepetitionCount(0)).map {
                    val entry = outer.getGroup(0, it)
                    entry.getString(0, 0) to entry.getLong(1, 0)
                }
            }
        }

    private fun readDeep(path: Path): List<List<Double>?> =
        readRows(path) { g ->
            if (g.getFieldRepetitionCount(0) == 0) {
                null
            } else {
                val d = g.getGroup(0, 0)
                if (d.getFieldRepetitionCount(0) == 0) {
                    null
                } else {
                    val runs = d.getGroup(0, 0)
                    (0 until runs.getFieldRepetitionCount(0)).map {
                        runs.getGroup(0, it).getGroup(0, 0).getDouble(0, 0)
                    }
                }
            }
        }

    private fun readRowIds(path: Path): List<Long> {
        val index = schemaOf(path).getFieldIndex(ParquetRewriter.ROW_ID_COLUMN)
        return readRows(path) { g -> g.getLong(index, 0) }
    }
}
