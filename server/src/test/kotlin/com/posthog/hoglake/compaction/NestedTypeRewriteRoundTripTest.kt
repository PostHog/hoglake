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

    // ---- container-typed map keys ------------------------------------------

    /** `mk map<struct{x long}, string>` — ids 1, 2(key), 3(x), 4(value). */
    private val structKeyMapColumn =
        Column(
            1,
            0,
            ColumnDef("mk", ColType.MAP),
            listOf(
                Column(
                    2,
                    0,
                    ColumnDef("key", ColType.STRUCT, nullable = false),
                    listOf(Column(3, 0, ColumnDef("x", ColType.LONG))),
                ),
                Column(4, 1, ColumnDef("value", ColType.STRING)),
            ),
        )

    /** `ml map<list<int>, long>` — ids 1, 2(key), 3(element), 4(value). */
    private val listKeyMapColumn =
        Column(
            1,
            0,
            ColumnDef("ml", ColType.MAP),
            listOf(
                Column(
                    2,
                    0,
                    ColumnDef("key", ColType.LIST, nullable = false),
                    listOf(Column(3, 0, ColumnDef("element", ColType.INT))),
                ),
                Column(4, 1, ColumnDef("value", ColType.LONG)),
            ),
        )

    @Test
    fun `a map with a STRUCT key survives compaction, keys and values intact`() {
        // Iceberg permits non-scalar map keys and nothing in this
        // pipeline breaks on one — but "nothing breaks" is a claim, and
        // an untested claim about a shape the DDL accepts is the kind
        // that stops being true quietly.
        val rows = listOf(listOf(7L to "a", 3L to "b"), emptyList(), null)
        val schema = structKeyMapSchema()
        val path =
            write("skmap-in", schema, rows = rows.size) { g, i ->
                val value = rows[i]
                if (value != null) {
                    val outer = g.addGroup(0)
                    for ((k, v) in value) {
                        val entry = outer.addGroup(0)
                        entry.addGroup(0).add(0, k)
                        entry.add(1, v)
                    }
                }
            }
        for (out in roundTrip("skmap", path, listOf(structKeyMapColumn))) {
            assertThat(readStructKeyMaps(out)).describedAs("values in %s", out.fileName).isEqualTo(rows)
        }

        // The key's LEAF bounds are ordinary leaf bounds; the key group
        // itself, like every container, gets none.
        val catalog =
            listOf(
                CatalogColumn(
                    1,
                    "mk",
                    ColType.MAP,
                    null,
                    listOf(
                        CatalogColumn(
                            2,
                            "key",
                            ColType.STRUCT,
                            null,
                            listOf(CatalogColumn(3, "x", ColType.LONG, null)),
                        ),
                        CatalogColumn(4, "value", ColType.STRING, null),
                    ),
                ),
            )
        val bounds = boundsOf(path, catalog)
        assertThat(bounds.keys).describedAs("only LEAVES bound").containsExactlyInAnyOrder(3L, 4L)
        assertThat(bounds.getValue(3L).first).isEqualTo(IcebergSingleValue.encodeLong(3))
        assertThat(bounds.getValue(3L).second).isEqualTo(IcebergSingleValue.encodeLong(7))
        for (out in roundTrip("skmap2", path, listOf(structKeyMapColumn))) {
            // Byte-for-byte, per field: ByteArray equality is identity,
            // so comparing the maps directly would compare references.
            val after = boundsOf(out, catalog)
            assertThat(after.keys).isEqualTo(bounds.keys)
            for ((fieldId, pair) in bounds) {
                assertThat(after.getValue(fieldId).first)
                    .describedAs("lower bound of field %d in %s", fieldId, out.fileName)
                    .isEqualTo(pair.first)
                assertThat(after.getValue(fieldId).second)
                    .describedAs("upper bound of field %d in %s", fieldId, out.fileName)
                    .isEqualTo(pair.second)
            }
        }
    }

    @Test
    fun `a map with a LIST key survives compaction`() {
        val rows = listOf(listOf(listOf(1, 2) to 10L), listOf(listOf(3) to 20L, emptyList<Int>() to 30L))
        val schema = listKeyMapSchema()
        val path =
            write("lkmap-in", schema, rows = rows.size) { g, i ->
                val outer = g.addGroup(0)
                for ((k, v) in rows[i]) {
                    val entry = outer.addGroup(0)
                    val keyList = entry.addGroup(0)
                    for (e in k) keyList.addGroup(0).add(0, e)
                    entry.add(1, v)
                }
            }
        for (out in roundTrip("lkmap", path, listOf(listKeyMapColumn))) {
            assertThat(readListKeyMaps(out)).describedAs("values in %s", out.fileName).isEqualTo(rows)
        }
        // And the output's key is still REQUIRED — the parquet MAP shape
        // demands it whether the key is a scalar or a whole list.
        val out = tmp.resolve("lkmap-shape.parquet")
        rewrite(path, listOf(listKeyMapColumn), out)
        val entry = schemaOf(out).getType("ml").asGroupType().getType(0).asGroupType()
        assertThat(entry.getType(0).isRepetition(Type.Repetition.REQUIRED)).isTrue()
        assertThat(FooterStats.missingFieldIds(schemaOf(out))).isFalse()
    }

    private fun structKeyMapSchema(): MessageType =
        Types.buildMessage()
            .addField(
                Types.optionalGroup()
                    .addField(
                        Types.repeatedGroup()
                            .addFields(
                                Types.requiredGroup()
                                    .addField(Types.optional(PrimitiveTypeName.INT64).id(3).named("x"))
                                    .id(2).named("key"),
                                Types.optional(PrimitiveTypeName.BINARY)
                                    .`as`(LogicalTypeAnnotation.stringType()).id(4).named("value"),
                            )
                            .named("key_value"),
                    )
                    .`as`(LogicalTypeAnnotation.mapType())
                    .id(1).named("mk"),
            )
            .named("t")

    private fun listKeyMapSchema(): MessageType =
        Types.buildMessage()
            .addField(
                Types.optionalGroup()
                    .addField(
                        Types.repeatedGroup()
                            .addFields(
                                Types.requiredGroup()
                                    .addField(
                                        Types.repeatedGroup()
                                            .addField(
                                                Types.optional(PrimitiveTypeName.INT32).id(3).named("element"),
                                            )
                                            .named("list"),
                                    )
                                    .`as`(LogicalTypeAnnotation.listType())
                                    .id(2).named("key"),
                                Types.optional(PrimitiveTypeName.INT64).id(4).named("value"),
                            )
                            .named("key_value"),
                    )
                    .`as`(LogicalTypeAnnotation.mapType())
                    .id(1).named("ml"),
            )
            .named("t")

    private fun readStructKeyMaps(path: Path): List<List<Pair<Long, String>>?> =
        readRows(path) { g ->
            if (g.getFieldRepetitionCount(0) == 0) {
                null
            } else {
                val outer = g.getGroup(0, 0)
                (0 until outer.getFieldRepetitionCount(0)).map {
                    val entry = outer.getGroup(0, it)
                    entry.getGroup(0, 0).getLong(0, 0) to entry.getString(1, 0)
                }
            }
        }

    private fun readListKeyMaps(path: Path): List<List<Pair<List<Int>, Long>>> =
        readRows(path) { g ->
            val outer = g.getGroup(0, 0)
            (0 until outer.getFieldRepetitionCount(0)).map {
                val entry = outer.getGroup(0, it)
                val keyList = entry.getGroup(0, 0)
                val key =
                    (0 until keyList.getFieldRepetitionCount(0)).map {
                            j ->
                        keyList.getGroup(0, j).getInteger(0, 0)
                    }
                key to entry.getLong(1, 0)
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
    fun `a compaction output never flags itself id-less, for any nested shape`() {
        // THE trap in the field-id contract check. The check now covers
        // container WRAPPER groups, and it must keep exempting the
        // synthetic repetition layers parquet inserts (`list`,
        // `key_value`) — which this writer deliberately emits without
        // ids, because Iceberg has nothing to match one against. Get
        // that wrong and every rewrite produces a file that instantly
        // fails its own contract and blocks renames on its own table
        // forever, with compaction re-creating the condition each run.
        val cases =
            listOf(
                Triple("selfflag-list", listSchema(), listColumn),
                Triple("selfflag-struct", structSchema(), structColumn),
                Triple("selfflag-map", mapSchema(), mapColumn),
                Triple("selfflag-deep", deepSchema(), deepColumn),
            )
        for ((name, schema, live) in cases) {
            val path = write("$name-in", schema, rows = 1) { g, _ -> fillOne(schema, g) }
            val out = tmp.resolve("$name-out.parquet")
            rewrite(path, listOf(live), out)
            assertThat(FooterStats.missingFieldIds(schemaOf(out)))
                .describedAs("%s output flags itself", name)
                .isFalse()
            // ...and re-compacting it keeps the property, which is what
            // makes the run idempotent rather than one-shot-clean.
            val out2 = tmp.resolve("$name-out2.parquet")
            rewrite(out, listOf(live), out2, explicitRowIds = true)
            assertThat(FooterStats.missingFieldIds(schemaOf(out2)))
                .describedAs("%s re-compacted output flags itself", name)
                .isFalse()
        }
    }

    /** One minimal row for whichever fixture schema is passed. */
    private fun fillOne(
        schema: MessageType,
        g: Group,
    ) {
        when (schema.getType(0).name) {
            "l" -> g.addGroup(0).addGroup(0).add(0, 1)
            "s" ->
                g.addGroup(0).also {
                    it.add(0, 1)
                    it.add(1, "x")
                }
            "m" ->
                g.addGroup(0).addGroup(0).also {
                    it.add(0, "k")
                    it.add(1, 1L)
                }
            "d" -> g.addGroup(0).addGroup(0).addGroup(0).addGroup(0).add(0, 1.0)
            else -> error("unknown fixture ${schema.getType(0).name}")
        }
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
            rewriteToLocal(
                listOf(localInput(path, 100)),
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
        rewriteToLocal(
            // `first` is a compaction output; its ids live in the
            // carrier, so the catalog bit says so here too.
            listOf(localInput(first, 0, null, explicitRowIds = true)),
            listOf(listColumn),
            emptyList(),
            second,
        )
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
        rewriteToLocal(
            listOf(localInput(path, 0)),
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
            rewriteToLocal(
                listOf(localInput(path, 0)),
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
            rewriteToLocal(
                listOf(localInput(path, 0)),
                listOf(listColumn),
                // field 2 is the list ELEMENT: many values per row.
                listOf(SortFieldDef(2, SortDirection.ASC, NullOrder.NULLS_LAST)),
                tmp.resolve("sortlist-out.parquet"),
            )
        }
            .isInstanceOf(UnconvertibleSchemaException::class.java)
            .hasMessageContaining("not sortable")
    }

    @Test
    fun `a sort key inside an unannotated repeated group is refused`() {
        // Reached through sortKeyPath directly: rewrite() only ever sees
        // the schema outputSchema just built, which always annotates its
        // LIST and MAP wrappers, so this shape cannot arrive through the
        // public entry point. The check still has to be here — the
        // annotation is what tells a struct from a repetition layer, and
        // a repeated group WITHOUT one (a legacy 2-level list, say) would
        // otherwise be walked into as though it were a struct, handing
        // the comparator a leaf with many values per row.
        val schema =
            MessageType(
                "t",
                listOf(
                    Types.optionalGroup()
                        .addField(
                            Types.repeatedGroup()
                                .addField(Types.optional(PrimitiveTypeName.INT32).id(9).named("element"))
                                .named("bag"),
                        )
                        .id(1).named("l"),
                ),
            )
        assertThatThrownBy {
            ParquetRewriter.sortKeyPath(schema, SortFieldDef(9, SortDirection.ASC, NullOrder.NULLS_LAST))
        }
            .isInstanceOf(UnconvertibleSchemaException::class.java)
            .hasMessageContaining("not sortable")
    }

    // ---- repetition, duplicate ids, synthetic names ------------------------

    @Test
    fun `a REPEATED group wearing a struct's field id is refused, not half-copied`() {
        // The copy reads repetition 0 and only repetition 0. Measured
        // before the guard: a 2-repetition struct rewrote to its first
        // repetition alone — half the values gone, rowsWritten still
        // equal to the record count so nothing looked wrong — and then
        // the commit end-snapshotted the input and expiry deleted it.
        val repeated =
            Types.buildMessage()
                .addField(
                    Types.repeatedGroup()
                        .addFields(
                            Types.optional(PrimitiveTypeName.INT32).id(2).named("a"),
                            Types.optional(PrimitiveTypeName.BINARY)
                                .`as`(LogicalTypeAnnotation.stringType()).id(3).named("b"),
                        )
                        .id(1).named("s"),
                )
                .named("t")
        val path =
            write("repstruct-in", repeated, rows = 3) { g, i ->
                g.addGroup(0).also {
                    it.add(0, i * 10)
                    it.add(1, "r$i")
                }
                g.addGroup(0).also {
                    it.add(0, i * 10 + 1)
                    it.add(1, "z$i")
                }
            }
        assertThatThrownBy { rewrite(path, listOf(structColumn), tmp.resolve("repstruct-out.parquet")) }
            .isInstanceOf(UnconvertibleSchemaException::class.java)
            .hasMessageContaining("REPEATED")
            .hasMessageContaining("one value per row")
    }

    @Test
    fun `a REPEATED primitive under a scalar column is refused`() {
        val repeated =
            Types.buildMessage()
                .addField(Types.repeated(PrimitiveTypeName.INT32).id(1).named("x"))
                .named("t")
        val path =
            write("repscalar-in", repeated, rows = 3) { g, i ->
                g.add(0, i * 10)
                g.add(0, i * 10 + 1)
            }
        assertThatThrownBy {
            rewrite(
                path,
                listOf(Column(1, 0, ColumnDef("x", ColType.INT))),
                tmp.resolve("repscalar-out.parquet"),
            )
        }
            .isInstanceOf(UnconvertibleSchemaException::class.java)
            .hasMessageContaining("REPEATED")
    }

    @Test
    fun `a 2-level-list-shaped input under a STRUCT column is refused`() {
        // A plain group holding a repeated primitive — the legacy list
        // shape — wearing a struct's id. The struct's `a` would have
        // bound to the repeated leaf and taken its first value per row.
        val twoLevel =
            Types.buildMessage()
                .addField(
                    Types.optionalGroup()
                        .addField(Types.repeated(PrimitiveTypeName.INT32).id(2).named("a"))
                        .id(1).named("s"),
                )
                .named("t")
        val path =
            write("twolevel-in", twoLevel, rows = 2) { g, i ->
                val inner = g.addGroup(0)
                inner.add(0, i)
                inner.add(0, i + 100)
            }
        assertThatThrownBy { rewrite(path, listOf(structColumn), tmp.resolve("twolevel-out.parquet")) }
            .isInstanceOf(UnconvertibleSchemaException::class.java)
            .hasMessageContaining("REPEATED")
    }

    @Test
    fun `a file declaring one field id twice is refused rather than first-wins`() {
        // planChildren elects the FIRST match, so the rewrite would have
        // sourced live data from whichever field came first and then
        // end-snapshotted the input, making the guess permanent. There
        // is no correct resolution, so there is no binding to make.
        val dupes =
            Types.buildMessage()
                .addFields(
                    Types.optional(PrimitiveTypeName.INT32).id(1).named("first"),
                    Types.optional(PrimitiveTypeName.INT32).id(1).named("second"),
                )
                .named("t")
        val path =
            write("dupid-in", dupes, rows = 3) { g, i ->
                g.add(0, i)
                g.add(1, 1000 + i)
            }
        assertThatThrownBy {
            rewrite(
                path,
                listOf(Column(1, 0, ColumnDef("x", ColType.INT))),
                tmp.resolve("dupid-out.parquet"),
            )
        }
            .isInstanceOf(UnconvertibleSchemaException::class.java)
            .hasMessageContaining("more than once")
        // Deep, too: a duplicate inside a struct is the same hazard.
        val deepDupes =
            Types.buildMessage()
                .addFields(
                    Types.optional(PrimitiveTypeName.INT32).id(2).named("top"),
                    Types.optionalGroup()
                        .addFields(
                            Types.optional(PrimitiveTypeName.INT32).id(2).named("a"),
                            Types.optional(PrimitiveTypeName.BINARY)
                                .`as`(LogicalTypeAnnotation.stringType()).id(3).named("b"),
                        )
                        .id(1).named("s"),
                )
                .named("t")
        val deepPath =
            write("dupdeep-in", deepDupes, rows = 1) { g, _ ->
                g.add(0, 7)
                g.addGroup(1).also {
                    it.add(0, 1)
                    it.add(1, "x")
                }
            }
        assertThatThrownBy { rewrite(deepPath, listOf(structColumn), tmp.resolve("dupdeep-out.parquet")) }
            .isInstanceOf(UnconvertibleSchemaException::class.java)
            .hasMessageContaining("more than once")
    }

    @Test
    fun `a map entry group in the WRONG ORDER is refused cleanly, not thrown out of`() {
        // The reader binds map entry children POSITIONALLY and verifies
        // the id; the rewriter used to search for the id ANYWHERE, so it
        // accepted an entry group whose key and value are in the other
        // order — and then the copy writes the key unconditionally into
        // slot 0, on the strength of the input key being REQUIRED, which
        // is only true of the field actually in slot 0. Measured: the
        // plan succeeded and the copy threw `not found 1(key) element
        // number 0 in group`, which the sweep counts as a FAILED group
        // (error level, retried every run) rather than the
        // skip-with-reason it is.
        val swapped =
            Types.buildMessage()
                .addField(
                    Types.optionalGroup()
                        .addField(
                            Types.repeatedGroup()
                                .addFields(
                                    // value first, and REQUIRED so the
                                    // required-key check cannot catch it
                                    Types.required(PrimitiveTypeName.INT64).id(3).named("value"),
                                    Types.optional(PrimitiveTypeName.BINARY)
                                        .`as`(LogicalTypeAnnotation.stringType()).id(2).named("key"),
                                )
                                .named("key_value"),
                        )
                        .`as`(LogicalTypeAnnotation.mapType())
                        .id(1).named("m"),
                )
                .named("t")
        val path =
            write("swapentry-in", swapped, rows = 2) { g, i ->
                val e = g.addGroup(0).addGroup(0)
                e.add(0, 100L + i)
                // The second row leaves the OPTIONAL key absent, which is
                // what turned the mis-binding into a throw.
                if (i == 0) e.add(1, "k$i")
            }
        assertThatThrownBy { rewrite(path, listOf(mapColumn), tmp.resolve("swapentry-out.parquet")) }
            .describedAs("a clean skip-with-reason, not a raw throw")
            .isInstanceOf(UnconvertibleSchemaException::class.java)
            .hasMessageContaining("key does not match the live key field id")

        // The reader's answer for the same file: no stats, same refusal
        // in spirit. The two surfaces must accept the same files.
        assertThat(
            boundsOf(
                path,
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
                ),
            ),
        ).describedAs("the reader refuses the same file").isEmpty()
    }

    @Test
    fun `an id-less list element binds by SHAPE whatever it is named`() {
        // The reader binds this positionally and produces stats; the
        // rewriter demanded the live column's name (`element`) even
        // though its own repeatedEntryGroup doc says the spec makes
        // those names insignificant. So an id-less `item` under a `bag`
        // layer produced stats and refused to compact — the two surfaces
        // disagreeing about one file.
        val foreign =
            Types.buildMessage()
                .addFields(
                    Types.optional(PrimitiveTypeName.INT32).id(9).named("k"),
                    Types.optionalGroup()
                        .addField(
                            Types.repeatedGroup()
                                .addField(Types.optional(PrimitiveTypeName.INT32).named("item"))
                                .named("bag"),
                        )
                        .`as`(LogicalTypeAnnotation.listType())
                        .id(1).named("l"),
                )
                .named("t")
        val path =
            write("foreignlist-in", foreign, rows = 2) { g, i ->
                g.add(0, i)
                val l = g.addGroup(1)
                l.addGroup(0).add(0, i * 10)
                l.addGroup(0).add(0, i * 10 + 1)
            }
        val out = tmp.resolve("foreignlist-out.parquet")
        rewrite(path, listOf(listColumn), out)
        assertThat(readLists(out)).containsExactly(listOf(0, 1), listOf(10, 11))
    }

    @Test
    fun `id-less map key and value bind by SHAPE whatever they are named`() {
        val foreign =
            Types.buildMessage()
                .addFields(
                    Types.optional(PrimitiveTypeName.INT32).id(9).named("k"),
                    Types.optionalGroup()
                        .addField(
                            Types.repeatedGroup()
                                .addFields(
                                    Types.required(PrimitiveTypeName.BINARY)
                                        .`as`(LogicalTypeAnnotation.stringType()).named("kk"),
                                    Types.optional(PrimitiveTypeName.INT64).named("vv"),
                                )
                                .named("entries"),
                        )
                        .`as`(LogicalTypeAnnotation.mapType())
                        .id(1).named("m"),
                )
                .named("t")
        val path =
            write("foreignmap-in", foreign, rows = 2) { g, i ->
                g.add(0, i)
                val e = g.addGroup(1).addGroup(0)
                e.add(0, "k$i")
                e.add(1, 10L + i)
            }
        val out = tmp.resolve("foreignmap-out.parquet")
        rewrite(path, listOf(mapColumn), out)
        assertThat(readMaps(out)).containsExactly(listOf("k0" to 10L), listOf("k1" to 11L))
    }

    @Test
    fun `a container CATALOG row with the wrong child count is refused, never thrown out of`() {
        // A corrupt or hand-edited catalog. `single()` threw a raw
        // NoSuchElementException straight out of the sweep; the failure
        // has to be a skip-with-reason like every other disagreement.
        val path =
            write("arity-in", listSchema(), rows = 1) { g, _ ->
                g.addGroup(0).addGroup(0).add(0, 1)
            }
        assertThatThrownBy {
            rewrite(
                path,
                listOf(Column(1, 0, ColumnDef("l", ColType.LIST), emptyList())),
                tmp.resolve("arity-out.parquet"),
            )
        }
            .isInstanceOf(UnconvertibleSchemaException::class.java)
            .hasMessageContaining("children, not 1")

        val mapPath =
            write("arity2-in", mapSchema(), rows = 1) { g, _ ->
                val e = g.addGroup(0).addGroup(0)
                e.add(0, "k")
                e.add(1, 1L)
            }
        assertThatThrownBy {
            rewrite(
                mapPath,
                listOf(
                    Column(
                        1,
                        0,
                        ColumnDef("m", ColType.MAP),
                        listOf(
                            Column(2, 0, ColumnDef("key", ColType.STRING, nullable = false)),
                            Column(3, 1, ColumnDef("value", ColType.LONG)),
                            Column(4, 2, ColumnDef("extra", ColType.INT)),
                        ),
                    ),
                ),
                tmp.resolve("arity2-out.parquet"),
            )
        }
            .isInstanceOf(UnconvertibleSchemaException::class.java)
            .hasMessageContaining("children, not 2")
    }

    @Test
    fun `a struct-shaped group with a NON-container annotation still rewrites`() {
        // My own overreach from the previous round: refusing ANY
        // annotation made a struct-shaped group carrying a stray
        // unrelated one permanently uncompactable, and the reader
        // stopped bounding it too. Only LIST/MAP mean "this is a
        // container"; ENUM says nothing about shape.
        val odd =
            Types.buildMessage()
                .addField(
                    Types.optionalGroup()
                        .addFields(
                            Types.optional(PrimitiveTypeName.INT32).id(2).named("a"),
                            Types.optional(PrimitiveTypeName.BINARY)
                                .`as`(LogicalTypeAnnotation.stringType()).id(3).named("b"),
                        )
                        .`as`(LogicalTypeAnnotation.enumType())
                        .id(1).named("s"),
                )
                .named("t")
        val path =
            write("enumstruct-in", odd, rows = 2) { g, i ->
                val inner = g.addGroup(0)
                inner.add(0, i)
                inner.add(1, "r$i")
            }
        val out = tmp.resolve("enumstruct-out.parquet")
        rewrite(path, listOf(structColumn), out)
        assertThat(readStructs(out)).containsExactly(0 to "r0", 1 to "r1")
    }

    // ---- #65's decimal modes, one level down -------------------------------

    @Test
    fun `a decimal leaf INSIDE a struct takes the decimal copy modes`() {
        // #65 added DECIMAL_INT32/INT64/BINARY to copyMode. copyField
        // dispatches Step.Scalar into the same copyValue at any depth,
        // so nested decimals inherit them — asserted rather than assumed,
        // since "it composes" is exactly the claim a later refactor
        // breaks quietly.
        val live =
            Column(
                1,
                0,
                ColumnDef("s", ColType.STRUCT),
                listOf(
                    Column(
                        2,
                        0,
                        ColumnDef("amount", ColType.DECIMAL, typeParams = mapOf("precision" to 9, "scale" to 2)),
                    ),
                ),
            )
        // An INT32-backed decimal input — the form #65's DECIMAL_INT32
        // arm exists for — under a struct.
        val int32Decimal =
            Types.buildMessage()
                .addField(
                    Types.optionalGroup()
                        .addField(
                            Types.optional(PrimitiveTypeName.INT32)
                                .`as`(LogicalTypeAnnotation.decimalType(2, 9)).id(2).named("amount"),
                        )
                        .id(1).named("s"),
                )
                .named("t")
        val path =
            write("decstruct-in", int32Decimal, rows = 3) { g, i ->
                g.addGroup(0).add(0, (i - 1) * 12345)
            }
        val out = tmp.resolve("decstruct-out.parquet")
        rewrite(path, listOf(live), out)

        // The output's leaf is the catalog's BINARY decimal form...
        val leaf = schemaOf(out).getType("s").asGroupType().getType("amount").asPrimitiveType()
        assertThat(leaf.primitiveTypeName).isEqualTo(PrimitiveTypeName.BINARY)
        assertThat(leaf.logicalTypeAnnotation)
            .isEqualTo(LogicalTypeAnnotation.decimalType(2, 9))
        // ...and the VALUES survived the widening, negatives included.
        val catalog =
            listOf(
                CatalogColumn(
                    1,
                    "s",
                    ColType.STRUCT,
                    null,
                    listOf(CatalogColumn(2, "amount", ColType.DECIMAL, 2)),
                ),
            )
        val bounds = boundsOf(out, catalog)
        assertThat(bounds.keys).containsExactly(2L)
        assertThat(java.math.BigInteger(bounds.getValue(2L).first)).isEqualTo(java.math.BigInteger.valueOf(-12345))
        assertThat(java.math.BigInteger(bounds.getValue(2L).second)).isEqualTo(java.math.BigInteger.valueOf(12345))
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
    fun `a LIST wrapper wearing a struct's field id is refused, not null-filled`() {
        // F1's data-loss shape through a different door, and the reason
        // it is worth its own guard: a struct's fields are the ONE
        // interior that null-fills. Treat a LIST wrapper as a struct and
        // every field misses, so the rewrite emits rows of EMPTY structs
        // and the commit end-snapshots the input that held the real
        // values. Measured before the fix: 3 rows in, 3 rows of
        // `s | _hog_row_id: n` out, every element gone.
        val listShaped =
            Types.buildMessage()
                .addField(
                    Types.optionalGroup()
                        .addField(
                            Types.repeatedGroup()
                                .addField(Types.optional(PrimitiveTypeName.INT32).id(9).named("element"))
                                .named("list"),
                        )
                        .`as`(LogicalTypeAnnotation.listType())
                        // the STRUCT's field id, on a LIST wrapper
                        .id(1).named("s"),
                )
                .named("t")
        val path =
            write("structoverlist-in", listShaped, rows = 3) { g, i ->
                val l = g.addGroup(0)
                l.addGroup(0).add(0, i * 10)
                l.addGroup(0).add(0, i * 10 + 1)
            }
        assertThatThrownBy { rewrite(path, listOf(structColumn), tmp.resolve("structoverlist-out.parquet")) }
            .isInstanceOf(UnconvertibleSchemaException::class.java)
            .hasMessageContaining("not a struct")
    }

    @Test
    fun `a MAP wrapper wearing a struct's field id is refused too`() {
        val mapShaped =
            Types.buildMessage()
                .addField(
                    Types.optionalGroup()
                        .addField(
                            Types.repeatedGroup()
                                .addFields(
                                    Types.required(PrimitiveTypeName.BINARY)
                                        .`as`(LogicalTypeAnnotation.stringType()).id(8).named("key"),
                                    Types.optional(PrimitiveTypeName.INT64).id(9).named("value"),
                                )
                                .named("key_value"),
                        )
                        .`as`(LogicalTypeAnnotation.mapType())
                        .id(1).named("s"),
                )
                .named("t")
        val path =
            write("structovermap-in", mapShaped, rows = 1) { g, _ ->
                val e = g.addGroup(0).addGroup(0)
                e.add(0, "k")
                e.add(1, 1L)
            }
        assertThatThrownBy { rewrite(path, listOf(structColumn), tmp.resolve("structovermap-out.parquet")) }
            .isInstanceOf(UnconvertibleSchemaException::class.java)
            .hasMessageContaining("not a struct")
    }

    @Test
    fun `a plain group is still accepted for a struct, annotation-free`() {
        // The control: the refusal is about the ANNOTATION, not about
        // groups. An ordinary struct still rewrites.
        val path =
            write("plainstruct-in", structSchema(), rows = 2) { g, i ->
                val inner = g.addGroup(0)
                inner.add(0, i)
                inner.add(1, "r$i")
            }
        val out = tmp.resolve("plainstruct-out.parquet")
        rewrite(path, listOf(structColumn), out)
        assertThat(readStructs(out)).containsExactly(0 to "r0", 1 to "r1")
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
        // The second pass reads a compaction OUTPUT.
        rewrite(first, live, second, explicitRowIds = true)
        return listOf(first, second)
    }

    /**
     * [explicitRowIds] mirrors `hog_data_file.explicit_row_ids`: false
     * for a client append, true when the input is itself a compaction
     * OUTPUT. The rewriter refuses a file that disagrees with its
     * registration in either direction, so a second pass has to say so.
     */
    private fun rewrite(
        input: Path,
        live: List<Column>,
        out: Path,
        explicitRowIds: Boolean = false,
    ) {
        rewriteToLocal(
            listOf(localInput(input, 0, null, explicitRowIds)),
            live,
            emptyList(),
            out,
        )
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
