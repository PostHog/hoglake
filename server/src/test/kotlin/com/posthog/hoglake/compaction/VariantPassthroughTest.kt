package com.posthog.hoglake.compaction

import com.posthog.hoglake.compaction.VariantFixtures.Source
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.MessageTypeParser
import org.apache.parquet.schema.Type
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

/**
 * VARIANT compaction copies each variant's storage through the REAL
 * rewrite path — [ParquetRewriter.SurvivorReader]'s plain-group decode,
 * the [ParquetRewriter.RowPlan] walk and [OutputWriter] — over files
 * pyhoglake wrote ([VariantFixtures]): every surviving row's `metadata`,
 * `value` and `typed_value` leaves, with which of them are present, come
 * out byte for byte as they went in. The rows hold what a copy could
 * lose: declared keys missing (both leaves null), values of the wrong
 * type (in `value`), a Variant null beside an SQL NULL, rows that are not
 * objects, every leaf type and an array of objects. The `other_*`
 * fixtures hold `props` in ANOTHER declaration's layout: the rewrite
 * copies them the same way into that layout when it is named
 * (`variantLayouts`), which is how a group of them compacts after the
 * declaration changed ([VariantLayoutGroups]).
 */
class VariantPassthroughTest {
    @TempDir
    lateinit var tmp: Path

    @Test
    fun `an unsorted rewrite with a DV copies every surviving row's variants byte for byte`() {
        val out = tmp.resolve("out.parquet")
        val inputs = listOf(localInput(fixture("a"), 0, dv(1, 3)), localInput(fixture("b"), 100))
        val result = rewriteToLocal(inputs, live, emptyList(), out, variantCompaction = true)

        val expected = expected(Source(fixture("a"), 0, setOf(1, 3)), Source(fixture("b"), 100))
        assertThat(result.rowsWritten).isEqualTo(expected.size.toLong())
        assertThat(rows(out)).isEqualTo(expected)
        // The output is the catalog's layout, every variant group OPTIONAL.
        val schema = read(out).first
        assertThat(schema).isEqualTo(ParquetRewriter.outputSchema(live))
        assertThat(schema.getType("props")).isEqualTo(VariantLayout.groupType(live[1], Type.Repetition.OPTIONAL))
        // The fixture holds what the copy must keep apart: an SQL NULL (no
        // group) and a Variant null (the group, `value` 0x00, no typed_value).
        val props = expected.values.map { it.getValue("props") }
        assertThat(props).contains("[]")
        assertThat(props).anyMatch { it.endsWith(" value=[00] typed_value=[]}]") }
        assertThat(props.toSet()).hasSizeGreaterThan(10)
    }

    @Test
    fun `a sorted rewrite that spills and merges copies them byte for byte too`() {
        val out = tmp.resolve("sorted.parquet")
        val result =
            rewriteToLocal(
                listOf(localInput(fixture("a"), 0, dv(0)), localInput(fixture("b"), 100)),
                live,
                listOf(SortFieldDef(1, SortDirection.ASC, NullOrder.NULLS_LAST)),
                out,
                spill =
                    SortSpill(
                        chunkRows = 2,
                        mergeBudgetBytes = Long.MAX_VALUE / 4,
                        spillBudgetBytes = Long.MAX_VALUE / 4,
                        spillDir = tmp,
                    ),
                variantCompaction = true,
            )
        // Spill files are written in the output schema and read back as
        // themselves: the variant layout makes that trip too.
        assertThat(result.runsSpilled).isGreaterThan(1)
        assertThat(rows(out)).isEqualTo(expected(Source(fixture("a"), 0, setOf(0)), Source(fixture("b"), 100)))
        val ids = read(out).second.map { it.getLong("id", 0) }
        assertThat(ids).isSorted().hasSize(23)
    }

    @Test
    fun `a prior output is appended byte for byte, and its variants read back as written`() {
        val first = tmp.resolve("first.parquet")
        rewriteToLocal(
            listOf(localInput(fixture("a"), 0), localInput(fixture("b"), 100)),
            live,
            emptyList(),
            first,
            variantCompaction = true,
        )
        val out = tmp.resolve("second.parquet")
        val result =
            ParquetRewriter.rewrite(
                listOf(localInput(first, 0, explicitRowIds = true), localInput(fixture("b"), 1000)),
                live,
                emptyList(),
                LocalDiscardableOutput(out),
                appendFloorBytes = 0,
                variantCompaction = true,
            )
        // Appended needs the schemas EQUAL, which only an output of the same
        // declarations is.
        assertThat(result.rowGroupsAppended).isPositive()
        assertThat(rows(out))
            .isEqualTo(expected(Source(fixture("a"), 0), Source(fixture("b"), 100), Source(fixture("b"), 1000)))
    }

    @Test
    fun `an input in another layout or written by DuckDB is refused, typed, and nothing is written`() {
        for ((file, reason) in listOf("mismatch" to "layout", "duckdb" to "duckdb")) {
            val out = tmp.resolve("$file-out.parquet")
            assertThatThrownBy {
                rewriteToLocal(
                    listOf(localInput(fixture("a"), 0), localInput(fixture(file), 100)),
                    live,
                    emptyList(),
                    out,
                    variantCompaction = true,
                )
            }.isInstanceOfSatisfying(VariantLayoutMismatchException::class.java) {
                assertThat(it.reason).isEqualTo(reason)
                assertThat(it.message).contains("variant column 'props'", "$file.parquet")
            }
            assertThat(out).doesNotExist()
        }
    }

    @Test
    fun `a file DuckDB wrote in its own layout is counted as DuckDB's, not as layout drift`() {
        // A real DuckDB file (the hydrator's fixture, pyhoglake's
        // tests/data/native_variant.parquet): OPTIONAL `value` and field
        // groups, shredded by what it holds, so it is no layout compaction
        // writes either, and only the created_by check tells it apart.
        val duck = Path.of(javaClass.getResource("/variant/native_variant.parquet")!!.toURI())
        val columns =
            listOf(
                Column(1, 0, ColumnDef("id", ColType.LONG)),
                Column(2, 1, ColumnDef("properties", ColType.VARIANT)),
            )
        assertThatThrownBy {
            rewriteToLocal(
                listOf(localInput(duck, 0)),
                columns,
                emptyList(),
                tmp.resolve("duck-out.parquet"),
                variantCompaction = true,
            )
        }.isInstanceOfSatisfying(VariantLayoutMismatchException::class.java) {
            assertThat(it.reason).isEqualTo("duckdb")
            assertThat(it.message).contains("DuckDB version v1.5.5")
        }
    }

    @Test
    fun `a variant column bound to a leaf or a repeated group is refused as a layout mismatch`() {
        val decl = live.subList(0, 2)
        for ((case, field) in listOf(
            "leaf" to "optional binary props = 2;",
            "repeated" to "repeated group props (VARIANT(1)) = 2 { required binary metadata; required binary value; }",
        )) {
            val schema = MessageTypeParser.parseMessageType("message m { required int64 id = 1; $field }")
            val path = tmp.resolve("$case.parquet")
            ExampleParquetWriter.builder(LocalOutputFile(path)).withType(schema).build().use { w ->
                w.write(SimpleGroupFactory(schema).newGroup().append("id", 1L))
            }
            assertThatThrownBy {
                rewriteToLocal(
                    listOf(localInput(path, 0)),
                    decl,
                    emptyList(),
                    tmp.resolve("$case-out.parquet"),
                    variantCompaction = true,
                )
            }.describedAs(case)
                .isInstanceOfSatisfying(VariantLayoutMismatchException::class.java) {
                    assertThat(it.reason).isEqualTo("layout")
                    assertThat(it.message).contains("not one VARIANT group").doesNotContain("\n")
                }
        }
    }

    @Test
    fun `with the flag off a variant table is refused as before, not as a layout mismatch`() {
        assertThatThrownBy {
            rewriteToLocal(listOf(localInput(fixture("a"), 0)), live, emptyList(), tmp.resolve("off.parquet"))
        }.isExactlyInstanceOf(UnconvertibleSchemaException::class.java)
            .hasMessageContaining("variant compaction is off")
    }

    // ---- another layout -------------------------------------------------------

    @Test
    fun `a group whose inputs share another layout rewrites into that layout, byte for byte, through a DV`() {
        // The files a table holds after `set_variant_shredding` moved
        // `props` away from the declaration they were written to. The
        // service hands the rewrite the layout the inputs share
        // (VariantLayoutGroups); here it is read off one of their footers
        // exactly as the probe reads it.
        val layouts = probe("other_a")
        assertThat(layouts.keys).containsExactly(2L, 3L, 4L)
        assertThat(layouts.getValue(2L))
            .describedAs("props as other.json declares it")
            .isEqualTo(VariantLayout.groupType(other[1], Type.Repetition.OPTIONAL))
            .isNotEqualTo(VariantLayout.groupType(live[1], Type.Repetition.OPTIONAL))
        assertThat(layouts.getValue(3L)).isEqualTo(VariantLayout.groupType(live[2], Type.Repetition.OPTIONAL))

        val out = tmp.resolve("other-out.parquet")
        val result =
            ParquetRewriter.rewrite(
                listOf(localInput(fixture("other_a"), 0, dv(1, 3)), localInput(fixture("other_b"), 100)),
                live,
                emptyList(),
                LocalDiscardableOutput(out),
                variantCompaction = true,
                variantLayouts = layouts,
            )
        val expected = expected(Source(fixture("other_a"), 0, setOf(1, 3)), Source(fixture("other_b"), 100))
        assertThat(result.rowsWritten).isEqualTo(expected.size.toLong())
        assertThat(rows(out)).isEqualTo(expected)
        // The output schema is the LIVE schema with props in the inputs'
        // layout: what the other declaration lays out, every group
        // OPTIONAL, the field id on the top group and nowhere below.
        val schema = read(out).first
        assertThat(schema)
            .isEqualTo(ParquetRewriter.outputSchema(live, layouts))
            .isEqualTo(ParquetRewriter.outputSchema(other))
        assertThat(schema.getType("props")).isEqualTo(layouts.getValue(2L))
        assertThat(schema.getType("rich")).isEqualTo(VariantLayout.groupType(live[2], Type.Repetition.OPTIONAL))
    }

    @Test
    fun `a sorted rewrite that spills and merges copies another layout byte for byte too`() {
        val out = tmp.resolve("other-sorted.parquet")
        val result =
            ParquetRewriter.rewrite(
                listOf(localInput(fixture("other_a"), 0, dv(0)), localInput(fixture("other_b"), 100)),
                live,
                listOf(SortFieldDef(1, SortDirection.ASC, NullOrder.NULLS_LAST)),
                LocalDiscardableOutput(out),
                spill =
                    SortSpill(
                        chunkRows = 2,
                        mergeBudgetBytes = Long.MAX_VALUE / 4,
                        spillBudgetBytes = Long.MAX_VALUE / 4,
                        spillDir = tmp,
                    ),
                variantCompaction = true,
                variantLayouts = probe("other_b"),
            )
        // Spill files are written in the OUTPUT schema — the other layout —
        // and read back as themselves.
        assertThat(result.runsSpilled).isGreaterThan(1)
        assertThat(rows(out))
            .isEqualTo(expected(Source(fixture("other_a"), 0, setOf(0)), Source(fixture("other_b"), 100)))
        assertThat(read(out).second.map { it.getLong("id", 0) }).isSorted().hasSize(23)
        assertThat(read(out).first.getType("props"))
            .isEqualTo(VariantLayout.groupType(other[1], Type.Repetition.OPTIONAL))
    }

    @Test
    fun `an input in the declared layout is refused, typed, when another layout is named for it`() {
        // The backstop behind VariantLayoutGroups: the rewrite holds every
        // input to the layout it writes, whichever that is.
        val out = tmp.resolve("declared-under-other.parquet")
        assertThatThrownBy {
            ParquetRewriter.rewrite(
                listOf(localInput(fixture("other_b"), 0), localInput(fixture("b"), 100)),
                live,
                emptyList(),
                LocalDiscardableOutput(out),
                variantCompaction = true,
                variantLayouts = probe("other_b"),
            )
        }.isInstanceOfSatisfying(VariantLayoutMismatchException::class.java) {
            assertThat(it.reason).isEqualTo("layout")
            assertThat(it.message).contains("variant column 'props'", "b.parquet")
        }
        assertThat(out).doesNotExist()
    }

    @Test
    fun `the probe reads the declared layout off pyhoglake's files, and refuses what is no variant group`() {
        assertThat(probe("a")).isEqualTo(VariantLayout.declared(live))
        assertThat(probe("b")).isEqualTo(VariantLayout.declared(live))
        assertThat(probe("mismatch")).isEqualTo(probe("other_a")).isNotEqualTo(VariantLayout.declared(live))
        // DuckDB's file is b's bytes under another created_by: the probe
        // reads its layout like any other; excluding it is the caller's.
        assertThat(probe("duckdb")).isEqualTo(VariantLayout.declared(live))
        // Fingerprints follow: equal layouts, equal fingerprints.
        assertThat(VariantLayout.fingerprint(probe("a")))
            .isEqualTo(VariantLayout.fingerprint(VariantLayout.declared(live)))
        assertThat(VariantLayout.fingerprint(probe("other_a"))).isNotEqualTo(VariantLayout.fingerprint(probe("a")))

        val schema =
            MessageTypeParser.parseMessageType(
                "message m { required int64 id = 1; optional binary props = 2; }",
            )
        val leaf = tmp.resolve("leaf.parquet")
        ExampleParquetWriter.builder(LocalOutputFile(leaf)).withType(schema).build().use { w ->
            w.write(SimpleGroupFactory(schema).newGroup().append("id", 1L))
        }
        assertThatThrownBy { probe(leaf) }
            .isInstanceOfSatisfying(VariantLayoutMismatchException::class.java) {
                assertThat(it.reason).isEqualTo("layout")
                assertThat(it.message).contains("not one VARIANT group")
            }
    }

    @Test
    fun `a variant whose value is REPEATED is refused by the probe and by the rewrite, so no value is lost`() {
        // Two values per row in a repeated child: the copy would keep the
        // first, drop the second, and commit with the row count intact.
        val schema =
            MessageTypeParser.parseMessageType(
                "message m { required int64 id = 1; " +
                    "optional group props (VARIANT(1)) = 2 { required binary metadata; repeated binary value; } }",
            )
        val path = tmp.resolve("repeated-value.parquet")
        ExampleParquetWriter.builder(LocalOutputFile(path)).withType(schema).build().use { w ->
            val factory = SimpleGroupFactory(schema)
            for (i in 0 until 3) {
                val row = factory.newGroup().append("id", i.toLong())
                val props = row.addGroup("props")
                props.add("metadata", Binary.fromConstantByteArray(byteArrayOf(1, 0, 0)))
                props.add("value", Binary.fromConstantByteArray(byteArrayOf(0)))
                props.add("value", Binary.fromConstantByteArray(byteArrayOf(12, 1)))
                w.write(row)
            }
        }
        assertThatThrownBy { probe(path) }
            .isInstanceOfSatisfying(VariantLayoutMismatchException::class.java) {
                assertThat(it.reason).isEqualTo("layout")
                assertThat(it.message).contains("`props.value` is REPEATED outside a LIST or MAP layer")
            }
        // The rewrite, under the declared layout, under another named
        // layout, and under the file's OWN subtree named as the layout
        // (what a probe that let it through would have handed the rewrite):
        // there is no layout this can be written in, and the last case is
        // the one only the subtree check refuses — layoutMismatch passes a
        // subtree against itself.
        val own = mapOf(2L to schema.getType("props").asGroupType())
        assertThat(VariantLayout.layoutMismatch(schema.getType("props"), own.getValue(2L))).isNull()
        for ((label, layouts) in listOf("declared" to emptyMap(), "other" to probe("a"), "own" to own)) {
            val out = tmp.resolve("repeated-out-$label.parquet")
            assertThatThrownBy {
                ParquetRewriter.rewrite(
                    listOf(localInput(path, 0)),
                    live,
                    emptyList(),
                    LocalDiscardableOutput(out),
                    variantCompaction = true,
                    variantLayouts = layouts,
                )
            }.describedAs(label)
                .isInstanceOfSatisfying(VariantLayoutMismatchException::class.java) {
                    assertThat(it.reason).describedAs(label).isEqualTo("layout")
                    assertThat(
                        it.message,
                    ).describedAs(label).contains("`props.value` is REPEATED outside a LIST or MAP layer")
                }
            assertThat(out).doesNotExist()
        }
    }

    @Test
    fun `a variant written value-before-metadata is a layout of its own, probed and copied in its order`() {
        // Spark's order. The probe reads it as a layout (metadata is found
        // by name), and a group of such files rewrites into that very order,
        // every leaf byte for byte.
        val schema =
            MessageTypeParser.parseMessageType(
                "message m { required int64 id = 1; " +
                    "optional group props (VARIANT(1)) = 2 { required binary value; required binary metadata; } }",
            )
        val path = tmp.resolve("value-first.parquet")
        ExampleParquetWriter.builder(LocalOutputFile(path)).withType(schema).build().use { w ->
            val factory = SimpleGroupFactory(schema)
            for (i in 0 until 4) {
                val row = factory.newGroup().append("id", i.toLong())
                if (i != 2) {
                    val props = row.addGroup("props")
                    props.add("value", Binary.fromConstantByteArray(byteArrayOf(12, i.toByte())))
                    props.add("metadata", Binary.fromConstantByteArray(byteArrayOf(1, 0, 0)))
                }
                w.write(row)
            }
        }
        val layouts = probe(path)
        assertThat(layouts.getValue(2L).fields.map { it.name }).containsExactly("value", "metadata")
        val out = tmp.resolve("value-first-out.parquet")
        val result =
            ParquetRewriter.rewrite(
                listOf(localInput(path, 0, dv(1)), localInput(path, 100)),
                live.take(2),
                emptyList(),
                LocalDiscardableOutput(out),
                variantCompaction = true,
                variantLayouts = layouts,
            )
        assertThat(result.rowsWritten).isEqualTo(7)
        val written = read(out)
        assertThat(written.first.getType("props")).isEqualTo(layouts.getValue(2L))
        val expectedRows = VariantFixtures.read(path).second
        val byId = written.second.associateBy { it.getLong(ParquetRewriter.ROW_ID_COLUMN, 0) }
        for ((position, source) in expectedRows.withIndex()) {
            for (start in listOf(0L, 100L)) {
                if (start == 0L && position == 1) continue
                val got = byId.getValue(start + position)
                assertThat(VariantFixtures.render(got, got.type.getFieldIndex("props")))
                    .describedAs("row $position from $start")
                    .isEqualTo(VariantFixtures.render(source, source.type.getFieldIndex("props")))
            }
        }
    }

    @Test
    fun `an input opened over its probed footer reads exactly as one opened cold`() {
        val footer = ParquetFileReader.open(LocalInputFile(fixture("a"))).use { it.footer }
        val out = tmp.resolve("footer-out.parquet")
        val warm = localInput(fixture("a"), 0, dv(2)).copy(footer = footer)
        rewriteToLocal(listOf(warm, localInput(fixture("b"), 100)), live, emptyList(), out, variantCompaction = true)
        assertThat(rows(out)).isEqualTo(expected(Source(fixture("a"), 0, setOf(2)), Source(fixture("b"), 100)))
    }

    // ---- the comparison ------------------------------------------------------

    private fun probe(name: String) = probe(fixture(name))

    private fun probe(path: Path) =
        ParquetFileReader.open(LocalInputFile(path)).use {
            ParquetRewriter.variantLayoutsOf(it.footer, live, path.toString())
        }

    private fun expected(vararg inputs: Source) = VariantFixtures.expected(*inputs)

    private fun rows(path: Path) = VariantFixtures.rows(path)

    private fun read(path: Path) = VariantFixtures.read(path)

    private fun dv(vararg positions: Long) =
        PuffinDeletionVector.read(
            PuffinTestFiles.deletionVector(positions.toList()),
        )

    private fun fixture(name: String): Path = VariantFixtures.path(name)

    /** The live table the fixtures were written for: id, then props, rich and raw. */
    private val live: List<Column> get() = VariantFixtures.live

    /** The same table with `props` under the declaration the `other_*` fixtures were written to. */
    private val other: List<Column> get() = VariantFixtures.other
}
