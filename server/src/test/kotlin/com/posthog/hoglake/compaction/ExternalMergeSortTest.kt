package com.posthog.hoglake.compaction

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import com.sun.management.UnixOperatingSystemMXBean
import io.kotest.property.Arb
import io.kotest.property.arbitrary.arbitrary
import io.kotest.property.arbitrary.boolean
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.list
import io.kotest.property.arbitrary.of
import io.kotest.property.checkAll
import kotlinx.coroutines.runBlocking
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.io.SeekableInputStream
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.io.IOException
import java.lang.management.ManagementFactory
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.PosixFilePermissions
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.exists
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.name

/**
 * The sorted rewrite as an external merge sort (hoglake#134): output
 * equivalence against an in-memory reference, the tie guarantee as the
 * class doc states it, admission and demotion, both budgets' refusals,
 * the spill directory's lifecycle and reader closing on every exit path,
 * the DV check at a trusted run's exhaustion, and spill read-back of
 * every scalar type. Nested spill read-back rides
 * [NestedTypeRewriteRoundTripTest]'s forced-spill pass.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExternalMergeSortTest {
    private val tmp: Path = Files.createTempDirectory("external-merge-sort")
    private var seq = 0

    @AfterAll
    fun tearDown() {
        tmp.toFile().deleteRecursively()
    }

    private fun fresh(name: String): Path = tmp.resolve("${seq++}-$name")

    private fun spillParent(): Path = Files.createDirectory(fresh("spill"))

    // ---- a small table: k long (1), v string (2) ---------------------------

    private val kv =
        listOf(
            Column(1, 0, ColumnDef("k", ColType.LONG)),
            Column(2, 1, ColumnDef("v", ColType.STRING)),
        )
    private val byK = listOf(SortFieldDef(1, SortDirection.ASC, NullOrder.NULLS_LAST))

    private val kvSchema: MessageType =
        Types.buildMessage()
            .addField(Types.optional(PrimitiveTypeName.INT64).id(1).named("k"))
            .addField(
                Types.optional(PrimitiveTypeName.BINARY).`as`(LogicalTypeAnnotation.stringType()).id(2).named("v"),
            )
            .named("t")

    /** The compaction-output shape: k, v, and the reserved row-id carrier. */
    private val kvOutputSchema: MessageType =
        Types.buildMessage()
            .addField(Types.optional(PrimitiveTypeName.INT64).id(1).named("k"))
            .addField(
                Types.optional(PrimitiveTypeName.BINARY).`as`(LogicalTypeAnnotation.stringType()).id(2).named("v"),
            )
            .addField(
                Types.required(PrimitiveTypeName.INT64).id(ParquetRewriter.ROW_ID_FIELD_ID)
                    .named(ParquetRewriter.ROW_ID_COLUMN),
            )
            .named("t")

    private fun write(
        name: String,
        schema: MessageType,
        rows: List<(Group) -> Unit>,
    ): Path {
        val path = fresh("$name.parquet")
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

    /** A client file of (k, "v<k>") rows; positional ids. */
    private fun clientKv(
        name: String,
        keys: List<Long>,
    ): Path = write(name, kvSchema, keys.map { k -> { g: Group -> g.add(0, k).also { g.add(1, "v$k") } } })

    /** A compaction-output-shaped file: (k, rowId) pairs in the order given. */
    private fun outputKv(
        name: String,
        rows: List<Pair<Long, Long>>,
    ): Path =
        write(
            name,
            kvOutputSchema,
            rows.map { (k, id) -> { g: Group -> g.add(0, k).also { g.add(1, "v$k") }.also { g.add(2, id) } } },
        )

    private fun dv(positions: Collection<Long>): DeletionVector? =
        if (positions.isEmpty()) null else PuffinDeletionVector.read(PuffinTestFiles.deletionVector(positions.toList()))

    private fun input(
        source: InputFile,
        label: String,
        rowIdStart: Long = 0,
        deletes: DeletionVector? = null,
        trusted: Boolean = false,
        fileSizeBytes: Long = source.length,
        survivors: Long = 0,
    ) = ParquetRewriter.Input(
        source = source,
        label = label,
        rowIdStart = rowIdStart,
        deletes = deletes,
        explicitRowIds = trusted,
        trustedSorted = trusted,
        fileSizeBytes = fileSizeBytes,
        survivingRecords = survivors,
    )

    /**
     * Bounds in BYTES the tests can reason about exactly: no readahead,
     * so a trusted run costs its file size (estimate) or its largest row
     * group (exact), and a spilled run costs [block].
     */
    private fun spill(
        dir: Path,
        chunkRows: Long = Long.MAX_VALUE,
        mergeBudget: Long = Long.MAX_VALUE / 4,
        spillBudget: Long = Long.MAX_VALUE / 4,
        block: Int = 1,
    ) = SortSpill(
        chunkRows = chunkRows,
        mergeBudgetBytes = mergeBudget,
        spillBudgetBytes = spillBudget,
        spillDir = dir,
        spillBlockBytes = block,
        readaheadBytes = 0,
    )

    private fun rewrite(
        inputs: List<ParquetRewriter.Input>,
        out: Path,
        spill: SortSpill,
        live: List<Column> = kv,
        sort: List<SortFieldDef> = byK,
        parallelism: Int = 1,
    ): ParquetRewriter.RewriteResult =
        ParquetRewriter.rewrite(
            inputs,
            live,
            sort,
            LocalDiscardableOutput(out),
            inputOpenParallelism = parallelism,
            spill = spill,
        )

    /** Every row of [path] in file order, by the reader's own schema. */
    private fun readGroups(path: Path): Pair<MessageType, List<Group>> {
        val out = mutableListOf<Group>()
        lateinit var schema: MessageType
        ParquetFileReader.open(LocalInputFile(path)).use { reader ->
            schema = reader.footer.fileMetaData.schema
            val io = ColumnIOFactory().getColumnIO(schema)
            var pages = reader.readNextRowGroup()
            while (pages != null) {
                val rr = io.getRecordReader(pages, GroupRecordConverter(schema))
                repeat(Math.toIntExact(pages.rowCount)) { out += rr.read() }
                pages = reader.readNextRowGroup()
            }
        }
        return schema to out
    }

    /** (k, rowId) of a kv output, in file order. */
    private fun readKv(path: Path): List<Pair<Long?, Long>> {
        val (schema, groups) = readGroups(path)
        val k = schema.getFieldIndex("k")
        val id = schema.getFieldIndex(ParquetRewriter.ROW_ID_COLUMN)
        return groups.map {
                g ->
            (if (g.getFieldRepetitionCount(k) == 0) null else g.getLong(k, 0)) to g.getLong(id, 0)
        }
    }

    // ---- reference equivalence ------------------------------------------------

    /** One generated row of the property table; null fields are SQL nulls. */
    private data class Rec(
        val a: Long?,
        val u: Long?,
        val f: Double?,
        val d: BigInteger?,
        val s: Pair<Int?, String?>?,
        val str: String?,
    )

    private val propLive =
        listOf(
            Column(1, 0, ColumnDef("a", ColType.LONG)),
            Column(2, 1, ColumnDef("u", ColType.UINT64)),
            Column(3, 2, ColumnDef("f", ColType.DOUBLE)),
            Column(4, 3, ColumnDef("d", ColType.DECIMAL, mapOf("precision" to 10, "scale" to 2))),
            Column(
                5,
                4,
                ColumnDef("s", ColType.STRUCT),
                listOf(
                    Column(6, 0, ColumnDef("x", ColType.INT)),
                    Column(7, 1, ColumnDef("t", ColType.STRING)),
                ),
            ),
            Column(8, 5, ColumnDef("str", ColType.STRING)),
        )

    private val propSchema: MessageType =
        Types.buildMessage()
            .addField(Types.optional(PrimitiveTypeName.INT64).id(1).named("a"))
            .addField(
                Types.optional(PrimitiveTypeName.INT64).`as`(LogicalTypeAnnotation.intType(64, false)).id(2).named("u"),
            )
            .addField(Types.optional(PrimitiveTypeName.DOUBLE).id(3).named("f"))
            .addField(
                Types.optional(
                    PrimitiveTypeName.BINARY,
                ).`as`(LogicalTypeAnnotation.decimalType(2, 10)).id(4).named("d"),
            )
            .addField(
                Types.optionalGroup()
                    .addField(Types.optional(PrimitiveTypeName.INT32).id(6).named("x"))
                    .addField(
                        Types.optional(
                            PrimitiveTypeName.BINARY,
                        ).`as`(LogicalTypeAnnotation.stringType()).id(7).named("t"),
                    )
                    .id(5).named("s"),
            )
            .addField(
                Types.optional(PrimitiveTypeName.BINARY).`as`(LogicalTypeAnnotation.stringType()).id(8).named("str"),
            )
            .named("t")

    private fun fill(
        g: Group,
        r: Rec,
    ) {
        r.a?.let { g.add(0, it) }
        r.u?.let { g.add(1, it) }
        r.f?.let { g.add(2, it) }
        r.d?.let { g.add(3, Binary.fromConstantByteArray(it.toByteArray())) }
        r.s?.let { (x, t) ->
            val s = g.addGroup(4)
            x?.let { s.add(0, it) }
            t?.let { s.add(1, it) }
        }
        r.str?.let { g.add(5, it) }
    }

    private fun decode(
        schema: MessageType,
        g: Group,
    ): Rec {
        fun has(name: String) = g.getFieldRepetitionCount(schema.getFieldIndex(name)) > 0
        val s =
            if (has("s")) {
                val sg = g.getGroup(schema.getFieldIndex("s"), 0)
                (if (sg.getFieldRepetitionCount(0) > 0) sg.getInteger(0, 0) else null) to
                    (if (sg.getFieldRepetitionCount(1) > 0) sg.getString(1, 0) else null)
            } else {
                null
            }
        return Rec(
            a = if (has("a")) g.getLong(schema.getFieldIndex("a"), 0) else null,
            u = if (has("u")) g.getLong(schema.getFieldIndex("u"), 0) else null,
            f = if (has("f")) g.getDouble(schema.getFieldIndex("f"), 0) else null,
            d = if (has("d")) BigInteger(g.getBinary(schema.getFieldIndex("d"), 0).bytes) else null,
            s = s,
            str = if (has("str")) g.getString(schema.getFieldIndex("str"), 0) else null,
        )
    }

    /**
     * The reference order, written independently of the rewriter's
     * comparator: per key, nulls placed by the null order whatever the
     * direction, unsigned for `u`, Double.compareTo for `f` (NaN
     * greatest, -0.0 below 0.0), signed unscaled for `d`; then row id.
     */
    private fun reference(keys: List<SortFieldDef>): Comparator<Pair<Long, Rec>> =
        Comparator { (aId, a), (bId, b) ->
            for (key in keys) {
                val av = valueOf(key.sourceFieldId, a)
                val bv = valueOf(key.sourceFieldId, b)
                if (av == null || bv == null) {
                    if (av == null && bv == null) continue
                    val nullsFirst = key.nullOrder == NullOrder.NULLS_FIRST
                    return@Comparator if ((av == null) == nullsFirst) -1 else 1
                }
                var c =
                    if (key.sourceFieldId == 2L) {
                        java.lang.Long.compareUnsigned(av as Long, bv as Long)
                    } else if (av is String) {
                        compareCodePoints(av, bv as String)
                    } else {
                        @Suppress("UNCHECKED_CAST")
                        (av as Comparable<Any>).compareTo(bv)
                    }
                if (key.direction == SortDirection.DESC) c = -c
                if (c != 0) return@Comparator c
            }
            aId.compareTo(bId)
        }

    /**
     * Code-point order, which is what the rewriter's unsigned UTF-8 byte
     * compare produces; String.compareTo is UTF-16 order, which puts a
     * supplementary character (a surrogate pair, D800-DBFF first) below
     * U+E000..U+FFFF.
     */
    private fun compareCodePoints(
        a: String,
        b: String,
    ): Int = java.util.Arrays.compare(a.codePoints().toArray(), b.codePoints().toArray())

    private fun valueOf(
        fieldId: Long,
        r: Rec,
    ): Any? =
        when (fieldId) {
            1L -> r.a
            2L -> r.u
            3L -> r.f
            4L -> r.d
            6L -> r.s?.first
            7L -> r.s?.second
            8L -> r.str
            else -> error("no field $fieldId")
        }

    private fun <T> maybe(arb: Arb<T>): Arb<T?> = arbitrary { if (Arb.int(0..4).bind() == 0) null else arb.bind() }

    private val arbRec: Arb<Rec> =
        arbitrary {
            Rec(
                a = maybe(Arb.of(-3L, -1L, 0L, 1L, 2L)).bind(),
                u = maybe(Arb.of(0L, 1L, 5L, Long.MIN_VALUE, -1L)).bind(),
                f =
                    maybe(
                        Arb.of(Double.NaN, -0.0, 0.0, 1.5, -1.5, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY),
                    ).bind(),
                d = maybe(Arb.of(-12_345L, -1L, 0L, 1L, 99_999L)).bind()?.let { BigInteger.valueOf(it) },
                s =
                    maybe(
                        arbitrary {
                            maybe(Arb.of(-2, 0, 2)).bind() to
                                maybe(Arb.of("", "a", "ab", "b", "\uFFFD", "\uD83D\uDE00")).bind()
                        },
                    ).bind(),
                str = maybe(Arb.of("", "x", "xy", "y", "\uFFFD", "\uD83D\uDE00", "x\uD83D\uDE00")).bind(),
            )
        }

    private class GenInput(
        val rows: List<Rec>,
        val trusted: Boolean,
        val dvSeed: List<Boolean>,
    )

    private class Case(
        val inputs: List<GenInput>,
        /** Empty: the UNSORTED path, whose output is compared as a set. */
        val keys: List<SortFieldDef>,
        val chunkRows: Long,
        /** Merge budget: 0 unbounded, 1 halfway, 2 just enough for every survivor spilled. */
        val demotion: Int,
        val parallelism: Int,
        /**
         * The append floor: 0 makes every DV-free prior output (a trusted
         * input, written through the rewriter first) appendable byte for
         * byte, so the oracle covers appended runs; MAX appends nothing.
         */
        val appendFloor: Long,
    )

    private val arbKey: Arb<SortFieldDef> =
        arbitrary {
            SortFieldDef(
                Arb.of(1L, 2L, 3L, 4L, 6L, 7L, 8L).bind(),
                Arb.of(SortDirection.ASC, SortDirection.DESC).bind(),
                Arb.of(NullOrder.NULLS_FIRST, NullOrder.NULLS_LAST).bind(),
            )
        }

    private val arbCase: Arb<Case> =
        arbitrary {
            // BANDED inputs put every row's `a` in a band of their own
            // (input i: 100(i+1) .. 100(i+1)+3, never null), so with `a` as
            // the first key their ranges are disjoint from every other
            // input's — the shape a sorted append needs. Unbanded ones
            // overlap everything.
            val banded = Arb.boolean().bind()
            val inputs =
                Arb.list(
                    arbitrary {
                        val band = if (banded && Arb.int(0..3).bind() > 0) 100L * (Arb.int(1..50).bind()) else null
                        val rows =
                            Arb.list(arbRec, 0..25).bind().map { r ->
                                if (band == null) r else r.copy(a = band + Arb.int(0..3).bind())
                            }
                        // Two inputs in three have no DV at all: an input
                        // with one is never appended.
                        val holes = Arb.int(0..2).bind() == 0
                        GenInput(rows, Arb.boolean().bind(), rows.map { holes && Arb.int(0..5).bind() == 0 })
                    },
                    1..5,
                ).bind()
            val first =
                if (banded) {
                    listOf(
                        SortFieldDef(
                            1,
                            Arb.of(SortDirection.ASC, SortDirection.DESC).bind(),
                            Arb.of(NullOrder.NULLS_FIRST, NullOrder.NULLS_LAST).bind(),
                        ),
                    )
                } else {
                    emptyList()
                }
            Case(
                inputs = inputs,
                keys =
                    if (Arb.int(0..4).bind() == 0) {
                        emptyList()
                    } else {
                        (first + Arb.list(arbKey, 1..3).bind()).distinctBy { it.sourceFieldId }
                    },
                chunkRows = Arb.of(1L, 2L, 7L, 1_000_000L).bind(),
                demotion = Arb.int(0..2).bind(),
                parallelism = Arb.of(1, 3).bind(),
                appendFloor = Arb.of(0L, Long.MAX_VALUE).bind(),
            )
        }

    @Test
    fun `the merge equals an in-memory sort by (keys, row id) for any mix of runs and budgets`() {
        appendedSorted = 0
        appendedUnsorted = 0
        runBlocking {
            checkAll(300, arbCase) { case -> checkCase(case) }
        }
        // The oracle must actually have SEEN appended runs on both paths,
        // or it covers nothing about them.
        assertThat(appendedSorted).describedAs("sorted cases with an appended run").isPositive()
        assertThat(appendedUnsorted).describedAs("unsorted cases with an appended input").isPositive()
    }

    private var appendedSorted = 0
    private var appendedUnsorted = 0

    private fun checkCase(case: Case) {
        val expected = ArrayList<Pair<Long, Rec>>()
        val inputs = ArrayList<ParquetRewriter.Input>()
        for ((i, gen) in case.inputs.withIndex()) {
            val rowIdStart = i * 1_000L
            val client = write("prop-$i", propSchema, gen.rows.map { r -> { g: Group -> fill(g, r) } })
            // A trusted run is what this rewriter itself writes: the
            // client file compacted under the SAME keys, so its physical
            // order is (keys, row id) and its ids ride the carrier.
            val (path, physical) =
                if (gen.trusted) {
                    val out = fresh("prop-$i-trusted.parquet")
                    rewriteToLocal(listOf(localInput(client, rowIdStart)), propLive, case.keys, out)
                    val (schema, groups) = readGroups(out)
                    val id = schema.getFieldIndex(ParquetRewriter.ROW_ID_COLUMN)
                    out to groups.map { it.getLong(id, 0) to decode(schema, it) }
                } else {
                    client to gen.rows.mapIndexed { pos, r -> rowIdStart + pos to r }
                }
            val deleted = physical.indices.filter { gen.dvSeed[it] }.map { it.toLong() }.toSet()
            physical.filterIndexed { pos, _ -> pos.toLong() !in deleted }.forEach { expected += it }
            inputs +=
                input(
                    LocalInputFile(path),
                    "prop-$i",
                    rowIdStart = rowIdStart,
                    deletes = dv(deleted),
                    trusted = gen.trusted,
                    survivors = physical.size - deleted.size.toLong(),
                )
        }
        expected.sortWith(reference(case.keys))
        val sorted = case.keys.isNotEmpty()

        val parent = spillParent()
        val bounds = spill(parent, chunkRows = case.chunkRows)
        val leaves = 8 // a, u, f, d, s.x, s.t, str, _hog_row_id
        val trustedBytes =
            inputs.filter { it.trustedSorted }.sumOf { ExternalMergeSort.estimatedTrustedRunBytes(it, leaves, bounds) }
        // Every survivor spilled: the cost with nothing admitted, which
        // must always fit or the group is refused.
        val allSpilled =
            ExternalMergeSort.spillRuns(expected.size.toLong(), bounds) *
                ExternalMergeSort.predictedSpilledRunBytes(leaves, bounds)
        val budget =
            when (case.demotion) {
                0 -> Long.MAX_VALUE / 4
                1 -> maxOf(1, allSpilled + trustedBytes / 2)
                else -> maxOf(1, allSpilled)
            }
        val out = fresh("prop-out.parquet")
        val result =
            ParquetRewriter.rewrite(
                inputs,
                propLive,
                case.keys,
                LocalDiscardableOutput(out),
                inputOpenParallelism = case.parallelism,
                spill = if (sorted) bounds.copy(mergeBudgetBytes = budget) else null,
                appendFloorBytes = case.appendFloor,
            )

        val (schema, groups) = readGroups(out)
        val id = schema.getFieldIndex(ParquetRewriter.ROW_ID_COLUMN)
        val actual = groups.map { it.getLong(id, 0) to decode(schema, it) }
        if (sorted) {
            assertThat(actual.map { it.first }).describedAs("row-id order").isEqualTo(expected.map { it.first })
            // Rec equality on Double is Double.equals, so NaN == NaN and -0.0 != 0.0.
            assertThat(actual).describedAs("rows").isEqualTo(expected)
        } else {
            // No order to keep: the same rows, each under its own id.
            assertThat(actual.sortedBy { it.first }).describedAs("rows").isEqualTo(expected.sortedBy { it.first })
        }
        assertThat(result.rowsWritten).isEqualTo(expected.size.toLong())
        assertThat(result.minRowId).isEqualTo(expected.minOfOrNull { it.first })
        if (sorted) {
            val trusted = inputs.count { it.trustedSorted }
            assertThat(result.runsTrusted + result.runsDemoted).isEqualTo(trusted)
            if (case.demotion == 0) assertThat(result.runsDemoted).isZero()
        }
        if (case.appendFloor == Long.MAX_VALUE) assertThat(result.rowGroupsAppended).isZero()
        if (result.rowGroupsAppended > 0) {
            if (sorted) appendedSorted++ else appendedUnsorted++
            // The footer's row groups are the appended ones plus the
            // encoded ones; every row is in one of them.
            assertThat(result.footer!!.blocks.sumOf { it.rowCount }).isEqualTo(expected.size.toLong())
        }
        assertThat(parent.listDirectoryEntries()).describedAs("spill dirs left behind").isEmpty()
    }

    // ---- the tie guarantee ----------------------------------------------------

    @Test
    fun `a trusted run whose ties are out of row-id order keeps KEY order exact`() {
        // An output written before #134 broke ties in INPUT order, so its
        // ties can be out of row-id order. The class doc promises exact
        // KEY order and nothing about such ties; pin exactly that.
        val trusted = outputKv("ties-trusted", listOf(1L to 10L, 1L to 5L, 1L to 30L, 2L to 20L, 3L to 2L))
        val client = clientKv("ties-client", listOf(3, 1, 2, 1))
        val out = fresh("ties-out.parquet")
        rewrite(
            listOf(
                input(LocalInputFile(trusted), "trusted", trusted = true),
                input(LocalInputFile(client), "client", 100),
            ),
            out,
            spill(spillParent(), chunkRows = 1),
        )
        val rows = readKv(out)
        assertThat(rows.map { it.first }).isSorted()
        assertThat(rows.map { it.second }).containsExactlyInAnyOrder(10, 5, 30, 20, 2, 100, 101, 102, 103)
    }

    // ---- the arithmetic -----------------------------------------------------------

    @Test
    fun `spilled runs are the ceiling of rows over chunk rows`() {
        val bounds = spill(tmp, chunkRows = 2)
        assertThat(listOf(0L, 1L, 2L, 4L, 5L).map { ExternalMergeSort.spillRuns(it, bounds) })
            .containsExactly(0L, 1L, 1L, 2L, 3L)
    }

    @Test
    fun `a lone chunk with no trusted run costs nothing, anything else costs its runs`() {
        assertThat(ExternalMergeSort.mergeCost(0, 0, 0, 0)).isZero()
        assertThat(ExternalMergeSort.mergeCost(0, 0, 1, 500)).isZero()
        assertThat(ExternalMergeSort.mergeCost(0, 0, 2, 500)).isEqualTo(500)
        assertThat(ExternalMergeSort.mergeCost(1, 70, 0, 0)).isEqualTo(70)
        assertThat(ExternalMergeSort.mergeCost(1, 70, 1, 500)).isEqualTo(570)
    }

    @Test
    fun `run estimates charge the row group, a page per leaf and the stream buffer`() {
        val bounds = spill(tmp, block = 10).copy(readaheadBytes = 7)
        val page = ExternalMergeSort.TRUSTED_PAGE_BYTES_PER_LEAF
        assertThat(page).isEqualTo(2L * 1024 * 1024)
        assertThat(ExternalMergeSort.estimatedTrustedRunBytes(fake("t", 100, 0), 3, bounds))
            .isEqualTo(100 + 3 * page + 7)
        assertThat(ExternalMergeSort.estimatedTrustedRunBytes(fake("t", 0, 0), 3, bounds))
            .describedAs("unknown size: a full output block")
            .isEqualTo(ExternalMergeSort.OUTPUT_BLOCK_BYTES + 3 * page + 7)
        assertThat(ExternalMergeSort.estimatedTrustedRunBytes(fake("t", 1L shl 40, 0), 3, bounds))
            .describedAs("no row group exceeds the output block")
            .isEqualTo(ExternalMergeSort.OUTPUT_BLOCK_BYTES + 3 * page + 7)
        assertThat(ExternalMergeSort.predictedSpilledRunBytes(3, bounds))
            .isEqualTo(10 + 3 * ExternalMergeSort.SPILL_PAGE_BYTES_PER_LEAF)
    }

    @Test
    fun `a footer costs its largest row group plus each leaf's page, capped by the leaf's chunk`() {
        fun column(
            name: String,
            compressed: Long,
            uncompressed: Long,
        ) = org.apache.parquet.hadoop.metadata.ColumnChunkMetaData.get(
            org.apache.parquet.hadoop.metadata.ColumnPath.get(name),
            PrimitiveTypeName.INT64,
            CompressionCodecName.SNAPPY,
            emptySet(),
            0,
            0,
            1,
            compressed,
            uncompressed,
        )

        fun rowGroup(vararg columns: org.apache.parquet.hadoop.metadata.ColumnChunkMetaData) =
            org.apache.parquet.hadoop.metadata.BlockMetaData().apply { columns.forEach { addColumn(it) } }
        val footer =
            org.apache.parquet.hadoop.metadata.ParquetMetadata(
                null,
                listOf(
                    // 100 compressed; leaves capped at 50: 30 + 50.
                    rowGroup(column("a", 40, 30), column("b", 60, 900)),
                    // 300 compressed; 10 + 20.
                    rowGroup(column("a", 290, 10), column("b", 10, 20)),
                ),
            )
        assertThat(ExternalMergeSort.footerRunBytes(footer, 50)).isEqualTo(maxOf(100L + 30 + 50, 300L + 10 + 20))
        assertThat(
            ExternalMergeSort.footerRunBytes(org.apache.parquet.hadoop.metadata.ParquetMetadata(null, emptyList()), 50),
        ).isZero()
    }

    @Test
    fun `an INT(32, unsigned) key compares unsigned`() {
        // No output schema compaction writes carries one (uint32 is written
        // as INT64), so the rewriter cannot reach this shape; SortKeys is
        // handed it directly, because the comparator must not depend on
        // what today's writer happens to emit.
        val schema =
            Types.buildMessage()
                .addField(
                    Types.optional(
                        PrimitiveTypeName.INT32,
                    ).`as`(LogicalTypeAnnotation.intType(32, false)).id(1).named("u"),
                )
                .addField(Types.required(PrimitiveTypeName.INT64).id(2).named("id"))
                .named("t")
        val keys = ParquetRewriter.SortKeys(schema, listOf(SortFieldDef(1, SortDirection.ASC, NullOrder.NULLS_LAST)))
        val factory = SimpleGroupFactory(schema)

        fun row(
            value: Int,
            id: Long,
        ): ParquetRewriter.Row {
            val g = factory.newGroup().apply { add(0, value) }
            return ParquetRewriter.Row(g, id, keys.extract(g, "test"))
        }
        // -1 is 2^32 - 1 unsigned: greater than 1.
        assertThat(keys.comparator.compare(row(-1, 0), row(1, 1))).isPositive()
        assertThat(keys.comparator.compare(row(Int.MIN_VALUE, 0), row(Int.MAX_VALUE, 1))).isPositive()
    }

    // ---- admission (pure) -------------------------------------------------------------

    /** An input admission can cost but nothing will open. */
    private fun fake(
        name: String,
        size: Long,
        rows: Long,
        trusted: Boolean = true,
    ) = ParquetRewriter.Input(
        source = LocalInputFile(tmp.resolve("never-opened-$name")),
        label = name,
        rowIdStart = 0,
        explicitRowIds = trusted,
        trustedSorted = trusted,
        fileSizeBytes = size,
        survivingRecords = rows,
    )

    /** Admission with no page term: the arithmetic below is in plain bytes. */
    private fun admit(
        inputs: List<ParquetRewriter.Input>,
        bounds: SortSpill,
    ) = ExternalMergeSort.admit(inputs, bounds, leaves = 0)

    @Test
    fun `admission is largest first and first fit`() {
        val large = fake("large", 3_000, 300)
        val medium = fake("medium", 1_000, 100)
        val small = fake("small", 100, 5)
        // Room for large + small + medium's one spilled run, not for medium as a run.
        val result = admit(listOf(small, medium, large), spill(tmp, mergeBudget = 3_000 + 100 + 1))
        assertThat(result.admitted).containsExactly(large, small)
        assertThat(result.demoted).containsExactly(medium)
        assertThat(result.projectedBytes).isEqualTo(3_101)
    }

    @Test
    fun `admitted trusted runs need no spill reservation for their own rows`() {
        // 130 rows at 10 per chunk would be 13 runs of 1,000 B if spilled.
        val result =
            admit(
                listOf(fake("big", 4_000, 100), fake("small", 1_500, 30)),
                spill(tmp, chunkRows = 10, mergeBudget = 5_500, block = 1_000),
            )
        assertThat(result.admitted).hasSize(2)
        assertThat(result.demoted).isEmpty()
    }

    @Test
    fun `readahead is charged per trusted run`() {
        fun admitted(readahead: Int) =
            admit(
                listOf(fake("t", 1_000, 10)),
                spill(tmp, mergeBudget = 1_100).copy(readaheadBytes = readahead),
            ).admitted
        assertThat(admitted(100)).hasSize(1)
        assertThat(admitted(101)).isEmpty()
    }

    @Test
    fun `a refused candidate's rows are spilled runs the next candidate is costed against`() {
        // `large` is refused on its size, so its 30 rows become 3 spilled
        // runs; `small` alone would fit, but not beside them.
        val result =
            admit(
                listOf(fake("large", 1L shl 40, 30), fake("small", 500, 5)),
                spill(tmp, chunkRows = 10, mergeBudget = 500 + 2_000, block = 1_000),
            )
        assertThat(result.admitted).isEmpty()
        assertThat(result.projectedBytes).describedAs("4 runs").isEqualTo(4_000)
    }

    @Test
    fun `a later refusal sheds the smallest admitted run until the group fits`() {
        // `big` fits alone; `little` is refused, and its rows are one
        // spilled run that `big` was never costed beside. Shedding `big`
        // leaves one chunk and no run at all, which fits anything.
        val big = fake("big", 1_000, 3)
        val little = fake("little", 600, 2)
        val result = admit(listOf(big, little), spill(tmp, mergeBudget = 1_001, block = 1_000_000))
        assertThat(result.admitted).isEmpty()
        assertThat(result.demoted).containsExactlyInAnyOrder(big, little)
        assertThat(result.projectedBytes).isZero()
    }

    @Test
    fun `shedding takes the smallest admitted run first`() {
        // l1 and l2 fit; s does not, and its one spilled run then breaks
        // the pair; dropping l2 (whose rows join s's run) fits.
        val l1 = fake("l1", 3_000, 300)
        val l2 = fake("l2", 2_000, 100)
        val s = fake("s", 1_500, 20)
        val result = admit(listOf(s, l2, l1), spill(tmp, mergeBudget = 3_000 + 2_000 + 1, block = 1_000))
        assertThat(result.admitted).containsExactly(l1)
        assertThat(result.demoted).containsExactlyInAnyOrder(s, l2)
    }

    @Test
    fun `registered spill bytes over the spill budget are refused, exactly at it admitted`() {
        val bounds = spill(tmp, chunkRows = 1)
        val untrusted = listOf(fake("a", 1_000, 2, trusted = false))
        assertThatThrownBy { admit(untrusted, bounds.copy(spillBudgetBytes = 999)) }
            .isInstanceOf(SpillBudgetExceededException::class.java)
        admit(untrusted, bounds.copy(spillBudgetBytes = 1_000))
    }

    @Test
    fun `a group that will never spill is not refused for its spill bytes`() {
        // One chunk, no trusted run: sorted in memory, no disk.
        admit(listOf(fake("a", 1_000, 2, trusted = false)), spill(tmp, spillBudget = 1))
    }

    @Test
    fun `demoted inputs count toward the spill pre-refusal`() {
        // No room for the run as a run (its estimate is 1,000 B); its 20
        // rows are 4 spilled runs, and its bytes do not fit the spill.
        assertThatThrownBy {
            admit(listOf(fake("t", 1_000, 20)), spill(tmp, chunkRows = 5, mergeBudget = 4, spillBudget = 999))
        }.isInstanceOf(SpillBudgetExceededException::class.java)
    }

    // ---- admission and footers, end to end ------------------------------------------

    /** A trusted kv file of [rows] keys 0 until rows, ids from [firstId]. */
    private fun trustedOf(
        name: String,
        rows: Int,
        firstId: Long,
    ): Path = outputKv(name, (0 until rows).map { it.toLong() to firstId + it })

    /** kv's leaves: k, v, _hog_row_id. */
    private val kvLeaves = 3

    private fun estimate(
        input: ParquetRewriter.Input,
        bounds: SortSpill,
    ) = ExternalMergeSort.estimatedTrustedRunBytes(input, kvLeaves, bounds)

    /**
     * A trusted kv file whose row group (~8 MB of incompressible text in
     * one row group) is LARGER than its estimate when its size is
     * registered as 1 B, which the per-leaf page term otherwise dwarfs.
     */
    private val fatTrusted: Path by lazy {
        val rng = java.util.Random(7)
        val alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
        write(
            "fat-trusted",
            kvOutputSchema,
            (0 until 100).map { i ->
                { g: Group ->
                    g.add(0, i.toLong())
                    g.add(1, String(CharArray(80_000) { alphabet[rng.nextInt(64)] }))
                    g.add(2, i.toLong())
                }
            },
        )
    }

    private fun exactTrusted(
        path: Path,
        readahead: Int,
    ): Long =
        ParquetFileReader.open(LocalInputFile(path)).use {
            ExternalMergeSort.footerRunBytes(it.footer, ExternalMergeSort.TRUSTED_PAGE_BYTES_PER_LEAF)
        } + readahead

    @Test
    fun `admitted runs are opened largest first, before the chunk phase opens the demoted`() {
        val large = CountingInputFile(trustedOf("adm-large", 300, 0))
        val medium = CountingInputFile(trustedOf("adm-medium", 100, 1_000))
        val small = CountingInputFile(trustedOf("adm-small", 5, 2_000))
        val opens = mutableListOf<String>()
        large.onOpen = { opens += "large" }
        medium.onOpen = { opens += "medium" }
        small.onOpen = { opens += "small" }
        val inputs =
            listOf(
                // Input order is NOT size order: largest-first must sort.
                input(small, "small", trusted = true, survivors = 5),
                input(medium, "medium", trusted = true, survivors = 100),
                input(large, "large", trusted = true, survivors = 300),
            )
        val bounds = spill(spillParent())
        // Room for the largest as a run beside the others' one spilled run.
        val budget = estimate(inputs[2], bounds) + ExternalMergeSort.predictedSpilledRunBytes(kvLeaves, bounds)
        val out = fresh("adm-out.parquet")
        val result = rewrite(inputs, out, bounds.copy(mergeBudgetBytes = budget))
        assertThat(result.runsTrusted).isEqualTo(1)
        assertThat(result.runsDemoted).isEqualTo(2)
        assertThat(result.runsSpilled).isEqualTo(1)
        // The admitted footer first; then the chunk phase, in input order.
        assertThat(opens).containsExactly("large", "small", "medium")
        assertThat(readKv(out).map { it.first }).isSorted().hasSize(405)
        for (f in listOf(large, medium, small)) assertThat(f.openStreams()).isZero()
    }

    @Test
    fun `a run whose exact footer cost breaks the budget is demoted before any data is read`() {
        // The registered size UNDERSTATES the file, so the estimate
        // admits it; its footer's row group does not fit.
        val file = CountingInputFile(fatTrusted)
        val client = CountingInputFile(clientKv("footer-client", listOf(5, 4)))
        var dataReadsAtDemotion = -1
        var closedAtDemotion = -1
        // The demoted run is re-opened by the chunk phase; record what had
        // been read from it, and whether its footer reader was closed.
        file.onOpen = {
            if (file.opened.get() == 2) {
                dataReadsAtDemotion = file.dataReads.get()
                closedAtDemotion = file.closed.get()
            }
        }
        val inputs =
            listOf(
                input(file, "understated", trusted = true, fileSizeBytes = 1, survivors = 100),
                input(client, "client", rowIdStart = 1_000, survivors = 2),
            )
        val bounds = spill(spillParent())
        val budget = estimate(inputs[0], bounds) + ExternalMergeSort.predictedSpilledRunBytes(kvLeaves, bounds)
        assertThat(exactTrusted(fatTrusted, 0)).isGreaterThan(budget)
        val result = rewrite(inputs, fresh("footer-out.parquet"), bounds.copy(mergeBudgetBytes = budget))
        assertThat(result.runsTrusted).isZero()
        assertThat(result.runsDemoted).isEqualTo(1)
        assertThat(file.opened.get()).describedAs("footer read, then re-opened for the chunk phase").isEqualTo(2)
        assertThat(dataReadsAtDemotion).describedAs("row-group reads before demotion").isZero()
        assertThat(closedAtDemotion).describedAs("footer reader closed at demotion").isEqualTo(1)
        assertThat(file.openStreams()).isZero()
        assertThat(result.rowsWritten).isEqualTo(102)
    }

    @Test
    fun `a run whose exact footer cost, readahead included, equals the budget is kept`() {
        val readahead = 1_000
        val exact = exactTrusted(fatTrusted, readahead)

        fun at(budget: Long) =
            rewrite(
                // Size understated, so admission passes and the footer decides.
                listOf(input(LocalInputFile(fatTrusted), "t", trusted = true, fileSizeBytes = 1, survivors = 100)),
                fresh("footer-exact-out.parquet"),
                spill(spillParent(), mergeBudget = budget).copy(readaheadBytes = readahead),
            )
        assertThat(at(exact).runsTrusted).isEqualTo(1)
        assertThat(at(exact - 1).runsDemoted).isEqualTo(1)
    }

    @Test
    fun `a footer demotion that breaks the merge budget refuses before any data is read`() {
        // Admitted on an understated size; demoted by its footer; its 100
        // rows at one per chunk are then 100 spilled runs, which do not fit.
        val file = CountingInputFile(fatTrusted)
        val client = CountingInputFile(clientKv("footer-refuse-client", listOf(1, 2)))
        val parent = spillParent()
        val inputs =
            listOf(
                input(file, "understated", trusted = true, fileSizeBytes = 1, survivors = 100),
                input(client, "client", 1_000, survivors = 2),
            )
        val bounds = spill(parent, chunkRows = 1)
        val budget = estimate(inputs[0], bounds) + 2 * ExternalMergeSort.predictedSpilledRunBytes(kvLeaves, bounds)
        assertThatThrownBy {
            rewrite(inputs, fresh("footer-refuse-out.parquet"), bounds.copy(mergeBudgetBytes = budget))
        }.isInstanceOf(MergeBudgetExceededException::class.java)
        assertThat(file.dataReads.get() + client.dataReads.get()).describedAs("row-group reads").isZero()
        assertThat(file.openStreams()).isZero()
        assertThat(parent.listDirectoryEntries()).isEmpty()
    }

    @Test
    fun `a trusted run of unknown size is costed at a full output block and demoted unopened`() {
        val file = CountingInputFile(trustedOf("unknown-size", 10, 0))
        val result =
            rewrite(
                listOf(input(file, "unknown", trusted = true, fileSizeBytes = 0, survivors = 10)),
                fresh("unknown-out.parquet"),
                spill(spillParent(), mergeBudget = 64L shl 20),
            )
        assertThat(result.runsDemoted).isEqualTo(1)
        assertThat(file.opened.get()).describedAs("opened by the chunk phase only").isEqualTo(1)
    }

    @Test
    fun `a trusted input the live schema cannot take is refused before the chunk phase spends anything`() {
        // k is a STRING in this file and a long in the live schema.
        val schema =
            Types.buildMessage()
                .addField(
                    Types.optional(PrimitiveTypeName.BINARY).`as`(LogicalTypeAnnotation.stringType()).id(1).named("k"),
                )
                .addField(
                    Types.optional(PrimitiveTypeName.BINARY).`as`(LogicalTypeAnnotation.stringType()).id(2).named("v"),
                )
                .addField(
                    Types.required(PrimitiveTypeName.INT64).id(ParquetRewriter.ROW_ID_FIELD_ID)
                        .named(ParquetRewriter.ROW_ID_COLUMN),
                )
                .named("t")
        val trusted =
            write(
                "unconvertible-trusted",
                schema,
                listOf {
                        g: Group ->
                    g.add(0, "x").also { g.add(1, "y") }.also { g.add(2, 0L) }
                },
            )
        val client = CountingInputFile(clientKv("unconvertible-client", listOf(3, 2, 1)))
        val parent = spillParent()
        assertThatThrownBy {
            rewrite(
                listOf(input(LocalInputFile(trusted), "trusted", trusted = true), input(client, "client", 100)),
                fresh("unconvertible-out.parquet"),
                spill(parent, chunkRows = 1),
            )
        }.isInstanceOf(UnconvertibleSchemaException::class.java)
        assertThat(client.dataReads.get()).describedAs("untrusted rows read").isZero()
        assertThat(parent.listDirectoryEntries()).isEmpty()
    }

    // ---- merge budget, end to end ------------------------------------------------

    private fun perSpill(bounds: SortSpill) = ExternalMergeSort.predictedSpilledRunBytes(kvLeaves, bounds)

    @Test
    fun `predicted spilled runs over the merge budget refuse the group before any IO`() {
        val a = CountingInputFile(clientKv("mb-a", listOf(3, 2, 1)))
        val b = CountingInputFile(clientKv("mb-b", listOf(5, 4)))
        val parent = spillParent()
        val out = fresh("mb-out.parquet")
        // 5 survivors at 2 per chunk = 3 runs (a partial chunk is a run).
        val bounds = spill(parent, chunkRows = 2, block = 10)
        val thrown =
            catchThrowable {
                rewrite(
                    listOf(input(a, "a", survivors = 3), input(b, "b", 10, survivors = 2)),
                    out,
                    bounds.copy(mergeBudgetBytes = 3 * perSpill(bounds) - 1),
                )
            }
        assertThat(thrown).isInstanceOf(MergeBudgetExceededException::class.java)
            .hasMessageContaining("3 spilled runs")
        assertThat(a.opened.get() + b.opened.get()).describedAs("streams opened").isZero()
        assertThat(parent.listDirectoryEntries()).isEmpty()
        assertThat(out.exists()).isFalse()
    }

    @Test
    fun `the same group at exactly the budget is admitted`() {
        val a = clientKv("mbe-a", listOf(3, 2, 1))
        val b = clientKv("mbe-b", listOf(6, 5, 4))
        val out = fresh("mbe-out.parquet")
        val bounds = spill(spillParent(), chunkRows = 2, block = 10)
        val result =
            rewrite(
                listOf(input(LocalInputFile(a), "a", survivors = 3), input(LocalInputFile(b), "b", 10, survivors = 3)),
                out,
                bounds.copy(mergeBudgetBytes = 3 * perSpill(bounds)),
            )
        assertThat(result.runsSpilled).isEqualTo(3)
        assertThat(readKv(out).map { it.first }).containsExactly(1, 2, 3, 4, 5, 6)
    }

    @Test
    fun `a spilled run the registered counts did not predict is refused at the spill that breaks the budget`() {
        // Unknown survivor counts (0) predict nothing; the run count is
        // then caught as it happens, from the spill files' own footers.
        val a = CountingInputFile(clientKv("mbr-a", listOf(3, 2, 1, 0)))
        val parent = spillParent()
        val out = fresh("mbr-out.parquet")
        assertThatThrownBy {
            rewrite(listOf(input(a, "a")), out, spill(parent, chunkRows = 1, mergeBudget = 25, block = 10))
        }.isInstanceOf(MergeBudgetExceededException::class.java)
        assertThat(parent.listDirectoryEntries()).isEmpty()
        assertThat(a.openStreams()).isZero()
        assertThat(out.exists()).isFalse()
    }

    @Test
    fun `the mid-run merge check charges each spill file's exact footer, not the block`() {
        // Four tiny spill files against a budget just under four blocks:
        // the files' real row groups are a few hundred bytes each.
        val result =
            rewrite(
                listOf(input(LocalInputFile(clientKv("exact", (1L..8L).toList())), "a")),
                fresh("exact-out.parquet"),
                spill(spillParent(), chunkRows = 2, mergeBudget = 4L * (1 shl 20) - 1, block = 1 shl 20),
            )
        assertThat(result.runsSpilled).isEqualTo(4)
    }

    @Test
    fun `a group that fits one chunk with no trusted run never spills, at exactly chunkRows`() {
        // The chunk flushes when the NEXT row arrives: exactly chunkRows
        // survivors are one chunk, written straight to the output.
        val keys = listOf(4L, 1, 3, 2)
        val out = fresh("one-chunk.parquet")
        // Predicted as one run against a budget smaller than one spilled
        // run: admitted only because a lone chunk is never a run.
        val result =
            rewrite(
                listOf(input(LocalInputFile(clientKv("one", keys)), "one", survivors = 4)),
                out,
                spill(spillParent(), chunkRows = 4, mergeBudget = 1, block = 1_000),
            )
        assertThat(result.runsSpilled).isZero()
        assertThat(result.spillBytes).isZero()
        assertThat(readKv(out).map { it.first }).containsExactly(1, 2, 3, 4)

        // One more row is two chunks and a real merge.
        val two =
            rewrite(
                listOf(input(LocalInputFile(clientKv("two", keys + 0L)), "two")),
                fresh("two-chunks.parquet"),
                spill(spillParent(), chunkRows = 4),
            )
        assertThat(two.runsSpilled).isEqualTo(2)
        assertThat(two.spillBytes).isPositive()
    }

    // ---- the spill budget -----------------------------------------------------

    @Test
    fun `registered input bytes over the spill budget refuse the group before any IO`() {
        val a = CountingInputFile(clientKv("sb-a", listOf(1, 2)))
        val parent = spillParent()
        assertThatThrownBy {
            rewrite(
                listOf(input(a, "a", survivors = 2)),
                fresh("sb-out.parquet"),
                spill(parent, chunkRows = 1, spillBudget = a.length - 1),
            )
        }.isInstanceOf(SpillBudgetExceededException::class.java)
            .satisfies({
                val e = it as SpillBudgetExceededException
                assertThat(listOf(e.spillBytes, e.runsSpilled.toLong(), e.runsDemoted.toLong()))
                    .describedAs("a refusal before any IO spent nothing")
                    .containsOnly(0L)
            })
        assertThat(a.opened.get()).isZero()
        assertThat(parent.listDirectoryEntries()).isEmpty()
    }

    @Test
    fun `the mid-run spill budget is exact`() {
        val path = clientKv("sbx", (1L..40L).toList())

        fun run(budget: Long) =
            rewrite(
                listOf(input(LocalInputFile(path), "a", fileSizeBytes = 0)),
                fresh("sbx-out.parquet"),
                spill(spillParent(), chunkRows = 10, spillBudget = budget),
            )
        val bytes = run(Long.MAX_VALUE / 4).spillBytes
        assertThat(run(bytes).spillBytes).isEqualTo(bytes)
        val stopped = catchThrowable { run(bytes - 1) }
        assertThat(stopped).isInstanceOf(SpillBudgetExceededException::class.java)
        // The stop carries the work it spent (its caller has no result):
        // three of the four chunk files complete, the fourth's bytes up to
        // the refused write metered, never past the budget.
        stopped as SpillBudgetExceededException
        assertThat(stopped.runsSpilled).isEqualTo(3)
        assertThat(stopped.spillBytes).isPositive().isLessThanOrEqualTo(bytes - 1)
        assertThat(stopped.runsDemoted).isZero()
    }

    @Test
    fun `spill files are SNAPPY, in row groups of the spill block`() {
        val parent = spillParent()
        val footers = mutableListOf<org.apache.parquet.hadoop.metadata.ParquetMetadata>()
        // A trusted run is first read by the merge, when every spill file exists.
        val probe = CountingInputFile(trustedOf("block-probe", 1, 100_000))
        probe.onDataRead = {
            if (footers.isEmpty()) {
                Files.walk(parent).filter { Files.isRegularFile(it) }.forEach { f ->
                    ParquetFileReader.open(LocalInputFile(f)).use { footers += it.footer }
                }
            }
        }
        val rows = (1L..4_000L).map { (it * 7_919) % 4_000 }
        rewrite(
            listOf(input(probe, "probe", trusted = true), input(LocalInputFile(clientKv("block", rows)), "a", 10)),
            fresh("block-out.parquet"),
            spill(parent, chunkRows = 2_000, block = 8 * 1024),
        )
        assertThat(footers).hasSize(2)
        for (footer in footers) {
            assertThat(footer.blocks.size).describedAs("row groups per 2,000-row spill file").isGreaterThan(1)
            assertThat(footer.blocks.flatMap { b -> b.columns.map { it.codec } }.toSet())
                .containsExactly(CompressionCodecName.SNAPPY)
        }
    }

    /** Footers of every spill file under [parent], read while they exist. */
    private fun spillFooters(parent: Path): List<org.apache.parquet.hadoop.metadata.ParquetMetadata> =
        Files.walk(parent).use { paths ->
            paths.filter { Files.isRegularFile(it) }.toList().map { f ->
                ParquetFileReader.open(LocalInputFile(f)).use { it.footer }
            }
        }

    @Test
    fun `fat rows do not overshoot the spill block`() {
        // parquet-java checks a row group's size every 100 rows at first:
        // at 200 KB a row that alone is a 20 MB row group against a 4 MiB
        // block, and reading it back would hold all of it.
        val block = 4 shl 20
        val rng = java.util.Random(11)
        val alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
        val fat =
            write(
                "fat-rows",
                kvSchema,
                (0 until 120).map { i ->
                    { g: Group ->
                        g.add(0, (i * 7_919L) % 120)
                        g.add(1, String(CharArray(200_000) { alphabet[rng.nextInt(64)] }))
                    }
                },
            )
        val parent = spillParent()
        val footers = mutableListOf<org.apache.parquet.hadoop.metadata.ParquetMetadata>()
        val probe = CountingInputFile(trustedOf("fat-probe", 1, 10_000))
        probe.onDataRead = { if (footers.isEmpty()) footers += spillFooters(parent) }
        val result =
            rewrite(
                listOf(input(probe, "probe", trusted = true), input(LocalInputFile(fat), "fat", 100)),
                fresh("fat-out.parquet"),
                spill(parent, chunkRows = 60, block = block),
            )
        assertThat(result.runsSpilled).isEqualTo(2)
        assertThat(footers).hasSize(2)
        for (footer in footers) {
            assertThat(footer.blocks.size).isGreaterThan(1)
            // One row past the block is the most a per-row check allows.
            assertThat(footer.blocks.maxOf { it.compressedSize }).isLessThanOrEqualTo(block + 400_000L)
        }
    }

    @Test
    fun `rows that turn fat after small ones do not overshoot the spill block by more than a few rows`() {
        // The size check is scheduled from the AVERAGE row: after 300
        // tiny rows the average says the block is far off, and parquet-
        // java would wait up to 10,000 rows before looking again.
        val block = 1 shl 20
        val fatBytes = 50_000
        val rng = java.util.Random(13)
        val alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
        val mixed =
            write(
                "mixed-rows",
                kvSchema,
                (0 until 500).map { i ->
                    { g: Group ->
                        g.add(0, i.toLong())
                        g.add(1, if (i < 300) "s" else String(CharArray(fatBytes) { alphabet[rng.nextInt(64)] }))
                    }
                },
            )
        val parent = spillParent()
        val footers = mutableListOf<org.apache.parquet.hadoop.metadata.ParquetMetadata>()
        val probe = CountingInputFile(trustedOf("mixed-probe", 1, 10_000))
        probe.onDataRead = { if (footers.isEmpty()) footers += spillFooters(parent) }
        rewrite(
            listOf(input(probe, "probe", trusted = true), input(LocalInputFile(mixed), "mixed", 100)),
            fresh("mixed-out.parquet"),
            spill(parent, block = block),
        )
        val largest = footers.single().blocks.maxOf { it.compressedSize }
        assertThat(largest)
            .isLessThanOrEqualTo(block + ExternalMergeSort.SPILL_MAX_ROWS_PER_SIZE_CHECK.toLong() * (fatBytes + 1_000))
    }

    @Test
    fun `the mid-run merge check is exact at its boundary`() {
        // The exact cost of four spill files, measured from their own
        // footers as the merge's output is created (every spill exists,
        // none is deleted yet); then the same group at, and one byte
        // under, that cost.
        val path = clientKv("boundary", (1L..8L).toList())

        fun run(budget: Long): Long {
            val parent = spillParent()
            var measured = 0L
            val sink = RecordingOutput(LocalDiscardableOutput(fresh("boundary-out.parquet")))
            sink.onCreate = {
                measured =
                    spillFooters(parent).sumOf {
                        ExternalMergeSort.footerRunBytes(it, ExternalMergeSort.SPILL_PAGE_BYTES_PER_LEAF)
                    }
            }
            ParquetRewriter.rewrite(
                listOf(input(LocalInputFile(path), "a")),
                kv,
                byK,
                sink,
                spill = spill(parent, chunkRows = 2, mergeBudget = budget),
            )
            return measured
        }
        val exact = run(Long.MAX_VALUE / 4)
        assertThat(run(exact)).isEqualTo(exact)
        assertThatThrownBy { run(exact - 1) }.isInstanceOf(MergeBudgetExceededException::class.java)
    }

    @Test
    fun `a spill directory that cannot be removed is reported, not swallowed`() {
        // Real permissions rather than a hook: once the merge starts, the
        // spill directory is made read-only, so its files cannot be
        // unlinked and `deleteRecursively` returns false.
        val parent = spillParent()
        val probe = CountingInputFile(trustedOf("cleanup-probe", 1, 10_000))
        var locked: Path? = null
        probe.onDataRead = {
            if (locked == null) {
                locked = parent.listDirectoryEntries().single()
                Files.setPosixFilePermissions(locked!!, PosixFilePermissions.fromString("r-xr-xr-x"))
            }
        }
        try {
            val result =
                rewrite(
                    listOf(
                        input(probe, "probe", trusted = true),
                        input(LocalInputFile(clientKv("cleanup", listOf(3, 2, 1))), "a", 10),
                    ),
                    fresh("cleanup-out.parquet"),
                    spill(parent, chunkRows = 1),
                )
            assertThat(result.spillCleanupFailed).isTrue()
            assertThat(result.rowsWritten).isEqualTo(4)
            assertThat(locked).isNotNull().exists()
        } finally {
            locked?.let {
                Files.setPosixFilePermissions(
                    it,
                    PosixFilePermissions.fromString("rwxr-xr-x"),
                )
            }
        }
    }

    @Test
    fun `spill bytes crossing the budget mid-run stop the group, discard the output and leave no spill dir`() {
        // Size unknown (0) gets past the pre-refusal; the meter does not.
        val a = CountingInputFile(clientKv("sbm-a", (1L..50L).toList()))
        val parent = spillParent()
        val out = fresh("sbm-out.parquet")
        assertThatThrownBy {
            rewrite(
                listOf(input(a, "a", fileSizeBytes = 0)),
                out,
                spill(parent, chunkRows = 10, spillBudget = 600),
            )
        }.isInstanceOf(SpillBudgetExceededException::class.java)
            .hasMessageContaining("mid-run")
        assertThat(out.exists()).describedAs("output discarded").isFalse()
        assertThat(parent.listDirectoryEntries()).isEmpty()
        assertThat(a.openStreams()).isZero()
    }

    @Test
    fun `the spill meter counts every byte written`() {
        val a = clientKv("sbc-a", (1L..50L).toList())
        val parent = spillParent()
        var onDisk = -1L
        // A trusted run is first read by the MERGE, after every chunk has
        // been spilled and before the directory is removed: measure there.
        val probe = CountingInputFile(trustedOf("sbc-probe", 1, 1_000))
        probe.onDataRead = {
            if (onDisk < 0) {
                onDisk = Files.walk(parent).filter { Files.isRegularFile(it) }.mapToLong { Files.size(it) }.sum()
            }
        }
        val result =
            rewrite(
                listOf(input(probe, "probe", trusted = true), input(LocalInputFile(a), "a", 100)),
                fresh("sbc-out.parquet"),
                spill(parent, chunkRows = 10),
            )
        assertThat(result.runsSpilled).isEqualTo(5)
        assertThat(onDisk).isPositive()
        assertThat(result.spillBytes).isEqualTo(onDisk)
    }

    // ---- spill dir and readers on every exit path ------------------------------

    /**
     * A group that spills (chunkRows 1 over two client files) AND merges
     * a trusted run whose data reads can be made to fail: the trusted run
     * is only read in the merge, so a failure there is mid-merge, with
     * spill files and every kind of reader open.
     */
    private fun lifecycle(
        failTrusted: (() -> Throwable)? = null,
        bMisregistered: Boolean = false,
    ): Pair<Throwable?, List<CountingInputFile>> {
        val parent = spillParent()
        val trusted = CountingInputFile(trustedOf("life-trusted", 20, 0))
        trusted.failDataRead = failTrusted
        val a = CountingInputFile(clientKv("life-a", listOf(5, 3, 9)))
        val b = CountingInputFile(clientKv("life-b", listOf(4, 8)))
        val seen = mutableListOf<String>()
        // b is opened after a's chunks were spilled: the dir exists by then.
        b.onOpen = { if (seen.isEmpty()) parent.listDirectoryEntries().mapTo(seen) { it.name } }
        val out = fresh("life-out.parquet")
        val thrown =
            catchThrowable {
                rewrite(
                    listOf(
                        input(trusted, "trusted", trusted = true),
                        input(a, "a", 100),
                        // Registered explicit with no carrier column: the
                        // refusal fires as b is opened, with spills on disk.
                        input(b, "b", 200).copy(explicitRowIds = bMisregistered),
                    ),
                    out,
                    spill(parent, chunkRows = 1),
                )
            }
        assertThat(seen).describedAs("one spill dir while the group ran").singleElement()
            .satisfies({ assertThat(it).startsWith(SpillDirectory.PREFIX) })
        if (thrown == null) {
            assertThat(readKv(out)).hasSize(25)
        } else {
            assertThat(out.exists()).describedAs("output discarded").isFalse()
        }
        assertThat(parent.listDirectoryEntries()).describedAs("spill dir removed").isEmpty()
        for (f in listOf(trusted, a, b)) {
            assertThat(f.opened.get()).describedAs("%s opened", f).isPositive()
            assertThat(f.openStreams()).describedAs("%s streams left open", f).isZero()
        }
        return thrown to listOf(trusted, a, b)
    }

    @Test
    fun `success creates one spill dir, removes it, and closes every reader`() {
        val (thrown, _) = lifecycle()
        assertThat(thrown).isNull()
    }

    @Test
    fun `a failure after rows were written discards the output`() {
        // The trusted run has two row groups; its keys sort before every
        // other run's, so the merge has handed all 100 rows of the first
        // group to the output writer when reading the second one fails —
        // the second read is the hasNext() after the 100th pop.
        val path = fresh("partial-trusted.parquet")
        ExampleParquetWriter.builder(LocalOutputFile(path))
            .withType(kvOutputSchema)
            .withRowGroupRowCountLimit(100)
            .build()
            .use { w ->
                val f = SimpleGroupFactory(kvOutputSchema)
                for (i in 0 until 200) w.write(
                    f.newGroup().apply {
                        add(0, i.toLong())
                        add(1, "v")
                        add(2, i.toLong())
                    },
                )
            }
        val secondRowGroup =
            ParquetFileReader.open(LocalInputFile(path)).use { r ->
                assertThat(r.footer.blocks).hasSize(2)
                r.footer.blocks[1].startingPos
            }
        val trusted = CountingInputFile(path)
        trusted.failFrom = secondRowGroup
        trusted.failDataRead = { IOException("injected after the first row group") }
        val out = fresh("partial-out.parquet")
        val sink = RecordingOutput(LocalDiscardableOutput(out))
        val thrown =
            catchThrowable {
                ParquetRewriter.rewrite(
                    listOf(
                        input(trusted, "trusted", trusted = true),
                        input(LocalInputFile(clientKv("partial-client", listOf(1_002, 1_001))), "client", 1_000),
                    ),
                    kv,
                    byK,
                    sink,
                    spill = spill(spillParent(), chunkRows = 1),
                )
            }
        assertThat(generateSequence(thrown) { it.cause }.map { it.message }.toList())
            .contains("injected after the first row group")
        assertThat(trusted.dataReads.get()).describedAs("both row groups were reached").isEqualTo(2)
        assertThat(sink.discards.get()).isPositive()
        assertThat(out.exists()).isFalse()
        assertThat(trusted.openStreams()).isZero()
    }

    @Test
    fun `an IOException mid-merge removes the spill dir and closes every reader`() {
        val (thrown, files) = lifecycle(failTrusted = { IOException("injected mid-merge") })
        assertThat(generateSequence(thrown) { it.cause }.map { it.message }.toList()).contains("injected mid-merge")
        assertThat(files[0].dataReads.get()).isPositive()
    }

    @Test
    fun `an OutOfMemoryError mid-merge removes the spill dir and closes every reader`() {
        val (thrown, _) = lifecycle(failTrusted = { OutOfMemoryError("injected") })
        assertThat(thrown).isInstanceOf(OutOfMemoryError::class.java)
    }

    @Test
    fun `an InvalidDataException in the chunk phase removes the spill dir and closes every reader`() {
        val (thrown, _) = lifecycle(bMisregistered = true)
        assertThat(thrown).isInstanceOf(InvalidDataException::class.java)
    }

    @Test
    fun `a sort without bounds is refused, not run in memory`() {
        assertThatThrownBy {
            ParquetRewriter.rewrite(
                listOf(input(LocalInputFile(clientKv("nobounds", listOf(1))), "x")),
                kv,
                byK,
                LocalDiscardableOutput(fresh("nobounds-out.parquet")),
            )
        }.isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("SortSpill")
    }

    private fun openFds(): Long =
        (ManagementFactory.getOperatingSystemMXBean() as UnixOperatingSystemMXBean).openFileDescriptorCount

    @Test
    fun `spill files are closed on success and on a mid-run spill refusal`() {
        // Spill files are local and unlinked with their directory, which
        // on POSIX leaves an unclosed handle invisible except as a
        // descriptor the process never gets back.
        val path = clientKv("fd", (1L..40L).toList())
        val before = openFds()
        repeat(3) {
            rewrite(
                listOf(input(LocalInputFile(path), "a")),
                fresh("fd-out.parquet"),
                spill(spillParent(), chunkRows = 2),
            )
        }
        assertThat(openFds()).describedAs("descriptors after 60 spilled runs").isLessThanOrEqualTo(before + 2)
        repeat(3) {
            catchThrowable {
                rewrite(
                    listOf(input(LocalInputFile(path), "a", fileSizeBytes = 0)),
                    fresh("fd-refused.parquet"),
                    spill(spillParent(), chunkRows = 2, spillBudget = 2_000),
                )
            }.let { assertThat(it).isInstanceOf(SpillBudgetExceededException::class.java) }
        }
        assertThat(openFds()).describedAs("descriptors after three refused spills").isLessThanOrEqualTo(before + 2)
    }

    // ---- DVs on trusted runs ------------------------------------------------------

    @Test
    fun `a DV on a trusted run skips exactly its positions`() {
        val trusted = trustedOf("dv-trusted", 6, 0) // physical positions 0..5 = ids 0..5
        val out = fresh("dv-out.parquet")
        val result =
            rewrite(
                listOf(
                    input(LocalInputFile(trusted), "trusted", deletes = dv(listOf(0, 2, 5)), trusted = true),
                    input(LocalInputFile(clientKv("dv-client", listOf(1))), "client", 100),
                ),
                out,
                spill(spillParent(), chunkRows = 1),
            )
        assertThat(result.runsTrusted).isEqualTo(1)
        assertThat(readKv(out).map { it.second }).containsExactly(1, 100, 3, 4)
    }

    @Test
    fun `a lying DV on a trusted run fires at the run's exhaustion and discards the output`() {
        val trusted = CountingInputFile(trustedOf("dvl-trusted", 4, 0))
        val parent = spillParent()
        val out = fresh("dvl-out.parquet")
        assertThatThrownBy {
            rewrite(
                listOf(
                    input(trusted, "trusted", deletes = dv(listOf(1, 17)), trusted = true),
                    input(LocalInputFile(clientKv("dvl-client", listOf(9, 8))), "client", 100),
                ),
                out,
                spill(parent, chunkRows = 1),
            )
        }.isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("refusing a lossy compaction")
        assertThat(trusted.dataReads.get()).describedAs("the run was read before the check fired").isPositive()
        assertThat(out.exists()).isFalse()
        assertThat(parent.listDirectoryEntries()).isEmpty()
        assertThat(trusted.openStreams()).isZero()
    }

    // ---- spill read-back of every scalar type -----------------------------------

    private val scalarTypes =
        listOf(
            ColType.BOOLEAN, ColType.INT8, ColType.INT16, ColType.INT, ColType.LONG, ColType.UINT8,
            ColType.UINT16, ColType.UINT32, ColType.UINT64, ColType.FLOAT, ColType.DOUBLE, ColType.DECIMAL,
            ColType.DATE, ColType.TIME, ColType.TIMESTAMP_S, ColType.TIMESTAMP_MS, ColType.TIMESTAMP,
            ColType.TIMESTAMP_NS, ColType.TIMESTAMPTZ, ColType.STRING, ColType.JSON, ColType.UUID_T, ColType.BINARY,
        )

    @Test
    fun `every scalar type reads back from a spill file exactly as it was written`() {
        // The test pins the type list against the enum, so a new scalar
        // type cannot skip this.
        assertThat(scalarTypes.toSet())
            .isEqualTo(ColType.entries.filter { !it.isNested && it != ColType.VARIANT }.toSet())
        val live =
            listOf(Column(1, 0, ColumnDef("k", ColType.LONG))) +
                scalarTypes.mapIndexed { i, type ->
                    val params = if (type == ColType.DECIMAL) mapOf("precision" to 18, "scale" to 3) else null
                    Column(10L + i, i + 1, ColumnDef("c_${type.wire}", type, params))
                }
        // The rewriter's own output schema, minus the carrier, IS a valid
        // input shape for every type: borrow it from an empty rewrite.
        val probe = fresh("scalar-probe.parquet")
        rewriteToLocal(
            listOf(localInput(write("scalar-k", kvSchema, emptyList()), 0)),
            live,
            emptyList(),
            probe,
        )
        val outSchema = readGroups(probe).first
        val inSchema = MessageType("t", outSchema.fields.dropLast(1))
        val rows = 12
        val input =
            write(
                "scalar-in",
                inSchema,
                (0 until rows).map { i ->
                    { g: Group ->
                        g.add(0, (rows - i).toLong())
                        // Every third row null in every column, so a
                        // null that comes back present (or vice versa)
                        // shows.
                        if (i % 3 != 2) {
                            for (f in 1 until inSchema.fieldCount) addValue(
                                g,
                                f,
                                inSchema.getType(f).asPrimitiveType(),
                                i,
                            )
                        }
                    }
                },
            )
        val reference = fresh("scalar-ref.parquet")
        rewriteToLocal(listOf(localInput(input, 0)), live, emptyList(), reference)
        val spilled = fresh("scalar-spilled.parquet")
        val result =
            rewrite(
                listOf(input(LocalInputFile(input), "scalar")),
                spilled,
                spill(spillParent(), chunkRows = 1),
                live = live,
            )
        assertThat(result.runsSpilled).isEqualTo(rows)
        val (refSchema, refRows) = readGroups(reference)
        val (outSchema2, outRows) = readGroups(spilled)
        assertThat(outSchema2).isEqualTo(refSchema)

        fun canonical(g: Group): List<String?> =
            (0 until refSchema.fieldCount).map { f ->
                if (g.getFieldRepetitionCount(f) == 0) null else g.getValueToString(f, 0)
            }
        val rowId = refSchema.getFieldIndex(ParquetRewriter.ROW_ID_COLUMN)
        assertThat(outRows.associate { it.getLong(rowId, 0) to canonical(it) })
            .isEqualTo(refRows.associate { it.getLong(rowId, 0) to canonical(it) })
        assertThat(outRows.map { it.getLong(0, 0) }).isSorted()
    }

    private fun addValue(
        g: Group,
        field: Int,
        type: PrimitiveType,
        i: Int,
    ) {
        val logical = type.logicalTypeAnnotation
        when (type.primitiveTypeName) {
            PrimitiveTypeName.BOOLEAN -> g.add(field, i % 2 == 0)
            PrimitiveTypeName.INT32 ->
                g.add(
                    field,
                    when (val int = logical as? LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
                        null -> 19_000 + i * 7 - 40
                        else ->
                            if (int.isSigned) {
                                (i * 37 % (1 shl (int.bitWidth - 1))) - (1 shl (int.bitWidth - 2))
                            } else {
                                i * 41 % (1 shl int.bitWidth)
                            }
                    },
                )
            PrimitiveTypeName.INT64 ->
                g.add(
                    field,
                    when {
                        logical is LogicalTypeAnnotation.IntLogicalTypeAnnotation && !logical.isSigned -> -1L - i
                        logical is LogicalTypeAnnotation.TimeLogicalTypeAnnotation -> i * 1_000_000L
                        logical is LogicalTypeAnnotation.TimestampLogicalTypeAnnotation -> 1_700_000_000_000L + i
                        else -> 4_000_000_000L + i * 1_000_003L
                    },
                )
            PrimitiveTypeName.FLOAT -> g.add(field, i * 0.5f - 2f)
            PrimitiveTypeName.DOUBLE -> g.add(field, if (i == 4) Double.NaN else i * -1.25)
            PrimitiveTypeName.BINARY ->
                g.add(
                    field,
                    when (logical) {
                        is LogicalTypeAnnotation.DecimalLogicalTypeAnnotation ->
                            Binary.fromConstantByteArray(BigInteger.valueOf(i * 123_457L - 500_000L).toByteArray())
                        is LogicalTypeAnnotation.StringLogicalTypeAnnotation -> Binary.fromString("s$i-é")
                        is LogicalTypeAnnotation.JsonLogicalTypeAnnotation -> Binary.fromString("{\"i\":$i}")
                        else -> Binary.fromConstantByteArray(byteArrayOf(i.toByte(), 0, -1))
                    },
                )
            PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY ->
                g.add(field, Binary.fromConstantByteArray(ByteArray(type.typeLength) { (it * i).toByte() }))
            else -> error("unexpected ${type.primitiveTypeName}")
        }
    }

    /** [LocalDiscardableOutput], counting the discards the rewriter asks for. */
    private class RecordingOutput(
        private val delegate: LocalDiscardableOutput,
    ) : org.apache.parquet.io.OutputFile by delegate, DiscardableOutputFile {
        val discards = AtomicInteger()
        var onCreate: () -> Unit = {}

        override fun createOrOverwrite(blockSizeHint: Long): org.apache.parquet.io.PositionOutputStream {
            onCreate()
            return delegate.createOrOverwrite(blockSizeHint)
        }

        override fun discard() {
            discards.incrementAndGet()
            delegate.discard()
        }
    }

    // ---- the sortedness pre-pass -------------------------------------------------

    /** Run [SortednessCheck] on [file] as the rewrite would, for [input]'s registration. */
    private fun checks(
        file: InputFile,
        sort: List<SortFieldDef>,
        explicitRowIds: Boolean = false,
    ): Boolean =
        ParquetFileReader.open(file).use { reader ->
            SortednessCheck.isSorted(
                ParquetRewriter.Input(file, file.toString(), 0, explicitRowIds = explicitRowIds),
                reader,
                sort,
            )
        }

    private val propCarrierSchema: MessageType =
        MessageType(
            "t",
            propSchema.fields +
                Types.required(PrimitiveTypeName.INT64).id(ParquetRewriter.ROW_ID_FIELD_ID)
                    .named(ParquetRewriter.ROW_ID_COLUMN),
        )

    private class CheckCase(
        val rows: List<Rec>,
        val keys: List<SortFieldDef>,
        val explicit: Boolean,
        val ids: List<Long>,
        val sorted: Boolean,
        val swap: Pair<Int, Int>,
    )

    private val arbCheckCase: Arb<CheckCase> =
        arbitrary {
            val rows = Arb.list(arbRec, 0..40).bind()
            val n = rows.size
            CheckCase(
                rows = rows,
                keys = Arb.list(arbKey, 1..3).bind().distinctBy { it.sourceFieldId },
                explicit = Arb.boolean().bind(),
                // Few distinct ids, so explicit ties land on equal AND
                // unequal ids.
                ids = rows.map { Arb.int(0..5).bind().toLong() },
                sorted = Arb.boolean().bind(),
                swap = if (n < 2) 0 to 0 else Arb.int(0 until n).bind() to Arb.int(0 until n).bind(),
            )
        }

    @Test
    fun `the pre-pass agrees with an independent check on sorted and perturbed files`() {
        // Each file is written in (key, id) order and then, half the
        // time, has two rows swapped. Whether the result is still sorted
        // is decided by the reference order below, which shares no code
        // with the rewriter's; the pre-pass must say the same.
        var verdicts = 0 to 0
        runBlocking {
            checkAll(200, arbCheckCase) { case ->
                val ref = reference(case.keys)

                // Positional ids are the position; explicit ones are drawn.
                fun idsOf(order: List<Int>) =
                    if (case.explicit) order.map { case.ids[it] } else order.indices.map { it.toLong() }
                var order =
                    case.rows.indices.sortedWith {
                            a,
                            b,
                        ->
                        ref.compare(case.ids[a] to case.rows[a], case.ids[b] to case.rows[b])
                    }
                if (!case.explicit) {
                    // A client file's ids are its positions: sorting by key
                    // alone, stably, is what "sorted" means for it.
                    order = case.rows.indices.sortedWith { a, b -> ref.compare(0L to case.rows[a], 0L to case.rows[b]) }
                }
                if (!case.sorted && order.size >= 2) {
                    val m = order.toMutableList()
                    val (i, j) = case.swap
                    m[i] = order[j].also { m[j] = order[i] }
                    order = m
                }
                val ids = idsOf(order)
                val written = order.map { case.rows[it] }
                val pairs = ids.zip(written)
                val expected = (1 until pairs.size).all { ref.compare(pairs[it - 1], pairs[it]) <= 0 }
                val schema = if (case.explicit) propCarrierSchema else propSchema
                val path =
                    write(
                        "check",
                        schema,
                        written.mapIndexed { i, r ->
                            { g: Group ->
                                fill(g, r)
                                if (case.explicit) g.add(6, ids[i])
                            }
                        },
                    )
                val actual = checks(LocalInputFile(path), case.keys, case.explicit)
                assertThat(
                    actual,
                ).describedAs("keys=%s explicit=%s ids=%s rows=%s", case.keys, case.explicit, ids, written)
                    .isEqualTo(expected)
                verdicts = if (actual) verdicts.first + 1 to verdicts.second else verdicts.first to verdicts.second + 1
            }
        }
        // Both verdicts were exercised, or the agreement proves little.
        assertThat(verdicts.first).isGreaterThan(20)
        assertThat(verdicts.second).isGreaterThan(20)
    }

    @Test
    fun `direction and null order are the spec's`() {
        val desc = listOf(SortFieldDef(1, SortDirection.DESC, NullOrder.NULLS_FIRST))
        val path = write("dir", kvSchema, listOf<(Group) -> Unit>({ }, { g -> g.add(0, 3L) }, { g -> g.add(0, 1L) }))
        assertThat(checks(LocalInputFile(path), desc)).isTrue()
        assertThat(
            checks(LocalInputFile(path), listOf(SortFieldDef(1, SortDirection.ASC, NullOrder.NULLS_FIRST))),
        ).isFalse()
        assertThat(
            checks(LocalInputFile(path), listOf(SortFieldDef(1, SortDirection.DESC, NullOrder.NULLS_LAST))),
        ).isFalse()
    }

    @Test
    fun `an explicit-row-id file with sorted keys but ties out of row-id order fails`() {
        val sorted = outputKv("ties-ok", listOf(1L to 5L, 1L to 10L, 2L to 1L))
        val ties = outputKv("ties-bad", listOf(1L to 10L, 1L to 5L, 2L to 1L))
        assertThat(checks(LocalInputFile(sorted), byK, explicitRowIds = true)).isTrue()
        assertThat(checks(LocalInputFile(ties), byK, explicitRowIds = true)).isFalse()
    }

    @Test
    fun `a file without the sort column verifies, an id-less file does not`() {
        val other =
            Types.buildMessage().addField(Types.optional(PrimitiveTypeName.INT64).id(7).named("x")).named("t")
        val missing = write("missing", other, listOf<(Group) -> Unit>({ g -> g.add(0, 2L) }, { g -> g.add(0, 1L) }))
        assertThat(checks(LocalInputFile(missing), byK)).describedAs("null in every row").isTrue()
        val idless =
            Types.buildMessage().addField(Types.optional(PrimitiveTypeName.INT64).named("k")).named("t")
        val noIds = write("idless", idless, listOf<(Group) -> Unit>({ g -> g.add(0, 1L) }, { g -> g.add(0, 2L) }))
        assertThat(checks(LocalInputFile(noIds), byK)).describedAs("binds by name; not checked").isFalse()
    }

    @Test
    fun `a sort field id under a list is not a key this file can be checked on`() {
        val listed =
            Types.buildMessage()
                .addField(
                    Types.optionalList()
                        .setElementType(Types.optional(PrimitiveTypeName.INT64).id(1).named("element"))
                        .id(9)
                        .named("l"),
                ).named("t")
        val path = write("listed", listed, listOf<(Group) -> Unit>({ g -> g.addGroup(0).addGroup(0).add(0, 1L) }))
        assertThat(checks(LocalInputFile(path), byK)).isFalse()
    }

    @Test
    fun `an int32 file under a long column verifies and rewrites as a run`() {
        val int32 = Types.buildMessage().addField(Types.optional(PrimitiveTypeName.INT32).id(1).named("k")).named("t")
        val path = write("int32", int32, (0 until 5).map { i -> { g: Group -> g.add(0, i * 3 - 4) } })
        val result =
            rewrite(
                listOf(
                    input(LocalInputFile(path), "int32"),
                    input(LocalInputFile(clientKv("int32-b", listOf(9, 8))), "b", 100),
                ),
                fresh("int32-out.parquet"),
                spill(spillParent()).copy(verifyMinBytes = 0),
            )
        assertThat(result.filesVerified).isEqualTo(1)
        assertThat(result.filesUnsorted).isEqualTo(1)
        assertThat(result.runsTrusted).isEqualTo(1)
    }

    @Test
    fun `the first violation stops the read - no later row group is read`() {
        val path = fresh("early.parquet")
        ExampleParquetWriter.builder(LocalOutputFile(path))
            .withType(kvSchema)
            .withRowGroupRowCountLimit(10)
            .build()
            .use { w ->
                val f = SimpleGroupFactory(kvSchema)
                // Out of order in the first row group; sorted after it.
                for (i in 0 until 50) w.write(
                    f.newGroup().apply {
                        add(0, if (i == 1) -1L else i.toLong())
                        add(1, "v")
                    },
                )
            }
        val second = ParquetFileReader.open(LocalInputFile(path)).use { it.footer.blocks[1].startingPos }
        val file = CountingInputFile(path)
        file.failFrom = second
        file.failDataRead = { AssertionError("read a row group after the violation") }
        assertThat(checks(file, byK)).isFalse()
        assertThat(file.dataReads.get()).isEqualTo(1)
    }

    @Test
    fun `the pre-pass reads only the key columns of a wide file`() {
        val wide =
            Types.buildMessage()
                .addField(Types.optional(PrimitiveTypeName.INT64).id(1).named("k"))
                .apply {
                    for (c in 0 until 20) {
                        addField(
                            Types.optional(
                                PrimitiveTypeName.BINARY,
                            ).`as`(LogicalTypeAnnotation.stringType()).id(100 + c).named("c$c"),
                        )
                    }
                }
                .named("t")
        val rng = java.util.Random(3)
        val path =
            write(
                "wide",
                wide,
                (0 until 2_000).map { i ->
                    { g: Group ->
                        g.add(0, i.toLong())
                        for (c in 1..20) g.add(c, java.lang.Long.toHexString(rng.nextLong()).repeat(4))
                    }
                },
            )
        val counting = ByteCountingInputFile(LocalInputFile(path))
        assertThat(checks(counting, byK)).isTrue()
        val nonKey =
            ParquetFileReader.open(LocalInputFile(path)).use { r ->
                r.footer.blocks.sumOf { b -> b.columns.filter { it.path.toDotString() != "k" }.sumOf { it.totalSize } }
            }
        assertThat(counting.bytesRead).describedAs("bytes read").isLessThan(nonKey / 10)
    }

    // ---- the pre-pass inside the rewrite -----------------------------------------

    @Test
    fun `a verified file is a run read in place and never spilled`() {
        val aPath = clientKv("verified-a", listOf(1, 3, 5))
        val a = CountingInputFile(aPath)
        // Its own handle for the pre-pass, as CompactionService gives it
        // (an S3InputFile with a 256 KiB readahead): the merge must read
        // the data through `source`, never through this one.
        val aKeys = CountingInputFile(aPath)
        val b = CountingInputFile(clientKv("verified-b", listOf(2, 4, 6)))
        val parent = spillParent()
        val out = fresh("verified-out.parquet")
        val result =
            rewrite(
                listOf(input(a, "a", survivors = 3).copy(keySource = aKeys), input(b, "b", 100, survivors = 3)),
                out,
                spill(parent, chunkRows = 1).copy(verifyMinBytes = 0),
            )
        assertThat(result.filesVerified).isEqualTo(2)
        assertThat(result.filesUnsorted).isZero()
        assertThat(result.runsTrusted).describedAs("verified => trusted candidate").isEqualTo(2)
        assertThat(result.runsSpilled).isZero()
        assertThat(result.spillBytes).isZero()
        assertThat(result.sortCheckBytes).isPositive()
        // The pre-pass opens the key source once; the merge reopens the
        // file once, from `source` (footer + data through one reader).
        assertThat(aKeys.opened.get()).describedAs("key source opens").isEqualTo(1)
        assertThat(a.opened.get()).describedAs("source opens").isEqualTo(1)
        assertThat(a.dataReads.get()).describedAs("the merge reads the data through source").isPositive()
        // Without its own key source the pre-pass falls back to `source`.
        assertThat(b.opened.get()).isEqualTo(2)
        assertThat(a.openStreams() + aKeys.openStreams() + b.openStreams()).isZero()
        assertThat(readKv(out).map { it.first }).containsExactly(1, 2, 3, 4, 5, 6)
    }

    @Test
    fun `an unsorted file among sorted ones is the only spill`() {
        val result =
            rewrite(
                listOf(
                    input(LocalInputFile(clientKv("mix-a", listOf(1, 3, 5))), "a"),
                    input(LocalInputFile(clientKv("mix-b", listOf(9, 2, 4))), "b", 100),
                    input(LocalInputFile(clientKv("mix-c", listOf(2, 6, 8))), "c", 200),
                ),
                fresh("mix-out.parquet"),
                spill(spillParent()).copy(verifyMinBytes = 0),
            )
        assertThat(result.filesVerified).isEqualTo(2)
        assertThat(result.filesUnsorted).isEqualTo(1)
        assertThat(result.runsTrusted).isEqualTo(2)
        assertThat(result.runsSpilled).isEqualTo(1)
    }

    @Test
    fun `a verified file the merge budget demotes is spilled, and counted both ways`() {
        val bounds = spill(spillParent()).copy(verifyMinBytes = 0)
        val result =
            rewrite(
                listOf(
                    input(LocalInputFile(clientKv("dem-a", listOf(1, 3))), "a", survivors = 2),
                    input(LocalInputFile(clientKv("dem-b", listOf(5, 2))), "b", 100, survivors = 2),
                ),
                fresh("dem-out.parquet"),
                // No room for a run: every row goes through one chunk.
                bounds.copy(mergeBudgetBytes = 1),
            )
        assertThat(result.filesVerified).isEqualTo(1)
        assertThat(result.runsDemoted).isEqualTo(1)
        assertThat(result.runsTrusted).isZero()
        assertThat(result.rowsWritten).isEqualTo(4)
    }

    @Test
    fun `files under the size floor are not checked, at it they are, and 0 checks every known size`() {
        val a = clientKv("floor-a", listOf(1, 2))
        val b = clientKv("floor-b", listOf(3, 4))
        val size = Files.size(a)

        fun at(
            floor: Long,
            sizeA: Long = size,
        ) = rewrite(
            listOf(input(LocalInputFile(a), "a", fileSizeBytes = sizeA), input(LocalInputFile(b), "b", 100)),
            fresh("floor-out.parquet"),
            spill(spillParent()).copy(verifyMinBytes = floor),
        )
        val under = at(size + 1)
        assertThat(listOf(under.filesVerified, under.filesUnchecked)).containsExactly(0, 2)
        val atFloor = at(minOf(size, Files.size(b)))
        assertThat(atFloor.filesVerified).isEqualTo(2)
        assertThat(atFloor.filesUnchecked).isZero()
        val unknown = at(0, sizeA = 0)
        assertThat(listOf(unknown.filesVerified, unknown.filesUnchecked))
            .describedAs("an unknown size is never checked")
            .containsExactly(1, 1)
    }

    @Test
    fun `pre-pass verdicts land on the file that earned them when inputs open in parallel`() {
        // verifySortedness attributes each verdict with a cursor (`next++`)
        // and not with the input the callback hands it, which is correct
        // ONLY because forEachOpenedInput calls back serially and in INPUT
        // order at any parallelism. This pins that contract directly; the
        // property test randomizes parallelism and catches a break only
        // by chance.
        //
        // Only the MIDDLE file is unsorted, and the FIRST file's pre-pass
        // open is slow, so with three slots its successors finish opening
        // before it: a window that handed readers over in completion order
        // would give a's slot b's verdict, read b in place as a trusted run
        // and publish a mis-sorted file.
        val labels = listOf("a", "b", "c")
        val paths =
            listOf(
                clientKv("order-a", listOf(1, 4, 7)),
                clientKv("order-b", listOf(8, 2, 5)),
                clientKv("order-c", listOf(3, 6, 9)),
            )
        val sources = paths.map { CountingInputFile(it) }
        val keySources = paths.map { CountingInputFile(it) }
        keySources[0].onOpen = { Thread.sleep(200) }
        // The merge's opens, which say WHICH file took which path: the
        // admitted runs' footers are opened before the chunk phase opens
        // anything (see `admitted runs are opened largest first ...`).
        val sourceOpens = java.util.Collections.synchronizedList(mutableListOf<String>())
        sources.forEachIndexed { i, f -> f.onOpen = { sourceOpens += labels[i] } }
        // A real floor every input clears, rather than 0.
        val floor = paths.minOf { Files.size(it) }
        val out = fresh("order-out.parquet")
        val result =
            rewrite(
                labels.indices.map { i ->
                    input(sources[i], labels[i], rowIdStart = i * 100L, survivors = 3).copy(keySource = keySources[i])
                },
                out,
                spill(spillParent()).copy(verifyMinBytes = floor),
                parallelism = 3,
            )
        assertThat(readKv(out).map { it.first }).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9)
        assertThat(result.filesVerified).isEqualTo(2)
        assertThat(result.filesUnsorted).isEqualTo(1)
        assertThat(result.filesUnchecked).isZero()
        assertThat(result.runsTrusted).describedAs("a and c, read in place").isEqualTo(2)
        assertThat(result.runsSpilled).describedAs("b, and only b").isEqualTo(1)
        assertThat(result.runsDemoted).isZero()
        assertThat(sourceOpens).hasSize(3)
        assertThat(sourceOpens.take(2)).describedAs("the trusted runs' footers").containsExactlyInAnyOrder("a", "c")
        assertThat(sourceOpens.drop(2)).describedAs("the chunk phase: the spilled file").containsExactly("b")
        for (f in sources + keySources) assertThat(f.openStreams()).isZero()
    }

    // ---- appends a run of unknown range blocks (files_unranged) -------------------

    /** A prior compaction output of [keys] at [rowIdStart], sorted by k, as the rewriter writes it. */
    private fun priorKv(
        name: String,
        keys: List<Long>,
        rowIdStart: Long,
    ): Path {
        val out = fresh("$name-prior.parquet")
        rewriteToLocal(listOf(localInput(clientKv(name, keys), rowIdStart)), kv, byK, out)
        return out
    }

    /**
     * An output-shaped file with NO statistics on k, so its first-key
     * range is unknown — otherwise appendable (explicit ids, the output
     * schema, row-id statistics).
     */
    private fun unrangedKv(
        name: String,
        keys: List<Long>,
        rowIdStart: Long,
    ): Path {
        val schema = ParquetRewriter.outputSchema(kv)
        val path = fresh("$name.parquet")
        val factory = SimpleGroupFactory(schema)
        ExampleParquetWriter.builder(LocalOutputFile(path))
            .withType(schema)
            .withStatisticsEnabled("k", false)
            .build()
            .use { w ->
                for ((i, k) in keys.withIndex()) {
                    w.write(
                        factory.newGroup().append("k", k).append("v", "v$k")
                            .append(ParquetRewriter.ROW_ID_COLUMN, rowIdStart + i),
                    )
                }
            }
        return path
    }

    private fun appendBlockedGroup(
        floor: Long,
        extra: List<ParquetRewriter.Input> = emptyList(),
    ): Pair<ParquetRewriter.RewriteResult, Path> {
        // Interleaved keys: a run of unknown range really does overlap.
        val ranged = priorKv("ranged", (0L until 10L).map { it * 2 }, 0)
        val unranged = unrangedKv("unranged", (0L until 10L).map { it * 2 + 1 }, 100)
        val out = fresh("unranged-out.parquet")
        val result =
            ParquetRewriter.rewrite(
                listOf(
                    localInput(ranged, 0, explicitRowIds = true, trustedSorted = true),
                    localInput(unranged, 0, explicitRowIds = true, trustedSorted = true),
                ) + extra,
                kv,
                byK,
                LocalDiscardableOutput(out),
                spill = spill(spillParent()),
                appendFloorBytes = floor,
            )
        return result to out
    }

    @Test
    fun `a run with no usable first-key range blocks every append, and is counted`() {
        // Both runs pass appendRefusal at a floor of 1 byte; the unranged
        // one makes both merge. Before files_unranged this was a debug
        // line and nothing else.
        val (result, out) = appendBlockedGroup(floor = 1)
        assertThat(result.rowGroupsAppended).isZero()
        assertThat(result.bytesAppended).isZero()
        assertThat(result.filesUnranged).describedAs("the one run whose range is unknown").isEqualTo(1)
        assertThat(result.runsTrusted).isEqualTo(2)
        assertThat(readKv(out).map { it.first }).isEqualTo((0L until 20L).toList())
    }

    @Test
    fun `an unranged run blocks nothing - and is not counted - when no run could append`() {
        // The production floor (32 MiB) refuses both kilobyte fixtures in
        // appendRefusal, so there is no candidate for the unknown range to
        // block: counting it would report a cost nobody paid.
        val (result, out) = appendBlockedGroup(floor = ParquetRewriter.APPEND_MIN_ROW_GROUP_BYTES)
        assertThat(result.rowGroupsAppended).isZero()
        assertThat(result.filesUnranged).isZero()
        assertThat(readKv(out).map { it.first }).isEqualTo((0L until 20L).toList())
    }

    @Test
    fun `an unranged run is not counted when the only candidate overlaps a spill chunk anyway`() {
        // The ranged candidate passes appendRefusal, but a client file's
        // spilled chunk [5, 7] sits inside its range [0, 18]: it would have
        // merged with no unranged run present, so the unranged run cost
        // nothing and is not counted.
        val client = clientKv("unranged-spill", listOf(7, 5))
        val (result, out) =
            appendBlockedGroup(floor = 1, extra = listOf(input(LocalInputFile(client), "client", 1_000)))
        assertThat(result.rowGroupsAppended).isZero()
        assertThat(result.runsSpilled).describedAs("the client file's chunk").isEqualTo(1)
        assertThat(result.filesUnranged).isZero()
        assertThat(readKv(out).map { it.first })
            .isEqualTo(((0L until 20L).toList() + listOf(5L, 7L)).sorted())
    }

    // ---- the counting / fault-injecting input ------------------------------------

    /**
     * A local parquet [InputFile] that counts what is done to it: streams
     * opened and closed, and reads of the DATA region (anything before the
     * footer), with an optional failure injected on the first data read.
     */
    private class CountingInputFile(private val path: Path) : InputFile {
        private val delegate = LocalInputFile(path)
        val opened = AtomicInteger()
        val closed = AtomicInteger()
        val dataReads = AtomicInteger()
        var failDataRead: (() -> Throwable)? = null

        /** Data reads at or past this position fail with [failDataRead]. */
        var failFrom: Long = 0
        var onOpen: () -> Unit = {}
        var onDataRead: () -> Unit = {}

        /** Where the footer starts: reads below it are row-group reads. */
        private val dataEnd: Long =
            run {
                val bytes = Files.readAllBytes(path)
                val footerLength = ByteBuffer.wrap(bytes, bytes.size - 8, 4).order(ByteOrder.LITTLE_ENDIAN).int
                bytes.size - 8L - footerLength
            }

        fun openStreams(): Int = opened.get() - closed.get()

        override fun getLength(): Long = delegate.length

        override fun newStream(): SeekableInputStream {
            opened.incrementAndGet()
            onOpen()
            return Counted(delegate.newStream())
        }

        override fun toString(): String = path.fileName.toString()

        private inner class Counted(private val d: SeekableInputStream) : SeekableInputStream() {
            private var isClosed = false

            private fun guard() {
                if (d.pos < dataEnd) {
                    dataReads.incrementAndGet()
                    onDataRead()
                    if (d.pos >= failFrom) failDataRead?.let { throw it() }
                }
            }

            override fun getPos(): Long = d.pos

            override fun seek(newPos: Long) = d.seek(newPos)

            override fun read(): Int {
                guard()
                return d.read()
            }

            override fun read(
                b: ByteArray,
                off: Int,
                len: Int,
            ): Int {
                guard()
                return d.read(b, off, len)
            }

            override fun readFully(bytes: ByteArray) {
                guard()
                d.readFully(bytes)
            }

            override fun readFully(
                bytes: ByteArray,
                start: Int,
                len: Int,
            ) {
                guard()
                d.readFully(bytes, start, len)
            }

            override fun read(buf: ByteBuffer): Int {
                guard()
                return d.read(buf)
            }

            override fun readFully(buf: ByteBuffer) {
                guard()
                d.readFully(buf)
            }

            override fun close() {
                if (!isClosed) {
                    isClosed = true
                    closed.incrementAndGet()
                }
                d.close()
            }
        }
    }
}
