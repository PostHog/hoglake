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
import org.apache.parquet.hadoop.metadata.ParquetMetadata
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
 * hoglake#134 package D1: an input that is a prior compaction output of
 * the live schema, with no deletes and big row groups, is APPENDED to the
 * output byte for byte instead of decoded and re-encoded — on the unsorted
 * path always, on the sorted path when its first-key range is disjoint
 * from every other run's. Each disqualifier is pinned by a test that
 * reds when it is removed; the property test in [ExternalMergeSortTest]
 * is the oracle for appended runs mixed into arbitrary groups.
 *
 * The append floor is passed explicitly ([ParquetRewriter.rewrite]'s
 * `appendFloorBytes`): fixtures are kilobytes, the production floor is
 * 32 MiB.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RowGroupAppendTest {
    private val tmp: Path = Files.createTempDirectory("row-group-append")
    private var seq = 0

    @AfterAll
    fun tearDown() {
        tmp.toFile().deleteRecursively()
    }

    private fun fresh(name: String): Path = tmp.resolve("${seq++}-$name")

    private val kv =
        listOf(
            Column(1, 0, ColumnDef("k", ColType.LONG)),
            Column(2, 1, ColumnDef("v", ColType.STRING)),
        )
    private val byK = listOf(SortFieldDef(1, SortDirection.ASC, NullOrder.NULLS_LAST))

    private val clientSchema: MessageType =
        Types.buildMessage()
            .addField(Types.optional(PrimitiveTypeName.INT64).id(1).named("k"))
            .addField(
                Types.optional(PrimitiveTypeName.BINARY).`as`(LogicalTypeAnnotation.stringType()).id(2).named("v"),
            )
            .named("t")

    /** A client file of (k, "v<k>") rows, null k for a null; positional ids. */
    private fun client(
        name: String,
        keys: List<Long?>,
    ): Path {
        val path = fresh("$name.parquet")
        val factory = SimpleGroupFactory(clientSchema)
        ExampleParquetWriter.builder(LocalOutputFile(path))
            .withType(clientSchema)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .build()
            .use { w ->
                for (k in keys) {
                    val g = factory.newGroup()
                    k?.let { g.add(0, it) }
                    g.add(1, "v$k")
                    w.write(g)
                }
            }
        return path
    }

    /**
     * A PRIOR OUTPUT: a client file of [keys] at [rowIdStart] rewritten by
     * the rewriter itself under [live] (sorted by [sort] when given).
     */
    private fun prior(
        name: String,
        keys: List<Long?>,
        rowIdStart: Long,
        sort: List<SortFieldDef> = emptyList(),
        live: List<Column> = kv,
        codec: ParquetRewriter.OutputCodec = ParquetRewriter.OutputCodec(),
    ): Path {
        val out = fresh("$name-prior.parquet")
        rewriteToLocal(listOf(localInput(client(name, keys), rowIdStart)), live, sort, out, codec = codec)
        return out
    }

    private fun explicit(
        path: Path,
        trusted: Boolean = false,
        deletes: DeletionVector? = null,
    ) = localInput(path, 0, deletes = deletes, explicitRowIds = true, trustedSorted = trusted)

    private fun rewrite(
        inputs: List<ParquetRewriter.Input>,
        out: Path,
        sort: List<SortFieldDef> = emptyList(),
        live: List<Column> = kv,
        floor: Long = 1,
        chunkRows: Long = Long.MAX_VALUE,
    ): ParquetRewriter.RewriteResult =
        ParquetRewriter.rewrite(
            inputs,
            live,
            sort,
            LocalDiscardableOutput(out),
            spill = if (sort.isEmpty()) null else roomySpill().copy(chunkRows = chunkRows),
            appendFloorBytes = floor,
        )

    private fun footer(path: Path): ParquetMetadata = ParquetFileReader.open(LocalInputFile(path)).use { it.footer }

    /** (k, rowId) in file order. */
    private fun rows(path: Path): List<Pair<Long?, Long>> {
        val out = ArrayList<Pair<Long?, Long>>()
        ParquetFileReader.open(LocalInputFile(path)).use { reader ->
            val schema = reader.footer.fileMetaData.schema
            val io = ColumnIOFactory().getColumnIO(schema)
            val k = schema.getFieldIndex("k")
            val id = schema.getFieldIndex(ParquetRewriter.ROW_ID_COLUMN)
            var pages = reader.readNextRowGroup()
            while (pages != null) {
                val rr = io.getRecordReader(pages, GroupRecordConverter(schema))
                repeat(Math.toIntExact(pages.rowCount)) {
                    val g: Group = rr.read()
                    out += (if (g.getFieldRepetitionCount(k) == 0) null else g.getLong(k, 0)) to g.getLong(id, 0)
                }
                pages = reader.readNextRowGroup()
            }
        }
        return out
    }

    private fun dv(vararg positions: Long): DeletionVector =
        PuffinDeletionVector.read(PuffinTestFiles.deletionVector(positions.toList()))

    // ---- the unsorted path ------------------------------------------------------

    @Test
    fun `appended column chunks are the input's bytes, with their indexes, and the counts hold`() {
        val a = prior("a", (0L until 40L).toList(), 0)
        val b = prior("b", (100L until 130L).toList(), 100)
        val c = client("c", listOf(7, 8, 9))
        val out = fresh("out.parquet")
        val result = rewrite(listOf(explicit(a), localInput(c, 1_000), explicit(b)), out)

        val inA = footer(a).blocks.single()
        val inB = footer(b).blocks.single()
        assertThat(result.rowGroupsAppended).isEqualTo(2)
        assertThat(result.bytesAppended).isEqualTo(inA.compressedSize + inB.compressedSize)
        assertThat(result.rowsWritten).isEqualTo(73)
        assertThat(result.minRowId).isEqualTo(0)
        assertThat(rows(out).map { it.second }.sorted())
            .isEqualTo((0L until 40L) + (100L until 130L) + listOf(1_000L, 1_001L, 1_002L))

        // Appended first, in input order; the encoded rows keep filling
        // their row group across the appends and land last.
        val blocks = footer(out).blocks
        assertThat(blocks.map { it.rowCount }).containsExactly(40L, 30L, 3L)
        val outBytes = Files.readAllBytes(out)
        for ((input, appended) in listOf(a to blocks[0], b to blocks[1])) {
            val inBytes = Files.readAllBytes(input)
            val source = footer(input).blocks.single()
            for ((i, chunk) in appended.columns.withIndex()) {
                val src = source.columns[i]
                assertThat(chunk.path).isEqualTo(src.path)
                assertThat(
                    outBytes.copyOfRange(chunk.startingPos.toInt(), (chunk.startingPos + chunk.totalSize).toInt()),
                )
                    .describedAs("%s of %s", chunk.path, input.fileName)
                    .isEqualTo(inBytes.copyOfRange(src.startingPos.toInt(), (src.startingPos + src.totalSize).toInt()))
                assertThat(chunk.statistics).isEqualTo(src.statistics)
                assertThat(chunk.codec).isEqualTo(src.codec)
            }
        }
        // The page indexes came along: appendRowGroups would have dropped
        // the column index.
        ParquetFileReader.open(LocalInputFile(out)).use { reader ->
            for (chunk in blocks.take(2).flatMap { it.columns }) {
                assertThat(reader.readColumnIndex(chunk)).describedAs("column index of %s", chunk.path).isNotNull()
                assertThat(reader.readOffsetIndex(chunk)).describedAs("offset index of %s", chunk.path).isNotNull()
            }
        }
        // And the output's statistics, which the commit registers from,
        // still cover every row id.
        val ids =
            blocks.map {
                    b ->
                b.columns.single { it.path.toDotString() == ParquetRewriter.ROW_ID_COLUMN }.statistics
            }
        assertThat(ids.minOf { it.genericGetMin() as Long }).isEqualTo(0L)
        assertThat(ids.maxOf { it.genericGetMax() as Long }).isEqualTo(1_002L)
    }

    @Test
    fun `an appended row group keeps its codec and its bloom filter`() {
        val a =
            prior(
                "snappy",
                (0L until 20L).toList(),
                0,
                codec = ParquetRewriter.OutputCodec(CompressionCodecName.SNAPPY),
            )
        // An output-shaped file with a bloom filter on k: the rewriter
        // writes none, so build one by hand.
        val schema = ParquetRewriter.outputSchema(kv)
        val bloomed = fresh("bloomed.parquet")
        val factory = SimpleGroupFactory(schema)
        ExampleParquetWriter.builder(LocalOutputFile(bloomed))
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.GZIP)
            .withBloomFilterEnabled("k", true)
            .build()
            .use {
                    w ->
                for (i in 50L until 60L) w.write(
                    factory.newGroup().append("k", i).append("v", "v$i").append(ParquetRewriter.ROW_ID_COLUMN, i),
                )
            }
        val out = fresh("mixed.parquet")
        val result = rewrite(listOf(explicit(a), explicit(bloomed), localInput(client("c", listOf(1)), 500)), out)
        assertThat(result.rowGroupsAppended).isEqualTo(2)
        val blocks = footer(out).blocks
        assertThat(blocks.map { b -> b.columns.map { it.codec }.distinct().single() })
            .containsExactly(CompressionCodecName.SNAPPY, CompressionCodecName.GZIP, CompressionCodecName.ZSTD)
        ParquetFileReader.open(LocalInputFile(out)).use { reader ->
            val k = blocks[1].columns.single { it.path.toDotString() == "k" }
            val bloom = reader.readBloomFilter(k)
            assertThat(bloom).isNotNull()
            assertThat(bloom.findHash(bloom.hash(55L))).isTrue()
        }
    }

    @Test
    fun `a zero-row row group in an appended file is skipped, not handed to endBlock`() {
        // parquet-java never writes one, other writers may; endBlock
        // refuses a block of zero rows. A footer with one spliced in.
        val a = prior("zero-row", (0L until 10L).toList(), 0)
        val real = footer(a)
        val spliced =
            ParquetMetadata(real.fileMetaData, real.blocks + org.apache.parquet.hadoop.metadata.BlockMetaData())
        val source = LocalInputFile(a)
        val out = fresh("zero-row-out.parquet")
        source.newStream().use { stream ->
            ParquetFileReader(source, spliced, org.apache.parquet.ParquetReadOptions.builder().build(), stream).use {
                    reader ->
                ParquetRewriter.writingTo(
                    LocalOutputFile(out),
                    ParquetRewriter.outputSchema(kv),
                    ParquetRewriter.OutputCodec(),
                ) { w ->
                    w.appendRowGroups(source, reader, flushPending = false)
                    assertThat(w.rowGroupsAppended).isEqualTo(1)
                }
            }
        }
        assertThat(rows(out).map { it.second }).isEqualTo((0L until 10L).toList())
    }

    // ---- each disqualifier routes to the decode path ----------------------------

    @Test
    fun `an input with a deletion vector is decoded, and its deletes applied`() {
        val a = prior("dv", (0L until 10L).toList(), 0)
        val out = fresh("dv-out.parquet")
        val result = rewrite(listOf(explicit(a, deletes = dv(2, 3))), out)
        assertThat(result.rowGroupsAppended).isZero()
        assertThat(rows(out).map { it.second }).isEqualTo((0L until 10L) - setOf(2L, 3L))
    }

    @Test
    fun `an output-shaped file registered with positional ids is refused, not appended`() {
        // The decode path refuses the contradiction (the reserved carrier
        // on a file registered without explicit ids); appending it would
        // have published its carrier's values as row ids the catalog never
        // assigned.
        val a = prior("positional", (0L until 10L).toList(), 0)
        assertThatThrownBy { rewrite(listOf(localInput(a, 0)), fresh("positional-out.parquet")) }
            .isInstanceOf(InvalidDataException::class.java)
    }

    @Test
    fun `a field id the live schema no longer has is decoded, not copied under the old id`() {
        val a = prior("old-id", (0L until 10L).toList(), 0)
        // v dropped and re-added: same name, new id.
        val live = listOf(kv[0], Column(3, 1, ColumnDef("v", ColType.STRING)))
        val out = fresh("new-id-out.parquet")
        val result = rewrite(listOf(explicit(a)), out, live = live)
        assertThat(result.rowGroupsAppended).isZero()
        val schema = footer(out).fileMetaData.schema
        assertThat(schema.getType("v").id.intValue()).isEqualTo(3)
        assertThat(rows(out).map { it.second }).isEqualTo((0L until 10L).toList())
    }

    @Test
    fun `a differing logical annotation is decoded and re-stamped`() {
        val a = prior("string", (0L until 10L).toList(), 0)
        // string -> json is a legal identity copy that re-annotates the leaf.
        val live = listOf(kv[0], Column(2, 1, ColumnDef("v", ColType.JSON)))
        val out = fresh("json-out.parquet")
        val result = rewrite(listOf(explicit(a)), out, live = live)
        assertThat(result.rowGroupsAppended).isZero()
        assertThat(footer(out).fileMetaData.schema.getType("v").logicalTypeAnnotation)
            .isEqualTo(LogicalTypeAnnotation.jsonType())
    }

    @Test
    fun `the append floor is inclusive, and a row group under it is re-encoded`() {
        val a = prior("floor", (0L until 50L).toList(), 0)
        val size = footer(a).blocks.single().compressedSize
        assertThat(rewrite(listOf(explicit(a)), fresh("at.parquet"), floor = size).rowGroupsAppended).isEqualTo(1)
        val under = fresh("under.parquet")
        assertThat(rewrite(listOf(explicit(a)), under, floor = size + 1).rowGroupsAppended).isZero()
        assertThat(rows(under).map { it.second }).isEqualTo((0L until 50L).toList())
    }

    @Test
    fun `the production floor is 32 MiB`() {
        assertThat(ParquetRewriter.APPEND_MIN_ROW_GROUP_BYTES).isEqualTo(32L * 1024 * 1024)
        // ...and it is the default a caller gets.
        val a = prior("default-floor", (0L until 10L).toList(), 0)
        val result =
            ParquetRewriter.rewrite(listOf(explicit(a)), kv, emptyList(), LocalDiscardableOutput(fresh("d.parquet")))
        assertThat(result.rowGroupsAppended).isZero()
    }

    @Test
    fun `a file without row-id statistics is decoded`() {
        // minRowId is read off the carrier's statistics when nothing is
        // decoded; without them the file cannot be appended.
        val schema = ParquetRewriter.outputSchema(kv)
        val path = fresh("no-stats.parquet")
        val factory = SimpleGroupFactory(schema)
        ExampleParquetWriter.builder(LocalOutputFile(path))
            .withType(schema)
            .withStatisticsEnabled(ParquetRewriter.ROW_ID_COLUMN, false)
            .build()
            .use {
                    w ->
                for (i in 5L until 15L) w.write(
                    factory.newGroup().append("k", i).append("v", "v$i").append(ParquetRewriter.ROW_ID_COLUMN, i),
                )
            }
        val out = fresh("no-stats-out.parquet")
        val result = rewrite(listOf(explicit(path)), out)
        assertThat(result.rowGroupsAppended).isZero()
        assertThat(result.minRowId).isEqualTo(5L)
    }

    @Test
    fun `appendRefusal names every disqualifier and passes a prior output`() {
        val a = prior("refusal", (0L until 10L).toList(), 0)
        val f = footer(a)
        val schema = ParquetRewriter.outputSchema(kv)
        assertThat(ParquetRewriter.appendRefusal(explicit(a), f, schema, 1)).isNull()
        assertThat(
            ParquetRewriter.appendRefusal(explicit(a, deletes = dv(1)), f, schema, 1),
        ).contains("deletion vector")
        assertThat(ParquetRewriter.appendRefusal(localInput(a, 0), f, schema, 1)).contains("positional")
        assertThat(ParquetRewriter.appendRefusal(explicit(a), f, ParquetRewriter.outputSchema(listOf(kv[0])), 1))
            .contains("schema")
        assertThat(ParquetRewriter.appendRefusal(explicit(a), f, schema, Long.MAX_VALUE)).contains("floor")
    }

    // ---- the sorted path ----------------------------------------------------------

    @Test
    fun `disjoint trusted runs append around the merged rows, in key order`() {
        val low = prior("low", (0L until 10L).toList(), 0, byK)
        val high = prior("high", (100L until 110L).toList(), 100, byK)
        val mid = client("mid", listOf(55, 50, 59, 52))
        val out = fresh("sorted-out.parquet")
        val result =
            rewrite(
                listOf(explicit(high, trusted = true), localInput(mid, 1_000), explicit(low, trusted = true)),
                out,
                sort = byK,
                chunkRows = 2,
            )
        assertThat(result.rowGroupsAppended).isEqualTo(2)
        assertThat(result.runsTrusted).isEqualTo(2)
        val got = rows(out)
        assertThat(got.map { it.first }).isEqualTo((0L until 10L) + listOf(50L, 52L, 55L, 59L) + (100L until 110L))
        assertThat(result.minRowId).isEqualTo(0)
        // The merged rows were flushed as their own row group before the
        // block that follows them.
        assertThat(footer(out).blocks.map { it.rowCount }).containsExactly(10L, 4L, 10L)
    }

    @Test
    fun `overlapping trusted runs merge, and only the disjoint one appends`() {
        val a = prior("a", (0L until 10L).toList(), 0, byK)
        val b = prior("b", (5L until 15L).toList(), 100, byK)
        val c = prior("c", (200L until 210L).toList(), 200, byK)
        val out = fresh("overlap-out.parquet")
        val result =
            rewrite(
                listOf(explicit(a, trusted = true), explicit(b, trusted = true), explicit(c, trusted = true)),
                out,
                byK,
            )
        assertThat(result.rowGroupsAppended).isEqualTo(1)
        assertThat(rows(out).map { it.first }).isSorted()
        assertThat(rows(out)).hasSize(30)
    }

    @Test
    fun `a tie on the first key at a boundary merges`() {
        // a ends at 9, b starts at 9: their rows interleave by row id at 9.
        val a = prior("tie-a", (0L until 10L).toList(), 50, byK)
        val b = prior("tie-b", (9L until 19L).toList(), 0, byK)
        val out = fresh("tie-out.parquet")
        val result = rewrite(listOf(explicit(a, trusted = true), explicit(b, trusted = true)), out, byK)
        assertThat(result.rowGroupsAppended).isZero()
        val got = rows(out)
        // (9, id 0) from b sorts before (9, id 59) from a.
        assertThat(got.filter { it.first == 9L }.map { it.second }).containsExactly(0L, 59L)
    }

    @Test
    fun `a spilled chunk whose range overlaps a trusted run blocks its append`() {
        val a = prior("chunk-a", (0L until 10L).toList(), 0, byK)
        val c = client("chunk-c", listOf(5, 200))
        val out = fresh("chunk-out.parquet")
        val result = rewrite(listOf(explicit(a, trusted = true), localInput(c, 1_000)), out, byK)
        assertThat(result.rowGroupsAppended).isZero()
        assertThat(rows(out).map { it.first }).isSorted()
    }

    @Test
    fun `a float or double first key never appends - NaN is outside the statistics`() {
        // a holds a NaN, which sorts GREATEST here and which parquet leaves
        // out of min/max: a's statistics say [0, 1], which looks disjoint
        // from b's [100, 101], but a's NaN belongs after b.
        val live = listOf(Column(1, 0, ColumnDef("k", ColType.DOUBLE)), kv[1])
        val byD = listOf(SortFieldDef(1, SortDirection.ASC, NullOrder.NULLS_LAST))
        val a = doubles("da", listOf(0.0, 1.0, Double.NaN), 0, live, byD)
        val b = doubles("db", listOf(100.0, 101.0), 100, live, byD)
        val out = fresh("double-out.parquet")
        val result = rewrite(listOf(explicit(a, trusted = true), explicit(b, trusted = true)), out, byD, live = live)
        assertThat(result.rowGroupsAppended).isZero()
        val values =
            ParquetFileReader.open(LocalInputFile(out)).use { reader ->
                val schema = reader.footer.fileMetaData.schema
                val rr =
                    ColumnIOFactory().getColumnIO(
                        schema,
                    ).getRecordReader(reader.readNextRowGroup(), GroupRecordConverter(schema))
                (0 until 5).map { rr.read().getDouble(0, 0) }
            }
        assertThat(values).containsExactly(0.0, 1.0, 100.0, 101.0, Double.NaN)
    }

    @Test
    fun `a run whose range is unknown blocks every append`() {
        // b is an output-shaped trusted run with NO statistics on k, so
        // nothing is known to be disjoint from it — and here it does
        // interleave with a, whose own range is known.
        val a = prior("known", (0L until 10L).map { it * 2 }, 0, byK)
        val schema = ParquetRewriter.outputSchema(kv)
        val b = fresh("unknown.parquet")
        val factory = SimpleGroupFactory(schema)
        ExampleParquetWriter.builder(LocalOutputFile(b))
            .withType(schema)
            .withStatisticsEnabled("k", false)
            .build()
            .use { w ->
                for (i in 0L until 10L) {
                    w.write(
                        factory.newGroup().append(
                            "k",
                            i * 2 + 1,
                        ).append("v", "v").append(ParquetRewriter.ROW_ID_COLUMN, 100 + i),
                    )
                }
            }
        val out = fresh("unknown-out.parquet")
        val result = rewrite(listOf(explicit(a, trusted = true), explicit(b, trusted = true)), out, byK)
        assertThat(result.rowGroupsAppended).isZero()
        assertThat(rows(out).map { it.first }).isEqualTo((0L until 20L).toList())
    }

    private fun doubles(
        name: String,
        values: List<Double>,
        start: Long,
        live: List<Column>,
        sort: List<SortFieldDef>,
    ): Path {
        val dSchema =
            Types.buildMessage()
                .addField(Types.optional(PrimitiveTypeName.DOUBLE).id(1).named("k"))
                .addField(
                    Types.optional(PrimitiveTypeName.BINARY).`as`(LogicalTypeAnnotation.stringType()).id(2).named("v"),
                )
                .named("t")
        val src = fresh("$name.parquet")
        ExampleParquetWriter.builder(LocalOutputFile(src)).withType(dSchema).build().use { w ->
            val f = SimpleGroupFactory(dSchema)
            for (v in values) w.write(f.newGroup().append("k", v).append("v", "x"))
        }
        val out = fresh("$name-prior.parquet")
        rewriteToLocal(listOf(localInput(src, start)), live, sort, out)
        return out
    }

    @Test
    fun `nulls sit at their null order's end of a range`() {
        // NULLS_LAST: a's nulls sort after b's values, so a spans b.
        val a = prior("nl-a", listOf(0L, 1L, null), 0, byK)
        val b = prior("nl-b", listOf(100L, 101L), 100, byK)
        val last = rewrite(listOf(explicit(a, trusted = true), explicit(b, trusted = true)), fresh("nl.parquet"), byK)
        assertThat(last.rowGroupsAppended).isZero()
        // NULLS_FIRST: a's nulls sort first, and a ends at 1 — disjoint.
        val nullsFirst = listOf(SortFieldDef(1, SortDirection.ASC, NullOrder.NULLS_FIRST))
        val a2 = prior("nf-a", listOf(0L, 1L, null), 0, nullsFirst)
        val b2 = prior("nf-b", listOf(100L, 101L), 100, nullsFirst)
        val out = fresh("nf.parquet")
        val first = rewrite(listOf(explicit(b2, trusted = true), explicit(a2, trusted = true)), out, nullsFirst)
        assertThat(first.rowGroupsAppended).isEqualTo(2)
        assertThat(rows(out).map { it.first }).containsExactly(null, 0L, 1L, 100L, 101L)
    }

    @Test
    fun `descending order emits the appended blocks high to low`() {
        val desc = listOf(SortFieldDef(1, SortDirection.DESC, NullOrder.NULLS_LAST))
        val a = prior("desc-a", (0L until 5L).toList(), 0, desc)
        val b = prior("desc-b", (100L until 105L).toList(), 100, desc)
        val out = fresh("desc.parquet")
        val result = rewrite(listOf(explicit(a, trusted = true), explicit(b, trusted = true)), out, desc)
        assertThat(result.rowGroupsAppended).isEqualTo(2)
        assertThat(rows(out).map { it.first }).isEqualTo((104L downTo 100L) + (4L downTo 0L))
    }

    @Test
    fun `descending runs that overlap merge`() {
        // DESC puts a run's MAX first: a [0, 10] and b [5, 15] overlap,
        // which only a range taken in key order (lo = max) sees.
        val desc = listOf(SortFieldDef(1, SortDirection.DESC, NullOrder.NULLS_LAST))
        val a = prior("desc-over-a", (0L..10L).toList(), 0, desc)
        val b = prior("desc-over-b", (5L..15L).toList(), 100, desc)
        val out = fresh("desc-over.parquet")
        val result = rewrite(listOf(explicit(a, trusted = true), explicit(b, trusted = true)), out, desc)
        assertThat(result.rowGroupsAppended).isZero()
        assertThat(rows(out).map { it.first }).isSortedAccordingTo(compareByDescending { it })
    }

    @Test
    fun `footerRange reads the first key in merge order, and refuses what it cannot vouch for`() {
        val schema = ParquetRewriter.outputSchema(kv)
        val f = footer(prior("range", listOf(3L, 7L, null, 5L), 0, byK))
        val asc = ParquetRewriter.SortKeys(schema, byK).footerRange(f)!!
        assertThat(listOf(asc.lo, asc.hi)).containsExactly(3L, null)
        val desc =
            ParquetRewriter.SortKeys(schema, listOf(SortFieldDef(1, SortDirection.DESC, NullOrder.NULLS_FIRST)))
                .footerRange(f)!!
        assertThat(listOf(desc.lo, desc.hi)).containsExactly(null, 3L)
        val descLast =
            ParquetRewriter.SortKeys(schema, listOf(SortFieldDef(1, SortDirection.DESC, NullOrder.NULLS_LAST)))
                .footerRange(f)!!
        assertThat(listOf(descLast.lo, descLast.hi)).containsExactly(7L, null)

        // A key leaf of another type than the output's: a signed INT32 file
        // under a uint8 column would mask -1 to the TOP of the range while
        // its statistics put it at the bottom.
        val uint8 = listOf(Column(1, 0, ColumnDef("k", ColType.UINT8)), kv[1])
        val signed =
            Types.buildMessage()
                .addField(Types.optional(PrimitiveTypeName.INT32).id(1).named("k"))
                .addField(
                    Types.optional(PrimitiveTypeName.BINARY).`as`(LogicalTypeAnnotation.stringType()).id(2).named("v"),
                )
                .named("t")
        val path = fresh("signed.parquet")
        ExampleParquetWriter.builder(LocalOutputFile(path)).withType(signed).build().use { w ->
            val g = SimpleGroupFactory(signed)
            for (v in listOf(-1, 5)) w.write(g.newGroup().append("k", v).append("v", "x"))
        }
        assertThat(ParquetRewriter.SortKeys(ParquetRewriter.outputSchema(uint8), byK).footerRange(footer(path)))
            .isNull()

        // A double key: NaN is not in min/max.
        val dLive = listOf(Column(1, 0, ColumnDef("k", ColType.DOUBLE)), kv[1])
        val d = doubles("range-d", listOf(1.0, 2.0), 0, dLive, byK)
        assertThat(ParquetRewriter.SortKeys(ParquetRewriter.outputSchema(dLive), byK).footerRange(footer(d))).isNull()
    }

    @Test
    fun `a trusted run with a deletion vector merges, and does not stop a disjoint one appending`() {
        val a = prior("dv-a", (0L until 10L).toList(), 0, byK)
        val b = prior("dv-b", (100L until 110L).toList(), 100, byK)
        val out = fresh("dv-sorted.parquet")
        val result =
            rewrite(listOf(explicit(a, trusted = true, deletes = dv(0, 9)), explicit(b, trusted = true)), out, byK)
        assertThat(result.rowGroupsAppended).isEqualTo(1)
        assertThat(rows(out).map { it.first }).isEqualTo((1L until 9L) + (100L until 110L))
    }
}
