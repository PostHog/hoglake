package com.posthog.hoglake.compaction

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.nio.file.Files
import java.nio.file.Path

/**
 * [OutputWriter] against parquet-java's own writer, on the same rows with
 * the same settings: the same row groups, the same column chunks byte for
 * byte (encodings, pages, page checksums, statistics), the same page
 * indexes, the same `created_by`, the same values.
 *
 * THIS IS THE TEST THAT PAYS ON A PARQUET-JAVA UPGRADE. [OutputWriter] is
 * a port of the library's `InternalParquetRecordWriter` (hoglake#134
 * package D1: the output has to mix appended and encoded row groups, and
 * `ParquetWriter` keeps its file writer private), so the row-group size
 * check, the page store wiring and the properties it builds are copies
 * that the library can move out from under. When this goes red after a
 * bump, re-port the changed part of `InternalParquetRecordWriter` /
 * `ParquetWriter`'s constructor rather than loosening an assertion.
 *
 * The only expected difference is the footer's `writer.model.name`
 * (`hoglake-compaction` against the example model's `example`).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OutputWriterParityTest {
    private val tmp: Path = Files.createTempDirectory("output-writer-parity")

    @AfterAll
    fun tearDown() {
        tmp.toFile().deleteRecursively()
    }

    private val live =
        listOf(
            Column(1, 0, ColumnDef("id", ColType.LONG)),
            Column(2, 1, ColumnDef("name", ColType.STRING)),
            Column(3, 2, ColumnDef("score", ColType.DOUBLE)),
            Column(4, 3, ColumnDef("amount", ColType.DECIMAL, mapOf("precision" to 10, "scale" to 2))),
            Column(
                5,
                4,
                ColumnDef("s", ColType.STRUCT),
                listOf(Column(6, 0, ColumnDef("a", ColType.INT)), Column(7, 1, ColumnDef("b", ColType.STRING))),
            ),
            Column(8, 5, ColumnDef("l", ColType.LIST), listOf(Column(9, 0, ColumnDef("element", ColType.LONG)))),
            Column(
                10,
                6,
                ColumnDef("m", ColType.MAP),
                listOf(
                    Column(11, 0, ColumnDef("key", ColType.STRING, nullable = false)),
                    Column(12, 1, ColumnDef("value", ColType.LONG)),
                ),
            ),
        )

    private val schema = ParquetRewriter.outputSchema(live)

    /** Output-schema rows, some fields null, lists and maps of varying length. */
    private fun rows(n: Int): List<Group> {
        val f = SimpleGroupFactory(schema)
        return (0 until n).map { i ->
            f.newGroup().also { g ->
                g.add(0, i.toLong())
                if (i % 7 != 0) g.add(1, "name-${i % 113}-" + "x".repeat(i % 40))
                if (i % 5 != 0) g.add(2, i * 0.25)
                g.add(
                    3,
                    org.apache.parquet.io.api.Binary.fromConstantByteArray(
                        java.math.BigInteger.valueOf(i * 37L).toByteArray(),
                    ),
                )
                if (i % 3 != 0) {
                    val s = g.addGroup(4)
                    s.add(0, i % 11)
                    if (i % 2 == 0) s.add(1, "b$i")
                }
                if (i % 4 != 0) {
                    val l = g.addGroup(5)
                    repeat(i % 6) { k -> l.addGroup(0).add(0, (i * k).toLong()) }
                }
                if (i % 6 != 0) {
                    val m = g.addGroup(6)
                    repeat(i % 4) { k -> m.addGroup(0).also { it.add(0, "k$k") }.add(1, (i + k).toLong()) }
                }
                g.add(7, 1_000_000L + i)
            }
        }
    }

    private val codec = ParquetRewriter.OutputCodec(CompressionCodecName.SNAPPY)
    private val tuning =
        ParquetRewriter.WriterTuning(
            rowGroupBytes = 64L * 1024,
            pageBytes = 8 * 1024,
            maxRowsPerSizeCheck = 50,
        )

    @Test
    fun `OutputWriter writes what parquet-java's writer writes, row group for row group`() {
        val data = rows(20_000)

        val ours = tmp.resolve("ours.parquet")
        val shape = ParquetRewriter.OutputShape(live, schema, ParquetRewriter.DEFAULT_MAX_NODES_PER_ROW)
        val (_, ourFooter) =
            ParquetRewriter.writingTo(LocalOutputFile(ours), schema, codec, tuning) { w ->
                for (g in data) w.write(ParquetRewriter.Row(g, g.getLong(7, 0), plan = shape.identityPlan))
            }

        val theirs = tmp.resolve("theirs.parquet")
        ExampleParquetWriter.builder(LocalOutputFile(theirs))
            .withType(schema)
            .withCompressionCodec(codec.name)
            .withConf(ParquetRewriter.writerConfiguration(codec))
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .withRowGroupSize(tuning.rowGroupBytes)
            .withPageSize(tuning.pageBytes)
            .withDictionaryPageSize(tuning.pageBytes)
            .withMinRowCountForPageSizeCheck(1)
            .withMaxRowCountForPageSizeCheck(tuning.maxRowsPerSizeCheck)
            .build()
            .use { w -> for (g in data) w.write(g) }

        val a = footer(ours)
        val b = footer(theirs)
        assertThat(ourFooter!!.blocks.size).isEqualTo(a.blocks.size)
        assertThat(a.blocks.size).describedAs("enough rows for several row groups").isGreaterThanOrEqualTo(2)
        assertThat(a.blocks.map { it.rowCount }).isEqualTo(b.blocks.map { it.rowCount })
        assertThat(a.fileMetaData.createdBy).isEqualTo(b.fileMetaData.createdBy)
        assertThat(a.fileMetaData.schema).isEqualTo(b.fileMetaData.schema)
        assertThat(a.fileMetaData.keyValueMetaData - ParquetWriter.OBJECT_MODEL_NAME_PROP)
            .isEqualTo(b.fileMetaData.keyValueMetaData - ParquetWriter.OBJECT_MODEL_NAME_PROP)
        assertThat(a.fileMetaData.keyValueMetaData[ParquetWriter.OBJECT_MODEL_NAME_PROP])
            .isEqualTo(OutputWriter.MODEL_NAME)

        val bytesA = Files.readAllBytes(ours)
        val bytesB = Files.readAllBytes(theirs)
        ParquetFileReader.open(LocalInputFile(ours)).use { ra ->
            ParquetFileReader.open(LocalInputFile(theirs)).use { rb ->
                for ((g, pair) in a.blocks.zip(b.blocks).withIndex()) {
                    val (blockA, blockB) = pair
                    for ((ca, cb) in blockA.columns.zip(blockB.columns)) {
                        val what = "row group $g, ${ca.path}"
                        assertThat(ca.path).isEqualTo(cb.path)
                        assertThat(ca.encodings).describedAs(what).isEqualTo(cb.encodings)
                        assertThat(stats(ca.encodingStats)).describedAs(what).isEqualTo(stats(cb.encodingStats))
                        assertThat(ca.codec).describedAs(what).isEqualTo(cb.codec)
                        assertThat(ca.statistics).describedAs(what).isEqualTo(cb.statistics)
                        // Pages, page headers and their CRCs: identical
                        // bytes or not identical writers.
                        assertThat(bytesA.copyOfRange(ca.startingPos.toInt(), (ca.startingPos + ca.totalSize).toInt()))
                            .describedAs("chunk bytes, %s", what)
                            .isEqualTo(
                                bytesB.copyOfRange(cb.startingPos.toInt(), (cb.startingPos + cb.totalSize).toInt()),
                            )
                        assertThat(ra.readColumnIndex(ca)).describedAs("our column index, %s", what).isNotNull()
                        assertThat(rb.readColumnIndex(cb)).describedAs("their column index, %s", what).isNotNull()
                        assertThat(ra.readOffsetIndex(ca)).describedAs("our offset index, %s", what).isNotNull()
                        assertThat(rb.readOffsetIndex(cb)).describedAs("their offset index, %s", what).isNotNull()
                    }
                }
            }
        }
        assertThat(values(ours)).isEqualTo(values(theirs)).hasSize(data.size)
    }

    /** EncodingStats has no equals: its encodings and their page counts. */
    private fun stats(e: org.apache.parquet.column.EncodingStats?): List<Pair<String, Int>> =
        if (e == null) {
            emptyList()
        } else {
            e.dictionaryEncodings.map { "dict:$it" to e.getNumDictionaryPagesEncodedAs(it) } +
                e.dataEncodings.map { "data:$it" to e.getNumDataPagesEncodedAs(it) }
        }

    private fun footer(path: Path): ParquetMetadata = ParquetFileReader.open(LocalInputFile(path)).use { it.footer }

    private fun values(path: Path): List<String> =
        ParquetFileReader.open(LocalInputFile(path)).use { reader ->
            val s = reader.footer.fileMetaData.schema
            val io = ColumnIOFactory().getColumnIO(s)
            val out = ArrayList<String>()
            var pages = reader.readNextRowGroup()
            while (pages != null) {
                val rr = io.getRecordReader(pages, GroupRecordConverter(s))
                repeat(Math.toIntExact(pages.rowCount)) { out += rr.read().toString() }
                pages = reader.readNextRowGroup()
            }
            out
        }
}
