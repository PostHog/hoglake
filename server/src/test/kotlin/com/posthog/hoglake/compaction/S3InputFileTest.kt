package com.posthog.hoglake.compaction

import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.io.EOFException
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.util.Random

/**
 * [S3InputFile] against a real parquet file served out of memory.
 *
 * The point of these is the footer hint: it is writer-supplied for every
 * client-written file, so "a wrong hint still reads correctly" is the
 * property that matters, not "a right hint is fast".
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class S3InputFileTest {
    private val tmp: Path = Files.createTempDirectory("hoglake-s3-input")

    @AfterAll
    fun cleanup() {
        tmp.toFile().deleteRecursively()
    }

    /** An ObjectStore that serves one object from a byte array, counting reads. */
    private class FakeStore(private val bytes: ByteArray) :
        ObjectStore(null, "us-east-1", "k", "s", true) {
        val ranges = mutableListOf<Pair<Long, Int>>()

        override fun get(pathUri: String): ByteArray {
            throw AssertionError("whole-object get must not be used: $pathUri")
        }

        override fun getRange(
            pathUri: String,
            startInclusive: Long,
            length: Int,
        ): ByteArray {
            ranges += startInclusive to length
            val end = minOf(startInclusive + length, bytes.size.toLong()).toInt()
            return bytes.copyOfRange(startInclusive.toInt(), end)
        }
    }

    private val schema: MessageType =
        Types.buildMessage()
            .addField(
                Types.primitive(PrimitiveTypeName.INT64, org.apache.parquet.schema.Type.Repetition.REQUIRED)
                    .id(1).named("id"),
            )
            .addField(
                Types.primitive(PrimitiveTypeName.BINARY, org.apache.parquet.schema.Type.Repetition.REQUIRED)
                    .id(2).named("payload"),
            )
            .named("row")

    /** A parquet file big enough to span several readahead windows. */
    private fun sampleFile(rows: Int = 20_000): Path {
        val path = tmp.resolve("sample-$rows.parquet")
        if (Files.exists(path)) return path
        val factory = SimpleGroupFactory(schema)
        ExampleParquetWriter.builder(LocalOutputFile(path))
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build()
            .use { writer ->
                repeat(rows) { i ->
                    writer.write(
                        factory.newGroup()
                            .append("id", i.toLong())
                            .append("payload", Binary.fromString("value-$i-${"x".repeat(64)}")),
                    )
                }
            }
        return path
    }

    private fun footerSizeOf(path: Path): Long {
        val bytes = Files.readAllBytes(path)
        return java.nio.ByteBuffer.wrap(bytes, bytes.size - 8, 4)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN).int.toLong()
    }

    private fun rowCount(file: org.apache.parquet.io.InputFile): Long =
        ParquetFileReader.open(file).use { r -> r.footer.blocks.sumOf { it.rowCount } }

    @Test
    fun `reads the same footer and row count as a local file`() {
        val path = sampleFile()
        val bytes = Files.readAllBytes(path)
        val store = FakeStore(bytes)
        val s3 = S3InputFile(store, "s3://b/k.parquet", bytes.size.toLong(), footerSizeOf(path))

        assertThat(s3.length).isEqualTo(bytes.size.toLong())
        assertThat(rowCount(s3)).isEqualTo(rowCount(LocalInputFile(path)))
        ParquetFileReader.open(s3).use { fromS3 ->
            ParquetFileReader.open(LocalInputFile(path)).use { local ->
                assertThat(fromS3.footer.fileMetaData.schema)
                    .isEqualTo(local.footer.fileMetaData.schema)
            }
        }
    }

    @Test
    fun `the sortedness pre-pass's key source fetches the key column, not the file`() {
        // The pre-pass profiles headers, then reads `id`'s column data.
        // Each pass can over-read by one source buffer.
        // The merge's 8 MiB buffer covers the payload too.
        // The key buffer keeps both passes below the payload's size.
        val path = sampleFile()
        val bytes = Files.readAllBytes(path)
        val (idChunk, payload) =
            ParquetFileReader.open(LocalInputFile(path)).use { r ->
                val cols = r.footer.blocks.flatMap { it.columns }
                cols.filter { it.path.toDotString() == "id" }.sumOf { it.totalSize } to
                    cols.filter { it.path.toDotString() == "payload" }.sumOf { it.totalSize }
            }

        fun fetchedWith(readahead: Int): Long {
            val store = FakeStore(bytes)
            val s3 = S3InputFile(store, "s3://b/k.parquet", bytes.size.toLong(), footerSizeOf(path), readahead)
            val sorted =
                ParquetFileReader.open(s3).use { reader ->
                    SortednessCheck.isSorted(
                        ParquetRewriter.Input(s3, "k", 0),
                        reader,
                        listOf(
                            com.posthog.hoglake.model.SortFieldDef(
                                1,
                                com.posthog.hoglake.model.SortDirection.ASC,
                                com.posthog.hoglake.model.NullOrder.NULLS_LAST,
                            ),
                        ),
                    )
                }
            assertThat(sorted).isTrue()
            // Everything but the footer prefetch.
            return store.ranges.drop(1).sumOf { it.second.toLong() }
        }
        assertThat(fetchedWith(S3InputFile.DEFAULT_READAHEAD_BYTES))
            .describedAs("the merge's readahead over-reads into the payload")
            .isGreaterThan(idChunk + payload / 2)
        val keyBytes = fetchedWith(S3InputFile.KEY_COLUMN_READAHEAD_BYTES)
        assertThat(keyBytes).isLessThanOrEqualTo(2 * (idChunk + S3InputFile.KEY_COLUMN_READAHEAD_BYTES))
        assertThat(keyBytes).isLessThan(payload)
    }

    @Test
    fun `a fixed near-target dictionary input shares reader excess without spilling`() {
        val rows = 60_000
        val target = 8L * 1024 * 1024
        val nearSchema =
            Types.buildMessage()
                .addField(Types.required(PrimitiveTypeName.INT64).id(1).named("id"))
                .addField(
                    Types.required(PrimitiveTypeName.BINARY).`as`(LogicalTypeAnnotation.stringType())
                        .id(2).named("category"),
                )
                .addField(Types.required(PrimitiveTypeName.BINARY).id(3).named("payload"))
                .named("near_target")

        fun payload(id: Long) = ByteArray(112).also { Random(553L xor (id * -7046029254386353131L)).nextBytes(it) }

        fun category(position: Int) = "c" + (position / 2).toString().padStart(7, '0')
        val sourcePath = tmp.resolve("near-target-input.parquet")
        val factory = SimpleGroupFactory(nearSchema)
        // Default pages and dictionaries exercise the producer's normal decoder state.
        ExampleParquetWriter.builder(LocalOutputFile(sourcePath))
            .withType(nearSchema)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .build().use { writer ->
                repeat(rows) { position ->
                    val id = (rows - position - 1).toLong()
                    writer.write(
                        factory.newGroup()
                            .append("id", id)
                            .append("category", category(position))
                            .append("payload", Binary.fromConstantByteArray(payload(id))),
                    )
                }
            }
        val sourceBytes = Files.readAllBytes(sourcePath)
        assertThat(sourceBytes.size.toLong()).isBetween(target * 85 / 100, target - 1)
        ParquetFileReader.open(LocalInputFile(sourcePath)).use { reader ->
            assertThat(reader.footer.blocks).hasSize(1)
            assertThat(reader.footer.blocks.single().columns[1].hasDictionaryPage()).isTrue()
        }
        val source =
            S3InputFile(
                FakeStore(sourceBytes),
                "s3://b/near-target.parquet",
                sourceBytes.size.toLong(),
                footerSizeOf(sourcePath),
            )
        val live =
            listOf(
                Column(1, 0, ColumnDef("id", ColType.LONG, nullable = false)),
                Column(2, 1, ColumnDef("category", ColType.STRING, nullable = false)),
                Column(3, 2, ColumnDef("payload", ColType.BINARY, nullable = false)),
            )
        val config =
            CompactionConfig(
                sortedHeapBytes = 64L * 1024 * 1024,
                targetBytes = target,
                parallelGroups = 1,
                maxGroupsPerRun = 1,
                spillDir = Files.createDirectory(tmp.resolve("near-target-spills")),
            )
        val readerEvents = ArrayList<Map<String, Long>>()
        val spill = config.sortSpill(live)
        assertThat(spill.readerBudgetBytes).isEqualTo(16L * 1024 * 1024)
        spill.memoryObserver = { event, values -> if (event == "reader") readerEvents += values }
        val out = tmp.resolve("near-target-output.parquet")
        val result =
            ParquetRewriter.rewrite(
                listOf(
                    ParquetRewriter.Input(
                        source,
                        "near-target",
                        100_000,
                        fileSizeBytes = sourceBytes.size.toLong(),
                        survivingRecords = rows.toLong(),
                    ),
                ),
                live,
                listOf(SortFieldDef(1, SortDirection.ASC, NullOrder.NULLS_LAST)),
                LocalDiscardableOutput(out),
                spill = spill,
            )
        assertThat(result.rowsWritten).isEqualTo(rows.toLong())
        assertThat(result.runsSpilled).isZero()
        assertThat(readerEvents).hasSize(1)
        val event = readerEvents.single()
        assertThat(event.getValue("extraBytes")).isPositive()
        assertThat(event.getValue("readerBytes"))
            .isBetween(spill.readerBudgetBytes + 1, spill.readerBudgetBytes + config.sortedHeapBytes)
        var seen = 0L
        ParquetFileReader.open(LocalInputFile(out)).use { reader ->
            val outputSchema = reader.footer.fileMetaData.schema
            val columnIO = ColumnIOFactory().getColumnIO(outputSchema)
            while (true) {
                val pages = reader.readNextRowGroup() ?: break
                pages.use {
                    val records = columnIO.getRecordReader(pages, GroupRecordConverter(outputSchema))
                    repeat(pages.rowCount.toInt()) {
                        val row = records.read()
                        val position = rows - seen.toInt() - 1
                        check(row.getLong(0, 0) == seen)
                        check(row.getString(1, 0) == category(position))
                        check(row.getBinary(2, 0).bytes.contentEquals(payload(seen)))
                        check(row.getLong(3, 0) == 100_000L + position)
                        seen++
                    }
                }
            }
        }
        assertThat(seen).isEqualTo(rows.toLong())
        Files.list(config.spillDir).use { assertThat(it.count()).isZero() }
    }

    @Test
    fun `a data stream does not prefetch the footer, and the append uses one`() {
        val path = sampleFile()
        val bytes = Files.readAllBytes(path)
        val store = FakeStore(bytes)
        val s3 = S3InputFile(store, "s3://b/k.parquet", bytes.size.toLong(), footerSizeOf(path))
        s3.newDataStream().use { assertThat(store.ranges).describedAs("no read before the first byte").isEmpty() }
        s3.newStream().use { assertThat(store.ranges).describedAs("newStream prefetches the tail").hasSize(1) }

        // The byte-level append (package D1): the open reader has the
        // footer already, so appending must not GET the tail again.
        val live =
            listOf(
                com.posthog.hoglake.model.Column(
                    1,
                    0,
                    com.posthog.hoglake.model.ColumnDef("k", com.posthog.hoglake.model.ColType.LONG),
                ),
            )
        val client = tmp.resolve("append-client.parquet")
        val clientSchema =
            Types.buildMessage().addField(
                Types.optional(PrimitiveTypeName.INT64).id(1).named("k"),
            ).named("t")
        ExampleParquetWriter.builder(LocalOutputFile(client)).withType(clientSchema).build().use { w ->
            val f = SimpleGroupFactory(clientSchema)
            repeat(5_000) { w.write(f.newGroup().append("k", it.toLong())) }
        }
        val prior = tmp.resolve("append-prior.parquet")
        rewriteToLocal(listOf(localInput(client, 0)), live, emptyList(), prior)
        val priorBytes = Files.readAllBytes(prior)
        val priorStore = FakeStore(priorBytes)
        val source = S3InputFile(priorStore, "s3://b/prior.parquet", priorBytes.size.toLong(), footerSizeOf(prior))
        ParquetFileReader.open(source).use { reader ->
            val tail = priorStore.ranges.single()
            priorStore.ranges.clear()
            val out = tmp.resolve("append-out.parquet")
            ParquetRewriter.writingTo(
                LocalOutputFile(out),
                ParquetRewriter.outputSchema(live),
                ParquetRewriter.OutputCodec(),
            ) { w -> w.appendRowGroups(source, reader, flushPending = false) }
            assertThat(priorStore.ranges).describedAs("the append re-read the footer tail").doesNotContain(tail)
            assertThat(rowCount(LocalInputFile(out))).isEqualTo(5_000)
        }
    }

    @Test
    fun `a correct footer hint serves the open from one ranged read`() {
        val path = sampleFile()
        val bytes = Files.readAllBytes(path)
        val store = FakeStore(bytes)
        val s3 = S3InputFile(store, "s3://b/k.parquet", bytes.size.toLong(), footerSizeOf(path))

        ParquetFileReader.open(s3).use { it.footer }

        // The prefetch, and nothing else: opening the file costs exactly
        // the one tail read the hint describes.
        assertThat(store.ranges).hasSize(1)
        val (start, len) = store.ranges.single()
        assertThat(start + len).isEqualTo(bytes.size.toLong())
    }

    @Test
    fun `a hint that is null, zero, absurd or simply wrong still reads correctly`() {
        val path = sampleFile()
        val bytes = Files.readAllBytes(path)
        val real = footerSizeOf(path)
        val expected = rowCount(LocalInputFile(path))

        val hints = listOf(null, 0L, -1L, 1L, real - 1, real + 1, bytes.size.toLong() * 2, Long.MAX_VALUE)
        for (hint in hints) {
            val store = FakeStore(bytes)
            val s3 = S3InputFile(store, "s3://b/k.parquet", bytes.size.toLong(), hint)
            assertThat(rowCount(s3)).describedAs("hint=%s", hint).isEqualTo(expected)
        }
    }

    @Test
    fun `an oversized footer hint is ignored rather than pulling the object onto the heap`() {
        // The hazardous band the first version of this test missed: a
        // hint bigger than the readahead but no bigger than the object.
        // Commit-time validation only rejects footer_size >
        // file_size - 8 (FileValidation.kt), so a writer can register a
        // hint of almost the whole object — and an uncapped prefetch
        // would fetch all of it in one heap array, reinstating exactly
        // the whole-object GET this class removes.
        val bytes = ByteArray(4_000_000) { (it % 251).toByte() }
        val cap = 64 * 1024

        for (hint in listOf(cap.toLong() + 1, 1_000_000L, bytes.size.toLong() - 8)) {
            val store = FakeStore(bytes)
            S3InputFile(
                store,
                "s3://b/k",
                bytes.size.toLong(),
                footerSizeHint = hint,
                readaheadBytes = 8 * 1024,
                maxPrefetchBytes = cap,
            ).newStream().use { }
            assertThat(store.ranges)
                .describedAs("hint=%s must not prefetch", hint)
                .isEmpty()
        }

        // And a hint under the cap is still taken.
        val store = FakeStore(bytes)
        S3InputFile(
            store,
            "s3://b/k",
            bytes.size.toLong(),
            footerSizeHint = 1024L,
            readaheadBytes = 8 * 1024,
            maxPrefetchBytes = cap,
        ).newStream().use { }
        assertThat(store.ranges).hasSize(1)
        assertThat(store.ranges.single().second).isEqualTo(1024 + 8)
    }

    @Test
    fun `seeking backwards and forwards reads the right bytes`() {
        val bytes = ByteArray(5000) { (it % 251).toByte() }
        val store = FakeStore(bytes)
        val s3 = S3InputFile(store, "s3://b/k", bytes.size.toLong(), null, readaheadBytes = 512)

        s3.newStream().use { stream ->
            stream.seek(4000)
            val tail = ByteArray(100)
            stream.readFully(tail)
            assertThat(tail).isEqualTo(bytes.copyOfRange(4000, 4100))
            assertThat(stream.pos).isEqualTo(4100L)

            // Backwards, into a region the buffer no longer covers.
            stream.seek(10)
            val head = ByteArray(100)
            stream.readFully(head)
            assertThat(head).isEqualTo(bytes.copyOfRange(10, 110))

            // A ByteBuffer read lands at the same place.
            stream.seek(2048)
            val buf = ByteBuffer.allocate(64)
            stream.readFully(buf)
            assertThat(buf.array()).isEqualTo(bytes.copyOfRange(2048, 2112))
        }
    }

    @Test
    fun `a read spanning several readahead windows returns every byte`() {
        val bytes = ByteArray(10_000) { (it % 97).toByte() }
        val store = FakeStore(bytes)
        val s3 = S3InputFile(store, "s3://b/k", bytes.size.toLong(), null, readaheadBytes = 256)

        s3.newStream().use { stream ->
            val all = ByteArray(bytes.size)
            stream.readFully(all)
            assertThat(all).isEqualTo(bytes)
        }
        // Readahead actually batched: 10000 bytes at 256 a time, not 10000 reads.
        assertThat(store.ranges.size).isLessThan(64)
    }

    @Test
    fun `reading past the end raises EOF rather than returning short`() {
        val bytes = ByteArray(100) { 1 }
        val s3 = S3InputFile(FakeStore(bytes), "s3://b/k", bytes.size.toLong(), null)

        s3.newStream().use { stream ->
            stream.seek(90)
            assertThatThrownBy { stream.readFully(ByteArray(50)) }
                .isInstanceOf(EOFException::class.java)
                .hasMessageContaining("s3://b/k")
        }
        s3.newStream().use { stream ->
            stream.seek(bytes.size.toLong())
            assertThat(stream.read()).isEqualTo(-1)
        }
    }
}
