package com.posthog.hoglake.compaction

import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path

/**
 * [S3OutputFile] against an in-memory multipart store.
 *
 * The load-bearing test is [a failed write aborts instead of publishing
 * a truncated object]: multipart completion is atomic, so completing
 * after a partial write would publish corruption that looks whole, and
 * `close()` runs on the exception path too.
 */
class S3OutputFileTest {
    private val tmp: Path = Files.createTempDirectory("hoglake-s3-output")

    /** Records the multipart protocol and assembles the finished object. */
    private class FakeStore(
        /** Part number to fail on, or null to accept every part. */
        private val failOnPart: Int? = null,
        /**
         * Nth uploadPart CALL to fail on, counting attempts rather than
         * part numbers. The difference matters: flushPart numbers parts
         * `etags.size + 1`, so a failed part is not appended and a retry
         * re-attempts the same NUMBER — meaning failOnPart cannot tell a
         * stream that refused to continue from one that retried and
         * failed again.
         */
        private val failOnCall: Int? = null,
    ) : ObjectStore(null, "us-east-1", "k", "s", true) {
        var uploadCalls = 0
        val parts = mutableListOf<ByteArray>()
        var started = 0
        var completed: ByteArray? = null
        var aborted = false

        override fun startMultipartUpload(pathUri: String): String {
            started++
            return "upload-$started"
        }

        override fun uploadPart(
            pathUri: String,
            uploadId: String,
            partNumber: Int,
            bytes: ByteArray,
            length: Int,
        ): String {
            uploadCalls++
            if (partNumber == failOnPart) throw IOException("injected part failure")
            if (uploadCalls == failOnCall) throw IOException("injected part failure on call $uploadCalls")
            parts += bytes.copyOfRange(0, length)
            return "etag-$partNumber"
        }

        override fun completeMultipartUpload(
            pathUri: String,
            uploadId: String,
            etags: List<String>,
        ) {
            check(etags.size == parts.size) { "etag/part mismatch" }
            val out = ByteArrayOutputStream()
            parts.forEach(out::write)
            completed = out.toByteArray()
        }

        override fun abortMultipartUpload(
            pathUri: String,
            uploadId: String,
        ) {
            aborted = true
        }

        override fun get(pathUri: String): ByteArray = completed ?: error("nothing completed")

        override fun getRange(
            pathUri: String,
            startInclusive: Long,
            length: Int,
        ): ByteArray {
            val all = completed ?: error("nothing completed")
            val end = minOf(startInclusive + length, all.size.toLong()).toInt()
            return all.copyOfRange(startInclusive.toInt(), end)
        }
    }

    private val schema: MessageType =
        Types.buildMessage()
            .addField(
                Types.primitive(PrimitiveTypeName.INT64, Type.Repetition.REQUIRED).id(1).named("id"),
            )
            .addField(
                Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED).id(2).named("payload"),
            )
            .named("row")

    private fun writeParquet(
        out: S3OutputFile,
        rows: Int,
    ) {
        val factory = SimpleGroupFactory(schema)
        ExampleParquetWriter.builder(out)
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build()
            .use { writer ->
                repeat(rows) { i ->
                    writer.write(
                        factory.newGroup()
                            .append("id", i.toLong())
                            .append("payload", Binary.fromString("v-$i-${"y".repeat(64)}")),
                    )
                }
            }
    }

    @Test
    fun `a streamed parquet file round-trips and reports its own footer size`() {
        val store = FakeStore()
        val out = S3OutputFile(store, "s3://b/out.parquet", partSizeBytes = S3OutputFile.MIN_PART_SIZE_BYTES)

        writeParquet(out, 150_000)

        val bytes = store.completed!!
        assertThat(out.bytesWritten).isEqualTo(bytes.size.toLong())
        assertThat(store.aborted).isFalse()
        assertThat(store.parts.size).isGreaterThan(1)

        // The footer size the stream reported matches the file's trailer.
        val fromTrailer =
            java.nio.ByteBuffer.wrap(bytes, bytes.size - 8, 4)
                .order(java.nio.ByteOrder.LITTLE_ENDIAN).int.toLong()
        assertThat(out.footerSize).isEqualTo(fromTrailer)

        // And the object is readable parquet with every row present.
        val input = S3InputFile(store, "s3://b/out.parquet", bytes.size.toLong(), out.footerSize)
        ParquetFileReader.open(input).use { r ->
            assertThat(r.footer.blocks.sumOf { it.rowCount }).isEqualTo(150_000L)
            assertThat(r.footer.fileMetaData.schema).isEqualTo(schema)
        }
    }

    @Test
    fun `a failed write aborts instead of publishing a truncated object`() {
        val store = FakeStore()
        val out = S3OutputFile(store, "s3://b/out.parquet", partSizeBytes = S3OutputFile.MIN_PART_SIZE_BYTES)

        // Write real bytes, then close without the parquet trailer —
        // exactly what `use {}` does when the rewrite throws mid-file.
        assertThatThrownBy {
            out.create(0).use { stream ->
                stream.write(ByteArray(1024) { 7 })
            }
        }.isInstanceOf(IOException::class.java)
            .hasMessageContaining("did not finish")

        assertThat(store.aborted).isTrue()
        assertThat(store.completed).isNull()
    }

    @Test
    fun `a failure uploading a part aborts the upload`() {
        val store = FakeStore(failOnPart = 2)
        val out = S3OutputFile(store, "s3://b/out.parquet", partSizeBytes = S3OutputFile.MIN_PART_SIZE_BYTES)

        assertThatThrownBy { writeParquet(out, 200_000) }
            .isInstanceOf(IOException::class.java)

        assertThat(store.aborted).isTrue()
        assertThat(store.completed).isNull()
    }

    @Test
    fun `a rewrite that fails mid-file discards the upload instead of publishing it`() {
        // The test the first version of this file did not have, and the
        // reason a real defect shipped: the sibling above fakes a
        // failure by writing raw bytes and closing without a trailer,
        // which is NOT how the rewriter fails. Drive the production
        // entry point and let it break on its own.
        //
        // What makes this sharp: on the exception path parquet's own
        // close() flushes the pending row group and writes a valid
        // footer and PAR1. The trailer gate alone is satisfied by that,
        // so unless the sink is discarded FIRST the upload completes and
        // a well-formed file with a truncated row set is published.
        //
        // This is the S3 twin of ParquetRewriterTest's `assertThat(out)
        // .doesNotExist()`, which is the assertion the local sink has
        // had all along.
        val store = FakeStore()
        val out = S3OutputFile(store, "s3://b/out.parquet", partSizeBytes = S3OutputFile.MIN_PART_SIZE_BYTES)

        // A DECIMAL(10,2)-annotated INT64 read as DECIMAL(10,2): the
        // copy plan accepts it, so rows flow — until one value will not
        // fit the precision, which is a DATA refusal thrown mid-write
        // with 20,000 rows already handed to the writer. That is the
        // shape that matters: a plan-time refusal would never have
        // written anything.
        val src = tmp.resolve("overflow.parquet")
        val schema =
            Types.buildMessage()
                .addField(
                    Types.optional(PrimitiveTypeName.INT64)
                        .`as`(LogicalTypeAnnotation.decimalType(2, 10)).id(1).named("amount"),
                )
                .named("row")
        val factory = SimpleGroupFactory(schema)
        ExampleParquetWriter.builder(LocalOutputFile(src))
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build()
            .use { w ->
                repeat(20_000) { w.write(factory.newGroup().append("amount", 1L)) }
                w.write(factory.newGroup().append("amount", 10_000_000_000L))
            }

        assertThatThrownBy {
            ParquetRewriter.rewrite(
                listOf(localInput(src, 0L)),
                listOf(
                    Column(
                        1,
                        0,
                        ColumnDef("amount", ColType.DECIMAL, mapOf("precision" to 10, "scale" to 2)),
                    ),
                ),
                emptyList(),
                out,
            )
        }.isInstanceOf(InvalidDataException::class.java)

        assertThat(store.completed).describedAs("nothing may be published").isNull()
        assertThat(store.aborted).describedAs("the upload must be discarded").isTrue()
    }

    @Test
    fun `a stream whose part upload failed refuses to continue or complete`() {
        // Pins the `failed` flag, which nothing else does: fail the
        // FIRST upload call and accept everything after, so a stream
        // that merely retried would sail on and complete a file with a
        // hole in it. Only refusing outright produces these assertions.
        val store = FakeStore(failOnCall = 1)
        val out = S3OutputFile(store, "s3://b/k", partSizeBytes = S3OutputFile.MIN_PART_SIZE_BYTES)
        val stream = out.create(0)
        val chunk = ByteArray(1_000_000) { 5 }

        assertThatThrownBy { repeat(8) { stream.write(chunk, 0, chunk.size) } }
            .isInstanceOf(IOException::class.java)
        assertThat(store.aborted).isTrue()

        // Poisoned: no further writes, and close() must not complete.
        assertThatThrownBy { stream.write(chunk, 0, chunk.size) }
            .isInstanceOf(IllegalStateException::class.java)
        stream.close()
        assertThat(store.completed).isNull()

        // And the accessors refuse rather than reporting zeroes.
        assertThatThrownBy { out.bytesWritten }.isInstanceOf(IllegalStateException::class.java)
        assertThatThrownBy { out.footerSize }.isInstanceOf(IllegalStateException::class.java)
    }

    @Test
    fun `position tracks bytes written across part boundaries`() {
        val store = FakeStore()
        val out = S3OutputFile(store, "s3://b/k", partSizeBytes = S3OutputFile.MIN_PART_SIZE_BYTES)
        val stream = out.create(0)

        assertThat(stream.pos).isEqualTo(0L)
        val chunk = ByteArray(1_000_000) { 3 }
        repeat(7) { stream.write(chunk, 0, chunk.size) }
        assertThat(stream.pos).isEqualTo(7_000_000L)
        // 7 MB at a 5 MiB part size means one part already went up.
        assertThat(store.parts).isNotEmpty()
    }

    @Test
    fun `a part size below S3's minimum is refused`() {
        assertThatThrownBy { S3OutputFile(FakeStore(), "s3://b/k", partSizeBytes = 1024) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("below S3's")
    }
}
