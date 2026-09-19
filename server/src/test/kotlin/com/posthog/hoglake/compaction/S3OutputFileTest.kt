package com.posthog.hoglake.compaction

import com.posthog.hoglake.hydrator.ObjectStore
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import java.io.IOException

/**
 * [S3OutputFile] against an in-memory multipart store.
 *
 * The load-bearing test is [a failed write aborts instead of publishing
 * a truncated object]: multipart completion is atomic, so completing
 * after a partial write would publish corruption that looks whole, and
 * `close()` runs on the exception path too.
 */
class S3OutputFileTest {
    /** Records the multipart protocol and assembles the finished object. */
    private class FakeStore(
        /** Part number to fail on, or null to accept every part. */
        private val failOnPart: Int? = null,
    ) : ObjectStore(null, "us-east-1", "k", "s", true) {
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
            if (partNumber == failOnPart) throw IOException("injected part failure")
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
