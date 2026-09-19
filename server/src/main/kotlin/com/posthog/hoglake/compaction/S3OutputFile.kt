package com.posthog.hoglake.compaction

import com.posthog.hoglake.hydrator.ObjectStore
import org.apache.parquet.io.OutputFile
import org.apache.parquet.io.PositionOutputStream
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlin.math.min

/**
 * A parquet [OutputFile] that streams straight into an object-store
 * multipart upload, with no local file.
 *
 * S3 has no append, so multipart IS its streaming write: buffer a part,
 * upload it, repeat, complete at the end. That is exactly the shape of a
 * parquet writer, which emits bytes forward and only knows the total
 * when it closes — [PositionOutputStream.getPos] is a running count, and
 * parquet never seeks backwards on output.
 *
 * What this replaces: compaction wrote the rewrite to `java.io.tmpdir`,
 * read the whole file back once to find the footer length, then read it
 * back AGAIN into a heap `ByteArray` for a single-part `putObject`. Two
 * full passes over every byte written, a local file the size of the
 * compaction target, and the int-indexed 2 GiB array ceiling. None of
 * that exists here; the peak is one part buffer.
 *
 * ## Completion is gated on the parquet trailer
 *
 * A stream that is closed after a FAILED write must not be completed:
 * multipart completion is atomic and would publish a truncated object
 * that looks whole. The caller cannot help — parquet's writer is closed
 * by `use {}` on the exception path too, so `close()` runs either way and
 * cannot tell why.
 *
 * So the stream checks the bytes it actually received. A parquet file
 * ends with the footer length and the ASCII magic `PAR1`; if the last
 * four bytes are not that, the write did not finish, and [close] aborts
 * the upload and raises instead of completing. Self-checking, and it
 * needs no cooperation from the caller.
 *
 * The same eight bytes yield `footer_size` ([footerSize]), so the
 * catalog's value comes from the write rather than from reading the
 * object back.
 */
class S3OutputFile(
    private val store: ObjectStore,
    private val path: String,
    private val partSizeBytes: Int = DEFAULT_PART_SIZE_BYTES,
) : OutputFile {
    init {
        require(partSizeBytes >= MIN_PART_SIZE_BYTES) {
            "part size $partSizeBytes is below S3's $MIN_PART_SIZE_BYTES minimum for $path"
        }
    }

    private var stream: S3MultipartOutputStream? = null

    override fun create(blockSizeHint: Long): PositionOutputStream {
        check(stream == null) { "output stream for $path was already created" }
        return S3MultipartOutputStream(store, path, partSizeBytes).also { stream = it }
    }

    /**
     * Same as [create]. S3 has no "does it exist" step worth paying for
     * here: compaction writes to a freshly minted uuid path that the
     * removal ledger has already staged, so there is nothing to clobber.
     */
    override fun createOrOverwrite(blockSizeHint: Long): PositionOutputStream = create(blockSizeHint)

    override fun supportsBlockSize(): Boolean = false

    override fun defaultBlockSize(): Long = 0

    override fun getPath(): String = path

    /** Total bytes uploaded. Valid once the stream has closed. */
    val bytesWritten: Long
        get() = requireClosed().written

    /** Thrift footer length, taken from the trailer the writer emitted. */
    val footerSize: Long
        get() = requireClosed().footerSize

    private fun requireClosed(): S3MultipartOutputStream {
        val s = stream ?: error("no output stream was created for $path")
        check(s.isClosed) { "output stream for $path has not been closed" }
        return s
    }

    override fun toString(): String = path

    companion object {
        /**
         * 16 MiB. Above S3's 5 MiB floor with room to spare, and at the
         * 512 MiB compaction target it makes ~32 parts — nowhere near
         * the 10,000-part limit, which this size puts at 156 GiB.
         */
        const val DEFAULT_PART_SIZE_BYTES: Int = 16 * 1024 * 1024

        /** S3's minimum for every part except the last. */
        const val MIN_PART_SIZE_BYTES: Int = 5 * 1024 * 1024
    }
}

private class S3MultipartOutputStream(
    private val store: ObjectStore,
    private val path: String,
    partSizeBytes: Int,
) : PositionOutputStream() {
    private val buffer = ByteArray(partSizeBytes)
    private var buffered = 0
    private val etags = mutableListOf<String>()
    private val uploadId: String = store.startMultipartUpload(path)

    /** The last 8 bytes written: parquet's footer length plus `PAR1`. */
    private val trailer = ByteArray(TRAILER_BYTES)
    private var trailerLen = 0

    var written: Long = 0
        private set
    var isClosed: Boolean = false
        private set

    /** A part upload failed; the object can never be completed. */
    private var failed = false
    var footerSize: Long = 0
        private set

    override fun getPos(): Long = written

    override fun write(b: Int) {
        single[0] = b.toByte()
        write(single, 0, 1)
    }

    override fun write(
        b: ByteArray,
        off: Int,
        len: Int,
    ) {
        check(!isClosed) { "write to a closed stream for $path" }
        check(!failed) { "write to a stream for $path whose part upload already failed" }
        var done = 0
        while (done < len) {
            val n = min(buffer.size - buffered, len - done)
            System.arraycopy(b, off + done, buffer, buffered, n)
            buffered += n
            done += n
            if (buffered == buffer.size) flushPart()
        }
        rememberTrailer(b, off, len)
        written += len
    }

    /** Keep the last [TRAILER_BYTES] bytes seen, across any chunking. */
    private fun rememberTrailer(
        b: ByteArray,
        off: Int,
        len: Int,
    ) {
        if (len <= 0) return
        val take = min(TRAILER_BYTES, len)
        val keep = TRAILER_BYTES - take
        if (keep > 0 && trailerLen > 0) {
            val shift = min(keep, trailerLen)
            System.arraycopy(trailer, trailerLen - shift, trailer, 0, shift)
            trailerLen = shift
        } else {
            trailerLen = 0
        }
        System.arraycopy(b, off + len - take, trailer, trailerLen, take)
        trailerLen += take
    }

    private fun flushPart() {
        if (buffered == 0) return
        try {
            etags += store.uploadPart(path, uploadId, etags.size + 1, buffer, buffered)
        } catch (e: Throwable) {
            // Abort HERE, not in close(). Parquet buffers a whole row
            // group and writes it while closing, so a part that fails
            // during that flush throws from inside ParquetWriter.close()
            // and this stream's own close() is never reached — leaving
            // the upload dangling, invisible as an object and still
            // billed. Cleaning up at the point of failure is the only
            // placement that does not depend on who calls us back.
            //
            // The flag additionally refuses completion: a part that did
            // not land leaves a hole no later write can fill, and parquet
            // may still write a perfectly good footer on top of it, which
            // would otherwise satisfy the trailer check below.
            failed = true
            store.abortMultipartUpload(path, uploadId)
            throw e
        }
        buffered = 0
    }

    override fun close() {
        if (isClosed) return
        isClosed = true
        // Already aborted at the point of failure; the original
        // exception is on its way up and a second one here would mask it.
        if (failed) return
        try {
            require(trailerLen == TRAILER_BYTES && trailerIsParquet()) {
                "refusing to complete $path: the stream ended without a parquet trailer " +
                    "after $written bytes, so the rewrite did not finish"
            }
            footerSize =
                ByteBuffer.wrap(trailer, 0, 4).order(ByteOrder.LITTLE_ENDIAN).int.toLong()
            flushPart()
            store.completeMultipartUpload(path, uploadId, etags)
        } catch (e: Throwable) {
            store.abortMultipartUpload(path, uploadId)
            if (e is IllegalArgumentException) throw IOException(e.message, e)
            throw e
        }
    }

    private fun trailerIsParquet(): Boolean =
        trailer[4] == 'P'.code.toByte() &&
            trailer[5] == 'A'.code.toByte() &&
            trailer[6] == 'R'.code.toByte() &&
            trailer[7] == '1'.code.toByte()

    private val single = ByteArray(1)

    private companion object {
        /** 4 bytes of little-endian footer length, then `PAR1`. */
        const val TRAILER_BYTES = 8
    }
}
