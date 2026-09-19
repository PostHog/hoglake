package com.posthog.hoglake.compaction

import com.posthog.hoglake.hydrator.ObjectStore
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.SeekableInputStream
import java.io.EOFException
import java.nio.ByteBuffer
import kotlin.math.max
import kotlin.math.min

/**
 * A parquet [InputFile] that reads an object-store object in place,
 * instead of staging the whole thing to local disk first.
 *
 * What this replaces: compaction used to do
 * `Files.write(local, store.get(path))` per input — a full-object GET
 * into one heap `ByteArray`, written out to `java.io.tmpdir`, and read
 * back by the parquet reader. Three passes over every input byte before
 * the rewrite starts, an uncapped `/tmp` footprint that scales with the
 * group (and an emptyDir `sizeLimit` overrun EVICTS the pod rather than
 * raising), and the int-indexed 2 GiB array ceiling applied to every
 * input.
 *
 * ## Length comes from the catalog
 *
 * `getLength()` is answered from `hog_data_file.file_size_bytes` rather
 * than a HEAD. The catalog is the registry of these objects; if that
 * column disagreed with the store, compaction has a much larger problem
 * than one round trip.
 *
 * ## footer_size is a HINT, never a contract
 *
 * Opening a parquet file costs two round trips before any data: read the
 * last 8 bytes to learn the footer's length, then read the footer. The
 * catalog already carries `footer_size` (the hydrator's ranged tail read
 * runs on it), so the stream PRE-FETCHES the tail that covers both, and
 * those two reads are served from memory.
 *
 * Deliberately a hint: if the value is null, zero, absurd, or simply
 * wrong, the prefetch is skipped or lands short and the ordinary ranged
 * read path answers instead. Nothing here trusts it for correctness —
 * it only decides whether the first two reads cost a round trip. That
 * matters because the value is writer-supplied for every client-written
 * file, and `StatsSanity`'s posture elsewhere in this codebase is that
 * writer-supplied numbers get sanitized, not believed.
 *
 * ## Buffering
 *
 * Reads are buffered with readahead, NOT issued per requested range. A
 * naive implementation that turned every parquet read into its own GET
 * would trade the copies this class removes for per-request latency, and
 * S3 punishes small sequential requests far more than it punishes one
 * larger one. Compaction reads every column of every input, so the
 * access pattern is close to sequential and readahead is nearly always
 * the right guess.
 */
class S3InputFile(
    private val store: ObjectStore,
    private val path: String,
    private val length: Long,
    private val footerSizeHint: Long? = null,
    private val readaheadBytes: Int = DEFAULT_READAHEAD_BYTES,
    private val maxPrefetchBytes: Int = DEFAULT_MAX_PREFETCH_BYTES,
) : InputFile {
    init {
        require(length >= 0) { "negative length $length for $path" }
        require(readaheadBytes > 0) { "readahead must be positive, got $readaheadBytes" }
    }

    override fun getLength(): Long = length

    override fun newStream(): SeekableInputStream =
        S3SeekableInputStream(store, path, length, footerSizeHint, readaheadBytes, maxPrefetchBytes)

    /**
     * The object URI: parquet-java and this package's error messages
     * interpolate the InputFile, and "s3://bucket/key" is the only
     * identity worth printing.
     */
    override fun toString(): String = path

    companion object {
        /**
         * 8 MiB. Large enough that a column chunk is usually one read,
         * small enough that a seek backwards (parquet's footer-then-data
         * pattern) does not throw away much.
         */
        const val DEFAULT_READAHEAD_BYTES: Int = 8 * 1024 * 1024

        /**
         * Ceiling on the footer prefetch. 64 MiB: real footers get large
         * — a 200-column file with hundreds of row groups runs to single
         * -digit MB — but they do not approach this, so a hint that asks
         * for more is wrong rather than unusual.
         *
         * The cap is the difference between trusting the hint and merely
         * taking it. `footer_size` is writer-supplied and commit-time
         * validation only rejects `footer > file_size - 8`
         * (FileValidation.kt), so without a ceiling a hint of
         * `file_size - 8` pulls the ENTIRE object into one heap array —
         * reinstating both the whole-object fetch this class exists to
         * remove and the int-indexed 2 GiB limit. The hydrator caps the
         * same risk with HOGLAKE_HYDRATOR_MAX_WHOLE_OBJECT_BYTES.
         */
        const val DEFAULT_MAX_PREFETCH_BYTES: Int = 64 * 1024 * 1024

        /** Parquet's trailer: 4 bytes of footer length, then "PAR1". */
        const val FOOTER_SUFFIX_BYTES: Int = 8
    }
}

private class S3SeekableInputStream(
    private val store: ObjectStore,
    private val path: String,
    private val length: Long,
    footerSizeHint: Long?,
    private val readaheadBytes: Int,
    maxPrefetchBytes: Int,
) : SeekableInputStream() {
    private var pos: Long = 0

    /** Buffer contents cover `[bufferStart, bufferStart + buffer.size)`. */
    private var buffer: ByteArray = EMPTY
    private var bufferStart: Long = 0

    init {
        prefetchFooter(footerSizeHint, maxPrefetchBytes)
    }

    /**
     * Prime the buffer with the tail that parquet is about to read
     * twice. Best effort by construction: any reason to doubt the hint
     * simply leaves the buffer empty.
     */
    private fun prefetchFooter(
        hint: Long?,
        maxPrefetchBytes: Int,
    ) {
        if (hint == null || hint <= 0) return
        val tailBytes = hint + S3InputFile.FOOTER_SUFFIX_BYTES
        // An out-of-range hint is a wrong hint, not an error to raise —
        // and "larger than the cap" is out of range for the same reason
        // "larger than the object" is. Skipping costs one round trip;
        // believing it costs the whole object on the heap.
        if (tailBytes <= 0 || tailBytes > length || tailBytes > maxPrefetchBytes) return
        val start = length - tailBytes
        runCatching { store.getRange(path, start, tailBytes.toInt()) }
            .onSuccess {
                buffer = it
                bufferStart = start
            }
    }

    override fun getPos(): Long = pos

    override fun seek(newPos: Long) {
        require(newPos >= 0) { "negative seek to $newPos in $path" }
        pos = newPos
    }

    /** Bytes of [buffer] usable at [pos], or 0 when it does not cover it. */
    private fun buffered(): Int {
        val offset = pos - bufferStart
        if (offset < 0 || offset >= buffer.size) return 0
        return (buffer.size - offset).toInt()
    }

    /**
     * Ensure at least one byte at [pos] is buffered, reading at least
     * [need] bytes when it has to go to the store. Returns false at EOF.
     */
    private fun fill(need: Int): Boolean {
        if (buffered() > 0) return true
        if (pos >= length) return false
        val want = min(max(need.toLong(), readaheadBytes.toLong()), length - pos)
        buffer = store.getRange(path, pos, want.toInt())
        bufferStart = pos
        return buffer.isNotEmpty()
    }

    private fun copyOut(
        dest: ByteArray,
        offset: Int,
        len: Int,
    ): Int {
        if (len == 0) return 0
        if (!fill(len)) return -1
        val available = buffered()
        val n = min(available, len)
        val from = (pos - bufferStart).toInt()
        System.arraycopy(buffer, from, dest, offset, n)
        pos += n
        return n
    }

    override fun read(): Int {
        val one = ByteArray(1)
        return if (copyOut(one, 0, 1) == 1) one[0].toInt() and 0xff else -1
    }

    override fun read(
        b: ByteArray,
        off: Int,
        len: Int,
    ): Int = copyOut(b, off, len)

    override fun readFully(bytes: ByteArray) = readFully(bytes, 0, bytes.size)

    override fun readFully(
        bytes: ByteArray,
        start: Int,
        len: Int,
    ) {
        var done = 0
        while (done < len) {
            val n = copyOut(bytes, start + done, len - done)
            if (n < 0) {
                throw EOFException("reached end of $path after $done of $len bytes at $pos")
            }
            done += n
        }
    }

    override fun read(buf: ByteBuffer): Int {
        if (!buf.hasRemaining()) return 0
        if (!fill(buf.remaining())) return -1
        val n = min(buffered(), buf.remaining())
        buf.put(buffer, (pos - bufferStart).toInt(), n)
        pos += n
        return n
    }

    override fun readFully(buf: ByteBuffer) {
        while (buf.hasRemaining()) {
            if (read(buf) < 0) {
                throw EOFException(
                    "reached end of $path with ${buf.remaining()} bytes still wanted at $pos",
                )
            }
        }
    }

    /**
     * Nothing to release: the buffer is plain heap and the store owns
     * its client. Close exists so the reader's `use {}` is honest.
     */
    override fun close() {
        buffer = EMPTY
    }

    private companion object {
        val EMPTY = ByteArray(0)
    }
}
