package com.posthog.hoglake

import com.posthog.hoglake.compaction.DiscardableOutputFile
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.PositionOutputStream
import org.apache.parquet.io.SeekableInputStream
import java.io.ByteArrayOutputStream
import java.io.EOFException
import java.nio.ByteBuffer

// parquet-java InputFile / DiscardableOutputFile pairs over plain byte
// arrays, for the test and fuzz surfaces.
//
// Production reads its inputs through S3InputFile and the hydrator's
// RegionInputFile, both of which are already byte ranges rather than
// files; nothing on the server ever stages a parquet object to local
// disk. A test that writes one to a temp directory therefore adds a
// filesystem the subject does not have, and pays for it.
//
// These are the same shape as the in-memory wrapper the footer fuzz
// target used to carry privately; it now uses this one, so there is a
// single implementation of the bounds checks to get right.

/** An [InputFile] over [bytes]; reads past the end raise [EOFException]. */
fun memoryInput(bytes: ByteArray): InputFile =
    object : InputFile {
        override fun getLength(): Long = bytes.size.toLong()

        override fun newStream(): SeekableInputStream = MemoryInputStream(bytes)

        override fun toString(): String = "memory:${bytes.size}b"
    }

private class MemoryInputStream(private val bytes: ByteArray) : SeekableInputStream() {
    private var pos = 0L

    override fun getPos(): Long = pos

    override fun seek(newPos: Long) {
        pos = newPos
    }

    override fun read(): Int {
        if (pos < 0 || pos >= bytes.size) return -1
        val v = bytes[pos.toInt()].toInt() and 0xFF
        pos += 1
        return v
    }

    override fun read(
        b: ByteArray,
        off: Int,
        len: Int,
    ): Int {
        if (len == 0) return 0
        if (pos < 0 || pos >= bytes.size) return -1
        val n = minOf(len.toLong(), bytes.size - pos).toInt()
        System.arraycopy(bytes, checkedPos(n), b, off, n)
        pos += n
        return n
    }

    override fun readFully(b: ByteArray) = readFully(b, 0, b.size)

    override fun readFully(
        b: ByteArray,
        start: Int,
        len: Int,
    ) {
        System.arraycopy(bytes, checkedPos(len), b, start, len)
        pos += len
    }

    override fun read(buf: ByteBuffer): Int {
        val len = buf.remaining()
        if (len == 0) return 0
        if (pos < 0 || pos >= bytes.size) return -1
        val n = minOf(len.toLong(), bytes.size - pos).toInt()
        buf.put(bytes, checkedPos(n), n)
        pos += n
        return n
    }

    override fun readFully(buf: ByteBuffer) {
        val len = buf.remaining()
        buf.put(bytes, checkedPos(len), len)
        pos += len
    }

    /**
     * The one bounds check, for every accessor above. A short read is a
     * legal answer to `read`, but never to `readFully` — and the
     * position arithmetic is done in Long so a hostile seek past 2^31
     * cannot wrap into a valid-looking index.
     */
    private fun checkedPos(len: Int): Int {
        if (pos < 0 || len < 0 || pos + len > bytes.size) {
            throw EOFException("range [$pos, +$len) outside a ${bytes.size} byte input")
        }
        return pos.toInt()
    }
}

/**
 * A [DiscardableOutputFile] collecting into memory. [bytes] is what the
 * writer produced; [discard] drops it, the in-memory twin of unlinking
 * a half-written file.
 */
class MemoryOutputFile : DiscardableOutputFile {
    private var sink = ByteArrayOutputStream()

    fun bytes(): ByteArray = sink.toByteArray()

    override fun create(blockSizeHint: Long): PositionOutputStream = createOrOverwrite(blockSizeHint)

    override fun createOrOverwrite(blockSizeHint: Long): PositionOutputStream {
        sink = ByteArrayOutputStream()
        val out = sink
        return object : PositionOutputStream() {
            override fun getPos(): Long = out.size().toLong()

            override fun write(b: Int) = out.write(b)

            override fun write(
                b: ByteArray,
                off: Int,
                len: Int,
            ) = out.write(b, off, len)
        }
    }

    override fun supportsBlockSize(): Boolean = false

    override fun defaultBlockSize(): Long = 0

    override fun getPath(): String = "memory:out"

    override fun discard() {
        sink = ByteArrayOutputStream()
    }

    override fun toString(): String = path
}
