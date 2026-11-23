package com.posthog.hoglake.hydrator

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.SeekableInputStream
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.S3Exception
import java.io.EOFException
import java.io.IOException
import java.nio.ByteBuffer

/** A retryable object-store failure: the file must STAY pending. */
internal class TransientFetchException(message: String, cause: Throwable) : RuntimeException(message, cause)

/**
 * The hydrator's footer read, shared by both catalog stores ([Hydrator],
 * `HogliteHydrator`): a ranged tail GET when the registration carried a
 * usable `footer_size`, else the whole object up to [maxWholeObjectBytes].
 * A missing object is structural; every other fetch failure is a
 * [TransientFetchException] (see [Hydrator]'s failure taxonomy).
 */
internal class FooterFetch(
    private val maxWholeObjectBytes: Long,
) {
    /** What [read] needs of one registered file. */
    data class Target(
        val path: String,
        val fileSizeBytes: Long,
        val footerSize: Long?,
    )

    private val log = KotlinLogging.logger {}

    fun read(
        file: Target,
        objects: ObjectStore,
    ): ParquetMetadata {
        val footerSize = file.footerSize
        if (footerSize != null && footerSize > 0 && footerSize + FOOTER_SUFFIX < file.fileSizeBytes) {
            try {
                val tailStart = file.fileSizeBytes - footerSize - FOOTER_SUFFIX
                // Fetch failures are classified HERE, not swallowed into the
                // whole-object fallback: a throttled tail GET must surface as
                // transient (stay pending), and a 404 as structural — falling
                // back would turn an S3 blip into a whole-object fetch that
                // can trip the size cap and wrongly fail a good registration.
                val tail =
                    try {
                        objects.getTail(file.path, tailStart)
                    } catch (e: Exception) {
                        throw classifyFetchFailure(e, file.path)
                    }
                return parseFooter(
                    RegionInputFile(file.fileSizeBytes, tailStart, tail, file.path),
                )
            } catch (e: TransientFetchException) {
                throw e
            } catch (e: Exception) {
                if (isMissingObject(e)) throw e
                // The tail PARSED wrong (registered footer_size too small,
                // region misses) — the whole-object fallback is for exactly
                // this case.
                log.debug(e) {
                    "tail read of ${file.path} (footer_size=$footerSize) insufficient; " +
                        "falling back to whole-object GET"
                }
            }
        }
        // The whole-object fallback buffers the object on the heap; the cap
        // is the OOM guard. Over-cap without a usable footer_size is
        // STRUCTURAL: retrying cannot shrink the file. file_size_bytes is
        // the registered size — the only pre-GET signal we have.
        if (file.fileSizeBytes > maxWholeObjectBytes) {
            throw IllegalStateException(
                "cannot hydrate ${file.path}: no usable footer_size and file_size_bytes " +
                    "${file.fileSizeBytes} exceeds the whole-object fallback cap " +
                    "$maxWholeObjectBytes bytes (HOGLAKE_HYDRATOR_MAX_WHOLE_OBJECT_BYTES); " +
                    "re-register with a correct footer_size or raise the cap, then rehydrate",
            )
        }
        val bytes =
            try {
                objects.get(file.path)
            } catch (e: Exception) {
                throw classifyFetchFailure(e, file.path)
            }
        return parseFooter(RegionInputFile(bytes.size.toLong(), 0, bytes, file.path))
    }

    /**
     * Structural iff the object is definitively absent (NoSuchKey / bare
     * 404); everything else the store can throw — throttle, 5xx, auth
     * hiccups, connection resets, timeouts — is transient: retrying is
     * free (the file just stays pending) and correct once the condition
     * clears, whereas a terminal 'failed' would permanently de-stat every
     * file swept through one throttle storm.
     */
    private fun classifyFetchFailure(
        e: Exception,
        path: String,
    ): Exception =
        if (isMissingObject(e)) {
            e
        } else {
            TransientFetchException("transient object-store failure fetching $path", e)
        }

    private fun isMissingObject(e: Throwable): Boolean =
        generateSequence(e) { it.cause }.any {
            it is NoSuchKeyException || (it is S3Exception && it.statusCode() == 404)
        }

    /**
     * [FooterParse] rather than ParquetFileReader directly: it translates
     * everything parquet-java lets escape that is not an IOException into
     * a typed refusal, so a corrupt footer reaches the sweep's structural
     * branch as a refusal and not as a bare NPE (three fuzzer-found
     * escapes so far; see FooterParse's note).
     */
    private fun parseFooter(input: InputFile): ParquetMetadata = FooterParse.parse(input)

    /**
     * A parquet-java [InputFile] over one cached byte region of the
     * object (the footer tail, or the whole object): serves reads that
     * fall entirely inside the region and refuses everything else with
     * [IOException] so the caller can fall back to a whole-object read.
     * The footer parse only touches the tail (footer length + magic,
     * then the thrift footer), so a correct footer_size never leaves
     * the region.
     */
    private class RegionInputFile(
        private val totalLength: Long,
        private val regionStart: Long,
        private val region: ByteArray,
        private val uri: String,
    ) : InputFile {
        override fun getLength(): Long = totalLength

        override fun newStream(): SeekableInputStream = RegionStream()

        private inner class RegionStream : SeekableInputStream() {
            private var pos = 0L

            private fun regionOffset(
                offset: Long,
                len: Int,
            ): Int {
                if (offset < regionStart || offset + len > regionStart + region.size) {
                    throw IOException(
                        "range [$offset, +$len) outside cached region " +
                            "[$regionStart, ${regionStart + region.size}) of $uri",
                    )
                }
                return Math.toIntExact(offset - regionStart)
            }

            override fun getPos(): Long = pos

            override fun seek(newPos: Long) {
                pos = newPos
            }

            override fun read(): Int {
                if (pos >= totalLength) return -1
                val idx = regionOffset(pos, 1)
                pos += 1
                return region[idx].toInt() and 0xFF
            }

            override fun read(
                b: ByteArray,
                off: Int,
                len: Int,
            ): Int {
                if (len == 0) return 0
                if (pos >= totalLength) return -1
                val n = Math.toIntExact(minOf(len.toLong(), totalLength - pos))
                val idx = regionOffset(pos, n)
                System.arraycopy(region, idx, b, off, n)
                pos += n
                return n
            }

            override fun readFully(bytes: ByteArray) = readFully(bytes, 0, bytes.size)

            override fun readFully(
                bytes: ByteArray,
                start: Int,
                len: Int,
            ) {
                if (pos + len > totalLength) throw EOFException("read past end of $uri")
                val idx = regionOffset(pos, len)
                System.arraycopy(region, idx, bytes, start, len)
                pos += len
            }

            override fun read(buf: ByteBuffer): Int {
                val len = buf.remaining()
                if (len == 0) return 0
                if (pos >= totalLength) return -1
                val n = Math.toIntExact(minOf(len.toLong(), totalLength - pos))
                val idx = regionOffset(pos, n)
                buf.put(region, idx, n)
                pos += n
                return n
            }

            override fun readFully(buf: ByteBuffer) {
                val len = buf.remaining()
                if (pos + len > totalLength) throw EOFException("read past end of $uri")
                val idx = regionOffset(pos, len)
                buf.put(region, idx, len)
                pos += len
            }
        }
    }

    private companion object {
        /** 4-byte footer length + 4-byte "PAR1" magic at the end of the file. */
        const val FOOTER_SUFFIX = 8L
    }
}
