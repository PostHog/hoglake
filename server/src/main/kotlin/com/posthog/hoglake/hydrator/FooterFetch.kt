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
 *
 * [maxFooterBytes] picks how far the registration is trusted. Null (the
 * PostgreSQL [Hydrator]) reads as before #675: the tail runs from
 * `file_size_bytes - footer_size - 8` to the object's end, at any
 * `footer_size`, and the whole-object read is the whole object. Set (the
 * Hoglite rehydrate sweep, whose sizes a tenant registers and can rewrite,
 * #668), every read is held to the registration: the tail is a closed
 * range ending at `file_size_bytes`, a `footer_size` over [maxFooterBytes]
 * is not usable (one near `file_size_bytes` would pull the whole object
 * through the tail), and the whole-object read stops one byte past
 * `file_size_bytes` ([ObjectStore.getHead]), so an object longer than its
 * registration is structural instead of landing on the heap whole. An
 * object shorter than its registration (a tail start past its end answers
 * 416) and a `file_size_bytes` under the 8-byte footer suffix are
 * structural too: no retry changes either, and a transient file stays
 * pending and takes a sweep slot on every call.
 */
internal class FooterFetch(
    private val maxWholeObjectBytes: Long,
    private val maxFooterBytes: Long? = null,
) {
    init {
        require(maxFooterBytes == null || maxFooterBytes in 1..MAX_FOOTER_BYTES) {
            "maxFooterBytes=$maxFooterBytes must be in 1..$MAX_FOOTER_BYTES"
        }
    }

    /** What [read] needs of one registered file. */
    data class Target(
        val path: String,
        val fileSizeBytes: Long,
        val footerSize: Long?,
    )

    private val log = KotlinLogging.logger {}

    /**
     * The whole-object cap; held to the registration, also held to what one heap array can take
     * ([ObjectStore.getHead] reads one byte past a size).
     */
    private val maxBufferBytes =
        if (maxFooterBytes == null) maxWholeObjectBytes else minOf(maxWholeObjectBytes, Int.MAX_VALUE - 1L)

    fun read(
        file: Target,
        objects: ObjectStore,
    ): ParquetMetadata {
        // Outside the fetch's classification below, which would call it transient: no read can serve it.
        if (maxFooterBytes != null && file.fileSizeBytes < FOOTER_SUFFIX) {
            throw IllegalStateException(
                "cannot hydrate ${file.path}: file_size_bytes ${file.fileSizeBytes} is under the " +
                    "$FOOTER_SUFFIX-byte parquet footer suffix; re-register it with its real size, then rehydrate",
            )
        }
        // A footer_size over the footer cap is not usable: the whole-object
        // read below, under its own cap, is the bound for such a file.
        val footerSize =
            file.footerSize?.takeIf {
                it > 0 && (maxFooterBytes == null || it <= maxFooterBytes) && it + FOOTER_SUFFIX < file.fileSizeBytes
            }
        if (footerSize != null) {
            try {
                val tailStart = file.fileSizeBytes - footerSize - FOOTER_SUFFIX
                // Fetch failures are classified HERE, not swallowed into the
                // whole-object fallback: a throttled tail GET must surface as
                // transient (stay pending), and a 404 as structural — falling
                // back would turn an S3 blip into a whole-object fetch that
                // can trip the size cap and wrongly fail a good registration.
                val tail =
                    try {
                        if (maxFooterBytes == null) {
                            objects.getTail(file.path, tailStart)
                        } else {
                            objects.getRange(file.path, tailStart, Math.toIntExact(footerSize + FOOTER_SUFFIX))
                        }
                    } catch (e: Exception) {
                        // Held to the registration, a start past the object's end is no throttle.
                        if (maxFooterBytes != null && isUnsatisfiableRange(e)) {
                            throw IllegalStateException(
                                "cannot hydrate ${file.path}: the object is shorter than its registered " +
                                    "file_size_bytes ${file.fileSizeBytes} (its tail at byte $tailStart is out of " +
                                    "range); re-register it with its real size, then rehydrate",
                                e,
                            )
                        }
                        throw classifyFetchFailure(e, file.path)
                    }
                return parseFooter(
                    RegionInputFile(file.fileSizeBytes, tailStart, tail, file.path),
                )
            } catch (e: TransientFetchException) {
                throw e
            } catch (e: Exception) {
                if (isMissingObject(e) || isUnsatisfiableRange(e)) throw e
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
        // is the OOM guard. Over-cap without a tail that parsed is
        // STRUCTURAL: retrying cannot shrink the file. file_size_bytes is
        // the registered size — the only pre-GET signal we have, and,
        // held to the registration, the read below holds the object to it.
        if (file.fileSizeBytes > maxBufferBytes) {
            val tail =
                if (footerSize != null) {
                    "its tail read at footer_size=$footerSize did not parse"
                } else if (maxFooterBytes == null) {
                    "no usable footer_size (footer_size=${file.footerSize})"
                } else {
                    "no usable footer_size (footer_size=${file.footerSize}, footer cap $maxFooterBytes bytes, " +
                        "HOGLAKE_HOGLITE_MAX_FOOTER_BYTES)"
                }
            throw IllegalStateException(
                "cannot hydrate ${file.path}: $tail and file_size_bytes " +
                    "${file.fileSizeBytes} exceeds the whole-object fallback cap " +
                    "$maxWholeObjectBytes bytes (HOGLAKE_HYDRATOR_MAX_WHOLE_OBJECT_BYTES); " +
                    "re-register with a correct footer_size or raise the cap, then rehydrate",
            )
        }
        val bytes =
            try {
                if (maxFooterBytes == null) {
                    objects.get(file.path)
                } else {
                    objects.getHead(file.path, Math.toIntExact(file.fileSizeBytes + 1))
                }
            } catch (e: Exception) {
                throw classifyFetchFailure(e, file.path)
            }
        // A tail that overlapped a shorter object's end did not parse and lands here: shorter is
        // structural as the 416 above is.
        if (maxFooterBytes != null && bytes.size.toLong() != file.fileSizeBytes) {
            throw IllegalStateException(
                "cannot hydrate ${file.path}: the object is " +
                    (if (bytes.size > file.fileSizeBytes) "longer" else "shorter (${bytes.size} bytes)") +
                    " than its registered file_size_bytes ${file.fileSizeBytes}; re-register it with its real " +
                    "size, then rehydrate",
            )
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

    /** A 416 / InvalidRange: a closed range starting at or past the object's end. */
    private fun isUnsatisfiableRange(e: Throwable): Boolean =
        generateSequence(e) { it.cause }.any { it is S3Exception && it.statusCode() == 416 }

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

    companion object {
        /** 4-byte footer length + 4-byte "PAR1" magic at the end of the file. */
        private const val FOOTER_SUFFIX = 8L

        /** The largest footer cap: the tail, footer length and magic included, is one heap array. */
        const val MAX_FOOTER_BYTES: Long = Int.MAX_VALUE - FOOTER_SUFFIX
    }
}
