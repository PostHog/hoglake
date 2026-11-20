package com.posthog.hoglake.compaction

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CodecPool
import org.apache.parquet.bytes.ByteBufferAllocator
import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.bytes.CapacityByteArrayOutputStream
import org.apache.parquet.hadoop.CodecFactory
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import java.io.OutputStream

/**
 * Compresses into guarded slabs without changing the codec stream.
 * The callback replaces any external checksum reservation with zero.
 * Parquet charges checksum copies through the guarded allocator.
 */
internal class GuardedCodecFactory(
    config: Configuration,
    pageSize: Int,
    private val allocator: ByteBufferAllocator,
    private val replaceChecksumReservation: (Long) -> Unit,
) : CodecFactory(config, pageSize) {
    private val slabPageSize = pageSize

    override fun createCompressor(codecName: CompressionCodecName): BytesCompressor {
        val codec = getCodec(codecName)
        val compressor = codec?.let { CodecPool.getCompressor(it) }
        val output = CapacityByteArrayOutputStream.withTargetNumSlabs(64, slabPageSize, 10, allocator)
        // Codec close must not release the returned compressed slabs.
        val destination =
            object : OutputStream() {
                override fun write(value: Int) = output.write(value)

                override fun write(
                    bytes: ByteArray,
                    offset: Int,
                    length: Int,
                ) = output.write(bytes, offset, length)
            }
        return object : BytesCompressor() {
            private var released = false

            override fun compress(bytes: BytesInput): BytesInput {
                check(!released) { "the compressor is released" }
                replaceChecksumReservation(0)
                output.reset()
                compressor?.reset()
                try {
                    val result =
                        if (codec == null) {
                            bytes
                        } else {
                            codec.createOutputStream(destination, compressor).use { stream ->
                                bytes.writeAllTo(stream)
                                stream.finish()
                            }
                            BytesInput.from(output)
                        }
                    // The page store charges checksum copies through the guarded allocator.
                    return result
                } catch (error: Throwable) {
                    runCatching { replaceChecksumReservation(0) }
                    output.reset()
                    throw error
                }
            }

            override fun getCodecName(): CompressionCodecName = codecName

            override fun release() {
                if (released) return
                released = true
                try {
                    replaceChecksumReservation(0)
                } finally {
                    try {
                        output.close()
                    } finally {
                        if (compressor != null) CodecPool.returnCompressor(compressor)
                    }
                }
            }
        }
    }
}
