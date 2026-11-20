package com.posthog.hoglake.compaction

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.bytes.ByteBufferAllocator
import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.hadoop.CodecFactory
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.util.IdentityHashMap
import java.util.Random

class GuardedCodecFactoryTest {
    private class AllocationLimit : RuntimeException()

    private class Allocator(private val limit: Long = Long.MAX_VALUE) : ByteBufferAllocator {
        private val sizes = IdentityHashMap<ByteBuffer, Int>()
        var live = 0L
            private set
        var peak = 0L
            private set

        override fun allocate(size: Int): ByteBuffer {
            if (size > limit - live) throw AllocationLimit()
            return ByteBuffer.allocate(size).also {
                sizes[it] = size
                live += size
                peak = maxOf(peak, live)
            }
        }

        override fun release(buffer: ByteBuffer) {
            live -= checkNotNull(sizes.remove(buffer))
        }

        override fun isDirect(): Boolean = false
    }

    @Test
    fun `guarded compression preserves the encoded bytes`() {
        val random = ByteArray(24 shl 20).also { Random(553).nextBytes(it) }
        val compressible = ByteArray(random.size) { 97 }
        for (codec in listOf(
            CompressionCodecName.SNAPPY,
            CompressionCodecName.ZSTD,
            CompressionCodecName.UNCOMPRESSED,
        )) {
            val allocator = Allocator()
            val reservations = ArrayList<Long>()
            val config = Configuration(false)
            val stock = CodecFactory(config, 64 shl 10)
            val guarded = GuardedCodecFactory(config, 64 shl 10, allocator, reservations::add)
            try {
                val original = stock.getCompressor(codec)
                val replacement = guarded.getCompressor(codec)
                for (payload in listOf(byteArrayOf(1), random, random, compressible, byteArrayOf(2))) {
                    val expected = original.compress(BytesInput.from(payload)).toByteArray()
                    val actual = replacement.compress(BytesInput.from(payload)).toByteArray()
                    assertThat(actual).isEqualTo(expected)
                    assertThat(reservations).containsOnly(0L)
                }
            } finally {
                guarded.release()
                stock.release()
            }
            assertThat(reservations.last()).isZero()
            assertThat(allocator.live).isZero()
            if (codec != CompressionCodecName.UNCOMPRESSED) assertThat(allocator.peak).isPositive()
        }
    }

    @Test
    fun `a refused slab releases the partial compressed output`() {
        val allocator = Allocator(1 shl 20)
        val reservations = ArrayList<Long>()
        val guarded = GuardedCodecFactory(Configuration(false), 64 shl 10, allocator, reservations::add)
        try {
            val payload = ByteArray(24 shl 20).also { Random(554).nextBytes(it) }
            assertThatThrownBy {
                guarded.getCompressor(CompressionCodecName.SNAPPY).compress(BytesInput.from(payload))
            }.isInstanceOf(AllocationLimit::class.java)
            assertThat(allocator.live).isZero()
            assertThat(reservations.last()).isZero()
        } finally {
            guarded.release()
        }
    }
}
