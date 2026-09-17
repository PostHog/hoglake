package com.posthog.hoglake.compaction

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * Byte-level contract of the puffin `deletion-vector-v1` reader: the
 * writer half lives in [PuffinTestFiles] and mirrors the Iceberg v3
 * layout exactly, so the roundtrip pins both directions. Corruption
 * must fail loudly — a mis-decoded DV silently loses or resurrects
 * deletes.
 */
class PuffinDeletionVectorTest {
    @Test
    fun `roundtrips positions including bucket boundaries`() {
        val positions = listOf(0L, 1L, 3L, 4_000_000_000L, (1L shl 32) + 7, (5L shl 32))
        val dv = PuffinDeletionVector.read(PuffinTestFiles.deletionVector(positions))
        assertThat(dv.cardinality).isEqualTo(positions.size.toLong())
        for (p in positions) assertThat(dv.contains(p)).describedAs("position $p").isTrue()
        for (p in listOf(2L, 5L, 4_000_000_001L, (1L shl 32) + 8, (5L shl 32) + 1, 1L shl 33)) {
            assertThat(dv.contains(p)).describedAs("position $p").isFalse()
        }
    }

    @Test
    fun `empty vector decodes to zero cardinality`() {
        val dv = PuffinDeletionVector.read(PuffinTestFiles.deletionVector(emptyList()))
        assertThat(dv.cardinality).isZero()
        assertThat(dv.contains(0)).isFalse()
    }

    @Test
    fun `a hostile roaring container count is a typed refusal, not a raw throw`() {
        // #83, from the nightly fuzzer. Every length field this reader
        // owns passes on this input — footer payload size, blob offset
        // and length, the blob's own length prefix, the bucket count,
        // even the CRC. The bad field belongs to the roaring bitmap's
        // serialized format (container count -50331647, at file offset
        // 28) and is read inside the library, which threw
        // NegativeArraySizeException straight through compaction's DV
        // path. Pinned on the exact bytes the fuzzer produced.
        val crafted =
            checkNotNull(
                javaClass.classLoader.getResourceAsStream(
                    "com/posthog/hoglake/fuzz/PuffinDeletionVectorFuzzTestInputs/" +
                        "readRefusesLoudlyOrDecodesDeterministically/crash-1c1d87ae",
                ),
            ) { "the #83 fuzz corpus entry is missing" }.use { it.readBytes() }

        assertThatThrownBy { PuffinDeletionVector.read(crafted) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("corrupt deletion-vector-v1 roaring bitmap")
            .hasRootCauseInstanceOf(NegativeArraySizeException::class.java)
    }

    @Test
    fun `a corrupted vector byte fails the CRC check`() {
        val bytes = PuffinTestFiles.deletionVector(listOf(1L, 2L, 3L))
        // Flip a byte inside the blob's vector region (offset 4 = length
        // prefix, +4 magic; +12 lands in the serialized bitmap).
        bytes[16] = (bytes[16].toInt() xor 0x40).toByte()
        assertThatThrownBy { PuffinDeletionVector.read(bytes) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("CRC")
    }

    @Test
    fun `bad header magic is refused`() {
        val bytes = PuffinTestFiles.deletionVector(listOf(1L))
        bytes[0] = 'X'.code.toByte()
        assertThatThrownBy { PuffinDeletionVector.read(bytes) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("magic")
    }

    @Test
    fun `a compressed footer flag is refused, not mis-decoded`() {
        val bytes = PuffinTestFiles.deletionVector(listOf(1L))
        bytes[bytes.size - 8] = 1 // flags byte 0 bit 0 = payload compressed
        assertThatThrownBy { PuffinDeletionVector.read(bytes) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("compressed")
    }
}
