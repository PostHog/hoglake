package com.posthog.hoglake.stats

import com.posthog.hoglake.model.ColType
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.UUID

/**
 * Exhaustive byte-pattern tests for the Iceberg single-value codec —
 * every ColType, with known encodings from the Iceberg spec's
 * "Binary single-value serialization" section.
 */
class IcebergSingleValueTest {
    private fun bytes(vararg b: Int): ByteArray = ByteArray(b.size) { b[it].toByte() }

    @Nested
    inner class Booleans {
        @Test
        fun `false is 0x00`() {
            assertThat(IcebergSingleValue.encodeBoolean(false)).isEqualTo(bytes(0x00))
            assertThat(IcebergSingleValue.encode(ColType.BOOLEAN, false)).isEqualTo(bytes(0x00))
        }

        @Test
        fun `true is 0x01`() {
            assertThat(IcebergSingleValue.encodeBoolean(true)).isEqualTo(bytes(0x01))
            assertThat(IcebergSingleValue.encode(ColType.BOOLEAN, true)).isEqualTo(bytes(0x01))
        }
    }

    @Nested
    inner class Ints {
        @Test
        fun `1 is little-endian 01 00 00 00`() {
            assertThat(IcebergSingleValue.encode(ColType.INT, 1)).isEqualTo(bytes(0x01, 0, 0, 0))
        }

        @Test
        fun `-1 is all FF`() {
            assertThat(IcebergSingleValue.encodeInt(-1)).isEqualTo(bytes(0xFF, 0xFF, 0xFF, 0xFF))
        }

        @Test
        fun `0x12345678 lays out LSB first`() {
            assertThat(IcebergSingleValue.encodeInt(0x12345678))
                .isEqualTo(bytes(0x78, 0x56, 0x34, 0x12))
        }

        @Test
        fun extremes() {
            assertThat(IcebergSingleValue.encodeInt(Int.MAX_VALUE))
                .isEqualTo(bytes(0xFF, 0xFF, 0xFF, 0x7F))
            assertThat(IcebergSingleValue.encodeInt(Int.MIN_VALUE))
                .isEqualTo(bytes(0x00, 0x00, 0x00, 0x80))
        }
    }

    @Nested
    inner class Longs {
        @Test
        fun `1 is little-endian 8 bytes`() {
            assertThat(IcebergSingleValue.encode(ColType.LONG, 1L))
                .isEqualTo(bytes(0x01, 0, 0, 0, 0, 0, 0, 0))
        }

        @Test
        fun `-1 is all FF`() {
            assertThat(IcebergSingleValue.encodeLong(-1L)).isEqualTo(ByteArray(8) { 0xFF.toByte() })
        }

        @Test
        fun `0x0102030405060708 lays out LSB first`() {
            assertThat(IcebergSingleValue.encodeLong(0x0102030405060708L))
                .isEqualTo(bytes(0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01))
        }

        @Test
        fun `Int is widened for LONG columns`() {
            assertThat(IcebergSingleValue.encode(ColType.LONG, 7))
                .isEqualTo(IcebergSingleValue.encodeLong(7L))
        }
    }

    @Nested
    inner class Floats {
        @Test
        fun `1_0f has bit pattern 3F800000 LE`() {
            assertThat(IcebergSingleValue.encode(ColType.FLOAT, 1.0f))
                .isEqualTo(bytes(0x00, 0x00, 0x80, 0x3F))
        }

        @Test
        fun `-2_5f round-trips through IEEE754 bits`() {
            val enc = IcebergSingleValue.encodeFloat(-2.5f)
            val back = ByteBuffer.wrap(enc).order(java.nio.ByteOrder.LITTLE_ENDIAN).float
            assertThat(back).isEqualTo(-2.5f)
            assertThat(enc).hasSize(4)
        }
    }

    @Nested
    inner class Doubles {
        @Test
        fun `1_0 has bit pattern 3FF0000000000000 LE`() {
            assertThat(IcebergSingleValue.encode(ColType.DOUBLE, 1.0))
                .isEqualTo(bytes(0, 0, 0, 0, 0, 0, 0xF0, 0x3F))
        }

        @Test
        fun `-0_0 keeps its sign bit`() {
            assertThat(IcebergSingleValue.encodeDouble(-0.0))
                .isEqualTo(bytes(0, 0, 0, 0, 0, 0, 0, 0x80))
        }
    }

    @Nested
    inner class Dates {
        @Test
        fun `epoch day 1 is int 1`() {
            assertThat(IcebergSingleValue.encode(ColType.DATE, LocalDate.of(1970, 1, 2)))
                .isEqualTo(bytes(0x01, 0, 0, 0))
        }

        @Test
        fun `pre-epoch dates are negative`() {
            assertThat(IcebergSingleValue.encode(ColType.DATE, LocalDate.of(1969, 12, 31)))
                .isEqualTo(bytes(0xFF, 0xFF, 0xFF, 0xFF))
        }

        @Test
        fun `raw day count and LocalDate agree`() {
            val d = LocalDate.of(2026, 9, 4)
            assertThat(IcebergSingleValue.encode(ColType.DATE, d))
                .isEqualTo(IcebergSingleValue.encodeDate(d.toEpochDay().toInt()))
        }
    }

    @Nested
    inner class Times {
        @Test
        fun `22-31-08 is 81068000000 micros`() {
            // Spec example: 22:31:08 -> 81068000000 microseconds from midnight.
            assertThat(IcebergSingleValue.encode(ColType.TIME, LocalTime.of(22, 31, 8)))
                .isEqualTo(IcebergSingleValue.encodeLong(81_068_000_000L))
        }

        @Test
        fun `midnight is zero`() {
            assertThat(IcebergSingleValue.encode(ColType.TIME, 0L))
                .isEqualTo(ByteArray(8))
        }
    }

    @Nested
    inner class Timestamps {
        @Test
        fun `spec example 2017-11-16T22-31-08 is 1510871468000000 micros`() {
            assertThat(
                IcebergSingleValue.encode(ColType.TIMESTAMP, LocalDateTime.of(2017, 11, 16, 22, 31, 8)),
            ).isEqualTo(IcebergSingleValue.encodeLong(1_510_871_468_000_000L))
        }

        @Test
        fun `timestamptz encodes Instant micros`() {
            val i = Instant.parse("2017-11-16T22:31:08.000001Z")
            assertThat(IcebergSingleValue.encode(ColType.TIMESTAMPTZ, i))
                .isEqualTo(IcebergSingleValue.encodeLong(1_510_871_468_000_001L))
        }

        @Test
        fun `pre-epoch instants are negative micros`() {
            val i = Instant.parse("1969-12-31T23:59:59.999999Z")
            assertThat(IcebergSingleValue.encode(ColType.TIMESTAMPTZ, i))
                .isEqualTo(IcebergSingleValue.encodeLong(-1L))
        }

        @Test
        fun `raw long micros pass through for both timestamp types`() {
            assertThat(IcebergSingleValue.encode(ColType.TIMESTAMP, 42L))
                .isEqualTo(IcebergSingleValue.encodeLong(42L))
            assertThat(IcebergSingleValue.encode(ColType.TIMESTAMPTZ, 42L))
                .isEqualTo(IcebergSingleValue.encodeLong(42L))
        }
    }

    @Nested
    inner class Strings {
        @Test
        fun `ab is 0x61 0x62 with no length prefix`() {
            assertThat(IcebergSingleValue.encode(ColType.STRING, "ab")).isEqualTo(bytes(0x61, 0x62))
        }

        @Test
        fun `empty string is empty bytes`() {
            assertThat(IcebergSingleValue.encodeString("")).isEmpty()
        }

        @Test
        fun `non-ascii is UTF-8`() {
            assertThat(IcebergSingleValue.encodeString("µ")).isEqualTo(bytes(0xC2, 0xB5))
            assertThat(IcebergSingleValue.encodeString("iceberg"))
                .isEqualTo("iceberg".toByteArray(Charsets.UTF_8))
        }
    }

    @Nested
    inner class Uuids {
        @Test
        fun `spec example lays out big-endian`() {
            // Spec example: f79c3e09-677c-4bbd-a479-3f349cb785e7
            val u = UUID.fromString("f79c3e09-677c-4bbd-a479-3f349cb785e7")
            assertThat(IcebergSingleValue.encode(ColType.UUID_T, u)).isEqualTo(
                bytes(
                    0xF7, 0x9C, 0x3E, 0x09, 0x67, 0x7C, 0x4B, 0xBD,
                    0xA4, 0x79, 0x3F, 0x34, 0x9C, 0xB7, 0x85, 0xE7,
                ),
            )
        }

        @Test
        fun `round-trips through ByteBuffer`() {
            val u = UUID.randomUUID()
            val enc = IcebergSingleValue.encodeUuid(u)
            val bb = ByteBuffer.wrap(enc)
            assertThat(UUID(bb.long, bb.long)).isEqualTo(u)
        }
    }

    @Nested
    inner class Binaries {
        @Test
        fun `bytes pass through`() {
            val b = bytes(0x00, 0xFF, 0x10)
            assertThat(IcebergSingleValue.encode(ColType.BINARY, b)).isEqualTo(b)
        }

        @Test
        fun `result is a defensive copy`() {
            val b = bytes(0x01)
            val enc = IcebergSingleValue.encodeBinary(b)
            b[0] = 0x02
            assertThat(enc).isEqualTo(bytes(0x01))
        }
    }

    @Nested
    inner class Decimals {
        @Test
        fun `spec example 14_20 is unscaled 1420 = 0x058C`() {
            assertThat(IcebergSingleValue.encode(ColType.DECIMAL, BigDecimal("14.20")))
                .isEqualTo(bytes(0x05, 0x8C))
        }

        @Test
        fun `zero is a single zero byte`() {
            assertThat(IcebergSingleValue.encodeDecimal(BigDecimal("0.00"))).isEqualTo(bytes(0x00))
        }

        @Test
        fun `negative values use minimal twos complement`() {
            assertThat(IcebergSingleValue.encodeDecimalUnscaled(BigInteger.valueOf(-1)))
                .isEqualTo(bytes(0xFF))
            assertThat(IcebergSingleValue.encodeDecimalUnscaled(BigInteger.valueOf(-129)))
                .isEqualTo(bytes(0xFF, 0x7F))
        }

        @Test
        fun `positive high-bit values get a leading zero byte`() {
            assertThat(IcebergSingleValue.encodeDecimalUnscaled(BigInteger.valueOf(128)))
                .isEqualTo(bytes(0x00, 0x80))
        }

        @Test
        fun `wider than 8 bytes is fine`() {
            val unscaled = BigInteger("123456789012345678901234567890")
            assertThat(IcebergSingleValue.encodeDecimalUnscaled(unscaled))
                .isEqualTo(unscaled.toByteArray())
        }
    }

    @Nested
    inner class Dispatch {
        @Test
        fun `type mismatches throw IllegalArgumentException`() {
            assertThatThrownBy { IcebergSingleValue.encode(ColType.INT, "1") }
                .isInstanceOf(IllegalArgumentException::class.java)
            assertThatThrownBy { IcebergSingleValue.encode(ColType.STRING, 1) }
                .isInstanceOf(IllegalArgumentException::class.java)
            assertThatThrownBy { IcebergSingleValue.encode(ColType.LONG, 1.0) }
                .isInstanceOf(IllegalArgumentException::class.java)
            assertThatThrownBy { IcebergSingleValue.encode(ColType.FLOAT, 1.0) }
                .isInstanceOf(IllegalArgumentException::class.java)
            assertThatThrownBy { IcebergSingleValue.encode(ColType.UUID_T, "not-a-uuid") }
                .isInstanceOf(IllegalArgumentException::class.java)
            assertThatThrownBy { IcebergSingleValue.encode(ColType.DECIMAL, 1.5) }
                .isInstanceOf(IllegalArgumentException::class.java)
        }

        @Test
        fun `every ColType dispatches`() {
            val samples =
                mapOf<ColType, Any>(
                    ColType.BOOLEAN to true,
                    ColType.INT8 to 1,
                    ColType.INT16 to 1,
                    ColType.INT to 1,
                    ColType.LONG to 1L,
                    ColType.UINT8 to 1,
                    ColType.UINT16 to 1,
                    ColType.UINT32 to 1L,
                    ColType.UINT64 to BigInteger.ONE,
                    ColType.FLOAT to 1f,
                    ColType.DOUBLE to 1.0,
                    ColType.DECIMAL to BigDecimal.ONE,
                    ColType.DATE to LocalDate.EPOCH,
                    ColType.TIME to LocalTime.NOON,
                    ColType.TIMESTAMP_S to 1L,
                    ColType.TIMESTAMP_MS to 1L,
                    ColType.TIMESTAMP to LocalDateTime.of(2020, 1, 1, 0, 0),
                    ColType.TIMESTAMP_NS to 1L,
                    ColType.TIMESTAMPTZ to Instant.EPOCH,
                    ColType.STRING to "s",
                    ColType.JSON to "{}",
                    ColType.UUID_T to UUID.randomUUID(),
                    ColType.BINARY to byteArrayOf(1),
                )
            // Exhaustive by assertion, not by hope: a new ColType with no
            // sample here fails LOUDLY instead of going untested.
            assertThat(samples.keys).containsExactlyInAnyOrderElementsOf(ColType.entries)
            for ((type, value) in samples) {
                assertThat(IcebergSingleValue.encode(type, value))
                    .describedAs(type.wire)
                    .isNotNull()
            }
        }
    }

    // ---- the DuckLake scalar-parity types ---------------------------------

    @Nested
    inner class ScalarParityTypes {
        @Test
        fun `every int-mapped type shares the 4-byte int encoding`() {
            for (type in listOf(ColType.INT8, ColType.INT16, ColType.UINT8, ColType.UINT16)) {
                assertThat(IcebergSingleValue.encode(type, -7))
                    .describedAs(type.wire)
                    .isEqualTo(IcebergSingleValue.encode(ColType.INT, -7))
                assertThat(IcebergSingleValue.decode(type, IcebergSingleValue.encode(type, -7)))
                    .describedAs(type.wire)
                    .isEqualTo(-7)
            }
        }

        @Test
        fun `uint32 uses the 8-byte long encoding, not int`() {
            // The facade maps uint32 to Iceberg long, so its bound is 8
            // bytes. Getting this wrong makes every uint32 bound
            // undecodable under the live type.
            val enc = IcebergSingleValue.encode(ColType.UINT32, 4_294_967_295L)
            assertThat(enc).hasSize(8)
            assertThat(enc).isEqualTo(IcebergSingleValue.encodeLong(4_294_967_295L))
            assertThat(IcebergSingleValue.decode(ColType.UINT32, enc)).isEqualTo(4_294_967_295L)
        }

        @Test
        fun `uint64 encodes as the decimal unscaled value, growing a sign byte above 2 to the 63`() {
            val big = BigInteger.ONE.shiftLeft(63) // 9223372036854775808
            val enc = IcebergSingleValue.encode(ColType.UINT64, big)
            // Minimal two's-complement of a positive value with the high
            // bit set needs the leading 0x00.
            assertThat(enc).hasSize(9)
            assertThat(enc[0]).isEqualTo(0.toByte())
            assertThat(IcebergSingleValue.decode(ColType.UINT64, enc)).isEqualTo(big)

            val max = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE)
            assertThat(IcebergSingleValue.decode(ColType.UINT64, IcebergSingleValue.encode(ColType.UINT64, max)))
                .isEqualTo(max)
        }

        @Test
        fun `uint64 refuses a Long, whose sign would be a guess`() {
            assertThatThrownBy { IcebergSingleValue.encode(ColType.UINT64, -1L) }
                .isInstanceOf(IllegalArgumentException::class.java)
        }

        @Test
        fun `timestamp_s and timestamp_ms store micros, exactly like timestamp`() {
            for (type in listOf(ColType.TIMESTAMP_S, ColType.TIMESTAMP_MS)) {
                assertThat(IcebergSingleValue.encode(type, -1_500_000L))
                    .describedAs(type.wire)
                    .isEqualTo(IcebergSingleValue.encode(ColType.TIMESTAMP, -1_500_000L))
            }
        }

        @Test
        fun `the declared-unit helpers convert to micros and refuse overflow`() {
            assertThat(IcebergSingleValue.encodeTimestampSeconds(-2L))
                .isEqualTo(IcebergSingleValue.encodeLong(-2_000_000L))
            assertThat(IcebergSingleValue.encodeTimestampMillis(-2L))
                .isEqualTo(IcebergSingleValue.encodeLong(-2_000L))
            assertThatThrownBy { IcebergSingleValue.encodeTimestampSeconds(Long.MAX_VALUE) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("timestamp_s")
            assertThatThrownBy { IcebergSingleValue.encodeTimestampMillis(Long.MAX_VALUE) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("timestamp_ms")
        }

        @Test
        fun `timestamp_ns stores nanos - the one temporal type that is not micros`() {
            val nanos = 1_510_871_468_123_456_789L
            assertThat(IcebergSingleValue.encode(ColType.TIMESTAMP_NS, nanos))
                .isEqualTo(IcebergSingleValue.encodeLong(nanos))
            assertThat(IcebergSingleValue.decode(ColType.TIMESTAMP_NS, IcebergSingleValue.encodeLong(nanos)))
                .isEqualTo(nanos)
        }

        @Test
        fun `a timestamp_ns LocalDateTime converts to nanos and refuses overflow`() {
            assertThat(IcebergSingleValue.encode(ColType.TIMESTAMP_NS, LocalDateTime.of(1970, 1, 1, 0, 0)))
                .isEqualTo(IcebergSingleValue.encodeLong(0L))
            // Year 2600 is ~2e19 nanos: past int64. Nanosecond timestamps
            // only span 1677..2262, and the codec must say so rather than wrap.
            assertThatThrownBy {
                IcebergSingleValue.encode(ColType.TIMESTAMP_NS, LocalDateTime.of(2600, 1, 1, 0, 0))
            }.isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("timestamp_ns")
        }

        @Test
        fun `json encodes as UTF-8 bytes and compares as unsigned bytes like string`() {
            val document = """{"µ":1}"""
            assertThat(IcebergSingleValue.encode(ColType.JSON, document))
                .isEqualTo(document.toByteArray(Charsets.UTF_8))
            assertThat(IcebergSingleValue.decode(ColType.JSON, document.toByteArray(Charsets.UTF_8)))
                .isEqualTo(document)
            // Above the BMP, UTF-16 unit order (String.compareTo) disagrees
            // with code-point order; json must use the same unsigned byte
            // compare string does.
            val astral = "😀"
            val bmp = "�"
            assertThat(IcebergSingleValue.compareValues(ColType.JSON, astral, bmp))
                .isEqualTo(IcebergSingleValue.compareValues(ColType.STRING, astral, bmp))
        }
    }
}
