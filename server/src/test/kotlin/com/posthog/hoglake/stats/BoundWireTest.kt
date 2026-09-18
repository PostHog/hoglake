package com.posthog.hoglake.stats

import com.fasterxml.jackson.databind.JsonNode
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.wireObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.math.BigInteger
import java.util.UUID

/**
 * The JSON wire form of decoded bounds ([BoundWire]), pinned value by
 * value. The full cross-language matrix is replayed by
 * [QeBoundsWireVectorsTest] through the shared vector file; this class
 * pins the conventions themselves — exact tokens, sentinels, refusals —
 * so a convention regression names the rule it broke rather than a
 * vector index.
 *
 * Conventions under test (documented in openapi/hoglake.yaml,
 * FileColumnStats + DecodeBoundRequest):
 *  - integers and decimals render as EXACT JSON numbers (never through
 *    a double);
 *  - float/double render as JSON numbers; the infinities render as the
 *    string sentinels "Infinity"/"-Infinity" (not JSON numbers); NaN is
 *    REFUSED — the store never holds a NaN bound (StatsSanity), so
 *    rendering one would invent data;
 *  - -0.0 renders with its sign (it is what is stored: lower bounds are
 *    normalized to -0.0 by role);
 *  - temporals render as ISO-8601 strings, timestamptz with a trailing
 *    Z; date as an ISO date;
 *  - string/json/uuid render as strings, binary as base64, boolean as a
 *    boolean;
 *  - a value that cannot exist in its type's domain (an out-of-day time
 *    bound, a non-UTF-8 "string") is refused with a typed
 *    IllegalArgumentException, never rendered as something else and
 *    never a crash.
 */
class BoundWireTest {
    private val mapper = wireObjectMapper()

    private fun wire(
        type: ColType,
        value: Any,
        scale: Int = 0,
    ): String = mapper.writeValueAsString(BoundWire.render(type, scale, IcebergSingleValue.encode(type, value)))

    private fun render(
        type: ColType,
        bytes: ByteArray,
        scale: Int = 0,
    ): JsonNode = BoundWire.render(type, scale, bytes)

    // ---- booleans and integers --------------------------------------------

    @Test
    fun `boolean renders as a JSON boolean`() {
        assertThat(wire(ColType.BOOLEAN, true)).isEqualTo("true")
        assertThat(wire(ColType.BOOLEAN, false)).isEqualTo("false")
    }

    @Test
    fun `int family renders as exact JSON numbers`() {
        assertThat(wire(ColType.INT8, -128)).isEqualTo("-128")
        assertThat(wire(ColType.UINT16, 65535)).isEqualTo("65535")
        assertThat(wire(ColType.INT, Int.MIN_VALUE)).isEqualTo("-2147483648")
        assertThat(wire(ColType.UINT32, 4294967295L)).isEqualTo("4294967295")
    }

    @Test
    fun `long renders exactly above 2^53`() {
        // The double-routing hazard: 2^53 + 1 is the first long JSON.parse
        // rounds. The wire token must carry every digit.
        assertThat(wire(ColType.LONG, 9007199254740993L)).isEqualTo("9007199254740993")
        assertThat(wire(ColType.LONG, Long.MIN_VALUE)).isEqualTo("-9223372036854775808")
    }

    @Test
    fun `uint64 renders as an exact JSON integer past int64`() {
        assertThat(wire(ColType.UINT64, BigInteger("18446744073709551615")))
            .isEqualTo("18446744073709551615")
    }

    // ---- decimal -----------------------------------------------------------

    @Test
    fun `decimal renders at the column scale, exactly`() {
        // Unscaled 150 at scale 2 is 1.50 — the trailing zero is part of
        // the value's scale and must survive.
        assertThat(wire(ColType.DECIMAL, BigInteger("150"), scale = 2)).isEqualTo("1.50")
        assertThat(wire(ColType.DECIMAL, BigInteger("-1234"), scale = 2)).isEqualTo("-12.34")
    }

    @Test
    fun `decimal precision 38 renders every digit`() {
        val digits38 = "9".repeat(38)
        assertThat(wire(ColType.DECIMAL, BigInteger(digits38))).isEqualTo(digits38)
    }

    @Test
    fun `empty decimal bytes are refused by the codec, not rendered`() {
        assertThatThrownBy { render(ColType.DECIMAL, ByteArray(0)) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    // ---- float and double --------------------------------------------------

    @Test
    fun `finite float and double render as JSON numbers`() {
        assertThat(wire(ColType.FLOAT, 1.5f)).isEqualTo("1.5")
        assertThat(wire(ColType.DOUBLE, 0.1)).isEqualTo("0.1")
    }

    @Test
    fun `negative zero renders with its sign`() {
        // Lower bounds are STORED as -0.0 by the normalization rule; the
        // wire shows what is stored.
        assertThat(wire(ColType.FLOAT, -0.0f)).isEqualTo("-0.0")
        assertThat(wire(ColType.DOUBLE, -0.0)).isEqualTo("-0.0")
        assertThat(wire(ColType.DOUBLE, 0.0)).isEqualTo("0.0")
    }

    @Test
    fun `infinities render as string sentinels`() {
        // JSON has no Infinity token, so the sentinel is a STRING.
        assertThat(wire(ColType.FLOAT, Float.POSITIVE_INFINITY)).isEqualTo("\"Infinity\"")
        assertThat(wire(ColType.FLOAT, Float.NEGATIVE_INFINITY)).isEqualTo("\"-Infinity\"")
        assertThat(wire(ColType.DOUBLE, Double.POSITIVE_INFINITY)).isEqualTo("\"Infinity\"")
        assertThat(wire(ColType.DOUBLE, Double.NEGATIVE_INFINITY)).isEqualTo("\"-Infinity\"")
    }

    @Test
    fun `NaN is refused, not rendered`() {
        // StatsSanity keeps NaN out of stored bounds; the debug decode
        // endpoint maps this refusal to a named 422.
        assertThatThrownBy { render(ColType.FLOAT, IcebergSingleValue.encode(ColType.FLOAT, Float.NaN)) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("NaN")
        assertThatThrownBy { render(ColType.DOUBLE, IcebergSingleValue.encode(ColType.DOUBLE, Double.NaN)) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("NaN")
    }

    // ---- temporals ---------------------------------------------------------

    @Test
    fun `date renders as an ISO date string`() {
        assertThat(wire(ColType.DATE, 0)).isEqualTo("\"1970-01-01\"")
        assertThat(wire(ColType.DATE, -1)).isEqualTo("\"1969-12-31\"")
        assertThat(wire(ColType.DATE, 20701)).isEqualTo("\"2026-09-05\"")
    }

    @Test
    fun `time renders as an ISO time string`() {
        assertThat(wire(ColType.TIME, 0L)).isEqualTo("\"00:00\"")
        assertThat(wire(ColType.TIME, 86_399_999_999L)).isEqualTo("\"23:59:59.999999\"")
    }

    @Test
    fun `time outside the day is refused`() {
        // The codec is deliberately total (any 8 bytes decode), so the
        // domain check is the renderer's: -1 micros is not a time of day.
        assertThatThrownBy { render(ColType.TIME, IcebergSingleValue.encodeTimeMicros(-1L)) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("time")
        assertThatThrownBy { render(ColType.TIME, IcebergSingleValue.encodeTimeMicros(86_400_000_000L)) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `timestamps render as ISO date-time strings in their stored unit`() {
        assertThat(wire(ColType.TIMESTAMP, 0L)).isEqualTo("\"1970-01-01T00:00\"")
        assertThat(wire(ColType.TIMESTAMP, -1L)).isEqualTo("\"1969-12-31T23:59:59.999999\"")
        // timestamp_s/_ms STORE micros (the mapped Iceberg type's unit).
        assertThat(wire(ColType.TIMESTAMP_MS, 1_788_609_600_123_000L))
            .isEqualTo("\"2026-09-05T12:00:00.123\"")
        // timestamp_ns stores nanos — the one non-micros temporal.
        assertThat(wire(ColType.TIMESTAMP_NS, 1_788_609_600_123_456_789L))
            .isEqualTo("\"2026-09-05T12:00:00.123456789\"")
    }

    @Test
    fun `timestamptz renders as a UTC instant with Z`() {
        assertThat(wire(ColType.TIMESTAMPTZ, 1_788_609_600_000_000L))
            .isEqualTo("\"2026-09-05T12:00:00Z\"")
    }

    @Test
    fun `extreme timestamps render rather than crash`() {
        // Long.MAX_VALUE micros is year ~294247 — ISO-8601 expanded
        // representation, still a string, never an exception.
        assertThat(wire(ColType.TIMESTAMP, Long.MAX_VALUE)).startsWith("\"+294247-")
        assertThat(wire(ColType.TIMESTAMP, Long.MIN_VALUE)).startsWith("\"-29")
        assertThat(wire(ColType.TIMESTAMPTZ, Long.MAX_VALUE)).endsWith("Z\"")
    }

    // ---- strings, uuid, binary ---------------------------------------------

    @Test
    fun `string and json render as strings`() {
        assertThat(wire(ColType.STRING, "héllo")).isEqualTo("\"héllo\"")
        assertThat(wire(ColType.JSON, """{"a":1}""")).isEqualTo("\"{\\\"a\\\":1}\"")
        assertThat(wire(ColType.STRING, "")).isEqualTo("\"\"")
    }

    @Test
    fun `a non-UTF-8 string bound is refused, not replaced`() {
        // decode() hands back raw bytes for a mislabelled string bound;
        // rendering them as a string would silently substitute U+FFFD.
        assertThatThrownBy { render(ColType.STRING, byteArrayOf(0xFE.toByte(), 0x02)) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("UTF-8")
    }

    @Test
    fun `uuid renders canonically and binary as base64`() {
        val uuid = UUID.fromString("123e4567-e89b-12d3-a456-426614174000")
        assertThat(wire(ColType.UUID_T, uuid)).isEqualTo("\"123e4567-e89b-12d3-a456-426614174000\"")
        assertThat(wire(ColType.BINARY, byteArrayOf(0, 1, 2))).isEqualTo("\"AAEC\"")
        assertThat(wire(ColType.BINARY, ByteArray(0))).isEqualTo("\"\"")
    }

    // ---- structural refusals -----------------------------------------------

    @Test
    fun `variant and containers are refused by the codec`() {
        for (type in listOf(ColType.VARIANT, ColType.LIST, ColType.STRUCT, ColType.MAP)) {
            assertThatThrownBy { render(type, ByteArray(4)) }
                .describedAs(type.wire)
                .isInstanceOf(IllegalArgumentException::class.java)
        }
    }

    @Test
    fun `wrong-length bytes propagate the codec's refusal`() {
        assertThatThrownBy { render(ColType.LONG, ByteArray(4)) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("8 bytes")
    }

    // ---- scale parameter ---------------------------------------------------

    @Test
    fun `scaleOf reads the catalog's decimal scale safely`() {
        assertThat(BoundWire.scaleOf(null)).isEqualTo(0)
        assertThat(BoundWire.scaleOf(mapOf("scale" to 2))).isEqualTo(2)
        // JSON-sourced params may bind as Long; toInt() on one truncates
        // silently, which is exactly what this accessor must not do.
        assertThat(BoundWire.scaleOf(mapOf("scale" to 3L))).isEqualTo(3)
        assertThatThrownBy { BoundWire.scaleOf(mapOf("scale" to "two")) }
            .isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy { BoundWire.scaleOf(mapOf("scale" to 4294967297L)) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `decimal scale zero renders as an integer token`() {
        assertThat(mapper.writeValueAsString(render(ColType.DECIMAL, BigInteger("7").toByteArray())))
            .isEqualTo("7")
        // BigDecimal(unscaled, 0) — no fraction, no exponent.
        assertThat(
            BoundWire.render(ColType.DECIMAL, 0, BigInteger.ZERO.toByteArray().let { byteArrayOf(0) })
                .decimalValue(),
        ).isEqualByComparingTo(BigDecimal.ZERO)
    }
}
