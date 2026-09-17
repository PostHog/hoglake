package com.posthog.hoglake.stats

import com.posthog.hoglake.model.ColType
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset
import java.util.UUID

/**
 * Iceberg "Binary single-value serialization"
 * (https://iceberg.apache.org/spec/#binary-single-value-serialization) —
 * the encoding used for manifest `lower_bounds` / `upper_bounds` and for
 * `hog_file_column_stats.lower_bound` / `upper_bound` (V1__init.sql),
 * so manifest generation for the Iceberg facade is a mechanical copy.
 *
 * The encoding is always that of the column type's MAPPED Iceberg type
 * (ColType.icebergType, docs/iceberg-federation.md §2), never of the hoglake
 * type name — that is what keeps manifest generation a copy, and it is
 * why several hoglake types share one encoding.
 *
 * Encodings:
 *  - boolean: 1 byte, 0x00 = false / 0x01 = true
 *  - int8/int16/uint8/uint16/int: 4-byte little-endian (all map to
 *    Iceberg int; the small widths fit int32 exactly, signed or not)
 *  - uint32/long: 8-byte little-endian (uint32 maps to Iceberg long
 *    because 32 unsigned bits do not fit a signed int32)
 *  - uint64: minimal two's-complement big-endian unscaled value of the
 *    mapped decimal(20,0) — i.e. the decimal encoding of the unsigned
 *    value, which is 9 bytes with a 0x00 sign byte above 2^63
 *  - float: 4-byte IEEE-754, little-endian
 *  - double: 8-byte IEEE-754, little-endian
 *  - date: days since 1970-01-01 as int, 4-byte little-endian
 *  - time: microseconds since midnight as long, 8-byte little-endian
 *  - timestamp_s/timestamp_ms/timestamp/timestamptz: MICROseconds since
 *    epoch as long, 8-byte LE. The seconds and millis variants store
 *    micros because they map to Iceberg timestamp, whose single-value
 *    unit is micros — the declared precision is catalog metadata, not a
 *    bound unit.
 *  - timestamp_ns: NANOseconds since epoch as long, 8-byte LE (Iceberg
 *    V3 timestamp_ns single-value serialization). The one temporal type
 *    whose bounds are not micros.
 *  - string/json: UTF-8 bytes, no length prefix (json maps to Iceberg
 *    string; its bytes are the document verbatim, never re-canonicalized)
 *  - uuid: 16 bytes, big-endian (most significant byte first)
 *  - binary: the bytes themselves
 *  - decimal: unscaled value as minimal two's-complement big-endian
 *
 * Domain enforcement is deliberately NOT here: the codec is total in
 * both directions (`encode(type, decode(type, b)) contentEquals b` for
 * every well-formed b), so a uint8 bound holding 300 encodes and decodes
 * without complaint. Keeping values inside their type's domain is the
 * writer's job; a codec that threw here would make `decode` un-invertible
 * for hostile footers, which is the failure mode this design refuses.
 */
object IcebergSingleValue {
    fun encodeBoolean(value: Boolean): ByteArray = byteArrayOf(if (value) 0x01 else 0x00)

    fun encodeInt(value: Int): ByteArray = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array()

    fun encodeLong(value: Long): ByteArray =
        ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array()

    fun encodeFloat(value: Float): ByteArray =
        ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(value).array()

    fun encodeDouble(value: Double): ByteArray =
        ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(value).array()

    /** Date as days since the unix epoch (may be negative). */
    fun encodeDate(daysSinceEpoch: Int): ByteArray = encodeInt(daysSinceEpoch)

    fun encodeDate(value: LocalDate): ByteArray =
        try {
            encodeDate(Math.toIntExact(value.toEpochDay()))
        } catch (_: ArithmeticException) {
            throw IllegalArgumentException(
                "date $value is outside the encodable range (int32 epoch days)",
            )
        }

    /** Time of day as microseconds since midnight. */
    fun encodeTimeMicros(microsSinceMidnight: Long): ByteArray = encodeLong(microsSinceMidnight)

    fun encodeTime(value: LocalTime): ByteArray = encodeTimeMicros(value.toNanoOfDay() / 1_000)

    /** Timestamp (with or without zone) as microseconds since the unix epoch. */
    fun encodeTimestampMicros(microsSinceEpoch: Long): ByteArray = encodeLong(microsSinceEpoch)

    fun encodeTimestamp(value: LocalDateTime): ByteArray = encodeTimestamptz(value.toInstant(ZoneOffset.UTC))

    fun encodeTimestamptz(value: Instant): ByteArray =
        try {
            encodeTimestampMicros(
                Math.addExact(
                    Math.multiplyExact(value.epochSecond, 1_000_000L),
                    (value.nano / 1_000).toLong(),
                ),
            )
        } catch (_: ArithmeticException) {
            throw IllegalArgumentException(
                "timestamp $value is outside the encodable range (int64 micros since epoch)",
            )
        }

    /**
     * A `timestamp_s` bound expressed in its declared unit, converted to
     * the stored micros. Throws rather than silently wrapping: a bound
     * that cannot be expressed in int64 micros is not a bound, and the
     * footer path's duty is to leave it NULL (see FooterStats) rather
     * than guess.
     */
    fun encodeTimestampSeconds(secondsSinceEpoch: Long): ByteArray =
        encodeTimestampMicros(scaleOrThrow(secondsSinceEpoch, 1_000_000L, "timestamp_s", "micros"))

    /** A `timestamp_ms` bound in its declared unit, converted to stored micros. */
    fun encodeTimestampMillis(millisSinceEpoch: Long): ByteArray =
        encodeTimestampMicros(scaleOrThrow(millisSinceEpoch, 1_000L, "timestamp_ms", "micros"))

    /**
     * A `timestamp_ns` bound: nanos are the STORED unit (Iceberg V3
     * timestamp_ns), so this is a plain 8-byte LE long — no conversion,
     * and no precision to lose.
     */
    fun encodeTimestampNanos(nanosSinceEpoch: Long): ByteArray = encodeLong(nanosSinceEpoch)

    private fun scaleOrThrow(
        value: Long,
        factor: Long,
        what: String,
        unit: String,
    ): Long =
        try {
            Math.multiplyExact(value, factor)
        } catch (_: ArithmeticException) {
            throw IllegalArgumentException(
                "$what value $value is outside the encodable range (int64 $unit since epoch)",
            )
        }

    /** Nanos since the epoch for a naive timestamp, refusing int64 overflow. */
    private fun nanosOf(value: LocalDateTime): Long {
        val instant = value.toInstant(ZoneOffset.UTC)
        val seconds = scaleOrThrow(instant.epochSecond, 1_000_000_000L, "timestamp_ns", "nanos")
        return try {
            Math.addExact(seconds, instant.nano.toLong())
        } catch (_: ArithmeticException) {
            throw IllegalArgumentException(
                "timestamp_ns value $value is outside the encodable range (int64 nanos since epoch)",
            )
        }
    }

    fun encodeString(value: String): ByteArray = value.toByteArray(Charsets.UTF_8)

    /** UUID as 16 bytes, most significant byte first. */
    fun encodeUuid(value: UUID): ByteArray =
        ByteBuffer.allocate(16)
            .putLong(value.mostSignificantBits)
            .putLong(value.leastSignificantBits)
            .array()

    fun encodeBinary(value: ByteArray): ByteArray = value.copyOf()

    /**
     * Decimal as the minimal two's-complement big-endian bytes of the
     * unscaled value. The scale is carried by the column type, not the
     * serialized value; callers must ensure [unscaled] is already at the
     * column's scale.
     */
    fun encodeDecimalUnscaled(unscaled: BigInteger): ByteArray = unscaled.toByteArray()

    fun encodeDecimal(value: BigDecimal): ByteArray = encodeDecimalUnscaled(value.unscaledValue())

    /**
     * Encode [value] for catalog column type [type]. Accepts the natural
     * JVM primitive for each type plus the obvious java.time /
     * java.math companions. Throws [IllegalArgumentException] on a
     * type/value mismatch — bounds must never be guessed.
     */
    fun encode(
        type: ColType,
        value: Any,
    ): ByteArray =
        when (type) {
            ColType.VARIANT -> throw IllegalArgumentException("variant has no scalar bounds encoding")
            ColType.BOOLEAN -> encodeBoolean(expect(type, value))
            // Iceberg int: one encoding for five hoglake types.
            ColType.INT8, ColType.INT16, ColType.UINT8, ColType.UINT16, ColType.INT ->
                encodeInt(expect(type, value))
            ColType.UINT32, ColType.LONG ->
                when (value) {
                    is Long -> encodeLong(value)
                    is Int -> encodeLong(value.toLong())
                    else -> mismatch(type, value)
                }
            // uint64 maps to decimal(20,0): BigInteger is the only
            // unambiguous JVM carrier for [0, 2^64), so it is the only
            // one accepted — a Long would silently mean its signed value.
            ColType.UINT64 -> encodeDecimalUnscaled(expect(type, value))
            ColType.FLOAT -> encodeFloat(expect(type, value))
            ColType.DOUBLE -> encodeDouble(expect(type, value))
            ColType.DATE ->
                when (value) {
                    is Int -> encodeDate(value)
                    is LocalDate -> encodeDate(value)
                    else -> mismatch(type, value)
                }
            ColType.TIME ->
                when (value) {
                    is Long -> encodeTimeMicros(value)
                    is LocalTime -> encodeTime(value)
                    else -> mismatch(type, value)
                }
            // The Long is always the STORED unit, so these three share one
            // arm: micros. Callers holding a bound in the type's declared
            // unit convert through encodeTimestampSeconds/Millis first.
            ColType.TIMESTAMP_S, ColType.TIMESTAMP_MS, ColType.TIMESTAMP ->
                when (value) {
                    is Long -> encodeTimestampMicros(value)
                    is LocalDateTime -> encodeTimestamp(value)
                    else -> mismatch(type, value)
                }
            // Nanos, not micros — the Iceberg V3 timestamp_ns encoding.
            ColType.TIMESTAMP_NS ->
                when (value) {
                    is Long -> encodeTimestampNanos(value)
                    is LocalDateTime -> encodeTimestampNanos(nanosOf(value))
                    else -> mismatch(type, value)
                }
            ColType.TIMESTAMPTZ ->
                when (value) {
                    is Long -> encodeTimestampMicros(value)
                    is Instant -> encodeTimestamptz(value)
                    else -> mismatch(type, value)
                }
            // json maps to Iceberg string: same bytes, no canonicalization.
            ColType.STRING, ColType.JSON -> encodeString(expect(type, value))
            ColType.UUID_T -> encodeUuid(expect(type, value))
            ColType.BINARY -> encodeBinary(expect(type, value))
            ColType.DECIMAL ->
                when (value) {
                    is BigDecimal -> encodeDecimal(value)
                    is BigInteger -> encodeDecimalUnscaled(value)
                    else -> mismatch(type, value)
                }
        }

    /**
     * Inverse of [encode] — mirrors pyhoglake's `decode_bound` (the two
     * codecs are kept bit-identical by the shared vector file
     * pyhoglake/tests/vectors/bounds_vectors.json). Returns the natural
     * JVM value for each type: Boolean, Int (int/date-days), Long
     * (long/time/timestamp micros), Float, Double, String, UUID,
     * ByteArray (binary), BigInteger (decimal UNSCALED — the scale is
     * carried by the column type, exactly like [encodeDecimalUnscaled]).
     * The invariant `encode(type, decode(type, b)) contentEquals b`
     * holds for every well-formed encoding.
     */
    fun decode(
        type: ColType,
        data: ByteArray,
    ): Any =
        when (type) {
            ColType.VARIANT -> throw IllegalArgumentException("variant has no scalar bounds encoding")
            ColType.BOOLEAN -> {
                expectLength(type, data, 1)
                data[0] != 0.toByte()
            }
            ColType.INT8, ColType.INT16, ColType.UINT8, ColType.UINT16,
            ColType.INT, ColType.DATE,
            -> {
                expectLength(type, data, 4)
                ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).int
            }
            ColType.UINT32, ColType.LONG, ColType.TIME,
            ColType.TIMESTAMP_S, ColType.TIMESTAMP_MS, ColType.TIMESTAMP,
            ColType.TIMESTAMP_NS, ColType.TIMESTAMPTZ,
            -> {
                expectLength(type, data, 8)
                ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).long
            }
            ColType.FLOAT -> {
                expectLength(type, data, 4)
                Float.fromBits(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).int)
            }
            ColType.DOUBLE -> {
                expectLength(type, data, 8)
                Double.fromBits(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).long)
            }
            ColType.STRING, ColType.JSON -> String(data, Charsets.UTF_8)
            ColType.UUID_T -> {
                expectLength(type, data, 16)
                val buf = ByteBuffer.wrap(data)
                UUID(buf.long, buf.long)
            }
            ColType.BINARY -> data.copyOf()
            // uint64 shares decimal's encoding; both decode to the UNSCALED
            // BigInteger (for uint64 the scale is 0, so it is the value).
            ColType.UINT64, ColType.DECIMAL -> {
                require(data.isNotEmpty()) { "empty ${type.wire} encoding" }
                BigInteger(data)
            }
        }

    /**
     * Typed comparison of two [decode]d values for [type]. STRING
     * compares as UTF-8 bytes unsigned (== code-point order — Java's
     * String.compareTo is UTF-16 unit order, which disagrees above the
     * BMP); UUID/BINARY compare bytes unsigned; everything else through
     * its natural Comparable. This is what makes compaction's
     * bounds-merge correct where a raw byte compare of the ENCODINGS
     * would not be (signed little-endian ints do not sort bytewise).
     */
    @Suppress("UNCHECKED_CAST")
    fun compareValues(
        type: ColType,
        a: Any,
        b: Any,
    ): Int =
        when (type) {
            ColType.VARIANT -> throw IllegalArgumentException("variant has no scalar bounds encoding")
            // json decodes to a String too, and must use the same unsigned
            // UTF-8 byte order — not String.compareTo.
            ColType.STRING, ColType.JSON ->
                java.util.Arrays.compareUnsigned(
                    (a as String).toByteArray(Charsets.UTF_8),
                    (b as String).toByteArray(Charsets.UTF_8),
                )
            ColType.BINARY ->
                java.util.Arrays.compareUnsigned(a as ByteArray, b as ByteArray)
            ColType.UUID_T ->
                java.util.Arrays.compareUnsigned(encodeUuid(a as UUID), encodeUuid(b as UUID))
            else -> (a as Comparable<Any>).compareTo(b)
        }

    private fun expectLength(
        type: ColType,
        data: ByteArray,
        expected: Int,
    ) {
        require(data.size == expected) {
            "cannot decode ${type.wire}: expected $expected bytes, got ${data.size}"
        }
    }

    private inline fun <reified T> expect(
        type: ColType,
        value: Any,
    ): T = value as? T ?: mismatch(type, value)

    private fun mismatch(
        type: ColType,
        value: Any,
    ): Nothing =
        throw IllegalArgumentException(
            "cannot encode ${value::class.qualifiedName} as Iceberg single-value for column type ${type.wire}",
        )
}
