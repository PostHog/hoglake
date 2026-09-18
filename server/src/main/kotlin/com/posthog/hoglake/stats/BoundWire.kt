package com.posthog.hoglake.stats

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.BigIntegerNode
import com.fasterxml.jackson.databind.node.BooleanNode
import com.fasterxml.jackson.databind.node.DecimalNode
import com.fasterxml.jackson.databind.node.DoubleNode
import com.fasterxml.jackson.databind.node.FloatNode
import com.fasterxml.jackson.databind.node.IntNode
import com.fasterxml.jackson.databind.node.LongNode
import com.fasterxml.jackson.databind.node.TextNode
import com.posthog.hoglake.model.ColType
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset
import java.util.Base64
import java.util.UUID

/**
 * The JSON wire form of a decoded column-statistics bound — the ONE
 * rendering layer over [IcebergSingleValue.decode], shared by
 * GET .../files/{fileId}/stats and POST /v1/debug/decode-bound (the
 * OpenAPI spec documents the conventions on their schemas). This object
 * never decodes bytes itself: decoding is the codec's, and a second
 * decode path is exactly the drift this layer exists to avoid.
 *
 * Conventions (openapi/hoglake.yaml, FileColumnStats):
 *
 *  - boolean -> JSON boolean.
 *  - every integer type (int8..uint64) and decimal -> an EXACT JSON
 *    number, never routed through a double. uint64 and decimal render
 *    as arbitrary-precision integers/decimals; a consumer must parse
 *    them exactly (JS: BigInt/raw-token parsing — the webui's int64
 *    handling), because JSON.parse's double rounds past 2^53.
 *  - decimal renders at the COLUMN's scale (unscaled x 10^-scale); the
 *    scale travels in the column's type_params, not in the bytes.
 *  - float/double -> JSON numbers, except the infinities, which JSON
 *    cannot spell as numbers: the string sentinels "Infinity" /
 *    "-Infinity". NaN is REFUSED — StatsSanity keeps NaN out of stored
 *    bounds on every door, so a NaN here is input that could never have
 *    been a bound, and the debug endpoint answers its named 422.
 *  - -0.0 renders with its sign: it is what is stored (the
 *    normalization rule fixes -0.0 as the LOWER-bound zero), and the
 *    wire reports the store.
 *  - temporals -> ISO-8601 strings, rendered with java.time's ISO
 *    formatters: date "2026-09-05"; time "13:37:42.123456"; the naive
 *    timestamps (timestamp_s/_ms/timestamp/timestamp_ns)
 *    "2026-09-05T12:00:00.123456" with no offset designator (the type
 *    is zoneless; the epoch reference is UTC); timestamptz as an
 *    instant with a trailing Z. timestamp_s/_ms bounds are stored in
 *    MICROS (the mapped Iceberg type's unit) and render accordingly;
 *    timestamp_ns renders its nanos.
 *  - string/json -> the string; uuid -> canonical 8-4-4-4-12; binary ->
 *    base64.
 *
 * REFUSALS, all [IllegalArgumentException] (the codec's own refusal
 * type, so callers map one exception family to one named 422): the
 * codec's wrong-length/empty/container/variant refusals propagate, and
 * this layer adds three of its own for values no bound of the type can
 * hold — a NaN float/double, a time outside [00:00, 24:00), and a
 * string/json bound that is not UTF-8 (decode hands those back as raw
 * bytes; rendering them as a string would silently substitute U+FFFD —
 * the mislabelled-file case, refused rather than guessed). The read
 * endpoint's caller (Dto layer) turns a refusal on a STORED bound into
 * JSON null — the "NULL, never guessed" read-side dual: a bound a
 * reader cannot decode is treated as absent, and absent bounds mean "do
 * not prune".
 */
object BoundWire {
    /**
     * Render the Iceberg single-value [bytes] of a [type] bound as its
     * wire JSON. [scale] is the decimal scale (ignored for every other
     * type); pass [scaleOf] of the column's type_params.
     */
    fun render(
        type: ColType,
        scale: Int,
        bytes: ByteArray,
    ): JsonNode = renderDecoded(type, scale, IcebergSingleValue.decode(type, bytes))

    private fun renderDecoded(
        type: ColType,
        scale: Int,
        decoded: Any,
    ): JsonNode =
        when (type) {
            // decode() already refused these; listed so the when is total.
            ColType.VARIANT, ColType.LIST, ColType.STRUCT, ColType.MAP ->
                throw IllegalArgumentException("column type '${type.wire}' has no bounds to render")
            ColType.BOOLEAN -> BooleanNode.valueOf(decoded as Boolean)
            ColType.INT8, ColType.INT16, ColType.UINT8, ColType.UINT16, ColType.INT ->
                IntNode.valueOf(decoded as Int)
            ColType.UINT32, ColType.LONG -> LongNode.valueOf(decoded as Long)
            // decimal(20,0) — scale 0 by construction, so the unscaled
            // BigInteger IS the value.
            ColType.UINT64 -> BigIntegerNode.valueOf(decoded as BigInteger)
            ColType.DECIMAL -> DecimalNode.valueOf(BigDecimal(decoded as BigInteger, scale))
            ColType.FLOAT -> {
                val value = decoded as Float
                when {
                    value.isNaN() -> refuseNaN(type)
                    value == Float.POSITIVE_INFINITY -> TextNode.valueOf("Infinity")
                    value == Float.NEGATIVE_INFINITY -> TextNode.valueOf("-Infinity")
                    else -> FloatNode.valueOf(value)
                }
            }
            ColType.DOUBLE -> {
                val value = decoded as Double
                when {
                    value.isNaN() -> refuseNaN(type)
                    value == Double.POSITIVE_INFINITY -> TextNode.valueOf("Infinity")
                    value == Double.NEGATIVE_INFINITY -> TextNode.valueOf("-Infinity")
                    else -> DoubleNode.valueOf(value)
                }
            }
            ColType.DATE ->
                // Every int32 epoch-day fits LocalDate's year range.
                TextNode.valueOf(LocalDate.ofEpochDay((decoded as Int).toLong()).toString())
            ColType.TIME -> {
                val micros = decoded as Long
                // The codec is total over 8-byte values; the day is not.
                if (micros !in 0 until 86_400_000_000L) {
                    throw IllegalArgumentException(
                        "time bound $micros is outside the day [0, 86400000000) microseconds",
                    )
                }
                TextNode.valueOf(LocalTime.ofNanoOfDay(micros * 1_000).toString())
            }
            // Stored micros for all three (the mapped Iceberg timestamp's
            // single-value unit) — see IcebergSingleValue's header.
            ColType.TIMESTAMP_S, ColType.TIMESTAMP_MS, ColType.TIMESTAMP ->
                TextNode.valueOf(localDateTimeOf(decoded as Long, 1_000_000L).toString())
            ColType.TIMESTAMP_NS ->
                TextNode.valueOf(localDateTimeOf(decoded as Long, 1_000_000_000L).toString())
            ColType.TIMESTAMPTZ -> {
                val micros = decoded as Long
                val instant =
                    Instant.ofEpochSecond(
                        Math.floorDiv(micros, 1_000_000L),
                        Math.floorMod(micros, 1_000_000L) * 1_000L,
                    )
                TextNode.valueOf(instant.toString())
            }
            ColType.STRING, ColType.JSON ->
                when (decoded) {
                    is String -> TextNode.valueOf(decoded)
                    // decode() returns the raw bytes for a bound that is
                    // not valid UTF-8: the file is mislabelled, and a
                    // U+FFFD-substituted rendering would be a different
                    // value pretending to be the bound.
                    else ->
                        throw IllegalArgumentException(
                            "${type.wire} bound is not valid UTF-8 " +
                                "(mislabelled binary data; cannot render as a string)",
                        )
                }
            ColType.UUID_T -> TextNode.valueOf((decoded as UUID).toString())
            ColType.BINARY -> TextNode.valueOf(Base64.getEncoder().encodeToString(decoded as ByteArray))
        }

    /**
     * Every int64 count of [unit]s since the epoch fits LocalDateTime
     * (year range ±999,999,999), so this never throws — which the fuzz
     * target verifies rather than assumes.
     */
    private fun localDateTimeOf(
        value: Long,
        unit: Long,
    ): LocalDateTime {
        val nanosPerUnit = 1_000_000_000L / unit
        return LocalDateTime.ofEpochSecond(
            Math.floorDiv(value, unit),
            (Math.floorMod(value, unit) * nanosPerUnit).toInt(),
            ZoneOffset.UTC,
        )
    }

    private fun refuseNaN(type: ColType): Nothing =
        throw IllegalArgumentException(
            "${type.wire} bound is NaN, which has no place in a bound " +
                "(NaNs are counted in nan_count, never stored as bounds)",
        )

    /**
     * The decimal scale in a column's type_params, safely: absent -> 0
     * (pyhoglake's convention), a non-integer or a value that does not
     * FIT an Int -> refusal. Never `as? Number` + `toInt()`, which
     * silently truncates a Long — the narrowing bug ColumnTrees'
     * decimalParam exists to prevent, kept out of this reader too.
     */
    fun scaleOf(typeParams: Map<String, Any?>?): Int {
        val raw = typeParams?.get("scale") ?: return 0
        val asLong =
            when (raw) {
                is Int -> raw.toLong()
                is Long -> raw
                is Short, is Byte -> (raw as Number).toLong()
                else -> throw IllegalArgumentException("decimal scale is not an integer")
            }
        if (asLong !in 0..ColumnScaleBound.MAX) {
            throw IllegalArgumentException("decimal scale $asLong is outside 0..${ColumnScaleBound.MAX}")
        }
        return asLong.toInt()
    }

    /**
     * The widest scale a hoglake decimal column can declare
     * (ColumnTrees.MAX_DECIMAL_PRECISION); a scale past it cannot have
     * come from validated DDL.
     */
    private object ColumnScaleBound {
        const val MAX = 38L
    }
}
