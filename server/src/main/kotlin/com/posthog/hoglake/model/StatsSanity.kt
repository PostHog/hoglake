package com.posthog.hoglake.model

/**
 * The one place a `hog_file_column_stats` row is checked for internal
 * sense before it is stored.
 *
 * Two surfaces produce stats rows and NEITHER of them owned this check:
 *
 *  - the commit path takes `column_stats` verbatim from a client. It
 *    verified the field id is live, is a leaf, is not duplicated, and
 *    that the counts are non-negative — and then stored whatever bytes
 *    the caller called a bound. A four-byte "long" bound, a `lower`
 *    above its `upper`, a `null_count` larger than the `value_count`
 *    it is a subset of: all accepted.
 *  - the hydrator produces stats from a parquet footer, and a footer is
 *    no more trustworthy than a client — it is written by the same
 *    client, one layer down.
 *
 * A bound that cannot be decoded as its catalog type is not a slightly
 * wrong bound; it is a bound every reader must either reject or
 * misread, and readers PRUNE on these. An inverted pair prunes away
 * data that is there. So the policy is to DROP rather than store: a
 * missing bound costs a scan, a wrong one costs a wrong answer.
 *
 * Counts are clamped rather than dropped — they are not nullable, and
 * `null_count > value_count` has an obvious nearest-true value — but
 * every repair is reported so the caller's bug is visible instead of
 * absorbed.
 *
 * App-level only. No migration, no CHECK constraint: the rule needs the
 * column's TYPE to evaluate, which the stats table does not carry.
 */
object StatsSanity {
    /**
     * [stats] as it should be stored, plus a human-readable line per
     * repair ([repairs] empty = the input was already sound).
     */
    data class Checked(val stats: ColumnStats, val repairs: List<String>)

    /**
     * Sanitize one stats row against its column's catalog [type], or
     * against nothing when the type is unknown (the counts are still
     * checked; the bounds can only be compared, not decoded).
     */
    fun check(
        stats: ColumnStats,
        type: ColType?,
    ): Checked {
        val repairs = mutableListOf<String>()
        var valueCount = stats.valueCount
        var nullCount = stats.nullCount
        var nanCount = stats.nanCount
        var lower = stats.lowerBound
        var upper = stats.upperBound

        if (valueCount < 0) {
            repairs += "value_count ${stats.valueCount} is negative; clamped to 0"
            valueCount = 0
        }
        if (nullCount < 0) {
            repairs += "null_count ${stats.nullCount} is negative; clamped to 0"
            nullCount = 0
        }
        // The nulls are a SUBSET of the values counted, so this is not a
        // taste judgement: a null_count above value_count describes a
        // file that cannot exist, and every "fraction of nulls" consumer
        // downstream reads it as > 1.
        if (nullCount > valueCount) {
            repairs += "null_count $nullCount exceeds value_count $valueCount; clamped to $valueCount"
            nullCount = valueCount
        }
        nanCount?.let {
            if (it < 0) {
                repairs += "nan_count $it is negative; dropped"
                nanCount = null
            } else if (it > valueCount) {
                repairs += "nan_count $it exceeds value_count $valueCount; clamped to $valueCount"
                nanCount = valueCount
            }
        }
        if (stats.sizeBytes != null && stats.sizeBytes < 0) {
            repairs += "size_bytes ${stats.sizeBytes} is negative"
        }

        if (type != null) {
            if (lower != null && !decodable(type, lower)) {
                repairs += "lower_bound is not decodable as '${type.wire}' (${lower.size} bytes); dropped"
                lower = null
            }
            if (upper != null && !decodable(type, upper)) {
                repairs += "upper_bound is not decodable as '${type.wire}' (${upper.size} bytes); dropped"
                upper = null
            }
        }
        // BOTH go, not the "wrong" one: an inverted pair says one of the
        // two is wrong and there is nothing in the row that says which.
        // Keeping either would be picking at random, and the one kept
        // would still prune.
        if (lower != null && upper != null && type != null && compare(type, lower, upper) > 0) {
            repairs += "lower_bound sorts above upper_bound for '${type.wire}'; both dropped"
            lower = null
            upper = null
        }

        val fixed =
            if (repairs.isEmpty()) {
                stats
            } else {
                ColumnStats(
                    fieldId = stats.fieldId,
                    valueCount = valueCount,
                    nullCount = nullCount,
                    nanCount = nanCount,
                    sizeBytes = stats.sizeBytes?.takeIf { it >= 0 },
                    lowerBound = lower,
                    upperBound = upper,
                )
            }
        return Checked(fixed, repairs)
    }

    /**
     * Whether [bytes] can be read back as a single value of [type],
     * under Iceberg's single-value serialization (iceberg-federation.md
     * §2.1) — which is a pure LENGTH question for every fixed-width
     * type and an emptiness question for the variable-width ones.
     *
     * `string`/`json`/`binary` accept any length INCLUDING zero (the
     * empty string is a value). `decimal` needs at least one byte: its
     * encoding is the minimal two's-complement big-endian unscaled
     * value, and zero bytes is not a number — the same rule the
     * compaction rewriter enforces on the data itself.
     */
    private fun decodable(
        type: ColType,
        bytes: ByteArray,
    ): Boolean =
        when (type.icebergType) {
            IcebergType.BOOLEAN -> bytes.size == 1
            IcebergType.INT, IcebergType.DATE, IcebergType.FLOAT -> bytes.size == 4
            IcebergType.LONG,
            IcebergType.DOUBLE,
            IcebergType.TIME,
            IcebergType.TIMESTAMP,
            IcebergType.TIMESTAMPTZ,
            IcebergType.TIMESTAMP_NS,
            -> bytes.size == 8
            IcebergType.UUID -> bytes.size == 16
            IcebergType.DECIMAL -> bytes.isNotEmpty()
            IcebergType.STRING, IcebergType.BINARY -> true
            else -> true
        }

    /**
     * Compare two bounds in the ORDER OF THEIR TYPE, which is the only
     * order an inversion check can be asked in. A raw byte compare would
     * call every negative int32 "above" every positive one (little-endian
     * two's complement), so a byte-wise check would have invented far
     * more inversions than it caught.
     *
     * Numeric bounds are little-endian (Iceberg's encoding); decimal is
     * big-endian two's complement; string/binary/uuid compare unsigned
     * lexicographic. A length this function cannot read has already been
     * dropped by [decodable], so the reads below are safe.
     */
    private fun compare(
        type: ColType,
        a: ByteArray,
        b: ByteArray,
    ): Int =
        when (type.icebergType) {
            IcebergType.BOOLEAN -> a[0].compareTo(b[0])
            IcebergType.INT, IcebergType.DATE -> intLE(a).compareTo(intLE(b))
            IcebergType.LONG,
            IcebergType.TIME,
            IcebergType.TIMESTAMP,
            IcebergType.TIMESTAMPTZ,
            IcebergType.TIMESTAMP_NS,
            -> longLE(a).compareTo(longLE(b))
            // NaN is unordered, so an "inverted" pair involving one is
            // not evidence of anything. Comparing equal leaves it alone.
            IcebergType.FLOAT -> {
                val x = java.lang.Float.intBitsToFloat(intLE(a))
                val y = java.lang.Float.intBitsToFloat(intLE(b))
                if (x.isNaN() || y.isNaN()) 0 else x.compareTo(y)
            }
            IcebergType.DOUBLE -> {
                val x = java.lang.Double.longBitsToDouble(longLE(a))
                val y = java.lang.Double.longBitsToDouble(longLE(b))
                if (x.isNaN() || y.isNaN()) 0 else x.compareTo(y)
            }
            IcebergType.DECIMAL -> java.math.BigInteger(a).compareTo(java.math.BigInteger(b))
            else -> java.util.Arrays.compareUnsigned(a, b)
        }

    private fun intLE(raw: ByteArray): Int = java.nio.ByteBuffer.wrap(raw).order(java.nio.ByteOrder.LITTLE_ENDIAN).int

    private fun longLE(raw: ByteArray): Long =
        java.nio.ByteBuffer.wrap(raw).order(java.nio.ByteOrder.LITTLE_ENDIAN).long
}
