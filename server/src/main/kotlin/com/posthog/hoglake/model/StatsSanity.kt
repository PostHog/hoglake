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
        // nan_count counts NON-NULL floating-point values, so its
        // ceiling is value_count MINUS null_count, not value_count. The
        // looser clamp accepted 10 values / 9 nulls / 10 NaNs — a file
        // claiming more NaNs than it has non-null values at all.
        val nonNull = valueCount - nullCount
        nanCount?.let {
            if (it < 0) {
                repairs += "nan_count $it is negative; dropped"
                nanCount = null
            } else if (type != null && type.icebergType != IcebergType.FLOAT &&
                type.icebergType != IcebergType.DOUBLE
            ) {
                // Iceberg's nan_value_counts is defined for float and
                // double only; there is no NaN in any other domain, so a
                // count here describes something that cannot exist.
                repairs += "nan_count $it on '${type.wire}', which has no NaN; dropped"
                nanCount = null
            } else if (it > nonNull) {
                repairs +=
                    "nan_count $it exceeds the non-null value count $nonNull " +
                    "(value_count $valueCount - null_count $nullCount); clamped to $nonNull"
                nanCount = nonNull
            }
        }
        if (stats.sizeBytes != null && stats.sizeBytes < 0) {
            repairs += "size_bytes ${stats.sizeBytes} is negative; dropped"
        }

        // A column with no non-null values has nothing to bound. The
        // hydrator's footer path already refuses exactly this
        // (`chunkBounds` returns null unless `hasNonNullValue()`), so
        // without it the commit door accepted pruning metadata a footer
        // could never produce — the same asymmetry as the NaN case
        // below, one relationship over.
        if (nonNull == 0L && (lower != null || upper != null)) {
            repairs += "bounds present on a column with $valueCount values and $nullCount nulls; dropped"
            lower = null
            upper = null
        }

        if (type != null) {
            if (lower != null && !decodable(type, lower)) {
                repairs += "lower_bound ${undecodableReason(type, lower)}; dropped"
                lower = null
            }
            if (upper != null && !decodable(type, upper)) {
                repairs += "upper_bound ${undecodableReason(type, upper)}; dropped"
                upper = null
            }
        }
        // SIGNED ZEROS, before the ordering check below rather than
        // after: the pair this fixes is exactly the one that check is
        // told to tolerate.
        val normalizedLower = normalizeBound(type, lower, lower = true)
        val normalizedUpper = normalizeBound(type, upper, lower = false)
        val zeroNormalized =
            !sameBytes(normalizedLower, lower) || !sameBytes(normalizedUpper, upper)
        lower = normalizedLower
        upper = normalizedUpper

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
            if (repairs.isEmpty() && !zeroNormalized) {
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
     * The bytes to STORE for a bound of [type] in this role.
     *
     * Only the signed zeros move. +0.0 and -0.0 are IEEE-equal, so which
     * one a writer reports for a bound is arbitrary — DuckDB reports
     * +0.0 for both bounds of an all-zero column, pyarrow normalizes to
     * (-0.0, +0.0) — but Iceberg's evaluators compare float and double
     * bounds in NATURAL order, where -0.0 < 0.0. Stored verbatim, the
     * pair (lower = +0.0, upper = -0.0) is therefore an empty range, and
     * a reader skips the file for `x = 0.0`: data that is there, pruned
     * away. Iceberg removes the arbitrariness by fixing the sign per
     * ROLE — a lower bound stores -0.0, an upper bound stores +0.0 —
     * and docs/iceberg-federation.md §2.1 requires the stored bound to BE the
     * Iceberg single-value serialization, because manifest generation
     * copies it mechanically. Rewriting one zero as the other widens
     * nothing: they are the same number.
     *
     * The cases are pinned cross-language in
     * `pyhoglake/tests/vectors/bounds_vectors.json` under
     * `bound_normalization`; `pyhoglake.bounds.normalize_bound` is the
     * other half and must answer that file identically.
     *
     * Anything that is not a float/double bound of the right width comes
     * back untouched. A bound of the wrong width is not a zero to
     * canonicalize; it is a malformed bound, and [decodable] drops those
     * rather than rewriting them into something storable.
     */
    fun normalizeBound(
        type: ColType?,
        bytes: ByteArray?,
        lower: Boolean,
    ): ByteArray? {
        if (type == null || bytes == null) return bytes
        val zero =
            when (type.icebergType) {
                IcebergType.FLOAT ->
                    if (bytes.size == 4 && java.lang.Float.intBitsToFloat(intLE(bytes)) == 0.0f) {
                        if (lower) FLOAT_NEGATIVE_ZERO else FLOAT_POSITIVE_ZERO
                    } else {
                        null
                    }
                IcebergType.DOUBLE ->
                    if (bytes.size == 8 && java.lang.Double.longBitsToDouble(longLE(bytes)) == 0.0) {
                        if (lower) DOUBLE_NEGATIVE_ZERO else DOUBLE_POSITIVE_ZERO
                    } else {
                        null
                    }
                else -> null
            }
        return zero?.copyOf() ?: bytes
    }

    // Little-endian, like every other fixed-width Iceberg single value:
    // the sign bit is the LAST byte. Pinned against the codec itself by
    // QeBoundNormalizationVectorsTest through the shared vector file.
    private val FLOAT_POSITIVE_ZERO = ByteArray(4)
    private val FLOAT_NEGATIVE_ZERO = byteArrayOf(0, 0, 0, 0x80.toByte())
    private val DOUBLE_POSITIVE_ZERO = ByteArray(8)
    private val DOUBLE_NEGATIVE_ZERO = byteArrayOf(0, 0, 0, 0, 0, 0, 0, 0x80.toByte())

    private fun sameBytes(
        a: ByteArray?,
        b: ByteArray?,
    ): Boolean = if (a == null || b == null) a === b else a.contentEquals(b)

    /**
     * Whether [bytes] can be read back as a single value of [type],
     * under Iceberg's single-value serialization (docs/iceberg-federation.md
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
            IcebergType.INT, IcebergType.DATE -> bytes.size == 4
            // Length is NOT sufficient for the floating types: a NaN
            // encodes in four or eight bytes like any other value, and
            // it is UNORDERED — so [compare] treats it as equal to
            // everything and an inverted pair containing one sails
            // through. Iceberg keeps NaNs out of lower/upper bounds
            // entirely (they are counted in nan_value_counts instead),
            // and the hydrator's footer path already drops any pair with
            // one. Without this the commit door accepted exactly what
            // the hydrator door refuses: a client could publish pruning
            // metadata a footer could never produce.
            //
            // -0.0 is NOT refused: it is an ordinary value. The pair
            // (+0.0, -0.0) is not an inversion either — [compare] uses
            // IEEE equality, under which the two zeros are equal — but
            // that tolerance now only ever describes RAW writer input:
            // [normalizeBound] canonicalizes both zeros by role before
            // the ordering check runs, so no such pair reaches storage.
            IcebergType.FLOAT ->
                bytes.size == 4 && !java.lang.Float.intBitsToFloat(intLE(bytes)).isNaN()
            IcebergType.DOUBLE ->
                bytes.size == 8 && !java.lang.Double.longBitsToDouble(longLE(bytes)).isNaN()
            IcebergType.LONG,
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
     * Why [bytes] failed [decodable], in the operator's terms.
     *
     * The single diagnostic has to be true: reporting a NaN as "not
     * decodable as 'float' (4 bytes)" names the one property that was
     * correct, and an operator reading it goes looking for a length bug
     * that is not there.
     */
    private fun undecodableReason(
        type: ColType,
        bytes: ByteArray,
    ): String {
        val nan =
            when {
                type.icebergType == IcebergType.FLOAT && bytes.size == 4 ->
                    java.lang.Float.intBitsToFloat(intLE(bytes)).isNaN()
                type.icebergType == IcebergType.DOUBLE && bytes.size == 8 ->
                    java.lang.Double.longBitsToDouble(longLE(bytes)).isNaN()
                else -> false
            }
        return if (nan) {
            "is NaN, which has no place in a '${type.wire}' bound (NaNs are counted in nan_count)"
        } else {
            "is not decodable as '${type.wire}' (${bytes.size} bytes)"
        }
    }

    /**
     * Compare two bounds in the ORDER OF THEIR TYPE, which is the only
     * order an inversion check can be asked in. A raw byte compare would
     * call every negative int32 "above" every positive one (little-endian
     * two's complement), so a byte-wise check would have invented far
     * more inversions than it caught.
     *
     * Every arm is the order of the DECODED value, because the encoding
     * and the value do not always sort alike. The full audit, so the
     * next reader need not redo it:
     *
     *  - boolean: decoded, NOT the raw byte. The codec reads any nonzero
     *    byte as true, and a signed byte compare put `0xff` (true) BELOW
     *    `0x00` (false) — an inverted semantic range that passed.
     *  - int/date: 4-byte LE signed. `int8`, `int16`, `uint8` and
     *    `uint16` all map here and all fit int32 exactly, unsigned or
     *    not, so one signed compare is right for the whole family.
     *  - long/time/timestamp/timestamptz/timestamp_ns: 8-byte LE signed.
     *    `uint32` maps to long precisely so its values stay positive.
     *  - float/double: IEEE numeric, with NaN unordered (an "inversion"
     *    against a NaN is not evidence of anything) and -0.0 EQUAL to
     *    +0.0. Kotlin's `compareTo` is the total order, which ranks
     *    -0.0 below +0.0 and would have deleted the perfectly good pair
     *    (lower = +0.0, upper = -0.0). That pair is no longer stored
     *    either: [normalizeBound] runs first and gives each zero the
     *    sign Iceberg fixes for its role, which is what keeps a
     *    total-order READER — every Iceberg evaluator — from reading an
     *    empty range where hoglake sees an equal one. Dropping the pair
     *    here instead would have thrown away a true bound to avoid a
     *    problem the canonical bytes do not have.
     *  - decimal: unscaled big-endian two's complement. Both bounds of
     *    one column share that column's scale, so comparing unscaled IS
     *    comparing values. `uint64` maps here and decodes signed with a
     *    0x00 sign byte above 2^63 — which `BigInteger` reproduces.
     *  - string/json/binary/uuid: unsigned lexicographic, which for
     *    UTF-8 is code-point order.
     *  - list/struct/map: no arm, and none needed. A container carries
     *    no values, so it has no bound to compare — the commit door
     *    refuses a stats row for a container field id outright, and the
     *    hydrator only ever emits rows for leaves. They fall to the
     *    unsigned-bytes branch, which is unreachable for them; listed
     *    here so the audit is complete rather than merely long.
     *
     * A length this function cannot read has already been dropped by
     * [decodable], so the reads below are safe.
     */
    private fun compare(
        type: ColType,
        a: ByteArray,
        b: ByteArray,
    ): Int =
        when (type.icebergType) {
            IcebergType.BOOLEAN -> (a[0] != 0.toByte()).compareTo(b[0] != 0.toByte())
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
                ieee(x.toDouble(), y.toDouble())
            }
            IcebergType.DOUBLE ->
                ieee(
                    java.lang.Double.longBitsToDouble(longLE(a)),
                    java.lang.Double.longBitsToDouble(longLE(b)),
                )
            IcebergType.DECIMAL -> java.math.BigInteger(a).compareTo(java.math.BigInteger(b))
            else -> java.util.Arrays.compareUnsigned(a, b)
        }

    /**
     * IEEE comparison, not Kotlin's total order: NaN is unordered (so an
     * apparent inversion against one proves nothing) and -0.0 equals
     * +0.0 (so a bound pair that merely disagrees about zero's sign is
     * not inverted). `compareTo` says otherwise on both counts, and both
     * of its answers here would have deleted sound bounds.
     *
     * The zero half of that is now belt over braces: [check] normalizes
     * the signed zeros before it calls this, so the pair reaching here
     * is already (-0.0, +0.0), which BOTH orders read the same way. The
     * IEEE rule stays because this function is about what an inversion
     * IS, not about what the last caller happened to hand it.
     */
    private fun ieee(
        x: Double,
        y: Double,
    ): Int =
        when {
            x.isNaN() || y.isNaN() -> 0
            x < y -> -1
            x > y -> 1
            else -> 0
        }

    private fun intLE(raw: ByteArray): Int = java.nio.ByteBuffer.wrap(raw).order(java.nio.ByteOrder.LITTLE_ENDIAN).int

    private fun longLE(raw: ByteArray): Long =
        java.nio.ByteBuffer.wrap(raw).order(java.nio.ByteOrder.LITTLE_ENDIAN).long
}
