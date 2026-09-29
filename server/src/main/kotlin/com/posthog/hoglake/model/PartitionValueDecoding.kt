package com.posthog.hoglake.model

import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Locale

/**
 * Stored partition value -> the string an operator reads.
 *
 * The catalog stores a partition tuple the way the writer transformed it
 * — `["20713"]`, not `["2026-09-17"]` — and the server has never decoded
 * it: `GET .../partitions/values` returns the stored strings verbatim
 * and `webui/src/lib/partitions.ts` does the decoding in the browser.
 *
 * The partitions listing cannot work that way. Its `filter=` matches on
 * the value the user is looking at, and its default sort is by that
 * value, both of which happen in the SERVER before paging. A server that
 * filtered on `"20713"` while the page showed `2026-09-17` would be a
 * filter nobody could type.
 *
 * So this is the SECOND implementation of one rule, and the rule is the
 * writer's: pyhoglake's `transforms.py` produces these ordinals, and
 * `webui/src/lib/partitions.ts#decodeValue` renders them.
 *
 *   year   calendar year - 1970
 *   month  (year - 1970) * 12 + (month - 1)   — months since 1970-01
 *   day    days since the epoch
 *   hour   hours since the epoch
 *
 * All four FLOOR, so a pre-epoch ordinal is negative and must render as
 * a date before 1970 rather than as `1970--1` or month 0.
 *
 * PARITY IS PINNED BY A SHARED FIXTURE, not by this paragraph:
 * `src/test/resources/vectors/partition_decode_vectors.json` holds the
 * (transform, raw, transform_param, expected) table, and BOTH
 * `PartitionValueDecodingTest` and `webui/test/partitions.test.ts` read
 * that one file and pin its count. A hand-copied table would only catch
 * a change made to one implementation AND forgotten in its own test
 * list; one artifact reds the other side directly. The precedent is
 * `pyhoglake/tests/vectors/bounds_vectors.json`.
 *
 * MEASUREMENT: pure string/arithmetic work, no I/O, no allocation beyond
 * the result. The listing decodes at most one tuple per sampled group,
 * a few thousand per table (see [PartitionListing]); the 5,000-group
 * integration fixture decodes in well under a millisecond of the
 * request's budget.
 */
object PartitionValueDecoding {
    private val ISO_DATE: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE

    private const val SECONDS_PER_DAY = 86_400L
    private const val SECONDS_PER_HOUR = 3_600L

    /**
     * Render one stored element under [transform].
     *
     * Returns null for a null stored value — a null partition value is
     * its own thing, not the string "null", and the listing sorts it
     * first and matches it with an EMPTY filter. (The webui renders the
     * same case as the literal "null"; that is a display choice on top
     * of this, not a different decoding.)
     *
     * An ordinal this cannot interpret degrades to the raw string. A
     * malformed value must never become a confident wrong date: the
     * reader has no way to tell one from the truth.
     */
    fun decode(
        transform: String,
        raw: String?,
        transformParam: Int? = null,
    ): String? {
        if (raw == null) return null
        // Above 2^53 the webui's Number() would lose precision and
        // refuses; Kotlin's Long would not, but the two must agree on
        // WHICH inputs decode, so the bound is shared. A non-numeric
        // ordinal falls through the same way.
        val n = raw.toLongOrNull()?.takeIf { it in -MAX_SAFE_INTEGER..MAX_SAFE_INTEGER }
        return when (transform) {
            "year" -> if (n == null) raw else (1970 + n).toString()
            "month" -> {
                if (n == null) {
                    raw
                } else {
                    // Floor division, so months before 1970-01 land in
                    // the right year: -1 is 1969-12, not 1970--1.
                    val year = 1970 + Math.floorDiv(n, 12)
                    val month = Math.floorMod(n, 12) + 1
                    // Locale.ROOT, not the default: `String.format`
                    // without one uses Locale.getDefault(FORMAT), and a
                    // pod whose LANG is ar_SA renders "٢٠٢٦-٠٤". That
                    // would corrupt the two things server-side decoding
                    // exists for — the filter the operator types in
                    // ASCII, and the sort, which would order by
                    // Arabic-Indic code points. ISO_DATE is already
                    // safe (DecimalStyle.STANDARD is ASCII).
                    String.format(Locale.ROOT, "%d-%02d", year, month)
                }
            }
            // An ordinal inside the safe-integer bound can still be
            // outside the representable instant range, so the
            // conversion is GUARDED and falls back to the raw string —
            // the same degradation a non-numeric ordinal takes.
            "day" -> utc(n, SECONDS_PER_DAY)?.let { ISO_DATE.format(it) } ?: raw
            "hour" ->
                utc(n, SECONDS_PER_HOUR)
                    ?.let { ISO_DATE.format(it) + String.format(Locale.ROOT, "T%02d", it.hour) } ?: raw
            // The bucket INDEX, not a value: say so, or it reads as data.
            "bucket" ->
                if (transformParam == null) "bucket $raw" else "bucket $raw/$transformParam"
            // identity, truncate, and anything a newer writer adds: the
            // stored value is already the answer.
            else -> raw
        }
    }

    /**
     * [ordinal] units of [secondsPerUnit] as a UTC instant, or null when
     * the ordinal is unusable (non-numeric, past the shared safe-integer
     * bound, or multiplying/converting out of the instant range).
     * Partition ordinals are epoch-relative, never local.
     */
    private fun utc(
        ordinal: Long?,
        secondsPerUnit: Long,
    ): java.time.OffsetDateTime? {
        if (ordinal == null) return null
        return runCatching {
            Instant.ofEpochSecond(Math.multiplyExact(ordinal, secondsPerUnit)).atOffset(ZoneOffset.UTC)
        }.getOrNull()
    }

    /**
     * `Number.MAX_SAFE_INTEGER`, the webui decoder's own bound. Shared so
     * both sides refuse the same inputs rather than one of them
     * inventing a date the other declines to.
     */
    const val MAX_SAFE_INTEGER: Long = 9_007_199_254_740_991L
}
