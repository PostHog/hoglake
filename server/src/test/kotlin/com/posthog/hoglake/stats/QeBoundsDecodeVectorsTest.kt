package com.posthog.hoglake.stats

import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.model.ColType
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.math.BigInteger
import java.nio.file.Files
import java.util.Base64
import java.util.UUID

/**
 * The decode side of the shared cross-language vectors
 * (pyhoglake/tests/vectors/bounds_vectors.json): the Kotlin decoder
 * must satisfy decode(unhex(hex)) == value and
 * encode(decode(unhex(hex))) == unhex(hex), the file's own contract.
 * A `verify: encode_only` marker does not automatically exempt a vector
 * here; the loop below says which types it exempts and why.
 * [QeBoundsVectorsTest] covers
 * the encode direction; this class exists because compaction's
 * bounds-merge now DECODES stored bounds to recompute typed min/max,
 * so decoder divergence would corrupt pruning bounds silently.
 *
 * A missing vector file FAILS (see [BoundsVectorFile]) rather than
 * skipping: a skipped test reads as green, so a broken path would
 * disable the gate without anyone noticing.
 */
class QeBoundsDecodeVectorsTest {
    @Test
    fun `kotlin decoder round-trips every two-way vector`() {
        val vectorFile = BoundsVectorFile.resolve()
        val root = ObjectMapper().readTree(Files.readString(vectorFile))
        val vectors = if (root.isArray) root else root["vectors"]
        assertThat(vectors.size())
            .describedAs("vector count (pinned in both languages; see BoundsVectorFile.EXPECTED_COUNT)")
            .isEqualTo(BoundsVectorFile.EXPECTED_COUNT)

        val failures = mutableListOf<String>()
        var checked = 0
        var skipped = 0
        for ((i, v) in vectors.withIndex()) {
            val type = ColType.fromWire(v["type"].asText())
            // `encode_only` marks a vector whose PYTHON decode is knowingly
            // lossy. The one remaining case comes from a Python carrier
            // type the JVM does not use — the ambient decimal context that
            // rounds >28-digit unscaled values, where BigInteger has none —
            // so the marker says nothing about Kotlin, and honouring it
            // would forfeit coverage of the nastiest values in the file.
            // Any OTHER type's encode_only is a defect of the codec
            // itself, presumed shared, so it still skips.
            if (v["verify"]?.asText() == "encode_only" && type !in PYTHON_ONLY_DECODE_DEFECTS) {
                skipped++
                continue
            }
            val hex = v["hex"].asText()
            val valueStr = v["value"].asText()
            val bytes = decodeHex(hex)
            checked++

            val decoded = IcebergSingleValue.decode(type, bytes)

            // decode(bytes) must equal the conventional value...
            if (!valueMatches(type, valueStr, decoded)) {
                failures += "vector[$i] $type value='$valueStr': decoded=$decoded"
                continue
            }
            // ...and re-encoding must reproduce the exact bytes.
            val reencoded = IcebergSingleValue.encode(type, decoded)
            if (!reencoded.contentEquals(bytes)) {
                failures += "vector[$i] $type value='$valueStr': " +
                    "re-encode ${hexOf(reencoded)} != $hex"
            }
        }
        // Every vector is accounted for: run or deliberately exempted. A
        // silently dropped one cannot hide inside a loose lower bound.
        assertThat(checked + skipped)
            .describedAs("two-way vectors checked (%d) plus encode_only skips (%d)", checked, skipped)
            .isEqualTo(BoundsVectorFile.EXPECTED_COUNT)
        // The exemption set must describe the file, not outlive it: an
        // entry for a type the file no longer marks encode_only would
        // silently exempt every future encode_only vector of that type.
        val markedInFile =
            vectors.filter { it["verify"]?.asText() == "encode_only" }
                .map { ColType.fromWire(it["type"].asText()) }
                .toSet()
        assertThat(PYTHON_ONLY_DECODE_DEFECTS)
            .describedAs("exempted types vs the file's actual encode_only types")
            .isEqualTo(markedInFile)

        assertThat(failures).describedAs("decode divergences (%d checked)", checked).isEmpty()
    }

    /** Compare a decoded value against the vector file's value conventions. */
    private fun valueMatches(
        type: ColType,
        value: String,
        decoded: Any,
    ): Boolean =
        when (type) {
            ColType.VARIANT -> error("VARIANT has no scalar bounds vector")
            ColType.BOOLEAN -> decoded == value.toBooleanStrict()
            ColType.INT8, ColType.INT16, ColType.UINT8, ColType.UINT16,
            ColType.INT, ColType.DATE,
            -> decoded == value.toInt()
            ColType.UINT32, ColType.LONG, ColType.TIME,
            ColType.TIMESTAMP_S, ColType.TIMESTAMP_MS, ColType.TIMESTAMP,
            ColType.TIMESTAMP_NS, ColType.TIMESTAMPTZ,
            -> decoded == value.toLong()
            ColType.FLOAT ->
                when (value) {
                    // NaN payloads: the hex bits are authoritative.
                    "NaN" -> (decoded as Float).isNaN()
                    "Infinity" -> decoded == Float.POSITIVE_INFINITY
                    "-Infinity" -> decoded == Float.NEGATIVE_INFINITY
                    else -> decoded == value.toFloat()
                }
            ColType.DOUBLE ->
                when (value) {
                    "NaN" -> (decoded as Double).isNaN()
                    "Infinity" -> decoded == Double.POSITIVE_INFINITY
                    "-Infinity" -> decoded == Double.NEGATIVE_INFINITY
                    else -> decoded == value.toDouble()
                }
            ColType.STRING, ColType.JSON -> decoded == value
            ColType.UUID_T -> decoded == UUID.fromString(value)
            ColType.BINARY -> (decoded as ByteArray).contentEquals(Base64.getDecoder().decode(value))
            ColType.UINT64, ColType.DECIMAL -> decoded == BigInteger(value)
            // Containers never appear in the bounds vector file — they
            // have no single-value encoding — so reaching this arm means
            // someone put one there.
            ColType.LIST, ColType.STRUCT, ColType.MAP ->
                error("nested container '${type.wire}' has no bounds vector")
        }

    private companion object {
        /**
         * Types whose `encode_only` vectors record a PYTHON decode
         * defect the JVM does not share, so the Kotlin side runs them as
         * full round-trips anyway. Only decimal remains: Python rounds
         * >28-digit unscaled values through the ambient decimal context,
         * while BigInteger has no such context.
         *
         * timestamp_s/timestamp_ms were here too, for a datetime
         * year-9999 ceiling that decode_bound now falls back past — a
         * real codec gap, since fixed. The set is asserted against the
         * vector file's actual `encode_only` types below, so a stale
         * entry fails rather than quietly exempting a live type.
         */
        val PYTHON_ONLY_DECODE_DEFECTS = setOf(ColType.DECIMAL)
    }

    private fun decodeHex(hex: String): ByteArray {
        require(hex.length % 2 == 0) { "odd hex length in '$hex'" }
        return ByteArray(hex.length / 2) { i ->
            hex.substring(i * 2, i * 2 + 2).toInt(16).toByte()
        }
    }

    private fun hexOf(bytes: ByteArray): String = bytes.joinToString("") { "%02x".format(it) }
}
