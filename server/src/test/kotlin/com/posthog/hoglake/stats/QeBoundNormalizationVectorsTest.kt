package com.posthog.hoglake.stats

import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.StatsSanity
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.file.Files

/**
 * The second half of the cross-language contract, and the semantic one.
 *
 * [QeBoundsVectorsTest] pins what a value ENCODES to. This pins what
 * gets STORED, which the encoding alone does not settle: +0.0 and -0.0
 * are IEEE-equal, so a writer may report either for a bound, while
 * Iceberg's evaluators compare float/double bounds in natural (total)
 * order, where -0.0 < 0.0. A stored pair of (lower = +0.0, upper = -0.0)
 * therefore reads as an EMPTY range and prunes away a file that holds
 * 0.0. Iceberg fixes the sign by ROLE — lower stores -0.0, upper stores
 * +0.0 — and both languages must answer the same file, or the same data
 * prunes differently depending on which one wrote it.
 *
 * The Python side runs these same cases in
 * `pyhoglake/tests/qe_vectors.py`.
 */
class QeBoundNormalizationVectorsTest {
    @Test
    fun `kotlin normalization matches every shared normalization case`() {
        val root = ObjectMapper().readTree(Files.readString(BoundsVectorFile.resolve()))
        val section =
            root["bound_normalization"]
                ?: error("the shared vector file carries no bound_normalization section")
        val cases = section["cases"] ?: error("bound_normalization has no cases")
        assertThat(cases.size())
            .describedAs("normalization cases (the Python side asserts the same file)")
            .isGreaterThanOrEqualTo(EXPECTED_CASES)

        val failures = mutableListOf<String>()
        val moved = mutableSetOf<Pair<String, String>>()
        for ((i, c) in cases.withIndex()) {
            val typeStr = c["type"].asText()
            val role = c["role"].asText()
            val raw = decodeHex(c["raw_hex"].asText())
            val want = decodeHex(c["stored_hex"].asText())
            val got = StatsSanity.normalizeBound(ColType.fromWire(typeStr), raw, lower = role == "lower")
            if (got == null || !got.contentEquals(want)) {
                failures +=
                    "case[$i] type=$typeStr role=$role raw=${c["raw_hex"].asText()}: " +
                    "expected ${c["stored_hex"].asText()}, got ${got?.toHex()}"
            }
            if (!raw.contentEquals(want)) moved += typeStr to role
        }
        assertThat(failures).describedAs("normalization disagreements").isEmpty()
        // A file that only ever moved lower bounds, or only doubles,
        // would satisfy every case above and still store an inverted
        // pair. Both roles of both floating types must MOVE somewhere.
        assertThat(moved)
            .containsExactlyInAnyOrder(
                "float" to "lower",
                "float" to "upper",
                "double" to "lower",
                "double" to "upper",
            )
    }

    @Test
    fun `an absent bound stays absent and a malformed one is left for the dropper`() {
        assertThat(StatsSanity.normalizeBound(ColType.DOUBLE, null, lower = true)).isNull()
        // Two bytes are not a double. Rewriting them as a canonical zero
        // would turn a bound StatsSanity DROPS into one it stores.
        val short = byteArrayOf(0, 0)
        assertThat(StatsSanity.normalizeBound(ColType.DOUBLE, short, lower = true)).isEqualTo(short)
    }

    private fun decodeHex(hex: String): ByteArray =
        ByteArray(hex.length / 2) { ((hex[it * 2].digitToInt(16) shl 4) or hex[it * 2 + 1].digitToInt(16)).toByte() }

    private fun ByteArray.toHex(): String = joinToString("") { "%02x".format(it) }

    private companion object {
        /**
         * Pinned like the codec vectors' count: a case lost to a bad
         * merge fails here instead of shrinking the gate in silence.
         */
        const val EXPECTED_CASES = 13
    }
}
