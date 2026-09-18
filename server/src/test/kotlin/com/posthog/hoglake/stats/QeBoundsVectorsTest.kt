package com.posthog.hoglake.stats

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.model.ColType
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import java.nio.file.Path
import java.util.Base64
import java.util.UUID

/**
 * Cross-language differential vectors (docs/fuzzing.md layer 2): the Python
 * codec (pyhoglake/bounds.py) emits canonical (type, type_params,
 * value, hex) vectors to pyhoglake/tests/vectors/bounds_vectors.json;
 * this test asserts the Kotlin encoder produces the same bytes for
 * every vector. Any divergence between the two codecs corrupts pruning
 * silently — here it is a test failure instead.
 *
 * Value conventions (from the file's own header, version 1):
 *  - boolean: "true" | "false"
 *  - int/date: decimal int32 string; long/time/timestamp/timestamptz:
 *    decimal int64 string
 *  - float/double: decimal string or "NaN"/"Infinity"/"-Infinity"; for
 *    NaN the HEX BYTES are authoritative (payload NaNs share the "NaN"
 *    token), so the JVM value is reconstructed from the hex bits and
 *    must re-encode to those exact bytes — this is precisely the
 *    raw-bits-preservation contract.
 *  - string: the string itself; uuid: canonical form; binary: base64
 *  - decimal: UNSCALED integer as decimal string
 *
 * A missing vector file FAILS this test. It used to skip, which made a
 * broken path indistinguishable from a passing cross-language gate —
 * the one failure mode this whole layer exists to rule out.
 */
class QeBoundsVectorsTest {
    @Test
    fun `kotlin encoder matches every python-generated vector`() {
        val vectorFile = BoundsVectorFile.resolve()
        val root = ObjectMapper().readTree(Files.readString(vectorFile))
        val vectors: JsonNode =
            when {
                root.isArray -> root
                root.has("vectors") && root["vectors"].isArray -> root["vectors"]
                else -> error("unrecognized vector file shape: expected array or {vectors: [...]}")
            }
        assertThat(vectors.size())
            .describedAs("vector count (pinned in both languages; see BoundsVectorFile.EXPECTED_COUNT)")
            .isEqualTo(BoundsVectorFile.EXPECTED_COUNT)

        val failures = mutableListOf<String>()
        var checked = 0
        for ((i, v) in vectors.withIndex()) {
            val typeStr = v["type"]?.asText() ?: error("vector[$i] has no type: $v")
            val hex = v["hex"]?.asText() ?: error("vector[$i] has no hex: $v")
            val valueStr = v["value"]?.asText() ?: error("vector[$i] has no value: $v")
            val type = ColType.fromWire(typeStr)
            val expected = decodeHex(hex)
            val value = jvmValue(type, valueStr, expected)
            val actual = IcebergSingleValue.encode(type, value)
            checked++
            if (!actual.contentEquals(expected)) {
                failures += "vector[$i] type=$typeStr value='$valueStr' " +
                    "(${v["note"]?.asText()}): python=${hexOf(expected)} kotlin=${hexOf(actual)}"
            }
        }
        assertThat(checked).isEqualTo(vectors.size())
        assertThat(failures)
            .describedAs("cross-language encoding divergences (checked %d vectors)", checked)
            .isEmpty()
    }

    // ---- value coercion per the file's conventions ------------------------

    private fun jvmValue(
        type: ColType,
        value: String,
        hexBytes: ByteArray,
    ): Any =
        when (type) {
            ColType.VARIANT -> error("VARIANT has no scalar bounds vector")
            ColType.BOOLEAN -> value.toBooleanStrict()
            // Everything that maps to Iceberg int carries a decimal int32
            // string; everything that maps to long (or to Iceberg
            // timestamp/timestamp_ns) carries a decimal int64 string in
            // the STORED unit — micros for timestamp_s/timestamp_ms,
            // nanos for timestamp_ns.
            ColType.INT8, ColType.INT16, ColType.UINT8, ColType.UINT16,
            ColType.INT, ColType.DATE,
            -> value.toInt()
            ColType.UINT32, ColType.LONG, ColType.TIME,
            ColType.TIMESTAMP_S, ColType.TIMESTAMP_MS, ColType.TIMESTAMP,
            ColType.TIMESTAMP_NS, ColType.TIMESTAMPTZ,
            -> value.toLong()
            ColType.FLOAT ->
                when (value) {
                    // NaN: the hex is authoritative for the payload; reconstruct
                    // the exact bit pattern and demand it round-trips.
                    "NaN" ->
                        Float.fromBits(
                            ByteBuffer.wrap(hexBytes).order(ByteOrder.LITTLE_ENDIAN).int,
                        )
                    "Infinity" -> Float.POSITIVE_INFINITY
                    "-Infinity" -> Float.NEGATIVE_INFINITY
                    else -> value.toFloat()
                }
            ColType.DOUBLE ->
                when (value) {
                    "NaN" ->
                        Double.fromBits(
                            ByteBuffer.wrap(hexBytes).order(ByteOrder.LITTLE_ENDIAN).long,
                        )
                    "Infinity" -> Double.POSITIVE_INFINITY
                    "-Infinity" -> Double.NEGATIVE_INFINITY
                    else -> value.toDouble()
                }
            ColType.STRING, ColType.JSON -> value
            ColType.UUID_T -> UUID.fromString(value)
            ColType.BINARY -> Base64.getDecoder().decode(value)
            // uint64 shares decimal's carrier: the unsigned value as a
            // BigInteger, which is also its unscaled decimal(20,0) value.
            ColType.UINT64, ColType.DECIMAL -> BigInteger(value)
            // Containers never appear in the bounds vector file — they
            // have no single-value encoding — so reaching this arm means
            // someone put one there.
            ColType.LIST, ColType.STRUCT, ColType.MAP ->
                error("nested container '${type.wire}' has no bounds vector")
        }

    // ---- hex helpers -----------------------------------------------------

    private fun decodeHex(hex: String): ByteArray {
        require(hex.length % 2 == 0) { "odd hex length in '$hex'" }
        return ByteArray(hex.length / 2) { i ->
            hex.substring(i * 2, i * 2 + 2).toInt(16).toByte()
        }
    }

    private fun hexOf(bytes: ByteArray): String = bytes.joinToString("") { "%02x".format(it) }
}

/**
 * Locating the shared vector file, for both cross-language test classes.
 *
 * The plain relative path only works when the JVM's working directory is
 * `server/`, which is a gradle detail no test should depend on: when it
 * moves, the file "does not exist" and — before this — the gate quietly
 * stopped running. So resolution falls back to walking up from user.dir,
 * and exhausting both strategies is a failure naming every path tried.
 */
internal object BoundsVectorFile {
    /**
     * Pinned exactly so a vector lost to a bad merge fails instead of
     * shrinking coverage in silence. Update DELIBERATELY when vectors are
     * added, together with the identical pin in pyhoglake/tests/qe_vectors.py.
     */
    const val EXPECTED_COUNT = 109

    private const val REPO_RELATIVE = "pyhoglake/tests/vectors/bounds_vectors.json"

    fun resolve(): Path {
        val fromCwd = Path.of("..").resolve(REPO_RELATIVE).normalize()
        if (Files.exists(fromCwd)) return fromCwd

        val tried = mutableListOf("cwd-relative: ${fromCwd.toAbsolutePath()}")
        var dir: Path? = Path.of(System.getProperty("user.dir")).toAbsolutePath()
        while (dir != null) {
            val candidate = dir.resolve(REPO_RELATIVE)
            if (Files.exists(candidate)) return candidate
            tried += "walk-up: $candidate"
            dir = dir.parent
        }
        throw AssertionError(
            "cross-language bounds vector file not found — the Python/Kotlin codec parity gate " +
                "cannot run. Tried:\n" + tried.joinToString("\n") { "  $it" },
        )
    }
}
