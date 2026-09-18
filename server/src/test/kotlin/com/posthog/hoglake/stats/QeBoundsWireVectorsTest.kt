package com.posthog.hoglake.stats

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.wireObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.file.Files

/**
 * The JSON-rendering layer over the shared cross-language vectors: every
 * vector in pyhoglake/tests/vectors/bounds_vectors.json carries a `wire`
 * field (the decoded bound as JSON, per the file's wire_conventions
 * header) or a `wire_refused` marker (a value the JSON layer refuses —
 * NaN bounds), and [BoundWire.render] must answer each one.
 *
 * Comparison is per family, because token text is not portable across
 * languages for the floating types (both sides print shortest
 * round-trip, with different exponent spelling):
 *
 *  - float/double compare by VALUE, against the vector's own `value`
 *    string (bit-exact through Float/Double.parse — including the sign
 *    of zero, which a BigDecimal comparison would erase);
 *  - decimal compares numerically (compareTo) — the exact-scale plain
 *    token is pinned in [BoundWireTest] and, token-exactly, by the
 *    Python oracle (pyhoglake/tests/wire_oracle.py, which generates and
 *    checks every wire cell); this reader parses the file's number
 *    tokens through doubles, so it compares values;
 *  - every integer family compares as exact BigIntegers AND must render
 *    as an integral JSON number;
 *  - everything else (strings, booleans, temporal strings, sentinels)
 *    compares as the exact JsonNode.
 *
 * The `bound_normalization` cases are replayed too: the raw and stored
 * encodings of the signed zeros must render as the tokens their bits
 * say — a stored -0.0 lower bound reaches the wire with its sign.
 *
 * Counts are pinned (109 vectors, 13 cases) like every other reader of
 * this file, so a silently shrunken file cannot pass.
 */
class QeBoundsWireVectorsTest {
    private val mapper = ObjectMapper()
    private val wireMapper = wireObjectMapper()

    private fun scaleOf(vector: JsonNode): Int = vector["type_params"]?.get("scale")?.asInt() ?: 0

    @Test
    fun `every vector renders to its pinned wire form or is refused`() {
        val root = mapper.readTree(Files.readString(BoundsVectorFile.resolve()))
        val vectors = root["vectors"]
        assertThat(vectors.size()).isEqualTo(BoundsVectorFile.EXPECTED_COUNT)

        val failures = mutableListOf<String>()
        var rendered = 0
        var refused = 0
        for ((i, v) in vectors.withIndex()) {
            val type = ColType.fromWire(v["type"].asText())
            val bytes = unhex(v["hex"].asText())
            val scale = scaleOf(v)
            val label = "vector[$i] ${type.wire} value='${v["value"].asText().take(24)}'"

            if (v.has("wire_refused")) {
                refused++
                try {
                    val node = BoundWire.render(type, scale, bytes)
                    failures += "$label: expected refusal '${v["wire_refused"].asText()}', rendered $node"
                } catch (_: IllegalArgumentException) {
                    // the documented refusal — the only legal outcome
                }
                continue
            }

            rendered++
            val expected = v["wire"] ?: error("$label carries neither wire nor wire_refused")
            val node =
                try {
                    BoundWire.render(type, scale, bytes)
                } catch (e: IllegalArgumentException) {
                    failures += "$label: refused a value with a pinned wire form: ${e.message}"
                    continue
                }
            // Whatever the family, the node must survive the actual wire:
            // serialize -> reparse without throwing.
            wireMapper.readTree(wireMapper.writeValueAsString(node))
            if (!matches(type, v, expected, node)) {
                failures += "$label: rendered $node != expected $expected"
            }
        }

        // Every vector is accounted for — rendered or deliberately refused.
        assertThat(rendered + refused).isEqualTo(BoundsVectorFile.EXPECTED_COUNT)
        assertThat(failures).describedAs("wire divergences (%d rendered)", rendered).isEmpty()
    }

    private fun matches(
        type: ColType,
        vector: JsonNode,
        expected: JsonNode,
        node: JsonNode,
    ): Boolean =
        when (type) {
            ColType.FLOAT -> {
                val value = vector["value"].asText()
                when (value) {
                    "Infinity", "-Infinity" -> node.isTextual && node.asText() == value
                    else ->
                        node.isFloat &&
                            node.floatValue().toRawBits() == value.toFloat().toRawBits()
                }
            }
            ColType.DOUBLE -> {
                val value = vector["value"].asText()
                when (value) {
                    "Infinity", "-Infinity" -> node.isTextual && node.asText() == value
                    else ->
                        node.isDouble &&
                            node.doubleValue().toRawBits() == value.toDouble().toRawBits()
                }
            }
            ColType.DECIMAL ->
                node.isNumber && expected.isNumber &&
                    node.decimalValue().compareTo(expected.decimalValue()) == 0
            ColType.INT8, ColType.INT16, ColType.INT, ColType.LONG,
            ColType.UINT8, ColType.UINT16, ColType.UINT32, ColType.UINT64,
            ->
                node.isIntegralNumber && expected.isIntegralNumber &&
                    node.bigIntegerValue() == expected.bigIntegerValue()
            else -> node == expected
        }

    @Test
    fun `normalization cases render raw and stored bytes as their pinned tokens`() {
        val root = mapper.readTree(Files.readString(BoundsVectorFile.resolve()))
        val cases = root["bound_normalization"]["cases"]
        assertThat(cases.size()).isEqualTo(13)

        val failures = mutableListOf<String>()
        for ((i, c) in cases.withIndex()) {
            val type = ColType.fromWire(c["type"].asText())
            for ((hexField, wireField) in listOf("raw_hex" to "raw_wire", "stored_hex" to "stored_wire")) {
                val node = BoundWire.render(type, 0, unhex(c[hexField].asText()))
                // These 13 values (zeros of both signs, small integers)
                // print identically in every shortest-round-trip
                // language, so the SERIALIZED TOKEN is comparable — which
                // is what pins the sign of -0.0 on the wire.
                val got = wireMapper.writeValueAsString(node)
                val want = wireMapper.writeValueAsString(c[wireField])
                if (got != want) {
                    failures += "case[$i] ${type.wire} ${c["role"].asText()} $hexField: $got != $want"
                }
            }
        }
        assertThat(failures).isEmpty()
    }

    private fun unhex(hex: String): ByteArray {
        require(hex.length % 2 == 0) { "odd hex length in '$hex'" }
        return ByteArray(hex.length / 2) { i -> hex.substring(i * 2, i * 2 + 2).toInt(16).toByte() }
    }
}
