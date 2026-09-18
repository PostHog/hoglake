package com.posthog.hoglake.fuzz

import com.code_intelligence.jazzer.junit.FuzzTest
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.stats.BoundWire
import com.posthog.hoglake.wireObjectMapper

/**
 * Fuzz target for the read-time bounds decode surface as the endpoints
 * expose it (GET .../files/{fileId}/stats and POST
 * /v1/debug/decode-bound): [BoundWire.render] over an arbitrary type ×
 * scale × arbitrary bytes.
 *
 * Contract under test:
 *  - render either produces a value or throws
 *    [IllegalArgumentException] — the one exception family the
 *    endpoints map (debug route -> named 422; stats route -> JSON
 *    null). Anything else escaping would be a 500 on attacker-shaped
 *    input, which is exactly what this surface must never do;
 *  - a produced node must survive the actual wire: serialize through
 *    the production ObjectMapper and reparse without throwing (a node
 *    that serializes into invalid JSON is a crash deferred to the
 *    client);
 *  - no crash/OOM: temporal rendering in particular must be total over
 *    the full int64 range (year ±292M in micros), and the decimal path
 *    total over any unscaled magnitude at any legal scale.
 *
 * Input shape: byte 0 selects the ColType (modulo the vocabulary size
 * — so, like the codec targets, the corpus must be REGENERATED when
 * the enum changes); byte 1 selects the decimal scale (modulo 39, the
 * legal 0..38 range — [BoundWire.scaleOf]'s own domain is unit-tested,
 * not fuzzed, because catalog type_params reach it pre-validated);
 * the rest is the encoding under test.
 */
class BoundWireFuzzTest {
    private val mapper = wireObjectMapper()

    @FuzzTest(maxDuration = "120s")
    fun renderIsTotalOrRefusesTyped(data: ByteArray) {
        if (data.size < 2) return
        val type = ColType.entries[(data[0].toInt() and 0xFF) % ColType.entries.size]
        val scale = (data[1].toInt() and 0xFF) % 39
        val payload = data.copyOfRange(2, data.size)

        val node =
            try {
                BoundWire.render(type, scale, payload)
            } catch (_: IllegalArgumentException) {
                return // the documented refusal; anything else propagates as a finding
            }

        // The rendered node must be real JSON on the real wire.
        val text = mapper.writeValueAsString(node)
        val reparsed = mapper.readTree(text)
        checkNotNull(reparsed) { "rendered ${type.wire} value serialized to unparseable JSON: $text" }
    }
}
