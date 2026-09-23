package com.posthog.hoglake.fuzz

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.code_intelligence.jazzer.junit.FuzzTest
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.service.CorruptDefinitionException
import com.posthog.hoglake.service.ReplacementTarget
import com.posthog.hoglake.service.TableCreationDefinition
import com.posthog.hoglake.service.TableCreationDefinitionCodec
import java.util.UUID

/**
 * Fuzz target (docs/fuzzing.md layer 4): the versioned durable format for a
 * prepared table creation — `TableCreationDefinitionCodec.encode` /
 * `decode`, versions 0 through 6.
 *
 * This codec is the only thing standing between a stored receipt and the
 * table that gets published from it, and it is read by binaries OLDER
 * than the one that wrote it (a rolling deploy), which is why the
 * version choice is part of the contract rather than an implementation
 * detail.
 *
 * Contracts under test:
 *
 *  - **decode is total over arbitrary bytes.** Whatever a receipt row
 *    holds — truncated JSON, a version from the future, a column whose
 *    `type` is a number — the only thing that comes out is
 *    [CorruptDefinitionException], naming the receipt. Anything else
 *    (the raw NPE from a chained `.asText()`, an
 *    IllegalArgumentException out of an enum lookup) is a 500 with a
 *    stack trace pointing at the reader instead of at the row, which is
 *    exactly what the typed refusal exists to prevent.
 *  - **encode round-trips.** `decode(encode(d)) == d` for every
 *    definition the generator can build, nested columns, partition and
 *    sort specs, comment and properties included.
 *  - **encode picks the LOWEST version that can carry the definition.**
 *    Asserted against a restatement of the documented version
 *    CAPABILITIES — each feature's first version, maximised — rather
 *    than against the codec's own if/else ladder. An old reader refuses
 *    a version it does not know, so emitting 6 for a plain scalar table
 *    makes a receipt unreadable to a server that would have handled it
 *    perfectly well, and emitting 1 for a nested one makes the old
 *    reader silently DROP the children and publish a different table.
 *
 * **Input shape.** There is none: the whole input is the stored receipt,
 * AND the same bytes drive the definition generator, read left to right
 * as a plain byte stream. Splitting them was the obvious design and it
 * was wrong — a length-prefixed receipt means every mutation that
 * changes the receipt's length moves the boundary, so libFuzzer's
 * feedback stops pointing at the byte it changed and the decode arm
 * stops searching (measured: coverage flat at 574 across 735k
 * executions, while a hand-written `type_params: 5` receipt that the
 * oracle does catch went unfound for 30 seconds). With no boundary,
 * every byte the fuzzer touches reaches both arms, and a committed seed
 * is simply a readable receipt.
 */
class TableCreationDefinitionCodecFuzzTest {
    @FuzzTest(maxDuration = "300s")
    fun decodeIsTypedAndEncodePicksTheLowestVersion(data: FuzzedDataProvider) {
        val raw = data.consumeRemainingAsBytes()
        if (raw.size > MAX_INPUT_BYTES) return

        // ---- phase 1: decode over whatever the row holds -----------------
        val stored = String(raw, Charsets.UTF_8)
        try {
            TableCreationDefinitionCodec.decode(stored, "fuzz receipt")
        } catch (t: CorruptDefinitionException) {
            check(t.message!!.startsWith("fuzz receipt: ")) {
                "a corrupt receipt must name the receipt: ${t.message?.take(120)}"
            }
        } catch (t: Exception) {
            throw IllegalStateException(
                "decode escaped with ${t.javaClass.name} instead of CorruptDefinitionException: ${t.message}",
                t,
            )
        }

        // ---- phase 2: encode/decode round trip and version choice --------
        val definition = generateDefinition(ByteStream(raw))
        val encoded = TableCreationDefinitionCodec.encode(definition)
        val decoded = TableCreationDefinitionCodec.decode(encoded, "fuzz receipt")
        check(decoded == definition) {
            "round trip changed the definition\n  in : $definition\n  out: $decoded\n  blob: ${encoded.take(400)}"
        }
        val written = MAPPER.readTree(encoded)["version"].asInt()
        val lowest = lowestVersionFor(definition)
        check(written == lowest) {
            "encoded at version $written; the lowest version that can carry this definition is $lowest " +
                "(${featureSummary(definition)})"
        }
    }

    /**
     * The version ladder restated from the CAPABILITY each version added
     * (the codec's KDoc): 1 is flat scalars in wire spellings, 2 added
     * `children`, 3 the replacement guard, 4 initial partition fields,
     * 5 initial sort fields, 6 comments and custom properties. The
     * versions are cumulative, so the lowest version that can express a
     * definition is the HIGHEST first-version among the features it
     * actually uses — which is a different statement from the codec's
     * descending if/else, and that is the point of writing it here.
     */
    private fun lowestVersionFor(d: TableCreationDefinition): Int =
        maxOf(
            FLAT_VERSION,
            if (d.columns.any { anyChildren(it) }) 2 else FLAT_VERSION,
            if (d.replacement != null) 3 else FLAT_VERSION,
            if (d.partitionFields.isNotEmpty()) 4 else FLAT_VERSION,
            if (d.sortFields.isNotEmpty()) 5 else FLAT_VERSION,
            if (d.comment != null || d.properties.isNotEmpty() || d.columns.any { anyComment(it) }) 6 else FLAT_VERSION,
        )

    /**
     * A DECLARED `children` list is version 2's feature, an empty one
     * included; a null is its absence. No recursion is needed, because a
     * descendant can only exist under a declared list, so the subtree
     * answer is already the node's own.
     */
    private fun anyChildren(c: ColumnDef): Boolean = c.children != null

    private fun anyComment(c: ColumnDef): Boolean = c.comment != null || c.children?.any { anyComment(it) } == true

    private fun featureSummary(d: TableCreationDefinition): String =
        "children=${d.columns.any { anyChildren(it) }} replacement=${d.replacement != null} " +
            "partition=${d.partitionFields.size} sort=${d.sortFields.size} " +
            "comment=${d.comment != null} properties=${d.properties.size} " +
            "columnComments=${d.columns.any { anyComment(it) }}"

    // ---- generator -------------------------------------------------------

    private fun generateDefinition(e: ByteStream): TableCreationDefinition =
        TableCreationDefinition(
            namespace = e.text(MAX_NAME_BYTES),
            name = e.text(MAX_NAME_BYTES),
            // `until`, so an EMPTY column list is reachable: `"columns":[]`
            // is a receipt the codec writes and has to read back.
            columns = (0 until e.int(0, MAX_COLUMNS)).map { generateColumn(e, MAX_COLUMN_DEPTH) },
            replacement =
                if (e.bool()) {
                    ReplacementTarget(
                        if (e.bool()) UUID(e.u32().toLong(), e.u32().toLong()) else null,
                        // Non-negative on purpose: decode refuses a
                        // negative read_snapshot, so a generated one
                        // would be asserting that encode can write a
                        // receipt it cannot read back — a real question,
                        // but one about the SERVICE's validation, and
                        // TableCreationServiceTest is where it belongs.
                        e.u32().toLong(),
                    )
                } else {
                    null
                },
            partitionFields =
                (0 until e.int(0, 2)).map {
                    PartitionFieldDef(
                        e.u32().toLong() + 1,
                        Transform.entries[e.int(0, Transform.entries.size - 1)],
                        if (e.bool()) e.u16() else null,
                    )
                },
            sortFields =
                (0 until e.int(0, 2)).map {
                    SortFieldDef(
                        e.u32().toLong() + 1,
                        SortDirection.entries[e.int(0, SortDirection.entries.size - 1)],
                        NullOrder.entries[e.int(0, NullOrder.entries.size - 1)],
                    )
                },
            comment = if (e.bool()) e.text(MAX_NAME_BYTES) else null,
            properties = (0 until e.int(0, 2)).associate { e.text(MAX_NAME_BYTES) to e.text(MAX_NAME_BYTES) },
        )

    private fun generateColumn(
        e: ByteStream,
        depthLeft: Int,
    ): ColumnDef {
        val type = ColType.entries[e.int(0, ColType.entries.size - 1)]
        // The codec stores a column tree; it does not validate one
        // (ColumnTrees does, at a different layer), so children are
        // generated for their own sake and not only under containers.
        val children =
            if (depthLeft > 0 && e.bool()) (0 until e.int(0, 2)).map { generateColumn(e, depthLeft - 1) } else null
        return ColumnDef(
            name = e.text(MAX_NAME_BYTES),
            type = type,
            typeParams = generateTypeParams(e, type),
            nullable = e.bool(),
            children = children,
            comment = if (e.bool()) e.text(MAX_NAME_BYTES) else null,
        )
    }

    /**
     * `type_params` values are Int and String only, deliberately.
     * Jackson reads an untyped JSON number back as the narrowest box
     * that holds it, so a generated `Long` of small value returns as an
     * `Integer` and fails the round trip on a difference the codec
     * never had — the harness would be asserting Jackson's boxing, not
     * the format.
     */
    private fun generateTypeParams(
        e: ByteStream,
        type: ColType,
    ): Map<String, Any?>? =
        when {
            type == ColType.DECIMAL -> {
                val precision = e.int(1, MAX_DECIMAL_PRECISION)
                mapOf("precision" to precision, "scale" to e.int(0, precision))
            }
            e.bool() -> mapOf(e.text(MAX_NAME_BYTES) to if (e.bool()) e.u16() else e.text(MAX_NAME_BYTES))
            else -> null
        }

    /**
     * A strictly left-to-right reader over the raw input.
     *
     * Reading the input directly rather than through the provider's
     * mixed-end accessors (`consumeBytes` from the front,
     * `consumeInt`/`consumeBoolean` from the back) is what lets the same
     * bytes be a readable receipt and a generator tape at once.
     * Exhaustion answers zero rather than falling back to an RNG, so a
     * short input is a SHORT definition and stays reproducible.
     */
    private class ByteStream(private val raw: ByteArray) {
        private var pos = 0

        fun take(n: Int): ByteArray {
            if (n <= 0) return ByteArray(0)
            val out = ByteArray(n)
            for (i in 0 until n) out[i] = if (pos < raw.size) raw[pos++] else 0
            return out
        }

        fun u8(): Int = take(1)[0].toInt() and 0xFF

        fun u16(): Int = (u8() shl 8) or u8()

        fun u32(): Int = ((u16() shl 16) or u16()) and Int.MAX_VALUE

        fun bool(): Boolean = (u8() and 1) == 1

        fun int(
            lo: Int,
            hi: Int,
        ): Int = if (hi <= lo) lo else lo + u8() % (hi - lo + 1)

        /** A UTF-8 string of a length-prefixed byte run; invalid bytes map to replacement chars. */
        fun text(maxBytes: Int): String = String(take(u8() % (maxBytes + 1)), Charsets.UTF_8)
    }

    private companion object {
        const val FLAT_VERSION = 1
        const val MAX_INPUT_BYTES = 1 shl 20
        const val MAX_COLUMNS = 3
        const val MAX_COLUMN_DEPTH = 3
        const val MAX_NAME_BYTES = 12
        const val MAX_DECIMAL_PRECISION = 38

        /** Only to read the `version` field back out of an encoded blob. */
        val MAPPER = ObjectMapper()
    }
}
