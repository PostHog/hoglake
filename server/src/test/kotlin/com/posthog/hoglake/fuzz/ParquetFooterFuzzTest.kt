package com.posthog.hoglake.fuzz

import com.code_intelligence.jazzer.junit.FuzzTest
import com.posthog.hoglake.hydrator.CatalogColumn
import com.posthog.hoglake.hydrator.FooterParse
import com.posthog.hoglake.hydrator.FooterSplitOffsets
import com.posthog.hoglake.hydrator.FooterStats
import com.posthog.hoglake.memoryInput
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.SplitOffsets
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Fuzz target (docs/fuzzing.md layer 4, target c): the parquet footer parse
 * the hydrator runs on writer-supplied bytes, plus [FooterStats] over
 * whatever parses.
 *
 * The harness mirrors Hydrator.parseFooter exactly: [FooterParse.parse]
 * over an in-memory `InputFile` — the same SHAPE production uses (a byte
 * range, never a file), so the contract below is tested where it is
 * enforced. That wrapper is the shared `memoryInput` helper rather than
 * a private copy, so the nested campaigns and this target share one
 * implementation of the bounds checks.
 *
 * Input shape: bytes starting with "PAR1" are treated as a whole parquet
 * file; anything else is wrapped as the thrift footer of a synthetic
 * file (PAR1 + data + LE len + PAR1) so the fuzzer spends its time
 * inside the thrift decode, not hunting for magic bytes.
 *
 * Contract under test:
 *  - the footer parse fails as an IOException and nothing else.
 *    [FooterParse] collapses parquet-java's three refusal styles (typed
 *    IOException, deliberate bare RuntimeException, and the accidental
 *    runtime exceptions it lets escape from unguarded optional thrift
 *    fields) into that one category, so this assertion needs no stack
 *    inspection and no per-class carve-out — which is the point: the
 *    previous frame-matching version silently stopped matching once
 *    HotSpot began fast-throwing the hot NPE with an empty stack (#15).
 *    Never a hang, never unbounded allocation (input capped at 1 MiB;
 *    thrift's own limits govern the rest);
 *  - when the footer DOES parse, FooterStats.missingFieldIds /
 *    usesFieldIds / aggregate are total: they never throw, whatever the
 *    schema shape (that is the hydrator's "bounds NULL, never guessed"
 *    contract — the TransformGlobalStatsRow crash class from the defect
 *    ledger), and so is FooterSplitOffsets.of, whose list — when it
 *    gives one — must honour the split_offsets contract in full.
 */
class ParquetFooterFuzzTest {
    @FuzzTest(maxDuration = "180s")
    fun footerParseFailsTypedAndStatsAreTotal(data: ByteArray) {
        if (data.size > MAX_INPUT_BYTES) return
        val file = if (startsWithMagic(data)) data else wrapAsFooter(data)

        val footer: ParquetMetadata =
            try {
                FooterParse.parse(memoryInput(file))
            } catch (e: Exception) {
                checkAllowedParseFailure(e)
                return
            }

        // Parsed: the pure stats layer must be total over any schema shape.
        val schema = footer.fileMetaData.schema
        FooterStats.missingFieldIds(schema)
        FooterStats.usesFieldIds(schema)
        FooterStats.aggregate(footer, CATALOG_COLUMNS, "fuzz://footer")

        // Row-group offsets: total too (never throws on a parsed footer),
        // and NEVER a list that breaks the contract a reader relies on —
        // checked here from first principles, not by the shared
        // validator the code under test already calls.
        FooterSplitOffsets.of(footer, file.size.toLong())?.let { offsets ->
            check(offsets.isNotEmpty() && offsets.size <= SplitOffsets.MAX_ROW_GROUPS) { "bad size ${offsets.size}" }
            check(offsets.size == footer.blocks.size) { "partial list: ${offsets.size} of ${footer.blocks.size}" }
            check(offsets.first() >= 0 && offsets.last() < file.size) { "out of range: $offsets" }
            check(offsets.zipWithNext().all { (a, b) -> a < b }) { "not strictly increasing: $offsets" }
        }
    }

    private fun checkAllowedParseFailure(e: Exception) {
        // One rule, no exceptions to it: the parse refuses with an
        // IOException. Anything else escaping FooterParse is a hole in the
        // translation, which is exactly what this target exists to find.
        if (e !is IOException) {
            throw IllegalStateException("footer parse escaped with ${e.javaClass.name}: ${e.message}", e)
        }
    }

    private fun startsWithMagic(data: ByteArray): Boolean =
        data.size >= 4 && data[0] == 'P'.code.toByte() && data[1] == 'A'.code.toByte() &&
            data[2] == 'R'.code.toByte() && data[3] == '1'.code.toByte()

    /** PAR1 + data-as-thrift-footer + LE footer length + PAR1. */
    private fun wrapAsFooter(data: ByteArray): ByteArray {
        val out = ByteBuffer.allocate(4 + data.size + 8).order(ByteOrder.LITTLE_ENDIAN)
        out.put(MAGIC)
        out.put(data)
        out.putInt(data.size)
        out.put(MAGIC)
        return out.array()
    }

    private companion object {
        val MAGIC = byteArrayOf(0x50, 0x41, 0x52, 0x31) // "PAR1"
        const val MAX_INPUT_BYTES = 1 shl 20

        /** One catalog column per ColType so aggregate exercises every decode arm. */
        val CATALOG_COLUMNS: List<CatalogColumn> =
            ColType.entries.mapIndexed { i, type ->
                CatalogColumn(
                    fieldId = (i + 1).toLong(),
                    name = "c_${type.wire}",
                    type = type,
                    decimalScale = if (type == ColType.DECIMAL) 2 else null,
                )
            }
    }
}
