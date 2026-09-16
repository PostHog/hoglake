package com.posthog.hoglake.fuzz

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.code_intelligence.jazzer.junit.FuzzTest
import java.nio.file.Files
import java.nio.file.Path

/**
 * Fuzz target (fuzzing.md layer 4, phase-2 nested): the reader/rewriter
 * AGREEMENT surface.
 *
 * One execution generates a random VALID catalog column tree, derives a
 * parquet schema from it (canonical, or mutated in every way phase 2
 * has a guard for), writes a small data file, and runs both
 * `FooterStats.aggregate` and `ParquetRewriter.rewrite` over it.
 *
 * Contract under test (see [NestedAgreement] for the oracle bodies):
 *  - neither surface throws anything but its typed refusal — the reader
 *    is total, the rewriter refuses only with `UnconvertibleSchemaException`;
 *  - stats never appear for a field id the catalog does not own, bounds
 *    satisfy lower <= upper under the catalog type's ordering;
 *  - a leaf the rewriter copied real values into produced a stats row on
 *    the read side (the two surfaces bind the same files);
 *  - per-leaf non-null value counts are conserved across an accepted
 *    rewrite.
 *
 * The bulk campaign runs through [NestedFuzzSoak] (deterministic seeds,
 * replayable); this target exists so the same oracles get libFuzzer
 * coverage feedback and live in the normal `:test` corpus replay.
 */
class NestedAgreementFuzzTest {
    @FuzzTest(maxDuration = "300s")
    fun readerAndRewriterAgree(data: FuzzedDataProvider) {
        val findings = ArrayList<Finding>()
        NestedAgreement.runOne(FdpEntropy(data), tmp) { findings.add(it) }
        if (findings.isNotEmpty()) {
            val first = findings.first()
            throw IllegalStateException("nested agreement violated: $first", first.cause)
        }
    }

    private companion object {
        val tmp: Path = Files.createTempDirectory("hoglake-nested-agreement-fuzz")
    }
}
