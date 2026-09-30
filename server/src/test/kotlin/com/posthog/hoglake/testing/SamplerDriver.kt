package com.posthog.hoglake.testing

import com.posthog.hoglake.compaction.CompactionGrouping
import com.posthog.hoglake.service.MaintenanceSummarySampler
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.useHandleUnchecked

/**
 * Run the maintenance sampler until every catalog has PUBLISHED a
 * generation — the state a head table GET's totals come from (#232).
 *
 * WHY A TEST HELPER AND NOT A LOOP. `App.startBackground()` is not
 * called by any test fixture, so in a test the sampler never ticks and
 * `published_generation` never advances: a head read reports its totals
 * absent, correctly and forever. A test whose subject is the SAMPLED
 * path therefore has to drive the sampler, and a test whose subject is
 * the manifest must not (it pins a snapshot instead —
 * [tableWithExactTotals]).
 *
 * `next_batch_at` is forced to now because the sampler paces itself with
 * it (`HOGLAKE_MAINTENANCE_SUMMARY_REFRESH_SECONDS`): without the nudge
 * a second call inside the same test would find nothing due and publish
 * nothing, which reads as "the sample did not move" rather than as "the
 * fixture did not ask it to".
 *
 * The policy values are the sampler's compaction knobs
 * (`targetBytes`/min/max) and they do not matter here: they scale
 * `small_count`, `small_bytes` and `selected`, none of which the
 * per-table totals read touches — which is also why
 * `CatalogService.getTable` applies no policy check to the sample it
 * reads. A test asserting DEBT must supply the policy under test.
 */
fun publishMaintenanceSample(
    jdbi: Jdbi,
    targetBytes: Long = 512L * 1024 * 1024,
) {
    jdbi.useHandleUnchecked { it.execute("UPDATE hog_maintenance_summary SET next_batch_at = now()") }
    val sampler =
        MaintenanceSummarySampler(
            jdbi,
            targetBytes,
            CompactionGrouping.DEFAULT_MIN_INPUT_FILES,
            CompactionGrouping.DEFAULT_MAX_INPUT_FILES,
            // One hour, so the sampler does not start a SECOND generation
            // in the middle of this call: `runOnce` returning false has
            // to mean "the published generation is complete", not "the
            // next one is not due yet".
            3600,
        )
    // runOnce() == false means nothing was due; each true is one bounded
    // page of one catalog's scan, which is how the loop advances.
    var guard = 0
    while (sampler.runOnce()) {
        check(++guard < 10_000) { "sampler did not finish a generation in $guard ticks" }
    }
}
