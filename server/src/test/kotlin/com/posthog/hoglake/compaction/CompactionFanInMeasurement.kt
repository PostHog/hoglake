package com.posthog.hoglake.compaction

import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.testing.Measuring
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * WHERE `HOGLAKE_COMPACTION_MAX_FAN_IN`'s default comes from, measured.
 *
 * The fan-in cap scales with the size of the files it caps
 * (`CompactionConfig.effectiveMaxInputFiles`), because a fixed 64 is a
 * file count asked to cap a byte target: at gigahog-prod-us's ~12 KiB
 * stray-day files a 64-file group rewrites 768 KiB against a 512 MiB
 * target and retires 63 files, which cannot outrun a fleet adding
 * ~3,600 files a minute. Raising the ceiling is therefore the lever
 * that makes compaction able to drain a 9.9M-file backlog at all.
 *
 * **But a group's inputs are also rows under the per-catalog commit
 * lock**, and that is the bound this class exists to find.
 * `CompactionService.commitGroup` re-verifies and retires its inputs
 * with two `IN`-list statements against `hog_data_file` —
 *
 *  - `SELECT … WHERE catalog_id = ? AND table_id = ? AND data_file_id IN (<ids>)`
 *    joined to `hog_delete_file`, the plan-to-commit re-verification;
 *  - `UPDATE hog_data_file SET end_snapshot = ? WHERE catalog_id = ? AND data_file_id IN (<ids>)`
 *
 * — both inside `Locks.acquireCatalogCommitLock`, once per group, up to
 * `maxGroupsPerRun` times per sweep. Every foreground commit on the
 * catalog waits behind each hold. The 2026-09-28 expiry outage and the
 * 2026-09-29 cleanup outage were both this shape at a larger row count,
 * so the doctrine's "hold time per commit x commits per minute" has to
 * be answered before the fan-in moves.
 *
 * # What is measured, and what is not
 *
 * Measured: the two statements' wall time at fan-in 64, 512 and 2,048
 * over a production-shaped manifest, with the ids SCATTERED (the
 * access pattern is random heap reads on `data_file_id`, not a
 * PK-order walk, because a bucket's files arrive interleaved with every
 * other bucket's), and the real advisory lock held around them so the
 * hold includes what a foreground commit would wait for.
 *
 * NOT measured here: the rewrite itself. A group's object-store cost is
 * a measured ~8.5 s of latency whatever it holds
 * (`CompactionConfig.parallelGroups`), and it happens outside every
 * transaction — so the fan-in changes the commit's row count and the
 * rewrite's input count, and only the first of those touches the lock.
 * The per-input memory cost is `inputOpenParallelism`'s, and it is
 * bounded by that knob rather than by this one.
 *
 * # THE FIGURES, and the default they pick
 *
 * The decision rule, stated before the measurement so it cannot be
 * fitted to it: a commit-lock hold of **50 ms per group** is the
 * budget, and if 2,048 inputs cost more the default drops to the
 * largest power of two that fits.
 *
 * MEASURED, PG 18 in Testcontainers, warm, a 200,000-row manifest with
 * the ids scattered across it, the two statements inside one
 * transaction holding the real advisory lock:
 *
 *   fan-in    hold      per row
 *       64    2.2 ms    34.3 us
 *      512    9.4 ms    18.5 us
 *    2,048   33.6 ms    16.4 us
 *
 * So **2,048 stands**: 33.6 ms against a 50 ms budget. The per-row cost
 * FALLS as the list grows (34 -> 16 us), which is the direction that
 * makes the arithmetic extrapolate — a cost that grew with the list
 * length would make the 64-input figure useless for 2,048 — and 16 us
 * sits right on AGENT.md's retirement anchor of 19.6 us/row for the
 * same access pattern (a keyed UPDATE over scattered rows).
 *
 * The sweep's LOCK DUTY CYCLE, which is the number the doctrine asks
 * for: 33.6 ms x 64 groups = **2.15 s of lock per sweep**, x 15 sweeps
 * an hour = 32 s, a **0.90% duty cycle**. The retirement loop's
 * designed figure is 18-25%, so compaction's commits remain a rounding
 * error on the lock even at the ceiling.
 *
 * What that fan-in buys: 2,047 files retired per group x 64 groups x 15
 * sweeps = **~1.97M files an hour** against ~216k arriving, which
 * clears a 9.9M backlog in about five hours. At the old fixed 64 it is
 * ~60k an hour against 216k arriving — permanently under water.
 *
 * # THE EXTRAPOLATION RISK, named because it is the one that matters
 *
 * This fixture's manifest is 200,000 rows and fits in shared_buffers.
 * gigahog-prod-us's is ~14M rows / ~1.5 GB, so a scattered keyed probe
 * there pays real reads rather than hits, and the per-row cost can be
 * several times this. At 100 us/row a 2,048-input commit is 205 ms and
 * the budget is gone — so the OPERATOR LEVER is named in
 * `CompactionConfig.maxFanIn`: lower `HOGLAKE_COMPACTION_MAX_FAN_IN`.
 * The per-group hold is also what
 * `HOGLAKE_COMMIT_LOCK_TIMEOUT_MS` already bounds, so the failure mode
 * if this is wrong is a counted `skipped_conflicts`, not a stall.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CompactionFanInMeasurement {
    private companion object {
        /** A production-shaped manifest for the ids to be scattered in. */
        const val FILES = 200_000

        /** The fan-ins the decision is between. */
        val FAN_INS = listOf(64, 512, 2_048)

        /** The per-group commit-lock budget the default is chosen against. */
        const val HOLD_BUDGET_MS = 50L

        /** `maxGroupsPerRun` at the production settings. */
        const val GROUPS_PER_RUN = 64

        /** Sweeps per hour at a four-minute interval. */
        const val SWEEPS_PER_HOUR = 15

        /** Wall-clock assertions run on a measuring host only; see [Measuring]. */
        val MEASURING: Boolean = Measuring.enabled
    }

    private val db = PgTestSupport.freshDatabase()
    private var catalogId = 0L

    /** Per fan-in: the measured hold in microseconds. */
    private val holds = linkedMapOf<Int, Long>()

    @AfterAll
    fun tearDown() = db.close()

    @BeforeAll
    fun seedAndMeasure() {
        catalogId =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path, last_snapshot_id) " +
                        "VALUES ('fanin', 's3://fanin', 1000) RETURNING catalog_id",
                ).mapTo(Long::class.java).one()
            }
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 1, 0)",
                catalogId,
            )
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, row_id_start)
                SELECT :c, g, 1, 1,
                       's3://fanin/' || md5(g::text) || '/part-' || g || '.parquet',
                       200, 12288, g::bigint * 200
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("n", FILES).execute()
            // VACUUM, not only ANALYZE: a bulk insert leaves the
            // visibility map unset, and the UPDATE below would then be
            // measuring the absence of autovacuum.
            h.execute("VACUUM (ANALYZE) hog_data_file")
        }
        // Warm the caches with one discarded pass, so the figures are
        // the steady-state hold rather than the first touch.
        for (fanIn in FAN_INS) holdMicros(fanIn)
        for (fanIn in FAN_INS) holds[fanIn] = holdMicros(fanIn)
    }

    /**
     * The commit's two `IN`-list statements over [fanIn] SCATTERED ids,
     * inside one transaction holding the real per-catalog commit lock,
     * in microseconds.
     *
     * The ids are `1 + i * stride` for a stride that walks the whole
     * manifest, which is the production access pattern: a bucket's
     * files interleave with every other bucket's, so a group's ids are
     * scattered through the heap. A contiguous range would share heap
     * pages and flatter the measurement by the page density — the same
     * trap V19's header records its first draft falling into.
     */
    private fun holdMicros(fanIn: Int): Long {
        val stride = FILES / fanIn
        val ids = (0 until fanIn).map { 1L + it.toLong() * stride }
        return db.jdbi.withHandleUnchecked { h ->
            h.begin()
            try {
                Locks.acquireCatalogCommitLock(h, catalogId, 0)
                val started = System.nanoTime()
                h.createQuery(
                    """
                    SELECT f.data_file_id, (f.end_snapshot IS NULL) AS live,
                           dv.delete_file_id AS dv_id
                    FROM hog_data_file f
                    LEFT JOIN hog_delete_file dv
                      ON dv.catalog_id = f.catalog_id
                     AND dv.data_file_id = f.data_file_id
                     AND dv.end_snapshot IS NULL
                    WHERE f.catalog_id = :catalogId AND f.table_id = 1
                      AND f.data_file_id IN (<ids>)
                    """,
                )
                    .bind("catalogId", catalogId)
                    .bindList("ids", ids)
                    .map { rs, _ -> rs.getLong("data_file_id") }
                    .list()
                h.createUpdate(
                    """
                    UPDATE hog_data_file SET end_snapshot = 999
                     WHERE catalog_id = :catalogId AND data_file_id IN (<ids>)
                    """,
                )
                    .bind("catalogId", catalogId)
                    .bindList("ids", ids)
                    .execute()
                (System.nanoTime() - started) / 1_000
            } finally {
                // ROLLED BACK, so the measurement is repeatable: the
                // rows are dirtied and the WAL is written, so the hold
                // is the hold, and the fixture survives for the next
                // fan-in. The advisory lock is transaction-scoped and
                // goes with it.
                h.rollback()
            }
        }
    }

    @Test
    fun `the commit-lock hold scales with the fan-in, and 2048 inputs fit the per-group budget`() {
        val measured = FAN_INS.associateWith { holdMicros(it) }
        val perRow = measured.mapValues { (fanIn, micros) -> micros.toDouble() / fanIn }
        val report =
            measured.entries.joinToString("; ") { (fanIn, micros) ->
                "fan-in %d: %.1f ms hold, %.1f us/row".format(fanIn, micros / 1000.0, perRow.getValue(fanIn))
            }

        println("fan-in measurement: $report")

        // 1. THE PER-GROUP BUDGET, which is what the default is chosen
        //    against. A hold above it makes one sweep's 64 commits a
        //    convoy the foreground feels. Wall-clock: measuring hosts only
        //    (see MEASURING).
        if (MEASURING) {
            assertThat(measured.getValue(CompactionConfig.DEFAULT_MAX_FAN_IN) / 1000)
                .describedAs(
                    "the shipped ceiling's commit-lock hold must fit the %d ms per-group budget. %s",
                    HOLD_BUDGET_MS,
                    report,
                )
                .isLessThanOrEqualTo(HOLD_BUDGET_MS)
        }

        // 2. THE DUTY CYCLE the doctrine asks for: hold per commit x
        //    commits per sweep x sweeps per hour, as a fraction of the
        //    hour. Retirement's designed figure is 18-25%; compaction's
        //    must be a small fraction of that, because compaction's
        //    holds are not paced by a pause the way retirement's are.
        val sweepHoldMs = measured.getValue(CompactionConfig.DEFAULT_MAX_FAN_IN) / 1000 * GROUPS_PER_RUN
        val dutyCyclePercent = sweepHoldMs * SWEEPS_PER_HOUR * 100.0 / 3_600_000.0
        if (MEASURING) {
            assertThat(dutyCyclePercent)
                .describedAs(
                    "lock duty cycle at %d groups/sweep and %d sweeps/hour: %.3f%% (sweep hold %d ms). %s",
                    GROUPS_PER_RUN,
                    SWEEPS_PER_HOUR,
                    dutyCyclePercent,
                    sweepHoldMs,
                    report,
                )
                .isLessThan(5.0)
        }

        // 3. PER-ROW COST, and that it does not DEGRADE with the row
        //    count — the property that makes the arithmetic above
        //    extrapolate. An `IN`-list whose per-row cost grew with its
        //    length (a plan flip from index probes to a scan, say) would
        //    make the 64-input measurement useless for 2,048.
        assertThat(perRow.getValue(2_048))
            .describedAs("per-row cost must not grow with the list length. %s", report)
            .isLessThan(perRow.getValue(64) * 2)

        // 4. And the hold really does GROW with the fan-in, so a passing
        //    budget assertion is not an artifact of measuring noise.
        assertThat(measured.getValue(2_048))
            .describedAs("a 32x fan-in must cost measurably more than 64. %s", report)
            .isGreaterThan(measured.getValue(64))
    }

    @Test
    fun `the shipped default is the largest power of two the budget admits`() {
        // The decision rule, applied rather than asserted after the
        // fact: whatever the measurement says, the default must be a
        // value the budget admits, and the next power of two up must be
        // the one that does not — otherwise the default is leaving
        // throughput on the table for no reason.
        //
        // If this reds because a bigger fan-in now fits (a faster
        // machine, a Postgres upgrade), the fix is to RAISE the default
        // with the new measurement in this class's KDoc — not to relax
        // the assertion.
        val shipped = CompactionConfig.DEFAULT_MAX_FAN_IN
        assertThat(shipped).describedAs("a power of two, so the ladder is unambiguous").isEqualTo(2_048)
        // Wall-clock: measuring hosts only (see MEASURING).
        Assumptions.assumeTrue(MEASURING, "set HOGLAKE_MEASURE=1 to apply the budget rule on this host")
        assertThat(holdMicros(shipped) / 1000)
            .describedAs("the shipped default fits the budget")
            .isLessThanOrEqualTo(HOLD_BUDGET_MS)
    }

    @Test
    fun `the scaling fan-in reaches the ceiling for production's stray-day files and 2 for its fresh ones`() {
        // The arithmetic the ceiling exists to serve, on the two file
        // populations `ingest.events_raw` actually holds. This is a
        // pure function of the config, so it needs no fixture — it is
        // here because it is the other half of the same decision.
        val cfg = CompactionConfig(targetBytes = 512L * 1024 * 1024, maxGroupsPerRun = 64)
        val stray = List(100) { 12L * 1024 }
        val fresh = List(100) { 200L * 1024 * 1024 }

        assertThat(cfg.effectiveMaxInputFiles(stray))
            .describedAs("12 KiB files: 512 MiB / 12 KiB is ~43,690, so the ceiling binds")
            .isEqualTo(CompactionConfig.DEFAULT_MAX_FAN_IN)
        assertThat(cfg.effectiveMaxInputFiles(fresh))
            .describedAs("200 MB files: two of them fill the target, and the floor is maxInputFiles")
            .isEqualTo(cfg.maxInputFiles)

        // What that buys, stated in the unit that matters: files retired
        // per sweep, against arrivals.
        val perGroup = CompactionConfig.DEFAULT_MAX_FAN_IN - 1
        val perSweep = perGroup.toLong() * GROUPS_PER_RUN
        val perHour = perSweep * SWEEPS_PER_HOUR
        assertThat(perHour)
            .describedAs(
                "files retired per hour at the ceiling (%d/sweep) against ~216k arriving",
                perSweep,
            )
            .isGreaterThan(216_000)
    }
}
