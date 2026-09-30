package com.posthog.hoglake

import com.posthog.hoglake.service.CleanupService
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * The three ways the cleanup drain can be configured into a failure that
 * lands on somebody else, refused at boot where the knob names are still
 * in scope.
 *
 * A unit test, deliberately: these are facts about a `require` block, so
 * they red in the fast lane with no Docker — and `Config` is a data class,
 * so each case is one construction.
 *
 * THE POOL CHECK IS AGGREGATE OVER THE LOOPS THAT ACTUALLY RUN, and both
 * halves of that sentence are findings. Aggregate, because a per-service
 * check passed twice while the two services together took the whole pool:
 * at the maintenance shape (pool 10, reserve 4, groups 6)
 * `compactionParallelGroups <= 6` and `cleanupWorkers <= 6` are both
 * satisfied by 6 + 4 = 10 of 10 connections, and the first foreground
 * commit gets a Hikari timeout and a 500 instead of the typed 503. Priced
 * per loop, because an unconditional price refuses a draw that cannot
 * happen on that pod — both loops are per-workload (`BackgroundLoops`
 * returns early at `intervalMs <= 0`), and a server pod that runs neither
 * would be refused for a drain and a sweep it never starts.
 *
 * The one connection the arithmetic deliberately does not price is the
 * manual endpoint's, which `CleanupService` clamps to a single worker when
 * the loop is off — see the `cleanupWorkers` KDoc for why that is safe on
 * each workload, and `CleanupServiceIntegrationTest.the manual path on a
 * loop-off pod runs one worker` for the clamp itself.
 */
class CleanupConfigTest {
    private companion object {
        /**
         * A loop interval that turns cleanup ON, stated rather than
         * inherited.
         *
         * HYGIENE, NOT A FIX: the compiled default is already 1,800,000
         * (`Config.env` reads only `System.getenv`, and the test task sets
         * no environment), so these cases would price the full worker
         * count without it — they were never vacuous. What it buys is that
         * a case which means "the loop is on" says so, and cannot change
         * meaning if that default ever does.
         */
        const val LOOP_ON = 1_800_000L

        /**
         * A compaction interval that turns THAT loop on. It has to be
         * stated, unlike `LOOP_ON`: compaction's compiled default is **0**
         * (the loop is off unless a chart turns it on), so a pool case that
         * means "compaction is drawing" and does not say so prices a draw
         * of zero and asserts nothing.
         */
        const val COMPACTION_ON = 3_600_000L
    }

    @Test
    fun `the pool refusal is AGGREGATE over compaction groups and cleanup workers`() {
        // The shape that used to pass two independent checks and take the
        // whole pool. MUTATION: split the `require` back into two
        // per-service ones and this reds.
        assertThatThrownBy {
            Config(
                dbPoolSize = 10,
                compactionParallelGroups = 6,
                compactionIntervalMs = COMPACTION_ON,
                cleanupWorkers = 4,
                requestThreads = 10,
                cleanupIntervalMs = LOOP_ON,
            )
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_COMPACTION_PARALLEL_GROUPS=6")
            .hasMessageContaining("HOGLAKE_CLEANUP_WORKERS=4")
            .hasMessageContaining("HOGLAKE_DB_POOL_SIZE")

        // Each knob ALONE passes its own old per-service check at that
        // pool — `6 <= 10 - 4` and `4 <= 10 - 4` — which is exactly how two
        // independent checks admitted 10 of 10 connections. Under the
        // aggregate one, the production shape's budget for workers is not
        // merely small, it is NEGATIVE: six groups already spend the whole
        // non-reserved pool, so even the default single worker is refused.
        // THAT IS THE ROLLOUT CONSTRAINT, and a boot failure naming both
        // knobs is the right place to learn it.
        assertThatThrownBy {
            Config(
                dbPoolSize = 10,
                compactionParallelGroups = 6,
                compactionIntervalMs = COMPACTION_ON,
                cleanupWorkers = 1,
                requestThreads = 10,
                cleanupIntervalMs = LOOP_ON,
            )
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("needs a database pool of at least 11")
        // Raising the pool is what makes it legal, which is the fix the
        // message asks for.
        Config(
            dbPoolSize = 11,
            compactionParallelGroups = 6,
            compactionIntervalMs = COMPACTION_ON,
            cleanupWorkers = 1,
            requestThreads = 11,
            cleanupIntervalMs = LOOP_ON,
        )
        Config(
            dbPoolSize = 10,
            compactionParallelGroups = 1,
            compactionIntervalMs = COMPACTION_ON,
            cleanupWorkers = 5,
            requestThreads = 10,
            cleanupIntervalMs = LOOP_ON,
        )

        // And the boundary is exactly `pool - reserve`, not one less: the
        // reserve is a floor the sum must leave, and a check written with
        // `<` would refuse a legal configuration at every pool size.
        val pool = 12
        val budget = pool - Config.FOREGROUND_CONNECTION_RESERVE
        Config(
            dbPoolSize = pool,
            compactionParallelGroups = budget - 2,
            compactionIntervalMs = COMPACTION_ON,
            cleanupWorkers = 2,
            requestThreads = pool,
            cleanupIntervalMs = LOOP_ON,
        )
        assertThatThrownBy {
            Config(
                dbPoolSize = pool,
                compactionParallelGroups = budget - 1,
                compactionIntervalMs = COMPACTION_ON,
                cleanupWorkers = 2,
                requestThreads = pool,
                cleanupIntervalMs = LOOP_ON,
            )
        }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `the production shapes construct, and turning cleanup on is what needs the bigger pool`() {
        // THE TWO SHAPES THAT HAVE TO BOOT ON TODAY'S CHART, because every
        // pod builds a `Config` and a refusal is a pod that never serves.
        //
        // The gigahog-server pods run NEITHER loop and are the only pods
        // behind the ingress: priced draw 0, so a manual
        // POST /v1/maintenance/cleanup lands where one connection is
        // trivially available (and the service clamps it to one worker).
        Config(
            dbPoolSize = 10,
            compactionParallelGroups = 6,
            compactionIntervalMs = 0,
            cleanupWorkers = 1,
            cleanupIntervalMs = 0,
            requestThreads = 10,
        )
        // The maintenance pods run compaction at six groups and no
        // cleanup: priced draw 6, exactly the non-reserved budget, and
        // they serve no ingress — a manual run reaches them only by
        // port-forward and then costs the one clamped connection.
        Config(
            dbPoolSize = 10,
            compactionParallelGroups = 6,
            compactionIntervalMs = 3_600_000,
            cleanupWorkers = 1,
            cleanupIntervalMs = 0,
            requestThreads = 10,
        )

        // AND THE ROLLOUT CONSTRAINT: turning cleanup ON on that same
        // maintenance pod — even at ONE worker — needs one more
        // connection than the chart has. This is the assertion that keeps
        // that from being discovered by a crash-looping pod, and the
        // message has to name the pool as the fix.
        assertThatThrownBy {
            Config(
                dbPoolSize = 10,
                compactionParallelGroups = 6,
                compactionIntervalMs = 3_600_000,
                cleanupWorkers = 1,
                cleanupIntervalMs = LOOP_ON,
                requestThreads = 10,
            )
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("a compaction draw of 6")
            .hasMessageContaining("a cleanup draw of 1")
            .hasMessageContaining("needs a database pool of at least 11")
            .hasMessageContaining("HOGLAKE_DB_POOL_SIZE is 10")
        Config(
            dbPoolSize = 11,
            compactionParallelGroups = 6,
            compactionIntervalMs = 3_600_000,
            cleanupWorkers = 1,
            cleanupIntervalMs = LOOP_ON,
            requestThreads = 11,
        )

        // The four-worker drain-down is the other number the rollout note
        // quotes: 13 is one short, 14 works.
        assertThatThrownBy {
            Config(
                dbPoolSize = 13,
                compactionParallelGroups = 6,
                compactionIntervalMs = 3_600_000,
                cleanupWorkers = 4,
                cleanupIntervalMs = LOOP_ON,
                requestThreads = 13,
            )
        }
            .isInstanceOf(IllegalArgumentException::class.java)
        Config(
            dbPoolSize = 14,
            compactionParallelGroups = 6,
            compactionIntervalMs = 3_600_000,
            cleanupWorkers = 4,
            cleanupIntervalMs = LOOP_ON,
            requestThreads = 14,
        )
    }

    @Test
    fun `a loop that is off is not priced, and the message says which draw is which`() {
        // Each loop is priced only where it runs, so the same knob values
        // are legal or refused depending on the INTERVALS — which is the
        // whole point, since the intervals are what decide whether the
        // draw exists on that pod.
        val overBudget = 12 - Config.FOREGROUND_CONNECTION_RESERVE + 1

        // Both loops off: nothing priced, whatever the knobs say.
        Config(
            dbPoolSize = 12,
            compactionParallelGroups = overBudget,
            compactionIntervalMs = 0,
            cleanupWorkers = overBudget,
            cleanupIntervalMs = 0,
            requestThreads = 12,
        )
        // Compaction on alone: its draw is priced and this one is over.
        assertThatThrownBy {
            Config(
                dbPoolSize = 12,
                compactionParallelGroups = overBudget,
                compactionIntervalMs = 3_600_000,
                cleanupWorkers = 1,
                cleanupIntervalMs = 0,
                requestThreads = 12,
            )
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("a compaction draw of $overBudget")
            .hasMessageContaining("a cleanup draw of 0")
        // Cleanup on alone: the same, from the other side.
        assertThatThrownBy {
            Config(
                dbPoolSize = 12,
                compactionParallelGroups = 1,
                compactionIntervalMs = 0,
                cleanupWorkers = overBudget,
                cleanupIntervalMs = LOOP_ON,
                requestThreads = 12,
            )
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("a compaction draw of 0")
            .hasMessageContaining("a cleanup draw of $overBudget")
            .hasMessageContaining("the cleanup loop is ON")

        // And with cleanup off the message explains the one unpriced
        // connection rather than claiming the loop is on at interval 0.
        assertThatThrownBy {
            Config(
                dbPoolSize = 10,
                compactionParallelGroups = 7,
                compactionIntervalMs = 3_600_000,
                cleanupWorkers = 4,
                cleanupIntervalMs = 0,
                requestThreads = 10,
            )
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("the cleanup loop is OFF")
            .hasMessageContaining("clamped to ONE worker")
            .hasMessageContaining("port-forward")
    }

    @Test
    fun `App wires cleanup through the Config constructor and states no predicate of its own`() {
        // THE GATE IS ON THE SOURCE, in this repo's own idiom
        // (`IntegrationTagGateTest`, `MigrationLockWindowTest`), because
        // the property is about a call site rather than about behaviour.
        //
        // `CleanupService`'s primary constructor is public and its
        // `loopEnabled` has a default, so `App.kt` COULD bypass the
        // `Config` overload and state the predicate itself — and if it
        // ever wrote `loopEnabled = true`, the pool refusal's argument
        // (a loop-off pod draws the one connection it priced) would stop
        // holding in the shipped binary with every behavioural test still
        // green, because no test constructs App. The behavioural half is
        // pinned by `CleanupServiceIntegrationTest.the production wiring
        // clamps the manual path when the loop is off`; this is the half
        // that keeps App on that path.
        val app = java.io.File("src/main/kotlin/com/posthog/hoglake/App.kt").readText()
        assertThat(app)
            .describedAs("App must build the cleanup service from Config, not knob by knob")
            .contains("CleanupService(jdbi, removalStore, cfg)")
        assertThat(app)
            .describedAs(
                "and must not state the loop predicate itself: the derivation belongs in " +
                    "CleanupService's Config constructor, where a test can build it",
            )
            .doesNotContain("loop" + "Enabled")
    }

    @Test
    fun `zero workers is refused rather than turning the loop into a silent no-op`() {
        // There is no reading of 0 that means "do not drain": that is
        // what HOGLAKE_CLEANUP_INTERVAL_MS=0 is for, and it says so in
        // the status endpoint. Zero workers would leave the loop
        // recording runs that claim nothing, with every counter at 0 and
        // nothing distinguishing it from an empty queue.
        assertThatThrownBy { Config(cleanupWorkers = 0) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_CLEANUP_WORKERS=0")
            .hasMessageContaining("HOGLAKE_CLEANUP_INTERVAL_MS")
    }

    @Test
    fun `a claim lease of zero is refused, because it hands one row to two workers`() {
        // `claimed_at < now() - 0s` is true of a claim made in the same
        // statement, so at 0 every claim is immediately reclaimable and
        // two workers drain the same row by construction — the one thing
        // the lease exists to prevent.
        assertThatThrownBy { Config(cleanupClaimLeaseSeconds = 0) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_CLEANUP_CLAIM_LEASE_SECONDS=0")
        assertThatThrownBy { Config(cleanupClaimLeaseSeconds = -1) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `the defaults construct, and the cadence they describe is the one they mean`() {
        // The arithmetic Config's KDoc states, asserted so the KDoc and
        // the defaults cannot drift: rows/h = workers x batch x 3600000 /
        // interval. At the compiled defaults that is 4,000 rows/h, which
        // is a floor and nowhere near a busy catalog's arrivals — the
        // values that clear a backlog live in the chart, not here.
        val cfg = Config()
        assertThat(cfg.cleanupWorkers).isEqualTo(1)
        assertThat(cfg.cleanupClaimLeaseSeconds).isEqualTo(CleanupService.CLAIM_LEASE_SECONDS)
        val rowsPerHour =
            cfg.cleanupWorkers.toLong() * cfg.cleanupBatchSize * 3_600_000 / cfg.cleanupIntervalMs
        assertThat(rowsPerHour)
            .describedAs(
                "workers=%d batch=%d interval=%dms",
                cfg.cleanupWorkers,
                cfg.cleanupBatchSize,
                cfg.cleanupIntervalMs,
            )
            .isEqualTo(4_000)
    }
}
