package com.posthog.hoglake.service

import com.posthog.hoglake.compaction.CompactionGrouping
import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.InstanceMaintenanceStatus
import com.posthog.hoglake.model.LoopObservation
import com.posthog.hoglake.model.MaintenanceBacklog
import com.posthog.hoglake.model.MaintenanceRun
import com.posthog.hoglake.model.MaintenanceRunPage
import com.posthog.hoglake.model.MaintenanceStatus
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTaskStatus
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.MaintenanceRunStore
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import java.time.Duration
import java.time.Instant

/**
 * Dashboard reads touch only catalog identity, persisted summaries and
 * bounded latest-run index lookups. NEVER scan the manifest on this path.
 * Backlogs are the last completed sampling window; absent means warming
 * up, not zero. Catalog head/options and run history remain current.
 */
class MaintenanceStatusService(
    private val jdbi: Jdbi,
    private val hydratorIntervalMs: Long,
    private val expiryIntervalMs: Long,
    private val cleanupIntervalMs: Long,
    private val compactionIntervalMs: Long,
    private val verifyIntervalMs: Long,
    /**
     * NOT defaulted, unlike everything else optional on this class.
     * The value is what `GET /maintenance/status` reports as
     * retirement's `loop_interval_ms`, and a default of 0 would let
     * App forget to wire it while the endpoint kept answering
     * "disabled" — which is a claim about the fleet that nothing
     * would contradict. Making it required means the compiler asks.
     */
    private val retirementIntervalMs: Long,
    private val smallFileThresholdBytes: Long,
    private val minInputFiles: Int = CompactionGrouping.DEFAULT_MIN_INPUT_FILES,
    private val maxInputFiles: Int = CompactionGrouping.DEFAULT_MAX_INPUT_FILES,
    private val runStore: MaintenanceRunStore = MaintenanceRunStore(jdbi),
) {
    fun status(catalog: String): MaintenanceStatus =
        jdbi.inTransactionUnchecked { h ->
            h.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            val cat = CatalogRepo.require(h, catalog)
            assemble(
                cat,
                MaintenanceSummarySampler.read(h, listOf(cat.catalogId))[cat.catalogId],
                runStore.lastByTask(h, cat.catalogId),
                runStore.recentLoopRuns(h, cat.catalogId),
            )
        }

    fun instanceStatus(
        after: String? = null,
        limit: Int = 50,
    ): InstanceMaintenanceStatus {
        val cap = validateLimit(limit).coerceAtMost(100)
        return jdbi.inTransactionUnchecked { h ->
            h.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            val rows = CatalogRepo.page(h, after, cap + 1)
            val page = rows.take(cap)
            val ids = page.map { it.catalogId }
            val summaries = MaintenanceSummarySampler.read(h, ids)
            val runs = runStore.lastByTaskAll(h, ids)
            val loopRuns = runStore.recentLoopRunsAll(h, ids)
            InstanceMaintenanceStatus(
                catalogs =
                    page.map { cat ->
                        assemble(
                            cat,
                            summaries[cat.catalogId],
                            MaintenanceTask.entries.mapNotNull { task ->
                                runs[cat.name to task]?.let { task to it }
                            }.toMap(),
                            MaintenanceTask.entries.mapNotNull { task ->
                                loopRuns[cat.name to task]?.let { task to it }
                            }.toMap(),
                        )
                    },
                hasMore = rows.size > cap,
                nextAfter = page.lastOrNull()?.name?.takeIf { rows.size > cap },
            )
        }
    }

    /**
     * What the ledger says about [task]'s loop, fleet-wide.
     *
     * The interval is the MEDIAN gap, not the mean: one slow sweep, one
     * pod restart or one row that landed late must not move the number
     * an operator reads as the rhythm.
     */
    private fun observe(
        task: MaintenanceTask,
        loopRuns: List<Instant>,
        now: Instant,
    ): LoopObservation? {
        if (!task.hasLoop) return null
        val lastRunAt = loopRuns.maxOrNull()
        val gaps =
            loopRuns.sortedDescending()
                .zipWithNext { newer, older -> Duration.between(older, newer).toMillis() }
                .filter { it > 0 }
                .sorted()
        val median = gaps.getOrNull(gaps.size / 2)?.takeIf { task.loopRecordsEverySweep }
        // A cadence describes what the loop is doing NOW, so it survives
        // only while runs keep arriving at it. Without this, a loop that
        // stopped an hour ago would still read "every 1m" — the same
        // class of confident wrong answer as reading it off local config.
        val live =
            median != null &&
                lastRunAt != null &&
                Duration.between(lastRunAt, now).toMillis() <=
                maxOf(median * STALE_GAP_MULTIPLE, MIN_STALE_WINDOW_MS)
        return LoopObservation(
            intervalMs = median.takeIf { live },
            lastRunAt = lastRunAt,
            recordsEverySweep = task.loopRecordsEverySweep,
        )
    }

    private fun assemble(
        cat: CatalogInfo,
        published: MaintenanceSummarySampler.Published?,
        runs: Map<MaintenanceTask, MaintenanceRun>,
        loopRuns: Map<MaintenanceTask, List<Instant>>,
    ): MaintenanceStatus {
        /**
         * A published sample is only usable if it was computed under the
         * policy that is running NOW. The ladder's predecessor compared the
         * tier ratio here; the bin-packing bounds are its replacement, and
         * `Sample` carries both for exactly this check. Without it, changing
         * a bound leaves both endpoints serving debt from the old policy
         * until the next full scan republishes.
         */
        val valid =
            published?.takeIf {
                it.sample.targetBytes == smallFileThresholdBytes &&
                    it.sample.minInputFiles == minInputFiles &&
                    it.sample.maxInputFiles == maxInputFiles
            }
        val sample = valid?.sample
        val now = Instant.now()

        fun loop(task: MaintenanceTask) = observe(task, loopRuns[task].orEmpty(), now)
        return MaintenanceStatus(
            catalog = cat.name,
            sampledAt = valid?.sampledAt,
            sampleStartedAt = sample?.startedAt,
            sampledSnapshotId = sample?.snapshotId,
            tasks =
                listOf(
                    MaintenanceTaskStatus(
                        MaintenanceTask.HYDRATOR,
                        hydratorIntervalMs,
                        runs[MaintenanceTask.HYDRATOR],
                        MaintenanceBacklog.HydratorBacklog(sample?.pendingFiles, sample?.failedFiles),
                        loop(MaintenanceTask.HYDRATOR),
                    ),
                    MaintenanceTaskStatus(
                        MaintenanceTask.EXPIRY,
                        expiryIntervalMs,
                        runs[MaintenanceTask.EXPIRY],
                        MaintenanceBacklog.ExpiryBacklog(
                            cat.snapshotRetentionSeconds,
                            cat.consumerFloor,
                            cat.earliestSnapshotId,
                            cat.headSnapshotId,
                        ),
                        loop(MaintenanceTask.EXPIRY),
                    ),
                    MaintenanceTaskStatus(
                        MaintenanceTask.CLEANUP,
                        cleanupIntervalMs,
                        runs[MaintenanceTask.CLEANUP],
                        MaintenanceBacklog.CleanupBacklog(
                            sample?.queuedRemovals,
                            sample?.oldestQueuedAt?.let {
                                Duration.between(it, now).seconds.coerceAtLeast(0).toDouble()
                            },
                        ),
                        loop(MaintenanceTask.CLEANUP),
                    ),
                    MaintenanceTaskStatus(
                        MaintenanceTask.COMPACTION,
                        compactionIntervalMs,
                        runs[MaintenanceTask.COMPACTION],
                        MaintenanceBacklog.CompactionBacklog(sample?.smallFiles, smallFileThresholdBytes),
                        loop(MaintenanceTask.COMPACTION),
                    ),
                    MaintenanceTaskStatus(
                        MaintenanceTask.VERIFY,
                        verifyIntervalMs,
                        runs[MaintenanceTask.VERIFY],
                        MaintenanceBacklog.VerifyBacklog,
                        loop(MaintenanceTask.VERIFY),
                    ),
                    // Appended, never inserted: the task list's ORDER is
                    // what the webui's matrix and every positional test
                    // read, and a new task in the middle silently
                    // renumbers both.
                    MaintenanceTaskStatus(
                        MaintenanceTask.RETIREMENT,
                        retirementIntervalMs,
                        runs[MaintenanceTask.RETIREMENT],
                        // No backlog number, and see
                        // MaintenanceBacklog.RetirementBacklog for why:
                        // the honest one is a manifest scan, which this
                        // path may never do.
                        MaintenanceBacklog.RetirementBacklog,
                        loop(MaintenanceTask.RETIREMENT),
                    ),
                ),
        )
    }

    fun runs(
        catalog: String,
        task: MaintenanceTask?,
        before: Long?,
        limit: Int,
    ): MaintenanceRunPage {
        val cap = validateLimit(limit)
        return jdbi.inTransactionUnchecked { h ->
            val cat = CatalogRepo.require(h, catalog)
            page(runStore.history(h, cat.catalogId, task, before, cap + 1), cap)
        }
    }

    fun instanceRuns(
        task: MaintenanceTask?,
        before: Long?,
        limit: Int,
    ): MaintenanceRunPage {
        val cap = validateLimit(limit)
        return jdbi.inTransactionUnchecked { h -> page(runStore.historyAll(h, task, before, cap + 1), cap) }
    }

    private fun validateLimit(limit: Int): Int {
        if (limit <= 0) throw HoglakeException.Validation("limit must be positive (got $limit)")
        return limit.coerceAtMost(MAX_LIMIT)
    }

    private fun page(
        rows: List<MaintenanceRun>,
        limit: Int,
    ) = MaintenanceRunPage(rows.take(limit), rows.size > limit)

    companion object {
        const val MAX_LIMIT = 500

        /**
         * How many of its own gaps a loop may miss before its cadence
         * stops being reported. Loops sleep their interval AFTER the
         * body (BackgroundLoops), so the real gap is interval + run
         * duration and a slow sweep must not read as a stopped loop.
         */
        const val STALE_GAP_MULTIPLE = 3

        /** Floor on that window, so a sub-second loop cannot flap. */
        const val MIN_STALE_WINDOW_MS = 30_000L
    }
}
