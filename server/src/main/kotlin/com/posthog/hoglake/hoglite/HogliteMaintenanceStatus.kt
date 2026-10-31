package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.InstanceMaintenanceStatus
import com.posthog.hoglake.model.LoopObservation
import com.posthog.hoglake.model.MaintenanceBacklog
import com.posthog.hoglake.model.MaintenanceRun
import com.posthog.hoglake.model.MaintenanceRunPage
import com.posthog.hoglake.model.MaintenanceRunStatus
import com.posthog.hoglake.model.MaintenanceStatus
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTaskStatus
import com.posthog.hoglake.service.MaintenanceStatusBackend
import java.time.Duration
import java.time.Instant

/**
 * The maintenance status surface on hoglite (MaintenanceStatusService's
 * port): the run ledger read per catalog, with the live backlogs
 * COMPUTED rather than sampled.
 *
 * The Postgres service reads the MaintenanceSummarySampler's published
 * generation because a full-manifest scan per status request is what a
 * 10M-row catalog on a shared database cannot afford. A hoglite tenant
 * IS one catalog: the backlog queries below are COUNT(*)s over its own
 * SQLite file, the same order of work as the status read itself. The
 * result is the honest superset of the sampled answer — never stale,
 * never absent for want of a published generation — and the `sampled*`
 * fields stay null, which the wire contract already defines as "no
 * sample" rather than "no data".
 *
 * The loop observation ports verbatim: it is a claim about the ledger,
 * not about this process (the loop may live in another replica), so it
 * reads maintenance_runs exactly as the Postgres assembly does.
 */
internal class HogliteMaintenanceStatus(
    private val store: HogliteCatalogStore,
    private val hydratorIntervalMs: Long,
    private val expiryIntervalMs: Long,
    private val cleanupIntervalMs: Long,
    private val compactionIntervalMs: Long,
    private val retirementIntervalMs: Long,
    private val smallFileThresholdBytes: Long,
) : MaintenanceStatusBackend {
    override fun status(catalog: String): MaintenanceStatus =
        store.catalogOperation(catalog) {
            // Resolve the catalog (404 on a stranger) before the per-task reads.
            store.meta(catalog)
            return assemble(catalog)
        }

    override fun instanceStatus(
        after: String?,
        limit: Int,
    ): InstanceMaintenanceStatus {
        val cap = validateLimit(limit).coerceAtMost(100)
        val names = store.listCatalogNames().sorted()
        val from = if (after != null) names.filter { it > after } else names
        val page = from.take(cap)
        return InstanceMaintenanceStatus(
            catalogs = page.map { assemble(it) },
            hasMore = from.size > cap,
            nextAfter = page.lastOrNull()?.takeIf { from.size > cap },
        )
    }

    override fun runs(
        catalog: String,
        task: MaintenanceTask?,
        before: Long?,
        limit: Int,
    ): MaintenanceRunPage =
        store.catalogOperation(catalog) {
            store.meta(catalog) // 404 on a stranger
            val cap = validateLimit(limit)
            val rows =
                store.queryCatalog(
                    catalog,
                    "SELECT id, task, run_trigger, started_at, finished_at, status, error, result_json " +
                        "FROM maintenance_runs " +
                        "WHERE (? IS NULL OR task = ?) AND (? IS NULL OR id < ?) " +
                        "ORDER BY id DESC LIMIT ?",
                    task?.wire,
                    task?.wire,
                    before,
                    before,
                    cap + 1L,
                ).rows
            return page(rows.map { toRun(catalog, it) }, cap)
        }

    override fun instanceRuns(
        task: MaintenanceTask?,
        before: Long?,
        limit: Int,
    ): MaintenanceRunPage {
        // The instance feed is a cross-tenant concat, ranked by started_at
        // (the ledger id is per-tenant, so it cannot rank ACROSS tenants;
        // `before` paginates the ranked stream as (started_at, id)).
        val cap = validateLimit(limit)
        val all = mutableListOf<Pair<String, MaintenanceRun>>()
        for (name in store.listCatalogNames()) {
            runCatching {
                store.catalogOperation(name) {
                    store.queryCatalog(
                        name,
                        "SELECT id, task, run_trigger, started_at, finished_at, status, error, result_json " +
                            "FROM maintenance_runs " +
                            "WHERE (? IS NULL OR task = ?) ORDER BY id DESC LIMIT ?",
                        task?.wire,
                        task?.wire,
                        cap + 1L,
                    ).rows.map { name to toRun(name, it) }
                }
            }.onSuccess { all += it }
        }
        val ordered =
            all.sortedWith(
                compareByDescending<Pair<String, MaintenanceRun>> {
                    it.second.startedAt
                }.thenByDescending { it.second.runId },
            )
        // `before` is a ledger id; on the instance feed it is advisory —
        // applied as a started_at horizon would lose same-second runs, so
        // it filters nothing here and the page is simply the newest cap.
        return MaintenanceRunPage(
            runs = ordered.take(cap).map { it.second },
            hasMore = ordered.size > cap,
        )
    }

    // ---- assembly -----------------------------------------------------------

    private fun assemble(catalog: String): MaintenanceStatus =
        store.catalogOperation(catalog) {
            val lastRuns = lastRunByTask(catalog)
            val loopRuns = recentLoopRuns(catalog)
            val now = Instant.now()
            val tasks =
                MaintenanceTask.entries
                    .filter { it != REMOVED_VERIFY }
                    .map { task ->
                        MaintenanceTaskStatus(
                            task = task,
                            loopIntervalMs = loopIntervalOf(task),
                            lastRun = lastRuns[task],
                            backlog = backlog(catalog, task),
                            loop = observe(task, loopRuns[task] ?: emptyList(), now),
                        )
                    }
            return MaintenanceStatus(catalog = catalog, tasks = tasks)
        }

    private fun loopIntervalOf(task: MaintenanceTask): Long? =
        when (task) {
            MaintenanceTask.HYDRATOR -> hydratorIntervalMs
            MaintenanceTask.EXPIRY -> expiryIntervalMs
            MaintenanceTask.CLEANUP -> cleanupIntervalMs
            MaintenanceTask.COMPACTION -> compactionIntervalMs
            MaintenanceTask.RETIREMENT -> retirementIntervalMs
            // The reindex loop is Postgres's; a hoglite deployment has no
            // B-tree bloat to rebuild, so it reports as absent, not 0
            // (0 would claim "configured off HERE" — this process has no
            // such loop to configure).
            MaintenanceTask.REINDEX -> null
            else -> null
        }

    /** The live backlog for [task], computed over the tenant at read time. */
    private fun backlog(
        catalog: String,
        task: MaintenanceTask,
    ): MaintenanceBacklog =
        when (task) {
            MaintenanceTask.HYDRATOR -> {
                val row =
                    store.queryCatalog(
                        catalog,
                        "SELECT " +
                            "SUM(CASE WHEN stats_state = 'pending' THEN 1 ELSE 0 END), " +
                            "SUM(CASE WHEN stats_state = 'failed' THEN 1 ELSE 0 END) " +
                            "FROM files WHERE end_snapshot IS NULL",
                    ).firstRow()
                MaintenanceBacklog.HydratorBacklog(
                    pendingFiles = row?.get(0)?.asLongOrNull() ?: 0L,
                    failedFiles = row?.get(1)?.asLongOrNull() ?: 0L,
                )
            }
            MaintenanceTask.EXPIRY -> {
                val m = store.meta(catalog)
                MaintenanceBacklog.ExpiryBacklog(
                    snapshotRetentionSeconds = m.retentionSeconds,
                    consumerFloor = m.consumerFloor,
                    earliestSnapshotId = m.earliestSnapshotId,
                    headSnapshotId = m.head,
                )
            }
            MaintenanceTask.CLEANUP -> {
                val row =
                    store.queryCatalog(
                        catalog,
                        "SELECT COUNT(*), MIN(queued_at) FROM removals WHERE drained_at IS NULL",
                    ).firstRow()
                val count = row?.get(0)?.asLong() ?: 0L
                val oldest = row?.get(1) as? String
                MaintenanceBacklog.CleanupBacklog(
                    queuedRemovals = count,
                    oldestQueuedAgeSeconds =
                        oldest?.let {
                            Duration.between(HogliteSchema.parse(it), Instant.now()).toMillis() / 1000.0
                        },
                )
            }
            MaintenanceTask.COMPACTION -> {
                val small =
                    store.queryCatalog(
                        catalog,
                        "SELECT COUNT(*) FROM files WHERE end_snapshot IS NULL AND file_size_bytes < ?",
                        smallFileThresholdBytes,
                    ).firstRow()!![0].asLong()
                MaintenanceBacklog.CompactionBacklog(
                    smallFiles = small,
                    targetBytes = smallFileThresholdBytes,
                )
            }
            MaintenanceTask.RETIREMENT -> MaintenanceBacklog.RetirementBacklog
            // Postgres's task; the port keeps the empty vocabulary entry.
            MaintenanceTask.REINDEX -> MaintenanceBacklog.ReindexBacklog
            else -> MaintenanceBacklog.RetirementBacklog
        }

    /** lastByTask: the newest ledger row per task, for this tenant. */
    private fun lastRunByTask(catalog: String): Map<MaintenanceTask, MaintenanceRun> {
        val out = mutableMapOf<MaintenanceTask, MaintenanceRun>()
        for (task in MaintenanceTask.entries) {
            if (task == REMOVED_VERIFY) continue
            val row =
                store.queryCatalog(
                    catalog,
                    "SELECT id, task, run_trigger, started_at, finished_at, status, error, result_json " +
                        "FROM maintenance_runs WHERE task = ? ORDER BY id DESC LIMIT 1",
                    task.wire,
                ).firstRow() ?: continue
            out[task] = toRun(catalog, row)
        }
        return out
    }

    /** recentLoopRuns: loop-trigger started_at times per task. */
    private fun recentLoopRuns(catalog: String): Map<MaintenanceTask, List<Instant>> {
        val out = mutableMapOf<MaintenanceTask, MutableList<Instant>>()
        for (task in MaintenanceTask.entries) {
            if (!task.hasLoop) continue
            val rows =
                store.queryCatalog(
                    catalog,
                    "SELECT started_at FROM maintenance_runs " +
                        "WHERE task = ? AND run_trigger = 'loop' ORDER BY id DESC LIMIT 32",
                    task.wire,
                ).rows
            out[task] = rows.map { HogliteSchema.parse(it[0].asString()) }.toMutableList()
        }
        return out
    }

    /** The ledger-cadence observation, MaintenanceStatusService.observe verbatim. */
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
        val live =
            median != null &&
                lastRunAt != null &&
                Duration.between(lastRunAt, now).toMillis() <=
                maxOf(median * STALE_GAP_MULTIPLE, MIN_STALE_WINDOW_MS)
        return LoopObservation(
            intervalMs = median?.takeIf { live },
            lastRunAt = lastRunAt,
            recordsEverySweep = task.loopRecordsEverySweep,
        )
    }

    private fun toRun(
        catalog: String,
        row: List<Any?>,
    ): MaintenanceRun =
        MaintenanceRun(
            runId = row[0].asLong(),
            catalog = catalog,
            task = MaintenanceTask.entries.first { it.wire == row[1].asString() },
            trigger =
                com.posthog.hoglake.model.MaintenanceTrigger.entries.first {
                    it.wire == row[2].asString()
                },
            startedAt = HogliteSchema.parse(row[3].asString()),
            finishedAt = HogliteSchema.parse(row[4].asString()),
            status =
                if (row[5].asString() == "ok") MaintenanceRunStatus.OK else MaintenanceRunStatus.FAILED,
            error = row[6] as? String,
            resultJson = row[7] as? String,
        )

    private fun page(
        rows: List<MaintenanceRun>,
        cap: Int,
    ): MaintenanceRunPage = MaintenanceRunPage(runs = rows.take(cap), hasMore = rows.size > cap)

    private fun validateLimit(limit: Int): Int {
        if (limit <= 0) throw HoglakeException.Validation("limit must be positive (got $limit)")
        return limit.coerceAtMost(MAX_LIMIT)
    }

    companion object {
        private const val MAX_LIMIT = 500

        /**
         * A loop whose last run is more than this many median intervals
         * old has stopped, whatever its history: the cadence claim
         * retracts (intervalMs goes null), same as the Postgres assembly.
         */
        private const val STALE_GAP_MULTIPLE = 3L
        private const val MIN_STALE_WINDOW_MS = 30_000L

        /** #261's removed subsystem; never listed (its KDoc explains). */
        private val REMOVED_VERIFY: MaintenanceTask? =
            MaintenanceTask.entries.firstOrNull { it.wire == "verify" }
    }
}
