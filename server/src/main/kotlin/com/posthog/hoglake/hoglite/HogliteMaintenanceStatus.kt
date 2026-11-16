package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.HoglakeException
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
 * Read one tenant's maintenance ledger and indexed live backlogs.
 * No registry, fleet sampler, or PostgreSQL service participates.
 * Older schemas report unknown backlogs when the required indexes are absent.
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
            val indexes =
                store.queryCatalog(
                    catalog,
                    "SELECT name FROM sqlite_master WHERE type = 'index' AND name IN " +
                        "('files_live_stats', 'files_live_size', 'removals_queued_at', 'maintenance_runs_loop')",
                ).rows.map { it[0].asString() }.toSet()
            val tasks = MaintenanceTask.entries.filter { it != MaintenanceTask.VERIFY }
            val loops =
                tasks.filter {
                    it.hasLoop && it != MaintenanceTask.REINDEX && "maintenance_runs_loop" in indexes
                }
            val statements = mutableListOf(HogliteStatement(store.metaSql))
            if ("files_live_stats" in indexes) {
                statements +=
                    HogliteStatement(
                        "SELECT " +
                            "(SELECT COUNT(*) FROM files INDEXED BY files_live_stats " +
                            "WHERE end_snapshot IS NULL AND stats_state = 'pending'), " +
                            "(SELECT COUNT(*) FROM files INDEXED BY files_live_stats " +
                            "WHERE end_snapshot IS NULL AND stats_state = 'failed')",
                    )
            }
            if ("removals_queued_at" in indexes) {
                statements +=
                    HogliteStatement(
                        "SELECT COUNT(*), MIN(queued_at) FROM removals INDEXED BY removals_queued_at " +
                            "WHERE drained_at IS NULL",
                    )
            }
            if ("files_live_size" in indexes) {
                statements +=
                    HogliteStatement(
                        "SELECT COUNT(*) FROM files INDEXED BY files_live_size " +
                            "WHERE end_snapshot IS NULL AND file_size_bytes < ?",
                        listOf(smallFileThresholdBytes),
                    )
            }
            tasks.forEach { task ->
                statements +=
                    HogliteStatement(
                        "$RUN_SELECT WHERE task = ? ORDER BY id DESC LIMIT 1",
                        listOf(task.wire),
                    )
            }
            loops.forEach { task ->
                statements +=
                    HogliteStatement(
                        "SELECT started_at FROM maintenance_runs INDEXED BY maintenance_runs_loop " +
                            "WHERE task = ? AND run_trigger = 'loop' ORDER BY id DESC LIMIT 32",
                        listOf(task.wire),
                    )
            }
            // Every value uses the same read snapshot and the same tenant binding.
            val results = store.readCatalog(catalog, statements).iterator()
            val meta = store.metaRow(results.next().firstRow() ?: throw HoglakeException.NotFound("catalog '$catalog'"))
            if (meta.name != catalog) throw HoglakeException.NotFound("catalog '$catalog'")
            val hydration = if ("files_live_stats" in indexes) results.next().firstRow() else null
            val removals = if ("removals_queued_at" in indexes) results.next().firstRow() else null
            val small = if ("files_live_size" in indexes) results.next().firstRow()?.get(0).asLongOrNull() else null
            val lastRuns = tasks.associateWith { results.next().firstRow()?.let { toRun(catalog, it) } }
            val loopRuns =
                loops.associateWith {
                    results.next().rows.map { HogliteSchema.parse(it[0].asString()) }
                }
            val now = Instant.now()
            val ageSeconds =
                (removals?.get(1) as? String)?.let {
                    val age = Duration.between(HogliteSchema.parse(it), now).toMillis() / 1000.0
                    age.coerceAtLeast(0.0)
                }
            MaintenanceStatus(
                catalog = catalog,
                tasks =
                    tasks.map { task ->
                        MaintenanceTaskStatus(
                            task = task,
                            loopIntervalMs = loopIntervalOf(task),
                            lastRun = lastRuns[task],
                            backlog =
                                when (task) {
                                    MaintenanceTask.HYDRATOR ->
                                        MaintenanceBacklog.HydratorBacklog(
                                            hydration?.get(0).asLongOrNull(),
                                            hydration?.get(1).asLongOrNull(),
                                        )
                                    MaintenanceTask.EXPIRY ->
                                        MaintenanceBacklog.ExpiryBacklog(
                                            meta.retentionSeconds,
                                            meta.consumerFloor,
                                            meta.earliestSnapshotId,
                                            meta.head,
                                        )
                                    MaintenanceTask.CLEANUP ->
                                        MaintenanceBacklog.CleanupBacklog(
                                            removals?.get(0).asLongOrNull(),
                                            ageSeconds,
                                        )
                                    MaintenanceTask.COMPACTION ->
                                        MaintenanceBacklog.CompactionBacklog(
                                            small,
                                            smallFileThresholdBytes,
                                        )
                                    MaintenanceTask.RETIREMENT -> MaintenanceBacklog.RetirementBacklog
                                    MaintenanceTask.REINDEX -> MaintenanceBacklog.ReindexBacklog
                                    MaintenanceTask.VERIFY -> error("removed task")
                                },
                            loop =
                                if (task == MaintenanceTask.REINDEX) {
                                    null
                                } else {
                                    observe(
                                        task,
                                        loopRuns[task].orEmpty(),
                                        now,
                                    )
                                },
                        )
                    },
            )
        }

    override fun runs(
        catalog: String,
        task: MaintenanceTask?,
        before: Long?,
        limit: Int,
    ): MaintenanceRunPage {
        val cap = validateLimit(limit)
        return store.catalogOperation(catalog) {
            val predicates = mutableListOf<String>()
            val args = mutableListOf<Any?>()
            if (task != null) {
                predicates += "task = ?"
                args += task.wire
            }
            if (before != null) {
                predicates += "id < ?"
                args += before
            }
            args += cap + 1L
            val where = if (predicates.isEmpty()) "" else " WHERE " + predicates.joinToString(" AND ")
            val results =
                store.readCatalog(
                    catalog,
                    listOf(
                        HogliteStatement(store.metaSql),
                        HogliteStatement("$RUN_SELECT$where ORDER BY id DESC LIMIT ?", args),
                    ),
                )
            val meta = results[0].firstRow() ?: throw HoglakeException.NotFound("catalog '$catalog'")
            if (meta[0].asString() != catalog) throw HoglakeException.NotFound("catalog '$catalog'")
            page(results[1].rows.map { toRun(catalog, it) }, cap)
        }
    }

    private fun loopIntervalOf(task: MaintenanceTask): Long? =
        when (task) {
            MaintenanceTask.HYDRATOR -> hydratorIntervalMs
            MaintenanceTask.EXPIRY -> expiryIntervalMs
            MaintenanceTask.CLEANUP -> cleanupIntervalMs
            MaintenanceTask.COMPACTION -> compactionIntervalMs
            MaintenanceTask.RETIREMENT -> retirementIntervalMs
            // Reindex is PostgreSQL-only. No interval applies to Hoglite.
            MaintenanceTask.REINDEX -> null
            else -> null
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
            taskName = row[1].asString(),
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

        /** Stop reporting the cadence after three missed intervals. */
        private const val STALE_GAP_MULTIPLE = 3L
        private const val MIN_STALE_WINDOW_MS = 30_000L

        private const val RUN_SELECT =
            "SELECT id, task, run_trigger, started_at, finished_at, status, error, result_json FROM maintenance_runs"
    }
}
