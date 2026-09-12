package com.posthog.hoglake.service

import com.posthog.hoglake.compaction.CompactionTiers
import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.InstanceMaintenanceStatus
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
    private val smallFileThresholdBytes: Long,
    private val runStore: MaintenanceRunStore = MaintenanceRunStore(jdbi),
    private val tierTarget: Int = CompactionTiers.DEFAULT_TIER_TARGET,
) {
    fun status(catalog: String): MaintenanceStatus =
        jdbi.inTransactionUnchecked { h ->
            h.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            val cat = CatalogRepo.require(h, catalog)
            assemble(
                cat,
                MaintenanceSummarySampler.read(h, listOf(cat.catalogId))[cat.catalogId],
                runStore.lastByTask(h, cat.catalogId),
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
            InstanceMaintenanceStatus(
                catalogs =
                    page.map { cat ->
                        assemble(
                            cat,
                            summaries[cat.catalogId],
                            MaintenanceTask.entries.mapNotNull { task ->
                                runs[cat.name to task]?.let { task to it }
                            }.toMap(),
                        )
                    },
                hasMore = rows.size > cap,
                nextAfter = page.lastOrNull()?.name?.takeIf { rows.size > cap },
            )
        }
    }

    private fun assemble(
        cat: CatalogInfo,
        published: MaintenanceSummarySampler.Published?,
        runs: Map<MaintenanceTask, MaintenanceRun>,
    ): MaintenanceStatus {
        val valid =
            published?.takeIf {
                it.sample.targetBytes == smallFileThresholdBytes && it.sample.tierTarget == tierTarget
            }
        val sample = valid?.sample
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
                    ),
                    MaintenanceTaskStatus(
                        MaintenanceTask.CLEANUP,
                        cleanupIntervalMs,
                        runs[MaintenanceTask.CLEANUP],
                        MaintenanceBacklog.CleanupBacklog(
                            sample?.queuedRemovals,
                            sample?.oldestQueuedAt?.let {
                                Duration.between(it, Instant.now()).seconds.coerceAtLeast(0).toDouble()
                            },
                        ),
                    ),
                    MaintenanceTaskStatus(
                        MaintenanceTask.COMPACTION,
                        compactionIntervalMs,
                        runs[MaintenanceTask.COMPACTION],
                        MaintenanceBacklog.CompactionBacklog(sample?.smallFiles, smallFileThresholdBytes),
                    ),
                    MaintenanceTaskStatus(
                        MaintenanceTask.VERIFY,
                        null,
                        runs[MaintenanceTask.VERIFY],
                        MaintenanceBacklog.VerifyBacklog,
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
    }
}
