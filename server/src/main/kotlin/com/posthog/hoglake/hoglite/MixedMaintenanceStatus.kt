package com.posthog.hoglake.hoglite

import com.posthog.hoglake.DatabaseBackend
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.InstanceMaintenanceStatus
import com.posthog.hoglake.model.MaintenanceRun
import com.posthog.hoglake.model.MaintenanceRunPage
import com.posthog.hoglake.model.MaintenanceStatus
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.service.MaintenanceStatusBackend

/** Instance reads use the same authoritative roster and bindings as catalog reads. Partial failures fail the read. */
internal class MixedMaintenanceStatus(
    private val router: CatalogRouter,
    private val postgres: MaintenanceStatusBackend,
    private val hoglite: MaintenanceStatusBackend,
) : MaintenanceStatusBackend {
    private fun target(e: CatalogDirectory.Entry) = if (e.backend == DatabaseBackend.POSTGRES) postgres else hoglite

    override fun status(catalog: String): MaintenanceStatus =
        router.operation(catalog) { e -> target(e).status(catalog) }

    override fun runs(
        catalog: String,
        task: MaintenanceTask?,
        before: Long?,
        limit: Int,
    ): MaintenanceRunPage = router.operation(catalog) { e -> target(e).runs(catalog, task, before, limit) }

    override fun instanceStatus(
        after: String?,
        limit: Int,
    ): InstanceMaintenanceStatus {
        validateLimit(limit)
        val cap = limit.coerceAtMost(100)
        val entries = router.directory.entries(after, cap + 1, readyOnly = true)
        val page = entries.take(cap)
        return InstanceMaintenanceStatus(
            catalogs = page.map { e -> router.bound(e) { target(e).status(e.name) } },
            hasMore = entries.size > cap,
            nextAfter = page.lastOrNull()?.name?.takeIf { entries.size > cap },
        )
    }

    override fun instanceRuns(
        task: MaintenanceTask?,
        before: Long?,
        limit: Int,
    ): MaintenanceRunPage {
        validateLimit(limit)
        // Like the existing hoglite instance feed, before is advisory: IDs are local to their ledger.
        // Never apply one catalog's ID as another catalog's horizon.
        val order =
            compareByDescending<MaintenanceRun> { it.startedAt }.thenByDescending { it.runId }.thenBy { it.catalog }
        var runs = emptyList<MaintenanceRun>()
        var hasMore = false
        for (e in router.directory.all(readyOnly = true)) {
            val page = router.bound(e) { target(e).runs(e.name, task, null, limit) }
            val merged = (runs + page.runs).sortedWith(order)
            hasMore = hasMore || page.hasMore || merged.size > limit
            runs = merged.take(limit)
        }
        return MaintenanceRunPage(runs, hasMore)
    }

    private fun validateLimit(limit: Int) {
        if (limit !in 1..1000) throw HoglakeException.Validation("limit must be between 1 and 1000")
    }
}
