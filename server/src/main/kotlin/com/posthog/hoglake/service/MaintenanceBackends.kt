package com.posthog.hoglake.service

import com.posthog.hoglake.model.CatalogOptions
import com.posthog.hoglake.model.CleanupResult
import com.posthog.hoglake.model.CompactionResult
import com.posthog.hoglake.model.DatabaseHealth
import com.posthog.hoglake.model.ExpiryResult
import com.posthog.hoglake.model.InstanceMaintenanceStatus
import com.posthog.hoglake.model.MaintenanceRunPage
import com.posthog.hoglake.model.MaintenanceStatus
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.PartitionListing
import com.posthog.hoglake.model.PartitionStatsReport
import com.posthog.hoglake.model.RehydrateResult

/**
 * The maintenance routes' service surface, hoisted so the hoglite
 * backend (HOGLAKE_DATABASE=hoglite) can answer the same routes with
 * tenant-local implementations. Same pattern as [ViewBackend]: the
 * interface is the routes' call surface and nothing more; the Postgres
 * services implement it, and the hoglite ports live in the `hoglite`
 * package next to HogliteCatalogStore.
 *
 * `runOnceAllCatalogs` is deliberately NOT here: it is the loops'
 * surface, and each backend wires its own loop registration in App.
 */
interface OptionsBackend {
    fun get(catalog: String): CatalogOptions

    fun patch(
        catalog: String,
        snapshotRetentionSeconds: PatchField<Long>,
        consumerFloor: Boolean?,
        author: String? = null,
        message: String? = null,
    ): CatalogOptions
}

interface ExpiryBackend {
    fun runOnce(
        catalog: String,
        batchSize: Int,
        trigger: MaintenanceTrigger = MaintenanceTrigger.MANUAL,
    ): ExpiryResult
}

interface CleanupBackend {
    fun runOnce(
        catalog: String,
        batchSize: Int,
        trigger: MaintenanceTrigger = MaintenanceTrigger.MANUAL,
    ): CleanupResult
}

interface CompactionBackend {
    fun runOnce(
        catalog: String,
        batchOverride: Int? = null,
        trigger: MaintenanceTrigger = MaintenanceTrigger.MANUAL,
    ): CompactionResult
}

/** The hydrator's ROUTE surface — the operator requeue. */
interface HydratorMaintenanceBackend {
    fun rehydrateFailed(
        catalog: String,
        namespace: String? = null,
        table: String? = null,
    ): RehydrateResult
}

interface MaintenanceStatusBackend {
    fun status(catalog: String): MaintenanceStatus

    fun instanceStatus(
        after: String? = null,
        limit: Int = 50,
    ): InstanceMaintenanceStatus

    fun runs(
        catalog: String,
        task: MaintenanceTask?,
        before: Long?,
        limit: Int,
    ): MaintenanceRunPage

    fun instanceRuns(
        task: MaintenanceTask?,
        before: Long?,
        limit: Int,
    ): MaintenanceRunPage
}

interface DatabaseHealthBackend {
    fun report(): DatabaseHealth
}

interface PartitionStatsBackend {
    fun partitionStats(
        catalog: String,
        namespace: String?,
        table: String?,
        limit: Int,
    ): PartitionStatsReport
}

interface PartitionListingBackend {
    fun listPartitions(
        catalog: String,
        namespace: String,
        table: String,
        sort: PartitionListingService.SortColumn = PartitionListingService.SortColumn.PARTITION,
        desc: Boolean? = null,
        limit: Int = PartitionListingService.DEFAULT_LIMIT,
        offset: Int = 0,
        filters: List<PartitionListingService.Filter> = emptyList(),
    ): PartitionListing
}
