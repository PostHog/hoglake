package com.posthog.hoglake.service

import com.posthog.hoglake.model.CatalogOptions
import com.posthog.hoglake.model.CleanupResult
import com.posthog.hoglake.model.CompactionResult
import com.posthog.hoglake.model.ExpiryResult
import com.posthog.hoglake.model.MaintenanceRunPage
import com.posthog.hoglake.model.MaintenanceStatus
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.RehydrateResult
import com.posthog.hoglake.model.RetirementResult

/** Catalog maintenance operations. Fleet operations stay on the PostgreSQL services. */
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

interface RetirementBackend {
    fun runOnce(
        catalog: String,
        trigger: MaintenanceTrigger = MaintenanceTrigger.MANUAL,
    ): RetirementResult
}

/** The operator requeue operation. The hydrator sweep stays on PostgreSQL. */
interface HydratorMaintenanceBackend {
    fun rehydrateFailed(
        catalog: String,
        namespace: String? = null,
        table: String? = null,
    ): RehydrateResult
}

interface MaintenanceStatusBackend {
    fun status(catalog: String): MaintenanceStatus

    fun runs(
        catalog: String,
        task: MaintenanceTask?,
        before: Long?,
        limit: Int,
    ): MaintenanceRunPage
}
