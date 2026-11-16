package com.posthog.hoglake.store

import com.posthog.hoglake.auth.StoreGrant
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.CatalogOptions
import com.posthog.hoglake.model.CleanupResult
import com.posthog.hoglake.model.CommitResult
import com.posthog.hoglake.model.CompactionResult
import com.posthog.hoglake.model.ExpiryResult
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.MaintenanceRunPage
import com.posthog.hoglake.model.MaintenanceStatus
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.PartitionListing
import com.posthog.hoglake.model.PartitionLookup
import com.posthog.hoglake.model.PartitionStatsReport
import com.posthog.hoglake.model.RehydrateResult
import com.posthog.hoglake.model.RetirementResult
import com.posthog.hoglake.model.TableInfo
import com.posthog.hoglake.model.ViewInfo
import com.posthog.hoglake.service.AlterBackend
import com.posthog.hoglake.service.CleanupBackend
import com.posthog.hoglake.service.CompactionBackend
import com.posthog.hoglake.service.ExpiryBackend
import com.posthog.hoglake.service.HydratorMaintenanceBackend
import com.posthog.hoglake.service.MaintenanceStatusBackend
import com.posthog.hoglake.service.OptionsBackend
import com.posthog.hoglake.service.PartitionListingBackend
import com.posthog.hoglake.service.PartitionListingService
import com.posthog.hoglake.service.PartitionLookupBackend
import com.posthog.hoglake.service.PartitionStatsBackend
import com.posthog.hoglake.service.PatchField
import com.posthog.hoglake.service.RetirementBackend
import com.posthog.hoglake.service.TableCreation
import com.posthog.hoglake.service.TableCreationBackend
import com.posthog.hoglake.service.TableCreationDefinition
import com.posthog.hoglake.service.UploadBackend
import com.posthog.hoglake.service.UploadClaim
import com.posthog.hoglake.service.ViewBackend
import java.util.UUID

/*
 * A request's view of each sub-backend, as BoundCatalogStore is of the
 * store, handed out in place of the PostgreSQL service or the hoglite
 * backend. The rules are StoreGrant's (auth/StoreGrant.kt).
 */

internal class BoundUploads(
    private val grant: StoreGrant,
    private val inner: UploadBackend,
) : UploadBackend {
    override fun claim(
        catalog: String,
        id: UUID,
        owner: UUID,
        prefix: String,
        kind: String,
    ): UploadClaim {
        grant.claim(catalog, prefix)
        return inner.claim(catalog, id, owner, prefix, kind)
    }

    /** Keyed by the claim owner, an id the writer chose: open to a scoped caller (AuthPolicy.SCOPED_ROUTES). */
    override fun renew(
        catalog: String,
        owner: UUID,
    ): Int {
        grant.open(catalog)
        return inner.renew(catalog, owner)
    }

    /** As [renew]. */
    override fun abandon(
        catalog: String,
        owner: UUID,
        paths: List<String>,
    ): Int {
        grant.open(catalog)
        return inner.abandon(catalog, owner, paths)
    }

    override fun scheduleExpired(
        catalog: String,
        limit: Int,
    ): Int {
        grant.closed(catalog)
        return inner.scheduleExpired(catalog, limit)
    }
}

internal class BoundTableCreations(
    private val grant: StoreGrant,
    private val inner: TableCreationBackend,
) : TableCreationBackend {
    override fun prepare(
        catalog: String,
        operationId: UUID,
        definition: TableCreationDefinition,
    ): TableCreation {
        grant.write(catalog, definition.namespace)
        return inner.prepare(catalog, operationId, definition)
    }

    /** The namespace is the creation's own, known only once it is read: read, then checked, then answered. */
    override fun status(
        catalog: String,
        operationId: UUID,
    ): TableCreation {
        grant.open(catalog)
        val creation = inner.status(catalog, operationId)
        grant.read(catalog, operationId, creation.definition.namespace)
        return creation
    }

    override fun abort(
        catalog: String,
        operationId: UUID,
    ): TableCreation {
        grant.write(catalog, operationId) { inner.status(catalog, operationId).definition.namespace }
        return inner.abort(catalog, operationId)
    }

    override fun publish(
        catalog: String,
        operationId: UUID,
        files: List<FileRegistration>,
    ): TableCreation {
        grant.write(catalog, operationId) { inner.status(catalog, operationId).definition.namespace }
        return inner.publish(catalog, operationId, files)
    }
}

internal class BoundAlters(
    private val grant: StoreGrant,
    private val inner: AlterBackend,
) : AlterBackend {
    override fun alterTable(
        catalog: String,
        namespace: String,
        table: String,
        ops: List<AlterOp>,
        expectedTableUuid: UUID?,
        readSnapshot: Long?,
        author: String?,
        message: String?,
    ): TableInfo {
        grant.write(catalog, namespace)
        return inner.alterTable(catalog, namespace, table, ops, expectedTableUuid, readSnapshot, author, message)
    }
}

internal class BoundViews(
    private val grant: StoreGrant,
    private val inner: ViewBackend,
) : ViewBackend {
    override fun create(
        catalog: String,
        namespace: String,
        name: String,
        sql: String,
        dialect: String,
        author: String?,
        message: String?,
    ): ViewInfo {
        grant.write(catalog, namespace)
        return inner.create(catalog, namespace, name, sql, dialect, author, message)
    }

    override fun get(
        catalog: String,
        namespace: String,
        name: String,
    ): ViewInfo {
        grant.read(catalog, namespace)
        return inner.get(catalog, namespace, name)
    }

    override fun list(
        catalog: String,
        namespace: String,
    ): List<ViewInfo> {
        grant.read(catalog, namespace)
        return inner.list(catalog, namespace)
    }

    override fun drop(
        catalog: String,
        namespace: String,
        name: String,
        author: String?,
        message: String?,
    ): CommitResult {
        grant.write(catalog, namespace)
        return inner.drop(catalog, namespace, name, author, message)
    }
}

internal class BoundPartitionStats(
    private val grant: StoreGrant,
    private val inner: PartitionStatsBackend,
) : PartitionStatsBackend {
    /** With no namespace the report covers the catalog. */
    override fun partitionStats(
        catalog: String,
        namespace: String?,
        table: String?,
        limit: Int,
    ): PartitionStatsReport {
        if (namespace == null) grant.closed(catalog) else grant.read(catalog, namespace)
        return inner.partitionStats(catalog, namespace, table, limit)
    }
}

internal class BoundPartitionListing(
    private val grant: StoreGrant,
    private val inner: PartitionListingBackend,
) : PartitionListingBackend {
    override fun listPartitions(
        catalog: String,
        namespace: String,
        table: String,
        sort: PartitionListingService.SortColumn,
        desc: Boolean?,
        limit: Int,
        offset: Int,
        filters: List<PartitionListingService.Filter>,
    ): PartitionListing {
        grant.read(catalog, namespace)
        return inner.listPartitions(catalog, namespace, table, sort, desc, limit, offset, filters)
    }
}

internal class BoundPartitionLookup(
    private val grant: StoreGrant,
    private val inner: PartitionLookupBackend,
) : PartitionLookupBackend {
    override fun lookup(
        catalog: String,
        namespace: String,
        table: String,
        specId: Long,
        values: List<String?>,
        snapshot: Long?,
        limit: Int?,
        cursor: String?,
    ): PartitionLookup {
        grant.read(catalog, namespace)
        return inner.lookup(catalog, namespace, table, specId, values, snapshot, limit, cursor)
    }
}

// ---- maintenance: the catalog for every caller, and never a scoped one ----

internal class BoundOptions(
    private val grant: StoreGrant,
    private val inner: OptionsBackend,
) : OptionsBackend {
    override fun get(catalog: String): CatalogOptions {
        grant.closed(catalog)
        return inner.get(catalog)
    }

    override fun patch(
        catalog: String,
        snapshotRetentionSeconds: PatchField<Long>,
        consumerFloor: Boolean?,
        author: String?,
        message: String?,
    ): CatalogOptions {
        grant.closed(catalog)
        return inner.patch(catalog, snapshotRetentionSeconds, consumerFloor, author, message)
    }
}

internal class BoundExpiry(
    private val grant: StoreGrant,
    private val inner: ExpiryBackend,
) : ExpiryBackend {
    override fun runOnce(
        catalog: String,
        batchSize: Int,
        trigger: MaintenanceTrigger,
    ): ExpiryResult {
        grant.closed(catalog)
        return inner.runOnce(catalog, batchSize, trigger)
    }
}

internal class BoundCleanup(
    private val grant: StoreGrant,
    private val inner: CleanupBackend,
) : CleanupBackend {
    override fun runOnce(
        catalog: String,
        batchSize: Int,
        trigger: MaintenanceTrigger,
    ): CleanupResult {
        grant.closed(catalog)
        return inner.runOnce(catalog, batchSize, trigger)
    }
}

internal class BoundCompaction(
    private val grant: StoreGrant,
    private val inner: CompactionBackend,
) : CompactionBackend {
    override fun runOnce(
        catalog: String,
        batchOverride: Int?,
        trigger: MaintenanceTrigger,
    ): CompactionResult {
        grant.closed(catalog)
        return inner.runOnce(catalog, batchOverride, trigger)
    }
}

internal class BoundRetirement(
    private val grant: StoreGrant,
    private val inner: RetirementBackend,
) : RetirementBackend {
    override fun runOnce(
        catalog: String,
        trigger: MaintenanceTrigger,
    ): RetirementResult {
        grant.closed(catalog)
        return inner.runOnce(catalog, trigger)
    }
}

internal class BoundHydratorMaintenance(
    private val grant: StoreGrant,
    private val inner: HydratorMaintenanceBackend,
) : HydratorMaintenanceBackend {
    override fun rehydrateFailed(
        catalog: String,
        namespace: String?,
        table: String?,
    ): RehydrateResult {
        grant.closed(catalog)
        return inner.rehydrateFailed(catalog, namespace, table)
    }
}

internal class BoundMaintenanceStatus(
    private val grant: StoreGrant,
    private val inner: MaintenanceStatusBackend,
) : MaintenanceStatusBackend {
    override fun status(catalog: String): MaintenanceStatus {
        grant.closed(catalog)
        return inner.status(catalog)
    }

    override fun runs(
        catalog: String,
        task: MaintenanceTask?,
        before: Long?,
        limit: Int,
    ): MaintenanceRunPage {
        grant.closed(catalog)
        return inner.runs(catalog, task, before, limit)
    }
}
