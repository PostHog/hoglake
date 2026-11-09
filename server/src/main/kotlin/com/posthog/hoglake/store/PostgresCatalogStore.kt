package com.posthog.hoglake.store

import com.posthog.hoglake.Config
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.model.ChangesPlan
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.CommitResult
import com.posthog.hoglake.model.ConsumerOffset
import com.posthog.hoglake.model.ConsumerTableOffset
import com.posthog.hoglake.model.DataFile
import com.posthog.hoglake.model.DroppedCatalog
import com.posthog.hoglake.model.FileStats
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.model.NamespaceInfo
import com.posthog.hoglake.model.PartitionValues
import com.posthog.hoglake.model.ScanFile
import com.posthog.hoglake.model.Snapshot
import com.posthog.hoglake.model.SnapshotResolution
import com.posthog.hoglake.model.StorageUsage
import com.posthog.hoglake.model.TableInfo
import com.posthog.hoglake.model.TableSummaryInfo
import com.posthog.hoglake.persistence.FileRepo
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.ScanService
import org.jdbi.v3.core.Jdbi
import java.time.Instant
import java.util.UUID

/**
 * The PostgreSQL backend: a pure delegate over the existing services.
 *
 * Each method passes its arguments unchanged, in order, to the service
 * method of the same name. Nothing here adds logic. Audit, metrics,
 * locks and validation stay in the services.
 */
class PostgresCatalogStore(
    catalogProvider: () -> CatalogService,
    commitProvider: () -> CommitService,
    scanProvider: () -> ScanService,
) : CatalogStore {
    constructor(catalogs: CatalogService, commits: CommitService, scans: ScanService) :
        this({ catalogs }, { commits }, { scans })

    constructor(jdbi: () -> Jdbi, cfg: Config) : this(
        { CatalogService(jdbi()) },
        { CommitService(jdbi(), cfg.commitLockTimeoutMs, cfg.refuseBlindPartitionedAppends) },
        { ScanService(jdbi()) },
    )

    private val catalogs by lazy(catalogProvider)
    private val commits by lazy(commitProvider)
    private val scans by lazy(scanProvider)

    override fun createCatalog(
        name: String,
        dataPath: String,
        author: String?,
        message: String?,
    ): CatalogInfo = catalogs.createCatalog(name, dataPath, author, message)

    override fun getCatalog(name: String): CatalogInfo = catalogs.getCatalog(name)

    override fun listCatalogs(visible: Set<String>?): List<CatalogInfo> = catalogs.listCatalogs(visible)

    override fun dropCatalog(
        catalog: String,
        author: String?,
        message: String?,
    ): DroppedCatalog = catalogs.dropCatalog(catalog, author, message)

    override fun storageUsage(catalog: String): StorageUsage = catalogs.storageUsage(catalog)

    override fun createNamespace(
        catalog: String,
        name: String,
        author: String?,
        message: String?,
    ): NamespaceInfo = catalogs.createNamespace(catalog, name, author, message)

    override fun listNamespaces(
        catalog: String,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): List<NamespaceInfo> = catalogs.listNamespaces(catalog, snapshot, atTimestamp)

    override fun getNamespace(
        catalog: String,
        namespace: String,
    ): NamespaceInfo = catalogs.getNamespace(catalog, namespace)

    override fun dropNamespace(
        catalog: String,
        namespace: String,
        expectedNamespaceId: Long?,
        author: String?,
        message: String?,
        mayDropContained: Boolean,
    ): CommitResult = catalogs.dropNamespace(catalog, namespace, expectedNamespaceId, author, message, mayDropContained)

    override fun setNamespaceContainment(
        catalog: String,
        namespace: String,
        containment: NamespaceContainment?,
        author: String?,
    ): NamespaceInfo = catalogs.setNamespaceContainment(catalog, namespace, containment, author)

    override fun liveContainment(
        catalog: String,
        namespace: String,
    ): NamespaceContainment? = catalogs.liveContainment(catalog, namespace)

    override fun createTable(
        catalog: String,
        namespace: String,
        name: String,
        columns: List<ColumnDef>,
        author: String?,
        message: String?,
    ): TableInfo = catalogs.createTable(catalog, namespace, name, columns, author, message)

    override fun getTable(
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long?,
        atTimestamp: Instant?,
        totals: Boolean,
    ): TableInfo = catalogs.getTable(catalog, namespace, table, snapshot, atTimestamp, totals)

    override fun listTables(
        catalog: String,
        namespace: String,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): List<TableSummaryInfo> = catalogs.listTables(catalog, namespace, snapshot, atTimestamp)

    override fun dropTable(
        catalog: String,
        namespace: String,
        table: String,
        expectedTableUuid: UUID?,
        author: String?,
        message: String?,
    ): CommitResult = catalogs.dropTable(catalog, namespace, table, expectedTableUuid, author, message)

    override fun truncateTable(
        catalog: String,
        namespace: String,
        table: String,
        expectedTableUuid: UUID,
        author: String?,
        message: String?,
    ): CommitResult = catalogs.truncateTable(catalog, namespace, table, expectedTableUuid, author, message)

    override fun listFiles(
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long?,
        atTimestamp: Instant?,
        sort: FileRepo.FileSortColumn?,
        desc: Boolean,
        limit: Int?,
        offset: Int,
        partitionFilter: Map<Int, String>,
    ): List<DataFile> =
        catalogs.listFiles(catalog, namespace, table, snapshot, atTimestamp, sort, desc, limit, offset, partitionFilter)

    override fun partitionValues(
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): PartitionValues = catalogs.partitionValues(catalog, namespace, table, snapshot, atTimestamp)

    override fun fileStats(
        catalog: String,
        namespace: String,
        table: String,
        fileId: Long,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): FileStats = catalogs.fileStats(catalog, namespace, table, fileId, snapshot, atTimestamp)

    override fun changes(
        catalog: String,
        namespace: String,
        table: String,
        fromSnapshot: Long,
        toSnapshot: Long?,
    ): ChangesPlan = catalogs.changes(catalog, namespace, table, fromSnapshot, toSnapshot)

    override fun resolveSnapshot(
        catalog: String,
        atTimestamp: Instant,
    ): SnapshotResolution = catalogs.resolveSnapshot(catalog, atTimestamp)

    override fun listSnapshots(
        catalog: String,
        after: Long,
        limit: Int,
        before: Long?,
    ): Pair<List<Snapshot>, Boolean> = catalogs.listSnapshots(catalog, after, limit, before)

    override fun commit(
        catalog: String,
        request: CommitRequest,
        userAgent: String?,
    ): CommitResult = commits.commit(catalog, request, userAgent)

    override fun receipt(
        catalog: String,
        operation: UUID,
    ): CommitResult = commits.receipt(catalog, operation)

    override fun planScan(
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long?,
        atTimestamp: Instant?,
        columnStats: ScanService.ColumnStatsRequest?,
        splitOffsets: Boolean,
    ): List<ScanFile> = scans.planScan(catalog, namespace, table, snapshot, atTimestamp, columnStats, splitOffsets)

    override fun listConsumers(catalog: String): List<ConsumerTableOffset> = catalogs.listConsumers(catalog)

    override fun listOffsets(
        catalog: String,
        consumerId: String,
    ): List<ConsumerOffset> = catalogs.listOffsets(catalog, consumerId)

    override fun getOffset(
        catalog: String,
        consumerId: String,
        tableUuid: UUID,
    ): ConsumerOffset = catalogs.getOffset(catalog, consumerId, tableUuid)

    override fun commitOffset(
        catalog: String,
        consumerId: String,
        tableUuid: UUID,
        snapshotId: Long,
    ): ConsumerOffset = catalogs.commitOffset(catalog, consumerId, tableUuid, snapshotId)
}
