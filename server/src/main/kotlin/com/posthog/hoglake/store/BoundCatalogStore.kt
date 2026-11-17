package com.posthog.hoglake.store

import com.posthog.hoglake.auth.StoreGrant
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
import com.posthog.hoglake.service.ScanService
import java.time.Instant
import java.util.UUID

/**
 * A request's view of a catalog store: what [CatalogStoreSelector] hands a
 * handler in place of the store itself, under every auth mode that resolves
 * a caller. The first line of each method is its rule; the rules are
 * [StoreGrant]'s.
 */
internal class BoundCatalogStore(
    private val grant: StoreGrant,
    private val inner: CatalogStore,
) : CatalogStore {
    // ---- catalogs -------------------------------------------------------

    override fun createCatalog(
        name: String,
        dataPath: String,
        author: String?,
        message: String?,
    ): CatalogInfo {
        grant.closed(name)
        return inner.createCatalog(name, dataPath, author, message)
    }

    /** Open to a scoped caller: it is what its catalog listing shows, and where an upload claim's check reads. */
    override fun getCatalog(name: String): CatalogInfo {
        grant.open(name)
        return inner.getCatalog(name).also { grant.dataPath(it.dataPath) }
    }

    override fun listCatalogs(visible: Set<String>?): List<CatalogInfo> {
        grant.catalogs(visible)
        return inner.listCatalogs(visible)
    }

    override fun dropCatalog(
        catalog: String,
        author: String?,
        message: String?,
    ): DroppedCatalog {
        grant.closed(catalog)
        return inner.dropCatalog(catalog, author, message)
    }

    override fun storageUsage(catalog: String): StorageUsage {
        grant.closed(catalog)
        return inner.storageUsage(catalog)
    }

    // ---- namespaces ------------------------------------------------------

    override fun createNamespace(
        catalog: String,
        name: String,
        author: String?,
        message: String?,
        containment: NamespaceContainment?,
    ): NamespaceInfo {
        grant.closed(catalog)
        return inner.createNamespace(catalog, name, author, message, containment)
    }

    override fun listNamespaces(
        catalog: String,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): List<NamespaceInfo> {
        grant.open(catalog)
        return grant.readable(catalog, inner.listNamespaces(catalog, snapshot, atTimestamp)) { it.containment }
    }

    override fun getNamespace(
        catalog: String,
        namespace: String,
    ): NamespaceInfo {
        grant.read(catalog, namespace)
        return inner.getNamespace(catalog, namespace)
    }

    override fun dropNamespace(
        catalog: String,
        namespace: String,
        expectedNamespaceId: Long?,
        author: String?,
        message: String?,
        mayDropContained: Boolean,
    ): CommitResult {
        grant.closed(catalog)
        return inner.dropNamespace(catalog, namespace, expectedNamespaceId, author, message, mayDropContained)
    }

    override fun setNamespaceContainment(
        catalog: String,
        namespace: String,
        containment: NamespaceContainment?,
        author: String?,
    ): NamespaceInfo {
        grant.closed(catalog)
        return inner.setNamespaceContainment(catalog, namespace, containment, author)
    }

    /** What the checks read. The grant reads it from the store, once per request for a scoped caller. */
    override fun liveContainment(
        catalog: String,
        namespace: String,
    ): NamespaceContainment? = grant.containment(catalog, namespace)

    override fun liveContainedFor(
        catalog: String,
        field: String,
        value: Long,
    ): Pair<String, NamespaceContainment>? = grant.containedFor(catalog, field, value)

    // ---- tables ----------------------------------------------------------

    override fun createTable(
        catalog: String,
        namespace: String,
        name: String,
        columns: List<ColumnDef>,
        author: String?,
        message: String?,
    ): TableInfo {
        grant.write(catalog, namespace)
        return inner.createTable(catalog, namespace, name, columns, author, message)
    }

    override fun getTable(
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long?,
        atTimestamp: Instant?,
        totals: Boolean,
    ): TableInfo {
        grant.read(catalog, namespace)
        return inner.getTable(catalog, namespace, table, snapshot, atTimestamp, totals)
    }

    override fun listTables(
        catalog: String,
        namespace: String,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): List<TableSummaryInfo> {
        grant.read(catalog, namespace)
        return inner.listTables(catalog, namespace, snapshot, atTimestamp)
    }

    override fun dropTable(
        catalog: String,
        namespace: String,
        table: String,
        expectedTableUuid: UUID?,
        author: String?,
        message: String?,
    ): CommitResult {
        grant.write(catalog, namespace)
        return inner.dropTable(catalog, namespace, table, expectedTableUuid, author, message)
    }

    override fun truncateTable(
        catalog: String,
        namespace: String,
        table: String,
        expectedTableUuid: UUID,
        author: String?,
        message: String?,
    ): CommitResult {
        grant.write(catalog, namespace)
        return inner.truncateTable(catalog, namespace, table, expectedTableUuid, author, message)
    }

    // ---- files and changefeed --------------------------------------------

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
    ): List<DataFile> {
        grant.read(catalog, namespace)
        return inner.listFiles(
            catalog, namespace, table, snapshot, atTimestamp, sort, desc, limit, offset, partitionFilter,
        )
    }

    override fun partitionValues(
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): PartitionValues {
        grant.read(catalog, namespace)
        return inner.partitionValues(catalog, namespace, table, snapshot, atTimestamp)
    }

    override fun fileStats(
        catalog: String,
        namespace: String,
        table: String,
        fileId: Long,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): FileStats {
        grant.read(catalog, namespace)
        return inner.fileStats(catalog, namespace, table, fileId, snapshot, atTimestamp)
    }

    override fun changes(
        catalog: String,
        namespace: String,
        table: String,
        fromSnapshot: Long,
        toSnapshot: Long?,
    ): ChangesPlan {
        grant.read(catalog, namespace)
        return inner.changes(catalog, namespace, table, fromSnapshot, toSnapshot)
    }

    // ---- snapshots -------------------------------------------------------

    override fun resolveSnapshot(
        catalog: String,
        atTimestamp: Instant,
    ): SnapshotResolution {
        grant.closed(catalog)
        return inner.resolveSnapshot(catalog, atTimestamp)
    }

    override fun listSnapshots(
        catalog: String,
        after: Long,
        limit: Int,
        before: Long?,
    ): Pair<List<Snapshot>, Boolean> {
        grant.closed(catalog)
        return inner.listSnapshots(catalog, after, limit, before)
    }

    // ---- commits ---------------------------------------------------------

    /** Every namespace the request names, before the store: a receipt replays a commit's stored result. */
    override fun commit(
        catalog: String,
        request: CommitRequest,
        userAgent: String?,
    ): CommitResult {
        grant.open(catalog)
        for (append in request.appends) grant.write(catalog, append.namespace)
        for (delete in request.deletes) grant.write(catalog, delete.namespace)
        for (ends in request.fileEnds.orEmpty()) grant.write(catalog, ends.namespace)
        return inner.commit(catalog, request, userAgent)
    }

    /** A receipt names an operation and no namespace. */
    override fun receipt(
        catalog: String,
        operation: UUID,
    ): CommitResult {
        grant.closed(catalog)
        return inner.receipt(catalog, operation)
    }

    // ---- read planning ---------------------------------------------------

    override fun planScan(
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long?,
        atTimestamp: Instant?,
        columnStats: ScanService.ColumnStatsRequest?,
        splitOffsets: Boolean,
    ): List<ScanFile> {
        grant.read(catalog, namespace)
        return inner.planScan(catalog, namespace, table, snapshot, atTimestamp, columnStats, splitOffsets)
    }

    // ---- consumer offsets ------------------------------------------------

    override fun listConsumers(catalog: String): List<ConsumerTableOffset> {
        grant.closed(catalog)
        return inner.listConsumers(catalog)
    }

    override fun listOffsets(
        catalog: String,
        consumerId: String,
    ): List<ConsumerOffset> {
        grant.closed(catalog)
        return inner.listOffsets(catalog, consumerId)
    }

    override fun getOffset(
        catalog: String,
        consumerId: String,
        tableUuid: UUID,
    ): ConsumerOffset {
        grant.closed(catalog)
        return inner.getOffset(catalog, consumerId, tableUuid)
    }

    override fun commitOffset(
        catalog: String,
        consumerId: String,
        tableUuid: UUID,
        snapshotId: Long,
    ): ConsumerOffset {
        grant.closed(catalog)
        return inner.commitOffset(catalog, consumerId, tableUuid, snapshotId)
    }
}
