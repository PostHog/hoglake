package com.posthog.hoglake.store

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
 * The catalog persistence seam. The core `/v1` catalog routes and the
 * scan route call these operations and nothing else.
 *
 * The seam is the service surface, not a SQL abstraction. Some backends
 * accept only one-shot transactions with declarative guards, so an
 * interactive "open a handle, run statements" seam cannot work for them.
 *
 * Implementations:
 *
 *  - [PostgresCatalogStore] delegates to CatalogService, CommitService and
 *    ScanService. The server uses only this implementation.
 *  - The hoglite adapter implements the same surface against the hoglite
 *    engine. Nothing constructs it in the server yet.
 *
 * The parameter defaults are the defaults of the PostgreSQL services. A
 * route that leaves out an argument thus gets the same value as before.
 *
 * The surface contains only operations that both implementations have.
 * Routes for backend-specific features (namespace containment, alters,
 * uploads, table creations, views, maintenance) keep their own services.
 */
interface CatalogStore {
    // ---- catalogs -------------------------------------------------------

    fun createCatalog(
        name: String,
        dataPath: String,
        author: String? = null,
        message: String? = null,
    ): CatalogInfo

    fun getCatalog(name: String): CatalogInfo

    /**
     * All catalogs when [visible] is null, else only the named catalogs that exist.
     * The hoglite adapter refuses a null [visible]. See TODO(wiring) there.
     */
    fun listCatalogs(visible: Set<String>? = null): List<CatalogInfo>

    fun dropCatalog(
        catalog: String,
        author: String?,
        message: String?,
    ): DroppedCatalog

    fun storageUsage(catalog: String): StorageUsage

    // ---- namespaces ------------------------------------------------------

    fun createNamespace(
        catalog: String,
        name: String,
        author: String? = null,
        message: String? = null,
    ): NamespaceInfo

    fun listNamespaces(
        catalog: String,
        snapshot: Long? = null,
        atTimestamp: Instant? = null,
    ): List<NamespaceInfo>

    fun getNamespace(
        catalog: String,
        namespace: String,
    ): NamespaceInfo

    /** [mayDropContained] is false for a non-operator caller. A contained namespace is then refused. */
    fun dropNamespace(
        catalog: String,
        namespace: String,
        expectedNamespaceId: Long? = null,
        author: String? = null,
        message: String? = null,
        mayDropContained: Boolean = true,
    ): CommitResult

    // ---- tables ----------------------------------------------------------

    fun createTable(
        catalog: String,
        namespace: String,
        name: String,
        columns: List<ColumnDef>,
        author: String? = null,
        message: String? = null,
    ): TableInfo

    fun getTable(
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long? = null,
        atTimestamp: Instant? = null,
        totals: Boolean = true,
    ): TableInfo

    fun listTables(
        catalog: String,
        namespace: String,
        snapshot: Long? = null,
        atTimestamp: Instant? = null,
    ): List<TableSummaryInfo>

    fun dropTable(
        catalog: String,
        namespace: String,
        table: String,
        expectedTableUuid: UUID? = null,
        author: String? = null,
        message: String? = null,
    ): CommitResult

    fun truncateTable(
        catalog: String,
        namespace: String,
        table: String,
        expectedTableUuid: UUID,
        author: String? = null,
        message: String? = null,
    ): CommitResult

    // ---- files and changefeed --------------------------------------------

    fun listFiles(
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long? = null,
        atTimestamp: Instant? = null,
        sort: FileRepo.FileSortColumn? = null,
        desc: Boolean = false,
        limit: Int? = null,
        offset: Int = 0,
        partitionFilter: Map<Int, String> = emptyMap(),
    ): List<DataFile>

    fun partitionValues(
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long? = null,
        atTimestamp: Instant? = null,
    ): PartitionValues

    fun fileStats(
        catalog: String,
        namespace: String,
        table: String,
        fileId: Long,
        snapshot: Long? = null,
        atTimestamp: Instant? = null,
    ): FileStats

    fun changes(
        catalog: String,
        namespace: String,
        table: String,
        fromSnapshot: Long,
        toSnapshot: Long? = null,
    ): ChangesPlan

    // ---- snapshots -------------------------------------------------------

    fun resolveSnapshot(
        catalog: String,
        atTimestamp: Instant,
    ): SnapshotResolution

    /** One page of snapshots, and whether more pages exist. */
    fun listSnapshots(
        catalog: String,
        after: Long,
        limit: Int,
        before: Long? = null,
    ): Pair<List<Snapshot>, Boolean>

    // ---- commits ---------------------------------------------------------

    /**
     * [userAgent] is a parameter and not a [CommitRequest] field. The
     * receipt fingerprint covers the whole request, so a user agent in it
     * would make a replay from a different client fail.
     */
    fun commit(
        catalog: String,
        request: CommitRequest,
        userAgent: String? = null,
    ): CommitResult

    fun receipt(
        catalog: String,
        operation: UUID,
    ): CommitResult

    // ---- read planning ---------------------------------------------------

    fun planScan(
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long? = null,
        atTimestamp: Instant? = null,
        columnStats: ScanService.ColumnStatsRequest? = null,
        splitOffsets: Boolean = false,
    ): List<ScanFile>

    // ---- consumer offsets ------------------------------------------------

    fun listConsumers(catalog: String): List<ConsumerTableOffset>

    fun listOffsets(
        catalog: String,
        consumerId: String,
    ): List<ConsumerOffset>

    fun getOffset(
        catalog: String,
        consumerId: String,
        tableUuid: UUID,
    ): ConsumerOffset

    fun commitOffset(
        catalog: String,
        consumerId: String,
        tableUuid: UUID,
        snapshotId: Long,
    ): ConsumerOffset
}
