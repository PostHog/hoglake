package com.posthog.hoglake.store

import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.model.ChangesPlan
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
 * The catalog persistence seam: every catalog-scoped operation the /v1
 * routes need, behind one interface, so the backend is a STARTUP choice
 * (`HOGLAKE_DATABASE`) rather than a property of the call sites.
 *
 * Two implementations:
 *
 *  - [PostgresCatalogStore] — the historical path: it delegates to
 *    CatalogService / CommitService / ScanService unchanged, and the
 *    `gigahog` deployment runs exactly this.
 *  - [com.posthog.hoglake.hoglite.HogliteCatalogStore] — every catalog
 *    is a hoglite tenant; operations become hoglite transactions and
 *    read-only queries. Semantics (OCC guards, snapshot allocation,
 *    deletion-vector supersession, idempotent publication) are hoglake's
 *    own, re-expressed against hoglite's one-shot transaction API.
 *
 * The interface is deliberately the SERVICE surface, not a SQL
 * abstraction: hoglite transactions are single-round-trip batches with
 * declarative guards, so an interactive "open a handle, run statements"
 * seam cannot be implemented against it. What crosses the seam is
 * therefore the operation (`commit`, `getTable`, `planScan`, …), and the
 * Postgres-side behavior is defined by the services the delegate calls.
 *
 * Every route hoglake serves is implemented by BOTH backends — the
 * route table is one definition shared by them, with no 501 carve-outs
 * (the only 501s are the ones the Postgres path already serves, e.g.
 * publications/export). A backend that cannot answer at all — outage,
 * not semantics — throws
 * [com.posthog.hoglake.model.HoglakeException.BackendUnavailable] (503).
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

    fun dropNamespace(
        catalog: String,
        namespace: String,
        expectedNamespaceId: Long? = null,
        author: String? = null,
        message: String? = null,
    ): CommitResult

    // ---- tables ----------------------------------------------------------

    fun createTable(
        catalog: String,
        namespace: String,
        name: String,
        columns: List<com.posthog.hoglake.model.ColumnDef>,
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

    // ---- files + changefeed ----------------------------------------------

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

    /** One page of snapshots, plus whether more pages exist. */
    fun listSnapshots(
        catalog: String,
        after: Long,
        limit: Int,
        before: Long? = null,
    ): Pair<List<Snapshot>, Boolean>

    // ---- commits ---------------------------------------------------------

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
