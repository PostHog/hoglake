package com.posthog.hoglake.store

import com.posthog.hoglake.auth.Caller
import com.posthog.hoglake.model.DataFile
import com.posthog.hoglake.model.DeleteFile
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceInfo
import com.posthog.hoglake.model.ScanFile
import com.posthog.hoglake.model.StatsState
import com.posthog.hoglake.model.TableInfo
import com.posthog.hoglake.model.TableSummaryInfo
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.FileRepo
import com.posthog.hoglake.persistence.TableRepo
import com.posthog.hoglake.persistence.TimeTravelRepo
import com.posthog.hoglake.persistence.getBigintListOrNull
import com.posthog.hoglake.service.ScanService
import com.posthog.hoglake.service.columnsByFieldId
import com.posthog.hoglake.service.enforceScanColumnStatsEntryCap
import com.posthog.hoglake.service.resolveColumnStats
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import java.time.Instant
import java.util.UUID
import java.util.concurrent.Semaphore

/**
 * The read surface a mount (export) serves to the Client (plan row 1.3h):
 * the five scoped read routes — namespaces, namespace, tables, table and
 * scan — over the ONE source table pinned as instance configuration
 * ([sourceCatalog], [sourceTableUuid], [sourceFieldId]; #291 decision 3,
 * hoglake#557's `hog_mount` stores no source). The catalog route and the
 * listing are 1.1b's; every other route is refused ([mountRefusal]).
 *
 * Isolation is [ReadScope] (1.2) in the scan's SQL and the tables totals,
 * never anything the Client is trusted to do. The mount exposes EXACTLY
 * the pinned table: a request naming any other (namespace, table) is a 404
 * worded as a missing namespace/table, so the source catalog's other
 * tables are never reachable through the mount.
 *
 * The scan also:
 *  - carries each file's team id (its proven value on [sourceFieldId]) so
 *    the Client can vend a per-team credential for it;
 *  - is admitted through ONE shared concurrency cap over every export scan
 *    ([scanCap]), non-blocking, answering 503 `mount_scan_busy` when full
 *    (1.6's deferred cap; org fairness is Trino resource groups, not this);
 *  - is bounded by a file-count budget (422 `mount_scan_too_many_files`)
 *    and a statement deadline (503 `mount_scan_timeout`), both counted on
 *    `hoglake_mount_refusals_total`; and
 *  - records per-mount served/latency/in-flight metrics.
 */
class MountReadService(
    private val jdbi: Jdbi,
    /** The PostgreSQL store, for the table route's full [TableInfo] build. */
    private val source: () -> CatalogStore,
    private val sourceCatalog: String,
    private val sourceTableUuid: UUID?,
    private val sourceFieldId: Long?,
    private val fileBudget: Int,
    private val scanDeadlineMs: Long,
    maxConcurrentScans: Int,
) {
    /**
     * ONE cap over ALL export scans, not per mount: the invariant it keeps
     * is that Trino reads cannot starve ingest writers on the shared
     * request dispatcher. Non-blocking, so a full cap sheds rather than
     * queues. 0 (or less) disables it.
     */
    internal val scanCap: Semaphore? = maxConcurrentScans.takeIf { it > 0 }?.let { Semaphore(it) }

    /** The pinned source table, resolved by uuid at a read snapshot. */
    private data class Pinned(
        val catalogId: Long,
        val tableId: Long,
        val namespaceId: Long,
        val namespace: String,
        val table: String,
        val comment: String?,
    )

    // ---- namespaces / namespace -----------------------------------------

    /** The one namespace the pinned table lives in (containment stripped: a mount has none). */
    fun namespaces(
        snapshot: Long?,
        atTimestamp: Instant?,
    ): List<NamespaceInfo> =
        jdbi.withHandleUnchecked { h ->
            val (_, pinned) = resolve(h, snapshot, atTimestamp)
            listOf(NamespaceInfo(namespaceId = pinned.namespaceId, name = pinned.namespace))
        }

    /** [namespace] if it is the pinned table's; a 404 worded as a missing namespace otherwise. */
    fun namespace(
        namespace: String,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): NamespaceInfo =
        jdbi.withHandleUnchecked { h ->
            val (_, pinned) = resolve(h, snapshot, atTimestamp)
            if (namespace != pinned.namespace) notFoundNamespace(namespace)
            NamespaceInfo(namespaceId = pinned.namespaceId, name = pinned.namespace)
        }

    // ---- tables / table --------------------------------------------------

    /**
     * The pinned table as a [TableSummaryInfo], with SCOPED totals — the
     * record/file/byte counts over the caller's `ReadScope`, never the
     * whole table's (a count is isolation-bearing, §3.2). History counts
     * are catalog-wide, which a read-only mount does not expose, so
     * snapshotCount is 0 and earliestSnapshotId null (plan row 1.3h:
     * totals optional for a read-only catalog).
     */
    fun tables(
        caller: Caller?,
        record: MountRecord,
        namespace: String,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): List<TableSummaryInfo> =
        inScanTransaction(record) { h ->
            val (at, pinned) = resolve(h, snapshot, atTimestamp)
            if (namespace != pinned.namespace) notFoundNamespace(namespace)
            val scope = loadScope(h, caller, record, pinned, at)
            val totals = scopedTotals(h, scope, at)
            listOf(
                TableSummaryInfo(
                    tableId = pinned.tableId,
                    tableUuid = sourceTableUuid!!,
                    name = pinned.table,
                    comment = pinned.comment,
                    recordCount = totals.records,
                    fileCount = totals.files,
                    fileSizeBytes = totals.bytes,
                    snapshotCount = 0,
                    earliestSnapshotId = null,
                ),
            )
        }

    /**
     * The pinned table's [TableInfo] WITHOUT totals (plan row 1.3h: "totals
     * omitted"), delegated to the source store so columns, specs, comment
     * and properties are the real ones. A request naming any other
     * (namespace, table) is a 404, so no other table of the source is
     * reachable through the mount.
     */
    fun table(
        namespace: String,
        table: String,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): TableInfo {
        jdbi.withHandleUnchecked { h ->
            val (_, pinned) = resolve(h, snapshot, atTimestamp)
            if (namespace != pinned.namespace || table != pinned.table) notFoundTable(namespace, table)
        }
        // totals=false: the identity read, no manifest aggregate and no leak.
        return source().getTable(sourceCatalog, namespace, table, snapshot, atTimestamp, totals = false)
    }

    // ---- scan ------------------------------------------------------------

    /**
     * The scoped scan of the pinned table: `ReadScope` in the SQL, each
     * file paired with its live DV and carrying its team id, under the
     * shared cap, the file budget and the deadline. `include=column_stats`
     * is scoped to the returned files and its cap counts only them.
     */
    fun scan(
        caller: Caller?,
        record: MountRecord,
        namespace: String,
        table: String,
        snapshot: Long?,
        atTimestamp: Instant?,
        columnStats: ScanService.ColumnStatsRequest?,
        splitOffsets: Boolean,
    ): List<ScanFile> {
        val cap = scanCap
        if (cap != null && !cap.tryAcquire()) {
            Metrics.mountRefused(record.name, "mount_scan_busy")
            throw HoglakeException.MountScanBusy("too many export scans in flight; retry shortly")
        }
        // The permit is released on EVERY exit: nothing between the acquire
        // and this try can leak it.
        try {
            val inFlight = Metrics.mountScanInFlight(record.name)?.also { it.incrementAndGet() }
            val started = System.nanoTime()
            try {
                val files =
                    inScanTransaction(record) { h ->
                        val (at, pinned) = resolve(h, snapshot, atTimestamp)
                        if (namespace != pinned.namespace || table != pinned.table) notFoundTable(namespace, table)
                        val scope = loadScope(h, caller, record, pinned, at)
                        scopedScan(h, record, pinned, scope, at, columnStats, splitOffsets)
                    }
                Metrics.mountScanServed(record.name, System.nanoTime() - started)
                return files
            } finally {
                inFlight?.decrementAndGet()
            }
        } finally {
            cap?.release()
        }
    }

    // ---- internals -------------------------------------------------------

    private fun resolve(
        h: Handle,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): Pair<Long, Pinned> {
        val cat =
            CatalogRepo.findByName(h, sourceCatalog)
                ?: error("a mount resolved, but HOGLAKE_MOUNT_SOURCE_CATALOG names no catalog")
        if (snapshot != null && atTimestamp != null) {
            throw HoglakeException.Validation("snapshot and at_timestamp are mutually exclusive; supply at most one")
        }
        val at: Long
        if (atTimestamp != null) {
            at =
                TimeTravelRepo.resolveTimestamp(
                    h,
                    cat.catalogId,
                    TimeTravelRepo.earliestSnapshotId(h, cat.catalogId),
                    atTimestamp,
                )
        } else {
            at = snapshot ?: cat.headSnapshotId
            // These messages must not name the source catalog: its name is
            // hidden from a mount reader (the DTO renames it; mountSource()
            // 500s rather than name it). The head/floor are the export's own
            // snapshot clock, already on the mount's catalog DTO (1.1b).
            if (at < 0 || at > cat.headSnapshotId) {
                throw HoglakeException.Validation(
                    "snapshot $at out of range [0, ${cat.headSnapshotId}] for this export",
                )
            }
            if (snapshot != null) {
                val floor = TimeTravelRepo.expiryFloor(h, cat.catalogId)
                if (at < floor.earliestSnapshotId) {
                    throw HoglakeException.Expired(
                        "snapshot $at is below the expiry floor (earliest retained snapshot is " +
                            "${floor.earliestSnapshotId}${floor.reachedAtSuffix()}) for this export",
                    )
                }
            }
        }
        val uuid = sourceTableUuid ?: error("a mount resolved, but HOGLAKE_MOUNT_SOURCE_TABLE_UUID is not set")
        val pinned =
            h.createQuery(
                """
                SELECT t.table_id, tv.namespace_id, tv.name AS table_name, tv.comment AS table_comment,
                       n.name AS namespace_name
                  FROM hog_table t
                  JOIN hog_table_version tv ON t.catalog_id = tv.catalog_id AND t.table_id = tv.table_id
                  JOIN hog_namespace n ON n.catalog_id = tv.catalog_id AND n.namespace_id = tv.namespace_id
                 WHERE t.catalog_id = :catalogId AND t.table_uuid = :uuid
                   AND tv.begin_snapshot <= :at AND (tv.end_snapshot IS NULL OR :at < tv.end_snapshot)
                   AND (t.dropped_snapshot IS NULL OR :at < t.dropped_snapshot)
                """,
            )
                .bind("catalogId", cat.catalogId)
                .bind("uuid", uuid)
                .bind("at", at)
                .map { rs, _ ->
                    Pinned(
                        catalogId = cat.catalogId,
                        tableId = rs.getLong("table_id"),
                        namespaceId = rs.getLong("namespace_id"),
                        namespace = rs.getString("namespace_name"),
                        table = rs.getString("table_name"),
                        comment = rs.getString("table_comment"),
                    )
                }
                .findOne()
                .orElse(null)
                ?: error(
                    "HOGLAKE_MOUNT_SOURCE_TABLE_UUID $uuid is not a live table of '$sourceCatalog' at snapshot $at",
                )
        return at to pinned
    }

    private fun notFoundNamespace(namespace: String): Nothing =
        throw HoglakeException.NotFound("namespace '$namespace'")

    private fun notFoundTable(
        namespace: String,
        table: String,
    ): Nothing = throw HoglakeException.NotFound("table '$namespace.$table'")

    /**
     * The caller's `ReadScope` on the pinned table. The predicate field is
     * the source column's name (`team_id`), so the grant is
     * `caller.predicates[name]`; `*` means the mount's values and an
     * absent grant reads no file (§3.2, #147 D5/D6).
     */
    private fun loadScope(
        h: Handle,
        caller: Caller?,
        record: MountRecord,
        pinned: Pinned,
        at: Long,
    ): ReadScope {
        val fieldId = sourceFieldId ?: error("a mount resolved, but HOGLAKE_MOUNT_SOURCE_FIELD_ID is not set")
        val fieldName =
            columnsByFieldId(TableRepo.columnsAt(h, pinned.catalogId, pinned.tableId, at))[fieldId]?.second?.def?.name
                ?: throw MountIntegrityError("source field $fieldId is not a column of the pinned table")
        return ReadScope.load(
            h,
            pinned.catalogId,
            pinned.tableId,
            at,
            fieldId,
            record.values,
            caller?.predicates?.get(fieldName),
        )
    }

    private data class Totals(val records: Long, val files: Long, val bytes: Long)

    private fun scopedTotals(
        h: Handle,
        scope: ReadScope,
        at: Long,
    ): Totals {
        if (scope.isEmpty) return Totals(0, 0, 0)
        return scope.bind(
            h.createQuery(
                """
                SELECT COALESCE(SUM(df.record_count), 0) AS records,
                       COUNT(*) AS files,
                       COALESCE(SUM(df.file_size_bytes), 0) AS bytes
                  FROM hog_data_file df
                 WHERE ${FileRepo.visibleAt("df")} AND ${scope.sql("df")}
                """,
            ),
        )
            .bind("snapshot", at)
            .map { rs, _ -> Totals(rs.getLong("records"), rs.getLong("files"), rs.getLong("bytes")) }
            .one()
    }

    private fun scopedScan(
        h: Handle,
        record: MountRecord,
        pinned: Pinned,
        scope: ReadScope,
        at: Long,
        columnStats: ScanService.ColumnStatsRequest?,
        splitOffsets: Boolean,
    ): List<ScanFile> {
        if (scope.isEmpty) return emptyList()
        // key_index of the source field per spec, so each file's team id is
        // read from its own partition values (ReadScope keeps key_indexes
        // contiguous from 0, so the array index is the key_index).
        val keyBySpec = scope.arms.flatMap { arm -> arm.specIds.map { it to arm.keyIndex } }.toMap()
        val files =
            scope.bind(
                h.createQuery(
                    """
                    SELECT df.data_file_id, df.table_id, df.path, df.file_format,
                           df.record_count, df.file_size_bytes, df.footer_size,
                           df.row_id_start, df.stats_state, df.begin_snapshot, df.spec_id,
                           df.explicit_row_ids,
                           CASE WHEN :splitOffsets THEN df.split_offsets END AS split_offsets,
                           pv.partition_values,
                           dv.delete_file_id AS dv_id, dv.path AS dv_path,
                           dv.file_format AS dv_format, dv.delete_count AS dv_delete_count,
                           dv.file_size_bytes AS dv_file_size_bytes,
                           dv.begin_snapshot AS dv_begin_snapshot
                      FROM hog_data_file df
                      LEFT JOIN hog_delete_file dv
                        ON dv.catalog_id = df.catalog_id
                       AND dv.data_file_id = df.data_file_id
                       AND ${FileRepo.visibleAt("dv")}
                      LEFT JOIN LATERAL (
                            SELECT array_agg(v.value ORDER BY v.key_index) AS partition_values
                              FROM hog_file_partition_value v
                             WHERE v.catalog_id = df.catalog_id
                               AND v.data_file_id = df.data_file_id
                           ) pv ON true
                     WHERE ${FileRepo.visibleAt("df")} AND ${scope.sql("df")}
                     ORDER BY df.row_id_start, df.data_file_id
                     LIMIT :fileLimit
                    """,
                ),
            )
                .bind("snapshot", at)
                .bind("splitOffsets", splitOffsets)
                // One over the budget, so a result AT the budget is served and only a true overflow is refused.
                .bind("fileLimit", fileBudget.toLong() + 1)
                .map { rs, _ -> scanRow(rs, keyBySpec) }
                .list()
        if (files.size > fileBudget) {
            Metrics.mountRefused(record.name, "mount_scan_too_many_files")
            throw HoglakeException.MountScanTooManyFiles(
                "the scoped scan returns more than the file budget of $fileBudget files; the scope is fixed by the " +
                    "token, so this table is too large to plan through the export and must be read another way",
            )
        }
        return if (columnStats == null) files else withScopedColumnStats(h, pinned, at, files, columnStats)
    }

    /**
     * Column statistics for the SCOPED files only: the cap counts the
     * scoped provided-file count (plan row 1.3h, "stats cap applied to the
     * scoped plan"), and only files in [files] are given rows.
     */
    private fun withScopedColumnStats(
        h: Handle,
        pinned: Pinned,
        at: Long,
        files: List<ScanFile>,
        request: ScanService.ColumnStatsRequest,
    ): List<ScanFile> {
        if (files.none { it.dataFile.statsState == StatsState.PROVIDED }) return files
        val byFieldId = columnsByFieldId(TableRepo.columnsAt(h, pinned.catalogId, pinned.tableId, at))
        val leafFieldIds = byFieldId.filterValues { (_, column) -> column.children.isEmpty() }.keys
        enforceScanColumnStatsEntryCap(
            files.count { it.dataFile.statsState == StatsState.PROVIDED }.toLong(),
            leafFieldIds,
            request,
        )
        val scopedIds = files.map { it.dataFile.dataFileId }.toSet()
        val rowsByFile =
            FileRepo.providedColumnStatsAt(h, pinned.catalogId, pinned.tableId, at, request.fieldIds)
                .filterKeys { it in scopedIds }
        return files.map { file ->
            if (file.dataFile.statsState != StatsState.PROVIDED) return@map file
            val rows = rowsByFile[file.dataFile.dataFileId].orEmpty()
            file.copy(dataFile = file.dataFile.copy(columnStats = resolveColumnStats(rows, byFieldId)))
        }
    }

    private fun scanRow(
        rs: java.sql.ResultSet,
        keyBySpec: Map<Long, Int>,
    ): ScanFile {
        val specId = rs.getLong("spec_id").let { if (rs.wasNull()) null else it }
        val values = rs.getArray("partition_values")?.let { arr -> (arr.array as Array<*>).map { it as String? } }
        val dataFile =
            DataFile(
                dataFileId = rs.getLong("data_file_id"),
                tableId = rs.getLong("table_id"),
                path = rs.getString("path"),
                fileFormat = rs.getString("file_format"),
                recordCount = rs.getLong("record_count"),
                fileSizeBytes = rs.getLong("file_size_bytes"),
                footerSize = rs.getLong("footer_size").let { if (rs.wasNull()) null else it },
                rowIdStart = rs.getLong("row_id_start"),
                statsState = StatsState.fromWire(rs.getString("stats_state")),
                beginSnapshot = rs.getLong("begin_snapshot"),
                specId = specId,
                partitionValues = values,
                explicitRowIds = rs.getBoolean("explicit_row_ids"),
                splitOffsets = rs.getBigintListOrNull("split_offsets"),
            )
        val dvId = rs.getLong("dv_id")
        val deleteFile =
            if (rs.wasNull()) {
                null
            } else {
                DeleteFile(
                    deleteFileId = dvId,
                    dataFileId = dataFile.dataFileId,
                    path = rs.getString("dv_path"),
                    fileFormat = rs.getString("dv_format"),
                    deleteCount = rs.getLong("dv_delete_count"),
                    fileSizeBytes = rs.getLong("dv_file_size_bytes"),
                    beginSnapshot = rs.getLong("dv_begin_snapshot"),
                )
            }
        // The team id is the file's proven value on the source field, read
        // at that spec's key_index. ReadScope admitted the file, so the
        // value is present and canonical; defence in depth leaves it null
        // rather than throwing if a row is somehow malformed.
        val teamId = specId?.let { keyBySpec[it] }?.let { values?.getOrNull(it)?.toLongOrNull() }
        return ScanFile(dataFile, deleteFile, teamId)
    }

    /**
     * Run [body] in a read transaction with the scan deadline as a LOCAL
     * `statement_timeout`, mapping Postgres's cancellation (SQLSTATE 57014)
     * to 503 `mount_scan_timeout`. This bounds the time; the file budget
     * bounds the returned rows. The InitPlan array ReadScope collects is
     * bounded by neither (see [ReadScope]).
     */
    internal fun <R> inScanTransaction(
        record: MountRecord,
        body: (Handle) -> R,
    ): R =
        jdbi.withHandleUnchecked { h ->
            h.inTransaction<R, RuntimeException> { txn ->
                if (scanDeadlineMs > 0) txn.execute("SET LOCAL statement_timeout = $scanDeadlineMs")
                try {
                    body(txn)
                } catch (e: Exception) {
                    if (isStatementTimeout(e)) {
                        Metrics.mountRefused(record.name, "mount_scan_timeout")
                        throw HoglakeException.MountScanTimeout(
                            "the export scan ran past the ${scanDeadlineMs}ms deadline; retry shortly",
                        )
                    }
                    throw e
                }
            }
        }

    companion object {
        private fun isStatementTimeout(e: Throwable?): Boolean {
            var t = e
            while (t != null) {
                if (t is java.sql.SQLException && t.sqlState == "57014") return true
                t = t.cause
            }
            return false
        }
    }
}
