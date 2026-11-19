package com.posthog.hoglake.store

import com.posthog.hoglake.model.DataFile
import com.posthog.hoglake.model.DeleteFile
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.ScanFile
import com.posthog.hoglake.model.StatsState
import com.posthog.hoglake.model.TableInfo
import com.posthog.hoglake.model.TableSummaryInfo
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.FileRepo
import com.posthog.hoglake.persistence.LayoutRepo
import com.posthog.hoglake.persistence.NamespaceRepo
import com.posthog.hoglake.persistence.TableRepo
import com.posthog.hoglake.persistence.TimeTravelRepo
import com.posthog.hoglake.persistence.getBigintListOrNull
import com.posthog.hoglake.service.ScanService
import com.posthog.hoglake.service.columnsByFieldId
import com.posthog.hoglake.service.enforceScanColumnStatsEntryCap
import com.posthog.hoglake.service.resolveColumnStats
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import java.time.Instant
import java.util.UUID
import java.util.concurrent.Semaphore

/**
 * The scoped reads of the stream half of a served `team_<id>` (below).
 *
 * Isolation is [ReadScope] (1.2) in the scan's SQL and the tables totals,
 * never anything the Client is trusted to do.
 *
 * The scan also:
 *  - carries each file's team id (its proven value on the team field) so
 *    the Client can vend a per-team credential for it;
 *  - is admitted through ONE shared concurrency cap over every export scan
 *    ([scanCap]), non-blocking, answering 503 `mount_scan_busy` when full
 *    (1.6's deferred cap; org fairness is Trino resource groups, not this);
 *  - is bounded by a file-count budget (422 `mount_scan_too_many_files`)
 *    and a statement deadline (503 `mount_scan_timeout`), both counted on
 *    `hoglake_mount_refusals_total`; and
 *  - records served/latency/in-flight metrics.
 *
 * THE STREAM HALF of a served `team_<id>` (docs/auth-tenancy.md §6.5d) is
 * read here, by the `stream*` methods: these scoped statements, cap,
 * budget and deadline, over the published tables of one configured namespace
 * with the one team the caller rule proved
 * ([StreamHalf.team]) as the only value. A table is found by its name in that
 * namespace of the stream catalog and nowhere else. Its series are labelled
 * by the family's label, never a team id or an org.
 */
class MountReadService(
    private val jdbi: Jdbi,
    /** The PostgreSQL store, for the table route's full [TableInfo] build. */
    private val source: () -> CatalogStore,
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

    private val log = KotlinLogging.logger {}

    /** The pinned source table, resolved by uuid at a read snapshot. */
    private data class Pinned(
        val catalogId: Long,
        val tableId: Long,
        val tableUuid: UUID,
        val namespaceId: Long,
        val namespace: String,
        val table: String,
        val comment: String?,
    )

    /** One scan under the shared cap, counted and timed against [label]: a mount's name or a stream's family label. */
    private fun underCap(
        label: String,
        plan: () -> List<ScanFile>,
    ): List<ScanFile> =
        withPermit(label) {
            val inFlight = Metrics.mountScanInFlight(label)?.also { it.incrementAndGet() }
            val started = System.nanoTime()
            try {
                plan().also { Metrics.mountScanServed(label, System.nanoTime() - started) }
            } finally {
                inFlight?.decrementAndGet()
            }
        }

    /** [work] holding one permit of the shared cap; 503 `mount_scan_busy`, counted against [label], when it is full. */
    private fun <R> withPermit(
        label: String,
        work: () -> R,
    ): R {
        val cap = scanCap
        if (cap != null && !cap.tryAcquire()) {
            Metrics.mountRefused(label, "mount_scan_busy")
            throw HoglakeException.MountScanBusy("too many export scans in flight; retry shortly")
        }
        // The permit is released on EVERY exit: nothing between the acquire
        // and this try can leak it.
        try {
            return work()
        } finally {
            cap?.release()
        }
    }

    // ---- the stream half of a served team_<id> (docs/auth-tenancy.md §6.5d) ----

    /** The stream namespace in the stream catalog at a read snapshot. */
    private data class StreamAt(
        val catalogId: Long,
        val at: Long,
        val namespaceId: Long,
    )

    /**
     * The published tables of [half] that exist in the stream namespace and
     * can be scoped, each with totals over team `half.team`'s files only.
     * Listed at [atTimestamp], else at the stream catalog's head: a listing's
     * `snapshot` is the import catalog's. A name with no live table, and a
     * table whose object-prefix field is not the family's predicate column, is
     * left out. So is a table whose layout and specs disagree
     * ([MountIntegrityError]), and it is counted: one table's fault must not
     * fail the listing for every team. Under the shared cap (one permit) and
     * the scan deadline.
     */
    fun streamTables(
        half: StreamHalf,
        atTimestamp: Instant?,
    ): List<TableSummaryInfo> =
        withPermit(half.label) {
            inScanTransaction(half.label) { h ->
                val at = streamAt(h, half, null, atTimestamp) ?: return@inScanTransaction emptyList()
                half.stream.tables.mapNotNull { name ->
                    val table = streamTableAt(h, half, at, name) ?: return@mapNotNull null
                    val fieldId = teamFieldId(h, half.field, table, at.at) ?: return@mapNotNull null
                    val scope =
                        try {
                            streamScope(h, half, table, fieldId, at.at)
                        } catch (e: MountIntegrityError) {
                            // The table's id, never a name from the request, a team id or an org.
                            log.warn { "served stream '${half.label}': a table is left out: ${e.message}" }
                            Metrics.servedStreamUnscopable(half.label)
                            return@mapNotNull null
                        }
                    val totals = scopedTotals(h, scope, at.at)
                    TableSummaryInfo(
                        tableId = table.tableId,
                        tableUuid = table.tableUuid,
                        name = table.table,
                        comment = table.comment,
                        recordCount = totals.records,
                        fileCount = totals.files,
                        fileSizeBytes = totals.bytes,
                        snapshotCount = 0,
                        earliestSnapshotId = null,
                    )
                }
            }
        }

    /**
     * The definition of the published table [name], from the source store,
     * at the one snapshot the check resolved: [snapshot] and [atTimestamp]
     * (or the head) are resolved once, so the definition is the table that was
     * checked. WITHOUT totals: whole-table counts are every team's. Without
     * the table's properties: on a shared table they are PostHog's own
     * metadata, and no reader needs them to read it. 404 when it is missing
     * or cannot be scoped.
     */
    fun streamTable(
        half: StreamHalf,
        name: String,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): TableInfo {
        val at = jdbi.withHandleUnchecked { h -> requireStreamTable(h, half, name, snapshot, atTimestamp).first }
        // totals=false: the identity read, no manifest aggregate and no leak.
        return source().getTable(half.stream.catalog, half.stream.namespace, name, at, null, totals = false)
            .copy(properties = emptyMap())
    }

    /** The scan of the published table [name] with team `half.team`'s files only, as [scan] plans the pinned one. */
    fun streamScan(
        half: StreamHalf,
        name: String,
        snapshot: Long?,
        atTimestamp: Instant?,
        columnStats: ScanService.ColumnStatsRequest?,
        splitOffsets: Boolean,
    ): List<ScanFile> =
        underCap(half.label) {
            inScanTransaction(half.label) { h ->
                val (at, table, fieldId) = requireStreamTable(h, half, name, snapshot, atTimestamp)
                val scope = streamScope(h, half, table, fieldId, at)
                scopedScan(h, half.label, table, scope, at, columnStats, splitOffsets)
            }
        }

    /**
     * The stream namespace at the read snapshot, or null when it does not
     * exist there. A missing stream catalog is a broken deployment: a 500
     * whose message no response carries.
     */
    private fun streamAt(
        h: Handle,
        half: StreamHalf,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): StreamAt? {
        val missing = "a served family's stream catalog does not exist"
        val (catalogId, at) = readSnapshot(h, half.stream.catalog, snapshot, atTimestamp, missing)
        val namespace = NamespaceRepo.findAt(h, catalogId, half.stream.namespace, at) ?: return null
        return StreamAt(catalogId, at, namespace.namespaceId)
    }

    /**
     * The live table [name] of the stream namespace at the read snapshot, or
     * null. Found by (stream catalog id, stream namespace id, name) and no
     * other way, so a table of another namespace is never returned.
     */
    private fun streamTableAt(
        h: Handle,
        half: StreamHalf,
        at: StreamAt,
        name: String,
    ): Pinned? =
        h.createQuery(
            """
            SELECT t.table_id, t.table_uuid, tv.comment AS table_comment
              FROM hog_table_version tv
              JOIN hog_table t ON t.catalog_id = tv.catalog_id AND t.table_id = tv.table_id
             WHERE tv.catalog_id = :catalogId AND tv.namespace_id = :namespaceId AND tv.name = :name
               AND tv.begin_snapshot <= :at AND (tv.end_snapshot IS NULL OR :at < tv.end_snapshot)
               AND (t.dropped_snapshot IS NULL OR :at < t.dropped_snapshot)
            """,
        )
            .bind("catalogId", at.catalogId)
            .bind("namespaceId", at.namespaceId)
            .bind("name", name)
            .bind("at", at.at)
            .map { rs, _ ->
                Pinned(
                    catalogId = at.catalogId,
                    tableId = rs.getLong("table_id"),
                    tableUuid = rs.getObject("table_uuid", UUID::class.java),
                    namespaceId = at.namespaceId,
                    namespace = half.stream.namespace,
                    table = name,
                    comment = rs.getString("table_comment"),
                )
            }
            .findOne()
            .orElse(null)

    /**
     * The published table [name] at its read snapshot, with its team field
     * id, or a 404 whose words name nothing: the reader answers it in the
     * served namespace's terms.
     */
    private fun requireStreamTable(
        h: Handle,
        half: StreamHalf,
        name: String,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): Triple<Long, Pinned, Long> {
        val missing = HoglakeException.NotFound("table")
        if (!half.stream.publishes(name)) throw missing
        val at = streamAt(h, half, snapshot, atTimestamp) ?: throw missing
        val table = streamTableAt(h, half, at, name) ?: throw missing
        val fieldId = teamFieldId(h, half.field, table, at.at) ?: throw missing
        return Triple(at.at, table, fieldId)
    }

    /**
     * The field a stream table is scoped on: its object-prefix field at the
     * read snapshot, when that is the top-level column [field]. Null
     * otherwise, and the table is not served: with no prefix field nothing
     * proves which team a file belongs to.
     */
    private fun teamFieldId(
        h: Handle,
        field: String,
        table: Pinned,
        at: Long,
    ): Long? {
        val fieldId = LayoutRepo.layoutAt(h, table.catalogId, table.tableId, at) ?: return null
        val path = columnsByFieldId(TableRepo.columnsAt(h, table.catalogId, table.tableId, at))[fieldId]?.first
        return fieldId.takeIf { path == field }
    }

    /**
     * The files of [table] a stream read may see: the one proven team, never
     * the caller's other granted values and never a mount's, and only while
     * the caller's grant still holds that team.
     */
    private fun streamScope(
        h: Handle,
        half: StreamHalf,
        table: Pinned,
        fieldId: Long,
        at: Long,
    ): ReadScope = ReadScope.load(h, table.catalogId, table.tableId, at, fieldId, listOf(half.team.id), half.grant)

    // ---- internals -------------------------------------------------------

    /**
     * The id of the source catalog [catalog] and the snapshot a read of it is
     * served at. A missing catalog is a broken deployment: a 500 with
     * [missing], which only the log carries.
     */
    private fun readSnapshot(
        h: Handle,
        catalog: String,
        snapshot: Long?,
        atTimestamp: Instant?,
        missing: String,
    ): Pair<Long, Long> {
        val cat = CatalogRepo.findByName(h, catalog) ?: error(missing)
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
            // hidden from a stream reader (streamAt
            // 500s rather than name it). The head/floor are the export's own
            // snapshot clock.
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
        return cat.catalogId to at
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
        label: String,
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
            Metrics.mountRefused(label, "mount_scan_too_many_files")
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
     * scoped plan"), and only the statistics rows of files in [files] are
     * read. The statement is keyed on those files' ids, so its cost is the
     * caller's plan and never the table's, which holds every org's files.
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
        val provided = files.filter { it.dataFile.statsState == StatsState.PROVIDED }.map { it.dataFile.dataFileId }
        val rowsByFile =
            FileRepo.providedColumnStatsOf(h, pinned.catalogId, pinned.tableId, provided, request.fieldIds)
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
    private fun <R> inScanTransaction(
        label: String,
        body: (Handle) -> R,
    ): R =
        jdbi.withHandleUnchecked { h ->
            h.inTransaction<R, RuntimeException> { txn ->
                if (scanDeadlineMs > 0) txn.execute("SET LOCAL statement_timeout = $scanDeadlineMs")
                try {
                    body(txn)
                } catch (e: Exception) {
                    if (isStatementTimeout(e)) {
                        Metrics.mountRefused(label, "mount_scan_timeout")
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
