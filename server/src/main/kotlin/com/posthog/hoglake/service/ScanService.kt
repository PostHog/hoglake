package com.posthog.hoglake.service

import com.posthog.hoglake.model.DataFile
import com.posthog.hoglake.model.DeleteFile
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.ScanFile
import com.posthog.hoglake.model.StatsState
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.FileRepo
import com.posthog.hoglake.persistence.NamespaceRepo
import com.posthog.hoglake.persistence.TableRepo
import com.posthog.hoglake.persistence.TimeTravelRepo
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import java.time.Instant

/**
 * Read planning: the data files visible at a snapshot, each paired with
 * the deletion-vector file visible at that same snapshot (at most one by
 * construction — the DV supersession chain end-snapshots the old vector
 * when a new one is registered, so time travel at an old snapshot
 * correctly returns the DV that was live back then).
 *
 * Visibility is the standard versioned-row rule on both sides:
 * begin_snapshot <= S AND (end_snapshot IS NULL OR S < end_snapshot).
 *
 * Each DataFile carries its partitioning binding (spec_id + the
 * transformed partition values in key_index order) when the file was
 * written under a partition spec. On request ([ColumnStatsRequest]) a
 * `provided` file also carries its stored column-stats rows, so an
 * engine can prune files at planning time instead of opening each
 * footer. Opt-in because most scan consumers (the DuckDB client,
 * pyhoglake, an unfiltered engine scan) cannot use bounds and should not
 * pay the join or the payload.
 *
 * Statement shape, independent of the file count: the catalog /
 * namespace / table resolution, ONE file+DV query, then (only when stats
 * were requested and some file is `provided`) ONE column query and ONE
 * stats query. Never a statement per file.
 */
class ScanService(private val jdbi: Jdbi) {
    /**
     * Which column statistics a scan plan should carry. [fieldIds] null
     * means every stored row; a set narrows the rows to those field ids
     * (the columns an engine's pushed-down predicate names), so a
     * one-column filter over a wide table fetches files x 1 rows, not
     * files x columns. Field ids that name no visible leaf simply match
     * nothing — the answer is reflective, as on the stats endpoint.
     */
    data class ColumnStatsRequest(val fieldIds: Set<Long>? = null)

    fun planScan(
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long? = null,
        atTimestamp: Instant? = null,
        columnStats: ColumnStatsRequest? = null,
    ): List<ScanFile> =
        jdbi.withHandleUnchecked { h ->
            val cat =
                CatalogRepo.findByName(h, catalog)
                    ?: throw HoglakeException.NotFound("catalog '$catalog'")
            // Same read-target rules as CatalogService.getTable/listFiles:
            // at most one of snapshot / at_timestamp; explicit targets below
            // the expiry floor are Expired (410); head reads never are.
            if (snapshot != null && atTimestamp != null) {
                throw HoglakeException.Validation(
                    "snapshot and at_timestamp are mutually exclusive; supply at most one",
                )
            }
            val at: Long
            if (atTimestamp != null) {
                at =
                    TimeTravelRepo.resolveTimestamp(
                        h, cat.catalogId, TimeTravelRepo.earliestSnapshotId(h, cat.catalogId), atTimestamp,
                    )
            } else {
                at = snapshot ?: cat.headSnapshotId
                if (at < 0 || at > cat.headSnapshotId) {
                    throw HoglakeException.Validation(
                        "snapshot $at out of range [0, ${cat.headSnapshotId}] for catalog '${cat.name}'",
                    )
                }
                if (snapshot != null) {
                    val floor = TimeTravelRepo.expiryFloor(h, cat.catalogId)
                    if (at < floor.earliestSnapshotId) {
                        throw HoglakeException.Expired(
                            "snapshot $at is below the expiry floor (earliest retained " +
                                "snapshot is ${floor.earliestSnapshotId}" +
                                "${floor.reachedAtSuffix()}) for catalog '${cat.name}'",
                        )
                    }
                }
            }
            val ns =
                NamespaceRepo.findLiveByName(h, cat.catalogId, namespace)
                    ?: throw HoglakeException.NotFound("namespace '$namespace' in catalog '$catalog'")
            val t =
                TableRepo.findAt(h, cat.catalogId, ns.namespaceId, table, at)
                    ?: throw HoglakeException.NotFound(
                        "table '$namespace.$table' in catalog '$catalog' at snapshot $at",
                    )

            h.createQuery(
                """
            SELECT df.data_file_id, df.table_id, df.path, df.file_format,
                   df.record_count, df.file_size_bytes, df.footer_size,
                   df.row_id_start, df.stats_state, df.begin_snapshot, df.spec_id,
                   df.explicit_row_ids,
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
             WHERE df.catalog_id = :catalogId AND df.table_id = :tableId
               AND ${FileRepo.visibleAt("df")}
             ORDER BY df.row_id_start, df.data_file_id
            """,
            )
                .bind("catalogId", cat.catalogId)
                .bind("tableId", t.tableId)
                .bind("snapshot", at)
                .map { rs, _ ->
                    val specId = rs.getLong("spec_id").let { if (rs.wasNull()) null else it }
                    val values =
                        rs.getArray("partition_values")?.let { arr ->
                            (arr.array as Array<*>).map { it as String? }
                        }
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
                    ScanFile(dataFile, deleteFile)
                }
                .list()
                .let { files ->
                    if (columnStats == null) {
                        files
                    } else {
                        withColumnStats(
                            h,
                            cat.catalogId,
                            t.tableId,
                            at,
                            files,
                            columnStats,
                        )
                    }
                }
        }

    /**
     * Attach each `provided` file's stats rows, resolved against the
     * columns visible at [at] exactly as GET .../files/{fileId}/stats
     * resolves them (same join, same omissions), so the two surfaces
     * answer identically for the same file and snapshot.
     *
     * One stats query for the whole plan, not one per file
     * ([FileRepo.providedColumnStatsAt]). The attachment is keyed off the
     * plan, not the query: a file the hydrator flips to `provided` between
     * the two statements was `pending` in the plan, and stays stat-less in
     * it rather than contradicting its own stats_state.
     */
    private fun withColumnStats(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        at: Long,
        files: List<ScanFile>,
        request: ColumnStatsRequest,
    ): List<ScanFile> {
        if (files.none { it.dataFile.statsState == StatsState.PROVIDED }) return files
        val byFieldId = columnsByFieldId(TableRepo.columnsAt(h, catalogId, tableId, at))
        val rowsByFile =
            FileRepo.providedColumnStatsAt(h, catalogId, tableId, at, request.fieldIds)
        return files.map { file ->
            if (file.dataFile.statsState != StatsState.PROVIDED) return@map file
            val rows = rowsByFile[file.dataFile.dataFileId].orEmpty()
            file.copy(dataFile = file.dataFile.copy(columnStats = resolveColumnStats(rows, byFieldId)))
        }
    }
}
