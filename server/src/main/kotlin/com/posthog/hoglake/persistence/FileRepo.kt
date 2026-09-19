package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.DataFile
import com.posthog.hoglake.model.StatsState
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.mapper.RowMapper

/**
 * hog_data_file reads for planning (listFiles) and the changefeed, plus
 * the drop-time end-snapshotting. File INSERTs belong to the commit
 * service, not here.
 */
object FileRepo {
    /**
     * The columns a client may sort the file listing by, each mapped to
     * its SQL column. A CLOSED set, because the value is interpolated
     * into ORDER BY — only the raw stored columns are here. The decoded,
     * polymorphic columns the webui also shows (partition, and the
     * ordering-key min/max) are absent on purpose: their display order is
     * computed from encoded bytes / a text[] of ordinals and does not
     * match any single SQL column's order, so they cannot be sorted
     * server-side and are not offered.
     */
    enum class FileSortColumn(val wire: String, val sql: String) {
        ID("id", "data_file_id"),
        PATH("path", "path"),
        RECORD_COUNT("record_count", "record_count"),
        SIZE("size", "file_size_bytes"),
        STATS("stats", "stats_state"),
        BEGIN_SNAPSHOT("begin_snapshot", "begin_snapshot"),
        ;

        companion object {
            fun fromWire(s: String): FileSortColumn? = entries.firstOrNull { it.wire == s }
        }
    }

    private val fileMapper =
        RowMapper { rs, _ ->
            DataFile(
                dataFileId = rs.getLong("data_file_id"),
                tableId = rs.getLong("table_id"),
                path = rs.getString("path"),
                fileFormat = rs.getString("file_format"),
                recordCount = rs.getLong("record_count"),
                fileSizeBytes = rs.getLong("file_size_bytes"),
                footerSize = rs.getObject("footer_size")?.let { (it as Number).toLong() },
                rowIdStart = rs.getLong("row_id_start"),
                statsState = StatsState.fromWire(rs.getString("stats_state")),
                beginSnapshot = rs.getLong("begin_snapshot"),
                specId = rs.getObject("spec_id")?.let { (it as Number).toLong() },
                partitionValues =
                    (rs.getArray("partition_values")?.array as? Array<*>)
                        ?.map { it as String? }
                        ?.takeIf { it.isNotEmpty() },
                explicitRowIds = rs.getBoolean("explicit_row_ids"),
            )
        }

    private const val COLUMNS =
        """f.data_file_id, f.table_id, f.path, f.file_format, f.record_count,
           f.file_size_bytes, f.footer_size, f.row_id_start, f.stats_state,
           f.begin_snapshot, f.spec_id, f.explicit_row_ids,
           (SELECT array_agg(pv.value ORDER BY pv.key_index)
            FROM hog_file_partition_value pv
            WHERE pv.catalog_id = f.catalog_id
              AND pv.data_file_id = f.data_file_id) AS partition_values"""

    /**
     * Files visible at [snapshot].
     *
     * Default ([sort] null): manifest order (begin_snapshot, row_id_start,
     * data_file_id) — the order every existing caller and the unpaged
     * listing rely on. A [sort] orders by that column with a fixed
     * data_file_id tiebreak, so the total order is deterministic across
     * offset pages (two files with the same value never swap between
     * pages). [limit] null returns everything (the historical, unbounded
     * behavior); a non-null [limit]/[offset] pages the result.
     *
     * [sort.sql] is a controlled literal from a closed enum, never client
     * text, so interpolating it into ORDER BY carries no injection.
     */
    fun listAt(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
        sort: FileSortColumn? = null,
        desc: Boolean = false,
        limit: Int? = null,
        offset: Int = 0,
    ): List<DataFile> {
        val dir = if (desc) "DESC" else "ASC"
        val orderBy =
            if (sort == null) {
                "begin_snapshot, row_id_start, data_file_id"
            } else {
                // Fixed ascending id tiebreak: a stable total order under
                // paging regardless of the primary direction.
                "${sort.sql} $dir, data_file_id ASC"
            }
        val paging = if (limit == null) "" else "LIMIT :limit OFFSET :offset"
        return handle.createQuery(
            """
            SELECT $COLUMNS
            FROM hog_data_file f
            WHERE f.catalog_id = :catalogId AND f.table_id = :tableId
              AND f.begin_snapshot <= :snapshot
              AND (f.end_snapshot IS NULL OR :snapshot < f.end_snapshot)
            ORDER BY $orderBy
            $paging
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("snapshot", snapshot)
            .apply {
                if (limit != null) {
                    bind("limit", limit)
                    bind("offset", offset)
                }
            }
            .map(fileMapper)
            .list()
    }

    /**
     * One file by id, IF it belongs to [tableId] and is visible at
     * [snapshot] — the read the per-file stats endpoint resolves
     * through, so a file id from another table (or a not-yet/no-longer
     * visible file) answers null and the route 404s.
     */
    fun findAt(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
        dataFileId: Long,
        snapshot: Long,
    ): DataFile? =
        handle.createQuery(
            """
            SELECT $COLUMNS
            FROM hog_data_file f
            WHERE f.catalog_id = :catalogId AND f.table_id = :tableId
              AND f.data_file_id = :dataFileId
              AND f.begin_snapshot <= :snapshot
              AND (f.end_snapshot IS NULL OR :snapshot < f.end_snapshot)
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("dataFileId", dataFileId)
            .bind("snapshot", snapshot)
            .map(fileMapper)
            .findOne()
            .orElse(null)

    /**
     * The stored `hog_file_column_stats` rows for one file, field-id
     * order. Rows are NOT versioned (they live and die with the file),
     * so there is no snapshot predicate here; visibility is the FILE's,
     * checked by [findAt].
     */
    fun columnStats(
        handle: Handle,
        catalogId: Long,
        dataFileId: Long,
    ): List<ColumnStats> =
        handle.createQuery(
            """
            SELECT field_id, value_count, null_count, nan_count, size_bytes,
                   lower_bound, upper_bound
            FROM hog_file_column_stats
            WHERE catalog_id = :catalogId AND data_file_id = :dataFileId
            ORDER BY field_id
            """,
        )
            .bind("catalogId", catalogId)
            .bind("dataFileId", dataFileId)
            .map { rs, _ ->
                ColumnStats(
                    fieldId = rs.getLong("field_id"),
                    valueCount = rs.getLong("value_count"),
                    nullCount = rs.getLong("null_count"),
                    nanCount = rs.getObject("nan_count")?.let { (it as Number).toLong() },
                    sizeBytes = rs.getObject("size_bytes")?.let { (it as Number).toLong() },
                    lowerBound = rs.getBytes("lower_bound"),
                    upperBound = rs.getBytes("upper_bound"),
                )
            }
            .list()

    /**
     * ONE field's stats rows across MANY files, keyed by data file id —
     * the files listing's ordering-key bounds.
     *
     * Deliberately a single query over the whole page rather than
     * [columnStats] per row: the listing returns every live file at the
     * snapshot, and a per-file read would turn one screen into
     * thousands of round trips for a column the table shows in every
     * row. Files with no row for [fieldId] (stats pending or failed, a
     * column added after the file landed, a heterogeneous compaction
     * group) are simply absent from the map — the caller renders that
     * as "no bounds", never as a zero.
     */
    fun columnStatsFor(
        handle: Handle,
        catalogId: Long,
        dataFileIds: List<Long>,
        fieldId: Long,
    ): Map<Long, ColumnStats> {
        // `IN (<ids>)` with an empty list is not valid SQL, and an empty
        // table has no files to bound.
        if (dataFileIds.isEmpty()) return emptyMap()
        return handle.createQuery(
            """
            SELECT data_file_id, field_id, value_count, null_count, nan_count,
                   size_bytes, lower_bound, upper_bound
            FROM hog_file_column_stats
            WHERE catalog_id = :catalogId AND field_id = :fieldId
              AND data_file_id IN (<dataFileIds>)
            """,
        )
            .bind("catalogId", catalogId)
            .bind("fieldId", fieldId)
            .bindList("dataFileIds", dataFileIds)
            .map { rs, _ ->
                rs.getLong("data_file_id") to
                    ColumnStats(
                        fieldId = rs.getLong("field_id"),
                        valueCount = rs.getLong("value_count"),
                        nullCount = rs.getLong("null_count"),
                        nanCount = rs.getObject("nan_count")?.let { (it as Number).toLong() },
                        sizeBytes = rs.getObject("size_bytes")?.let { (it as Number).toLong() },
                        lowerBound = rs.getBytes("lower_bound"),
                        upperBound = rs.getBytes("upper_bound"),
                    )
            }
            .list()
            .toMap()
    }

    /**
     * Snapshot-scoped table aggregates: (file_count, record_count,
     * file_size_bytes) over the files VISIBLE at [snapshot]. TableInfo
     * must use this, never hog_table_stats — the stats row is the gross
     * append counter (row-id allocator anchor) and is head-scoped by
     * nature; aggregating visible files keeps time-travel reads honest
     * (found by pyhoglake's integration suite, 2026-09-05).
     */
    data class TableAggregates(val fileCount: Long, val recordCount: Long, val fileSizeBytes: Long)

    fun aggregateAt(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
    ): TableAggregates =
        handle.createQuery(
            """
            SELECT count(*) AS fc, coalesce(sum(record_count), 0) AS rc,
                   coalesce(sum(file_size_bytes), 0) AS fb
            FROM hog_data_file
            WHERE catalog_id = :catalogId AND table_id = :tableId
              AND begin_snapshot <= :snapshot
              AND (end_snapshot IS NULL OR :snapshot < end_snapshot)
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("snapshot", snapshot)
            .map { rs, _ -> TableAggregates(rs.getLong("fc"), rs.getLong("rc"), rs.getLong("fb")) }
            .one()

    /** Count of files visible at [snapshot]. */
    fun countAt(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
    ): Long =
        handle.createQuery(
            """
            SELECT count(*) FROM hog_data_file
            WHERE catalog_id = :catalogId AND table_id = :tableId
              AND begin_snapshot <= :snapshot
              AND (end_snapshot IS NULL OR :snapshot < end_snapshot)
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("snapshot", snapshot)
            .mapTo(Long::class.javaObjectType)
            .one()

    /**
     * Changefeed range read: files appended in (from, to] — i.e.
     * begin_snapshot > [fromSnapshot] AND begin_snapshot <= [toSnapshot]
     * — ordered by begin_snapshot, row_id_start. No end_snapshot filter:
     * the feed reports what was appended in the range, regardless of
     * later lifecycle.
     *
     * Compaction outputs are EXCLUDED (the NOT EXISTS against the
     * snapshot's 'table_compacted' change row): a compaction rewrites
     * rows the feed already delivered under their original files/ranges,
     * so surfacing the output here would re-deliver every merged row as
     * a fresh append. A consumer replaying across a compaction must see
     * the ORIGINAL files in their original ranges and never the
     * compacted file.
     */
    fun changedIn(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
        fromSnapshot: Long,
        toSnapshot: Long,
    ): List<DataFile> =
        handle.createQuery(
            """
            SELECT $COLUMNS
            FROM hog_data_file f
            WHERE f.catalog_id = :catalogId AND f.table_id = :tableId
              AND f.begin_snapshot > :fromSnapshot
              AND f.begin_snapshot <= :toSnapshot
              AND NOT EXISTS (
                    SELECT 1 FROM hog_snapshot_change c
                    WHERE c.catalog_id = f.catalog_id
                      AND c.snapshot_id = f.begin_snapshot
                      AND c.kind = 'table_compacted'
                      AND c.object_id = f.table_id)
            ORDER BY begin_snapshot, row_id_start, data_file_id
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("fromSnapshot", fromSnapshot)
            .bind("toSnapshot", toSnapshot)
            .map(fileMapper)
            .list()

    /**
     * Drop tail, DV half: end-snapshot every live deletion vector of the
     * table (same UPDATE pattern as [endLiveFiles]). Runs BEFORE the
     * data-file pass in dropTable so no live DV survives its data file's
     * retirement; expiry's step-1 range predicate then reclaims the row
     * and queues the object once the drop snapshot sinks under the floor.
     */
    fun endLiveDeleteFiles(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
    ): Int =
        handle.createUpdate(
            """
            UPDATE hog_delete_file SET end_snapshot = :snapshot
            WHERE catalog_id = :catalogId AND table_id = :tableId AND end_snapshot IS NULL
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("snapshot", snapshot)
            .execute()

    /** Drop tail: end-snapshot every live file of the table. */
    fun endLiveFiles(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
    ): Int =
        handle.createUpdate(
            """
            UPDATE hog_data_file SET end_snapshot = :snapshot
            WHERE catalog_id = :catalogId AND table_id = :tableId AND end_snapshot IS NULL
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("snapshot", snapshot)
            .execute()
}
