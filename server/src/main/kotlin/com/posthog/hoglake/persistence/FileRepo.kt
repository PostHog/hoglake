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
            .map(columnStatsMapper)
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
            .map { rs, ctx -> rs.getLong("data_file_id") to columnStatsMapper.map(rs, ctx) }
            .list()
            .toMap()
    }

    /**
     * The scan plan's stats statement, as SQL text — `internal` so the
     * query-plan test can EXPLAIN the statement production runs rather
     * than a retyped copy of it (AGENT.md: an index proves itself
     * against the query it serves, and V14's first index was green
     * against a predicate no code path issues).
     *
     * [filtered] selects the `stats_fields` shape. The two shapes get
     * different plans and both are asserted, so the flag is the whole
     * reason this is a function rather than a constant; nothing but the
     * literal filter text is spliced in (invariant 9).
     */
    internal fun providedColumnStatsSql(filtered: Boolean): String =
        """
        SELECT s.data_file_id, s.field_id, s.value_count, s.null_count, s.nan_count,
               s.lower_bound, s.upper_bound
          FROM hog_data_file df
          JOIN hog_file_column_stats s
            ON s.catalog_id = df.catalog_id
           AND s.data_file_id = df.data_file_id
         WHERE df.catalog_id = :catalogId AND df.table_id = :tableId
           AND df.stats_state = 'provided'
           AND ${visibleAt("df")}
           ${if (filtered) "AND s.field_id = ANY(:fieldIds)" else ""}
         ORDER BY s.data_file_id, s.field_id
        """

    /**
     * The stats rows of every `provided` file of [tableId] visible at
     * [snapshot], grouped by data file id, field-id order within each —
     * narrowed to [fieldIds] when given (the scan plan's
     * `include=column_stats` / `stats_fields`).
     *
     * ONE statement for the whole plan. It re-applies the plan's
     * file-visibility predicate as a join ([visibleAt], the same text the
     * plan uses) rather than taking the id list back as `IN (...)`: a large table's plan would otherwise bind one
     * parameter per file, and the protocol caps a statement at 65535.
     * The field narrowing is ONE array parameter however many ids.
     *
     * The `stats_state = 'provided'` filter is part of the STATEMENT and
     * not only of the caller's guard: this function is the one place a
     * `pending` file's half-written rows could reach a plan, and a
     * reader that prunes on them prunes on bounds no writer vouched for.
     * `FileColumnStatsRepoIntegrationTest` calls it directly, against a
     * pending file that HAS rows, for exactly that reason.
     *
     * `size_bytes` is deliberately NOT selected. The scan plan's wire
     * entry drops it (ScanColumnStats: it says nothing about whether a
     * file can be pruned, which is the array's one job) and this
     * function serves the scan plan alone, so selecting it would move
     * one bigint per file per column across the wire for a value nobody
     * renders. The returned [ColumnStats.sizeBytes] is therefore always
     * null, and null here means NOT READ rather than NOT STORED — every
     * other reader ([columnStats], [columnStatsFor]) selects the column
     * and means the other thing.
     */
    fun providedColumnStatsAt(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
        fieldIds: Set<Long>?,
    ): Map<Long, List<ColumnStats>> {
        val query =
            handle.createQuery(providedColumnStatsSql(filtered = fieldIds != null))
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("snapshot", snapshot)
        if (fieldIds != null) query.bindArray("fieldIds", Long::class.javaObjectType, fieldIds.toList())
        return query
            .map { rs, ctx -> rs.getLong("data_file_id") to scanColumnStatsMapper.map(rs, ctx) }
            .list()
            .groupBy({ it.first }, { it.second })
    }

    /**
     * The versioned-row visibility predicate (invariant 6) for [alias] at
     * `:snapshot`, as SQL text. The scan plan builds its file and
     * deletion-vector joins from it and [providedColumnStatsAt] its stats
     * join, so a plan's stats can never be read under different snapshot
     * semantics than its files. [alias] is always a literal table alias at
     * the call site; no value is ever spliced in (invariant 9).
     */
    internal fun visibleAt(alias: String): String =
        "$alias.begin_snapshot <= :snapshot AND ($alias.end_snapshot IS NULL OR :snapshot < $alias.end_snapshot)"

    /**
     * One `hog_file_column_stats` row. Shared by every stats read so a new
     * column on the table is mapped in one place, not in each query.
     */
    private val columnStatsMapper =
        RowMapper { rs, _ ->
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

    /**
     * [columnStatsMapper] minus `size_bytes`, for
     * [providedColumnStatsAt] — the one read whose statement does not
     * select the column (see that function). A separate mapper rather
     * than a nullable lookup, so the omission is stated where the rows
     * are built and a future column added to [columnStatsMapper] cannot
     * silently start throwing here.
     */
    private val scanColumnStatsMapper =
        RowMapper { rs, _ ->
            ColumnStats(
                fieldId = rs.getLong("field_id"),
                valueCount = rs.getLong("value_count"),
                nullCount = rs.getLong("null_count"),
                nanCount = rs.getObject("nan_count")?.let { (it as Number).toLong() },
                sizeBytes = null,
                lowerBound = rs.getBytes("lower_bound"),
                upperBound = rs.getBytes("upper_bound"),
            )
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
