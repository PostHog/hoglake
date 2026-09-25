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
import com.posthog.hoglake.persistence.getBigintListOrNull
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
 * Also on request (`splitOffsets`), every file the catalog holds
 * row-group start offsets for carries them (`hog_data_file.split_offsets`,
 * [com.posthog.hoglake.model.SplitOffsets]), so an engine can cut
 * byte-range splits on row-group boundaries. That column lives on the
 * file row itself, so asking for it adds no statement — the file query
 * selects it under a bound flag and returns NULL for it otherwise, which
 * keeps the arrays out of an ordinary plan's result set. A file with no
 * stored offsets simply carries none; `stats_state` does not matter here,
 * because a provided-stats registration may lack offsets and a pending
 * one may have shipped them.
 *
 * Statement shape, independent of the file count: the catalog /
 * namespace / table resolution, ONE file+DV query, then (only when stats
 * were requested and some file is `provided`) ONE column query and ONE
 * stats query. Never a statement per file.
 */
class ScanService(private val jdbi: Jdbi) {
    companion object {
        /**
         * Most `column_stats` entries ONE scan plan may carry — the
         * bound this response had none of.
         *
         * Every other listing on the API has a ceiling (`/files` 10,000
         * per page, `/snapshots` 100, verify samples 20). This one
         * multiplies: entries are provided files x requested columns,
         * so a table that is unremarkable on both axes is enormous on
         * the product. MEASURED at 106 bytes per rendered entry on the
         * slim ScanColumnStats shape, which puts a 20,000-file,
         * 30-column table at 66 MB from one GET and a 50,000-file,
         * 40-column one at ~225 MB. A million entries is therefore
         * about 100 MB: the largest answer this endpoint is willing to
         * build in memory, and still a payload no engine should want.
         *
         * The lever for a caller over it is `stats_fields`, which
         * divides the second factor by naming only the columns a
         * pushed-down predicate uses — and which the refusal message
         * names, because a 422 that does not say what to do instead is
         * an outage to the operator reading it.
         *
         * IT ONLY DIVIDES THE SECOND FACTOR. One column costs one entry
         * per file, so a table with more than a million provided files
         * is past this cap at every narrowing and `include=column_stats`
         * is unreachable for it — an engine planning such a table gets
         * no bounds and must read every file. That is not hypothetical
         * at the top of the fleet: 3M files x 15 columns is 45x the
         * cap, and one column of it is still 3x. Raising the cap is not
         * the answer either, because the number it bounds is a RESPONSE
         * this server builds in memory; the answer, when a table needs
         * it, is a paged or filtered statistics surface, which this
         * endpoint is not and which is not built here.
         *
         * This bounds the STATISTICS only. The plan's file list is
         * uncapped and stays that way: a scan that returned some of a
         * table's files would be a wrong answer, where a scan that
         * returns them without bounds is a slow one.
         *
         * Sibling of `FILES_MAX_LIMIT` in api/Routes.kt; it lives here
         * rather than there because the count is only knowable once the
         * plan and the table's columns are resolved, which is a service
         * fact and not a route one.
         */
        const val SCAN_COLUMN_STATS_MAX_ENTRIES: Long = 1_000_000
    }

    /**
     * Which column statistics a scan plan should carry. [fieldIds] null
     * means every stored row; a set narrows the rows to those field ids
     * (the columns an engine's pushed-down predicate names), so a
     * one-column filter over a wide table fetches files x 1 rows, not
     * files x columns. Field ids that name no visible leaf simply match
     * nothing — the answer is reflective, as on the stats endpoint.
     */
    data class ColumnStatsRequest(val fieldIds: Set<Long>? = null)

    /**
     * THE TWO-STATEMENT ARGUMENT, in both directions.
     *
     * `withHandleUnchecked` runs on autocommit, so the file+DV query and
     * the stats query are separate implicit transactions and see
     * different Postgres snapshots. That is safe, and it is safe because
     * of what can change between them — not because nothing can:
     *
     *  - **A file appearing.** A commit that lands between the two
     *    statements mints a snapshot ABOVE `at`, so its files are
     *    invisible to both under invariant 6's predicate, which the
     *    stats statement re-applies verbatim ([FileRepo.visibleAt], the
     *    same text). Nothing the second statement returns can belong to
     *    a file the first did not see, and anything it did return is
     *    keyed off the plan rather than off the query.
     *
     *  - **A file being retired.** A delete or a compaction
     *    end-snapshots the row at a snapshot above `at`, so the file
     *    stays visible at `at` on both sides. The row is not deleted.
     *
     *  - **A file's stats arriving.** The hydrator only moves
     *    `pending` -> `provided`/`failed`, and the attachment skips any
     *    file the PLAN did not call `provided`, so a flip in the window
     *    leaves the file stat-less in this plan rather than
     *    contradicting its own `stats_state`. `/maintenance/rehydrate`
     *    moves `failed` -> `pending` and touches nothing `provided`.
     *
     *  - **A file's stats vanishing** — the direction the original
     *    comment did not argue. Stats rows are deleted only by the
     *    cascade from `hog_data_file`, and the only statement that
     *    deletes those rows is the expiry sweep's step 2, which takes
     *    files with `end_snapshot <= earliest_snapshot_id`. `at` was
     *    checked at or above that floor, so a file visible at `at` is
     *    not reclaimable — unless the sweep ADVANCES the floor past `at`
     *    inside this window. Then the file the plan already returned can
     *    lose its rows and arrive with an EMPTY `column_stats` array.
     *    That is still a correct answer under the wire contract: an
     *    empty array says the file has no statistics for the requested
     *    columns, and a column absent from the array must not be pruned
     *    on. The plan degrades to "read everything", never to "prune on
     *    bounds that are not there" — and a reader racing expiry that
     *    hard is about to get a 410 on its next explicit read anyway.
     *
     * A single REPEATABLE READ transaction would remove the window. It
     * is not taken because the window has no unsafe direction, and
     * holding a snapshot open across a read that can return a hundred
     * megabytes is a cost paid by every commit vacuuming behind it.
     */
    fun planScan(
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long? = null,
        atTimestamp: Instant? = null,
        columnStats: ColumnStatsRequest? = null,
        splitOffsets: Boolean = false,
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
             WHERE df.catalog_id = :catalogId AND df.table_id = :tableId
               AND ${FileRepo.visibleAt("df")}
             ORDER BY df.row_id_start, df.data_file_id
            """,
            )
                .bind("catalogId", cat.catalogId)
                .bind("tableId", t.tableId)
                .bind("snapshot", at)
                .bind("splitOffsets", splitOffsets)
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
     *
     * BOUNDED BEFORE IT IS BUILT. The entry count is provided files x
     * requested visible LEAF columns, and both factors are known here — the plan is
     * already in hand and the column forest is read for the resolution
     * this function performs anyway — so the refusal happens before the
     * stats statement runs and before a row is materialized. It is an
     * UPPER bound, deliberately: it counts the columns a request could
     * match, not the rows stored for them, because a file that shipped
     * statistics for three of its thirty columns cannot be distinguished
     * from one that shipped all thirty without reading the very rows the
     * cap exists to avoid reading. A request refused at 1.02 million
     * possible entries that would have rendered 400,000 real ones is the
     * price, and it is one `stats_fields` list away from being served.
     *
     * The requested ids are intersected with the visible LEAF columns
     * first, so the bound tracks the answer rather than the question:
     * an engine naming ten thousand field ids of which one exists on
     * this table asks for files x 1 entries and is measured as such,
     * and a container's field id is not counted because a container
     * never has a statistics row to return.
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
        // LEAVES only. list/struct/map nodes carry no values and never
        // have a hog_file_column_stats row (iceberg-federation.md; a
        // commit shipping one for a container's field id is refused by
        // name), so counting them would refuse a request on columns
        // that cannot contribute an entry. A deeply nested table is
        // mostly containers, which is where the difference is large.
        val leafFieldIds = byFieldId.filterValues { (_, column) -> column.children.isEmpty() }.keys
        enforceEntryCap(files, leafFieldIds, request)
        val rowsByFile =
            FileRepo.providedColumnStatsAt(h, catalogId, tableId, at, request.fieldIds)
        return files.map { file ->
            if (file.dataFile.statsState != StatsState.PROVIDED) return@map file
            val rows = rowsByFile[file.dataFile.dataFileId].orEmpty()
            file.copy(dataFile = file.dataFile.copy(columnStats = resolveColumnStats(rows, byFieldId)))
        }
    }

    /**
     * Refuse a plan whose `column_stats` array would exceed
     * [SCAN_COLUMN_STATS_MAX_ENTRIES] entries, stating the arithmetic
     * and the one parameter that changes it.
     *
     * THE ARITHMETIC IS `provided files x requested visible leaf
     * columns`, and the message says so, because only one of the two
     * factors is under the caller's control. `stats_fields` divides the
     * second. Nothing divides the first: `/scan` plans a table at one
     * snapshot and has no range, page or limit parameter, so a table
     * whose FILE count alone exceeds the cap cannot reach these
     * statistics at any narrowing — one column still costs one entry
     * per file. That is a real, reachable state (see the spec's 422
     * text and server/README.md), and the honest answer for such a
     * table is that the engine plans without bounds; suggesting a
     * range this endpoint does not have would send the caller looking
     * for a parameter that is not there.
     *
     * 422 and not 400: the request is well-formed and would have been
     * legal against a smaller table, which is the same line
     * `snapshot`-out-of-range and `stats_fields`-without-`include` sit
     * on. Multiplied in Long, because the product overflows Int well
     * inside what this catalog can hold — roughly 215,000 files at the
     * 10,000-column maximum — and an overflowed product is a cap that
     * passes the requests it exists to refuse.
     */
    private fun enforceEntryCap(
        files: List<ScanFile>,
        visibleLeafFieldIds: Set<Long>,
        request: ColumnStatsRequest,
    ) {
        val providedFiles = files.count { it.dataFile.statsState == StatsState.PROVIDED }.toLong()
        val requested = request.fieldIds
        val columns =
            if (requested == null) {
                visibleLeafFieldIds.size.toLong()
            } else {
                requested.count { it in visibleLeafFieldIds }.toLong()
            }
        val entries = providedFiles * columns
        if (entries > SCAN_COLUMN_STATS_MAX_ENTRIES) {
            val narrowed = if (requested == null) "" else " already narrowed by stats_fields;"
            throw HoglakeException.Validation(
                "column_stats would carry up to $entries entries ($providedFiles files with statistics " +
                    "x $columns requested column(s)), over the maximum $SCAN_COLUMN_STATS_MAX_ENTRIES " +
                    "per scan;$narrowed narrow it with stats_fields, naming only the field ids a " +
                    "predicate prunes on — one column costs one entry per file, so a table with more " +
                    "than $SCAN_COLUMN_STATS_MAX_ENTRIES files cannot carry column_stats at all and " +
                    "must be planned without bounds",
            )
        }
    }
}
