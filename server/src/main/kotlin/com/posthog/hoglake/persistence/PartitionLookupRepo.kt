package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.DataFile
import com.posthog.hoglake.model.PartitionLookupAggregates
import com.posthog.hoglake.model.PartitionLookupStatsState
import com.posthog.hoglake.model.PartitionLookupTotals
import com.posthog.hoglake.model.StatsState
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.statement.Query
import java.sql.ResultSet

/**
 * Fixed windows of the existing inverse index; unrelated tables and ended files consume the budget too.
 * The 120k-file plan fixture examines at most cap + 1 candidates: at cap 1,000, the worst
 * custom/generic plan used 6,832 buffers in 3.469 ms (about 3.5 microseconds per candidate).
 * Production caps at 10,000 candidates per statement, with a separate three-second timeout.
 */
internal object PartitionLookupRepo {
    private const val FILE_COLUMNS =
        "data_file_id, table_id, path, file_format, record_count, file_size_bytes, " +
            "footer_size, row_id_start, stats_state, begin_snapshot, explicit_row_ids"

    data class Page(val files: List<DataFile>, val candidateCount: Int, val lastExamined: Long?)

    fun specArity(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        specId: Long,
        snapshot: Long,
    ): Int? =
        h.createQuery(
            """
            SELECT (SELECT count(*) FROM (
                        SELECT 1 FROM hog_partition_field pf
                        WHERE pf.catalog_id = s.catalog_id AND pf.table_id = s.table_id AND pf.spec_id = s.spec_id
                        LIMIT 65
                    ) fields)
            FROM hog_partition_spec s
            WHERE s.catalog_id = :catalogId AND s.table_id = :tableId
              AND s.spec_id = :specId AND s.begin_snapshot <= :snapshot
            """,
        ).bind("catalogId", catalogId).bind("tableId", tableId).bind("specId", specId).bind("snapshot", snapshot)
            .mapTo(Int::class.javaObjectType).findOne().orElse(null)

    fun aggregates(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        specId: Long,
        values: List<String?>,
        snapshot: Long,
        upper: Long,
        candidateLimit: Int,
    ): PartitionLookupAggregates =
        bind(
            h.createQuery(
                aggregateSql(values),
            ),
            catalogId, tableId, specId, values, snapshot, upper, -1, candidateLimit,
        )
            .map { rs, _ ->
                if (rs.getInt("candidate_count") > candidateLimit) {
                    PartitionLookupAggregates("unavailable", "candidate_scan_limit", null)
                } else {
                    try {
                        PartitionLookupAggregates(
                            "exact",
                            null,
                            PartitionLookupTotals(
                                exactLong(rs, "file_count"),
                                exactLong(rs, "physical_rows"),
                                exactLong(rs, "total_bytes"),
                                exactLong(rs, "dv_count"),
                                PartitionLookupStatsState(
                                    exactLong(rs, "provided"),
                                    exactLong(rs, "pending"),
                                    exactLong(rs, "failed"),
                                ),
                                rs.getObject("last_write")?.let { (it as Number).toLong() },
                            ),
                        )
                    } catch (_: ArithmeticException) {
                        PartitionLookupAggregates("unavailable", "overflow", null)
                    }
                }
            }.one()

    private fun exactLong(
        rs: ResultSet,
        column: String,
    ): Long =
        rs.getBigDecimal(
            column,
        ).toBigIntegerExact().longValueExact()

    fun page(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        specId: Long,
        values: List<String?>,
        snapshot: Long,
        upper: Long,
        after: Long,
        candidateLimit: Int,
        limit: Int,
    ): Page {
        var count = 0
        var last: Long? = null
        val files =
            bind(
                h.createQuery(
                    pageSql(values),
                ),
                catalogId, tableId, specId, values, snapshot, upper, after, candidateLimit,
            )
                .bind("pageLimit", limit + 1)
                .map { rs, _ ->
                    count = rs.getInt("candidate_count")
                    last = rs.getObject("last_examined")?.let { (it as Number).toLong() }
                    if (rs.getObject("data_file_id") == null) {
                        null
                    } else {
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
                            specId = specId,
                            partitionValues = values,
                            explicitRowIds = rs.getBoolean("explicit_row_ids"),
                        )
                    }
                }.list().filterNotNull()
        return Page(files, count, last)
    }

    internal fun bind(
        query: Query,
        catalogId: Long,
        tableId: Long,
        specId: Long,
        values: List<String?>,
        snapshot: Long,
        upper: Long,
        after: Long,
        candidateLimit: Int,
    ): Query {
        query.bind("catalogId", catalogId).bind("tableId", tableId).bind("specId", specId)
            .bind("snapshot", snapshot).bind("upper", upper).bind("after", after).bind("candidateLimit", candidateLimit)
            .bind("candidateProbeLimit", candidateLimit + 1).bind("arity", values.size)
            .addCustomizer(Pg.CUSTOM_PLANS_ONLY)
        values.forEachIndexed { i, value -> if (value != null) query.bind("value$i", value) }
        return query
    }

    // IS NULL does not fix an equality pathkey; retaining value keeps the inverse index's ordering.
    private fun windows(values: List<String?>): String =
        """
        WITH candidates AS MATERIALIZED (
            SELECT data_file_id FROM hog_file_partition_value
            WHERE catalog_id = :catalogId AND key_index = 0
              AND ${if (values[0] == null) "value IS NULL" else "value = :value0"}
              AND data_file_id > :after
              AND data_file_id < :upper
            ORDER BY value, data_file_id LIMIT :candidateProbeLimit
        ), scan_window AS MATERIALIZED (
            SELECT data_file_id FROM candidates ORDER BY data_file_id LIMIT :candidateLimit
        )
        """

    private fun matched(
        values: List<String?>,
        aggregate: Boolean,
    ): String {
        val predicates =
            values.mapIndexed { i, value ->
                """EXISTS (SELECT 1 FROM hog_file_partition_value pv
                    WHERE pv.catalog_id = f.catalog_id AND pv.data_file_id = f.data_file_id
                      AND pv.key_index = $i AND ${if (value == null) "pv.value IS NULL" else "pv.value = :value$i"})"""
            }.joinToString(" AND ")
        val gate = if (aggregate) "WHERE (SELECT count(*) FROM candidates) <= :candidateLimit" else ""
        // OFFSET 0 keeps the manifest access a bounded primary-key probe per candidate.
        val columns =
            if (aggregate) "data_file_id, record_count, file_size_bytes, stats_state, begin_snapshot" else FILE_COLUMNS
        val projected = columns.split(", ").joinToString(", ") { "f.$it" }
        return """
        , matched AS MATERIALIZED (
            SELECT $projected FROM (SELECT data_file_id FROM scan_window $gate) w
            JOIN LATERAL (
                SELECT $projected FROM hog_data_file f
                WHERE f.catalog_id = :catalogId AND f.data_file_id = w.data_file_id
                  AND f.table_id = :tableId AND f.spec_id = :specId
                  AND f.begin_snapshot <= :snapshot AND (f.end_snapshot IS NULL OR :snapshot < f.end_snapshot)
                  AND $predicates
                  AND NOT EXISTS (SELECT 1 FROM hog_file_partition_value extra
                      WHERE extra.catalog_id = f.catalog_id AND extra.data_file_id = f.data_file_id
                        AND (extra.key_index < 0 OR extra.key_index >= :arity))
                OFFSET 0
            ) f ON true
        )
        """
    }

    internal fun pageSql(values: List<String?>): String =
        windows(values) + matched(values, aggregate = false) +
            """
            SELECT ${FILE_COLUMNS.split(", ").joinToString(", ") { "p.$it" }}, meta.candidate_count, meta.last_examined
            FROM (SELECT (SELECT count(*) FROM candidates) AS candidate_count,
                         (SELECT max(data_file_id) FROM scan_window) AS last_examined) meta
            LEFT JOIN LATERAL (
                SELECT $FILE_COLUMNS FROM matched ORDER BY data_file_id LIMIT :pageLimit
            ) p ON true
            ORDER BY p.data_file_id
            """

    internal fun aggregateSql(values: List<String?>): String =
        windows(values) + matched(values, aggregate = true) +
            """
            SELECT (SELECT count(*) FROM candidates) AS candidate_count,
                   count(*) AS file_count, coalesce(sum(f.record_count), 0) AS physical_rows,
                   coalesce(sum(f.file_size_bytes), 0) AS total_bytes,
                   count(*) FILTER (WHERE dv.delete_file_id IS NOT NULL
                       AND (dv.end_snapshot IS NULL OR :snapshot < dv.end_snapshot)) AS dv_count,
                   count(*) FILTER (WHERE f.stats_state = 'provided') AS provided,
                   count(*) FILTER (WHERE f.stats_state = 'pending') AS pending,
                   count(*) FILTER (WHERE f.stats_state = 'failed') AS failed,
                   max(f.begin_snapshot) AS last_write
            FROM matched f
            LEFT JOIN LATERAL (
                SELECT d.delete_file_id, d.end_snapshot FROM hog_delete_file d
                WHERE d.catalog_id = :catalogId AND d.data_file_id = f.data_file_id
                  AND d.begin_snapshot <= :snapshot
                ORDER BY d.begin_snapshot DESC, d.delete_file_id DESC LIMIT 1
            ) dv ON true
            """
}
