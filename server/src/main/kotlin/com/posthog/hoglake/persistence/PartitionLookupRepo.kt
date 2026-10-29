package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.DataFile
import com.posthog.hoglake.model.PartitionLookupAggregates
import com.posthog.hoglake.model.PartitionLookupStatsState
import com.posthog.hoglake.model.PartitionLookupTotals
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.statement.Query
import java.sql.ResultSet

/**
 * Probe each tuple key through the inverse index, then examine the smallest bounded candidate set.
 * Each key reads at most cap + 1 entries; unrelated tables and retained ended files still count.
 * Equal counts choose the probe covering the widest id range, then the lowest key index.
 * Aggregates and the page share one materialized match set; continuations skip aggregate DV seeks.
 * PG18 warm serial fixture: 120k files / 426k partition values; the two-key first page examines
 * 4,706 candidates in 47–50 ms (10–11 us/candidate), with 40,011 shared buffers. The plan fixture
 * checks custom and generic plans at the production 10k cap, including 64-key tuples.
 */
internal object PartitionLookupRepo {
    private const val FILE_COLUMNS =
        "data_file_id, table_id, path, file_format, record_count, file_size_bytes, " +
            "footer_size, row_id_start, stats_state, begin_snapshot, explicit_row_ids"

    data class Page(val files: List<DataFile>, val candidateCount: Int, val lastExamined: Long?)

    data class Result(val aggregates: PartitionLookupAggregates, val page: Page)

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
            .map { rs, _ -> aggregate(rs, candidateLimit) }.one()

    private fun aggregate(
        rs: ResultSet,
        candidateLimit: Int,
    ): PartitionLookupAggregates =
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

    private fun exactLong(
        rs: ResultSet,
        column: String,
    ): Long =
        rs.getBigDecimal(
            column,
        ).toBigIntegerExact().longValueExact()

    fun lookup(
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
        includeAggregates: Boolean,
    ): Result {
        var aggregates = PartitionLookupAggregates("unavailable", "continuation", null)
        var count = 0
        var last: Long? = null
        val files =
            bind(
                h.createQuery(
                    lookupSql(values),
                ),
                catalogId, tableId, specId, values, snapshot, upper, after, candidateLimit,
            )
                .bind("pageLimit", limit + 1)
                .bind("includeAggregates", includeAggregates)
                .map { rs, _ ->
                    if (includeAggregates) aggregates = aggregate(rs, candidateLimit)
                    count = rs.getInt("candidate_count")
                    last = rs.getObject("last_examined")?.let { (it as Number).toLong() }
                    if (rs.getObject("data_file_id") == null) {
                        null
                    } else {
                        FileRepo.mapFile(rs, specId, values)
                    }
                }.list().filterNotNull()
        return Result(aggregates, Page(files, count, last))
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
    private fun windows(values: List<String?>): String {
        val probes =
            values.mapIndexed { i, value ->
                """SELECT $i AS key_index, data_file_id FROM (
                SELECT data_file_id FROM hog_file_partition_value
                WHERE catalog_id = :catalogId AND key_index = $i
                  AND ${if (value == null) "value IS NULL" else "value = :value$i"}
                  AND data_file_id > :after AND data_file_id < :upper
                ORDER BY value, data_file_id LIMIT :candidateProbeLimit
            ) key_$i"""
            }.joinToString(" UNION ALL ")
        val keys = values.indices.joinToString(", ") { "($it)" }
        return """
        WITH probes AS MATERIALIZED ($probes),
        pivot AS MATERIALIZED (
            SELECT keys.key_index FROM (VALUES $keys) keys(key_index)
            LEFT JOIN (
                SELECT key_index, count(*) AS candidate_count, max(data_file_id) AS covered_through
                FROM probes GROUP BY key_index
            ) counts USING (key_index)
            ORDER BY coalesce(candidate_count, 0), covered_through DESC, keys.key_index LIMIT 1
        ),
        candidates AS MATERIALIZED (
            SELECT data_file_id FROM probes WHERE key_index = (SELECT key_index FROM pivot)
        ), scan_window AS MATERIALIZED (
            SELECT data_file_id FROM candidates ORDER BY data_file_id LIMIT :candidateLimit
        )
        """
    }

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

    internal fun lookupSql(values: List<String?>): String =
        windows(values) + matched(values, aggregate = false) +
            """
            , totals AS MATERIALIZED (
                ${totalsSql("WHERE :includeAggregates AND (SELECT count(*) FROM candidates) <= :candidateLimit")}
            )
            SELECT ${FILE_COLUMNS.split(", ").joinToString(", ") { "p.$it" }}, totals.*,
                   meta.candidate_count, meta.last_examined
            FROM (SELECT (SELECT count(*) FROM candidates) AS candidate_count,
                         (SELECT max(data_file_id) FROM scan_window) AS last_examined) meta
            CROSS JOIN totals
            LEFT JOIN LATERAL (
                SELECT $FILE_COLUMNS FROM matched ORDER BY data_file_id LIMIT :pageLimit
            ) p ON true
            ORDER BY p.data_file_id
            """

    internal fun aggregateSql(values: List<String?>): String =
        windows(values) + matched(values, aggregate = true) +
            "SELECT (SELECT count(*) FROM candidates) AS candidate_count, totals.* FROM (${totalsSql("")}) totals"

    private fun totalsSql(gate: String): String =
        """
            SELECT count(*) AS file_count, coalesce(sum(f.record_count), 0) AS physical_rows,
                   coalesce(sum(f.file_size_bytes), 0) AS total_bytes,
                   count(*) FILTER (WHERE dv.delete_file_id IS NOT NULL
                       AND (dv.end_snapshot IS NULL OR :snapshot < dv.end_snapshot)) AS dv_count,
                   count(*) FILTER (WHERE f.stats_state = 'provided') AS provided,
                   count(*) FILTER (WHERE f.stats_state = 'pending') AS pending,
                   count(*) FILTER (WHERE f.stats_state = 'failed') AS failed,
                   max(f.begin_snapshot) AS last_write
            FROM (SELECT * FROM matched $gate) f
            LEFT JOIN LATERAL (
                SELECT d.delete_file_id, d.end_snapshot FROM hog_delete_file d
                WHERE d.catalog_id = :catalogId AND d.data_file_id = f.data_file_id
                  AND d.begin_snapshot <= :snapshot
                ORDER BY d.begin_snapshot DESC, d.delete_file_id DESC LIMIT 1
            ) dv ON true
        """
}
