package com.posthog.hoglake.observability

import com.posthog.hoglake.model.DatabaseTable
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.persistence.DatabaseHealthRepo
import io.micrometer.core.instrument.Tags
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.jdbi.v3.core.statement.Query
import java.time.Instant

internal data class CatalogRow(
    val name: String,
    val head: Long,
    val earliest: Long,
    val headAgeSeconds: Double?,
    val removalDepth: Long,
    val statsPending: Long,
    val statsPendingHeadFileId: Long,
    val statsFailed: Long,
    val missingFieldIds: Long,
    val prefixQuarantined: Long,
    val tables: Long,
    /** Whether the maintenance summary has stamped this catalog's live totals (V27). */
    val liveSampled: Boolean,
    /** Seconds since the generation the totals are as of was published; null when unsampled. */
    val liveTotalsAgeSeconds: Number?,
    /** Null when unsampled, or when the published generation did not measure rows. */
    val liveRows: Long?,
    val liveBytes: Long?,
    val liveFiles: Long?,
    val oldestSnapshotTime: Instant?,
)

internal data class TableRow(
    val catalog: String,
    val namespace: String,
    val table: String,
    val measured: Boolean,
    val files: Long,
    val smallFiles: Long,
    val bytes: Long,
    val smallBytes: Long,
    val rows: Long,
    val deleteFiles: Long,
    val partitions: Long,
    val largestPartitionFiles: Long,
    /** Live tables the published generation covers in this catalog, before the cap. */
    val covered: Long,
) {
    val tags: Tags get() = Tags.of("catalog", catalog, "namespace", namespace, "table", table)
}

internal data class LastRuns(
    val catalog: String,
    val task: String,
    val okAt: Double?,
    val failedAt: Double?,
    val duration: Double,
)

internal interface CatalogMetricSource {
    val identities: Map<String, String>? get() = null

    fun startSample() {}

    fun failure(group: String): Throwable? = null

    fun catalogs(): Pair<List<CatalogRow>, List<Triple<String, String, Long>>>

    fun tables(cap: Int): Pair<List<String>, List<TableRow>>

    fun deleteFiles(): List<Triple<String, Long, Long>>

    fun lifecycle(probeCap: Int): List<Lifecycle>

    fun maintenanceRuns(lookback: Int): Pair<Set<String>, List<LastRuns>>

    fun relations(): List<DatabaseTable>
}

internal data class Lifecycle(val name: String, val earliestAge: Double?, val droppedPending: Long)

/** Legacy reads retain their SQL. Mixed reads restrict every catalog join to its pinned identity. */
internal class PostgresCatalogMetricSource(
    private val jdbi: Jdbi,
    private val catalogId: Long? = null,
) : CatalogMetricSource {
    private fun Handle.metricQuery(sql: String): Query {
        val query =
            createQuery(
                if (catalogId == null) {
                    sql
                } else {
                    sql.replace(
                        "hog_catalog c",
                        "(SELECT * FROM hog_catalog WHERE catalog_id = :metricCatalogId) c",
                    )
                },
            ).setQueryTimeout(60)
        if (catalogId != null && sql.contains("hog_catalog c")) query.bind("metricCatalogId", catalogId)
        return query
    }

    override fun catalogs(): Pair<List<CatalogRow>, List<Triple<String, String, Long>>> =
        jdbi.inTransactionUnchecked { h ->
            if (catalogId != null) h.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            val rows =
                h.metricQuery(CatalogMetrics.SAMPLE_SQL)
                    .map { rs, _ ->
                        CatalogRow(
                            name = rs.getString("name"),
                            head = rs.getLong("last_snapshot_id"),
                            earliest = rs.getLong("earliest_snapshot_id"),
                            headAgeSeconds =
                                rs.getObject("head_age_seconds")?.let {
                                    rs.getDouble("head_age_seconds")
                                },
                            removalDepth = rs.getLong("removal_depth"),
                            statsPending = rs.getLong("stats_pending"),
                            statsPendingHeadFileId = rs.getLong("stats_pending_head_file_id"),
                            statsFailed = rs.getLong("stats_failed"),
                            missingFieldIds = rs.getLong("missing_field_ids"),
                            prefixQuarantined = rs.getLong("prefix_quarantined"),
                            tables = rs.getLong("table_count"),
                            liveSampled = rs.getBoolean("live_sampled"),
                            liveTotalsAgeSeconds =
                                rs.getObject("live_totals_age_seconds")?.let {
                                    rs.getDouble("live_totals_age_seconds")
                                },
                            liveRows = rs.getObject("live_rows", java.lang.Long::class.java)?.toLong(),
                            liveBytes = rs.getObject("live_bytes", java.lang.Long::class.java)?.toLong(),
                            liveFiles = rs.getObject("live_files", java.lang.Long::class.java)?.toLong(),
                            oldestSnapshotTime =
                                rs.getObject(
                                    "oldest_snapshot_time",
                                    java.time.OffsetDateTime::class.java,
                                )?.toInstant(),
                        )
                    }
                    .list()
            // consumer -> worst (lowest) committed snapshot across its
            // tables. The hog_table join mirrors ExpiryService's floor
            // query: only offsets whose table identity exists (any
            // incarnation) count — a garbage/expired uuid cannot distort
            // the lag series any more than it can pin retention.
            val offsets =
                h.metricQuery(
                    """
                SELECT c.name AS catalog, o.consumer_id,
                       min(o.committed_snapshot) AS committed
                  FROM hog_consumer_offset o
                  JOIN hog_catalog c ON c.catalog_id = o.catalog_id
                  JOIN hog_table t
                    ON t.catalog_id = o.catalog_id AND t.table_uuid = o.table_uuid
                 GROUP BY c.name, o.consumer_id
                """ +
                        if (catalogId == null) {
                            ""
                        } else {
                            " ORDER BY committed, o.consumer_id LIMIT ${CatalogMetrics.MAX_CONSUMER_SERIES + 1}"
                        },
                )
                    .map { rs, _ ->
                        Triple(rs.getString("catalog"), rs.getString("consumer_id"), rs.getLong("committed"))
                    }
                    .list()
            if (catalogId != null) check(rows.size == 1) { "selected catalog incarnation is absent" }
            rows to offsets
        }

    override fun tables(cap: Int): Pair<List<String>, List<TableRow>> =
        jdbi.inTransactionUnchecked { h ->
            // First statement of the transaction, or it does not apply.
            h.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            val names = h.metricQuery(CatalogMetrics.PUBLISHED_CATALOGS_SQL).mapTo(String::class.java).list()
            val tables =
                h.metricQuery(CatalogMetrics.TABLE_SQL)
                    .bind("cap", cap)
                    .map { rs, _ ->
                        TableRow(
                            catalog = rs.getString("catalog"),
                            namespace = rs.getString("namespace_name"),
                            table = rs.getString("table_name"),
                            measured = rs.getBoolean("measured"),
                            files = rs.getLong("files"),
                            smallFiles = rs.getLong("small_files"),
                            bytes = rs.getLong("bytes"),
                            smallBytes = rs.getLong("small_bytes"),
                            rows = rs.getLong("record_count"),
                            deleteFiles = rs.getLong("delete_files"),
                            partitions = rs.getLong("partitions"),
                            largestPartitionFiles = rs.getLong("largest_partition_files"),
                            covered = rs.getLong("covered"),
                        )
                    }
                    .list()
            names to tables
        }

    override fun deleteFiles(): List<Triple<String, Long, Long>> =
        jdbi.inTransactionUnchecked { h ->
            h.metricQuery(CatalogMetrics.DELETE_FILES_SQL)
                .map { rs, _ -> Triple(rs.getString("name"), rs.getLong("files"), rs.getLong("bytes")) }
                .list()
        }

    override fun lifecycle(probeCap: Int): List<Lifecycle> =
        jdbi.inTransactionUnchecked { h ->
            h.metricQuery(CatalogMetrics.LIFECYCLE_SQL)
                .bind("probeCap", probeCap)
                .map { rs, _ ->
                    Lifecycle(
                        rs.getString("name"),
                        rs.getObject("earliest_age_seconds")?.let { rs.getDouble("earliest_age_seconds") },
                        rs.getLong("dropped_pending"),
                    )
                }
                .list()
        }

    override fun maintenanceRuns(lookback: Int): Pair<Set<String>, List<LastRuns>> =
        jdbi.inTransactionUnchecked { h ->
            if (catalogId != null) {
                h.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                // A generic task parameter can misestimate an absent task during a long failure streak.
                h.execute("SET LOCAL plan_cache_mode = force_custom_plan")
            }
            val names = h.metricQuery("SELECT c.name FROM hog_catalog c").mapTo(String::class.java).set()

            fun Query.runRows(): List<LastRuns> =
                map { rs, _ ->
                    LastRuns(
                        if (catalogId == null) rs.getString("catalog") else names.single(),
                        rs.getString("task"),
                        rs.getObject("ok_at")?.let { rs.getDouble("ok_at") },
                        rs.getObject("failed_at")?.let { rs.getDouble("failed_at") },
                        rs.getDouble("last_duration"),
                    )
                }.list()
            val runs =
                if (catalogId == null) {
                    h.metricQuery(CatalogMetrics.MAINTENANCE_RUNS_SQL)
                        .bindArray("tasks", String::class.java, MaintenanceTask.entries.map { it.wire })
                        .bind("lookback", lookback).runRows()
                } else if (names.isEmpty()) {
                    emptyList()
                } else {
                    // One bounded statement per task. No task roster joins can scan the entire ledger.
                    MaintenanceTask.entries.flatMap { task ->
                        h.createQuery(MIXED_MAINTENANCE_RUN_SQL)
                            .bind("catalogId", catalogId).bind("task", task.wire)
                            .setQueryTimeout(60).runRows()
                    }
                }
            names to runs
        }

    companion object {
        internal const val MIXED_MAINTENANCE_RUN_SQL = """
            SELECT :task AS task,
                   (SELECT extract(epoch FROM r.finished_at) FROM hog_maintenance_run r
                     WHERE r.catalog_id = :catalogId AND r.task = :task AND r.status = 'ok'
                     ORDER BY r.run_id DESC LIMIT 1) AS ok_at,
                   (SELECT extract(epoch FROM r.finished_at) FROM hog_maintenance_run r
                     WHERE r.catalog_id = :catalogId AND r.task = :task AND r.status = 'failed'
                     ORDER BY r.run_id DESC LIMIT 1) AS failed_at,
                   extract(epoch FROM (latest.finished_at - latest.started_at)) AS last_duration
              FROM (SELECT started_at, finished_at FROM hog_maintenance_run r
                     WHERE r.catalog_id = :catalogId AND r.task = :task ORDER BY r.run_id DESC LIMIT 1) latest
        """
    }

    override fun relations(): List<DatabaseTable> = jdbi.withHandleUnchecked { DatabaseHealthRepo.tables(it) }
}
