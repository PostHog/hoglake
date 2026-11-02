package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.DatabaseTable
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.observability.CatalogMetricSource
import com.posthog.hoglake.observability.CatalogMetrics
import com.posthog.hoglake.observability.CatalogRow
import com.posthog.hoglake.observability.LastRuns
import com.posthog.hoglake.observability.Lifecycle
import com.posthog.hoglake.observability.TableRow
import java.time.Duration
import java.time.Instant

/** Exact tenant aggregates. Queries return scalars or capped groups, never individual manifest rows. */
internal class HogliteCatalogMetrics(
    private val store: HogliteCatalogStore,
    private val catalog: String,
    private val smallFileThresholdBytes: Long,
) : CatalogMetricSource {
    override fun catalogs(): Pair<List<CatalogRow>, List<Triple<String, String, Long>>> {
        val began = Instant.now()
        // Both reads use one durable snapshot on the pinned tenant.
        val result =
            store.readCatalog(
                catalog,
                listOf(
                    HogliteStatement(CORE_SQL),
                    HogliteStatement(OFFSETS_SQL, listOf(CatalogMetrics.MAX_CONSUMER_SERIES + 1)),
                ),
            )
        val r = result[0].rows.single()
        check(r[0].asString() == catalog) { "catalog identity mismatch" }
        val oldest = (r[14] as? String)?.let(HogliteSchema::parse)
        val headTime = (r[3] as? String)?.let(HogliteSchema::parse)
        val row =
            CatalogRow(
                name = catalog,
                head = r[1].asLong(),
                earliest = r[2].asLong(),
                headAgeSeconds = headTime?.let { secondsBetween(it, began) },
                removalDepth = r[4].asLong(),
                statsPending = r[5].asLong(),
                statsPendingHeadFileId = r[6].asLong(),
                statsFailed = r[7].asLong(),
                missingFieldIds = r[8].asLong(),
                prefixQuarantined = r[9].asLong(),
                tables = r[10].asLong(),
                liveSampled = true,
                liveTotalsAgeSeconds = AggregateAge(began),
                liveRows = r[11].asLong(),
                liveBytes = r[12].asLong(),
                liveFiles = r[13].asLong(),
                oldestSnapshotTime = oldest,
            )
        return listOf(row) to result[1].rows.map { Triple(catalog, it[0].asString(), it[1].asLong()) }
    }

    override fun tables(cap: Int): Pair<List<String>, List<TableRow>> {
        val rows = store.queryCatalog(catalog, TABLES_SQL, smallFileThresholdBytes, smallFileThresholdBytes, cap).rows
        return listOf(catalog) to
            rows.map { r ->
                TableRow(
                    catalog, r[0].asString(), r[1].asString(), true,
                    r[2].asLong(), r[3].asLong(), r[4].asLong(), r[5].asLong(), r[6].asLong(),
                    r[7].asLong(), r[8].asLong(), r[9].asLong(), r[10].asLong(),
                )
            }
    }

    override fun deleteFiles(): List<Triple<String, Long, Long>> {
        val r =
            store.queryCatalog(
                catalog,
                "SELECT COUNT(*), COALESCE(SUM(d.file_size_bytes),0) FROM dvs d " +
                    "JOIN tables t ON t.id = d.table_id AND t.dropped_snapshot IS NULL WHERE d.end_snapshot IS NULL",
            ).rows.single()
        return listOf(Triple(catalog, r[0].asLong(), r[1].asLong()))
    }

    override fun lifecycle(probeCap: Int): List<Lifecycle> {
        val r =
            store.queryCatalog(
                catalog,
                "SELECT (SELECT ts FROM snapshots WHERE id >= m.earliest_snapshot_id ORDER BY id LIMIT 1), " +
                    "(SELECT COUNT(*) FROM (SELECT id FROM tables WHERE dropped_snapshot IS NOT NULL " +
                    "ORDER BY id DESC LIMIT ?) t WHERE EXISTS (SELECT 1 FROM files f " +
                    "WHERE f.table_id = t.id AND f.end_snapshot IS NULL)) FROM meta m WHERE id = 1",
                probeCap,
            ).rows.single()
        return listOf(
            Lifecycle(
                catalog,
                (r[0] as? String)?.let {
                    secondsBetween(HogliteSchema.parse(it), Instant.now())
                },
                r[1].asLong(),
            ),
        )
    }

    override fun maintenanceRuns(lookback: Int): Pair<Set<String>, List<LastRuns>> {
        // Three indexed LIMIT 1 probes per task recover history beyond any failure streak.
        val rows =
            store.queryCatalog(
                catalog,
                RUNS_SQL,
                store.json.writeValueAsString(MaintenanceTask.entries.map { it.wire }),
            ).rows.map {
                    r ->
                LastRuns(
                    catalog,
                    r[0].asString(),
                    (r[1] as? String)?.let { HogliteSchema.parse(it).toEpochMilli() / 1000.0 },
                    (r[2] as? String)?.let { HogliteSchema.parse(it).toEpochMilli() / 1000.0 },
                    secondsBetween(HogliteSchema.parse(r[3].asString()), HogliteSchema.parse(r[4].asString())),
                )
            }
        return setOf(catalog) to rows
    }

    override fun relations(): List<DatabaseTable> = emptyList()

    companion object {
        private fun secondsBetween(
            start: Instant,
            end: Instant,
        ): Double = Duration.between(start, end).toMillis() / 1000.0

        internal const val CORE_SQL = """
            SELECT m.name, m.next_snapshot - 1, m.earliest_snapshot_id,
                   (SELECT ts FROM snapshots ORDER BY id DESC LIMIT 1),
                   (SELECT COUNT(*) FROM removals WHERE drained_at IS NULL),
                   COALESCE(f.pending,0), COALESCE(f.pending_head,0), COALESCE(f.failed,0),
                   COALESCE(f.missing,0), COALESCE(f.quarantined,0),
                   (SELECT COUNT(*) FROM tables WHERE dropped_snapshot IS NULL),
                   COALESCE(f.records,0), COALESCE(f.bytes,0), COALESCE(f.files,0),
                   (SELECT ts FROM snapshots WHERE id >= m.earliest_snapshot_id ORDER BY id LIMIT 1)
              FROM meta m LEFT JOIN (
                SELECT SUM(CASE WHEN f.stats_state = 'pending' THEN 1 ELSE 0 END) pending,
                       MIN(CASE WHEN f.stats_state = 'pending' THEN f.id END) pending_head,
                       SUM(CASE WHEN f.stats_state = 'failed' THEN 1 ELSE 0 END) failed,
                       SUM(f.missing_field_ids) missing, SUM(f.prefix_quarantined) quarantined,
                       SUM(f.record_count) records, SUM(f.file_size_bytes) bytes, COUNT(*) files
                  FROM files f JOIN tables t ON t.id = f.table_id AND t.dropped_snapshot IS NULL
                 WHERE f.end_snapshot IS NULL
              ) f ON 1 = 1 WHERE m.id = 1
        """

        internal const val OFFSETS_SQL = """
            SELECT o.consumer_id, MIN(o.committed_snapshot) committed
              FROM offsets o JOIN tables t ON t.table_uuid = o.table_uuid
             GROUP BY o.consumer_id ORDER BY committed, o.consumer_id LIMIT ?
        """

        internal const val TABLES_SQL = """
            WITH partitions AS (
                SELECT f.table_id, f.spec_id, f.partition_values_json,
                       COUNT(*) files, SUM(CASE WHEN f.file_size_bytes < ? THEN 1 ELSE 0 END) small_files,
                       SUM(f.file_size_bytes) bytes,
                       SUM(CASE WHEN f.file_size_bytes < ? THEN f.file_size_bytes ELSE 0 END) small_bytes,
                       SUM(f.record_count) records,
                       SUM(CASE WHEN EXISTS (SELECT 1 FROM dvs d WHERE d.data_file_id = f.id AND d.end_snapshot IS NULL)
                                THEN 1 ELSE 0 END) delete_files
                  FROM files f JOIN tables t ON t.id = f.table_id AND t.dropped_snapshot IS NULL
                 WHERE f.end_snapshot IS NULL GROUP BY f.table_id, f.spec_id, f.partition_values_json
            ), totals AS (
                SELECT table_id, SUM(files) files, SUM(small_files) small_files, SUM(bytes) bytes,
                       SUM(small_bytes) small_bytes, SUM(records) records, SUM(delete_files) delete_files,
                       COUNT(*) partitions, MAX(files) largest FROM partitions GROUP BY table_id
            )
            SELECT v.namespace, v.name, COALESCE(p.files,0), COALESCE(p.small_files,0), COALESCE(p.bytes,0),
                   COALESCE(p.small_bytes,0), COALESCE(p.records,0), COALESCE(p.delete_files,0),
                   COALESCE(p.partitions,0), COALESCE(p.largest,0), COUNT(*) OVER ()
              FROM tables t JOIN table_versions v ON v.table_id = t.id AND v.end_snapshot IS NULL
              LEFT JOIN totals p ON p.table_id = t.id WHERE t.dropped_snapshot IS NULL
             ORDER BY COALESCE(p.files,0) DESC, v.namespace, v.name LIMIT ?
        """

        internal const val LAST_STATUS_SQL =
            "SELECT finished_at FROM maintenance_runs WHERE task = ? AND status = ? ORDER BY id DESC LIMIT 1"

        private val RUNS_SQL = """
            SELECT tasks.task,
                   (SELECT finished_at FROM maintenance_runs WHERE task = tasks.task AND status = 'ok' ORDER BY id DESC LIMIT 1),
                   (SELECT finished_at FROM maintenance_runs WHERE task = tasks.task AND status = 'failed' ORDER BY id DESC LIMIT 1),
                   r.started_at, r.finished_at
              FROM (SELECT value AS task FROM json_each(?)) tasks
              JOIN maintenance_runs r ON r.id = (SELECT id FROM maintenance_runs WHERE task = tasks.task ORDER BY id DESC LIMIT 1)
        """
    }
}

/** A scrape ages the retained observation. It never records a new success. */
private class AggregateAge(private val began: Instant) : Number() {
    override fun toDouble(): Double = Duration.between(began, Instant.now()).toMillis().coerceAtLeast(0) / 1000.0

    override fun toFloat(): Float = toDouble().toFloat()

    override fun toLong(): Long = toDouble().toLong()

    override fun toInt(): Int = toLong().toInt()

    override fun toShort(): Short = toInt().toShort()

    override fun toByte(): Byte = toInt().toByte()
}
