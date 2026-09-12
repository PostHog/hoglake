package com.posthog.hoglake.service

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.compaction.CompactionTiers
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import java.security.MessageDigest
import java.time.Instant
import java.time.OffsetDateTime

/**
 * Durable, bounded dashboard sampling. Each iteration claims ONE catalog
 * checkpoint (SKIP LOCKED), scans at most batchSize indexed metadata rows,
 * and saves the cursor + tier accumulators atomically. No long-lived DB
 * snapshot, no manifest walk on a dashboard request, no work on commits.
 *
 * Files are evaluated at the captured catalog snapshot, traversed in the
 * same table/row-id/file-id order as compaction. If expiry overtakes the
 * captured snapshot, restart without publishing a partial sample. Hydration
 * state and the removal queue are mutable: those counts are observations
 * over the sampling window, not a transactional point-in-time guarantee.
 */
class MaintenanceSummarySampler(
    private val jdbi: Jdbi,
    targetBytes: Long,
    tierTarget: Int,
    private val refreshSeconds: Long = 60,
) {
    private val tiers = CompactionTiers.of(targetBytes, tierTarget)
    private val target = targetBytes

    init {
        require(refreshSeconds > 0)
    }

    data class Sample(
        val pendingFiles: Long,
        val failedFiles: Long,
        val smallFiles: Long,
        val queuedRemovals: Long,
        val oldestQueuedAt: Instant?,
        val snapshotId: Long,
        val startedAt: Instant,
        val targetBytes: Long,
        val tierTarget: Int,
    )

    data class Published(val sampledAt: Instant, val sample: Sample)

    data class Scan(
        val snapshot: Long,
        val startedAt: Instant,
        val upperTable: Long,
        val upperRow: Long,
        val upperFile: Long,
        val upperRemoval: Long,
        val targetBytes: Long,
        val tierTarget: Int,
        var table: Long = 0,
        var row: Long = 0,
        var file: Long = 0,
        var removal: Long = 0,
        var phase: String = "files",
        var pending: Long = 0,
        var failed: Long = 0,
        var debt: Long = 0,
        var queued: Long = 0,
        var oldest: Instant? = null,
    )

    private data class Job(val catalogId: Long, val generation: Long, val publishedGeneration: Long, val scan: Scan?)

    private data class FileRow(
        val table: Long,
        val row: Long,
        val file: Long,
        val size: Long,
        val stats: String,
        val visible: Boolean,
        val spec: Long?,
        val values: List<String?>?,
        val hasDv: Boolean,
    )

    private data class Pool(
        val table: Long,
        val spec: Long?,
        val values: List<String?>?,
        val quota: Long,
        var remaining: Long,
        var pending: Int,
        var selected: Long = 0,
        var files: Long = 0,
        var small: Long = 0,
        var bytes: Long = 0,
        var smallBytes: Long = 0,
        var dvs: Long = 0,
    )

    /** Share a tick's row budget across at most ten catalogs/checkpoints. */
    fun tick(rowBudget: Int = 10_000) {
        require(rowBudget > 0)
        val jobs = minOf(10, rowBudget)
        repeat(jobs) {
            if (!runOnce(rowBudget / jobs)) return
        }
    }

    /** True if a checkpoint was advanced; false when nothing is due. */
    fun runOnce(batchSize: Int = 10_000): Boolean {
        require(batchSize > 0)
        return jdbi.inTransactionUnchecked { h ->
            h.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            // Discover new catalogs in bounded batches; catalog identity
            // is small, indexed state, not file metadata.
            h.createUpdate(
                """
                INSERT INTO hog_maintenance_summary (catalog_id)
                SELECT c.catalog_id FROM hog_catalog c
                WHERE NOT EXISTS (SELECT 1 FROM hog_maintenance_summary s WHERE s.catalog_id = c.catalog_id)
                ORDER BY c.catalog_id LIMIT 100 ON CONFLICT DO NOTHING
                """,
            ).execute()
            val job =
                h.createQuery(
                    """
                SELECT catalog_id, generation, published_generation, scan_state::text AS scan
                FROM hog_maintenance_summary WHERE next_batch_at <= now()
                ORDER BY next_batch_at, catalog_id LIMIT 1 FOR UPDATE SKIP LOCKED
                """,
                ).map { rs, _ ->
                    Job(
                        rs.getLong("catalog_id"), rs.getLong("generation"), rs.getLong("published_generation"),
                        rs.getString("scan")?.let { json.readValue(it, Scan::class.java) },
                    )
                }.findOne().orElse(null) ?: return@inTransactionUnchecked false

            var generation = job.generation
            var scan = job.scan
            if (scan != null) {
                val earliest =
                    h.createQuery("SELECT earliest_snapshot_id FROM hog_catalog WHERE catalog_id = :id")
                        .bind("id", job.catalogId).mapTo(Long::class.javaObjectType).one()
                if (earliest > scan.snapshot || scan.targetBytes != target || scan.tierTarget != tiers.tierTarget) {
                    checkpoint(h, job.catalogId, generation, null)
                    return@inTransactionUnchecked true
                }
            } else {
                // Garbage collection is bounded too. Preserve the published
                // generation until a replacement has completed.
                // Two range seeks avoid rescanning the preserved generation
                // for every cleanup batch after an abandoned scan.
                val stale =
                    h.createQuery(
                        """
                    SELECT generation FROM (
                        (SELECT generation FROM hog_maintenance_summary_tier
                         WHERE catalog_id = :id AND generation < :published ORDER BY generation LIMIT 1)
                        UNION ALL
                        (SELECT generation FROM hog_maintenance_summary_tier
                         WHERE catalog_id = :id AND generation > :published ORDER BY generation LIMIT 1)
                    ) stale ORDER BY generation LIMIT 1
                    """,
                    ).bind("id", job.catalogId).bind("published", job.publishedGeneration)
                        .mapTo(Long::class.javaObjectType).findOne().orElse(null)
                val removed =
                    if (stale == null) {
                        0
                    } else {
                        h.createUpdate(
                            """
                    DELETE FROM hog_maintenance_summary_tier
                    WHERE catalog_id = :id AND generation = :stale AND bucket_key IN (
                        SELECT bucket_key FROM hog_maintenance_summary_tier
                        WHERE catalog_id = :id AND generation = :stale ORDER BY bucket_key LIMIT :batch
                    )
                    """,
                        ).bind("id", job.catalogId).bind("stale", stale).bind("batch", batchSize).execute()
                    }
                if (removed > 0) {
                    checkpoint(h, job.catalogId, generation, null)
                    return@inTransactionUnchecked true
                }
                generation++
                scan = begin(h, job.catalogId)
            }
            if (scan.phase == "files") {
                val rows =
                    h.createQuery(
                        """
                    SELECT f.table_id, f.row_id_start, f.data_file_id, f.file_size_bytes, f.stats_state, f.spec_id,
                           EXISTS (SELECT 1 FROM hog_delete_file dv WHERE dv.catalog_id = f.catalog_id
                             AND dv.data_file_id = f.data_file_id AND dv.begin_snapshot <= :snapshot
                             AND (dv.end_snapshot IS NULL OR :snapshot < dv.end_snapshot)) AS has_dv,
                           (f.begin_snapshot <= :snapshot AND (f.end_snapshot IS NULL OR :snapshot < f.end_snapshot)) AS visible,
                           (SELECT array_agg(p.value ORDER BY p.key_index) FROM hog_file_partition_value p
                            WHERE p.catalog_id = f.catalog_id AND p.data_file_id = f.data_file_id) AS vals
                    FROM hog_data_file f
                    WHERE f.catalog_id = :id
                      AND (f.table_id, f.row_id_start, f.data_file_id) > (:table, :row, :file)
                      AND (f.table_id, f.row_id_start, f.data_file_id) <= (:upperTable, :upperRow, :upperFile)
                    ORDER BY f.table_id, f.row_id_start, f.data_file_id LIMIT :batch
                    """,
                    ).bind("id", job.catalogId).bind("snapshot", scan.snapshot)
                        .bind("table", scan.table).bind("row", scan.row).bind("file", scan.file)
                        .bind(
                            "upperTable",
                            scan.upperTable,
                        ).bind("upperRow", scan.upperRow).bind("upperFile", scan.upperFile)
                        .bind("batch", batchSize).map { rs, _ ->
                            FileRow(
                                rs.getLong("table_id"), rs.getLong("row_id_start"), rs.getLong("data_file_id"),
                                rs.getLong("file_size_bytes"), rs.getString("stats_state"), rs.getBoolean("visible"),
                                rs.getObject("spec_id")?.let { (it as Number).toLong() },
                                (
                                    rs.getArray(
                                        "vals",
                                    )?.array as? Array<*>
                                )?.map { it as String? },
                                rs.getBoolean("has_dv"),
                            )
                        }.list()
                accumulate(h, job.catalogId, generation, scan, rows)
                rows.lastOrNull()?.let {
                    scan.table = it.table
                    scan.row = it.row
                    scan.file = it.file
                }
                val reachedEnd =
                    scan.table == scan.upperTable && scan.row == scan.upperRow && scan.file == scan.upperFile
                if (rows.size < batchSize || reachedEnd) {
                    scan.phase = "removals"
                }
                checkpoint(h, job.catalogId, generation, scan)
            } else {
                val rows =
                    h.createQuery(
                        """
                    SELECT removal_id, scheduled_at FROM hog_file_removal
                    WHERE catalog_id = :id AND drained_at IS NULL
                      AND removal_id > :cursor AND removal_id <= :upper
                    ORDER BY removal_id LIMIT :batch
                    """,
                    ).bind("id", job.catalogId).bind("cursor", scan.removal).bind("upper", scan.upperRemoval)
                        .bind("batch", batchSize).map { rs, _ ->
                            val at = rs.getObject("scheduled_at", OffsetDateTime::class.java).toInstant()
                            rs.getLong("removal_id") to at
                        }.list()
                scan.queued += rows.size
                for ((id, at) in rows) {
                    scan.removal = id
                    scan.oldest = scan.oldest?.let { minOf(it, at) } ?: at
                }
                if (rows.size < batchSize || scan.removal >= scan.upperRemoval) {
                    val sample =
                        Sample(
                            scan.pending, scan.failed, scan.debt, scan.queued, scan.oldest,
                            scan.snapshot, scan.startedAt, target, tiers.tierTarget,
                        )
                    h.createUpdate(
                        """
                        UPDATE hog_maintenance_summary SET generation = :generation, published_generation = :generation,
                            sampled_at = now(), sample = CAST(:sample AS jsonb), scan_state = NULL,
                            next_batch_at = now() + make_interval(secs => :refresh)
                        WHERE catalog_id = :id
                        """,
                    ).bind("generation", generation).bind("sample", json.writeValueAsString(sample))
                        .bind("refresh", refreshSeconds).bind("id", job.catalogId).execute()
                } else {
                    checkpoint(h, job.catalogId, generation, scan)
                }
            }
            true
        }
    }

    private fun begin(
        h: Handle,
        catalogId: Long,
    ): Scan {
        val snapshot =
            h.createQuery("SELECT last_snapshot_id FROM hog_catalog WHERE catalog_id = :id")
                .bind("id", catalogId).mapTo(Long::class.javaObjectType).one()
        val upper =
            h.createQuery(
                """
            SELECT table_id, row_id_start, data_file_id FROM hog_data_file WHERE catalog_id = :id
            ORDER BY table_id DESC, row_id_start DESC, data_file_id DESC LIMIT 1
            """,
            ).bind("id", catalogId).map { rs, _ -> Triple(rs.getLong(1), rs.getLong(2), rs.getLong(3)) }
                .findOne().orElse(Triple(0L, 0L, 0L))
        val removal =
            h.createQuery(
                """
                SELECT removal_id FROM hog_file_removal
                WHERE catalog_id = :id AND drained_at IS NULL ORDER BY removal_id DESC LIMIT 1
                """,
            ).bind("id", catalogId).mapTo(Long::class.javaObjectType).findOne().orElse(0L)
        return Scan(snapshot, Instant.now(), upper.first, upper.second, upper.third, removal, target, tiers.tierTarget)
    }

    private fun accumulate(
        h: Handle,
        catalogId: Long,
        generation: Long,
        scan: Scan,
        rows: List<FileRow>,
    ) {
        val keyed =
            rows.filter { it.visible }.map { f ->
                if (f.stats == "pending") scan.pending++
                if (f.stats == "failed") scan.failed++
                val quota = tiers.tierOf(f.size)?.let { tiers.reachFloor(it) } ?: 0L
                val bytes = json.writeValueAsBytes(listOf(f.table, f.spec, f.values, quota))
                val key = java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))
                Triple(key, quota, f)
            }
        if (keyed.isEmpty()) return
        val pools =
            h.createQuery(
                """
            SELECT * FROM hog_maintenance_summary_tier
            WHERE catalog_id = :id AND generation = :generation AND bucket_key = ANY(:keys)
            """,
            ).bind("id", catalogId).bind("generation", generation)
                .bindArray("keys", String::class.java, keyed.map { it.first }.distinct())
                .map { rs, _ ->
                    rs.getString("bucket_key") to
                        Pool(
                            rs.getLong("table_id"), rs.getObject("spec_id")?.let { (it as Number).toLong() },
                            (rs.getArray("partition_values")?.array as? Array<*>)?.map { it as String? },
                            rs.getLong("quota"), rs.getLong("remaining"), rs.getInt("pending"), rs.getLong("selected"),
                            rs.getLong("file_count"), rs.getLong("small_count"), rs.getLong("total_bytes"),
                            rs.getLong("small_bytes"), rs.getLong("dv_count"),
                        )
                }
                .list().toMap().toMutableMap()
        for ((key, quota, f) in keyed) {
            val pool = pools.getOrPut(key) { Pool(f.table, f.spec, f.values, quota, maxOf(1, quota), 0) }
            pool.files++
            pool.bytes = Math.addExact(pool.bytes, f.size)
            if (f.hasDv) pool.dvs++
            if (f.size < target) {
                pool.small++
                pool.smallBytes = Math.addExact(pool.smallBytes, f.size)
            }
            if (quota == 0L || f.size == 0L) continue
            if (f.size >= pool.remaining) {
                scan.debt += pool.pending + 1
                pool.selected += pool.pending + 1
                pool.pending = 0
                pool.remaining = quota
            } else {
                pool.remaining -= f.size
                pool.pending++
            }
        }
        val batch =
            h.prepareBatch(
                """
            INSERT INTO hog_maintenance_summary_tier
                (catalog_id, generation, bucket_key, table_id, spec_id, partition_values, quota, remaining, pending,
                 selected, file_count, small_count, total_bytes, small_bytes, dv_count)
            VALUES (:id, :generation, :key, :table, :spec,
                    CASE WHEN :vals::jsonb = 'null'::jsonb THEN NULL
                         ELSE ARRAY(SELECT jsonb_array_elements_text(:vals::jsonb)) END,
                    :quota, :remaining, :pending,
                    :selected, :files, :small, :bytes, :smallBytes, :dvs)
            ON CONFLICT (catalog_id, generation, bucket_key) DO UPDATE
            SET remaining = excluded.remaining, pending = excluded.pending, selected = excluded.selected,
                file_count = excluded.file_count, small_count = excluded.small_count, total_bytes = excluded.total_bytes,
                small_bytes = excluded.small_bytes, dv_count = excluded.dv_count
            """,
            )
        for ((key, pool) in pools) {
            batch.bind("id", catalogId).bind("generation", generation).bind("key", key).bind("quota", pool.quota)
                .bind("table", pool.table).bindBySqlType("spec", pool.spec, java.sql.Types.BIGINT)
                .bind("vals", json.writeValueAsString(pool.values))
                .bind("remaining", pool.remaining).bind("pending", pool.pending).bind("selected", pool.selected)
                .bind("files", pool.files).bind("small", pool.small).bind("bytes", pool.bytes)
                .bind("smallBytes", pool.smallBytes).bind("dvs", pool.dvs).add()
        }
        batch.execute()
    }

    private fun checkpoint(
        h: Handle,
        id: Long,
        generation: Long,
        scan: Scan?,
    ) {
        h.createUpdate(
            """
            UPDATE hog_maintenance_summary SET generation = :generation, scan_state = CAST(:scan AS jsonb),
                next_batch_at = clock_timestamp() WHERE catalog_id = :id
            """,
        ).bind("id", id).bind("generation", generation)
            .bind("scan", scan?.let { json.writeValueAsString(it) }).execute()
    }

    companion object {
        private val json =
            jacksonObjectMapper().registerModule(JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

        /** Indexed summary reads only. Missing is unknown, never a fabricated zero. */
        fun read(
            h: Handle,
            catalogIds: List<Long>,
        ): Map<Long, Published> {
            if (catalogIds.isEmpty()) return emptyMap()
            return h.createQuery(
                """
                SELECT catalog_id, sampled_at, sample::text AS sample FROM hog_maintenance_summary
                WHERE catalog_id = ANY(:ids) AND sample IS NOT NULL
                """,
            ).bindArray("ids", Long::class.javaObjectType, catalogIds).map { rs, _ ->
                rs.getLong("catalog_id") to
                    Published(
                        rs.getObject("sampled_at", java.time.OffsetDateTime::class.java).toInstant(),
                        json.readValue(rs.getString("sample"), Sample::class.java),
                    )
            }.list().toMap()
        }
    }
}
