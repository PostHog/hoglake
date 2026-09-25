package com.posthog.hoglake.service

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.compaction.CompactionGrouping
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import java.security.MessageDigest
import java.time.Instant
import java.time.OffsetDateTime

/**
 * Durable, bounded dashboard sampling. Each iteration claims ONE catalog
 * checkpoint (SKIP LOCKED), scans at most batchSize indexed metadata rows,
 * and saves the cursor + bucket accumulators atomically. No long-lived DB
 * snapshot, no manifest walk on a dashboard request, no work on commits.
 *
 * Files are evaluated at the captured catalog snapshot, traversed in
 * `(table_id, file_size_bytes, data_file_id)` order — the order
 * compaction BIN-PACKS in, which is why V10 adds an index for it. The
 * rule mirrored here is only decidable in size order. If expiry overtakes the
 * captured snapshot, restart without publishing a partial sample. Hydration
 * state and the removal queue are mutable: those counts are observations
 * over the sampling window, not a transactional point-in-time guarantee.
 */
class MaintenanceSummarySampler(
    private val jdbi: Jdbi,
    targetBytes: Long,
    private val minInputFiles: Int,
    private val maxInputFiles: Int,
    private val refreshSeconds: Long = 60,
) {
    private val grouping = CompactionGrouping.of(targetBytes)
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
        val minInputFiles: Int,
        val maxInputFiles: Int,
    )

    data class Published(val sampledAt: Instant, val sample: Sample)

    data class Scan(
        val snapshot: Long,
        val startedAt: Instant,
        val upperTable: Long,
        val upperSize: Long,
        val upperFile: Long,
        val upperRemoval: Long,
        val targetBytes: Long,
        val minInputFiles: Int,
        val maxInputFiles: Int,
        var table: Long = 0,
        var size: Long = -1,
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
        /** Largest file in the partial group — its last, the scan being size-ascending. */
        var pendingMax: Long = 0,
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
                        // A scan_state this build cannot parse is a scan
                        // from a build whose cursor had a different shape
                        // (the keyset moved from row-id to size order).
                        // The state is a disposable sample, so drop it and
                        // start a fresh scan rather than killing the sweep.
                        rs.getString("scan")?.let {
                            runCatching { json.readValue(it, Scan::class.java) }.getOrNull()
                        },
                    )
                }.findOne().orElse(null) ?: return@inTransactionUnchecked false

            var generation = job.generation
            var scan = job.scan
            if (scan != null) {
                val earliest =
                    h.createQuery("SELECT earliest_snapshot_id FROM hog_catalog WHERE catalog_id = :id")
                        .bind("id", job.catalogId).mapTo(Long::class.javaObjectType).one()
                if (earliest > scan.snapshot || scan.targetBytes != target || scan.minInputFiles != minInputFiles ||
                    scan.maxInputFiles != maxInputFiles
                ) {
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
                // DROPPED TABLES ARE SKIPPED BY KEY RANGE, not by page.
                //
                // Since #193 a drop touches no file row, so a dropped
                // 3M-row table stays in this scan's key space in full.
                // At the default 10,000 rows a tick and a 1 s interval
                // that is five hours of ticks spent paging through rows
                // the compaction planner will never plan (its
                // `liveTables` excludes dropped tables) — and the
                // catalog publishes no sample at all until the scan
                // finishes, so every dashboard on the instance goes
                // stale for those five hours.
                //
                // Read FRESH EVERY TICK rather than at `begin()`, so a
                // table dropped mid-scan is skipped from the next tick
                // instead of at the next generation. Cheap: it is a
                // range scan of this catalog's slice of hog_table's
                // primary key, against a page of 10,000 manifest rows
                // with their partition values and DV probes.
                //
                // NOT IN `scan_state`. That column is parsed with
                // `runCatching` and a shape change DISCARDS the
                // checkpoint (see the Job mapper), so putting a
                // mutable set in it would throw away every in-flight
                // scan on the deploy that added it — and the set would
                // be stale by construction anyway.
                val dropped =
                    h.createQuery(
                        """
                    SELECT table_id FROM hog_table
                    WHERE catalog_id = :id AND dropped_snapshot IS NOT NULL
                    """,
                    ).bind("id", job.catalogId).mapTo(Long::class.javaObjectType).list().toSet()
                val rows =
                    h.createQuery(
                        """
                    SELECT f.table_id, f.data_file_id, f.file_size_bytes, f.stats_state, f.spec_id,
                           EXISTS (SELECT 1 FROM hog_delete_file dv WHERE dv.catalog_id = f.catalog_id
                             AND dv.data_file_id = f.data_file_id AND dv.begin_snapshot <= :snapshot
                             AND (dv.end_snapshot IS NULL OR :snapshot < dv.end_snapshot)) AS has_dv,
                           (f.begin_snapshot <= :snapshot AND (f.end_snapshot IS NULL OR :snapshot < f.end_snapshot)) AS visible,
                           (SELECT array_agg(p.value ORDER BY p.key_index) FROM hog_file_partition_value p
                            WHERE p.catalog_id = f.catalog_id AND p.data_file_id = f.data_file_id) AS vals
                    FROM hog_data_file f
                    WHERE f.catalog_id = :id
                      AND (f.table_id, f.file_size_bytes, f.data_file_id) > (:table, :size, :file)
                      AND (f.table_id, f.file_size_bytes, f.data_file_id) <= (:upperTable, :upperSize, :upperFile)
                    ORDER BY f.table_id, f.file_size_bytes, f.data_file_id LIMIT :batch
                    """,
                    ).bind("id", job.catalogId).bind("snapshot", scan.snapshot)
                        .bind("table", scan.table).bind("size", scan.size).bind("file", scan.file)
                        .bind(
                            "upperTable",
                            scan.upperTable,
                        ).bind("upperSize", scan.upperSize).bind("upperFile", scan.upperFile)
                        .bind("batch", batchSize).map { rs, _ ->
                            FileRow(
                                rs.getLong("table_id"),
                                rs.getLong("data_file_id"),
                                rs.getLong("file_size_bytes"),
                                rs.getString("stats_state"),
                                rs.getBoolean("visible"),
                                rs.getObject("spec_id")?.let { (it as Number).toLong() },
                                (
                                    rs.getArray(
                                        "vals",
                                    )?.array as? Array<*>
                                )?.map { it as String? },
                                rs.getBoolean("has_dv"),
                            )
                        }.list()
                // THE PAGE'S FIRST ROW DECIDES. If it belongs to a
                // dropped table, the whole page is discarded unread and
                // the cursor jumps PAST that table's key range —
                // (table_id, +inf, +inf) — so the table costs ONE page
                // however many million rows it holds. A page that
                // starts live and runs into a dropped table mid-way is
                // accumulated minus the dropped rows; the next tick
                // starts inside the dropped table and takes this
                // branch, so the waste is bounded at one page per
                // dropped table per scan.
                //
                // That page is not free, and it is worth being honest
                // about: it is a FULL batch (10,000 rows by default)
                // carrying a per-row `EXISTS` over hog_delete_file and
                // a per-row `array_agg` over the partition values,
                // fetched and then thrown away. One of those per
                // dropped table per generation is the price of an O(1)
                // skip; paging the table instead costs one of them per
                // 10,000 ROWS.
                val leadsInDroppedTable = rows.firstOrNull()?.takeIf { it.table in dropped }
                if (leadsInDroppedTable != null) {
                    scan.table = leadsInDroppedTable.table
                    scan.size = Long.MAX_VALUE
                    scan.file = Long.MAX_VALUE
                    checkpoint(h, job.catalogId, generation, scan)
                    return@inTransactionUnchecked true
                }
                accumulate(h, job.catalogId, generation, scan, rows.filter { it.table !in dropped })
                rows.lastOrNull()?.let {
                    scan.table = it.table
                    scan.size = it.size
                    scan.file = it.file
                }
                val reachedEnd =
                    scan.table == scan.upperTable && scan.size == scan.upperSize && scan.file == scan.upperFile
                if (rows.size < batchSize || reachedEnd) {
                    flushTails(h, job.catalogId, generation, scan)
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
                            scan.snapshot, scan.startedAt, target, minInputFiles, maxInputFiles,
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
            SELECT table_id, file_size_bytes, data_file_id FROM hog_data_file WHERE catalog_id = :id
            ORDER BY table_id DESC, file_size_bytes DESC, data_file_id DESC LIMIT 1
            """,
            ).bind("id", catalogId).map { rs, _ -> Triple(rs.getLong(1), rs.getLong(2), rs.getLong(3)) }
                .findOne().orElse(Triple(0L, -1L, 0L))
        val removal =
            h.createQuery(
                """
                SELECT removal_id FROM hog_file_removal
                WHERE catalog_id = :id AND drained_at IS NULL ORDER BY removal_id DESC LIMIT 1
                """,
            ).bind("id", catalogId).mapTo(Long::class.javaObjectType).findOne().orElse(0L)
        return Scan(
            snapshot, Instant.now(), upper.first, upper.second, upper.third, removal,
            target, minInputFiles, maxInputFiles,
        )
    }

    /**
     * Saturating, not checked, addition for the DISPLAY byte counters.
     *
     * `file_size_bytes` is writer-supplied and validated only as
     * non-negative, so a registration near Long.MAX_VALUE is reachable.
     * `Math.addExact` threw on it, and a throw here is not confined to
     * the offending catalog: `runOnce` rolls back, so `next_batch_at` is
     * never advanced, so `ORDER BY next_batch_at, catalog_id LIMIT 1`
     * re-picks the same catalog on the next tick — and `tick` abandons
     * the whole round on the first failure. One bad row stopped the
     * sampler for EVERY catalog on the instance, which a burn-in
     * confirmed: an innocent catalog stopped publishing entirely.
     *
     * These two fields are a dashboard total. Saturating is honest
     * enough at that magnitude and cannot take the sampler down.
     */
    private fun satAdd(
        a: Long,
        b: Long,
    ): Long {
        val sum = a + b
        // Overflow iff the operands share a sign that the result does not.
        return if (((a xor sum) and (b xor sum)) < 0) Long.MAX_VALUE else sum
    }

    /**
     * Files a group holding a largest-file of [largest] must have to be
     * worth rewriting — `min(minInputFiles, target / largest)`, floored
     * at 2. The mirror of the same expression in
     * `CompactionGrouping.groups`; the two must not drift, or the debt
     * this reports is debt from a policy the planner does not run.
     */

    private fun needFor(largest: Long): Int {
        val fit = if (largest <= 0) minInputFiles.toLong() else target / largest
        return maxOf(2L, minOf(minInputFiles.toLong(), fit)).toInt()
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
                // One quota now, not one per size band: every group
                // packs to the compaction target. The pool key keeps the
                // field so the checkpoint schema is unchanged.
                val quota = grouping.targetBytes
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
                            rs.getLong("pending_max_bytes"),
                            rs.getLong("file_count"), rs.getLong("small_count"), rs.getLong("total_bytes"),
                            rs.getLong("small_bytes"), rs.getLong("dv_count"),
                        )
                }
                .list().toMap().toMutableMap()
        for ((key, quota, f) in keyed) {
            val pool = pools.getOrPut(key) { Pool(f.table, f.spec, f.values, quota, maxOf(1, quota), 0) }
            pool.files++
            pool.bytes = satAdd(pool.bytes, f.size)
            if (f.hasDv) pool.dvs++
            if (f.size < target) {
                pool.small++
                pool.smallBytes = satAdd(pool.smallBytes, f.size)
            }
            // A file already at the target is not a candidate — the
            // planner's own query says `file_size_bytes < :targetBytes`
            // — so it must not join a group here either. It still counts
            // in file_count and total_bytes above: the dashboard reports
            // it, compaction leaves it alone.
            if (f.size >= target) continue
            // Mirror CompactionGrouping.groups exactly, or the debt this
            // reports is debt from a policy the planner no longer runs.
            // Close on EITHER bound, and only count a group the planner
            // would actually take.
            // Mirror CompactionGrouping's split: close before a file
            // that outweighs everything the pool holds, so a near-target
            // file never gets counted as debt merely for absorbing a
            // trickle -- and so the smalls behind it are not stranded.
            if (pool.pending > 0 &&
                (quota - pool.remaining) * CompactionGrouping.DOMINANCE_FACTOR < f.size
            ) {
                if (pool.pending >= needFor(pool.pendingMax)) {
                    scan.debt += pool.pending
                    pool.selected += pool.pending
                }
                pool.pending = 0
                pool.pendingMax = 0
                pool.remaining = quota
            }
            val held = pool.pending + 1
            if (f.size >= pool.remaining || held >= maxInputFiles) {
                // The closing file is the group's LARGEST, the scan being
                // size-ascending — so the same per-group rule the planner
                // applies is decidable right here, with no lookahead. The
                // group's bytes come back the same way the planner
                // recovers them: everything held before this file is
                // `quota - remaining`.
                if (held >= needFor(f.size)) {
                    scan.debt += held
                    pool.selected += held
                }
                pool.pending = 0
                pool.pendingMax = 0
                pool.remaining = quota
            } else {
                pool.remaining -= f.size
                pool.pending++
                pool.pendingMax = maxOf(pool.pendingMax, f.size)
            }
        }
        val batch =
            h.prepareBatch(
                """
            INSERT INTO hog_maintenance_summary_tier
                (catalog_id, generation, bucket_key, table_id, spec_id, partition_values, quota, remaining, pending,
                 selected, pending_max_bytes, file_count, small_count, total_bytes, small_bytes, dv_count)
            VALUES (:id, :generation, :key, :table, :spec,
                    CASE WHEN :vals::jsonb = 'null'::jsonb THEN NULL
                         ELSE ARRAY(SELECT jsonb_array_elements_text(:vals::jsonb)) END,
                    :quota, :remaining, :pending,
                    :selected, :pendingMax, :files, :small, :bytes, :smallBytes, :dvs)
            ON CONFLICT (catalog_id, generation, bucket_key) DO UPDATE
            SET remaining = excluded.remaining, pending = excluded.pending, selected = excluded.selected,
                pending_max_bytes = excluded.pending_max_bytes,
                file_count = excluded.file_count, small_count = excluded.small_count, total_bytes = excluded.total_bytes,
                small_bytes = excluded.small_bytes, dv_count = excluded.dv_count
            """,
            )
        for ((key, pool) in pools) {
            batch.bind("id", catalogId).bind("generation", generation).bind("key", key).bind("quota", pool.quota)
                .bind("table", pool.table).bindBySqlType("spec", pool.spec, java.sql.Types.BIGINT)
                .bind("vals", json.writeValueAsString(pool.values))
                .bind("remaining", pool.remaining).bind("pending", pool.pending).bind("selected", pool.selected)
                .bind("pendingMax", pool.pendingMax)
                .bind("files", pool.files).bind("small", pool.small).bind("bytes", pool.bytes)
                .bind("smallBytes", pool.smallBytes).bind("dvs", pool.dvs).add()
        }
        batch.execute()
    }

    /**
     * Count every bucket's trailing remainder, once the file phase has
     * seen the last file.
     *
     * [accumulate] can only close a group when a file arrives that fills
     * the quota or the fan-in, so when the scan runs out of files each
     * bucket is left holding a partial group in `pending`. The planner
     * emits that tail — see the tail paragraph in
     * `CompactionGrouping.groups` — so debt that ignored it would report
     * zero for exactly the tables the planner is about to rewrite: a
     * partition that stopped receiving writes is ALL tail.
     *
     * Under the ladder this function had no counterpart, and correctly
     * so: a bucket that could not reach its tier floor was skipped, so a
     * remainder was not debt. That is the single behavioural difference
     * between the two accounting rules.
     *
     * Runs inside the scan's transaction and zeroes what it counts, so a
     * retry after the checkpoint cannot count a tail twice.
     */
    private fun flushTails(
        h: Handle,
        catalogId: Long,
        generation: Long,
        scan: Scan,
    ) {
        // `need` is computed in SQL from each bucket's own carried max,
        // so this stays one statement per phase rather than a read of
        // every bucket into the JVM. greatest(2, least(min, target/max))
        // is needFor() transcribed; a bucket that never held a file has
        // pending_max_bytes 0, and `pending > 0` excludes it anyway.
        val need =
            "greatest(2, least(CAST(:min AS bigint), " +
                "CASE WHEN pending_max_bytes <= 0 THEN CAST(:min AS bigint) " +
                "ELSE CAST(:target AS bigint) / pending_max_bytes END))"
        val tail =
            h.createQuery(
                """
                SELECT COALESCE(sum(pending), 0) FROM hog_maintenance_summary_tier
                WHERE catalog_id = :id AND generation = :generation AND pending >= $need
                """,
            ).bind("id", catalogId).bind("generation", generation)
                .bind("min", minInputFiles).bind("target", target)
                .mapTo(Long::class.java).one()
        scan.debt += tail
        h.createUpdate(
            """
            UPDATE hog_maintenance_summary_tier
            SET selected = selected + CASE WHEN pending >= $need THEN pending ELSE 0 END,
                pending = 0, pending_max_bytes = 0, remaining = quota
            WHERE catalog_id = :id AND generation = :generation AND pending > 0
            """,
        ).bind("id", catalogId).bind("generation", generation)
            .bind("min", minInputFiles).bind("target", target).execute()
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
                // A sample this build cannot parse was published by a
                // build whose Sample had a different shape, and this runs
                // on the REQUEST path: /maintenance/status and
                // /stats/partitions both read it. Dropping it degrades
                // those to the warmup state they already model — absent
                // sampled_at, empty partitions — for the minute or so
                // until the sampler republishes. Raising instead would
                // 500 every dashboard read across the deploy window.
                val sample =
                    runCatching {
                        json.readValue(rs.getString("sample"), Sample::class.java)
                    }.getOrNull()
                sample?.let {
                    rs.getLong("catalog_id") to
                        Published(
                            rs.getObject("sampled_at", java.time.OffsetDateTime::class.java).toInstant(),
                            it,
                        )
                }
            }.list().filterNotNull().toMap()
        }
    }
}
