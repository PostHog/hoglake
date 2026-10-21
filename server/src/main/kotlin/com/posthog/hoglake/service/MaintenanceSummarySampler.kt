package com.posthog.hoglake.service

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
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
 * Files are traversed in `(table_id, file_size_bytes, data_file_id)`
 * order — the order compaction BIN-PACKS in, which is why V10 adds an
 * index for it. The rule mirrored here is only decidable in size order.
 * Hydration state and the removal queue are mutable: those counts are
 * observations over the sampling window, not a transactional
 * point-in-time guarantee. Since the re-anchor below, so is file
 * visibility, whenever the published sample's `reanchors` is nonzero.
 *
 * # Expiry overtaking a scan RE-ANCHORS it; it no longer restarts it
 *
 * A scan captures `Scan.snapshot` (the catalog head when it began) and
 * evaluates each page's visibility at `Scan.visibleAt`, which starts
 * equal to it. When expiry advances `earliest_snapshot_id` past the
 * snapshot being evaluated at, the rows that decided visibility there
 * are being purged (expiry deletes ended rows below the floor in pages
 * outside its lock, invariant 5), so a later page evaluated at the old
 * snapshot could silently miss files that were visible at it. The scan
 * used to answer that by DISCARDING itself and starting over at head.
 *
 * That restart is unbounded once a scan takes longer than retention,
 * and on gigahog-prod-us (2026-10-21) both did: millpond's last
 * complete scan was 58 min for ~7.6M rows (~0.36 ms/row at 10,000 rows
 * per 1 s tick) against a 3,600 s retention, millrace's against 600 s.
 * Neither published a generation after 2026-10-16. The gauges went
 * stale, and worse, the compaction planner — which picks buckets only
 * from the published generation — stopped seeing every partition
 * written after it, i.e. the hottest ones.
 *
 * Now the scan RE-ANCHORS: `visibleAt` moves to the catalog's current
 * head (`last_snapshot_id`), `reanchors` counts it, and the scan
 * continues FROM ITS CURSOR. Head is always legal — expiry never passes
 * head (invariant 5) — and it maximises the time until the next
 * re-anchor. Any snapshot at or above the new floor is decidable: a row
 * ended at or below the floor is invisible at every such snapshot, so
 * its purge cannot change the answer.
 *
 * The RACE is the one the restart already relied on: expiry commits the
 * floor advance under the commit lock BEFORE it purges a single row, and
 * this transaction is REPEATABLE READ from its first statement, so a
 * tick either sees the new floor (and re-anchors before its page) or
 * sees none of the purge either.
 *
 * The other restart triggers stay restarts: a changed target, min or
 * max input files changes what `selected`/`small_count` MEAN, and a
 * generation mixing two definitions is not an observation of anything.
 *
 * # The keyset argument: no row is skipped or counted twice
 *
 * The cursor is a strict `>` on `(table_id, file_size_bytes,
 * data_file_id)` and the upper bound an inclusive `<=` captured at
 * `begin()`. All three columns are IMMUTABLE on a `hog_data_file` row
 * (a file is never resized or re-tabled; compaction writes a NEW row)
 * and `data_file_id` is unique within the catalog, so the key is a
 * total order over rows that no snapshot changes. A re-anchor moves the
 * snapshot that `visible` and the DV probe are evaluated at and NOTHING
 * ELSE: it happens between pages, before the tick's page is read, so
 * every page is evaluated at exactly one snapshot, every row at or below
 * the cursor was paged exactly once (at the old one) and every row above
 * it will be paged exactly once (at the new one). The page boundary is
 * the cursor itself, so a row cannot sit on both sides of it.
 *
 * What a re-anchor DOES change is which DATA the rows describe, and the
 * error is bounded by the files committed or ended inside the scan's
 * window, `(snapshot, visibleAt]`:
 *
 *  - ABOVE the cursor the sample is strictly more current: a file ended
 *    in the window is invisible at `visibleAt` and is not counted, and a
 *    file appended in the window is.
 *  - BELOW the cursor it is the old snapshot's: a file paged before the
 *    re-anchor and ended since stays counted, and a file appended since
 *    with a key below the cursor is not seen. Size order makes the
 *    second case the common one (new ingest files are SMALL), which is
 *    why the compaction planner does not trust the sample alone for new
 *    files — `CompactionService.newBuckets` reads them from the
 *    changefeed since `snapshot`.
 *  - ACROSS the cursor, a compaction in the window can be counted on
 *    both sides: its small inputs paged at the old snapshot, its larger
 *    output above the cursor at the new one. The live totals (V27) can
 *    therefore over-count by up to the bytes and rows compacted in the
 *    window, and the small-file measure can over-count those inputs.
 *
 * So for a re-anchored generation the V27 live totals and every tier
 * measure are an OBSERVATION OVER THE SCAN WINDOW, like the hydration
 * and removal counts already were; `reanchors` on the published
 * [Sample] is what says so. Rows above the upper bound — files
 * registered after `begin()` that sort past the last key it saw, and
 * every table created since — are outside the generation as before.
 *
 * `Scan.snapshot` is deliberately NOT moved, and three readers depend on
 * it staying the scan's START:
 *
 *  - the COVERAGE rule (`TierTotalsRepo`, `CatalogService.getTable`): a
 *    table created at or before `Sample.snapshotId` is covered. A table
 *    created later has a `table_id` above `begin()`'s upper bound and
 *    was never scanned, so advancing the snapshot would report it as
 *    covered and empty;
 *  - `published_snapshot` (V27): the metrics tick subtracts buckets of
 *    tables dropped AFTER it. A table dropped inside the window may have
 *    been paged before its drop, so a later snapshot would leave its
 *    buckets in the totals;
 *  - the planner's new-bucket read, whose lower bound must be the
 *    earliest snapshot the sample may have missed appends after.
 *
 * The dropped-table key-range skip is unaffected: it reads the CURRENT
 * set of dropped tables every tick, not a snapshot. The removal phase
 * reads no file row, so it neither checks nor re-anchors.
 *
 * # Before a rollout or a rollback: read this
 *
 * A catalog whose scan outlasts its retention — millpond-prod-us and
 * millrace-prod-us today — RE-ANCHORS ON EVERY GENERATION, and a build
 * from before this change cannot read a re-anchored checkpoint: it
 * discards it and starts over, which is the old restart. While old and
 * new pods both run the sampler, the two take ticks of the same catalog
 * in turn, and every old pod's tick throws the scan away. Measured (QE,
 * old and new pods alternating ticks under expiry pressure): 120 ticks,
 * 40 generations started, NOTHING published. So:
 *
 *  - DURING A ROLLOUT, a re-anchoring catalog publishes nothing until
 *    the last old sampler pod is gone. Its published sample, gauges,
 *    table-GET totals and the planner's bucket list stay at the last
 *    generation published before the rollout began. Catalogs whose scan
 *    fits inside retention never re-anchor and are unaffected. The
 *    sampler runs only where `HOGLAKE_MAINTENANCE_SUMMARY_INTERVAL_MS >
 *    0` (the maintenance pod on gigahog), so the window is that pod's
 *    replacement, not the API fleet's.
 *  - A ROLLBACK TO A BUILD BEFORE THIS ONE returns those catalogs to the
 *    no-publish state of 2026-10-16 to 10-21, for as long as it runs.
 *    It also cannot read a sample this build published with a nonzero
 *    `reanchors` (the older parser is strict), so their status page,
 *    head table-GET totals and partition listing go ABSENT on that build
 *    rather than stale, and the compaction planner, which reads only the
 *    generation number, keeps planning from that sample's buckets.
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
        /**
         * How many times the scan behind this sample RE-ANCHORED its
         * visibility snapshot because expiry overtook it (see the class
         * KDoc). Zero means every file measure is exact at [snapshotId];
         * nonzero means they are an observation over the scan window
         * that starts there, as the hydration and removal counts always
         * were.
         *
         * OMITTED FROM THE JSON WHEN ZERO, for the deploy straddle: a
         * build BEFORE this one parses the sample strictly, so a key it
         * does not know makes it drop the WHOLE sample — every
         * dashboard and every head table GET's totals on that build go
         * absent until it republishes, which on a rollback is the very
         * build that cannot finish a large catalog's scan. Written only
         * when nonzero, the sample an older build sees is byte-for-byte
         * the shape it knows for every catalog that never re-anchored.
         * This build and later ignore unknown sample keys ([sampleFrom]).
         */
        @get:JsonInclude(JsonInclude.Include.NON_DEFAULT)
        val reanchors: Int = 0,
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
        /**
         * Whether THIS scan's accumulators measure `record_count` and
         * `newest_begin_snapshot` (V22). Carried in the checkpoint so
         * the PUBLISH can stamp it, rather than being written when the
         * generation begins: a marker that named the generation being
         * SCANNED would disagree with `published_generation` for the
         * whole of every scan, which on a 30-minute production
         * generation is ~97% of wall-clock time with the two measures
         * reported as "not measured".
         *
         * Defaults false so a checkpoint written by a pre-V22 build and
         * resumed here is treated as unmeasured — the deploy straddle
         * V22's header describes. `begin()` constructs it true, so
         * every scan this build starts publishes measured.
         */
        val measures: Boolean = false,
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
        /**
         * The snapshot file visibility is evaluated at, when a re-anchor
         * has moved it off [snapshot]; null means [snapshot]. See the
         * class KDoc for why [snapshot] itself never moves.
         *
         * NULL AND ZERO ARE OMITTED from the checkpoint for the same
         * reason [Sample.reanchors] is: the checkpoint is parsed
         * strictly and an unknown key DISCARDS the in-flight scan, so a
         * rollback past this build keeps every scan that never
         * re-anchored. One that did re-anchor is discarded by an old pod
         * on each tick it claims the catalog, and the next new-build
         * tick starts it over: during a rolling deploy (or a rollback in
         * progress) a re-anchoring catalog can lose its in-flight scan
         * once per tick on an old pod, bounded by the rollout's length.
         */
        @get:JsonInclude(JsonInclude.Include.NON_NULL)
        var visibleAt: Long? = null,
        /**
         * Re-anchors so far; published as [Sample.reanchors]. Counts
         * EVENTS — file-phase ticks that found the floor past
         * [evaluatedAt] and MOVED [visibleAt] to a newer head — not
         * pages: a tick whose page then only skips a dropped table's key
         * range counts as one too, and a tick that would leave the
         * snapshot where it is counts nothing.
         */
        @get:JsonInclude(JsonInclude.Include.NON_DEFAULT)
        var reanchors: Int = 0,
    ) {
        /** The snapshot this scan's next page is evaluated at. */
        @get:JsonIgnore
        val evaluatedAt: Long get() = visibleAt ?: snapshot
    }

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
        /** Rows in the file, summed per bucket for the partitions listing. */
        val rows: Long,
        /** The file's begin snapshot; the bucket keeps the newest. */
        val begin: Long,
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
        /** Rows in the bucket's live files (V22). */
        var rows: Long = 0,
        /**
         * max(begin_snapshot) over them, null while the bucket has seen
         * no file — which is also every row a pre-V22 sampler wrote, and
         * what the listing reads "not sampled yet" off.
         */
        var newest: Long? = null,
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
                // A POLICY CHANGE RESTARTS: it changes what the
                // accumulators measure, so a generation spanning it would
                // be two definitions summed.
                if (scan.targetBytes != target || scan.minInputFiles != minInputFiles ||
                    scan.maxInputFiles != maxInputFiles
                ) {
                    checkpoint(h, job.catalogId, generation, null)
                    return@inTransactionUnchecked true
                }
                // EXPIRY OVERTAKING THE SCAN RE-ANCHORS IT, and only in
                // the file phase, the one that reads visibility. The
                // class KDoc has the argument; the short form is that the
                // cursor is over immutable keys, so moving the snapshot
                // between pages neither skips nor repeats a row, and
                // restarting instead never finishes once a scan outlasts
                // retention (millpond and millrace on prod-us, 2026-10-16
                // to 10-21).
                if (scan.phase == "files") {
                    val (earliest, head) =
                        h.createQuery(
                            "SELECT earliest_snapshot_id, last_snapshot_id FROM hog_catalog WHERE catalog_id = :id",
                        ).bind("id", job.catalogId).map { rs, _ -> rs.getLong(1) to rs.getLong(2) }.one()
                    // Counted only when the snapshot MOVES. With no commit
                    // since the last re-anchor, head is still what the
                    // scan evaluates at, and the floor cannot pass head
                    // (invariant 5), so `earliest > evaluatedAt` with
                    // `head == evaluatedAt` is not reachable through
                    // expiry; the guard keeps the counter honest about
                    // what it counts — snapshot changes — whatever moved
                    // the floor.
                    if (earliest > scan.evaluatedAt && head != scan.evaluatedAt) {
                        scan.visibleAt = head
                        scan.reanchors++
                    }
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
                           f.record_count, f.begin_snapshot,
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
                    ).bind("id", job.catalogId).bind("snapshot", scan.evaluatedAt)
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
                                rs.getLong("record_count"),
                                rs.getLong("begin_snapshot"),
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
                            reanchors = scan.reanchors,
                        )
                    // A RE-ANCHORED GENERATION PUBLISHES LIKE ANY OTHER.
                    // `published_snapshot` and the sample's `snapshotId`
                    // stay the scan's START (`scan.snapshot`), never the
                    // last re-anchor: see the class KDoc for the three
                    // readers that need the start. `live_as_of` is the
                    // start too, so a re-anchored generation's totals read
                    // as "observed since then", which is what they are.
                    // THE LIVE TOTALS ARE SUMMED HERE, ONCE PER GENERATION
                    // (V27, #269). The metrics sampler used to sum the
                    // manifest every 15 s on every pod; summing the tier
                    // rows there instead would still be O(buckets) per
                    // catalog per tick. So the publish takes the sum from
                    // the generation it has just finished — one range of
                    // the tier primary key, bounded by this catalog's
                    // buckets, paid once per generation — and the tick
                    // reads them. `live_rows` only when the scan measured
                    // record_count: an unmeasured generation publishes
                    // NULL (absent), not an undercount. `live_generation`
                    // names the generation the stamp came from, and the
                    // tick reads the stamp only while it is the published
                    // one; `live_as_of` is the scan's start, which is what
                    // the totals are as of.
                    h.createUpdate(
                        """
                        UPDATE hog_maintenance_summary SET generation = :generation, published_generation = :generation,
                            sampled_at = now(), sample = CAST(:sample AS jsonb), scan_state = NULL,
                            next_batch_at = now() + make_interval(secs => :refresh),
                            live_files = tot.files, live_bytes = tot.bytes,
                            live_rows = CASE WHEN :measured THEN tot.rows END,
                            live_generation = :generation, live_as_of = :startedAt,
                            published_snapshot = :snapshot,
                            -- STAMPED AT PUBLISH, from the scan's own
                            -- flag, so `measures_generation` names the
                            -- last generation PUBLISHED with the V22
                            -- measures. A marker written when a
                            -- generation BEGINS names a generation
                            -- nobody can read yet and disagrees with
                            -- published_generation for the whole scan.
                            -- Left untouched when the scan does not
                            -- measure (a checkpoint resumed from a
                            -- pre-V22 build), which is what keeps the
                            -- deploy straddle unreported.
                            measures_generation =
                                CASE WHEN :measured THEN :generation ELSE measures_generation END
                        FROM (
                            SELECT COALESCE(SUM(file_count), 0) AS files, COALESCE(SUM(total_bytes), 0) AS bytes,
                                   COALESCE(SUM(record_count), 0) AS rows
                            FROM hog_maintenance_summary_tier
                            WHERE catalog_id = :id AND generation = :generation
                        ) tot
                        WHERE catalog_id = :id
                        """,
                    ).bind("generation", generation).bind("sample", json.writeValueAsString(sample))
                        .bind("measured", scan.measures).bind("snapshot", scan.snapshot)
                        .bind("startedAt", scan.startedAt.atOffset(java.time.ZoneOffset.UTC))
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
            target, minInputFiles, maxInputFiles, measures = true,
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
                            rs.getLong("record_count"),
                            // A pre-V22 row resumed mid-scan reads back
                            // 0 / NULL: the ADD COLUMN defaults ARE the
                            // backward-compatible start, so a generation
                            // straddling the deploy accumulates from
                            // there rather than losing its checkpoint.
                            rs.getObject("newest_begin_snapshot")?.let { (it as Number).toLong() },
                        )
                }
                .list().toMap().toMutableMap()
        for ((key, quota, f) in keyed) {
            val pool = pools.getOrPut(key) { Pool(f.table, f.spec, f.values, quota, maxOf(1, quota), 0) }
            pool.files++
            pool.bytes = satAdd(pool.bytes, f.size)
            pool.rows = satAdd(pool.rows, f.rows)
            pool.newest = maxOf(pool.newest ?: f.begin, f.begin)
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
                 selected, pending_max_bytes, file_count, small_count, total_bytes, small_bytes, dv_count,
                 record_count, newest_begin_snapshot)
            VALUES (:id, :generation, :key, :table, :spec,
                    CASE WHEN :vals::jsonb = 'null'::jsonb THEN NULL
                         ELSE ARRAY(SELECT jsonb_array_elements_text(:vals::jsonb)) END,
                    :quota, :remaining, :pending,
                    :selected, :pendingMax, :files, :small, :bytes, :smallBytes, :dvs,
                    :rows, :newest)
            ON CONFLICT (catalog_id, generation, bucket_key) DO UPDATE
            SET remaining = excluded.remaining, pending = excluded.pending, selected = excluded.selected,
                pending_max_bytes = excluded.pending_max_bytes,
                file_count = excluded.file_count, small_count = excluded.small_count, total_bytes = excluded.total_bytes,
                small_bytes = excluded.small_bytes, dv_count = excluded.dv_count,
                record_count = excluded.record_count,
                newest_begin_snapshot = excluded.newest_begin_snapshot
            """,
            )
        for ((key, pool) in pools) {
            batch.bind("id", catalogId).bind("generation", generation).bind("key", key).bind("quota", pool.quota)
                .bind("table", pool.table).bindBySqlType("spec", pool.spec, java.sql.Types.BIGINT)
                .bind("vals", json.writeValueAsString(pool.values))
                .bind("remaining", pool.remaining).bind("pending", pool.pending).bind("selected", pool.selected)
                .bind("pendingMax", pool.pendingMax)
                .bind("files", pool.files).bind("small", pool.small).bind("bytes", pool.bytes)
                .bind("smallBytes", pool.smallBytes).bind("dvs", pool.dvs)
                .bind("rows", pool.rows)
                .bindBySqlType("newest", pool.newest, java.sql.Types.BIGINT).add()
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
                val sample = sampleFrom(rs.getString("sample"))
                sample?.let {
                    rs.getLong("catalog_id") to
                        Published(
                            rs.getObject("sampled_at", java.time.OffsetDateTime::class.java).toInstant(),
                            it,
                        )
                }
            }.list().filterNotNull().toMap()
        }

        /**
         * Decode a stored `sample` payload, or null when this build
         * cannot — the ONE parser, so a second reader cannot get the
         * lenient-vs-strict decision wrong.
         *
         * `internal` because `TierTotalsRepo`'s per-table read fetches
         * the summary row in the SAME statement as the tier sums (one
         * round trip on the hottest endpoint) and so has the JSON text
         * in hand rather than a `Published`. The leniency is [read]'s,
         * for [read]'s reason: a sample a newer replica published in a
         * shape this build does not know must degrade the numbers to
         * "not sampled", never 500 a request path.
         *
         * UNKNOWN PROPERTIES ARE IGNORED, which narrows "a shape this
         * build does not know" to a missing or mistyped field. [Sample]
         * is additive counters and stamps: a field a newer build adds
         * carries no meaning this build's readers depend on, so dropping
         * the WHOLE sample over it — every status page, head table GET
         * and partition listing for the catalog absent until this build
         * republishes — was all cost. That matters most for exactly the
         * catalogs that re-anchor on every generation (millpond's,
         * millrace's), which [Sample.reanchors]' NON_DEFAULT does not
         * protect. It protects rollbacks TO this build and later; a
         * rollback past it still drops a re-anchored sample, because the
         * older parser is the strict one.
         *
         * [Scan] stays strict, deliberately: its fields are a CURSOR, and
         * resuming a cursor whose shape this build only partly
         * understands is how the row-id-to-size keyset change would have
         * skipped or repeated rows.
         */
        internal fun sampleFrom(text: String?): Sample? =
            text?.let { runCatching { lenientSample.readValue<Sample>(it) }.getOrNull() }

        /**
         * Lenient about EXTRA keys only. A MISSING required field must
         * still drop the sample: Jackson's default would read an absent
         * `minInputFiles` as 0 and serve the sample as one measured under
         * a policy nobody runs, so `FAIL_ON_NULL_FOR_PRIMITIVES` is on.
         * Fields with a Kotlin default (`reanchors`) take the default when
         * absent, which is what lets every older sample keep parsing.
         */
        private val lenientSample =
            json.readerFor(Sample::class.java)
                .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .with(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
    }
}
