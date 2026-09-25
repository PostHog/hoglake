package com.posthog.hoglake.observability

import com.posthog.hoglake.model.VerifyReport
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.MultiGauge
import io.micrometer.core.instrument.Tags
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

/** Instance-wide live-data totals, refreshed by the metrics sampler. */
data class InstanceTotals(val totalRows: Long, val totalSizeBytes: Long)

/**
 * `hoglake_verify_violations{catalog, check}` — the violation count of
 * each check of the last verify SWEEP of each catalog, 0 on a pass, so
 * an alert keys on `> 0` and a healthy catalog is a published zero
 * rather than an absent series.
 *
 * It lives beside the other per-catalog gauges and is a [MultiGauge]
 * like them, refreshed with `overwrite = true`: one sweep replaces the
 * whole row set, so a catalog that has been deleted (or that the sweep
 * could no longer read) stops emitting instead of freezing its last
 * value forever. That is the only way a per-catalog series retires, and
 * it is why this is not a map of individually registered gauges.
 *
 * It is NOT part of [CatalogMetrics]'s sample: verify is not samplable.
 * Its answer costs eleven aggregate queries in a REPEATABLE READ
 * transaction, which is an hourly sweep's worth of work rather than a
 * 15-second sampler's, so the value is PUSHED by
 * `VerifyService.runOnceAllCatalogs` and stands until the next sweep.
 *
 * LOOP RUNS ONLY. A manual `POST /maintenance/verify` publishes
 * nothing. The trigger works on every replica, including the ones whose
 * loop is off (the API workload runs `HOGLAKE_VERIFY_INTERVAL_MS=0`),
 * and one manual run there would mint an alerting series that nothing
 * ever refreshes — a stale nonzero that pages forever, or a stale zero
 * that says "healthy" about a catalog nobody is checking. Both are
 * worse than no series.
 *
 * A catalog whose sweep THREW is absent from the row set rather than
 * carrying a stale value: the sweep has no answer for it, and
 * `hoglake_background_loop_failures_total` plus the ledger's absence of
 * a run row are what say so.
 */
object VerifyGauges {
    /**
     * The MultiGauge, and the registry it belongs to. Held as a pair
     * because `Metrics.bind` can be called again (tests bind a fresh
     * registry per case) and a MultiGauge registered against the old one
     * would silently publish into a registry nothing scrapes.
     */
    @Volatile
    private var bound: Pair<MeterRegistry, MultiGauge>? = null

    private fun gauge(): MultiGauge? {
        val registry = Metrics.boundRegistry ?: return null
        bound?.let { (bound, gauge) -> if (bound === registry) return gauge }
        val gauge =
            MultiGauge.builder("hoglake_verify_violations")
                .description("Violations found by the last verify sweep, per catalog and check (0 = pass)")
                .register(registry)
        bound = registry to gauge
        return gauge
    }

    /**
     * Publish one sweep's whole picture: every (catalog, check) it could
     * report on, and nothing else.
     */
    fun publish(reports: List<Pair<String, VerifyReport>>) {
        val gauge = gauge() ?: return
        gauge.register(
            reports.flatMap { (catalog, report) ->
                report.checks.map { check ->
                    MultiGauge.Row.of(Tags.of("catalog", catalog, "check", check.check), check.violations)
                }
            },
            true,
        )
    }

    /** Forget the registry binding (tests); never called in production. */
    fun clear() {
        bound = null
    }
}

/**
 * One catalog's live totals, as of the last metrics sample.
 *
 * Retained rather than recomputed: the sampler already produces these
 * for the Prometheus gauges, so serving them costs nothing extra. A SUM
 * over live data files per catalog on every listing request is the
 * O(live files) load issue #8 exists to remove, on the same instance
 * that serves the commit tail.
 *
 * The consequence is that these are up to one sample interval old, and
 * ABSENT before the first sample. Callers must render that absence
 * rather than substituting zero — an empty catalog and an unsampled one
 * are different facts.
 */
data class CatalogTotals(
    val tableCount: Long,
    val liveRows: Long,
    val liveBytes: Long,
    /**
     * Commit time of the oldest RETAINED snapshot — MIN(snapshot_time),
     * not the stored expiry floor (which is null until expiry first
     * advances it). Null only when the catalog has no snapshot at all.
     * The listing sends the instant and lets the client render the age,
     * so "3 days ago" stays live without the sampler re-running.
     */
    val oldestSnapshotTime: Instant?,
)

/**
 * Catalog-health gauges (README.md §8 — the catalog reports on itself,
 * retiring the metrics-cron layer). A lightweight periodic sampler
 * refreshes per-catalog MultiGauges; each sample is ONE batched query
 * over all catalogs (scalar subselects, no per-gauge round trips) plus
 * one grouped query for consumer offsets.
 *
 * Notes on semantics:
 *  - hoglake_snapshot_count is the head - earliest + 1 approximation
 *    (retained ids are dense between floor and head by construction;
 *    cheap, no count(*) over hog_snapshot).
 *  - hoglake_head_age_seconds is now() - head snapshot_time, both read
 *    on the Postgres side so app/db clock skew cannot bend it.
 *  - hoglake_removal_queue_depth counts UNDRAINED entries only: drained
 *    rows are the soft-deleted cleanup ledger, not pending work.
 *  - hoglake_missing_field_id_files counts LIVE flagged files — the
 *    same population the rename guard refuses on.
 *  - EVERY hog_data_file gauge joins hog_table and excludes dropped
 *    tables. Since #193 a drop touches no file row, so `end_snapshot IS
 *    NULL` is no longer the whole of "live" — the rows of a dropped
 *    table stay open until the retirement sweep deletes them, which on
 *    a catalog with no retention is never. The five of them are
 *    computed in ONE grouped pass (CatalogMetrics.SAMPLE_SQL), not five
 *    correlated subqueries per catalog.
 *  - hoglake_consumer_lag_snapshots{consumer} is head - min committed
 *    snapshot across the consumer's tables (its worst table). To cap
 *    cardinality, per-consumer series are only emitted while a catalog
 *    has <= MAX_CONSUMER_SERIES consumers; hoglake_consumer_lag_max is
 *    ALWAYS emitted per catalog with offsets, so the alerting family
 *    never flaps when a catalog crosses the cap. Offsets are joined to
 *    hog_table (ExpiryService's floor query, mirrored) so garbage or
 *    expired uuids can never distort the lag series, head + offsets are
 *    read in one transaction, and lag is clamped at >= 0.
 *  - hoglake_metrics_last_sample_epoch (global) is the wall-clock time
 *    of the last successful sample — staleness of every other gauge is
 *    visible; hoglake_metrics_sample_errors_total counts failed samples.
 */
class CatalogMetrics(private val jdbi: Jdbi, private val registry: MeterRegistry) {
    /** Null until the first successful sample (boot runs one immediately). */
    @Volatile
    var latestTotals: InstanceTotals? = null
        private set

    /**
     * Per-catalog live totals from the last sample, keyed by catalog
     * name. Empty until the first sample; a catalog created since then is
     * absent rather than zero.
     */
    @Volatile
    var latestByCatalog: Map<String, CatalogTotals> = emptyMap()
        private set

    private fun multiGauge(
        name: String,
        description: String,
    ): MultiGauge = MultiGauge.builder(name).description(description).register(registry)

    private val headSnapshotId =
        multiGauge("hoglake_head_snapshot_id", "Catalog head snapshot id")
    private val earliestSnapshotId =
        multiGauge("hoglake_earliest_snapshot_id", "Earliest retained snapshot id (expiry floor)")
    private val snapshotCount =
        multiGauge("hoglake_snapshot_count", "Retained snapshots (head - earliest + 1)")
    private val headAgeSeconds =
        multiGauge("hoglake_head_age_seconds", "Seconds since the head snapshot was committed")
    private val removalQueueDepth =
        multiGauge("hoglake_removal_queue_depth", "hog_file_removal entries awaiting cleanup")
    private val statsPendingFiles =
        multiGauge(
            "hoglake_stats_pending_files",
            "Data files with stats_state = 'pending' on a live table — the hydrator's actual queue",
        )
    private val statsFailedFiles =
        multiGauge(
            "hoglake_stats_failed_files",
            "Data files with stats_state = 'failed' on a live table (hydration failed loudly; B1)",
        )
    private val missingFieldIdFiles =
        multiGauge(
            "hoglake_missing_field_id_files",
            "Live data files whose parquet schema lacks field ids (rename-blocking); " +
                "dropped tables excluded",
        )
    private val liveRows =
        multiGauge(
            "hoglake_live_rows",
            "Live registered rows per catalog (gross of DV masking; dropped tables excluded)",
        )
    private val liveBytes =
        multiGauge("hoglake_live_bytes", "Live data-file bytes per catalog (dropped tables excluded)")
    private val tableCount =
        multiGauge("hoglake_table_count", "Live (non-dropped) tables")
    private val consumerLag =
        multiGauge("hoglake_consumer_lag_snapshots", "head - min committed snapshot per consumer")
    private val consumerLagMax =
        multiGauge(
            "hoglake_consumer_lag_max",
            "Worst consumer lag per catalog; always emitted (per-consumer series stop past " +
                "$MAX_CONSUMER_SERIES consumers, this never does)",
        )

    /** Epoch seconds of the last successful sample; 0 = never sampled. */
    private val lastSampleEpoch = AtomicLong(0)

    init {
        Gauge.builder("hoglake_metrics_last_sample_epoch", lastSampleEpoch) { it.get().toDouble() }
            .description("Epoch seconds of the last successful catalog-metrics sample (0 = never)")
            .register(registry)
    }

    private val sampleErrors: Counter =
        Counter.builder("hoglake_metrics_sample_errors_total")
            .description("Catalog-metrics samples that failed with an exception")
            .register(registry)

    private data class CatalogRow(
        val name: String,
        val head: Long,
        val earliest: Long,
        val headAgeSeconds: Double?,
        val removalDepth: Long,
        val statsPending: Long,
        val statsFailed: Long,
        val missingFieldIds: Long,
        val tables: Long,
        val liveRows: Long,
        val liveBytes: Long,
        val oldestSnapshotTime: Instant?,
    )

    /** One sample: refresh every gauge from the catalog. Safe to call concurrently with traffic. */
    fun sampleOnce() {
        val (rows, offsets) =
            try {
                readSample()
            } catch (t: Throwable) {
                sampleErrors.increment()
                throw t
            }

        fun rowsOf(value: (CatalogRow) -> Number) =
            rows.map { MultiGauge.Row.of(Tags.of("catalog", it.name), value(it)) }

        headSnapshotId.register(rowsOf { it.head }, true)
        earliestSnapshotId.register(rowsOf { it.earliest }, true)
        snapshotCount.register(rowsOf { it.head - it.earliest + 1 }, true)
        headAgeSeconds.register(
            rows.mapNotNull { r ->
                r.headAgeSeconds?.let { MultiGauge.Row.of(Tags.of("catalog", r.name), it) }
            },
            true,
        )
        removalQueueDepth.register(rowsOf { it.removalDepth }, true)
        statsPendingFiles.register(rowsOf { it.statsPending }, true)
        statsFailedFiles.register(rowsOf { it.statsFailed }, true)
        missingFieldIdFiles.register(rowsOf { it.missingFieldIds }, true)
        tableCount.register(rowsOf { it.tables }, true)
        liveRows.register(rowsOf { it.liveRows }, true)
        liveBytes.register(rowsOf { it.liveBytes }, true)

        // Instance-wide totals for /v1/info: served from this sample,
        // never computed per call (a manifest sum per request would tax
        // the same RDS that serves the commit tail at fleet scale).
        latestTotals = InstanceTotals(rows.sumOf { it.liveRows }, rows.sumOf { it.liveBytes })
        // The same rows, kept per catalog for the catalogs listing. The
        // map is replaced wholesale so a reader never sees a half-updated
        // mixture of two samples.
        latestByCatalog =
            rows.associate {
                it.name to
                    CatalogTotals(it.tables, it.liveRows, it.liveBytes, it.oldestSnapshotTime)
            }

        val headByCatalog = rows.associate { it.name to it.head }
        val byCatalog = offsets.groupBy { it.first }
        val lagRows = mutableListOf<MultiGauge.Row<Number>>()
        val lagMaxRows = mutableListOf<MultiGauge.Row<Number>>()
        for ((catalog, entries) in byCatalog) {
            val head = headByCatalog[catalog] ?: continue

            // Head and offsets come from one transaction, so lag cannot go
            // negative from a racing commit; the clamp is belt-and-braces.
            fun lag(committed: Long) = (head - committed).coerceAtLeast(0)
            if (entries.size <= MAX_CONSUMER_SERIES) {
                entries.mapTo(lagRows) { (_, consumer, committed) ->
                    MultiGauge.Row.of(Tags.of("catalog", catalog, "consumer", consumer), lag(committed))
                }
            }
            // lag_max is emitted unconditionally so the series never flaps
            // as a catalog crosses the per-consumer cardinality cap.
            lagMaxRows += MultiGauge.Row.of(Tags.of("catalog", catalog), entries.maxOf { lag(it.third) })
        }
        consumerLag.register(lagRows, true)
        consumerLagMax.register(lagMaxRows, true)
        lastSampleEpoch.set(System.currentTimeMillis() / 1000)
    }

    /** The two sample queries, in ONE transaction: head and offsets are a consistent pair. */
    private fun readSample(): Pair<List<CatalogRow>, List<Triple<String, String, Long>>> =
        jdbi.inTransactionUnchecked { h ->
            val rows =
                h.createQuery(SAMPLE_SQL)
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
                            statsFailed = rs.getLong("stats_failed"),
                            missingFieldIds = rs.getLong("missing_field_ids"),
                            tables = rs.getLong("table_count"),
                            liveRows = rs.getLong("live_rows"),
                            liveBytes = rs.getLong("live_bytes"),
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
                h.createQuery(
                    """
                SELECT c.name AS catalog, o.consumer_id,
                       min(o.committed_snapshot) AS committed
                  FROM hog_consumer_offset o
                  JOIN hog_catalog c ON c.catalog_id = o.catalog_id
                  JOIN hog_table t
                    ON t.catalog_id = o.catalog_id AND t.table_uuid = o.table_uuid
                 GROUP BY c.name, o.consumer_id
                """,
                )
                    .map { rs, _ ->
                        Triple(rs.getString("catalog"), rs.getString("consumer_id"), rs.getLong("committed"))
                    }
                    .list()
            rows to offsets
        }

    companion object {
        /** Per-catalog cap on hoglake_consumer_lag_snapshots series. */
        const val MAX_CONSUMER_SERIES = 100

        /**
         * The per-catalog sample, `internal` so the plan test EXPLAINs
         * the SQL PRODUCTION runs rather than a lookalike.
         *
         * ONE PASS OVER THE MANIFEST, not five. Every 15 seconds, for
         * every catalog, this used to issue five CORRELATED subqueries
         * over `hog_data_file` — stats_pending, stats_failed,
         * missing_field_ids, live_rows, live_bytes — each of which the
         * planner runs once per catalog row. At two catalogs and a 5M-row
         * manifest that was measured at 1.6M buffers and 2.2 s per
         * sample, i.e. ten full scans of the manifest a minute against
         * the database that also serves the commit tail, and it scaled
         * with catalogs x subqueries. The `files` CTE reads the manifest
         * ONCE and splits it with aggregate FILTERs: measured 213k
         * buffers and 0.53 s on the same fixture.
         *
         * THE JOIN TO hog_table IS THE POINT, not a detail. Since #193 a
         * dropped table's file rows are still `end_snapshot IS NULL` —
         * the drop touches none of them, and the retirement sweep
         * deletes them later — so `end_snapshot IS NULL` alone is no
         * longer "live". Without this join `hoglake_live_rows` and
         * `hoglake_live_bytes` would keep counting a dropped 3M-row
         * table's data as live data for as long as its rows survived,
         * which on a retention-NULL catalog is forever. This is the
         * invariant's third carve-out in practice: a gauge counts rows
         * at NO snapshot, so it filters on `hog_table` instead of
         * resolving visibility at one.
         *
         * `stats_pending` and `stats_failed` carry the same filter for
         * a second-order reason: the HYDRATOR no longer claims a
         * dropped table's pending files, so counting them would publish
         * a backlog nothing is draining — an alert that can never clear
         * and a number no operator can act on.
         *
         * `LEFT JOIN`, and the COALESCEs, because a catalog with no
         * file rows at all must still publish zeros. An INNER join
         * would make an empty catalog's whole gauge row vanish, and a
         * MultiGauge refreshed with `overwrite = true` retires a series
         * that stops appearing — an empty catalog would look deleted.
         *
         * The three subqueries that remain are over other relations
         * (hog_snapshot twice, hog_file_removal once, hog_table once)
         * and are index-driven per catalog; folding them in would trade
         * four cheap probes for extra grouped scans.
         */
        internal const val SAMPLE_SQL: String =
            """
            WITH files AS (
                SELECT f.catalog_id,
                       count(*) FILTER (
                           WHERE f.stats_state = 'pending'
                             AND t.dropped_snapshot IS NULL) AS stats_pending,
                       count(*) FILTER (
                           WHERE f.stats_state = 'failed'
                             AND t.dropped_snapshot IS NULL) AS stats_failed,
                       count(*) FILTER (
                           WHERE f.missing_field_ids
                             AND f.end_snapshot IS NULL
                             AND t.dropped_snapshot IS NULL) AS missing_field_ids,
                       COALESCE(SUM(f.record_count) FILTER (
                           WHERE f.end_snapshot IS NULL
                             AND t.dropped_snapshot IS NULL), 0) AS live_rows,
                       COALESCE(SUM(f.file_size_bytes) FILTER (
                           WHERE f.end_snapshot IS NULL
                             AND t.dropped_snapshot IS NULL), 0) AS live_bytes
                  FROM hog_data_file f
                  JOIN hog_table t
                    ON t.catalog_id = f.catalog_id AND t.table_id = f.table_id
                 GROUP BY f.catalog_id
            )
            SELECT c.name,
                   c.last_snapshot_id,
                   c.earliest_snapshot_id,
                   (SELECT extract(epoch FROM (now() - s.snapshot_time))
                      FROM hog_snapshot s
                     WHERE s.catalog_id = c.catalog_id
                       AND s.snapshot_id = c.last_snapshot_id) AS head_age_seconds,
                   (SELECT min(s.snapshot_time) FROM hog_snapshot s
                     WHERE s.catalog_id = c.catalog_id) AS oldest_snapshot_time,
                   (SELECT count(*) FROM hog_file_removal r
                     WHERE r.catalog_id = c.catalog_id
                       AND r.drained_at IS NULL) AS removal_depth,
                   (SELECT count(*) FROM hog_table t
                     WHERE t.catalog_id = c.catalog_id
                       AND t.dropped_snapshot IS NULL) AS table_count,
                   COALESCE(fl.stats_pending, 0) AS stats_pending,
                   COALESCE(fl.stats_failed, 0) AS stats_failed,
                   COALESCE(fl.missing_field_ids, 0) AS missing_field_ids,
                   COALESCE(fl.live_rows, 0) AS live_rows,
                   COALESCE(fl.live_bytes, 0) AS live_bytes
              FROM hog_catalog c
              LEFT JOIN files fl ON fl.catalog_id = c.catalog_id
            """
    }
}
