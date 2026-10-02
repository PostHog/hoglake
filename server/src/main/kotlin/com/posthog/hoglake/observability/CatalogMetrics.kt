package com.posthog.hoglake.observability

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
 * `hoglake_expiry_purge_remaining{catalog}` — file rows still eligible
 * below the floor after the catalog's last expiry sweep, SATURATING at
 * `ExpiryService.PURGE_REMAINING_CAP`.
 *
 * A GAUGE BECAUSE THE QUESTION IS "HOW FAR BEHIND", which a counter
 * cannot answer. `hoglake_expiry_purge_truncated_total` says the purge
 * stopped early; this says by how much, so an alert can separate "one
 * sweep spilled over" from "the backlog is growing". Published 0 by a
 * drained sweep rather than left absent, which is the rule every
 * per-catalog gauge here follows and for its reason: an alert keys on
 * `> 0`, so a healthy catalog has to be a published zero rather than a
 * missing series. (`VerifyGauges` was the other one that stated it;
 * #261 removed it with the verify subsystem.)
 *
 * SET BY THE SWEEP, not by the metrics sampler, because it is a property
 * of a run rather than of the catalog's state: the sweep has just
 * counted it (and only when it stopped early, since the count is itself
 * a capped scan), and recomputing it on the sampler's cadence would pay
 * that scan on catalogs that are perfectly healthy.
 */
object ExpiryGauges {
    /**
     * The MultiGauge and the registry it belongs to, held as a pair
     * because `Metrics.bind` can be called again (tests bind a fresh
     * registry per case) and a MultiGauge registered against the old one
     * would publish into a registry nothing scrapes.
     */
    @Volatile
    private var bound: Pair<MeterRegistry, MultiGauge>? = null

    private val latest = java.util.concurrent.ConcurrentHashMap<String, Double>()

    private fun gauge(): MultiGauge? {
        val registry = Metrics.boundRegistry ?: return null
        bound?.let { (boundRegistry, gauge) -> if (boundRegistry === registry) return gauge }
        val gauge =
            MultiGauge.builder("hoglake_expiry_purge_remaining")
                .description("File rows still eligible below the expiry floor after the last sweep (0 = drained)")
                .register(registry)
        bound = registry to gauge
        return gauge
    }

    /**
     * Record [remaining] for [catalog] and republish every catalog this
     * process has swept.
     *
     * EVERY CATALOG, not just this one: `MultiGauge.register` with
     * `overwrite = true` replaces the whole row set, so publishing one
     * row at a time would delete the others on every sweep and leave a
     * series that flickered between catalogs.
     */
    fun publish(
        catalog: String,
        remaining: Long?,
    ) {
        // NaN for an unknown, which is what Prometheus has for "no
        // value" — an alert on `> 0` does not fire on it, and neither
        // does it fire on a fabricated zero that would have claimed the
        // purge drained. `hoglake_expiry_purge_truncated_total` is the
        // series that still moves in that state, which is why it and not
        // this gauge is the one the README nominates for the alert.
        latest[catalog] = remaining?.toDouble() ?: Double.NaN
        val gauge = gauge() ?: return
        gauge.register(
            latest.entries.map { (name, value) -> MultiGauge.Row.of(Tags.of("catalog", name), value) },
            true,
        )
    }

    /**
     * Forget every catalog not in [names] — the set the sweep just
     * enumerated.
     *
     * Without it a DROPPED catalog's row is republished at its last value
     * forever: `latest` only ever gains keys, and the publish deliberately
     * writes every key it holds (a one-row `register(..., true)` would
     * delete the siblings). A gauge that keeps claiming a deleted
     * catalog is 40,000 rows behind is worse than no gauge, because it is
     * the kind of alert nobody can close.
     *
     * Called by the fleet sweep, which is the only caller that knows the
     * whole set; a single-catalog sweep adds a key and removes none.
     *
     * AN EMPTY SET IS AUTHORITATIVE, not a no-op, and the difference is
     * the last catalog on an instance. `runOnceAllCatalogs` enumerates
     * the catalogs and then calls this with what it found, so an empty
     * set means "this instance has none" — and an early return there
     * would republish the dropped one's backlog forever, which is
     * exactly the unclosable alert this function exists to prevent. The
     * caller that must not clear everything is a SINGLE-catalog sweep,
     * and that one does not call this at all.
     */
    fun retain(names: Set<String>) {
        val gone = latest.keys.filterNot { it in names }
        if (gone.isEmpty()) return
        gone.forEach { latest.remove(it) }
        gauge()?.register(
            latest.entries.map { (name, value) -> MultiGauge.Row.of(Tags.of("catalog", name), value) },
            true,
        )
    }

    /** Forget the registry binding and the values (tests). */
    fun clear() {
        bound = null
        latest.clear()
    }
}

/**
 * `hoglake_retirement_consecutive_timeouts{catalog, table}` —
 * CONSECUTIVE retirement runs that ended a table on its own statement
 * bound, for the one failure this loop cannot fix for itself (#263).
 *
 * WHY A STREAK AND NOT A COUNT OF TIMEOUTS. One cancelled batch is a
 * COLD batch: the per-row cost is flat in the batch size
 * (`RetirementCostIntegrationTest`), so a batch that crossed the bound
 * crossed it on cache misses, the cancelled statement warmed the pages
 * it touched, and the next run's identical batch is the cheap case.
 * `hoglake_retirement_timeouts_total` already counts those, and on a
 * healthy instance it is meant to tick occasionally and mean nothing. A
 * table that times out RUN AFTER RUN is the other thing: its per-row
 * cascade does not fit the bound at HOGLAKE_RETIREMENT_BATCH, no amount
 * of retrying will change that, and the fix is for an operator to lower
 * the knob ON THE INSTANCE THAT RETIRES THAT CATALOG — it is
 * process-wide rather than per catalog, so the lever is the maintenance
 * workload's env and it applies to everything that pod retires. The
 * streak is the only shape that
 * separates the two, which is why retirement no longer resizes itself
 * and publishes this instead.
 *
 * A GAUGE, AND A [MultiGauge], for the reason [ExpiryGauges] is one (and
 * the removed verify gauge was — #261): a COUNTER's series never
 * retires, so a table that recovered would keep its last nonzero
 * forever — an alert nobody can close, on a table that is fine. Here a committed batch drops the row, `register(..., true)`
 * replaces the whole set, and the series DISAPPEARS. An alert is
 * therefore `hoglake_retirement_consecutive_timeouts >= 3` and needs no
 * `for` clause to suppress one cold run (a one-run blip is `== 1`), and
 * absence means "no table on this catalog is stuck", which is the
 * steady state.
 *
 * CARDINALITY IS BOUNDED BY THE TABLES THAT ARE CURRENTLY FAILING, not
 * by the catalog's tables: a row exists only between a table's first
 * timeout and its next non-timeout outcome. A run's candidate set is
 * capped at `RetirementService.MAX_TABLES_PER_RUN` (1,000) and [retain]
 * prunes every key that is no longer eligible, so the worst case per
 * catalog is 1,000 series — a mass drop where every dropped table times
 * out — and the steady state is zero. There is deliberately no
 * `table_name` label: the id is stable across a rename and is what the
 * ledger row, the service's logs and `hog_table.retirement_eligible_at`
 * all key on.
 *
 * [retain] PRUNES WITHIN THE PREFIX IT WAS GIVEN, which is how a
 * truncated candidate read is still usable: the query is `ORDER BY
 * table_id LIMIT maxTablesPerRun`, so a full page proves nothing about
 * ids past its last one and everything below it. The caller passes that
 * last id as `prefixCeiling`; tables above it keep their series, tables
 * below it that the page did not return lose theirs.
 *
 * ONE WRITER, which is what makes the non-atomic read-modify-publish
 * below sound. Retirement has no HTTP trigger (deliberately — a trigger
 * reaches every replica), its loop is a single coroutine, and
 * `runOnceAllCatalogs` walks catalogs SERIALLY, so every mutation here
 * comes from one thread in one process. The map is a
 * `ConcurrentHashMap` for the iteration in [publish] and for the
 * scrape, NOT because two runs can interleave: if retirement ever gains
 * a second concurrent driver, the snapshot-then-register below has to
 * become one synchronized step or a published row set can lose a
 * concurrent streak.
 *
 * IN MEMORY, like every streak. A restart forgets it and the first run
 * after a restart republishes whatever still times out, which costs one
 * run of alert latency on a condition that has by then persisted for
 * however long the deploy took.
 */
object RetirementGauges {
    /**
     * The MultiGauge and the registry it belongs to, held as a pair for
     * [ExpiryGauges]' reason: `Metrics.bind` can be called again (tests
     * bind a fresh registry per case) and a MultiGauge registered
     * against the old one would publish into a registry nothing
     * scrapes.
     */
    @Volatile
    private var bound: Pair<MeterRegistry, MultiGauge>? = null

    /** (catalog, table id) -> consecutive runs that timed out on it. */
    private val streaks = java.util.concurrent.ConcurrentHashMap<Pair<String, Long>, Long>()

    private fun gauge(): MultiGauge? {
        val registry = Metrics.boundRegistry ?: return null
        bound?.let { (boundRegistry, gauge) -> if (boundRegistry === registry) return gauge }
        val gauge =
            MultiGauge.builder("hoglake_retirement_consecutive_timeouts")
                .description(
                    "Consecutive retirement runs that ended a dropped table on its statement " +
                        "bound (absent = none; a standing value means HOGLAKE_RETIREMENT_BATCH " +
                        "is too large for that table, and the knob is process-wide: lower it on " +
                        "the instance that retires the catalog)",
                )
                .register(registry)
        bound = registry to gauge
        return gauge
    }

    /**
     * Record that [tableId] on [catalog] ended a run on its statement
     * bound, and return the streak INCLUDING this run — the number the
     * caller logs, so the log and the series cannot disagree.
     */
    fun timedOut(
        catalog: String,
        tableId: Long,
    ): Long {
        val streak = streaks.merge(catalog to tableId, 1L) { a, b -> a + b }!!
        publish()
        return streak
    }

    /**
     * Record that [tableId] reached a NON-TIMEOUT outcome — a committed
     * batch, a drain (including the probe that finds the table already
     * empty), a stuck table, a floor that no longer covers it: its
     * streak is over and its series goes away.
     *
     * Any of those, not only progress. The gauge's claim is "this table
     * keeps failing on its statement bound", and a table that failed
     * some other way this run is not making that claim true; the other
     * faults have their own counters (`skipped_tables`).
     *
     * A no-op, and NOT a republish, when there was no streak — which is
     * the overwhelmingly common case (every table of every healthy run),
     * and `register(..., true)` walks the whole row set.
     */
    fun recovered(
        catalog: String,
        tableId: Long,
    ) {
        if (streaks.remove(catalog to tableId) != null) publish()
    }

    /**
     * Forget every table of [catalog] outside [tableIds] — the eligible
     * set the run just read — WITHIN THE PREFIX THAT SET COVERS.
     *
     * A table that drained, or that stopped being eligible, cannot still
     * be timing out; without this its row would be republished at its
     * last value forever, which is `ExpiryGauges.retain`'s unclosable
     * alert. AN EMPTY SET IS AUTHORITATIVE for the same reason it is
     * there: a catalog with nothing eligible has nothing stuck. Only
     * [catalog]'s keys are touched, because that is the only catalog
     * this caller enumerated.
     *
     * [prefixCeiling] IS WHAT MAKES A TRUNCATED READ USABLE. The
     * candidate query is `ORDER BY table_id LIMIT maxTablesPerRun`, so a
     * FULL page is a prefix: it proves nothing about ids past its last
     * one, but it proves everything below it — an id `<= prefixCeiling`
     * that the page does not contain was offered to that ORDER BY and
     * not returned, so it is no longer eligible. Pass the page's last
     * table id on a full page and NULL on a short one (where the set is
     * the whole eligible set and every tracked id is in scope).
     *
     * Skipping the prune altogether on a full page — the shape this
     * replaces — is the bug Copilot found on #282: a drained table's
     * streak of 3 had nothing left that could ever clear it while the
     * catalog's pages stayed full, so the alert outlived the table.
     */
    fun retain(
        catalog: String,
        tableIds: Set<Long>,
        prefixCeiling: Long? = null,
    ) {
        val gone =
            streaks.keys.filter { (name, tableId) ->
                name == catalog &&
                    tableId !in tableIds &&
                    (prefixCeiling == null || tableId <= prefixCeiling)
            }
        if (gone.isEmpty()) return
        gone.forEach { streaks.remove(it) }
        publish()
    }

    /**
     * Forget every catalog outside [catalogs] — the set the FLEET sweep
     * just enumerated.
     *
     * [retain] can only prune TABLES of a catalog that still exists, so
     * a catalog DELETED while one of its tables was failing would keep
     * its row forever: nothing else enumerates catalogs, and a deleted
     * one never gets another run. `ExpiryGauges.retain(names)` exists
     * for the same hole, and an EMPTY set is authoritative here too — an
     * instance with no catalogs has nothing stuck, and an early return
     * on empty is what would republish the last deleted catalog's streak
     * forever.
     *
     * Called by `runOnceAllCatalogs` alone. A single-catalog run must
     * not call it: it knows one name and would retire every other
     * catalog's series on the spot.
     */
    fun retainCatalogs(catalogs: Set<String>) {
        val gone = streaks.keys.filterNot { it.first in catalogs }
        if (gone.isEmpty()) return
        gone.forEach { streaks.remove(it) }
        publish()
    }

    /**
     * Publish every streak this process holds. EVERY one, not just the
     * key that changed: `register(..., true)` replaces the row set, so a
     * one-row publish would delete the siblings and leave a series that
     * flickered between tables.
     *
     * The snapshot and the register are not one atomic step; see the ONE
     * WRITER paragraph above for why that is sound today and what has to
     * change if it stops being.
     */
    private fun publish() {
        val gauge = gauge() ?: return
        gauge.register(
            streaks.entries.map { (key, streak) ->
                MultiGauge.Row.of(Tags.of("catalog", key.first, "table", key.second.toString()), streak)
            },
            true,
        )
    }

    /** Forget the registry binding and the streaks (tests). */
    fun clear() {
        bound = null
        streaks.clear()
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
