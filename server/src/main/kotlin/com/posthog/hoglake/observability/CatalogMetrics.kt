package com.posthog.hoglake.observability

import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.persistence.DatabaseHealthRepo
import com.posthog.hoglake.persistence.TierTotalsRepo
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.MultiGauge
import io.micrometer.core.instrument.Tags
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

/**
 * Instance-wide live-data totals, refreshed by the metrics sampler:
 * the sum over every catalog whose maintenance summary has stamped
 * BOTH rows and bytes for its published generation, and how many
 * catalogs are left out of both sums (no stamp, or a generation that
 * did not measure rows). All or nothing per catalog, so the two sums
 * always cover the same set.
 */
data class InstanceTotals(val totalRows: Long, val totalSizeBytes: Long, val unsampledCatalogs: Int)

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
 * [retain] PRUNES WITHIN THE PREFIX IT WAS GIVEN AND RECONCILES WHAT IS
 * PAST IT, which is how a truncated candidate read is still usable: the
 * query is `ORDER BY table_id LIMIT maxTablesPerRun`, so a full page
 * proves everything below its last id and nothing above it. Tables
 * below the ceiling that the page did not return lose their series;
 * tables above it are re-checked for eligibility in one bounded
 * statement, because a table drained OUT OF BAND up there would
 * otherwise keep its streak forever.
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
     * [beyondCeilingStillEligible] CLOSES THE REST, and the two guards
     * are for two different mistakes, both found by review on #282:
     *
     *  - pruning on a full page alone retires a series for a table that
     *    is still eligible and still failing (the first version);
     *  - skipping the prune on a full page leaves a drained table's
     *    streak with nothing that can ever clear it (the second);
     *  - and the ceiling alone leaves a THIRD: a tracked table ABOVE the
     *    ceiling, drained out of band while low ids keep the page full,
     *    never enters the prefix and never reaches a batch outcome, so
     *    its streak is preserved on every run — a 3 alerting for work
     *    that no longer exists.
     *
     * So the ids past the ceiling are RECONCILED instead of assumed: the
     * callback is handed exactly those ids and returns the subset that
     * is still retirable, and everything it omits is pruned. It is
     * called ONLY when there are such ids (never on a short page, never
     * in the steady state where nothing is tracked), and it is one
     * bounded statement over the currently-failing set — see
     * `RetirementService.STILL_ELIGIBLE_SQL`. A callback that THROWS
     * leaves those streaks alone: "no information" is not "gone", and
     * the next run asks again.
     */
    fun retain(
        catalog: String,
        tableIds: Set<Long>,
        prefixCeiling: Long? = null,
        beyondCeilingStillEligible: ((Set<Long>) -> Set<Long>)? = null,
    ) {
        val mine = streaks.keys.filter { it.first == catalog }
        val beyond =
            if (prefixCeiling == null) {
                emptySet()
            } else {
                mine.map { it.second }.filter { it > prefixCeiling && it !in tableIds }.toSet()
            }
        val stillEligible =
            if (beyond.isEmpty() || beyondCeilingStillEligible == null) {
                beyond
            } else {
                runCatching { beyondCeilingStillEligible(beyond) }.getOrElse { beyond }
            }
        val gone =
            mine.filter { (_, tableId) ->
                tableId !in tableIds &&
                    (prefixCeiling == null || tableId <= prefixCeiling || tableId !in stillEligible)
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
 * `hoglake_index_bloat_bytes{table,index}` — the estimated bytes each
 * btree index on a hog_* table carries beyond a freshly built copy of
 * itself (`ReindexService.ESTIMATE_SQL`).
 *
 * A GAUGE because autovacuum never shrinks an index: after a large
 * retirement the excess arrives in one step and stays until a REINDEX,
 * so "how much, per index, right now" is the whole question. Published
 * ONLY on the pod running the reindex loop (`ReindexService.publishGauge`):
 * by the daily run (twice when it rebuilds, so the rebuilt index reads
 * its new size at once) and by every metrics-sampler tick there
 * (`App.metricsTick`). An API pod serving `GET /v1/database/health`
 * computes the same estimate but never publishes it, so the series has
 * one source and does not flap between pods.
 *
 * THE ROW SET IS REPLACED, never merged: the estimate enumerates every
 * index, so a dropped or renamed one leaves the series on the next
 * publish rather than reporting its last value forever. An index whose
 * estimate cannot be made publishes NaN — present, so a dashboard sees
 * the index, and never a fabricated zero an alert would read as healthy.
 */
object IndexBloatGauges {
    @Volatile
    private var bound: Pair<MeterRegistry, MultiGauge>? = null

    private fun gauge(): MultiGauge? {
        val registry = Metrics.boundRegistry ?: return null
        bound?.let { (boundRegistry, gauge) -> if (boundRegistry === registry) return gauge }
        val gauge =
            MultiGauge.builder("hoglake_index_bloat_bytes")
                .description(
                    "Estimated bytes a hog_* btree index carries beyond a freshly built copy (NaN = no estimate)",
                )
                .register(registry)
        bound = registry to gauge
        return gauge
    }

    /** Replace the published set with [estimates]. */
    fun publish(estimates: List<com.posthog.hoglake.model.IndexBloatEstimate>) {
        val gauge = gauge() ?: return
        gauge.register(
            estimates.map {
                MultiGauge.Row.of(
                    Tags.of("table", it.table, "index", it.index),
                    it.excessBytes?.toDouble() ?: Double.NaN,
                )
            },
            true,
        )
    }

    /** Forget the registry binding (tests). */
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
    /**
     * As of the maintenance summary's published generation; null while
     * the catalog has no stamped generation (SAMPLE_SQL's KDoc), and
     * rows alone null while the generation did not measure them.
     */
    val liveRows: Long?,
    val liveBytes: Long?,
    /**
     * Commit time of the oldest RETAINED snapshot: the first snapshot
     * at or above the expiry floor, by primary key (one index descent —
     * see `CatalogMetrics.SAMPLE_SQL`), not the stored
     * `earliest_snapshot_time` (which is null until expiry first
     * advances the floor). Null only when the catalog has no snapshot at
     * all.
     * The listing sends the instant and lets the client render the age,
     * so "3 days ago" stays live without the sampler re-running.
     */
    val oldestSnapshotTime: Instant?,
)

/**
 * Catalog-health gauges (README.md §8 — the catalog reports on itself,
 * retiring the metrics-cron layer). A lightweight periodic sampler
 * refreshes per-catalog MultiGauges in GROUPS: the core group is ONE
 * batched query over all catalogs (scalar subselects, no per-gauge round
 * trips) plus one grouped query for consumer offsets; the five extended
 * groups below are each their own statement.
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
 *    a catalog with no retention is never. The six of them are
 *    computed in ONE grouped pass (CatalogMetrics.SAMPLE_SQL), not six
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
 *  - hoglake_table_*{catalog,namespace,table} come from the maintenance
 *    sampler's PUBLISHED generation ([TABLE_SQL]), never from the
 *    manifest, and are capped per catalog at [maxTableSeries] tables,
 *    largest by files first, with hoglake_table_series_truncated{catalog}
 *    saying how many were left out — the consumer-lag cap's idiom, with
 *    the omission made visible rather than silent. `namespace` and
 *    `table` are separate labels, as on
 *    hoglake_blind_partitioned_appends_total.
 *  - hoglake_metrics_last_sample_epoch (global) is the wall-clock time
 *    of the last sample whose CORE group ("catalogs", SAMPLE_SQL plus
 *    the offsets read) succeeded, whatever the other groups did: the
 *    gigahog dashboard and its alert read it as "the sampler is alive",
 *    and a failing ledger or relation query must not make it look dead.
 *    hoglake_metrics_sample_errors_total counts samples in which at
 *    least one group failed; hoglake_metrics_group_failures_total{group}
 *    says which, and is pre-registered at zero for the closed set of
 *    [GROUPS] so a first failure is a 0 -> 1 step `increase()` can see.
 *
 * GROUPS FAIL INDEPENDENTLY. The core per-catalog read, the per-table
 * summary read, the deletion-vector totals, the snapshot/retirement
 * lifecycle read, the relation sizes and the maintenance ledger are each
 * their own statement in their own try. One that throws leaves ITS
 * gauges at their last published values and does not stop the others;
 * the sample then counts ONE `hoglake_metrics_sample_errors_total` (that
 * counter's unit is a sample, as its description has always said) and
 * one `hoglake_metrics_group_failures_total{group}` per failed group,
 * logs each failed group's NAME, and rethrows the first failure (the
 * others attached as suppressed) so `BackgroundLoops` counts the
 * iteration and logs the stack traces once. The staleness epoch is NOT
 * held back by a failed extended group: it advances as soon as the core
 * group has published, because its readers treat it as liveness, and
 * the per-group counter is the signal for the rest.
 *
 * WHERE THE EXTENDED GROUPS RUN. The core group runs on every pod that
 * samples (`HOGLAKE_METRICS_INTERVAL_MS > 0`), because `/v1/info` and the
 * catalogs listing are served from it. The five extended groups run only
 * when [extendedGroups] is true, which `App` sets from
 * `HOGLAKE_MAINTENANCE_SUMMARY_INTERVAL_MS > 0` — the maintenance
 * Deployment, which also produces the summary the tables group reads.
 * Otherwise every API replica would publish the same instance-wide
 * series, and a `sum by (table)` over them would multiply by the replica
 * count.
 */
class CatalogMetrics(
    private val jdbi: Jdbi,
    private val registry: MeterRegistry,
    /**
     * Per-catalog cap on the hoglake_table_* series. A constructor
     * argument so a test can set it to 1 and watch it truncate;
     * production uses [MAX_TABLE_SERIES].
     */
    private val maxTableSeries: Int = MAX_TABLE_SERIES,
    /**
     * Dropped tables probed per catalog by
     * hoglake_dropped_tables_pending_retirement; see
     * [DROPPED_TABLE_PROBE_CAP].
     */
    private val droppedTableProbeCap: Int = DROPPED_TABLE_PROBE_CAP,
    /** Ledger rows read per (catalog, task); see [MAINTENANCE_RUN_LOOKBACK]. */
    private val maintenanceRunLookback: Int = MAINTENANCE_RUN_LOOKBACK,
    /** Whether the five extended groups run on this pod; see the class KDoc. */
    private val extendedGroups: Boolean = true,
) {
    init {
        require(maxTableSeries > 0) { "maxTableSeries must be positive, was $maxTableSeries" }
        require(droppedTableProbeCap > 0) { "droppedTableProbeCap must be positive, was $droppedTableProbeCap" }
        require(maintenanceRunLookback > 0) { "maintenanceRunLookback must be positive, was $maintenanceRunLookback" }
    }

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
    private val liveTotalsSampled =
        multiGauge(
            "hoglake_live_totals_sampled",
            "1 when the catalog's maintenance summary has published a generation with live totals, " +
                "which hoglake_live_rows/_bytes/_files and the API totals are read from; 0 while it " +
                "has not, in which case those series are absent rather than zero",
        )
    private val liveTotalsAge =
        multiGauge(
            "hoglake_live_totals_age_seconds",
            "Seconds since the maintenance summary scan that hoglake_live_rows/_bytes/_files are as of " +
                "BEGAN (the data's age, not the publish's: a scan runs for hours on a large catalog); " +
                "absent while the catalog has none. Grows without bound if the maintenance sampler " +
                "stops, which is how a frozen total announces itself",
        )
    private val liveRows =
        multiGauge(
            "hoglake_live_rows",
            "Live registered rows per catalog (gross of DV masking; dropped tables excluded), as of " +
                "the maintenance summary's published generation; absent while the catalog has none " +
                "(hoglake_live_totals_sampled = 0) or the generation did not measure rows",
        )
    private val liveBytes =
        multiGauge(
            "hoglake_live_bytes",
            "Live data-file bytes per catalog (dropped tables excluded), as of the maintenance summary's " +
                "published generation; absent while the catalog has none (hoglake_live_totals_sampled = 0)",
        )
    private val liveFiles =
        multiGauge(
            "hoglake_live_files",
            "Live data files per catalog (dropped tables excluded), as of the maintenance summary's " +
                "published generation; absent while the catalog has none (hoglake_live_totals_sampled = 0)",
        )
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

    // --- per-table, from the maintenance sampler's published generation ---
    private val tableFiles =
        multiGauge(
            "hoglake_table_files",
            "Live data files per table, as of the maintenance sampler's published generation",
        )
    private val tableSmallFiles =
        multiGauge(
            "hoglake_table_small_files",
            "Live data files under the compaction target size per table (published generation)",
        )
    private val tableBytes =
        multiGauge("hoglake_table_bytes", "Live data-file bytes per table (published generation)")
    private val tableSmallBytes =
        multiGauge(
            "hoglake_table_small_bytes",
            "Bytes in live data files under the compaction target size per table (published generation)",
        )
    private val tableRows =
        multiGauge(
            "hoglake_table_rows",
            "Live registered rows per table, gross of DV masking (published generation; absent " +
                "while the generation's row measure is incomplete)",
        )
    private val tableDeleteFiles =
        multiGauge(
            "hoglake_table_delete_files",
            "Live data files carrying a deletion vector per table (published generation)",
        )
    private val tablePartitions =
        multiGauge(
            "hoglake_table_partitions",
            "Partitions holding at least one live data file per table (published generation)",
        )
    private val tableLargestPartitionFiles =
        multiGauge(
            "hoglake_table_largest_partition_files",
            "Live data files in the table's fullest partition (published generation)",
        )
    private val tableSeriesTruncated =
        multiGauge(
            "hoglake_table_series_truncated",
            "Live tables left out of the hoglake_table_* series by the per-catalog cap of " +
                "$maxTableSeries (0 = none; the largest tables by files are the ones kept)",
        )

    // --- deletion vectors, catalog lifecycle ---
    private val liveDeleteFiles =
        multiGauge(
            "hoglake_live_delete_files",
            "Live deletion-vector files per catalog (dropped tables excluded)",
        )
    private val liveDeleteBytes =
        multiGauge(
            "hoglake_live_delete_bytes",
            "Live deletion-vector bytes per catalog (dropped tables excluded)",
        )
    private val earliestSnapshotAgeSeconds =
        multiGauge(
            "hoglake_earliest_snapshot_age_seconds",
            "Seconds since the earliest retained snapshot was committed (how far back the catalog reads)",
        )
    private val droppedTablesPendingRetirement =
        multiGauge(
            "hoglake_dropped_tables_pending_retirement",
            "Dropped tables still holding live data-file rows, counted over the newest " +
                "$droppedTableProbeCap dropped tables only: a LOWER BOUND, and an older pending drop " +
                "is not counted",
        )

    // --- Postgres relations, instance-wide ---
    private val relationBytes =
        multiGauge(
            "hoglake_relation_bytes",
            "On-disk bytes of each hog_* relation by kind (heap, index, toast), as GET /v1/database/health reports",
        )
    private val relationLiveTuples =
        multiGauge("hoglake_relation_live_tuples", "Estimated live tuples per hog_* relation (pg_stat_user_tables)")
    private val relationDeadTuples =
        multiGauge("hoglake_relation_dead_tuples", "Estimated dead tuples per hog_* relation (pg_stat_user_tables)")

    // --- maintenance ledger ---
    private val maintenanceLastRunEpoch =
        multiGauge(
            "hoglake_maintenance_last_run_epoch",
            "Epoch seconds the latest maintenance run with this status finished. Read from the task's " +
                "last $maintenanceRunLookback ledger rows; when that window holds none, the last value " +
                "this process saw is republished (a restart forgets it, so absent = none seen since boot)",
        )
    private val maintenanceLastRunDuration =
        multiGauge(
            "hoglake_maintenance_last_run_duration_seconds",
            "Wall-clock duration of the latest recorded maintenance run per task",
        )

    /**
     * (catalog, task, status) -> the newest finish epoch this process has
     * seen. What keeps `status="ok"` alive through a failure streak longer
     * than the lookback window: a staleness alert keyed on
     * `time() - hoglake_maintenance_last_run_epoch{status="ok"}` must keep
     * RISING while a task fails, not go silent when the last success
     * scrolls out of the window. One writer (the sampler loop), so a plain
     * map replaced wholesale is enough.
     *
     * KEYED BY CATALOG NAME, as every series is. A catalog that no longer
     * exists is forgotten on the next sample; one deleted and re-created
     * under the same name within one sample interval inherits the old
     * epochs until its own runs replace them. Accepted: there is no
     * catalog delete API today, and the window shows the new catalog's
     * runs as soon as it has any.
     */
    @Volatile
    private var lastRunMemory: Map<Triple<String, String, String>, Double> = emptyMap()

    /** Epoch seconds of the last successful core sample; 0 = never sampled. */
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

    /**
     * `hoglake_metrics_group_failures_total{group}`, one counter per group
     * of the closed set [GROUPS], registered at zero up front so the first
     * failure is a step `rate()`/`increase()` can see rather than a series
     * that is born at 1.
     */
    private val groupFailures: Map<String, Counter> =
        GROUPS.associateWith { group ->
            Counter.builder("hoglake_metrics_group_failures_total")
                .description("Catalog-metrics sample groups that failed with an exception, by group")
                .tag("group", group)
                .register(registry)
        }

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
        /** Whether the maintenance summary has stamped this catalog's live totals (V27). */
        val liveSampled: Boolean,
        /** Seconds since the generation the totals are as of was published; null when unsampled. */
        val liveTotalsAgeSeconds: Double?,
        /** Null when unsampled, or when the published generation did not measure rows. */
        val liveRows: Long?,
        val liveBytes: Long?,
        val liveFiles: Long?,
        val oldestSnapshotTime: Instant?,
    )

    private data class TableRow(
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

    /**
     * One sample: refresh every gauge from the catalog. Safe to call
     * concurrently with traffic. Each group is attempted whatever the
     * others did; see the class KDoc.
     */
    fun sampleOnce() {
        val failures = mutableListOf<Pair<String, Throwable>>()

        fun group(
            name: String,
            block: () -> Unit,
        ) {
            try {
                block()
            } catch (t: Throwable) {
                groupFailures.getValue(name).increment()
                // An Error (OOM and friends) is not a failed query; count
                // the sample and stop rather than carry on regardless.
                if (t is Error) {
                    sampleErrors.increment()
                    throw t
                }
                failures += name to t
            }
        }

        group(GROUP_CATALOGS) {
            sampleCatalogs()
            // Liveness, not completeness: set as soon as the core group
            // has published, so a failing extended group below cannot
            // make the sampler look dead (see the class KDoc).
            lastSampleEpoch.set(System.currentTimeMillis() / 1000)
        }
        if (extendedGroups) {
            group(GROUP_TABLES) { sampleTables() }
            group(GROUP_DELETE_FILES) { sampleDeleteFiles() }
            group(GROUP_LIFECYCLE) { sampleLifecycle() }
            group(GROUP_RELATIONS) { sampleRelations() }
            group(GROUP_MAINTENANCE_RUNS) { sampleMaintenanceRuns() }
        }

        if (failures.isNotEmpty()) {
            sampleErrors.increment()
            // Names only: the rethrown exception carries the rest as
            // suppressed, and BackgroundLoops logs it with its trace.
            log.warn { "catalog metrics: failed groups ${failures.map { it.first }}" }
            val first = failures.first().second
            failures.drop(1).forEach { first.addSuppressed(it.second) }
            throw first
        }
    }

    private fun sampleCatalogs() {
        val (rows, offsets) = readSample()

        fun rowsOf(value: (CatalogRow) -> Number) =
            rows.map {
                MultiGauge.Row.of(
                    Tags.of("catalog", it.name),
                    value(it),
                )
            }

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

        // The live totals are ABSENT, not zero, on a catalog the
        // maintenance summary has not stamped (SAMPLE_SQL's KDoc):
        // `overwrite = true` retires the series, and the sampled flag —
        // published for every catalog — says why it is missing.
        fun liveRowsOf(value: (CatalogRow) -> Long?) =
            rows.mapNotNull { r -> value(r)?.let { MultiGauge.Row.of(Tags.of("catalog", r.name), it) } }
        liveTotalsSampled.register(rowsOf { if (it.liveSampled) 1 else 0 }, true)
        liveTotalsAge.register(
            rows.mapNotNull { r ->
                r.liveTotalsAgeSeconds?.let { MultiGauge.Row.of(Tags.of("catalog", r.name), it) }
            },
            true,
        )
        liveRows.register(liveRowsOf { it.liveRows }, true)
        liveBytes.register(liveRowsOf { it.liveBytes }, true)
        liveFiles.register(liveRowsOf { it.liveFiles }, true)

        // Instance-wide totals for /v1/info: served from this sample,
        // never computed per call (a manifest sum per request would tax
        // the same RDS that serves the commit tail at fleet scale). ALL
        // OR NOTHING per catalog: one with rows OR bytes missing (no
        // stamp, or a generation that did not measure rows) is left out
        // of BOTH sums and counted, so the two totals always cover the
        // same catalogs and the count says exactly what they omit.
        val covered = rows.filter { it.liveRows != null && it.liveBytes != null }
        latestTotals =
            InstanceTotals(
                totalRows = covered.sumOf { it.liveRows!! },
                totalSizeBytes = covered.sumOf { it.liveBytes!! },
                unsampledCatalogs = rows.size - covered.size,
            )
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
    }

    /**
     * The per-table series, read off the published summary generation
     * ([TABLE_SQL]) together with the set of catalogs that HAVE one
     * ([PUBLISHED_CATALOGS_SQL]), under ONE REPEATABLE READ READ ONLY
     * snapshot — `PartitionListingService.read`'s isolation. Under the
     * default READ COMMITTED each statement takes its own snapshot, so a
     * publish landing between the two could pair one generation's
     * truncation count with another's table rows; REPEATABLE READ makes
     * both statements see the same summary rows.
     */
    private fun sampleTables() {
        val (published, rows) =
            jdbi.inTransactionUnchecked { h ->
                // First statement of the transaction, or it does not apply.
                h.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                val names = h.createQuery(PUBLISHED_CATALOGS_SQL).mapTo(String::class.java).list()
                val tables =
                    h.createQuery(TABLE_SQL)
                        .bind("cap", maxTableSeries)
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

        fun rowsOf(value: (TableRow) -> Number) = rows.map { MultiGauge.Row.of(it.tags, value(it)) }

        tableFiles.register(rowsOf { it.files }, true)
        tableSmallFiles.register(rowsOf { it.smallFiles }, true)
        tableBytes.register(rowsOf { it.bytes }, true)
        tableSmallBytes.register(rowsOf { it.smallBytes }, true)
        // Rows only where the generation MEASURED them: a generation an
        // older replica began accumulates record_count from zero partway
        // through, and an undercount is worse than an absence — the rule
        // `CatalogService.getTable` applies to the same column.
        tableRows.register(rows.filter { it.measured }.map { MultiGauge.Row.of(it.tags, it.rows) }, true)
        tableDeleteFiles.register(rowsOf { it.deleteFiles }, true)
        tablePartitions.register(rowsOf { it.partitions }, true)
        tableLargestPartitionFiles.register(rowsOf { it.largestPartitionFiles }, true)
        // Published for EVERY catalog with a published generation, 0 when
        // nothing was cut, so `> 0` is an alert and absence means "not
        // sampled yet" rather than "fine".
        val coveredByCatalog = rows.groupBy { it.catalog }.mapValues { (_, r) -> r.first().covered }
        tableSeriesTruncated.register(
            published.map { name ->
                val covered = coveredByCatalog[name] ?: 0L
                MultiGauge.Row.of(
                    Tags.of("catalog", name),
                    (covered - maxTableSeries).coerceAtLeast(0),
                )
            },
            true,
        )
    }

    private fun sampleDeleteFiles() {
        val rows =
            jdbi.inTransactionUnchecked { h ->
                h.createQuery(DELETE_FILES_SQL)
                    .map { rs, _ -> Triple(rs.getString("name"), rs.getLong("files"), rs.getLong("bytes")) }
                    .list()
            }
        liveDeleteFiles.register(rows.map { MultiGauge.Row.of(Tags.of("catalog", it.first), it.second) }, true)
        liveDeleteBytes.register(rows.map { MultiGauge.Row.of(Tags.of("catalog", it.first), it.third) }, true)
    }

    private fun sampleLifecycle() {
        data class Lifecycle(val name: String, val earliestAge: Double?, val droppedPending: Long)
        val rows =
            jdbi.inTransactionUnchecked { h ->
                h.createQuery(LIFECYCLE_SQL)
                    .bind("probeCap", droppedTableProbeCap)
                    .map { rs, _ ->
                        Lifecycle(
                            rs.getString("name"),
                            rs.getObject("earliest_age_seconds")?.let { rs.getDouble("earliest_age_seconds") },
                            rs.getLong("dropped_pending"),
                        )
                    }
                    .list()
            }
        earliestSnapshotAgeSeconds.register(
            rows.mapNotNull { r -> r.earliestAge?.let { MultiGauge.Row.of(Tags.of("catalog", r.name), it) } },
            true,
        )
        droppedTablesPendingRetirement.register(
            rows.map { MultiGauge.Row.of(Tags.of("catalog", it.name), it.droppedPending) },
            true,
        )
    }

    /**
     * Relation sizes and tuple estimates through
     * [DatabaseHealthRepo.tables] — the SAME function
     * `GET /v1/database/health` calls, so the two surfaces cannot
     * disagree on what "heap bytes" means. Statistics views and the
     * catalog only; no `hog_*` row is read.
     */
    private fun sampleRelations() {
        val tables = jdbi.withHandleUnchecked { h -> DatabaseHealthRepo.tables(h) }
        relationBytes.register(
            tables.flatMap { t ->
                listOf(
                    MultiGauge.Row.of(Tags.of("relation", t.name, "kind", "heap"), t.tableBytes),
                    MultiGauge.Row.of(Tags.of("relation", t.name, "kind", "index"), t.indexBytes),
                    MultiGauge.Row.of(Tags.of("relation", t.name, "kind", "toast"), t.toastBytes),
                )
            },
            true,
        )
        relationLiveTuples.register(
            tables.map { MultiGauge.Row.of(Tags.of("relation", it.name), it.liveTuples) },
            true,
        )
        relationDeadTuples.register(
            tables.map { MultiGauge.Row.of(Tags.of("relation", it.name), it.deadTuples) },
            true,
        )
    }

    private fun sampleMaintenanceRuns() {
        data class LastRuns(
            val catalog: String,
            val task: String,
            val okAt: Double?,
            val failedAt: Double?,
            val duration: Double,
        )
        val (catalogs, rows) =
            jdbi.inTransactionUnchecked { h ->
                val names = h.createQuery("SELECT name FROM hog_catalog").mapTo(String::class.java).set()
                val runs =
                    h.createQuery(MAINTENANCE_RUNS_SQL)
                        .bindArray("tasks", String::class.java, MaintenanceTask.entries.map { it.wire })
                        .bind("lookback", maintenanceRunLookback)
                        .map { rs, _ ->
                            LastRuns(
                                rs.getString("catalog"),
                                rs.getString("task"),
                                rs.getObject("ok_at")?.let { rs.getDouble("ok_at") },
                                rs.getObject("failed_at")?.let { rs.getDouble("failed_at") },
                                rs.getDouble("last_duration"),
                            )
                        }
                        .list()
                names to runs
            }
        // Merge this window into the memory: a newer epoch wins, a status
        // the window does not hold keeps what was seen before. A catalog
        // that no longer exists is forgotten (ExpiryGauges.retain's
        // unclosable-series argument).
        val seen =
            rows.flatMap { r ->
                listOfNotNull(
                    r.okAt?.let { Triple(r.catalog, r.task, "ok") to it },
                    r.failedAt?.let { Triple(r.catalog, r.task, "failed") to it },
                )
            }
        val merged = lastRunMemory.filterKeys { it.first in catalogs }.toMutableMap()
        for ((key, epoch) in seen) merged.merge(key, epoch) { old, new -> maxOf(old, new) }
        lastRunMemory = merged
        maintenanceLastRunEpoch.register(
            merged.map { (key, epoch) ->
                MultiGauge.Row.of(Tags.of("catalog", key.first, "task", key.second, "status", key.third), epoch)
            },
            true,
        )
        maintenanceLastRunDuration.register(
            rows.map { MultiGauge.Row.of(Tags.of("catalog", it.catalog, "task", it.task), it.duration) },
            true,
        )
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
        private val log = KotlinLogging.logger {}

        const val GROUP_CATALOGS = "catalogs"
        const val GROUP_TABLES = "tables"
        const val GROUP_DELETE_FILES = "delete_files"
        const val GROUP_LIFECYCLE = "lifecycle"
        const val GROUP_RELATIONS = "relations"
        const val GROUP_MAINTENANCE_RUNS = "maintenance_runs"

        /** The closed set of sample groups, in the order a sample runs them. */
        val GROUPS: List<String> =
            listOf(
                GROUP_CATALOGS,
                GROUP_TABLES,
                GROUP_DELETE_FILES,
                GROUP_LIFECYCLE,
                GROUP_RELATIONS,
                GROUP_MAINTENANCE_RUNS,
            )

        /** Per-catalog cap on hoglake_consumer_lag_snapshots series. */
        const val MAX_CONSUMER_SERIES = 100

        /**
         * Per-catalog cap on the hoglake_table_* series. Tables per
         * catalog are tens on production; the cap is for the catalog that
         * is not (a 54,000-table namespace has existed), where nine
         * families x 54k tables would be ~500k series from one scrape.
         * Largest-by-files first, so the tables an operator watches for
         * small-file backlog are the ones kept.
         */
        const val MAX_TABLE_SERIES = 500

        /**
         * Dropped tables probed per catalog per sample by
         * hoglake_dropped_tables_pending_retirement, newest table id
         * first. `hog_table` rows are never deleted, so the dropped set
         * only grows; without a cap the probe count is every drop the
         * catalog ever had. The gauge is therefore a LOWER BOUND over the
         * newest N drops: once N newer drops exist, an older drop that
         * still has rows is not counted at all, and the gauge can read 0
         * with work pending (reproduced with 10,001 drops). Counting it
         * exactly needs an index on pending drops, which is a migration
         * this change does not take.
         */
        const val DROPPED_TABLE_PROBE_CAP = 10_000

        /**
         * Ledger rows read per (catalog, task) by the maintenance-run
         * gauges. `hog_maintenance_run_recent (catalog_id, task, run_id
         * DESC)` makes "the newest N rows" one ordered index range, but
         * `status` is not in the index, so "the newest FAILED row" on a
         * task that has not failed inside retention walks the task's
         * whole ledger — at the hydrator's per-sweep cadence that is
         * hundreds of thousands of rows per sample. A window bounds it; a
         * partial index on `status = 'failed'` would be the unbounded
         * alternative and needs a migration this change does not take.
         * A status that has scrolled out of the window is republished
         * from the process's memory (see `lastRunMemory`).
         */
        const val MAINTENANCE_RUN_LOOKBACK = 1_000

        /**
         * The per-catalog sample, `internal` so the plan test EXPLAINs
         * the SQL PRODUCTION runs rather than a lookalike.
         *
         * NO PASS OVER THE MANIFEST, AND NOTHING THAT GROWS WITH IT
         * (#269). This used to read every row of `hog_data_file` once
         * per sample — every 15 s, on every pod — because "sum the live
         * rows" had no index to answer it: at gigahog-prod-us's ~1.5 GiB
         * manifest that was 1.5 GiB of buffer traffic per tick against
         * the database that serves the commit tail. Each number now
         * comes from a structure bounded by something smaller than the
         * manifest, and by something that does not grow with it:
         *
         *  - `live_rows`, `live_bytes`, `live_files`: FOUR COLUMNS OF
         *    `hog_maintenance_summary` (V27), stamped by the maintenance
         *    sampler's publish from the generation it has just finished
         *    — the same tier rows the table GET totals and the
         *    partitions listing read, summed once per generation instead
         *    of once per tick per pod. One primary-key row per catalog.
         *    Rows only while the generation MEASURED them
         *    (`measures_generation = published_generation`, the rule
         *    every other tier reader applies): an unmeasured
         *    generation's record_count is an undercount, and absent
         *    beats wrong. The publish stamps NULL for one; the gate here
         *    covers a flag moved after the stamp.
         *    The totals are therefore AS OF THE PUBLISHED GENERATION —
         *    minutes behind head on prod-us, hours on a 10 PB instance —
         *    and the gauge descriptions say so. #292's KDoc chose
         *    exactness over this; the rule that overrides it is that
         *    nothing in the sampler may scan a whole table (#269).
         *  - A catalog whose PUBLISHED generation has no stamp — no
         *    generation yet (new, or a dev stack with no maintenance
         *    pod), or a generation published by a sampler that does not
         *    stamp (older than V27, in a rollout or after a rollback),
         *    which `live_generation = published_generation` catches the
         *    way `measures_generation` is caught — publishes NO live series and
         *    `null` on the wire (`live_sampled` says which), and is NOT
         *    read from its own manifest. The first version of this
         *    change did that, scoped by catalog; the 10 PB review
         *    killed it: the catalog that never publishes a generation
         *    is the one whose scan outruns its retention, i.e. the
         *    LARGEST catalog, and "scan its manifest every 15 s instead"
         *    is the regression this change exists to remove. Absence is
         *    the honest answer; `hoglake_live_totals_sampled{catalog}`
         *    makes it a visible one.
         *  - A table dropped AFTER the generation's scan snapshot may
         *    still have buckets in it (the drop touches no file row and
         *    leaves the generation alone — the production order), so its
         *    share is SUBTRACTED: `hog_table` by catalog, filtered to
         *    `dropped_snapshot > published_snapshot`, each dropped table
         *    summed over its buckets through
         *    `hog_maintenance_summary_tier_table`. A table dropped at or
         *    before the snapshot was skipped by the scan and has no
         *    buckets. The cost is the catalog's tables (the same range
         *    `table_count` reads) plus the buckets of its recent drops,
         *    never the whole generation.
         *  - `stats_pending`, `stats_failed`, `missing_field_ids`: one
         *    index-only range per catalog each, over the partial indexes
         *    that hold exactly that LIVE population —
         *    `hog_data_file_pending`, `hog_data_file_failed` and
         *    `hog_data_file_missing_field_ids` (all V26, each carrying
         *    `table_id`). Each is joined to `hog_table` per distinct
         *    table of the rare population, not per manifest row.
         *    `end_snapshot IS NULL` on all three for the reason the
         *    indexes carry it: the hydrator's claim skips ended rows, so
         *    an ended pending row is not a backlog anything drains.
         *
         * THE JOIN TO hog_table IS THE POINT, not a detail. Since #193 a
         * dropped table's file rows are still `end_snapshot IS NULL` —
         * the drop touches none of them, and the retirement sweep
         * deletes them later — so `end_snapshot IS NULL` alone is no
         * longer "live". The published generation carries the dropped
         * table's buckets for the same reason. Without the join
         * `hoglake_live_rows` and `hoglake_live_bytes` would keep
         * counting a dropped 3M-row table's data as live data for as
         * long as its rows survived, which on a retention-NULL catalog
         * is forever. `stats_pending` and `stats_failed` carry the same
         * filter for a second-order reason: the HYDRATOR no longer
         * claims a dropped table's pending files, so counting them would
         * publish a backlog nothing is draining.
         *
         * WHAT IS STILL O(POPULATION): a dropped table's pending rows
         * stay in `hog_data_file_pending` until retirement deletes them
         * (the drop ends nothing), and the pending count walks them —
         * index-only, with one memoized hog_table probe — every tick
         * until then. That is the rare population by construction
         * (pending is a transient state); a catalog with 10^7 pending
         * rows on a dropped table is a catalog whose retirement sweep is
         * off, and the gauge says so by its size.
         *
         * `LEFT JOIN`, and the COALESCEs on the counts, because a catalog
         * with no rows anywhere must still publish zeros for them: a
         * MultiGauge refreshed with `overwrite = true` retires a series
         * that stops appearing, and an empty catalog would look deleted.
         * The live totals are the deliberate exception: NULL means "not
         * sampled", and zero would be a lie about an unsampled catalog.
         *
         * `oldest_snapshot_time` is the first snapshot AT OR ABOVE THE
         * FLOOR by primary key — one descent of `hog_snapshot_pkey`,
         * `LIMIT 1`, <= 3 buffers per catalog on the plan test's 50,000
         * snapshots — and the same probe [LIFECYCLE_SQL] ages. Snapshot
         * ids are assigned in commit order under the commit lock, so the
         * first retained id is the earliest retained commit to within one
         * transaction's start skew; the floor bound is defence, since
         * expiry deletes below the floor in the transaction that advances
         * it.
         */
        internal const val SAMPLE_SQL: String =
            """
            SELECT c.name,
                   c.last_snapshot_id,
                   c.earliest_snapshot_id,
                   (SELECT extract(epoch FROM (now() - s.snapshot_time))
                      FROM hog_snapshot s
                     WHERE s.catalog_id = c.catalog_id
                       AND s.snapshot_id = c.last_snapshot_id) AS head_age_seconds,
                   (SELECT s.snapshot_time FROM hog_snapshot s
                     WHERE s.catalog_id = c.catalog_id
                       AND s.snapshot_id >= c.earliest_snapshot_id
                     ORDER BY s.snapshot_id
                     LIMIT 1) AS oldest_snapshot_time,
                   (SELECT count(*) FROM hog_file_removal r
                     WHERE r.catalog_id = c.catalog_id
                       AND r.drained_at IS NULL) AS removal_depth,
                   (SELECT count(*) FROM hog_table t
                     WHERE t.catalog_id = c.catalog_id
                       AND t.dropped_snapshot IS NULL) AS table_count,
                   -- Each count: the index range GROUPED BY table_id first,
                   -- then the hog_table join per distinct table. The
                   -- aggregate is a fence the planner cannot move the join
                   -- below, so the read is always the attention index's
                   -- range for the catalog (index-only: table_id is in each
                   -- index) and never the catalog's live files probed from
                   -- hog_table through some other index — the plan it picks
                   -- when the attention population is a large fraction of
                   -- the catalog, i.e. the dropped-backlog case this guards.
                   (SELECT COALESCE(SUM(b.n), 0)::bigint
                      FROM (SELECT f.table_id, count(*) AS n FROM hog_data_file f
                             WHERE f.catalog_id = c.catalog_id
                               AND f.stats_state = 'pending'
                               AND f.end_snapshot IS NULL
                             GROUP BY f.table_id) b
                      JOIN hog_table t ON t.catalog_id = c.catalog_id AND t.table_id = b.table_id
                     WHERE t.dropped_snapshot IS NULL) AS stats_pending,
                   (SELECT COALESCE(SUM(b.n), 0)::bigint
                      FROM (SELECT f.table_id, count(*) AS n FROM hog_data_file f
                             WHERE f.catalog_id = c.catalog_id
                               AND f.stats_state = 'failed'
                               AND f.end_snapshot IS NULL
                             GROUP BY f.table_id) b
                      JOIN hog_table t ON t.catalog_id = c.catalog_id AND t.table_id = b.table_id
                     WHERE t.dropped_snapshot IS NULL) AS stats_failed,
                   (SELECT COALESCE(SUM(b.n), 0)::bigint
                      FROM (SELECT f.table_id, count(*) AS n FROM hog_data_file f
                             WHERE f.catalog_id = c.catalog_id
                               AND f.missing_field_ids
                               AND f.end_snapshot IS NULL
                             GROUP BY f.table_id) b
                      JOIN hog_table t ON t.catalog_id = c.catalog_id AND t.table_id = b.table_id
                     WHERE t.dropped_snapshot IS NULL) AS missing_field_ids,
                   ms.catalog_id IS NOT NULL AS live_sampled,
                   extract(epoch FROM (now() - ms.live_as_of)) AS live_totals_age_seconds,
                   CASE WHEN ms.measures_generation = ms.published_generation
                        THEN (ms.live_rows - COALESCE(dr.rows, 0))::bigint END AS live_rows,
                   (ms.live_bytes - COALESCE(dr.bytes, 0))::bigint AS live_bytes,
                   (ms.live_files - COALESCE(dr.files, 0))::bigint AS live_files
              FROM hog_catalog c
              -- One row per catalog whose PUBLISHED generation carries
              -- stamped totals. `live_generation = published_generation`
              -- is the whole test, the rule measures_generation follows:
              -- a publish by a sampler that does not stamp (an older pod
              -- in a mixed-version rollout, a rollback) moves
              -- published_generation on and leaves the stamp behind, and
              -- the totals must go absent, not freeze while the age
              -- gauge reads fresh and the subtraction probes the new
              -- generation with the old snapshot. NULL = NULL is false,
              -- so a never-stamped row fails the test too.
              LEFT JOIN hog_maintenance_summary ms
                ON ms.catalog_id = c.catalog_id AND ms.live_generation = ms.published_generation
              -- Tables dropped since the generation's snapshot, and their
              -- buckets in it: one aggregate probe of the tier's
              -- (catalog_id, generation, table_id) index PER DROPPED
              -- TABLE, as a nested LATERAL, so the planner cannot answer
              -- "which buckets" with a range over the whole generation
              -- hashed against hog_table (its estimate for `dropped_snapshot
              -- > ?` is a third of the catalog's tables, which makes that
              -- look cheap). The outer guard sits inside the subquery,
              -- where it is a one-time filter per catalog rather than a
              -- join filter applied after the read.
              LEFT JOIN LATERAL (
                  SELECT SUM(b.rows) AS rows, SUM(b.bytes) AS bytes, SUM(b.files) AS files
                    FROM hog_table t
                   CROSS JOIN LATERAL (
                       SELECT SUM(p.record_count) AS rows,
                              SUM(p.total_bytes) AS bytes,
                              SUM(p.file_count) AS files
                         FROM hog_maintenance_summary_tier p
                        WHERE p.catalog_id = t.catalog_id
                          AND p.generation = ms.published_generation
                          AND p.table_id = t.table_id
                   ) b
                   WHERE ms.catalog_id IS NOT NULL
                     AND t.catalog_id = c.catalog_id
                     AND t.dropped_snapshot > ms.published_snapshot
              ) dr ON true
            """

        /**
         * Catalogs whose maintenance sampler has PUBLISHED a generation —
         * the set hoglake_table_series_truncated is published for. One
         * row per catalog, by `hog_maintenance_summary`'s primary key.
         * `published_generation > 0` is `TierTotalsRepo.publishedGeneration`'s
         * own test for "has published". Runs in the same REPEATABLE READ
         * snapshot as [TABLE_SQL] (see `sampleTables`).
         */
        internal const val PUBLISHED_CATALOGS_SQL: String =
            """
            SELECT c.name
              FROM hog_maintenance_summary s
              JOIN hog_catalog c ON c.catalog_id = s.catalog_id
             WHERE s.published_generation > 0 AND s.sample IS NOT NULL
            """

        /**
         * Per-table totals over each catalog's PUBLISHED summary
         * generation, capped per catalog. `internal` for
         * `CatalogMetricsPlanIntegrationTest`.
         *
         * THE SAME GENERATION /maintenance/status and the partitions
         * endpoints read: the tier rows are selected through
         * [TierTotalsRepo.PUBLISHED_GENERATION_JOIN], spliced rather than
         * retyped, so the per-table numbers here and the partitions
         * listing cannot disagree about which scan they describe. Nothing
         * here reads `hog_data_file`.
         *
         * PARTITIONS ARE GROUPED BEFORE TABLES. A tier row's bucket key
         * also hashes the bucket's `quota`, so a generation that straddles
         * a compaction-target change can hold TWO rows for one partition.
         * `partition_groups` folds them per `(table, spec_id,
         * partition_values)` — `PartitionListingService.GROUP_ROWS_SQL`'s
         * grouping — so `partitions` counts partitions and
         * `largest_partition_files` is a partition's total, not a
         * bucket's.
         *
         * LIVE TABLES ONLY, from `hog_table` (invariant 11): a drop is
         * O(columns) and leaves the dropped table's tier rows in the
         * published generation until the next one, so without the
         * `dropped_snapshot IS NULL` term a dropped table keeps its series
         * for a whole generation. The live-version join (`v.end_snapshot
         * IS NULL`) excludes them too, since `TableRepo.markDropped` ends
         * the live version row — so no state the server produces tells
         * the two apart, and a mutation removing either one alone stays
         * green. The `hog_table` predicate stays as the authority
         * invariant 11 names; the version join is what supplies the name
         * (a renamed table publishes under its new name only). And only
         * tables the generation COVERS: `created_snapshot <=
         * sample.snapshotId`, `CatalogService.getTable`'s own coverage
         * test — a table created after the scan began has no tier rows,
         * and the LEFT JOIN would otherwise publish its files as a
         * confident 0. Such a table appears with the next generation.
         *
         * A covered table with no files publishes zeros (LEFT JOIN), for
         * the same reason SAMPLE_SQL's catalog row does.
         *
         * A catalog with NO published generation publishes no table rows.
         * `published_generation > 0 AND sample IS NOT NULL` says so, and
         * is also implied by the coverage clause (a NULL sample makes
         * `created_snapshot <= NULL` unknown), so removing the guard alone
         * changes nothing a test can see; it stays because it is
         * [PUBLISHED_CATALOGS_SQL]'s predicate and the two must agree.
         *
         * COST, measured on CatalogMetricsPlanIntegrationTest's fixture
         * (200,050 live data files, 100,000 DV rows, 102,010 tier rows in
         * two generations with one 50,000-bucket table, 501,203 ledger
         * rows, 11,050 tables; PG 18, serial, warm): ONE pass over the
         * tier relation — the published generation is half of it, so the
         * planner takes a sequential scan over the index — 2,590 shared
         * buffers for 2,466 tier pages, i.e. ~0.025 buffers per tier row,
         * plus a 6 MB spilled sort to group 51,005 published buckets into
         * partitions at the default 4 MB work_mem. Bounded by buckets x
         * retained generations (two), like `TierTotalsRepo.PER_TABLE_SQL`;
         * the cap bounds what is FETCHED, the ranking runs in-database.
         */
        internal const val TABLE_SQL: String =
            """
            WITH partition_groups AS (
                SELECT p.catalog_id, p.table_id, p.spec_id, p.partition_values,
                       sum(p.file_count)   AS files,
                       sum(p.small_count)  AS small_files,
                       sum(p.total_bytes)  AS bytes,
                       sum(p.small_bytes)  AS small_bytes,
                       sum(p.record_count) AS record_count,
                       sum(p.dv_count)     AS delete_files
                  FROM hog_maintenance_summary_tier p
                  ${TierTotalsRepo.PUBLISHED_GENERATION_JOIN}
                 GROUP BY p.catalog_id, p.table_id, p.spec_id, p.partition_values
            ),
            per_table AS (
                SELECT g.catalog_id, g.table_id,
                       sum(g.files)        AS files,
                       sum(g.small_files)  AS small_files,
                       sum(g.bytes)        AS bytes,
                       sum(g.small_bytes)  AS small_bytes,
                       sum(g.record_count) AS record_count,
                       sum(g.delete_files) AS delete_files,
                       count(*) FILTER (WHERE g.files > 0) AS partitions,
                       max(g.files)        AS largest_partition_files
                  FROM partition_groups g
                 GROUP BY g.catalog_id, g.table_id
            ),
            ranked AS (
                SELECT c.name AS catalog,
                       n.name AS namespace_name,
                       v.name AS table_name,
                       s.measures_generation = s.published_generation AS measured,
                       COALESCE(pt.files, 0)                   AS files,
                       COALESCE(pt.small_files, 0)             AS small_files,
                       COALESCE(pt.bytes, 0)                   AS bytes,
                       COALESCE(pt.small_bytes, 0)             AS small_bytes,
                       COALESCE(pt.record_count, 0)            AS record_count,
                       COALESCE(pt.delete_files, 0)            AS delete_files,
                       COALESCE(pt.partitions, 0)              AS partitions,
                       COALESCE(pt.largest_partition_files, 0) AS largest_partition_files,
                       row_number() OVER (
                           PARTITION BY t.catalog_id
                           ORDER BY COALESCE(pt.files, 0) DESC, n.name, v.name) AS rank,
                       count(*) OVER (PARTITION BY t.catalog_id) AS covered
                  FROM hog_maintenance_summary s
                  JOIN hog_catalog c ON c.catalog_id = s.catalog_id
                  JOIN hog_table t ON t.catalog_id = s.catalog_id
                  JOIN hog_table_version v
                    ON v.catalog_id = t.catalog_id AND v.table_id = t.table_id
                   AND v.end_snapshot IS NULL
                  JOIN hog_namespace n
                    ON n.catalog_id = v.catalog_id AND n.namespace_id = v.namespace_id
                  LEFT JOIN per_table pt
                    ON pt.catalog_id = t.catalog_id AND pt.table_id = t.table_id
                 WHERE s.published_generation > 0
                   AND s.sample IS NOT NULL
                   AND t.dropped_snapshot IS NULL
                   AND t.created_snapshot <= (s.sample ->> 'snapshotId')::bigint
            )
            SELECT * FROM ranked WHERE rank <= :cap
            """

        /**
         * Live deletion vectors per catalog. `internal` for
         * `CatalogMetricsPlanIntegrationTest`.
         *
         * THE SAME POPULATION the summary tier's `dv_count` counts, read at
         * head instead of at the generation's snapshot: the sampler marks
         * a visible data file `has_dv` when a visible DV names it, and
         * invariant 3 allows ONE live DV per data file, so the count of
         * live `hog_delete_file` rows on live tables equals the number of
         * live data files carrying one. The two differ only by the
         * generation's age.
         *
         * `hog_delete_file_live (catalog_id, table_id) WHERE end_snapshot
         * IS NULL` holds the live DVs only; never `hog_data_file`. Dropped
         * tables excluded through `hog_table`, as every file gauge is.
         * LEFT JOIN from `hog_catalog` so a catalog with no DVs publishes 0.
         *
         * COST, same fixture: 100,000 DV rows on 1,539 heap pages, the
         * 5,000 live ones scattered one in twenty — a bitmap scan of
         * `hog_delete_file_live` that touches every heap page, 1,548
         * shared buffers (~0.31 per live DV). Scattered live rows make the
         * cost min(live DVs, heap pages): never more than one pass over
         * the DV relation, never the manifest.
         */
        internal const val DELETE_FILES_SQL: String =
            """
            SELECT c.name,
                   COALESCE(d.files, 0) AS files,
                   COALESCE(d.bytes, 0) AS bytes
              FROM hog_catalog c
              LEFT JOIN (
                  SELECT d.catalog_id, count(*) AS files, sum(d.file_size_bytes) AS bytes
                    FROM hog_delete_file d
                    JOIN hog_table t
                      ON t.catalog_id = d.catalog_id AND t.table_id = d.table_id
                   WHERE d.end_snapshot IS NULL
                     AND t.dropped_snapshot IS NULL
                   GROUP BY d.catalog_id
              ) d ON d.catalog_id = c.catalog_id
            """

        /**
         * Earliest-snapshot age and dropped tables pending retirement,
         * per catalog. `internal` for `CatalogMetricsPlanIntegrationTest`.
         *
         * THE AGE is the commit time of the first snapshot at or above
         * the floor, by primary key — the probe SAMPLE_SQL's
         * `oldest_snapshot_time` uses, aged on the Postgres side as
         * head_age is, so pod clock skew cannot bend it.
         *
         * PENDING RETIREMENT is RetirementService's own notion of work —
         * a dropped table with at least one LIVE (`end_snapshot IS NULL`)
         * data-file row — but WITHOUT its floor clause, so a drop still
         * above the floor (waiting, not yet eligible) counts too. Ended
         * rows of a dropped table are expiry's to purge, not retirement's,
         * and a DV cannot outlive its data file (ON DELETE CASCADE), so one
         * `hog_data_file_live` descent per dropped table answers it. Only
         * the newest :probeCap dropped tables (by table id) are probed, so
         * the count is a LOWER BOUND (see [DROPPED_TABLE_PROBE_CAP]).
         *
         * COST, same fixture: 11,000 dropped tables (50 pending, the
         * oldest), so exactly the cap — 10,000 index-only descents of
         * `hog_data_file_live` — is probed: 20,165 shared buffers on PG
         * 18 (20,206 on PG 16), ~2.0 per probed drop, i.e. ~20k buffers
         * per catalog at the default cap. The age probe is 3 buffers per
         * catalog.
         */
        internal const val LIFECYCLE_SQL: String =
            """
            SELECT c.name,
                   (SELECT extract(epoch FROM (now() - s.snapshot_time))
                      FROM hog_snapshot s
                     WHERE s.catalog_id = c.catalog_id
                       AND s.snapshot_id >= c.earliest_snapshot_id
                     ORDER BY s.snapshot_id
                     LIMIT 1) AS earliest_age_seconds,
                   (SELECT count(*)
                      FROM (SELECT t.catalog_id, t.table_id
                              FROM hog_table t
                             WHERE t.catalog_id = c.catalog_id
                               AND t.dropped_snapshot IS NOT NULL
                             ORDER BY t.table_id DESC
                             LIMIT :probeCap) d
                     WHERE EXISTS (
                           SELECT 1 FROM hog_data_file f
                            WHERE f.catalog_id = d.catalog_id
                              AND f.table_id = d.table_id
                              AND f.end_snapshot IS NULL)) AS dropped_pending
              FROM hog_catalog c
            """

        /**
         * The latest `ok` and `failed` finish time and the latest run's
         * duration per (catalog, task), over the task's newest
         * :lookback ledger rows. `internal` for
         * `CatalogMetricsPlanIntegrationTest`.
         *
         * TWO STEPS, AND THE SPLIT IS THE FIX. The window's run ids come
         * from an INDEX-ONLY read of `hog_maintenance_run_recent
         * (catalog_id, task, run_id DESC)` — both equality columns in the
         * Index Cond, stopping at the LIMIT — and the rows are then
         * fetched by primary key. Written as one ordered `LIMIT` over the
         * table, the planner chooses `hog_maintenance_run_task_history
         * (task, run_id DESC)` and applies `catalog_id` as a Filter once
         * one catalog dominates the ledger and the others have so few rows
         * that ANALYZE sees one distinct `catalog_id` — the production
         * shape. `CatalogMetricsPlanIntegrationTest` reproduces it and
         * asserts it (PG 18): 62,650 rows removed by the filter per loop
         * x 24 loops, 27,513 shared buffers for that one-step form (a
         * review measured the same shape on PG 16, ~30k buffers, 150-200
         * ms). No new index: both steps use existing ones.
         *
         * A pair with no rows produces an all-NULL aggregate, filtered
         * out, so a task that never ran has no series rather than a 0
         * epoch. `finished_at` is the epoch: the moment the status became
         * true, which is what "when did compaction last succeed" asks.
         *
         * COST, same fixture (one catalog with 500,000 rows on `expiry`
         * and 300 on each other task, one catalog with 3 OLDER rows, two
         * with none): 24 pairs, 2,203 window rows, and no ledger scan
         * removes a row. On PG 18: 149 shared buffers — 93 for the
         * index-only windows, 55 for the PK fetch, which PG 17+ does as a
         * few ordered index searches over the id array — ~0.07 per window
         * row. On PG 16, which descends the primary key ONCE PER ID for
         * `run_id = ANY(...)`: 8,943 buffers, of which 8,849 are the PK
         * fetch, ~4.0 per window row. Either way the window, not the
         * ledger, sets the cost; the plan test's budget is 12 buffers per
         * window row, which admits PG 16 and excludes the one-step form.
         */
        internal const val MAINTENANCE_RUNS_SQL: String =
            """
            SELECT c.name AS catalog, tasks.task, w.ok_at, w.failed_at, w.last_duration
              FROM hog_catalog c
              CROSS JOIN unnest(:tasks::text[]) AS tasks(task)
              CROSS JOIN LATERAL (
                  SELECT extract(epoch FROM max(r.finished_at) FILTER (WHERE r.status = 'ok')) AS ok_at,
                         extract(epoch FROM max(r.finished_at) FILTER (WHERE r.status = 'failed')) AS failed_at,
                         (array_agg(extract(epoch FROM (r.finished_at - r.started_at))
                                    ORDER BY r.run_id DESC))[1] AS last_duration
                    FROM hog_maintenance_run r
                   WHERE r.run_id = ANY (ARRAY(
                             SELECT k.run_id
                               FROM hog_maintenance_run k
                              WHERE k.catalog_id = c.catalog_id AND k.task = tasks.task
                              ORDER BY k.run_id DESC
                              LIMIT :lookback))
              ) w
             WHERE w.last_duration IS NOT NULL
            """
    }
}
