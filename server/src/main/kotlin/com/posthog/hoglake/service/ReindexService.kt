package com.posthog.hoglake.service

import com.posthog.hoglake.Database
import com.posthog.hoglake.model.IndexBloatEstimate
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.ReindexResult
import com.posthog.hoglake.model.ReindexSkip
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.IndexBloatGauges
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.persistence.MaintenanceRunStore
import com.posthog.hoglake.persistence.PartialResult
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import java.time.Duration
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneOffset

/**
 * THE DAILY INDEX-BLOAT CHECK, and at most one `REINDEX INDEX
 * CONCURRENTLY` per day (#268).
 *
 * WHY A TASK AND NOT A RUNBOOK. Autovacuum reclaims dead heap tuples and
 * marks wholly empty btree pages reusable; it never gives an index's
 * pages back. After the 13.9M-row retirement on gigahog-prod-us,
 * `hog_data_file` carried 14.8 GB of indexes over 1.48M rows and the
 * `hog_file_column_stats` primary key 26 GB over 14.9M rows. Bloat on
 * this schema arrives in STEPS — a retirement, an expiry purge, a large
 * compaction — not as a slope, so a measured daily check rebuilds what
 * actually grew, where a calendar REINDEX rebuilds everything or nothing.
 *
 * THE ESTIMATE is the check_postgres / ioguix btree arithmetic
 * ([ESTIMATE_SQL]): expected leaf pages from `reltuples`, the key
 * columns' `pg_stats` widths and null fractions, the page and tuple
 * headers and the fillfactor, against `relpages`. It reads `pg_index`,
 * `pg_class`, `pg_attribute` and `pg_stats` only — no hog_* row, no
 * `pgstattuple` — so its cost is the catalog's size, not the manifest's.
 * It is an ESTIMATE in both directions: it is only as fresh as the last
 * VACUUM/ANALYZE (`relpages`, `reltuples` and `pg_stats` all come from
 * them), and btree DEDUPLICATION (PG 13+) stores a run of equal keys as
 * one tuple, so a low-cardinality index reads SMALLER than expected and
 * its bloat is under-reported. That is the safe side: it can withhold a
 * rebuild, never cause one. KNOWN LIMIT, measured: a fresh build of
 * `hog_data_file_changefeed` / `_live` (many rows per table and snapshot)
 * reads ~0.17, so their bloat is under-reported ~6x and the ratio arm
 * needs ~18x real bloat to fire there. A nullable key column reads ~0.82
 * fresh (the null-bitmap header term). Indexes whose keys end in a unique
 * column read 1.01-1.02 fresh, which is what the tests pin.
 *
 * THE THRESHOLD ([overThreshold]) is a ratio of [RATIO_THRESHOLD] (the
 * index is three times the size a rebuild would leave) or an excess of
 * [EXCESS_BYTES_THRESHOLD] at a ratio of at least [EXCESS_MIN_RATIO] (a
 * gigabyte of dead pages is worth a rebuild, but a random-key btree's own
 * ~1.35x steady state is not dead pages), with the ratio arm floored at
 * [MIN_RATIO_INDEX_BYTES] — the estimate's one-page metapage term makes
 * any index of a few pages read as 2-3x, and rebuilding a 40 KiB index
 * every night is noise in the ledger and nothing on disk.
 *
 * ONE INDEX PER RUN, the largest excess first ([rank]), and never one
 * expected to rebuild past [MAX_REBUILD_EXPECTED_BYTES] (16 GiB; its WAL,
 * temp spill and second copy are ~3x that in transient disk) nor one whose
 * newest attempt failed ([LAST_ATTEMPT_SQL]) — those are excluded and the
 * next candidate goes; with none left the row names the first and why
 * (`too_large`, `last_attempt_failed`). A rebuild
 * is minutes of I/O on the instance that serves the commit tail, so the
 * daily budget is one; the next-largest is tomorrow's. Constraint-backing
 * indexes (primary keys, unique constraints) are ELIGIBLE: `REINDEX INDEX
 * CONCURRENTLY` rebuilds them in place on PG >= 12, keeping the
 * constraint, and the 26 GB `hog_file_column_stats_pkey` is the reason
 * this task exists. On an exact tie in excess a non-constraint index goes
 * first, because if anything were to go wrong the one NOT enforcing a
 * constraint is the cheaper casualty.
 *
 * THE REBUILD runs on its own connection, NOT under any catalog's commit
 * lock — REINDEX CONCURRENTLY takes SHARE UPDATE EXCLUSIVE on the table,
 * which does not conflict with the commit path's ROW EXCLUSIVE, and it
 * waits out older transactions rather than blocking newer ones. Its
 * `statement_timeout` is raised to [REINDEX_STATEMENT_TIMEOUT] on that
 * session and restored afterwards (see the constant for the arithmetic).
 * What it does cost: two heap passes of the table's I/O, ~0.9x the new
 * index's size in WAL and ~1x in temp files (maintenance_work_mem 16 MB),
 * the old index until the swap, a pooled connection for the duration
 * (priced in Config's pool budget), and a snapshot held per phase that
 * pins the vacuum horizon while it runs — which is why the run is at
 * 03:00 UTC and not whenever bloat is noticed. Its SHARE UPDATE EXCLUSIVE
 * lock also CANCELS an autovacuum already running on the table and makes
 * autovacuum skip the table for the rebuild's duration (30-60 minutes on
 * `hog_file_column_stats`); dead tuples there wait until it ends.
 *
 * THE GUARDS ([ReindexSkip]) stop a rebuild when it would be wasted
 * (retirement or an expiry purge still deleting rows, so the index bloats
 * again tomorrow) or would collide with other DDL (a migration, another
 * REINDEX). A skipped run still estimates, still drops leftovers where it
 * safely can, and still writes a ledger row, so a day without a rebuild
 * says why.
 *
 * FAILURE is a failed ledger row carrying the error and the attempt
 * ([ReindexFailed]); it is never retried inside the run. A failed REINDEX
 * CONCURRENTLY leaves an INVALID copy behind (`<index>_ccnew`, or
 * `<index>_ccold` if it failed after the swap), which every write keeps
 * maintaining and nothing reads (one cancelled in validation is READY,
 * so every insert pays for it). The failure path drops it at once; a
 * copy that survives that (a dead session) is dropped by the next run
 * before anything else ([LEFTOVERS_SQL]). The index itself is then
 * excluded while its failed attempt is in the ledger.
 *
 * A POD KILLED MID-REBUILD does not stop it: the backend keeps running
 * the REINDEX, holding the single-flight session lock, until it finishes
 * or hits its bound. The killed run writes no ledger row; the next
 * same-day poll finds the lock held and records `reindex_lock_held`,
 * which CLOSES the day — the orphan is today's rebuild.
 *
 * ON EXACTLY ONE POD: the loop is off unless HOGLAKE_REINDEX_INTERVAL_MS
 * is positive, which the chart sets on the maintenance Deployment alone.
 * A second pod configured by mistake is caught by the single-flight
 * session lock ([Locks.tryAcquireReindexLock]) and the daily gate, which
 * reads the shared ledger. The `hoglake_index_bloat_bytes` gauge belongs
 * to that pod too ([publishGauge]): published by the daily run and on
 * every metrics-sampler tick there, never by an API pod serving the
 * health page.
 */
class ReindexService(
    private val jdbi: Jdbi,
    private val statementTimeout: Duration = REINDEX_STATEMENT_TIMEOUT,
    /**
     * The ratio arm's size floor, a PARAMETER only so an integration test
     * can bloat a few-MiB index instead of seeding hundreds of thousands of
     * rows; production takes [MIN_RATIO_INDEX_BYTES] and nothing in Config
     * exposes it.
     */
    private val minRatioIndexBytes: Long = MIN_RATIO_INDEX_BYTES,
    /**
     * The `too_large` cap, a PARAMETER only so a test can exceed it with a
     * small index; production takes [MAX_REBUILD_EXPECTED_BYTES].
     */
    private val maxRebuildExpectedBytes: Long = MAX_REBUILD_EXPECTED_BYTES,
    /**
     * Whether this process publishes `hoglake_index_bloat_bytes`. True only
     * where the reindex loop runs (App passes `HOGLAKE_REINDEX_INTERVAL_MS
     * > 0`), so ONE pod owns the series: an API pod computes the estimate
     * for the health page but never registers the gauge, or every pod a
     * console user happened to hit would publish its own copy and the
     * series would flap between pods.
     */
    private val publishGauge: Boolean = false,
    /**
     * Runs between a successful REINDEX and reading the result back. A
     * test seam only — it is how a test fails the post-steps after a real
     * rebuild — and a no-op in production.
     */
    private val afterRebuild: (Handle) -> Unit = {},
    /** Wall clock, injected so the daily gate can be driven across midnight in a test. */
    private val clock: () -> Instant = Instant::now,
) {
    private val log = KotlinLogging.logger {}
    private val runStore = MaintenanceRunStore(jdbi)

    /**
     * The newest run THIS process started, and whether its outcome lets the
     * gate retry inside the window ([RETRYABLE_SKIPS]). The ledger is the
     * gate; this is its backstop, because ledger writes are best-effort and
     * swallow their failures (`MaintenanceRunStore`), and a run the gate
     * cannot see would otherwise rebuild an index again on every poll.
     */
    @Volatile
    private var lastHere: LastRun? = null

    /** The newest run the gate knows of: when it started, and whether a same-window retry is allowed. */
    data class LastRun(val startedAt: Instant, val retryable: Boolean)

    init {
        require(!statementTimeout.isNegative && !statementTimeout.isZero) {
            "reindex statement timeout must be positive (got $statementTimeout): 0 means UNLIMITED to Postgres"
        }
    }

    /** Thrown when the REINDEX itself failed; the funnel records [partial] on the failed row. */
    class ReindexFailed(
        override val partial: ReindexResult,
        cause: Throwable,
    ) : RuntimeException(cause.message, cause),
        PartialResult

    /**
     * The loop body: run the day's reindex if it is due, else nothing.
     * Returns the run's result, or null when no run happened.
     *
     * The gate reads the LEDGER, not process memory, so a restart after
     * today's run does not run again, and a pod that was down at 03:00
     * runs on its first poll after it comes back the same day. A run that
     * stepped aside for a migration or another build does NOT close the
     * day before [RETRY_UNTIL_UTC] ([isDue]).
     */
    fun pollOnce(): ReindexResult? {
        val now = clock()
        val (ledger, catalogs) =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(LAST_RUN_SQL)
                    .map { rs, _ ->
                        LastRun(
                            rs.getObject("started_at", java.time.OffsetDateTime::class.java).toInstant(),
                            rs.getString("status") == "ok" && rs.getString("skipped_reason") in RETRYABLE_WIRE,
                        )
                    }
                    .findOne()
                    .orElse(null) to
                    h.createQuery("SELECT count(*) FROM hog_catalog").mapTo(Long::class.java).one()
            }
        val newest = listOfNotNull(ledger, lastHere).maxByOrNull { it.startedAt }
        if (!isDue(now, newest?.startedAt, newest?.retryable ?: false)) return null
        // The ledger is per-catalog: with no catalog a run could not be
        // recorded, so the gate would never see it and every poll would
        // run again. An instance with no catalog has no manifest to bloat.
        if (catalogs == 0L) {
            log.debug { "reindex due but this instance has no catalog to record it against; skipping" }
            return null
        }
        lastHere = LastRun(now, retryable = false)
        val result = runOnce(MaintenanceTrigger.LOOP)
        lastHere = LastRun(now, retryable = result.skippedReason in RETRYABLE_WIRE)
        return result
    }

    /** One run, recorded once per catalog. Tests call this directly. */
    fun runOnce(trigger: MaintenanceTrigger = MaintenanceTrigger.MANUAL): ReindexResult =
        runStore.recordedAllCatalogs(MaintenanceTask.REINDEX, trigger, clock) {
            val result =
                try {
                    run()
                } catch (e: Throwable) {
                    Audit.event(
                        "reindex",
                        null,
                        (e as? ReindexFailed)?.partial?.index,
                        Audit.failureOutcome(e),
                        e.message,
                    )
                    throw e
                }
            Audit.event(
                "reindex",
                null,
                result.index,
                outcome = "ok",
                detail =
                    "checked=${result.checked} over_threshold=${result.overThreshold} " +
                        "table=${result.table} expected_bytes=${result.expectedBytes} " +
                        "before_bytes=${result.beforeBytes} after_bytes=${result.afterBytes} " +
                        "duration_ms=${result.durationMs} skipped_reason=${result.skippedReason} " +
                        "invalid_dropped=${result.invalidDropped}",
            )
            result
        }

    /**
     * The estimate alone, for the health page: one read on the caller's
     * handle (its REPEATABLE READ snapshot). Publishes the gauge only where
     * [publishGauge] says this process owns it.
     */
    fun estimate(h: Handle): List<IndexBloatEstimate> =
        estimateOn(h).also { if (publishGauge) IndexBloatGauges.publish(it) }

    /**
     * Republish the gauge from a fresh estimate (~4 ms on the compose
     * stack), for the metrics sampler's tick on the pod that owns it, so
     * the series tracks VACUUM/ANALYZE between daily runs. A no-op
     * elsewhere.
     */
    fun refreshGauge() {
        if (!publishGauge) return
        // Never throws: a failure is logged and counted on its own series,
        // so it neither fails the metrics loop's tick it rides nor hides
        // inside that loop's failure counter.
        runCatching { jdbi.withHandleUnchecked { h -> estimate(h) } }
            .onFailure { e ->
                log.warn(e) { "reindex: index-bloat gauge refresh failed" }
                Metrics.reindexGaugeRefreshFailure()
            }
    }

    private fun run(): ReindexResult =
        // ONE session for the whole run: the single-flight lock is a
        // SESSION advisory lock, and the raised statement_timeout must be
        // on the connection that issues the REINDEX.
        jdbi.open().use { h ->
            val estimates = estimate(h)
            val over = estimates.filter { overThreshold(it, minRatioIndexBytes) }
            val base = ReindexResult(checked = estimates.size.toLong(), overThreshold = over.size.toLong())
            if (!Locks.tryAcquireReindexLock(h)) {
                return@use base.copy(skippedReason = ReindexSkip.REINDEX_LOCK_HELD.wire)
            }
            preservingPrimary(
                body = {
                    // Before the leftovers are dropped: an in-flight build's
                    // own `_ccnew` IS an invalid index, and a migration's
                    // half-built one is the operator's to look at.
                    guard(h, beforeDrop = true)?.let { return@preservingPrimary base.copy(skippedReason = it.wire) }
                    withStatementTimeout(h) { locked(h, over, base) }
                },
                cleanup = { Locks.releaseReindexLock(h) },
                onCleanupFailure = { h.connection.abort { it.run() } },
            )
        }

    private fun locked(
        h: Handle,
        over: List<IndexBloatEstimate>,
        base: ReindexResult,
    ): ReindexResult {
        val afterDrop = base.copy(invalidDropped = dropLeftovers(h))
        val ranked = rank(over)
        if (ranked.isEmpty()) return afterDrop
        val failedLast = lastAttemptFailed(h, ranked.map { it.index })
        val (victim, reason) = select(ranked, failedLast, maxRebuildExpectedBytes)
        if (victim == null) {
            // Every candidate is excluded. Name the one that would have gone
            // first, so the ledger says what is waiting for a human and why.
            val top = ranked.first()
            return afterDrop.copy(
                skippedReason = reason!!.wire,
                index = top.index,
                table = top.table,
                expectedBytes = top.expectedBytes,
            )
        }
        // Only with something to rebuild: these two say the rebuild would
        // be wasted, and a quiet day must stay quiet in the ledger while
        // retirement runs for a week.
        val skip = guard(h, beforeDrop = false)
        if (skip != null) return afterDrop.copy(skippedReason = skip.wire)
        val excluded = ranked.count { it.index in failedLast || (it.expectedBytes ?: 0L) > maxRebuildExpectedBytes }
        return rebuild(h, victim, afterDrop.copy(excluded = excluded.toLong()))
    }

    /**
     * The first guard that fires, or null. Split in two because the
     * in-progress and migration checks must precede the leftover drop and
     * the retirement and purge checks need not.
     */
    private fun guard(
        h: Handle,
        beforeDrop: Boolean,
    ): ReindexSkip? =
        if (beforeDrop) {
            when {
                h.queryBool(INDEX_BUILD_IN_PROGRESS_SQL) -> ReindexSkip.REINDEX_IN_PROGRESS
                migrationPending(h) -> ReindexSkip.MIGRATION_PENDING
                else -> null
            }
        } else {
            when {
                h.queryBool(RETIREMENT_PENDING_SQL) -> ReindexSkip.RETIREMENT_PENDING
                h.createQuery(PURGE_PENDING_SQL)
                    .bind("cap", ExpiryService.PURGE_REMAINING_CAP)
                    .mapTo(Boolean::class.javaObjectType)
                    .one() -> ReindexSkip.PURGE_PENDING
                else -> null
            }
        }

    private fun migrationPending(h: Handle): Boolean {
        val lockHeld =
            h.createQuery(MIGRATION_LOCK_HELD_SQL)
                .bind("hi", Database.MIGRATION_LOCK_KEY ushr 32)
                .bind("lo", Database.MIGRATION_LOCK_KEY and 0xFFFFFFFFL)
                .mapTo(Boolean::class.javaObjectType)
                .one()
        if (lockHeld) return true
        // Flyway's history table exists on every database this server
        // has migrated, which is every one it serves; the lookup keeps a
        // hand-made database from turning the guard into a 500.
        val hasHistory =
            h.createQuery("SELECT to_regclass('flyway_schema_history') IS NOT NULL")
                .mapTo(Boolean::class.javaObjectType)
                .one()
        return hasHistory && h.queryBool(FAILED_MIGRATION_SQL)
    }

    /** Names among [indexes] whose newest recorded REBUILD ATTEMPT failed. */
    private fun lastAttemptFailed(
        h: Handle,
        indexes: List<String>,
    ): Set<String> =
        h.createQuery(LAST_ATTEMPT_SQL)
            .bindArray("names", String::class.java, indexes)
            .map { rs, _ -> rs.getString("idx") to rs.getString("status") }
            .list()
            .filter { it.second == "failed" }
            .map { it.first }
            .toSet()

    /** Drop every invalid REINDEX CONCURRENTLY leftover on a hog_* table; returns how many. */
    private fun dropLeftovers(h: Handle): Long {
        val statements = h.createQuery(LEFTOVERS_SQL).mapTo(String::class.java).list()
        for (ddl in statements) {
            log.info { "reindex: dropping leftover of a failed REINDEX CONCURRENTLY: $ddl" }
            h.execute(ddl)
        }
        return statements.size.toLong()
    }

    private fun rebuild(
        h: Handle,
        victim: IndexBloatEstimate,
        base: ReindexResult,
    ): ReindexResult {
        val before = h.relationSize(victim.index)
        val attempt =
            base.copy(
                index = victim.index,
                table = victim.table,
                expectedBytes = victim.expectedBytes,
                beforeBytes = before,
            )
        log.info {
            "reindex: rebuilding ${victim.table}.${victim.index} " +
                "($before bytes, expected ${victim.expectedBytes}, ratio ${victim.ratio})"
        }
        val started = System.nanoTime()
        try {
            h.execute(h.createQuery(REINDEX_DDL_SQL).bind("index", victim.index).mapTo(String::class.java).one())
        } catch (e: Exception) {
            val failed = attempt.copy(durationMs = (System.nanoTime() - started) / 1_000_000)
            // Drop the copy NOW rather than tomorrow: a build cancelled in
            // its validation phase leaves a `_ccnew` that is READY for
            // writes, so every insert into the table maintains it until it
            // goes. Best-effort: a dead session cannot drop anything, and
            // tomorrow's run drops whatever is left before it starts.
            val dropped =
                runCatching { dropLeftovers(h) }
                    .onFailure { e.addSuppressed(it) }
                    .getOrDefault(0L)
            throw ReindexFailed(failed.copy(invalidDropped = failed.invalidDropped + dropped), e)
        }
        val durationMs = (System.nanoTime() - started) / 1_000_000
        val rebuilt = attempt.copy(durationMs = durationMs)
        // THE REBUILD HAPPENED, so nothing after it may turn the row into a
        // failure: a failed `failed` row would also exclude this index
        // tomorrow (last_attempt_failed) for a rebuild that succeeded. The
        // post-steps are best-effort and their error rides the ok row.
        return try {
            afterRebuild(h)
            // The rebuild set the new index's relpages/reltuples, so a
            // second estimate publishes its post-rebuild excess rather than
            // leaving yesterday's figure on the gauge until tomorrow.
            estimate(h)
            rebuilt.copy(afterBytes = h.relationSize(victim.index))
        } catch (e: Exception) {
            log.warn(e) { "reindex: ${victim.index} was rebuilt, but reading it back failed" }
            rebuilt.copy(postStepError = (e.cause ?: e).message?.take(MaintenanceRunStore.MAX_ERROR_LENGTH))
        }
    }

    /**
     * Run [body] with this session's `statement_timeout` raised to
     * [statementTimeout], restoring the value it had. A pooled
     * connection that kept an hour-long bound would hand it to the next
     * request that borrowed it, so a failed restore ABORTS the connection
     * rather than returning it to the pool.
     */
    private fun <T> withStatementTimeout(
        h: Handle,
        body: () -> T,
    ): T {
        val previous = h.createQuery("SELECT current_setting('statement_timeout')").mapTo(String::class.java).one()
        h.createQuery("SELECT set_config('statement_timeout', :v, false)")
            .bind("v", "${statementTimeout.toMillis()}ms")
            .mapTo(String::class.java)
            .one()
        return preservingPrimary(
            body = body,
            cleanup = {
                h.createQuery("SELECT set_config('statement_timeout', :v, false)")
                    .bind("v", previous)
                    .mapTo(String::class.java)
                    .one()
            },
            onCleanupFailure = { h.connection.abort { it.run() } },
        )
    }

    private fun estimateOn(h: Handle): List<IndexBloatEstimate> =
        h.createQuery(ESTIMATE_SQL)
            .map { rs, _ ->
                IndexBloatEstimate(
                    table = rs.getString("table_name"),
                    index = rs.getString("index_name"),
                    sizeBytes = rs.getLong("size_bytes"),
                    expectedBytes = rs.getLong("expected_bytes").takeUnless { rs.wasNull() },
                    constraintBacking = rs.getBoolean("constraint_backing"),
                )
            }
            .list()

    private fun Handle.queryBool(sql: String): Boolean = createQuery(sql).mapTo(Boolean::class.javaObjectType).one()

    private fun Handle.relationSize(index: String): Long =
        createQuery("SELECT pg_relation_size(to_regclass(quote_ident(:index)))")
            .bind("index", index)
            .mapTo(Long::class.javaObjectType)
            .one()

    internal companion object {
        /**
         * When the day's run is due, in UTC. HARDCODED for now; Jakob
         * wants it configurable later (#268). 03:00 UTC is the trough of
         * the US and EU write curves, and the rebuild's held snapshot pins
         * the vacuum horizon while it runs, which is cheapest when the
         * fewest rows are dying.
         */
        val RUN_AT_UTC: LocalTime = LocalTime.of(3, 0)

        /**
         * Rebuild when actual / expected reaches this. Configurable later
         * (#268), like [RUN_AT_UTC]. 3x is well past what a healthy btree
         * drifts to (page splits leave ~70% full pages, ~1.3-1.5x) and
         * well short of what the prod-us retirement left (14.8 GB over
         * 1.48M rows on hog_data_file).
         */
        const val RATIO_THRESHOLD = 3.0

        /**
         * Rebuild when the estimated excess reaches this, whatever the
         * ratio — a large index at 1.8x can be carrying more dead
         * gigabytes than a small one at 10x. Configurable later (#268).
         */
        const val EXCESS_BYTES_THRESHOLD = 1L shl 30

        /**
         * The excess arm also needs this ratio. Configurable later (#268).
         * A random-key btree (an md5 path, a uuid) settles at ~1.35x on
         * its own from 50/50 page splits, so without a ratio floor every
         * such index past ~3.9 GiB sits permanently over 1 GiB of
         * "excess" and is rebuilt every day for nothing —
         * `hog_data_file_path` is headed there. 1.5 is above that steady
         * state and below any bloat worth a gigabyte.
         */
        const val EXCESS_MIN_RATIO = 1.5

        /**
         * Largest EXPECTED size (the estimate's freshly-built size) the
         * task rebuilds unattended. Configurable later (#268). Measured on
         * PG 18 with maintenance_work_mem 16 MB: a concurrent rebuild
         * writes ~0.91x its new size in WAL and spills ~1x to temp files,
         * while the old index stays until the swap — so a rebuild costs
         * ~3x its expected size in transient disk and WAL at once. 16 GiB
         * (Jakob, 2026-10-05) admits the first prod-us candidate,
         * hog_file_column_stats_pkey at ~15 GB expected — ~45 GB
         * transient, so the RDS volume needs that headroom on the night it
         * runs — and stops anything larger. Above the cap the run skips
         * with `too_large`, naming the index and its expected size, and an
         * operator runs it by hand off-peak.
         */
        const val MAX_REBUILD_EXPECTED_BYTES = 16L shl 30

        /**
         * Floor under the RATIO arm only: an index smaller than this is
         * never rebuilt for its ratio. Configurable later (#268). The
         * estimate counts a metapage and ignores internal pages, so a
         * few-page index routinely reads 2-3x while holding kilobytes;
         * 8 MiB is the health page's own `SIGNIFICANT_INDEX_BYTES`, the
         * size below which it calls an index noise.
         */
        const val MIN_RATIO_INDEX_BYTES = 8L shl 20

        /**
         * The REINDEX statement's bound. An hour, against the session's
         * 60 s: rebuilding concurrently is two passes over the TABLE (the
         * build and the validation) plus a sort, so the cost follows the
         * heap. `hog_file_column_stats` is the worst case at ~66 GiB of
         * heap on prod-us, which at an idle volume's ~125 MB/s is ~9
         * minutes a pass and 20-30 minutes on a busy one; the hour is that
         * with headroom for the phases that wait out older transactions.
         * Past it something other than size is wrong (a foreign snapshot
         * the build is parked behind), and cancelling leaves a `_ccnew`
         * that tomorrow's run drops before retrying — which is cheaper
         * than an unbounded statement on the commit-path instance.
         */
        val REINDEX_STATEMENT_TIMEOUT: Duration = Duration.ofHours(1)

        /**
         * Until when, in UTC, a run that stepped aside for a TRANSIENT
         * reason ([RETRYABLE_SKIPS]) is retried on the next poll instead
         * of closing the day. Configurable later (#268), like
         * [RUN_AT_UTC]. A pod boot holds the migration lock for seconds;
         * without the retry a 03:00 poll that lands inside it would cost
         * the whole day. Three hours keeps the retries inside the low-
         * traffic window the run is scheduled for.
         */
        val RETRY_UNTIL_UTC: LocalTime = LocalTime.of(6, 0)

        /**
         * Skips that say "not now" rather than "not today": a FOREIGN
         * build or a migration is in flight and will be gone in seconds to
         * minutes. Retirement and purge skips are not here — those last
         * hours — nor the exclusions of a candidate, nor
         * [ReindexSkip.REINDEX_LOCK_HELD]: that holder is this task's own
         * rebuild (an orphan of a killed pod, or a second pod), so today's
         * rebuild is already under way.
         */
        val RETRYABLE_SKIPS: Set<ReindexSkip> = setOf(ReindexSkip.MIGRATION_PENDING, ReindexSkip.REINDEX_IN_PROGRESS)
        private val RETRYABLE_WIRE: Set<String> = RETRYABLE_SKIPS.map { it.wire }.toSet()

        /**
         * True when [now] is past today's [RUN_AT_UTC] and the newest
         * recorded run started before it — or started after it but only
         * stepped aside for a transient reason ([lastRetryable]) and it is
         * not yet [retryUntil]. "Today" is the UTC date of [now]: between
         * midnight and 03:00 a run is never due, so a run missed yesterday
         * and not caught up before midnight waits for today's 03:00
         * rather than running at 00:01 and again at 03:00.
         */
        fun isDue(
            now: Instant,
            lastStartedAt: Instant?,
            lastRetryable: Boolean = false,
            runAt: LocalTime = RUN_AT_UTC,
            retryUntil: LocalTime = RETRY_UNTIL_UTC,
        ): Boolean {
            val today = now.atOffset(ZoneOffset.UTC).toLocalDate()
            val window = today.atTime(runAt).toInstant(ZoneOffset.UTC)
            if (now.isBefore(window)) return false
            if (lastStartedAt == null || lastStartedAt.isBefore(window)) return true
            return lastRetryable && now.isBefore(today.atTime(retryUntil).toInstant(ZoneOffset.UTC))
        }

        /**
         * Helper for the two cleanups in this class (restoring the
         * statement bound, releasing the session lock): run [cleanup]
         * after [body] and, when the cleanup throws, ATTACH its failure to
         * [body]'s instead of replacing it. A plain `finally { throw }`
         * replaced a `ReindexFailed` and the ledger row lost the index it
         * was about. [onCleanupFailure] aborts the connection so a session
         * in an unknown state never goes back to the pool.
         *
         * After a body that SUCCEEDED, a cleanup failure is logged, not
         * thrown, once the abort has gone through: the abort is itself the
         * cleanup (a closed session holds neither a raised statement bound
         * nor a session lock), and throwing would turn a completed rebuild
         * into an anonymous failed row. Only a failed abort is rethrown —
         * then nobody knows what the pooled session still carries.
         */
        internal fun <T> preservingPrimary(
            body: () -> T,
            cleanup: () -> Unit,
            onCleanupFailure: () -> Unit,
        ): T {
            var primary: Throwable? = null
            try {
                return body()
            } catch (t: Throwable) {
                primary = t
                throw t
            } finally {
                try {
                    cleanup()
                } catch (c: Throwable) {
                    val aborted = runCatching(onCleanupFailure).onFailure { c.addSuppressed(it) }.isSuccess
                    when {
                        primary != null -> primary.addSuppressed(c)
                        aborted ->
                            companionLog.warn(
                                c,
                            ) { "reindex: cleanup failed after a completed step; session aborted" }
                        else -> throw c
                    }
                }
            }
        }

        private val companionLog = KotlinLogging.logger {}

        /**
         * The threshold. No estimate (null ratio) is never over: an index
         * whose widths are unknown cannot be judged, and the unknown
         * direction is the one that inflates bloat.
         */
        fun overThreshold(
            e: IndexBloatEstimate,
            minRatioIndexBytes: Long = MIN_RATIO_INDEX_BYTES,
        ): Boolean {
            val ratio = e.ratio ?: return false
            val excess = e.excessBytes ?: return false
            return (excess >= EXCESS_BYTES_THRESHOLD && ratio >= EXCESS_MIN_RATIO) ||
                (ratio >= RATIO_THRESHOLD && e.sizeBytes >= minRatioIndexBytes)
        }

        /**
         * Candidates in the order a run tries them: largest excess, then
         * non-constraint before constraint-backing, then name (so a tie is
         * deterministic across runs). [over] is already filtered.
         */
        fun rank(over: List<IndexBloatEstimate>): List<IndexBloatEstimate> =
            over.sortedWith(
                compareByDescending<IndexBloatEstimate> { it.excessBytes ?: 0L }
                    .thenBy { it.constraintBacking }
                    .thenBy { it.index },
            )

        /**
         * The run's victim from [ranked] (already [rank]ed): the first that
         * is neither expected past [cap] nor in [failedLast]. With none,
         * null and the reason the FIRST candidate was excluded — its last
         * attempt failing outranks its size, since that one a human has to
         * look at either way.
         */
        fun select(
            ranked: List<IndexBloatEstimate>,
            failedLast: Set<String>,
            cap: Long = MAX_REBUILD_EXPECTED_BYTES,
        ): Pair<IndexBloatEstimate?, ReindexSkip?> {
            val victim = ranked.firstOrNull { it.index !in failedLast && (it.expectedBytes ?: 0L) <= cap }
            if (victim != null || ranked.isEmpty()) return victim to null
            val top = ranked.first()
            return null to if (top.index in failedLast) ReindexSkip.LAST_ATTEMPT_FAILED else ReindexSkip.TOO_LARGE
        }

        /** The first of [rank], before the exclusions a run applies (too large, last attempt failed). */
        fun pickVictim(over: List<IndexBloatEstimate>): IndexBloatEstimate? = rank(over).firstOrNull()

        /**
         * The btree bloat estimate, check_postgres / ioguix style, over
         * the valid btree indexes of this schema's hog_* tables.
         * `internal` so the test measures the statement production runs.
         *
         * Per index: expected leaf pages = 1 (metapage) + ceil(reltuples
         * / tuples-per-page), where tuples-per-page = floor((block_size -
         * 24 page header - 16 btree opaque) x fillfactor/100 / (4 line
         * pointer + MAXALIGN(tuple header) + MAXALIGN(data width))). The
         * tuple header is 8 bytes, 12 with a null bitmap when any key
         * column has nulls; the data width is sum((1 - null_frac) x
         * avg_width) over the key columns (expression columns read the
         * index's own pg_stats row). MAXALIGN is 8: every platform RDS
         * runs (x86_64 and Graviton) is 64-bit.
         *
         * `expected_bytes` is NULL when any key column lacks a `pg_stats`
         * row or the index has never been vacuumed (`reltuples < 0`,
         * PG 14+): ioguix joins pg_stats INNER, which silently drops an
         * unanalyzed column's width and inflates the bloat — the
         * direction that would trigger a rebuild.
         *
         * `size_bytes` is `relpages`, not `pg_relation_size`: the
         * estimate's `reltuples` and the size it is compared to must come
         * from the same VACUUM/ANALYZE, or an index that grew since would
         * read as bloated.
         *
         * The `'hog\_%'` escape is correct HERE because this is a raw
         * string; DatabaseHealthRepo's note on the two string forms
         * applies.
         */
        internal const val ESTIMATE_SQL: String =
            """
            WITH idx AS (
                SELECT t.relname AS table_name, c.relname AS index_name, n.nspname,
                       t.oid AS table_oid, c.oid AS index_oid,
                       c.reltuples, c.relpages, i.indnatts, i.indkey,
                       i.indisunique OR i.indisprimary AS constraint_backing,
                       COALESCE(substring(array_to_string(c.reloptions, ' ')
                                          FROM 'fillfactor=([0-9]+)')::int, 90) AS fillfactor
                  FROM pg_index i
                  JOIN pg_class c ON c.oid = i.indexrelid
                  JOIN pg_class t ON t.oid = i.indrelid
                  JOIN pg_namespace n ON n.oid = t.relnamespace
                  JOIN pg_am am ON am.oid = c.relam
                 WHERE am.amname = 'btree'
                   AND n.nspname = current_schema()
                   AND t.relname LIKE 'hog\_%'
                   AND i.indisvalid
            ),
            cols AS (
                SELECT idx.index_oid, s.null_frac, s.avg_width
                  FROM idx
                 CROSS JOIN LATERAL generate_series(0, idx.indnatts - 1) AS k(pos)
                  LEFT JOIN pg_attribute ta
                         ON idx.indkey[k.pos] <> 0
                        AND ta.attrelid = idx.table_oid AND ta.attnum = idx.indkey[k.pos]
                  LEFT JOIN pg_attribute ia
                         ON idx.indkey[k.pos] = 0
                        AND ia.attrelid = idx.index_oid AND ia.attnum = k.pos + 1
                  LEFT JOIN pg_stats s
                         ON s.schemaname = idx.nspname
                        AND NOT s.inherited
                        AND s.tablename = CASE WHEN idx.indkey[k.pos] <> 0
                                               THEN idx.table_name ELSE idx.index_name END
                        AND s.attname = COALESCE(ta.attname, ia.attname)
            ),
            widths AS (
                SELECT index_oid,
                       bool_and(avg_width IS NOT NULL) AS complete,
                       CASE WHEN max(COALESCE(null_frac, 0)) = 0 THEN 8 ELSE 12 END AS tuple_header,
                       sum((1 - COALESCE(null_frac, 0)) * COALESCE(avg_width, 0)) AS data_width
                  FROM cols
                 GROUP BY index_oid
            )
            SELECT idx.table_name, idx.index_name, idx.constraint_backing,
                   idx.relpages::bigint * current_setting('block_size')::bigint AS size_bytes,
                   CASE WHEN w.complete AND idx.reltuples >= 0 THEN
                       (1 + ceil(idx.reltuples / floor(
                           (current_setting('block_size')::numeric - 24 - 16) * idx.fillfactor
                           / (100 * (4 + ceil(w.tuple_header / 8.0) * 8 + ceil(w.data_width / 8.0) * 8))
                       )))::bigint * current_setting('block_size')::bigint
                   END AS expected_bytes
              FROM idx
              JOIN widths w ON w.index_oid = idx.index_oid
             ORDER BY idx.table_name, idx.index_name
            """

        /**
         * `retirement_pending`: some catalog has a dropped table whose
         * rows the retirement loop may delete now — RetirementService's
         * own predicate, so the two can never disagree about "pending".
         * Unbounded over catalogs on purpose: `EXISTS` stops at the first
         * hit, and the walk is `hog_table`'s dropped rows with one
         * `hog_data_file_live` descent each, once a day.
         */
        internal const val RETIREMENT_PENDING_SQL: String =
            """
            SELECT EXISTS (
                SELECT 1 FROM hog_table t
                JOIN hog_catalog c ON c.catalog_id = t.catalog_id
                WHERE ${RetirementService.ELIGIBLE_PREDICATE}
            )
            """

        /**
         * `purge_pending`: some catalog's LATEST expiry run left a
         * MASS deletion behind — the remaining count at its cap
         * (`ExpiryService.PURGE_REMAINING_CAP`, bound as `:cap`: "behind by
         * more than any sweep will catch up"), or a truncated purge whose
         * count is unknown (NULL: the count timed out, and NULL compared
         * with anything is not true, so an unguarded `> 0` let exactly the
         * worst case through).
         *
         * NOT `> 0`. A purge that routinely lags by a few pages deletes in
         * the steady state, and steady deletions are what vacuum's page
         * reuse keeps up with — they do not undo a rebuild. A mass,
         * scattered delete does: it leaves the rebuilt index as bloated
         * tomorrow as it was today. One descent of
         * `hog_maintenance_run_recent` per catalog.
         */
        internal const val PURGE_PENDING_SQL: String =
            """
            SELECT EXISTS (
                SELECT 1 FROM hog_catalog c
                CROSS JOIN LATERAL (
                    SELECT r.result FROM hog_maintenance_run r
                    WHERE r.catalog_id = c.catalog_id AND r.task = 'expiry'
                    ORDER BY r.run_id DESC LIMIT 1
                ) last
                WHERE (last.result ->> 'purge_remaining')::bigint >= :cap
                   OR (COALESCE((last.result ->> 'purge_truncated')::boolean, false)
                       AND last.result ->> 'purge_remaining' IS NULL)
            )
            """

        /**
         * `reindex_in_progress`, the half the single-flight lock cannot
         * see: ANY index build in THIS database — an operator's REINDEX, a
         * migration's CREATE INDEX CONCURRENTLY. Not filtered on `command`:
         * for a non-superuser (RDS's application role) every column but
         * pid/datid/datname of another role's row reads NULL, so an
         * operator's REINDEX as the master user would be invisible to a
         * `command LIKE 'REINDEX%'`. The view is cluster-wide, hence the
         * database filter.
         */
        internal const val INDEX_BUILD_IN_PROGRESS_SQL: String =
            """
            SELECT EXISTS (
                SELECT 1 FROM pg_stat_progress_create_index p
                WHERE p.datid = (SELECT oid FROM pg_database WHERE datname = current_database())
            )
            """

        /** The newest reindex row: what the daily gate decides on. */
        internal const val LAST_RUN_SQL: String =
            """
            SELECT started_at, status, result ->> 'skipped_reason' AS skipped_reason
              FROM hog_maintenance_run
             WHERE task = 'reindex'
             ORDER BY run_id DESC
             LIMIT 1
            """

        /**
         * The newest rebuild ATTEMPT per candidate index — rows carrying
         * `before_bytes`, so a skip that merely NAMES the index (too_large,
         * last_attempt_failed) does not count as a retry and clear the
         * exclusion. An index whose newest attempt failed is excluded until
         * that row ages out of the ledger's retention (7 days by default)
         * or an operator rebuilds it by hand. Bounded by the reindex rows
         * the ledger holds (one per catalog per day), read through
         * `hog_maintenance_run_task_history`.
         */
        internal const val LAST_ATTEMPT_SQL: String =
            """
            SELECT DISTINCT ON (r.result ->> 'index') r.result ->> 'index' AS idx, r.status
              FROM hog_maintenance_run r
             WHERE r.task = 'reindex'
               AND r.result ->> 'before_bytes' IS NOT NULL
               AND r.result ->> 'index' = ANY(:names)
             ORDER BY r.result ->> 'index', r.run_id DESC
            """

        /**
         * `migration_pending`, first half: a replica holds the migration
         * advisory lock (`Database.MIGRATION_LOCK_KEY`), i.e. is inside
         * `Database.migrate` right now. A single-bigint advisory key shows
         * in pg_locks as classid = high 32 bits, objid = low 32 bits,
         * objsubid = 1. Advisory locks are per database, hence the filter.
         */
        internal const val MIGRATION_LOCK_HELD_SQL: String =
            """
            SELECT EXISTS (
                SELECT 1 FROM pg_locks l
                JOIN pg_database d ON d.oid = l.database
                WHERE d.datname = current_database()
                  AND l.locktype = 'advisory'
                  AND l.granted
                  AND l.classid::bigint = :hi
                  AND l.objid::bigint = :lo
                  AND l.objsubid = 1
            )
            """

        /**
         * `migration_pending`, second half: a non-transactional migration
         * that half-applied leaves `success = false` in Flyway's history,
         * and every replica fails validate until `flyway repair`. A
         * rebuild on top of that is the wrong order of repairs.
         */
        internal const val FAILED_MIGRATION_SQL: String =
            "SELECT EXISTS (SELECT 1 FROM flyway_schema_history WHERE NOT success)"

        /**
         * The DROP statements for invalid REINDEX CONCURRENTLY leftovers on
         * this schema's hog_* tables. Postgres names them
         * `<index>_ccnew` (failed before the swap) or `<index>_ccold`
         * (failed after it), with a digit appended when the name is taken
         * — hence the regex. Only INVALID ones: a valid index with such a
         * name is somebody's, not a corpse.
         *
         * Built with `format('%I')` in SQL because an identifier cannot be
         * a bind parameter; the names come from `pg_class`, never from a
         * client (invariant 9 is about values). `CONCURRENTLY`, so the
         * drop takes SHARE UPDATE EXCLUSIVE and never blocks the commit
         * path; it therefore cannot run in a transaction block, which is
         * why each statement is executed alone on the autocommit session.
         */
        internal const val LEFTOVERS_SQL: String =
            """
            SELECT format('DROP INDEX CONCURRENTLY IF EXISTS %I.%I', n.nspname, c.relname)
              FROM pg_index i
              JOIN pg_class c ON c.oid = i.indexrelid
              JOIN pg_class t ON t.oid = i.indrelid
              JOIN pg_namespace n ON n.oid = t.relnamespace
             WHERE n.nspname = current_schema()
               AND t.relname LIKE 'hog\_%'
               AND NOT i.indisvalid
               AND c.relname ~ '_cc(new|old)[0-9]*$'
             ORDER BY c.relname
            """

        /** The rebuild, with the identifier quoted by Postgres (see [LEFTOVERS_SQL]). */
        internal const val REINDEX_DDL_SQL: String =
            "SELECT format('REINDEX INDEX CONCURRENTLY %I.%I', current_schema(), :index::text)"
    }
}
