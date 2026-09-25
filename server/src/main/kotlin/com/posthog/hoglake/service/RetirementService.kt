package com.posthog.hoglake.service

import com.posthog.hoglake.Database
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.RetirementResult
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.persistence.MaintenanceRunStore
import com.posthog.hoglake.persistence.Pg
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.jdbi.v3.core.statement.UnableToExecuteStatementException

/**
 * PACED RETIREMENT of dropped tables' file rows — the other half of the
 * O(columns) drop (#193).
 *
 * THE INVARIANT THIS LOOP EXISTS TO SERVE. No code path may treat a
 * dropped table's file rows as reachable data: the TABLE is the
 * authority, `hog_table.dropped_snapshot` is where it says so, and
 * `TableRepo.markDropped` writes that in three UPDATEs that size with
 * the table's COLUMNS. So a drop is milliseconds, and the rows it
 * orphaned are deleted here, later, in bounded batches, with every
 * object path queued for the cleanup drain. Ending three million file
 * rows inside the drop transaction was 44.6 s and 3.9 GB of WAL under
 * the per-catalog commit lock, and on gigahog-prod-us it was a drop
 * that could not complete at all.
 *
 * THE GATE IS THE EXPIRY FLOOR, and it is not negotiable. A table
 * dropped in snapshot D is still fully readable at every S < D (time
 * travel is unchanged by a drop), so its rows may only be DELETED once
 * no retained snapshot can ask for them:
 *
 *     dropped_snapshot <= hog_catalog.earliest_snapshot_id
 *
 * read inside each batch's transaction, under the commit lock, so a
 * floor that has not moved yet cannot be read as one that has.
 *
 * A CATALOG WITH NO RETENTION NEVER RETIRES ANYTHING. Its floor never
 * advances, so the gate never opens, so a dropped table's rows stay
 * forever. That is CORRECT — every snapshot below the drop is still a
 * legal read — and it is documented rather than worked around: an
 * operator who wants the storage back configures retention (or, on a
 * consumer-floored catalog, gets the consumer's offset moving).
 * `/verify`'s orphans check reports that population as an
 * informational count, never a violation, for exactly this reason.
 *
 * WHAT A BATCH COSTS, AND WHY EVERY KNOB IS A TIME KNOB. One batch is
 * one transaction holding the per-catalog commit lock.
 * `RetirementCostIntegrationTest` measures it on a fixture carrying the
 * production cascade ratio (9 `hog_file_column_stats` rows and 2
 * partition values per data file, 175-byte paths, PG 18.6). One run:
 *
 *   batch 2,000  ->   39 ms hold, 19.5 us/row, 2,949 B of WAL per row
 *   batch 4,000  ->   75 ms hold, 18.8 us/row, 2,908 B per row
 *   batch 8,000  ->  157 ms hold, 19.6 us/row, 2,902 B per row
 *
 * The TIME figure moves with the machine (19-24 us/row across runs
 * here); the WAL figure does not, because it is bytes rather than
 * scheduling. What the test ASSERTS, and what the design depends on, is
 * that both are FLAT in the batch size — a superlinear cost would make
 * the adaptive halving below useless.
 *
 * At gigahog-prod-us's `main.events_raw` (3,008,849 rows) that is
 * ~8.7 GB of WAL here and ~10.2 GB at the 3,389 B/row a second
 * fixture measured (full-page images depend on checkpoint timing and
 * `wal_compression`, not on the code — budget against the larger, and
 * remember it is RETIREMENT ALONE: cleanup's settles, the audit stream
 * and the post-retirement VACUUM are on top). At the default
 * 8,000 / 750 ms, ~376 batches and ~60 s of accumulated lock hold at
 * an 18% duty cycle. Read the hold as 160-250 ms depending on the
 * hardware (the design was sized at 31.6 us/row on a slower fixture,
 * and the default is chosen so that BOTH numbers stay inside a quarter
 * of the 30 s admission bound).
 *
 * THE ELAPSED TIME IS SET BY THE CLEANUP DRAIN, NOT BY THIS LOOP'S
 * KNOBS, and that is the arithmetic an operator actually needs. A
 * batch is ~160 ms of hold plus a 750 ms pause, and the PAUSE IS
 * CHARGED against the run budget because the budget is wall clock —
 * so a 60 s run gets through ~66 batches and retires ~528,000 rows.
 * At a 60 s interval plus a 60 s budget a run lands every ~120 s:
 * ~15.8M rows/hour of CAPACITY, against a drain that manages 600,000
 * paths/hour at the runbook's event settings and 4,000/h at the
 * standing ones.
 *
 * So retirement outruns cleanup by more than an order of magnitude,
 * and what paces a large retirement is [queueCeiling]: bursts of ~60 s
 * of holds, once per ~50 minutes of drain. Elapsed time for 3,008,849
 * rows is `queued paths / cleanup rate` — about five hours at the
 * event settings — and neither the interval nor the budget moves it.
 * The only lever is HOGLAKE_CLEANUP_BATCH / _INTERVAL_MS.
 *
 * Those are the numbers an operator tunes against, and they are why the
 * batch is bounded in ROWS while the RUN is bounded in WALL CLOCK: the
 * per-row cost is a property of the TABLE (a 200-column table has ~20x
 * the cascade per row), so a fixed row count cannot bound a hold on its
 * own — which is what the adaptive halving below is for.
 *
 * THE FIVE THINGS THE COMMIT LOCK SERIALIZES A BATCH AGAINST, stated
 * because taking that lock at all is a deliberate act (AGENT.md: the
 * per-catalog commit lock is for commits, and nothing takes it unless
 * it settles state inside a commit transaction — this does):
 *
 *  1. the COMMIT PATH's path-reuse guard, which 409s any registered
 *     path holding an undrained `hog_file_removal` row. This batch
 *     INSERTS those rows; unserialized, a commit could read the queue
 *     between the delete and the insert and register a path this batch
 *     is about to queue for deletion;
 *  2. the CLEANUP DRAIN's re-check and liveness check, which run under
 *     the same lock and ask "does any file row still name this path".
 *     Deleting the row and queueing the path must be one atomic step
 *     to that reader, or the drain can see the row gone and the queue
 *     entry absent — an object nothing will ever reclaim;
 *  3. EXPIRY's floor advance, which is the gate this batch reads. The
 *     floor is read inside the batch transaction, so it cannot move
 *     under the batch;
 *  4. COMPACTION's group commit, whose plan-to-commit re-verification
 *     runs under the lock and would otherwise be re-verifying rows
 *     this batch is deleting;
 *  5. DROP and atomic REPLACEMENT themselves, which mark the table and
 *     allocate a snapshot under the lock.
 *
 * A RETIREMENT CONNECTION IS NEVER IDLE IN TRANSACTION, which is why
 * `CleanupService`'s hold budget arithmetic does NOT apply here.
 * Cleanup derives `hold <= min(idle bound, admission bound)` because it
 * makes object-store calls INSIDE the transaction — a backend awaiting
 * an S3 response is idle in transaction and Postgres will kill it.
 * This batch issues nothing but SQL, so the only bound that can fire is
 * `statement_timeout`, and the batch sets its own,
 * transaction-locally: `min(session statement_timeout / 4, commit
 * admission / 2)`. Both halves matter — the first keeps a batch well
 * inside the session bound a pod's connection carries, the second keeps
 * one statement under half the window a foreground commit is willing to
 * queue for.
 *
 * ADAPTIVE, NEVER SPINNING. A batch cancelled by that bound rolls back
 * whole and halves the batch size FOR THAT TABLE for the rest of the
 * run, because "too big" is a fact about the table's cascade fan-out
 * and not about the config. A batch that selects rows and deletes none
 * ends the run for that table and is counted: the select and the
 * DELETEs name the same primary keys, so it cannot happen on a healthy
 * catalog, and the one thing a loop must never do about an impossible
 * state is repeat it.
 *
 * SINGLE FLIGHT PER CATALOG. A second maintainer skips a catalog
 * another one is retiring rather than queueing behind its holds
 * (`Locks.tryAcquireCatalogRetirementLock`, a SESSION lock held for the
 * whole run). Postgres's lock queue is FIFO so nobody starves, but with
 * W maintainers the foreground's p99 tax is `(W - 0.5) x hold` and the
 * work is idempotent, so waiting buys nothing.
 *
 * PACED AGAINST THE DRAIN. Retirement's output is cleanup's input —
 * one queued path per deleted row — and the drain is much the slower of
 * the two. A run declines to start when the catalog's undrained queue
 * is already over [queueCeiling], counted one count per RUN (about
 * 100k buffers at 3M queued rows) rather than one per batch.
 *
 * NO PER-PATH AUDIT. One summary event per run, and that is all: the
 * cleanup drain already emits one `file_deleted` per object, and three
 * million of those is the audit stream a retirement of
 * `main.events_raw` produces whatever this class does.
 */
class RetirementService(
    private val jdbi: Jdbi,
    private val batchSize: Int = DEFAULT_BATCH,
    private val pauseMs: Long = DEFAULT_PAUSE_MS,
    private val runBudgetMs: Long = DEFAULT_RUN_BUDGET_MS,
    private val queueCeiling: Long = DEFAULT_QUEUE_CEILING,
    private val commitLockTimeoutMs: Long = CommitService.DEFAULT_COMMIT_LOCK_TIMEOUT_MS,
    /**
     * Monotonic nanoseconds, injected so a test can spend the run
     * budget without spending the wall clock. Never `Instant.now()`:
     * the budget is an elapsed-time bound and a wall clock can go
     * backwards.
     */
    private val nanoTime: () -> Long = System::nanoTime,
    /** The inter-batch pause, injected so a test does not sleep. */
    private val sleep: (Long) -> Unit = { Thread.sleep(it) },
) {
    private val log = KotlinLogging.logger {}

    /** The run ledger; records after the run resolves, never inside it. */
    private val runStore = MaintenanceRunStore(jdbi)

    /**
     * The batch size the adaptive halving SETTLED ON, per table,
     * remembered across runs.
     *
     * Without this, `n` resets to [batchSize] at the top of every table
     * of every run, so a wide table pays its whole halving sequence —
     * each step a rolled-back transaction that held the commit lock for
     * its full statement bound — EVERY RUN, forever. On a table that
     * needs six halvings and takes fifty runs to drain, that is three
     * hundred pointless holds.
     *
     * DELIBERATELY IN MEMORY, and deliberately not in the ledger or a
     * column. It is a hint, not state: it is re-derived in at most a
     * handful of rollbacks if it is missing, it is per-process so two
     * maintainers do not have to agree on it, and persisting it would
     * mean a schema change and a staleness question (a table whose
     * schema narrowed should go back UP) for something the halving
     * already answers. It RESETS ON RESTART, which costs one run's
     * worth of rollbacks on the tables that need them.
     *
     * MONOTONE DOWN UNTIL RESTART. Nothing ever raises an entry: the
     * halving writes it and only a DRAIN removes it, so a table whose
     * per-row cost FELL — an `ALTER TABLE ... DROP COLUMN` that slims
     * the stats cascade, a partition spec removed — keeps the small
     * batch the wide schema earned until the process restarts. That is
     * a throughput cost and never a correctness one, it is bounded by
     * the deploy cadence, and the alternative (probing upward) would
     * pay a rolled-back lock hold to discover the schema changed. If it
     * ever matters, restart the maintenance pod.
     *
     * Keyed by (catalog, table). Bounded by the same thing the
     * candidate query is: only eligible tables ever enter it, and an
     * entry is removed when its table drains.
     */
    private val settledBatchSize = java.util.concurrent.ConcurrentHashMap<Pair<Long, Long>, Int>()

    init {
        require(batchSize > 0) { "retirement batch size must be positive (got $batchSize)" }
        require(pauseMs >= 0) { "retirement pause must not be negative (got $pauseMs)" }
        require(runBudgetMs > 0) { "retirement run budget must be positive (got $runBudgetMs)" }
        require(queueCeiling >= 0) { "retirement queue ceiling must not be negative (got $queueCeiling)" }
    }

    /**
     * The per-statement bound a batch sets transaction-locally.
     *
     * IT BOUNDS A STATEMENT; THE HOLD IS WHAT MATTERS, so the whole
     * hold is what it is derived from. A batch runs
     * [BOUNDED_STATEMENTS_PER_BATCH] statements between taking the lock
     * and committing — the floor read, the victim select, the DV
     * delete, the data delete — and `statement_timeout` applies to each
     * SEPARATELY. A bound of `admission / 2` per statement is therefore
     * a hold of `2 x admission` in the worst case, which is the
     * opposite of what the number was meant to promise. Dividing by the
     * count makes the arithmetic say what it claims:
     *
     *     per statement = min(session / 4, admission / 2) / statements
     *     whole hold   <= admission / 2
     *
     * At the defaults (session 60 s, admission 30 s, 4 statements)
     * that is 3.75 s each and at most 15 s of hold — half the window a
     * foreground commit is willing to queue for, leaving the other half
     * for the queue it is already in.
     *
     * Both terms are DERIVED rather than written down, so lowering
     * either moves this with it. A zero (unbounded) admission bound
     * drops that half of the minimum rather than collapsing the whole
     * expression to zero: `HOGLAKE_COMMIT_LOCK_TIMEOUT_MS=0` means "no
     * admission bound", not "no statement bound".
     *
     * Floored at 1 ms, because `SET statement_timeout = 0` means
     * UNLIMITED and a rounding-down to zero would silently remove the
     * bound this whole comment is about.
     */
    internal val callBoundMs: Long =
        run {
            val session = Database.SESSION_INIT_SQL_STATEMENT_TIMEOUT.toMillis() / 4
            val whole = if (commitLockTimeoutMs > 0) minOf(session, commitLockTimeoutMs / 2) else session
            maxOf(1, whole / BOUNDED_STATEMENTS_PER_BATCH)
        }

    /**
     * One retirement run for [catalog]. Every run is recorded in the
     * maintenance ledger — including the ones that retire nothing,
     * which is what makes "no dropped table is eligible yet"
     * distinguishable from "nothing is sweeping this catalog".
     */
    fun runOnce(
        catalog: String,
        trigger: MaintenanceTrigger = MaintenanceTrigger.MANUAL,
    ): RetirementResult =
        runStore.recorded(catalog, MaintenanceTask.RETIREMENT, trigger) {
            runSweep(catalog)
        }

    private fun runSweep(catalog: String): RetirementResult {
        val result =
            try {
                sweep(catalog)
            } catch (e: Throwable) {
                Audit.event("retirement", catalog, null, Audit.failureOutcome(e), e.message)
                throw e
            }
        Metrics.rowsRetired(catalog, result.rowsRetired + result.dvsRetired)
        Metrics.retirementBatches(catalog, result.batches)
        Metrics.retirementTimeouts(catalog, result.timeouts)
        Metrics.retirementConvoyed(catalog, result.convoyed)
        // A run that did nothing AND was not stopped by anything is the
        // steady state of a catalog with no eligible drop, and an
        // audit line per interval per catalog for that is noise. Every
        // other shape — work done, a table skipped, the queue ceiling,
        // the lock held elsewhere — is a fact worth a line.
        val quiet =
            result.rowsRetired == 0L && result.dvsRetired == 0L && result.timeouts == 0L &&
                result.skippedTables == 0L && result.skippedQueueFull == 0L &&
                result.skippedLocked == 0L && result.convoyed == 0L &&
                result.tablesRemaining == 0L
        if (quiet) {
            log.debug { "retirement sweep for catalog '$catalog': nothing eligible" }
        } else {
            Audit.event(
                "retirement",
                catalog,
                null,
                outcome = if (result.skippedTables > 0) "invariant_violation" else "ok",
                detail =
                    "tables=${result.tables} rows_retired=${result.rowsRetired} " +
                        "dvs_retired=${result.dvsRetired} paths_queued=${result.pathsQueued} " +
                        "batches=${result.batches} timeouts=${result.timeouts} " +
                        "skipped_tables=${result.skippedTables} " +
                        "skipped_queue_full=${result.skippedQueueFull} " +
                        "convoyed=${result.convoyed} " +
                        "skipped_locked=${result.skippedLocked} " +
                        "tables_remaining=${result.tablesRemaining}",
            )
        }
        return result
    }

    /** A dropped table that has sunk under the floor. */
    private data class Candidate(val tableId: Long, val droppedSnapshot: Long)

    /** What one batch transaction resolved to. */
    private sealed interface BatchOutcome {
        /** Rows went. [rows] data files, [dvs] deletion vectors. */
        data class Retired(val rows: Long, val dvs: Long) : BatchOutcome

        /** The table has no live file row left. */
        data object Drained : BatchOutcome

        /** The batch's own statement bound fired; nothing was written. */
        data object Timeout : BatchOutcome

        /** Rows were selected and none deleted — impossible on a healthy catalog. */
        data object Stuck : BatchOutcome

        /** The floor, re-read under the lock, no longer covers the drop. */
        data object NotEligible : BatchOutcome

        /** The commit lock could not be had inside the admission bound. */
        data object Convoyed : BatchOutcome
    }

    private fun sweep(catalog: String): RetirementResult {
        val catalogId = jdbi.withHandleUnchecked { h -> CatalogRepo.require(h, catalog).catalogId }
        val started = nanoTime()
        val empty =
            RetirementResult(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

        // The single-flight lock lives on ONE connection for the whole
        // run, so the handle is opened here and closed in the `finally`
        // that releases it. A session advisory lock outlives the
        // batches' transactions, which is the point: those come and go
        // hundreds of times inside one run.
        return jdbi.open().use { session ->
            if (!Locks.tryAcquireCatalogRetirementLock(session, catalogId)) {
                log.debug {
                    "retirement for catalog '$catalog' is already running elsewhere; skipping"
                }
                return@use empty.copy(skippedLocked = 1)
            }
            try {
                sweepLocked(catalog, catalogId, started)
            } finally {
                Locks.releaseCatalogRetirementLock(session, catalogId)
            }
        }
    }

    private fun sweepLocked(
        catalog: String,
        catalogId: Long,
        started: Long,
    ): RetirementResult {
        // ONE count per run. At three million undrained rows this is
        // ~100k buffers; per BATCH it would be that times the batch
        // count, which for a 3M-row table is 376 of them.
        //
        // An exact count is what the ceiling compares against, and at
        // this cadence it is affordable. If it ever is not, the cheap
        // form is `max(removal_id) - min(removal_id)` over the
        // undrained rows — two index probes on
        // `hog_file_removal_drain` — which OVERCOUNTS by the settled
        // rows inside the range and is therefore a safe bound for a
        // ceiling whose job is to stop early.
        val queued =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT count(*) FROM hog_file_removal
                    WHERE catalog_id = :catalogId AND drained_at IS NULL
                    """,
                )
                    .bind("catalogId", catalogId)
                    .mapTo(Long::class.java)
                    .one()
            }
        // ELIGIBILITY IS RECORDED BEFORE THE CEILING CAN STOP THE RUN,
        // and the order is the whole point.
        //
        // `retirement_eligible_at` is what `/verify`'s orphans arm dates
        // a leak from: a dropped table under the floor that STILL holds
        // live file rows is a violation only once it has been stamped
        // for longer than the grace. Stamping after the ceiling check
        // meant a catalog whose cleanup queue never drains — the exact
        // state an operator most needs told about — was never stamped,
        // so the arm could never fire and retirement wedged in SILENCE:
        // every run a no-op, every counter zero, and `/verify` green.
        //
        // Stamping first costs one UPDATE on a handful of rows and
        // turns that silence into an alert after the grace. The stamp
        // is not a promise that anything was retired; it is the
        // observation that the table BECAME retirable, which is true
        // whether or not this run got to it.
        val candidates = candidates(catalogId)
        if (candidates.isEmpty()) return RetirementResult(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        stampEligible(catalogId, candidates.map { it.tableId })

        if (queued > queueCeiling) {
            log.info {
                "retirement for catalog '$catalog' skipped: $queued undrained cleanup-queue rows " +
                    "exceed the ceiling of $queueCeiling; retiring more would grow a queue that " +
                    "is already not draining. ${candidates.size} eligible table(s) stamped, so " +
                    "/verify's orphans check will report them once they are past its grace"
            }
            return RetirementResult(
                0, 0, 0, 0, 0, 0, 0, 1, 0, 0,
                // The eligible tables ARE remaining work, and saying so
                // is what keeps a ceiling-skipped run out of the "quiet"
                // branch of the audit stream.
                candidates.size.toLong(),
            )
        }

        var tables = 0L
        var rows = 0L
        var dvs = 0L
        var batches = 0L
        var timeouts = 0L
        var stuck = 0L
        var convoyed = 0L
        var index = 0
        while (index < candidates.size) {
            if (budgetSpent(started)) break
            val candidate = candidates[index]
            val key = catalogId to candidate.tableId
            // Start from what the last run discovered for THIS table,
            // never from the configured batch: the rollbacks that found
            // it are not worth paying twice.
            var n = settledBatchSize[key] ?: batchSize
            var touched = false
            var done = false
            while (!done && !budgetSpent(started)) {
                when (val outcome = batch(catalogId, candidate, n)) {
                    is BatchOutcome.Retired -> {
                        rows += outcome.rows
                        dvs += outcome.dvs
                        batches++
                        touched = true
                        // THE DUTY CYCLE, and it is paid after EVERY
                        // committed batch including the last of a
                        // table. Skipping the trailing one would be
                        // wrong, not merely tidier: the next hold this
                        // pause protects is usually the NEXT TABLE's
                        // first batch, taken by this same run
                        // microseconds later, and on a catalog with
                        // many small dropped tables "skip the last one"
                        // degenerates to no pacing at all. The cost is
                        // one pause per run charged against the run
                        // budget, which the budget accounts for because
                        // it is measured in wall clock.
                        if (pauseMs > 0) sleep(pauseMs)
                    }

                    BatchOutcome.Drained -> {
                        // The table is finished: forget its remembered
                        // size so a future incarnation of the same
                        // table_id cannot inherit a bound discovered
                        // for a different schema.
                        settledBatchSize.remove(key)
                        done = true
                    }

                    BatchOutcome.Timeout -> {
                        timeouts++
                        val halved = maxOf(1, n / 2)
                        log.warn {
                            "retirement batch of $n rows on catalog '$catalog' table " +
                                "${candidate.tableId} hit its ${callBoundMs}ms statement bound and " +
                                "rolled back; halving to $halved for the rest of this run (a wide " +
                                "table's per-row cascade is what this bound is measuring)"
                        }
                        if (n == 1) {
                            // One row that cannot be deleted inside the
                            // bound is not a batch-size problem.
                            done = true
                        }
                        n = halved
                        settledBatchSize[key] = halved
                        // THE PAUSE IS PAID AFTER A TIMEOUT TOO. A
                        // rolled-back batch still HELD the lock for its
                        // whole statement bound before it was
                        // cancelled, so retrying immediately turns a
                        // halving sequence into a near-continuous hold
                        // — the duty cycle is about the lock, and the
                        // lock does not care whether the transaction
                        // committed.
                        if (pauseMs > 0) sleep(pauseMs)
                    }

                    BatchOutcome.Stuck -> {
                        stuck++
                        log.error {
                            "retirement batch on catalog '$catalog' table ${candidate.tableId} " +
                                "selected rows and deleted none; ending the run for this table " +
                                "rather than spinning. The victim select and the DELETEs name the " +
                                "same primary keys, so this is a concurrent writer on a dropped " +
                                "table or a broken cascade"
                        }
                        done = true
                    }

                    BatchOutcome.NotEligible -> {
                        log.debug {
                            "retirement skipped catalog '$catalog' table ${candidate.tableId}: the " +
                                "floor read under the lock no longer covers drop snapshot " +
                                "${candidate.droppedSnapshot}"
                        }
                        done = true
                    }

                    BatchOutcome.Convoyed -> {
                        log.info {
                            "retirement for catalog '$catalog' gave up the run: the commit lock was " +
                                "not available inside ${commitLockTimeoutMs}ms. The catalog is busy; " +
                                "the next interval continues"
                        }
                        // COUNTED SEPARATELY from `timeouts`, because
                        // the two ask for opposite remedies. A timeout
                        // says the BATCH is too big for the table and
                        // the answer is a smaller batch; a convoy says
                        // somebody else holds the catalog's commit lock
                        // and the answer is either to leave it alone or
                        // to look at what is holding it. Summed into one
                        // counter they would cancel each other out as a
                        // signal.
                        convoyed++
                        return tally(
                            tables + if (touched) 1 else 0,
                            rows,
                            dvs,
                            batches,
                            timeouts,
                            stuck,
                            convoyed,
                            remaining = candidates.size - index,
                        )
                    }
                }
            }
            if (touched) tables++
            // ONLY advance past a table the run actually FINISHED with.
            // A table the budget interrupted is still this catalog's
            // work, and counting it as reached would make a paced run
            // report `tables_remaining = 0` — indistinguishable from an
            // idle one, which is the reading the counter exists to
            // prevent.
            if (!done) break
            index++
        }
        return tally(tables, rows, dvs, batches, timeouts, stuck, convoyed, remaining = candidates.size - index)
    }

    private fun tally(
        tables: Long,
        rows: Long,
        dvs: Long,
        batches: Long,
        timeouts: Long,
        stuck: Long,
        convoyed: Long,
        remaining: Int,
    ) = RetirementResult(
        tables = tables,
        rowsRetired = rows,
        dvsRetired = dvs,
        pathsQueued = rows + dvs,
        batches = batches,
        timeouts = timeouts,
        skippedTables = stuck,
        skippedQueueFull = 0,
        skippedLocked = 0,
        convoyed = convoyed,
        tablesRemaining = remaining.toLong().coerceAtLeast(0),
    )

    private fun budgetSpent(started: Long): Boolean = (nanoTime() - started) / 1_000_000 >= runBudgetMs

    /**
     * Dropped tables whose drop snapshot has sunk to or below the
     * catalog's CURRENT expiry floor.
     *
     * `<=`, not `<`: `earliest_snapshot_id` is the oldest snapshot a
     * reader may still ask for, and a table dropped IN that snapshot is
     * already gone at it — the version row's `end_snapshot` equals the
     * drop, so invariant 6 makes it invisible at S = drop. Flipping
     * this to `<` would retire nothing until one more snapshot expired;
     * flipping the visibility predicate the other way would delete rows
     * a read at S = drop - 1 still needs. Both directions are pinned by
     * tests.
     */
    private fun candidates(catalogId: Long): List<Candidate> =
        jdbi.withHandleUnchecked { h ->
            h.createQuery(CANDIDATE_SQL)
                .bind("catalogId", catalogId)
                .bind("limit", MAX_TABLES_PER_RUN)
                .map { rs, _ -> Candidate(rs.getLong("table_id"), rs.getLong("dropped_snapshot")) }
                .list()
        }

    /**
     * Stamp `retirement_eligible_at` on first observation, once.
     *
     * The column is not read by this loop at all — it is read by
     * `/verify`'s orphans check, which calls a table leaked only after
     * it has been ELIGIBLE (not merely dropped) for several retirement
     * intervals. Without a first-observation timestamp that check would
     * have to date the leak from the DROP, which on a catalog whose
     * floor moves slowly is an alert on a system working exactly as
     * designed.
     *
     * `WHERE retirement_eligible_at IS NULL` is what makes it stamp
     * ONCE: a run that is interrupted and resumed, or a table that
     * takes fifty runs to retire, must keep the first observation.
     */
    private fun stampEligible(
        catalogId: Long,
        tableIds: List<Long>,
    ) {
        jdbi.withHandleUnchecked { h ->
            h.createUpdate(
                """
                UPDATE hog_table SET retirement_eligible_at = now()
                WHERE catalog_id = :catalogId
                  AND table_id = ANY(:tableIds)
                  AND retirement_eligible_at IS NULL
                """,
            )
                .bind("catalogId", catalogId)
                .bindArray("tableIds", Long::class.javaObjectType, tableIds)
                .execute()
        }
    }

    /**
     * ONE batch: one transaction, one hold of the per-catalog commit
     * lock, at most [n] data files and all of their deletion vectors.
     */
    private fun batch(
        catalogId: Long,
        candidate: Candidate,
        n: Int,
    ): BatchOutcome =
        try {
            jdbi.inTransactionUnchecked { h ->
                Locks.acquireCatalogCommitLock(h, catalogId, commitLockTimeoutMs)
                // THE BOUND GOES ON FIRST, before anything it has to
                // bound. It used to sit after the floor read, which
                // left that statement running under the SESSION's 60 s
                // bound — inside a transaction already holding the
                // commit lock, and therefore capable on its own of a
                // hold twice the 30 s admission window. Every statement
                // between the lock and the commit is bounded now, and
                // [callBoundMs] is sized so that all of them together
                // fit inside half the admission bound.
                h.createQuery("SELECT set_config('statement_timeout', ?, true)")
                    .bind(0, callBoundMs.toString())
                    .mapToMap()
                    .one()
                // The gate, re-read INSIDE the transaction that is about
                // to delete. Reading it in `candidates` alone would be
                // reading it outside the serialization that makes it
                // true: expiry advances the floor under this same lock,
                // and so does everything else that could move it.
                //
                // THE FLOOR IS MONOTONE, which is why this branch is
                // unreachable in a healthy system and why its test has
                // to hand-edit `earliest_snapshot_id` to reach it:
                // nothing in the server ever lowers it, so a table that
                // was eligible when `candidates` read it is still
                // eligible now. The re-read is here for the state the
                // server does NOT produce — an operator's repair, a
                // restore from a backup taken before the floor moved, a
                // future sweep that learns to move it back — because a
                // DELETE of rows a legal time-travel read still needs
                // is unrecoverable, and re-reading one bigint under a
                // lock this transaction already holds costs nothing.
                val floor =
                    h.createQuery("SELECT earliest_snapshot_id FROM hog_catalog WHERE catalog_id = :c")
                        .bind("c", catalogId)
                        .mapTo(Long::class.java)
                        .one()
                if (candidate.droppedSnapshot > floor) {
                    return@inTransactionUnchecked BatchOutcome.NotEligible
                }
                val victims =
                    h.createQuery(VICTIM_SELECT_SQL)
                        .bind("catalogId", catalogId)
                        .bind("tableId", candidate.tableId)
                        .bind("n", n)
                        .mapTo(Long::class.javaObjectType)
                        .list()
                if (victims.isEmpty()) return@inTransactionUnchecked BatchOutcome.Drained

                // DELETION VECTORS FIRST, AND WITHOUT AN `end_snapshot`
                // CLAUSE. Bug hunt #16's failure, carried forward: the
                // data-file DELETE below cascades `hog_delete_file`
                // (the FK is ON DELETE CASCADE), so any DV row still
                // present when it runs is taken away WITHOUT its path
                // being queued — the puffin object is then referenced by
                // nothing and reclaimed by nobody, forever. Restricting
                // this to live DVs would leave exactly the SUPERSEDED
                // ones to be cascaded away un-queued, which is the same
                // leak with a smaller population.
                val dvs =
                    h.createUpdate(DV_DELETE_SQL)
                        .bind("catalogId", catalogId)
                        .bindArray("victims", Long::class.javaObjectType, victims)
                        .execute()
                        .toLong()
                val rows =
                    h.createUpdate(DATA_DELETE_SQL)
                        .bind("catalogId", catalogId)
                        .bindArray("victims", Long::class.javaObjectType, victims)
                        .execute()
                        .toLong()
                if (rows == 0L) BatchOutcome.Stuck else BatchOutcome.Retired(rows, dvs)
            }
        } catch (e: HoglakeException.CommitQueueTimeout) {
            BatchOutcome.Convoyed
        } catch (e: UnableToExecuteStatementException) {
            if (Pg.isQueryCanceled(e)) BatchOutcome.Timeout else throw e
        }

    /**
     * One run across every catalog, for the background loop. Catalogs
     * are isolated: one catalog's failure is logged and the rest
     * proceed.
     */
    fun runOnceAllCatalogs(): List<Pair<String, RetirementResult>> {
        val names = jdbi.withHandleUnchecked { h -> CatalogRepo.listAll(h) }.map { it.name }
        val results = mutableListOf<Pair<String, RetirementResult>>()
        for (name in names) {
            try {
                results += name to runOnce(name, MaintenanceTrigger.LOOP)
            } catch (e: Exception) {
                log.error(e) { "retirement run failed for catalog '$name'; continuing" }
            }
        }
        return results
    }

    internal companion object {
        /** See Config.retirementBatch for where 8,000 comes from. */
        const val DEFAULT_BATCH = 8_000

        /** See Config.retirementPauseMs. */
        const val DEFAULT_PAUSE_MS = 750L

        /** See Config.retirementRunBudgetMs. */
        const val DEFAULT_RUN_BUDGET_MS = 60_000L

        /** See Config.retirementQueueCeiling. */
        const val DEFAULT_QUEUE_CEILING = 500_000L

        /** `hog_file_removal.reason` for a path a retirement batch queued. */
        const val REASON = "table_drop_gc"

        /**
         * Statements a batch runs under its own `statement_timeout`,
         * between taking the commit lock and committing: the floor
         * read, the victim select, the DV delete, the data delete.
         * [callBoundMs] divides by this so the whole HOLD, and not each
         * statement, is what the admission bound is compared against.
         *
         * The `SET LOCAL statement_timeout` itself is not counted: it
         * is a catalog write on an already-open transaction, and it is
         * what puts the bound in force rather than something the bound
         * applies to.
         */
        const val BOUNDED_STATEMENTS_PER_BATCH = 4

        /**
         * Eligible tables ONE run will look at, and therefore stamp.
         *
         * Not a throughput knob — the run budget is that — but a bound
         * on the two statements that are not per-batch: the candidate
         * read and the `retirement_eligible_at` stamp, both of which
         * take a list of table ids into the JVM. A Portola-shaped
         * catalog holds 54,000 tables, and a mass drop (a tenant
         * offboarded, a migration reversed) can make every one of them
         * eligible in the same sweep; without a bound, a run that the
         * budget will stop after a handful of tables still reads and
         * stamps all 54,000 first.
         *
         * Ordered by `table_id`, so the bound is a stable PREFIX rather
         * than an arbitrary sample: the same tables come back next run
         * and are finished before the loop moves on. 1,000 is far more
         * than a 60 s budget can retire and far less than a catalog can
         * hold, which is where a bound of this kind belongs.
         */
        const val MAX_TABLES_PER_RUN = 1_000

        /**
         * The eligibility query, `internal` so a test can EXPLAIN the
         * SQL production runs rather than a lookalike.
         *
         * THE `EXISTS` IS NOT AN OPTIMISATION, it is what makes this
         * query terminate as a SET. `hog_table` rows are NEVER DELETED
         * — the identity row is the lineage `replaced_table_id` walks
         * and the uuid a consumer's offset keys on — so a table that
         * retirement fully drained six months ago is still dropped,
         * still under the floor, and still matches every other clause
         * here. Without the probe:
         *
         *  - every run takes the per-catalog COMMIT LOCK once per
         *    historical drop, finds nothing, and calls it `Drained` —
         *    a silent no-op that costs a lock hold each;
         *  - and worse, `ORDER BY t.table_id LIMIT :limit` is a PREFIX,
         *    so once a catalog has accumulated
         *    [MAX_TABLES_PER_RUN] historical drops with low table ids
         *    the window is entirely drained tables and retirement never
         *    reaches live work again. It would stop, permanently,
         *    reporting zero and looking healthy.
         *
         * The probe asks `hog_data_file` alone. A `hog_delete_file` row
         * cannot outlive its data file — the FK is ON DELETE CASCADE —
         * so "no live data file" implies "nothing left to retire", and
         * a second EXISTS would be a second index probe per candidate
         * for an answer the first one already gives.
         *
         * It is driven by `hog_data_file_live`, the same partial index
         * the victim select uses, so it is one descent per candidate
         * rather than a scan.
         */
        internal const val CANDIDATE_SQL: String =
            """
            SELECT t.table_id, t.dropped_snapshot
            FROM hog_table t
            JOIN hog_catalog c ON c.catalog_id = t.catalog_id
            WHERE t.catalog_id = :catalogId
              AND t.dropped_snapshot IS NOT NULL
              AND t.dropped_snapshot <= c.earliest_snapshot_id
              AND EXISTS (
                  SELECT 1 FROM hog_data_file f
                  WHERE f.catalog_id = t.catalog_id
                    AND f.table_id = t.table_id
                    AND f.end_snapshot IS NULL)
            ORDER BY t.table_id
            LIMIT :limit
            """

        /**
         * The victim select, `internal` for the plan test.
         *
         * NO `FOR UPDATE`, and that is measured rather than assumed:
         * locking 8,000 rows costs ~50x the buffers because every row
         * lock DIRTIES its heap page, which then has to be written and
         * WAL-logged for a statement that is about to delete those rows
         * anyway. The DELETEs' own row locks are the fence, and the only
         * row-level writer that could otherwise touch a dropped table's
         * files is the hydrator — which no longer claims them at all.
         *
         * `ORDER BY table_id, begin_snapshot` is the `hog_data_file_live
         * (catalog_id, table_id, begin_snapshot) WHERE end_snapshot IS
         * NULL` index's own order with `catalog_id` and `table_id` both
         * equalities, so this is one ordered index scan that stops at
         * LIMIT: `Index Searches: 1`, no Bitmap, no Sort, and the work
         * sizes with the BATCH rather than with the table.
         */
        internal const val VICTIM_SELECT_SQL: String =
            """
            SELECT data_file_id FROM hog_data_file
            WHERE catalog_id = :catalogId AND table_id = :tableId AND end_snapshot IS NULL
            ORDER BY table_id, begin_snapshot
            LIMIT :n
            """

        /**
         * The DV arm, `internal` for the plan test.
         *
         * NO `end_snapshot` CLAUSE, and this is the load-bearing
         * omission rather than an oversight. `hog_delete_file`'s FK to
         * `hog_data_file` is ON DELETE CASCADE, so any vector still
         * present when [DATA_DELETE_SQL] runs is taken away WITHOUT its
         * path being queued — the puffin object is then referenced by
         * nothing and reclaimed by nobody, forever. Restricting this to
         * LIVE vectors would leave exactly the superseded ones to be
         * cascaded away un-queued: bug hunt #16's failure, one layer
         * down and harder to see, because the row vanishes instead of
         * lingering. `DropDvLifecycleIntegrationTest` seeds a
         * superseded vector for that reason.
         *
         * It runs BEFORE the data-file delete, in the same transaction,
         * for the same reason.
         *
         * `data_file_id = ANY(:victims)` is served by
         * `hog_delete_file_data_lookup` (V2, non-partial on
         * `(catalog_id, data_file_id)`) — the partial
         * `one_live_per_data_file` index could not serve a statement
         * that carries no `end_snapshot` predicate.
         */
        internal const val DV_DELETE_SQL: String =
            """
            WITH doomed AS (
                DELETE FROM hog_delete_file
                WHERE catalog_id = :catalogId AND data_file_id = ANY(:victims)
                RETURNING path
            )
            INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
            SELECT :catalogId, path, 'delete', '$REASON' FROM doomed
            """

        /**
         * The data-file arm, `internal` for the plan test.
         *
         * The stats and partition-value rows go with it, through the FK
         * cascades that already exist; a set-based pre-delete of them
         * was measured SLOWER than letting the triggers run, because it
         * reads the same rows twice.
         *
         * THREE CASCADES FIRE PER DELETED ROW, and all three are
         * index-driven, which is what keeps a batch's cost flat in the
         * size of the relations rather than in the size of the batch
         * times them: `hog_file_column_stats` and
         * `hog_file_partition_value` on their primary keys, and
         * `hog_delete_file` on `hog_delete_file_data_lookup`
         * (V2__maintenance.sql, non-partial on `(catalog_id,
         * data_file_id)`). That last one is the non-obvious of the
         * three — the partial `one_live_per_data_file` index could not
         * serve an RI query that carries no `end_snapshot` predicate —
         * and `RetirementCostIntegrationTest` asks the planner rather
         * than taking the schema's word for it: measured flat at 42 ms
         * per 2,000-row batch against an empty hog_delete_file and
         * 48 ms against 100,000 rows in 2,128 heap pages.
         */
        internal const val DATA_DELETE_SQL: String =
            """
            WITH doomed AS (
                DELETE FROM hog_data_file
                WHERE catalog_id = :catalogId AND data_file_id = ANY(:victims)
                RETURNING path
            )
            INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
            SELECT :catalogId, path, 'data', '$REASON' FROM doomed
            """
    }
}
