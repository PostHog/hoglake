package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.RetirementResult
import com.posthog.hoglake.observability.Audit
import io.github.oshai.kotlinlogging.KotlinLogging

/**
 * Retirement on hoglite (RetirementService's port): the paced reclaim of
 * a DROPPED table's file rows once the drop has sunk under the snapshot
 * floor — no reader can pin those rows any more, so the rows go and
 * every object path joins the removal queue in the same transaction.
 *
 * THE ORDERING RULE, carried from bug hunt #16 and as load-bearing here
 * as there: a victim file's deletion-vector rows go FIRST, in the same
 * transaction, each queueing its own puffin path. The Postgres schema
 * enforced the order with an ON DELETE CASCADE the code had to outrun;
 * the tenant schema has no FK, so the two DELETEs here ARE the whole
 * mechanism — a DV left behind when its file's row goes is an object
 * referenced by nothing and reclaimed by nobody.
 *
 * What the port drops, and why: the per-catalog advisory locks and the
 * statement-bound ladder (a hoglite transaction serializes on the
 * tenant's single writer, so `convoyed` and `skipped_locked` are
 * always 0), and the adaptive batch RESIZE (#263's lesson is kept the
 * other way: the batch size is fixed, a failed batch rolls back whole
 * and the next run asks again). The queue ceiling check stays —
 * retirement's output is cleanup's input, on any backend.
 */
internal class HogliteRetirement(
    private val store: HogliteCatalogStore,
    private val batchSize: Int = DEFAULT_BATCH,
    private val queueCeiling: Long = DEFAULT_QUEUE_CEILING,
    private val runBudgetMs: Long = DEFAULT_RUN_BUDGET_MS,
) {
    private val log = KotlinLogging.logger {}

    init {
        require(batchSize > 0) { "retirement batch must be positive (got $batchSize)" }
    }

    fun runOnce(
        catalog: String,
        trigger: MaintenanceTrigger = MaintenanceTrigger.MANUAL,
    ): RetirementResult =
        store.recorded(catalog, MaintenanceTask.RETIREMENT, trigger) {
            runSweep(catalog)
        }

    fun runOnceAllCatalogs(): List<Pair<String, RetirementResult>> {
        // Shuffled per sweep (#455): no catalog is always first.
        val names = store.listCatalogNames().shuffled()
        val results = mutableListOf<Pair<String, RetirementResult>>()
        for (name in names) {
            try {
                results += name to runOnce(name, MaintenanceTrigger.LOOP)
            } catch (e: Exception) {
                log.error(e) { "retirement sweep failed for catalog '$name'; continuing" }
            }
            if (Thread.currentThread().isInterrupted) {
                throw InterruptedException("retirement sweep cancelled after catalog '$name'")
            }
        }
        return results
    }

    private fun runSweep(catalog: String): RetirementResult {
        val result =
            try {
                sweep(catalog)
            } catch (e: Throwable) {
                Audit.event(
                    "retirement",
                    catalog,
                    null,
                    outcome = "failure",
                    detail = e.message ?: e.javaClass.simpleName,
                )
                throw e
            }
        Audit.event(
            "retirement",
            catalog,
            null,
            outcome = if (result.skippedTables > 0) "invariant_violation" else "ok",
            detail =
                "tables=${result.tables} rows_retired=${result.rowsRetired} " +
                    "dvs_retired=${result.dvsRetired} paths_queued=${result.pathsQueued} " +
                    "batches=${result.batches} skipped_tables=${result.skippedTables} " +
                    "skipped_queue_full=${result.skippedQueueFull} " +
                    "tables_remaining=${result.tablesRemaining}",
        )
        return result
    }

    /** A dropped table that has sunk under the floor. */
    private data class Candidate(val tableId: Long, val droppedSnapshot: Long)

    private fun sweep(catalog: String): RetirementResult {
        val started = System.nanoTime()

        fun overBudget() = (System.nanoTime() - started) / 1_000_000 >= runBudgetMs

        // The ceiling, BEFORE any candidate work: when cleanup is this far
        // behind, another queued page is a worse failure than a skipped run.
        val queued =
            store.queryCatalog(
                catalog,
                "SELECT COUNT(*) FROM removals WHERE drained_at IS NULL",
            ).firstRow()!![0].asLong()
        if (queued >= queueCeiling) {
            log.warn {
                "retirement for catalog '$catalog' skipped: $queued undrained removal rows " +
                    "meet the ceiling $queueCeiling; the cleanup drain has to catch up first"
            }
            return RetirementResult(0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0)
        }

        val floor = store.meta(catalog).earliestSnapshotId
        val candidates =
            store.queryCatalog(
                catalog,
                "SELECT id, dropped_snapshot FROM tables " +
                    "WHERE dropped_snapshot IS NOT NULL AND dropped_snapshot <= ? " +
                    "ORDER BY id LIMIT $MAX_TABLES_PER_RUN",
                floor,
            ).rows.map { Candidate(it[0].asLong(), it[1].asLong()) }

        var tables = 0L
        var rowsRetired = 0L
        var dvsRetired = 0L
        var batches = 0L
        var skippedTables = 0L
        var tablesRemaining = 0L
        for (candidate in candidates) {
            if (overBudget()) break
            var tableWorked = false
            var tableStuck = false
            while (true) {
                if (overBudget()) break
                when (val outcome = retireBatch(catalog, candidate, floor)) {
                    is BatchOutcome.Retired -> {
                        rowsRetired += outcome.rows
                        dvsRetired += outcome.dvs
                        batches++
                        tableWorked = true
                        if (outcome.rows < batchSize) break // short batch: drained
                    }
                    BatchOutcome.Drained -> break
                    is BatchOutcome.Stuck -> {
                        skippedTables++
                        tableStuck = true
                        log.error {
                            "retirement batch for catalog '$catalog' table ${candidate.tableId} " +
                                "is stuck: ${outcome.reason}; skipping the table"
                        }
                        break
                    }
                    BatchOutcome.NotEligible -> break
                }
                if (Thread.currentThread().isInterrupted) break
            }
            if (tableWorked) tables++
            // A table still holding live rows after this run counts as
            // remaining, whether the budget stopped it or it is stuck.
            if (tableStuck || liveRows(catalog, candidate.tableId) > 0) tablesRemaining++
        }
        // Candidates past the page are remaining by construction.
        val unexamined =
            store.queryCatalog(
                catalog,
                "SELECT COUNT(*) FROM tables WHERE dropped_snapshot IS NOT NULL AND dropped_snapshot <= ?",
                floor,
            ).firstRow()!![0].asLong() - candidates.size
        return RetirementResult(
            tables = tables,
            rowsRetired = rowsRetired,
            dvsRetired = dvsRetired,
            pathsQueued = rowsRetired + dvsRetired,
            batches = batches,
            timeouts = 0,
            skippedTables = skippedTables,
            skippedQueueFull = 0,
            convoyed = 0,
            skippedLocked = 0,
            tablesRemaining = tablesRemaining + unexamined.coerceAtLeast(0),
        )
    }

    private fun liveRows(
        catalog: String,
        tableId: Long,
    ): Long =
        store.queryCatalog(
            catalog,
            "SELECT COUNT(*) FROM files WHERE table_id = ? AND end_snapshot IS NULL",
            tableId,
        ).firstRow()!![0].asLong()

    private sealed interface BatchOutcome {
        data class Retired(val rows: Long, val dvs: Long) : BatchOutcome

        data object Drained : BatchOutcome

        data class Stuck(val reason: String) : BatchOutcome

        data object NotEligible : BatchOutcome
    }

    /**
     * One batch: pick the live victims, then ONE transaction that queues
     * every DV path, deletes the DV rows, queues every file path, and
     * deletes the file rows — the same atomicity as the Postgres CTEs.
     * The floor is re-read per batch: a batch that lost its eligibility
     * (someone raised... the floor only rises; a RISEN floor can no
     * longer make a candidate ineligible, but expiry of the drop
     * snapshot itself is the guard the Postgres port re-checks, so the
     * re-read stays).
     */
    private fun retireBatch(
        catalog: String,
        candidate: Candidate,
        floorAtRead: Long,
    ): BatchOutcome {
        // Re-read the floor: a candidate read before expiry advanced
        // could otherwise retire rows a consumer still pins. (The floor
        // is monotone, so a fresh value can only make this SAFER — but
        // eligibility is candidate.droppedSnapshot <= floor, and the
        // candidate was selected under floorAtRead; re-checking costs one
        // read and keeps the predicate evaluated where the write lands.)
        val floor = store.meta(catalog).earliestSnapshotId
        if (candidate.droppedSnapshot > floor.coerceAtLeast(floorAtRead)) return BatchOutcome.NotEligible

        val victims =
            store.queryCatalog(
                catalog,
                "SELECT id, path FROM files " +
                    "WHERE table_id = ? AND end_snapshot IS NULL ORDER BY id LIMIT ?",
                candidate.tableId,
                batchSize,
            ).rows
        if (victims.isEmpty()) return BatchOutcome.Drained
        val ids = victims.map { it[0].asLong() }
        // The victim files' vectors — ALL of them, superseded ones
        // included (the #16 rule).
        val dvRows =
            store.queryCatalog(
                catalog,
                "SELECT delete_file_id, path, table_id FROM dvs " +
                    "WHERE data_file_id IN (${ids.joinToString(",") { "?" }})",
                *ids.toTypedArray(),
            ).rows
        val foreign = dvRows.count { it[2].asLong() != candidate.tableId }
        if (foreign > 0) {
            return BatchOutcome.Stuck(
                "$foreign deletion-vector rows on the batch belong to another table; refusing",
            )
        }
        val tx = HogliteTx()
        // The victim predicate rides every DELETE: a row that left the
        // set between the read and the write fails the batch whole. That
        // window is REAL here — the read and the tx are separate
        // requests; hoglite's serial writer only serializes the tx
        // itself — and these guards are what closes it.
        for (dv in dvRows) {
            tx.add(
                "INSERT INTO removals(path, file_kind, reason, queued_snapshot, queued_at) " +
                    "VALUES (?, 'delete', '$REASON', ?, ?)",
                dv[1].asString(),
                floor,
                HogliteSchema.now(),
            )
            tx.affectOne(
                "DELETE FROM dvs WHERE delete_file_id = ?",
                dv[0].asLong(),
            ) {
                com.posthog.hoglake.model.HoglakeException.CommitConflict(
                    "retirement DV row ${dv[0].asLong()} vanished mid-batch",
                )
            }
        }
        for (victim in victims) {
            tx.add(
                "INSERT INTO removals(path, file_kind, reason, queued_snapshot, queued_at) " +
                    "VALUES (?, 'data', '$REASON', ?, ?)",
                victim[1].asString(),
                floor,
                HogliteSchema.now(),
            )
            tx.affectOne(
                "DELETE FROM files WHERE id = ? AND table_id = ? AND end_snapshot IS NULL",
                victim[0].asLong(),
                candidate.tableId,
            ) {
                com.posthog.hoglake.model.HoglakeException.CommitConflict(
                    "retirement victim ${victim[0].asLong()} vanished mid-batch",
                )
            }
        }
        store.runTx(catalog, HogliteCatalogStore.freshTxid("retire-batch"), null, tx)
        return BatchOutcome.Retired(victims.size.toLong(), dvRows.size.toLong())
    }

    companion object {
        /** hog_file_removal.reason for a path a retirement batch queued. */
        const val REASON = "table_drop_gc"
        const val DEFAULT_BATCH = 1_000
        const val DEFAULT_QUEUE_CEILING = 100_000L
        const val DEFAULT_RUN_BUDGET_MS = 30_000L

        /** Bound on the candidate read; a fuller page just defers to the next run. */
        const val MAX_TABLES_PER_RUN = 1_000
    }
}
