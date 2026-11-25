package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.RetirementResult
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.persistence.PartialResult
import com.posthog.hoglake.service.RetirementBackend

/** Retire live file rows after their table's drop reaches the retention floor. */
internal class HogliteRetirement(
    private val store: HogliteCatalogStore,
    internal val batchSize: Int = DEFAULT_BATCH,
    internal val queueCeiling: Long = DEFAULT_QUEUE_CEILING,
    internal val runBudgetMs: Long = DEFAULT_RUN_BUDGET_MS,
) : RetirementBackend {
    init {
        require(batchSize in 1..DEFAULT_BATCH) { "retirement batch must be between 1 and $DEFAULT_BATCH" }
        require(queueCeiling >= 0) { "retirement queue ceiling must not be negative" }
        require(runBudgetMs >= 0) { "retirement run budget must not be negative" }
    }

    override fun runOnce(
        catalog: String,
        trigger: MaintenanceTrigger,
    ): RetirementResult = runUntil(catalog, trigger, System.nanoTime() + runBudgetMs * 1_000_000, runBudgetMs > 0)

    /**
     * One run that starts no batch after [deadline] (a `System.nanoTime()` instant), except an [owed]
     * one, which it attempts however long its reads took and however late it started. A batch that
     * commits no rows (a stuck or emptied table) does not pay that debt: the run tries the next table,
     * starting none more than half of [OVERRUN_MS] past the deadline or its start, so one stuck table
     * cannot take every run's only batch. Every backend call is bounded too: work by [OVERRUN_MS]
     * past the deadline, then the backlog count by half of [BOOKKEEPING_MS] after that and the
     * ledger write by the rest,
     * so one run ends within [OVERRUN_MS] + [BOOKKEEPING_MS] of the deadline, or of its start
     * if that is later, and a stalled count cannot cost the run its ledger row.
     */
    internal fun runUntil(
        catalog: String,
        trigger: MaintenanceTrigger,
        deadline: Long,
        owed: Boolean,
    ): RetirementResult {
        val started = System.nanoTime()
        val late = if (deadline - started > 0) deadline else started
        val workEnd = late + OVERRUN_MS * 1_000_000
        val countEnd = workEnd + BOOKKEEPING_MS / 2 * 1_000_000
        return HogliteClient.withDeadline(workEnd + BOOKKEEPING_MS * 1_000_000) {
            store.recorded(catalog, MaintenanceTask.RETIREMENT, trigger) {
                val tally = Tally()
                val budget = Budget(deadline, late + OVERRUN_MS / 2 * 1_000_000, owed)
                try {
                    sweep(catalog, tally, budget, workEnd, countEnd).also { audit(catalog, it, "ok") }
                } catch (e: Throwable) {
                    // Refresh the backlog the run leaves, once it has read the floor that defines it.
                    // A failed probe must not replace the task error.
                    tally.floor?.let { floor ->
                        runCatching {
                            tally.remaining = remaining(catalog, floor, countEnd)
                        }.onFailure { secondary ->
                            if (secondary !== e) e.addSuppressed(secondary)
                        }
                    }
                    audit(catalog, tally.result(), "failure")
                    if (tally.batches > 0) throw RetirementFailed(e, tally.result())
                    throw e
                }
            }
        }
    }

    /** The backlog the run leaves, counted by [countEnd] so the ledger write keeps its own time. */
    private fun remaining(
        catalog: String,
        floor: Long,
        countEnd: Long,
    ) = HogliteClient.withDeadline(countEnd) {
        store.queryCatalog(catalog, REMAINING_SQL, floor).firstRow()!![0].asLong()
    }

    private class RetirementFailed(cause: Throwable, override val partial: RetirementResult) :
        RuntimeException(cause.message, cause), PartialResult

    private class Tally {
        val tables = mutableSetOf<Long>()
        var rows = 0L
        var dvs = 0L
        var paths = 0L
        var batches = 0L
        var skipped = 0L
        var full = 0L
        var remaining = 0L
        var floor: Long? = null

        fun result() =
            RetirementResult(tables.size.toLong(), rows, dvs, paths, batches, 0, skipped, full, 0, 0, remaining)
    }

    private fun audit(
        catalog: String,
        result: RetirementResult,
        outcome: String,
    ) {
        Audit.event(
            "retirement",
            catalog,
            null,
            outcome = if (outcome == "ok" && result.skippedTables > 0) "invariant_violation" else outcome,
            detail =
                "tables=${result.tables} rows_retired=${result.rowsRetired} dvs_retired=${result.dvsRetired} " +
                    "paths_queued=${result.pathsQueued} batches=${result.batches} " +
                    "skipped_tables=${result.skippedTables} " +
                    "skipped_queue_full=${result.skippedQueueFull} tables_remaining=${result.tablesRemaining}",
        )
    }

    /**
     * Owed one batch until one commits rows, so neither reads that use the whole budget nor a stuck
     * table can starve every run. Once one is attempted, the debt starts no batch after [owedUntil].
     */
    private class Budget(private val deadline: Long, private val owedUntil: Long, var owed: Boolean) {
        var attempted = false

        fun over() = System.nanoTime() - (if (owed) owedUntil else deadline) >= 0 && (!owed || attempted)
    }

    private fun sweep(
        catalog: String,
        tally: Tally,
        budget: Budget,
        workEnd: Long,
        countEnd: Long,
    ): RetirementResult {
        // The exact backlog is counted after the work, not before it: on the work path it spent the budget.
        HogliteClient.withDeadline(workEnd) { work(catalog, tally, budget) }
        tally.remaining = remaining(catalog, tally.floor!!, countEnd)
        return tally.result()
    }

    private fun work(
        catalog: String,
        tally: Tally,
        budget: Budget,
    ) {
        fun interrupted() {
            if (Thread.currentThread().isInterrupted) throw InterruptedException("retirement cancelled")
        }
        val floor = store.meta(catalog).earliestSnapshotId
        tally.floor = floor
        var cursor = 0L
        while (true) {
            interrupted()
            if (budget.over()) break
            // Saturates at the ceiling: only "is there room" matters here.
            val queued =
                store.queryCatalog(
                    catalog,
                    "SELECT COUNT(*) FROM (SELECT 1 FROM removals WHERE drained_at IS NULL LIMIT ?)",
                    queueCeiling,
                ).firstRow()!![0].asLong()
            if (queued >= queueCeiling) {
                tally.full = 1
                break
            }
            val candidates =
                store.queryCatalog(
                    catalog,
                    CANDIDATES_SQL.replace("ORDER BY t.id", "AND t.id > ? ORDER BY t.id"),
                    floor,
                    cursor,
                ).rows
            if (candidates.isEmpty()) break
            for (candidate in candidates) {
                interrupted()
                if (budget.over()) break
                val tableId = candidate[0].asLong()
                cursor = tableId
                var after = FileCursor(-1, 0)
                while (!budget.over()) {
                    interrupted()
                    budget.attempted = true
                    val outcome = retireBatch(catalog, tableId, candidate[1].asLong(), after)
                    if (outcome.stuck) {
                        tally.skipped++
                        break
                    }
                    if (outcome.full) {
                        tally.full = 1
                        break
                    }
                    after = outcome.next ?: break
                    if (outcome.rows == 0L) continue
                    budget.owed = false
                    tally.tables += tableId
                    tally.rows += outcome.rows
                    tally.dvs += outcome.dvs
                    tally.paths += outcome.paths
                    tally.batches++
                }
                if (tally.full > 0) break
            }
            if (tally.full > 0 || candidates.size < CANDIDATE_PAGE_SIZE) break
        }
    }

    private data class FileCursor(val rowIdStart: Long, val fileId: Long)

    private data class Batch(
        val rows: Long = 0,
        val dvs: Long = 0,
        val paths: Long = 0,
        val stuck: Boolean = false,
        val full: Boolean = false,
        val next: FileCursor? = null,
    )

    /** Queue current paths and remove their rows in one transaction. Delete all vectors before their files. */
    private fun retireBatch(
        catalog: String,
        tableId: Long,
        droppedSnapshot: Long,
        after: FileCursor,
    ): Batch {
        val victimsAtRead =
            store.queryCatalog(
                catalog,
                VICTIMS_SQL,
                tableId,
                after.rowIdStart,
                after.fileId,
                batchSize,
            ).rows
        if (victimsAtRead.isEmpty()) return Batch()
        val ids = victimsAtRead.map { it[0].asLong() }
        val last = victimsAtRead.last()
        val next = FileCursor(last[1].asLong(), last[0].asLong())
        // Only integer IDs enter this SQL. Recheck every victim in the transaction, not in the planning read.
        val victims =
            "SELECT id FROM files WHERE table_id = $tableId AND end_snapshot IS NULL " +
                "AND id IN (${ids.joinToString(",")})"
        val tx = HogliteTx()
        tx.expectOne(
            "SELECT COUNT(*) FROM tables t JOIN meta m ON m.id = 1 " +
                "WHERE t.id = ? AND t.dropped_snapshot = ? AND t.dropped_snapshot <= m.earliest_snapshot_id",
            tableId,
            droppedSnapshot,
        ) { HoglakeException.CommitConflict("retirement table $tableId is no longer eligible") }
        tx.expectZero(
            "SELECT CASE WHEN COUNT(*) >= ? THEN 1 ELSE 0 END FROM removals WHERE drained_at IS NULL",
            queueCeiling,
        ) { HoglakeException.CommitConflict(QUEUE_FULL) }
        tx.expectZero(
            "SELECT COUNT(*) FROM dvs WHERE data_file_id IN ($victims) AND table_id != ?",
            tableId,
        ) { HoglakeException.CommitConflict(FOREIGN_VECTORS) }
        tx.add(
            "INSERT INTO removals(path, file_kind, reason, queued_snapshot, queued_at) " +
                "SELECT path, 'delete', '$REASON', (SELECT earliest_snapshot_id FROM meta WHERE id = 1), ? " +
                "FROM dvs WHERE data_file_id IN ($victims)",
            HogliteSchema.now(),
        )
        val queuedDvs = tx.add("SELECT changes()")
        tx.add("DELETE FROM dvs WHERE data_file_id IN ($victims)")
        val deletedDvs = tx.add("SELECT changes()")
        tx.add(
            "INSERT INTO removals(path, file_kind, reason, queued_snapshot, queued_at) " +
                "SELECT path, 'data', '$REASON', (SELECT earliest_snapshot_id FROM meta WHERE id = 1), ? " +
                "FROM files WHERE id IN ($victims)",
            HogliteSchema.now(),
        )
        val queuedFiles = tx.add("SELECT changes()")
        tx.add("DELETE FROM files WHERE id IN ($victims)")
        val deletedFiles = tx.add("SELECT changes()")
        val result =
            try {
                store.runTx(catalog, HogliteCatalogStore.freshTxid("retire-batch"), null, tx)
            } catch (e: HoglakeException.CommitConflict) {
                return when (e.message) {
                    FOREIGN_VECTORS -> Batch(stuck = true)
                    QUEUE_FULL -> Batch(full = true)
                    else -> throw e
                }
            }

        fun count(index: Int) = result[index].firstRow()!![0].asLong()
        return Batch(count(deletedFiles), count(deletedDvs), count(queuedDvs) + count(queuedFiles), next = next)
    }

    companion object {
        const val REASON = "table_drop_gc"
        const val DEFAULT_BATCH = 1_000
        const val DEFAULT_QUEUE_CEILING = 100_000L
        const val DEFAULT_RUN_BUDGET_MS = 30_000L

        /** How long past its deadline a run's work calls may still answer: the batch in flight, or the owed one. */
        const val OVERRUN_MS = 2_000L

        /** What the backlog count and the ledger write get after the work, half each. */
        const val BOOKKEEPING_MS = 1_000L
        const val CANDIDATE_PAGE_SIZE = 1_000
        private const val FOREIGN_VECTORS = "retirement found deletion vectors from another table"
        private const val QUEUE_FULL = "retirement removal queue is full"
        internal const val VICTIMS_SQL =
            "SELECT id, row_id_start FROM files WHERE table_id = ? AND (row_id_start, id) > (?, ?) " +
                "AND end_snapshot IS NULL ORDER BY row_id_start, id LIMIT ?"
        internal const val ELIGIBLE_PREDICATE =
            "t.dropped_snapshot IS NOT NULL AND t.dropped_snapshot <= ? " +
                "AND EXISTS (SELECT 1 FROM files f WHERE f.table_id = t.id AND f.end_snapshot IS NULL)"
        internal const val CANDIDATES_SQL =
            "SELECT t.id, t.dropped_snapshot FROM tables t WHERE $ELIGIBLE_PREDICATE " +
                "ORDER BY t.id LIMIT $CANDIDATE_PAGE_SIZE"
        internal const val REMAINING_SQL = "SELECT COUNT(*) FROM tables t WHERE $ELIGIBLE_PREDICATE"
    }
}
