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
    private val batchSize: Int = DEFAULT_BATCH,
    private val queueCeiling: Long = DEFAULT_QUEUE_CEILING,
    private val runBudgetMs: Long = DEFAULT_RUN_BUDGET_MS,
) : RetirementBackend {
    init {
        require(batchSize in 1..DEFAULT_BATCH) { "retirement batch must be between 1 and $DEFAULT_BATCH" }
        require(queueCeiling >= 0) { "retirement queue ceiling must not be negative" }
        require(runBudgetMs >= 0) { "retirement run budget must not be negative" }
    }

    override fun runOnce(
        catalog: String,
        trigger: MaintenanceTrigger,
    ): RetirementResult =
        store.recorded(catalog, MaintenanceTask.RETIREMENT, trigger) {
            val tally = Tally()
            try {
                sweep(catalog, tally).also { audit(catalog, it, "ok") }
            } catch (e: Throwable) {
                if (tally.batches > 0) {
                    // Refresh partial backlog after committed batches. A failed probe must not replace the task error.
                    runCatching {
                        tally.remaining = remaining(catalog, tally.floor)
                    }.onFailure { secondary ->
                        if (secondary !== e) e.addSuppressed(secondary)
                    }
                }
                audit(catalog, tally.result(), "failure")
                if (tally.batches > 0) throw RetirementFailed(e, tally.result())
                throw e
            }
        }

    private fun remaining(
        catalog: String,
        floor: Long,
    ) = store.queryCatalog(catalog, REMAINING_SQL, floor).firstRow()!![0].asLong()

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
        var floor = 0L

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

    private fun sweep(
        catalog: String,
        tally: Tally,
    ): RetirementResult {
        val started = System.nanoTime()

        fun overBudget() = (System.nanoTime() - started) / 1_000_000 >= runBudgetMs

        fun interrupted() {
            if (Thread.currentThread().isInterrupted) throw InterruptedException("retirement cancelled")
        }
        val floor = store.meta(catalog).earliestSnapshotId
        tally.floor = floor
        tally.remaining = store.queryCatalog(catalog, REMAINING_SQL, floor).firstRow()!![0].asLong()
        var cursor = 0L
        while (true) {
            interrupted()
            if (overBudget()) break
            val queued =
                store.queryCatalog(
                    catalog,
                    "SELECT COUNT(*) FROM removals WHERE drained_at IS NULL",
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
                if (overBudget()) break
                val tableId = candidate[0].asLong()
                cursor = tableId
                var after = FileCursor(-1, 0)
                while (!overBudget()) {
                    interrupted()
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
        tally.remaining = store.queryCatalog(catalog, REMAINING_SQL, floor).firstRow()!![0].asLong()
        return tally.result()
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
