package com.posthog.hoglake.hoglite

import com.posthog.hoglake.Config
import com.posthog.hoglake.model.CleanupResult
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.persistence.PartialResult
import com.posthog.hoglake.service.CleanupBackend
import com.posthog.hoglake.service.CleanupService
import com.posthog.hoglake.service.RemovalStore
import io.github.oshai.kotlinlogging.KotlinLogging
import java.time.Instant
import java.util.UUID

/**
 * Drain one tenant's removal queue with the server's shared object store.
 * Claims and settlement use separate transactions. No transaction spans object I/O.
 * Each arm has its own row budget. Staging tickets also require the grace interval.
 * Hygiene keeps PostgreSQL's phase order and failure contracts.
 * The caller owns [removalStore]; a request never closes it.
 */
internal class HogliteCleanup(
    private val store: HogliteCatalogStore,
    private val removalStore: RemovalStore,
    private val removalLedgerRetentionSeconds: Long,
    private val maintenanceLedgerRetentionSeconds: Long,
    private val receiptRetentionSeconds: Long,
    private val receiptPurgePage: Int,
    private val receiptPurgeBudgetMs: Long,
    private val claimLeaseSeconds: Long,
    private val stagingGraceSeconds: Long,
    private val subBatchSize: Int,
    private val stagingSubBatch: Int,
    private val workerIdPrefix: String,
) : CleanupBackend {
    /** The production wiring, from [Config] — CleanupService's rule. */
    constructor(cfg: Config, store: HogliteCatalogStore, removalStore: RemovalStore) : this(
        store = store,
        removalStore = removalStore,
        removalLedgerRetentionSeconds = cfg.removalLedgerRetentionSeconds,
        maintenanceLedgerRetentionSeconds = cfg.maintenanceLedgerRetentionSeconds,
        receiptRetentionSeconds = cfg.receiptRetentionSeconds,
        receiptPurgePage = RECEIPT_PURGE_PAGE,
        receiptPurgeBudgetMs = RECEIPT_PURGE_BUDGET_MS,
        claimLeaseSeconds = cfg.cleanupClaimLeaseSeconds,
        stagingGraceSeconds = cfg.cleanupStagingGraceSeconds,
        subBatchSize = cfg.cleanupSubBatchSize,
        stagingSubBatch = STAGING_SUB_BATCH,
        workerIdPrefix = CleanupService.defaultWorkerIdPrefix(),
    )

    private val log = KotlinLogging.logger {}

    private data class Entry(val removalId: Long, val path: String, val reason: String?)

    private data class Tally(
        val removed: Long = 0,
        val missing: Long = 0,
        val stillReferenced: Long = 0,
        val objectsRemoved: Long = 0,
        val settledElsewhere: Long = 0,
    ) {
        operator fun plus(o: Tally) =
            Tally(
                removed + o.removed,
                missing + o.missing,
                stillReferenced + o.stillReferenced,
                objectsRemoved + o.objectsRemoved,
                settledElsewhere + o.settledElsewhere,
            )
    }

    override fun runOnce(
        catalog: String,
        batchSize: Int,
        trigger: MaintenanceTrigger,
    ): CleanupResult =
        store.recorded(catalog, MaintenanceTask.CLEANUP, trigger) {
            doRunOnce(catalog, batchSize).also { Metrics.filesRemoved(catalog, it.objectsRemoved) }
        }

    private fun doRunOnce(
        catalog: String,
        batchSize: Int,
    ): CleanupResult {
        if (batchSize <= 0) {
            throw HoglakeException.Validation("batch size must be positive (got $batchSize)")
        }
        val m = store.meta(catalog)
        val worker = "$workerIdPrefix:${UUID.randomUUID()}"
        val objects = removalStore.forCatalog(catalog, m.dataPath)
        val handled = mutableSetOf<Long>()
        var total = Tally()
        // ONE BUDGET PER ARM — CleanupService.drainWorker's rule.
        val budgets = mutableMapOf(false to batchSize, true to batchSize)
        val afterIds = mutableMapOf(false to 0L, true to 0L)
        while (budgets.values.any { it > 0 }) {
            var progressed = false
            for (staging in listOf(false, true)) {
                val budget = budgets.getValue(staging)
                val want = minOf(budget, if (staging) stagingSubBatch else subBatchSize)
                if (want <= 0) continue
                val claimed = claim(catalog, worker, want, staging, afterIds.getValue(staging))
                if (claimed.isEmpty()) {
                    budgets[staging] = 0
                    continue
                }
                // Released rows wait for the next run instead of hiding higher queue ids.
                afterIds[staging] = claimed.maxOf { it.removalId }
                budgets[staging] = budget - claimed.size
                if (claimed.size == want) progressed = true
                val (fresh, again) = claimed.partition { handled.add(it.removalId) }
                if (again.isNotEmpty()) {
                    releaseClaims(catalog, worker, again.map { it.removalId })
                }
                if (fresh.isNotEmpty()) {
                    total += drainSubBatch(catalog, worker, fresh, probeEachPath = staging, objects = objects)
                }
            }
            if (!progressed) break
        }

        // These are independent hygiene phases, in the existing order. A ledger
        // refusal must not starve the other purges, but must still fail this run.
        val ledgerFailure =
            try {
                purgeMaintenanceLedger(catalog)
                null
            } catch (e: Exception) {
                requireOrdinaryPurgeFailure(catalog, e)
                e
            }
        val receipts =
            try {
                val receipts =
                    try {
                        purgeCommitReceipts(catalog, m.retentionSeconds)
                    } catch (e: Exception) {
                        requireOrdinaryPurgeFailure(catalog, e)
                        log.warn(e) {
                            "cleanup: the commit-receipt retention purge failed for catalog '$catalog'; " +
                                "the drain itself is unaffected and the next run retries"
                        }
                        ReceiptPurge(failures = 1)
                    }
                Metrics.commitReceiptPurgeFailures(catalog, receipts.failures)
                val drainedFailure = purgeDrainedLedger(catalog)
                if (ledgerFailure != null && drainedFailure != null) {
                    suppressOnce(ledgerFailure, drainedFailure)
                }
                receipts
            } catch (e: Throwable) {
                if (ledgerFailure == null) throw e
                // Shutdown/fatal signals escape immediately, never another phase.
                if (e !is Exception || terminalPurgeFailure(e)) {
                    suppressOnce(e, ledgerFailure)
                    throw e
                }
                suppressOnce(ledgerFailure, e)
                throw ledgerFailure
            }
        if (ledgerFailure != null) {
            throw if (ledgerFailure is HoglakeException) {
                ledgerFailure
            } else {
                HygieneFailed(
                    ledgerFailure,
                    cleanupResult(total, receipts),
                )
            }
        }
        return cleanupResult(total, receipts)
    }

    private fun cleanupResult(
        total: Tally,
        receipts: ReceiptPurge,
    ): CleanupResult =
        CleanupResult(
            total.removed,
            total.missing,
            total.stillReferenced,
            total.objectsRemoved,
            total.settledElsewhere,
            deadlineSkipped = 0,
            receiptsPurged = receipts.purged,
            receiptsPurgeFailures = receipts.failures,
        )

    private class HygieneFailed(
        cause: Throwable,
        override val partial: CleanupResult,
    ) : RuntimeException(cause.message, cause), PartialResult

    // ---- claims ----------------------------------------------------------

    private fun claim(
        catalog: String,
        worker: String,
        want: Int,
        staging: Boolean,
        afterId: Long,
    ): List<Entry> {
        val now = HogliteSchema.now()
        val leaseCutoff = HogliteSchema.fmt(Instant.now().minusSeconds(claimLeaseSeconds))
        val reasonClause =
            if (staging) {
                "reason = 'compaction_staging' AND scheduled_at < '${
                    HogliteSchema.fmt(Instant.now().minusSeconds(stagingGraceSeconds))
                }'"
            } else {
                "(reason IS NULL OR reason <> 'compaction_staging')"
            }
        // Bound the candidate read. The UPDATE rechecks the lease after the read/write gap.
        val candidates =
            store.queryCatalog(
                catalog,
                "SELECT id, path, reason FROM removals WHERE drained_at IS NULL AND $reasonClause " +
                    "AND (claimed_at IS NULL OR claimed_at < ?) AND id > ? ORDER BY id LIMIT ?",
                leaseCutoff,
                afterId,
                want,
            ).rows
        if (candidates.isEmpty()) return emptyList()
        val ids = candidates.map { it[0].asLong() }
        val tx = HogliteTx()
        tx.add(
            "UPDATE removals SET claimed_at = ?, claimed_by = ? " +
                "WHERE id IN (${ids.joinToString(",") { "?" }}) AND drained_at IS NULL " +
                "AND (claimed_at IS NULL OR claimed_at < ?)",
            now,
            worker,
            *ids.toTypedArray(),
            leaseCutoff,
        )
        // Return the rows the claim UPDATE actually took, not the
        // preselected candidates: a row another worker claimed or drained
        // in the gap is THEIRS, and deleting its object under our lease
        // would break the lease's authority over external deletion.
        val acquired =
            tx.add(
                "SELECT id, path, reason FROM removals WHERE claimed_by = ? AND claimed_at = ? " +
                    "AND drained_at IS NULL AND id IN (${ids.joinToString(",") { "?" }})",
                worker,
                now,
                *ids.toTypedArray(),
            )
        val rows = store.runTx(catalog, HogliteCatalogStore.freshTxid("cleanup-claim"), null, tx)
        return rows[acquired].rows.map { Entry(it[0].asLong(), it[1].asString(), it[2] as? String) }
    }

    private fun releaseClaims(
        catalog: String,
        worker: String,
        ids: List<Long>,
    ) {
        if (ids.isEmpty()) return
        val tx = HogliteTx()
        tx.add(
            "UPDATE removals SET claimed_at = NULL, claimed_by = NULL " +
                "WHERE id IN (${ids.joinToString(",") { "?" }}) " +
                "AND drained_at IS NULL AND claimed_by = ?",
            *ids.toTypedArray(),
            worker,
        )
        store.runTx(catalog, HogliteCatalogStore.freshTxid("cleanup-release"), null, tx)
    }

    // ---- the drain ---------------------------------------------------------

    private data class PathEvent(val action: String, val path: String, val outcome: String, val detail: String?)

    private fun drainSubBatch(
        catalog: String,
        worker: String,
        sub: List<Entry>,
        probeEachPath: Boolean,
        objects: RemovalStore,
    ): Tally {
        val referenced = referencedPaths(catalog, sub.map { it.path })
        val violations = mutableListOf<Entry>()
        val survivors = mutableListOf<Entry>()
        for (entry in sub) {
            if (entry.path in referenced) violations += entry else survivors += entry
        }

        val drainedByOutcome = mapOf("deleted" to mutableListOf<Long>(), "absent" to mutableListOf())
        val failed = mutableListOf<Entry>()
        val removedPaths = LinkedHashSet<String>()

        if (probeEachPath) {
            for (entry in survivors) {
                try {
                    when (objects.deleteIfExists(entry.path)) {
                        RemovalStore.Outcome.REMOVED -> {
                            removedPaths += entry.path
                            drainedByOutcome.getValue("deleted") += entry.removalId
                        }
                        RemovalStore.Outcome.MISSING -> {
                            log.info { "cleanup: '${entry.path}' already gone; draining queue row" }
                            drainedByOutcome.getValue("absent") += entry.removalId
                        }
                    }
                } catch (e: Exception) {
                    requireOrdinaryPurgeFailure(catalog, e)
                    log.error(e) {
                        "cleanup: delete failed for '${entry.path}' " +
                            "(removal_id ${entry.removalId}); leaving queued"
                    }
                    failed += entry
                }
            }
        } else if (survivors.isNotEmpty()) {
            val rowsByPath = survivors.groupBy { it.path }
            val failures = objects.deleteBatch(rowsByPath.keys)
            for ((path, rows) in rowsByPath) {
                val failure = failures[path]
                if (failure == null) {
                    removedPaths += path
                    drainedByOutcome.getValue("deleted") += rows.map { it.removalId }
                } else {
                    log.error {
                        "cleanup: batched delete failed for '$path': $failure; leaving queued"
                    }
                    failed += rows
                }
            }
        }

        var tally = Tally(objectsRemoved = removedPaths.size.toLong())
        val events = mutableListOf<PathEvent>()
        var registeredAfterDelete = 0L
        // Settle successful deletions together. Attempts and releases follow in fenced transactions.
        val tx = HogliteTx()
        val now = HogliteSchema.now()
        var removed = 0L
        var missing = 0L
        var settledElsewhere = 0L
        // outcome -> (ids asked, statement index) for the settle tx.
        val settleStmts = LinkedHashMap<String, Pair<List<Long>, Int>>()
        for ((outcome, ids) in drainedByOutcome) {
            if (ids.isEmpty()) continue
            // Settle, fenced on drained_at IS NULL AND claimed_by = worker —
            // both halves load-bearing (CleanupService.SETTLE_SQL's KDoc).
            // The readback classifies a miss rather than counting it.
            tx.add(
                "UPDATE removals SET drained_at = ?, drained_outcome = ?, last_attempt_at = ?, " +
                    "claimed_at = NULL, claimed_by = NULL " +
                    "WHERE id IN (${ids.joinToString(",") { "?" }}) " +
                    "AND drained_at IS NULL AND claimed_by = ?",
                now,
                outcome,
                now,
                *ids.toTypedArray(),
                worker,
            )
            settleStmts[outcome] = ids to tx.add("SELECT changes()")
        }
        // An all-referenced or all-failed sub-batch has no settle
        // statements — hoglite refuses an empty transaction, and the
        // attempts/release bookkeeping below must still run.
        val results =
            if (tx.size == 0) {
                emptyList()
            } else {
                store.runTx(catalog, HogliteCatalogStore.freshTxid("cleanup-settle"), null, tx)
            }
        for ((outcome, ids) in drainedByOutcome) {
            if (ids.isEmpty()) continue
            val settled = results[settleStmts.getValue(outcome).second].firstRow()!![0].asLong()
            val missedCount = ids.size - settled
            settledElsewhere += missedCount
            if (outcome == "deleted") removed = settled else missing = settled
            if (missedCount > 0) {
                // Classify: a settle lost to 'registered' over a path this
                // sub-batch DELETED is the invariant violation; anything
                // else is ordinary contention with another worker.
                val missedIds =
                    store.queryCatalog(
                        catalog,
                        "SELECT id, path, drained_outcome, claimed_by FROM removals " +
                            "WHERE id IN (${ids.joinToString(",") { "?" }})",
                        *ids.toTypedArray(),
                    ).rows.filter { row ->
                        // "missed" = the fence did not take it. The fence's
                        // negative: drained already, or claimed by another.
                        row[2] != null || row[3] != worker
                    }
                for (row in missedIds) {
                    val path = row[1].asString()
                    val outcome2 = row[2] as? String
                    val claimedBy = row[3] as? String
                    if (outcome2 == "registered" && path in removedPaths) {
                        log.error {
                            "cleanup: removal_id ${row[0].asLong()} path '$path' was settled " +
                                "'registered' by a compaction group AFTER this sub-batch deleted " +
                                "the object — invariant violation"
                        }
                        events +=
                            PathEvent(
                                "cleanup_violation",
                                path,
                                "invariant_violation",
                                "removal_id=${row[0].asLong()} registered after this drain deleted the object",
                            )
                        registeredAfterDelete++
                    } else if (claimedBy != null && claimedBy != worker) {
                        log.warn {
                            "cleanup: removal_id ${row[0].asLong()} is now claimed by '$claimedBy' " +
                                "rather than '$worker' — the lease lapsed while this worker was working"
                        }
                    } else {
                        log.warn {
                            "cleanup: removal_id ${row[0].asLong()} was settled by another writer " +
                                "('$outcome2') before this sub-batch could; not counting it"
                        }
                    }
                }
            }
        }
        // Violations and failures: attempts bump + release, one tx each,
        // the fence deciding what was a real violation (a compaction
        // group committing between the claim and the check settles the
        // ticket 'registered' — the bump's RETURNING-equivalent tells
        // them apart).
        val trueViolations = bumpAttempts(catalog, worker, violations.map { it.removalId })
        tally = tally.copy(stillReferenced = tally.stillReferenced + trueViolations.size.toLong())
        settledElsewhere += violations.size - trueViolations.size
        for (entry in violations.filter { trueViolations.contains(it.removalId) }) {
            log.error {
                "cleanup: path '${entry.path}' (removal_id ${entry.removalId}) is still " +
                    "referenced by the catalog — invariant violation; skipping"
            }
            events +=
                PathEvent(
                    "cleanup_violation",
                    entry.path,
                    "invariant_violation",
                    "removal_id=${entry.removalId} still referenced; not deleted",
                )
        }
        val bumpedFailed = bumpAttempts(catalog, worker, failed.map { it.removalId })
        settledElsewhere += failed.size - bumpedFailed.size
        tally =
            tally.copy(
                removed = removed,
                missing = missing,
                stillReferenced = tally.stillReferenced + registeredAfterDelete,
                settledElsewhere = settledElsewhere,
            )
        for (path in removedPaths) {
            events += PathEvent("file_deleted", path, "ok", null)
        }
        for (e in events) {
            Audit.event(e.action, catalog, e.path, outcome = e.outcome, detail = e.detail)
        }
        return tally
    }

    /** Bump attempts + release, fenced; returns the ids the fence took. */
    private fun bumpAttempts(
        catalog: String,
        worker: String,
        ids: List<Long>,
    ): Set<Long> {
        if (ids.isEmpty()) return emptySet()
        val tx = HogliteTx()
        // Read the exact fence winners in the same serial transaction as the update.
        val acquired =
            tx.add(
                "SELECT id FROM removals WHERE id IN (${ids.joinToString(",") { "?" }}) " +
                    "AND drained_at IS NULL AND claimed_by = ?",
                *ids.toTypedArray(),
                worker,
            )
        tx.add(
            "UPDATE removals SET attempts = attempts + 1, last_attempt_at = ?, " +
                "claimed_at = NULL, claimed_by = NULL " +
                "WHERE id IN (${ids.joinToString(",") { "?" }}) " +
                "AND drained_at IS NULL AND claimed_by = ?",
            HogliteSchema.now(),
            *ids.toTypedArray(),
            worker,
        )
        return store.runTx(catalog, HogliteCatalogStore.freshTxid("cleanup-bump"), null, tx)[acquired]
            .rows.map { it[0].asLong() }.toSet()
    }

    /** Live OR historical references claim the object — CleanupService.referencedPaths. */
    private fun referencedPaths(
        catalog: String,
        paths: List<String>,
    ): Set<String> {
        if (paths.isEmpty()) return emptySet()
        val marks = paths.joinToString(",") { "?" }
        return store.queryCatalog(
            catalog,
            "SELECT path, 0 FROM files WHERE path IN ($marks) " +
                "UNION SELECT path, 0 FROM dvs WHERE path IN ($marks) " +
                "UNION SELECT path, 0 FROM uploads WHERE path IN ($marks) AND state = 'active'",
            *(paths + paths + paths).toTypedArray(),
        ).rows.map { it[0].asString() }.toSet()
    }

    // ---- the after-run purges ------------------------------------------------

    private fun terminalPurgeFailure(e: Exception): Boolean =
        e is InterruptedException || e is java.util.concurrent.CancellationException ||
            Thread.currentThread().isInterrupted

    /** Ordinary failures can be isolated; shutdown and poisoned bindings cannot. */
    private fun requireOrdinaryPurgeFailure(
        catalog: String,
        e: Exception,
    ) {
        if (terminalPurgeFailure(e)) throw e
        store.boundTenant(catalog) // Throws the memoized poison before any sibling I/O.
    }

    private fun suppressOnce(
        original: Throwable,
        secondary: Throwable,
    ) {
        if (original !== secondary && original.suppressed.none { it === secondary }) original.addSuppressed(secondary)
    }

    private fun purgeMaintenanceLedger(catalog: String) {
        val cutoff = HogliteSchema.fmt(Instant.now().minusSeconds(maintenanceLedgerRetentionSeconds))
        val deadline = System.nanoTime() + LEDGER_PURGE_BUDGET_MS * 1_000_000
        while (System.nanoTime() < deadline) {
            val tx = HogliteTx()
            tx.add(
                PURGE_MAINTENANCE_SQL,
                cutoff,
                LEDGER_PURGE_PAGE,
            )
            val count = tx.add("SELECT changes()")
            val purged =
                store.runTx(catalog, HogliteCatalogStore.freshTxid("cleanup-ledger"), null, tx)[count]
                    .firstRow()!![0].asLong()
            if (purged < LEDGER_PURGE_PAGE) break
        }
    }

    private data class ReceiptPurge(val purged: Long = 0, val failures: Long = 0)

    private fun purgeCommitReceipts(
        catalog: String,
        snapshotRetentionSeconds: Long?,
    ): ReceiptPurge {
        if (receiptRetentionSeconds <= 0) return ReceiptPurge()
        val floor =
            snapshotRetentionSeconds?.let { (it * 2).coerceAtMost(RECEIPT_FLOOR_CEILING_SECONDS) }
                ?: receiptRetentionSeconds
        val retention = maxOf(receiptRetentionSeconds, floor)
        val cutoff = HogliteSchema.fmt(Instant.now().minusSeconds(retention))
        val deadline = System.nanoTime() + receiptPurgeBudgetMs * 1_000_000
        var purged = 0L
        var failures = 0L
        while (true) {
            if (System.nanoTime() >= deadline) break
            val gone =
                try {
                    val tx = HogliteTx()
                    tx.add(
                        PURGE_RECEIPTS_SQL,
                        cutoff,
                        receiptPurgePage,
                    )
                    val idx = tx.add("SELECT changes()")
                    store.runTx(catalog, HogliteCatalogStore.freshTxid("cleanup-receipts"), null, tx)[idx]
                        .firstRow()!![0].asLong()
                } catch (e: Exception) {
                    requireOrdinaryPurgeFailure(catalog, e)
                    failures++
                    log.warn(e) { "cleanup: a commit-receipt purge page failed for catalog '$catalog'" }
                    break
                }
            purged += gone
            if (gone < receiptPurgePage) break
        }
        return ReceiptPurge(purged, failures)
    }

    /** The drained-ledger retention: paged from the oldest eligible cutoff key, budgeted. */
    private fun purgeDrainedLedger(catalog: String): Exception? =
        try {
            val cutoff = HogliteSchema.fmt(Instant.now().minusSeconds(removalLedgerRetentionSeconds))
            val deadline = System.nanoTime() + LEDGER_PURGE_BUDGET_MS * 1_000_000
            while (System.nanoTime() < deadline) {
                val tx = HogliteTx()
                tx.add(
                    PURGE_DRAINED_SQL,
                    cutoff,
                    LEDGER_PURGE_PAGE,
                )
                val idx = tx.add("SELECT changes()")
                val gone =
                    store.runTx(catalog, HogliteCatalogStore.freshTxid("cleanup-drained"), null, tx)[idx]
                        .firstRow()!![0].asLong()
                if (gone < LEDGER_PURGE_PAGE.toLong()) break
            }
            null
        } catch (e: Exception) {
            requireOrdinaryPurgeFailure(catalog, e)
            log.warn(e) {
                "cleanup: the drained-ledger retention purge failed for catalog '$catalog'; " +
                    "the drain itself is unaffected and the next run retries from the bottom of the key"
            }
            // Retain the existing standalone best-effort contract, but attach
            // this context if the earlier maintenance-ledger purge failed.
            e
        }

    companion object {
        internal const val PURGE_MAINTENANCE_SQL =
            "DELETE FROM maintenance_runs WHERE id IN (" +
                "SELECT id FROM maintenance_runs WHERE started_at < ? ORDER BY started_at, id LIMIT ?)"
        internal const val PURGE_RECEIPTS_SQL =
            "DELETE FROM receipts WHERE idempotency_key IN (" +
                "SELECT idempotency_key FROM receipts WHERE created_at < ? " +
                "ORDER BY created_at LIMIT ?)"
        internal const val PURGE_DRAINED_SQL =
            "DELETE FROM removals WHERE id IN (" +
                "SELECT id FROM removals WHERE drained_at IS NOT NULL AND drained_at < ? " +
                "ORDER BY drained_at, id LIMIT ?)"

        private const val STAGING_SUB_BATCH = CleanupService.STAGING_SUB_BATCH
        private const val LEDGER_PURGE_PAGE = CleanupService.LEDGER_PURGE_PAGE
        private const val LEDGER_PURGE_BUDGET_MS = CleanupService.LEDGER_PURGE_BUDGET_MS
        private const val RECEIPT_PURGE_PAGE = CleanupService.RECEIPT_PURGE_PAGE
        private const val RECEIPT_PURGE_BUDGET_MS = CleanupService.RECEIPT_PURGE_BUDGET_MS
        private const val RECEIPT_FLOOR_CEILING_SECONDS = CleanupService.RECEIPT_FLOOR_CEILING_SECONDS
    }
}
