package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.ExpiryResult
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.persistence.PartialResult
import com.posthog.hoglake.service.ExpiryBackend
import java.time.Instant

/**
 * Port [com.posthog.hoglake.service.ExpiryService]'s two-phase sweep to one tenant.
 *
 * Phase A guards the retention floor, policy, and consumer positions in one transaction.
 * It expires snapshots and ended DDL versions, but preserves partition specs that files still reference.
 * Phase B purges ended files in bounded pages and queues their objects in the removal ledger.
 * Each page commits its deletions and removal rows together.
 *
 * Hoglite serializes writes per tenant. It needs no PostgreSQL advisory lock or statement-timeout ladder.
 * The page size and wall-clock budget retain their PostgreSQL meanings.
 */
internal class HogliteExpiry(
    private val store: HogliteCatalogStore,
    private val purgePage: Int,
    private val purgeBudgetMs: Long,
) : ExpiryBackend {
    init {
        require(purgePage > 0) { "expiry purge page must be positive (got $purgePage)" }
        require(purgeBudgetMs >= 0) { "expiry purge budget must be >= 0 (got $purgeBudgetMs)" }
    }

    override fun runOnce(
        catalog: String,
        batchSize: Int,
        trigger: MaintenanceTrigger,
    ): ExpiryResult =
        store.recorded(catalog, MaintenanceTask.EXPIRY, trigger) {
            runSweep(catalog, batchSize)
        }

    private fun runSweep(
        catalog: String,
        batchSize: Int,
    ): ExpiryResult {
        val pre =
            try {
                if (batchSize <= 0) {
                    throw HoglakeException.Validation("batch size must be positive (got $batchSize)")
                }
                store.meta(catalog)
            } catch (e: Throwable) {
                Audit.event("expiry", catalog, null, Audit.failureOutcome(e), e.message)
                throw e
            }
        val attempt = runCatching { advanceFloor(catalog, pre, batchSize) }
        // Drain an earlier sweep's backlog even when this floor advance failed.
        val purge = purge(catalog, pre.earliestSnapshotId)
        Metrics.expiryPurgeRows(catalog, purge.dataRows + purge.deleteRows)
        Metrics.expiryPurgeFailures(catalog, purge.failures)
        if (purge.truncated) Metrics.expiryPurgeTruncated(catalog)
        val advance = attempt.getOrNull() ?: Advance(0, purge.floor, null, 0)
        val result =
            ExpiryResult(
                snapshotsExpired = advance.snapshotsExpired,
                dataFilesQueued = purge.dataRows,
                deleteFilesQueued = purge.deleteRows,
                newEarliestSnapshotId = advance.newEarliest,
                flooredByConsumer = advance.flooredBy,
                offsetsReleased = advance.offsetsReleased,
                dataFilesPurged = purge.dataRows,
                purgePages = purge.pages,
                purgeFailures = purge.failures,
                purgeTruncated = purge.truncated,
                purgeRemaining = purge.remaining,
                advanceHalvings = 0,
                purgeHalvings = 0,
            )
        attempt.exceptionOrNull()?.let { e ->
            Metrics.expiryAdvanceFailures(catalog)
            Audit.event("expiry", catalog, null, Audit.failureOutcome(e), e.message)
            // Keep typed HTTP refusals unchanged, as PostgreSQL does.
            throw if (e is HoglakeException) e else AdvanceFailed(e, result)
        }
        Metrics.snapshotsExpired(catalog, result.snapshotsExpired)
        if (result.snapshotsExpired > 0 || result.dataFilesQueued > 0 || result.deleteFilesQueued > 0 ||
            result.offsetsReleased > 0 || result.flooredByConsumer != null ||
            result.purgeFailures > 0 || result.purgeTruncated
        ) {
            Audit.event(
                "expiry",
                catalog,
                null,
                "ok",
                "snapshots_expired=${result.snapshotsExpired} " +
                    "data_files_queued=${result.dataFilesQueued} delete_files_queued=${result.deleteFilesQueued} " +
                    "offsets_released=${result.offsetsReleased} purge_failures=${result.purgeFailures} " +
                    "purge_truncated=${result.purgeTruncated}",
            )
        }
        result.flooredByConsumer?.let { consumer ->
            io.github.oshai.kotlinlogging.KotlinLogging.logger {}.warn {
                "expiry for catalog '$catalog' floored by consumer '$consumer' " +
                    "(earliest stays at ${result.newEarliestSnapshotId})"
            }
        }
        return result
    }

    /** Preserve the purge result on failed runs without changing typed HTTP errors. */
    private class AdvanceFailed(
        cause: Throwable,
        override val partial: ExpiryResult,
    ) : RuntimeException(cause.message, cause), PartialResult

    // ---- phase A ---------------------------------------------------------

    private class Advance(
        val snapshotsExpired: Long,
        val newEarliest: Long,
        val flooredBy: String?,
        val offsetsReleased: Long,
    )

    private fun advanceFloor(
        catalog: String,
        pre: HogliteCatalogStore.MetaRow,
        batchSize: Int,
    ): Advance {
        // The forward catch-all for offsets a replacement left behind:
        // OffsetRepo.SUPERSEDED_LINEAGE_CTE verbatim (SQLite's WITH
        // RECURSIVE is the same), over the tenant's tables/offsets.
        val released =
            store.runTx(
                catalog,
                HogliteCatalogStore.freshTxid("expiry-release"),
                null,
                HogliteTx().apply {
                    add(
                        """
                        WITH RECURSIVE lineage AS (
                            SELECT o.consumer_id,
                                   o.table_uuid       AS origin_uuid,
                                   t.id,
                                   t.table_uuid,
                                   t.created_snapshot
                            FROM offsets o
                            JOIN tables t ON t.table_uuid = o.table_uuid
                            WHERE t.dropped_snapshot IS NOT NULL
                            UNION ALL
                            SELECT l.consumer_id, l.origin_uuid,
                                   n.id, n.table_uuid, n.created_snapshot
                            FROM lineage l
                            JOIN tables n
                              ON n.replaced_table_id = l.id
                             AND n.created_snapshot > l.created_snapshot
                        )
                        DELETE FROM offsets WHERE EXISTS (
                            SELECT 1
                            FROM lineage l
                            JOIN offsets reconciled
                              ON reconciled.consumer_id = l.consumer_id
                             AND reconciled.table_uuid = l.table_uuid
                            WHERE l.consumer_id = offsets.consumer_id
                              AND l.origin_uuid = offsets.table_uuid
                              AND l.table_uuid <> offsets.table_uuid
                              AND reconciled.committed_snapshot >= l.created_snapshot
                        )
                        """.trimIndent(),
                    )
                    // Hoglite returns no affected count for a top-level WITH statement.
                    // Read SQLite's count before another write can replace it.
                    add("SELECT changes()")
                },
            )[1].firstRow()!![0].asLong()

        val retention =
            pre.retentionSeconds
                ?: return Advance(0, pre.earliestSnapshotId, null, released)

        val now = Instant.now()
        val cutoff = HogliteSchema.fmt(now.minusSeconds(retention))
        val firstFresh =
            store.queryCatalog(
                catalog,
                "SELECT MIN(id) FROM snapshots WHERE id >= ? AND ts >= ?",
                pre.earliestSnapshotId,
                cutoff,
            ).firstRow()!![0].asLongOrNull()

        val minOffset =
            if (pre.consumerFloor) {
                store.queryCatalog(
                    catalog,
                    "SELECT o.consumer_id, o.committed_snapshot FROM offsets o " +
                        "JOIN tables t ON t.table_uuid = o.table_uuid " +
                        "ORDER BY o.committed_snapshot, o.consumer_id LIMIT 1",
                ).firstRow()?.let { it[0].asString() to it[1].asLong() }
            } else {
                null
            }

        val unfloored =
            minOf(
                firstFresh ?: Long.MAX_VALUE,
                pre.head,
                pre.earliestSnapshotId + batchSize,
            )
        val newEarliest = minOf(unfloored, minOffset?.second ?: Long.MAX_VALUE)
        val flooredBy =
            minOffset
                ?.takeIf { it.second < unfloored && unfloored > pre.earliestSnapshotId }
                ?.first
        if (newEarliest <= pre.earliestSnapshotId) {
            return Advance(0, pre.earliestSnapshotId, flooredBy, released)
        }

        val tx = HogliteTx()
        // The floor's invariants, re-asserted IN the transaction (the
        // planning reads above are separate requests; hoglite serializes
        // the tx, not them):
        //  - OCC on the floor itself: a concurrent sweep that advanced it
        //    invalidates this plan (and the floor must never regress).
        //  - The consumer floor: a consumer that registered below the
        //    planned floor since planning keeps its history.
        // A miss is a typed conflict; the next sweep re-plans.
        tx.expectScalar(
            pre.earliestSnapshotId,
            "SELECT earliest_snapshot_id FROM meta WHERE id = 1",
        ) {
            HoglakeException.CommitConflict(
                "the expiry floor moved under this sweep; the next run re-plans",
            )
        }
        // The POLICY the sweep planned against, re-asserted in the same
        // transaction: a retention patch (or its removal) and a
        // consumer-floor toggle both land between planning and here
        // otherwise, and a plan made under a policy no longer in force
        // must not publish (null retention reads back as -1).
        tx.expectScalar(
            retention,
            "SELECT COALESCE(snapshot_retention_seconds, -1) FROM meta WHERE id = 1",
        ) {
            HoglakeException.CommitConflict(
                "the retention policy changed under this sweep; the next run re-plans",
            )
        }
        tx.expectScalar(
            if (pre.consumerFloor) 1 else 0,
            "SELECT consumer_floor FROM meta WHERE id = 1",
        ) {
            HoglakeException.CommitConflict(
                "the consumer-floor policy changed under this sweep; the next run re-plans",
            )
        }
        if (pre.consumerFloor) {
            tx.expectZero(
                "SELECT COUNT(*) FROM offsets o JOIN tables t ON t.table_uuid = o.table_uuid " +
                    "WHERE o.committed_snapshot < ?",
                newEarliest,
            ) {
                HoglakeException.CommitConflict(
                    "a consumer registered below this sweep's planned floor; the next run re-plans",
                )
            }
        }
        tx.add(
            "DELETE FROM changes WHERE snapshot_id >= ? AND snapshot_id < ?",
            pre.earliestSnapshotId,
            newEarliest,
        )
        val snapshots =
            tx.add(
                "DELETE FROM snapshots WHERE id >= ? AND id < ?",
                pre.earliestSnapshotId,
                newEarliest,
            )
        // The new floor snapshot's time rides the same update: that row
        // survives this sweep, and 410 reconciliation needs the time
        // after the rows below it are gone.
        tx.affectOne(
            "UPDATE meta SET earliest_snapshot_id = ?, " +
                "earliest_snapshot_time = (SELECT ts FROM snapshots WHERE id = ?) WHERE id = 1",
            newEarliest,
            newEarliest,
        ) { HoglakeException.NotFound("catalog (meta row missing)") }
        // Versioned-row retention: end/dropped <= floor means no retained
        // snapshot can see the row (visibility is begin <= S < end).
        // partition_specs is deliberately absent (class KDoc), and
        // table_layouts is here although nothing ends a layout row yet
        // (no unset op), so the first op that does cannot leave its
        // corpses unbounded — ExpiryService's two lists, ported.
        for (table in VERSIONED_RETENTION_TABLES) {
            tx.add(
                "DELETE FROM $table WHERE end_snapshot IS NOT NULL AND end_snapshot <= ?",
                newEarliest,
            )
        }
        tx.add(
            "DELETE FROM views WHERE dropped_snapshot IS NOT NULL AND dropped_snapshot <= ?",
            newEarliest,
        )
        val results = store.runTx(catalog, HogliteCatalogStore.freshTxid("expiry-advance"), null, tx)
        return Advance(results[snapshots].affected, newEarliest, flooredBy, released)
    }

    // ---- phase B ---------------------------------------------------------

    private class Purge(
        val dataRows: Long,
        val deleteRows: Long,
        val pages: Long,
        val failures: Long,
        val truncated: Boolean,
        val remaining: Long?,
        val floor: Long,
    )

    private fun purge(
        catalog: String,
        knownFloor: Long,
    ): Purge {
        // Read the floor FRESH: it is monotone, so a value read now is at
        // least phase A's. A floor of 0 skips the phase: every ended
        // row's end_snapshot >= 1, so nothing is eligible.
        val floor =
            runCatching { store.meta(catalog).earliestSnapshotId }.getOrNull()
                ?: return Purge(0, 0, 0, 1, truncated = true, remaining = null, floor = knownFloor)
        if (floor <= 0) return Purge(0, 0, 0, 0, truncated = false, remaining = 0, floor = floor)

        val started = System.nanoTime()
        var dataRows = 0L
        var deleteRows = 0L
        var pages = 0L
        var failures = 0L
        var truncated = false

        fun overBudget() = (System.nanoTime() - started) / 1_000_000 >= purgeBudgetMs

        // Arm 1: superseded vectors with nothing to ride — their own
        // end_snapshot below the floor, data file wherever it is.
        while (true) {
            if (overBudget()) {
                truncated = true
                break
            }
            val page =
                runCatching {
                    store.queryCatalog(
                        catalog,
                        "SELECT delete_file_id, path FROM dvs " +
                            "WHERE end_snapshot IS NOT NULL AND end_snapshot <= ? LIMIT ?",
                        floor,
                        purgePage,
                    ).rows
                }.getOrElse {
                    failures++
                    truncated = true
                    break
                }
            if (page.isEmpty()) break
            val ids = page.map { it[0].asLong() }
            runCatching {
                val tx = HogliteTx()
                // Queue from transaction-local rows, not the page's stale paths.
                tx.add(
                    "INSERT INTO removals(path, file_kind, reason, queued_snapshot, queued_at) " +
                        "SELECT path, 'delete', 'snapshot_expiry', ?, ? FROM dvs " +
                        "WHERE delete_file_id IN (${ids.joinToString(",") { "?" }}) " +
                        "AND end_snapshot IS NOT NULL AND end_snapshot <= ?",
                    floor,
                    HogliteSchema.now(),
                    *ids.toTypedArray(),
                    floor,
                )
                val deleted =
                    tx.add(
                        "DELETE FROM dvs WHERE delete_file_id IN (${ids.joinToString(",") { "?" }}) " +
                            "AND end_snapshot IS NOT NULL AND end_snapshot <= ?",
                        *ids.toTypedArray(),
                        floor,
                    )
                val results = store.runTx(catalog, HogliteCatalogStore.freshTxid("expiry-purge-dv"), null, tx)
                deleteRows += results[deleted].affected
            }.onFailure {
                failures++
                truncated = true
            }
            if (failures > 0) break
            pages++
            if (ids.size < purgePage) break
        }

        // Arm 2: the data files — a page takes the vectors RIDING its
        // files with it, whatever their own end state, and queues both
        // kinds. One transaction: a path is queued iff its row is gone.
        while (!truncated) {
            if (overBudget()) {
                truncated = true
                break
            }
            val page =
                runCatching {
                    store.queryCatalog(
                        catalog,
                        "SELECT id, path FROM files " +
                            "WHERE end_snapshot IS NOT NULL AND end_snapshot <= ? LIMIT ?",
                        floor,
                        purgePage,
                    ).rows
                }.getOrElse {
                    failures++
                    truncated = true
                    break
                }
            if (page.isEmpty()) break
            val ids = page.map { it[0].asLong() }
            runCatching {
                val tx = HogliteTx()
                // The riding vectors' removals come from the same
                // predicate INSIDE the transaction: a DV that lands
                // between the page read and this tx is queued AND
                // deleted atomically, never orphaned.
                val dvQueue =
                    tx.add(
                        "INSERT INTO removals(path, file_kind, reason, queued_snapshot, queued_at) " +
                            "SELECT path, 'delete', 'snapshot_expiry', ?, ? FROM dvs " +
                            "WHERE data_file_id IN (${ids.joinToString(",") { "?" }})",
                        floor,
                        HogliteSchema.now(),
                        *ids.toTypedArray(),
                    )
                tx.add(
                    "DELETE FROM dvs WHERE data_file_id IN (${ids.joinToString(",") { "?" }})",
                    *ids.toTypedArray(),
                )
                tx.add(
                    "INSERT INTO removals(path, file_kind, reason, queued_snapshot, queued_at) " +
                        "SELECT path, 'data', 'snapshot_expiry', ?, ? FROM files " +
                        "WHERE id IN (${ids.joinToString(",") { "?" }}) " +
                        "AND end_snapshot IS NOT NULL AND end_snapshot <= ?",
                    floor,
                    HogliteSchema.now(),
                    *ids.toTypedArray(),
                    floor,
                )
                val deleted =
                    tx.add(
                        "DELETE FROM files WHERE id IN (${ids.joinToString(",") { "?" }}) " +
                            "AND end_snapshot IS NOT NULL AND end_snapshot <= ?",
                        *ids.toTypedArray(),
                        floor,
                    )
                val results = store.runTx(catalog, HogliteCatalogStore.freshTxid("expiry-purge-data"), null, tx)
                deleteRows += results[dvQueue].affected
                dataRows += results[deleted].affected
            }.onFailure {
                failures++
                truncated = true
            }
            if (failures > 0) break
            pages++
            if (ids.size < purgePage) break
        }

        val remaining =
            if (truncated) {
                runCatching {
                    val cap = PURGE_REMAINING_CAP
                    store.queryCatalog(
                        catalog,
                        "SELECT (SELECT COUNT(*) FROM (SELECT 1 FROM files " +
                            "WHERE end_snapshot IS NOT NULL AND end_snapshot <= ? LIMIT ?)) + " +
                            "(SELECT COUNT(*) FROM (SELECT 1 FROM dvs " +
                            "WHERE end_snapshot IS NOT NULL AND end_snapshot <= ? LIMIT ?))",
                        floor,
                        cap,
                        floor,
                        cap,
                    ).firstRow()!![0].asLong()
                }.getOrNull()
            } else {
                0L
            }
        return Purge(dataRows, deleteRows, pages, failures, truncated, remaining, floor)
    }

    internal companion object {
        /** Saturating backlog cap — ExpiryService.PURGE_REMAINING_CAP. */
        const val PURGE_REMAINING_CAP = 100_000

        /** ExpiryService.VERSIONED_RETENTION_TABLES over the tenant schema (views purge by dropped_snapshot). */
        val VERSIONED_RETENTION_TABLES = listOf("table_versions", "columns", "sort_specs", "table_layouts")

        /** ExpiryService.VERSIONED_TABLES_NEVER_CLEARED over the tenant schema; see the class KDoc. */
        val VERSIONED_TABLES_NEVER_CLEARED = listOf("partition_specs")
    }
}
