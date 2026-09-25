package com.posthog.hoglake.service

import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.model.ExpiryResult
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.persistence.MaintenanceRunStore
import com.posthog.hoglake.persistence.OffsetRepo
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked

/**
 * Consumer-aware, incremental snapshot expiry (README.md §6). One sweep
 * is ONE transaction under the per-catalog commit lock, so expiry
 * serializes with commits/DDL and a sweep can never observe (or leave)
 * a half-advanced floor.
 *
 * The floor math: with `earliest` the current earliest_snapshot_id,
 * the sweep computes
 *
 *   newEarliest = min( firstFresh ?: +inf,   // oldest snapshot inside the
 *                                            // retention window survives
 *                      head,                 // head NEVER expires
 *                      minConsumerOffset,    // consumer must still read
 *                                            // from its committed offset
 *                                            // (consumer_floor = true only)
 *                      earliest + batchSize) // incremental: bounded work
 *
 * where firstFresh = min snapshot_id with snapshot_time >= now() -
 * retention (so every snapshot below newEarliest is strictly older than
 * the cutoff, even under non-monotone clocks). Snapshots in
 * [earliest, newEarliest) are range-deleted; newEarliest <= earliest is
 * a zero-work sweep. flooredByConsumer names the pinning consumer iff
 * the consumer constraint bound the sweep below what time/head/batch
 * would have allowed — that includes a zero-work sweep the consumer
 * caused (page-worthy: a lagging consumer is pinning retention).
 *
 * File GC rides the same transaction, all range predicates (never
 * IN-lists): a file row whose end_snapshot <= newEarliest is invisible
 * at every surviving snapshot, so its path is queued into
 * hog_file_removal (a *suggestion* for CleanupService, never an
 * authorization — README.md §8) and the row deleted (FK cascades take
 * stats and partition values). Delete-vector rows go FIRST, including
 * live DVs whose data file is expiring — deleting the data-file row
 * would cascade them away un-queued, orphaning the object.
 *
 * A fifth step applies the same reachability rule to the accumulating
 * versioned DDL tables (hog_table_version, hog_column,
 * hog_partition_spec, hog_sort_spec, hog_view): rows with
 * end_snapshot <= newEarliest are invisible at every retained snapshot
 * and are deleted — DDL churn no longer grows them without bound. The
 * floor advance also captures the new floor snapshot's snapshot_time
 * into hog_catalog.earliest_snapshot_time (the reconciliation anchor
 * that 410s cite once the snapshot rows below the floor are gone).
 */
class ExpiryService(private val jdbi: Jdbi) {
    private val log = KotlinLogging.logger {}

    /** The run ledger; records after the sweep resolves, never inside it. */
    private val runStore = MaintenanceRunStore(jdbi)

    internal companion object {
        /**
         * WHICH OFFSETS CAN FLOOR EXPIRY — the source clause of the
         * consumer-floor query, shared verbatim with
         * `VerifyService`'s `expiry_floor` check.
         *
         * The join to `hog_table` is the load-bearing part: an offset
         * counts only while its table identity still exists (any
         * incarnation, dropped included — offsets survive drops by
         * design), because an offset naming a uuid the catalog no longer
         * has cannot be advanced by anyone and must not pin retention
         * forever. The check asserts the invariant this query enforces,
         * so it asks THIS clause rather than a restatement of it: a copy
         * that lost the join would let the check pass on exactly the
         * rows the sweep ignores (AGENT.md — a parity test that
         * restates its subject asserts only that the file compiles).
         *
         * Binds `:catalogId`. No interpolated values (invariant 9
         * intact).
         */
        internal const val FLOOR_CANDIDATE_OFFSETS: String =
            """
            FROM hog_consumer_offset o
            JOIN hog_table t
              ON t.catalog_id = o.catalog_id AND t.table_uuid = o.table_uuid
            WHERE o.catalog_id = :catalogId
            """

        /**
         * SWEEP STEP 1, the deletion-vector arm, `internal` so the plan
         * test EXPLAINs the SQL PRODUCTION runs rather than a lookalike.
         *
         * Superseded DVs (their own `end_snapshot` in range) plus live
         * DVs riding an expiring data file — the data-file delete in
         * [DATA_FILE_EXPIRY_SQL] would cascade those away without
         * queueing them.
         *
         * NO INDEX SERVES THIS, and V18 deliberately does not add one.
         * The predicate is an `OR` whose second arm is a correlated
         * `EXISTS` over hog_data_file; the planner reads that as one
         * pass and never chooses a `(catalog_id, end_snapshot)` index
         * for the first arm, so an index built for it would be paid on
         * every write and used by nothing (V18's header carries the
         * measurement). Splitting the statement into its two arms is
         * what would make an index choosable, and that is a change to
         * the sweep's behaviour, ticketed separately.
         */
        internal const val DELETE_FILE_EXPIRY_SQL: String =
            """
            WITH doomed AS (
                DELETE FROM hog_delete_file dv
                WHERE dv.catalog_id = :catalogId
                  AND ((dv.end_snapshot IS NOT NULL AND dv.end_snapshot <= :newEarliest)
                       OR EXISTS (
                              SELECT 1 FROM hog_data_file df
                              WHERE df.catalog_id = dv.catalog_id
                                AND df.data_file_id = dv.data_file_id
                                AND df.end_snapshot IS NOT NULL
                                AND df.end_snapshot <= :newEarliest))
                RETURNING path
            )
            INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
            SELECT :catalogId, path, 'delete', 'snapshot_expiry' FROM doomed
            """

        /**
         * SWEEP STEP 2, the data-file arm, `internal` for the same
         * reason.
         *
         * THE STATEMENT V18 EXISTS FOR. `end_snapshot` appears in no
         * index's leading columns before V18 — `hog_data_file_live` is
         * partial on its COMPLEMENT — so this was a sequential scan of
         * the whole manifest, inside the sweep transaction, under the
         * per-catalog commit lock. `hog_data_file_ended (catalog_id,
         * end_snapshot) WHERE end_snapshot IS NOT NULL` is exactly this
         * predicate, and `V18DataFileEndedIndexMigrationIntegrationTest`
         * EXPLAINs THIS string before and after the migration.
         */
        internal const val DATA_FILE_EXPIRY_SQL: String =
            """
            WITH doomed AS (
                DELETE FROM hog_data_file
                WHERE catalog_id = :catalogId
                  AND end_snapshot IS NOT NULL AND end_snapshot <= :newEarliest
                RETURNING path
            )
            INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
            SELECT :catalogId, path, 'data', 'snapshot_expiry' FROM doomed
            """

        /**
         * The end-snapshotted-but-never-deleted versioned tables (DDL
         * churn grows them without bound); sweep step 5 deletes their
         * below-floor corpses. Table names are a fixed compile-time
         * vocabulary, never derived from input (invariant 9 intact).
         */
        val VERSIONED_RETENTION_TABLES =
            listOf(
                "hog_table_version",
                "hog_column",
                "hog_partition_spec",
                "hog_sort_spec",
                "hog_view",
            )
    }

    /**
     * One expiry sweep for [catalog], expiring at most [batchSize]
     * snapshots. The audit event and the expired-snapshots counter are
     * emitted here, AFTER the sweep transaction has committed — and a
     * ZERO-WORK sweep (nothing expired or queued, no consumer floor in
     * play) emits no audit event at all, only an app-log debug line:
     * every-minute background no-ops must not flood the audit stream.
     * Every run (zero-work included) is recorded in the maintenance run
     * ledger — the row is the per-catalog loop-liveness signal.
     */
    fun runOnce(
        catalog: String,
        batchSize: Int,
        trigger: MaintenanceTrigger = MaintenanceTrigger.MANUAL,
    ): ExpiryResult =
        runStore.recorded(catalog, MaintenanceTask.EXPIRY, trigger) {
            runSweep(catalog, batchSize)
        }

    private fun runSweep(
        catalog: String,
        batchSize: Int,
    ): ExpiryResult {
        val result =
            try {
                if (batchSize <= 0) {
                    throw HoglakeException.Validation("batch size must be positive (got $batchSize)")
                }
                jdbi.inTransactionUnchecked { h ->
                    val pre = CatalogRepo.require(h, catalog)
                    Locks.acquireCatalogCommitLock(h, pre.catalogId)
                    // Re-read under the lock: head/options may have moved while
                    // we queued behind a committer.
                    val cat = CatalogRepo.require(h, catalog)
                    sweep(h, cat, batchSize)
                }
            } catch (e: Throwable) {
                Audit.event("expiry", catalog, null, Audit.failureOutcome(e), e.message)
                throw e
            }
        // The floored-by page-worthy warn lives HERE, outside the sweep
        // transaction (and outside the advisory lock): the data is already
        // in the result, and log I/O must never ride the commit tail.
        result.flooredByConsumer?.let { consumer ->
            log.warn {
                "expiry for catalog '$catalog' floored by consumer '$consumer' " +
                    "(earliest stays at ${result.newEarliestSnapshotId})"
            }
        }
        Metrics.snapshotsExpired(catalog, result.snapshotsExpired)
        // A sweep that DELETED consumer positions did work, even when it
        // expired nothing — on a retention-null catalog that is the only
        // work it can do, and logging it as "nothing to do" is how the
        // stranded-offset release stayed invisible for as long as it did.
        val zeroWork =
            result.snapshotsExpired == 0L && result.dataFilesQueued == 0L &&
                result.deleteFilesQueued == 0L && result.flooredByConsumer == null &&
                result.offsetsReleased == 0L
        if (zeroWork) {
            log.debug { "expiry sweep for catalog '$catalog': nothing to do" }
        } else {
            Audit.event(
                "expiry",
                catalog,
                null,
                outcome = "ok",
                detail =
                    "snapshots_expired=${result.snapshotsExpired} " +
                        "data_files_queued=${result.dataFilesQueued} " +
                        "delete_files_queued=${result.deleteFilesQueued} " +
                        "new_earliest=${result.newEarliestSnapshotId} " +
                        "offsets_released=${result.offsetsReleased}" +
                        (result.flooredByConsumer?.let { " floored_by_consumer=$it" } ?: ""),
            )
        }
        return result
    }

    private fun sweep(
        h: Handle,
        cat: CatalogInfo,
        batchSize: Int,
    ): ExpiryResult {
        // FIRST, and deliberately ABOVE the retention check below.
        //
        // An offset left on an incarnation that atomic replacement
        // retired can never be advanced by anyone (see
        // OffsetRepo.releaseSupersededOffsets), so it is dead state
        // whatever the catalog's retention setting is: GET /consumers
        // shows a position the consumer will never read again, and
        // /maintenance/verify's offset_release check flags it. It used
        // to sit after the retention early-return, which meant a
        // retention-NULL catalog — expiry configured off, which several
        // production catalogs are — could never release anything: the
        // rows were stranded with no code path left that would ever
        // clear them, and the check would have fired forever with no
        // remedy an operator could apply. The commit path's cheap
        // backward walk still handles the common case; this forward form
        // is the catch-all for rows stranded before that rule existed.
        //
        // The per-catalog commit lock is already held (runSweep takes it
        // before calling here), so this runs under the same
        // serialization as every other write in the sweep.
        //
        // Idempotent, but not free: it joins every offset in the catalog
        // to hog_table. That is affordable because a sweep is once per
        // interval and a commit is not, which is exactly why the commit
        // path uses the cheap backward form instead.
        // It is NOT gated on consumerFloor either: these rows are dead
        // whether or not they would floor anything, and GET /consumers
        // must not keep showing a position the consumer will never read.
        val offsetsReleased = OffsetRepo.releaseSupersededOffsets(h, cat.catalogId).toLong()

        val retention =
            cat.snapshotRetentionSeconds
                ?: return ExpiryResult(0, 0, 0, cat.earliestSnapshotId, null, offsetsReleased)

        // Oldest snapshot still inside the retention window; everything
        // below it is expirable time-wise.
        val firstFresh: Long? =
            h.createQuery(
                """
            SELECT min(snapshot_id) FROM hog_snapshot
            WHERE catalog_id = :catalogId
              AND snapshot_id >= :earliest
              AND snapshot_time >= now() - make_interval(secs => :retention)
            """,
            )
                .bind("catalogId", cat.catalogId)
                .bind("earliest", cat.earliestSnapshotId)
                .bind("retention", retention)
                .mapTo(Long::class.javaObjectType)
                .one()

        // [FLOOR_CANDIDATE_OFFSETS] scopes the floor to offsets whose
        // table identity still exists (any incarnation, dropped included
        // — offsets survive drops by design). An offset whose table row
        // is gone entirely (expired away) must not pin retention
        // forever. VerifyService's expiry_floor check asks the SAME
        // fragment rather than a copy of it.
        val minOffset: Pair<String, Long>? =
            if (cat.consumerFloor) {
                h.createQuery(
                    """
                SELECT o.consumer_id, o.committed_snapshot
                $FLOOR_CANDIDATE_OFFSETS
                ORDER BY o.committed_snapshot, o.consumer_id
                LIMIT 1
                """,
                )
                    .bind("catalogId", cat.catalogId)
                    .map { rs, _ -> rs.getString("consumer_id") to rs.getLong("committed_snapshot") }
                    .findOne()
                    .orElse(null)
            } else {
                null
            }

        val unfloored =
            minOf(
                firstFresh ?: Long.MAX_VALUE,
                cat.headSnapshotId,
                cat.earliestSnapshotId + batchSize,
            )
        val newEarliest = minOf(unfloored, minOffset?.second ?: Long.MAX_VALUE)
        val flooredBy =
            minOffset
                ?.takeIf { it.second < unfloored && unfloored > cat.earliestSnapshotId }
                ?.first
        // NOTE: no logging in here — this runs inside the sweep transaction
        // under the catalog commit lock; runOnce warns AFTER commit.
        if (newEarliest <= cat.earliestSnapshotId) {
            return ExpiryResult(0, 0, 0, cat.earliestSnapshotId, flooredBy, offsetsReleased)
        }

        // 1) Delete-vector rows first: superseded DVs (end_snapshot in
        // range) plus live DVs riding an expiring data file — the data-file
        // delete below would cascade those away without queueing them.
        val deleteFilesQueued =
            h.createUpdate(DELETE_FILE_EXPIRY_SQL)
                .bind("catalogId", cat.catalogId)
                .bind("newEarliest", newEarliest)
                .execute()

        // 2) Unreachable data files (cascades stats + partition values).
        val dataFilesQueued =
            h.createUpdate(DATA_FILE_EXPIRY_SQL)
                .bind("catalogId", cat.catalogId)
                .bind("newEarliest", newEarliest)
                .execute()

        // 3) The snapshots themselves (cascades hog_snapshot_change).
        val snapshotsExpired =
            h.createUpdate(
                """
            DELETE FROM hog_snapshot
            WHERE catalog_id = :catalogId
              AND snapshot_id >= :earliest AND snapshot_id < :newEarliest
            """,
            )
                .bind("catalogId", cat.catalogId)
                .bind("earliest", cat.earliestSnapshotId)
                .bind("newEarliest", newEarliest)
                .execute()

        // 4) Advance the floor, capturing the new floor snapshot's time in
        // the SAME update: that snapshot survives this sweep (only ids
        // BELOW newEarliest were deleted) but a later sweep will kill it,
        // and 410 reconciliation needs "the floor was reached at T" after
        // the times below it are gone.
        h.createUpdate(
            """
            UPDATE hog_catalog
               SET earliest_snapshot_id = :newEarliest,
                   earliest_snapshot_time =
                       (SELECT snapshot_time FROM hog_snapshot
                         WHERE catalog_id = :catalogId AND snapshot_id = :newEarliest)
             WHERE catalog_id = :catalogId
            """,
        )
            .bind("newEarliest", newEarliest)
            .bind("catalogId", cat.catalogId)
            .execute()

        // 5) Versioned-row retention for the accumulating DDL tables.
        // Correctness: a versioned row with end_snapshot = E is visible at
        // S iff S < E (visibility rule: begin <= S AND (end IS NULL OR
        // S < end)); every retained snapshot satisfies S >= newEarliest;
        // so E <= newEarliest means NO retained snapshot can see the row —
        // it is unreachable by any valid read (head reads see only
        // end IS NULL rows) and can be deleted outright. Live rows
        // (end IS NULL) and rows ending above the floor are untouched, so
        // time travel at every S >= newEarliest is unchanged. Child tables
        // (hog_partition_field, hog_sort_field) follow their spec headers
        // via FK ON DELETE CASCADE. Incremental like the steps above: the
        // range is bounded by newEarliest, which batchSize caps per sweep.
        for (table in VERSIONED_RETENTION_TABLES) {
            h.createUpdate(
                """
                DELETE FROM $table
                WHERE catalog_id = :catalogId
                  AND end_snapshot IS NOT NULL AND end_snapshot <= :newEarliest
                """,
            )
                .bind("catalogId", cat.catalogId)
                .bind("newEarliest", newEarliest)
                .execute()
        }

        return ExpiryResult(
            snapshotsExpired = snapshotsExpired.toLong(),
            dataFilesQueued = dataFilesQueued.toLong(),
            deleteFilesQueued = deleteFilesQueued.toLong(),
            newEarliestSnapshotId = newEarliest,
            flooredByConsumer = flooredBy,
            offsetsReleased = offsetsReleased,
        )
    }

    /**
     * One sweep across every catalog, for the background loop
     * (BackgroundLoops in App.kt). Catalogs are isolated: one catalog's
     * failure is logged and the rest proceed.
     */
    fun runOnceAllCatalogs(batchSize: Int): List<Pair<String, ExpiryResult>> {
        val names = jdbi.withHandleUnchecked { h -> CatalogRepo.listAll(h) }.map { it.name }
        val results = mutableListOf<Pair<String, ExpiryResult>>()
        for (name in names) {
            try {
                results += name to runOnce(name, batchSize, MaintenanceTrigger.LOOP)
            } catch (e: Exception) {
                // '$name', not '$': the string interpolation was
                // truncated, so every failure line named no catalog at
                // all — on a fan-out across the fleet that is a log
                // entry an operator cannot act on.
                log.error(e) { "expiry sweep failed for catalog '$name'; continuing" }
            }
        }
        return results
    }
}
