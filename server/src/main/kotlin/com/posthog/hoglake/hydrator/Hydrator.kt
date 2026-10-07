package com.posthog.hoglake.hydrator

import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.HydratorSweepResult
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.RehydrateResult
import com.posthog.hoglake.model.SplitOffsets
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.MaintenanceRunStore
import com.posthog.hoglake.persistence.NamespaceRepo
import com.posthog.hoglake.persistence.TableRepo
import com.posthog.hoglake.persistence.bindBigintArrayOrNull
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.SeekableInputStream
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.statement.Update
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.S3Exception
import java.io.EOFException
import java.io.IOException
import java.nio.ByteBuffer
import java.sql.Types
import java.time.Instant

/**
 * Async stats hydration for deferred-stats registrations (the
 * footer-shipping decision in ../README.md): files committed with
 * `stats_state = 'pending'` get their parquet footers read from the object
 * store (parquet-java — the project's one parquet library), per-column
 * statistics aggregated across row groups, bounds encoded in Iceberg
 * single-value form, and the row flipped to `'provided'` — or `'failed'`
 * when the object is missing/unparseable or the registered record_count
 * does not match the footer.
 *
 * The footer read doubles as the field-id contract check: any primitive
 * leaf without a `PARQUET:field_id` flags the row
 * (`hog_data_file.missing_field_ids`) — such files bind columns by name,
 * so AlterService refuses column renames while one is live, and
 * CatalogMetrics gauges the flagged population
 * (`hoglake_missing_field_id_files`).
 *
 * The same footer also yields the file's row-group start offsets
 * (`hog_data_file.split_offsets`, [FooterSplitOffsets]), written in the
 * statement that flips the file to `provided` — or NULL past
 * [SplitOffsets.MAX_ROW_GROUPS] row groups or when the footer gives no
 * list honouring the contract. Readers cut such a file evenly.
 *
 * Footer-only: nothing here decodes data pages. When the registration
 * carried `footer_size`, only the object's tail is fetched (ranged GET);
 * otherwise (or if the tail turns out not to contain everything the footer
 * parse needs) the whole object is fetched — capped at
 * [maxWholeObjectBytes] (HOGLAKE_HYDRATOR_MAX_WHOLE_OBJECT_BYTES): a
 * larger file without a usable footer_size fails structurally instead of
 * buffering unbounded bytes on the heap.
 *
 * Failure taxonomy: TRANSIENT object-store failures (throttle/5xx,
 * connection, timeout — anything but a definitive 404/NoSuchKey) leave
 * the file 'pending' (logged + `hoglake_hydrator_transient_errors_total`)
 * for the next sweep; STRUCTURAL ones (object missing, unparseable
 * footer, registration mismatch, over-cap) mark it 'failed'. 'failed' is
 * terminal to the sweep but operator-recoverable: [rehydrateFailed]
 * (POST /maintenance/rehydrate) flips failed rows back to pending.
 *
 * Work-claiming: one sweep is ONE transaction whose claim query uses
 * FOR UPDATE SKIP LOCKED, so concurrent replicas hydrate DISJOINT sets
 * instead of racing the same pending head through S3 (the guarded flip
 * kept that correct but paid N× the GETs). The row locks are held across
 * the footer fetches — acceptable for a background sweep bounded by
 * [runOnce]'s limit; foreground paths never lock pending rows. The claim
 * is PER CATALOG, each under its own savepoint and timeout
 * ([claimPending], #269): catalogs share the sweep's limit evenly, and
 * a catalog whose claim is slow loses its share, not the sweep.
 */
class Hydrator(
    private val jdbi: Jdbi,
    private val store: ObjectStore,
    /** Whole-object fallback cap; see class KDoc. */
    private val maxWholeObjectBytes: Long = DEFAULT_MAX_WHOLE_OBJECT_BYTES,
    /**
     * statement_timeout for ONE catalog's claim inside the sweep
     * ([claimPending]); a catalog whose claim exceeds it is skipped
     * for the sweep. A claim is `limit` rows off one index range, so
     * the default is generous for any catalog that is not wedged.
     * `HOGLAKE_HYDRATOR_CLAIM_TIMEOUT_MS` (Config.hydratorClaimTimeoutMs).
     */
    private val claimTimeoutMs: Long = DEFAULT_CLAIM_TIMEOUT_MS,
    /**
     * Most row groups whose offsets a hydrated file stores
     * ([SplitOffsets.MAX_ROW_GROUPS]); past it the file hydrates with no
     * split_offsets. A parameter only so a test can reach the over-cap
     * path without writing a 100,001-row-group parquet file.
     */
    private val maxSplitOffsetRowGroups: Int = SplitOffsets.MAX_ROW_GROUPS,
) {
    private val log = KotlinLogging.logger {}
    private val json = ObjectMapper()

    /** The run ledger; sweep rows are recorded after the sweep commits, per claimed catalog. */
    private val runStore = MaintenanceRunStore(jdbi)

    internal data class PendingFile(
        val catalogId: Long,
        val dataFileId: Long,
        val tableId: Long,
        val path: String,
        val recordCount: Long,
        val fileSizeBytes: Long,
        val footerSize: Long?,
        val beginSnapshot: Long,
    )

    /** A retryable object-store failure: the file must STAY pending. */
    private class TransientFetchException(message: String, cause: Throwable) : RuntimeException(message, cause)

    /**
     * The sweep's claim, PER CATALOG with a fair share of [limit] (#269).
     *
     * One instance-wide claim ordered by `(catalog_id, data_file_id)`
     * served the lowest catalog id first: a catalog with a backlog sat
     * at the head of the queue and every catalog behind it waited until
     * that backlog drained, however small their own. Now each catalog
     * is claimed on its own, with an even share of the limit; a second
     * pass hands what the first left unused to the catalogs that filled
     * their share, evenly again, continuing past the rows they already
     * hold (`data_file_id > :after`, since the claim orders by it —
     * SKIP LOCKED skips OTHER transactions' locks, not our own, so a
     * re-run of the same claim would return the same rows).
     *
     * ROTATION, BY CATALOGS SERVED. When there are more catalogs than
     * the limit, each sweep can serve only `limit` of them (one row
     * each), so the start of the order advances by the number served:
     * every catalog's turn comes once per `ceil(catalogs / limit)`
     * sweeps. Advancing by ONE would serve a window of catalogs for
     * `limit` consecutive sweeps and starve the rest for the remainder
     * — at 5,000 catalogs and a limit of 1,000 that is ten days on,
     * forty off. The counter is seeded at random per replica so
     * replicas booted together do not walk the same window in step
     * (SKIP LOCKED keeps their rows disjoint; it does not spread them).
     *
     * EACH CATALOG'S CLAIM IS ITS OWN UNIT OF WORK, under a savepoint
     * and [claimTimeoutMs]: a claim that times out — a dropped table's
     * 10^7 still-pending rows, which the index holds and the hog_table
     * join discards, walked every sweep until retirement deletes them
     * — costs that catalog its share, is counted
     * (`hoglake_hydrator_claim_timeouts_total{catalog}`) and logged, and
     * the sweep goes on to the next catalog. Without the savepoint the
     * timeout aborted the ONE sweep transaction, and with it every
     * other catalog's hydration, which is the instance-wide blocking
     * this change exists to end. A claim that fails for any other
     * reason still fails the sweep: that is a database that is not
     * answering, not a catalog that is slow.
     *
     * Cost per sweep is O(catalogs) statements, one per catalog with
     * work plus one empty descent of `hog_data_file_pending` per
     * catalog without (a fraction of a millisecond each; thousands of
     * catalogs are seconds inside a fifteen-minute sweep). The pending
     * index is never walked across catalogs. Must run inside the sweep
     * transaction (the locks ARE the claim).
     */
    internal fun claimPending(
        h: Handle,
        limit: Int,
    ): List<PendingFile> {
        if (limit <= 0) return emptyList()
        val names =
            h.createQuery("SELECT catalog_id, name FROM hog_catalog ORDER BY catalog_id")
                .map { rs, _ -> rs.getLong("catalog_id") to rs.getString("name") }
                .list().toMap(java.util.LinkedHashMap())
        val catalogs = names.keys.toList()
        if (catalogs.isEmpty()) return emptyList()
        val start = Math.floorMod(rotation.get(), catalogs.size)
        val order = catalogs.drop(start) + catalogs.take(start)
        val share = maxOf(1, limit / order.size)
        val claimed = mutableListOf<PendingFile>()
        val filled = mutableListOf<Pair<Long, Long>>() // catalog -> last id claimed, when its share filled
        var served = 0
        // The claim's statement_timeout for the length of the loop,
        // restored after it (set_config's `is_local` is SET LOCAL, and a
        // SET LOCAL survives RELEASE SAVEPOINT — hence outside the
        // savepoints, once, and put back by hand).
        val sessionTimeout =
            h.createQuery("SELECT current_setting('statement_timeout')").mapTo(String::class.java).one()
        setStatementTimeout(h, "${claimTimeoutMs}ms")
        try {
            for (catalogId in order) {
                val room = minOf(share, limit - claimed.size)
                if (room <= 0) break
                served++
                val rows =
                    claimBounded(h, catalogId, names.getValue(catalogId), after = Long.MIN_VALUE, limit = room)
                        ?: continue
                claimed += rows
                if (rows.size == room) filled += catalogId to rows.last().dataFileId
            }
            // The leftover, shared evenly among the catalogs that filled
            // their share, in the same order; what one cannot use passes
            // to the next.
            var left = filled.size
            for ((catalogId, after) in filled) {
                val remaining = limit - claimed.size
                val room = minOf(maxOf(1, remaining / left), remaining)
                left--
                if (room <= 0) break
                claimed +=
                    claimBounded(h, catalogId, names.getValue(catalogId), after = after, limit = room) ?: continue
            }
        } finally {
            setStatementTimeout(h, sessionTimeout)
        }
        // Advance past the catalogs this sweep served; by one when it
        // served them all, so the leftover's first taker rotates too.
        rotation.addAndGet(if (served >= catalogs.size) 1 else served)
        return claimed
    }

    /** Where the next sweep's catalog order starts; seeded at random so replicas differ. */
    private val rotation =
        java.util.concurrent.atomic.AtomicInteger(java.util.concurrent.ThreadLocalRandom.current().nextInt())

    private fun setStatementTimeout(
        h: Handle,
        value: String,
    ) {
        h.createQuery("SELECT set_config('statement_timeout', :t, true)").bind("t", value)
            .mapTo(String::class.java).one()
    }

    /**
     * [claimCatalog] under a savepoint: null when the claim timed out
     * (SQLSTATE 57014, `query_canceled`), which rolls back to the
     * savepoint and leaves the sweep transaction usable; any other
     * failure propagates.
     */
    private fun claimBounded(
        h: Handle,
        catalogId: Long,
        catalog: String,
        after: Long,
        limit: Int,
    ): List<PendingFile>? {
        h.savepoint(CLAIM_SAVEPOINT)
        return try {
            claimCatalog(h, catalogId, after, limit).also { h.release(CLAIM_SAVEPOINT) }
        } catch (e: Exception) {
            if (!isQueryCanceled(e)) throw e
            h.rollbackToSavepoint(CLAIM_SAVEPOINT)
            Metrics.hydratorClaimTimeout(catalog)
            log.warn {
                "hydrator claim for catalog '$catalog' exceeded ${claimTimeoutMs}ms and was skipped this " +
                    "sweep (a backlog of pending rows the claim walks and discards — a dropped table's, " +
                    "until retirement deletes them); other catalogs are unaffected"
            }
            null
        }
    }

    private fun isQueryCanceled(e: Throwable): Boolean =
        generateSequence(e) { it.cause.takeIf { c -> c !== it } }
            .any { it is java.sql.SQLException && it.sqlState == "57014" }

    /** One catalog's claim: its pending, live, undropped files after [after], locked. */
    internal fun claimCatalog(
        h: Handle,
        catalogId: Long,
        after: Long,
        limit: Int,
    ): List<PendingFile> =
        h.createQuery(CLAIM_PENDING_SQL)
            .bind("catalogId", catalogId)
            .bind("after", after)
            .bind("limit", limit)
            .map { rs, _ ->
                PendingFile(
                    catalogId = rs.getLong("catalog_id"),
                    dataFileId = rs.getLong("data_file_id"),
                    tableId = rs.getLong("table_id"),
                    path = rs.getString("path"),
                    recordCount = rs.getLong("record_count"),
                    fileSizeBytes = rs.getLong("file_size_bytes"),
                    footerSize = rs.getObject("footer_size", java.lang.Long::class.java)?.toLong(),
                    beginSnapshot = rs.getLong("begin_snapshot"),
                )
            }
            .list()

    /**
     * One sweep: hydrate up to [limit] pending files. Returns files
     * processed (transient failures included — they were claimed and
     * attempted). Each file's writes ride a savepoint so one file's DB
     * failure never poisons the sweep transaction for the rest.
     *
     * The sweep spans every catalog, so its ledger rows fan out per catalog:
     * one hog_maintenance_run row per catalog the sweep claimed files for,
     * recorded AFTER the sweep transaction commits (a no-claim sweep
     * records nothing — a catalog's waiting work is the stats_state
     * backlog, not a run).
     *
     * [limit] defaults to [DEFAULT_SWEEP_LIMIT] — see that constant for
     * why it is not 100.
     */
    fun runOnce(limit: Int = DEFAULT_SWEEP_LIMIT): Int {
        val startedAt = Instant.now()
        // Per-catalog outcome tallies, accumulated inside the sweep
        // transaction and recorded after it commits.
        val tallies = mutableMapOf<Long, MutableList<Long>>()
        val processed =
            try {
                jdbi.inTransaction<Int, Exception> { h ->
                    val pending = claimPending(h, limit)
                    for ((catalogId, files) in pending.groupBy { it.catalogId }) {
                        tallies[catalogId] = mutableListOf(files.size.toLong(), 0, 0, 0)
                    }
                    for (file in pending) {
                        // [claimed, hydrated, failed, transient]
                        val tally = tallies.getValue(file.catalogId)
                        h.savepoint(FILE_SAVEPOINT)
                        try {
                            if (hydrate(h, file)) tally[1]++ else tally[2]++
                        } catch (e: TransientFetchException) {
                            h.rollbackToSavepoint(FILE_SAVEPOINT)
                            // Transient: the file STAYS pending; the next sweep
                            // retries it. The counter is the throttle-storm trace.
                            Metrics.hydratorTransientError()
                            tally[3]++
                            log.warn(e) {
                                "transient object-store failure hydrating file ${file.dataFileId} " +
                                    "(${file.path}); leaving pending for the next sweep"
                            }
                        } catch (e: Exception) {
                            h.rollbackToSavepoint(FILE_SAVEPOINT)
                            // Structural: one bad file must not wedge the sweep.
                            log.error(e) {
                                "hydration failed for file ${file.dataFileId} (${file.path}); marking failed"
                            }
                            markFailed(h, file)
                            tally[2]++
                        }
                    }
                    pending.size
                }
            } catch (e: Throwable) {
                // The transaction rolled back. Record the sweep failure
                // for ALL claimed catalogs, never partial success tallies.
                for (catalogId in tallies.keys) {
                    runStore.recordSweepById(catalogId, MaintenanceTask.HYDRATOR, startedAt, Instant.now(), null, e)
                }
                throw e
            }
        val finishedAt = Instant.now()
        for ((catalogId, tally) in tallies) {
            runStore.recordSweepById(
                catalogId,
                MaintenanceTask.HYDRATOR,
                startedAt,
                finishedAt,
                HydratorSweepResult(
                    claimed = tally[0],
                    hydrated = tally[1],
                    failed = tally[2],
                    transient = tally[3],
                ),
            )
        }
        return processed
    }

    /**
     * Operator requeue (POST /v1/catalogs/{c}/maintenance/rehydrate):
     * flip the catalog's 'failed' files back to 'pending' — optionally
     * scoped to one table — so the sweep retries them. The recovery path
     * for structural failures whose cause was fixed (object re-uploaded,
     * registration corrected, cap raised). Recorded in the run ledger as
     * a manual hydrator run.
     */
    fun rehydrateFailed(
        catalog: String,
        namespace: String? = null,
        table: String? = null,
    ): RehydrateResult =
        runStore.recorded(catalog, MaintenanceTask.HYDRATOR, MaintenanceTrigger.MANUAL) {
            rehydrateFailedAudited(catalog, namespace, table)
        }

    private fun rehydrateFailedAudited(
        catalog: String,
        namespace: String? = null,
        table: String? = null,
    ): RehydrateResult =
        Audit.audited(
            "rehydrate",
            catalog,
            table?.let { "$namespace.$it" },
            detail = { "requeued=${it.requeued}" },
        ) {
            if ((namespace == null) != (table == null)) {
                throw HoglakeException.Validation(
                    "namespace and table must be supplied together (or neither, for the whole catalog)",
                )
            }
            jdbi.inTransactionUnchecked { h ->
                val cat = CatalogRepo.require(h, catalog)
                val tableId =
                    if (namespace != null && table != null) {
                        val ns =
                            NamespaceRepo.findLiveByName(h, cat.catalogId, namespace)
                                ?: throw HoglakeException.NotFound(
                                    "namespace '$namespace' in catalog '$catalog'",
                                )
                        val t =
                            TableRepo.findLive(h, cat.catalogId, ns.namespaceId, table)
                                ?: throw HoglakeException.NotFound(
                                    "table '$namespace.$table' in catalog '$catalog'",
                                )
                        t.tableId
                    } else {
                        null
                    }
                val requeued =
                    h.createUpdate(
                        // The named-table form is already refused by
                        // `TableRepo.findLive` above, which excludes a
                        // dropped table and 404s. This clause is the
                        // WHOLE-CATALOG form's half of the same rule:
                        // requeueing a dropped table's failed rows to
                        // 'pending' would hand the sweep work it now
                        // declines to claim, so the rows would sit
                        // 'pending' forever and the hydrator backlog
                        // gauge would report a queue nothing is draining.
                        """
                        UPDATE hog_data_file f
                           SET stats_state = 'pending'
                        WHERE f.catalog_id = :catalogId AND f.stats_state = 'failed'
                          AND f.end_snapshot IS NULL
                          AND (:tableId::bigint IS NULL OR f.table_id = :tableId)
                          AND EXISTS (
                              SELECT 1 FROM hog_table t
                              WHERE t.catalog_id = f.catalog_id
                                AND t.table_id = f.table_id
                                AND t.dropped_snapshot IS NULL)
                        """,
                    )
                        .bind("catalogId", cat.catalogId)
                        .apply {
                            if (tableId == null) bindNull("tableId", Types.BIGINT) else bind("tableId", tableId)
                        }
                        .execute()
                RehydrateResult(requeued.toLong())
            }
        }

    private fun hydrate(
        h: Handle,
        file: PendingFile,
    ): Boolean {
        val footer = readFooter(file)
        // The field-id contract check rides the footer we already hold.
        val missingFieldIds = FooterStats.missingFieldIds(footer.fileMetaData.schema)
        val footerRows = footer.blocks.sumOf { it.rowCount }
        if (footerRows != file.recordCount) {
            log.error {
                "REGISTRATION MISMATCH for file ${file.dataFileId} (${file.path}): " +
                    "parquet footer has $footerRows rows but hog_data_file.record_count " +
                    "is ${file.recordCount}; marking failed, writing no stats"
            }
            markFailed(h, file, missingFieldIds)
            return false
        }
        // Column binding: id-bearing files map by field id, a stable
        // identity — the LIVE column set is correct at any time. Id-less
        // files bind by NAME, so the names must resolve against the schema
        // the file was committed under (visible at its begin_snapshot),
        // never live-at-hydration: a drop+add-same-name between commit and
        // this sweep would otherwise land the old incarnation's stats
        // under the NEW field id, poisoning pruning on the new column.
        val columns =
            if (FooterStats.usesFieldIds(footer.fileMetaData.schema)) {
                catalogColumns(h, file, at = null)
            } else {
                catalogColumns(h, file, at = file.beginSnapshot)
            }
        // FooterStats.aggregate applies StatsSanity before it returns,
        // so these are already checked: a bound the catalog type cannot
        // decode, or one that sorts above its partner, never reaches
        // here (same rule the commit path runs on client-supplied
        // column_stats).
        val aggs = FooterStats.aggregate(footer, columns, file.path)
        for (agg in aggs) upsertStats(h, file, agg)
        // Row-group start offsets ride the same footer and the same flip,
        // so a file is never `provided` in one statement and offset-less
        // in the next. The FOOTER is authoritative: it overwrites any
        // list the registration shipped, and null (no usable list) clears
        // one, because a list the footer contradicts is worse than none.
        val splitOffsets = FooterSplitOffsets.of(footer, file.fileSizeBytes, maxSplitOffsetRowGroups)
        if (splitOffsets == null) {
            val rowGroups = footer.blocks.size
            log.debug {
                if (rowGroups > maxSplitOffsetRowGroups) {
                    "file ${file.dataFileId} (${file.path}) has $rowGroups row groups, over the " +
                        "$maxSplitOffsetRowGroups cap; storing no split_offsets (readers cut it evenly)"
                } else {
                    "file ${file.dataFileId} (${file.path}): footer gives no usable row-group " +
                        "offsets ($rowGroups row groups); storing no split_offsets"
                }
            }
        }
        val flipped =
            h.createUpdate(
                """
                UPDATE hog_data_file
                   SET stats_state = 'provided', missing_field_ids = :missingFieldIds,
                       split_offsets = :splitOffsets
                WHERE catalog_id = :catalogId AND data_file_id = :dataFileId
                  AND stats_state = 'pending'
                """,
            )
                .bind("missingFieldIds", missingFieldIds)
                .bindBigintArrayOrNull("splitOffsets", splitOffsets)
                .bind("catalogId", file.catalogId)
                .bind("dataFileId", file.dataFileId)
                .execute()
        if (flipped == 0) {
            log.warn {
                "file ${file.dataFileId} left 'pending' concurrently; stats upserted anyway"
            }
        }
        Metrics.statsHydrated("provided")
        if (missingFieldIds) {
            log.warn {
                "file ${file.dataFileId} (${file.path}) has leaves without parquet field ids; " +
                    "flagged missing_field_ids (column renames on its table are blocked while it is live)"
            }
        }
        log.debug { "hydrated file ${file.dataFileId} (${file.path}): ${aggs.size} column stats" }
        return true
    }

    private fun upsertStats(
        h: Handle,
        file: PendingFile,
        agg: FooterStats.ColumnAgg,
    ) {
        h.createUpdate(
            """
            INSERT INTO hog_file_column_stats
                (catalog_id, data_file_id, field_id, value_count, null_count,
                 nan_count, size_bytes, lower_bound, upper_bound)
            VALUES (:catalogId, :dataFileId, :fieldId, :valueCount, :nullCount,
                    :nanCount, :sizeBytes, :lowerBound, :upperBound)
            ON CONFLICT (catalog_id, data_file_id, field_id) DO UPDATE SET
                value_count = EXCLUDED.value_count,
                null_count  = EXCLUDED.null_count,
                nan_count   = EXCLUDED.nan_count,
                size_bytes  = EXCLUDED.size_bytes,
                lower_bound = EXCLUDED.lower_bound,
                upper_bound = EXCLUDED.upper_bound
            """,
        )
            .bind("catalogId", file.catalogId)
            .bind("dataFileId", file.dataFileId)
            .bind("fieldId", agg.fieldId)
            .bind("valueCount", agg.valueCount)
            .bind("nullCount", agg.nullCount)
            .bindNullableLong("nanCount", agg.nanCount)
            .bindNullableLong("sizeBytes", agg.sizeBytes)
            .bindNullableBytes("lowerBound", agg.lowerBound)
            .bindNullableBytes("upperBound", agg.upperBound)
            .execute()
    }

    /**
     * Flip to 'failed'; when the footer WAS parsed (record-count
     * mismatch), [missingFieldIds] still records the contract check.
     * Rides the sweep transaction's handle (after a rollback to the
     * per-file savepoint the transaction is healthy again).
     */
    private fun markFailed(
        h: Handle,
        file: PendingFile,
        missingFieldIds: Boolean? = null,
    ) {
        Metrics.statsHydrated("failed")
        try {
            h.createUpdate(
                """
                UPDATE hog_data_file
                   SET stats_state = 'failed',
                       missing_field_ids = COALESCE(:missingFieldIds, missing_field_ids)
                WHERE catalog_id = :catalogId AND data_file_id = :dataFileId
                  AND stats_state = 'pending'
                """,
            )
                .apply {
                    if (missingFieldIds == null) {
                        bindNull("missingFieldIds", Types.BOOLEAN)
                    } else {
                        bind("missingFieldIds", missingFieldIds)
                    }
                }
                .bind("catalogId", file.catalogId)
                .bind("dataFileId", file.dataFileId)
                .execute()
        } catch (e: Exception) {
            log.error(e) { "could not mark file ${file.dataFileId} failed" }
        }
    }

    /**
     * The table's catalog columns for stats binding: [at] null = the LIVE
     * set (end_snapshot IS NULL — field-id binding); [at] non-null = the
     * set visible at that snapshot (the versioned-row rule — name-fallback
     * binding at the file's begin_snapshot).
     */
    private fun catalogColumns(
        h: Handle,
        file: PendingFile,
        at: Long?,
    ): List<CatalogColumn> {
        data class Row(val parentFieldId: Long?, val ordinal: Int, val col: CatalogColumn)

        val rows =
            h.createQuery(
                if (at == null) {
                    """
                    SELECT field_id, parent_field_id, ordinal, name, col_type,
                           type_params::text AS type_params
                    FROM hog_column
                    WHERE catalog_id = :catalogId AND table_id = :tableId
                      AND end_snapshot IS NULL
                    """
                } else {
                    """
                    SELECT field_id, parent_field_id, ordinal, name, col_type,
                           type_params::text AS type_params
                    FROM hog_column
                    WHERE catalog_id = :catalogId AND table_id = :tableId
                      AND begin_snapshot <= :at
                      AND (end_snapshot IS NULL OR :at < end_snapshot)
                    """
                },
            )
                .bind("catalogId", file.catalogId)
                .bind("tableId", file.tableId)
                .apply { if (at != null) bind("at", at) }
                .map { rs, _ ->
                    Row(
                        parentFieldId = rs.getObject("parent_field_id", java.lang.Long::class.java)?.toLong(),
                        ordinal = rs.getInt("ordinal"),
                        col =
                            CatalogColumn(
                                fieldId = rs.getLong("field_id"),
                                name = rs.getString("name"),
                                type = ColType.fromWire(rs.getString("col_type")),
                                decimalScale =
                                    rs.getString("type_params")?.let { params ->
                                        json.readTree(params).get("scale")?.takeIf { it.isInt }?.asInt()
                                    },
                            ),
                    )
                }
                .list()

        // Assemble the tree: ordinals order SIBLINGS, so the sort has to
        // happen per parent group, not over the whole result set.
        val byParent = rows.groupBy { it.parentFieldId }

        fun build(parent: Long?): List<CatalogColumn> =
            (byParent[parent] ?: emptyList())
                .sortedBy { it.ordinal }
                .map { row ->
                    if (row.col.type.isNested) {
                        row.col.copy(children = build(row.col.fieldId))
                    } else {
                        row.col
                    }
                }
        return build(null)
    }

    // ---- footer fetch ------------------------------------------------------

    private fun readFooter(file: PendingFile): ParquetMetadata {
        val footerSize = file.footerSize
        if (footerSize != null && footerSize > 0 && footerSize + FOOTER_SUFFIX < file.fileSizeBytes) {
            try {
                val tailStart = file.fileSizeBytes - footerSize - FOOTER_SUFFIX
                // Fetch failures are classified HERE, not swallowed into the
                // whole-object fallback: a throttled tail GET must surface as
                // transient (stay pending), and a 404 as structural — falling
                // back would turn an S3 blip into a whole-object fetch that
                // can trip the size cap and wrongly fail a good registration.
                val tail =
                    try {
                        store.getTail(file.path, tailStart)
                    } catch (e: Exception) {
                        throw classifyFetchFailure(e, file.path)
                    }
                return parseFooter(
                    RegionInputFile(file.fileSizeBytes, tailStart, tail, file.path),
                )
            } catch (e: TransientFetchException) {
                throw e
            } catch (e: Exception) {
                if (isMissingObject(e)) throw e
                // The tail PARSED wrong (registered footer_size too small,
                // region misses) — the whole-object fallback is for exactly
                // this case.
                log.debug(e) {
                    "tail read of ${file.path} (footer_size=$footerSize) insufficient; " +
                        "falling back to whole-object GET"
                }
            }
        }
        // The whole-object fallback buffers the object on the heap; the cap
        // is the OOM guard. Over-cap without a usable footer_size is
        // STRUCTURAL: retrying cannot shrink the file. file_size_bytes is
        // the registered size — the only pre-GET signal we have.
        if (file.fileSizeBytes > maxWholeObjectBytes) {
            throw IllegalStateException(
                "cannot hydrate ${file.path}: no usable footer_size and file_size_bytes " +
                    "${file.fileSizeBytes} exceeds the whole-object fallback cap " +
                    "$maxWholeObjectBytes bytes (HOGLAKE_HYDRATOR_MAX_WHOLE_OBJECT_BYTES); " +
                    "re-register with a correct footer_size or raise the cap, then rehydrate",
            )
        }
        val bytes =
            try {
                store.get(file.path)
            } catch (e: Exception) {
                throw classifyFetchFailure(e, file.path)
            }
        return parseFooter(RegionInputFile(bytes.size.toLong(), 0, bytes, file.path))
    }

    /**
     * Structural iff the object is definitively absent (NoSuchKey / bare
     * 404); everything else the store can throw — throttle, 5xx, auth
     * hiccups, connection resets, timeouts — is transient: retrying is
     * free (the file just stays pending) and correct once the condition
     * clears, whereas a terminal 'failed' would permanently de-stat every
     * file swept through one throttle storm.
     */
    private fun classifyFetchFailure(
        e: Exception,
        path: String,
    ): Exception =
        if (isMissingObject(e)) {
            e
        } else {
            TransientFetchException("transient object-store failure fetching $path", e)
        }

    private fun isMissingObject(e: Throwable): Boolean =
        generateSequence(e) { it.cause }.any {
            it is NoSuchKeyException || (it is S3Exception && it.statusCode() == 404)
        }

    /**
     * [FooterParse] rather than ParquetFileReader directly: it translates
     * everything parquet-java lets escape that is not an IOException into
     * a typed refusal, so a corrupt footer reaches the sweep's structural
     * branch as a refusal and not as a bare NPE (three fuzzer-found
     * escapes so far; see FooterParse's note).
     */
    private fun parseFooter(input: InputFile): ParquetMetadata = FooterParse.parse(input)

    /**
     * A parquet-java [InputFile] over one cached byte region of the
     * object (the footer tail, or the whole object): serves reads that
     * fall entirely inside the region and refuses everything else with
     * [IOException] so the caller can fall back to a whole-object read.
     * The footer parse only touches the tail (footer length + magic,
     * then the thrift footer), so a correct footer_size never leaves
     * the region.
     */
    private class RegionInputFile(
        private val totalLength: Long,
        private val regionStart: Long,
        private val region: ByteArray,
        private val uri: String,
    ) : InputFile {
        override fun getLength(): Long = totalLength

        override fun newStream(): SeekableInputStream = RegionStream()

        private inner class RegionStream : SeekableInputStream() {
            private var pos = 0L

            private fun regionOffset(
                offset: Long,
                len: Int,
            ): Int {
                if (offset < regionStart || offset + len > regionStart + region.size) {
                    throw IOException(
                        "range [$offset, +$len) outside cached region " +
                            "[$regionStart, ${regionStart + region.size}) of $uri",
                    )
                }
                return Math.toIntExact(offset - regionStart)
            }

            override fun getPos(): Long = pos

            override fun seek(newPos: Long) {
                pos = newPos
            }

            override fun read(): Int {
                if (pos >= totalLength) return -1
                val idx = regionOffset(pos, 1)
                pos += 1
                return region[idx].toInt() and 0xFF
            }

            override fun read(
                b: ByteArray,
                off: Int,
                len: Int,
            ): Int {
                if (len == 0) return 0
                if (pos >= totalLength) return -1
                val n = Math.toIntExact(minOf(len.toLong(), totalLength - pos))
                val idx = regionOffset(pos, n)
                System.arraycopy(region, idx, b, off, n)
                pos += n
                return n
            }

            override fun readFully(bytes: ByteArray) = readFully(bytes, 0, bytes.size)

            override fun readFully(
                bytes: ByteArray,
                start: Int,
                len: Int,
            ) {
                if (pos + len > totalLength) throw EOFException("read past end of $uri")
                val idx = regionOffset(pos, len)
                System.arraycopy(region, idx, bytes, start, len)
                pos += len
            }

            override fun read(buf: ByteBuffer): Int {
                val len = buf.remaining()
                if (len == 0) return 0
                if (pos >= totalLength) return -1
                val n = Math.toIntExact(minOf(len.toLong(), totalLength - pos))
                val idx = regionOffset(pos, n)
                buf.put(region, idx, n)
                pos += n
                return n
            }

            override fun readFully(buf: ByteBuffer) {
                val len = buf.remaining()
                if (pos + len > totalLength) throw EOFException("read past end of $uri")
                val idx = regionOffset(pos, len)
                buf.put(region, idx, len)
                pos += len
            }
        }
    }

    companion object {
        /**
         * The sweep's claim, `internal` so the plan test EXPLAINs the
         * SQL PRODUCTION runs rather than a lookalike (AGENT.md: V14's
         * first index was proven against a predicate no code path
         * issues).
         *
         * A DROPPED TABLE'S PENDING FILES ARE NOT WORK. Since #193 a
         * drop touches no file row, so a dropped table's `pending` rows
         * stay pending and this sweep would otherwise keep claiming
         * them — fetching footers from S3 to write stats onto rows the
         * retirement loop is about to delete, and holding row locks on
         * a dropped table while it does. The claim is ordered by
         * data_file_id within the catalog, so a big dropped table's
         * pending backlog would sit at the HEAD of the catalog's queue
         * and starve every live table behind it, forever, on a catalog
         * whose floor has not reached the drop yet. The join discards
         * them, but the index range still WALKS them — index-only
         * since V26 put `table_id` in the index's payload, one memoized
         * hog_table probe per table, and still O(backlog) per sweep
         * until retirement deletes the rows. [claimPending]'s
         * per-catalog timeout is what bounds that walk's cost to the
         * one catalog.
         *
         * It is also what lets the retirement batch's victim select
         * drop its `FOR UPDATE`: the hydrator was the only row-level
         * writer that could otherwise touch a dropped table's file
         * rows.
         *
         * `FOR UPDATE OF f`, not a bare `FOR UPDATE`: the join must not
         * take row locks on `hog_table`, which every DDL path writes.
         * The scan stays driven by `hog_data_file_pending` (the partial
         * index on `stats_state = 'pending' AND end_snapshot IS NULL`,
         * `(catalog_id, data_file_id)`), probed by `catalog_id` and
         * continued past `:after`, with a primary-key probe into
         * `hog_table` per claimed row — asserted in
         * `HydratorClaimPlanIntegrationTest`, because a join added to a
         * hot loop with no plan test is how an index stops being used
         * without anything saying so.
         *
         * `f.end_snapshot IS NULL` (#269): an ENDED pending row — the
         * file was compacted away, or its snapshot expired, before the
         * hydrator reached it — is not work either. Its stats would be
         * written onto a row no scan can return, after a footer read
         * from the object store for nothing. The term is IN THE INDEX'S
         * PREDICATE (V26), not merely in this WHERE: a row the claim
         * skips must not stay in the index the claim walks, or every
         * sweep would walk the catalog's whole ended-pending residue —
         * unbounded on a retention-NULL catalog, where nothing deletes
         * ended rows — before reaching its first live row. With it in
         * the predicate, the UPDATE that ends a row drops it from the
         * index at that moment. The same rule keeps `rehydrateFailed`
         * from requeueing an ended failed row: the sweep would never
         * take it, and the backlog gauge would count it forever.
         */
        internal const val CLAIM_PENDING_SQL: String =
            """
            SELECT f.catalog_id, f.data_file_id, f.table_id, f.path, f.record_count,
                   f.file_size_bytes, f.footer_size, f.begin_snapshot
            FROM hog_data_file f
            JOIN hog_table t
              ON t.catalog_id = f.catalog_id AND t.table_id = f.table_id
            WHERE f.catalog_id = :catalogId
              AND f.stats_state = 'pending'
              AND f.data_file_id > :after
              AND f.end_snapshot IS NULL
              AND t.dropped_snapshot IS NULL
            ORDER BY f.data_file_id
            LIMIT :limit
            FOR UPDATE OF f SKIP LOCKED
            """

        /**
         * Files claimed per sweep.
         *
         * 100 was a placeholder that outlived its assumption. Hydration
         * is a backfill for files registered WITHOUT stats, and the
         * steady-state queue is empty because every writer in the fleet
         * ships its own footer — so the batch size never mattered and a
         * small one looked prudent.
         *
         * A writer that defers stats breaks that assumption, and then
         * the arithmetic is unforgiving: the loop takes its batch and
         * sleeps the whole interval whether one file waited or ten
         * thousand did. At 100 per sweep on a 15-minute loop that is
         * ~400 files/hour per deployment, against a deferred-stats
         * writer producing them far faster. Measured on gigahog-dev: a
         * 13,224-file backlog draining at ~800/hour while still growing.
         *
         * A pending file is unprunable on EVERY column until it is
         * hydrated, so the backlog is not just a queue — it is files no
         * reader can skip.
         *
         * 10,000 was sized off the per-file cost rather than a guess:
         * ~150ms each (a ranged footer read plus a catalog write, from
         * the same dev measurement), so a full batch is minutes of work
         * and only ever runs when that much has genuinely accumulated.
         * In practice that sized the wrong thing. The sweep is ONE
         * transaction, claimed FOR UPDATE SKIP LOCKED, so 10,000 files
         * is minutes of row locks and one commit's worth of WAL held
         * open on the catalog database — and any failure rolls the
         * whole sweep back, not one file.
         *
         * 1,000 keeps the drain rate that matters: at ~150ms each a
         * full batch is ~2.5 minutes against a 15-minute interval, so
         * a backlog still drains at ~4,000/hour — five times the rate
         * that let the 13,224-file backlog grow under 100 — while the
         * worst-case transaction is a tenth the length and a failure
         * loses a tenth the work.
         */
        const val DEFAULT_SWEEP_LIMIT = 1_000

        /** 4-byte footer length + 4-byte "PAR1" magic at the end of the file. */
        private const val FOOTER_SUFFIX = 8L

        /** Default whole-object fallback cap: 256 MiB. */
        const val DEFAULT_MAX_WHOLE_OBJECT_BYTES: Long = 256L * 1024 * 1024

        /** Per-file savepoint name inside the sweep transaction. */
        private const val FILE_SAVEPOINT = "hoglake_hydrate_file"

        private const val CLAIM_SAVEPOINT = "hoglake_hydrate_claim"

        /**
         * [Hydrator.claimTimeoutMs]'s default. A claim is one range of
         * `hog_data_file_pending` bounded by its LIMIT; ten seconds is
         * four orders of magnitude over a healthy one and well under the
         * session's 60 s, so the sweep spends at most this per wedged
         * catalog and never its whole statement budget on one.
         */
        const val DEFAULT_CLAIM_TIMEOUT_MS: Long = 10_000
    }
}

private fun Update.bindNullableLong(
    name: String,
    value: Long?,
): Update = if (value == null) bindNull(name, Types.BIGINT) else bind(name, value)

private fun Update.bindNullableBytes(
    name: String,
    value: ByteArray?,
): Update = if (value == null) bindNull(name, Types.BINARY) else bind(name, value)
