package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.hydrator.CatalogColumn
import com.posthog.hoglake.hydrator.FooterFetch
import com.posthog.hoglake.hydrator.FooterSplitOffsets
import com.posthog.hoglake.hydrator.FooterStats
import com.posthog.hoglake.hydrator.Hydrator
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.hydrator.TransientFetchException
import com.posthog.hoglake.hydrator.asCatalogColumns
import com.posthog.hoglake.hydrator.withLiveTypes
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.HydratorSweepResult
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.ObjectPrefix
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.RehydrateResult
import com.posthog.hoglake.model.pathStaysUnder
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.service.ExpiryService
import com.posthog.hoglake.service.HydratorMaintenanceBackend
import com.posthog.hoglake.storage.StorageAccess
import io.github.oshai.kotlinlogging.KotlinLogging

/**
 * Requeue failed live files and, with [objects], hydrate pending ones.
 *
 * Hoglite has no hydrator loop, so a requeue that succeeds then runs one bounded sweep over the same
 * scope (#675, [hydratePending]): at most [sweepLimit] live `pending` files of live tables, each started
 * within [sweepBudgetMs]. The files already pending before the requeue come first, then the ones it
 * flipped, each group in file-id order, so files that fail on every requeue cannot hold back newer ones.
 * Per file it does [Hydrator]'s work on the same footer read ([FooterFetch]) and aggregate: the
 * record-count check, the field-id audit, split offsets, the object-prefix proof and the [minFileBytes]
 * floor. The sweep records its own `hydrator` run; its failure is logged and does not fail the requeue,
 * which has already committed. Without [objects] (the store's own instance) the requeue runs alone.
 *
 * Each file is one transaction, after its object-store reads. The flip is fenced on the file still
 * `pending` and its table live at the `schema_version` read before its columns, so a concurrent ALTER
 * leaves it pending for the next run instead of landing stats bound to the old columns; a promote that
 * read the stats first fences on the table's stat-less file count (HogliteAlters). A transient
 * object-store failure leaves the file pending; anything else wrong with one file marks it `failed`,
 * and an unavailable or timed-out hoglited stops the sweep with the file still pending.
 *
 * `files.path` is tenant-writable (#668), and the read uses the server's storage identity, so a path
 * outside the catalog's `data_path` is marked `failed` unread rather than having its bounds copied
 * into this catalog's stats. `meta.data_path` is tenant-writable too; #668 holds the full fix.
 */
internal class HogliteHydrator(
    private val store: HogliteCatalogStore,
    private val objects: (() -> ObjectStore)? = null,
    maxWholeObjectBytes: Long = Hydrator.DEFAULT_MAX_WHOLE_OBJECT_BYTES,
    private val minFileBytes: Long = 0,
    private val sweepLimit: Int = Hydrator.DEFAULT_SWEEP_LIMIT,
    private val sweepBudgetMs: Long = DEFAULT_SWEEP_BUDGET_MS,
) : HydratorMaintenanceBackend {
    init {
        require(minFileBytes >= 0) { "minFileBytes=$minFileBytes must not be negative" }
        require(sweepLimit > 0) { "hydration sweep limit must be positive (got $sweepLimit)" }
        require(sweepBudgetMs >= 0) { "hydration sweep budget must be >= 0 (got $sweepBudgetMs)" }
    }

    private val log = KotlinLogging.logger {}
    private val footers = FooterFetch(maxWholeObjectBytes)

    override fun rehydrateFailed(
        catalog: String,
        namespace: String?,
        table: String?,
    ): RehydrateResult {
        var scope: Long? = null
        var ahead: List<PendingFile> = emptyList()
        val result =
            store.recorded(catalog, MaintenanceTask.HYDRATOR, MaintenanceTrigger.MANUAL) {
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
                    store.meta(catalog)
                    val tableId =
                        if (namespace != null && table != null) {
                            store.liveTable(
                                catalog,
                                namespace,
                                table,
                            ).tableId
                        } else {
                            null
                        }
                    scope = tableId
                    // Read before the flip, so the sweep reaches these first.
                    if (objects != null) ahead = pendingPage(catalog, tableId, sweepLimit, emptyList())
                    val tx = HogliteTx()
                    tx.add(
                        "UPDATE files SET stats_state = 'pending' " +
                            "WHERE stats_state = 'failed' AND end_snapshot IS NULL " +
                            (if (tableId != null) "AND table_id = ? " else "") +
                            "AND EXISTS (SELECT 1 FROM tables t WHERE t.id = files.table_id " +
                            "AND t.dropped_snapshot IS NULL)",
                        *(if (tableId != null) arrayOf(tableId) else emptyArray()),
                    )
                    val count = tx.add("SELECT changes()")
                    val requeued =
                        store.runTx(
                            catalog,
                            HogliteCatalogStore.freshTxid("rehydrate"),
                            null,
                            tx,
                        )[count].firstRow()!![0].asLong()
                    RehydrateResult(requeued)
                }
            }
        if (objects != null) {
            try {
                hydratePending(catalog, scope, ahead = ahead)
            } catch (e: Exception) {
                if (e is InterruptedException) Thread.currentThread().interrupt()
                log.error(e) { "hydration after rehydrate failed for catalog '$catalog'" }
            }
        }
        return result
    }

    /**
     * One bounded sweep of [catalog]'s pending files ([tableId]'s only, when given), recorded as a
     * `hydrator` run: [ahead] first, then the rest in file-id order.
     */
    internal fun hydratePending(
        catalog: String,
        tableId: Long?,
        trigger: MaintenanceTrigger = MaintenanceTrigger.MANUAL,
        ahead: List<PendingFile> = emptyList(),
    ): HydratorSweepResult =
        store.recorded(catalog, MaintenanceTask.HYDRATOR, trigger) { sweep(catalog, tableId, ahead) }

    internal class PendingFile(
        val id: Long,
        val tableId: Long,
        val path: String,
        val recordCount: Long,
        val fileSizeBytes: Long,
        val footerSize: Long?,
        val beginSnapshot: Long,
        val specId: Long?,
        val partitionValuesJson: String?,
        val schemaVersion: Long,
    )

    /** Up to [limit] live pending files of live tables, in file-id order, without [exclude]. */
    private fun pendingPage(
        catalog: String,
        tableId: Long?,
        limit: Int,
        exclude: List<Long>,
    ): List<PendingFile> {
        if (limit <= 0) return emptyList()
        val args =
            buildList<Any?> {
                if (tableId != null) add(tableId)
                addAll(exclude)
                add(limit)
            }
        return store.queryCatalog(
            catalog,
            "SELECT f.id, f.table_id, f.path, f.record_count, f.file_size_bytes, f.footer_size, " +
                "f.begin_snapshot, f.spec_id, f.partition_values_json, t.schema_version " +
                "FROM files f JOIN tables t ON t.id = f.table_id " +
                "WHERE f.end_snapshot IS NULL AND f.stats_state = 'pending' AND t.dropped_snapshot IS NULL " +
                (if (tableId != null) "AND f.table_id = ? " else "") +
                (if (exclude.isNotEmpty()) "AND f.id NOT IN (${exclude.joinToString(", ") { "?" }}) " else "") +
                "ORDER BY f.id LIMIT ?",
            *args.toTypedArray(),
        ).rows.map { r ->
            PendingFile(
                id = r[0].asLong(),
                tableId = r[1].asLong(),
                path = r[2].asString(),
                recordCount = r[3].asLong(),
                fileSizeBytes = r[4].asLong(),
                footerSize = r[5].asLongOrNull(),
                beginSnapshot = r[6].asLong(),
                specId = r[7].asLongOrNull(),
                partitionValuesJson = r[8] as? String,
                schemaVersion = r[9].asLong(),
            )
        }
    }

    /** What every file of one sweep binds against, read once. */
    private class SweepContext(
        val catalog: String,
        val head: Long,
        val dataPathPrefix: String,
        val view: ObjectStore,
        val layouts: Map<Long, Long>,
        val liveColumns: MutableMap<Long, List<CatalogColumn>> = HashMap(),
        val keyIndexes: MutableMap<Pair<Long, Long>, Int?> = HashMap(),
        // [claimed, hydrated, failed, transient, stats skipped below floor, quarantined]
        val tally: LongArray = LongArray(6),
    )

    private fun sweep(
        catalog: String,
        tableId: Long?,
        ahead: List<PendingFile>,
    ): HydratorSweepResult {
        val deadline = System.nanoTime() + sweepBudgetMs * 1_000_000
        // The page first, then what binds against it: a schema change after the page bumps the
        // schema_version the flip is fenced on, so columns and layouts read later are never older.
        val pending = ahead + pendingPage(catalog, tableId, sweepLimit - ahead.size, ahead.map { it.id })
        if (pending.isEmpty()) return HydratorSweepResult(claimed = 0, hydrated = 0, failed = 0, transient = 0)
        val meta = store.meta(catalog)
        val ctx =
            SweepContext(
                catalog = catalog,
                head = meta.head,
                dataPathPrefix = meta.dataPath.trimEnd('/') + "/",
                view = checkNotNull(objects)().forCatalog(catalog, meta.dataPath, StorageAccess.READ),
                layouts = store.liveLayouts(catalog),
            )
        val tally = ctx.tally
        for (file in pending) {
            if (tally[0] > 0 && System.nanoTime() >= deadline) break
            tally[0]++
            try {
                hydrate(ctx, file)
            } catch (e: TransientFetchException) {
                Metrics.hydratorTransientError()
                log.warn(e) {
                    "transient object-store failure hydrating file ${file.id} (${file.path}) of catalog " +
                        "'$catalog'; leaving pending for the next run"
                }
                tally[3]++
            } catch (e: HoglakeException.BackendUnavailable) {
                // Nothing about this file: it stays pending, and the next run resumes with it.
                throw e
            } catch (e: HoglakeException.BackendQueryTimeout) {
                throw e
            } catch (e: Exception) {
                if (e is InterruptedException) throw e
                // One bad file must not wedge the sweep: in file-id order it would lead every later run.
                log.error(e) { "hydration failed for file ${file.id} (${file.path}) of catalog '$catalog'" }
                markFailed(catalog, file, null)
                tally[2]++
            }
        }
        return HydratorSweepResult(
            claimed = tally[0],
            hydrated = tally[1],
            failed = tally[2],
            transient = tally[3],
            statsSkippedBelowFloor = tally[4],
            quarantined = tally[5],
        )
    }

    /** Hydrate one claimed [file], counting its outcome in [SweepContext.tally]. */
    private fun hydrate(
        ctx: SweepContext,
        file: PendingFile,
    ) {
        val catalog = ctx.catalog
        val tally = ctx.tally
        if (!pathStaysUnder(file.path, ctx.dataPathPrefix)) {
            log.error {
                "file ${file.id} (${file.path}) of catalog '$catalog' is not under its data_path " +
                    "${ctx.dataPathPrefix}; marking failed without reading it"
            }
            markFailed(catalog, file, null)
            tally[2]++
            return
        }
        val footer = footers.read(FooterFetch.Target(file.path, file.fileSizeBytes, file.footerSize), ctx.view)
        val schema = footer.fileMetaData.schema
        val missingFieldIds = FooterStats.missingFieldIds(schema)
        val footerRows = footer.blocks.sumOf { it.rowCount }
        if (footerRows != file.recordCount) {
            log.error {
                "REGISTRATION MISMATCH for file ${file.id} (${file.path}) of catalog '$catalog': parquet " +
                    "footer has $footerRows rows but files.record_count is ${file.recordCount}; marking failed"
            }
            markFailed(catalog, file, missingFieldIds)
            tally[2]++
            return
        }
        // [Hydrator]'s binding: id-bearing files against the live columns, id-less files by name
        // against the columns visible at their begin_snapshot, encoded under the live types.
        val live =
            ctx.liveColumns.getOrPut(file.tableId) {
                store.columnsAt(catalog, file.tableId, ctx.head).asCatalogColumns()
            }
        val columns =
            if (FooterStats.usesFieldIds(schema)) {
                live
            } else {
                store.columnsAt(catalog, file.tableId, file.beginSnapshot).asCatalogColumns().withLiveTypes(live)
            }
        val prefixFieldId = ctx.layouts[file.tableId]
        val rawPrefix = prefixFieldId?.let { prefixValue(catalog, file, it, ctx.keyIndexes) }
        val aggs = FooterStats.aggregate(footer, columns, file.path)
        // The proof runs ahead of the floor: a file under it skips its stats, never the check.
        val mismatch = prefixFieldId?.let { Hydrator.prefixMismatch(it, rawPrefix, columns, aggs) }
        val belowFloor = file.fileSizeBytes < minFileBytes
        val stats =
            if (belowFloor) {
                emptyList()
            } else {
                aggs.map {
                    ColumnStats(
                        it.fieldId,
                        it.valueCount,
                        it.nullCount,
                        it.nanCount,
                        it.sizeBytes,
                        it.lowerBound,
                        it.upperBound,
                    )
                }
            }
        val tx = HogliteTx()
        tx.add(
            "UPDATE files SET stats_state = 'provided', stats_json = ?, split_offsets_json = ?, " +
                "missing_field_ids = ?, prefix_quarantined = ? " +
                "WHERE id = ? AND stats_state = 'pending' AND EXISTS (SELECT 1 FROM tables t " +
                "WHERE t.id = files.table_id AND t.schema_version = ? AND t.dropped_snapshot IS NULL)",
            store.json.writeValueAsString(stats),
            FooterSplitOffsets.of(footer, file.fileSizeBytes)?.let { store.json.writeValueAsString(it) },
            if (missingFieldIds) 1L else 0L,
            if (mismatch != null) 1L else 0L,
            file.id,
            file.schemaVersion,
        )
        val changed = tx.add("SELECT changes()")
        val flipped =
            store.runTx(catalog, HogliteCatalogStore.freshTxid("hydrate"), null, tx)[changed]
                .firstRow()!![0].asLong() == 1L
        if (!flipped) {
            // Hydrated by another run, ended, or its table dropped or altered since the page: whatever
            // is still pending is the next run's. Claimed only; no object-store failure happened.
            log.info { "file ${file.id} of catalog '$catalog' changed under its hydration; not flipped" }
            return
        }
        tally[1]++
        if (belowFloor) tally[4]++
        if (mismatch != null) {
            tally[5]++
            log.warn {
                "file ${file.id} (${file.path}) of catalog '$catalog' quarantined: its footer does not prove " +
                    "every row holds its object-prefix value ($mismatch)"
            }
        }
        Metrics.statsHydrated(if (mismatch != null) "quarantined" else "provided")
    }

    /**
     * The raw value [file] was registered with for the prefix field [fieldId], at its own spec's identity
     * key index; null when none resolves, which the proof quarantines ([Hydrator.prefixMismatch]).
     */
    private fun prefixValue(
        catalog: String,
        file: PendingFile,
        fieldId: Long,
        keyIndexes: MutableMap<Pair<Long, Long>, Int?>,
    ): String? {
        val specId = file.specId ?: return null
        val key = file.tableId to specId
        val keyIndex =
            if (key in keyIndexes) {
                keyIndexes[key]
            } else {
                store.queryCatalog(
                    catalog,
                    "SELECT fields_json FROM partition_specs WHERE table_id = ? AND spec_id = ?",
                    file.tableId,
                    specId,
                ).firstRow()
                    ?.let { store.json.readValue<List<PartitionFieldDef>>(it[0].asString()) }
                    ?.let { ObjectPrefix.keyIndex(it, fieldId) }
                    .also { keyIndexes[key] = it }
            } ?: return null
        return file.partitionValuesJson?.let { store.json.readValue<List<String?>>(it) }?.getOrNull(keyIndex)
    }

    private fun markFailed(
        catalog: String,
        file: PendingFile,
        missingFieldIds: Boolean?,
    ) {
        val tx = HogliteTx()
        tx.add(
            "UPDATE files SET stats_state = 'failed', missing_field_ids = COALESCE(?, missing_field_ids) " +
                "WHERE id = ? AND stats_state = 'pending'",
            missingFieldIds?.let { if (it) 1L else 0L },
            file.id,
        )
        store.runTx(catalog, HogliteCatalogStore.freshTxid("hydrate-failed"), null, tx)
        Metrics.statsHydrated("failed")
    }

    companion object {
        /** The expiry purge budget: the sweep runs inside the synchronous rehydrate request. */
        const val DEFAULT_SWEEP_BUDGET_MS = ExpiryService.PURGE_BUDGET_MS
    }
}
