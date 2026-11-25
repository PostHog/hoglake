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
import com.posthog.hoglake.persistence.PartialResult
import com.posthog.hoglake.service.ExpiryService
import com.posthog.hoglake.service.HydratorMaintenanceBackend
import com.posthog.hoglake.service.Identifiers
import com.posthog.hoglake.storage.StorageAccess
import com.posthog.hoglake.storage.StorageCredentialException
import io.github.oshai.kotlinlogging.KotlinLogging

/**
 * Requeue failed live files and, with [objects], hydrate pending ones.
 *
 * Hoglite has no hydrator loop, so a requeue that succeeds then runs one bounded sweep over the same
 * scope (#675, [hydratePending]): at most [sweepLimit] live `pending` files of live tables, each started
 * within [sweepBudgetMs], read in byte-bounded pages. The sweep starts after the highest file id the
 * requeue found `failed`, the previous sweep's failures, and wraps to the lowest ids after that, so
 * files that fail on every call rotate through the sweep instead of leading it, and no pending file
 * waits on them for more than a few calls. Per file it does [Hydrator]'s work on the same footer read
 * ([FooterFetch]) and aggregate: the record-count check, the field-id audit, split offsets, the
 * object-prefix proof and the [minFileBytes] floor. Unlike [Hydrator]'s, the read is held to the
 * tenant's registration, whose sizes the tenant can rewrite: the tail to `maxFooterBytes`
 * (HOGLAKE_HOGLITE_MAX_FOOTER_BYTES) and both reads to `file_size_bytes`. Stats or split offsets over the scan's per-file
 * limit ([HogliteCatalogStore.SCAN_MAX_ROW_JSON_BYTES]) are not stored, since a scan asking for them
 * would refuse the file. The sweep records its own `hydrator` run, with the files it resolved before a
 * failure; its failure is logged and does not fail the requeue, which has already committed. Without
 * [objects] (the store's own instance) the requeue runs alone.
 *
 * Each file is one transaction, after its object-store reads. Both outcomes are fenced on the file
 * still live and `pending` and its table live at the `schema_version` read before its columns, so a
 * concurrent ALTER, drop or end leaves it alone (pending for the next run) instead of landing stats
 * bound to the old columns or a failure found against them; a promote that read the stats first
 * fences on the table's stat-less file count (HogliteAlters). A transient object-store failure leaves
 * the file pending; anything else wrong with one file marks it `failed`, and an unavailable or
 * timed-out hoglited, or a storage session the provider will not vend, stops the sweep with the file
 * still pending (the refused session's file not counted as claimed).
 *
 * Reads use the server's storage identity, and the tenant can write both `files.path` and
 * `meta.data_path` (#668), so neither may say where that identity reads. The footer reads are confined
 * to a prefix the tenant cannot write ([dataPathTemplate], HOGLAKE_HOGLITE_DATA_PATH_TEMPLATE), or,
 * without one, to the catalog's own `broker` session, which the broker binds to the catalog; a file
 * outside it is marked `failed` unread rather than having another catalog's bounds copied into this
 * one's stats. [HogliteCatalogStore.createCatalog] holds a catalog's `data_path` to the template; a
 * catalog whose `meta.data_path` is not its placement (created before the template, or rewritten) is
 * not swept. The broker is asked for `meta.data_path`, which the tenant can rewrite, so it must
 * refuse a prefix outside the catalog's own placement (docs/auth.md); the session is resolved before
 * any file is claimed, so a refusal fails the run once instead of every file's GET. With neither (an
 * `ambient` or `static` provider and no template) the requeue runs alone. A sweep that does not run
 * records a failed `hydrator` run saying why ([SweepSkipped]), so `GET /maintenance/status` shows it.
 */
internal class HogliteHydrator(
    private val store: HogliteCatalogStore,
    private val objects: (() -> ObjectStore)? = null,
    maxWholeObjectBytes: Long = Hydrator.DEFAULT_MAX_WHOLE_OBJECT_BYTES,
    maxFooterBytes: Long = DEFAULT_MAX_FOOTER_BYTES,
    private val minFileBytes: Long = 0,
    private val sweepLimit: Int = Hydrator.DEFAULT_SWEEP_LIMIT,
    private val sweepBudgetMs: Long = DEFAULT_SWEEP_BUDGET_MS,
    private val dataPathTemplate: String? = store.dataPathTemplate,
    private val pageRows: Int = PAGE_ROWS,
) : HydratorMaintenanceBackend {
    init {
        require(minFileBytes >= 0) { "minFileBytes=$minFileBytes must not be negative" }
        require(sweepLimit > 0) { "hydration sweep limit must be positive (got $sweepLimit)" }
        require(sweepBudgetMs >= 0) { "hydration sweep budget must be >= 0 (got $sweepBudgetMs)" }
        require(pageRows > 0) { "pending page rows must be positive (got $pageRows)" }
        require(dataPathTemplate == null || isDataPathTemplate(dataPathTemplate)) {
            "data path template '$dataPathTemplate' must be an s3:// URI containing $CATALOG"
        }
    }

    private val log = KotlinLogging.logger {}
    private val footers = FooterFetch(maxWholeObjectBytes, maxFooterBytes)

    override fun rehydrateFailed(
        catalog: String,
        namespace: String?,
        table: String?,
    ): RehydrateResult {
        var scope: Long? = null
        var after: Long? = null
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
                    // Read before the flip makes them pending again: the sweep starts after these.
                    if (objects != null) after = lastFailed(catalog, tableId)
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
                hydratePending(catalog, scope, MaintenanceTrigger.MANUAL, after)
            } catch (e: SweepSkipped) {
                log.warn { "rehydrate of catalog '$catalog' requeued only: ${e.message}" }
            } catch (e: Exception) {
                if (e is InterruptedException) Thread.currentThread().interrupt()
                log.error(e) { "hydration after rehydrate failed for catalog '$catalog'" }
            }
        }
        return result
    }

    /**
     * One bounded sweep of [catalog]'s pending files ([tableId]'s only, when given), recorded as a
     * `hydrator` run: ids above [after] first, then the rest in file-id order. Without a [Reach] the
     * run is recorded failed with [SweepSkipped]'s reason.
     */
    internal fun hydratePending(
        catalog: String,
        tableId: Long?,
        trigger: MaintenanceTrigger = MaintenanceTrigger.MANUAL,
        after: Long? = null,
    ): HydratorSweepResult =
        store.recorded(catalog, MaintenanceTask.HYDRATOR, trigger) {
            sweep(catalog, tableId, after, reach(catalog) ?: throw SweepSkipped())
        }

    /** No prefix the tenant cannot write bounds the footer reads, or the catalog is not at it: the ledger's error says so. */
    private class SweepSkipped(
        message: String =
            "hydration skipped: footer reads need HOGLAKE_HOGLITE_DATA_PATH_TEMPLATE or the broker storage provider",
    ) : IllegalStateException(message)

    /** Where the sweep may read: a prefix the tenant cannot write, and the store that reads it. */
    private class Reach(val dataPathPrefix: String, val view: ObjectStore)

    /**
     * [catalog]'s [Reach], or null when nothing the tenant cannot write bounds it. Throws [SweepSkipped]
     * when the catalog is not at its template placement, and the provider's refusal of its session.
     */
    private fun reach(catalog: String): Reach? {
        val base = checkNotNull(objects)()
        val created = store.meta(catalog).dataPath
        val dataPath =
            if (dataPathTemplate != null) {
                placement(dataPathTemplate, catalog).also { placed ->
                    // Its files would all be outside the bound: one failed run, not every file marked failed.
                    if (created.trimEnd('/') != placed) {
                        throw SweepSkipped(
                            "hydration skipped: data_path '${Identifiers.cap(created)}' is not the catalog's " +
                                "placement '$placed' under HOGLAKE_HOGLITE_DATA_PATH_TEMPLATE",
                        )
                    }
                }
            } else if (base.scoped) {
                // Only asks: the broker vends from its own placement and refuses a prefix outside it.
                created
            } else {
                return null
            }
        val view = base.forCatalog(catalog, dataPath, StorageAccess.READ)
        // The SDK resolves credentials per request, so a refusal would otherwise surface as each file's
        // GET failing, counted transient: resolve once, and a refusal fails the run before any claim.
        view.verifyCredentials()
        return Reach(dataPath.trimEnd('/') + "/", view)
    }

    /** The highest live `failed` file id of a live table in scope: the previous sweep's last failure, near enough. */
    private fun lastFailed(
        catalog: String,
        tableId: Long?,
    ): Long? =
        store.queryCatalog(
            catalog,
            "SELECT MAX(id) FROM files WHERE stats_state = 'failed' AND end_snapshot IS NULL" +
                (if (tableId != null) " AND +table_id = ?" else "") +
                // The requeue's scope: a dropped table's failures stay live, and would pin the cursor.
                " AND EXISTS (SELECT 1 FROM tables t WHERE t.id = files.table_id AND t.dropped_snapshot IS NULL)",
            *(if (tableId != null) arrayOf<Any?>(tableId) else emptyArray()),
        ).firstRow()?.get(0)?.asLongOrNull()

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

    /**
     * Up to [limit] live pending files of live tables with ids in ([after], [upTo]], in file-id order,
     * and whether that was the last of them. A page the engine's result cap refuses is asked again
     * at half the rows, so long paths or partition values narrow the page instead of failing it.
     */
    private fun pendingPage(
        catalog: String,
        tableId: Long?,
        after: Long?,
        upTo: Long?,
        limit: Int,
    ): Pair<List<PendingFile>, Boolean> {
        var rows = minOf(limit, pageRows)
        while (true) {
            val args =
                buildList<Any?> {
                    if (tableId != null) add(tableId)
                    if (after != null) add(after)
                    if (upTo != null) add(upTo)
                    add(rows)
                }
            val page =
                try {
                    store.queryCatalog(
                        catalog,
                        "SELECT f.id, f.table_id, f.path, f.record_count, f.file_size_bytes, f.footer_size, " +
                            "f.begin_snapshot, f.spec_id, f.partition_values_json, t.schema_version " +
                            "FROM files f JOIN tables t ON t.id = f.table_id " +
                            "WHERE f.end_snapshot IS NULL AND f.stats_state = 'pending' " +
                            "AND t.dropped_snapshot IS NULL " +
                            (if (tableId != null) "AND f.table_id = ? " else "") +
                            (if (after != null) "AND f.id > ? " else "") +
                            (if (upTo != null) "AND f.id <= ? " else "") +
                            "ORDER BY f.id LIMIT ?",
                        *args.toTypedArray(),
                    ).rows
                } catch (tooLarge: HoglakeException.BackendResultTooLarge) {
                    if (rows == 1) throw tooLarge
                    rows = maxOf(1, rows / 2)
                    continue
                }
            return page.map { r ->
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
            } to (page.size < rows)
        }
    }

    /**
     * How one sweep's claimed files resolved, so far. A file is claimed once the sweep starts on it, unless
     * its storage session is refused ([claim]).
     */
    private class Tally {
        var claimed = 0L
        var hydrated = 0L
        var failed = 0L
        var transient = 0L
        var belowFloor = 0L
        var quarantined = 0L

        fun result() =
            HydratorSweepResult(
                claimed = claimed,
                hydrated = hydrated,
                failed = failed,
                transient = transient,
                statsSkippedBelowFloor = belowFloor,
                quarantined = quarantined,
            )
    }

    /** A sweep that stopped after resolving files: the ledger row keeps what it committed. */
    private class SweepFailed(cause: Throwable, override val partial: HydratorSweepResult) :
        RuntimeException(cause.message, cause), PartialResult

    /** What the files of one page bind against, read after the page; caches span pages. */
    private class SweepContext(
        val catalog: String,
        val reach: Reach,
        val tally: Tally,
        var head: Long = 0,
        var layouts: Map<Long, Long> = emptyMap(),
        // By (table, schema_version): a later page at a newer version reads its own.
        val liveColumns: MutableMap<Pair<Long, Long>, List<CatalogColumn>> = HashMap(),
        val keyIndexes: MutableMap<Pair<Long, Long>, Int?> = HashMap(),
    )

    private fun sweep(
        catalog: String,
        tableId: Long?,
        after: Long?,
        reach: Reach,
    ): HydratorSweepResult {
        val deadline = System.nanoTime() + sweepBudgetMs * 1_000_000
        val ctx = SweepContext(catalog, reach, Tally())
        val tally = ctx.tally
        // Above the cursor, then from the lowest id up to it.
        val ranges = if (after == null) listOf(null to null) else listOf(after to null, null to after)
        try {
            ranges@ for ((from, upTo) in ranges) {
                var cursor = from
                while (true) {
                    if (tally.claimed >= sweepLimit) break@ranges
                    if (tally.claimed > 0 && System.nanoTime() >= deadline) break@ranges
                    val (page, last) = pendingPage(catalog, tableId, cursor, upTo, (sweepLimit - tally.claimed).toInt())
                    if (page.isEmpty()) break
                    // The page first, then what binds against it: a schema change after the page bumps
                    // the schema_version the flip is fenced on, so columns and layouts read later are
                    // never older.
                    ctx.head = store.meta(catalog).head
                    ctx.layouts = store.liveLayouts(catalog)
                    for (file in page) {
                        if (tally.claimed > 0 && System.nanoTime() >= deadline) break@ranges
                        claim(ctx, file)
                    }
                    if (last) break
                    cursor = page.last().id
                }
            }
        } catch (e: Exception) {
            if (e is InterruptedException || tally.claimed == 0L) throw e
            throw SweepFailed(e, tally.result())
        }
        return tally.result()
    }

    /** Claim one [file] and resolve it, or rethrow what stops the sweep. */
    private fun claim(
        ctx: SweepContext,
        file: PendingFile,
    ) {
        val catalog = ctx.catalog
        val tally = ctx.tally
        tally.claimed++
        try {
            hydrate(ctx, file)
        } catch (e: TransientFetchException) {
            // The catalog's session, not this file: every later file would ask the provider again. Nothing
            // was read, so the file is not claimed.
            if (generateSequence<Throwable>(e) { it.cause }.any { it is StorageCredentialException }) {
                tally.claimed--
                throw e
            }
            Metrics.hydratorTransientError()
            log.warn(e) {
                "transient object-store failure hydrating file ${file.id} (${file.path}) of catalog " +
                    "'$catalog'; leaving pending for the next run"
            }
            tally.transient++
        } catch (e: HoglakeException.BackendUnavailable) {
            // Nothing about this file: it stays pending, and the next run resumes with it.
            throw e
        } catch (e: HoglakeException.BackendQueryTimeout) {
            throw e
        } catch (e: Exception) {
            if (e is InterruptedException) throw e
            // One bad file must not wedge the sweep: in file-id order it would lead every later run.
            log.error(e) { "hydration failed for file ${file.id} (${file.path}) of catalog '$catalog'" }
            markFailed(ctx, file, null)
        }
    }

    /** Hydrate one claimed [file], counting its outcome in [SweepContext.tally]. */
    private fun hydrate(
        ctx: SweepContext,
        file: PendingFile,
    ) {
        val catalog = ctx.catalog
        val tally = ctx.tally
        if (!pathStaysUnder(file.path, ctx.reach.dataPathPrefix)) {
            log.error {
                "file ${file.id} (${file.path}) of catalog '$catalog' is not under its data_path " +
                    "${ctx.reach.dataPathPrefix}; marking failed without reading it"
            }
            markFailed(ctx, file, null)
            return
        }
        val footer = footers.read(FooterFetch.Target(file.path, file.fileSizeBytes, file.footerSize), ctx.reach.view)
        val schema = footer.fileMetaData.schema
        val missingFieldIds = FooterStats.missingFieldIds(schema)
        val footerRows = footer.blocks.sumOf { it.rowCount }
        if (footerRows != file.recordCount) {
            log.error {
                "REGISTRATION MISMATCH for file ${file.id} (${file.path}) of catalog '$catalog': parquet " +
                    "footer has $footerRows rows but files.record_count is ${file.recordCount}; marking failed"
            }
            markFailed(ctx, file, missingFieldIds)
            return
        }
        // [Hydrator]'s binding: id-bearing files against the live columns, id-less files by name
        // against the columns visible at their begin_snapshot, encoded under the live types.
        val live =
            ctx.liveColumns.getOrPut(file.tableId to file.schemaVersion) {
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
        // A scan that asks for either refuses a file whose value is over its limit: store none instead.
        val statsJson =
            store.json.writeValueAsString(stats).takeIf { scannable(it) } ?: run {
                log.warn { "stats of file ${file.id} (${file.path}) of catalog '$catalog' over the scan limit" }
                "[]"
            }
        val offsetsJson =
            FooterSplitOffsets.of(footer, file.fileSizeBytes)?.let { store.json.writeValueAsString(it) }
                ?.takeIf { scannable(it) }
        val tx = HogliteTx()
        tx.add(
            "UPDATE files SET stats_state = 'provided', stats_json = ?, split_offsets_json = ?, " +
                "missing_field_ids = ?, prefix_quarantined = ? WHERE $UNCHANGED",
            statsJson,
            offsetsJson,
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
        tally.hydrated++
        if (belowFloor) tally.belowFloor++
        if (mismatch != null) {
            tally.quarantined++
            log.warn {
                "file ${file.id} (${file.path}) of catalog '$catalog' quarantined: its footer does not prove " +
                    "every row holds its object-prefix value ($mismatch)"
            }
        }
        Metrics.statsHydrated(if (mismatch != null) "quarantined" else "provided")
    }

    private fun scannable(json: String): Boolean =
        json.toByteArray(Charsets.UTF_8).size <= HogliteCatalogStore.SCAN_MAX_ROW_JSON_BYTES

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

    /** Mark [file] `failed` under the flip's fence, counting it only when the row changed. */
    private fun markFailed(
        ctx: SweepContext,
        file: PendingFile,
        missingFieldIds: Boolean?,
    ) {
        val tx = HogliteTx()
        tx.add(
            "UPDATE files SET stats_state = 'failed', missing_field_ids = COALESCE(?, missing_field_ids) " +
                "WHERE $UNCHANGED",
            missingFieldIds?.let { if (it) 1L else 0L },
            file.id,
            file.schemaVersion,
        )
        val changed = tx.add("SELECT changes()")
        val marked =
            store.runTx(ctx.catalog, HogliteCatalogStore.freshTxid("hydrate-failed"), null, tx)[changed]
                .firstRow()!![0].asLong() == 1L
        if (!marked) {
            log.info { "file ${file.id} of catalog '${ctx.catalog}' changed under its hydration; not marked failed" }
            return
        }
        ctx.tally.failed++
        Metrics.statsHydrated("failed")
    }

    companion object {
        /** The expiry purge budget: the sweep runs inside the synchronous rehydrate request. */
        const val DEFAULT_SWEEP_BUDGET_MS = ExpiryService.PURGE_BUDGET_MS

        /** Default footer tail cap (HOGLAKE_HOGLITE_MAX_FOOTER_BYTES): 64 MiB. */
        const val DEFAULT_MAX_FOOTER_BYTES: Long = 64L * 1024 * 1024

        /** What a data path template ([HogliteHydrator.dataPathTemplate]) replaces with the catalog's name. */
        const val CATALOG = "{catalog}"

        /** Rows per pending page before the result cap narrows it. */
        private const val PAGE_ROWS = 100

        /**
         * Both outcomes' fence (args: file id, schema_version): the file still live and `pending`, its
         * table live and unaltered since the page.
         */
        private const val UNCHANGED =
            "id = ? AND stats_state = 'pending' AND end_snapshot IS NULL AND EXISTS (SELECT 1 FROM tables t " +
                "WHERE t.id = files.table_id AND t.schema_version = ? AND t.dropped_snapshot IS NULL)"

        fun isDataPathTemplate(template: String): Boolean = template.startsWith("s3://") && CATALOG in template

        /** [catalog]'s `data_path` under [template], without a trailing slash. */
        fun placement(
            template: String,
            catalog: String,
        ): String = template.replace(CATALOG, catalog).trimEnd('/')
    }
}
