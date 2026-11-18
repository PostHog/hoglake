package com.posthog.hoglake.hoglite

import com.posthog.hoglake.compaction.CompactionCandidate
import com.posthog.hoglake.compaction.CompactionClaimKey
import com.posthog.hoglake.compaction.CompactionConfig
import com.posthog.hoglake.compaction.CompactionExecutor
import com.posthog.hoglake.compaction.CompactionGroup
import com.posthog.hoglake.compaction.CompactionService.GroupOutcome
import com.posthog.hoglake.compaction.CompactionStorage
import com.posthog.hoglake.compaction.CompactionWork.Bucket
import com.posthog.hoglake.compaction.CompactionWork.CandidateFetch
import com.posthog.hoglake.compaction.CompactionWork.PlanWithContext
import com.posthog.hoglake.compaction.CompactionWork.PlannedWork
import com.posthog.hoglake.compaction.CompactionWork.TableContext
import com.posthog.hoglake.compaction.CompactionWork.WorkItem
import com.posthog.hoglake.compaction.LiveDv
import com.posthog.hoglake.compaction.MemoryRefusalMemory
import com.posthog.hoglake.compaction.VariantLayoutMemory
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CompactionResult
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.ObjectPrefix
import com.posthog.hoglake.model.ObjectPrefixLayout
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.service.CompactionBackend
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.Collections
import java.util.IdentityHashMap
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * The request owns this adapter. Both backends use CompactionExecutor.
 * Each table plan returns at most candidateBudget candidates.
 * Indexed candidate pages hold at most 1,000 rows.
 * Publication uses primary-key probes on at most maxInputFiles inputs.
 * No write transaction spans object-store work.
 *
 * One recorded 100,004-input live fixture took 5.52 seconds, about 55 microseconds per input.
 * This includes setup, capped planning, one rewrite, and plan checks.
 * It is fixture timing, not a production latency estimate.
 * An old tenant without the bucket index needs one capped read with a temporary sort.
 */
internal class HogliteCompaction(
    private val store: HogliteCatalogStore,
    objectStore: ObjectStore,
    private val defaults: CompactionConfig,
    private val beforeCommit: (catalog: String, group: CompactionGroup) -> Unit = { _, _ -> },
    private val cursors: HogliteCompactionCursors = HogliteCompactionCursors(),
) : CompactionBackend {
    private val log = KotlinLogging.logger {}
    private val storage = HogliteStorage()
    private val executor = CompactionExecutor(storage, objectStore, cursors.variantLayouts, cursors.memoryRefusals)

    override fun runOnce(
        catalog: String,
        batchOverride: Int?,
        trigger: MaintenanceTrigger,
    ): CompactionResult =
        runOnce(catalog, defaults.copy(maxGroupsPerRun = batchOverride ?: defaults.maxGroupsPerRun), trigger)

    fun runOnce(
        catalog: String,
        cfg: CompactionConfig,
        trigger: MaintenanceTrigger = MaintenanceTrigger.MANUAL,
    ): CompactionResult = executor.runOnce(catalog, cfg, trigger)

    private class PlanningState(val head: Long, val schemaVersion: Long, val dataPath: String) {
        var candidates: List<Long> = emptyList()
    }

    /** Local table churn is a recoverable plan failure, not a missing bound catalog. */
    private class TableNoLongerLive(tableId: Long) : RuntimeException("table $tableId is no longer live")

    private val states = Collections.synchronizedMap(IdentityHashMap<TableContext, PlanningState>())
    private val catalogNames = ConcurrentHashMap<Long, String>()
    private val operations = ConcurrentHashMap<Long, HogliteCatalogStore.CatalogOperation>()
    private val tableIds = ConcurrentHashMap<Triple<String, String, String>, Long>()
    private val indexes = ConcurrentHashMap<String, Boolean>()
    private val claims = ConcurrentHashMap<Triple<String, Long, String>, List<Long>>()

    private fun interrupted() {
        if (Thread.currentThread().isInterrupted) throw InterruptedException("compaction cancelled")
    }

    private fun catalogId(catalog: String): Long =
        cursors.catalogId(store.boundTenant(catalog)).also {
            catalogNames[it] = catalog
            operations[it] = store.captureCatalogOperation(catalog)
        }

    private fun <T> operation(
        catalogId: Long,
        body: () -> T,
    ): T = store.withCatalogOperation(catalogNames.getValue(catalogId), operations.getValue(catalogId), body)

    private fun context(
        catalog: String,
        tableId: Long,
        cfg: CompactionConfig,
    ): TableContext {
        // Read the head, schema fence, data path and current names together.
        val row =
            store.queryCatalog(
                catalog,
                "SELECT m.next_snapshot - 1, t.schema_version, m.data_path, v.namespace, v.name FROM tables t " +
                    "JOIN table_versions v ON v.table_id = t.id AND v.end_snapshot IS NULL CROSS JOIN meta m " +
                    "WHERE t.id = ? AND t.dropped_snapshot IS NULL AND m.id = 1",
                tableId,
            ).firstRow() ?: throw TableNoLongerLive(tableId)
        val state = PlanningState(row[0].asLong(), row[1].asLong(), row[2].asString())
        val columns = store.columnsAt(catalog, tableId, state.head)
        val sort = store.sortSpecAt(catalog, tableId, state.head)
        val prefixField = store.layoutAt(catalog, tableId, state.head)
        val prefixColumn = prefixField?.let { id -> columns.singleOrNull { it.fieldId == id } }
        val prefix =
            prefixColumn?.takeIf {
                it.def.type in ObjectPrefix.ALLOWED_TYPES
            }?.let { ObjectPrefixLayout(it.fieldId, it.def.name, it.def.type) }
        return TableContext(
            catalogId = catalogId(catalog), catalog = catalog,
            namespace = row[3].asString(), table = row[4].asString(), tableId = tableId,
            dataPath = state.dataPath, columns = columns,
            sortId = sort?.sortId, sortFields = sort?.fields ?: emptyList(),
            sortBeginSnapshot = sort?.beginSnapshot,
            sortSpill = if (sort?.fields?.isNotEmpty() == true) cfg.sortSpill(columns) else null,
            variantCompaction = cfg.variantCompactionFor(catalog),
            objectPrefixFieldId = prefixField, objectPrefix = prefix,
            prefixKeyIndex = mutableMapOf(), inputOpenParallelism = cfg.inputOpenParallelism,
            commitLockTimeoutMs = cfg.commitLockTimeoutMs, maxNodesPerRow = cfg.maxNodesPerRow, codec = cfg.codec,
        ).also { states[it] = state }
    }

    private fun emptyFetch(
        ctx: TableContext,
        cfg: CompactionConfig,
    ) = CandidateFetch(ctx, cfg.targetBytes, emptyMap(), 0, 0, 0, false)

    private fun fetch(
        catalog: String,
        namespace: String,
        table: String,
        cfg: CompactionConfig,
    ): CandidateFetch {
        interrupted()
        val tableId =
            tableIds[Triple(catalog, namespace, table)]
                ?: throw HoglakeException.NotFound("table $namespace.$table")
        // The executor joins each table's workers before it fetches the next table.
        // Retain candidate IDs and schema fences only for the current table.
        states.clear()
        val ctx = context(catalog, tableId, cfg)
        if (!executor.canPlan(ctx)) return emptyFetch(ctx, cfg)
        val m = states.getValue(ctx)
        val indexed = indexes.getValue(catalog)
        val verifiedOnly =
            if (ctx.objectPrefixFieldId != null) {
                "AND f.stats_state = 'provided' AND f.prefix_quarantined = 0 "
            } else {
                ""
            }
        val rows = mutableListOf<List<Any?>>()
        val cursorKey = store.boundTenant(catalog) to tableId
        val start = cursors.get(cursorKey)
        var after = start
        val budget = cfg.candidateBudget
        val candidateIndex = if (indexed) " INDEXED BY files_compaction_bucket" else ""
        var exhausted = false
        while (rows.size < budget) {
            // Old tenants have no bucket-order index. One capped read avoids
            // re-walking and re-sorting their full history for every page.
            val limit = if (indexed) minOf(PAGE_SIZE, budget - rows.size) else budget - rows.size
            interrupted()
            val page =
                store.queryCatalog(
                    catalog,
                    "SELECT f.id, f.path, f.record_count, f.file_size_bytes, f.footer_size, " +
                        "f.row_id_start, f.explicit_row_ids, f.begin_snapshot, f.spec_id, " +
                        "f.partition_values_json, d.delete_file_id, d.path, d.delete_count " +
                        "FROM files f$candidateIndex LEFT JOIN dvs d ON d.data_file_id = f.id " +
                        "AND d.end_snapshot IS NULL " +
                        "WHERE f.table_id = ? AND f.end_snapshot IS NULL AND f.file_size_bytes < ? " +
                        "AND f.begin_snapshot <= ? " +
                        verifiedOnly +
                        "AND (COALESCE(f.spec_id, -1), COALESCE(f.partition_values_json, ''), " +
                        "f.file_size_bytes, f.id) > (?, ?, ?, ?) " +
                        "ORDER BY COALESCE(f.spec_id, -1), COALESCE(f.partition_values_json, ''), " +
                        "f.file_size_bytes, f.id LIMIT ?",
                    tableId,
                    cfg.targetBytes,
                    m.head,
                    after.specId,
                    after.valuesJson,
                    after.bytes,
                    after.fileId,
                    limit,
                ).rows
            rows.addAll(page)
            if (page.size < limit) {
                exhausted = true
                break
            }
            after = candidatePosition(page.last())
        }
        val truncated = !exhausted
        val candidates =
            rows.map { r ->
                @Suppress("UNCHECKED_CAST")
                val partitionValues =
                    (r[9] as? String)?.let {
                        store.json.readValue(it, Array<String>::class.java).toList()
                    }
                CompactionCandidate(
                    dataFileId = r[0].asLong(),
                    path = r[1].asString(),
                    recordCount = r[2].asLong(),
                    fileSizeBytes = r[3].asLong(),
                    footerSize = r[4].asLongOrNull(),
                    rowIdStart = r[5].asLong(),
                    explicitRowIds = r[6].asLong() != 0L,
                    dv =
                        r[10].asLongOrNull()?.let { dvId ->
                            LiveDv(dvId, r[11].asString(), r[12].asLong())
                        },
                    beginSnapshot = r[7].asLong(),
                ) to Pair(r[8].asLongOrNull(), partitionValues)
            }
        val byBucket = candidates.groupBy({ Bucket(it.second.first, it.second.second) }) { it.first }
        if (ctx.objectPrefixFieldId != null) {
            for (ids in byBucket.keys.mapNotNull { it.specId }.distinct().chunked(PAGE_SIZE)) {
                val specs =
                    store.queryCatalog(
                        catalog,
                        "SELECT spec_id, fields_json FROM partition_specs WHERE table_id = ? " +
                            "AND spec_id IN (${ids.joinToString(",") { "?" }})",
                        tableId,
                        *ids.toTypedArray(),
                    ).rows
                for (row in specs) {
                    val fields = store.json.readValue(row[1].asString(), Array<PartitionFieldDef>::class.java).toList()
                    val indexes = ctx.prefixKeyIndex as MutableMap<Long, Int?>
                    indexes[row[0].asLong()] = ObjectPrefix.keyIndex(fields, ctx.objectPrefixFieldId)
                }
            }
        }
        m.candidates = candidates.map { it.first.dataFileId }
        // Retain at most minInputFiles - 1 rows from the last contiguous bucket.
        // The window always advances: candidateBudget is at least minInputFiles.
        // This progress rule does not pack groups or repeat admission rules.
        var next = HogliteCompactionCursors.START
        if (!exhausted && rows.isNotEmpty()) {
            val tail =
                byBucket.getValue(Bucket(candidates.last().second.first, candidates.last().second.second))
                    .takeLast(cfg.minInputFiles - 1)
            val first =
                tail.firstOrNull()?.let {
                        candidate ->
                    rows.indexOfFirst { it[0].asLong() == candidate.dataFileId }
                } ?: rows.size
            next = if (first == 0) start else candidatePosition(rows[first - 1])
        }
        cursors.put(cursorKey, next)
        return CandidateFetch(
            ctx,
            cfg.targetBytes,
            byBucket,
            rows.size.toLong(),
            byBucket.size.toLong(),
            byBucket.size.toLong(),
            truncated,
        )
    }

    private fun candidatePosition(row: List<Any?>) =
        HogliteCompactionCursors.Position(
            row[8].asLongOrNull() ?: -1,
            row[9] as? String ?: "",
            row[3].asLong(),
            row[0].asLong(),
        )

    private fun purgeExpiredClaims(catalog: String) {
        val now = HogliteSchema.now()
        val expired = store.queryCatalog(catalog, EXPIRED_CLAIMS_SQL, now).rows
        if (expired.isEmpty()) return
        val tx = HogliteTx()
        for (row in expired) {
            tx.add(
                "DELETE FROM compaction_claims WHERE table_id = ? AND group_key = ? AND expires_at <= ?",
                row[0].asLong(),
                row[1].asString(),
                now,
            )
        }
        store.runTx(catalog, HogliteCatalogStore.freshTxid("compact-purge"), null, tx)
    }

    /** Reserve every input by primary key. One busy input rolls back the entire acquisition. */
    private fun claimGroup(
        catalog: String,
        tableId: Long,
        group: CompactionGroup,
        claimant: UUID,
        ttlSeconds: Long,
    ): Boolean {
        val now = java.time.Instant.now()
        val tx = HogliteTx()
        for (file in group.files) {
            tx.add(
                "INSERT INTO compaction_claims(table_id, group_key, input_file_ids_json, claimant, " +
                    "claimed_at, expires_at) VALUES (?, ?, ?, ?, ?, ?) " +
                    "ON CONFLICT(table_id, group_key) DO UPDATE SET " +
                    "claimant = excluded.claimant, claimed_at = excluded.claimed_at, " +
                    "expires_at = excluded.expires_at " +
                    "WHERE compaction_claims.expires_at <= excluded.claimed_at",
                tableId,
                claimKey(file.dataFileId),
                store.json.writeValueAsString(listOf(file.dataFileId)),
                claimant.toString(),
                HogliteSchema.fmt(now),
                HogliteSchema.fmt(now.plusSeconds(ttlSeconds)),
            )
            tx.expectOne("SELECT changes()") { HoglakeException.CommitConflict(CLAIM_BUSY) }
        }
        return try {
            store.runTx(catalog, HogliteCatalogStore.freshTxid("compact-claim"), null, tx)
            true
        } catch (e: HoglakeException.CommitConflict) {
            if (e.message == CLAIM_BUSY) false else throw e
        }
    }

    private fun claimKey(fileId: Long) = "file:$fileId"

    private inner class HogliteStorage : CompactionStorage {
        override fun <T> withPlanningSession(body: (CompactionStorage.PlanningSession) -> T): T =
            body(
                object : CompactionStorage.PlanningSession {
                    override fun fetch(
                        catalog: String,
                        namespace: String,
                        table: String,
                        cfg: CompactionConfig,
                    ): CandidateFetch = this@HogliteCompaction.fetch(catalog, namespace, table, cfg)

                    override fun liveClaimedFileIds(ctx: TableContext): Set<Long> {
                        val result = hashSetOf<Long>()
                        val now = HogliteSchema.now()
                        for (ids in states.getValue(ctx).candidates.chunked(PAGE_SIZE)) {
                            interrupted()
                            val keys = ids.joinToString(",") { "?" }
                            val rows =
                                store.queryCatalog(
                                    ctx.catalog,
                                    "SELECT group_key FROM compaction_claims WHERE table_id = ? " +
                                        "AND group_key IN ($keys) AND expires_at > ?",
                                    ctx.tableId,
                                    *ids.map(::claimKey).toTypedArray(),
                                    now,
                                ).rows
                            rows.mapTo(result) { it[0].asString().removePrefix("file:").toLong() }
                        }
                        return result
                    }

                    override fun reserve(
                        planned: PlanWithContext,
                        cfg: CompactionConfig,
                        room: Int,
                        claimant: UUID,
                    ): PlannedWork {
                        val ctx = planned.ctx
                        val items = mutableListOf<WorkItem>()
                        var busy = 0L
                        try {
                            for (group in planned.plan.groups) {
                                if (items.size >= room) break
                                interrupted()
                                val acquired =
                                    !cfg.claimsEnabled ||
                                        claimGroup(ctx.catalog, ctx.tableId, group, claimant, cfg.claimTtlSeconds)
                                if (!acquired) {
                                    busy++
                                    continue
                                }
                                val key = CompactionClaimKey.groupKey(group)
                                if (cfg.claimsEnabled) {
                                    claims[Triple(ctx.catalog, ctx.tableId, key)] = group.files.map { it.dataFileId }
                                }
                                items += WorkItem(ctx, group, claimant.takeIf { cfg.claimsEnabled })
                            }
                        } catch (error: Throwable) {
                            for (item in items) if (item.claimant != null) {
                                runCatching { releaseClaim(ctx, CompactionClaimKey.groupKey(item.group), claimant) }
                            }
                            throw error
                        }
                        return PlannedWork(planned.plan.copy(claimedGroups = planned.plan.claimedGroups + busy), items)
                    }
                },
            )

        override fun liveTables(catalog: String): List<Pair<String, String>> {
            interrupted()
            store.meta(catalog)
            indexes[catalog] =
                store.queryCatalog(
                    catalog,
                    "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'files_compaction_bucket' LIMIT 1",
                ).rows.isNotEmpty()
            if (indexes[catalog] != true) {
                log.warn { "compaction has no bucket-order index; using one bounded read per table" }
            }
            val result = mutableListOf<Pair<String, String>>()
            var after = 0L
            while (true) {
                interrupted()
                val rows =
                    store.queryCatalog(
                        catalog,
                        "SELECT v.namespace, v.name, t.id FROM tables t " +
                            "JOIN table_versions v ON v.table_id = t.id AND v.end_snapshot IS NULL " +
                            "WHERE t.dropped_snapshot IS NULL AND t.id > ? ORDER BY t.id LIMIT $PAGE_SIZE",
                        after,
                    ).rows
                for (row in rows) {
                    val namespace = row[0].asString()
                    val table = row[1].asString()
                    tableIds[Triple(catalog, namespace, table)] = row[2].asLong()
                    result += namespace to table
                }
                if (rows.size < PAGE_SIZE) break
                after = rows.last()[2].asLong()
            }
            return result
        }

        override fun purgeExpired(catalog: String) = purgeExpiredClaims(catalog)

        override fun refreshClaim(
            ctx: TableContext,
            claimKey: String,
            bucketKeys: List<String>,
            claimant: UUID,
            cfg: CompactionConfig,
        ): Boolean = operation(ctx.catalogId) { refreshBound(ctx, claimKey, claimant, cfg) }

        private fun refreshBound(
            ctx: TableContext,
            claimKey: String,
            claimant: UUID,
            cfg: CompactionConfig,
        ): Boolean {
            val ids = claims[Triple(ctx.catalog, ctx.tableId, claimKey)] ?: return false
            val now = java.time.Instant.now()
            val tx = HogliteTx()
            for (id in ids) {
                tx.add(
                    "UPDATE compaction_claims SET expires_at = ? WHERE table_id = ? " +
                        "AND group_key = ? AND claimant = ? AND expires_at > ?",
                    HogliteSchema.fmt(now.plusSeconds(cfg.claimTtlSeconds)),
                    ctx.tableId,
                    this@HogliteCompaction.claimKey(id),
                    claimant.toString(),
                    HogliteSchema.fmt(now),
                )
                tx.expectOne("SELECT changes()") { HoglakeException.CommitConflict(CLAIM_BUSY) }
            }
            return try {
                store.runTx(ctx.catalog, HogliteCatalogStore.freshTxid("compact-refresh"), null, tx)
                true
            } catch (e: HoglakeException.CommitConflict) {
                if (e.message == CLAIM_BUSY) false else throw e
            }
        }

        override fun releaseClaim(
            ctx: TableContext,
            claimKey: String,
            claimant: UUID,
        ) = operation(ctx.catalogId) { releaseBound(ctx, claimKey, claimant) }

        private fun releaseBound(
            ctx: TableContext,
            claimKey: String,
            claimant: UUID,
        ) {
            val key = Triple(ctx.catalog, ctx.tableId, claimKey)
            val ids = claims[key] ?: return
            val tx = HogliteTx()
            for (id in ids) tx.add(
                "DELETE FROM compaction_claims WHERE table_id = ? AND group_key = ? AND claimant = ?",
                ctx.tableId,
                this@HogliteCompaction.claimKey(id),
                claimant.toString(),
            )
            store.runTx(ctx.catalog, HogliteCatalogStore.freshTxid("compact-release"), null, tx)
            claims.remove(key, ids)
        }

        override fun settleClaim(
            ctx: TableContext,
            claimKey: String,
            claimant: UUID,
            cfg: CompactionConfig,
            committed: Boolean,
            inputFileIds: List<Long>?,
        ) = releaseClaim(ctx, claimKey, claimant)

        override fun stageOutputPath(
            catalogId: Long,
            outputPath: String,
        ): Long =
            operation(
                catalogId,
            ) { this@HogliteCompaction.stageOutputPath(catalogNames.getValue(catalogId), outputPath) }

        override fun publish(
            ctx: TableContext,
            group: CompactionGroup,
            outputPath: String,
            outputBytes: Long,
            footerSize: Long,
            stats: List<ColumnStats>?,
            splitOffsets: List<Long>?,
            survivors: Long,
            rowIdStart: Long,
            stagingId: Long,
        ): GroupOutcome =
            operation(ctx.catalogId) {
                publishBound(
                    ctx, group, outputPath, outputBytes, footerSize, stats, splitOffsets,
                    survivors, rowIdStart, stagingId,
                )
            }

        private fun publishBound(
            ctx: TableContext,
            group: CompactionGroup,
            outputPath: String,
            outputBytes: Long,
            footerSize: Long,
            stats: List<ColumnStats>?,
            splitOffsets: List<Long>?,
            survivors: Long,
            rowIdStart: Long,
            stagingId: Long,
        ): GroupOutcome {
            beforeCommit(ctx.catalog, group)
            val prefix = executor.prefixValue(ctx, group)
            return commitGroup(
                ctx.catalog, ctx, group, outputPath, outputBytes, footerSize, stats, splitOffsets,
                survivors, rowIdStart, stagingId, prefix,
            )
        }

        override fun recorded(
            catalog: String,
            trigger: MaintenanceTrigger,
            body: () -> CompactionResult,
        ): CompactionResult = store.recorded(catalog, MaintenanceTask.COMPACTION, trigger, body)

        override fun isStatementLockTimeout(error: Throwable): Boolean = false

        override fun isTerminalFailure(error: Throwable): Boolean =
            error is HoglakeException.NotFound || error is HoglakeException.BackendUnavailable ||
                error is HoglakeException.CommitOutcomeUnknown ||
                error is InterruptedException || Thread.currentThread().isInterrupted

        override fun isRecoverablePlanningFailure(error: Throwable): Boolean =
            error is Exception && !isTerminalFailure(error)
    }

    private fun stageOutputPath(
        catalog: String,
        outputPath: String,
    ): Long {
        val tx = HogliteTx()
        // PostgreSQL defaults scheduled_at to now(). Hoglite has no default.
        // Cleanup applies its staging grace to this timestamp.
        tx.add(
            "INSERT INTO removals(path, file_kind, reason, queued_snapshot, queued_at, scheduled_at) " +
                "VALUES (?, 'data', 'compaction_staging', (SELECT next_snapshot - 1 FROM meta WHERE id = 1), ?, ?)",
            outputPath,
            HogliteSchema.now(),
            HogliteSchema.now(),
        )
        val readback = tx.add("SELECT last_insert_rowid()")
        return store.runTx(catalog, HogliteCatalogStore.freshTxid("compact-stage"), null, tx)[readback]
            .firstRow()!![0].asLong()
    }

    private fun commitGroup(
        catalog: String,
        ctx: TableContext,
        group: CompactionGroup,
        outputPath: String,
        outputBytes: Long,
        footerSize: Long,
        stats: List<ColumnStats>?,
        splitOffsets: List<Long>?,
        survivors: Long,
        rowIdStart: Long,
        stagingId: Long,
        prefixValue: String?,
    ): GroupOutcome {
        val ids = group.files.map { it.dataFileId }
        val tx = HogliteTx()
        var guardOutcome: GroupOutcome? = null

        fun guard(o: GroupOutcome): HoglakeException {
            guardOutcome = o
            return HoglakeException.CommitConflict("compaction guard refused: $o")
        }
        tx.expectOne(
            "SELECT COUNT(*) FROM removals WHERE id = ? " +
                "AND drained_at IS NULL AND claimed_at IS NULL AND attempts = 0",
            stagingId,
        ) {
            reStage(catalog, outputPath)
            guard(GroupOutcome.SkippedConflict)
        }
        tx.expectOne(
            "SELECT COUNT(*) FROM tables WHERE id = ? AND dropped_snapshot IS NULL",
            ctx.tableId,
        ) { guard(GroupOutcome.SkippedConflict) }
        tx.expectScalar(
            states.getValue(ctx).schemaVersion,
            "SELECT schema_version FROM tables WHERE id = ?",
            ctx.tableId,
        ) {
            guard(GroupOutcome.SkippedConflict)
        }
        if (ctx.sortId == null) {
            tx.expectZero(
                "SELECT COUNT(*) FROM sort_specs WHERE table_id = ? AND end_snapshot IS NULL",
                ctx.tableId,
            ) { guard(GroupOutcome.SkippedConflict) }
        } else {
            tx.expectOne(
                "SELECT COUNT(*) FROM sort_specs WHERE table_id = ? AND end_snapshot IS NULL " +
                    "AND spec_id = ?",
                ctx.tableId,
                ctx.sortId,
            ) { guard(GroupOutcome.SkippedConflict) }
        }
        tx.expectScalar(
            ids.size.toLong(),
            "SELECT COUNT(*) FROM files WHERE id IN (${ids.joinToString(",") { "?" }}) " +
                "AND end_snapshot IS NULL",
            *ids.toTypedArray(),
        ) { guard(GroupOutcome.SkippedConflict) }
        for (f in group.files) {
            val planned = f.dv
            if (planned == null) {
                tx.expectZero(
                    "SELECT COUNT(*) FROM dvs WHERE data_file_id = ? AND end_snapshot IS NULL",
                    f.dataFileId,
                ) { guard(GroupOutcome.SkippedDvSuperseded) }
            } else {
                tx.expectOne(
                    "SELECT COUNT(*) FROM dvs WHERE data_file_id = ? AND end_snapshot IS NULL " +
                        "AND delete_file_id = ?",
                    f.dataFileId,
                    planned.deleteFileId,
                ) { guard(GroupOutcome.SkippedDvSuperseded) }
            }
        }

        if (survivors == 0L) {
            tx.allocateCommitSnapshot()
            tx.insertSnapshotRow(
                author = "compaction",
                message = "retired ${group.files.size} fully-deleted files of ${ctx.namespace}.${ctx.table}",
            )
            tx.add(
                "INSERT INTO changes(snapshot_id, kind, object_id) " +
                    "VALUES (${HogliteSchema.HEAD_SQL}, 'table_compacted', ?)",
                ctx.tableId,
            )
            endSnapshotInputs(tx, ctx, ids, group)
            val readback = tx.add(HogliteSql.READBACK_COMMIT)
            val results =
                try {
                    store.runTx(catalog, HogliteCatalogStore.freshTxid("compact-retire"), null, tx)
                } catch (e: HoglakeException.CommitConflict) {
                    val o = guardOutcome
                    if (o != null) return o
                    throw e
                }
            return GroupOutcome.Retired(results[readback].firstRow()!![0].asLong())
        }
        val plannedLayout = ctx.objectPrefix
        if (ctx.objectPrefixFieldId == null) {
            tx.expectZero(
                "SELECT COUNT(*) FROM table_layouts WHERE table_id = ? AND end_snapshot IS NULL",
                ctx.tableId,
            ) { layoutMoved(ctx, outputPath).let { guard(GroupOutcome.SkippedConflict) } }
        } else if (plannedLayout != null && prefixValue != null) {
            val parsingTypes =
                ObjectPrefix.ALLOWED_TYPES.filter { ObjectPrefix.parseValue(prefixValue, it) != null }.map { it.wire }
            tx.expectOne(
                "SELECT COUNT(*) FROM table_layouts l " +
                    "JOIN table_versions v ON v.table_id = l.table_id AND v.end_snapshot IS NULL " +
                    "JOIN columns c ON c.table_id = l.table_id AND c.field_id = l.object_prefix_field_id " +
                    "AND c.end_snapshot IS NULL " +
                    "WHERE l.table_id = ? AND l.end_snapshot IS NULL AND l.object_prefix_field_id = ? " +
                    "AND v.namespace = ? AND v.name = ? AND c.name = ? " +
                    "AND c.type IN (${parsingTypes.joinToString(",") { "?" }})",
                ctx.tableId,
                ctx.objectPrefixFieldId,
                ctx.namespace,
                ctx.table,
                plannedLayout.fieldName,
                *parsingTypes.toTypedArray(),
            ) { layoutMoved(ctx, outputPath).let { guard(GroupOutcome.SkippedConflict) } }
        } else {
            error("object-prefix group of ${ctx.namespace}.${ctx.table} reached the commit without a prefix")
        }
        tx.allocateCommitSnapshot()
        tx.affectOne(
            "UPDATE meta SET next_file_id = next_file_id + 1 WHERE id = 1",
        ) { HoglakeException.NotFound("catalog (meta row missing)") }
        tx.insertSnapshotRow(
            author = "compaction",
            message = "compacted ${group.files.size} files of ${ctx.namespace}.${ctx.table}",
        )
        tx.add(
            "INSERT INTO changes(snapshot_id, kind, object_id) " +
                "VALUES (${HogliteSchema.HEAD_SQL}, 'table_compacted', ?)",
            ctx.tableId,
        )
        tx.add(
            "INSERT INTO files(id, table_id, path, file_format, record_count, file_size_bytes, " +
                "footer_size, row_id_start, explicit_row_ids, spec_id, partition_values_json, " +
                "stats_state, stats_json, split_offsets_json, begin_snapshot) " +
                "VALUES ((SELECT next_file_id - 1 FROM meta WHERE id = 1), ?, ?, 'parquet', ?, ?, ?, ?, 1, " +
                "?, ?, ?, ?, ?, " +
                "${HogliteSchema.HEAD_SQL})",
            ctx.tableId,
            outputPath,
            survivors,
            outputBytes,
            footerSize,
            rowIdStart,
            group.specId,
            group.partitionValues?.let { store.json.writeValueAsString(it) },
            if (stats != null) "provided" else "pending",
            stats?.let { store.json.writeValueAsString(it) },
            splitOffsets?.let { store.json.writeValueAsString(it) },
        )
        tx.add(
            "INSERT INTO table_totals(table_id, record_count, file_count, file_size_bytes) " +
                "VALUES (?, ?, 1, ?) " +
                "ON CONFLICT(table_id) DO UPDATE SET " +
                "record_count = record_count + excluded.record_count, " +
                "file_count = file_count + 1, " +
                "file_size_bytes = file_size_bytes + excluded.file_size_bytes",
            ctx.tableId,
            survivors,
            outputBytes,
        )
        endSnapshotInputs(tx, ctx, ids, group)
        tx.affectOne(
            "UPDATE removals SET drained_at = ?, drained_outcome = 'registered', last_attempt_at = ? " +
                "WHERE id = ? AND drained_at IS NULL AND claimed_at IS NULL AND attempts = 0",
            HogliteSchema.now(),
            HogliteSchema.now(),
            stagingId,
        ) { guard(GroupOutcome.SkippedConflict) }
        val readback = tx.add(HogliteSql.READBACK_COMMIT)
        val fileReadback = tx.add("SELECT next_file_id - 1 FROM meta WHERE id = 1")
        val results =
            try {
                store.runTx(catalog, HogliteCatalogStore.freshTxid("compact-commit"), null, tx)
            } catch (e: HoglakeException.CommitConflict) {
                val o = guardOutcome
                if (o != null) return o
                throw e
            }
        return GroupOutcome.Committed(
            results[readback].firstRow()!![0].asLong(),
            outputBytes,
            results[fileReadback].firstRow()!![0].asLong(),
        )
    }

    private fun layoutMoved(
        ctx: TableContext,
        outputPath: String,
    ) {
        log.warn {
            "compaction group for ${ctx.namespace}.${ctx.table} was written to $outputPath under " +
                "object-prefix field ${ctx.objectPrefixFieldId ?: "none"}, but the live layout no longer names " +
                "that prefix; skipping the commit. The staged output stays queued for the cleanup drain"
        }
    }

    private fun reStage(
        catalog: String,
        outputPath: String,
    ) {
        runCatching { stageOutputPath(catalog, outputPath) }
        log.warn {
            "compaction staging ticket for $outputPath was drained or claimed by cleanup " +
                "before the group committed; aborting the group and re-staging the path"
        }
    }

    private fun endSnapshotInputs(
        tx: HogliteTx,
        ctx: TableContext,
        ids: List<Long>,
        group: CompactionGroup,
    ) {
        val idList = ids.joinToString(",") { "?" }
        val inputs = "FROM files WHERE table_id = ? AND id IN ($idList) AND end_snapshot IS NULL"
        tx.add(
            "UPDATE table_totals SET " +
                "record_count = record_count - (SELECT COALESCE(SUM(record_count), 0) $inputs), " +
                "file_count = file_count - (SELECT COUNT(*) $inputs), " +
                "file_size_bytes = file_size_bytes - (SELECT COALESCE(SUM(file_size_bytes), 0) $inputs) " +
                "WHERE table_id = ?",
            ctx.tableId,
            *ids.toTypedArray(),
            ctx.tableId,
            *ids.toTypedArray(),
            ctx.tableId,
            *ids.toTypedArray(),
            ctx.tableId,
        )
        tx.add(
            "UPDATE files SET end_snapshot = ${HogliteSchema.HEAD_SQL} " +
                "WHERE table_id = ? AND id IN (${ids.joinToString(",") { "?" }}) AND end_snapshot IS NULL",
            ctx.tableId,
            *ids.toTypedArray(),
        )
        tx.expectScalar(ids.size.toLong(), "SELECT changes()") {
            HoglakeException.CommitConflict("compaction inputs changed")
        }
        for (file in group.files) {
            val dv = file.dv ?: continue
            tx.affectOne(
                "UPDATE dvs SET end_snapshot = ${HogliteSchema.HEAD_SQL} " +
                    "WHERE data_file_id = ? AND delete_file_id = ? AND end_snapshot IS NULL",
                file.dataFileId,
                dv.deleteFileId,
            ) { HoglakeException.CommitConflict("compaction vector changed") }
        }
    }

    companion object {
        const val PAGE_SIZE = 1_000
        const val EXPIRED_CLAIMS_SQL =
            "SELECT table_id, group_key FROM compaction_claims WHERE expires_at <= ? " +
                "ORDER BY table_id, group_key LIMIT $PAGE_SIZE"
        private const val CLAIM_BUSY = "compaction inputs are already claimed"
    }
}

/** Bounded, best-effort planning progress. Losing a cursor only repeats metadata work. */
internal class HogliteCompactionCursors {
    data class Position(val specId: Long, val valuesJson: String, val bytes: Long, val fileId: Long)

    val variantLayouts = VariantLayoutMemory()
    val memoryRefusals = MemoryRefusalMemory()
    private val nextCatalogId = AtomicLong()
    private val catalogIds =
        object : LinkedHashMap<String, Long>(16, 0.75f, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, Long>?) = size > 4096
        }

    @Synchronized
    fun catalogId(tenant: String): Long = catalogIds.getOrPut(tenant) { nextCatalogId.incrementAndGet() }

    private val positions =
        object : LinkedHashMap<Pair<String, Long>, Position>(16, 0.75f, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<Pair<String, Long>, Position>?) = size > 4096
        }

    @Synchronized
    fun get(key: Pair<String, Long>): Position = positions[key] ?: START

    @Synchronized
    fun put(
        key: Pair<String, Long>,
        value: Position,
    ) {
        if (value == START) positions.remove(key) else positions[key] = value
    }

    companion object {
        val START = Position(-1, "", -1, 0)
    }
}
