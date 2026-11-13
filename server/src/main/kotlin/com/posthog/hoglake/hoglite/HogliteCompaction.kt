package com.posthog.hoglake.hoglite

import com.posthog.hoglake.compaction.CompactionCandidate
import com.posthog.hoglake.compaction.CompactionConfig
import com.posthog.hoglake.compaction.CompactionGroup
import com.posthog.hoglake.compaction.CompactionGrouping
import com.posthog.hoglake.compaction.CompactionService
import com.posthog.hoglake.compaction.ExternalMergeSort
import com.posthog.hoglake.compaction.InvalidDataException
import com.posthog.hoglake.compaction.LiveDv
import com.posthog.hoglake.compaction.MergeBudgetExceededException
import com.posthog.hoglake.compaction.ParquetRewriter
import com.posthog.hoglake.compaction.PuffinDeletionVector
import com.posthog.hoglake.compaction.RunCandidate
import com.posthog.hoglake.compaction.S3InputFile
import com.posthog.hoglake.compaction.S3OutputFile
import com.posthog.hoglake.compaction.SpillBudgetExceededException
import com.posthog.hoglake.compaction.UnconvertibleSchemaException
import com.posthog.hoglake.compaction.VariantLayout
import com.posthog.hoglake.hydrator.FooterSplitOffsets
import com.posthog.hoglake.hydrator.FooterStats
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.hydrator.asCatalogColumns
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CompactionResult
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.ObjectPrefix
import com.posthog.hoglake.model.ObjectPrefixLayout
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.model.allNodes
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.persistence.PartialResult
import com.posthog.hoglake.storage.StorageAccess
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.UUID

/** Plan and publish one catalog's groups. No write transaction spans object I/O.
 * TODO(shared-rules): share the group executor without changing PostgreSQL behavior.
 */
internal class HogliteCompaction(
    private val store: HogliteCatalogStore,
    private val objectStore: ObjectStore,
    private val defaults: CompactionConfig,
    private val beforeCommit: (catalog: String, group: CompactionGroup) -> Unit = { _, _ -> },
    private val cursors: HogliteCompactionCursors = HogliteCompactionCursors(),
) : com.posthog.hoglake.service.CompactionBackend {
    private val log = KotlinLogging.logger {}

    private val lastPrefixRefusal = java.util.concurrent.ConcurrentHashMap<Pair<String, Long>, String>()

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
    ): CompactionResult =
        store.recorded(catalog, MaintenanceTask.COMPACTION, trigger) {
            runSweep(catalog, cfg)
        }

    private fun runSweep(
        catalog: String,
        cfg: CompactionConfig,
    ): CompactionResult =
        Audit.audited(
            "compaction",
            catalog,
            null,
            detail = { r ->
                "groups_compacted=${r.groupsCompacted} groups_retired=${r.groupsRetired} " +
                    "files_in=${r.filesIn} files_out=${r.filesOut} " +
                    "bytes_in=${r.bytesIn} bytes_out=${r.bytesOut} skipped_conflicts=${r.skippedConflicts} " +
                    "dv_superseded=${r.dvSuperseded} unconvertible_schema=${r.unconvertibleSchema} " +
                    "invalid_data=${r.invalidData} failed_groups=${r.failedGroups} " +
                    "claimed_elsewhere=${r.claimedElsewhere} candidates_fetched=${r.candidatesFetched} " +
                    "object_prefix_unresolvable=${r.objectPrefixUnresolvable}"
            },
        ) {
            if (cfg.maxGroupsPerRun <= 0) {
                throw HoglakeException.Validation(
                    "batch (groups per run) must be positive (got ${cfg.maxGroupsPerRun})",
                )
            }
            val tally = Tally()
            val result =
                try {
                    doRunOnce(catalog, cfg, tally)
                } catch (e: Throwable) {
                    if (tally.candidatesFetched > 0 || tally.groupsCompacted > 0 || tally.failedGroups > 0) {
                        throw CompactionFailed(e, tally.result())
                    }
                    throw e
                }
            Metrics.compactionGroups(catalog, result.groupsCompacted)
            Metrics.compactionGroupsRetired(catalog, result.groupsRetired)
            Metrics.compactionFilesRewritten(catalog, result.filesIn)
            Metrics.compactionSkipped(catalog, "unconvertible_schema", result.unconvertibleSchema)
            Metrics.compactionSkipped(catalog, "invalid_data", result.invalidData)
            Metrics.compactionSkipped(catalog, "failed", result.failedGroups)
            Metrics.compactionSkipped(catalog, "claimed_elsewhere", result.claimedElsewhere)
            Metrics.compactionSkipped(catalog, "object_prefix_unresolvable", result.objectPrefixUnresolvable)
            result
        }

    private class PlanningState(val head: Long, val schemaVersion: Long, val dataPath: String)

    private class TableContext(
        val schemaVersion: Long,
        val dataPath: String,
        val namespace: String,
        val table: String,
        val tableId: Long,
        val columns: List<com.posthog.hoglake.model.Column>,
        val sortFields: List<SortFieldDef>,
        val sortId: Long?,
        val sortBeginSnapshot: Long?,
        val maxNodesPerRow: Int,
        val codec: ParquetRewriter.OutputCodec,
        val inputOpenParallelism: Int,
        val sortSpill: com.posthog.hoglake.compaction.SortSpill?,
        val variantCompaction: Boolean,
        val objectPrefixFieldId: Long? = null,
        val objectPrefix: ObjectPrefixLayout? = null,
        val prefixKeyIndex: MutableMap<Long, Int?> = mutableMapOf(),
    )

    private fun prefixValue(
        ctx: TableContext,
        group: CompactionGroup,
    ): String? {
        val layout = ctx.objectPrefix ?: return null
        val keyIndex = group.specId?.let { ctx.prefixKeyIndex[it] } ?: return null
        val raw = group.partitionValues?.getOrNull(keyIndex) ?: return null
        return raw.takeIf { ObjectPrefix.parseValue(it, layout.colType) != null }
    }

    private fun tableContext(
        catalog: String,
        m: PlanningState,
        namespace: String,
        table: String,
        tableId: Long,
        cfg: CompactionConfig,
    ): TableContext? {
        val head = m.head
        val columns = store.columnsAt(catalog, tableId, head)
        val variants = columns.allNodes().filter { it.def.type == ColType.VARIANT }
        if (variants.isNotEmpty() &&
            (!cfg.variantCompactionFor(catalog) || variants.any { VariantLayout.shape(it) == null })
        ) {
            return null
        }
        val sort = store.sortSpecAt(catalog, tableId, head)
        val sortFields = sort?.fields.orEmpty()
        val sortSpill =
            if (sortFields.isNotEmpty()) {
                cfg.sortSpill(columns)
            } else {
                null
            }
        val prefixFieldId = store.layoutAt(catalog, tableId, head)
        val prefixColumn = prefixFieldId?.let { id -> columns.firstOrNull { it.fieldId == id } }
        return TableContext(
            schemaVersion = m.schemaVersion,
            dataPath = m.dataPath,
            namespace = namespace,
            table = table,
            tableId = tableId,
            columns = columns,
            sortFields = sortFields,
            sortId = sort?.sortId,
            sortBeginSnapshot = sort?.beginSnapshot,
            maxNodesPerRow = cfg.maxNodesPerRow,
            codec = cfg.codec,
            inputOpenParallelism = cfg.inputOpenParallelism,
            sortSpill = sortSpill,
            variantCompaction = cfg.variantCompactionFor(catalog),
            objectPrefixFieldId = prefixFieldId,
            objectPrefix =
                prefixColumn
                    ?.takeIf { it.def.type in ObjectPrefix.ALLOWED_TYPES }
                    ?.let { ObjectPrefixLayout(it.fieldId, it.def.name, it.def.type) },
            prefixKeyIndex = mutableMapOf(),
        )
    }

    private class TablePlan(
        val ctx: TableContext,
        val groups: List<CompactionGroup>,
        val prefixUnresolvable: Long = 0,
        val spillRefused: Long = 0,
        val mergeRefused: Long = 0,
        val candidatesFetched: Long = 0,
        val truncated: Long = 0,
    )

    private fun planTable(
        catalog: String,
        tableId: Long,
        cfg: CompactionConfig,
        indexed: Boolean,
    ): TablePlan? {
        // The head and schema fence MUST come from one snapshot. A later fence
        // read could accept columns pinned before an intervening promotion.
        val state =
            store.queryCatalog(
                catalog,
                "SELECT m.next_snapshot - 1, t.schema_version, m.data_path, v.namespace, v.name FROM tables t " +
                    "JOIN table_versions v ON v.table_id = t.id AND v.end_snapshot IS NULL CROSS JOIN meta m " +
                    "WHERE t.id = ? AND t.dropped_snapshot IS NULL AND m.id = 1",
                tableId,
            ).firstRow() ?: return null
        val m = PlanningState(state[0].asLong(), state[1].asLong(), state[2].asString())
        val ctx = tableContext(catalog, m, state[3].asString(), state[4].asString(), tableId, cfg) ?: return null
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
        val truncated = if (exhausted) 0L else 1L
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
        val byBucket = candidates.groupBy({ it.second }) { it.first }
        if (ctx.objectPrefixFieldId != null) {
            for (ids in byBucket.keys.mapNotNull { it.first }.distinct().chunked(PAGE_SIZE)) {
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
                    ctx.prefixKeyIndex[row[0].asLong()] = ObjectPrefix.keyIndex(fields, ctx.objectPrefixFieldId)
                }
            }
        }
        val grouping = CompactionGrouping.of(cfg.targetBytes)
        val groups = mutableListOf<CompactionGroup>()
        for ((bucket, cands) in byBucket) {
            val packed =
                grouping.groups(
                    cands,
                    cfg.minInputFiles,
                    cfg.effectiveMaxInputFiles(cands.map { it.fileSizeBytes }),
                ) { it.fileSizeBytes }
            for (take in packed) {
                groups += CompactionGroup(take.sortedBy { it.rowIdStart }, bucket.first, bucket.second)
            }
        }
        // Buckets are contiguous in the read order. Retain only the unfinished
        // tail, bounded by minInputFiles - 1, so its files can meet the next page.
        // Packed-but-refused groups advance normally; they must not pin a window.
        var next = if (exhausted) HogliteCompactionCursors.START else candidatePosition(rows.last())
        if (!exhausted) {
            val packedIds = groups.flatMap { it.files }.mapTo(hashSetOf()) { it.dataFileId }
            val tail =
                byBucket.getValue(candidates.last().second)
                    .asReversed().takeWhile { it.dataFileId !in packedIds }.take(cfg.minInputFiles - 1)
            if (tail.isNotEmpty()) {
                val first = rows.indexOfFirst { it[0].asLong() == tail.last().dataFileId }
                next = if (first == 0) start else candidatePosition(rows[first - 1])
            }
        }
        cursors.put(cursorKey, next)
        val (resolvable, unresolvable) =
            if (ctx.objectPrefixFieldId == null) {
                groups to emptyList()
            } else {
                groups.partition { prefixValue(ctx, it) != null }
            }
        logPrefixRefusals(catalog, ctx, unresolvable)
        if (resolvable.isEmpty()) {
            return TablePlan(
                ctx,
                emptyList(),
                unresolvable.size.toLong(),
                candidatesFetched = rows.size.toLong(),
                truncated = truncated,
            )
        }
        var spillRefused = 0L
        var mergeRefused = 0L
        val spill = ctx.sortSpill
        val leaves = spill?.let { runCatching { ParquetRewriter.outputLeafCount(ctx.columns) }.getOrNull() }
        val fits =
            resolvable.filter { group ->
                if (spill == null || leaves == null) return@filter true
                val runs =
                    group.files.map { f ->
                        object : RunCandidate {
                            override val trustedSorted = trustedSorted(ctx, f)
                            override val fileSizeBytes = f.fileSizeBytes
                            override val survivingRecords = f.survivingRecords
                        }
                    }
                val admission =
                    try {
                        ExternalMergeSort.admit(runs, spill, leaves)
                    } catch (_: SpillBudgetExceededException) {
                        spillRefused++
                        return@filter false
                    }
                if (admission.projectedBytes > spill.mergeBudgetBytes) {
                    mergeRefused++
                    false
                } else {
                    true
                }
            }
        return TablePlan(
            ctx,
            fits.sortedWith(compareBy({ -it.files.size }, { it.files.first().rowIdStart })),
            unresolvable.size.toLong(),
            spillRefused,
            mergeRefused,
            rows.size.toLong(),
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

    private fun trustedSorted(
        ctx: TableContext,
        f: CompactionCandidate,
    ): Boolean = f.explicitRowIds && ctx.sortBeginSnapshot != null && f.beginSnapshot >= ctx.sortBeginSnapshot

    private fun logPrefixRefusals(
        catalog: String,
        ctx: TableContext,
        unresolvable: List<CompactionGroup>,
    ) {
        val key = catalog to ctx.tableId
        if (unresolvable.isEmpty()) {
            lastPrefixRefusal.remove(key)
            return
        }
        val sample = unresolvable.take(5).joinToString { "${it.specId}, ${it.partitionValues}" }
        val signature = "${unresolvable.size} ${ctx.objectPrefix} $sample"
        if (lastPrefixRefusal.put(key, signature) == signature) return
        log.warn {
            "compaction refused ${unresolvable.size} group(s) of ${ctx.namespace}.${ctx.table} whose object " +
                "prefix cannot be resolved (field_id=${ctx.objectPrefixFieldId} layout=${ctx.objectPrefix}; " +
                "a null layout is a column that is not live or not of an allowed type, which refuses every " +
                "group). First (spec_id, partition values): $sample"
        }
    }

    /** Plan outside the writer. Recheck expiry while deleting exact primary keys. */
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

    private fun releaseClaim(
        catalog: String,
        tableId: Long,
        group: CompactionGroup,
        claimant: UUID,
    ) {
        val tx = HogliteTx()
        for (file in group.files) {
            tx.add(
                "DELETE FROM compaction_claims WHERE table_id = ? AND group_key = ? AND claimant = ?",
                tableId,
                claimKey(file.dataFileId),
                claimant.toString(),
            )
        }
        store.runTx(catalog, HogliteCatalogStore.freshTxid("compact-release"), null, tx)
    }

    private fun claimKey(fileId: Long) = "file:$fileId"

    private fun doRunOnce(
        catalog: String,
        cfg: CompactionConfig,
        tally: Tally,
    ): CompactionResult {
        store.meta(catalog)
        bestEffortCompactionPurge({ purgeExpiredClaims(catalog) }) {
            log.warn(it) { "compaction claim purge failed; continuing" }
        }
        val indexed =
            store.queryCatalog(
                catalog,
                "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'files_compaction_bucket' LIMIT 1",
            ).rows.isNotEmpty()
        if (!indexed) log.warn { "compaction has no bucket-order index; using one bounded read per table" }
        val claimant = UUID.randomUUID()
        var attempts = 0
        val maxGroups = cfg.maxGroupsPerRun
        val tables =
            sequence {
                var after = 0L
                while (true) {
                    interrupted()
                    val page =
                        store.queryCatalog(
                            catalog,
                            "SELECT v.namespace, v.name, t.id FROM tables t " +
                                "JOIN table_versions v ON v.table_id = t.id AND v.end_snapshot IS NULL " +
                                "WHERE t.dropped_snapshot IS NULL AND t.id > ? ORDER BY t.id LIMIT $PAGE_SIZE",
                            after,
                        ).rows
                    yieldAll(page)
                    if (page.size < PAGE_SIZE) break
                    after = page.last()[2].asLong()
                }
            }
        for (t in tables) {
            if (attempts >= maxGroups) break
            if (Thread.currentThread().isInterrupted) throw InterruptedException()
            val (namespace, table, tableId) = Triple(t[0].asString(), t[1].asString(), t[2].asLong())
            val planned =
                try {
                    planTable(catalog, tableId, cfg, indexed)
                } catch (e: Exception) {
                    if (e is InterruptedException || Thread.currentThread().isInterrupted) throw e
                    if (e is HoglakeException.NotFound || e is HoglakeException.BackendUnavailable ||
                        e is HoglakeException.CommitOutcomeUnknown
                    ) {
                        throw e
                    }
                    tally.failedGroups++
                    log.error(e) { "compaction could not plan $catalog/$namespace.$table; continuing" }
                    continue
                } ?: continue
            val ctx = planned.ctx
            val groups = planned.groups
            tally.objectPrefixUnresolvable += planned.prefixUnresolvable
            tally.candidatesFetched += planned.candidatesFetched
            tally.candidatesTruncated += planned.truncated
            tally.spillBudgetExceeded += planned.spillRefused
            tally.mergeBudgetExceeded += planned.mergeRefused
            for (group in groups) {
                if (attempts >= maxGroups) break
                if (Thread.currentThread().isInterrupted) throw InterruptedException()
                var acquired = false
                try {
                    if (cfg.claimsEnabled &&
                        !claimGroup(catalog, tableId, group, claimant, cfg.claimTtlSeconds)
                    ) {
                        tally.claimedElsewhere++
                        continue
                    }
                    acquired = cfg.claimsEnabled
                    when (val outcome = compactGroup(catalog, ctx, group, tally)) {
                        is Outcome.Committed -> {
                            tally.groupsCompacted++
                            tally.filesIn += group.files.size
                            tally.bytesIn += group.totalBytes
                            tally.bytesOut += outcome.bytesOut
                            attempts++
                        }
                        is Outcome.Retired -> {
                            tally.groupsRetired++
                            attempts++
                        }
                        Outcome.SkippedConflict -> {
                            tally.skippedConflicts++
                            attempts++
                        }
                        Outcome.SkippedDvSuperseded -> {
                            tally.dvSuperseded++
                            attempts++
                        }
                    }
                } catch (e: SpillBudgetExceededException) {
                    tally.spillBudgetExceeded++
                    tally.work(
                        e.runsSpilled,
                        e.runsDemoted,
                        e.spillBytes,
                        e.filesVerified,
                        e.filesUnsorted,
                        e.filesUnchecked,
                    )
                    Metrics.compactionSortCheckBytes(catalog, e.sortCheckBytes)
                } catch (e: MergeBudgetExceededException) {
                    tally.mergeBudgetExceeded++
                    tally.work(
                        e.runsSpilled,
                        e.runsDemoted,
                        e.spillBytes,
                        e.filesVerified,
                        e.filesUnsorted,
                        e.filesUnchecked,
                    )
                    Metrics.compactionSortCheckBytes(catalog, e.sortCheckBytes)
                } catch (e: UnconvertibleSchemaException) {
                    tally.unconvertibleSchema++
                    attempts++
                    log.warn(e) { "compaction group of $namespace.$table: unconvertible schema" }
                } catch (e: InvalidDataException) {
                    tally.invalidData++
                    attempts++
                    log.warn(e) { "compaction group of $namespace.$table: invalid data" }
                } catch (e: OutOfMemoryError) {
                    tally.failedGroups++
                    log.error(e) { "compaction exhausted the heap; ending the sweep" }
                    return tally.result()
                } catch (e: Throwable) {
                    if (e is InterruptedException || Thread.currentThread().isInterrupted) {
                        Thread.currentThread().interrupt()
                        throw e
                    }
                    if (e is HoglakeException.NotFound || e is HoglakeException.BackendUnavailable) throw e
                    tally.failedGroups++
                    attempts++
                    log.error(e) {
                        "compaction group of ${group.files.size} files failed for " +
                            "$catalog/$namespace.$table; continuing"
                    }
                } finally {
                    if (acquired) {
                        runCatching { releaseClaim(catalog, tableId, group, claimant) }
                            .onFailure { log.warn(it) { "compaction claim release failed; the claim will expire" } }
                    }
                }
                interrupted()
            }
        }
        interrupted()
        return tally.result()
    }

    private class Tally {
        var groupsCompacted = 0L
        var groupsRetired = 0L
        var filesIn = 0L
        var bytesIn = 0L
        var bytesOut = 0L
        var skippedConflicts = 0L
        var dvSuperseded = 0L
        var unconvertibleSchema = 0L
        var invalidData = 0L
        var failedGroups = 0L
        var claimedElsewhere = 0L
        var objectPrefixUnresolvable = 0L
        var candidatesFetched = 0L
        var candidatesTruncated = 0L
        var spillBudgetExceeded = 0L
        var mergeBudgetExceeded = 0L
        var runsTrusted = 0L
        var runsSpilled = 0L
        var runsDemoted = 0L
        var spillBytes = 0L
        var spillCleanupFailures = 0L
        var filesVerified = 0L
        var filesUnsorted = 0L
        var filesUnchecked = 0L
        var rowGroupsAppended = 0L
        var bytesAppended = 0L
        var filesUnranged = 0L

        fun work(
            spilled: Int,
            demoted: Int,
            bytes: Long,
            verified: Int,
            unsorted: Int,
            unchecked: Int,
        ) {
            runsSpilled += spilled
            runsDemoted += demoted
            spillBytes += bytes
            filesVerified += verified
            filesUnsorted += unsorted
            filesUnchecked += unchecked
        }

        fun result() =
            CompactionResult(
                groupsCompacted = groupsCompacted,
                filesIn = filesIn,
                filesOut = groupsCompacted,
                bytesIn = bytesIn,
                bytesOut = bytesOut,
                skippedConflicts = skippedConflicts,
                dvSuperseded = dvSuperseded,
                unconvertibleSchema = unconvertibleSchema,
                invalidData = invalidData,
                failedGroups = failedGroups,
                claimedElsewhere = claimedElsewhere,
                candidatesFetched = candidatesFetched,
                candidatesTruncated = candidatesTruncated,
                groupsRetired = groupsRetired,
                objectPrefixUnresolvable = objectPrefixUnresolvable,
                spillBudgetExceeded = spillBudgetExceeded,
                mergeBudgetExceeded = mergeBudgetExceeded,
                runsTrusted = runsTrusted,
                runsSpilled = runsSpilled,
                runsDemoted = runsDemoted,
                spillBytes = spillBytes,
                spillCleanupFailures = spillCleanupFailures,
                filesVerified = filesVerified,
                filesUnsorted = filesUnsorted,
                filesUnchecked = filesUnchecked,
                rowGroupsAppended = rowGroupsAppended,
                bytesAppended = bytesAppended,
                filesUnranged = filesUnranged,
            )
    }

    private class CompactionFailed(cause: Throwable, override val partial: CompactionResult) :
        RuntimeException(cause.message, cause), PartialResult

    private fun interrupted() {
        if (Thread.currentThread().isInterrupted) throw InterruptedException("compaction cancelled")
    }

    internal sealed class Outcome {
        data class Committed(val snapshotId: Long, val bytesOut: Long) : Outcome()

        data class Retired(val snapshotId: Long) : Outcome()

        object SkippedConflict : Outcome()

        object SkippedDvSuperseded : Outcome()
    }

    private fun compactGroup(
        catalog: String,
        ctx: TableContext,
        group: CompactionGroup,
        tally: Tally,
    ): Outcome {
        val objects = objectStore.forCatalog(catalog, ctx.dataPath, StorageAccess.WRITE)
        val named = CompactionService.MAX_NAMED_INPUT_PATHS
        val prefixValue = prefixValue(ctx, group)
        check(ctx.objectPrefixFieldId == null || prefixValue != null) {
            "group of ${ctx.namespace}.${ctx.table} (spec_id ${group.specId}, values ${group.partitionValues}) " +
                "has no resolvable object-prefix value; refusing to write its output"
        }
        val inputs =
            group.files.map { f ->
                val source = S3InputFile(objects, f.path, f.fileSizeBytes, f.footerSize)
                val dv =
                    f.dv?.let { planned ->
                        val raw = objects.get(planned.path)
                        val decoded =
                            try {
                                PuffinDeletionVector.read(raw)
                            } catch (e: IllegalArgumentException) {
                                if (e is InvalidDataException) throw e
                                throw InvalidDataException(
                                    "DV ${planned.path} does not decode: ${e.message}",
                                )
                            }
                        if (decoded.cardinality != planned.deleteCount) {
                            throw InvalidDataException(
                                "DV ${planned.path} decodes to ${decoded.cardinality} positions " +
                                    "but is registered with delete_count ${planned.deleteCount} — " +
                                    "refusing to compact on inconsistent metadata",
                            )
                        }
                        decoded
                    }
                ParquetRewriter.Input(
                    source,
                    f.path,
                    f.rowIdStart,
                    dv,
                    f.explicitRowIds,
                    trustedSorted = trustedSorted(ctx, f),
                    fileSizeBytes = f.fileSizeBytes,
                    survivingRecords = f.survivingRecords,
                    keySource =
                        S3InputFile(
                            objects,
                            f.path,
                            f.fileSizeBytes,
                            f.footerSize,
                            readaheadBytes = S3InputFile.KEY_COLUMN_READAHEAD_BYTES,
                        ),
                )
            }
        val outputPath =
            ObjectPrefix.dataFilePath(ctx.dataPath, ctx.namespace, ctx.table, ctx.objectPrefix, prefixValue)
        val stagingId = stageOutputPath(catalog, outputPath)

        val sink = S3OutputFile(objects, outputPath)
        val rewritten =
            ParquetRewriter.rewrite(
                inputs,
                ctx.columns,
                ctx.sortFields,
                sink,
                ctx.maxNodesPerRow,
                ctx.codec,
                ctx.inputOpenParallelism,
                ctx.sortSpill,
                ParquetRewriter.APPEND_MIN_ROW_GROUP_BYTES,
                ctx.variantCompaction,
            )
        tally.work(
            rewritten.runsSpilled,
            rewritten.runsDemoted,
            rewritten.spillBytes,
            rewritten.filesVerified,
            rewritten.filesUnsorted,
            rewritten.filesUnchecked,
        )
        tally.runsTrusted += rewritten.runsTrusted
        tally.spillCleanupFailures += if (rewritten.spillCleanupFailed) 1 else 0
        tally.rowGroupsAppended += rewritten.rowGroupsAppended
        tally.bytesAppended += rewritten.bytesAppended
        tally.filesUnranged += rewritten.filesUnranged
        Metrics.compactionSortCheckBytes(catalog, rewritten.sortCheckBytes)
        check(rewritten.rowsWritten == group.survivingRecords) {
            "rewrite produced ${rewritten.rowsWritten} rows but inputs registered " +
                "${group.survivingRecords} survivors — refusing to commit a lossy compaction"
        }
        val outputBytes = sink.bytesWritten
        val footerSize = sink.footerSize
        val splitOffsets = rewritten.footer?.let { FooterSplitOffsets.of(it, outputBytes) }
        val stats =
            rewritten.footer?.let { footer ->
                FooterStats.aggregate(footer, ctx.columns.asCatalogColumns(), outputPath)
                    .map { agg ->
                        ColumnStats(
                            fieldId = agg.fieldId,
                            valueCount = agg.valueCount,
                            nullCount = agg.nullCount,
                            nanCount = agg.nanCount,
                            sizeBytes = agg.sizeBytes,
                            lowerBound = agg.lowerBound,
                            upperBound = agg.upperBound,
                        )
                    }
            }
        val layout = ctx.objectPrefix
        if (layout != null && rewritten.rowsWritten > 0) {
            val aggregate =
                checkNotNull(stats) {
                    "the rewrite of ${group.files.size} files of ${ctx.namespace}.${ctx.table} returned no " +
                        "footer; an object-prefix output is never registered unproven"
                }
            val verdict =
                ObjectPrefix.checkFieldStats(
                    layout,
                    ObjectPrefix.parseValue(prefixValue, layout.colType)!!,
                    aggregate.firstOrNull { it.fieldId == layout.fieldId },
                )
            if (verdict != ObjectPrefix.Verdict.OK) {
                throw InvalidDataException(
                    "output of ${group.files.size} inputs does not prove ${layout.fieldName} = $prefixValue " +
                        "for $outputPath ($verdict): an input holds rows of another value or nulls although " +
                        "its registration proved the value. Inputs (data_file_id path), first " +
                        "${minOf(group.files.size, named)}: " +
                        group.files.take(named).joinToString { "${it.dataFileId} ${it.path}" } +
                        if (group.files.size > named) {
                            "; all data_file_ids: " + group.files.joinToString { "${it.dataFileId}" }
                        } else {
                            ""
                        },
                )
            }
        }
        val rowIdStart = group.files.minOf { it.rowIdStart }
        beforeCommit(catalog, group)
        return commitGroup(
            catalog, ctx, group, outputPath, outputBytes, footerSize, stats, splitOffsets,
            rewritten.rowsWritten, rowIdStart, stagingId, prefixValue,
        )
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
    ): Outcome {
        val ids = group.files.map { it.dataFileId }
        val tx = HogliteTx()
        var guardOutcome: Outcome? = null

        fun guard(o: Outcome): HoglakeException {
            guardOutcome = o
            return HoglakeException.CommitConflict("compaction guard refused: $o")
        }
        tx.expectOne(
            "SELECT COUNT(*) FROM removals WHERE id = ? " +
                "AND drained_at IS NULL AND claimed_at IS NULL AND attempts = 0",
            stagingId,
        ) {
            reStage(catalog, outputPath)
            guard(Outcome.SkippedConflict)
        }
        tx.expectOne(
            "SELECT COUNT(*) FROM tables WHERE id = ? AND dropped_snapshot IS NULL",
            ctx.tableId,
        ) { guard(Outcome.SkippedConflict) }
        tx.expectScalar(ctx.schemaVersion, "SELECT schema_version FROM tables WHERE id = ?", ctx.tableId) {
            guard(Outcome.SkippedConflict)
        }
        if (ctx.sortId == null) {
            tx.expectZero(
                "SELECT COUNT(*) FROM sort_specs WHERE table_id = ? AND end_snapshot IS NULL",
                ctx.tableId,
            ) { guard(Outcome.SkippedConflict) }
        } else {
            tx.expectOne(
                "SELECT COUNT(*) FROM sort_specs WHERE table_id = ? AND end_snapshot IS NULL " +
                    "AND spec_id = ?",
                ctx.tableId,
                ctx.sortId,
            ) { guard(Outcome.SkippedConflict) }
        }
        tx.expectScalar(
            ids.size.toLong(),
            "SELECT COUNT(*) FROM files WHERE id IN (${ids.joinToString(",") { "?" }}) " +
                "AND end_snapshot IS NULL",
            *ids.toTypedArray(),
        ) { guard(Outcome.SkippedConflict) }
        for (f in group.files) {
            val planned = f.dv
            if (planned == null) {
                tx.expectZero(
                    "SELECT COUNT(*) FROM dvs WHERE data_file_id = ? AND end_snapshot IS NULL",
                    f.dataFileId,
                ) { guard(Outcome.SkippedDvSuperseded) }
            } else {
                tx.expectOne(
                    "SELECT COUNT(*) FROM dvs WHERE data_file_id = ? AND end_snapshot IS NULL " +
                        "AND delete_file_id = ?",
                    f.dataFileId,
                    planned.deleteFileId,
                ) { guard(Outcome.SkippedDvSuperseded) }
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
            return Outcome.Retired(results[readback].firstRow()!![0].asLong())
        }
        val plannedLayout = ctx.objectPrefix
        if (ctx.objectPrefixFieldId == null) {
            tx.expectZero(
                "SELECT COUNT(*) FROM table_layouts WHERE table_id = ? AND end_snapshot IS NULL",
                ctx.tableId,
            ) { layoutMoved(ctx, outputPath).let { guard(Outcome.SkippedConflict) } }
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
            ) { layoutMoved(ctx, outputPath).let { guard(Outcome.SkippedConflict) } }
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
        ) { guard(Outcome.SkippedConflict) }
        val readback = tx.add(HogliteSql.READBACK_COMMIT)
        val results =
            try {
                store.runTx(catalog, HogliteCatalogStore.freshTxid("compact-commit"), null, tx)
            } catch (e: HoglakeException.CommitConflict) {
                val o = guardOutcome
                if (o != null) return o
                throw e
            }
        return Outcome.Committed(results[readback].firstRow()!![0].asLong(), outputBytes)
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

internal fun bestEffortCompactionPurge(
    purge: () -> Unit,
    onFailure: (Throwable) -> Unit,
) {
    try {
        purge()
    } catch (e: Throwable) {
        if (e is InterruptedException || Thread.currentThread().isInterrupted) {
            Thread.currentThread().interrupt()
            throw e
        }
        onFailure(e)
    }
}
