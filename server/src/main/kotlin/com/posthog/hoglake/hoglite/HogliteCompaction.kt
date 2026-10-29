package com.posthog.hoglake.hoglite

import com.posthog.hoglake.compaction.CompactionCandidate
import com.posthog.hoglake.compaction.CompactionClaimRepo
import com.posthog.hoglake.compaction.CompactionConfig
import com.posthog.hoglake.compaction.CompactionGroup
import com.posthog.hoglake.compaction.CompactionGrouping
import com.posthog.hoglake.compaction.InvalidDataException
import com.posthog.hoglake.compaction.LiveDv
import com.posthog.hoglake.compaction.ParquetRewriter
import com.posthog.hoglake.compaction.PuffinDeletionVector
import com.posthog.hoglake.compaction.S3InputFile
import com.posthog.hoglake.compaction.S3OutputFile
import com.posthog.hoglake.compaction.UnconvertibleSchemaException
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
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.UUID

/**
 * Compaction on hoglite: [com.posthog.hoglake.compaction.CompactionService]'s
 * semantics over the tenant schema, with the DATA plane shared
 * (ParquetRewriter, the S3 file adapters, the DV codec, the grouping
 * arithmetic — all storage-agnostic) and the catalog plane ported.
 *
 * WHAT THE PORT KEEPS, because it is the correctness: the commit's
 * five guards, verbatim — the staging ticket must be untouched
 * (`drained_at IS NULL AND claimed_at IS NULL AND attempts = 0`), the
 * table must not have been dropped mid-rewrite, the live sort spec
 * must still be the one planned against, every input must still be
 * live, and every input must still carry exactly its planned DV —
 * then the end-snapshot of inputs (and their vectors) with the
 * row-count checks, the empty-rewrite retirement, and the ticket's
 * `registered` settle in the same transaction as the output row.
 *
 * WHAT THE PORT SIMPLIFIES, because it is efficiency rather than
 * semantics: the planner is the straightforward shape — one candidate
 * read per table (live files under the target size), grouped by
 * (spec, partition), packed by the same [CompactionGrouping] — without
 * Postgres's bucketed cursor machinery, which exists to bound a
 * 9.9M-row probe's cost on a shared database. A hoglite tenant is one
 * catalog's own file: the whole live-manifest read is the kind of
 * query it serves all day, and a new warehouse catalog's manifest is
 * small. The claims stay (cross-replica dedup is real), one
 * transaction per claim batch under hoglite's per-tenant writer
 * serialization; the bucket-claim layer (a guard against re-probing
 * cost the simple planner does not have) does not.
 *
 * Groups execute SEQUENTIALLY (the Postgres path's parallelGroups pool
 * exists to overlap S3 latency across catalogs' row locks; one tenant
 * has one writer, and the rewrite's own inputOpenParallelism already
 * overlaps the reads). And one budget accounting rule changes with
 * the shape: the sweep's interrupt bookkeeping is the loop's, not a
 * pool's.
 */
internal class HogliteCompaction(
    private val store: HogliteCatalogStore,
    private val objectStore: ObjectStore,
    private val defaults: CompactionConfig,
) : com.posthog.hoglake.service.CompactionBackend {
    private val log = KotlinLogging.logger {}

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

    fun runOnceAllCatalogs(cfg: CompactionConfig = defaults): List<Pair<String, CompactionResult>> {
        val names = store.listCatalogNames().shuffled()
        val results = mutableListOf<Pair<String, CompactionResult>>()
        for (name in names) {
            try {
                results += name to runOnce(name, cfg, MaintenanceTrigger.LOOP)
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                throw e
            } catch (e: Exception) {
                log.error(e) { "compaction sweep failed for catalog '$name'; continuing" }
            }
            if (Thread.currentThread().isInterrupted) {
                throw InterruptedException("compaction sweep cancelled after catalog '$name'")
            }
        }
        return results
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
                    "claimed_elsewhere=${r.claimedElsewhere} candidates_fetched=${r.candidatesFetched}"
            },
        ) {
            if (cfg.maxGroupsPerRun <= 0) {
                throw HoglakeException.Validation(
                    "batch (groups per run) must be positive (got ${cfg.maxGroupsPerRun})",
                )
            }
            val result = doRunOnce(catalog, cfg)
            Metrics.compactionGroups(catalog, result.groupsCompacted)
            Metrics.compactionGroupsRetired(catalog, result.groupsRetired)
            Metrics.compactionFilesRewritten(catalog, result.filesIn)
            Metrics.compactionSkipped(catalog, "unconvertible_schema", result.unconvertibleSchema)
            Metrics.compactionSkipped(catalog, "invalid_data", result.invalidData)
            Metrics.compactionSkipped(catalog, "failed", result.failedGroups)
            Metrics.compactionSkipped(catalog, "claimed_elsewhere", result.claimedElsewhere)
            result
        }

    // ---- context + planning ----------------------------------------------

    /** The live-table context the rewrite binds: columns, sort spec, layout. */
    private class TableContext(
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
    )

    private fun tableContext(
        catalog: String,
        m: HogliteCatalogStore.MetaRow,
        namespace: String,
        table: String,
        tableId: Long,
        cfg: CompactionConfig,
    ): TableContext? {
        val head = m.head
        val columns = store.columnsAt(catalog, tableId, head)
        if (columns.any { it.def.type == ColType.VARIANT } && !cfg.variantCompaction) {
            // Variant layout rewrites are gated fleet-wide; the planner
            // refuses the table rather than the group (CompactionService's
            // rule — a variant table's groups would all refuse anyway).
            return null
        }
        val sortRow =
            store.queryCatalog(
                catalog,
                "SELECT spec_id, fields_json, begin_snapshot FROM sort_specs WHERE table_id = ? " +
                    "AND end_snapshot IS NULL",
                tableId,
            ).firstRow()
        val sortFields =
            sortRow?.let {
                store.json.readValue(it[1].asString(), Array<SortFieldDef>::class.java).toList()
            } ?: emptyList()
        val sortSpill =
            if (sortFields.isNotEmpty()) {
                SortSpillFrom(cfg, columns)
            } else {
                null
            }
        return TableContext(
            dataPath = m.dataPath,
            namespace = namespace,
            table = table,
            tableId = tableId,
            columns = columns,
            sortFields = sortFields,
            sortId = sortRow?.get(0)?.asLong(),
            sortBeginSnapshot = sortRow?.get(2)?.asLong(),
            maxNodesPerRow = cfg.maxNodesPerRow,
            codec = cfg.codec,
            inputOpenParallelism = cfg.inputOpenParallelism,
            sortSpill = sortSpill,
            variantCompaction = cfg.variantCompaction,
        )
    }

    private fun SortSpillFrom(
        cfg: CompactionConfig,
        columns: List<com.posthog.hoglake.model.Column>,
    ): com.posthog.hoglake.compaction.SortSpill =
        com.posthog.hoglake.compaction.SortSpill(
            chunkRows = cfg.spillChunkRows(columns),
            mergeBudgetBytes = cfg.sortedHeapBytesPerGroup,
            spillBudgetBytes = cfg.spillBytes,
            spillDir = cfg.spillDir,
            verifyMinBytes = cfg.verifyMinBytes,
        )

    /** The plan for one table: candidates under the target, packed, claims dropped out. */
    private fun planTable(
        catalog: String,
        m: HogliteCatalogStore.MetaRow,
        namespace: String,
        table: String,
        tableId: Long,
        cfg: CompactionConfig,
    ): Pair<TableContext, List<CompactionGroup>>? {
        val ctx = tableContext(catalog, m, namespace, table, tableId, cfg) ?: return null
        val rows =
            store.queryCatalog(
                catalog,
                "SELECT f.id, f.path, f.record_count, f.file_size_bytes, f.footer_size, " +
                    "f.row_id_start, f.explicit_row_ids, f.begin_snapshot, f.spec_id, " +
                    "f.partition_values_json, d.delete_file_id, d.path, d.delete_count " +
                    "FROM files f LEFT JOIN dvs d ON d.data_file_id = f.id AND d.end_snapshot IS NULL " +
                    "WHERE f.table_id = ? AND f.end_snapshot IS NULL AND f.file_size_bytes < ? " +
                    "ORDER BY f.row_id_start",
                tableId,
                cfg.targetBytes,
            ).rows
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
        // Group by (spec, partition) — one bucket per pair, packed greedily.
        val byBucket = candidates.groupBy({ it.second }) { it.first }
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
        if (groups.isEmpty()) return ctx to emptyList()
        // Drop groups overlapping a live claim (the efficiency half; the
        // commit guards remain the correctness half).
        val claimed = liveClaimedFileIds(catalog, tableId)
        val free = groups.filter { g -> g.files.none { it.dataFileId in claimed } }
        return ctx to free.sortedWith(compareBy({ -it.files.size }, { it.files.first().rowIdStart }))
    }

    // ---- claims ------------------------------------------------------------

    private fun liveClaimedFileIds(
        catalog: String,
        tableId: Long,
    ): Set<Long> {
        val now = HogliteSchema.now()
        return store.queryCatalog(
            catalog,
            "SELECT input_file_ids_json FROM compaction_claims WHERE table_id = ? AND expires_at > ?",
            tableId,
            now,
        ).rows.flatMap { r ->
            store.json.readValue(r[0].asString(), Array<Long>::class.java).toList()
        }.toSet()
    }

    /** INSERT ... ON CONFLICT-take-if-expired, CompactionClaimRepo.acquire's semantics. */
    private fun claimGroup(
        catalog: String,
        tableId: Long,
        group: CompactionGroup,
        claimant: UUID,
        ttlSeconds: Long,
    ): Boolean {
        val now = java.time.Instant.now()
        val key = CompactionClaimRepo.groupKey(group)
        val tx = HogliteTx()
        tx.add(
            "INSERT INTO compaction_claims(table_id, group_key, input_file_ids_json, claimant, " +
                "claimed_at, expires_at) " +
                "VALUES (?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT(table_id, group_key) DO UPDATE SET " +
                "claimant = excluded.claimant, claimed_at = excluded.claimed_at, " +
                "input_file_ids_json = excluded.input_file_ids_json, expires_at = excluded.expires_at " +
                "WHERE compaction_claims.expires_at <= excluded.claimed_at",
            tableId,
            key,
            store.json.writeValueAsString(group.files.map { it.dataFileId }),
            claimant.toString(),
            HogliteSchema.fmt(now),
            HogliteSchema.fmt(now.plusSeconds(ttlSeconds)),
        )
        // The upsert reports affected = 1 on a take, 0 when the live
        // claim's guard refused the replace — exactly acquire's RETURNING.
        return store.runTx(catalog, HogliteCatalogStore.freshTxid("compact-claim"), null, tx)[0].affected == 1L
    }

    /** Renew the lease before the IO; a lost lease spends none. */
    private fun refreshClaim(
        catalog: String,
        tableId: Long,
        groupKey: String,
        claimant: UUID,
        ttlSeconds: Long,
    ): Boolean {
        val now = java.time.Instant.now()
        val tx = HogliteTx()
        tx.add(
            "UPDATE compaction_claims SET expires_at = ? " +
                "WHERE table_id = ? AND group_key = ? AND claimant = ? AND expires_at > ?",
            HogliteSchema.fmt(now.plusSeconds(ttlSeconds)),
            tableId,
            groupKey,
            claimant.toString(),
            HogliteSchema.fmt(now),
        )
        return store.runTx(catalog, HogliteCatalogStore.freshTxid("compact-refresh"), null, tx)[0].affected == 1L
    }

    private fun releaseClaim(
        catalog: String,
        tableId: Long,
        groupKey: String,
        claimant: UUID,
    ) {
        val tx = HogliteTx()
        tx.add(
            "DELETE FROM compaction_claims WHERE table_id = ? AND group_key = ? AND claimant = ?",
            tableId,
            groupKey,
            claimant.toString(),
        )
        store.runTx(catalog, HogliteCatalogStore.freshTxid("compact-release"), null, tx)
    }

    // ---- the sweep -----------------------------------------------------------

    private fun doRunOnce(
        catalog: String,
        cfg: CompactionConfig,
    ): CompactionResult {
        val m = store.meta(catalog)
        val claimant = UUID.randomUUID()
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
        var candidatesFetched = 0L
        var attempts = 0
        val maxGroups = cfg.maxGroupsPerRun
        // Live tables in name order — the Postgres sweep's iteration.
        val tables =
            store.queryCatalog(
                catalog,
                "SELECT v.namespace, v.name, t.id FROM tables t " +
                    "JOIN table_versions v ON v.table_id = t.id AND v.end_snapshot IS NULL " +
                    "WHERE t.dropped_snapshot IS NULL ORDER BY v.namespace, v.name",
            ).rows
        for (t in tables) {
            if (attempts >= maxGroups) break
            if (Thread.currentThread().isInterrupted) throw InterruptedException()
            val (namespace, table, tableId) = Triple(t[0].asString(), t[1].asString(), t[2].asLong())
            val planned = planTable(catalog, m, namespace, table, tableId, cfg) ?: continue
            val (ctx, groups) = planned
            candidatesFetched += groups.sumOf { it.files.size }
            for (group in groups) {
                if (attempts >= maxGroups) break
                if (Thread.currentThread().isInterrupted) throw InterruptedException()
                val groupKey = CompactionClaimRepo.groupKey(group)
                if (cfg.claimsEnabled &&
                    !claimGroup(catalog, tableId, group, claimant, cfg.claimTtlSeconds)
                ) {
                    claimedElsewhere++
                    continue
                }
                try {
                    if (cfg.claimsEnabled &&
                        !refreshClaim(catalog, tableId, groupKey, claimant, cfg.claimTtlSeconds)
                    ) {
                        claimedElsewhere++
                        continue
                    }
                    when (val outcome = compactGroup(catalog, ctx, group)) {
                        is Outcome.Committed -> {
                            groupsCompacted++
                            filesIn += group.files.size
                            bytesIn += group.totalBytes
                            bytesOut += outcome.bytesOut
                            attempts++
                        }
                        is Outcome.Retired -> {
                            groupsRetired++
                            attempts++
                        }
                        Outcome.SkippedConflict -> {
                            skippedConflicts++
                            attempts++
                        }
                        Outcome.SkippedDvSuperseded -> {
                            dvSuperseded++
                            attempts++
                        }
                    }
                } catch (e: UnconvertibleSchemaException) {
                    unconvertibleSchema++
                    attempts++
                    log.warn(e) { "compaction group of $namespace.$table: unconvertible schema" }
                } catch (e: InvalidDataException) {
                    invalidData++
                    attempts++
                    log.warn(e) { "compaction group of $namespace.$table: invalid data" }
                } catch (e: Exception) {
                    if (e is InterruptedException) Thread.currentThread().interrupt()
                    failedGroups++
                    attempts++
                    log.error(e) {
                        "compaction group of ${group.files.size} files failed for " +
                            "$catalog/$namespace.$table; continuing"
                    }
                } finally {
                    if (cfg.claimsEnabled) releaseClaim(catalog, tableId, groupKey, claimant)
                }
            }
        }
        return CompactionResult(
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
            groupsRetired = groupsRetired,
        )
    }

    // ---- one group ---------------------------------------------------------

    internal sealed class Outcome {
        data class Committed(val snapshotId: Long, val bytesOut: Long) : Outcome()

        data class Retired(val snapshotId: Long) : Outcome()

        object SkippedConflict : Outcome()

        object SkippedDvSuperseded : Outcome()
    }

    /** The rewrite: inputs from S3, one output object, staging ticket first. */
    private fun compactGroup(
        catalog: String,
        ctx: TableContext,
        group: CompactionGroup,
    ): Outcome {
        val inputs =
            group.files.map { f ->
                val source = S3InputFile(objectStore, f.path, f.fileSizeBytes, f.footerSize)
                val dv =
                    f.dv?.let { planned ->
                        val raw = objectStore.get(planned.path)
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
                    // A compaction output written under the live spec is a
                    // trusted run of it (CompactionService.trustedSorted).
                    trustedSorted =
                        f.explicitRowIds &&
                            ctx.sortBeginSnapshot != null &&
                            f.beginSnapshot >= ctx.sortBeginSnapshot,
                    fileSizeBytes = f.fileSizeBytes,
                    survivingRecords = f.survivingRecords,
                    keySource =
                        S3InputFile(
                            objectStore,
                            f.path,
                            f.fileSizeBytes,
                            f.footerSize,
                            readaheadBytes = S3InputFile.KEY_COLUMN_READAHEAD_BYTES,
                        ),
                )
            }
        val outputPath =
            "${ctx.dataPath.trimEnd('/')}/data/${ctx.namespace}/${ctx.table}/" +
                "${UUID.randomUUID()}.parquet"

        // The staging ticket BEFORE any bytes: the object is always named
        // by an undrained removal row, so a crash mid-rewrite leaves the
        // drain a ticket to collect (CompactionService.stageOutputPath).
        val stagingId = stageOutputPath(catalog, outputPath)

        val sink = S3OutputFile(objectStore, outputPath)
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
        val rowIdStart = group.files.minOf { it.rowIdStart }
        return commitGroup(
            catalog, ctx, group, outputPath, outputBytes, footerSize, stats, splitOffsets,
            rewritten.rowsWritten, rowIdStart, stagingId,
        )
    }

    /** The staging ticket: an undrained 'compaction_staging' removal row. */
    private fun stageOutputPath(
        catalog: String,
        outputPath: String,
    ): Long {
        val tx = HogliteTx()
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

    /**
     * The metadata commit for one group: ONE guarded hoglite transaction
     * carrying CompactionService.commitGroup's five guards verbatim.
     */
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
    ): Outcome {
        val ids = group.files.map { it.dataFileId }
        val tx = HogliteTx()
        // Guard failures are not errors here — they are the race the
        // OCC contract exists for, and each decodes to a planned
        // outcome, not an exception. HogliteTx's failure registry is
        // typed to HoglakeException (hoglake's sealed model — it cannot
        // be extended from this package), so the outcome travels
        // side-channel: the lambda records it and returns the typed
        // conflict hoglite's 409 requires, and the catch below maps it.
        var guardOutcome: Outcome? = null

        fun guard(o: Outcome): HoglakeException {
            guardOutcome = o
            return HoglakeException.CommitConflict("compaction guard refused: $o")
        }

        // GUARD 1: the staging ticket is untouched — a claimed/drained/
        // attempted one means cleanup may already have deleted the object
        // (commitGroup's three-term predicate, verbatim).
        tx.expectOne(
            "SELECT COUNT(*) FROM removals WHERE id = ? " +
                "AND drained_at IS NULL AND claimed_at IS NULL AND attempts = 0",
            stagingId,
        ) {
            // Not necessarily gone — but not this group's to register.
            // Re-stage so the object cannot outlive a ticket naming it.
            reStage(catalog, outputPath)
            guard(Outcome.SkippedConflict)
        }
        // GUARD 2: the table still exists (a drop mid-rewrite).
        tx.expectOne(
            "SELECT COUNT(*) FROM tables WHERE id = ? AND dropped_snapshot IS NULL",
            ctx.tableId,
        ) { guard(Outcome.SkippedConflict) }
        // GUARD 3: the live sort spec is still the one planned against —
        // a spec that moved mid-rewrite would publish a mis-sorted file
        // as a trusted run of the new spec.
        if (ctx.sortId == null) {
            // No spec at planning: guard that none ARRIVED.
            tx.expectZero(
                "SELECT COUNT(*) FROM sort_specs WHERE table_id = ? AND end_snapshot IS NULL",
                ctx.tableId,
            ) { guard(Outcome.SkippedConflict) }
        } else {
            // A spec at planning: guard the live spec is still that one.
            tx.expectOne(
                "SELECT COUNT(*) FROM sort_specs WHERE table_id = ? AND end_snapshot IS NULL " +
                    "AND spec_id = ?",
                ctx.tableId,
                ctx.sortId,
            ) { guard(Outcome.SkippedConflict) }
        }
        // GUARD 4: every input still live.
        tx.expectScalar(
            ids.size.toLong(),
            "SELECT COUNT(*) FROM files WHERE id IN (${ids.joinToString(",") { "?" }}) " +
                "AND end_snapshot IS NULL",
            *ids.toTypedArray(),
        ) { guard(Outcome.SkippedConflict) }
        // GUARD 5: every input still carries exactly its planned DV (a
        // superseded one deletes rows the rewrite did not apply).
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
            // The empty-rewrite retirement: a snapshot, a change row, the
            // inputs and their vectors ended — and NO output registered
            // (commitGroup's retireDeadGroup; the ticket stays undrained,
            // the cleanup drain reclaims the empty object).
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

        // The live commit: allocate snapshot + file id (commit allocation,
        // schema_version untouched — compaction is not DDL).
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
        // table_totals: gross append counters, exactly like a commit's
        // (the ended inputs' counts stay — the DVs already masked them).
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
        // Settle the staging ticket in the same transaction that makes the
        // path live — the predicate is GUARD 1's, verbatim.
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

    /** Re-stage an output whose ticket was touched — commitGroup's comment explains the leak it closes. */
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

    /** End the inputs and their applied DVs at the new snapshot, with the row-count checks. */
    private fun endSnapshotInputs(
        tx: HogliteTx,
        ctx: TableContext,
        ids: List<Long>,
        group: CompactionGroup,
    ) {
        tx.add(
            "UPDATE files SET end_snapshot = ${HogliteSchema.HEAD_SQL} " +
                "WHERE table_id = ? AND id IN (${ids.joinToString(",") { "?" }})",
            ctx.tableId,
            *ids.toTypedArray(),
        )
        val dvIds = group.files.mapNotNull { it.dv?.deleteFileId }
        if (dvIds.isNotEmpty()) {
            tx.add(
                "UPDATE dvs SET end_snapshot = ${HogliteSchema.HEAD_SQL} " +
                    "WHERE delete_file_id IN (${dvIds.joinToString(",") { "?" }})",
                *dvIds.toTypedArray(),
            )
        }
    }
}
