package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.hydrator.CatalogColumn
import com.posthog.hoglake.hydrator.FooterReader
import com.posthog.hoglake.hydrator.FooterSplitOffsets
import com.posthog.hoglake.hydrator.FooterStats
import com.posthog.hoglake.hydrator.Hydrator
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.hydrator.TransientFetchException
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.HydratorSweepResult
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.ObjectPrefix
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.RehydrateResult
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger

/**
 * The hydrator on hoglite: [Hydrator]'s semantics over the tenant
 * schema. Claims the catalog's pending files (live, undropped — the
 * CLAIM_PENDING_SQL predicates), reads each footer through the shared
 * [FooterReader] (the tail-range/whole-object logic is NOT a copy), and
 * resolves the row: `provided` with its stats in the row's `stats_json`
 * (the tenant schema's one-JSON-array-per-file shape rather than
 * hog_file_column_stats rows), `provided` with none under the size
 * floor, or `failed`. Transient object-store failures leave the file
 * pending; one file's failure never poisons the sweep (one transaction
 * per file, where Postgres uses savepoints).
 *
 * The ledger fan-out is Postgres's: one run row per catalog the sweep
 * claimed files for, after the work commits; a no-claim sweep records
 * nothing.
 *
 * THE OBJECT-PREFIX CHECK is [Hydrator]'s (PR 2.1c), with the same
 * shared rule ([Hydrator.prefixMismatch]): on a table with the layout
 * (`table_layouts`), after the aggregate and BEFORE the size floor, the
 * prefix field's footer aggregate must prove every row holds the value
 * the file was registered with at its OWN spec's identity key; a file
 * it cannot prove flips to `provided` with its truthful stats and
 * `prefix_quarantined = 1`, which compaction's candidate read excludes.
 * One layout read per claimed catalog per sweep, one spec read per
 * (table, spec).
 */
internal class HogliteHydrator(
    private val store: HogliteCatalogStore,
    objectStore: ObjectStore,
    maxWholeObjectBytes: Long = Hydrator.DEFAULT_MAX_WHOLE_OBJECT_BYTES,
    private val maxSplitOffsetRowGroups: Int = SplitOffsetsCap,
    private val minFileBytes: Long = 0,
) : com.posthog.hoglake.service.HydratorMaintenanceBackend {
    private val log = KotlinLogging.logger {}
    private val footerReader = FooterReader(objectStore, maxWholeObjectBytes)

    /** Where the next sweep's catalog order starts; seeded at random so replicas differ. */
    private val rotation = AtomicInteger(ThreadLocalRandom.current().nextInt())

    fun runOnce(limit: Int = Hydrator.DEFAULT_SWEEP_LIMIT): Int {
        if (limit <= 0) return 0
        val names = store.listCatalogNames()
        if (names.isEmpty()) return 0
        val start = Math.floorMod(rotation.get(), names.size)
        val order = names.drop(start) + names.take(start)
        rotation.incrementAndGet()
        var served = 0
        val share = maxOf(1, limit / order.size)
        for (catalog in order) {
            val room = minOf(share, limit - served)
            if (room <= 0) break
            served += runCatalog(catalog, room)[0].toInt()
        }
        return served
    }

    private fun runCatalog(
        catalog: String,
        room: Int,
    ): LongArray =
        store.catalogOperation(catalog) {
            val tally = sweepCatalog(catalog, room)
            if (tally[0] > 0) {
                // The ledger shares the claim/hydration operation's tenant binding.
                store.recorded(catalog, MaintenanceTask.HYDRATOR, MaintenanceTrigger.LOOP) {
                    HydratorSweepResult(
                        claimed = tally[0],
                        hydrated = tally[1],
                        failed = tally[2],
                        transient = tally[3],
                        statsSkippedBelowFloor = tally[4],
                        quarantined = tally[5],
                    )
                }
            }
            tally
        }

    /** [claimed, hydrated, failed, transient, stats skipped below floor, quarantined] */
    private fun sweepCatalog(
        catalog: String,
        limit: Int,
    ): LongArray {
        val tally = longArrayOf(0, 0, 0, 0, 0, 0)
        val pending =
            store.queryCatalog(
                catalog,
                "SELECT f.id, f.table_id, f.path, f.record_count, f.file_size_bytes, f.footer_size, " +
                    "f.begin_snapshot, f.spec_id, f.partition_values_json " +
                    "FROM files f JOIN tables t ON t.id = f.table_id " +
                    "WHERE f.stats_state = 'pending' AND f.end_snapshot IS NULL " +
                    "AND t.dropped_snapshot IS NULL ORDER BY f.id LIMIT ?",
                limit,
            ).rows
        // Hydrator.prefixFields: field ids only, the column is resolved per
        // file against the columns its stats bind to.
        val prefixFields = if (pending.isEmpty()) emptyMap() else store.liveLayouts(catalog)
        val keyIndexes = HashMap<Pair<Long, Long>, Int?>()
        for (row in pending) {
            val file =
                Hydrator.PendingFile(
                    // the tenant IS the catalog; nothing reads catalogId
                    catalogId = 0,
                    dataFileId = row[0].asLong(),
                    tableId = row[1].asLong(),
                    path = row[2].asString(),
                    recordCount = row[3].asLong(),
                    fileSizeBytes = row[4].asLong(),
                    footerSize = row[5].asLongOrNull(),
                    beginSnapshot = row[6].asLong(),
                    specId = row[7].asLongOrNull(),
                )
            tally[0]++
            try {
                val prefix =
                    prefixFields[file.tableId]?.let { fieldId ->
                        Hydrator.PrefixClaim(
                            fieldId,
                            prefixValue(catalog, file, row[8] as? String, fieldId, keyIndexes),
                        )
                    }
                val outcome = hydrate(catalog, file, prefix)
                if (outcome == Outcome.FAILED) {
                    tally[2]++
                } else {
                    tally[1]++
                    if (outcome.withoutStats) tally[4]++
                    if (outcome.quarantined) tally[5]++
                }
            } catch (e: TransientFetchException) {
                Metrics.hydratorTransientError()
                tally[3]++
                log.warn(e) {
                    "transient object-store failure hydrating file ${file.dataFileId} " +
                        "(${file.path}); leaving pending for the next sweep"
                }
            } catch (e: HoglakeException.BackendUnavailable) {
                // hoglite did not answer, or a write's outcome is unknown
                // (CommitOutcomeUnknown): nothing about the FILE failed,
                // and the hydrate flip may even have landed. Both state
                // flips are guarded on stats_state='pending', so leaving
                // the file for the next sweep is safe either way; marking
                // it failed would make a backend blip permanent.
                tally[3]++
                log.warn(e) {
                    "hoglite unavailable hydrating file ${file.dataFileId} " +
                        "(${file.path}); leaving pending for the next sweep"
                }
            } catch (e: Exception) {
                log.error(e) {
                    "hydration failed for file ${file.dataFileId} (${file.path}); marking failed"
                }
                markFailed(catalog, file)
                tally[2]++
            }
        }
        return tally
    }

    /** How one claimed file resolved (Hydrator's Outcome). Every outcome but [FAILED] is `provided`. */
    private enum class Outcome(val withoutStats: Boolean = false, val quarantined: Boolean = false) {
        HYDRATED,
        HYDRATED_WITHOUT_STATS(withoutStats = true),
        QUARANTINED(quarantined = true),
        QUARANTINED_WITHOUT_STATS(withoutStats = true, quarantined = true),
        FAILED,
    }

    /**
     * The raw value [file] was registered with for the prefix field
     * [fieldId] (Hydrator.prefixValue): its own spec's identity key_index,
     * cached per sweep by (table, spec), then that slot of its stored
     * partition values. Null when the file has no spec, its spec has no
     * single identity field on [fieldId], or the value is absent or NULL —
     * states the commit refuses, which the check quarantines.
     */
    private fun prefixValue(
        catalog: String,
        file: Hydrator.PendingFile,
        partitionValuesJson: String?,
        fieldId: Long,
        keyIndexes: MutableMap<Pair<Long, Long>, Int?>,
    ): String? {
        val specId = file.specId ?: return null
        val key = file.tableId to specId
        val keyIndex =
            if (key in keyIndexes) {
                keyIndexes[key]
            } else {
                val fields =
                    store.queryCatalog(
                        catalog,
                        "SELECT fields_json FROM partition_specs WHERE table_id = ? AND spec_id = ?",
                        file.tableId,
                        specId,
                    ).firstRow()?.let { store.json.readValue<List<PartitionFieldDef>>(it[0].asString()) }
                fields?.let { ObjectPrefix.keyIndex(it, fieldId) }.also { keyIndexes[key] = it }
            } ?: return null
        val values = partitionValuesJson?.let { store.json.readValue<List<String?>>(it) } ?: return null
        return values.getOrNull(keyIndex)
    }

    /** [Hydrator.hydrate]'s flow, writing the tenant row instead of stats rows. */
    private fun hydrate(
        catalog: String,
        file: Hydrator.PendingFile,
        prefix: Hydrator.PrefixClaim?,
    ): Outcome {
        val footer = footerReader.read(file)
        val missingFieldIds = FooterStats.missingFieldIds(footer.fileMetaData.schema)
        val footerRows = footer.blocks.sumOf { it.rowCount }
        if (footerRows != file.recordCount) {
            log.error {
                "REGISTRATION MISMATCH for file ${file.dataFileId} (${file.path}): " +
                    "parquet footer has $footerRows rows but record_count is ${file.recordCount}; " +
                    "marking failed, writing no stats"
            }
            markFailed(catalog, file, missingFieldIds)
            return Outcome.FAILED
        }
        // Id-bearing files bind by field id against the LIVE columns;
        // id-less files by name against the schema the file was
        // committed under (Hydrator.hydrate's binding rule).
        val columns =
            if (FooterStats.usesFieldIds(footer.fileMetaData.schema)) {
                catalogColumns(catalog, file.tableId, at = null)
            } else {
                catalogColumns(catalog, file.tableId, at = file.beginSnapshot)
            }
        val aggs = FooterStats.aggregate(footer, columns, file.path)
        // THE OBJECT-PREFIX PROOF, ahead of the floor: a file under it
        // skips its stats, never the check of its tenant boundary.
        val mismatch = prefix?.let { Hydrator.prefixMismatch(it, columns, aggs) }
        val quarantined = mismatch != null
        val belowFloor = file.fileSizeBytes < minFileBytes
        val splitOffsets = FooterSplitOffsets.of(footer, file.fileSizeBytes, maxSplitOffsetRowGroups)
        val statsJson =
            if (belowFloor) {
                null
            } else {
                store.json.writeValueAsString(
                    aggs.map { agg ->
                        ColumnStats(
                            fieldId = agg.fieldId,
                            valueCount = agg.valueCount,
                            nullCount = agg.nullCount,
                            nanCount = agg.nanCount,
                            sizeBytes = agg.sizeBytes,
                            lowerBound = agg.lowerBound,
                            upperBound = agg.upperBound,
                        )
                    },
                )
            }
        // The flip is guarded on still-'pending': an ended/concurrently-
        // resolved row is not overwritten (Hydrator's `flipped == 0`
        // case, here simply not counted as hydrated).
        val tx = HogliteTx()
        val flip =
            tx.add(
                "UPDATE files SET stats_state = 'provided', missing_field_ids = ?, " +
                    "stats_json = COALESCE(?, stats_json), split_offsets_json = ?, prefix_quarantined = ? " +
                    "WHERE id = ? AND stats_state = 'pending'",
                if (missingFieldIds) 1L else 0L,
                statsJson,
                splitOffsets?.let { offs -> store.json.writeValueAsString(offs.toList()) },
                if (quarantined) 1L else 0L,
                file.dataFileId,
            )
        val flipped = store.runTx(catalog, HogliteCatalogStore.freshTxid("hydrate"), null, tx)[flip].affected
        if (flipped == 0L) {
            log.warn { "file ${file.dataFileId} left 'pending' concurrently; stats upserted anyway" }
        }
        Metrics.statsHydrated(if (quarantined) "quarantined" else "provided")
        if (mismatch != null) {
            log.warn {
                "file ${file.dataFileId} (${file.path}) quarantined: its footer does not prove every row " +
                    "holds its object-prefix value ($mismatch)"
            }
        }
        return when {
            quarantined && belowFloor -> Outcome.QUARANTINED_WITHOUT_STATS
            quarantined -> Outcome.QUARANTINED
            belowFloor -> Outcome.HYDRATED_WITHOUT_STATS
            else -> Outcome.HYDRATED
        }
    }

    private fun markFailed(
        catalog: String,
        file: Hydrator.PendingFile,
        missingFieldIds: Boolean? = null,
    ) {
        Metrics.statsHydrated("failed")
        val tx = HogliteTx()
        tx.add(
            "UPDATE files SET stats_state = 'failed'" +
                (if (missingFieldIds != null) ", missing_field_ids = ?" else "") +
                " WHERE id = ? AND stats_state = 'pending'",
            *(
                if (missingFieldIds != null) {
                    arrayOf(if (missingFieldIds) 1L else 0L, file.dataFileId)
                } else {
                    arrayOf(file.dataFileId)
                }
            ),
        )
        store.runTx(catalog, HogliteCatalogStore.freshTxid("hydrate-failed"), null, tx)
    }

    /** [Hydrator.catalogColumns]'s tree assembly over the tenant's columns table. */
    private fun catalogColumns(
        catalog: String,
        tableId: Long,
        at: Long?,
    ): List<CatalogColumn> {
        val rows =
            if (at == null) {
                store.queryCatalog(
                    catalog,
                    "SELECT field_id, parent_field_id, ordinal, name, type, type_params_json " +
                        "FROM columns WHERE table_id = ? AND end_snapshot IS NULL",
                    tableId,
                ).rows
            } else {
                store.queryCatalog(
                    catalog,
                    "SELECT field_id, parent_field_id, ordinal, name, type, type_params_json " +
                        "FROM columns WHERE table_id = ? AND begin_snapshot <= ? " +
                        "AND (end_snapshot IS NULL OR end_snapshot > ?)",
                    tableId,
                    at,
                    at,
                ).rows
            }

        data class Row(val parentFieldId: Long?, val ordinal: Int, val col: CatalogColumn)
        val mapped =
            rows.map { r ->
                Row(
                    parentFieldId = r[1].asLongOrNull(),
                    ordinal = r[2].asLong().toInt(),
                    col =
                        CatalogColumn(
                            fieldId = r[0].asLong(),
                            name = r[3].asString(),
                            type = ColType.fromWire(r[4].asString()),
                            decimalScale =
                                (r[5] as? String)?.let { params ->
                                    store.json.readTree(params).get("scale")?.takeIf { it.isInt }?.asInt()
                                },
                        ),
                )
            }
        val byParent = mapped.groupBy { it.parentFieldId }

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

    /**
     * Operator requeue (POST .../maintenance/rehydrate): flip the
     * catalog's 'failed' files back to 'pending', optionally scoped to
     * one table — [Hydrator.rehydrateFailed]'s semantics, and like it
     * never requeueing an ENDED row (the sweep would never take it).
     */
    override fun rehydrateFailed(
        catalog: String,
        namespace: String?,
        table: String?,
    ): RehydrateResult =
        store.recorded(catalog, MaintenanceTask.HYDRATOR, MaintenanceTrigger.MANUAL) {
            if ((namespace == null) != (table == null)) {
                throw HoglakeException.Validation(
                    "namespace and table must be supplied together (or neither, for the whole catalog)",
                )
            }
            store.meta(catalog) // 404 on an unknown catalog, like CatalogRepo.require
            val tableId =
                if (namespace != null && table != null) {
                    store.liveTable(catalog, namespace, table).tableId
                } else {
                    null
                }
            val tx = HogliteTx()
            val idx =
                if (tableId != null) {
                    tx.add(
                        "UPDATE files SET stats_state = 'pending' " +
                            "WHERE stats_state = 'failed' AND end_snapshot IS NULL AND table_id = ?",
                        tableId,
                    )
                } else {
                    tx.add(
                        "UPDATE files SET stats_state = 'pending' " +
                            "WHERE stats_state = 'failed' AND end_snapshot IS NULL",
                    )
                }
            val requeued =
                store.runTx(catalog, HogliteCatalogStore.freshTxid("rehydrate"), null, tx)[idx].affected
            Audit.audited("rehydrate", catalog, table?.let { "$namespace.$it" }, detail = { "requeued=$requeued" }) {
                RehydrateResult(requeued.toLong())
            }
        }

    private companion object {
        /** SplitOffsets.MAX_ROW_GROUPS — the split-offsets cap Hydrator passes. */
        val SplitOffsetsCap: Int = com.posthog.hoglake.model.SplitOffsets.MAX_ROW_GROUPS
    }
}
