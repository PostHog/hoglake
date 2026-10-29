package com.posthog.hoglake.hoglite

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
            val tally = sweepCatalog(catalog, room)
            served += tally[0].toInt()
            if (tally[0] > 0) {
                // The ledger row, after the work committed.
                store.recorded(catalog, MaintenanceTask.HYDRATOR, MaintenanceTrigger.LOOP) {
                    HydratorSweepResult(
                        claimed = tally[0],
                        hydrated = tally[1],
                        failed = tally[2],
                        transient = tally[3],
                        statsSkippedBelowFloor = tally[4],
                    )
                }
            }
        }
        return served
    }

    /** [claimed, hydrated, failed, transient, stats skipped below floor] */
    private fun sweepCatalog(
        catalog: String,
        limit: Int,
    ): LongArray {
        val tally = longArrayOf(0, 0, 0, 0, 0)
        val pending =
            store.queryCatalog(
                catalog,
                "SELECT f.id, f.table_id, f.path, f.record_count, f.file_size_bytes, f.footer_size, " +
                    "f.begin_snapshot FROM files f JOIN tables t ON t.id = f.table_id " +
                    "WHERE f.stats_state = 'pending' AND f.end_snapshot IS NULL " +
                    "AND t.dropped_snapshot IS NULL ORDER BY f.id LIMIT ?",
                limit,
            ).rows
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
                )
            tally[0]++
            try {
                when (hydrate(catalog, file)) {
                    Outcome.HYDRATED -> tally[1]++
                    Outcome.HYDRATED_WITHOUT_STATS -> {
                        tally[1]++
                        tally[4]++
                    }
                    Outcome.FAILED -> tally[2]++
                }
            } catch (e: TransientFetchException) {
                Metrics.hydratorTransientError()
                tally[3]++
                log.warn(e) {
                    "transient object-store failure hydrating file ${file.dataFileId} " +
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

    private enum class Outcome { HYDRATED, HYDRATED_WITHOUT_STATS, FAILED }

    /** [Hydrator.hydrate]'s flow, writing the tenant row instead of stats rows. */
    private fun hydrate(
        catalog: String,
        file: Hydrator.PendingFile,
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
                    "stats_json = COALESCE(?, stats_json), split_offsets_json = ? " +
                    "WHERE id = ? AND stats_state = 'pending'",
                if (missingFieldIds) 1L else 0L,
                statsJson,
                splitOffsets?.let { offs -> store.json.writeValueAsString(offs.toList()) },
                file.dataFileId,
            )
        val flipped = store.runTx(catalog, HogliteCatalogStore.freshTxid("hydrate"), null, tx)[flip].affected
        if (flipped == 0L) {
            log.warn { "file ${file.dataFileId} left 'pending' concurrently; stats upserted anyway" }
        }
        Metrics.statsHydrated("provided")
        return if (belowFloor) Outcome.HYDRATED_WITHOUT_STATS else Outcome.HYDRATED
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
