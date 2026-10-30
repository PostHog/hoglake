package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.initialColumns
import com.posthog.hoglake.model.validateFooterSize
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.Identifiers
import com.posthog.hoglake.service.TableCreation
import com.posthog.hoglake.service.TableCreationBackend
import com.posthog.hoglake.service.TableCreationDefinition
import com.posthog.hoglake.service.TableCreationDefinitionCodec
import com.posthog.hoglake.service.TableMetadata
import com.posthog.hoglake.service.requireOperationMetadata
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

/**
 * [TableCreationBackend] on hoglite: the prepare → publish|abort
 * protocol of [com.posthog.hoglake.service.TableCreationService] over
 * the tenant's `table_creations` table.
 *
 * Semantics ported from the Postgres path:
 *  - prepare is idempotent on the operation id: a replay with the same
 *    definition returns the receipt, a reuse with a different one is a
 *    409 ("operation id reused with a different definition").
 *  - publish's replay/terminal handling: `committed`/`rejected` compare
 *    the file list and return the receipt; `aborted` answers as-is.
 *  - publish's rejection reasons — `namespace_changed`,
 *    `target_changed` (any table_* change except `table_compacted`
 *    since the guarded read snapshot, or a uuid/expiry-floor miss),
 *    `target_exists`, `definition_invalid` — land as DURABLE receipt
 *    states, never as rolled-back 4xx that would strand the operation
 *    `prepared` forever.
 *  - a successful publish is ONE hoglite transaction: allocate the DDL
 *    snapshot, retire the replaced incarnation (drop mark + the
 *    consumer-fencing change pair), insert the new incarnation
 *    (identity, version, columns, specs, totals), register the initial
 *    files AT THAT SAME snapshot with server-assigned row lineage,
 *    settle the operation's upload claims (owner = operation id), and
 *    transition the receipt to `committed`.
 *  - lazy expiry: a `prepared` receipt past its 24h `expires_at` is
 *    aborted with reason `expired` on the next touch; no sweep, and no
 *    object deletion (same as Postgres).
 */
internal class HogliteTableCreations(
    private val store: HogliteCatalogStore,
) : TableCreationBackend {
    private val mapper = jacksonObjectMapper()

    override fun prepare(
        catalog: String,
        operationId: UUID,
        definition: TableCreationDefinition,
    ): TableCreation =
        store.catalogOperation(catalog) {
            val meta = store.meta(catalog)
            val encoded = TableCreationDefinitionCodec.encode(definition)
            // Idempotent replay: the stored definition decides.
            loadOrNull(catalog, operationId)?.let { existing ->
                requireSameDefinition(existing, definition, operationId)
                return existing
            }
            // hoglake refuses a malformed definition at PREPARE, so the
            // receipt never records something publish must reject.
            TableMetadata.validateComment(definition.comment)
            TableMetadata.validateProperties(definition.properties)
            Identifiers.validate("table", definition.name)
            if (definition.columns.isEmpty()) {
                throw HoglakeException.Validation("table '${definition.name}' must have at least one column")
            }
            com.posthog.hoglake.service.ColumnTrees.validate(definition.columns)
            val cols = initialColumns(definition.columns)
            AlterService.validatePartitionFields(cols, definition.partitionFields)
            AlterService.validateSortFields(cols, definition.sortFields)
            Identifiers.validate("namespace", definition.namespace)
            val ns = store.liveNamespace(catalog, definition.namespace)
            definition.replacement?.let {
                if (it.expectedTableUuid != null) requireOperationMetadata(definition.author, definition.message)
                if (it.readSnapshot < meta.earliestSnapshotId || it.readSnapshot > meta.head) {
                    throw HoglakeException.CommitConflict("replacement read snapshot is not retained")
                }
                val target =
                    tableAtOrNull(catalog, ns.namespaceId, definition.namespace, definition.name, it.readSnapshot)
                if (target?.tableUuid != it.expectedTableUuid) {
                    throw HoglakeException.CommitConflict("replacement target does not match read snapshot")
                }
                // TableCreationService.prepare: no receipt is ever prepared
                // for a replacement no definition could publish. Live at head
                // is sound because the option has no unset; a set after the
                // read snapshot is `target_changed` at publish, and publish's
                // definition check below is the backstop.
                if (target != null && store.liveLayouts(catalog).containsKey(target.tableId)) {
                    throw HoglakeException.Validation(
                        "cannot replace table '${definition.namespace}.${definition.name}': it has the " +
                            "object-prefix layout (object_prefix_field), which a replacement would not carry " +
                            "over; drop the table and create it instead",
                    )
                }
            }
            // The receipt's write path carries a server-generated identity:
            // even a future catalog recreation and reused operation id
            // cannot reuse old object paths.
            val tableUuid = UUID.randomUUID()
            val writePath = meta.dataPath.trimEnd('/') + "/data/$tableUuid/"
            val now = Instant.now()
            val tx = HogliteTx()
            // A racing prepare of the same operation id makes this guard
            // fail; the classify below re-reads and answers as a replay or
            // the reuse conflict, mirroring ON CONFLICT DO NOTHING.
            tx.expectZero(
                "SELECT COUNT(*) FROM table_creations WHERE operation = ?",
                operationId.toString(),
            ) {
                val stored = store.loadCreationOrThrow(catalog, operationId)
                if (stored.definition == definition) {
                    throw PrepareRaceWon(catalog, operationId, stored)
                }
                HoglakeException.CommitConflict("operation id reused with a different definition")
            }
            tx.add(
                "INSERT INTO table_creations(operation, namespace_id, definition_json, table_uuid, write_path, " +
                    "state, expires_at, created_at) VALUES (?, ?, ?, ?, ?, 'prepared', ?, ?)",
                operationId.toString(),
                ns.namespaceId,
                encoded,
                tableUuid.toString(),
                writePath,
                HogliteSchema.fmt(now.plus(24, ChronoUnit.HOURS)),
                HogliteSchema.fmt(now),
            )
            try {
                store.runTx(catalog, HogliteCatalogStore.freshTxid("prepare"), null, tx)
            } catch (won: PrepareRaceWon) {
                return won.receipt
            }
            return loadOrNull(catalog, operationId)!!
        }

    override fun status(
        catalog: String,
        operationId: UUID,
    ): TableCreation =
        store.catalogOperation(catalog) {
            store.loadCreationOrThrow(catalog, operationId)
        }

    override fun abort(
        catalog: String,
        operationId: UUID,
    ): TableCreation =
        store.catalogOperation(catalog) {
            val operation = store.loadCreationOrThrow(catalog, operationId)
            if (operation.state != "prepared") return operation
            val tx = HogliteTx()
            tx.add(
                "UPDATE table_creations SET state = 'aborted', reason = 'client_abort' " +
                    "WHERE operation = ? AND state = 'prepared'",
                operationId.toString(),
            )
            store.runTx(catalog, HogliteCatalogStore.freshTxid("abort"), null, tx)
            return store.loadCreationOrThrow(catalog, operationId)
        }

    override fun publish(
        catalog: String,
        operationId: UUID,
        files: List<FileRegistration>,
    ): TableCreation =
        store.catalogOperation(catalog) {
            val meta = store.meta(catalog)
            val operation = store.loadCreationOrThrow(catalog, operationId)
            val encodedFiles = mapper.writeValueAsString(files)
            // Terminal receipts are the answer to every retry.
            if (operation.state == "committed" || operation.state == "rejected") {
                requireSameFiles(catalog, operationId, encodedFiles)
                return operation
            }
            if (operation.state == "aborted") return operation
            if (files.size > 10000) throw HoglakeException.Validation("too many files")
            if (files.map { it.path }.toSet().size != files.size) {
                throw HoglakeException.Validation("duplicate file paths")
            }
            files.forEach {
                if (!it.path.startsWith(operation.writePath)) {
                    throw HoglakeException.Validation("file outside operation write_path")
                }
                it.validateFooterSize(required = true)
            }
            val definition = operation.definition
            val ns =
                store.queryCatalog(
                    catalog,
                    "SELECT id FROM namespaces WHERE name = ? AND dropped_snapshot IS NULL",
                    definition.namespace,
                ).firstRow()?.get(0)?.asLong()
            val originalNamespace =
                store.queryCatalog(
                    catalog,
                    "SELECT namespace_id FROM table_creations WHERE operation = ?",
                    operationId.toString(),
                ).firstRow()!![0].asLong()
            val target =
                ns?.let {
                    store.queryCatalog(
                        catalog,
                        "SELECT t.id, t.table_uuid FROM tables t JOIN table_versions v ON v.table_id = t.id " +
                            "WHERE v.namespace_id = ? AND v.name = ? AND v.end_snapshot IS NULL " +
                            "AND t.dropped_snapshot IS NULL",
                        it,
                        definition.name,
                    ).firstRow()
                }
            val replacement = definition.replacement
            val targetModified =
                replacement != null && target != null &&
                    store.queryCatalog(
                        catalog,
                        "SELECT COUNT(*) FROM changes WHERE object_id = ? AND snapshot_id > ? " +
                            "AND kind LIKE 'table_%' AND kind <> 'table_compacted'",
                        target[0].asLong(),
                        replacement.readSnapshot,
                    ).firstRow()!![0].asLong() > 0
            val targetChanged =
                replacement != null && (
                    replacement.readSnapshot < meta.earliestSnapshotId ||
                        target?.get(1)?.asString() != replacement.expectedTableUuid?.toString() ||
                        targetModified
                )
            val rejectReason =
                when {
                    ns == null || ns != originalNamespace -> "namespace_changed"
                    targetChanged -> "target_changed"
                    replacement == null && target != null -> "target_exists"
                    else -> null
                }
            if (rejectReason != null) {
                // The attempted files are recorded WITH the rejection, so a
                // replay of the same publish compares equal (Postgres writes
                // the payload before the rejection branches).
                val tx = HogliteTx()
                tx.affectOne(
                    "UPDATE table_creations SET files_json = ? WHERE operation = ? AND state = 'prepared'",
                    encodedFiles,
                    operationId.toString(),
                ) {
                    throw PublishRace(store.loadCreationOrThrow(catalog, operationId))
                }
                tx.add(
                    "UPDATE table_creations SET state = 'rejected', reason = ? WHERE operation = ?",
                    rejectReason,
                    operationId.toString(),
                )
                try {
                    store.runTx(catalog, HogliteCatalogStore.freshTxid("publish-reject"), null, tx)
                } catch (race: PublishRace) {
                    // The winner's receipt answers only a replay of the SAME
                    // payload; a different payload raced is the typed reuse.
                    if (race.receipt.state != "aborted") requireSameFiles(catalog, operationId, encodedFiles)
                    return race.receipt
                }
                return store.loadCreationOrThrow(catalog, operationId)
            }

            // The publish transaction. Every preflight fact that could have
            // changed is re-asserted as a guard inside it.
            val cols = initialColumns(definition.columns)
            val maxFieldId = cols.flatMap { it.selfAndDescendants() }.maxOf { it.fieldId }
            val newIdSql = "(SELECT next_table_id - 1 FROM meta WHERE id = 1)"
            val tx = HogliteTx()
            tx.affectOne(
                "UPDATE table_creations SET files_json = ? WHERE operation = ? AND state = 'prepared'",
                encodedFiles,
                operationId.toString(),
            ) {
                // A concurrent abort/expiry won; answer with the receipt it left.
                throw PublishRace(store.loadCreationOrThrow(catalog, operationId))
            }
            tx.expectOne(
                "SELECT COUNT(*) FROM namespaces WHERE id = ? AND dropped_snapshot IS NULL",
                originalNamespace,
            ) { nsChangedRace }
            if (replacement != null) {
                // The guarded target must still be the live incarnation, and
                // nothing but compaction may have touched it since the read.
                tx.expectOne(
                    "SELECT COUNT(*) FROM tables t JOIN table_versions v ON v.table_id = t.id " +
                        "WHERE v.namespace_id = ? AND v.name = ? AND v.end_snapshot IS NULL " +
                        "AND t.dropped_snapshot IS NULL AND t.table_uuid = ?",
                    originalNamespace,
                    definition.name,
                    replacement.expectedTableUuid?.toString() ?: "",
                ) { targetChangedRace }
                tx.expectZero(
                    "SELECT COUNT(*) FROM changes WHERE object_id = ? AND snapshot_id > ? " +
                        "AND kind LIKE 'table_%' AND kind <> 'table_compacted'",
                    target!![0].asLong(),
                    replacement.readSnapshot,
                ) { targetChangedRace }
            } else {
                tx.expectZero(
                    "SELECT COUNT(*) FROM tables t JOIN table_versions v ON v.table_id = t.id " +
                        "WHERE v.namespace_id = ? AND v.name = ? AND v.end_snapshot IS NULL " +
                        "AND t.dropped_snapshot IS NULL",
                    originalNamespace,
                    definition.name,
                ) { targetExistsRace }
            }
            // A definition the server NOW refuses (prepared under a laxer
            // binary) must land as a durable rejection, not a 422 that
            // strands the receipt prepared forever.
            try {
                TableMetadata.validateComment(definition.comment)
                TableMetadata.validateProperties(definition.properties)
                Identifiers.validate("table", definition.name)
                com.posthog.hoglake.service.ColumnTrees.validate(definition.columns)
                AlterService.validatePartitionFields(cols, definition.partitionFields)
                AlterService.validateSortFields(cols, definition.sortFields)
                // CatalogService.createTable's backstop: replacing a table
                // with the object-prefix layout would mint an incarnation
                // without it under the same path scope. Reaching here means
                // the receipt was prepared without prepare's check (an older
                // binary); rejected, never left prepared. The layout cannot
                // be set between this read and the transaction without a
                // `table_altered` change, which the guard above refuses.
                val replacingLayoutTable =
                    replacement != null && target != null && store.liveLayouts(catalog).containsKey(target[0].asLong())
                if (replacingLayoutTable) {
                    throw HoglakeException.Validation(
                        "cannot replace table '${definition.namespace}.${definition.name}': it has the " +
                            "object-prefix layout, which a replacement would not carry over",
                    )
                }
            } catch (e: HoglakeException.Validation) {
                val tx = HogliteTx()
                tx.add(
                    "UPDATE table_creations SET files_json = ? WHERE operation = ?",
                    encodedFiles,
                    operationId.toString(),
                )
                tx.add(
                    "UPDATE table_creations SET state = 'rejected', reason = 'definition_invalid' WHERE operation = ?",
                    operationId.toString(),
                )
                store.runTx(catalog, HogliteCatalogStore.freshTxid("publish-reject"), null, tx)
                return store.loadCreationOrThrow(catalog, operationId)
            }

            tx.allocateDdlSnapshot()
            if (replacement != null) {
                val oldId = target!![0].asLong()
                tx.add(
                    "INSERT INTO changes(snapshot_id, kind, object_id) " +
                        "VALUES (${HogliteSchema.HEAD_SQL}, 'table_dropped', ?)",
                    oldId,
                )
                tx.affectOne(
                    "UPDATE tables SET dropped_snapshot = ${HogliteSchema.HEAD_SQL} WHERE id = ?",
                    oldId,
                ) { targetChangedRace }
                tx.affectOne(
                    "UPDATE table_versions SET end_snapshot = ${HogliteSchema.HEAD_SQL} " +
                        "WHERE table_id = ? AND end_snapshot IS NULL",
                    oldId,
                ) { targetChangedRace }
            }
            tx.affectOne(
                "UPDATE meta SET next_table_id = next_table_id + 1 WHERE id = 1",
            ) { HoglakeException.NotFound("catalog '$catalog'") }
            tx.add(
                "INSERT INTO tables(id, namespace_id, namespace, name, table_uuid, replaced_table_id, " +
                    "next_field_id, created_snapshot) " +
                    "VALUES ($newIdSql, ?, ?, ?, ?, ?, ?, ${HogliteSchema.HEAD_SQL})",
                originalNamespace,
                definition.namespace,
                definition.name,
                operation.tableUuid.toString(),
                target?.get(0)?.asLong(),
                maxFieldId + 1,
            )
            tx.add(
                "INSERT INTO changes(snapshot_id, kind, object_id) " +
                    "VALUES (${HogliteSchema.HEAD_SQL}, 'table_created', $newIdSql)",
            )
            if (replacement != null) {
                // The new incarnation's feed fences consumers whose window
                // starts before publication (same pair as truncate).
                tx.add(
                    "INSERT INTO changes(snapshot_id, kind, object_id) " +
                        "VALUES (${HogliteSchema.HEAD_SQL}, 'table_altered', $newIdSql)",
                )
                tx.add(
                    "INSERT INTO changes(snapshot_id, kind, object_id) " +
                        "VALUES (${HogliteSchema.HEAD_SQL}, 'table_deleted_from', $newIdSql)",
                )
            }
            tx.add(
                "INSERT INTO table_versions(table_id, namespace_id, namespace, name, comment, properties_json, " +
                    "begin_snapshot) VALUES ($newIdSql, ?, ?, ?, ?, ?, ${HogliteSchema.HEAD_SQL})",
                originalNamespace,
                definition.namespace,
                definition.name,
                definition.comment,
                mapper.writeValueAsString(definition.properties),
            )
            store.insertColumnRows(tx, newIdSql, cols)
            tx.add("INSERT INTO table_totals(table_id) VALUES ($newIdSql)")
            if (definition.partitionFields.isNotEmpty()) {
                tx.add(
                    "INSERT INTO partition_specs(table_id, spec_id, fields_json, begin_snapshot) " +
                        "VALUES ($newIdSql, 1, ?, ${HogliteSchema.HEAD_SQL})",
                    mapper.writeValueAsString(definition.partitionFields),
                )
            }
            if (definition.sortFields.isNotEmpty()) {
                tx.add(
                    "INSERT INTO sort_specs(table_id, spec_id, fields_json, begin_snapshot) " +
                        "VALUES ($newIdSql, 1, ?, ${HogliteSchema.HEAD_SQL})",
                    mapper.writeValueAsString(definition.sortFields),
                )
            }
            tx.insertSnapshotRow(definition.author, definition.message)

            // registerInitialFiles, AT the creation snapshot: file rows with
            // server-assigned row lineage from the new table's zeroed
            // allocator, the claims the operation owned settled, and the
            // removal-queue refusal.
            if (files.isNotEmpty()) {
                val liveColumnTypes =
                    cols.flatMap { it.selfAndDescendants() }
                        .filter { !it.def.type.isNested }
                        .associate { it.fieldId to it.def.type }
                val specFieldCount = definition.partitionFields.size.takeIf { definition.partitionFields.isNotEmpty() }
                val specId: Long? = if (specFieldCount != null) 1L else null
                tx.expectZero(
                    "SELECT COUNT(*) FROM removals WHERE drained_at IS NULL AND path IN " +
                        "(${files.joinToString(",") { "?" }})",
                    *files.map { it.path }.toTypedArray(),
                ) {
                    HoglakeException.CommitConflict(
                        "path(s) scheduled for deletion by the cleanup queue; use fresh paths, " +
                            "or retry after the removal queue drains",
                    )
                }
                tx.affectOne(
                    "UPDATE meta SET next_file_id = next_file_id + ${files.size} WHERE id = 1",
                ) { HoglakeException.NotFound("catalog '$catalog'") }
                var rowOffset = 0L
                files.forEachIndexed { i, f ->
                    val sanitized =
                        HogliteCommit.validateFile(
                            f,
                            "${definition.namespace}.${definition.name}",
                            specFieldCount,
                            liveColumnTypes,
                        )
                    val idSql = "(SELECT next_file_id - ${files.size} + $i FROM meta WHERE id = 1)"
                    tx.add(
                        "INSERT INTO files(id, table_id, path, record_count, file_size_bytes, footer_size, " +
                            "row_id_start, spec_id, partition_values_json, stats_state, stats_json, " +
                            "split_offsets_json, begin_snapshot) " +
                            "VALUES ($idSql, $newIdSql, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ${HogliteSchema.HEAD_SQL})",
                        f.path,
                        f.recordCount,
                        f.fileSizeBytes,
                        f.footerSize,
                        rowOffset,
                        specId,
                        f.partitionValues?.let { mapper.writeValueAsString(it) },
                        if (sanitized == null) "pending" else "provided",
                        sanitized?.let { mapper.writeValueAsString(it) },
                        f.splitOffsets?.let { mapper.writeValueAsString(it) },
                    )
                    rowOffset = Math.addExact(rowOffset, f.recordCount)
                }
                tx.add(
                    "UPDATE tables SET next_row_id = next_row_id + $rowOffset WHERE id = $newIdSql",
                )
                tx.add(
                    "UPDATE table_totals SET record_count = ?, file_count = ?, file_size_bytes = ? " +
                        "WHERE table_id = $newIdSql",
                    files.sumOf { it.recordCount },
                    files.size.toLong(),
                    files.sumOf { it.fileSizeBytes },
                )
                tx.add(
                    "INSERT INTO changes(snapshot_id, kind, object_id) " +
                        "VALUES (${HogliteSchema.HEAD_SQL}, 'table_inserted_into', $newIdSql)",
                )
                // Settle the operation's claims (owner = operation id), the
                // same decision decideClaims makes for commits.
                val claimPaths = files.associate { it.path to "data" }
                val toSettle = HogliteCommit.decideClaims(store, catalog, claimPaths, operationId)
                for (uploadId in toSettle) {
                    tx.affectOne(
                        "UPDATE uploads SET state = 'registered' " +
                            "WHERE upload_id = ? AND state = 'active' AND owner = ?",
                        uploadId,
                        operationId.toString(),
                    ) {
                        HoglakeException.CommitConflict(
                            "upload is fenced, already registered, or belongs to another operation",
                        )
                    }
                }
            }
            tx.add(
                "UPDATE table_creations SET state = 'committed', snapshot_id = ${HogliteSchema.HEAD_SQL}, " +
                    "schema_version = (SELECT schema_version FROM meta WHERE id = 1) WHERE operation = ?",
                operationId.toString(),
            )
            try {
                store.runTx(catalog, HogliteCatalogStore.freshTxid("publish"), null, tx)
            } catch (race: PublishRace) {
                if (race.receipt.state != "aborted") requireSameFiles(catalog, operationId, encodedFiles)
                return race.receipt
            } catch (e: HoglakeException) {
                when (e) {
                    // The three durable guards; anything else (removal queue,
                    // claim settle) stays the transient refusal the caller
                    // retries.
                    nsChangedRace -> return rejectDurably(catalog, operationId, encodedFiles, "namespace_changed")
                    targetChangedRace -> return rejectDurably(catalog, operationId, encodedFiles, "target_changed")
                    targetExistsRace -> return rejectDurably(catalog, operationId, encodedFiles, "target_exists")
                    else -> throw e
                }
            }
            return store.loadCreationOrThrow(catalog, operationId)
        }

    /**
     * Land the operation's terminal REJECTED receipt (with the attempted
     * payload recorded, so a replay compares equal) and answer it.
     */
    private fun rejectDurably(
        catalog: String,
        operationId: UUID,
        encodedFiles: String,
        reason: String,
    ): TableCreation {
        val tx = HogliteTx()
        tx.affectOne(
            "UPDATE table_creations SET files_json = ? WHERE operation = ? AND state = 'prepared'",
            encodedFiles,
            operationId.toString(),
        ) { throw PublishRace(store.loadCreationOrThrow(catalog, operationId)) }
        tx.add(
            "UPDATE table_creations SET state = 'rejected', reason = ? WHERE operation = ?",
            reason,
            operationId.toString(),
        )
        try {
            store.runTx(catalog, HogliteCatalogStore.freshTxid("publish-reject"), null, tx)
        } catch (race: PublishRace) {
            if (race.receipt.state != "aborted") requireSameFiles(catalog, operationId, encodedFiles)
            return race.receipt
        }
        return store.loadCreationOrThrow(catalog, operationId)
    }

    /** Same-definition check for a prepare replay (decoded structures, not bytes). */
    private fun requireSameDefinition(
        existing: TableCreation,
        requested: TableCreationDefinition,
        operationId: UUID,
    ) {
        if (existing.definition != requested) {
            throw HoglakeException.CommitConflict("operation id reused with a different definition")
        }
    }

    private fun requireSameFiles(
        catalog: String,
        operationId: UUID,
        encodedFiles: String,
    ) {
        val stored =
            store.queryCatalog(
                catalog,
                "SELECT files_json FROM table_creations WHERE operation = ?",
                operationId.toString(),
            ).firstRow()?.get(0) as? String
        val same =
            stored != null &&
                mapper.readValue<List<FileRegistration>>(stored) ==
                mapper.readValue<List<FileRegistration>>(encodedFiles)
        if (!same) throw HoglakeException.CommitConflict("operation id reused with a different files")
    }

    private fun transition(
        catalog: String,
        operationId: UUID,
        state: String,
        reason: String,
    ) {
        val tx = HogliteTx()
        tx.add(
            "UPDATE table_creations SET state = ?, reason = ? WHERE operation = ? AND state = 'prepared'",
            state,
            reason,
            operationId.toString(),
        )
        store.runTx(catalog, HogliteCatalogStore.freshTxid("transition"), null, tx)
    }

    private fun tableAtOrNull(
        catalog: String,
        namespaceId: Long,
        namespace: String,
        table: String,
        at: Long,
    ): HogliteCatalogStore.TableRow? =
        try {
            store.tableAt(catalog, namespaceId, namespace, table, at)
        } catch (e: HoglakeException.NotFound) {
            null
        }

    private fun loadOrNull(
        catalog: String,
        operationId: UUID,
    ): TableCreation? =
        try {
            store.loadCreationOrThrow(catalog, operationId)
        } catch (e: HoglakeException.NotFound) {
            null
        }

    /** A prepare that lost the race to an identical one: the winner's receipt is the answer. */
    private class PrepareRaceWon(
        catalog: String,
        operationId: UUID,
        val receipt: TableCreation,
    ) : RuntimeException("prepare race on $operationId in $catalog")

    /** A publish whose receipt transitioned mid-flight: the new receipt is the answer. */
    private class PublishRace(val receipt: TableCreation) : RuntimeException("publish race")

    /*
     * Publish-time guard failures that must land as DURABLE rejections
     * (the operation's terminal receipt), matched by IDENTITY so a
     * lookalike transient conflict — the removal-queue guard, a claim
     * settle — still propagates to the caller as retryable.
     */
    private val nsChangedRace = HoglakeException.NotFound("namespace changed during publication")
    private val targetChangedRace = HoglakeException.CommitConflict("replacement target changed during publication")
    private val targetExistsRace = HoglakeException.AlreadyExists("table appeared during publication")
}
