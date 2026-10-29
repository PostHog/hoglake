package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.PartitionSpec
import com.posthog.hoglake.model.SortSpec
import com.posthog.hoglake.model.TableInfo
import com.posthog.hoglake.model.allNodes
import com.posthog.hoglake.model.assignFieldIds
import com.posthog.hoglake.model.boundReencodeFor
import com.posthog.hoglake.model.canPromoteTo
import com.posthog.hoglake.service.AlterBackend
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.ColumnTrees
import com.posthog.hoglake.service.Identifiers
import com.posthog.hoglake.service.TableMetadata
import com.posthog.hoglake.stats.IcebergSingleValue
import java.util.UUID

/**
 * [AlterBackend] on hoglite: [com.posthog.hoglake.service.AlterService]'s
 * op semantics over the tenant's versioned rows.
 *
 * One ALTER is one hoglite transaction: guards (incarnation, optional
 * read_snapshot DDL fence), the DDL snapshot allocation, the
 * `table_altered` change row, and the per-op effects — column version
 * rows ended/opened, table version rows ended/opened, spec
 * replacements — land together. The op-by-op VALIDATION logic is a
 * direct port of AlterService's: same refusal order, same messages,
 * same typed exceptions; the pure validators (requireColumn,
 * replaceNode, the struct-interior rules) are duplicated here because
 * AlterService's are private to it and worth exactly one copy each.
 */
internal class HogliteAlters(
    private val store: HogliteCatalogStore,
) : AlterBackend {
    override fun alterTable(
        catalog: String,
        namespace: String,
        table: String,
        ops: List<AlterOp>,
        expectedTableUuid: UUID?,
        readSnapshot: Long?,
        author: String?,
        message: String?,
    ): TableInfo {
        if (ops.isEmpty()) throw HoglakeException.Validation("ops must contain at least one operation")
        val meta = store.meta(catalog)
        val ns = store.liveNamespace(catalog, namespace)
        val t = store.liveTable(catalog, namespace, table)
        if (expectedTableUuid != null && t.tableUuid != expectedTableUuid) {
            throw HoglakeException.TableRecreated(
                "table '$namespace.$table' no longer has the expected UUID: it was " +
                    "recreated; re-read it before retrying",
                table = "$namespace.$table",
                expectedTableUuid = expectedTableUuid,
                currentTableUuid = t.tableUuid,
            )
        }
        if (readSnapshot != null) {
            if (readSnapshot < 0 || readSnapshot > meta.head) {
                throw HoglakeException.Validation("read_snapshot must be between zero and catalog head")
            }
            if (readSnapshot < meta.earliestSnapshotId) {
                throw HoglakeException.Expired("read_snapshot is below the expiry floor")
            }
            if (ddlChangedSince(catalog, t.tableId, readSnapshot)) {
                throw HoglakeException.DdlSinceReadSnapshot(
                    "concurrent DDL since snapshot $readSnapshot on table(s): " +
                        "$namespace.$table; re-read the table and re-issue the alter, " +
                        "replaying this request cannot succeed",
                    tables = listOf("$namespace.$table"),
                    readSnapshot = readSnapshot,
                )
            }
        }

        // Live shape as of the pre-alter head.
        val state =
            TableState(
                cols = store.columnsAt(catalog, t.tableId, meta.head),
                name = t.name,
                comment = t.comment,
                properties = t.properties,
                spec = store.specAt(catalog, t.tableId, meta.head),
                sortSpec = store.sortSpecAt(catalog, t.tableId, meta.head),
            )

        // The field-id allocator is planned LOCALLY across the ops of
        // one alter: the first AddColumn pins it with a scalar guard, and
        // later ones advance the pinned value (each reads the same
        // pre-tx row, so without this the second AddColumn's guard would
        // always trip on the first's bump).
        val plannedNextFieldId = longArrayOf(-1L)

        val tx = HogliteTx()
        tx.expectOne(
            "SELECT COUNT(*) FROM tables WHERE id = ? AND dropped_snapshot IS NULL AND table_uuid = ?",
            t.tableId,
            t.tableUuid.toString(),
        ) {
            HoglakeException.TableRecreated(
                "table '$namespace.$table' was recreated under the alter",
                table = "$namespace.$table",
                expectedTableUuid = t.tableUuid,
                currentTableUuid = t.tableUuid,
            )
        }
        if (readSnapshot != null) {
            tx.expectZero(
                "SELECT COUNT(*) FROM changes WHERE object_id = ? AND snapshot_id > ? " +
                    "AND kind IN ('table_altered', 'table_dropped')",
                t.tableId,
                readSnapshot,
            ) {
                HoglakeException.DdlSinceReadSnapshot(
                    "concurrent DDL since snapshot $readSnapshot on table(s): $namespace.$table; " +
                        "re-read the table and re-issue the alter",
                    tables = listOf("$namespace.$table"),
                    readSnapshot = readSnapshot,
                )
            }
        }
        // The schema fence: the ops were planned against head's
        // columns/specs in Kotlin; an unguarded alter (readSnapshot =
        // null) has no changes-window guard, so without this a
        // concurrent alter between planning and the tx would be
        // overwritten by stale in-memory state. Same OCC as the commit
        // path's schema fence, but an alter IS DDL, so a miss is the
        // typed retryable conflict, not a replan.
        tx.expectScalar(
            meta.schemaVersion,
            "SELECT schema_version FROM meta WHERE id = 1",
        ) {
            HoglakeException.CommitConflict(
                "catalog '$catalog' schema changed under the alter on $namespace.$table; re-read and retry",
            )
        }
        if (readSnapshot != null) {
            // The expiry floor, in-tx: a sweep that purged the window
            // since planning must not let the changes guard pass empty.
            tx.expectZero(
                "SELECT COUNT(*) FROM meta WHERE id = 1 AND earliest_snapshot_id > ?",
                readSnapshot,
            ) {
                HoglakeException.Expired(
                    "read_snapshot $readSnapshot is below the expiry floor; re-read the table and re-issue",
                )
            }
        }
        tx.allocateDdlSnapshot()
        tx.insertSnapshotRow(author, message)
        tx.add(
            "INSERT INTO changes(snapshot_id, kind, object_id) " +
                "VALUES (${HogliteSchema.HEAD_SQL}, 'table_altered', ?)",
            t.tableId,
        )
        for (op in ops) applyOp(catalog, tx, t, ns.namespaceId, namespace, state, op, plannedNextFieldId)

        val readback = tx.add(HogliteSql.READBACK_DDL)
        val results = store.runTx(catalog, HogliteCatalogStore.freshTxid("alter"), null, tx)
        val snapshotId = results[readback].firstRow()!![0].asLong()

        // STAYS EXACT, like the Postgres path (#232): the snapshot this
        // receipt answers for was allocated in this transaction, so no
        // sampler could have seen it; an alter is rare DDL beside which
        // one aggregate is noise.
        val agg =
            store.queryCatalog(
                catalog,
                "SELECT COALESCE(SUM(record_count),0), COUNT(*), COALESCE(SUM(file_size_bytes),0) " +
                    "FROM files WHERE table_id = ? AND begin_snapshot <= ? " +
                    "AND (end_snapshot IS NULL OR end_snapshot > ?)",
                t.tableId,
                snapshotId,
                snapshotId,
            ).firstRow()!!
        return TableInfo(
            tableId = t.tableId,
            tableUuid = t.tableUuid,
            namespace = namespace,
            name = state.name,
            comment = state.comment,
            properties = state.properties,
            columns = state.cols.sortedBy { it.ordinal },
            readSnapshotId = snapshotId,
            recordCount = agg[0].asLong(),
            fileCount = agg[1].asLong(),
            fileSizeBytes = agg[2].asLong(),
            partitionSpec = state.spec,
            sortSpec = state.sortSpec,
            snapshotId = snapshotId,
        )
    }

    private fun ddlChangedSince(
        catalog: String,
        tableId: Long,
        readSnapshot: Long,
    ): Boolean =
        store.queryCatalog(
            catalog,
            "SELECT COUNT(*) FROM changes WHERE object_id = ? AND snapshot_id > ? " +
                "AND kind IN ('table_altered', 'table_dropped')",
            tableId,
            readSnapshot,
        ).firstRow()!![0].asLong() > 0

    // ---- op application (AlterService.applyOp, ported) -------------------

    private class TableState(
        var cols: List<Column>,
        var name: String,
        var comment: String?,
        var properties: Map<String, String>,
        var spec: PartitionSpec?,
        var sortSpec: SortSpec?,
    )

    private class Located(
        val column: Column,
        val parent: Column?,
    )

    private fun applyOp(
        catalog: String,
        tx: HogliteTx,
        t: HogliteCatalogStore.TableRow,
        namespaceId: Long,
        namespaceName: String,
        state: TableState,
        op: AlterOp,
        plannedNextFieldId: LongArray,
    ) {
        when (op) {
            is AlterOp.AddColumn -> addColumn(catalog, tx, t, state, op, plannedNextFieldId)
            is AlterOp.DropColumn -> dropColumn(tx, t, state, op)
            is AlterOp.RenameColumn -> renameColumn(catalog, tx, t, state, op)
            is AlterOp.PromoteColumn -> promoteColumn(catalog, tx, t, state, op)
            is AlterOp.RenameTable -> renameTable(tx, t, namespaceId, namespaceName, state, op)
            is AlterOp.SetPartitionSpec -> setPartitionSpec(tx, t, state, op)
            is AlterOp.SetSortOrder -> setSortOrder(tx, t, state, op)
            is AlterOp.SetColumnComment -> {
                TableMetadata.validateComment(op.comment)
                val located = requireColumn(state, op.name)
                val col = located.column
                val updated = col.copy(def = col.def.copy(comment = op.comment))
                endOrDeleteColumnRow(tx, t.tableId, col.fieldId)
                insertColumnVersion(tx, t.tableId, updated, located.parent?.fieldId)
                state.cols = replaceNode(state.cols, col.fieldId) { updated }
            }
            is AlterOp.SetTableComment -> {
                TableMetadata.validateComment(op.comment)
                state.comment = op.comment
                rewriteMetadata(tx, t, namespaceId, state)
            }
            is AlterOp.SetProperties -> {
                TableMetadata.validateProperties(op.properties)
                state.properties = op.properties.toMap()
                rewriteMetadata(tx, t, namespaceId, state)
            }
        }
    }

    private fun addColumn(
        catalog: String,
        tx: HogliteTx,
        t: HogliteCatalogStore.TableRow,
        state: TableState,
        op: AlterOp.AddColumn,
        plannedNextFieldId: LongArray,
    ) {
        val parent = op.parent?.let { requireStructParent(state, it) }
        val siblings = parent?.children ?: state.cols
        val where = if (parent == null) "" else " of struct '${op.parent}'"
        if (siblings.any { it.def.name == op.def.name }) {
            throw HoglakeException.Validation(
                "column '${Identifiers.cap(op.def.name)}'$where already exists",
            )
        }
        ColumnTrees.validate(
            listOf(op.def),
            depthOffset = parent?.let { depthOf(state, it) } ?: 0,
            existingNodes = state.cols.allNodes().size,
        )
        requireAddedSubtreeNullable(op)
        requireFieldIds(catalog, tx, t, "cannot add column '${op.def.name}'")
        val count = ColumnTrees.nodeCount(listOf(op.def))
        // Allocate the field-id range in the transaction; the VALUES are
        // computed here, pinned by a scalar guard on the allocator. Only
        // the FIRST AddColumn of this alter reads and guards the row;
        // later ones advance the locally pinned value.
        val nextField =
            if (plannedNextFieldId[0] >= 0) {
                plannedNextFieldId[0]
            } else {
                val current =
                    store.queryCatalog(
                        catalog,
                        "SELECT next_field_id FROM tables WHERE id = ?",
                        t.tableId,
                    ).firstRow()!![0].asLong()
                tx.expectScalar(
                    current,
                    "SELECT next_field_id FROM tables WHERE id = ?",
                    t.tableId,
                ) { HoglakeException.CommitConflict("table ${t.namespace}.${t.name} changed under the alter") }
                current
            }
        plannedNextFieldId[0] = nextField + count
        tx.affectOne(
            "UPDATE tables SET next_field_id = next_field_id + $count WHERE id = ?",
            t.tableId,
        ) { HoglakeException.NotFound("table '${t.namespace}.${t.name}' in catalog") }
        val assigned = assignFieldIds(listOf(op.def), nextField).single()
        val col = assigned.copy(ordinal = (siblings.maxOfOrNull { it.ordinal } ?: -1) + 1)
        insertSubtree(tx, t.tableId, col, parent?.fieldId)
        state.cols =
            if (parent == null) {
                state.cols + col
            } else {
                replaceNode(state.cols, parent.fieldId) { it.copy(children = it.children + col) }
            }
    }

    private fun requireAddedSubtreeNullable(op: AlterOp.AddColumn) {
        val top = op.parent?.let { "$it.${op.def.name}" } ?: op.def.name

        fun walk(
            def: com.posthog.hoglake.model.ColumnDef,
            qualified: String,
            isMapKey: Boolean,
        ) {
            if (!def.nullable && !isMapKey) {
                val which = if (qualified == top) "it is" else "'$qualified' inside it is"
                throw HoglakeException.Validation(
                    "cannot add column '$top': $which declared required (nullable=false), but " +
                        "hoglake has no column defaults; files written before the column read NULL " +
                        "for it, so every node of an added column must be nullable (a map's key excepted)",
                )
            }
            def.children?.forEachIndexed { i, child ->
                walk(child, "$qualified.${child.name}", isMapKey = def.type == ColType.MAP && i == 0)
            }
        }
        walk(op.def, top, isMapKey = false)
    }

    private fun dropColumn(
        tx: HogliteTx,
        t: HogliteCatalogStore.TableRow,
        state: TableState,
        op: AlterOp.DropColumn,
    ) {
        val located = requireColumn(state, op.name)
        val col = located.column
        val parent = located.parent
        if (parent == null && state.cols.size == 1) {
            throw HoglakeException.Validation("cannot drop '${op.name}': it is the last column")
        }
        if (parent != null && parent.children.size == 1) {
            throw HoglakeException.Validation(
                "cannot drop '${op.name}': it is the last field of struct '${parent.def.name}', " +
                    "and a struct with no fields has no representation in parquet or Iceberg",
            )
        }
        val doomed = col.selfAndDescendants().map { it.fieldId }.toSet()
        state.spec?.fields?.firstOrNull { it.sourceFieldId in doomed }?.let { f ->
            throw HoglakeException.Validation(
                if (f.sourceFieldId == col.fieldId) {
                    "cannot drop column '${op.name}': it is a source of the live partition spec"
                } else {
                    "cannot drop column '${op.name}': field_id ${f.sourceFieldId} inside it " +
                        "is a source of the live partition spec"
                },
            )
        }
        state.sortSpec?.fields?.firstOrNull { it.sourceFieldId in doomed }?.let { f ->
            throw HoglakeException.Validation(
                if (f.sourceFieldId == col.fieldId) {
                    "cannot drop column '${op.name}': it is a source of the live sort order"
                } else {
                    "cannot drop column '${op.name}': field_id ${f.sourceFieldId} inside it " +
                        "is a source of the live sort order"
                },
            )
        }
        for (id in doomed) endOrDeleteColumnRow(tx, t.tableId, id)
        state.cols =
            if (parent == null) {
                state.cols.filter { it.fieldId != col.fieldId }
            } else {
                replaceNode(state.cols, parent.fieldId) { p ->
                    p.copy(children = p.children.filter { it.fieldId != col.fieldId })
                }
            }
    }

    private fun renameColumn(
        catalog: String,
        tx: HogliteTx,
        t: HogliteCatalogStore.TableRow,
        state: TableState,
        op: AlterOp.RenameColumn,
    ) {
        Identifiers.validateColumn(op.to)
        val located = requireColumn(state, op.from)
        val col = located.column
        val siblings = located.parent?.children ?: state.cols
        if (siblings.any { it.def.name == op.to }) {
            val where = located.parent?.let { " of struct '${it.def.name}'" } ?: ""
            throw HoglakeException.Validation("column '${op.to}'$where already exists")
        }
        // The field-id contract guard: files whose parquet schema
        // carries no field ids bind columns by NAME; renaming while any
        // such file is live would silently NULL that column's history.
        requireFieldIds(catalog, tx, t, "cannot rename column '${op.from}' to '${op.to}'")
        endOrDeleteColumnRow(tx, t.tableId, col.fieldId)
        // Only THIS node's row is rewritten; children keep their rows.
        val renamed = col.copy(def = col.def.copy(name = op.to))
        insertColumnVersion(tx, t.tableId, renamed, located.parent?.fieldId)
        state.cols = replaceNode(state.cols, col.fieldId) { renamed }
    }

    private fun promoteColumn(
        catalog: String,
        tx: HogliteTx,
        t: HogliteCatalogStore.TableRow,
        state: TableState,
        op: AlterOp.PromoteColumn,
    ) {
        val located = requireColumn(state, op.name)
        val col = located.column
        if (col.def.type.isNested || op.to.isNested) {
            throw HoglakeException.Validation(
                "cannot promote column '${op.name}': '${col.def.type.wire}' and '${op.to.wire}' " +
                    "include a nested container type, and nested containers are not promotable; " +
                    "promote a struct's LEAF field instead",
            )
        }
        if (!col.def.type.canPromoteTo(op.to)) {
            throw HoglakeException.Validation(
                "cannot promote column '${op.name}' from '${col.def.type.wire}' to '${op.to.wire}'",
            )
        }
        endOrDeleteColumnRow(tx, t.tableId, col.fieldId)
        val promoted = col.copy(def = col.def.copy(type = op.to))
        insertColumnVersion(tx, t.tableId, promoted, located.parent?.fieldId)
        state.cols = replaceNode(state.cols, col.fieldId) { promoted }
        reencodeStatsOnPromote(catalog, tx, t.tableId, col.fieldId, col.def.type, op.to)
    }

    /**
     * Same-transaction stats re-encode for a width-changing promote
     * (AlterService.reencodeStatsOnPromote): stored bounds for the
     * column were written in the OLD type's 4-byte encoding; readers
     * decode bounds under the LIVE type. The stored stats are the file
     * row's JSON column-stats list, so the re-encode is read-patch-write
     * per file, guarded on the old content.
     */
    private fun reencodeStatsOnPromote(
        catalog: String,
        tx: HogliteTx,
        tableId: Long,
        fieldId: Long,
        from: ColType,
        to: ColType,
    ) {
        val widen: (ByteArray) -> ByteArray =
            when (boundReencodeFor(from, to)) {
                com.posthog.hoglake.model.BoundReencode.NONE -> return
                com.posthog.hoglake.model.BoundReencode.INT_TO_LONG -> { b ->
                    IcebergSingleValue.encode(ColType.LONG, (IcebergSingleValue.decode(ColType.INT, b) as Int).toLong())
                }
                com.posthog.hoglake.model.BoundReencode.FLOAT_TO_DOUBLE -> { b ->
                    IcebergSingleValue.encode(
                        ColType.DOUBLE,
                        (IcebergSingleValue.decode(ColType.FLOAT, b) as Float).toDouble(),
                    )
                }
            }
        val rows =
            store.queryCatalog(
                catalog,
                "SELECT id, stats_json FROM files WHERE table_id = ? AND stats_json IS NOT NULL",
                tableId,
            ).rows
        for (r in rows) {
            val fileId = r[0].asLong()
            val statsJson = r[1].asString()
            val stats = store.json.readValue<List<ColumnStats>>(statsJson)
            var touched = false
            val patched =
                stats.map { stat ->
                    if (stat.fieldId != fieldId) {
                        stat
                    } else {
                        val lower = stat.lowerBound?.let { if (it.size == 4) widen(it) else it }
                        val upper = stat.upperBound?.let { if (it.size == 4) widen(it) else it }
                        if (lower !== stat.lowerBound || upper !== stat.upperBound) {
                            touched = true
                            stat.copy(lowerBound = lower, upperBound = upper)
                        } else {
                            stat
                        }
                    }
                }
            if (touched) {
                tx.affectOne(
                    "UPDATE files SET stats_json = ? WHERE id = ? AND stats_json = ?",
                    store.json.writeValueAsString(patched),
                    fileId,
                    statsJson,
                ) { HoglakeException.CommitConflict("file stats changed under the alter") }
            }
        }
    }

    private fun rewriteMetadata(
        tx: HogliteTx,
        t: HogliteCatalogStore.TableRow,
        namespaceId: Long,
        state: TableState,
    ) {
        endOrDeleteVersionRow(tx, t.tableId)
        insertVersion(tx, t.tableId, namespaceId, t.namespace, state.name, state.comment, state.properties)
    }

    private fun renameTable(
        tx: HogliteTx,
        t: HogliteCatalogStore.TableRow,
        namespaceId: Long,
        namespaceName: String,
        state: TableState,
        op: AlterOp.RenameTable,
    ) {
        Identifiers.validate("table", op.newName)
        tx.expectZero(
            "SELECT COUNT(*) FROM table_versions WHERE namespace_id = ? AND name = ? " +
                "AND end_snapshot IS NULL AND table_id <> ?",
            namespaceId,
            op.newName,
            t.tableId,
        ) {
            HoglakeException.AlreadyExists(
                "table '${op.newName}' already exists in namespace '$namespaceName'",
            )
        }
        endOrDeleteVersionRow(tx, t.tableId)
        insertVersion(tx, t.tableId, namespaceId, namespaceName, op.newName, state.comment, state.properties)
        // The identity row's namespace/name are the head cache.
        tx.affectOne(
            "UPDATE tables SET name = ? WHERE id = ?",
            op.newName,
            t.tableId,
        ) { HoglakeException.NotFound("table '$namespaceName.${t.name}' in catalog") }
        state.name = op.newName
    }

    private fun setPartitionSpec(
        tx: HogliteTx,
        t: HogliteCatalogStore.TableRow,
        state: TableState,
        op: AlterOp.SetPartitionSpec,
    ) {
        AlterService.validatePartitionFields(state.cols, op.fields)
        tx.add(
            "UPDATE partition_specs SET end_snapshot = ${HogliteSchema.HEAD_SQL} " +
                "WHERE table_id = ? AND end_snapshot IS NULL",
            t.tableId,
        )
        if (op.fields.isEmpty()) {
            state.spec = null
            return
        }
        tx.add(
            "INSERT INTO partition_specs(table_id, spec_id, fields_json, begin_snapshot) " +
                "VALUES (?, (SELECT COALESCE(MAX(spec_id), 0) + 1 FROM partition_specs WHERE table_id = ?), " +
                "?, ${HogliteSchema.HEAD_SQL})",
            t.tableId,
            t.tableId,
            store.json.writeValueAsString(op.fields),
        )
        // The new id is max+1 computed in-transaction; the receipt wants the value.
        state.spec = PartitionSpec((state.spec?.specId ?: 0L) + 1, op.fields)
    }

    private fun setSortOrder(
        tx: HogliteTx,
        t: HogliteCatalogStore.TableRow,
        state: TableState,
        op: AlterOp.SetSortOrder,
    ) {
        AlterService.validateSortFields(state.cols, op.fields)
        tx.add(
            "UPDATE sort_specs SET end_snapshot = ${HogliteSchema.HEAD_SQL} " +
                "WHERE table_id = ? AND end_snapshot IS NULL",
            t.tableId,
        )
        if (op.fields.isEmpty()) {
            state.sortSpec = null
            return
        }
        tx.add(
            "INSERT INTO sort_specs(table_id, spec_id, fields_json, begin_snapshot) " +
                "VALUES (?, (SELECT COALESCE(MAX(spec_id), 0) + 1 FROM sort_specs WHERE table_id = ?), " +
                "?, ${HogliteSchema.HEAD_SQL})",
            t.tableId,
            t.tableId,
            store.json.writeValueAsString(op.fields),
        )
        // begin_snapshot: this alter's snapshot, resolved at readback;
        // the receipt only needs fields + id, and reads resolve the
        // interval from the row.
        state.sortSpec = SortSpec((state.sortSpec?.sortId ?: 0L) + 1, op.fields, state.sortSpec?.beginSnapshot ?: 0)
    }

    // ---- row lifecycle helpers -------------------------------------------

    /**
     * Retire a field's live column row at this transaction's snapshot:
     * delete it if it was OPENED by this same transaction (never
     * visible), else end it.
     */
    private fun endOrDeleteColumnRow(
        tx: HogliteTx,
        tableId: Long,
        fieldId: Long,
    ) {
        tx.add(
            "DELETE FROM columns WHERE table_id = ? AND field_id = ? " +
                "AND begin_snapshot = ${HogliteSchema.HEAD_SQL}",
            tableId,
            fieldId,
        )
        tx.add(
            "UPDATE columns SET end_snapshot = ${HogliteSchema.HEAD_SQL} " +
                "WHERE table_id = ? AND field_id = ? AND end_snapshot IS NULL",
            tableId,
            fieldId,
        )
    }

    /** Insert one column VERSION row (node only — children keep their own rows). */
    private fun insertColumnVersion(
        tx: HogliteTx,
        tableId: Long,
        col: Column,
        parentFieldId: Long?,
    ) {
        tx.add(
            "INSERT INTO columns(table_id, field_id, parent_field_id, ordinal, name, type, " +
                "type_params_json, nullable, comment, begin_snapshot) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ${HogliteSchema.HEAD_SQL})",
            tableId,
            col.fieldId,
            parentFieldId,
            col.ordinal,
            col.def.name,
            col.def.type.wire,
            col.def.typeParams?.let { store.json.writeValueAsString(it) },
            if (col.def.nullable) 1L else 0L,
            col.def.comment,
        )
    }

    /** Insert a whole new subtree's column rows (add_column). */
    private fun insertSubtree(
        tx: HogliteTx,
        tableId: Long,
        root: Column,
        rootParent: Long?,
    ) {
        fun walk(
            col: Column,
            parent: Long?,
        ) {
            insertColumnVersion(tx, tableId, col, parent)
            col.children.forEach { walk(it, col.fieldId) }
        }
        walk(root, rootParent)
    }

    private fun endOrDeleteVersionRow(
        tx: HogliteTx,
        tableId: Long,
    ) {
        tx.add(
            "DELETE FROM table_versions WHERE table_id = ? AND begin_snapshot = ${HogliteSchema.HEAD_SQL}",
            tableId,
        )
        tx.add(
            "UPDATE table_versions SET end_snapshot = ${HogliteSchema.HEAD_SQL} " +
                "WHERE table_id = ? AND end_snapshot IS NULL",
            tableId,
        )
    }

    private fun insertVersion(
        tx: HogliteTx,
        tableId: Long,
        namespaceId: Long,
        namespace: String,
        name: String,
        comment: String?,
        properties: Map<String, String>,
    ) {
        tx.add(
            "INSERT INTO table_versions(table_id, namespace_id, namespace, name, comment, properties_json, " +
                "begin_snapshot) VALUES (?, ?, ?, ?, ?, ?, ${HogliteSchema.HEAD_SQL})",
            tableId,
            namespaceId,
            namespace,
            name,
            comment,
            store.json.writeValueAsString(properties),
        )
    }

    /**
     * The field-id guard for add/rename column: refuse (409
     * `idless_files_present`) while any LIVE file of the table is
     * flagged missing_field_ids. A guard INSIDE the transaction, so a
     * hydrator flag landing mid-alter refuses it rather than racing it.
     */
    private fun requireFieldIds(
        catalog: String,
        tx: HogliteTx,
        t: HogliteCatalogStore.TableRow,
        action: String,
    ) {
        val idlessLive =
            store.queryCatalog(
                catalog,
                "SELECT COUNT(*) FROM files WHERE table_id = ? AND missing_field_ids = 1 " +
                    "AND end_snapshot IS NULL",
                t.tableId,
            ).firstRow()!![0].asLong()
        tx.expectZero(
            "SELECT COUNT(*) FROM files WHERE table_id = ? AND missing_field_ids = 1 " +
                "AND end_snapshot IS NULL",
            t.tableId,
        ) {
            HoglakeException.IdlessFilesPresent(
                "$action: $idlessLive id-less live data file(s) bind columns by name; " +
                    "rewrite (compact) or retire them first",
            )
        }
    }

    // ---- the pure forest walkers (AlterService's, one copy each) ---------

    private fun requireColumn(
        state: TableState,
        path: String,
    ): Located {
        val shown = Identifiers.cap(path)
        val segments = path.split('.')
        if (segments.any { it.isEmpty() }) {
            throw HoglakeException.Validation(
                "invalid column path '$shown': a path is dot-separated column names, none empty",
            )
        }
        var siblings = state.cols
        var parent: Column? = null
        var current: Column? = null
        for ((i, segment) in segments.withIndex()) {
            if (i > 0) {
                val container =
                    current ?: throw HoglakeException.Validation("column '$shown' does not exist")
                assertStructInterior(container, segments.take(i).joinToString("."), path)
                parent = container
                siblings = container.children
            }
            val candidates = siblings.filter { it.def.name == segment }
            if (candidates.size > 1) {
                throw HoglakeException.Validation(
                    "column path '$shown' is ambiguous: ${candidates.size} live columns are named " +
                        "'$segment' here (field ids ${candidates.map { it.fieldId }.sorted()}); " +
                        "the catalog is inconsistent and this ALTER will not guess",
                )
            }
            current =
                candidates.singleOrNull()
                    ?: throw HoglakeException.Validation(
                        if (i == 0) {
                            "column '$shown' does not exist"
                        } else {
                            "column '$shown' does not exist: struct " +
                                "'${segments.take(i).joinToString(".")}' has no field " +
                                "'${Identifiers.cap(segment)}'"
                        },
                    )
        }
        return Located(current!!, parent)
    }

    private fun requireStructParent(
        state: TableState,
        path: String,
    ): Column {
        val located = requireColumn(state, path)
        assertStructInterior(located.column, path, path)
        return located.column
    }

    private fun assertStructInterior(
        container: Column,
        containerPath: String,
        fullPath: String,
    ) {
        if (container.def.type == ColType.STRUCT) return
        if (container.def.type.isNested) {
            throw HoglakeException.Validation(
                "cannot address '${Identifiers.cap(fullPath)}': " +
                    "'$containerPath' is a '${container.def.type.wire}', " +
                    "and list/map internals (element, key, value) cannot be added, dropped or " +
                    "renamed — only struct fields can",
            )
        }
        throw HoglakeException.Validation(
            "cannot address '${Identifiers.cap(fullPath)}': " +
                "'$containerPath' is '${container.def.type.wire}', not a struct",
        )
    }

    private fun depthOf(
        state: TableState,
        node: Column,
    ): Int = AlterService.findChain(state.cols, node.fieldId, emptyList())?.size ?: 1

    private fun replaceNode(
        siblings: List<Column>,
        fieldId: Long,
        transform: (Column) -> Column,
    ): List<Column> =
        siblings.map { c ->
            when {
                c.fieldId == fieldId -> transform(c)
                c.children.isEmpty() -> c
                else -> c.copy(children = replaceNode(c.children, fieldId, transform))
            }
        }
}
