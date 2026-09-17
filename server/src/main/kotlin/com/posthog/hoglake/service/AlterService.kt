package com.posthog.hoglake.service

import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.BoundReencode
import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.PartitionSpec
import com.posthog.hoglake.model.SortSpec
import com.posthog.hoglake.model.TableInfo
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.model.allNodes
import com.posthog.hoglake.model.boundReencodeFor
import com.posthog.hoglake.model.canPromoteTo
import com.posthog.hoglake.model.icebergType
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.FileRepo
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.persistence.NamespaceRepo
import com.posthog.hoglake.persistence.SnapshotRepo
import com.posthog.hoglake.persistence.SortRepo
import com.posthog.hoglake.persistence.SpecRepo
import com.posthog.hoglake.persistence.TableRepo
import com.posthog.hoglake.stats.IcebergSingleValue
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked

/**
 * Schema evolution: one ALTER request is one DDL commit. The whole op
 * list applies atomically in a single transaction under the per-catalog
 * commit lock, minting exactly one snapshot with exactly one
 * `table_altered` change row regardless of op count.
 *
 * Ops apply IN ORDER against the evolving table state: each op is
 * validated against the shape produced by the ops before it, so e.g.
 * `[add c2, rename c2 -> c3]` works in one request and `[drop x,
 * rename x -> y]` fails Validation on the second op.
 *
 * Versioned-row mechanics follow the schema's rule (visible at S iff
 * begin_snapshot <= S AND (end_snapshot IS NULL OR S < end_snapshot)):
 * a mutation ends the live row at the new snapshot and inserts a
 * replacement beginning there. When an op mutates a row that an earlier
 * op in the SAME request created (begin_snapshot == new snapshot), the
 * row is deleted instead of ended — it was never visible at any
 * snapshot, and end == begin would violate the schema CHECK.
 */
class AlterService(private val jdbi: Jdbi) {
    fun alterTable(
        catalog: String,
        namespace: String,
        table: String,
        ops: List<AlterOp>,
    ): TableInfo =
        Audit.audited(
            "table_alter",
            catalog,
            "$namespace.$table",
            detail = { "ops=${summarize(ops)}" },
        ) {
            if (ops.isEmpty()) {
                throw HoglakeException.Validation("ops must contain at least one operation")
            }
            jdbi.inTransactionUnchecked { h ->
                val cat =
                    CatalogRepo.findByName(h, catalog)
                        ?: throw HoglakeException.NotFound("catalog '$catalog'")
                Locks.acquireCatalogCommitLock(h, cat.catalogId)
                val ns =
                    NamespaceRepo.findLiveByName(h, cat.catalogId, namespace)
                        ?: throw HoglakeException.NotFound("namespace '$namespace' in catalog '$catalog'")
                val t =
                    TableRepo.findLive(h, cat.catalogId, ns.namespaceId, table)
                        ?: throw HoglakeException.NotFound("table '$namespace.$table' in catalog '$catalog'")

                val alloc = CatalogRepo.allocateSnapshot(h, cat.catalogId)
                SnapshotRepo.insert(h, cat.catalogId, alloc.snapshotId, alloc.schemaVersion)
                SnapshotRepo.insertChange(
                    h,
                    cat.catalogId,
                    alloc.snapshotId,
                    ChangeKind.TABLE_ALTERED,
                    t.tableId,
                )

                // Live shape as of the pre-alter head (read under the lock).
                val state =
                    TableState(
                        cols = TableRepo.columnsAt(h, cat.catalogId, t.tableId, alloc.snapshotId - 1),
                        name = t.name,
                        spec = SpecRepo.specAt(h, cat.catalogId, t.tableId, alloc.snapshotId - 1),
                        sortSpec = SortRepo.sortSpecAt(h, cat.catalogId, t.tableId, alloc.snapshotId - 1),
                    )
                for (op in ops) {
                    applyOp(h, cat.catalogId, t.tableId, ns.namespaceId, namespace, alloc.snapshotId, state, op)
                }

                val agg = FileRepo.aggregateAt(h, cat.catalogId, t.tableId, alloc.snapshotId)
                TableInfo(
                    tableId = t.tableId,
                    tableUuid = t.tableUuid,
                    namespace = ns.name,
                    name = state.name,
                    columns = state.cols.sortedBy { it.ordinal },
                    recordCount = agg.recordCount,
                    fileCount = agg.fileCount,
                    fileSizeBytes = agg.fileSizeBytes,
                    partitionSpec = state.spec,
                    sortSpec = state.sortSpec,
                )
            }
        }

    /** Audit-detail summary of an op list: op kinds with counts, in order of first appearance. */
    private fun summarize(ops: List<AlterOp>): String =
        ops.groupingBy { it::class.simpleName ?: "Op" }
            .eachCount()
            .entries
            .joinToString(",") { (kind, n) -> if (n == 1) kind else "${kind}x$n" }

    // ---- op application --------------------------------------------------

    /**
     * The evolving in-request view of the table's live shape. [cols] is
     * the column FOREST (top-level columns, each container carrying its
     * children), rebuilt rather than mutated — the tree is immutable, so
     * every op produces a new forest.
     */
    private class TableState(
        var cols: List<Column>,
        var name: String,
        var spec: PartitionSpec?,
        var sortSpec: SortSpec?,
    )

    /** A column found by dotted path, with the chain that reached it. */
    private class Located(
        val column: Column,
        /** null when [column] is top-level. */
        val parent: Column?,
    )

    private fun applyOp(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        namespaceId: Long,
        namespaceName: String,
        snapshot: Long,
        state: TableState,
        op: AlterOp,
    ) = when (op) {
        is AlterOp.AddColumn -> addColumn(h, catalogId, tableId, snapshot, state, op)
        is AlterOp.DropColumn -> dropColumn(h, catalogId, tableId, snapshot, state, op)
        is AlterOp.RenameColumn -> renameColumn(h, catalogId, tableId, snapshot, state, op)
        is AlterOp.PromoteColumn -> promoteColumn(h, catalogId, tableId, snapshot, state, op)
        is AlterOp.RenameTable ->
            renameTable(h, catalogId, tableId, namespaceId, namespaceName, snapshot, state, op)
        is AlterOp.SetPartitionSpec -> setPartitionSpec(h, catalogId, tableId, snapshot, state, op)
        is AlterOp.SetSortOrder -> setSortOrder(h, catalogId, tableId, snapshot, state, op)
    }

    /**
     * Add a column, either at top level or as a new field of an
     * existing STRUCT ([AlterOp.AddColumn.parent], a dotted path).
     *
     * The new column may itself be nested: the whole subtree is
     * validated (shape, synthetic names, depth — measured from the
     * graft point, so adding a 3-deep struct into a 6-deep one is
     * refused exactly like declaring a 9-deep column would be) and its
     * ids are allocated depth-first in one go.
     *
     * Adding into a LIST or a MAP is refused: Iceberg has no "add a
     * field to a list", and the only thing a list HAS is its element.
     */
    private fun addColumn(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
        state: TableState,
        op: AlterOp.AddColumn,
    ) {
        val parent = op.parent?.let { requireStructParent(state, it) }
        val siblings = parent?.children ?: state.cols
        // CAPPED: reached before ColumnTrees.validate, so the name has
        // not met the identifier pattern yet.
        val where = if (parent == null) "" else " of struct '${Identifiers.cap(op.parent)}'"
        if (siblings.any { it.def.name == op.def.name }) {
            throw HoglakeException.Validation(
                "column '${Identifiers.cap(op.def.name)}'$where already exists",
            )
        }
        // existingNodes makes the node cap a cap on the POST-GRAFT
        // total. Capping the addition alone would be no cap at all: the
        // caller simply adds again.
        ColumnTrees.validate(
            listOf(op.def),
            depthOffset = parent?.let { depthOf(state, it) } ?: 0,
            existingNodes = state.cols.allNodes().size,
        )
        val count = ColumnTrees.nodeCount(listOf(op.def))
        val firstFieldId = TableRepo.allocateFieldIds(h, catalogId, tableId, count)
        val assigned = ColumnTrees.assignFieldIds(listOf(op.def), firstFieldId).single()
        val col = assigned.copy(ordinal = (siblings.maxOfOrNull { it.ordinal } ?: -1) + 1)
        TableRepo.insertColumns(h, catalogId, tableId, snapshot, listOf(col), parent?.fieldId)
        state.cols =
            if (parent == null) {
                state.cols + col
            } else {
                replaceNode(state.cols, parent.fieldId) { it.copy(children = it.children + col) }
            }
    }

    /**
     * Drop a column — top-level, or a field of a struct by dotted path.
     * The whole SUBTREE goes: a dropped struct takes its fields with it,
     * and each of their versioned rows is retired at [snapshot].
     *
     * A field of a list or a map cannot be dropped (there is nothing
     * left of a list without its element); the path resolver refuses to
     * address one at all.
     */
    private fun dropColumn(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
        state: TableState,
        op: AlterOp.DropColumn,
    ) {
        val located = requireColumn(state, op.name)
        val col = located.column
        val parent = located.parent
        if (parent == null && state.cols.size == 1) {
            throw HoglakeException.Validation(
                "cannot drop '${Identifiers.cap(op.name)}': it is the last column",
            )
        }
        if (parent != null && parent.children.size == 1) {
            throw HoglakeException.Validation(
                "cannot drop '${op.name}': it is the last field of struct '${parent.def.name}', " +
                    "and a struct with no fields has no representation in parquet or Iceberg",
            )
        }
        // The whole subtree's ids, not just this node's: a partition or
        // sort source hiding inside the dropped struct is exactly as
        // fatal as the struct itself being one.
        val doomed = col.selfAndDescendants().map { it.fieldId }.toSet()
        val spec = state.spec
        spec?.fields?.firstOrNull { it.sourceFieldId in doomed }?.let { f ->
            throw HoglakeException.Validation(
                dropBlockedMessage(op.name, col, f.sourceFieldId, "partition spec"),
            )
        }
        val sortSpec = state.sortSpec
        sortSpec?.fields?.firstOrNull { it.sourceFieldId in doomed }?.let { f ->
            throw HoglakeException.Validation(
                dropBlockedMessage(op.name, col, f.sourceFieldId, "sort order"),
            )
        }
        for (id in doomed) endOrDeleteColumnRow(h, catalogId, tableId, id, snapshot)
        state.cols =
            if (parent == null) {
                state.cols.filter { it.fieldId != col.fieldId }
            } else {
                replaceNode(state.cols, parent.fieldId) { p ->
                    p.copy(children = p.children.filter { it.fieldId != col.fieldId })
                }
            }
    }

    private fun dropBlockedMessage(
        path: String,
        dropped: Column,
        sourceFieldId: Long,
        what: String,
    ): String =
        if (sourceFieldId == dropped.fieldId) {
            "cannot drop column '${Identifiers.cap(path)}': it is a source of the live $what"
        } else {
            "cannot drop column '${Identifiers.cap(path)}': field_id $sourceFieldId inside it " +
                "is a source of the live $what"
        }

    private fun renameColumn(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
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
        // The field-id contract guard: files whose parquet schema carries
        // no field ids bind columns by NAME. Renaming while any such file
        // is live (visible at head) would silently NULL that column's
        // history in readers, so the rename is refused (409) until the
        // id-less files are compacted, expired, or otherwise retired.
        // 'pending' files count too — missing_field_ids is only written by
        // the hydrator's footer read, so a not-yet-hydrated file's id state
        // is UNKNOWN and must be treated as id-less until proven otherwise
        // (the TOCTOU: commit deferred-stats id-less file -> rename slips
        // through before the sweep -> the flag arrives too late).
        // RenameTable is unaffected — table binding rides table_uuid.
        val (idlessLive, pendingLive) =
            h.createQuery(
                """
                SELECT count(*) FILTER (WHERE missing_field_ids) AS idless,
                       count(*) FILTER (WHERE stats_state = 'pending' AND NOT missing_field_ids) AS pending
                FROM hog_data_file
                WHERE catalog_id = :catalogId AND table_id = :tableId
                  AND end_snapshot IS NULL
                  AND (missing_field_ids OR stats_state = 'pending')
                """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .map { rs, _ -> rs.getLong("idless") to rs.getLong("pending") }
                .one()
        if (idlessLive > 0 || pendingLive > 0) {
            val blockers =
                buildList {
                    if (idlessLive > 0) add("$idlessLive id-less")
                    if (pendingLive > 0) {
                        add(
                            "$pendingLive not-yet-hydrated (id state unknown until the footer is read)",
                        )
                    }
                }.joinToString(" and ")
            throw HoglakeException.IdlessFilesPresent(
                "cannot rename column '${op.from}' to '${op.to}': $blockers live data " +
                    "file(s) may bind columns by name — renaming would silently NULL " +
                    "their history in readers; hydrate, rewrite, or retire them first",
            )
        }
        endOrDeleteColumnRow(h, catalogId, tableId, col.fieldId, snapshot)
        // Only THIS row is rewritten; the children keep their own live
        // rows (and their parent_field_id, which is the stable field id,
        // not the version) — so renaming a struct does not churn its
        // fields' history.
        val renamed = col.copy(def = col.def.copy(name = op.to))
        TableRepo.insertColumns(
            h,
            catalogId,
            tableId,
            snapshot,
            listOf(renamed.copy(children = emptyList())),
            located.parent?.fieldId,
        )
        state.cols = replaceNode(state.cols, col.fieldId) { renamed }
    }

    private fun promoteColumn(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
        state: TableState,
        op: AlterOp.PromoteColumn,
    ) {
        val located = requireColumn(state, op.name)
        val col = located.column
        // Containers first, with their own reason: "cannot promote list
        // to long" is true but unhelpful, and the honest answer is that
        // a container has no promotion at all — not that this particular
        // target is wrong.
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
        endOrDeleteColumnRow(h, catalogId, tableId, col.fieldId, snapshot)
        val promoted = col.copy(def = col.def.copy(type = op.to))
        TableRepo.insertColumns(
            h,
            catalogId,
            tableId,
            snapshot,
            listOf(promoted.copy(children = emptyList())),
            located.parent?.fieldId,
        )
        state.cols = replaceNode(state.cols, col.fieldId) { promoted }
        // Field-id-keyed, so a struct LEAF re-encodes exactly like a
        // top-level column: nothing about the stats path knows or cares
        // that the column lives inside a struct.
        reencodeStatsOnPromote(h, catalogId, tableId, col.fieldId, col.def.type, op.to)
    }

    /**
     * Same-transaction stats re-encode for a width-changing promote:
     * existing hog_file_column_stats bounds for the column were written
     * in the OLD type's 4-byte Iceberg encoding; readers and compaction's
     * bound-merge decode bounds under the LIVE type (8 bytes after
     * int->long / float->double), so stale-width rows would either fail
     * decoding forever (the poison-group compaction wedge) or be skipped.
     * Values are preserved exactly — both promotions are lossless widens.
     *
     * Caveat, stated: a time-travel reader decoding these bounds at a
     * PRE-promote snapshot (column type still int/float there) sees
     * 8-byte encodings. Bounds are advisory pruning metadata, and the
     * head-correctness + never-wedge-compaction trade wins; clients that
     * cannot decode a bound must treat it as absent (the "NULL, never
     * guessed" contract's read-side dual).
     *
     * Bounded work: only this column's rows with 4-byte bounds, under the
     * catalog lock — promote is rare DDL. Rows hydrated concurrently under
     * the old type can still slip in AFTER this (hydrator race);
     * compaction's bound-merge skips undecodable widths as the backstop.
     *
     * Which promotions need this falls out of the facade mapping rather
     * than a list: bounds are stored in the MAPPED Iceberg type's
     * encoding, so a promotion re-encodes iff the mapped type changes
     * ([boundReencodeFor]). Over the matrix as it actually stands:
     *
     *  - FREE, both sides map to Iceberg int: int8 -> int16, int8 -> int,
     *    int16 -> int, uint8 -> uint16.
     *  - WIDENS 4 bytes to 8, int -> long: int8/int16/int -> long, and
     *    uint8/uint16 -> uint32 — note that last pair, where neither type
     *    name says "long" and a rule written over names would miss it.
     *  - WIDENS 4 to 8, float -> double: float -> double.
     *
     * Nothing else is promotable, so nothing else can reach here.
     */
    private fun reencodeStatsOnPromote(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        fieldId: Long,
        from: ColType,
        to: ColType,
    ) {
        val widen: (ByteArray) -> ByteArray =
            when (boundReencodeFor(from, to)) {
                BoundReencode.NONE -> return
                BoundReencode.INT_TO_LONG -> ::widenIntToLong
                BoundReencode.FLOAT_TO_DOUBLE -> ::widenFloatToDouble
            }

        data class Stale(val dataFileId: Long, val lower: ByteArray?, val upper: ByteArray?)

        val stale =
            h.createQuery(
                """
                SELECT s.data_file_id, s.lower_bound, s.upper_bound
                FROM hog_file_column_stats s
                JOIN hog_data_file f
                  ON f.catalog_id = s.catalog_id AND f.data_file_id = s.data_file_id
                WHERE s.catalog_id = :catalogId AND f.table_id = :tableId
                  AND s.field_id = :fieldId
                  AND (octet_length(s.lower_bound) = 4 OR octet_length(s.upper_bound) = 4)
                """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("fieldId", fieldId)
                .map {
                        rs,
                        _,
                    ->
                    Stale(rs.getLong("data_file_id"), rs.getBytes("lower_bound"), rs.getBytes("upper_bound"))
                }
                .list()
        if (stale.isEmpty()) return
        val batch =
            h.prepareBatch(
                """
                UPDATE hog_file_column_stats
                   SET lower_bound = :lower, upper_bound = :upper
                 WHERE catalog_id = :catalogId AND data_file_id = :dataFileId AND field_id = :fieldId
                """,
            )
        for (row in stale) {
            batch
                .bind("lower", row.lower?.let { if (it.size == 4) widen(it) else it })
                .bind("upper", row.upper?.let { if (it.size == 4) widen(it) else it })
                .bind("catalogId", catalogId)
                .bind("dataFileId", row.dataFileId)
                .bind("fieldId", fieldId)
                .add()
        }
        batch.execute()
    }

    /** Lossless width promotion of one 4-byte int bound to the long encoding. */
    private fun widenIntToLong(b: ByteArray): ByteArray =
        IcebergSingleValue.encode(ColType.LONG, (IcebergSingleValue.decode(ColType.INT, b) as Int).toLong())

    /** Lossless width promotion of one 4-byte float bound to the double encoding. */
    private fun widenFloatToDouble(b: ByteArray): ByteArray =
        IcebergSingleValue.encode(ColType.DOUBLE, (IcebergSingleValue.decode(ColType.FLOAT, b) as Float).toDouble())

    private fun renameTable(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        namespaceId: Long,
        namespaceName: String,
        snapshot: Long,
        state: TableState,
        op: AlterOp.RenameTable,
    ) {
        Identifiers.validate("table", op.newName)
        val existing = TableRepo.findLive(h, catalogId, namespaceId, op.newName)
        if (existing != null && existing.tableId != tableId) {
            throw HoglakeException.AlreadyExists(
                "table '${op.newName}' already exists in namespace '$namespaceName'",
            )
        }
        endOrDeleteVersionRow(h, catalogId, tableId, snapshot)
        TableRepo.insertVersion(h, catalogId, tableId, snapshot, namespaceId, op.newName)
        state.name = op.newName
    }

    private fun setPartitionSpec(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
        state: TableState,
        op: AlterOp.SetPartitionSpec,
    ) {
        for (f in op.fields) {
            val col = requireSourceField(state, f.sourceFieldId, "partition")
            when (f.transform) {
                Transform.BUCKET -> {
                    if (f.transformParam == null || f.transformParam < 1) {
                        throw HoglakeException.Validation(
                            "bucket transform requires transform_param >= 1",
                        )
                    }
                    if (col.def.type !in BUCKETABLE_TYPES) {
                        // Name the actual reason: "not bucketable" sends a
                        // caller looking for a syntax error, when the answer
                        // is that nobody has specified how to hash this type.
                        val why =
                            when (col.def.type) {
                                in HASH_DOMAIN_MISMATCHED ->
                                    "Iceberg hashes the MAPPED type's representation " +
                                        "('${col.def.type.wire}' maps to " +
                                        "'${col.def.type.icebergType.wire}'), and hoglake has no " +
                                        "cross-language contract for that hash yet"
                                // NOT "the spec excludes it" — Iceberg buckets
                                // strings, and json maps to string. The problem
                                // is that JSON has no canonical byte form.
                                ColType.JSON ->
                                    "the bucket hash runs over bytes, and documents that are equal " +
                                        "as JSON can have different bytes (key order, whitespace, " +
                                        "number spelling), so bucket assignment would depend on " +
                                        "which writer serialized the value rather than on the value"
                                else -> "the Iceberg spec excludes it from the bucket hash domain"
                            }
                        throw HoglakeException.Validation(
                            "bucket transform cannot be applied to column '${col.def.name}' of type " +
                                "'${col.def.type.wire}': $why; bucketable types are " +
                                "${BUCKETABLE_TYPES.map { it.wire }.sorted()}",
                        )
                    }
                }
                else ->
                    if (f.transformParam != null) {
                        throw HoglakeException.Validation(
                            "transform '${f.transform.wire}' does not take transform_param",
                        )
                    }
            }
            if (f.transform in TEMPORAL_TRANSFORMS && col.def.type !in TEMPORAL_TYPES) {
                throw HoglakeException.Validation(
                    "transform '${f.transform.wire}' requires a date or timestamp column; " +
                        "'${col.def.name}' is '${col.def.type.wire}'",
                )
            }
        }
        endOrDeleteSpec(h, catalogId, tableId, snapshot)
        if (op.fields.isEmpty()) {
            state.spec = null
            return
        }
        val specId =
            h.createQuery(
                """
            SELECT COALESCE(MAX(spec_id), 0) + 1 FROM hog_partition_spec
            WHERE catalog_id = :catalogId AND table_id = :tableId
            """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .mapTo(Long::class.javaObjectType)
                .one()
        h.createUpdate(
            """
            INSERT INTO hog_partition_spec (catalog_id, table_id, spec_id, begin_snapshot)
            VALUES (:catalogId, :tableId, :specId, :beginSnapshot)
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("specId", specId)
            .bind("beginSnapshot", snapshot)
            .execute()
        val batch =
            h.prepareBatch(
                """
            INSERT INTO hog_partition_field
                (catalog_id, table_id, spec_id, key_index, source_field_id, transform, transform_param)
            VALUES (:catalogId, :tableId, :specId, :keyIndex, :sourceFieldId, :transform, :transformParam)
            """,
            )
        op.fields.forEachIndexed { i, f ->
            batch
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("specId", specId)
                .bind("keyIndex", i)
                .bind("sourceFieldId", f.sourceFieldId)
                .bind("transform", f.transform.wire)
                .bind("transformParam", f.transformParam)
                .add()
        }
        batch.execute()
        state.spec = PartitionSpec(specId, op.fields)
    }

    /**
     * SetPartitionSpec's twin for sort orders: validate every source
     * field against the post-ops live columns, retire the live spec,
     * mint sort_id = max + 1 with the new fields (empty = unsorted).
     * Advisory for writers, binding for compaction rewrites.
     */
    private fun setSortOrder(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
        state: TableState,
        op: AlterOp.SetSortOrder,
    ) {
        val seen = HashSet<Long>()
        for (f in op.fields) {
            requireSourceField(state, f.sourceFieldId, "sort")
            if (!seen.add(f.sourceFieldId)) {
                throw HoglakeException.Validation(
                    "duplicate sort source field_id ${f.sourceFieldId}",
                )
            }
        }
        endOrDeleteSortSpec(h, catalogId, tableId, snapshot)
        if (op.fields.isEmpty()) {
            state.sortSpec = null
            return
        }
        val sortId =
            h.createQuery(
                """
            SELECT COALESCE(MAX(sort_id), 0) + 1 FROM hog_sort_spec
            WHERE catalog_id = :catalogId AND table_id = :tableId
            """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .mapTo(Long::class.javaObjectType)
                .one()
        h.createUpdate(
            """
            INSERT INTO hog_sort_spec (catalog_id, table_id, sort_id, begin_snapshot)
            VALUES (:catalogId, :tableId, :sortId, :beginSnapshot)
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("sortId", sortId)
            .bind("beginSnapshot", snapshot)
            .execute()
        val batch =
            h.prepareBatch(
                """
            INSERT INTO hog_sort_field
                (catalog_id, table_id, sort_id, key_index, source_field_id, direction, null_order)
            VALUES (:catalogId, :tableId, :sortId, :keyIndex, :sourceFieldId, :direction, :nullOrder)
            """,
            )
        op.fields.forEachIndexed { i, f ->
            batch
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("sortId", sortId)
                .bind("keyIndex", i)
                .bind("sourceFieldId", f.sourceFieldId)
                .bind("direction", f.direction.wire)
                .bind("nullOrder", f.nullOrder.wire)
                .add()
        }
        batch.execute()
        state.sortSpec = SortSpec(sortId, op.fields)
    }

    // ---- row lifecycle helpers -------------------------------------------

    /**
     * Resolve a column by DOTTED PATH — `addr` for a top-level column,
     * `addr.zip` for a field of the struct `addr`. Names cannot contain
     * `.` (the identifier policy), so the split is unambiguous.
     *
     * Only STRUCT interiors are addressable. A path that steps through a
     * `list` or a `map` is refused by name, because Iceberg has no
     * add/drop/rename for an element, a key or a value — the container's
     * shape IS its type, and changing it would be a type change, not a
     * column op.
     */
    private fun requireColumn(
        state: TableState,
        path: String,
    ): Located {
        // CAPPED throughout. A column PATH is caller input that never
        // meets Identifiers.validate — its segments are matched against
        // stored names, not vetted — so nothing bounds its length before
        // these messages quote it.
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
            // singleOrNull, not find: the DDL path refuses duplicate
            // sibling names, but the DATABASE only enforces unique
            // (parent, ordinal) — not (parent, name). A catalog that
            // acquired two siblings called the same thing (a hand-edited
            // row, a restored dump) would otherwise have alter ops act on
            // whichever came first, silently, forever. Belt and
            // suspenders, and loud.
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
                                "'${segments.take(i).joinToString(".")}' has no field '$segment'"
                        },
                    )
        }
        return Located(current!!, parent)
    }

    /** Resolve a dotted path that must name an existing struct (add_column's `parent`). */
    private fun requireStructParent(
        state: TableState,
        path: String,
    ): Column {
        val located = requireColumn(state, path)
        assertStructInterior(located.column, path, path)
        return located.column
    }

    /** [container] must be a struct for [path] to be addressable inside it. */
    private fun assertStructInterior(
        container: Column,
        containerPath: String,
        fullPath: String,
    ) {
        if (container.def.type == ColType.STRUCT) return
        if (container.def.type.isNested) {
            throw HoglakeException.Validation(
                "cannot address '${Identifiers.cap(fullPath)}': " +
                    "'${Identifiers.cap(containerPath)}' is a '${container.def.type.wire}', " +
                    "and list/map internals (element, key, value) cannot be added, dropped or " +
                    "renamed — only struct fields can",
            )
        }
        throw HoglakeException.Validation(
            "cannot address '${Identifiers.cap(fullPath)}': " +
                "'${Identifiers.cap(containerPath)}' is '${container.def.type.wire}', not a struct",
        )
    }

    /**
     * A partition or sort source must be a LEAF that no list or map
     * sits above. Iceberg's `source-id` may point at a struct leaf, so
     * `addr.zip` is a legal partition source — but nothing under a
     * repeated element is, because a row has many of those values and a
     * partition/sort key is one value per row.
     */
    private fun requireSourceField(
        state: TableState,
        fieldId: Long,
        what: String,
    ): Column {
        val chain =
            findChain(state.cols, fieldId, emptyList())
                ?: throw HoglakeException.Validation(
                    "$what source field_id $fieldId is not a live column",
                )
        val col = chain.last()
        val path = chain.joinToString(".") { it.def.name }
        if (col.def.type.isNested) {
            throw HoglakeException.Validation(
                "$what source field_id $fieldId ('${Identifiers.cap(path)}') is a " +
                    "'${col.def.type.wire}': a nested " +
                    "container has no single value per row and cannot be a $what source; use one " +
                    "of its leaf fields",
            )
        }
        val repeated = chain.dropLast(1).firstOrNull { it.def.type == ColType.LIST || it.def.type == ColType.MAP }
        if (repeated != null) {
            throw HoglakeException.Validation(
                "$what source field_id $fieldId ('${Identifiers.cap(path)}') sits under " +
                    "'${repeated.def.name}', a " +
                    "'${repeated.def.type.wire}': a row has many such values, so it cannot be a " +
                    "$what source; struct leaves are the only nested fields that can",
            )
        }
        return col
    }

    /** The root-to-node chain for [fieldId], or null when it is not in the forest. */
    private fun findChain(
        siblings: List<Column>,
        fieldId: Long,
        prefix: List<Column>,
    ): List<Column>? {
        for (c in siblings) {
            val here = prefix + c
            if (c.fieldId == fieldId) return here
            findChain(c.children, fieldId, here)?.let { return it }
        }
        return null
    }

    /** Depth of [node] in [state]'s forest, top-level counting as 1. */
    private fun depthOf(
        state: TableState,
        node: Column,
    ): Int = findChain(state.cols, node.fieldId, emptyList())?.size ?: 1

    /** Rebuild the forest with [transform] applied to the node carrying [fieldId]. */
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

    /**
     * Retire a field's live column row at [snapshot]: delete it if this
     * request created it (begin_snapshot == snapshot — never visible),
     * else end-snapshot it.
     */
    private fun endOrDeleteColumnRow(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        fieldId: Long,
        snapshot: Long,
    ) {
        val deleted =
            h.createUpdate(
                """
            DELETE FROM hog_column
            WHERE catalog_id = :catalogId AND table_id = :tableId
              AND field_id = :fieldId AND begin_snapshot = :snapshot
            """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("fieldId", fieldId)
                .bind("snapshot", snapshot)
                .execute()
        if (deleted == 0) {
            h.createUpdate(
                """
                UPDATE hog_column SET end_snapshot = :snapshot
                WHERE catalog_id = :catalogId AND table_id = :tableId
                  AND field_id = :fieldId AND end_snapshot IS NULL
                """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("fieldId", fieldId)
                .bind("snapshot", snapshot)
                .execute()
        }
    }

    /** Same delete-if-created-here-else-end rule for hog_table_version. */
    private fun endOrDeleteVersionRow(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
    ) {
        val deleted =
            h.createUpdate(
                """
            DELETE FROM hog_table_version
            WHERE catalog_id = :catalogId AND table_id = :tableId AND begin_snapshot = :snapshot
            """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("snapshot", snapshot)
                .execute()
        if (deleted == 0) {
            h.createUpdate(
                """
                UPDATE hog_table_version SET end_snapshot = :snapshot
                WHERE catalog_id = :catalogId AND table_id = :tableId AND end_snapshot IS NULL
                """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("snapshot", snapshot)
                .execute()
        }
    }

    /**
     * Retire the live partition spec (if any): delete it (fields cascade)
     * when this request created it, else end-snapshot it.
     */
    private fun endOrDeleteSpec(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
    ) {
        val deleted =
            h.createUpdate(
                """
            DELETE FROM hog_partition_spec
            WHERE catalog_id = :catalogId AND table_id = :tableId
              AND end_snapshot IS NULL AND begin_snapshot = :snapshot
            """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("snapshot", snapshot)
                .execute()
        if (deleted == 0) {
            h.createUpdate(
                """
                UPDATE hog_partition_spec SET end_snapshot = :snapshot
                WHERE catalog_id = :catalogId AND table_id = :tableId AND end_snapshot IS NULL
                """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("snapshot", snapshot)
                .execute()
        }
    }

    /** Same delete-if-created-here-else-end rule for the sort spec (fields cascade). */
    private fun endOrDeleteSortSpec(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
    ) {
        val deleted =
            h.createUpdate(
                """
            DELETE FROM hog_sort_spec
            WHERE catalog_id = :catalogId AND table_id = :tableId
              AND end_snapshot IS NULL AND begin_snapshot = :snapshot
            """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("snapshot", snapshot)
                .execute()
        if (deleted == 0) {
            h.createUpdate(
                """
                UPDATE hog_sort_spec SET end_snapshot = :snapshot
                WHERE catalog_id = :catalogId AND table_id = :tableId AND end_snapshot IS NULL
                """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("snapshot", snapshot)
                .execute()
        }
    }

    /**
     * Not private: BUCKETABLE_TYPES is a contract the tests assert by
     * CONTENTS (so adding a type is a conscious act) and that
     * pyhoglake's parity test compares against its own allowlist.
     */
    companion object {
        val TEMPORAL_TRANSFORMS =
            setOf(Transform.YEAR, Transform.MONTH, Transform.DAY, Transform.HOUR)

        /**
         * Sources year/month/day/hour accept. Every timestamp precision
         * qualifies: the transforms are epoch-relative integers, which
         * the declared precision does not change. `hour` on a `date`
         * column is accepted here for continuity with the pre-parity
         * behaviour, even though Iceberg does not define it — tightening
         * that is a separate, breaking change, not a type-parity one.
         */
        val TEMPORAL_TYPES =
            setOf(
                ColType.DATE,
                ColType.TIMESTAMP_S,
                ColType.TIMESTAMP_MS,
                ColType.TIMESTAMP,
                ColType.TIMESTAMP_NS,
                ColType.TIMESTAMPTZ,
            )

        /**
         * Sources bucket(n) accepts — an explicit ALLOWLIST, not the
         * complement of an exclusion list.
         *
         * That shape is the point. Written as `entries - excluded`, every
         * ColType added in future became bucketable by default, and the
         * only thing standing between a new type and an unverified hash
         * contract was someone remembering to add a line. The policy is
         * admit-deliberately (iceberg-federation.md §3), so the code has
         * to fail closed: a new member is NOT bucketable until it is
         * named here, and naming it means having decided how it hashes.
         * pyhoglake's `_BUCKETABLE` is already a positive list, and the
         * two must stay set-equal (test_transforms.py parses this
         * declaration); the client is where bucket values are actually
         * computed, so a divergence means the server accepting a spec the
         * writer cannot honour.
         *
         * What is deliberately absent, and why:
         *
         *  - boolean/float/double: outside the Iceberg spec's Appendix-B
         *    hash domain outright.
         *  - json: two documents that are equal AS JSON (key order,
         *    whitespace, number spelling, unicode escaping) have
         *    different bytes, and the hash runs over bytes — so bucket
         *    assignment depends on which writer serialized the value,
         *    not on the value. Equal data would scatter across
         *    partitions and prune wrong. Iceberg buckets strings
         *    perfectly well; it is JSON's lack of a canonical byte form
         *    that makes this unsafe.
         *  - uint32/uint64/timestamp_s/timestamp_ms/timestamp_ns: the
         *    hash-domain mismatch. Appendix B hashes the MAPPED type's
         *    representation — timestamps as micros, uint64-as-decimal as
         *    minimal two's-complement bytes, uint32-as-long as the
         *    zero-extended value — and the server never computes bucket
         *    values, it only accepts the strings clients send. Until
         *    there is a client contract for hashing on the mapped value
         *    AND cross-language bucket vectors proving both sides agree,
         *    accepting a bucket spec on these would be accepting
         *    partition values nobody has verified. Identity and truncate
         *    only; re-admitting them is a deliberate future change with
         *    those vectors attached, not a default.
         */
        val BUCKETABLE_TYPES =
            setOf(
                ColType.INT8,
                ColType.INT16,
                ColType.INT,
                ColType.LONG,
                ColType.UINT8,
                ColType.UINT16,
                ColType.DECIMAL,
                ColType.DATE,
                ColType.TIME,
                ColType.TIMESTAMP,
                ColType.TIMESTAMPTZ,
                ColType.STRING,
                ColType.UUID_T,
                ColType.BINARY,
            )

        /** Types whose bucket refusal is about the mapped type's hash domain. */
        val HASH_DOMAIN_MISMATCHED =
            setOf(
                ColType.UINT32,
                ColType.UINT64,
                ColType.TIMESTAMP_S,
                ColType.TIMESTAMP_MS,
                ColType.TIMESTAMP_NS,
            )
    }
}
