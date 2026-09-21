package com.posthog.hoglake.service

import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.ChangesPlan
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitResult
import com.posthog.hoglake.model.ConsumerOffset
import com.posthog.hoglake.model.DataFile
import com.posthog.hoglake.model.FileColumnStats
import com.posthog.hoglake.model.FileOrderingBounds
import com.posthog.hoglake.model.FileStats
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceInfo
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.Snapshot
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.model.StatsState
import com.posthog.hoglake.model.TableInfo
import com.posthog.hoglake.model.initialColumns
import com.posthog.hoglake.model.nodeCount
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.DeleteFileReadRepo
import com.posthog.hoglake.persistence.FileRepo
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.persistence.NamespaceRepo
import com.posthog.hoglake.persistence.OffsetRepo
import com.posthog.hoglake.persistence.SnapshotRepo
import com.posthog.hoglake.persistence.SortRepo
import com.posthog.hoglake.persistence.SpecRepo
import com.posthog.hoglake.persistence.TableRepo
import com.posthog.hoglake.persistence.TimeTravelRepo
import com.posthog.hoglake.persistence.ViewRepo
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import java.time.Instant
import java.util.UUID

/**
 * Catalog DDL + read paths. Every DDL operation is one transaction that
 * (1) takes the per-catalog commit lock, (2) allocates the next
 * snapshot id / schema version off hog_catalog, (3) records the
 * snapshot + typed change row, then (4) writes the object rows. The
 * append-commit tail (commit/) follows the same shape.
 *
 * Reads honor the versioned-row rule: visible at S iff
 * begin_snapshot <= S AND (end_snapshot IS NULL OR S < end_snapshot).
 */
class CatalogService(private val jdbi: Jdbi) {
    companion object {
        /**
         * Ceiling on a catalog's `data_path`.
         *
         * S3 keys stop at 1024 bytes and a data_path is only the PREFIX
         * under which keys are built, so anything near this is already
         * unusable — the bound exists because nothing else provided one
         * (the column is `text` with no CHECK, the OpenAPI schema is a
         * bare string), and an unbounded value registered once is echoed
         * by every overlap refusal afterwards.
         */
        const val MAX_DATA_PATH_LENGTH = 512
    }

    // ---- catalogs --------------------------------------------------------

    fun createCatalog(
        name: String,
        dataPath: String,
    ): CatalogInfo =
        Audit.audited("catalog_create", name, null, detail = { "data_path=$dataPath" }) {
            validateDataPath(dataPath)
            jdbi.inTransactionUnchecked { h ->
                // data_path prefix-overlap with ANY existing catalog is
                // refused, both directions: cleanup's drain-time liveness
                // check is per-catalog, so two catalogs sharing a prefix
                // let catalog A's drain physically delete an object
                // catalog B still references (invariant 4, made global
                // here at the only place overlap can be introduced).
                // Runs inside the insert transaction; the hog_catalog
                // name-unique insert serializes concurrent creates enough
                // for this dev-era check.
                val newPrefix = dataPath.trimEnd('/') + "/"
                CatalogRepo.listAll(h).forEach { existing ->
                    // Same-name recreate falls through to the insert's
                    // unique constraint -> AlreadyExists (409), the
                    // canonical duplicate answer; overlap 422s are for
                    // OTHER catalogs' prefixes.
                    if (existing.name == name) return@forEach
                    val theirPrefix = existing.dataPath.trimEnd('/') + "/"
                    if (newPrefix.startsWith(theirPrefix) || theirPrefix.startsWith(newPrefix)) {
                        throw HoglakeException.Validation(
                            "data_path '${Identifiers.cap(dataPath)}' overlaps catalog " +
                                "'${existing.name}' (data_path " +
                                "'${Identifiers.cap(existing.dataPath)}'); " +
                                "catalog data_paths must be disjoint",
                        )
                    }
                }
                val info = CatalogRepo.insert(h, name, dataPath)
                // Snapshot 0: the empty catalog at schema_version 0, no changes.
                SnapshotRepo.insert(h, info.catalogId, 0, 0)
                info
            }
        }

    /**
     * data_path shape: `s3://<bucket>[/<key-prefix>]` with a non-empty
     * bucket, no whitespace/control chars, no dot segments. The
     * commit-time path guard is a prefix comparison against this
     * value, so a degenerate data_path is a guard bypass: `s3://`
     * normalizes to a prefix every s3 URI starts with. A bucket-ROOT
     * data_path is legal — it is the fleet convention (a catalog owns
     * its bucket); the overlap check above keeps other catalogs off
     * it. Non-s3 schemes are refused because the hydrator/cleanup
     * object store only speaks s3 — a catalog with an unparseable
     * data_path poisons the removal queue (rows retry forever).
     */
    private fun validateDataPath(dataPath: String) {
        if (dataPath.isBlank()) throw HoglakeException.Validation("data_path must not be blank")
        // A LENGTH BOUND, which nothing provided: the column is `text`
        // with no CHECK, validateDataPath tested shape but never size,
        // and the OpenAPI schema is a bare string. A 100 KB data_path
        // registered fine and then rode into every overlap 422 any later
        // catalog triggered — capping the echo only shortens the
        // message, it does not stop the value being stored.
        if (dataPath.length > MAX_DATA_PATH_LENGTH) {
            throw HoglakeException.Validation(
                "data_path is ${dataPath.length} characters, over the maximum $MAX_DATA_PATH_LENGTH",
            )
        }
        if (dataPath.any { it.isWhitespace() || it.isISOControl() }) {
            throw HoglakeException.Validation("data_path must not contain whitespace or control characters")
        }
        val rest =
            dataPath.removePrefix("s3://").takeIf { it != dataPath }
                ?: throw HoglakeException.Validation("data_path must be an s3://<bucket>[/<prefix>] URI")
        val bucket = rest.substringBefore('/')
        if (bucket.isEmpty()) {
            throw HoglakeException.Validation(
                "data_path must be s3://<bucket>[/<prefix>] with a non-empty bucket, " +
                    "got '${Identifiers.cap(dataPath)}'",
            )
        }
        if (rest.split('/').any { it == "." || it == ".." }) {
            throw HoglakeException.Validation("data_path must not contain '.' or '..' segments")
        }
    }

    fun getCatalog(name: String): CatalogInfo = jdbi.withHandleUnchecked { h -> requireCatalog(h, name) }

    fun listCatalogs(): List<CatalogInfo> = jdbi.withHandleUnchecked { h -> CatalogRepo.listAll(h) }

    // ---- namespaces ------------------------------------------------------

    fun createNamespace(
        catalog: String,
        name: String,
    ): NamespaceInfo =
        Audit.audited(
            "namespace_create",
            catalog,
            name,
        ) {
            Identifiers.validate("namespace", name)
            jdbi.inTransactionUnchecked { h ->
                val cat = requireCatalog(h, catalog)
                Locks.acquireCatalogCommitLock(h, cat.catalogId)
                if (NamespaceRepo.findLiveByName(h, cat.catalogId, name) != null) {
                    throw HoglakeException.AlreadyExists("namespace '$name' already exists")
                }
                val alloc = CatalogRepo.allocateSnapshot(h, cat.catalogId)
                SnapshotRepo.insert(h, cat.catalogId, alloc.snapshotId, alloc.schemaVersion)
                val namespaceId = CatalogRepo.allocateNamespaceId(h, cat.catalogId)
                SnapshotRepo.insertChange(
                    h,
                    cat.catalogId,
                    alloc.snapshotId,
                    ChangeKind.NAMESPACE_CREATED,
                    namespaceId,
                )
                NamespaceRepo.insert(h, cat.catalogId, namespaceId, name)
            }
        }

    fun listNamespaces(catalog: String): List<NamespaceInfo> =
        jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            NamespaceRepo.listLive(h, cat.catalogId)
        }

    fun getNamespace(
        catalog: String,
        namespace: String,
    ): NamespaceInfo =
        jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            requireNamespace(h, cat, namespace)
        }

    /**
     * Drop a namespace. Emptiness precondition, no CASCADE — matching
     * dropTable's no-CASCADE position: a namespace with live tables or
     * views is a 409, never a recursive delete. One transaction taking the
     * per-catalog commit lock, allocating a snapshot, recording the
     * namespace_dropped change, and setting the liveness flag; the row
     * stays (its id is not reused), so a same-named namespace created
     * later is a new id, not a resurrection.
     */
    fun dropNamespace(
        catalog: String,
        namespace: String,
        expectedNamespaceId: Long? = null,
    ): CommitResult =
        Audit.audited(
            "namespace_drop",
            catalog,
            namespace,
            detail = { "snapshot=${it.snapshotId}" },
        ) {
            jdbi.inTransactionUnchecked { h ->
                val cat = requireCatalog(h, catalog)
                Locks.acquireCatalogCommitLock(h, cat.catalogId)
                val ns = requireNamespace(h, cat, namespace)
                if (expectedNamespaceId != null && ns.namespaceId != expectedNamespaceId) {
                    throw HoglakeException.CommitConflict("namespace '$namespace' no longer has the expected identity")
                }
                // Emptiness under the commit lock: a concurrent createTable
                // or createView into this namespace serializes behind the
                // same lock, so the live sets read here are the live sets
                // the flag commits against.
                val liveTables = TableRepo.listLive(h, cat.catalogId, ns.namespaceId)
                val liveViews = ViewRepo.listLive(h, cat.catalogId, ns.namespaceId, ns.name)
                if (liveTables.isNotEmpty() || liveViews.isNotEmpty()) {
                    val what =
                        buildList {
                            if (liveTables.isNotEmpty()) add("${liveTables.size} table(s)")
                            if (liveViews.isNotEmpty()) add("${liveViews.size} view(s)")
                        }.joinToString(" and ")
                    throw HoglakeException.NamespaceNotEmpty(
                        "namespace '$namespace' in catalog '$catalog' is not empty: $what remain",
                    )
                }
                val alloc = CatalogRepo.allocateSnapshot(h, cat.catalogId)
                SnapshotRepo.insert(h, cat.catalogId, alloc.snapshotId, alloc.schemaVersion)
                SnapshotRepo.insertChange(
                    h,
                    cat.catalogId,
                    alloc.snapshotId,
                    ChangeKind.NAMESPACE_DROPPED,
                    ns.namespaceId,
                )
                NamespaceRepo.markDropped(h, cat.catalogId, ns.namespaceId)
                CommitResult(snapshotId = alloc.snapshotId, schemaVersion = alloc.schemaVersion)
            }
        }

    // ---- tables ----------------------------------------------------------

    fun createTable(
        catalog: String,
        namespace: String,
        name: String,
        columns: List<ColumnDef>,
    ): TableInfo =
        Audit.audited(
            "table_create",
            catalog,
            "$namespace.$name",
            detail = { "columns=${columns.size}" },
        ) {
            jdbi.inTransactionUnchecked { h -> createTable(h, catalog, namespace, name, columns) }
        }

    /** Caller may compose creation with file registration in the same transaction. */
    internal fun createTable(
        h: Handle,
        catalog: String,
        namespace: String,
        name: String,
        columns: List<ColumnDef>,
        tableUuid: UUID = UUID.randomUUID(),
        replacementTableId: Long? = null,
        partitionFields: List<PartitionFieldDef> = emptyList(),
        sortFields: List<SortFieldDef> = emptyList(),
        comment: String? = null,
        properties: Map<String, String> = emptyMap(),
    ): TableInfo {
        TableMetadata.validateComment(comment)
        TableMetadata.validateProperties(properties)
        validateTableDefinition(name, columns)
        val cols = initialColumns(columns)
        // Publication catches definition validation and records a rejected receipt.
        // All such refusals must precede snapshot allocation or table mutation.
        AlterService(jdbi).validatePartitionFields(cols, partitionFields)
        AlterService(jdbi).validateSortFields(cols, sortFields)
        val cat = requireCatalog(h, catalog)
        Locks.acquireCatalogCommitLock(h, cat.catalogId)
        val ns = requireNamespace(h, cat, namespace)
        if (TableRepo.findLive(h, cat.catalogId, ns.namespaceId, name)?.tableId != replacementTableId) {
            throw HoglakeException.AlreadyExists(
                "table '$name' already exists in namespace '$namespace'",
            )
        }
        val alloc = CatalogRepo.allocateSnapshot(h, cat.catalogId)
        SnapshotRepo.insert(h, cat.catalogId, alloc.snapshotId, alloc.schemaVersion)
        if (replacementTableId != null) {
            SnapshotRepo.insertChange(h, cat.catalogId, alloc.snapshotId, ChangeKind.TABLE_DROPPED, replacementTableId)
            TableRepo.markDropped(h, cat.catalogId, replacementTableId, alloc.snapshotId)
            FileRepo.endLiveDeleteFiles(h, cat.catalogId, replacementTableId, alloc.snapshotId)
            FileRepo.endLiveFiles(h, cat.catalogId, replacementTableId, alloc.snapshotId)
        }
        val tableId = CatalogRepo.allocateTableId(h, cat.catalogId)
        SnapshotRepo.insertChange(
            h,
            cat.catalogId,
            alloc.snapshotId,
            ChangeKind.TABLE_CREATED,
            tableId,
        )
        if (replacementTableId != null) {
            // The new incarnation's feed must also fence consumers whose window starts
            // before publication, even after expiry removes the old name's version row.
            SnapshotRepo.insertChange(h, cat.catalogId, alloc.snapshotId, ChangeKind.TABLE_ALTERED, tableId)
            SnapshotRepo.insertChange(h, cat.catalogId, alloc.snapshotId, ChangeKind.TABLE_DELETED_FROM, tableId)
        }
        val createdUuid = TableRepo.insertTable(h, cat.catalogId, tableId, alloc.snapshotId, tableUuid)
        // nodeCount, not columns.size: a nested column needs one id per
        // NODE, not one per top-level column. Allocating by size would
        // hand back a range too short and every subtree after the first
        // container would collide with the next table's ids.
        val firstFieldId =
            TableRepo.allocateFieldIds(h, cat.catalogId, tableId, nodeCount(columns))
        check(firstFieldId == cols.first().fieldId) { "new table field allocation must start at one" }
        TableRepo.insertVersion(h, cat.catalogId, tableId, alloc.snapshotId, ns.namespaceId, name, comment, properties)
        TableRepo.insertColumns(h, cat.catalogId, tableId, alloc.snapshotId, cols)
        TableRepo.insertStatsRow(h, cat.catalogId, tableId)
        val partitionSpec =
            AlterService(
                jdbi,
            ).installPartitionSpec(h, cat.catalogId, tableId, alloc.snapshotId, cols, partitionFields)
        return TableInfo(
            tableId = tableId,
            tableUuid = createdUuid,
            comment = comment,
            properties = properties,
            namespace = ns.name,
            name = name,
            columns = cols,
            partitionSpec = partitionSpec,
            sortSpec =
                AlterService(
                    jdbi,
                ).installSortSpec(h, cat.catalogId, tableId, alloc.snapshotId, cols, sortFields),
            recordCount = 0,
            fileCount = 0,
            fileSizeBytes = 0,
            snapshotId = alloc.snapshotId,
        )
    }

    internal fun validateTableDefinition(
        name: String,
        columns: List<ColumnDef>,
    ) {
        Identifiers.validate("table", name)
        if (columns.isEmpty()) {
            throw HoglakeException.Validation("table '$name' must have at least one column")
        }
        // Column NAMES are validated by ColumnTrees too, at every
        // nesting level rather than only this top one.
        // Nesting shape, node cap, depth cap, synthetic child names, map-key
        // requiredness, per-parent duplicate names — all of it BEFORE a
        // field id is allocated for any part of the request, and here
        // rather than in createTable so the PREPARE side of an atomic
        // creation refuses a malformed nested definition at prepare
        // time instead of at publish, when the receipt already exists.
        // (ColumnTrees subsumes the flat duplicate-name check: it
        // applies the same rule per sibling group.)
        ColumnTrees.validate(columns)
    }

    fun dropTable(
        catalog: String,
        namespace: String,
        table: String,
        expectedTableUuid: UUID? = null,
    ): CommitResult =
        Audit.audited(
            "table_drop",
            catalog,
            "$namespace.$table",
            detail = { "snapshot=${it.snapshotId}" },
        ) {
            jdbi.inTransactionUnchecked { h ->
                val cat = requireCatalog(h, catalog)
                Locks.acquireCatalogCommitLock(h, cat.catalogId)
                val ns = requireNamespace(h, cat, namespace)
                val t =
                    TableRepo.findLive(h, cat.catalogId, ns.namespaceId, table)
                        ?: throw HoglakeException.NotFound("table '$namespace.$table' in catalog '$catalog'")
                if (expectedTableUuid != null && t.tableUuid != expectedTableUuid) {
                    throw HoglakeException.CommitConflict("table '$namespace.$table' no longer has the expected UUID")
                }
                val alloc = CatalogRepo.allocateSnapshot(h, cat.catalogId)
                SnapshotRepo.insert(h, cat.catalogId, alloc.snapshotId, alloc.schemaVersion)
                SnapshotRepo.insertChange(
                    h,
                    cat.catalogId,
                    alloc.snapshotId,
                    ChangeKind.TABLE_DROPPED,
                    t.tableId,
                )
                TableRepo.markDropped(h, cat.catalogId, t.tableId, alloc.snapshotId)
                // DVs FIRST, same end-snapshot pattern as the data files: a
                // live DV left open on a dropped table would be invisible
                // to expiry's range predicates (end_snapshot IS NULL never
                // sinks below the floor), leaking the row AND the object
                // forever. End-snapshotted here, the superseded-DV
                // lifecycle reclaims it: expiry queues the path once the
                // drop snapshot falls under the retention floor.
                FileRepo.endLiveDeleteFiles(h, cat.catalogId, t.tableId, alloc.snapshotId)
                FileRepo.endLiveFiles(h, cat.catalogId, t.tableId, alloc.snapshotId)
                CommitResult(snapshotId = alloc.snapshotId, schemaVersion = alloc.schemaVersion)
            }
        }

    /** A DDL barrier: stale snapshot-based writers conflict; blind appends follow lock order. */
    fun truncateTable(
        catalog: String,
        namespace: String,
        table: String,
        expectedTableUuid: UUID,
    ): CommitResult =
        Audit.audited("table_truncate", catalog, "$namespace.$table", detail = { "snapshot=${it.snapshotId}" }) {
            jdbi.inTransactionUnchecked { h ->
                val cat = requireCatalog(h, catalog)
                Locks.acquireCatalogCommitLock(h, cat.catalogId)
                val ns = requireNamespace(h, cat, namespace)
                val t =
                    TableRepo.findLive(h, cat.catalogId, ns.namespaceId, table)
                        ?: throw HoglakeException.NotFound("table '$namespace.$table' in catalog '$catalog'")
                if (t.tableUuid != expectedTableUuid) {
                    throw HoglakeException.CommitConflict("table '$namespace.$table' no longer has the expected UUID")
                }
                val alloc = CatalogRepo.allocateSnapshot(h, cat.catalogId)
                SnapshotRepo.insert(h, cat.catalogId, alloc.snapshotId, alloc.schemaVersion)
                // Reuse the DDL conflict barrier, without changing the table's versioned metadata.
                SnapshotRepo.insertChange(h, cat.catalogId, alloc.snapshotId, ChangeKind.TABLE_ALTERED, t.tableId)
                SnapshotRepo.insertChange(h, cat.catalogId, alloc.snapshotId, ChangeKind.TABLE_DELETED_FROM, t.tableId)
                FileRepo.endLiveDeleteFiles(h, cat.catalogId, t.tableId, alloc.snapshotId)
                FileRepo.endLiveFiles(h, cat.catalogId, t.tableId, alloc.snapshotId)
                // Keep the row-id allocator: truncation must never reuse historical row IDs.
                CommitResult(snapshotId = alloc.snapshotId, schemaVersion = alloc.schemaVersion)
            }
        }

    fun getTable(
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long? = null,
        atTimestamp: Instant? = null,
    ): TableInfo =
        jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            val at = resolveReadSnapshot(h, cat, snapshot, atTimestamp)
            val ns = requireNamespace(h, cat, namespace)
            val t =
                TableRepo.findAt(h, cat.catalogId, ns.namespaceId, table, at)
                    ?: throw HoglakeException.NotFound(
                        "table '$namespace.$table' in catalog '$catalog' at snapshot $at",
                    )
            val agg = FileRepo.aggregateAt(h, cat.catalogId, t.tableId, at)
            TableInfo(
                tableId = t.tableId,
                tableUuid = t.tableUuid,
                comment = t.comment,
                properties = t.properties,
                namespace = ns.name,
                name = t.name,
                columns = TableRepo.columnsAt(h, cat.catalogId, t.tableId, at),
                recordCount = agg.recordCount,
                fileCount = agg.fileCount,
                fileSizeBytes = agg.fileSizeBytes,
                // The specs visible at the requested snapshot (null =
                // unpartitioned/unsorted there); listTables skips both.
                partitionSpec = SpecRepo.specAt(h, cat.catalogId, t.tableId, at),
                sortSpec = SortRepo.sortSpecAt(h, cat.catalogId, t.tableId, at),
            )
        }

    /** Live tables at head. Columns are NOT loaded here (empty lists). */
    fun listTables(
        catalog: String,
        namespace: String,
    ): List<TableInfo> =
        jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            val ns = requireNamespace(h, cat, namespace)
            TableRepo.listLive(h, cat.catalogId, ns.namespaceId).map { t ->
                val agg = FileRepo.aggregateAt(h, cat.catalogId, t.tableId, cat.headSnapshotId)
                TableInfo(
                    tableId = t.tableId,
                    tableUuid = t.tableUuid,
                    comment = t.comment,
                    properties = t.properties,
                    namespace = ns.name,
                    name = t.name,
                    columns = emptyList(),
                    recordCount = agg.recordCount,
                    fileCount = agg.fileCount,
                    fileSizeBytes = agg.fileSizeBytes,
                )
            }
        }

    // ---- files + changefeed ----------------------------------------------

    fun listFiles(
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long? = null,
        atTimestamp: Instant? = null,
        sort: FileRepo.FileSortColumn? = null,
        desc: Boolean = false,
        limit: Int? = null,
        offset: Int = 0,
    ): List<DataFile> =
        jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            val at = resolveReadSnapshot(h, cat, snapshot, atTimestamp)
            val ns = requireNamespace(h, cat, namespace)
            val t =
                TableRepo.findAt(h, cat.catalogId, ns.namespaceId, table, at)
                    ?: throw HoglakeException.NotFound(
                        "table '$namespace.$table' in catalog '$catalog' at snapshot $at",
                    )
            // Ordering bounds are attached to the RETURNED page only, so a
            // paged request pays the bound decode for its page, not the
            // whole manifest.
            withOrderingBounds(
                h,
                cat.catalogId,
                t.tableId,
                at,
                FileRepo.listAt(h, cat.catalogId, t.tableId, at, sort, desc, limit, offset),
            )
        }

    /**
     * Attach each file's ordering-key range — the bounds of the column
     * the table's rows are ORDERED by, which is what tells a reader
     * which files a predicate can skip and which files overlap.
     *
     * The key is the TABLE's, resolved once for the whole listing:
     *
     *  - a sort spec at [at] means the rows are ordered by its LEADING
     *    field, so that field's stored bounds are the answer. The
     *    leading field alone: it is the only one whose range is a
     *    contiguous interval per file (the second key only orders rows
     *    that TIE on the first, so its per-file min/max spans the whole
     *    column and prunes nothing), and it is the field a reader's
     *    predicate has to hit before any other key matters.
     *  - no sort spec means the only ordering is the append order,
     *    which is the row id.
     *
     * Bounds come from ONE query for the whole page, not one per file.
     * A file with no stats row for the key gets no bounds rather than
     * an invented range: pending/failed files have no rows at all, and
     * a key column added after a file landed has none on that file.
     * The row-id range needs no stats and is always given — row_id_start
     * is assigned at commit for every file, deferred stats included.
     */
    private fun withOrderingBounds(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        at: Long,
        files: List<DataFile>,
    ): List<DataFile> {
        val leading =
            SortRepo.sortSpecAt(h, catalogId, tableId, at)?.fields?.firstOrNull()
                ?: return files.map { it.copy(orderingBounds = rowIdBounds(it)) }
        // A sort spec can outlive the column it names only through a
        // drop, which AlterService refuses while the column is a sort
        // source — but a read at an older snapshot can still land on a
        // spec whose source is not visible there, and a field id with no
        // column has no type to decode its bounds under.
        val (path, column) =
            columnsByFieldId(TableRepo.columnsAt(h, catalogId, tableId, at))[leading.sourceFieldId]
                ?: return files
        val statsByFile =
            FileRepo.columnStatsFor(h, catalogId, files.map { it.dataFileId }, leading.sourceFieldId)
        return files.map { file ->
            val stats = statsByFile[file.dataFileId] ?: return@map file
            file.copy(
                orderingBounds =
                    FileOrderingBounds.SortKey(
                        FileColumnStats(
                            fieldId = leading.sourceFieldId,
                            name = column.def.name,
                            path = path,
                            type = column.def.type,
                            typeParams = column.def.typeParams,
                            stats = stats,
                        ),
                    ),
            )
        }
    }

    /**
     * The row-id range of one file, or null when there is no range to
     * state: an EMPTY file spans nothing, and `row_id_start - 1` as its
     * maximum would read as a range running backwards.
     *
     * The maximum is positional arithmetic, which is only sound while
     * the ids ARE positions. A compaction output's are not (see
     * [FileOrderingBounds.RowIds]), so it reports its minimum and
     * leaves the maximum unknown.
     */
    private fun rowIdBounds(file: DataFile): FileOrderingBounds? {
        if (file.recordCount <= 0L) return null
        return FileOrderingBounds.RowIds(
            lower = file.rowIdStart,
            upper = if (file.explicitRowIds) null else file.rowIdStart + file.recordCount - 1,
        )
    }

    /**
     * Per-column statistics for ONE data file (GET
     * .../files/{fileId}/stats), with each stats row joined to its
     * column identity — name, dotted path, type — as visible at the
     * resolved snapshot. The file must belong to the named table and be
     * visible at that snapshot (404 otherwise, like every read here).
     *
     * The join is deliberately REFLECTIVE, never generative: one entry
     * per stored stats row whose field id resolves to a visible column.
     * Variant columns and containers never have rows (the commit door
     * refuses them; the hydrator never emits them), so they never
     * appear; a row whose field id is not visible at the snapshot (a
     * dropped column) is omitted, because without a column there is no
     * type to decode its bounds under.
     *
     * Decoding of the bound BYTES is the wire layer's job
     * (api/FileStatsDto, over stats/BoundWire) — this returns the
     * stored rows and the type context, nothing pre-rendered.
     */
    fun fileStats(
        catalog: String,
        namespace: String,
        table: String,
        fileId: Long,
        snapshot: Long? = null,
        atTimestamp: Instant? = null,
    ): FileStats =
        jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            val at = resolveReadSnapshot(h, cat, snapshot, atTimestamp)
            val ns = requireNamespace(h, cat, namespace)
            val t =
                TableRepo.findAt(h, cat.catalogId, ns.namespaceId, table, at)
                    ?: throw HoglakeException.NotFound(
                        "table '$namespace.$table' in catalog '$catalog' at snapshot $at",
                    )
            val file =
                FileRepo.findAt(h, cat.catalogId, t.tableId, fileId, at)
                    ?: throw HoglakeException.NotFound(
                        "data file $fileId of table '$namespace.$table' in catalog " +
                            "'$catalog' at snapshot $at",
                    )
            // pending/failed files have no stats rows by construction —
            // answer the explicit empty shape without querying for rows
            // that cannot exist.
            if (file.statsState != StatsState.PROVIDED) {
                return@withHandleUnchecked FileStats(fileId, file.statsState, emptyList())
            }
            val byFieldId = columnsByFieldId(TableRepo.columnsAt(h, cat.catalogId, t.tableId, at))
            val columns =
                FileRepo.columnStats(h, cat.catalogId, fileId).mapNotNull { row ->
                    byFieldId[row.fieldId]?.let { (path, column) ->
                        FileColumnStats(
                            fieldId = row.fieldId,
                            name = column.def.name,
                            path = path,
                            type = column.def.type,
                            typeParams = column.def.typeParams,
                            stats = row,
                        )
                    }
                }
            FileStats(fileId, file.statsState, columns)
        }

    /** Every node of the column forest keyed by field id, with its dotted path. */
    private fun columnsByFieldId(forest: List<Column>): Map<Long, Pair<String, Column>> {
        val out = mutableMapOf<Long, Pair<String, Column>>()

        fun walk(
            columns: List<Column>,
            prefix: String,
        ) {
            for (column in columns) {
                val path = if (prefix.isEmpty()) column.def.name else "$prefix.${column.def.name}"
                out[column.fieldId] = path to column
                walk(column.children, path)
            }
        }
        walk(forest, "")
        return out
    }

    /**
     * Changefeed plan for (fromSnapshot, toSnapshot]: the table's
     * identity (uuid — incarnation changes are visible, not deduced),
     * the resolved range, the files whose begin_snapshot falls in the
     * half-open range (append order), and the deletion vectors
     * registered in the same range (begin_snapshot, delete_file_id
     * order) — the deletions feed.
     *
     * Expiry guard: fromSnapshot is EXCLUSIVE, so the lowest snapshot
     * the plan draws from is fromSnapshot + 1. That must be retained:
     * fromSnapshot < earliest_snapshot_id - 1 means part of the range
     * has been expired -> [HoglakeException.Expired] (HTTP 410); the
     * consumer must reconcile from a full scan, never silently skip.
     */
    fun changes(
        catalog: String,
        namespace: String,
        table: String,
        fromSnapshot: Long,
        toSnapshot: Long? = null,
    ): ChangesPlan =
        jdbi.inTransactionUnchecked { h ->
            // Keep the retention floor, truncate barriers and file plan on one MVCC snapshot.
            h.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            val cat = requireCatalog(h, catalog)
            val to = resolveSnapshot(cat, toSnapshot)
            if (fromSnapshot < 0) {
                throw HoglakeException.Validation("from_snapshot must be >= 0, got $fromSnapshot")
            }
            if (fromSnapshot > to) {
                throw HoglakeException.Validation(
                    "from_snapshot $fromSnapshot is beyond to_snapshot $to",
                )
            }
            val floor = TimeTravelRepo.expiryFloor(h, cat.catalogId)
            if (fromSnapshot < floor.earliestSnapshotId - 1) {
                throw HoglakeException.Expired(
                    "from_snapshot $fromSnapshot reaches below the expiry floor " +
                        "(earliest retained snapshot is ${floor.earliestSnapshotId}" +
                        "${floor.reachedAtSuffix()}): part of the range is " +
                        "gone; reconcile by re-reading from a full scan at a retained " +
                        "snapshot instead of consuming this feed",
                )
            }
            val ns = requireNamespace(h, cat, namespace)
            val t =
                TableRepo.findAt(h, cat.catalogId, ns.namespaceId, table, to)
                    ?: throw HoglakeException.NotFound(
                        "table '$namespace.$table' in catalog '$catalog' at snapshot $to",
                    )
            // TRUNCATE and replacement have no newly registered DV to carry its deletion through this
            // append-oriented feed. The paired change kinds identify its DDL barrier.
            val truncated =
                h.createQuery(
                    """
                    SELECT c.snapshot_id FROM hog_snapshot_change c
                    WHERE c.catalog_id = :catalogId AND c.object_id = :tableId
                      AND c.kind = 'table_deleted_from'
                      AND c.snapshot_id > :fromSnapshot AND c.snapshot_id <= :toSnapshot
                      AND EXISTS (
                        SELECT 1 FROM hog_snapshot_change ddl
                        WHERE ddl.catalog_id = c.catalog_id AND ddl.object_id = c.object_id
                          AND ddl.snapshot_id = c.snapshot_id AND ddl.kind = 'table_altered')
                    ORDER BY c.snapshot_id LIMIT 1
                    """,
                ).bind("catalogId", cat.catalogId)
                    .bind("tableId", t.tableId)
                    .bind("fromSnapshot", fromSnapshot)
                    .bind("toSnapshot", to)
                    .mapTo(Long::class.java)
                    .findOne()
            if (truncated.isPresent) {
                throw HoglakeException.ReconciliationRequired(
                    "table '$namespace.$table' was truncated or replaced at snapshot ${truncated.get()}; " +
                        "changefeed window ($fromSnapshot, $to] cannot represent this deletion. " +
                        "Reconcile the destination from a full snapshot before advancing its checkpoint.",
                )
            }
            ChangesPlan(
                tableUuid = t.tableUuid,
                fromSnapshot = fromSnapshot,
                toSnapshot = to,
                files = FileRepo.changedIn(h, cat.catalogId, t.tableId, fromSnapshot, to),
                deleteFiles =
                    DeleteFileReadRepo.changedIn(
                        h,
                        cat.catalogId,
                        t.tableId,
                        fromSnapshot,
                        to,
                    ),
            )
        }

    // ---- snapshots -------------------------------------------------------

    /**
     * One page of snapshots (changes populated), plus whether more pages
     * exist. Two mutually exclusive cursors:
     *
     *  - [after] (default): ids > after, ascending — the original feed.
     *  - [before] non-null: ids < before, DESCENDING — a UI walks
     *    newest-first starting at head + 1 and pages down with the last
     *    id of each page. Supplying [before] alongside a non-zero
     *    [after] is a Validation (422).
     */
    fun listSnapshots(
        catalog: String,
        after: Long,
        limit: Int,
        before: Long? = null,
    ): Pair<List<Snapshot>, Boolean> {
        if (limit < 1) throw HoglakeException.Validation("limit must be >= 1, got $limit")
        if (before != null && after != 0L) {
            throw HoglakeException.Validation(
                "'before' and a non-zero 'after' are mutually exclusive; supply at most one cursor",
            )
        }
        return jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            val raw =
                if (before != null) {
                    SnapshotRepo.pageBefore(h, cat.catalogId, before, limit + 1)
                } else {
                    SnapshotRepo.page(h, cat.catalogId, after, limit + 1)
                }
            val hasMore = raw.size > limit
            val page = raw.take(limit)
            if (page.isEmpty()) return@withHandleUnchecked Pair(emptyList(), false)
            // The page is contiguous in either direction; changesFor takes
            // the range low-to-high.
            val changes =
                SnapshotRepo.changesFor(
                    h,
                    cat.catalogId,
                    minOf(page.first().snapshotId, page.last().snapshotId),
                    maxOf(page.first().snapshotId, page.last().snapshotId),
                )
            Pair(
                page.map { it.copy(changes = changes[it.snapshotId] ?: emptyList()) },
                hasMore,
            )
        }
    }

    // ---- consumer offsets ------------------------------------------------

    fun commitOffset(
        catalog: String,
        consumerId: String,
        tableUuid: UUID,
        snapshotId: Long,
    ): ConsumerOffset =
        Audit.audited(
            "offset_commit",
            catalog,
            "$consumerId/$tableUuid",
            detail = { "snapshot=$snapshotId" },
        ) {
            jdbi.inTransactionUnchecked { h ->
                val cat = requireCatalog(h, catalog)
                if (snapshotId < 0 || snapshotId > cat.headSnapshotId) {
                    throw HoglakeException.Validation(
                        "snapshot $snapshotId out of range [0, ${cat.headSnapshotId}]",
                    )
                }
                // The floor guard (mirrors every read path's 410 contract):
                // a committed offset below earliest_snapshot_id is a
                // position in expired history the consumer can never read
                // from — and on consumer_floor catalogs it would pin every
                // future expiry sweep to a zero-work return FOREVER (the
                // sweep's min-offset bound could never rise above the
                // floor, and no offset-delete API exists to unwedge it).
                if (snapshotId < cat.earliestSnapshotId) {
                    val floor = TimeTravelRepo.expiryFloor(h, cat.catalogId)
                    throw HoglakeException.Expired(
                        "cannot commit offset at snapshot $snapshotId: it is below the " +
                            "expiry floor (earliest retained snapshot is " +
                            "${floor.earliestSnapshotId}${floor.reachedAtSuffix()}); " +
                            "reconcile from a full scan at a retained snapshot",
                    )
                }
                // The uuid must name a table this catalog has EVER had — any
                // incarnation, dropped included (offsets deliberately survive
                // drops so consumers SEE incarnation changes). A garbage uuid
                // would otherwise pin the consumer_floor retention floor
                // forever.
                val known =
                    h.createQuery(
                        "SELECT 1 FROM hog_table WHERE catalog_id = :catalogId AND table_uuid = :tableUuid",
                    )
                        .bind("catalogId", cat.catalogId)
                        .bind("tableUuid", tableUuid)
                        .mapTo(Int::class.javaObjectType)
                        .findOne()
                        .isPresent
                if (!known) {
                    throw HoglakeException.Validation(
                        "unknown table_uuid $tableUuid in catalog '$catalog'",
                    )
                }
                OffsetRepo.upsert(h, cat.catalogId, consumerId, tableUuid, snapshotId)
                    ?: throw HoglakeException.OffsetRegression(
                        "consumer '$consumerId' already committed past snapshot $snapshotId " +
                            "for table $tableUuid",
                    )
            }
        }

    /** Every consumer's offsets, enriched with table names for listing. */
    fun listConsumers(catalog: String): List<com.posthog.hoglake.model.ConsumerTableOffset> =
        jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            OffsetRepo.listAll(h, cat.catalogId)
        }

    fun listOffsets(
        catalog: String,
        consumerId: String,
    ): List<ConsumerOffset> =
        jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            OffsetRepo.list(h, cat.catalogId, consumerId)
        }

    /** One (consumer, table_uuid) offset; no stored row -> NotFound (404). */
    fun getOffset(
        catalog: String,
        consumerId: String,
        tableUuid: UUID,
    ): ConsumerOffset =
        jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            OffsetRepo.find(h, cat.catalogId, consumerId, tableUuid)
                ?: throw HoglakeException.NotFound(
                    "no offset for consumer '$consumerId' on table $tableUuid " +
                        "in catalog '$catalog'",
                )
        }

    // ---- time travel -----------------------------------------------------

    /**
     * Resolve an ISO timestamp to a snapshot id: the largest retained
     * snapshot with snapshot_time <= [atTimestamp] (between two
     * snapshots -> the lower; after head's time -> head). A timestamp
     * before the earliest RETAINED snapshot's time ->
     * [HoglakeException.Expired].
     */
    fun resolveTimestamp(
        catalog: String,
        atTimestamp: Instant,
    ): Long =
        jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            TimeTravelRepo.resolveTimestamp(
                h,
                cat.catalogId,
                TimeTravelRepo.earliestSnapshotId(h, cat.catalogId),
                atTimestamp,
            )
        }

    // ---- helpers ---------------------------------------------------------

    private fun requireCatalog(
        h: Handle,
        name: String,
    ): CatalogInfo =
        CatalogRepo.findByName(h, name)
            ?: throw HoglakeException.NotFound("catalog '$name'")

    private fun requireNamespace(
        h: Handle,
        cat: CatalogInfo,
        name: String,
    ): NamespaceInfo =
        NamespaceRepo.findLiveByName(h, cat.catalogId, name)
            ?: throw HoglakeException.NotFound("namespace '$name' in catalog '${cat.name}'")

    /** Time-travel target: default head; reject out-of-range ids. */
    private fun resolveSnapshot(
        cat: CatalogInfo,
        snapshot: Long?,
    ): Long {
        val s = snapshot ?: return cat.headSnapshotId
        if (s < 0 || s > cat.headSnapshotId) {
            throw HoglakeException.Validation(
                "snapshot $s out of range [0, ${cat.headSnapshotId}] for catalog '${cat.name}'",
            )
        }
        return s
    }

    /**
     * Point-in-time read target for getTable/listFiles: at most one of
     * [snapshot] / [atTimestamp] (both -> Validation); a timestamp
     * resolves via [TimeTravelRepo.resolveTimestamp]; an explicit
     * snapshot id below the expiry floor -> [HoglakeException.Expired].
     * Head reads (neither given) never hit the floor.
     */
    private fun resolveReadSnapshot(
        h: Handle,
        cat: CatalogInfo,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): Long {
        if (snapshot != null && atTimestamp != null) {
            throw HoglakeException.Validation(
                "snapshot and at_timestamp are mutually exclusive; supply at most one",
            )
        }
        if (atTimestamp != null) {
            return TimeTravelRepo.resolveTimestamp(
                h,
                cat.catalogId,
                TimeTravelRepo.earliestSnapshotId(h, cat.catalogId),
                atTimestamp,
            )
        }
        val at = resolveSnapshot(cat, snapshot)
        if (snapshot != null) {
            val floor = TimeTravelRepo.expiryFloor(h, cat.catalogId)
            if (at < floor.earliestSnapshotId) {
                throw HoglakeException.Expired(
                    "snapshot $at is below the expiry floor (earliest retained snapshot " +
                        "is ${floor.earliestSnapshotId}${floor.reachedAtSuffix()}) " +
                        "for catalog '${cat.name}'",
                )
            }
        }
        return at
    }
}
