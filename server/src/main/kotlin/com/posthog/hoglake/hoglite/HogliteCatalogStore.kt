package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.model.CatalogNotEmptyReason
import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.ChangesPlan
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.CommitResult
import com.posthog.hoglake.model.ConsumerOffset
import com.posthog.hoglake.model.ConsumerTableOffset
import com.posthog.hoglake.model.DataFile
import com.posthog.hoglake.model.DeleteFile
import com.posthog.hoglake.model.DroppedCatalog
import com.posthog.hoglake.model.FileColumnStats
import com.posthog.hoglake.model.FileOrderingBounds
import com.posthog.hoglake.model.FileStats
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceInfo
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.PartitionFieldValues
import com.posthog.hoglake.model.PartitionSpec
import com.posthog.hoglake.model.PartitionValues
import com.posthog.hoglake.model.ScanFile
import com.posthog.hoglake.model.Snapshot
import com.posthog.hoglake.model.SnapshotChange
import com.posthog.hoglake.model.SnapshotResolution
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.model.SortSpec
import com.posthog.hoglake.model.StatsState
import com.posthog.hoglake.model.StorageUsage
import com.posthog.hoglake.model.TableInfo
import com.posthog.hoglake.model.TableSummaryInfo
import com.posthog.hoglake.model.assignFieldIds
import com.posthog.hoglake.persistence.FileRepo
import com.posthog.hoglake.service.ColumnTrees
import com.posthog.hoglake.service.Identifiers
import com.posthog.hoglake.service.ScanService
import com.posthog.hoglake.service.requireOperationMetadata
import com.posthog.hoglake.store.CatalogStore
import java.time.Instant
import java.util.UUID

/**
 * The hoglite backend of [com.posthog.hoglake.store.CatalogStore]:
 * every catalog is one hoglite TENANT, and every operation is one
 * hoglite transaction (writes) or one read-only query (reads). The
 * semantics are hoglake's own — the OCC guards, snapshot allocation,
 * history-preserving deletion vectors and idempotent publication that
 * CommitService/CatalogService implement against Postgres —
 * re-expressed against hoglite's single-round-trip transaction API,
 * where the per-tenant serialization hoglite already provides replaces
 * the per-catalog advisory lock.
 *
 * A second tenant, [HogliteCatalogStore.REGISTRY_TENANT], holds the
 * cross-catalog invariants (unique names, disjoint data_path prefixes)
 * that cannot live inside any one catalog. Its name cannot collide with
 * a user catalog: catalog names must start with a lowercase letter and
 * this one starts with two underscores.
 */
class HogliteCatalogStore(
    internal val client: HogliteClient,
    /*
     * Deployment prefix for tenant ids (Config.hogliteTenantPrefix):
     * catalog `wh` lives in tenant `hoglake-wh`. Two hoglake
     * deployments sharing one hoglite cluster set distinct prefixes.
     */
    private val tenantPrefix: String = "hoglake-",
    /** CommitService's blind-partitioned-append enforcement flag, for the commit port. */
    internal val refuseBlindPartitionedAppends: Boolean = false,
    /** Removal-ledger retention, for the upload sweep's rested-tombstone rule. */
    private val ledgerRetentionSeconds: Long = com.posthog.hoglake.service.CleanupService.LEDGER_RETENTION_SECONDS,
) : CatalogStore {
    internal val json: ObjectMapper = jacksonObjectMapper()

    /** The hoglite tenant id a catalog lives in. Applied at the client boundary ONLY. */
    internal fun tenantOf(catalog: String): String = tenantPrefix + catalog

    private val log = io.github.oshai.kotlinlogging.KotlinLogging.logger {}

    // ==================== catalogs =========================================

    override fun createCatalog(
        name: String,
        dataPath: String,
        author: String?,
        message: String?,
    ): CatalogInfo {
        validateCatalogName(name)
        validateDataPathShape(dataPath)
        // The cross-catalog invariants (unique name, disjoint data_path
        // prefixes) live in the registry tenant: one row per catalog,
        // written in the same transaction as the guard that checks the
        // name. The overlap check is a pre-flight read, exactly like
        // hoglake's in-transaction Kotlin loop over every catalog — same
        // check, same accepted race (two concurrent creates of DIFFERENT
        // names with overlapping paths; CatalogService.createCatalog's
        // comment carries the argument for accepting it).
        ensureRegistry()
        val existing = queryRegistry("SELECT name, data_path FROM catalogs")
        val newPrefix = dataPath.trimEnd('/') + "/"
        for (row in existing.rows) {
            val theirName = row[0].asString()
            if (theirName == name) continue // the guarded insert below answers same-name
            val theirPrefix = row[1].asString().trimEnd('/') + "/"
            if (newPrefix.startsWith(theirPrefix) || theirPrefix.startsWith(newPrefix)) {
                throw HoglakeException.Validation(
                    "data_path '${Identifiers.cap(dataPath)}' overlaps catalog '$theirName' (data_path " +
                        "'${Identifiers.cap(row[1].asString())}'); catalog data_paths must be disjoint",
                )
            }
        }
        // The tenant FIRST, then the registry row: a crash between
        // them leaves an orphan TENANT (harmless — nothing lists it and
        // a retried create adopts it), never a ghost registry row that
        // poisons every listCatalogs caller and can be neither created
        // nor dropped. Both steps are idempotent: createTenant answers
        // an existing tenant as success, and the bootstrap's
        // INSERT OR IGNORE + hoglite's dedup make a replay a read.
        client.createTenant(tenantOf(name))
        val boot = HogliteTx()
        HogliteSchema.DDL.forEach { boot.add(it) }
        boot.add(
            "INSERT OR IGNORE INTO meta(id, name, data_path) VALUES (1, ?, ?)",
            name,
            dataPath,
        )
        boot.add(
            "INSERT OR IGNORE INTO snapshots(id, ts, author, message, schema_version) VALUES (0, ?, ?, ?, 0)",
            HogliteSchema.now(),
            author,
            message,
        )
        // Readback, so a bootstrap racing a different data_path is caught
        // here rather than silently adopting the old one.
        val readback = boot.add("SELECT data_path FROM meta WHERE id = 1")
        val results = runTx(name, "boot:$name", fp = "bootstrap:v1", boot, fenced = false)
        val storedPath = results[readback].firstRow()?.get(0).asString()
        if (storedPath != dataPath) {
            throw HoglakeException.AlreadyExists(
                "catalog '$name' already exists with a different data_path; drop it first",
            )
        }

        // The registry row publishes the catalog. A fresh txid per CALL:
        // hoglake's createCatalog is not an idempotent endpoint (a
        // same-name create is a 409), so the guard — not hoglite's dedup
        // — is what must answer a replay.
        val reg = HogliteTx()
        reg.expectZero(
            "SELECT COUNT(*) FROM catalogs WHERE name = ?",
            name,
        ) { HoglakeException.AlreadyExists("catalog '$name' already exists") }
        reg.add("INSERT INTO catalogs(name, data_path) VALUES (?, ?)", name, dataPath)
        runTx(REGISTRY_TENANT, freshTxid("create-catalog"), null, reg, fenced = false)
        return getCatalog(name)
    }

    override fun getCatalog(name: String): CatalogInfo = meta(name).toCatalogInfo()

    override fun listCatalogs(visible: Set<String>?): List<CatalogInfo> {
        ensureRegistry()
        val rows = queryRegistry("SELECT name FROM catalogs ORDER BY name")
        return rows.rows
            .map { it[0].asString() }
            .filter { visible == null || it in visible }
            .mapNotNull {
                try {
                    getCatalog(it)
                } catch (e: HoglakeException.NotFound) {
                    // A registry row whose tenant is mid-create or
                    // mid-drop. Skip it rather than failing every
                    // caller's list; dropCatalog cleans the row up.
                    log.warn { "catalog '$it' is registered but its tenant is not readable; skipping" }
                    null
                }
            }
    }

    override fun dropCatalog(
        catalog: String,
        author: String?,
        message: String?,
    ): DroppedCatalog {
        // A ghost registry row (a create that died before its tenant
        // existed) is droppable too: skip the tenant and take the row.
        val info =
            try {
                meta(catalog)
            } catch (e: HoglakeException.NotFound) {
                // No tenant: only a ghost REGISTRY row (a create that
                // died mid-flight) may still be dropped. An unknown name
                // is the plain 404.
                val registered =
                    queryRegistry("SELECT data_path FROM catalogs WHERE name = ?", catalog)
                        .firstRow()
                if (registered == null) {
                    throw HoglakeException.NotFound("catalog '$catalog'")
                }
                runTx(
                    REGISTRY_TENANT,
                    freshTxid("drop-catalog"),
                    null,
                    HogliteTx().apply { add("DELETE FROM catalogs WHERE name = ?", catalog) },
                    fenced = false,
                )
                return DroppedCatalog(catalog, registered[0].asString())
            }

        // The emptiness preconditions AND the write fence in ONE
        // transaction: the guards run in the resolution ORDER the
        // Postgres side documents (CatalogNotEmptyReason's KDoc is the
        // contract), and the same tx sets meta.dropping, which every
        // mutating tenant tx guards on (runTx statement 0) — so no
        // namespace, commit, or loop sweep can land between these
        // checks and the tenant deletion below. The fence tx itself
        // opts out of the fence (a retried drop after a partial failure
        // must be able to proceed).
        fun countNow(sql: String): Long = queryCatalog(catalog, sql).firstRow()!!.get(0).asLong()

        val fenceNow = HogliteSchema.now()
        val fence = HogliteTx()

        fun emptyGuard(
            sql: String,
            reason: CatalogNotEmptyReason,
            describe: (Long) -> String,
        ) {
            fence.expectZero(sql) {
                val n = countNow(sql)
                HoglakeException.CatalogNotEmpty(
                    "catalog '$catalog' is not empty: ${describe(n)}",
                    reason,
                )
            }
        }
        emptyGuard(
            "SELECT COUNT(*) FROM namespaces WHERE dropped_snapshot IS NULL",
            CatalogNotEmptyReason.LIVE_NAMESPACES,
        ) { "$it live namespace(s) remain" }
        emptyGuard(
            "SELECT (SELECT COUNT(*) FROM files WHERE end_snapshot IS NULL) + " +
                "(SELECT COUNT(*) FROM dvs WHERE end_snapshot IS NULL)",
            CatalogNotEmptyReason.TABLES_PENDING_RETIREMENT,
        ) { "$it live file row(s) await retirement" }
        emptyGuard(
            "SELECT (SELECT COUNT(*) FROM files) + (SELECT COUNT(*) FROM dvs)",
            CatalogNotEmptyReason.FILES_PENDING_EXPIRY,
        ) { "$it ended file row(s) await expiry" }
        emptyGuard(
            "SELECT COUNT(*) FROM removals WHERE drained_at IS NULL",
            CatalogNotEmptyReason.REMOVALS_PENDING,
        ) { "$it removal(s) are not drained" }
        emptyGuard(
            "SELECT COUNT(*) FROM compaction_claims WHERE expires_at > '$fenceNow'",
            CatalogNotEmptyReason.ACTIVE_COMPACTION_CLAIMS,
        ) { "$it live compaction claim(s); a compactor may be mid-rewrite" }
        emptyGuard(
            "SELECT COUNT(*) FROM offsets",
            CatalogNotEmptyReason.CONSUMER_OFFSETS,
        ) { "$it consumer offset(s) remain" }
        emptyGuard(
            "SELECT COUNT(*) FROM uploads WHERE state = 'active'",
            CatalogNotEmptyReason.ACTIVE_UPLOADS,
        ) { "$it active upload claim(s) remain" }
        emptyGuard(
            "SELECT COUNT(*) FROM uploads WHERE state = 'abandoned' " +
                "AND (scheduled_at IS NULL OR expires_at > '$fenceNow')",
            CatalogNotEmptyReason.ABANDONED_UPLOADS_UNSETTLED,
        ) {
            "$it abandoned upload(s) are not settled (schedule them with POST .../uploads/schedule-expired, " +
                "or wait out their expiry window)"
        }
        fence.add("UPDATE meta SET dropping = 1 WHERE id = 1")
        runTx(catalog, freshTxid("drop-fence"), null, fence, fenced = false)

        // Order matters: fence the owner so nothing in flight can
        // re-materialize the tenant, THEN delete its durable state, then
        // the registry row. release and tenant DELETE are owner-LOCAL in
        // hoglite (not proxied), so each attempt first pins the owner via
        // a routed status call; a 409 from DELETE means a live remote
        // owner still holds it — re-pin, fence again, retry with a pause.
        var fencingFailure: HogliteRejected? = null
        repeat(3) {
            if (it > 0) Thread.sleep(200)
            client.status(tenantOf(catalog))
            client.release(tenantOf(catalog))
            try {
                client.deleteTenant(tenantOf(catalog))
                fencingFailure = null
                return@repeat
            } catch (e: HogliteRejected) {
                if (e.status == 409) fencingFailure = e else throw mapRejected(catalog, e)
            }
        }
        fencingFailure?.let {
            throw HoglakeException.BackendUnavailable(
                "hoglite refused to delete tenant '$catalog' three times in a row (a live owner " +
                    "kept re-claiming it): ${it.body}",
            )
        }
        val reg = HogliteTx()
        reg.add("DELETE FROM catalogs WHERE name = ?", catalog)
        // Fresh txid: the registry tenant's dedup table survives the
        // catalog's own tenant, so a fixed txid would make a
        // recreate-then-redrop silently skip the registry delete.
        runTx(REGISTRY_TENANT, freshTxid("drop-catalog"), null, reg, fenced = false)
        return DroppedCatalog(catalog, info.dataPath)
    }

    override fun storageUsage(catalog: String): StorageUsage {
        meta(catalog)
        val row =
            queryCatalog(
                catalog,
                "SELECT (SELECT COALESCE(SUM(file_size_bytes),0) FROM files WHERE end_snapshot IS NULL) + " +
                    "(SELECT COALESCE(SUM(file_size_bytes),0) FROM dvs WHERE end_snapshot IS NULL), " +
                    "(SELECT COUNT(*) FROM removals)",
            ).firstRow()!!
        return StorageUsage(
            trackedBytes = row[0].asLong(),
            pendingDeleteFiles = row[1].asLong(),
        )
    }

    // ==================== namespaces =======================================

    override fun createNamespace(
        catalog: String,
        name: String,
        author: String?,
        message: String?,
    ): NamespaceInfo {
        Identifiers.validate("namespace", name)
        meta(catalog) // 404 first, as on the Postgres path
        val tx = HogliteTx()
        tx.expectZero(
            "SELECT COUNT(*) FROM namespaces WHERE name = ? AND dropped_snapshot IS NULL",
            name,
        ) { HoglakeException.AlreadyExists("namespace '$name' already exists") }
        tx.allocateDdlSnapshot()
        tx.affectOne(
            "UPDATE meta SET next_namespace_id = next_namespace_id + 1 WHERE id = 1",
        ) { HoglakeException.NotFound("catalog '$catalog'") }
        tx.add(
            "INSERT INTO namespaces(id, name, created_snapshot) " +
                "VALUES ((SELECT next_namespace_id - 1 FROM meta WHERE id = 1), ?, ${HogliteSchema.HEAD_SQL})",
            name,
        )
        tx.add(
            "INSERT INTO changes(snapshot_id, kind, object_id) " +
                "VALUES (${HogliteSchema.HEAD_SQL}, 'namespace_created', " +
                "(SELECT next_namespace_id - 1 FROM meta WHERE id = 1))",
        )
        tx.insertSnapshotRow(author, message)
        val readback = tx.add(HogliteSql.READBACK_DDL)
        val results = runTx(catalog, freshTxid("create-ns"), null, tx)
        val row = results[readback].firstRow()!!
        return NamespaceInfo(namespaceId = row[3].asLong(), name = name)
    }

    override fun listNamespaces(
        catalog: String,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): List<NamespaceInfo> {
        val m = meta(catalog)
        val rows =
            if (snapshot == null && atTimestamp == null) {
                queryCatalog(
                    catalog,
                    "SELECT id, name FROM namespaces WHERE dropped_snapshot IS NULL ORDER BY name",
                )
            } else {
                val at = resolveRead(catalog, m, snapshot, atTimestamp)
                queryCatalog(
                    catalog,
                    "SELECT id, name FROM namespaces WHERE created_snapshot <= ? " +
                        "AND (dropped_snapshot IS NULL OR dropped_snapshot > ?) ORDER BY name",
                    at,
                    at,
                )
            }
        return rows.rows.map { NamespaceInfo(it[0].asLong(), it[1].asString()) }
    }

    override fun getNamespace(
        catalog: String,
        namespace: String,
    ): NamespaceInfo {
        meta(catalog)
        return liveNamespace(catalog, namespace)
    }

    override fun dropNamespace(
        catalog: String,
        namespace: String,
        expectedNamespaceId: Long?,
        author: String?,
        message: String?,
    ): CommitResult {
        requireOperationMetadata(author, message)
        val ns = liveNamespace(catalog, namespace)
        if (expectedNamespaceId != null && ns.namespaceId != expectedNamespaceId) {
            throw HoglakeException.CommitConflict(
                "namespace '$namespace' no longer has the expected identity",
            )
        }
        val tx = HogliteTx()
        // Emptiness under hoglite's per-tenant serialization: the guard
        // runs IN the transaction that flips the flag, so the live set
        // it counts is the live set the drop commits against.
        tx.expectZero(
            "SELECT COUNT(*) FROM tables WHERE namespace_id = ? AND dropped_snapshot IS NULL",
            ns.namespaceId,
        ) {
            HoglakeException.NamespaceNotEmpty(
                "namespace '$namespace' in catalog '$catalog' is not empty: live table(s) remain",
            )
        }
        tx.expectZero(
            "SELECT COUNT(*) FROM views WHERE namespace_id = ? AND dropped_snapshot IS NULL",
            ns.namespaceId,
        ) {
            HoglakeException.NamespaceNotEmpty(
                "namespace '$namespace' in catalog '$catalog' is not empty: live view(s) remain",
            )
        }
        tx.expectOne(
            "SELECT COUNT(*) FROM namespaces WHERE id = ? AND dropped_snapshot IS NULL",
            ns.namespaceId,
        ) { HoglakeException.NotFound("namespace '$namespace' in catalog '$catalog'") }
        tx.allocateDdlSnapshot()
        tx.affectOne(
            "UPDATE namespaces SET dropped_snapshot = ${HogliteSchema.HEAD_SQL} WHERE id = ?",
            ns.namespaceId,
        ) { HoglakeException.NotFound("namespace '$namespace' in catalog '$catalog'") }
        tx.add(
            "INSERT INTO changes(snapshot_id, kind, object_id) " +
                "VALUES (${HogliteSchema.HEAD_SQL}, 'namespace_dropped', ?)",
            ns.namespaceId,
        )
        tx.insertSnapshotRow(author, message)
        val readback = tx.add(HogliteSql.READBACK_DDL)
        val results = runTx(catalog, freshTxid("drop-ns"), null, tx)
        val row = results[readback].firstRow()!!
        return CommitResult(snapshotId = row[0].asLong(), schemaVersion = row[1].asLong())
    }

    // ==================== tables ===========================================

    override fun createTable(
        catalog: String,
        namespace: String,
        name: String,
        columns: List<ColumnDef>,
        author: String?,
        message: String?,
    ): TableInfo {
        Identifiers.validate("table", name)
        // The column-tree rules (nesting depth, synthetic child names,
        // decimal params, duplicates) are hoglake's own object — same
        // refusals, same messages, as the Postgres path.
        ColumnTrees.validate(columns)
        val ns = liveNamespace(catalog, namespace)
        val assigned = assignFieldIds(columns, firstFieldId = 1)
        val tableUuid = UUID.randomUUID()
        val maxFieldId = assigned.flatMap { it.selfAndDescendants() }.maxOf { it.fieldId }

        val tx = HogliteTx()
        tx.expectOne(
            "SELECT COUNT(*) FROM namespaces WHERE id = ? AND dropped_snapshot IS NULL",
            ns.namespaceId,
        ) { HoglakeException.NotFound("namespace '$namespace' in catalog '$catalog'") }
        tx.expectZero(
            "SELECT COUNT(*) FROM tables WHERE namespace = ? AND name = ? AND dropped_snapshot IS NULL",
            namespace,
            name,
        ) { HoglakeException.AlreadyExists("table '$namespace.$name' already exists in catalog '$catalog'") }
        tx.allocateDdlSnapshot()
        tx.affectOne(
            "UPDATE meta SET next_table_id = next_table_id + 1 WHERE id = 1",
        ) { HoglakeException.NotFound("catalog '$catalog'") }
        tx.add(
            "INSERT INTO tables(id, namespace_id, namespace, name, table_uuid, next_field_id, created_snapshot) " +
                "VALUES ((SELECT next_table_id - 1 FROM meta WHERE id = 1), ?, ?, ?, ?, ?, ${HogliteSchema.HEAD_SQL})",
            ns.namespaceId,
            namespace,
            name,
            tableUuid.toString(),
            maxFieldId + 1,
        )
        insertColumnRows(tx, "(SELECT next_table_id - 1 FROM meta WHERE id = 1)", assigned)
        tx.add(
            "INSERT INTO table_versions(table_id, namespace_id, namespace, name, begin_snapshot) " +
                "VALUES ((SELECT next_table_id - 1 FROM meta WHERE id = 1), ?, ?, ?, ${HogliteSchema.HEAD_SQL})",
            ns.namespaceId,
            namespace,
            name,
        )
        tx.add(
            "INSERT INTO table_totals(table_id) VALUES ((SELECT next_table_id - 1 FROM meta WHERE id = 1))",
        )
        tx.add(
            "INSERT INTO changes(snapshot_id, kind, object_id) " +
                "VALUES (${HogliteSchema.HEAD_SQL}, 'table_created', " +
                "(SELECT next_table_id - 1 FROM meta WHERE id = 1))",
        )
        tx.insertSnapshotRow(author, message)
        val readback = tx.add(HogliteSql.READBACK_DDL)
        val results = runTx(catalog, freshTxid("create-table"), null, tx)
        val row = results[readback].firstRow()!!
        val snapshotId = row[0].asLong()
        return TableInfo(
            tableId = row[2].asLong(),
            tableUuid = tableUuid,
            namespace = namespace,
            name = name,
            columns = assigned,
            readSnapshotId = snapshotId,
            recordCount = 0,
            fileCount = 0,
            fileSizeBytes = 0,
            // snapshotId set ONLY here and in alter/creation receipts —
            // reads leave it null (the TableInfo KDoc's pyhoglake
            // contract).
            snapshotId = snapshotId,
        )
    }

    override fun getTable(
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long?,
        atTimestamp: Instant?,
        totals: Boolean,
    ): TableInfo {
        val m = meta(catalog)
        val at = resolveRead(catalog, m, snapshot, atTimestamp)
        val ns = liveNamespace(catalog, namespace)
        val t = tableAt(catalog, ns.namespaceId, namespace, table, at)
        val headRead = snapshot == null && atTimestamp == null
        var recordCount: Long? = null
        var fileCount: Long? = null
        var fileSizeBytes: Long? = null
        if (totals) {
            if (headRead) {
                // Exact, maintained transactionally by every commit — the
                // hoglite backend has no sampler lag, so head totals are
                // not a sample and carry no freshness fields (null, per
                // the TableInfo KDoc: freshness fields are for samples).
                val row =
                    queryCatalog(
                        catalog,
                        "SELECT record_count, file_count, file_size_bytes FROM table_totals WHERE table_id = ?",
                        t.tableId,
                    ).firstRow()
                recordCount = row?.get(0).asLongOrNull() ?: 0
                fileCount = row?.get(1).asLongOrNull() ?: 0
                fileSizeBytes = row?.get(2).asLongOrNull() ?: 0
            } else {
                val row =
                    queryCatalog(
                        catalog,
                        "SELECT COALESCE(SUM(record_count),0), COUNT(*), COALESCE(SUM(file_size_bytes),0) " +
                            "FROM files WHERE table_id = ? AND begin_snapshot <= ? " +
                            "AND (end_snapshot IS NULL OR end_snapshot > ?)",
                        t.tableId,
                        at,
                        at,
                    ).firstRow()!!
                recordCount = row[0].asLong()
                fileCount = row[1].asLong()
                fileSizeBytes = row[2].asLong()
            }
        }
        return TableInfo(
            tableId = t.tableId,
            tableUuid = t.tableUuid,
            namespace = namespace,
            name = table,
            columns = columnsAt(catalog, t.tableId, at),
            readSnapshotId = at,
            recordCount = recordCount,
            fileCount = fileCount,
            fileSizeBytes = fileSizeBytes,
            partitionSpec = specAt(catalog, t.tableId, at),
            sortSpec = sortSpecAt(catalog, t.tableId, at),
            comment = t.comment,
            properties = t.properties,
        )
    }

    override fun listTables(
        catalog: String,
        namespace: String,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): List<TableSummaryInfo> {
        val m = meta(catalog)
        val at = resolveRead(catalog, m, snapshot, atTimestamp)
        val ns = liveNamespace(catalog, namespace)
        // snapshotCount/earliestSnapshotId count RETAINED change rows, so
        // the window opens at the expiry floor — read from meta so the
        // day expiry lands the read is already right.
        val rows =
            queryCatalog(
                catalog,
                """SELECT t.id, t.table_uuid, v.name, v.comment,
                          COALESCE(tt.record_count,0), COALESCE(tt.file_count,0), COALESCE(tt.file_size_bytes,0),
                          (SELECT COUNT(DISTINCT c.snapshot_id) FROM changes c
                            WHERE c.object_id = t.id AND c.snapshot_id >= ? AND c.snapshot_id <= ?),
                          (SELECT MIN(c.snapshot_id) FROM changes c
                            WHERE c.object_id = t.id AND c.snapshot_id >= ? AND c.snapshot_id <= ?)
                     FROM tables t
                     JOIN table_versions v ON v.table_id = t.id
                      AND v.begin_snapshot <= ? AND (v.end_snapshot IS NULL OR v.end_snapshot > ?)
                     LEFT JOIN table_totals tt ON tt.table_id = t.id
                    WHERE v.namespace_id = ?
                      AND t.created_snapshot <= ? AND (t.dropped_snapshot IS NULL OR t.dropped_snapshot > ?)
                    ORDER BY v.name""",
                m.earliestSnapshotId,
                at,
                m.earliestSnapshotId,
                at,
                at,
                at,
                ns.namespaceId,
                at,
                at,
            )
        return rows.rows.map { r ->
            TableSummaryInfo(
                tableId = r[0].asLong(),
                tableUuid = UUID.fromString(r[1].asString()),
                name = r[2].asString(),
                comment = r[3] as? String,
                recordCount = r[4].asLong(),
                fileCount = r[5].asLong(),
                fileSizeBytes = r[6].asLong(),
                snapshotCount = r[7].asLong(),
                earliestSnapshotId = r[8].asLongOrNull(),
            )
        }
    }

    override fun dropTable(
        catalog: String,
        namespace: String,
        table: String,
        expectedTableUuid: UUID?,
        author: String?,
        message: String?,
    ): CommitResult {
        requireOperationMetadata(author, message)
        val t = liveTable(catalog, namespace, table)
        if (expectedTableUuid != null && t.tableUuid != expectedTableUuid) {
            throw HoglakeException.TableRecreated(
                "table '$namespace.$table' was dropped and recreated: expected incarnation " +
                    "$expectedTableUuid, live incarnation is ${t.tableUuid}",
                table = "$namespace.$table",
                expectedTableUuid = expectedTableUuid,
                currentTableUuid = t.tableUuid,
            )
        }
        // THE WHOLE DROP is the mark: file rows stay live on the dropped
        // table and RetirementService ends + queues them in paced batches
        // once the drop sinks under the expiry floor (hoglake #193:
        // ending 3M file rows under the commit lock was an outage).
        val tx = HogliteTx()
        tx.expectOne(
            "SELECT COUNT(*) FROM tables WHERE id = ? AND dropped_snapshot IS NULL",
            t.tableId,
        ) { HoglakeException.NotFound("table '$namespace.$table' in catalog '$catalog'") }
        tx.allocateDdlSnapshot()
        tx.affectOne(
            "UPDATE tables SET dropped_snapshot = ${HogliteSchema.HEAD_SQL} WHERE id = ?",
            t.tableId,
        ) { HoglakeException.NotFound("table '$namespace.$table' in catalog '$catalog'") }
        tx.affectOne(
            "UPDATE table_versions SET end_snapshot = ${HogliteSchema.HEAD_SQL} " +
                "WHERE table_id = ? AND end_snapshot IS NULL",
            t.tableId,
        ) { HoglakeException.NotFound("table '$namespace.$table' in catalog '$catalog'") }
        tx.add(
            "INSERT INTO changes(snapshot_id, kind, object_id) " +
                "VALUES (${HogliteSchema.HEAD_SQL}, 'table_dropped', ?)",
            t.tableId,
        )
        tx.insertSnapshotRow(author, message)
        val readback = tx.add(HogliteSql.READBACK_DDL)
        val results = runTx(catalog, freshTxid("drop-table"), null, tx)
        val row = results[readback].firstRow()!!
        return CommitResult(snapshotId = row[0].asLong(), schemaVersion = row[1].asLong())
    }

    override fun truncateTable(
        catalog: String,
        namespace: String,
        table: String,
        expectedTableUuid: UUID,
        author: String?,
        message: String?,
    ): CommitResult {
        requireOperationMetadata(author, message)
        val t = liveTable(catalog, namespace, table)
        if (t.tableUuid != expectedTableUuid) {
            throw HoglakeException.TableRecreated(
                "table '$namespace.$table' was dropped and recreated: expected incarnation " +
                    "$expectedTableUuid, live incarnation is ${t.tableUuid}",
                table = "$namespace.$table",
                expectedTableUuid = expectedTableUuid,
                currentTableUuid = t.tableUuid,
            )
        }
        val tx = HogliteTx()
        tx.expectOne(
            "SELECT COUNT(*) FROM tables WHERE id = ? AND dropped_snapshot IS NULL AND table_uuid = ?",
            t.tableId,
            expectedTableUuid.toString(),
        ) {
            HoglakeException.TableRecreated(
                "table '$namespace.$table' was recreated under the truncate",
                table = "$namespace.$table",
                expectedTableUuid = expectedTableUuid,
                currentTableUuid = t.tableUuid,
            )
        }
        tx.allocateDdlSnapshot()
        // End every live file and DV at the new snapshot and zero the
        // totals. The OBJECTS are not queued for removal here — a
        // retained pre-truncate snapshot still references them; the
        // expiry loop queues removals once the floor passes the end
        // snapshot (same as the Postgres path, where truncate only
        // calls endLiveFiles/endLiveDeleteFiles). The row-id allocator
        // stays: truncation must never reuse historical row ids.
        tx.add(
            "UPDATE dvs SET end_snapshot = ${HogliteSchema.HEAD_SQL} WHERE table_id = ? AND end_snapshot IS NULL",
            t.tableId,
        )
        tx.add(
            "UPDATE files SET end_snapshot = ${HogliteSchema.HEAD_SQL} WHERE table_id = ? AND end_snapshot IS NULL",
            t.tableId,
        )
        tx.add(
            "UPDATE table_totals SET record_count = 0, file_count = 0, file_size_bytes = 0 WHERE table_id = ?",
            t.tableId,
        )
        // The changefeed barrier pair (hoglake writes the same two rows):
        // 'table_altered' + 'table_deleted_from' in one snapshot is what
        // changes() refuses to represent — a consumer must reconcile
        // from a full scan.
        tx.add(
            "INSERT INTO changes(snapshot_id, kind, object_id) VALUES (${HogliteSchema.HEAD_SQL}, 'table_altered', ?)",
            t.tableId,
        )
        tx.add(
            "INSERT INTO changes(snapshot_id, kind, object_id) " +
                "VALUES (${HogliteSchema.HEAD_SQL}, 'table_deleted_from', ?)",
            t.tableId,
        )
        tx.insertSnapshotRow(author, message)
        val readback = tx.add(HogliteSql.READBACK_DDL)
        val results = runTx(catalog, freshTxid("truncate"), null, tx)
        val row = results[readback].firstRow()!!
        return CommitResult(snapshotId = row[0].asLong(), schemaVersion = row[1].asLong())
    }

    // ==================== files + changefeed ===============================

    override fun listFiles(
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long?,
        atTimestamp: Instant?,
        sort: FileRepo.FileSortColumn?,
        desc: Boolean,
        limit: Int?,
        offset: Int,
        partitionFilter: Map<Int, String>,
    ): List<DataFile> {
        val m = meta(catalog)
        val at = resolveRead(catalog, m, snapshot, atTimestamp)
        val ns = liveNamespace(catalog, namespace)
        val t = tableAt(catalog, ns.namespaceId, namespace, table, at)
        if (partitionFilter.isNotEmpty()) {
            // A filter on a key the table does not partition by at `at`
            // can only be a client bug (or a stale spec), so refuse it
            // rather than return an empty page that reads as "no files".
            val arity = specAt(catalog, t.tableId, at)?.fields?.size ?: 0
            val bad = partitionFilter.keys.firstOrNull { it < 0 || it >= arity }
            if (bad != null) {
                throw HoglakeException.Validation(
                    "partition key_index $bad is out of range for table '$namespace.$table' ($arity partition keys)",
                )
            }
        }
        val dir = if (desc) "DESC" else "ASC"
        val orderBy =
            if (sort == null) {
                "begin_snapshot, row_id_start, id"
            } else {
                // Fixed ascending id tiebreak: a stable total order under
                // paging regardless of the primary direction.
                "${sortColumnSql(sort)} $dir, id ASC"
            }
        val partitionWhere =
            partitionFilter.entries.sortedBy { it.key }.joinToString("") { (keyIndex, _) ->
                " AND EXISTS (SELECT 1 FROM json_each(f.partition_values_json) pv " +
                    "WHERE pv.key = $keyIndex AND pv.value = ?)"
            }
        val sql =
            "SELECT ${HogliteSql.FILE_COLUMNS} FROM files f WHERE f.table_id = ? " +
                "AND f.begin_snapshot <= ? AND (f.end_snapshot IS NULL OR f.end_snapshot > ?)" +
                partitionWhere + " ORDER BY $orderBy" +
                if (limit != null) " LIMIT ? OFFSET ?" else ""
        val args =
            buildList<Any?> {
                add(t.tableId)
                add(at)
                add(at)
                partitionFilter.entries.sortedBy { it.key }.forEach { add(it.value) }
                if (limit != null) {
                    add(limit)
                    add(offset)
                }
            }
        val page =
            queryCatalog(catalog, sql, *args.toTypedArray()).rows.map { toFileRow(it) }
        return withOrderingBounds(catalog, t.tableId, at, page)
    }

    override fun partitionValues(
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): PartitionValues {
        val m = meta(catalog)
        val at = resolveRead(catalog, m, snapshot, atTimestamp)
        val ns = liveNamespace(catalog, namespace)
        val t = tableAt(catalog, ns.namespaceId, namespace, table, at)
        val spec = specAt(catalog, t.tableId, at) ?: return PartitionValues(-1, emptyList())
        val fields =
            spec.fields.mapIndexed { keyIndex, field ->
                val cap = PartitionValues.PARTITION_VALUES_CAP
                // One past the cap, so "there are more" is
                // distinguishable from "exactly cap values".
                val rows =
                    queryCatalog(
                        catalog,
                        "SELECT pv.value, COUNT(*) AS n FROM files f, json_each(f.partition_values_json) pv " +
                            "WHERE f.table_id = ? AND f.begin_snapshot <= ? " +
                            "AND (f.end_snapshot IS NULL OR f.end_snapshot > ?) " +
                            "AND pv.key = ? GROUP BY pv.value ORDER BY n DESC, pv.value LIMIT ?",
                        t.tableId,
                        at,
                        at,
                        keyIndex,
                        cap + 1,
                    )
                PartitionFieldValues(
                    sourceFieldId = field.sourceFieldId,
                    transform = field.transform,
                    transformParam = field.transformParam,
                    values = rows.rows.take(cap).map { it[0] as? String },
                    truncated = rows.rows.size > cap,
                )
            }
        return PartitionValues(spec.specId, fields)
    }

    override fun fileStats(
        catalog: String,
        namespace: String,
        table: String,
        fileId: Long,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): FileStats {
        val m = meta(catalog)
        val at = resolveRead(catalog, m, snapshot, atTimestamp)
        val ns = liveNamespace(catalog, namespace)
        val t = tableAt(catalog, ns.namespaceId, namespace, table, at)
        val row =
            queryCatalog(
                catalog,
                "SELECT ${HogliteSql.FILE_COLUMNS} FROM files f " +
                    "WHERE f.id = ? AND f.table_id = ? AND f.begin_snapshot <= ? " +
                    "AND (f.end_snapshot IS NULL OR f.end_snapshot > ?)",
                fileId,
                t.tableId,
                at,
                at,
            ).firstRow()
                ?: throw HoglakeException.NotFound(
                    "file $fileId in table '$namespace.$table' in catalog '$catalog' at snapshot $at",
                )
        val file = toFileRow(row)
        val columns = columnsAt(catalog, t.tableId, at)
        return FileStats(
            dataFileId = file.file.dataFileId,
            statsState = file.file.statsState,
            columns = resolveStoredStats(file, columns, null),
        )
    }

    override fun changes(
        catalog: String,
        namespace: String,
        table: String,
        fromSnapshot: Long,
        toSnapshot: Long?,
    ): ChangesPlan {
        val m = meta(catalog)
        val to =
            when {
                toSnapshot == null -> m.head
                toSnapshot > m.head ->
                    throw HoglakeException.Validation(
                        "to_snapshot $toSnapshot is beyond head ${m.head}",
                    )
                else -> toSnapshot
            }
        if (fromSnapshot < 0) throw HoglakeException.Validation("from_snapshot must be >= 0, got $fromSnapshot")
        if (fromSnapshot > to) {
            throw HoglakeException.Validation("from_snapshot $fromSnapshot is beyond to_snapshot $to")
        }
        if (fromSnapshot < m.earliestSnapshotId - 1) {
            throw HoglakeException.Expired(
                "from_snapshot $fromSnapshot reaches below the expiry floor " +
                    "(earliest retained snapshot is ${m.earliestSnapshotId}); part of the range is " +
                    "gone; reconcile by re-reading from a full scan at a retained snapshot",
            )
        }
        val ns = liveNamespace(catalog, namespace)
        val t = tableAt(catalog, ns.namespaceId, namespace, table, to)
        // TRUNCATE / replacement barrier: a window that crosses a
        // table_altered + table_deleted_from pair cannot be represented
        // by an append-oriented feed.
        val truncated =
            queryCatalog(
                catalog,
                "SELECT c.snapshot_id FROM changes c WHERE c.object_id = ? AND c.kind = 'table_deleted_from' " +
                    "AND c.snapshot_id > ? AND c.snapshot_id <= ? AND EXISTS (" +
                    "SELECT 1 FROM changes d WHERE d.object_id = c.object_id " +
                    "AND d.snapshot_id = c.snapshot_id AND d.kind = 'table_altered') " +
                    "ORDER BY c.snapshot_id LIMIT 1",
                t.tableId,
                fromSnapshot,
                to,
            ).firstRow()
        if (truncated != null) {
            throw HoglakeException.ReconciliationRequired(
                "table '$namespace.$table' was truncated or replaced at snapshot ${truncated[0].asLong()}; " +
                    "changefeed window ($fromSnapshot, $to] cannot represent this deletion. " +
                    "Reconcile the destination from a full snapshot before advancing its checkpoint.",
            )
        }
        val files =
            queryCatalog(
                catalog,
                "SELECT ${HogliteSql.FILE_COLUMNS} FROM files f " +
                    "WHERE f.table_id = ? AND f.begin_snapshot > ? AND f.begin_snapshot <= ? " +
                    "ORDER BY f.begin_snapshot, f.id",
                t.tableId,
                fromSnapshot,
                to,
            ).rows.map { toDataFile(it) }
        val dvs =
            queryCatalog(
                catalog,
                "SELECT delete_file_id, data_file_id, path, delete_count, file_size_bytes, begin_snapshot " +
                    "FROM dvs WHERE table_id = ? AND begin_snapshot > ? AND begin_snapshot <= ? " +
                    "ORDER BY begin_snapshot, delete_file_id",
                t.tableId,
                fromSnapshot,
                to,
            ).rows.map { r ->
                DeleteFile(
                    deleteFileId = r[0].asLong(),
                    dataFileId = r[1].asLong(),
                    path = r[2].asString(),
                    fileFormat = "puffin",
                    deleteCount = r[3].asLong(),
                    fileSizeBytes = r[4].asLong(),
                    beginSnapshot = r[5].asLong(),
                )
            }
        return ChangesPlan(t.tableUuid, fromSnapshot, to, files, dvs)
    }

    // ==================== snapshots ========================================

    override fun resolveSnapshot(
        catalog: String,
        atTimestamp: Instant,
    ): SnapshotResolution {
        val m = meta(catalog)
        val at = resolveTimestamp(catalog, m, atTimestamp)
        val ts =
            queryCatalog(catalog, "SELECT ts FROM snapshots WHERE id = ?", at)
                .firstRow()!![0].asString()
        return SnapshotResolution(at, HogliteSchema.parse(ts))
    }

    override fun listSnapshots(
        catalog: String,
        after: Long,
        limit: Int,
        before: Long?,
    ): Pair<List<Snapshot>, Boolean> {
        if (limit < 1) throw HoglakeException.Validation("limit must be >= 1, got $limit")
        if (before != null && after != 0L) {
            throw HoglakeException.Validation(
                "'before' and a non-zero 'after' are mutually exclusive; supply at most one cursor",
            )
        }
        meta(catalog)
        // limit + 1 rows: the extra one answers "are there more".
        val rows =
            if (before != null) {
                queryCatalog(
                    catalog,
                    "SELECT id, ts, schema_version, author, message FROM snapshots " +
                        "WHERE id < ? ORDER BY id DESC LIMIT ?",
                    before,
                    limit + 1,
                )
            } else {
                queryCatalog(
                    catalog,
                    "SELECT id, ts, schema_version, author, message FROM snapshots " +
                        "WHERE id > ? ORDER BY id ASC LIMIT ?",
                    after,
                    limit + 1,
                )
            }
        val hasMore = rows.rows.size > limit
        val page = rows.rows.take(limit)
        if (page.isEmpty()) return Pair(emptyList(), false)
        val low = minOf(page.first()[0].asLong(), page.last()[0].asLong())
        val high = maxOf(page.first()[0].asLong(), page.last()[0].asLong())
        val changes =
            queryCatalog(
                catalog,
                "SELECT snapshot_id, kind, object_id FROM changes WHERE snapshot_id >= ? AND snapshot_id <= ? " +
                    "ORDER BY snapshot_id, kind, object_id",
                low,
                high,
            ).rows.groupBy({ it[0].asLong() }) { SnapshotChange(ChangeKind.fromWire(it[1].asString()), it[2].asLong()) }
        return Pair(
            page.map { r ->
                Snapshot(
                    snapshotId = r[0].asLong(),
                    snapshotTime = HogliteSchema.parse(r[1].asString()),
                    schemaVersion = r[2].asLong(),
                    author = r[3] as? String,
                    message = r[4] as? String,
                    changes = changes[r[0].asLong()] ?: emptyList(),
                )
            },
            hasMore,
        )
    }

    // ==================== read planning ====================================

    override fun planScan(
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long?,
        atTimestamp: Instant?,
        columnStats: ScanService.ColumnStatsRequest?,
        splitOffsets: Boolean,
    ): List<ScanFile> {
        val m = meta(catalog)
        val at = resolveRead(catalog, m, snapshot, atTimestamp)
        val ns = liveNamespace(catalog, namespace)
        val t = tableAt(catalog, ns.namespaceId, namespace, table, at)
        // The DV join picks the newest epoch VISIBLE AT the pin: history-
        // preserving supersession means a pinned scan sees the vector
        // that was live then.
        val rows =
            queryCatalog(
                catalog,
                """SELECT ${HogliteSql.FILE_COLUMNS},
                          d.delete_file_id, d.path, d.delete_count, d.file_size_bytes, d.begin_snapshot
                     FROM files f
                     LEFT JOIN dvs d ON d.data_file_id = f.id
                       AND d.begin_snapshot <= ? AND (d.end_snapshot IS NULL OR d.end_snapshot > ?)
                       AND d.epoch = (SELECT MAX(d2.epoch) FROM dvs d2
                                       WHERE d2.data_file_id = f.id AND d2.begin_snapshot <= ?
                                         AND (d2.end_snapshot IS NULL OR d2.end_snapshot > ?))
                    WHERE f.table_id = ? AND f.begin_snapshot <= ?
                      AND (f.end_snapshot IS NULL OR f.end_snapshot > ?)
                    ORDER BY f.row_id_start, f.id""",
                at,
                at,
                at,
                at,
                t.tableId,
                at,
                at,
            )
        var files =
            rows.rows.map { r ->
                val fr = toFileRow(r)
                val dv =
                    if (r[15] == null) {
                        null
                    } else {
                        DeleteFile(
                            deleteFileId = r[15].asLong(),
                            dataFileId = fr.file.dataFileId,
                            path = r[16].asString(),
                            fileFormat = "puffin",
                            deleteCount = r[17].asLong(),
                            fileSizeBytes = r[18].asLong(),
                            beginSnapshot = r[19].asLong(),
                        )
                    }
                ScanFile(fr.file, dv) to fr
            }
        if (columnStats != null) {
            val columns = columnsAt(catalog, t.tableId, at)
            // Variant columns and containers never carry stats rows; the
            // cap counts REQUESTED VISIBLE LEAVES per file.
            val leaves =
                columns.flatMap { it.selfAndDescendants() }
                    .filter { it.def.type != ColType.VARIANT && !it.def.type.isNested }
            val requested =
                columnStats.fieldIds?.let { ids -> leaves.filter { it.fieldId in ids } } ?: leaves
            val entries = files.size.toLong() * requested.size
            if (entries > ScanService.SCAN_COLUMN_STATS_MAX_ENTRIES) {
                throw HoglakeException.Validation(
                    "column_stats for ${files.size} files x ${requested.size} requested column(s) is " +
                        "$entries entries, over the maximum ${ScanService.SCAN_COLUMN_STATS_MAX_ENTRIES}",
                )
            }
            val wanted = requested.map { it.fieldId }.toSet()
            files =
                files.map { (sf, fr) ->
                    sf.copy(dataFile = sf.dataFile.copy(columnStats = resolveStoredStats(fr, columns, wanted))) to fr
                }
        }
        if (splitOffsets) {
            files =
                files.map { (sf, fr) ->
                    val offsets = fr.splitOffsetsJson?.let { json.readValue<List<Long>>(it) }
                    sf.copy(dataFile = sf.dataFile.copy(splitOffsets = offsets)) to fr
                }
        }
        return files.map { it.first }
    }

    // ==================== consumer offsets =================================

    override fun listConsumers(catalog: String): List<ConsumerTableOffset> {
        meta(catalog)
        return queryCatalog(
            catalog,
            """SELECT o.consumer_id, o.table_uuid, o.committed_snapshot, o.updated_at,
                      t.namespace, t.name, CASE WHEN t.dropped_snapshot IS NULL THEN 0 ELSE 1 END
                 FROM offsets o LEFT JOIN tables t ON t.table_uuid = o.table_uuid
                ORDER BY o.consumer_id, o.table_uuid""",
        ).rows.map { r ->
            ConsumerTableOffset(
                consumerId = r[0].asString(),
                tableUuid = UUID.fromString(r[1].asString()),
                committedSnapshot = r[2].asLong(),
                updatedAt = HogliteSchema.parse(r[3].asString()),
                namespace = r[4] as? String,
                tableName = r[5] as? String,
                tableDropped = r[6].asLongOrNull() == 1L,
            )
        }
    }

    override fun listOffsets(
        catalog: String,
        consumerId: String,
    ): List<ConsumerOffset> {
        meta(catalog)
        return queryCatalog(
            catalog,
            "SELECT consumer_id, table_uuid, committed_snapshot, updated_at FROM offsets " +
                "WHERE consumer_id = ? ORDER BY table_uuid",
            consumerId,
        ).rows.map { r ->
            ConsumerOffset(
                consumerId = r[0].asString(),
                tableUuid = UUID.fromString(r[1].asString()),
                committedSnapshot = r[2].asLong(),
                updatedAt = HogliteSchema.parse(r[3].asString()),
            )
        }
    }

    override fun getOffset(
        catalog: String,
        consumerId: String,
        tableUuid: UUID,
    ): ConsumerOffset {
        meta(catalog)
        val row =
            queryCatalog(
                catalog,
                "SELECT consumer_id, table_uuid, committed_snapshot, updated_at FROM offsets " +
                    "WHERE consumer_id = ? AND table_uuid = ?",
                consumerId,
                tableUuid.toString(),
            ).firstRow()
                ?: throw HoglakeException.NotFound(
                    "no offset for consumer '$consumerId' on table $tableUuid in catalog '$catalog'",
                )
        return ConsumerOffset(
            consumerId = row[0].asString(),
            tableUuid = UUID.fromString(row[1].asString()),
            committedSnapshot = row[2].asLong(),
            updatedAt = HogliteSchema.parse(row[3].asString()),
        )
    }

    override fun commitOffset(
        catalog: String,
        consumerId: String,
        tableUuid: UUID,
        snapshotId: Long,
    ): ConsumerOffset {
        // Postgres enforces the consumer id's 1..128 length as a CHECK;
        // here it is a typed refusal instead of a SQL error.
        if (consumerId.isEmpty() || consumerId.length > 128) {
            throw HoglakeException.Validation("consumer_id must be 1..128 characters, got ${consumerId.length}")
        }
        val m = meta(catalog)
        if (snapshotId < 0 || snapshotId > m.head) {
            throw HoglakeException.Validation("snapshot $snapshotId out of range [0, ${m.head}]")
        }
        // The floor guard (mirrors every read path's 410 contract): an
        // offset below earliest_snapshot_id is a position the consumer
        // can never read from.
        if (snapshotId < m.earliestSnapshotId) {
            throw HoglakeException.Expired(
                "cannot commit offset at snapshot $snapshotId: it is below the expiry floor " +
                    "(earliest retained snapshot is ${m.earliestSnapshotId}); reconcile from a " +
                    "full scan at a retained snapshot",
            )
        }
        // The uuid must name a table this catalog has EVER had — any
        // incarnation, dropped included (offsets deliberately survive
        // drops so consumers SEE incarnation changes).
        val known =
            queryCatalog(catalog, "SELECT COUNT(*) FROM tables WHERE table_uuid = ?", tableUuid.toString())
                .firstRow()!![0].asLong() > 0
        if (!known) {
            throw HoglakeException.Validation("unknown table_uuid $tableUuid in catalog '$catalog'")
        }
        val now = HogliteSchema.now()
        val tx = HogliteTx()
        // Monotonic: never move BACKWARDS. An EQUAL re-commit is the
        // Postgres upsert's `<= EXCLUDED` arm — accepted, a no-op — which
        // is what makes a client retry of a timed-out-but-applied call
        // safe (each call carries a fresh txid).
        tx.expectZero(
            "SELECT COUNT(*) FROM offsets WHERE consumer_id = ? AND table_uuid = ? AND committed_snapshot > ?",
            consumerId,
            tableUuid.toString(),
            snapshotId,
        ) {
            HoglakeException.OffsetRegression(
                "consumer '$consumerId' already committed past snapshot $snapshotId for table $tableUuid",
            )
        }
        tx.add(
            "INSERT INTO offsets(consumer_id, table_uuid, committed_snapshot, updated_at) VALUES (?, ?, ?, ?) " +
                "ON CONFLICT(consumer_id, table_uuid) DO UPDATE SET " +
                "committed_snapshot = excluded.committed_snapshot, updated_at = excluded.updated_at",
            consumerId,
            tableUuid.toString(),
            snapshotId,
            now,
        )
        // Reaching an incarnation at or past the snapshot that created it
        // is the consumer's statement that it has reconciled across the
        // replacement: release its offsets on the incarnations this one
        // replaced (they would otherwise pin the consumer_floor forever).
        tx.add(
            """WITH RECURSIVE ancestors(id, uuid, created, replaced) AS (
                 SELECT t.id, t.table_uuid, t.created_snapshot, t.replaced_table_id FROM tables t
                  WHERE t.table_uuid = ? AND ? >= t.created_snapshot
                 UNION ALL
                 SELECT a.id, a.table_uuid, a.created_snapshot, a.replaced_table_id
                   FROM ancestors l JOIN tables a
                     ON a.id = l.replaced AND a.created_snapshot < l.created
               )
               DELETE FROM offsets WHERE consumer_id = ? AND table_uuid <> ?
                 AND table_uuid IN (SELECT uuid FROM ancestors)""",
            tableUuid.toString(),
            snapshotId,
            consumerId,
            tableUuid.toString(),
        )
        runTx(catalog, freshTxid("offset"), null, tx)
        return ConsumerOffset(consumerId, tableUuid, snapshotId, HogliteSchema.parse(now))
    }

    // ==================== commits ==========================================

    override fun commit(
        catalog: String,
        request: CommitRequest,
        userAgent: String?,
    ): CommitResult = HogliteCommit.commit(this, catalog, request)

    override fun receipt(
        catalog: String,
        operation: UUID,
    ): CommitResult = HogliteCommit.receipt(this, catalog, operation)

    // ==================== shared internals =================================

    /** The one meta row of a catalog tenant. */
    internal class MetaRow(
        val name: String,
        val dataPath: String,
        val head: Long,
        val schemaVersion: Long,
        val earliestSnapshotId: Long,
        val earliestSnapshotTime: Instant?,
        val retentionSeconds: Long?,
        val consumerFloor: Boolean,
    ) {
        fun toCatalogInfo(): CatalogInfo =
            CatalogInfo(
                // hoglite tenants have no numeric id; the field is
                // internal-only, so a stable name hash keeps logs that
                // print it distinguishable without inventing a sequence.
                catalogId = name.hashCode().toLong() and 0x7fffffff,
                name = name,
                dataPath = dataPath,
                headSnapshotId = head,
                schemaVersion = schemaVersion,
                earliestSnapshotTime = earliestSnapshotTime,
                snapshotRetentionSeconds = retentionSeconds,
                consumerFloor = consumerFloor,
                earliestSnapshotId = earliestSnapshotId,
            )
    }

    internal fun meta(catalog: String): MetaRow {
        val row =
            queryCatalog(
                catalog,
                "SELECT name, data_path, next_snapshot - 1, schema_version, earliest_snapshot_id, " +
                    "earliest_snapshot_time, snapshot_retention_seconds, consumer_floor FROM meta WHERE id = 1",
            ).firstRow()
                ?: throw HoglakeException.NotFound("catalog '$catalog'")
        return MetaRow(
            name = row[0].asString(),
            dataPath = row[1].asString(),
            head = row[2].asLong(),
            schemaVersion = row[3].asLong(),
            earliestSnapshotId = row[4].asLong(),
            earliestSnapshotTime = (row[5] as? String)?.let { HogliteSchema.parse(it) },
            retentionSeconds = row[6].asLongOrNull(),
            consumerFloor = row[7].asLong() != 0L,
        )
    }

    /** Resolve the read snapshot for a read path, with hoglake's guard order and typed refusals. */
    internal fun resolveRead(
        catalog: String,
        m: MetaRow,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): Long {
        if (snapshot != null && atTimestamp != null) {
            throw HoglakeException.Validation("snapshot and at_timestamp are mutually exclusive")
        }
        if (snapshot != null) {
            if (snapshot < 0) throw HoglakeException.Validation("snapshot must be >= 0, got $snapshot")
            if (snapshot > m.head) {
                throw HoglakeException.Validation("snapshot $snapshot is ahead of head ${m.head}")
            }
            if (snapshot < m.earliestSnapshotId) {
                throw HoglakeException.Expired(
                    "snapshot $snapshot is below the expiry floor " +
                        "(earliest retained snapshot is ${m.earliestSnapshotId})",
                )
            }
            return snapshot
        }
        if (atTimestamp != null) return resolveTimestamp(catalog, m, atTimestamp)
        return m.head
    }

    /** Largest retained snapshot with snapshot_time <= [atTimestamp]; before the floor -> Expired. */
    internal fun resolveTimestamp(
        catalog: String,
        m: MetaRow,
        atTimestamp: Instant,
    ): Long =
        queryCatalog(
            catalog,
            "SELECT id FROM snapshots WHERE ts <= ? ORDER BY ts DESC, id DESC LIMIT 1",
            HogliteSchema.fmt(atTimestamp),
        ).firstRow()?.get(0)?.asLong()
            ?: throw HoglakeException.Expired(
                "no retained snapshot at or before $atTimestamp " +
                    "(earliest retained snapshot is ${m.earliestSnapshotId})",
            )

    internal fun liveNamespace(
        catalog: String,
        namespace: String,
    ): NamespaceInfo =
        queryCatalog(
            catalog,
            "SELECT id, name FROM namespaces WHERE name = ? AND dropped_snapshot IS NULL",
            namespace,
        ).firstRow()?.let { NamespaceInfo(it[0].asLong(), it[1].asString()) }
            ?: throw HoglakeException.NotFound("namespace '$namespace' in catalog '$catalog'")

    /** A live table row as the store uses one. */
    internal class TableRow(
        val tableId: Long,
        val tableUuid: UUID,
        val namespaceId: Long,
        val namespace: String,
        val name: String,
        val comment: String?,
        val properties: Map<String, String>,
        val nextRowId: Long,
    )

    /** Resolve (namespace, name) at [at]: the VERSION visible then, of an incarnation alive then. */
    internal fun tableAt(
        catalog: String,
        namespaceId: Long,
        namespace: String,
        table: String,
        at: Long,
    ): TableRow {
        val row =
            queryCatalog(
                catalog,
                "SELECT t.id, t.table_uuid, v.namespace_id, v.namespace, v.name, v.comment, " +
                    "v.properties_json, t.next_row_id FROM table_versions v " +
                    "JOIN tables t ON t.id = v.table_id " +
                    "WHERE v.namespace_id = ? AND v.name = ? " +
                    "AND v.begin_snapshot <= ? AND (v.end_snapshot IS NULL OR v.end_snapshot > ?) " +
                    "AND t.created_snapshot <= ? AND (t.dropped_snapshot IS NULL OR t.dropped_snapshot > ?)",
                namespaceId,
                table,
                at,
                at,
                at,
                at,
            ).firstRow()
                ?: throw HoglakeException.NotFound(
                    "table '$namespace.$table' in catalog '$catalog' at snapshot $at",
                )
        return tableRowOf(row)
    }

    internal fun liveTable(
        catalog: String,
        namespace: String,
        table: String,
    ): TableRow {
        val ns = liveNamespace(catalog, namespace)
        val row =
            queryCatalog(
                catalog,
                "SELECT t.id, t.table_uuid, v.namespace_id, v.namespace, v.name, v.comment, " +
                    "v.properties_json, t.next_row_id FROM table_versions v " +
                    "JOIN tables t ON t.id = v.table_id " +
                    "WHERE v.namespace_id = ? AND v.name = ? AND v.end_snapshot IS NULL " +
                    "AND t.dropped_snapshot IS NULL",
                ns.namespaceId,
                table,
            ).firstRow()
                ?: throw HoglakeException.NotFound("table '$namespace.$table' in catalog '$catalog'")
        return tableRowOf(row)
    }

    @Suppress("UNCHECKED_CAST")
    private fun tableRowOf(row: List<Any?>): TableRow =
        TableRow(
            tableId = row[0].asLong(),
            tableUuid = UUID.fromString(row[1].asString()),
            namespaceId = row[2].asLong(),
            namespace = row[3].asString(),
            name = row[4].asString(),
            comment = row[5] as? String,
            properties = json.readValue<Map<String, String>>(row[6].asString()),
            nextRowId = row[7].asLong(),
        )

    /** One column row as read back, before the tree is rebuilt. */
    private class ColumnRow(
        val fieldId: Long,
        val parentFieldId: Long?,
        val ordinal: Int,
        val name: String,
        val type: ColType,
        val typeParams: Map<String, Any?>?,
        val nullable: Boolean,
        val comment: String?,
    )

    /** The column forest visible at [at], parents before children. */
    internal fun columnsAt(
        catalog: String,
        tableId: Long,
        at: Long,
    ): List<Column> {
        val rows =
            queryCatalog(
                catalog,
                "SELECT field_id, parent_field_id, ordinal, name, type, type_params_json, nullable, comment " +
                    "FROM columns WHERE table_id = ? AND begin_snapshot <= ? " +
                    "AND (end_snapshot IS NULL OR end_snapshot > ?)",
                tableId,
                at,
                at,
            ).rows.map { r ->
                @Suppress("UNCHECKED_CAST")
                ColumnRow(
                    fieldId = r[0].asLong(),
                    parentFieldId = r[1].asLongOrNull(),
                    ordinal = r[2].asLong().toInt(),
                    name = r[3].asString(),
                    type = ColType.fromWire(r[4].asString()),
                    typeParams = (r[5] as? String)?.let { json.readValue<Map<String, Any?>>(it) },
                    nullable = r[6].asLong() != 0L,
                    comment = r[7] as? String,
                )
            }

        fun build(parent: Long?): List<Column> =
            rows.filter { it.parentFieldId == parent }.sortedBy { it.ordinal }.map { r ->
                Column(
                    fieldId = r.fieldId,
                    ordinal = r.ordinal,
                    def =
                        ColumnDef(
                            name = r.name,
                            type = r.type,
                            typeParams = r.typeParams,
                            nullable = r.nullable,
                            children = null,
                            comment = r.comment,
                        ),
                    children = build(r.fieldId),
                )
            }
        return build(null)
    }

    internal fun specAt(
        catalog: String,
        tableId: Long,
        at: Long,
    ): PartitionSpec? =
        queryCatalog(
            catalog,
            "SELECT spec_id, fields_json FROM partition_specs WHERE table_id = ? " +
                "AND begin_snapshot <= ? AND (end_snapshot IS NULL OR end_snapshot > ?) " +
                "ORDER BY spec_id DESC LIMIT 1",
            tableId,
            at,
            at,
        ).firstRow()?.let { r ->
            PartitionSpec(r[0].asLong(), json.readValue<List<PartitionFieldDef>>(r[1].asString()))
        }

    internal fun sortSpecAt(
        catalog: String,
        tableId: Long,
        at: Long,
    ): SortSpec? =
        queryCatalog(
            catalog,
            "SELECT spec_id, fields_json, begin_snapshot FROM sort_specs WHERE table_id = ? " +
                "AND begin_snapshot <= ? AND (end_snapshot IS NULL OR end_snapshot > ?) " +
                "ORDER BY spec_id DESC LIMIT 1",
            tableId,
            at,
            at,
        ).firstRow()?.let { r ->
            SortSpec(r[0].asLong(), json.readValue<List<SortFieldDef>>(r[1].asString()), r[2].asLong())
        }

    /** A file row plus its stored payloads, before per-endpoint projection. */
    internal class FileRow(
        val file: DataFile,
        val statsJson: String?,
        val splitOffsetsJson: String?,
    )

    internal fun toFileRow(r: List<Any?>): FileRow {
        val file =
            DataFile(
                dataFileId = r[0].asLong(),
                tableId = r[1].asLong(),
                path = r[2].asString(),
                fileFormat = r[3].asString(),
                recordCount = r[4].asLong(),
                fileSizeBytes = r[5].asLong(),
                footerSize = r[6].asLongOrNull(),
                rowIdStart = r[7].asLong(),
                explicitRowIds = r[8].asLong() != 0L,
                specId = r[9].asLongOrNull(),
                partitionValues = (r[10] as? String)?.let { json.readValue<List<String?>>(it) },
                statsState = StatsState.fromWire(r[11].asString()),
                beginSnapshot = r[14].asLong(),
            )
        return FileRow(file, r[12] as? String, r[13] as? String)
    }

    internal fun toDataFile(r: List<Any?>): DataFile = toFileRow(r).file

    /**
     * A file's stored stats rows resolved against the columns visible at
     * the read snapshot — one [FileColumnStats] per stored row whose
     * field id is a visible LEAF (and in [wanted], when given); a row
     * whose field id is not visible (a dropped column) is omitted.
     */
    internal fun resolveStoredStats(
        file: FileRow,
        columns: List<Column>,
        wanted: Set<Long>?,
    ): List<FileColumnStats> {
        val stored = file.statsJson?.let { json.readValue<List<ColumnStats>>(it) } ?: return emptyList()
        val byId = mutableMapOf<Long, Pair<Column, String>>()

        fun walk(
            col: Column,
            path: String,
        ) {
            val here = if (path.isEmpty()) col.def.name else "$path.${col.def.name}"
            byId[col.fieldId] = col to here
            col.children.forEach { walk(it, here) }
        }
        columns.forEach { walk(it, "") }
        return stored.mapNotNull { stat ->
            if (wanted != null && stat.fieldId !in wanted) return@mapNotNull null
            val (col, path) = byId[stat.fieldId] ?: return@mapNotNull null
            if (col.def.type == ColType.VARIANT || col.def.type.isNested) return@mapNotNull null
            FileColumnStats(
                fieldId = stat.fieldId,
                name = col.def.name,
                path = path,
                type = col.def.type,
                typeParams = col.def.typeParams,
                stats = stat,
            )
        }
    }

    /** Attach each listed file's ordering-key range (RowIds for an unsorted table, SortKey for a sorted one). */
    private fun withOrderingBounds(
        catalog: String,
        tableId: Long,
        at: Long,
        page: List<FileRow>,
    ): List<DataFile> {
        if (page.isEmpty()) return emptyList()
        val sort = sortSpecAt(catalog, tableId, at)
        if (sort == null || sort.fields.isEmpty()) {
            return page.map { fr ->
                val f = fr.file
                f.copy(
                    orderingBounds =
                        FileOrderingBounds.RowIds(
                            lower = f.rowIdStart,
                            // Explicit row ids: row_id_start is only
                            // min(input row ids); the upper bound is
                            // arithmetic on a meaning the file lacks.
                            upper = if (f.explicitRowIds) null else f.rowIdStart + f.recordCount - 1,
                        ),
                )
            }
        }
        val leadingFieldId = sort.fields.first().sourceFieldId
        val columns = columnsAt(catalog, tableId, at)
        return page.map { fr ->
            // A file with no stored stats for the sort key carries no
            // bound — null is "unknown", not a computed lie.
            val bound = resolveStoredStats(fr, columns, setOf(leadingFieldId)).firstOrNull()
            fr.file.copy(orderingBounds = bound?.let { FileOrderingBounds.SortKey(it) })
        }
    }

    // ---- backends over the same tenant -------------------------------------

    /** Upload claims (claim/renew/abandon/expiry sweep) on this store's tenant tables. */
    internal val uploads: HogliteUploads by lazy { HogliteUploads(this, ledgerRetentionSeconds) }

    /** Atomic table creations (prepare/publish/abort receipts) on this store's tenant tables. */
    internal val tableCreations: HogliteTableCreations by lazy { HogliteTableCreations(this) }

    /** ALTER TABLE on this store's tenant tables. */
    internal val alters: HogliteAlters by lazy { HogliteAlters(this) }
    internal val views: HogliteViews by lazy { HogliteViews(this) }

    /**
     * One table-creation receipt, with lazy expiry applied: a `prepared`
     * receipt past its expires_at is aborted with reason `expired` on
     * the touch (the Postgres path's `load`, verbatim).
     */
    internal fun loadCreationOrThrow(
        catalog: String,
        operationId: UUID,
    ): com.posthog.hoglake.service.TableCreation {
        val now = HogliteSchema.now()
        runTx(
            catalog,
            freshTxid("creation-expire"),
            null,
            HogliteTx().apply {
                add(
                    "UPDATE table_creations SET state = 'aborted', reason = 'expired' " +
                        "WHERE operation = ? AND state = 'prepared' AND expires_at <= ?",
                    operationId.toString(),
                    now,
                )
            },
        )
        val row =
            queryCatalog(
                catalog,
                "SELECT table_uuid, definition_json, write_path, state, expires_at, snapshot_id, " +
                    "schema_version, reason FROM table_creations WHERE operation = ?",
                operationId.toString(),
            ).firstRow()
                ?: throw HoglakeException.NotFound("table creation operation '$operationId'")
        val definition =
            com.posthog.hoglake.service.TableCreationDefinitionCodec.decode(
                row[1].asString(),
                "table creation operation $operationId",
            )
        return com.posthog.hoglake.service.TableCreation(
            operationId,
            UUID.fromString(row[0].asString()),
            definition,
            com.posthog.hoglake.model.initialColumns(definition.columns),
            row[2].asString(),
            row[3].asString(),
            HogliteSchema.parse(row[4].asString()),
            row[5].asLongOrNull(),
            row[6].asLongOrNull(),
            row[7] as? String,
        )
    }

    // ---- transports --------------------------------------------------------

    /**
     * Run [tx] against [catalog]'s tenant.
     *
     * Every mutating tenant transaction carries the DROP FENCE as
     * statement 0: dropCatalog's fencing phase sets `meta.dropping`
     * under the same guard set that proves the catalog empty, and this
     * statement is what stops any later write (namespace create, commit,
     * loop sweep) from landing between those checks and the tenant's
     * deletion. Registry and bootstrap transactions opt out (the
     * registry tenant has no meta row; bootstrap is what CREATES it).
     */
    internal fun runTx(
        catalog: String,
        txid: String,
        fp: String?,
        tx: HogliteTx,
        fenced: Boolean = true,
    ): List<HogliteQueryResult> =
        try {
            val statements =
                if (fenced) {
                    listOf(
                        HogliteStatement(
                            "SELECT COUNT(*) FROM meta WHERE id = 1 AND dropping = 0",
                            emptyList(),
                            expectScalarInt = 1,
                        ),
                    ) + tx.statements()
                } else {
                    tx.statements()
                }
            val results = client.tx(tenantOf(catalog), txid, fp, statements)
            if (fenced) results.subList(1, results.size) else results
        } catch (g: HogliteGuardFailure) {
            if (fenced) {
                if (g.statementIndex == 0) {
                    throw HoglakeException.NotFound("catalog '$catalog' (it is being dropped)")
                }
                throw tx.classify(HogliteGuardFailure(g.statementIndex - 1, g.message ?: "guard failed"))
            }
            throw tx.classify(g)
        } catch (r: HogliteRejected) {
            throw mapRejected(catalog, r)
        }

    internal fun queryCatalog(
        catalog: String,
        sql: String,
        vararg args: Any?,
    ): HogliteQueryResult =
        try {
            client.query(tenantOf(catalog), sql, args.toList())
        } catch (r: HogliteRejected) {
            throw mapRejected(catalog, r)
        }

    internal fun mapRejected(
        catalog: String,
        r: HogliteRejected,
    ): HoglakeException =
        when (r.status) {
            404 -> HoglakeException.NotFound("catalog '$catalog'")
            // A 422 from hoglite is a SQL error — constraint violation or
            // a malformed statement. Both are this store's bug or a bad
            // input the guards should have named; surface as Validation
            // with the engine's message rather than a bare 500.
            422 -> {
                // The body is hoglite's {"error": "..."}; surface the
                // engine's message, not raw JSON, into the API error.
                val detail =
                    try {
                        json.readTree(r.body).get("error")?.asText()
                    } catch (_: Exception) {
                        null
                    }
                HoglakeException.Validation("hoglite refused the statement: ${detail ?: r.body}")
            }
            else -> HoglakeException.BackendUnavailable("hoglite answered ${r.status}: ${r.body}")
        }

    /**
     * The maintenance run ledger, one row per run in the CATALOG's
     * tenant (the tenant IS the catalog, so Postgres's catalog_id
     * column has no counterpart). Mirrors MaintenanceRunStore.recorded:
     * the row lands after the run resolves, a failure records FAILED
     * with the error text (and a PartialResult's earned counters), and
     * a ledger-write failure is a WARN, never the run's result.
     */
    internal fun <T> recorded(
        catalog: String,
        task: com.posthog.hoglake.model.MaintenanceTask,
        trigger: com.posthog.hoglake.model.MaintenanceTrigger,
        body: () -> T,
    ): T {
        val startedAt = java.time.Instant.now()
        try {
            val result = body()
            recordRun(
                catalog,
                task,
                trigger,
                startedAt,
                java.time.Instant.now(),
                com.posthog.hoglake.model.MaintenanceRunStatus.OK,
                null,
                result,
            )
            return result
        } catch (e: Throwable) {
            recordRun(
                catalog,
                task,
                trigger,
                startedAt,
                java.time.Instant.now(),
                com.posthog.hoglake.model.MaintenanceRunStatus.FAILED,
                e.message,
                (e as? com.posthog.hoglake.persistence.PartialResult)?.partial,
            )
            throw e
        }
    }

    private val ledgerJson = com.posthog.hoglake.wireObjectMapper()

    private fun recordRun(
        catalog: String,
        task: com.posthog.hoglake.model.MaintenanceTask,
        trigger: com.posthog.hoglake.model.MaintenanceTrigger,
        startedAt: java.time.Instant,
        finishedAt: java.time.Instant,
        status: com.posthog.hoglake.model.MaintenanceRunStatus,
        error: String?,
        result: Any?,
    ) {
        runCatching {
            val tx = HogliteTx()
            tx.add(
                "INSERT INTO maintenance_runs(task, run_trigger, started_at, finished_at, status, error, " +
                    "result_json) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                task.wire,
                trigger.wire,
                HogliteSchema.fmt(startedAt),
                HogliteSchema.fmt(finishedAt),
                status.wire,
                error,
                result?.let { ledgerJson.writeValueAsString(it) },
            )
            runTx(catalog, freshTxid("ledger"), null, tx)
        }.onFailure { e ->
            io.github.oshai.kotlinlogging.KotlinLogging.logger {}
                .warn(e) { "maintenance ledger: failed to record ${'$'}task run for catalog '${'$'}catalog'" }
        }
    }

    /** Every catalog name in the registry — the loops' instance-wide fan-out. */
    internal fun listCatalogNames(): List<String> {
        ensureRegistry()
        return queryRegistry("SELECT name FROM catalogs ORDER BY name").rows.map { it[0].asString() }
    }

    internal fun ensureRegistry() {
        client.createTenant(tenantOf(REGISTRY_TENANT))
        val tx = HogliteTx()
        tx.add(
            "CREATE TABLE IF NOT EXISTS catalogs(name TEXT PRIMARY KEY, data_path TEXT NOT NULL)",
        )
        // Fixed txid + fp: replaying the bootstrap is a no-op read.
        runTx(REGISTRY_TENANT, "boot:$REGISTRY_TENANT", "registry:v1", tx, fenced = false)
    }

    internal fun queryRegistry(
        sql: String,
        vararg args: Any?,
    ): HogliteQueryResult =
        try {
            client.query(tenantOf(REGISTRY_TENANT), sql, args.toList())
        } catch (r: HogliteRejected) {
            if (r.status == 404 || (r.status == 422 && "no such table" in r.body)) {
                // Registry tenant not bootstrapped yet (first-ever call,
                // or a catalog list before any create): bootstrap and
                // retry once.
                ensureRegistry()
                client.query(tenantOf(REGISTRY_TENANT), sql, args.toList())
            } else {
                throw mapRejected(REGISTRY_TENANT, r)
            }
        }

    // ---- small shared pieces ------------------------------------------------

    private fun sortColumnSql(sort: FileRepo.FileSortColumn): String =
        when (sort) {
            FileRepo.FileSortColumn.ID -> "id"
            FileRepo.FileSortColumn.PATH -> "path"
            FileRepo.FileSortColumn.RECORD_COUNT -> "record_count"
            FileRepo.FileSortColumn.SIZE -> "file_size_bytes"
            FileRepo.FileSortColumn.STATS -> "stats_state"
            FileRepo.FileSortColumn.BEGIN_SNAPSHOT -> "begin_snapshot"
        }

    internal fun insertColumnRows(
        tx: HogliteTx,
        tableIdSql: String,
        columns: List<Column>,
    ) {
        fun walk(
            cols: List<Column>,
            parent: Long?,
        ) {
            for (c in cols) {
                tx.add(
                    "INSERT INTO columns(table_id, field_id, parent_field_id, ordinal, name, type, " +
                        "type_params_json, nullable, comment, begin_snapshot) " +
                        "VALUES ($tableIdSql, ?, ?, ?, ?, ?, ?, ?, ?, ${HogliteSchema.HEAD_SQL})",
                    c.fieldId,
                    parent,
                    c.ordinal,
                    c.def.name,
                    c.def.type.wire,
                    c.def.typeParams?.let { json.writeValueAsString(it) },
                    if (c.def.nullable) 1L else 0L,
                    c.def.comment,
                )
                walk(c.children, c.fieldId)
            }
        }
        walk(columns, null)
    }

    private fun validateCatalogName(name: String) {
        if (!CATALOG_NAME_REGEX.matches(name)) {
            throw HoglakeException.Validation(
                "invalid catalog name '${Identifiers.cap(name)}' (must match $CATALOG_NAME_PATTERN)",
            )
        }
        if (name == REGISTRY_TENANT) {
            throw HoglakeException.Validation("catalog name '$name' is reserved")
        }
    }

    /** [com.posthog.hoglake.service.CatalogService.validateDataPath], ported verbatim. */
    private fun validateDataPathShape(dataPath: String) {
        if (dataPath.isBlank()) throw HoglakeException.Validation("data_path must not be blank")
        if (dataPath.length > 512) {
            throw HoglakeException.Validation(
                "data_path is ${dataPath.length} characters, over the maximum 512",
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

    companion object {
        /**
         * The tenant holding the cross-catalog registry (one row per
         * catalog: name, data_path). Its name cannot collide with a
         * user catalog — catalog names must start with a lowercase
         * letter.
         */
        const val REGISTRY_TENANT = "__hoglake_registry"

        private const val CATALOG_NAME_PATTERN = "^[a-z][a-z0-9_-]{0,62}\$"
        private val CATALOG_NAME_REGEX = Regex(CATALOG_NAME_PATTERN)

        internal fun freshTxid(prefix: String): String = "$prefix:${UUID.randomUUID()}"
    }
}
