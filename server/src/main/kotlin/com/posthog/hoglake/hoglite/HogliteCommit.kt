package com.posthog.hoglake.hoglite

import com.posthog.hoglake.commit.commitFingerprint
import com.posthog.hoglake.commit.commitFingerprintDigest
import com.posthog.hoglake.commit.pathStaysUnder
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.CommitResult
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.StatsSanity
import com.posthog.hoglake.model.validateSplitOffsets
import io.github.oshai.kotlinlogging.KotlinLogging
import java.security.MessageDigest
import java.util.UUID

/**
 * The hoglite port of CommitService.doCommit: one hoglake commit as ONE
 * hoglite transaction.
 *
 * The structure is the Postgres path's, with hoglite's serialization
 * standing in for the per-catalog advisory lock:
 *
 *  1. Kotlin-side validation against pre-flight reads (file shapes,
 *     stats sanity, path containment, claim settlement decisions) —
 *     everything that refuses on the REQUEST rather than the state.
 *  2. Guards IN the transaction for everything that refuses on the
 *     STATE: table liveness and incarnation, read_snapshot freshness,
 *     conflict windows, DV staleness and monotonicity. A guard miss
 *     rolls the whole transaction back and [HogliteTx.classify] decodes
 *     the failing statement's index to the typed hoglake refusal.
 *  3. Allocation (snapshot, file ids) as guarded counter UPDATEs whose
 *     post-bump expressions the inserts reference.
 *  4. The writes: file rows with server-assigned row lineage, DV epoch
 *     rows, typed change rows, the snapshot row, the receipt.
 *  5. The readback answers the CommitResult.
 *
 * Exactly-once is two layers and both are load-bearing:
 *
 *  - the txid is `ik:<key>` for a keyed request, `ch:<catalog>:<fp16>`
 *    otherwise, so a client retry of the SAME logical commit is answered
 *    from hoglite's dedup table without re-executing;
 *  - the receipts table is the durable `GET /commit/receipts/{op}`
 *    lookup AND the pre-flight that turns a reused key with a different
 *    payload into a 422 before any statement runs. A race between the
 *    pre-read and the tx is closed by the receipt insert's PRIMARY KEY:
 *    the loser's whole transaction rolls back on the constraint
 *    violation.
 */
internal object HogliteCommit {
    private val log = KotlinLogging.logger {}

    /** hoglake's per-file record cap (2^48); CommitService's own constant is private. */
    private const val MAX_FILE_RECORD_COUNT: Long = 1L shl 48

    private const val CONTENT_KINDS = "('table_created','table_inserted_into','table_deleted_from')"
    private const val DDL_KINDS = "('table_dropped','table_altered')"

    fun commit(
        store: HogliteCatalogStore,
        catalog: String,
        request: CommitRequest,
    ): CommitResult {
        if (request.appends.isEmpty() && request.deletes.isEmpty()) {
            throw HoglakeException.Validation("at least one of appends/deletes must be non-empty")
        }
        if (request.deletes.isNotEmpty() && request.readSnapshot == null) {
            throw HoglakeException.Validation("read_snapshot is required when deletes are present")
        }

        // Merge duplicate (namespace, table) entries preserving order,
        // as the Postgres path does, so one table gets one guard set
        // and one change row per kind.
        val order = LinkedHashMap<String, Touched>()
        fun touch(
            namespace: String,
            table: String,
            expectedUuid: UUID?,
        ): Touched {
            val t = order.getOrPut("$namespace.$table") { Touched(namespace, table) }
            if (expectedUuid != null) {
                if (t.expectedUuid != null && t.expectedUuid != expectedUuid) {
                    throw HoglakeException.Validation(
                        "conflicting expected_table_uuid for $namespace.$table",
                    )
                }
                t.expectedUuid = expectedUuid
            }
            return t
        }
        val appends =
            request.appends.groupBy { it.namespace to it.table }.values.map { group ->
                val first = group.first()
                val t = touch(first.namespace, first.table, null)
                group.forEach { touch(it.namespace, it.table, it.expectedTableUuid) }
                t.appends = true
                first.copy(
                    files = group.flatMap { it.files },
                    expectedTableUuid = group.firstNotNullOfOrNull { it.expectedTableUuid },
                )
            }
        val deletes =
            request.deletes.groupBy { it.namespace to it.table }.values.map { group ->
                val first = group.first()
                val t = touch(first.namespace, first.table, null)
                group.forEach { touch(it.namespace, it.table, it.expectedTableUuid) }
                t.deletes = true
                first.copy(
                    files = group.flatMap { it.files },
                    expectedTableUuid = group.firstNotNullOfOrNull { it.expectedTableUuid },
                )
            }

        val meta = store.meta(catalog)
        val dataPathPrefix = meta.dataPath.trimEnd('/') + "/"

        // Idempotency: the receipt pre-read (the tx's PK insert closes
        // the race; see the class KDoc).
        val canonical = commitFingerprint(request)
        val fingerprint = commitFingerprintDigest(canonical).toHex()
        if (request.idempotencyKey != null) {
            val existing =
                store.queryCatalog(
                    catalog,
                    "SELECT fingerprint, snapshot_id, schema_version FROM receipts WHERE idempotency_key = ?",
                    request.idempotencyKey.toString(),
                ).firstRow()
            if (existing != null) {
                if (existing[0].asString() == fingerprint) {
                    return CommitResult(existing[1].asLong(), existing[2].asLong())
                }
                throw HoglakeException.Validation(
                    "idempotency_key ${request.idempotencyKey} was already used with a different payload",
                )
            }
        }

        val readSnapshot = request.readSnapshot
        if (readSnapshot != null) {
            if (readSnapshot > meta.head) {
                throw HoglakeException.Validation(
                    "read_snapshot $readSnapshot is ahead of head ${meta.head}",
                )
            }
            if (readSnapshot < meta.earliestSnapshotId) {
                throw HoglakeException.Expired(
                    "read_snapshot $readSnapshot is below the expiry floor " +
                        "(earliest retained snapshot is ${meta.earliestSnapshotId})",
                )
            }
        }

        // Resolve every touched table. A name that misses the live index
        // but has a DROPPED incarnation is the typed 409 — a running
        // writer learns its table went away, with the drop snapshot to
        // line up against its own last commit.
        val resolved = LinkedHashMap<String, HogliteCatalogStore.TableRow>()
        for (t in order.values) {
            val row =
                try {
                    store.liveTable(catalog, t.namespace, t.table)
                } catch (e: HoglakeException.NotFound) {
                    val dropped =
                        store.queryCatalog(
                            catalog,
                            "SELECT t.dropped_snapshot FROM tables t " +
                                "JOIN table_versions v ON v.table_id = t.id " +
                                "WHERE v.namespace = ? AND v.name = ? " +
                                "AND t.dropped_snapshot IS NOT NULL ORDER BY t.dropped_snapshot DESC LIMIT 1",
                            t.namespace,
                            t.table,
                        ).firstRow()
                    if (dropped != null) {
                        throw HoglakeException.TableDropped(
                            "table '${t.namespace}.${t.table}' in catalog '$catalog' was dropped at " +
                                "snapshot ${dropped[0].asLong()}; stop, or recreate it",
                        )
                    }
                    throw e
                }
            val expected = t.expectedUuid
            if (expected != null && row.tableUuid != expected) {
                throw HoglakeException.TableRecreated(
                    "table '${t.namespace}.${t.table}' no longer has the expected UUID: it was " +
                        "recreated; re-read it before retrying",
                    table = "${t.namespace}.${t.table}",
                    expectedTableUuid = expected,
                    currentTableUuid = row.tableUuid,
                )
            }
            resolved["${t.namespace}.${t.table}"] = row
        }

        // ---- request-side validation (refusals that do not depend on tx state)

        val allPaths = LinkedHashMap<String, String>() // path -> file_kind
        fun claimPath(path: String, kind: String) {
            val prev = allPaths.putIfAbsent(path, kind)
            if (prev != null && prev != kind) {
                throw HoglakeException.Validation("one upload path cannot hold both data and deletion vectors")
            }
            if (prev != null) {
                throw HoglakeException.Validation("duplicate file path in commit: $path")
            }
            if (!pathStaysUnder(path, dataPathPrefix)) {
                throw HoglakeException.Validation(
                    "file path escapes the catalog's data_path: $path",
                )
            }
        }

        val plannedAppends = mutableListOf<PlannedAppend>()
        for (append in appends) {
            val table = resolved.getValue("${append.namespace}.${append.table}")
            val spec = store.specAt(catalog, table.tableId, meta.head)
            val liveColumnTypes = liveColumnTypes(store, catalog, table.tableId)
            var rows = table.nextRowId
            for (file in append.files) {
                val sanitizedStats =
                    validateFile(file, "${append.namespace}.${append.table}", spec?.fields?.size, liveColumnTypes)
                claimPath(file.path, "data")
                plannedAppends +=
                    PlannedAppend(table, file, rowOffset = rows - table.nextRowId, spec?.specId, sanitizedStats)
                rows =
                    try {
                        Math.addExact(rows, file.recordCount)
                    } catch (e: ArithmeticException) {
                        throw HoglakeException.Validation(
                            "row id overflow appending to ${append.namespace}.${append.table}",
                        )
                    }
            }
        }

        // DV targets: staged (this commit's appends, by path) or durable
        // (a data_file_id). Staged references must carry dataFileId 0.
        val stagedIdByPath = mutableMapOf<String, Int>() // path -> index into plannedAppends
        plannedAppends.forEachIndexed { i, p -> stagedIdByPath[p.file.path] = i }
        val plannedDeletes = mutableListOf<PlannedDelete>()
        val seenTargets = mutableSetOf<Long>()
        val seenStaged = mutableSetOf<Int>()
        for (delete in deletes) {
            val table = resolved.getValue("${delete.namespace}.${delete.table}")
            for (dv in delete.files) {
                claimPath(dv.path, "delete")
                val staged = dv.dataFilePath
                if (staged != null) {
                    if (dv.dataFileId != 0L) {
                        throw HoglakeException.Validation(
                            "a deletion vector that names data_file_path must carry data_file_id 0 " +
                                "(the file is allocated by this commit)",
                        )
                    }
                    val target = stagedIdByPath[staged]
                        ?: throw HoglakeException.Validation(
                            "deletion vector for ${delete.namespace}.${delete.table} references " +
                                "data_file_path $staged, which this commit does not append",
                        )
                    if (!seenStaged.add(target)) {
                        throw HoglakeException.Validation(
                            "the staged file $staged has two deletion vectors in one commit",
                        )
                    }
                    if (dv.deleteCount < 0 || dv.fileSizeBytes < 0) {
                        throw HoglakeException.Validation("negative delete_count/file_size_bytes for ${dv.path}")
                    }
                    plannedDeletes += PlannedDelete(table, dv, stagedTarget = target)
                } else {
                    if (dv.dataFileId <= 0) {
                        throw HoglakeException.Validation(
                            "a deletion vector needs a data_file_id (or data_file_path for a same-commit target)",
                        )
                    }
                    if (!seenTargets.add(dv.dataFileId)) {
                        throw HoglakeException.Validation(
                            "data_file_id ${dv.dataFileId} has two deletion vectors in one commit",
                        )
                    }
                    if (dv.deleteCount < 0 || dv.fileSizeBytes < 0) {
                        throw HoglakeException.Validation("negative delete_count/file_size_bytes for ${dv.path}")
                    }
                    plannedDeletes += PlannedDelete(table, dv, stagedTarget = null)
                }
            }
        }

        // Upload claims: only paths WITH a claim row are gated; an
        // unclaimed path (a direct-to-S3 writer) passes. The claim owner
        // is the commit's OWN idempotency key (that is the wire contract:
        // a writer claims with owner=<key> and commits with
        // idempotency_key=<key>). The decision is pre-flight; the settle
        // lands in the transaction.
        val claimsToSettle = decideClaims(store, catalog, allPaths, request.idempotencyKey)

        // ---- the conflict window (the mixed rule needs the UNION, so
        // pre-flight collects it; the in-tx guards below close the race
        // with content conflicts FIRST so a mixed window answers
        // commit_conflict, never the non-retryable one).
        if (readSnapshot != null) {
            val ids = resolved.values.map { it.tableId }
            val unchanged = unchangedIds(order, resolved, request)
            // Empty IN-lists are not SQL, so the content arm exists only
            // when the request has unchanged delete targets.
            val contentArm =
                if (unchanged.isEmpty()) {
                    ""
                } else {
                    " OR (kind IN $CONTENT_KINDS AND object_id IN (${unchanged.joinToString(",") { "?" }}))"
                }
            val conflicts =
                store.queryCatalog(
                    catalog,
                    "SELECT DISTINCT object_id, CASE WHEN kind IN $DDL_KINDS THEN 1 ELSE 0 END FROM changes " +
                        "WHERE snapshot_id > ? AND object_id IN (${ids.joinToString(",") { "?" }}) " +
                        "AND (kind IN $DDL_KINDS$contentArm)",
                    *buildList {
                        add(readSnapshot)
                        addAll(ids)
                        addAll(unchanged)
                    }.toTypedArray(),
                )
            if (conflicts.rows.isNotEmpty()) {
                val nameById = resolved.entries.associate { it.value.tableId to it.key }
                val names = conflicts.rows.mapNotNull { nameById[it[0].asLong()] }.distinct().sorted()
                val allDdl = conflicts.rows.all { it[1].asLong() == 1L }
                if (allDdl) {
                    throw HoglakeException.DdlSinceReadSnapshot(
                        "concurrent DDL since snapshot $readSnapshot on table(s): " +
                            names.joinToString(", ") +
                            "; re-prepare against current table info, replaying this request cannot succeed",
                        tables = names,
                        readSnapshot = readSnapshot,
                    )
                }
                throw HoglakeException.CommitConflict(
                    "concurrent table change since snapshot $readSnapshot on table(s): " +
                        names.joinToString(", "),
                )
            }
        }

        // ---- build the transaction

        val tx = HogliteTx()

        // 1. Table guards: live + incarnation, inside the transaction.
        for ((qname, t) in resolved) {
            tx.expectOne(
                "SELECT COUNT(*) FROM tables WHERE id = ? AND dropped_snapshot IS NULL",
                t.tableId,
            ) { HoglakeException.NotFound("table '$qname' in catalog '$catalog'") }
            val touched = order.values.first { "${it.namespace}.${it.table}" == qname }
            touched.expectedUuid?.let { expected ->
                tx.expectOne(
                    "SELECT COUNT(*) FROM tables WHERE id = ? AND dropped_snapshot IS NULL AND table_uuid = ?",
                    t.tableId,
                    expected.toString(),
                ) {
                    HoglakeException.TableRecreated(
                        "table '$qname' no longer has the expected UUID: it was recreated; " +
                            "re-read it before retrying",
                        table = qname,
                        expectedTableUuid = expected,
                        currentTableUuid = t.tableUuid,
                    )
                }
            }
        }

        // 2. Conflict guards for the race between the pre-flight read
        // and this transaction. CONTENT FIRST: a mixed window must
        // answer the retryable code.
        if (readSnapshot != null) {
            val unchanged = unchangedIds(order, resolved, request)
            if (unchanged.isNotEmpty()) {
                tx.expectZero(
                    "SELECT COUNT(*) FROM changes WHERE snapshot_id > ? AND kind IN $CONTENT_KINDS " +
                        "AND object_id IN (${unchanged.joinToString(",") { "?" }})",
                    *buildList {
                        add(readSnapshot)
                        addAll(unchanged)
                    }.toTypedArray(),
                ) {
                    HoglakeException.CommitConflict(
                        "concurrent table change since snapshot $readSnapshot on a delete target",
                    )
                }
            }
            for ((qname, t) in resolved) {
                tx.expectZero(
                    "SELECT COUNT(*) FROM changes WHERE snapshot_id > ? AND kind IN $DDL_KINDS AND object_id = ?",
                    readSnapshot,
                    t.tableId,
                ) {
                    HoglakeException.DdlSinceReadSnapshot(
                        "concurrent DDL since snapshot $readSnapshot on table(s): $qname; " +
                            "re-prepare against current table info, replaying this request cannot succeed",
                        tables = listOf(qname),
                        readSnapshot = readSnapshot,
                    )
                }
            }
        }

        // 3. Removal-queue collision: a path cleanup owns cannot be
        // registered (hoglake's refuseQueuedPaths).
        if (allPaths.isNotEmpty()) {
            tx.expectZero(
                "SELECT COUNT(*) FROM removals WHERE path IN (${allPaths.keys.joinToString(",") { "?" }})",
                *allPaths.keys.toTypedArray(),
            ) {
                HoglakeException.CommitConflict(
                    "a file path in this commit is queued for removal; claim a fresh path and retry",
                )
            }
        }

        // 4. Allocate: the snapshot, then the file ids this commit needs
        // (data files first, then DVs — the shared allocator's order).
        tx.allocateCommitSnapshot()
        val totalFiles = plannedAppends.size + plannedDeletes.size
        if (totalFiles > 0) {
            tx.affectOne(
                "UPDATE meta SET next_file_id = next_file_id + $totalFiles WHERE id = 1",
            ) { HoglakeException.NotFound("catalog '$catalog'") }
        }
        fun allocatedId(offset: Int): String =
            "(SELECT next_file_id - $totalFiles + $offset FROM meta WHERE id = 1)"

        // 5. Appends: file rows with server-assigned row lineage.
        val appendedIdByIndex = mutableMapOf<Int, String>()
        plannedAppends.forEachIndexed { i, p ->
            val idSql = allocatedId(i)
            appendedIdByIndex[i] = idSql
            val statsJson = p.sanitizedStats?.let { store.json.writeValueAsString(it) }
            tx.add(
                "INSERT INTO files(id, table_id, path, record_count, file_size_bytes, footer_size, " +
                    "row_id_start, spec_id, partition_values_json, stats_state, stats_json, " +
                    "split_offsets_json, begin_snapshot) " +
                    "VALUES ($idSql, ?, ?, ?, ?, ?, " +
                    "(SELECT next_row_id FROM tables WHERE id = ?) + ${p.rowOffset}, ?, ?, ?, ?, ?, " +
                    "${HogliteSchema.HEAD_SQL})",
                p.table.tableId,
                p.file.path,
                p.file.recordCount,
                p.file.fileSizeBytes,
                p.file.footerSize,
                p.table.tableId,
                p.specId,
                p.file.partitionValues?.let { store.json.writeValueAsString(it) },
                if (p.sanitizedStats == null) "pending" else "provided",
                statsJson,
                p.file.splitOffsets?.let { offs -> store.json.writeValueAsString(offs) },
            )
        }
        // Row lineage advances once per appended table.
        for (append in appends) {
            val table = resolved.getValue("${append.namespace}.${append.table}")
            val rows = append.files.sumOf { it.recordCount }
            if (rows > 0) {
                tx.add(
                    "UPDATE tables SET next_row_id = next_row_id + $rows WHERE id = ?",
                    table.tableId,
                )
            }
            val bytes = append.files.sumOf { it.fileSizeBytes }
            tx.add(
                "INSERT INTO table_totals(table_id, record_count, file_count, file_size_bytes) VALUES (?, ?, ?, ?) " +
                    "ON CONFLICT(table_id) DO UPDATE SET " +
                    "record_count = table_totals.record_count + excluded.record_count, " +
                    "file_count = table_totals.file_count + excluded.file_count, " +
                    "file_size_bytes = table_totals.file_size_bytes + excluded.file_size_bytes",
                table.tableId,
                rows,
                append.files.size.toLong(),
                bytes,
            )
        }

        // 6. Deletes: per-target guards (staleness BEFORE monotonicity),
        // then the history-preserving epoch insert.
        plannedDeletes.forEachIndexed { i, p ->
            val idSql = allocatedId(plannedAppends.size + i)
            val dv = p.registration
            val qname = "${p.table.namespace}.${p.table.name}"
            if (p.stagedTarget != null) {
                // Same-commit target: the file is minted above, so its
                // guards are the append's own. delete_count is checked
                // against the registration in Kotlin already.
                val target = plannedAppends[p.stagedTarget]
                if (dv.deleteCount > target.file.recordCount) {
                    throw HoglakeException.Validation(
                        "delete_count ${dv.deleteCount} exceeds record_count of the staged file " +
                            "${target.file.path} in $qname",
                    )
                }
                tx.add(
                    "INSERT INTO dvs(data_file_id, table_id, delete_file_id, path, delete_count, " +
                        "file_size_bytes, begin_snapshot, epoch) " +
                        "VALUES (${appendedIdByIndex.getValue(p.stagedTarget)}, ?, $idSql, ?, ?, ?, " +
                        "${HogliteSchema.HEAD_SQL}, 1)",
                    p.table.tableId,
                    dv.path,
                    dv.deleteCount,
                    dv.fileSizeBytes,
                )
            } else {
                val target = dv.dataFileId
                tx.expectOne(
                    "SELECT COUNT(*) FROM files WHERE id = ? AND table_id = ?",
                    target,
                    p.table.tableId,
                ) {
                    HoglakeException.Validation(
                        "delete for $qname targets unknown data_file_id $target",
                    )
                }
                // The target appeared after the read: the vector was
                // planned without it. Retryable.
                tx.expectZero(
                    "SELECT COUNT(*) FROM files WHERE id = ? AND begin_snapshot > ?",
                    target,
                    readSnapshot!!,
                ) {
                    HoglakeException.CommitConflict(
                        "delete for $qname targets data_file_id $target created after read snapshot $readSnapshot",
                    )
                }
                tx.expectZero(
                    "SELECT COUNT(*) FROM files WHERE id = ? AND record_count < ?",
                    target,
                    dv.deleteCount,
                ) {
                    HoglakeException.Validation(
                        "delete_count ${dv.deleteCount} exceeds record_count of data_file_id $target in $qname",
                    )
                }
                // Staleness before monotonicity: a vector registered
                // after the read was not seen by this writer.
                tx.expectZero(
                    "SELECT COUNT(*) FROM dvs WHERE data_file_id = ? AND begin_snapshot > ?",
                    target,
                    readSnapshot,
                ) {
                    HoglakeException.CommitConflict(
                        "deletion vector for data_file_id $target in $qname was superseded after " +
                            "read snapshot $readSnapshot",
                    )
                }
                tx.expectZero(
                    "SELECT COUNT(*) FROM dvs d WHERE d.data_file_id = ? AND d.delete_count > ? " +
                        "AND d.epoch = (SELECT MAX(d2.epoch) FROM dvs d2 WHERE d2.data_file_id = d.data_file_id)",
                    target,
                    dv.deleteCount,
                ) {
                    HoglakeException.Validation(
                        "delete_count ${dv.deleteCount} for data_file_id $target in $qname shrinks the " +
                            "live deletion vector — vectors only grow",
                    )
                }
                tx.add(
                    "INSERT INTO dvs(data_file_id, table_id, delete_file_id, path, delete_count, " +
                        "file_size_bytes, begin_snapshot, epoch) " +
                        "SELECT f.id, f.table_id, $idSql, ?, ?, ?, ${HogliteSchema.HEAD_SQL}, " +
                        "COALESCE((SELECT MAX(d.epoch) FROM dvs d WHERE d.data_file_id = f.id), 0) + 1 " +
                        "FROM files f WHERE f.id = ?",
                    dv.path,
                    dv.deleteCount,
                    dv.fileSizeBytes,
                    target,
                )
            }
        }

        // 7. Upload claim settlement (the decision was pre-flight).
        for (claimId in claimsToSettle) {
            tx.affectOne(
                "UPDATE uploads SET state = 'registered' WHERE upload_id = ?",
                claimId,
            ) {
                HoglakeException.CommitConflict(
                    "upload is fenced, already registered, or belongs to another operation",
                )
            }
        }

        // 8. Typed change rows, the snapshot row, the receipt, readback.
        for (t in order.values) {
            val table = resolved.getValue("${t.namespace}.${t.table}")
            if (t.appends) {
                tx.add(
                    "INSERT INTO changes(snapshot_id, kind, object_id) " +
                        "VALUES (${HogliteSchema.HEAD_SQL}, 'table_inserted_into', ?)",
                    table.tableId,
                )
            }
            if (t.deletes) {
                tx.add(
                    "INSERT INTO changes(snapshot_id, kind, object_id) " +
                        "VALUES (${HogliteSchema.HEAD_SQL}, 'table_deleted_from', ?)",
                    table.tableId,
                )
            }
        }
        tx.insertSnapshotRow(request.author, request.message)
        if (request.idempotencyKey != null) {
            tx.add(
                "INSERT INTO receipts(idempotency_key, fingerprint, snapshot_id, schema_version, created_at) " +
                    "VALUES (?, ?, ${HogliteSchema.HEAD_SQL}, (SELECT schema_version FROM meta WHERE id = 1), ?)",
                request.idempotencyKey.toString(),
                fingerprint,
                HogliteSchema.now(),
            )
        }
        val readback = tx.add(HogliteSql.READBACK_COMMIT)

        val txid =
            request.idempotencyKey?.let { "ik:$catalog:$it" }
                ?: "ch:$catalog:${sha256Hex16(canonical)}"
        val results =
            try {
                store.runTx(catalog, txid, fingerprint, tx)
            } catch (e: HoglakeException.Validation) {
                // The receipt PRIMARY KEY is the race closer: two
                // different payloads under one key -> the loser's
                // constraint violation lands here.
                if (request.idempotencyKey != null && "receipts" in (e.message ?: "")) {
                    throw HoglakeException.Validation(
                        "idempotency_key ${request.idempotencyKey} was already used with a different payload",
                    )
                }
                throw e
            }
        val row = results[readback].firstRow()!!
        return CommitResult(snapshotId = row[0].asLong(), schemaVersion = row[1].asLong())
    }

    /** The durable publication receipt, as GET /commit/receipts/{operation} serves it. */
    fun receipt(
        store: HogliteCatalogStore,
        catalog: String,
        operation: UUID,
    ): CommitResult {
        store.meta(catalog)
        val row =
            store.queryCatalog(
                catalog,
                "SELECT snapshot_id, schema_version FROM receipts WHERE idempotency_key = ?",
                operation.toString(),
            ).firstRow()
                ?: throw HoglakeException.NotFound(
                    "no commit receipt for operation $operation in catalog '$catalog'",
                )
        return CommitResult(snapshotId = row[0].asLong(), schemaVersion = row[1].asLong())
    }

    // ---- internals ----------------------------------------------------------

    private class Touched(
        val namespace: String,
        val table: String,
    ) {
        var expectedUuid: UUID? = null
        var appends: Boolean = false
        var deletes: Boolean = false
    }

    /** One append planned against its table, with the running row offset within the commit. */
    private class PlannedAppend(
        val table: HogliteCatalogStore.TableRow,
        val file: FileRegistration,
        val rowOffset: Long,
        val specId: Long?,
        val sanitizedStats: List<ColumnStats>?,
    )

    private class PlannedDelete(
        val table: HogliteCatalogStore.TableRow,
        val registration: DeleteFileRegistration,
        /** Index into the commit's planned appends for a data_file_path reference; null for a durable target. */
        val stagedTarget: Int?,
    )

    /** The tables a guarded request's CONTENT window covers: delete targets when requireUnchangedTables. */
    private fun unchangedIds(
        order: Map<String, Touched>,
        resolved: Map<String, HogliteCatalogStore.TableRow>,
        request: CommitRequest,
    ): List<Long> {
        if (!request.requireUnchangedTables) return emptyList()
        return order.values
            .filter { it.deletes }
            .map { resolved.getValue("${it.namespace}.${it.table}").tableId }
    }

    /** The live field_id -> type map stats validation checks against (every node, containers included). */
    private fun liveColumnTypes(
        store: HogliteCatalogStore,
        catalog: String,
        tableId: Long,
    ): Map<Long, ColType> =
        store.queryCatalog(
            catalog,
            "SELECT field_id, type FROM columns WHERE table_id = ? AND end_snapshot IS NULL",
            tableId,
        ).rows.associate { it[0].asLong() to ColType.fromWire(it[1].asString()) }

    /**
     * CommitService.validateFiles + sanitizeStats, ported check for
     * check. Returns the SANITIZED stats (bounds dropped, counts
     * clamped) for storage; null when the registration defers stats
     * (PENDING, for the hydrator).
     */
    internal fun validateFile(
        file: FileRegistration,
        qualified: String,
        specFieldCount: Int?,
        liveColumnTypes: Map<Long, ColType>,
    ): List<ColumnStats>? {
        if (file.path.isBlank()) {
            throw HoglakeException.Validation("blank file path in append to $qualified")
        }
        if (file.recordCount < 0) {
            throw HoglakeException.Validation("negative record_count for ${file.path} in $qualified")
        }
        if (file.recordCount > MAX_FILE_RECORD_COUNT) {
            throw HoglakeException.Validation(
                "record_count ${file.recordCount} for ${file.path} in $qualified exceeds " +
                    "the per-file cap $MAX_FILE_RECORD_COUNT (2^48); no real file has that many rows",
            )
        }
        if (file.fileSizeBytes < 0) {
            throw HoglakeException.Validation("negative file_size_bytes for ${file.path} in $qualified")
        }
        file.validateSplitOffsets()
        val values = file.partitionValues
        if (specFieldCount != null) {
            if (values == null) {
                throw HoglakeException.Validation(
                    "$qualified is partitioned ($specFieldCount field(s)) but ${file.path} has no partition_values",
                )
            }
            if (values.size != specFieldCount) {
                throw HoglakeException.Validation(
                    "partition_values arity ${values.size} for ${file.path} in $qualified " +
                        "does not match the live spec's $specFieldCount field(s)",
                )
            }
        } else if (values != null) {
            throw HoglakeException.Validation(
                "partition_values given for ${file.path} but $qualified is not partitioned",
            )
        }
        val stats = file.columnStats ?: return null
        val seenFieldIds = HashSet<Long>()
        val sanitized =
            stats.map { stat ->
                val colType =
                    liveColumnTypes[stat.fieldId]
                        ?: throw HoglakeException.Validation(
                            "unknown field_id ${stat.fieldId} in stats for ${file.path} in $qualified",
                        )
                if (colType == ColType.VARIANT) {
                    throw HoglakeException.Validation(
                        "variant column statistics are not supported; omit field_id ${stat.fieldId}",
                    )
                }
                if (colType.isNested) {
                    throw HoglakeException.Validation(
                        "field_id ${stat.fieldId} in stats for ${file.path} in $qualified is a " +
                            "'${colType.wire}' column; nested containers carry no values, so stats are " +
                            "per LEAF field — ship the element/key/value/struct-field ids instead",
                    )
                }
                if (!seenFieldIds.add(stat.fieldId)) {
                    throw HoglakeException.Validation(
                        "duplicate field_id ${stat.fieldId} in stats for ${file.path} in $qualified",
                    )
                }
                if (stat.valueCount < 0 || stat.nullCount < 0) {
                    throw HoglakeException.Validation(
                        "negative value_count/null_count for field_id ${stat.fieldId} in stats for " +
                            "${file.path} in $qualified",
                    )
                }
                // StatsSanity's repairs (drop impossible bounds, clamp
                // impossible counts) — refuse nothing more, fix what a
                // client cannot have meant, warn once per repaired row.
                val checked = StatsSanity.check(stat, colType)
                if (checked.repairs.isNotEmpty()) {
                    log.warn {
                        "repaired stats for field_id ${stat.fieldId} in ${file.path}: " +
                            checked.repairs.joinToString("; ")
                    }
                }
                checked.stats
            }
        // Store the SANITIZED stats, not the wire's — readers prune on
        // these bounds, so storing a wrong one is a wrong answer.
        return sanitized
    }

    /**
     * The upload-claim decision (UploadService.claimsToSettle's logic,
     * against the tenant's uploads table): only paths WITH a claim row
     * are gated; each must be active, owned by this commit's owner, and
     * of the right file kind — anything else is the fenced/registered/
     * foreign-owner CommitConflict. Returns the claim ids to settle in
     * the transaction.
     */
    internal fun decideClaims(
        store: HogliteCatalogStore,
        catalog: String,
        paths: Map<String, String>,
        owner: UUID?,
    ): List<String> {
        if (paths.isEmpty()) return emptyList()
        val claims =
            store.queryCatalog(
                catalog,
                "SELECT upload_id, path, file_kind, state, owner FROM uploads " +
                    "WHERE path IN (${paths.keys.joinToString(",") { "?" }})",
                *paths.keys.toTypedArray(),
            ).rows
        if (claims.isEmpty()) return emptyList()
        // Immutable objects still referenced at a retained snapshot may
        // be referenced again; ownership never permits rewriting them.
        // Numeric kind markers, not the 'data'/'delete' strings: hoglite's
        // /query gate scans the TEXT for DML words, and a 'delete' literal
        // is one. The mapping back happens here.
        val retainedPaths =
            store.queryCatalog(
                catalog,
                "SELECT path, 0 FROM files WHERE path IN (${paths.keys.joinToString(",") { "?" }}) " +
                    "UNION SELECT path, 1 FROM dvs WHERE path IN (${paths.keys.joinToString(",") { "?" }})",
                // toList() first: Set + Set is the Set overload and DEDUPLICATES the
                // doubled keys, shorting the second IN-list its arguments.
                *(paths.keys.toList() + paths.keys).toTypedArray(),
            ).rows.map { it[0].asString() to (if (it[1].asLong() == 0L) "data" else "delete") }.toSet()
        val settle = mutableListOf<String>()
        for (claim in claims) {
            val path = claim[1].asString()
            val kind = claim[2].asString()
            val state = claim[3].asString()
            val claimOwner = claim[4].asString()
            if (state == "registered" && kind == paths[path] && (path to kind) in retainedPaths) {
                continue
            }
            if (state != "active" || claimOwner != owner?.toString() || kind != paths[path]) {
                throw HoglakeException.CommitConflict(
                    "upload is fenced, already registered, or belongs to another operation",
                )
            }
            settle += claim[0].asString()
        }
        return settle
    }

    private fun ByteArray.toHex(): String = joinToString("") { "%02x".format(it) }

    /** The content-addressed txid's payload half: first 16 bytes of the canonical request's SHA-256. */
    private fun sha256Hex16(canonical: String): String =
        MessageDigest.getInstance("SHA-256").digest(canonical.toByteArray()).take(16).toByteArray().toHex()
}
