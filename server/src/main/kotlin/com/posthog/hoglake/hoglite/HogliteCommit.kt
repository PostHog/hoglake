package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.commit.COMMIT_FINGERPRINT_LENGTH
import com.posthog.hoglake.commit.COMMIT_FINGERPRINT_VERSION
import com.posthog.hoglake.commit.CommitService.Companion.BLIND_PARTITIONED_REFUSAL
import com.posthog.hoglake.commit.CommitService.Companion.blindPartitionedRefusal
import com.posthog.hoglake.commit.ObjectPrefixRules
import com.posthog.hoglake.commit.commitFingerprint
import com.posthog.hoglake.commit.commitFingerprintDigest
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.CommitResult
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.ObjectPrefix
import com.posthog.hoglake.model.ObjectPrefixLayout
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.PartitionSpec
import com.posthog.hoglake.model.StatsSanity
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.model.pathStaysUnder
import com.posthog.hoglake.model.validateFooterSize
import com.posthog.hoglake.model.validateSplitOffsets
import com.posthog.hoglake.observability.Metrics
import io.github.oshai.kotlinlogging.KotlinLogging
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
 * Each materialized transaction has a fresh engine txid. Transport retries
 * keep its complete body fixed. An ambiguous outcome never permits replanning.
 *
 * Application receipts control client-key replay for their retention lifetime.
 * A receipt guard precedes state guards inside the transaction. Its private
 * rollback marker permits a bound receipt lookup. A missing receipt shares
 * the schema fence's single replan slot.
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
        // Match CommitService's transaction admission before any planning or writes.
        if (request.allowPendingDeletes &&
            (
                request.idempotencyKey == null || request.readSnapshot == null ||
                    !request.requireUnchangedTables ||
                    request.appends.any { it.expectedTableUuid == null } ||
                    request.deletes.any { it.expectedTableUuid == null }
            )
        ) {
            throw HoglakeException.Validation("transaction requires guarded idempotent publication")
        }
        if (request.deletes.any { table -> table.files.any { it.dataFilePath != null } } &&
            !request.allowPendingDeletes
        ) {
            throw HoglakeException.Validation("pending delete targets require /commit/transaction")
        }
        // Schema changes and receipts removed during recovery share one replan.
        // Both private markers prove that the materialized write rolls back.
        return try {
            doCommit(store, catalog, request)
        } catch (e: HoglakeException.CommitConflict) {
            if (e !== SCHEMA_CHANGED && e !== RECEIPT_CHANGED) throw e
            try {
                doCommit(store, catalog, request)
            } catch (e2: HoglakeException.CommitConflict) {
                if (e2 !== SCHEMA_CHANGED && e2 !== RECEIPT_CHANGED) throw e2
                throw HoglakeException.CommitConflict(
                    if (e2 === SCHEMA_CHANGED) {
                        "concurrent DDL changed the schema twice while this commit was being planned; retry"
                    } else {
                        "receipt changed during publication; retry"
                    },
                )
            }
        }
    }

    private fun doCommit(
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
            if (expectedUuid != null) t.expectedUuids += expectedUuid
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

        // Every read this attempt plans against, as ONE read-only
        // transaction (see PlanReads). Resolve identities before consuming
        // conflicts. Conflicts precede live-file validation and claims.
        val plan = PlanReads.read(store, catalog, request, order.values, appends, deletes)
        val meta = plan.meta()
        val dataPathPrefix = meta.dataPath.trimEnd('/') + "/"

        // Compare the original logical request, not its merged statement layout.
        val fingerprintBytes = commitFingerprintDigest(commitFingerprint(request))
        val fingerprint = fingerprintBytes.toHex()
        if (request.idempotencyKey != null) {
            val existing = plan.receipt()
            if (existing != null) {
                return replayedReceipt(catalog, request.idempotencyKey, fingerprintBytes, existing)
            }
        }

        val readSnapshot = request.readSnapshot
        if (readSnapshot != null) {
            if (readSnapshot < 0) {
                throw HoglakeException.Validation("read_snapshot must be non-negative, got $readSnapshot")
            }
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
                    plan.table(t)
                } catch (e: HoglakeException.NotFound) {
                    // A failure path (the commit is refused either way):
                    // this probe stays a separate read after the batch.
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
            val expected = t.expectedUuids.firstOrNull { it != row.tableUuid }
            if (expected != null) {
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

        // ---- the conflict window (the mixed rule needs the UNION, so
        // pre-flight collects it; the in-tx guards below close the race
        // with content conflicts FIRST so a mixed window answers
        // commit_conflict, never the non-retryable one).
        if (readSnapshot != null) {
            val conflicts = plan.conflicts()
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

        // The snapshot every definition below (spec, layout) is read at
        // is meta.head, read in the SAME snapshot as every table's fence
        // (tables.schema_version, read with its row above): the plan reads
        // are one transaction. The head must not be older than the fence —
        // DDL landing between a head read and a later fence read would be
        // invisible to planning while the fence already includes it, and
        // the tx would pass with the plan made against the older
        // definitions (an append planned without a just-set object-prefix
        // layout, or against a just-replaced spec). Read in one snapshot,
        // every DDL the fence admits is visible at the plan head, and any
        // DDL after it bumps a schema_version the fence then refuses (a
        // replan). When the reads were separate `/query` calls this took a
        // second meta read after the table reads; in one snapshot that
        // read returns this same row, so it is not repeated.

        // ---- request-side validation (refusals that do not depend on tx state)

        val allPaths = LinkedHashMap<String, String>() // path -> file_kind

        fun claimPath(
            path: String,
            kind: String,
        ) {
            val prev = allPaths.putIfAbsent(path, kind)
            if (prev != null && prev != kind) {
                throw HoglakeException.Validation("one upload path cannot hold both data and deletion vectors")
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
            val spec = plan.spec(table)
            val liveColumnTypes = plan.columnTypes(table)
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

        // Blind partitioned appends (CommitService's rule, verbatim):
        // a commit with no read_snapshot whose files carry partition
        // values. WARN + counter always; the 422 once the deployment's
        // flag is on.
        if (readSnapshot == null) {
            val offending =
                appends.filter { a -> a.files.any { it.partitionValues != null } }
                    .map { "${it.namespace}.${it.table}" }.sorted()
            for (qname in offending) {
                com.posthog.hoglake.observability.Metrics.blindPartitionedAppend(
                    catalog,
                    qname.substringBefore('.'),
                    qname.substringAfter('.'),
                )
                if (WARNED_BLIND_PARTITIONED.add("$catalog:$qname")) {
                    log.warn {
                        "blind partitioned append to $catalog.$qname accepted: $BLIND_PARTITIONED_REFUSAL"
                    }
                }
            }
            if (offending.isNotEmpty() && store.refuseBlindPartitionedAppends) {
                throw HoglakeException.Validation(
                    "${blindPartitionedRefusal(offending)}: $BLIND_PARTITIONED_REFUSAL",
                )
            }
        }

        // DV targets: staged (this commit's appends, by path) or durable
        // (a data_file_id). Staged references must carry dataFileId 0.
        // Staged references resolve by (TABLE, path) — Postgres's
        // (tableId, path) resolution: a DV's target must be a file THIS
        // delete's table is appending, so a cross-table staged reference
        // can never bind another table's file to this table's DV row.
        val stagedIdsByPath =
            plannedAppends.withIndex().groupBy({ it.value.table.tableId to it.value.file.path }) { it.index }
        val plannedDeletes = mutableListOf<PlannedDelete>()
        val seenTargets = mutableSetOf<Long>()
        val seenStaged = mutableSetOf<Pair<Long, String>>()
        for (delete in deletes) {
            val table = resolved.getValue("${delete.namespace}.${delete.table}")
            for (dv in delete.files) {
                claimPath(dv.path, "delete")
                if (dv.deleteCount <= 0) {
                    throw HoglakeException.Validation(
                        "delete_count must be > 0 for data_file_id ${dv.dataFileId} " +
                            "in ${delete.namespace}.${delete.table}, got ${dv.deleteCount}",
                    )
                }
                val staged = dv.dataFilePath
                if (staged != null) {
                    if (dv.fileSizeBytes < 0) {
                        throw HoglakeException.Validation(
                            "negative file_size_bytes for delete of data_file_id ${dv.dataFileId} " +
                                "in ${delete.namespace}.${delete.table}",
                        )
                    }
                    if (dv.dataFileId != 0L) {
                        throw HoglakeException.Validation(
                            "a deletion vector that names data_file_path must carry data_file_id 0 " +
                                "(the file is allocated by this commit)",
                        )
                    }
                    if (!seenStaged.add(table.tableId to staged)) {
                        throw HoglakeException.Validation(
                            "the staged file $staged has two deletion vectors in one commit",
                        )
                    }
                    val targets = stagedIdsByPath[table.tableId to staged].orEmpty()
                    if (targets.size != 1) {
                        val refusal =
                            HoglakeException.Validation(
                                "pending delete must target exactly one same-table append in this commit",
                            )
                        if (targets.isEmpty()) throw refusal
                        // PostgreSQL checks path protection before ambiguous targets.
                        // Keep every path, but do not select a target.
                        plannedDeletes += PlannedDelete(table, dv, stagedTarget = null, refusal = refusal)
                        continue
                    }
                    val target = targets.single()
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
                    if (dv.fileSizeBytes < 0) {
                        throw HoglakeException.Validation(
                            "negative file_size_bytes for delete of data_file_id ${dv.dataFileId} " +
                                "in ${delete.namespace}.${delete.table}",
                        )
                    }
                    plannedDeletes += PlannedDelete(table, dv, stagedTarget = null)
                }
            }
        }

        // The object-prefix layout (CommitService step 4a): on the
        // SANITIZED stats, after the structural checks of appends and
        // DVs alike, before claims. A table without the option skips it.
        // The layout is read at the plan head (with the table's fence);
        // setting it bumps tables.schema_version, so the in-tx schema
        // fence below replans a commit that planned against the option
        // being off.
        val layouts = HashMap<Long, ObjectPrefixLayout?>()

        fun layoutFor(table: HogliteCatalogStore.TableRow): ObjectPrefixLayout? =
            layouts.getOrPut(table.tableId) { plan.layout(table) }

        for (append in appends) {
            val table = resolved.getValue("${append.namespace}.${append.table}")
            val layout = layoutFor(table) ?: continue
            val spec = plan.spec(table)
            val keyIndex = spec?.let { ObjectPrefix.keyIndex(it.fields, layout.fieldId) }
            // The DDL keeps exactly one identity field on the prefix field
            // in every spec while the option is on.
            check(keyIndex != null) {
                "${append.namespace}.${append.table} has the object-prefix layout on '${layout.fieldName}' but " +
                    "its live spec has no single identity field on it"
            }
            val sanitized =
                plannedAppends.filter { it.table.tableId == table.tableId }
                    .map { it.file.copy(columnStats = it.sanitizedStats) }
            val violations =
                ObjectPrefixRules.appendViolations(
                    sanitized,
                    layout,
                    keyIndex,
                    append.namespace,
                    append.table,
                    meta.dataPath,
                )
            if (violations.isNotEmpty()) {
                throw ObjectPrefixRules.violation(append.namespace, append.table, meta.dataPath, layout, violations)
            }
        }
        // Deletion vectors on an option-on table live under their TARGET's
        // prefix. Computed here from immutable facts (a staged target's
        // planned values; a durable target's own spec and values), and
        // raised by a guard AFTER every per-target guard in the
        // transaction: CommitService checks DVs after their conflict and
        // liveness checks, so a stale or unknown target still answers
        // with those refusals first.
        val dvViolations = LinkedHashMap<Long, MutableList<String>>()
        for (p in plannedDeletes) {
            if (p.refusal != null) continue
            val layout = layoutFor(p.table) ?: continue
            val raw =
                if (p.stagedTarget != null) {
                    val target = plannedAppends[p.stagedTarget]
                    val keyIndex =
                        plan.spec(p.table)?.let { ObjectPrefix.keyIndex(it.fields, layout.fieldId) }
                    keyIndex?.let { target.file.partitionValues?.getOrNull(it) }
                } else {
                    // A target that is missing or another table's is refused
                    // by the in-tx guards, ahead of this check.
                    val target = plan.durableTarget(p.table, p.registration.dataFileId) ?: continue
                    target.specFields?.let { ObjectPrefix.keyIndex(it, layout.fieldId) }
                        ?.let { target.partitionValues?.getOrNull(it) }
                }
            ObjectPrefixRules.dvViolation(
                p.registration,
                raw,
                layout,
                p.table.namespace,
                p.table.name,
                meta.dataPath,
            )?.let { dvViolations.getOrPut(p.table.tableId) { mutableListOf() } += it }
        }

        // Upload claims: only paths WITH a claim row are gated; an
        // unclaimed path (a direct-to-S3 writer) passes. The claim owner
        // is the commit's OWN idempotency key (that is the wire contract:
        // a writer claims with owner=<key> and commits with
        // idempotency_key=<key>). The decision is pre-flight; the settle
        // lands in the transaction.
        val claimsToSettle = settleDecision(plan.claims(), plan.retainedPaths(), allPaths, request.idempotencyKey)

        // ---- build the transaction

        val tx = HogliteTx()
        // The store retains its outer catalog fence before this receipt guard.
        // A retained publication takes precedence over stale claim or DV state.
        request.idempotencyKey?.let { key ->
            tx.expectZero(
                "SELECT COUNT(*) FROM receipts WHERE idempotency_key = ?",
                key.toString(),
            ) { RECEIPT_CHANGED }
        }

        // 1. Table guards: live + incarnation, inside the transaction.
        for ((qname, t) in resolved) {
            // Incarnation BEFORE liveness: a recreate changes the table
            // id, and a caller holding the old incarnation must get the
            // retryable 409s (re-read), never a bare 404.
            val touched = order.values.first { "${it.namespace}.${it.table}" == qname }
            touched.expectedUuids.firstOrNull()?.let { expected ->
                tx.expectOne(
                    "SELECT COUNT(*) FROM tables WHERE id = ? AND dropped_snapshot IS NULL AND table_uuid = ?",
                    t.tableId,
                    expected.toString(),
                ) {
                    // Classify AFTER the rollback: the read sees the state
                    // that refused us. A live incarnation under the same
                    // name with a DIFFERENT uuid is a recreate (re-read,
                    // retryable); a dropped row with no successor is the
                    // drop ("stop") — Postgres's table_dropped, not the
                    // table_recreated a bare liveness miss would imply.
                    val row =
                        store.queryCatalog(
                            catalog,
                            "SELECT t.dropped_snapshot, " +
                                "(SELECT COUNT(*) FROM tables s JOIN table_versions w " +
                                "ON w.table_id = s.id AND w.end_snapshot IS NULL " +
                                "WHERE s.dropped_snapshot IS NULL AND w.namespace = ? AND w.name = ? " +
                                "AND s.table_uuid <> ?) " +
                                "FROM tables t WHERE t.id = ?",
                            t.namespace,
                            t.name,
                            expected.toString(),
                            t.tableId,
                        ).firstRow()
                    if (row != null && row[0] != null && row[1].asLong() == 0L) {
                        HoglakeException.TableDropped(
                            "table '$qname' in catalog '$catalog' was dropped at snapshot " +
                                "${row[0].asLong()}; stop, or recreate it",
                        )
                    } else {
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
            tx.expectOne(
                "SELECT COUNT(*) FROM tables WHERE id = ? AND dropped_snapshot IS NULL",
                t.tableId,
            ) {
                // Classify AFTER the rollback: the failure lambda runs
                // once the transaction is already dead, so this read sees
                // the state that refused us.
                val row =
                    store.queryCatalog(
                        catalog,
                        "SELECT dropped_snapshot FROM tables WHERE id = ?",
                        t.tableId,
                    ).firstRow()
                if (row != null && row[0] != null) {
                    HoglakeException.TableDropped(
                        "table '$qname' in catalog '$catalog' was dropped at snapshot " +
                            "${row[0].asLong()}; stop, or recreate it",
                    )
                } else {
                    HoglakeException.NotFound("table '$qname' in catalog '$catalog'")
                }
            }
        }

        // 2a. The schema fence, PER TOUCHED TABLE: planning (specs,
        // column types, stats validation) ran against the table's schema
        // as resolved; an ALTER that landed since invalidates that
        // planning even when the request carries no read_snapshot (a
        // blind append). Per-table — not the catalog-wide
        // meta.schema_version — so a createTable or an ALTER of an
        // UNRELATED table never replans (or worse, falsely 409s) a blind
        // append in a busy catalog. Failure replans once via the
        // commit() wrapper.
        for (t in resolved.values) {
            tx.expectScalar(
                t.schemaVersion,
                "SELECT schema_version FROM tables WHERE id = ?",
                t.tableId,
            ) { SCHEMA_CHANGED }
        }

        // 2b. The expiry floor, in-tx: a sweep that advanced the floor
        // past this commit's read snapshot since planning must not let
        // the commit's conflict window silently empty (the change rows
        // are already purged). Same 410 the pre-flight check answers.
        if (readSnapshot != null) {
            tx.expectZero(
                "SELECT COUNT(*) FROM meta WHERE id = 1 AND earliest_snapshot_id > ?",
                readSnapshot,
            ) {
                HoglakeException.Expired(
                    "read_snapshot $readSnapshot is below the expiry floor; reconcile from a full " +
                        "scan at a retained snapshot",
                )
            }
        }

        // 2c. Conflict guards for the race between the pre-flight read
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
                "SELECT COUNT(*) FROM removals WHERE drained_at IS NULL " +
                    "AND path IN (${allPaths.keys.joinToString(",") { "?" }})",
                *allPaths.keys.toTypedArray(),
            ) {
                HoglakeException.CommitConflict(
                    "a file path in this commit is queued for removal; claim a fresh path and retry",
                )
            }
        }

        // Target resolution follows merged registration order, AFTER
        // executed path protection and BEFORE allocation or mutation.
        // An ambiguous slot keeps its position without choosing an append.
        for (p in plannedDeletes) {
            val dv = p.registration
            val qname = "${p.table.namespace}.${p.table.name}"
            if (p.refusal != null) {
                tx.expectZero("SELECT 1") { p.refusal }
            } else if (p.stagedTarget != null) {
                val target = plannedAppends[p.stagedTarget]
                if (dv.deleteCount > target.file.recordCount) {
                    tx.expectZero("SELECT 1") {
                        HoglakeException.Validation(
                            "delete_count ${dv.deleteCount} exceeds record_count of the staged file " +
                                "${target.file.path} in $qname",
                        )
                    }
                }
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
                // Target liveness, typed like Postgres's planDeletes:
                // retired AFTER the writer's read is the retryable
                // conflict (compaction landed mid-flight); retired AT OR
                // BEFORE it is the stale-request refusal. Without these a
                // DV can land on a dead compaction input and its deletes
                // never apply to the replacement's rows.
                tx.expectZero(
                    "SELECT COUNT(*) FROM files WHERE id = ? AND end_snapshot IS NOT NULL " +
                        "AND end_snapshot > ?",
                    target,
                    readSnapshot,
                ) {
                    HoglakeException.CommitConflict(
                        "delete for $qname targets data_file_id $target, which was retired after " +
                            "read snapshot $readSnapshot",
                    )
                }
                tx.expectOne(
                    "SELECT COUNT(*) FROM files WHERE id = ? AND end_snapshot IS NULL",
                    target,
                ) {
                    HoglakeException.Validation(
                        "delete for $qname targets data_file_id $target, which was retired at or " +
                            "before read snapshot $readSnapshot",
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
            }
        }

        // The DV object-prefix refusal, after every per-target guard
        // above (CommitService throws it after its planDeletes loop, for
        // the first table with violations).
        dvViolations.entries.firstOrNull()?.let { (tableId, violations) ->
            val table = resolved.values.first { it.tableId == tableId }
            val layout = layouts.getValue(tableId)!!
            tx.expectZero("SELECT ?", violations.size.toLong()) {
                ObjectPrefixRules.violation(table.namespace, table.name, meta.dataPath, layout, violations)
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

        fun allocatedId(offset: Int): String = "(SELECT next_file_id - $totalFiles + $offset FROM meta WHERE id = 1)"

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

        // 6. Deletes: history-preserving epoch writes. The live-epoch
        // invariant is a POSTCONDITION of each durable supersession.
        plannedDeletes.forEachIndexed { i, p ->
            if (p.refusal != null) return@forEachIndexed
            val idSql = allocatedId(plannedAppends.size + i)
            val dv = p.registration
            val qname = "${p.table.namespace}.${p.table.name}"
            if (p.stagedTarget != null) {
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
                // Supersession ends the live predecessor atomically
                // (Postgres's ended_dvs): exactly one DV per data file is
                // live at any head, which compaction's candidate join and
                // its GUARD 5, expiry's purge, and storage accounting all
                // assume. The UPDATE runs BEFORE the insert so the new
                // epoch stays live.
                tx.add(
                    "UPDATE dvs SET end_snapshot = ${HogliteSchema.HEAD_SQL} " +
                        "WHERE data_file_id = ? AND end_snapshot IS NULL",
                    target,
                )
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
                // The invariant, asserted: after supersession exactly one
                // live epoch remains for this file.
                tx.expectScalar(
                    1,
                    "SELECT COUNT(*) FROM dvs WHERE data_file_id = ? AND end_snapshot IS NULL",
                    target,
                ) {
                    HoglakeException.CommitConflict(
                        "deletion vector supersession for data_file_id $target in $qname violated " +
                            "the one-live-epoch invariant (a store bug; rolled back cleanly)",
                    )
                }
            }
        }

        // 7. Upload claim settlement (the decision was pre-flight).
        // The settle re-asserts the FULL claim predicate — active AND
        // ours — so an abandonment or a fence that landed between
        // pre-flight and this transaction stops the publication rather
        // than being overwritten back to registered.
        for (claimId in claimsToSettle) {
            tx.affectOne(
                "UPDATE uploads SET state = 'registered' WHERE upload_id = ? AND state = 'active' AND owner = ?",
                claimId,
                request.idempotencyKey?.toString(),
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

        // Only a new materialization mints an identity. Exact transport retries
        // reuse this txid, fingerprint, statement layout, arguments and timestamps.
        val txid = "ch:$catalog:${UUID.randomUUID()}"
        val results =
            try {
                store.runTx(catalog, txid, fingerprint, tx)
            } catch (e: HoglakeException.CommitConflict) {
                if (e !== RECEIPT_CHANGED) throw e
                val key = checkNotNull(request.idempotencyKey)
                // This lookup stays inside the original catalogOperation binding.
                val existing =
                    store.queryCatalog(
                        catalog,
                        "SELECT fingerprint, snapshot_id, schema_version FROM receipts WHERE idempotency_key = ?",
                        key.toString(),
                    ).firstRow()
                if (existing != null) return replayedReceipt(catalog, key, fingerprintBytes, existing)
                // Cleanup removed the receipt. Only the shared budget permits a replan.
                throw e
            }
        try {
            val result = results[readback]
            check(result.cols.size == 2 && result.rows.size == 1) { "invalid commit readback shape" }
            val row = result.rows.single()
            check(row.size == 2 && row.all { it is Int || it is Long }) { "invalid commit readback integers" }
            return CommitResult(snapshotId = row[0].asLong(), schemaVersion = row[1].asLong())
        } catch (e: Exception) {
            throw HoglakeException.CommitOutcomeUnknown(txid, "invalid commit readback: ${e.message}")
        }
    }

    private fun replayedReceipt(
        catalog: String,
        key: UUID,
        fingerprint: ByteArray,
        row: List<Any?>,
    ): CommitResult {
        val stored = row.getOrNull(0) as? String
        val valid =
            stored != null && stored.length == COMMIT_FINGERPRINT_LENGTH * 2 &&
                stored.all { it in '0'..'9' || it in 'a'..'f' || it in 'A'..'F' }
        if (!valid || row.size != 3 || row.drop(1).any { it !is Int && it !is Long }) {
            throw HoglakeException.Validation("idempotency_key $key has an invalid stored receipt")
        }
        val digest = checkNotNull(stored).chunked(2).map { it.toInt(16).toByte() }.toByteArray()
        if (digest[0] == COMMIT_FINGERPRINT_VERSION) {
            if (!digest.contentEquals(fingerprint)) {
                throw HoglakeException.Validation("idempotency_key $key was already used with a different payload")
            }
        } else {
            Metrics.commitReceiptUnknownDigestVersion(catalog)
        }
        return CommitResult(row[1].asLong(), row[2].asLong())
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
        val expectedUuids = linkedSetOf<UUID>()
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
        /** Exact-one append index, or null for a durable or unresolved target. */
        val stagedTarget: Int?,
        /** An ambiguous staged target refuses at this slot, never by selecting an append. */
        val refusal: HoglakeException.Validation? = null,
    )

    /** A durable DV target's own spec fields and partition values, as the prefix check reads them. */
    private class DurableTarget(
        val specFields: List<PartitionFieldDef>?,
        val partitionValues: List<String?>?,
    )

    /**
     * Every read one commit attempt plans against, sent as ONE read-only
     * hoglite transaction ([HogliteCatalogStore.readCatalog]) on the
     * operation's bound incarnation.
     *
     * They used to be separate `/query` calls: 8 for an unkeyed append to
     * one table, 10 with an idempotency key and a read_snapshot. hoglite
     * parks each read until its snapshot is durable, and a parked read
     * holds one of the tenant's reader slots while it waits, so under
     * concurrent writers a commit's own reads queued behind each other's
     * parks. One transaction takes one slot and parks once.
     *
     * One snapshot is also the basis the separate reads approximated. The
     * meta row, the receipt, every table row (with its schema_version
     * fence), the definitions at the plan head, the claims and the
     * conflict window are one moment. The plan head is that snapshot's
     * meta.head, so every DDL the fence admits is visible to planning.
     *
     * Table ids are unknown while the statements are built, so a per-table
     * read names its table by [LIVE_TABLE_ID_SQL] (the live resolution
     * [table] decodes, as a subquery) and the plan head by
     * [HogliteSchema.HEAD_SQL]. Both evaluate inside the same snapshot as
     * the table and meta rows, so they are the id and head read back.
     *
     * Not batched, because they only run once the commit is refused: the
     * dropped-incarnation probe after a table miss and the post-rollback
     * classifiers of the guards.
     */
    private class PlanReads private constructor(
        private val store: HogliteCatalogStore,
        private val catalog: String,
    ) {
        private val statements = mutableListOf<HogliteStatement>()
        private lateinit var results: List<HogliteQueryResult>

        private var metaAt = -1
        private var receiptAt: Int? = null
        private val namespaceAt = HashMap<String, Int>()
        private val tableAt = HashMap<String, Int>()
        private val specAt = HashMap<String, Int>()
        private val columnTypesAt = HashMap<String, Int>()
        private val layoutAt = HashMap<String, Int>()
        private var claimsAt: Int? = null
        private var retainedAt: Int? = null
        private var conflictsAt: Int? = null
        private var durableAt: Int? = null

        private fun add(
            sql: String,
            args: List<Any?>,
        ): Int {
            statements += HogliteStatement(sql, args)
            return statements.size - 1
        }

        /** The catalog's meta row; a missing one is the missing catalog, as [HogliteCatalogStore.meta]. */
        fun meta(): HogliteCatalogStore.MetaRow =
            store.metaRow(
                results[metaAt].firstRow() ?: throw HoglakeException.NotFound("catalog '$catalog'"),
            )

        /** (fingerprint, snapshot_id, schema_version) of the request's key, or null. */
        fun receipt(): List<Any?>? = results[checkNotNull(receiptAt)].firstRow()

        /** [HogliteCatalogStore.liveTable]'s answer and refusals for [t]. */
        fun table(t: Touched): HogliteCatalogStore.TableRow {
            val key = key(t.namespace, t.table)
            if (results[namespaceAt.getValue(key)].firstRow() == null) {
                throw HoglakeException.NotFound("namespace '${t.namespace}' in catalog '$catalog'")
            }
            val row =
                results[tableAt.getValue(key)].firstRow()
                    ?: throw HoglakeException.NotFound("table '${t.namespace}.${t.table}' in catalog '$catalog'")
            return store.tableRowOf(row)
        }

        /** The live partition spec at the plan head of an appended table ([HogliteCatalogStore.specAt]). */
        fun spec(table: HogliteCatalogStore.TableRow): PartitionSpec? =
            results[specAt.getValue(key(table.namespace, table.name))].firstRow()?.let { r ->
                PartitionSpec(r[0].asLong(), store.json.readValue<List<PartitionFieldDef>>(r[1].asString()))
            }

        /** The live field_id -> type map stats validation checks against (every node, containers included). */
        fun columnTypes(table: HogliteCatalogStore.TableRow): Map<Long, ColType> =
            results[columnTypesAt.getValue(key(table.namespace, table.name))].rows
                .associate { it[0].asLong() to ColType.fromWire(it[1].asString()) }

        /**
         * The table's object-prefix layout at the plan head, or null when
         * the option is off: CommitService.layoutOf over `table_layouts`
         * and the live top-level columns. A layout row whose column is not
         * a live top-level column of an allowed type is a broken invariant
         * (the DDL refuses dropping, renaming, or promoting the field out
         * of the allowed set): a 500, never a commit that silently skips
         * the check.
         */
        fun layout(table: HogliteCatalogStore.TableRow): ObjectPrefixLayout? {
            val row = results[layoutAt.getValue(key(table.namespace, table.name))].firstRow() ?: return null
            val fieldId = row[0].asLong()
            val name = row[1] as? String
            val type = (row[2] as? String)?.let { ColType.fromWire(it) }
            check(name != null && type != null && type in ObjectPrefix.ALLOWED_TYPES) {
                "object-prefix field_id $fieldId of table_id=${table.tableId} is not a live column of an allowed " +
                    "type (name=$name, type=${type?.wire})"
            }
            return ObjectPrefixLayout(fieldId, name, type)
        }

        /** The upload rows of the commit's paths. */
        fun claims(): List<List<Any?>> = claimsAt?.let { results[it].rows } ?: emptyList()

        /** (path, kind) still held by a file or DV row, for every claimed path. */
        fun retainedPaths(): Set<Pair<String, String>> = retainedAt?.let { retainedPathSet(results[it]) } ?: emptySet()

        /** (object_id, is_ddl) of the changes in the conflict window since read_snapshot. */
        fun conflicts(): HogliteQueryResult = results[checkNotNull(conflictsAt)]

        /**
         * [dataFileId]'s spec (its OWN, retired or not — expiry keeps every
         * spec a file can name) and partition values, or null when it is
         * not a file of [table] (the transaction's guards refuse that
         * first). Both are immutable for a file id, so a pre-flight read is
         * exact.
         */
        fun durableTarget(
            table: HogliteCatalogStore.TableRow,
            dataFileId: Long,
        ): DurableTarget? {
            val row =
                results[checkNotNull(durableAt)].rows.firstOrNull {
                    it[0].asLong() == dataFileId && it[1].asLong() == table.tableId
                } ?: return null
            return DurableTarget(
                specFields = (row[3] as? String)?.let { store.json.readValue<List<PartitionFieldDef>>(it) },
                partitionValues = (row[2] as? String)?.let { store.json.readValue<List<String?>>(it) },
            )
        }

        companion object {
            private fun key(
                namespace: String,
                table: String,
            ) = "$namespace.$table"

            fun read(
                store: HogliteCatalogStore,
                catalog: String,
                request: CommitRequest,
                touched: Collection<Touched>,
                appends: List<TableAppend>,
                deletes: List<TableDeletes>,
            ): PlanReads {
                val p = PlanReads(store, catalog)
                p.metaAt = p.add(store.metaSql, emptyList())
                request.idempotencyKey?.let {
                    p.receiptAt =
                        p.add(
                            "SELECT fingerprint, snapshot_id, schema_version FROM receipts WHERE idempotency_key = ?",
                            listOf(it.toString()),
                        )
                }
                val head = HogliteSchema.HEAD_SQL
                for (t in touched) {
                    val key = key(t.namespace, t.table)
                    val id = listOf(t.namespace, t.table)
                    p.namespaceAt[key] =
                        p.add(
                            "SELECT id, name FROM namespaces WHERE name = ? AND dropped_snapshot IS NULL",
                            listOf(t.namespace),
                        )
                    p.tableAt[key] =
                        p.add(
                            "SELECT t.id, t.table_uuid, v.namespace_id, v.namespace, v.name, v.comment, " +
                                "v.properties_json, t.next_row_id, t.schema_version FROM table_versions v " +
                                "JOIN tables t ON t.id = v.table_id " +
                                "WHERE v.namespace_id = ($LIVE_NAMESPACE_ID_SQL) AND v.name = ? " +
                                "AND v.end_snapshot IS NULL AND t.dropped_snapshot IS NULL",
                            id,
                        )
                    if (t.appends) {
                        p.specAt[key] =
                            p.add(
                                "SELECT spec_id, fields_json FROM partition_specs " +
                                    "WHERE table_id = $LIVE_TABLE_ID_SQL " +
                                    "AND begin_snapshot <= $head AND (end_snapshot IS NULL OR end_snapshot > $head) " +
                                    "ORDER BY spec_id DESC LIMIT 1",
                                id,
                            )
                        p.columnTypesAt[key] =
                            p.add(
                                "SELECT field_id, type FROM columns WHERE table_id = $LIVE_TABLE_ID_SQL " +
                                    "AND end_snapshot IS NULL",
                                id,
                            )
                    }
                    // The layout field and its live top-level column, at the plan head.
                    p.layoutAt[key] =
                        p.add(
                            "SELECT l.object_prefix_field_id, c.name, c.type FROM table_layouts l " +
                                "LEFT JOIN columns c ON c.table_id = l.table_id " +
                                "AND c.field_id = l.object_prefix_field_id AND c.parent_field_id IS NULL " +
                                "AND c.begin_snapshot <= $head " +
                                "AND (c.end_snapshot IS NULL OR c.end_snapshot > $head) " +
                                "WHERE l.table_id = $LIVE_TABLE_ID_SQL " +
                                "AND l.begin_snapshot <= $head " +
                                "AND (l.end_snapshot IS NULL OR l.end_snapshot > $head) " +
                                "ORDER BY c.ordinal",
                            id,
                        )
                }

                val paths = LinkedHashSet<String>()
                appends.forEach { a -> a.files.forEach { paths += it.path } }
                deletes.forEach { d -> d.files.forEach { paths += it.path } }
                if (paths.isNotEmpty()) {
                    val inList = paths.joinToString(",") { "?" }
                    p.claimsAt = p.add(CLAIMS_SQL + "WHERE path IN ($inList)", paths.toList())
                    // Only claimed paths are ever looked up in this set, so
                    // the read is limited to them: a commit without claims
                    // probes no file or DV row.
                    val claimed = "SELECT path FROM uploads WHERE path IN ($inList)"
                    p.retainedAt =
                        p.add(
                            "SELECT path, 0 FROM files WHERE path IN ($claimed) " +
                                "UNION SELECT path, 1 FROM dvs WHERE path IN ($claimed)",
                            paths.toList() + paths.toList(),
                        )
                }

                val readSnapshot = request.readSnapshot
                if (readSnapshot != null) {
                    // The window covers every touched table for DDL; the
                    // content arm (an empty IN-list is not SQL) only the
                    // unchanged delete targets.
                    val all = touched.toList()
                    val unchanged = if (request.requireUnchangedTables) all.filter { it.deletes } else emptyList()
                    val contentArm =
                        if (unchanged.isEmpty()) {
                            ""
                        } else {
                            " OR (kind IN $CONTENT_KINDS AND object_id IN " +
                                "(${unchanged.joinToString(",") { LIVE_TABLE_ID_SQL }}))"
                        }
                    p.conflictsAt =
                        p.add(
                            "SELECT DISTINCT object_id, CASE WHEN kind IN $DDL_KINDS THEN 1 ELSE 0 END FROM changes " +
                                "WHERE snapshot_id > ? " +
                                "AND object_id IN (${all.joinToString(",") { LIVE_TABLE_ID_SQL }}) " +
                                "AND (kind IN $DDL_KINDS$contentArm)",
                            buildList {
                                add(readSnapshot)
                                all.forEach { addAll(listOf(it.namespace, it.table)) }
                                unchanged.forEach { addAll(listOf(it.namespace, it.table)) }
                            },
                        )
                }

                // Durable DV targets, for the object-prefix check on layout
                // tables; by primary key, filtered to the DV's table in Kotlin.
                val targets =
                    deletes.flatMap { d -> d.files.filter { it.dataFilePath == null && it.dataFileId > 0 } }
                        .map { it.dataFileId }.distinct()
                if (targets.isNotEmpty()) {
                    p.durableAt =
                        p.add(
                            "SELECT f.id, f.table_id, f.partition_values_json, s.fields_json FROM files f " +
                                "LEFT JOIN partition_specs s ON s.table_id = f.table_id AND s.spec_id = f.spec_id " +
                                "WHERE f.id IN (${targets.joinToString(",") { "?" }})",
                            targets,
                        )
                }

                p.results = store.readCatalog(catalog, p.statements)
                return p
            }
        }
    }

    /** The live namespace id of a name, as [HogliteCatalogStore.liveNamespace] resolves it. One `?`: the name. */
    private const val LIVE_NAMESPACE_ID_SQL = "SELECT id FROM namespaces WHERE name = ? AND dropped_snapshot IS NULL"

    /**
     * The live table id of (namespace, name), as [HogliteCatalogStore.liveTable]
     * resolves it, for a read that cannot bind the id yet. Two `?`: namespace, name.
     */
    private const val LIVE_TABLE_ID_SQL =
        "(SELECT t.id FROM table_versions v JOIN tables t ON t.id = v.table_id " +
            "WHERE v.namespace_id = ($LIVE_NAMESPACE_ID_SQL) AND v.name = ? " +
            "AND v.end_snapshot IS NULL AND t.dropped_snapshot IS NULL)"

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
        file.validateFooterSize()
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
                CLAIMS_SQL + "WHERE path IN (${paths.keys.joinToString(",") { "?" }})",
                *paths.keys.toTypedArray(),
            ).rows
        if (claims.isEmpty()) return emptyList()
        val retainedPaths =
            retainedPathSet(
                store.queryCatalog(
                    catalog,
                    "SELECT path, 0 FROM files WHERE path IN (${paths.keys.joinToString(",") { "?" }}) " +
                        "UNION SELECT path, 1 FROM dvs WHERE path IN (${paths.keys.joinToString(",") { "?" }})",
                    // toList() first: Set + Set is the Set overload and DEDUPLICATES the
                    // doubled keys, shorting the second IN-list its arguments.
                    *(paths.keys.toList() + paths.keys).toTypedArray(),
                ),
            )
        return settleDecision(claims, retainedPaths, paths, owner)
    }

    /** The upload rows a claim decision reads; the caller appends the path filter. */
    private const val CLAIMS_SQL = "SELECT upload_id, path, file_kind, state, owner FROM uploads "

    /**
     * Immutable objects still referenced at a retained snapshot may be
     * referenced again; ownership never permits rewriting them. Numeric
     * kind markers, not the 'data'/'delete' strings: hoglite's /query gate
     * scans the TEXT for DML words, and a 'delete' literal is one. The
     * mapping back happens here.
     */
    private fun retainedPathSet(result: HogliteQueryResult): Set<Pair<String, String>> =
        result.rows.map { it[0].asString() to (if (it[1].asLong() == 0L) "data" else "delete") }.toSet()

    /**
     * [decideClaims]'s decision over rows already read: [claims] are the
     * upload rows of [paths], [retainedPaths] the (path, kind) pairs a
     * file or DV row still holds, for at least every claimed path.
     */
    private fun settleDecision(
        claims: List<List<Any?>>,
        retainedPaths: Set<Pair<String, String>>,
        paths: Map<String, String>,
        owner: UUID?,
    ): List<String> {
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
}

// The schema-fence failure, matched by IDENTITY in commit()'s retry
// wrapper: a guard lambda returns this one instance, classify passes it
// through, and only a reference-equal catch means "replan", never a
// lookalike CommitConflict from another guard.
private val SCHEMA_CHANGED =
    HoglakeException.CommitConflict("concurrent DDL changed the schema while this commit was being planned")

// Only this registered guard marker permits receipt recovery after a rollback.
private val RECEIPT_CHANGED = HoglakeException.CommitConflict("receipt changed during publication")

// Blind-partitioned WARN dedupe, per (catalog, table), for the process's life.
private val WARNED_BLIND_PARTITIONED = java.util.concurrent.ConcurrentHashMap.newKeySet<String>()
