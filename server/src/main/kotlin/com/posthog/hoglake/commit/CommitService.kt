package com.posthog.hoglake.commit

import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.CommitResult
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.ObjectPrefix
import com.posthog.hoglake.model.ObjectPrefixLayout
import com.posthog.hoglake.model.StatsSanity
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.pathStaysUnder
import com.posthog.hoglake.model.validateFooterSize
import com.posthog.hoglake.model.validateSplitOffsets
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.storedPayloadObjectMapper
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import java.util.UUID

/**
 * The commit endpoint (README.md commit-protocol / commit-serialization
 * sections): register client-written parquet files with footer-derived
 * stats and/or deletion-vector (DV) files, producing one snapshot.
 *
 * One SQL transaction per commit. The tail is serialized per catalog by
 * a Postgres advisory xact lock (taken up front, before any allocation),
 * so snapshot ids, file ids, and per-table row-id ranges are dense and
 * ordered with commit order — id order = commit order by construction,
 * and per-file row-id ranges are never reused (THE lineage guarantee).
 *
 * Appends and partitioning: if the touched table has a live partition
 * spec, every registered file must carry partition_values with arity
 * matching the spec's field count; the values are opaque TRANSFORMED
 * strings (no server-side transform computation in this milestone) and
 * land in hog_file_partition_value, with hog_data_file.spec_id binding
 * the file to the spec it was written under. On an unpartitioned table
 * partition_values are forbidden.
 *
 * Deletes (deletion vectors): each DeleteFileRegistration supersedes the
 * target data file's current live DV (there is at most one, enforced by
 * a unique partial index). Vectors only grow — the new delete_count must
 * cover the superseded one — and a DV registered after the writer's
 * readSnapshot means the writer built on a stale vector, which is a
 * CommitConflict (retryable), so readSnapshot is REQUIRED whenever
 * deletes are present. Deleted-from tables get the same table-level
 * DDL conflict check as appends ('table_dropped'/'table_altered' after
 * readSnapshot).
 *
 * A commit may mix appends and deletes, including on the same table;
 * appends are applied first. Ordinary commits cannot delete newly created files.
 * Explicit DML transactions may reference a same-commit append by path, reflecting
 * a read of their private staged files; ordinary numeric IDs retain snapshot checks.
 *
 * NOTE on stats: deletes do NOT touch hog_table_stats. record_count /
 * file_size_bytes remain the gross append counters (and next_row_id the
 * lineage allocator); net row liveness is computed at read time from the
 * live DVs, never denormalized here.
 *
 * Conflict rule (append fast path): appends only conflict with DDL on
 * the tables they touch. With a non-null readSnapshot we look for
 * 'table_dropped' / 'table_altered' changes on the touched tables since
 * that snapshot — one indexed lookup — and a pure-DDL refusal is typed
 * apart as `ddl_since_read_snapshot` (re-prepare; replaying the same
 * payload can never succeed) from the retryable `commit_conflict` a
 * row-content conflict gets. THAT is what makes a prepared append a
 * full check-and-set on the table's shape: a spec or schema change
 * between the writer's read and its commit is this refusal, atomically
 * and with zero writes.
 *
 * A null readSnapshot is a blind append with no conflict window, legal
 * only when the commit has no deletes AND no file carries
 * partition_values — partition values are writer-computed against one
 * spec and a blind append could register them under another (#233).
 *
 * requireUnchangedTables adds the read-set rule ON TOP of that, and only
 * for the tables the request DELETES from: those additionally conflict
 * with 'table_created' / 'table_inserted_into' / 'table_deleted_from'
 * since readSnapshot, because a delete is planned against a read of the
 * target's rows. A table a guarded request only APPENDS to keeps the
 * DDL-only rule — appends never conflict with appends, inside a
 * transaction as much as outside one. See [checkConflicts].
 */
class CommitService(
    private val jdbi: Jdbi,
    /**
     * lock_timeout for the commit transaction's advisory-lock wait
     * (HOGLAKE_COMMIT_LOCK_TIMEOUT_MS; B2 admission control). 0 =
     * unbounded. Expiry surfaces as CommitQueueTimeout -> 503.
     */
    private val commitLockTimeoutMs: Long = DEFAULT_COMMIT_LOCK_TIMEOUT_MS,
    /**
     * Whether a blind append carrying partition_values is REFUSED (422)
     * or merely warned about
     * (HOGLAKE_REFUSE_BLIND_PARTITIONED_APPENDS, default false).
     *
     * The rule itself is invariant 12 and is not optional; the flag only
     * says when it starts biting, because `duckdb-client` sends exactly
     * that shape today. Flip it once the extension takes a snapshot for
     * partitioned appends.
     */
    private val refuseBlindPartitionedAppends: Boolean = false,
) {
    companion object {
        /** Default commit admission bound: 30s (Config's default mirrors it). */
        const val DEFAULT_COMMIT_LOCK_TIMEOUT_MS: Long = 30_000

        /**
         * Per-file record_count sanity cap (2^48 ≈ 281T rows). No real
         * parquet file gets anywhere close; the cap keeps a hostile
         * registration from racing the row-id allocator toward overflow.
         */
        private const val MAX_FILE_RECORD_COUNT: Long = 1L shl 48

        /**
         * Cap on the blind-partitioned WARN's dedupe set — see
         * `warnedBlindPartitioned`. Sized well past any real catalog's
         * table count; it is defence in depth, not the primary bound,
         * which is that the key holds nothing a client chooses.
         */
        private const val MAX_WARNED_BLIND_PARTITIONED: Int = 512

        /** Which table(s) the blind-partitioned refusal is about. */
        internal fun blindPartitionedRefusal(qualified: List<String>): String =
            "append to table(s) ${qualified.joinToString(", ")}"

        /**
         * The 422 a blind partitioned append gets once
         * HOGLAKE_REFUSE_BLIND_PARTITIONED_APPENDS is on — and the text
         * the WARN quotes until then, so the message a client will
         * eventually be refused with is the one it is warned about.
         * Prefixed by [blindPartitionedRefusal] so it names the tables.
         */
        internal const val BLIND_PARTITIONED_REFUSAL: String =
            "read_snapshot is required when an append carries partition_values: partition " +
                "values are only valid under the spec they were computed with, and a blind " +
                "append has no conflict window in which a spec change could be detected"

        /**
         * THE PATH-REUSE GUARD, issued once per commit under the
         * per-catalog commit lock (invariant 4) — VERBATIM as the `queued`
         * CTE of [CommitTail.READ_SQL], and on its own by the
         * atomic-creation tail. `internal` so
         * `V16FileRemovalPathIndexMigrationIntegrationTest` can EXPLAIN
         * the statement production runs instead of a hand-written
         * lookalike — V14's first index was proven by a predicate no
         * code path sends, and was on the wrong column.
         *
         * Its access path is `hog_file_removal_undrained_path`
         * (catalog_id, path) WHERE drained_at IS NULL, added in V16;
         * before it, this statement read every undrained row in the
         * catalog on every commit (#199). The `drained_at IS NULL`
         * clause is not decorative: it is both the correctness
         * predicate (a settled row no longer owns its path) and the
         * index's own partial predicate, so dropping it costs the index
         * as well as the meaning.
         *
         * IT IS UNCHANGED BY THE CLEANUP CLAIM (V21), and that is the
         * half of the drain's safety argument that lives here. A claimed
         * row is still an UNDRAINED row, so a commit registering that
         * path still gets a 409 until it settles — which is why the
         * drain can delete an object with no lock at all. The pair is:
         * this guard refuses to make a queued path live, and the drain
         * only ever reads rows that are still queued.
         *
         * WHAT REMAINS IS A QUEUE INSERT RACING A REGISTRATION, and there
         * are FOUR inserters with THREE serializers — stated here because
         * an earlier version of this argument said "every queue insert
         * happens under the commit lock" and two of the four do not:
         *
         *  - `ExpiryService` / `RetirementService`: under the commit
         *    lock, and only for paths whose file rows they just deleted;
         *  - `CompactionService.stageOutputPath`'s INITIAL stage: NO
         *    lock (autocommit, before the rewrite), safe because the path
         *    is a UUID that sweep just minted and nobody else knows. Its
         *    re-stage runs inside `commitGroup` and so under the lock;
         *  - `UploadService.fenceAndQueue`: NO commit lock, deliberately.
         *    Serialized by the `hog_upload` ROW LOCK — `UploadService
         *    .register` takes `FOR UPDATE` on the claim rows inside THIS
         *    transaction and raises `CommitConflict` unless the claim is
         *    still active, while the sweep re-evaluates its candidate
         *    predicate after waiting on that lock. This is the arm the
         *    removed commit lock was standing in for.
         *
         * The standing constraint that follows: a commit path that
         * registers a path with NO `hog_upload` claim has no serializer
         * against a concurrent queue insert for that path. Unreachable
         * today (paths are never reused, and every externally-supplied
         * path arrives through an upload claim) — a future one must bring
         * its own serializer.
         *
         * Binds `:catalogId` and the `:paths` array. No interpolated
         * values (invariant 9 intact).
         */
        internal const val REMOVAL_QUEUE_COLLISION_SQL: String =
            """
            SELECT DISTINCT path FROM hog_file_removal
            WHERE catalog_id = :catalogId AND drained_at IS NULL AND path = ANY(:paths)
            """
    }

    /**
     * Commit entry point. Metrics + audit are emitted HERE, after the
     * transaction has committed (or rolled back on the way out as an
     * exception) — never inside it.
     */
    fun commit(
        catalog: String,
        request: CommitRequest,
        /**
         * The caller's User-Agent, for the blind-partitioned WARN — the
         * line has to name WHICH client so the fleet can be moved before
         * the refusal is switched on.
         *
         * A parameter and NOT a CommitRequest field, deliberately: the
         * request is what the receipt's fingerprint is taken over
         * (commitFingerprint serializes all of it), so a user agent in
         * there would make a replay from a different client version fail
         * the "same key, same request" check.
         */
        userAgent: String? = null,
    ): CommitResult {
        val files = request.appends.sumOf { it.files.size }
        val deletes = request.deletes.sumOf { it.files.size }
        val result =
            try {
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
                // Canonicalizing large registrations needs neither a connection nor
                // the catalog lock. Receipt comparison and publication stay locked.
                val requestJson = request.idempotencyKey?.let { commitFingerprint(request) }
                // The digest is taken HERE, outside the transaction and the
                // catalog lock, for the reason the line above canonicalizes
                // here: hashing 1.5 MB of canonical JSON is ~1 ms of CPU and
                // it is nobody else's business. Both halves are kept because
                // a pre-V24 receipt still has to be compared as a STRING —
                // see the fallback in [doCommit].
                val fingerprint = requestJson?.let { commitFingerprintDigest(it) }
                jdbi.inTransaction<CommitResult, RuntimeException> { handle ->
                    doCommit(handle, catalog, request, requestJson, fingerprint, userAgent)
                }
            } catch (e: HoglakeException) {
                Metrics.commitFailureResult(e)?.let { Metrics.commitRecorded(catalog, it) }
                Audit.event(
                    action = "commit",
                    catalog = catalog,
                    obj = null,
                    outcome = Audit.failureOutcome(e),
                    detail = e.message,
                )
                throw e
            } catch (e: Throwable) {
                // An unexpected failure mid-commit (bug, dead pool, broken
                // state) must still leave an audit trace and a counter tick
                // before it becomes the API's 500 — silence here was the
                // adversarial-review finding.
                Metrics.commitRecorded(catalog, "error")
                Audit.event(
                    action = "commit",
                    catalog = catalog,
                    obj = null,
                    outcome = "error",
                    detail = e.message,
                )
                throw e
            }
        Metrics.commitRecorded(catalog, "committed")
        Audit.event(
            action = "commit",
            catalog = catalog,
            obj = null,
            outcome = "committed",
            detail = "snapshot=${result.snapshotId} files=$files deletes=$deletes",
        )
        return result
    }

    /** A missing receipt is not a fence: an in-flight commit may still publish. */
    fun receipt(
        catalog: String,
        operation: UUID,
    ): CommitResult =
        jdbi.withHandle<CommitResult, RuntimeException> { h ->
            h.createQuery(
                """
                SELECT r.snapshot_id, r.schema_version FROM hog_commit_receipt r
                JOIN hog_catalog c USING (catalog_id)
                WHERE c.name = :catalog AND r.idempotency_key = :operation
                """,
            ).bind("catalog", catalog).bind("operation", operation)
                .map { rs, _ -> CommitResult(rs.getLong("snapshot_id"), rs.getLong("schema_version")) }
                .findOne().orElseThrow { HoglakeException.NotFound("commit receipt '$operation'") }
        }

    /** Register initial files in a caller-owned DDL snapshot, under the catalog lock. */
    internal fun registerInitialFiles(
        h: Handle,
        catalogId: Long,
        dataPath: String,
        namespace: String,
        table: String,
        tableId: Long,
        snapshotId: Long,
        files: List<FileRegistration>,
        uploadOwner: UUID? = null,
    ) {
        val request = CommitRequest(appends = listOf(TableAppend(namespace, table, files)))
        validatePathsUnderDataPath(dataPath, request)
        // No object-prefix layout, and none to read: the table was created
        // in THIS transaction, a creation definition cannot carry the
        // option, and replacing a table that has it is refused at prepare
        // (TableCreationService.prepare, with CatalogService.createTable as
        // the backstop). So the layout check is skipped here by
        // construction, not by omission.
        val append =
            validateFiles(
                ResolvedAppend(namespace, table, tableId, files, liveSpec(h, catalogId, tableId), layout = null),
                liveColumnTypes(h, catalogId, tableId),
            )
        com.posthog.hoglake.service.UploadService.register(h, catalogId, uploadOwner, files.map { it.path to "data" })
        checkRemovalQueueCollisions(h, catalogId, listOf(append), emptyList())
        if (files.isEmpty()) return
        val firstId =
            h.createQuery(
                """
                UPDATE hog_catalog SET next_file_id = next_file_id + :count WHERE catalog_id = :catalogId RETURNING
                next_file_id - :count
                """,
            ).bind("count", files.size).bind("catalogId", catalogId).mapTo(Long::class.java).one()
        h.createUpdate(
            """
            INSERT INTO hog_snapshot_change (catalog_id, snapshot_id, kind, object_id) VALUES (:catalogId,
            :snapshotId, 'table_inserted_into', :tableId)
            """,
        ).bind("catalogId", catalogId).bind("snapshotId", snapshotId).bind("tableId", tableId).execute()
        val nextRowId =
            h.createQuery(
                "SELECT next_row_id FROM hog_table_stats WHERE catalog_id = :catalogId AND table_id = :tableId",
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .mapTo(Long::class.javaObjectType)
                .findOne()
                .orElse(null)
        val (rows, _) = planAppends(listOf(append), firstId) { nextRowId }
        CommitTail.writeAppendRows(h, catalogId, snapshotId, rows)
    }

    /**
     * (catalogId, tableId) already warned about a blind partitioned
     * append, so the line fires ONCE per table per pod instead of once
     * per flush.
     *
     * Both halves are SERVER-ASSIGNED, and that is the property that
     * makes the set safe: nothing a client supplies can grow it. Two
     * earlier keys were wrong in exactly that way — the User-Agent (a
     * client minting one per request fills the cap and silences the
     * signal for everyone) and then the request's own (namespace, table)
     * STRINGS, which a client can invent 512 of without a single table
     * existing. The caller resolves first now, so an unresolvable name
     * gets the "unknown table" refusal instead of a slot here.
     *
     * The cost of keying on the table alone is that a second client on an
     * already-warned table is named only in whatever logs it writes
     * itself — acceptable, because the first line already says the table
     * needs attention, and the agent is still in the message.
     *
     * Still CAPPED at [MAX_WARNED_BLIND_PARTITIONED] as defence in depth
     * against a pathological table count; past it the line says so once
     * and stops. Never shrinks, which is fine: it stops growing the
     * moment the writers are fixed.
     *
     * Concurrent-safe without a lock because it is only ever `add`ed to
     * and `add` is what decides whether to log; `size` is a racy read,
     * used only to decide when to stop, so an over- or under-shoot of a
     * few around the cap is immaterial.
     */
    private val warnedBlindPartitioned =
        java.util.concurrent.ConcurrentHashMap.newKeySet<Pair<Long, Long>>()

    /**
     * Whether the cap's own line has been logged. A flag rather than a
     * sentinel IN the set, which is what it was: a sentinel added after
     * the `size >= cap` check let the set reach cap + 1, so the cap did
     * not mean what it said.
     */
    private val warnedBlindPartitionedCapped = java.util.concurrent.atomic.AtomicBoolean(false)

    /** hog_catalog head-of-line state, read once under the commit lock. */
    private data class CatalogHead(
        val head: Long,
        val schemaVersion: Long,
        val earliestSnapshotId: Long,
        /** Expiry-floor anchor time; null until expiry first advances the floor. */
        val earliestSnapshotTime: java.time.Instant?,
    ) {
        /** ", reached at <time>" suffix for 410 messages (TimeTravelRepo's convention). */
        fun reachedAtSuffix(): String = earliestSnapshotTime?.let { ", reached at $it" } ?: ""
    }

    private val log = KotlinLogging.logger {}

    /** Live partition spec header: id + field arity. */
    private data class LiveSpec(val specId: Long, val fieldCount: Int)

    private data class ResolvedAppend(
        val namespace: String,
        val table: String,
        val tableId: Long,
        val files: List<FileRegistration>,
        /** Live spec with >= 1 field, or null for an unpartitioned table. */
        val spec: LiveSpec?,
        /** The table's object-prefix layout, or null when the option is off. */
        val layout: ResolvedLayout?,
    )

    /**
     * An appended table's object-prefix layout as the commit read returned
     * it, with the key_index of the identity field on the prefix field in
     * the live spec: null when the spec has none or several, which the DDL
     * makes impossible and [checkObjectPrefix] reports as a broken
     * invariant.
     */
    private data class ResolvedLayout(val layout: ObjectPrefixLayout, val keyIndex: Int?)

    /** The targeted data file's row, as [planDeletes]'s per-target checks need it. */
    private data class DeleteTarget(
        val tableId: Long,
        val recordCount: Long,
        val beginSnapshot: Long,
        /** null = live; otherwise the snapshot that retired the file. */
        val endSnapshot: Long?,
    )

    private data class ResolvedDeletes(
        val namespace: String,
        val table: String,
        val tableId: Long,
        val files: List<DeleteFileRegistration>,
        /** The table's object-prefix layout, or null when the option is off. */
        val layout: ObjectPrefixLayout?,
    )

    /**
     * The commit tail. Under the lock it sends TWO statements
     * ([CommitTail]): one read of everything the checks below decide on,
     * and one write of everything the commit publishes. The checks run
     * between them in Kotlin, in the order and with the refusals they had
     * when each read was its own statement — the numbered steps below are
     * that order, and moving one moves what a client is told.
     */
    private fun doCommit(
        h: Handle,
        catalogName: String,
        req: CommitRequest,
        requestJson: String?,
        fingerprint: ByteArray?,
        userAgent: String? = null,
    ): CommitResult {
        // Resolve only the catalog before checking receipts. Replays must not
        // depend on current table, snapshot, or data-path validation.
        val (catalogId, dataPath) =
            h.createQuery("SELECT catalog_id, data_path FROM hog_catalog WHERE name = ?")
                .bind(0, catalogName)
                .map { rs, _ -> rs.getLong(1) to rs.getString(2) }
                .findOne()
                .orElseThrow { HoglakeException.NotFound("catalog '$catalogName'") }
        if (req.idempotencyKey == null) validatePathsUnderDataPath(dataPath, req)

        // 2. Merge duplicate (namespace, table) appends/deletes, preserving
        // request order (first occurrence for tables, concatenation for
        // files); they are resolved to live tables at head below. Every
        // expected_table_uuid supplied for a table is collected and checked
        // against the resolved incarnation — merged duplicates that
        // disagree cannot both match, so a stale one still conflicts.
        // Request-only, so it runs before the lock: the read needs the
        // names, and nothing here can refuse.
        val mergedAppends = LinkedHashMap<Pair<String, String>, MutableList<FileRegistration>>()
        val expectedUuids = HashMap<Pair<String, String>, MutableSet<UUID>>()
        for (append in req.appends) {
            mergedAppends.getOrPut(append.namespace to append.table) { mutableListOf() }
                .addAll(append.files)
            append.expectedTableUuid?.let {
                expectedUuids.getOrPut(append.namespace to append.table) { mutableSetOf() } += it
            }
        }
        val mergedDeletes = LinkedHashMap<Pair<String, String>, MutableList<DeleteFileRegistration>>()
        for (deletes in req.deletes) {
            mergedDeletes.getOrPut(deletes.namespace to deletes.table) { mutableListOf() }
                .addAll(deletes.files)
            deletes.expectedTableUuid?.let {
                expectedUuids.getOrPut(deletes.namespace to deletes.table) { mutableSetOf() } += it
            }
        }
        // The read's table list, in the order resolution decides refusals:
        // the appended tables, then the tables only deleted from.
        // requireUnchangedTables binds the DELETE targets, not every
        // touched table. A delete is published against a read of the
        // target's file set, so anything that moved that file set is a
        // conflict; an APPEND carries no such read, so a table this commit
        // only appends to keeps the DDL-only rule even inside a guarded
        // transaction — otherwise an append-only explicit transaction would
        // 409 on a concurrent INSERT into the same table, which is the one
        // thing appends have never done.
        val tableKeys =
            (mergedAppends.keys + mergedDeletes.keys).map { key ->
                CommitTail.TableKey(
                    namespace = key.first,
                    table = key.second,
                    appending = key in mergedAppends,
                    unchanged = req.requireUnchangedTables && key in mergedDeletes,
                )
            }
        val ordOf = tableKeys.withIndex().associate { (i, k) -> (k.namespace to k.table) to i + 1 }

        Locks.acquireCatalogCommitLock(h, catalogId, commitLockTimeoutMs)

        // ONE statement reads everything the steps below decide on: the
        // receipt, the head, every table's resolution, spec, live columns
        // and allocator, the conflict rows, the upload claims (locked), the
        // removal queue's claims on these paths, and every DV target and its
        // live vector. See CommitTail for why one snapshot serves them all.
        val read =
            CommitTail.read(
                h,
                CommitTail.ReadInputs(
                    catalogId = catalogId,
                    idempotencyKey = req.idempotencyKey,
                    tables = tableKeys,
                    readSnapshot = req.readSnapshot,
                    paths =
                        (
                            mergedAppends.values.flatMap { files -> files.map { it.path } } +
                                mergedDeletes.values.flatMap { files -> files.map { it.path } }
                        ).distinct(),
                    targetIds =
                        mergedDeletes.values.flatMap { files ->
                            files.filter { it.dataFilePath == null }.map { it.dataFileId }
                        }.distinct(),
                ),
            )

        // Under the same catalog lock as publication, so concurrent retries
        // cannot both allocate rows. A receipt is not tied to snapshot
        // expiry — it has its own retention, which the cleanup sweep
        // enforces (`HOGLAKE_RECEIPT_RETENTION_SECONDS`, 7 days; see
        // CleanupService.purgeCommitReceipts for why a receipt has to
        // outlive any payload a client may still replay).
        read.receipt?.let { return replayedReceipt(catalogName, it, requestJson, fingerprint) }

        if (req.idempotencyKey != null) validatePathsUnderDataPath(dataPath, req)

        val catalogHead =
            CatalogHead(
                head = read.head.lastSnapshotId,
                schemaVersion = read.head.schemaVersion,
                earliestSnapshotId = read.head.earliestSnapshotId,
                earliestSnapshotTime = read.head.earliestSnapshotTime,
            )
        val (head, schemaVersion) = catalogHead.head to catalogHead.schemaVersion

        if (mergedAppends.isEmpty() && mergedDeletes.isEmpty()) {
            throw HoglakeException.Validation("commit has no appends or deletes")
        }
        val readSnapshot = req.readSnapshot
        if (readSnapshot != null && readSnapshot < 0) {
            throw HoglakeException.Validation(
                "read_snapshot must be >= 0, got $readSnapshot",
            )
        }
        if (mergedDeletes.isNotEmpty() && readSnapshot == null) {
            throw HoglakeException.Validation(
                "read_snapshot is required when the commit contains deletes",
            )
        }

        // The read's rows for one table: one per live spec, and a spec is
        // only read for an appended table. Two live incarnations of one
        // name is a broken invariant (the unique live-name indexes), and is
        // reported as the per-table lookup's `findOne` reported it.
        fun resolveGuarded(key: Pair<String, String>): List<CommitTail.ResolvedTable> {
            val (namespace, table) = key
            val rows =
                read.tables[ordOf.getValue(key)]
                    // The miss path, and only the miss path, asks WHY:
                    // a name that never existed is still the 422 it was,
                    // while a name whose table was dropped is a typed
                    // 409 naming the drop snapshot. Before #193 a drop
                    // also end-snapshotted the files, so a writer that
                    // kept committing into a dropped table got
                    // "unknown table" and had no way to tell a typo
                    // from a table pulled out from under it. Now that
                    // the table row is the ONLY thing that says a
                    // dropped table's rows are unreachable, the refusal
                    // says so too.
                    ?: throw droppedTableRefusal(h, catalogId, namespace, table)
            check(rows.distinctBy { it.tableId }.size == 1) {
                "more than one live table resolves '$namespace.$table' in catalog_id=$catalogId"
            }
            val live = rows.first()
            for (expected in expectedUuids[key].orEmpty()) {
                // The atomic incarnation guard: the name resolved, but to a
                // different incarnation than the writer planned against —
                // a drop+recreate happened. NOT retryable (the expected
                // incarnation is gone and its history does not carry
                // over), never a silent write into the wrong table.
                if (expected != live.tableUuid) {
                    throw HoglakeException.TableRecreated(
                        "table '$namespace.$table' is uuid ${live.tableUuid}, " +
                            "expected $expected: the table was recreated; re-prepare against the " +
                            "current incarnation, replaying this request cannot succeed",
                        table = "$namespace.$table",
                        expectedTableUuid = expected,
                        currentTableUuid = live.tableUuid,
                    )
                }
            }
            return rows
        }
        // hog_table_stats.next_row_id of each appended table, as read; null = no stats row.
        val nextRowIds = HashMap<Long, Long?>()
        val resolvedAppends =
            mergedAppends.map { (key, files) ->
                val (namespace, table) = key
                val rows = resolveGuarded(key)
                val live = rows.first()
                nextRowIds[live.tableId] = live.nextRowId
                val layout = layoutOf(live)?.let { ResolvedLayout(it, live.prefixKeyIndexes?.singleOrNull()) }
                ResolvedAppend(namespace, table, live.tableId, files, liveSpec(rows), layout)
            }
        val appendByName = resolvedAppends.associateBy { it.namespace to it.table }
        val resolvedDeletes =
            mergedDeletes.map { (key, files) ->
                val (namespace, table) = key
                val appended = appendByName[key]
                if (appended != null) {
                    ResolvedDeletes(namespace, table, appended.tableId, files, appended.layout?.layout)
                } else {
                    val live = resolveGuarded(key).first()
                    ResolvedDeletes(namespace, table, live.tableId, files, layoutOf(live))
                }
            }

        // The second thing read_snapshot is REQUIRED for, and the reason
        // is the trust boundary on partition values (invariant 12): they
        // are computed by the WRITER under one partition spec, the server
        // never opens the parquet and so can neither verify nor recompute
        // them, and validateFiles' arity check cannot tell two same-arity
        // specs apart. With a read_snapshot, a spec change between the
        // writer's read and its commit is caught by [checkConflicts]' DDL
        // arm and answered with re-prepare. WITHOUT one there is no
        // conflict window at all, so the same change silently registers
        // values computed under the old transform against the new
        // spec_id — files that mis-prune every read of them, with nothing
        // anywhere able to detect it afterwards.
        //
        // A blind append to an UNPARTITIONED table stays legal and
        // unchanged: it carries no partition values, so it has nothing
        // bound to a spec and nothing a concurrent alter can invalidate
        // that the arity/field-id content checks do not already catch.
        //
        // OFF BY DEFAULT, and the reason is the fleet, not timidity:
        // EVERY production flush is this shape today — millpond strips the
        // read_snapshot `prepare_append_files` puts on, and duckdb-client
        // never sends one. See Config.refuseBlindPartitionedAppends for
        // both writers and the two-part flip precondition. Until then the
        // refusal is a WARN naming the client.
        //
        // AFTER RESOLUTION, and that is what makes the dedupe key sound:
        // the key is the server-assigned tableId, so nothing a client
        // supplies can grow the set. Keyed on the request's (namespace,
        // table) strings — which is where this check used to sit — 512
        // bogus names would have silenced the line for the pod's life,
        // and a name that does not resolve now gets the "unknown table"
        // refusal it deserves instead of this one.
        //
        // The OFFENDING TABLES, not a request-level boolean: a commit may
        // append to several tables (`/commit` and `/commit/transaction`
        // both take many groups, and both millpond and the duckdb
        // extension write multi-table requests), and only the ones
        // carrying partition values are at fault. A boolean made the
        // refusal name no table at all and made the WARN name EVERY table
        // in the request — a false statement about each unpartitioned one,
        // and each of those keys then poisoned the dedupe set so the line
        // could never be corrected.
        val blindPartitioned =
            if (readSnapshot != null) {
                emptyList()
            } else {
                resolvedAppends.filter { a -> a.files.any { it.partitionValues != null } }
            }
        if (blindPartitioned.isNotEmpty()) {
            val qualified = blindPartitioned.map { "${it.namespace}.${it.table}" }.sorted()
            // Counted per OCCURRENCE, above the flag and outside the WARN's
            // dedupe, because this counter is what the flag flip is decided
            // on: a rate that reaches zero and stays there. Counting only
            // the accepted ones would make the line fall to zero the moment
            // enforcement is switched on, which is exactly the observation
            // the operator needs to keep after the flip.
            blindPartitioned.forEach {
                Metrics.blindPartitionedAppend(catalogName, it.namespace, it.table)
            }
            if (refuseBlindPartitionedAppends) {
                throw HoglakeException.Validation(
                    "${blindPartitionedRefusal(qualified)}: $BLIND_PARTITIONED_REFUSAL",
                )
            }
            warnBlindPartitioned(catalogName, catalogId, blindPartitioned, userAgent)
        }

        // 3. Conflict check ('table_dropped'/'table_altered' since
        // readSnapshot on every touched table — appends and deletes share
        // the one-query pattern). readSnapshot is non-null whenever deletes
        // exist; a null readSnapshot is an appends-only blind commit.
        //
        // BEFORE the structural validation below, and the order is a
        // contract decision, not a micro-optimization. A DDL change and
        // the file-level symptom it produces arrive together — DROP COLUMN
        // leaves stats naming a dead field_id, a new spec leaves the
        // arity wrong, a dropped spec leaves partition values on an
        // unpartitioned table — and whichever check runs first decides
        // what the client is told. validateFiles answers 422 Validation,
        // which is "your request is malformed": not retryable, not
        // re-preparable, and it tells a caching writer nothing about its
        // stale basis, so it rebuilds the same doomed payload until its
        // cache ages out. checkConflicts answers the typed 409 that says
        // re-prepare. The conflict is the CAUSE and the malformed file is
        // the SYMPTOM, so the cause answers first.
        // `UploadService.register`'s claimed-path refusal moved below this
        // too, so a request that is both conflicted and path-colliding now
        // gets the 409 rather than that refusal — same trade, same better
        // answer, and worth naming since this list enumerates the cases.
        //
        // Safe to hoist because this block reads only what step 2
        // produced — the resolved table ids and names — and nothing below
        // it depends on having run: validateFiles' sanitized return is
        // consumed at write time, and the removal-queue/upload serializers
        // key on paths. All of them read under the same catalog lock, in
        // the one read statement above.
        if (readSnapshot != null) {
            if (readSnapshot > head) {
                throw HoglakeException.Validation(
                    "readSnapshot $readSnapshot is ahead of catalog head $head",
                )
            }
            // The floor guard: expiry cascade-deletes hog_snapshot_change
            // rows below earliest_snapshot_id, so a readSnapshot under the
            // floor has a partly-expired conflict window — checkConflicts
            // below could silently miss a 'table_altered'/'table_dropped'
            // in the expired range. 410, matching every read path's
            // below-floor contract: the writer's conflict basis is gone;
            // it must re-read at a retained snapshot and re-commit.
            // readSnapshot == earliest is fine (the window (earliest, head]
            // is fully retained).
            if (readSnapshot < catalogHead.earliestSnapshotId) {
                throw HoglakeException.Expired(
                    "read_snapshot $readSnapshot is below the expiry floor (earliest " +
                        "retained snapshot is ${catalogHead.earliestSnapshotId}" +
                        "${catalogHead.reachedAtSuffix()}): the conflict window since " +
                        "that snapshot is partly expired; re-read at a retained " +
                        "snapshot and re-commit",
                )
            }
            val names = HashMap<Long, String>()
            resolvedAppends.forEach { names[it.tableId] = "${it.namespace}.${it.table}" }
            resolvedDeletes.forEach { names[it.tableId] = "${it.namespace}.${it.table}" }
            checkConflicts(readSnapshot, read.conflicts, names)
        }

        // 4. Structural validation. Nothing is written unless all of it
        // passes. The RESULT is what gets written: validateFiles also
        // sanitizes each file's stats (StatsSanity).
        val validatedAppends = resolvedAppends.map { validateFiles(it, read.columns[it.tableId].orEmpty()) }
        validateDeleteRegistrations(resolvedDeletes)
        // 4a. The object-prefix layout, on the SANITIZED stats, and after
        // the structural checks: a malformed file is malformed whatever
        // the table's layout. A table without the option skips it whole.
        // Its DVs are checked in planDeletes, after their conflicts.
        validatedAppends.forEach { checkObjectPrefix(it, dataPath) }

        // 4b. Upload claims, then the removal-queue collision check, and
        // THE LATTER IS THE SERIALIZER NOW RATHER THAN THE COMMIT LOCK:
        // cleanup's drain takes no lock at all (V21 — it claims rows
        // instead), so what keeps it off a path this commit is registering
        // is this refusal plus the fact that the drain reads only UNDRAINED
        // rows. A registered path with an undrained hog_file_removal row is
        // scheduled for physical deletion — accepting it would let the
        // drain delete the object out from under the new live row (path
        // reuse under a deterministic path scheme / writer retry). A
        // CLAIMED row is still an undrained row, so the refusal stands for
        // the whole time the path is somebody's work.
        // Duplicate paths against live/historical file rows stay legal —
        // this rejects only paths the cleanup queue currently owns; once
        // the entry drains (drained_at set) the path is registrable again.
        // Both read their rows in the read statement, the claims FOR
        // UPDATE; the claims settle in the write.
        val registered =
            resolvedAppends.flatMap { a -> a.files.map { it.path to "data" } } +
                resolvedDeletes.flatMap { d -> d.files.map { it.path to "delete" } }
        val settleUploads =
            if (registered.isEmpty()) {
                emptyList()
            } else {
                com.posthog.hoglake.service.UploadService.claimsToSettle(
                    read.claims,
                    read.retained,
                    com.posthog.hoglake.service.UploadService.registrationKinds(registered),
                    req.idempotencyKey,
                )
            }
        refuseQueuedPaths(read.queued)

        // 5. Allocations: the snapshot is head + 1 and the files take ids
        // from next_file_id as the read returned them — data files and DV
        // files share the allocator, appends first. The write's guarded
        // UPDATE advances both. Neither appends nor deletes are DDL:
        // schema_version is NOT bumped.
        val appendFileCount = resolvedAppends.sumOf { it.files.size }
        val deleteFileCount = resolvedDeletes.sumOf { it.files.size }
        val totalFiles = appendFileCount + deleteFileCount
        val snapshotId = head + 1
        val firstFileId = read.head.nextFileId

        // 6. The rows, appends before deletes (a delete targeting a
        // same-commit data file is detected by its begin_snapshot and
        // rejected, rolling the whole commit back) — every check the
        // separate writes made, in the order they made it, before the one
        // statement that writes them.
        val (appendRows, firstDeleteFileId) = planAppends(validatedAppends, firstFileId) { nextRowIds[it] }
        val deleteRows =
            planDeletes(
                resolvedDeletes,
                readSnapshot,
                snapshotId,
                firstDeleteFileId,
                appendRows.files,
                stagedPrefixValues(validatedAppends, appendRows),
                read,
                dataPath,
            )

        // snapshot_time = clock_timestamp(), NOT the column default now():
        // now() is the TRANSACTION start time, which predates the advisory
        // lock wait, so defaulting it would let a queued commit record a
        // time older than an earlier-committed snapshot's. clock_timestamp()
        // executes in the write, under the lock, keeping snapshot_time
        // monotone with snapshot id (modulo the DB clock stepping
        // backwards) — the premise resolveTimestamp relies on.
        //
        // The receipt is THE FINGERPRINT, AND NO BODY. `request` is left
        // NULL: it used to hold the whole canonical payload, which on the
        // prod-us events writer's 270-file x 25-column append is 1.5 MB of
        // jsonb, 566 KB on disk after TOAST, and ~38 ms of the ~100 ms this
        // commit holds the per-catalog lock (#240). Nothing reads it that
        // 32 bytes cannot answer — see [commitFingerprintDigest] for what
        // is lost with it.
        //
        // ROLLING-DEPLOY NOTE, and NULL is the deliberate choice. A
        // pre-V24 replica replaying a receipt this statement wrote reads
        // `request` as NULL and throws inside its own commit tail: a
        // transient 500 on an already-published key, for the length of
        // the deploy, on a path that retries. The alternative — writing a
        // placeholder body so the old decode succeeds — makes that
        // replica compare a placeholder against the real request and
        // answer `idempotency_key reused with a different request`: a 422
        // that tells a correct client it is wrong, which is strictly
        // worse than a retryable 500.
        CommitTail.write(
            h,
            CommitTail.WriteSet(
                catalogId = catalogId,
                head = head,
                firstFileId = firstFileId,
                fileCount = totalFiles,
                schemaVersion = schemaVersion,
                author = req.author,
                message = req.message,
                changes =
                    resolvedAppends.map { "table_inserted_into" to it.tableId } +
                        resolvedDeletes.map { "table_deleted_from" to it.tableId },
                appends = appendRows,
                deletes = deleteRows,
                settleUploads = settleUploads,
                idempotencyKey = req.idempotencyKey,
                fingerprint = fingerprint,
            ),
        )
        return CommitResult(snapshotId, schemaVersion)
    }

    /**
     * The answer for an idempotency key that already published: the
     * stored snapshot, or the refusal for a different request under the
     * same key.
     */
    private fun replayedReceipt(
        catalogName: String,
        receipt: CommitTail.Receipt,
        requestJson: String?,
        fingerprint: ByteArray?,
    ): CommitResult {
        val stored = receipt.fingerprint
        val same =
            when {
                stored == null -> {
                    // A PRE-V24 RECEIPT, which carries the body and no
                    // digest. Re-canonicalize and compare strings, which
                    // is what this check did before the column existed.
                    // Deleted with the `request` column once no
                    // null-fingerprint row is left (V24's header names
                    // the follow-up).
                    //
                    // storedPayloadObjectMapper, NOT the strict API mapper:
                    // this JSON was written by a replica of this service,
                    // which during a rolling deploy may be a NEWER one
                    // carrying a field this version does not know. A strict
                    // decode would throw here, inside the commit tail under
                    // the catalog lock, and turn a half-finished deploy into
                    // 500s on replay.
                    val body = storedPayloadObjectMapper().readValue<CommitRequest>(receipt.request!!)
                    commitFingerprint(body) == requestJson
                }
                stored.firstOrNull() != COMMIT_FINGERPRINT_VERSION -> {
                    // A DIGEST THIS BUILD CANNOT COMPARE, and the answer
                    // is the stored snapshot rather than a refusal.
                    //
                    // The byte moves when the canonical string moves (see
                    // COMMIT_FINGERPRINT_VERSION), and an old canonical
                    // string is unrecoverable — so there is no comparison
                    // to make. The two available answers are "this key was
                    // published, here is its snapshot" and "422, your
                    // request differs". The first is true of every correct
                    // client and of the one case a wrong one could reach
                    // (a key genuinely reused with a different payload,
                    // which then gets the earlier publication's result
                    // instead of a refusal — a client bug reported as a
                    // success, which is what every idempotency-key API
                    // does). The second is a NEW 4xx on a path deployed
                    // writers use, for a whole retention window, caused by
                    // OUR format change: the failure AGENT.md's "stage
                    // every refusal a live client could hit" forbids.
                    //
                    // Counted, because it is otherwise invisible: a
                    // standing nonzero rate means a version bump is being
                    // answered by this arm and the operator should know how
                    // many replays it covers.
                    Metrics.commitReceiptUnknownDigestVersion(catalogName)
                    true
                }
                else ->
                    // THE V24 PATH: 33 bytes against 33 bytes, deciding
                    // exactly what the string comparison above decided,
                    // because the digest is taken OF that string
                    // (commitFingerprintDigest). `contentEquals`, not
                    // `==`: arrays compare by identity in Kotlin, and
                    // reference equality here would refuse every replay.
                    stored.contentEquals(fingerprint)
            }
        if (!same) {
            throw HoglakeException.Validation("idempotency_key reused with a different request")
        }
        return CommitResult(receipt.snapshotId, receipt.schemaVersion)
    }

    /**
     * Reject any registered path (data or DV) outside the catalog's
     * data_path prefix. The commit contract is footer-shipping — the
     * server never opens registered objects at commit time — so path
     * VALIDITY is the one structural check available: a catalog's files
     * live under its data_path, full stop. Without this, a buggy or
     * hostile writer can register `s3://anything`, and that path then
     * flows into the removal queue at expiry/drop time, where the
     * cleanup drain would HEAD+DELETE an object that was never ours
     * (the drain's liveness check verifies non-reference, not
     * provenance). Prefix comparison is on the
     * raw URI with a normalized trailing slash, so `data_path
     * s3://b/demo` does not admit `s3://b/demo-other/...`.
     */
    private fun validatePathsUnderDataPath(
        dataPath: String,
        req: CommitRequest,
    ) {
        val prefix = dataPath.trimEnd('/') + "/"
        val offending =
            (
                req.appends.flatMap { a -> a.files.map { it.path } } +
                    req.deletes.flatMap { d -> d.files.map { it.path } }
            )
                .filter { !pathStaysUnder(it, prefix) }
                .distinct()
        if (offending.isNotEmpty()) {
            throw HoglakeException.Validation(
                "path(s) outside the catalog data_path '$prefix': " +
                    offending.sorted().take(5).joinToString(", ") +
                    (if (offending.size > 5) " (+${offending.size - 5} more)" else "") +
                    "; registered files must live under the catalog's data_path, " +
                    "with no whitespace, backslash, dot (literal or %2E), %2F, or empty path segments",
            )
        }
    }

    /**
     * Reject any registered path (data or DV) that has an undrained
     * hog_file_removal row in this catalog: the path is scheduled for
     * physical deletion and the cleanup drain would delete the object out
     * from under the new row. Typed 409 — the writer retries with a fresh
     * path, or after the drain settles the entry.
     *
     * THIS CHECK IS HALF OF THE DRAIN'S SAFETY ARGUMENT, and since V21 it
     * is not backed by a shared advisory lock: cleanup claims rows
     * (`claimed_at`/`claimed_by`) and takes no catalog lock anywhere. A
     * claimed row is still an UNDRAINED row, so this refusal covers the
     * whole time a path is somebody's work, and the drain never reads a
     * row that is not still queued. See [REMOVAL_QUEUE_COLLISION_SQL] for
     * the other half — which queue inserters race a registration, and
     * what serializes each of them.
     *
     * The atomic-creation tail's own statement; a commit runs the same
     * SQL inside [CommitTail.READ_SQL] and refuses through [refuseQueuedPaths].
     */
    private fun checkRemovalQueueCollisions(
        h: Handle,
        catalogId: Long,
        appends: List<ResolvedAppend>,
        deletes: List<ResolvedDeletes>,
    ) {
        val paths =
            (appends.flatMap { a -> a.files.map { it.path } } + deletes.flatMap { d -> d.files.map { it.path } })
                .distinct()
        if (paths.isEmpty()) return
        refuseQueuedPaths(
            h.createQuery(REMOVAL_QUEUE_COLLISION_SQL)
                .bind("catalogId", catalogId)
                .bindArray("paths", String::class.java, paths)
                .mapTo(String::class.java)
                .list(),
        )
    }

    /** The path-reuse refusal over the [REMOVAL_QUEUE_COLLISION_SQL] hits [queued]; none is a pass. */
    private fun refuseQueuedPaths(queued: List<String>) {
        if (queued.isNotEmpty()) {
            throw HoglakeException.CommitConflict(
                "path(s) scheduled for deletion by the cleanup queue: " +
                    queued.sorted().joinToString(", ") +
                    " — registering them would race the physical delete; use fresh " +
                    "paths, or retry after the removal queue drains",
            )
        }
    }

    /**
     * The hog_table_stats / hog_data_file / hog_file_partition_value /
     * hog_file_column_stats rows for [resolved]: file ids from
     * [firstFileId] in request order, and each table's row-id range from
     * [nextRowId] — its hog_table_stats.next_row_id as read under the
     * lock, null when the table has no stats row. Returns the rows and the
     * next free file id. Every refusal the per-table statements made is
     * made here, in the same order, before anything is written.
     */
    private fun planAppends(
        resolved: List<ResolvedAppend>,
        firstFileId: Long,
        nextRowId: (Long) -> Long?,
    ): Pair<CommitTail.AppendRows, Long> {
        val stats = ArrayList<CommitTail.StatsAdvance>()
        val files = ArrayList<CommitTail.DataFileRow>()
        val partitionValues = ArrayList<CommitTail.PartitionValueRow>()
        val columnStats = ArrayList<CommitTail.ColumnStatsRow>()
        var nextFileId = firstFileId
        for (append in resolved) {
            // The per-table record sum and the allocator advancement are
            // both overflow-checked (Math.addExact): a wrapped sum would
            // hand out negative / reused row-id ranges, silently breaking
            // THE lineage guarantee. The CHECK (next_row_id >= 0) on
            // hog_table_stats is the DB backstop for the same invariant.
            val recordSum =
                try {
                    append.files.fold(0L) { acc, f -> Math.addExact(acc, f.recordCount) }
                } catch (_: ArithmeticException) {
                    throw HoglakeException.Validation("record_count sum overflows row-id space")
                }
            // Overflow-checked like recordSum: a wrapped byte sum would
            // corrupt the hog_table_stats rollup (and the CHECK
            // (file_size_bytes >= 0) on hog_table_stats is the DB backstop).
            val byteSum =
                try {
                    append.files.fold(0L) { acc, f -> Math.addExact(acc, f.fileSizeBytes) }
                } catch (_: ArithmeticException) {
                    throw HoglakeException.Validation(
                        "file_size_bytes sum overflows for ${append.namespace}.${append.table}",
                    )
                }
            // Per-table row-id range: advance the allocator by the table's
            // rollup (safe read-then-write — the per-catalog advisory lock
            // serializes every writer, and the write's UPDATE is guarded
            // by the value read); each file gets a contiguous slice in
            // request order.
            val rowIdStart =
                nextRowId(append.tableId)
                    ?: throw IllegalStateException("missing hog_table_stats row for table_id=${append.tableId}")
            val newNextRowId =
                try {
                    Math.addExact(rowIdStart, recordSum)
                } catch (_: ArithmeticException) {
                    throw HoglakeException.Validation("record_count sum overflows row-id space")
                }
            stats += CommitTail.StatsAdvance(append.tableId, rowIdStart, newNextRowId, recordSum, byteSum)
            var rowId = rowIdStart
            for (file in append.files) {
                file.validateFooterSize()
                val dataFileId = nextFileId++
                files +=
                    CommitTail.DataFileRow(
                        dataFileId = dataFileId,
                        tableId = append.tableId,
                        path = file.path,
                        recordCount = file.recordCount,
                        fileSizeBytes = file.fileSizeBytes,
                        footerSize = file.footerSize,
                        rowIdStart = rowId,
                        statsState = if (file.columnStats != null) "provided" else "pending",
                        specId = append.spec?.specId,
                        splitOffsets = file.splitOffsets,
                    )
                rowId += file.recordCount
                if (append.spec != null) {
                    for ((keyIndex, value) in file.partitionValues!!.withIndex()) {
                        partitionValues += CommitTail.PartitionValueRow(dataFileId, keyIndex, value)
                    }
                }
                for (stat in file.columnStats.orEmpty()) {
                    columnStats += CommitTail.ColumnStatsRow(dataFileId, stat)
                }
            }
        }
        return CommitTail.AppendRows(stats, files, partitionValues, columnStats) to nextFileId
    }

    /**
     * DV registrations: per-target existence/ownership/liveness checks,
     * the supersession chain (stale-vector -> CommitConflict, shrink ->
     * Validation), then the hog_delete_file rows, ids from [firstFileId].
     * Checked AFTER the appends are planned, against [appended] as well as
     * the targets the read returned, so a same-commit target is seen
     * exactly as it was when the appends were inserted before this ran:
     * rejected by its begin_snapshot, or — a transaction's reference by
     * path — resolved to it. hog_table_stats is deliberately untouched
     * (see class KDoc).
     *
     * On a table with the object-prefix layout every DV must also sit
     * under its TARGET's prefix: the value is the target's identity
     * partition value on the prefix field, from [stagedPrefixValues] for a
     * same-commit target and from the read for a numeric one. Checked
     * after every other per-target check, so a stale vector or a retired
     * target still answers its retryable 409 first, and refused for the
     * whole commit once the loop is done, naming up to five DVs.
     */
    private fun planDeletes(
        resolved: List<ResolvedDeletes>,
        readSnapshot: Long?,
        snapshotId: Long,
        firstFileId: Long,
        appended: List<CommitTail.DataFileRow>,
        /** data_file_id -> planned prefix value of this commit's appends to tables with the option. */
        stagedPrefixValues: Map<Long, String?>,
        read: CommitTail.ReadSet,
        dataPath: String,
    ): CommitTail.DeleteRows {
        if (resolved.isEmpty()) return CommitTail.DeleteRows(emptyList(), emptyList())
        checkNotNull(readSnapshot) { "deletes require a readSnapshot (validated earlier)" }
        val appendedById = appended.associateBy { it.dataFileId }
        val appendedByPath = appended.groupBy({ it.tableId to it.path }) { it.dataFileId }
        var nextFileId = firstFileId
        val ended = ArrayList<Long>()
        val files = ArrayList<CommitTail.DeleteFileRow>()
        // By table_id, in request order: the first table's are refused.
        val prefixViolations = LinkedHashMap<Long, MutableList<String>>()
        for (deletes in resolved) {
            val qualified = "${deletes.namespace}.${deletes.table}"
            for (registration in deletes.files) {
                val reg =
                    if (registration.dataFilePath == null) {
                        registration
                    } else {
                        // This commit's appends to the same table under that
                        // path — what `begin_snapshot = <this snapshot>`
                        // found once they were inserted.
                        val ids = appendedByPath[deletes.tableId to registration.dataFilePath].orEmpty()
                        if (ids.size != 1) {
                            throw HoglakeException.Validation(
                                "pending delete must target exactly one same-table append in this commit",
                            )
                        }
                        registration.copy(dataFileId = ids.single())
                    }
                val target =
                    appendedById[reg.dataFileId]?.let { DeleteTarget(it.tableId, it.recordCount, snapshotId, null) }
                        ?: read.targets[reg.dataFileId]?.let {
                            DeleteTarget(it.tableId, it.recordCount, it.beginSnapshot, it.endSnapshot)
                        }
                        ?: throw HoglakeException.Validation(
                            "delete for $qualified targets unknown data_file_id ${reg.dataFileId}",
                        )
                val targetTableId = target.tableId
                val recordCount = target.recordCount
                val targetBegin = target.beginSnapshot
                if (targetBegin == snapshotId && reg.dataFilePath == null) {
                    throw HoglakeException.Validation(
                        "delete for $qualified targets data_file_id ${reg.dataFileId} " +
                            "created in this same commit",
                    )
                }
                if (targetBegin > readSnapshot && !(targetBegin == snapshotId && reg.dataFilePath != null)) {
                    throw HoglakeException.CommitConflict(
                        "delete for $qualified targets data_file_id ${reg.dataFileId} " +
                            "created after read snapshot $readSnapshot",
                    )
                }
                if (targetTableId != deletes.tableId) {
                    throw HoglakeException.Validation(
                        "delete for $qualified targets data_file_id ${reg.dataFileId} " +
                            "which belongs to another table",
                    )
                }
                // Liveness, split by WHEN the file died. A file that was live
                // at the writer's read snapshot and has since been retired
                // lost a race — compaction rewriting it, or another DML
                // commit — and the writer wins it by re-reading: 409,
                // retryable. This is the common case on a compacted table,
                // and 422 was making the connector give up on a DELETE that
                // one retry would have published. A file that was ALREADY
                // dead at the read snapshot is a malformed request, not a
                // race: 422, as before.
                //
                // Expiry cannot reach this branch: it only removes rows
                // below the floor, and a readSnapshot below the floor was
                // already answered 410 by the guard above.
                target.endSnapshot?.let { end ->
                    if (end > readSnapshot) {
                        throw HoglakeException.CommitConflict(
                            "delete for $qualified targets data_file_id ${reg.dataFileId} " +
                                "which was retired at snapshot $end, since read snapshot " +
                                "$readSnapshot",
                        )
                    }
                    throw HoglakeException.Validation(
                        "delete for $qualified targets data_file_id ${reg.dataFileId} " +
                            "which is no longer live",
                    )
                }
                if (reg.deleteCount > recordCount) {
                    throw HoglakeException.Validation(
                        "delete_count ${reg.deleteCount} exceeds record_count $recordCount " +
                            "of data_file_id ${reg.dataFileId} in $qualified",
                    )
                }
                // Supersession chain: at most one live DV per data file (unique
                // partial index). Staleness is checked before monotonicity: a
                // DV registered after the writer's readSnapshot means the new
                // vector was built without seeing it — retryable conflict,
                // regardless of counts. A same-commit target has no vector.
                val current = read.liveDvs[reg.dataFileId]?.takeIf { reg.dataFileId !in appendedById }
                if (current != null) {
                    if (current.beginSnapshot > readSnapshot) {
                        throw HoglakeException.CommitConflict(
                            "deletion vector for data_file_id ${reg.dataFileId} in $qualified " +
                                "was superseded at snapshot ${current.beginSnapshot}, after read snapshot " +
                                "$readSnapshot",
                        )
                    }
                    if (current.deleteCount > reg.deleteCount) {
                        throw HoglakeException.Validation(
                            "delete_count ${reg.deleteCount} for data_file_id ${reg.dataFileId} " +
                                "in $qualified shrinks the live deletion vector " +
                                "(delete_count ${current.deleteCount}) — vectors only grow",
                        )
                    }
                    ended += current.deleteFileId
                }
                deletes.layout?.let { layout ->
                    val raw =
                        if (reg.dataFileId in appendedById) {
                            stagedPrefixValues[reg.dataFileId]
                        } else {
                            read.targets[reg.dataFileId]?.prefixValues?.singleOrNull()
                        }
                    dvPrefixViolation(reg, raw, layout, deletes, dataPath)?.let {
                        prefixViolations.getOrPut(deletes.tableId) { mutableListOf() } += it
                    }
                }
                files +=
                    CommitTail.DeleteFileRow(
                        deleteFileId = nextFileId++,
                        tableId = deletes.tableId,
                        dataFileId = reg.dataFileId,
                        path = reg.path,
                        deleteCount = reg.deleteCount,
                        fileSizeBytes = reg.fileSizeBytes,
                    )
            }
        }
        prefixViolations.entries.firstOrNull()?.let { (tableId, violations) ->
            val deletes = resolved.first { it.tableId == tableId }
            throw objectPrefixViolation(deletes.namespace, deletes.table, dataPath, deletes.layout!!, violations)
        }
        return CommitTail.DeleteRows(ended, files)
    }

    /**
     * Why one DV registration breaks its table's object-prefix layout, or
     * null when it does not: its target has no canonical value for the
     * field (fail closed — every live file of a table with the option was
     * registered with one, and expiry keeps the partition spec it names
     * (`ExpiryService.VERSIONED_TABLES_NEVER_CLEARED`), so this is a broken
     * invariant or a foreign row), or the DV is not under the target's
     * prefix. The latter is the
     * DuckDB extension's shape today, a DV at the table root
     * (`hoglake-dv-<uuid>.puffin`).
     */
    private fun dvPrefixViolation(
        reg: DeleteFileRegistration,
        raw: String?,
        layout: ObjectPrefixLayout,
        deletes: ResolvedDeletes,
        dataPath: String,
    ): String? = ObjectPrefixRules.dvViolation(reg, raw, layout, deletes.namespace, deletes.table, dataPath)

    /**
     * The refusal for a commit whose table did not resolve live: a
     * typed [HoglakeException.TableDropped] naming the drop snapshot
     * when the name belonged to a dropped table, and the historical
     * [HoglakeException.Validation] otherwise.
     *
     * "Belonged to a dropped table" is asked precisely: a version row
     * under this (namespace, name) whose `end_snapshot` is exactly its
     * table's `dropped_snapshot` — the row `TableRepo.markDropped`
     * closed. A rename closes a version row too, and a table renamed
     * away is NOT a dropped table under the old name; matching the drop
     * snapshot is what separates the two. The newest such incarnation
     * wins, because a name can have been dropped and recreated and
     * dropped again, and the last drop is the one a writer is racing.
     *
     * Runs on the failing path only; a successful commit never issues
     * it.
     */
    private fun droppedTableRefusal(
        h: Handle,
        catalogId: Long,
        namespace: String,
        table: String,
    ): HoglakeException {
        val droppedAt =
            h.createQuery(
                """
            SELECT t.dropped_snapshot
              FROM hog_table_version tv
              JOIN hog_namespace ns
                ON ns.catalog_id = tv.catalog_id AND ns.namespace_id = tv.namespace_id
              JOIN hog_table t
                ON t.catalog_id = tv.catalog_id AND t.table_id = tv.table_id
             WHERE tv.catalog_id = :catalogId
               AND tv.name = :table
               AND ns.name = :namespace
               AND NOT ns.dropped
               AND t.dropped_snapshot IS NOT NULL
               AND tv.end_snapshot = t.dropped_snapshot
             ORDER BY t.dropped_snapshot DESC
             LIMIT 1
            """,
            )
                .bind("catalogId", catalogId)
                .bind("namespace", namespace)
                .bind("table", table)
                .mapTo(Long::class.javaObjectType)
                .findOne()
                .orElse(null)
                ?: return HoglakeException.Validation("unknown table $namespace.$table")
        return HoglakeException.TableDropped(
            "table '$namespace.$table' was dropped in snapshot $droppedAt; its files are " +
                "no longer part of the catalog and are being retired",
        )
    }

    /**
     * The read's object-prefix layout for one resolved table, or null when
     * the option is off. A layout row whose column is not live, or whose
     * type is not an allowed one, is a broken invariant (the DDL refuses
     * dropping, renaming or promoting the field out of the allowed set),
     * so it is a 500 rather than a commit that silently skips the check.
     */
    private fun layoutOf(row: CommitTail.ResolvedTable): ObjectPrefixLayout? {
        val fieldId = row.prefixFieldId ?: return null
        val name = row.prefixFieldName
        val type = row.prefixColType?.let { ColType.fromWire(it) }
        check(name != null && type != null && type in ObjectPrefix.ALLOWED_TYPES) {
            "object-prefix field_id $fieldId of table_id=${row.tableId} is not a live column of an allowed " +
                "type (name=$name, type=${row.prefixColType})"
        }
        return ObjectPrefixLayout(fieldId, name, type)
    }

    /**
     * The planned value of the prefix field for every file this commit
     * appends to a table with the object-prefix option, by data_file_id:
     * what a transaction's DV on its own staged file is checked against.
     */
    private fun stagedPrefixValues(
        appends: List<ResolvedAppend>,
        rows: CommitTail.AppendRows,
    ): Map<Long, String?> {
        val keyByTable = appends.mapNotNull { a -> a.layout?.keyIndex?.let { a.tableId to it } }.toMap()
        if (keyByTable.isEmpty()) return emptyMap()
        val tableOf = rows.files.associate { it.dataFileId to it.tableId }
        return rows.partitionValues
            .filter { keyByTable[tableOf[it.dataFileId]] == it.keyIndex }
            .associate { it.dataFileId to it.value }
    }

    /**
     * The object-prefix layout's commit rule for one append
     * ([ObjectPrefix]), on the files [validateFiles] returned — SANITIZED,
     * because StatsSanity drops a bound it cannot decode and a dropped
     * bound proves nothing. On a table with the option every file must:
     *
     *  - carry a canonical value for the prefix field at its identity
     *    key_index ([ObjectPrefix.parseValue]: no sign, no leading zero,
     *    ASCII digits, in the column type's range), so a null value,
     *    `"042"`, `"+4"` and an out-of-range value are all refused;
     *  - sit under that value's prefix ([ObjectPrefix.isUnder]), which is
     *    what keeps `team_id=4/` from admitting `team_id=42/`;
     *  - prove with its inline stats that every row holds the value
     *    ([ObjectPrefix.checkFieldStats]: both bounds equal to it, no
     *    nulls). A zero-row file therefore cannot register, since its
     *    bounds are dropped. A file with DEFERRED stats has none to prove
     *    it with: it registers `pending`, and the hydrator makes the same
     *    proof from its footer, quarantining a file that fails it
     *    (`hog_data_file.prefix_quarantined`). A pending file is outside
     *    a per-value read until then, by its state.
     *
     * Violations are collected rather than thrown at the first, so the
     * one 422 names up to five paths, each with the prefix it should have
     * been under.
     */
    private fun checkObjectPrefix(
        append: ResolvedAppend,
        dataPath: String,
    ) {
        val (layout, keyIndex) = append.layout ?: return
        // The DDL keeps exactly one identity field on the prefix field in
        // every spec while the option is on (AlterService.setPartitionSpec),
        // so anything else is a broken invariant, not a client error.
        check(append.spec != null && keyIndex != null) {
            "${append.namespace}.${append.table} has the object-prefix layout on '${layout.fieldName}' but its " +
                "live spec has no single identity field on it"
        }
        val violations =
            ObjectPrefixRules.appendViolations(append.files, layout, keyIndex, append.namespace, append.table, dataPath)
        if (violations.isNotEmpty()) {
            throw objectPrefixViolation(append.namespace, append.table, dataPath, layout, violations)
        }
    }

    /**
     * The 422 for [violations] (one entry per offending path, with its
     * reason) on [namespace].[table]: the first five, the count of the
     * rest, and the layout a writer has to follow, so one that did not
     * know the table has the option learns where its files belong.
     */
    private fun objectPrefixViolation(
        namespace: String,
        table: String,
        dataPath: String,
        layout: ObjectPrefixLayout,
        violations: List<String>,
    ): HoglakeException.ObjectPrefixViolation =
        ObjectPrefixRules.violation(namespace, table, dataPath, layout, violations)

    /**
     * The table's live partition spec, or null when unpartitioned. A spec
     * with zero fields (SetPartitionSpec([])) is unpartitioned too.
     * Deliberately inline rather than persistence/SpecRepo.specAt: the
     * commit tail only needs the spec header + field COUNT (one query),
     * not the full field list. The atomic-creation tail's own statement;
     * a commit reads the same columns in [CommitTail.READ_SQL] and takes
     * them through the overload below.
     */
    private fun liveSpec(
        h: Handle,
        catalogId: Long,
        tableId: Long,
    ): LiveSpec? =
        h.createQuery(
            """
            SELECT ps.spec_id,
                   (SELECT count(*) FROM hog_partition_field pf
                     WHERE pf.catalog_id = ps.catalog_id AND pf.table_id = ps.table_id
                       AND pf.spec_id = ps.spec_id) AS field_count
              FROM hog_partition_spec ps
             WHERE ps.catalog_id = :catalogId AND ps.table_id = :tableId
               AND ps.end_snapshot IS NULL
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .map { rs, _ -> LiveSpec(rs.getLong(1), rs.getInt(2)) }
            .findOne()
            .orElse(null)
            ?.takeIf { it.fieldCount > 0 }

    /**
     * [liveSpec] from the read's rows for one resolved table: one row per
     * live spec, so more than one is the broken invariant the per-table
     * `findOne` refused.
     */
    private fun liveSpec(rows: List<CommitTail.ResolvedTable>): LiveSpec? {
        check(rows.size == 1) { "more than one live partition spec for table_id=${rows.first().tableId}" }
        val row = rows.single()
        return row.specId?.let { LiveSpec(it, row.fieldCount ?: 0) }?.takeIf { it.fieldCount > 0 }
    }

    /**
     * field_id -> col_type of the table's live columns, for [validateFiles]:
     * the atomic-creation tail's own statement, which a commit reads in
     * [CommitTail.READ_SQL] instead.
     */
    private fun liveColumnTypes(
        h: Handle,
        catalogId: Long,
        tableId: Long,
    ): Map<Long, String> =
        h.createQuery(
            """
            SELECT field_id, col_type FROM hog_column
             WHERE catalog_id = ? AND table_id = ? AND end_snapshot IS NULL
            """,
        )
            .bind(0, catalogId)
            .bind(1, tableId)
            .map { rs, _ -> rs.getLong("field_id") to rs.getString("col_type") }
            .toList()
            .toMap()

    /**
     * Structural checks on one append's files, returning the append with
     * its stats SANITIZED (StatsSanity): a bound that cannot be decoded
     * as its column's type, or that sorts above its partner, is dropped
     * rather than stored, and impossible counts are clamped. The return
     * value is what gets written — using the argument instead would
     * store exactly the rows this pass exists to repair.
     *
     * [liveColumnTypes] is field_id -> col_type of the table's live
     * columns. The TYPE is carried because a stats row is only
     * meaningful for a LEAF: hog_file_column_stats holds counts and
     * bounds, and a list/struct/map has neither. A client shipping stats
     * for a container id is confused about the shape of its own file,
     * and a silent accept would put a bound on a column no reader can
     * decode it for.
     */
    private fun validateFiles(
        append: ResolvedAppend,
        liveColumnTypes: Map<Long, String>,
    ): ResolvedAppend {
        val qualified = "${append.namespace}.${append.table}"
        for (file in append.files) {
            if (file.path.isBlank()) {
                throw HoglakeException.Validation("blank file path in append to $qualified")
            }
            if (file.recordCount < 0) {
                throw HoglakeException.Validation(
                    "negative record_count for ${file.path} in $qualified",
                )
            }
            if (file.recordCount > MAX_FILE_RECORD_COUNT) {
                throw HoglakeException.Validation(
                    "record_count ${file.recordCount} for ${file.path} in $qualified exceeds " +
                        "the per-file cap $MAX_FILE_RECORD_COUNT (2^48); no real file has that many rows",
                )
            }
            if (file.fileSizeBytes < 0) {
                throw HoglakeException.Validation(
                    "negative file_size_bytes for ${file.path} in $qualified",
                )
            }
            // Refused, not repaired: unlike a stats row there is no
            // partial list worth keeping — readers ignore a list that
            // breaks the contract, and a sorted-but-wrong one misplaces
            // every cut. Absent stays absent (NULL), for provided and
            // pending registrations alike. The one check for BOTH doors:
            // atomic table creation publishes through registerInitialFiles,
            // which runs this function too.
            file.validateSplitOffsets()
            val spec = append.spec
            val values = file.partitionValues
            if (spec != null) {
                if (values == null) {
                    throw HoglakeException.Validation(
                        "$qualified is partitioned (${spec.fieldCount} field(s)) but " +
                            "${file.path} has no partition_values",
                    )
                }
                if (values.size != spec.fieldCount) {
                    throw HoglakeException.Validation(
                        "partition_values arity ${values.size} for ${file.path} in $qualified " +
                            "does not match the live spec's ${spec.fieldCount} field(s)",
                    )
                }
            } else if (values != null) {
                throw HoglakeException.Validation(
                    "partition_values given for ${file.path} but $qualified is not partitioned",
                )
            }
            val stats = file.columnStats ?: continue
            val seenFieldIds = HashSet<Long>()
            for (stat in stats) {
                val colType =
                    liveColumnTypes[stat.fieldId]
                        ?: throw HoglakeException.Validation(
                            "unknown field_id ${stat.fieldId} in stats for ${file.path} in $qualified",
                        )
                // #77's refusal, kept, read off the SAME live type map
                // the container check below uses rather than a second
                // per-table query. (Its own query was already depth
                // -agnostic — every hog_column row, not just the
                // top-level ones — so this is one query fewer, not a
                // behaviour change.)
                if (ColType.fromWire(colType) == ColType.VARIANT) {
                    throw HoglakeException.Validation(
                        "variant column statistics are not supported; omit field_id ${stat.fieldId}",
                    )
                }
                if (ColType.fromWire(colType).isNested) {
                    throw HoglakeException.Validation(
                        "field_id ${stat.fieldId} in stats for ${file.path} in $qualified is a " +
                            "'$colType' column; nested containers carry no values, so stats are " +
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
                        "negative value_count/null_count for field_id ${stat.fieldId} " +
                            "in stats for ${file.path} in $qualified",
                    )
                }
            }
        }
        return sanitizeStats(append, liveColumnTypes)
    }

    /**
     * Drop the bounds a client cannot have meant and clamp the counts it
     * cannot have measured, per file, warning once per repaired row.
     *
     * The commit is NOT refused: the file's data is fine, its metadata
     * is not, and a 422 here would reject a correct append over a
     * cosmetic field the writer can fix later. Readers prune on these
     * bounds, though, so storing a wrong one is a wrong answer — hence
     * drop rather than keep, counted so the writer's bug is visible.
     */
    private fun sanitizeStats(
        append: ResolvedAppend,
        liveColumnTypes: Map<Long, String>,
    ): ResolvedAppend {
        val qualified = "${append.namespace}.${append.table}"
        val files =
            append.files.map { file ->
                val stats = file.columnStats ?: return@map file
                val checked =
                    stats.map { stat ->
                        val type = liveColumnTypes[stat.fieldId]?.let { ColType.fromWire(it) }
                        val result = StatsSanity.check(stat, type)
                        if (result.repairs.isNotEmpty()) {
                            Metrics.statsRepaired("commit")
                            log.warn {
                                "column_stats for field_id ${stat.fieldId} of ${file.path} in " +
                                    "$qualified are not internally consistent " +
                                    "(${result.repairs.joinToString("; ")}); storing the repaired row"
                            }
                        }
                        result.stats
                    }
                // The sanitizer's output is what gets stored, whether or
                // not it reported anything. Taking it only when a repair
                // was REPORTED dropped the signed-zero canonicalization
                // on the floor, which is silent and not a repair.
                file.copy(columnStats = checked)
            }
        return append.copy(files = files)
    }

    /** DB-independent DV registration checks: shapes, ranges, duplicate targets. */
    private fun validateDeleteRegistrations(resolved: List<ResolvedDeletes>) {
        val seenTargets = HashSet<Any>()
        for (deletes in resolved) {
            val qualified = "${deletes.namespace}.${deletes.table}"
            for (reg in deletes.files) {
                if (reg.path.isBlank()) {
                    throw HoglakeException.Validation("blank delete file path in $qualified")
                }
                if (reg.deleteCount <= 0) {
                    throw HoglakeException.Validation(
                        "delete_count must be > 0 for data_file_id ${reg.dataFileId} " +
                            "in $qualified, got ${reg.deleteCount}",
                    )
                }
                if (reg.fileSizeBytes < 0) {
                    throw HoglakeException.Validation(
                        "negative file_size_bytes for delete of data_file_id " +
                            "${reg.dataFileId} in $qualified",
                    )
                }
                if (reg.dataFilePath != null && (reg.dataFileId != 0L || reg.dataFilePath.isBlank())) {
                    throw HoglakeException.Validation(
                        "pending delete requires data_file_id zero and a nonblank data_file_path",
                    )
                }
                val targetKey: Any =
                    reg.dataFilePath?.let { Triple(deletes.namespace, deletes.table, it) } ?: reg.dataFileId
                if (!seenTargets.add(targetKey)) {
                    throw HoglakeException.Validation(
                        "duplicate delete target data_file_id ${reg.dataFileId} in one commit",
                    )
                }
            }
        }
    }

    /**
     * The transition WARN for a blind append carrying partition_values:
     * once per RESOLVED table per pod, naming the client in the message
     * so the line is actionable by whoever owns it rather than being
     * noise on every flush. Takes the resolved appends, so the dedupe key
     * is the server's own (catalogId, tableId) — see
     * `warnedBlindPartitioned` for why that matters.
     */
    private fun warnBlindPartitioned(
        catalogName: String,
        catalogId: Long,
        appends: List<ResolvedAppend>,
        userAgent: String?,
    ) {
        val agent = userAgent ?: "unknown"
        for (append in appends) {
            if (warnedBlindPartitioned.size >= MAX_WARNED_BLIND_PARTITIONED) {
                // Said once, at the cap, and then never again.
                if (warnedBlindPartitionedCapped.compareAndSet(false, true)) {
                    log.warn {
                        "blind-partitioned append warnings suppressed past " +
                            "$MAX_WARNED_BLIND_PARTITIONED distinct tables"
                    }
                }
                return
            }
            if (!warnedBlindPartitioned.add(catalogId to append.tableId)) continue
            log.warn {
                "blind append with partition_values on " +
                    "'$catalogName'/'${append.namespace}.${append.table}' from " +
                    "user_agent=$agent; accepted for now, and it will be refused once " +
                    "HOGLAKE_REFUSE_BLIND_PARTITIONED_APPENDS is on: $BLIND_PARTITIONED_REFUSAL"
            }
        }
    }

    /**
     * One indexed lookup over the typed change table covers appends and
     * deletes alike: the `conflicts` CTE of [CommitTail.READ_SQL], whose
     * rows are [conflicted] — distinct (table id, is-DDL) pairs since
     * [readSnapshot] on the tables in [nameByTableId].
     *
     * Every touched table gets the DDL check ('table_dropped' /
     * 'table_altered' since readSnapshot), and a refusal where EVERY
     * conflicted row is DDL is the typed, non-retryable
     * [HoglakeException.DdlSinceReadSnapshot] rather than a
     * [HoglakeException.CommitConflict] — see that class for why the two
     * need opposite client behaviour. The UNCHANGED tables
     * ([CommitTail.TableKey.unchanged]) — the DELETE targets of a guarded
     * request, see CommitRequest.requireUnchangedTables — additionally
     * require that nothing changed their ROW CONTENT, because the delete
     * was planned against a read of it: 'table_created',
     * 'table_inserted_into' and 'table_deleted_from'. Legacy writes mark
     * none and retain the per-file DV staleness checks in [planDeletes].
     *
     * 'table_compacted' is deliberately NOT in that list, and never was
     * a correct member of it. Compaction changes which FILES back a
     * table, never which rows are visible in it, so it cannot invalidate
     * a read set. Treating it as a conflict livelocked every mutation on
     * a continuously-compacted table: production publishes a compaction
     * group every ~2.6 minutes on one table, so any UPDATE or MERGE
     * whose read-to-publish window exceeded that interval retried into
     * the next compaction forever. What compaction CAN invalidate is a
     * specific DV target, and that is caught per file by the liveness
     * check in [planDeletes], which stays.
     */
    private fun checkConflicts(
        readSnapshot: Long,
        conflicted: List<CommitTail.Conflict>,
        nameByTableId: Map<Long, String>,
    ) {
        if (conflicted.isNotEmpty()) {
            val names = conflicted.mapNotNull { nameByTableId[it.objectId] }.distinct().sorted()
            // TYPED, because the two answers need opposite client
            // behaviour and used to be the same code. Pure DDL is
            // permanent for this payload: a prepared request's
            // read_snapshot is durable and must be replayed
            // byte-identically, so nothing about a retry can make the
            // alter un-happen, and a client that treats the refusal as
            // retryable (which `commit_conflict` tells it to —
            // pyhoglake's CommitConflictError is retryable=True) loops on
            // a request that can only be refused again. The recovery is
            // to re-prepare against a fresh table read.
            //
            // A ROW-CONTENT conflict is the opposite: re-reading and
            // replanning is the recovery, and the same payload can
            // succeed next time. It keeps `commit_conflict`, and so does
            // a MIXED refusal — `all { ddl }` rather than `any`, because
            // telling a caller "re-prepare, replaying cannot work" when
            // half the reason was a concurrent insert would send it down
            // the more expensive path for a conflict a plain retry
            // clears.
            if (conflicted.all { it.ddl }) {
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
}
