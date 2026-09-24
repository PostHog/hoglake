package com.posthog.hoglake.commit

import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.CommitResult
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.StatsSanity
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.validateFooterSize
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
 * that snapshot — one indexed lookup. A null readSnapshot is a blind
 * append with no conflict window (only legal when the commit has no
 * deletes).
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
         * THE PATH-REUSE GUARD, issued once per commit under the
         * per-catalog commit lock (invariant 4). `internal` so
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
                jdbi.inTransaction<CommitResult, RuntimeException> { handle ->
                    doCommit(handle, catalog, request, requestJson)
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
        val append =
            validateFiles(
                h,
                catalogId,
                ResolvedAppend(namespace, table, tableId, files, liveSpec(h, catalogId, tableId)),
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
        writeAppends(h, catalogId, snapshotId, firstId, listOf(append))
    }

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
    )

    /** The targeted data file's row, as [applyDeletes]'s per-target checks need it. */
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
    )

    private fun doCommit(
        h: Handle,
        catalogName: String,
        req: CommitRequest,
        requestJson: String?,
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
        Locks.acquireCatalogCommitLock(h, catalogId, commitLockTimeoutMs)

        // Under the same catalog lock as publication, so concurrent retries
        // cannot both allocate rows. A receipt is not tied to snapshot expiry.
        req.idempotencyKey?.let { key ->
            val receipt =
                h.createQuery(
                    """
                    SELECT snapshot_id, schema_version, request::text AS request
                    FROM hog_commit_receipt WHERE catalog_id = :catalog AND idempotency_key = :key
                    """,
                ).bind("catalog", catalogId).bind("key", key)
                    .map { rs, _ ->
                        // storedPayloadObjectMapper, NOT the strict API mapper: this
                        // JSON was written by a replica of this service, which during
                        // a rolling deploy may be a NEWER one carrying a field this
                        // version does not know. A strict decode would throw here,
                        // inside the commit tail under the catalog lock, and turn a
                        // half-finished deploy into 500s on replay.
                        val stored = storedPayloadObjectMapper().readValue<CommitRequest>(rs.getString("request"))
                        if (commitFingerprint(stored) != requestJson) {
                            throw HoglakeException.Validation("idempotency_key reused with a different request")
                        }
                        CommitResult(rs.getLong("snapshot_id"), rs.getLong("schema_version"))
                    }.findOne().orElse(null)
            if (receipt != null) return receipt
        }

        if (req.idempotencyKey != null) validatePathsUnderDataPath(dataPath, req)

        val catalogHead =
            h.createQuery(
                """
                SELECT last_snapshot_id, schema_version, earliest_snapshot_id, earliest_snapshot_time
                FROM hog_catalog WHERE catalog_id = ?
                """,
            )
                .bind(0, catalogId)
                .map { rs, _ ->
                    CatalogHead(
                        head = rs.getLong(1),
                        schemaVersion = rs.getLong(2),
                        earliestSnapshotId = rs.getLong(3),
                        earliestSnapshotTime =
                            rs.getObject(4, java.time.OffsetDateTime::class.java)?.toInstant(),
                    )
                }
                .one()
        val (head, schemaVersion) = catalogHead.head to catalogHead.schemaVersion

        // 2. Merge duplicate (namespace, table) appends/deletes, preserving
        // request order (first occurrence for tables, concatenation for
        // files), then resolve each to a live table at head. Every
        // expected_table_uuid supplied for a table is collected and checked
        // against the resolved incarnation — merged duplicates that
        // disagree cannot both match, so a stale one still conflicts.
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

        fun resolveGuarded(key: Pair<String, String>): LiveTable {
            val (namespace, table) = key
            val live =
                resolveLiveTable(h, catalogId, namespace, table)
                    ?: throw HoglakeException.Validation("unknown table $namespace.$table")
            for (expected in expectedUuids[key].orEmpty()) {
                // The atomic incarnation guard: the name resolved, but to a
                // different incarnation than the writer planned against —
                // a drop+recreate happened. Retryable conflict, never a
                // silent write into the wrong table.
                if (expected != live.tableUuid) {
                    throw HoglakeException.CommitConflict(
                        "table '$namespace.$table' is uuid ${live.tableUuid}, " +
                            "expected $expected: the table was recreated",
                    )
                }
            }
            return live
        }
        val resolvedAppends =
            mergedAppends.map { (key, files) ->
                val (namespace, table) = key
                val live = resolveGuarded(key)
                ResolvedAppend(namespace, table, live.tableId, files, liveSpec(h, catalogId, live.tableId))
            }
        val appendTableIdByName =
            resolvedAppends.associate { (it.namespace to it.table) to it.tableId }
        val resolvedDeletes =
            mergedDeletes.map { (key, files) ->
                val (namespace, table) = key
                val tableId = appendTableIdByName[key] ?: resolveGuarded(key).tableId
                ResolvedDeletes(namespace, table, tableId, files)
            }

        // 3. Structural validation. Nothing is written unless all of it
        // passes. The RESULT is what gets written: validateFiles also
        // sanitizes each file's stats (StatsSanity).
        val validatedAppends = resolvedAppends.map { validateFiles(h, catalogId, it) }
        validateDeleteRegistrations(resolvedDeletes)

        // 3b. Removal-queue collision check (under the commit lock, so it
        // serializes with cleanup's drain sub-batches, which take the same
        // lock): a registered path with an UNDRAINED hog_file_removal row
        // is scheduled for physical deletion — accepting it would let the
        // cleanup drain delete the object out from under the new live row
        // (path reuse under a deterministic path scheme / writer retry).
        // Duplicate paths against live/historical file rows stay legal —
        // this rejects only paths the cleanup queue currently owns; once
        // the entry drains (drained_at set) the path is registrable again.
        com.posthog.hoglake.service.UploadService.register(
            h,
            catalogId,
            req.idempotencyKey,
            resolvedAppends.flatMap { a -> a.files.map { it.path to "data" } } +
                resolvedDeletes.flatMap { d -> d.files.map { it.path to "delete" } },
        )
        checkRemovalQueueCollisions(h, catalogId, resolvedAppends, resolvedDeletes)

        // 4. Conflict check ('table_dropped'/'table_altered' since
        // readSnapshot on every touched table — appends and deletes share
        // the one-query pattern). readSnapshot is non-null whenever deletes
        // exist; a null readSnapshot is an appends-only blind commit.
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
            // requireUnchangedTables binds the DELETE targets, not every
            // touched table. A delete is published against a read of the
            // target's file set, so anything that moved that file set is a
            // conflict; an APPEND carries no such read, so a table this
            // commit only appends to keeps the DDL-only rule even inside a
            // guarded transaction — otherwise an append-only explicit
            // transaction would 409 on a concurrent INSERT into the same
            // table, which is the one thing appends have never done.
            val unchangedTableIds =
                if (req.requireUnchangedTables) resolvedDeletes.mapTo(HashSet()) { it.tableId } else emptySet()
            checkConflicts(h, catalogId, readSnapshot, names, unchangedTableIds)
        }

        // 5. Allocations, all under the lock via UPDATE..RETURNING. Data
        // files and DV files share the next_file_id allocator. Neither
        // appends nor deletes are DDL: schema_version is NOT bumped.
        val appendFileCount = resolvedAppends.sumOf { it.files.size }
        val deleteFileCount = resolvedDeletes.sumOf { it.files.size }
        val totalFiles = appendFileCount + deleteFileCount
        val (snapshotId, firstFileId) =
            h.createQuery(
                """
            UPDATE hog_catalog
               SET last_snapshot_id = last_snapshot_id + 1,
                   next_file_id = next_file_id + ?
             WHERE catalog_id = ?
            RETURNING last_snapshot_id, next_file_id
            """,
            )
                .bind(0, totalFiles)
                .bind(1, catalogId)
                .map { rs, _ -> rs.getLong(1) to (rs.getLong(2) - totalFiles) }
                .one()

        // 6. Writes: snapshot, change rows, then appends before deletes (a
        // delete targeting a same-commit data file is detected below by its
        // begin_snapshot and rejected, rolling the whole commit back).
        // snapshot_time = clock_timestamp(), NOT the column default now():
        // now() is the TRANSACTION start time, which predates the advisory
        // lock wait, so defaulting it would let a queued commit record a
        // time older than an earlier-committed snapshot's. clock_timestamp()
        // executes here, under the lock, keeping snapshot_time monotone
        // with snapshot id (modulo the DB clock stepping backwards) — the
        // premise resolveTimestamp relies on.
        h.createUpdate(
            """
            INSERT INTO hog_snapshot (catalog_id, snapshot_id, snapshot_time, schema_version, author, commit_message)
            VALUES (:catalogId, :snapshotId, clock_timestamp(), :schemaVersion, :author, :message)
            """,
        )
            .bind("catalogId", catalogId)
            .bind("snapshotId", snapshotId)
            .bind("schemaVersion", schemaVersion)
            .bind("author", req.author)
            .bind("message", req.message)
            .execute()

        val changeBatch =
            h.prepareBatch(
                """
            INSERT INTO hog_snapshot_change (catalog_id, snapshot_id, kind, object_id)
            VALUES (:catalogId, :snapshotId, :kind, :tableId)
            """,
            )
        for (append in resolvedAppends) {
            changeBatch
                .bind("catalogId", catalogId)
                .bind("snapshotId", snapshotId)
                .bind("kind", "table_inserted_into")
                .bind("tableId", append.tableId)
                .add()
        }
        for (deletes in resolvedDeletes) {
            changeBatch
                .bind("catalogId", catalogId)
                .bind("snapshotId", snapshotId)
                .bind("kind", "table_deleted_from")
                .bind("tableId", deletes.tableId)
                .add()
        }
        changeBatch.execute()

        var nextFileId = firstFileId
        nextFileId = writeAppends(h, catalogId, snapshotId, nextFileId, validatedAppends)
        applyDeletes(h, catalogId, snapshotId, readSnapshot, nextFileId, resolvedDeletes)

        req.idempotencyKey?.let { key ->
            h.createUpdate(
                """
                INSERT INTO hog_commit_receipt (catalog_id, idempotency_key, request, snapshot_id, schema_version)
                VALUES (:catalog, :key, CAST(:request AS jsonb), :snapshot, :schema)
                """,
            ).bind("catalog", catalogId).bind("key", key).bind("request", requestJson)
                .bind("snapshot", snapshotId).bind("schema", schemaVersion).execute()
        }
        return CommitResult(snapshotId, schemaVersion)
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
                    "with no whitespace, dot, or empty path segments",
            )
        }
    }

    /**
     * A literal startsWith is not enough: `s3://b/demo/../victim/x` starts
     * with `s3://b/demo/` but some reader stacks normalize dot segments,
     * re-addressing the object OUTSIDE the prefix. Whitespace/control
     * characters are refused outright (they survive into removal-queue
     * rows and reader URIs), as are empty and dot segments.
     */
    private fun pathStaysUnder(
        path: String,
        prefix: String,
    ): Boolean {
        if (!path.startsWith(prefix)) return false
        if (path.any { it.isWhitespace() || it.isISOControl() }) return false
        val relative = path.removePrefix(prefix)
        if (relative.isEmpty()) return false
        return relative.split('/').none { it.isEmpty() || it == "." || it == ".." }
    }

    /**
     * Reject any registered path (data or DV) that has an undrained
     * hog_file_removal row in this catalog: the path is scheduled for
     * physical deletion and the cleanup drain (serialized against this
     * check by the shared per-catalog advisory lock) would delete the
     * object out from under the new row. Typed 409 — the writer retries
     * with a fresh path, or after the drain settles the entry.
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
        val queued =
            h.createQuery(REMOVAL_QUEUE_COLLISION_SQL)
                .bind("catalogId", catalogId)
                .bindArray("paths", String::class.java, paths)
                .mapTo(String::class.java)
                .list()
        if (queued.isNotEmpty()) {
            throw HoglakeException.CommitConflict(
                "path(s) scheduled for deletion by the cleanup queue: " +
                    queued.sorted().joinToString(", ") +
                    " — registering them would race the physical delete; use fresh " +
                    "paths, or retry after the removal queue drains",
            )
        }
    }

    /** hog_data_file / hog_file_partition_value / hog_file_column_stats writes. Returns the next free file id. */
    private fun writeAppends(
        h: Handle,
        catalogId: Long,
        snapshotId: Long,
        firstFileId: Long,
        resolved: List<ResolvedAppend>,
    ): Long {
        if (resolved.isEmpty()) return firstFileId
        val fileBatch =
            h.prepareBatch(
                """
            INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                       path, record_count, file_size_bytes, footer_size,
                                       row_id_start, stats_state, spec_id)
            VALUES (:catalogId, :dataFileId, :tableId, :beginSnapshot,
                    :path, :recordCount, :fileSizeBytes, :footerSize,
                    :rowIdStart, :statsState, :specId)
            """,
            )
        val statsBatch =
            h.prepareBatch(
                """
            INSERT INTO hog_file_column_stats (catalog_id, data_file_id, field_id, value_count,
                                               null_count, nan_count, size_bytes,
                                               lower_bound, upper_bound)
            VALUES (:catalogId, :dataFileId, :fieldId, :valueCount,
                    :nullCount, :nanCount, :sizeBytes,
                    :lowerBound, :upperBound)
            """,
            )
        val partitionBatch =
            h.prepareBatch(
                """
            INSERT INTO hog_file_partition_value (catalog_id, data_file_id, key_index, value)
            VALUES (:catalogId, :dataFileId, :keyIndex, :value)
            """,
            )
        var nextFileId = firstFileId
        var haveStats = false
        var havePartitionValues = false
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
            // Per-table row-id range: read the allocator, advance it with
            // the table's rollup (safe read-then-write — the per-catalog
            // advisory lock serializes every writer); each file gets a
            // contiguous slice in request order.
            val rowIdStart =
                h.createQuery(
                    """
                SELECT next_row_id FROM hog_table_stats
                 WHERE catalog_id = :catalogId AND table_id = :tableId
                """,
                )
                    .bind("catalogId", catalogId)
                    .bind("tableId", append.tableId)
                    .mapTo(Long::class.java)
                    .findOne()
                    .orElseThrow {
                        IllegalStateException(
                            "missing hog_table_stats row for table_id=${append.tableId}",
                        )
                    }
            val newNextRowId =
                try {
                    Math.addExact(rowIdStart, recordSum)
                } catch (_: ArithmeticException) {
                    throw HoglakeException.Validation("record_count sum overflows row-id space")
                }
            h.createUpdate(
                """
                UPDATE hog_table_stats
                   SET next_row_id = :newNextRowId,
                       record_count = record_count + :records,
                       file_size_bytes = file_size_bytes + :bytes
                 WHERE catalog_id = :catalogId AND table_id = :tableId
                """,
            )
                .bind("newNextRowId", newNextRowId)
                .bind("records", recordSum)
                .bind("bytes", byteSum)
                .bind("catalogId", catalogId)
                .bind("tableId", append.tableId)
                .execute()
            var rowId = rowIdStart

            for (file in append.files) {
                file.validateFooterSize()
                val dataFileId = nextFileId++
                fileBatch
                    .bind("catalogId", catalogId)
                    .bind("dataFileId", dataFileId)
                    .bind("tableId", append.tableId)
                    .bind("beginSnapshot", snapshotId)
                    .bind("path", file.path)
                    .bind("recordCount", file.recordCount)
                    .bind("fileSizeBytes", file.fileSizeBytes)
                    .bind("footerSize", file.footerSize)
                    .bind("rowIdStart", rowId)
                    .bind("statsState", if (file.columnStats != null) "provided" else "pending")
                    .bind("specId", append.spec?.specId)
                    .add()
                rowId += file.recordCount
                if (append.spec != null) {
                    for ((keyIndex, value) in file.partitionValues!!.withIndex()) {
                        havePartitionValues = true
                        partitionBatch
                            .bind("catalogId", catalogId)
                            .bind("dataFileId", dataFileId)
                            .bind("keyIndex", keyIndex)
                            .bind("value", value)
                            .add()
                    }
                }
                for (stats in file.columnStats.orEmpty()) {
                    haveStats = true
                    statsBatch
                        .bind("catalogId", catalogId)
                        .bind("dataFileId", dataFileId)
                        .bind("fieldId", stats.fieldId)
                        .bind("valueCount", stats.valueCount)
                        .bind("nullCount", stats.nullCount)
                        .bind("nanCount", stats.nanCount)
                        .bind("sizeBytes", stats.sizeBytes)
                        .bind("lowerBound", stats.lowerBound)
                        .bind("upperBound", stats.upperBound)
                        .add()
                }
            }
        }
        fileBatch.execute()
        if (havePartitionValues) partitionBatch.execute()
        if (haveStats) statsBatch.execute()
        return nextFileId
    }

    /**
     * DV registrations: per-target existence/ownership/liveness checks,
     * the supersession chain (stale-vector -> CommitConflict, shrink ->
     * Validation), then hog_delete_file inserts. Runs AFTER appends are
     * written so a same-commit target is visible here and rejected by its
     * begin_snapshot. hog_table_stats is deliberately untouched (see class
     * KDoc).
     */
    private fun applyDeletes(
        h: Handle,
        catalogId: Long,
        snapshotId: Long,
        readSnapshot: Long?,
        firstFileId: Long,
        resolved: List<ResolvedDeletes>,
    ) {
        if (resolved.isEmpty()) return
        checkNotNull(readSnapshot) { "deletes require a readSnapshot (validated earlier)" }
        var nextFileId = firstFileId
        val insertBatch =
            h.prepareBatch(
                """
            INSERT INTO hog_delete_file (catalog_id, delete_file_id, table_id, data_file_id,
                                         begin_snapshot, path, delete_count, file_size_bytes)
            VALUES (:catalogId, :deleteFileId, :tableId, :dataFileId,
                    :beginSnapshot, :path, :deleteCount, :fileSizeBytes)
            """,
            )
        for (deletes in resolved) {
            val qualified = "${deletes.namespace}.${deletes.table}"
            for (registration in deletes.files) {
                val reg =
                    if (registration.dataFilePath == null) {
                        registration
                    } else {
                        val ids =
                            h.createQuery(
                                """
                        SELECT data_file_id FROM hog_data_file
                        WHERE catalog_id = :catalog AND table_id = :table AND begin_snapshot = :snapshot
                          AND path = :path
                        LIMIT 2
                        """,
                            ).bind("catalog", catalogId).bind("table", deletes.tableId)
                                .bind("snapshot", snapshotId).bind("path", registration.dataFilePath)
                                .mapTo(Long::class.java).list()
                        if (ids.size != 1) {
                            throw HoglakeException.Validation(
                                "pending delete must target exactly one same-table append in this commit",
                            )
                        }
                        registration.copy(dataFileId = ids.single())
                    }
                val target =
                    h.createQuery(
                        """
                    SELECT table_id, record_count, begin_snapshot, end_snapshot
                      FROM hog_data_file
                     WHERE catalog_id = :catalogId AND data_file_id = :dataFileId
                    """,
                    )
                        .bind("catalogId", catalogId)
                        .bind("dataFileId", reg.dataFileId)
                        .map { rs, _ ->
                            DeleteTarget(
                                tableId = rs.getLong("table_id"),
                                recordCount = rs.getLong("record_count"),
                                beginSnapshot = rs.getLong("begin_snapshot"),
                                endSnapshot = rs.getObject("end_snapshot") as Long?,
                            )
                        }
                        .findOne()
                        .orElseThrow {
                            HoglakeException.Validation(
                                "delete for $qualified targets unknown data_file_id ${reg.dataFileId}",
                            )
                        }
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
                // regardless of counts.
                val current =
                    h.createQuery(
                        """
                    SELECT delete_file_id, delete_count, begin_snapshot
                      FROM hog_delete_file
                     WHERE catalog_id = :catalogId AND data_file_id = :dataFileId
                       AND end_snapshot IS NULL
                    """,
                    )
                        .bind("catalogId", catalogId)
                        .bind("dataFileId", reg.dataFileId)
                        .map { rs, _ -> Triple(rs.getLong(1), rs.getLong(2), rs.getLong(3)) }
                        .findOne()
                        .orElse(null)
                if (current != null) {
                    val (currentId, currentCount, currentBegin) = current
                    if (currentBegin > readSnapshot) {
                        throw HoglakeException.CommitConflict(
                            "deletion vector for data_file_id ${reg.dataFileId} in $qualified " +
                                "was superseded at snapshot $currentBegin, after read snapshot " +
                                "$readSnapshot",
                        )
                    }
                    if (currentCount > reg.deleteCount) {
                        throw HoglakeException.Validation(
                            "delete_count ${reg.deleteCount} for data_file_id ${reg.dataFileId} " +
                                "in $qualified shrinks the live deletion vector " +
                                "(delete_count $currentCount) — vectors only grow",
                        )
                    }
                    h.createUpdate(
                        """
                        UPDATE hog_delete_file SET end_snapshot = :snapshotId
                         WHERE catalog_id = :catalogId AND delete_file_id = :deleteFileId
                        """,
                    )
                        .bind("snapshotId", snapshotId)
                        .bind("catalogId", catalogId)
                        .bind("deleteFileId", currentId)
                        .execute()
                }
                insertBatch
                    .bind("catalogId", catalogId)
                    .bind("deleteFileId", nextFileId++)
                    .bind("tableId", deletes.tableId)
                    .bind("dataFileId", reg.dataFileId)
                    .bind("beginSnapshot", snapshotId)
                    .bind("path", reg.path)
                    .bind("deleteCount", reg.deleteCount)
                    .bind("fileSizeBytes", reg.fileSizeBytes)
                    .add()
            }
        }
        insertBatch.execute()
    }

    /** A live table's id + identity uuid (the incarnation the name currently binds to). */
    private data class LiveTable(val tableId: Long, val tableUuid: UUID)

    /** A table is live iff its head version row is open and neither it nor its namespace is dropped. */
    private fun resolveLiveTable(
        h: Handle,
        catalogId: Long,
        namespace: String,
        table: String,
    ): LiveTable? =
        h.createQuery(
            """
        SELECT tv.table_id, t.table_uuid
          FROM hog_table_version tv
          JOIN hog_namespace ns
            ON ns.catalog_id = tv.catalog_id AND ns.namespace_id = tv.namespace_id
          JOIN hog_table t
            ON t.catalog_id = tv.catalog_id AND t.table_id = tv.table_id
         WHERE tv.catalog_id = :catalogId
           AND tv.end_snapshot IS NULL
           AND tv.name = :table
           AND ns.name = :namespace
           AND NOT ns.dropped
           AND t.dropped_snapshot IS NULL
        """,
        )
            .bind("catalogId", catalogId)
            .bind("namespace", namespace)
            .bind("table", table)
            .map { rs, _ -> LiveTable(rs.getLong(1), rs.getObject(2) as UUID) }
            .findOne()
            .orElse(null)

    /**
     * The table's live partition spec, or null when unpartitioned. A spec
     * with zero fields (SetPartitionSpec([])) is unpartitioned too.
     * Deliberately inline rather than persistence/SpecRepo.specAt: the
     * commit tail only needs the spec header + field COUNT (one query),
     * not the full field list.
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
     * Structural checks on one append's files, returning the append with
     * its stats SANITIZED (StatsSanity): a bound that cannot be decoded
     * as its column's type, or that sorts above its partner, is dropped
     * rather than stored, and impossible counts are clamped. The return
     * value is what gets written — using the argument instead would
     * store exactly the rows this pass exists to repair.
     */
    private fun validateFiles(
        h: Handle,
        catalogId: Long,
        append: ResolvedAppend,
    ): ResolvedAppend {
        val qualified = "${append.namespace}.${append.table}"
        // field_id -> col_type. The TYPE is carried because a stats row
        // is only meaningful for a LEAF: hog_file_column_stats holds
        // counts and bounds, and a list/struct/map has neither. A client
        // shipping stats for a container id is confused about the shape
        // of its own file, and a silent accept would put a bound on a
        // column no reader can decode it for.
        val liveColumnTypes: Map<Long, String> by lazy {
            h.createQuery(
                """
                SELECT field_id, col_type FROM hog_column
                 WHERE catalog_id = ? AND table_id = ? AND end_snapshot IS NULL
                """,
            )
                .bind(0, catalogId)
                .bind(1, append.tableId)
                .map { rs, _ -> rs.getLong("field_id") to rs.getString("col_type") }
                .toList()
                .toMap()
        }
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
     * One indexed lookup over the typed change table covers appends and
     * deletes alike.
     *
     * Every touched table gets the DDL check ('table_dropped' /
     * 'table_altered' since readSnapshot). The tables in
     * [unchangedTableIds] — the DELETE targets of a guarded request, see
     * CommitRequest.requireUnchangedTables — additionally require that
     * nothing changed their ROW CONTENT, because the delete was planned
     * against a read of it: 'table_created', 'table_inserted_into' and
     * 'table_deleted_from'. Legacy writes pass an empty set and retain
     * the per-file DV staleness checks in [applyDeletes].
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
     * check in [applyDeletes], which stays.
     */
    private fun checkConflicts(
        h: Handle,
        catalogId: Long,
        readSnapshot: Long,
        nameByTableId: Map<Long, String>,
        unchangedTableIds: Set<Long>,
    ) {
        val conflicted =
            h.createQuery(
                """
            SELECT DISTINCT object_id,
                   kind IN ('table_dropped', 'table_altered') AS ddl
              FROM hog_snapshot_change
             WHERE catalog_id = :catalogId
               AND snapshot_id > :readSnapshot
               AND object_id IN (<tableIds>)
               AND (kind IN ('table_dropped', 'table_altered') OR
                    (kind IN ('table_created', 'table_inserted_into', 'table_deleted_from')
                     AND object_id = ANY(:unchangedTableIds)))
            """,
            )
                .bind("catalogId", catalogId)
                .bind("readSnapshot", readSnapshot)
                .bindArray("unchangedTableIds", Long::class.javaObjectType, unchangedTableIds)
                .bindList("tableIds", nameByTableId.keys.toList())
                .map { rs, _ -> rs.getLong("object_id") to rs.getBoolean("ddl") }
                .list()
        if (conflicted.isNotEmpty()) {
            val names = conflicted.mapNotNull { nameByTableId[it.first] }.distinct().sorted()
            val change = if (conflicted.all { it.second }) "DDL" else "table change"
            throw HoglakeException.CommitConflict(
                "concurrent $change since snapshot $readSnapshot on table(s): " +
                    names.joinToString(", "),
            )
        }
    }
}
