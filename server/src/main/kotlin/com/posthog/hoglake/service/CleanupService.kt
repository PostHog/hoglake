package com.posthog.hoglake.service

import com.posthog.hoglake.Config
import com.posthog.hoglake.Database
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.CleanupResult
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.persistence.MaintenanceRunStore
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.useTransactionUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.Delete
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.ObjectIdentifier
import software.amazon.awssdk.services.s3.model.S3Error
import software.amazon.awssdk.services.s3.model.S3Exception
import java.net.URI
import java.time.Duration

/**
 * Physical-delete side of the file-removal queue [ObjectStore] cannot
 * provide (it reads, puts and uploads, but never deletes — that is
 * off-limits to that layer): existence probe, single delete and BATCHED
 * delete, speaking the same `s3://bucket/key` URIs via
 * [ObjectStore.parse]. Same construction surface as ObjectStore so
 * App.kt wires it identically (`RemovalStore(cfg)`).
 *
 * `open`, with [deleteBatch] and [deleteIfExists] open, for ONE reason,
 * and it is [ObjectStore]'s: a per-key failure inside an otherwise
 * successful `DeleteObjects` response is real S3 behaviour that no
 * bucket can be asked to produce on demand, and how the drain handles
 * it — that row stays queued with `attempts + 1` while its siblings
 * settle — is exactly what a test has to pin.
 */
open class RemovalStore(
    endpoint: String?,
    region: String,
    accessKey: String?,
    secretKey: String?,
    pathStyle: Boolean,
    /**
     * The commit admission bound THIS process runs with
     * (HOGLAKE_COMMIT_LOCK_TIMEOUT_MS), not the compiled default: it is
     * one of the two bounds [apiCallTimeout] is derived from, and an
     * operator who lowers it must lower the call bound with it. 0 means
     * an unbounded wait, so only the idle bound applies.
     */
    commitLockTimeoutMs: Long = com.posthog.hoglake.commit.CommitService.DEFAULT_COMMIT_LOCK_TIMEOUT_MS,
) : AutoCloseable {
    constructor(config: Config) : this(
        endpoint = config.s3Endpoint.ifBlank { null },
        region = config.s3Region,
        accessKey = config.s3AccessKey.ifBlank { null },
        secretKey = config.s3SecretKey.ifBlank { null },
        pathStyle = config.s3PathStyle,
        commitLockTimeoutMs = config.commitLockTimeoutMs,
    )

    /** See [callBoundFor]: the bound this instance's calls actually run under. */
    val apiCallTimeout: Duration = callBoundFor(commitLockTimeoutMs)

    /** Half [apiCallTimeout] — see [callBoundFor] for what that buys. */
    val apiCallAttemptTimeout: Duration = apiCallTimeout.dividedBy(2)

    /**
     * How long one `CleanupService` sub-batch may spend on object-store
     * calls — EXACTLY two of [apiCallTimeout], and defined here rather
     * than on the drain because it is derived from the same two bounds
     * and has to move with them.
     *
     * That is the whole point: a hold budget written against the idle
     * bound alone survives an operator lowering
     * HOGLAKE_COMMIT_LOCK_TIMEOUT_MS to keep commits responsive, and
     * then holds the lock for longer than the admission window the
     * operator just chose — 503ing every writer, which is the
     * production failure this change exists to remove. Two thirds of
     * `min(idle, admission)`, with the last third left as the one call
     * the budget's own gate reserves room for:
     *
     *   hold <= holdBudget = 2 x apiCallTimeout
     *   holdBudget + one call <= min(idle, admission)
     */
    val holdBudget: Duration = apiCallTimeout.multipliedBy(2)

    private val s3: S3Client =
        S3Client.builder()
            .region(Region.of(region))
            .overrideConfiguration(
                ClientOverrideConfiguration.builder()
                    .apiCallTimeout(apiCallTimeout)
                    .apiCallAttemptTimeout(apiCallAttemptTimeout)
                    .build(),
            )
            .apply {
                if (endpoint != null) endpointOverride(URI.create(endpoint))
                if (accessKey != null && secretKey != null) {
                    credentialsProvider(
                        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)),
                    )
                }
            }
            .forcePathStyle(pathStyle)
            .build()

    private val log = KotlinLogging.logger {}

    /**
     * What one path's probe-and-delete did. [SKIPPED] is not a failure:
     * the caller's hold ran out of budget before a call could be issued,
     * so nothing was attempted and the row is the next hold's work.
     */
    enum class Outcome { REMOVED, MISSING, SKIPPED }

    /**
     * One [deleteBatch]'s result. [unattempted] is separate from
     * [failures] because the two settle differently: a failure is a
     * touch that did not drain the row (`attempts + 1`), an unattempted
     * path was never reached and must be left exactly as it was found.
     */
    data class BatchOutcome(
        val failures: Map<String, String>,
        val unattempted: List<String>,
    )

    fun exists(pathUri: String): Boolean {
        val loc = ObjectStore.parse(pathUri)
        return try {
            s3.headObject(HeadObjectRequest.builder().bucket(loc.bucket).key(loc.key).build())
            true
        } catch (_: NoSuchKeyException) {
            false
        } catch (e: S3Exception) {
            // Some S3 implementations surface HEAD misses as a bare 404
            // (no modeled NoSuchKey error on a body-less response).
            if (e.statusCode() == 404) false else throw e
        }
    }

    /**
     * Delete the object at [pathUri]; an already-absent object is
     * [Outcome.MISSING] (DeleteObject alone is silently idempotent, so
     * the probe is what distinguishes "removed" from "was never there").
     *
     * Two round trips per path, so the drain uses it ONLY where the
     * distinction is read by something: `compaction_staging` tickets,
     * whose `'absent'` outcome is an arm of `/verify`'s
     * `staging_tickets` check (a staged path drained `'absent'` that IS
     * a live file row is the staged-output race resolved the wrong way).
     * Everything else goes through [deleteBatch].
     */
    open fun deleteIfExists(
        pathUri: String,
        mayIssueCall: () -> Boolean = { true },
    ): Outcome {
        // TWO calls, gated SEPARATELY. The gate's contract is per CALL —
        // no request may start unless the hold can absorb a whole
        // [apiCallTimeout] — so the DELETE has to ask again after the
        // HEAD returns. A gate that closed in between leaves the row
        // queued and the object present, which is the state the next
        // run re-probes; the alternative, one gate check for a pair,
        // would need two call bounds of headroom and at a budget of two
        // call bounds that lets exactly one row through per hold.
        if (!mayIssueCall()) return Outcome.SKIPPED
        if (!exists(pathUri)) return Outcome.MISSING
        if (!mayIssueCall()) return Outcome.SKIPPED
        val loc = ObjectStore.parse(pathUri)
        s3.deleteObject(DeleteObjectRequest.builder().bucket(loc.bucket).key(loc.key).build())
        return Outcome.REMOVED
    }

    /**
     * Delete every path in [paths] with as few round trips as S3 allows,
     * returning ONLY the failures as `path -> reason`. An empty map means
     * every key is gone.
     *
     * There is no [Outcome.MISSING] here and there cannot be:
     * `DeleteObjects` reports a key that was never there exactly as it
     * reports one it removed, so the HEAD that used to draw that line is
     * the round trip this method exists to remove. A missing key IS the
     * end state the queue asks for, so the caller settles those rows
     * `'deleted'`.
     *
     * Failures are per KEY. A modelled error in the response fails only
     * its own key; a whole-call failure (the request threw, the bucket is
     * gone, credentials expired) fails every key in that call and leaves
     * the other calls' keys alone; and a path that is not an
     * `s3://bucket/key` URI fails on its own before any call is made. The
     * drain leaves every failed row queued with `attempts + 1`, which is
     * what a per-object delete failure always did.
     *
     * Paths are grouped by bucket and chunked to [MAX_KEYS_PER_DELETE]
     * because both are S3's rules, not the caller's; each chunk is one
     * [deleteChunk] request, which is the unit the drain's lock hold is
     * measured in.
     */
    open fun deleteBatch(
        paths: Collection<String>,
        mayIssueCall: () -> Boolean = { true },
    ): BatchOutcome {
        val failures = mutableMapOf<String, String>()
        val unattempted = mutableListOf<String>()
        val byBucket = mutableMapOf<String, MutableList<Pair<String, String>>>()
        for (path in paths.distinct()) {
            val loc =
                try {
                    ObjectStore.parse(path)
                } catch (e: IllegalArgumentException) {
                    // A parse failure costs no call, so the gate is not
                    // consulted: this path is a failure whatever the
                    // hold's budget is, and a retry will fail the same
                    // way.
                    failures[path] = e.message ?: "unparseable object path"
                    continue
                }
            byBucket.getOrPut(loc.bucket) { mutableListOf() } += loc.key to path
        }
        for ((bucket, entries) in byBucket) {
            for (chunk in entries.chunked(MAX_KEYS_PER_DELETE)) {
                // Per REQUEST, because a request is what the hold pays
                // for: a sub-batch spanning several buckets, or past the
                // key ceiling, is several requests and the budget has to
                // be asked between them.
                if (!mayIssueCall()) {
                    unattempted += chunk.map { (_, path) -> path }
                    continue
                }
                failures += deleteChunk(bucket, chunk)
            }
        }
        return BatchOutcome(failures, unattempted)
    }

    /**
     * ONE `DeleteObjects` request: one bucket, at most
     * [MAX_KEYS_PER_DELETE] keys, given as `(key, path)` pairs, with the
     * failures keyed by path.
     *
     * A named, `open` function rather than an inline loop because it is
     * the unit the cleanup drain's lock hold is measured in, and that
     * unit is the whole point of the change: a test that counts the
     * paths handed to [deleteBatch] cannot tell a batched implementation
     * from a per-key loop — the outcomes are identical and only the
     * round trips differ. Counting THESE is what tells them apart.
     */
    internal open fun deleteChunk(
        bucket: String,
        chunk: List<Pair<String, String>>,
    ): Map<String, String> {
        // Safe as a map: ObjectStore.parse normalizes nothing (no
        // dot-segment collapsing, no percent-decoding, no case folding),
        // so two distinct path strings cannot arrive at one
        // (bucket, key) and silently share an entry here. CommitService
        // refuses dot and empty segments in registered paths for the
        // same reason (see [failuresFor], which builds that map).
        try {
            val response =
                s3.deleteObjects(
                    DeleteObjectsRequest.builder()
                        .bucket(bucket)
                        .delete(
                            Delete.builder()
                                .objects(
                                    chunk.map { (key, _) ->
                                        ObjectIdentifier.builder().key(key).build()
                                    },
                                )
                                // Successes are the common case and the
                                // caller does not read them; quiet mode
                                // returns errors only.
                                .quiet(true)
                                .build(),
                        )
                        .build(),
                )
            return failuresFor(bucket, chunk, response.errors())
        } catch (e: Exception) {
            return chunk.associate { (_, p) -> p to "${e::class.simpleName}: ${e.message}" }
        }
    }

    /**
     * Read one `DeleteObjects` response's error list into `path ->
     * reason`, for the keys this request actually sent.
     *
     * `internal` and separate from the request so it can be tested
     * directly: the interesting case is an error naming a key we did NOT
     * send, and no bucket can be asked to return one.
     *
     * THAT CASE FAILS THE WHOLE CHUNK. The response is the only record
     * of what happened to these objects, and an unrecognised key in it
     * says the response cannot be trusted to name the survivors.
     * Settling any of the chunk `'deleted'` on the strength of it would
     * leave an object with no ledger row — and the removal queue is the
     * only thing that ever knew the path, so that orphan is invisible
     * and permanent. A retried delete costs one round trip.
     */
    internal fun failuresFor(
        bucket: String,
        chunk: List<Pair<String, String>>,
        errors: List<S3Error>,
    ): Map<String, String> {
        val pathByKey = chunk.toMap()
        val failures = mutableMapOf<String, String>()
        for (error in errors) {
            val path = pathByKey[error.key()]
            if (path == null) {
                log.error {
                    "cleanup: DeleteObjects on bucket '$bucket' returned an error for key " +
                        "'${error.key()}', which this request did not send " +
                        "(${error.code()}: ${error.message()}); failing all ${chunk.size} " +
                        "paths in the chunk rather than settling any of them"
                }
                val reason = "unmatched error key '${error.key()}' in the response"
                return chunk.associate { (_, p) -> p to reason }
            }
            failures[path] = "${error.code()}: ${error.message()}"
        }
        return failures
    }

    override fun close() = s3.close()

    companion object {
        /** S3's own ceiling on one DeleteObjects request. */
        const val MAX_KEYS_PER_DELETE = 1000

        /**
         * THE CALL BOUND, AND IT IS AN INEQUALITY, NOT A NUMBER.
         *
         * `CleanupService`'s object-store calls run INSIDE a transaction
         * that holds the per-catalog commit lock, so a call that hangs
         * is a transaction that sits idle-in-transaction holding the one
         * lock the catalog's writers queue on. Two bounds are already in
         * force on that transaction, and this one has to sit under BOTH
         * of them or it can never fire usefully:
         *
         *  - `Database.SESSION_INIT_SQL` sets
         *    `idle_in_transaction_session_timeout = '30s'` on every
         *    pooled connection, and a connection waiting on an S3
         *    response is exactly idle-in-transaction. Past that bound
         *    Postgres kills the backend, the sub-batch rolls back with
         *    its objects already deleted and its ledger rows unsettled,
         *    and the next run does the same thing again.
         *  - [commitLockTimeoutMs] (HOGLAKE_COMMIT_LOCK_TIMEOUT_MS, 30 s
         *    by default) is what a foreground commit will wait for this
         *    lock before answering a typed 503. A hold longer than that
         *    turns every concurrent writer's commit into backpressure.
         *    0 means an unbounded wait, so it constrains nothing.
         *
         * So the call bound is derived from the smaller of the two,
         * divided by three: one third leaves room for the SDK to fail,
         * retry, and still finish inside both bounds, and [holdBudget]
         * is the other two thirds — so a drain's whole hold plus one
         * more call still fits inside `min(idle, admission)`.
         *
         * `apiCallTimeout` is an OVERALL budget for the call —
         * every attempt and all the backoff between them share it,
         * rather than each attempt getting its own. That is why the
         * per-attempt bound exists and is half of it: without it one
         * stalled attempt consumes the whole budget and the call fails
         * having never retried, which is the opposite of what a
         * retryable object-store fault wants.
         */
        fun callBoundFor(commitLockTimeoutMs: Long): Duration {
            val idle = Database.SESSION_INIT_SQL_IDLE_TIMEOUT
            val admission =
                if (commitLockTimeoutMs > 0) Duration.ofMillis(commitLockTimeoutMs) else idle
            return minOf(idle, admission).dividedBy(3)
        }

        /** See [holdBudget]: two call bounds, at any admission bound. */
        fun holdBudgetFor(commitLockTimeoutMs: Long): Duration = callBoundFor(commitLockTimeoutMs).multipliedBy(2)

        /** The bound at the compiled admission default, for no-arg construction. */
        val API_CALL_TIMEOUT: Duration =
            callBoundFor(com.posthog.hoglake.commit.CommitService.DEFAULT_COMMIT_LOCK_TIMEOUT_MS)

        /** The hold budget at the compiled admission default. */
        val HOLD_BUDGET: Duration =
            holdBudgetFor(com.posthog.hoglake.commit.CommitService.DEFAULT_COMMIT_LOCK_TIMEOUT_MS)
    }
}

/**
 * Drains hog_file_removal: the physical-deletion half of expiry/GC,
 * decoupled from metadata deletion (README.md §8).
 *
 * The non-negotiable invariant: a queue entry is a suggestion, never an
 * authorization. At drain time every path is re-checked against
 * hog_data_file AND hog_delete_file (any row, live or historical, in
 * the entry's catalog); a still-referenced path is counted as an
 * invariant violation (`still_referenced` — alert-worthy), skipped,
 * and its queue row LEFT UNDRAINED with attempts/last_attempt_at
 * bumped: the reference may legitimately go away later (v1 accepts
 * that a permanently-referenced entry is re-checked every run —
 * bounded by batch order, never a deletion).
 *
 * Draining SOFT-deletes (the forensics lesson: "we reconstructed
 * split-brain forensics from S3 delete markers"): a settled entry is
 * marked drained_at + drained_outcome ('deleted' for a physical
 * removal, 'absent' for verified-already-gone) instead of losing its
 * row, so what cleanup touched, when, and after how many attempts
 * stays queryable. The drain query reads only undrained rows (partial
 * index), and every sweep also PURGES drained rows older than
 * [ledgerRetentionSeconds] — the ledger must not itself become the
 * unbounded-accumulation problem it documents.
 *
 * Deletes run in sub-batches of [subBatchSize]
 * (HOGLAKE_CLEANUP_SUB_BATCH, 1,000 in production). Each sub-batch is
 * ONE transaction that (1) takes the per-catalog advisory commit lock
 * (Locks.kt — the SAME key as every commit/DDL tail), (2) re-checks
 * references, (3) physically deletes, (4) settles the ledger rows — so
 * a later sub-batch failure never rolls back completed ones. A
 * per-object delete failure bumps attempts and leaves the row queued
 * for the next run without wedging the rest of the batch.
 *
 * STEP 3 IS ONE `DeleteObjects` CALL PER BUCKET CHUNK, not a HEAD and a
 * DELETE per path. S3 caps that call at 1,000 keys, which is where
 * [subBatchSize]'s default comes from; a sub-batch whose paths span
 * several buckets, or exceed the key ceiling, is that many calls. A key
 * that was never there is reported exactly like one that was removed —
 * deleting a missing key IS the end state the queue asks for — so those
 * rows settle 'deleted' and `missing` is no longer produced for them.
 *
 * THE ONE CARVE-OUT is `reason = 'compaction_staging'`, which keeps
 * HEAD + DELETE per path: `/verify`'s `staging_tickets` check reads
 * their `'absent'` outcome (a staged path drained `'absent'` that IS a
 * live file row is the staged-output race resolved the wrong way), and
 * a `DeleteObjects` response cannot produce it. `'absent'` therefore
 * remains a legal and produced ledger value — for staging tickets, and
 * on every row settled before batching landed, which the 30-day ledger
 * keeps visible.
 *
 * BECAUSE THEY COST TWO ROUND TRIPS EACH, STAGING TICKETS DRAIN IN
 * THEIR OWN SUB-BATCHES, of [STAGING_SUB_BATCH] rather than
 * [subBatchSize]. The batch is partitioned by reason before any
 * sub-batch is formed. Mixed, a 1,000-row sub-batch of tickets would be
 * 1,000 probe pairs in one hold — ~128 s at the measured 64 ms per
 * round trip, 40x the hold this class exists to bound — and the staging
 * grace clusters them, because tickets become eligible in the order
 * their groups ran.
 *
 * Staging tickets are additionally left alone until they are past
 * [stagingGraceSeconds] (HOGLAKE_CLEANUP_STAGING_GRACE_SECONDS, 1 h).
 * The ticket is inserted BEFORE the rewrite starts, so on an empty
 * queue a drain could settle a fresh one 'absent' while its group was
 * still uploading; the group then aborts and re-stages, and between the
 * two the object exists with no ticket naming it. An hour is
 * comfortably longer than a group (~8.5 s on gigahog-prod-us), so the
 * case disappears for any group that finishes, while a ticket left by a
 * group that died is still reclaimed — an hour later, by the same
 * drain.
 *
 * WHY the lock (the check-then-delete TOCTOU): without it, a commit
 * transaction could pass ITS removal-queue check, insert a hog_data_file
 * row for a queued path, and be mid-flight (uncommitted, invisible to
 * READ COMMITTED) exactly when this drain computes referencedPaths —
 * the drain would see the path unreferenced, delete the object, and the
 * commit would then land a live row pointing at a deleted object.
 * Holding the catalog commit lock across the check+delete pair
 * serializes the two: either the commit finished first (its rows are
 * visible to the check, path skipped as still-referenced... and its own
 * queue-collision check would have 409'd anyway while the entry was
 * undrained), or the drain finishes first and the commit's collision
 * check runs after the entry settles. No interleaving remains.
 *
 * LOCK-HOLD BOUND, and it is the reason this class was rewritten.
 *
 * A sub-batch holds rows of ONE kind, so the TYPICAL hold is whichever
 * of these its kind costs, at the 64 ms per round trip measured on
 * gigahog-prod-us:
 *
 *  - bulk: an undrained re-check + a reference check + ONE
 *    `DeleteObjects` call per bucket chunk + two ledger UPDATEs. At the
 *    default sub-batch and a single bucket that is one round trip,
 *    ~64 ms;
 *  - staging: the same database statements + at most
 *    [STAGING_SUB_BATCH] HEAD/DELETE pairs — 25, so ~3.2 s.
 *
 * THE WORST CASE IS NOT THAT COUNT TIMES 64 ms, and this is the part a
 * call-count bound cannot state: each call may take a whole
 * [RemovalStore.apiCallTimeout]. So the hold is bounded in TIME by
 * [RemovalStore.holdBudget], checked before every call and requiring a
 * whole call bound of headroom — so the last call a sub-batch starts
 * always has room to finish inside the budget:
 *
 *   hold <= HOLD_BUDGET = 2 x call bound
 *   HOLD_BUDGET + one call <= min(idle bound, admission bound)
 *
 * — 20 s, and 20 s + 10 s <= 30 s at the defaults, every term derived
 * from `Database.SESSION_INIT_SQL_IDLE_TIMEOUT` and the EFFECTIVE
 * `HOGLAKE_COMMIT_LOCK_TIMEOUT_MS` rather than written down, so
 * lowering the admission bound shortens the hold with it. A sub-batch
 * the deadline stops short of leaves its remaining rows exactly as it
 * found them and they are the next hold's work (counted
 * `deadline_skipped`); without it, the backend is killed
 * idle-in-transaction and the sub-batch rolls back with its objects
 * already deleted.
 *
 * THE HOLD WAS NEVER THE PROBLEM; THE DUTY CYCLE WAS. Sub-batches run
 * back to back, so a run of [batchSize] rows takes
 * `batchSize / subBatchSize` holds IN A ROW, and the old arithmetic
 * bounded one of them and not the run. Measured on gigahog-prod-us
 * (2026-09-24, catalog millpond-prod-us): at the old sub-batch of 25 x
 * (HEAD + DELETE) each hold ran ~3.2 s, so a 2,000-row run spent ~255 s
 * holding the lock across 80 slices. Commit latency on the API pods
 * went from 200-400 ms average to 12-22 s (max 34 s, against the 30 s
 * admission bound) and both gigahog-server pods were liveness-killed:
 * every queued commit holds a pool connection and `/healthz` needs one
 * too.
 *
 * With one round trip per 1,000 rows instead of 50 per 25, that same
 * 2,000-row run is two holds of a few hundred milliseconds. The lock
 * stays exactly where it was — it is still what serializes the
 * check-then-delete pair against the commit tail — and what changed is
 * that holding it is now cheap. [subBatchSize] is still the knob that
 * scales the hold, and it should stay at or below the 1,000-key ceiling
 * of one `DeleteObjects` call: past that a sub-batch is several round
 * trips inside one hold, which is the shape this change removed.
 */
class CleanupService(
    private val jdbi: Jdbi,
    private val store: RemovalStore,
    private val subBatchSize: Int = SUB_BATCH,
    private val ledgerRetentionSeconds: Long = LEDGER_RETENTION_SECONDS,
    private val maintenanceLedgerRetentionSeconds: Long = MAINTENANCE_LEDGER_RETENTION_SECONDS,
    private val stagingGraceSeconds: Long = STAGING_GRACE_SECONDS,
    /**
     * How long one sub-batch may spend on object-store calls before it
     * stops issuing them.
     *
     * Defaults to the store's own [RemovalStore.holdBudget], which is
     * two of its call bounds — so it is derived from
     * `min(idle, admission)` exactly as the call bound is, and an
     * operator who lowers HOGLAKE_COMMIT_LOCK_TIMEOUT_MS gets a shorter
     * hold with it. Constructor-tunable so a test can shrink it rather
     * than sleep through it.
     */
    private val holdBudget: Duration = store.holdBudget,
    /**
     * The clock the hold deadline reads, as nanoseconds. Injected so a
     * test can drive the deadline exactly instead of sleeping — a
     * deadline test that sleeps is a deadline test that is flaky.
     */
    private val nanoTime: () -> Long = System::nanoTime,
) {
    private val log = KotlinLogging.logger {}

    /** The maintenance run ledger; also the owner of its retention purge. */
    private val runStore = MaintenanceRunStore(jdbi)

    init {
        require(subBatchSize > 0) { "subBatchSize must be positive (got $subBatchSize)" }
        require(ledgerRetentionSeconds > 0) {
            "ledgerRetentionSeconds must be positive (got $ledgerRetentionSeconds)"
        }
        require(maintenanceLedgerRetentionSeconds > 0) {
            "maintenanceLedgerRetentionSeconds must be positive (got $maintenanceLedgerRetentionSeconds)"
        }
        require(stagingGraceSeconds >= 0) {
            "stagingGraceSeconds must not be negative (got $stagingGraceSeconds)"
        }
        // STRICT: the gate is `elapsed <= holdBudget - apiCallTimeout`, so
        // at equality it reads `elapsed <= 0` and only a call issued in
        // the same nanosecond as the lock could ever pass it. A drain
        // that issues no calls is not a smaller drain, it is a stopped
        // one.
        require(holdBudget > store.apiCallTimeout) {
            "holdBudget ($holdBudget) must exceed one object-store call " +
                "(${store.apiCallTimeout}), or no sub-batch can issue one"
        }
    }

    private data class Entry(val removalId: Long, val path: String, val reason: String)

    /** A per-path audit event, collected in a sub-batch and emitted after it commits. */
    private data class PathEvent(val action: String, val path: String, val outcome: String, val detail: String?)

    /**
     * Drain up to [batchSize] queue entries for [catalog]. The summary
     * audit event and the files-removed counter are emitted at the end
     * of the run (each sub-batch's queue drain is its own transaction;
     * nothing is emitted inside one); per-path events (file_deleted /
     * cleanup_violation) are emitted as the drain progresses, bounded by
     * the batch size. still_referenced > 0 is an invariant violation and
     * flags the run's audit outcome accordingly. A ZERO-WORK drain
     * (nothing removed, missing, or violated) emits no audit event —
     * app-log debug only, so idle background loops stay out of the
     * audit stream.
     */
    fun runOnce(
        catalog: String,
        batchSize: Int,
        trigger: MaintenanceTrigger = MaintenanceTrigger.MANUAL,
    ): CleanupResult =
        runStore.recorded(catalog, MaintenanceTask.CLEANUP, trigger) {
            runDrain(catalog, batchSize)
        }

    private fun runDrain(
        catalog: String,
        batchSize: Int,
    ): CleanupResult {
        val result =
            try {
                doRunOnce(catalog, batchSize)
            } catch (e: Throwable) {
                Audit.event("cleanup", catalog, null, Audit.failureOutcome(e), e.message)
                throw e
            }
        // objectsRemoved, not removed: the metric is documented as
        // physical deletes and `removed` counts ledger rows.
        Metrics.filesRemoved(catalog, result.objectsRemoved)
        // deadlineSkipped is in the condition, not just the detail: a
        // drain whose holds all end on their budget settles nothing, and
        // without it here that run takes the "nothing to do" branch and
        // a wedged drain is indistinguishable from an idle one in both
        // the audit stream and the run ledger.
        if (result.removed == 0L && result.missing == 0L && result.stillReferenced == 0L &&
            result.deadlineSkipped == 0L
        ) {
            log.debug { "cleanup drain for catalog '$catalog': nothing to do" }
        } else {
            Audit.event(
                "cleanup",
                catalog,
                null,
                outcome = if (result.stillReferenced > 0) "invariant_violation" else "ok",
                detail =
                    "removed=${result.removed} missing=${result.missing} " +
                        "still_referenced=${result.stillReferenced} " +
                        "objects_removed=${result.objectsRemoved} " +
                        "settled_elsewhere=${result.settledElsewhere} " +
                        "deadline_skipped=${result.deadlineSkipped}",
            )
        }
        return result
    }

    private fun doRunOnce(
        catalog: String,
        batchSize: Int,
    ): CleanupResult {
        if (batchSize <= 0) {
            throw HoglakeException.Validation("batch size must be positive (got $batchSize)")
        }
        val catalogId =
            jdbi.withHandleUnchecked { h ->
                CatalogRepo.require(h, catalog).catalogId
            }
        val batch =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(DRAIN_BATCH_SQL)
                    .bind("catalogId", catalogId)
                    .bind("limit", batchSize)
                    .bind("stagingGraceSeconds", stagingGraceSeconds.toDouble())
                    .map { rs, _ ->
                        Entry(rs.getLong("removal_id"), rs.getString("path"), rs.getString("reason"))
                    }
                    .list()
            }

        // PARTITIONED BY REASON, and this is a lock-hold decision, not
        // tidiness. A `compaction_staging` row costs a HEAD and a DELETE
        // (see [RemovalStore.deleteIfExists] for why it may not be
        // batched); every other row costs a share of one DeleteObjects
        // call. Left mixed, one 1,000-row sub-batch of staging tickets
        // would be 1,000 probe pairs inside ONE lock hold — ~128 s at the
        // 64 ms per round trip measured on gigahog-prod-us, which is
        // 40x the hold this change exists to remove — and the staging
        // grace CLUSTERS them, because tickets become eligible in the
        // order their groups ran. So the two reasons drain in their own
        // sub-batches, at their own sizes, each its own transaction and
        // its own lock hold. Ordering across the two is not a contract:
        // the queue is age-ordered within each, and `staging_tickets`
        // stops firing because the run drains, not because of where in
        // the run a row sits.
        val (stagingRows, bulkRows) = batch.partition { it.reason == STAGING_REASON }
        val subBatches =
            bulkRows.chunked(subBatchSize).map { it to false } +
                stagingRows.chunked(STAGING_SUB_BATCH).map { it to true }

        var removed = 0L
        var missing = 0L
        var stillReferenced = 0L
        var objectsRemoved = 0L
        var settledElsewhere = 0L
        var deadlineSkipped = 0L

        for ((sub, probeEachPath) in subBatches) {
            val result = drainSubBatch(catalog, catalogId, sub, probeEachPath)
            removed += result.removed
            missing += result.missing
            stillReferenced += result.stillReferenced
            objectsRemoved += result.objectsRemoved
            settledElsewhere += result.settledElsewhere
            deadlineSkipped += result.deadlineSkipped
        }
        purgeDrainedLedger(catalog, catalogId)
        purgeMaintenanceLedger(catalog, catalogId)
        return CleanupResult(
            removed,
            missing,
            stillReferenced,
            objectsRemoved,
            settledElsewhere,
            deadlineSkipped,
        )
    }

    /** One sub-batch's tally, accumulated only after its transaction commits. */
    private data class SubBatchResult(
        val removed: Long,
        val missing: Long,
        val stillReferenced: Long,
        val objectsRemoved: Long,
        val settledElsewhere: Long,
        val deadlineSkipped: Long,
    )

    /**
     * ONE sub-batch: one transaction, one lock hold, one flush of audit
     * events after it commits.
     *
     * [probeEachPath] is what separates the two shapes. False (the bulk
     * path) deletes with [RemovalStore.deleteBatch] — one call per bucket
     * chunk — and settles everything it did not fail as `'deleted'`.
     * True (the `compaction_staging` path) does HEAD + DELETE per path so
     * `'absent'` still reaches the ledger for `/verify`, and is called
     * with at most [STAGING_SUB_BATCH] rows for exactly that reason.
     *
     * Nothing is accumulated into the run's counters until the
     * transaction has committed: a sub-batch that rolls back must
     * contribute nothing, including its violations.
     */
    private fun drainSubBatch(
        catalog: String,
        catalogId: Long,
        sub: List<Entry>,
        probeEachPath: Boolean,
    ): SubBatchResult {
        // Per-path audit events are collected inside the sub-batch
        // transaction and emitted AFTER it commits (invariant 8: audit
        // never rides a transaction — and never rides the catalog lock).
        val events = mutableListOf<PathEvent>()
        var subRemoved = 0L
        var subMissing = 0L
        var subStillReferenced = 0L
        var subObjects = 0L
        var subSettledElsewhere = 0L
        var subDeadlineSkipped = 0L
        jdbi.useTransactionUnchecked { h ->
            // The check+delete pair is serialized against the commit
            // tail by the SAME per-catalog advisory lock every commit
            // takes (see class KDoc for the race and the hold bound):
            // the reference check below can never go stale against an
            // in-flight commit registering one of these paths.
            Locks.acquireCatalogCommitLock(h, catalogId)

            // FIRST STATEMENT UNDER THE LOCK: is each row still
            // undrained? The batch select ran outside the lock, and the
            // one writer besides this drain that settles a row —
            // CompactionService's group commit, which settles its
            // staging ticket 'registered' in the transaction that makes
            // the path live — can land in between. Without this
            // re-check that row reaches the reference check, whose
            // answer is now "referenced" because the path is a live
            // file: an ERROR log, a `cleanup_violation` audit event, a
            // `still_referenced` count that pages someone, and an
            // attempts bump on a settled row. All four of them are
            // wrong, and the state that produced them is a compaction
            // group committing normally.
            val live =
                h.createQuery(STILL_UNDRAINED_SQL)
                    .bind("catalogId", catalogId)
                    .bindArray("ids", Long::class.javaObjectType, sub.map { it.removalId })
                    .mapTo(Long::class.javaObjectType)
                    .list()
                    .toSet()
            val entries = sub.filter { it.removalId in live }
            subSettledElsewhere = (sub.size - entries.size).toLong()
            if (subSettledElsewhere > 0) {
                log.debug {
                    "cleanup: $subSettledElsewhere of ${sub.size} rows in this sub-batch were " +
                        "settled between the batch select and the lock (a compaction group's " +
                        "'registered'); dropping them from the sub-batch"
                }
            }
            if (entries.isEmpty()) return@useTransactionUnchecked

            // THE HOLD DEADLINE. The clock starts once the lock is ours,
            // and no object-store call may START unless the budget can
            // still absorb a whole one — so the LAST call always has a
            // full call bound of room inside the budget, and what the
            // gate enforces is:
            //
            //   hold <= HOLD_BUDGET = 2 x call bound
            //   HOLD_BUDGET + one call <= min(idle, admission)
            //
            // Without it the hold is bounded only in CALLS, and a call is
            // bounded by RemovalStore.apiCallTimeout — so 25 probe pairs
            // could reach 500 s and a three-chunk bulk sub-batch 30 s,
            // both past the idle bound. Past it Postgres kills the
            // backend: the sub-batch rolls back with its objects DELETED
            // and its ledger rows unsettled, and the next run re-drains
            // the same rows and does it again, forever.
            //
            // Rows the deadline stops short of are left exactly as found
            // — no settle, no attempts bump — because nothing was
            // attempted on them. They are simply the next hold's work,
            // and they are COUNTED (`deadline_skipped`), because a drain
            // whose every hold ends on the budget settles nothing and
            // would otherwise look idle.
            val holdStart = nanoTime()
            val mayIssueCall = {
                Duration.ofNanos(nanoTime() - holdStart) <= holdBudget.minus(store.apiCallTimeout)
            }
            val unattempted = mutableListOf<String>()

            // Liveness is checked per sub-batch at drain time, under
            // the lock: the freshest answer possible before touching
            // the object.
            val referenced = referencedPaths(h, catalogId, entries.map { it.path })
            // Ledger outcomes for this sub-batch: settled entries by
            // outcome, plus the ones that stay queued (attempts bump).
            val drainedByOutcome = mapOf("deleted" to mutableListOf<Long>(), "absent" to mutableListOf())
            val attempted = mutableListOf<Long>()
            // Paths whose object this sub-batch physically removed. A SET
            // because two queue rows over one path are legitimate state
            // (V16's non-unique argument) and one object went: this is
            // what the physical-delete metric and the audit events count.
            val removedPaths = LinkedHashSet<String>()
            val survivors = mutableListOf<Entry>()
            for (entry in entries) {
                if (entry.path in referenced) {
                    log.error {
                        "cleanup: path '${entry.path}' (removal_id ${entry.removalId}) is " +
                            "still referenced by the catalog — invariant violation; skipping"
                    }
                    // Per-path audit trail for the alert-worthy case: WHICH
                    // path the queue wrongly suggested. Bounded by batch size.
                    events +=
                        PathEvent(
                            "cleanup_violation",
                            entry.path,
                            "invariant_violation",
                            "removal_id=${entry.removalId} still referenced; not deleted",
                        )
                    subStillReferenced++
                    attempted += entry.removalId
                    continue
                }
                survivors += entry
            }

            if (probeEachPath) {
                // Staging tickets: HEAD before DELETE, one path at a
                // time, at most STAGING_SUB_BATCH of them in this hold.
                for (entry in survivors) {
                    try {
                        when (store.deleteIfExists(entry.path, mayIssueCall)) {
                            RemovalStore.Outcome.SKIPPED -> {
                                // The budget closed. Everything from here
                                // on is untouched, including this row.
                                unattempted += survivors.dropWhile { it !== entry }.map { it.path }
                                break
                            }
                            RemovalStore.Outcome.REMOVED -> {
                                removedPaths += entry.path
                                drainedByOutcome.getValue("deleted") += entry.removalId
                            }
                            RemovalStore.Outcome.MISSING -> {
                                log.info { "cleanup: '${entry.path}' already gone; draining queue row" }
                                drainedByOutcome.getValue("absent") += entry.removalId
                            }
                        }
                    } catch (e: Exception) {
                        // Leave the row queued (attempts bumped); the next run
                        // retries it.
                        log.error(e) {
                            "cleanup: delete failed for '${entry.path}' " +
                                "(removal_id ${entry.removalId}); leaving queued"
                        }
                        attempted += entry.removalId
                    }
                }
                if (unattempted.isNotEmpty()) {
                    log.warn {
                        "cleanup: hold budget ($holdBudget) spent after " +
                            "${survivors.size - unattempted.size} of ${survivors.size} staging " +
                            "tickets; leaving the rest untouched for the next sub-batch"
                    }
                }
            } else if (survivors.isNotEmpty()) {
                // Everything else: one DeleteObjects call per bucket
                // chunk. A key that was not there comes back
                // indistinguishable from one that was removed, and that
                // is the end state the queue asked for, so the row
                // settles 'deleted' either way.
                val rowsByPath = survivors.groupBy { it.path }
                val outcome = store.deleteBatch(rowsByPath.keys, mayIssueCall)
                val failures = outcome.failures
                unattempted += outcome.unattempted
                if (outcome.unattempted.isNotEmpty()) {
                    log.warn {
                        "cleanup: hold budget ($holdBudget) spent after " +
                            "${rowsByPath.size - outcome.unattempted.size} of " +
                            "${rowsByPath.size} paths; leaving the rest untouched for the next " +
                            "sub-batch"
                    }
                }
                val skipped = outcome.unattempted.toSet()
                for ((path, rows) in rowsByPath) {
                    if (path in skipped) continue
                    val failure = failures[path]
                    if (failure == null) {
                        removedPaths += path
                        drainedByOutcome.getValue("deleted") += rows.map { it.removalId }
                    } else {
                        log.error {
                            "cleanup: batched delete failed for '$path' (removal_id " +
                                rows.joinToString { it.removalId.toString() } +
                                "): $failure; leaving queued"
                        }
                        attempted += rows.map { it.removalId }
                    }
                }
            }

            // Soft-delete the settled entries: the row survives as
            // the queryable ledger of what cleanup did and when.
            for ((outcome, ids) in drainedByOutcome) {
                if (ids.isEmpty()) continue
                val settled =
                    h.createQuery(SETTLE_SQL)
                        .bind("outcome", outcome)
                        .bind("catalogId", catalogId)
                        .bindArray("ids", Long::class.javaObjectType, ids)
                        .mapTo(Long::class.javaObjectType)
                        .list()
                for (lost in ids - settled.toSet()) {
                    log.warn {
                        "cleanup: removal_id $lost was settled by another writer inside this " +
                            "sub-batch's own lock hold; leaving that row alone and not counting " +
                            "it as '$outcome'"
                    }
                }
                when (outcome) {
                    "deleted" -> subRemoved = settled.size.toLong()
                    "absent" -> subMissing = settled.size.toLong()
                }
            }
            // Physical deletions are the audit log's whole point: one
            // event per object actually removed.
            subObjects = removedPaths.size.toLong()
            for (path in removedPaths) {
                events += PathEvent("file_deleted", path, "ok", null)
            }
            // Rows the deadline never reached. Counted, not settled and
            // not attempted — see CleanupResult.deadlineSkipped for why
            // a run has to report them rather than look idle.
            subDeadlineSkipped = unattempted.size.toLong()
            // Undrained entries (still-referenced, transient S3
            // failure) record the attempt and stay queued. Fenced on
            // `drained_at IS NULL` for the same reason the settle is:
            // this statement must never touch a row another writer
            // settled, and `last_attempt_at` on a settled row is a lie
            // about what cleanup did to it.
            if (attempted.isNotEmpty()) {
                h.createUpdate(BUMP_ATTEMPTS_SQL)
                    .bind("catalogId", catalogId)
                    .bindArray("ids", Long::class.javaObjectType, attempted)
                    .execute()
            }
        }
        // Sub-batch committed (lock released): emit its audit events.
        for (e in events) {
            Audit.event(e.action, catalog, e.path, outcome = e.outcome, detail = e.detail)
        }
        return SubBatchResult(
            subRemoved,
            subMissing,
            subStillReferenced,
            subObjects,
            subSettledElsewhere,
            subDeadlineSkipped,
        )
    }

    /**
     * Maintenance-ledger retention (the hog_maintenance_run twin of
     * [purgeDrainedLedger]): run rows older than
     * [maintenanceLedgerRetentionSeconds] are hard-deleted so the ledger
     * stays a bounded recent history, not an accumulation.
     */
    private fun purgeMaintenanceLedger(
        catalog: String,
        catalogId: Long,
    ) {
        val purged =
            jdbi.withHandleUnchecked { h ->
                runStore.purge(h, catalogId, maintenanceLedgerRetentionSeconds)
            }
        if (purged > 0) {
            log.debug { "cleanup: purged $purged maintenance run rows for catalog '$catalog'" }
        }
    }

    /**
     * Ledger retention: drained rows older than [ledgerRetentionSeconds]
     * are hard-deleted so the soft-delete ledger cannot itself
     * accumulate without bound (the A1 lesson, applied to the fix for
     * A3). Undrained rows are never touched here.
     */
    private fun purgeDrainedLedger(
        catalog: String,
        catalogId: Long,
    ) {
        val purged =
            jdbi.withHandleUnchecked { h ->
                h.createUpdate(
                    """
                    DELETE FROM hog_file_removal
                    WHERE catalog_id = :catalogId
                      AND drained_at < now() - make_interval(secs => :retention)
                    """,
                )
                    .bind("catalogId", catalogId)
                    .bind("retention", ledgerRetentionSeconds)
                    .execute()
            }
        if (purged > 0) {
            log.debug { "cleanup: purged $purged drained ledger rows for catalog '$catalog'" }
        }
    }

    /**
     * Paths from [paths] that any file row (live or not) still claims.
     * Runs on the sub-batch transaction's handle, under the catalog
     * commit lock, so the answer cannot go stale against a commit.
     */
    private fun referencedPaths(
        h: Handle,
        catalogId: Long,
        paths: List<String>,
    ): Set<String> =
        h.createQuery(
            """
            SELECT path FROM hog_data_file
            WHERE catalog_id = :catalogId AND path = ANY(:paths)
            UNION
            SELECT path FROM hog_delete_file
            WHERE catalog_id = :catalogId AND path = ANY(:paths)
            UNION
            SELECT path FROM hog_upload
            WHERE catalog_id = :catalogId AND path = ANY(:paths) AND state = 'active'
            """,
        )
            .bind("catalogId", catalogId)
            .bindArray("paths", String::class.java, paths)
            .mapTo(String::class.java)
            .list()
            .toSet()

    /**
     * One drain across every catalog, for the background loop
     * (BackgroundLoops in App.kt). Catalogs are isolated: one catalog's
     * failure is logged and the rest proceed.
     */
    fun runOnceAllCatalogs(batchSize: Int): List<Pair<String, CleanupResult>> {
        val names = jdbi.withHandleUnchecked { h -> CatalogRepo.listAll(h) }.map { it.name }
        val results = mutableListOf<Pair<String, CleanupResult>>()
        for (name in names) {
            try {
                results += name to runOnce(name, batchSize, MaintenanceTrigger.LOOP)
            } catch (e: Exception) {
                log.error(e) { "cleanup drain failed for catalog '$name'; continuing" }
            }
        }
        return results
    }

    /** internal, not private: UploadService's sweep shares the drained-ledger retention. */
    internal companion object {
        /**
         * `hog_file_removal.reason` for a compaction staging ticket: the
         * one reason the drain treats differently, on both counts — HEAD
         * before DELETE, so `'absent'` still reaches the ledger for
         * `/verify`, and the staging grace below.
         */
        const val STAGING_REASON = "compaction_staging"

        /**
         * Production sub-batch size for physical deletes
         * (HOGLAKE_CLEANUP_SUB_BATCH).
         *
         * 1,000 is S3's own ceiling on ONE `DeleteObjects` request, so
         * the default sub-batch is exactly one object-store round trip
         * inside the commit-lock hold. It was 25, and that number was
         * sized for a different statement: the drain then issued
         * subBatchSize x (HEAD + DELETE), so the constant was a
         * commit-latency bound (~3.2 s per hold, ~255 s of holding per
         * 2,000-row run — see the class KDoc's measurement) rather than
         * a batching decision.
         *
         * The lock hold scales with the number of CALLS a sub-batch
         * makes, not with this number directly: 1,000 keys in one bucket
         * is one call, and 2,500 keys spread over three buckets is four
         * whatever this is set to. Raising it past 1,000 buys nothing —
         * the extra keys chunk into extra calls inside the same hold —
         * so the ceiling is where it stops being free, not where it
         * starts being wrong. `compaction_staging` rows do not use this
         * knob at all; see [STAGING_SUB_BATCH].
         */
        const val SUB_BATCH = 1000

        /**
         * Staging tickets per sub-batch, and therefore per lock hold.
         *
         * 25 is the OLD whole-queue sub-batch, kept for the one reason
         * that still costs what the old one did: a
         * `compaction_staging` row is a HeadObject and a DeleteObject,
         * and there is no batched form that reports `'absent'`. At the
         * 64 ms per round trip measured on gigahog-prod-us that is
         * ~3.2 s of TYPICAL hold — the bound the old design claimed for
         * the whole drain, now paid only by the rows that genuinely need
         * two probes, and only when a run actually holds that many stale
         * tickets. The WORST case is not this count times 64 ms; it is
         * [HOLD_BUDGET], because a slow call takes what the call bound
         * allows and the deadline is what stops the hold.
         *
         * Splitting the reasons is not cosmetic. One sub-batch of 1,000
         * tickets would be ~128 s of typical hold, 40x what this change
         * removed, and [STAGING_GRACE_SECONDS] clusters them: tickets
         * become eligible in the order their groups ran, so a compaction
         * sweep that aborted a run of groups puts its whole run into one
         * sub-batch an hour later.
         */
        const val STAGING_SUB_BATCH = 25

        /**
         * How long a `compaction_staging` ticket is left alone
         * (HOGLAKE_CLEANUP_STAGING_GRACE_SECONDS): 3,600 s.
         *
         * 0 does not remove the predicate — it makes every ticket from a
         * transaction that has already committed eligible, since
         * `scheduled_at < now()` holds for any row this drain can read.
         *
         * THE CEILING IS `/verify`, not taste:
         * `VerifyService.DEFAULT_STAGING_TICKET_MAX_AGE_SECONDS` (6 h)
         * is when `staging_tickets` calls an undrained ticket leaked.
         * This grace plus `HOGLAKE_CLEANUP_INTERVAL_MS` plus however
         * long the backlog takes to reach the row must stay well under
         * that, or the check reports tickets the drain is deliberately
         * leaving alone — an alert that fires on a healthy system, which
         * is how an alert stops being read. At the defaults (1 h grace,
         * 30 min interval) there is a factor of four in hand.
         *
         * The ticket is inserted BEFORE the rewrite starts, so on an
         * empty queue a drain can settle a fresh one `'absent'` while its
         * group is still uploading; the group then aborts and re-stages,
         * and between the two the object exists with no ticket naming it.
         * An hour is comfortably longer than a group (~8.5 s on
         * gigahog-prod-us), so the case disappears for every group that
         * finishes, while a ticket left by a group that died is still
         * reclaimed — an hour later, by the same drain.
         */
        const val STAGING_GRACE_SECONDS = 3600L

        /**
         * The drain's batch select: the oldest undrained rows of one
         * catalog, in queue order. This is the statement
         * `hog_file_removal_drain (catalog_id, removal_id) WHERE
         * drained_at IS NULL` exists for — when it is chosen, the index
         * supplies both the predicate and the `ORDER BY`, so the LIMIT
         * stops the scan rather than trimming a sort.
         *
         * WHEN IT IS CHOSEN is a real qualifier, and it is the SPARSE
         * shape: a catalog whose undrained rows are a small fraction of
         * the table, which is what a 30-day ledger gives one that
         * drains. Where they are DENSE — a catalog that is behind, which
         * is the state #199 describes — the primary key on `removal_id`
         * already arrives in the right order and discards only a row or
         * two per row it emits, and the planner takes THAT instead.
         * Both plans are correct and both stop at the LIMIT; the drain
         * index is the one that keeps the cost bounded as the settled
         * ledger grows around the queue.
         *
         * `internal` so `V16FileRemovalPathIndexMigrationIntegrationTest`
         * can prove the drain index survives V16 — on both shapes. A new
         * index the planner prefers HERE would be a regression, not a
         * win: `(catalog_id, path)` supplies no `removal_id` ordering, so
         * the LIMIT would sit on top of a sort of every undrained row.
         *
         * The `compaction_staging` clause is a FILTER on rows the index
         * has already narrowed to one catalog's queue, at most a batch
         * past the leading columns — it is not something to index. See
         * [STAGING_GRACE_SECONDS] for what it is for.
         *
         * Binds `:catalogId`, `:limit` and `:stagingGraceSeconds`. No
         * interpolated values (invariant 9 intact).
         */
        internal const val DRAIN_BATCH_SQL: String =
            """
            SELECT removal_id, path, reason FROM hog_file_removal
            WHERE catalog_id = :catalogId AND drained_at IS NULL
              AND (reason <> 'compaction_staging'
                   OR scheduled_at < now() - make_interval(secs => :stagingGraceSeconds))
            ORDER BY removal_id
            LIMIT :limit
            """

        /**
         * Which of this sub-batch's rows are STILL undrained, asked as
         * the first statement under the commit lock.
         *
         * The batch select runs outside the lock, and one other writer
         * settles rows: `CompactionService`'s group commit, which
         * settles its staging ticket `'registered'` in the same
         * transaction that registers the output path. A row it settled
         * in that window is not this drain's to touch — and worse, its
         * path is now a live file, so the reference check would call it
         * an invariant violation and page someone about a compaction
         * group committing normally.
         *
         * Binds `:catalogId` and the `:ids` array.
         */
        internal const val STILL_UNDRAINED_SQL: String =
            """
            SELECT removal_id FROM hog_file_removal
            WHERE catalog_id = :catalogId AND removal_id = ANY(:ids) AND drained_at IS NULL
            """

        /**
         * Record a touch that did NOT drain the row — a still-referenced
         * skip, or a delete that failed. Fenced on `drained_at IS NULL`
         * for the same reason [SETTLE_SQL] is: a settled row's
         * `attempts`/`last_attempt_at` describe what cleanup did to it
         * before it settled, and bumping them afterwards is a false
         * statement in the forensics ledger.
         *
         * Binds `:catalogId` and the `:ids` array.
         */
        internal const val BUMP_ATTEMPTS_SQL: String =
            """
            UPDATE hog_file_removal
               SET attempts = attempts + 1, last_attempt_at = now()
             WHERE catalog_id = :catalogId AND removal_id = ANY(:ids)
               AND drained_at IS NULL
            """

        /**
         * Settle a drained row, FENCED on `drained_at IS NULL`.
         *
         * THE BACKSTOP, not the catcher. What actually catches the
         * realistic race — a compaction group settling its staging
         * ticket `'registered'` between this run's batch select and its
         * sub-batch — is [STILL_UNDRAINED_SQL], asked as the first
         * statement under the lock, which drops the row from the
         * sub-batch before anything touches it. This predicate covers
         * the remainder: a settle that lands after that re-check, inside
         * this sub-batch's own lock hold. Without it the statement can
         * stamp its outcome over one another writer already recorded,
         * erasing the only record that the path became a live catalog
         * file and leaving a ledger saying cleanup deleted an object the
         * catalog is serving.
         *
         * `RETURNING` is therefore the count that may be trusted: the
         * caller reports the difference against the ids it asked for and
         * counts only what it actually settled.
         *
         * Binds `:outcome`, `:catalogId` and the `:ids` array.
         */
        internal const val SETTLE_SQL: String =
            """
            UPDATE hog_file_removal
               SET drained_at = now(), drained_outcome = :outcome,
                   last_attempt_at = now()
             WHERE catalog_id = :catalogId AND removal_id = ANY(:ids)
               AND drained_at IS NULL
            RETURNING removal_id
            """

        /** Default drained-ledger retention: 30 days (HOGLAKE_REMOVAL_LEDGER_RETENTION_SECONDS). */
        const val LEDGER_RETENTION_SECONDS = 30L * 24 * 60 * 60

        /** Default run-ledger retention: 7 days (HOGLAKE_MAINTENANCE_LEDGER_RETENTION_SECONDS). */
        const val MAINTENANCE_LEDGER_RETENTION_SECONDS = 7L * 24 * 60 * 60
    }
}
