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
import com.posthog.hoglake.persistence.MaintenanceRunStore
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
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
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors

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
     * (HOGLAKE_COMMIT_LOCK_TIMEOUT_MS), not the compiled default: see
     * [callBoundFor] for what the call bound still derives from it, now
     * that a drain holds no lock. 0 means an unbounded wait.
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

    /** What one path's probe-and-delete did. */
    enum class Outcome { REMOVED, MISSING }

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
    open fun deleteIfExists(pathUri: String): Outcome {
        if (!exists(pathUri)) return Outcome.MISSING
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
     * [deleteChunk] request, which is the unit a sub-batch's object-store
     * cost is measured in.
     */
    open fun deleteBatch(paths: Collection<String>): Map<String, String> {
        val failures = mutableMapOf<String, String>()
        val byBucket = mutableMapOf<String, MutableList<Pair<String, String>>>()
        for (path in paths.distinct()) {
            val loc =
                try {
                    ObjectStore.parse(path)
                } catch (e: IllegalArgumentException) {
                    failures[path] = e.message ?: "unparseable object path"
                    continue
                }
            byBucket.getOrPut(loc.bucket) { mutableListOf() } += loc.key to path
        }
        for ((bucket, entries) in byBucket) {
            for (chunk in entries.chunked(MAX_KEYS_PER_DELETE)) {
                failures += deleteChunk(bucket, chunk)
            }
        }
        return failures
    }

    /**
     * ONE `DeleteObjects` request: one bucket, at most
     * [MAX_KEYS_PER_DELETE] keys, given as `(key, path)` pairs, with the
     * failures keyed by path.
     *
     * A named, `open` function rather than an inline loop because it is
     * the unit a sub-batch's object-store cost is measured in, and that
     * unit is the whole point of the batching: a test that counts the
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
         * THE CALL BOUND, and what it is FOR has changed even though the
         * number has not.
         *
         * It used to be an inequality. `CleanupService`'s object-store
         * calls ran inside a transaction that held the per-catalog
         * commit lock, so a call that hung was a connection sitting
         * idle-in-transaction holding the one lock the catalog's writers
         * queue on — and the bound had to sit under BOTH
         * `Database.SESSION_INIT_SQL`'s 30 s
         * `idle_in_transaction_session_timeout` and
         * [commitLockTimeoutMs] or it could never fire usefully.
         *
         * NEITHER BOUND APPLIES ANY MORE. The drain is a claimed work
         * queue: the claim commits before the first call and the settle
         * opens a new transaction after the last one, so no transaction
         * is open across an object-store call and cleanup takes no
         * catalog lock at any point. What a hung call costs now is one
         * PARKED WORKER holding a claim until its lease expires
         * (HOGLAKE_CLEANUP_CLAIM_LEASE_SECONDS, 900 s) — work delayed,
         * never a convoy and never a lost object.
         *
         * The number is KEPT rather than re-derived, deliberately:
         * `min(idle, admission) / 3` is 10 s at the defaults, it is the
         * bound every measured drain in this repo ran under, and nothing
         * measured argues for another. What it still buys is that an
         * operator who lowers HOGLAKE_COMMIT_LOCK_TIMEOUT_MS to keep a
         * pod responsive gets shorter object-store calls with it, and
         * that A WHOLE CLAIM'S WORST CASE FITS INSIDE THE LEASE at this
         * bound — which is a statement about a CLAIM and is only true
         * because the claim is reason-aware: a staging claim is
         * `STAGING_SUB_BATCH` rows and two calls each (25 x 2 x 10 s =
         * 500 s), a bulk claim is one call per bucket chunk. It was NOT
         * true of a claim with no reason predicate, which could hold
         * `SUB_BATCH` tickets and 20,000 s of calls under one 900 s
         * lease. `RemovalStoreBoundsTest` asserts both arms.
         * A drain that wanted a different bound would get its own knob;
         * inventing one here, with no measurement behind it, would only
         * move the arbitrariness.
         *
         * `apiCallTimeout` is an OVERALL budget for the call — every
         * attempt and all the backoff between them share it, rather than
         * each attempt getting its own. That is why the per-attempt
         * bound exists and is half of it: without it one stalled attempt
         * consumes the whole budget and the call fails having never
         * retried, which is the opposite of what a retryable
         * object-store fault wants.
         */
        fun callBoundFor(commitLockTimeoutMs: Long): Duration {
            val idle = Database.SESSION_INIT_SQL_IDLE_TIMEOUT
            val admission =
                if (commitLockTimeoutMs > 0) Duration.ofMillis(commitLockTimeoutMs) else idle
            return minOf(idle, admission).dividedBy(3)
        }

        /** The bound at the compiled admission default, for no-arg construction. */
        val API_CALL_TIMEOUT: Duration =
            callBoundFor(com.posthog.hoglake.commit.CommitService.DEFAULT_COMMIT_LOCK_TIMEOUT_MS)
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
 * stays queryable. The drain reads only undrained rows (partial
 * index), and every sweep also PURGES drained rows older than
 * [ledgerRetentionSeconds] — the ledger must not itself become the
 * unbounded-accumulation problem it documents.
 *
 * IT IS A CLAIMED WORK QUEUE, AND IT TAKES NO CATALOG LOCK ANYWHERE.
 * That is the change, and what has to be true for it is that the pair
 * (this drain, the commit path) cannot make a path live and delete its
 * object at the same time. TWO CLAIMS DO THE WORK:
 *
 *  - a commit passes `CommitService.REMOVAL_QUEUE_COLLISION_SQL` only
 *    when no UNDRAINED row names the path it is registering, and this
 *    drain only ever reads undrained rows — a claimed row is still an
 *    undrained row, so the guard refuses that path for the whole time it
 *    is somebody's work;
 *  - so what remains is a queue INSERT racing a registration of the same
 *    path. That is per-inserter, and THERE ARE FOUR INSERTERS WITH THREE
 *    DIFFERENT SERIALIZERS. They are listed here because the previous
 *    version of this argument said "every queue insert happens under the
 *    commit lock", and two of the four do not:
 *
 *      1. `ExpiryService` and `RetirementService` queue paths whose file
 *         rows they have just deleted, inside a transaction that holds
 *         the per-catalog COMMIT LOCK. A commit cannot register those
 *         paths in that window, and cannot reuse them afterwards: no
 *         writer in this system reuses a path (the Python writer, the
 *         Trino connector and compaction all mint fresh names).
 *      2. `CompactionService.stageOutputPath`'s INITIAL stage takes NO
 *         lock — it is an autocommit INSERT before the rewrite starts.
 *         It is safe for a different reason: the path is a UUID minted
 *         by that sweep and known to nobody else, so no concurrent
 *         commit can be registering it. (The RE-stage on the
 *         `SkippedConflict` path does run under the commit lock, because
 *         it is inside `commitGroup`'s transaction.)
 *      3. `UploadService.fenceAndQueue` takes NO commit lock either, and
 *         deliberately — its own comment says taking one would convoy
 *         the catalog. It is serialized against a concurrent
 *         registration by the `hog_upload` ROW LOCK:
 *         `UploadService.register` takes `SELECT ... FOR UPDATE` on the
 *         claim rows inside the commit transaction and raises
 *         `CommitConflict` unless the claim is still `'active'`, while
 *         the sweep's own fence re-evaluates its candidate predicate
 *         after waiting on that same row lock. THIS is the arm the
 *         removed commit lock was actually standing in for.
 *
 *    The standing constraint that falls out of (3), and it is an
 *    invariant rather than a coincidence: A COMMIT THAT REGISTERS A PATH
 *    WITH NO `hog_upload` CLAIM HAS NO SERIALIZER AGAINST A CONCURRENT
 *    QUEUE INSERT FOR THAT PATH. Today that is unreachable because paths
 *    are never reused and every externally-supplied path arrives through
 *    an upload claim; a future path that is neither must bring its own
 *    serializer.
 *
 * What the lock cost, measured on gigahog-prod-us 2026-09-24: ~19 s of
 * held lock per 1,000-row sub-batch, during which every commit on the
 * catalog waits. Cleanup has been OFF in production for it; as of
 * 2026-09-29 the queue stands at ~2.6M undrained rows growing ~190k/h,
 * about 9.4k of them orphaned `compaction_staging` tickets past their
 * grace. The ONE genuine two-writer case on one ROW —
 * compaction's staging ticket, which is legitimately both a deletion
 * ticket here and a row to settle `'registered'` in the group's commit —
 * is serialized by POSTGRES ROW LOCKING, which is all it ever needed:
 * `CompactionService.commitGroup` re-reads the ticket `FOR UPDATE` and
 * refuses it unless it is UNTOUCHED (`drained_at IS NULL AND claimed_at
 * IS NULL AND attempts = 0`), re-staging the path and returning
 * `SkippedConflict` when it loses. It refuses a LAPSED claim too, and
 * that is the point: a lapsed claim does not mean the object survived, it
 * means nobody knows, and the row lock cannot stand between a DELETE no
 * transaction holds and a registration.
 *
 * THE THREE STEPS, and the transaction boundaries are the design:
 *
 *  1. CLAIM ([CLAIM_BULK_SQL] / [CLAIM_STAGING_SQL]) — one short
 *     transaction per sub-batch, one statement per REASON:
 *     `UPDATE ... WHERE removal_id IN (SELECT ... FOR UPDATE SKIP
 *     LOCKED) RETURNING`, stamping `claimed_at`/`claimed_by`. It commits
 *     BEFORE any object-store call, so nothing is open while a worker
 *     waits on S3 and `idle_in_transaction_session_timeout` no longer
 *     bounds this class at all. `SKIP LOCKED` is what lets [workers]
 *     workers and any number of replicas partition the queue with no
 *     coordination, which is why there is no single-flight advisory lock
 *     either.
 *  2. WORK — the reference check ([referencedPaths], unchanged SQL, now
 *     blocking nobody) and then the deletes. No transaction, no lock.
 *  3. SETTLE ([SETTLE_SQL]) — one short transaction, FENCED on
 *     `drained_at IS NULL AND claimed_by = :worker`. Rows it does not
 *     return were settled by another writer (a compaction group
 *     registering the path) and are counted `settled_elsewhere`.
 *
 * A CLAIM IS A LEASE, NOT A LOCK. A worker killed between its claim and
 * its settle leaves rows claimed; after [claimLeaseSeconds] (900 s) any
 * worker may reclaim them. `DeleteObjects` is idempotent, so a re-drain
 * of an already-deleted object settles exactly as the first attempt
 * would have. The fence is what makes the lapse safe in the other
 * direction: the lapsed worker's own settle and attempts bump match no
 * row, so it can never stamp its outcome over the writer that took the
 * row from it.
 *
 * STEP 2'S BULK DELETE IS ONE `DeleteObjects` CALL PER BUCKET CHUNK, not
 * a HEAD and a DELETE per path. S3 caps that call at 1,000 keys, which
 * is where [subBatchSize]'s default comes from; a sub-batch whose paths
 * span several buckets, or exceed the key ceiling, is that many calls. A
 * key that was never there is reported exactly like one that was removed
 * — deleting a missing key IS the end state the queue asks for — so
 * those rows settle 'deleted' and `missing` is no longer produced for
 * them.
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
 * BECAUSE THEY COST TWO ROUND TRIPS EACH, STAGING TICKETS ARE CLAIMED
 * [STAGING_SUB_BATCH] AT A TIME rather than [subBatchSize], as their own
 * claim statement. THE CLAIM IS THE UNIT THE LEASE BOUNDS, which is why
 * the reason predicate is in the SQL and not in the code that partitions
 * what came back: one claim with no reason predicate could hold 1,000
 * tickets — 2,000 HEAD/DELETE round trips under a single lease, ~128 s at
 * the 64 ms measured on gigahog-prod-us but 20,000 s if every call takes
 * a whole `RemovalStore.apiCallTimeout`, which is 22x the lease. Claimed
 * at 25 the worst case is 50 calls, 500 s, inside the lease. Each claim
 * is then exactly one sub-batch, and a sub-batch settles in its own
 * transaction, so a claim that ends badly never loses what earlier ones
 * did.
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
 * THE DATABASE HALF IS INDEXED, AND IT WAS NOT. The reference check
 * ([referencedPaths]) probes three tables by `(catalog_id, path)`;
 * hog_upload has carried that key since V12, and hog_data_file and
 * hog_delete_file got it in V17. Before V17 both of those legs read
 * EVERY file row the catalog has ever registered — live and historical —
 * to answer a 1,000-path question: 190,884 buffers and 692 ms on a
 * 5M-row fixture, against 8,728 and 33 ms after. That statement is now
 * the drain's rate, and nothing waits on it.
 */
class CleanupService(
    private val jdbi: Jdbi,
    private val store: RemovalStore,
    private val subBatchSize: Int = SUB_BATCH,
    private val ledgerRetentionSeconds: Long = LEDGER_RETENTION_SECONDS,
    private val maintenanceLedgerRetentionSeconds: Long = MAINTENANCE_LEDGER_RETENTION_SECONDS,
    private val stagingGraceSeconds: Long = STAGING_GRACE_SECONDS,
    /**
     * How long a claim is honoured before any worker may reclaim the row
     * (HOGLAKE_CLEANUP_CLAIM_LEASE_SECONDS, 900 s).
     *
     * ONE VALUE, SHARED WITH COMPACTION: `CompactionConfig`'s copy
     * decides when a group commit treats a claimed ticket as reclaimable,
     * and two different numbers would give the two services different
     * answers about the same row. App.kt passes `cfg` to both.
     */
    private val claimLeaseSeconds: Long = CLAIM_LEASE_SECONDS,
    /**
     * Parallel workers per run (HOGLAKE_CLEANUP_WORKERS, 1).
     *
     * Each worker runs its own claim -> work -> settle loop over its own
     * sub-batches until the run's `batchSize` is consumed or the queue is
     * empty for this catalog. They need no coordination: `SKIP LOCKED` in
     * the claim partitions the queue between them, and between replicas,
     * for free.
     *
     * A worker holds a pooled connection for the claim, the reference
     * check and the settle and NEVER across an object-store call, so N
     * workers do not need N permanent pool slots — see Config's
     * `cleanupWorkers` for the boot check that still keeps them clear of
     * the foreground's reserve.
     */
    private val workers: Int = 1,
    /**
     * Whether this process's cleanup LOOP is registered
     * (`HOGLAKE_CLEANUP_INTERVAL_MS > 0`).
     *
     * It exists to CLAMP [workers] on a pod whose loop is off. The
     * manual `POST /v1/maintenance/cleanup` runs this same drain on
     * whichever pod serves it, so without the clamp a pod that boots with
     * `HOGLAKE_CLEANUP_WORKERS=4` and no loop — a configuration `Config`'s
     * pool refusal deliberately prices at ONE worker, because that is all
     * a loop-off pod can spend — would take four connections the moment
     * somebody curled the endpoint, which is the failure that refusal
     * exists to prevent. With the loop on, the configured count is what
     * the refusal priced and what runs.
     *
     * DERIVED IN ONE PLACE — the `Config` secondary constructor below,
     * which is what `App.kt` uses — because the predicate has to be the
     * one the pool refusal prices on. The default of `true` is for the
     * tests that construct a service by hand.
     */
    private val loopEnabled: Boolean = true,
    /**
     * The identity the claim fence is written against: pod name plus a
     * per-process UUID, with the worker index appended per worker.
     *
     * Per PROCESS, not per catalog: it only has to be unique among
     * everything that might claim the same row, and a restarted pod must
     * NOT inherit its predecessor's id — that is what makes the lease
     * the only way a dead worker's rows come back. Injectable so a test
     * can read the value out of a statement's bindings instead of
     * reverse-engineering a UUID.
     */
    private val workerIdPrefix: String = defaultWorkerIdPrefix(),
    /**
     * Ids the drained-ledger purge reads per page (see
     * [purgeDrainedLedger]). Constructor-tunable so a test can make the
     * WALK observable — several pages and a page that purges nothing —
     * without seeding a production-sized ledger.
     */
    private val ledgerPurgePage: Int = LEDGER_PURGE_PAGE,
    /**
     * Wall clock the drained-ledger purge may spend per run (see
     * [LEDGER_PURGE_BUDGET_MS]). Constructor-tunable so a test can make
     * the budget's stop observable — 0 stops the walk before its first
     * page — rather than sleeping a second.
     */
    private val ledgerPurgeBudgetMs: Long = LEDGER_PURGE_BUDGET_MS,
    /**
     * How long a commit receipt is kept (HOGLAKE_RECEIPT_RETENTION_SECONDS,
     * 7 days); `<= 0` purges nothing, which is V7's keep-forever
     * behaviour. See [purgeCommitReceipts] and `Config`'s knob for why
     * the number is a correctness floor and not only a size one.
     */
    private val receiptRetentionSeconds: Long = RECEIPT_RETENTION_SECONDS,
    /**
     * Receipts the purge deletes per page ([RECEIPT_PURGE_PAGE]).
     * Constructor-tunable so a test can make the WALK observable —
     * several pages, and a run the budget stops — without seeding a
     * production-sized receipt table.
     */
    private val receiptPurgePage: Int = RECEIPT_PURGE_PAGE,
    /**
     * Wall clock the receipt purge may spend per run
     * ([RECEIPT_PURGE_BUDGET_MS]). Constructor-tunable for the same
     * reason as [ledgerPurgeBudgetMs]: 0 stops the walk before its first
     * page, which makes the budget's stop observable without sleeping.
     */
    private val receiptPurgeBudgetMs: Long = RECEIPT_PURGE_BUDGET_MS,
) {
    /**
     * The production wiring, and the ONLY place the service's knobs are
     * derived from [Config].
     *
     * It exists because one of those derivations is load-bearing and used
     * to be stated in `App.kt`, where nothing could test it:
     * `loopEnabled = cleanupIntervalMs > 0` is the predicate `Config`'s
     * pool refusal prices on, and if the two ever disagreed — a later
     * edit writing `loopEnabled = true` for convenience — the refusal's
     * argument would stop holding in the deployed binary with every test
     * still green, because [loopEnabled] has a default and no test
     * constructed the app's wiring. Here the expression is inside the
     * class, where `the production wiring clamps the manual path when the
     * loop is off` builds it directly, and `App.kt` has no way to state
     * it differently.
     */
    constructor(
        jdbi: Jdbi,
        store: RemovalStore,
        config: Config,
    ) : this(
        jdbi = jdbi,
        store = store,
        subBatchSize = config.cleanupSubBatchSize,
        ledgerRetentionSeconds = config.removalLedgerRetentionSeconds,
        maintenanceLedgerRetentionSeconds = config.maintenanceLedgerRetentionSeconds,
        stagingGraceSeconds = config.cleanupStagingGraceSeconds,
        claimLeaseSeconds = config.cleanupClaimLeaseSeconds,
        workers = config.cleanupWorkers,
        loopEnabled = config.cleanupIntervalMs > 0,
        receiptRetentionSeconds = config.receiptRetentionSeconds,
    )

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
        // A lease of 0 makes every claim immediately reclaimable, so two
        // workers drain one row by construction — and a negative one
        // pushes the reclaim horizon into the future, so a row is
        // claimable BEFORE it is claimed. Config refuses both at boot;
        // this is the same refusal for a service built by hand.
        require(claimLeaseSeconds > 0) {
            "claimLeaseSeconds must be positive (got $claimLeaseSeconds): a lease of 0 makes " +
                "every claim immediately reclaimable"
        }
        require(workers >= 1) { "workers must be at least 1 (got $workers)" }
        require(ledgerPurgePage > 0) { "ledgerPurgePage must be positive (got $ledgerPurgePage)" }
        // 0 is legal and means "do not purge this run", which is what a
        // test uses to make the budget's stop observable without sleeping.
        require(ledgerPurgeBudgetMs >= 0) {
            "ledgerPurgeBudgetMs must not be negative (got $ledgerPurgeBudgetMs)"
        }
        require(receiptPurgePage > 0) { "receiptPurgePage must be positive (got $receiptPurgePage)" }
        // 0 is legal for the same reason it is on the ledger budget: a
        // test uses it to stop the walk before its first page.
        require(receiptPurgeBudgetMs >= 0) {
            "receiptPurgeBudgetMs must not be negative (got $receiptPurgeBudgetMs)"
        }
    }

    /**
     * What one catalog's receipt purge did: rows deleted, and pages that
     * threw.
     *
     * TWO NUMBERS BECAUSE ZERO IS AMBIGUOUS WITH ONE. `purged = 0` alone
     * describes "nothing was eligible", "the run budget expired first"
     * and "every page timed out" identically, and the third is the one
     * that matters — it is the state the 58 GiB legacy backlog fails in.
     * See [purgeCommitReceipts].
     */
    private data class ReceiptPurge(val purged: Long = 0, val failures: Long = 0)

    private data class Entry(val removalId: Long, val path: String, val reason: String)

    /** A per-path audit event, collected in a sub-batch and emitted after it settles. */
    private data class PathEvent(val action: String, val path: String, val outcome: String, val detail: String?)

    /**
     * Drain up to [batchSize] queue entries for [catalog]. The summary
     * audit event and the files-removed counter are emitted at the end
     * of the run (each sub-batch's claim and settle are their own
     * transactions; nothing is emitted inside one); per-path events
     * (file_deleted / cleanup_violation) are emitted as the drain
     * progresses, bounded by the batch size. still_referenced > 0 is an
     * invariant violation and flags the run's audit outcome accordingly.
     * A ZERO-WORK drain (nothing removed, missing, or violated) emits no
     * audit event — app-log debug only, so idle background loops stay out
     * of the audit stream.
     */
    fun runOnce(
        catalog: String,
        batchSize: Int,
        trigger: MaintenanceTrigger = MaintenanceTrigger.MANUAL,
    ): CleanupResult =
        drainCatalog(catalog, batchSize, trigger, receiptPurgeDeadline()).also {
            // ONCE PER RUN, after the drain: the retention purge is
            // instance-wide, so a fan-out must not pay for it per catalog
            // (see [runOnceAllCatalogs], which is why this is not inside
            // `doRunOnce`).
            purgeAfterRun("manual run for '$catalog'")
        }

    /** One catalog's drain and its run-ledger row, with no instance-wide work. */
    private fun drainCatalog(
        catalog: String,
        batchSize: Int,
        trigger: MaintenanceTrigger,
        /**
         * When the RUN's receipt-purge budget expires — one value for the
         * whole sweep, not one per catalog. See [receiptPurgeDeadline].
         */
        receiptDeadline: Long,
    ): CleanupResult =
        runStore.recorded(catalog, MaintenanceTask.CLEANUP, trigger) {
            runDrain(catalog, batchSize, receiptDeadline)
        }

    private fun runDrain(
        catalog: String,
        batchSize: Int,
        receiptDeadline: Long,
    ): CleanupResult {
        val result =
            try {
                doRunOnce(catalog, batchSize, receiptDeadline)
            } catch (e: Throwable) {
                Audit.event("cleanup", catalog, null, Audit.failureOutcome(e), e.message)
                throw e
            }
        // objectsRemoved, not removed: the metric is documented as
        // physical deletes and `removed` counts ledger rows.
        Metrics.filesRemoved(catalog, result.objectsRemoved)
        if (result.removed == 0L && result.missing == 0L && result.stillReferenced == 0L) {
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
        receiptDeadline: Long,
    ): CleanupResult {
        if (batchSize <= 0) {
            throw HoglakeException.Validation("batch size must be positive (got $batchSize)")
        }
        // The whole row, not just the id: the receipt purge's cutoff has a
        // floor derived from this catalog's snapshot retention (see
        // [purgeCommitReceipts]), and reading it here costs nothing on top
        // of the lookup the drain already does.
        val catalogInfo =
            jdbi.withHandleUnchecked { h ->
                CatalogRepo.require(h, catalog)
            }
        val catalogId = catalogInfo.catalogId
        // What this run has already handled. A still-referenced row (and
        // a row whose delete failed) has its claim RELEASED so the next
        // run, and an operator reading the queue, can see it — which also
        // makes it claimable again inside THIS run, and a second pass
        // would bump `attempts` twice and count one violation twice. A
        // row this set already holds is released immediately rather than
        // left claimed, because a claim this run will not act on is a
        // lease of invisibility for nothing.
        val handled = ConcurrentHashMap.newKeySet<Long>()

        val tallies =
            if (effectiveWorkers == 1) {
                listOf(drainWorker(catalog, catalogId, workerId(0), batchSize, handled))
            } else {
                val pool = Executors.newFixedThreadPool(effectiveWorkers)
                try {
                    pool.invokeAll(
                        (0 until effectiveWorkers).map { i ->
                            java.util.concurrent.Callable {
                                drainWorker(catalog, catalogId, workerId(i), batchSize, handled)
                            }
                        },
                    ).map { future ->
                        try {
                            future.get()
                        } catch (e: java.util.concurrent.ExecutionException) {
                            // THE CAUSE, NOT THE WRAPPER. A worker's
                            // failure has to reach the run ledger's
                            // `error`, the audit event's detail and the
                            // manual POST's status as itself — otherwise
                            // the same object-store fault is reported as
                            // an `IllegalStateException` at one worker and
                            // an `ExecutionException` at two, and which
                            // one an operator sees depends on a knob.
                            throw e.cause ?: e
                        }
                    }
                } finally {
                    pool.shutdown()
                }
            }
        // The DRAINED-LEDGER purge is not here: it is instance-wide (it
        // walks the removal table's primary key, not one catalog's rows),
        // so it runs ONCE PER RUN rather than once per catalog — see
        // [runOnce] and [runOnceAllCatalogs]. The maintenance-run ledger's
        // purge IS per catalog, and stays.
        purgeMaintenanceLedger(catalog, catalogId)
        // THE RECEIPT PURGE IS PER CATALOG, like the run-ledger purge
        // above and unlike the drained-ledger one: V24's index leads on
        // `catalog_id`, and the cutoff's floor is derived from THIS
        // catalog's snapshot retention, so there is nothing instance-wide
        // to amortize.
        //
        // FENCED, because retention is hygiene and the drain's report is
        // not: a purge that throws is a WARN and a zero on the wire, and
        // the drain it followed still says what it did. The next sweep
        // starts again from the oldest eligible receipt, so nothing is
        // lost but one interval. (The WARN is the only trace; nothing
        // counts it.)
        val receipts =
            try {
                purgeCommitReceipts(catalog, catalogId, catalogInfo.snapshotRetentionSeconds, receiptDeadline)
            } catch (e: Exception) {
                // Anything the per-page fence did not already catch: the
                // catalog lookup, a pool failure, a bug. Counted as one
                // failure for the same reason the pages are — a zero that
                // is not "nothing eligible" has to be distinguishable.
                log.warn(e) {
                    "cleanup: the commit-receipt retention purge failed for catalog '$catalog'; " +
                        "the drain itself is unaffected and the next run retries from the oldest " +
                        "eligible receipt"
                }
                ReceiptPurge(failures = 1)
            }
        Metrics.commitReceiptPurgeFailures(catalog, receipts.failures)
        val total = tallies.fold(Tally()) { a, b -> a + b }
        return CleanupResult(
            total.removed,
            total.missing,
            total.stillReferenced,
            total.objectsRemoved,
            total.settledElsewhere,
            // Always 0. The counter belongs to the hold budget that
            // bounded a lock this class no longer takes; it stays on the
            // wire because the maintenance ledger holds rows that carry
            // it and every client already reads it (see
            // CleanupResult.deadlineSkipped).
            deadlineSkipped = 0,
            receiptsPurged = receipts.purged,
            receiptsPurgeFailures = receipts.failures,
        )
    }

    /** One worker's, or one sub-batch's, tally. Summed, never reported alone. */
    private data class Tally(
        val removed: Long = 0,
        val missing: Long = 0,
        val stillReferenced: Long = 0,
        val objectsRemoved: Long = 0,
        val settledElsewhere: Long = 0,
    ) {
        operator fun plus(o: Tally) =
            Tally(
                removed + o.removed,
                missing + o.missing,
                stillReferenced + o.stillReferenced,
                objectsRemoved + o.objectsRemoved,
                settledElsewhere + o.settledElsewhere,
            )
    }

    /**
     * The workers a run actually starts: [workers] while the loop is
     * registered, and ONE otherwise.
     *
     * A loop-off pod still serves `POST /v1/maintenance/cleanup`, and
     * `Config`'s pool refusal prices exactly one worker for it (it cannot
     * price more without refusing every API pod in the fleet). The clamp
     * is what makes that price true: a hand-triggered drain on a pod whose
     * loop is disabled can never take more of the pool than was reserved
     * for it, however `HOGLAKE_CLEANUP_WORKERS` is set.
     */
    private val effectiveWorkers: Int = if (loopEnabled) workers else 1

    /** `pod/uuid#n` — see [workerIdPrefix] for why it is per process. */
    private fun workerId(index: Int): String = "$workerIdPrefix#$index"

    /**
     * ONE worker: claim, work, settle, repeat, until its own budget is
     * spent or the queue is empty for this catalog.
     *
     * THE BUDGET IS PER WORKER AND PER REASON, and that is the knob's
     * whole arithmetic: a run asks for at most `workers x 2 x batchSize`
     * rows — `batchSize` of bulk and `batchSize` of staging tickets per
     * worker — so HOGLAKE_CLEANUP_WORKERS multiplies the drain rate
     * instead of dividing one shared budget (which is what a shared
     * counter did — four workers at a 2,000-row batch and a 1,000-row
     * sub-batch left two of them with nothing to claim), and the bulk
     * backlog cannot starve the staging tickets (see the loop below). See
     * Config's cleanup knobs for the rows/hour this works out to at a
     * given interval.
     *
     * TWO CLAIMS PER ITERATION, one per REASON, because the lease has to
     * bound the WORK A CLAIM CARRIES and the two kinds cost different
     * numbers of round trips: a bulk claim is [subBatchSize] rows and one
     * `DeleteObjects` call per bucket chunk, a staging claim is
     * [STAGING_SUB_BATCH] rows and two round trips EACH. Claiming them
     * together — one statement with no reason predicate — let one claim
     * hold 1,000 tickets, which is 2,000 round trips under a single
     * lease: 128 s at the measured 64 ms, but 20,000 s at the call bound,
     * 22x the lease. Each claim is now exactly one sub-batch, so the
     * lease bounds what it has to.
     *
     * A SHORT CLAIM ENDS THAT ARM FOR THIS ITERATION, and when neither
     * arm filled a claim the worker stops. Rows are missing from a claim for exactly two reasons: the
     * queue is empty for this catalog and this reason, or a concurrent
     * worker's claim holds them under `SKIP LOCKED` — and that worker
     * claims and works them, since its UPDATE is what locked them. So a
     * worker that stops leaves behind only what another worker has
     * already taken. What keeps a row a still-referenced skip RELEASED
     * from being handled twice inside one run is `handled`, not this
     * condition.
     */
    private fun drainWorker(
        catalog: String,
        catalogId: Long,
        worker: String,
        /** THIS worker's budget, PER REASON, not the run's: see the KDoc above. */
        batchSize: Int,
        handled: MutableSet<Long>,
    ): Tally {
        var tally = Tally()
        // ONE BUDGET PER ARM, and it is not tidiness. Shared, the bulk arm
        // spends it first — it claims [subBatchSize] rows to staging's 25 —
        // so on a catalog whose bulk queue is bigger than the batch, a
        // staging ticket got at most 25 per worker per run, and with
        // `batchSize <= subBatchSize` (a natural setting once the interval
        // drops to seconds) it got NONE: the bulk arm consumed the budget
        // in the first iteration and the staging `want` was 0 for ever
        // after. On gigahog-prod-us that is ~9.4k orphaned tickets at 50/h
        // — eight days, with `/verify`'s `staging_tickets.leaked` arm
        // firing for all of it, on a drain working exactly as designed.
        // Per arm, the same queue clears in ~5 runs.
        val budgets = mutableMapOf(false to batchSize, true to batchSize)
        while (budgets.values.any { it > 0 }) {
            var progressed = false
            for (staging in listOf(false, true)) {
                val budget = budgets.getValue(staging)
                val want = minOf(budget, if (staging) STAGING_SUB_BATCH else subBatchSize)
                if (want <= 0) continue
                val claimed = claim(catalogId, worker, want, staging)
                if (claimed.isEmpty()) {
                    // This arm is empty for this catalog. Zero its budget
                    // rather than asking again every iteration: the other
                    // arm can still have 80 iterations left in it, and
                    // each one would spend a round trip re-discovering
                    // that this one has nothing.
                    budgets[staging] = 0
                    continue
                }
                budgets[staging] = budget - claimed.size
                if (claimed.size == want) progressed = true
                val (fresh, again) = claimed.partition { handled.add(it.removalId) }
                if (again.isNotEmpty()) {
                    releaseClaims(catalogId, worker, again.map { it.removalId })
                    log.debug {
                        "cleanup: ${again.size} rows this run already handled were claimed again " +
                            "(a still-referenced skip releases its claim); released without a " +
                            "second touch"
                    }
                }
                if (fresh.isNotEmpty()) {
                    tally += drainSubBatch(catalog, catalogId, worker, fresh, probeEachPath = staging)
                }
            }
            // Neither kind filled a claim: the queue is empty for this
            // worker and there is nothing left to ask for.
            if (!progressed) return tally
        }
        return tally
    }

    /**
     * Claim up to [want] rows of ONE reason for [worker]: ONE statement,
     * its own transaction, committed before anything touches an object
     * store.
     *
     * No explicit transaction is opened, so this is autocommit — one
     * statement is one transaction, which is the shortest form the claim
     * can take and the one a test can assert cannot span an S3 call.
     *
     * The returned order is the UPDATE's, not the inner select's
     * (`RETURNING` does not carry an `ORDER BY`), and nothing depends on
     * it: one claim is one sub-batch, so there is no partition of the
     * claim for an order to decide.
     */
    private fun claim(
        catalogId: Long,
        worker: String,
        want: Int,
        staging: Boolean,
    ): List<Entry> =
        jdbi.withHandleUnchecked { h ->
            h.createQuery(if (staging) CLAIM_STAGING_SQL else CLAIM_BULK_SQL)
                .bind("catalogId", catalogId)
                .bind("worker", worker)
                .bind("limit", want)
                .bind("leaseSeconds", claimLeaseSeconds.toDouble())
                .apply { if (staging) bind("stagingGraceSeconds", stagingGraceSeconds.toDouble()) }
                .map { rs, _ ->
                    Entry(rs.getLong("removal_id"), rs.getString("path"), rs.getString("reason"))
                }
                .list()
        }

    private fun releaseClaims(
        catalogId: Long,
        worker: String,
        ids: List<Long>,
    ) {
        jdbi.withHandleUnchecked { h ->
            h.createUpdate(RELEASE_CLAIM_SQL)
                .bind("catalogId", catalogId)
                .bind("worker", worker)
                .bindArray("ids", Long::class.javaObjectType, ids)
                .execute()
        }
    }

    /**
     * ONE sub-batch of a worker's claim: reference check, deletes, one
     * settle transaction, one flush of audit events after it commits.
     *
     * [probeEachPath] is what separates the two shapes. False (the bulk
     * path) deletes with [RemovalStore.deleteBatch] — one call per bucket
     * chunk — and settles everything it did not fail as `'deleted'`.
     * True (the `compaction_staging` path) does HEAD + DELETE per path so
     * `'absent'` still reaches the ledger for `/verify`, and is called
     * with at most [STAGING_SUB_BATCH] rows for exactly that reason.
     *
     * Nothing is accumulated into the run's counters until the settle
     * transaction has committed: a settle that rolls back must contribute
     * nothing, including its violations.
     */
    private fun drainSubBatch(
        catalog: String,
        catalogId: Long,
        worker: String,
        sub: List<Entry>,
        probeEachPath: Boolean,
    ): Tally {
        // Liveness is checked at drain time, on the freshest answer the
        // database can give — and it now blocks nobody. The claim already
        // committed, so this runs on a connection with no transaction of
        // its own and no lock of any kind.
        val referenced = referencedPaths(catalogId, sub.map { it.path })
        val violations = mutableListOf<Entry>()
        val survivors = mutableListOf<Entry>()
        for (entry in sub) {
            if (entry.path in referenced) violations += entry else survivors += entry
        }

        // Ledger outcomes for this sub-batch: settled entries by outcome,
        // plus the ones that stay queued (attempts bump).
        val drainedByOutcome = mapOf("deleted" to mutableListOf<Long>(), "absent" to mutableListOf())
        val failed = mutableListOf<Entry>()
        // Paths whose object this sub-batch physically removed. A SET
        // because two queue rows over one path are legitimate state
        // (V16's non-unique argument) and one object went: this is what
        // the physical-delete metric and the audit events count.
        val removedPaths = LinkedHashSet<String>()

        if (probeEachPath) {
            // Staging tickets: HEAD before DELETE, one path at a time.
            for (entry in survivors) {
                try {
                    when (store.deleteIfExists(entry.path)) {
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
                    // Leave the row queued (attempts bumped, claim
                    // released); the next run retries it.
                    log.error(e) {
                        "cleanup: delete failed for '${entry.path}' " +
                            "(removal_id ${entry.removalId}); leaving queued"
                    }
                    failed += entry
                }
            }
        } else if (survivors.isNotEmpty()) {
            // Everything else: one DeleteObjects call per bucket chunk.
            // A key that was not there comes back indistinguishable from
            // one that was removed, and that is the end state the queue
            // asked for, so the row settles 'deleted' either way.
            val rowsByPath = survivors.groupBy { it.path }
            val failures = store.deleteBatch(rowsByPath.keys)
            for ((path, rows) in rowsByPath) {
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
                    failed += rows
                }
            }
        }

        var tally = Tally(objectsRemoved = removedPaths.size.toLong())
        val events = mutableListOf<PathEvent>()
        // Settles this sub-batch lost to a 'registered' over a path it had
        // already deleted — B1's signature, counted as an invariant
        // violation rather than as ordinary contention.
        var registeredAfterDelete = 0L
        // ONE transaction for every ledger write this sub-batch makes, so
        // its accounting is atomic: either the settles, the bumps and the
        // releases all landed or none did.
        jdbi.useTransactionUnchecked { h ->
            var removed = 0L
            var missing = 0L
            var settledElsewhere = 0L
            // Soft-delete the settled entries: the row survives as the
            // queryable ledger of what cleanup did and when.
            for ((outcome, ids) in drainedByOutcome) {
                if (ids.isEmpty()) continue
                val settled =
                    h.createQuery(SETTLE_SQL)
                        .bind("outcome", outcome)
                        .bind("catalogId", catalogId)
                        .bind("worker", worker)
                        .bindArray("ids", Long::class.javaObjectType, ids)
                        .mapTo(Long::class.javaObjectType)
                        .list()
                val missed = ids - settled.toSet()
                if (missed.isNotEmpty()) {
                    // WHY THE SETTLE MISSED IS NOT A DETAIL, and the fence
                    // cannot say: `drained_at IS NULL AND claimed_by =
                    // :worker` refuses two events with opposite meanings.
                    // One is a compaction group registering the path
                    // (normal). The other is A LIVE FILE ROW FOR A PATH
                    // THIS SUB-BATCH JUST DELETED, which is an invariant
                    // violation and the ONLY observable signature it has —
                    // nothing else in the system can see it, because
                    // /verify's staging_tickets arms all pass once the file
                    // row exists. So the missed ids are read back and
                    // classified.
                    //
                    // It is unreachable as of this change: compaction's
                    // re-read refuses any ticket cleanup has TOUCHED
                    // (claimed or attempted), not merely one whose lease
                    // is live. This is the detector for the day that stops
                    // being true.
                    for (row in missedRows(h, catalogId, missed)) {
                        val deletedHere = row.path in removedPaths
                        if (row.drainedOutcome == "registered" && deletedHere) {
                            log.error {
                                "cleanup: removal_id ${row.removalId} path '${row.path}' was " +
                                    "settled 'registered' by a compaction group AFTER this " +
                                    "sub-batch deleted the object — a live file row now points " +
                                    "at a deleted object; invariant violation"
                            }
                            events +=
                                PathEvent(
                                    "cleanup_violation",
                                    row.path,
                                    "invariant_violation",
                                    "removal_id=${row.removalId} registered after this drain " +
                                        "deleted the object",
                                )
                            registeredAfterDelete++
                        } else if (row.claimedBy != null && row.claimedBy != worker) {
                            log.warn {
                                "cleanup: removal_id ${row.removalId} is now claimed by " +
                                    "'${row.claimedBy}' rather than '$worker' — this worker's " +
                                    "lease lapsed while it was working; the new claimant settles it"
                            }
                        } else {
                            log.warn {
                                "cleanup: removal_id ${row.removalId} was settled by another " +
                                    "writer ('${row.drainedOutcome}') before this sub-batch " +
                                    "could; leaving it alone and not counting it as '$outcome'"
                            }
                        }
                    }
                }
                settledElsewhere += missed.size
                when (outcome) {
                    "deleted" -> removed = settled.size.toLong()
                    "absent" -> missing = settled.size.toLong()
                }
            }
            // THE FENCE DECIDES WHAT WAS A VIOLATION, and it has to,
            // because the reference check is not authoritative about
            // WHOSE row it read. A compaction group that committed
            // between this worker's claim and its check makes the path a
            // live file and settles the ticket 'registered' in one
            // transaction — a group committing normally. Counting the
            // check's hit would page someone for it. So the bump is
            // asked first and only the rows it actually touched are
            // violations; the rest were settled elsewhere.
            val trueViolations = bumpAttempts(h, catalogId, worker, violations)
            tally = tally.copy(stillReferenced = trueViolations.size.toLong())
            settledElsewhere += violations.size - trueViolations.size
            for (entry in trueViolations) {
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
            }
            settledElsewhere += failed.size - bumpAttempts(h, catalogId, worker, failed).size
            tally =
                tally.copy(
                    removed = removed,
                    missing = missing,
                    // A settle lost over a path this sub-batch deleted is
                    // an invariant violation, not contention: it flags the
                    // run `invariant_violation` exactly as a
                    // still-referenced path does.
                    stillReferenced = tally.stillReferenced + registeredAfterDelete,
                    settledElsewhere = settledElsewhere,
                )
        }
        // Physical deletions are the audit log's whole point: one event
        // per object actually removed.
        for (path in removedPaths) {
            events += PathEvent("file_deleted", path, "ok", null)
        }
        // Settled (no transaction open): emit this sub-batch's events.
        for (e in events) {
            Audit.event(e.action, catalog, e.path, outcome = e.outcome, detail = e.detail)
        }
        return tally
    }

    /** A row a settle did not match, read back so the miss can be classified. */
    private data class MissedRow(
        val removalId: Long,
        val path: String,
        val drainedOutcome: String?,
        val claimedBy: String?,
    )

    /**
     * Read back the rows a settle did not match, in the settle's own
     * transaction. Four columns, because the classification needs all of
     * them: which row, which path (to compare against what this
     * sub-batch deleted), what outcome another writer recorded, and
     * whether the claim moved.
     */
    private fun missedRows(
        h: Handle,
        catalogId: Long,
        ids: List<Long>,
    ): List<MissedRow> =
        h.createQuery(MISSED_SETTLE_SQL)
            .bind("catalogId", catalogId)
            .bindArray("ids", Long::class.javaObjectType, ids)
            .map { rs, _ ->
                MissedRow(
                    rs.getLong("removal_id"),
                    rs.getString("path"),
                    rs.getString("drained_outcome"),
                    rs.getString("claimed_by"),
                )
            }
            .list()

    /**
     * Bump `attempts` on rows that were touched and NOT drained — a
     * still-referenced skip, or a delete that failed — releasing the
     * claim with it, and return the entries the fence actually matched.
     *
     * The RETURNING is the point: a row this worker no longer holds (its
     * lease lapsed, or a compaction group settled it 'registered') must
     * not be counted, logged or audited as anything, and the only way to
     * know is to let the database say which rows the statement touched.
     */
    private fun bumpAttempts(
        h: Handle,
        catalogId: Long,
        worker: String,
        entries: List<Entry>,
    ): List<Entry> {
        if (entries.isEmpty()) return emptyList()
        val bumped =
            h.createQuery(BUMP_ATTEMPTS_SQL)
                .bind("catalogId", catalogId)
                .bind("worker", worker)
                .bindArray("ids", Long::class.javaObjectType, entries.map { it.removalId })
                .mapTo(Long::class.javaObjectType)
                .list()
                .toSet()
        return entries.filter { it.removalId in bumped }
    }

    /**
     * The retention purge as a run's LAST act, and never as its verdict.
     *
     * It sits OUTSIDE `MaintenanceRunStore.recorded` because it is
     * instance-wide rather than a catalog's task, and that placement has a
     * consequence worth fencing: unfenced, a purge failure would escape as
     * the manual endpoint's 500 or as a counted background-loop failure
     * while every ledger row for the drain it followed said `ok` — a run
     * reported two ways at once. Retention is hygiene: it is allowed to
     * fail, it is not allowed to misreport the drain, and the next run
     * starts again from the bottom of the key.
     *
     * WARN rather than debug because it is the only trace: nothing counts
     * it, and a purge that has been failing for a week is a ledger growing
     * without bound.
     */
    private fun purgeAfterRun(label: String) {
        try {
            purgeDrainedLedger(label)
        } catch (e: Exception) {
            log.warn(e) {
                "cleanup: the drained-ledger retention purge failed after the $label; the drain " +
                    "itself is unaffected and the next run retries from the bottom of the key"
            }
        }
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
     * COMMIT-RECEIPT RETENTION (#240): receipts of [catalogId] older than
     * the effective cutoff are hard-deleted, in bounded pages, and the
     * count goes on the run's `CleanupResult`.
     *
     * WHY THE TABLE NEEDED THIS AT ALL. `hog_commit_receipt` was written
     * once per idempotent commit and never read except on replay, and
     * nothing deleted a row — V7's "receipts outlive snapshot expiry"
     * implemented as "outlive everything". On gigahog-prod-us that was
     * 366,740 rows and 58.2 GiB (58.1 of it TOAST) by 2026-09-30, growing
     * ~4 GiB/day and 4-5x that after pyhoglake #234. V24 takes the body
     * out; this is the other half, and AGENT.md's rule is explicit that a
     * table written per commit needs a retention AND a bounded purge from
     * the day it is created.
     *
     * THE CUTOFF HAS A PER-CATALOG FLOOR, and it is a correctness floor
     * rather than a courtesy. A receipt purged while a client can still
     * replay its request does not produce an error — the replay becomes a
     * COMMIT, publishing the same files again under a new snapshot, with
     * every counter reporting success. That is the ACCEPTED CONSEQUENCE
     * of giving receipts a retention at all, and
     * `PurgedReceiptReplayIntegrationTest` asserts it rather than
     * describing it: two live `hog_data_file` rows over one path, with
     * non-overlapping row-id spans, and nothing anywhere reporting a
     * problem. There is no detector for it and none is wanted; the fence
     * is that the receipt outlives every payload a client may hold,
     * which is what this cutoff is. The longest legitimate gap between
     * a prepared payload and its retry is set by the CATALOG's snapshot
     * retention (pyhoglake keeps a prepared payload for at least
     * `snapshot_retention_seconds / 2`, hedgerow's `PendingStore` keeps
     * one across restarts), so the cutoff is
     * `max(receiptRetentionSeconds, 2 x snapshot_retention_seconds)`: the
     * instance knob unless this catalog's own window makes it too short,
     * never shorter than the knob. A catalog with retention DISABLED
     * contributes no floor — its snapshots are never expired, so the
     * derivation has no term, and the instance knob stands.
     *
     * THE WALK IS A PREFIX, NOT A SEARCH, which is why it carries none of
     * [purgeDrainedLedger]'s cursor/skip/empty-page machinery — see
     * [RECEIPT_PURGE_PAGE_SQL] for the difference. Pages of
     * [receiptPurgePage] under a RUN-scoped wall budget (see
     * [receiptPurgeDeadline]), each its own transaction under its own
     * `statement_timeout`, stopping on a short page (nothing eligible
     * left), on the budget, or on a page that failed. Nothing here takes
     * the commit lock, and no page is held across anything but its own
     * DELETE.
     *
     * THE PAGE IS BOUNDED IN RECEIPTS AND THE WORK IS IN TOAST CHUNKS,
     * and for seven days after the V24 deploy those are different
     * numbers by two orders of magnitude. A legacy receipt carries a
     * ~160 KiB `request` body, which is ~82 chunk rows in the TOAST
     * relation, and `heap_delete` calls `heap_toast_delete` on a tuple
     * with external attributes SYNCHRONOUSLY, inside the page's own
     * transaction — so a page of 1,000 legacy receipts is 1,000 heap
     * deletes plus ~82,000 chunk deletes and their TOAST-index entries,
     * against a 58 GiB relation that is not in `shared_buffers`. Every
     * post-V24 receipt stores nothing out of line and costs none of that.
     * [RECEIPT_PURGE_PAGE] is sized for the expensive regime and both
     * measurements are recorded there.
     *
     * HENCE `SET LOCAL statement_timeout`, which is the bound the wall
     * budget cannot be. The budget is checked between pages, so without a
     * per-statement bound "one page over the budget" means one page of up
     * to the session's 60 s (`Database.SESSION_INIT_SQL`) — and a page
     * that cannot finish in [RECEIPT_PURGE_STATEMENT_TIMEOUT] is a page
     * that is too big, which is a thing to learn in five seconds rather
     * than in sixty.
     *
     * A FAILED PAGE IS COUNTED, not swallowed. Without
     * [ReceiptPurge.failures] a page that times out is indistinguishable
     * on every surface from "nothing was eligible" — both report
     * `receipts_purged = 0` — and the failure mode is that seven days
     * after the deploy every sweep forever attempts the same first page,
     * times out, logs, reports 0, and the 58 GiB never goes while the
     * console reads exactly like a healthy idle instance. The count rides
     * the ledger row and `hoglake_commit_receipt_purge_failures_total`.
     * Pages already committed still count as purged: they are separate
     * transactions, so a failure on page 3 does not un-delete pages 1
     * and 2.
     *
     * WHAT IT DOES RECLAIM, corrected from an earlier draft of this
     * comment that had it backwards. `heap_delete` DELETES the chunk rows
     * (it does not merely mark the parent tuple dead), autovacuum then
     * reclaims them, and because every surviving receipt is post-V24 and
     * stores nothing out of line, the TOAST relation ends with no live
     * chunks at all — so plain VACUUM's truncation phase can return
     * essentially the whole 58 GiB of FILES. `DROP COLUMN request` (the
     * follow-up V24's header owes) returns none of it by itself: it sets
     * `attisdropped` and leaves both the TOAST relation and the existing
     * toast pointers alone. What never shrinks either way is the RDS
     * ALLOCATED volume, so the win is free space, backup size and restore
     * time rather than a smaller bill.
     *
     * THE ONE-TIME DRAIN IS ALSO AN AUTOVACUUM EVENT, and it is the real
     * cost: ~31M dead chunk tuples over 58 GiB, hours of throttled I/O
     * beside the commit path. It takes no blocking lock. V24's deploy
     * note says so.
     *
     * IT MUST NEVER FAIL THE DRAIN, so the caller fences it too: anything
     * that escapes this function is a WARN and a failure count, and the
     * drain it followed still reports what it did. That is
     * [purgeDrainedLedger]'s rule for the same reason — retention is
     * hygiene, and hygiene is not allowed to misreport a sweep.
     */
    private fun purgeCommitReceipts(
        catalog: String,
        catalogId: Long,
        snapshotRetentionSeconds: Long?,
        deadline: Long,
    ): ReceiptPurge {
        if (receiptRetentionSeconds <= 0) return ReceiptPurge()
        // NULL snapshot retention -> THE KNOB, and the one line is the
        // whole of it. A catalog with retention off never expires a
        // snapshot, so pyhoglake's prepared-payload shelf life there is
        // `inf` (client.py: `float("inf") if seconds is None`) and no
        // DERIVED floor can cover an unbounded window. The knob is the
        // floor, which means idempotency is unprotected for a payload
        // held past it — see `Config.receiptRetentionSeconds`, which says
        // the same thing about hedgerow on every catalog. Every real
        // catalog has retention set, so this is an edge rather than a
        // regime.
        val floor =
            snapshotRetentionSeconds?.let { (it * 2).coerceAtMost(RECEIPT_FLOOR_CEILING_SECONDS) }
                ?: receiptRetentionSeconds
        val retention = maxOf(receiptRetentionSeconds, floor)
        var purged = 0L
        var failures = 0L
        var pages = 0
        var stop = "nothing older than the cutoff"
        while (true) {
            if (System.nanoTime() >= deadline) {
                stop = "the ${receiptPurgeBudgetMs}ms run budget"
                break
            }
            val gone =
                try {
                    jdbi.inTransactionUnchecked { h ->
                        h.execute("SET LOCAL statement_timeout = '$RECEIPT_PURGE_STATEMENT_TIMEOUT'")
                        h.createUpdate(RECEIPT_PURGE_PAGE_SQL)
                            .bind("catalogId", catalogId)
                            .bind("page", receiptPurgePage)
                            .bind("retention", retention)
                            .execute()
                    }
                } catch (e: Exception) {
                    // One page, not the walk and not the drain. WARN
                    // because the counter says HOW MANY and only the log
                    // says WHY — a statement timeout here means the page
                    // is too big for the rows it is meeting, which is a
                    // knob, not a bug.
                    failures++
                    log.warn(e) {
                        "cleanup: a commit-receipt purge page failed for catalog '$catalog' " +
                            "(page $receiptPurgePage, retention ${retention}s, bound " +
                            "$RECEIPT_PURGE_STATEMENT_TIMEOUT); $purged receipts already purged this " +
                            "run are unaffected and the next run retries from the oldest eligible"
                    }
                    stop = "a failed page"
                    break
                }
            pages++
            purged += gone
            // The eligible rows are a dense prefix of the index range, so
            // a page that could not be filled is the end of them. There is
            // no un-purgeable row to walk past.
            if (gone < receiptPurgePage) break
        }
        if (purged > 0 || failures > 0) {
            log.debug {
                "cleanup: purged $purged commit receipts for catalog '$catalog' over $pages " +
                    "pages of $receiptPurgePage ($failures failed), stopped on $stop " +
                    "(retention ${retention}s)"
            }
        }
        return ReceiptPurge(purged, failures)
    }

    /**
     * When this RUN's receipt-purge budget expires.
     *
     * Per run rather than per catalog: `runOnceAllCatalogs` computes it
     * once and hands the same value to every catalog's drain, so a
     * sweep's receipt-purge wall cost is [receiptPurgeBudgetMs] whatever
     * the catalog count. The purge WORK stays per catalog (the index
     * leads on `catalog_id`, and the cutoff's floor is derived from the
     * catalog's own snapshot retention); only the clock is shared.
     */
    private fun receiptPurgeDeadline(): Long = System.nanoTime() + Duration.ofMillis(receiptPurgeBudgetMs).toNanos()

    /**
     * Ledger retention: drained rows older than [ledgerRetentionSeconds]
     * are hard-deleted so the soft-delete ledger cannot itself accumulate
     * without bound (the A1 lesson, applied to the fix for A3). Undrained
     * rows are never touched — `drained_at < cutoff` is NULL on them, so
     * the cutoff predicate is the whole fence.
     *
     * IT IS A LOOP OF BOUNDED DELETES, and that is the change. It used to
     * be one statement — `DELETE ... WHERE catalog_id = :c AND drained_at
     * < cutoff` — with NO access path: both indexes on the table are
     * partial on `drained_at IS NULL`, i.e. on the complement of the rows
     * this deletes, so the predicate could only be answered by a
     * sequential scan of the whole table, and the DELETE was unbounded in
     * rows AND in duration. At 190k rows/h drained and the 30-day default
     * the drained ledger heads for ~137M rows, and re-enabling cleanup on
     * prod-us is what would have fired that statement against it: an
     * unbounded, PK-uncorrelated DELETE, which is the exact shape of the
     * 2026-09-28 expiry-lock outage.
     *
     * THE PAGE BOUNDS THE ROWS EXAMINED, not only the rows deleted, and
     * that distinction is why the walk carries NO `catalog_id`. There is
     * no non-partial `(catalog_id, removal_id)` index, so a per-catalog
     * page would descend the primary key and FILTER on catalog: a catalog
     * holding a tenth of the table would read ten pages' worth of tuples
     * to fill one page, with the wall budget checked only BETWEEN pages
     * and nothing bounding the statement itself. Dropping the column makes
     * the window a dense prefix of the key. It is also correct rather than
     * merely cheaper: [ledgerRetentionSeconds] is a per-PROCESS constant,
     * so "drained longer ago than the retention" means the same thing for
     * every catalog, and the outer DELETE needs no `catalog_id` for the
     * reason `CLAIM_BULK_SQL`'s KDoc gives — `removal_id` is the table's
     * own global identity primary key.
     *
     * The cursor therefore advances over the rows EXAMINED (the window's
     * max id), not over the rows deleted. Advancing over deletions cannot
     * pass an un-purgeable row at all, so it re-read the same tail every
     * iteration; this form makes each page strictly new work.
     *
     * A SKIP, AND A CAP ON THE SKIPPING. A page that purges nothing is
     * SKIPPED — the cursor moves past it — rather than ending the walk,
     * because the walk is global and stopping at the first one would park
     * EVERY catalog's retention behind ONE catalog's un-purgeable window
     * (a tenant whose drain is wedged on bad credentials or a
     * still-referenced cluster holds ~[LEDGER_PURGE_PAGE] consecutive
     * undrained ids, and nothing in the deployment would purge again).
     *
     * But only [LEDGER_PURGE_EMPTY_PAGES] in a row, and the reason is day
     * one: the cursor restarts at the bottom of the key every run, and on
     * gigahog-prod-us the bottom is the 2.6M-row UNDRAINED backlog, so an
     * uncapped walk would spend its whole wall budget on ~200 pages that
     * purge nothing — every run, every interval, for as long as the
     * backlog stands. The cap keeps the wedge-skip (a wedge narrower than
     * it is still walked past) and bounds what a wedge costs. The walk
     * also stops on an EMPTY or SHORT window — the end of the table — or
     * on [LEDGER_PURGE_BUDGET_MS] of wall clock.
     *
     * A RUN THAT EXAMINED PAGES AND PURGED NOTHING SAYS SO AT INFO. Either
     * it is behind a wedge or it ran out of budget, and at debug that is
     * indistinguishable from a purge with nothing to do.
     *
     * IT IS GLOBAL, AND THAT IS OPERATOR-VISIBLE. A run for catalog A
     * purges the expired ledger rows of catalogs B, C, … as well; the
     * cutoff is [ledgerRetentionSeconds], a per-process constant, so every
     * catalog's rows are judged by the same rule and nothing is deleted
     * early. Because it is instance-wide it runs ONCE PER RUN rather than
     * once per catalog — [runOnce] calls it after its drain and
     * [runOnceAllCatalogs] calls it once for the whole sweep, where the
     * per-catalog form would have paid N wall budgets to re-walk the same
     * prefix of the key. The endpoint's OpenAPI description says so,
     * because an operator running a single-catalog cleanup during an
     * incident should not be surprised by another catalog's ledger
     * shrinking.
     */
    private fun purgeDrainedLedger(runLabel: String) {
        val deadline = System.nanoTime() + Duration.ofMillis(ledgerPurgeBudgetMs).toNanos()
        var after = 0L
        var purged = 0L
        var pages = 0
        var skipped = 0
        var consecutiveEmpty = 0
        // A BUDGET STOP IS A BUDGET STOP EVEN BEFORE THE FIRST PAGE. The
        // earlier form reported it as `pages > 0`, so a purge that never
        // got to run at all looked exactly like an idle one — the class of
        // indistinguishability that made the 2026-09-28 misdiagnosis take
        // as long as it did.
        var stop = "the end of the ledger"
        while (true) {
            if (System.nanoTime() >= deadline) {
                stop = "the ${ledgerPurgeBudgetMs}ms budget"
                break
            }
            val page =
                jdbi.withHandleUnchecked { h ->
                    h.createQuery(PURGE_PAGE_SQL)
                        .bind("after", after)
                        .bind("page", ledgerPurgePage)
                        .bind("retention", ledgerRetentionSeconds)
                        .map { rs, _ ->
                            Triple(rs.getInt("examined"), rs.getLong("cursor_id"), rs.getInt("purged"))
                        }
                        .one()
                }
            pages++
            val (examined, cursor, gone) = page
            purged += gone
            // An empty window is the end of the table.
            if (examined == 0) break
            // A page that purged NOTHING is SKIPPED, not a stop: the walk
            // is global, so stopping at the first one would park every
            // catalog's retention behind one catalog's un-purgeable
            // window. The cursor moves past it.
            if (gone == 0) {
                skipped++
                consecutiveEmpty++
            } else {
                consecutiveEmpty = 0
            }
            after = cursor
            // BUT ONLY [LEDGER_PURGE_EMPTY_PAGES] IN A ROW, because on day
            // one the bottom of the global key is the UNDRAINED backlog
            // (2.6M rows on gigahog-prod-us), and the cursor restarts at
            // the bottom every run: without this cap every run spends its
            // whole wall budget re-scanning ~200 pages that purge nothing,
            // every interval, for as long as the backlog stands. The cap
            // buys the wedge-skip — a wedge narrower than it is still
            // walked past — at a bounded cost per run.
            if (consecutiveEmpty >= LEDGER_PURGE_EMPTY_PAGES) {
                stop = "$LEDGER_PURGE_EMPTY_PAGES consecutive pages with nothing to purge"
                break
            }
            // A page the window could not fill has nothing above it.
            if (examined < ledgerPurgePage) break
        }
        val summary =
            "cleanup: purged $purged drained ledger rows over $pages pages of $ledgerPurgePage " +
                "($skipped with nothing to purge), stopped on $stop (after the $runLabel)"
        // AT INFO WHEN IT GOT NOWHERE, because that is the state an
        // operator has to be able to see: a purge that examined pages and
        // deleted nothing is either behind a wedge or out of budget, and
        // at debug it is indistinguishable from a purge with nothing to do
        // — which is the shape that made the 2026-09-28 misdiagnosis slow.
        if (pages > 0 && purged == 0L) log.info { summary } else log.debug { summary }
    }

    /**
     * Paths from [paths] that any file row (live or not) still claims.
     *
     * NO LOCK AND NO TRANSACTION. It used to run under the per-catalog
     * commit lock so its answer could not go stale against an in-flight
     * commit; that window does not exist (see the class KDoc: a commit
     * cannot register a path while an undrained row names it), and what
     * the lock actually bought was ~19 s of blocked commits per
     * sub-batch. What makes the check safe is the CLAIM plus the commit
     * path's own refusal, not a lock.
     *
     * LIVE OR NOT is the load-bearing half, and it is why V17's indexes
     * are not partial: a historical row still claims its object at
     * every retained snapshot, so `end_snapshot IS NULL` here — or in
     * the index that serves it — would authorize deleting an object a
     * time-travel read still needs.
     *
     * Each leg is one `(catalog_id, path)` probe per sub-batch path:
     * `hog_data_file_path` and `hog_delete_file_path` (V17), and
     * hog_upload's own `UNIQUE (catalog_id, path)` from V12, with
     * `state` a filter on the rows it fetches. The statement is
     * unchanged by V17 — the indexes serve it as written — and
     * `V17FilePathIndexMigrationIntegrationTest` EXPLAINs the text this
     * function actually issues, captured off a real drain, rather than
     * a copy of it.
     */
    private fun referencedPaths(
        catalogId: Long,
        paths: List<String>,
    ): Set<String> =
        jdbi.withHandleUnchecked { h ->
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
        }

    /**
     * One drain across every catalog, for the background loop
     * (BackgroundLoops in App.kt). Catalogs are isolated: one catalog's
     * failure is logged and the rest proceed.
     */
    fun runOnceAllCatalogs(batchSize: Int): List<Pair<String, CleanupResult>> {
        val names = jdbi.withHandleUnchecked { h -> CatalogRepo.listAll(h) }.map { it.name }
        // ONE receipt-purge budget for the whole sweep, computed here and
        // shared by every catalog's drain. Inside `doRunOnce` the purge is
        // necessarily PER CATALOG — V24's index leads on `catalog_id` and
        // the cutoff's floor is derived per catalog — but its wall BUDGET
        // must not be, or a sweep's receipt-purge cost is
        // `N x receiptPurgeBudgetMs` and grows with the catalog count
        // while nothing in the loop bounds it. The first catalogs in the
        // list can therefore spend the whole budget; that is the intended
        // behaviour and it self-corrects, because a catalog whose
        // receipts went unpurged this sweep is first in line next sweep
        // once the earlier ones have nothing eligible left.
        val receiptDeadline = receiptPurgeDeadline()
        val results = mutableListOf<Pair<String, CleanupResult>>()
        for (name in names) {
            try {
                results += name to drainCatalog(name, batchSize, MaintenanceTrigger.LOOP, receiptDeadline)
            } catch (e: Exception) {
                log.error(e) { "cleanup drain failed for catalog '$name'; continuing" }
            }
        }
        // ONE retention purge for the whole sweep, not one per catalog:
        // the walk is instance-wide, so N catalogs would have paid N times
        // for the same work — and on a catalog-rich deployment that is N
        // wall budgets spent re-walking the same prefix of the key.
        purgeAfterRun("sweep over ${names.size} catalog(s)")
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
         * (HOGLAKE_CLEANUP_SUB_BATCH) — and therefore the size of one
         * CLAIM.
         *
         * 1,000 is S3's own ceiling on ONE `DeleteObjects` request, so a
         * default sub-batch whose paths share a bucket is exactly one
         * object-store round trip. It was 25, and that number was sized
         * for a different statement: the drain then issued
         * subBatchSize x (HEAD + DELETE) under the per-catalog commit
         * lock, so the constant was a commit-latency bound (~3.2 s per
         * hold, ~255 s of holding per 2,000-row run) rather than a
         * batching decision.
         *
         * Raising it past 1,000 buys nothing — the extra keys chunk into
         * extra calls inside the same sub-batch — so the ceiling is where
         * it stops being free, not where it starts being wrong.
         * `compaction_staging` rows are settled in their own sub-batches
         * of [STAGING_SUB_BATCH] whatever this is.
         */
        const val SUB_BATCH = 1000

        /**
         * Staging tickets settled per sub-batch.
         *
         * 25 is the OLD whole-queue sub-batch, kept for the one reason
         * that still costs what the old one did: a `compaction_staging`
         * row is a HeadObject and a DeleteObject, and there is no
         * batched form that reports `'absent'`. At the 64 ms per round
         * trip measured on gigahog-prod-us that is ~3.2 s of
         * object-store work per sub-batch. It is no longer a LOCK bound
         * — there is no lock — it is the unit of PROGRESS: each
         * sub-batch settles in its own transaction, so a claim that ends
         * badly forty tickets in keeps the first twenty-five.
         *
         * [STAGING_GRACE_SECONDS] clusters them, which is why the size
         * still matters: tickets become eligible in the order their
         * groups ran, so a compaction sweep that aborted a run of groups
         * puts its whole run into one claim an hour later.
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
         * How long a claim is honoured (HOGLAKE_CLEANUP_CLAIM_LEASE_SECONDS):
         * 900 s.
         *
         * A LEASE, not a lock, and the quantity it has to cover is how
         * long ONE CLAIM's work can take AT THE CALL BOUND, not at a
         * measured average — which is the whole reason the claim is
         * reason-aware. A bulk claim is [SUB_BATCH] paths and one
         * `DeleteObjects` call per bucket chunk: a handful of calls, so
         * ~64 ms measured and ~`chunks x 10 s` worst case. A staging
         * claim is [STAGING_SUB_BATCH] tickets and two calls each: 50
         * calls, 500 s worst case, inside this lease with 400 s to
         * spare. A single claim of [SUB_BATCH] TICKETS would be 20,000 s
         * worst case and could not fit — which is why no such claim can
         * be issued (`CLAIM_STAGING_SQL`), and
         * `RemovalStoreBoundsTest` asserts both arms against this
         * constant.
         *
         * Wrong in either direction costs work, never an object. Too
         * short and two workers do the same delete — idempotent, and the
         * loser's settle is refused by the `claimed_by` fence, counted
         * `settled_elsewhere`. Too long and a killed worker's rows wait
         * out the lease before anyone retries them.
         *
         * It is ALSO compaction's number: `CompactionConfig`'s copy is
         * what decides when a group commit may take a claimed staging
         * ticket back, and the two services must not disagree about the
         * same row.
         */
        const val CLAIM_LEASE_SECONDS = 900L

        /**
         * `pod/uuid` for this process's workers. `HOSTNAME` is the pod
         * name on Kubernetes and the machine name locally; the UUID is
         * what makes a restarted pod a DIFFERENT claimant, so its
         * predecessor's rows come back through the lease rather than
         * being silently re-adopted by a process that never claimed them.
         */
        internal fun defaultWorkerIdPrefix(): String =
            "${System.getenv("HOSTNAME")?.takeIf { it.isNotBlank() } ?: "local"}/${UUID.randomUUID()}"

        /**
         * THE CLAIM'S INNER SELECT, bulk arm: the oldest claimable rows
         * of one catalog that are NOT `compaction_staging`, in queue
         * order, locked and skipped over rather than waited for.
         *
         * `internal` and separate from its UPDATE because it is the half
         * whose PLAN matters, and a plan test must EXPLAIN the statement
         * production issues rather than a copy of it
         * (`V16FileRemovalPathIndexMigrationIntegrationTest` and
         * `V21CleanupClaimMigrationIntegrationTest` both do). EXPLAIN
         * ANALYZE of the UPDATE itself would claim rows for real.
         *
         * This is the statement `hog_file_removal_drain (catalog_id,
         * removal_id) WHERE drained_at IS NULL` exists for — when it is
         * chosen, the index supplies both the predicate and the
         * `ORDER BY`, so the LIMIT stops the scan rather than trimming a
         * sort.
         *
         * WHEN IT IS CHOSEN is a real qualifier, and it is the SPARSE
         * shape: a catalog whose undrained rows are a small fraction of
         * the table, which is what a 30-day ledger gives one that
         * drains. Where they are DENSE — a catalog that is behind, which
         * is the state #199 describes — the primary key on `removal_id`
         * already arrives in the right order and discards only a row or
         * two per row it emits, and the planner takes THAT instead. Both
         * plans are correct and both stop at the LIMIT; the drain index
         * is the one that keeps the cost bounded as the settled ledger
         * grows around the queue.
         *
         * The claim and reason clauses are FILTERS on rows the index has
         * already narrowed to one catalog's queue, at most a batch past
         * the leading columns — neither is something to index. A new
         * index the planner preferred here would be a regression, not a
         * win: `(catalog_id, path)` supplies no `removal_id` ordering, so
         * the LIMIT would sit on top of a sort of every undrained row.
         *
         * THE CLAIM CLAUSE IS `claimed_at IS NULL OR claimed_at < now() -
         * lease`, which is the lease read as a predicate: an unclaimed
         * row, or one whose claimant has been gone longer than
         * [CLAIM_LEASE_SECONDS]. Drop it and two workers drain one row at
         * once; invert it and a fresh claim is stolen immediately.
         *
         * Binds `:catalogId`, `:limit` and `:leaseSeconds`.
         */
        internal const val CLAIM_CANDIDATE_SQL: String =
            """
            SELECT removal_id FROM hog_file_removal
                    WHERE catalog_id = :catalogId AND drained_at IS NULL
                      AND reason <> 'compaction_staging'
                      AND (claimed_at IS NULL
                           OR claimed_at < now() - make_interval(secs => :leaseSeconds))
                    ORDER BY removal_id
                    LIMIT :limit
                    FOR UPDATE SKIP LOCKED
            """

        /**
         * The same, STAGING arm: `compaction_staging` tickets past
         * [STAGING_GRACE_SECONDS].
         *
         * A SEPARATE STATEMENT BECAUSE THE LEASE HAS TO BOUND THE WORK A
         * CLAIM CARRIES. One claim with no reason predicate could hold
         * [SUB_BATCH] tickets — 2,000 HEAD/DELETE round trips under a
         * single lease, 128 s at the measured 64 ms but 20,000 s at
         * `RemovalStore.apiCallTimeout`, which is 22x
         * [CLAIM_LEASE_SECONDS]. Claimed at [STAGING_SUB_BATCH] the worst
         * case is 50 calls, 500 s, inside the lease — and that is the
         * inequality `RemovalStoreBoundsTest` asserts, for both arms.
         *
         * Binds `:catalogId`, `:limit`, `:leaseSeconds` and
         * `:stagingGraceSeconds`.
         */
        internal const val CLAIM_STAGING_CANDIDATE_SQL: String =
            """
            SELECT removal_id FROM hog_file_removal
                    WHERE catalog_id = :catalogId AND drained_at IS NULL
                      AND reason = 'compaction_staging'
                      AND scheduled_at < now() - make_interval(secs => :stagingGraceSeconds)
                      AND (claimed_at IS NULL
                           OR claimed_at < now() - make_interval(secs => :leaseSeconds))
                    ORDER BY removal_id
                    LIMIT :limit
                    FOR UPDATE SKIP LOCKED
            """

        /**
         * The claim: the candidate select above, wrapped in the UPDATE
         * that makes those rows this worker's.
         *
         * One statement, one transaction, committed before any
         * object-store call — so a worker waiting on S3 holds no
         * transaction, no row lock and no advisory lock, and
         * `idle_in_transaction_session_timeout` bounds nothing here.
         *
         * `FOR UPDATE SKIP LOCKED` on the inner select is what makes
         * concurrent workers and concurrent replicas partition the queue
         * with no coordination: a row another claim is holding is SKIPPED
         * rather than waited for, so a claim's cost never depends on how
         * many drains are running. It is also why there is no
         * single-flight advisory lock: the thing such a lock would
         * prevent — two workers draining the same row — cannot happen.
         *
         * THE OUTER UPDATE CARRIES NO `catalog_id` AND NO `drained_at IS
         * NULL`, unlike every other statement here, and that is safe for
         * two specific reasons: `removal_id` is the table's own
         * `GENERATED ALWAYS AS IDENTITY PRIMARY KEY`, so it is globally
         * unique and cannot name another catalog's row; and the inner
         * `FOR UPDATE` holds every row it returned, so no re-check can go
         * stale between the select and the update. A future composite key
         * would break the first half.
         *
         * No interpolated values (invariant 9 intact) — the composition
         * is two CONSTANTS, not a value.
         */
        internal const val CLAIM_BULK_SQL: String =
            """
            UPDATE hog_file_removal r
               SET claimed_at = now(), claimed_by = :worker
             WHERE r.removal_id IN ($CLAIM_CANDIDATE_SQL)
            RETURNING removal_id, path, reason
            """

        /** The staging arm's UPDATE — see [CLAIM_BULK_SQL] for the shape. */
        internal const val CLAIM_STAGING_SQL: String =
            """
            UPDATE hog_file_removal r
               SET claimed_at = now(), claimed_by = :worker
             WHERE r.removal_id IN ($CLAIM_STAGING_CANDIDATE_SQL)
            RETURNING removal_id, path, reason
            """

        /**
         * The rows a settle did not match, read back inside the settle's
         * own transaction so the miss can be CLASSIFIED rather than
         * counted.
         *
         * Binds `:catalogId` and the `:ids` array.
         */
        internal const val MISSED_SETTLE_SQL: String =
            """
            SELECT removal_id, path, drained_outcome, claimed_by
              FROM hog_file_removal
             WHERE catalog_id = :catalogId AND removal_id = ANY(:ids)
            """

        /**
         * Give a claimed row back, unchanged: the run already handled it
         * and claimed it again (a still-referenced skip releases its
         * claim, which makes the row claimable inside the same run).
         *
         * Fenced like every other write here — `drained_at IS NULL AND
         * claimed_by = :worker` — so releasing can never disturb a row
         * that has since become someone else's.
         *
         * Binds `:catalogId`, `:worker` and the `:ids` array.
         */
        internal const val RELEASE_CLAIM_SQL: String =
            """
            UPDATE hog_file_removal
               SET claimed_at = NULL, claimed_by = NULL
             WHERE catalog_id = :catalogId AND removal_id = ANY(:ids)
               AND drained_at IS NULL AND claimed_by = :worker
            """

        /**
         * Record a touch that did NOT drain the row — a still-referenced
         * skip, or a delete that failed — and RELEASE the claim, so the
         * row is visible to the next run and to an operator reading the
         * queue instead of waiting out a lease it will not use.
         *
         * FENCED ON `drained_at IS NULL AND claimed_by = :worker`, and
         * `RETURNING` is what the caller counts: `attempts` and
         * `last_attempt_at` on a row this worker no longer holds are a
         * false statement about what cleanup did to it, and — the reason
         * the return value is read rather than discarded — so is calling
         * that row an invariant violation. A compaction group that
         * commits between the claim and the reference check makes the
         * path live and settles the ticket in one transaction; the check
         * sees a live file, and only the fence can tell that from the
         * real thing.
         *
         * Binds `:catalogId`, `:worker` and the `:ids` array.
         */
        internal const val BUMP_ATTEMPTS_SQL: String =
            """
            UPDATE hog_file_removal
               SET attempts = attempts + 1, last_attempt_at = now(),
                   claimed_at = NULL, claimed_by = NULL
             WHERE catalog_id = :catalogId AND removal_id = ANY(:ids)
               AND drained_at IS NULL AND claimed_by = :worker
            RETURNING removal_id
            """

        /**
         * Settle a drained row, FENCED on `drained_at IS NULL AND
         * claimed_by = :worker`.
         *
         * BOTH HALVES ARE LOAD-BEARING. `drained_at IS NULL` keeps this
         * statement off a row another writer already settled — without
         * it, cleanup would stamp `'deleted'` over a compaction group's
         * `'registered'`, erasing the only record that the path became a
         * live catalog file and leaving a ledger that says cleanup
         * deleted an object the catalog is serving. `claimed_by =
         * :worker` is the LEASE's half: a worker whose claim lapsed while
         * it was talking to S3 no longer owns the row, and the worker
         * that reclaimed it is the one entitled to say what happened.
         *
         * The claim is cleared with the settle, so a settled row carries
         * no stale claimant.
         *
         * `RETURNING` is therefore the count that may be trusted: the
         * caller reports the difference against the ids it asked for as
         * `settled_elsewhere` and counts only what it actually settled.
         *
         * Binds `:outcome`, `:catalogId`, `:worker` and the `:ids` array.
         */
        internal const val SETTLE_SQL: String =
            """
            UPDATE hog_file_removal
               SET drained_at = now(), drained_outcome = :outcome,
                   last_attempt_at = now(), claimed_at = NULL, claimed_by = NULL
             WHERE catalog_id = :catalogId AND removal_id = ANY(:ids)
               AND drained_at IS NULL AND claimed_by = :worker
            RETURNING removal_id
            """

        /**
         * One page of the drained-ledger purge: the ids it reads through
         * the primary key, and therefore the rows one DELETE may remove.
         *
         * 1,000 is the same order as a sub-batch, which is the only
         * reference point that matters — the purge's cost per page is a
         * key descent plus a heap page per row, so a page is a handful of
         * milliseconds and a cancelled one costs a page.
         */
        const val LEDGER_PURGE_PAGE = 1000

        /**
         * Wall clock the drained-ledger purge may spend, per run. A pacing
         * bound, not a correctness one: whatever it does not reach is the
         * next run's, because the walk always restarts from the bottom of
         * the key.
         *
         * THE NUMBER IT BUYS, honestly: a 1,000-row page is a
         * `DELETE ... RETURNING` plus its WAL, order 5-15 ms warm, so one
         * second is **roughly 70-200 pages, i.e. 70-200k rows** per run —
         * not the "~1,000 pages" an earlier draft of this KDoc claimed. At
         * a 30-minute interval that is 140-400k rows/h against ~190k/h of
         * arrivals on prod-us: it keeps up, with little margin. The lever
         * if it does not is the interval (which the drain-down shortens
         * anyway) or `HOGLAKE_REMOVAL_LEDGER_RETENTION_SECONDS`, which is
         * what actually decides how large the drained ledger gets — at 30
         * days and 190k/h it heads for ~137M rows, and bounding the
         * STATEMENT did not bound the TABLE.
         */
        const val LEDGER_PURGE_BUDGET_MS = 1000L

        /**
         * Consecutive pages with nothing to purge before the walk gives up
         * for this run.
         *
         * THE CAP IS ABOUT DAY ONE, not about wedges. The cursor restarts
         * at the bottom of the primary key every run, and on a queue that
         * is behind, the bottom is the UNDRAINED backlog — 2.6M rows on
         * gigahog-prod-us, ~2,600 pages of nothing to purge. Uncapped, the
         * walk spends the whole [LEDGER_PURGE_BUDGET_MS] on them every
         * run and never reaches a purgeable row, at debug, for as long as
         * the backlog stands.
         *
         * WHAT TEN CLEARS, AND WHAT IT DOES NOT, because the queue's rows
         * arrive in ONE-STATEMENT BLOCKS and those blocks are the wedges
         * that actually occur: a retirement batch queues
         * `HOGLAKE_RETIREMENT_BATCH` paths (8,000 by default, ~8 pages) and
         * an expiry sweep queues a whole floor advance's worth (tens of
         * thousands — ~50 pages at 50,000). So ten pages walks past a
         * retirement batch's block of undrained rows and does NOT walk past
         * an expiry sweep's; the rows above a block that wide wait for the
         * runs after it drains.
         *
         * That is a pacing choice, not a correctness one in either
         * direction — the rows above any wedge are purged once it settles,
         * and until then they sit inside a retention window that is 30 days
         * wide — and a walk parked behind a block is VISIBLE rather than
         * silent: the purge logs its stop reason at INFO whenever it
         * examined pages and purged nothing. Raising the cap trades a
         * longer walk per run for reaching past wider blocks; the wall
         * budget bounds both.
         */
        const val LEDGER_PURGE_EMPTY_PAGES = 10

        /**
         * ONE PAGE of the drained-ledger purge: read up to `:page` ids
         * above `:after` THROUGH THE PRIMARY KEY, delete those past the
         * retention cutoff, and report what the page EXAMINED, how far it
         * got, and what went.
         *
         * THREE NUMBERS BECAUSE THE CURSOR MOVES OVER WORK, NOT OVER
         * DELETIONS. `examined` is what bounds the statement — the window
         * is ordered and limited, so its row count is the page size
         * whatever the ledger holds — `cursor_id` is the window's max id,
         * so the next page is strictly new work even when this one deleted
         * nothing, and `purged` is what actually went. The old single
         * `DELETE ... WHERE drained_at < cutoff` could bound none of the
         * three: every index on this table is partial on `drained_at IS
         * NULL`, the complement of the rows it deletes, so the predicate
         * had no access path at all and read the whole table.
         *
         * NO `catalog_id`, in the window or in the DELETE. In the window
         * because there is no non-partial `(catalog_id, removal_id)` index,
         * so filtering by catalog would make one page scan N pages' worth
         * of tuples for a catalog holding 1/N of the table — the page would
         * bound deletions and not work, which is the defect this shape
         * fixes. In the DELETE for `CLAIM_BULK_SQL`'s reason: `removal_id`
         * is the table's own `GENERATED ALWAYS AS IDENTITY PRIMARY KEY`, so
         * it is globally unique and cannot name another catalog's row. A
         * future composite key breaks both.
         *
         * `internal` so the plan test can EXPLAIN the statement production
         * issues (`V21CleanupClaimMigrationIntegrationTest`): the property
         * is an index scan on the key with the page as its bound, never a
         * sequential scan, and nothing removed by a filter.
         *
         * Binds `:after`, `:page` and `:retention`.
         */
        internal const val PURGE_PAGE_SQL: String =
            """
            WITH page AS (
                SELECT removal_id FROM hog_file_removal
                 WHERE removal_id > :after
                 ORDER BY removal_id
                 LIMIT :page
            ), gone AS (
                DELETE FROM hog_file_removal
                 WHERE removal_id IN (SELECT removal_id FROM page)
                   AND drained_at < now() - make_interval(secs => :retention)
                RETURNING removal_id
            )
            SELECT (SELECT count(*) FROM page)::int AS examined,
                   coalesce((SELECT max(removal_id) FROM page), :after) AS cursor_id,
                   (SELECT count(*) FROM gone)::int AS purged
            """

        /** Default drained-ledger retention: 30 days (HOGLAKE_REMOVAL_LEDGER_RETENTION_SECONDS). */
        const val LEDGER_RETENTION_SECONDS = 30L * 24 * 60 * 60

        /** Default run-ledger retention: 7 days (HOGLAKE_MAINTENANCE_LEDGER_RETENTION_SECONDS). */
        const val MAINTENANCE_LEDGER_RETENTION_SECONDS = 7L * 24 * 60 * 60

        /**
         * Default receipt retention: 7 days
         * (HOGLAKE_RECEIPT_RETENTION_SECONDS; `Config` carries the
         * argument for the number). 0 here would make every hand-built
         * service purge nothing, which is the wrong default for a class
         * whose tests are the only callers of the primary constructor.
         */
        const val RECEIPT_RETENTION_SECONDS = 7L * 24 * 60 * 60

        /**
         * Receipts deleted per page.
         *
         * 200, NOT [LEDGER_PURGE_PAGE]'s 1,000, and the difference is the
         * whole of P1-1: A PAGE IS BOUNDED IN RECEIPTS AND THE WORK IS IN
         * TOAST CHUNKS. A post-V24 receipt is ~100 bytes of fixed-width
         * columns and stores nothing out of line; a PRE-V24 one carries a
         * ~160 KiB `request` body, which is ~80 rows in the TOAST
         * relation that `heap_delete` removes synchronously inside the
         * page's transaction. For the first retention window after the
         * deploy, the only eligible rows are the expensive kind.
         *
         * MEASURED, both regimes, by
         * `CleanupReceiptPurgeIntegrationTest` (PG 18, one catalog, every
         * row eligible, the legacy fixture at 150 KiB of out-of-line body
         * per row):
         *
         *   post-V24 rows (no body):    1.6-5.6 us/row
         *   legacy rows (150 KiB):     49-187 us/row, 10-37 ms per 200-row page
         *
         * ~60-100x per row, and the fixture is the OPTIMISTIC end of it:
         * its TOAST relation is 91 MiB and fits in the container's cache,
         * production's is 58 GiB. What keeps the production page from
         * being random I/O is
         * that the legacy rows all share one `created_at` (V24's fast
         * default), so the walk takes them in ctid order, which is
         * insertion order, which is the order their chunks were written —
         * the reads are largely sequential. At 200 receipts a page is
         * ~32 MB of mostly-sequential reads, ~27 ms on the fixture and a
         * few hundred milliseconds cold: well inside
         * [RECEIPT_PURGE_STATEMENT_TIMEOUT] with room for the difference
         * between a fixture and a volume.
         *
         * THE LEGACY DRAIN, re-stated for this page size. 366,740 rows is
         * 1,834 pages. At [RECEIPT_PURGE_BUDGET_MS] and a few hundred ms
         * per cold page that is ~10-40 pages per run, so ~50-180 runs —
         * ONE TO FOUR DAYS at the cleanup loop's 30-minute default,
         * starting seven days after the deploy. An operator who wants the
         * space back faster drives `POST .../maintenance/cleanup`, which
         * runs this same purge with a fresh budget per call. Steady state
         * is nothing next to that: ~72k receipts expire a day at
         * ~50 commits/min, which is ~360 cheap pages, and one run absorbs
         * them.
         */
        const val RECEIPT_PURGE_PAGE = 200

        /**
         * Per-statement bound on one page.
         *
         * THE BUDGET CANNOT BE THIS BOUND. [RECEIPT_PURGE_BUDGET_MS] is
         * checked BETWEEN pages, so without a statement bound a single
         * page inherits the session's 60 s
         * (`Database.SESSION_INIT_SQL`) and "one page over the budget"
         * means a minute. Five seconds is two orders of magnitude above
         * the measured page in either regime, so it can only fire on a
         * page that is genuinely too big for the rows it is meeting —
         * which is a thing to learn in five seconds, retryably, with a
         * counted failure, rather than in sixty.
         *
         * Matched to the migration window's `lock_timeout` value for the
         * same reason every other bound in this repo is a stated number
         * rather than a derived one: it is the figure an operator can
         * hold in their head while reading a WARN.
         */
        const val RECEIPT_PURGE_STATEMENT_TIMEOUT = "5s"

        /**
         * Ceiling on the floor the purge derives from a catalog's
         * snapshot retention (see [purgeCommitReceipts]).
         *
         * 30 days. Without it a catalog PATCHed to 90-day snapshot
         * retention gets a 180-day receipt floor and effectively never
         * purges, silently — and `2 x` an arbitrary operator-supplied
         * `bigint` is also the only arithmetic here that could overflow.
         * The knob is still the lower bound, so capping the derived term
         * can only ever shorten a floor that was longer than a month,
         * never shorten the configured retention.
         */
        const val RECEIPT_FLOOR_CEILING_SECONDS = 30L * 24 * 60 * 60

        /**
         * Wall clock the receipt purge may spend per run.
         *
         * PER RUN, not per catalog: `runOnceAllCatalogs` computes one
         * deadline and shares it across the sweep, so the cost does not
         * scale with the catalog count (see [receiptPurgeDeadline]).
         *
         * TEN SECONDS, where [LEDGER_PURGE_BUDGET_MS] is one, and the
         * difference is the one-time drain. The ledger purge walks a
         * table heading for 137M rows and is genuinely instance-wide, so
         * a second per run is the right pace for it. This purge has a
         * 366,740-row backlog to clear ONCE and then ~360 cheap pages a
         * day forever; at one second per run the backlog would take
         * weeks. Ten seconds of a 30-minute cleanup cycle is a 0.5% duty
         * on one pooled connection, holding no lock, after the drain has
         * finished — nothing waits on it, and it turns the backlog into
         * days (see [RECEIPT_PURGE_PAGE]'s arithmetic).
         *
         * The budget is checked BETWEEN pages, so the real bound is one
         * page over it — and [RECEIPT_PURGE_STATEMENT_TIMEOUT] is what
         * bounds that page, which is the half a wall budget cannot do.
         */
        const val RECEIPT_PURGE_BUDGET_MS = 10_000L

        /**
         * ONE PAGE of the receipt purge: delete up to `:page` of ONE
         * catalog's receipts older than the cutoff, through V24's
         * `hog_commit_receipt_created (catalog_id, created_at)` index,
         * and report how many went.
         *
         * ONE NUMBER, WHERE [PURGE_PAGE_SQL] NEEDS THREE, and the
         * difference is worth stating because it is the reason this purge
         * has no cursor, no skip and no empty-page cap. The drained-ledger
         * walk steps over a key (`removal_id`) that is UNCORRELATED with
         * its predicate (`drained_at < cutoff`), so a page can examine
         * 1,000 rows and purge none of them, forever, and the machinery
         * exists to walk past that. Here the predicate is ON THE INDEXED
         * COLUMN: the eligible rows are exactly a dense PREFIX of the
         * catalog's `(catalog_id, created_at)` range, so every page but
         * the last deletes a full page, `purged < :page` means "nothing
         * eligible is left", and a page that purges nothing cannot have a
         * successor that does.
         *
         * SCOPED TO ONE CATALOG, where the ledger purge is deliberately
         * global. The index leads on `catalog_id`, so a per-catalog page
         * is a descent rather than a filter, and the caller already holds
         * the catalog's id and its snapshot retention — which is what the
         * cutoff's floor is derived from, and it differs per catalog.
         *
         * THE DELETE ADDRESSES THE PAGE BY `ctid`, and the two forms that
         * look more natural are both wrong. Joining back on the PRIMARY
         * KEY (`(catalog_id, idempotency_key) IN (page)`) makes the
         * planner hash-join 1,000 keys against a SEQUENTIAL SCAN of the
         * catalog's whole receipt table — measured on the 100,000-row
         * fixture as 2,568 buffers of seq scan on top of the page's own
         * 1,006, i.e. the per-page cost is O(the catalog's receipts) and
         * the index bought nothing. Widening the page into a RANGE
         * (`created_at <= max(page)`) is worse: V24 dates every legacy
         * row at the migration, so 366,740 rows share one timestamp on
         * gigahog-prod-us and a range delete would take all of them in
         * one statement — the unbounded-DELETE shape of the 2026-09-28
         * outage. `ctid` is exact, bounded by the page, and safe on this
         * table because a receipt is INSERT-only: nothing UPDATEs one, so
         * no live row's ctid moves, and the subselect and the delete are
         * one statement under one snapshot regardless.
         *
         * `internal` so the plan test can EXPLAIN the statement production
         * issues (`V24CommitReceiptRetentionMigrationIntegrationTest`).
         *
         * Binds `:catalogId`, `:page` and `:retention`. The clock is the
         * DATABASE's, as it is for every other retention statement here:
         * a cutoff computed on a pod would be that pod's clock skew.
         */
        internal const val RECEIPT_PURGE_PAGE_SQL: String =
            """
            DELETE FROM hog_commit_receipt
             WHERE ctid = ANY (
                 ARRAY(
                     SELECT ctid FROM hog_commit_receipt
                      WHERE catalog_id = :catalogId
                        AND created_at < now() - make_interval(secs => :retention)
                      ORDER BY created_at
                      LIMIT :page
                 )
             )
            """
    }
}
