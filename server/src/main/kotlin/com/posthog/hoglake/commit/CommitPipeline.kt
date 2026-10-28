package com.posthog.hoglake.commit

import com.posthog.hoglake.model.CommitResult
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap

/**
 * The adaptive per-catalog commit pipeline.
 *
 * ## The problem
 *
 * The commit path serializes writers per catalog with a Postgres
 * advisory lock ([com.posthog.hoglake.persistence.Locks]). Taken
 * transaction-scoped, that lock is held for the WHOLE commit — the
 * read, the checks, the write, AND the JDBC COMMIT's WAL flush wait —
 * and the flush is the slow part on network storage (RDS commit latency
 * is milliseconds; everything else in the tail is sub-millisecond
 * since #393 cut the tail to two statements). Under N writers the
 * advisory queue convoys: per-catalog throughput caps at
 * 1/(tail + flush) and every writer's latency is N times that.
 *
 * ## The shape
 *
 * The commit splits at the point the tail's last statement returns:
 *
 *  - **Stage A (serial per catalog, unchanged contents).** Catalog
 *    resolution, the read, every check, snapshot/file-id/row-id
 *    allocation, row building, and the write — everything
 *    `CommitService.doCommit` does today between the lock and the
 *    write's return. Serialization stays the DATABASE advisory lock
 *    (session-scoped now, [Locks.acquireCatalogCommitLockSession]), not
 *    an in-memory one: that keeps stage A exclusive against DDL,
 *    expiry, retirement and compaction tails on ANY pod, keeps the
 *    `lock_timeout` -> 503 admission contract where it is, and keeps
 *    cross-pod stage As off each other. The lock is released by an
 *    explicit statement after the write returns — a separate statement,
 *    not a write CTE, because Postgres does not order independent CTEs
 *    (an unlock CTE was observed to run BEFORE the write's UPDATE).
 *  - **Stage B (overlapped).** The JDBC COMMIT — the flush wait — runs
 *    after the lock is gone, on the commit's own connection, with up to
 *    `depth` commits of one catalog in flight at once.
 *
 * The durability contract is UNCHANGED: a commit's response is sent
 * only after its own COMMIT returns. Pipelining overlaps the waits; it
 * never acks early.
 *
 * ## Why the order is still the order — and the real ceiling
 *
 * Stage A assigns snapshot ids serially, and stage B cannot reorder
 * them: every commit's write UPDATEs the catalog's hog_catalog row
 * (the `alloc` CTE), and its qual is KEY-ONLY, so under READ COMMITTED
 * the UPDATE blocks on an in-flight predecessor's row lock until the
 * predecessor's COMMIT (Postgres releases row locks after the sync
 * flush) and then applies RELATIVE increments to the newest version,
 * re-validating against the merged view after the wait
 * (`alloc_moved`/`stats_moved` in the write's counts). An earlier form
 * of the guard predicated the UPDATE on the merged values, which READ
 * COMMITTED evaluates against the stale visible version: it matched no
 * row, never waited, and turned every overlap into a fail-and-retry.
 *
 * Two consequences, stated plainly:
 *
 *  - **The WAL sees one catalog's commits in stage-A order**; a reader
 *    never observes snapshot N+1 without N; ids stay dense and ordered
 *    with commit order — the lineage premise, intact.
 *  - **The flushes of one catalog's commits CANNOT overlap.** N+1's
 *    COMMIT can only start after its write returns, and its write
 *    waits for N's COMMIT. What pipelining overlaps is the LOCK WAIT,
 *    the READ, the CHECKS and the ROW BUILDING of N+1 (and N+2, ...)
 *    with N's flush: the serial residue per commit is the write round
 *    trip, the explicit unlock's round trip, and the flush — not the
 *    whole tail. Against network storage (RDS), where the pre-write
 *    tail is several round trips of milliseconds each, that is the
 *    win; it is not N×, and no group commit batches these flushes.
 *
 * ## Read visibility under pipelining
 *
 * A pipelined commit is invisible to readers between its stage A and
 * its COMMIT — but so is every tail that could interleave: DDL,
 * expiry, retirement and compaction all serialize on the same advisory
 * lock (so they start no earlier than the commit's unlock) AND
 * allocate by UPDATEing the hog_catalog row (so their writes queue on
 * the in-flight commit's row lock and become visible only after it).
 * The invariant that keeps them safe: every hog_catalog/
 * hog_table_stats allocation anywhere in the codebase is a RELATIVE
 * increment (`SET x = x + 1 ... RETURNING`), never an absolute SET
 * computed from a pre-lock read — an absolute-SET allocator added
 * later would corrupt under pipelining, and the alloc_moved check is
 * what catches it. Commit-to-commit order cannot invert (the row-lock
 * chain above), so there is no window that could produce a visibility
 * GAP in the snapshot sequence.
 *
 * ## What stage A must know about in-flight commits
 *
 * Stage A of commit N+1 reads the database at a snapshot that predates
 * N's COMMIT — N's writes are invisible to it. Everything the checks
 * could miss is carried in memory instead, as absolute-value DELTAS
 * ([CatalogPipe.Delta]) registered before the write's unlock executes
 * and merged with max/union semantics, so a delta whose commit HAS
 * landed merges harmlessly with the read that now shows it:
 *
 *  - head snapshot id and next_file_id (allocation stays serial and
 *    dense — this is WHERE it stays serial);
 *  - per-table next_row_id after each in-flight append (the lineage
 *    allocator);
 *  - the paths in-flight commits registered, merged into the upload
 *    claim check's retained set (a settled claim's file row may be
 *    invisible at the read's snapshot);
 *  - idempotency keys in flight: a replay of one waits on the
 *    original's durability and answers from its result — exactly the
 *    receipt answer, deferred — rather than publishing a second commit
 *    that the receipt's PRIMARY KEY would turn into a 500.
 *
 * Deltas are pruned when a stage A begins: every commit whose stage B
 * completed before the current stage A started is visible to that stage
 * A's read and is dropped. The deque is bounded by `depth`.
 *
 * ## What is NOT pipelined, and why
 *
 * Commits carrying DELETES take the lock-to-commit path they always
 * took, plus one statement first: `FOR NO KEY UPDATE` on the catalog
 * row, which waits out every in-flight pipelined commit (each holds
 * that row lock until its COMMIT) — a database-level drain, cross-pod
 * for free. Only then do they read. This is the one place the
 * in-memory delta model would not have been enough: the DV supersession
 * chain, the per-target liveness checks, and — on the other side —
 * compaction's and retirement's "did the DV set move?" commit guards
 * all read COMMITTED state, and a delete registered-but-uncommitted
 * would be invisible to a compaction group retiring its target file
 * (the delete would be silently lost). Deletes are the rare, heavy
 * commits; keeping them serialized keeps every one of those guards
 * exact. Appends have no such readers (appends never conflict with
 * appends, and their only conflict kinds — DDL — are never in flight),
 * which is what makes the append path the safe one to pipeline.
 *
 * ## Cross-pod, and the OCC backstop
 *
 * Two pods pipelining one catalog can interleave a read and a write
 * across the stage-A boundary (the advisory lock excludes their stage
 * As from each other, not one's stage A from the other's stage B).
 * Every commit's write re-validates the allocator values it read
 * AFTER the row-lock wait (key-only quals + RETURNING counts), so the
 * pod whose write reaches the hog_catalog row SECOND blocks on the
 * winner's row lock, fails the post-wait check, rolls back everything
 * it wrote, and retries whole against a fresh (post-drain) read —
 * [CommitTail.AllocationMoved]. OCC is preserved exactly: the loser's
 * conflict checks re-run against state that includes the winner.
 * Bounded attempts; exhaustion is the typed, retryable
 * CommitQueueTimeout the admission contract already uses. Within one
 * pod the only reachable trigger is a rolled-back in-flight
 * predecessor (its delta was merged; its rows never land) — same
 * retry, and [CommitPipeline.CatalogPipe.failDelta] has already
 * unpublished the phantom allocation.
 *
 * ## Adaptive depth
 *
 * Depth starts at 1, where the pipeline is EXACTLY the old path: the
 * permit is held across the COMMIT, so the next commit's stage A cannot
 * start until the previous COMMIT returned — and commits that find the
 * catalog at rest (depth 1, nothing in flight) take the original
 * xact-locked tail verbatim, no session lock, no unlock statement. A
 * commit that arrives at a FULL pipeline doubles the depth (to the cap)
 * and enters through the slot the doubling opened; only at the cap does
 * a contender actually wait. A catalog whose pipeline drains (nothing
 * in flight, nothing queued) decays: halved when the drained cycle
 * contended, straight to 1 when it did not — one quiet commit after a
 * lull is the old path again. Quiet catalogs are byte-for-byte the old
 * behavior; the machinery only exists while contention does.
 *
 * `CommitPipelineTest` pins the adaptation arithmetic;
 * `CommitPipelineIntegrationTest` pins ordering, density, OCC,
 * idempotent replay and durability under pipelining.
 */
internal class CommitPipeline(
    maxInFlight: Int,
) {
    /** The cap, clamped: past it the pool, not the pipeline, is the limit. */
    private val cap = maxInFlight.coerceIn(1, MAX_DEPTH)

    private val pipes = ConcurrentHashMap<Long, CatalogPipe>()

    /** The per-catalog pipeline, created on first use. */
    fun pipe(catalogId: Long): CatalogPipe = pipes.computeIfAbsent(catalogId) { CatalogPipe(cap) }

    /**
     * Drop [pipe] from the map when it is at rest, so the map does not
     * grow with the catalog count. Retirement is atomic with admission:
     * the pipe is marked retired under its monitor BEFORE the map
     * remove, so a thread that already holds the reference but has not
     * yet acquired gets [CatalogPipe.Acquire.RETIRED] from
     * [CatalogPipe.acquire] and re-looks-up the fresh pipe — two live
     * pipes for one catalog would split the depth budget, the deltas
     * and the deferred receipts.
     */
    fun retireIfIdle(
        catalogId: Long,
        pipe: CatalogPipe,
    ) {
        if (pipe.markRetiredIfAtRest()) pipes.remove(catalogId, pipe)
    }

    companion object {
        /** Hard ceiling on per-catalog in-flight commits, whatever is configured. */
        const val MAX_DEPTH: Int = 64
    }

    /** How one commit goes through the pipeline, chosen at admission. */
    internal enum class Path {
        /**
         * The pre-pipeline tail, verbatim: xact-scoped lock held to
         * COMMIT. Chosen when the catalog is at rest (depth 1, nothing
         * in flight) — quiet catalogs keep the old code path exactly.
         */
        LOCKED,

        /**
         * Session lock + drain (`FOR NO KEY UPDATE` on the catalog row)
         * + the legacy tail, COMMIT under the lock. For commits carrying
         * deletes — see the class KDoc — and for retried commits, whose
         * first attempt proved another writer is in flight: after the
         * drain the read is authoritative and no deltas are needed.
         */
        DRAINED,

        /**
         * Stage A under the session-scoped lock with the in-flight
         * deltas merged, the lock released by an explicit statement
         * right after the write returns; stage B is the COMMIT,
         * outside the serialization.
         */
        PIPELINED,
    }

    /**
     * One catalog's pipeline state: the adaptive depth, the in-flight
     * permit count, and the not-yet-visible commits' deltas.
     *
     * The depth/permit fields are guarded by [monitor]; the delta deque
     * and key map are mutated from stage A (serialized by the database
     * advisory lock) and from stage-B completions, and are always
     * touched under [monitor] too — the deque is also read under it.
     */
    internal class CatalogPipe(
        private val cap: Int,
    ) {
        private val monitor = Object()

        /** Commits admitted and not yet durable (stage B incomplete). */
        private var inFlight = 0

        /** Commits blocked in [acquire]. */
        private var waiters = 0

        /** Set by [markRetiredIfAtRest]; a retired pipe never admits again. */
        private var retired = false

        /** True when the current burst actually contended; an uncontended drain decays depth straight to 1. */
        private var sawContention = false

        /** Current in-flight depth limit; 1 at rest, doubles under pressure, decays when drained. */
        @Volatile
        internal var depth = 1
            private set

        /** In stage-A order; the back is the newest allocation. */
        private val deltas = ArrayDeque<Delta>()

        /** In-flight commits by idempotency key, for the deferred-receipt replay. */
        private val byKey = HashMap<UUID, Delta>()

        private var nextSeq = 0L

        /**
         * Mark the pipe retired when it is at rest (nothing in flight,
         * nobody waiting, depth decayed to 1 — the state a fresh pipe
         * would have). Atomic with [acquire] under [monitor]: after
         * this returns true, every future [acquire] on this instance
         * answers [Acquire.RETIRED].
         */
        fun markRetiredIfAtRest(): Boolean =
            synchronized(monitor) {
                if (inFlight == 0 && waiters == 0 && depth == 1 && !retired) {
                    retired = true
                }
                retired
            }

        /** Outcome of [acquire]; RETIRED sends the caller back to the map for the fresh pipe. */
        internal enum class Acquire { ACQUIRED, TIMED_OUT, RETIRED }

        /**
         * Take an in-flight permit. A commit that arrives at a FULL
         * pipeline doubles the depth (to the cap) and enters through the
         * slot the doubling opened — queueing behind an in-flight
         * commit's flush is exactly when widening helps, and the
         * widened slot is the help. Only at the cap does the caller
         * actually wait; [timeoutMs] <= 0 waits unbounded (the
         * configured admission bound's documented meaning); expiry is
         * [Acquire.TIMED_OUT], which the caller maps to the typed
         * CommitQueueTimeout, the same 503 the lock wait produces.
         */
        fun acquire(timeoutMs: Long): Acquire {
            val deadline = if (timeoutMs > 0) System.nanoTime() + timeoutMs * 1_000_000 else 0L
            synchronized(monitor) {
                while (true) {
                    if (retired) return Acquire.RETIRED
                    if (inFlight < depth) {
                        inFlight++
                        return Acquire.ACQUIRED
                    }
                    sawContention = true
                    if (depth < cap) {
                        depth = minOf(cap, depth * 2)
                        continue
                    }
                    waiters++
                    try {
                        if (deadline == 0L) {
                            monitor.wait()
                        } else {
                            val remainMs = (deadline - System.nanoTime()) / 1_000_000
                            if (remainMs <= 0) return Acquire.TIMED_OUT
                            monitor.wait(remainMs)
                        }
                    } finally {
                        waiters--
                    }
                }
            }
        }

        /**
         * Return a permit at stage-B completion. When the pipeline
         * DRAINED (nothing in flight, nobody waiting) the depth decays:
         * halved when the drained cycle contended, straight to 1 when it
         * did not — one quiet commit after an uncontended lull takes the
         * lock-to-commit path again instead of lingering on the
         * session-lock path for log2(depth) commits.
         */
        fun release() {
            synchronized(monitor) {
                inFlight--
                if (inFlight == 0 && waiters == 0 && depth > 1) {
                    depth = if (sawContention) maxOf(1, depth / 2) else 1
                    sawContention = false
                }
                if (inFlight == 0 && deltas.isNotEmpty()) {
                    // Every delta's stage B has completed: nothing a
                    // future read could miss remains. Their receipts
                    // are durable now, so the deferred-replay map is
                    // dropped with them (a replay finds the receipt in
                    // the database, as before).
                    deltas.forEach { d -> d.idempotencyKey?.let { byKey.remove(it, d) } }
                    deltas.clear()
                }
                monitor.notifyAll()
            }
        }

        /**
         * Which way this commit goes through. Chosen AFTER [acquire], so
         * [inFlight] includes the caller: a catalog at rest (depth 1, the
         * caller alone) gets [Path.LOCKED], the pre-pipeline tail.
         */
        fun pathFor(hasDeletes: Boolean): Path =
            synchronized(monitor) {
                when {
                    hasDeletes -> Path.DRAINED
                    inFlight == 1 && depth == 1 -> Path.LOCKED
                    else -> Path.PIPELINED
                }
            }

        /**
         * The in-flight replay check: the delta an earlier stage A
         * registered under [key], or null. Atomic against registration
         * (both under [monitor]), so a replay either finds the in-flight
         * original or is guaranteed the original committed before the
         * replay's own read could start.
         */
        fun inFlightReceipt(key: UUID): Delta? = synchronized(monitor) { byKey[key] }

        /**
         * Drop the deltas whose stage B completed — a stage A is about
         * to read, and everything those commits wrote is visible to a
         * statement that starts now. Called at stage-A entry.
         */
        fun pruneDeltas() {
            synchronized(monitor) {
                while (deltas.isNotEmpty() && deltas.first().result.isDone) {
                    val d = deltas.removeFirst()
                    d.idempotencyKey?.let { byKey.remove(it, d) }
                }
            }
        }

        /** The merged view a pipelined stage A applies over its read. See [Delta] for the merge rule. */
        fun view(): View =
            synchronized(monitor) {
                View(
                    headSnapshotId = deltas.maxOfOrNull { it.snapshotId },
                    nextFileId = deltas.maxOfOrNull { it.nextFileIdAfter },
                    nextRowIds =
                        deltas
                            .flatMap { it.nextRowIdsAfter.asSequence() }
                            .groupBy({ it.key }, { it.value })
                            .mapValues { (_, v) -> v.max() },
                    registeredPaths = deltas.flatMapTo(HashSet()) { it.registeredPaths },
                )
            }

        /**
         * Register the commit's allocations BEFORE the write statement
         * runs: the explicit unlock right after the write frees the
         * next stage A, and the delta is what that stage A merges, so
         * registration must win the race against the release. Returns
         * the delta with its sequence number — stage-A order — assigned.
         *
         * Runs inside stage A (serialized per catalog by the database
         * lock), which is what makes the idempotency-key re-check here
         * atomic: two commits sharing a key can both pass the driver's
         * lock-free pre-check, but the second one's stage A runs after
         * the first registered, and throwing [DeferredReplay] sends it
         * to wait on the original's durability instead of publishing a
         * second receipt the PRIMARY KEY would have turned into a 500.
         */
        fun registerDelta(
            snapshotId: Long,
            nextFileIdAfter: Long,
            nextRowIdsAfter: Map<Long, Long>,
            registeredPaths: Set<Pair<String, String>>,
            idempotencyKey: UUID?,
            fingerprint: ByteArray?,
        ): Delta =
            synchronized(monitor) {
                idempotencyKey?.let { key -> byKey[key]?.let { throw DeferredReplay(it) } }
                val delta =
                    Delta(
                        seq = nextSeq++,
                        snapshotId = snapshotId,
                        nextFileIdAfter = nextFileIdAfter,
                        nextRowIdsAfter = nextRowIdsAfter,
                        registeredPaths = registeredPaths,
                        idempotencyKey = idempotencyKey,
                        fingerprint = fingerprint,
                    )
                deltas.addLast(delta)
                idempotencyKey?.let { byKey[it] = delta }
                delta
            }

        /**
         * Stage A found its idempotency key already registered by an
         * in-flight commit (see [registerDelta]). Internal to the
         * driver: caught, and the request waits on [original]'s
         * durability for the receipt answer.
         */
        internal class DeferredReplay(val original: Delta) : RuntimeException()

        /** Stage B completed: the commit is durable. Replay waiters get its result. */
        fun completeDelta(
            delta: Delta,
            result: CommitResult,
        ) {
            delta.result.complete(result)
        }

        /**
         * The commit failed — stage A after registration (the write's
         * guards moved: the allocation never happened) or stage B (the
         * COMMIT rolled back). Either way the delta must not survive to
         * be merged by a successor, and a replay waiting on it falls
         * through to publish its own commit.
         */
        fun failDelta(
            delta: Delta,
            error: Throwable,
        ) {
            synchronized(monitor) {
                deltas.remove(delta)
                delta.idempotencyKey?.let { byKey.remove(it, delta) }
            }
            delta.result.completeExceptionally(error)
        }

        /**
         * One in-flight commit's allocations — the part of the catalog's
         * state a pipelined stage A cannot see in the database because
         * this commit has not COMMITted yet.
         *
         * MERGE RULE: every value is ABSOLUTE (the state AFTER this
         * commit), never a delta to apply, and merges by max/union. A
         * commit that has already landed is therefore harmless to keep:
         * the read that shows its rows yields the same values the merge
         * does. That is what makes the prune timing (stage-A entry)
         * sufficient without knowing exactly which commits a given read
         * snapshot includes.
         */
        internal class Delta(
            val seq: Long,
            /** The snapshot this commit publishes (head after it). */
            val snapshotId: Long,
            /** next_file_id after this commit's files. */
            val nextFileIdAfter: Long,
            /** table_id -> next_row_id after this commit's appends to it. */
            val nextRowIdsAfter: Map<Long, Long>,
            /** (path, file kind) this commit registered. */
            val registeredPaths: Set<Pair<String, String>>,
            val idempotencyKey: UUID?,
            val fingerprint: ByteArray?,
        ) {
            /** Stage B's outcome; replays of [idempotencyKey] wait on it. Completed exactly once. */
            val result = CompletableFuture<CommitResult>()
        }

        /** What [view] hands a pipelined stage A; null/empty fields mean "nothing in flight adds to the read". */
        internal class View(
            val headSnapshotId: Long?,
            val nextFileId: Long?,
            val nextRowIds: Map<Long, Long>,
            val registeredPaths: Set<Pair<String, String>>,
        )
    }
}
