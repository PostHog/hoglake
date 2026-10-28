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
 *    cross-pod stage As off each other. The lock is released by the
 *    write statement itself ([CommitTail.PIPELINED_WRITE_SQL]) — before
 *    COMMIT.
 *  - **Stage B (overlapped).** The JDBC COMMIT — the flush wait — runs
 *    after the lock is gone, on the commit's own connection, with up to
 *    `depth` commits of one catalog in flight at once. Postgres group
 *    commit batches the flushes of concurrently-committing backends,
 *    so the slow part overlaps instead of convoying.
 *
 * The durability contract is UNCHANGED: a commit's response is sent
 * only after its own COMMIT returns. Pipelining overlaps the waits; it
 * never acks early.
 *
 * ## Why the order is still the order
 *
 * Stage A assigns snapshot ids serially, and stage B cannot reorder
 * them: every commit's write UPDATEs the catalog's hog_catalog row
 * (the guarded `alloc` CTE), a row lock held until COMMIT, so commit
 * N+1's write cannot execute until commit N's COMMIT has completed.
 * The WAL sees the commits in stage-A order, a reader never observes
 * snapshot N+1 without N, and snapshot ids stay dense and ordered with
 * commit order — the lineage premise, intact. What pipelining buys is
 * that N+1's read and checks (and N+2's, ...) run DURING N's flush
 * instead of after it: the serial residue per commit is the write plus
 * the flush, not the whole tail.
 *
 * READ-VISIBILITY NOTE. Because stage A ends before the flush, a
 * maintenance or DDL tail on this catalog (advisory-lock holders all)
 * can now commit in the window between a commit's stage A and its
 * COMMIT, and so become visible to readers a few milliseconds before
 * that commit does. Both orders were always reachable lock-race
 * outcomes — the database state that results is identical; what changed
 * is which side of the race the reader observes first. Commit-to-commit
 * order cannot invert (the row-lock chain above), so this window can
 * never produce a visibility GAP in the snapshot sequence.
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
 * Every commit's write is guarded on the allocator values it read, so
 * the pod whose write reaches the hog_catalog row SECOND fails its
 * guard, rolls back having written nothing, and retries whole against
 * a fresh (post-drain) read — [CommitTail.AllocationMoved]. OCC is
 * preserved exactly: the loser's conflict checks re-run against state
 * that includes the winner. Bounded attempts; exhaustion is the typed,
 * retryable CommitQueueTimeout the admission contract already uses.
 * Within one pod this race is unreachable (stage A is serial and the
 * deltas make the read whole).
 *
 * ## Adaptive depth
 *
 * Depth starts at 1, where the pipeline is EXACTLY the old path: the
 * permit is held across the COMMIT, so the next commit's stage A cannot
 * start until the previous COMMIT returned — and commits that find the
 * catalog at rest (depth 1, nothing in flight) take the original
 * xact-locked tail verbatim, no session lock, no unlock CTE. A commit
 * that has to WAIT for a permit doubles the depth (to the cap); a
 * catalog whose pipeline drains (nothing in flight, nothing queued)
 * halves it back toward 1. Quiet catalogs are byte-for-byte the old
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
     * grow with the catalog count. A racing submit gets a fresh pipe at
     * depth 1 with no deltas — the state a retired pipe had, so the swap
     * is invisible.
     */
    fun retireIfIdle(
        catalogId: Long,
        pipe: CatalogPipe,
    ) {
        if (pipe.atRest()) pipes.remove(catalogId, pipe)
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
         * deltas merged, the write releasing the lock; stage B is the
         * COMMIT, outside the serialization.
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

        /** Current in-flight depth limit; 1 at rest, doubles under pressure, halves when drained. */
        @Volatile
        internal var depth = 1
            private set

        /** In stage-A order; the back is the newest allocation. */
        private val deltas = ArrayDeque<Delta>()

        /** In-flight commits by idempotency key, for the deferred-receipt replay. */
        private val byKey = HashMap<UUID, Delta>()

        private var nextSeq = 0L

        /**
         * True when nothing is in flight, nothing waits, and the depth
         * has decayed to 1 — the state a fresh pipe would have, so the
         * map entry can be dropped.
         */
        fun atRest(): Boolean =
            synchronized(monitor) {
                inFlight == 0 && waiters == 0 && depth == 1
            }

        /**
         * Take an in-flight permit, waiting while [inFlight] >= [depth]
         * and DOUBLING the depth (to the cap) on contention — a wait
         * means commits are queueing behind the flush of the one ahead,
         * which is exactly when widening helps. [timeoutMs] <= 0 waits
         * unbounded (the configured admission bound's documented
         * meaning); expiry returns false and the caller maps it to the
         * typed CommitQueueTimeout, the same 503 the lock wait produces.
         */
        fun acquire(timeoutMs: Long): Boolean {
            val deadline = if (timeoutMs > 0) System.nanoTime() + timeoutMs * 1_000_000 else 0L
            synchronized(monitor) {
                while (inFlight >= depth) {
                    if (depth < cap) depth = minOf(cap, depth * 2)
                    waiters++
                    try {
                        if (deadline == 0L) {
                            monitor.wait()
                        } else {
                            val remainMs = (deadline - System.nanoTime()) / 1_000_000
                            if (remainMs <= 0) return false
                            monitor.wait(remainMs)
                        }
                    } finally {
                        waiters--
                    }
                }
                inFlight++
                return true
            }
        }

        /**
         * Return a permit at stage-B completion. When the pipeline
         * DRAINED (nothing in flight, nobody waiting) the depth halves
         * toward 1 — the burst is over, and the next quiet commit takes
         * the lock-to-commit path again.
         */
        fun release() {
            synchronized(monitor) {
                inFlight--
                if (inFlight == 0 && waiters == 0 && depth > 1) {
                    depth = maxOf(1, depth / 2)
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
         * (whose unlock CTE frees the next stage A) runs: the delta is
         * what the next stage A merges, so it must be visible no later
         * than the lock release the write performs. Returns the delta
         * with its sequence number — stage-A order — assigned.
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
