package com.posthog.hoglake

import com.posthog.hoglake.model.HoglakeException
import kotlinx.coroutines.ThreadContextElement
import kotlinx.coroutines.asContextElement

/**
 * How long THIS request already spent waiting for a handler thread, made
 * reachable from the blocking code the handler calls (#218).
 *
 * ## Why it exists
 *
 * Taking blocking work off the event loop turns starvation into
 * QUEUEING, and a queue nobody accounts for is a second invisible
 * budget. The commit admission contract promises a bounded wait —
 * `HOGLAKE_COMMIT_LOCK_TIMEOUT_MS`, 30 s, then a typed 503 +
 * Retry-After — and a request that spent 25 s in the dispatcher's queue
 * before its handler even started would then wait 30 s MORE on the
 * advisory lock: 55 s of server time, an admission bound that means
 * nothing, and a caller that has almost certainly gone. The queue wait
 * has to be charged against the same budget, which means the code
 * taking the lock has to be able to see it.
 *
 * ## How it travels
 *
 * A `ThreadLocal` mirrored into the coroutine context by
 * [asContextElement], established ONCE at the dispatch seam
 * (`api/BlockingDispatch.kt`). Not a plain `ThreadLocal`: a coroutine
 * inside `withContext(dispatcher)` may resume on a different thread of
 * the same pool after any suspension, so a bare thread-local would be
 * read as null exactly when a handler had done IO. Not a parameter
 * threaded through every service either — that is the ~90-call-site
 * shape the dispatch seam exists to avoid, and the failure mode is a
 * new commit path that forgets it.
 *
 * Background loops have NO ambient value, deliberately: they never
 * queued for a handler thread, so [remainingLockTimeoutMs] returns
 * their configured bound unchanged. The ambient is a property of a
 * REQUEST, and its absence is the correct reading for everything else.
 *
 * Work a request hands to ANOTHER pool carries none either — a
 * compaction sweep at `HOGLAKE_COMPACTION_PARALLEL_GROUPS > 1` commits
 * its groups on executor threads, which the context element does not
 * reach. That is the same reading as a background loop's and it is the
 * right one: those threads did not queue for anything. It does mean a
 * manually triggered sweep's commits are charged differently depending
 * on the knob — the handler thread's own commit carries the (frozen)
 * queue wait, the worker threads' do not — and with the wait frozen at
 * admission the difference is at most the few milliseconds the request
 * actually spent queued, rather than the whole length of the sweep.
 */
class RequestAdmission(
    /** `System.nanoTime()` when the interceptor handed the call to the dispatcher. */
    val enqueuedNanos: Long,
) {
    @Volatile
    private var queueWaitNanos: Long = UNSET

    /**
     * FREEZE the wait, at the moment the call reaches a thread. Called
     * once, by the dispatch seam, before anything else in the handler
     * runs.
     *
     * The frozen value is the whole point. An earlier version computed
     * `now - enqueuedNanos` on every read, which is elapsed-since-
     * dispatch and not queue wait: it grows with THE HANDLER'S OWN
     * EXECUTION. `POST /v1/catalogs/{c}/maintenance/compact` runs a
     * synchronous sweep on the handler thread, so at the default
     * `parallelGroups = 1` every group after the first ~30 s of the run
     * would reach `acquireCatalogCommitLock` with the arithmetic
     * claiming the request had queued for longer than the admission
     * bound — a 1 s lock bound for the rest of the sweep, and a 503
     * blaming a queue wait that never happened. The budget is charged
     * for TIME SPENT WAITING TO START, and that quantity stops changing
     * the moment the handler starts.
     */
    internal fun admit(nowNanos: Long) {
        if (queueWaitNanos == UNSET) queueWaitNanos = nowNanos - enqueuedNanos
    }

    /**
     * How long this call waited for a handler thread. Frozen at
     * admission; zero before it, which no caller can observe (nothing
     * reads an admission before its own handler is running).
     */
    fun queueWaitMs(): Long = queueWaitNanos.let { if (it == UNSET) 0L else it / 1_000_000 }

    companion object {
        /**
         * What a request is left with after its queue wait is charged
         * against the admission bound, however long it queued.
         *
         * A commit that reaches the lock with 40 ms of budget left
         * would 503 on arrival for no useful reason — the lock may be
         * free — so the remainder never falls below this, and never
         * below the CONFIGURED bound when that is smaller still. The
         * cost of the floor is that the worst case is `admission bound
         * + 1 s` rather than exactly the bound; the alternative is a
         * bound that shrinks to zero and refuses work the catalog could
         * have done.
         */
        const val MIN_REMAINING_LOCK_TIMEOUT_MS: Long = 1_000

        private const val UNSET: Long = -1

        internal val holder = ThreadLocal<RequestAdmission?>()

        /** The ambient value, or null outside a dispatched request. */
        fun current(): RequestAdmission? = holder.get()

        /**
         * The context element the dispatch seam adds for [admission];
         * see the class comment for why it is a context element and not
         * a bare thread-local.
         */
        fun element(admission: RequestAdmission): ThreadContextElement<RequestAdmission?> =
            holder.asContextElement(admission)

        /**
         * The `lock_timeout` a commit-lock acquirer should actually use:
         * [configured] less whatever this request already spent queued,
         * floored at [MIN_REMAINING_LOCK_TIMEOUT_MS].
         *
         * Applied inside `Locks.acquireCatalogCommitLock` rather than at
         * each of its call sites, because there are seven of them and
         * the subtraction is one rule; a call site that forgot it would
         * be a path where the admission bound silently doubles.
         *
         * `configured <= 0` (an unbounded wait, the documented meaning
         * of `HOGLAKE_COMMIT_LOCK_TIMEOUT_MS=0`) is returned unchanged:
         * there is no budget to charge against.
         */
        fun remainingLockTimeoutMs(
            configured: Long,
            admission: RequestAdmission? = current(),
        ): Long {
            if (configured <= 0 || admission == null) return configured
            // The floor is itself capped at the configured bound. A
            // `maxOf(MIN, ...)` alone would RAISE a bound set below the
            // floor — a deployment (or a test) that configures 250 ms
            // would silently get 1 s — which is the opposite of what a
            // floor is for: it exists so a request does not arrive with
            // a useless remainder, never to overrule the operator.
            val floor = minOf(MIN_REMAINING_LOCK_TIMEOUT_MS, configured)
            return maxOf(floor, configured - admission.queueWaitMs())
        }

        /**
         * Shed a request whose queue wait has already exhausted the
         * admission bound, BEFORE it borrows a pooled connection.
         *
         * The bound is the commit admission bound because that is the
         * only wall-clock promise this service publishes to callers: a
         * request that has already outlived it has outlived the promise
         * it was made, and its client is very likely gone. Answering
         * the same typed `CommitQueueTimeout` a queued commit would get
         * means the response is the documented 503 + Retry-After rather
         * than a new shape clients have never seen — and it costs no
         * connection, no lock wait and no transaction, which is the
         * whole point of shedding here rather than discovering it
         * later.
         *
         * It applies to EVERY dispatched route, not only commits. A
         * `GET` that waited past the admission bound is a `GET` whose
         * caller has gone too, and serving it spends a handler thread
         * the requests still waiting need.
         *
         * `budgetMs <= 0` disables shedding, matching
         * `HOGLAKE_COMMIT_LOCK_TIMEOUT_MS=0`'s "unbounded" meaning.
         *
         * [queueExhausted] is the same predicate without the throw, so
         * the seam can COUNT a shed before raising it rather than
         * restating the comparison next to the counter — the shape in
         * which a metric and the behaviour it claims to measure drift
         * apart.
         */
        fun queueExhausted(
            budgetMs: Long,
            queueWaitMs: Long,
        ): Boolean = budgetMs > 0 && queueWaitMs >= budgetMs

        /** Throws [HoglakeException.CommitQueueTimeout] when [queueExhausted]; see above. */
        fun refuseIfQueueExhausted(
            budgetMs: Long,
            queueWaitMs: Long,
        ) {
            if (!queueExhausted(budgetMs, queueWaitMs)) return
            throw HoglakeException.CommitQueueTimeout(
                "request waited ${queueWaitMs}ms for a handler thread, past the ${budgetMs}ms " +
                    "admission bound (HOGLAKE_COMMIT_LOCK_TIMEOUT_MS); the instance is saturated " +
                    "— retry",
            )
        }
    }
}
