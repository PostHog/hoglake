package com.posthog.hoglake.persistence

import com.posthog.hoglake.RequestAdmission
import com.posthog.hoglake.model.HoglakeException
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.statement.Cleanable
import org.jdbi.v3.core.statement.UnableToExecuteStatementException

/**
 * Advisory-lock discipline for the catalog. Every DDL / commit tail in
 * the codebase serializes writers per catalog through
 * [acquireCatalogCommitLock]; the lock is transaction-scoped
 * (pg_advisory_xact_lock) so release is tied to commit/rollback and can
 * never be leaked by a code path that forgets to unlock.
 */
object Locks {
    /**
     * Discriminator for the per-catalog commit lock: the upper 32 bits
     * of the single-bigint advisory lock key. The lower 32 bits are the
     * catalog_id (masked to 32 bits, so the key stays collision-free
     * until 2^32 catalogs). The two-int lock form is NOT used: its
     * second argument is an int4, which errors for catalog_id > 2^31.
     */
    const val CATALOG_COMMIT_LOCK_CLASS: Int = 4740871

    /**
     * Discriminator for the per-catalog RETIREMENT lock: a distinct
     * class, so a retirement sweep excluding its own siblings can never
     * collide with the commit lock's key space.
     *
     * Held SESSION-scoped ([tryAcquireCatalogRetirementLock]), not
     * transaction-scoped, because a retirement run is MANY transactions
     * — one per batch — and an xact lock would be released and re-taken
     * between every pair of them, which is not single flight at all.
     */
    const val CATALOG_RETIREMENT_LOCK_CLASS: Int = 4740872

    /**
     * Separate from commit and retirement locks. Held only across a
     * compaction plan's CLAIM transaction (the in-lock claim re-read and
     * the claim inserts), never across its probes or packing — #418,
     * `CompactionService.planAndClaim` has why.
     */
    const val CATALOG_COMPACTION_PLAN_LOCK_CLASS: Int = 4740873

    /**
     * Discriminator for the INSTANCE-WIDE reindex lock (#268): the low 32
     * bits are always 0, because an index belongs to the database, not
     * to a catalog. SESSION-scoped, held by the connection that issues
     * the REINDEX for the whole run, so a second maintainer configured by
     * mistake skips rather than rebuilds the same index beside it.
     */
    const val REINDEX_LOCK_CLASS: Int = 4740874

    /**
     * Take the session lock inside a short transaction with a bounded wait.
     * The caller must release it on this handle after its claim transaction
     * commits or rolls back. Session-scoped so it can be taken in its own
     * transaction ahead of the claim transaction, so a timed-out wait fails
     * the plan before any claim work and is never released by a caller that
     * did not get it. The wait is bounded by the 15 s `lock_timeout` set
     * here, and it queues only behind siblings' claim transactions, which
     * run under `CompactionService.CLAIM_STATEMENT_TIMEOUT_MS` — the same
     * 15 s, deliberately: a waiter never outlives the holder's bound.
     */
    fun acquireCatalogCompactionPlanLock(
        handle: Handle,
        catalogId: Long,
    ) {
        handle.execute("SET LOCAL lock_timeout = '15s'")
        handle.execute("SET LOCAL statement_timeout = '15s'")
        handle.createQuery(
            "SELECT pg_advisory_lock(($CATALOG_COMPACTION_PLAN_LOCK_CLASS::bigint << 32) | " +
                "(?::bigint & 4294967295))",
        )
            .bind(0, catalogId)
            .mapToMap()
            .one()
    }

    fun releaseCatalogCompactionPlanLock(
        handle: Handle,
        catalogId: Long,
    ) {
        try {
            handle.createQuery(
                "SELECT pg_advisory_unlock(($CATALOG_COMPACTION_PLAN_LOCK_CLASS::bigint << 32) | " +
                    "(?::bigint & 4294967295))",
            )
                .bind(0, catalogId)
                .mapTo(Boolean::class.javaObjectType)
                .one()
        } catch (e: Exception) {
            // A pooled connection must never retain this session lock.
            handle.connection.abort { it.run() }
            throw e
        }
    }

    /**
     * Take the per-catalog commit lock for the current transaction.
     * Blocks until the holder commits or rolls back. Must be called
     * inside an open transaction (xact-scoped locks are meaningless
     * outside one; Postgres raises an error).
     *
     * The lock key MUST be computed identically everywhere — any copy of
     * this SQL (tests probing contention included) has to build the same
     * `(class << 32) | (catalog_id & 0xFFFFFFFF)` bigint, or commit
     * serialization silently breaks (two writers would take DIFFERENT
     * locks for the same catalog and interleave the commit tail).
     *
     * Observability + admission control (B2, #439):
     *  - the wait is always recorded into the
     *    `hoglake_commit_lock_wait_seconds` histogram (commit path and
     *    every DDL/maintenance tail alike), and the session is counted
     *    in `hoglake_commit_lock_waiting{catalog}` from before the lock
     *    statement until it is granted or throws;
     *  - from the grant to the END OF THE TRANSACTION, the hold is
     *    counted in `hoglake_commit_lock_held{catalog}` and then recorded
     *    into `hoglake_commit_lock_hold_seconds{catalog,path}`, [path]
     *    saying which kind of work held it ([CommitLockPath]). [catalog]
     *    is the catalog's NAME, the label every other per-catalog series
     *    carries; each caller has it from the same row as [catalogId].
     *
     * WHY THE HOLD ENDS ON THE HANDLE'S HOOKS, NOT IN A WRAPPER OR A
     * TOKEN. The lock is transaction-scoped, so its release is the
     * transaction's COMMIT or ROLLBACK, and both alternatives put that
     * moment in the callers' hands:
     *  - a `withCatalogCommitLock(h, ...) { }` block running INSIDE the
     *    caller's transaction returns before the COMMIT, so it measures
     *    to the wrong point; one that OWNS the transaction would have to
     *    restructure most of the fourteen call sites, which take the
     *    lock mid-transaction after pre-lock reads (`CommitService`), or
     *    re-entrantly inside another acquirer's transaction
     *    (`TableCreationService.publish` -> `CatalogService.createTable`);
     *  - a token returned here and settled in each caller's `finally`
     *    is fourteen `finally`s placed OUTSIDE fourteen transactions,
     *    and the fifteenth caller that forgets one is a held gauge
     *    stuck at 1 that nothing reds.
     * JDBI already knows when the transaction ends: [CommitLockObserver]
     * settles the hold on the handle's `afterCommit` / `afterRollback`,
     * with a close-time `Cleanable` as the backstop. The release is
     * registered by the acquire itself, so there is no caller code to
     * miss, and "every caller is covered" reduces to "every lock goes
     * through this function", which the private [lockStatement] and
     * `CommitLockCallSitesTest` pin.
     *
     * Admission control:
     *  - [lockTimeoutMs] > 0 sets a transaction-local `lock_timeout`
     *    (via set_config(..., is_local => true), so it dies with the
     *    transaction) BEFORE queuing; a timeout expiry surfaces as
     *    [HoglakeException.CommitQueueTimeout] — typed, retryable
     *    backpressure, mapped to HTTP 503 — never a generic failure.
     *    0 (the default) leaves the wait unbounded, exactly the old
     *    behavior. The timeout stays in force for the rest of the
     *    transaction, so a pathological row-lock convoy later in the
     *    tail is bounded by the same admission contract.
     *
     * THE BOUND IS CHARGED THE TIME THE REQUEST ALREADY SPENT QUEUED
     * (#218). Since route handlers run on a bounded dispatcher rather
     * than the Netty event loop, a request can wait for a handler
     * thread before it ever reaches this line, and the admission
     * contract promises ONE bounded wait, not one per queue: a commit
     * that queued 25 s for a thread and then waited the full 30 s here
     * would spend 55 s of server time on a caller that has gone.
     * [RequestAdmission.remainingLockTimeoutMs] subtracts the queue
     * wait, floored at [RequestAdmission.MIN_REMAINING_LOCK_TIMEOUT_MS]
     * so a commit never arrives with a bound too small to be worth
     * trying. It is applied HERE rather than at the seven call sites
     * because the subtraction is one rule, and a call site that forgot
     * it would be a path where the admission bound silently doubled.
     * Background loops carry no ambient request, so their bound is
     * unchanged — they never queued for a handler thread.
     */
    fun acquireCatalogCommitLock(
        handle: Handle,
        catalogId: Long,
        catalog: String,
        path: CommitLockPath,
        lockTimeoutMs: Long = 0,
    ) {
        val effectiveTimeoutMs = RequestAdmission.remainingLockTimeoutMs(lockTimeoutMs)
        if (effectiveTimeoutMs > 0) {
            handle.createQuery("SELECT set_config('lock_timeout', ?, true)")
                .bind(0, effectiveTimeoutMs.toString())
                .mapToMap()
                .one()
        }
        CommitLockObserver.observe(catalogId, catalog, path, HandleEnd(handle)) {
            lockStatement(handle, catalogId, lockTimeoutMs, effectiveTimeoutMs)
        }
    }

    /**
     * The one statement that takes the per-catalog commit lock. PRIVATE:
     * reachable only through [acquireCatalogCommitLock], so no caller can
     * take the lock outside the observer. `CommitLockCallSitesTest` pins
     * that this SQL appears nowhere else in the main sources.
     */
    private fun lockStatement(
        handle: Handle,
        catalogId: Long,
        lockTimeoutMs: Long,
        effectiveTimeoutMs: Long,
    ) {
        try {
            handle.createQuery(
                "SELECT pg_advisory_xact_lock(($CATALOG_COMMIT_LOCK_CLASS::bigint << 32) | (?::bigint & 4294967295))",
            )
                .bind(0, catalogId)
                .mapToMap()
                .one()
        } catch (e: UnableToExecuteStatementException) {
            if (Pg.isLockTimeout(e)) {
                throw HoglakeException.CommitQueueTimeout(
                    "commit admission timed out after ${effectiveTimeoutMs}ms waiting for the " +
                        "catalog commit lock (catalog_id=$catalogId); the catalog is busy — retry" +
                        queueWaitSuffix(lockTimeoutMs, effectiveTimeoutMs),
                )
            }
            throw e
        }
    }

    /**
     * The handle's transaction end, for [CommitLockObserver]. Three
     * routes, because the observer settles once and a hold that is never
     * settled is a held gauge stuck at 1: JDBI's `afterCommit` and
     * `afterRollback` (every `inTransaction`, and a hand-rolled
     * `begin`/`commit`), and a `Cleanable` that runs when the handle
     * closes — the backstop for a transaction that ended without either
     * hook firing (a failed `COMMIT`, an aborted connection). A
     * transaction rewound to a SAVEPOINT taken before the lock is not
     * seen as an end; no caller does that today.
     */
    private class HandleEnd(private val handle: Handle) : CommitLockObserver.TransactionEnd {
        override val owner: Any get() = handle
        override val open: Boolean get() = handle.isInTransaction

        override fun onEnd(settle: () -> Unit) {
            val onClose = Cleanable { settle() }
            val once = {
                settle()
                // A long-lived handle runs many transactions; without this
                // its cleanable set would grow by one per lock taken.
                handle.removeCleanable(onClose)
            }
            handle.afterCommit(once)
            handle.afterRollback(once)
            handle.addCleanable(onClose)
        }
    }

    /**
     * Names the queue wait in the 503 when it shortened the bound, so an
     * operator reading "timed out after 4000ms" against a 30 s setting
     * is not left to wonder which knob lied.
     */
    private fun queueWaitSuffix(
        configuredMs: Long,
        effectiveMs: Long,
    ): String =
        if (effectiveMs >= configuredMs) {
            ""
        } else {
            " (the configured ${configuredMs}ms admission bound less the " +
                "${configuredMs - effectiveMs}ms this request already spent queued for a handler thread)"
        }

    /**
     * Try to take the per-catalog retirement lock on [handle]'s SESSION.
     * Returns false immediately when another maintainer holds it.
     *
     * SINGLE FLIGHT, NOT A QUEUE, and that distinction is the whole
     * reason this is `pg_try_advisory_lock` rather than a blocking wait.
     * A retirement run takes the per-catalog COMMIT lock once per batch;
     * with W maintainers queued on one catalog a foreground commit's p99
     * tax is `(W - 0.5) x hold` — Postgres's lock queue is FIFO, so
     * nobody starves, but everybody waits behind every holder. A second
     * maintainer that simply SKIPS the catalog costs nothing and loses
     * nothing: the work is idempotent and the next interval picks it up.
     *
     * The lock lives on the CONNECTION, so the caller must hold
     * [handle] open for the whole run and release it through
     * [releaseCatalogRetirementLock] in a `finally`. A session lock
     * survives a rolled-back batch — which is exactly what is wanted,
     * since the run continues — and dies with the connection if the pod
     * does, so a killed maintainer costs one interval and nothing else.
     */
    fun tryAcquireCatalogRetirementLock(
        handle: Handle,
        catalogId: Long,
    ): Boolean =
        handle.createQuery(
            "SELECT pg_try_advisory_lock(($CATALOG_RETIREMENT_LOCK_CLASS::bigint << 32) | " +
                "(?::bigint & 4294967295))",
        )
            .bind(0, catalogId)
            .mapTo(Boolean::class.javaObjectType)
            .one()

    /**
     * Try to take the instance-wide reindex lock on [handle]'s SESSION;
     * false at once when another maintainer holds it. The caller holds
     * [handle] for the run and releases through [releaseReindexLock] in a
     * `finally`; a killed pod's lock dies with its connection.
     */
    fun tryAcquireReindexLock(handle: Handle): Boolean =
        handle.createQuery("SELECT pg_try_advisory_lock($REINDEX_LOCK_CLASS::bigint << 32)")
            .mapTo(Boolean::class.javaObjectType)
            .one()

    /** Release what [tryAcquireReindexLock] took, on the same session. */
    fun releaseReindexLock(handle: Handle) {
        handle.createQuery("SELECT pg_advisory_unlock($REINDEX_LOCK_CLASS::bigint << 32)")
            .mapTo(Boolean::class.javaObjectType)
            .one()
    }

    /** Release what [tryAcquireCatalogRetirementLock] took, on the same session. */
    fun releaseCatalogRetirementLock(
        handle: Handle,
        catalogId: Long,
    ) {
        handle.createQuery(
            "SELECT pg_advisory_unlock(($CATALOG_RETIREMENT_LOCK_CLASS::bigint << 32) | " +
                "(?::bigint & 4294967295))",
        )
            .bind(0, catalogId)
            .mapTo(Boolean::class.javaObjectType)
            .one()
    }
}
