package com.posthog.hoglake.persistence

import com.posthog.hoglake.RequestAdmission
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.observability.Metrics
import org.jdbi.v3.core.Handle
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
     * The single-bigint advisory lock key for [catalogId], computed
     * exactly as the acquisition SQL below computes it
     * (`(class << 32) | (catalog_id & 0xFFFFFFFF)`). The SQL keeps its
     * inline form (a bound catalog id, no interpolation — invariant 9);
     * this function exists for callers that must name the SAME key as a
     * bound value — `CommitTail.PIPELINED_WRITE_SQL`'s unlock CTE and
     * [releaseCatalogCommitLock] — and a test pins that the two agree.
     * ANY change to one without the other silently breaks commit
     * serialization (the unlock would release a lock nobody holds, and
     * stage A would stop serializing).
     */
    fun catalogCommitLockKey(catalogId: Long): Long =
        (CATALOG_COMMIT_LOCK_CLASS.toLong() shl 32) or (catalogId and 0xFFFFFFFFL)

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
     * Observability + admission control (B2):
     *  - the wait is always recorded into the
     *    `hoglake_commit_lock_wait_seconds` histogram (commit path and
     *    every DDL/maintenance tail alike);
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
        lockTimeoutMs: Long = 0,
    ) = acquireCatalogCommitLock(handle, catalogId, lockTimeoutMs, sessionScoped = false)

    private fun acquireCatalogCommitLock(
        handle: Handle,
        catalogId: Long,
        lockTimeoutMs: Long,
        sessionScoped: Boolean,
    ) {
        val effectiveTimeoutMs = RequestAdmission.remainingLockTimeoutMs(lockTimeoutMs)
        if (effectiveTimeoutMs > 0) {
            handle.createQuery("SELECT set_config('lock_timeout', ?, true)")
                .bind(0, effectiveTimeoutMs.toString())
                .mapToMap()
                .one()
        }
        val start = System.nanoTime()
        try {
            // pg_advisory_lock vs pg_advisory_xact_lock: same key space,
            // same wait semantics; only the release point differs.
            handle.createQuery(
                "SELECT ${if (sessionScoped) "pg_advisory_lock" else "pg_advisory_xact_lock"}(:key)",
            )
                .bind("key", catalogCommitLockKey(catalogId))
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
        } finally {
            Metrics.commitLockWait(System.nanoTime() - start)
        }
    }

    /**
     * [acquireCatalogCommitLock]'s SESSION-scoped twin, for the
     * pipelined commit path (`CommitPipeline`): the lock outlives the
     * transaction it is taken in and is released explicitly —
     * `CommitTail.PIPELINED_WRITE_SQL`'s unlock CTE on success,
     * [releaseCatalogCommitLock] on every other exit — so the
     * per-catalog serialization covers stage A (the read, the checks,
     * the write) and NOT the JDBC COMMIT's flush wait. The wait
     * recording, the [RequestAdmission] budget charge and the
     * `lock_timeout` -> CommitQueueTimeout mapping are identical to the
     * xact-scoped form: queueing for stage A is the admission wait it
     * always was.
     *
     * SESSION scope is a leak risk an xact lock never had: a pooled
     * connection returned with the lock still held would poison every
     * later borrower. The caller's finally must run
     * [releaseCatalogCommitLock] whenever the write's unlock CTE may not
     * have run, and a failed release aborts the connection rather than
     * return it locked.
     */
    fun acquireCatalogCommitLockSession(
        handle: Handle,
        catalogId: Long,
        lockTimeoutMs: Long = 0,
    ) = acquireCatalogCommitLock(handle, catalogId, lockTimeoutMs, sessionScoped = true)

    /**
     * Release what [acquireCatalogCommitLockSession] took, on the same
     * connection. Best-effort about double release (the write's unlock
     * CTE may already have run — Postgres answers false and a WARNING,
     * harmless on a failure path that is about to roll back) but NOT
     * about failure: a connection whose unlock failed is aborted, never
     * returned to the pool still holding the lock.
     */
    fun releaseCatalogCommitLock(
        handle: Handle,
        catalogId: Long,
    ) {
        try {
            handle.createQuery("SELECT pg_advisory_unlock(:key)")
                .bind("key", catalogCommitLockKey(catalogId))
                .mapTo(Boolean::class.javaObjectType)
                .one()
        } catch (e: Exception) {
            // A pooled connection must never retain this session lock.
            handle.connection.abort { it.run() }
            throw e
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
