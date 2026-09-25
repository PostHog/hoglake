package com.posthog.hoglake.persistence

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
     */
    fun acquireCatalogCommitLock(
        handle: Handle,
        catalogId: Long,
        lockTimeoutMs: Long = 0,
    ) {
        if (lockTimeoutMs > 0) {
            handle.createQuery("SELECT set_config('lock_timeout', ?, true)")
                .bind(0, lockTimeoutMs.toString())
                .mapToMap()
                .one()
        }
        val start = System.nanoTime()
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
                    "commit admission timed out after ${lockTimeoutMs}ms waiting for the " +
                        "catalog commit lock (catalog_id=$catalogId); the catalog is busy — retry",
                )
            }
            throw e
        } finally {
            Metrics.commitLockWait(System.nanoTime() - start)
        }
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
