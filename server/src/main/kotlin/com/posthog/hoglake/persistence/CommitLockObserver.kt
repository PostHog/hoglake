package com.posthog.hoglake.persistence

import com.posthog.hoglake.observability.Metrics
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * What kind of work holds the per-catalog commit lock: the `path` label
 * of `hoglake_commit_lock_hold_seconds`. Every caller of
 * [Locks.acquireCatalogCommitLock] passes one, with no default, so a new
 * caller cannot take the lock without saying what it is.
 * `CommitLockCallSitesTest` pins which value each call site passes.
 */
enum class CommitLockPath(val label: String) {
    /** `CommitService` — the data-commit tail (appends, deletes, DVs). */
    COMMIT("commit"),

    /** One compaction group's registration (`CompactionService.commitGroup`). */
    COMPACTION("compaction"),

    /** Expiry's floor advance (phase A of a sweep). */
    EXPIRY("expiry"),

    /** One retirement batch of a dropped table's file rows. */
    RETIREMENT("retirement"),

    /**
     * Schema and namespace changes that allocate a snapshot: create
     * namespace/table/view, ALTER, a staged table creation's publish, and
     * TRUNCATE (end-snapshots the live files, but is a table-management
     * operation, not the commit endpoint; separating it from `commit`
     * keeps the hot path's histogram the hot path's).
     */
    DDL("ddl"),

    /** DROP of a catalog, namespace, table or view. */
    DROP("drop"),
}

/**
 * The in-process half of the commit lock's observability (#439): the
 * waiting gauge and wait timer around the grant, then the held gauge
 * and hold timer from grant to the end of the transaction.
 *
 * Separate from [Locks] so the bookkeeping can be unit-tested around a
 * stubbed acquire and a stubbed transaction end; [Locks] supplies the
 * real ones (the advisory-lock statement, and the JDBI handle's
 * commit/rollback/close hooks).
 *
 * Passive (AGENT.md invariant 8): nothing here issues a statement, and a
 * settle that throws is logged and swallowed, because it runs inside
 * JDBI's commit/rollback and must never turn a committed transaction
 * into a failed call.
 */
internal object CommitLockObserver {
    private val log = KotlinLogging.logger {}

    /** The end of the transaction that took the lock, as the observer needs it. */
    interface TransactionEnd {
        /**
         * The transaction's owner, compared by IDENTITY: a second acquire
         * of the same catalog by the same owner before its end is the
         * re-entrant case (a staged table creation's publish, then the
         * `createTable` it calls) and is one hold, not two.
         */
        val owner: Any

        /** False in autocommit, where the statement's own transaction released the lock already. */
        val open: Boolean

        /**
         * Run [settle] when the transaction ends, by whatever route. It
         * may be invoked more than once (commit AND close); the observer
         * settles once.
         */
        fun onEnd(settle: () -> Unit)
    }

    private class Key(val owner: Any, val catalogId: Long) {
        override fun equals(other: Any?): Boolean =
            other is Key && other.owner === owner && other.catalogId == catalogId

        override fun hashCode(): Int = 31 * System.identityHashCode(owner) + catalogId.hashCode()
    }

    private class Hold(
        val catalog: String,
        val path: CommitLockPath,
        val grantedNanos: Long,
        val held: AtomicInteger?,
    ) {
        val settled = AtomicBoolean(false)
    }

    private val open = ConcurrentHashMap<Key, Hold>()

    /** Holds not yet settled, in this process. Tests read it to prove nothing leaks. */
    internal val openHolds: Int get() = open.size

    /**
     * Run [acquire] — which blocks until the lock is granted or throws
     * (the lock timeout as `CommitQueueTimeout`, anything else as itself)
     * — inside the waiting gauge and the wait timer, then open a hold
     * that [txn] settles when the transaction ends.
     *
     * The waiting decrement is in a `finally` for the reason the gauge
     * exists: a timed-out waiter that stayed counted would read as a
     * queue that never drains, on exactly the catalog that is in trouble.
     */
    fun observe(
        catalogId: Long,
        catalog: String,
        path: CommitLockPath,
        txn: TransactionEnd,
        acquire: () -> Unit,
    ) {
        val waiting = Metrics.commitLockWaiting(catalog)
        waiting?.incrementAndGet()
        val start = System.nanoTime()
        try {
            acquire()
        } finally {
            waiting?.decrementAndGet()
            Metrics.commitLockWait(System.nanoTime() - start)
        }
        startHold(catalogId, catalog, path, txn)
    }

    private fun startHold(
        catalogId: Long,
        catalog: String,
        path: CommitLockPath,
        txn: TransactionEnd,
    ) {
        val granted = System.nanoTime()
        if (!txn.open) {
            // Autocommit: the lock was taken and released by the one
            // statement. Recorded as the (near-zero) hold it was.
            Metrics.commitLockHold(catalog, path.label, System.nanoTime() - granted)
            return
        }
        val key = Key(txn.owner, catalogId)
        val hold = Hold(catalog, path, granted, Metrics.commitLockHeld(catalog))
        // Re-entrant: the first acquire's hold already runs to this
        // transaction's end, and it keeps the first acquire's path.
        if (open.putIfAbsent(key, hold) != null) return
        hold.held?.incrementAndGet()
        txn.onEnd { settle(key, hold) }
    }

    private fun settle(
        key: Key,
        hold: Hold,
    ) {
        if (!hold.settled.compareAndSet(false, true)) return
        try {
            open.remove(key, hold)
            hold.held?.decrementAndGet()
            Metrics.commitLockHold(hold.catalog, hold.path.label, System.nanoTime() - hold.grantedNanos)
        } catch (e: Exception) {
            log.warn(e) { "commit lock hold bookkeeping failed for catalog '${hold.catalog}'" }
        }
    }
}
