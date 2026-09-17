package com.posthog.hoglake.observability

import com.posthog.hoglake.model.HoglakeException
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import java.util.concurrent.TimeUnit

/**
 * Source-incremented counters (README.md §8): counted where the event
 * happens (commit tail, expiry sweep, cleanup drain, hydrator), never
 * sampled. The facade is a process-global hook so services need no
 * registry plumbing; with no registry bound every call is a no-op, so
 * tests that never touch observability keep running unchanged.
 */
object Metrics {
    @Volatile
    private var registry: MeterRegistry? = null

    /** Bind the process registry (App.build). Last bind wins. */
    fun bind(r: MeterRegistry) {
        registry = r
    }

    /** Unbind (tests). */
    fun clear() {
        registry = null
    }

    /** hoglake_commits_total{catalog, result=committed|conflict|validation|error} */
    fun tableCreationRecorded(
        catalog: String,
        action: String,
        outcome: String,
    ) {
        increment("hoglake_table_creation_total", 1.0, "catalog", catalog, "action", action, "outcome", outcome)
    }

    fun commitRecorded(
        catalog: String,
        result: String,
    ) = increment("hoglake_commits_total", 1.0, "catalog", catalog, "result", result)

    /** hoglake_snapshots_expired_total{catalog} */
    fun snapshotsExpired(
        catalog: String,
        count: Long,
    ) {
        if (count > 0) increment("hoglake_snapshots_expired_total", count.toDouble(), "catalog", catalog)
    }

    /** hoglake_files_removed_total{catalog} — physical S3 deletes only. */
    fun filesRemoved(
        catalog: String,
        count: Long,
    ) {
        if (count > 0) increment("hoglake_files_removed_total", count.toDouble(), "catalog", catalog)
    }

    /**
     * hoglake_stats_repaired_total{source=commit|hydrator|compaction} —
     * stats rows stored only after StatsSanity had to repair them (an
     * undecodable or inverted bound dropped, an impossible count
     * clamped).
     *
     * `compaction` is the one that can fire on HISTORY: it repairs the
     * input rows it merges, which may predate the rule entirely, so a
     * standing nonzero count there means old malformed rows are still
     * being read rather than that something is writing new ones.
     *
     * Nonzero means a WRITER is producing metadata its own data
     * contradicts. Silence here is the normal state; a rising line is a
     * bug report against whoever is writing those files, and without the
     * counter the repair would be invisible — the commit still succeeds.
     */
    fun statsRepaired(source: String) = increment("hoglake_stats_repaired_total", 1.0, "source", source)

    /** hoglake_stats_hydrated_total{result=provided|failed} */
    fun statsHydrated(result: String) = increment("hoglake_stats_hydrated_total", 1.0, "result", result)

    /**
     * hoglake_hydrator_transient_errors_total — footer fetches that
     * failed transiently (S3 throttle/5xx, connection/timeout): the
     * file STAYS 'pending' and the next sweep retries it, so this is
     * the only trace a throttle storm leaves.
     */
    fun hydratorTransientError() = increment("hoglake_hydrator_transient_errors_total", 1.0)

    /** hoglake_compaction_groups_total{catalog} — groups successfully rewritten + committed. */
    fun compactionGroups(
        catalog: String,
        count: Long,
    ) {
        if (count > 0) increment("hoglake_compaction_groups_total", count.toDouble(), "catalog", catalog)
    }

    /** hoglake_compaction_files_rewritten_total{catalog} — input files merged away. */
    fun compactionFilesRewritten(
        catalog: String,
        count: Long,
    ) {
        if (count > 0) {
            increment("hoglake_compaction_files_rewritten_total", count.toDouble(), "catalog", catalog)
        }
    }

    /** hoglake_background_loop_failures_total{loop} — iterations that threw (loop continued). */
    fun backgroundLoopFailure(loop: String) = increment("hoglake_background_loop_failures_total", 1.0, "loop", loop)

    /**
     * hoglake_commit_lock_wait_seconds histogram (B2): time spent
     * waiting on the per-catalog advisory commit lock — recorded by
     * Locks.acquireCatalogCommitLock, so the commit path AND every DDL /
     * maintenance tail contribute. A forming convoy is visible here
     * before it is an incident.
     */
    fun commitLockWait(nanos: Long) {
        val r = registry ?: return
        Timer.builder("hoglake_commit_lock_wait")
            .description("Advisory catalog-commit-lock acquisition wait")
            .publishPercentileHistogram()
            .register(r)
            .record(nanos, TimeUnit.NANOSECONDS)
    }

    /** The commit counter's result tag for a failed commit. */
    fun commitFailureResult(e: HoglakeException): String? =
        when (e) {
            is HoglakeException.CommitConflict -> "conflict"
            is HoglakeException.Validation -> "validation"
            is HoglakeException.CommitQueueTimeout -> "timeout"
            else -> null
        }

    private fun increment(
        name: String,
        amount: Double,
        vararg tags: String,
    ) {
        val r = registry ?: return
        r.counter(name, *tags).increment(amount)
    }
}
