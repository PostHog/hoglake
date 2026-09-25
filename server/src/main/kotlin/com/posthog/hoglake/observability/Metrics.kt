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

    /**
     * The bound registry, for the one observability surface in this
     * package that is a GAUGE rather than a counter and is therefore not
     * served by [increment]: [VerifyGauges]. Internal — nothing outside
     * observability/ reaches the registry directly.
     */
    internal val boundRegistry: MeterRegistry?
        get() = registry

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

    /**
     * hoglake_files_removed_total{catalog} — physical S3 deletes only.
     *
     * Fed from `CleanupResult.objectsRemoved`, which is DISTINCT paths,
     * NOT `removed`, which is queue rows. The two differ: two undrained
     * `hog_file_removal` rows over one path are legitimate state
     * (nothing makes a file path unique — see V16's non-unique
     * argument), and one batched delete settles both while removing one
     * object. Counting rows here would make a duplicate look like extra
     * storage reclaimed.
     */
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

    /**
     * hoglake_rows_retired_total{catalog} — file METADATA rows a
     * retirement run deleted off dropped tables (data files + deletion
     * vectors), each of which queued one path for the cleanup drain.
     *
     * Deliberately not the same thing as `hoglake_files_removed_total`,
     * which counts OBJECTS cleanup physically deleted. The gap between
     * the two is the queue depth, and watching it is how an operator
     * sees retirement outrunning the drain.
     */
    fun rowsRetired(
        catalog: String,
        count: Long,
    ) {
        if (count > 0) increment("hoglake_rows_retired_total", count.toDouble(), "catalog", catalog)
    }

    /** hoglake_retirement_batches_total{catalog} — batch transactions that committed. */
    fun retirementBatches(
        catalog: String,
        count: Long,
    ) {
        if (count > 0) increment("hoglake_retirement_batches_total", count.toDouble(), "catalog", catalog)
    }

    /**
     * hoglake_retirement_timeouts_total{catalog} — batches rolled back
     * by their OWN transaction-local statement bound. Each one halves
     * the batch size for that table, and the size is remembered for
     * the rest of the process's life, so a standing nonzero means new
     * tables keep arriving that the configured batch is too big for —
     * not that the same table is rediscovering it every run.
     *
     * The remedy is HOGLAKE_RETIREMENT_BATCH. Convoys are NOT counted
     * here (see [retirementConvoyed]): they ask for the opposite
     * remedy, and summing the two hides both.
     */
    fun retirementTimeouts(
        catalog: String,
        count: Long,
    ) {
        if (count > 0) increment("hoglake_retirement_timeouts_total", count.toDouble(), "catalog", catalog)
    }

    /**
     * hoglake_retirement_convoyed_total{catalog} — runs that gave up
     * because the per-catalog COMMIT LOCK was not available inside
     * HOGLAKE_COMMIT_LOCK_TIMEOUT_MS.
     *
     * Not a retirement problem and not fixed by a smaller batch: it
     * says the catalog's commit lock is held by something else long
     * enough to exhaust the admission window — a long expiry sweep, a
     * cleanup drain, a truncate. Retirement stepping aside is the
     * correct response; a rising line is a pointer at whatever is
     * holding the lock.
     */
    fun retirementConvoyed(
        catalog: String,
        count: Long,
    ) {
        if (count > 0) increment("hoglake_retirement_convoyed_total", count.toDouble(), "catalog", catalog)
    }

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

    /**
     * hoglake_compaction_skipped_total{catalog, reason} — groups the
     * sweep declined to compact, by reason.
     *
     * `unconvertible_schema` and `invalid_data` had a DTO field, a log
     * line and a ledger row each, and no counter — so the only way to
     * see either was to read a run's payload or grep the logs. They are
     * the two an operator most needs a LINE for: unconvertible_schema
     * rising means a table has stopped compacting, invalid_data rising
     * means a writer is emitting values its own schema forbids. Neither
     * is visible as a failure, which is exactly why neither gets
     * noticed.
     *
     * `heap_budget` is a third of the same kind: a table whose sorted
     * groups are too many ROWS for the compaction heap budget stops
     * compacting, silently, forever — the debt page shows growing debt
     * and nothing says why. It is also the counter that says the row
     * ceiling is doing its job, since the alternative reading of the
     * same condition is the OOM it replaced.
     *
     * `failed` joins them, for the opposite reason: it IS the red-flag
     * outcome and it had no series either. (An earlier version of this
     * comment claimed every other compaction outcome was already a
     * series. It was not — `skipped_conflicts`, `dv_superseded`,
     * `bytes_in`/`bytes_out` and `failed_groups` all had none. Only
     * groups and files-rewritten did.)
     *
     * The self-healing skips — commit conflicts, DV supersession — stay
     * uncounted: they re-plan on the next run, so a line for them is
     * noise rather than signal.
     */
    fun compactionSkipped(
        catalog: String,
        reason: String,
        count: Long,
    ) {
        if (count > 0) {
            increment(
                "hoglake_compaction_skipped_total",
                count.toDouble(),
                "catalog",
                catalog,
                "reason",
                reason,
            )
        }
    }

    /**
     * hoglake_multipart_abort_failures_total — aborts of a compaction
     * output's multipart upload that themselves failed.
     *
     * Worth a series of its own because nothing else can see the
     * consequence: unfinished parts are not objects, so the removal
     * ledger and the cleanup drain cannot reach them, and they are
     * billed until a bucket lifecycle rule reaps them. Any sustained
     * nonzero value here is storage growing silently.
     */
    fun multipartAbortFailed() = increment("hoglake_multipart_abort_failures_total", 1.0)

    /**
     * hoglake_verify_errors_total{catalog} — verify sweeps that THREW
     * for one catalog.
     *
     * The verify sweep catches per catalog so one bad catalog cannot
     * stop the others, which means `hoglake_background_loop_failures_
     * total` never fires for it — the iteration succeeded. And because
     * the violation gauge is a MultiGauge refreshed with the sweep's
     * whole row set, a catalog that throws is simply absent from it:
     * its series RETIRE, which is correct (the sweep has no answer for
     * it) and silent (nothing left says so). This counter is the thing
     * that says so. A standing `increase(...) > 0` is a catalog nobody
     * is checking, which is worse than a catalog that fails a check.
     */
    fun verifyError(catalog: String) = increment("hoglake_verify_errors_total", 1.0, "catalog", catalog)

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
            // Counted, and counted as a conflict: it is a 409, and
            // before it was typed it was counted as "validation".
            // Left in the `else` it would stop being counted at all.
            is HoglakeException.TableDropped -> "conflict"
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
