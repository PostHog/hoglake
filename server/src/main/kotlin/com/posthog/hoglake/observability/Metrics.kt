package com.posthog.hoglake.observability

import com.posthog.hoglake.api.RequestDispatcher
import com.posthog.hoglake.model.HoglakeException
import com.zaxxer.hikari.HikariDataSource
import io.micrometer.core.instrument.Gauge
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
     * The two histograms on per-request / per-commit paths, resolved
     * ONCE per bound registry.
     *
     * `Timer.builder(...).register(r)` is idempotent but not free: it
     * allocates a builder and walks the registry's meter map on every
     * call, and these two are hit on every request and every commit.
     * Held beside the registry rather than in a lazy of their own so
     * that [bind] and [clear] cannot leave a timer pointing at a
     * registry nobody scrapes — which is the shape a test that rebinds
     * would produce.
     */
    private class BoundTimers(private val registry: MeterRegistry) {
        // LAZY, not eager. Registering at bind() would mint both series
        // on a registry nothing has recorded into yet, and this facade's
        // contract is that a metric appears when its event happens —
        // `MetricsFacadeTest.zero-count expiry and removal increments
        // create no series` pins exactly that for the counters, and a
        // timer family materializing out of `bind` alone would be the
        // same lie with more buckets.
        val commitLockWait: Timer by lazy {
            Timer.builder("hoglake_commit_lock_wait")
                .description("Advisory catalog-commit-lock acquisition wait")
                .publishPercentileHistogram()
                .register(registry)
        }
        val requestQueueWait: Timer by lazy {
            Timer.builder("hoglake_request_queue_wait")
                .description("Wait for a blocking-dispatcher thread, before the handler started")
                .publishPercentileHistogram()
                .register(registry)
        }
    }

    @Volatile
    private var timers: BoundTimers? = null

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
        timers = BoundTimers(r)
    }

    /** Unbind (tests). */
    fun clear() {
        registry = null
        timers = null
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
        timers?.commitLockWait?.record(nanos, TimeUnit.NANOSECONDS)
    }

    /**
     * `hoglake_request_queue_wait_seconds` histogram (#218): how long a
     * request waited for a thread of the blocking dispatcher before its
     * handler started.
     *
     * Recorded at handler entry, once per request, so it measures FIRST
     * dispatch and not the resumptions that follow. It is the latency
     * the dispatch seam introduced and the only place saturation shows
     * up as a number a caller would recognise: `hoglake_request_pool_*`
     * says how full the pool is, this says what that cost.
     *
     * Its p99 against `HOGLAKE_COMMIT_LOCK_TIMEOUT_MS` is the reading
     * that matters — a request whose wait reaches the bound is shed
     * with a typed 503 before it borrows a connection, so a rising tail
     * here is the warning that precedes visible refusals.
     */
    fun requestQueueWait(millis: Long) {
        timers?.requestQueueWait?.record(millis, TimeUnit.MILLISECONDS)
    }

    /**
     * `hoglake_request_queue_abandoned_total` — calls that left the
     * dispatcher queue without ever reaching a thread (the client
     * disconnected, the call was cancelled).
     *
     * The companion to [requestQueueWait], and the reason that
     * histogram cannot be read alone: the histogram is CONDITIONED ON
     * SURVIVAL. It records at handler entry, so a wait that ended in
     * the caller giving up contributes nothing — under a convoy bad
     * enough that most callers time out first, the surviving waits are
     * the SHORT ones and the p99 can fall while the instance gets
     * worse. This counter is what says that happened.
     */
    fun requestQueueAbandoned() = increment("hoglake_request_queue_abandoned_total", 1.0)

    /**
     * `hoglake_requests_shed_total` — requests refused at handler entry
     * because their queue wait had already exhausted the admission
     * bound (`api/BlockingDispatch.kt`).
     *
     * NOT counted in `hoglake_commits_total{result="timeout"}`, and the
     * asymmetry is deliberate rather than an oversight: a shed request
     * is refused BEFORE Routing has run, so its catalog path parameter
     * has not been parsed and there is no catalog to tag. Tagging them
     * `unknown` would put a meaningless series inside the per-catalog
     * counter that commit alerts key on, and dropping the shed on the
     * floor would leave the refusals invisible. So they live here, and
     * the two are read TOGETHER during saturation: a gap between the
     * 503s a client sees and `hoglake_commits_total{result="timeout"}`
     * is this counter.
     *
     * For the same reason a shed request carries no `route` tag on
     * `ktor.http.server.requests` — Routing never resolved one.
     */
    fun requestShed() = increment("hoglake_requests_shed_total", 1.0)

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

    /**
     * `hoglake_db_pool_active` / `_idle` / `_pending` / `_max` — the
     * Hikari request pool, sampled off its own `HikariPoolMXBean`
     * (#218).
     *
     * The pool filling is the state every commit convoy passes through
     * on its way to an incident, and until #218 it had NO series at
     * all: the only evidence was the 5 s `connectionTimeout` 500s that
     * arrive after it is already too late, and — before the probe got a
     * connection of its own — a liveness kill. `active` is connections
     * in use, `idle` connections free, `pending` THREADS WAITING for
     * one, and `max` the configured ceiling, which is here so that
     * saturation is expressible as `active / max` without an alert
     * hard-coding `HOGLAKE_DB_POOL_SIZE`.
     *
     * `pending` is the one to alert on. `active == max` is the normal
     * state of a busy instance; `pending > 0` means a caller is queued
     * inside `getConnection` and has at most 5 s before it gets a 500.
     *
     * Gauges, not counters, and sampled by the scrape: Hikari's MXBean
     * is O(1) over the pool's own bookkeeping, so there is no sampler
     * loop and nothing to fall behind.
     */
    fun registerDbPoolGauges(
        registry: MeterRegistry,
        pool: HikariDataSource,
    ) {
        // hikariPoolMXBean is resolved INSIDE each lambda, never once at
        // registration, and every read tolerates its absence.
        // HikariDataSource builds its pool LAZILY — on the first
        // getConnection, or on construction only when
        // initializationFailTimeout says so — so at registration time
        // the bean may not exist yet, and after `close()` it is gone
        // again. A captured reference would be null or stale; an
        // unguarded read would THROW, and a gauge supplier that throws
        // fails the whole `/metrics` scrape, taking every other series
        // with it precisely when somebody is looking.
        gauge(registry, "hoglake_db_pool_active", "Pooled catalog connections in use") {
            pool.hikariPoolMXBean?.activeConnections ?: 0
        }
        gauge(registry, "hoglake_db_pool_idle", "Pooled catalog connections free") {
            pool.hikariPoolMXBean?.idleConnections ?: 0
        }
        gauge(
            registry,
            "hoglake_db_pool_pending",
            "Threads waiting inside HikariPool.getConnection for a catalog connection",
        ) { pool.hikariPoolMXBean?.threadsAwaitingConnection ?: 0 }
        gauge(registry, "hoglake_db_pool_max", "Configured maximum size of the catalog connection pool") {
            pool.maximumPoolSize
        }
    }

    /**
     * `hoglake_request_pool_active` / `_queued` / `_max` — the blocking
     * request dispatcher (#218), the pool route handlers run on now
     * that they are off the Netty event loop.
     *
     * `active` is THREADS inside a handler's blocking call — an
     * approximation (`ThreadPoolExecutor.getActiveCount` walks the
     * worker set) that undercounts requests, because a handler
     * suspended parsing its body runs on `Dispatchers.IO` and holds no
     * thread here. `queued` is requests waiting for their FIRST thread,
     * counted by the interceptor rather than read off
     * `executor.queue.size`: the executor's queue also holds the
     * re-dispatch of every already-admitted coroutine that resumed
     * after a suspension, so its depth counts admitted work as if it
     * were waiting. `max` is the width (`HOGLAKE_REQUEST_THREADS`,
     * defaulting to the database pool size).
     *
     * `queued` is the saturation signal and
     * `hoglake_request_queue_wait_seconds` is what it costs; the
     * dispatcher is bounded by the AGE of a wait rather than by its
     * depth (see `api/BlockingDispatch.kt`), so the histogram is the
     * one to alert on and this is the one that explains it.
     *
     * Read the two TOGETHER with `hoglake_db_pool_pending`. Handlers
     * queued here are NOT holding catalog connections; handlers pending
     * there are. `active == max` with `queued` climbing and
     * `db_pool_pending` at zero is the dispatcher doing its job —
     * requests waiting instead of timing out on Hikari.
     */
    fun registerRequestPoolGauges(
        registry: MeterRegistry,
        requests: RequestDispatcher,
    ) {
        gauge(registry, "hoglake_request_pool_active", "Request threads inside a blocking handler call") {
            requests.active
        }
        gauge(registry, "hoglake_request_pool_queued", "Requests dispatched and waiting for a request thread") {
            requests.queued
        }
        gauge(registry, "hoglake_request_pool_max", "Width of the blocking request dispatcher") {
            requests.threads
        }
    }

    /**
     * `hoglake_health_probe_attempts_hung` — probe attempts started and
     * not finished (`HealthProbe`'s own cap is 2).
     *
     * THE CAP IS AN ABSORBING STATE, which is why it needs a series of
     * its own. Two attempts hung at any point in a pod's life park both
     * threads forever — the connection attempts have no way to be
     * interrupted — and from then on `/healthz` answers 503 without
     * launching anything, which is indistinguishable from a dead
     * database. `hoglake_health_probe_attempts_hung == 2` against a
     * Postgres that is demonstrably answering means the pod is stuck
     * and needs restarting; that is the only reading of this gauge and
     * it is in the README.
     */
    fun registerHealthProbeGauge(
        registry: MeterRegistry,
        outstandingAttempts: () -> Int,
    ) {
        gauge(
            registry,
            "hoglake_health_probe_attempts_hung",
            "Health-probe attempts started and not finished (2 = the cap; the pod is stuck)",
        ) { outstandingAttempts() }
    }

    private fun gauge(
        registry: MeterRegistry,
        name: String,
        description: String,
        value: () -> Number,
    ) {
        Gauge.builder(name) { value().toDouble() }
            .description(description)
            .strongReference(true)
            .register(registry)
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
