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
     * The bound registry, for the observability surfaces in this
     * package that are GAUGES rather than counters and are therefore
     * not served by [increment] ([CatalogMetrics]'s MultiGauges).
     * Internal — nothing outside observability/ reaches the registry
     * directly.
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
     * hoglake_expiry_purge_rows_total{catalog} — file rows expiry's
     * phase-B purge DELETED (data files plus delete vectors).
     *
     * The rate to compare against the arrival rate: ended rows appear at
     * compaction's retirement rate (12,288 per sweep on prod-us at
     * 1.3.7's six groups of 2,048), and a purge whose rate is below that
     * is a purge falling behind however healthy each sweep looks. The
     * same number is the cleanup queue's inflow, so it also prices the
     * drain.
     */
    fun expiryPurgeRows(
        catalog: String,
        count: Long,
    ) {
        if (count > 0) increment("hoglake_expiry_purge_rows_total", count.toDouble(), "catalog", catalog)
    }

    /**
     * hoglake_expiry_purge_failures_total{catalog} — pages of that
     * purge which threw.
     *
     * The purge is fenced so it can never fail the floor advance it
     * follows, so without this a page that hits its statement bound is
     * indistinguishable on every surface from "nothing was eligible":
     * `data_files_purged` reads 0 either way, and the floor keeps
     * advancing, so every other signal says the sweep is working. A
     * standing nonzero means the page is too big for the rows it is
     * meeting — a wide table's stats cascade — and the remedy is
     * HOGLAKE_EXPIRY_PURGE_PAGE. Nothing is corrupt: the next sweep
     * retries from the oldest eligible row.
     */
    fun expiryPurgeFailures(
        catalog: String,
        count: Long,
    ) {
        if (count > 0) increment("hoglake_expiry_purge_failures_total", count.toDouble(), "catalog", catalog)
    }

    /**
     * hoglake_expiry_purge_truncated_total{catalog} — sweeps whose file
     * purge stopped with work left: the run budget expired, a page
     * failed its way down to one row, or the floor could not be read.
     *
     * THE SERIES TO ALERT ON, and the reason it exists at all. A purge
     * that is chronically budget-starved never FAILS — every page
     * succeeds, `hoglake_expiry_purge_rows_total` keeps a healthy-looking
     * rate forever, and `hoglake_expiry_purge_failures_total` stays at
     * zero — while the rows below the floor accumulate without bound.
     * Before this counter the only trace was the ledger row and the
     * console badge, neither of which Prometheus can see. A sustained
     * rate near one per sweep per catalog means the budget is too small
     * for the arrival rate (HOGLAKE_EXPIRY_PURGE_BUDGET_MS, or fewer
     * catalogs per maintenance pod); a rate that rises and falls is a
     * catalog catching up after a backlog, which is the design working.
     *
     * It used to be what made `/verify`'s `expiry_floor` gating safe —
     * that check skipped its file-row arm while a purge was behind, so
     * "behind" had to be loud somewhere else. #261 removed the check, so
     * this counter is no longer the quiet half of a pair: it and
     * `hoglake_expiry_purge_remaining` are now the ONLY things that say
     * a catalog is falling behind, and nothing at all says a drained
     * purge left rows below the floor.
     */
    fun expiryPurgeTruncated(catalog: String) {
        increment("hoglake_expiry_purge_truncated_total", 1.0, "catalog", catalog)
    }

    /**
     * hoglake_expiry_advance_failures_total{catalog} — sweeps whose
     * phase A (the floor advance, under the commit lock) threw after
     * exhausting its batch halvings.
     *
     * ZERO IS THE EXPECTED VALUE, and a nonzero one is actionable in a
     * way nothing else in the repo was: there is no maintenance-run
     * failure series anywhere, so before this a floor that stopped
     * advancing showed up only as a `failed` ledger row nobody scrapes.
     * The floor not advancing means retention is not being enforced —
     * snapshots, their change rows and their file rows all stop being
     * reclaimable — so this is the one expiry series that means "the
     * feature is off" rather than "the feature is behind".
     *
     * The purge still runs on that path, so a nonzero rate here with a
     * healthy `hoglake_expiry_purge_rows_total` means exactly that: the
     * backlog below the floor is draining, the floor itself is stuck.
     */
    fun expiryAdvanceFailures(catalog: String) {
        increment("hoglake_expiry_advance_failures_total", 1.0, "catalog", catalog)
    }

    /**
     * hoglake_expiry_halvings_total{catalog, phase} — batch or page
     * halvings, the adaptive bound discovering a cost the configuration
     * guessed wrong.
     *
     * `phase="advance"` is HOGLAKE_EXPIRY_BATCH too large for the
     * snapshot delete's `hog_snapshot_change` cascade on that catalog;
     * `phase="purge"` is HOGLAKE_EXPIRY_PURGE_PAGE too large for a
     * table's per-row cascade. Both are knobs, not bugs, and both are
     * invisible without a counter: the halved value is remembered only
     * for the rest of the run, so a standing rate means new work keeps
     * arriving that the configured size cannot absorb — which is the
     * same reading `hoglake_retirement_timeouts_total` carries for the
     * same mechanism.
     */
    fun expiryHalvings(
        catalog: String,
        phase: String,
        count: Long,
    ) {
        if (count > 0) {
            increment("hoglake_expiry_halvings_total", count.toDouble(), "catalog", catalog, "phase", phase)
        }
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
     * hoglake_commit_receipt_unknown_digest_version_total{catalog} — a
     * replay whose stored receipt digest carries a version byte this
     * build does not know (#240, V24).
     *
     * ZERO FOREVER IS THE EXPECTED VALUE. It can only become nonzero
     * after [com.posthog.hoglake.commit.COMMIT_FINGERPRINT_VERSION] is
     * bumped — which is required whenever the canonical fingerprint
     * string changes shape — and then only for replays of receipts
     * written before that deploy, i.e. for at most one
     * HOGLAKE_RECEIPT_RETENTION_SECONDS window. Each one was answered
     * with the stored snapshot rather than a refusal, which is the right
     * answer and an invisible one: this counter is the only way an
     * operator learns that the bump is in force and how many replays it
     * is covering.
     *
     * A nonzero rate with no recent deploy means something is writing
     * receipts this build cannot read — a rolled-back binary, or two
     * versions of the format live at once.
     */
    fun commitReceiptUnknownDigestVersion(catalog: String) {
        increment("hoglake_commit_receipt_unknown_digest_version_total", 1.0, "catalog", catalog)
    }

    /**
     * hoglake_commit_receipt_purge_failures_total{catalog} — pages of
     * the commit-receipt retention purge that threw (#240, V24).
     *
     * The purge is fenced so it can never fail the drain it rides, so
     * without this a page that times out is indistinguishable on every
     * surface from "nothing was eligible": `receipts_purged` reads 0
     * either way. That matters most for the one-time legacy backlog,
     * where the rows carry ~82 TOAST chunks each and a page is the only
     * statement in this change whose cost is not bounded by its row
     * count. A standing nonzero rate means the backlog is NOT draining
     * and the page size or the statement bound needs looking at.
     */
    fun commitReceiptPurgeFailures(
        catalog: String,
        count: Long,
    ) {
        if (count > 0) {
            increment("hoglake_commit_receipt_purge_failures_total", count.toDouble(), "catalog", catalog)
        }
    }

    /**
     * hoglake_stats_repaired_total{source=commit|hydrator} — stats rows
     * stored only after StatsSanity had to repair them (an undecodable
     * or inverted bound dropped, an impossible count clamped).
     *
     * `commit` is a client's shipped `column_stats`; `hydrator` is a
     * footer, including a COMPACTION OUTPUT's — compaction derives its
     * output's stats with FooterStats.aggregate, whose own
     * `FooterStats.sane` is the door, so a repair on a compacted file
     * reports as `hydrator`. There is no longer a `compaction` source at
     * all: nothing re-reads and re-merges the inputs' stored rows, so
     * the one surface that could repair HISTORY is gone (a backfill of
     * historical malformed rows remains a separate operation).
     *
     * Nonzero means a WRITER is producing metadata its own data
     * contradicts. Silence here is the normal state; a rising line is a
     * bug report against whoever is writing those files, and without the
     * counter the repair would be invisible — the commit still succeeds.
     */
    fun statsRepaired(source: String) = increment("hoglake_stats_repaired_total", 1.0, "source", source)

    /**
     * hoglake_blind_partitioned_appends_total{catalog,namespace,table} —
     * prepared appends that carried partition values with NO
     * read_snapshot, counted per occurrence.
     *
     * The flip signal for HOGLAKE_REFUSE_BLIND_PARTITIONED_APPENDS, and
     * the reason it is a counter and not only the WARN beside it: that
     * line fires once per (catalog, table) per pod and then goes quiet
     * forever, so a pod that logged it at startup and a pod whose client
     * was fixed an hour later read identically, and a second offending
     * client on an already-warned table is never named at all. "Has the
     * fleet stopped doing this?" is answerable from a rate, not from a
     * once-per-lifetime log line — so the flag flips on this going to
     * zero and staying there, not on someone grepping logs.
     *
     * Per (namespace, table) because the remediation is per writer and
     * the writers are per table; the series only exists for tables doing
     * it, which is a set the rollout is driving to empty.
     */
    fun blindPartitionedAppend(
        catalog: String,
        namespace: String,
        table: String,
    ) = increment(
        "hoglake_blind_partitioned_appends_total",
        1.0,
        "catalog",
        catalog,
        "namespace",
        namespace,
        "table",
        table,
    )

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
     * hoglake_hydrator_claim_timeouts_total{catalog} — sweeps in which
     * this catalog's claim exceeded the hydrator's claim timeout and
     * was skipped (#269: the catalog lost its share of that sweep; no
     * other catalog was affected). A catalog that counts here every
     * sweep has a pending backlog the claim walks and discards — a
     * dropped table's rows, until retirement deletes them.
     */
    fun hydratorClaimTimeout(catalog: String) =
        increment("hoglake_hydrator_claim_timeouts_total", 1.0, "catalog", catalog)

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
     * by their OWN transaction-local statement bound.
     *
     * AN OCCASIONAL TICK HERE IS NOT A PROBLEM, and that is the whole
     * reading of this series since #263. The per-row cost is flat in
     * the batch size, so a cancelled batch was COLD, not big; the
     * statement warmed the pages it touched, the table is left for the
     * next run at the SAME size, and the retry is the cheap case. The
     * loop no longer resizes anything in response — the batch is always
     * HOGLAKE_RETIREMENT_BATCH.
     *
     * WHAT MATTERS IS WHETHER IT IS THE SAME TABLE EVERY RUN, which
     * this counter cannot say and
     * `hoglake_retirement_consecutive_timeouts{catalog,table}`
     * ([com.posthog.hoglake.observability.RetirementGauges]) exists to:
     * alert on that gauge, not on this rate. A standing streak there is
     * a table whose per-row cascade does not fit the bound at the
     * configured batch, and the remedy — a smaller
     * HOGLAKE_RETIREMENT_BATCH on the instance that retires that
     * catalog, the knob being process-wide — is an operator's call.
     *
     * Convoys are NOT counted here (see [retirementConvoyed]): they ask
     * for a different remedy, and summing the two hides both.
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
     * `spill_budget` and `merge_budget` are the same kind: a sorted
     * group the external merge sort's spill-disk or heap budget refuses
     * (hoglake#134), in metadata at planning or as the rewrite's own
     * hard stop. Configuration rather than fault, and the same groups
     * re-refuse every sweep until a knob moves, so they need a line.
     * `heap_budget` is HISTORICAL: the pre-#134 sorted row ceiling's
     * refusal, which nothing produces any more. An existing series stays
     * scrapeable until the process restarts and is never incremented.
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
     * hoglake_compaction_spill_bytes_total{catalog} — local bytes sorted
     * rewrites spilled (hoglake#134). Its rate against
     * `hoglake_compaction_groups_total` is the spill a sorted group costs;
     * the per-group bound is HOGLAKE_COMPACTION_SPILL_BYTES.
     */
    fun compactionSpillBytes(
        catalog: String,
        bytes: Long,
    ) {
        if (bytes > 0) increment("hoglake_compaction_spill_bytes_total", bytes.toDouble(), "catalog", catalog)
    }

    /**
     * hoglake_compaction_merge_runs_total{catalog, kind} — the runs sorted
     * rewrites merged: `trusted` (inputs read in place: compaction outputs
     * of the live sort spec, and files the sortedness pre-pass verified),
     * `spilled` (chunk files written and read back) and
     * `demoted` (trusted inputs the heap budget sent down the spill path
     * instead). `trusted` against `spilled` is how much sort work the
     * trust predicate saves; `demoted` rising says the sorted heap budget
     * is small for the group size.
     */
    fun compactionMergeRuns(
        catalog: String,
        kind: String,
        count: Long,
    ) {
        if (count > 0) {
            increment("hoglake_compaction_merge_runs_total", count.toDouble(), "catalog", catalog, "kind", kind)
        }
    }

    /**
     * hoglake_compaction_sort_check_total{catalog, outcome} — inputs the
     * sorted rewrite's pre-pass checked: `sorted` (already in merge-key
     * order, read in place as a run and never spilled) or `unsorted`
     * (spilled). `sorted` against the whole is how much of a table's
     * intake arrives pre-sorted — millpond's flushes do, a Trino INSERT's
     * files do not — and so how much spill the pre-pass saves.
     */
    fun compactionSortCheck(
        catalog: String,
        outcome: String,
        count: Long,
    ) {
        if (count > 0) {
            increment("hoglake_compaction_sort_check_total", count.toDouble(), "catalog", catalog, "outcome", outcome)
        }
    }

    /**
     * hoglake_compaction_sort_check_bytes_total{catalog} — bytes the
     * pre-pass read: the sort-key column chunks of the files it checked
     * (the row-id carrier's too for an explicit-row-id file), up to each
     * file's first out-of-order row. Its rate against the files' sizes is
     * what verification costs in object-store reads.
     */
    fun compactionSortCheckBytes(
        catalog: String,
        bytes: Long,
    ) {
        if (bytes > 0) increment("hoglake_compaction_sort_check_bytes_total", bytes.toDouble(), "catalog", catalog)
    }

    /**
     * hoglake_compaction_appended_bytes_total{catalog} — compressed input
     * bytes compaction copied into its outputs byte for byte instead of
     * decoding and re-encoding them (hoglake#134 package D1): row groups
     * of prior outputs of the live schema, DV-free, at least 32 MiB each.
     * Its rate against the input bytes is how much of the rewrite's work
     * the append saves (measured 8-9x cheaper per byte).
     */
    fun compactionAppendedBytes(
        catalog: String,
        bytes: Long,
    ) {
        if (bytes > 0) increment("hoglake_compaction_appended_bytes_total", bytes.toDouble(), "catalog", catalog)
    }

    /**
     * hoglake_compaction_spill_cleanup_failures_total — spill directories
     * that could not be removed: after a rewrite, or by the startup sweep
     * of a previous process's leftovers. Any nonzero value is disk the
     * spill volume (an emptyDir whose overrun evicts the pod) keeps
     * counting until the process restarts; the WARN names the path.
     */
    fun compactionSpillCleanupFailed(count: Long = 1) {
        if (count > 0) increment("hoglake_compaction_spill_cleanup_failures_total", count.toDouble())
    }

    /**
     * `hoglake_compaction_spill_dir_bytes` — bytes currently held by
     * sorted-rewrite spill directories under HOGLAKE_COMPACTION_SPILL_DIR,
     * MEASURED by walking them at scrape time.
     *
     * Not the filesystem's free space, deliberately: on the chart's
     * emptyDir `FileStore.usableSpace` reports the node's disk, while
     * what evicts the pod is the volume's `sizeLimit` against what is
     * written to it — this. Alert on it against `tmpSizeLimit`.
     *
     * COST: a `Files.walk` per SCRAPE, on every pod, O(spill files) —
     * one directory listing plus one `size` per spill file of every group
     * in flight. Today that is a few dozen files per group, because a
     * spill file holds a whole chunk in 16 MiB row groups
     * (`SortSpill.SPILL_BLOCK_BYTES` sizes the reads, the chunk sizes the
     * file count). Anything that multiplies the spill FILE count — smaller
     * chunks, a file per row group — multiplies this scrape's work with
     * it; re-check it then. It touches only `hoglake-compaction-spill-*`
     * directories and never throws.
     */
    fun registerSpillDirGauge(
        registry: MeterRegistry,
        bytes: () -> Long,
    ) {
        gauge(
            registry,
            "hoglake_compaction_spill_dir_bytes",
            "Bytes held by compaction spill directories under HOGLAKE_COMPACTION_SPILL_DIR (measured)",
        ) { bytes() }
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
     * hoglake_reindex_gauge_refresh_failures_total — metrics ticks whose
     * `hoglake_index_bloat_bytes` refresh threw. Its own counter, not the
     * metrics loop's: the refresh rides that loop's tick on the reindex pod
     * but must neither fail it nor be skipped by its failure.
     */
    fun reindexGaugeRefreshFailure() = increment("hoglake_reindex_gauge_refresh_failures_total", 1.0)

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
            // Counted, and counted as a conflict: it is a 409, and
            // before it was typed it was counted as "validation".
            // Left in the `else` it would stop being counted at all.
            is HoglakeException.TableDropped -> "conflict"
            // Same argument as TableDropped above: a 409 that used to be
            // counted as a conflict (it WAS a CommitConflict) and would
            // silently stop being counted at all if left to the `else`.
            // The two RE-PREPARE refusals, before CommitConflict because
            // DdlSinceReadSnapshot is a subclass of it and a `when` would
            // otherwise answer the base arm.
            //
            // Their values are DISTINCT, and that is the point: these are
            // the refusals a writer cannot retry its way out of, so "how
            // often is the fleet re-preparing" has to be a series an
            // operator can graph rather than a slice of `conflict` only
            // the logs can separate. An arm that answered "conflict" here
            // would be indistinguishable from having no arm at all.
            is HoglakeException.DdlSinceReadSnapshot -> "ddl_since_read_snapshot"
            is HoglakeException.TableRecreated -> "table_recreated"
            is HoglakeException.CommitConflict -> "conflict"
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
