package com.posthog.hoglake.compaction

import com.posthog.hoglake.hydrator.FooterSplitOffsets
import com.posthog.hoglake.hydrator.FooterStats
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.hydrator.asCatalogColumns
import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CompactionResult
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.model.allNodes
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.persistence.MaintenanceRunStore
import com.posthog.hoglake.persistence.NamespaceRepo
import com.posthog.hoglake.persistence.PartialResult
import com.posthog.hoglake.persistence.Pg
import com.posthog.hoglake.persistence.SnapshotRepo
import com.posthog.hoglake.persistence.SortRepo
import com.posthog.hoglake.persistence.TableRepo
import com.posthog.hoglake.persistence.TierTotalsRepo
import com.posthog.hoglake.persistence.bindBigintArrayOrNull
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.jdbi.v3.core.statement.UnableToExecuteStatementException
import java.sql.ResultSet
import java.util.UUID
import java.util.concurrent.TimeUnit

/** Per-run compaction knobs (Config's HOGLAKE_COMPACTION_* env surface). */
data class CompactionConfig(
    /** The size a group packs to, in one rewrite. */
    val targetBytes: Long,
    /**
     * Files a group must hold to be worth rewriting — the condition that
     * makes compaction TERMINATE. See CompactionGrouping.groups.
     */
    val minInputFiles: Int = CompactionGrouping.DEFAULT_MIN_INPUT_FILES,
    /**
     * The FLOOR of the fan-in cap — see [effectiveMaxInputFiles] and
     * CompactionGrouping.groups.
     *
     * It used to be the cap itself, and at 12 KiB per file that made a
     * 64-file group rewrite 768 KiB against a 512 MiB target: 0.15% of
     * the target, and 63 files retired per group when the bucket holds
     * thousands. See [effectiveMaxInputFiles] for what replaced it and
     * why this stays a floor rather than becoming irrelevant.
     */
    val maxInputFiles: Int = CompactionGrouping.DEFAULT_MAX_INPUT_FILES,
    /**
     * The CEILING of the fan-in cap
     * (`HOGLAKE_COMPACTION_MAX_FAN_IN`), whatever the file sizes say.
     *
     * [effectiveMaxInputFiles] scales the fan-in up for a bucket of tiny
     * files, and this is where it stops. What it bounds is not bytes —
     * the byte target does that — but the per-GROUP resources that scale
     * with the INPUT COUNT rather than with the data: one open reader
     * and its parsed footer per input while
     * [inputOpenParallelism] of them are in flight, one `hog_data_file`
     * row locked per input in the commit tail, and one entry per input
     * in the claim key.
     *
     * # 2,048, and where that comes from
     *
     * A group's inputs are also ROWS UNDER THE PER-CATALOG COMMIT LOCK:
     * `commitGroup`'s plan-to-commit re-verification and its
     * `end_snapshot` retirement are both `IN`-list statements over
     * every input, once per group, up to [maxGroupsPerRun] times a
     * sweep, and every foreground commit waits behind each hold. That
     * is the bound, and `CompactionFanInMeasurement` measures it
     * (PG 18, warm, a 200,000-row manifest, ids scattered through it):
     *
     *   fan-in 64 = 2.2 ms hold / 34.3 us per row;
     *   512 = 9.4 ms / 18.5 us; **2,048 = 33.6 ms / 16.4 us**.
     *
     * Against a stated budget of 50 ms per group, 2,048 fits with
     * margin, and the per-row cost FALLS with the list length rather
     * than growing — so the sweep's lock duty cycle is 33.6 ms x 64
     * groups x 15 sweeps an hour = **0.90%**, against the retirement
     * loop's designed 18-25%.
     *
     * What it buys: ~1.97M files retired an hour against ~216k
     * arriving, clearing a 9.9M-file backlog in about five hours. At
     * the old fixed 64 it is ~60k an hour — permanently under water.
     *
     * THE EXTRAPOLATION IS THE RISK, not the arithmetic. That manifest
     * fits in shared_buffers and gigahog-prod-us's ~14M rows / ~1.5 GB
     * does not, so a scattered keyed probe there pays reads rather than
     * hits and the per-row cost can be several times as much; at
     * 100 us/row a 2,048-input commit is 205 ms and the budget is gone.
     * LOWER THIS KNOB if it does. The failure mode meanwhile is bounded
     * rather than open-ended: [commitLockTimeoutMs] already bounds the
     * commit, so a hold that outgrows it is a counted
     * `skipped_conflicts` and not a stall.
     *
     * `HOGLAKE_COMPACTION_PARALLEL_INPUT_OPENS` is the other half of
     * the per-group footprint — it, not this, is what bounds how many
     * inputs are OPEN at once, so this knob's memory cost is one
     * `CompactionCandidate` per input rather than one reader.
     */
    val maxFanIn: Int = DEFAULT_MAX_FAN_IN,
    /** Groups rewritten per run per catalog — tiny bites, never a storm. */
    val maxGroupsPerRun: Int,
    /**
     * How many times a run's own capacity in FILES the planner may fetch
     * candidate rows for (`HOGLAKE_COMPACTION_CANDIDATE_HEADROOM`).
     *
     * A run rewrites at most [maxGroupsPerRun] groups of at most
     * [maxFanIn] files. The planner fetches that times this, and the
     * multiplier exists because the two numbers are not the same thing:
     * candidates are not all groupable. A bucket's trailing remainder is
     * under the file minimum, a group another maintainer claims is
     * dropped, and a sorted group the spill or merge budget refuses is
     * not executed — so a fetch of exactly the run's capacity would plan
     * fewer groups than the run can execute.
     *
     * 2 is a headroom, not a model. It doubles the read to absorb those
     * losses. [maxCandidates] is what actually caps the product at the
     * production settings, which is deliberate — see [candidateBudget].
     */
    val candidateHeadroom: Int = DEFAULT_CANDIDATE_HEADROOM,
    /**
     * The HARD CAP on any one table plan's candidate read
     * (`HOGLAKE_COMPACTION_MAX_CANDIDATES`).
     *
     * Two things sit under it: the bucket loop's budget
     * ([candidateBudget] is the minimum of the two), and the
     * no-published-generation fallback, which has no bucket list to
     * scope by and reads a size-ordered prefix of the table.
     *
     * The number that matters about it is that it EXISTS. The statement
     * it caps used to read every live file of the table under the target,
     * which is what made a 9.9M-row table's sweep die inside its own
     * planning transaction.
     */
    val maxCandidates: Int = DEFAULT_MAX_CANDIDATES,
    /**
     * How many of a sweep's planned groups are REWRITTEN AND COMMITTED
     * at once (`HOGLAKE_COMPACTION_PARALLEL_GROUPS`).
     *
     * **1 — off — is the default, and it is today's behaviour exactly**:
     * one worker, groups in plan order, no thread hop anywhere. An
     * existing deployment that sets nothing sees no change at all.
     *
     * # What it buys, and why the number is what it is
     *
     * A group costs a FIXED amount of wall time almost regardless of its
     * size, because the cost is object-store LATENCY, not bytes:
     * measured ~8.5 s per group on gigahog-prod-us for a 64-file group,
     * dominated by the serialized opens ([inputOpenParallelism] attacks
     * that half) plus the plan and the commit. Groups are independent —
     * they share no input file by construction (one pass of bin packing
     * over a disjoint partition of the candidates) — so the only
     * serialization point between them is the per-catalog commit lock,
     * which [CompactionService.commitGroup] takes for the metadata
     * transaction ALONE and never across the rewrite or the upload. N
     * groups therefore overlap N rewrites against one short critical
     * section.
     *
     * # Three costs an operator is buying with it
     *
     *  - **Database connections.** Each in-flight group needs a
     *    connection for its staging ticket and, briefly, one for its
     *    commit. Raising this above the JDBI pool's free capacity turns
     *    compaction into a connection-starvation source for the
     *    foreground.
     *  - **Sorted-path heap and spill disk** — see [sortedHeapBytes]
     *    and [spillBytes]. The heap budget is DIVIDED by this value, so
     *    N concurrent sorted groups cannot exceed what one was allowed;
     *    the price is smaller chunks (more spilled runs) and fewer
     *    trusted runs admitted per group, never a smaller group. The
     *    spill budget is PER GROUP, so N groups may stage N times it on
     *    the spill volume at once.
     *  - **Commit-lock pressure.** N groups queue their commits behind
     *    the same per-catalog lock that foreground writers use. Each
     *    wait is milliseconds of metadata, but N of them are N.
     */
    val parallelGroups: Int = DEFAULT_PARALLEL_GROUPS,
    /**
     * How many of ONE group's input files may be open at once
     * (`HOGLAKE_COMPACTION_PARALLEL_INPUT_OPENS`) — the other half of
     * the fixed per-group cost. See
     * `ParquetRewriter.forEachOpenedInput`: merge order and the
     * streaming memory bound are both preserved; only the `open` round
     * trips overlap.
     */
    val inputOpenParallelism: Int = ParquetRewriter.DEFAULT_INPUT_OPEN_PARALLELISM,
    /**
     * Whether a maintainer CLAIMS a group before rewriting it, so a
     * second maintainer's planner skips it (`hog_compaction_claim`, V15;
     * `HOGLAKE_COMPACTION_CLAIMS_ENABLED`).
     *
     * A claim is an OPTIMIZATION, never authorization — see
     * [CompactionClaimRepo]. Turning it off costs duplicated rewrites
     * between replicas and costs nothing else; the plan-to-commit
     * re-verification is the correctness backstop either way.
     */
    val claimsEnabled: Boolean = true,
    /**
     * How long a group claim is held before any maintainer may reclaim
     * it (`HOGLAKE_COMPACTION_CLAIM_TTL_SECONDS`).
     *
     * The lease covers queue time and execution for the entire plan.
     * The default is one hour. Set a longer lease if a full run can
     * exceed one hour. A dead worker delays its queued groups until expiry.
     * Each group refreshes its own lease before execution.
     */
    val claimTtlSeconds: Long = DEFAULT_CLAIM_TTL_SECONDS,
    /**
     * How long a claim survives once its group has COMMITTED
     * (`HOGLAKE_COMPACTION_COMMITTED_CLAIM_TTL_SECONDS`).
     *
     * A claim outlives its own group on purpose (see
     * `CompactionService.executeGroup`'s release), but not necessarily
     * for the full [claimTtlSeconds].
     *
     * # What it has to cover, in the units that matter
     *
     * The only reader a committed group's claim has left is a SIBLING
     * MAINTAINER'S PLAN formed before the commit — so the quantity to
     * cover is how old that plan can be, which is one sweep, not one
     * sweep INTERVAL. Those are wildly different numbers and the first
     * version of this knob confused them. A group costs a measured
     * ~8.5 s, so a [maxGroupsPerRun] of 64 at [parallelGroups] = 1 is a
     * sweep of roughly **544 s** — and a 120 s lease covered about a
     * fifth of it, leaving four fifths of the sibling's plans to arrive
     * at an expired claim and spend the rewrite anyway.
     *
     * 600 s covers that sweep with margin and stays inside the default
     * [claimTtlSeconds] of 3600.
     *
     * # What it costs
     *
     * Rows, and the planner reads their `input_file_ids` on every pass.
     * The amplification is `committed groups per sweep x lease / sweep
     * duration`, which at the same settings is 64 x 600/544, or about
     * **70 rows per table** — not the ~2,000 an earlier version of this
     * comment arrived at by dividing the lease by the sweep INTERVAL
     * instead of by the sweep's duration. Seventy rows is not a number
     * worth trading correctness of the window for.
     *
     * Nothing breaks if it is too short: the sibling's stale plan then
     * arrives, takes the claim, rewrites, and loses its commit
     * re-verification — which is exactly today's behaviour without
     * claims. Too long costs rows and nothing else.
     */
    val committedClaimTtlSeconds: Long = DEFAULT_COMMITTED_CLAIM_TTL_SECONDS,
    /**
     * The admission bound each group's COMMIT queues under
     * (`HOGLAKE_COMMIT_LOCK_TIMEOUT_MS`, shared with the commit path).
     *
     * Compaction commits used to queue on the per-catalog lock with NO
     * bound, which AGENT.md already called out as the exception to
     * "every acquirer passes `commitLockTimeoutMs`". Under
     * [parallelGroups] > 1 the exception stops being survivable: each
     * waiting worker holds a pooled connection for the whole wait, so N
     * workers queued behind a slow commit take N connections out of a
     * pool the foreground shares — and a foreground writer that cannot
     * get a CONNECTION fails with a Hikari timeout (a 500) instead of
     * the typed, retryable [HoglakeException.CommitQueueTimeout] (a 503
     * with Retry-After) that the admission contract promises.
     *
     * Bounding it puts the failure back on compaction's side of the
     * line, where it is a counted skip: the timeout is caught per group,
     * counted with the plan-to-commit races, and the staged output is
     * left undrained for the cleanup drain, which is what every other
     * race already does. 0 restores the unbounded wait.
     */
    val commitLockTimeoutMs: Long = 0,
    /**
     * How far the sorted rewrite's CHUNK is shrunk for a table with nested
     * columns (HOGLAKE_COMPACTION_NESTED_SORT_EXPANSION).
     *
     * A chunk holds its rows as parquet-java `Group` objects so it can
     * sort them. For NESTED rows that object graph runs far ahead of the
     * bytes: a measured `list<long>` table with five elements per row
     * peaked at 343 MiB of heap from a 4.6 MiB compressed input — **70x**
     * — because every element becomes its own `SimpleGroup` with its own
     * object header, field array and boxed value, and compression that
     * packs an int64 column 10:1 does nothing for object headers.
     *
     * A nested row's node count is not knowable from the catalog (list
     * lengths are data), so [spillChunkRows]' per-node accounting cannot
     * see it. This expansion is what covers the gap: for a table with BOTH
     * nested columns and a live sort order the chunk's row count is
     * divided by it. 64 is deliberately near the top of the measured
     * 30-70x range — erring large costs more, smaller spill files, erring
     * small costs an OOM in a background loop.
     *
     * That is its ONLY role since hoglake#134. It used to size GROUPS (the
     * sorted row ceiling, and before that a derated byte target), because
     * the sort held the whole group; a group is now bounded by
     * [targetBytes] alone and only the chunk is materialized.
     *
     * It bounds the sorted path's CHUNK only: one pathological ROW (a
     * million-element list) still materializes whole on either path, and
     * nothing here changes that — that is [maxNodesPerRow]'s job.
     */
    val nestedSortExpansion: Int = DEFAULT_NESTED_SORT_EXPANSION,
    /**
     * How much HEAP one sorted rewrite may hold, PER PHASE
     * (HOGLAKE_COMPACTION_SORTED_HEAP_BYTES), divided by
     * [parallelGroups] ([sortedHeapBytesPerGroup]).
     *
     * A sorted rewrite is an external merge sort (hoglake#134;
     * `ParquetRewriter`'s class doc and `ExternalMergeSort`'s carry the
     * mechanics) with two phases that never overlap, and this one number
     * bounds each of them:
     *
     *  - **CHUNK phase.** Survivors of every input not read as a trusted
     *    run are read into a chunk of [spillChunkRows] rows, sorted, and
     *    spilled to [spillDir]. The chunk's object graph is what this
     *    bounds, through the measured per-node cost
     *    ([SORTED_HEAP_BYTES_PER_NODE]) and, for nested tables,
     *    [nestedSortExpansion].
     *  - **MERGE phase.** Every run — a trusted input read in place, or a
     *    spill file — holds one row group while it is read. Trusted
     *    inputs are ADMITTED largest first while the projected cost of
     *    all runs fits this budget; the rest are demoted to the spill
     *    path. A group whose spilled runs alone exceed it is refused in
     *    metadata (`merge_budget_exceeded`), which only configuration can
     *    cause.
     *
     * What it no longer does is bound the GROUP. A sorted group packs to
     * [targetBytes] like any other; this knob decides how many spill files
     * that takes and how many trusted runs fit beside them, i.e. how much
     * local disk work a group costs, never whether it forms.
     *
     * # The default, and the pod it assumes
     *
     * 1 GiB, which fits the 4 GiB maintenance pod (~2.8 GiB of heap at
     * the image's `MaxRAMPercentage=70`, server/build.gradle.kts). The
     * honest per-process peak is the larger of two phases, + the
     * hydrator's 256 MiB whole-object ceiling if it fires in the same tick:
     * the MERGE, `sortedHeapBytes` (the admitted runs at their charge) +
     * per group in flight the OUTPUT writer's buffered row group (flat at
     * ~140 MiB, ~1.1x the 128 MiB output row group) and the 16 MiB S3
     * part buffer; and the CHUNK phase, the chunk (~0.79x its charge,
     * flat ~82 MB at 50k-row chunks however large the group) + the
     * input's row group (up to a target-sized 512 MiB) + the spill
     * writer's block and the sort's reference array. With a target-sized
     * single-row-group input the chunk phase is the larger, and the
     * default's peak is ~1614 MiB, 56% of that heap. The writer term used
     * to grow ~40 MB per million rows (643 MB at 8M rows) because its
     * statistics pinned input pages; the materializer's copy of every
     * binary value removed that (see ParquetRewriter's `unpinned`). The
     * merge's per-run charge is conservative (~17.4 MiB charged against
     * ~6.6 MB measured per spilled run). server/README.md carries the
     * measurements and the pod table.
     *
     * Bigger pods take a bigger value, which buys larger chunks (fewer
     * spill files) and more trusted runs read in place. Raising it WITHOUT
     * raising the pod is how the OOM this bound exists to prevent comes
     * back.
     */
    val sortedHeapBytes: Long = DEFAULT_SORTED_HEAP_BYTES,
    /**
     * How many bytes one sorted rewrite may write to [spillDir]
     * (HOGLAKE_COMPACTION_SPILL_BYTES), PER GROUP — N groups in flight
     * may stage N times this.
     *
     * The bound is in-process because nothing outside the process will
     * raise it as an error: the spill directory is an emptyDir in the
     * chart, an overrun of its `sizeLimit` is a kubelet EVICTION of the
     * whole pod (found by a periodic scan, after the overshoot), and
     * `FileStore.usableSpace` reports the node's disk rather than the
     * limit. So a group is refused in METADATA when the registered bytes
     * its spill path would read exceed this (`spill_budget_exceeded`,
     * no IO spent), and stopped mid-run, output discarded, the moment
     * the bytes actually written would cross it.
     *
     * 4 GiB default, against an expected footprint of under ~0.9 GiB for
     * a 512 MiB group of zstd client inputs (snappy spill of zstd input
     * runs ~1.6x its bytes): headroom for a denser input, small enough
     * that the chart's default 10 GiB `/tmp` holds two groups' worth.
     * Size the volume as `parallelGroups x spillBytes` with margin.
     */
    val spillBytes: Long = DEFAULT_SPILL_BYTES,
    /**
     * Where sorted rewrites stage their spill files
     * (HOGLAKE_COMPACTION_SPILL_DIR, default `java.io.tmpdir`), one
     * `hoglake-compaction-spill-<uuid>` directory per rewrite, removed on every exit path.
     *
     * Must exist and be writable at BOOT ([SpillDirectory.requireUsable],
     * on a pod whose compaction loop runs), because the
     * alternative is worse than a crash loop: a missing directory fails
     * every sorted group's first spill with `NoSuchFileException`, which
     * the sweep counts as `failed_groups` and retries forever. Leftovers
     * of a killed process are removed once at startup
     * ([SpillDirectory.sweepLeftovers]).
     */
    val spillDir: java.nio.file.Path = java.nio.file.Path.of(System.getProperty("java.io.tmpdir")),
    /**
     * The smallest registered input a sorted rewrite VERIFIES as already
     * sorted (HOGLAKE_COMPACTION_VERIFY_MIN_BYTES); smaller inputs go
     * straight to the chunk phase, counted `files_unchecked`. 0 verifies
     * every input of known size (an unknown size, 0, is never verified).
     *
     * The trade, per input. Verifying it costs `1 + keys x row groups`
     * ranged GETs — the footer, then one per sort-key column chunk per row
     * group, because the chunks are not adjacent and each becomes its own
     * request — and merging it as a run costs a second footer read at the
     * reopen plus a merge slot; against the chunk path's one read, one
     * local spill write and one local read back. A writer with many small
     * row groups multiplies the first term: DuckDB's 122,880-row default
     * is 40+ row groups in a 500 MB file, so 40+ GETs per key column. At 16 MiB, the spill
     * block, the local round trip costs about what the extra open does;
     * below it the chunk path wins outright, and at millions of small
     * files a day it is also what keeps the merge's fan-in small: a few
     * spilled runs instead of thousands of one-file runs the heap budget
     * would demote anyway. Metadata-trusted outputs are never checked, so
     * the floor does not apply to them.
     */
    val verifyMinBytes: Long = DEFAULT_VERIFY_MIN_BYTES,
    /**
     * Per-ROW node budget for the rewrite
     * (HOGLAKE_COMPACTION_MAX_NODES_PER_ROW). [nestedSortExpansion]
     * bounds a GROUP's materialized heap; this bounds a single ROW's,
     * which no group budget can. See
     * ParquetRewriter.DEFAULT_MAX_NODES_PER_ROW.
     */
    val maxNodesPerRow: Int = ParquetRewriter.DEFAULT_MAX_NODES_PER_ROW,
    /**
     * The compression codec (and zstd level) compaction outputs are
     * written with — `HOGLAKE_COMPACTION_CODEC` /
     * `HOGLAKE_COMPACTION_ZSTD_LEVEL`. See
     * [ParquetRewriter.OutputCodec] for why zstd and why the level is
     * pinned; the short version is that compaction rewrites a table's
     * rows into target-sized files and then leaves them alone, so this
     * is the codec a compacted table is stored and scanned under from
     * then on, not a per-file detail.
     */
    val codec: ParquetRewriter.OutputCodec = ParquetRewriter.OutputCodec(),
) {
    init {
        CompactionGrouping.of(targetBytes)
        // Validated HERE, at construction, which is boot — not in
        // CompactionGrouping.groups, which is reached once per bucket per
        // table per sweep. A bad value caught there throws out of
        // planSnapshot, which is outside the per-group catch, so it kills
        // the whole sweep for every catalog on every interval while the
        // process still looks healthy.
        require(minInputFiles >= 2) {
            "HOGLAKE_COMPACTION_MIN_INPUT_FILES must be at least 2, got $minInputFiles: " +
                "a group of one file is a copy, not a compaction"
        }
        require(maxInputFiles >= minInputFiles) {
            "HOGLAKE_COMPACTION_MAX_INPUT_FILES $maxInputFiles is below " +
                "HOGLAKE_COMPACTION_MIN_INPUT_FILES $minInputFiles: no group could form"
        }
        require(nestedSortExpansion >= 1) { "nested sort expansion must be at least 1" }
        require(maxNodesPerRow >= 1) { "max nodes per row must be at least 1" }
        require(sortedHeapBytes >= 1) { "sorted heap bytes must be at least 1" }
        require(spillBytes >= 1) {
            "HOGLAKE_COMPACTION_SPILL_BYTES must be at least 1, got $spillBytes: a sorted group " +
                "that could spill nothing could never be rewritten"
        }
        require(verifyMinBytes >= 0) {
            "HOGLAKE_COMPACTION_VERIFY_MIN_BYTES must not be negative, got $verifyMinBytes " +
                "(0 verifies every input of known size)"
        }
        // Validated at CONSTRUCTION, which is boot, for the same reason
        // the file minimums are: a bad value caught inside the sweep
        // throws out of planSnapshot, outside the per-group catch, and
        // kills every catalog's sweep on every interval while the
        // process still looks healthy.
        require(parallelGroups >= 1) {
            "HOGLAKE_COMPACTION_PARALLEL_GROUPS must be at least 1, got $parallelGroups: " +
                "1 is the sequential default, and 0 would mean a sweep that executes nothing " +
                "while still reporting a clean run"
        }
        require(inputOpenParallelism >= 1) {
            "HOGLAKE_COMPACTION_PARALLEL_INPUT_OPENS must be at least 1, got $inputOpenParallelism"
        }
        require(claimTtlSeconds >= 1) {
            "HOGLAKE_COMPACTION_CLAIM_TTL_SECONDS must be at least 1, got $claimTtlSeconds: " +
                "a claim that expires the instant it is taken is worse than no claim, because " +
                "every planner still pays to read it"
        }
        require(committedClaimTtlSeconds >= 1) {
            "HOGLAKE_COMPACTION_COMMITTED_CLAIM_TTL_SECONDS must be at least 1, got " +
                "$committedClaimTtlSeconds"
        }
        // Validated at CONSTRUCTION, which is boot, like every knob
        // above: a zero here would make the planner fetch no candidates
        // at all and report clean, empty sweeps forever.
        // UPPER BOUNDS, not only lower ones: a fat-fingered
        // HOGLAKE_COMPACTION_MAX_CANDIDATES is the 2026-09-30 incident
        // again, and `candidateBudget`'s coercion prevents the overflow
        // but not the absurdity. The ceilings are an order of magnitude
        // above any value that has been reasoned about, so they refuse
        // a typo and nothing else.
        require(candidateHeadroom in 1..MAX_CANDIDATE_HEADROOM) {
            "HOGLAKE_COMPACTION_CANDIDATE_HEADROOM must be in 1..$MAX_CANDIDATE_HEADROOM, got " +
                "$candidateHeadroom: 0 would fetch no candidates and report a clean sweep that " +
                "compacted nothing, and a large multiplier reintroduces the unbounded planning " +
                "read this bound exists to stop"
        }
        require(maxCandidates in maxInputFiles..MAX_MAX_CANDIDATES) {
            "HOGLAKE_COMPACTION_MAX_CANDIDATES must be in " +
                "$maxInputFiles..$MAX_MAX_CANDIDATES, got $maxCandidates: below " +
                "HOGLAKE_COMPACTION_MAX_INPUT_FILES no plan could fill one group, and above the " +
                "ceiling one plan reads more rows than a run can possibly use — which is the " +
                "2026-09-30 planning read by another name"
        }
        require(maxFanIn >= maxInputFiles) {
            "HOGLAKE_COMPACTION_MAX_FAN_IN $maxFanIn is below " +
                "HOGLAKE_COMPACTION_MAX_INPUT_FILES $maxInputFiles: the fan-in ceiling cannot be " +
                "under its floor"
        }
    }

    /**
     * The fan-in cap for a group of files of THESE sizes —
     * `targetBytes / p50(sizes)`, held inside
     * `[maxInputFiles, maxFanIn]`.
     *
     * # Why the cap has to scale
     *
     * A fixed 64 is a FILE COUNT being asked to cap a BYTE target, which
     * is the same mistake the group MINIMUM already scales to avoid
     * (`CompactionGrouping.groups`' `need`). On gigahog-prod-us's
     * stray-day buckets the files are ~12 KiB, so a 64-file group
     * rewrites 768 KiB — 0.15% of the 512 MiB target — and retires 63
     * files. At `maxGroupsPerRun` 64 that is 4,032 files a run and
     * ~60k an hour, against ~216k files/hour arriving: compaction
     * 3.6x under water on arrivals alone, before touching a 9.9M-file
     * backlog. At 2,000 inputs the same bucket's group is ~24 MB, still
     * a twentieth of the target, and retires 1,999 files — ~1.9M an
     * hour, which outruns arrivals ~9x and drains the backlog in hours
     * rather than never.
     *
     * The other direction matters just as much: a bucket of 200 MB
     * files must still get a handful, or one group would try to rewrite
     * hundreds of gigabytes. `targetBytes / p50` gives 2 there and
     * thousands for the stray buckets, from one expression, so an
     * operator is not asked to choose a single number for both.
     *
     * # p50, and why the median rather than the mean
     *
     * The candidates of one bucket are already fetched when this is
     * called, so the median is free and exact over them. It is the
     * median rather than the mean because a bucket mixing one
     * near-target file with thousands of tiny ones is exactly the shape
     * a mean mis-sizes — and the packer's own dominance split is what
     * then keeps the big file out of the small files' group.
     *
     * It is a CEILING, not a promise: the byte target still closes a
     * group first if the bytes get there, and `need` still refuses one
     * too short.
     */
    fun effectiveMaxInputFiles(sizes: List<Long>): Int {
        if (sizes.isEmpty()) return maxInputFiles
        val p50 = sizes.sorted()[sizes.size / 2]
        if (p50 <= 0) return maxFanIn
        return (targetBytes / p50).coerceIn(maxInputFiles.toLong(), maxFanIn.toLong()).toInt()
    }

    /**
     * How many candidate rows one table's plan may fetch —
     * `min(candidateHeadroom x maxGroupsPerRun x maxFanIn,
     * maxCandidates)`.
     *
     * At the production settings the first term is 2 x 64 x 2,048 =
     * 262,144 and the second is 50,000, so [maxCandidates] is the
     * binding one — deliberately. The first term is what a run could
     * consume if every bucket held 12 KiB files; the second is what one
     * plan may READ, and reading a quarter of a million rows to plan a
     * sweep is the shape this change exists to remove. The consequence
     * is stated rather than hidden: on a table of tiny files a run can
     * execute more groups than one plan's candidates fill, so the
     * sweep is candidate-bound rather than budget-bound and the ledger
     * says so (`candidates_truncated`).
     *
     * Counted on ROWS ACTUALLY RETURNED rather than on what the sampler
     * predicted, so a stale sample cannot make the read unbounded.
     *
     * `Long` arithmetic, then floored into `Int`: every factor is
     * operator-settable and their product overflows a 32-bit
     * multiplication long before it means anything.
     */
    val candidateBudget: Int
        get() =
            minOf(
                candidateHeadroom.toLong() * maxGroupsPerRun.toLong() * maxFanIn.toLong(),
                maxCandidates.toLong(),
            )
                .coerceIn(maxInputFiles.toLong(), Int.MAX_VALUE.toLong())
                .toInt()

    /**
     * The sorted path's heap budget for ONE group, after the division
     * [parallelGroups] forces.
     *
     * [sortedHeapBytes] is a statement about this PROCESS's heap, and N
     * groups each sized to the whole heap is N times the heap. So it is
     * DIVIDED, not gated: every sorted group gets 1/N of it for its chunk
     * and for its merge, and N of them together are exactly the old
     * bound. The arithmetic is evaluated in metadata at planning time
     * (the merge-budget refusal) and again by the rewrite itself, so the
     * bound holds no matter how the sweep interleaves — no semaphore, no
     * permit held across object-store IO.
     *
     * The price is per group, and since hoglake#134 it is WORK, not size:
     * a smaller budget means smaller chunks (more spill files to merge)
     * and fewer trusted runs read in place. At the default parallelGroups
     * of 1 this is [sortedHeapBytes] unchanged.
     */
    val sortedHeapBytesPerGroup: Long
        get() = maxOf(1L, sortedHeapBytes / parallelGroups)

    /**
     * How many ROWS of [columns] one CHUNK of the sorted rewrite holds —
     * the chunk phase's heap bound ([sortedHeapBytesPerGroup]) in the unit
     * the heap actually holds. Only the arithmetic of the former sorted
     * row ceiling survives here: it used to bound a whole GROUP, and now
     * bounds one chunk (hoglake#134).
     *
     * A materialized row is not its bytes; it is a `SimpleGroup`, a
     * `List<Object>[]` field array, and then an `ArrayList` plus that
     * list's backing `Object[]` plus one boxed value for every populated
     * field. Measured at **151.6 bytes per node** for the flat event
     * shape this catalog holds (`SortedHeapMeasurement`, steady-state
     * retained heap over 200k rows);
     * [SORTED_HEAP_BYTES_PER_NODE] rounds that up, because erring large
     * costs smaller chunks and erring small costs an OOM in a background
     * loop — the same asymmetry [nestedSortExpansion] is calibrated on.
     *
     * Nodes are counted off the LIVE schema (every node of the forest,
     * plus one for the `_hog_row_id` carrier every output writes), which
     * is exact for flat tables and a floor for nested ones — hence the
     * [nestedSortExpansion] division, which is the only thing standing in
     * for list lengths the catalog cannot know.
     */
    fun spillChunkRows(columns: List<Column>): Long {
        val nodes = columns.allNodes().size + 1L // + the _hog_row_id carrier
        val perRow = SORTED_HEAP_BYTES_PER_NODE * nodes
        val ceiling = sortedHeapBytesPerGroup / perRow
        val nested = columns.allNodes().any { it.def.type.isNested }
        return maxOf(1L, if (nested) ceiling / nestedSortExpansion else ceiling)
    }

    /**
     * The bounds a sorted rewrite of a table shaped [columns] runs under:
     * [spillChunkRows], [sortedHeapBytesPerGroup] for the merge,
     * [spillBytes] and [spillDir]. One function, called by the planner's
     * metadata refusals and by the rewrite alike, so the two cannot
     * disagree about a group.
     */
    fun sortSpill(columns: List<Column>): SortSpill =
        SortSpill(
            chunkRows = spillChunkRows(columns),
            mergeBudgetBytes = sortedHeapBytesPerGroup,
            spillBudgetBytes = spillBytes,
            spillDir = spillDir,
            verifyMinBytes = verifyMinBytes,
        )

    companion object {
        /**
         * Erring at the top of the measured 30-70x expansion; see
         * [nestedSortExpansion].
         */
        const val DEFAULT_NESTED_SORT_EXPANSION = 64

        /**
         * Heap cost of one materialized parquet-java node, for
         * [spillChunkRows].
         *
         * Measured 151.6 B/node (`SortedHeapMeasurement`: 200k flat
         * 11-field event rows retained 333.5 MB, 1667 B/row); 192 rounds
         * up by a quarter to cover the less-settled samples, allocator
         * variance and the reference array `sortedWith` copies. A node is
         * one populated field: its `ArrayList`, that list's backing array
         * and one boxed value.
         */
        const val SORTED_HEAP_BYTES_PER_NODE = 192L

        /**
         * See [sortedHeapBytes]. 1 GiB: worst-case process peak ~1614 MiB
         * (the larger of the chunk phase with a target-sized input row
         * group and the merge with the flat ~140 MiB output writer, plus
         * the hydrator's whole-object ceiling), 56% of the 4 GiB
         * maintenance pod's ~2.8 GiB heap. server/README.md has the pod
         * table.
         */
        const val DEFAULT_SORTED_HEAP_BYTES = 1024L * 1024 * 1024

        /** See [spillBytes]: 4 GiB of local spill per sorted group. */
        const val DEFAULT_SPILL_BYTES = 4L * 1024 * 1024 * 1024

        /** See [verifyMinBytes]: the spill block, 16 MiB. */
        const val DEFAULT_VERIFY_MIN_BYTES = SortSpill.SPILL_BLOCK_BYTES.toLong()

        /**
         * See [parallelGroups]. 1 = today's sequential sweep, so an
         * existing deployment that sets nothing changes in no way.
         */
        const val DEFAULT_PARALLEL_GROUPS = 1

        /** See [claimTtlSeconds]: one hour for the full queue. */
        const val DEFAULT_CLAIM_TTL_SECONDS = 3600L

        /**
         * See [committedClaimTtlSeconds]: ten minutes, which covers a
         * full 64-group sweep (~544 s at the measured 8.5 s a group)
         * with margin and stays inside the full rewrite lease. The
         * quantity it has to cover is the AGE OF A SIBLING'S PLAN, which
         * is one sweep — not one sweep interval.
         */
        const val DEFAULT_COMMITTED_CLAIM_TTL_SECONDS = 600L

        /**
         * See [candidateHeadroom]: fetch twice the files a run can
         * possibly consume, because not every candidate is groupable.
         */
        const val DEFAULT_CANDIDATE_HEADROOM = 2

        /**
         * See [maxCandidates]: the hard cap on one table plan's
         * candidate read, and the binding term of [candidateBudget] at
         * the production settings.
         */
        const val DEFAULT_MAX_CANDIDATES = 50_000

        /**
         * See [maxFanIn]: 2,048 inputs per group, chosen from
         * `CompactionFanInMeasurement`'s measured commit-lock hold
         * (33.6 ms against a 50 ms per-group budget) rather than
         * guessed.
         */
        const val DEFAULT_MAX_FAN_IN = 2_048

        /**
         * The ceiling on [candidateHeadroom] — an order of magnitude
         * above the default, so it refuses a typo and nothing a person
         * has reasoned about.
         */
        const val MAX_CANDIDATE_HEADROOM = 32

        /**
         * The ceiling on [maxCandidates], for the same reason. 200,000
         * is four times the shipped default and still an order of
         * magnitude under the read this change removed, so it refuses a
         * typo without blessing a value nobody has reasoned about.
         */
        const val MAX_MAX_CANDIDATES = 200_000
    }
}

/** A candidate's live deletion vector, captured at planning time. */
data class LiveDv(
    val deleteFileId: Long,
    val path: String,
    val deleteCount: Long,
)

/** One live small file eligible for merging. */
data class CompactionCandidate(
    val dataFileId: Long,
    val path: String,
    val recordCount: Long,
    val fileSizeBytes: Long,
    /**
     * Registered thrift footer length, or null when the writer never
     * supplied one. A HINT for [S3InputFile]'s tail prefetch — it
     * decides whether opening the file costs a round trip, never
     * whether it reads correctly.
     */
    val footerSize: Long?,
    val rowIdStart: Long,
    /**
     * `hog_data_file.explicit_row_ids` — true when this file is a
     * COMPACTION OUTPUT and carries its row ids in a physical
     * `_hog_row_id` column, false when its ids are positional from
     * [rowIdStart].
     *
     * The authoritative answer to a question the rewriter was
     * previously guessing at from the file's own schema. It decides
     * whether a malformed `_hog_row_id` field is corruption (in a file
     * hoglake wrote, where the carrier is the identity) or an ordinary
     * client column that happens to share the name.
     */
    val explicitRowIds: Boolean = false,
    /** The file's live DV as planned; the rewrite APPLIES it. Null = none. */
    val dv: LiveDv? = null,
    /**
     * `hog_data_file.begin_snapshot` — the snapshot that registered the
     * file. Read on the candidate row itself (no extra descent) for the
     * sorted rewrite's trust predicate: an explicit-row-id file
     * registered at or after the live sort spec's `begin_snapshot` is a
     * compaction output written under that spec. 0 = unknown, which a
     * live spec's begin never is at or below, so it is never trusted.
     */
    val beginSnapshot: Long = 0,
) {
    /** Rows the rewrite will read from this file: registered records minus its planned DV's deletes. */
    val survivingRecords: Long get() = recordCount - (dv?.deleteCount ?: 0)
}

/** A greedy run of candidates sharing (spec_id, partition_values). */
data class CompactionGroup(
    val files: List<CompactionCandidate>,
    val specId: Long?,
    val partitionValues: List<String?>?,
) {
    val totalBytes: Long get() = files.sumOf { it.fileSizeBytes }

    /** Gross input rows, before DV application. */
    val totalRecords: Long get() = files.sumOf { it.recordCount }

    /** Rows the output will hold: gross minus the planned DVs' deletes. */
    val survivingRecords: Long get() = files.sumOf { it.recordCount - (it.dv?.deleteCount ?: 0) }
}

/** The metadata-only plan for one table. */
data class CompactionPlan(
    val tableId: Long,
    val namespace: String,
    val table: String,
    val groups: List<CompactionGroup>,
    /**
     * Sorted groups refused in METADATA because the registered bytes
     * their spill path would read exceed CompactionConfig.spillBytes
     * (`spill_budget_exceeded`). Decided from `file_size_bytes` by the
     * same arithmetic the rewrite applies (`ExternalMergeSort.admit`),
     * so a group that cannot fit the spill volume never spends a byte
     * of IO finding out — and never staged a byte toward the emptyDir
     * limit whose overrun evicts the pod.
     */
    val spillRefusedGroups: Long = 0,
    /**
     * Sorted groups refused in METADATA because their merge cannot fit
     * CompactionConfig.sortedHeapBytesPerGroup even with every trusted
     * input demoted to the spill path (`merge_budget_exceeded`): too
     * many spilled runs, each holding a row group while it is read.
     * Predicted from the registered survivor counts. Reachable only by
     * configuration — a heap budget small against the byte target.
     */
    val mergeRefusedGroups: Long = 0,
    /**
     * Groups this plan formed and then dropped because another
     * maintainer holds a LIVE claim over one or more of their input
     * files (`hog_compaction_claim`, V15).
     *
     * Not a conflict and not a failure — the other replica is rewriting
     * those files right now, and everything this one would spend on them
     * would be thrown away at its own commit. Counted so the value is
     * visible: on a two-replica fleet a steady nonzero here is the
     * feature working, and a steady ZERO with two replicas running means
     * claims are off or the planners are seeing different candidate
     * sets.
     *
     * Like [spillRefusedGroups] and [mergeRefusedGroups], and unlike
     * every other skip flavor, it spends no IO, so it does NOT consume
     * the run's group budget.
     */
    val claimedGroups: Long = 0,
    /**
     * Candidate file rows this plan actually read.
     *
     * The measure the planner's own defect had no series for. It is
     * bounded by CompactionConfig.candidateBudget on the bucket-scoped
     * path and by CompactionConfig.maxCandidates on the whole-table
     * fallback, and a value AT either bound means the table has debt
     * this plan could not see — which is what [candidatesTruncated]
     * says without the reader having to know the configuration.
     */
    val candidatesFetched: Long = 0,
    /**
     * Buckets this plan fetched candidates for, of the
     * [bucketsAvailable] the sample offered.
     *
     * Read the pair together: `considered` well under `available` on
     * every sweep means the candidate budget is the binding constraint
     * and the table's other buckets wait their turn (which is correct,
     * and is why the sample is ordered by debt), while `considered` ==
     * `available` means the plan saw the whole table.
     */
    val bucketsConsidered: Long = 0,
    /**
     * Buckets the sampler's published generation credits this table with
     * enough small files to form a group.
     *
     * Zero on the whole-table fallback's unpartitioned case — there are
     * no partition buckets — and zero for a catalog whose sampler has
     * published nothing yet, which is the same answer as "no debt" and
     * is why the fallback exists rather than a refusal.
     */
    val bucketsAvailable: Long = 0,
    /** 1 when the fetch hit its own cap; see [candidatesFetched]. */
    val candidatesTruncated: Long = 0,
    /**
     * Wall-clock milliseconds this plan took, all three phases.
     *
     * Recorded because the planner's failure mode was a TIME one: the
     * in-JVM packing grew past `idle_in_transaction_session_timeout`
     * while a transaction was open, and nothing in the ledger said the
     * plan had become slow before every sweep started dying.
     */
    val planMs: Long = 0,
)

/**
 * Server-side compaction (M4 — the maintenance-parity item the
 * predecessor never survived in production; README.md §4's
 * commit-storm history is the design constraint here).
 *
 * PLANNING is metadata-only, per table, and ONE-PASS
 * (CompactionGrouping): candidates are LIVE data files below the target
 * size — DV-bearing ones included, each carrying its live DV's identity
 * ([LiveDv]) so execution can apply it and commit can detect
 * supersession — bucketed by (spec_id, identical partition_values),
 * ordered by row_id_start. A bucket packs its files in that order until
 * their bytes reach compaction_target_bytes or the group holds
 * compaction_max_input_files, then closes and starts another; the
 * trailing remainder is a group too. A group is dropped if it holds
 * fewer than compaction_min_input_files, which is what makes this
 * TERMINATE: compression puts every output back under the target, so
 * without a file minimum outputs would merge with outputs forever.
 * Output size is estimated from input bytes; encoding and DV removal
 * can change it. A run plans each table before executing any of its
 * groups, so a file written this run is never an input in the SAME
 * run. UNLIKE the predecessor's
 * merge_adjacent_files, row-id ADJACENCY IS NOT REQUIRED — which is
 * exactly why outputs must materialize ids explicitly (ParquetRewriter).
 *
 * EXECUTION happens entirely OUTSIDE any catalog transaction: inputs
 * (and their DV puffin files) are fetched from the object store,
 * DV-applied/merged/re-shaped under the live schema/sorted locally, and
 * the output uploaded — only then does the group COMMIT open a
 * transaction. Applying a DV drops the deleted rows FOREVER (they were
 * deleted; every survivor keeps its original row id in `_hog_row_id`),
 * and the input's DV rows are end-snapshotted with the inputs — the DV
 * dies with its file. Changefeed semantics are untouched: compaction
 * outputs never appear in the feed. The commit is small metadata under
 * the per-catalog commit lock, following CommitService's tail shape
 * (allocate under lock via UPDATE..RETURNING, snapshot + typed change
 * row, no schema_version bump): foreground writers wait milliseconds,
 * never on S3 IO. Rate-awareness is by construction: max_groups_per_run
 * (default 1) per catalog per sweep — tiny bites, never the 60-180s
 * commit convoys of the 2026-09-04 incident.
 *
 * Plan-to-commit races: appenders never conflict with compaction (its
 * change kind, 'table_compacted', is invisible to the append conflict
 * check), but the DELETE state of an input can move under the plan: a
 * DV registered against a DV-free input, or a planned DV superseded by
 * a grown one, means the rewrite (which applied the PLANNED vectors)
 * would resurrect rows deleted after the plan's read. The commit
 * transaction therefore RE-VERIFIES, under the lock, that every input
 * is still live AND still carries exactly its planned DV (by
 * delete_file_id — supersession always mints a new row); any miss
 * aborts just that group (skipped, logged, counted as
 * skipped_conflicts or dv_superseded) and the next run re-plans. A
 * delete that happened after planning is NEVER dropped. The live sort
 * spec is re-verified the same way (by `sort_id`, null for unsorted):
 * a sorted table's compaction outputs are TRUSTED as already-sorted
 * runs by later rewrites when registered after the live spec began, so
 * an output sorted under a spec that changed mid-rewrite must not
 * register at all.
 *
 * Aborted-upload orphans: before uploading, the output path is
 * PRE-REGISTERED as an undrained hog_file_removal row (reason
 * 'compaction_staging') — a claim ticket. If the group commits, the
 * same transaction settles the ticket (drained_outcome 'registered')
 * before end of transaction, so cleanup's guard and the commit path
 * guard both see a settled row for a live path. If the group aborts —
 * skip, crash, failed upload — the ticket stays undrained and the
 * NORMAL cleanup drain reclaims the object (its liveness check passes:
 * the path was never registered). Because cleanup may legally drain the
 * ticket while the group is still in flight (it holds no lock at all
 * between upload and commit), the commit transaction first re-claims the
 * ticket — `FOR UPDATE`, and NOT OURS unless it is untouched
 * (`drained_at IS NULL AND claimed_at IS NULL AND attempts = 0`), because
 * a claimed or attempted row's object may already be gone and no lock can
 * stand between a DELETE nobody holds and this registration — and aborts
 * the group if cleanup got there first, re-staging the path so the object
 * can never outlive a ticket naming it. Re-plan next sweep, with a fresh
 * UUID path.
 *
 * Inputs are END-SNAPSHOTTED, never deleted: they remain visible to
 * time travel below the compaction snapshot, and expiry queues their
 * paths (and their dead DVs' paths) for physical removal once
 * end_snapshot falls under the retention floor — exactly the
 * superseded-DV lifecycle. Nothing else enters hog_file_removal here.
 *
 * Stats for the output come from the FOOTER THE REWRITE JUST WROTE
 * ([FooterStats.aggregate], the hydrator's own function): the parquet
 * writer accumulated them over exactly the rows it wrote, so they are
 * exact for the survivor set, cost no catalog read and no object read,
 * and are right for a group with DV'd inputs too. The output therefore
 * registers 'provided' whenever the writer handed back a footer, and
 * 'pending' only when it did not — the hydrator's ordinary path.
 */
class CompactionService(
    private val jdbi: Jdbi,
    private val store: ObjectStore,
    private val defaults: CompactionConfig,
) {
    private val log = KotlinLogging.logger {}

    /** The run ledger; records after the sweep resolves, never inside it. */
    private val runStore = MaintenanceRunStore(jdbi)

    /**
     * Last budget-refusal picture per table (spill and merge budget
     * refusals, hoglake#134; the sorted row ceiling's before them), so a
     * permanent condition is
     * logged when it CHANGES rather than on every sweep. One short string
     * per table that has ever been refused; the planner is the only
     * writer, but a manual sweep and the loop can plan concurrently, so
     * it is a concurrent map.
     *
     * Keyed by (catalog, table), NOT table alone. `table_id` is scoped
     * per catalog -- every catalog has a table 1 -- so a table-only key
     * let each non-refusing catalog's sweep `remove` the entry a refusing
     * catalog had just written. The result in production was the exact
     * per-sweep warning this map exists to stop: 302 identical lines in
     * 17 hours for one unchanged refusal.
     */
    private val lastBudgetRefusal = java.util.concurrent.ConcurrentHashMap<Pair<Long, Long>, String>()

    /** Everything execution needs beyond the group list. */
    private data class TableContext(
        val catalogId: Long,
        val dataPath: String,
        val namespace: String,
        val table: String,
        val tableId: Long,
        /** Live columns at the planning head — BINDING for the rewrite shape. */
        val columns: List<Column>,
        /** Live sort order — BINDING for the rewrite. Empty = row-id order. */
        val sortFields: List<SortFieldDef>,
        /**
         * The live sort spec's id at planning, null = unsorted. The commit
         * re-reads it under the catalog lock and refuses the group when it
         * moved (null <-> non-null included): an output sorted under a
         * spec that is no longer live would otherwise register with a
         * `begin_snapshot` past the NEW spec's and be trusted as a run of
         * it forever.
         */
        val sortId: Long? = null,
        /**
         * The live sort spec's `begin_snapshot`, null = unsorted: the
         * trust predicate's threshold (see [trustedSorted]).
         */
        val sortBeginSnapshot: Long? = null,
        /** Per-row node budget for the rewrite (CompactionConfig.maxNodesPerRow). */
        val maxNodesPerRow: Int = ParquetRewriter.DEFAULT_MAX_NODES_PER_ROW,
        /** Output compression for the rewrite (CompactionConfig.codec). */
        val codec: ParquetRewriter.OutputCodec = ParquetRewriter.OutputCodec(),
        /**
         * How many of a group's inputs the rewrite may OPEN at once
         * (CompactionConfig.inputOpenParallelism). Merge order and the
         * streaming memory bound are unaffected; only the object-store
         * round trips overlap.
         */
        val inputOpenParallelism: Int = ParquetRewriter.DEFAULT_INPUT_OPEN_PARALLELISM,
        /**
         * The commit's admission bound (CompactionConfig.commitLockTimeoutMs).
         * 0 = the unbounded wait this path used to take unconditionally.
         */
        val commitLockTimeoutMs: Long = 0,
        /**
         * The sorted rewrite's bounds; null exactly when [sortFields] is
         * empty, because the rewriter refuses a sort without them.
         */
        val sortSpill: SortSpill? = null,
    )

    private data class PlanWithContext(val ctx: TableContext, val plan: CompactionPlan)

    /** How one group's execution+commit resolved (the sweep's accounting unit). */
    internal sealed class GroupOutcome {
        data class Committed(val snapshotId: Long, val bytesOut: Long) : GroupOutcome()

        /** An input vanished/died, or cleanup reclaimed the staged output. */
        object SkippedConflict : GroupOutcome()

        /** An input's DV state moved since planning (grew/appeared/superseded). */
        object SkippedDvSuperseded : GroupOutcome()
    }

    // ---- planning --------------------------------------------------------

    /**
     * A hook the packing phase calls before it starts, so a test can
     * make that phase SLOW.
     *
     * The reason it exists is the bug this planner was rebuilt for: the
     * candidate read, the in-JVM bin packing and the claim read all ran
     * inside ONE transaction, and a connection sitting idle inside a
     * transaction is killed by `Database.SESSION_INIT_SQL`'s
     * `idle_in_transaction_session_timeout` (30 s in production). On
     * gigahog-prod-us's `ingest.events_raw` — ~9.9M candidate rows over
     * ~2,800 buckets — the packing took minutes and every sweep died on
     * the statement AFTER it with `FATAL: terminating connection due to
     * idle-in-transaction timeout`.
     *
     * No transaction remains open across packing. Execution plans hold
     * one idle connection for the session advisory lock. The test makes packing
     * take longer than the session's idle bound — hence a seam rather
     * than a sleep in production code. Default is a no-op, called
     * exactly once per table plan, and nothing but a test ever sets it.
     */
    internal var beforePacking: () -> Unit = {}

    /**
     * A hook phase (a) calls on its OWN handle, before the candidate
     * fetch — so a test can learn which backend the planner is using.
     *
     * The companion to [beforePacking], and it exists because the
     * property they test together is about a specific CONNECTION:
     * "nothing is held across the packing" is only checkable by asking
     * `pg_stat_activity` about the planner's backend from a DIFFERENT
     * one, and its pid is knowable only from inside the transaction.
     * A test that probed on the planner's own pooled connection would
     * be served that connection and see `active` whatever the code did
     * — which is exactly how the first version of that test passed
     * under the mutation it existed to catch.
     */
    internal var beforeFetch: (Handle) -> Unit = {}

    /**
     * A hook [commitGroup] calls on its OWN transaction's handle, once
     * the catalog commit lock is held and before the commit tail runs —
     * so a test can make a statement in that transaction FAIL for real.
     *
     * The property it exists for is [executeGroup]'s failure isolation,
     * and only a genuine driver exception tests it: the sweep has to
     * charge a `failed_groups` and carry on when a commit statement dies
     * of anything that is not a lock timeout (a `statement_timeout`
     * cancellation, SQLSTATE 57014, is the one production produced). A
     * fake exception thrown from Kotlin would not travel the same path,
     * because the arm under test is keyed on the driver's SQLSTATE.
     * Default is a no-op, called once per commit attempt, and nothing
     * but a test ever sets it.
     */
    internal var beforeCommitTail: (Handle) -> Unit = {}

    /**
     * A hook [compactGroup] calls with the [ParquetRewriter.Input]s it
     * built, just before the rewrite — so a test can check what each
     * input reads THROUGH (the sortedness pre-pass's key source and its
     * readahead), which no outcome of the rewrite reveals: a key source
     * at the merge's 8 MiB readahead is correct and only costs fetches.
     * Default is a no-op, called once per rewrite, and nothing but a test
     * ever sets it.
     */
    internal var beforeRewrite: (List<ParquetRewriter.Input>) -> Unit = {}

    /**
     * The smallest input row group the rewrite appends byte for byte
     * ([ParquetRewriter.APPEND_MIN_ROW_GROUP_BYTES], a constant, not a
     * knob). A var only so an integration test can append kilobyte
     * fixtures; nothing but a test ever sets it.
     */
    internal var appendFloorBytes: Long = ParquetRewriter.APPEND_MIN_ROW_GROUP_BYTES

    /** Public metadata-only planning for one table (also the test surface). */
    fun planTable(
        catalog: String,
        namespace: String,
        table: String,
        cfg: CompactionConfig = defaults,
    ): CompactionPlan = planSnapshot(catalog, namespace, table, cfg).plan

    /**
     * Plan one table in three phases on one connection. No transaction
     * remains open across packing. Execution plans also hold a session lock.
     *
     * # Why it is split, and what each phase may do
     *
     * This used to be one `inTransactionUnchecked` around the whole
     * thing. The in-JVM bin packing therefore ran with a connection
     * parked inside an open transaction, which
     * `idle_in_transaction_session_timeout` (30 s per
     * `Database.SESSION_INIT_SQL`) kills — and did, on every sweep of a
     * table whose candidate set had grown to ~9.9M rows. The rule that
     * replaces it: **a transaction may hold nothing but statements.**
     *
     *  - **(a) READ**, one short `REPEATABLE READ READ ONLY`
     *    transaction: resolve the catalog/namespace/table, the live
     *    columns and the live sort order at the catalog head, pick the
     *    buckets to work on, and fetch their candidate rows. Everything
     *    that must agree with everything else is in here, on ONE MVCC
     *    snapshot — the schema the rewrite will be shaped by, the sort
     *    spec whose `begin_snapshot` decides which files are trusted
     *    runs, and the files. That invariant is why this phase is a transaction
     *    and not three autocommit reads.
     *  - **(b) PACK**, outside a transaction: bin packing, a sorted
     *    group's spill and merge budget refusals, the WARN. Pure CPU
     *    over the rows phase (a) returned.
     *  - **(c) CLAIM READ**, one short autocommit read: which of this
     *    table's files another maintainer is already rewriting.
     *
     * # Why (c) is safe outside (a)'s snapshot
     *
     * Because a claim is an OPTIMIZATION, never authorization
     * (`CompactionClaimRepo`), and because the direction of the error is
     * the safe one. Read later than the candidates, this sees MORE
     * claims, never fewer: a claim taken during (b) is visible here and
     * its group is dropped as `claimed_elsewhere`, where the old shape
     * would have planned it and lost the rewrite at commit. It can
     * never miss a claim that (a)'s snapshot would have shown, because
     * claims are only deleted by a release or an expiry purge, and both
     * of those mean the group is no longer being rewritten — planning it
     * is then correct.
     *
     * # Why a file that died between (a) and (c) is still safe
     *
     * Nothing here is authorization either. A plan is a proposal; the
     * commit is what checks. `commitGroup` re-verifies, under the
     * per-catalog commit lock, that the staging ticket is untouched,
     * that the TABLE is not dropped (`hog_table.dropped_snapshot`),
     * that the live SORT SPEC is the one planned against
     * (`hog_sort_spec.sort_id`), that every input is still live (`end_snapshot IS NULL`) and that
     * every input still carries EXACTLY its planned deletion vector by
     * `delete_file_id`. A stale plan therefore costs one group —
     * `skipped_conflicts` or `dv_superseded` — and never a wrong commit.
     * That was already true of a plan the old shape held across minutes
     * of rewriting other groups; widening the window by the packing time
     * changes nothing about which check catches it.
     */
    private fun planSnapshot(
        catalog: String,
        namespace: String,
        table: String,
        cfg: CompactionConfig,
    ): PlanWithContext = jdbi.withHandleUnchecked { h -> planSnapshot(h, catalog, namespace, table, cfg) }

    private fun planSnapshot(
        h: Handle,
        catalog: String,
        namespace: String,
        table: String,
        cfg: CompactionConfig,
    ): PlanWithContext {
        val startedAt = System.nanoTime()
        // (a) One short read transaction: the table's identity, its
        // shape, and its candidate rows, all on one MVCC snapshot.
        val fetched =
            h.inTransaction<CandidateFetch, Exception> {
                h.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                // A BOUND TIGHTER THAN THE SESSION'S, because the
                // doctrine asks every statement inside a transaction for
                // one and the session's is 60 s
                // (`Database.SESSION_INIT_SQL`).
                //
                // 15 s, and the number is derived rather than picked: a
                // sweep plans one table at a time and executes its
                // groups before planning the next
                // (`doRunOnce`, for freshness), and a group costs a
                // measured ~8.5 s of object-store latency. So a plan
                // that spends more than about two groups' worth of wall
                // clock on metadata has stopped being the cheap half of
                // the sweep, and the honest outcome is to fail THIS
                // TABLE fast and let the sweep move on rather than to
                // sit on a statement whose bound is four minutes of
                // groups.
                //
                // It is a fail-fast, not a correctness bound: the throw
                // leaves the read-only transaction rolled back with
                // nothing staged, and `runOnceAllCatalogs` treats it as
                // a per-catalog failure the ledger counts. `SET LOCAL`,
                // so it expires with the transaction and the pooled
                // connection goes back carrying the session's 60 s.
                h.execute("SET LOCAL statement_timeout = '${PLAN_STATEMENT_TIMEOUT_MS}ms'")
                beforeFetch(h)
                fetchCandidates(h, catalog, namespace, table, cfg)
            }
        // (b) Packing outside a transaction. The hook is the test seam that
        // makes this phase slower than the session's idle bound; see
        // [beforePacking].
        beforePacking()
        val packed = pack(fetched, cfg)
        // (c) One short read for the sibling maintainer's claims.
        val free = withoutClaimedGroups(h, fetched.ctx, cfg, packed.groups)
        val planMs = (System.nanoTime() - startedAt) / 1_000_000
        return PlanWithContext(
            fetched.ctx,
            CompactionPlan(
                tableId = fetched.ctx.tableId,
                namespace = fetched.ctx.namespace,
                table = fetched.ctx.table,
                groups = free.groups,
                spillRefusedGroups = packed.spillRefused,
                mergeRefusedGroups = packed.mergeRefused,
                claimedGroups = free.claimed,
                candidatesFetched = fetched.candidatesFetched,
                bucketsConsidered = fetched.bucketsConsidered,
                bucketsAvailable = fetched.bucketsAvailable,
                candidatesTruncated = if (fetched.truncated) 1 else 0,
                planMs = planMs,
            ),
        )
    }

    /**
     * The table's identity and shape at the catalog head — everything
     * execution needs that is not a file row.
     *
     * Separate from the candidate fetch because [compactPlannedGroup]
     * wants exactly this and no plan: re-running a bounded fetch and a
     * whole pack to throw both away was what asking `planSnapshot` for
     * its context alone used to cost.
     */
    private fun tableContext(
        h: Handle,
        catalog: String,
        namespace: String,
        table: String,
        cfg: CompactionConfig,
    ): TableContext {
        val cat =
            CatalogRepo.findByName(h, catalog)
                ?: throw HoglakeException.NotFound("catalog '$catalog'")
        val ns =
            NamespaceRepo.findLiveByName(h, cat.catalogId, namespace)
                ?: throw HoglakeException.NotFound("namespace '$namespace' in catalog '$catalog'")
        val t =
            TableRepo.findLive(h, cat.catalogId, ns.namespaceId, table)
                ?: throw HoglakeException.NotFound("table '$namespace.$table' in catalog '$catalog'")
        val columns = TableRepo.columnsAt(h, cat.catalogId, t.tableId, cat.headSnapshotId)
        val sortSpec = SortRepo.sortSpecAt(h, cat.catalogId, t.tableId, cat.headSnapshotId)
        val sortFields = sortSpec?.fields ?: emptyList()
        return TableContext(
            catalogId = cat.catalogId,
            dataPath = cat.dataPath,
            namespace = ns.name,
            table = t.name,
            tableId = t.tableId,
            columns = columns,
            sortFields = sortFields,
            sortId = sortSpec?.sortId,
            sortBeginSnapshot = sortSpec?.beginSnapshot,
            maxNodesPerRow = cfg.maxNodesPerRow,
            codec = cfg.codec,
            inputOpenParallelism = cfg.inputOpenParallelism,
            commitLockTimeoutMs = cfg.commitLockTimeoutMs,
            sortSpill = if (sortFields.isEmpty()) null else cfg.sortSpill(columns),
        )
    }

    /**
     * Whether [f] may be read as an already-sorted RUN of [ctx]'s live
     * sort order instead of being spilled (hoglake#134).
     *
     * Both halves are load-bearing, and both are metadata:
     *
     *  - `explicit_row_ids`: only compaction writes it (registration has
     *    no such field and `CommitService` never sets it), and a
     *    compaction output of a sorted table is written sorted by the
     *    spec live at its plan. A client's sort order is ADVISORY and
     *    never verified (`schema.sql`), so no client file is trusted.
     *  - `begin_snapshot >= spec.begin_snapshot`: the output was
     *    committed under THIS spec. An output of an earlier spec, or of
     *    the table while it was unsorted, predates it and is spilled.
     *    What makes "committed after" mean "sorted under" is the commit's
     *    re-verification of the live `sort_id` ([commitGroup]): a group
     *    planned under one spec cannot commit under another.
     *
     * Never verified against the bytes (out of scope by decision): a
     * trusted run that is not key-sorted yields a mis-sorted output, not
     * an error. Unsorted tables trust nothing — there is no merge.
     */
    private fun trustedSorted(
        ctx: TableContext,
        f: CompactionCandidate,
    ): Boolean = f.explicitRowIds && ctx.sortBeginSnapshot != null && f.beginSnapshot >= ctx.sortBeginSnapshot

    /** A candidate bucket: one (spec, partition values) pair. */
    private data class Bucket(val specId: Long?, val values: List<String?>?)

    /** Phase (a)'s output: what to pack, and what it cost to find. */
    private data class CandidateFetch(
        val ctx: TableContext,
        /** The group byte budget this fetch filtered on: the target, for every table. */
        val budget: Long,
        val byBucket: Map<Bucket, List<CompactionCandidate>>,
        val candidatesFetched: Long,
        val bucketsConsidered: Long,
        val bucketsAvailable: Long,
        /** The fetch hit its own cap, so the table has debt this plan cannot see. */
        val truncated: Boolean,
    )

    /**
     * THE BOUNDED CANDIDATE READ, and the reason this class was
     * rewritten.
     *
     * # The shape it replaces
     *
     * One statement selected EVERY live file of the table under the
     * target, with a correlated `array_agg` over
     * `hog_file_partition_value` per row, into a Kotlin list. A run
     * rewrites at most `maxGroupsPerRun` groups of at most
     * `effectiveMaxInputFiles` files; on gigahog-prod-us the statement
     * fetched **9.9 million** rows and ran 9.9M correlated subqueries
     * doing it. The packing that followed took minutes with the
     * transaction still open, which is what the idle-in-transaction kill
     * was measuring.
     *
     * # ONE STATEMENT, and the LIMIT is on the ORDERED SCAN
     *
     * Every fetch here is [CANDIDATE_SQL]: an ordered index scan of
     * `hog_data_file_maintenance_size_scan (catalog_id, table_id,
     * file_size_bytes, data_file_id)` (V10), smallest first,
     * table-scoped, with the bucket's partition tuple applied as one
     * `EXISTS` arm per key against V23's index and the `LIMIT` on that
     * scan — so the read STOPS at the cap instead of materialising a
     * bucket and sorting it.
     *
     * The first version of this change drove the other way: an
     * `INTERSECT` of V23 index arms into a `hog_data_file` primary-key
     * probe per id, with `ORDER BY file_size_bytes` and the `LIMIT` on
     * top. That bounds the ROWS RETURNED and not the WORK: the sort key
     * is not obtainable from the intersect's order, so Postgres had to
     * materialise every id the bucket holds, probe the manifest for each
     * one and sort the survivors before the `LIMIT` applied. On a
     * ~100k-file bucket that is ~100k primary-key probes and a ~50 MB
     * sort — past `work_mem` — to return 8,192 rows. Worse, the arms
     * are CATALOG-scoped: `hog_file_partition_value` carries no
     * `table_id` (`schema.sql`), so an arm on `value = 'day-…'` returns
     * that day's files in *every* table of the catalog, and every
     * day-partitioned table writes the same day string.
     *
     * Leading on the manifest fixes both: the scan is table-scoped by
     * the index's own second column, it is already in the sort order, and
     * nothing is materialised. WHICH PLAN POSTGRES PICKS IS NOT FIXED,
     * and both are bounded — see [CANDIDATE_SQL].
     *
     * # Buckets first, from the published sample
     *
     * Groups never span a `(spec_id, partition_values)` bucket, so the
     * question "which files might this run rewrite" is really "which
     * BUCKETS are worth rewriting" — and the maintenance sampler has
     * already answered that per bucket in
     * `hog_maintenance_summary_tier`. See [sampledBuckets] for the order
     * and [bucketCursor] for why a strict order is not enough.
     *
     * The sample is allowed to be stale, and nothing here depends on it
     * being right. It decides WHERE TO LOOK; the files come from live
     * rows on phase (a)'s snapshot. A bucket that emptied since the
     * generation was published yields no candidates and no group.
     *
     * # Ordered by SIZE, and the reason is VALUE rather than access path
     *
     * The old statement ordered by `row_id_start`, and this one orders
     * by `file_size_bytes`. The reason is NOT that the old order lacked
     * an index — it has one, V10's `hog_data_file_maintenance_scan
     * (catalog_id, table_id, row_id_start, data_file_id)`, and a
     * measurement on a 250k-row fixture shows both orders served as
     * ordered index scans with no `Sort` node and near-identical cost
     * (106 ms against 107 ms). An earlier version of this KDoc claimed
     * otherwise and it was wrong.
     *
     * The reason is what a TRUNCATED prefix should contain. Both orders
     * let the `LIMIT` stop the scan; only one of them makes the rows it
     * stops on the right rows. Size-ascending is the order
     * `CompactionGrouping` consumes candidates in, and the smallest
     * files are where compaction buys the most — a group of 12 KiB
     * files retires 63 files for 768 KiB rewritten, a group of 200 MB
     * files retires the same 63 for up to 12.8 GB. So a prefix of the
     * table's smallest candidates is the most compactable part of it,
     * and a prefix in row-id order is an arbitrary slice of arrival
     * history.
     *
     * The consequence is handled rather than ignored: the rewriter's
     * input order must stay row-id order, and [rowIdOrder] restores it
     * after packing.
     *
     * # Three states, not two
     *
     *  - **no published generation** (a fresh install, the sampler's
     *    warm-up): there is no bucket list, so the fetch is the same
     *    statement with no `EXISTS` arms and the aggregated tuple
     *    projected per returned row, capped at
     *    [CompactionConfig.maxCandidates];
     *  - **a published generation with no qualifying bucket**: THERE IS
     *    NO WORK. Return an empty fetch. The first version of this
     *    change conflated this with the state above and ran the
     *    50,000-row whole-table statement, every sweep, forever, for a
     *    table whose debt is real but spread thinner than the file
     *    minimum — which is the one statement in this change that still
     *    carries the per-row `array_agg` the incident was about;
     *  - **a published generation with buckets**: the bucket loop.
     */
    private fun fetchCandidates(
        h: Handle,
        catalog: String,
        namespace: String,
        table: String,
        cfg: CompactionConfig,
    ): CandidateFetch {
        val ctx = tableContext(h, catalog, namespace, table, cfg)
        // THE TARGET, FOR EVERY TABLE. A sorted table's candidates used to
        // be filtered and packed under a row ceiling (and, nested, a
        // derated byte budget), because the sorted rewrite held the whole
        // group in heap. It is an external merge sort now (hoglake#134),
        // so a sorted group's heap is bounded by its CHUNK and its runs,
        // not by its size; what a sorted group can still be refused for
        // is decided per group in [pack], in metadata.
        val budget = cfg.targetBytes

        fun empty(
            available: Long = 0,
            truncated: Boolean = false,
        ) = CandidateFetch(ctx, budget, emptyMap(), 0, 0, available, truncated)
        // The scalar rewriter cannot preserve VARIANT groups yet. Do not enqueue
        // work that could drop payloads or repeatedly fail the maintenance loop.
        // allNodes, not the top level: a variant nested inside a struct
        // is still a variant the rewriter cannot write, and `struct{v:
        // variant}` has no top-level one. #77's check predates
        // containers, where the two were the same question.
        //
        // Checked BEFORE the fetch, not after: a variant table's
        // candidate rows were read and thrown away on every sweep.
        if (ctx.columns.allNodes().any { it.def.type == ColType.VARIANT }) return empty()

        // Does a published generation exist AT ALL? That is a different
        // question from "does it credit this table with a compactable
        // bucket", and the two have opposite answers.
        if (TierTotalsRepo.publishedGeneration(h, ctx.catalogId) == null) {
            return fetchWholeTable(h, ctx, budget, cfg)
        }
        val sampled = sampledBuckets(h, ctx, cfg)
        if (sampled.isEmpty()) return empty()

        val candidateBudget = cfg.candidateBudget
        // THE NUMBER OF STATEMENTS IS BOUNDED TOO, not only the rows.
        // A sample that credits 2,800 buckets a large compaction has
        // since emptied would otherwise make one plan issue 2,800 round
        // trips and never reach the row cap — the doctrine bounds the
        // WORK per run, not only what it returns. A bucket cannot yield
        // a group with fewer than `minInputFiles` files, so a budget of
        // N rows cannot be spent by more than N/minInputFiles useful
        // buckets.
        val bucketLimit = maxOf(1, candidateBudget / cfg.minInputFiles)
        val byBucket = LinkedHashMap<Bucket, List<CompactionCandidate>>()
        var fetched = 0L
        var considered = 0L
        var truncated = false
        for (bucket in rotated(ctx, sampled)) {
            if (fetched >= candidateBudget || considered >= bucketLimit) {
                truncated = true
                break
            }
            // `room + 1` so the truncation flag is EXACT: a bucket that
            // holds exactly its remaining room was truncated by nothing,
            // and a bucket that holds one more was. Trimmed below.
            val room = (candidateBudget - fetched).toInt()
            val rows = bucketCandidates(h, ctx, budget, cfg, bucket.bucket, room + 1)
            considered++
            if (rows.size > room) truncated = true
            val taken = if (rows.size > room) rows.subList(0, room) else rows
            fetched += taken.size
            if (taken.isNotEmpty()) byBucket[bucket.bucket] = taken
            rememberCursor(ctx, bucket.bucket)
        }
        return CandidateFetch(
            ctx = ctx,
            budget = budget,
            byBucket = byBucket,
            candidatesFetched = fetched,
            bucketsConsidered = considered,
            bucketsAvailable = sampled.size.toLong(),
            truncated = truncated,
        )
    }

    /** One sampled bucket and the debt the sample credits it with. */
    private data class SampledBucket(
        val bucket: Bucket,
        /** Files the sampler's mirror of the packer would put in a group. */
        val selected: Long,
        val smallCount: Long,
        val smallBytes: Long,
    )

    /**
     * This table's buckets in the sampler's PUBLISHED generation that
     * are worth rewriting, BEST VALUE FIRST.
     *
     * # The order is compaction VALUE, not file population
     *
     * The first version of this change ordered by `small_count DESC`,
     * which ranks buckets by raw file count — and on
     * gigahog-prod-us's `ingest.events_raw` that is the FRESH-DAY
     * buckets (~100k files of up to 200 MB), not the stray-day buckets
     * (~3.5k files of ~12 KiB each) that hold the 9.9M-file problem.
     * Files removed per byte rewritten differs by four orders of
     * magnitude between the two, and `small_count DESC` picks the wrong
     * end — then keeps picking it, because a day's bucket outranks every
     * stray bucket for about a day and the next day's grows into the
     * same position.
     *
     * One FLOOR and two SORT KEYS:
     *
     *  - **the floor is `selected >= 2`**, and both halves of that are
     *    deliberate.
     *    `selected` (`MaintenanceSummarySampler`'s per-bucket
     *    accumulator) is the number of files that would land in groups
     *    the planner would ACTUALLY TAKE — the sampler mirrors the byte
     *    rule, the dominance split and the scaling `need` minimum to
     *    compute it — so it answers "is there actionable work here"
     *    rather than "how many files are there", which is what
     *    `small_count` answered.
     *    And the threshold is 2, not `minInputFiles`, because
     *    `CompactionGrouping`'s minimum SCALES: a group needs
     *    `max(2, min(minInputFiles, targetBytes / its largest file))`
     *    files, so a bucket of three 300 MB files under a 512 MiB
     *    target is groupable at `need = 2` while `small_count` and
     *    `selected` are both 3. A floor of `minInputFiles` would have
     *    excluded it from the fetch forever while the compaction-debt
     *    page went on reporting its debt. 2 is the floor the packer
     *    itself cannot go below — one file is a copy, not a
     *    compaction — so it is the only threshold that cannot exclude
     *    groupable work.
     *  - **the first sort key is BYTES PER FILE, ascending**
     *    (`small_bytes / small_count`), which is files removed per byte
     *    rewritten, descending. It is the measure the whole change
     *    exists to serve: a group of 12 KiB files rewrites 768 KiB to
     *    retire 63 files (~84 files per MB), a group of 200 MB files
     *    rewrites up to 12.8 GB to retire the same 63 (~0.005 per MB).
     *  - **the second is `selected`, descending**: among buckets of
     *    comparably sized files, take the one with the most actionable
     *    work.
     *
     * `GREATEST(small_count, 1)` because the floor above admits only
     * buckets with files, and the guard costs nothing.
     *
     * # The order is also what keeps the FETCH bounded
     *
     * This is not only a throughput argument. [candidateSql] leads on
     * V10's size index and lets the `LIMIT` stop the scan, which is only
     * cheap while the bucket's files are among the table's SMALLEST: a
     * probe for a bucket of near-target files, with smaller files
     * elsewhere in the table, has to walk past all of them, and
     * Postgres then picks a whole-table plan instead (measured on
     * `CompactionCandidateFetchPlanIntegrationTest`'s fixture: a
     * hash join over a sequential scan of the table). Ordering by
     * bytes-per-file ascending makes that combination UNREACHABLE while
     * smaller files exist — the buckets whose files are the table's
     * smallest are exactly the ones probed first — so the ordering and
     * the plan's bound are the same decision.
     *
     * # It is LIMITED, because the doctrine bounds every fetch
     *
     * One row per partition bucket of one table, reached through V22's
     * `hog_maintenance_summary_tier_table (catalog_id, generation,
     * table_id)` index, is small for a day-partitioned table — 2,804
     * rows, 2.2 ms, 87 buffers measured — and NOT small for a
     * `(team, day)` spec at 1,000 teams x 90 days, which is 90,000 rows
     * into one `HashAggregate` and sort per table per sweep. Partitions
     * are operator data; "buckets are few" is not a bound.
     *
     * So [MAX_SAMPLED_BUCKETS] caps it, and the order above is what
     * makes a cap safe: the rows that survive it are the ones the
     * planner would have probed first anyway. [bucketsAvailable] then
     * counts what was OFFERED after the cap, which is a number an
     * operator can still read against [bucketsConsidered] — and the
     * cap is an order of magnitude above the documented 5,000-bucket
     * stress shape, so it bites only where the unbounded read would
     * have hurt.
     *
     * The number of buckets the planner PROBES is bounded separately
     * and much more tightly, in [fetchCandidates].
     *
     * TUPLE-LESS BUCKETS ARE INCLUDED. A table partitioned mid-life
     * keeps one — every file written before the spec existed, with a
     * null `spec_id` and no `hog_file_partition_value` rows — and an
     * unpartitioned table is nothing but that bucket. Excluding it would
     * have left those files uncompactable forever, silently.
     *
     * The `GROUP BY` survives from `PartitionStatsService`'s reader for
     * the same reason it has one — the bucket key also hashes the
     * sampler's `quota`, so a generation written either side of a target
     * change can hold two rows for one bucket.
     */
    private fun sampledBuckets(
        h: Handle,
        ctx: TableContext,
        cfg: CompactionConfig,
    ): List<SampledBucket> =
        h.createQuery(
            """
            SELECT p.spec_id,
                   p.partition_values,
                   sum(p.selected) AS selected,
                   sum(p.small_count) AS small_count,
                   sum(p.small_bytes) AS small_bytes
              FROM hog_maintenance_summary_tier p
              ${TierTotalsRepo.PUBLISHED_GENERATION_JOIN}
             WHERE p.catalog_id = :catalogId
               AND p.table_id = :tableId
             GROUP BY p.spec_id, p.partition_values
            HAVING sum(p.selected) >= :minSelected
             ORDER BY sum(p.small_bytes)::float8 / GREATEST(sum(p.small_count), 1) ASC,
                      selected DESC,
                      small_count DESC
             LIMIT :bucketScanCap
            """,
        )
            .bind("catalogId", ctx.catalogId)
            .bind("tableId", ctx.tableId)
            // The packer's own absolute floor, not `minInputFiles`:
            // see the KDoc.
            .bind("minSelected", 2L)
            .bind("bucketScanCap", MAX_SAMPLED_BUCKETS)
            .map { rs, _ ->
                SampledBucket(
                    Bucket(
                        specId = rs.getObject("spec_id")?.let { (it as Number).toLong() },
                        values = (rs.getArray("partition_values")?.array as? Array<*>)?.map { it as String? },
                    ),
                    selected = rs.getLong("selected"),
                    smallCount = rs.getLong("small_count"),
                    smallBytes = rs.getLong("small_bytes"),
                )
            }
            .list()

    /**
     * Where the last sweep's bucket loop stopped, per (catalog, table).
     *
     * # Why a strict order is not enough
     *
     * Any total order over buckets starves its own tail. `selected DESC`
     * is a fixed point on gigahog-prod-us: yesterday's day bucket keeps
     * ~1.5M files and outranks every stray bucket for the ~25 hours it
     * takes to drain, by which time today's bucket has grown into the
     * same position — so the 2,790 stray buckets that hold the actual
     * problem are never reached. Ten sweeps of the first version's
     * ordering touched ONE bucket, ten times.
     *
     * So the order decides PRIORITY and the cursor decides WHERE THIS
     * SWEEP STARTS: the loop begins after the last bucket the previous
     * sweep planned and wraps, so every bucket the sample offers is
     * reached within `ceil(available / considered)` sweeps whatever the
     * order says.
     *
     * # In memory, and that is a deliberate limitation
     *
     * Keyed by (catalog, table) — `table_id` is scoped per catalog, so
     * a table-only key would let one catalog's sweep move another's
     * cursor (the bug `lastBudgetRefusal`'s key already records). It is
     * lost on restart and NOT shared between replicas, which costs a
     * repeated bucket after a deploy and nothing else: the cursor is
     * fairness, never correctness, and two replicas planning the same
     * bucket is what the group claims already arbitrate. Persisting it
     * would mean a write per plan on the maintenance path for a
     * tie-break.
     */
    private val bucketCursor = java.util.concurrent.ConcurrentHashMap<Pair<Long, Long>, Bucket>()

    /**
     * [sampled] rotated so this sweep starts after the bucket the last
     * one stopped on — the order is preserved, only the entry point
     * moves.
     */
    private fun rotated(
        ctx: TableContext,
        sampled: List<SampledBucket>,
    ): List<SampledBucket> {
        val last = bucketCursor[ctx.catalogId to ctx.tableId] ?: return sampled
        val at = sampled.indexOfFirst { it.bucket == last }
        // Not found: the bucket is gone from the sample (compacted away,
        // or the generation moved), so there is nothing to start after
        // and the natural order is right.
        if (at < 0) return sampled
        val from = (at + 1) % sampled.size
        return sampled.subList(from, sampled.size) + sampled.subList(0, from)
    }

    private fun rememberCursor(
        ctx: TableContext,
        bucket: Bucket,
    ) {
        bucketCursor[ctx.catalogId to ctx.tableId] = bucket
    }

    /**
     * ONE candidate statement, built for the shape of [arms].
     *
     * `internal` so the plan tests can EXPLAIN THIS STRING rather than a
     * retyped copy of it — a plan test that restates its query asserts
     * the plan of something no code path runs (AGENT.md: an index proves
     * itself against the query it serves).
     *
     * # The structure, and where each bound sits
     *
     * ```
     * SELECT … FROM (
     *     SELECT … FROM hog_data_file f0
     *      WHERE catalog_id = ? AND table_id = ?        -- V10's index, leading
     *        AND end_snapshot IS NULL
     *        AND file_size_bytes < ?                    -- V10's index, range
     *        [AND EXISTS (one arm per partition key)]    -- V23's index
     *      [ORDER BY file_size_bytes, data_file_id]      -- ONLY when there are no arms
     *      LIMIT ?                                       -- THE SCAN CAP
     * ) f
     * LEFT JOIN hog_delete_file dv …                    -- the planned DV
     * [LEFT JOIN LATERAL (array_agg of the tuple) …]     -- no-sample fallback only
     * [WHERE f.spec_id IS NOT DISTINCT FROM ?]           -- bucket identity
     * ORDER BY f.file_size_bytes, f.data_file_id         -- over at most the cap
     * LIMIT ?                                            -- THE RESULT CAP
     * ```
     *
     * The inner `LIMIT` is what makes the read bounded, and **the inner
     * `ORDER BY` is what would stop it being bounded** — so it is there
     * only in the case where it is free.
     *
     * With NO ARMS the sort key is V10's own index order
     * (`hog_data_file_maintenance_size_scan (catalog_id, table_id,
     * file_size_bytes, data_file_id)`), so ordering costs nothing: the
     * scan is an ordered index scan and the `LIMIT` stops it. Measured:
     * 239 buffers for 8,192 rows out of a 140,000-row table.
     *
     * With ARMS it is the opposite. A bucket's files are not a prefix of
     * the size order — they are interleaved with every other bucket's —
     * so `ORDER BY file_size_bytes` cannot be satisfied by any access
     * path that also applies the arms, and Postgres has to produce
     * **the whole bucket** before the `LIMIT` sees a row: measured on a
     * 20,000-file bucket of a 140,000-file table, a hash join over a
     * SEQUENTIAL SCAN of the manifest feeding an external-merge sort
     * that spilled 3 MB to disk. The first version of this statement had
     * that shape and its KDoc claimed the opposite.
     *
     * Dropping the inner `ORDER BY` removes the requirement, and the
     * `LIMIT` then stops whichever plan Postgres picks after the cap's
     * worth of MATCHING rows. What it costs is that the candidates are
     * an arbitrary `scanLimit` of the bucket rather than its smallest —
     * which is no loss in practice and no loss in principle:
     *
     *  - a bucket is one partition's ingest, so its files are within a
     *    factor of each other (they are all about one flush size), and
     *    "the smallest of them" is not a meaningful preference;
     *  - `CompactionGrouping` sorts by size ITSELF before packing, so
     *    group membership is still decided smallest-first over whatever
     *    arrives;
     *  - and it only bites at all when a bucket holds more than the cap,
     *    which for the ~3.5k-file stray-day buckets this change exists
     *    to drain it never does.
     *
     * The outer `ORDER BY` stays, over at most `limit` rows, so the
     * result is deterministic.
     *
     * # WHICH PLAN POSTGRES PICKS IS NOT FIXED, and both are bounded
     *
     * With a SELECTIVE bucket it rewrites the arms into a semi-join and
     * drives from V23's index, reading the bucket's entries index-only
     * and probing the manifest per id: bounded by the BUCKET, and the
     * right choice at that selectivity. With a bucket that is a large
     * fraction of the table it drives the manifest and applies the arms
     * as filters: bounded by the `LIMIT`, because there is no ordering
     * left to force materialization. The plan test measures both shapes
     * on one fixture and asserts the bound in each, rather than pinning
     * whichever plan the planner picks — which is the mistake the first
     * version's test made (it asserted "one primary-key probe per bucket
     * member", i.e. the unbounded shape, as the desired property).
     *
     * `value = ?` or `value IS NULL` per key, chosen by the sampled
     * tuple's own shape. A null partition value is legal (`schema.sql`:
     * "NULL = null partition value") and `IS NOT DISTINCT FROM` drives
     * no btree, while `IS NULL` is an ordinary range over the index's
     * null entries. Only the PREDICATE SHAPE is built from the tuple;
     * every value is bound.
     *
     * # PER-ROW COST on production's access pattern
     *
     * Measured on `CompactionCandidateFetchPlanIntegrationTest`'s
     * production-shaped fixture (3 tables in one catalog sharing day
     * values, 2,000 tiny buckets of 40 files at 200 rows/12 KiB plus 3
     * fresh buckets of 20,000 large files, 210,000 file rows), PG 18,
     * warm, serial, scan nodes only — the figures the test asserts
     * against are in its own KDoc. The bound to design against is the
     * CAP, not the bucket: the read is `min(candidateBudget,
     * maxCandidates)` rows however big the bucket is, which is what the
     * first version got wrong.
     */
    internal fun candidateSql(
        arms: List<Int>,
        nullArms: Set<Int>,
        scopeToSpec: Boolean,
        withPartitionValues: Boolean,
    ): String {
        val armSql =
            arms.joinToString("\n") { keyIndex ->
                val valuePredicate =
                    if (keyIndex in nullArms) "pv$keyIndex.value IS NULL" else "pv$keyIndex.value = :v$keyIndex"
                """
                       AND EXISTS (SELECT 1 FROM hog_file_partition_value pv$keyIndex
                                    WHERE pv$keyIndex.catalog_id = f0.catalog_id
                                      AND pv$keyIndex.data_file_id = f0.data_file_id
                                      AND pv$keyIndex.key_index = :k$keyIndex
                                      AND $valuePredicate)
                """.trimEnd()
            }
        // THE INNER ORDER BY EXISTS ONLY WHEN THERE ARE NO ARMS, and
        // that asymmetry is the whole bound. See the KDoc.
        val innerOrder =
            if (arms.isEmpty()) "\n                     ORDER BY f0.file_size_bytes, f0.data_file_id" else ""
        val specFilter = if (scopeToSpec) "\n             WHERE f.spec_id IS NOT DISTINCT FROM :specId" else ""
        val valuesProjection = if (withPartitionValues) ",\n                   pvs.partition_values" else ""
        val valuesJoin =
            if (withPartitionValues) {
                """
              LEFT JOIN LATERAL (
                    SELECT array_agg(pv.value ORDER BY pv.key_index) AS partition_values
                      FROM hog_file_partition_value pv
                     WHERE pv.catalog_id = f.catalog_id AND pv.data_file_id = f.data_file_id
                   ) pvs ON true
                """.trimEnd()
            } else {
                ""
            }
        return """
            SELECT f.data_file_id, f.path, f.record_count, f.file_size_bytes,
                   f.footer_size, f.row_id_start, f.explicit_row_ids,
                   f.begin_snapshot, f.spec_id,
                   dv.delete_file_id AS dv_id, dv.path AS dv_path, dv.delete_count AS dv_count$valuesProjection
              FROM (
                    SELECT f0.catalog_id, f0.data_file_id, f0.path, f0.record_count,
                           f0.file_size_bytes, f0.footer_size, f0.row_id_start,
                           f0.explicit_row_ids, f0.begin_snapshot, f0.spec_id
                      FROM hog_data_file f0
                     WHERE f0.catalog_id = :catalogId AND f0.table_id = :tableId
                       AND f0.end_snapshot IS NULL
                       AND f0.file_size_bytes < :targetBytes$armSql$innerOrder
                     LIMIT :scanLimit
                   ) f
              LEFT JOIN hog_delete_file dv
                ON dv.catalog_id = f.catalog_id
               AND dv.data_file_id = f.data_file_id
               AND dv.end_snapshot IS NULL$valuesJoin$specFilter
             ORDER BY f.file_size_bytes, f.data_file_id
             LIMIT :limit
            """
    }

    /**
     * One bucket's smallest [limit] live candidate files.
     *
     * A bucket WITH a tuple gets one `EXISTS` arm per key; the
     * TUPLE-LESS bucket — an unpartitioned table, or the vintage a
     * mid-life-partitioned table wrote before its spec — gets none, and
     * is separated from the partitioned buckets by the `spec_id` filter
     * alone.
     *
     * That is why the tuple-less case reads a SCAN CAP of
     * [CompactionConfig.maxCandidates] rather than its share of the
     * budget: `spec_id` has no index, so its filter sits outside the
     * inner `LIMIT`, and a cap of "this bucket's remaining room" would
     * read only that many of the table's smallest candidates and
     * probably find none of the vintage's. The cost is stated rather
     * than hidden: a table whose tuple-less files are all LARGER than
     * its 50,000 smallest candidates keeps that vintage's debt until the
     * smaller files drain. Size order is compaction's own order, and the
     * alternative is a partial index on a table the hottest write in
     * the system already pays two indexes for.
     */
    private fun bucketCandidates(
        h: Handle,
        ctx: TableContext,
        budget: Long,
        cfg: CompactionConfig,
        bucket: Bucket,
        limit: Int,
    ): List<CompactionCandidate> {
        val values = bucket.values ?: emptyList()
        val arms = values.indices.toList()
        val nullArms = values.indices.filter { values[it] == null }.toSet()
        val scanLimit = if (arms.isEmpty()) maxOf(limit, cfg.maxCandidates) else limit
        val query =
            h.createQuery(
                candidateSql(
                    arms = arms,
                    nullArms = nullArms,
                    scopeToSpec = true,
                    withPartitionValues = false,
                ),
            )
                .bind("catalogId", ctx.catalogId)
                .bind("tableId", ctx.tableId)
                .bind("targetBytes", budget)
                .bind("specId", bucket.specId)
                .bind("scanLimit", scanLimit)
                .bind("limit", limit)
        values.forEachIndexed { keyIndex, value ->
            query.bind("k$keyIndex", keyIndex)
            if (value != null) query.bind("v$keyIndex", value)
        }
        return query.map { rs, _ -> candidate(rs) }.list()
    }

    /**
     * The NO-SAMPLE fallback: the smallest
     * [CompactionConfig.maxCandidates] live candidates of the table,
     * with their partition tuples, bucketed in the JVM.
     *
     * Reached only when the catalog has no published generation at all —
     * a fresh install, or the sampler's warm-up. There is no bucket list
     * to scope by, so this is the one shape that must derive the buckets
     * itself, which is what the `array_agg` lateral is for. It runs at
     * most `maxCandidates` times, OUTSIDE the inner `LIMIT`, rather than
     * once per live file of the table: on gigahog-prod-us the statement
     * this replaces ran it 9.9 million times per table per sweep.
     */
    private fun fetchWholeTable(
        h: Handle,
        ctx: TableContext,
        budget: Long,
        cfg: CompactionConfig,
    ): CandidateFetch {
        val query =
            h.createQuery(
                candidateSql(
                    arms = emptyList(),
                    nullArms = emptySet(),
                    scopeToSpec = false,
                    withPartitionValues = true,
                ),
            )
                .bind("catalogId", ctx.catalogId)
                .bind("tableId", ctx.tableId)
                .bind("targetBytes", budget)
                .bind("scanLimit", cfg.maxCandidates)
                .bind("limit", cfg.maxCandidates)
        val rows =
            query
                .map { rs, _ ->
                    candidate(rs) to
                        Bucket(
                            specId = rs.getObject("spec_id")?.let { (it as Number).toLong() },
                            values =
                                (rs.getArray("partition_values")?.array as? Array<*>)
                                    ?.map { it as String? },
                        )
                }
                .list()
        val byBucket = rows.groupBy({ it.second }, { it.first })
        if (rows.size >= cfg.maxCandidates) {
            log.debug {
                "compaction candidate fetch for ${ctx.namespace}.${ctx.table} hit its cap of " +
                    "${cfg.maxCandidates} rows (HOGLAKE_COMPACTION_MAX_CANDIDATES); the table has " +
                    "debt this plan cannot see, and the next sweep sees the next-smallest files"
            }
        }
        return CandidateFetch(
            ctx = ctx,
            budget = budget,
            byBucket = byBucket,
            candidatesFetched = rows.size.toLong(),
            // BOTH ZERO, and that is the honest report: this path did
            // not pick buckets at all, so "considered" and "available"
            // have no value to state. The candidate count and the
            // truncation flag are what describe it, and a reader
            // separates the two paths by the zeros.
            bucketsConsidered = 0,
            bucketsAvailable = 0,
            truncated = rows.size >= cfg.maxCandidates,
        )
    }

    /** One candidate row. */
    private fun candidate(rs: ResultSet): CompactionCandidate =
        CompactionCandidate(
            dataFileId = rs.getLong("data_file_id"),
            path = rs.getString("path"),
            recordCount = rs.getLong("record_count"),
            fileSizeBytes = rs.getLong("file_size_bytes"),
            footerSize = rs.getObject("footer_size", java.lang.Long::class.java)?.toLong(),
            rowIdStart = rs.getLong("row_id_start"),
            explicitRowIds = rs.getBoolean("explicit_row_ids"),
            beginSnapshot = rs.getLong("begin_snapshot"),
            dv =
                rs.getObject("dv_id")?.let {
                    LiveDv(
                        deleteFileId = (it as Number).toLong(),
                        path = rs.getString("dv_path"),
                        deleteCount = rs.getLong("dv_count"),
                    )
                },
        )

    /**
     * What [pack] produced: what will be attempted, and the sorted groups
     * the spill and merge budgets refused in metadata.
     */
    private data class PackedGroups(
        val groups: List<CompactionGroup>,
        val spillRefused: Long,
        val mergeRefused: Long,
    )

    /** A candidate as sorted-rewrite admission sees it: catalog facts only. */
    private data class PlannedRun(
        override val trustedSorted: Boolean,
        override val fileSizeBytes: Long,
        override val survivingRecords: Long,
    ) : RunCandidate

    /** Why the planner refused a sorted group; see [budgetRefusal]. */
    private enum class BudgetRefusal { SPILL, MERGE }

    /**
     * Whether a SORTED group would be refused by the rewrite's own
     * budgets, decided from registered sizes and survivor counts before
     * any IO: [BudgetRefusal.SPILL] when the bytes its spill path would
     * read exceed [SortSpill.spillBudgetBytes], [BudgetRefusal.MERGE]
     * when its spilled runs alone exceed [SortSpill.mergeBudgetBytes].
     * Null = attempt it.
     *
     * THE REWRITER'S ARITHMETIC, NOT A COPY OF IT: this calls
     * [ExternalMergeSort.admit] with the same trust predicate, sizes and
     * survivors [compactGroup] hands the rewrite, so the planner cannot
     * refuse a group the rewrite would take or wave through one it would
     * refuse in the same way. The rewrite still checks for itself — from
     * the footers it opens and the bytes it actually writes — and those
     * hard stops reach [executeGroup]'s arms as the same two exceptions.
     *
     * WORST CASE about sortedness: only metadata-trusted files count as
     * runs here; every other file is assumed to spill. The rewrite's
     * pre-pass may VERIFY some of them as already sorted and read them in
     * place, and the planner cannot know which without reading them. So
     * this refuses only groups that would need more spill (or more
     * spilled runs) than the budgets allow even if nothing verified; a
     * group it admits can only do better at execution, never worse.
     */
    private fun budgetRefusal(
        ctx: TableContext,
        spill: SortSpill,
        leaves: Int,
        group: CompactionGroup,
    ): BudgetRefusal? {
        val runs = group.files.map { PlannedRun(trustedSorted(ctx, it), it.fileSizeBytes, it.survivingRecords) }
        val admission =
            try {
                ExternalMergeSort.admit(runs, spill, leaves)
            } catch (_: SpillBudgetExceededException) {
                return BudgetRefusal.SPILL
            }
        return if (admission.projectedBytes > spill.mergeBudgetBytes) BudgetRefusal.MERGE else null
    }

    /**
     * A group's files in ROW-ID order, which is not cosmetic.
     *
     * `CompactionGrouping` decides MEMBERSHIP on size and hands each
     * group back in the CALLER's order — which used to be row-id order,
     * because the candidate read was `ORDER BY row_id_start`. The
     * bounded read is ordered by SIZE now (for the reason
     * [fetchCandidates] gives: a truncated prefix should hold the
     * table's most compactable files, not an arbitrary slice of its
     * arrival history), so the order has to be restored here.
     *
     * What depends on it: the rewriter consumes inputs in the order it
     * is given, and for an UNSORTED table the output's row order IS
     * that order. Leaving the files in size order would silently
     * reorder every unsorted compaction output — legal, since ids are
     * explicit (invariant 2), and a gratuitous change to what readers
     * see.
     */
    private fun rowIdOrder(files: List<CompactionCandidate>): List<CompactionCandidate> =
        files.sortedWith(compareBy({ it.rowIdStart }, { it.dataFileId }))

    /**
     * PHASE (b): bin-pack the fetched candidates outside a transaction.
     *
     * Pure CPU by construction — every argument is already in memory —
     * which is the property [beforePacking] exists to test and the one
     * the idle-in-transaction kill was the absence of.
     */
    private fun pack(
        fetched: CandidateFetch,
        cfg: CompactionConfig,
    ): PackedGroups {
        val ctx = fetched.ctx
        // ONE CAPACITY: BYTES. The sorted path used to pack to a row
        // ceiling as well, because its rewrite held the whole group in
        // heap; on gigahog-prod-us's `ingest.events_raw` that closed
        // every group far short of the target. The rewrite is an external
        // merge sort now (hoglake#134) and a sorted group packs exactly
        // like an unsorted one.
        val grouping = CompactionGrouping.of(fetched.budget)
        val out = mutableListOf<CompactionGroup>()
        for ((bucket, candidates) in fetched.byBucket) {
            val packed =
                grouping.groups(
                    candidates,
                    cfg.minInputFiles,
                    // THE FAN-IN SCALES WITH THE FILES IT IS CAPPING.
                    // Computed per bucket from the rows just fetched, so
                    // a bucket of 12 KiB files gets thousands of inputs
                    // and a bucket of 200 MB files still gets a handful.
                    cfg.effectiveMaxInputFiles(candidates.map { it.fileSizeBytes }),
                ) { it.fileSizeBytes }
            for (take in packed) out += CompactionGroup(rowIdOrder(take), bucket.specId, bucket.values)
        }
        // A SORTED group the rewrite's own budgets would refuse is refused
        // HERE, in metadata, uncharged — see [budgetRefusal]. Only
        // configuration produces either (a spill budget small against the
        // byte target, a heap budget small against it), so the same groups
        // re-plan and re-refuse every sweep until an operator moves a knob.
        // The leaf count is the output schema's; a live schema the rewrite
        // cannot shape at all has no count, and its groups are left for
        // the rewrite to refuse with its own typed reason.
        val spill = ctx.sortSpill
        val leaves = spill?.let { runCatching { ParquetRewriter.outputLeafCount(ctx.columns) }.getOrNull() }
        val spillRefused = mutableListOf<CompactionGroup>()
        val mergeRefused = mutableListOf<CompactionGroup>()
        val fits =
            if (spill == null || leaves == null) {
                out
            } else {
                out.filter { group ->
                    when (budgetRefusal(ctx, spill, leaves, group)) {
                        BudgetRefusal.SPILL -> false.also { spillRefused += group }
                        BudgetRefusal.MERGE -> false.also { mergeRefused += group }
                        null -> true
                    }
                }
            }
        logBudgetRefusals(ctx, cfg, spillRefused, mergeRefused)
        // Most files first: every group now targets the same size, so the
        // one holding the most files buys the largest drop in file count
        // for the same bytes rewritten. Row-id order breaks ties, which
        // keeps a group's inputs adjacent in arrival order.
        return PackedGroups(
            fits.sortedWith(compareBy({ -it.files.size }, { it.files.first().rowIdStart })),
            spillRefused.size.toLong(),
            mergeRefused.size.toLong(),
        )
    }

    /**
     * WARN about a table's budget refusals when the picture CHANGES, not
     * on every sweep.
     *
     * A refused table is a STATE, not an event: the same files re-pack
     * and re-refuse every sweep until an operator moves a knob. Logging
     * that per sweep buried a burn-in in 289 identical warnings — 235 KB
     * — in three minutes for a single table (the row ceiling's refusal,
     * which this replaces). The metric and the ledger carry the rest.
     */
    private fun logBudgetRefusals(
        ctx: TableContext,
        cfg: CompactionConfig,
        spillRefused: List<CompactionGroup>,
        mergeRefused: List<CompactionGroup>,
    ) {
        val key = ctx.catalogId to ctx.tableId
        if (spillRefused.isEmpty() && mergeRefused.isEmpty()) {
            lastBudgetRefusal.remove(key)
            return
        }
        val worstSpill = spillRefused.maxByOrNull { it.totalBytes }
        val worstMerge = mergeRefused.maxByOrNull { it.survivingRecords }
        val signature =
            "spill=${spillRefused.size}/${worstSpill?.totalBytes ?: 0}/${cfg.spillBytes} " +
                "merge=${mergeRefused.size}/${worstMerge?.survivingRecords ?: 0}/${cfg.sortedHeapBytesPerGroup}"
        if (lastBudgetRefusal.put(key, signature) == signature) return
        if (worstSpill != null) {
            log.warn {
                "compaction refused ${spillRefused.size} sorted group(s) of ${ctx.namespace}.${ctx.table} " +
                    "whose spill would exceed HOGLAKE_COMPACTION_SPILL_BYTES (${cfg.spillBytes} B per " +
                    "group): the largest holds ${worstSpill.files.size} file(s), ${worstSpill.totalBytes} B " +
                    "registered. Refused in metadata, before any IO, because an overrun of the spill " +
                    "volume's emptyDir limit evicts the pod rather than failing the write. Levers: raise " +
                    "HOGLAKE_COMPACTION_SPILL_BYTES together with the volume (the chart's tmpSizeLimit; " +
                    "size it as parallel groups x spill bytes), or lower HOGLAKE_COMPACTION_TARGET_BYTES"
            }
        }
        if (worstMerge != null) {
            log.warn {
                "compaction refused ${mergeRefused.size} sorted group(s) of ${ctx.namespace}.${ctx.table} " +
                    "whose merge cannot fit the sorted heap budget even with every trusted input " +
                    "spilled: the largest holds ${worstMerge.survivingRecords} surviving row(s), " +
                    "${cfg.spillChunkRows(ctx.columns)} row(s) per spill file, against " +
                    "${cfg.sortedHeapBytesPerGroup} B per group (HOGLAKE_COMPACTION_SORTED_HEAP_BYTES " +
                    "${cfg.sortedHeapBytes} B / HOGLAKE_COMPACTION_PARALLEL_GROUPS " +
                    "${cfg.parallelGroups}). Each spilled run holds a row group while it is merged, " +
                    "so the budget is too small for the group size: raise " +
                    "HOGLAKE_COMPACTION_SORTED_HEAP_BYTES (with the pod's memory), lower " +
                    "HOGLAKE_COMPACTION_PARALLEL_GROUPS, or lower HOGLAKE_COMPACTION_TARGET_BYTES"
            }
        }
    }

    /** Phase (c)'s output: the groups to attempt, and how many a sibling holds. */
    private data class FreeGroups(val groups: List<CompactionGroup>, val claimed: Long)

    /**
     * PHASE (c): drop the groups another maintainer is already
     * rewriting.
     *
     * THE PLANNER SKIPS CLAIMED GROUPS — the other half of the
     * two-replica fix, and the half that costs nothing when it is
     * wrong.
     *
     * By OVERLAP, not by claim-key equality. Two replicas planning
     * the same catalog metadata form identical groups and would
     * match exactly; a replica planning a moment later, after one
     * more ingest file landed, packs the same files into groups with
     * different keys, and an exact match would wave every one of
     * them through. Overlap covers both, and a group sharing even one
     * input with an in-flight rewrite is a group whose commit would
     * lose the re-verification anyway.
     *
     * READ IN ITS OWN SHORT TRANSACTION, after the packing, and both
     * halves of that are deliberate. It is outside the candidate read's
     * snapshot because packing must run outside a transaction; and that
     * is harmless because this read can only see MORE claims than the
     * candidate snapshot would have — a claim taken during the packing
     * is honoured here, where the old shape planned the group and lost
     * its rewrite at commit. A claim is an optimization, never
     * authorization (`CompactionClaimRepo`), and the plan-to-commit
     * re-verification is the correctness backstop either way.
     *
     * A FAILING claim read plans the table as if nothing were claimed,
     * rather than throwing: this runs inside `planSnapshot`, which is
     * outside the per-group catch, so an error here would kill the
     * sweep for every catalog on every interval — and it would do so on
     * behalf of an optimization. `planAndClaim` takes the same position
     * on the write.
     *
     * NO SAVEPOINT, unlike the version of this read that lived inside
     * the planning transaction. A failed statement poisons a Postgres
     * transaction, so catching the exception there without rewinding
     * would have turned a broken optimization into a broken plan. This
     * transaction has nothing else in it, so rolling it back IS the
     * rewind.
     */
    private fun withoutClaimedGroups(
        h: Handle,
        ctx: TableContext,
        cfg: CompactionConfig,
        groups: List<CompactionGroup>,
    ): FreeGroups {
        if (!cfg.claimsEnabled || groups.isEmpty()) return FreeGroups(groups, 0)
        val claimed =
            try {
                CompactionClaimRepo.liveClaimedFileIds(h, ctx.catalogId, ctx.tableId)
            } catch (e: Exception) {
                log.warn(e) {
                    "compaction claim read failed for ${ctx.namespace}.${ctx.table}; " +
                        "planning as if unclaimed (a claim is an optimization, and the " +
                        "plan-to-commit re-verification is the correctness backstop)"
                }
                emptySet()
            }
        if (claimed.isEmpty()) return FreeGroups(groups, 0)
        val (free, held) = groups.partition { g -> g.files.none { it.dataFileId in claimed } }
        return FreeGroups(free, held.size.toLong())
    }

    // ---- one run ---------------------------------------------------------

    /** Manual-trigger convenience: defaults with an optional groups-per-run override. */
    fun runOnce(
        catalog: String,
        batchOverride: Int? = null,
        trigger: MaintenanceTrigger = MaintenanceTrigger.MANUAL,
    ): CompactionResult =
        runOnce(catalog, defaults.copy(maxGroupsPerRun = batchOverride ?: defaults.maxGroupsPerRun), trigger)

    /**
     * One compaction sweep over [catalog]: plan tables in name order and
     * rewrite at most cfg.maxGroupsPerRun groups (every skip flavor
     * consumes budget too — a skipped group already spent the IO).
     * Metrics and the audit event are emitted here, after all group
     * transactions resolved; every run is recorded in the maintenance
     * run ledger.
     */
    fun runOnce(
        catalog: String,
        cfg: CompactionConfig,
        trigger: MaintenanceTrigger = MaintenanceTrigger.MANUAL,
    ): CompactionResult =
        runStore.recorded(catalog, MaintenanceTask.COMPACTION, trigger) {
            runSweep(catalog, cfg)
        }

    private fun runSweep(
        catalog: String,
        cfg: CompactionConfig,
    ): CompactionResult =
        Audit.audited(
            "compaction",
            catalog,
            null,
            detail = { r ->
                "groups_compacted=${r.groupsCompacted} files_in=${r.filesIn} files_out=${r.filesOut} " +
                    "bytes_in=${r.bytesIn} bytes_out=${r.bytesOut} skipped_conflicts=${r.skippedConflicts} " +
                    "dv_superseded=${r.dvSuperseded} unconvertible_schema=${r.unconvertibleSchema} " +
                    "invalid_data=${r.invalidData} spill_budget_exceeded=${r.spillBudgetExceeded} " +
                    "merge_budget_exceeded=${r.mergeBudgetExceeded} " +
                    "failed_groups=${r.failedGroups} claimed_elsewhere=${r.claimedElsewhere} " +
                    "runs_trusted=${r.runsTrusted} runs_spilled=${r.runsSpilled} " +
                    "runs_demoted=${r.runsDemoted} spill_bytes=${r.spillBytes} " +
                    "spill_cleanup_failures=${r.spillCleanupFailures} " +
                    "files_verified=${r.filesVerified} files_unsorted=${r.filesUnsorted} " +
                    "files_unchecked=${r.filesUnchecked} " +
                    "row_groups_appended=${r.rowGroupsAppended} bytes_appended=${r.bytesAppended} " +
                    "candidates_fetched=${r.candidatesFetched} " +
                    "buckets_considered=${r.bucketsConsidered}/${r.bucketsAvailable} " +
                    "candidates_truncated=${r.candidatesTruncated} plan_ms=${r.planMs}"
            },
        ) {
            if (cfg.maxGroupsPerRun <= 0) {
                throw HoglakeException.Validation(
                    "batch (groups per run) must be positive (got ${cfg.maxGroupsPerRun})",
                )
            }
            val result = doRunOnce(catalog, cfg)
            Metrics.compactionGroups(catalog, result.groupsCompacted)
            Metrics.compactionFilesRewritten(catalog, result.filesIn)
            Metrics.compactionSkipped(catalog, "unconvertible_schema", result.unconvertibleSchema)
            Metrics.compactionSkipped(catalog, "invalid_data", result.invalidData)
            // `heap_budget` is HISTORICAL: nothing produces it since the
            // external merge sort (hoglake#134). Its successors:
            Metrics.compactionSkipped(catalog, "spill_budget", result.spillBudgetExceeded)
            Metrics.compactionSkipped(catalog, "merge_budget", result.mergeBudgetExceeded)
            Metrics.compactionSpillBytes(catalog, result.spillBytes)
            Metrics.compactionAppendedBytes(catalog, result.bytesAppended)
            Metrics.compactionMergeRuns(catalog, "trusted", result.runsTrusted)
            Metrics.compactionMergeRuns(catalog, "spilled", result.runsSpilled)
            Metrics.compactionMergeRuns(catalog, "demoted", result.runsDemoted)
            Metrics.compactionSpillCleanupFailed(result.spillCleanupFailures)
            Metrics.compactionSortCheck(catalog, "sorted", result.filesVerified)
            Metrics.compactionSortCheck(catalog, "unsorted", result.filesUnsorted)
            // The red-flag outcome, and it had no series either.
            Metrics.compactionSkipped(catalog, "failed", result.failedGroups)
            // Its own reason label, because it is the one "skip" that is
            // GOOD news: it is duplicated work that did not happen.
            Metrics.compactionSkipped(catalog, "claimed_elsewhere", result.claimedElsewhere)
            result
        }

    /**
     * One group's contribution to the run's counters.
     *
     * Every outcome a group can have is ONE of these, produced by the
     * worker that ran it and summed once, after every worker has joined.
     * That is what makes the counters exact under
     * [CompactionConfig.parallelGroups] > 1 without a single shared
     * mutable variable, a lock or an atomic: there is nothing to
     * interleave. The previous shape incremented eleven `var`s from the
     * loop body, which is correct for one thread and silently lossy for
     * two (`x++` on a JVM `long` is neither atomic nor visible).
     */
    private data class GroupTally(
        val groupsCompacted: Long = 0,
        val filesIn: Long = 0,
        val bytesIn: Long = 0,
        val bytesOut: Long = 0,
        val skippedConflicts: Long = 0,
        val dvSuperseded: Long = 0,
        val unconvertibleSchema: Long = 0,
        val invalidData: Long = 0,
        val spillBudgetExceeded: Long = 0,
        val mergeBudgetExceeded: Long = 0,
        val failedGroups: Long = 0,
        val claimedElsewhere: Long = 0,
        /**
         * What sorted rewrites DID, summed over every rewrite that
         * returned — committed or not, since the work was spent either
         * way — and every rewrite a spill or merge budget stopped mid-run
         * (carried on the exception; trusted runs are not counted there,
         * since none was merged): inputs read in place as runs, spill files written, trusted
         * inputs the merge budget demoted, bytes spilled, and spill
         * directories that could not be removed afterwards. Not outcomes,
         * so none of them is in [attempts].
         */
        val runsTrusted: Long = 0,
        val runsSpilled: Long = 0,
        val runsDemoted: Long = 0,
        val spillBytes: Long = 0,
        val spillCleanupFailures: Long = 0,
        /**
         * The sortedness pre-pass: inputs it verified as already sorted
         * and inputs it found unsorted. Work, like the run counters, so
         * not in [attempts]. Its bytes go straight to a metric where the
         * rewrite returns ([executeGroup]); they are not a ledger field.
         */
        val filesVerified: Long = 0,
        val filesUnsorted: Long = 0,
        /** Inputs under the pre-pass's size floor, sent to the chunk phase unchecked. */
        val filesUnchecked: Long = 0,
        /**
         * Input row groups the rewrite appended byte for byte, and their
         * compressed bytes. Work, like the run counters, so not in
         * [attempts].
         */
        val rowGroupsAppended: Long = 0,
        val bytesAppended: Long = 0,
        /**
         * The PLAN measures, which are not group outcomes at all: they
         * describe what the planner READ, not what a rewrite did.
         *
         * They ride this accumulator because it is the one thing the
         * sweep already sums per table and per wave, and because they
         * must reach the run ledger by the same route the counters do.
         * None of them is in [attempts] — a candidate row is not a
         * group's IO.
         */
        val candidatesFetched: Long = 0,
        val bucketsConsidered: Long = 0,
        val bucketsAvailable: Long = 0,
        val candidatesTruncated: Long = 0,
        val planMs: Long = 0,
    ) {
        /**
         * What this tally has spent of `maxGroupsPerRun`.
         *
         * The budget is a bound on OBJECT-STORE WORK, so it counts every
         * outcome that spent a group's IO and, with one exception, no
         * outcome that did not. Not charged:
         *
         *  - [spillBudgetExceeded] and [mergeBudgetExceeded], the sorted
         *    rewrite's budgets, which the planner decides in metadata
         *    before a byte is fetched;
         *  - [claimedElsewhere], which is a group another maintainer is
         *    already rewriting — refused by the planner's claim read or
         *    by the claim insert, both of which run before the fetch.
         *
         * Charging either would let one un-compactable table, or one
         * busy sibling replica, consume the whole sweep's budget and
         * starve every other table forever. The counters' own KDoc and
         * the OpenAPI both state this; `attempts` is where it is
         * actually true.
         *
         * The exception: the same two budget refusals raised by the
         * REWRITE mid-run (an exact footer or the bytes actually spilled
         * broke a budget the registered metadata fit) did spend IO, and
         * they are still not charged — they arrive through the same
         * counters, and splitting them would put one condition under two
         * names. They are bounded anyway: the planner admits only groups
         * whose registered metadata fits, so a hard stop needs metadata
         * that under-predicts the bytes, and a sweep still executes at
         * most the groups its plans claimed.
         */
        val attempts: Int
            get() =
                (
                    groupsCompacted + skippedConflicts + dvSuperseded +
                        unconvertibleSchema + invalidData + failedGroups
                ).toInt()

        operator fun plus(other: GroupTally) =
            GroupTally(
                groupsCompacted + other.groupsCompacted,
                filesIn + other.filesIn,
                bytesIn + other.bytesIn,
                bytesOut + other.bytesOut,
                skippedConflicts + other.skippedConflicts,
                dvSuperseded + other.dvSuperseded,
                unconvertibleSchema + other.unconvertibleSchema,
                invalidData + other.invalidData,
                spillBudgetExceeded + other.spillBudgetExceeded,
                mergeBudgetExceeded + other.mergeBudgetExceeded,
                failedGroups + other.failedGroups,
                claimedElsewhere + other.claimedElsewhere,
                runsTrusted + other.runsTrusted,
                runsSpilled + other.runsSpilled,
                runsDemoted + other.runsDemoted,
                spillBytes + other.spillBytes,
                spillCleanupFailures + other.spillCleanupFailures,
                filesVerified + other.filesVerified,
                filesUnsorted + other.filesUnsorted,
                filesUnchecked + other.filesUnchecked,
                rowGroupsAppended + other.rowGroupsAppended,
                bytesAppended + other.bytesAppended,
                candidatesFetched + other.candidatesFetched,
                bucketsConsidered + other.bucketsConsidered,
                bucketsAvailable + other.bucketsAvailable,
                candidatesTruncated + other.candidatesTruncated,
                planMs + other.planMs,
            )

        companion object {
            /** A group that was never attempted: no counter moves. */
            val NOT_ATTEMPTED = GroupTally()
        }
    }

    /** One planned group with the table context its execution needs. */
    private data class WorkItem(
        val ctx: TableContext,
        val group: CompactionGroup,
        val claimant: UUID? = null,
        val started: java.util.concurrent.atomic.AtomicBoolean = java.util.concurrent.atomic.AtomicBoolean(false),
    )

    private data class PlannedWork(val plan: CompactionPlan, val items: List<WorkItem>)

    /**
     * Reserve this table's queue before another planner can read candidates.
     * One connection holds the session lock across short transactions and packing.
     * Claims are bounded by the remaining run budget. File I/O starts after unlock.
     */
    private fun planAndClaim(
        catalog: String,
        namespace: String,
        table: String,
        cfg: CompactionConfig,
        room: Int,
        claimant: UUID,
    ): PlannedWork =
        jdbi.withHandleUnchecked { h ->
            val catalogId = catalogIdOf(h, catalog)
            try {
                if (cfg.claimsEnabled) {
                    h.inTransaction<Unit, Exception> { Locks.acquireCatalogCompactionPlanLock(h, catalogId) }
                }
                val (ctx, plan) = planSnapshot(h, catalog, namespace, table, cfg)
                if (!cfg.claimsEnabled) {
                    return@withHandleUnchecked PlannedWork(plan, plan.groups.take(room).map { WorkItem(ctx, it) })
                }
                try {
                    h.inTransaction<PlannedWork, Exception> {
                        h.execute("SET LOCAL statement_timeout = '${PLAN_STATEMENT_TIMEOUT_MS}ms'")
                        val items = mutableListOf<WorkItem>()
                        var claimed = plan.claimedGroups
                        for (group in plan.groups) {
                            if (items.size >= room) break
                            if (Thread.currentThread().isInterrupted) throw InterruptedException()
                            if (CompactionClaimRepo.acquire(
                                    h,
                                    ctx.catalogId,
                                    ctx.tableId,
                                    CompactionClaimRepo.groupKey(group),
                                    group.files.map { it.dataFileId },
                                    claimant,
                                    cfg.claimTtlSeconds,
                                )
                            ) {
                                items += WorkItem(ctx, group, claimant)
                            } else {
                                claimed++
                            }
                        }
                        PlannedWork(plan.copy(claimedGroups = claimed), items)
                    }
                } catch (e: Exception) {
                    if (e is InterruptedException || Thread.currentThread().isInterrupted) throw e
                    log.warn(e) { "compaction plan claims failed for $namespace.$table; continuing without claims" }
                    PlannedWork(plan, plan.groups.take(room).map { WorkItem(ctx, it) })
                }
            } finally {
                if (cfg.claimsEnabled) Locks.releaseCatalogCompactionPlanLock(h, catalogId)
            }
        }

    /** Release queued work on cancellation or a heap refusal. Started groups settle their own claims. */
    private fun releaseQueuedClaims(items: List<WorkItem>) {
        val interrupted = Thread.interrupted()
        try {
            for (item in items) {
                if (item.claimant == null || item.started.get()) continue
                runCatching {
                    jdbi.withHandleUnchecked { h ->
                        CompactionClaimRepo.release(
                            h,
                            item.ctx.catalogId,
                            item.ctx.tableId,
                            CompactionClaimRepo.groupKey(item.group),
                            item.claimant,
                        )
                    }
                }.onFailure { e -> log.warn(e) { "could not release queued compaction claim; it will expire" } }
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt()
        }
    }

    private fun doRunOnce(
        catalog: String,
        cfg: CompactionConfig,
    ): CompactionResult {
        // Expired claims first, once per catalog per sweep. The reclaim
        // arm of CompactionClaimRepo.acquire covers a row a re-planned
        // group lands on again; this covers the rest — a group whose
        // files the OTHER replica compacted is never re-planned, so
        // nothing would ever look at its abandoned claim. The verify
        // subsystem's compaction_claims check was what redded if this
        // stopped running; #261 removed it, so nothing does today.
        //
        // Run UNCONDITIONALLY, not under `claimsEnabled`. Turning claims
        // off does not delete the rows a previous configuration wrote, so
        // a flag flip that left them stranded would have turned the old
        // `compaction_claims` check red an hour later for a deployment
        // that did nothing wrong. The
        // purge is a DELETE of expired rows: harmless when there are
        // none, and the only thing that cleans up after the flip.
        runCatching {
            jdbi.withHandleUnchecked { h -> CompactionClaimRepo.purgeExpired(h, catalogIdOf(h, catalog)) }
        }
            .onFailure { e -> log.warn(e) { "compaction claim purge failed for '$catalog'; continuing" } }
        val tables = jdbi.withHandleUnchecked { h -> liveTables(h, catalog) }

        // PLAN AND EXECUTE PER TABLE, in name order — the loop order
        // this sweep has always had, and the reason is FRESHNESS.
        //
        // Planning every table up front and executing the whole batch
        // afterwards would make the last table's plan as old as every
        // rewrite before it: at the production settings
        // (maxGroupsPerRun=64, ~8.5 s a group) that is minutes of ingest
        // and compaction by a sibling replica between the read and the
        // attempt, and every one of those groups arrives at its commit
        // with a stale input set. A plan is cheap — metadata only — and
        // its value decays fast, so it is taken as late as it can be.
        //
        // At parallelGroups = 1 this is the pre-existing loop exactly:
        // one table planned, its groups executed one at a time on the
        // calling thread, the next table planned only if budget remains.
        val pool = groupPool(cfg)
        val claimant = UUID.randomUUID()
        var pending = emptyList<WorkItem>()
        var tally = GroupTally()
        val heapExhausted = java.util.concurrent.atomic.AtomicBoolean(false)
        try {
            for ((namespace, table) in tables) {
                if (tally.attempts >= cfg.maxGroupsPerRun || heapExhausted.get()) break
                // CANCELLATION IS CHECKED HERE, on the sweep's own
                // thread, and it has to be.
                //
                // At parallelGroups = 1 there is no pool: `shutdown` is a
                // no-op, nothing joins a future, and the only signal that
                // the loop was asked to stop is this thread's interrupt
                // flag. Without this check a cancelled default-configured
                // sweep kept going — [executeGroup] catches the
                // interrupted rewrite as an ordinary failure, counts it,
                // and the loop pulls the next group; six groups were
                // asked to stop and five of them committed afterwards.
                stopIfInterrupted(tally)
                val (plan, items) =
                    planAndClaim(
                        catalog,
                        namespace,
                        table,
                        cfg,
                        cfg.maxGroupsPerRun - tally.attempts,
                        claimant,
                    )
                pending = items
                // Sorted groups the spill or merge budget refused in
                // METADATA, and groups another maintainer holds. Counted but deliberately NOT
                // charged to maxGroupsPerRun: every other skip flavor spends
                // the group's IO before it resolves and these spend none, so
                // letting either consume the run's slots would let one
                // un-compactable table — or one busy sibling replica — starve
                // every other table of the sweep forever.
                tally +=
                    GroupTally(
                        spillBudgetExceeded = plan.spillRefusedGroups,
                        mergeBudgetExceeded = plan.mergeRefusedGroups,
                        claimedElsewhere = plan.claimedGroups,
                        // The plan measures, summed over every table the
                        // sweep plans — so the ledger row says how much
                        // metadata the whole sweep read and how long
                        // reading it took, which is the quantity that grew
                        // until every sweep died.
                        candidatesFetched = plan.candidatesFetched,
                        bucketsConsidered = plan.bucketsConsidered,
                        bucketsAvailable = plan.bucketsAvailable,
                        candidatesTruncated = plan.candidatesTruncated,
                        planMs = plan.planMs,
                    )
                val queue = ArrayDeque(items)
                while (queue.isNotEmpty() && !heapExhausted.get()) {
                    val room = cfg.maxGroupsPerRun - tally.attempts
                    if (room <= 0) break
                    // A wave of at most `parallelGroups` groups, never
                    // more than the budget's remaining room. The budget
                    // is spent by ATTEMPTS — see GroupTally.attempts,
                    // which counts every outcome that spends IO and no
                    // outcome that does not — so a group the claim
                    // arbitration turns away at execute time refunds its
                    // slot here and the sweep pulls the next candidate
                    // instead of ending a group short.
                    // `room` is at least 1 and the queue is non-empty,
                    // so this always takes at least one group.
                    val wave =
                        (0 until minOf(room.toLong(), cfg.parallelGroups.toLong()).toInt())
                            .mapNotNull { queue.removeFirstOrNull() }
                    tally += executeWave(catalog, wave, cfg, pool, heapExhausted)
                    // Between waves as well as between tables: one wave
                    // is up to `parallelGroups` rewrites, which is
                    // minutes of work to do after being told to stop.
                    stopIfInterrupted(tally)
                }
            }
        } catch (e: InterruptedException) {
            // The POOL path's cancellation arrives here rather than
            // through [stopIfInterrupted]: a worker's future throws it
            // out of `executeWave`. Same treatment either way — restore
            // the flag (`Future.get` clears it on the way out, and the
            // flag is how `runInterruptible` learns this was a
            // cancellation rather than a failure) and carry the partial
            // tally, because the groups it counts are committed.
            if (e is SweepInterrupted) throw e
            Thread.currentThread().interrupt()
            throw SweepInterrupted(tally.toResult()).also { it.initCause(e) }
        } finally {
            shutdown(pool, interrupted = Thread.currentThread().isInterrupted)
            releaseQueuedClaims(pending)
        }
        return tally.toResult()
    }

    /**
     * Stop the sweep if this thread has been asked to, carrying the
     * counters it has earned so far.
     *
     * THE PARTIAL TALLY IS THE POINT. A cancelled sweep has usually
     * committed groups already — those commits are durable, their
     * inputs are retired, their outputs are live — and throwing a bare
     * `InterruptedException` recorded a failed ledger row with NO
     * counters for them. An operator reading that row sees a compactor
     * that did nothing, on a sweep that did most of its work, which is
     * the same class of lie as the uncounted swallow that
     * `failed_groups` exists to prevent.
     *
     * Still an `InterruptedException`, so `BackgroundLoops`'
     * `runInterruptible` and every other caller treat it as the
     * cancellation it is; the counters ride along for the ledger.
     */
    private fun stopIfInterrupted(tally: GroupTally) {
        if (!Thread.currentThread().isInterrupted) return
        throw SweepInterrupted(tally.toResult())
    }

    private fun GroupTally.toResult(): CompactionResult =
        CompactionResult(
            groupsCompacted = groupsCompacted,
            filesIn = filesIn,
            filesOut = groupsCompacted,
            bytesIn = bytesIn,
            bytesOut = bytesOut,
            skippedConflicts = skippedConflicts,
            dvSuperseded = dvSuperseded,
            unconvertibleSchema = unconvertibleSchema,
            invalidData = invalidData,
            failedGroups = failedGroups,
            claimedElsewhere = claimedElsewhere,
            runsTrusted = runsTrusted,
            runsSpilled = runsSpilled,
            runsDemoted = runsDemoted,
            spillBytes = spillBytes,
            spillBudgetExceeded = spillBudgetExceeded,
            mergeBudgetExceeded = mergeBudgetExceeded,
            spillCleanupFailures = spillCleanupFailures,
            filesVerified = filesVerified,
            filesUnsorted = filesUnsorted,
            filesUnchecked = filesUnchecked,
            rowGroupsAppended = rowGroupsAppended,
            bytesAppended = bytesAppended,
            candidatesFetched = candidatesFetched,
            bucketsConsidered = bucketsConsidered,
            bucketsAvailable = bucketsAvailable,
            candidatesTruncated = candidatesTruncated,
            planMs = planMs,
        )

    /**
     * The sweep's worker pool, or null at
     * [CompactionConfig.parallelGroups] = 1.
     *
     * Null is not an optimization, it is the CONTRACT: the default
     * configuration must execute groups on the calling thread, with no
     * executor in the picture at all, so a deployment that sets nothing
     * runs the sweep it has always run — same thread, same ordering,
     * same interruption behaviour.
     */
    private fun groupPool(cfg: CompactionConfig): java.util.concurrent.ExecutorService? =
        if (cfg.parallelGroups <= 1) {
            null
        } else {
            java.util.concurrent.Executors.newFixedThreadPool(cfg.parallelGroups) { r ->
                Thread(r, "compaction-group").apply { isDaemon = true }
            }
        }

    /**
     * Stop the pool and do not return until its workers have.
     *
     * # Why `shutdown()` alone was wrong
     *
     * A compaction worker rewrites and COMMITS. Leaving one running past
     * the sweep is not a tidy-up detail: `BackgroundLoops` runs the loop
     * body under `runInterruptible` and gives shutdown a 5 s hard cap,
     * so a worker that outlives the sweep thread is a thread that
     * rewrites and commits after the supervisor believes compaction has
     * stopped — and a JVM kill in that window lands mid-upload, leaving
     * multipart parts that are billed and that no ledger row points at.
     * `shutdown()` on its own makes that the NORMAL path on interrupt:
     * it lets every queued group run to completion.
     *
     * So the exceptional path is [java.util.concurrent.ExecutorService.shutdownNow]
     * — drop what is queued, interrupt what is running — followed by a
     * bounded wait, so this returns only once the workers are gone or
     * the wait expired (logged, because a worker that ignores an
     * interrupt is an operator's problem). The interrupt flag is
     * restored on the way out: swallowing it would leave the loop's
     * supervisor unable to see that it was asked to stop.
     *
     * The ORDINARY path still drains: a sweep that finished normally has
     * no in-flight work, so `shutdown()` returns at once.
     */
    private fun shutdown(
        pool: java.util.concurrent.ExecutorService?,
        interrupted: Boolean,
    ) {
        if (pool == null) return
        if (interrupted) pool.shutdownNow() else pool.shutdown()
        // CLEAR the flag for the wait, and restore it after.
        //
        // `awaitTermination` on a thread whose interrupt flag is already
        // set throws immediately without waiting at all — which is
        // exactly the state this method is called in on the interrupt
        // path, and it made the bounded wait a no-op: the sweep returned
        // while its workers were still mid-rewrite, aborting uploads
        // behind it. The wait is the whole point of the method, so the
        // flag is taken down for its duration and put back before
        // returning, which is what any caller-facing "I was cancelled"
        // contract requires.
        val hadFlag = Thread.interrupted() || interrupted
        var interruptedDuringWait = false
        val stopped =
            try {
                pool.awaitTermination(SHUTDOWN_WAIT_SECONDS, TimeUnit.SECONDS)
            } catch (e: InterruptedException) {
                // A SECOND interrupt, arriving DURING the wait. Two
                // things follow, and the first version of this got both
                // wrong. The flag has to be restored — `hadFlag` was
                // computed before the wait, so it is false here and the
                // cancellation would be swallowed on exactly the path
                // where somebody is insisting on it. And this is not the
                // "workers ignored us" condition: nobody waited three
                // seconds, so the error below would be describing a wait
                // that never happened.
                interruptedDuringWait = true
                log.debug(e) { "interrupted while waiting for compaction workers to stop" }
                false
            }
        if (!stopped) pool.shutdownNow()
        if (!stopped && !interruptedDuringWait) {
            log.error {
                "compaction workers did not stop within ${SHUTDOWN_WAIT_SECONDS}s of the sweep " +
                    "ending; a rewrite or commit may still be in flight after the sweep returned"
            }
        }
        if (hadFlag || interruptedDuringWait) Thread.currentThread().interrupt()
    }

    /**
     * Run one wave of groups — at most [CompactionConfig.parallelGroups]
     * of them — and return their summed tally.
     *
     * # Concurrency
     *
     * [pool] null (the default configuration) means the loop below runs
     * on the calling thread with no executor: today's sweep, thread for
     * thread.
     *
     * Otherwise every item is submitted and every future is joined
     * before this returns — no orphan continues past the wave, and the
     * next wave's plan is never read while a previous group is still
     * committing under it. The workers share nothing: the groups
     * partition the candidate files (one pass of bin packing over
     * disjoint buckets), the per-group staging ticket is its own row,
     * and the only contended resource is the per-catalog commit lock,
     * which each group holds for its metadata transaction alone — never
     * across the rewrite or the upload (see [commitGroup], which opens
     * the transaction, takes the lock, and is reached only after
     * [ParquetRewriter.rewrite] has returned and the output is
     * uploaded).
     *
     * # Why `Callable` never throws
     *
     * [executeGroup] converts every outcome, `Throwable` included, into
     * a [GroupTally]. So `Future.get()` cannot throw an
     * `ExecutionException` and the reduction cannot lose a group's
     * accounting to an exception raised on a thread nobody is watching —
     * the failure mode that made the pre-#118 sweep report a clean run
     * for a dead compactor. `get()` can still throw
     * `InterruptedException`, which is the sweep being cancelled and is
     * handled by [shutdown]'s exceptional path.
     *
     * # The heap stop
     *
     * An `OutOfMemoryError` ends the sweep, as it always has: continuing
     * would allocate the next group's inputs into a heap that just
     * proved it has none to spare. Concurrently, "ends" means items not
     * yet STARTED do nothing and report [GroupTally.NOT_ATTEMPTED];
     * groups already in flight run to completion, because cancelling
     * them would strand staged objects that their own commit would
     * otherwise settle, and because the allocation that OOM'd is
     * unreachable the moment its frame unwinds.
     */
    private fun executeWave(
        catalog: String,
        items: List<WorkItem>,
        cfg: CompactionConfig,
        pool: java.util.concurrent.ExecutorService?,
        heapExhausted: java.util.concurrent.atomic.AtomicBoolean,
    ): GroupTally {
        if (items.isEmpty()) return GroupTally()
        if (pool == null) {
            var tally = GroupTally()
            for (item in items) tally += executeGroup(catalog, item, cfg, heapExhausted)
            return tally
        }
        val futures =
            items.map { item ->
                pool.submit(
                    java.util.concurrent.Callable { executeGroup(catalog, item, cfg, heapExhausted) },
                )
            }
        return try {
            futures.fold(GroupTally()) { acc, f -> acc + f.get() }
        } catch (e: InterruptedException) {
            // The sweep was cancelled. Stop the workers rather than
            // letting the queue drain behind us, and let it up: the
            // caller ([doRunOnce]) restores the flag and attaches the
            // partial tally, in ONE place. Setting the flag here as well
            // was redundant — removing it changed nothing any test could
            // see, which is how it was found — and two places that must
            // agree about a thread's interrupt state is one too many.
            futures.forEach { it.cancel(true) }
            throw e
        }
    }

    /**
     * Claim, rewrite, commit and release ONE group, converting every
     * outcome into a [GroupTally].
     *
     * NOTHING escapes, `Throwable` included. The sweep's per-group
     * failure isolation is what keeps one unreadable input from wedging
     * a catalog; under concurrency it additionally keeps a worker
     * thread's exception from vanishing. `Exception` alone was not
     * enough: the nested copier recurses, so a `StackOverflowError` is
     * live in this code path, and an `Error` escaping a pool worker
     * loses that group's accounting, leaves its claim held for a full
     * lease, and — before [shutdown] — let the rest of the queue keep
     * running behind a sweep that had already returned.
     */
    private fun executeGroup(
        catalog: String,
        item: WorkItem,
        cfg: CompactionConfig,
        heapExhausted: java.util.concurrent.atomic.AtomicBoolean,
    ): GroupTally {
        val (ctx, group) = item
        if (heapExhausted.get()) return GroupTally.NOT_ATTEMPTED
        item.started.set(true)
        // Refresh the reservation before file I/O. A lost lease spends no I/O.
        val claimKey = item.claimant?.let { CompactionClaimRepo.groupKey(group) }
        if (claimKey != null && !refreshClaim(ctx, claimKey, item.claimant, cfg)) {
            log.debug {
                "compaction group of ${group.files.size} files for " +
                    "$catalog/${ctx.namespace}.${ctx.table} is claimed by another maintainer; " +
                    "skipping without spending its IO"
            }
            // No IO spent, so no budget spent: GroupTally.attempts does
            // not count this and doRunOnce pulls the next candidate.
            return GroupTally(claimedElsewhere = 1)
        }

        // ONE bad-group path, reached from TWO catch arms, and it has to
        // be a function rather than a shared arm because of what a
        // `throw` inside a catch block does: it leaves the try
        // statement, so the SIBLING arms — including the `Throwable` one
        // below, which is the whole failure-isolation mechanism — never
        // see it. The `UnableToExecuteStatementException` arm used to
        // rethrow a non-lock-timeout straight out of `executeGroup`,
        // where nothing caught it: `f.get()` in [executeWave] surfaced
        // it as an `ExecutionException` and the ENTIRE sweep aborted
        // with an empty ledger result, losing the accounting of every
        // group that had not started yet (on gigahog-prod-us, 2026-09-30:
        // one group's `statement_timeout` took ~58 others with it and
        // the run recorded `{}`). A statement that dies for a reason
        // this code has no rule for is ONE failed group, like any other.
        fun failGroup(e: Throwable): GroupTally {
            // AN INTERRUPT IN DISGUISE STILL HAS TO SET THE FLAG.
            //
            // `e is InterruptedException` was not enough. The object
            // store's client, the HTTP stack under it and NIO all
            // translate an interrupt into something else — a
            // `ClosedByInterruptException`, an SDK exception wrapping
            // one, an `IOException` with it somewhere down the cause
            // chain — and several of them CLEAR the flag on the way
            // past. On the sequential path this frame runs on the
            // sweep's own thread, so a cancellation that arrives as a
            // wrapped exception and leaves the flag down is a sweep
            // that was asked to stop, counted the group as an ordinary
            // failure, and carried on to the next one.
            if (wasInterrupt(e)) Thread.currentThread().interrupt()
            log.error(e) {
                "compaction group of ${group.files.size} files failed for " +
                    "$catalog/${ctx.namespace}.${ctx.table}; continuing"
            }
            return GroupTally(failedGroups = 1)
        }
        var committed = false
        // What the rewrite did, whatever the commit then decides: the
        // runs and the spill were spent either way.
        var work = GroupTally()
        try {
            val outcome =
                compactGroup(ctx, group) { r ->
                    work =
                        GroupTally(
                            runsTrusted = r.runsTrusted.toLong(),
                            runsSpilled = r.runsSpilled.toLong(),
                            runsDemoted = r.runsDemoted.toLong(),
                            spillBytes = r.spillBytes,
                            spillCleanupFailures = if (r.spillCleanupFailed) 1 else 0,
                            filesVerified = r.filesVerified.toLong(),
                            filesUnsorted = r.filesUnsorted.toLong(),
                            filesUnchecked = r.filesUnchecked.toLong(),
                            rowGroupsAppended = r.rowGroupsAppended.toLong(),
                            bytesAppended = r.bytesAppended,
                        )
                    Metrics.compactionSortCheckBytes(catalog, r.sortCheckBytes)
                }
            return work +
                when (outcome) {
                    is GroupOutcome.Committed -> {
                        committed = true
                        GroupTally(
                            groupsCompacted = 1,
                            filesIn = group.files.size.toLong(),
                            bytesIn = group.totalBytes,
                            bytesOut = outcome.bytesOut,
                        )
                    }
                    GroupOutcome.SkippedConflict -> GroupTally(skippedConflicts = 1)
                    GroupOutcome.SkippedDvSuperseded -> GroupTally(dvSuperseded = 1)
                }
        } catch (e: HoglakeException.CommitQueueTimeout) {
            // The commit could not get the catalog lock inside the
            // admission bound. Race-class, not failure-class: nothing is
            // wrong with the group, the catalog was busy, and the staged
            // output is left with an undrained ticket for the cleanup
            // drain exactly as a lost plan-to-commit race leaves it.
            // Counted with the other races so a busy catalog reads as
            // contention rather than as a broken compactor.
            log.warn {
                "compaction group of ${group.files.size} files for " +
                    "$catalog/${ctx.namespace}.${ctx.table} timed out waiting for the catalog " +
                    "commit lock (${e.message}); skipping — the staged output stays queued for " +
                    "the cleanup drain"
            }
            return GroupTally(skippedConflicts = 1)
        } catch (e: UnableToExecuteStatementException) {
            // A lock timeout ANYWHERE in the commit transaction, not
            // just on the advisory lock itself.
            //
            // `acquireCatalogCommitLock(..., commitLockTimeoutMs)` sets
            // a TRANSACTION-LOCAL `lock_timeout`, and it stays in force
            // for the rest of the tail — deliberately, per Locks.kt: the
            // admission contract is meant to bound the whole commit, not
            // only its first statement. The consequence is that a row
            // lock later in the tail (the inputs' `end_snapshot` update
            // queueing behind a concurrent writer, say) can now expire
            // too, and it arrives as a raw driver exception rather than
            // the typed CommitQueueTimeout. Counting that as
            // `failed_groups` would read as a broken compactor when it
            // is a busy catalog.
            //
            // ANYTHING ELSE IS AN ORDINARY FAILED GROUP, and it takes
            // [failGroup] rather than a `throw` — see that function for
            // the sweep this arm's rethrow used to kill.
            if (!Pg.isLockTimeout(e)) return failGroup(e)
            log.warn {
                "compaction group of ${group.files.size} files for " +
                    "$catalog/${ctx.namespace}.${ctx.table} hit the commit transaction's " +
                    "lock_timeout inside the commit tail; skipping — the staged output stays " +
                    "queued for the cleanup drain"
            }
            return GroupTally(skippedConflicts = 1)
        } catch (e: UnconvertibleSchemaException) {
            // Skip-with-reason, not a failure: the group stays
            // uncompacted until the schema or the file set moves.
            log.warn {
                "compaction group of ${group.files.size} files for " +
                    "$catalog/${ctx.namespace}.${ctx.table} is not convertible to the live " +
                    "schema (${e.message}); skipping"
            }
            return GroupTally(unconvertibleSchema = 1)
        } catch (e: InvalidDataException) {
            // Skip-with-reason as well, but a DIFFERENT reason:
            // DURABLE and the writer's fault. Unlike a schema
            // skip this will not clear on its own, so re-planning
            // it every sweep is a permanent loop. Counted apart
            // so a nonzero value reads as "a writer produced
            // something its own registration or schema forbids",
            // and logged at warn with the offending detail for
            // exactly that hunt.
            log.warn {
                "compaction group of ${group.files.size} files for " +
                    "$catalog/${ctx.namespace}.${ctx.table} cannot be rewritten as registered " +
                    "(${e.message}); skipping"
            }
            return GroupTally(invalidData = 1)
        } catch (e: SpillBudgetExceededException) {
            // The rewrite's own HARD STOP: the bytes actually spilled (or
            // the exact footers) broke a budget the registered metadata
            // fit — the planner refuses the predictable cases before any
            // IO. Its own arm, ahead of `Throwable`, because it is a
            // configuration signal and not a failure: counted with the
            // planner's refusals and, like them, not charged. The output
            // upload was discarded and the spill directory removed by the
            // rewriter on the way out.
            //
            // The WORK it spent is on the exception, not on a result —
            // there is none — and it is folded in here: a hard stop is
            // the group that spilled the most, and leaving it out made
            // `spill_bytes` / `runs_spilled` miss exactly those groups.
            log.warn {
                "compaction group of ${group.files.size} files for " +
                    "$catalog/${ctx.namespace}.${ctx.table} stopped at its spill budget " +
                    "(${e.message}); skipping"
            }
            Metrics.compactionSortCheckBytes(catalog, e.sortCheckBytes)
            return work +
                stoppedWork(
                    e.spillBytes,
                    e.runsSpilled,
                    e.runsDemoted,
                    e.filesVerified,
                    e.filesUnsorted,
                    e.filesUnchecked,
                ) +
                GroupTally(spillBudgetExceeded = 1)
        } catch (e: MergeBudgetExceededException) {
            log.warn {
                "compaction group of ${group.files.size} files for " +
                    "$catalog/${ctx.namespace}.${ctx.table} cannot be merged inside its heap budget " +
                    "(${e.message}); skipping"
            }
            Metrics.compactionSortCheckBytes(catalog, e.sortCheckBytes)
            return work +
                stoppedWork(
                    e.spillBytes,
                    e.runsSpilled,
                    e.runsDemoted,
                    e.filesVerified,
                    e.filesUnsorted,
                    e.filesUnchecked,
                ) +
                GroupTally(mergeBudgetExceeded = 1)
        } catch (e: OutOfMemoryError) {
            // The backstop, and it should be unreachable: a sorted
            // rewrite's chunk is sized by the measured per-node cost and
            // its merge is admitted run by run against the same budget
            // (hoglake#134). Reaching it means one of those two
            // under-counted THIS table's shape — a row far wider than its
            // schema's node count says, or row groups larger than their
            // footers report — which is an operator signal, not a retry.
            //
            // Caught at the GROUP boundary because nothing else caught it
            // at all: `catch (e: Exception)` below does not match an
            // Error, so the OOM used to unwind the whole sweep — losing
            // every other table's accounting and leaving the run ledger a
            // bare "Java heap space" with no counters (hoglake#118).
            // Catching it is defensible because what it aborts is one
            // rewrite whose chunk, runs and writer are unreachable the
            // instant this frame unwinds.
            //
            // And then the sweep STOPS. Continuing would allocate the next
            // group into a heap that just proved it has none to spare.
            // Counted as a failed group: `heap_budget_exceeded` is the
            // pre-#134 planner refusal and stays historical.
            heapExhausted.set(true)
            log.error(e) {
                "compaction group of ${group.files.size} files " +
                    "(${group.survivingRecords} survivors) exhausted the heap for " +
                    "$catalog/${ctx.namespace}.${ctx.table}; ending the sweep. A sorted rewrite " +
                    "holds one chunk of ${cfg.spillChunkRows(ctx.columns)} rows or its admitted " +
                    "merge runs, each bounded by ${cfg.sortedHeapBytesPerGroup} B per group, so " +
                    "the chunk accounting (per-node heap estimate, nested expansion) or the merge " +
                    "admission under-counted this table's shape. Lower " +
                    "HOGLAKE_COMPACTION_SORTED_HEAP_BYTES or raise " +
                    "HOGLAKE_COMPACTION_NESTED_SORT_EXPANSION, and report the table's shape"
            }
            return GroupTally(failedGroups = 1)
        } catch (e: Throwable) {
            // One bad group (unreadable input, corrupt DV, S3
            // hiccup, a stack overflow in the nested copier) never
            // wedges the sweep — but it IS counted: an uncounted
            // swallow is a silently-dead compactor with a green run
            // ledger (the NoSuchBucket incident).
            return failGroup(e)
        } finally {
            // RELEASED ONLY IF THE GROUP DID NOT COMMIT, and that
            // asymmetry is the point.
            //
            // A group that did NOT commit — skipped, failed, refused —
            // leaves files that are still live candidates, and the next
            // maintainer to plan them should be free to try immediately;
            // holding the claim for the rest of its lease would just
            // delay the retry.
            //
            // A group that DID commit leaves the opposite situation. Its
            // inputs are end-snapshotted, so no future plan can include
            // them — but a plan another maintainer formed BEFORE the
            // commit still names them, and that maintainer is about to
            // arrive, acquire the freshly-released claim, and spend a
            // full rewrite and upload on files that are already dead
            // before its own commit re-verification refuses it. That is
            // exactly the lost race the claims exist to remove, arriving
            // one step later. Keeping the claim in place turns it into a
            // counted claimed_elsewhere.
            //
            // It is kept on a SHORT lease, not the full one
            // (CompactionConfig.committedClaimTtlSeconds): the only
            // reader it has left is a sibling's plan that predates this
            // commit, and a plan is at most one sweep old. Holding the
            // row for the full rewrite lease instead would leave roughly
            // committed-groups-per-sweep x lease/interval rows per
            // table, every one of whose input-id arrays the planner then
            // reads on every pass.
            //
            // Best-effort either way — the lease is the backstop for a
            // release that never runs at all (a killed pod), so a failed
            // DELETE is a delay and never a leak.
            if (claimKey != null) {
                runCatching {
                    jdbi.withHandleUnchecked { h ->
                        if (committed) {
                            CompactionClaimRepo.shortenToCommitted(
                                h,
                                ctx.catalogId,
                                ctx.tableId,
                                claimKey,
                                item.claimant,
                                cfg.committedClaimTtlSeconds,
                            )
                        } else {
                            CompactionClaimRepo.release(h, ctx.catalogId, ctx.tableId, claimKey, item.claimant)
                        }
                    }
                }.onFailure { e ->
                    log.warn(e) {
                        "could not settle the compaction claim for " +
                            "$catalog/${ctx.namespace}.${ctx.table}; it expires in " +
                            "${cfg.claimTtlSeconds}s"
                    }
                }
            }
        }
    }

    /** What a sorted rewrite spent before a budget hard stop (carried on the exception). */
    private fun stoppedWork(
        spillBytes: Long,
        runsSpilled: Int,
        runsDemoted: Int,
        filesVerified: Int,
        filesUnsorted: Int,
        filesUnchecked: Int,
    ) = GroupTally(
        spillBytes = spillBytes,
        runsSpilled = runsSpilled.toLong(),
        runsDemoted = runsDemoted.toLong(),
        filesVerified = filesVerified.toLong(),
        filesUnsorted = filesUnsorted.toLong(),
        filesUnchecked = filesUnchecked.toLong(),
    )

    /** A broken claim table still permits work; a lost claim does not. */
    private fun refreshClaim(
        ctx: TableContext,
        claimKey: String,
        claimant: UUID,
        cfg: CompactionConfig,
    ): Boolean =
        runCatching {
            jdbi.withHandleUnchecked { h ->
                CompactionClaimRepo.refresh(
                    h,
                    ctx.catalogId,
                    ctx.tableId,
                    claimKey,
                    claimant,
                    cfg.claimTtlSeconds,
                )
            }
        }.getOrElse { e ->
            log.warn(
                e,
            ) { "compaction claim refresh failed for ${ctx.namespace}.${ctx.table}; continuing without a claim" }
            true
        }

    /** The catalog id, for the sweep's own bookkeeping statements. */
    private fun catalogIdOf(
        h: Handle,
        catalog: String,
    ): Long =
        CatalogRepo.findByName(h, catalog)?.catalogId
            ?: throw HoglakeException.NotFound("catalog '$catalog'")

    private fun liveTables(
        h: Handle,
        catalog: String,
    ): List<Pair<String, String>> {
        val cat =
            CatalogRepo.findByName(h, catalog)
                ?: throw HoglakeException.NotFound("catalog '$catalog'")
        return h.createQuery(
            """
            SELECT ns.name AS namespace, tv.name AS table_name
            FROM hog_table_version tv
            JOIN hog_namespace ns
              ON ns.catalog_id = tv.catalog_id AND ns.namespace_id = tv.namespace_id
            JOIN hog_table t
              ON t.catalog_id = tv.catalog_id AND t.table_id = tv.table_id
            WHERE tv.catalog_id = :catalogId AND tv.end_snapshot IS NULL
              AND NOT ns.dropped AND t.dropped_snapshot IS NULL
            ORDER BY ns.name, tv.name
            """,
        )
            .bind("catalogId", cat.catalogId)
            .map { rs, _ -> rs.getString("namespace") to rs.getString("table_name") }
            .list()
    }

    // ---- group execution + commit ----------------------------------------

    /**
     * Execute + commit one ALREADY-PLANNED group, without re-planning.
     * Test surface for the plan-to-commit races (an input dying, a DV
     * appearing or growing between planning and here must abort the
     * commit); production traffic goes through [runOnce], which plans
     * and executes in one sweep.
     */
    internal fun compactPlannedGroup(
        catalog: String,
        namespace: String,
        table: String,
        group: CompactionGroup,
    ): GroupOutcome {
        // The CONTEXT only, on its own short read: this surface is handed
        // a group, so planning one (a bounded candidate fetch and a whole
        // pack) would be work thrown away.
        val ctx =
            jdbi.inTransactionUnchecked { h ->
                h.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                tableContext(h, catalog, namespace, table, defaults)
            }
        return compactGroup(ctx, group)
    }

    /**
     * Rewrite one group (all IO outside any transaction) and commit it.
     */
    private fun compactGroup(
        ctx: TableContext,
        group: CompactionGroup,
        onRewritten: (ParquetRewriter.RewriteResult) -> Unit = {},
    ): GroupOutcome {
        val inputs =
            group.files.map { f ->
                // Read the object IN PLACE. This used to fetch the
                // whole thing into a heap array and write it to
                // java.io.tmpdir first — three passes over every
                // input byte before the rewrite began, and a staging
                // footprint that scaled with the group against an
                // emptyDir whose overrun EVICTS the pod.
                val source =
                    S3InputFile(store, f.path, f.fileSizeBytes, f.footerSize)
                val dv =
                    f.dv?.let { planned ->
                        // The FETCH stays outside: an object-store error is
                        // transient and belongs in the retryable channel.
                        // What the decoder says about bytes it already has
                        // is durable — a registered .dv never changes — so
                        // a refusal from it (PuffinDeletionVector's own
                        // requires, and the containment around the roaring
                        // library) is invalid_data, not a group re-planned
                        // and re-refused every sweep forever.
                        val raw = store.get(planned.path)
                        val decoded =
                            try {
                                PuffinDeletionVector.read(raw)
                            } catch (e: IllegalArgumentException) {
                                if (e is InvalidDataException) throw e
                                throw InvalidDataException(
                                    "DV ${planned.path} does not decode: ${e.message}",
                                )
                            }
                        if (decoded.cardinality != planned.deleteCount) {
                            throw InvalidDataException(
                                "DV ${planned.path} decodes to ${decoded.cardinality} positions " +
                                    "but is registered with delete_count ${planned.deleteCount} — " +
                                    "refusing to compact on inconsistent metadata",
                            )
                        }
                        decoded
                    }
                ParquetRewriter.Input(
                    source,
                    f.path,
                    f.rowIdStart,
                    dv,
                    f.explicitRowIds,
                    trustedSorted = trustedSorted(ctx, f),
                    // Always passed: admission costs runs from them before
                    // any byte is read, and 0 would mean "unknown".
                    fileSizeBytes = f.fileSizeBytes,
                    survivingRecords = f.survivingRecords,
                    // The sortedness pre-pass's own handle on the object:
                    // the same footer hint, a readahead sized for reading
                    // a few non-adjacent key column chunks (see the
                    // constant). Only a sorted table's rewrite opens it.
                    keySource =
                        S3InputFile(
                            store,
                            f.path,
                            f.fileSizeBytes,
                            f.footerSize,
                            readaheadBytes = S3InputFile.KEY_COLUMN_READAHEAD_BYTES,
                        ),
                )
            }
        // Bare UUID, deliberately indistinguishable from an ingested
        // file (the pyhoglake writer's shape). A `compacted-` prefix
        // used to sit here; it told readers nothing the catalog does
        // not already say — explicit_row_ids is the flag that decides
        // how a file's row ids are read, and no reader may infer that
        // from a path — while giving every compaction output in a
        // table the same 10-character lead-in.
        val outputPath =
            "${ctx.dataPath.trimEnd('/')}/data/${ctx.namespace}/${ctx.table}/" +
                "${UUID.randomUUID()}.parquet"

        // Claim ticket BEFORE the rewrite (its own committed
        // transaction): if this group never commits — skip, crash,
        // failed upload — the undrained row hands the object to the
        // normal cleanup drain. The group commit settles it.
        //
        // ORDERING CHANGED when the rewrite began streaming, and the
        // consequence is worth stating because it relaxed an
        // invariant the tests used to pin. The old sequence was
        // fetch -> rewrite to local disk -> claim -> upload, so a
        // rewrite that failed had touched nothing and left no row.
        // The output now streams to its final path, so bytes can
        // land the moment parquet flushes its first part, and the
        // claim has to come first.
        //
        // So for a DV-free group there is no longer any point at
        // which compaction can fail WITHOUT having claimed a ticket.
        // Every failure leaves one removal row for a path that holds
        // no object — S3OutputFile aborts its multipart upload — and
        // the cleanup drain reclaims it, counting it `missing`,
        // which it already handles.
        //
        // The trade: one extra removal row per failed group, against
        // a claim that now covers the WHOLE window in which bytes
        // could exist rather than starting after it. Taken
        // deliberately; the alternative is to have the sink claim
        // the ticket on its first part upload, which preserves the
        // old invariant exactly but threads a side effect into the
        // writer and makes the claim's timing invisible at this call
        // site. Revisit if the `missing` rows ever become noise.
        val stagingId = stageOutputPath(ctx.catalogId, outputPath)

        // The rewrite streams STRAIGHT to the output path, so the
        // ticket above must already be claimed: bytes begin landing
        // the moment parquet flushes its first part, not after. That
        // is the whole reason the path is minted before the rewrite
        // rather than after it.
        //
        // Nothing is written locally and nothing is read back: the
        // stream reports its own size and footer length, which used
        // to cost two more full passes over the output.
        val sink = S3OutputFile(store, outputPath)
        beforeRewrite(inputs)
        val rewritten =
            ParquetRewriter.rewrite(
                inputs,
                ctx.columns,
                ctx.sortFields,
                sink,
                ctx.maxNodesPerRow,
                ctx.codec,
                ctx.inputOpenParallelism,
                ctx.sortSpill,
                appendFloorBytes,
            )
        onRewritten(rewritten)
        check(rewritten.rowsWritten == group.survivingRecords) {
            // Name the FILES, not just the counts. This check is durable
            // by nature — a mis-registered record_count does not heal —
            // so the group is re-planned and re-failed every sweep, and
            // because a failure is charged to maxGroupsPerRun (default 1)
            // the whole catalog stops compacting behind it. An operator
            // reading `failed_groups=1` on a loop needs the offending
            // data_file_id to get anywhere; counts alone give them
            // nothing to grep for.
            "rewrite produced ${rewritten.rowsWritten} rows but inputs registered " +
                "${group.survivingRecords} survivors — refusing to commit a lossy compaction. " +
                "Inputs (data_file_id: registered records, live deletes): " +
                group.files.joinToString(", ") {
                    "${it.dataFileId}: ${it.recordCount}, ${it.dv?.deleteCount ?: 0}"
                }
        }
        val outputBytes = sink.bytesWritten
        val footerSize = sink.footerSize
        // The output's row-group offsets, off the footer the writer just
        // built and still holds — no read-back, no extra IO, a walk over
        // the row groups. Registered for pending outputs too (the
        // hydrator would compute the same list from the same footer);
        // null only when that footer gives no list honouring the
        // contract, and then readers cut the file evenly.
        val splitOffsets = rewritten.footer?.let { FooterSplitOffsets.of(it, outputBytes) }

        // THE OUTPUT'S STATS COME OFF THE FOOTER THE WRITER JUST BUILT,
        // not out of the catalog. The parquet writer accumulated a
        // min/max/null-count per column chunk over exactly the rows it
        // wrote, so the aggregate below is EXACT for the survivor set,
        // needs no query, and holds no connection — and it is the same
        // [FooterStats.aggregate] the hydrator runs on every
        // client-written file, sanity checks included.
        //
        // What it replaces, and why: this used to sum the INPUTS'
        // hog_file_column_stats rows (`WHERE data_file_id IN (<ids>)`)
        // and re-merge their typed bounds. Fan-in scales with the byte
        // target, so on gigahog-prod-us a group is 2,048 files: an
        // IN-list of 2,048 ids against the PK
        // `(catalog_id, data_file_id, field_id)` is 2,048 index range
        // scans, each yielding that file's ~26 adjacent column rows —
        // ~2,048 index descents and ~53k heap rows, spread across a
        // 66 GiB table's pages written at different times — to recompute
        // what the writer had just observed directly. On 2026-09-30 that
        // read crossed the 60 s session statement_timeout and every
        // sweep failed. It also had to give up on any group with a DV'd
        // input, whose registered counts describe pre-delete rows: those
        // outputs registered 'pending' and waited for a hydrator sweep
        // to re-read the footer from S3. The footer answers both.
        //
        // Null footer = the writer gave none back (ParquetRewriter asks
        // for it outside the failure path and never lets it fail the
        // rewrite), which is the one case that still registers 'pending'
        // for the hydrator. An EMPTY aggregate is not that case: it
        // registers 'provided' with no rows, exactly as the hydrator
        // leaves such a file.
        val stats =
            rewritten.footer?.let { footer ->
                FooterStats.aggregate(footer, ctx.columns.asCatalogColumns(), outputPath)
                    .map { agg ->
                        ColumnStats(
                            fieldId = agg.fieldId,
                            valueCount = agg.valueCount,
                            nullCount = agg.nullCount,
                            nanCount = agg.nanCount,
                            sizeBytes = agg.sizeBytes,
                            lowerBound = agg.lowerBound,
                            upperBound = agg.upperBound,
                        )
                    }
            }
        val outcome =
            commitGroup(
                ctx, group, outputPath, outputBytes, footerSize, stats,
                splitOffsets = splitOffsets,
                survivors = rewritten.rowsWritten,
                rowIdStart = rewritten.minRowId ?: group.files.minOf { it.rowIdStart },
                stagingId = stagingId,
            )
        when (outcome) {
            is GroupOutcome.Committed ->
                log.info {
                    "compacted ${group.files.size} files (${group.totalBytes} B, " +
                        "${group.totalRecords} rows, ${rewritten.rowsWritten} survivors) of " +
                        "${ctx.namespace}.${ctx.table} into $outputPath ($outputBytes B) " +
                        "at snapshot ${outcome.snapshotId}"
                }
            GroupOutcome.SkippedConflict, GroupOutcome.SkippedDvSuperseded ->
                log.warn {
                    "compaction group for ${ctx.namespace}.${ctx.table} lost a " +
                        "plan-to-commit race ($outcome); skipping — staged output " +
                        "$outputPath stays queued for the cleanup drain to reclaim"
                }
        }
        return outcome
    }

    /** Insert the output path's compaction_staging claim ticket; returns removal_id. */
    private fun stageOutputPath(
        catalogId: Long,
        outputPath: String,
    ): Long = jdbi.withHandleUnchecked { h -> stageOutputPath(h, catalogId, outputPath) }

    /**
     * Same, on a caller-supplied handle — so the commit path can
     * re-stage inside its own transaction instead of taking a second
     * connection from the pool while holding one under the catalog lock.
     */
    private fun stageOutputPath(
        h: Handle,
        catalogId: Long,
        outputPath: String,
    ): Long =
        run {
            h.createQuery(
                """
                INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
                VALUES (:catalogId, :path, 'data', 'compaction_staging')
                RETURNING removal_id
                """,
            )
                .bind("catalogId", catalogId)
                .bind("path", outputPath)
                .mapTo(Long::class.javaObjectType)
                .one()
        }

    /**
     * The metadata commit for one group: one transaction under the
     * per-catalog commit lock, CommitService's tail shape (parallel
     * code by design — its helpers are private and shaped around
     * appends; the comments here mark each mirrored step).
     *
     * [stats] is the output's own footer aggregate (see [compactGroup]):
     * a list — possibly EMPTY — registers the file 'provided', and null
     * registers it 'pending' for the hydrator. Null means the writer
     * returned no footer, nothing else.
     *
     * An empty list cannot happen for a compaction output today, and is
     * accepted rather than rejected only to keep one contract with the
     * hydrator, whose client-written files can produce it. The output
     * schema is machine-generated from the live columns, so the shapes
     * FooterStats refuses a whole file for — duplicate field ids,
     * duplicate names inside a group — are structurally impossible here,
     * and a zero-row group still produces a row per column. If one ever
     * did arrive it registers 'provided' with no stats rows, which is
     * what the hydrator does with such a file.
     *
     * The two paths' rows are not identical, and the difference is
     * deliberate: the hydrator's flip also sets `missing_field_ids` and
     * `split_offsets` from the footer it read, while compaction sets
     * `split_offsets` and leaves `missing_field_ids` at its DEFAULT
     * false — correct for its own output, which stamps an id on every
     * node it writes.
     */
    private fun commitGroup(
        ctx: TableContext,
        group: CompactionGroup,
        outputPath: String,
        outputBytes: Long,
        footerSize: Long,
        stats: List<ColumnStats>?,
        splitOffsets: List<Long>?,
        survivors: Long,
        rowIdStart: Long,
        stagingId: Long,
    ): GroupOutcome =
        jdbi.inTransactionUnchecked { h ->
            Locks.acquireCatalogCommitLock(h, ctx.catalogId, ctx.commitLockTimeoutMs)
            beforeCommitTail(h)

            // RE-CLAIM THE STAGING TICKET: THE OBJECT IS ONLY OURS TO
            // REGISTER IF NOBODY HAS TOUCHED THE TICKET. It used to be the
            // catalog commit lock that made this safe; cleanup no longer
            // takes one.
            //
            // Cleanup is a claimed work queue: it stamps
            // `claimed_at`/`claimed_by` in one short transaction, then
            // deletes objects with NOTHING OPEN — no transaction, no row
            // lock — and settles in another. So the row lock this read
            // takes serializes the two writers on the ROW and does
            // nothing whatsoever about the S3 DELETE, which is why the
            // predicate is three terms and not one:
            //
            //  - `drained_at IS NULL` — a settled ticket is somebody's
            //    finished work;
            //  - `claimed_at IS NULL` — ANY claim, live or lapsed. A
            //    lapsed claim does not mean the object survived; it means
            //    NOBODY KNOWS. A worker whose lease ran out mid-work, or
            //    that was killed after its DELETE and before its settle,
            //    leaves exactly this row — and registering it produces a
            //    live `hog_data_file` row pointing at a deleted object,
            //    which no check in the system can see (every
            //    `staging_tickets` arm passes once the file row exists);
            //  - `attempts = 0` — nobody has tried and failed either. A
            //    lost `DeleteObjects` RESPONSE fails the chunk, bumps
            //    `attempts` and RELEASES the claim, so `claimed_at` is
            //    null again while the object may well be gone server-side.
            //    `attempts` is the monotone "somebody has been here" mark
            //    that closes that route.
            //
            // FOR UPDATE takes the row lock, so this read and the settle
            // below see the same row and no drain can claim or settle it
            // in between — which is what makes the settle's
            // `check(settled == 1)` unreachable. It waits at most for one
            // claim statement (a single UPDATE) because the claim uses
            // SKIP LOCKED and never waits on us.
            //
            // The cost of refusing a touched ticket is ONE GROUP: the
            // rewrite is thrown away, the path is re-staged so the object
            // cannot outlive a ticket naming it, and the next sweep plans
            // a fresh group with a NEW UUID output path. Compaction
            // therefore does not know or care how long a cleanup lease is
            // — the lease is CleanupService's business alone.
            val ticketLive =
                h.createQuery(
                    """
                SELECT (drained_at IS NULL AND claimed_at IS NULL AND attempts = 0)
                FROM hog_file_removal
                WHERE catalog_id = :catalogId AND removal_id = :removalId
                FOR UPDATE
                """,
                )
                    .bind("catalogId", ctx.catalogId)
                    .bind("removalId", stagingId)
                    .mapTo(Boolean::class.javaObjectType)
                    .findOne()
                    .orElse(false)
            if (!ticketLive) {
                // The object is NOT necessarily gone — it may be settled
                // and deleted, or merely CLAIMED by a worker that is
                // deleting it as this runs. Either way the path is not
                // this group's to register.
                //
                // That it may still exist is a consequence of streaming
                // the output. The old ordering
                // claimed the ticket immediately before a single PUT, so
                // a drain that beat the commit almost always beat the
                // object's existence too. Now the claim precedes the
                // whole rewrite, and the drain window is the entire
                // rewrite: cleanup can find nothing, mark the ticket
                // absent, and the upload can complete afterwards.
                //
                // At that point the object exists with no catalog row
                // and a ticket that says it was already handled — which
                // is a leak nothing else can see, because the removal
                // ledger is the only thing that knows the path. So
                // re-stage it: a fresh undrained ticket for the same
                // path puts it back in front of the drain.
                //
                // Unconditional rather than conditional on a HEAD: a
                // ticket for an object that does not exist costs one
                // `missing` on the next drain, which cleanup already
                // counts, and probing would add a round trip inside the
                // commit lock to save nothing.
                stageOutputPath(h, ctx.catalogId, outputPath)
                log.warn {
                    "compaction staging ticket $stagingId for $outputPath was drained or claimed " +
                        "by cleanup before the group committed; aborting the group and " +
                        "re-staging the path so the object cannot outlive its ticket"
                }
                return@inTransactionUnchecked GroupOutcome.SkippedConflict
            }

            // THE TABLE IS STILL THE AUTHORITY AT COMMIT TIME. The
            // planner's `liveTables` already excludes dropped tables, so
            // a group is only ever planned against a live one; what this
            // covers is the IN-FLIGHT window — a rewrite takes ~8.5 s of
            // object-store latency, and a DROP can land inside it.
            //
            // Until #193 the fence was accidental: the drop end-snapshotted
            // every file row, so the `live` check below caught it. Now a
            // drop touches no file row at all, so every input is still
            // `end_snapshot IS NULL` and the group would COMMIT — writing
            // a new data file, a snapshot and a change row into a table
            // that no longer exists, and settling its staging ticket
            // `registered` so cleanup would never reclaim the object it
            // just uploaded. Read under the same lock as everything else
            // here, so a drop either precedes this read or waits behind
            // this transaction.
            val droppedSnapshot =
                h.createQuery(
                    """
                SELECT dropped_snapshot FROM hog_table
                WHERE catalog_id = :catalogId AND table_id = :tableId
                """,
                )
                    .bind("catalogId", ctx.catalogId)
                    .bind("tableId", ctx.tableId)
                    .mapTo(Long::class.javaObjectType)
                    .findOne()
                    .orElse(null)
            if (droppedSnapshot != null) {
                log.info {
                    "compaction group for ${ctx.namespace}.${ctx.table} was rewritten against a " +
                        "table dropped in snapshot $droppedSnapshot; skipping the commit. The " +
                        "uploaded output stays staged and the normal cleanup drain reclaims it"
                }
                return@inTransactionUnchecked GroupOutcome.SkippedConflict
            }

            // THE SORT SPEC IS STILL THE ONE PLANNED AGAINST. The output was
            // sorted by the spec live at planning (or not sorted at all),
            // and it is about to register with a `begin_snapshot` past
            // every spec that exists — which [trustedSorted] reads as
            // "written under the live spec". A `set_sort_order` that landed
            // inside the rewrite would make this output a TRUSTED RUN of a
            // spec it is not sorted by, forever: every later sorted rewrite
            // would merge it in place and publish a mis-sorted file. So a
            // moved spec — changed, set on an unsorted table, or dropped —
            // is a lost race like any other: skipped, re-planned next
            // sweep under the new spec, the staged output left for the
            // cleanup drain. One probe on `hog_sort_spec_live`, under the
            // lock DDL also takes, so a spec change either precedes this
            // read or waits behind this transaction.
            val liveSortId =
                h.createQuery(
                    """
                SELECT sort_id FROM hog_sort_spec
                WHERE catalog_id = :catalogId AND table_id = :tableId AND end_snapshot IS NULL
                """,
                )
                    .bind("catalogId", ctx.catalogId)
                    .bind("tableId", ctx.tableId)
                    .mapTo(Long::class.javaObjectType)
                    .findOne()
                    .orElse(null)
            if (liveSortId != ctx.sortId) {
                log.info {
                    "compaction group for ${ctx.namespace}.${ctx.table} was rewritten under sort " +
                        "spec ${ctx.sortId ?: "none"} but the live spec is now ${liveSortId ?: "none"}; " +
                        "skipping the commit so the output cannot be trusted as a run of a spec it " +
                        "is not sorted by. The staged output stays queued for the cleanup drain"
                }
                return@inTransactionUnchecked GroupOutcome.SkippedConflict
            }

            // Re-verify under the lock: every input must still be live and
            // must still carry EXACTLY its planned DV (supersession mints a
            // new delete_file_id; growth without supersession is impossible).
            data class LiveRow(val live: Boolean, val liveDvId: Long?)

            val ids = group.files.map { it.dataFileId }
            val liveState =
                h.createQuery(
                    """
                SELECT f.data_file_id, (f.end_snapshot IS NULL) AS live,
                       dv.delete_file_id AS dv_id
                FROM hog_data_file f
                LEFT JOIN hog_delete_file dv
                  ON dv.catalog_id = f.catalog_id
                 AND dv.data_file_id = f.data_file_id
                 AND dv.end_snapshot IS NULL
                WHERE f.catalog_id = :catalogId AND f.table_id = :tableId
                  AND f.data_file_id IN (<ids>)
                """,
                )
                    .bind("catalogId", ctx.catalogId)
                    .bind("tableId", ctx.tableId)
                    .bindList("ids", ids)
                    .map { rs, _ ->
                        rs.getLong("data_file_id") to
                            LiveRow(
                                live = rs.getBoolean("live"),
                                liveDvId = rs.getObject("dv_id")?.let { (it as Number).toLong() },
                            )
                    }
                    .list()
                    .toMap()
            if (group.files.any { liveState[it.dataFileId]?.live != true }) {
                return@inTransactionUnchecked GroupOutcome.SkippedConflict
            }
            if (group.files.any { liveState.getValue(it.dataFileId).liveDvId != it.dv?.deleteFileId }) {
                // A DV appeared or grew after planning: committing the
                // rewrite would resurrect those deletes. Never drop them.
                return@inTransactionUnchecked GroupOutcome.SkippedDvSuperseded
            }

            // Allocate snapshot + file id under the lock (CommitService
            // step 5); compaction is not DDL: schema_version untouched.
            val (snapshotId, dataFileId, schemaVersion) =
                h.createQuery(
                    """
                UPDATE hog_catalog
                   SET last_snapshot_id = last_snapshot_id + 1,
                       next_file_id = next_file_id + 1
                 WHERE catalog_id = :catalogId
                RETURNING last_snapshot_id, next_file_id - 1 AS file_id, schema_version
                """,
                )
                    .bind("catalogId", ctx.catalogId)
                    .map { rs, _ -> Triple(rs.getLong(1), rs.getLong(2), rs.getLong(3)) }
                    .one()

            SnapshotRepo.insert(
                h,
                ctx.catalogId,
                snapshotId,
                schemaVersion,
                author = "compaction",
                message = "compacted ${group.files.size} files of ${ctx.namespace}.${ctx.table}",
            )
            SnapshotRepo.insertChange(h, ctx.catalogId, snapshotId, ChangeKind.TABLE_COMPACTED, ctx.tableId)

            // The output row. record_count = SURVIVORS (post-DV);
            // row_id_start = min surviving id — with explicit_row_ids its
            // positional meaning is void (the ids live in the _hog_row_id
            // column); it survives as the range-min for ordering and
            // diagnostics.
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, footer_size,
                                           row_id_start, stats_state, spec_id, explicit_row_ids,
                                           split_offsets)
                VALUES (:catalogId, :dataFileId, :tableId, :beginSnapshot,
                        :path, :recordCount, :fileSizeBytes, :footerSize,
                        :rowIdStart, :statsState, :specId, true,
                        :splitOffsets)
                """,
            )
                .bind("catalogId", ctx.catalogId)
                .bind("dataFileId", dataFileId)
                .bind("tableId", ctx.tableId)
                .bind("beginSnapshot", snapshotId)
                .bind("path", outputPath)
                .bind("recordCount", survivors)
                .bind("fileSizeBytes", outputBytes)
                .bind("footerSize", footerSize)
                .bind("rowIdStart", rowIdStart)
                .bind("statsState", if (stats != null) "provided" else "pending")
                .bind("specId", group.specId)
                .bindBigintArrayOrNull("splitOffsets", splitOffsets)
                .execute()

            val values = group.partitionValues
            if (values != null) {
                val batch =
                    h.prepareBatch(
                        """
                    INSERT INTO hog_file_partition_value (catalog_id, data_file_id, key_index, value)
                    VALUES (:catalogId, :dataFileId, :keyIndex, :value)
                    """,
                    )
                values.forEachIndexed { keyIndex, value ->
                    batch
                        .bind("catalogId", ctx.catalogId)
                        .bind("dataFileId", dataFileId)
                        .bind("keyIndex", keyIndex)
                        .bind("value", value)
                        .add()
                }
                batch.execute()
            }

            if (stats != null && stats.isNotEmpty()) {
                val batch =
                    h.prepareBatch(
                        """
                    INSERT INTO hog_file_column_stats (catalog_id, data_file_id, field_id, value_count,
                                                       null_count, nan_count, size_bytes,
                                                       lower_bound, upper_bound)
                    VALUES (:catalogId, :dataFileId, :fieldId, :valueCount,
                            :nullCount, :nanCount, :sizeBytes,
                            :lowerBound, :upperBound)
                    """,
                    )
                for (s in stats) {
                    batch
                        .bind("catalogId", ctx.catalogId)
                        .bind("dataFileId", dataFileId)
                        .bind("fieldId", s.fieldId)
                        .bind("valueCount", s.valueCount)
                        .bind("nullCount", s.nullCount)
                        .bind("nanCount", s.nanCount)
                        .bind("sizeBytes", s.sizeBytes)
                        .bind("lowerBound", s.lowerBound)
                        .bind("upperBound", s.upperBound)
                        .add()
                }
                batch.execute()
            }

            // End-snapshot the inputs — NOT delete: they stay readable at
            // every snapshot below this one, and expiry queues their paths
            // once end_snapshot sinks under the retention floor (the
            // superseded-DV lifecycle). Nothing enters hog_file_removal
            // here. hog_table_stats is untouched (gross append counters;
            // visible-file aggregates shrink only by the rows the DVs
            // already masked).
            //
            // `table_id = :tableId` on both updates is a guard (#264), and
            // a CHECKED one. The liveState re-verify above, in this same
            // transaction under this same lock, already filtered every id
            // on `table_id` and bailed on any that was missing, so the
            // predicate matches every row the id set does — today. The
            // check is for the day it does not (an identity that is no
            // longer `(catalog_id, data_file_id)` alone, a DV whose
            // `table_id` disagrees with its file's): an UPDATE that ended
            // fewer rows than it was given would otherwise commit the
            // output LIVE beside a still-live input, serving its rows
            // twice with nothing logged. Failing here rolls the group
            // back into the one-failed-group path instead.
            val ended =
                h.createUpdate(
                    """
                    UPDATE hog_data_file SET end_snapshot = :snapshotId
                    WHERE catalog_id = :catalogId AND table_id = :tableId AND data_file_id IN (<ids>)
                    """,
                )
                    .bind("snapshotId", snapshotId)
                    .bind("catalogId", ctx.catalogId)
                    .bind("tableId", ctx.tableId)
                    .bindList("ids", ids)
                    .execute()
            check(ended == ids.size) {
                "end-snapshotted $ended of ${ids.size} inputs of ${ctx.namespace}.${ctx.table}; " +
                    "refusing to commit the group"
            }

            // The applied DVs die with their files: end-snapshot them so
            // scans at older snapshots still mask, and expiry queues the
            // puffin paths alongside the input parquets.
            val dvIds = group.files.mapNotNull { it.dv?.deleteFileId }
            if (dvIds.isNotEmpty()) {
                val endedDvs =
                    h.createUpdate(
                        """
                        UPDATE hog_delete_file SET end_snapshot = :snapshotId
                        WHERE catalog_id = :catalogId AND table_id = :tableId AND delete_file_id IN (<ids>)
                        """,
                    )
                        .bind("snapshotId", snapshotId)
                        .bind("catalogId", ctx.catalogId)
                        .bind("tableId", ctx.tableId)
                        .bindList("ids", dvIds)
                        .execute()
                check(endedDvs == dvIds.size) {
                    "end-snapshotted $endedDvs of ${dvIds.size} deletion vectors of " +
                        "${ctx.namespace}.${ctx.table}; refusing to commit the group"
                }
            }

            // Settle the staging ticket in the SAME transaction that makes
            // the path live: cleanup's drain and the commit path guard only
            // act on UNDRAINED rows, so the registered output can never be
            // reclaimed or refused. It leaves `claimed_at`/`claimed_by`
            // alone, and must — it only runs when they are already NULL,
            // because the re-read above refuses a ticket carrying ANY
            // claim, live or lapsed.
            //
            // THE PREDICATE IS THE RE-READ'S, VERBATIM — all three terms.
            // The row has been locked since the re-read, so the two
            // statements see the same row and `check(settled == 1)` is
            // unreachable rather than merely unlikely; a settle with a
            // different predicate from the read that authorised it would
            // make that check fire on a row this transaction had already
            // called its own.
            val settled =
                h.createUpdate(
                    """
                    UPDATE hog_file_removal
                       SET drained_at = now(), drained_outcome = 'registered',
                           last_attempt_at = now()
                     WHERE catalog_id = :catalogId AND removal_id = :removalId
                       AND drained_at IS NULL AND claimed_at IS NULL AND attempts = 0
                    """,
                )
                    .bind("catalogId", ctx.catalogId)
                    .bind("removalId", stagingId)
                    .execute()
            check(settled == 1) { "compaction staging ticket $stagingId vanished mid-commit" }

            GroupOutcome.Committed(snapshotId, outputBytes)
        }

    // ---- loops -----------------------------------------------------------

    /**
     * One sweep across every catalog, for the background loop
     * (BackgroundLoops in App.kt); per-catalog failure isolation
     * (Expiry pattern).
     */
    fun runOnceAllCatalogs(cfg: CompactionConfig = defaults): List<Pair<String, CompactionResult>> {
        val names = jdbi.withHandleUnchecked { h -> CatalogRepo.listAll(h) }.map { it.name }
        val results = mutableListOf<Pair<String, CompactionResult>>()
        for (name in names) {
            try {
                results += name to runOnce(name, cfg, MaintenanceTrigger.LOOP)
            } catch (e: InterruptedException) {
                // CANCELLATION IS NOT A PER-CATALOG FAILURE.
                //
                // `catch (Exception)` matched this, and per-catalog
                // isolation then did exactly what it is for and exactly
                // the wrong thing: it logged the cancelled catalog and
                // swept every remaining one. On a fleet with a dozen
                // catalogs that is a dozen purges, plans, waves and
                // bounded shutdowns after the supervisor asked the loop
                // to stop — and the supervisor's own cap is 5 s.
                //
                // Rethrown rather than broken out of, because the flag
                // is how `BackgroundLoops`' `runInterruptible` learns
                // the body was cancelled rather than merely finished.
                Thread.currentThread().interrupt()
                log.warn {
                    "compaction sweep was cancelled during catalog '$name'; " +
                        "stopping the instance-wide sweep after ${results.size} catalog(s)"
                }
                throw e
            } catch (e: Exception) {
                log.error(e) { "compaction sweep failed for catalog '$name'; continuing" }
            }
            // A catalog whose sweep swallowed the interrupt itself — a
            // wrapped one that never surfaced as InterruptedException —
            // still stops the fan-out here.
            if (Thread.currentThread().isInterrupted) {
                log.warn {
                    "compaction sweep was cancelled after catalog '$name'; " +
                        "stopping the instance-wide sweep after ${results.size} catalog(s)"
                }
                throw InterruptedException("compaction sweep cancelled after catalog '$name'")
            }
        }
        return results
    }

    /**
     * A cancelled sweep, carrying the counters it had already earned.
     *
     * An `InterruptedException` first and foremost, so every caller —
     * `BackgroundLoops`' `runInterruptible` above all — treats it as
     * cancellation rather than as an error. [MaintenanceRunStore.recorded]
     * reads [partial] off it and stores those counters on the failed
     * ledger row, because the groups they count are committed and
     * durable and a row that says a cancelled sweep did nothing is
     * simply wrong.
     */
    internal class SweepInterrupted(
        private val result: CompactionResult,
    ) : InterruptedException("compaction sweep was cancelled"), PartialResult {
        override val partial: Any get() = result
    }

    internal companion object {
        /**
         * The cap on how many of a table's sampled buckets one plan may
         * READ — see `CompactionService.sampledBuckets`. 50,000 is an
         * order of magnitude above `PartitionListingService`'s
         * documented 5,000-bucket stress shape, so it bounds a
         * `(team, day)` spec's partition explosion and bites nothing
         * else.
         */
        const val MAX_SAMPLED_BUCKETS = 50_000

        /**
         * The `SET LOCAL statement_timeout` phase (a) of a plan runs
         * under — see `planSnapshot`. Two groups' measured wall time,
         * which is the point at which the cheap half of a sweep has
         * stopped being cheap.
         */
        const val PLAN_STATEMENT_TIMEOUT_MS = 15_000L

        /**
         * Does this throwable's cause chain hold an interrupt?
         *
         * The object store's client, the HTTP stack and NIO all
         * translate an interrupt into something else on the way up — a
         * `ClosedByInterruptException`, an SDK exception wrapping one,
         * an `IOException` with it several frames down — and several of
         * them clear the flag while they are at it. So the type of the
         * exception a worker catches is not the question; whether an
         * interrupt is anywhere underneath it is.
         *
         * Bounded walk: a self-referential cause chain is rare but it is
         * not this method's job to hang on one.
         */
        fun wasInterrupt(top: Throwable): Boolean {
            if (Thread.currentThread().isInterrupted) return true
            var e: Throwable? = top
            var depth = 0
            while (e != null && depth++ < CAUSE_CHAIN_LIMIT) {
                if (e is InterruptedException || e is java.nio.channels.ClosedByInterruptException) return true
                e = e.cause.takeIf { it !== e }
            }
            return false
        }

        /** How far [wasInterrupt] walks a cause chain. */
        const val CAUSE_CHAIN_LIMIT = 16

        /**
         * How long [shutdown] waits for a worker to notice that the
         * sweep is over.
         *
         * Under BackgroundLoops the supervisor's own hard cap is 5 s, so
         * this is deliberately inside it: a compaction worker that
         * outlives the supervisor's shutdown is a thread that commits
         * after the process believes compaction has stopped. 3 s is long
         * enough for an S3 call to notice an interrupt and short enough
         * to leave the supervisor margin to do its own bookkeeping.
         */
        const val SHUTDOWN_WAIT_SECONDS = 3L
    }
}
