package com.posthog.hoglake.compaction

import com.posthog.hoglake.compaction.CompactionWork.Bucket
import com.posthog.hoglake.compaction.CompactionWork.BucketClaim
import com.posthog.hoglake.compaction.CompactionWork.CandidateFetch
import com.posthog.hoglake.compaction.CompactionWork.PlanWithContext
import com.posthog.hoglake.compaction.CompactionWork.PlannedWork
import com.posthog.hoglake.compaction.CompactionWork.TableContext
import com.posthog.hoglake.compaction.CompactionWork.WorkItem
import com.posthog.hoglake.hydrator.FooterStats
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CompactionResult
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.ObjectPrefix
import com.posthog.hoglake.model.ObjectPrefixLayout
import com.posthog.hoglake.model.allNodes
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.CommitLockPath
import com.posthog.hoglake.persistence.LayoutRepo
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.persistence.MaintenanceRunStore
import com.posthog.hoglake.persistence.NamespaceRepo
import com.posthog.hoglake.persistence.PartialResult
import com.posthog.hoglake.persistence.Pg
import com.posthog.hoglake.persistence.SnapshotRepo
import com.posthog.hoglake.persistence.SortRepo
import com.posthog.hoglake.persistence.SpecRepo
import com.posthog.hoglake.persistence.TableRepo
import com.posthog.hoglake.persistence.TierTotalsRepo
import com.posthog.hoglake.persistence.bindBigintArrayOrNull
import com.posthog.hoglake.service.CompactionBackend
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.parquet.schema.GroupType
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.jdbi.v3.core.statement.UnableToExecuteStatementException
import java.sql.ResultSet
import java.util.UUID

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
     * How many of a table's NEWEST live small files one plan reads to
     * DISCOVER buckets the published sample cannot name
     * (`HOGLAKE_COMPACTION_NEW_BUCKET_WINDOW`; `CompactionService.newBuckets`).
     * 0 turns the read off, which is the pre-2026-10-21 planner exactly.
     *
     * Its own knob, not [maxCandidates], because it bounds a different
     * read for a different purpose. [maxCandidates] bounds the files a
     * plan fetches AS CANDIDATES; this bounds the files it walks only to
     * learn which partition tuples exist, each one costing a heap fetch
     * and a primary-key descent of `hog_file_partition_value` (4 buffers
     * per file measured), inside `planSnapshot`'s 15 s `SET LOCAL
     * statement_timeout`, on every sweep, on every pod that compacts.
     *
     * ON A HOT TABLE THE WINDOW IS FULL ON EVERY SWEEP, in normal
     * operation and not only when the sampler is behind: the read's lower
     * bound is the published sample's START snapshot, which with a
     * healthy sampler is one scan plus one refresh old — on
     * gigahog-prod-us's `events_raw`, ~1-2 h, ~200-400k appends at ~3,600
     * a minute. So the cost per sweep is this number of files, always.
     * At 50,000 (what borrowing [maxCandidates] cost) that is ~200k
     * buffers: sub-second warm, tens of seconds cold, and a timeout fails
     * the whole table's plan — on the one table that matters.
     *
     * 10,000 is ~3 minutes of prod-us appends, newest first: a tuple
     * being written to now is inside it, which is the case the read
     * exists for (today's partition, a new team's). A tuple written once
     * and never again since falls out of it and waits for the next
     * generation, which a re-anchoring sampler now publishes.
     */
    val newBucketWindow: Int = DEFAULT_NEW_BUCKET_WINDOW,
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
     * How long a BUCKET claim lives between refreshes
     * (`HOGLAKE_COMPACTION_BUCKET_CLAIM_TTL_SECONDS`), against
     * [claimTtlSeconds] for a group claim.
     *
     * # Why short, and why that is safe
     *
     * A bucket claim hides a WHOLE BUCKET from every sibling's planner —
     * every file in it, including the ones appended after the claim —
     * where a group claim hides only its own inputs. So a LEAKED one is
     * far more expensive, and leaks are routine: on SIGTERM
     * `BackgroundLoops.close` cancels, waits 5 s and abandons, so a
     * sweep mid-rewrite never reaches the release, and every bucket
     * claim its plan wrote would outlive the pod by the full lease. At
     * [claimTtlSeconds]' hour, a rolling deploy of nine pods takes a
     * large share of a hot table's buckets — today's day bucket among
     * them, where new small files land — out of compaction fleet-wide.
     *
     * So the claim takes this shorter lease and is RENEWED by the
     * per-group refresh that already runs before every group's IO
     * (`CompactionClaimRepo.refresh`, one UPDATE): each refresh renews
     * EVERY still-outstanding bucket claim of the run's current plan,
     * not only the group's own, so a bucket whose groups sit at the back
     * of the queue stays claimed as long as the queue ahead of it makes
     * progress. The lease therefore has to cover the longest gap between
     * two consecutive group starts — one group's rewrite — not the
     * whole queue: a live
     * queue keeps its claims, a dead one loses them within this many
     * seconds. 600 s ([committedClaimTtlSeconds]' figure) is ~70 times
     * the measured ~8.5 s a group.
     *
     * Too short costs nothing but the old behaviour: an expired bucket
     * claim lets a sibling probe the bucket, and its held groups are
     * then dropped by overlap as `claimed_elsewhere`.
     */
    val bucketClaimTtlSeconds: Long = DEFAULT_BUCKET_CLAIM_TTL_SECONDS,
    /**
     * The `SET LOCAL statement_timeout` a plan's READ (probe) transaction
     * runs under — not its claim transaction, which has its own fixed
     * bound equal to the plan lock's wait
     * (`CompactionService.CLAIM_STATEMENT_TIMEOUT_MS`)
     * (`HOGLAKE_COMPACTION_PLAN_STATEMENT_TIMEOUT_MS`, default 60,000 —
     * the session's own bound, `Database.SESSION_INIT_SQL`).
     *
     * It was a 15 s constant, a FAIL-FAST chosen while the probes ran
     * under the catalog planning lock: a slow probe then held the lock,
     * every sibling planner queued behind it, and failing that table fast
     * was the only way to let the fleet move on. Since #418 the lock
     * covers only the claim transaction, so a slow probe costs ONE pod
     * ONE sweep, and a probe that finishes yields the bucket's whole
     * window; cutting it at 15 s threw that work away on exactly the hot
     * buckets (millpond's 50k-127k-file days) that need it. So the bound
     * is the session's, and this knob exists to lower it again if a
     * deployment wants the fail-fast back.
     *
     * At least 1,000 ms: below that every non-trivial probe fails and the
     * planner reports failed sweeps forever.
     */
    val planStatementTimeoutMs: Long = DEFAULT_PLAN_STATEMENT_TIMEOUT_MS,
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
    /**
     * Whether tables with a VARIANT column compact
     * (`HOGLAKE_COMPACTION_VARIANT`, default off), each variant copied as
     * its declared layout (see the ParquetRewriter class doc); off, they
     * are never planned. [variantCompactionCatalogs]
     * (`HOGLAKE_COMPACTION_VARIANT_CATALOGS`) narrows it to those catalogs
     * by name, empty meaning every catalog.
     */
    val variantCompaction: Boolean = false,
    val variantCompactionCatalogs: Set<String> = emptySet(),
) {
    /** Whether [catalog]'s variant tables compact: see [variantCompaction]. */
    fun variantCompactionFor(catalog: String): Boolean =
        variantCompaction && (variantCompactionCatalogs.isEmpty() || catalog in variantCompactionCatalogs)

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
        require(bucketClaimTtlSeconds >= 1) {
            "HOGLAKE_COMPACTION_BUCKET_CLAIM_TTL_SECONDS must be at least 1, got $bucketClaimTtlSeconds"
        }
        require(planStatementTimeoutMs >= 1_000) {
            "HOGLAKE_COMPACTION_PLAN_STATEMENT_TIMEOUT_MS must be at least 1000, got $planStatementTimeoutMs"
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
        // 0 is legal here (the read off); the ceiling is maxCandidates'
        // for maxCandidates' reason — it refuses a typo that would put
        // the discovery read back over the plan statement timeout.
        require(newBucketWindow in 0..MAX_MAX_CANDIDATES) {
            "HOGLAKE_COMPACTION_NEW_BUCKET_WINDOW must be in 0..$MAX_MAX_CANDIDATES, got " +
                "$newBucketWindow: it is a per-sweep read of that many files' partition values " +
                "inside the plan's statement timeout"
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
     * for list lengths the catalog cannot know. A VARIANT is one catalog
     * node but a Parquet group of up to thousands ([VariantLayout.shape]),
     * so it counts as its layout's nodes, and as nested when that layout
     * holds a list.
     *
     * [variantLayouts], by field id, is the subtree a rewrite actually
     * writes a variant column in where that is not the declared one — the
     * layout a group's inputs share (`VariantLayoutGroups`). A row of
     * files written under a wider declaration since cleared or narrowed
     * holds that wider subtree, and a chunk sized by the declaration would
     * hold too many of them for the heap.
     */
    fun spillChunkRows(
        columns: List<Column>,
        variantLayouts: Map<Long, GroupType> = emptyMap(),
    ): Long = spillChunkRowsOf(columns) { column -> variantLayouts[column.fieldId]?.let { VariantLayout.shape(it) } }

    /**
     * [spillChunkRows] for a part whose layouts the planner holds only as
     * remembered SHAPES ([VariantLayoutMemory.Shape], node count and
     * nestedness per deviating column), not as groups: what lets the
     * planner refuse a known-over-budget layout in metadata, before another
     * footer probe.
     */
    internal fun spillChunkRowsOfShapes(
        columns: List<Column>,
        shapes: Map<Long, VariantLayoutMemory.Shape>,
    ): Long = spillChunkRowsOf(columns) { column -> shapes[column.fieldId]?.let { it.nodes to it.nested } }

    private fun spillChunkRowsOf(
        columns: List<Column>,
        shapeOf: (Column) -> Pair<Int, Boolean>?,
    ): Long {
        val variants = columns.allNodes().filter { it.def.type == ColType.VARIANT }
        val layouts = variants.map { column -> shapeOf(column) ?: VariantLayout.shape(column) ?: (1 to false) }
        // + the _hog_row_id carrier, and each variant's nodes beyond the one counted
        val nodes = columns.allNodes().size + 1L + layouts.sumOf { (n, _) -> n - 1L }
        val perRow = SORTED_HEAP_BYTES_PER_NODE * nodes
        val ceiling = sortedHeapBytesPerGroup / perRow
        val nested = columns.allNodes().any { it.def.type.isNested } || layouts.any { (_, list) -> list }
        return maxOf(1L, if (nested) ceiling / nestedSortExpansion else ceiling)
    }

    /** [sortSpill] from remembered shapes; see [spillChunkRowsOfShapes]. */
    internal fun sortSpillOfShapes(
        columns: List<Column>,
        shapes: Map<Long, VariantLayoutMemory.Shape>,
    ): SortSpill = sortSpill(columns).copy(chunkRows = spillChunkRowsOfShapes(columns, shapes))

    /**
     * The bounds a sorted rewrite of a table shaped [columns] runs under:
     * [spillChunkRows], [sortedHeapBytesPerGroup] for the merge,
     * [spillBytes] and [spillDir]. One function, called by the planner's
     * metadata refusals and by the rewrite alike, so the two cannot
     * disagree about a group. [variantLayouts] as [spillChunkRows] takes
     * it: a part of a variant table's group is bounded by ITS layout's
     * shape, re-admitted against these bounds before its rewrite.
     */
    fun sortSpill(
        columns: List<Column>,
        variantLayouts: Map<Long, GroupType> = emptyMap(),
    ): SortSpill =
        SortSpill(
            chunkRows = spillChunkRows(columns, variantLayouts),
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

        /** See [bucketClaimTtlSeconds]: ten minutes between refreshes. */
        const val DEFAULT_BUCKET_CLAIM_TTL_SECONDS = 600L

        /** See [planStatementTimeoutMs]: the session's own 60 s bound. */
        const val DEFAULT_PLAN_STATEMENT_TIMEOUT_MS = 60_000L

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

        /** See [newBucketWindow]: ~3 minutes of gigahog-prod-us appends. */
        const val DEFAULT_NEW_BUCKET_WINDOW = 10_000

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
     * Groups of a table with the object-prefix layout refused in
     * METADATA because their prefix cannot be named (no spec, no single
     * identity key on the field, a missing or non-canonical value).
     * Uncharged, like [spillRefusedGroups]; see `CompactionResult`.
     */
    val objectPrefixUnresolvableGroups: Long = 0,
    /**
     * Groups this plan formed and then dropped because another
     * maintainer holds a LIVE claim over one or more of their input
     * files (`hog_compaction_claim`, V15).
     *
     * Not a conflict and not a failure — the other replica is rewriting
     * those files right now, and everything this one would spend on them
     * would be thrown away at its own commit. Counted so the value is
     * visible: on a two-replica fleet a steady nonzero here is the
     * feature working. So, since bucket claims, is a steady ZERO here
     * with a nonzero [bucketsSkippedClaimed] — the sibling's claims were
     * found before the probe rather than after it. Both zero with two
     * replicas running means claims are off or the planners are seeing
     * different candidate sets.
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
    /**
     * Buckets this plan did NOT probe because another run holds a live
     * BUCKET claim on them (`CompactionClaimRepo.bucketKey`): it planned
     * the bucket, claimed every group the bucket packed into, and has
     * not settled them yet.
     *
     * The pre-probe counterpart of [claimedGroups]. That one is counted
     * after the probe, so every unit of it is a bucket's candidate rows
     * read and thrown away; this one is counted instead of the probe.
     * On a fleet of N maintenance pods sharing a catalog a nonzero value
     * here, with [claimedGroups] near zero, is the planners dividing the
     * buckets between them rather than each re-reading its
     * predecessor's. Not in [bucketsConsidered], which counts probes.
     */
    val bucketsSkippedClaimed: Long = 0,
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
 * the path was never registered). A group whose rewrite produced ZERO
 * rows (every input fully deleted) commits as a RETIREMENT — inputs and
 * DVs end-snapshotted, no output registered — and leaves its ticket
 * undrained the same way, so the drain disposes of the empty object.
 * Because cleanup may legally drain the
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
    store: ObjectStore,
    private val defaults: CompactionConfig,
    /**
     * This process's STABLE identity among the maintenance pods, which
     * seeds where each table's bucket loop starts
     * ([initialCursorOffset]). `App` passes the pod name
     * ([defaultPlannerIdentity]); a test passes two different strings to
     * get two pods. Not `HOGLAKE_INSTANCE_NAME`, which names the
     * DEPLOYMENT for the webui and is the same on every pod.
     *
     * NULL — the default — starts every table's first sweep at the head
     * of the order, the pre-seed behaviour. The default is null rather
     * than the pod name so that a test's plan does not depend on the
     * HOSTNAME of the machine it runs on: tests that pin which bucket a
     * fresh planner probes first would otherwise pass on one runner and
     * fail on the next. The production construction is the one place
     * that must pass it, and does.
     */
    private val plannerIdentity: String? = null,
) : CompactionBackend {
    private val log = KotlinLogging.logger {}

    /** The run ledger; records after the sweep resolves, never inside it. */
    private val runStore = MaintenanceRunStore(jdbi)
    private val executor = CompactionExecutor(PostgresStorage(), store)
    internal val variantLayouts get() = executor.variantLayouts

    /** How one group's execution+commit resolved (the sweep's accounting unit). */
    internal sealed class GroupOutcome {
        data class Committed(val snapshotId: Long, val bytesOut: Long, val dataFileId: Long) : GroupOutcome()

        /**
         * Every input was dead — the rewrite read each one through its
         * live DV and wrote ZERO rows — so the inputs and their DVs were
         * end-snapshotted at [snapshotId] and NO output was registered.
         * The empty object the rewrite uploaded is left to the cleanup
         * drain (see [commitGroup]).
         */
        data class Retired(val snapshotId: Long) : GroupOutcome()

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
     * No transaction remains open across packing, and no lock is held
     * across it either (the planning lock covers only the claim
     * transaction; see `planAndClaim`). The test makes packing
     * take longer than the session's idle bound — hence a seam rather
     * than a sleep in production code. Default is a no-op, called
     * exactly once per table plan, and nothing but a test ever sets it.
     */
    internal var beforePacking: () -> Unit
        get() = executor.beforePacking
        set(value) {
            executor.beforePacking = value
        }

    /**
     * A hook [reserve] calls after the plan is formed and BEFORE it
     * takes the catalog planning lock — the window in which a sibling can
     * plan and claim the same files, which the in-lock claim re-read
     * exists for. Default is a no-op; nothing but a test sets it.
     */
    internal var beforeClaims: () -> Unit = {}

    /**
     * A hook [reserve] calls inside its claim transaction with the
     * planning lock held, so a test can observe the lock from another
     * session or make the claim transaction fail. Default is a no-op;
     * nothing but a test sets it.
     */
    internal var insideClaims: (Handle) -> Unit = {}

    /**
     * A hook [reserve] calls as the LAST statement of its claim
     * transaction — every claim inserted, nothing committed — so a test
     * can pin that the planning lock outlives the commit. Default is a
     * no-op; nothing but a test sets it.
     */
    internal var beforeClaimCommit: (Handle) -> Unit = {}

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
     * The property it exists for is [CompactionExecutor.executeGroup]'s failure isolation,
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
     * A hook [CompactionExecutor.compactGroup] calls with the [ParquetRewriter.Input]s it
     * built, just before the rewrite — so a test can check what each
     * input reads THROUGH (the sortedness pre-pass's key source and its
     * readahead), which no outcome of the rewrite reveals: a key source
     * at the merge's 8 MiB readahead is correct and only costs fetches.
     * Default is a no-op, called once per rewrite, and nothing but a test
     * ever sets it.
     */
    internal var beforeRewrite: (List<ParquetRewriter.Input>) -> Unit
        get() = executor.beforeRewrite
        set(value) {
            executor.beforeRewrite = value
        }

    /**
     * A hook [CompactionExecutor.layoutParts] calls with what the variant-layout probe read of
     * a group's inputs, before they are partitioned: a test's view of the
     * probe's own memory (that equal layouts are one interned tree).
     * Default is a no-op; nothing but a test sets it.
     */
    internal var afterProbe: (List<VariantLayoutGroups.Probed>) -> Unit
        get() = executor.afterProbe
        set(value) {
            executor.afterProbe = value
        }

    /**
     * The smallest input row group the rewrite appends byte for byte
     * ([ParquetRewriter.APPEND_MIN_ROW_GROUP_BYTES], a constant, not a
     * knob). A var only so an integration test can append kilobyte
     * fixtures; nothing but a test ever sets it.
     */
    internal var appendFloorBytes: Long
        get() = executor.appendFloorBytes
        set(value) {
            executor.appendFloorBytes = value
        }

    /** Public metadata-only planning for one table (also the test surface). */
    fun planTable(
        catalog: String,
        namespace: String,
        table: String,
        cfg: CompactionConfig = defaults,
    ): CompactionPlan = executor.planTable(catalog, namespace, table, cfg)

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
        // The OBJECT-PREFIX LAYOUT, on this same snapshot: the field, its
        // top-level column, and its identity key_index in every spec of
        // the table (one PK-range read of the spec ids, then
        // `SpecRepo.fields` per spec; a table has as many specs as it has
        // had spec changes). Option-off tables read only the layout probe.
        val prefixFieldId = LayoutRepo.layoutAt(h, cat.catalogId, t.tableId, cat.headSnapshotId)
        val prefixColumn = prefixFieldId?.let { id -> columns.firstOrNull { it.fieldId == id } }
        val prefixKeyIndex =
            prefixFieldId?.let { id ->
                h.createQuery("SELECT spec_id FROM hog_partition_spec WHERE catalog_id = :c AND table_id = :t")
                    .bind("c", cat.catalogId)
                    .bind("t", t.tableId)
                    .mapTo(Long::class.java)
                    .list()
                    .associateWith { specId ->
                        ObjectPrefix.keyIndex(SpecRepo.fields(h, cat.catalogId, t.tableId, specId), id)
                    }
            }
        return TableContext(
            catalogId = cat.catalogId,
            catalog = cat.name,
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
            variantCompaction = cfg.variantCompactionFor(catalog),
            objectPrefixFieldId = prefixFieldId,
            objectPrefix =
                prefixColumn
                    ?.takeIf { it.def.type in ObjectPrefix.ALLOWED_TYPES }
                    ?.let { ObjectPrefixLayout(it.fieldId, it.def.name, it.def.type) },
            prefixKeyIndex = prefixKeyIndex.orEmpty(),
            containment = ns.containment,
        )
    }

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
     * # ONE STATEMENT, and the LIMIT is on the SCAN
     *
     * Every fetch here is [candidateSql]. The tuple-less and no-sample
     * fetches are an ordered index scan of
     * `hog_data_file_maintenance_size_scan (catalog_id, table_id,
     * file_size_bytes, data_file_id)` (V10), smallest first, table-scoped,
     * with the `LIMIT` on that scan — so the read STOPS at the cap instead
     * of materialising the table and sorting it. A bucket WITH a tuple is
     * driven from its own files: the tuple's ids from V23's index, the
     * manifest by key from them, and the `LIMIT` on that.
     *
     * Two earlier versions got the arm'd case wrong in opposite
     * directions. The first drove from an `INTERSECT` of V23 index arms
     * with `ORDER BY file_size_bytes` and the `LIMIT` on top, which
     * bounds the ROWS RETURNED and not the WORK: the sort needed every id
     * of the bucket probed and sorted before the `LIMIT` applied (~100k
     * primary-key probes and a ~50 MB sort to return 8,192 rows on a
     * ~100k-file bucket). The second dropped the sort but led on the
     * manifest with one `EXISTS` per key, which left Postgres free to
     * walk the table's rows until the `LIMIT` filled — O(table) per
     * probe, and the bucket loop made that minutes per plan on
     * gigahog-prod-us. Both halves are kept now: no inner sort, and an
     * id-set InitPlan that PG 18 always costs as a handful of primary-key
     * descents, so no statistic makes the walk look cheaper. See
     * [candidateSql] for why that holds and where it costs, which also
     * states what the arms still cost: they are CATALOG-scoped
     * (`hog_file_partition_value` carries no `table_id`), so a probe is
     * bounded by the bucket across the catalog.
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
     *    statement with no partition arms and the aggregated tuple
     *    projected per returned row, capped at
     *    [CompactionConfig.maxCandidates];
     *  - **a published generation with no qualifying bucket**, in the
     *    sample OR among the buckets appended to since it began
     *    ([newBuckets]): THERE IS NO WORK. Return an empty fetch. The
     *    first version of this change conflated this with the state
     *    above and ran the 50,000-row whole-table statement, every
     *    sweep, forever, for a table whose debt is real but spread
     *    thinner than the file minimum — which is the one statement in
     *    this change that still carries the per-row `array_agg` the
     *    incident was about;
     *  - **a published generation with buckets**: the bucket loop, over
     *    the sample's buckets merged with [newBuckets]' in one value
     *    order ([mergeBuckets]). Before the merge existed the planner
     *    saw only what the generation saw, which on 2026-10-21 was
     *    five days stale on millpond's `events_raw` (1,794 of 5,036
     *    partitions) and blind to every partition written since.
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
        // is decided per group in [CompactionExecutor.pack], in metadata.
        val budget = cfg.targetBytes

        fun empty(
            available: Long = 0,
            truncated: Boolean = false,
        ) = CandidateFetch(ctx, budget, emptyMap(), 0, 0, available, truncated)
        if (!executor.canPlan(ctx)) return empty()

        // Does a published generation exist AT ALL? That is a different
        // question from "does it credit this table with a compactable
        // bucket", and the two have opposite answers.
        if (TierTotalsRepo.publishedGeneration(h, ctx.catalogId) == null) {
            return fetchWholeTable(h, ctx, budget, cfg)
        }
        // The sample, plus every bucket written to since the sample's
        // scan began. See [newBuckets] for why the sample alone is not
        // enough, and [mergeBuckets] for the order.
        val sampled =
            mergeBuckets(
                sampledBuckets(h, ctx, cfg),
                TierTotalsRepo.publishedSampleSnapshot(h, ctx.catalogId)
                    ?.let { since -> newBuckets(h, ctx, budget, cfg, since) }
                    .orEmpty(),
            )
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
        val held = liveBucketClaims(h, ctx, cfg)
        val byBucket = LinkedHashMap<Bucket, List<CompactionCandidate>>()
        val markable = HashSet<Bucket>()
        var fetched = 0L
        var considered = 0L
        var skipped = 0L
        var truncated = false
        for (bucket in rotated(ctx, sampled)) {
            if (fetched >= candidateBudget || considered >= bucketLimit) {
                truncated = true
                break
            }
            // THE PRE-PROBE CLAIM FILTER. Another run planned this bucket
            // and claimed every group it packed into; probing it would
            // read its candidate rows only for phase (c) to drop each of
            // those groups as `claimed_elsewhere`. Not charged to
            // `considered` or the budget, because no statement ran.
            if (held.isNotEmpty() &&
                CompactionClaimRepo.bucketKey(bucket.bucket.specId, bucket.bucket.values) in held
            ) {
                skipped++
                continue
            }
            // `room + 1` so the truncation flag is EXACT: a bucket that
            // holds exactly its remaining room was truncated by nothing,
            // and a bucket that holds one more was. Trimmed below.
            val room = (candidateBudget - fetched).toInt()
            val rows = bucketCandidates(h, ctx, budget, cfg, bucket.bucket, room + 1)
            considered++
            if (rows.size > room) truncated = true else markable += bucket.bucket
            val taken = if (rows.size > room) rows.subList(0, room) else rows
            fetched += taken.size
            if (taken.isNotEmpty()) byBucket[bucket.bucket] = taken
            rememberCursor(ctx, bucket)
        }
        return CandidateFetch(
            ctx = ctx,
            budget = budget,
            byBucket = byBucket,
            candidatesFetched = fetched,
            bucketsConsidered = considered,
            bucketsAvailable = sampled.size.toLong(),
            truncated = truncated,
            bucketsSkippedClaimed = skipped,
            markable = markable,
        )
    }

    /**
     * The live bucket claims on this table, read on phase (a)'s own
     * snapshot, for [fetchCandidates]' pre-probe filter.
     *
     * # What phase (a)'s snapshot can miss, and what that costs
     *
     * Planning takes no lock (#418; `planAndClaim` has the argument), so
     * a sibling may commit a marker after this transaction's snapshot
     * was taken, and this planner then probes a bucket it could have
     * skipped. The cost is that probe: the groups it packs overlap the
     * sibling's, and phase (c) or `planAndClaim`'s in-lock re-read drops
     * them as `claimed_elsewhere` before any claim is inserted.
     * Never a duplicate rewrite. Until #418 the planning lock was held
     * from before this snapshot, which made the read exact and made one
     * slow probe every sibling's lock wait.
     *
     * # Fail open, behind a savepoint
     *
     * An optimization, like [withoutClaimedGroups]' read: a failing read
     * plans the table as if no bucket were claimed, which is the
     * pre-filter behaviour and still correct (phase (c) and the claim
     * insert drop the overlapping groups). Unlike that read this one is
     * INSIDE the planning transaction, and a failed statement poisons a
     * Postgres transaction, so it runs behind a savepoint and rewinds
     * to it on failure rather than taking the candidate fetch down with
     * it.
     *
     * Skipped when claims are off: nothing in this configuration writes
     * a marker, and a sibling configured differently is a mixed fleet the
     * README tells operators not to run.
     */
    private fun liveBucketClaims(
        h: Handle,
        ctx: TableContext,
        cfg: CompactionConfig,
    ): Set<String> {
        if (!cfg.claimsEnabled) return emptySet()
        h.savepoint(BUCKET_CLAIM_SAVEPOINT)
        return try {
            CompactionClaimRepo.liveBucketKeys(h, ctx.catalogId, ctx.tableId).also {
                h.releaseSavepoint(BUCKET_CLAIM_SAVEPOINT)
            }
        } catch (e: Exception) {
            h.rollbackToSavepoint(BUCKET_CLAIM_SAVEPOINT)
            log.warn(e) {
                "compaction bucket-claim read failed for ${ctx.namespace}.${ctx.table}; " +
                    "probing every bucket (a claim is an optimization)"
            }
            emptySet()
        }
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
     * # The order USED to keep the fetch bounded too
     *
     * While [candidateSql] led on V10's size index, a probe was cheap
     * only if the bucket's files were among the table's SMALLEST, and
     * bytes-per-file ascending was what kept the expensive probes
     * (a bucket of near-target files, with smaller files elsewhere)
     * unreachable while smaller files existed. The probe is driven from
     * the bucket's own ids now, so its cost no longer depends on where
     * the bucket sits in the size order, and this ordering is a VALUE
     * ordering only. `CompactionCandidateFetchPlanIntegrationTest` still
     * pins it.
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
     * This table's buckets that hold small live files APPENDED SINCE the
     * published sample's scan began — the buckets the sample cannot
     * credit because it never saw their files.
     *
     * # Why the sample alone is not enough
     *
     * [sampledBuckets] is a generation old by construction: on
     * gigahog-prod-us a scan takes ~1 h for ~7.6M rows, and the planner
     * reads the generation published BEFORE the one in flight, so the
     * newest bucket it can name is one or two scans old. On 2026-10-21 it
     * was five days old, because the sampler had stopped publishing
     * (its scan outlasted retention and restarted forever — fixed in
     * `MaintenanceSummarySampler`, which now re-anchors): millpond's
     * `events_raw` had 5,036 partitions and the planner saw the 1,794 of
     * the 2026-10-16 generation. Every day partition written since — the
     * hottest — was invisible to compaction, and its small-file backlog
     * grew untouched. A working sampler shrinks that window to a scan
     * and a refresh, and does not close it: a new day's partition, or a
     * new team's under a `(team, day)` spec, is invisible until the
     * first generation that STARTED after it. And a re-anchored scan
     * misses appends whose key sorted below its cursor, which in size
     * order is most new ingest files.
     *
     * # The lower bound is the sample's START snapshot
     *
     * `Sample.snapshotId` ([TierTotalsRepo.publishedSampleSnapshot]):
     * the snapshot the scan captured when it began, which a re-anchor
     * never moves. Every live file the sample may have missed began
     * after it. A file that began after it and that the sample DID see
     * (above the cursor of a re-anchored scan) is counted in both
     * lists, so [mergeBuckets]' summed `small_count`/`small_bytes`/
     * `selected` for that bucket are INFLATED by it. Those measures only
     * ORDER the bucket loop — no group is sized from them, the probe
     * reads the bucket's real files — so the cost is a bucket ranked
     * slightly off, never a wrong plan.
     *
     * Not `hog_maintenance_summary.published_snapshot` (V27): it holds
     * the same value when it is set, but a publish by a pre-V27 build
     * moves `published_generation` without stamping it, and the sample
     * is written in the same statement as `published_generation`, so it
     * is never another generation's.
     *
     * # The access path and THE BOUND
     *
     * `(catalog_id, table_id, begin_snapshot)`, and WHICH index carrying
     * it matters. V3's `hog_data_file_changefeed` is that key over every
     * row; `hog_data_file_live` is the same key partial on `end_snapshot
     * IS NULL`. The inner `LIMIT` bounds the rows the walk RETURNS, not
     * the rows it VISITS: over the changefeed index the walk reads and
     * filters every ENDED row since `since` on the way to its live ones,
     * and on a stale sample — hours of appends, most of them compacted
     * away since — that is the whole range, inside the plan's 15 s
     * bound. Over the live index an ended row is not an entry, so the
     * walk visits only live rows and the `LIMIT` bounds what it visits.
     *
     * The walk is therefore the LIVE index's, and Postgres chooses it
     * rather than being told: for one range, the partial index's
     * estimated cost is never above the full one's (fewer index pages,
     * and no heap fetch for the estimated ended fraction it would then
     * filter), so it wins whenever the statistics show ANY ended rows,
     * which a manifest under expiry lag always has. With none (a fixture
     * of only live rows) the two tie, the planner may take either, and
     * the two then cost the same. Measured on
     * `CompactionCandidateFetchPlanIntegrationTest`'s fixture plus
     * 200,000 ended rows since `since`, NEWER than 1,000 live ones:
     * `Index Scan Backward using hog_data_file_live`, 1,000 rows, 0
     * removed, ~30-40 manifest-side buffers, 1.3 ms. The same statement
     * with the live index dropped walks the changefeed index: `Rows
     * Removed by Filter: 200000`, 16,679 buffers, 30.6 ms, growing with
     * the dead rows. That test PINS the live index on the dead-row
     * shape, so a planner change that flips it is a red test, not an
     * outage. On the all-live shape the changefeed walk measured 2,000
     * rows, 63 manifest-side buffers (against 5,307 heap pages for one
     * pass of the manifest) plus 4 per file for the partition-value
     * descent.
     *
     * A lower bound on `begin_snapshot` (the newest K snapshots) was the
     * alternative and was not taken: it bounds the dead rows visited by
     * K snapshots' appends rather than removing them, and it adds a
     * second horizon — a tuple older than K snapshots and unseen by the
     * sample would wait for the next generation even with room in the
     * window.
     *
     * Walked NEWEST FIRST and capped at [CompactionConfig.newBucketWindow]
     * files (`HOGLAKE_COMPACTION_NEW_BUCKET_WINDOW`, default 10,000), with
     * the per-row `array_agg` in the select list so it is evaluated per
     * row the `LIMIT` lets through, never per row of the range. Its own
     * knob and NOT [CompactionConfig.maxCandidates], because this read
     * DISCOVERS tuples, it does not fetch candidates — and because on a
     * hot table THE CAP IS HIT ON EVERY SWEEP in normal operation, not
     * only when the sampler is behind: with a healthy sampler `since` is
     * one scan plus one refresh old, ~1-2 h and ~200-400k appends on
     * prod-us's `events_raw`. So the window is a per-sweep cost, paid
     * inside [CompactionConfig.planStatementTimeoutMs] (then 15 s) on every plan of the table, and
     * a timeout here fails the whole table's plan. At the 50,000 that
     * borrowing `maxCandidates` meant, ~200k buffers per sweep; at
     * 10,000, ~40k, of which the cold reads are far fewer than the hits,
     * because one commit's files have adjacent `data_file_id`s and so
     * share `hog_file_partition_value` leaf pages. 10,000 is ~3 minutes
     * of prod-us appends: a tuple being written to now is inside it,
     * a tuple written once and then never again since may fall outside
     * it until the next generation names it. That is the stated cost of
     * the cap. 0 turns the read off. The grouped output is capped at
     * [MAX_NEW_BUCKETS], independently of the sample's cap (see [mergeBuckets]).
     *
     * WHY NOT CHEAPER THAN ONE DESCENT PER FILE. `hog_file_partition_value`
     * carries neither `table_id` nor `begin_snapshot`, so neither its
     * primary key nor V23's `hog_file_partition_value_lookup (catalog_id,
     * key_index, value, data_file_id)` can be driven by "this table's
     * files since S"; the only way from the window to its tuples is
     * through the window's ids. A distinct-tuples read would need
     * `table_id` and `begin_snapshot` in a partition-value index, a
     * schema change on the second-hottest write in the system, and is not
     * this change.
     *
     * Only files under the target, the planner's own candidate rule, and
     * only buckets with at least two of them (the packer's floor, which
     * [sampledBuckets] applies to `selected`). The two floors leave a
     * gap, stated rather than closed: a tuple the sample saw ONE file of
     * (`selected` 0, so [sampledBuckets] drops it) that has since gained
     * ONE more (one new file, under this read's floor) is in NEITHER
     * list, although together they make a group of two. It is picked up
     * by the next generation, or by this read once a second new file
     * lands. Closing it would mean un-flooring one of the two reads.
     *
     * The tuple of a file with no partition-value rows is NULL — the
     * tuple-less bucket, exactly as the sampler and [fetchWholeTable]
     * derive it — so an unpartitioned table's one bucket and a
     * mid-life-partitioned table's pre-spec vintage take the same
     * tuple-less probe in [bucketCandidates] as before.
     */
    private fun newBuckets(
        h: Handle,
        ctx: TableContext,
        budget: Long,
        cfg: CompactionConfig,
        since: Long,
    ): List<SampledBucket> {
        if (cfg.newBucketWindow == 0) return emptyList()
        return h.createQuery(NEW_BUCKETS_SQL)
            .bind("catalogId", ctx.catalogId)
            .bind("tableId", ctx.tableId)
            .bind("since", since)
            .bind("targetBytes", budget)
            .bind("fileCap", cfg.newBucketWindow)
            .bind("minFiles", 2L)
            .bind("bucketCap", MAX_NEW_BUCKETS)
            // Custom plans only: `since` is near the top of the
            // begin_snapshot range, and only a plan built with the value
            // knows the range is narrow.
            .addCustomizer(Pg.CUSTOM_PLANS_ONLY)
            .map { rs, _ ->
                val count = rs.getLong("small_count")
                SampledBucket(
                    Bucket(
                        specId = rs.getObject("spec_id")?.let { (it as Number).toLong() },
                        values = (rs.getArray("partition_values")?.array as? Array<*>)?.map { it as String? },
                    ),
                    // An UPPER BOUND on what the packer would take — the
                    // sampler's mirror of the packer has not run over
                    // these files — and used for ordering only.
                    selected = count,
                    smallCount = count,
                    smallBytes = rs.getLong("small_bytes"),
                )
            }
            .list()
    }

    /**
     * The sample's buckets and [newBuckets]' as ONE list, in the
     * sample's order.
     *
     * # The merge order: one value order, not new-first
     *
     * A bucket present in both is ONE entry whose measures are the sum
     * (the files the generation saw plus the files since). The merged
     * list is then sorted by the key [sampledBuckets] sorts by —
     * bytes-per-file ascending, then `selected` descending, then
     * `small_count` descending — with a STABLE sort, so buckets only the
     * sample knows keep the exact relative order the SQL gave them.
     *
     * Not new-first, deliberately. New-first would put a fresh day's
     * bucket — prod's ~100k files of up to 200 MB, the four-orders-
     * of-magnitude worse rewrite [sampledBuckets]' KDoc measures — ahead
     * of every stray bucket on every sweep, which is the ordering that KDoc
     * records as having touched one bucket in ten sweeps. Fairness is not
     * the order's job anyway: [rotated]'s cursor reaches every bucket
     * of the merged list within `ceil(available / considered)` sweeps,
     * wherever the order puts it, and a new bucket of small files is
     * ranked AHEAD of the sample by that same order.
     *
     * # Two caps, INDEPENDENT, and the union is not re-capped
     *
     * The sample's list is capped at [MAX_SAMPLED_BUCKETS] by
     * [sampledBuckets], the new-bucket list at [MAX_NEW_BUCKETS] by
     * [newBuckets] (and in practice by its window: at most
     * `newBucketWindow / 2` tuples clear its floor of two), and the union
     * keeps EVERY entry of both. It used to be cut back to
     * [MAX_SAMPLED_BUCKETS] after the sort, and since new buckets of
     * small files sort AHEAD of most of the sample, a full sample plus
     * any new bucket lost its value-last sampled buckets from the list
     * altogether — not deferred by the rotation, never offered. With the
     * caps independent, one list cannot starve the other; the union is at
     * most `MAX_SAMPLED_BUCKETS + MAX_NEW_BUCKETS` small objects in heap,
     * and the bucket loop's own `bucketLimit` is what bounds the probes.
     */
    private fun mergeBuckets(
        sampled: List<SampledBucket>,
        fresh: List<SampledBucket>,
    ): List<SampledBucket> {
        if (fresh.isEmpty()) return sampled
        val merged = LinkedHashMap<Bucket, SampledBucket>()
        for (b in sampled) merged[b.bucket] = b
        for (b in fresh) {
            merged.merge(b.bucket, b) { old, new ->
                old.copy(
                    selected = old.selected + new.selected,
                    smallCount = old.smallCount + new.smallCount,
                    smallBytes = old.smallBytes + new.smallBytes,
                )
            }
        }
        return merged.values.sortedWith(bucketOrder)
    }

    /**
     * [sampledBuckets]' SQL `ORDER BY`, in Kotlin: bytes per file
     * ascending (`GREATEST(small_count, 1)` as the divisor), `selected`
     * descending, `small_count` descending. Shared by [mergeBuckets] and
     * [rotated]'s resume, so the order the list is in and the order the
     * cursor resumes by cannot drift.
     */
    private val bucketOrder: Comparator<SampledBucket> =
        compareBy<SampledBucket> { it.smallBytes.toDouble() / maxOf(it.smallCount, 1L) }
            .thenByDescending { it.selected }
            .thenByDescending { it.smallCount }

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
     * # In memory, per pod, and SEEDED per pod
     *
     * Keyed by (catalog, table) — `table_id` is scoped per catalog, so
     * a table-only key would let one catalog's sweep move another's
     * cursor (the bug `lastBudgetRefusal`'s key already records). It is
     * lost on restart and NOT shared between replicas. Persisting it
     * would mean a write per plan on the maintenance path for a
     * tie-break, so it stays in memory.
     *
     * What being per pod used to cost is the 2026-10-21 herd on
     * gigahog-prod-us. Every pod's cursor started EMPTY, so every pod's
     * first sweep started at the head of the same list, and every pod
     * then advanced by the same `considered` — so nine pods walked the
     * same buckets in lockstep for as long as they ran: six sweeps
     * inside two seconds with identical `buckets_considered 123 /
     * candidates_fetched 50,029`, and `claimed_elsewhere` 0, 64, 128,
     * 192, 256, 320 as each pod re-probed the buckets its predecessors
     * had already claimed. The group claims made the EXECUTION disjoint;
     * nothing made the PLANNING disjoint.
     *
     * Two things do now. An empty cursor starts at
     * [initialCursorOffset] — a position hashed from this pod's
     * [plannerIdentity] and the table — so pods begin on different
     * slices and then advance side by side. And a bucket another run has
     * claimed in full is skipped before it is probed
     * ([fetchCandidates]' pre-probe filter, `CompactionClaimRepo`'s
     * bucket claims), which covers the slices that the hash happens to
     * overlap. The cursor is still fairness, never correctness.
     *
     * The cursor records the last bucket PROBED, not the last one
     * visited: a bucket the pre-probe filter skips does not move it. So a
     * sweep whose final visits were skips resumes, next time, just after
     * its last probe and walks over those (by then usually released)
     * buckets again — deliberately, since a skipped bucket was not
     * looked at and should get its turn.
     *
     * # The cursor carries the bucket's SORT KEY, not only its identity
     *
     * The bucket the last sweep stopped on can be GONE from this sweep's
     * list: compacted below the floor, the generation moved, or — since
     * [newBuckets] — a new bucket that this sweep's window no longer
     * reaches or that the next generation now names under a different
     * measure. New-bucket membership changes every sweep on a churny
     * spec, so "gone" is routine, not rare. Restarting from the top when
     * it happens is the fixed point above by another route: a sweep that
     * ends on a transient bucket sends the next one back to the head of
     * the order, and the tail is never reached.
     *
     * So the cursor also remembers the bucket's position in the ORDER —
     * [bucketOrder]'s key, `(bytes per file, selected, small_count)` —
     * and a sweep whose bucket is gone resumes at the first bucket that
     * sorts at or after that key. Ties can repeat a bucket, which costs a
     * probe; they cannot send the loop back to the top.
     */
    private val bucketCursor = java.util.concurrent.ConcurrentHashMap<Pair<Long, Long>, SampledBucket>()

    /**
     * [sampled] rotated so this sweep starts after the bucket the last
     * one stopped on — the order is preserved, only the entry point
     * moves. When that bucket is gone, at the first bucket sorting at or
     * after where it sat (see [bucketCursor]). When this pod has no
     * cursor for the table yet, at [initialCursorOffset]'s position for
     * this pod.
     */
    private fun rotated(
        ctx: TableContext,
        sampled: List<SampledBucket>,
    ): List<SampledBucket> {
        if (sampled.isEmpty()) return sampled
        val last = bucketCursor[ctx.catalogId to ctx.tableId]
        if (last == null) {
            // No sweep of this table yet on this pod: start on THIS
            // pod's slice, not at the head every pod starts at.
            val identity = plannerIdentity ?: return sampled
            val from = initialCursorOffset(identity, ctx.catalogId, ctx.tableId, sampled.size)
            return sampled.subList(from, sampled.size) + sampled.subList(0, from)
        }
        val at = sampled.indexOfFirst { it.bucket == last.bucket }
        val from =
            if (at >= 0) {
                (at + 1) % sampled.size
            } else {
                // Gone: resume where it WOULD sort. None at or after it
                // means it sat past the end, so the next sweep wraps.
                sampled.indexOfFirst { bucketOrder.compare(it, last) >= 0 }.takeIf { it >= 0 } ?: 0
            }
        return sampled.subList(from, sampled.size) + sampled.subList(0, from)
    }

    private fun rememberCursor(
        ctx: TableContext,
        bucket: SampledBucket,
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
     *      WHERE catalog_id = ? AND table_id = ?        -- table scope
     *        AND end_snapshot IS NULL
     *        AND file_size_bytes < ?
     *        [AND data_file_id = ANY (ARRAY(            -- ARMS: the bucket's ids,
     *              SELECT pv.data_file_id                --   V23's index, as ONE
     *                FROM hog_file_partition_value pv    --   InitPlan; keys
     *               WHERE catalog_id = ? AND key_index = ? AND value = ?
     *                 [AND EXISTS (each further key)])))] --   intersected by cost
     *        [AND spec_id IS NOT DISTINCT FROM ?]        -- bucket identity, ARMS only
     *      [ORDER BY file_size_bytes, data_file_id]      -- ONLY when there are no arms
     *      LIMIT ?                                       -- THE SCAN CAP
     * ) f
     * LEFT JOIN hog_delete_file dv …                    -- the planned DV
     * [LEFT JOIN LATERAL (array_agg of the tuple) …]     -- no-sample fallback only
     * [WHERE f.spec_id IS NOT DISTINCT FROM ?]           -- bucket identity, NO arms
     * ORDER BY f.file_size_bytes, f.data_file_id         -- over at most the cap
     * LIMIT ?                                            -- THE RESULT CAP
     * ```
     *
     * The inner `LIMIT` is what makes the read bounded, and **the inner
     * `ORDER BY` is what would stop it being bounded** — so it is there
     * only in the case where it is free.
     *
     * # NO ARMS: an ordered scan of V10's size index
     *
     * The sort key is the index's own order
     * (`hog_data_file_maintenance_size_scan (catalog_id, table_id,
     * file_size_bytes, data_file_id)`), so ordering costs nothing: the
     * scan is an ordered index scan and the `LIMIT` stops it. Measured:
     * 239 buffers for 8,192 rows out of a 140,000-row table. This is the
     * tuple-less bucket and the no-sample fallback, and it is unchanged.
     *
     * # ARMS: driven FROM the bucket, never from the table
     *
     * A bucket's files are not a prefix of the size order — they are
     * interleaved with every other bucket's — so with arms there is no
     * inner `ORDER BY` (an access path that applied the arms could not
     * also produce size order, and Postgres would have to produce the
     * whole bucket before the `LIMIT` saw a row; the first version of
     * this statement did that and spilled a sort to disk). The candidates
     * are an arbitrary `scanLimit` of the bucket rather than its smallest,
     * which is no loss: a bucket is one partition's ingest, so its files
     * are all about one flush size; `CompactionGrouping` sorts by size
     * itself before packing; and it only bites when a bucket holds more
     * than the cap. The outer `ORDER BY` stays, over at most `limit`
     * rows, so the result is deterministic.
     *
     * WHAT THE ARMS USED TO BE, and why that was O(table). One correlated
     * `EXISTS` per key, filtering the manifest scan above. Postgres may
     * rewrite such arms into a semi-join driven from V23's index, or keep
     * driving the MANIFEST and apply them as filters — and the second is
     * a walk of the table's rows until the `LIMIT` fills, which for a
     * bucket smaller than the cap is the whole table. The choice rests
     * on an estimate of how many of the table's rows match, and that
     * estimate is the CATALOG-wide arm size spread uniformly over the
     * manifest, because `hog_file_partition_value` carries no `table_id`:
     * a value shared by every table in the catalog looks common in this
     * one. Measured on `CompactionCandidateFetchPlanIntegrationTest`'s
     * fixture: a hash join whose outer side is a `Seq Scan on
     * hog_data_file`, 57,342 manifest rows read for 8,192 returned. On
     * gigahog-prod-us (2026-10-21, a table of 1.33M small files in >50k
     * buckets of median 66, a catalog of 41M live files and 67M partition
     * values): 1,064,028 sequential scans of `hog_data_file`, plans of
     * 18-37 minutes for 240-480 probes, and 193 of 200 sweeps failing
     * [CompactionConfig.planStatementTimeoutMs] (then 15 s) on one probe. The bucket loop
     * multiplies an O(table) probe by the bucket count.
     *
     * WHAT THEY ARE NOW: the tuple becomes ONE id set, `ARRAY(...)` over
     * V23's index, which Postgres evaluates once as an InitPlan, and the
     * manifest is reached FROM it — `data_file_id = ANY (that array)` as
     * an index condition, normally on the primary key, the `table_id`
     * and size predicates checked per id, and the `LIMIT` stopping that
     * scan at the cap.
     *
     * WHY POSTGRES PICKS THAT, and it is a costing property rather than
     * a missing plan. A manifest-driven plan still exists — with index
     * scans disabled PG 18 plans `Seq Scan on hog_data_file … Filter:
     * data_file_id = ANY ((InitPlan 1).col1)` — but the InitPlan's output
     * is a Param with no statistics, and `estimate_array_length()`
     * returns its constant default of 10 for a Param. So the `= ANY`
     * primary-key scan is always costed as about ten descents (EXPLAIN
     * shows the InitPlan at ~19 rows and the scan at ~3), and no
     * statistic on `value`, however wrong, makes a walk of the table look
     * cheaper. Two consequences, both deliberate costs:
     *
     *  - the planner also cannot prefer a TABLE-scoped path when one
     *    would be cheaper (regimes (a) and (b) below);
     *  - everything above the scan is costed at ~3 rows, which is what
     *    makes the DV join a nested loop at any result size (regime (c)).
     *
     * This is a PG 18 property. A future Postgres that estimates a Param
     * array from its subplan's row estimate restores a statistics-driven
     * choice, and with it the old failure mode; the plan tests pin the
     * shape so that upgrade is noticed rather than inherited.
     *
     * INSIDE THE ARRAY the planner intersects the keys BY COST: it pulls
     * the `EXISTS` up into a semi-join within the InitPlan and picks the
     * driving side itself. The SQL's first key is syntactic only —
     * `arms = [1, 0]` (month first) produces the identical plan to
     * `[0, 1]`. That subquery has no `LIMIT`, so it is costed whole: in a
     * custom plan, with the bound values' statistics, Postgres drives
     * from the SELECTIVE key and probes the common one per id through
     * the same index (measured: 250 team-key entries, 250 probes of an
     * 80,000-entry month key, 957 buffers for a 50-file bucket); where
     * no key is selective it merge-joins both.
     *
     * # THE BOUND, stated rather than implied
     *
     * A probe reads:
     *
     *  - the cheapest intersection of the keys' index entries,
     *    CATALOG-wide (the selective key's entries plus a probe each, or
     *    both keys' entries in a merge join);
     *  - one primary-key visit per id in the CATALOG-WIDE bucket, until
     *    the cap's worth match this table.
     *
     * So it is O(the bucket across the catalog), not O(the table). The
     * catalog-wide part is the cost of the keys being catalog-scoped: a
     * big customer's stray bucket — 50 files on this table, 20,064 across
     * the catalog — costs 13,235 buffers (one sorted `= ANY` scan of the
     * primary key; the replaced statement paid a descent per id, 81,857).
     * Bounding it by the TABLE would need `table_id` in V23's index, a
     * schema change and the real fix for (a) and (b); not this one.
     *
     * WHERE THIS STATEMENT IS WORSE THAN THE ONE IT REPLACED, measured on
     * a reproduction of `CompactionBucketProbePlanIntegrationTest`'s
     * fixture on PG 18:
     *
     *  - (a) A SMALL TABLE WHOSE VALUES A BIG SIBLING SHARES (millpond's
     *    small `day(ts)` tables beside its 7.6M-file one). A one-key
     *    probe for month 682 on a 50-file table: 825 buffers, against
     *    163 for the old statement. The whole catalog-wide key is read
     *    index-only, then the manifest is walked over ids that mostly
     *    belong to the sibling; the old statement walked the small table.
     *  - (b) A ONE-KEY PROBE ON A WIDELY SHARED VALUE, where no key is
     *    selective and the id set is the value's files across the whole
     *    catalog. Over the cap (month 682: 20,000 files on this table,
     *    80,000 across the catalog, cap 8,193): 21,830 buffers against
     *    4,898 with no DVs, of which 16,386 is (c); with a live DV on a
     *    third of the files, 33,137 against 7,524, 27,700 of it (c).
     *    Under the cap (a big team: 250 files here, 100,263 across the
     *    catalog): 13,270 against 13,625 with DVs, 12,925 against 10,992
     *    without — the same order, not cheaper. The direction is MORE
     *    than before, bounded by the value's catalog-wide files;
     *    `CompactionBucketProbePlanIntegrationTest` pins the bound and
     *    records both figures.
     *  - (c) THE DV LEFT JOIN is a nested loop at any result size, 2-3.4
     *    buffers per returned row, because the outer estimate is ~3 rows;
     *    the old statement estimated thousands and used a hash left
     *    join. At the cap it is the dominant term. NOT FENCED, on
     *    purpose: an `OFFSET 0` (or any fence) does not change the row
     *    estimate, which comes from the same Param-array default; a hash
     *    join's alternative is a read of every live DV of the CATALOG,
     *    which on production is not cheaper than 8,192 probes of
     *    `hog_delete_file_snapshot_lookup`; and gigahog's buckets are a
     *    median of 66 files, where the nested loop is ~130-225 buffers.
     *  - (d) MEMORY. `ARRAY(subquery)` accumulates in the per-query
     *    memory context — ~32 bytes per id, doubling growth plus a copy,
     *    then `_bt_preprocess_array_keys` copies and sorts it again — and
     *    none of it is governed by `work_mem`. `scanLimit` bounds nothing
     *    here: the array is built whole before the scan starts. 1M ids is
     *    ~32 MB, 10M ~320 MB, and the hard ceiling is Postgres's 1 GB
     *    array limit at ~134M ids. n is |team ∩ month| across the catalog
     *    for a two-key spec, and for a single-key `day(ts)` spec it is
     *    that day's files across EVERY table in the catalog.
     *
     * The millpond verdict, per catalog: the big table gets better —
     * stray days under the cap cost O(that day's files catalog-wide)
     * instead of a potential O(7.6M) walk, and over-cap fresh days are
     * the same to 4x worse but bounded — and the small siblings get worse
     * by (a). Net better, and the `table_id`-in-index follow-up removes
     * (a) and (b).
     *
     * # NEVER A GENERIC PLAN ([Pg.CUSTOM_PLANS_ONLY])
     *
     * pgjdbc server-prepares a statement on its fifth execution on a
     * connection, and one plan's bucket loop runs this statement hundreds
     * of times on one handle. Postgres may then switch to a GENERIC plan,
     * built without the bound values — which cannot tell a selective
     * key from a common one, and merge-joins both: measured, every
     * month-key entry in the catalog (78,432) read to find a 50-file
     * bucket. The array keeps even that plan driven from V23's index,
     * but the probe is sent unnamed so it is never planned generic at
     * all.
     *
     * `value = ?` or `value IS NULL` per key, chosen by the sampled
     * tuple's own shape. A null partition value is legal (`schema.sql`:
     * "NULL = null partition value"), a btree indexes NULLs, and `IS
     * NULL` is an ordinary range over the index's null entries, while
     * `IS NOT DISTINCT FROM` drives no btree. Only the PREDICATE SHAPE is
     * built from the tuple; every value is bound.
     *
     * The figures above are from `CompactionBucketProbePlanIntegrationTest`
     * (one catalog, 401,000 files: 2,000 buckets x 50 files on the
     * subject table, three neighbour tables sharing its month values and
     * a big team) and `CompactionCandidateFetchPlanIntegrationTest`
     * (production's skewed bucket sizes), PG 18, warm, serial; the
     * assertions are in those tests.
     *
     * # VERIFIED ONLY, for a table with the object-prefix layout
     *
     * [verifiedOnly] adds [VERIFIED_ONLY_SQL] — the per-value mount
     * predicate (`ObjectPrefix`) — to the inner scan, inside the scan
     * cap, and nothing else: an input whose stats are still pending, or
     * that the hydrator quarantined, never reaches an output the mount
     * would then serve. It is a filter on the same scan, so the plan is
     * the option-off plan with one more Filter
     * (`CompactionCandidateFetchPlanIntegrationTest` asserts both). On
     * an arm'd probe the rows it removes are the bucket's unverified
     * files, bounded by the bucket like the rest; on the no-sample
     * fallback's ordered size scan they are the table's unverified small
     * files below the cap's last row — the hydrator's backlog on that
     * table, read only before the catalog's first generation publishes.
     * Off, the text is byte-identical to the statement before the layout
     * existed (`CandidateSqlGoldenTest`).
     */
    internal fun candidateSql(
        arms: List<Int>,
        nullArms: Set<Int>,
        scopeToSpec: Boolean,
        withPartitionValues: Boolean,
        verifiedOnly: Boolean = false,
    ): String {
        fun valuePredicate(keyIndex: Int) =
            if (keyIndex in nullArms) "pv$keyIndex.value IS NULL" else "pv$keyIndex.value = :v$keyIndex"
        // ONE id set for the whole tuple, evaluated ONCE as an InitPlan,
        // and the manifest reached by primary key from it. See the KDoc
        // for why this is an array and not a semi-join. `driver` is only
        // where the SQL starts: the planner intersects the keys by cost.
        val armSql =
            if (arms.isEmpty()) {
                ""
            } else {
                val driver = arms.first()
                val probes =
                    arms.drop(1).joinToString("") { keyIndex ->
                        """
                                AND EXISTS (SELECT 1 FROM hog_file_partition_value pv$keyIndex
                                             WHERE pv$keyIndex.catalog_id = pv$driver.catalog_id
                                               AND pv$keyIndex.data_file_id = pv$driver.data_file_id
                                               AND pv$keyIndex.key_index = :k$keyIndex
                                               AND ${valuePredicate(keyIndex)})"""
                    }
                """
                       AND f0.data_file_id = ANY (ARRAY(
                             SELECT pv$driver.data_file_id FROM hog_file_partition_value pv$driver
                              WHERE pv$driver.catalog_id = :catalogId
                                AND pv$driver.key_index = :k$driver
                                AND ${valuePredicate(driver)}$probes))
                """.trimEnd()
            }
        // THE INNER ORDER BY EXISTS ONLY WHEN THERE ARE NO ARMS: there it
        // is the index's own order and free. With arms the bound is the
        // InitPlan plus the LIMIT on the scan it drives, and a sort would
        // make the LIMIT wait for the whole bucket. See the KDoc.
        val innerOrder =
            if (arms.isEmpty()) "\n                     ORDER BY f0.file_size_bytes, f0.data_file_id" else ""
        // THE SPEC PREDICATE: inside the scan cap for an arm'd probe, where
        // it is a plain filter on the id-driven scan and the cap then
        // counts only this bucket's spec. Outside it for the no-arm probe,
        // where inside would make the ordered size scan walk past every
        // smaller file of other vintages; [bucketCandidates] states that
        // cost and the scan cap that covers it.
        val specInner =
            if (scopeToSpec && arms.isNotEmpty()) {
                "\n                       AND f0.spec_id IS NOT DISTINCT FROM :specId"
            } else {
                ""
            }
        val specFilter =
            if (scopeToSpec && arms.isEmpty()) "\n             WHERE f.spec_id IS NOT DISTINCT FROM :specId" else ""
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
                       AND f0.file_size_bytes < :targetBytes${if (verifiedOnly) VERIFIED_ONLY_SQL else ""}$armSql$specInner$innerOrder
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
     * One bucket's [limit] live candidate files (its smallest when it has
     * no tuple; an arbitrary [limit] of it otherwise — see
     * [candidateSql]).
     *
     * A bucket WITH a tuple gets its ids from V23's index, the planner
     * intersecting the keys by cost (see [candidateSql]); the
     * TUPLE-LESS bucket — an unpartitioned table, or the vintage a
     * mid-life-partitioned table wrote before its spec — gets none, and
     * is separated from the partitioned buckets by the `spec_id` filter
     * alone.
     *
     * THE SPEC FILTER IS INSIDE THE SCAN CAP WITH A TUPLE. The same tuple
     * can exist under two `spec_id`s of one table (a spec change that
     * keeps the keys and adds one, say), and with the filter outside the
     * cap the cap counted both vintages and the probe under-fetched this
     * one: QE's equivalence fixture recorded `values=[1,a] spec=2
     * scanLimit=40: truth=20 new=15 old=10`. Inside, it is a plain
     * filter on the id-driven primary-key scan — same plan shape — and
     * the cap counts only this bucket. `CandidateSqlEquivalenceIntegrationTest`
     * asserts the truth count.
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
                    verifiedOnly = ctx.objectPrefixFieldId != null,
                ),
            )
                .bind("catalogId", ctx.catalogId)
                .bind("tableId", ctx.tableId)
                .bind("targetBytes", budget)
                .bind("specId", bucket.specId)
                .bind("scanLimit", scanLimit)
                .bind("limit", limit)
                // Never a generic plan: it cannot tell the selective arm
                // from the common one. See [candidateSql].
                .addCustomizer(Pg.CUSTOM_PLANS_ONLY)
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
                    verifiedOnly = ctx.objectPrefixFieldId != null,
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

    // ---- one run ---------------------------------------------------------

    /** Manual-trigger convenience: defaults with an optional groups-per-run override. */
    override fun runOnce(
        catalog: String,
        batchOverride: Int?,
        trigger: MaintenanceTrigger,
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
    ): CompactionResult = executor.runOnce(catalog, cfg, trigger)

    /** Reserve the plan on the session handle. Keep the planning lock through the claim transaction. */
    private fun reserve(
        h: Handle,
        planned: PlanWithContext,
        cfg: CompactionConfig,
        room: Int,
        claimant: UUID,
    ): PlannedWork {
        val (ctx, plan, markable) = planned
        beforeClaims()
        // THE LOCK, across the claim transaction and nothing else, in
        // its own short transaction and outside the try below: a lock
        // timeout fails the plan, and a lock this call never took is
        // never released (1.4.1 released unconditionally, which is the
        // `you don't own a lock of type ExclusiveLock` WARNING after
        // every timed-out wait on prod).
        h.inTransaction<Unit, Exception> { Locks.acquireCatalogCompactionPlanLock(h, ctx.catalogId) }
        return try {
            // No catch: a failed claim transaction plans nothing for
            // this table (see the KDoc).
            h.inTransaction<PlannedWork, Exception> {
                // ITS OWN BOUND, not the probe's knob, and EQUAL to the
                // plan lock's wait (`Locks.acquireCatalogCompactionPlanLock`,
                // 15 s) on purpose: a sibling waiting on this lock is never
                // made to outlive the holder's bound, so a slow claim
                // transaction fails here before it fails every waiter. The
                // work is one re-read and a few inserts — milliseconds.
                h.execute("SET LOCAL statement_timeout = '${CLAIM_STATEMENT_TIMEOUT_MS}ms'")
                insideClaims(h)
                // THE RE-READ UNDER THE LOCK. Every sibling's claims
                // committed before it released the lock, so this sees
                // all of them; phase (c)'s read, unlocked, may not have.
                val held = CompactionClaimRepo.liveClaimedFileIds(h, ctx.catalogId, ctx.tableId)
                val (free, lost) = plan.groups.partition { g -> g.files.none { it.dataFileId in held } }
                val rechecked = plan.copy(groups = free, claimedGroups = plan.claimedGroups + lost.size)
                val items = mutableListOf<WorkItem>()
                var claimed = rechecked.claimedGroups
                for (group in free) {
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
                val work =
                    PlannedWork(
                        rechecked.copy(claimedGroups = claimed),
                        withBucketClaims(h, ctx, rechecked, markable, items, claimant, cfg),
                    )
                beforeClaimCommit(h)
                work
            }
        } finally {
            // After the claim transaction has committed or rolled back:
            // a sibling granted the lock next sees our claims.
            Locks.releaseCatalogCompactionPlanLock(h, ctx.catalogId)
        }
    }

    /**
     * Take the bucket claims [reserve]'s KDoc describes, inside its
     * claim transaction, and return [items] with each covered group's
     * [WorkItem.bucketClaim] set.
     */
    private fun withBucketClaims(
        h: Handle,
        ctx: TableContext,
        plan: CompactionPlan,
        markable: Set<Bucket>,
        items: List<WorkItem>,
        claimant: UUID,
        cfg: CompactionConfig,
    ): List<WorkItem> {
        if (markable.isEmpty() || items.isEmpty()) return items

        fun bucketOf(g: CompactionGroup) = Bucket(g.specId, g.partitionValues)
        val taken = items.groupBy { bucketOf(it.group) }
        val free = plan.groups.groupingBy { bucketOf(it) }.eachCount()
        val claims = HashMap<Bucket, BucketClaim>()
        for ((bucket, mine) in taken) {
            // Every free group of the bucket is ours, or the bucket keeps
            // work a sibling should be able to reach: no marker.
            if (bucket !in markable || mine.size != free[bucket]) continue
            val key = CompactionClaimRepo.bucketKey(bucket.specId, bucket.values)
            if (CompactionClaimRepo.acquire(
                    h,
                    ctx.catalogId,
                    ctx.tableId,
                    key,
                    emptyList(),
                    claimant,
                    cfg.bucketClaimTtlSeconds,
                )
            ) {
                claims[bucket] = BucketClaim(key, mine.size)
            }
        }
        if (claims.isEmpty()) return items
        val all = claims.values.toList()
        return items.map { item ->
            item.copy(bucketClaim = claims[bucketOf(item.group)], planBuckets = all)
        }
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
        return executor.compactGroup(ctx, group)
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
     * [stats] is the output's own footer aggregate (see [CompactionExecutor.compactGroup]):
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
     *
     * [survivors] of zero does not register the output at all: past the
     * same guards, the group is RETIRED ([retireDeadGroup]) and its
     * staging ticket is left for the cleanup drain.
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
            Locks.acquireCatalogCommitLock(
                h,
                ctx.catalogId,
                ctx.catalog,
                CommitLockPath.COMPACTION,
                ctx.commitLockTimeoutMs,
            )
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

            // A GROUP WITH NO SURVIVORS RETIRES ITS INPUTS AND REGISTERS
            // NOTHING. Every input's live DV deletes every row it holds, so
            // the rewrite wrote an EMPTY parquet — and registering that
            // object used to put a phantom in `hog_data_file`: a live file
            // of `record_count = 0` whose `row_id_start` (no survivor to
            // take it from, so the inputs' minimum) is a row-id floor
            // nothing in it owns, scanned, sampled and re-planned forever
            // by everything that walks live files.
            //
            // The proof the group is dead is `survivors == 0` AFTER the
            // rewrite read every input through its DV, with the caller's
            // `rowsWritten == survivingRecords` check behind it — never
            // the registered counts alone. A planner shortcut on
            // `survivingRecords == 0` would retire live rows the day a DV
            // registers a wrong `delete_count`; reading the rows is what
            // makes the retirement safe. And it is reached only past every
            // race guard above — ticket, drop, sort spec, input liveness
            // and the DV identity check — so a DV superseded since the
            // plan is still `dv_superseded`, never a retirement on a
            // vector that is no longer live.
            if (survivors == 0L) {
                return@inTransactionUnchecked retireDeadGroup(h, ctx, group, ids)
            }

            // THE OBJECT-PREFIX LAYOUT IS STILL THE ONE THE OUTPUT WAS
            // WRITTEN UNDER. Re-read under the lock (one probe of
            // `hog_table_layout_live`, which an option-off table answers
            // with no row): the field, its live name and type, and the
            // table's live names, and the output must be under the prefix
            // they name for the group's value. The DDL keeps all of them
            // fixed while the option is on and refuses setting it on a
            // table with live files, so a mismatch is a route that changes
            // them (a relayout, setting the option on a non-empty table)
            // or a broken invariant — and registering the output then would
            // put a file outside its prefix that no later check revisits.
            // A lost race like any other: skipped, the staged output left
            // for the cleanup drain.
            val liveLayout =
                h.createQuery(LAYOUT_RECHECK_SQL)
                    .bind("catalogId", ctx.catalogId)
                    .bind("tableId", ctx.tableId)
                    .map { rs, _ ->
                        rs.getLong("field_id") to
                            runCatching {
                                val layout =
                                    ObjectPrefixLayout(
                                        rs.getLong("field_id"),
                                        rs.getString("field_name")!!,
                                        ColType.fromWire(rs.getString("col_type")!!),
                                    )
                                ObjectPrefix.prefix(
                                    ctx.dataPath,
                                    rs.getString("namespace_name"),
                                    rs.getString("table_name"),
                                    layout,
                                    executor.prefixValue(ctx, group)!!,
                                )
                            }.getOrNull()
                    }
                    .findOne()
                    .orElse(null)
            val layoutHolds =
                if (liveLayout == null) {
                    ctx.objectPrefixFieldId == null
                } else {
                    liveLayout.first == ctx.objectPrefixFieldId &&
                        liveLayout.second?.let { ObjectPrefix.isUnder(outputPath, it) } == true
                }
            if (!layoutHolds) {
                log.warn {
                    "compaction group for ${ctx.namespace}.${ctx.table} was written to $outputPath under " +
                        "object-prefix field ${ctx.objectPrefixFieldId ?: "none"}, but the live layout is " +
                        "${liveLayout?.first ?: "none"} with prefix ${liveLayout?.second ?: "none"}; skipping " +
                        "the commit. The staged output stays queued for the cleanup drain"
                }
                return@inTransactionUnchecked GroupOutcome.SkippedConflict
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

            endSnapshotInputs(h, ctx, group, ids, snapshotId)

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

            GroupOutcome.Committed(snapshotId, outputBytes, dataFileId)
        }

    /**
     * End-snapshot a committing group's inputs and their applied DVs at
     * [snapshotId] — the tail both a compaction ([commitGroup]) and a
     * retirement ([retireDeadGroup]) share, inside the caller's
     * transaction and under its catalog lock.
     */
    private fun endSnapshotInputs(
        h: Handle,
        ctx: TableContext,
        group: CompactionGroup,
        ids: List<Long>,
        snapshotId: Long,
    ) {
        // End-snapshot the inputs — NOT delete: they stay readable at
        // every snapshot below this one, and expiry queues their paths
        // once end_snapshot sinks under the retention floor (the
        // superseded-DV lifecycle). Nothing enters hog_file_removal
        // here. hog_table_stats is untouched (gross append counters;
        // visible-file aggregates shrink only by the rows the DVs
        // already masked).
        //
        // `table_id = :tableId` on both updates is a guard (#264), and
        // a CHECKED one. [commitGroup]'s liveState re-verify, in this same
        // transaction under this same lock, already filtered every id
        // on `table_id` and bailed on any that was missing, so the
        // predicate matches every row the id set does — today. The
        // check is for the day it does not (an identity that is no
        // longer `(catalog_id, data_file_id)` alone, a DV whose
        // `table_id` disagrees with its file's): an UPDATE that ended
        // fewer rows than it was given would otherwise commit the
        // output LIVE beside a still-live input, serving its rows
        // twice with nothing logged (or, for a retirement, leave a
        // dead input live whose DV was ended, un-masking its rows).
        // Failing here rolls the group back into the one-failed-group
        // path instead.
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
    }

    /**
     * Retire a group whose rewrite produced ZERO rows: one snapshot, one
     * `table_compacted` change, every input and its DV end-snapshotted —
     * and nothing registered in their place. Called from [commitGroup]
     * inside its transaction, under the catalog commit lock, after every
     * plan-to-commit guard has passed; see the call site for why the
     * proof of death is the rewrite's row count and not the catalog's.
     *
     * NO FILE ID IS ALLOCATED. The snapshot allocation is the
     * [commitGroup] UPDATE without its `next_file_id` term: nothing is
     * registered, and a consumed id would be a hole in the sequence that
     * names no file. The cost is a second statement shape for the same
     * row, which is cheap; a uniform one would have spent an id per
     * retirement for nothing.
     *
     * THE STAGING TICKET IS NOT SETTLED, and that is the disposal path,
     * not an omission. The rewrite already uploaded an empty object to
     * the group's output path, under an undrained `compaction_staging`
     * ticket; leaving the ticket exactly as a lost race
     * ([GroupOutcome.SkippedConflict], [GroupOutcome.SkippedDvSuperseded])
     * leaves it hands the object to the normal cleanup drain, whose
     * liveness check passes because no file row names the path. No
     * direct delete here: an S3 call inside the commit lock would hold
     * the lock for a round trip, and one that failed after the commit
     * would leak the object with nothing left to retry it — the ticket is
     * already the retrying, counted, crash-safe delete. The row lock the
     * re-claim took is released at commit with the row untouched.
     */
    private fun retireDeadGroup(
        h: Handle,
        ctx: TableContext,
        group: CompactionGroup,
        ids: List<Long>,
    ): GroupOutcome {
        val (snapshotId, schemaVersion) =
            h.createQuery(
                """
                UPDATE hog_catalog
                   SET last_snapshot_id = last_snapshot_id + 1
                 WHERE catalog_id = :catalogId
                RETURNING last_snapshot_id, schema_version
                """,
            )
                .bind("catalogId", ctx.catalogId)
                .map { rs, _ -> rs.getLong(1) to rs.getLong(2) }
                .one()
        SnapshotRepo.insert(
            h,
            ctx.catalogId,
            snapshotId,
            schemaVersion,
            author = "compaction",
            message = "retired ${group.files.size} fully-deleted files of ${ctx.namespace}.${ctx.table}",
        )
        // The same change kind as a compaction, on purpose: a retirement
        // changes no visible row either (every row was already masked),
        // so it must stay invisible to the append-conflict guards and to
        // the changefeed's exclusion exactly as `table_compacted` is.
        SnapshotRepo.insertChange(h, ctx.catalogId, snapshotId, ChangeKind.TABLE_COMPACTED, ctx.tableId)
        endSnapshotInputs(h, ctx, group, ids, snapshotId)
        return GroupOutcome.Retired(snapshotId)
    }

    // ---- loops -----------------------------------------------------------

    /**
     * One sweep across every catalog, for the background loop
     * (BackgroundLoops in App.kt); per-catalog failure isolation
     * (Expiry pattern).
     */
    fun runOnceAllCatalogs(cfg: CompactionConfig = defaults): List<Pair<String, CompactionResult>> {
        // Shuffled per sweep (#455). In list order every replica started
        // every cycle on the same catalog, so the others always waited
        // behind its run (prod-us: millpond ~32 min a run, millrace
        // ~3.6 min) and all replicas moved in lockstep. The shuffle
        // spreads which catalog waits and desynchronises replicas; it
        // does NOT change a catalog's share of a pod's cycle, which is
        // the sum of one capped run per catalog whatever the order.
        // Every catalog still runs exactly once per sweep.
        val names = jdbi.withHandleUnchecked { h -> CatalogRepo.listAll(h) }.map { it.name }.shuffled()
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

    /** PostgreSQL queries retain their transaction boundaries behind the executor seam. */
    private inner class PostgresStorage : CompactionStorage {
        override fun <T> withPlanningSession(body: (CompactionStorage.PlanningSession) -> T): T =
            jdbi.withHandleUnchecked { h ->
                body(
                    object : CompactionStorage.PlanningSession {
                        override fun fetch(
                            catalog: String,
                            namespace: String,
                            table: String,
                            cfg: CompactionConfig,
                        ): CandidateFetch =
                            h.inTransaction<CandidateFetch, Exception> {
                                h.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                                h.execute("SET LOCAL statement_timeout = '${cfg.planStatementTimeoutMs}ms'")
                                beforeFetch(h)
                                fetchCandidates(h, catalog, namespace, table, cfg)
                            }

                        override fun liveClaimedFileIds(ctx: TableContext): Set<Long> =
                            CompactionClaimRepo.liveClaimedFileIds(h, ctx.catalogId, ctx.tableId)

                        override fun reserve(
                            planned: PlanWithContext,
                            cfg: CompactionConfig,
                            room: Int,
                            claimant: UUID,
                        ): PlannedWork = this@CompactionService.reserve(h, planned, cfg, room, claimant)
                    },
                )
            }

        override fun liveTables(catalog: String): List<Pair<String, String>> =
            jdbi.withHandleUnchecked { h -> this@CompactionService.liveTables(h, catalog) }

        override fun purgeExpired(catalog: String) {
            jdbi.withHandleUnchecked { h -> CompactionClaimRepo.purgeExpired(h, catalogIdOf(h, catalog)) }
        }

        override fun refreshClaim(
            ctx: TableContext,
            claimKey: String,
            bucketKeys: List<String>,
            claimant: UUID,
            cfg: CompactionConfig,
        ): Boolean =
            jdbi.withHandleUnchecked { h ->
                CompactionClaimRepo.refresh(
                    h,
                    ctx.catalogId,
                    ctx.tableId,
                    claimKey,
                    claimant,
                    cfg.claimTtlSeconds,
                    bucketKeys = bucketKeys,
                    bucketTtlSeconds = cfg.bucketClaimTtlSeconds,
                )
            }

        override fun releaseClaim(
            ctx: TableContext,
            claimKey: String,
            claimant: UUID,
        ) {
            jdbi.withHandleUnchecked {
                    h ->
                CompactionClaimRepo.release(h, ctx.catalogId, ctx.tableId, claimKey, claimant)
            }
        }

        override fun settleClaim(
            ctx: TableContext,
            claimKey: String,
            claimant: UUID,
            cfg: CompactionConfig,
            committed: Boolean,
            inputFileIds: List<Long>?,
        ) {
            jdbi.withHandleUnchecked { h ->
                if (committed) {
                    CompactionClaimRepo.shortenToCommitted(
                        h,
                        ctx.catalogId,
                        ctx.tableId,
                        claimKey,
                        claimant,
                        cfg.committedClaimTtlSeconds,
                        inputFileIds = inputFileIds,
                    )
                } else {
                    CompactionClaimRepo.release(h, ctx.catalogId, ctx.tableId, claimKey, claimant)
                }
            }
        }

        override fun stageOutputPath(
            catalogId: Long,
            outputPath: String,
        ): Long = this@CompactionService.stageOutputPath(catalogId, outputPath)

        override fun publish(
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
            commitGroup(
                ctx = ctx,
                group = group,
                outputPath = outputPath,
                outputBytes = outputBytes,
                footerSize = footerSize,
                stats = stats,
                splitOffsets = splitOffsets,
                survivors = survivors,
                rowIdStart = rowIdStart,
                stagingId = stagingId,
            )

        override fun recorded(
            catalog: String,
            trigger: MaintenanceTrigger,
            body: () -> CompactionResult,
        ): CompactionResult = runStore.recorded(catalog, MaintenanceTask.COMPACTION, trigger, body)

        override fun isStatementLockTimeout(error: Throwable): Boolean =
            error is UnableToExecuteStatementException && Pg.isLockTimeout(error)
    }

    internal companion object {
        /**
         * Whether [CompactionExecutor.executeGroup] yields before the part at [index] of a
         * split group: never before the FIRST part — a group past its claim
         * refresh is in flight, and the wave's contract (executeWave) is
         * that an in-flight group runs to completion, as every group did
         * before groups had parts — and before a later part only when a
         * sibling exhausted the heap or the sweep was cancelled.
         */
        fun yieldsBeforePart(
            index: Int,
            heapExhausted: Boolean,
            interrupted: Boolean,
        ): Boolean = index > 0 && (heapExhausted || interrupted)

        /** Inputs named with their path in an output-proof refusal; the rest by id only. */
        const val MAX_NAMED_INPUT_PATHS = 20

        /**
         * The claim transaction's `statement_timeout`, fixed and EQUAL to
         * the plan lock's 15 s wait (`Locks.acquireCatalogCompactionPlanLock`):
         * a waiter never outlives the holder's bound. Not
         * [CompactionConfig.planStatementTimeoutMs], which bounds the probe.
         */
        const val CLAIM_STATEMENT_TIMEOUT_MS = 15_000L

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
         * The cap on the buckets `CompactionService.newBuckets` may
         * offer one plan, INDEPENDENT of [MAX_SAMPLED_BUCKETS] (see
         * `mergeBuckets`). The window bounds it first — a tuple needs two
         * files, so at most `newBucketWindow / 2`, 5,000 at the default
         * — and this is the hard number behind that for a window raised
         * toward its 200,000 ceiling.
         */
        const val MAX_NEW_BUCKETS = 50_000

        /**
         * [candidateSql]'s `verifiedOnly` fragment: the per-value mount
         * predicate (`ObjectPrefix`), appended to the inner scan's WHERE
         * for a table with the object-prefix layout and for no other.
         */
        internal const val VERIFIED_ONLY_SQL: String =
            "\n                       AND f0.stats_state = 'provided' AND NOT f0.prefix_quarantined"

        /**
         * `commitGroup`'s object-prefix recheck under the commit lock: the
         * table's live layout row (`hog_table_layout_live`, unique and
         * partial, so an option-off table costs one empty probe) with its
         * column's live name and type and the table's live names, each a
         * live-index or primary-key probe. A layout whose column is not
         * live comes back with a null name, which fails the recheck.
         *
         * THE COLUMN IS READ NEWEST FIRST, `LIMIT 1`. `hog_column`'s only
         * key that names the field is the primary key `(catalog_id,
         * table_id, field_id, begin_snapshot)`, and every rename, promote
         * and `set_column_comment` adds a version under it (the DDL
         * refuses only the rename while the option is on), so filtering
         * the field's rows for the live one walked its history — 500
         * versions, 15 buffers, in `CompactionLayoutRecheckPlanIntegrationTest`.
         * The live version is the newest by construction (a version
         * begins where its predecessor ends), so a backward scan stops at
         * it. The namespace is a correlated primary-key lookup for the
         * same reason in another table: joined, the planner hashed every
         * namespace of a 1,001-namespace catalog (9 buffers). Measured
         * there, warm: 2 buffers for an option-off table, 12 for an
         * option-on one with 1,000 table versions and 500 field versions.
         */
        internal const val LAYOUT_RECHECK_SQL: String =
            """
            SELECT l.object_prefix_field_id AS field_id, c.name AS field_name, c.col_type,
                   tv.name AS table_name,
                   (SELECT ns.name FROM hog_namespace ns
                     WHERE ns.catalog_id = tv.catalog_id AND ns.namespace_id = tv.namespace_id) AS namespace_name
              FROM hog_table_layout l
              JOIN hog_table_version tv
                ON tv.catalog_id = l.catalog_id AND tv.table_id = l.table_id AND tv.end_snapshot IS NULL
              LEFT JOIN LATERAL (
                    SELECT c.name, c.col_type FROM hog_column c
                     WHERE c.catalog_id = l.catalog_id AND c.table_id = l.table_id
                       AND c.field_id = l.object_prefix_field_id AND c.end_snapshot IS NULL
                     ORDER BY c.begin_snapshot DESC
                     LIMIT 1
                   ) c ON true
             WHERE l.catalog_id = :catalogId AND l.table_id = :tableId AND l.end_snapshot IS NULL
            """

        /**
         * [newBuckets]' statement — `internal` so
         * `CompactionCandidateFetchPlanIntegrationTest` EXPLAINs this
         * string and not a retyped copy.
         *
         * The inner `ORDER BY begin_snapshot DESC LIMIT` is what bounds
         * it: a backward walk of the `(catalog_id, table_id,
         * begin_snapshot)` range (`hog_data_file_live`, so no ended row is visited) from the newest
         * file, stopping at `:fileCap`, with the partition-value lookup
         * in the select list so it runs once per row that LIMIT passes
         * and never for a file the size filter drops. The outer
         * aggregate is over at most `:fileCap` rows.
         */
        internal const val NEW_BUCKETS_SQL: String =
            """
            SELECT n.spec_id,
                   n.partition_values,
                   count(*) AS small_count,
                   sum(n.file_size_bytes) AS small_bytes
              FROM (
                    SELECT f.spec_id, f.file_size_bytes,
                           (SELECT array_agg(pv.value ORDER BY pv.key_index)
                              FROM hog_file_partition_value pv
                             WHERE pv.catalog_id = f.catalog_id
                               AND pv.data_file_id = f.data_file_id) AS partition_values
                      FROM hog_data_file f
                     WHERE f.catalog_id = :catalogId AND f.table_id = :tableId
                       AND f.begin_snapshot > :since
                       AND f.end_snapshot IS NULL
                       AND f.file_size_bytes < :targetBytes
                     ORDER BY f.begin_snapshot DESC
                     LIMIT :fileCap
                   ) n
             GROUP BY n.spec_id, n.partition_values
            HAVING count(*) >= :minFiles
             ORDER BY sum(n.file_size_bytes)::float8 / count(*) ASC,
                      count(*) DESC
             LIMIT :bucketCap
            """

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

        /** The savepoint [liveBucketClaims] rewinds to when its read fails. */
        const val BUCKET_CLAIM_SAVEPOINT = "compaction_bucket_claims"

        /**
         * This pod's planner identity: `POD_NAME` (the downward-API name,
         * if a chart passes one), else `HOSTNAME` (the pod name on
         * Kubernetes, the machine name locally — the source
         * `CleanupService.defaultWorkerIdPrefix` already uses), else
         * `local`.
         *
         * Deliberately WITHOUT a per-process UUID, unlike the cleanup
         * claimant: the point here is a STABLE spread, and a restarted
         * container in the same pod should resume the same slice of the
         * bucket list rather than land on a fresh random one. Two pods
         * that both fall through to `local` (only possible off
         * Kubernetes) share a start, which costs what every pod sharing
         * one cost before this existed and nothing more: the bucket
         * claims still keep their probes apart.
         */
        fun defaultPlannerIdentity(): String =
            listOf("POD_NAME", "HOSTNAME")
                .firstNotNullOfOrNull { System.getenv(it)?.takeIf { v -> v.isNotBlank() } }
                ?: "local"

        /**
         * Where a pod's FIRST sweep of (catalog, table) starts in a
         * bucket list of [size]: a position derived from [identity] and
         * the table, the same on every call, different between pods.
         *
         * # Why hash, and not `k * size / N`
         *
         * An even split — pod k of N at `k * size / N` — is the ideal,
         * and the server cannot compute it: the maintenance pods are a
         * Deployment, so they have no ordinal, and nothing tells a pod
         * how many siblings it has. A hash of the identity modulo the
         * size is the best an unknowing pod can do: uniform in
         * expectation, so with N pods each probing `considered` buckets a
         * sweep, a pair overlaps on a fraction ~`2 x considered / size`
         * of its first sweep. At 2026-10-21's shape (9 pods, 123 buckets
         * probed a sweep, ~2,200 offered) the expected number of pairs
         * starting closer than one sweep apart is ~3.3, and the chance of
         * none is under 1%: overlap on the FIRST sweep is the norm, not
         * the exception. The bucket claims absorb it — the later planner
         * skips what the earlier one claimed — and the cursors then
         * leapfrog rather than walk together, where before all 36 pairs
         * walked the same buckets forever.
         *
         * SHA-256, not `String.hashCode`: pod names differ in their last
         * few characters, and the polynomial hash maps two names that
         * differ only in the last character to values one apart — which
         * modulo the size are ADJACENT starts, the herd again.
         * `catalogId` and `tableId` are hashed in so that one pod's
         * tables do not all start at the same fraction of their lists.
         *
         * Only the first sweep: after it, the cursor ([bucketCursor])
         * carries on from where this pod stopped, so the pods' slices
         * advance side by side instead of converging.
         */
        fun initialCursorOffset(
            identity: String,
            catalogId: Long,
            tableId: Long,
            size: Int,
        ): Int {
            if (size <= 0) return 0
            return Math.floorMod(identitySeed("$identity/$catalogId/$tableId"), size.toLong()).toInt()
        }

        /**
         * The first 8 bytes of SHA-256 over [identity]: a well-mixed
         * 64-bit value for a string whose variants differ in their last
         * characters (pod names). [initialCursorOffset]'s hash, and the
         * compaction loop's jitter seed (`BackgroundLoops.Jitter`), where
         * `String.hashCode`'s near-equal values for near-equal names would
         * give `java.util.Random` near-equal seeds.
         */
        fun identitySeed(identity: String): Long =
            java.nio.ByteBuffer.wrap(
                java.security.MessageDigest.getInstance("SHA-256").digest(identity.toByteArray(Charsets.UTF_8)),
            ).long
    }
}
