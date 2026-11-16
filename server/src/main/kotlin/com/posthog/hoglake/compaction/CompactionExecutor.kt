package com.posthog.hoglake.compaction

import com.posthog.hoglake.compaction.CompactionService.Companion.MAX_NAMED_INPUT_PATHS
import com.posthog.hoglake.compaction.CompactionService.Companion.SHUTDOWN_WAIT_SECONDS
import com.posthog.hoglake.compaction.CompactionService.Companion.wasInterrupt
import com.posthog.hoglake.compaction.CompactionService.Companion.yieldsBeforePart
import com.posthog.hoglake.compaction.CompactionService.GroupOutcome
import com.posthog.hoglake.compaction.CompactionService.SweepInterrupted
import com.posthog.hoglake.compaction.CompactionWork.CandidateFetch
import com.posthog.hoglake.compaction.CompactionWork.FreeGroups
import com.posthog.hoglake.compaction.CompactionWork.PlanWithContext
import com.posthog.hoglake.compaction.CompactionWork.PlannedWork
import com.posthog.hoglake.compaction.CompactionWork.TableContext
import com.posthog.hoglake.compaction.CompactionWork.WorkItem
import com.posthog.hoglake.hydrator.FooterSplitOffsets
import com.posthog.hoglake.hydrator.FooterStats
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.hydrator.asCatalogColumns
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CompactionResult
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.ObjectPrefix
import com.posthog.hoglake.model.allNodes
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.storage.StorageAccess
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.schema.GroupType
import java.util.UUID
import java.util.concurrent.TimeUnit

/** Plans and executes compaction work without a catalog connection. */
internal class CompactionExecutor(
    private val storage: CompactionStorage,
    private val store: ObjectStore,
) {
    private val log = KotlinLogging.logger(CompactionService::class.java.name)

    internal var beforePacking: () -> Unit = {}
    internal var beforeRewrite: (List<ParquetRewriter.Input>) -> Unit = {}
    internal var afterProbe: (List<VariantLayoutGroups.Probed>) -> Unit = {}
    internal var appendFloorBytes: Long = ParquetRewriter.APPEND_MIN_ROW_GROUP_BYTES

    /** Check the layout before the backend reads candidate rows. */
    fun canPlan(ctx: TableContext): Boolean {
        // A VARIANT table compacts only with HOGLAKE_COMPACTION_VARIANT on
        // for its catalog, which copies each variant in the layout its
        // group's files share (ParquetRewriter's class doc,
        // VariantLayoutGroups). Off, do not enqueue work the
        // rewriter refuses on every sweep. allNodes, not the top level: a
        // variant nested inside a struct is still a variant, and `struct{v:
        // variant}` has no top-level one. #77's check predates
        // containers, where the two were the same question. On, a
        // declaration no layout can be derived from (one predating a
        // grammar rule) is still never planned.
        //
        // Checked BEFORE the fetch, not after: a variant table's
        // candidate rows were read and thrown away on every sweep.
        val variants = ctx.columns.allNodes().filter { it.def.type == ColType.VARIANT }
        return !(
            variants.isNotEmpty() &&
                (!ctx.variantCompaction || variants.any { VariantLayout.shape(it) == null })
        )
    }

    fun planTable(
        catalog: String,
        namespace: String,
        table: String,
        cfg: CompactionConfig,
    ): CompactionPlan =
        storage.withPlanningSession { source -> planSnapshot(source, catalog, namespace, table, cfg).plan }

    fun runOnce(
        catalog: String,
        cfg: CompactionConfig,
        trigger: MaintenanceTrigger,
    ): CompactionResult = storage.recorded(catalog, trigger) { runSweep(catalog, cfg) }

    /**
     * Plan one table with NO lock held, then claim its queue under the
     * catalog planning lock. The queue is bounded by the remaining run
     * budget; file I/O starts after unlock.
     *
     * # What the planning lock covers, and why only that (#418)
     *
     * `Locks.acquireCatalogCompactionPlanLock` is taken AFTER
     * [planSnapshot] returns, in its own short transaction, and released
     * after the claim transaction has committed (or rolled back), so the
     * probes, the packing and phase (c) all run unlocked and two planners
     * of one catalog plan at the same time.
     *
     * The lock used to be taken before [planSnapshot] and held across
     * every probe. Its job was to stop two overlapping plans from
     * reserving DIFFERENT group keys for the same input files: a claim
     * insert arbitrates one key ([CompactionClaimRepo.acquire]), and bin
     * packing is a function of the candidate set, so a plan formed one
     * ingest file later packs the same files under another key. With the
     * probes serialized, every sibling's claims were committed before
     * this plan read anything, and phase (c)'s overlap read was exact.
     *
     * What production showed is what that cost (gigahog-prod-us, 1.4.1,
     * 2026-10-22, nine planners): one pod in a 15 s probe held the lock,
     * the other eight queued on `pg_advisory_lock` and hit their own 15 s
     * bound there, and one slow bucket cost the fleet a sweep — 38 of 40
     * millpond-prod-us sweeps failed, most of them on the lock wait.
     *
     * The claim transaction keeps that guarantee on its own: under the
     * lock it RE-READS the table's live claims
     * ([CompactionClaimRepo.liveClaimedFileIds]) and drops every planned
     * group that overlaps one before inserting anything, counted as
     * `claimed_elsewhere` like phase (c)'s drops. The lock is released
     * only after the claim transaction COMMITS (the release is a session
     * statement after `inTransaction` returns), so a sibling granted the
     * lock next re-reads with our rows visible, and two runs never hold
     * overlapping groups. Phase (c) stays: it is the preview's only claim
     * read, and it spares an execution plan the lock wait for a table a
     * sibling already holds. The re-read materializes every live claim's
     * `input_file_ids` a second time per plan, under the lock; at the
     * queue bound (`maxGroupsPerRun` claims per concurrent run, each at
     * most `maxInputFiles` ids) that is tens of thousands of ids, and an
     * SQL-side `input_file_ids && :planned` returning keys would shorten
     * the hold further — not done, because the hold it would save is
     * milliseconds against the probe this change took out from under it.
     *
     * What concurrency costs instead, stated: two planners may PROBE the
     * same bucket at once, pack the same groups, and the later one drops
     * them — in phase (c), or here by the re-read. A wasted probe, never a
     * duplicate rewrite. Excluding live-claimed files inside the probe's
     * SQL was measured and rejected: it costs O(claimed ids) per probe and
     * goes linear past `work_mem` (server/README.md has the numbers).
     *
     * # A failed claim transaction plans NOTHING for the table
     *
     * Not the unclaimed plan. Before #418 that fallback ran groups phase
     * (c) had filtered under the lock; now phase (c) ran unlocked and the
     * re-read either never ran or rolled back, so executing the plan
     * unclaimed could rewrite files a sibling holds — a duplicate rewrite
     * that one side then loses at commit. So the failure throws, the
     * sweep's catalog run fails the way a failed probe does, and the
     * ledger records it. A lock timeout does the same (the acquire is
     * outside the claim transaction's scope for that reason too).
     *
     * # Bucket claims, in the same transaction as the group claims
     *
     * After the group claims, a BUCKET claim (`CompactionClaimRepo`'s
     * class KDoc has why it is a row in the same table) for every bucket
     * that is (1) in [PlanWithContext.markable] — probed in full, not
     * cut by the candidate budget — and (2) whose every free group this
     * plan just claimed, "free" meaning after the in-lock re-read. The
     * next planner skips such a bucket before probing it
     * ([fetchCandidates]), so the 2026-10-21 shape — a pod re-reading
     * 50,029 candidate rows to drop its predecessors' 64 groups — cannot
     * recur for the claim's lifetime.
     *
     * (2) is what keeps the skip from HIDING work. A bucket that packed
     * into more groups than the run's remaining room still has groups
     * nobody holds; marking it would hide them from every sibling until
     * this run settled. Such a bucket is left unmarked and the sibling
     * finds its held groups the old way, by overlap. A group
     * claim insert that loses to a sibling's live row is not in `items`,
     * so its bucket is not marked either. A bucket that packed into NO
     * free group is not marked: there is no group whose settlement would
     * release the claim, and a marker this run cannot tell from its own
     * next sweep's would make that sweep skip a bucket whose files may
     * have grown into a group.
     *
     * The marker's lease is the SHORT [CompactionConfig.bucketClaimTtlSeconds],
     * renewed by EVERY group's pre-IO refresh ([refreshClaim]) — each
     * renews all of the plan's still-outstanding markers, so a marked
     * bucket stays hidden until the last of its own groups settles, however
     * far back in the run's queue they are, and not for one lease. A pod
     * killed mid-sweep — `BackgroundLoops.close` abandons the sweep after
     * 5 s, and no release runs — hides its buckets for one short lease
     * after its last refresh, not for the hour-long group lease. A marker
     * that still expires (one group's rewrite outlasting the lease) costs
     * a sibling one probe and the overlap skip, never a duplicate rewrite.
     * A lost marker insert (a sibling marked the bucket after this plan's
     * snapshot) is simply not tracked.
     */
    private fun planAndClaim(
        catalog: String,
        namespace: String,
        table: String,
        cfg: CompactionConfig,
        room: Int,
        claimant: UUID,
    ): PlannedWork =
        storage.withPlanningSession { source ->
            val planned = planSnapshot(source, catalog, namespace, table, cfg)
            val (ctx, plan) = planned
            if (!cfg.claimsEnabled) {
                return@withPlanningSession PlannedWork(plan, plan.groups.take(room).map { WorkItem(ctx, it) })
            }
            if (plan.groups.isEmpty()) return@withPlanningSession PlannedWork(plan, emptyList())
            source.reserve(planned, cfg, room, claimant)
        }

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
        source: CompactionStorage.PlanningSession,
        ctx: TableContext,
        cfg: CompactionConfig,
        groups: List<CompactionGroup>,
    ): FreeGroups {
        if (!cfg.claimsEnabled || groups.isEmpty()) return FreeGroups(groups, 0)
        val claimed =
            try {
                source.liveClaimedFileIds(ctx)
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

    /**
     * Plan one table in three phases on one connection. No transaction
     * remains open across packing, and no lock is held during it: execution plans take the
     * catalog planning lock afterwards, for their claim transaction only (`planAndClaim`).
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
     * (`hog_sort_spec.sort_id`), that every input is still live (`end_snapshot IS NULL`),
     * that every input still carries EXACTLY its planned deletion vector by
     * `delete_file_id`, and that the output is under the prefix the live
     * object-prefix layout names (`hog_table_layout`). A stale plan therefore costs one group —
     * `skipped_conflicts` or `dv_superseded` — and never a wrong commit.
     * That was already true of a plan the old shape held across minutes
     * of rewriting other groups; widening the window by the packing time
     * changes nothing about which check catches it.
     */
    private fun planSnapshot(
        source: CompactionStorage.PlanningSession,
        catalog: String,
        namespace: String,
        table: String,
        cfg: CompactionConfig,
    ): PlanWithContext {
        val startedAt = System.nanoTime()
        // (a) One short read transaction: the table's identity, its
        // shape, and its candidate rows, all on one MVCC snapshot.
        val fetched = source.fetch(catalog, namespace, table, cfg)
        // (b) Packing outside a transaction. The hook is the test seam that
        // makes this phase slower than the session's idle bound; see
        // [beforePacking].
        beforePacking()
        val packed = pack(fetched, cfg)
        // (c) One short read for the sibling maintainer's claims.
        val free = withoutClaimedGroups(source, fetched.ctx, cfg, packed.groups)
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
                objectPrefixUnresolvableGroups = packed.prefixUnresolvable,
                claimedGroups = free.claimed,
                candidatesFetched = fetched.candidatesFetched,
                bucketsConsidered = fetched.bucketsConsidered,
                bucketsAvailable = fetched.bucketsAvailable,
                bucketsSkippedClaimed = fetched.bucketsSkippedClaimed,
                candidatesTruncated = if (fetched.truncated) 1 else 0,
                planMs = planMs,
            ),
            fetched.markable,
        )
    }

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

    /** [lastBudgetRefusal]'s twin for the object-prefix refusals in [pack], keyed the same way. */
    private val lastPrefixRefusal = java.util.concurrent.ConcurrentHashMap<Pair<Long, Long>, String>()

    /**
     * The variant layouts of files this process has probed, so [pack]
     * can keep a VARIANT table's files of different layouts apart without
     * reading a footer: see [VariantLayoutGroups] and [layoutParts].
     */
    internal val variantLayouts = VariantLayoutMemory()

    /**
     * The canonical value of [ctx]'s object-prefix field that names
     * [group]'s prefix, or null when the option is on and the group has
     * none: no spec, no single identity key on the field in its spec, or
     * a missing or non-canonical value at that key. Groups never span a
     * bucket, so the value is one for the whole group. Always null with
     * the option off.
     */
    fun prefixValue(
        ctx: TableContext,
        group: CompactionGroup,
    ): String? {
        val layout = ctx.objectPrefix ?: return null
        val keyIndex = group.specId?.let { ctx.prefixKeyIndex[it] } ?: return null
        val raw = group.partitionValues?.getOrNull(keyIndex) ?: return null
        return raw.takeIf { ObjectPrefix.parseValue(it, layout.colType) != null }
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

    /**
     * What [pack] produced: what will be attempted, and the sorted groups
     * the spill and merge budgets refused in metadata.
     */
    private data class PackedGroups(
        val groups: List<CompactionGroup>,
        val spillRefused: Long,
        val mergeRefused: Long,
        val prefixUnresolvable: Long = 0,
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
        // A VARIANT table's files pack BY LAYOUT where this process has
        // learned one ([variantLayouts], filled by [layoutParts]'s probe):
        // a file remembered in a layout other than the declared packs only
        // with its own kind, a DuckDB-written one packs with nothing, and
        // the rest — the declared layout's and the never-probed — pack
        // together as every file did before. The planner reads no footer
        // for this; a group it packs wrong is split by the probe once and
        // remembered. Null when the table has no variant or the flag is
        // off: nothing to keep apart.
        val declaredFingerprint =
            if (ctx.variantCompaction && ctx.columns.allNodes().any { it.def.type == ColType.VARIANT }) {
                runCatching { VariantLayout.fingerprint(VariantLayout.declared(ctx.columns)) }.getOrNull()
            } else {
                null
            }
        // A SORTED group of a remembered deviating layout is admitted against
        // THAT layout's shape here, in metadata — the probe that first met
        // the layout remembered its node, nesting and leaf counts beside its
        // fingerprint (VariantLayoutMemory.Shape) — so a layout the budgets
        // refuse is refused before another footer is read, uncharged and
        // counted with the planner's refusals below, instead of re-probed
        // and charged every sweep. Admission is recomputed each plan from
        // the current inputs and the current budgets, and a declaration
        // change makes the entries stale (re-probed once).
        val spill = ctx.sortSpill
        val spillRefused = mutableListOf<CompactionGroup>()
        val mergeRefused = mutableListOf<CompactionGroup>()
        // Groups admitted against their KNOWN shape: that verdict is the
        // whole admission, so the declared-estimate filter below passes them
        // through. Judged against the declared estimate as well, a group of
        // files in a NARROWER historical layout (a declaration since
        // widened) would be refused — uncharged, and for good — for rows it
        // does not hold.
        val judgedByShape = java.util.Collections.newSetFromMap(java.util.IdentityHashMap<CompactionGroup, Boolean>())
        val declaredLeaves by lazy { runCatching { ParquetRewriter.outputLeafCount(ctx.columns) }.getOrNull() }
        val declaredVariantLeaves by lazy {
            ctx.columns.allNodes().filter { it.def.type == ColType.VARIANT }.associate { column ->
                column.fieldId to
                    runCatching {
                        VariantLayout.leaves(
                            VariantLayout.groupType(column, org.apache.parquet.schema.Type.Repetition.OPTIONAL),
                        )
                    }.getOrDefault(0)
            }
        }

        // The budgets' verdict on a group in a known layout: null admits.
        // Pure, so that a loan the planner only CONSIDERS (borrow, below)
        // can be judged without being counted a refusal.
        fun refusalOfKnown(
            group: CompactionGroup,
            shapes: Map<Long, VariantLayoutMemory.Shape>,
        ): BudgetRefusal? {
            if (spill == null) return null
            val leaves = declaredLeaves ?: return null
            val partLeaves =
                leaves - shapes.keys.sumOf { declaredVariantLeaves[it] ?: 0 } + shapes.values.sumOf { it.leaves }
            return budgetRefusal(ctx, cfg.sortSpillOfShapes(ctx.columns, shapes), partLeaves, group)
        }

        fun admitKnown(
            group: CompactionGroup,
            shapes: Map<Long, VariantLayoutMemory.Shape>,
        ): Boolean {
            if (spill == null || declaredLeaves == null) return true
            judgedByShape += group
            return when (refusalOfKnown(group, shapes)) {
                BudgetRefusal.SPILL -> false.also { spillRefused += group }
                BudgetRefusal.MERGE -> false.also { mergeRefused += group }
                null -> true
            }
        }
        for ((bucket, candidates) in fetched.byBucket) {
            // THE FAN-IN SCALES WITH THE FILES IT IS CAPPING. Computed per
            // bucket from the rows just fetched, so a bucket of 12 KiB
            // files gets thousands of inputs and a bucket of 200 MB files
            // still gets a handful.
            val fanIn = cfg.effectiveMaxInputFiles(candidates.map { it.fileSizeBytes })
            if (declaredFingerprint == null) {
                val packed = grouping.groups(candidates, cfg.minInputFiles, fanIn) { it.fileSizeBytes }
                for (take in packed) out += CompactionGroup(rowIdOrder(take), bucket.specId, bucket.values)
                continue
            }
            val divided = variantLayouts.populations(ctx.catalogId, declaredFingerprint, candidates)
            // A remembered population with a short remainder may borrow idle
            // unknowns to complete it (VariantLayoutGroups.borrow), under
            // the packing below and the same shape admission: a loan to a
            // group the budgets would refuse is never made, so the arrival
            // is free for a population they admit. Idle means in no main
            // group the declared estimate admits — the admission `fits`
            // applies below, applied here first and uncounted, so that a
            // main group the budgets refuse (no work this sweep or any
            // other) does not keep its unknowns from a population that
            // could use them. The object-prefix refusal (below) needs no
            // such check: it depends only on the bucket, so a bucket it
            // refuses refuses every group, loans included.
            VariantLayoutGroups.borrow(
                divided,
                grouping,
                cfg.minInputFiles,
                fanIn,
                admit = { deviations, take ->
                    val shapes = variantLayouts.shapeOf(deviations)
                    shapes == null ||
                        refusalOfKnown(CompactionGroup(rowIdOrder(take), bucket.specId, bucket.values), shapes) == null
                },
                admitMain = { take ->
                    val leaves = spill?.let { declaredLeaves }
                    val group = CompactionGroup(rowIdOrder(take), bucket.specId, bucket.values)
                    spill == null || leaves == null || budgetRefusal(ctx, spill, leaves, group) == null
                },
            ) { it.fileSizeBytes }
            for (take in grouping.groups(divided.main, cfg.minInputFiles, fanIn) { it.fileSizeBytes }) {
                out += CompactionGroup(rowIdOrder(take), bucket.specId, bucket.values)
            }
            for (other in divided.others) {
                val shapes = variantLayouts.shapeOf(other.deviations)
                for (take in grouping.groups(other.files, cfg.minInputFiles, fanIn) { it.fileSizeBytes }) {
                    val group = CompactionGroup(rowIdOrder(take), bucket.specId, bucket.values)
                    if (shapes == null || admitKnown(group, shapes)) out += group
                }
            }
        }
        // OBJECT-PREFIX LAYOUT: a group whose prefix cannot be named — no
        // spec, no single identity key on the field, a missing or
        // non-canonical value, or a layout whose column is not live or not
        // of an allowed type, which refuses every group ([prefixValue]) —
        // has no path its output may be written to. Refused HERE, in
        // metadata and uncharged like the budget refusals below, because it
        // re-refuses every sweep: the commit and DDL guards make it
        // unreachable through the API, so a nonzero count is a broken
        // invariant or a foreign row, and charging it would let one such
        // bucket starve the sweep.
        val (resolvable, unresolvable) =
            if (ctx.objectPrefixFieldId == null) out to emptyList() else out.partition { prefixValue(ctx, it) != null }
        logPrefixRefusals(ctx, unresolvable)
        // A SORTED group the rewrite's own budgets would refuse is refused
        // HERE, in metadata, uncharged — see [budgetRefusal]. Only
        // configuration produces either (a spill budget small against the
        // byte target, a heap budget small against it), so the same groups
        // re-plan and re-refuse every sweep until an operator moves a knob.
        // The leaf count is the output schema's; a live schema the rewrite
        // cannot shape at all has no count, and its groups are left for
        // the rewrite to refuse with its own typed reason. A group already
        // judged against its known layout's shape ([judgedByShape]) is not
        // judged again against the declared estimate: its rows are its
        // layout's, wider or narrower than the declaration says.
        val leaves = spill?.let { declaredLeaves }
        val fits =
            if (spill == null || leaves == null) {
                resolvable
            } else {
                resolvable.filter { group ->
                    group in judgedByShape ||
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
            unresolvable.size.toLong(),
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
                    "${cfg.spillChunkRows(ctx.columns)} row(s) per spill file (the declared layout's estimate; " +
                    "a part in another variant layout is re-judged by its own at execution), against " +
                    "${cfg.sortedHeapBytesPerGroup} B per group (HOGLAKE_COMPACTION_SORTED_HEAP_BYTES " +
                    "${cfg.sortedHeapBytes} B / HOGLAKE_COMPACTION_PARALLEL_GROUPS " +
                    "${cfg.parallelGroups}). Each spilled run holds a row group while it is merged, " +
                    "so the budget is too small for the group size: raise " +
                    "HOGLAKE_COMPACTION_SORTED_HEAP_BYTES (with the pod's memory), lower " +
                    "HOGLAKE_COMPACTION_PARALLEL_GROUPS, or lower HOGLAKE_COMPACTION_TARGET_BYTES"
            }
        }
    }

    /**
     * WARN about a table's unresolvable object-prefix groups when the
     * picture CHANGES, for [logBudgetRefusals]' reason: the same groups
     * re-refuse every sweep, and the counter is the per-sweep signal.
     */
    private fun logPrefixRefusals(
        ctx: TableContext,
        unresolvable: List<CompactionGroup>,
    ) {
        val key = ctx.catalogId to ctx.tableId
        if (unresolvable.isEmpty()) {
            lastPrefixRefusal.remove(key)
            return
        }
        val sample = unresolvable.take(5).joinToString { "${it.specId}, ${it.partitionValues}" }
        val signature = "${unresolvable.size} ${ctx.objectPrefix} $sample"
        if (lastPrefixRefusal.put(key, signature) == signature) return
        log.warn {
            "compaction refused ${unresolvable.size} group(s) of ${ctx.namespace}.${ctx.table} whose object " +
                "prefix cannot be resolved (field_id=${ctx.objectPrefixFieldId} layout=${ctx.objectPrefix}; " +
                "a null layout is a column that is not live or not of an allowed type, which refuses every " +
                "group). First (spec_id, partition values): $sample"
        }
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
                "groups_compacted=${r.groupsCompacted} groups_retired=${r.groupsRetired} " +
                    "files_in=${r.filesIn} files_out=${r.filesOut} " +
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
                    "files_unranged=${r.filesUnranged} " +
                    "candidates_fetched=${r.candidatesFetched} " +
                    "buckets_considered=${r.bucketsConsidered}/${r.bucketsAvailable} " +
                    "buckets_skipped_claimed=${r.bucketsSkippedClaimed} " +
                    "candidates_truncated=${r.candidatesTruncated} plan_ms=${r.planMs} " +
                    "object_prefix_unresolvable=${r.objectPrefixUnresolvable} " +
                    "layout_probe_only=${r.layoutProbeOnly}"
            },
        ) {
            if (cfg.maxGroupsPerRun <= 0) {
                throw HoglakeException.Validation(
                    "batch (groups per run) must be positive (got ${cfg.maxGroupsPerRun})",
                )
            }
            val result = doRunOnce(catalog, cfg)
            Metrics.compactionGroups(catalog, result.groupsCompacted)
            Metrics.compactionGroupsRetired(catalog, result.groupsRetired)
            Metrics.compactionFilesRewritten(catalog, result.filesIn)
            Metrics.compactionSkipped(catalog, "unconvertible_schema", result.unconvertibleSchema)
            Metrics.compactionSkipped(catalog, "invalid_data", result.invalidData)
            // `heap_budget` is HISTORICAL: nothing produces it since the
            // external merge sort (hoglake#134). Its successors:
            Metrics.compactionSkipped(catalog, "spill_budget", result.spillBudgetExceeded)
            Metrics.compactionSkipped(catalog, "merge_budget", result.mergeBudgetExceeded)
            Metrics.compactionSpillBytes(catalog, result.spillBytes)
            Metrics.compactionAppendedBytes(catalog, result.bytesAppended)
            Metrics.compactionAppendSkipped(catalog, "unknown_range", result.filesUnranged)
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
            Metrics.compactionSkipped(catalog, "object_prefix_unresolvable", result.objectPrefixUnresolvable)
            Metrics.compactionSkipped(catalog, "layout_probe_only", result.layoutProbeOnly)
            // Buckets, not groups, so not a `skipped` reason: a bucket
            // can hold any number of groups, and summing the two in one
            // series would make neither readable.
            Metrics.compactionBucketsSkippedClaimed(catalog, result.bucketsSkippedClaimed)
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
        /**
         * Groups whose every input was fully deleted: retired at a
         * snapshot of their own with no output (see [GroupOutcome.Retired]).
         * An OUTCOME, so in [attempts]; and deliberately NOT in
         * [groupsCompacted], [filesIn], [bytesIn] or [bytesOut], which
         * describe rewrites that produced a file — `files_out` is
         * `groups_compacted`, and counting a retirement there would claim
         * an output that was never registered.
         */
        val groupsRetired: Long = 0,
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
        /** Refused in metadata by the planner, so not in [attempts]; see [CompactionPlan.objectPrefixUnresolvableGroups]. */
        val objectPrefixUnresolvable: Long = 0,
        /**
         * Groups of a VARIANT table whose layout probe read every input's
         * footer and found no partition worth a rewrite (each layout too
         * few files, or every file DuckDB's): IO spent, nothing compacted,
         * so IN [attempts]. See [layoutParts].
         */
        val layoutProbeOnly: Long = 0,
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
         * Runs of sorted rewrites whose first-key range was unknown, in
         * groups where that cost an append: a run that passed every other
         * condition, disjoint from every known range, merged because of
         * them. Work, like the append counters, so not in [attempts].
         */
        val filesUnranged: Long = 0,
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
        val bucketsSkippedClaimed: Long = 0,
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
                    groupsCompacted + groupsRetired + skippedConflicts + dvSuperseded +
                        unconvertibleSchema + invalidData + failedGroups + layoutProbeOnly
                ).toInt()

        operator fun plus(other: GroupTally) =
            GroupTally(
                groupsCompacted + other.groupsCompacted,
                groupsRetired + other.groupsRetired,
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
                objectPrefixUnresolvable + other.objectPrefixUnresolvable,
                layoutProbeOnly + other.layoutProbeOnly,
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
                filesUnranged + other.filesUnranged,
                candidatesFetched + other.candidatesFetched,
                bucketsConsidered + other.bucketsConsidered,
                bucketsAvailable + other.bucketsAvailable,
                candidatesTruncated + other.candidatesTruncated,
                planMs + other.planMs,
                bucketsSkippedClaimed + other.bucketsSkippedClaimed,
            )

        companion object {
            /** A group that was never attempted: no counter moves. */
            val NOT_ATTEMPTED = GroupTally()
        }
    }

    /**
     * One of a bucket claim's groups has SETTLED — committed, skipped,
     * failed, lost its lease, or released unstarted. The last one to do
     * so deletes the claim, so a sibling's next plan probes the bucket
     * again.
     *
     * Deleted rather than shortened on a commit, unlike a group claim
     * (`executeGroup`'s release): a group claim outlives its commit for a
     * sibling's plan that ALREADY names the dead inputs, and no sibling
     * plan can name this bucket's groups without having probed it —
     * which the marker stopped. A sibling that probes it after the
     * release reads the live rows, so it sees the outputs, not the
     * inputs. Best effort: a failed DELETE leaves a lease that expires.
     */
    private fun settleBucketClaim(item: WorkItem) {
        val bucket = item.bucketClaim ?: return
        val claimant = item.claimant ?: return
        if (bucket.outstanding.decrementAndGet() != 0) return
        runCatching {
            storage.releaseClaim(item.ctx, bucket.key, claimant)
        }.onFailure { e -> log.warn(e) { "could not release a compaction bucket claim; it will expire" } }
    }

    /**
     * Release queued work on cancellation or a heap refusal. Started groups settle their own claims.
     *
     * An item is released here only if this call WINS it from the worker
     * (`started.compareAndSet`, the same flag [executeGroup] takes), so a
     * pool worker that starts the group a moment later cannot settle it a
     * second time — a double settle would release the bucket claim one
     * group early. Bucket claims are settled in a first pass, BEFORE the
     * group releases: they are the costlier leak (a whole bucket), so if
     * this is cut short by the process dying they are the ones to have
     * gone.
     */
    private fun releaseQueuedClaims(items: List<WorkItem>) {
        val interrupted = Thread.interrupted()
        try {
            val won = items.filter { it.claimant != null && it.started.compareAndSet(false, true) }
            for (item in won) settleBucketClaim(item)
            for (item in won) {
                val claimant = item.claimant ?: continue
                runCatching {
                    storage.releaseClaim(item.ctx, CompactionClaimKey.groupKey(item.group), claimant)
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
            storage.purgeExpired(catalog)
        }
            .onFailure { e -> log.warn(e) { "compaction claim purge failed for '$catalog'; continuing" } }
        val tables = storage.liveTables(catalog)

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
                        objectPrefixUnresolvable = plan.objectPrefixUnresolvableGroups,
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
                        bucketsSkippedClaimed = plan.bucketsSkippedClaimed,
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
            groupsRetired = groupsRetired,
            filesUnranged = filesUnranged,
            candidatesFetched = candidatesFetched,
            bucketsConsidered = bucketsConsidered,
            bucketsAvailable = bucketsAvailable,
            candidatesTruncated = candidatesTruncated,
            planMs = planMs,
            bucketsSkippedClaimed = bucketsSkippedClaimed,
            objectPrefixUnresolvable = objectPrefixUnresolvable,
            layoutProbeOnly = layoutProbeOnly,
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
     * A broken claim table still permits work; a lost claim does not.
     *
     * Renews the plan's still-outstanding bucket claims ([bucketKeys])
     * in the same statement, on their own short lease — see
     * [CompactionConfig.bucketClaimTtlSeconds] for why a bucket claim
     * must lapse quickly once nothing is working its queue, and why
     * every group renews all of them rather than only its own. Only the
     * GROUP claim's survival decides the answer: a lapsed bucket claim
     * costs a sibling a probe, never this group its run.
     */
    private fun refreshClaim(
        ctx: TableContext,
        claimKey: String,
        bucketKeys: List<String>,
        claimant: UUID,
        cfg: CompactionConfig,
    ): Boolean =
        runCatching {
            storage.refreshClaim(ctx, claimKey, bucketKeys, claimant, cfg)
        }.getOrElse { e ->
            log.warn(
                e,
            ) { "compaction claim refresh failed for ${ctx.namespace}.${ctx.table}; continuing without a claim" }
            true
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
     *
     * A group of a VARIANT table may be MORE THAN ONE rewrite: its inputs
     * are probed and partitioned by variant layout first ([layoutParts]),
     * and each partition worth a rewrite is compacted on its own, in its
     * own layout, through its own [attempt].
     */
    private fun executeGroup(
        catalog: String,
        item: WorkItem,
        cfg: CompactionConfig,
        heapExhausted: java.util.concurrent.atomic.AtomicBoolean,
    ): GroupTally {
        val (ctx, group) = item
        if (heapExhausted.get()) return GroupTally.NOT_ATTEMPTED
        // Take the item, or find that `releaseQueuedClaims` already did
        // (cancellation racing a pool worker): exactly one of the two
        // settles its claims.
        if (!item.started.compareAndSet(false, true)) return GroupTally.NOT_ATTEMPTED
        // Refresh the reservation before file I/O. A lost lease spends no I/O.
        val claimKey = item.claimant?.let { CompactionClaimKey.groupKey(group) }
        val liveBuckets = item.planBuckets.filter { it.outstanding.get() > 0 }.map { it.key }
        if (claimKey != null && !refreshClaim(ctx, claimKey, liveBuckets, item.claimant, cfg)) {
            log.debug {
                "compaction group of ${group.files.size} files for " +
                    "$catalog/${ctx.namespace}.${ctx.table} is claimed by another maintainer; " +
                    "skipping without spending its IO"
            }
            // No IO spent, so no budget spent: GroupTally.attempts does
            // not count this and doRunOnce pulls the next candidate.
            settleBucketClaim(item)
            return GroupTally(claimedElsewhere = 1)
        }

        var committed = false
        // The inputs that died under this item's claim: all of the group's
        // for the one part a group ordinarily is; the committed parts' for a
        // variant group the probe split, whose other files are still live
        // candidates the committed claim must not hold.
        val dead = mutableListOf<Long>()
        try {
            // THE VARIANT-LAYOUT PROBE, as its own attempt. A table with
            // no variant is one part, the group as planned, and nothing is
            // read here. A variant table's inputs are read footer by footer
            // and partitioned by layout ([layoutParts]); the probe's own
            // refusals (a shape no layout can be derived from, an object
            // store error) resolve the whole group through the same arms a
            // rewrite's would, and nothing below runs.
            var decided: LayoutParts? = null
            var tally =
                attempt(catalog, ctx, group, cfg, heapExhausted) {
                    decided = layoutParts(ctx, group, cfg)
                    GroupTally()
                }
            val planned = decided ?: return tally
            // How many parts reached a rewrite. A probe that read every
            // footer and ran none — nothing worth a rewrite, or every part
            // refused on its own re-admission below — still spent a group's
            // IO: charged as one attempt (`layout_probe_only`) after the
            // loop, so a sweep over a backlog of such groups is bounded by
            // maxGroupsPerRun like any other. (The budget refusals are not
            // attempts, as the planner's are not; without this the probe's
            // footer reads would repeat every sweep, uncounted.)
            var ran = 0
            // ONE REWRITE PER PART, each resolved on its own: a part that
            // fails after another committed must not take the committed
            // one's accounting with it, which is why the arms wrap each
            // attempt and not the loop. A split group spends one attempt
            // per part, so a sweep may run past `maxGroupsPerRun` by the
            // extra parts of its last wave — at most (layouts - 1) x
            // parallelGroups attempts, spent on work the planner meant to
            // do. The FIRST part never yields: a group that got this far is
            // in flight, and the wave's contract is that an in-flight group
            // runs to completion (executeWave); only the parts after it
            // yield to a heap exhausted by a sibling or to cancellation.
            // Any part, the first included, can still be refused on
            // re-admission below.
            for ((index, part) in planned.parts.withIndex()) {
                if (yieldsBeforePart(index, heapExhausted.get(), Thread.currentThread().isInterrupted)) break
                // A SORTED part is bounded by ITS layout, not the declared
                // one: the chunk is sized from the subtree its rows actually
                // hold (a declaration since cleared or narrowed leaves files
                // far wider than the catalog says), and the part is
                // re-admitted against those bounds in metadata, as the
                // planner admitted the group against the declared ones —
                // refused uncharged, the planner's way, when the wider rows
                // break a budget the declared estimate fit.
                val partSpill =
                    ctx.sortSpill?.let {
                        if (part.layouts.isEmpty()) {
                            it
                        } else {
                            cfg.sortSpill(
                                ctx.columns,
                                part.layouts,
                            )
                        }
                    }
                if (partSpill != null && part.layouts.isNotEmpty()) {
                    // The leaf count is the part's output schema's. That schema
                    // cannot fail to build for a planned part: every layout in
                    // it is one the probe's walk accepted, and the live
                    // columns are the ones the planner built the declared
                    // output schema from (a live schema no output schema can
                    // be made of — a `_hog_row_id` collision, a corrupt
                    // container row — is unreachable through the API, and
                    // [pack] leaves such a table for the rewrite to refuse,
                    // typed, before any IO). Kept total on the same terms as
                    // [pack]: no count, no budget check, and the rewrite's own
                    // refusal counts the part.
                    val leaves = runCatching { ParquetRewriter.outputLeafCount(ctx.columns, part.layouts) }.getOrNull()
                    val refusal = leaves?.let { budgetRefusal(ctx, partSpill, it, part.group) }
                    if (refusal != null) {
                        log.warn {
                            "compaction part of ${part.group.files.size} files for " +
                                "$catalog/${ctx.namespace}.${ctx.table} in a variant layout of " +
                                "${partSpill.chunkRows} rows per chunk is refused by the " +
                                "sorted rewrite's ${refusal.name.lowercase()} budget; the planner admitted the group " +
                                "against the declared layout's estimate of ${ctx.sortSpill?.chunkRows} rows per " +
                                "chunk. Levers as for the planner's refusals"
                        }
                        tally +=
                            when (refusal) {
                                BudgetRefusal.SPILL -> GroupTally(spillBudgetExceeded = 1)
                                BudgetRefusal.MERGE -> GroupTally(mergeBudgetExceeded = 1)
                            }
                        part.dropFooters()
                        continue
                    }
                }
                ran++
                tally +=
                    attempt(catalog, ctx, part.group, cfg, heapExhausted, partSpill) { work ->
                        val outcome =
                            compactGroup(ctx, part.group, part.layouts, part.footers, partSpill) { r ->
                                work.tally =
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
                                        filesUnranged = r.filesUnranged.toLong(),
                                    )
                                Metrics.compactionSortCheckBytes(catalog, r.sortCheckBytes)
                            }
                        work.tally +
                            when (outcome) {
                                is GroupOutcome.Committed -> {
                                    committed = true
                                    dead += part.group.files.map { it.dataFileId }
                                    compactedAway(ctx, planned, part, outcome.dataFileId)
                                    GroupTally(
                                        groupsCompacted = 1,
                                        filesIn = part.group.files.size.toLong(),
                                        bytesIn = part.group.totalBytes,
                                        bytesOut = outcome.bytesOut,
                                    )
                                }
                                is GroupOutcome.Retired -> {
                                    // Committed for the claim's purposes: the
                                    // inputs are dead, exactly as after a
                                    // rewrite, and a sibling's older plan still
                                    // names them.
                                    committed = true
                                    dead += part.group.files.map { it.dataFileId }
                                    compactedAway(ctx, planned, part, outputId = null)
                                    GroupTally(groupsRetired = 1)
                                }
                                GroupOutcome.SkippedConflict -> GroupTally(skippedConflicts = 1)
                                GroupOutcome.SkippedDvSuperseded -> GroupTally(dvSuperseded = 1)
                            }
                    }
                // The probe's footers for this part are spent either way.
                part.dropFooters()
            }
            if (planned.probed && ran == 0) tally += GroupTally(layoutProbeOnly = 1)
            return tally
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
                    storage.settleClaim(
                        ctx,
                        claimKey,
                        item.claimant,
                        cfg,
                        committed,
                        dead.takeIf { it.size < group.files.size },
                    )
                }.onFailure { e ->
                    log.warn(e) {
                        "could not settle the compaction claim for " +
                            "$catalog/${ctx.namespace}.${ctx.table}; it expires in " +
                            "${cfg.claimTtlSeconds}s"
                    }
                }
            }
            settleBucketClaim(item)
        }
    }

    /** What a rewrite did before it resolved — the runs and the spill were spent whatever the commit then decides — for the arms of [attempt] that count it. */
    private class RewriteWork {
        var tally = GroupTally()
    }

    /**
     * Run [body] — one rewrite of [group], or the probe that decides what
     * the rewrites are — and convert every way it can end into a
     * [GroupTally]. NOTHING escapes, `Throwable` included: see
     * [executeGroup] for why, and [failGroup] for the one bad-group path
     * two of the arms share.
     */
    private fun attempt(
        catalog: String,
        ctx: TableContext,
        group: CompactionGroup,
        cfg: CompactionConfig,
        heapExhausted: java.util.concurrent.atomic.AtomicBoolean,
        /** The sorted bounds this rewrite runs under (a part's own, see executeGroup), for the heap arm's message. */
        spill: SortSpill? = ctx.sortSpill,
        body: (RewriteWork) -> GroupTally,
    ): GroupTally {
        val work = RewriteWork()
        try {
            return body(work)
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
        } catch (e: UnconvertibleSchemaException) {
            // Skip-with-reason, not a failure: the group stays
            // uncompacted until the schema or the file set moves.
            if (e is VariantLayoutMismatchException) Metrics.compactionVariantLayoutMismatch(catalog, e.reason)
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
            return work.tally +
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
            return work.tally +
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
                    "holds one chunk of ${spill?.chunkRows ?: cfg.spillChunkRows(ctx.columns)} rows or its admitted " +
                    "merge runs, each bounded by ${cfg.sortedHeapBytesPerGroup} B per group, so " +
                    "the chunk accounting (per-node heap estimate, nested expansion) or the merge " +
                    "admission under-counted this table's shape. Lower " +
                    "HOGLAKE_COMPACTION_SORTED_HEAP_BYTES or raise " +
                    "HOGLAKE_COMPACTION_NESTED_SORT_EXPANSION, and report the table's shape"
            }
            return GroupTally(failedGroups = 1)
        } catch (e: Throwable) {
            // The backend identifies statement lock timeouts. Every other statement error follows the failed-group path.
            if (storage.isStatementLockTimeout(e)) {
                log.warn {
                    "compaction group of ${group.files.size} files for " +
                        "$catalog/${ctx.namespace}.${ctx.table} hit the commit transaction's " +
                        "lock_timeout inside the commit tail; skipping — the staged output stays " +
                        "queued for the cleanup drain"
                }
                return GroupTally(skippedConflicts = 1)
            }

            // One bad group (unreadable input, corrupt DV, S3
            // hiccup, a stack overflow in the nested copier) never
            // wedges the sweep — but it IS counted: an uncounted
            // swallow is a silently-dead compactor with a green run
            // ledger (the NoSuchBucket incident).
            return failGroup(e, catalog, ctx, group)
        }
    }

    /**
     * Record one failed group. Preserve interrupt detection on the original exception.
     *
     * A throw inside a catch bypasses its sibling catch arms. The former
     * `UnableToExecuteStatementException` arm used to rethrow a
     * non-lock-timeout straight out of `executeGroup`, where nothing
     * caught it: `f.get()` in [executeWave] surfaced it as an
     * `ExecutionException` and the ENTIRE sweep aborted with an empty
     * ledger result, losing the accounting of every group that had not
     * started yet (on gigahog-prod-us, 2026-09-30: one group's
     * `statement_timeout` took ~58 others with it and the run recorded
     * `{}`). A statement that dies for a reason this code has no rule for
     * is ONE failed group, like any other.
     */
    private fun failGroup(
        e: Throwable,
        catalog: String,
        ctx: TableContext,
        group: CompactionGroup,
    ): GroupTally {
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

    /**
     * How [group] is rewritten: as planned, for a table with no VARIANT
     * (or the flag off, which never plans one); for a variant table, per
     * layout — see [VariantLayoutGroups] for the whole account.
     *
     * The probe reads every input's footer (the tail read the rewrite's
     * open would make; the footer is handed on so it is not made twice,
     * within [VariantLayoutGroups.FOOTER_RETENTION_BYTES]) and takes each
     * variant column's layout off it by the rewrite's own plan walk
     * ([ParquetRewriter.variantLayoutsOf]), so what it accepts the rewrite
     * accepts, and its refusals are the rewrite's, typed. What it learns
     * is remembered ([variantLayouts]) so the planner packs these files
     * by layout from now on, and a partition too small to rewrite is not
     * planned again until it has company.
     */
    private fun layoutParts(
        ctx: TableContext,
        group: CompactionGroup,
        cfg: CompactionConfig,
    ): LayoutParts {
        if (!ctx.variantCompaction || ctx.columns.allNodes().none { it.def.type == ColType.VARIANT }) {
            return LayoutParts(listOf(VariantLayoutGroups.LayoutPart(group, emptyMap(), emptyMap())), probed = false)
        }
        val declared = VariantLayout.declared(ctx.columns)
        val declaredFingerprint = VariantLayout.fingerprint(declared)
        val probed = probeLayouts(ctx, group)
        afterProbe(probed)
        val split =
            VariantLayoutGroups.split(
                group,
                probed,
                declared,
                CompactionGrouping.of(cfg.targetBytes),
                cfg.minInputFiles,
            )
        val leftBehind = split.leftBehind.mapTo(HashSet()) { it.file.dataFileId }
        // The files left behind that the memory did not already hold as set
        // aside, under this declaration: what `files_left` counts. A
        // remembered file is probed again as the company of a loan
        // (VariantLayoutGroups.borrow) that proves to be another layout, or
        // as the declared file beside arrivals that prove so, and either
        // leaves it exactly as it was; counting it again would count the
        // same four files on every arrival that disappointed them. A file
        // learned anew — after a restart, an eviction, a declaration change
        // — is counted anew, and a compaction output, remembered at its
        // registration and never yet set aside, is counted the first time a
        // probe leaves it.
        var newlyLeft = 0L
        for (p in probed) {
            // How the file deviates from THIS declaration, under its
            // fingerprint: a column the file predates deviates in nothing,
            // and a later declaration makes the entry stale (see
            // VariantLayoutMemory). A deviating layout's SHAPE is remembered
            // beside the fingerprint, so the planner can admit a group of it
            // against the sorted budgets without another probe.
            val deviations = if (p.duckdb) emptyMap() else VariantLayoutGroups.deviations(p.layouts, declared)
            val deviationFingerprint = VariantLayout.fingerprint(deviations)
            if (!p.duckdb && deviations.isNotEmpty()) {
                variantLayouts.rememberShape(
                    deviationFingerprint,
                    deviations.mapValues { (_, layout) ->
                        val (nodes, nested) = VariantLayout.shape(layout)
                        VariantLayoutMemory.Shape(nodes, nested, VariantLayout.leaves(layout))
                    },
                )
            }
            val known =
                VariantLayoutMemory.Known(
                    declaredFingerprint,
                    if (p.duckdb) VariantLayoutMemory.DUCKDB else deviationFingerprint,
                    setAside = p.file.dataFileId in leftBehind,
                )
            if (known.setAside && variantLayouts.recall(ctx.catalogId, p.file.dataFileId) != known) newlyLeft++
            variantLayouts.remember(ctx.catalogId, p.file.dataFileId, known.declared, known.deviations, known.setAside)
        }
        // The counters. `split` once per group the probe found mixed, so
        // its rate is how often the planner's metadata picture was wrong;
        // `duckdb` per file excluded, as the refusal it still is; the files
        // left behind per file, each once per time the memory learns it —
        // the memory keeps them out of the next plan, so the count is files
        // set aside, not sweeps, and not a loan's disappointments.
        if (split.layoutCount > 1) Metrics.compactionVariantLayoutMismatch(ctx.catalog, "split")
        Metrics.compactionVariantLayoutMismatch(ctx.catalog, "duckdb", split.duckdb.size.toLong())
        Metrics.compactionVariantLayoutFilesLeft(ctx.catalog, newlyLeft)
        if (split.mixed || split.leftBehind.isNotEmpty()) {
            log.info {
                "compaction group of ${group.files.size} files for ${ctx.catalog}/${ctx.namespace}.${ctx.table} " +
                    "holds ${split.layoutCount} variant layout(s)" +
                    (if (split.duckdb.isNotEmpty()) " and ${split.duckdb.size} DuckDB-written file(s)" else "") +
                    ": rewriting ${split.parts.size} part(s) of " +
                    split.parts.joinToString { "${it.group.files.size}${if (it.declared) " (declared)" else ""}" } +
                    " file(s); ${split.leftBehind.size} file(s) left for want of company in their layout" +
                    (
                        if (split.duckdb.isNotEmpty()) {
                            "; excluded (data_file_id): " + split.duckdb.joinToString { "${it.file.dataFileId}" }
                        } else {
                            ""
                        }
                    )
            }
        }
        return LayoutParts(split.parts, probed = true, declaredFingerprint = declaredFingerprint)
    }

    /** What [layoutParts] decided: the rewrites, whether a probe read the inputs' footers to decide, and under which declaration. */
    private class LayoutParts(
        val parts: List<VariantLayoutGroups.LayoutPart>,
        val probed: Boolean,
        val declaredFingerprint: Long = 0,
    )

    /**
     * Every input of [group] opened for its footer, [TableContext.inputOpenParallelism]
     * at a time, and read for its variant layouts: see [layoutParts].
     */
    private fun probeLayouts(
        ctx: TableContext,
        group: CompactionGroup,
    ): List<VariantLayoutGroups.Probed> {
        // The SAME catalog-scoped view [compactGroup] rewrites through —
        // WRITE, not READ: under `broker` the base store holds no session
        // for the catalog's objects at all, and a READ view would mint and
        // cache a second session per catalog for footers the WRITE session
        // the rewrite needs moments later reads just as well. One view,
        // one session, probe and rewrite alike.
        val store = this.store.forCatalog(ctx.catalog, ctx.dataPath, StorageAccess.WRITE)
        val inputs =
            group.files.map { f ->
                ParquetRewriter.Input(
                    S3InputFile(store, f.path, f.fileSizeBytes, f.footerSize),
                    f.path,
                    f.rowIdStart,
                    fileSizeBytes = f.fileSizeBytes,
                )
            }
        val probed = ArrayList<VariantLayoutGroups.Probed>(inputs.size)
        var retained = 0L
        // INTERNED: one canonical tree per distinct layout tuple, however
        // many inputs hold it. Each footer's walk builds a fresh tree (a
        // thousand-field declaration is a few thousand nodes), and holding
        // one per input would scale the probe's heap with fan-in x layout
        // width; held per distinct layout it scales with the layouts a
        // group holds, which is what the rest of the probe already costs.
        val interned = HashMap<VariantLayoutGroups.LayoutKey, Map<Long, GroupType>>()
        ParquetRewriter.forEachOpenedInput(inputs, ctx.inputOpenParallelism) { input, reader ->
            val file = group.files[probed.size]
            val footer = reader.footer
            // created_by FIRST. A DuckDB-written file is excluded from every
            // part whatever its variant field holds, so a shape the walk
            // would refuse (`layout`) must not refuse the whole group on its
            // account; and a DuckDB file holding NO live variant column
            // (scalars written before the column existed) is an ordinary
            // input the rewrite null-fills — Trino's SQL-NULL reading of
            // DuckDB's Variant nulls has nothing to read in it.
            val duck = footer.fileMetaData.createdBy?.startsWith("DuckDB") == true
            val layouts =
                try {
                    ParquetRewriter.variantLayoutsOf(footer, ctx.columns, input.label)
                        .let { interned.getOrPut(VariantLayoutGroups.LayoutKey(it)) { it } }
                } catch (e: VariantLayoutMismatchException) {
                    if (!duck) throw e
                    null
                }
            // Kept for the rewrite while the serialized footer bytes of
            // those kept so far fit the retention budget (see the constant):
            // the footer's own measure, not the registered hint.
            val bytes = VariantLayoutGroups.footerBytes(footer, file.fileSizeBytes)
            val keep = retained + bytes <= VariantLayoutGroups.FOOTER_RETENTION_BYTES
            if (keep) retained += bytes
            probed +=
                VariantLayoutGroups.Probed(
                    file,
                    if (keep) footer else null,
                    layouts ?: emptyMap(),
                    duckdb = duck && (layouts == null || layouts.isNotEmpty()),
                )
        }
        return probed
    }

    /**
     * A part's inputs are dead: forget their layouts, count the output's,
     * and remember the output ([outputId], null for a retirement) as what
     * was just written — its deviations are the part's — so it is not
     * probed on the next sweep to learn what this sweep knew.
     */
    private fun compactedAway(
        ctx: TableContext,
        parts: LayoutParts,
        part: VariantLayoutGroups.LayoutPart,
        outputId: Long?,
    ) {
        if (!parts.probed) return
        variantLayouts.forget(ctx.catalogId, part.group.files.map { it.dataFileId })
        if (outputId != null) {
            variantLayouts.remember(
                ctx.catalogId,
                outputId,
                parts.declaredFingerprint,
                VariantLayout.fingerprint(part.deviations),
            )
        }
        Metrics.compactionVariantLayoutOutputs(ctx.catalog, if (part.declared) "declared" else "other")
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

    /**
     * Rewrite one group (all IO outside any transaction) and commit it.
     *
     * [variantLayouts] is the subtree each VARIANT column is written in
     * (empty = the declared layouts), and [footers] the inputs' footers
     * the probe already read, by data_file_id — both from
     * [layoutParts]; the test surface [compactPlannedGroup] passes neither.
     */
    fun compactGroup(
        ctx: TableContext,
        group: CompactionGroup,
        variantLayouts: Map<Long, GroupType> = emptyMap(),
        footers: Map<Long, ParquetMetadata> = emptyMap(),
        /** The sorted rewrite's bounds for THIS group's layout (executeGroup sizes a part's from [variantLayouts]). */
        spill: SortSpill? = ctx.sortSpill,
        onRewritten: (ParquetRewriter.RewriteResult) -> Unit = {},
    ): GroupOutcome {
        // The store scoped to THIS catalog's prefix: under `broker` a
        // write session the provider granted for exactly these objects,
        // under the unscoped providers the shared store itself. Every
        // read and write of the group goes through it.
        val store = this.store.forCatalog(ctx.catalog, ctx.dataPath, StorageAccess.WRITE)
        // The OBJECT-PREFIX value, before any IO. The planner refuses a
        // group without one ([pack]); this surface is also reached from
        // [compactPlannedGroup], which is handed a group, so it fails
        // closed rather than writing outside every prefix.
        val prefixValue = prefixValue(ctx, group)
        check(ctx.objectPrefixFieldId == null || prefixValue != null) {
            "group of ${ctx.namespace}.${ctx.table} (spec_id ${group.specId}, values ${group.partitionValues}) " +
                "has no resolvable object-prefix value; refusing to write its output"
        }
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
                    // The probe's footer, when it kept one: the rewrite's
                    // open then costs no tail read ([ParquetRewriter.open]).
                    footer = footers[f.dataFileId],
                )
            }
        // Bare UUID, deliberately indistinguishable from an ingested
        // file (the pyhoglake writer's shape). A `compacted-` prefix
        // used to sit here; it told readers nothing the catalog does
        // not already say — explicit_row_ids is the flag that decides
        // how a file's row ids are read, and no reader may infer that
        // from a path — while giving every compaction output in a
        // table the same 10-character lead-in.
        // With the object-prefix layout, under the group's value's prefix,
        // which is what the commit's recheck and every later DV on the
        // output are held to; without it, exactly the old path.
        val outputPath =
            ObjectPrefix.dataFilePath(ctx.dataPath, ctx.namespace, ctx.table, ctx.objectPrefix, prefixValue)

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
        val stagingId = storage.stageOutputPath(ctx.catalogId, outputPath)

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
                spill,
                appendFloorBytes,
                ctx.variantCompaction,
                variantLayouts,
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
        // THE OUTPUT'S PREFIX PROOF, the commit's rule for inline stats
        // made on the footer the writer just built: an output that does
        // not prove every row holds the value is never registered, and
        // a missing footer is no exception — a `pending` output would be
        // left to the hydrator, which would quarantine it after the
        // inputs were already retired. Not needed for a group with no
        // survivors, which registers nothing.
        val layout = ctx.objectPrefix
        if (layout != null && rewritten.rowsWritten > 0) {
            val aggregate =
                checkNotNull(stats) {
                    "the rewrite of ${group.files.size} files of ${ctx.namespace}.${ctx.table} returned no " +
                        "footer; an object-prefix output is never registered unproven"
                }
            val verdict =
                ObjectPrefix.checkFieldStats(
                    layout,
                    ObjectPrefix.parseValue(prefixValue, layout.colType)!!,
                    aggregate.firstOrNull { it.fieldId == layout.fieldId },
                )
            if (verdict != ObjectPrefix.Verdict.OK) {
                // EVERY input's id, for the lossy-rewrite check's reason above:
                // this refusal is durable, so the group re-plans and
                // re-refuses every sweep, and the operator needs the files
                // to hunt the writer. Which input lies is not known here —
                // the rewrite keeps no input's footer — so none is singled out.
                // Paths only for the first 20: at a 2,048-file fan-in they
                // would make a ~300 KB line, past what a log pipeline keeps.
                throw InvalidDataException(
                    "output of ${group.files.size} inputs does not prove ${layout.fieldName} = $prefixValue " +
                        "for $outputPath ($verdict): an input holds rows of another value or nulls although " +
                        "its registration proved the value. Inputs (data_file_id path), first " +
                        "${minOf(group.files.size, MAX_NAMED_INPUT_PATHS)}: " +
                        group.files.take(MAX_NAMED_INPUT_PATHS).joinToString { "${it.dataFileId} ${it.path}" } +
                        if (group.files.size > MAX_NAMED_INPUT_PATHS) {
                            "; all data_file_ids: " + group.files.joinToString { "${it.dataFileId}" }
                        } else {
                            ""
                        },
                )
            }
        }
        val outcome =
            storage.publish(
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
            is GroupOutcome.Retired ->
                log.info {
                    "retired ${group.files.size} fully-deleted files (${group.totalBytes} B, " +
                        "${group.totalRecords} registered rows, 0 survivors) of " +
                        "${ctx.namespace}.${ctx.table} at snapshot ${outcome.snapshotId} with no " +
                        "output; the empty object at $outputPath stays queued for the cleanup drain"
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
}
