package com.posthog.hoglake.compaction

import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.model.ObjectPrefix
import com.posthog.hoglake.model.ObjectPrefixLayout
import com.posthog.hoglake.model.SortFieldDef
import java.util.UUID

/** Values shared by the executor and the PostgreSQL storage operations. */
internal object CompactionWork {
    /** Everything execution needs beyond the group list. */
    data class TableContext(
        val catalogId: Long,
        /** The catalog's name: the `catalog` label of the commit-lock series. */
        val catalog: String,
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
         * trust predicate's threshold (see `CompactionExecutor.trustedSorted`).
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
        /** Whether this catalog's variant tables compact (CompactionConfig.variantCompactionFor). */
        val variantCompaction: Boolean = false,
        /**
         * The table's object-prefix field id at the planning head
         * (`hog_table_layout`), null = the option is off. THIS, not
         * [objectPrefix], is what says the option is on: a layout whose
         * column is not live resolves no [objectPrefix], and every group
         * of the table is then refused as unresolvable rather than
         * compacted as if the option were off.
         */
        val objectPrefixFieldId: Long? = null,
        /** The layout at the planning head, with the column's live name and type; see [objectPrefixFieldId]. */
        val objectPrefix: ObjectPrefixLayout? = null,
        /**
         * spec_id -> the key_index of the one identity field on
         * [objectPrefixFieldId] in that spec ([ObjectPrefix.keyIndex]),
         * null when it has none, for every spec of the table, retired
         * ones included (`SpecRepo.fields`): a file names its spec by id,
         * and the specs are kept for as long as such a file lives. Empty
         * when the option is off.
         */
        val prefixKeyIndex: Map<Long, Int?> = emptyMap(),
        /**
         * The namespace's containment at the planning head. An org mark
         * roots the output paths under its prefix ([ObjectPrefix.dataFilePath]).
         */
        val containment: NamespaceContainment? = null,
    )

    data class PlanWithContext(
        val ctx: TableContext,
        val plan: CompactionPlan,
        /**
         * Buckets this plan probed IN FULL (not cut by the candidate
         * budget), the only ones `CompactionExecutor.planAndClaim` may take a bucket claim
         * on: a truncated bucket has files the plan never saw, and
         * hiding them from a sibling would be hiding work nobody took.
         */
        val markable: Set<Bucket> = emptySet(),
    )

    /** A candidate bucket: one (spec, partition values) pair. */
    data class Bucket(val specId: Long?, val values: List<String?>?)

    /** Phase (a)'s output: what to pack, and what it cost to find. */
    data class CandidateFetch(
        val ctx: TableContext,
        /** The group byte budget this fetch filtered on: the target, for every table. */
        val budget: Long,
        val byBucket: Map<Bucket, List<CompactionCandidate>>,
        val candidatesFetched: Long,
        val bucketsConsidered: Long,
        val bucketsAvailable: Long,
        /** The fetch hit its own cap, so the table has debt this plan cannot see. */
        val truncated: Boolean,
        /** Buckets skipped unprobed under a live bucket claim; see [CompactionPlan.bucketsSkippedClaimed]. */
        val bucketsSkippedClaimed: Long = 0,
        /** See [PlanWithContext.markable]. */
        val markable: Set<Bucket> = emptySet(),
    )

    /** Phase (c)'s output: the groups to attempt, and how many a sibling holds. */
    data class FreeGroups(val groups: List<CompactionGroup>, val claimed: Long)

    /**
     * A bucket claim this run holds, shared by the WorkItems of every
     * group the bucket packed into. [outstanding] counts the groups not
     * yet settled; the one that takes it to zero releases the claim
     * (`CompactionExecutor.settleBucketClaim`).
     */
    class BucketClaim(
        val key: String,
        groups: Int,
    ) {
        val outstanding = java.util.concurrent.atomic.AtomicInteger(groups)
    }

    /** One planned group with the table context its execution needs. */
    data class WorkItem(
        val ctx: TableContext,
        val group: CompactionGroup,
        val claimant: UUID? = null,
        val started: java.util.concurrent.atomic.AtomicBoolean = java.util.concurrent.atomic.AtomicBoolean(false),
        val bucketClaim: BucketClaim? = null,
        /**
         * Every bucket claim the same plan took — this item's own among
         * them — so each group's pre-IO refresh can renew all of them
         * that are still outstanding (`CompactionExecutor.refreshClaim`). Bounded by the
         * plan's queue, so at most `maxGroupsPerRun` keys.
         */
        val planBuckets: List<BucketClaim> = emptyList(),
    )

    data class PlannedWork(val plan: CompactionPlan, val items: List<WorkItem>)
}
