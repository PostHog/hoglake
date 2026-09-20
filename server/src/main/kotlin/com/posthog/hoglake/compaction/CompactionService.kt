package com.posthog.hoglake.compaction

import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CompactionResult
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.model.StatsSanity
import com.posthog.hoglake.model.allNodes
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.persistence.MaintenanceRunStore
import com.posthog.hoglake.persistence.NamespaceRepo
import com.posthog.hoglake.persistence.SnapshotRepo
import com.posthog.hoglake.persistence.SortRepo
import com.posthog.hoglake.persistence.TableRepo
import com.posthog.hoglake.stats.IcebergSingleValue
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
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
    /** Fan-in cap — see CompactionGrouping.groups. */
    val maxInputFiles: Int = CompactionGrouping.DEFAULT_MAX_INPUT_FILES,
    /** Groups rewritten per run per catalog — tiny bites, never a storm. */
    val maxGroupsPerRun: Int,
    /**
     * How far the SORTED path's group budget is derated for a table with
     * nested columns (HOGLAKE_COMPACTION_NESTED_SORT_EXPANSION).
     *
     * The sorted path materializes every survivor of a group as
     * parquet-java `Group` objects so it can sort them — that is what
     * makes sorting safe at all, since the row ids are explicit data
     * rather than position. For NESTED rows the object graph runs far
     * ahead of the bytes: a measured `list<long>` table with five
     * elements per row peaked at 343 MiB of heap from a 4.6 MiB
     * compressed input — **70x** — because every element becomes its own
     * `SimpleGroup` with its own object header, field array and boxed
     * value, and compression that packs an int64 column 10:1 does
     * nothing for object headers.
     *
     * A nested row's node count is not knowable from the catalog (list
     * lengths are data), so [sortedHeapBytes]'s per-node accounting
     * cannot see it. This expansion is what covers the gap: for a table
     * with BOTH nested columns and a live sort order the sorted ROW
     * CEILING is divided by it. 64 is deliberately near the top of the
     * measured 30-70x range — erring large costs smaller compaction
     * groups, erring small costs an OOM in a background loop.
     *
     * It used to divide [targetBytes] directly, on the stated assumption
     * that "for FLAT rows the object graph is a few boxed values per row
     * and targetBytes is a fair proxy for the heap". That assumption was
     * measured in #118 and is false by an order of magnitude
     * (`SortedHeapMeasurement`): a flat 11-column event row costs ~1.7 KiB
     * of materialized heap against ~119 bytes of snappy input, so the
     * "proxy" was already 14x optimistic before #115 made compaction's
     * own zstd output — 1.70x denser — an input in its own right, taking
     * it to 24x. [sortedHeapBytes] is the real bound now;
     * this is the nested multiplier on top of it.
     *
     * NOT a spill implementation, and not a promise. It bounds the
     * SORTED path only, and only per GROUP: one pathological ROW (a
     * million-element list) still materializes whole on either path, and
     * nothing here changes that — that is [maxNodesPerRow]'s job.
     */
    val nestedSortExpansion: Int = DEFAULT_NESTED_SORT_EXPANSION,
    /**
     * How much HEAP one group's sorted-path materialization may take
     * (HOGLAKE_COMPACTION_SORTED_HEAP_BYTES).
     *
     * This is the quantity [targetBytes] was being used as a proxy for,
     * and the two are not the same thing at all: [targetBytes] is how
     * big an OUTPUT FILE should be, measured in compressed bytes on
     * object storage, while this is how much of the JVM heap the sort
     * buffer may occupy. Nothing relates them but the input's density,
     * which is why the conversion between them ([effectiveTargetBytes])
     * has to consult it.
     *
     * # THIS BOUND IS TEMPORARY, AND THE WAY OUT IS KNOWN
     *
     * It exists because the sorted rewrite reads the WHOLE group into an
     * `ArrayList<Group>` and calls `sortedWith`. That is an in-memory
     * sort, so the group has to fit in memory, so the group has to be
     * small — measured, about 34 MiB of zstd input per GiB of sort
     * buffer for a ten-column table. Capping a 512 MiB compaction target
     * at tens of megabytes is a real cost: sorted tables stop reaching
     * the target at all, and no setting of this knob fixes that, it only
     * moves it.
     *
     * The fix is an EXTERNAL MERGE SORT, and compaction is unusually
     * well set up for one:
     *
     *  - **A compaction OUTPUT needs no sort at all.** This rewriter
     *    sorts what it writes (`ParquetRewriter.rewriteInto`, and
     *    schema.sql's sort-spec comment: the spec is BINDING for
     *    compaction rewrites), so such an input is an already-sorted
     *    RUN, and merging k sorted runs needs one row per run in a
     *    priority queue — O(files) live rows, not O(group).
     *  - **A CLIENT-WRITTEN file cannot assume it**, because a client's
     *    sort order is ADVISORY — `schema.sql` says so in as many words,
     *    and the server never verifies file sortedness. But a
     *    client-written file is bounded by the ingest flush size, and
     *    sorting one file alone is bounded by that one file rather than
     *    by the group. Sort each on its own, spill it as a temp run, and
     *    stream-merge the runs like any other.
     *
     *    Note which case now carries the bytes. Under the ladder, most
     *    input was a previous output being carried up a rung, so most
     *    groups were free merges. One-pass compaction consumes each file
     *    once, so nearly every input is client-written and the spill
     *    path is the ordinary one — the external sort is MORE work to
     *    build than it was, and worth more, because it is the only thing
     *    that lets a sorted table reach the target in one rewrite.
     *
     *    NOTE the spill now needs a scratch directory of its own.
     *    `compactGroup` used to create one per group and this plan was
     *    written to borrow it; compaction streams both ends now and
     *    touches no local disk, so whoever builds the external sort owns
     *    that decision — including whether spilling to an emptyDir whose
     *    overrun EVICTS the pod is the right place for it.
     *
     * Do that and group size stops being a heap question entirely — this
     * knob, [sortedRowCeiling], and the `heap_budget` skip all go away.
     * Until then this is the bound that converts an OOM into a counted
     * refusal (hoglake#118).
     *
     * # The default, and the pod it assumes
     *
     * 1 GiB, which is the LARGEST value that is safe on the maintenance
     * pod as it exists today: 4 GiB, so ~2.8 GiB of heap at the image's
     * `MaxRAMPercentage=70` (server/build.gradle.kts). Worst-case peak
     * at 1 GiB is ~1130 MiB — the sort buffer's measured ~0.79x of the
     * declared budget (the 192 B/node constant rounds up from 151.6),
     * plus parquet-java's 128 MiB row-group block, plus the hydrator's
     * 256 MiB whole-object ceiling if it fires in the same tick — which
     * is 39% of that heap.
     *
     * This figure USED to include the group's input and output byte
     * arrays, which scaled with the data. They are gone: compaction
     * streams both ends (S3InputFile / S3OutputFile), so its transport
     * costs one 8 MiB readahead buffer and one 16 MiB part buffer,
     * flat, whatever the group holds. The headroom that frees is
     * unclaimed — this knob was not raised with it.
     * Going higher on a 4 GiB pod spends margin this process does not
     * have.
     *
     * Bigger pods buy proportionally bigger groups, and the arithmetic
     * is linear (server/README.md carries the table). Raising this knob
     * WITHOUT raising the pod converts the counted refusal back into the
     * OOM it replaced.
     */
    val sortedHeapBytes: Long = DEFAULT_SORTED_HEAP_BYTES,
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
    }

    /**
     * How many ROWS of [columns] the sorted path may materialize inside
     * [sortedHeapBytes] — the bound the group budget exists to respect,
     * in the unit the heap actually holds.
     *
     * A materialized row is not its bytes; it is a `SimpleGroup`, a
     * `List<Object>[]` field array, and then an `ArrayList` plus that
     * list's backing `Object[]` plus one boxed value for every populated
     * field. Measured at **151.6 bytes per node** for the flat event
     * shape this catalog holds (`SortedHeapMeasurement`, steady-state
     * retained heap over 200k rows);
     * [SORTED_HEAP_BYTES_PER_NODE] rounds that up, because erring large
     * costs smaller groups and erring small costs an OOM in a background
     * loop — the same asymmetry [nestedSortExpansion] is calibrated on.
     *
     * Nodes are counted off the LIVE schema (every node of the forest,
     * plus one for the `_hog_row_id` carrier every output writes), which
     * is exact for flat tables and a floor for nested ones — hence the
     * [nestedSortExpansion] division, which is the only thing standing in
     * for list lengths the catalog cannot know.
     */
    fun sortedRowCeiling(columns: List<Column>): Long {
        val nodes = columns.allNodes().size + 1L // + the _hog_row_id carrier
        val perRow = SORTED_HEAP_BYTES_PER_NODE * nodes
        val ceiling = sortedHeapBytes / perRow
        val nested = columns.allNodes().any { it.def.type.isNested }
        return maxOf(1L, if (nested) ceiling / nestedSortExpansion else ceiling)
    }

    /**
     * The group byte budget to plan a table under.
     *
     * [targetBytes] for the UNSORTED path, which streams one `Group` at
     * a time and whose heap is therefore flat in group size. For the
     * SORTED path it is the byte size of [sortedRowCeiling] rows AT THIS
     * TABLE'S OBSERVED DENSITY, capped at [targetBytes] — rows are what
     * the heap holds, bytes are what the target is expressed in, and
     * [density] is the only thing that converts between them.
     *
     * Density is why this changed (#118). Group selection reads input
     * file BYTES, and since #115 an input may be compaction's own zstd
     * output rather than a client's snappy: measured 1.70x
     * denser on event data, so the same byte budget started admitting
     * 1.70x the rows, and rows are what become `Group` objects. Deriving
     * the budget FROM the row ceiling makes that self-correcting — denser
     * inputs buy fewer bytes per group, automatically, per table, with no
     * guessed ratio anywhere.
     *
     * It stays a BYTE budget rather than becoming a row budget because
     * grouping is byte-anchored: the candidate filter and the group
     * quota are both sizes. Deriving the budget from the row ceiling
     * keeps the two in one currency instead of bolting a row cap onto a
     * byte quota, which would form groups and then refuse them.
     *
     * Never below 2 — CompactionGrouping refuses a smaller target, and a
     * table whose target derated to nothing would stop compacting.
     * Note this derate is exactly why the group minimum has to scale
     * with file size: it can land the effective target at tens of
     * megabytes, where no five candidate files fit under it.
     * [InputDensity.UNKNOWN] (a table with no candidate rows to measure)
     * falls back to the pre-#118 shape, which is never looser.
     */
    fun effectiveTargetBytes(
        columns: List<Column>,
        sorted: Boolean,
        density: InputDensity = InputDensity.UNKNOWN,
    ): Long {
        if (!sorted) return targetBytes
        // Two bounds, both enforced, tightest wins — they are estimates
        // of the same heap by different routes and neither subsumes the
        // other. The NESTED one is a statement about BYTES (a nested
        // group's graph measured 30-70x its compressed size) and is the
        // only thing that sees list lengths at all. The DENSITY one is a
        // statement about ROWS and is exact for a flat schema. Take the
        // nested bound away and a nested table with few, fat rows plans
        // at the raw target again; take the density bound away and #118
        // comes back.
        val nestedBound =
            if (nestedSortExpansion == 1 || columns.allNodes().none { it.def.type.isNested }) {
                targetBytes
            } else {
                targetBytes / nestedSortExpansion
            }
        val densityBound =
            density.bytesPerRow?.let { bytesPerRow ->
                // Through Double deliberately: ceiling * bytesPerRow is a
                // row count times a per-row size and overflows Long for a
                // sparse table long before it means anything.
                (sortedRowCeiling(columns).toDouble() * bytesPerRow)
                    .coerceIn(0.0, targetBytes.toDouble())
                    .toLong()
            } ?: targetBytes
        return maxOf(2L, minOf(targetBytes, nestedBound, densityBound))
    }

    companion object {
        /**
         * Erring at the top of the measured 30-70x expansion; see
         * [nestedSortExpansion].
         */
        const val DEFAULT_NESTED_SORT_EXPANSION = 64

        /**
         * Heap cost of one materialized parquet-java node, for
         * [sortedRowCeiling].
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
         * See [sortedHeapBytes] — including why this bound is TEMPORARY
         * and what replaces it (an external merge sort, which a group
         * of compaction outputs barely needs, since those are already
         * sorted runs).
         *
         * 1 GiB: the largest value whose worst-case peak (~1260 MiB)
         * still fits the 4 GiB maintenance pod's ~2.8 GiB heap with
         * margin. Bigger pods take a bigger value; the knob is linear in
         * the group bytes it buys and server/README.md has the ladder.
         */
        const val DEFAULT_SORTED_HEAP_BYTES = 1024L * 1024 * 1024
    }
}

/**
 * The two catalog-known quantities the sorted path's heap depends on,
 * summed over a table's compaction candidates: registered file bytes and
 * registered rows.
 *
 * Both are already columns of `hog_data_file` (`file_size_bytes`,
 * `record_count`), so measuring a table's density costs one metadata
 * aggregate and NO object-store IO — which is the reason this rather
 * than the parquet footer's `total_uncompressed_size`. The footer
 * carries the exact ratio per file, but reading it means opening every
 * candidate over S3 on every sweep, and compaction planning is
 * metadata-only by design.
 */
data class InputDensity(val totalBytes: Long, val totalRecords: Long) {
    /**
     * Registered bytes per registered row, or null when the candidates
     * hold no rows (nothing to materialize, so nothing to bound).
     */
    val bytesPerRow: Double?
        get() = if (totalRecords > 0 && totalBytes > 0) totalBytes.toDouble() / totalRecords else null

    companion object {
        /** No candidates measured — callers fall back to the un-derated budget. */
        val UNKNOWN = InputDensity(0, 0)
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
    val statsProvided: Boolean,
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
)

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
     * Groups this plan formed and then REFUSED because their registered
     * survivor count is above the table's sorted-path row ceiling
     * (CompactionConfig.sortedRowCeiling) — they would not fit the heap.
     *
     * Refused HERE, in metadata, rather than discovered by an
     * OutOfMemoryError ninety seconds into a rewrite: record_count is
     * already in the catalog, so the check is exact and free, and a
     * group that cannot fit never spends the IO to find out. The count
     * reaches the run outcome as CompactionResult.heapBudgetExceeded.
     */
    val heapRefusedGroups: Long = 0,
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
 * delete that happened after planning is NEVER dropped.
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
 * ticket while the group is still in flight (it holds no lock between
 * upload and commit), the commit transaction first re-claims the ticket
 * (still undrained?) and aborts the group if cleanup got there first —
 * the object is already gone; re-plan next sweep.
 *
 * Inputs are END-SNAPSHOTTED, never deleted: they remain visible to
 * time travel below the compaction snapshot, and expiry queues their
 * paths (and their dead DVs' paths) for physical removal once
 * end_snapshot falls under the retention floor — exactly the
 * superseded-DV lifecycle. Nothing else enters hog_file_removal here.
 *
 * Stats for the output are aggregated server-side from the inputs'
 * hog_file_column_stats: counts sum; bounds are recomputed from the
 * TYPED decoded bounds (IcebergSingleValue.decode + compareValues +
 * re-encode) — a raw binary min/max of the encodings would be wrong for
 * signed little-endian types. If any input lacks provided stats — or
 * any input has a DV, which makes the inputs' counts wrong for the
 * survivor set — the output registers as 'pending' and the hydrator
 * fills it from the footer.
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
     * Last heap-refusal picture per table, so a permanent condition is
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
    private val lastHeapRefusal = java.util.concurrent.ConcurrentHashMap<Pair<Long, Long>, String>()

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
        /** Per-row node budget for the rewrite (CompactionConfig.maxNodesPerRow). */
        val maxNodesPerRow: Int = ParquetRewriter.DEFAULT_MAX_NODES_PER_ROW,
        /** Output compression for the rewrite (CompactionConfig.codec). */
        val codec: ParquetRewriter.OutputCodec = ParquetRewriter.OutputCodec(),
    ) {
        /**
         * Live column types by field id (stats aggregation), over EVERY
         * node of the column forest — not just the top level.
         *
         * Stats are keyed on LEAF field ids, and a leaf inside a struct,
         * a list or a map is not a top-level column. Built from the
         * top-level list alone, the merge silently dropped every nested
         * leaf's stats row on the way through compaction: counts and
         * bounds present before the rewrite, gone after it, with nothing
         * anywhere saying so.
         */
        val columnTypes: Map<Long, ColType>
            get() = columns.allNodes().associate { it.fieldId to it.def.type }
    }

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

    /** Public metadata-only planning for one table (also the test surface). */
    fun planTable(
        catalog: String,
        namespace: String,
        table: String,
        cfg: CompactionConfig = defaults,
    ): CompactionPlan = planSnapshot(catalog, namespace, table, cfg).plan

    /** Schema, head and input files must come from the SAME MVCC view. */
    private fun planSnapshot(
        catalog: String,
        namespace: String,
        table: String,
        cfg: CompactionConfig,
    ): PlanWithContext =
        jdbi.inTransactionUnchecked { h ->
            h.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            planWithContext(h, catalog, namespace, table, cfg)
        }

    private fun planWithContext(
        h: Handle,
        catalog: String,
        namespace: String,
        table: String,
        cfg: CompactionConfig,
    ): PlanWithContext {
        val cat =
            CatalogRepo.findByName(h, catalog)
                ?: throw HoglakeException.NotFound("catalog '$catalog'")
        val ns =
            NamespaceRepo.findLiveByName(h, cat.catalogId, namespace)
                ?: throw HoglakeException.NotFound("namespace '$namespace' in catalog '$catalog'")
        val t =
            TableRepo.findLive(h, cat.catalogId, ns.namespaceId, table)
                ?: throw HoglakeException.NotFound("table '$namespace.$table' in catalog '$catalog'")
        val ctx =
            TableContext(
                catalogId = cat.catalogId,
                dataPath = cat.dataPath,
                namespace = ns.name,
                table = t.name,
                tableId = t.tableId,
                columns = TableRepo.columnsAt(h, cat.catalogId, t.tableId, cat.headSnapshotId),
                sortFields =
                    SortRepo.sortSpecAt(h, cat.catalogId, t.tableId, cat.headSnapshotId)
                        ?.fields ?: emptyList(),
                maxNodesPerRow = cfg.maxNodesPerRow,
                codec = cfg.codec,
            )
        val planned = groups(h, ctx, cfg)
        return PlanWithContext(
            ctx,
            CompactionPlan(t.tableId, ns.name, t.name, planned.groups, planned.heapRefused),
        )
    }

    /** What [groups] produced: what will be attempted, and what the heap ceiling refused. */
    private data class PlannedGroups(
        val groups: List<CompactionGroup>,
        val heapRefused: Long,
    )

    private fun groups(
        h: Handle,
        ctx: TableContext,
        cfg: CompactionConfig,
    ): PlannedGroups {
        // The scalar rewriter cannot preserve VARIANT groups yet. Do not enqueue
        // work that could drop payloads or repeatedly fail the maintenance loop.
        // allNodes, not the top level: a variant nested inside a struct
        // is still a variant the rewriter cannot write, and `struct{v:
        // variant}` has no top-level one. #77's check predates
        // containers, where the two were the same question.
        if (ctx.columns.allNodes().any { it.def.type == ColType.VARIANT }) {
            return PlannedGroups(emptyList(), 0)
        }

        data class Bucket(val specId: Long?, val values: List<String?>?)

        data class Row(val candidate: CompactionCandidate, val bucket: Bucket)

        val sorted = ctx.sortFields.isNotEmpty()
        // The DERATED budget, not the raw one: a sorted table's group has
        // to stay small enough that materializing it to sort fits in heap
        // (CompactionConfig.effectiveTargetBytes), which depends on how
        // many ROWS its bytes carry — so the density is measured first,
        // from the same MVCC snapshot, in metadata. It narrows the
        // candidate filter too: a file above the derated target can never
        // fill a group under it, so fetching it would only be work.
        val budget = cfg.effectiveTargetBytes(ctx.columns, sorted, density(h, ctx, cfg))

        val rows =
            h.createQuery(
                """
            SELECT f.data_file_id, f.path, f.record_count, f.file_size_bytes,
                   f.footer_size, f.row_id_start, f.spec_id, f.stats_state,
                   f.explicit_row_ids,
                   dv.delete_file_id AS dv_id, dv.path AS dv_path, dv.delete_count AS dv_count,
                   (SELECT array_agg(pv.value ORDER BY pv.key_index)
                    FROM hog_file_partition_value pv
                    WHERE pv.catalog_id = f.catalog_id
                      AND pv.data_file_id = f.data_file_id) AS partition_values
            FROM hog_data_file f
            LEFT JOIN hog_delete_file dv
              ON dv.catalog_id = f.catalog_id
             AND dv.data_file_id = f.data_file_id
             AND dv.end_snapshot IS NULL
            WHERE f.catalog_id = :catalogId AND f.table_id = :tableId
              AND f.end_snapshot IS NULL
              AND f.file_size_bytes < :targetBytes
            ORDER BY f.row_id_start, f.data_file_id
            """,
            )
                .bind("catalogId", ctx.catalogId)
                .bind("tableId", ctx.tableId)
                .bind("targetBytes", budget)
                .map { rs, _ ->
                    Row(
                        CompactionCandidate(
                            dataFileId = rs.getLong("data_file_id"),
                            path = rs.getString("path"),
                            recordCount = rs.getLong("record_count"),
                            fileSizeBytes = rs.getLong("file_size_bytes"),
                            footerSize = rs.getObject("footer_size", java.lang.Long::class.java)?.toLong(),
                            rowIdStart = rs.getLong("row_id_start"),
                            statsProvided = rs.getString("stats_state") == "provided",
                            explicitRowIds = rs.getBoolean("explicit_row_ids"),
                            dv =
                                rs.getObject("dv_id")?.let {
                                    LiveDv(
                                        deleteFileId = (it as Number).toLong(),
                                        path = rs.getString("dv_path"),
                                        deleteCount = rs.getLong("dv_count"),
                                    )
                                },
                        ),
                        Bucket(
                            specId = rs.getObject("spec_id")?.let { (it as Number).toLong() },
                            values =
                                (rs.getArray("partition_values")?.array as? Array<*>)
                                    ?.map { it as String? },
                        ),
                    )
                }
                .list()

        val grouping = CompactionGrouping.of(budget)
        val out = mutableListOf<CompactionGroup>()
        for ((bucket, bucketRows) in rows.groupBy { it.bucket }) {
            val candidates = bucketRows.map { it.candidate }
            val takes =
                grouping.groups(
                    candidates,
                    cfg.minInputFiles,
                    cfg.maxInputFiles,
                ) { it.fileSizeBytes }
            for (take in takes) {
                out += CompactionGroup(take, bucket.specId, bucket.values)
            }
        }
        // The EXACT check, after the estimate. The budget above scales the
        // target by the table's AVERAGE density, which is an estimate;
        // hog_data_file.record_count is not, so a group whose registered
        // survivors are above the ceiling is refused here on the true
        // number rather than attempted and discovered by an OOM. Only the
        // sorted path materializes, so only it has a ceiling.
        val ceiling = if (sorted) cfg.sortedRowCeiling(ctx.columns) else Long.MAX_VALUE
        val (fits, refused) = out.partition { it.survivingRecords <= ceiling }
        // A table at the row ceiling is a STATE, not an event: the same
        // groups are re-planned and re-refused on every sweep, forever,
        // until an operator raises the heap or drops the sort order.
        // Logging it per sweep buried a burn-in in 289 identical
        // warnings -- 235 KB -- in three minutes for a single table. Log
        // when the picture CHANGES; the metric below carries the rest.
        if (refused.isEmpty()) {
            lastHeapRefusal.remove(ctx.catalogId to ctx.tableId)
        }
        val signature = "${refused.size}/${refused.maxOfOrNull { it.survivingRecords } ?: 0}/$ceiling"
        if (refused.isNotEmpty() && lastHeapRefusal.put(ctx.catalogId to ctx.tableId, signature) != signature) {
            log.warn {
                "compaction refused ${refused.size} planned group(s) of " +
                    "${ctx.namespace}.${ctx.table}: the sorted path would materialize up to " +
                    "${refused.maxOf { it.survivingRecords }} rows against a ceiling of " +
                    "$ceiling (heap budget ${cfg.sortedHeapBytes} B). The table keeps its debt. " +
                    "Levers today: raise HOGLAKE_COMPACTION_SORTED_HEAP_BYTES together with the " +
                    "pod's memory (the default is sized for a 4 GiB pod), or drop the table's " +
                    "sort order to move it to the streaming path, whose heap is flat in group " +
                    "size. This ceiling is TEMPORARY: the sorted rewrite sorts the whole group " +
                    "in memory, and replacing that with an external merge sort removes it — " +
                    "a group of compaction outputs is a merge of already-sorted runs, so merging " +
                    "them needs one row per input rather than all of them. See " +
                    "CompactionConfig.sortedHeapBytes"
            }
        }
        // Most files first: every group now targets the same size, so the
        // one holding the most files buys the largest drop in file count
        // for the same bytes rewritten. Row-id order breaks ties, which
        // keeps a group's inputs adjacent in arrival order.
        return PlannedGroups(
            fits
                .sortedWith(compareBy({ -it.files.size }, { it.files.first().rowIdStart })),
            refused.size.toLong(),
        )
    }

    /**
     * Registered bytes and rows over the table's candidate population —
     * one metadata aggregate on the same REPEATABLE READ snapshot as the
     * candidate read, no object-store IO.
     *
     * Filtered by the RAW [CompactionConfig.targetBytes], not the derated
     * budget, because the derate is what this is being measured to
     * compute. It is a superset of the eventual candidate set, which
     * makes the density a whole-table average: a table holding both
     * client snappy and compaction zstd measures between the two, and
     * the exact per-group row check in
     * [groups] is what covers the residual.
     */
    private fun density(
        h: Handle,
        ctx: TableContext,
        cfg: CompactionConfig,
    ): InputDensity =
        h.createQuery(
            """
            SELECT COALESCE(SUM(f.file_size_bytes), 0) AS total_bytes,
                   COALESCE(SUM(f.record_count), 0) AS total_records
            FROM hog_data_file f
            WHERE f.catalog_id = :catalogId AND f.table_id = :tableId
              AND f.end_snapshot IS NULL
              AND f.file_size_bytes < :targetBytes
            """,
        )
            .bind("catalogId", ctx.catalogId)
            .bind("tableId", ctx.tableId)
            .bind("targetBytes", cfg.targetBytes)
            .map { rs, _ -> InputDensity(rs.getLong("total_bytes"), rs.getLong("total_records")) }
            .one()

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
                    "invalid_data=${r.invalidData} heap_budget_exceeded=${r.heapBudgetExceeded} " +
                    "failed_groups=${r.failedGroups}"
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
            Metrics.compactionSkipped(catalog, "heap_budget", result.heapBudgetExceeded)
            // The red-flag outcome, and it had no series either.
            Metrics.compactionSkipped(catalog, "failed", result.failedGroups)
            result
        }

    private fun doRunOnce(
        catalog: String,
        cfg: CompactionConfig,
    ): CompactionResult {
        val tables = jdbi.withHandleUnchecked { h -> liveTables(h, catalog) }
        var groupsCompacted = 0L
        var filesIn = 0L
        var bytesIn = 0L
        var bytesOut = 0L
        var skipped = 0L
        var dvSuperseded = 0L
        var unconvertible = 0L
        var invalidData = 0L
        var heapBudgetExceeded = 0L
        var failed = 0L

        fun budgetSpent() =
            groupsCompacted + skipped + dvSuperseded + unconvertible + invalidData + failed >=
                cfg.maxGroupsPerRun
        outer@ for ((namespace, table) in tables) {
            if (budgetSpent()) break
            val (ctx, plan) = planSnapshot(catalog, namespace, table, cfg)
            // Groups the heap ceiling refused in METADATA. Counted but
            // deliberately NOT charged to maxGroupsPerRun: every other
            // skip flavor spends the group's IO before it resolves, and
            // this one spends none, so letting it consume the run's one
            // slot would let a single un-compactable table starve every
            // other table of the sweep forever.
            heapBudgetExceeded += plan.heapRefusedGroups
            for (group in plan.groups) {
                if (budgetSpent()) break@outer
                try {
                    when (val outcome = compactGroup(ctx, group)) {
                        is GroupOutcome.Committed -> {
                            groupsCompacted++
                            filesIn += group.files.size
                            bytesIn += group.totalBytes
                            bytesOut += outcome.bytesOut
                        }
                        GroupOutcome.SkippedConflict -> skipped++
                        GroupOutcome.SkippedDvSuperseded -> dvSuperseded++
                    }
                } catch (e: UnconvertibleSchemaException) {
                    // Skip-with-reason, not a failure: the group stays
                    // uncompacted until the schema or the file set moves.
                    log.warn {
                        "compaction group of ${group.files.size} files for " +
                            "$catalog/$namespace.$table is not convertible to the live " +
                            "schema (${e.message}); skipping"
                    }
                    unconvertible++
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
                            "$catalog/$namespace.$table cannot be rewritten as registered " +
                            "(${e.message}); skipping"
                    }
                    invalidData++
                } catch (e: OutOfMemoryError) {
                    // The ceiling in `groups` is supposed to make this
                    // unreachable; reaching it means the per-node heap
                    // estimate is wrong for this table's shape, which is
                    // an operator signal, not a retry.
                    //
                    // Caught at the GROUP boundary because nothing else
                    // caught it at all: `catch (e: Exception)` below does
                    // not match an Error, so the OOM used to unwind the
                    // whole sweep — losing every other table's accounting
                    // and leaving the run ledger a bare "Java heap space"
                    // with no counters (hoglake#118). Catching an OOM is
                    // only defensible because the allocation it aborts is
                    // one ArrayList of Groups that is unreachable the
                    // instant this frame unwinds.
                    //
                    // And then the sweep STOPS. Continuing would allocate
                    // the next group's inputs into a heap that just
                    // proved it has none to spare.
                    heapBudgetExceeded++
                    log.error(e) {
                        "compaction group of ${group.files.size} files " +
                            "(${group.survivingRecords} survivors) exhausted the heap for " +
                            "$catalog/$namespace.$table despite a row ceiling of " +
                            "${cfg.sortedRowCeiling(ctx.columns)}; ending the sweep. The " +
                            "per-node heap estimate is too small for this table's shape — " +
                            "lower HOGLAKE_COMPACTION_SORTED_HEAP_BYTES or raise the heap. " +
                            "Both are workarounds for an in-memory group sort that should be " +
                            "an external merge sort; see CompactionConfig.sortedHeapBytes"
                    }
                    break@outer
                } catch (e: Exception) {
                    // One bad group (unreadable input, corrupt DV, S3
                    // hiccup) never wedges the sweep — but it IS counted:
                    // an uncounted swallow is a silently-dead compactor
                    // with a green run ledger (the NoSuchBucket incident).
                    failed++
                    log.error(e) {
                        "compaction group of ${group.files.size} files failed for " +
                            "$catalog/$namespace.$table; continuing"
                    }
                }
            }
        }
        return CompactionResult(
            groupsCompacted = groupsCompacted,
            filesIn = filesIn,
            filesOut = groupsCompacted,
            bytesIn = bytesIn,
            bytesOut = bytesOut,
            skippedConflicts = skipped,
            dvSuperseded = dvSuperseded,
            unconvertibleSchema = unconvertible,
            invalidData = invalidData,
            heapBudgetExceeded = heapBudgetExceeded,
            failedGroups = failed,
        )
    }

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
        val ctx = planSnapshot(catalog, namespace, table, defaults).ctx
        return compactGroup(ctx, group)
    }

    /**
     * Rewrite one group (all IO outside any transaction) and commit it.
     */
    private fun compactGroup(
        ctx: TableContext,
        group: CompactionGroup,
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
                ParquetRewriter.Input(source, f.path, f.rowIdStart, dv, f.explicitRowIds)
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
        val rewritten =
            ParquetRewriter.rewrite(
                inputs,
                ctx.columns,
                ctx.sortFields,
                sink,
                ctx.maxNodesPerRow,
                ctx.codec,
            )
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

        val stats =
            if (group.files.all { it.statsProvided && it.dv == null }) {
                aggregateStats(group.files.map { it.dataFileId }, ctx)
            } else {
                // A DV'd input's registered counts describe pre-delete
                // rows; honest 'pending' beats wrong 'provided'.
                null
            }
        val outcome =
            commitGroup(
                ctx, group, outputPath, outputBytes, footerSize, stats,
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
     */
    private fun commitGroup(
        ctx: TableContext,
        group: CompactionGroup,
        outputPath: String,
        outputBytes: Long,
        footerSize: Long,
        stats: List<ColumnStats>?,
        survivors: Long,
        rowIdStart: Long,
        stagingId: Long,
    ): GroupOutcome =
        jdbi.inTransactionUnchecked { h ->
            Locks.acquireCatalogCommitLock(h, ctx.catalogId)

            // Re-claim the staging ticket under the lock: cleanup drains
            // under the SAME lock, so "still undrained" here means the
            // uploaded object still exists and is ours to register.
            val ticketLive =
                h.createQuery(
                    """
                SELECT (drained_at IS NULL) FROM hog_file_removal
                WHERE catalog_id = :catalogId AND removal_id = :removalId
                """,
                )
                    .bind("catalogId", ctx.catalogId)
                    .bind("removalId", stagingId)
                    .mapTo(Boolean::class.javaObjectType)
                    .findOne()
                    .orElse(false)
            if (!ticketLive) {
                // The object is NOT necessarily gone, and that is a
                // consequence of streaming the output. The old ordering
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
                    "compaction staging ticket $stagingId for $outputPath was drained by " +
                        "cleanup before the group committed; aborting the group and " +
                        "re-staging the path so the object cannot outlive its ticket"
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
                                           row_id_start, stats_state, spec_id, explicit_row_ids)
                VALUES (:catalogId, :dataFileId, :tableId, :beginSnapshot,
                        :path, :recordCount, :fileSizeBytes, :footerSize,
                        :rowIdStart, :statsState, :specId, true)
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
            h.createUpdate(
                """
                UPDATE hog_data_file SET end_snapshot = :snapshotId
                WHERE catalog_id = :catalogId AND data_file_id IN (<ids>)
                """,
            )
                .bind("snapshotId", snapshotId)
                .bind("catalogId", ctx.catalogId)
                .bindList("ids", ids)
                .execute()

            // The applied DVs die with their files: end-snapshot them so
            // scans at older snapshots still mask, and expiry queues the
            // puffin paths alongside the input parquets.
            val dvIds = group.files.mapNotNull { it.dv?.deleteFileId }
            if (dvIds.isNotEmpty()) {
                h.createUpdate(
                    """
                    UPDATE hog_delete_file SET end_snapshot = :snapshotId
                    WHERE catalog_id = :catalogId AND delete_file_id IN (<ids>)
                    """,
                )
                    .bind("snapshotId", snapshotId)
                    .bind("catalogId", ctx.catalogId)
                    .bindList("ids", dvIds)
                    .execute()
            }

            // Settle the staging ticket in the SAME transaction that makes
            // the path live: cleanup's drain and the commit path guard only
            // act on UNDRAINED rows, so the registered output can never be
            // reclaimed or refused.
            val settled =
                h.createUpdate(
                    """
                    UPDATE hog_file_removal
                       SET drained_at = now(), drained_outcome = 'registered',
                           last_attempt_at = now()
                     WHERE catalog_id = :catalogId AND removal_id = :removalId
                       AND drained_at IS NULL
                    """,
                )
                    .bind("catalogId", ctx.catalogId)
                    .bind("removalId", stagingId)
                    .execute()
            check(settled == 1) { "compaction staging ticket $stagingId vanished mid-commit" }

            GroupOutcome.Committed(snapshotId, outputBytes)
        }

    // ---- stats aggregation -----------------------------------------------

    /**
     * Merge the inputs' per-column stats into the output's: counts sum;
     * bounds are recomputed by DECODING each input bound to its typed
     * value, comparing typed, and re-encoding the winner — never a raw
     * binary compare of the encodings (wrong for signed little-endian
     * types). A field only gets a stats row when EVERY input has one
     * (heterogeneous groups: an added column simply has no row); within
     * a field, nan/size/bounds go null if any input's is null.
     */
    private fun aggregateStats(
        inputIds: List<Long>,
        ctx: TableContext,
    ): List<ColumnStats> {
        data class StatsRow(
            val fieldId: Long,
            val valueCount: Long,
            val nullCount: Long,
            val nanCount: Long?,
            val sizeBytes: Long?,
            val lower: ByteArray?,
            val upper: ByteArray?,
        )

        val rows =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                SELECT field_id, value_count, null_count, nan_count, size_bytes,
                       lower_bound, upper_bound
                FROM hog_file_column_stats
                WHERE catalog_id = :catalogId AND data_file_id IN (<ids>)
                """,
                )
                    .bind("catalogId", ctx.catalogId)
                    .bindList("ids", inputIds)
                    .map { rs, _ ->
                        StatsRow(
                            fieldId = rs.getLong("field_id"),
                            valueCount = rs.getLong("value_count"),
                            nullCount = rs.getLong("null_count"),
                            nanCount = rs.getObject("nan_count")?.let { (it as Number).toLong() },
                            sizeBytes = rs.getObject("size_bytes")?.let { (it as Number).toLong() },
                            lower = rs.getBytes("lower_bound"),
                            upper = rs.getBytes("upper_bound"),
                        )
                    }
                    .list()
            }

        val columnTypes = ctx.columnTypes
        val out = mutableListOf<ColumnStats>()
        for ((fieldId, fieldRows) in rows.groupBy { it.fieldId }) {
            if (fieldRows.size != inputIds.size) continue // not every input covered the field
            val type = columnTypes[fieldId] ?: continue // column dropped since the inputs landed
            // REPAIR ON READ, before the merge. Every row here was
            // stored before StatsSanity existed or came through a door
            // that predates it, so an inverted pair or an impossible
            // count can already be sitting in the table — and a merge
            // takes min(lowers) and max(uppers), which carries the
            // damage into a BRAND NEW file and keeps it live for
            // another compaction generation. Repairing the inputs as
            // they are read stops the propagation without a migration;
            // a backfill of the historical rows is a separate operation
            // (noted as a follow-up, deliberately not done here — it
            // rewrites rows for files nothing is compacting).
            val clean =
                fieldRows.map { r ->
                    sane(
                        ColumnStats(
                            fieldId = fieldId,
                            valueCount = r.valueCount,
                            nullCount = r.nullCount,
                            nanCount = r.nanCount,
                            sizeBytes = r.sizeBytes,
                            lowerBound = r.lower,
                            upperBound = r.upper,
                        ),
                        type,
                        "input of ${ctx.namespace}.${ctx.table}",
                    )
                }
            // And again on the MERGE: the sum of sound inputs is not
            // automatically sound (bounds merged from files with
            // different live types, counts that overflow their
            // relationship), and this is the row that gets stored.
            out +=
                sane(
                    ColumnStats(
                        fieldId = fieldId,
                        valueCount = clean.sumOf { it.valueCount },
                        nullCount = clean.sumOf { it.nullCount },
                        nanCount =
                            if (clean.any { it.nanCount == null }) null else clean.sumOf { it.nanCount!! },
                        sizeBytes =
                            if (clean.any { it.sizeBytes == null }) null else clean.sumOf { it.sizeBytes!! },
                        lowerBound = mergeBound(type, clean.map { it.lowerBound }, takeUpper = false),
                        upperBound = mergeBound(type, clean.map { it.upperBound }, takeUpper = true),
                    ),
                    type,
                    "merged output for ${ctx.namespace}.${ctx.table}",
                )
        }
        return out.sortedBy { it.fieldId }
    }

    /**
     * One stats row through [StatsSanity], warning + counting any
     * repair under the `compaction` source.
     *
     * The THIRD door. The commit path and the hydrator each ran this
     * rule; compaction wrote `hog_file_column_stats` directly, so a
     * malformed row could be merged into a new file's metadata and stay
     * live — and every reader prunes on it.
     */
    private fun sane(
        stats: ColumnStats,
        type: ColType,
        where: String,
    ): ColumnStats {
        val checked = StatsSanity.check(stats, type)
        // The sanitizer's output is stored whether or not it reported
        // anything: signed-zero canonicalization is a conformance
        // rewrite, not a repair, so it carries no warning and no metric.
        if (checked.repairs.isNotEmpty()) {
            Metrics.statsRepaired("compaction")
            log.warn {
                "column stats for field_id ${stats.fieldId} in the $where are not internally " +
                    "consistent (${checked.repairs.joinToString("; ")}); using the repaired row"
            }
        }
        return checked.stats
    }

    private fun mergeBound(
        type: ColType,
        bounds: List<ByteArray?>,
        takeUpper: Boolean,
    ): ByteArray? {
        if (bounds.any { it == null }) return null
        // A bound that does not decode under the LIVE type (wrong width —
        // e.g. a 4-byte int bound left behind by a pre-fix promote, or one
        // a racing hydrator wrote under the pre-promote type) is treated
        // as ABSENT, nulling this column's merged bound: honest missing
        // metadata over a poison group that would throw here every sweep
        // until the inputs expire. The sweep must never wedge on stats.
        val decoded =
            bounds.map { bound ->
                try {
                    IcebergSingleValue.decode(type, bound!!)
                } catch (e: IllegalArgumentException) {
                    log.warn {
                        "compaction bound-merge: input bound (${bound!!.size} bytes) does not " +
                            "decode as ${type.wire} (${e.message}); treating as absent"
                    }
                    return null
                }
            }
        val winner =
            decoded.reduce { a, b ->
                val cmp = IcebergSingleValue.compareValues(type, a, b)
                if ((cmp < 0) != takeUpper) a else b
            }
        return IcebergSingleValue.encode(type, winner)
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
            } catch (e: Exception) {
                log.error(e) { "compaction sweep failed for catalog '$name'; continuing" }
            }
        }
        return results
    }
}
