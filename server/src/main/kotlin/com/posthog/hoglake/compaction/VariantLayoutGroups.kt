package com.posthog.hoglake.compaction

import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.schema.GroupType
import java.util.concurrent.ConcurrentHashMap

/**
 * How a planned group of a VARIANT table is rewritten when its inputs do
 * not all hold one variant layout — per layout, not refused.
 *
 * # Why a group can hold two layouts
 *
 * The rewrite copies each variant's storage positionally into the
 * output's subtree ([VariantLayout]), so every input of ONE rewrite must
 * hold the subtree the output is written with. A declaration change
 * (`set_variant_shredding`) leaves every partition with files in the old
 * layout and files in the new; a writer that registered files it laid
 * out itself (`prepare_append_files`) leaves its own. Refusing such a
 * group, as the rewrite did, refused it again every sweep for as long as
 * both kinds of file lived in the partition — forever, on a partition
 * that had stopped receiving writes — and each refusal spent one of the
 * sweep's `maxGroupsPerRun` slots, so one changed declaration could
 * starve a catalog.
 *
 * # What happens instead
 *
 * Before a group of a variant table is rewritten, the service reads
 * every input's footer (the same tail read the rewrite's open would do;
 * the footer is handed on, [ParquetRewriter.Input.footer], so it is not
 * read twice) and takes the layout of each variant column off it
 * ([ParquetRewriter.variantLayoutsOf]). [split] then partitions the
 * inputs by how they DEVIATE from the declaration — the variant columns
 * whose subtree is not the declared one, with that subtree — and judges
 * each partition by the SAME rule the planner packed the group by
 * ([CompactionGrouping.groups]): a partition worth a rewrite becomes a
 * [LayoutPart], rewritten on its own with its output in ITS layout — the
 * declared layout when its inputs deviate in nothing, which is what every
 * writer honouring the catalog produces, and so what the table converges
 * on as old files age out; another layout still compacts, into that
 * layout. A partition too small is left alone, uncompacted and
 * unrefused, until more files of its layout arrive; a file DuckDB wrote
 * is excluded on its own (Trino reads its top-level Variant nulls as SQL
 * NULL by `created_by`, which an output does not keep) and never blocks
 * the rest.
 *
 * Keyed by DEVIATIONS rather than the whole layout tuple so that a file
 * is not kept from its kind by a column it does not hold: a file written
 * before a variant column was added has no subtree for it, the rewrite
 * null-fills the column in whatever layout the output takes, and so the
 * file belongs with every file that deviates as it does, whether or not
 * they hold the newer column.
 *
 * # Not starving the sweep
 *
 * A group whose every partition is too small spends its slot and
 * compacts nothing, and would do so again every sweep: the planner packs
 * in metadata and cannot see a layout. [VariantLayoutMemory] is what
 * stops the repeat: the probe remembers each file's deviations as a
 * fingerprint, and the planner packs files it remembers as deviating, or
 * as DuckDB's, apart from the rest — a remembered remainder forms no
 * group until enough files of its layout are known, and a DuckDB file
 * forms none. Files it does not remember (every new arrival, every file
 * after a restart) pack with the declared layout's, since that is what a
 * writer honouring the catalog produces: the probe is paid once per file
 * per process lifetime, and only a group whose files turn out to be mixed
 * spends a slot on it. A probe that found nothing to rewrite is still
 * charged to the sweep as one attempt (`layout_probe_only`): it read
 * every footer.
 *
 * What this is NOT: a codec. No variant is decoded or re-shredded; a
 * layout the declaration no longer names is copied as it is.
 */
internal object VariantLayoutGroups {
    /** One input as the probe read it. */
    class Probed(
        val file: CompactionCandidate,
        /** The footer the probe read, kept for the rewrite's open when it fits [FOOTER_RETENTION_BYTES]. */
        val footer: ParquetMetadata?,
        /** Every variant column's layout in this file, by field id ([ParquetRewriter.variantLayoutsOf]). */
        val layouts: Map<Long, GroupType>,
        /** The file's `created_by` names DuckDB. */
        val duckdb: Boolean,
    )

    /**
     * One rewrite: [group]'s inputs all hold [layouts] (empty = the
     * declared layouts; a table with no variant, or a group that was not
     * probed), which its output is written in. [deviations] is the part of
     * [layouts] that is not the declaration's: empty when the output is in
     * the declared layout. [footers] by data_file_id, for the inputs whose
     * footer the probe kept; [dropFooters] releases them once the part's
     * rewrite is over, so a sweep holds at most one part's footers beyond
     * the probe.
     */
    class LayoutPart(
        val group: CompactionGroup,
        val layouts: Map<Long, GroupType>,
        val deviations: Map<Long, GroupType>,
        footers: Map<Long, ParquetMetadata> = emptyMap(),
    ) {
        private val retained = HashMap(footers)

        val footers: Map<Long, ParquetMetadata> get() = retained

        val declared: Boolean get() = deviations.isEmpty()

        fun dropFooters() = retained.clear()
    }

    /** What [split] made of a probed group. */
    class Split(
        /** The rewrites to run, declared layout first, then most files first. */
        val parts: List<LayoutPart>,
        /** Inputs whose layout has too few files in this group to be worth a rewrite: left as they are. */
        val leftBehind: List<Probed>,
        /** Inputs DuckDB wrote, excluded from every part. */
        val duckdb: List<Probed>,
        /** Distinct layouts among the inputs that are not DuckDB's. */
        val layoutCount: Int,
    ) {
        /** Whether the group held more than one layout, or a DuckDB file beside the rest: the probe changed the plan. */
        val mixed: Boolean get() = layoutCount > 1 || (duckdb.isNotEmpty() && layoutCount > 0)
    }

    /**
     * The variant columns of [layouts] whose subtree is not [declared]'s,
     * with that subtree: what [split] partitions by and what
     * [VariantLayoutMemory] remembers. Empty for a file in the declared
     * layout, a column it predates included.
     */
    fun deviations(
        layouts: Map<Long, GroupType>,
        declared: Map<Long, GroupType>,
    ): Map<Long, GroupType> = layouts.filter { (fieldId, layout) -> declared[fieldId] != layout }

    /**
     * Partition [group]'s [probed] inputs by their [deviations] from
     * [declared] and judge each partition by [grouping]'s rule with
     * [minInputFiles], as [pack] judged the group. A group whose inputs
     * all deviate alike and none of which DuckDB wrote is returned as the
     * one part it was planned as, unchanged; nothing is re-packed that the
     * planner already packed.
     *
     * The fan-in cap for a re-judged partition is the group's own size:
     * the planner already applied the bucket's cap, and a partition is
     * never larger than its group.
     */
    fun split(
        group: CompactionGroup,
        probed: List<Probed>,
        declared: Map<Long, GroupType>,
        grouping: CompactionGrouping,
        minInputFiles: Int,
    ): Split {
        require(probed.size == group.files.size) { "every input of the group is probed exactly once" }
        val (duck, rest) = probed.partition { it.duckdb }
        val byDeviation = LinkedHashMap<LayoutKey, MutableList<Probed>>()
        for (p in rest) byDeviation.getOrPut(LayoutKey(deviations(p.layouts, declared))) { mutableListOf() } += p
        if (duck.isEmpty() && byDeviation.size == 1) {
            val deviations = byDeviation.keys.single().layouts
            return Split(
                listOf(LayoutPart(group, declared + deviations, deviations, footersOf(probed))),
                emptyList(),
                emptyList(),
                1,
            )
        }
        val parts = mutableListOf<LayoutPart>()
        val leftBehind = mutableListOf<Probed>()
        for ((key, members) in byDeviation) {
            val deviations = key.layouts
            val packed =
                grouping.groups(
                    members,
                    minInputFiles,
                    maxOf(minInputFiles, group.files.size),
                ) { it.file.fileSizeBytes }
            val taken = HashSet<Long>()
            for (take in packed) {
                take.forEach { taken += it.file.dataFileId }
                parts +=
                    LayoutPart(
                        CompactionGroup(
                            take.map { it.file }.sortedWith(compareBy({ it.rowIdStart }, { it.dataFileId })),
                            group.specId,
                            group.partitionValues,
                        ),
                        declared + deviations,
                        deviations,
                        footersOf(take),
                    )
            }
            leftBehind += members.filter { it.file.dataFileId !in taken }
        }
        // The declared layout first: when the sweep stops early (heap, an
        // interrupt) the rewrite that moves data toward the catalog's
        // layout is the one that ran. Then most files first, as the
        // planner orders its groups.
        parts.sortWith(compareBy({ !it.declared }, { -it.group.files.size }, { it.group.files.first().rowIdStart }))
        return Split(parts, leftBehind, duck, byDeviation.size)
    }

    /**
     * A layout tuple as a map key. Equality is the tuple's own
     * (`GroupType.equals`, the relation `layoutMismatch` decides); the hash
     * is over the rendered groups, because parquet-java's
     * `VariantLogicalTypeAnnotation` overrides `equals` without
     * `hashCode`, so a `GroupType` carrying one hashes by identity and two
     * equal layouts read off two footers would land in two buckets.
     */
    private class LayoutKey(val layouts: Map<Long, GroupType>) {
        private val hash =
            layouts.entries.sumOf {
                    (fieldId, layout) ->
                fieldId.hashCode() * 31 + layout.toString().hashCode()
            }

        override fun equals(other: Any?): Boolean = other is LayoutKey && other.layouts == layouts

        override fun hashCode(): Int = hash
    }

    /** The footers to hand the rewrite, by data_file_id, of the inputs that kept one (see [FOOTER_RETENTION_BYTES]). */
    private fun footersOf(inputs: List<Probed>): Map<Long, ParquetMetadata> =
        inputs.mapNotNull { p -> p.footer?.let { p.file.dataFileId to it } }.toMap()

    /**
     * The serialized length of a file's footer, as an upper bound read off
     * the footer itself: everything past the last row group's last column
     * chunk, less the 8-byte trailer. Page indexes or bloom filters stored
     * between the data and the footer are counted in, which errs toward
     * keeping fewer footers. Not the registered `footer_size`, which is a
     * writer-supplied hint (`S3InputFile`) and never enforced.
     */
    fun footerBytes(
        footer: ParquetMetadata,
        fileSizeBytes: Long,
    ): Long {
        val dataEnd =
            footer.blocks.maxOfOrNull { block ->
                block.columns.maxOfOrNull { it.startingPos + it.totalSize } ?: block.startingPos
            } ?: 0L
        return (fileSizeBytes - dataEnd - 8).coerceAtLeast(0)
    }

    /**
     * How much serialized footer ([footerBytes]) one group's probe keeps
     * in heap for the rewrite to reuse. The probe must read every footer
     * before any rewrite begins — which inputs share a layout is only
     * known once all are read — so reusing them means holding them across
     * the group, where the rewrite's own open holds at most
     * `inputOpenParallelism` at a time. A parsed footer is several times
     * its serialized length (one `ColumnChunkMetaData` with statistics per
     * leaf per row group), so 16 MiB serialized is up to a few hundred MB
     * of heap at worst, held from the probe to the end of the part that
     * uses it ([LayoutPart.dropFooters]) and NOT counted against
     * `CompactionConfig.sortedHeapBytesPerGroup`: that budget bounds the
     * sorted rewrite's chunk and merge, and this sits beside it as
     * headroom the pod's memory must leave. Every footer of a group of
     * ordinary files (tens of KB each) fits; a group of wide variant files
     * with many row groups keeps what fits and pays the tail read the
     * rewrite always paid for the rest.
     */
    const val FOOTER_RETENTION_BYTES: Long = 16L * 1024 * 1024
}

/**
 * What the compactor remembers about files it has probed, so the planner
 * can pack a VARIANT table's files by layout without reading a footer
 * (see [VariantLayoutGroups]): per file, a fingerprint of how its layouts
 * deviate from the declaration ([VariantLayoutGroups.deviations],
 * [VariantLayout.fingerprint]) together with a fingerprint of the
 * declaration it was judged against, or [DUCKDB] for a file DuckDB wrote.
 *
 * The declaration's fingerprint is what keeps an entry honest across
 * DDL: a declaration change (`set_variant_shredding`), or a variant
 * column added or renamed, changes what "deviating" means, so an entry
 * judged against another declaration is treated as never probed — the
 * file packs with the declared population again and is probed once more.
 * Deviations rather than whole layouts are what let a file written before
 * a variant column existed pack with later files of its layout.
 *
 * In this process's heap and nowhere else, deliberately. A column on
 * `hog_data_file` written by the hydrator would be the durable form, but
 * the hydrator sees only files registered without stats — pyhoglake
 * ships stats inline and is never hydrated — so the planner would need
 * this fallback anyway, and a migration plus a client protocol change to
 * close the gap buys one probe per file per restart. The planner is
 * correct without this memory (the probe splits any group); what the
 * memory removes is the repeat.
 *
 * Entries are removed when their files are compacted away, and a
 * compaction output is entered as it is registered (its layout is what
 * was just written), so what stays is the files left behind for want of
 * company: a handful per partition per layout. [capacity] is the backstop
 * for everything else (files of dropped tables, files a sibling replica
 * compacted): over it, the memory is cleared and every file is probed
 * again once.
 */
internal class VariantLayoutMemory(
    private val capacity: Int = DEFAULT_CAPACITY,
) {
    private data class Key(val catalogId: Long, val dataFileId: Long)

    /** [declared] the declaration's fingerprint at probe time; [deviations] the file's, or [DUCKDB]. */
    data class Known(val declared: Long, val deviations: Long) {
        val duckdb: Boolean get() = deviations == DUCKDB
    }

    private val known = ConcurrentHashMap<Key, Known>()

    fun remember(
        catalogId: Long,
        dataFileId: Long,
        declared: Long,
        deviations: Long,
    ) {
        if (known.size >= capacity) known.clear()
        known[Key(catalogId, dataFileId)] = Known(declared, deviations)
    }

    /** What is remembered of a file, or null for one never probed (or forgotten). */
    fun recall(
        catalogId: Long,
        dataFileId: Long,
    ): Known? = known[Key(catalogId, dataFileId)]

    fun forget(
        catalogId: Long,
        dataFileIds: Iterable<Long>,
    ) {
        for (id in dataFileIds) known.remove(Key(catalogId, id))
    }

    /**
     * [candidates] of one bucket as the planner should pack them, under a
     * declaration whose fingerprint is [declared]: a file remembered as
     * deviating from it packs only with files deviating the same way; a
     * file remembered as DuckDB's is dropped (it compacts under no
     * layout); every other file — the declared layout's, the never-probed,
     * and those judged against another declaration — packs together, since
     * a writer honouring the catalog produces the declared layout and a
     * group that proves otherwise is split by the probe and remembered.
     * Returns the populations to pack separately and how many DuckDB files
     * were dropped.
     */
    fun populations(
        catalogId: Long,
        declared: Long,
        candidates: List<CompactionCandidate>,
    ): Pair<List<List<CompactionCandidate>>, Int> {
        if (known.isEmpty()) return listOf(candidates) to 0
        val main = mutableListOf<CompactionCandidate>()
        val others = LinkedHashMap<Long, MutableList<CompactionCandidate>>()
        var duckdb = 0
        for (candidate in candidates) {
            val entry = recall(catalogId, candidate.dataFileId)
            when {
                entry == null -> main += candidate
                entry.duckdb -> duckdb++
                entry.declared != declared || entry.deviations == NONE -> main += candidate
                else -> others.getOrPut(entry.deviations) { mutableListOf() } += candidate
            }
        }
        return (listOf(main) + others.values).filter { it.isNotEmpty() } to duckdb
    }

    val size: Int get() = known.size

    companion object {
        /** The deviations fingerprint remembered for a file DuckDB wrote: not a layout, a refusal. */
        const val DUCKDB: Long = Long.MIN_VALUE

        /** The fingerprint of no deviation at all: a file in the declared layout. */
        val NONE: Long = VariantLayout.fingerprint(emptyMap())

        /**
         * 250k entries, ~30 MB of heap at ~120 B an entry. What the memory
         * holds in steady state is the files left behind (a handful per
         * partition per layout; ~2,800 buckets on the largest table today),
         * so this is reached only by churn the forget-on-commit misses.
         */
        const val DEFAULT_CAPACITY = 250_000
    }
}
