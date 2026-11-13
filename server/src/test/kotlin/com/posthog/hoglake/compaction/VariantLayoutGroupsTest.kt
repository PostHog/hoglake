package com.posthog.hoglake.compaction

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData
import org.apache.parquet.hadoop.metadata.ColumnPath
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * [VariantLayoutGroups.split] — how a planned group whose inputs hold
 * more than one variant layout becomes the rewrites that run and the
 * files left alone — and [VariantLayoutMemory], what the planner packs
 * apart once a probe has taught it a file's layout. Pure: candidates are
 * made up, layouts are derived from declarations, no file is read.
 */
class VariantLayoutGroupsTest {
    private val grouping = CompactionGrouping.of(TARGET)

    @Test
    fun `a group of one layout and no DuckDB file is the one part it was planned as, in its own layout`() {
        val files = (1..5).map { candidate(it.toLong()) }
        val group = CompactionGroup(files, 1, listOf("7"))
        for ((layouts, declared) in listOf(DECLARED to true, OLD to false)) {
            val split = VariantLayoutGroups.split(group, files.map { probed(it, layouts) }, DECLARED, grouping, 5)
            assertThat(split.parts).hasSize(1)
            val part = split.parts.single()
            assertThat(part.group).isSameAs(group)
            assertThat(part.layouts).isEqualTo(layouts)
            assertThat(part.declared).isEqualTo(declared)
            assertThat(
                part.deviations,
            ).isEqualTo(if (declared) emptyMap() else mapOf(V.fieldId to OLD.getValue(V.fieldId)))
            assertThat(split.leftBehind).isEmpty()
            assertThat(split.duckdb).isEmpty()
            assertThat(split.layoutCount).isEqualTo(1)
            assertThat(split.mixed).isFalse()
        }
    }

    @Test
    fun `a mixed group becomes one part per layout, the declared layout first, each in row-id order`() {
        // Five old-layout files and five declared, interleaved by row id and
        // size, under a minimum of 5: both partitions are worth a rewrite.
        val old = (0 until 5).map { candidate(id = 10L + it, rowIdStart = 1000L - it * 100, bytes = 100) }
        val new = (0 until 5).map { candidate(id = 20L + it, rowIdStart = 950L - it * 100, bytes = 120) }
        val group = CompactionGroup((old + new).sortedBy { it.rowIdStart }, 1, listOf("7"))
        val probed = group.files.map { probed(it, if (it.dataFileId < 20) OLD else DECLARED) }

        val split = VariantLayoutGroups.split(group, probed, DECLARED, grouping, 5)
        assertThat(split.layoutCount).isEqualTo(2)
        assertThat(split.mixed).isTrue()
        assertThat(split.leftBehind).isEmpty()
        assertThat(split.parts).hasSize(2)
        val (first, second) = split.parts
        assertThat(first.declared).isTrue()
        assertThat(first.layouts).isEqualTo(DECLARED)
        assertThat(first.group.files.map { it.dataFileId }).containsExactly(24, 23, 22, 21, 20)
        assertThat(first.group.files.map { it.rowIdStart }).isSorted()
        assertThat(first.group.specId).isEqualTo(1)
        assertThat(first.group.partitionValues).containsExactly("7")
        assertThat(second.declared).isFalse()
        assertThat(second.layouts).isEqualTo(OLD)
        assertThat(second.group.files.map { it.dataFileId }).containsExactly(14, 13, 12, 11, 10)
        // Each part carries the footers of exactly its own inputs, until it
        // is told to let them go.
        assertThat(first.footers.keys).containsExactlyInAnyOrder(20, 21, 22, 23, 24)
        assertThat(second.footers.keys).containsExactlyInAnyOrder(10, 11, 12, 13, 14)
        first.dropFooters()
        assertThat(first.footers).isEmpty()
        assertThat(second.footers).hasSize(5)
    }

    @Test
    fun `a layout with too few files in the group is left alone, not refused and not rewritten`() {
        val files = (1..7).map { candidate(it.toLong()) }
        val group = CompactionGroup(files, null, null)
        // Two old-layout files among five declared: the declared five
        // rewrite; the two wait for company.
        val probed = files.map { probed(it, if (it.dataFileId <= 2) OLD else DECLARED) }
        val split = VariantLayoutGroups.split(group, probed, DECLARED, grouping, 5)
        assertThat(split.parts).hasSize(1)
        assertThat(split.parts.single().declared).isTrue()
        assertThat(split.parts.single().group.files.map { it.dataFileId }).containsExactly(3, 4, 5, 6, 7)
        assertThat(split.leftBehind.map { it.file.dataFileId }).containsExactly(1, 2)
        assertThat(split.layoutCount).isEqualTo(2)

        // Neither partition large enough: nothing runs, everything waits.
        val halves = files.take(6).map { probed(it, if (it.dataFileId <= 3) OLD else DECLARED) }
        val none = VariantLayoutGroups.split(CompactionGroup(files.take(6), null, null), halves, DECLARED, grouping, 5)
        assertThat(none.parts).isEmpty()
        assertThat(none.leftBehind).hasSize(6)
    }

    @Test
    fun `the minimum scales with the files it judges, as the planner's does`() {
        // Files of half the target: two are a group (CompactionGrouping's `need`).
        val big = (1..4).map { candidate(it.toLong(), bytes = TARGET / 2) }
        val probed = big.map { probed(it, if (it.dataFileId <= 2) OLD else DECLARED) }
        val split = VariantLayoutGroups.split(CompactionGroup(big, null, null), probed, DECLARED, grouping, 5)
        assertThat(split.parts).hasSize(2)
        assertThat(split.leftBehind).isEmpty()
    }

    @Test
    fun `a DuckDB-written file is excluded on its own and never blocks the rest`() {
        val files = (1..6).map { candidate(it.toLong()) }
        val group = CompactionGroup(files, null, null)
        val probed = files.map { probed(it, DECLARED, duckdb = it.dataFileId == 6L) }
        val split = VariantLayoutGroups.split(group, probed, DECLARED, grouping, 5)
        assertThat(split.duckdb.map { it.file.dataFileId }).containsExactly(6)
        assertThat(split.parts).hasSize(1)
        assertThat(split.parts.single().group.files.map { it.dataFileId }).containsExactly(1, 2, 3, 4, 5)
        assertThat(split.parts.single().declared).isTrue()
        assertThat(split.layoutCount).isEqualTo(1)
        assertThat(split.mixed).describedAs("the probe changed the plan").isTrue()
        assertThat(split.leftBehind).isEmpty()

        // A group of DuckDB files alone: nothing to rewrite, nothing left behind, no layout seen.
        val ducks =
            VariantLayoutGroups.split(group, files.map { probed(it, DECLARED, duckdb = true) }, DECLARED, grouping, 5)
        assertThat(ducks.parts).isEmpty()
        assertThat(ducks.layoutCount).isZero()
        assertThat(ducks.mixed).isFalse()
    }

    @Test
    fun `a file predating a variant column packs with files of its layout, whether or not they hold the column`() {
        val files = (1..6).map { candidate(it.toLong()) }
        // Partitions are by DEVIATION from the declaration, so a file
        // without `w` (written before it was added) deviates in nothing when
        // `v` is declared, and in `v` alone when `v` is old — exactly as a
        // file holding `w` does.
        val probed =
            files.map {
                probed(
                    it,
                    when (it.dataFileId) {
                        1L -> DECLARED - W.fieldId
                        2L, 3L -> OLD - W.fieldId
                        4L -> OLD
                        else -> DECLARED
                    },
                )
            }
        val split = VariantLayoutGroups.split(CompactionGroup(files, null, null), probed, DECLARED, grouping, 3)
        assertThat(split.layoutCount).isEqualTo(2)
        assertThat(split.parts).hasSize(2)
        assertThat(split.parts[0].declared).isTrue()
        assertThat(split.parts[0].group.files.map { it.dataFileId }).containsExactly(1, 5, 6)
        assertThat(split.parts[1].declared).isFalse()
        assertThat(split.parts[1].group.files.map { it.dataFileId }).containsExactly(2, 3, 4)
        // The part's output layout completes the deviation with the declaration.
        assertThat(split.parts[1].layouts).isEqualTo(OLD)
        assertThat(split.parts[1].deviations).containsOnlyKeys(V.fieldId)
        assertThat(
            VariantLayoutGroups.deviations(OLD - W.fieldId, DECLARED),
        ).isEqualTo(VariantLayoutGroups.deviations(OLD, DECLARED))
        assertThat(VariantLayoutGroups.deviations(DECLARED - W.fieldId, DECLARED)).isEmpty()
    }

    @Test
    fun `every input must be probed`() {
        val files = (1..3).map { candidate(it.toLong()) }
        assertThatThrownBy {
            VariantLayoutGroups.split(
                CompactionGroup(files, null, null),
                files.take(2).map { probed(it, DECLARED) },
                DECLARED,
                grouping,
                2,
            )
        }.isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `a footer's serialized length is read off the footer, never off the registered hint`() {
        val schema = MessageType("m", Types.required(PrimitiveTypeName.INT64).named("a"))
        // Two row groups, the last column chunk ending at byte 1000 of a
        // 1500-byte file: 492 bytes of footer (the trailer's 8 apart).
        val blocks =
            listOf(100L to 400L, 500L to 500L).map { (start, size) ->
                BlockMetaData().apply {
                    addColumn(
                        ColumnChunkMetaData.get(
                            ColumnPath.get("a"),
                            PrimitiveTypeName.INT64,
                            CompressionCodecName.UNCOMPRESSED,
                            emptySet(),
                            null,
                            start,
                            start,
                            10,
                            size,
                            size,
                        ),
                    )
                }
            }
        val footer = ParquetMetadata(FileMetaData(schema, emptyMap(), "test"), blocks)
        assertThat(VariantLayoutGroups.footerBytes(footer, 1500)).isEqualTo(492)
        // No row groups: the whole file but the trailer, which is what an empty file's footer is.
        assertThat(
            VariantLayoutGroups.footerBytes(ParquetMetadata(FileMetaData(schema, emptyMap(), "t"), emptyList()), 50),
        )
            .isEqualTo(42)
        // Never negative on a lying size.
        assertThat(VariantLayoutGroups.footerBytes(footer, 900)).isZero()
    }

    // ---- the memory ------------------------------------------------------------

    @Test
    fun `the planner packs remembered files by deviation and drops DuckDB's, and unknown files with the declared`() {
        val memory = VariantLayoutMemory()
        val declared = VariantLayout.fingerprint(DECLARED)
        val old = VariantLayout.fingerprint(VariantLayoutGroups.deviations(OLD, DECLARED))
        val files = (1..8).map { candidate(it.toLong()) }
        // Nothing remembered: one population, as before this existed.
        assertThat(memory.populations(1, declared, files).all()).isEqualTo(listOf(files))

        memory.remember(1, 1, declared, old)
        memory.remember(1, 2, declared, old)
        memory.remember(1, 3, declared, VariantLayoutMemory.NONE)
        memory.remember(1, 4, declared, VariantLayoutMemory.DUCKDB)
        // Another catalog's file 5 is not this catalog's.
        memory.remember(2, 5, declared, old)
        val divided = memory.populations(1, declared, files)
        assertThat(divided.duckdb).isEqualTo(1)
        assertThat(divided.all().map { p -> p.map { it.dataFileId } })
            .containsExactly(listOf(3L, 5L, 6L, 7L, 8L), listOf(1L, 2L))
        assertThat(divided.unknown.map { it.dataFileId }).containsExactly(5L, 6L, 7L, 8L)

        // Compacted away: forgotten.
        memory.forget(1, listOf(1L, 2L))
        assertThat(memory.recall(1, 1)).isNull()
        assertThat(memory.recall(1, 3)).isEqualTo(VariantLayoutMemory.Known(declared, VariantLayoutMemory.NONE))
        assertThat(memory.recall(1, 4)!!.duckdb).isTrue()
        assertThat(memory.size).isEqualTo(3)
    }

    @Test
    fun `an entry judged against another declaration is unknown again, except a DuckDB file`() {
        // After set_variant_shredding, or a variant column added or renamed,
        // what "deviating" means has changed: every remembered file packs
        // with the declared population and is probed once more. A DuckDB
        // file stays one whatever the declaration.
        val memory = VariantLayoutMemory()
        val before = VariantLayout.fingerprint(DECLARED)
        val after = VariantLayout.fingerprint(DECLARED + (W.fieldId to OLD.getValue(V.fieldId)))
        val old = VariantLayout.fingerprint(VariantLayoutGroups.deviations(OLD, DECLARED))
        val files = (1..4).map { candidate(it.toLong()) }
        memory.remember(1, 1, before, old)
        memory.remember(1, 2, after, old)
        memory.remember(1, 3, before, VariantLayoutMemory.DUCKDB)
        val divided = memory.populations(1, after, files)
        assertThat(divided.duckdb).isEqualTo(1)
        assertThat(divided.all().map { p -> p.map { it.dataFileId } }).containsExactly(listOf(1L, 4L), listOf(2L))
        assertThat(
            divided.unknown.map {
                it.dataFileId
            },
        ).describedAs("the stale entry is unknown again").containsExactly(1L, 4L)
    }

    @Test
    fun `a remembered population short of the minimum borrows idle unknown arrivals, boundedly`() {
        // Four remembered old-layout files and a fifth, unprobed, in the same
        // layout: apart, neither population forms a group of 5, ever. The
        // short population borrows what would complete it; the borrowed
        // file is probed with it and, in its layout, compacts with it.
        val memory = VariantLayoutMemory()
        val declared = VariantLayout.fingerprint(DECLARED)
        val old = VariantLayout.fingerprint(VariantLayoutGroups.deviations(OLD, DECLARED))
        val files = (1..9).map { candidate(it.toLong()) }
        (1L..4L).forEach { memory.remember(1, it, declared, old) }
        memory.remember(1, 5, declared, VariantLayoutMemory.NONE)
        // 6, 7, 8 unprobed; 9 judged against another declaration (stale: unknown again).
        memory.remember(1, 9, declared + 1, old)

        fun divide(
            candidates: List<CompactionCandidate>,
            min: Int = 5,
        ): Pair<List<CompactionCandidate>, List<List<Long>>> {
            val divided = memory.populations(1, declared, candidates)
            val lent = VariantLayoutGroups.borrow(divided, grouping, min, 64) { it.fileSizeBytes }
            return lent to divided.all().map { p -> p.map { it.dataFileId } }
        }
        // The main population (5 files, 4 of them unknown) forms a group of
        // its own at this minimum: nothing is borrowed from work it has.
        assertThat(divide(files).second).containsExactly(listOf(5L, 6L, 7L, 8L, 9L), listOf(1L, 2L, 3L, 4L))
        // At a minimum of 6 the main population is idle; the short one
        // borrows the two unknowns that complete it, in candidate order.
        val (lent, populations) = divide(files, min = 6)
        assertThat(lent.map { it.dataFileId }).containsExactly(6L, 7L)
        assertThat(populations).containsExactly(listOf(5L, 8L, 9L), listOf(1L, 2L, 3L, 4L, 6L, 7L))

        // Bounded: the LARGEST short population only; a population at the
        // minimum borrows nothing.
        val other = VariantLayout.fingerprint(mapOf(W.fieldId to OLD.getValue(V.fieldId)))
        memory.remember(1, 10, declared, other)
        memory.remember(1, 11, declared, other)
        val more = files + (10..11).map { candidate(it.toLong()) }
        assertThat(divide(more, min = 6).second)
            .containsExactly(listOf(5L, 8L, 9L), listOf(1L, 2L, 3L, 4L, 6L, 7L), listOf(10L, 11L))
        (6L..8L).forEach { memory.remember(1, it, declared, old) }
        assertThat(divide(files, min = 6).second).containsExactly(listOf(5L, 9L), listOf(1L, 2L, 3L, 4L, 6L, 7L, 8L))

        // Not lent when the loan would not be probed: a file the packing
        // rule's dominance split would set apart from the short population.
        val memory2 = VariantLayoutMemory()
        (1L..4L).forEach { memory2.remember(1, it, declared, old) }
        val smallAndHuge = (1..4).map { candidate(it.toLong(), bytes = 100) } + candidate(5, bytes = 4_000)
        val divided = memory2.populations(1, declared, smallAndHuge)
        assertThat(VariantLayoutGroups.borrow(divided, grouping, 5, 64) { it.fileSizeBytes }).isEmpty()
        assertThat(
            divided.all().map {
                    p ->
                p.map { it.dataFileId }
            },
        ).containsExactly(listOf(5L), listOf(1L, 2L, 3L, 4L))
    }

    @Test
    fun `over capacity the memory evicts its eldest entry, one for each new one`() {
        val memory = VariantLayoutMemory(capacity = 3)
        repeat(3) { memory.remember(1, it.toLong(), 7, 42) }
        assertThat(memory.size).isEqualTo(3)
        memory.remember(1, 99, 7, 42)
        // Not cleared: the eldest went, the rest stayed.
        assertThat(memory.size).isEqualTo(3)
        assertThat(memory.recall(1, 0)).isNull()
        assertThat(memory.recall(1, 1)).isNotNull()
        assertThat(memory.recall(1, 2)).isNotNull()
        assertThat(memory.recall(1, 99)).isEqualTo(VariantLayoutMemory.Known(7, 42))
        memory.remember(1, 100, 7, 42)
        assertThat(memory.recall(1, 1)).isNull()
        assertThat(memory.size).isEqualTo(3)
    }

    // ---- fixtures --------------------------------------------------------------

    private fun candidate(
        id: Long,
        rowIdStart: Long = id * 100,
        bytes: Long = 100,
    ) = CompactionCandidate(
        dataFileId = id,
        path = "s3://b/$id.parquet",
        recordCount = 10,
        fileSizeBytes = bytes,
        footerSize = 50,
        rowIdStart = rowIdStart,
    )

    /** A probed input whose footer is stood in for by a distinct empty one, so parts can be checked for carrying their own. */
    private fun probed(
        file: CompactionCandidate,
        layouts: Map<Long, GroupType>,
        duckdb: Boolean = false,
    ) = VariantLayoutGroups.Probed(
        file,
        ParquetMetadata(
            FileMetaData(MessageType("m", emptyList()), emptyMap(), "test ${file.dataFileId}"),
            emptyList(),
        ),
        layouts,
        duckdb,
    )

    private companion object {
        const val TARGET = 10_000L

        val V =
            Column(
                2,
                1,
                ColumnDef(
                    "v",
                    ColType.VARIANT,
                    mapOf(
                        "shredding" to
                            mapOf("type" to "object", "fields" to listOf(mapOf("name" to "a", "type" to "string"))),
                    ),
                ),
            )
        val W = Column(3, 2, ColumnDef("w", ColType.VARIANT))
        val LIVE = listOf(Column(1, 0, ColumnDef("id", ColType.LONG)), V, W)

        /** The declaration's layouts: `v` shredded on `a`, `w` unshredded. */
        val DECLARED: Map<Long, GroupType> = VariantLayout.declared(LIVE)

        /** The layouts of a file written before `v` was shredded: both unshredded. */
        val OLD: Map<Long, GroupType> =
            DECLARED +
                (
                    V.fieldId to
                        VariantLayout.groupType(V.copy(def = V.def.copy(typeParams = null)), Type.Repetition.OPTIONAL)
                )
    }
}
