package com.posthog.hoglake.compaction

import com.posthog.hoglake.compaction.CompactionWork.TableContext
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.file.Path

class MemoryRefusalMemoryTest {
    private val column = Column(1, 0, ColumnDef("key", ColType.LONG))
    private val context =
        TableContext(
            catalogId = 1, catalog = "catalog", dataPath = "s3://bucket",
            namespace = "namespace", table = "table", tableId = 2,
            columns = listOf(column),
            sortFields = listOf(SortFieldDef(1, SortDirection.ASC, NullOrder.NULLS_LAST)),
            sortId = 3, sortBeginSnapshot = 4,
        )
    private val file = CompactionCandidate(5, "input.parquet", 100, 1000, 80, 0, beginSnapshot = 6)
    private val group = CompactionGroup(listOf(file), 7, listOf("partition"))
    private val spill = SortSpill(100, 1024 * 1024, 1024 * 1024, Path.of("/tmp"))
    private val layouts = VariantLayoutMemory()

    private fun key(
        ctx: TableContext = context,
        inputs: CompactionGroup = group,
        bounds: SortSpill = spill,
        appendFloor: Long = 32L * 1024 * 1024,
    ) = MemoryRefusalMemory.key(ctx, inputs, bounds, layouts, appendFloor)

    @Test
    fun `cache bounds entries and evicts the least recently recalled identity`() {
        val cache = MemoryRefusalMemory(2)
        val a = MemoryRefusalMemory.Identity(1, 0, 0, 0)
        val b = MemoryRefusalMemory.Identity(2, 0, 0, 0)
        val c = MemoryRefusalMemory.Identity(3, 0, 0, 0)
        cache.remember(a, MemoryRefusalMemory.Reason.MERGE)
        cache.remember(b, MemoryRefusalMemory.Reason.OVERSIZED_ROW)
        assertThat(cache.recall(a)).isEqualTo(MemoryRefusalMemory.Reason.MERGE)
        cache.remember(c, MemoryRefusalMemory.Reason.MERGE)
        assertThat(cache.size()).isEqualTo(2)
        assertThat(cache.recall(b)).isNull()
        assertThat(cache.recall(a)).isNotNull()
    }

    @Test
    fun `immutable file and DV changes invalidate a refusal`() {
        val changes =
            listOf(
                file.copy(dataFileId = 50),
                file.copy(path = "other.parquet"),
                file.copy(beginSnapshot = 60),
                file.copy(recordCount = 101),
                file.copy(fileSizeBytes = 1001),
                file.copy(rowIdStart = 100),
                file.copy(explicitRowIds = true),
                file.copy(dv = LiveDv(8, "dv", 1)),
                file.copy(dv = LiveDv(9, "dv", 1)),
                file.copy(dv = LiveDv(8, "other-dv", 1)),
                file.copy(dv = LiveDv(8, "dv", 2)),
            )
        for (changed in changes) {
            assertThat(key(inputs = group.copy(files = listOf(changed)))).isNotEqualTo(key())
        }
    }

    @Test
    fun `changing an existing DV identity invalidates a refusal`() {
        val baseline = file.copy(dv = LiveDv(8, "dv", 1))
        val original = key(inputs = group.copy(files = listOf(baseline)))
        val changes = listOf(LiveDv(9, "dv", 1), LiveDv(8, "other-dv", 1))
        for (dv in changes) {
            val changed = baseline.copy(dv = dv)
            assertThat(key(inputs = group.copy(files = listOf(changed)))).isNotEqualTo(original)
        }
    }

    @Test
    fun `schema sort layout and effective budgets invalidate a refusal`() {
        val changes =
            listOf(
                context.copy(catalogId = 10),
                context.copy(catalog = "other-catalog"),
                context.copy(dataPath = "s3://other-bucket"),
                context.copy(namespace = "other-namespace"),
                context.copy(table = "other-table"),
                context.copy(tableId = 20),
                context.copy(sortId = 30),
                context.copy(sortBeginSnapshot = 40),
                context.copy(sortFields = listOf(SortFieldDef(1, SortDirection.DESC, NullOrder.NULLS_LAST))),
                context.copy(columns = listOf(column.copy(def = ColumnDef("renamed", ColType.LONG)))),
                context.copy(columns = listOf(column.copy(def = ColumnDef("key", ColType.INT)))),
                context.copy(maxNodesPerRow = 10),
                context.copy(variantCompaction = true),
            )
        for (changed in changes) assertThat(key(ctx = changed)).isNotEqualTo(key())
        val bounds =
            listOf(
                spill.copy(chunkRows = 99),
                spill.copy(mergeBudgetBytes = spill.mergeBudgetBytes + 1),
                spill.copy(spillBudgetBytes = spill.spillBudgetBytes + 1),
                spill.copy(readerBudgetBytes = spill.readerBudgetBytes + 1),
                spill.copy(spillBlockBytes = spill.spillBlockBytes + 1),
                spill.copy(readaheadBytes = spill.readaheadBytes + 1),
                spill.copy(verifyMinBytes = spill.verifyMinBytes + 1),
            )
        for (changed in bounds) assertThat(key(bounds = changed)).isNotEqualTo(key())
        assertThat(key(appendFloor = 1)).isNotEqualTo(key())
        assertThat(key(inputs = group.copy(partitionValues = listOf("other")))).isNotEqualTo(key())
    }

    @Test
    fun `file order and observer do not change immutable admission identity`() {
        val pair = group.copy(files = listOf(file, file.copy(dataFileId = 9)))
        assertThat(key(inputs = pair.copy(files = pair.files.reversed()))).isEqualTo(key(inputs = pair))
        assertThat(key(bounds = spill.copy(memoryObserver = { _, _ -> }))).isEqualTo(key())
    }

    @Test
    fun `remembered variant layout changes invalidate a refusal`() {
        val original = key()
        layouts.remember(1, file.dataFileId, declared = 10, deviations = 20)
        val firstLayout = key()
        assertThat(firstLayout).isNotEqualTo(original)
        layouts.remember(1, file.dataFileId, declared = 10, deviations = 21)
        assertThat(key()).isNotEqualTo(firstLayout)
    }

    @Test
    fun `binary type parameters hash by content and maps hash by key order`() {
        fun parameters(
            reverse: Boolean,
            bytes: ByteArray,
        ): TableContext {
            val entries = listOf("binary" to bytes, "scale" to 2)
            val params = (if (reverse) entries.reversed() else entries).toMap()
            return context.copy(
                columns = listOf(column.copy(def = ColumnDef("key", ColType.LONG, typeParams = params))),
            )
        }
        val first = key(ctx = parameters(false, byteArrayOf(1, 2)))
        assertThat(key(ctx = parameters(true, byteArrayOf(1, 2)))).isEqualTo(first)
        assertThat(key(ctx = parameters(true, byteArrayOf(1, 3)))).isNotEqualTo(first)
    }

    @Test
    fun `a wide schema fingerprint is reused for a bounded fifty-thousand-key workload`() {
        val wide =
            context.copy(
                columns = (1L..151L).map { column.copy(fieldId = it, ordinal = it.toInt()) },
            )
        val first = wide.memoryRefusalIdentity
        val start = System.nanoTime()
        repeat(50_000) { index ->
            key(ctx = wide, inputs = group.copy(files = listOf(file.copy(dataFileId = index.toLong()))))
        }
        val elapsed = (System.nanoTime() - start) / 1_000_000
        assertThat(wide.memoryRefusalIdentity).isSameAs(first)
        println("MEMORY_KEY_CALIBRATION keys=50000 schemaNodes=151 elapsedMs=$elapsed")
    }
}
