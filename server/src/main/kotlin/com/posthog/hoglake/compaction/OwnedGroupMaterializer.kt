package com.posthog.hoglake.compaction

import com.posthog.hoglake.compaction.ParquetRewriter.NodeBudget
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.Converter
import org.apache.parquet.io.api.GroupConverter
import org.apache.parquet.io.api.PrimitiveConverter
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.MessageType

/** Input-only decoder. Converter state releases completed children and transfers returned root groups. */
internal class OwnedGroupMaterializer(
    schema: MessageType,
    budget: NodeBudget,
    memory: SortMemory.RowBudget?,
) : RecordMaterializer<Group>() {
    private val root = CountingGroupConverter(budget, schema, memory)

    override fun getCurrentRecord(): Group = checkNotNull(root.current).also { root.current = null }

    override fun getRootConverter(): GroupConverter = root

    /** Deterministic reachability check for the converter-lifetime regression. */
    internal fun retainedGroupCount(): Int = root.retainedGroupCount()
}

private class CountingGroupConverter(
    private val budget: NodeBudget,
    private val schema: GroupType,
    private val memory: SortMemory.RowBudget?,
    private val parent: CountingGroupConverter? = null,
    private val index: Int = -1,
) : GroupConverter() {
    private val children = HashMap<Int, Converter>()
    var current: Group? = null

    fun retainedGroupCount(): Int =
        (if (current == null) 0 else 1) +
            children.values.filterIsInstance<CountingGroupConverter>().sumOf { it.retainedGroupCount() }

    override fun getConverter(fieldIndex: Int): Converter =
        children.getOrPut(fieldIndex) {
            val type = schema.getType(fieldIndex)
            if (type.isPrimitive) {
                CountingPrimitiveConverter(this, fieldIndex, budget, memory)
            } else {
                CountingGroupConverter(budget, type.asGroupType(), memory, this, fieldIndex)
            }
        }

    override fun start() {
        // The root starts a new physical row.
        // Reset both allowances at that boundary.
        if (parent == null) {
            budget.reset()
            memory?.start(schema.fieldCount)
        } else {
            budget.spend()
            memory?.group(schema.fieldCount)
        }
        current = if (parent == null) SimpleGroup(schema) else checkNotNull(parent.current).addGroup(index)
    }

    override fun end() {
        if (parent != null) current = null
    }
}

private class CountingPrimitiveConverter(
    private val parent: CountingGroupConverter,
    private val index: Int,
    private val budget: NodeBudget,
    private val memory: SortMemory.RowBudget?,
) : PrimitiveConverter() {
    override fun hasDictionarySupport(): Boolean = false

    override fun addBinary(value: Binary) {
        budget.spend()
        memory?.value(value.length().toLong())
        checkNotNull(parent.current).add(index, ParquetRewriter.unpinned(value))
    }

    override fun addBoolean(value: Boolean) {
        budget.spend()
        memory?.value()
        checkNotNull(parent.current).add(index, value)
    }

    override fun addDouble(value: Double) {
        budget.spend()
        memory?.value()
        checkNotNull(parent.current).add(index, value)
    }

    override fun addFloat(value: Float) {
        budget.spend()
        memory?.value()
        checkNotNull(parent.current).add(index, value)
    }

    override fun addInt(value: Int) {
        budget.spend()
        memory?.value()
        checkNotNull(parent.current).add(index, value)
    }

    override fun addLong(value: Long) {
        budget.spend()
        memory?.value()
        checkNotNull(parent.current).add(index, value)
    }
}
