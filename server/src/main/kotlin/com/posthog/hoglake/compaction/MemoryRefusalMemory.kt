package com.posthog.hoglake.compaction

import com.posthog.hoglake.compaction.CompactionWork.TableContext
import com.posthog.hoglake.model.Column
import java.security.MessageDigest
import java.util.LinkedHashMap

/** Remembers deterministic resource refusals without retaining inputs or schemas. */
internal class MemoryRefusalMemory(
    private val capacity: Int = CompactionConfig.DEFAULT_MAX_CANDIDATES,
) {
    enum class Reason { SPILL, MERGE, OVERSIZED_ROW }

    data class Identity(val a: Long, val b: Long, val c: Long, val d: Long)

    private val entries =
        object : LinkedHashMap<Identity, Reason>(16, 0.75f, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<Identity, Reason>): Boolean = size > capacity
        }

    @Synchronized
    fun recall(key: Identity): Reason? = entries[key]

    @Synchronized
    fun remember(
        key: Identity,
        reason: Reason,
    ) {
        entries[key] = reason
    }

    @Synchronized
    fun size(): Int = entries.size

    companion object {
        fun contextKey(ctx: TableContext): Identity =
            Hash().run {
                feed(ctx.catalogId)
                feed(ctx.catalog)
                feed(ctx.dataPath)
                feed(ctx.namespace)
                feed(ctx.table)
                feed(ctx.tableId)
                feed(ctx.sortId)
                feed(ctx.sortBeginSnapshot)
                ctx.sortFields.forEach { feed(it) }
                ctx.columns.forEach(::column)
                feed(ctx.maxNodesPerRow)
                feed(ctx.codec)
                feed(ctx.variantCompaction)
                finish()
            }

        /** Every identity or bound that can change a deterministic refusal changes this digest. */
        fun key(
            ctx: TableContext,
            group: CompactionGroup,
            spill: SortSpill,
            layouts: VariantLayoutMemory,
            appendFloorBytes: Long,
        ): Identity {
            return Hash().run {
                feed(ctx.memoryRefusalIdentity)
                feed(spill.chunkRows)
                feed(spill.mergeBudgetBytes)
                feed(spill.spillBudgetBytes)
                feed(spill.spillBlockBytes)
                feed(spill.readaheadBytes)
                feed(spill.verifyMinBytes)
                feed(spill.readerBudgetBytes)
                feed(appendFloorBytes)
                feed(group.specId)
                structured(group.partitionValues)
                for (file in group.files.sortedBy { it.dataFileId }) {
                    feed(file.dataFileId)
                    feed(file.path)
                    feed(file.beginSnapshot)
                    feed(file.fileSizeBytes)
                    feed(file.recordCount)
                    feed(file.survivingRecords)
                    feed(file.rowIdStart)
                    feed(file.explicitRowIds)
                    feed(file.dv?.deleteFileId)
                    feed(file.dv?.path)
                    feed(file.dv?.deleteCount)
                    structured(layouts.recall(ctx.catalogId, file.dataFileId)?.deviations)
                }
                finish()
            }
        }

        private class Hash {
            private val digest = MessageDigest.getInstance("SHA-256")

            fun feed(value: Any?) {
                val bytes = (value?.toString() ?: "null").toByteArray(Charsets.UTF_8)
                digest.update(bytes.size.toString().toByteArray(Charsets.UTF_8))
                digest.update(':'.code.toByte())
                digest.update(bytes)
            }

            fun structured(value: Any?) {
                when (value) {
                    is ByteArray -> {
                        feed("bytes:${value.size}")
                        digest.update(value)
                    }
                    is Map<*, *> -> {
                        feed("map:${value.size}")
                        for ((key, child) in value.entries.sortedBy { it.key.toString() }) {
                            feed(key)
                            structured(child)
                        }
                    }
                    is List<*> -> {
                        feed("list:${value.size}")
                        value.forEach { structured(it) }
                    }
                    else -> {
                        feed(value?.javaClass?.name)
                        feed(value)
                    }
                }
            }

            fun column(column: Column) {
                feed(column.fieldId)
                feed(column.ordinal)
                feed(column.def.name)
                feed(column.def.type)
                feed(column.def.nullable)
                structured(column.def.typeParams)
                feed(column.children.size)
                column.children.forEach(::column)
            }

            fun finish(): Identity {
                val bytes = digest.digest()

                fun word(offset: Int): Long {
                    var value = 0L
                    for (index in offset until offset + 8) {
                        value = (value shl 8) or (bytes[index].toLong() and 255)
                    }
                    return value
                }
                return Identity(word(0), word(8), word(16), word(24))
            }
        }
    }
}
