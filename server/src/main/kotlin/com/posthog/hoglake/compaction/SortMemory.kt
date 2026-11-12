package com.posthog.hoglake.compaction

import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type

/** Divide the existing heap allowance between retained rows, readers, writers, and transient allocations. */
internal class SortMemory(val bytes: Long) {
    val chunkBytes = bytes / 4
    val rowBytes = bytes / 16
    val readerBytes = bytes / 4
    val mergeBytes = bytes / 2
    val writerBytes = bytes / 4

    fun headBytes(
        footer: ParquetMetadata,
        keyCount: Int = 0,
    ): Long {
        fun nodes(type: Type): Long =
            if (type.isPrimitive) 1 else type.asGroupType().fields.fold(1L) { sum, field -> add(sum, nodes(field)) }
        val fields = nodes(footer.fileMetaData.schema)
        val descriptors = footer.fileMetaData.schema.columns.associateBy { it.path.toList() }
        val largest =
            footer.blocks.maxOfOrNull { block ->
                block.columns.fold(multiply(NODE_BYTES, keyCount + 1L)) { sum, column ->
                    val descriptor = descriptors[column.path.toArray().toList()]
                    val values =
                        if (column.valueCount >= 0 && descriptor?.maxRepetitionLevel == 0) {
                            minOf(1, column.valueCount)
                        } else {
                            column.valueCount
                        }
                    val payload =
                        when (descriptor?.primitiveType?.primitiveTypeName) {
                            PrimitiveTypeName.BOOLEAN -> 1L
                            PrimitiveTypeName.INT32,
                            PrimitiveTypeName.FLOAT,
                            -> 4L
                            PrimitiveTypeName.INT64,
                            PrimitiveTypeName.DOUBLE,
                            -> 8L
                            PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY ->
                                descriptor.primitiveType.typeLength.toLong()
                            else -> column.totalUncompressedSize
                        }
                    add(
                        sum,
                        multiply(multiply(add(multiply(2, fields), 4), NODE_BYTES), values),
                        // A dictionary entry can supply every repeated value in one row.
                        multiply(3, multiply(values, payload)),
                    )
                }
            } ?: 0L
        return minOf(rowBytes, largest)
    }

    fun runBytes(
        footer: ParquetMetadata,
        readahead: Long = 0,
        keyCount: Int = 0,
    ): Long = add(ExternalMergeSort.footerRunBytes(footer), multiply(2, headBytes(footer, keyCount)), readahead)

    fun refuse(message: String): Nothing = throw MergeBudgetExceededException(message, 0, 0, 0)

    fun checkReader(
        blocks: List<BlockMetaData>,
        schema: MessageType,
        label: String,
    ) {
        val paths = schema.columns.map { it.path.toList() }.toSet()
        val largest = blocks.maxOfOrNull { block -> readerCharge(block, paths) } ?: 0L
        if (largest > readerBytes) {
            refuse(
                "reading $label needs $largest B before materialization. The reader allowance is $readerBytes B.",
            )
        }
    }

    fun writerTuning(
        blockBytes: Long,
        leaves: Int,
    ): ParquetRewriter.WriterTuning {
        val rowGroupBytes = minOf(blockBytes, maxOf(1, bytes / 64))
        val fixedBytes = add(multiply(leaves.toLong(), WRITER_FIXED_BYTES_PER_LEAF), multiply(2, rowGroupBytes))
        if (fixedBytes > writerBytes) {
            refuse("$leaves live output leaves need more than $writerBytes B of writer allowance.")
        }
        val pageBytes =
            minOf(
                ExternalMergeSort.SPILL_PAGE_BYTES.toLong(),
                (writerBytes - fixedBytes) / maxOf(1, multiply(leaves.toLong(), WRITER_BUFFER_MULTIPLIER)),
            )
        if (pageBytes < MIN_WRITER_PAGE_BYTES) {
            refuse("the writer allowance cannot fund $MIN_WRITER_PAGE_BYTES-byte pages for $leaves live output leaves.")
        }
        return ParquetRewriter.WriterTuning(
            rowGroupBytes,
            pageBytes.toInt(),
            ExternalMergeSort.SPILL_MAX_ROWS_PER_SIZE_CHECK,
        )
    }

    companion object {
        const val NODE_BYTES = 192L

        /** Reserve the first 4,096 dictionary IDs and fixed writer objects for each live output leaf. */
        const val WRITER_FIXED_BYTES_PER_LEAF = 32L * 1024

        /** Reserve dictionary maps, page buffers, and codec buffers above their encoded sizes. */
        const val WRITER_BUFFER_MULTIPLIER = 32L

        /** Match parquet-java's private minimum initial slab size. */
        const val MIN_WRITER_PAGE_BYTES = 64L

        /** Treat negative metadata and overflow as unaffordable sizes. */
        fun add(vararg values: Long): Long {
            var sum = 0L
            for (value in values) {
                if (value < 0 || value > Long.MAX_VALUE - sum) return Long.MAX_VALUE
                sum += value
            }
            return sum
        }

        fun multiply(
            a: Long,
            b: Long,
        ): Long = if (a < 0 || b < 0 || (a != 0L && b > Long.MAX_VALUE / a)) Long.MAX_VALUE else a * b

        fun readerCharge(
            block: BlockMetaData,
            paths: Set<List<String>>? = null,
        ): Long {
            var bytes = 0L
            for (column in block.columns) {
                if (paths != null && column.path.toArray().toList() !in paths) continue
                // Reserve compressed copies, decompression buffers, dictionaries, and level-reader arrays.
                bytes =
                    add(
                        bytes,
                        multiply(2, column.totalSize),
                        multiply(2, column.totalUncompressedSize),
                        multiply(64, column.valueCount),
                    )
            }
            return bytes
        }
    }
}
