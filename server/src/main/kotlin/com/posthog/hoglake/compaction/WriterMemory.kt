package com.posthog.hoglake.compaction

import com.github.luben.zstd.Zstd
import com.posthog.hoglake.compaction.ParquetRewriter.OutputCodec
import com.posthog.hoglake.compaction.ParquetRewriter.Row
import org.apache.parquet.bytes.ByteBufferAllocator
import org.apache.parquet.bytes.HeapByteBufferAllocator
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.MessageType
import java.nio.ByteBuffer
import java.util.IdentityHashMap

/** Writer-only allocations never become retained spill-head charges. */
internal class WriterMemory(
    val memory: SortMemory,
    schema: MessageType,
    private val codec: OutputCodec,
    private val baseBytes: Long,
    private val largeRowBytes: Long,
    private val pageBytes: Long,
    private val retainFooter: Boolean = false,
    private val decimalWidths: IntArray = intArrayOf(),
    private val availableExtra: () -> Long = { Long.MAX_VALUE },
    private val reserve: (Long) -> Unit,
) {
    private val primitiveColumns = schema.columns
    val hasRepeatedValues = primitiveColumns.any { it.maxRepetitionLevel > 0 }
    private val valueCounts = LongArray(primitiveColumns.size)
    private val valueIndices =
        primitiveColumns.mapIndexed { index, column ->
            var group: GroupType = schema
            val path =
                column.path.map { name ->
                    val field = group.getFieldIndex(name)
                    val type = group.getType(field)
                    if (!type.isPrimitive) group = type.asGroupType()
                    field
                }
            path to index
        }.toMap()
    private var countingValues = false
    private val measurement =
        memory.MeasuringConsumer(
            schema,
            Sink,
            measureEncoding = true,
            primitiveObserved = { path ->
                if (countingValues) {
                    val index = checkNotNull(valueIndices[path])
                    valueCounts[index] = SortMemory.add(valueCounts[index], 1)
                }
            },
        )
    private val allocations = IdentityHashMap<ByteBuffer, Long>()
    private var allocatedBytes = 0L
    private var externalBytes = 0L
    private var dictionaryBytes = 0L
    private var largestExtra = 0L
    private var statisticsBytes = 0L
    private var completedStatisticsBytes = 0L

    var largestBinary = 0
        private set

    private class StatsNode(
        val children: List<StatsNode> = emptyList(),
        var largest: Long = 0,
        var second: Long = 0,
    )

    private fun statsNode(type: org.apache.parquet.schema.Type): StatsNode =
        StatsNode(if (type.isPrimitive) emptyList() else type.asGroupType().fields.map(::statsNode))

    private val stats = statsNode(schema)

    /** Length extrema bound retained Binary statistics without retaining their values. */
    fun observeBinary(
        path: List<Int>,
        value: Binary,
    ) {
        largestBinary = maxOf(largestBinary, value.length())
        if (!retainFooter) return
        var node = stats
        for (index in path) node = node.children[index]
        val length = value.length().toLong()
        val delta =
            if (length > node.largest) {
                val increase = length - node.second
                node.second = node.largest
                node.largest = length
                increase
            } else if (length > node.second) {
                (length - node.second).also { node.second = length }
            } else {
                0
            }
        statisticsBytes = SortMemory.add(statisticsBytes, delta)
        reserveActual(allocatedBytes)
    }

    fun finishRowGroup(retainedBytes: Long = 0) {
        completedStatisticsBytes = SortMemory.add(completedStatisticsBytes, retainedBytes)
        statisticsBytes = completedStatisticsBytes

        fun clear(node: StatsNode) {
            node.largest = 0
            node.second = 0
            node.children.forEach(::clear)
        }
        clear(stats)
    }

    /** Footer conversion copies the already bounded extrema outside the slab allocator. */
    fun beforeFooter() {
        reserveActual(SortMemory.add(allocatedBytes, statisticsBytes))
    }

    /** Parquet slabs and compressed-page copies reserve their actual capacities before allocation. */
    val allocator: ByteBufferAllocator =
        object : ByteBufferAllocator {
            override fun allocate(size: Int): ByteBuffer {
                val charge = SortMemory.add(size.toLong(), 192)
                reserveActual(SortMemory.add(allocatedBytes, charge))
                return HeapByteBufferAllocator.getInstance().allocate(size).also {
                    allocations[it] = charge
                    allocatedBytes = SortMemory.add(allocatedBytes, charge)
                }
            }

            override fun release(buffer: ByteBuffer) {
                allocatedBytes -= allocations.remove(buffer) ?: 0
                HeapByteBufferAllocator.getInstance().release(buffer)
            }

            override fun isDirect(): Boolean = false
        }

    fun replaceExternalReservation(bytes: Long) {
        reserveActual(allocatedBytes, external = bytes)
        externalBytes = bytes
    }

    /** Dictionary arrays have separate lifetimes from compressor reservations. */
    fun replaceDictionaryReservation(bytes: Long) {
        reserveActual(allocatedBytes, dictionary = bytes)
        dictionaryBytes = bytes
    }

    private fun reserveActual(
        allocated: Long,
        external: Long = externalBytes,
        dictionary: Long = dictionaryBytes,
    ) {
        request(SortMemory.excess(SortMemory.add(allocated, external, dictionary, statisticsBytes), baseBytes))
    }

    private fun request(extra: Long) {
        if (extra > largestExtra) {
            reserve(extra)
            largestExtra = extra
        }
    }

    /**
     * Ordinary rows use a schema bound.
     * Only potentially large emissions need a dry walk.
     */
    fun encodedBytes(row: Row): Long {
        if (row.writerBytes > 0) return row.writerBytes
        val bound =
            if (row.writerBytes < 0) {
                -row.writerBytes
            } else {
                checkNotNull(
                    row.plan,
                ).emittedMemoryBound(row, memory)
            }
        if (bound <= largeRowBytes) {
            row.writerBytes = -bound
            return bound
        }
        checkNotNull(row.plan).write(measurement, row.group, row.rowId)
        finishMeasurement(row)
        return row.writerBytes
    }

    private fun finishMeasurement(row: Row) {
        if (!SortMemory.fits(
                SortMemory.add(measurement.rowBytes, memory.keyBytes(row.key, decimalWidths)),
                memory.bytes,
            )
        ) {
            throw MergeBudgetExceededException(
                "an emitted row exceeds the readback allowance",
                0,
                0,
                0,
                oversizedRow = true,
            )
        }
        row.writerBytes = measurement.encodingBytes
    }

    /** Bound repeated values without walking an ordinary row. */
    fun dictionaryGrowthBound(
        row: Row,
        forecast: (ColumnDescriptor, Long) -> Long,
    ): Long {
        val repeated =
            if (row.memoryBytes <= 0) {
                Long.MAX_VALUE
            } else {
                checkNotNull(row.plan).emittedMemoryBound(row, memory) / memory.valueBytes()
            }
        return primitiveColumns.fold(0L) { total, column ->
            SortMemory.add(total, forecast(column, if (column.maxRepetitionLevel == 0) 1 else repeated))
        }
    }

    /** A rare dry walk resolves an inconclusive repeated-value forecast. */
    fun dictionaryGrowthMeasured(
        row: Row,
        forecast: (ColumnDescriptor, Long) -> Long,
    ): Long {
        valueCounts.fill(0)
        countingValues = true
        try {
            checkNotNull(row.plan).write(measurement, row.group, row.rowId)
            finishMeasurement(row)
        } finally {
            countingValues = false
        }
        return primitiveColumns.foldIndexed(0L) { index, total, column ->
            SortMemory.add(total, forecast(column, valueCounts[index]))
        }
    }

    fun extraBytes(row: Row): Long {
        val encoded = encodedBytes(row)
        if (encoded <= largeRowBytes) return 0
        val compressed = predictedCompressedBytes(encoded)
        // Slab growth retains less than twice its encoded contents.
        // Compression slabs retain less than twice their encoded contents.
        // Checksum and page-collection copies share one transition allowance.
        val footprint = footprint(encoded, compressed)
        return maxOf(
            SortMemory.excess(footprint, baseBytes),
            if (codec.name == CompressionCodecName.UNCOMPRESSED) {
                0
            } else {
                SortMemory.multiply(2, SortMemory.excess(compressed, predictedCompressedBytes(pageBytes)))
            },
        )
    }

    private fun footprint(
        encoded: Long,
        compressed: Long,
    ): Long =
        SortMemory.add(
            SortMemory.multiply(2, encoded),
            SortMemory.multiply(if (codec.name == CompressionCodecName.UNCOMPRESSED) 1 else 3, compressed),
        )

    fun beforeWrite(
        row: Row,
        dictionaryGrowthBytes: Long = 0,
    ): Boolean {
        val encoded = encodedBytes(row)
        val ordinary = encoded <= largeRowBytes
        // Avoid a native forecast call on ordinary rows.
        val compressed =
            if (ordinary) SortMemory.add(encoded, encoded / 6, 128) else predictedCompressedBytes(encoded)
        val required =
            SortMemory.excess(
                SortMemory.add(
                    allocatedBytes,
                    footprint(encoded, compressed),
                    SortMemory.add(externalBytes, dictionaryBytes, dictionaryGrowthBytes),
                    statisticsBytes,
                ),
                baseBytes,
            )
        return SortMemory.fits(maxOf(required, if (ordinary) 0 else extraBytes(row)), availableExtra())
    }

    /** A spill forecast, not a streaming-codec guarantee. Actual capacities enforce the allowance. */
    private fun predictedCompressedBytes(input: Long): Long {
        val result =
            when (codec.name) {
                CompressionCodecName.UNCOMPRESSED -> input
                CompressionCodecName.SNAPPY -> SortMemory.add(input, input / 6, 32)
                CompressionCodecName.ZSTD -> Zstd.compressBound(input)
                CompressionCodecName.LZ4_RAW -> SortMemory.add(input, input / 255, 16)
                CompressionCodecName.GZIP -> SortMemory.add(input, input / 4096, input / 16384, input / 33554432, 31)
                else -> error("unsupported writer codec ${codec.name}")
            }
        return result.coerceAtLeast(input)
    }

    private object Sink : RecordConsumer() {
        override fun startMessage() {}

        override fun endMessage() {}

        override fun startField(
            field: String,
            index: Int,
        ) {}

        override fun endField(
            field: String,
            index: Int,
        ) {}

        override fun startGroup() {}

        override fun endGroup() {}

        override fun addInteger(value: Int) {}

        override fun addLong(value: Long) {}

        override fun addBoolean(value: Boolean) {}

        override fun addBinary(value: Binary) {}

        override fun addFloat(value: Float) {}

        override fun addDouble(value: Double) {}
    }
}
