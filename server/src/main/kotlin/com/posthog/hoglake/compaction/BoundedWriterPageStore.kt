package com.posthog.hoglake.compaction

import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.Encoding
import org.apache.parquet.column.page.PageWriteStore
import org.apache.parquet.column.page.PageWriter
import org.apache.parquet.column.statistics.BinaryStatistics
import org.apache.parquet.column.statistics.SizeStatistics
import org.apache.parquet.column.statistics.Statistics
import org.apache.parquet.column.statistics.geospatial.GeospatialStatistics
import org.apache.parquet.format.converter.ParquetMetadataConverter

/** Bounds retained extrema without changing page indexes or serialized statistics. */
internal class BoundedWriterPageStore(
    private val delegate: PageWriteStore,
) : PageWriteStore {
    private class Holder(var aggregate: BoundedStatistics? = null)

    private val holders = HashMap<ColumnDescriptor, Holder>()

    var retainedExtremaBytes: Long = 0
        private set

    var suppressedColumns: Int = 0
        private set

    override fun getPageWriter(path: ColumnDescriptor): PageWriter {
        val holder = holders.getOrPut(path) { Holder() }
        val target = delegate.getPageWriter(path)
        return object : PageWriter by target {
            private fun bounded(statistics: Statistics<*>?): Statistics<*>? =
                if (statistics is BinaryStatistics) BoundedStatistics(statistics, holder) else statistics

            override fun writePage(
                bytes: BytesInput,
                valueCount: Int,
                rowCount: Int,
                statistics: Statistics<*>?,
                sizeStatistics: SizeStatistics?,
                geospatialStatistics: GeospatialStatistics?,
                repetitionEncoding: Encoding,
                definitionEncoding: Encoding,
                valuesEncoding: Encoding,
            ) = target.writePage(
                bytes,
                valueCount,
                rowCount,
                bounded(statistics),
                sizeStatistics,
                geospatialStatistics,
                repetitionEncoding,
                definitionEncoding,
                valuesEncoding,
            )

            override fun writePageV2(
                rowCount: Int,
                nullCount: Int,
                valueCount: Int,
                repetitionLevels: BytesInput,
                definitionLevels: BytesInput,
                encoding: Encoding,
                data: BytesInput,
                statistics: Statistics<*>?,
                sizeStatistics: SizeStatistics?,
                geospatialStatistics: GeospatialStatistics?,
            ) = target.writePageV2(
                rowCount,
                nullCount,
                valueCount,
                repetitionLevels,
                definitionLevels,
                encoding,
                data,
                bounded(statistics),
                sizeStatistics,
                geospatialStatistics,
            )
        }
    }

    /** Call after the column store flushes, before the page store writes the columns. */
    fun finishStatistics() {
        for (holder in holders.values) {
            val aggregate = holder.aggregate ?: continue
            aggregate.finish()
            if (aggregate.omitted) {
                suppressedColumns++
            } else if (aggregate.hasNonNullValue()) {
                retainedExtremaBytes += aggregate.genericGetMin().length().toLong() + aggregate.genericGetMax().length()
            }
            holder.aggregate = null
        }
    }

    override fun close() {
        holders.clear()
        delegate.close()
    }

    private class BoundedStatistics(
        source: BinaryStatistics,
        private val holder: Holder,
    ) : BinaryStatistics(source) {
        var omitted: Boolean = (source as? BoundedStatistics)?.omitted ?: false
            private set

        override fun copy(): BoundedStatistics = BoundedStatistics(this, holder).also { holder.aggregate = it }

        override fun hasNonNullValue(): Boolean = !omitted && super.hasNonNullValue()

        // Keep the empty format structure that the converter writes for oversized extrema.
        override fun isEmpty(): Boolean = if (omitted) false else super.isEmpty()

        override fun isSmallerThan(size: Long): Boolean = !omitted && super.isSmallerThan(size)

        override fun isSmallerThanWithTruncation(
            size: Long,
            truncation: Int,
        ): Boolean = !omitted && super.isSmallerThanWithTruncation(size, truncation)

        fun finish() {
            if (omitted) return
            if (!isSmallerThan(ParquetMetadataConverter.MAX_STATS_SIZE)) {
                min = null
                max = null
                omitted = true
            } else if (hasNonNullValue()) {
                // Exact arrays prevent short extrema from retaining larger source buffers.
                setMinMaxFromBytes(minBytes, maxBytes)
            }
        }
    }
}
