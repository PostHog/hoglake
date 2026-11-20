package com.posthog.hoglake.compaction

import com.sun.management.HotSpotDiagnosticMXBean
import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.bytes.CapacityByteArrayOutputStream
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.Encoding
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.column.page.DictionaryPage
import org.apache.parquet.column.values.ValuesWriter
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter
import org.apache.parquet.column.values.dictionary.IntList
import org.apache.parquet.column.values.factory.DefaultValuesWriterFactory
import org.apache.parquet.column.values.factory.ValuesWriterFactory
import org.apache.parquet.column.values.fallback.FallbackValuesWriter
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter
import org.apache.parquet.column.values.plain.PlainValuesWriter
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import shaded.parquet.it.unimi.dsi.fastutil.HashCommon
import shaded.parquet.it.unimi.dsi.fastutil.ints.Int2IntLinkedOpenHashMap
import shaded.parquet.it.unimi.dsi.fastutil.longs.Long2IntLinkedOpenHashMap
import shaded.parquet.it.unimi.dsi.fastutil.objects.Object2IntLinkedOpenHashMap
import java.lang.management.ManagementFactory

/**
 * Guards dictionary arrays before allocation.
 * The implementation follows Parquet 1.18.1 and its shaded fastutil 8.5.19.
 * Structural writer objects retain their existing schema allowance.
 */
internal class GuardedValuesWriterFactory(
    private val replaceUnmanagedBytes: (Long) -> Unit,
) : ValuesWriterFactory, AutoCloseable {
    private lateinit var properties: ParquetProperties
    private val defaults = DefaultValuesWriterFactory()
    private val dictionaries = HashMap<ColumnDescriptor, Dictionary>()
    private val writers = ArrayList<Tracked>()

    var unmanagedBytes: Long = 0
        private set

    /** This forecast never reserves memory or rejects a row. */
    val nextGrowthBytes: Long
        get() = dictionaries.values.fold(0L) { total, writer -> add(total, writer.forecast(1)) }

    /** The result covers additional array capacities, not the current reservation. */
    fun forecastGrowthBytes(
        path: ColumnDescriptor,
        additionalValues: Long,
    ): Long = dictionaries[path]?.forecast(additionalValues.coerceAtLeast(0)) ?: 0

    override fun initialize(properties: ParquetProperties) {
        this.properties = properties
        defaults.initialize(properties)
    }

    override fun newValuesWriter(path: ColumnDescriptor): ValuesWriter {
        if (path.type == PrimitiveTypeName.BOOLEAN &&
            properties.writerVersion == ParquetProperties.WriterVersion.PARQUET_1_0
        ) {
            return own(BooleanWriter())
        }
        val creation = Slot()
        val fixed = fixedFallbackBytes(path)
        // The stock factory constructs a small dictionary before its fallback becomes accessible.
        val stockDictionary =
            properties.isDictionaryEnabled(path) &&
                path.type != PrimitiveTypeName.BOOLEAN &&
                !(
                    path.type == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY &&
                        properties.writerVersion == ParquetProperties.WriterVersion.PARQUET_1_0
                )
        val keyWidth = if (path.type == PrimitiveTypeName.INT32 || path.type == PrimitiveTypeName.FLOAT) 4 else 8
        creation.set(add(if (stockDictionary) mapBytes(32, keyWidth) else 0, fixed))
        try {
            val stock = defaults.newValuesWriter(path)
            if (stock !is FallbackValuesWriter<*, *>) return own(Fallback(stock, fixed))
            stock.initialWriter.close()
            val fallback = own(Fallback(stock.fallBackWriter, fixed))
            val dictionary = Dictionary(path)
            own(dictionary)
            dictionaries[path] = dictionary
            return own(FallbackValuesWriter.of(dictionary, fallback))
        } finally {
            creation.set(0)
        }
    }

    private fun own(writer: ValuesWriter): ValuesWriter = Tracked(writer).also { writers.add(it) }

    override fun close() {
        var failure: Throwable? = null
        for (writer in writers.asReversed()) {
            try {
                writer.close()
            } catch (error: Throwable) {
                if (failure == null) {
                    failure = error
                } else if (failure !== error) {
                    failure.addSuppressed(error)
                }
            }
        }
        writers.clear()
        dictionaries.clear()
        failure?.let { throw it }
    }

    internal class Tracked(
        internal val delegate: ValuesWriter,
    ) : ValuesWriter() {
        private var closed = false

        override fun getBufferedSize(): Long = delegate.bufferedSize

        override fun getAllocatedSize(): Long = delegate.allocatedSize

        override fun getBytes(): BytesInput = delegate.bytes

        override fun getEncoding(): Encoding = delegate.encoding

        override fun memUsageString(prefix: String): String = delegate.memUsageString(prefix)

        override fun writeByte(value: Int) = delegate.writeByte(value)

        override fun writeBoolean(value: Boolean) = delegate.writeBoolean(value)

        override fun writeInteger(value: Int) = delegate.writeInteger(value)

        override fun writeLong(value: Long) = delegate.writeLong(value)

        override fun writeFloat(value: Float) = delegate.writeFloat(value)

        override fun writeDouble(value: Double) = delegate.writeDouble(value)

        override fun writeBytes(value: Binary) = delegate.writeBytes(value)

        override fun reset() = delegate.reset()

        override fun resetDictionary() = delegate.resetDictionary()

        override fun toDictPageAndClose(): DictionaryPage? = delegate.toDictPageAndClose()

        override fun close() {
            if (closed) return
            closed = true
            delegate.close()
        }
    }

    private inner class Slot {
        var bytes: Long = 0
            private set

        fun set(next: Long) {
            if (next == bytes) return
            val total = add(unmanagedBytes - bytes, next)
            replaceUnmanagedBytes(total)
            unmanagedBytes = total
            bytes = next
        }
    }

    private inner class IntMap(private val charge: Slot) : Int2IntLinkedOpenHashMap(16, LOAD_FACTOR) {
        init {
            defaultReturnValue(-1)
        }

        val capacity: Int get() = n

        override fun rehash(newCapacity: Int) {
            val next = mapBytes(newCapacity, 4)
            charge.set(add(charge.bytes, next))
            try {
                super.rehash(newCapacity)
            } finally {
                charge.set(next)
            }
        }
    }

    private inner class LongMap(private val charge: Slot) : Long2IntLinkedOpenHashMap(16, LOAD_FACTOR) {
        init {
            defaultReturnValue(-1)
        }

        val capacity: Int get() = n

        override fun rehash(newCapacity: Int) {
            val next = mapBytes(newCapacity, 8)
            charge.set(add(charge.bytes, next))
            try {
                super.rehash(newCapacity)
            } finally {
                charge.set(next)
            }
        }
    }

    private inner class BinaryMap(private val charge: Slot) : Object2IntLinkedOpenHashMap<Binary>(16, LOAD_FACTOR) {
        init {
            defaultReturnValue(-1)
        }

        val capacity: Int get() = n

        override fun rehash(newCapacity: Int) {
            val next = mapBytes(newCapacity, 8)
            charge.set(add(charge.bytes, next))
            try {
                super.rehash(newCapacity)
            } finally {
                charge.set(next)
            }
        }
    }

    private inner class Ids : IntList() {
        private val slabs = Slot()
        private val iterators = Slot()

        private var slabSize = 4096
        private var remaining = 0
        private var slabBytes = 0L
        private var previousSlabs = 0
        private var listCapacity = 0

        override fun add(value: Int) {
            if (remaining == 0) {
                val oldList = if (listCapacity == 0) 0 else arrayBytes(listCapacity.toLong() * 8)
                var nextList = oldList
                if (slabBytes > 0) {
                    if (previousSlabs == listCapacity) {
                        listCapacity = if (listCapacity == 0) 10 else listCapacity + listCapacity / 2
                        nextList = arrayBytes(listCapacity.toLong() * 8)
                    }
                    previousSlabs++
                    slabSize = minOf(65536, slabSize * 2)
                }
                val nextSlabs = add(slabBytes, arrayBytes(slabSize.toLong() * 4))
                slabs.set(add(nextSlabs, nextList, if (nextList != oldList) oldList else 0))
                super.add(value)
                slabBytes = nextSlabs
                slabs.set(add(slabBytes, nextList))
                remaining = slabSize - 1
            } else {
                super.add(value)
                remaining--
            }
        }

        override fun iterator(): IntIterator {
            if (slabBytes == 0L) {
                slabBytes = arrayBytes(4096L * 4)
                slabs.set(slabBytes)
                remaining = 4096
            }
            // IntList creates a new reference array for each iterator.
            iterators.set(add(iterators.bytes, arrayBytes((previousSlabs + 1L) * 8), aligned(64)))
            return super.iterator()
        }

        fun forecast(additional: Long): Long {
            var needed = (additional - remaining).coerceAtLeast(0)
            var size = if (slabBytes == 0L) 4096 else minOf(65536, slabSize * 2)
            var extra = 0L
            var newSlabs = 0L
            while (needed > 0 && size < 65536) {
                extra = add(extra, arrayBytes(size.toLong() * 4))
                newSlabs++
                needed -= size
                size = minOf(65536, size * 2)
            }
            if (needed > 0) {
                val fullSlabs = 1 + (needed - 1) / 65536
                extra = add(extra, multiply(fullSlabs, arrayBytes(65536L * 4)))
                newSlabs = add(newSlabs, fullSlabs)
            }
            val desiredPrevious = add(previousSlabs.toLong(), newSlabs) - if (slabBytes == 0L && newSlabs > 0) 1 else 0
            var nextList = listCapacity.toLong()
            while (nextList < desiredPrevious) {
                nextList = if (nextList == 0L) 10 else add(nextList, nextList / 2)
                if (nextList == Long.MAX_VALUE) break
            }
            if (nextList > listCapacity) extra = add(extra, arrayBytes(multiply(nextList, 8)))
            // First-page rejection can create one encoding iterator and one fallback iterator.
            extra = add(extra, multiply(2, arrayBytes(multiply(add(desiredPrevious, 1), 8))), aligned(64L * 2))
            return extra
        }

        fun release() {
            slabs.set(0)
            iterators.set(0)
        }
    }

    private inner class Dictionary(
        private val path: ColumnDescriptor,
    ) : DictionaryValuesWriter(
            properties.dictionaryPageSizeThreshold,
            if (properties.writerVersion == ParquetProperties.WriterVersion.PARQUET_1_0) {
                Encoding.PLAIN_DICTIONARY
            } else {
                Encoding.RLE_DICTIONARY
            },
            if (properties.writerVersion == ParquetProperties.WriterVersion.PARQUET_1_0) {
                Encoding.PLAIN_DICTIONARY
            } else {
                Encoding.PLAIN
            },
            properties.allocator,
        ) {
        private val mapCharge = Slot()
        private val payloadCharge = Slot()
        private val encoderCharge = Slot()
        private var ints: IntMap? = null
        private var longs: LongMap? = null
        private var binaries: BinaryMap? = null
        private var ids: Ids? = Ids()
        private var resetting = false
        private var active = true
        private val type = path.type
        private val fixedLength =
            when (type) {
                PrimitiveTypeName.INT96 -> 12
                PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY -> path.typeLength
                else -> 0
            }

        init {
            encodedValues = ids
        }

        private fun intMap(): IntMap =
            ints ?: run {
                mapCharge.set(mapBytes(32, 4))
                IntMap(mapCharge).also { ints = it }
            }

        private fun longMap(): LongMap =
            longs ?: run {
                mapCharge.set(mapBytes(32, 8))
                LongMap(mapCharge).also { longs = it }
            }

        private fun binaryMap(): BinaryMap =
            binaries ?: run {
                mapCharge.set(mapBytes(32, 8))
                BinaryMap(mapCharge).also { binaries = it }
            }

        private fun integer(value: Int) {
            val map = intMap()
            var id = map.get(value)
            if (id == -1) {
                id = map.size
                map.put(value, id)
                dictionaryByteSize += 4
            }
            encodedValues.add(id)
        }

        override fun writeInteger(value: Int) = integer(value)

        override fun writeFloat(value: Float) = integer(java.lang.Float.floatToRawIntBits(value))

        private fun long(value: Long) {
            val map = longMap()
            var id = map.get(value)
            if (id == -1) {
                id = map.size
                map.put(value, id)
                dictionaryByteSize += 8
            }
            encodedValues.add(id)
        }

        override fun writeLong(value: Long) = long(value)

        override fun writeDouble(value: Double) = long(java.lang.Double.doubleToRawLongBits(value))

        override fun writeBytes(value: Binary) {
            val map = binaryMap()
            var id = map.getInt(value)
            if (id == -1) {
                // Constant payloads still outlive merge heads while the dictionary owns them.
                // The wrapper bound includes Binary and an optional retained heap ByteBuffer.
                payloadCharge.set(add(payloadCharge.bytes, arrayBytes(value.length().toLong()), aligned(128)))
                id = map.size
                map.put(dictionaryKey(value), id)
                dictionaryByteSize += if (fixedLength > 0) fixedLength.toLong() else value.length().toLong() + 4
            }
            encodedValues.add(id)
        }

        private fun dictionaryKey(value: Binary): Binary {
            if (value.isBackingBytesReused) return value.copy()
            val buffer = value.toByteBuffer()
            if (!buffer.isDirect && (!buffer.hasArray() || buffer.array().size != value.length())) {
                return Binary.fromConstantByteArray(value.bytes)
            }
            return value.copy()
        }

        public override fun getDictionarySize(): Int = ints?.size ?: longs?.size ?: binaries?.size ?: 0

        override fun getBytes(): BytesInput {
            // The RLE encoder retains a packed-byte array, eight integers, and its byte header.
            encoderCharge.set(add(encoderCharge.bytes, arrayBytes(32), arrayBytes(8L * 4), arrayBytes(1)))
            return super.getBytes()
        }

        override fun clearDictionaryContent() {
            ints?.clear()
            longs?.clear()
            binaries?.clear()
            payloadCharge.set(0)
        }

        override fun fallBackAllValuesTo(writer: ValuesWriter) {
            super.fallBackAllValuesTo(writer)
            active = false
            if (lastUsedDictionarySize == 0) replaceIds()
        }

        override fun fallBackDictionaryEncodedData(writer: ValuesWriter) {
            val temporary = Slot()
            val count = dictionarySize
            temporary.set(arrayBytes(count.toLong() * if (ints != null) 4 else 8))
            try {
                when {
                    ints != null -> {
                        val reverse = IntArray(count)
                        val keys = checkNotNull(ints).keys.iterator()
                        for (index in reverse.indices) reverse[index] = keys.nextInt()
                        val values = encodedValues.iterator()
                        while (values.hasNext()) {
                            val key = reverse[values.next()]
                            if (type == PrimitiveTypeName.FLOAT) {
                                writer.writeFloat(java.lang.Float.intBitsToFloat(key))
                            } else {
                                writer.writeInteger(key)
                            }
                        }
                    }
                    longs != null -> {
                        val reverse = LongArray(count)
                        val keys = checkNotNull(longs).keys.iterator()
                        for (index in reverse.indices) reverse[index] = keys.nextLong()
                        val values = encodedValues.iterator()
                        while (values.hasNext()) {
                            val key = reverse[values.next()]
                            if (type == PrimitiveTypeName.DOUBLE) {
                                writer.writeDouble(java.lang.Double.longBitsToDouble(key))
                            } else {
                                writer.writeLong(key)
                            }
                        }
                    }
                    else -> {
                        val reverse = arrayOfNulls<Binary>(count)
                        val keys = binaries?.keys?.iterator()
                        for (index in reverse.indices) reverse[index] = checkNotNull(keys).next()
                        val values = encodedValues.iterator()
                        while (values.hasNext()) writer.writeBytes(checkNotNull(reverse[values.next()]))
                    }
                }
            } finally {
                temporary.set(0)
            }
        }

        override fun toDictPageAndClose(): DictionaryPage? {
            if (lastUsedDictionarySize == 0) return null
            encoderCharge.set(add(encoderCharge.bytes, arrayBytes(8)))
            val writer =
                if (fixedLength > 0) {
                    FixedLenByteArrayPlainValuesWriter(
                        fixedLength,
                        lastUsedDictionaryByteSize,
                        maxDictionaryByteSize,
                        allocator,
                    )
                } else {
                    PlainValuesWriter(lastUsedDictionaryByteSize, maxDictionaryByteSize, allocator)
                }
            when {
                ints != null -> {
                    val keys = checkNotNull(ints).keys.iterator()
                    repeat(lastUsedDictionarySize) {
                        val key = keys.nextInt()
                        if (type == PrimitiveTypeName.FLOAT) {
                            writer.writeFloat(java.lang.Float.intBitsToFloat(key))
                        } else {
                            writer.writeInteger(key)
                        }
                    }
                }
                longs != null -> {
                    val keys = checkNotNull(longs).keys.iterator()
                    repeat(lastUsedDictionarySize) {
                        val key = keys.nextLong()
                        if (type == PrimitiveTypeName.DOUBLE) {
                            writer.writeDouble(java.lang.Double.longBitsToDouble(key))
                        } else {
                            writer.writeLong(key)
                        }
                    }
                }
                else -> {
                    val keys = checkNotNull(binaries).keys.iterator()
                    repeat(lastUsedDictionarySize) { writer.writeBytes(keys.next()) }
                }
            }
            return dictPage(writer)
        }

        private fun replaceIds() {
            val previous = ids
            ids = Ids()
            encodedValues = ids
            previous?.release()
        }

        override fun reset() {
            resetting = true
            try {
                super.reset()
                replaceIds()
            } finally {
                resetting = false
            }
        }

        override fun resetDictionary() {
            super.resetDictionary()
            active = true
        }

        override fun close() {
            try {
                super.close()
            } finally {
                val previous = ids
                ids = null
                previous?.release()
                encoderCharge.set(0)
                if (!resetting) {
                    ints = null
                    longs = null
                    binaries = null
                    mapCharge.set(0)
                    payloadCharge.set(0)
                    active = false
                }
            }
        }

        fun forecast(additional: Long): Long {
            if (!active || additional == 0L) return 0
            val currentCapacity = ints?.capacity ?: longs?.capacity ?: binaries?.capacity ?: 0
            val currentSize = dictionarySize.toLong()
            val width = if (type == PrimitiveTypeName.INT32 || type == PrimitiveTypeName.FLOAT) 4 else 8
            val entryBytes =
                if (binaries != null || type == PrimitiveTypeName.BINARY) {
                    4
                } else if (fixedLength > 0) {
                    fixedLength
                } else {
                    width
                }
            val desired = minOf(add(currentSize, additional), maxDictionaryByteSize.toLong() / entryBytes + 1)
            val nextCapacity =
                if (desired > Int.MAX_VALUE - 1L) {
                    1 shl 30
                } else if (currentCapacity > 0 && desired <= HashCommon.maxFill(currentCapacity, LOAD_FACTOR)) {
                    currentCapacity
                } else {
                    maxOf(32, HashCommon.arraySize((desired + 1).toInt(), LOAD_FACTOR))
                }
            val mapExtra =
                if (nextCapacity == currentCapacity) {
                    0
                } else {
                    add(
                        mapBytes(nextCapacity, width),
                        if (nextCapacity > 32) mapBytes(nextCapacity / 2, width) else 0,
                    ) - mapCharge.bytes
                }
            return add(
                mapExtra.coerceAtLeast(0),
                ids?.forecast(additional) ?: 0,
                arrayBytes(desired.coerceAtMost(Int.MAX_VALUE.toLong()) * width),
                arrayBytes(32),
                arrayBytes(8L * 4),
                arrayBytes(1),
            )
        }
    }

    private inner class Fallback(
        writer: ValuesWriter,
        fixedBytes: Long,
    ) : ValuesWriter() {
        private var target: ValuesWriter? = writer
        private val delegate: ValuesWriter get() = checkNotNull(target)
        private val fixed = Slot().also { it.set(fixedBytes) }
        private val previous = Slot()
        private val delta = writer is DeltaByteArrayWriter

        override fun getBufferedSize(): Long = delegate.bufferedSize

        override fun getAllocatedSize(): Long = delegate.allocatedSize

        override fun getBytes(): BytesInput = delegate.bytes

        override fun getEncoding(): Encoding = delegate.encoding

        override fun memUsageString(prefix: String): String = delegate.memUsageString(prefix)

        override fun writeByte(value: Int) = delegate.writeByte(value)

        override fun writeBoolean(value: Boolean) = delegate.writeBoolean(value)

        override fun writeInteger(value: Int) = delegate.writeInteger(value)

        override fun writeLong(value: Long) = delegate.writeLong(value)

        override fun writeFloat(value: Float) = delegate.writeFloat(value)

        override fun writeDouble(value: Double) = delegate.writeDouble(value)

        override fun writeBytes(value: Binary) {
            if (!delta) {
                delegate.writeBytes(value)
                return
            }
            val next = arrayBytes(value.length().toLong())
            previous.set(add(previous.bytes, next))
            delegate.writeBytes(value)
            previous.set(next)
        }

        override fun reset() {
            delegate.reset()
            previous.set(0)
        }

        override fun resetDictionary() = delegate.resetDictionary()

        override fun toDictPageAndClose(): DictionaryPage? = delegate.toDictPageAndClose()

        override fun close() {
            val writer = target ?: return
            target = null
            try {
                writer.close()
            } finally {
                previous.set(0)
                fixed.set(0)
            }
        }
    }

    private inner class BooleanWriter : ValuesWriter() {
        private val output = CapacityByteArrayOutputStream(1024, 64 * 1024, properties.allocator)
        private var current = 0
        private var bits = 0

        override fun writeBoolean(value: Boolean) {
            if (value) current = current or (1 shl bits)
            bits++
            if (bits == 8) {
                output.write(current)
                current = 0
                bits = 0
            }
        }

        override fun getBufferedSize(): Long = output.size() + if (bits > 0) 1 else 0

        override fun getAllocatedSize(): Long = output.capacity.toLong()

        override fun getEncoding(): Encoding = Encoding.PLAIN

        override fun getBytes(): BytesInput {
            if (bits > 0) {
                output.write(current)
                current = 0
                bits = 0
            }
            return BytesInput.from(output)
        }

        override fun reset() {
            output.reset()
            current = 0
            bits = 0
        }

        override fun close() = output.close()

        override fun memUsageString(prefix: String): String = output.memUsageString(prefix)
    }

    private fun fixedFallbackBytes(path: ColumnDescriptor): Long {
        if (properties.isByteStreamSplitEnabled(path)) {
            val width =
                when (path.type) {
                    PrimitiveTypeName.INT32, PrimitiveTypeName.FLOAT -> 4
                    PrimitiveTypeName.INT64, PrimitiveTypeName.DOUBLE -> 8
                    else -> path.typeLength
                }
            return add(arrayBytes(width.toLong() * 8), arrayBytes(width.toLong() * 8), arrayBytes(width.toLong()))
        }
        // Plain and fixed-plain writers own one LittleEndianDataOutputStream scratch array.
        val plain = arrayBytes(8)
        if (properties.writerVersion == ParquetProperties.WriterVersion.PARQUET_1_0) return plain
        val integer = add(arrayBytes(128L * 4), arrayBytes(32L * 32), arrayBytes(4L * 4))
        return when (path.type) {
            PrimitiveTypeName.INT32 -> integer
            PrimitiveTypeName.INT64 -> add(arrayBytes(128L * 8), arrayBytes(32L * 64), arrayBytes(4L * 4))
            PrimitiveTypeName.BINARY, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY ->
                add(
                    integer,
                    integer,
                    arrayBytes(0),
                    plain,
                )
            PrimitiveTypeName.BOOLEAN -> add(arrayBytes(1), arrayBytes(8L * 4))
            else -> plain
        }
    }

    private fun mapBytes(
        capacity: Int,
        keyWidth: Int,
    ): Long {
        val length = capacity.toLong() + 1
        return add(arrayBytes(length * keyWidth), arrayBytes(length * 4), arrayBytes(length * 8))
    }

    private fun arrayBytes(payload: Long): Long = aligned(add(32, payload))

    private fun aligned(value: Long): Long {
        val padding = alignment - 1
        return if (value > Long.MAX_VALUE - padding) Long.MAX_VALUE else (value + padding) / alignment * alignment
    }

    private fun add(
        a: Long,
        b: Long,
    ): Long = if (a < 0 || b < 0 || b > Long.MAX_VALUE - a) Long.MAX_VALUE else a + b

    private fun add(
        a: Long,
        b: Long,
        c: Long,
    ): Long = add(add(a, b), c)

    private fun add(
        a: Long,
        b: Long,
        c: Long,
        d: Long,
    ): Long = add(add(a, b), add(c, d))

    private fun add(
        a: Long,
        b: Long,
        c: Long,
        d: Long,
        e: Long,
        f: Long,
    ): Long = add(add(a, b, c), add(d, e, f))

    private fun multiply(
        a: Long,
        b: Long,
    ): Long = if (a < 0 || b < 0 || a != 0L && b > Long.MAX_VALUE / a) Long.MAX_VALUE else a * b

    private companion object {
        const val LOAD_FACTOR = 0.75f
        val alignment: Long =
            runCatching {
                ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean::class.java)
                    .getVMOption("ObjectAlignmentInBytes").value.toLong()
            }.getOrDefault(256)
    }
}
