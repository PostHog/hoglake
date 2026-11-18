package com.posthog.hoglake.compaction

import com.sun.management.HotSpotDiagnosticMXBean
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.GroupType
import java.lang.management.ManagementFactory
import java.math.BigInteger

/** Charges detached rows independently from their compressed representation. */
internal class SortMemory(
    val bytes: Long,
    private val compressedReferences: Boolean = compressedOops,
) {
    class DecodeStats(var readers: Long = 0, var rows: Long = 0)

    private val padding = (objectAlignment - 8).coerceAtLeast(0)
    private val fieldBytes = (if (compressedReferences) 32L else 48L) + padding
    private val valueBytes = (if (compressedReferences) 128L else 192L) + 4 * padding
    private val rowBytes = (if (compressedReferences) 64L else 96L) + 2 * padding
    private val keyArrayBytes = 32 + padding
    private val keyValueBytes = (if (compressedReferences) 64L else 80L) + 3 * padding

    /** Includes the group header, field array, and empty field lists. */
    fun groupBytes(fields: Int): Long = add(64, 2 * padding, multiply(fieldBytes, fields.toLong()))

    /** Includes the value wrapper, list growth, binary wrapper, and array header. */
    fun valueBytes(payload: Long = 0): Long = add(valueBytes, payload)

    fun keyBytes(
        keys: Array<Any?>,
        decimalWidths: IntArray = intArrayOf(),
    ): Long =
        keys.foldIndexed(add(keyArrayBytes, multiply(8, keys.size.toLong()))) { index, sum, key ->
            add(
                sum,
                keyValueBytes,
                when (key) {
                    is ByteArray -> multiply(2, key.size.toLong())
                    is BigInteger ->
                        multiply(
                            2,
                            maxOf(decimalWidths.getOrElse(index) { 0 }.toLong(), key.bitLength().toLong() / 8 + 1),
                        )
                    else -> 0
                },
            )
        }

    /** A callback can spill retained rows before the next allocation. */
    inner class RowBudget(
        private val label: String,
        val limit: Long = bytes,
        private val makeRoom: (Long, Long) -> Unit = { _, _ -> },
    ) {
        var spent: Long = 0
            private set

        fun start(fields: Int) {
            spent = 0
            charge(add(rowBytes, groupBytes(fields)))
        }

        fun group(fields: Int) = charge(add(valueBytes(), groupBytes(fields)))

        fun value(payload: Long = 0) = charge(valueBytes(payload))

        fun keys(count: Int) = charge(add(keyArrayBytes, multiply(8 + keyValueBytes, count.toLong())))

        fun keyPayload(bytes: Long) = charge(multiply(2, bytes))

        private fun charge(amount: Long) {
            val next = add(spent, amount)
            if (!fits(next, limit)) {
                throw MergeBudgetExceededException(
                    "a row in $label needs $next B. The row allowance is $limit B.",
                    0,
                    0,
                    0,
                    oversizedRow = true,
                )
            }
            makeRoom(next, spent)
            spent = next
        }
    }

    /** Measures the representation that a spill reader decodes, without building another graph. */
    inner class MeasuringConsumer(
        private val schema: GroupType,
        private val delegate: RecordConsumer,
        private val measureEncoding: Boolean = false,
        private val binaryObserved: ((List<Int>, Binary) -> Unit)? = null,
        private val measureRetained: Boolean = true,
        private val primitiveObserved: ((List<Int>) -> Unit)? = null,
    ) : RecordConsumer() {
        private val groups = ArrayList<GroupType>()
        private val fields = ArrayList<Int>()
        var rowBytes: Long = 0
            private set
        var encodingBytes: Long = 0
            private set
        private val encodingControls = java.util.IdentityHashMap<GroupType, Long>()

        private fun controls(group: GroupType): Long =
            encodingControls.getOrPut(group) {
                // Each level needs at most a five-byte run header and a four-byte value.
                // Two levels cover repetition and definition controls.
                multiply(
                    18,
                    group.fields.sumOf { if (it.isPrimitive) 1 else controls(it.asGroupType()) / 18 },
                )
            }

        override fun startMessage() {
            groups.clear()
            groups += schema
            fields.clear()
            fields += 0
            if (measureRetained) rowBytes = add(this@SortMemory.rowBytes, groupBytes(schema.fieldCount))
            if (measureEncoding) encodingBytes = controls(schema)
            delegate.startMessage()
        }

        override fun endMessage() = delegate.endMessage()

        override fun startField(
            name: String,
            index: Int,
        ) {
            fields[fields.lastIndex] = index
            delegate.startField(name, index)
        }

        override fun endField(
            name: String,
            index: Int,
        ) = delegate.endField(name, index)

        override fun startGroup() {
            val group = groups.last().getType(fields.last()).asGroupType()
            if (measureRetained) rowBytes = add(rowBytes, valueBytes(), groupBytes(group.fieldCount))
            if (measureEncoding) encodingBytes = add(encodingBytes, controls(group))
            groups += group
            fields += 0
            delegate.startGroup()
        }

        override fun endGroup() {
            groups.removeAt(groups.lastIndex)
            fields.removeAt(fields.lastIndex)
            delegate.endGroup()
        }

        private fun value(payload: Long = 0) {
            if (measureRetained) rowBytes = add(rowBytes, valueBytes(payload))
            primitiveObserved?.invoke(fields)
        }

        override fun addBinary(value: Binary) {
            value(value.length().toLong())
            if (measureEncoding) encodingBytes = add(encodingBytes, 4, value.length().toLong())
            binaryObserved?.invoke(fields, value)
            delegate.addBinary(value)
        }

        override fun addBoolean(value: Boolean) {
            value()
            if (measureEncoding) encodingBytes = add(encodingBytes, 1)
            delegate.addBoolean(value)
        }

        override fun addInteger(value: Int) {
            value()
            if (measureEncoding) encodingBytes = add(encodingBytes, 4)
            delegate.addInteger(value)
        }

        override fun addLong(value: Long) {
            value()
            if (measureEncoding) encodingBytes = add(encodingBytes, 8)
            delegate.addLong(value)
        }

        override fun addFloat(value: Float) {
            value()
            if (measureEncoding) encodingBytes = add(encodingBytes, 4)
            delegate.addFloat(value)
        }

        override fun addDouble(value: Double) {
            value()
            if (measureEncoding) encodingBytes = add(encodingBytes, 8)
            delegate.addDouble(value)
        }

        override fun flush() = delegate.flush()
    }

    companion object {
        private val compressedOops =
            runCatching {
                val bean = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean::class.java)
                bean.getVMOption("UseCompressedOops").value.toBoolean() &&
                    bean.getVMOption("UseCompressedClassPointers").value.toBoolean()
            }.getOrDefault(false)
        private val objectAlignment =
            runCatching {
                ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean::class.java)
                    .getVMOption("ObjectAlignmentInBytes").value.toLong()
            }.getOrDefault(256)

        /** Treat invalid sizes and overflow as unaffordable. */
        fun add(
            a: Long,
            b: Long,
        ): Long = if (a < 0 || b < 0 || b > Long.MAX_VALUE - a) Long.MAX_VALUE else a + b

        /** Common hot paths avoid vararg arrays. */
        fun add(
            a: Long,
            b: Long,
            c: Long,
        ): Long = add(add(a, b), c)

        fun add(
            a: Long,
            b: Long,
            c: Long,
            d: Long,
        ): Long = add(add(a, b, c), d)

        fun add(
            a: Long,
            b: Long,
            c: Long,
            d: Long,
            e: Long,
        ): Long = add(add(a, b, c, d), e)

        fun add(vararg values: Long): Long {
            var total = 0L
            for (value in values) {
                total = add(total, value)
            }
            return total
        }

        fun multiply(
            a: Long,
            b: Long,
        ): Long = if (a < 0 || b < 0 || (a != 0L && b > Long.MAX_VALUE / a)) Long.MAX_VALUE else a * b

        /** The overflow sentinel never fits, including under a maximum-valued budget. */
        fun fits(
            bytes: Long,
            allowance: Long,
        ): Boolean = bytes >= 0 && bytes != Long.MAX_VALUE && bytes <= allowance

        /** Subtract only after the full retained total is known to fit. */
        fun remaining(
            allowance: Long,
            retained: Long,
        ): Long = if (fits(retained, allowance)) allowance - retained else 0

        fun remaining(
            allowance: Long,
            a: Long,
            b: Long,
        ): Long = remaining(allowance, add(a, b))

        fun remaining(
            allowance: Long,
            a: Long,
            b: Long,
            c: Long,
        ): Long = remaining(allowance, add(a, b, c))

        fun remaining(
            allowance: Long,
            vararg retained: Long,
        ): Long {
            var total = 0L
            for (value in retained) total = add(total, value)
            return remaining(allowance, total)
        }

        /** Removing a fixed reserve must not turn an overflow sentinel into a finite charge. */
        fun excess(
            total: Long,
            reserve: Long,
        ): Long =
            if (!fits(total, Long.MAX_VALUE) || !fits(reserve, Long.MAX_VALUE)) {
                Long.MAX_VALUE
            } else {
                (total - reserve).coerceAtLeast(0)
            }

        /** A replacement row can overlap its previous merge head. */
        fun runBytes(
            readerBytes: Long,
            headBytes: Long,
        ): Long = add(readerBytes, multiply(2, headBytes))

        fun readerFailure(error: SortedReaderMemoryException): MergeBudgetExceededException =
            MergeBudgetExceededException(
                error.message ?: "sorted reader exceeds its allowance.",
                0,
                0,
                0,
                oversizedRow = error.binaryValueRefusal,
            )
                .apply { initCause(error) }
    }
}
