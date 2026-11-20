package com.posthog.hoglake.compaction

import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import org.apache.parquet.example.data.GroupWriter
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.math.BigInteger

class SortMemoryTest {
    @Test
    fun `flat event charge preserves the four-run row ceiling`() {
        val memory = SortMemory(1L shl 30, compressedReferences = true)
        val row = memory.RowBudget("event")
        row.start(10)
        for (size in listOf(36, 18, 36, 32, 3, 7, 7)) row.value(size.toLong())
        repeat(3) { row.value() }
        row.keys(1)
        assertThat(row.spent).isEqualTo(1971)
        val rowCap = memory.bytes / (192 * 11)
        assertThat(rowCap * row.spent).isLessThan(memory.bytes)
        assertThat((2_000_000 + rowCap - 1) / rowCap).isEqualTo(4)
    }

    @Test
    fun `sparse fields charge empty lists rather than absent value wrappers`() {
        fun charge(compressed: Boolean): Long {
            val row = SortMemory(1L shl 30, compressed).RowBudget("sparse")
            row.start(151)
            row.value(4)
            row.keys(1)
            return row.spent
        }
        assertThat(charge(true)).isEqualTo(5196).isGreaterThan(4496)
        assertThat(charge(false)).isEqualTo(7724).isGreaterThan(6376)
        assertThat(charge(true)).isLessThan(192L * 152 / 4)
    }

    @Test
    fun `binary allocation is refused before the guarded converter copies it`() {
        val schema = Types.buildMessage().optional(PrimitiveTypeName.BINARY).named("value").named("t")
        val memory = SortMemory(1024)
        val budget = memory.RowBudget("large-value")
        val materializer =
            ParquetRewriter.budgetedMaterializer(schema, ParquetRewriter.NodeBudget(100, "large-value"), budget)
        val root = materializer.rootConverter
        root.start()
        val spent = budget.spent
        val failure =
            assertThrows<MergeBudgetExceededException> {
                root.getConverter(0).asPrimitiveConverter().addBinary(Binary.fromConstantByteArray(ByteArray(2048)))
            }
        assertThat(failure.oversizedRow).isTrue()
        assertThat(budget.spent).isEqualTo(spent)
    }

    @Test
    fun `emitted row measurement matches charged readback with duplicate decimal and binary keys`() {
        val schema =
            Types.buildMessage()
                .addField(Types.optional(PrimitiveTypeName.BINARY).id(1).named("key"))
                .addField(
                    Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(16)
                        .`as`(LogicalTypeAnnotation.decimalType(2, 38)).id(2).named("decimal"),
                )
                .addField(Types.optionalGroup().optional(PrimitiveTypeName.INT64).named("null").id(3).named("nested"))
                .named("t")
        for (compressed in listOf(true, false)) {
            val memory = SortMemory(1L shl 30, compressed)
            val budget = memory.RowBudget("readback")
            val materializer =
                ParquetRewriter.budgetedMaterializer(schema, ParquetRewriter.NodeBudget(100, "readback"), budget)
            val root = materializer.rootConverter
            root.start()
            root.getConverter(0).asPrimitiveConverter().addBinary(Binary.fromConstantByteArray(byteArrayOf(1, 2, 3)))
            root.getConverter(1).asPrimitiveConverter().addBinary(
                Binary.fromConstantByteArray(ByteArray(16).apply { this[15] = 12 }),
            )
            root.getConverter(2).asGroupConverter().apply {
                start()
                end()
            }
            root.end()
            val group = materializer.currentRecord
            val keys =
                ParquetRewriter.SortKeys(
                    schema,
                    listOf(1L, 2L, 1L, 2L).map { SortFieldDef(it, SortDirection.ASC, NullOrder.NULLS_LAST) },
                ).extract(group, "readback", budget)
            assertThat(keys[1]).isEqualTo(BigInteger.valueOf(12))
            val observer = memory.MeasuringConsumer(schema, IgnoringConsumer())
            GroupWriter(observer, schema).write(group)
            assertThat(
                SortMemory.add(observer.rowBytes, memory.keyBytes(keys, intArrayOf(0, 16, 0, 16))),
            ).isEqualTo(budget.spent)
        }
    }

    @Test
    fun `cached nested converters release large omitted groups at each row boundary`() {
        val schema =
            Types.buildMessage()
                .optionalGroup().optional(PrimitiveTypeName.BINARY).named("payload").named("a")
                .optionalGroup().optional(PrimitiveTypeName.BINARY).named("payload").named("b")
                .named("t")
        val memory = SortMemory(64L shl 20)
        val materializer =
            ParquetRewriter.budgetedMaterializer(
                schema,
                ParquetRewriter.NodeBudget(100, "nested"),
                memory.RowBudget("nested"),
            )
        val root = materializer.rootConverter

        fun decode(index: Int): org.apache.parquet.example.data.Group {
            root.start()
            root.getConverter(index).asGroupConverter().apply {
                start()
                getConverter(0).asPrimitiveConverter().addBinary(Binary.fromConstantByteArray(ByteArray(8 shl 20)))
                end()
            }
            root.end()
            return materializer.currentRecord
        }
        val first = decode(0)
        assertThat(first.getGroup(0, 0).getBinary(0, 0).length()).isEqualTo(8 shl 20)
        assertThat(materializer.retainedGroupCount()).isZero()
        val second = decode(1)
        assertThat(second.getFieldRepetitionCount(0)).isZero()
        assertThat(second.getGroup(1, 0).getBinary(0, 0).length()).isEqualTo(8 shl 20)
        assertThat(materializer.retainedGroupCount()).isZero()
    }

    @Test
    fun `run arithmetic saturates and includes an overlapping replacement head`() {
        assertThat(SortMemory.runBytes(100, 20)).isEqualTo(140)
        assertThat(SortMemory.runBytes(Long.MAX_VALUE, 1)).isEqualTo(Long.MAX_VALUE)
        assertThat(SortMemory.multiply(Long.MAX_VALUE, 2)).isEqualTo(Long.MAX_VALUE)
        assertThat(SortMemory.fits(Long.MAX_VALUE, Long.MAX_VALUE)).isFalse()
        assertThat(SortMemory.fits(Long.MAX_VALUE - 1, Long.MAX_VALUE)).isTrue()
        assertThat(SortMemory.remaining(100, Long.MAX_VALUE, Long.MAX_VALUE)).isZero()
        assertThat(SortMemory.remaining(Long.MAX_VALUE, Long.MAX_VALUE - 10, 1)).isEqualTo(9)
        assertThat(SortMemory.excess(Long.MAX_VALUE, 32L shl 20)).isEqualTo(Long.MAX_VALUE)
        assertThat(SortMemory.excess(100, 20)).isEqualTo(80)
        assertThrows<MergeBudgetExceededException> {
            SortMemory(Long.MAX_VALUE).RowBudget("overflow").value(Long.MAX_VALUE)
        }
    }

    private class IgnoringConsumer : RecordConsumer() {
        override fun startMessage() {}

        override fun endMessage() {}

        override fun startField(
            name: String,
            index: Int,
        ) {}

        override fun endField(
            name: String,
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
