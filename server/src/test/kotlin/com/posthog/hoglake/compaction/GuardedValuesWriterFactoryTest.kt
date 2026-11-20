package com.posthog.hoglake.compaction

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.Encoding
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.column.values.ValuesWriter
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter
import org.apache.parquet.column.values.factory.ValuesWriterFactory
import org.apache.parquet.column.values.fallback.FallbackValuesWriter
import org.apache.parquet.hadoop.CodecFactory
import org.apache.parquet.hadoop.ColumnChunkPageWriteStore
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.MessageTypeParser
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path

class GuardedValuesWriterFactoryTest {
    @TempDir
    lateinit var directory: Path

    private class Limit : RuntimeException()

    private class Budget {
        var limit = Long.MAX_VALUE
        var live = 0L
        var peak = 0L
        var calls = 0

        fun reserve(bytes: Long) {
            calls++
            if (bytes > limit) throw Limit()
            live = bytes
            peak = maxOf(peak, live)
        }
    }

    private val intPath = path(PrimitiveTypeName.INT32)

    private fun path(type: PrimitiveTypeName): ColumnDescriptor {
        val name =
            when (type) {
                PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY -> "fixed_len_byte_array(12)"
                else -> type.name.lowercase()
            }
        return MessageTypeParser.parseMessageType("message m { required $name value; }").columns.single()
    }

    private fun properties(
        factory: ValuesWriterFactory? = null,
        version: ParquetProperties.WriterVersion = ParquetProperties.WriterVersion.PARQUET_1_0,
        dictionaryBytes: Int = 64 shl 10,
    ): ParquetProperties {
        val builder =
            ParquetProperties.builder()
                .withWriterVersion(version)
                .withDictionaryPageSize(dictionaryBytes)
                .withPageSize(64 shl 10)
        if (factory != null) builder.withValuesWriterFactory(factory)
        return builder.build()
    }

    private fun fallback(writer: ValuesWriter): FallbackValuesWriter<*, *> =
        (writer as GuardedValuesWriterFactory.Tracked).delegate as FallbackValuesWriter<*, *>

    private fun initial(writer: ValuesWriter): DictionaryValuesWriter =
        fallback(
            writer,
        ).initialWriter as DictionaryValuesWriter

    @Test
    fun `duplicates at a growth boundary need no speculative reservation`() {
        val budget = Budget()
        val factory = GuardedValuesWriterFactory(budget::reserve)
        val props = properties(factory)
        val writer = props.newValuesWriter(intPath)
        try {
            repeat(24) { writer.writeInteger(it) }
            val live = budget.live
            budget.limit = live
            repeat(100) { writer.writeInteger(it % 24) }
            assertThat(budget.live).isEqualTo(live)
            val calls = budget.calls
            assertThat(factory.forecastGrowthBytes(intPath, 1)).isPositive()
            assertThat(factory.nextGrowthBytes).isPositive()
            assertThat(factory.forecastGrowthBytes(intPath, Long.MAX_VALUE)).isPositive()
            assertThat(budget.calls).isEqualTo(calls)
            val forecast = factory.forecastGrowthBytes(intPath, 1)
            assertThatThrownBy { writer.writeInteger(24) }.isInstanceOf(Limit::class.java)
            assertThat(budget.live).isEqualTo(live)
            assertThat(factory.forecastGrowthBytes(intPath, 1)).isGreaterThanOrEqualTo(forecast)
        } finally {
            writer.close()
        }
        assertThat(budget.live).isZero()
        assertThat(factory.unmanagedBytes).isZero()
    }

    @Test
    fun `page reset and dictionary clear retain capacity until terminal close`() {
        val budget = Budget()
        val factory = GuardedValuesWriterFactory(budget::reserve)
        val writer = properties(factory, dictionaryBytes = 1 shl 20).newValuesWriter(intPath)
        try {
            repeat(20000) { writer.writeInteger(it) }
            val dictionary = initial(writer)
            dictionary.bytes.toByteArray()
            dictionary.reset()
            val capacity = budget.live
            assertThat(capacity).isGreaterThan(500000)
            repeat(1000) { dictionary.writeInteger(it % 20000) }
            dictionary.bytes.toByteArray()
            dictionary.reset()
            assertThat(budget.live).isEqualTo(capacity)
            dictionary.resetDictionary()
            assertThat(budget.live).isEqualTo(capacity)
            dictionary.writeInteger(7)
            assertThat(budget.live - capacity).isLessThan(20000)
        } finally {
            writer.close()
        }
        assertThat(budget.live).isZero()
    }

    @Test
    fun `never used fallback replaces its ID list with another guarded list`() {
        val budget = Budget()
        val factory = GuardedValuesWriterFactory(budget::reserve)
        val writer = properties(factory, dictionaryBytes = 64).newValuesWriter(intPath)
        try {
            repeat(17) { writer.writeInteger(it) }
            val dictionary = initial(writer)
            val capacity = budget.live
            assertThat(capacity).isBetween(500, 2000)
            budget.limit = capacity
            assertThatThrownBy { dictionary.writeInteger(0) }.isInstanceOf(Limit::class.java)
            assertThat(budget.live).isEqualTo(capacity)
        } finally {
            writer.close()
        }
        assertThat(budget.live).isZero()
    }

    @Test
    fun `reverse dictionary refusal happens before fallback allocation and cleans up`() {
        val budget = Budget()
        val factory = GuardedValuesWriterFactory(budget::reserve)
        val writer = properties(factory, dictionaryBytes = 1 shl 20).newValuesWriter(intPath)
        try {
            repeat(20000) { writer.writeInteger(it) }
            val before = budget.live
            budget.limit = before + 1024
            val target = fallback(writer).fallBackWriter
            assertThatThrownBy { initial(writer).fallBackAllValuesTo(target) }.isInstanceOf(Limit::class.java)
            assertThat(budget.live).isEqualTo(before)
        } finally {
            writer.close()
        }
        assertThat(budget.live).isZero()
    }

    @Test
    fun `factory closes a partial column store after second column constructor refusal`() {
        val budget = Budget()
        val guarded = GuardedValuesWriterFactory(budget::reserve)
        val schema = MessageTypeParser.parseMessageType("message m { required int32 first; required int64 second; }")
        val factory =
            object : ValuesWriterFactory {
                override fun initialize(properties: ParquetProperties) = guarded.initialize(properties)

                override fun newValuesWriter(path: ColumnDescriptor): ValuesWriter {
                    if (path.path.single() == "second") budget.limit = budget.live
                    return guarded.newValuesWriter(path)
                }
            }
        val props = properties(factory, ParquetProperties.WriterVersion.PARQUET_2_0)
        val codecs = CodecFactory(Configuration(false), props.pageSizeThreshold)
        val pages =
            ColumnChunkPageWriteStore.builder()
                .withCompressorProvider { codecs.getCompressor(CompressionCodecName.SNAPPY) }
                .withSchema(schema)
                .withAllocator(props.allocator)
                .withColumnIndexTruncateLength(props.columnIndexTruncateLength)
                .build()
        try {
            assertThatThrownBy { props.newColumnWriteStore(schema, pages, pages) }.isInstanceOf(Limit::class.java)
            assertThat(budget.live).isPositive()
        } finally {
            guarded.close()
            pages.close()
            codecs.release()
        }
        guarded.close()
        assertThat(budget.live).isZero()
        assertThat(guarded.unmanagedBytes).isZero()
        assertThat(guarded.nextGrowthBytes).isZero()
    }

    @Test
    fun `binary copies reserve their payload before retaining caller bytes`() {
        val budget = Budget()
        val factory = GuardedValuesWriterFactory(budget::reserve)
        val writer = properties(factory).newValuesWriter(path(PrimitiveTypeName.BINARY))
        try {
            writer.writeBytes(Binary.fromConstantByteArray(byteArrayOf(1)))
            val before = budget.live
            budget.limit = before
            assertThatThrownBy { writer.writeBytes(Binary.fromReusedByteArray(ByteArray(1 shl 20))) }
                .isInstanceOf(Limit::class.java)
            assertThat(budget.live).isEqualTo(before)
        } finally {
            writer.close()
        }
        assertThat(budget.live).isZero()
    }

    @Test
    fun `large backed and inaccessible heap slices retain only exact visible bytes`() {
        for (readOnly in listOf(false, true)) {
            val backing = ByteArray(4 shl 20) { 99 }
            val visible = byteArrayOf(11, 22, 33)
            visible.copyInto(backing, 123)
            val value =
                if (readOnly) {
                    val buffer = ByteBuffer.wrap(backing).position(123).limit(126).slice().asReadOnlyBuffer()
                    Binary.fromConstantByteBuffer(buffer)
                } else {
                    Binary.fromConstantByteArray(backing, 123, visible.size)
                }
            val budget = Budget()
            val factory = GuardedValuesWriterFactory(budget::reserve)
            val guarded = properties(factory).newValuesWriter(path(PrimitiveTypeName.BINARY))
            val reference = properties().newValuesWriter(path(PrimitiveTypeName.BINARY))
            try {
                repeat(1000) {
                    guarded.writeBytes(if (it == 0) value else Binary.fromConstantByteArray(visible))
                    reference.writeBytes(Binary.fromConstantByteArray(visible))
                }
                // Mutation proves detachment without a timing-dependent garbage-collection assertion.
                backing.fill(77)
                assertThat(guarded.bytes.toByteArray()).isEqualTo(reference.bytes.toByteArray())
                assertThat(guarded.encoding).isEqualTo(reference.encoding)
                val actual = checkNotNull(guarded.toDictPageAndClose())
                val expected = checkNotNull(reference.toDictPageAndClose())
                assertThat(actual.dictionarySize).isEqualTo(1)
                assertThat(actual.bytes.toByteArray()).isEqualTo(expected.bytes.toByteArray())
                assertThat(budget.live).isLessThan(64L shl 10)
            } finally {
                reference.close()
                factory.close()
            }
            assertThat(budget.live).isZero()
        }
    }

    @Test
    fun `ordinary detached constant binaries keep stock alias behavior`() {
        val bytes = byteArrayOf(11, 22, 33)
        val value = Binary.fromConstantByteArray(bytes)
        val budget = Budget()
        val factory = GuardedValuesWriterFactory(budget::reserve)
        val writer = properties(factory).newValuesWriter(path(PrimitiveTypeName.BINARY))
        try {
            repeat(1000) { writer.writeBytes(value) }
            bytes[0] = 44
            writer.bytes.toByteArray()
            writer.encoding
            val dictionary = checkNotNull(writer.toDictPageAndClose())
            assertThat(dictionary.bytes.toByteArray()).isEqualTo(byteArrayOf(3, 0, 0, 0, 44, 22, 33))
        } finally {
            factory.close()
        }
        assertThat(budget.live).isZero()
    }

    @Test
    fun `ID slab growth refuses before allocating the next slab`() {
        val budget = Budget()
        val factory = GuardedValuesWriterFactory(budget::reserve)
        val writer = properties(factory).newValuesWriter(intPath)
        try {
            repeat(4096) { writer.writeInteger(7) }
            budget.limit = budget.live
            val before = initial(writer).bufferedSize
            assertThatThrownBy { writer.writeInteger(7) }.isInstanceOf(Limit::class.java)
            assertThat(initial(writer).bufferedSize).isEqualTo(before)
        } finally {
            factory.close()
        }
        assertThat(budget.live).isZero()
    }

    @Test
    fun `large ID lists preserve bytes beyond the slab size cap`() {
        val stock = properties().newValuesWriter(intPath)
        val budget = Budget()
        val factory = GuardedValuesWriterFactory(budget::reserve)
        val guarded = properties(factory).newValuesWriter(intPath)
        try {
            repeat(800000) {
                stock.writeInteger(it % 13)
                guarded.writeInteger(it % 13)
            }
            assertThat(guarded.bytes.toByteArray()).isEqualTo(stock.bytes.toByteArray())
            assertThat(guarded.encoding).isEqualTo(stock.encoding)
            assertThat(budget.live).isGreaterThan(800000L * 4)
        } finally {
            stock.close()
            factory.close()
        }
        assertThat(budget.live).isZero()
    }

    private data class Encoded(
        val pages: List<Pair<Encoding, List<Byte>>>,
        val dictionary: Pair<Encoding, List<Byte>>?,
    )

    private fun writeValue(
        writer: ValuesWriter,
        type: PrimitiveTypeName,
        value: Int,
    ) {
        when (type) {
            PrimitiveTypeName.INT32 -> writer.writeInteger(value * 1664525)
            PrimitiveTypeName.INT64 -> writer.writeLong(value.toLong() * 6364136223846793005)
            PrimitiveTypeName.FLOAT -> {
                val bits =
                    when (value % 7) {
                        0 -> 0x7fc00000 + value
                        1 -> Int.MIN_VALUE
                        else -> value * 1664525
                    }
                writer.writeFloat(java.lang.Float.intBitsToFloat(bits))
            }
            PrimitiveTypeName.DOUBLE -> {
                val bits =
                    when (value % 7) {
                        0 -> 0x7ff8000000000000L + value
                        1 -> Long.MIN_VALUE
                        else -> value.toLong() * 6364136223846793005
                    }
                writer.writeDouble(java.lang.Double.longBitsToDouble(bits))
            }
            PrimitiveTypeName.BOOLEAN -> writer.writeBoolean(value % 3 == 0)
            else -> {
                val bytes = ByteBuffer.allocate(12).putInt(value).putLong(value.toLong() * 6364136223846793005).array()
                val binary =
                    when (value % 3) {
                        0 -> Binary.fromConstantByteArray(bytes)
                        1 -> Binary.fromReusedByteArray(bytes)
                        else -> Binary.fromConstantByteBuffer(ByteBuffer.allocateDirect(12).put(bytes).flip())
                    }
                writer.writeBytes(binary)
                if (value % 3 == 1) bytes.fill(99)
            }
        }
    }

    private fun encode(
        type: PrimitiveTypeName,
        version: ParquetProperties.WriterVersion,
        dictionaryBytes: Int,
        factory: GuardedValuesWriterFactory?,
    ): Encoded {
        val writer = properties(factory, version, dictionaryBytes).newValuesWriter(path(type))
        try {
            val pages = ArrayList<Pair<Encoding, List<Byte>>>()
            // The first page keeps a useful dictionary. Later pages force the fallback path.
            for (page in 0..3) {
                repeat(18000) { row ->
                    val value = if (page == 0) row % 13 else (page - 1) * 18000 + row
                    writeValue(writer, type, value)
                }
                val bytes = writer.bytes.toByteArray().toList()
                pages += writer.encoding to bytes
                writer.reset()
            }
            val dictionary = writer.toDictPageAndClose()?.let { it.encoding to it.bytes.toByteArray().toList() }
            writer.resetDictionary()
            return Encoded(pages, dictionary)
        } finally {
            writer.close()
        }
    }

    @Test
    fun `all scalar types preserve exact V1 and V2 pages through fallback`() {
        for (version in ParquetProperties.WriterVersion.values()) {
            for (type in PrimitiveTypeName.values()) {
                for (dictionaryBytes in listOf(64, 64 shl 10)) {
                    val budget = Budget()
                    val factory = GuardedValuesWriterFactory(budget::reserve)
                    val stock = encode(type, version, dictionaryBytes, null)
                    val guarded = encode(type, version, dictionaryBytes, factory)
                    assertThat(guarded).describedAs("$version $type dictionary=$dictionaryBytes").isEqualTo(stock)
                    assertThat(budget.live).isZero()
                    assertThat(factory.unmanagedBytes).isZero()
                }
            }
        }
    }

    @Test
    fun `complete nullable multi-page files remain identical in both versions`() {
        val schema =
            MessageTypeParser.parseMessageType(
                """
                message m {
                  optional boolean b;
                  optional int32 i;
                  optional int64 l;
                  optional float f;
                  optional double d;
                  optional binary s;
                  optional fixed_len_byte_array(12) x;
                  optional int96 t;
                }
                """.trimIndent(),
            )
        for (version in ParquetProperties.WriterVersion.values()) {
            val paths = ArrayList<Path>()
            for (guarded in listOf(false, true)) {
                val path = directory.resolve("$version-$guarded.parquet")
                paths.add(path)
                val props = properties(version = version)
                val codecs = CodecFactory(Configuration(false), props.pageSizeThreshold)
                val compressor = codecs.getCompressor(CompressionCodecName.SNAPPY)
                val file =
                    ParquetFileWriter(
                        LocalOutputFile(path),
                        schema,
                        ParquetFileWriter.Mode.OVERWRITE,
                        16L shl 20,
                        0,
                        null,
                        props,
                    )
                try {
                    file.start()
                    repeat(2) { group ->
                        val budget = Budget()
                        val factory = if (guarded) GuardedValuesWriterFactory(budget::reserve) else null
                        val groupProps = properties(factory, version)
                        val pages =
                            ColumnChunkPageWriteStore.builder()
                                .withCompressorProvider { compressor }
                                .withSchema(schema)
                                .withAllocator(groupProps.allocator)
                                .withColumnIndexTruncateLength(groupProps.columnIndexTruncateLength)
                                .build()
                        val columns = groupProps.newColumnWriteStore(schema, pages, pages)
                        try {
                            val consumer = ColumnIOFactory(false).getColumnIO(schema).getRecordWriter(columns)
                            repeat(6000) { row ->
                                consumer.startMessage()
                                for (field in 0..7) {
                                    if (row % 23 == field) continue
                                    val value = if (row < 1500) row % 13 else row + group * 6000
                                    consumer.startField(schema.getFieldName(field), field)
                                    when (field) {
                                        0 -> consumer.addBoolean(value % 3 == 0)
                                        1 -> consumer.addInteger(value * 1664525)
                                        2 -> consumer.addLong(value.toLong() * 6364136223846793005)
                                        3 -> consumer.addFloat(java.lang.Float.intBitsToFloat(value * 1664525))
                                        4 ->
                                            consumer.addDouble(
                                                java.lang.Double.longBitsToDouble(value.toLong() * 6364136223846793005),
                                            )
                                        else -> {
                                            val bytes =
                                                ByteBuffer.allocate(
                                                    12,
                                                ).putInt(value).putLong(value.toLong() * 6364136223846793005).array()
                                            consumer.addBinary(Binary.fromConstantByteArray(bytes))
                                        }
                                    }
                                    consumer.endField(schema.getFieldName(field), field)
                                }
                                consumer.endMessage()
                            }
                            consumer.flush()
                            file.startBlock(6000)
                            columns.flush()
                            pages.flushToFileWriter(file)
                            file.endBlock()
                        } finally {
                            columns.close()
                            factory?.close()
                            pages.close()
                        }
                        assertThat(budget.live).isZero()
                    }
                    file.end(emptyMap())
                } finally {
                    file.close()
                    codecs.release()
                }
            }
            assertThat(Files.mismatch(paths[0], paths[1])).describedAs(version.name).isEqualTo(-1L)
        }
    }
}
