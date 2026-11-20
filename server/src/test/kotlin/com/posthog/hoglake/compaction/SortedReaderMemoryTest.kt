@file:Suppress("DEPRECATION")

package com.posthog.hoglake.compaction

import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.bytes.HeapByteBufferAllocator
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.Encoding
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.column.page.DataPage
import org.apache.parquet.column.page.DataPageV1
import org.apache.parquet.column.page.DataPageV2
import org.apache.parquet.column.page.DictionaryPage
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.column.page.PageReader
import org.apache.parquet.column.statistics.Statistics
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter
import org.apache.parquet.compression.CompressionCodecFactory
import org.apache.parquet.conf.HadoopParquetConfiguration
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.hadoop.ParquetEmptyBlockException
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.hadoop.util.HadoopCodecs
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.DelegatingSeekableInputStream
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.MessageTypeParser
import org.apache.parquet.schema.Type
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.nio.file.Path
import java.util.Random

class SortedReaderMemoryTest {
    @TempDir
    lateinit var directory: Path

    private val allowance = 256L * 1024 * 1024

    @Test
    fun `a large single row group uses current pages rather than all values`() {
        val schema =
            MessageTypeParser.parseMessageType(
                "message events { ${(0 until 10).joinToString(" ") { "optional int64 c$it;" }} }",
            )
        val input =
            write("large.parquet", schema, 400_000, ParquetProperties.WriterVersion.PARQUET_1_0) { row, index ->
                repeat(10) { column -> if ((index + column) % 17 != 0) row.add(column, index.toLong() * 7919 + column) }
            }
        val footer = ParquetFileReader.open(input).use { it.footer }
        assertThat(footer.blocks).hasSize(1)
        val profile = SortedReaderMemory.profile(input, footer, schema, allowance)
        assertThat(profile.chargeBytes).isLessThan(128L * 1024 * 1024)
        profile.open(input, footer, input.newStream()).use { reader ->
            reader.readRowGroup(0).use { pages ->
                val records = ColumnIOFactory().getColumnIO(schema).getRecordReader(pages, GroupRecordConverter(schema))
                repeat(400_000) { index ->
                    val row = records.read()
                    if (index % 17 != 0) assertThat(row.getLong(0, 0)).isEqualTo(index.toLong() * 7919)
                }
            }
        }
    }

    @Test
    fun `a sparse wide row group does not charge absent values as objects`() {
        val schema =
            MessageTypeParser.parseMessageType(
                "message sparse { ${(0 until 151).joinToString(" ") { "optional binary c$it;" }} }",
            )
        val input =
            write("sparse.parquet", schema, 200_000, ParquetProperties.WriterVersion.PARQUET_1_0) { row, index ->
                if (index % 1999 == 0) row.add(index % 151, Binary.fromString("value-$index"))
            }
        val footer = ParquetFileReader.open(input).use { it.footer }
        assertThat(footer.blocks).hasSize(1)
        val profile = SortedReaderMemory.profile(input, footer, schema, allowance)
        assertThat(profile.chargeBytes).isLessThan(64L * 1024 * 1024)
        profile.open(input, footer, input.newStream()).use { reader ->
            reader.readRowGroup(0).use { pages ->
                val records = ColumnIOFactory().getColumnIO(schema).getRecordReader(pages, GroupRecordConverter(schema))
                repeat(200_000) { records.read() }
            }
        }
    }

    @Test
    fun `both page versions decode nullable delta and dictionary values`() {
        val schema = MessageTypeParser.parseMessageType("message values { optional int64 id; optional binary value; }")
        for (version in ParquetProperties.WriterVersion.entries) {
            val input =
                write("${version.name}.parquet", schema, 10_000, version) { row, index ->
                    if (index % 3 != 0) row.add(0, index.toLong())
                    if (index % 5 != 0) row.add(1, Binary.fromString("value-${index % 31}"))
                }
            val footer = ParquetFileReader.open(input).use { it.footer }
            val profile = SortedReaderMemory.profile(input, footer, schema, allowance)
            profile.open(input, footer, input.newStream()).use { reader ->
                reader.readRowGroup(0).use { pages ->
                    val records =
                        ColumnIOFactory().getColumnIO(
                            schema,
                        ).getRecordReader(pages, GroupRecordConverter(schema))
                    repeat(10_000) { index ->
                        val row = records.read()
                        if (index % 3 != 0) assertThat(row.getLong(0, 0)).isEqualTo(index.toLong())
                    }
                }
            }
        }
    }

    @Test
    fun `the codec guard rejects before allocating an oversized page`() {
        val schema = MessageTypeParser.parseMessageType("message values { required int64 id; }")
        val input =
            write(
                "small.parquet",
                schema,
                10,
                ParquetProperties.WriterVersion.PARQUET_1_0,
            ) { row, index -> row.add(0, index.toLong()) }
        val footer = ParquetFileReader.open(input).use { it.footer }
        val profile = SortedReaderMemory.profile(input, footer, schema, allowance)
        val codecs = profile.options().codecFactory
        try {
            assertThatThrownBy {
                codecs.getDecompressor(CompressionCodecName.ZSTD).decompress(BytesInput.empty(), Int.MAX_VALUE)
            }.isInstanceOf(SortedReaderMemoryException::class.java)
        } finally {
            codecs.release()
        }
    }

    @Test
    fun `packed dictionary runs accept seven final padding values`() {
        val schema = MessageTypeParser.parseMessageType("message values { required int64 id; }")
        val page = page(schema, byteArrayOf(0, 3), 1, Encoding.RLE_DICTIONARY)
        guarded(schema, page, dictionaryEntries = 1).readPage()
    }

    @Test
    fun `packed dictionary runs reject counts before allocating arrays`() {
        val schema = MessageTypeParser.parseMessageType("message values { required int64 id; }")
        val bytes =
            ByteArrayOutputStream().apply {
                write(0)
                unsigned(this, (100_000_000L shl 1) or 1)
            }.toByteArray()
        val page = page(schema, bytes, 1, Encoding.RLE_DICTIONARY)
        assertThatThrownBy { guarded(schema, page, dictionaryEntries = 1).readPage() }
            .isInstanceOf(IOException::class.java)
            .hasMessageContaining("padding")
    }

    @Test
    fun `DELTA allocation counts cannot exceed the page count`() {
        val schema = MessageTypeParser.parseMessageType("message values { required int64 id; }")
        val bytes =
            ByteArrayOutputStream().apply {
                unsigned(this, 128)
                unsigned(this, 4)
                unsigned(this, 500_000_000)
                write(0)
            }.toByteArray()
        assertThatThrownBy { guarded(schema, page(schema, bytes, 1, Encoding.DELTA_BINARY_PACKED)).readPage() }
            .isInstanceOf(IOException::class.java)
            .hasMessageContaining("allocation counts")
    }

    @Test
    fun `DELTA miniblock padding spends the decoder allowance before allocation`() {
        val schema = MessageTypeParser.parseMessageType("message values { required int64 id; }")
        val bytes =
            ByteArrayOutputStream().apply {
                unsigned(this, 1_000_000_000)
                unsigned(this, 1)
                unsigned(this, 1)
                write(0)
            }.toByteArray()
        assertThatThrownBy { guarded(schema, page(schema, bytes, 1, Encoding.DELTA_BINARY_PACKED)).readPage() }
            .isInstanceOf(SortedReaderMemoryException::class.java)
    }

    @Test
    fun `V2 definition levels include packed padding and null values`() {
        val schema = MessageTypeParser.parseMessageType("message values { optional int64 id; }")
        val page =
            DataPageV2.uncompressed(
                9,
                4,
                9,
                BytesInput.empty(),
                BytesInput.from(byteArrayOf(5, 0x55, 1)),
                Encoding.PLAIN,
                BytesInput.from(ByteArray(5 * 8)),
                Statistics.createStats(schema.getType(0)),
            )
        guarded(schema, page).readPage()
    }

    @Test
    fun `repeated V1 levels use their own bounded sections`() {
        val schema = MessageTypeParser.parseMessageType("message values { repeated int64 id; }")
        val bytes =
            ByteArrayOutputStream().apply {
                little(this, 4)
                write(byteArrayOf(2, 0, 16, 1))
                little(this, 2)
                write(byteArrayOf(18, 1))
                write(ByteArray(9 * 8))
            }.toByteArray()
        guarded(schema, page(schema, bytes, 9, Encoding.PLAIN, Encoding.RLE)).readPage()
    }

    @Test
    fun `delta binary encodings accept real writer output`() {
        val schema = MessageTypeParser.parseMessageType("message values { required binary value; }")
        val values = listOf("prefix-one", "prefix-two", "prefix-three", "other")
        DeltaByteArrayWriter(64, 4096, HeapByteBufferAllocator()).use { writer ->
            values.forEach { writer.writeBytes(Binary.fromString(it)) }
            guarded(schema, page(schema, writer.bytes.toByteArray(), values.size, Encoding.DELTA_BYTE_ARRAY)).readPage()
        }
        DeltaLengthByteArrayValuesWriter(64, 4096, HeapByteBufferAllocator()).use { writer ->
            values.forEach { writer.writeBytes(Binary.fromString(it)) }
            guarded(
                schema,
                page(schema, writer.bytes.toByteArray(), values.size, Encoding.DELTA_LENGTH_BYTE_ARRAY),
            ).readPage()
        }
    }

    @Test
    fun `a dictionary count is checked before dictionary decoding`() {
        val schema = MessageTypeParser.parseMessageType("message values { required int64 id; }")
        val data = page(schema, ByteArray(8), 1, Encoding.PLAIN)
        val reader =
            guarded(
                schema,
                data,
                dictionaryEntries = 1,
                dictionary = DictionaryPage(BytesInput.empty(), Int.MAX_VALUE, Encoding.PLAIN),
            )
        assertThatThrownBy { reader.readDictionaryPage() }.isInstanceOf(IOException::class.java)
    }

    @Test
    fun `a two GiB decoded row group uses only its current page`() {
        val schema = MessageTypeParser.parseMessageType("message values { required binary value; }")
        val path = directory.resolve("large-decoded.parquet")
        val factory = SimpleGroupFactory(schema)
        val payload = Binary.fromConstantByteArray(ByteArray(10_500))
        ExampleParquetWriter.builder(LocalOutputFile(path))
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.ZSTD)
            .withDictionaryEncoding(false)
            .withRowGroupSize(3L * 1024 * 1024 * 1024)
            .withPageSize(1024 * 1024)
            .build().use { writer ->
                repeat(205_000) { writer.write(factory.newGroup().also { it.add(0, payload) }) }
            }
        val input = LocalInputFile(path)
        val footer = ParquetFileReader.open(input).use { it.footer }
        assertThat(footer.blocks).hasSize(1)
        assertThat(
            footer.blocks.single().columns.sumOf { it.totalUncompressedSize },
        ).isGreaterThan(2L * 1024 * 1024 * 1024)
        val profile = SortedReaderMemory.profile(input, footer, schema, allowance)
        assertThat(profile.chargeBytes).isLessThan(64L * 1024 * 1024)
        profile.open(input, footer, input.newStream()).use { reader ->
            reader.readRowGroup(0).use { pages ->
                val records = ColumnIOFactory().getColumnIO(schema).getRecordReader(pages, GroupRecordConverter(schema))
                repeat(10) { assertThat(records.read().getBinary(0, 0).length()).isEqualTo(10_500) }
            }
        }
    }

    @Test
    fun `local input has no source buffer floor`() {
        val schema = MessageTypeParser.parseMessageType("message values { required int64 id; }")
        val input =
            write("local.parquet", schema, 10, ParquetProperties.WriterVersion.PARQUET_1_0) { row, index ->
                row.add(0, index.toLong())
            }
        val footer = ParquetFileReader.open(input).use { it.footer }
        val local = SortedReaderMemory.profile(input, footer, schema, allowance, sourceReadaheadBytes = 0)
        val buffered = SortedReaderMemory.profile(input, footer, schema, allowance, sourceReadaheadBytes = 256 * 1024)
        assertThat(buffered.chargeBytes - local.chargeBytes).isEqualTo(2 * input.length)
        assertThat(local.chargeBytes).isLessThan(1024 * 1024)
    }

    @Test
    fun `large source buffers respect both allocation size and file length`() {
        val schema = MessageTypeParser.parseMessageType("message values { required binary value; }")
        val random = Random(7)
        val input =
            write("large-source.parquet", schema, 12_000, ParquetProperties.WriterVersion.PARQUET_1_0) { row, _ ->
                val bytes = ByteArray(1024)
                random.nextBytes(bytes)
                row.add(0, Binary.fromConstantByteArray(bytes))
            }
        assertThat(input.length).isGreaterThan(SortedReaderMemory.MAX_ALLOCATION_BYTES)
        val footer = ParquetFileReader.open(input).use { it.footer }
        val local = SortedReaderMemory.profile(input, footer, schema, allowance)
        val ordinary = SortedReaderMemory.profile(input, footer, schema, allowance, sourceReadaheadBytes = 256 * 1024)
        val larger =
            SortedReaderMemory.profile(
                input,
                footer,
                schema,
                allowance,
                sourceReadaheadBytes = 20L * 1024 * 1024,
            )
        assertThat(ordinary.chargeBytes - local.chargeBytes).isEqualTo(2 * SortedReaderMemory.MAX_ALLOCATION_BYTES)
        assertThat(larger.chargeBytes - local.chargeBytes).isEqualTo(2 * input.length)
    }

    @Test
    fun `the shared page row limit matches the pinned reader version`() {
        assertThat(SortedReaderMemory.PAGE_ROW_COUNT_LIMIT).isEqualTo(ParquetProperties.DEFAULT_PAGE_ROW_COUNT_LIMIT)
        val schema = MessageTypeParser.parseMessageType("message values { required binary value; }")
        assertThat(SortedReaderMemory.dictionaryCharge(schema.columns.single(), 100, 2))
            .isEqualTo(200 + 2 * SortedReaderMemory.DICTIONARY_ENTRY_BYTES)
    }

    @Test
    fun `the final binary cap reuses the header scan`() {
        val schema = MessageTypeParser.parseMessageType("message values { required binary value; }")
        val input =
            write("binary.parquet", schema, 1, ParquetProperties.WriterVersion.PARQUET_1_0) { row, _ ->
                row.add(0, Binary.fromString("a binary payload"))
            }
        val footer = ParquetFileReader.open(input).use { it.footer }
        var scans = 0
        val original =
            SortedReaderMemory.profile(input, footer, schema, allowance, streamFactory = {
                scans++
                input.newStream()
            })
        val profile = original.withMaxBinaryBytes(4)
        assertThat(scans).isEqualTo(1)
        assertThat(profile.chargeBytes).isEqualTo(original.chargeBytes)
        assertThat(profile.retainedMetadataBytes).isEqualTo(original.retainedMetadataBytes).isPositive()
        profile.open(input, footer, input.newStream()).use { reader ->
            reader.readRowGroup(0).use { pages ->
                val error =
                    catchThrowable {
                        ColumnIOFactory().getColumnIO(
                            schema,
                        ).getRecordReader(pages, GroupRecordConverter(schema)).read()
                    }
                assertThat(error).isInstanceOf(SortedReaderMemoryException::class.java)
                assertThat((error as SortedReaderMemoryException).binaryValueRefusal).isTrue()
            }
        }
    }

    @Test
    fun `V1 V2 and dictionary headers reject huge pages before decompression`() {
        val schema = MessageTypeParser.parseMessageType("message values { required int64 id; }")
        for (kind in listOf("v1", "v2", "dictionary")) {
            val path = directory.resolve("huge-$kind.parquet")
            ParquetFileWriter(LocalOutputFile(path), schema, ParquetFileWriter.Mode.CREATE, 1024L * 1024, 0).use {
                    writer ->
                writer.start()
                writer.startBlock(1)
                writer.startColumn(schema.columns.single(), 1, CompressionCodecName.ZSTD)
                if (kind == "dictionary") {
                    writer.writeDictionaryPage(
                        DictionaryPage(BytesInput.from(byteArrayOf(0)), Int.MAX_VALUE, 1, Encoding.PLAIN),
                    )
                }
                if (kind == "v2") {
                    writer.writeDataPageV2(
                        1, 0, 1, BytesInput.empty(), BytesInput.empty(), Encoding.PLAIN,
                        BytesInput.from(byteArrayOf(0)), Int.MAX_VALUE, Statistics.createStats(schema.getType(0)),
                    )
                } else {
                    writer.writeDataPage(
                        1,
                        if (kind == "dictionary") 8 else Int.MAX_VALUE,
                        BytesInput.from(byteArrayOf(0)),
                        Encoding.BIT_PACKED,
                        Encoding.BIT_PACKED,
                        Encoding.PLAIN,
                    )
                }
                writer.endColumn()
                writer.endBlock()
                writer.end(emptyMap())
            }
            val input = LocalInputFile(path)
            val footer = ParquetFileReader.open(input).use { it.footer }
            assertThatThrownBy { SortedReaderMemory.profile(input, footer, schema, allowance) }
                .isInstanceOf(SortedReaderMemoryException::class.java)
        }
    }

    @Test
    fun `indexed readers close the old store before allocating another`() {
        val schema = MessageTypeParser.parseMessageType("message values { required int64 id; }")
        val input =
            write("lifecycle.parquet", schema, 1, ParquetProperties.WriterVersion.PARQUET_1_0) { row, _ ->
                row.add(0, 1L)
            }
        val footer = ParquetFileReader.open(input).use { it.footer }
        val group = SortedReaderMemory.GroupProfile(emptyMap(), 0)
        val profile = SortedReaderMemory.Profile(listOf(group, group), 0, 0, allowance, allowance, schema)
        var closed = 0
        val options = profile.options()
        assertThat(options.allocator.isDirect).isFalse()
        val delegate =
            object : ParquetFileReader(input, footer, options, input.newStream()) {
                override fun readRowGroup(index: Int): PageReadStore {
                    assertThat(closed).isEqualTo(index)
                    return object : PageReadStore {
                        override fun getRowCount(): Long = 1

                        override fun getPageReader(column: ColumnDescriptor): PageReader = error("unused column")

                        override fun close() {
                            closed++
                        }
                    }
                }
            }
        val reader = SortedReaderMemory.Reader(delegate, profile)
        reader.readRowGroup(0)
        reader.readRowGroup(1)
        assertThat(closed).isEqualTo(1)
        reader.close()
        reader.close()
        assertThat(closed).isEqualTo(2)
        assertThatThrownBy { reader.readRowGroup(0) }.isInstanceOf(IllegalStateException::class.java)
    }

    @Test
    fun `construction and projection failures release owned resources once`() {
        val schema = MessageTypeParser.parseMessageType("message values { required int64 id; }")
        val input =
            write("open-ownership.parquet", schema, 1, ParquetProperties.WriterVersion.PARQUET_1_0) { row, _ ->
                row.add(0, 1L)
            }
        val footer = ParquetFileReader.open(input).use { it.footer }
        for (stage in listOf("constructor", "filter", "projection", "success")) {
            val fields = mutableListOf<Type?>(schema.getType(0))
            val projection = MessageType("projection", fields)
            val profile = SortedReaderMemory.profile(input, footer, projection, allowance)
            if (stage == "projection") fields[0] = null
            val suppliedFooter =
                when (stage) {
                    "constructor" -> ParquetMetadata(null, footer.blocks)
                    "filter" ->
                        ParquetMetadata(
                            footer.fileMetaData,
                            object : AbstractList<BlockMetaData>() {
                                override val size: Int = 1

                                override fun get(index: Int): BlockMetaData = throw IOException("filter failure")
                            },
                        )
                    else -> footer
                }
            var closes = 0
            val stream = input.newStream()
            val trackedStream =
                object : DelegatingSeekableInputStream(stream) {
                    override fun getPos(): Long = stream.pos

                    override fun seek(position: Long) = stream.seek(position)

                    override fun close() {
                        closes++
                        stream.close()
                    }
                }
            var releases = 0
            val configuration = HadoopParquetConfiguration()
            val factory = HadoopCodecs.newFactory(configuration, 0)
            val trackedFactory =
                object : CompressionCodecFactory by factory {
                    override fun release() {
                        releases++
                        factory.release()
                    }
                }
            val open = {
                profile.openOwned(input, suppliedFooter, trackedStream) {
                    ParquetReadOptions.builder(configuration).withCodecFactory(trackedFactory).apply {
                        if (stage == "filter") {
                            withRecordFilter(FilterCompat.get(FilterApi.eq(FilterApi.longColumn("id"), 1L)))
                        }
                    }.build()
                }
            }
            if (stage == "success") {
                val reader = open()
                reader.readRowGroup(0).use { assertThat(it.rowCount).isEqualTo(1) }
                reader.close()
                reader.close()
            } else {
                assertThatThrownBy { open() }.isInstanceOf(Exception::class.java)
            }
            assertThat(closes).describedAs("stream closes after %s", stage).isEqualTo(1)
            assertThat(releases).describedAs("factory releases after %s", stage).isEqualTo(1)
        }
    }

    @Test
    fun `a Snappy preamble cannot bypass the page bound`() {
        val schema = MessageTypeParser.parseMessageType("message values { required int64 id; }")
        val input =
            write("snappy-bound.parquet", schema, 10, ParquetProperties.WriterVersion.PARQUET_1_0) { row, index ->
                row.add(0, index.toLong())
            }
        val footer = ParquetFileReader.open(input).use { it.footer }
        val profile = SortedReaderMemory.profile(input, footer, schema, allowance)
        val bytes = ByteArrayOutputStream().apply { unsigned(this, 1_000_000_000) }.toByteArray()
        val codecs = profile.options().codecFactory
        try {
            assertThatThrownBy {
                codecs.getDecompressor(CompressionCodecName.SNAPPY).decompress(BytesInput.from(bytes), 8)
            }.isInstanceOf(IOException::class.java).hasMessageContaining("Snappy size")
        } finally {
            codecs.release()
        }
    }

    @Test
    fun `Snappy and uncompressed V2 pages retain the allocation guards`() {
        val schema = MessageTypeParser.parseMessageType("message values { optional int64 id; }")
        for (codec in listOf(CompressionCodecName.SNAPPY, CompressionCodecName.UNCOMPRESSED)) {
            val input =
                write(
                    "${codec.name}.parquet",
                    schema,
                    1000,
                    ParquetProperties.WriterVersion.PARQUET_2_0,
                    codec,
                ) { row, index -> if (index % 3 != 0) row.add(0, index.toLong()) }
            val footer = ParquetFileReader.open(input).use { it.footer }
            val profile = SortedReaderMemory.profile(input, footer, schema, allowance)
            profile.open(input, footer, input.newStream()).use { reader ->
                reader.readRowGroup(0).use { pages ->
                    val records =
                        ColumnIOFactory().getColumnIO(
                            schema,
                        ).getRecordReader(pages, GroupRecordConverter(schema))
                    repeat(1000) { index ->
                        val row = records.read()
                        if (index % 3 != 0) assertThat(row.getLong(0, 0)).isEqualTo(index.toLong())
                    }
                }
            }
        }
    }

    @Test
    fun `tiny Snappy pages remain reusable across versions dictionaries and row groups`() {
        for (v2 in listOf(false, true)) {
            for (dictionary in listOf(false, true)) {
                val schema = MessageTypeParser.parseMessageType("message values { required binary value; }")
                val path = directory.resolve("tiny-$v2-$dictionary.parquet")
                val codecs = HadoopCodecs.newFactory(HadoopParquetConfiguration(), 4096)
                try {
                    val compressor = codecs.getCompressor(CompressionCodecName.SNAPPY)
                    ParquetFileWriter(LocalOutputFile(path), schema, ParquetFileWriter.Mode.CREATE, 4096, 0).use {
                            writer ->
                        writer.start()
                        repeat(3) { group ->
                            writer.startBlock(2)
                            writer.startColumn(schema.columns.single(), 2, CompressionCodecName.SNAPPY)
                            val raw =
                                ByteArrayOutputStream().apply {
                                    repeat(2) { index ->
                                        val value = "value${group * 2 + index}".toByteArray()
                                        little(this, value.size)
                                        write(value)
                                    }
                                }.toByteArray()
                            val data: ByteArray
                            val encoding: Encoding
                            if (dictionary) {
                                writer.writeDictionaryPage(
                                    DictionaryPage(
                                        compressor.compress(BytesInput.from(raw)),
                                        raw.size,
                                        2,
                                        Encoding.PLAIN,
                                    ),
                                )
                                data = byteArrayOf(1, 3, 2)
                                encoding = Encoding.RLE_DICTIONARY
                            } else {
                                data = raw
                                encoding = Encoding.PLAIN
                            }
                            val compressed = compressor.compress(BytesInput.from(data))
                            if (v2) {
                                writer.writeDataPageV2(
                                    2, 0, 2, BytesInput.empty(), BytesInput.empty(), encoding, compressed,
                                    data.size, Statistics.createStats(schema.getType(0)),
                                )
                            } else {
                                writer.writeDataPage(
                                    2,
                                    data.size,
                                    compressed,
                                    Encoding.BIT_PACKED,
                                    Encoding.BIT_PACKED,
                                    encoding,
                                )
                            }
                            writer.endColumn()
                            writer.endBlock()
                        }
                        writer.end(emptyMap())
                    }
                } finally {
                    codecs.release()
                }
                val input = LocalInputFile(path)
                val footer = ParquetFileReader.open(input).use { it.footer }
                assertThat(footer.blocks).hasSize(3)
                val profile = SortedReaderMemory.profile(input, footer, schema, allowance)
                profile.open(input, footer, input.newStream()).use { reader ->
                    repeat(3) { group ->
                        reader.readRowGroup(group).use { pages ->
                            val records =
                                ColumnIOFactory().getColumnIO(
                                    schema,
                                ).getRecordReader(pages, GroupRecordConverter(schema))
                            repeat(2) { index ->
                                assertThat(records.read().getBinary(0, 0).toStringUsingUTF8())
                                    .isEqualTo("value${group * 2 + index}")
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    fun `an empty footer block keeps the following real block index`() {
        val schema = MessageTypeParser.parseMessageType("message values { required int64 id; }")
        val input =
            write("empty-before-real.parquet", schema, 1, ParquetProperties.WriterVersion.PARQUET_1_0) { row, _ ->
                row.add(0, 42L)
            }
        val footer = ParquetFileReader.open(input).use { it.footer }
        val empty =
            BlockMetaData().apply {
                rowCount = 0
                // An empty block's column chunks remain unused.
                footer.blocks.single().columns.forEach { addColumn(it) }
            }
        val withEmpty = ParquetMetadata(footer.fileMetaData, listOf(empty) + footer.blocks)
        val baseline = SortedReaderMemory.profile(input, footer, schema, allowance)
        val profile = SortedReaderMemory.profile(input, withEmpty, schema, allowance)
        assertThat(profile.chargeBytes - baseline.chargeBytes).isLessThan(1024)
        profile.open(input, withEmpty, input.newStream()).use { reader ->
            assertThatThrownBy { reader.readRowGroup(0) }.isInstanceOf(ParquetEmptyBlockException::class.java)
            reader.readRowGroup(1).use { pages ->
                val records = ColumnIOFactory().getColumnIO(schema).getRecordReader(pages, GroupRecordConverter(schema))
                assertThat(records.read().getLong(0, 0)).isEqualTo(42)
            }
        }
    }

    @Test
    fun `negative footer row counts remain corruption`() {
        val schema = MessageTypeParser.parseMessageType("message values { required int64 id; }")
        val input =
            write("negative-count.parquet", schema, 1, ParquetProperties.WriterVersion.PARQUET_1_0) { row, _ ->
                row.add(0, 42L)
            }
        val footer = ParquetFileReader.open(input).use { it.footer }
        val negative =
            BlockMetaData().apply {
                rowCount = -1
                footer.blocks.single().columns.forEach { addColumn(it) }
            }
        val corrupt = ParquetMetadata(footer.fileMetaData, listOf(negative))
        assertThatThrownBy { SortedReaderMemory.profile(input, corrupt, schema, allowance) }
            .isInstanceOf(IOException::class.java).hasMessageContaining("negative row-group")
    }

    private fun write(
        name: String,
        schema: MessageType,
        count: Int,
        version: ParquetProperties.WriterVersion,
        codec: CompressionCodecName = CompressionCodecName.ZSTD,
        append: (Group, Int) -> Unit,
    ): LocalInputFile {
        val path = directory.resolve(name)
        val factory = SimpleGroupFactory(schema)
        ExampleParquetWriter.builder(LocalOutputFile(path))
            .withType(schema)
            .withWriterVersion(version)
            .withCompressionCodec(codec)
            .withRowGroupSize(512L * 1024 * 1024)
            .withPageSize(1024 * 1024)
            .build().use { writer ->
                repeat(count) { index -> writer.write(factory.newGroup().also { append(it, index) }) }
            }
        return LocalInputFile(path)
    }

    private fun page(
        schema: MessageType,
        bytes: ByteArray,
        count: Int,
        encoding: Encoding,
        levels: Encoding = Encoding.BIT_PACKED,
    ): DataPageV1 =
        DataPageV1(
            BytesInput.from(bytes),
            count,
            bytes.size,
            Statistics.createStats(schema.getType(0)),
            levels,
            levels,
            encoding,
        )

    private fun guarded(
        schema: MessageType,
        page: DataPage,
        dictionaryEntries: Int = 0,
        dictionary: DictionaryPage? = null,
    ): PageReader {
        val descriptor = schema.columns.single()
        val column =
            SortedReaderMemory.ColumnProfile(descriptor, page.compressedSize.toLong()).apply {
                this.dictionaryEntries = dictionaryEntries
                dictionaryBytes = dictionaryEntries.toLong() * 8
                maxPage = page.uncompressedSize.toLong()
                val encoding =
                    page.accept(
                        object : DataPage.Visitor<Encoding> {
                            override fun visit(value: DataPageV1): Encoding = value.valueEncoding

                            override fun visit(value: DataPageV2): Encoding = value.dataEncoding
                        },
                    )
                observe(page.uncompressedSize.toLong(), page.valueCount, encoding)
            }
        val group = SortedReaderMemory.GroupProfile(mapOf(descriptor.path.toList() to column), 16L * 1024 * 1024)
        val profile = SortedReaderMemory.Profile(listOf(group), group.chargeBytes, 0, allowance, allowance, schema)
        val pages =
            object : PageReadStore {
                override fun getRowCount(): Long = 1

                override fun getPageReader(column: ColumnDescriptor): PageReader =
                    object : PageReader {
                        override fun getTotalValueCount(): Long = page.valueCount.toLong()

                        override fun readDictionaryPage(): DictionaryPage? = dictionary

                        override fun readPage(): DataPage = page
                    }
            }
        return profile.wrap(0, pages).getPageReader(descriptor)
    }

    private fun unsigned(
        output: ByteArrayOutputStream,
        value: Long,
    ) {
        var remaining = value
        while (remaining >= 128) {
            output.write(((remaining and 127) or 128).toInt())
            remaining = remaining ushr 7
        }
        output.write(remaining.toInt())
    }

    private fun little(
        output: ByteArrayOutputStream,
        value: Int,
    ) {
        repeat(4) { output.write((value ushr (it * 8)) and 255) }
    }
}
