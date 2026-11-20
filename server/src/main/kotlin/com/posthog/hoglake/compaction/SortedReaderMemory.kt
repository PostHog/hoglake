@file:Suppress("DEPRECATION")

package com.posthog.hoglake.compaction

import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.bytes.ByteBufferInputStream
import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.bytes.HeapByteBufferAllocator
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.Encoding
import org.apache.parquet.column.page.DataPage
import org.apache.parquet.column.page.DataPageV1
import org.apache.parquet.column.page.DataPageV2
import org.apache.parquet.column.page.DictionaryPage
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.column.page.PageReader
import org.apache.parquet.compression.CompressionCodecFactory
import org.apache.parquet.conf.HadoopParquetConfiguration
import org.apache.parquet.format.PageHeader
import org.apache.parquet.format.PageType
import org.apache.parquet.format.Statistics
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.hadoop.util.HadoopCodecs
import org.apache.parquet.io.DelegatingSeekableInputStream
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.SeekableInputStream
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import shaded.parquet.org.apache.thrift.TConfiguration
import shaded.parquet.org.apache.thrift.TException
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol
import shaded.parquet.org.apache.thrift.transport.TIOStreamTransport
import java.io.EOFException
import java.io.IOException
import java.io.InputStream
import java.nio.ByteBuffer
import java.util.Optional
import java.util.PrimitiveIterator
import kotlin.math.max
import kotlin.math.min

/** A deterministic reader refusal. The caller records it through the sorted path's refusal cache. */
internal class SortedReaderMemoryException(
    val chargedBytes: Long,
    val allowanceBytes: Long,
    message: String,
    val binaryValueRefusal: Boolean = false,
) : RuntimeException(message)

/** Profiles compressed chunks plus live decoder state. It never charges all decoded row-group values. */
internal object SortedReaderMemory {
    private const val MIB = 1024L * 1024
    const val PAGE_ROW_COUNT_LIMIT = 20_000
    const val MAX_ALLOCATION_BYTES = 8 * MIB
    const val PAGE_OBJECT_BYTES = 1024L
    const val COLUMN_OBJECT_BYTES = 4096L
    const val DICTIONARY_ENTRY_BYTES = 96L
    private const val DELTA_PADDING_BYTES = 64 * 1024L

    /** The stream factory can supply a footer-free stream. This method owns each supplied stream. */
    fun profile(
        input: InputFile,
        footer: ParquetMetadata,
        requestedSchema: MessageType,
        allowanceBytes: Long,
        maxBinaryBytes: Long = allowanceBytes,
        sourceReadaheadBytes: Long = 0,
        streamFactory: () -> SeekableInputStream = input::newStream,
    ): Profile {
        require(allowanceBytes > 0)
        require(maxBinaryBytes > 0)
        require(sourceReadaheadBytes >= 0)
        val inputLength = input.length
        if (inputLength < 0) corrupt("negative input file size")
        // S3 fill receives Parquet's allocation size as its requested read size.
        // Local streams have no second source buffer.
        val ioBytes =
            if (sourceReadaheadBytes == 0L) {
                0
            } else {
                multiply(min(inputLength, max(MAX_ALLOCATION_BYTES, sourceReadaheadBytes)), 2)
            }
        val requested = requestedSchema.columns.associateBy { it.path.toList() }
        val groups = ArrayList<GroupProfile>()
        var profileBytes = 256L
        val emptyGroup = GroupProfile(emptyMap(), 0)
        streamFactory().use { stream ->
            for (block in footer.blocks) {
                if (block.rowCount < 0) corrupt("negative row-group row count")
                profileBytes = add(profileBytes, 128)
                refuse(profileBytes, allowanceBytes, "reader profile")
                if (block.rowCount == 0L) {
                    // Keep the footer index without inspecting unused column chunks.
                    groups += emptyGroup
                    continue
                }
                val columns = LinkedHashMap<List<String>, ColumnProfile>()
                for (chunk in block.columns) {
                    val descriptor = requested[chunk.path.toArray().toList()] ?: continue
                    val column = ColumnProfile(descriptor, chunk.totalSize)
                    if (column.compressedBytes < 0) corrupt("negative compressed chunk size")
                    val start = chunk.startingPos
                    val end = add(start, column.compressedBytes)
                    if (start < 0 || end > inputLength) corrupt("column chunk exceeds its file")
                    // Refuse before Parquet allocates the compressed chunk.
                    refuse(add(ioBytes, column.compressedBytes), allowanceBytes, "compressed column chunk")
                    stream.seek(start)
                    var values = 0L
                    var dictionary = false
                    while (stream.pos < end) {
                        val headerStart = stream.pos
                        val header = header(stream, min(100 * MIB, max(1L, allowanceBytes / 2)))
                        val headerBytes = stream.pos - headerStart
                        val compressed = header.compressed_page_size.toLong()
                        val uncompressed = header.uncompressed_page_size.toLong()
                        if (compressed < 0 || uncompressed < 0) corrupt("negative page size")
                        val next = add(stream.pos, compressed)
                        if (next > end) corrupt("page exceeds its column chunk")
                        column.pageCount = add(column.pageCount, 1)
                        column.statisticsBytes = add(column.statisticsBytes, statisticsBytes(header))
                        column.maxCompressedPage = max(column.maxCompressedPage, compressed)
                        column.maxPage = max(column.maxPage, uncompressed)
                        column.headerBytes = max(column.headerBytes, headerBytes)
                        when (header.type) {
                            PageType.DICTIONARY_PAGE -> {
                                if (dictionary || values != 0L) corrupt("misplaced dictionary page")
                                dictionary = true
                                val dictionaryHeader =
                                    header.dictionary_page_header
                                        ?: corrupt("missing dictionary header")
                                val entries = dictionaryHeader.num_values
                                if (entries < 0) corrupt("negative dictionary entry count")
                                if (dictionaryHeader.encoding != org.apache.parquet.format.Encoding.PLAIN &&
                                    dictionaryHeader.encoding != org.apache.parquet.format.Encoding.PLAIN_DICTIONARY
                                ) {
                                    corrupt("unsupported dictionary encoding")
                                }
                                validateDictionaryShape(descriptor, entries, uncompressed)
                                column.dictionaryEntries = entries
                                column.dictionaryBytes = uncompressed
                            }
                            PageType.DATA_PAGE -> {
                                val data = header.data_page_header ?: corrupt("missing data page header")
                                if (data.num_values < 0) corrupt("negative page value count")
                                values = add(values, data.num_values.toLong())
                                column.observe(uncompressed, data.num_values, Encoding.valueOf(data.encoding.name))
                            }
                            PageType.DATA_PAGE_V2 -> {
                                val data = header.data_page_header_v2 ?: corrupt("missing V2 page header")
                                if (data.num_values < 0 || data.num_nulls < 0 ||
                                    data.num_nulls > data.num_values || data.num_rows < 0
                                ) {
                                    corrupt("invalid V2 page counts")
                                }
                                val levels =
                                    add(
                                        data.repetition_levels_byte_length.toLong(),
                                        data.definition_levels_byte_length.toLong(),
                                    )
                                if (data.repetition_levels_byte_length < 0 || data.definition_levels_byte_length < 0 ||
                                    levels > compressed || levels > uncompressed
                                ) {
                                    corrupt("invalid V2 level lengths")
                                }
                                values = add(values, data.num_values.toLong())
                                column.observe(uncompressed, data.num_values, Encoding.valueOf(data.encoding.name))
                            }
                            PageType.INDEX_PAGE -> Unit
                            else -> corrupt("missing page type")
                        }
                        if (values > chunk.valueCount) corrupt("page values exceed the footer count")
                        stream.seek(next)
                        refuse(column.charge(), allowanceBytes, "column reader")
                    }
                    if (values != chunk.valueCount) corrupt("page values disagree with the footer")
                    columns[descriptor.path.toList()] = column
                    // Covers wide-pointer objects, both path arrays, and retained schema names.
                    val pathBytes =
                        descriptor.path.fold(0L) { bytes, segment ->
                            add(bytes, add(256, multiply(segment.length.toLong(), 2)))
                        }
                    profileBytes = add(profileBytes, add(1024, pathBytes))
                }
                val group = GroupProfile(columns, ioBytes)
                refuse(add(group.chargeBytes, profileBytes), allowanceBytes, "row-group reader")
                groups += group
            }
        }
        val charge = add(groups.maxOfOrNull { it.chargeBytes } ?: ioBytes, profileBytes)
        refuse(charge, allowanceBytes, "file reader")
        return Profile(groups, charge, profileBytes, allowanceBytes, maxBinaryBytes, requestedSchema)
    }

    internal class Profile internal constructor(
        private val groups: List<GroupProfile>,
        val chargeBytes: Long,
        val retainedMetadataBytes: Long,
        private val allowanceBytes: Long,
        private val maxBinaryBytes: Long,
        private val schema: MessageType,
    ) {
        private val maxPageBytes =
            groups.maxOfOrNull {
                    group ->
                group.columns.values.maxOfOrNull { it.maxPage } ?: 0
            } ?: 0

        /** Reuse the scanned summary with the final merge-head limit. */
        fun withMaxBinaryBytes(limit: Long): Profile {
            require(limit > 0)
            return Profile(groups, chargeBytes, retainedMetadataBytes, allowanceBytes, limit, schema)
        }

        /** Each call creates a fresh factory. ParquetFileReader owns that factory. */
        fun options(): ParquetReadOptions {
            val configuration = HadoopParquetConfiguration()
            val factory = GuardedCodecs(HadoopCodecs.newFactory(configuration, 0), maxPageBytes, allowanceBytes)
            try {
                return ParquetReadOptions.builder(configuration)
                    .withCodecFactory(factory)
                    .withAllocator(HeapByteBufferAllocator())
                    .useOffHeapDecryptBuffer(false)
                    .withUseHadoopVectoredIo(false)
                    .withMaxAllocationInBytes(MAX_ALLOCATION_BYTES.toInt())
                    .build()
            } catch (error: Throwable) {
                closeAfterFailure(error) { factory.release() }
                throw error
            }
        }

        /** The returned reader owns the supplied stream. */
        fun open(
            input: InputFile,
            footer: ParquetMetadata,
            stream: SeekableInputStream,
        ): Reader = openOwned(input, footer, stream, ::options)

        /** The separate factory also makes construction failures observable in focused tests. */
        internal fun openOwned(
            input: InputFile,
            footer: ParquetMetadata,
            stream: SeekableInputStream,
            optionsFactory: () -> ParquetReadOptions,
        ): Reader {
            val ownedStream = CloseOnceStream(stream)
            var options: ParquetReadOptions? = null
            var reader: ParquetFileReader? = null
            try {
                options = optionsFactory()
                reader = ParquetFileReader(input, footer, options, ownedStream)
                reader.setRequestedSchema(schema)
                return Reader(reader, this)
            } catch (error: Throwable) {
                if (reader == null) {
                    closeAfterFailure(error) { ownedStream.close() }
                    closeAfterFailure(error) { options?.codecFactory?.release() }
                } else {
                    closeAfterFailure(error) { reader.close() }
                }
                throw error
            }
        }

        /** Use indexed readRowGroup. Close this store before clearing the old record reader. */
        fun wrap(
            rowGroupIndex: Int,
            pages: PageReadStore,
        ): PageReadStore {
            val group = groups.getOrNull(rowGroupIndex) ?: corrupt("unknown row-group index")
            return object : PageReadStore {
                private val readers = HashMap<List<String>, PageReader>()
                private var closed = false

                override fun getRowCount(): Long = pages.rowCount

                override fun getRowIndexOffset(): Optional<Long> = pages.rowIndexOffset

                override fun getRowIndexes(): Optional<PrimitiveIterator.OfLong> = pages.rowIndexes

                override fun getPageReader(descriptor: ColumnDescriptor): PageReader =
                    readers.getOrPut(descriptor.path.toList()) {
                        val column = group.columns[descriptor.path.toList()] ?: corrupt("unprofiled column")
                        GuardedPages(pages.getPageReader(descriptor), column, maxBinaryBytes, allowanceBytes)
                    }

                override fun close() {
                    if (!closed) {
                        closed = true
                        readers.clear()
                        pages.close()
                    }
                }
            }
        }
    }

    /** The pinned constructor sometimes closes its stream before throwing. */
    private class CloseOnceStream(private val delegate: SeekableInputStream) : DelegatingSeekableInputStream(delegate) {
        private var closed = false

        override fun getPos(): Long = delegate.pos

        override fun seek(position: Long) = delegate.seek(position)

        override fun readFully(bytes: ByteArray) = delegate.readFully(bytes)

        override fun readFully(
            bytes: ByteArray,
            offset: Int,
            length: Int,
        ) = delegate.readFully(bytes, offset, length)

        override fun read(buffer: ByteBuffer): Int = delegate.read(buffer)

        override fun readFully(buffer: ByteBuffer) = delegate.readFully(buffer)

        override fun close() {
            if (!closed) {
                closed = true
                delegate.close()
            }
        }
    }

    private fun closeAfterFailure(
        error: Throwable,
        cleanup: () -> Unit,
    ) {
        try {
            cleanup()
        } catch (failure: Throwable) {
            if (failure !== error) error.addSuppressed(failure)
        }
    }

    internal class Reader internal constructor(
        private val delegate: ParquetFileReader,
        private val profile: Profile,
    ) : AutoCloseable {
        private var active: PageReadStore? = null
        private var closed = false

        /** Clear the old record-reader reference before this call. */
        fun readRowGroup(index: Int): PageReadStore {
            check(!closed) { "reader is closed" }
            closeActive()
            val pages = delegate.readRowGroup(index) ?: corrupt("unknown row-group index")
            try {
                return profile.wrap(index, pages).also { active = it }
            } catch (error: Throwable) {
                pages.close()
                throw error
            }
        }

        private fun closeActive() {
            try {
                active?.close()
            } finally {
                active = null
            }
        }

        override fun close() {
            if (!closed) {
                closed = true
                try {
                    active?.close()
                } finally {
                    active = null
                    delegate.close()
                }
            }
        }
    }

    internal class GroupProfile(val columns: Map<List<String>, ColumnProfile>, ioBytes: Long) {
        val chargeBytes: Long =
            run {
                var steady = ioBytes
                var transition = 0L
                for (column in columns.values) {
                    steady = add(steady, column.charge())
                    transition = max(transition, add(column.maxPage, column.decoderBytes))
                }
                add(steady, transition)
            }
    }

    internal class ColumnProfile(val descriptor: ColumnDescriptor, val compressedBytes: Long) {
        var pageCount = 0L
        var statisticsBytes = 0L
        var headerBytes = 0L
        var maxCompressedPage = 0L
        var maxPage = 0L
        var maxDataPage = 0L
        var maxValues = 0
        var dictionaryEntries = 0
        var dictionaryBytes = 0L
        var decoderBytes = 0L

        fun observe(
            bytes: Long,
            values: Int,
            encoding: Encoding,
        ) {
            maxDataPage = max(maxDataPage, bytes)
            maxValues = max(maxValues, values)
            var decoder = COLUMN_OBJECT_BYTES
            if (descriptor.maxRepetitionLevel > 0) decoder = add(decoder, rleAllowance(bytes, values, false))
            if (descriptor.maxDefinitionLevel > 0) decoder = add(decoder, rleAllowance(bytes, values, false))
            decoder =
                add(
                    decoder,
                    when (encoding) {
                        Encoding.PLAIN_DICTIONARY, Encoding.RLE_DICTIONARY ->
                            rleAllowance(bytes, values, dictionaryEntries <= 1)
                        Encoding.RLE -> rleAllowance(bytes, values, false)
                        Encoding.DELTA_BINARY_PACKED, Encoding.DELTA_LENGTH_BYTE_ARRAY ->
                            add(multiply(values.toLong() + 1, 16), DELTA_PADDING_BYTES)
                        Encoding.DELTA_BYTE_ARRAY ->
                            add(add(multiply(values.toLong() + 1, 32), 2 * DELTA_PADDING_BYTES), multiply(bytes, 2))
                        Encoding.BYTE_STREAM_SPLIT -> bytes
                        Encoding.PLAIN ->
                            if (descriptor.primitiveType.primitiveTypeName == PrimitiveTypeName.BOOLEAN) bytes else 0
                        else -> 0
                    },
                )
            decoderBytes = max(decoderBytes, decoder)
        }

        fun charge(): Long {
            val dictionary = dictionaryCharge(descriptor, dictionaryBytes, dictionaryEntries.toLong())
            return add(
                add(add(compressedBytes, maxDataPage), add(dictionary, decoderBytes)),
                add(add(COLUMN_OBJECT_BYTES, multiply(pageCount, PAGE_OBJECT_BYTES)), multiply(statisticsBytes, 2)),
            )
        }
    }

    /** Shared by the observed profile and the structural estimate. */
    fun dictionaryCharge(
        descriptor: ColumnDescriptor,
        uncompressedBytes: Long,
        entries: Long,
    ): Long {
        require(uncompressedBytes >= 0)
        require(entries >= 0)
        return when (descriptor.primitiveType.primitiveTypeName) {
            PrimitiveTypeName.BINARY, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, PrimitiveTypeName.INT96 ->
                add(multiply(uncompressedBytes, 2), multiply(entries, DICTIONARY_ENTRY_BYTES))
            else -> add(uncompressedBytes, multiply(entries, primitiveWidth(descriptor).toLong()))
        }
    }

    private class GuardedCodecs(
        private val delegate: CompressionCodecFactory,
        private val pageBytes: Long,
        private val allowanceBytes: Long,
    ) : CompressionCodecFactory {
        private var released = false

        override fun getCompressor(codec: CompressionCodecName): CompressionCodecFactory.BytesInputCompressor =
            delegate.getCompressor(
                codec,
            )

        override fun getDecompressor(codec: CompressionCodecName): CompressionCodecFactory.BytesInputDecompressor {
            val target = delegate.getDecompressor(codec)
            return object : CompressionCodecFactory.BytesInputDecompressor {
                private fun check(size: Int) {
                    if (size < 0) corrupt("negative decompressed size")
                    refuse(size.toLong(), min(pageBytes, allowanceBytes), "decompressed page")
                }

                override fun decompress(
                    bytes: BytesInput,
                    decompressedSize: Int,
                ): BytesInput {
                    check(decompressedSize)
                    if (codec == CompressionCodecName.SNAPPY &&
                        unsignedInt(bytes.toInputStream()) != decompressedSize.toLong()
                    ) {
                        corrupt("Snappy size disagrees with its page header")
                    }
                    // Some heap codecs return a one-use stream.
                    // Materialize it once before the validator reads the page.
                    return BytesInput.from(target.decompress(bytes, decompressedSize).toByteBuffer())
                }

                override fun decompress(
                    input: ByteBuffer,
                    compressedSize: Int,
                    output: ByteBuffer,
                    decompressedSize: Int,
                ) {
                    check(decompressedSize)
                    if (compressedSize < 0 || compressedSize > input.remaining()) corrupt("invalid compressed size")
                    if (codec == CompressionCodecName.SNAPPY) {
                        val duplicate = input.duplicate()
                        duplicate.limit(duplicate.position() + compressedSize)
                        if (unsignedInt(ByteBufferInputStream.wrap(duplicate)) != decompressedSize.toLong()) {
                            corrupt("Snappy size disagrees with its page header")
                        }
                    }
                    target.decompress(input, compressedSize, output, decompressedSize)
                }

                // The delegate factory owns its cached decompressors.
                override fun release() = Unit
            }
        }

        override fun release() {
            if (!released) {
                released = true
                delegate.release()
            }
        }
    }

    private class GuardedPages(
        private val delegate: PageReader,
        private val column: ColumnProfile,
        private val maxBinaryBytes: Long,
        private val allowanceBytes: Long,
    ) : PageReader {
        private var previousBinaryLength = 0L

        override fun getTotalValueCount(): Long = delegate.totalValueCount

        override fun readDictionaryPage(): DictionaryPage? {
            val page = delegate.readDictionaryPage() ?: return null
            if (page.dictionarySize != column.dictionaryEntries ||
                page.uncompressedSize.toLong() != column.dictionaryBytes
            ) {
                corrupt("dictionary disagrees with its profile")
            }
            validateDictionaryShape(column.descriptor, page.dictionarySize, page.uncompressedSize.toLong())
            if (column.descriptor.primitiveType.primitiveTypeName == PrimitiveTypeName.BINARY) {
                validatePlainBinary(page.bytes.toInputStream(), page.dictionarySize, maxBinaryBytes)
            }
            return page
        }

        override fun readPage(): DataPage? {
            val page = delegate.readPage() ?: return null
            if (page.valueCount < 0 || page.valueCount > column.maxValues ||
                page.uncompressedSize.toLong() > column.maxDataPage
            ) {
                corrupt("data page disagrees with its profile")
            }
            val budget = DecoderBudget(column.decoderBytes, allowanceBytes)
            page.accept(
                object : DataPage.Visitor<Unit> {
                    override fun visit(value: DataPageV1) {
                        val stream = value.bytes.toInputStream()
                        levelsV1(
                            stream,
                            value.rlEncoding,
                            column.descriptor.maxRepetitionLevel,
                            value.valueCount,
                            false,
                            budget,
                        )
                        val nonNull =
                            levelsV1(
                                stream,
                                value.dlEncoding,
                                column.descriptor.maxDefinitionLevel,
                                value.valueCount,
                                true,
                                budget,
                            )
                        validateValues(stream, value.valueEncoding, nonNull, budget)
                    }

                    override fun visit(value: DataPageV2) {
                        val nonNull = levelsV2(value, budget)
                        validateValues(value.data.toInputStream(), value.dataEncoding, nonNull, budget)
                    }
                },
            )
            return page
        }

        private fun levelsV2(
            page: DataPageV2,
            budget: DecoderBudget,
        ): Int {
            val descriptor = column.descriptor
            if (descriptor.maxRepetitionLevel > 0) {
                rle(
                    page.repetitionLevels.toInputStream(),
                    width(descriptor.maxRepetitionLevel),
                    page.valueCount,
                    null,
                    budget,
                )
            } else if (page.repetitionLevels.size() != 0L) {
                corrupt("unexpected repetition levels")
            }
            val nonNull =
                if (descriptor.maxDefinitionLevel > 0) {
                    rle(
                        page.definitionLevels.toInputStream(),
                        width(descriptor.maxDefinitionLevel),
                        page.valueCount,
                        descriptor.maxDefinitionLevel,
                        budget,
                    )
                } else {
                    if (page.definitionLevels.size() != 0L) corrupt("unexpected definition levels")
                    page.valueCount
                }
            if (page.nullCount < 0 || page.valueCount - page.nullCount != nonNull) {
                corrupt(
                    "V2 null count disagrees with its levels",
                )
            }
            return nonNull
        }

        private fun validateValues(
            stream: ByteBufferInputStream,
            encoding: Encoding,
            nonNull: Int,
            budget: DecoderBudget,
        ) {
            when (encoding) {
                Encoding.PLAIN ->
                    when (column.descriptor.primitiveType.primitiveTypeName) {
                        PrimitiveTypeName.BINARY -> validatePlainBinary(stream, nonNull, maxBinaryBytes)
                        PrimitiveTypeName.BOOLEAN -> budget.spend(stream.available().toLong())
                        else -> {
                            val bytes = multiply(nonNull.toLong(), primitiveWidth(column.descriptor).toLong())
                            if (bytes > stream.available()) corrupt("truncated plain values")
                        }
                    }
                Encoding.PLAIN_DICTIONARY, Encoding.RLE_DICTIONARY -> {
                    if (stream.available() == 0 && nonNull == 0) return
                    val bitWidth = byte(stream)
                    rle(stream, bitWidth, nonNull, null, budget)
                }
                Encoding.RLE -> {
                    val length = littleInt(stream)
                    if (length < 0) corrupt("negative RLE value length")
                    rle(stream.sliceStream(length.toLong()), 1, nonNull, null, budget)
                }
                Encoding.DELTA_BINARY_PACKED -> delta(stream, nonNull, budget)
                Encoding.DELTA_LENGTH_BYTE_ARRAY -> {
                    var payload = 0L
                    delta(stream, nonNull, budget) { _, value ->
                        binaryLength(value)
                        payload = add(payload, value)
                    }
                    if (payload > stream.available()) corrupt("truncated delta binary payload")
                }
                Encoding.DELTA_BYTE_ARRAY -> {
                    budget.spend(multiply(nonNull.toLong(), 4))
                    val prefixes = IntArray(nonNull)
                    delta(stream, nonNull, budget) { index, value ->
                        if (value < 0 || value > Int.MAX_VALUE) corrupt("invalid delta prefix length")
                        prefixes[index] = value.toInt()
                    }
                    var payload = 0L
                    var previous = previousBinaryLength
                    var longest = 0L
                    val binaryLimit = min(maxBinaryBytes, column.maxDataPage)
                    delta(stream, nonNull, budget) { index, suffix ->
                        if (suffix < 0 || suffix > Int.MAX_VALUE) corrupt("invalid delta suffix length")
                        val prefix = prefixes[index].toLong()
                        if (prefix > previous) corrupt("delta prefix exceeds the previous value")
                        previous = add(prefix, suffix)
                        longest = max(longest, previous)
                        refuse(previous, binaryLimit, "decoded delta binary value", previous > maxBinaryBytes)
                        payload = add(payload, suffix)
                    }
                    if (payload > stream.available()) corrupt("truncated delta suffix payload")
                    previousBinaryLength = previous
                    budget.spend(multiply(longest, 2))
                }
                Encoding.BYTE_STREAM_SPLIT -> budget.spend(stream.available().toLong())
                else -> corrupt("unsupported page encoding $encoding")
            }
        }

        private fun binaryLength(value: Long) {
            if (value < 0 || value > Int.MAX_VALUE) corrupt("invalid binary length")
            refuse(value, maxBinaryBytes, "binary value", true)
        }
    }

    private class DecoderBudget(private val limit: Long, private val allowance: Long) {
        private var spent = 0L

        fun limitBytes(): Long = min(limit, allowance)

        fun spend(bytes: Long) {
            spent = add(spent, bytes)
            refuse(spent, min(limit, allowance), "encoded page expansion")
        }
    }

    private fun levelsV1(
        stream: ByteBufferInputStream,
        encoding: Encoding,
        maximum: Int,
        values: Int,
        countPresent: Boolean,
        budget: DecoderBudget,
    ): Int {
        if (maximum == 0) return values
        return when (encoding) {
            Encoding.RLE -> {
                val length = littleInt(stream)
                if (length < 0) corrupt("negative level length")
                rle(
                    stream.sliceStream(length.toLong()),
                    width(maximum),
                    values,
                    if (countPresent) maximum else null,
                    budget,
                )
            }
            Encoding.BIT_PACKED -> {
                val bits = width(maximum)
                val bytes = (multiply(values.toLong(), bits.toLong()) + 7) / 8
                val levels = stream.sliceStream(bytes)
                var present = 0
                val reader = PackedBits(levels, false)
                repeat(values) {
                    val level = reader.read(bits).toInt()
                    if (level > maximum) corrupt("invalid bit-packed level")
                    if (level == maximum) present++
                }
                if (countPresent) present else values
            }
            else -> corrupt("unsupported level encoding")
        }
    }

    /** Only packed runs allocate arrays. An RLE run has constant allocation size. */
    private fun rle(
        stream: ByteBufferInputStream,
        bits: Int,
        values: Int,
        countedValue: Int?,
        budget: DecoderBudget,
    ): Int {
        if (bits !in 0..32) corrupt("invalid RLE bit width")
        var left = values.toLong()
        var present = 0L
        var maxPacked = 0L
        var maxEncoded = 0L
        while (left > 0) {
            val header = unsignedInt(stream)
            val groups = header ushr 1
            if (groups == 0L) corrupt("empty RLE run")
            if (header and 1L == 0L) {
                if (groups > left) corrupt("RLE run exceeds its page")
                var value = 0L
                repeat((bits + 7) / 8) { shift -> value = value or (byte(stream).toLong() shl (shift * 8)) }
                if (countedValue != null && value == countedValue.toLong()) present += groups
                left -= groups
            } else {
                val count = multiply(groups, 8)
                // The final packed group can contain at most seven padding values.
                if (count > add(left, 7)) corrupt("packed run exceeds legal padding")
                if (count > Int.MAX_VALUE) corrupt("packed run exceeds the JVM array limit")
                val encoded = multiply(groups, bits.toLong())
                if (encoded > stream.available()) corrupt("truncated packed run")
                maxPacked = max(maxPacked, multiply(count, 4))
                maxEncoded = max(maxEncoded, encoded)
                // Check before Parquet constructs the run arrays.
                refuse(add(multiply(maxPacked, 2), maxEncoded), budgetLimit(budget), "RLE packed run")
                val run = stream.sliceStream(encoded)
                if (countedValue != null) {
                    val reader = PackedBits(run)
                    repeat(min(count, left).toInt()) { if (reader.read(bits) == countedValue.toLong()) present++ }
                }
                left -= min(count, left)
            }
        }
        if (stream.available() != 0) corrupt("trailing bytes in RLE stream")
        budget.spend(add(add(multiply(maxPacked, 2), maxEncoded), 128))
        return if (countedValue == null) values else present.toInt()
    }

    // The preliminary check uses the same per-page budget as the accumulated check.
    private fun budgetLimit(budget: DecoderBudget): Long = budget.limitBytes()

    private fun delta(
        stream: ByteBufferInputStream,
        expected: Int,
        budget: DecoderBudget,
        consume: ((Int, Long) -> Unit)? = null,
    ) {
        val block = unsignedInt(stream)
        val miniBlocks = unsignedInt(stream)
        val count = unsignedInt(stream)
        if (block == 0L || miniBlocks == 0L || block % miniBlocks != 0L || count != expected.toLong()) {
            corrupt("invalid DELTA allocation counts")
        }
        val mini = block / miniBlocks
        if (mini == 0L || mini % 8 != 0L || block > Int.MAX_VALUE || miniBlocks > Int.MAX_VALUE) {
            corrupt("invalid DELTA miniblock configuration")
        }
        val rounded = add(multiply((count + mini - 1) / mini, mini), 1)
        if (rounded > Int.MAX_VALUE) corrupt("DELTA value array exceeds the JVM limit")
        budget.spend(add(add(multiply(rounded, 8), multiply(miniBlocks, 4)), 64))
        var previous = zigzag(stream)
        var index = 0
        if (expected > 0) {
            consume?.invoke(index, previous)
            index++
        }
        while (index < expected) {
            val minimum = zigzag(stream)
            val widths = stream.sliceStream(miniBlocks)
            var miniIndex = 0L
            while (miniIndex < miniBlocks && index < expected) {
                val bits = byte(widths)
                if (bits > 64) corrupt("invalid DELTA bit width")
                val encoded = multiply(mini, bits.toLong()) / 8
                val input = stream.sliceStream(encoded)
                val countInMini = min(mini, (expected - index).toLong()).toInt()
                if (consume == null) {
                    index += countInMini
                } else {
                    val packed = PackedBits(input)
                    repeat(countInMini) {
                        previous += minimum + packed.read(bits)
                        consume(index++, previous)
                    }
                }
                miniIndex++
            }
        }
    }

    private class PackedBits(private val input: InputStream, private val littleEndian: Boolean = true) {
        private var current = 0
        private var left = 0

        fun read(bits: Int): Long {
            var result = 0L
            var consumed = 0
            while (consumed < bits) {
                if (left == 0) {
                    current = byte(input)
                    left = 8
                }
                val take = min(left, bits - consumed)
                val mask = (1 shl take) - 1
                val next =
                    if (littleEndian) {
                        val value = current and mask
                        current = current ushr take
                        value
                    } else {
                        val value = (current ushr (left - take)) and mask
                        value
                    }
                left -= take
                result =
                    if (littleEndian) {
                        result or (next.toLong() shl consumed)
                    } else {
                        (result shl take) or next.toLong()
                    }
                consumed += take
            }
            return result
        }
    }

    private fun header(
        stream: InputStream,
        limit: Long,
    ): PageHeader {
        val bytes = min(limit, Int.MAX_VALUE.toLong()).toInt()
        val configuration = TConfiguration(bytes, bytes, 32)
        try {
            val transport = TIOStreamTransport(configuration, stream)
            transport.updateKnownMessageSize(0)
            val protocol = TCompactProtocol(transport, limit, limit)
            return PageHeader().also { it.read(protocol) }
        } catch (error: TException) {
            throw IOException("invalid or oversized page header", error)
        }
    }

    private fun statisticsBytes(header: PageHeader): Long {
        val statistics: Statistics =
            header.data_page_header?.statistics
                ?: header.data_page_header_v2?.statistics ?: return 0
        var bytes = 0L
        for (buffer in listOf(
            statistics.bufferForMin(),
            statistics.bufferForMax(),
            statistics.bufferForMin_value(),
            statistics.bufferForMax_value(),
        )) {
            bytes = add(bytes, buffer?.remaining()?.toLong() ?: 0)
        }
        return bytes
    }

    private fun validateDictionaryShape(
        descriptor: ColumnDescriptor,
        entries: Int,
        bytes: Long,
    ) {
        val minimum =
            when (descriptor.primitiveType.primitiveTypeName) {
                PrimitiveTypeName.BINARY -> multiply(entries.toLong(), 4)
                PrimitiveTypeName.BOOLEAN -> (entries.toLong() + 7) / 8
                else -> multiply(entries.toLong(), primitiveWidth(descriptor).toLong())
            }
        if (minimum > bytes) corrupt("dictionary entries exceed their encoded bytes")
    }

    private fun validatePlainBinary(
        stream: ByteBufferInputStream,
        count: Int,
        limit: Long,
    ) {
        repeat(count) {
            val length = littleInt(stream)
            if (length < 0 || length > stream.available()) corrupt("invalid plain binary length")
            refuse(length.toLong(), limit, "binary value", true)
            stream.skipFully(length.toLong())
        }
    }

    private fun primitiveWidth(descriptor: ColumnDescriptor): Int =
        when (descriptor.primitiveType.primitiveTypeName) {
            PrimitiveTypeName.INT32, PrimitiveTypeName.FLOAT -> 4
            PrimitiveTypeName.INT64, PrimitiveTypeName.DOUBLE -> 8
            PrimitiveTypeName.INT96 -> 12
            PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY ->
                descriptor.primitiveType.typeLength.also {
                    if (it <= 0) corrupt("invalid fixed binary width")
                }
            PrimitiveTypeName.BOOLEAN -> 1
            PrimitiveTypeName.BINARY -> 0
        }

    private fun rleAllowance(
        bytes: Long,
        values: Int,
        zeroWidth: Boolean,
    ): Long {
        val valueBytes = multiply(values.toLong() + 7, 4)
        val packed = if (zeroWidth) valueBytes else min(multiply(bytes, 32), valueBytes)
        return add(add(multiply(packed, 2), min(bytes, valueBytes)), 128)
    }

    private fun width(maximum: Int): Int = 32 - Integer.numberOfLeadingZeros(maximum)

    private fun byte(input: InputStream): Int =
        input.read().also {
            if (it < 0) throw EOFException("truncated page control")
        }

    private fun littleInt(input: InputStream): Int =
        byte(input) or (byte(input) shl 8) or (byte(input) shl 16) or (byte(input) shl 24)

    private fun unsignedInt(input: InputStream): Long {
        var result = 0L
        repeat(5) { index ->
            val next = byte(input)
            if (index == 4 && next and 0xf0 != 0) corrupt("oversized unsigned integer")
            result = result or ((next and 0x7f).toLong() shl (index * 7))
            if (next and 0x80 == 0) return result
        }
        corrupt("unterminated unsigned integer")
    }

    private fun zigzag(input: InputStream): Long {
        var result = 0L
        repeat(10) { index ->
            val next = byte(input)
            if (index == 9 && next and 0xfe != 0) corrupt("oversized DELTA integer")
            result = result or ((next and 0x7f).toLong() shl (index * 7))
            if (next and 0x80 == 0) return (result ushr 1) xor -(result and 1)
        }
        corrupt("unterminated DELTA integer")
    }

    private fun add(
        left: Long,
        right: Long,
    ): Long =
        try {
            Math.addExact(left, right)
        } catch (_: ArithmeticException) {
            corrupt("reader size overflow")
        }

    private fun multiply(
        left: Long,
        right: Long,
    ): Long =
        try {
            Math.multiplyExact(left, right)
        } catch (_: ArithmeticException) {
            corrupt("reader size overflow")
        }

    private fun refuse(
        bytes: Long,
        allowance: Long,
        what: String,
        binaryValueRefusal: Boolean = false,
    ) {
        if (bytes > allowance) {
            throw SortedReaderMemoryException(
                bytes,
                allowance,
                "$what needs $bytes bytes. The allowance is $allowance bytes.",
                binaryValueRefusal,
            )
        }
    }

    private fun corrupt(message: String): Nothing = throw IOException(message)
}
