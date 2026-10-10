package com.posthog.hoglake.compaction

import com.posthog.hoglake.compaction.ParquetRewriter.OutputCodec
import com.posthog.hoglake.compaction.ParquetRewriter.Row
import com.posthog.hoglake.compaction.ParquetRewriter.WriterTuning
import org.apache.parquet.column.ColumnWriteStore
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.hadoop.CodecFactory
import org.apache.parquet.hadoop.ColumnChunkPageWriteStore
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.ColumnPath
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.OutputFile
import org.apache.parquet.io.PositionOutputStream
import org.apache.parquet.io.SeekableInputStream
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.MessageType
import java.nio.ByteBuffer

/**
 * The rewriter's parquet writer: every compaction output and every spill
 * file is written through one of these, driving parquet-java's
 * [ParquetFileWriter] directly.
 *
 * # Why not `ParquetWriter`
 *
 * Two things the rewrite needs that `ParquetWriter<T>` (and the
 * `ExampleParquetWriter` this replaced) cannot give it:
 *
 *  - RECORDS WITHOUT A COPY. `ParquetWriter` writes a `T` through a
 *    `WriteSupport<T>`; for the example model that meant building an
 *    output `SimpleGroup` per row and having `GroupWriter` walk it into
 *    the record consumer. [write] instead hands the consumer to the row's
 *    own [ParquetRewriter.RowPlan], which emits the mapped output record
 *    straight from the SOURCE row (hoglake#134 package D2).
 *  - MIXED ROW GROUPS. A row group copied byte for byte from an input
 *    ([appendRowGroups], package D1) has to sit in the same file as row
 *    groups encoded here, in a chosen order. `ParquetWriter` owns its
 *    `ParquetFileWriter` privately and flushes row groups on its own
 *    schedule, so nothing can be appended between them.
 *
 * So this is a port of parquet-java 1.18.1's `InternalParquetRecordWriter`
 * — the same column store, page store, record consumer and row-group
 * size check, constructed the way `ParquetWriter`'s constructor does —
 * with an append between encoded row groups. The cost of the choice is
 * that the row-group size heuristic ([checkRowGroupSize]) is ours to keep
 * in step with the library on an upgrade; it is the library's code, and
 * the spill block tests (`fat rows do not overshoot the spill block`) pin
 * its behaviour.
 *
 * Failure: any throw from [write] or [appendRowGroups] marks the writer
 * aborted, and [abort] (which [ParquetRewriter.writingTo] calls on every
 * failure path, after discarding the destination) closes the stream
 * without writing a footer.
 */
internal class OutputWriter(
    output: OutputFile,
    private val schema: MessageType,
    codec: OutputCodec,
    tuning: WriterTuning?,
) {
    private val rowGroupBytes: Long = tuning?.rowGroupBytes ?: ParquetWriter.DEFAULT_BLOCK_SIZE.toLong()

    private val props: ParquetProperties =
        ParquetProperties.builder()
            .apply {
                if (tuning != null) {
                    withPageSize(tuning.pageBytes)
                    withDictionaryPageSize(tuning.pageBytes)
                    // The row-group check shares the page check's cadence;
                    // from the first row, so it adapts to the row size.
                    withMinRowCountForPageSizeCheck(1)
                    withMaxRowCountForPageSizeCheck(tuning.maxRowsPerSizeCheck)
                }
            }
            .build()

    private val codecFactory = CodecFactory(ParquetRewriter.writerConfiguration(codec), props.pageSizeThreshold)
    private val compressor = codecFactory.getCompressor(codec.name)

    /** The stream [ParquetFileWriter] writes to, captured so [abort] can close it. */
    private var stream: PositionOutputStream? = null

    private val file: ParquetFileWriter =
        try {
            ParquetFileWriter(
                Capturing(output) { stream = it },
                schema,
                ParquetFileWriter.Mode.OVERWRITE,
                rowGroupBytes,
                ParquetWriter.MAX_PADDING_SIZE_DEFAULT,
                null,
                props,
            ).also { it.start() }
        } catch (e: Throwable) {
            codecFactory.release()
            runCatching { stream?.close() }
            throw e
        }

    private val columnIO = ColumnIOFactory(false).getColumnIO(schema)
    private var pageStore: ColumnChunkPageWriteStore? = null
    private var columnStore: ColumnWriteStore? = null
    private var consumer: RecordConsumer? = null

    private var recordCount = 0L
    private var recordCountForNextCheck = props.minRowCountForPageSizeCheck.toLong()
    private var nextRowGroupBytes = rowGroupBytes
    private var rowGroupOrdinal = 0
    private var aborted = false
    private var closed = false

    /** Row groups [appendRowGroups] copied in, and their bytes. */
    var rowGroupsAppended = 0
        private set
    var bytesAppended = 0L
        private set

    /** The footer, once [close] has written it. */
    val footer: ParquetMetadata
        get() = file.footer

    /** Write one row, emitted from its source by its plan. */
    fun write(row: Row) {
        check(!aborted) { "writer was aborted by an earlier failure" }
        try {
            val plan = checkNotNull(row.plan) { "row ${row.rowId} has no output plan" }
            plan.write(consumer(), row.group, row.rowId)
            recordCount++
            checkRowGroupSize()
        } catch (e: Throwable) {
            aborted = true
            throw e
        }
    }

    private fun consumer(): RecordConsumer {
        consumer?.let { return it }
        val pages =
            ColumnChunkPageWriteStore.builder()
                .withCompressorProvider { compressor }
                .withSchema(schema)
                .withAllocator(props.allocator)
                .withColumnIndexTruncateLength(props.columnIndexTruncateLength)
                .withPageWriteChecksumEnabled(props.pageWriteChecksumEnabled)
                .withFileEncryptor(null)
                .withRowGroupOrdinal(rowGroupOrdinal)
                .build()
        val columns = props.newColumnWriteStore(schema, pages, pages)
        pageStore = pages
        columnStore = columns
        return columnIO.getRecordWriter(columns).also { consumer = it }
    }

    /** `InternalParquetRecordWriter.checkBlockSizeReached`, 1.18.1. */
    private fun checkRowGroupSize() {
        if (recordCount >= props.rowGroupRowCountLimit) {
            flushRowGroup()
        } else if (recordCount >= recordCountForNextCheck) {
            val memSize = checkNotNull(columnStore).bufferedSize
            val recordSize = memSize / recordCount
            // Flush within ~2 records of the limit: slightly under is much
            // better than over at all.
            if (memSize > nextRowGroupBytes - 2 * recordSize) {
                flushRowGroup()
            } else {
                recordCountForNextCheck =
                    minOf(
                        maxOf(
                            props.minRowCountForPageSizeCheck.toLong(),
                            (recordCount + (nextRowGroupBytes / recordSize.toFloat()).toLong()) / 2,
                        ),
                        recordCount + props.maxRowCountForPageSizeCheck,
                    )
            }
        }
    }

    /** Write the buffered rows, if any, as one row group. */
    private fun flushRowGroup() {
        val rc = consumer ?: return
        try {
            rc.flush()
            if (recordCount > 0) {
                rowGroupOrdinal++
                file.startBlock(recordCount)
                checkNotNull(columnStore).flush()
                checkNotNull(pageStore).flushToFileWriter(file)
                recordCount = 0
                file.endBlock()
                nextRowGroupBytes = minOf(file.nextRowGroupSize, rowGroupBytes)
            }
        } finally {
            runCatching { columnStore?.close() }
            runCatching { pageStore?.close() }
            columnStore = null
            pageStore = null
            consumer = null
        }
        // The library's `min(max(minCheck, recordCount / 2), maxCheck)`,
        // evaluated as it is there: after the count has been reset.
        recordCountForNextCheck =
            minOf(props.minRowCountForPageSizeCheck, props.maxRowCountForPageSizeCheck).toLong()
    }

    /**
     * Copy every row group of the input [reader] has open into the output
     * BYTE FOR BYTE: each column chunk's pages as they are in the input
     * (its codec included), with the chunk's column index, offset index
     * (re-based to the chunk's new position) and bloom filter carried over
     * — `ParquetFileWriter.appendRowGroups` would drop the column index,
     * which is why this goes chunk by chunk through `appendColumnChunk`.
     * The caller has established that the input's schema IS [schema]
     * ([ParquetRewriter.appendRefusal]); the chunks are appended in the
     * output schema's column order, matched by path.
     *
     * [flushPending] first writes the rows buffered so far as their own
     * row group, so that the appended groups land AFTER them — what a
     * sorted merge needs. Without it the buffered rows keep accumulating
     * and land after the appended groups, which is what the unsorted path
     * wants: it keeps encoded row groups full-sized rather than cutting
     * one at every appended input.
     *
     * The bytes are read through a stream of [source]'s own, sequentially
     * per chunk; [reader] supplies the footer and the index structures.
     * For an [S3InputFile] that stream skips the footer prefetch
     * ([S3InputFile.newDataStream]): [reader] already paid that GET.
     *
     * Bloom filters: `appendColumnChunk` stores whatever it is handed for
     * the chunk, null included — there is no way to skip the call for a
     * chunk without one — and `ParquetFileWriter.serializeBloomFilters`
     * (1.18.1) skips the null entries. `an appended row group keeps its
     * codec and its bloom filter` pins it: an output mixing a bloomed
     * appended group with un-bloomed appended and encoded ones.
     *
     * Row-group ORDINALS advance only on encoded flushes ([flushRowGroup]);
     * appended groups do not advance them. The ordinal only feeds the page
     * store's encryption AAD, and the file encryptor is null, so nothing
     * reads it; the footer's own block ordinals are assigned by
     * `ParquetFileWriter.endBlock` for both kinds alike.
     */
    fun appendRowGroups(
        source: InputFile,
        reader: ParquetFileReader,
        flushPending: Boolean,
    ) {
        check(!aborted) { "writer was aborted by an earlier failure" }
        try {
            if (flushPending) flushRowGroup()
            val stream = (source as? S3InputFile)?.newDataStream() ?: source.newStream()
            BulkReads(stream).use { from ->
                for (block in reader.footer.blocks) {
                    // `ParquetFileWriter.endBlock` refuses a zero-row
                    // block; another writer may leave one in a file.
                    if (block.rowCount == 0L) continue
                    val byPath = block.columns.associateBy { it.path }
                    file.startBlock(block.rowCount)
                    for (descriptor in schema.columns) {
                        val chunk =
                            checkNotNull(byPath[ColumnPath.get(*descriptor.path)]) {
                                "appended input has no column chunk for ${descriptor.path.joinToString(".")}"
                            }
                        file.appendColumnChunk(
                            descriptor,
                            from,
                            chunk,
                            reader.readBloomFilter(chunk),
                            reader.readColumnIndex(chunk),
                            reader.readOffsetIndex(chunk),
                        )
                    }
                    file.endBlock()
                    rowGroupsAppended++
                    bytesAppended += block.compressedSize
                }
            }
            // Kept in step with flushRowGroup for a sink that pads row groups
            // to its block size. Every sink compaction writes to (S3, local
            // files) reports supportsBlockSize() == false, for which the
            // file writer answers the threshold itself and this is a no-op;
            // no test can red it.
            nextRowGroupBytes = minOf(file.nextRowGroupSize, rowGroupBytes)
        } catch (e: Throwable) {
            aborted = true
            throw e
        }
    }

    /** Flush, write the footer and close the stream. Only on success. */
    fun close() {
        if (closed) return
        check(!aborted) { "writer was aborted by an earlier failure" }
        try {
            flushRowGroup()
            file.end(mapOf(ParquetWriter.OBJECT_MODEL_NAME_PROP to MODEL_NAME))
        } catch (e: Throwable) {
            aborted = true
            file.abort()
            throw e
        } finally {
            closed = true
            runCatching { columnStore?.close() }
            runCatching { pageStore?.close() }
            runCatching { file.close() }
            codecFactory.release()
        }
    }

    /**
     * Give up: no footer, buffers released, the stream closed. The caller
     * has already discarded the destination, so closing it cannot
     * complete it.
     */
    fun abort() {
        if (closed) return
        closed = true
        aborted = true
        file.abort()
        runCatching { columnStore?.close() }
        runCatching { pageStore?.close() }
        runCatching { file.close() }
        runCatching { stream?.close() }
        codecFactory.release()
    }

    /**
     * A stream whose `read(byte[], off, len)` is one bulk read.
     * `ParquetFileWriter.appendColumnChunk` copies through exactly that
     * call, 8 KiB at a time, and an [InputFile] stream need not implement
     * it: parquet-java's own `LocalInputFile` inherits `InputStream`'s
     * byte-at-a-time loop, which made appending a 3 MiB row group cost
     * more than decoding it (measured: 17.8 s against 1.0 s for sixteen).
     * `S3InputFile` serves both from its buffer; this routes the call to
     * `read(ByteBuffer)`, which every [SeekableInputStream] implements in
     * bulk.
     */
    private class BulkReads(private val d: SeekableInputStream) : SeekableInputStream() {
        override fun read(): Int = d.read()

        override fun read(
            b: ByteArray,
            off: Int,
            len: Int,
        ): Int =
            // InputStream's contract for len == 0, whatever the delegate's
            // read(ByteBuffer) does with an empty buffer. Both streams
            // compaction reads (S3InputFile's, LocalInputFile's) already
            // return 0 there, so no test can red this guard.
            if (len == 0) 0 else d.read(ByteBuffer.wrap(b, off, len))

        override fun getPos(): Long = d.pos

        override fun seek(newPos: Long) = d.seek(newPos)

        override fun readFully(bytes: ByteArray) = d.readFully(bytes)

        override fun readFully(
            bytes: ByteArray,
            start: Int,
            len: Int,
        ) = d.readFully(bytes, start, len)

        override fun read(buf: ByteBuffer): Int = d.read(buf)

        override fun readFully(buf: ByteBuffer) = d.readFully(buf)

        override fun close() = d.close()
    }

    /** An [OutputFile] that reports the stream it creates. */
    private class Capturing(
        private val delegate: OutputFile,
        private val onCreate: (PositionOutputStream) -> Unit,
    ) : OutputFile by delegate {
        override fun create(blockSizeHint: Long): PositionOutputStream {
            val stream = delegate.create(blockSizeHint)
            onCreate(stream)
            return stream
        }

        override fun createOrOverwrite(blockSizeHint: Long): PositionOutputStream {
            val stream = delegate.createOrOverwrite(blockSizeHint)
            onCreate(stream)
            return stream
        }
    }

    companion object {
        /** The footer's `writer.model.name`. */
        const val MODEL_NAME = "hoglake-compaction"
    }
}
