package com.posthog.hoglake.compaction

import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import com.sun.management.OperatingSystemMXBean
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.format.Util
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.io.SeekableInputStream
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.util.BitSet
import java.util.Random
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/** Generates reusable event inputs, then verifies rewrites in separately bounded JVMs. */
object SortedMemoryCalibrationProbe {
    private const val FILES = 8
    private const val ROWS_PER_FILE = 250_000
    private const val ROWS = FILES * ROWS_PER_FILE
    private const val SEED = 553_589_844L
    private const val MIB = 1024L * 1024
    private const val INPUT_BLOCK = MIB
    private const val INPUT_PAGE = 64 * 1024
    private const val APPEND_ROWS = 500_000
    private const val APPEND_PAYLOAD_BYTES = 200
    private val names = listOf("event_id", "distinct_id", "session_id", "path", "country", "browser", "os")
    private val countries = (0 until 60).map { "C" + it.toString().padStart(2, '0') }
    private val browsers = listOf("Chrome", "Safari", "Firefox", "Edge")
    private val systems = listOf("macOS", "Windows", "Linux", "iOS")

    val columns: List<Column> =
        names.mapIndexed { index, name -> Column(index + 1L, index, ColumnDef(name, ColType.STRING)) } +
            listOf(
                Column(8, 7, ColumnDef("ts", ColType.TIMESTAMPTZ)),
                Column(9, 8, ColumnDef("duration_ms", ColType.LONG)),
                Column(10, 9, ColumnDef("viewport_width", ColType.INT)),
            )
    private val schema =
        Types.buildMessage().apply {
            names.forEachIndexed { index, name ->
                addField(
                    Types.optional(
                        PrimitiveTypeName.BINARY,
                    ).`as`(LogicalTypeAnnotation.stringType()).id(index + 1).named(name),
                )
            }
        }.addField(
            Types.optional(PrimitiveTypeName.INT64)
                .`as`(
                    LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS),
                ).id(8).named("ts"),
        ).addField(Types.optional(PrimitiveTypeName.INT64).id(9).named("duration_ms"))
            .addField(Types.optional(PrimitiveTypeName.INT32).id(10).named("viewport_width"))
            .named("pageviews")
    private val sort = listOf(SortFieldDef(8, SortDirection.ASC, NullOrder.NULLS_LAST))

    /** Each row has an independently reproducible payload, regardless of its sorted output position. */
    private class Event(val id: Long) {
        private val random = Random(SEED xor (id * -7046029254386353131L))
        val strings =
            listOf(
                UUID(random.nextLong(), random.nextLong()).toString(),
                "u_" + random.nextInt(100_000).toString(16),
                UUID(SEED, id / 20).toString(),
                "/app/section${random.nextInt(240)}/page?q=${random.nextInt(1_000_000)}",
                countries[random.nextInt(countries.size)],
                browsers[random.nextInt(browsers.size)],
                systems[random.nextInt(systems.size)],
            )
        val timestamp = 1_700_000_000_000_000L + random.nextInt(86_400_000).toLong() * 1000
        val duration = random.nextInt(120_000).toLong()
        val width = 320 + random.nextInt(2240)

        fun group(factory: SimpleGroupFactory): Group =
            factory.newGroup().also { group ->
                strings.forEachIndexed { index, value -> group.add(index, value) }
                group.add(7, timestamp)
                group.add(8, duration)
                group.add(9, width)
            }
    }

    @JvmStatic
    fun main(arguments: Array<String>) {
        val mode = arguments[0]
        val directory = Path.of(arguments[1])
        Files.createDirectories(directory)
        val report =
            when (mode) {
                "generate" -> generate(directory)
                "ordinary" -> ordinary(directory)
                "generate-append" -> generateAppend(directory)
                "append" -> append(directory)
                else -> error("unknown calibration mode: $mode")
            }
        report["mode"] = mode
        report["sourceRevision"] =
            System.getenv("HOGLAKE_CALIBRATION_REVISION") ?: "record checkout identity in the gate manifest"
        ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(directory.resolve("$mode.json").toFile(), report)
        println("CALIBRATION_OK $mode ${ObjectMapper().writeValueAsString(report)}")
    }

    private fun input(
        directory: Path,
        index: Int,
    ): Path = directory.resolve("events-$index.parquet")

    private fun config(directory: Path) =
        CompactionConfig(
            sortedHeapBytes = 1_073_741_824L,
            parallelGroups = 1,
            maxGroupsPerRun = 1,
            targetBytes = 536_870_912L,
            spillDir = directory,
        )

    private fun generate(directory: Path): MutableMap<String, Any> {
        val start = System.nanoTime()
        val cpuStart = cpu()
        val sizes = ArrayList<Long>()
        val hashes = ArrayList<String>()
        val factory = SimpleGroupFactory(schema)
        repeat(FILES) { file ->
            val path = input(directory, file)
            ExampleParquetWriter.builder(LocalOutputFile(path))
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withRowGroupSize(INPUT_BLOCK)
                .withPageSize(INPUT_PAGE)
                .withDictionaryPageSize(INPUT_PAGE)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build().use { writer ->
                    repeat(ROWS_PER_FILE) { index ->
                        writer.write(Event(file.toLong() * ROWS_PER_FILE + index).group(factory))
                    }
                }
            val footer = footer(path)
            check(footer.blocks.sumOf { it.rowCount } == ROWS_PER_FILE.toLong())
            check(Files.size(path) < 536_870_912)
            sizes += Files.size(path)
            hashes += digest(path)
        }
        return linkedMapOf(
            "seed" to SEED,
            "files" to FILES,
            "rowsPerFile" to ROWS_PER_FILE,
            "inputBytes" to sizes.sum(),
            "inputSizes" to sizes,
            "inputSha256" to hashes,
            "codec" to "ZSTD",
            "inputRowGroupTargetBytes" to INPUT_BLOCK,
            "inputPageTargetBytes" to INPUT_PAGE,
            "inputDictionaryTargetBytes" to INPUT_PAGE,
            "maximumHeapBytes" to Runtime.getRuntime().maxMemory(),
            "processCpuNanos" to cpu() - cpuStart,
            "elapsedNanos" to System.nanoTime() - start,
        )
    }

    private fun ordinary(directory: Path): MutableMap<String, Any> {
        val config = config(directory)
        val observations = ArrayList<Map<String, Any>>()
        val spill =
            config.sortSpill(columns).copy(
                memoryObserver = {
                        event,
                        values,
                    ->
                    observations += linkedMapOf<String, Any>("event" to event).apply { putAll(values) }
                },
            )
        val sources = (0 until FILES).map { CountingInput(input(directory, it)) }
        val inputs =
            sources.mapIndexed { index, source ->
                ParquetRewriter.Input(
                    source,
                    "events-$index",
                    index.toLong() * ROWS_PER_FILE,
                    fileSizeBytes = source.length,
                    survivingRecords = ROWS_PER_FILE.toLong(),
                )
            }
        val predicted = ExternalMergeSort.spillRuns(ROWS.toLong(), spill)
        check(predicted == 4L) { "the fixed event group predicts $predicted runs rather than four" }
        val admission = ExternalMergeSort.admit(inputs, spill, 11)
        check(admission.projectedBytes <= 1_073_741_824L)
        val output = directory.resolve("ordinary-output.parquet")
        val peak = HeapPeak()
        val cpuStart = cpu()
        val start = System.nanoTime()
        val result =
            peak.use {
                ParquetRewriter.rewrite(inputs, columns, sort, LocalDiscardableOutput(output), spill = spill)
            }
        val elapsed = System.nanoTime() - start
        val cpu = cpu() - cpuStart
        check(result.runsSpilled == 4) { "the fixed event group spills ${result.runsSpilled} runs rather than four" }
        check(result.rowsWritten == ROWS.toLong())
        check(!result.spillCleanupFailed)
        checkNoSpills(directory)
        check(sources.sumOf { it.opened.get() } == sources.sumOf { it.closed.get() })
        val spills = observations.filter { it["event"] == "spill" }
        check(spills.size == 4) { "the observer reports ${spills.size} spills" }
        val observed = spills.sumOf { (it.getValue("exactRunBytes") as Number).toLong() }
        check(observed <= admission.projectedBytes) {
            "observed merge charge $observed exceeds admission reservation ${admission.projectedBytes}; spills=$spills"
        }
        check(spills.all { (it.getValue("maxRowBytes") as Number).toLong() > 0 })
        val verified = verify(output, ROWS.toLong())
        val outputFooter = footer(output)
        val layout = layout(output)
        check(outputFooter.blocks.isNotEmpty())
        return linkedMapOf(
            "rowsVerified" to verified,
            "runsSpilled" to result.runsSpilled,
            "predictedRuns" to predicted,
            "admissionReservationBytes" to admission.projectedBytes,
            "observedMergeBytes" to observed,
            "observations" to observations,
            "inputBytes" to sources.sumOf { it.length },
            "inputReadBytes" to sources.sumOf { it.bytes.get() },
            "inputReadCalls" to sources.sumOf { it.reads.get() },
            "inputSeekCalls" to sources.sumOf { it.seeks.get() },
            "inputStreams" to sources.sumOf { it.opened.get() },
            "inputPageHeaders" to (0 until FILES).sumOf { pageSummary(input(directory, it)).getValue("headers") },
            "spillBytes" to result.spillBytes,
            "outputBytes" to Files.size(output),
            "outputLayout" to layout,
            "outputPageSummary" to pageSummary(output),
            "elapsedNanos" to elapsed,
            "processCpuNanos" to cpu,
            "sampledPeakHeapBytes" to peak.bytes.get(),
            "maximumHeapBytes" to Runtime.getRuntime().maxMemory(),
        )
    }

    private val appendColumns =
        listOf(
            Column(1, 0, ColumnDef("key", ColType.LONG)),
            Column(2, 1, ColumnDef("payload", ColType.BINARY)),
        )
    private val appendSchema =
        Types.buildMessage()
            .addField(Types.required(PrimitiveTypeName.INT64).id(1).named("key"))
            .addField(Types.required(PrimitiveTypeName.BINARY).id(2).named("payload"))
            .named("append_events")
    private val appendSort = listOf(SortFieldDef(1, SortDirection.ASC, NullOrder.NULLS_LAST))

    private fun appendPayload(id: Long): ByteArray =
        ByteArray(APPEND_PAYLOAD_BYTES).also {
            Random(SEED xor (id * -7046029254386353131L)).nextBytes(it)
        }

    private fun generateAppend(directory: Path): MutableMap<String, Any> {
        val factory = SimpleGroupFactory(appendSchema)
        val sizes = ArrayList<Long>()
        repeat(2) { file ->
            val path = directory.resolve("append-client-$file.parquet")
            ExampleParquetWriter.builder(LocalOutputFile(path))
                .withType(appendSchema)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withDictionaryEncoding(false)
                .withRowGroupSize(INPUT_BLOCK)
                .withPageSize(INPUT_PAGE)
                .build().use { writer ->
                    repeat(APPEND_ROWS) { row ->
                        val id = file.toLong() * APPEND_ROWS + row
                        writer.write(
                            factory.newGroup().apply {
                                add(0, id)
                                add(1, Binary.fromConstantByteArray(appendPayload(id)))
                            },
                        )
                    }
                }
            sizes += Files.size(path)
        }
        return linkedMapOf(
            "rowsPerFile" to APPEND_ROWS,
            "payloadBytes" to APPEND_PAYLOAD_BYTES,
            "seed" to SEED,
            "inputSizes" to sizes,
            "maximumHeapBytes" to Runtime.getRuntime().maxMemory(),
        )
    }

    /** Two genuine prior outputs retain the production append floor. */
    private fun append(directory: Path): MutableMap<String, Any> {
        val priors =
            (0 until 2).map { file ->
                val client = directory.resolve("append-client-$file.parquet")
                val prior = directory.resolve("append-prior-$file.parquet")
                val result =
                    ParquetRewriter.rewrite(
                        listOf(
                            localInput(
                                client,
                                file.toLong() * APPEND_ROWS,
                            ).copy(survivingRecords = APPEND_ROWS.toLong()),
                        ),
                        appendColumns,
                        appendSort,
                        LocalDiscardableOutput(prior),
                        spill = config(directory).sortSpill(appendColumns),
                    )
                check(result.rowsWritten == APPEND_ROWS.toLong())
                val footer = footer(prior)
                check(footer.blocks.isNotEmpty())
                check(
                    footer.blocks.all { it.compressedSize >= 32 * MIB },
                ) { "the append fixture has an ineligible group" }
                prior
            }
        val inputs =
            priors.map { prior ->
                localInput(prior, 0, explicitRowIds = true, trustedSorted = true)
                    .copy(survivingRecords = APPEND_ROWS.toLong())
            }
        inputs.forEachIndexed { index, input ->
            check(
                ParquetRewriter.appendRefusal(
                    input,
                    footer(priors[index]),
                    ParquetRewriter.outputSchema(appendColumns),
                    33_554_432L,
                ) == null,
            )
        }
        val observations = ArrayList<Map<String, Any>>()
        val spill =
            config(directory).sortSpill(appendColumns).copy(
                memoryObserver = {
                        event,
                        values,
                    ->
                    observations += linkedMapOf<String, Any>("event" to event).apply { putAll(values) }
                },
            )
        val output = directory.resolve("append-output.parquet")
        val peak = HeapPeak()
        val start = System.nanoTime()
        val cpuStart = cpu()
        val result =
            peak.use {
                ParquetRewriter.rewrite(
                    inputs,
                    appendColumns,
                    appendSort,
                    LocalDiscardableOutput(output),
                    spill = spill,
                )
            }
        val elapsed = System.nanoTime() - start
        val processCpu = cpu() - cpuStart
        val expectedGroups = priors.sumOf { footer(it).blocks.size }
        check(expectedGroups >= 2)
        check(result.rowGroupsAppended == expectedGroups)
        check(result.bytesAppended >= 64 * MIB)
        check(result.runsSpilled == 0)
        check(result.rowsWritten == 2L * APPEND_ROWS)
        val merge = observations.single { it["event"] == "merge" }
        check((merge.getValue("decodedReaders") as Number).toLong() == 0L)
        check((merge.getValue("decodedRows") as Number).toLong() == 0L)
        val identical = priors.flatMap(::columnDigests) == columnDigests(output)
        check(identical) { "byte append changes a compressed column chunk" }
        val verified = verifyAppend(output)
        val deletion = PuffinDeletionVector.read(PuffinTestFiles.deletionVector(listOf(0L)))
        check(
            ParquetRewriter.appendRefusal(
                inputs.first().copy(deletes = deletion),
                footer(priors.first()),
                ParquetRewriter.outputSchema(appendColumns),
                33_554_432L,
            ) != null,
        )
        checkNoSpills(directory)
        return linkedMapOf(
            "rowsVerified" to verified,
            "rowGroupsAppended" to result.rowGroupsAppended,
            "bytesAppended" to result.bytesAppended,
            "chunkBytesIdentical" to identical,
            "decodedReaders" to (merge.getValue("decodedReaders") as Number).toLong(),
            "decodedRows" to (merge.getValue("decodedRows") as Number).toLong(),
            "outputLayout" to layout(output),
            "outputPageSummary" to pageSummary(output),
            "observations" to observations,
            "elapsedNanos" to elapsed,
            "processCpuNanos" to processCpu,
            "sampledPeakHeapBytes" to peak.bytes.get(),
            "appendFloorBytes" to 33_554_432L,
        )
    }

    private fun verifyAppend(path: Path): Long {
        var count = 0L
        ParquetFileReader.open(LocalInputFile(path)).use { reader ->
            val schema = reader.footer.fileMetaData.schema
            val io = ColumnIOFactory().getColumnIO(schema)
            val idColumn = schema.getFieldIndex(ParquetRewriter.ROW_ID_COLUMN)
            while (true) {
                val pages = reader.readNextRowGroup() ?: break
                pages.use {
                    val records = io.getRecordReader(pages, GroupRecordConverter(schema))
                    repeat(Math.toIntExact(pages.rowCount)) {
                        val row = records.read()
                        check(row.getLong(0, 0) == count)
                        check(row.getLong(idColumn, 0) == count)
                        check(row.getBinary(1, 0).bytes.contentEquals(appendPayload(count)))
                        count++
                    }
                }
            }
        }
        check(count == 2L * APPEND_ROWS)
        return count
    }

    /** Checks every payload against its row ID without retaining decoded rows. */
    private fun verify(
        path: Path,
        expectedRows: Long,
    ): Long {
        val seen = BitSet(ROWS)
        var count = 0L
        var previousTimestamp = Long.MIN_VALUE
        var previousId = Long.MIN_VALUE
        ParquetFileReader.open(LocalInputFile(path)).use { reader ->
            val outputSchema = reader.footer.fileMetaData.schema
            val columnIO = ColumnIOFactory().getColumnIO(outputSchema)
            val idColumn = outputSchema.getFieldIndex(ParquetRewriter.ROW_ID_COLUMN)
            while (true) {
                val pages = reader.readNextRowGroup() ?: break
                pages.use {
                    val records = columnIO.getRecordReader(pages, GroupRecordConverter(outputSchema))
                    repeat(Math.toIntExact(pages.rowCount)) {
                        val row = records.read()
                        val id = row.getLong(idColumn, 0)
                        check(id in 0 until ROWS.toLong())
                        check(!seen[id.toInt()]) { "duplicate output row ID $id" }
                        seen.set(id.toInt())
                        val expected = Event(id)
                        expected.strings.forEachIndexed {
                                index,
                                value,
                            ->
                            check(row.getString(index, 0) == value) { "payload mismatch at row $id, field $index" }
                        }
                        check(row.getLong(7, 0) == expected.timestamp)
                        check(row.getLong(8, 0) == expected.duration)
                        check(row.getInteger(9, 0) == expected.width)
                        check(
                            expected.timestamp > previousTimestamp ||
                                (expected.timestamp == previousTimestamp && id > previousId),
                        ) {
                            "independent timestamp and row-ID order fails at $id"
                        }
                        previousTimestamp = expected.timestamp
                        previousId = id
                        count++
                    }
                }
            }
        }
        check(count == expectedRows)
        check(seen.cardinality().toLong() == expectedRows)
        if (expectedRows == ROWS.toLong()) check(seen.nextClearBit(0) == ROWS)
        return count
    }

    private fun footer(path: Path): ParquetMetadata = ParquetFileReader.open(LocalInputFile(path)).use { it.footer }

    private fun layout(path: Path): List<Map<String, Any>> =
        footer(path).blocks.map { block ->
            linkedMapOf(
                "rows" to block.rowCount,
                "compressedBytes" to block.compressedSize,
                "uncompressedBytes" to block.totalByteSize,
            )
        }

    private fun pageSummary(path: Path): Map<String, Long> {
        val footer = footer(path)
        var headers = 0L
        var maximumCompressed = 0L
        var maximumUncompressed = 0L
        LocalInputFile(path).newStream().use { stream ->
            for (block in footer.blocks) {
                for (column in block.columns) {
                    stream.seek(column.startingPos)
                    val end = column.startingPos + column.totalSize
                    while (stream.pos < end) {
                        val header = Util.readPageHeader(stream)
                        headers++
                        maximumCompressed = maxOf(maximumCompressed, header.compressed_page_size.toLong())
                        maximumUncompressed = maxOf(maximumUncompressed, header.uncompressed_page_size.toLong())
                        stream.seek(stream.pos + header.compressed_page_size)
                    }
                    check(stream.pos == end)
                }
            }
        }
        return linkedMapOf(
            "headers" to headers,
            "maximumCompressedPageBytes" to maximumCompressed,
            "maximumUncompressedPageBytes" to maximumUncompressed,
        )
    }

    private fun columnDigests(path: Path): List<String> {
        val footer = footer(path)
        return LocalInputFile(path).newStream().use { stream ->
            footer.blocks.flatMap { block ->
                block.columns.map { column ->
                    stream.seek(column.startingPos)
                    digestBytes(column.totalSize) { buffer, size -> stream.readFully(buffer, 0, size) }
                }
            }
        }
    }

    private fun digest(path: Path): String =
        Files.newInputStream(path).use { stream ->
            digestBytes(Files.size(path)) { buffer, size -> check(stream.readNBytes(buffer, 0, size) == size) }
        }

    private fun digestBytes(
        size: Long,
        read: (ByteArray, Int) -> Unit,
    ): String {
        val digest = MessageDigest.getInstance("SHA-256")
        val buffer = ByteArray(64 * 1024)
        var remaining = size
        while (remaining > 0) {
            val count = minOf(remaining, buffer.size.toLong()).toInt()
            read(buffer, count)
            digest.update(buffer, 0, count)
            remaining -= count
        }
        return digest.digest().joinToString("") { "%02x".format(it.toInt() and 255) }
    }

    private fun cpu(): Long = (ManagementFactory.getOperatingSystemMXBean() as OperatingSystemMXBean).processCpuTime

    private fun checkNoSpills(directory: Path) {
        Files.list(directory).use {
                paths ->
            check(paths.noneMatch { it.fileName.toString().startsWith(SpillDirectory.PREFIX) })
        }
    }

    private class HeapPeak : AutoCloseable {
        val bytes = AtomicLong()
        private val sampler =
            Executors.newSingleThreadScheduledExecutor {
                    task ->
                Thread(task, "calibration-heap").apply { isDaemon = true }
            }

        init {
            sampler.scheduleAtFixedRate({
                bytes.accumulateAndGet(ManagementFactory.getMemoryMXBean().heapMemoryUsage.used, ::maxOf)
            }, 0, 20, TimeUnit.MILLISECONDS)
        }

        override fun close() {
            sampler.shutdownNow()
            sampler.awaitTermination(10, TimeUnit.SECONDS)
        }
    }

    private class CountingInput(path: Path) : InputFile {
        private val delegate = LocalInputFile(path)
        val bytes = AtomicLong()
        val reads = AtomicLong()
        val seeks = AtomicLong()
        val opened = AtomicLong()
        val closed = AtomicLong()

        override fun getLength(): Long = delegate.length

        override fun newStream(): SeekableInputStream {
            val source = delegate.newStream()
            opened.incrementAndGet()
            return object : SeekableInputStream() {
                override fun getPos(): Long = source.pos

                override fun seek(position: Long) {
                    seeks.incrementAndGet()
                    source.seek(position)
                }

                override fun read(): Int =
                    source.read().also {
                        reads.incrementAndGet()
                        if (it >= 0) bytes.incrementAndGet()
                    }

                override fun read(
                    buffer: ByteArray,
                    offset: Int,
                    length: Int,
                ): Int =
                    source.read(
                        buffer,
                        offset,
                        length,
                    ).also {
                        reads.incrementAndGet()
                        if (it > 0) bytes.addAndGet(it.toLong())
                    }

                override fun readFully(buffer: ByteArray) = readFully(buffer, 0, buffer.size)

                override fun readFully(
                    buffer: ByteArray,
                    offset: Int,
                    length: Int,
                ) {
                    source.readFully(buffer, offset, length)
                    reads.incrementAndGet()
                    bytes.addAndGet(length.toLong())
                }

                override fun read(buffer: ByteBuffer): Int =
                    source.read(buffer).also {
                        reads.incrementAndGet()
                        if (it > 0) bytes.addAndGet(it.toLong())
                    }

                override fun readFully(buffer: ByteBuffer) {
                    val size = buffer.remaining()
                    source.readFully(buffer)
                    reads.incrementAndGet()
                    bytes.addAndGet(size.toLong())
                }

                override fun close() {
                    source.close()
                    closed.incrementAndGet()
                }
            }
        }
    }
}
