package com.posthog.hoglake.compaction

import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import com.sun.management.OperatingSystemMXBean
import org.apache.parquet.column.Encoding
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.format.PageType
import org.apache.parquet.format.Util
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import java.lang.management.ManagementFactory
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.util.BitSet
import java.util.Random
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/** Separates bounded generation from complete sorted-pipeline acceptance. */
object SortedReaderPipelineCalibrationProbe {
    private const val TARGET = 536_870_912L
    private const val BUDGET = 1_073_741_824L
    private const val SEED = 553_589_844L
    private const val STEP = -7046029254386353131L
    private const val SPARSE_MODULO = 1999
    private const val PAGE = 1_048_576
    private const val CARDINALITY = 60_000
    private val dictionary = Array(CARDINALITY) { it.toString(16).padStart(8, '0') }
    private val eventNames = listOf("event_id", "distinct_id", "session_id", "path", "country", "browser", "os")
    private val countries = (0 until 60).map { "C" + it.toString().padStart(2, '0') }
    private val browsers = listOf("Chrome", "Safari", "Firefox", "Edge")
    private val systems = listOf("macOS", "Windows", "Linux", "iOS")

    private data class Fixture(
        val name: String,
        val rows: Int,
        val columns: List<Column>,
        val schema: MessageType,
        val keyIndex: Int,
        val codec: CompressionCodecName,
    )

    private fun fixture(name: String): Fixture {
        val columns =
            when (name) {
                "near" ->
                    (0 until 10).map { Column(it + 1L, it, ColumnDef("s$it", ColType.STRING)) } +
                        listOf(
                            Column(11, 10, ColumnDef("payload", ColType.BINARY)),
                            Column(12, 11, ColumnDef("key", ColType.LONG)),
                        )
                "sparse" ->
                    listOf(Column(1, 0, ColumnDef("key", ColType.LONG))) +
                        (1..150).map {
                            val type = if (it % 2 == 0) ColType.STRING else ColType.BINARY
                            Column(it + 1L, it, ColumnDef("c$it", type))
                        }
                "event" -> SortedMemoryCalibrationProbe.columns
                else -> error("unknown fixture $name")
            }
        val schema =
            Types.buildMessage().apply {
                columns.forEach { column ->
                    val field =
                        when (column.def.type) {
                            ColType.STRING ->
                                Types.optional(
                                    PrimitiveTypeName.BINARY,
                                ).`as`(LogicalTypeAnnotation.stringType())
                            ColType.BINARY -> Types.optional(PrimitiveTypeName.BINARY)
                            ColType.INT -> Types.optional(PrimitiveTypeName.INT32)
                            ColType.TIMESTAMPTZ ->
                                Types.optional(PrimitiveTypeName.INT64).`as`(
                                    LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS),
                                )
                            else -> Types.optional(PrimitiveTypeName.INT64)
                        }
                    addField(field.id(column.fieldId.toInt()).named(column.def.name))
                }
            }.named("pageviews")
        return Fixture(
            name,
            when (name) {
                "near" -> 2_000_000
                "sparse" -> 200_000
                else -> 400_000
            },
            columns,
            schema,
            when (name) {
                "near" -> 11
                "sparse" -> 0
                else -> 7
            },
            if (name == "near") CompressionCodecName.SNAPPY else CompressionCodecName.ZSTD,
        )
    }

    @JvmStatic
    fun main(arguments: Array<String>) {
        val mode = arguments[0]
        val spec = fixture(mode.substringAfter('-'))
        val directory = Path.of(arguments[1])
        Files.createDirectories(directory)
        val report = if (mode.startsWith("generate-")) generate(spec, directory) else rewrite(spec, directory)
        report["mode"] = mode
        ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(directory.resolve("$mode.json").toFile(), report)
        println("READER_PIPELINE_OK $mode ${ObjectMapper().writeValueAsString(report)}")
    }

    private fun random(id: Long) = Random(SEED xor (id * STEP))

    private fun row(
        spec: Fixture,
        id: Long,
        factory: SimpleGroupFactory,
    ): Group {
        val group = factory.newGroup()
        val random = random(id)
        when (spec.name) {
            "near" -> {
                val pair = id / 2
                val categories = random(pair)
                repeat(10) { column ->
                    val value =
                        if (pair < CARDINALITY) {
                            ((pair * 7919 + column * 3931) % CARDINALITY).toInt()
                        } else {
                            categories.nextInt(CARDINALITY)
                        }
                    group.add(column, dictionary[value])
                }
                group.add(10, Binary.fromConstantByteArray(ByteArray(224).also(random::nextBytes)))
                group.add(11, spec.rows - id)
            }
            "sparse" -> {
                group.add(0, spec.rows - id)
                for (column in 1..150) if (populated(
                        id,
                        column,
                    )
                ) {
                    group.add(column, Binary.fromConstantByteArray(sparseValue(id, column)))
                }
            }
            else -> {
                eventStrings(id, random).forEachIndexed { index, value -> group.add(index, value) }
                group.add(7, 1_700_000_000_000_000L + random.nextInt(86_400_000).toLong() * 1000)
                group.add(8, random.nextInt(120_000).toLong())
                group.add(9, 320 + random.nextInt(2240))
            }
        }
        return group
    }

    private fun eventStrings(
        id: Long,
        random: Random,
    ): List<String> =
        listOf(
            UUID(random.nextLong(), random.nextLong()).toString(),
            "u_" + random.nextInt(100_000).toString(16),
            UUID(SEED, id / 20).toString(),
            "/app/section${random.nextInt(240)}/page?q=${random.nextInt(1_000_000)}",
            countries[random.nextInt(countries.size)],
            browsers[random.nextInt(browsers.size)],
            systems[random.nextInt(systems.size)],
        )

    private fun populated(
        id: Long,
        column: Int,
    ): Boolean = (id + column.toLong() * 37) % SPARSE_MODULO == 0L

    private fun sparseValue(
        id: Long,
        column: Int,
    ): ByteArray = "value-$column-$id".toByteArray(Charsets.UTF_8)

    private fun generate(
        spec: Fixture,
        directory: Path,
    ): MutableMap<String, Any> {
        val path = directory.resolve("${spec.name}-input.parquet")
        val factory = SimpleGroupFactory(spec.schema)
        val start = System.nanoTime()
        ExampleParquetWriter.builder(LocalOutputFile(path))
            .withType(spec.schema)
            .withCompressionCodec(spec.codec)
            .withRowGroupSize(2L shl 30)
            .withPageSize(PAGE)
            .withDictionaryPageSize(PAGE)
            .withDictionaryEncoding(true)
            .build().use { writer -> repeat(spec.rows) { writer.write(row(spec, it.toLong(), factory)) } }
        val footer = ParquetFileReader.open(LocalInputFile(path)).use { it.footer }
        check(footer.blocks.size == 1)
        check(footer.blocks.single().rowCount == spec.rows.toLong())
        if (spec.name == "near") {
            val ratio = Files.size(path).toDouble() / TARGET
            check(ratio in 0.90..0.98) { "near-target compressed ratio $ratio leaves the fixed calibration range" }
            check(Files.size(path) < TARGET)
        }
        return linkedMapOf<String, Any>(
            "rows" to spec.rows,
            "inputBytes" to Files.size(path),
            "inputRowGroups" to footer.blocks.size,
            "codec" to spec.codec.name,
            "pageTargetBytes" to PAGE,
            "dictionaryTargetBytes" to PAGE,
            "seed" to SEED,
            "elapsedNanos" to System.nanoTime() - start,
            "maximumHeapBytes" to Runtime.getRuntime().maxMemory(),
        ).apply {
            if (spec.name == "near") put("categoryDictionaries", categoryDictionaries(path, footer))
        }
    }

    private fun categoryDictionaries(
        path: Path,
        footer: ParquetMetadata,
    ): List<Map<String, Any>> {
        val columns = footer.blocks.single().columns.take(10)
        check(columns.size == 10)
        return LocalInputFile(path).newStream().use { stream ->
            columns.mapIndexed { index, column ->
                check(column.path.toDotString() == "s$index")
                check(column.encodings.any { it == Encoding.RLE_DICTIONARY || it == Encoding.PLAIN_DICTIONARY }) {
                    "category s$index does not use dictionary encoding"
                }
                check(column.dictionaryPageOffset >= column.startingPos)
                check(column.dictionaryPageOffset < column.startingPos + column.totalSize)
                stream.seek(column.dictionaryPageOffset)
                val header = Util.readPageHeader(stream)
                check(header.type == PageType.DICTIONARY_PAGE)
                val entries = header.dictionary_page_header.num_values
                stream.seek(column.firstDataPageOffset)
                val dataHeader = Util.readPageHeader(stream)
                val dataEncoding =
                    if (dataHeader.type == PageType.DATA_PAGE) {
                        dataHeader.data_page_header.encoding
                    } else {
                        check(dataHeader.type == PageType.DATA_PAGE_V2)
                        dataHeader.data_page_header_v2.encoding
                    }
                check(dataEncoding.name in setOf("RLE_DICTIONARY", "PLAIN_DICTIONARY"))
                check(entries == 60_000) { "category s$index has $entries dictionary entries rather than 60000" }
                linkedMapOf<String, Any>(
                    "column" to column.path.toDotString(),
                    "encodings" to column.encodings.map { it.name }.sorted(),
                    "dictionaryOffset" to column.dictionaryPageOffset,
                    "dictionaryEntries" to entries,
                    "firstDataEncoding" to dataEncoding.name,
                    "compressedDictionaryBytes" to header.compressed_page_size,
                    "uncompressedDictionaryBytes" to header.uncompressed_page_size,
                )
            }
        }
    }

    private fun rewrite(
        spec: Fixture,
        directory: Path,
    ): MutableMap<String, Any> {
        val path = directory.resolve("${spec.name}-input.parquet")
        val output = directory.resolve("${spec.name}-output.parquet")
        val config =
            CompactionConfig(
                targetBytes = TARGET,
                maxGroupsPerRun = 1,
                sortedHeapBytes = BUDGET,
                parallelGroups = 1,
                spillDir = directory,
            )
        val observations = ArrayList<Map<String, Any>>()
        val spill =
            config.sortSpill(spec.columns).copy(memoryObserver = { event, metrics ->
                observations += linkedMapOf<String, Any>("event" to event).apply { putAll(metrics) }
            })
        val footer = ParquetFileReader.open(LocalInputFile(path)).use { it.footer }
        check(footer.blocks.size == 1)
        if (spec.name == "near") categoryDictionaries(path, footer)
        val input = localInput(path, 0).copy(survivingRecords = spec.rows.toLong())
        val key = spec.columns[spec.keyIndex].fieldId
        val peak = HeapPeak()
        val start = System.nanoTime()
        val cpuStart = cpu()
        val result =
            peak.use {
                ParquetRewriter.rewrite(
                    listOf(input),
                    spec.columns,
                    listOf(SortFieldDef(key, SortDirection.ASC, NullOrder.NULLS_LAST)),
                    LocalDiscardableOutput(output),
                    spill = spill,
                )
            }
        val elapsed = System.nanoTime() - start
        val cpu = cpu() - cpuStart
        check(result.rowsWritten == spec.rows.toLong())
        check(!result.spillCleanupFailed)
        if (spec.name == "sparse") {
            check(result.runsSpilled == 6) {
                "sparse schema spills ${result.runsSpilled} runs rather than six"
            }
        }
        val reader = observations.filter { it["event"] == "reader" }
        check(reader.isNotEmpty()) { "the reader observer does not report its reservation" }
        val excess = reader.maxOf { ((it["readerExcessBytes"] ?: it.getValue("extraBytes")) as Number).toLong() }
        if (spec.name == "near") {
            check(
                excess > 0,
            ) { "the near-target fixture does not exercise positive reader excess" }
        }
        val verification = verify(spec, output)
        Files.list(directory).use {
                files ->
            check(files.noneMatch { it.fileName.toString().startsWith(SpillDirectory.PREFIX) })
        }
        return linkedMapOf<String, Any>(
            "rowsVerified" to result.rowsWritten,
            "runsSpilled" to result.runsSpilled,
            "spillBytes" to result.spillBytes,
            "inputBytes" to Files.size(path),
            "inputRowGroups" to footer.blocks.size,
            "compressedTargetRatio" to Files.size(path).toDouble() / TARGET,
            "readerExcessBytes" to excess,
            "observations" to observations,
            "elapsedNanos" to elapsed,
            "processCpuNanos" to cpu,
            "sampledPeakHeapBytes" to peak.bytes.get(),
            "maximumHeapBytes" to Runtime.getRuntime().maxMemory(),
            "pageTargetBytes" to PAGE,
            "dictionaryTargetBytes" to PAGE,
            "sparseModulo" to SPARSE_MODULO,
            "optionalFields" to 150,
            "outputLayout" to
                result.footer!!.blocks.map {
                    mapOf(
                        "rows" to it.rowCount,
                        "compressedBytes" to it.compressedSize,
                        "uncompressedBytes" to it.totalByteSize,
                    )
                },
        ).apply { putAll(verification) }
    }

    private fun verify(
        spec: Fixture,
        output: Path,
    ): Map<String, Any> {
        val actual = MessageDigest.getInstance("SHA-256")
        val expected = MessageDigest.getInstance("SHA-256")
        val seen = BitSet(spec.rows)
        val factory = SimpleGroupFactory(spec.schema)
        var count = 0L
        var previousKey = Long.MIN_VALUE
        var previousId = Long.MIN_VALUE
        var nonNull = 0L
        ParquetFileReader.open(LocalInputFile(output)).use { reader ->
            val schema = reader.footer.fileMetaData.schema
            check(schema.fieldCount == spec.columns.size + 1)
            val idIndex = schema.getFieldIndex(ParquetRewriter.ROW_ID_COLUMN)
            val io = ColumnIOFactory().getColumnIO(schema)
            while (true) {
                val pages = reader.readNextRowGroup() ?: break
                pages.use {
                    val records = io.getRecordReader(pages, GroupRecordConverter(schema))
                    repeat(Math.toIntExact(pages.rowCount)) {
                        val value = records.read()
                        val id = value.getLong(idIndex, 0)
                        check(id in 0 until spec.rows.toLong())
                        check(!seen[id.toInt()])
                        seen.set(id.toInt())
                        val key = value.getLong(spec.keyIndex, 0)
                        check(key > previousKey || (key == previousKey && id > previousId))
                        if (spec.name != "event") check(id == spec.rows - count - 1)
                        previousKey = key
                        previousId = id
                        val reference = row(spec, id, factory)
                        nonNull += digestRow(actual, value, spec)
                        digestRow(expected, reference, spec)
                        count++
                    }
                }
            }
        }
        check(count == spec.rows.toLong())
        check(seen.cardinality() == spec.rows)
        val actualHex = hex(actual.digest())
        val expectedHex = hex(expected.digest())
        check(actualHex == expectedHex) { "streamed payload digests disagree" }
        return mapOf(
            "payloadDigest" to actualHex,
            "expectedPayloadDigest" to expectedHex,
            "nonNullOptionalValues" to nonNull,
        )
    }

    private fun digestRow(
        digest: MessageDigest,
        row: Group,
        spec: Fixture,
    ): Long {
        var nonNull = 0L
        spec.columns.forEachIndexed { index, column ->
            val present = row.getFieldRepetitionCount(index) != 0
            digest.update(if (present) 1.toByte() else 0.toByte())
            if (present) {
                if (index != 0) nonNull++
                when (column.def.type) {
                    ColType.STRING, ColType.BINARY -> {
                        val bytes = row.getBinary(index, 0).bytes
                        digestLong(digest, bytes.size.toLong())
                        digest.update(bytes)
                    }
                    ColType.INT -> digestLong(digest, row.getInteger(index, 0).toLong())
                    else -> digestLong(digest, row.getLong(index, 0))
                }
            }
        }
        return nonNull
    }

    private fun digestLong(
        digest: MessageDigest,
        value: Long,
    ) {
        for (shift in 0..56 step 8) digest.update((value ushr shift).toByte())
    }

    private fun hex(bytes: ByteArray): String = bytes.joinToString("") { "%02x".format(it.toInt() and 255) }

    private fun cpu(): Long = (ManagementFactory.getOperatingSystemMXBean() as OperatingSystemMXBean).processCpuTime

    private class HeapPeak : AutoCloseable {
        val bytes = AtomicLong()
        private val sampler =
            Executors.newSingleThreadScheduledExecutor { task ->
                Thread(task, "reader-calibration-heap").apply { isDaemon = true }
            }

        init {
            sampler.scheduleAtFixedRate(
                { bytes.accumulateAndGet(ManagementFactory.getMemoryMXBean().heapMemoryUsage.used, ::maxOf) },
                0,
                20,
                TimeUnit.MILLISECONDS,
            )
        }

        override fun close() {
            sampler.shutdownNow()
            sampler.awaitTermination(10, TimeUnit.SECONDS)
        }
    }
}
