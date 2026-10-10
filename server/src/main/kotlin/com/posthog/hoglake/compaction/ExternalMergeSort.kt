package com.posthog.hoglake.compaction

import com.posthog.hoglake.ParquetReaders
import com.posthog.hoglake.compaction.ParquetRewriter.Input
import com.posthog.hoglake.compaction.ParquetRewriter.OutputCodec
import com.posthog.hoglake.compaction.ParquetRewriter.OutputShape
import com.posthog.hoglake.compaction.ParquetRewriter.RewriteResult
import com.posthog.hoglake.compaction.ParquetRewriter.Row
import com.posthog.hoglake.compaction.ParquetRewriter.SortKeys
import com.posthog.hoglake.compaction.ParquetRewriter.SurvivorReader
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.io.OutputFile
import org.apache.parquet.io.PositionOutputStream
import org.apache.parquet.schema.MessageType
import java.nio.file.Files
import java.nio.file.Path
import java.util.PriorityQueue
import java.util.UUID

/**
 * A sorted group whose spill would write more local bytes than
 * [SortSpill.spillBudgetBytes] allows: refused from the registered input
 * sizes before any IO, or stopped the moment the bytes actually written
 * cross the budget.
 *
 * Typed because the bound is the only thing between a large group and
 * a pod EVICTION: the spill directory is an emptyDir, an overrun of its
 * `sizeLimit` is not an IOException but a kubelet kill of the whole
 * process, and `FileStore.usableSpace` reports the node's disk rather
 * than the limit. A refusal is a configuration signal (spill budget
 * against group size), never a writer's fault, so it must not land in
 * `invalid_data` or `failed_groups`.
 *
 * It carries the WORK already spent, because a mid-run stop is exactly
 * the group that spilled the most and its caller has no result to read
 * the counters from. [SortedRewrite.run] rebuilds every escaping refusal
 * with the rewrite's authoritative counters.
 */
class SpillBudgetExceededException(
    message: String,
    /** Local bytes already spilled when the rewrite stopped; 0 for the refusal from registered sizes. */
    val spillBytes: Long,
    /** Spill files completed when it stopped. */
    val runsSpilled: Int,
    /** Trusted inputs demoted to the spill path before it stopped. */
    val runsDemoted: Int,
    /** The sortedness pre-pass's verdicts and bytes, spent before the stop (see [ParquetRewriter.RewriteResult]). */
    val filesVerified: Int = 0,
    val filesUnsorted: Int = 0,
    val filesUnchecked: Int = 0,
    val sortCheckBytes: Long = 0,
) : RuntimeException(message)

/**
 * A sorted group whose merge would buffer more heap than
 * [SortSpill.mergeBudgetBytes] allows even with every trusted input
 * demoted to the spill path: too many spilled runs, each of which holds
 * a whole row group while it is read. Refused before any data is read —
 * from the registered survivor counts — or at the spill whose exact cost
 * makes the runs exceed it.
 *
 * Reachable only by configuration (a heap budget small against the byte
 * target), and there is deliberately no multi-pass merge to absorb it:
 * that would double the spill disk and add a second code path for a
 * misconfiguration.
 */
class MergeBudgetExceededException(
    message: String,
    /** See [SpillBudgetExceededException.spillBytes]: the work spent before the stop. */
    val spillBytes: Long,
    val runsSpilled: Int,
    val runsDemoted: Int,
    val filesVerified: Int = 0,
    val filesUnsorted: Int = 0,
    val filesUnchecked: Int = 0,
    val sortCheckBytes: Long = 0,
) : RuntimeException(message)

/**
 * What admission reads off an input: catalog facts only, no bytes.
 * [ParquetRewriter.Input] is one; so is the planner's view of a
 * candidate file, which is how `CompactionService` refuses a group in
 * metadata with the SAME arithmetic the rewriter applies (hoglake#134)
 * rather than a restatement of it.
 */
interface RunCandidate {
    /** Read as an already-sorted run in place (see [ParquetRewriter.Input.trustedSorted]). */
    val trustedSorted: Boolean

    /** Registered size; 0 = unknown. */
    val fileSizeBytes: Long

    /** Registered survivors (record_count minus the live DV's count); 0 = unknown. */
    val survivingRecords: Long
}

/**
 * The bounds of one sorted rewrite (see [ParquetRewriter]'s class doc).
 * Required for any non-empty sort order.
 */
data class SortSpill(
    /** Rows per chunk: the chunk phase's heap bound, in the unit the heap holds. */
    val chunkRows: Long,
    /** Heap the merge may hold across all open runs. */
    val mergeBudgetBytes: Long,
    /** Local bytes one rewrite may spill. */
    val spillBudgetBytes: Long,
    /** Parent of the per-rewrite `hoglake-compaction-spill-<uuid>` directory. */
    val spillDir: Path,
    /** Row-group size spill files are written with: a spilled run's predicted compressed cost. */
    val spillBlockBytes: Int = SPILL_BLOCK_BYTES,
    /** Per-trusted-run stream buffer charged on top of its row group. */
    val readaheadBytes: Int = S3InputFile.DEFAULT_READAHEAD_BYTES,
    /**
     * The smallest registered size the sortedness pre-pass checks; a
     * smaller input (or one of unknown size) goes straight to the chunk
     * phase, counted `files_unchecked`. 0 checks every input of known
     * size. See CompactionConfig.verifyMinBytes for the default's reason.
     */
    val verifyMinBytes: Long = SPILL_BLOCK_BYTES.toLong(),
) {
    init {
        require(chunkRows >= 1) { "chunkRows must be at least 1, got $chunkRows" }
        require(mergeBudgetBytes >= 1) { "mergeBudgetBytes must be at least 1, got $mergeBudgetBytes" }
        require(spillBudgetBytes >= 1) { "spillBudgetBytes must be at least 1, got $spillBudgetBytes" }
        require(spillBlockBytes >= 1) { "spillBlockBytes must be at least 1, got $spillBlockBytes" }
        require(readaheadBytes >= 0) { "readaheadBytes must not be negative, got $readaheadBytes" }
        require(verifyMinBytes >= 0) { "verifyMinBytes must not be negative, got $verifyMinBytes" }
    }

    companion object {
        /**
         * 16 MiB. Spill files are written at this row-group size so
         * reading one back holds about this much compressed: small enough
         * that a merge of dozens of spilled runs fits a 1 GiB group budget,
         * large enough that a column chunk is still one sequential read.
         */
        const val SPILL_BLOCK_BYTES: Int = 16 * 1024 * 1024
    }
}

/**
 * The sorted rewrite's external merge sort: admission, the chunk phase,
 * the spill files, the merge, and the arithmetic that bounds them. The
 * design is in [ParquetRewriter]'s class doc; this object is called only
 * by [ParquetRewriter.rewrite], which owns the per-input pipeline and the
 * output writer it reuses.
 *
 * # What a run costs while it is merged
 *
 * Reading a run holds, at once: the row group's COMPRESSED column chunks
 * (`readNextRowGroup()` allocates all of them), one DECOMPRESSED data
 * page plus the decoded dictionary page per leaf column (each column
 * reader decodes one page at a time), and the stream's buffer. So:
 *
 *  - from a footer (exact): `max over row groups of (compressedSize +
 *    Σ leaves min(uncompressed chunk size, per-leaf page cap))` + buffer.
 *    A page is part of its chunk, so a column whose whole chunk is
 *    smaller than the cap costs only the chunk.
 *  - before the footer (estimate): the row-group bound (`min(file size,
 *    128 MiB output block)` for a trusted run, the spill block for a
 *    spilled one) + `leaves × per-leaf page cap` + buffer.
 *
 * The per-leaf cap is parquet-java's default data page plus dictionary
 * page ([TRUSTED_PAGE_BYTES_PER_LEAF], 2 MiB) for trusted runs, which
 * compaction writes at the defaults, and [SPILL_PAGE_BYTES_PER_LEAF]
 * (128 KiB) for spill files, which this object writes with small pages
 * so that a wide table's spilled runs stay cheap. The buffer is the
 * readahead for a trusted (S3) run and zero for a spill file
 * (`LocalInputFile` reads through an unbuffered `RandomAccessFile`).
 * What the formula does not cover is a single VALUE larger than a page:
 * parquet-java writes it as one page of that size, and the per-row bound
 * on that is [ParquetRewriter.DEFAULT_MAX_NODES_PER_ROW]'s node count,
 * not bytes.
 */
internal object ExternalMergeSort {
    private val log = KotlinLogging.logger {}

    /**
     * parquet-java's default row-group size, which every compaction
     * output is written at: the ceiling of a trusted run's largest row
     * group before its footer is read.
     */
    const val OUTPUT_BLOCK_BYTES: Long = ParquetWriter.DEFAULT_BLOCK_SIZE.toLong()

    /** Decoded data page + dictionary page per leaf, at parquet-java's defaults. */
    const val TRUSTED_PAGE_BYTES_PER_LEAF: Long =
        ParquetProperties.DEFAULT_PAGE_SIZE.toLong() + ParquetProperties.DEFAULT_DICTIONARY_PAGE_SIZE

    /** Data and dictionary page size spill files are written with. */
    const val SPILL_PAGE_BYTES: Int = 64 * 1024

    /** Decoded data page + dictionary page per leaf of a spill file. */
    const val SPILL_PAGE_BYTES_PER_LEAF: Long = 2L * SPILL_PAGE_BYTES

    /**
     * The most rows a spill writer buffers between row-group size checks.
     * parquet-java checks the buffered size only every 100 rows at first
     * and up to every 10,000 later, so with 200 KB rows a "16 MiB" row
     * group came out near 100 MiB — and reading it back allocates all of
     * it. The minimum is 1 (set beside this), so the check adapts to the
     * average row from the first row; the average is what this cap is
     * for: rows that turn fat after a run of small ones are scheduled as
     * if small, and the overshoot is then this many fat rows. Ten keeps
     * it to ten rows; a check is a sum over the column writers, which is
     * noise beside materializing the rows.
     */
    const val SPILL_MAX_ROWS_PER_SIZE_CHECK: Int = 10

    /**
     * Spill files are read once, locally, moments after they are written;
     * SNAPPY measured ~free against raw at ~0.7x the bytes
     * (`CodecMeasurement`), and raw spill of zstd inputs would be ~2.3x
     * their size against a disk budget that evicts the pod when overrun.
     */
    val SPILL_CODEC = OutputCodec(CompressionCodecName.SNAPPY)

    fun rewrite(
        inputs: List<Input>,
        shape: OutputShape,
        keys: SortKeys,
        output: OutputFile,
        codec: OutputCodec,
        parallelism: Int,
        spill: SortSpill,
        appendFloorBytes: Long = ParquetRewriter.APPEND_MIN_ROW_GROUP_BYTES,
    ): RewriteResult = SortedRewrite(inputs, shape, keys, output, codec, parallelism, spill, appendFloorBytes).run()

    // ---- the arithmetic (pure) ---------------------------------------------

    /**
     * Whether the sortedness pre-pass should check [input]: its
     * registered size known and at least [SortSpill.verifyMinBytes].
     */
    fun isWorthVerifying(
        input: RunCandidate,
        spill: SortSpill,
    ): Boolean = input.fileSizeBytes > 0 && input.fileSizeBytes >= spill.verifyMinBytes

    /** Spilled runs [rows] survivors make at [SortSpill.chunkRows] per chunk. */
    fun spillRuns(
        rows: Long,
        spill: SortSpill,
    ): Long = if (rows <= 0) 0 else (rows - 1) / spill.chunkRows + 1

    /**
     * Merge-phase heap: [trustedBytes] for the trusted runs plus
     * [spilledBytes] for the spilled ones. Zero for the degenerate merge
     * — no trusted run and at most one chunk — because that chunk is
     * written straight to the output and no run is ever opened; charging
     * it would refuse every small group under a budget below one run.
     */
    fun mergeCost(
        trustedRuns: Int,
        trustedBytes: Long,
        spilledRuns: Long,
        spilledBytes: Long,
    ): Long = if (isSingleChunk(trustedRuns, spilledRuns)) 0 else trustedBytes + spilledBytes

    /** No trusted run and at most one chunk: no merge, no spill file. */
    fun isSingleChunk(
        trustedRuns: Int,
        spilledRuns: Long,
    ): Boolean = trustedRuns == 0 && spilledRuns <= 1

    /** A trusted run's cost before its footer is read (see the object doc). */
    fun estimatedTrustedRunBytes(
        input: RunCandidate,
        leaves: Int,
        spill: SortSpill,
    ): Long =
        (if (input.fileSizeBytes > 0) minOf(input.fileSizeBytes, OUTPUT_BLOCK_BYTES) else OUTPUT_BLOCK_BYTES) +
            leaves * TRUSTED_PAGE_BYTES_PER_LEAF + spill.readaheadBytes

    /** A spilled run's cost before it is written (see the object doc). */
    fun predictedSpilledRunBytes(
        leaves: Int,
        spill: SortSpill,
    ): Long = spill.spillBlockBytes + leaves * SPILL_PAGE_BYTES_PER_LEAF

    /**
     * What reading the run [footer] describes holds at its largest row
     * group, before the stream buffer (see the object doc).
     */
    fun footerRunBytes(
        footer: ParquetMetadata,
        pageBytesPerLeaf: Long,
    ): Long =
        footer.blocks.maxOfOrNull { rowGroup ->
            rowGroup.compressedSize + rowGroup.columns.sumOf { minOf(it.totalUncompressedSize, pageBytesPerLeaf) }
        } ?: 0L

    /**
     * The catalog-only half of admission. [admitted] is largest first;
     * [untrustedRows] and [demotedRows] are the registered survivors the
     * spill path will receive from untrusted and demoted inputs;
     * [projectedBytes] is the merge cost of this outcome by estimate.
     */
    class Admission<T : RunCandidate>(
        val admitted: List<T>,
        val demoted: List<T>,
        val untrustedRows: Long,
        val demotedRows: Long,
        val projectedBytes: Long,
    )

    /**
     * Decide, from registered sizes and survivor counts alone, which
     * trusted inputs are read as runs, and refuse a group whose spill
     * cannot fit the disk budget — before a single byte is read. [leaves]
     * is the output schema's leaf-column count.
     *
     * LARGEST FIRST, because a large trusted file is the most work saved
     * by not spilling it, and because the small ones are the ones that
     * could number in the thousands (the planner admits up to 2,048 tiny
     * files per group). Each candidate is costed with the spilled runs of
     * the untrusted survivors AND of every candidate already refused —
     * a demoted input's rows are spilled like any other. First fit, not
     * first refusal: a smaller candidate after a refused large one may
     * still fit.
     *
     * A candidate refused LATER adds spilled runs that the earlier, larger
     * admissions were not costed against, so the whole is re-checked and
     * the smallest admitted runs are shed until it fits. Shedding the
     * smallest keeps the most saved work. A group that does not fit with
     * every candidate shed is refused by [SortedRewrite.confirmFooters],
     * which then has nothing to open.
     */
    fun <T : RunCandidate> admit(
        inputs: List<T>,
        spill: SortSpill,
        leaves: Int,
    ): Admission<T> {
        val untrustedRows = inputs.filterNot { it.trustedSorted }.sumOf { it.survivingRecords }
        val perSpilledRun = predictedSpilledRunBytes(leaves, spill)

        fun cost(
            admittedRuns: Int,
            admittedBytes: Long,
            spilledRows: Long,
        ): Long {
            val runs = spillRuns(spilledRows, spill)
            return mergeCost(admittedRuns, admittedBytes, runs, runs * perSpilledRun)
        }

        // sortedByDescending is stable: equal sizes keep input order, so
        // admission is a deterministic function of the input list.
        val candidates = inputs.filter { it.trustedSorted }.sortedByDescending { it.fileSizeBytes }
        var demotedRows = 0L
        var admittedBytes = 0L
        val admitted = ArrayList<T>()
        val demoted = ArrayList<T>()
        for (candidate in candidates) {
            val estimate = estimatedTrustedRunBytes(candidate, leaves, spill)
            val projected = cost(admitted.size + 1, admittedBytes + estimate, untrustedRows + demotedRows)
            if (projected <= spill.mergeBudgetBytes) {
                admitted += candidate
                admittedBytes += estimate
            } else {
                demoted += candidate
                demotedRows += candidate.survivingRecords
            }
        }
        while (admitted.isNotEmpty() &&
            cost(admitted.size, admittedBytes, untrustedRows + demotedRows) > spill.mergeBudgetBytes
        ) {
            val shed = admitted.removeAt(admitted.size - 1)
            admittedBytes -= estimatedTrustedRunBytes(shed, leaves, spill)
            demotedRows += shed.survivingRecords
            demoted += shed
        }
        // The bytes the spill path will READ, as registered. Compressed
        // input against snappy spill is not the same unit, which is why
        // the write itself is metered too; this is the refusal that costs
        // no IO. Not for a group that will never spill: one chunk and no
        // trusted run is sorted in memory and touches no disk.
        val spilledInputBytes =
            inputs.filterNot { it.trustedSorted }.sumOf { it.fileSizeBytes } + demoted.sumOf { it.fileSizeBytes }
        val willSpill = !isSingleChunk(admitted.size, spillRuns(untrustedRows + demotedRows, spill))
        if (willSpill && spilledInputBytes > spill.spillBudgetBytes) {
            throw SpillBudgetExceededException(
                "sorted rewrite would spill $spilledInputBytes registered input bytes, past the " +
                    "spill budget of ${spill.spillBudgetBytes} B; the group is larger than one " +
                    "rewrite may stage on local disk",
                spillBytes = 0,
                runsSpilled = 0,
                runsDemoted = 0,
            )
        }
        return Admission(
            admitted,
            demoted,
            untrustedRows,
            demotedRows,
            cost(admitted.size, admittedBytes, untrustedRows + demotedRows),
        )
    }

    private fun mergeRefusal(
        bytes: Long,
        trustedRuns: Int,
        spilledRuns: Long,
        spill: SortSpill,
        spillBytes: Long,
        runsSpilled: Int,
        runsDemoted: Int,
    ) = MergeBudgetExceededException(
        "sorted merge of $trustedRuns trusted and $spilledRuns spilled runs would buffer $bytes B, " +
            "past the merge budget of ${spill.mergeBudgetBytes} B; the per-group sorted heap budget " +
            "(HOGLAKE_COMPACTION_SORTED_HEAP_BYTES / parallel groups) is too small for the group size " +
            "(HOGLAKE_COMPACTION_TARGET_BYTES)",
        spillBytes,
        runsSpilled,
        runsDemoted,
    )

    /**
     * Remove a spill directory and say whether it is gone. A false here
     * is disk the pod's emptyDir keeps counting until the process dies,
     * so it is logged with the path and reported on the result rather
     * than swallowed; `deleteRecursively` signals failure by return value,
     * not by throwing, which is why both are checked.
     */
    fun deleteSpillDir(dir: Path): Boolean {
        val deleted =
            try {
                dir.toFile().deleteRecursively()
            } catch (e: Exception) {
                log.warn(e) { "could not remove compaction spill directory $dir" }
                return false
            }
        if (!deleted) log.warn { "could not remove compaction spill directory $dir; its files stay on the volume" }
        return deleted
    }

    // ---- runs --------------------------------------------------------------

    private class Head(
        var row: Row,
        val run: Iterator<Row>,
    )

    /** An admitted trusted run whose footer confirmed it fits: its reader and exact merge cost. */
    private class Confirmed(
        val input: Input,
        val reader: ParquetFileReader,
        val bytes: Long,
        /** The first sort key's range over the file (see [SortKeys.footerRange]); null = unknown. */
        val range: ParquetRewriter.KeyRange?,
    )

    /**
     * A spill file read back as the `Group`s it holds. Its schema is the
     * output schema by construction, so there is no column plan — rows
     * are written by the shape's identity plan — and no second
     * `maxNodesPerRow` charge: those rows were bounded when they were
     * first decoded. Each row's sort key is extracted again rather than
     * persisted: the file stays the output schema, and the extraction is
     * one leaf read per key. The reader belongs to the caller; [label]
     * names the file in a refusal.
     */
    private class SpillRows(
        private val reader: ParquetFileReader,
        private val shape: OutputShape,
        keys: SortKeys,
        private val label: String,
    ) : Iterator<Row> {
        private val schema: MessageType = shape.schema
        private val rowIdIndex = shape.rowIdIndex
        private val plan = shape.identityPlan
        private val keys = keys.bind(plan)
        private val columnIO = ColumnIOFactory().getColumnIO(schema)
        private var records: org.apache.parquet.io.RecordReader<Group>? = null
        private var leftInRowGroup = 0L
        private val unbounded = ParquetRewriter.NodeBudget(Int.MAX_VALUE, label)

        override fun hasNext(): Boolean {
            while (leftInRowGroup == 0L) {
                val pages = reader.readNextRowGroup() ?: return false
                // The rewrite's own materializer, for its binaries: a
                // spilled row goes straight to the output writer, which
                // must not keep a reference into this file's pages. The
                // budget is unbounded — these rows were bounded when first
                // decoded.
                records = columnIO.getRecordReader(pages, ParquetRewriter.budgetedMaterializer(schema, unbounded))
                leftInRowGroup = pages.rowCount
            }
            return true
        }

        override fun next(): Row {
            if (!hasNext()) throw NoSuchElementException("spill file is exhausted")
            leftInRowGroup--
            val group = records!!.read()
            return Row(group, group.getLong(rowIdIndex, 0), keys.extract(group, label), plan)
        }
    }

    // ---- spill files -------------------------------------------------------

    /**
     * Total bytes this rewrite has spilled; throws the moment a write
     * would take it past the budget. Charged BEFORE the bytes are handed
     * on, so the budget is never exceeded on disk, by any amount.
     */
    private class SpillMeter(private val budget: Long) {
        var written = 0L
            private set

        fun charge(bytes: Long) {
            if (written + bytes > budget) {
                throw SpillBudgetExceededException(
                    "sorted rewrite reached its spill budget of $budget B mid-run " +
                        "($written B written, ${written + bytes} B needed); stopping before the spill " +
                        "directory outgrows the volume it lives on",
                    // The meter knows only bytes; SortedRewrite.run adds
                    // the run counts on the way out.
                    spillBytes = written,
                    runsSpilled = 0,
                    runsDemoted = 0,
                )
            }
            written += bytes
        }
    }

    /**
     * A local spill file whose every byte passes through [meter]. Its
     * stream needs no cleanup of ours: when the meter's refusal unwinds
     * the writer, [ParquetRewriter.writingTo] aborts it, and
     * [OutputWriter.abort] closes the stream it created.
     */
    private class MeteredSpillFile(
        private val path: Path,
        private val meter: SpillMeter,
    ) : OutputFile {
        private val delegate = LocalOutputFile(path)

        override fun create(blockSizeHint: Long): PositionOutputStream = Metered(delegate.create(blockSizeHint), meter)

        override fun createOrOverwrite(blockSizeHint: Long): PositionOutputStream =
            Metered(delegate.createOrOverwrite(blockSizeHint), meter)

        override fun supportsBlockSize(): Boolean = delegate.supportsBlockSize()

        override fun defaultBlockSize(): Long = delegate.defaultBlockSize()

        override fun getPath(): String = path.toString()
    }

    private class Metered(
        private val out: PositionOutputStream,
        private val meter: SpillMeter,
    ) : PositionOutputStream() {
        override fun getPos(): Long = out.pos

        override fun write(b: Int) {
            meter.charge(1)
            out.write(b)
        }

        override fun write(
            b: ByteArray,
            off: Int,
            len: Int,
        ) {
            meter.charge(len.toLong())
            out.write(b, off, len)
        }

        override fun flush() = out.flush()

        override fun close() = out.close()
    }

    // ---- one rewrite -------------------------------------------------------

    /**
     * One sorted rewrite, as an object because its cleanup is the point:
     * every reader it opens is registered in [open] the moment it exists,
     * and [run]'s `finally` closes them all and THEN removes the spill
     * directory, on success, typed refusal, IO failure and
     * OutOfMemoryError alike. A leaked S3 stream holds a connection from a
     * bounded pool; a leaked spill directory is disk the pod's emptyDir
     * counts until the process dies.
     */
    private class SortedRewrite(
        private val inputs: List<Input>,
        private val shape: OutputShape,
        private val keys: SortKeys,
        private val output: OutputFile,
        private val codec: OutputCodec,
        private val parallelism: Int,
        private val spill: SortSpill,
        private val appendFloorBytes: Long,
    ) {
        private val leaves = shape.schema.columns.size

        /** The first key's range in each spill file, in [spillFiles] order (see [chooseAppended]). */
        private val spillRanges = ArrayList<ParquetRewriter.KeyRange>()
        private val open = ArrayList<AutoCloseable>()
        private val meter = SpillMeter(spill.spillBudgetBytes)
        private var spillDir: Path? = null
        private val spillFiles = ArrayList<Pair<Path, String>>()

        /** Exact merge cost of the spill files written so far, from their footers. */
        private var spilledBytes = 0L

        private var chunk = ArrayList<Row>()

        /** Labels of the inputs the current chunk holds rows of, for its errors. */
        private val chunkLabels = LinkedHashSet<String>()

        private var trustedRuns: List<Iterator<Row>> = emptyList()
        private var trustedBytes = 0L
        private var demoted = 0
        private var verified = 0
        private var unsorted = 0
        private var unchecked = 0
        private var sortCheckBytes = 0L

        fun run(): RewriteResult {
            var result: RewriteResult? = null
            try {
                result = rewrite()
            } catch (e: SpillBudgetExceededException) {
                // Rebuilt with the rewrite's own counters: the meter that
                // threw knows only bytes, and the caller has no result to
                // read the spent work from.
                throw SpillBudgetExceededException(
                    e.message!!,
                    meter.written,
                    spillFiles.size,
                    demoted,
                    verified,
                    unsorted,
                    unchecked,
                    sortCheckBytes,
                ).apply { initCause(e) }
            } catch (e: MergeBudgetExceededException) {
                throw MergeBudgetExceededException(
                    e.message!!,
                    meter.written,
                    spillFiles.size,
                    demoted,
                    verified,
                    unsorted,
                    unchecked,
                    sortCheckBytes,
                ).apply { initCause(e) }
            } finally {
                for (c in open.asReversed()) runCatching { c.close() }
                val dir = spillDir
                val cleaned = dir == null || deleteSpillDir(dir)
                if (result != null) result = result.copy(spillCleanupFailed = !cleaned)
            }
            return result!!
        }

        private fun rewrite(): RewriteResult {
            // Verified inputs come back marked trusted: from here on they
            // are run candidates like any metadata-trusted output.
            val candidates = verifySortedness()
            val admission = admit(candidates, spill, leaves)
            demoted = admission.demoted.size
            val kept = confirmFooters(admission)
            trustedBytes = kept.sumOf { it.bytes }
            // Built before the chunk phase spends anything: a trusted
            // input whose schema the live one cannot take is refused
            // here, not after the group has been spilled.
            val readers = kept.map { SurvivorReader(it.input, it.reader, shape, keys) }
            trustedRuns = readers

            val trusted = java.util.Collections.newSetFromMap(java.util.IdentityHashMap<Input, Boolean>())
            kept.mapTo(trusted) { it.input }
            val spillPath = candidates.filterNot { it in trusted }
            ParquetRewriter.forEachOpenedInput(spillPath, parallelism) { input, reader ->
                for (row in SurvivorReader(input, reader, shape, keys)) {
                    // Flush when the NEXT row arrives, not when the chunk
                    // fills: a group of exactly chunkRows survivors is
                    // then still one chunk, and takes the no-spill path.
                    if (chunk.size >= spill.chunkRows) spillChunk()
                    chunk.add(row)
                    chunkLabels.add(input.label)
                }
            }

            if (spillFiles.isEmpty() && trustedRuns.isEmpty()) return writeSingleChunk()
            if (chunk.isNotEmpty()) spillChunk()
            // The chunk phase is over; its backing array must not sit on
            // the heap through the merge, which has its own budget.
            chunk = ArrayList()

            // Trusted runs that need no merging at all: appended byte for
            // byte, in key order, around the rows the merge produces.
            val appended = chooseAppended(kept)
            val blocks = appended.map { kept[it] }.sortedWith { a, b -> keys.compareFirst(a.range!!.lo, b.range!!.lo) }
            val runs = ArrayList<Iterator<Row>>()
            for ((i, reader) in readers.withIndex()) if (i !in appended) runs += reader
            for ((path, label) in spillFiles) {
                val reader = ParquetReaders.open(LocalInputFile(path))
                open += reader
                runs += SpillRows(reader, shape, keys, label)
            }
            var written = 0L
            var minRowId: Long? = null
            var appendedGroups = 0
            var appendedBytes = 0L
            val (_, footer) =
                ParquetRewriter.writingTo(output, shape.schema, codec) { writer ->
                    var nextBlock = 0

                    // An appended run's rows are one contiguous span of the
                    // merge order: the rows encoded so far precede it, so
                    // they are flushed as their own row group first.
                    fun append(run: Confirmed) {
                        writer.appendRowGroups(run.input.source, run.reader, flushPending = true)
                        written += run.reader.footer.blocks.sumOf { it.rowCount }
                        val least = checkNotNull(ParquetRewriter.minAppendedRowId(run.reader.footer))
                        minRowId = minOf(minRowId ?: least, least)
                    }
                    val queue =
                        PriorityQueue<Head>(maxOf(1, runs.size)) { a, b -> keys.comparator.compare(a.row, b.row) }
                    for (run in runs) if (run.hasNext()) queue.add(Head(run.next(), run))
                    // Every run is driven to EXHAUSTION: a head leaves the
                    // queue for good only when its run's hasNext() says
                    // false, and that call is where a run checks its DV.
                    while (queue.isNotEmpty()) {
                        val head = queue.poll()
                        // Disjoint ranges: a block that starts before this
                        // row ends before it too.
                        while (nextBlock < blocks.size &&
                            keys.compareFirst(blocks[nextBlock].range!!.lo, head.row.key[0]) < 0
                        ) {
                            append(blocks[nextBlock++])
                        }
                        writer.write(head.row)
                        written++
                        minRowId = minOf(minRowId ?: head.row.rowId, head.row.rowId)
                        if (head.run.hasNext()) {
                            head.row = head.run.next()
                            queue.add(head)
                        }
                    }
                    while (nextBlock < blocks.size) append(blocks[nextBlock++])
                    appendedGroups = writer.rowGroupsAppended
                    appendedBytes = writer.bytesAppended
                }
            return RewriteResult(
                written,
                minRowId,
                footer,
                runsTrusted = trustedRuns.size,
                runsSpilled = spillFiles.size,
                runsDemoted = demoted,
                spillBytes = meter.written,
                filesVerified = verified,
                filesUnsorted = unsorted,
                filesUnchecked = unchecked,
                sortCheckBytes = sortCheckBytes,
                rowGroupsAppended = appendedGroups,
                bytesAppended = appendedBytes,
            )
        }

        /**
         * The indexes of the [kept] runs to APPEND rather than merge
         * (hoglake#134 package D1): a run is appended when it is
         * appendable byte for byte ([ParquetRewriter.appendRefusal]) AND
         * the range of its FIRST sort key ([SortKeys.footerRange]) is
         * strictly disjoint from every other kept run's range and every
         * spill file's — so that in the merge order its rows form one
         * contiguous block, and the merge only orders what overlaps.
         *
         * Conservative by construction, because the property test is the
         * only oracle and a wrong append is a mis-sorted file: a tie on
         * the first key at a boundary is an overlap (merged); a range is
         * computed over every PHYSICAL row of a file, deleted ones
         * included, so a run with a DV can only look wider than it is;
         * and any run whose range the statistics cannot give (no stats, a
         * float/double first key, a key leaf of another type) makes EVERY
         * candidate merge, since nothing is known to be disjoint from it.
         */
        private fun chooseAppended(kept: List<Confirmed>): Set<Int> {
            val candidates =
                kept.indices.filter {
                    val run = kept[it]
                    ParquetRewriter.appendRefusal(run.input, run.reader.footer, shape.schema, appendFloorBytes) == null
                }
            if (candidates.isEmpty()) return emptySet()
            // A run with no rows has no range and constrains nothing; a run
            // with rows whose range is unknown constrains everything.
            val others = kept.filter { it.reader.footer.blocks.any { b -> b.rowCount > 0 } }
            val unknown = others.firstOrNull { it.range == null }
            if (unknown != null) {
                // Otherwise invisible: the group just never appends.
                log.debug {
                    "sorted rewrite: no row group appended — ${unknown.input.label} gives no usable range for " +
                        "the first sort key (no footer statistics, a float/double key, or a key leaf of " +
                        "another type), so no run is known to be disjoint from it"
                }
                return emptySet()
            }
            return candidates.filter { i ->
                val mine = kept[i].range ?: return@filter false
                others.all { it === kept[i] || keys.disjoint(mine, it.range!!) } &&
                    spillRanges.all { keys.disjoint(mine, it) }
            }.toSet()
        }

        /**
         * The sortedness pre-pass ([SortednessCheck]) over every input the
         * caller did not trust: each one that passes is returned marked
         * [Input.trustedSorted], so admission takes it as a run candidate
         * like a metadata-trusted output; the rest are returned as they
         * were. Input order is kept.
         *
         * Through the open window, one file per slot, the check on this
         * thread in input order — the same discipline the chunk phase
         * uses. Each file is opened from its [Input.keySource] (a small
         * readahead) and closed when its check ends; the merge reopens the
         * admitted ones from their [Input.source], because a parquet-java
         * reader cannot rewind and this one holds a projected schema.
         */
        private fun verifySortedness(): List<Input> {
            val untrusted = inputs.withIndex().filter { !it.value.trustedSorted }
            // THE SIZE FLOOR. A small file is cheapest on the chunk path:
            // read once, batched with thousands of others into a few
            // sorted runs. Verifying it costs a projected open, and
            // merging it as a run a second open plus a queue slot. An
            // unknown size (0) is never worth the bet either.
            val candidates = untrusted.filter { isWorthVerifying(it.value, spill) }
            unchecked = untrusted.size - candidates.size
            if (candidates.isEmpty()) return inputs
            val counters = candidates.map { ByteCountingInputFile(it.value.keySource ?: it.value.source) }
            val checked = candidates.mapIndexed { i, c -> c.value.copy(source = counters[i], keySource = null) }
            val passed = BooleanArray(inputs.size)
            var next = 0
            try {
                ParquetRewriter.forEachOpenedInput(checked, parallelism) { input, reader ->
                    val counter = counters[next]
                    val index = candidates[next++].index
                    val sorted = SortednessCheck.isSorted(input, reader, keys.fields)
                    if (sorted) {
                        passed[index] = true
                        verified++
                    } else {
                        unsorted++
                    }
                    // Per file, so a writer whose row groups make the check
                    // expensive (many small ones: a read per key chunk per
                    // row group) shows up by name.
                    log.debug {
                        "sortedness pre-pass: ${input.label} ${if (sorted) "sorted" else "unsorted"} " +
                            "after ${counter.reads} reads, ${counter.bytesRead} B " +
                            "(${reader.footer.blocks.size} row groups)"
                    }
                }
            } finally {
                sortCheckBytes = counters.sumOf { it.bytesRead }
            }
            return inputs.mapIndexed { i, input -> if (passed[i]) input.copy(trustedSorted = true) else input }
        }

        /**
         * Open the admitted trusted runs — footers only; `S3InputFile`
         * prefetches the tail and no row group is read until the merge —
         * and re-cost each from its footer ([footerRunBytes]). A run whose
         * exact cost breaks the budget is demoted and its reader closed,
         * still before any data is read.
         *
         * Returns the kept runs in admission order.
         */
        private fun confirmFooters(admission: Admission<Input>): List<Confirmed> {
            val readers = ArrayList<ParquetFileReader>()
            ParquetRewriter.forEachOpenedReader(admission.admitted, parallelism) { _, reader ->
                open += reader
                readers += reader
            }
            val perSpilledRun = predictedSpilledRunBytes(leaves, spill)
            var pendingEstimates = admission.admitted.sumOf { estimatedTrustedRunBytes(it, leaves, spill) }
            var demotedRows = admission.demotedRows
            var keptBytes = 0L
            val kept = ArrayList<Confirmed>()
            for ((input, reader) in admission.admitted.zip(readers)) {
                val exact = footerRunBytes(reader.footer, TRUSTED_PAGE_BYTES_PER_LEAF) + spill.readaheadBytes
                pendingEstimates -= estimatedTrustedRunBytes(input, leaves, spill)
                val runs = spillRuns(admission.untrustedRows + demotedRows, spill)
                val projected =
                    mergeCost(kept.size + 1, keptBytes + exact + pendingEstimates, runs, runs * perSpilledRun)
                if (projected <= spill.mergeBudgetBytes) {
                    kept += Confirmed(input, reader, exact, keys.footerRange(reader.footer))
                    keptBytes += exact
                } else {
                    open.remove(reader)
                    runCatching { reader.close() }
                    demoted++
                    demotedRows += input.survivingRecords
                }
            }
            // Each check above assumed the runs after it would be kept at
            // their estimates; a later demotion moves rows to the spill
            // path, so the final shape is checked once more as a whole.
            // It is also where a group admission could not fit is refused:
            // admission shed every run, so nothing above was opened.
            val runs = spillRuns(admission.untrustedRows + demotedRows, spill)
            val exactCost = mergeCost(kept.size, keptBytes, runs, runs * perSpilledRun)
            if (exactCost > spill.mergeBudgetBytes) {
                throw mergeRefusal(exactCost, kept.size, runs, spill, meter.written, spillFiles.size, demoted)
            }
            return kept
        }

        /** No trusted run and one chunk: sort it and write it as the output, touching no disk. */
        private fun writeSingleChunk(): RewriteResult {
            sortChunk()
            var minRowId: Long? = null
            val (_, footer) =
                ParquetRewriter.writingTo(output, shape.schema, codec) { writer ->
                    for (row in chunk) {
                        writer.write(row)
                        minRowId = minOf(minRowId ?: row.rowId, row.rowId)
                    }
                }
            return RewriteResult(
                chunk.size.toLong(),
                minRowId,
                footer,
                runsDemoted = demoted,
                filesVerified = verified,
                filesUnsorted = unsorted,
                filesUnchecked = unchecked,
                sortCheckBytes = sortCheckBytes,
            )
        }

        /** Comparisons are on extracted keys: a bad key value was refused when its row was read. */
        private fun sortChunk() = chunk.sortWith(keys.comparator)

        private fun spillChunk() {
            sortChunk()
            // Sorted by the full key, so the first key's extremes are the
            // chunk's first and last rows.
            spillRanges += ParquetRewriter.KeyRange(chunk.first().key[0], chunk.last().key[0])
            val n = spillFiles.size + 1
            val dir =
                spillDir
                    ?: Files.createDirectory(spill.spillDir.resolve("${SpillDirectory.PREFIX}${UUID.randomUUID()}"))
                        .also { spillDir = it }
            val path = dir.resolve("spill-$n.parquet")
            val (_, written) =
                ParquetRewriter.writingTo(
                    MeteredSpillFile(path, meter),
                    shape.schema,
                    SPILL_CODEC,
                    ParquetRewriter.WriterTuning(
                        rowGroupBytes = spill.spillBlockBytes.toLong(),
                        pageBytes = SPILL_PAGE_BYTES,
                        maxRowsPerSizeCheck = SPILL_MAX_ROWS_PER_SIZE_CHECK,
                    ),
                ) { writer -> for (row in chunk) writer.write(row) }
            spillFiles += path to "spill-$n (${describe(chunkLabels)})"
            chunk.clear()
            chunkLabels.clear()
            // The EXACT cost of the run just written, off its own footer:
            // the predicted cost was the block, and the merge will hold
            // what was written, not what was intended. Checked here, before
            // the merge opens anything, so a run count the registered
            // survivors under-predicted is caught at the run that breaks it.
            val footer = written ?: ParquetReaders.open(LocalInputFile(path)).use { it.footer }
            spilledBytes += footerRunBytes(footer, SPILL_PAGE_BYTES_PER_LEAF)
            val cost = mergeCost(trustedRuns.size, trustedBytes, n.toLong(), spilledBytes)
            if (cost > spill.mergeBudgetBytes) {
                throw mergeRefusal(cost, trustedRuns.size, n.toLong(), spill, meter.written, spillFiles.size, demoted)
            }
        }

        private fun describe(labels: Collection<String>): String =
            if (labels.size <= 1) {
                labels.firstOrNull() ?: "no input"
            } else {
                "${labels.size} inputs, ${labels.first()} .. ${labels.last()}"
            }
    }
}
