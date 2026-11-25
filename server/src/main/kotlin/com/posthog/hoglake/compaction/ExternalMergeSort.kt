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
import org.apache.parquet.format.converter.ParquetMetadataConverter
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
 * A sorted reader, merge head, or retained row exceeds its memory allowance.
 * Metadata estimates ordinary runs.
 * Runtime profiles and allocation guards enforce the actual limits.
 * Deterministic refusals can be remembered without retaining input graphs.
 * No multi-pass merge is added.
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
    val oversizedRow: Boolean = false,
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

    /** Runtime proof permits a byte copy without decoded readers. */
    val byteAppend: Boolean get() = false
}

/**
 * The bounds of one sorted rewrite (see [ParquetRewriter]'s class doc).
 * Required for any non-empty sort order.
 */
data class SortSpill(
    /** Advisory row ceiling. Actual retained bytes can flush the chunk earlier. */
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
    /** Structural reader reserve. Positive excess shares the retained-row allowance. */
    val readerBudgetBytes: Long = maxOf(mergeBudgetBytes, ParquetWriter.DEFAULT_BLOCK_SIZE.toLong()),
    internal var memoryObserver: ((event: String, metrics: Map<String, Long>) -> Unit)? = null,
) {
    init {
        require(chunkRows >= 1) { "chunkRows must be at least 1, got $chunkRows" }
        require(mergeBudgetBytes >= 1) { "mergeBudgetBytes must be at least 1, got $mergeBudgetBytes" }
        require(spillBudgetBytes >= 1) { "spillBudgetBytes must be at least 1, got $spillBudgetBytes" }
        require(spillBlockBytes >= 1) { "spillBlockBytes must be at least 1, got $spillBlockBytes" }
        require(readaheadBytes >= 0) { "readaheadBytes must not be negative, got $readaheadBytes" }
        require(verifyMinBytes >= 0) { "verifyMinBytes must not be negative, got $verifyMinBytes" }
        require(readerBudgetBytes > 0) { "readerBudgetBytes must be positive" }
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
 * Metadata estimates ordinary compressed blocks and standard pages.
 * Actual page headers replace those estimates before decoded readers open.
 * Reader profiles charge compressed chunks, live pages, dictionaries, decoder state, and source buffers.
 * They do not charge every uncompressed row-group value.
 *
 * A decoded run adds two charged heads to its reader profile.
 * The second head covers replacement overlap.
 * Spill writers measure output-shaped rows without constructing another graph.
 * Trusted heads have fixed preflight caps and a shared runtime allocation ledger.
 * Byte-appended runs need neither decoded readers nor charged heads.
 *
 * Retained chunks use the full group allowance and an advisory row ceiling.
 * A structural reserve covers ordinary input readers.
 * Positive reader excess competes with retained rows before loading pages.
 * Deterministic runtime memory refusals feed the executor's immutable-identity cache.
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

    /** Flat producer pages contain at most 20,000 values, padded to groups of eight. */
    const val SPILL_PACKED_SCRATCH_BYTES_PER_LEAF: Long =
        4L * (ParquetProperties.DEFAULT_PAGE_ROW_COUNT_LIMIT + 7)

    /** Ordinary spill estimate. Exact profiles still enforce unknown payload and dictionary expansion. */
    const val SPILL_READER_BYTES_PER_LEAF: Long =
        SPILL_PAGE_BYTES_PER_LEAF + SPILL_PACKED_SCRATCH_BYTES_PER_LEAF +
            2 * SortedReaderMemory.COLUMN_OBJECT_BYTES + 2 * SortedReaderMemory.PAGE_OBJECT_BYTES

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

    private inline fun <T> Iterable<T>.saturatingSumOf(count: (T) -> Long): Long =
        fold(0L) { total, value -> SortMemory.add(total, count(value)) }

    /** Validate signed catalog facts before the optional I/O pre-pass. */
    private fun validateCandidates(inputs: List<RunCandidate>): Boolean {
        for (input in inputs) {
            if (input.fileSizeBytes < 0 || input.survivingRecords < 0) {
                throw InvalidDataException("sorted admission received negative registered bytes or survivors")
            }
        }
        if (inputs.saturatingSumOf { it.fileSizeBytes } == Long.MAX_VALUE) {
            throw SpillBudgetExceededException(
                "sorted admission cannot represent the registered input byte total",
                0,
                0,
                0,
            )
        }
        return inputs.saturatingSumOf { it.survivingRecords } == Long.MAX_VALUE
    }

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
    ): Long =
        when {
            rows < 0 || rows == Long.MAX_VALUE -> Long.MAX_VALUE
            rows == 0L -> 0
            else -> SortMemory.add((rows - 1) / spill.chunkRows, 1)
        }

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
    ): Long =
        when {
            trustedRuns < 0 || !SortMemory.fits(trustedBytes, Long.MAX_VALUE) ||
                !SortMemory.fits(spilledRuns, Long.MAX_VALUE) || !SortMemory.fits(spilledBytes, Long.MAX_VALUE) ->
                Long.MAX_VALUE
            isSingleChunk(trustedRuns, spilledRuns) -> 0
            else -> SortMemory.add(trustedBytes, spilledBytes)
        }

    /** No trusted run and at most one chunk: no merge, no spill file. */
    fun isSingleChunk(
        trustedRuns: Int,
        spilledRuns: Long,
    ): Boolean = trustedRuns == 0 && spilledRuns in 0..1

    /** A trusted run's cost before its footer is read (see the object doc). */
    fun estimatedTrustedRunBytes(
        input: RunCandidate,
        leaves: Int,
        spill: SortSpill,
    ): Long =
        if (input.byteAppend) {
            0
        } else {
            SortMemory.runBytes(
                SortMemory.add(
                    if (input.fileSizeBytes > 0) minOf(input.fileSizeBytes, OUTPUT_BLOCK_BYTES) else OUTPUT_BLOCK_BYTES,
                    SortMemory.multiply(leaves.toLong(), TRUSTED_PAGE_BYTES_PER_LEAF),
                    spill.readaheadBytes.toLong(),
                ),
                estimatedHeadBytes(leaves),
            )
        }

    /** A spilled run's cost before it is written (see the object doc). */
    fun predictedSpilledRunBytes(
        leaves: Int,
        spill: SortSpill,
    ): Long =
        SortMemory.runBytes(
            SortMemory.add(
                spill.spillBlockBytes.toLong(),
                SortMemory.multiply(leaves.toLong(), SPILL_READER_BYTES_PER_LEAF),
            ),
            estimatedHeadBytes(leaves),
        )

    private fun estimatedHeadBytes(leaves: Int): Long =
        SortMemory.multiply(CompactionConfig.SORTED_HEAP_BYTES_PER_NODE, SortMemory.add(leaves.toLong(), 1))

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
        require(leaves >= 0) { "output leaf count must not be negative" }
        val rowCountOverflow = validateCandidates(inputs)
        val untrusted = inputs.filterNot { it.trustedSorted }
        val untrustedRows = untrusted.saturatingSumOf { it.survivingRecords }
        val perSpilledRun = predictedSpilledRunBytes(leaves, spill)

        fun cost(
            admittedRuns: Int,
            admittedBytes: Long,
            spilledRows: Long,
        ): Long {
            if (rowCountOverflow) return Long.MAX_VALUE
            val runs = spillRuns(spilledRows, spill)
            return mergeCost(admittedRuns, admittedBytes, runs, SortMemory.multiply(runs, perSpilledRun))
        }

        // sortedByDescending is stable: equal sizes keep input order, so
        // admission is a deterministic function of the input list.
        val candidates = inputs.filter { it.trustedSorted }.sortedByDescending { it.fileSizeBytes }
        var demotedRows = 0L
        var admittedBytes = 0L
        val admitted = ArrayList<T>()
        val demoted = ArrayList<T>()
        // Restore a finite prefix directly.
        // Subtracting from a saturated sum loses its excess.
        val admittedPrefixBytes = LongArray(candidates.size)
        for (candidate in candidates) {
            val estimate = estimatedTrustedRunBytes(candidate, leaves, spill)
            val nextBytes = SortMemory.add(admittedBytes, estimate)
            val projected =
                cost(Math.addExact(admitted.size, 1), nextBytes, SortMemory.add(untrustedRows, demotedRows))
            if (SortMemory.fits(projected, spill.mergeBudgetBytes)) {
                admittedPrefixBytes[admitted.size] = nextBytes
                admitted += candidate
                admittedBytes = nextBytes
            } else {
                demoted += candidate
                demotedRows = SortMemory.add(demotedRows, candidate.survivingRecords)
            }
        }
        while (admitted.isNotEmpty() &&
            !SortMemory.fits(
                cost(admitted.size, admittedBytes, SortMemory.add(untrustedRows, demotedRows)),
                spill.mergeBudgetBytes,
            )
        ) {
            val shed = admitted.removeAt(admitted.size - 1)
            admittedBytes = if (admitted.isEmpty()) 0 else admittedPrefixBytes[admitted.lastIndex]
            demotedRows = SortMemory.add(demotedRows, shed.survivingRecords)
            demoted += shed
        }
        // The bytes the spill path will READ, as registered. Compressed
        // input against snappy spill is not the same unit, which is why
        // the write itself is metered too; this is the refusal that costs
        // no IO. Not for a group that will never spill: one chunk and no
        // trusted run is sorted in memory and touches no disk.
        val spilledInputBytes =
            SortMemory.add(
                untrusted.saturatingSumOf { it.fileSizeBytes },
                demoted.saturatingSumOf { it.fileSizeBytes },
            )
        val willSpill = !isSingleChunk(admitted.size, spillRuns(SortMemory.add(untrustedRows, demotedRows), spill))
        if (spilledInputBytes == Long.MAX_VALUE ||
            (willSpill && !SortMemory.fits(spilledInputBytes, spill.spillBudgetBytes))
        ) {
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
            cost(admitted.size, admittedBytes, SortMemory.add(untrustedRows, demotedRows)),
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
        val profile: SortedReaderMemory.Profile? = null,
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
     *
     * A spilled row is never re-checked against deletion-vector state,
     * and needs no check: DVs are applied ONCE, in the chunk phase, by
     * the [SurvivorReader] that fed the chunk, so a spill file holds
     * survivors only — there is no position left in it for a vector to
     * name. A DV superseded between plan and commit is not this merge's
     * to catch either: the rewrite applied the PLANNED vectors by
     * design, and `CompactionService.commitGroup` re-reads each input's
     * live `delete_file_id` under the catalog lock and refuses the group
     * (`dv_superseded`) when it moved.
     */
    private class SpillRows(
        private val reader: SortedReaderMemory.Reader,
        private val rowGroupCount: Int,
        private val shape: OutputShape,
        keys: SortKeys,
        private val label: String,
        private val memory: SortMemory.RowBudget,
        private val decodeStats: SortMemory.DecodeStats?,
    ) : Iterator<Row> {
        private val schema: MessageType = shape.schema
        private val rowIdIndex = shape.rowIdIndex
        private val plan = shape.identityPlan
        private val keys = keys.bind(plan)
        private val columnIO = ColumnIOFactory().getColumnIO(schema)
        private var records: org.apache.parquet.io.RecordReader<Group>? = null
        private var leftInRowGroup = 0L
        private var rowGroupIndex = 0
        private val unbounded = ParquetRewriter.NodeBudget(Int.MAX_VALUE, label)

        override fun hasNext(): Boolean {
            while (leftInRowGroup == 0L) {
                records = null
                if (rowGroupIndex >= rowGroupCount) {
                    reader.close()
                    return false
                }
                val pages = reader.readRowGroup(rowGroupIndex++)
                // The rewrite's own materializer, for its binaries: a
                // spilled row goes straight to the output writer, which
                // must not keep a reference into this file's pages. The
                // budget is unbounded — these rows were bounded when first
                // decoded.
                try {
                    records =
                        columnIO.getRecordReader(
                            pages,
                            ParquetRewriter.budgetedMaterializer(schema, unbounded, memory),
                        )
                } catch (error: SortedReaderMemoryException) {
                    throw SortMemory.readerFailure(error)
                }
                leftInRowGroup = pages.rowCount
            }
            return true
        }

        override fun next(): Row {
            if (!hasNext()) throw NoSuchElementException("spill file is exhausted")
            leftInRowGroup--
            val group =
                try {
                    records!!.read()
                } catch (error: SortedReaderMemoryException) {
                    throw SortMemory.readerFailure(error)
                }
            if (decodeStats != null) decodeStats.rows++
            val key = keys.extract(group, label, memory)
            return Row(group, group.getLong(rowIdIndex, 0), key, plan, memory.spent)
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
            val next = SortMemory.add(written, bytes)
            if (!SortMemory.fits(next, budget)) {
                throw SpillBudgetExceededException(
                    "sorted rewrite reached its spill budget of $budget B mid-run " +
                        "($written B written, $next B needed); stopping before the spill " +
                        "directory outgrows the volume it lives on",
                    // The meter knows only bytes; SortedRewrite.run adds
                    // the run counts on the way out.
                    spillBytes = written,
                    runsSpilled = 0,
                    runsDemoted = 0,
                )
            }
            written = next
        }
    }

    /**
     * A local spill file whose every byte passes through [meter]. Its
     * stream needs no cleanup of ours: a refusal while writing or while
     * closing (the last row group) unwinds to [ParquetRewriter.writingTo],
     * which aborts the writer, and [OutputWriter.abort] closes the stream
     * it created.
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
        private val memory = SortMemory(spill.mergeBudgetBytes)
        private val spillWriterSizer =
            WriterMemory(
                memory,
                shape.schema,
                SPILL_CODEC,
                2L * spill.spillBlockBytes,
                spill.spillBlockBytes.toLong() / SPILL_MAX_ROWS_PER_SIZE_CHECK,
                SPILL_PAGE_BYTES.toLong(),
                decimalWidths = keys.memoryDecimalWidths,
            ) {}
        private val decodeStats = if (spill.memoryObserver == null) null else SortMemory.DecodeStats()

        /** The first key's range in each spill file, in [spillFiles] order (see [chooseAppended]). */
        private val spillRanges = ArrayList<ParquetRewriter.KeyRange>()
        private var rangePayloadBytes = 0L
        private val open = ArrayList<AutoCloseable>()
        private val meter = SpillMeter(spill.spillBudgetBytes)
        private var spillDir: Path? = null

        private class SpillFile(
            val path: Path,
            val label: String,
            val maxRowBytes: Long,
            val footer: ParquetMetadata,
            val profile: SortedReaderMemory.Profile,
        )

        private val spillFiles = ArrayList<SpillFile>()
        private var completedSpills = 0

        /** Exact merge cost of the spill files written so far, from their footers. */
        private var spilledBytes = 0L

        private var chunk = ArrayList<Row>()
        private var chunkBytes = 0L
        private var chunkWriterExtra = 0L
        private var chunkReaderExtra = 0L
        private var decodingBytes = 0L
        private var trustedHeadBytes = 0L
        private var mergeWriterExtra = 0L
        private var retainedFooterBytes = 0L
        private var retainedProfileBytes = 0L
        private val demotedProfiles = java.util.IdentityHashMap<Input, SortedReaderMemory.Profile>()

        /** Labels of the inputs the current chunk holds rows of, for its errors. */
        private val chunkLabels = LinkedHashSet<String>()

        private var trustedRuns: List<Iterator<Row>> = emptyList()
        private var trustedCount = 0
        private var trustedBytes = 0L
        private var demoted = 0
        private var verified = 0
        private var unsorted = 0
        private var unchecked = 0
        private var sortCheckBytes = 0L

        /** Runs whose unknown first-key range blocked every append (see [chooseAppended]). */
        private var unranged = 0

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
                    completedSpills,
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
                    completedSpills,
                    demoted,
                    verified,
                    unsorted,
                    unchecked,
                    sortCheckBytes,
                    e.oversizedRow,
                ).apply { initCause(e) }
            } finally {
                for (c in open.asReversed()) runCatching { c.close() }
                val dir = spillDir
                val cleaned = dir == null || deleteSpillDir(dir)
                if (result != null) result = result.copy(spillCleanupFailed = !cleaned)
            }
            return result
        }

        private fun rewrite(): RewriteResult {
            if (validateCandidates(inputs)) {
                throw MergeBudgetExceededException("sorted admission cannot represent the surviving row total", 0, 0, 0)
            }
            // Verified inputs come back marked trusted: from here on they
            // are run candidates like any metadata-trusted output.
            val candidates = prepareAppends(verifySortedness())
            val admission = admit(candidates, spill, leaves)
            spill.memoryObserver?.invoke(
                "admission",
                mapOf(
                    "predictedRuns" to spillRuns(SortMemory.add(admission.untrustedRows, admission.demotedRows), spill),
                    "reservationBytes" to admission.projectedBytes,
                    "rows" to candidates.saturatingSumOf { it.survivingRecords },
                ),
            )
            demoted = admission.demoted.size
            val kept = confirmFooters(admission)
            trustedBytes = kept.saturatingSumOf { it.bytes }
            trustedCount = kept.size
            // Built before the chunk phase spends anything: a trusted
            // input whose schema the live one cannot take is refused
            // here, not after the group has been spilled.

            val trusted = java.util.Collections.newSetFromMap(java.util.IdentityHashMap<Input, Boolean>())
            kept.mapTo(trusted) { it.input }
            val spillPath = candidates.filterNot { it in trusted }
            val predictedRuns = spillRuns(spillPath.saturatingSumOf { it.survivingRecords }, spill)
            val reservedSpills = SortMemory.multiply(predictedRuns, predictedSpilledRunBytes(leaves, spill))
            val headCap = maxOf(1, SortMemory.remaining(memory.bytes, trustedBytes, reservedSpills) / 2)
            val headMaxima = LongArray(kept.size)

            val readers =
                kept.mapIndexed { index, run ->
                    if (run.input.byteAppend) {
                        null
                    } else {
                        val rowBudget =
                            memory.RowBudget(run.input.label, headCap) { next, _ ->
                                if (next > headMaxima[index]) {
                                    val heads = SortMemory.add(trustedHeadBytes, next - headMaxima[index])
                                    val cost =
                                        SortMemory.add(
                                            SortMemory.runBytes(SortMemory.add(trustedBytes, spilledBytes), heads),
                                            mergeWriterExtra,
                                            rangePayloadBytes,
                                        )
                                    if (!SortMemory.fits(cost, memory.bytes)) {
                                        throw mergeRefusal(
                                            cost,
                                            kept.size,
                                            spillFiles.size.toLong(),
                                            spill,
                                            meter.written,
                                            spillFiles.size,
                                            demoted,
                                        )
                                    }
                                    headMaxima[index] = next
                                    trustedHeadBytes = heads
                                }
                            }
                        SurvivorReader(
                            run.input,
                            run.reader,
                            shape,
                            keys,
                            rowBudget,
                            spill.readerBudgetBytes,
                            run.profile!!.withMaxBinaryBytes(headCap),
                            decodeStats,
                            readerClosed = true,
                        ).also { open += it }
                    }
                }
            trustedRuns = readers.filterNotNull()
            ParquetRewriter.forEachOpenedReader(spillPath, parallelism) { input, reader ->
                reader.close()
                val rowBudget =
                    memory.RowBudget(input.label) { next, spent ->
                        if (chunk.isNotEmpty() &&
                            !SortMemory.fits(
                                SortMemory.add(
                                    chunkBytes, next, chunkWriterExtra, chunkReaderExtra, rangePayloadBytes,
                                ),
                                memory.bytes,
                            )
                        ) {
                            decodingBytes = spent
                            try {
                                spillChunk()
                            } finally {
                                decodingBytes = 0
                            }
                        }
                        if (!SortMemory.fits(SortMemory.add(next, chunkReaderExtra, rangePayloadBytes), memory.bytes)) {
                            throw MergeBudgetExceededException(
                                "reader excess and retained bounds exhaust the row allowance",
                                0,
                                0,
                                0,
                            )
                        }
                    }
                val profile = demotedProfiles.remove(input)
                if (profile != null) retainedProfileBytes -= profile.retainedMetadataBytes
                try {
                    SurvivorReader(
                        input,
                        reader,
                        shape,
                        keys,
                        rowBudget,
                        SortMemory.add(spill.readerBudgetBytes, memory.bytes),
                        readerProfile = profile,
                        decodeStats = decodeStats,
                        readerClosed = true,
                        reserveReader = ::reserveChunkReader,
                    ).use { survivors ->
                        while (survivors.hasNext()) appendChunk(survivors.next(), input.label)
                    }
                } finally {
                    chunkReaderExtra = 0
                }
            }

            if (spillFiles.isEmpty() && kept.isEmpty()) return writeSingleChunk()
            if (chunk.isNotEmpty()) spillChunk()
            // The chunk phase is over; its backing array must not sit on
            // the heap through the merge, which has its own budget.
            chunk = ArrayList()

            // Trusted runs that need no merging at all: appended byte for
            // byte, in key order, around the rows the merge produces.
            val appended = chooseAppended(kept)
            val blocks = appended.map { kept[it] }.sortedWith { a, b -> keys.compareFirst(a.range!!.lo, b.range!!.lo) }
            val runs = ArrayList<Iterator<Row>>()
            for ((index, reader) in readers.withIndex()) if (index !in appended) runs += checkNotNull(reader)
            for (file in spillFiles) {
                val source = LocalInputFile(file.path)
                val reader = file.profile.open(source, file.footer, source.newStream())
                if (decodeStats != null) decodeStats.readers++
                open += reader
                runs +=
                    SpillRows(
                        reader, file.footer.blocks.size, shape, keys, file.label,
                        memory.RowBudget(file.label, file.maxRowBytes), decodeStats,
                    )
            }

            var written = 0L

            fun countRows(count: Long) {
                val next = SortMemory.add(written, count)
                if (!SortMemory.fits(next, Long.MAX_VALUE)) {
                    throw InvalidDataException("sorted output row count is unrepresentable")
                }
                written = next
            }
            var minRowId: Long? = null
            var appendedGroups = 0
            var appendedBytes = 0L
            val (_, footer) =
                ParquetRewriter.writingTo(
                    output,
                    shape.schema,
                    codec,
                    outputTuning { extra ->
                        val cost =
                            SortMemory.add(
                                SortMemory.runBytes(SortMemory.add(trustedBytes, spilledBytes), trustedHeadBytes),
                                extra,
                                rangePayloadBytes,
                            )
                        if (!SortMemory.fits(cost, memory.bytes)) {
                            throw mergeRefusal(
                                cost,
                                trustedCount,
                                spillFiles.size.toLong(),
                                spill,
                                meter.written,
                                spillFiles.size,
                                demoted,
                            )
                        }
                        mergeWriterExtra = maxOf(mergeWriterExtra, extra)
                    },
                ) { writer ->
                    var nextBlock = 0

                    // An appended run's rows are one contiguous span of the
                    // merge order: the rows encoded so far precede it, so
                    // they are flushed as their own row group first.
                    fun append(run: Confirmed) {
                        countRows(run.reader.footer.blocks.saturatingSumOf { it.rowCount })
                        writer.appendRowGroups(run.input.source, run.reader, flushPending = true)
                        run.reader.close()
                        open.remove(run.reader)

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
                            keys.compareFirst(blocks[nextBlock].range!!.lo, head.row.key[0]) <= 0
                        ) {
                            if (keys.compareFirst(blocks[nextBlock].range!!.hi, head.row.key[0]) >= 0) {
                                throw InvalidDataException("a merged row overlaps a preproved byte-append range")
                            }
                            append(blocks[nextBlock++])
                        }
                        countRows(1)
                        writer.write(head.row)
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
            spill.memoryObserver?.invoke(
                "merge",
                mapOf(
                    "predictedRuns" to predictedRuns,
                    "reservationBytes" to
                        SortMemory.add(
                            SortMemory.runBytes(SortMemory.add(trustedBytes, spilledBytes), trustedHeadBytes),
                            rangePayloadBytes,
                        ),
                    "maxRowBytes" to (spillFiles.maxOfOrNull { it.maxRowBytes } ?: 0),
                    "totalRunBytes" to
                        SortMemory.add(
                            SortMemory.runBytes(SortMemory.add(trustedBytes, spilledBytes), trustedHeadBytes),
                            rangePayloadBytes,
                        ),
                    "decodedReaders" to (decodeStats?.readers ?: 0),
                    "decodedRows" to (decodeStats?.rows ?: 0),
                    "writerExtraBytes" to mergeWriterExtra,
                ),
            )
            return RewriteResult(
                written,
                minRowId,
                footer,
                runsTrusted = kept.size,
                runsSpilled = spillFiles.size,
                runsDemoted = demoted,
                spillBytes = meter.written,
                filesVerified = verified,
                filesUnsorted = unsorted,
                filesUnchecked = unchecked,
                sortCheckBytes = sortCheckBytes,
                rowGroupsAppended = appendedGroups,
                bytesAppended = appendedBytes,
                filesUnranged = unranged,
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
         *
         * That last refusal is COUNTED ([unranged], the ledger's
         * `files_unranged`), and only where it actually cost an append: when
         * at least one candidate passed [ParquetRewriter.appendRefusal] AND
         * has a known range strictly disjoint from every other RANGED run
         * and every spill file's — that is, a candidate that would have
         * appended had the unranged runs not been there. A debug line alone
         * left it invisible: a table whose sort key is a double appends
         * nothing, ever, and nothing said why. Counting in any other group
         * would report a cost nobody paid — with no candidate, or with every
         * candidate overlapping something known, nothing could have
         * appended anyway.
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
            val ranged = others.filter { it.range != null }

            // Disjoint from everything whose range IS known: the whole test
            // when every run is ranged, and the "would have appended" test
            // when one is not.
            fun disjointFromKnown(i: Int): Boolean {
                val mine = kept[i].range ?: return false
                return ranged.all { it === kept[i] || keys.disjoint(mine, it.range!!) } &&
                    spillRanges.all { keys.disjoint(mine, it) }
            }
            val unknown = others.firstOrNull { it.range == null }
            if (unknown != null) {
                // Counted as well as logged, but only when an append was
                // actually lost to it (see the doc above).
                if (candidates.any { disjointFromKnown(it) }) unranged = others.count { it.range == null }
                log.debug {
                    "sorted rewrite: no row group appended — ${unknown.input.label} gives no usable range for " +
                        "the first sort key (no footer statistics, a float/double key, or a key leaf of " +
                        "another type), so no run is known to be disjoint from it"
                }
                return emptySet()
            }
            val selected = candidates.filter { kept[it].input.byteAppend }
            for (index in selected) {
                val run = kept[index]
                if (!ranged.all { it === run || keys.disjoint(run.range!!, it.range!!) }) {
                    throw InvalidDataException("a byte-append range changed after its input proof")
                }
                if (!spillRanges.all { keys.disjoint(run.range!!, it) }) {
                    // A chunk can span a proved gap between different source ranges.
                    // The original input proof still binds this run.
                    // The merge checks its actual boundaries.
                    log.debug { "a spill range spans a preproved byte-append gap for ${run.input.label}" }
                }
            }
            return selected.toSet()
        }

        /** Prove byte copies before a decoded-reader preflight can reject them. */
        private fun prepareAppends(candidates: List<Input>): List<Input> {
            val trusted = candidates.filter { it.trustedSorted }
            if (trusted.none {
                    it.explicitRowIds && it.deletes == null && it.fileSizeBytes >= appendFloorBytes
                }
            ) {
                retainedFooterBytes =
                    candidates.saturatingSumOf { input ->
                        input.footer?.let { VariantLayoutGroups.footerBytes(it, input.source.length) } ?: 0L
                    }.coerceAtMost(VariantLayoutGroups.FOOTER_RETENTION_BYTES)
                return candidates
            }
            val ranges = java.util.IdentityHashMap<Input, ParquetRewriter.KeyRange?>()
            val retained = java.util.IdentityHashMap<Input, ParquetMetadata>()
            val appendable = java.util.Collections.newSetFromMap(java.util.IdentityHashMap<Input, Boolean>())
            var footerBytes = 0L

            fun inspect(
                input: Input,
                footer: ParquetMetadata,
            ) {
                ranges[input] = keys.footerRange(footer)
                if (input.trustedSorted &&
                    ParquetRewriter.appendRefusal(input, footer, shape.schema, appendFloorBytes) == null
                ) {
                    appendable += input
                }
                val size = VariantLayoutGroups.footerBytes(footer, input.source.length)
                if (SortMemory.fits(
                        size,
                        SortMemory.remaining(VariantLayoutGroups.FOOTER_RETENTION_BYTES, footerBytes),
                    )
                ) {
                    retained[input] = footer
                    footerBytes = SortMemory.add(footerBytes, size)
                }
            }

            fun inspectAll(inputs: List<Input>) {
                for (input in inputs.filter { it.footer != null }) inspect(input, input.footer!!)
                ParquetRewriter.forEachOpenedInput(inputs.filter { it.footer == null }, parallelism) { input, reader ->
                    inspect(input, reader.footer)
                }
            }
            inspectAll(trusted)
            if (appendable.isNotEmpty()) inspectAll(candidates.filterNot { it.trustedSorted })
            retainedFooterBytes = footerBytes
            return candidates.map { input ->
                val range = ranges[input]
                val append =
                    input in appendable && range != null &&
                        candidates.all { other ->
                            other === input || ranges[other]?.let { keys.disjoint(range, it) } == true
                        }
                input.copy(footer = retained[input] ?: input.footer, byteAppend = append)
            }
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
                ParquetRewriter.forEachOpenedReader(checked, parallelism) { input, reader ->
                    reader.close()
                    val counter = counters[next]
                    val index = candidates[next++].index
                    var limited = false
                    val sorted =
                        try {
                            SortednessCheck.isSorted(
                                input,
                                reader,
                                keys.fields,
                                memory,
                                spill.readerBudgetBytes,
                                decodeStats,
                            )
                        } catch (_: MergeBudgetExceededException) {
                            limited = true
                            false
                        } catch (_: SortedReaderMemoryException) {
                            limited = true
                            false
                        }
                    if (sorted) {
                        passed[index] = true
                        verified++
                    } else if (limited) {
                        unchecked++
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
                sortCheckBytes = counters.saturatingSumOf { it.bytesRead }
            }
            return inputs.mapIndexed { i, input -> if (passed[i]) input.copy(trustedSorted = true) else input }
        }

        /**
         * Open admitted trusted runs without decoding rows.
         * Recalculate each reader charge from projected page headers.
         * Demote over-budget runs before the chunk phase.
         * Close each demoted reader.
         * No data row is read here.
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
            // Rebuild each suffix independently of earlier saturation, with one reverse pass.
            val pendingAfter = LongArray(admission.admitted.size)
            var pending = 0L
            for (index in admission.admitted.indices.reversed()) {
                pendingAfter[index] = pending
                pending = SortMemory.add(pending, estimatedTrustedRunBytes(admission.admitted[index], leaves, spill))
            }
            var demotedRows = admission.demotedRows
            var keptBytes = 0L
            var keptDecoded = 0L
            val kept = ArrayList<Confirmed>()
            for ((index, input) in admission.admitted.withIndex()) {
                val reader = readers[index]
                val profile =
                    if (input.byteAppend) {
                        null
                    } else {
                        try {
                            val projected = ParquetRewriter.projectedRead(input, reader.footer, shape)
                            ParquetRewriter.readerProfile(
                                input,
                                reader.footer,
                                projected.schema,
                                SortMemory.add(spill.readerBudgetBytes, memory.bytes),
                                memory.bytes,
                            )
                        } catch (error: SortedReaderMemoryException) {
                            // This exceeds the combined reader/row envelope, not merely merge admission.
                            throw SortMemory.readerFailure(error)
                        }
                    }
                val exact = if (input.byteAppend) 0 else profile?.chargeBytes ?: Long.MAX_VALUE
                val runs = spillRuns(SortMemory.add(admission.untrustedRows, demotedRows), spill)
                val projected =
                    mergeCost(
                        Math.addExact(kept.size, 1),
                        SortMemory.runBytes(
                            SortMemory.add(keptBytes, exact, pendingAfter[index]),
                            SortMemory.multiply(
                                SortMemory.add(keptDecoded, if (input.byteAppend) 0 else 1),
                                estimatedHeadBytes(leaves),
                            ),
                        ),
                        runs,
                        SortMemory.multiply(runs, perSpilledRun),
                    )
                if (SortMemory.fits(projected, spill.mergeBudgetBytes)) {
                    val footer = reader.footer
                    reader.close()
                    open.remove(reader)
                    val clean =
                        if (input.byteAppend) {
                            ParquetReaders.open(
                                input.source,
                                footer,
                                ParquetRewriter.dataStream(input.source),
                            ).also { open += it }
                        } else {
                            reader
                        }
                    kept += Confirmed(input, clean, exact, keys.footerRange(footer), profile)
                    keptBytes = SortMemory.add(keptBytes, exact)
                    if (!input.byteAppend) keptDecoded = SortMemory.add(keptDecoded, 1)
                } else {
                    if (profile != null &&
                        SortMemory.fits(
                            profile.retainedMetadataBytes,
                            SortMemory.remaining(
                                VariantLayoutGroups.FOOTER_RETENTION_BYTES, retainedFooterBytes, retainedProfileBytes,
                            ),
                        )
                    ) {
                        demotedProfiles[input] = profile
                        retainedProfileBytes = SortMemory.add(retainedProfileBytes, profile.retainedMetadataBytes)
                    }
                    open.remove(reader)
                    runCatching { reader.close() }
                    demoted++
                    demotedRows = SortMemory.add(demotedRows, input.survivingRecords)
                }
            }
            // Each check above assumed the runs after it would be kept at
            // their estimates; a later demotion moves rows to the spill
            // path, so the final shape is checked once more as a whole.
            // It is also where a group admission could not fit is refused:
            // admission shed every run, so nothing above was opened.
            val runs = spillRuns(SortMemory.add(admission.untrustedRows, demotedRows), spill)
            val exactCost =
                mergeCost(
                    kept.size,
                    SortMemory.runBytes(keptBytes, SortMemory.multiply(keptDecoded, estimatedHeadBytes(leaves))),
                    runs,
                    SortMemory.multiply(runs, perSpilledRun),
                )
            if (!SortMemory.fits(exactCost, spill.mergeBudgetBytes)) {
                throw mergeRefusal(exactCost, kept.size, runs, spill, meter.written, spillFiles.size, demoted)
            }
            return kept
        }

        /** Do not retain an old loop-local row across the next materialization. */
        private fun appendChunk(
            row: Row,
            label: String,
        ) {
            val extra = spillWriterSizer.extraBytes(row)

            // A deleted tail does not turn an exact row boundary into a spill.
            if (chunk.isNotEmpty() &&
                (
                    chunk.size >= spill.chunkRows ||
                        !SortMemory.fits(
                            SortMemory.add(
                                chunkBytes,
                                row.memoryBytes,
                                maxOf(chunkWriterExtra, extra),
                                chunkReaderExtra,
                                rangePayloadBytes,
                            ),
                            memory.bytes,
                        )
                )
            ) {
                decodingBytes = row.memoryBytes
                try {
                    spillChunk()
                } finally {
                    decodingBytes = 0
                }
            }
            if (!SortMemory.fits(SortMemory.add(row.memoryBytes, chunkReaderExtra, rangePayloadBytes), memory.bytes)) {
                throw MergeBudgetExceededException(
                    "reader excess and retained bounds exhaust the row allowance",
                    0,
                    0,
                    0,
                )
            }
            chunk.add(row)
            chunkBytes = SortMemory.add(chunkBytes, row.memoryBytes)
            // A forecast can request a spill, but cannot alone refuse one legal row.
            chunkWriterExtra =
                maxOf(
                    chunkWriterExtra,
                    minOf(
                        extra,
                        SortMemory.remaining(memory.bytes, row.memoryBytes, chunkReaderExtra, rangePayloadBytes),
                    ),
                )
            chunkLabels.add(label)
        }

        /** No trusted run and one chunk: sort it and write it as the output, touching no disk. */
        private fun writeSingleChunk(): RewriteResult {
            sortChunk()
            var minRowId: Long? = null
            val (_, footer) =
                ParquetRewriter.writingTo(output, shape.schema, codec, outputTuning(::reserveChunkWriter)) { writer ->
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

        private fun reserveChunkWriter(extra: Long) {
            if (!SortMemory.fits(
                    SortMemory.add(chunkBytes, decodingBytes, extra, chunkReaderExtra, rangePayloadBytes),
                    memory.bytes,
                )
            ) {
                throw MergeBudgetExceededException(
                    "writer buffers exceed the retained-row allowance",
                    0,
                    0,
                    0,
                    oversizedRow =
                        chunk.size == 1 &&
                            !SortMemory.fits(SortMemory.add(chunk.first().memoryBytes, extra), memory.bytes),
                )
            }
        }

        /** Install excess only after existing rows can spill without the new reader. */
        private fun reserveChunkReader(readerBytes: Long) {
            if (!SortMemory.fits(readerBytes, Long.MAX_VALUE)) {
                throw MergeBudgetExceededException("decoded reader size is unrepresentable", 0, 0, 0)
            }
            val extra = (readerBytes - spill.readerBudgetBytes).coerceAtLeast(0)
            if (chunk.isNotEmpty() &&
                !SortMemory.fits(SortMemory.add(chunkBytes, chunkWriterExtra, extra, rangePayloadBytes), memory.bytes)
            ) {
                spillChunk()
            }
            if (!SortMemory.fits(SortMemory.add(extra, rangePayloadBytes), memory.bytes)) {
                throw MergeBudgetExceededException("decoded reader exhausts the retained-row allowance", 0, 0, 0)
            }
            chunkReaderExtra = extra
            spill.memoryObserver?.invoke(
                "reader",
                mapOf(
                    "readerBytes" to readerBytes,
                    "structuralReserveBytes" to spill.readerBudgetBytes,
                    "extraBytes" to extra,
                    "retainedBytes" to chunkBytes,
                ),
            )
        }

        private fun outputTuning(reserve: (Long) -> Unit): ParquetRewriter.WriterTuning =
            ParquetRewriter.WriterTuning(
                OUTPUT_BLOCK_BYTES,
                ParquetProperties.DEFAULT_PAGE_SIZE,
                ParquetProperties.DEFAULT_MAXIMUM_RECORD_COUNT_FOR_CHECK,
                writerMemory =
                    WriterMemory(
                        memory, shape.schema, codec, OUTPUT_BLOCK_BYTES + 16L * 1024 * 1024,
                        OUTPUT_BLOCK_BYTES / ParquetProperties.DEFAULT_MAXIMUM_RECORD_COUNT_FOR_CHECK,
                        ParquetProperties.DEFAULT_PAGE_SIZE.toLong(),
                        retainFooter = chunk.isEmpty(),
                        decimalWidths = keys.memoryDecimalWidths,
                        availableExtra = {
                            val retained =
                                if (chunk.isEmpty()) {
                                    SortMemory.runBytes(SortMemory.add(trustedBytes, spilledBytes), trustedHeadBytes)
                                } else {
                                    SortMemory.add(chunkBytes, decodingBytes, chunkReaderExtra)
                                }
                            SortMemory.remaining(memory.bytes, retained, rangePayloadBytes)
                        },
                        reserve = reserve,
                    ),
                tuneChecks = false,
            )

        /** Comparisons are on extracted keys: a bad key value was refused when its row was read. */
        private fun sortChunk() = chunk.sortWith(keys.comparator)

        private fun spillChunk() {
            sortChunk()
            // Sorted by the full key, so the first key's extremes are the
            // chunk's first and last rows.
            val first = chunk.first().key[0]
            val last = chunk.last().key[0]
            if (listOf(first, last).all { it !is ByteArray || it.size < ParquetMetadataConverter.MAX_STATS_SIZE }) {
                spillRanges += ParquetRewriter.KeyRange(first, last)
                rangePayloadBytes =
                    SortMemory.add(
                        rangePayloadBytes,
                        (first as? ByteArray)?.size?.toLong() ?: 0,
                        if (last === first) 0 else (last as? ByteArray)?.size?.toLong() ?: 0,
                    )
            }
            val n = spillFiles.size + 1
            val dir =
                spillDir
                    ?: Files.createDirectory(spill.spillDir.resolve("${SpillDirectory.PREFIX}${UUID.randomUUID()}"))
                        .also { spillDir = it }
            val path = dir.resolve("spill-$n.parquet")
            var maxRowBytes = 0L
            val (_, written) =
                ParquetRewriter.writingTo(
                    MeteredSpillFile(path, meter),
                    shape.schema,
                    SPILL_CODEC,
                    ParquetRewriter.WriterTuning(
                        rowGroupBytes = spill.spillBlockBytes.toLong(),
                        pageBytes = SPILL_PAGE_BYTES,
                        maxRowsPerSizeCheck = SPILL_MAX_ROWS_PER_SIZE_CHECK,
                        measureRows = memory,
                        keyDecimalWidths = keys.memoryDecimalWidths,
                        writerMemory =
                            WriterMemory(
                                memory, shape.schema, SPILL_CODEC, 2L * spill.spillBlockBytes,
                                spill.spillBlockBytes.toLong() / SPILL_MAX_ROWS_PER_SIZE_CHECK,
                                SPILL_PAGE_BYTES.toLong(),
                                decimalWidths = keys.memoryDecimalWidths,
                                availableExtra = {
                                    val retained =
                                        SortMemory.add(
                                            chunkBytes,
                                            decodingBytes,
                                            chunkReaderExtra,
                                            rangePayloadBytes,
                                        )
                                    SortMemory.remaining(memory.bytes, retained)
                                },
                                reserve = ::reserveChunkWriter,
                            ),
                    ),
                ) { writer ->
                    for (row in chunk) writer.write(row)
                    maxRowBytes = writer.maxRowBytes
                }
            completedSpills++
            val footer = written ?: ParquetReaders.open(LocalInputFile(path)).use { it.footer }
            val profile =
                try {
                    SortedReaderMemory.profile(
                        LocalInputFile(path),
                        footer,
                        shape.schema,
                        spill.mergeBudgetBytes,
                        maxRowBytes,
                    )
                } catch (error: SortedReaderMemoryException) {
                    throw SortMemory.readerFailure(error)
                }
            spillFiles += SpillFile(path, "spill-$n (${describe(chunkLabels)})", maxRowBytes, footer, profile)
            chunk.clear()
            chunkBytes = 0
            chunkWriterExtra = 0
            chunkLabels.clear()
            // The EXACT cost of the run just written, off its own footer:
            // the predicted cost was the block, and the merge will hold
            // what was written, not what was intended. Checked here, before
            // the merge opens anything, so a run count the registered
            // survivors under-predicted is caught at the run that breaks it.
            spilledBytes = SortMemory.add(spilledBytes, SortMemory.runBytes(profile.chargeBytes, maxRowBytes))
            val cost = SortMemory.add(trustedBytes, spilledBytes, rangePayloadBytes)
            spill.memoryObserver?.invoke(
                "spill",
                mapOf(
                    "rows" to footer.blocks.saturatingSumOf { it.rowCount },
                    "maxRowBytes" to maxRowBytes,
                    "exactRunBytes" to SortMemory.runBytes(profile.chargeBytes, maxRowBytes),
                    "totalRunBytes" to cost,
                ),
            )
            if (!SortMemory.fits(cost, spill.mergeBudgetBytes)) {
                throw mergeRefusal(cost, trustedCount, n.toLong(), spill, meter.written, spillFiles.size, demoted)
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
