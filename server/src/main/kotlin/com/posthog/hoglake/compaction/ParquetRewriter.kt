package com.posthog.hoglake.compaction

import com.posthog.hoglake.ParquetReaders
import com.posthog.hoglake.hydrator.FooterStats
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.model.allNodes
import com.posthog.hoglake.model.maxUnsignedParquetWidth
import com.posthog.hoglake.service.Identifiers
import org.apache.parquet.column.Dictionary
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData
import org.apache.parquet.hadoop.metadata.ColumnPath
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.OutputFile
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.Converter
import org.apache.parquet.io.api.GroupConverter
import org.apache.parquet.io.api.PrimitiveConverter
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types
import java.math.BigInteger
import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit

/**
 * A compaction group whose inputs cannot be rewritten under the live
 * schema: some live column's type cannot be produced from an input
 * file's parquet type (anything outside identity or the int->long /
 * float->double promotions). The sweep treats this as skip-with-reason
 * (CompactionResult.unconvertibleSchema), never a failure.
 */
class UnconvertibleSchemaException(message: String) : IllegalArgumentException(message)

/**
 * A compaction group whose SCHEMA is convertible but whose DATA is not:
 * a value that cannot exist under the type its own file declares (an
 * empty byte array under a decimal, an unscaled value wider than the
 * destination precision), or a row so large it would exhaust the heap.
 *
 * Distinct from [UnconvertibleSchemaException] on the axis that matters
 * operationally: a schema skip clears when the schema or the file set
 * moves, so a re-plan is free. A data skip does NOT clear — the bad
 * bytes are durable — so retrying it hot is a permanent loop over the
 * same failing rows. Both are skip-with-reason; this one is counted
 * separately (CompactionResult.invalidData) because a nonzero count
 * means a WRITER is producing values its own schema forbids, and that
 * is a bug report, not a compaction backlog.
 *
 * DURABILITY AND FAULT, not values-versus-schema, is what that axis
 * measures — which is why a file disagreeing with its
 * `explicit_row_ids` registration lands here too, even though the
 * disagreement is in the SCHEMA. A reserved field id burned into a
 * registered parquet never clears: the group is re-planned and
 * re-refused every sweep forever. Filing it under
 * `unconvertible_schema` made it read as a backlog awaiting a schema
 * change that is never coming, while `invalid_data` — the counter that
 * exists to say "a writer produced something its own registration
 * forbids" — stayed at zero.
 *
 * Before this existed these threw raw NumberFormatException out of
 * BigInteger and landed in the catch-all as failed_groups, which retried
 * them every run forever.
 */
class InvalidDataException(message: String) : IllegalArgumentException(message)

/**
 * The compaction rewrite writer, on parquet-java — the project's one
 * parquet library (decision 2026-09-05: Hardwood is out entirely).
 * Compaction outputs MUST carry field ids on every column (files bind
 * to catalog columns by id, never by name; field ids are a registration
 * contract, enforced at hydration via
 * hog_data_file.missing_field_ids + the AlterService rename guard).
 *
 * What a rewrite does: read every input file's rows, APPLY each input's
 * live deletion vector (skip the deleted physical ordinals), re-shape
 * the survivors under the table's LIVE schema, concatenate them in
 * row-id order, and write one output file in which each row's hoglake
 * row id rides an explicit physical int64 column [ROW_ID_COLUMN]
 * (reserved field id [ROW_ID_FIELD_ID]). Explicit ids are the point:
 * merged inputs need not be row-id-contiguous — and DV application
 * punches holes inside a file's range — so the output can never rely on
 * positional ids (row_id_start + offset), and, because every row
 * carries its id, reordering the merged rows by the table's sort order
 * is SAFE. This kills the predecessor's sorted-compaction hazard
 * (DuckLake's sorted merge_adjacent_files silently REMAPPED rowids,
 * breaking CDC identity downstream; hoglake row ids survive any
 * ordering because they are data, not position).
 *
 * **A sorted rewrite is an external merge sort** (hoglake#134). The
 * in-memory sort it replaced held every survivor of the group as a
 * `Group` graph at once — heap O(group), measured at up to 70x the
 * compressed bytes for nested rows — so the planner had to bound sorted
 * groups in ROWS and a dense sorted table never reached its byte target.
 * Now neither phase of a sorted rewrite grows with the group:
 *
 *  - CHUNK phase: survivors of the spill-path inputs are read in input
 *    order into a chunk of at most [SortSpill.chunkRows] rows (chunks
 *    span input boundaries), sorted by the merge key, and written to a
 *    local parquet file in the OUTPUT schema (SNAPPY, row groups of
 *    [SortSpill.spillBlockBytes]). Heap: one chunk.
 *  - MERGE phase: a k-way merge of RUNS through a priority queue into the
 *    output. Every run is pull-style on the caller's thread — no threads,
 *    no handoff. Heap: one row group per run, plus readahead.
 *
 * Three kinds of run, and only the last costs a spill:
 *
 *  - METADATA-TRUSTED: an input the caller marks [Input.trustedSorted] —
 *    a compaction output written under the live sort spec — read in
 *    place, in file order, through the same per-input pipeline as every
 *    input (DV applied, live schema, row id from its carrier). Nothing
 *    checks its order: a trusted run that is not key-sorted yields a
 *    mis-sorted output, not an error.
 *  - VERIFIED: any other input of at least [SortSpill.verifyMinBytes]
 *    whose rows the sortedness PRE-PASS ([SortednessCheck]) found already
 *    in merge-key order (a smaller file is cheapest on the chunk path,
 *    and is not checked: `files_unchecked`). The pre-pass reads only
 *    the sort-key columns (a projected schema; the row-id carrier too
 *    for an explicit-row-id file), every physical row, deleted or not,
 *    and stops at the first row out of order. A file that passes is a
 *    trusted candidate exactly like a metadata-trusted one — same
 *    admission, same footer confirmation — and is read in place by the
 *    merge; one that fails is spilled, which is not an error. This is
 *    what makes the merge STREAMING for writers that already sort
 *    (millpond's flushes): their files are never spilled. The pre-pass
 *    runs through the open window ([inputOpenParallelism] files at
 *    once, the check on the calling thread); each slot holds a footer,
 *    one row group's PROJECTED column chunks and the key source's small
 *    readahead, before admission and outside the merge budget.
 *  - SPILLED: one chunk file of the remaining inputs, read back as the
 *    `Group`s it holds — its schema is the output schema by
 *    construction, so there is no column plan and no second
 *    [maxNodesPerRow] charge.
 *
 * A group with no admitted run whose survivors fit one chunk is the
 * degenerate merge: its chunk is sorted and written straight to the
 * output, and nothing touches disk.
 *
 * Budgets. parquet-java's `readNextRowGroup()` allocates a reader's WHOLE
 * compressed row group, and each column reader then holds one decoded
 * page, so a run costs its largest row group plus a page per leaf plus
 * its stream buffer — the formula, exact from a footer and estimated
 * before one, is in [ExternalMergeSort]'s doc. Trusted runs are outputs
 * written at the 128 MiB default block; spill files are written at
 * [SortSpill.spillBlockBytes] with small pages and a row-group size
 * check on every few rows, so the block actually bounds them. [SortSpill.mergeBudgetBytes] bounds both
 * phases, which never overlap. Before any data is read, trusted inputs
 * are admitted LARGEST FIRST while the projected merge cost — admitted
 * runs plus every spilled run the other survivors will make — fits; the
 * rest are DEMOTED to the spill path (and the smallest admitted runs are
 * shed when a later demotion's rows no longer fit beside them), which
 * changes work, never correctness. Admitted runs' footers are then read and their EXACT cost
 * re-checked, demoting again, still before any row group is read. A
 * group that does not fit even fully demoted is refused
 * ([MergeBudgetExceededException]); so is one whose spill would pass
 * [SortSpill.spillBudgetBytes] ([SpillBudgetExceededException]), from
 * the registered sizes up front and from the bytes actually written
 * mid-run, because an emptyDir overrun evicts the pod instead of failing
 * the write.
 *
 * Order. The merge key is the sort keys, then row id ascending; row ids
 * are unique per table and each key comparison is total per physical
 * type, so the key is a total order. KEY order of the output is exact
 * whenever every trusted run is key-nondecreasing. TIE order is
 * (key, row id) when every trusted input was itself written in that
 * order — true for every output this rewriter writes — and otherwise
 * deterministic but unspecified: outputs written before #134 broke ties
 * in INPUT order, which differs from row-id order when one of their own
 * inputs was a compaction output.
 *
 * DV check timing. Every run checks at its EXHAUSTION that each DV
 * position fell inside the file. A trusted run is exhausted at the end
 * of the merge, after rows have streamed into the output, so a lying DV
 * now aborts late; [rewrite]'s discard covers the partial output, and
 * the exception is the same IllegalStateException as before.
 *
 * Spill directory. One `hoglake-compaction-spill-<uuid>` per sorted rewrite under
 * [SortSpill.spillDir], created at the first spill and deleted — after
 * every reader is closed — on every exit path, success or not.
 *
 * **Appended row groups** (hoglake#134 package D1). An input that is a
 * prior compaction output of this very output schema, with no live DV,
 * `explicit_row_ids`, row groups of at least [APPEND_MIN_ROW_GROUP_BYTES]
 * and row-id statistics ([appendRefusal]) is not decoded at all: its row
 * groups are copied into the output byte for byte, indexes, bloom filters
 * and codec included ([OutputWriter.appendRowGroups]). The unsorted path
 * appends every such input; the sorted path appends a trusted run only
 * when its first-key range is strictly disjoint from every other run's
 * and every spill file's (see `ExternalMergeSort`'s `chooseAppended`), and
 * emits it in key order between the merged rows. The output then mixes
 * appended and encoded row groups — and possibly codecs — which is legal
 * parquet. Because an appended row skips the column plan, anything the
 * decode path would do to a value of an identical schema (today: nothing)
 * would have to become a disqualifier.
 *
 * **No output-shaped copy** (package D2). A decoded row is written by its
 * [RowPlan] straight into parquet's record consumer — the plan's
 * null-fills, drops, promotions and the row id applied on the way — so a
 * row is materialized once, as decoded. Each input is read PROJECTED to
 * the columns its plan maps plus the row-id carrier ([projectRead]), so a
 * column the live schema dropped is never decoded: the chunk phase holds
 * decoded rows, and its size is computed from the live schema's nodes.
 *
 * Heterogeneous inputs (files written across ALTERs) map to the live
 * schema by FIELD ID: a live column absent from an input null-fills; an
 * input column whose field id the live schema no longer knows (dropped
 * column) drops its data; promoted columns up-cast (int32->int64,
 * float->double). Id-less input columns (pre-field-id writers) fall
 * back to live-name matching. The one refusal is a live column whose
 * type cannot be produced from the input's physical type —
 * [UnconvertibleSchemaException], the group stays uncompacted. Because
 * the output must hold every live column (null-filled ones included),
 * all data columns are written OPTIONAL; catalog nullability is
 * metadata, not parquet repetition.
 *
 * **Nested columns rewrite, they are not copied around.** The record
 * pipeline above is already the parquet-java Group API, and a `Group`
 * is a tree: `GroupRecordConverter` materializes the whole nested
 * record and `getGroup` reaches into it. So list, struct and map extend
 * the SAME plan-and-write shape one level at a time — the plan becomes a
 * tree of [Step]s instead of a flat array — rather than needing a
 * copy-only escape hatch. That matters: making nested tables
 * `unconvertible_schema` would have meant a table with one `map` column
 * could never be compacted, which is a permanent debt leak, not a
 * deferral. The cost is per-ROW heap proportional to the nested payload
 * (the unsorted path still holds exactly one Group at a time) and a
 * recursive walk per row. The walk writes the OUTPUT record straight from
 * the decoded source row ([RowPlan]); there is no output-shaped copy.
 *
 * Nested structure is spec-shaped on both sides: the 3-level LIST
 * encoding (`optional group x (LIST) { repeated group list { optional
 * <t> element } }`) and the MAP encoding (`optional group x (MAP) {
 * repeated group key_value { required <k> key; optional <v> value } }`).
 * Inputs are matched by SHAPE, not by the synthetic names, because the
 * parquet spec says those names are not significant — but an input whose
 * shape disagrees with the live column's type is
 * [UnconvertibleSchemaException], never a guess.
 */
object ParquetRewriter {
    /** The explicit row-id column compaction outputs carry. */
    const val ROW_ID_COLUMN = "_hog_row_id"

    /**
     * Run [body] against a writer on [output], and make sure a failure
     * DISCARDS the destination rather than finishing it.
     *
     * The ordering is the whole point, and `use {}` cannot express it.
     * On the exception path `use` would close the writer before the
     * exception reaches any catch of ours — and a close flushes the
     * pending row group and writes a valid footer and `PAR1`. For a
     * streaming sink that is indistinguishable from success: the trailer
     * is there, so the multipart upload completes and a truncated-but-
     * well-formed parquet file is published. The rows are simply missing.
     *
     * So the sink is poisoned FIRST and the writer ABORTED afterwards —
     * no footer, buffers released, the stream closed, which the poisoned
     * sink has been told not to complete.
     */
    internal inline fun <T> writingTo(
        output: OutputFile,
        outputSchema: MessageType,
        codec: OutputCodec,
        tuning: WriterTuning? = null,
        body: (OutputWriter) -> T,
    ): Pair<T, ParquetMetadata?> {
        // Inside the guard too: the writer calls output.createOrOverwrite,
        // which for a streaming sink STARTS the upload — so a throw
        // between that and the first write would leave one dangling.
        val writer =
            try {
                OutputWriter(output, outputSchema, codec, tuning)
            } catch (e: Throwable) {
                (output as? DiscardableOutputFile)?.discard()
                throw e
            }
        val result =
            try {
                body(writer)
            } catch (e: Throwable) {
                (output as? DiscardableOutputFile)?.discard()
                runCatching { writer.abort() }
                throw e
            }
        // NOT inside the try: close() is what completes the upload, so a
        // failure here means the object never landed whole, and it must
        // discard for the same reason a write failure does.
        try {
            writer.close()
        } catch (e: Throwable) {
            (output as? DiscardableOutputFile)?.discard()
            throw e
        }
        // The written footer, for the output's row-group offsets. Asked
        // for only after a successful close (parquet-java refuses an
        // unfinished one) and never allowed to fail the rewrite: the
        // offsets are optional metadata, and the file is already whole.
        return result to runCatching { writer.footer }.getOrNull()
    }

    /**
     * Reserved parquet field id for [ROW_ID_COLUMN] (documented in
     * V1__init.sql on hog_data_file.explicit_row_ids and in AGENT.md):
     * Int.MAX_VALUE - 1, far outside hog_table.next_field_id's reach.
     */
    const val ROW_ID_FIELD_ID = 2147483646

    /**
     * Default per-ROW node budget: how many parquet-java `Group`/value
     * nodes one input row may materialize before the group is refused as
     * [InvalidDataException].
     *
     * Both rewrite paths materialize a row whole — one list element is
     * one `SimpleGroup` with a header, a field array and a boxed value,
     * roughly 50-100 bytes of heap — so a single row with a
     * hundred-million-element list is an OOM, and an OOM in a background
     * loop is process-fatal, not group-fatal: it takes the whole server
     * down with it, including the request path. Nothing upstream bounds
     * a row's element count (the commit path has no such limit), so the
     * bound lives here.
     *
     * Spent DURING the decode ([budgetedMaterializer]), where the bound
     * has to act: `recordReader.read()` builds the whole row before it
     * returns, so a budget charged afterwards reports an allocation
     * rather than preventing one — which is what the first version did,
     * and the OOM happened anyway. `BudgetOomRepro` demonstrates both
     * halves in a small-heap JVM: at THIS default an eight-million
     * -element row exhausts a 512 MB heap on the unbudgeted read path
     * and refuses cleanly here. Nothing else is charged: the output
     * record is walked out of the decoded row ([RowPlan]), which visits
     * each decoded node at most once and allocates none, so the walk
     * cannot exceed what the decode was allowed. (While a copy existed it
     * was charged from a FRESH allowance — sharing one across both phases
     * charged the same graph twice and silently halved the ceiling.)
     *
     * NODES, which is the unit to calibrate in. A scalar column costs 1
     * per row; a list element costs 2 (its synthetic entry group plus
     * the value), a map entry 3. So a million admits roughly half a
     * million list elements or a third of a million map entries in one
     * row — still far above any honest row, and far below what a 512 MB
     * heap can hold at ~50-100 bytes a node. The point is to convert a
     * process kill into one counted skip, not to police row shape.
     *
     * PEAK LIVE HEAP IS ONE ROW'S GRAPH, 1x the budget. It was up to 2x
     * while every row was copied into an output graph beside the decoded
     * one (before hoglake#134 package D2); measured then, a 999,999-node
     * row rewrote under `-Xmx192m`, so the bound is comfortably inside
     * any heap this server runs with.
     */
    const val DEFAULT_MAX_NODES_PER_ROW = 1_000_000

    /**
     * How many of a group's inputs [forEachOpenedInput] may have OPEN at
     * once (`HOGLAKE_COMPACTION_PARALLEL_INPUT_OPENS`).
     *
     * 1 here — off — because this is the LIBRARY default, and every unit
     * test and every caller that did not ask for concurrency gets
     * exactly the old single-threaded shape. `Config` supplies the
     * production default (8), and `CompactionService` is the only caller
     * that passes one.
     *
     * The quantity being overlapped is LATENCY, not bytes: opening a
     * parquet input costs at least one object-store round trip before a
     * row can be read, and a 64-file group paid 64 of them end to end —
     * which is why compaction's per-group cost was measured FLAT in
     * group size (~8.5 s) on gigahog-prod-us. See [forEachOpenedInput]
     * for what the window does and does not bound.
     */
    const val DEFAULT_INPUT_OPEN_PARALLELISM = 1

    /**
     * How long [forEachOpenedInput] waits for one prefetched `open` to
     * finish so it can close its reader, when the group is unwinding.
     *
     * Bounded because the alternative is a hung object-store call
     * turning a failed group into a hung sweep; two seconds is longer
     * than any healthy open and shorter than the background loop's own
     * shutdown cap.
     */
    const val OPEN_DRAIN_MILLIS = 2_000L

    /**
     * The smallest row group an input may have and still be APPENDED to
     * the output byte for byte instead of decoded and re-encoded
     * (hoglake#134 package D1; see [appendRefusal]). 32 MiB.
     *
     * A floor because appending keeps the input's row groups exactly as
     * they are, and the point of compacting small files is to produce big
     * row groups: a prior output of a few MiB appended as-is would carry
     * its small row group into every later scan, and into the next
     * compaction, forever. Re-encoding it merges it into a full one. At
     * 32 MiB — a quarter of the 128 MiB block every output is written at —
     * a row group is already large enough that scans are not paying for
     * it, while the append is still the 8-9x cheaper path (measured: 128-151
     * ms to append a same-schema group against 1,162 ms to decode and
     * re-encode it). Below it, the bytes are worth re-encoding.
     */
    const val APPEND_MIN_ROW_GROUP_BYTES: Long = 32L * 1024 * 1024

    /**
     * The synthetic repeated-group names the parquet LIST and MAP
     * encodings use. Written, never required on read: the parquet spec
     * says these names are not significant, and writers disagree about
     * them, so inputs are matched by SHAPE.
     */
    const val LIST_ENTRY_GROUP = "list"
    const val MAP_ENTRY_GROUP = "key_value"

    /**
     * parquet-java's zstd level key, and the level this rewriter pins by
     * default. Named rather than typed because the value travels to the
     * codec through the writer's untyped configuration map.
     *
     * Level 1, not parquet-java's 3 (hoglake#134 package D): a JFR profile
     * of the rewrite put zstd at 11-16% of its CPU on the event shape and
     * 41-48% on JSON-heavy rows at level 3, level 1 cut the rewrite's wall
     * time by ~12% and ~20-25% respectively, and level 3 bought no
     * measurable size over level 1 on that data (`CodecMeasurement` prints
     * both). Level 1 still decompresses at the same speed, which is what
     * every later scan pays.
     */
    const val ZSTD_LEVEL_KEY = "parquet.compression.codec.zstd.level"
    const val DEFAULT_ZSTD_LEVEL = 1
    const val MIN_ZSTD_LEVEL = 1
    const val MAX_ZSTD_LEVEL = 22

    /** See [OutputCodec]. */
    val DEFAULT_CODEC: CompressionCodecName = CompressionCodecName.ZSTD

    /** See [OutputCodec]: readable by every consumer, implemented on the classpath. */
    val SUPPORTED: List<CompressionCodecName> =
        listOf(
            CompressionCodecName.ZSTD,
            CompressionCodecName.SNAPPY,
            CompressionCodecName.GZIP,
            CompressionCodecName.LZ4_RAW,
            CompressionCodecName.UNCOMPRESSED,
        )

    /**
     * The compression codec a compaction output is written with.
     *
     * The default is ZSTD, and the reason is that compaction is the one
     * writer in this system that rewrites rows it did not write. Every
     * byte a client ingests is decoded and re-encoded here on its way to
     * a target-sized file, and stays in that encoding for the rest of
     * its life, so an output codec is not a per-file choice: it is the
     * codec a compacted table is stored and scanned under. UNCOMPRESSED
     * — which is what this used to be, inherited from
     * `ExampleParquetWriter`'s default rather than chosen — made
     * compaction a one-way decompressor: clients write snappy
     * (pyarrow's default, and DuckDB's, which is what pyhoglake and the
     * duckdb-client produce) or zstd (hedgerow's explicit COPY option),
     * and every merge threw that away permanently. A dev-catalog run
     * merged 67.6 MiB of inputs into 80.1 MiB of output.
     *
     * ZSTD over SNAPPY because the cost sits on the side that is paid
     * once. Compression happens once per rewrite; the output is then
     * read by every scan, by any later rewrite, and paid for in
     * S3 storage until expiry. zstd lands well under snappy's size on the
     * text-heavy event shapes this catalog holds (at level 1 as at 3 —
     * see [DEFAULT_ZSTD_LEVEL]), and its
     * DEcompression — what readers and later rewrites actually spend —
     * is in snappy's league. The compaction sweep is CPU-bound on a
     * shared maintenance pod, so the level is pinned rather than
     * inherited: [DEFAULT_ZSTD_LEVEL] is 1, below parquet-java's own
     * default of 3, and pinning it means a library bump cannot silently
     * move this pod's CPU budget. parquet-java's zstd workers default to 0
     * (in-thread), which is what a shared pod wants, so nothing here
     * asks for threads.
     *
     * The allow-list is [SUPPORTED] — every codec on it is readable by
     * all four consumers of these files (DuckDB extension, Trino
     * connector, pyarrow, parquet-java itself) and has its
     * implementation on the server's runtime classpath. UNCOMPRESSED
     * stays on it deliberately: it is the escape hatch for an operator
     * who has measured their own data, and it is what makes the
     * regression test's red state reachable. LZO and BROTLI are off it
     * because their codecs are not on the classpath — a rewrite would
     * fail at the writer rather than at boot.
     */
    data class OutputCodec(
        val name: CompressionCodecName = DEFAULT_CODEC,
        /** Ignored unless [name] is ZSTD. */
        val zstdLevel: Int = DEFAULT_ZSTD_LEVEL,
    ) {
        init {
            require(name in SUPPORTED) {
                "compaction codec '$name' is not supported; choose one of " +
                    SUPPORTED.joinToString(", ") { it.name.lowercase() }
            }
            // zstd's own legal range. Out of range, zstd-jni clamps or
            // throws from inside the writer, mid-rewrite, per group —
            // refuse it at construction, which is boot.
            require(zstdLevel in MIN_ZSTD_LEVEL..MAX_ZSTD_LEVEL) {
                "compaction zstd level must be in $MIN_ZSTD_LEVEL..$MAX_ZSTD_LEVEL, got $zstdLevel"
            }
        }

        companion object {
            /**
             * Parse an operator-supplied codec name (case-insensitive,
             * `HOGLAKE_COMPACTION_CODEC`). An unknown name is refused by
             * NAME, at boot, listing the legal set — `valueOf` alone
             * answers a typo with a bare IllegalArgumentException that
             * does not say what was legal.
             */
            fun parse(
                name: String,
                zstdLevel: Int = DEFAULT_ZSTD_LEVEL,
            ): OutputCodec {
                val wanted =
                    SUPPORTED.firstOrNull { it.name.equals(name.trim(), ignoreCase = true) }
                        ?: throw IllegalArgumentException(
                            "unknown compaction codec '$name'; choose one of " +
                                SUPPORTED.joinToString(", ") { it.name.lowercase() },
                        )
                return OutputCodec(wanted, zstdLevel)
            }
        }
    }

    /**
     * One input file staged to local disk: its row-id range start, the
     * decoded live DV to apply (null = no live DV), and whether the
     * catalog says this file carries EXPLICIT row ids.
     *
     * [explicitRowIds] is `hog_data_file.explicit_row_ids`, and it is
     * the authoritative answer to a question this object used to infer
     * from the file's own schema: true means a compaction output, whose
     * identities live in a physical [ROW_ID_COLUMN]; false means the ids
     * are positional from [rowIdStart], which is what every client
     * append is. Defaulting to false keeps the common case (and every
     * test that does not care) at the ordinary shape.
     */
    data class Input(
        /**
         * Where the bytes are. An [InputFile] rather than a local path so
         * compaction can read an object in place (see S3InputFile)
         * instead of staging it to disk first; a local file is still one
         * of these, via the secondary constructor below.
         */
        val source: InputFile,
        /**
         * Identity for diagnostics — the object URI or the local path.
         * Carried explicitly because [InputFile] has no name: parquet's
         * own LocalInputFile does not override toString, so leaning on it
         * would turn every error message into an object hash.
         */
        val label: String,
        val rowIdStart: Long,
        val deletes: DeletionVector? = null,
        val explicitRowIds: Boolean = false,
        /**
         * The caller's word that this file's rows are already in merge-key
         * order (a compaction output written under the live sort spec), so
         * a sorted rewrite may read it as a run in place without checking.
         * An input without it is VERIFIED by the sortedness pre-pass
         * instead (see the class doc's run kinds).
         */
        override val trustedSorted: Boolean = false,
        /**
         * `hog_data_file.file_size_bytes`: the trusted-run cost estimate
         * before the footer is read, and the spill pre-refusal's measure.
         * 0 = unknown, which estimates a trusted run at the full output
         * block until its footer says otherwise.
         */
        override val fileSizeBytes: Long = 0,
        /**
         * Registered survivors (record_count minus the live DV's count):
         * how many rows this input adds to the spill path, which is what
         * predicts the spilled-run count before any data is read. 0 =
         * unknown; the spill-time run check still bounds the merge.
         */
        override val survivingRecords: Long = 0,
        /**
         * Where the sortedness pre-pass reads this file's sort-key
         * columns; null reads [source]. A separate [InputFile] because the
         * pre-pass reads a few non-adjacent column chunks, and the merge's
         * readahead would over-read the columns between them (see
         * `S3InputFile.KEY_COLUMN_READAHEAD_BYTES`). Opened, read and closed
         * by the pre-pass alone; the merge reopens [source].
         */
        val keySource: InputFile? = null,
    ) : RunCandidate

    /**
     * [rowsWritten] survivors; [minRowId] their smallest row id (the
     * output's row_id_start), null when every input row was deleted.
     * [footer] is the footer the writer just wrote — already in memory
     * once the writer closes, so compaction can register the output's
     * row-group offsets without reading anything back — or null if
     * parquet-java would not hand it over.
     *
     * The run counters describe a sorted rewrite's merge and are zero on
     * the unsorted path: [runsTrusted] inputs read in place as runs,
     * [runsSpilled] chunk files written, [runsDemoted] trusted inputs the
     * merge budget sent to the spill path instead, [spillBytes] local
     * bytes written for them. [spillCleanupFailed] is true when the
     * rewrite's spill directory could not be removed afterwards: the
     * output is good, but the volume still holds its spill files.
     *
     * The pre-pass counters: [filesVerified] inputs the sortedness check
     * passed (each became a trusted candidate; [runsTrusted] counts the
     * ones admitted, [runsDemoted] the ones the budget sent to the spill
     * path), [filesUnsorted] the ones it failed, [filesUnchecked] the ones
     * under the size floor ([SortSpill.verifyMinBytes]) it did not check,
     * [sortCheckBytes] the bytes it read to decide.
     *
     * [rowGroupsAppended] and [bytesAppended]: input row groups copied
     * into the output byte for byte instead of re-encoded (see
     * [appendRefusal]), on either path; an appended trusted run of a
     * sorted rewrite is also counted in [runsTrusted]. [filesUnranged]:
     * on the sorted path, runs whose first-key range the footer could not
     * give, counted only when they cost an append — a run that passed
     * every other condition, disjoint from every known range, merged
     * because of them (see `ExternalMergeSort.SortedRewrite.chooseAppended`); always 0 on
     * the unsorted path, which needs no range to append.
     */
    data class RewriteResult(
        val rowsWritten: Long,
        val minRowId: Long?,
        val footer: ParquetMetadata? = null,
        val runsTrusted: Int = 0,
        val runsSpilled: Int = 0,
        val runsDemoted: Int = 0,
        val spillBytes: Long = 0,
        val spillCleanupFailed: Boolean = false,
        val filesVerified: Int = 0,
        val filesUnsorted: Int = 0,
        val filesUnchecked: Int = 0,
        val sortCheckBytes: Long = 0,
        val rowGroupsAppended: Int = 0,
        val bytesAppended: Long = 0,
        val filesUnranged: Int = 0,
    )

    /**
     * How a writer's row groups are bounded, when the default (128 MiB
     * row groups, default pages, parquet-java's size-check cadence) is
     * not the bound wanted. Only spill files ask: reading one back holds a
     * whole row group, so the block must actually bound it, which
     * parquet-java's check cadence does not ensure on its own (see
     * [ExternalMergeSort.SPILL_MAX_ROWS_PER_SIZE_CHECK]).
     */
    internal class WriterTuning(
        val rowGroupBytes: Long,
        val pageBytes: Int,
        val maxRowsPerSizeCheck: Int,
    )

    /**
     * One materialized row: [group] as DECODED from its source — an input
     * file's own schema, or the output schema for a spill file — and the
     * [plan] that emits it as an output record. There is no output-shaped
     * copy (hoglake#134 package D2): the writer walks [group] through
     * [plan] straight into parquet's record consumer. [key] is its
     * sort-key tuple in the OUTPUT domain, extracted once when the row is
     * materialized ([SortKeys.bind]), so the sorted path compares a few
     * values per comparison instead of walking the `Group` graph; empty
     * on the unsorted path. [plan] is null only for rows that are never
     * written (the sortedness pre-pass's projected rows).
     */
    internal class Row(
        val group: Group,
        val rowId: Long,
        val key: Array<Any?> = NO_KEY,
        val plan: RowPlan? = null,
    )

    private val NO_KEY = emptyArray<Any?>()

    /**
     * One row's node allowance, spent as the row is DECODED.
     *
     * "Decoded" is the load-bearing word and it was missing. The first
     * version of this charged only the copy, which runs after
     * `recordReader.read()` has already built the whole source row — so
     * the budget was a report on an allocation that had already
     * happened, and a row big enough to exhaust the heap did so before
     * anything consulted it. The bound is only real if it is spent
     * inside the record materializer, which is what
     * [budgetedMaterializer] is for.
     *
     * [reset] is called once per row, by the root converter's `start`.
     */
    internal class NodeBudget(private val limit: Int, private val source: String) {
        private var spent = 0

        fun reset() {
            spent = 0
        }

        fun spend() {
            spent++
            if (spent > limit) {
                throw InvalidDataException(
                    "a row in $source materializes more than $limit nodes; refusing the group " +
                        "rather than risking a process-fatal OOM in the compaction loop",
                )
            }
        }
    }

    /**
     * [value] copied into a byte array of its own, so that nothing that
     * keeps it — the output writer's column statistics and its per-page
     * column index, which live until the footer — keeps the reader's page.
     *
     * The reader hands out plain-decoded binaries as slices of the decoded
     * PAGE buffer, and `Binary.copy()` returns `this` for a binary over a
     * constant buffer. So every min/max the output writer kept was a
     * reference into an input page, pinning the whole page until the
     * output closed: retained heap grew with rows written and with the
     * INPUT's page size (measured 310 MB at 3.2M rows of 1 MiB input row
     * groups, 520 MB at 6.4M; ~80k live `ByteBufferBackedBinary` at the
     * peak), on the unsorted path, the merge and the spill files alike.
     *
     * Copy rather than marking the slice reused (which makes the writer
     * copy only the values it keeps): both flatten the writer, measured
     * the same, but a slice keeps its page alive for as long as its ROW
     * lives, and a sorted chunk holds tens of thousands of rows — the
     * chunk phase measured 97 MB against the copy's 82 MB at 50,000 rows,
     * and the copy was no slower (`SortedRewriteCpuMeasurement`).
     */
    private fun unpinned(value: Binary): Binary = Binary.fromConstantByteArray(value.bytes)

    /**
     * [GroupRecordConverter] with [budget] spent as the row is built, and
     * every binary value [unpinned].
     *
     * Parquet hands a `RecordMaterializer` a tree of converters and
     * drives it from the column pages: every `start()` on a group
     * converter is one `SimpleGroup` about to be allocated, and every
     * `addX` on a primitive converter is one value about to be appended.
     * Counting there is the only place a cap can act BEFORE the memory
     * is taken — by the time `read()` returns, the row exists.
     *
     * The wrapper tree is built ONCE and cached per node, because
     * parquet navigates it by index while binding columns and expects
     * the same converter object every time.
     */
    internal fun budgetedMaterializer(
        schema: MessageType,
        budget: NodeBudget,
    ): RecordMaterializer<Group> {
        val delegate = GroupRecordConverter(schema)
        val root = CountingGroupConverter(delegate.rootConverter, budget, isRoot = true)
        return object : RecordMaterializer<Group>() {
            override fun getCurrentRecord(): Group = delegate.currentRecord

            override fun getRootConverter(): GroupConverter = root

            override fun skipCurrentRecord() = delegate.skipCurrentRecord()
        }
    }

    private class CountingGroupConverter(
        private val delegate: GroupConverter,
        private val budget: NodeBudget,
        private val isRoot: Boolean = false,
    ) : GroupConverter() {
        private val children = HashMap<Int, Converter>()

        override fun getConverter(fieldIndex: Int): Converter =
            children.getOrPut(fieldIndex) {
                when (val c = delegate.getConverter(fieldIndex)) {
                    is PrimitiveConverter -> CountingPrimitiveConverter(c, budget)
                    else -> CountingGroupConverter(c.asGroupConverter(), budget)
                }
            }

        override fun start() {
            // The root's start is the row boundary: the allowance is per
            // ROW and per PHASE, so the decode's renews here.
            if (isRoot) budget.reset() else budget.spend()
            delegate.start()
        }

        override fun end() = delegate.end()
    }

    private class CountingPrimitiveConverter(
        private val delegate: PrimitiveConverter,
        private val budget: NodeBudget,
    ) : PrimitiveConverter() {
        override fun hasDictionarySupport(): Boolean = delegate.hasDictionarySupport()

        override fun setDictionary(dictionary: Dictionary) = delegate.setDictionary(dictionary)

        override fun addValueFromDictionary(dictionaryId: Int) {
            budget.spend()
            delegate.addValueFromDictionary(dictionaryId)
        }

        override fun addBinary(value: Binary) {
            budget.spend()
            delegate.addBinary(unpinned(value))
        }

        override fun addBoolean(value: Boolean) {
            budget.spend()
            delegate.addBoolean(value)
        }

        override fun addDouble(value: Double) {
            budget.spend()
            delegate.addDouble(value)
        }

        override fun addFloat(value: Float) {
            budget.spend()
            delegate.addFloat(value)
        }

        override fun addInt(value: Int) {
            budget.spend()
            delegate.addInt(value)
        }

        override fun addLong(value: Long) {
            budget.spend()
            delegate.addLong(value)
        }
    }

    /**
     * How one matched input column lands in the output.
     *
     * [UINT32_TO_LONG] exists because unsigned parquet int32s must NOT
     * sign-extend: a uint32 above 2^31 would become negative, and a
     * foreign writer's unsigned int32 file under a `long` column is
     * exactly that pairing.
     *
     * There is deliberately no millis -> micros mode. It existed to
     * serve a timestamp_ms -> timestamp promotion, and that promotion
     * left the matrix once PROMOTIONS was pinned to DuckLake's
     * documented set (which has no timestamp rungs at all). With no
     * legal path producing a millis file under a micros column, the only
     * way to reach one is a writer disagreeing with its own DDL, and
     * refusing that is the rewriter's job.
     */
    internal enum class CopyMode {
        IDENTITY,
        INT_TO_LONG,
        UINT32_TO_LONG,
        FLOAT_TO_DOUBLE,
        DECIMAL_INT32,
        DECIMAL_INT64,
        DECIMAL_BINARY,
    }

    /**
     * Merge [inputs] (caller orders them by rowIdStart) into [output]
     * under the live schema [liveColumns] (ordinal order). [sortFields]
     * non-empty sorts the merged survivors by the table's sort order
     * (nulls per spec, ties by row id) through the external merge sort
     * the class doc describes, bounded by [spill] — which is then
     * required: an in-memory sort is bounded by the chunk, and a group
     * that fits one chunk is sorted in memory and written directly. Empty
     * [sortFields] keeps row-id order and streams. [codec]
     * is the output's compression (see [OutputCodec]) — an output's codec
     * is independent of its inputs', which may be any mix.
     */
    fun rewrite(
        inputs: List<Input>,
        liveColumns: List<Column>,
        sortFields: List<SortFieldDef>,
        output: OutputFile,
        maxNodesPerRow: Int = DEFAULT_MAX_NODES_PER_ROW,
        codec: OutputCodec = OutputCodec(),
        inputOpenParallelism: Int = DEFAULT_INPUT_OPEN_PARALLELISM,
        spill: SortSpill? = null,
        appendFloorBytes: Long = APPEND_MIN_ROW_GROUP_BYTES,
    ): RewriteResult {
        require(inputs.isNotEmpty()) { "rewrite needs at least one input" }
        // A caller that forgot the bounds must not get an unbounded sort:
        // silently materializing the group is the heap-O(group) behaviour
        // #134 removed, and it OOMs the process rather than the group.
        require(sortFields.isEmpty() || spill != null) {
            "a sorted rewrite needs a SortSpill: an in-memory sort is bounded by the chunk — a group " +
                "that fits one chunk is sorted in memory and written directly (hoglake#134)"
        }
        // VARIANT anywhere in the forest, not just at the top. #77
        // checked `liveColumns.none {...}`, which was exhaustive in a
        // world without containers — `struct{v: variant}` has no
        // top-level variant, so post-merge that check waves it through
        // and `parquetTypeFor` reaches its `error(...)` arm: an
        // IllegalStateException into the sweep's catch-all, counted
        // `failed_groups` and retried every run forever.
        //
        // CompactionService excludes such tables from candidates by the
        // same rule, so in production this is the backstop rather than
        // the gate.
        if (liveColumns.allNodes().any { it.def.type == ColType.VARIANT }) {
            // TYPED, like every other "this shape cannot be rewritten"
            // in here. #77 used `require`, which is an
            // IllegalArgumentException: the sweep's catch-all counts
            // that as `failed_groups` and retries it every run, when the
            // honest reading is `unconvertible_schema` — a shape this
            // rewriter cannot produce YET, which clears the day variant
            // compaction lands rather than blaming a writer.
            throw UnconvertibleSchemaException(
                "variant compaction is not supported: no released parquet-java can read a " +
                    "realistic shredded variant (hoglake#70), so the rewrite would have to drop " +
                    "or guess the payload",
            )
        }
        // A refusal mid-write leaves a partial output — a truncated
        // local file with no footer, or a multipart upload with parts
        // that are billed and invisible. Either way nothing survives a
        // throw: the rewriter owns what it was handed, and asks the
        // destination to discard itself. HOW to discard differs by
        // destination (unlink vs abort the upload), which is why it is
        // the OutputFile's job and not a `delete` here.
        try {
            return rewriteInto(
                inputs,
                liveColumns,
                sortFields,
                output,
                maxNodesPerRow,
                codec,
                inputOpenParallelism,
                spill,
                appendFloorBytes,
            )
        } catch (e: Throwable) {
            runCatching { (output as? DiscardableOutputFile)?.discard() }
            throw e
        }
    }

    /** The live output schema and what every per-input pipeline derives from it. */
    internal class OutputShape(
        val liveColumns: List<Column>,
        val schema: MessageType,
        val maxNodesPerRow: Int,
    ) {
        val dataFields: List<Type> = schema.fields.dropLast(1) // all but _hog_row_id
        val rowIdIndex: Int = schema.fieldCount - 1

        /** Emits a row already in the output schema (a spill file's) as itself. */
        val identityPlan: RowPlan by lazy { RowPlan(identitySteps(dataFields), this) }
    }

    private fun rewriteInto(
        inputs: List<Input>,
        liveColumns: List<Column>,
        sortFields: List<SortFieldDef>,
        output: OutputFile,
        maxNodesPerRow: Int,
        codec: OutputCodec,
        inputOpenParallelism: Int,
        spill: SortSpill?,
        appendFloorBytes: Long,
    ): RewriteResult {
        val shape = OutputShape(liveColumns, outputSchema(liveColumns), maxNodesPerRow)

        if (sortFields.isNotEmpty()) {
            // Keys resolved before any IO: a sort spec the output schema
            // cannot serve is refused without opening a file.
            val keys = SortKeys(shape.schema, sortFields)
            return ExternalMergeSort.rewrite(
                inputs,
                shape,
                keys,
                output,
                codec,
                inputOpenParallelism,
                checkNotNull(spill),
                appendFloorBytes,
            )
        }

        // No sort order: STREAM — write each survivor as it is read,
        // never materializing the group. Materialize-then-write put the
        // whole group's Group objects on the heap and OOM'd the server on
        // a 400 MB catalog; heap must stay flat in group size
        // (parquet-java's own row-group buffering bounds it).
        //
        // Flat in GROUP size, not in ROW size: one row still materializes
        // whole. That used to be an accepted limit with no bound at all,
        // which made a single pathological row a process-fatal OOM;
        // [maxNodesPerRow] now caps it, so the worst case is one counted
        // invalid_data skip.
        //
        // An input that is already a compaction output of this very
        // schema, with no deletes and big row groups, is APPENDED instead
        // ([appendRefusal]): its row groups are copied byte for byte. The
        // rows the other inputs produce keep filling the current encoded
        // row group across it, so the encoded rows land after the appended
        // groups rather than being cut into a small row group at every
        // appended input; row ids ride the carrier, so where a row sits
        // in an unsorted output carries no meaning.
        var written = 0L
        var minRowId: Long? = null
        var appended = 0
        var appendedBytes = 0L
        val (_, footer) =
            writingTo(output, shape.schema, codec) { writer ->
                forEachOpenedInput(inputs, inputOpenParallelism) { input, reader ->
                    if (appendRefusal(input, reader.footer, shape.schema, appendFloorBytes) == null) {
                        writer.appendRowGroups(input.source, reader, flushPending = false)
                        written += reader.footer.blocks.sumOf { it.rowCount }
                        val least = checkNotNull(minAppendedRowId(reader.footer))
                        minRowId = minOf(minRowId ?: least, least)
                    } else {
                        for (row in SurvivorReader(input, reader, shape)) {
                            writer.write(row)
                            written++
                            minRowId = minOf(minRowId ?: row.rowId, row.rowId)
                        }
                    }
                }
                appended = writer.rowGroupsAppended
                appendedBytes = writer.bytesAppended
            }
        return RewriteResult(written, minRowId, footer, rowGroupsAppended = appended, bytesAppended = appendedBytes)
    }

    /**
     * Why [input] cannot be APPENDED to an output of [outputSchema] —
     * its row groups copied in byte for byte, never decoded — or null when
     * it can. Each condition is what makes the copy byte-identical to
     * what the decode path would have written, row for row:
     *
     *  - no live deletion vector: an appended row group cannot skip rows;
     *  - `explicit_row_ids`: the ids ride in the [ROW_ID_COLUMN] carrier.
     *    Positional ids live only in the catalog's `row_id_start`, and
     *    copying the bytes would drop them;
     *  - its parquet schema EQUALS the output schema — field ids, physical
     *    types, repetition, logical annotations, the carrier, the order of
     *    every field and the message name — which in practice means a
     *    prior compaction output of the same live schema. Anything else
     *    (a dropped or added column, a promotion, an annotation) needs the
     *    column plan;
     *  - every row group at least [floorBytes]
     *    ([APPEND_MIN_ROW_GROUP_BYTES]): small row groups are re-encoded
     *    into big ones, which is what compaction is for;
     *  - every row group carries statistics on the row-id carrier: the
     *    output's smallest row id ([RewriteResult.minRowId]) is read off
     *    them, since no row is decoded.
     *
     * A file with no row groups is decoded (nothing to gain). The output
     * then holds the input's codec on the appended row groups, which may
     * differ from [OutputCodec] — legal parquet, per column chunk.
     */
    internal fun appendRefusal(
        input: Input,
        footer: ParquetMetadata,
        outputSchema: MessageType,
        floorBytes: Long,
    ): String? =
        when {
            input.deletes != null -> "it has a live deletion vector"
            !input.explicitRowIds -> "its row ids are positional"
            footer.fileMetaData.schema != outputSchema -> "its schema is not the output schema"
            footer.blocks.isEmpty() -> "it has no row groups"
            footer.blocks.any { it.compressedSize < floorBytes } ->
                "a row group is smaller than the append floor of $floorBytes B"
            footer.blocks.any { rowIdStatistics(it) == null } -> "a row group has no row-id statistics"
            else -> null
        }

    /** The smallest row id of an appendable file, off its footer. */
    internal fun minAppendedRowId(footer: ParquetMetadata): Long? =
        footer.blocks.mapNotNull { rowIdStatistics(it)?.first }.minOrNull()

    /** The carrier's (min, max) in one row group, or null when its statistics cannot say. */
    private fun rowIdStatistics(block: BlockMetaData): Pair<Long, Long>? {
        val chunk = block.columns.firstOrNull { it.path.toArray().contentEquals(arrayOf(ROW_ID_COLUMN)) } ?: return null
        val stats = chunk.statistics ?: return null
        if (stats.isEmpty || !stats.hasNonNullValue() || !stats.isNumNullsSet || stats.numNulls != 0L) return null
        val min = stats.genericGetMin() as? Long ?: return null
        val max = stats.genericGetMax() as? Long ?: return null
        return min to max
    }

    /**
     * Walk [inputs] IN ORDER, handing each to [body] with its parquet
     * reader already open, and open up to [parallelism] of them at once.
     *
     * # Why this exists
     *
     * Opening a parquet input is round trips, not bytes. Even with
     * `S3InputFile`'s footer prefetch it is at least one GET before a
     * single row can be read, and the rewrite used to pay that cost
     * strictly one input at a time, twice per input (once for the
     * schema, once for the rows — see the single [ParquetFileReader]
     * threaded through [SurvivorReader] now, which removed the second).
     * That is the shape behind compaction's FIXED per-group cost:
     * measured on gigahog-prod-us, ~8.5 s per group regardless of the
     * group's size, because a 64-file group is 64 serialized opens. The
     * bytes are not the problem; the latency is, and latency is what
     * overlaps.
     *
     * # Order and the memory bound are both preserved
     *
     * ORDER: [body] is called for `inputs[0]`, then `inputs[1]`, and so
     * on, on the CALLER's thread. Only the `open` is concurrent. That
     * matters beyond tidiness — the unsorted path writes survivors
     * straight through in input order, and an input's row ids are
     * positional from its own `rowIdStart`, so reordering the inputs
     * would reorder the output for no gain.
     *
     * MEMORY: at most [parallelism] readers exist at any moment
     * (`window` submitted, one of which is the one being consumed), and
     * a reader that is open but not yet being read holds its parsed
     * footer plus whatever tail `S3InputFile` prefetched — NOT a
     * readahead buffer, which is only allocated when the data pages are
     * read, and only for the input [body] currently holds. The
     * streaming bound is therefore untouched: it was one 8 MiB readahead
     * buffer before and it is one now.
     *
     * The knob nevertheless has to stay small for a reason the group
     * size cannot see: a footer is unbounded in principle (capped by
     * `S3InputFile.DEFAULT_MAX_PREFETCH_BYTES`), so the window
     * multiplies the worst-case footer footprint. 8 against a 64-file
     * group is 8x the concurrency for 8x a quantity that is kilobytes
     * for every real file.
     *
     * # Failures
     *
     * An open that throws surfaces on the consuming thread, with the
     * original exception unwrapped, at the position that input occupies
     * — so a corrupt file is still reported as itself rather than as a
     * pool failure, and the rewriter's typed refusals keep their
     * meaning. Whatever was opened ahead is closed on the way out,
     * whether the walk finished or threw, because those readers hold
     * object-store streams nothing else will ever reach.
     */
    internal fun forEachOpenedInput(
        inputs: List<Input>,
        parallelism: Int,
        body: (Input, ParquetFileReader) -> Unit,
    ) = forEachOpenedReader(inputs, parallelism) { input, reader -> reader.use { body(input, it) } }

    /**
     * [forEachOpenedInput]'s window with OWNERSHIP handed over: [take]
     * owns each reader from the moment it is called, and must close it or
     * register it with something that will, even when it throws. The
     * sorted path holds its trusted runs open across the whole merge, so
     * the walk cannot close them; what it still guarantees is the same
     * drain of every reader opened ahead and never handed over.
     */
    internal fun forEachOpenedReader(
        inputs: List<Input>,
        parallelism: Int,
        take: (Input, ParquetFileReader) -> Unit,
    ) {
        val window = parallelism.coerceAtMost(inputs.size)
        if (window <= 1) {
            // EXACTLY the old shape, with no executor and no thread hop:
            // the default is 1 for the unit tests and for any caller that
            // did not ask, and "off" must mean off.
            for (input in inputs) take(input, ParquetReaders.open(input.source))
            return
        }
        val pool =
            Executors.newFixedThreadPool(window) { r ->
                Thread(r, "compaction-input-open").apply { isDaemon = true }
            }
        val pending = ArrayDeque<Future<ParquetFileReader?>>()
        var submitted = 0
        // Flipped once the window stops being read. A task that opens
        // its reader AFTER that closes it itself and hands back null —
        // which is the only thing that can close a reader nobody is
        // waiting for any more, since the future it would arrive on is
        // never read again.
        val abandoned = java.util.concurrent.atomic.AtomicBoolean(false)
        try {
            for (input in inputs) {
                while (submitted < inputs.size && pending.size < window) {
                    val next = inputs[submitted++]
                    pending.addLast(
                        pool.submit(
                            Callable {
                                val opened = ParquetReaders.open(next.source)
                                if (abandoned.get()) {
                                    runCatching { opened.close() }
                                    null
                                } else {
                                    opened
                                }
                            },
                        ),
                    )
                }
                val reader =
                    try {
                        checkNotNull(pending.removeFirst().get()) {
                            "a prefetched reader for ${input.label} was abandoned while the " +
                                "group was still being read"
                        }
                    } catch (e: ExecutionException) {
                        // The pool's wrapper is noise: every caller of
                        // this rewriter discriminates on the exception
                        // TYPE (UnconvertibleSchemaException,
                        // InvalidDataException, the retryable object-store
                        // ones), and an ExecutionException is none of
                        // them.
                        throw e.cause ?: e
                    }
                take(input, reader)
            }
        } finally {
            // Three steps, in this order, and each one covers a case
            // the others cannot.
            //
            // 1. `shutdownNow()` FIRST: drop what is queued and
            //    interrupt what is running, so a hung object-store open
            //    is told to stop before anything waits on it. The
            //    previous version called `shutdown()` first and then
            //    waited per-future, which at the defaults is a window of
            //    8 opens times a 2 s budget each — sixteen seconds of a
            //    failed group hanging on to a sweep.
            //
            // 2. `abandoned`, so a task whose `open` COMPLETES after we
            //    stop reading closes its own reader. This is the case
            //    cancelling could never handle: `cancel(true)` marks the
            //    future and interrupts the thread, but an interrupt that
            //    lands after the last object-store call — or that the
            //    client swallows — leaves `open` to finish normally and
            //    hand a live reader to a future nobody will ever read.
            //
            // 3. ONE overall deadline for the drain, not one per future.
            //    Whatever has already completed is closed here; whatever
            //    completes later closes itself via (2); and the group
            //    never waits longer than [OPEN_DRAIN_MILLIS] in total.
            pool.shutdownNow()
            abandoned.set(true)
            val deadline = System.nanoTime() + OPEN_DRAIN_MILLIS * 1_000_000
            for (future in pending) {
                val remaining = deadline - System.nanoTime()
                val reader =
                    if (remaining > 0) {
                        runCatching { future.get(remaining, TimeUnit.NANOSECONDS) }.getOrNull()
                    } else if (future.isDone) {
                        // Past the deadline, but this one is already
                        // finished: taking its reader costs nothing and
                        // leaving it costs an unclosed stream.
                        runCatching { future.get() }.getOrNull()
                    } else {
                        null
                    }
                runCatching { reader?.close() }
            }
        }
    }

    /**
     * The per-input pipeline, PULL-style: apply the DV, map to the live
     * schema, stamp the row id, one row per [next].
     *
     * Pull rather than a callback because the merge needs many inputs
     * advanced one row at a time on one thread; parquet-java's
     * `RecordReader.read()` is already pull-style, so row groups and
     * records are simply read lazily. The reader belongs to the caller,
     * which opened it (possibly ahead of time, on another thread) and
     * closes it — this must not, and must not assume any row group has
     * been consumed yet.
     *
     * The DV cardinality check runs in the EXHAUSTION path, whenever
     * [hasNext] finds no row group left: a run in a merge is only
     * exhausted at the merge's end, which is the one place the check can
     * run without reading the run twice.
     */
    internal class SurvivorReader(
        private val input: Input,
        private val reader: ParquetFileReader,
        private val shape: OutputShape,
        /** The sorted path's keys, extracted per row; null on the unsorted path. */
        private val keys: SortKeys? = null,
    ) : Iterator<Row> {
        // From the reader the caller already opened, not a second open of
        // its own. This used to be `readSchema(input.source)` followed by
        // `readRows(input.source, ...)`, which opened — and so re-read and
        // re-parsed the footer of — every input TWICE.
        private val fileSchema: MessageType = reader.footer.fileMetaData.schema

        init {
            refuseDuplicateNames(fileSchema, emptyList())
        }

        // The plan and the carrier are resolved against the FILE's schema
        // (every refusal sees the whole file), then the read is PROJECTED
        // to exactly the columns they use: a column the live schema
        // dropped is never decoded, never held in a chunk, never charged.
        private val projected: ProjectedRead =
            run {
                // A previously-compacted input carries its ids in its own
                // row-id column; positional ids would be wrong for it.
                // Resolved FIRST: its refusals (a duplicated reserved id is
                // invalid_data) take precedence over the plan's.
                val carrier = rowIdCarrier(fileSchema, input)
                projectRead(fileSchema, columnPlan(fileSchema, shape.liveColumns, input.label), carrier)
            }

        /** What this reader decodes: the file schema pruned to the plan. */
        internal val requestedSchema: MessageType = projected.schema
        private val schema: MessageType = projected.schema
        private val srcRowIdIndex = projected.carrier
        private val plan = projected.steps

        init {
            reader.setRequestedSchema(schema)
        }

        // One allowance per row, spent by the DECODE: the root converter
        // renews it at each row boundary. There is no second phase to
        // charge any more — the output record is walked out of the
        // decoded row (package D2), not copied into a second graph — and
        // the walk cannot outgrow the decode: it visits each decoded node
        // at most once, never allocates a node, and skips dropped
        // columns. (Before D2 the copy was charged from a fresh
        // allowance too; sharing one across both phases had charged the
        // same graph twice and halved the advertised ceiling.)
        private val budget = NodeBudget(shape.maxNodesPerRow, input.label)
        private val rowPlan = RowPlan(plan, shape)
        private val boundKeys = keys?.bind(rowPlan)
        private val columnIO = ColumnIOFactory().getColumnIO(schema)
        private var records: org.apache.parquet.io.RecordReader<Group>? = null
        private var leftInRowGroup = 0L
        private var ordinal = 0L
        private var applied = 0L
        private var pending: Row? = null

        override fun hasNext(): Boolean {
            if (pending == null) pending = advance()
            return pending != null
        }

        override fun next(): Row {
            if (!hasNext()) throw NoSuchElementException("${input.label} is exhausted")
            return pending!!.also { pending = null }
        }

        private fun advance(): Row? {
            while (true) {
                while (leftInRowGroup == 0L) {
                    val pages = reader.readNextRowGroup()
                    if (pages == null) {
                        val expected = input.deletes?.cardinality ?: 0L
                        check(applied == expected) {
                            "deletion vector for ${input.label} claims $expected positions but only " +
                                "$applied fell inside the file — refusing a lossy compaction"
                        }
                        return null
                    }
                    records = columnIO.getRecordReader(pages, budgetedMaterializer(schema, budget))
                    leftInRowGroup = pages.rowCount
                }
                val src = records!!.read()
                leftInRowGroup--
                val position = ordinal++
                if (input.deletes?.contains(position) == true) {
                    applied++
                    continue
                }
                return row(src, position)
            }
        }

        private fun row(
            src: Group,
            position: Long,
        ): Row {
            val rowId =
                if (srcRowIdIndex != null) {
                    // PRESENT, not merely declared. A compaction output's
                    // row id is required, so a null in the carrier means
                    // the file is not what it says it is — and reading it
                    // anyway threw a raw RuntimeException out of
                    // parquet-java's Group ("not found ... element
                    // number 0"). Falling back to the positional id would
                    // be worse than the crash: it would silently give the
                    // row a DIFFERENT identity from the one it was
                    // committed with.
                    if (src.getFieldRepetitionCount(srcRowIdIndex) == 0) {
                        throw InvalidDataException(
                            "row $position of ${input.label} has a null $ROW_ID_COLUMN; the " +
                                "row id of a compacted file is required",
                        )
                    }
                    src.getLong(srcRowIdIndex, 0)
                } else {
                    input.rowIdStart + position
                }
            val key = boundKeys?.extract(src, input.label) ?: NO_KEY
            return Row(src, rowId, key, rowPlan)
        }
    }

    /** A projected read: the requested schema, the plan re-indexed into it, the carrier's index in it. */
    internal class ProjectedRead(val schema: MessageType, val steps: List<Step?>, val carrier: Int?)

    /**
     * [fileSchema] pruned to the columns [steps] read and the row-id
     * [carrier], with the steps and the carrier re-indexed into the pruned
     * schema — what the per-input pipeline asks the reader for
     * (`setRequestedSchema`), so that a column the live schema DROPPED is
     * neither decoded nor materialized.
     *
     * Why it matters beyond CPU: the sorted rewrite's chunk holds decoded
     * rows, and its size ([CompactionConfig.spillChunkRows]) is computed
     * from the LIVE schema's node count. Without the projection a file
     * written before half its table's columns were dropped materialized
     * twice the nodes per row the chunk was sized for.
     *
     * Pruned at the top level and inside structs (a struct member is an
     * independently droppable column). A list or a map is kept whole: its
     * element/key/value steps already name exactly what they read, and a
     * dropped member of a struct INSIDE a list is a rare enough shape not
     * to complicate the index arithmetic for. A struct none of whose
     * members the plan reads is kept whole too, because its presence still
     * decides whether the output row has the (empty) struct or a null, and
     * parquet has no empty group. A file the plan reads NOTHING from — a
     * client file all of whose columns were dropped — projects to an empty
     * message, which parquet-java still reads row by row (pinned: its rows
     * survive, all-null).
     */
    internal fun projectRead(
        fileSchema: MessageType,
        steps: List<Step?>,
        carrier: Int?,
    ): ProjectedRead {
        val used = (steps.mapNotNull { it?.srcIndex } + listOfNotNull(carrier)).distinct().sorted()
        val newIndex = used.withIndex().associate { (i, src) -> src to i }
        val fields =
            used.map { src ->
                val field = fileSchema.getType(src)
                val struct = steps.filterIsInstance<Step.StructStep>().firstOrNull { it.srcIndex == src }
                if (struct == null) field else pruneStruct(field.asGroupType(), struct).first
            }
        val remapped =
            steps.map { step ->
                when (step) {
                    null -> null
                    is Step.StructStep -> pruneStruct(fileSchema.getType(step.srcIndex).asGroupType(), step).second
                    else -> step
                }?.let { reindex(it, newIndex.getValue(step!!.srcIndex)) }
            }
        return ProjectedRead(
            MessageType(fileSchema.name, fields),
            remapped,
            carrier?.let { newIndex.getValue(it) },
        )
    }

    /** [group] pruned to the members [step] reads, and [step] re-indexed into it (its own index unchanged). */
    private fun pruneStruct(
        group: GroupType,
        step: Step.StructStep,
    ): Pair<Type, Step.StructStep> {
        val used = step.children.mapNotNull { it?.srcIndex }.distinct().sorted()
        if (used.isEmpty()) return group to step
        val newIndex = used.withIndex().associate { (i, src) -> src to i }
        val members =
            used.map { src ->
                val member = group.getType(src)
                val child = step.children.firstOrNull { it?.srcIndex == src }
                if (child is Step.StructStep) pruneStruct(member.asGroupType(), child).first else member
            }
        val children =
            step.children.map { child ->
                when (child) {
                    null -> null
                    is Step.StructStep ->
                        reindex(
                            pruneStruct(group.getType(child.srcIndex).asGroupType(), child).second,
                            newIndex.getValue(child.srcIndex),
                        )
                    else -> reindex(child, newIndex.getValue(child.srcIndex))
                }
            }
        return group.withNewFields(members) to Step.StructStep(step.srcIndex, children)
    }

    /** [step] reading from [index] instead. */
    private fun reindex(
        step: Step,
        index: Int,
    ): Step =
        when (step) {
            is Step.Scalar -> Step.Scalar(index, step.mode)
            is Step.StructStep -> Step.StructStep(index, step.children)
            is Step.ListStep -> Step.ListStep(index, step.element)
            is Step.MapStep -> Step.MapStep(index, step.key, step.value)
        }

    /**
     * The index of this input's row-id carrier, or null when its ids are
     * POSITIONAL.
     *
     * THE FLAG DECIDES, NEVER THE FILE'S OWN FIELD IDS. That is
     * AGENT.md invariant 2 and duckdb-client/DESIGN.md's read path,
     * both of which also require the reader to REFUSE a file that
     * disagrees with its registration IN EITHER DIRECTION — "each by
     * its own explicit check (never a fall-through)" — because the
     * server cannot detect the disagreement at registration time
     * (registration never opens the parquet, and the removed `/verify` was
     * metadata-only). Compaction is the one server surface that does
     * open it, so it is the one place that can enforce what the docs
     * ask of readers.
     *
     * Arms, each its own check:
     *
     *  - **flag true, exactly one field with the RESERVED id, primitive
     *    int64, not repeated** — bind it.
     *  - **flag true, otherwise** — refuse. The file cannot produce the
     *    ids its registration promises, and positional numbering would
     *    give every row an identity it was never committed with, which
     *    is the predecessor's rowid-remap bug.
     *  - **flag false, reserved id present anywhere at top level** —
     *    refuse. The reserved id is never allocatable to a real column,
     *    so the file and its registration contradict each other.
     *  - **flag false, otherwise** — positional numbering. A field
     *    merely NAMED `_hog_row_id` is an ordinary client column with
     *    an unlucky name.
     *
     * BY ID ONLY, with no name fallback, and that matters in the
     * flag-TRUE direction specifically. The shipped reader binds the
     * carrier with `TryFindColumnByFieldId(local_columns,
     * HOG_ROW_ID_FIELD_ID)` — the name plays no part — and refuses a
     * flag-true file without that id outright. Accepting an id-less
     * field on its NAME here would compact a file the reader refuses
     * into one it trusts, promoting a foreign writer's values to
     * authoritative row ids under the reserved id, and then expiring
     * the input. `FooterStats.bindIndex`'s id-less-name fallback is
     * about foreign files binding to CATALOG columns; a flag-true file
     * was written by compaction, which always stamps the id.
     */
    internal fun rowIdCarrier(
        schema: MessageType,
        input: Input,
    ): Int? {
        val source = input.label
        val reservedAt = schema.fields.indices.filter { schema.fields[it].id?.intValue() == ROW_ID_FIELD_ID }

        // ONE carrier or none. Two fields sharing the reserved id is the
        // duplicate-binding argument `refuseDuplicateNames` makes about
        // names, with more force: this id decides row IDENTITY, and
        // `indexOfFirst` would have picked one of them silently.
        if (reservedAt.size > 1) {
            throw InvalidDataException(
                "$source declares the reserved field id $ROW_ID_FIELD_ID on " +
                    "${reservedAt.size} top-level fields " +
                    "(${reservedAt.joinToString { Identifiers.cap(schema.fields[it].name) }}); " +
                    "the id that decides row identity has no correct resolution when duplicated",
            )
        }
        val reserved = reservedAt.singleOrNull()

        if (!input.explicitRowIds) {
            // EXPLICIT CHECK, not a fall-through: the doc names this
            // direction first.
            if (reserved != null) {
                throw InvalidDataException(
                    "$source is registered WITHOUT explicit_row_ids but its schema carries the " +
                        "reserved field id $ROW_ID_FIELD_ID on " +
                        "'${Identifiers.cap(schema.fields[reserved].name)}'. The reserved id is " +
                        "never allocatable to a real column, so the file and its registration " +
                        "contradict each other and its row ids cannot be trusted " +
                        "(AGENT.md invariant 2)",
                )
            }
            return null
        }

        if (reserved == null) {
            // Name the two shapes apart: a file with no such column, and
            // one whose column carries somebody else's id.
            val impostor = schema.fields.firstOrNull { it.name == ROW_ID_COLUMN }
            if (impostor != null) {
                throw InvalidDataException(
                    "$source is registered with explicit_row_ids and has a " +
                        "'${Identifiers.cap(impostor.name)}', but it carries field id " +
                        "${impostor.id?.intValue()} rather than the reserved $ROW_ID_FIELD_ID; " +
                        "the carrier binds by id, so this file cannot produce the ids its " +
                        "registration promises",
                )
            }
            throw InvalidDataException(
                "$source is registered with explicit_row_ids but carries no column with the " +
                    "reserved field id $ROW_ID_FIELD_ID ($ROW_ID_COLUMN); its rows have no " +
                    "identity to preserve, and numbering them positionally would give every one " +
                    "a different id from the one it was committed with",
            )
        }
        val field = schema.fields[reserved]
        val primitive = if (field.isPrimitive) field.asPrimitiveType() else null
        if (primitive?.primitiveTypeName != PrimitiveType.PrimitiveTypeName.INT64) {
            throw InvalidDataException(
                "$source is registered with explicit_row_ids but its " +
                    "'${Identifiers.cap(field.name)}' is not a primitive int64; refusing rather " +
                    "than renumbering the file's rows",
            )
        }
        if (field.isRepetition(Type.Repetition.REPEATED)) {
            throw InvalidDataException(
                "$source carries a REPEATED row-id column '${Identifiers.cap(field.name)}'; the " +
                    "row id is one value per row",
            )
        }
        return reserved
    }

    /**
     * Refuse an input whose schema names two siblings the same thing, at
     * any level.
     *
     * Parquet's own type model does not forbid it, but everything above
     * it assumes otherwise: name-based binding picks one arbitrarily,
     * and the writer's `GroupType` lookups resolve by name too, so a
     * duplicate surfaced as a raw ParquetEncodingException (or an NPE)
     * from inside parquet-java rather than as a refusal here. Whichever
     * of the two columns a rewrite happened to pick would have been a
     * coin flip about the user's data.
     */
    private fun refuseDuplicateNames(
        group: GroupType,
        path: List<String>,
    ) {
        val dupes = group.fields.groupingBy { it.name }.eachCount().filterValues { it > 1 }.keys
        if (dupes.isNotEmpty()) {
            val where = if (path.isEmpty()) "the file schema" else "'${path.joinToString(".")}'"
            throw UnconvertibleSchemaException(
                "input schema names more than one field ${dupes.sorted()} inside $where; " +
                    "columns bind by name or id, and a duplicate name makes the binding a guess",
            )
        }
        for (field in group.fields) {
            if (!field.isPrimitive) refuseDuplicateNames(field.asGroupType(), path + field.name)
        }
    }

    /**
     * The Hadoop configuration a writer of [codec] is built with: ONE per
     * zstd level, built once and shared, the way [ParquetReaders] shares
     * the readers'.
     *
     * Because building one is not free. `Builder.config(...)` creates a
     * fresh `Configuration` per writer, and its first read parses the
     * Hadoop default XML resources off the classpath — ~35 ms, measured,
     * which a sorted rewrite paid for its output AND every spill file, and
     * a tiny-file sweep's profile spent a quarter of its time in. Sharing
     * is safe because nothing the writer does writes to it: the Group
     * write support is handed its schema directly rather than through the
     * configuration. The resources are loaded here, once, so no two
     * writers race to load them.
     */
    internal fun writerConfiguration(codec: OutputCodec): org.apache.hadoop.conf.Configuration =
        writerConfigurations.computeIfAbsent(codec.zstdLevel) { level ->
            org.apache.hadoop.conf.Configuration().apply {
                set(ZSTD_LEVEL_KEY, level.toString())
                size() // load the default resources now, not on a writer's first read
            }
        }

    private val writerConfigurations =
        java.util.concurrent.ConcurrentHashMap<Int, org.apache.hadoop.conf.Configuration>()

    // The writer itself is [OutputWriter]: [codec] is chosen, never
    // inherited (parquet-java's builders default to UNCOMPRESSED, which
    // made every compaction a permanent decompression of its inputs — see
    // [OutputCodec]); the zstd level rides the shared configuration above;
    // [WriterTuning] null keeps parquet-java's defaults (128 MiB row
    // groups), which is what every compaction output is written at and
    // what the merge admission's trusted-run estimate assumes.

    // ---- schema ----------------------------------------------------------

    /**
     * Leaf columns of the output schema [liveColumns] produce, `_hog_row_id`
     * included: the unit [ExternalMergeSort]'s per-run page charge is
     * counted in. The planner calls this so its metadata refusals cost
     * runs exactly as the rewrite will.
     */
    internal fun outputLeafCount(liveColumns: List<Column>): Int = outputSchema(liveColumns).columns.size

    /**
     * The output schema is the LIVE schema: every live column in
     * ordinal order (optional, field-id-stamped, type synthesized from
     * the catalog type), plus the required [ROW_ID_COLUMN].
     */
    internal fun outputSchema(liveColumns: List<Column>): MessageType {
        require(liveColumns.isNotEmpty()) { "table has no live columns" }
        // The DDL path reserves the `_hog` prefix
        // (Identifiers.validateColumn), so a live column with this name
        // can only be a pre-reservation table or a hand-edited catalog
        // row. Either way the schema below would declare the name TWICE
        // with two different field ids, and parquet-java answers that
        // with a raw ParquetDecodingException from inside the writer.
        // Refuse it here, typed, and let the table be fixed by a rename.
        liveColumns.firstOrNull { it.def.name == ROW_ID_COLUMN }?.let {
            throw UnconvertibleSchemaException(
                "live column '${it.def.name}' (field ${it.fieldId}) collides with compaction's " +
                    "reserved row-id column; rename it before this table can be compacted",
            )
        }
        val dataFields = liveColumns.map { parquetTypeFor(it) }
        val rowIdField: Type =
            Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                .id(ROW_ID_FIELD_ID)
                .named(ROW_ID_COLUMN)
        return MessageType("hoglake_compacted", dataFields + rowIdField)
    }

    /**
     * Catalog type -> parquet type (pyhoglake's writer conventions:
     * micros times, fixed(16) uuid), recursive for the containers.
     *
     * [repetition] is OPTIONAL everywhere except a map's key, which the
     * parquet MAP encoding requires — catalog nullability is metadata,
     * so the output null-fills freely, but a required key is structure,
     * not metadata.
     */
    private fun parquetTypeFor(
        column: Column,
        repetition: Type.Repetition = Type.Repetition.OPTIONAL,
    ): Type {
        val id = Math.toIntExact(column.fieldId)
        val name = column.def.name

        // The CATALOG's arity, checked where the OUTPUT schema is built
        // — which happens before any planning, so a corrupt container
        // row reached `single()` here first and threw a raw
        // NoSuchElementException out of the sweep. Every other
        // disagreement with a file or a catalog is a typed skip; this
        // one has to be too.
        column.def.type.requiredChildCount?.let { required ->
            if (column.children.size != required) {
                throw UnconvertibleSchemaException(
                    "live ${column.def.type.wire} column '$name' has ${column.children.size} " +
                        "children, not $required; its catalog row is inconsistent",
                )
            }
        }
        if (column.def.type == ColType.STRUCT && column.children.isEmpty()) {
            throw UnconvertibleSchemaException(
                "live struct column '$name' has no children; its catalog row is inconsistent",
            )
        }

        fun prim(physical: PrimitiveType.PrimitiveTypeName) = Types.primitive(physical, repetition)

        return when (column.def.type) {
            ColType.VARIANT -> error("variant compaction is not supported")
            ColType.BOOLEAN ->
                prim(PrimitiveType.PrimitiveTypeName.BOOLEAN).id(id).named(name)
            ColType.INT8 -> intColumn(id, name, 8, signed = true, repetition = repetition)
            ColType.INT16 -> intColumn(id, name, 16, signed = true, repetition = repetition)
            ColType.UINT8 -> intColumn(id, name, 8, signed = false, repetition = repetition)
            ColType.UINT16 -> intColumn(id, name, 16, signed = false, repetition = repetition)
            ColType.INT ->
                prim(PrimitiveType.PrimitiveTypeName.INT32).id(id).named(name)
            // uint32 is written as a plain INT64, NOT as the INT32 +
            // INT(32, unsigned) pyarrow and DuckDB emit natively: it maps
            // to Iceberg long, and an Iceberg reader takes an INT32 column
            // as SIGNED, so values above 2^31 would read back negative
            // through the facade. Reads still accept both forms; rewriting
            // converges files on the facade-readable one.
            ColType.UINT32 ->
                prim(PrimitiveType.PrimitiveTypeName.INT64).id(id).named(name)
            // uint64 keeps its native physical form. Its facade mapping is
            // decimal(20,0), which parquet cannot express as an INT64, so
            // uint64 is the one type whose FILES are not facade-readable in
            // place even though its BOUNDS already are (docs/iceberg-federation.md §2).
            ColType.UINT64 -> intColumn(id, name, 64, signed = false, repetition = repetition)
            ColType.LONG ->
                prim(PrimitiveType.PrimitiveTypeName.INT64).id(id).named(name)
            ColType.FLOAT ->
                prim(PrimitiveType.PrimitiveTypeName.FLOAT).id(id).named(name)
            ColType.DOUBLE ->
                prim(PrimitiveType.PrimitiveTypeName.DOUBLE).id(id).named(name)
            ColType.DECIMAL ->
                prim(PrimitiveType.PrimitiveTypeName.BINARY)
                    .`as`(LogicalTypeAnnotation.decimalType(decimalScale(column) ?: 0, decimalPrecision(column)))
                    .id(id).named(name)
            ColType.DATE ->
                prim(PrimitiveType.PrimitiveTypeName.INT32)
                    .`as`(LogicalTypeAnnotation.dateType()).id(id).named(name)
            ColType.TIME ->
                prim(PrimitiveType.PrimitiveTypeName.INT64)
                    .`as`(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
                    .id(id).named(name)
            // Parquet has no seconds timestamp unit, so timestamp_s files
            // are physically MILLIS (pyarrow 25 coerces timestamp[s] on
            // write; verified) and rewrite to MILLIS unchanged. The
            // declared precision lives in the catalog, never in the file.
            ColType.TIMESTAMP_S, ColType.TIMESTAMP_MS ->
                prim(PrimitiveType.PrimitiveTypeName.INT64)
                    .`as`(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
                    .id(id).named(name)
            ColType.TIMESTAMP ->
                prim(PrimitiveType.PrimitiveTypeName.INT64)
                    .`as`(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
                    .id(id).named(name)
            ColType.TIMESTAMP_NS ->
                prim(PrimitiveType.PrimitiveTypeName.INT64)
                    .`as`(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.NANOS))
                    .id(id).named(name)
            ColType.TIMESTAMPTZ ->
                prim(PrimitiveType.PrimitiveTypeName.INT64)
                    .`as`(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
                    .id(id).named(name)
            ColType.STRING ->
                prim(PrimitiveType.PrimitiveTypeName.BINARY)
                    .`as`(LogicalTypeAnnotation.stringType()).id(id).named(name)
            // json maps to Iceberg string; the JSON annotation is the only
            // thing that distinguishes it physically, and the bytes are
            // copied verbatim — compaction never reformats a document.
            ColType.JSON ->
                prim(PrimitiveType.PrimitiveTypeName.BINARY)
                    .`as`(LogicalTypeAnnotation.jsonType()).id(id).named(name)
            ColType.UUID_T ->
                prim(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(16)
                    .`as`(LogicalTypeAnnotation.uuidType()).id(id).named(name)
            ColType.BINARY ->
                prim(PrimitiveType.PrimitiveTypeName.BINARY).id(id).named(name)
            // A struct is a plain group; its FIELDS are optional for the
            // same reason top-level columns are (an input predating one
            // null-fills it).
            ColType.STRUCT ->
                Types.buildGroup(repetition)
                    .addFields(*column.children.map { parquetTypeFor(it) }.toTypedArray())
                    .id(id).named(name)
            // The 3-level LIST encoding. The middle `list` group is
            // synthetic and carries NO field id: Iceberg puts the
            // element's id on the element, and a reader has nothing to
            // match an id on the repetition layer against.
            ColType.LIST ->
                Types.buildGroup(repetition)
                    .addField(
                        Types.repeatedGroup()
                            .addField(parquetTypeFor(column.children.single()))
                            .named(LIST_ENTRY_GROUP),
                    )
                    .`as`(LogicalTypeAnnotation.listType())
                    .id(id).named(name)
            // The MAP encoding. The key is REQUIRED — Iceberg map keys are
            // non-nullable and the parquet MAP shape says so too — which
            // is the one place the "write everything optional" rule yields.
            ColType.MAP ->
                Types.buildGroup(repetition)
                    .addFields(
                        Types.repeatedGroup()
                            .addFields(
                                parquetTypeFor(column.children[0], Type.Repetition.REQUIRED),
                                parquetTypeFor(column.children[1]),
                            )
                            .named(MAP_ENTRY_GROUP),
                    )
                    .`as`(LogicalTypeAnnotation.mapType())
                    .id(id).named(name)
        }
    }

    /** An INT32/INT64 column carrying parquet's INT(width, signed) annotation. */
    private fun intColumn(
        id: Int,
        name: String,
        width: Int,
        signed: Boolean,
        repetition: Type.Repetition = Type.Repetition.OPTIONAL,
    ): Type =
        Types.primitive(
            if (width == 64) PrimitiveType.PrimitiveTypeName.INT64 else PrimitiveType.PrimitiveTypeName.INT32,
            repetition,
        )
            .`as`(LogicalTypeAnnotation.intType(width, signed))
            .id(id).named(name)

    private fun decimalScale(column: Column): Int? = (column.def.typeParams?.get("scale") as? Number)?.toInt()

    private fun decimalPrecision(column: Column): Int =
        (column.def.typeParams?.get("precision") as? Number)?.toInt() ?: 38

    /**
     * One node of the copy plan: where this output field's value comes
     * from in the INPUT group holding it ([srcIndex], an index into the
     * parent group), and what to do with it.
     *
     * The plan is a tree because the record is: a struct copies its
     * children, a list copies its element once per repetition, a map
     * copies key and value per entry. [srcIndex] is always relative to
     * the group the step is read from, never absolute.
     */
    internal sealed class Step {
        abstract val srcIndex: Int

        /** A primitive: copy the value, up-casting per [mode]. */
        class Scalar(override val srcIndex: Int, val mode: CopyMode) : Step()

        /** A struct: per OUTPUT child, its step (null = null-fill). */
        class StructStep(override val srcIndex: Int, val children: List<Step?>) : Step()

        /** A 3-level list: [element] reads out of each repeated entry group. */
        class ListStep(override val srcIndex: Int, val element: Step) : Step()

        /** A map: [key] and [value] read out of each repeated key_value group. */
        class MapStep(override val srcIndex: Int, val key: Step, val value: Step) : Step()
    }

    /**
     * Per live column (output order): where its value comes from in
     * [schema] and how it is up-cast, or null when the input predates
     * the column (null-fill). Matching is by parquet field id, with a
     * live-NAME fallback for id-less input columns. Unmatched input
     * columns (dropped field ids, unknown id-less names) simply do not
     * appear in any plan — their data drops with the rewrite, exactly
     * like every reader already treats them.
     */
    internal fun columnPlan(
        schema: MessageType,
        liveColumns: List<Column>,
        inputPath: String,
    ): List<Step?> {
        // DUPLICATE IDS, before any binding. planChildren elects the
        // FIRST field with a matching id, so a file declaring one id
        // twice would have had the rewrite source live data from
        // whichever came first — and then end-snapshot the input, making
        // the guess permanent. There is no correct resolution, so the
        // group skips with unconvertible_schema instead.
        val duplicates = duplicateFieldIds(schema.fields)
        if (duplicates.isNotEmpty()) {
            throw UnconvertibleSchemaException(
                "$inputPath declares field id(s) ${duplicates.sorted()} more than once; field ids " +
                    "are the binding contract and a duplicate has no correct resolution",
            )
        }
        // The FILE-level gate, computed once and threaded down — the
        // reader's gate, so the two surfaces answer the same question
        // about the same file.
        return planChildren(schema.fields, liveColumns, FooterStats.usesFieldIds(schema), inputPath)
    }

    /** Field ids [fields] declares more than once, at any depth. */
    private fun duplicateFieldIds(fields: List<Type>): Set<Int> {
        val seen = HashSet<Int>()
        val dupes = HashSet<Int>()

        fun walk(level: List<Type>) {
            for (field in level) {
                field.id?.intValue()?.let { if (!seen.add(it)) dupes.add(it) }
                if (!field.isPrimitive) walk(field.asGroupType().fields)
            }
        }
        walk(fields)
        return dupes
    }

    /**
     * [columnPlan]'s recursion: one step per live column among
     * [srcFields], bound by [FooterStats.bindIndex] — the reader's
     * SEARCH, called rather than re-implemented.
     *
     * The search, not the predicate. Scanning with the predicate
     * first-match-wins let an id-less field sharing a column's NAME beat
     * the field carrying its ID, and this surface then copied the wrong
     * column's values into the output and end-snapshotted the input.
     *
     * [useFieldIds] is the FILE-level gate the reader applies, and the
     * rewriter now applies it too. Without it the two surfaces answered
     * differently about the same file: a file with ids on its wrappers
     * and none on its leaves produced zero stats on the read side and a
     * complete copy on the write side.
     */
    private fun planChildren(
        srcFields: List<Type>,
        liveColumns: List<Column>,
        useFieldIds: Boolean,
        inputPath: String,
    ): List<Step?> =
        liveColumns.map { column ->
            val srcIndex =
                FooterStats
                    .bindIndex(srcFields, column.fieldId, column.def.name, useFieldIds)
                    .takeIf { it >= 0 }
                    ?: return@map null
            planNode(srcFields[srcIndex], srcIndex, column, useFieldIds, inputPath)
        }

    /**
     * The step for one SYNTHETIC child — a list's element, a map's key
     * or value — at [position] inside the repetition layer.
     *
     * Bound by ID when the file declares one, by POSITION when it does
     * not, and never by NAME. The parquet spec says the synthetic names
     * are insignificant (this object's own `repeatedEntryGroup` says so
     * about the layer above), writers use `item`, `bag`, `entries` — but
     * [planChildren]'s id-less fallback matches on the LIVE column's
     * name, which for a list element is always `element`. So an id-less
     * element named `item` was refused here while the reader bound it
     * positionally and produced stats: the two surfaces disagreeing
     * about the same file, which is the drift the shared
     * `maxUnsignedParquetWidth` exists to prevent.
     *
     * The positional fallback is the same exemption `missingFieldIds`
     * grants the repetition layer, and safe for the same reason: such a
     * file is already flagged, so renames on its table are blocked and
     * position cannot drift out from under it.
     */
    private fun planSynthetic(
        srcFields: List<Type>,
        position: Int,
        column: Column,
        useFieldIds: Boolean,
        inputPath: String,
    ): Step? {
        // POSITION decides, and the id only VERIFIES — the reader's rule
        // (FooterStats.childBinds), and the two have to hold the same
        // one. Searching for the id ANYWHERE accepted an entry group
        // whose key and value are in the other order, and then the copy
        // below writes the key unconditionally into slot 0 on the
        // strength of the input key being REQUIRED — which is only true
        // of the field actually in slot 0. Measured: a swapped-order
        // file planned fine and threw `not found 1(key) element number
        // 0` mid-copy, which the sweep counts as a FAILED group (error
        // level, retried forever) rather than the skip-with-reason it is.
        val candidate = srcFields.getOrNull(position) ?: return null
        val id = candidate.id
        // ...and only when the FILE binds by id at all, matching the
        // reader's file-level gate.
        if (id != null && (!useFieldIds || id.intValue().toLong() != column.fieldId)) return null
        return planNode(candidate, position, column, useFieldIds, inputPath)
    }

    /**
     * The step producing [column] from the input field [src].
     *
     * **An unmatched child INSIDE a container aborts the group; an
     * unmatched TOP-LEVEL column null-fills.** The asymmetry is
     * deliberate. A top-level column the input lacks is ordinary schema
     * evolution — the column was added after the file was written, every
     * reader already shows null for it, and null-filling reproduces
     * exactly what a reader sees. Inside a container there is no such
     * reading: a list with no element, or a map with no key, is not a
     * column that arrived late, it is a shape disagreement about a
     * structure that cannot exist without that member. Null-filling it
     * would invent a row count and a repetition structure nothing in the
     * input implies. So the group skips with `unconvertible_schema` —
     * self-healing once the schema or the file set changes, and never
     * wrong bytes in the meantime. Never guess inside a container.
     *
     * A STRUCT field is the one interior that does null-fill, and for
     * the top-level reason: a struct's members ARE independently
     * evolvable columns (add_column with a `parent`), so one the input
     * predates is the same situation one level down.
     */

    private fun planNode(
        src: Type,
        srcIndex: Int,
        column: Column,
        useFieldIds: Boolean,
        inputPath: String,
    ): Step {
        fun refuseShape(detail: String): Nothing =
            throw UnconvertibleSchemaException(
                "column '${column.def.name}' (live type ${column.def.type.wire}) cannot be " +
                    "produced from $inputPath: $detail",
            )

        // REPETITION, before anything else. Every catalog type reachable
        // here holds AT MOST ONE value per row: a scalar, a struct, or a
        // container whose repetition lives in its own synthetic layer
        // (which planNode is never handed — it descends THROUGH it). A
        // REPEATED node says "many per row", and the copy below reads
        // repetition 0 and only repetition 0. Measured before this
        // guard: a 2-repetition struct rewrote to its first repetition
        // alone, half the values gone, rowsWritten still equal to the
        // record count so nothing looked wrong — and the inputs were
        // then end-snapshotted and expired.
        if (src.isRepetition(Type.Repetition.REPEATED)) {
            refuseShape(
                "the input field is REPEATED, but '${column.def.type.wire}' holds one value per row",
            )
        }

        if (!column.def.type.isNested) {
            if (!src.isPrimitive) refuseShape("the input field is a group, not a primitive leaf")
            return Step.Scalar(srcIndex, copyMode(src.asPrimitiveType(), column, inputPath))
        }
        if (src.isPrimitive) refuseShape("the input field is a primitive leaf, not a group")
        val group = src.asGroupType()
        return when (column.def.type) {
            ColType.STRUCT -> {
                // A struct's parquet counterpart is a PLAIN group. A
                // LIST or MAP wrapper carrying the struct's field id is
                // not "a struct with unfamiliar children", it is a
                // different type wearing the same id — and treating it
                // as a struct is the one shape that loses data silently:
                // the wrapper's only child is the repetition layer, so
                // every one of the struct's fields fails to match, and
                // a struct's fields are the ONE interior that null-fills
                // (see the note above). The rewrite would then produce
                // rows of empty structs, and the commit would
                // end-snapshot the input that held the real values —
                // F1's shape through a different door. Refuse instead.
                // CONTAINER annotations only, matching the reader
                // (FooterStats.isContainerAnnotation). Refusing ANY
                // annotation made a struct-shaped group carrying a stray
                // unrelated one (ENUM, say) permanently uncompactable,
                // and the reader stopped bounding it too — a table that
                // worked before this phase would have quietly stopped.
                val annotation = group.logicalTypeAnnotation
                if (FooterStats.isContainerAnnotation(annotation)) {
                    refuseShape(
                        "the input field is a '$annotation' group, not a struct; a container " +
                            "wearing a struct's field id is a type mismatch, not a schema evolution",
                    )
                }
                Step.StructStep(srcIndex, planChildren(group.fields, column.children, useFieldIds, inputPath))
            }
            ColType.LIST -> {
                val entry =
                    repeatedEntryGroup(group)
                        ?: refuseShape("the input field is not the 3-level LIST encoding")
                if (entry.fieldCount != 1) {
                    refuseShape("the input list's repeated group has ${entry.fieldCount} fields, not 1")
                }
                // The CATALOG's arity too: a container row with the wrong
                // child count is a corrupt catalog, and `single()` threw
                // a raw NoSuchElementException out of the sweep.
                if (column.children.size != 1) {
                    refuseShape(
                        "the live list column has ${column.children.size} children, not 1 (its element)",
                    )
                }
                val element =
                    planSynthetic(entry.fields, 0, column.children[0], useFieldIds, inputPath)
                        ?: refuseShape("the input list's element does not match the live element field id")
                Step.ListStep(srcIndex, element)
            }
            ColType.MAP -> {
                val entry =
                    repeatedEntryGroup(group)
                        ?: refuseShape("the input field is not the MAP encoding")
                if (entry.fieldCount != 2) {
                    refuseShape("the input map's key_value group has ${entry.fieldCount} fields, not 2")
                }
                // A key the input marks OPTIONAL cannot be copied into
                // the required output key: some row may have none, and
                // parquet would fail the write halfway through the group.
                // Refuse at plan time instead — the whole point of
                // unconvertible_schema.
                if (!entry.getType(0).isRepetition(Type.Repetition.REQUIRED)) {
                    refuseShape("the input map's key is not REQUIRED; Iceberg map keys are non-nullable")
                }
                if (column.children.size != 2) {
                    refuseShape(
                        "the live map column has ${column.children.size} children, not 2 (key, value)",
                    )
                }
                val key =
                    planSynthetic(entry.fields, 0, column.children[0], useFieldIds, inputPath)
                        ?: refuseShape("the input map's key does not match the live key field id")
                val value =
                    planSynthetic(entry.fields, 1, column.children[1], useFieldIds, inputPath)
                        ?: refuseShape("the input map's value does not match the live value field id")
                Step.MapStep(srcIndex, key, value)
            }
            else -> error("unreachable: ${column.def.type} is not a container")
        }
    }

    /**
     * The single repeated group inside a LIST/MAP wrapper, or null when
     * [group] is not that shape. The group's NAME is not checked: the
     * parquet spec says the synthetic names are insignificant, and
     * writers disagree about them (`list` vs `bag`, `key_value` vs
     * `map`). The shape is what carries meaning.
     */
    private fun repeatedEntryGroup(group: org.apache.parquet.schema.GroupType): org.apache.parquet.schema.GroupType? {
        val only = group.fields.singleOrNull() ?: return null
        if (only.isPrimitive || !only.isRepetition(Type.Repetition.REPEATED)) return null
        return only.asGroupType()
    }

    /**
     * How the live column is produced from the input's physical type:
     * identity, int32->int64, or float->double. Anything else — a
     * narrowing, a physical mismatch, a decimal scale change, a
     * non-micros time(stamp) unit — is [UnconvertibleSchemaException].
     */
    private fun copyMode(
        src: PrimitiveType,
        column: Column,
        inputPath: String,
    ): CopyMode {
        val srcName = src.primitiveTypeName
        val live = column.def.type

        fun refuse(): Nothing =
            throw UnconvertibleSchemaException(
                "column '${column.def.name}' (live type ${live.wire}) cannot be produced from " +
                    "$srcName${src.logicalTypeAnnotation?.let { " ($it)" } ?: ""} in $inputPath",
            )

        // The same domain rule the hydrator's footer decode applies
        // (ColType.maxUnsignedParquetWidth). Without it the two surfaces
        // DISAGREED: an INT(32, unsigned) file under an int8 column was
        // refused by the hydrator and copied through by the rewriter,
        // which then re-stamped it with the live column's INT(8, signed)
        // annotation — compaction laundering an annotation the hydrator
        // had rejected, and turning a file with no bounds into a file
        // with wrong ones. Both surfaces now refuse.
        val unsignedWidth = unsignedWidthOf(src)
        if (unsignedWidth != null && unsignedWidth > live.maxUnsignedParquetWidth) refuse()

        return when (live) {
            ColType.VARIANT -> error("variant compaction is not supported")
            ColType.BOOLEAN ->
                if (srcName == PrimitiveType.PrimitiveTypeName.BOOLEAN) CopyMode.IDENTITY else refuse()
            // Every width <= 16 (signed or not) rides parquet INT32 and
            // holds its true value there, so the int8/int16/uint8/uint16
            // promotion ladder is a physical no-op: copy the int32.
            ColType.INT8, ColType.INT16, ColType.UINT8, ColType.UINT16, ColType.INT ->
                if (srcName == PrimitiveType.PrimitiveTypeName.INT32) CopyMode.IDENTITY else refuse()
            // uint32 reads back from either physical form (see
            // parquetTypeFor) and always writes INT64. A plain signed
            // INT32 is refused: nothing legal produces one for a uint32
            // column, and reading it would be a guess about the sign.
            ColType.UINT32 ->
                when {
                    srcName == PrimitiveType.PrimitiveTypeName.INT64 -> CopyMode.IDENTITY
                    srcName == PrimitiveType.PrimitiveTypeName.INT32 && isUnsigned(src) ->
                        CopyMode.UINT32_TO_LONG
                    else -> refuse()
                }
            ColType.UINT64 ->
                if (srcName == PrimitiveType.PrimitiveTypeName.INT64 && isUnsigned(src)) {
                    CopyMode.IDENTITY
                } else {
                    refuse()
                }
            ColType.LONG ->
                when {
                    srcName == PrimitiveType.PrimitiveTypeName.INT64 -> CopyMode.IDENTITY
                    // A foreign writer's unsigned int32 under a `long`
                    // column: zero-extend. Sign-extending an unsigned
                    // int32 above 2^31 silently negates it.
                    srcName == PrimitiveType.PrimitiveTypeName.INT32 && isUnsigned(src) ->
                        CopyMode.UINT32_TO_LONG
                    srcName == PrimitiveType.PrimitiveTypeName.INT32 -> CopyMode.INT_TO_LONG
                    else -> refuse()
                }
            ColType.FLOAT ->
                if (srcName == PrimitiveType.PrimitiveTypeName.FLOAT) CopyMode.IDENTITY else refuse()
            ColType.DOUBLE ->
                when (srcName) {
                    PrimitiveType.PrimitiveTypeName.DOUBLE -> CopyMode.IDENTITY
                    PrimitiveType.PrimitiveTypeName.FLOAT -> CopyMode.FLOAT_TO_DOUBLE
                    else -> refuse()
                }
            ColType.DATE ->
                if (srcName == PrimitiveType.PrimitiveTypeName.INT32) CopyMode.IDENTITY else refuse()
            ColType.TIME -> {
                val unit = (src.logicalTypeAnnotation as? LogicalTypeAnnotation.TimeLogicalTypeAnnotation)?.unit
                if (srcName == PrimitiveType.PrimitiveTypeName.INT64 &&
                    (unit == null || unit == LogicalTypeAnnotation.TimeUnit.MICROS)
                ) {
                    CopyMode.IDENTITY
                } else {
                    refuse()
                }
            }
            // timestamp_s and timestamp_ms are both physically MILLIS —
            // parquet has no seconds unit — so one arm serves both. They
            // are distinct catalog types, not promotable to each other.
            ColType.TIMESTAMP_S, ColType.TIMESTAMP_MS -> {
                val unit = timestampUnit(src)
                if (srcName == PrimitiveType.PrimitiveTypeName.INT64 &&
                    (unit == null || unit == LogicalTypeAnnotation.TimeUnit.MILLIS)
                ) {
                    CopyMode.IDENTITY
                } else {
                    refuse()
                }
            }
            ColType.TIMESTAMP, ColType.TIMESTAMPTZ -> {
                val unit = timestampUnit(src)
                if (srcName == PrimitiveType.PrimitiveTypeName.INT64 &&
                    (unit == null || unit == LogicalTypeAnnotation.TimeUnit.MICROS)
                ) {
                    CopyMode.IDENTITY
                } else {
                    refuse()
                }
            }
            ColType.TIMESTAMP_NS -> {
                // Nothing promotes INTO timestamp_ns, so a nanos input is
                // the only shape that can legally exist here.
                if (srcName == PrimitiveType.PrimitiveTypeName.INT64 &&
                    timestampUnit(src) == LogicalTypeAnnotation.TimeUnit.NANOS
                ) {
                    CopyMode.IDENTITY
                } else {
                    refuse()
                }
            }
            // json and string are both BYTE_ARRAY; the bytes pass through
            // untouched either way.
            // The bytes pass through untouched either way — but the
            // ANNOTATION does not: the output re-stamps this leaf
            // STRING/JSON/none, so copying a DECIMAL-annotated BINARY
            // would relabel a signed two's-complement number as text and
            // hand the new footer's statistics an ordering the bytes do
            // not have. Annotation laundering, and the reader refuses the
            // same pairing (FooterStats.bytesSortUnsigned).
            ColType.STRING, ColType.JSON ->
                if (srcName == PrimitiveType.PrimitiveTypeName.BINARY &&
                    FooterStats.bytesSortUnsigned(src.logicalTypeAnnotation)
                ) {
                    CopyMode.IDENTITY
                } else {
                    refuse()
                }
            // FIXED_LEN_BYTE_ARRAY too: a fixed-width blob IS bytes, the
            // reader has always bounded one under a binary column, and
            // refusing it here left such a table bounded but permanently
            // uncompactable. The output is BINARY, which holds them.
            ColType.BINARY ->
                if ((
                        srcName == PrimitiveType.PrimitiveTypeName.BINARY ||
                            srcName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
                    ) &&
                    FooterStats.bytesSortUnsigned(src.logicalTypeAnnotation)
                ) {
                    CopyMode.IDENTITY
                } else {
                    refuse()
                }
            // Same annotation gate as string/json/binary, and for the
            // same reason: sixteen bytes annotated DECIMAL are not a
            // uuid, and copying them under a `uuid` output column
            // re-stamps them UUID — laundering the annotation exactly as
            // the string arm used to.
            ColType.UUID_T ->
                if (srcName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY &&
                    src.typeLength == 16 &&
                    FooterStats.bytesSortUnsigned(src.logicalTypeAnnotation)
                ) {
                    CopyMode.IDENTITY
                } else {
                    refuse()
                }
            ColType.DECIMAL -> {
                val annotation =
                    src.logicalTypeAnnotation as? LogicalTypeAnnotation.DecimalLogicalTypeAnnotation
                        ?: refuse()
                val liveScale = decimalScale(column) ?: 0
                if (annotation.scale != liveScale ||
                    annotation.precision > decimalPrecision(column)
                ) {
                    refuse()
                }
                when (srcName) {
                    PrimitiveType.PrimitiveTypeName.INT32 -> CopyMode.DECIMAL_INT32
                    PrimitiveType.PrimitiveTypeName.INT64 -> CopyMode.DECIMAL_INT64
                    PrimitiveType.PrimitiveTypeName.BINARY,
                    PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
                    -> CopyMode.DECIMAL_BINARY
                    else -> refuse()
                }
            }
            // Unreachable: planNode routes containers to their own steps
            // and only ever calls copyMode for a scalar live column.
            ColType.LIST, ColType.STRUCT, ColType.MAP -> refuse()
        }
    }

    private fun isUnsigned(src: PrimitiveType): Boolean = unsignedWidthOf(src) != null

    /** The source's unsigned INT width, or null when it is not unsigned-annotated. */
    private fun unsignedWidthOf(src: PrimitiveType): Int? =
        (src.logicalTypeAnnotation as? LogicalTypeAnnotation.IntLogicalTypeAnnotation)
            ?.takeIf { !it.isSigned }
            ?.bitWidth

    private fun timestampUnit(src: PrimitiveType): LogicalTypeAnnotation.TimeUnit? =
        (src.logicalTypeAnnotation as? LogicalTypeAnnotation.TimestampLogicalTypeAnnotation)?.unit

    // ---- write -----------------------------------------------------------

    /**
     * The [Step] tree that reads an OUTPUT-schema row as itself: every
     * scalar [CopyMode.IDENTITY], every container by its own shape. What a
     * spill file's rows are emitted with — they were mapped to the live
     * schema when first read, so there is nothing to map and nothing to
     * convert.
     */
    internal fun identitySteps(fields: List<Type>): List<Step?> =
        fields.mapIndexed { i, field -> identityStep(field, i) }

    private fun identityStep(
        field: Type,
        index: Int,
    ): Step {
        if (field.isPrimitive) return Step.Scalar(index, CopyMode.IDENTITY)
        val group = field.asGroupType()
        return when (group.logicalTypeAnnotation) {
            is LogicalTypeAnnotation.ListLogicalTypeAnnotation ->
                Step.ListStep(index, identityStep(group.getType(0).asGroupType().getType(0), 0))
            is LogicalTypeAnnotation.MapLogicalTypeAnnotation -> {
                val entry = group.getType(0).asGroupType()
                Step.MapStep(index, identityStep(entry.getType(0), 0), identityStep(entry.getType(1), 1))
            }
            else -> Step.StructStep(index, identitySteps(group.fields))
        }
    }

    /**
     * How the rows of ONE source schema are written as output records:
     * the column plan ([columnPlan], or [identitySteps] for a spill file)
     * walked from the decoded source `Group` straight into parquet's
     * [RecordConsumer] — null-filling absent live columns by not writing
     * them, dropping columns the plan does not name, promoting per
     * [CopyMode], and stamping the row id.
     *
     * This replaced a COPY (hoglake#134 package D2): every row used to be
     * rebuilt as an output `SimpleGroup` and that graph walked into the
     * consumer by the example writer, so each row was materialized twice;
     * the profile put the copy plus the second walk at ~20% of a rewrite.
     * The emission is the example model's own (`GroupWriter`): a field is
     * started only when it has a value, a group per struct, list entry and
     * map entry. Binaries are emitted as decoded, which the materializer
     * has already [unpinned] — nothing the output writer keeps (statistics,
     * the column index) can hold an input page.
     */
    internal class RowPlan(
        private val steps: List<Step?>,
        shape: OutputShape,
    ) {
        private val fields = shape.dataFields
        private val rowIdIndex = shape.rowIdIndex

        /** For [SortKeys.bind]: the top-level steps, in output order. */
        internal val topSteps: List<Step?> get() = steps

        fun write(
            rc: RecordConsumer,
            src: Group,
            rowId: Long,
        ) {
            rc.startMessage()
            for (i in steps.indices) {
                val step = steps[i] ?: continue
                if (src.getFieldRepetitionCount(step.srcIndex) == 0) continue
                val type = fields[i]
                rc.startField(type.name, i)
                writeValue(rc, src, step, type)
                rc.endField(type.name, i)
            }
            rc.startField(ROW_ID_COLUMN, rowIdIndex)
            rc.addLong(rowId)
            rc.endField(ROW_ID_COLUMN, rowIdIndex)
            rc.endMessage()
        }

        /** One present field of [src], as the output [type] at its position. */
        private fun writeValue(
            rc: RecordConsumer,
            src: Group,
            step: Step,
            type: Type,
        ) {
            when (step) {
                is Step.Scalar -> writeScalar(rc, src, step, type.asPrimitiveType())
                is Step.StructStep -> {
                    val g = src.getGroup(step.srcIndex, 0)
                    val struct = type.asGroupType()
                    rc.startGroup()
                    for ((j, child) in step.children.withIndex()) {
                        if (child != null && g.getFieldRepetitionCount(child.srcIndex) > 0) {
                            val childType = struct.getType(j)
                            rc.startField(childType.name, j)
                            writeValue(rc, g, child, childType)
                            rc.endField(childType.name, j)
                        }
                    }
                    rc.endGroup()
                }
                is Step.ListStep -> {
                    // An EMPTY list stays an empty list, distinct from
                    // null: the group is written either way, and only the
                    // entries repeat.
                    val list = src.getGroup(step.srcIndex, 0)
                    val entryType = type.asGroupType().getType(0).asGroupType()
                    val elementType = entryType.getType(0)
                    val n = list.getFieldRepetitionCount(0)
                    rc.startGroup()
                    if (n > 0) {
                        rc.startField(entryType.name, 0)
                        for (k in 0 until n) {
                            val entry = list.getGroup(0, k)
                            rc.startGroup()
                            if (entry.getFieldRepetitionCount(step.element.srcIndex) > 0) {
                                rc.startField(elementType.name, 0)
                                writeValue(rc, entry, step.element, elementType)
                                rc.endField(elementType.name, 0)
                            }
                            rc.endGroup()
                        }
                        rc.endField(entryType.name, 0)
                    }
                    rc.endGroup()
                }
                is Step.MapStep -> {
                    val map = src.getGroup(step.srcIndex, 0)
                    val entryType = type.asGroupType().getType(0).asGroupType()
                    val keyType = entryType.getType(0)
                    val valueType = entryType.getType(1)
                    val n = map.getFieldRepetitionCount(0)
                    rc.startGroup()
                    if (n > 0) {
                        rc.startField(entryType.name, 0)
                        for (k in 0 until n) {
                            val entry = map.getGroup(0, k)
                            rc.startGroup()
                            // The key is REQUIRED on both sides (planNode
                            // refuses an optional input key), so it is
                            // always present.
                            rc.startField(keyType.name, 0)
                            writeValue(rc, entry, step.key, keyType)
                            rc.endField(keyType.name, 0)
                            if (entry.getFieldRepetitionCount(step.value.srcIndex) > 0) {
                                rc.startField(valueType.name, 1)
                                writeValue(rc, entry, step.value, valueType)
                                rc.endField(valueType.name, 1)
                            }
                            rc.endGroup()
                        }
                        rc.endField(entryType.name, 0)
                    }
                    rc.endGroup()
                }
            }
        }

        private fun writeScalar(
            rc: RecordConsumer,
            src: Group,
            step: Step.Scalar,
            primitive: PrimitiveType,
        ) {
            val srcIdx = step.srcIndex
            when (step.mode) {
                CopyMode.INT_TO_LONG -> rc.addLong(src.getInteger(srcIdx, 0).toLong())
                // Zero-extend, never sign-extend: this mode exists precisely
                // for the int32 bit patterns whose unsigned reading is > 2^31.
                CopyMode.UINT32_TO_LONG -> rc.addLong(src.getInteger(srcIdx, 0).toLong() and 0xFFFFFFFFL)
                CopyMode.FLOAT_TO_DOUBLE -> rc.addDouble(src.getFloat(srcIdx, 0).toDouble())
                CopyMode.DECIMAL_INT32, CopyMode.DECIMAL_INT64, CopyMode.DECIMAL_BINARY -> {
                    val unscaled = unscaledOf(src, step, primitive.name)
                    val annotation =
                        primitive.logicalTypeAnnotation as LogicalTypeAnnotation.DecimalLogicalTypeAnnotation
                    val precision = annotation.precision
                    if (unscaled.abs().toString().length > precision) {
                        // The VALUE is wrong, not the schema pairing: same
                        // two schemas with in-range values rewrite fine, so
                        // re-planning this group will never help.
                        throw InvalidDataException("decimal value exceeds destination precision $precision")
                    }
                    rc.addBinary(Binary.fromConstantByteArray(unscaled.toByteArray()))
                }
                CopyMode.IDENTITY ->
                    when (primitive.primitiveTypeName) {
                        PrimitiveType.PrimitiveTypeName.BOOLEAN -> rc.addBoolean(src.getBoolean(srcIdx, 0))
                        PrimitiveType.PrimitiveTypeName.INT32 -> rc.addInteger(src.getInteger(srcIdx, 0))
                        PrimitiveType.PrimitiveTypeName.INT64 -> rc.addLong(src.getLong(srcIdx, 0))
                        PrimitiveType.PrimitiveTypeName.FLOAT -> rc.addFloat(src.getFloat(srcIdx, 0))
                        PrimitiveType.PrimitiveTypeName.DOUBLE -> rc.addDouble(src.getDouble(srcIdx, 0))
                        PrimitiveType.PrimitiveTypeName.BINARY,
                        PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
                        -> rc.addBinary(src.getBinary(srcIdx, 0))
                        PrimitiveType.PrimitiveTypeName.INT96, null ->
                            throw IllegalArgumentException(
                                "unsupported physical type ${primitive.primitiveTypeName}",
                            )
                    }
            }
        }
    }

    /**
     * A decimal source value's unscaled integer. Binary decimals can
     * exceed Long.MAX_VALUE, so they are never narrowed through Long.
     */
    private fun unscaledOf(
        src: Group,
        step: Step.Scalar,
        column: String,
    ): BigInteger =
        when (step.mode) {
            CopyMode.DECIMAL_INT32 -> BigInteger.valueOf(src.getInteger(step.srcIndex, 0).toLong())
            CopyMode.DECIMAL_INT64 -> BigInteger.valueOf(src.getLong(step.srcIndex, 0))
            else -> {
                // A zero-length byte array is not a decimal: BigInteger("")
                // throws, and there is no sensible value to invent. The
                // file declared DECIMAL and then stored something else.
                val bytes = src.getBinary(step.srcIndex, 0).bytes
                if (bytes.isEmpty()) {
                    throw InvalidDataException(
                        "empty byte array under decimal column '$column' — " +
                            "a decimal's unscaled value needs at least one byte",
                    )
                }
                BigInteger(bytes)
            }
        }

    // ---- sorting ---------------------------------------------------------

    /**
     * One resolved sort key: the chain of field indexes from the record
     * root down to the primitive leaf, and the leaf's type.
     *
     * The chain has more than one element only for a STRUCT leaf, which
     * Iceberg (and AlterService) allow as a sort source. Nothing under a
     * list or a map can be one — a row has many such values — and the
     * container types themselves are not sortable at all; both are
     * refused at DDL time, and [sortKeyPath] refuses them again here so
     * a hand-built spec cannot reach the comparator.
     */
    internal class SortKey(
        val path: List<Int>,
        val primitive: PrimitiveType,
        val field: SortFieldDef,
    )

    /**
     * The group holding the leaf, walking [path]'s struct levels; null
     * when an ancestor struct is itself null (which makes the leaf null).
     */
    private fun navigate(
        root: Group,
        path: List<Int>,
    ): Group? {
        var g: Group = root
        for (i in 0 until path.size - 1) {
            if (g.getFieldRepetitionCount(path[i]) == 0) return null
            g = g.getGroup(path[i], 0)
        }
        return g
    }

    /**
     * Resolve one sort field to its index chain in the output schema.
     *
     * Internal rather than private so the refusals can be tested
     * directly: [rewrite] only ever hands this the schema [outputSchema]
     * just built, so the shapes it has to refuse — a repeated group with
     * no logical annotation, a container carrying the sort field's id —
     * are unreachable through the public entry point. A guard no test
     * can reach is not a guard.
     */
    internal fun sortKeyPath(
        schema: MessageType,
        field: SortFieldDef,
    ): SortKey {
        fun search(
            group: org.apache.parquet.schema.GroupType,
            prefix: List<Int>,
        ): Pair<List<Int>, Type>? {
            group.fields.forEachIndexed { i, type ->
                val here = prefix + i
                if (type.id?.intValue()?.toLong() == field.sourceFieldId) return here to type
                // Only STRUCT interiors are searched: a repeated group
                // (list/map) holds many values per row, so nothing under
                // one is addressable as a single sort key.
                if (!type.isPrimitive && !type.isRepetition(Type.Repetition.REPEATED)) {
                    val group2 = type.asGroupType()
                    if (group2.logicalTypeAnnotation == null) {
                        search(group2, here)?.let { return it }
                    }
                }
            }
            return null
        }
        val found =
            search(schema, emptyList())
                ?: throw UnconvertibleSchemaException(
                    "sort source field_id ${field.sourceFieldId} is not a top-level column or a " +
                        "struct leaf of the output schema; list/map internals and nested containers " +
                        "are not sortable",
                )
        val (path, type) = found
        if (!type.isPrimitive) {
            throw UnconvertibleSchemaException(
                "sort source field_id ${field.sourceFieldId} is a nested container " +
                    "('${type.name}'), and nested containers have no sort order",
            )
        }
        return SortKey(path, type.asPrimitiveType(), field)
    }

    /**
     * The sorted path's ONE ordering: each row's sort-key tuple extracted
     * once, when the row is materialized, then compared as plain values —
     * the sort keys in spec order, then the row id (the class doc's
     * "Order"). Comparing the `Group`s directly walked the object graph,
     * read the repetition count and boxed the value on every one of the
     * n·log n chunk-sort and log k merge comparisons.
     *
     * The extracted form keeps the exact semantics of the typed compare
     * it replaced, per physical type:
     *  - BOOLEAN, INT32, INT64 -> a Long. INT(w, unsigned) is PRE-MAPPED so
     *    a signed compare is the unsigned one: uint32 zero-extends,
     *    uint64 flips its sign bit. A signed compare of the raw value puts
     *    every uint64 above 2^63 (and uint32 above 2^31) below zero.
     *  - FLOAT, DOUBLE -> a Double, compared by `Double.compareTo`: NaN
     *    greatest, -0.0 below 0.0. A float widens exactly, NaN included.
     *  - decimal-annotated BINARY/FIXED -> the signed two's-complement
     *    BigInteger (an unsigned byte compare mis-sorts negatives); an
     *    empty array is not a decimal and is refused here, naming the row's
     *    source, as the copy path refuses it.
     *  - other BINARY/FIXED (string, uuid, bytes) -> the bytes, compared
     *    unsigned lexicographically, which for UTF-8 is code-point order.
     * A null (the leaf absent, or an ancestor struct null) is placed by
     * the field's null order whatever its direction; DESC negates the
     * non-null compare.
     */
    internal class SortKeys(
        schema: MessageType,
        sortFields: List<SortFieldDef>,
    ) {
        private val keys = sortFields.map { sortKeyPath(schema, it) }

        /** The spec these keys were resolved from, in order. */
        val fields: List<SortFieldDef> = sortFields
        private val kinds = keys.map { kindOf(it.primitive) }.toTypedArray()
        private val descending = BooleanArray(keys.size) { keys[it].field.direction == SortDirection.DESC }
        private val nullsFirst =
            BooleanArray(keys.size) { keys[it].field.nullOrder == com.posthog.hoglake.model.NullOrder.NULLS_FIRST }

        /** The key tuple of [group], an OUTPUT-schema row; [source] names it in a refusal. */
        fun extract(
            group: Group,
            source: String,
        ): Array<Any?> =
            Array(keys.size) { i ->
                val key = keys[i]
                val holder = navigate(group, key.path)
                val idx = key.path.last()
                if (holder == null || holder.getFieldRepetitionCount(idx) == 0) {
                    null
                } else {
                    valueOf(kinds[i], key, holder, idx, source)
                }
            }

        /** The leaf at [idx] of [holder], read as [kind]. */
        private fun valueOf(
            kind: KeyKind,
            key: SortKey,
            holder: Group,
            idx: Int,
            source: String,
        ): Any =
            when (kind) {
                KeyKind.BOOLEAN -> if (holder.getBoolean(idx, 0)) 1L else 0L
                KeyKind.INT32 -> holder.getInteger(idx, 0).toLong()
                KeyKind.UINT32 -> holder.getInteger(idx, 0).toLong() and 0xFFFF_FFFFL
                KeyKind.INT64 -> holder.getLong(idx, 0)
                KeyKind.UINT64 -> holder.getLong(idx, 0) xor Long.MIN_VALUE
                KeyKind.FLOAT -> holder.getFloat(idx, 0).toDouble()
                KeyKind.DOUBLE -> holder.getDouble(idx, 0)
                KeyKind.DECIMAL -> decimalOf(key.primitive, holder.getBinary(idx, 0).bytes, source)
                KeyKind.BYTES -> holder.getBinary(idx, 0).bytes
            }

        /**
         * These keys read out of a SOURCE row that [plan] emits: each key's
         * output path resolved through the plan to the source's own field
         * indexes, and its value produced in the OUTPUT domain by the
         * plan's [CopyMode] — the value the written row will hold, so a
         * row compares exactly as it would after the copy D2 removed. A
         * key the source does not have (a column added after the file was
         * written, or under a null-filled struct member) is null in every
         * row.
         */
        fun bind(plan: RowPlan): BoundKeys = BoundKeys(plan)

        inner class BoundKeys internal constructor(plan: RowPlan) {
            /** Per key: the source index chain to its leaf, or null when absent. */
            private val paths: Array<IntArray?>
            private val leaves: Array<Step.Scalar?>

            init {
                paths = arrayOfNulls(keys.size)
                leaves = arrayOfNulls(keys.size)
                for ((k, key) in keys.withIndex()) {
                    var level: List<Step?> = plan.topSteps
                    val chain = IntArray(key.path.size)
                    var leaf: Step.Scalar? = null
                    for ((depth, index) in key.path.withIndex()) {
                        val step = level[index] ?: break
                        chain[depth] = step.srcIndex
                        if (depth == key.path.lastIndex) {
                            leaf = step as Step.Scalar
                        } else {
                            level = (step as Step.StructStep).children
                        }
                    }
                    if (leaf != null) {
                        paths[k] = chain
                        leaves[k] = leaf
                    }
                }
            }

            fun extract(
                src: Group,
                source: String,
            ): Array<Any?> =
                Array(keys.size) { i ->
                    val chain = paths[i]
                    val leaf = leaves[i]
                    if (chain == null || leaf == null) return@Array null
                    var holder: Group = src
                    for (d in 0 until chain.size - 1) {
                        if (holder.getFieldRepetitionCount(chain[d]) == 0) return@Array null
                        holder = holder.getGroup(chain[d], 0)
                    }
                    val idx = chain[chain.size - 1]
                    if (holder.getFieldRepetitionCount(idx) == 0) return@Array null
                    when (leaf.mode) {
                        CopyMode.INT_TO_LONG -> holder.getInteger(idx, 0).toLong()
                        CopyMode.UINT32_TO_LONG -> holder.getInteger(idx, 0).toLong() and 0xFFFF_FFFFL
                        CopyMode.FLOAT_TO_DOUBLE -> holder.getFloat(idx, 0).toDouble()
                        CopyMode.DECIMAL_INT32 -> BigInteger.valueOf(holder.getInteger(idx, 0).toLong())
                        CopyMode.DECIMAL_INT64 -> BigInteger.valueOf(holder.getLong(idx, 0))
                        CopyMode.DECIMAL_BINARY ->
                            decimalOf(keys[i].primitive, holder.getBinary(idx, 0).bytes, source)
                        CopyMode.IDENTITY -> valueOf(kinds[i], keys[i], holder, idx, source)
                    }
                }
        }

        /** Sort keys, then row id. */
        val comparator: Comparator<Row> =
            Comparator { a, b ->
                val c = compareKeys(a.key, b.key)
                if (c != 0) c else a.rowId.compareTo(b.rowId)
            }

        private fun compareKeys(
            a: Array<Any?>,
            b: Array<Any?>,
        ): Int {
            for (i in kinds.indices) {
                val c = compareAt(i, a[i], b[i])
                if (c != 0) return c
            }
            return 0
        }

        /** Key [i]'s order: nulls by the null order whatever the direction, DESC negating the rest. */
        private fun compareAt(
            i: Int,
            av: Any?,
            bv: Any?,
        ): Int {
            if (av == null || bv == null) {
                if (av == null && bv == null) return 0
                return if ((av == null) == nullsFirst[i]) -1 else 1
            }
            val c =
                when (kinds[i]) {
                    KeyKind.BOOLEAN, KeyKind.INT32, KeyKind.UINT32, KeyKind.INT64, KeyKind.UINT64 ->
                        (av as Long).compareTo(bv as Long)
                    KeyKind.FLOAT, KeyKind.DOUBLE -> (av as Double).compareTo(bv as Double)
                    KeyKind.DECIMAL -> (av as BigInteger).compareTo(bv as BigInteger)
                    KeyKind.BYTES -> java.util.Arrays.compareUnsigned(av as ByteArray, bv as ByteArray)
                }
            return if (descending[i]) -c else c
        }

        /** The FIRST sort key's order on two of its values (null = SQL null). */
        fun compareFirst(
            a: Any?,
            b: Any?,
        ): Int = compareAt(0, a, b)

        /**
         * Whether every row of [a] precedes every row of [b] in the merge
         * order — STRICTLY on the first key, so that no tie on it can cross
         * the boundary and no later key or row id needs consulting.
         */
        fun before(
            a: KeyRange,
            b: KeyRange,
        ): Boolean = compareFirst(a.hi, b.lo) < 0

        /** Neither range has a first-key value the other's span reaches. */
        fun disjoint(
            a: KeyRange,
            b: KeyRange,
        ): Boolean = before(a, b) || before(b, a)

        /**
         * The FIRST key's range over every row of the file [footer]
         * describes — deleted rows included, so a superset of the survivors
         * — in merge order, from the column chunks' statistics. Null when
         * the statistics cannot say: the key leaf is not where the output
         * has it or not of the output's exact type (the values would be in
         * another domain), the key is a FLOAT or DOUBLE (parquet leaves NaN
         * out of min/max, and NaN sorts greatest here), or a chunk has no
         * null count, or non-null values but no min/max (a writer that
         * dropped oversized statistics). A file with no rows has no range
         * to speak of and returns null too.
         */
        fun footerRange(footer: ParquetMetadata): KeyRange? {
            val key = keys[0]
            val kind = kinds[0]
            if (kind == KeyKind.FLOAT || kind == KeyKind.DOUBLE) return null
            val schema = footer.fileMetaData.schema
            val leaf =
                try {
                    sortKeyPath(schema, key.field)
                } catch (e: UnconvertibleSchemaException) {
                    return null
                }
            val p = leaf.primitive
            val want = key.primitive
            if (p.primitiveTypeName != want.primitiveTypeName ||
                p.logicalTypeAnnotation != want.logicalTypeAnnotation ||
                p.typeLength != want.typeLength
            ) {
                return null
            }
            val names = ArrayList<String>()
            var group: GroupType = schema
            for ((depth, index) in leaf.path.withIndex()) {
                val type = group.getType(index)
                names += type.name
                if (depth < leaf.path.lastIndex) group = type.asGroupType()
            }
            val path = ColumnPath.get(*names.toTypedArray())
            var range: KeyRange? = null
            for (block in footer.blocks) {
                if (block.rowCount == 0L) continue
                val chunk = block.columns.firstOrNull { it.path == path } ?: return null
                val here = chunkRange(chunk, kind) ?: return null
                range =
                    if (range == null) {
                        here
                    } else {
                        KeyRange(
                            if (compareFirst(here.lo, range.lo) < 0) here.lo else range.lo,
                            if (compareFirst(here.hi, range.hi) > 0) here.hi else range.hi,
                        )
                    }
            }
            return range
        }

        private fun chunkRange(
            chunk: ColumnChunkMetaData,
            kind: KeyKind,
        ): KeyRange? {
            val stats = chunk.statistics ?: return null
            if (!stats.isNumNullsSet) return null
            val nulls = stats.numNulls
            val values = chunk.valueCount
            if (nulls > values || nulls < 0) return null
            if (nulls == values) return KeyRange(null, null)
            if (stats.isEmpty || !stats.hasNonNullValue()) return null
            val min = statValue(stats.genericGetMin(), kind) ?: return null
            val max = statValue(stats.genericGetMax(), kind) ?: return null
            var lo: Any? = if (descending[0]) max else min
            var hi: Any? = if (descending[0]) min else max
            if (nulls > 0) {
                if (nullsFirst[0]) lo = null else hi = null
            }
            return KeyRange(lo, hi)
        }

        /**
         * A statistics bound as the key value [extract] would produce, or
         * null when it cannot be one. The typed casts are a second line:
         * [footerRange] has already required the file's key leaf to be of
         * the output's exact type, which is what makes the statistics'
         * ORDER the merge's (a signed INT32 file under a uint8 column would
         * cast fine and mask negative values to the top of the range).
         */
        private fun statValue(
            raw: Any?,
            kind: KeyKind,
        ): Any? =
            when (kind) {
                KeyKind.BOOLEAN -> (raw as? Boolean)?.let { if (it) 1L else 0L }
                KeyKind.INT32 -> (raw as? Int)?.toLong()
                KeyKind.UINT32 -> (raw as? Int)?.let { it.toLong() and 0xFFFF_FFFFL }
                KeyKind.INT64 -> raw as? Long
                KeyKind.UINT64 -> (raw as? Long)?.let { it xor Long.MIN_VALUE }
                KeyKind.DECIMAL -> (raw as? Binary)?.bytes?.takeIf { it.isNotEmpty() }?.let { BigInteger(it) }
                KeyKind.BYTES -> (raw as? Binary)?.bytes
                // Converted faithfully; [footerRange] refuses these kinds
                // before asking, because NaN is outside the statistics.
                KeyKind.FLOAT -> (raw as? Float)?.toDouble()
                KeyKind.DOUBLE -> raw as? Double
            }
    }

    /**
     * The span of the FIRST sort key over a run's rows, in merge order:
     * [lo] sorts first, [hi] last, a null bound meaning SQL null (placed
     * by the key's null order). See [SortKeys.footerRange].
     */
    internal class KeyRange(val lo: Any?, val hi: Any?)

    private enum class KeyKind { BOOLEAN, INT32, UINT32, INT64, UINT64, FLOAT, DOUBLE, DECIMAL, BYTES }

    private fun kindOf(primitive: PrimitiveType): KeyKind =
        when (primitive.primitiveTypeName) {
            PrimitiveType.PrimitiveTypeName.BOOLEAN -> KeyKind.BOOLEAN
            PrimitiveType.PrimitiveTypeName.INT32 -> if (isUnsigned(primitive)) KeyKind.UINT32 else KeyKind.INT32
            PrimitiveType.PrimitiveTypeName.INT64 -> if (isUnsigned(primitive)) KeyKind.UINT64 else KeyKind.INT64
            PrimitiveType.PrimitiveTypeName.FLOAT -> KeyKind.FLOAT
            PrimitiveType.PrimitiveTypeName.DOUBLE -> KeyKind.DOUBLE
            PrimitiveType.PrimitiveTypeName.BINARY,
            PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
            ->
                if (primitive.logicalTypeAnnotation is LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                    KeyKind.DECIMAL
                } else {
                    KeyKind.BYTES
                }
            PrimitiveType.PrimitiveTypeName.INT96, null ->
                throw IllegalArgumentException("unsupported sort key type ${primitive.primitiveTypeName}")
        }

    /** Same refusal as the copy path: an empty blob is not a decimal. */
    private fun decimalOf(
        primitive: PrimitiveType,
        bytes: ByteArray,
        source: String,
    ): BigInteger {
        if (bytes.isEmpty()) {
            throw InvalidDataException(
                "empty byte array under decimal sort key '${primitive.name}' — " +
                    "a decimal's unscaled value needs at least one byte (in $source)",
            )
        }
        return BigInteger(bytes)
    }
}
