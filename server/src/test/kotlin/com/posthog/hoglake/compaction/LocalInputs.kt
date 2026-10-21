package com.posthog.hoglake.compaction

import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.SortFieldDef
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.io.PositionOutputStream
import java.nio.file.Path

/**
 * A [ParquetRewriter.Input] over a local parquet file.
 *
 * Lives in the TEST source set deliberately. Production compaction reads
 * its inputs in place through `S3InputFile` and never stages an object to
 * disk, so a `Path`-shaped constructor on `Input` itself would be an
 * affordance only tests want — and an invitation to stage to disk again,
 * which is exactly what the streaming reader removed.
 *
 * Tests still want files on disk: writing a fixture is easier than
 * standing up an object store, and what they are exercising is the
 * rewrite, not the transport.
 */
fun localInput(
    path: Path,
    rowIdStart: Long,
    deletes: DeletionVector? = null,
    explicitRowIds: Boolean = false,
    trustedSorted: Boolean = false,
): ParquetRewriter.Input =
    ParquetRewriter.Input(
        source = LocalInputFile(path),
        label = path.toString(),
        rowIdStart = rowIdStart,
        deletes = deletes,
        explicitRowIds = explicitRowIds,
        trustedSorted = trustedSorted,
        fileSizeBytes = path.toFile().length(),
    )

/**
 * Sort bounds no unit test reaches unless it means to: one chunk holds
 * any fixture, the budgets admit any group. Tests about the bounds pass
 * their own [SortSpill].
 */
fun roomySpill(spillDir: Path = defaultSpillDir): SortSpill =
    SortSpill(
        chunkRows = Long.MAX_VALUE,
        mergeBudgetBytes = Long.MAX_VALUE / 4,
        spillBudgetBytes = Long.MAX_VALUE / 4,
        spillDir = spillDir,
    )

private val defaultSpillDir: Path by lazy {
    java.nio.file.Files.createTempDirectory("rewriter-spill").also { it.toFile().deleteOnExit() }
}

/**
 * A local parquet destination that can discard a partial write.
 *
 * The production sink is `S3OutputFile`, which aborts its multipart
 * upload; this is its filesystem twin, and it exists so the rewriter's
 * "nothing survives a throw" contract can be exercised without an
 * object store.
 */
internal class LocalDiscardableOutput(private val path: Path) : DiscardableOutputFile {
    private val delegate = LocalOutputFile(path)

    override fun create(blockSizeHint: Long): PositionOutputStream = delegate.create(blockSizeHint)

    override fun createOrOverwrite(blockSizeHint: Long): PositionOutputStream =
        delegate.createOrOverwrite(blockSizeHint)

    override fun supportsBlockSize(): Boolean = delegate.supportsBlockSize()

    override fun defaultBlockSize(): Long = delegate.defaultBlockSize()

    override fun getPath(): String = path.toString()

    override fun discard() {
        path.toFile().delete()
    }

    override fun toString(): String = path.toString()
}

/** [ParquetRewriter.rewrite] onto a local path, for tests. */
fun rewriteToLocal(
    inputs: List<ParquetRewriter.Input>,
    liveColumns: List<Column>,
    sortFields: List<SortFieldDef>,
    output: Path,
    maxNodesPerRow: Int = ParquetRewriter.DEFAULT_MAX_NODES_PER_ROW,
    codec: ParquetRewriter.OutputCodec = ParquetRewriter.OutputCodec(),
    spill: SortSpill? = if (sortFields.isEmpty()) null else roomySpill(),
): ParquetRewriter.RewriteResult =
    ParquetRewriter.rewrite(
        inputs,
        liveColumns,
        sortFields,
        LocalDiscardableOutput(output),
        maxNodesPerRow,
        codec,
        spill = spill,
    )
