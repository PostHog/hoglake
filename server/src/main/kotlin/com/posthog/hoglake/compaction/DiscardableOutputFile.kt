package com.posthog.hoglake.compaction

import org.apache.parquet.io.OutputFile

/**
 * An [OutputFile] that can throw away a partial write.
 *
 * parquet's own interface has no such notion: it assumes a filesystem,
 * where an abandoned file is merely garbage someone else will notice.
 * Neither destination compaction uses is like that. A truncated local
 * file has no footer and is indistinguishable from a real output if the
 * path is reused; an abandoned multipart upload is invisible as an
 * object and still billed until a lifecycle rule reaps it.
 *
 * So the rewriter asks the destination to clean up after a failure, and
 * each one knows how: unlink the file, or abort the upload.
 */
interface DiscardableOutputFile : OutputFile {
    fun discard()
}
