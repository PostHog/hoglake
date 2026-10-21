package com.posthog.hoglake.compaction

import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.isDirectory

/**
 * The process-level half of the sorted rewrite's spill directory
 * (`CompactionConfig.spillDir`): the boot check, the startup sweep and
 * the size the gauge reports. The per-rewrite half — one
 * `hoglake-compaction-spill-<uuid>` directory created at the first spill and removed on
 * every exit path — is [ExternalMergeSort]'s.
 *
 * The sweep deletes ONLY directories hoglake created: every per-rewrite
 * directory is named [PREFIX] + a UUID, and the prefix is long and
 * hoglake's own precisely because the default spill directory is
 * `java.io.tmpdir`, which other tools share (a bare `spill-` would have
 * matched theirs). Within hoglake the directory must still be PRIVATE
 * to one process: two live servers sharing it would sweep each other's
 * in-flight rewrites. The chart's per-pod emptyDir satisfies both.
 */
object SpillDirectory {
    private val log = KotlinLogging.logger {}

    /** Prefix of every per-rewrite spill directory; what the sweep and the gauge match. */
    const val PREFIX = "hoglake-compaction-spill-"

    /**
     * Refuse to boot on a spill directory that cannot take a spill.
     *
     * A BOOT failure rather than a per-group one, deliberately: a missing
     * directory makes `Files.createDirectory` throw `NoSuchFileException`
     * at every sorted group's first spill, which the sweep counts as
     * `failed_groups` and re-plans every interval forever, while the pod
     * looks healthy. Writability is PROBED (a directory created and
     * removed) rather than read off permission bits, because a read-only
     * root filesystem answers the permission question with the bits of a
     * directory nobody can write.
     */
    fun requireUsable(dir: Path) {
        require(dir.isDirectory()) {
            "HOGLAKE_COMPACTION_SPILL_DIR=$dir is not an existing directory: every sorted " +
                "compaction group would fail at its first spill. Point it at a writable volume " +
                "(the chart's /tmp emptyDir is the default via java.io.tmpdir), or turn the " +
                "compaction loop off"
        }
        val probe =
            try {
                Files.createTempDirectory(dir, "hoglake-spill-probe-")
            } catch (e: IOException) {
                throw IllegalArgumentException(
                    "HOGLAKE_COMPACTION_SPILL_DIR=$dir is not writable ($e): every sorted " +
                        "compaction group would fail at its first spill",
                    e,
                )
            }
        Files.deleteIfExists(probe)
    }

    /**
     * Remove every `hoglake-compaction-spill-*` directory under [dir]: what a previous
     * container in the same pod left when it was killed mid-rewrite
     * (an emptyDir outlives a container restart, and nothing else would
     * ever delete them). Run ONCE at startup, before any loop or route
     * can start a rewrite, so it can never race one.
     *
     * Returns the number removed; a failure to remove one is logged,
     * counted, and does not stop the rest.
     */
    fun sweepLeftovers(dir: Path): Int {
        if (!dir.isDirectory()) return 0
        var removed = 0
        val leftovers =
            try {
                Files.newDirectoryStream(dir, "$PREFIX*").use { entries -> entries.filter { it.isDirectory() } }
            } catch (e: IOException) {
                log.warn(e) { "could not list compaction spill directory $dir for leftovers" }
                return 0
            }
        for (leftover in leftovers) {
            if (ExternalMergeSort.deleteSpillDir(leftover)) {
                removed++
            } else {
                com.posthog.hoglake.observability.Metrics.compactionSpillCleanupFailed()
            }
        }
        if (leftovers.isNotEmpty()) {
            log.info {
                "removed $removed of ${leftovers.size} leftover compaction spill director(ies) under " +
                    "$dir from a previous process"
            }
        }
        return removed
    }

    /**
     * Bytes currently held by `hoglake-compaction-spill-*` directories under [dir] — what the
     * spill volume is spending on compaction, as opposed to free space,
     * which on an emptyDir is the NODE's and says nothing about the
     * limit that evicts the pod. Never throws: it backs a gauge, and a
     * supplier that throws fails the whole scrape. A file removed while
     * this walks is simply not counted.
     */
    fun measuredBytes(dir: Path): Long =
        try {
            if (!dir.isDirectory()) {
                0L
            } else {
                Files.newDirectoryStream(dir, "$PREFIX*").use { entries ->
                    entries.filter { it.isDirectory() }.sumOf { entry ->
                        runCatching {
                            Files.walk(entry).use { files ->
                                files
                                    .filter { Files.isRegularFile(it) }
                                    .mapToLong { f -> runCatching { Files.size(f) }.getOrDefault(0L) }
                                    .sum()
                            }
                        }.getOrDefault(0L)
                    }
                }
            }
        } catch (_: Exception) {
            0L
        }
}
