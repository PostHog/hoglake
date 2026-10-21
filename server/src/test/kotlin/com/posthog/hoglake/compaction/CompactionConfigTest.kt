package com.posthog.hoglake.compaction

import com.posthog.hoglake.Config
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path

/**
 * The compaction knobs as PRODUCTION sees them.
 *
 * Every other derate test constructs its config explicitly, or copies
 * one with an override — which proves the arithmetic and nothing about
 * the value a running server actually uses. Two things stay invisible
 * to that: [CompactionConfig.DEFAULT_NESTED_SORT_EXPANSION] quietly
 * becoming 1, and App's `nestedSortExpansion = cfg.compactionNestedSortExpansion`
 * wiring being dropped. Either restores the unbounded sorted-path heap
 * this branch set out to bound, with the whole suite still green.
 *
 * So the assertions here take the default path deliberately: a config
 * built WITHOUT the argument, and the env-backed [Config] the server
 * boots from.
 */
class CompactionConfigTest {
    private companion object {
        /**
         * `CompactionClaimRepo.groupKey` for spec 7, partition values
         * ["2026-09-24", null] and files {1, 2, 3}. A literal on
         * purpose: see the test that uses it.
         */
        const val PINNED_GROUP_KEY = "9568652afedb97e8fddde6bf02266db215ce8dfd996601f29e228585ba4a540d"
    }

    /** `s struct{a int}` — the shape that triggers the derate. */
    private val nestedColumns =
        listOf(
            Column(
                1,
                0,
                ColumnDef("s", ColType.STRUCT),
                listOf(Column(2, 0, ColumnDef("a", ColType.INT))),
            ),
        )

    private val flatColumns = listOf(Column(1, 0, ColumnDef("id", ColType.LONG)))

    private val target = 512L * 1024 * 1024

    /** A config built the way production builds one: no derate argument. */
    private fun defaulted() =
        CompactionConfig(
            targetBytes = target,
            minInputFiles = 2,
            maxGroupsPerRun = 1,
        )

    @Test
    fun `the default claim lease covers the queued plan for one hour`() {
        assertThat(CompactionConfig.DEFAULT_CLAIM_TTL_SECONDS).isEqualTo(3600)
        assertThat(Config().compactionClaimTtlSeconds).isEqualTo(3600)
    }

    /** Rows of a chunk under [heap] for [nodes] schema nodes plus the row-id carrier. */
    private fun chunkOf(
        heap: Long,
        nodes: Int,
    ) = heap / (CompactionConfig.SORTED_HEAP_BYTES_PER_NODE * (nodes + 1))

    @Test
    fun `a config with NO override shrinks a nested sorted table's chunk`() {
        // The default value itself, exercised through the default path.
        // With DEFAULT_NESTED_SORT_EXPANSION at 1 this is an equality and a
        // nested chunk materializes 30-70x the heap its node count claims.
        // A CHUNK, not a group, since hoglake#134: the derate's only role.
        val cfg = defaulted()
        assertThat(cfg.nestedSortExpansion)
            .describedAs("the default must actually derate; 1 is the no-op")
            .isGreaterThan(1)
        assertThat(cfg.spillChunkRows(nestedColumns))
            .describedAs("a nested SORTED table's chunk is divided by the expansion")
            .isEqualTo(chunkOf(cfg.sortedHeapBytesPerGroup, 2) / cfg.nestedSortExpansion)
    }

    @Test
    fun `the default is large enough for the measured expansion`() {
        // 30-70x measured (a list<long> table peaked at 343 MiB from 4.6
        // MiB compressed). Erring large costs smaller compaction groups;
        // erring small costs an OOM in a background loop, so the floor is
        // the top of the measured band, not the bottom.
        // EXACTLY 64, not "at least". The number is documented — in
        // CompactionConfig's KDoc, in Config's, and in
        // docs/iceberg-federation.md §2.8 — and a `>=` assertion lets the code
        // drift upward while all three keep saying 64. If the value
        // should change, the docs change with it in the same commit.
        assertThat(CompactionConfig.DEFAULT_NESTED_SORT_EXPANSION)
            .describedAs("the documented default, exactly")
            .isEqualTo(64)
    }

    @Test
    fun `a config with NO override leaves a flat table's chunk underated`() {
        // The derate is about nested object graphs. Applying it to a flat
        // table would make its spill files 64x smaller — 64x the runs to
        // merge — for nothing.
        val cfg = defaulted()
        assertThat(cfg.spillChunkRows(flatColumns)).isEqualTo(chunkOf(cfg.sortedHeapBytesPerGroup, 1))
    }

    @Test
    fun `the env-backed Config defaults to the same value the planner does`() {
        // The server boots from Config, not from CompactionConfig's own
        // default. If the two drift, the documented knob and the code's
        // fallback disagree, and which one you get depends on whether the
        // wiring below is present.
        assertThat(Config().compactionNestedSortExpansion)
            .isEqualTo(CompactionConfig.DEFAULT_NESTED_SORT_EXPANSION)
    }

    @Test
    fun `App wires the env knob into the planner's config`() {
        // Read off disk, like ScalarTypeParityTest reads the enum and the
        // migration: a Kotlin test cannot reach App's private
        // compactionService, and the failure mode — the wiring line being
        // dropped — is silent precisely because CompactionConfig's own
        // default equals Config's, so every default-valued deployment
        // behaves identically and only an operator who SET the env var
        // discovers the knob does nothing.
        val app = Files.readString(Path.of("src/main/kotlin/com/posthog/hoglake/App.kt"))
        assertThat(app)
            .describedAs("App must pass Config.compactionNestedSortExpansion into CompactionConfig")
            .containsPattern("""nestedSortExpansion\s*=\s*cfg\.compactionNestedSortExpansion""")
    }

    // ---- maxNodesPerRow: the same four things, for the same reasons ----
    //
    // Added in the same branch as the derate above and pinned by
    // nothing, which is the identical hole this class was written to
    // close: DEFAULT_MAX_NODES_PER_ROW quietly becoming small enough to
    // refuse honest rows, or App's wiring line being dropped so the knob
    // does nothing, both with the whole suite green.

    @Test
    fun `a config with NO override carries the per-row node budget`() {
        assertThat(defaulted().maxNodesPerRow)
            .isEqualTo(ParquetRewriter.DEFAULT_MAX_NODES_PER_ROW)
    }

    @Test
    fun `the env-backed Config defaults to the same per-row budget the rewriter does`() {
        assertThat(Config().compactionMaxNodesPerRow)
            .isEqualTo(ParquetRewriter.DEFAULT_MAX_NODES_PER_ROW)
    }

    @Test
    fun `App wires the per-row budget into the planner's config`() {
        val app = Files.readString(Path.of("src/main/kotlin/com/posthog/hoglake/App.kt"))
        assertThat(app)
            .describedAs("App must pass Config.compactionMaxNodesPerRow into CompactionConfig")
            .containsPattern("""maxNodesPerRow\s*=\s*cfg\.compactionMaxNodesPerRow""")
    }

    @Test
    fun `a per-row budget of zero or less is refused at construction`() {
        for (bad in listOf(0, -1)) {
            assertThatThrownBy {
                CompactionConfig(
                    targetBytes = target,
                    maxGroupsPerRun = 1,
                    maxNodesPerRow = bad,
                )
            }.describedAs("maxNodesPerRow=%d", bad).isInstanceOf(IllegalArgumentException::class.java)
        }
    }

    @Test
    fun `the documented per-row budget is the one the code uses`() {
        // A default that drifts from its documentation is the same
        // failure as one that drifts from App: the operator sizing a
        // heap reads the number in the docs, not the constant.
        val documented = "1,000,000"
        for (doc in listOf("README.md", "../docs/iceberg-federation.md")) {
            assertThat(Files.readString(Path.of(doc)))
                .describedAs("%s must document HOGLAKE_COMPACTION_MAX_NODES_PER_ROW's real default", doc)
                .contains("HOGLAKE_COMPACTION_MAX_NODES_PER_ROW")
                .contains(documented)
        }
        assertThat(ParquetRewriter.DEFAULT_MAX_NODES_PER_ROW)
            .isEqualTo(documented.replace(",", "").toInt())
    }

    // ---- the output codec: the same four things again ------------------
    //
    // Same hole, same shape. The codec that matters is the one a running
    // server writes with, and CompactionCodecTest proves the rewriter
    // honours whatever it is handed — which is silent about whether
    // production hands it anything. UNCOMPRESSED was never chosen here;
    // it was inherited from ExampleParquetWriter's default, and the way
    // it stays chosen again is App's wiring line going away while
    // OutputCodec's own default keeps every test green.

    @Test
    fun `a config with NO override carries the rewriter's default codec`() {
        assertThat(defaulted().codec).isEqualTo(ParquetRewriter.OutputCodec())
        assertThat(defaulted().codec.name)
            .describedAs("compaction must not write UNCOMPRESSED by default")
            .isNotEqualTo(CompressionCodecName.UNCOMPRESSED)
    }

    @Test
    fun `the env-backed Config defaults to the same codec and level the rewriter does`() {
        assertThat(ParquetRewriter.OutputCodec.parse(Config().compactionCodec, Config().compactionZstdLevel))
            .isEqualTo(ParquetRewriter.OutputCodec())
        assertThat(Config().compactionZstdLevel).isEqualTo(ParquetRewriter.DEFAULT_ZSTD_LEVEL)
    }

    @Test
    fun `App wires the codec env knobs into the planner's config`() {
        val app = Files.readString(Path.of("src/main/kotlin/com/posthog/hoglake/App.kt"))
        assertThat(app)
            .describedAs("App must pass Config.compactionCodec and compactionZstdLevel into CompactionConfig")
            .containsPattern("""codec\s*=\s*ParquetRewriter\.OutputCodec\.parse\(""")
            .contains("cfg.compactionCodec")
            .contains("cfg.compactionZstdLevel")
    }

    @Test
    fun `an unusable codec name is refused at construction, not at the first rewrite`() {
        // The knob is a string an operator sets. A name this server
        // cannot write must fail at boot; failing per-group inside the
        // sweep is a catch-all counter rising forever with the cause
        // buried in a log line.
        assertThatThrownBy {
            CompactionConfig(
                targetBytes = target,
                maxGroupsPerRun = 1,
                codec = ParquetRewriter.OutputCodec.parse("lzo"),
            )
        }.isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `the documented codec default is the one the code uses`() {
        // Same rule as the node budget below: an operator sizing a
        // maintenance pod's CPU reads the docs, not the constant.
        for (doc in listOf("README.md", "../docs/iceberg-federation.md")) {
            assertThat(Files.readString(Path.of(doc)))
                .describedAs("%s must document HOGLAKE_COMPACTION_CODEC's real default", doc)
                .contains("HOGLAKE_COMPACTION_CODEC")
                .contains("HOGLAKE_COMPACTION_ZSTD_LEVEL")
                .containsPattern("""(?i)zstd_level[^.]{0,40}\*\*${ParquetRewriter.DEFAULT_ZSTD_LEVEL}\*\*""")
        }
        assertThat(ParquetRewriter.DEFAULT_CODEC).isEqualTo(CompressionCodecName.ZSTD)
    }

    @Test
    fun `a derate of zero or less is refused at construction`() {
        // The knob is operator-settable; 0 would divide by zero and a
        // negative would invert the budget. Fail at boot, not on the
        // first sweep of a nested table.
        for (bad in listOf(0, -1)) {
            assertThatThrownBy {
                CompactionConfig(
                    targetBytes = target,
                    minInputFiles = 2,
                    maxGroupsPerRun = 1,
                    nestedSortExpansion = bad,
                )
            }
                .describedAs("nestedSortExpansion=%d", bad)
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("nested sort expansion")
        }
        // 1 is legal and documented: it disables the derate for an
        // operator who has sized the heap for it.
        val undivided =
            CompactionConfig(
                targetBytes = target,
                minInputFiles = 2,
                maxGroupsPerRun = 1,
                nestedSortExpansion = 1,
            )
        assertThat(undivided.spillChunkRows(nestedColumns)).isEqualTo(chunkOf(undivided.sortedHeapBytesPerGroup, 2))
    }

    // ---- sortedHeapBytes: the chunk and the merge (hoglake#134) ----
    //
    // Same four checks again (default, env parity, App wiring, docs),
    // because the same hole is here: this knob has a default on both
    // sides and a wiring line in App, and every one of them can go
    // quiet. What changed is what the arithmetic below it bounds: a
    // CHUNK of the external merge sort and its merge, never the group.

    /** Ten thousand rows of [flatColumns] (one data node + the row-id carrier). */
    private fun heapForTenThousandFlatRows() = CompactionConfig.SORTED_HEAP_BYTES_PER_NODE * 2 * 10_000

    @Test
    fun `the old row ceiling's arithmetic sizes a chunk, and the sort spill carries every knob`() {
        // The sorted row ceiling used to be a GROUP capacity; the same
        // arithmetic now sizes one chunk, and the group packs to the
        // target. `sortSpill` is the one function the planner's refusals
        // and the rewrite both build their bounds from, so it has to carry
        // each knob through unchanged.
        val dir = Files.createTempDirectory("spill-cfg")
        val cfg =
            CompactionConfig(
                targetBytes = target,
                maxGroupsPerRun = 1,
                sortedHeapBytes = heapForTenThousandFlatRows(),
                spillBytes = 12_345,
                spillDir = dir,
            )
        assertThat(cfg.spillChunkRows(flatColumns)).isEqualTo(10_000)
        val spill = cfg.sortSpill(flatColumns)
        assertThat(spill.chunkRows).isEqualTo(10_000)
        assertThat(spill.mergeBudgetBytes).isEqualTo(cfg.sortedHeapBytesPerGroup)
        assertThat(spill.spillBudgetBytes).isEqualTo(12_345)
        assertThat(spill.spillDir).isEqualTo(dir)
        assertThat(spill.verifyMinBytes).isEqualTo(cfg.verifyMinBytes)
        assertThat(cfg.copy(verifyMinBytes = 77).sortSpill(flatColumns).verifyMinBytes).isEqualTo(77)
        assertThat(cfg.sortSpill(nestedColumns).chunkRows)
            .describedAs("nested: the chunk is divided by the expansion, and only the chunk")
            .isEqualTo(maxOf(1L, chunkOf(cfg.sortedHeapBytesPerGroup, 2) / cfg.nestedSortExpansion))
    }

    @Test
    fun `the chunk never falls below one row`() {
        // A heap budget smaller than one row, or a nested derate that
        // divides it to nothing, must still make progress: a chunk of zero
        // would spill nothing and SortSpill refuses it.
        val cfg = CompactionConfig(targetBytes = target, maxGroupsPerRun = 1, sortedHeapBytes = 1)
        assertThat(cfg.spillChunkRows(nestedColumns)).isEqualTo(1)
        cfg.sortSpill(nestedColumns) // must not throw
    }

    // ---- spillBytes / spillDir (hoglake#134) ----

    @Test
    fun `the spill knobs default the same on both sides and App wires them`() {
        assertThat(defaulted().spillBytes).isEqualTo(CompactionConfig.DEFAULT_SPILL_BYTES)
        assertThat(CompactionConfig.DEFAULT_SPILL_BYTES).isEqualTo(4L * 1024 * 1024 * 1024)
        assertThat(Config().compactionSpillBytes).isEqualTo(CompactionConfig.DEFAULT_SPILL_BYTES)
        assertThat(defaulted().spillDir).isEqualTo(Path.of(System.getProperty("java.io.tmpdir")))
        assertThat(Config().compactionSpillDir).isEqualTo(System.getProperty("java.io.tmpdir"))
        val app = Files.readString(Path.of("src/main/kotlin/com/posthog/hoglake/App.kt"))
        assertThat(app)
            .containsPattern("""spillBytes\s*=\s*cfg\.compactionSpillBytes""")
            .containsPattern("""spillDir\s*=\s*java\.nio\.file\.Path\.of\(cfg\.compactionSpillDir\)""")
        for (doc in listOf("README.md", "../docs/iceberg-federation.md")) {
            assertThat(Files.readString(Path.of(doc)))
                .describedAs("%s must document the spill knobs", doc)
                .contains("HOGLAKE_COMPACTION_SPILL_BYTES")
                .contains("HOGLAKE_COMPACTION_SPILL_DIR")
        }
    }

    @Test
    fun `the verification floor defaults to the spill block on both sides, App wires it, and negative is refused`() {
        assertThat(defaulted().verifyMinBytes).isEqualTo(CompactionConfig.DEFAULT_VERIFY_MIN_BYTES)
        assertThat(CompactionConfig.DEFAULT_VERIFY_MIN_BYTES).isEqualTo(SortSpill.SPILL_BLOCK_BYTES.toLong())
        assertThat(Config().compactionVerifyMinBytes).isEqualTo(CompactionConfig.DEFAULT_VERIFY_MIN_BYTES)
        assertThat(Files.readString(Path.of("src/main/kotlin/com/posthog/hoglake/App.kt")))
            .containsPattern("""verifyMinBytes\s*=\s*cfg\.compactionVerifyMinBytes""")
        assertThat(Files.readString(Path.of("README.md"))).contains("HOGLAKE_COMPACTION_VERIFY_MIN_BYTES")
        CompactionConfig(targetBytes = target, maxGroupsPerRun = 1, verifyMinBytes = 0) // 0 checks everything
        assertThatThrownBy { CompactionConfig(targetBytes = target, maxGroupsPerRun = 1, verifyMinBytes = -1) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_COMPACTION_VERIFY_MIN_BYTES")
        assertThatThrownBy { Config(compactionVerifyMinBytes = -1) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_COMPACTION_VERIFY_MIN_BYTES")
    }

    @Test
    fun `a spill budget of zero or less is refused at construction and at boot`() {
        for (bad in listOf(0L, -1L)) {
            assertThatThrownBy { CompactionConfig(targetBytes = target, maxGroupsPerRun = 1, spillBytes = bad) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("HOGLAKE_COMPACTION_SPILL_BYTES")
            assertThatThrownBy { Config(compactionSpillBytes = bad) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("HOGLAKE_COMPACTION_SPILL_BYTES")
        }
    }

    @Test
    fun `a spill directory that is missing or unwritable is refused at BOOT where the loop runs`() {
        // The failure this exists for: without it a missing directory is
        // a NoSuchFileException at every sorted group's first spill —
        // `failed_groups`, re-planned every interval, a healthy-looking
        // pod. Priced per workload like the pool check: an API pod with
        // the loop off may have no writable volume at all.
        val missing = Files.createTempDirectory("spill-boot").resolve("nope")
        assertThatThrownBy {
            Config(compactionIntervalMs = 3_600_000, compactionSpillDir = missing.toString())
        }.isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_COMPACTION_SPILL_DIR")
            .hasMessageContaining("not an existing directory")
        val readOnly = Files.createTempDirectory("spill-boot-ro")
        readOnly.toFile().setWritable(false)
        try {
            assertThatThrownBy {
                Config(compactionIntervalMs = 3_600_000, compactionSpillDir = readOnly.toString())
            }.isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("not writable")
        } finally {
            readOnly.toFile().setWritable(true)
        }
        // With the loop off the same directory boots.
        Config(compactionIntervalMs = 0, compactionSpillDir = missing.toString())
        // And a usable one boots with the loop on, leaving nothing behind.
        val ok = Files.createTempDirectory("spill-boot-ok")
        Config(compactionIntervalMs = 3_600_000, compactionSpillDir = ok.toString())
        assertThat(Files.list(ok).use { it.count() }).describedAs("the probe cleans up after itself").isZero()
    }

    @Test
    fun `the startup sweep removes leftover spill directories and nothing else`() {
        val dir = Files.createTempDirectory("spill-sweep")
        val leftover = Files.createDirectories(dir.resolve("${SpillDirectory.PREFIX}dead-process"))
        Files.write(leftover.resolve("spill-1.parquet"), ByteArray(1234))
        val bystanderDir = Files.createDirectories(dir.resolve("not-ours"))
        val bystanderFile = Files.write(dir.resolve("${SpillDirectory.PREFIX}a-file-not-a-dir"), ByteArray(7))
        // ANOTHER tool's spill directory in the same shared temp dir —
        // the default is java.io.tmpdir — must survive: the sweep owns
        // only the directories hoglake named.
        val foreign = Files.createDirectories(dir.resolve("spill-someone-elses"))
        Files.write(foreign.resolve("x"), ByteArray(3))
        assertThat(SpillDirectory.PREFIX).startsWith("hoglake-")
        assertThat(SpillDirectory.measuredBytes(dir)).describedAs("only spill-* directories' files").isEqualTo(1234)
        assertThat(SpillDirectory.sweepLeftovers(dir)).isEqualTo(1)
        assertThat(leftover).doesNotExist()
        assertThat(bystanderDir).exists()
        assertThat(bystanderFile).exists()
        assertThat(foreign).describedAs("a bare spill-* directory is not hoglake's").exists()
        assertThat(SpillDirectory.measuredBytes(dir)).isZero()
        // A directory that is not there is nothing to sweep, not an error.
        assertThat(SpillDirectory.sweepLeftovers(dir.resolve("absent"))).isZero()
        assertThat(SpillDirectory.measuredBytes(dir.resolve("absent"))).isZero()
        // Wired once, before the loops, and only where the loop runs.
        val app = Files.readString(Path.of("src/main/kotlin/com/posthog/hoglake/App.kt"))
        assertThat(app)
            .containsPattern(
                """(?s)fun startBackground\(\): AutoCloseable \{.{0,900}if \(cfg\.compactionIntervalMs > 0\) \{\s*""" +
                    """com\.posthog\.hoglake\.compaction\.SpillDirectory\.sweepLeftovers""" +
                    """.{0,200}val loops = BackgroundLoops\(\)""",
            )
    }

    @Test
    fun `a config with NO override carries the sorted heap budget`() {
        assertThat(defaulted().sortedHeapBytes).isEqualTo(CompactionConfig.DEFAULT_SORTED_HEAP_BYTES)
    }

    @Test
    fun `the env-backed Config defaults to the same sorted heap budget the planner does`() {
        assertThat(Config().compactionSortedHeapBytes)
            .isEqualTo(CompactionConfig.DEFAULT_SORTED_HEAP_BYTES)
    }

    @Test
    fun `App wires the sorted heap budget into the planner's config`() {
        val app = Files.readString(Path.of("src/main/kotlin/com/posthog/hoglake/App.kt"))
        assertThat(app)
            .describedAs("App must pass Config.compactionSortedHeapBytes into CompactionConfig")
            .containsPattern("""sortedHeapBytes\s*=\s*cfg\.compactionSortedHeapBytes""")
    }

    @Test
    fun `a sorted heap budget of zero or less is refused at construction`() {
        for (bad in listOf(0L, -1L)) {
            assertThatThrownBy {
                CompactionConfig(
                    targetBytes = target,
                    maxGroupsPerRun = 1,
                    sortedHeapBytes = bad,
                )
            }
                .describedAs("sortedHeapBytes=%d", bad)
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("sorted heap bytes")
        }
    }

    @Test
    fun `the documented sorted heap default is the one the code uses`() {
        for (doc in listOf("README.md", "../docs/iceberg-federation.md")) {
            assertThat(Files.readString(Path.of(doc)))
                .describedAs("%s must document HOGLAKE_COMPACTION_SORTED_HEAP_BYTES's real default", doc)
                .contains("HOGLAKE_COMPACTION_SORTED_HEAP_BYTES")
                .contains("1 GiB")
        }
        assertThat(CompactionConfig.DEFAULT_SORTED_HEAP_BYTES).isEqualTo(1024L * 1024 * 1024)
    }

    @Test
    fun `the default is sized for the pod the maintenance deployment actually has`() {
        // The knob is only honest if its default is safe on the pod that
        // exists. 4 GiB at MaxRAMPercentage=70 is ~2.8 GiB of heap. The
        // worst-case process peak since the external merge sort
        // (hoglake#134) is the LARGER of its two phases, plus the
        // hydrator's 256 MiB whole-object ceiling firing in the same tick:
        // the MERGE — the admitted runs charged up to the budget, the
        // OUTPUT writer's buffered row group (measured flat at ~147 MB,
        // SortedRewriteHeapMeasurement, since the materializer stopped its
        // statistics pinning input pages) and the 16 MiB S3 part buffer —
        // and the CHUNK phase — the chunk at ~0.79x the budget, a
        // target-sized 512 MiB input row group, the spill writer's slabs
        // and the sort's reference array (~37 MiB).
        //
        // Pinned as an inequality against the REAL heap rather than as
        // an equality on the constant: the failure this guards is
        // someone raising the default (or a term growing) without the
        // charts change that makes it survivable. The honest formula puts
        // the default at 56%, stated in server/README.md.
        val mib = 1024.0 * 1024
        val heap = 4.0 * 1024 * mib * 0.70
        val budget = CompactionConfig.DEFAULT_SORTED_HEAP_BYTES.toDouble()
        val outputWriter = 147_502.0 * 1024
        val merge = budget + outputWriter + 16 * mib
        val chunk = 0.79 * budget + 512 * mib + 37 * mib
        val peak = maxOf(merge, chunk) + 256 * mib
        assertThat(peak / heap)
            .describedAs(
                "worst-case peak %.0f MiB against a %.0f MiB heap — raise the pod before the knob",
                peak / mib,
                heap / mib,
            )
            .isLessThan(0.7)
    }

    @Test
    fun `the sorted heap knob no longer calls itself temporary, and the docs describe the spill`() {
        // The cap is gone: the bound that called itself TEMPORARY until an
        // external merge sort replaced it has been replaced by one. What
        // must not survive is the old promise — an operator reading
        // "TEMPORARY, the group is sorted in memory" would size a pod for
        // a mechanism that no longer exists.
        val source = Files.readString(Path.of("src/main/kotlin/com/posthog/hoglake/compaction/CompactionService.kt"))
        assertThat(source)
            .doesNotContain("THIS BOUND IS TEMPORARY")
            .doesNotContain("sortedRowCeiling")
            .doesNotContain("effectiveTargetBytes")
            .contains("external merge sort")
        for (doc in listOf("README.md", "../docs/iceberg-federation.md")) {
            assertThat(Files.readString(Path.of(doc)))
                .describedAs("%s must describe the external merge sort and its spill", doc)
                .containsIgnoringCase("external merge sort")
                .containsIgnoringCase("spill")
                .containsIgnoringCase("advisory")
        }
    }

    @Test
    fun `the image sizes the heap for the container it was given`() {
        // The other half of #118, and the half no runtime assertion can
        // reach: with no flag at all the JVM takes its container default
        // of 25%, so the 4 GiB maintenance pod ran compaction on ~1 GiB
        // of heap. Read off disk, like the App wiring checks above —
        // build.gradle.kts is what generates the start script the image's
        // ENTRYPOINT runs, and the failure mode is the line going away
        // with every test still green.
        val build = Files.readString(Path.of("build.gradle.kts"))
        val args =
            Regex("""applicationDefaultJvmArgs\s*=\s*listOf\(([^)]*)\)""")
                .find(build)
                ?.groupValues
                ?.get(1)
        assertThat(args)
            .describedAs("build.gradle.kts must set applicationDefaultJvmArgs")
            .isNotNull()
        val percent =
            Regex("""-XX:MaxRAMPercentage=(\d+(?:\.\d+)?)""").find(args!!)?.groupValues?.get(1)?.toDouble()
        assertThat(percent)
            .describedAs("the container heap percentage must be set, and be neither the 25% default nor 100%")
            .isNotNull()
        assertThat(percent!!).isGreaterThan(50.0).isLessThanOrEqualTo(80.0)
        // The image must not duplicate it: two sources for one number is
        // how they drift, and ENV JAVA_OPTS in the Dockerfile would be
        // replaced wholesale by any deploy that sets JAVA_OPTS.
        val dockerDirectives =
            Files.readString(Path.of("Dockerfile"))
                .lines()
                .filterNot { it.trimStart().startsWith("#") }
                .joinToString("\n")
        assertThat(dockerDirectives)
            .describedAs("JVM flags belong in applicationDefaultJvmArgs, not in the image's env")
            .doesNotContain("JAVA_OPTS")
            .doesNotContain("MaxRAMPercentage")
    }

    @Test
    fun `the group bounds are rejected at CONSTRUCTION, not at plan time`() {
        // Construction is boot. The same `require`s also live in
        // CompactionGrouping.groups, but that runs once per bucket per
        // table per sweep, inside planSnapshot -- which sits OUTSIDE the
        // per-group catch, so a bad value there kills the whole sweep for
        // every catalog on every interval while the process still looks
        // healthy. Catching it here turns that into a boot failure.
        for (bad in listOf(1, 0, -1)) {
            assertThatThrownBy {
                CompactionConfig(targetBytes = 1024, minInputFiles = bad, maxGroupsPerRun = 1)
            }.describedAs("minInputFiles=%d", bad)
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("HOGLAKE_COMPACTION_MIN_INPUT_FILES")
        }
        assertThatThrownBy {
            CompactionConfig(
                targetBytes = 1024,
                minInputFiles = 5,
                maxInputFiles = 4,
                maxGroupsPerRun = 1,
            )
        }.isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_COMPACTION_MAX_INPUT_FILES")
        // And the defaults are the documented ones.
        val cfg = CompactionConfig(targetBytes = 1024, maxGroupsPerRun = 1)
        assertThat(cfg.minInputFiles).isEqualTo(CompactionGrouping.DEFAULT_MIN_INPUT_FILES)
        assertThat(cfg.maxInputFiles).isEqualTo(CompactionGrouping.DEFAULT_MAX_INPUT_FILES)
    }

    // ---- concurrency knobs -------------------------------------------------

    @Test
    fun `the default config is the SEQUENTIAL sweep, so an existing deployment changes in no way`() {
        // The promise made to every values file that sets nothing:
        // parallelGroups 1 means one worker and no thread hop, and the
        // sorted-path budget is the undivided one. If
        // DEFAULT_PARALLEL_GROUPS ever moves, every sorted table's group
        // size moves with it silently — this is the assertion that
        // notices.
        val cfg = defaulted()
        assertThat(cfg.parallelGroups).isEqualTo(1)
        assertThat(cfg.sortedHeapBytesPerGroup).isEqualTo(cfg.sortedHeapBytes)
        assertThat(cfg.spillChunkRows(flatColumns))
            .isEqualTo(cfg.copy(parallelGroups = 1).spillChunkRows(flatColumns))
    }

    @Test
    fun `N concurrent sorted groups cannot exceed the heap one group was allowed`() {
        // THE heap-gating property, stated as the arithmetic it is: the
        // budget is DIVIDED, so N groups each at their ceiling cost what
        // one group used to. Asserted in the unit the heap holds — rows
        // times the per-node cost — rather than on the knob, because the
        // knob is not what OOMs.
        val heap = 1024L * 1024 * 1024
        val one = CompactionConfig(targetBytes = target, maxGroupsPerRun = 1, sortedHeapBytes = heap)
        val nodesPlusCarrier = flatColumns.size + 1
        val perRowBytes = CompactionConfig.SORTED_HEAP_BYTES_PER_NODE * nodesPlusCarrier
        for (n in listOf(1, 2, 4, 8, 64)) {
            val many = one.copy(parallelGroups = n)
            val concurrentBytes = many.spillChunkRows(flatColumns) * perRowBytes * n
            assertThat(concurrentBytes)
                .describedAs("%d concurrent sorted chunks", n)
                .isLessThanOrEqualTo(one.spillChunkRows(flatColumns) * perRowBytes)
            assertThat(many.sortSpill(flatColumns).mergeBudgetBytes * n)
                .describedAs("%d concurrent sorted merges", n)
                .isLessThanOrEqualTo(heap)
        }
    }

    @Test
    fun `the divided heap budget reaches both bounds the rewrite runs under`() {
        // The chunk and the merge both derive from the same heap; a
        // division that reached one and not the other would let N groups
        // take N times the heap in that phase.
        val one = CompactionConfig(targetBytes = target, maxGroupsPerRun = 1)
        val eight = one.copy(parallelGroups = 8)
        assertThat(eight.spillChunkRows(flatColumns) * 8)
            .describedAs("eight groups' chunks must add up to one group's")
            .isLessThanOrEqualTo(one.spillChunkRows(flatColumns) + 8)
            .isGreaterThan(0)
        assertThat(eight.sortSpill(flatColumns).mergeBudgetBytes * 8)
            .isLessThanOrEqualTo(one.sortSpill(flatColumns).mergeBudgetBytes)
        assertThat(eight.sortSpill(nestedColumns).chunkRows * 8)
            .describedAs("the nested chunk is divided too")
            .isLessThanOrEqualTo(one.sortSpill(nestedColumns).chunkRows + 8)
        // The SPILL budget is per group and NOT divided: it bounds one
        // rewrite's disk, and the chart sizes the volume as N x it.
        assertThat(eight.sortSpill(flatColumns).spillBudgetBytes).isEqualTo(one.spillBytes)
    }

    @Test
    fun `the concurrency knobs are rejected at CONSTRUCTION, not at plan time`() {
        // Same argument as the group bounds above: a bad value caught
        // inside the sweep throws out of planSnapshot, which is outside
        // the per-group catch, so it kills every catalog's sweep on every
        // interval while the process still looks healthy.
        for (bad in listOf(0, -1)) {
            assertThatThrownBy {
                CompactionConfig(targetBytes = 1024, maxGroupsPerRun = 1, parallelGroups = bad)
            }.describedAs("parallelGroups=%d", bad)
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("HOGLAKE_COMPACTION_PARALLEL_GROUPS")
            assertThatThrownBy {
                CompactionConfig(targetBytes = 1024, maxGroupsPerRun = 1, inputOpenParallelism = bad)
            }.describedAs("inputOpenParallelism=%d", bad)
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("HOGLAKE_COMPACTION_PARALLEL_INPUT_OPENS")
            assertThatThrownBy {
                CompactionConfig(targetBytes = 1024, maxGroupsPerRun = 1, claimTtlSeconds = bad.toLong())
            }.describedAs("claimTtlSeconds=%d", bad)
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("HOGLAKE_COMPACTION_CLAIM_TTL_SECONDS")
        }
    }

    @Test
    fun `the candidate-read bounds are rejected at CONSTRUCTION, not at plan time`() {
        // Same argument as every knob above: a bad value caught inside
        // the sweep throws out of planSnapshot, which is outside the
        // per-group catch, so it kills every catalog's sweep on every
        // interval while the process still looks healthy.
        //
        // BOTH DIRECTIONS, and the upper one is the point: a
        // fat-fingered HOGLAKE_COMPACTION_MAX_CANDIDATES is the
        // 2026-09-30 planning read again, and `candidateBudget`'s
        // coercion prevents the overflow but not the absurdity.
        for (bad in listOf(0, -1, CompactionConfig.MAX_CANDIDATE_HEADROOM + 1)) {
            assertThatThrownBy {
                CompactionConfig(targetBytes = 1024, maxGroupsPerRun = 1, candidateHeadroom = bad)
            }.describedAs("candidateHeadroom=%d", bad)
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("HOGLAKE_COMPACTION_CANDIDATE_HEADROOM")
        }
        for (bad in listOf(0, 1, CompactionConfig.MAX_MAX_CANDIDATES + 1)) {
            assertThatThrownBy {
                CompactionConfig(targetBytes = 1024, maxGroupsPerRun = 1, maxCandidates = bad)
            }.describedAs("maxCandidates=%d", bad)
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("HOGLAKE_COMPACTION_MAX_CANDIDATES")
        }
        // The fan-in CEILING cannot be under its own floor.
        assertThatThrownBy {
            CompactionConfig(targetBytes = 1024, maxGroupsPerRun = 1, maxInputFiles = 64, maxFanIn = 8)
        }.isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_COMPACTION_MAX_FAN_IN")
    }

    @Test
    fun `the fan-in scales with the files it caps, between its floor and its ceiling`() {
        // The knob that replaced a fixed 64, and the arithmetic is the
        // whole of it: `targetBytes / p50`, held inside
        // [maxInputFiles, maxFanIn].
        val cfg = CompactionConfig(targetBytes = 512L * 1024 * 1024, maxGroupsPerRun = 1)

        assertThat(cfg.effectiveMaxInputFiles(List(9) { 12L * 1024 }))
            .describedAs("12 KiB files: 512 MiB / 12 KiB is ~43,690, so the CEILING binds")
            .isEqualTo(cfg.maxFanIn)
        assertThat(cfg.effectiveMaxInputFiles(List(9) { 200L * 1024 * 1024 }))
            .describedAs("200 MB files: two fill the target, so the FLOOR binds")
            .isEqualTo(cfg.maxInputFiles)
        assertThat(cfg.effectiveMaxInputFiles(List(9) { 1L * 1024 * 1024 }))
            .describedAs("1 MiB files: 512 of them fill the target, and that is between the two")
            .isEqualTo(512)

        // THE MEDIAN, not the mean, and this is the fixture that
        // separates them: one near-target file among eight tiny ones.
        // The mean would be ~57 MiB and cap the fan-in at 9; the median
        // is 12 KiB and caps it at the ceiling, which is right, because
        // the packer's dominance split keeps the big file out of the
        // small files' group anyway.
        val skewed = List(8) { 12L * 1024 } + listOf(500L * 1024 * 1024)
        assertThat(cfg.effectiveMaxInputFiles(skewed))
            .describedAs("one near-target file must not shrink the whole bucket's fan-in")
            .isEqualTo(cfg.maxFanIn)

        // No candidates: nothing to measure, so the floor.
        assertThat(cfg.effectiveMaxInputFiles(emptyList())).isEqualTo(cfg.maxInputFiles)
        // A registered zero-byte file is legal, and dividing by it is not.
        assertThat(cfg.effectiveMaxInputFiles(List(3) { 0L })).isEqualTo(cfg.maxFanIn)
    }

    @Test
    fun `the candidate budget is the smaller of the run's capacity and the hard cap`() {
        // `min(headroom x maxGroupsPerRun x maxFanIn, maxCandidates)`.
        // At the shipped defaults the first term is 2 x 64 x 2,048 =
        // 262,144 and the second is 50,000, so the CAP is the binding
        // term — which is deliberate, and is the number an operator
        // reads `candidates_fetched` against.
        val shipped = CompactionConfig(targetBytes = 1024, maxGroupsPerRun = 64)
        assertThat(shipped.candidateBudget).isEqualTo(CompactionConfig.DEFAULT_MAX_CANDIDATES)
        // And the other term binds when it is the smaller one.
        val small = CompactionConfig(targetBytes = 1024, maxGroupsPerRun = 1, maxFanIn = 64)
        assertThat(small.candidateBudget)
            .describedAs("1 group x 64 files x headroom 2")
            .isEqualTo(128)
    }

    @Test
    fun `the env surface production boots from carries the candidate-read knobs through`() {
        // The wiring, for the same reason the concurrency test below
        // checks its own: a dropped `maxFanIn =` line in App would
        // leave a values file's setting inert with the whole suite
        // green.
        assertThat(Config().compactionCandidateHeadroom)
            .isEqualTo(CompactionConfig.DEFAULT_CANDIDATE_HEADROOM)
        assertThat(Config().compactionMaxCandidates).isEqualTo(CompactionConfig.DEFAULT_MAX_CANDIDATES)
        assertThat(Config().compactionMaxFanIn).isEqualTo(CompactionConfig.DEFAULT_MAX_FAN_IN)
    }

    @Test
    fun `the env surface production boots from carries the concurrency knobs through`() {
        // The wiring, not the arithmetic: App builds CompactionConfig
        // from these Config fields, and a dropped `parallelGroups =`
        // line would leave a values file's setting inert with the whole
        // suite green — the same invisibility the nested derate test at
        // the top of this file exists for.
        val cfg = Config.fromEnv()
        assertThat(cfg.compactionParallelGroups)
            .describedAs("default must be the sequential sweep")
            .isEqualTo(CompactionConfig.DEFAULT_PARALLEL_GROUPS)
        assertThat(cfg.compactionParallelInputOpens)
            .describedAs("input opens default ON: it is bounded, order-preserving and free")
            .isEqualTo(8)
        assertThat(cfg.compactionClaimsEnabled).isTrue()
        assertThat(cfg.compactionClaimTtlSeconds)
            .isEqualTo(CompactionConfig.DEFAULT_CLAIM_TTL_SECONDS)
        val app =
            CompactionConfig(
                targetBytes = cfg.compactionTargetBytes,
                minInputFiles = cfg.compactionMinInputFiles,
                maxInputFiles = cfg.compactionMaxInputFiles,
                maxGroupsPerRun = cfg.compactionMaxGroupsPerRun,
                parallelGroups = cfg.compactionParallelGroups,
                inputOpenParallelism = cfg.compactionParallelInputOpens,
                claimsEnabled = cfg.compactionClaimsEnabled,
                claimTtlSeconds = cfg.compactionClaimTtlSeconds,
            )
        assertThat(app.parallelGroups).isEqualTo(cfg.compactionParallelGroups)
        assertThat(app.inputOpenParallelism).isEqualTo(cfg.compactionParallelInputOpens)
        assertThat(app.claimTtlSeconds).isEqualTo(cfg.compactionClaimTtlSeconds)
    }

    @Test
    fun `the claim group key is the FILE SET, blind to order and to the worker`() {
        // Two maintainers plan the same group with no coordination and
        // must compute the same key, or the claim arbitrates nothing.
        // And two DIFFERENT groups must not collide, or one claim
        // silently fences work it never touched.
        fun candidate(id: Long) =
            CompactionCandidate(
                dataFileId = id,
                path = "s3://b/f$id.parquet",
                recordCount = 1,
                fileSizeBytes = 10,
                footerSize = null,
                rowIdStart = id,
            )

        val a = CompactionGroup(listOf(candidate(1), candidate(2), candidate(3)), 7, listOf("2026-09-24"))
        val reordered = CompactionGroup(listOf(candidate(3), candidate(1), candidate(2)), 7, listOf("2026-09-24"))
        assertThat(CompactionClaimRepo.groupKey(a))
            .describedAs("the SET is the identity; input order is not")
            .isEqualTo(CompactionClaimRepo.groupKey(reordered))

        val otherFiles = CompactionGroup(listOf(candidate(1), candidate(2)), 7, listOf("2026-09-24"))
        val otherPartition = CompactionGroup(a.files, 7, listOf("2026-09-25"))
        val otherSpec = CompactionGroup(a.files, 8, listOf("2026-09-24"))
        val unpartitioned = CompactionGroup(a.files, null, null)
        assertThat(
            setOf(
                CompactionClaimRepo.groupKey(a),
                CompactionClaimRepo.groupKey(otherFiles),
                CompactionClaimRepo.groupKey(otherPartition),
                CompactionClaimRepo.groupKey(otherSpec),
                CompactionClaimRepo.groupKey(unpartitioned),
            ),
        ).describedAs("distinct groups must get distinct keys").hasSize(5)

        // A null partition value is not an empty one, and a value
        // containing the separator cannot impersonate two values.
        val nullValue = CompactionGroup(a.files, 7, listOf(null))
        val emptyValue = CompactionGroup(a.files, 7, listOf(""))
        assertThat(CompactionClaimRepo.groupKey(nullValue))
            .isNotEqualTo(CompactionClaimRepo.groupKey(emptyValue))
    }

    @Test
    fun `partition values are length-prefixed, so a shifted boundary is a different group`() {
        // The reason the hash feeds LENGTH:VALUE rather than joining
        // with a separator, and the only shape that can tell the two
        // apart. ["a", "bc"] and ["ab", "c"] concatenate identically;
        // under a naive join they are one key, and one partition's claim
        // would then fence another partition's group — a claim is only
        // an optimization, but one that fences the WRONG work is a
        // permanent stall on whatever it points at rather than a saved
        // rewrite.
        //
        // No separator character is safe here either: partition values
        // are arbitrary strings from the writer, so whatever byte a join
        // picked would be a byte a value may contain.
        fun candidate(id: Long) =
            CompactionCandidate(
                dataFileId = id,
                path = "s3://b/f$id.parquet",
                recordCount = 1,
                fileSizeBytes = 10,
                footerSize = null,
                rowIdStart = id,
            )

        val files = listOf(candidate(1), candidate(2))
        val shifted =
            listOf(
                listOf("a", "bc"),
                listOf("ab", "c"),
                listOf("abc", ""),
                listOf("", "abc"),
                // And the count itself is part of the identity, so one
                // value cannot impersonate two that concatenate to it.
                listOf("abc"),
            )
        val keys = shifted.map { CompactionClaimRepo.groupKey(CompactionGroup(files, 1, it)) }
        assertThat(keys.toSet())
            .describedAs("partition value lists that concatenate alike must still key apart: %s", shifted)
            .hasSize(shifted.size)
    }

    @Test
    fun `the claim key's encoding is pinned, because a silent change re-keys every claim`() {
        // The key is a HASH, so nothing about it is visible until two
        // maintainers disagree — and they disagree by both taking "the
        // same" claim under different keys and rewriting the same group,
        // which is the exact defect claims exist to remove and which
        // looks from the outside like claims simply not working.
        //
        // A change here is not a correctness bug (a lease expires; the
        // plan-to-commit re-verification is the backstop), so nothing
        // else in the suite reds on it. Pinning one known vector is what
        // makes it a deliberate act rather than a drift.
        val group =
            CompactionGroup(
                files =
                    listOf(3L, 1L, 2L).map { id ->
                        CompactionCandidate(
                            dataFileId = id,
                            path = "s3://b/f$id.parquet",
                            recordCount = 1,
                            fileSizeBytes = 10,
                            footerSize = null,
                            rowIdStart = id,
                        )
                    },
                specId = 7,
                partitionValues = listOf("2026-09-24", null),
            )
        assertThat(CompactionClaimRepo.groupKey(group))
            .describedAs(
                "the claim key encoding changed; every in-flight claim re-keys, so two " +
                    "maintainers across a rolling deploy stop seeing each other's claims until " +
                    "the old leases expire. Fine to do deliberately — update this vector — and " +
                    "not fine to do by accident",
            )
            .isEqualTo(PINNED_GROUP_KEY)
    }

    // ---- the pool this concurrency draws on --------------------------------

    @Test
    fun `parallelGroups above the pool's spare capacity is refused at BOOT`() {
        // Each in-flight group holds a pooled connection across its
        // commit-lock wait, and the pool is the FOREGROUND's. Take
        // enough of it and a writer fails to get a CONNECTION — a Hikari
        // timeout, surfaced as a 500 — instead of the typed, retryable
        // CommitQueueTimeout (503 + Retry-After) the admission contract
        // promises, with nothing in the 500 naming the knob that caused
        // it. Refused where an operator is already reading logs.
        // THE BUDGET IS SHARED WITH CLEANUP'S WORKERS, AND EACH DRAW IS
        // PRICED ONLY WHERE ITS LOOP RUNS. A cleanup worker holds a pooled
        // connection across its reference check (19 s cold per 1,000
        // paths), so the refusal is
        // `compactionDraw + cleanupDraw <= pool - reserve` — with each
        // draw zeroed when its own interval is 0, because
        // `BackgroundLoops` starts no loop there. So this fixture has to
        // TURN BOTH LOOPS ON to mean what it says: with the compiled
        // defaults (compaction interval 0) the compaction draw is zero and
        // every case below would assert nothing. It is not edited around
        // the guard; it states the workload it is talking about, the way a
        // values file has to.
        val pool = Config().dbPoolSize
        val reserve = Config.FOREGROUND_CONNECTION_RESERVE
        val workers = Config().cleanupWorkers
        val budget = pool - reserve - workers
        val loopsOn: (Int, Int) -> Config = { groups, size ->
            Config(
                compactionParallelGroups = groups,
                compactionIntervalMs = 3_600_000,
                cleanupWorkers = workers,
                cleanupIntervalMs = 1_800_000,
                dbPoolSize = size,
                requestThreads = size,
            )
        }
        assertThatThrownBy { loopsOn(budget + 1, pool) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_COMPACTION_PARALLEL_GROUPS")
            .hasMessageContaining("HOGLAKE_CLEANUP_WORKERS")
            .hasMessageContaining("HOGLAKE_DB_POOL_SIZE")
        // Exactly at the line is legal, and so is raising the pool with
        // the knob.
        loopsOn(budget, pool)
        loopsOn(32, 32 + reserve + workers)
        assertThatThrownBy { loopsOn(32, 32 + reserve + workers - 1) }
            .isInstanceOf(IllegalArgumentException::class.java)
        // And with compaction's own loop OFF the same group count draws
        // nothing, which is the per-loop half of the check.
        Config(compactionParallelGroups = budget + 1, compactionIntervalMs = 0)
    }

    @Test
    fun `the commit admission bound reaches compaction's own commits`() {
        // AGENT.md's rule is that every acquirer of the per-catalog
        // commit lock passes commitLockTimeoutMs, and compaction was the
        // standing exception. Under concurrency the exception stops
        // being survivable: N workers queue on that lock holding N
        // connections. App wires the shared bound through, and a dropped
        // wiring line would leave it at the unbounded default with the
        // whole suite green.
        val cfg = Config()
        assertThat(cfg.commitLockTimeoutMs).isGreaterThan(0)
        val compaction =
            CompactionConfig(
                targetBytes = cfg.compactionTargetBytes,
                maxGroupsPerRun = cfg.compactionMaxGroupsPerRun,
                commitLockTimeoutMs = cfg.commitLockTimeoutMs,
            )
        assertThat(compaction.commitLockTimeoutMs).isEqualTo(cfg.commitLockTimeoutMs)
        assertThat(CompactionConfig(targetBytes = 1024, maxGroupsPerRun = 1).commitLockTimeoutMs)
            .describedAs("the library default stays the unbounded wait this path always took")
            .isZero()
    }

    @Test
    fun `a committed group's claim is kept on a SHORT lease, and the knob says how short`() {
        val cfg = Config()
        assertThat(cfg.compactionCommittedClaimTtlSeconds)
            .isEqualTo(CompactionConfig.DEFAULT_COMMITTED_CLAIM_TTL_SECONDS)
        assertThat(cfg.compactionCommittedClaimTtlSeconds)
            .describedAs("a committed claim only has to outlive a sibling's in-flight plan")
            .isLessThanOrEqualTo(cfg.compactionClaimTtlSeconds)
        // The SIZE is the whole point, and the first draft got it wrong
        // by an order of magnitude: the quantity to cover is how old a
        // sibling's PLAN can be, which is one whole sweep, not one sweep
        // INTERVAL. At the measured 8.5 s a group, a production batch of
        // 64 is ~544 s — so a lease under that leaves most of the
        // sibling's plans arriving at an expired claim and rewriting
        // anyway, which is the behaviour claims exist to remove.
        val productionSweepSeconds = (64 * 8.5).toLong()
        assertThat(cfg.compactionCommittedClaimTtlSeconds)
            .describedAs(
                "the lease must cover a full %d-group sweep (~%ds at the measured 8.5s a group)",
                64,
                productionSweepSeconds,
            )
            .isGreaterThanOrEqualTo(productionSweepSeconds)
        assertThatThrownBy {
            CompactionConfig(targetBytes = 1024, maxGroupsPerRun = 1, committedClaimTtlSeconds = 0)
        }.isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_COMPACTION_COMMITTED_CLAIM_TTL_SECONDS")
    }
}
