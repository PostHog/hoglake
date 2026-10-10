package com.posthog.hoglake.compaction

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.CleanupService
import com.posthog.hoglake.service.RemovalStore
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.testing.TestImages
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.slf4j.LoggerFactory
import org.testcontainers.containers.MinIOContainer
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/**
 * hoglake#134: the sorted rewrite as an EXTERNAL MERGE SORT, end to end
 * against Postgres and MinIO — and what the planner still refuses.
 *
 * This class used to pin #118's sorted ROW CEILING: a sorted group was
 * packed to the rows its in-memory sort could hold, and what could not
 * fit was refused as `heap_budget_exceeded`. That ceiling is gone. A
 * sorted group packs to the byte target like any other; its heap is
 * bounded by a CHUNK (rows sorted and spilled at a time) and by the
 * merge's per-run admission, and the planner refuses only what the
 * rewrite's own spill-disk and merge-heap budgets would, in metadata.
 *
 * Registrations are metadata-only where a group must NOT execute: the
 * objects deliberately do not exist, so an attempted fetch would surface
 * as `failed_groups`. That absence is the assertion that a refused group
 * spends no IO.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CompactionHeapBudgetIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val alter = AlterService(db.jdbi)
    private val counter = AtomicInteger(0)

    private companion object {
        const val BUCKET = "hoglake-heap-budget-test"

        /**
         * The fixture table is three scalar columns, so a chunk row is
         * FOUR nodes: the three data fields plus the `_hog_row_id`
         * carrier every compaction output writes.
         */
        const val NODES_PER_ROW = 4L

        val minio: MinIOContainer by lazy { TestImages.minio().also { it.start() } }

        val store: ObjectStore by lazy {
            ObjectStore(
                endpoint = minio.s3URL,
                region = "us-east-1",
                accessKey = minio.userName,
                secretKey = minio.password,
                pathStyle = true,
            ).also { it.createBucket(BUCKET) }
        }

        val removalStore: RemovalStore by lazy {
            RemovalStore(
                endpoint = minio.s3URL,
                region = "us-east-1",
                accessKey = minio.userName,
                secretKey = minio.password,
                pathStyle = true,
            )
        }

        val ASC = listOf(SortFieldDef(1, SortDirection.ASC, NullOrder.NULLS_LAST))
        val DESC = listOf(SortFieldDef(1, SortDirection.DESC, NullOrder.NULLS_LAST))
    }

    @AfterAll
    fun stop() {
        db.close()
    }

    /** A heap budget whose CHUNK is exactly [rows] rows of the fixture shape. */
    private fun heapBytesFor(rows: Long) = CompactionConfig.SORTED_HEAP_BYTES_PER_NODE * NODES_PER_ROW * rows

    /** A private spill directory, so "empty afterwards" is about this test alone. */
    private fun spillDir(): Path = Files.createTempDirectory("hoglake-spill-it")

    private fun spillEntries(dir: Path): List<String> =
        Files.list(dir).use { entries ->
            entries.map { it.fileName.toString() }.filter { it.startsWith(SpillDirectory.PREFIX) }.toList()
        }

    // ---- the full target, through many chunks ------------------------------

    @Test
    fun `a dense sorted table the row ceiling refused compacts to the FULL target in one rewrite`() {
        // THE PRODUCTION CASE. On gigahog-prod-us's `ingest.events_raw` a
        // byte-sized group was ~2.9M rows against a sorted row ceiling of
        // 552,336, so groups were packed short or refused and the table
        // never reached its target. Here: eight 50,000-row files under a
        // heap budget whose old ceiling (= today's chunk) is 100,000 rows,
        // so the old planner closed every group at two files. Now ONE
        // group takes all eight, and the rewrite sorts it in four chunks.
        //
        // Why 100,000 and not something tinier: the same budget bounds the
        // MERGE, and each spilled run is charged its 16 MiB read block, so
        // four runs need ~66 MiB — which is 100,000 rows of this shape. A
        // smaller budget is the merge refusal tested below.
        val rowsPerFile = 50_000L
        val files = 8
        val chunk = 100_000L
        val dir = spillDir()
        val cfg =
            CompactionConfig(
                targetBytes = 256L * 1024 * 1024,
                minInputFiles = 2,
                maxGroupsPerRun = 1,
                sortedHeapBytes = heapBytesFor(chunk),
                spillDir = dir,
            )
        assertThat(cfg.spillChunkRows(fixtureLiveColumns)).isEqualTo(chunk)
        val svc = CompactionService(db.jdbi, store, cfg)
        // Every file's ids DESCENDING and interleaved with every other
        // file's, so the output order is the sort's work and no input or
        // chunk is accidentally already in order.
        val contents = (0 until files).map { f -> (0 until rowsPerFile).map { it * files + f }.reversed() }
        val cat = realSortedTableOf("full-target", contents)

        val plan = svc.planTable(cat, "ns", "t", cfg)
        val group = plan.groups.single()
        assertThat(group.files).describedAs("one group takes the whole table").hasSize(files)
        assertThat(group.survivingRecords)
            .describedAs("four times what the old row ceiling admitted into a group")
            .isEqualTo(files * rowsPerFile)
            .isGreaterThan(cfg.spillChunkRows(fixtureLiveColumns) * 3)
        assertThat(plan.spillRefusedGroups + plan.mergeRefusedGroups).isZero()

        val result = svc.runOnce(cat, cfg, MaintenanceTrigger.LOOP)
        assertThat(result.groupsCompacted).describedAs("%s", result).isEqualTo(1)
        assertThat(result.failedGroups).isZero()
        assertThat(result.runsSpilled).describedAs("400k rows at 100k a chunk").isEqualTo(4)
        assertThat(result.runsTrusted).describedAs("client files are never trusted").isZero()
        assertThat(result.runsDemoted).isZero()
        assertThat(result.spillBytes).isGreaterThan(0)
        assertThat(result.spillBytes).isLessThanOrEqualTo(cfg.spillBytes)
        assertThat(result.spillBudgetExceeded).isZero()
        assertThat(result.mergeBudgetExceeded).isZero()
        // HISTORICAL: nothing produces heap_budget_exceeded since #134; it
        // stays on the wire reading 0, which is all this states.
        assertThat(result.heapBudgetExceeded).isZero()
        assertThat(result.spillCleanupFailures).isZero()
        assertThat(spillEntries(dir)).describedAs("the spill directory is removed afterwards").isEmpty()

        // The ledger row carries the counters, under their wire names.
        val ledger = com.fasterxml.jackson.databind.ObjectMapper().readTree(lastLedgerResult(cat))
        assertThat(ledger["runs_spilled"].asLong()).isEqualTo(4)
        assertThat(ledger["spill_bytes"].asLong()).isEqualTo(result.spillBytes)

        // And the output is the whole table, sorted.
        val ids = readIds(livePaths(cat).single())
        assertThat(ids).hasSize((files * rowsPerFile).toInt())
        assertThat(ids).isSorted()
        assertThat(ids.toSet()).isEqualTo((0L until files * rowsPerFile).toSet())
    }

    @Test
    fun `the rewrite's own spill hard stop is a counted refusal, not a failure, and leaves nothing behind`() {
        // The planner refuses on REGISTERED bytes; the rewrite meters the
        // bytes it actually writes. Snappy spill of snappy input plus the
        // row-id column comes out larger than the registered inputs, so a
        // spill budget of exactly the registered bytes passes planning and
        // stops mid-run — the output upload discarded, the spill
        // directory removed, and the outcome counted where the planner's
        // refusal is (typed arm before the catch-all; not failed_groups).
        val dir = spillDir()
        val contents = (0 until 4).map { f -> (0 until 50_000L).map { it * 4 + f }.reversed() }
        val cat = realSortedTableOf("spill-hard-stop", contents)
        val registered = db.jdbi.withHandleUnchecked { h -> registeredBytes(h, cat) }
        val cfg =
            CompactionConfig(
                targetBytes = 256L * 1024 * 1024,
                minInputFiles = 2,
                maxGroupsPerRun = 1,
                sortedHeapBytes = heapBytesFor(100_000),
                spillBytes = registered,
                spillDir = dir,
            )
        val svc = CompactionService(db.jdbi, store, cfg)
        assertThat(svc.planTable(cat, "ns", "t", cfg).spillRefusedGroups)
            .describedAs("the registered bytes fit, so the planner admits it")
            .isZero()
        val headBefore = headSnapshot(cat)
        val result = svc.runOnce(cat, cfg, MaintenanceTrigger.LOOP)
        assertThat(result.spillBudgetExceeded).describedAs("%s", result).isEqualTo(1)
        assertThat(result.failedGroups).isZero()
        assertThat(result.groupsCompacted).isZero()
        // The work the stopped rewrite DID reaches the ledger: it is the
        // group that spilled the most, and it has no result to report from.
        assertThat(result.spillBytes).isPositive().isLessThanOrEqualTo(registered)
        assertThat(result.runsSpilled).isPositive()
        val ledger = com.fasterxml.jackson.databind.ObjectMapper().readTree(lastLedgerResult(cat))
        assertThat(ledger["spill_bytes"].asLong()).isEqualTo(result.spillBytes)
        assertThat(ledger["runs_spilled"].asLong()).isEqualTo(result.runsSpilled)
        assertThat(headSnapshot(cat)).isEqualTo(headBefore)
        assertThat(spillEntries(dir)).describedAs("no spill directory survives the stop").isEmpty()
        val staged = removalRows(cat).single()
        assertThat(objectExists(staged.path)).describedAs("the partial output was discarded").isFalse()
    }

    @Test
    fun `the rewrite's own merge hard stop is a counted refusal, not a failure`() {
        // Registered survivors predict ONE chunk, so the planner admits
        // the group; the files really hold 50,000 rows each, so the
        // rewrite spills dozens of runs and stops at the one whose exact
        // cost breaks the merge budget. (Registration never opens the
        // parquet, which is exactly why the rewrite checks for itself.)
        val dir = spillDir()
        val cat = "heap-merge-hard-stop-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", fixtureColumns)
        alter.alterTable(cat, "ns", "t", listOf(AlterOp.SetSortOrder(ASC)))
        val regs =
            (0 until 8).map { f ->
                val bytes = parquetBytes((0 until 50_000L).map { it * 8 + f })
                val path = "s3://$BUCKET/$cat/data/ns/t/lie$f.parquet"
                store.put(path, bytes)
                FileRegistration(path, 1, bytes.size.toLong())
            }
        commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", regs))))
        val cfg =
            CompactionConfig(
                targetBytes = 256L * 1024 * 1024,
                minInputFiles = 2,
                maxGroupsPerRun = 1,
                sortedHeapBytes = heapBytesFor(10_000),
                spillDir = dir,
            )
        val svc = CompactionService(db.jdbi, store, cfg)
        assertThat(svc.planTable(cat, "ns", "t", cfg).mergeRefusedGroups).isZero()
        val result = svc.runOnce(cat, cfg, MaintenanceTrigger.LOOP)
        assertThat(result.mergeBudgetExceeded).describedAs("%s", result).isEqualTo(1)
        assertThat(result.failedGroups).isZero()
        assertThat(result.groupsCompacted).isZero()
        assertThat(result.runsSpilled).describedAs("the runs written before the stop").isPositive()
        assertThat(result.spillBytes).isPositive()
        assertThat(spillEntries(dir)).isEmpty()
    }

    // ---- trust: only the spec an output was written under ------------------

    @Test
    fun `a compaction output is a trusted run of the spec it was written under, and of no other`() {
        // The trust predicate is `explicit_row_ids AND begin_snapshot >=
        // spec.begin_snapshot`. Both halves get a positive control and a
        // negative one here: the first output is trusted by the next
        // rewrite under the SAME spec, and not after `set_sort_order`.
        val cfg = smallFileConfig().copy(maxInputFiles = 64, maxFanIn = 64)
        val svc = CompactionService(db.jdbi, store, cfg)
        val cat = realSortedTableOf("trust", (0 until 3).map { f -> listOf(30L - f, 20L - f, 10L - f) })

        // 1. Client files only: nothing to trust.
        val first = svc.runOnce(cat, cfg, MaintenanceTrigger.LOOP)
        assertThat(first.groupsCompacted).isEqualTo(1)
        assertThat(first.runsTrusted).isZero()
        assertThat(readIds(livePaths(cat).single())).isSorted()

        // 2. Same spec, more client files: the output above is TRUSTED,
        // read in place, and the new files are the one spilled run.
        appendReal(cat, (0 until 3).map { f -> listOf(35L + f, 5L + f) })
        val second = svc.runOnce(cat, cfg, MaintenanceTrigger.LOOP)
        assertThat(second.groupsCompacted).isEqualTo(1)
        assertThat(second.runsTrusted).describedAs("the previous output, under the same spec").isEqualTo(1)
        assertThat(second.runsSpilled).isEqualTo(1)
        val ascending = readIds(livePaths(cat).single())
        assertThat(ascending).hasSize(15).isSorted()

        // 3. The spec moves (ASC -> DESC). The output above is sorted by
        // the OLD spec, and its begin_snapshot predates the new one, so it
        // must not be trusted: everything goes down the spill path (here
        // one chunk, sorted in memory), and the result is sorted by DESC.
        alter.alterTable(cat, "ns", "t", listOf(AlterOp.SetSortOrder(DESC)))
        appendReal(cat, (0 until 3).map { f -> listOf(1L + f * 100, 2L + f * 100) })
        val third = svc.runOnce(cat, cfg, MaintenanceTrigger.LOOP)
        assertThat(third.groupsCompacted).isEqualTo(1)
        assertThat(third.runsTrusted).describedAs("an output of the OLD spec is not a run of the new one").isZero()
        val descending = readIds(livePaths(cat).single())
        assertThat(descending).hasSize(21)
        assertThat(descending).isEqualTo(descending.sortedDescending())
    }

    // ---- the byte-level append (hoglake#134 package D1) -------------------------

    @Test
    fun `a prior output is appended byte for byte, and the ledger and the metric say so`() {
        val registry =
            io.micrometer.prometheusmetrics.PrometheusMeterRegistry(
                io.micrometer.prometheusmetrics.PrometheusConfig.DEFAULT,
            )
        com.posthog.hoglake.observability.Metrics.bind(registry)
        val cfg = smallFileConfig().copy(maxInputFiles = 64, maxFanIn = 64, spillDir = spillDir())
        val svc = CompactionService(db.jdbi, store, cfg)
        // Kilobyte fixtures: the production floor is 32 MiB.
        svc.appendFloorBytes = 1
        val cat = realSortedTableOf("append", (0 until 3).map { f -> listOf(30L - f, 20L - f, 10L - f) })
        val first = svc.runOnce(cat, cfg, MaintenanceTrigger.LOOP)
        assertThat(first.groupsCompacted).isEqualTo(1)
        assertThat(first.rowGroupsAppended).describedAs("client files are never appended").isZero()
        val prior = livePaths(cat).single()

        // New client rows ABOVE the prior output's keys: its range is
        // disjoint from the new rows' chunk, so it is appended whole.
        appendReal(cat, (0 until 3).map { f -> listOf(105L + f, 100L + f) })
        val second = svc.runOnce(cat, cfg, MaintenanceTrigger.LOOP)
        assertThat(second.groupsCompacted).describedAs("%s", second).isEqualTo(1)
        assertThat(second.runsTrusted).isEqualTo(1)
        assertThat(second.rowGroupsAppended).isEqualTo(1)
        assertThat(second.bytesAppended).isPositive()
        val ids = readIds(livePaths(cat).single())
        assertThat(ids).hasSize(15).isSorted()
        assertThat(livePaths(cat).single()).isNotEqualTo(prior)

        val ledger = com.fasterxml.jackson.databind.ObjectMapper().readTree(lastLedgerResult(cat))
        assertThat(ledger["row_groups_appended"].asLong()).isEqualTo(1)
        assertThat(ledger["bytes_appended"].asLong()).isEqualTo(second.bytesAppended)
        assertThat(registry.scrape())
            .contains("hoglake_compaction_appended_bytes_total{catalog=\"$cat\"} ${second.bytesAppended}.0")
    }

    // ---- the sortedness pre-pass ----------------------------------------------

    /**
     * Millpond-shaped files: each one sorted by the table's key (a
     * writer that sorts its flushes), the files interleaved with each
     * other so the OUTPUT still needs the merge.
     */
    private fun presorted(
        files: Int,
        rowsEach: Int,
    ): List<List<Long>> = (0 until files).map { f -> (0 until rowsEach).map { it.toLong() * files + f } }

    @Test
    fun `pre-sorted client files are verified and merged in place - no spill at all`() {
        val registry =
            io.micrometer.prometheusmetrics.PrometheusMeterRegistry(
                io.micrometer.prometheusmetrics.PrometheusConfig.DEFAULT,
            )
        com.posthog.hoglake.observability.Metrics.bind(registry)
        val dir = spillDir()
        // A floor below these fixtures' size, so every file is checked.
        val cfg = smallFileConfig().copy(maxInputFiles = 64, maxFanIn = 64, spillDir = dir, verifyMinBytes = 1)
        val svc = CompactionService(db.jdbi, store, cfg)
        var built: List<ParquetRewriter.Input> = emptyList()
        svc.beforeRewrite = { built = it }
        val files = 5
        val cat = realSortedTableOf("presorted", presorted(files, 40))

        val result = svc.runOnce(cat, cfg, MaintenanceTrigger.LOOP)
        assertThat(result.groupsCompacted).describedAs("%s", result).isEqualTo(1)
        assertThat(result.filesVerified).isEqualTo(files.toLong())
        assertThat(result.filesUnsorted).isZero()
        assertThat(result.runsTrusted).describedAs("every verified file is a run").isEqualTo(files.toLong())
        assertThat(result.runsSpilled).isZero()
        assertThat(result.spillBytes).isZero()
        assertThat(spillEntries(dir)).isEmpty()
        val ids = readIds(livePaths(cat).single())
        assertThat(ids).hasSize(files * 40).isSorted()

        // The pre-pass reads through its own handle, at the small
        // readahead: correct at the merge's 8 MiB too, just ~32x the bytes.
        assertThat(built).hasSize(files)
        for (input in built) {
            val keys = input.keySource as S3InputFile
            assertThat(keys.readaheadBytes).isEqualTo(S3InputFile.KEY_COLUMN_READAHEAD_BYTES)
            assertThat((input.source as S3InputFile).readaheadBytes).isEqualTo(S3InputFile.DEFAULT_READAHEAD_BYTES)
        }

        val ledger = com.fasterxml.jackson.databind.ObjectMapper().readTree(lastLedgerResult(cat))
        assertThat(ledger["files_verified"].asLong()).isEqualTo(files.toLong())
        assertThat(ledger.has("files_unsorted")).describedAs("zero is omitted from the stored row").isFalse()
        val scrape = registry.scrape()
        assertThat(scrape).contains("hoglake_compaction_sort_check_total{catalog=\"$cat\",outcome=\"sorted\"} $files.0")
        assertThat(scrape).containsPattern("hoglake_compaction_sort_check_bytes_total\\{catalog=\"$cat\"\\} [1-9]")
    }

    @Test
    fun `an unsorted file among pre-sorted ones is the only spill`() {
        val dir = spillDir()
        val cfg = smallFileConfig().copy(maxInputFiles = 64, maxFanIn = 64, spillDir = dir, verifyMinBytes = 1)
        val svc = CompactionService(db.jdbi, store, cfg)
        // A Trino-shaped file: the same key range, in no particular order.
        val trino = (0L until 40L).map { (it * 7) % 40 + 1_000 }
        val cat = realSortedTableOf("one-unsorted", presorted(4, 40) + listOf(trino))

        val result = svc.runOnce(cat, cfg, MaintenanceTrigger.LOOP)
        assertThat(result.groupsCompacted).describedAs("%s", result).isEqualTo(1)
        assertThat(result.filesVerified).isEqualTo(4)
        assertThat(result.filesUnsorted).isEqualTo(1)
        assertThat(result.runsTrusted).isEqualTo(4)
        assertThat(result.runsSpilled).describedAs("the unsorted file's one chunk").isEqualTo(1)
        assertThat(result.spillBytes).isPositive()
        assertThat(readIds(livePaths(cat).single())).hasSize(200).isSorted()
        val ledger = com.fasterxml.jackson.databind.ObjectMapper().readTree(lastLedgerResult(cat))
        assertThat(ledger["files_unsorted"].asLong()).isEqualTo(1)
    }

    @Test
    fun `small files are not checked - they spill into a few runs, whatever their order`() {
        // Fifty sorted files of 3,000 rows (~30 KB each) under the default
        // 16 MiB floor: none is checked, and the 150,000 rows are three
        // spilled runs, not fifty one-file runs.
        val dir = spillDir()
        val chunk = 70_000L
        val cfg =
            smallFileConfig().copy(
                targetBytes = 256L * 1024 * 1024,
                maxInputFiles = 64,
                maxFanIn = 64,
                spillDir = dir,
                sortedHeapBytes = heapBytesFor(chunk),
            )
        assertThat(cfg.verifyMinBytes).isEqualTo(CompactionConfig.DEFAULT_VERIFY_MIN_BYTES)
        val svc = CompactionService(db.jdbi, store, cfg)
        val files = 50
        val cat = realSortedTableOf("tiny", presorted(files, 3_000))

        val result = svc.runOnce(cat, cfg, MaintenanceTrigger.LOOP)
        assertThat(result.groupsCompacted).describedAs("%s", result).isEqualTo(1)
        assertThat(result.filesUnchecked).isEqualTo(files.toLong())
        assertThat(result.filesVerified + result.filesUnsorted).isZero()
        assertThat(result.runsSpilled).describedAs("150k rows at 70k a chunk").isEqualTo(3)
        assertThat(result.spillBytes).isPositive()
        assertThat(readIds(livePaths(cat).single())).hasSize(files * 3_000).isSorted()
        val ledger = com.fasterxml.jackson.databind.ObjectMapper().readTree(lastLedgerResult(cat))
        assertThat(ledger["files_unchecked"].asLong()).isEqualTo(files.toLong())
    }

    // ---- what the planner still refuses, in metadata -----------------------

    @Test
    fun `a group whose spill exceeds the spill budget is refused in metadata and spends no IO`() {
        // Four 900-row files under a 1,000-row chunk: four spilled runs,
        // 4,096 registered bytes to spill against a 1,000-byte budget. The
        // objects do not exist, so any fetch would be a failed group.
        val cfg =
            CompactionConfig(
                targetBytes = 65536,
                minInputFiles = 2,
                maxGroupsPerRun = 1,
                sortedHeapBytes = heapBytesFor(1_000),
                spillBytes = 1_000,
                spillDir = spillDir(),
            )
        val svc = CompactionService(db.jdbi, store, cfg)
        val cat = sortedTable("spill-refused", files = 4, bytesEach = 1024, recordsEach = 900)

        val plan = svc.planTable(cat, "ns", "t", cfg)
        assertThat(plan.groups).isEmpty()
        assertThat(plan.spillRefusedGroups).isEqualTo(1)
        assertThat(plan.mergeRefusedGroups).describedAs("the spill check answers first").isZero()

        repeat(2) {
            val result = svc.runOnce(cat, cfg, MaintenanceTrigger.LOOP)
            assertThat(result.spillBudgetExceeded).isEqualTo(1)
            assertThat(result.failedGroups).describedAs("no fetch was attempted").isZero()
            assertThat(result.groupsCompacted).isZero()
        }
        assertThat(removalRows(cat)).describedAs("no staging ticket: nothing was attempted").isEmpty()
        assertThat(liveFileCount(cat)).isEqualTo(4)

        // The comparison is `>`, not `>=`: a budget of exactly the
        // registered bytes admits the group.
        val exact = cfg.copy(spillBytes = 4 * 1024)
        assertThat(svc.planTable(cat, "ns", "t", exact).spillRefusedGroups).isZero()
    }

    @Test
    fun `a group whose spilled runs alone exceed the merge budget is refused in metadata and spends no IO`() {
        // Same four files under the same 1,000-row chunk, default spill
        // budget: four spilled runs at ~16.5 MiB each against a merge
        // budget of 768 KB. Reachable only by configuration, which is what
        // the log line says.
        val cfg =
            CompactionConfig(
                targetBytes = 65536,
                minInputFiles = 2,
                maxGroupsPerRun = 1,
                sortedHeapBytes = heapBytesFor(1_000),
                spillDir = spillDir(),
            )
        val svc = CompactionService(db.jdbi, store, cfg)
        val cat = sortedTable("merge-refused", files = 4, bytesEach = 1024, recordsEach = 900)

        val plan = svc.planTable(cat, "ns", "t", cfg)
        assertThat(plan.groups).isEmpty()
        assertThat(plan.mergeRefusedGroups).isEqualTo(1)
        assertThat(plan.spillRefusedGroups).isZero()

        val result = svc.runOnce(cat, cfg, MaintenanceTrigger.LOOP)
        assertThat(result.mergeBudgetExceeded).isEqualTo(1)
        assertThat(result.failedGroups).isZero()
        assertThat(removalRows(cat)).isEmpty()

        // The boundary, from the rewriter's own arithmetic. Two spilled
        // runs cost exactly 2 x the predicted run, and a heap budget of
        // exactly that has a chunk of half of 80,000 rows give or take, so
        // the merge is two runs: admitted at the budget (the comparison is
        // `>`), refused one byte under it.
        val edge = sortedTable("merge-edge", files = 4, bytesEach = 1024, recordsEach = 20_000)
        val leaves = ParquetRewriter.outputLeafCount(fixtureLiveColumns)
        val twoRuns = 2 * ExternalMergeSort.predictedSpilledRunBytes(leaves, cfg.sortSpill(fixtureLiveColumns))
        for ((budget, refused) in listOf(twoRuns to 0L, twoRuns - 1 to 1L)) {
            val at = cfg.copy(sortedHeapBytes = budget)
            val runs = ExternalMergeSort.spillRuns(80_000, at.sortSpill(fixtureLiveColumns))
            assertThat(runs).describedAs("the fixture must be a two-run merge at %d B", budget).isEqualTo(2)
            assertThat(svc.planTable(edge, "ns", "t", at).mergeRefusedGroups)
                .describedAs("heap budget %d B against a two-run merge of %d B", budget, twoRuns)
                .isEqualTo(refused)
        }
    }

    @Test
    fun `refusals are not charged to the run budget - a refused table does not starve the next`() {
        // Two tables in one catalog, planned in name order: `a` is
        // refused (merge budget), `b` compacts. With maxGroupsPerRun = 1,
        // a refusal that charged the budget would leave `b` untouched
        // forever.
        val dir = spillDir()
        val cfg = smallFileConfig().copy(sortedHeapBytes = heapBytesFor(1_000), spillDir = dir)
        val svc = CompactionService(db.jdbi, store, cfg)
        val cat = "heap-uncharged-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        for (t in listOf("a", "b")) {
            catalogs.createTable(cat, "ns", t, fixtureColumns)
            alter.alterTable(cat, "ns", t, listOf(AlterOp.SetSortOrder(ASC)))
        }
        val refusedRegs =
            (0 until 3).map { i -> FileRegistration("s3://$BUCKET/$cat/data/ns/a/m$i.parquet", 900, 1024) }
        commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "a", refusedRegs))))
        val goodRegs =
            (0 until 3).map { i ->
                val bytes = parquetBytes((0 until 4).map { it + i * 4L })
                val path = "s3://$BUCKET/$cat/data/ns/b/r$i.parquet"
                store.put(path, bytes)
                FileRegistration(path, 4, bytes.size.toLong())
            }
        commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "b", goodRegs))))

        val result = svc.runOnce(cat, cfg, MaintenanceTrigger.LOOP)
        assertThat(result.mergeBudgetExceeded).isEqualTo(1)
        assertThat(result.groupsCompacted).describedAs("the refusal left the run's one slot for `b`").isEqualTo(1)
        assertThat(result.failedGroups).isZero()
    }

    @Test
    fun `a permanent refusal is logged once, even when other catalogs plan in between`() {
        // The refusal warning is rate-limited to fire only when a table's
        // refusal picture CHANGES. That state is keyed by (catalog, table)
        // — and the catalog half is load-bearing, because table_id is
        // scoped per catalog: every catalog's first table is table 1.
        // Keyed by table alone, each OTHER catalog's sweep, finding
        // nothing refused, cleared the refusing catalog's entry, and the
        // warning fired on every sweep. Production: 302 identical lines
        // in 17 hours for one unchanged refusal (of the row ceiling this
        // replaced; the mechanism is the same one, renamed).
        val cfg =
            CompactionConfig(
                targetBytes = 65536,
                minInputFiles = 2,
                maxGroupsPerRun = 1,
                sortedHeapBytes = heapBytesFor(1_000),
                spillDir = spillDir(),
            )
        val svc = CompactionService(db.jdbi, store, cfg)
        val refusing = sortedTable("refused-once", files = 4, bytesEach = 1024, recordsEach = 900)
        val innocent = sortedTable("innocent", files = 4, bytesEach = 1024, recordsEach = 100, sort = false)

        val events = CopyOnWriteArrayList<ILoggingEvent>()
        val appender =
            object : AppenderBase<ILoggingEvent>() {
                override fun append(event: ILoggingEvent) {
                    events += event
                }
            }
        val ctx = LoggerFactory.getILoggerFactory() as LoggerContext
        appender.context = ctx
        appender.start()
        val logger = LoggerFactory.getLogger(CompactionService::class.java) as Logger
        logger.addAppender(appender)
        try {
            repeat(3) {
                assertThat(svc.planTable(refusing, "ns", "t", cfg).mergeRefusedGroups).isEqualTo(1)
                assertThat(svc.planTable(innocent, "ns", "t", cfg).mergeRefusedGroups).isZero()
            }
        } finally {
            logger.detachAppender(appender)
            appender.stop()
        }
        val warnings = events.filter { it.formattedMessage.contains("compaction refused") }
        assertThat(warnings)
            .describedAs("one unchanged refusal, planned three times with another catalog in between, logs ONCE")
            .hasSize(1)

        // And the picture is FORGOTTEN when the table plans clean: raise
        // the budget (one chunk, no merge) and the refusal clears; lower
        // it again and the same refusal is news again, so it WARNs a
        // second time. Without the clear, a refusal that came back after
        // an operator's fix and revert would never be logged.
        events.clear()
        logger.addAppender(appender)
        appender.start()
        try {
            val raised = cfg.copy(sortedHeapBytes = CompactionConfig.DEFAULT_SORTED_HEAP_BYTES)
            assertThat(svc.planTable(refusing, "ns", "t", raised).mergeRefusedGroups).isZero()
            assertThat(svc.planTable(refusing, "ns", "t", cfg).mergeRefusedGroups).isEqualTo(1)
        } finally {
            logger.detachAppender(appender)
            appender.stop()
        }
        assertThat(events.filter { it.formattedMessage.contains("compaction refused") })
            .describedAs("refuse, clear, refuse again: a SECOND warning")
            .hasSize(1)
        assertThat(warnings.single().formattedMessage)
            .describedAs("and it names the knobs that clear it")
            .contains("HOGLAKE_COMPACTION_SORTED_HEAP_BYTES")
            .contains("HOGLAKE_COMPACTION_TARGET_BYTES")
    }

    @Test
    fun `an unsorted table never spills and is never refused for its density`() {
        val dir = spillDir()
        val cfg =
            CompactionConfig(
                targetBytes = 1024 * 1024,
                maxGroupsPerRun = 1,
                sortedHeapBytes = heapBytesFor(1),
                spillBytes = 1,
                spillDir = dir,
            )
        val svc = CompactionService(db.jdbi, store, cfg)
        val dense = sortedTable("unsorted-dense", files = 8, bytesEach = 128 * 1024, recordsEach = 4096, sort = false)
        val plan = svc.planTable(dense, "ns", "t", cfg)
        assertThat(plan.groups.single().files).hasSize(8)
        assertThat(plan.spillRefusedGroups + plan.mergeRefusedGroups).isZero()
    }

    // ---- concurrency and the spill directory -------------------------------

    @Test
    fun `two sorted groups in flight spill to separate directories and both are removed`() {
        // Two groups of four files, two workers. The store holds a read
        // on each worker, once its own group has started spilling, until
        // the OTHER group's directory exists too — so both are on disk at
        // once and must be distinct, and both must be gone afterwards.
        val dir = spillDir()
        val rowsPerFile = 50_000L
        val cfg =
            CompactionConfig(
                targetBytes = 256L * 1024 * 1024,
                minInputFiles = 4,
                maxInputFiles = 4,
                maxFanIn = 4,
                maxGroupsPerRun = 2,
                parallelGroups = 2,
                inputOpenParallelism = 1,
                // Per group (divided by 2): a 100,000-row chunk, so each
                // 200,000-row group is two spilled runs, which the same
                // per-group budget can merge (two 16.5 MiB read blocks).
                sortedHeapBytes = 2 * heapBytesFor(100_000),
                spillDir = dir,
            )
        assertThat(cfg.spillChunkRows(fixtureLiveColumns)).isEqualTo(2 * rowsPerFile)
        val cat =
            realSortedTableOf("two-dirs", (0 until 8).map { f -> (0 until rowsPerFile).map { it * 8 + f }.reversed() })
        val seen = java.util.concurrent.ConcurrentHashMap.newKeySet<String>()
        val maxConcurrent = AtomicInteger()
        val waitingStore =
            object : ObjectStore(minio.s3URL, "us-east-1", minio.userName, minio.password, true) {
                override fun getRange(
                    pathUri: String,
                    startInclusive: Long,
                    length: Int,
                ): ByteArray {
                    val deadline = System.nanoTime() + 20_000_000_000L
                    var now = spillEntries(dir)
                    while (now.size == 1 && System.nanoTime() < deadline) {
                        Thread.sleep(10)
                        now = spillEntries(dir)
                    }
                    seen += now
                    maxConcurrent.accumulateAndGet(now.size) { a, b -> maxOf(a, b) }
                    return super.getRange(pathUri, startInclusive, length)
                }
            }
        val result = CompactionService(db.jdbi, waitingStore, cfg).runOnce(cat, cfg, MaintenanceTrigger.LOOP)
        waitingStore.close()
        assertThat(result.groupsCompacted).describedAs("%s", result).isEqualTo(2)
        assertThat(result.runsSpilled).describedAs("two per group").isEqualTo(4)
        assertThat(maxConcurrent.get()).describedAs("both groups' spill directories existed at once").isEqualTo(2)
        assertThat(seen).hasSize(2)
        assertThat(spillEntries(dir)).describedAs("and both are gone").isEmpty()
        for (path in livePaths(cat)) assertThat(readIds(path)).hasSize(4 * rowsPerFile.toInt()).isSorted()
    }

    @Test
    fun `the startup sweep removes a planted spill directory before any loop runs, and only where compaction runs`() {
        val dir = spillDir()
        val planted = Files.createDirectories(dir.resolve("${SpillDirectory.PREFIX}from-a-killed-container"))
        Files.write(planted.resolve("spill-1.parquet"), ByteArray(4096))
        val bystander = Files.createDirectories(dir.resolve("someone-elses"))

        fun boot(compactionIntervalMs: Long) =
            App.build(
                Config(
                    compactionIntervalMs = compactionIntervalMs,
                    compactionSpillDir = dir.toString(),
                    hydratorIntervalMs = 0,
                    expiryIntervalMs = 0,
                    cleanupIntervalMs = 0,
                    metricsIntervalMs = 0,
                    maintenanceSummaryIntervalMs = 0,
                ),
                db.jdbi,
            )

        val off = boot(0)
        off.startBackground().close()
        off.close()
        assertThat(planted).describedAs("a pod that does not run the loop leaves the directory alone").exists()

        val on = boot(3_600_000)
        on.startBackground().close()
        on.close()
        assertThat(planted).doesNotExist()
        assertThat(bystander).describedAs("only hoglake-compaction-spill-* directories are compaction's").exists()
    }

    // ---- OOM containment and the staged-output ledger ----------------------

    @Test
    fun `an OOM after the staging ticket leaves a reclaimable orphan and no catalog row`() {
        // Compaction pre-registers its output path as an undrained
        // hog_file_removal row BEFORE uploading, so an aborted group
        // hands the object to the normal cleanup drain. That claim is
        // made for clean aborts; an OOM is not one, and the issue asked
        // for a test rather than the assumption.
        //
        // The OOM is injected at the upload, which is the first step
        // AFTER the ticket exists — the worst placement for the ledger,
        // since a ticket now exists for an object that may or may not.
        val cfg = smallFileConfig()
        val fx = realTable("oom-put")
        val svc = CompactionService(db.jdbi, OomOnPut(), cfg)
        val headBefore = headSnapshot(fx)

        val result = svc.runOnce(fx, cfg, MaintenanceTrigger.LOOP)

        // Contained: counted as a FAILED group (the external merge sort
        // makes an OOM an under-count, not a sizing outcome, and
        // `heap_budget_exceeded` is historical), not an escaped Error that
        // takes the whole sweep's accounting with it.
        assertThat(result.failedGroups).isEqualTo(1)
        assertThat(result.groupsCompacted).isZero()

        // The ledger: exactly one claim, still undrained, still ours.
        val staged = removalRows(fx).single()
        assertThat(staged.reason).isEqualTo("compaction_staging")
        assertThat(staged.drainedAt).describedAs("undrained = the cleanup drain still owns it").isNull()

        // No catalog row dangles: the output path was never registered,
        // no snapshot was cut, and every input is still live.
        assertThat(filePaths(fx)).doesNotContain(staged.path)
        assertThat(liveFileCount(fx)).isEqualTo(3)
        assertThat(headSnapshot(fx)).describedAs("no snapshot was cut").isEqualTo(headBefore)

        // And it is genuinely RECLAIMABLE, not merely present: the drain
        // settles it. stagingGraceSeconds = 0: the ticket is seconds old,
        // and the production grace (1 h) exists to protect one whose group
        // may still be uploading — this group's upload already failed.
        val drained =
            CleanupService(db.jdbi, removalStore, stagingGraceSeconds = 0).runOnce(fx, batchSize = 100)
        assertThat(drained.removed + drained.missing).isEqualTo(1)
        assertThat(drained.stillReferenced).describedAs("never an invariant violation").isZero()
        assertThat(removalRows(fx).single().drainedAt).isNotNull()
    }

    @Test
    fun `an OOM reading an input leaves a reclaimable orphan and no catalog row`() {
        // The group dies while reading its inputs, which happens after
        // the claim (inputs are read in place during the rewrite). What
        // matters: the failure is COUNTED rather than unwinding the run
        // ledger, no snapshot is cut, no catalog row appears, and the
        // claimed path is left for the cleanup drain.
        val cfg = smallFileConfig()
        val fx = realTable("oom-get")
        val svc = CompactionService(db.jdbi, OomOnGet(), cfg)
        val headBefore = headSnapshot(fx)

        val result = svc.runOnce(fx, cfg, MaintenanceTrigger.LOOP)

        assertThat(result.failedGroups).isEqualTo(1)
        assertThat(result.groupsCompacted).isZero()
        assertThat(removalRows(fx)).describedAs("the claimed path is left to reclaim").hasSize(1)
        assertThat(liveFileCount(fx)).isEqualTo(3)
        assertThat(headSnapshot(fx)).describedAs("no snapshot was cut").isEqualTo(headBefore)
    }

    @Test
    fun `a read fault on an UNSORTED table leaves no object behind`() {
        // On the streaming path the writer — and therefore the multipart
        // upload — is open before the first input is read, so a read-side
        // failure has a live upload to leave behind. It must be discarded,
        // not completed.
        val cfg = smallFileConfig()
        val fx = unsortedRealTable("oom-unsorted")
        val svc = CompactionService(db.jdbi, OomOnGet(), cfg)
        val headBefore = headSnapshot(fx)

        val result = svc.runOnce(fx, cfg, MaintenanceTrigger.LOOP)

        assertThat(result.groupsCompacted).isZero()
        assertThat(headSnapshot(fx)).describedAs("no snapshot was cut").isEqualTo(headBefore)
        assertThat(liveFileCount(fx)).isEqualTo(3)

        val staged = removalRows(fx).filter { it.reason == "compaction_staging" }
        assertThat(staged).describedAs("the path is claimed").hasSize(1)
        assertThat(objectExists(staged.single().path))
            .describedAs("nothing may be published at %s", staged.single().path)
            .isFalse()
    }

    /**
     * An ObjectStore that exhausts the heap while UPLOADING — after the
     * claim ticket.
     *
     * Cuts at `uploadPart`, not `put`: compaction streams its output
     * through a multipart upload and never calls `put`. Overriding the
     * old seam here would inject a fault nothing reaches, and the test
     * would pass by never failing at all.
     */
    private class OomOnPut : ObjectStore(minio.s3URL, "us-east-1", minio.userName, minio.password, true) {
        override fun uploadPart(
            pathUri: String,
            uploadId: String,
            partNumber: Int,
            bytes: ByteArray,
            length: Int,
        ): String = throw OutOfMemoryError("Java heap space")
    }

    /**
     * An ObjectStore that exhausts the heap while READING an input.
     *
     * Cuts at `getRange` for the same reason: inputs are read in place
     * now, so `get` is only reached for a deletion vector. Note this no
     * longer lands BEFORE the claim ticket — see the ordering note in
     * CompactionService.compactGroup. It is kept as the read-side fault,
     * with the post-ticket expectations that implies.
     */
    private class OomOnGet : ObjectStore(minio.s3URL, "us-east-1", minio.userName, minio.password, true) {
        override fun getRange(
            pathUri: String,
            startInclusive: Long,
            length: Int,
        ): ByteArray = throw OutOfMemoryError("Java heap space")
    }

    // ---- fixtures ----------------------------------------------------------

    /**
     * A table whose files are METADATA ONLY: the planner reads
     * record_count and file_size_bytes, and registration never opens the
     * parquet, so the objects need not exist — and where a group must not
     * execute, their absence is what proves it did not.
     */

    private fun sortedTable(
        label: String,
        files: Int,
        bytesEach: Int,
        recordsEach: Long,
        sort: Boolean = true,
    ): String {
        val cat = "heap-$label-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", fixtureColumns)
        if (sort) {
            alter.alterTable(
                cat,
                "ns",
                "t",
                listOf(AlterOp.SetSortOrder(listOf(SortFieldDef(1, SortDirection.ASC, NullOrder.NULLS_LAST)))),
            )
        }
        val regs =
            (0 until files).map { i ->
                FileRegistration(
                    path = "s3://$BUCKET/$cat/data/ns/t/m$i.parquet",
                    recordCount = recordsEach,
                    fileSizeBytes = bytesEach.toLong(),
                )
            }
        commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", regs))))
        return cat
    }

    /** A SORTED (id ASC) table with one real parquet object per entry of [files], rows in the given order. */
    private fun realSortedTableOf(
        label: String,
        files: List<List<Long>>,
    ): String {
        val cat = "heap-$label-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", fixtureColumns)
        alter.alterTable(cat, "ns", "t", listOf(AlterOp.SetSortOrder(ASC)))
        appendReal(cat, files)
        return cat
    }

    /** Append one real parquet object per entry of [files] to `ns.t`. */
    private fun appendReal(
        cat: String,
        files: List<List<Long>>,
    ) {
        val regs =
            files.map { ids ->
                val bytes = parquetBytes(ids)
                val path = "s3://$BUCKET/$cat/data/ns/t/a${counter.incrementAndGet()}.parquet"
                store.put(path, bytes)
                FileRegistration(path, ids.size.toLong(), bytes.size.toLong())
            }
        commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", regs))))
    }

    private fun livePaths(cat: String): List<String> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT f.path FROM hog_data_file f
                JOIN hog_catalog c ON c.catalog_id = f.catalog_id
                WHERE c.name = :cat AND f.end_snapshot IS NULL ORDER BY f.data_file_id
                """,
            ).bind("cat", cat).mapTo(String::class.java).list()
        }

    /** The `id` column of the object at [path], in physical order. */
    private fun readIds(path: String): List<Long> {
        val tmp = Files.createTempFile("heap-budget-read", ".parquet")
        val out = ArrayList<Long>()
        try {
            Files.write(tmp, store.get(path))
            org.apache.parquet.hadoop.ParquetFileReader.open(org.apache.parquet.io.LocalInputFile(tmp)).use { reader ->
                val schema = reader.footer.fileMetaData.schema
                val idIndex = schema.getFieldIndex("id")
                val columnIO = org.apache.parquet.io.ColumnIOFactory().getColumnIO(schema)
                var pages = reader.readNextRowGroup()
                while (pages != null) {
                    val rr =
                        columnIO.getRecordReader(
                            pages,
                            org.apache.parquet.example.data.simple.convert.GroupRecordConverter(schema),
                        )
                    repeat(Math.toIntExact(pages.rowCount)) { out += rr.read().getLong(idIndex, 0) }
                    pages = reader.readNextRowGroup()
                }
            }
        } finally {
            Files.deleteIfExists(tmp)
        }
        return out
    }

    private fun registeredBytes(
        h: org.jdbi.v3.core.Handle,
        cat: String,
    ): Long =
        h.createQuery(
            """
            SELECT sum(f.file_size_bytes) FROM hog_data_file f
            JOIN hog_catalog c ON c.catalog_id = f.catalog_id
            WHERE c.name = :cat AND f.end_snapshot IS NULL
            """,
        ).bind("cat", cat).mapTo(Long::class.java).one()

    /** The latest compaction ledger row's stored result JSON for [cat]. */
    private fun lastLedgerResult(cat: String): String =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT result FROM hog_maintenance_run
                 WHERE catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = :cat)
                   AND task = 'compaction'
                 ORDER BY run_id DESC LIMIT 1
                """,
            ).bind("cat", cat).mapTo(String::class.java).one()
        }

    /**
     * The UNSORTED twin of [realTable], which matters more than it
     * sounds. Every other failure test here sets a sort order, and the
     * sorted rewrite materialises all its inputs BEFORE opening the
     * writer — so the upload never starts and a failure cannot leave an
     * object behind whatever the code does. The streaming path opens
     * the writer first, which is where a failure genuinely can publish
     * something, and it had no failure coverage at all.
     */
    private fun unsortedRealTable(label: String): String {
        val cat = "heap-$label-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", fixtureColumns)
        val regs =
            (0 until 3).map { i ->
                val bytes = parquetBytes((0 until 4).map { it + i * 4L })
                val path = "s3://$BUCKET/$cat/data/ns/t/r$i.parquet"
                store.put(path, bytes)
                FileRegistration(path, 4, bytes.size.toLong())
            }
        commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", regs))))
        return cat
    }

    /** Does the object actually exist in MinIO? */
    private fun objectExists(pathUri: String): Boolean =
        try {
            store.get(pathUri)
            true
        } catch (_: Exception) {
            false
        }

    /** A sorted table with three REAL small parquet objects behind it. */
    private fun realTable(label: String): String {
        val cat = "heap-$label-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", fixtureColumns)
        alter.alterTable(
            cat,
            "ns",
            "t",
            listOf(AlterOp.SetSortOrder(listOf(SortFieldDef(1, SortDirection.ASC, NullOrder.NULLS_LAST)))),
        )
        val regs =
            (0 until 3).map { i ->
                val bytes = parquetBytes((0 until 4).map { it + i * 4L })
                val path = "s3://$BUCKET/$cat/data/ns/t/r$i.parquet"
                store.put(path, bytes)
                FileRegistration(path, 4, bytes.size.toLong())
            }
        commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", regs))))
        return cat
    }

    /**
     * A config whose target takes the whole of [realTable] as one group
     * and whose heap ceiling is far above it: these tests are about what
     * happens when a group blows up mid-flight, not about the ceiling.
     */
    private fun smallFileConfig() =
        CompactionConfig(
            targetBytes = 65536,
            // These fixtures are three small files: they are exercising
            // the heap ceiling and the failure paths, not the grouping
            // policy, so they pin a grouping that yields one group.
            minInputFiles = 2,
            maxInputFiles = 3,
            // Pinned: these fixtures are three small files and pin a
            // grouping that yields one group, so the fan-in must not
            // scale past them.
            maxFanIn = 3,
            maxGroupsPerRun = 1,
            sortedHeapBytes = CompactionConfig.DEFAULT_SORTED_HEAP_BYTES,
        )

    private fun liveFileCount(cat: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT count(*) FROM hog_data_file f
                JOIN hog_catalog c ON c.catalog_id = f.catalog_id
                WHERE c.name = :cat AND f.end_snapshot IS NULL
                """,
            ).bind("cat", cat).mapTo(Long::class.java).one()
        }

    private fun filePaths(cat: String): List<String> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT f.path FROM hog_data_file f
                JOIN hog_catalog c ON c.catalog_id = f.catalog_id WHERE c.name = :cat
                """,
            ).bind("cat", cat).mapTo(String::class.java).list()
        }

    private fun headSnapshot(cat: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT last_snapshot_id FROM hog_catalog WHERE name = :cat")
                .bind("cat", cat).mapTo(Long::class.java).one()
        }

    private data class RemovalRow(val path: String, val reason: String, val drainedAt: java.time.Instant?)

    private fun removalRows(cat: String): List<RemovalRow> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT r.path, r.reason, r.drained_at FROM hog_file_removal r
                JOIN hog_catalog c ON c.catalog_id = r.catalog_id
                WHERE c.name = :cat ORDER BY r.removal_id
                """,
            )
                .bind("cat", cat)
                .map { rs, _ ->
                    RemovalRow(
                        rs.getString("path"),
                        rs.getString("reason"),
                        rs.getTimestamp("drained_at")?.toInstant(),
                    )
                }
                .list()
        }

    private val fixtureColumns =
        listOf(
            ColumnDef("id", ColType.LONG, nullable = false),
            ColumnDef("name", ColType.STRING),
            ColumnDef("score", ColType.DOUBLE),
        )

    /** The same forest the catalog holds for [fixtureColumns] — three scalar nodes. */
    private val fixtureLiveColumns: List<Column> =
        fixtureColumns.mapIndexed { i, def -> Column((i + 1).toLong(), i, def) }

    private val fixtureSchema: MessageType =
        Types.buildMessage()
            .addField(Types.required(PrimitiveTypeName.INT64).id(1).named("id"))
            .addField(
                Types.optional(PrimitiveTypeName.BINARY)
                    .`as`(LogicalTypeAnnotation.stringType()).id(2).named("name"),
            )
            .addField(Types.optional(PrimitiveTypeName.DOUBLE).id(3).named("score"))
            .named("t")

    private fun parquetBytes(ids: List<Long>): ByteArray {
        val tmp = Files.createTempFile("heap-budget", ".parquet")
        Files.deleteIfExists(tmp)
        val factory = SimpleGroupFactory(fixtureSchema)
        ExampleParquetWriter.builder(LocalOutputFile(tmp))
            .withType(fixtureSchema)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .build()
            .use { w ->
                for (id in ids) {
                    val g = factory.newGroup()
                    g.add("id", id)
                    g.add("name", "n$id")
                    g.add("score", id * 1.5)
                    w.write(g)
                }
            }
        val bytes = Files.readAllBytes(tmp)
        Files.deleteIfExists(tmp)
        return bytes
    }
}
