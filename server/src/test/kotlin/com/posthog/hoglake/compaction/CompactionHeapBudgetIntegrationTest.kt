package com.posthog.hoglake.compaction

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
import com.posthog.hoglake.service.VerifyService
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
import org.testcontainers.containers.MinIOContainer
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

/**
 * hoglake#118: the sorted path's group budget, and what a group it
 * cannot hold does instead.
 *
 * The bug these cover is a UNIT MISMATCH, so the fixtures are built to
 * separate the two units that used to be conflated. Group selection
 * reads input file BYTES; the sorted rewrite materializes every survivor
 * as a parquet-java `Group`, so what it holds is ROWS. As long as every
 * writer produced the same bytes per row the byte budget tracked the row
 * count well enough to survive; #115 made compaction's own zstd output
 * the input at every tier above the first — 1.70x denser on event data
 * (`SortedHeapMeasurement`) — and the same byte budget started admitting
 * 1.70x the rows.
 *
 * So the tables here differ in DENSITY while agreeing on bytes, which is
 * the one axis a byte-only budget is blind to. Registrations are
 * metadata-only on purpose (`hog_data_file.record_count` and
 * `file_size_bytes` are what the planner reads, and registration never
 * opens the parquet): where a group must NOT be executed, the objects
 * deliberately do not exist, so an attempt to fetch one would surface as
 * `failed_groups` and fail the test. That absence is the assertion that
 * a refused group spends no IO.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CompactionHeapBudgetIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val alter = AlterService(db.jdbi)
    private val verify = VerifyService(db.jdbi)
    private val counter = AtomicInteger(0)

    private companion object {
        const val BUCKET = "hoglake-heap-budget-test"

        /**
         * The fixture table is three scalar columns, so the sorted path
         * materializes FOUR nodes per row: the three data fields plus the
         * `_hog_row_id` carrier every compaction output writes.
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
    }

    @AfterAll
    fun stop() {
        db.close()
    }

    /** A heap budget that admits exactly [rows] rows of the fixture shape. */
    private fun heapBytesFor(rows: Long) = CompactionConfig.SORTED_HEAP_BYTES_PER_NODE * NODES_PER_ROW * rows

    // ---- the budget is a row budget, expressed in bytes ---------------------

    @Test
    fun `two tables of identical bytes are planned on their rows, not their bytes`() {
        // The regression this whole change exists for. Both tables hold
        // eight 128 KiB files and the same 1 MiB compaction target; the
        // ONLY difference is how many rows those bytes carry — 1024 each
        // in one, 4096 each in the other, the 4x a codec change can
        // plausibly move on real event data (measured 1.70x for
        // snappy -> zstd; 4x keeps the arithmetic exact).
        //
        // Selecting on bytes, both tables plan the same group: eight
        // files, 1 MiB, the top of the ladder. For the dense one that
        // group is 32,768 rows against a sort buffer sized for 8,192 —
        // four times over, which on the dev pod is ninety seconds of IO
        // ending in `java.lang.OutOfMemoryError: Java heap space`.
        //
        // Selecting on rows, the dense table's whole LADDER scales down
        // by its density, and the two tables' groups come out holding the
        // SAME NUMBER OF ROWS out of different numbers of bytes. That
        // equality is the assertion: it is true only if the budget is
        // denominated in the unit the heap actually holds.
        val cfg =
            CompactionConfig(
                targetBytes = 1024 * 1024,
                tierTarget = 8,
                maxGroupsPerRun = 1,
                sortedHeapBytes = heapBytesFor(8192),
            )
        val svc = CompactionService(db.jdbi, store, cfg)

        val sparse = sortedTable("sparse", files = 8, bytesEach = 128 * 1024, recordsEach = 1024)
        val dense = sortedTable("dense", files = 8, bytesEach = 128 * 1024, recordsEach = 4096)

        val sparsePlan = svc.planTable(sparse, "ns", "t", cfg)
        val densePlan = svc.planTable(dense, "ns", "t", cfg)
        assertThat(densePlan.groups)
            .describedAs(
                "planned on bytes, the dense table's only group is 32,768 rows and the row ceiling " +
                    "then has to refuse it, leaving nothing to compact (refused=%d)",
                densePlan.heapRefusedGroups,
            )
            .isNotEmpty()
        val sparseGroup = sparsePlan.groups.first()
        val denseGroup = densePlan.groups.first()

        assertThat(denseGroup.survivingRecords)
            .describedAs("the sort buffer holds rows, so equal-heap groups hold equal rows")
            .isEqualTo(sparseGroup.survivingRecords)
            .isEqualTo(8192)
        assertThat(denseGroup.totalBytes)
            .describedAs("4x the rows per byte buys 4x fewer bytes per group")
            .isEqualTo(sparseGroup.totalBytes / 4)
        assertThat(sparseGroup.files).hasSize(8)
        assertThat(denseGroup.files).hasSize(2)
        // And the bound the numbers above exist to respect.
        for (group in densePlan.groups) {
            assertThat(group.survivingRecords)
                .describedAs("no planned group may exceed the sorted row ceiling")
                .isLessThanOrEqualTo(cfg.sortedRowCeiling(fixtureLiveColumns))
        }
    }

    @Test
    fun `an unsorted table keeps the full byte target however dense it is`() {
        // The derate is the SORTED path's, and only the sorted path
        // materializes a group: the streaming path writes each survivor
        // as it reads it and its heap is flat in group size. Shrinking
        // ordinary tables' groups for a heap cost they do not pay would
        // be a permanent throughput tax on every table in the catalog.
        val cfg =
            CompactionConfig(
                targetBytes = 1024 * 1024,
                tierTarget = 8,
                maxGroupsPerRun = 1,
                sortedHeapBytes = heapBytesFor(8192),
            )
        val svc = CompactionService(db.jdbi, store, cfg)
        val dense = sortedTable("unsorted-dense", files = 8, bytesEach = 128 * 1024, recordsEach = 4096, sort = false)

        val group = svc.planTable(dense, "ns", "t", cfg).groups.first()
        assertThat(group.files).hasSize(8)
        assertThat(group.totalBytes).isEqualTo(1024L * 1024)
    }

    // ---- a group that cannot fit -------------------------------------------

    @Test
    fun `a group above the row ceiling is refused in metadata and spends no IO`() {
        // The density derate scales the ladder by the table's AVERAGE
        // bytes-per-row, which is an estimate; record_count is not. A
        // group whose registered survivors land above the ceiling anyway
        // — the minimal prefix reaching a tier quota can overshoot, and a
        // table mixing client snappy with compaction zstd has no single
        // density — is refused on the true number.
        //
        // "Spends no IO" is asserted by construction: these paths were
        // never uploaded, so an attempted fetch would be NoSuchKey and
        // would land in failed_groups. failed_groups staying zero is the
        // proof that nothing was attempted.
        val ceiling = 1024L
        val cfg =
            CompactionConfig(
                targetBytes = 65536,
                tierTarget = 2,
                maxGroupsPerRun = 1,
                sortedHeapBytes = heapBytesFor(ceiling),
            )
        val svc = CompactionService(db.jdbi, store, cfg)
        val cat = sortedTable("too-big", files = 4, bytesEach = 1024, recordsEach = 900)
        assertThat(cfg.sortedRowCeiling(fixtureLiveColumns)).isEqualTo(ceiling)

        val plan = svc.planTable(cat, "ns", "t", cfg)
        assertThat(plan.groups).describedAs("nothing survives the ceiling").isEmpty()
        assertThat(plan.heapRefusedGroups).isEqualTo(2)

        val result = svc.runOnce(cat, cfg, MaintenanceTrigger.LOOP)
        assertThat(result.heapBudgetExceeded)
            .describedAs(
                "both refusals counted even though maxGroupsPerRun is 1: a refusal costs no IO, so " +
                    "charging it to the run budget would let one un-compactable table starve the sweep",
            )
            .isEqualTo(2)
        assertThat(result.groupsCompacted).isZero()
        assertThat(result.failedGroups).describedAs("no fetch was attempted").isZero()
        assertThat(result.invalidData).isZero()
        assertThat(result.unconvertibleSchema).isZero()

        // Nothing moved: no staged object claimed, every input still live.
        assertThat(removalRows(cat)).isEmpty()
        assertThat(liveFileCount(cat)).isEqualTo(4)

        // Deterministic, and just as cheap the second time. The behaviour
        // being replaced was ninety seconds of IO per sweep, forever.
        val again = svc.runOnce(cat, cfg, MaintenanceTrigger.LOOP)
        assertThat(again.heapBudgetExceeded).isEqualTo(2)
        assertThat(again.failedGroups).isZero()
        assertVerifyPasses(cat)
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

        // Contained: counted as a heap outcome, not an escaped Error that
        // takes the whole sweep's accounting with it (which is what
        // `catch (e: Exception)` did, because an Error is not one).
        assertThat(result.heapBudgetExceeded).isEqualTo(1)
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
        // settles it. The object never made it to MinIO, so the honest
        // outcome is 'missing' — the ledger row is what stops it being an
        // orphan nothing knows about.
        val drained = CleanupService(db.jdbi, removalStore).runOnce(fx, batchSize = 100)
        assertThat(drained.removed + drained.missing).isEqualTo(1)
        assertThat(drained.stillReferenced).describedAs("never an invariant violation").isZero()
        assertThat(removalRows(fx).single().drainedAt).isNotNull()
        assertVerifyPasses(fx)
    }

    @Test
    fun `an OOM reading an input leaves a reclaimable orphan and no catalog row`() {
        // The site the dev OOM actually hit: the group dies while
        // reading its inputs.
        //
        // This used to be "before the staging ticket leaves nothing at
        // all" — inputs were fetched before the output path was minted,
        // so nothing had been claimed. Inputs are now read in place
        // DURING the rewrite, which is after the claim, so a read-side
        // failure leaves the same reclaimable ticket a write-side one
        // does. See the ordering note in CompactionService.compactGroup:
        // for a DV-free group there is no longer a pre-claim failure
        // point at all.
        //
        // What still matters, and is what this asserts: the failure is
        // COUNTED rather than unwinding the run ledger, no snapshot is
        // cut, no catalog row appears, and the claimed path is left for
        // the cleanup drain — which finds no object, because the
        // multipart upload was aborted.
        val cfg = smallFileConfig()
        val fx = realTable("oom-get")
        val svc = CompactionService(db.jdbi, OomOnGet(), cfg)
        val headBefore = headSnapshot(fx)

        val result = svc.runOnce(fx, cfg, MaintenanceTrigger.LOOP)

        assertThat(result.heapBudgetExceeded).isEqualTo(1)
        assertThat(result.groupsCompacted).isZero()
        assertThat(removalRows(fx)).describedAs("the claimed path is left to reclaim").hasSize(1)
        assertThat(liveFileCount(fx)).isEqualTo(3)
        assertThat(headSnapshot(fx)).describedAs("no snapshot was cut").isEqualTo(headBefore)
        assertVerifyPasses(fx)
    }

    @Test
    fun `a read fault on an UNSORTED table leaves no object behind`() {
        // The case the rest of this class cannot reach. On the streaming
        // path the writer — and therefore the multipart upload — is open
        // before the first input is read, so a read-side failure has a
        // live upload to leave behind. It must be discarded, not
        // completed: completion is atomic, and parquet writes a valid
        // footer while closing even on the exception path, so a
        // truncated-but-well-formed object is exactly what an
        // insufficiently careful implementation publishes here.
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
        assertVerifyPasses(fx)
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
     * A config whose ladder takes the whole of [realTable] as one group
     * and whose heap ceiling is far above it: these tests are about what
     * happens when a group blows up mid-flight, not about the ceiling.
     */
    private fun smallFileConfig() =
        CompactionConfig(
            targetBytes = 65536,
            tierTarget = 4,
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

    private fun assertVerifyPasses(cat: String) {
        val report = verify.runOnce(cat)
        assertThat(report.status)
            .describedAs("verify checks: " + report.checks.joinToString { "${it.check}=${it.violations}" })
            .isEqualTo("pass")
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
