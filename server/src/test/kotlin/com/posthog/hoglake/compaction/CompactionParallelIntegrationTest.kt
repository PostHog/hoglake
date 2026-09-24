package com.posthog.hoglake.compaction

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.VerifyService
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.testing.TestImages
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.MinIOContainer
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Compaction under CONCURRENCY: several groups of one sweep in flight
 * at once, several of one group's inputs open at once, and two
 * maintainers sweeping one catalog at once.
 *
 * The production shape all three answer, measured on gigahog-prod-us
 * (2026-09-24 00:00-00:15 UTC, catalog millpond-prod-us, table
 * main.events_raw):
 *
 *  - a group costs a FIXED ~8.5 s whatever it holds, because the cost
 *    is object-store LATENCY — 64 serialized opens, a plan and a commit;
 *  - the sweep ran those groups one at a time on one worker;
 *  - and TWO maintenance replicas planned the same candidate set, so a
 *    547 s sweep committed 34 groups and lost 30 more to the other
 *    replica's commits — half the fleet's object-store work bought
 *    nothing.
 *
 * Net drain was ~480 files/min against ~2,800 files/min of inflow.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CompactionParallelIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val verify = VerifyService(db.jdbi)
    private val counter = AtomicInteger(0)

    private companion object {
        const val BUCKET = "hoglake-compaction-parallel-test"

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
    }

    @AfterAll
    fun tearDown() = db.close()

    // ---- fixture -----------------------------------------------------------

    /** `id long (1, required)` — the smallest honest parquet file. */
    private val schema: MessageType =
        Types.buildMessage()
            .addField(Types.required(PrimitiveTypeName.INT64).id(1).named("id"))
            .addField(
                Types.optional(PrimitiveTypeName.BINARY)
                    .`as`(LogicalTypeAnnotation.stringType()).id(2).named("name"),
            )
            .named("t")

    private fun parquetBytes(ids: List<Long>): ByteArray {
        val tmp = Files.createTempFile("compact-parallel", ".parquet")
        try {
            Files.delete(tmp)
            val factory = SimpleGroupFactory(schema)
            ExampleParquetWriter.builder(LocalOutputFile(tmp))
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
                .use { w ->
                    for (id in ids) {
                        val g: Group = factory.newGroup()
                        g.add("id", id)
                        g.add("name", "row-$id")
                        w.write(g)
                    }
                }
            return Files.readAllBytes(tmp)
        } finally {
            Files.deleteIfExists(tmp)
        }
    }

    /**
     * A catalog with one UNSORTED table holding [groups] x
     * [filesPerGroup] tiny files.
     *
     * The group COUNT has to be exact for every assertion below, so the
     * fan-in cap is what closes a group here, never the byte target:
     * [policy] sets `maxInputFiles == minInputFiles == filesPerGroup`
     * against a 1 GiB target no fixture can reach, which makes the plan
     * exactly `files / filesPerGroup` groups whatever the parquet
     * writer happens to produce. Packing to the target instead would
     * make every count in this class a function of the fixture's byte
     * sizes.
     */
    private data class Fixture(val cat: String, val groups: Int, val filesPerGroup: Int) {
        val expectedFiles get() = groups * filesPerGroup
    }

    private fun fixture(
        prefix: String,
        groups: Int,
        filesPerGroup: Int = 2,
    ): Fixture {
        val cat = "$prefix-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(
            cat,
            "ns",
            "t",
            listOf(ColumnDef("id", ColType.LONG, nullable = false), ColumnDef("name", ColType.STRING)),
        )
        // Each file's CONTENT is distinct, and distinct in the order the
        // files are registered. That is what lets an assertion tell a
        // correctly-ordered merge from a mis-paired one: with identical
        // fixtures, handing input i the reader of input j produces
        // byte-identical output and every ordering assertion is vacuous.
        var next = 0L
        val regs =
            (0 until groups).flatMap { g ->
                (0 until filesPerGroup).map { f ->
                    val path = "s3://$BUCKET/$cat/data/ns/t/g$g-f$f.parquet"
                    val bytes = parquetBytes(listOf(next * 100, next * 100 + 1, next * 100 + 2))
                    next++
                    store.put(path, bytes)
                    FileRegistration(path, 3, bytes.size.toLong())
                }
            }
        // Registered in chunks: one commit carrying 400 files is a
        // different test (and a slower one) than the compaction this
        // class is about.
        regs.chunked(50).forEach { chunk ->
            commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", chunk))))
        }
        return Fixture(cat, groups, filesPerGroup)
    }

    /**
     * Unsorted, small target, [filesPerGroup]-file groups exactly: the
     * max fan-in is what closes a group here, so the group count is
     * files / filesPerGroup and nothing else.
     */
    private fun policy(
        fixture: Fixture,
        parallelGroups: Int = 1,
        claims: Boolean = true,
        maxGroups: Int = fixture.groups * 2,
    ) = CompactionConfig(
        targetBytes = 1L shl 30,
        minInputFiles = fixture.filesPerGroup,
        maxInputFiles = fixture.filesPerGroup,
        maxGroupsPerRun = maxGroups,
        parallelGroups = parallelGroups,
        claimsEnabled = claims,
        inputOpenParallelism = 4,
    )

    private fun liveFileCount(cat: String): Int = catalogs.listFiles(cat, "ns", "t").size

    private fun assertVerifyPasses(cat: String) {
        val report = verify.runOnce(cat)
        assertThat(report.status)
            .describedAs("verify checks: " + report.checks.joinToString { "${it.check}=${it.violations}" })
            .isEqualTo("pass")
    }

    // ---- 1: concurrent groups within one sweep -----------------------------

    @Test
    fun `a parallel sweep commits every group exactly once and its counters stay exact`() {
        // EXACTNESS is the assertion, not throughput. Eleven counters
        // used to be `var`s incremented from the loop body, which is
        // correct for one thread and silently lossy for eight: `x++` on
        // a JVM long is neither atomic nor visible across threads, so a
        // parallel sweep would under-report compacted groups and
        // rewritten files while committing all of them — a compactor
        // that looks like it is falling behind while it is not.
        //
        // The three quantities are checked against each other AND
        // against the catalog, which is the only source that cannot lie:
        // 40 groups of 2 files must leave exactly 40 live files, and
        // every input's rows must still be readable.
        val fx = fixture("compact-par-counters", groups = 40)
        val svc = CompactionService(db.jdbi, store, policy(fx))
        val result = svc.runOnce(fx.cat, policy(fx, parallelGroups = 8))

        assertThat(result.groupsCompacted).isEqualTo(fx.groups.toLong())
        assertThat(result.filesOut).isEqualTo(fx.groups.toLong())
        assertThat(result.filesIn).isEqualTo(fx.expectedFiles.toLong())
        assertThat(result.failedGroups).isZero()
        assertThat(result.skippedConflicts).isZero()
        assertThat(result.dvSuperseded).isZero()
        assertThat(result.unconvertibleSchema).isZero()
        assertThat(result.invalidData).isZero()
        assertThat(result.heapBudgetExceeded).isZero()
        assertThat(result.bytesIn).isGreaterThan(0)
        assertThat(result.bytesOut).isGreaterThan(0)

        assertThat(liveFileCount(fx.cat))
            .describedAs("the catalog is the witness the counters are checked against")
            .isEqualTo(fx.groups)
        // Snapshot ids stay dense and ordered under concurrent commits —
        // invariant 1, which is the one thing the commit lock is for and
        // therefore the one thing parallel commits could break.
        assertVerifyPasses(fx.cat)
    }

    @Test
    fun `the sweep never attempts more groups than its budget, however many workers run`() {
        // maxGroupsPerRun is the commit-storm guard, and concurrency is
        // exactly the change that could turn it into a per-worker budget
        // by accident. 40 groups exist; 6 are allowed.
        val fx = fixture("compact-par-budget", groups = 40)
        val svc = CompactionService(db.jdbi, store, policy(fx))
        val result = svc.runOnce(fx.cat, policy(fx, parallelGroups = 8, maxGroups = 6))
        assertThat(result.groupsCompacted).isEqualTo(6)
        assertThat(liveFileCount(fx.cat)).isEqualTo(fx.expectedFiles - 6 * fx.filesPerGroup + 6)
        assertVerifyPasses(fx.cat)
    }

    @Test
    fun `the per-catalog commit lock is NOT held across the rewrite or the upload`() {
        // The property that makes concurrent groups safe at all, and the
        // one AGENT.md states as a rule: the commit lock is for commits.
        // If commitGroup's transaction were opened around the rewrite —
        // or if anything upstream of it took the lock — then N
        // concurrent groups would serialize on object-store IO AND every
        // foreground writer would queue behind a multi-second S3 read.
        //
        // Asserted by blocking the rewrite in the middle of its first
        // ranged read and then taking the real lock, with the real key,
        // from another connection. It must be free.
        val fx = fixture("compact-par-lock", groups = 1, filesPerGroup = 2)
        val catalogId =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = :n")
                    .bind("n", fx.cat).mapTo(Long::class.javaObjectType).one()
            }
        val inRewrite = CountDownLatch(1)
        val release = CountDownLatch(1)
        val blocking =
            object : ObjectStore(
                endpoint = minio.s3URL,
                region = "us-east-1",
                accessKey = minio.userName,
                secretKey = minio.password,
                pathStyle = true,
            ) {
                override fun getRange(
                    pathUri: String,
                    startInclusive: Long,
                    length: Int,
                ): ByteArray {
                    inRewrite.countDown()
                    // Bounded: a hang here must fail the test, not the
                    // suite.
                    release.await(30, TimeUnit.SECONDS)
                    return super.getRange(pathUri, startInclusive, length)
                }
            }
        val pool = Executors.newSingleThreadExecutor { r -> Thread(r, "rewrite").apply { isDaemon = true } }
        try {
            val sweep =
                pool.submit<Long> {
                    CompactionService(db.jdbi, blocking, policy(fx)).runOnce(fx.cat, policy(fx))
                        .groupsCompacted
                }
            assertThat(inRewrite.await(60, TimeUnit.SECONDS))
                .describedAs("the rewrite never reached its first object-store read")
                .isTrue()
            // The lock, taken the way every commit tail takes it. A held
            // lock would block here until the latch is released, which
            // is never on this thread.
            val heldElsewhere =
                Executors.newSingleThreadExecutor().submit<Boolean> {
                    db.jdbi.inTransactionUnchecked { h ->
                        Locks.acquireCatalogCommitLock(h, catalogId, lockTimeoutMs = 5_000)
                        true
                    }
                }
            assertThat(heldElsewhere.get(30, TimeUnit.SECONDS))
                .describedAs("the commit lock must be free while a rewrite is in flight")
                .isTrue()
            release.countDown()
            assertThat(sweep.get(120, TimeUnit.SECONDS)).isEqualTo(1L)
        } finally {
            release.countDown()
            pool.shutdownNow()
            blocking.close()
        }
        assertVerifyPasses(fx.cat)
    }

    // ---- 2: parallel input opens -------------------------------------------

    @Test
    fun `opening a 64-file group's inputs concurrently preserves the output and reports its cost`() {
        // The MEASUREMENT deliverable, and a correctness assertion
        // around it. The window may only overlap the `open` round trips:
        // the merge order, the row ids and the row count must come out
        // byte-for-byte the same as the one-at-a-time rewrite, or the
        // speed is bought with the lineage guarantee.
        //
        // The wall times are REPORTED, never asserted. Against a MinIO
        // container on the same host the per-open latency is a fraction
        // of a millisecond — the quantity this optimization removes is
        // barely present — so an assertion on the ratio would be
        // asserting the test machine's loopback, and it would be the
        // flakiest thing in the suite. The number that matters is
        // production's, and this prints the shape of it.
        val fx = fixture("compact-par-opens", groups = 1, filesPerGroup = 64)
        val files = catalogs.listFiles(fx.cat, "ns", "t").sortedBy { it.rowIdStart }
        assertThat(files).hasSize(64)
        val inputs =
            files.map { f ->
                ParquetRewriter.Input(
                    S3InputFile(store, f.path, f.fileSizeBytes, f.footerSize),
                    f.path,
                    f.rowIdStart,
                )
            }
        val (catalogId, tableId) = ids(fx.cat)
        val columns =
            db.jdbi.withHandleUnchecked { h ->
                val cat = com.posthog.hoglake.persistence.CatalogRepo.findByName(h, fx.cat)!!
                com.posthog.hoglake.persistence.TableRepo.columnsAt(
                    h,
                    catalogId,
                    tableId,
                    cat.headSnapshotId,
                )
            }

        /** Wall time, rows written, and the output's (row id, id) pairs in physical order. */
        fun rewriteAt(parallelism: Int): Triple<Long, Long, List<Pair<Long, Long>>> {
            val path = "s3://$BUCKET/${fx.cat}/measure-$parallelism-${UUID.randomUUID()}.parquet"
            val sink = S3OutputFile(store, path)
            val started = System.nanoTime()
            val out =
                ParquetRewriter.rewrite(
                    inputs,
                    columns,
                    emptyList(),
                    sink,
                    inputOpenParallelism = parallelism,
                )
            val elapsedMs = (System.nanoTime() - started) / 1_000_000
            return Triple(elapsedMs, out.rowsWritten, readRows(store.get(path)))
        }

        // Warm the JIT and the S3 client's connection pool once, so the
        // two measurements below are comparable to each other.
        rewriteAt(1)
        val sequential = rewriteAt(1)
        val parallel = rewriteAt(8)

        println(
            "[compaction] 64-file group rewrite: sequential opens ${sequential.first} ms, " +
                "8-way parallel opens ${parallel.first} ms " +
                "(${sequential.second} rows each, MinIO on localhost)",
        )

        assertThat(parallel.second)
            .describedAs("the window must not change what is written")
            .isEqualTo(sequential.second)
        assertThat(parallel.third)
            .describedAs("merge ORDER is preserved: only the open overlaps")
            .isEqualTo(sequential.third)
        // Pinned against the INPUTS, not merely against the other run:
        // each input's rows carry its own id values, and each input's row
        // ids are positional from its own rowIdStart, so a window that
        // handed input i the reader of input j shows up here as a value
        // paired with the wrong id. Asserting only "the two runs agree"
        // would pass on two runs that are wrong in the same way.
        val expected = (0L until 64L).flatMap { f -> (0L until 3L).map { r -> (f * 3 + r) to (f * 100 + r) } }
        assertThat(parallel.third)
            .describedAs("each row keeps its own file's value under its own row id")
            .isEqualTo(expected)
    }

    /** Every output row as (_hog_row_id, id), in physical order. */
    private fun readRows(bytes: ByteArray): List<Pair<Long, Long>> {
        val out = mutableListOf<Pair<Long, Long>>()
        val tmp = Files.createTempFile("compact-parallel-read", ".parquet")
        try {
            Files.write(tmp, bytes)
            org.apache.parquet.hadoop.ParquetFileReader.open(org.apache.parquet.io.LocalInputFile(tmp))
                .use { reader ->
                    val fileSchema = reader.footer.fileMetaData.schema
                    val rowIdIndex = fileSchema.getFieldIndex(ParquetRewriter.ROW_ID_COLUMN)
                    val idIndex = fileSchema.getFieldIndex("id")
                    val columnIO = org.apache.parquet.io.ColumnIOFactory().getColumnIO(fileSchema)
                    var pages = reader.readNextRowGroup()
                    while (pages != null) {
                        val rr =
                            columnIO.getRecordReader(
                                pages,
                                org.apache.parquet.example.data.simple.convert.GroupRecordConverter(
                                    fileSchema,
                                ),
                            )
                        repeat(Math.toIntExact(pages.rowCount)) {
                            val g = rr.read()
                            out += g.getLong(rowIdIndex, 0) to g.getLong(idIndex, 0)
                        }
                        pages = reader.readNextRowGroup()
                    }
                }
        } finally {
            Files.deleteIfExists(tmp)
        }
        return out
    }

    // ---- 3: group claims ---------------------------------------------------

    @Test
    fun `two maintainers sweeping one catalog lose no plan-to-commit races when claims are on`() {
        // THE production defect, reproduced at scale and then fixed. Two
        // CompactionService instances — two maintenance replicas, two
        // claimant ids — sweep the same catalog at the same moment over
        // 200 groups they both planned.
        //
        // Without claims, both rewrite and upload every group and one of
        // the two loses the re-verification at commit: measured on
        // gigahog-prod-us as 30 lost against 34 committed in one 547 s
        // sweep, every loss a full rewrite and upload discarded and its
        // staged object left for the cleanup drain.
        //
        // With claims, the group's claim arbitrates BEFORE the IO. The
        // two assertions that pin it are that the 200 groups are
        // committed EXACTLY once between the two maintainers and that
        // neither lost a race — and, crucially, that claimed_elsewhere
        // accounts for the other 200 attempts, because a version of this
        // that simply failed to plan anything would satisfy the first
        // two on its own.
        val fx = fixture("compact-par-claims", groups = 200)
        val a = CompactionService(db.jdbi, store, policy(fx))
        val b = CompactionService(db.jdbi, store, policy(fx))
        val cfg = policy(fx, parallelGroups = 4)
        val pool = Executors.newFixedThreadPool(2) { r -> Thread(r, "maintainer").apply { isDaemon = true } }
        val (ra, rb) =
            try {
                val fa = pool.submit<com.posthog.hoglake.model.CompactionResult> { a.runOnce(fx.cat, cfg) }
                val fb = pool.submit<com.posthog.hoglake.model.CompactionResult> { b.runOnce(fx.cat, cfg) }
                fa.get(600, TimeUnit.SECONDS) to fb.get(600, TimeUnit.SECONDS)
            } finally {
                pool.shutdownNow()
            }

        assertThat(ra.skippedConflicts + rb.skippedConflicts)
            .describedAs("a claim taken before the IO is a race not run: %s / %s", ra, rb)
            .isZero()
        assertThat(ra.groupsCompacted + rb.groupsCompacted)
            .describedAs("every group compacts exactly once across the two maintainers")
            .isEqualTo(fx.groups.toLong())
        assertThat(ra.claimedElsewhere + rb.claimedElsewhere)
            .describedAs(
                "both planned all %d groups, so every group one maintainer took is one the " +
                    "other counted as claimed — the counter that proves work was AVOIDED " +
                    "rather than never planned",
                fx.groups,
            )
            .isEqualTo(fx.groups.toLong())
        assertThat(ra.failedGroups + rb.failedGroups).isZero()
        assertThat(liveFileCount(fx.cat)).isEqualTo(fx.groups)

        // A COMMITTED group keeps its claim deliberately: its inputs are
        // dead, and the other maintainer's stale plan still names them,
        // so the row is what turns that maintainer's arrival into a
        // counted skip instead of a wasted rewrite. One row per
        // committed group, cleared by the lease and the purge.
        val settled = claimRows(fx.cat)
        assertThat(settled)
            .describedAs("one surviving claim per committed group, and no more")
            .hasSize(fx.groups)
        // And they do not accumulate: the purge at the head of the next
        // sweep takes every expired one. Asserted against the keys THIS
        // sweep left, not against an empty table — the next sweep
        // compacts the 200 fresh outputs and leaves claims of its own,
        // and an emptiness assertion would be measuring that instead.
        expireAllClaims(fx.cat)
        CompactionService(db.jdbi, store, policy(fx)).runOnce(fx.cat, policy(fx))
        assertThat(claimRows(fx.cat))
            .describedAs("expired claims are purged at the head of the next sweep")
            .doesNotContainAnyElementsOf(settled)
        assertVerifyPasses(fx.cat)
    }

    @Test
    fun `a claim is an optimization - with claims OFF the two maintainers race, and stay correct`() {
        // The other half of the claim's contract, and the one that
        // matters more: turning claims off must cost only WORK. The
        // plan-to-commit re-verification is the correctness backstop, so
        // every group still commits exactly once, nothing is lost, and
        // verify stays green — the losers simply spent a rewrite and an
        // upload to find out.
        //
        // Smaller than the 200-group case on purpose: this one does
        // roughly twice the object-store work by design.
        val fx = fixture("compact-par-noclaims", groups = 30)
        val a = CompactionService(db.jdbi, store, policy(fx))
        val b = CompactionService(db.jdbi, store, policy(fx))
        val cfg = policy(fx, parallelGroups = 4, claims = false)
        val pool = Executors.newFixedThreadPool(2) { r -> Thread(r, "maintainer").apply { isDaemon = true } }
        val (ra, rb) =
            try {
                val fa = pool.submit<com.posthog.hoglake.model.CompactionResult> { a.runOnce(fx.cat, cfg) }
                val fb = pool.submit<com.posthog.hoglake.model.CompactionResult> { b.runOnce(fx.cat, cfg) }
                fa.get(600, TimeUnit.SECONDS) to fb.get(600, TimeUnit.SECONDS)
            } finally {
                pool.shutdownNow()
            }
        println(
            "[compaction] claims OFF, two maintainers, ${fx.groups} groups: " +
                "committed ${ra.groupsCompacted + rb.groupsCompacted}, " +
                "lost races ${ra.skippedConflicts + rb.skippedConflicts}",
        )
        assertThat(ra.claimedElsewhere + rb.claimedElsewhere)
            .describedAs("claims are off, so nothing may be skipped as claimed")
            .isZero()
        assertThat(ra.groupsCompacted + rb.groupsCompacted)
            .describedAs("the backstop holds with the optimization off")
            .isEqualTo(fx.groups.toLong())
        assertThat(liveFileCount(fx.cat)).isEqualTo(fx.groups)
        assertVerifyPasses(fx.cat)
    }

    @Test
    fun `a dead maintainer's claim is reclaimable, and a live one is not`() {
        // The lease, at the row level: a claim nobody released blocks
        // the group only until it expires, and until then it blocks it
        // against every claimant including a second attempt by the same
        // one. Without the expiry a killed pod would fence its files
        // forever — which is precisely why nothing may depend on a claim
        // for correctness.
        val fx = fixture("compact-par-lease", groups = 1)
        val (catalogId, tableId) = ids(fx.cat)
        val one = UUID.randomUUID()
        val two = UUID.randomUUID()
        val key = "deadbeef"
        db.jdbi.useHandleUnchecked { h ->
            assertThat(CompactionClaimRepo.acquire(h, catalogId, tableId, key, listOf(1L, 2L), one, 900))
                .describedAs("an unclaimed group is claimable")
                .isTrue()
            assertThat(CompactionClaimRepo.acquire(h, catalogId, tableId, key, listOf(1L, 2L), two, 900))
                .describedAs("a LIVE claim fences every other claimant")
                .isFalse()
            assertThat(CompactionClaimRepo.liveClaimedFileIds(h, catalogId, tableId))
                .containsExactlyInAnyOrder(1L, 2L)

            // Someone else's claim is not ours to release.
            assertThat(CompactionClaimRepo.release(h, catalogId, tableId, key, two)).isZero()
            assertThat(CompactionClaimRepo.liveClaimedFileIds(h, catalogId, tableId)).isNotEmpty()

            // Expire it in place — the "maintainer died" state.
            h.createUpdate(
                "UPDATE hog_compaction_claim SET expires_at = now() - interval '1 second' " +
                    "WHERE catalog_id = :c AND table_id = :t",
            ).bind("c", catalogId).bind("t", tableId).execute()
            assertThat(CompactionClaimRepo.liveClaimedFileIds(h, catalogId, tableId))
                .describedAs("an expired claim fences nothing")
                .isEmpty()
            assertThat(CompactionClaimRepo.acquire(h, catalogId, tableId, key, listOf(1L, 2L), two, 900))
                .describedAs("and is reclaimable by whoever plans the group next")
                .isTrue()
            assertThat(CompactionClaimRepo.release(h, catalogId, tableId, key, two)).isEqualTo(1)

            // The bulk purge: the abandoned rows nothing re-plans.
            CompactionClaimRepo.acquire(h, catalogId, tableId, "orphan", listOf(9L), one, 900)
            h.createUpdate(
                "UPDATE hog_compaction_claim SET expires_at = now() - interval '1 hour' " +
                    "WHERE catalog_id = :c",
            ).bind("c", catalogId).execute()
            assertThat(CompactionClaimRepo.purgeExpired(h, catalogId)).isEqualTo(1)
            assertThat(CompactionClaimRepo.liveClaimedFileIds(h, catalogId, tableId)).isEmpty()
        }
    }

    @Test
    fun `the planner skips a claimed group by OVERLAP, not by claim-key equality`() {
        // Bin packing is a function of the candidate set, so two
        // replicas planning a moment apart pack the same files into
        // groups with different keys. A skip that matched keys would
        // wave every one of those through and leave the production race
        // exactly where it was.
        val fx = fixture("compact-par-overlap", groups = 2)
        val (catalogId, tableId) = ids(fx.cat)
        val cfg = policy(fx)
        val svc = CompactionService(db.jdbi, store, cfg)
        val before = svc.planTable(fx.cat, "ns", "t", cfg)
        assertThat(before.groups).hasSize(2)

        // Claim ONE file of the first group under a key no planner would
        // ever compute — the overlap is all the skip may use.
        val victim = before.groups.first().files.first().dataFileId
        db.jdbi.useHandleUnchecked { h ->
            CompactionClaimRepo.acquire(
                h,
                catalogId,
                tableId,
                "not-a-real-group-key",
                listOf(victim),
                UUID.randomUUID(),
                900,
            )
        }
        val after = svc.planTable(fx.cat, "ns", "t", cfg)
        assertThat(after.groups)
            .describedAs("the group sharing a single input with a live claim is not planned")
            .hasSize(1)
        assertThat(after.groups.single().files.map { it.dataFileId }).doesNotContain(victim)
        assertThat(after.claimedGroups).isEqualTo(1)

        // And the sweep spends its budget on the other group only.
        val result = svc.runOnce(fx.cat, cfg)
        assertThat(result.groupsCompacted).isEqualTo(1)
        assertThat(result.claimedElsewhere).isEqualTo(1)
    }

    // ---- 4: the budget, and what may spend it ------------------------------

    @Test
    fun `a group another maintainer claimed at EXECUTE time refunds its budget slot`() {
        // maxGroupsPerRun is a bound on OBJECT-STORE WORK, and both the
        // counter's KDoc and the OpenAPI say claimed_elsewhere does not
        // spend it. It did: the sweep collected an item per planned
        // group and charged the slot whether or not the group turned out
        // to be taken, so one pre-claimed group at maxGroupsPerRun=1
        // compacted NOTHING — the budget went to a group that never ran.
        //
        // The PLANNER's claim filter is not what is under test here; it
        // has always been free. This is the execute-time arbitration,
        // which is the one the planner cannot see: a claim taken after
        // the plan's snapshot. The fixture reproduces exactly that by
        // planting the first group's claim KEY with no input file ids,
        // so the planner's overlap read finds nothing and plans the
        // group, and the claim insert is what turns it away.
        val fx = fixture("compact-par-refund", groups = 2)
        val (catalogId, tableId) = ids(fx.cat)
        val cfg = policy(fx, maxGroups = 1)
        val svc = CompactionService(db.jdbi, store, cfg)
        val first = svc.planTable(fx.cat, "ns", "t", cfg).groups.first()
        db.jdbi.useHandleUnchecked { h ->
            assertThat(
                CompactionClaimRepo.acquire(
                    h,
                    catalogId,
                    tableId,
                    CompactionClaimRepo.groupKey(first),
                    // EMPTY: the planner skips by input-file overlap, and
                    // a claim it cannot see is the whole point.
                    emptyList(),
                    UUID.randomUUID(),
                    900,
                ),
            ).isTrue()
        }

        val result = svc.runOnce(fx.cat, cfg)
        assertThat(result.claimedElsewhere)
            .describedAs("the first group is taken, and the sweep found that out before any IO")
            .isEqualTo(1)
        assertThat(result.groupsCompacted)
            .describedAs("the refunded slot must buy the NEXT candidate: %s", result)
            .isEqualTo(1)
        assertThat(liveFileCount(fx.cat)).isEqualTo(fx.expectedFiles - fx.filesPerGroup + 1)
        assertVerifyPasses(fx.cat)
    }

    @Test
    fun `an EXPIRED claim stops blocking the group at the sweep level`() {
        // The lease is the only liveness protocol there is, so a
        // maintainer that died mid-rewrite must cost exactly one lease
        // and not one forever. Asserted through the whole sweep rather
        // than on the repo, because the planner's read and the claim
        // insert both carry their own `now()` comparison and either one
        // could pin the group on its own.
        val fx = fixture("compact-par-expired", groups = 1)
        val (catalogId, tableId) = ids(fx.cat)
        val cfg = policy(fx)
        val svc = CompactionService(db.jdbi, store, cfg)
        val group = svc.planTable(fx.cat, "ns", "t", cfg).groups.single()
        db.jdbi.useHandleUnchecked { h ->
            CompactionClaimRepo.acquire(
                h,
                catalogId,
                tableId,
                CompactionClaimRepo.groupKey(group),
                group.files.map { it.dataFileId },
                UUID.randomUUID(),
                900,
            )
            h.createUpdate(
                "UPDATE hog_compaction_claim SET expires_at = now() - interval '1 second' " +
                    "WHERE catalog_id = :c",
            ).bind("c", catalogId).execute()
        }
        // The planner sees it as dead...
        assertThat(svc.planTable(fx.cat, "ns", "t", cfg).groups)
            .describedAs("an expired claim fences nothing at plan time")
            .hasSize(1)
        // ...and so does the claim insert, which reclaims the row.
        val result = svc.runOnce(fx.cat, cfg)
        assertThat(result.groupsCompacted).isEqualTo(1)
        assertThat(result.claimedElsewhere).isZero()
    }

    @Test
    fun `a group that does not commit releases its claim immediately, not after the lease`() {
        // The release asymmetry's other half. A COMMITTED group keeps
        // its claim on a short lease on purpose; a group that failed,
        // skipped or was refused leaves files that are still live
        // candidates, and holding their claim for the rest of a
        // fifteen-minute lease would delay every retry — including the
        // sibling replica's, which is the one that might succeed.
        val fx = fixture("compact-par-release", groups = 1)
        // One input registered and never uploaded: the group fails in
        // the open, which is the cheapest non-committing outcome there
        // is.
        val victim = catalogs.listFiles(fx.cat, "ns", "t").first()
        deleteObject(victim.path)
        val cfg = policy(fx)
        val result = CompactionService(db.jdbi, store, cfg).runOnce(fx.cat, cfg)
        assertThat(result.failedGroups).isEqualTo(1)
        assertThat(result.groupsCompacted).isZero()
        assertThat(claimRows(fx.cat))
            .describedAs("a group that did not commit must leave no claim behind at all")
            .isEmpty()
    }

    @Test
    fun `a committed group's claim is kept on the SHORT lease, not the rewrite lease`() {
        // Kept, because a sibling's plan formed before this commit still
        // names the now-dead inputs — but kept for sweep intervals, not
        // for a rewrite lease. The full lease leaves roughly
        // committed-groups-per-sweep x lease/interval rows per table,
        // and the planner reads every one of their input-id arrays on
        // every pass.
        val fx = fixture("compact-par-shortlease", groups = 1)
        val cfg = policy(fx).copy(claimTtlSeconds = 3600, committedClaimTtlSeconds = 30)
        val result = CompactionService(db.jdbi, store, cfg).runOnce(fx.cat, cfg)
        assertThat(result.groupsCompacted).isEqualTo(1)
        val remaining = claimLeasesSeconds(fx.cat)
        assertThat(remaining).hasSize(1)
        assertThat(remaining.single())
            .describedAs("the committed claim must be on the 30s lease, not the 3600s one")
            .isBetween(0L, 60L)
    }

    @Test
    fun `a commit that cannot get the catalog lock in time is a counted race, not a failure`() {
        // AGENT.md's rule is that every acquirer of the per-catalog
        // commit lock passes commitLockTimeoutMs; compaction was the
        // standing exception, and under concurrency the exception stops
        // being survivable — N workers queue on that lock each holding a
        // pooled connection, and a foreground writer that then cannot
        // get a CONNECTION fails with a Hikari timeout (a 500) instead
        // of the typed, retryable CommitQueueTimeout (503) the admission
        // contract promises.
        //
        // Bounded, the timeout lands on compaction's side of the line:
        // race-class, counted with the plan-to-commit races, staged
        // output left undrained for the cleanup drain. Asserted by
        // holding the real lock, with the real key, from another
        // connection for the whole sweep.
        val fx = fixture("compact-par-lockwait", groups = 1)
        val (catalogId, _) = ids(fx.cat)
        val holding = java.util.concurrent.CountDownLatch(1)
        val release = java.util.concurrent.CountDownLatch(1)
        val holder =
            Thread({
                db.jdbi.inTransactionUnchecked { h ->
                    Locks.acquireCatalogCommitLock(h, catalogId)
                    holding.countDown()
                    release.await(60, TimeUnit.SECONDS)
                }
            }, "lock-holder")
        try {
            holder.start()
            assertThat(holding.await(30, TimeUnit.SECONDS)).isTrue()
            val cfg = policy(fx).copy(commitLockTimeoutMs = 750)
            val result = CompactionService(db.jdbi, store, cfg).runOnce(fx.cat, cfg)
            assertThat(result.skippedConflicts)
                .describedAs("a busy catalog is contention, not a broken compactor: %s", result)
                .isEqualTo(1)
            assertThat(result.failedGroups).isZero()
            assertThat(result.groupsCompacted).isZero()
            assertThat(liveFileCount(fx.cat)).isEqualTo(fx.expectedFiles)
        } finally {
            release.countDown()
            holder.join(30_000)
        }
        // The output it had already staged is reachable: an undrained
        // ticket is exactly what a lost plan-to-commit race leaves.
        assertThat(stagingTicketOutcomes(fx.cat))
            .describedAs("the staged output must stay queued for the cleanup drain")
            .containsExactly(null)
        // And the claim is released, because the group did not commit.
        assertThat(claimRows(fx.cat)).isEmpty()
    }

    @Test
    fun `each table is planned AFTER the previous table's groups have executed`() {
        // FRESHNESS, and the reason the sweep did not become
        // plan-everything-then-execute-everything when it learned to run
        // groups concurrently. A plan is metadata-only and cheap, and
        // its value decays: at the production settings (64 groups a
        // sweep, ~8.5 s a group) planning every table up front leaves
        // the last table's plan minutes old, so every one of its groups
        // arrives at its commit having been formed before several
        // minutes of ingest and of a sibling replica's commits.
        //
        // Asserted on the STATEMENTS the sweep issues, which is the only
        // place the ordering is visible: the second table's candidate
        // read must come after the first table's commit retired its
        // inputs.
        val fx = twoTableFixture("compact-par-freshness")
        val issued = CopyOnWriteArrayList<String>()
        val instrumented = com.posthog.hoglake.Database.jdbi(db.dataSource)
        instrumented.setSqlLogger(
            object : org.jdbi.v3.core.statement.SqlLogger {
                override fun logAfterExecution(context: org.jdbi.v3.core.statement.StatementContext) {
                    issued += context.renderedSql
                }
            },
        )
        val cfg =
            CompactionConfig(
                targetBytes = 1L shl 30,
                minInputFiles = 2,
                maxInputFiles = 2,
                maxGroupsPerRun = 8,
            )
        val result = CompactionService(instrumented, store, cfg).runOnce(fx, cfg)
        assertThat(result.groupsCompacted).isEqualTo(2)

        // The planner's candidate read, and the commit that retires a
        // group's inputs. Both are unmistakable in the statement stream
        // and neither is issued by anything else in a compaction sweep.
        val candidateReads =
            issued.withIndex()
                .filter { "FROM hog_data_file f" in it.value && "ORDER BY f.row_id_start" in it.value }
                .map { it.index }
        val retirements =
            issued.withIndex()
                .filter { "UPDATE hog_data_file SET end_snapshot" in it.value }
                .map { it.index }
        assertThat(candidateReads).describedAs("one candidate read per table").hasSize(2)
        assertThat(retirements).describedAs("one commit per group").hasSize(2)
        assertThat(candidateReads[0])
            .describedAs("the first table is planned before anything executes")
            .isLessThan(retirements[0])
        assertThat(candidateReads[1])
            .describedAs(
                "the second table was planned before the first table's group committed, so its " +
                    "plan is as old as every rewrite before it",
            )
            .isGreaterThan(retirements[0])
    }

    /** One catalog, two tables, one two-file group each. */
    private fun twoTableFixture(prefix: String): String {
        val cat = "$prefix-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        var next = 0L
        for (table in listOf("a", "b")) {
            catalogs.createTable(
                cat,
                "ns",
                table,
                listOf(ColumnDef("id", ColType.LONG, nullable = false), ColumnDef("name", ColType.STRING)),
            )
            val regs =
                (0 until 2).map { f ->
                    val path = "s3://$BUCKET/$cat/data/ns/$table/f$f.parquet"
                    val bytes = parquetBytes(listOf(next * 100, next * 100 + 1))
                    next++
                    store.put(path, bytes)
                    FileRegistration(path, 2, bytes.size.toLong())
                }
            commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", table, regs))))
        }
        return cat
    }

    @Test
    fun `the expired-claim purge runs even with claims DISABLED`() {
        // A flag flip must not strand rows. Claims that a previous
        // configuration wrote do not delete themselves, and the purge is
        // the only thing that removes the ones no re-planned group lands
        // on again — so gating it on the flag turns `compaction_claims`
        // red an hour after an operator turns claims off, on a
        // deployment that did nothing wrong.
        val fx = fixture("compact-par-purgeflip", groups = 1)
        val withClaims = policy(fx)
        assertThat(CompactionService(db.jdbi, store, withClaims).runOnce(fx.cat, withClaims).groupsCompacted)
            .isEqualTo(1)
        // The committed group's kept claim, aged past its lease.
        assertThat(claimRows(fx.cat)).hasSize(1)
        expireAllClaims(fx.cat)

        val withoutClaims = policy(fx, claims = false)
        CompactionService(db.jdbi, store, withoutClaims).runOnce(fx.cat, withoutClaims)
        assertThat(claimRows(fx.cat))
            .describedAs("turning claims off must not leave rows nothing will ever clear")
            .isEmpty()
        assertVerifyPasses(fx.cat)
    }

    // ---- 5: shutdown, interruption and the heap stop -----------------------

    @Test
    fun `interrupting the sweep stops the workers - nothing commits and every upload is aborted`() {
        // `finally { pool.shutdown() }` was not shutdown, it was a
        // promise to let the queue finish. BackgroundLoops runs the loop
        // body under runInterruptible and caps shutdown at 5s, so a
        // worker that outlives the sweep thread rewrites and COMMITS
        // after the supervisor believes compaction has stopped — and a
        // JVM kill in that window lands mid-upload, leaving multipart
        // parts that are billed and that no ledger row points at.
        val fx = fixture("compact-par-interrupt", groups = 6)
        val inRewrite = java.util.concurrent.CountDownLatch(1)
        val release = java.util.concurrent.CountDownLatch(1)
        val started = java.util.concurrent.atomic.AtomicInteger()
        val aborted = java.util.concurrent.atomic.AtomicInteger()
        val blocking =
            object : ObjectStore(
                endpoint = minio.s3URL,
                region = "us-east-1",
                accessKey = minio.userName,
                secretKey = minio.password,
                pathStyle = true,
            ) {
                override fun getRange(
                    pathUri: String,
                    startInclusive: Long,
                    length: Int,
                ): ByteArray {
                    inRewrite.countDown()
                    // Interruptible on purpose: this stands in for the
                    // object-store call a real worker is inside when the
                    // sweep is cancelled, and the whole question is
                    // whether anything interrupts it.
                    release.await(60, TimeUnit.SECONDS)
                    return super.getRange(pathUri, startInclusive, length)
                }

                override fun startMultipartUpload(pathUri: String): String =
                    super.startMultipartUpload(pathUri).also { started.incrementAndGet() }

                override fun abortMultipartUpload(
                    pathUri: String,
                    uploadId: String,
                ) {
                    aborted.incrementAndGet()
                    super.abortMultipartUpload(pathUri, uploadId)
                }
            }
        val cfg = policy(fx, parallelGroups = 3)
        val svc = CompactionService(db.jdbi, blocking, cfg)
        val sweep = Thread({ runCatching { svc.runOnce(fx.cat, cfg) } }, "sweep")
        try {
            sweep.start()
            assertThat(inRewrite.await(60, TimeUnit.SECONDS))
                .describedAs("no worker reached its first object-store read")
                .isTrue()
            sweep.interrupt()
            sweep.join(60_000)
            assertThat(sweep.isAlive)
                .describedAs("the sweep must not outlive its own interrupt")
                .isFalse()
            // The workers are gone WITH it: nothing may commit after the
            // sweep returned. Give a straggler a window to prove
            // otherwise before asserting.
            val liveAfterReturn = liveFileCount(fx.cat)
            Thread.sleep(1_000)
            assertThat(liveFileCount(fx.cat))
                .describedAs("a worker committed after the sweep returned")
                .isEqualTo(liveAfterReturn)
            assertThat(liveAfterReturn)
                .describedAs("no group may commit on an interrupted sweep")
                .isEqualTo(fx.expectedFiles)
            // Every upload a worker opened was aborted, so no multipart
            // parts are left billed and unreachable.
            assertThat(aborted.get())
                .describedAs("started %d uploads, aborted %d", started.get(), aborted.get())
                .isGreaterThanOrEqualTo(started.get())
        } finally {
            release.countDown()
            blocking.close()
        }
        // And the objects that WERE staged are reachable: every staging
        // ticket is undrained, so the cleanup drain reclaims them.
        assertThat(stagingTicketOutcomes(fx.cat))
            .describedAs("an interrupted sweep's staged outputs must stay queued for cleanup")
            .allMatch { it == null }
    }

    /**
     * A store that blocks the first read of every worker thread until
     * released, counting the multipart uploads it starts and aborts.
     */
    private inner class BlockingStore(
        private val inRewrite: java.util.concurrent.CountDownLatch,
        private val release: java.util.concurrent.CountDownLatch,
    ) : ObjectStore(
            endpoint = minio.s3URL,
            region = "us-east-1",
            accessKey = minio.userName,
            secretKey = minio.password,
            pathStyle = true,
        ) {
        val started = java.util.concurrent.atomic.AtomicInteger()
        val aborted = java.util.concurrent.atomic.AtomicInteger()
        val committedReads = java.util.concurrent.atomic.AtomicInteger()

        override fun getRange(
            pathUri: String,
            startInclusive: Long,
            length: Int,
        ): ByteArray {
            committedReads.incrementAndGet()
            inRewrite.countDown()
            release.await(60, TimeUnit.SECONDS)
            return super.getRange(pathUri, startInclusive, length)
        }

        override fun startMultipartUpload(pathUri: String): String =
            super.startMultipartUpload(pathUri).also { started.incrementAndGet() }

        override fun abortMultipartUpload(
            pathUri: String,
            uploadId: String,
        ) {
            // A worker's unwind takes measurable, NON-INTERRUPTIBLE
            // time — which is the only condition under which "did the
            // sweep wait for its workers?" is an observable question.
            // Without it the unwind finishes inside the microseconds
            // between the sweep returning and a test reading a counter,
            // and a shutdown that waits for nothing looks identical to
            // one that waits properly.
            val until = System.nanoTime() + 700_000_000L
            while (System.nanoTime() < until) Thread.onSpinWait()
            aborted.incrementAndGet()
            super.abortMultipartUpload(pathUri, uploadId)
        }
    }

    @Test
    fun `PROBE-SEQ - interrupting a SEQUENTIAL sweep stops it at the group it was in`() {
        // The default configuration has NO POOL: `shutdown` is a no-op,
        // nothing joins a future, and the only signal the loop gets is
        // this thread's interrupt flag. Without a check on it the sweep
        // sailed straight through cancellation — `executeGroup` catches
        // the interrupted rewrite as an ordinary failure, counts it, and
        // the loop pulls the next group. The probe: six groups,
        // interrupt at the first object-store read, five groups
        // committed afterwards.
        val fx = fixture("compact-par-seqint", groups = 6)
        val inRewrite = java.util.concurrent.CountDownLatch(1)
        val release = java.util.concurrent.CountDownLatch(1)
        val blocking = BlockingStore(inRewrite, release)
        val cfg = policy(fx).copy(inputOpenParallelism = 1)
        assertThat(cfg.parallelGroups).describedAs("this probe is about the DEFAULT path").isEqualTo(1)
        val svc = CompactionService(db.jdbi, blocking, cfg)
        val outcome = java.util.concurrent.atomic.AtomicReference<Throwable?>()
        val flagAfter = java.util.concurrent.atomic.AtomicBoolean(false)
        val sweep =
            Thread({
                try {
                    svc.runOnce(fx.cat, cfg)
                } catch (e: Throwable) {
                    outcome.set(e)
                } finally {
                    flagAfter.set(Thread.currentThread().isInterrupted)
                }
            }, "seq-sweep")
        try {
            sweep.start()
            assertThat(inRewrite.await(60, TimeUnit.SECONDS)).isTrue()
            sweep.interrupt()
            release.countDown()
            sweep.join(60_000)
            assertThat(sweep.isAlive).isFalse()
        } finally {
            release.countDown()
            blocking.close()
        }
        assertThat(outcome.get())
            .describedAs("a cancelled sweep must report cancellation, not a clean run")
            .isInstanceOf(InterruptedException::class.java)
        assertThat(flagAfter.get())
            .describedAs("the interrupt flag must survive for the caller that set it")
            .isTrue()
        assertThat(liveFileCount(fx.cat))
            .describedAs("no group may commit after the sweep was asked to stop")
            .isEqualTo(fx.expectedFiles)
        // And the ledger says what happened, WITH the counters the sweep
        // had earned (zero here, since the very first group was the one
        // interrupted — the point is that a row exists and is honest).
        val row = lastRun(fx.cat)
        assertThat(row.first).isEqualTo("failed")
        assertThat(row.second).describedAs("a cancelled sweep still records its tally").isNotNull()
    }

    @Test
    fun `PROBE-SEQ - a cancelled sweep records the groups it DID commit`() {
        // The accounting half of the same defect. The sweep rethrew and
        // lost its accumulator, so the ledger row for a sweep that
        // committed forty groups said it did nothing — and those commits
        // are durable, their inputs retired, their outputs live. Here
        // the first group is allowed through and the second is
        // interrupted.
        val fx = fixture("compact-par-seqpartial", groups = 3)
        val inSecond = java.util.concurrent.CountDownLatch(1)
        val release = java.util.concurrent.CountDownLatch(1)
        val groupsSeen = java.util.concurrent.atomic.AtomicInteger()
        val store2 =
            object : ObjectStore(
                endpoint = minio.s3URL,
                region = "us-east-1",
                accessKey = minio.userName,
                secretKey = minio.password,
                pathStyle = true,
            ) {
                override fun startMultipartUpload(pathUri: String): String {
                    // One output per group: the second group's output is
                    // where the sweep is told to stop.
                    if (groupsSeen.incrementAndGet() == 2) {
                        inSecond.countDown()
                        release.await(60, TimeUnit.SECONDS)
                    }
                    return super.startMultipartUpload(pathUri)
                }
            }
        val cfg = policy(fx).copy(inputOpenParallelism = 1)
        val svc = CompactionService(db.jdbi, store2, cfg)
        val sweep = Thread({ runCatching { svc.runOnce(fx.cat, cfg) } }, "seq-partial")
        try {
            sweep.start()
            assertThat(inSecond.await(60, TimeUnit.SECONDS)).isTrue()
            sweep.interrupt()
            release.countDown()
            sweep.join(60_000)
            assertThat(sweep.isAlive).isFalse()
        } finally {
            release.countDown()
            store2.close()
        }
        val (status, result) = lastRun(fx.cat)
        assertThat(status).isEqualTo("failed")
        val json = com.fasterxml.jackson.databind.ObjectMapper().readTree(checkNotNull(result))
        assertThat(json["groups_compacted"].asLong())
            .describedAs("the first group committed; the ledger has to say so: %s", result)
            .isEqualTo(1)
        assertThat(liveFileCount(fx.cat))
            .describedAs("exactly one group's worth of files was consolidated")
            .isEqualTo(fx.expectedFiles - fx.filesPerGroup + 1)
    }

    @Test
    fun `PROBE-SEQ - an interrupt the object store TRANSLATES still stops the sweep`() {
        // The shape that made `e is InterruptedException` insufficient.
        // An S3 client, the HTTP stack under it and NIO all turn an
        // interrupt into something else on the way up — a
        // ClosedByInterruptException, an IOException wrapping one — and
        // several of them CLEAR the flag while they are at it. On the
        // sequential path the catch runs on the sweep's own thread, so a
        // cancellation that arrives wrapped and leaves the flag down is
        // a sweep that counted the group as an ordinary failure and
        // carried on to the next one.
        val fx = fixture("compact-par-wrapped", groups = 5)
        val inRewrite = java.util.concurrent.CountDownLatch(1)
        val release = java.util.concurrent.CountDownLatch(1)
        val translating =
            object : ObjectStore(
                endpoint = minio.s3URL,
                region = "us-east-1",
                accessKey = minio.userName,
                secretKey = minio.password,
                pathStyle = true,
            ) {
                override fun getRange(
                    pathUri: String,
                    startInclusive: Long,
                    length: Int,
                ): ByteArray {
                    inRewrite.countDown()
                    try {
                        release.await(60, TimeUnit.SECONDS)
                    } catch (e: InterruptedException) {
                        // Exactly what a client does: swallow the flag,
                        // report a transport failure.
                        Thread.interrupted()
                        throw java.io.IOException("connection closed by interrupt", e)
                    }
                    return super.getRange(pathUri, startInclusive, length)
                }
            }
        val cfg = policy(fx).copy(inputOpenParallelism = 1)
        val svc = CompactionService(db.jdbi, translating, cfg)
        val outcome = java.util.concurrent.atomic.AtomicReference<Throwable?>()
        val sweep =
            Thread({
                try {
                    svc.runOnce(fx.cat, cfg)
                } catch (e: Throwable) {
                    outcome.set(e)
                }
            }, "wrapped-sweep")
        try {
            sweep.start()
            assertThat(inRewrite.await(60, TimeUnit.SECONDS)).isTrue()
            sweep.interrupt()
            release.countDown()
            sweep.join(60_000)
            assertThat(sweep.isAlive).isFalse()
        } finally {
            release.countDown()
            translating.close()
        }
        assertThat(outcome.get())
            .describedAs("an interrupt that arrived wrapped is still a cancellation")
            .isInstanceOf(InterruptedException::class.java)
        assertThat(liveFileCount(fx.cat))
            .describedAs("no group may commit after the sweep was asked to stop")
            .isEqualTo(fx.expectedFiles)
    }

    @Test
    fun `PROBE-ALLCAT - cancellation stops the instance sweep instead of visiting every catalog`() {
        // `catch (Exception)` matched InterruptedException, and
        // per-catalog isolation then did the wrong thing with it: it
        // logged the cancelled catalog and swept every remaining one,
        // each paying a purge, a plan, a wave and a bounded shutdown
        // after the supervisor had asked the loop to stop.
        val a = fixture("compact-par-allcat-a", groups = 2)
        val b = fixture("compact-par-allcat-b", groups = 2)
        val inRewrite = java.util.concurrent.CountDownLatch(1)
        val release = java.util.concurrent.CountDownLatch(1)
        val blocking = BlockingStore(inRewrite, release)
        // Both catalogs, in name order, from ONE service — the shape the
        // background loop runs.
        val cfg = policy(a).copy(inputOpenParallelism = 1)
        val svc = CompactionService(db.jdbi, blocking, cfg)
        val outcome = java.util.concurrent.atomic.AtomicReference<Throwable?>()
        val sweep =
            Thread({
                try {
                    svc.runOnceAllCatalogs(cfg)
                } catch (e: Throwable) {
                    outcome.set(e)
                }
            }, "allcat-sweep")
        try {
            sweep.start()
            assertThat(inRewrite.await(60, TimeUnit.SECONDS)).isTrue()
            sweep.interrupt()
            release.countDown()
            sweep.join(60_000)
            assertThat(sweep.isAlive).isFalse()
        } finally {
            release.countDown()
            blocking.close()
        }
        assertThat(outcome.get())
            .describedAs("the fan-out must surface cancellation, not swallow it per catalog")
            .isInstanceOf(InterruptedException::class.java)
        assertThat(liveFileCount(a.cat) + liveFileCount(b.cat))
            .describedAs("no catalog may be swept after the fan-out was asked to stop")
            .isEqualTo(a.expectedFiles + b.expectedFiles)
    }

    @Test
    fun `PROBE-POOL - an interrupted parallel sweep leaves no worker running past its return`() {
        // The pool path's half, and the assertion that pins `shutdown`'s
        // bounded wait: when the sweep RETURNS, its workers must already
        // have stopped. Without the flag being cleared for the wait,
        // `awaitTermination` throws instantly and the sweep hands back
        // to a caller while three workers are still aborting uploads
        // behind it.
        val fx = fixture("compact-par-poolint", groups = 6)
        val inRewrite = java.util.concurrent.CountDownLatch(1)
        val release = java.util.concurrent.CountDownLatch(1)
        val blocking = BlockingStore(inRewrite, release)
        val cfg = policy(fx, parallelGroups = 3).copy(inputOpenParallelism = 1)
        val svc = CompactionService(db.jdbi, blocking, cfg)
        val returned = java.util.concurrent.CountDownLatch(1)
        val outcome = java.util.concurrent.atomic.AtomicReference<Throwable?>()
        val flagAfter = java.util.concurrent.atomic.AtomicBoolean(false)
        val sweep =
            Thread({
                try {
                    svc.runOnce(fx.cat, cfg)
                } catch (e: Throwable) {
                    outcome.set(e)
                } finally {
                    flagAfter.set(Thread.currentThread().isInterrupted)
                    returned.countDown()
                }
            }, "pool-sweep")
        try {
            sweep.start()
            assertThat(inRewrite.await(60, TimeUnit.SECONDS)).isTrue()
            sweep.interrupt()
            release.countDown()
            assertThat(returned.await(60, TimeUnit.SECONDS)).isTrue()
            // THE ASSERTION: nothing moves after the sweep returned.
            val startsAtReturn = blocking.started.get()
            val abortsAtReturn = blocking.aborted.get()
            Thread.sleep(1_500)
            assertThat(blocking.started.get())
                .describedAs("a worker started an upload after the sweep returned")
                .isEqualTo(startsAtReturn)
            assertThat(blocking.aborted.get())
                .describedAs("a worker was still unwinding after the sweep returned")
                .isEqualTo(abortsAtReturn)
            sweep.join(60_000)
        } finally {
            release.countDown()
            blocking.close()
        }
        assertThat(outcome.get()).isInstanceOf(InterruptedException::class.java)
        assertThat(flagAfter.get())
            .describedAs(
                "the interrupt flag must survive the pool path too: it is how runInterruptible " +
                    "tells a cancelled loop body from a failed one",
            )
            .isTrue()
        assertThat(liveFileCount(fx.cat)).isEqualTo(fx.expectedFiles)
        assertThat(blocking.aborted.get())
            .describedAs("started %d uploads, aborted %d", blocking.started.get(), blocking.aborted.get())
            .isGreaterThanOrEqualTo(blocking.started.get())
        // And the ledger row carries the partial tally on this path as
        // well — the pool path reaches it through a worker's future
        // rather than through the loop's own flag check.
        assertThat(lastRun(fx.cat).let { it.first to (it.second != null) })
            .describedAs("a cancelled parallel sweep records a failed row WITH its counters")
            .isEqualTo("failed" to true)
    }

    @Test
    fun `an ERROR from a group is counted and the sweep carries on`() {
        // `catch (Exception)` did not match one. The nested copier
        // recurses, so a StackOverflowError is live in this path, and an
        // Error escaping a pool worker loses that group's accounting,
        // leaves its claim held for a lease, and used to let the rest of
        // the queue run behind a sweep that had already returned.
        val fx = fixture("compact-par-error", groups = 3)
        val failFirst = java.util.concurrent.atomic.AtomicBoolean(true)
        val erroring =
            object : ObjectStore(
                endpoint = minio.s3URL,
                region = "us-east-1",
                accessKey = minio.userName,
                secretKey = minio.password,
                pathStyle = true,
            ) {
                override fun getRange(
                    pathUri: String,
                    startInclusive: Long,
                    length: Int,
                ): ByteArray {
                    // An Error, not an Exception, and not an OOM — the
                    // OOM arm is separate and ends the sweep.
                    if (failFirst.compareAndSet(true, false)) throw StackOverflowError("synthetic")
                    return super.getRange(pathUri, startInclusive, length)
                }
            }
        val cfg = policy(fx).copy(inputOpenParallelism = 1)
        val result = CompactionService(db.jdbi, erroring, cfg).runOnce(fx.cat, cfg)
        erroring.close()
        assertThat(result.failedGroups)
            .describedAs("an Error is a counted group failure, not a lost one: %s", result)
            .isEqualTo(1)
        assertThat(result.groupsCompacted)
            .describedAs("and the other groups still compact")
            .isEqualTo((fx.groups - 1).toLong())
        // The failed group's claim is released like any non-commit.
        assertThat(claimLeasesSeconds(fx.cat)).hasSize(fx.groups - 1)
    }

    @Test
    fun `a row lock inside the commit tail times out as a counted race, not a failure`() {
        // The admission bound is TRANSACTION-LOCAL, so it stays in force
        // for the whole commit tail — deliberately, per Locks.kt. The
        // consequence is that a row lock later in the tail can expire
        // too, and it arrives as a raw driver exception rather than the
        // typed CommitQueueTimeout. Counting that as failed_groups reads
        // as a broken compactor when it is a busy catalog.
        val fx = fixture("compact-par-rowlock", groups = 1)
        val victim = catalogs.listFiles(fx.cat, "ns", "t").first()
        val (catalogId, _) = ids(fx.cat)
        val holding = java.util.concurrent.CountDownLatch(1)
        val release = java.util.concurrent.CountDownLatch(1)
        val holderFailed = java.util.concurrent.atomic.AtomicReference<Throwable?>()
        val holder =
            Thread({
                try {
                    db.jdbi.inTransactionUnchecked { h ->
                        // The row the commit tail's end_snapshot update
                        // must take: the advisory lock is free, so the
                        // group gets all the way INTO the tail before it
                        // blocks. Keyed on BOTH columns — data_file_id
                        // is per catalog, so the id alone matches rows
                        // this test does not own.
                        h.createQuery(
                            "SELECT data_file_id FROM hog_data_file " +
                                "WHERE catalog_id = :c AND data_file_id = :id FOR UPDATE",
                        )
                            .bind("c", catalogId)
                            .bind("id", victim.dataFileId)
                            .mapTo(Long::class.javaObjectType)
                            .list()
                        holding.countDown()
                        release.await(60, TimeUnit.SECONDS)
                    }
                } catch (e: Throwable) {
                    holderFailed.set(e)
                    holding.countDown()
                }
            }, "row-lock-holder")
        val result =
            try {
                holder.start()
                assertThat(holding.await(30, TimeUnit.SECONDS)).isTrue()
                assertThat(holderFailed.get()).describedAs("the row lock could not be taken").isNull()
                val cfg = policy(fx).copy(commitLockTimeoutMs = 750)
                CompactionService(db.jdbi, store, cfg).runOnce(fx.cat, cfg)
            } finally {
                release.countDown()
                holder.join(30_000)
            }
        assertThat(result.skippedConflicts)
            .describedAs("a lock timeout inside the tail is contention: %s", result)
            .isEqualTo(1)
        assertThat(result.failedGroups).isZero()
        assertThat(liveFileCount(fx.cat)).isEqualTo(fx.expectedFiles)
        assertThat(stagingTicketOutcomes(fx.cat))
            .describedAs("the staged output stays queued for the cleanup drain")
            .containsExactly(null)
    }

    /** The catalog's newest compaction ledger row: (status, result json). */
    private fun lastRun(cat: String): Pair<String, String?> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT status, CAST(result AS text) AS result
                  FROM hog_maintenance_run
                 WHERE catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = :cat)
                   AND task = 'compaction'
                 ORDER BY run_id DESC LIMIT 1
                """,
            ).bind("cat", cat).map { rs, _ -> rs.getString("status") to rs.getString("result") }.one()
        }

    @Test
    fun `a heap-exhausted group ends the sweep - the rest are never attempted`() {
        // The OOM stop, asserted on what it is FOR: the next group's
        // inputs must not be allocated into a heap that just proved it
        // has none to spare. Counting the object reads is the only
        // assertion that can tell "not attempted" from "attempted and
        // skipped" — the counters alone cannot.
        val fx = fixture("compact-par-oom", groups = 5)
        val read = java.util.concurrent.ConcurrentHashMap.newKeySet<String>()
        val oomStore =
            object : ObjectStore(
                endpoint = minio.s3URL,
                region = "us-east-1",
                accessKey = minio.userName,
                secretKey = minio.password,
                pathStyle = true,
            ) {
                override fun getRange(
                    pathUri: String,
                    startInclusive: Long,
                    length: Int,
                ): ByteArray {
                    read += pathUri
                    // Stands in for the allocation the row ceiling is
                    // supposed to have refused. A real one would come
                    // out of the sort buffer.
                    throw OutOfMemoryError("synthetic: sort buffer")
                }
            }
        val cfg = policy(fx, maxGroups = 5)
        val result = CompactionService(db.jdbi, oomStore, cfg).runOnce(fx.cat, cfg)
        assertThat(result.heapBudgetExceeded).isEqualTo(1)
        assertThat(result.groupsCompacted).isZero()
        assertThat(result.failedGroups).isZero()
        assertThat(read)
            .describedAs("only the group that OOM'd may have touched the object store")
            .hasSizeLessThanOrEqualTo(fx.filesPerGroup)
        oomStore.close()
    }

    // ---- 6: the claim table is an optimization, including when broken ------

    @Test
    fun `a broken claim table never stops compaction - the group runs unclaimed`() {
        // The position the whole design rests on, exercised rather than
        // asserted in a comment: claims are an optimization, so a claim
        // table that is missing, locked or broken must cost duplicated
        // work between replicas and cost NOTHING else. Both sides have
        // to hold — the planner's read (inside the planning transaction,
        // outside the per-group catch, so an error there kills the sweep
        // for every catalog on every interval) and the claim insert.
        //
        // Its own database, because the fix is to drop the table.
        PgTestSupport.freshDatabase().use { db2 ->
            val cats = CatalogService(db2.jdbi)
            val cat = "compact-noclaimtable"
            cats.createCatalog(cat, "s3://$BUCKET/$cat")
            cats.createNamespace(cat, "ns")
            cats.createTable(
                cat,
                "ns",
                "t",
                listOf(ColumnDef("id", ColType.LONG, nullable = false), ColumnDef("name", ColType.STRING)),
            )
            val bytes = parquetBytes(listOf(1L, 2L, 3L))
            val regs =
                (0 until 2).map { f ->
                    val path = "s3://$BUCKET/$cat/data/ns/t/broken-$f.parquet"
                    store.put(path, bytes)
                    FileRegistration(path, 3, bytes.size.toLong())
                }
            CommitService(db2.jdbi).commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", regs))))
            db2.jdbi.useHandleUnchecked { h -> h.execute("DROP TABLE hog_compaction_claim") }

            val cfg =
                CompactionConfig(
                    targetBytes = 1L shl 30,
                    minInputFiles = 2,
                    maxInputFiles = 2,
                    maxGroupsPerRun = 4,
                    claimsEnabled = true,
                )
            val result = CompactionService(db2.jdbi, store, cfg).runOnce(cat, cfg)
            assertThat(result.groupsCompacted)
                .describedAs("a broken optimization must not stop compaction: %s", result)
                .isEqualTo(1)
            assertThat(result.failedGroups).isZero()
            assertThat(result.claimedElsewhere)
                .describedAs("a claim that cannot be read fences nothing")
                .isZero()
        }
    }

    // ---- 7: concurrency, proved ---------------------------------------------

    @Test
    fun `parallelGroups really overlaps rewrites - N groups are inside the rewrite at once`() {
        // Every other test in this class passes with the worker count
        // hard-wired to 1: they assert the counters and the catalog, and
        // a sequential sweep gets those right. This one cannot: the
        // groups' rewrites hold a barrier that only trips when N of them
        // are inside simultaneously, so at parallelGroups=1 it times
        // out, the groups fail, and the assertions red.
        val n = 4
        val fx = fixture("compact-par-overlap-proof", groups = n)
        val barrier = java.util.concurrent.CyclicBarrier(n)
        val arrivedOnThisThread = ThreadLocal.withInitial { false }
        val peak = java.util.concurrent.atomic.AtomicInteger()
        val inside = java.util.concurrent.atomic.AtomicInteger()
        val barrierStore =
            object : ObjectStore(
                endpoint = minio.s3URL,
                region = "us-east-1",
                accessKey = minio.userName,
                secretKey = minio.password,
                pathStyle = true,
            ) {
                override fun getRange(
                    pathUri: String,
                    startInclusive: Long,
                    length: Int,
                ): ByteArray {
                    // Once per worker, at its first read, so the barrier
                    // measures GROUPS in flight rather than reads.
                    if (!arrivedOnThisThread.get()) {
                        arrivedOnThisThread.set(true)
                        val now = inside.incrementAndGet()
                        peak.updateAndGet { maxOf(it, now) }
                        barrier.await(20, TimeUnit.SECONDS)
                        inside.decrementAndGet()
                    }
                    return super.getRange(pathUri, startInclusive, length)
                }
            }
        // inputOpenParallelism = 1 so the barrier measures GROUPS.
        // The prefetch window opens a group's inputs on threads of its
        // own, and those reach `getRange` too — with the window on, 4
        // groups tripped a barrier of 4 using input-open threads alone,
        // which would have passed at parallelGroups=1.
        val cfg = policy(fx, parallelGroups = n).copy(inputOpenParallelism = 1)
        val result = CompactionService(db.jdbi, barrierStore, cfg).runOnce(fx.cat, cfg)
        barrierStore.close()
        assertThat(result.failedGroups)
            .describedAs("the barrier never tripped: the rewrites did not overlap (%s)", result)
            .isZero()
        assertThat(result.groupsCompacted).isEqualTo(n.toLong())
        assertThat(peak.get())
            .describedAs("at most one group was ever inside a rewrite")
            .isEqualTo(n)
        assertVerifyPasses(fx.cat)
    }

    // ---- 8: the prefetch window is a WINDOW ---------------------------------

    @Test
    fun `the input-open window never holds more than its bound open at once`() {
        // The bound is what makes the window safe to turn on by default:
        // an open-but-unread reader holds its parsed footer, and the
        // worst case is `window` of them. Unbounded, a 64-file group
        // would hold 64 — which for a real table's footers is the heap
        // this optimization was supposed to leave alone.
        val fx = fixture("compact-par-window", groups = 1, filesPerGroup = 16)
        val files = catalogs.listFiles(fx.cat, "ns", "t").sortedBy { it.rowIdStart }
        val open = java.util.concurrent.atomic.AtomicInteger()
        val peak = java.util.concurrent.atomic.AtomicInteger()
        val inputs =
            files.map { f ->
                val counted =
                    object : org.apache.parquet.io.InputFile {
                        private val delegate = S3InputFile(store, f.path, f.fileSizeBytes, f.footerSize)

                        override fun getLength(): Long = delegate.length

                        override fun newStream(): org.apache.parquet.io.SeekableInputStream {
                            // Slow, so the window has something to hold:
                            // with instant opens the pipeline drains as
                            // fast as it fills and the peak says nothing.
                            Thread.sleep(25)
                            val stream = delegate.newStream()
                            // The increment is OUTSIDE updateAndGet:
                            // AtomicInteger retries that lambda on CAS
                            // failure, so an increment inside it counts
                            // twice under contention — which is the one
                            // condition this assertion exists to measure.
                            val now = open.incrementAndGet()
                            peak.updateAndGet { maxOf(it, now) }
                            return object : org.apache.parquet.io.DelegatingSeekableInputStream(stream) {
                                override fun getPos(): Long = stream.pos

                                override fun seek(newPos: Long) = stream.seek(newPos)

                                override fun close() {
                                    open.decrementAndGet()
                                    stream.close()
                                }
                            }
                        }
                    }
                ParquetRewriter.Input(counted, f.path, f.rowIdStart)
            }
        val (catalogId, tableId) = ids(fx.cat)
        val columns =
            db.jdbi.withHandleUnchecked { h ->
                val c = com.posthog.hoglake.persistence.CatalogRepo.findByName(h, fx.cat)!!
                com.posthog.hoglake.persistence.TableRepo.columnsAt(h, catalogId, tableId, c.headSnapshotId)
            }
        val window = 4
        val out =
            ParquetRewriter.rewrite(
                inputs,
                columns,
                emptyList(),
                S3OutputFile(store, "s3://$BUCKET/${fx.cat}/window-${UUID.randomUUID()}.parquet"),
                inputOpenParallelism = window,
            )
        assertThat(out.rowsWritten).isEqualTo(48)
        assertThat(peak.get())
            .describedAs("the window must have been used at all")
            .isGreaterThan(1)
        assertThat(peak.get())
            .describedAs("more readers were open at once than the window allows")
            .isLessThanOrEqualTo(window)
        assertThat(open.get()).describedAs("every reader is closed when the group ends").isZero()
    }

    @Test
    fun `every prefetched reader is closed when a group fails mid-rewrite`() {
        // A task already inside `open` has an object-store stream in
        // flight, and cancelling its future does not close the reader
        // that call is about to return — nothing else holds a handle to
        // it. So the unwind path drains rather than cancels, and this is
        // what says so.
        val fx = fixture("compact-par-leak", groups = 1, filesPerGroup = 8)
        val files = catalogs.listFiles(fx.cat, "ns", "t").sortedBy { it.rowIdStart }
        val open = java.util.concurrent.atomic.AtomicInteger()
        val opened = java.util.concurrent.atomic.AtomicInteger()
        val inputs =
            files.mapIndexed { i, f ->
                val counted =
                    object : org.apache.parquet.io.InputFile {
                        private val delegate = S3InputFile(store, f.path, f.fileSizeBytes, f.footerSize)

                        override fun getLength(): Long = delegate.length

                        override fun newStream(): org.apache.parquet.io.SeekableInputStream {
                            // The inputs the window prefetches AHEAD of
                            // the one that fails open SLOWLY, and slowly
                            // enough to finish AFTER the unwind has
                            // given up waiting for them
                            // (ParquetRewriter.OPEN_DRAIN_MILLIS). That
                            // is the one state in which the unwind can
                            // leak a reader, and reproducing it takes
                            // three things, all of them deliberate:
                            //
                            //  - a BUSY wait, not a sleep, because an
                            //    interruptible sleep aborts instead of
                            //    completing and an aborted open leaks
                            //    nothing;
                            //  - longer than the drain deadline, so the
                            //    reader arrives when nobody is holding
                            //    the future any more;
                            //  - the interrupt flag CLEARED before the
                            //    real open, because that is what an
                            //    object-store client does with one, and
                            //    without it the open fails and parquet
                            //    closes its own stream — which is
                            //    exactly why the first version of this
                            //    test could not tell the two unwind
                            //    strategies apart.
                            if (i >= 3) {
                                val until =
                                    System.nanoTime() +
                                        (ParquetRewriter.OPEN_DRAIN_MILLIS + 500) * 1_000_000L
                                while (System.nanoTime() < until) { // busy
                                    Thread.onSpinWait()
                                }
                                Thread.interrupted()
                            }
                            val stream = delegate.newStream()
                            open.incrementAndGet()
                            opened.incrementAndGet()
                            return object : org.apache.parquet.io.DelegatingSeekableInputStream(stream) {
                                override fun getPos(): Long = stream.pos

                                override fun seek(newPos: Long) = stream.seek(newPos)

                                override fun close() {
                                    open.decrementAndGet()
                                    stream.close()
                                }
                            }
                        }
                    }
                // Input 2 claims explicit row ids it does not carry, so
                // the rewrite refuses it — mid-group, with the window
                // full of readers opened ahead of it.
                ParquetRewriter.Input(counted, f.path, f.rowIdStart, explicitRowIds = i == 2)
            }
        val (catalogId, tableId) = ids(fx.cat)
        val columns =
            db.jdbi.withHandleUnchecked { h ->
                val c = com.posthog.hoglake.persistence.CatalogRepo.findByName(h, fx.cat)!!
                com.posthog.hoglake.persistence.TableRepo.columnsAt(h, catalogId, tableId, c.headSnapshotId)
            }
        org.assertj.core.api.Assertions.assertThatThrownBy {
            ParquetRewriter.rewrite(
                inputs,
                columns,
                emptyList(),
                S3OutputFile(store, "s3://$BUCKET/${fx.cat}/leak-${UUID.randomUUID()}.parquet"),
                inputOpenParallelism = 4,
            )
        }.isInstanceOf(InvalidDataException::class.java)
        // SETTLE FIRST, and settle on the CONDITION rather than on
        // stability. A task abandoned mid-`open` keeps running and
        // finishes after the unwind has given up on it, so reading the
        // counters the instant the exception lands measures a race; but
        // waiting for the count to stop moving exits after one poll,
        // before the slow opens have finished at all. So: wait until the
        // late readers have arrived AND everything is closed, or give up
        // — and the give-up path is what the leak looks like.
        val deadline = System.nanoTime() + 8_000_000_000L
        while (System.nanoTime() < deadline && (opened.get() <= 3 || open.get() != 0)) {
            Thread.sleep(200)
        }
        assertThat(open.get())
            .describedAs("%d readers were opened and %d left open", opened.get(), open.get())
            .isZero()
        assertThat(opened.get())
            .describedAs("the window must have opened readers ahead of the one that failed")
            .isGreaterThan(3)
    }

    // ---- helpers for the above ----------------------------------------------

    private fun deleteObject(pathUri: String) {
        val loc = ObjectStore.parse(pathUri)
        minioClient().deleteObject(
            software.amazon.awssdk.services.s3.model.DeleteObjectRequest.builder()
                .bucket(loc.bucket).key(loc.key).build(),
        )
    }

    private fun minioClient(): software.amazon.awssdk.services.s3.S3Client =
        software.amazon.awssdk.services.s3.S3Client.builder()
            .region(software.amazon.awssdk.regions.Region.of("us-east-1"))
            .endpointOverride(java.net.URI.create(minio.s3URL))
            .credentialsProvider(
                software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                    software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(
                        minio.userName,
                        minio.password,
                    ),
                ),
            )
            .forcePathStyle(true)
            .build()

    /** Seconds left on each of the catalog's claims. */
    private fun claimLeasesSeconds(cat: String): List<Long> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT EXTRACT(EPOCH FROM (cc.expires_at - now()))::bigint AS left_seconds
                FROM hog_compaction_claim cc
                JOIN hog_catalog c ON c.catalog_id = cc.catalog_id
                WHERE c.name = :n
                """,
            ).bind("n", cat).mapTo(Long::class.javaObjectType).list()
        }

    /** Every compaction_staging ticket's drained_outcome (null = undrained). */
    private fun stagingTicketOutcomes(cat: String): List<String?> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT r.drained_outcome FROM hog_file_removal r
                JOIN hog_catalog c ON c.catalog_id = r.catalog_id
                WHERE c.name = :n AND r.reason = 'compaction_staging'
                """,
            ).bind("n", cat).map { rs, _ -> rs.getString("drained_outcome") }.list()
        }

    private fun ids(cat: String): Pair<Long, Long> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT c.catalog_id, tv.table_id
                FROM hog_catalog c
                JOIN hog_table_version tv ON tv.catalog_id = c.catalog_id AND tv.end_snapshot IS NULL
                WHERE c.name = :n
                """,
            )
                .bind("n", cat)
                .map { rs, _ -> rs.getLong("catalog_id") to rs.getLong("table_id") }
                .one()
        }

    private fun expireAllClaims(cat: String) =
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                "UPDATE hog_compaction_claim SET expires_at = now() - interval '1 second' " +
                    "WHERE catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = :n)",
            ).bind("n", cat).execute()
        }

    private fun claimRows(cat: String): List<String> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT cc.group_key FROM hog_compaction_claim cc
                JOIN hog_catalog c ON c.catalog_id = cc.catalog_id
                WHERE c.name = :n
                """,
            )
                .bind("n", cat)
                .mapTo(String::class.java)
                .list()
        }
}
