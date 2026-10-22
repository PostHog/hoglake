package com.posthog.hoglake.compaction

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.CompactionResult
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.MaintenanceSummarySampler
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
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.MinIOContainer
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Bucket claims: N planners on one catalog probe disjoint buckets.
 *
 * gigahog-prod-us, 2026-10-21, after the maintenance Deployment went
 * from 2 to 8 replicas: six millpond-prod-us sweeps started within two
 * seconds with identical `buckets_considered 123 / candidates_fetched
 * 50,029` and `claimed_elsewhere` 0, 64, 128, 192, 256, 320 — each pod,
 * in turn under the catalog planning lock, probed the same buckets and
 * discarded its predecessors' groups only AFTER reading their candidate
 * rows. The group claims made execution disjoint; planning was repeated
 * nine times. `CompactionService.planAndClaim` now also claims every
 * bucket whose groups it claimed in full, and `fetchCandidates` skips a
 * live bucket claim before probing.
 *
 * Two `CompactionService` instances with different planner identities
 * stand in for two pods. The first is held inside its first rewrite
 * (the `beforeRewrite` seam) with its whole queue claimed, which is the
 * window in which a sibling's plan used to re-probe everything.
 *
 * Real parquet in MinIO, because the claims are released by a group
 * SETTLING, and only a real rewrite settles one.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CompactionBucketClaimIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val alter = AlterService(db.jdbi)
    private val counter = AtomicInteger(0)

    private val cfg =
        CompactionConfig(targetBytes = 1L shl 20, minInputFiles = 2, maxGroupsPerRun = 10, parallelGroups = 1)

    private companion object {
        const val BUCKET = "hoglake-bucket-claims-test"
        val DAYS = listOf("2026-10-01", "2026-10-02", "2026-10-03", "2026-10-04")

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

    private val schema: MessageType =
        Types.buildMessage()
            .addField(Types.required(PrimitiveTypeName.INT64).id(1).named("id"))
            .addField(
                Types.optional(PrimitiveTypeName.BINARY)
                    .`as`(LogicalTypeAnnotation.stringType()).id(2).named("day"),
            )
            .named("t")

    private fun parquet(
        ids: LongRange,
        day: String,
    ): ByteArray {
        val tmp = Files.createTempFile("bucket-claims", ".parquet")
        try {
            Files.delete(tmp)
            val factory = SimpleGroupFactory(schema)
            ExampleParquetWriter.builder(LocalOutputFile(tmp))
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
                .use { w ->
                    for (id in ids) w.write(factory.newGroup().append("id", id).append("day", day))
                }
            return Files.readAllBytes(tmp)
        } finally {
            Files.deleteIfExists(tmp)
        }
    }

    private fun head(cat: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT last_snapshot_id FROM hog_catalog WHERE name = :n")
                .bind("n", cat).mapTo(Long::class.java).one()
        }

    /**
     * [count] files of [rows] rows each in partition [day], one commit.
     * Every file of a day holds the SAME rows, so the files are the same
     * size and the candidate fetch's (size, id) order is id order — which
     * is what lets a test say which files a cut plan saw.
     */
    private fun append(
        cat: String,
        day: String,
        count: Int,
        rows: Int = 3,
    ) {
        val regs =
            (0 until count).map { i ->
                val path = "s3://$BUCKET/$cat/$day-$i.parquet"
                val bytes = parquet(0L until rows.toLong(), day)
                store.put(path, bytes)
                FileRegistration(path, rows.toLong(), bytes.size.toLong(), partitionValues = listOf(day))
            }
        commits.commit(
            cat,
            CommitRequest(readSnapshot = head(cat), appends = listOf(TableAppend("ns", "t", regs))),
        )
    }

    /**
     * A day-partitioned table with [filesPerDay] three-row files in each
     * of [days] — or the files [layout] names, `day -> (files, rows per
     * file)` — and a published sample.
     */
    private fun fixture(
        days: List<String> = DAYS,
        filesPerDay: Int = 3,
        layout: Map<String, Pair<Int, Int>> = days.associateWith { filesPerDay to 3 },
    ): String {
        val cat = "bucket-claims-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG), ColumnDef("day", ColType.STRING)))
        alter.alterTable(
            cat,
            "ns",
            "t",
            listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(2, Transform.IDENTITY)))),
        )
        for ((day, shape) in layout) append(cat, day, shape.first, shape.second)
        val sampler = MaintenanceSummarySampler(db.jdbi, cfg.targetBytes, cfg.minInputFiles, cfg.maxInputFiles, 3600)
        var steps = 0
        while (sampler.runOnce(10_000)) check(++steps < 1_000)
        return cat
    }

    private fun ids(cat: String): Pair<Long, Long> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT DISTINCT c.catalog_id, f.table_id FROM hog_catalog c
                JOIN hog_data_file f ON f.catalog_id = c.catalog_id
                WHERE c.name = :n
                """,
            ).bind("n", cat).map { rs, _ -> rs.getLong(1) to rs.getLong(2) }.one()
        }

    /** The live bucket-claim rows of [cat], by key. */
    private fun bucketClaims(cat: String): List<String> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT cc.group_key FROM hog_compaction_claim cc
                JOIN hog_catalog c ON c.catalog_id = cc.catalog_id
                WHERE c.name = :n AND starts_with(cc.group_key, 'bucket:') AND cc.expires_at > now()
                """,
            ).bind("n", cat).mapTo(String::class.java).list()
        }

    private fun liveByDay(cat: String): Map<String, Int> =
        catalogs.listFiles(cat, "ns", "t")
            .groupingBy { f -> f.partitionValues!!.single()!! }
            .eachCount()

    /** The table's one partition spec id, as the planner's bucket carries it. */
    private fun specId(cat: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT DISTINCT f.spec_id FROM hog_data_file f
                JOIN hog_catalog c ON c.catalog_id = f.catalog_id
                WHERE c.name = :n
                """,
            ).bind("n", cat).mapTo(Long::class.java).one()
        }

    private fun bucketKey(
        cat: String,
        day: String,
    ) = CompactionClaimRepo.bucketKey(specId(cat), listOf(day))

    /**
     * Run [first] on another thread and hold it inside its first rewrite,
     * so its plan's claims are all live; run [during] in that window, then
     * let [first] finish.
     */
    private fun <T> whileHeld(
        first: CompactionService,
        cat: String,
        firstCfg: CompactionConfig,
        during: () -> T,
    ): Pair<CompactionResult, T> {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        first.beforeRewrite = {
            entered.countDown()
            check(release.await(60, TimeUnit.SECONDS))
        }
        val pool = Executors.newSingleThreadExecutor()
        try {
            val run = pool.submit<CompactionResult> { first.runOnce(cat, firstCfg) }
            assertThat(entered.await(60, TimeUnit.SECONDS)).describedAs("the first run reached a rewrite").isTrue()
            val seen = during()
            release.countDown()
            return run.get(120, TimeUnit.SECONDS) to seen
        } finally {
            release.countDown()
            pool.shutdownNow()
        }
    }

    @Test
    fun `a second planner skips every bucket the first probed and claimed, before probing it`() {
        val cat = fixture()
        val podA = CompactionService(db.jdbi, store, cfg, plannerIdentity = "maintenance-a")
        val podB = CompactionService(db.jdbi, store, cfg, plannerIdentity = "maintenance-b")

        val (first, second) =
            whileHeld(podA, cat, cfg) {
                // Every bucket packed into one group and the first run
                // claimed all four, so all four carry a bucket claim.
                assertThat(bucketClaims(cat))
                    .containsExactlyInAnyOrderElementsOf(DAYS.map { bucketKey(cat, it) })
                podB.runOnce(cat, cfg)
            }

        // MUTATION: drop the pre-probe filter's `continue` in
        // `fetchCandidates` (or never call `withBucketClaims`) and this
        // reds the way prod-us did: the second plan probes all four
        // buckets (`bucketsConsidered` 4), reads their twelve candidate
        // rows, and drops the first run's four groups after the probe
        // (`claimedElsewhere` 4, `bucketsSkippedClaimed` 0).
        assertThat(second.bucketsSkippedClaimed).isEqualTo(DAYS.size.toLong())
        assertThat(second.claimedElsewhere).isZero()
        assertThat(second.bucketsConsidered).isZero()
        assertThat(second.candidatesFetched).isZero()
        assertThat(second.groupsCompacted).isZero()

        assertThat(first.groupsCompacted).isEqualTo(DAYS.size.toLong())
        assertThat(first.failedGroups).isZero()
        assertThat(first.bucketsSkippedClaimed).isZero()
        assertThat(liveByDay(cat)).isEqualTo(DAYS.associateWith { 1 })
        // MUTATION: drop `settleBucketClaim(item)` from `executeGroup`'s
        // finally and the four markers outlive the sweep that held them,
        // for a full claim lease.
        assertThat(bucketClaims(cat)).describedAs("released as each bucket's last group settled").isEmpty()
    }

    @Test
    fun `a released bucket claim is probed again`() {
        val cat = fixture()
        val podA = CompactionService(db.jdbi, store, cfg, plannerIdentity = "maintenance-a")
        val podB = CompactionService(db.jdbi, store, cfg, plannerIdentity = "maintenance-b")
        assertThat(podA.runOnce(cat, cfg).groupsCompacted).isEqualTo(DAYS.size.toLong())

        // The sample is a generation old, so it still offers all four
        // buckets; each holds one output now and packs into nothing. What
        // this pins is that the second planner PROBES them rather than
        // skipping on a claim the first run settled.
        val plan = podB.planTable(cat, "ns", "t", cfg)
        assertThat(plan.bucketsSkippedClaimed).isZero()
        assertThat(plan.bucketsConsidered).isEqualTo(DAYS.size.toLong())
        assertThat(plan.groups).isEmpty()
    }

    @Test
    fun `a live bucket claim is skipped and an expired one is probed`() {
        val cat = fixture(days = listOf("2026-10-01"))
        val (catalogId, tableId) = ids(cat)
        val svc = CompactionService(db.jdbi, store, cfg, plannerIdentity = "maintenance-a")
        val other = UUID.randomUUID()
        db.jdbi.useHandleUnchecked { h ->
            assertThat(
                CompactionClaimRepo.acquire(
                    h,
                    catalogId,
                    tableId,
                    bucketKey(cat, "2026-10-01"),
                    emptyList(),
                    other,
                    3600,
                ),
            ).isTrue()
        }
        val held = svc.planTable(cat, "ns", "t", cfg)
        assertThat(held.bucketsSkippedClaimed).isEqualTo(1)
        assertThat(held.bucketsConsidered).isZero()
        assertThat(held.groups).isEmpty()

        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_compaction_claim SET expires_at = now() - interval '1 second' " +
                    "WHERE catalog_id = ? AND table_id = ?",
                catalogId,
                tableId,
            )
        }
        // MUTATION: drop `AND expires_at > now()` from
        // `CompactionClaimRepo.LIVE_BUCKET_CLAIMS_SQL` and an expired
        // marker — a pod killed mid-sweep — hides its bucket until the
        // next sweep's purge, where it must hide it for no time at all.
        val expired = svc.planTable(cat, "ns", "t", cfg)
        assertThat(expired.bucketsSkippedClaimed).isZero()
        assertThat(expired.bucketsConsidered).isEqualTo(1)
        assertThat(expired.groups).hasSize(1)

        // And claims off means no filter: the same live marker is
        // ignored, as `liveBucketClaims` documents.
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_compaction_claim SET expires_at = now() + interval '1 hour' " +
                    "WHERE catalog_id = ? AND table_id = ?",
                catalogId,
                tableId,
            )
        }
        val off = svc.planTable(cat, "ns", "t", cfg.copy(claimsEnabled = false))
        assertThat(off.bucketsSkippedClaimed).isZero()
        assertThat(off.groups).hasSize(1)
    }

    @Test
    fun `a bucket with a group left unclaimed is not hidden from the sibling`() {
        // Two groups in ONE bucket (fan-in pinned to 3, six files), and a
        // first run with room for one. Its bucket is not fully claimed,
        // so it takes no bucket claim, and the sibling probes the bucket,
        // drops the held group by overlap and compacts the other.
        val cat = fixture(days = listOf("2026-10-01"), filesPerDay = 6)
        val narrow = cfg.copy(maxInputFiles = 3, maxFanIn = 3, minInputFiles = 2)
        val podA = CompactionService(db.jdbi, store, narrow, plannerIdentity = "maintenance-a")
        val podB = CompactionService(db.jdbi, store, narrow, plannerIdentity = "maintenance-b")
        val (first, second) =
            whileHeld(podA, cat, narrow.copy(maxGroupsPerRun = 1)) {
                assertThat(bucketClaims(cat)).isEmpty()
                podB.runOnce(cat, narrow)
            }
        // MUTATION: drop `mine.size != free[bucket]` from
        // `withBucketClaims` and the bucket is claimed after its first
        // group: the sibling skips it (`bucketsSkippedClaimed` 1) and the
        // second group waits a whole sweep for nobody.
        assertThat(second.bucketsSkippedClaimed).isZero()
        assertThat(second.claimedElsewhere).isEqualTo(1)
        assertThat(second.groupsCompacted).isEqualTo(1)
        assertThat(first.groupsCompacted).isEqualTo(1)
        assertThat(liveByDay(cat)).isEqualTo(mapOf("2026-10-01" to 2))
    }

    @Test
    fun `only buckets whose groups were claimed are marked, so the sibling takes the rest`() {
        // Two buckets of one group each, a first run with room for one:
        // the bucket it claimed is marked, the other — probed in full,
        // but its group never claimed — is not.
        val days = listOf("2026-10-01", "2026-10-02")
        val cat = fixture(days = days)
        val podA = CompactionService(db.jdbi, store, cfg, plannerIdentity = "maintenance-a")
        val podB = CompactionService(db.jdbi, store, cfg, plannerIdentity = "maintenance-b")
        val (first, second) =
            whileHeld(podA, cat, cfg.copy(maxGroupsPerRun = 1)) {
                assertThat(bucketClaims(cat)).hasSize(1)
                podB.runOnce(cat, cfg)
            }
        // MUTATION: take a bucket claim for every probed (`markable`)
        // bucket instead of only the buckets whose groups this plan
        // claimed, and the sibling skips both: `bucketsSkippedClaimed` 2,
        // nothing compacted.
        assertThat(second.bucketsSkippedClaimed).isEqualTo(1)
        assertThat(second.claimedElsewhere).isZero()
        assertThat(second.groupsCompacted).isEqualTo(1)
        assertThat(first.groupsCompacted).isEqualTo(1)
        assertThat(liveByDay(cat)).isEqualTo(days.associateWith { 1 })
        assertThat(bucketClaims(cat)).isEmpty()
    }

    @Test
    fun `a bucket the candidate budget cut is not claimed, so the sibling reads past the cut`() {
        // A candidate budget of 4 rows. Bucket `x` (two one-row files, the
        // smaller bytes per file, so first in value order) takes 2 of it;
        // bucket `y` (six files) is probed with room for 2, CUT there, and
        // packs one group of 2 from the rows it saw. The first run claims
        // both groups — every free group of both buckets — but only `x`
        // was probed in full. A claim on `y` would hide four files the
        // first run never read from a sibling that, skipping `x`, comes
        // to `y` with the whole budget.
        val cat = fixture(layout = mapOf("x" to (2 to 1), "y" to (6 to 3)))
        val cut = cfg.copy(maxInputFiles = 2, maxFanIn = 2, candidateHeadroom = 1, maxCandidates = 4)
        // Null identities: both start at the head, so `x` comes first —
        // probed by the first run, skipped by the second before the
        // budget check can end its loop on `y`.
        val podA = CompactionService(db.jdbi, store, cut)
        val podB = CompactionService(db.jdbi, store, cut)
        val (first, second) =
            whileHeld(podA, cat, cut) {
                assertThat(bucketClaims(cat)).containsExactly(bucketKey(cat, "x"))
                podB.runOnce(cat, cut)
            }
        // MUTATION: drop `bucket !in markable` from `withBucketClaims`
        // and `y` is claimed too: the sibling skips both buckets
        // (`bucketsSkippedClaimed` 2) and the files past the cut wait for
        // the first run to settle.
        assertThat(first.candidatesTruncated).isEqualTo(1)
        assertThat(second.bucketsSkippedClaimed).isEqualTo(1)
        assertThat(second.claimedElsewhere).describedAs("y's first group, held by the first run").isEqualTo(1)
        assertThat(second.groupsCompacted).describedAs("y's next two files, past the first run's cut").isEqualTo(1)
        assertThat(first.groupsCompacted).isEqualTo(2)
    }

    /** Seconds until [key]'s claim row expires (negative once it has). */
    private fun leftSeconds(
        cat: String,
        key: String,
    ): Double =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT extract(epoch FROM cc.expires_at - now())::float8 FROM hog_compaction_claim cc
                JOIN hog_catalog c ON c.catalog_id = cc.catalog_id
                WHERE c.name = :n AND cc.group_key = :k
                """,
            ).bind("n", cat).bind("k", key).mapTo(Double::class.java).one()
        }

    /** Every GROUP claim row's seconds left. */
    private fun groupClaimsLeft(cat: String): List<Double> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT extract(epoch FROM cc.expires_at - now())::float8 FROM hog_compaction_claim cc
                JOIN hog_catalog c ON c.catalog_id = cc.catalog_id
                WHERE c.name = :n AND NOT starts_with(cc.group_key, 'bucket:')
                """,
            ).bind("n", cat).mapTo(Double::class.java).list()
        }

    @Test
    fun `a bucket claim whose owner stops refreshing lapses on its short lease while its group claims live on`() {
        // The leak a deploy makes: a pod SIGTERMed mid-sweep is abandoned
        // by BackgroundLoops.close after 5 s and releases nothing. Here
        // the owner is held inside its only group's rewrite, so nothing
        // refreshes the claims after that group's pre-IO refresh — the
        // same state, from the claim table's point of view. TWO buckets:
        // the held group's and one whose group is still QUEUED. The held
        // group's pre-IO refresh renewed both, once, to the short lease;
        // nothing renews them after it.
        val cat = fixture(days = listOf("2026-10-01", "2026-10-02"))
        val short = cfg.copy(bucketClaimTtlSeconds = 2)
        val owner = CompactionService(db.jdbi, store, short, plannerIdentity = "maintenance-a")
        val sibling = CompactionService(db.jdbi, store, short, plannerIdentity = "maintenance-b")
        val (_, plans) =
            whileHeld(owner, cat, short) {
                val held = sibling.planTable(cat, "ns", "t", short)
                Thread.sleep(3_000)
                held to sibling.planTable(cat, "ns", "t", short)
            }
        val (held, lapsed) = plans
        assertThat(held.bucketsSkippedClaimed).isEqualTo(2)
        // MUTATION: renew bucket claims on `cfg.claimTtlSeconds` in
        // `refreshClaim` (the hour-long group lease) and both buckets are
        // still skipped here — hidden for an hour after their owner died.
        // (The lease the claim is TAKEN with is overwritten by the first
        // group's refresh moments later, so it only matters for a pod
        // killed between the claim transaction and that refresh; no test
        // reaches that window.)
        assertThat(lapsed.bucketsSkippedClaimed).describedAs("the marker lapsed after its 2 s lease").isZero()
        assertThat(lapsed.bucketsConsidered).isEqualTo(2)
        // The group claim is still live, so the probe finds the held group
        // by overlap: the pre-marker behaviour, not a duplicate rewrite.
        assertThat(lapsed.claimedGroups).isEqualTo(2)
        assertThat(lapsed.groups).isEmpty()
    }

    /** Seconds left on every bucket-claim row of [cat], expired ones included. */
    private fun bucketClaimsLeft(cat: String): List<Double> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT extract(epoch FROM cc.expires_at - now())::float8 FROM hog_compaction_claim cc
                JOIN hog_catalog c ON c.catalog_id = cc.catalog_id
                WHERE c.name = :n AND starts_with(cc.group_key, 'bucket:')
                """,
            ).bind("n", cat).mapTo(Double::class.java).list()
        }

    @Test
    fun `every group's pre-IO refresh renews all the plan's outstanding bucket claims, queued ones included`() {
        // Three buckets of one group each and a 6 s bucket lease. The
        // first group's rewrite is held 3.5 s, so the SECOND group's
        // refresh runs ~3.5 s after the claims were taken; the claims are
        // read ~7 s in, inside the second rewrite — past the unrenewed
        // expiry (~6 s), inside the renewed one (~9.5 s). The first
        // bucket's claim is gone (its only group settled); the second's
        // is renewed by its own group's refresh; the THIRD's group is
        // still queued, so only the second group's refresh renewing
        // every outstanding claim of the plan keeps it.
        val cat = fixture(days = listOf("2026-10-01", "2026-10-02", "2026-10-03"), filesPerDay = 2)
        val short = cfg.copy(bucketClaimTtlSeconds = 6)
        val owner = CompactionService(db.jdbi, store, short, plannerIdentity = "maintenance-a")
        val rewrites = AtomicInteger(0)
        val inSecond = CountDownLatch(1)
        val release = CountDownLatch(1)
        owner.beforeRewrite = {
            when (rewrites.incrementAndGet()) {
                1 -> Thread.sleep(3_500)
                2 -> {
                    inSecond.countDown()
                    check(release.await(60, TimeUnit.SECONDS))
                }
                else -> {}
            }
        }
        val pool = Executors.newSingleThreadExecutor()
        try {
            val run = pool.submit<CompactionResult> { owner.runOnce(cat, short) }
            assertThat(inSecond.await(60, TimeUnit.SECONDS)).isTrue()
            Thread.sleep(3_500)
            // MUTATION: renew only the group's own bucket (pass
            // `listOfNotNull(item.bucketClaim?.key)` to `refreshClaim`) and
            // the queued bucket's claim expired ~1 s ago, mid-sweep; pass
            // no keys at all and both have.
            val left = bucketClaimsLeft(cat)
            assertThat(left).describedAs("the in-flight bucket's and the queued bucket's").hasSize(2)
            assertThat(left)
                .describedAs("all renewed by the second group's refresh: %s", left)
                .allMatch { it > 0.0 && it <= 6.0 }
            // The in-flight group's claim is on the hour; the first,
            // committed, group's is on the committed lease.
            assertThat(groupClaimsLeft(cat).max()).isGreaterThan(3_000.0)
            release.countDown()
            assertThat(run.get(120, TimeUnit.SECONDS).groupsCompacted).isEqualTo(3)
        } finally {
            release.countDown()
            pool.shutdownNow()
        }
        assertThat(bucketClaims(cat)).isEmpty()
    }

    @Test
    fun `the refresh renews our live bucket claim, never revives an expired one, and answers for the group`() {
        val cat = fixture(days = listOf("2026-10-01"))
        val (catalogId, tableId) = ids(cat)
        val me = UUID.randomUUID()
        val bucket = bucketKey(cat, "2026-10-01")
        db.jdbi.useHandleUnchecked { h ->
            assertThat(CompactionClaimRepo.acquire(h, catalogId, tableId, "g", listOf(1L), me, 5)).isTrue()
            assertThat(CompactionClaimRepo.acquire(h, catalogId, tableId, bucket, emptyList(), me, 5)).isTrue()
            assertThat(
                CompactionClaimRepo.refresh(
                    h,
                    catalogId,
                    tableId,
                    "g",
                    me,
                    3600,
                    bucketKeys = listOf(bucket),
                    bucketTtlSeconds = 600,
                ),
            ).isTrue()
        }
        assertThat(leftSeconds(cat, "g")).isGreaterThan(3_500.0)
        assertThat(leftSeconds(cat, bucket)).describedAs("its own lease, not the group's").isBetween(500.0, 600.0)

        // Expired: a sibling may already have planned the bucket as free.
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_compaction_claim SET expires_at = now() - interval '1 second' WHERE group_key = ?",
                bucket,
            )
            assertThat(
                CompactionClaimRepo.refresh(
                    h,
                    catalogId,
                    tableId,
                    "g",
                    me,
                    3600,
                    bucketKeys = listOf(bucket),
                    bucketTtlSeconds = 600,
                ),
            ).describedAs("the group's answer, unaffected by the bucket row").isTrue()
        }
        // MUTATION: drop `AND expires_at > now()` from the refresh and an
        // expired bucket claim comes back to life over a sibling's plan.
        assertThat(leftSeconds(cat, bucket)).isLessThan(0.0)

        // A LOST group claim — expired, then taken over by another run —
        // answers false, and does not renew the live bucket claim beside
        // it: a run that lost its lease is not working this queue.
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_compaction_claim SET expires_at = now() + interval '60 seconds' WHERE group_key = ?",
                bucket,
            )
            h.execute("UPDATE hog_compaction_claim SET expires_at = now() - interval '1 second' WHERE group_key = 'g'")
            assertThat(
                CompactionClaimRepo.refresh(
                    h,
                    catalogId,
                    tableId,
                    "g",
                    me,
                    3600,
                    bucketKeys = listOf(bucket),
                    bucketTtlSeconds = 600,
                ),
            ).describedAs("expired group row").isFalse()
            assertThat(CompactionClaimRepo.acquire(h, catalogId, tableId, "g", listOf(1L), UUID.randomUUID(), 3600))
                .describedAs("taken over by another run")
                .isTrue()
        }
        // MUTATION: drop the `OR EXISTS (... g.group_key = :groupKey ...)`
        // gate from the refresh and the bucket claim is renewed to 600 s
        // by a run whose group lease is already gone.
        assertThat(leftSeconds(cat, bucket)).describedAs("not renewed by the lost group").isLessThanOrEqualTo(60.0)
        db.jdbi.useHandleUnchecked { h ->
            assertThat(
                CompactionClaimRepo.refresh(
                    h,
                    catalogId,
                    tableId,
                    "g",
                    me,
                    3600,
                    bucketKeys = listOf(bucket),
                    bucketTtlSeconds = 600,
                ),
            ).isFalse()
            h.execute("DELETE FROM hog_compaction_claim WHERE catalog_id = ?", catalogId)
        }
    }
}
