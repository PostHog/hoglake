package com.posthog.hoglake.compaction

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
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
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.MinIOContainer
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

/**
 * The planner sees buckets written AFTER the published generation.
 *
 * gigahog-prod-us, 2026-10-21: the maintenance sampler had not published
 * for millpond-prod-us since 2026-10-16, and the planner picks buckets
 * only from the published generation, so on `events_raw` it saw the
 * 1,794 buckets of that generation while the table had 5,036
 * partitions — every day partition written in five days, the hottest,
 * invisible to compaction. Re-anchoring (`MaintenanceSummarySampler`)
 * makes the sampler publish again; it does not make a generation name a
 * partition that did not exist when its scan began. So
 * `CompactionService.newBuckets` reads the buckets of the live files
 * appended since the sample's start snapshot and merges them in.
 *
 * Real parquet in MinIO, because the claim is that a sweep COMPACTS the
 * new bucket, not only that a plan names it.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CompactionNewBucketsIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val alter = AlterService(db.jdbi)
    private val counter = AtomicInteger(0)

    private val cfg = CompactionConfig(targetBytes = 1L shl 20, minInputFiles = 2, maxGroupsPerRun = 10)

    private companion object {
        const val BUCKET = "hoglake-new-buckets-test"

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

    private val svc by lazy { CompactionService(db.jdbi, store, cfg) }

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
        val tmp = Files.createTempFile("new-buckets", ".parquet")
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

    private fun fixture(): String {
        val cat = "new-buckets-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG), ColumnDef("day", ColType.STRING)))
        alter.alterTable(
            cat,
            "ns",
            "t",
            listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(2, Transform.IDENTITY)))),
        )
        return cat
    }

    private fun head(cat: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT last_snapshot_id FROM hog_catalog WHERE name = :n")
                .bind("n", cat).mapTo(Long::class.java).one()
        }

    /** [count] three-row files in partition [day], one commit. */
    private fun append(
        cat: String,
        day: String,
        count: Int,
    ) {
        val regs =
            (0 until count).map { i ->
                val path = "s3://$BUCKET/$cat/$day-$i.parquet"
                val bytes = parquet((i * 3L) until (i * 3L + 3), day)
                store.put(path, bytes)
                FileRegistration(path, 3, bytes.size.toLong(), partitionValues = listOf(day))
            }
        commits.commit(
            cat,
            CommitRequest(readSnapshot = head(cat), appends = listOf(TableAppend("ns", "t", regs))),
        )
    }

    /**
     * [count] files of a declared [bytes] each in partition [day], with
     * NO object behind them: for the planning-only test, where the size
     * has to be chosen to fix the bucket order and nothing is rewritten.
     */
    private fun appendMetadataOnly(
        cat: String,
        day: String,
        count: Int,
        bytes: Long,
    ) {
        val regs =
            (0 until count).map { i ->
                FileRegistration("s3://$BUCKET/$cat/meta-$day-$i.parquet", 3, bytes, partitionValues = listOf(day))
            }
        commits.commit(
            cat,
            CommitRequest(readSnapshot = head(cat), appends = listOf(TableAppend("ns", "t", regs))),
        )
    }

    private fun publishSample() {
        val sampler =
            MaintenanceSummarySampler(db.jdbi, cfg.targetBytes, cfg.minInputFiles, cfg.maxInputFiles, 3600)
        var steps = 0
        while (sampler.runOnce(10_000)) check(++steps < 1_000)
    }

    /** The partition values of the table's live files, with their counts. */
    private fun liveByDay(cat: String): Map<String, Int> =
        catalogs.listFiles(cat, "ns", "t")
            .groupingBy { f -> f.partitionValues!!.single()!! }
            .eachCount()

    @Test
    fun `a partition first written after the published generation is planned and compacted`() {
        val cat = fixture()
        append(cat, "2026-10-16", 3)
        publishSample()
        // The generation knows one bucket...
        val sampled =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT p.partition_values[1] FROM hog_maintenance_summary_tier p
                    JOIN hog_maintenance_summary s
                      ON s.catalog_id = p.catalog_id AND s.published_generation = p.generation
                    JOIN hog_catalog c ON c.catalog_id = p.catalog_id
                    WHERE c.name = :n
                    """,
                ).bind("n", cat).mapTo(String::class.java).list()
            }
        assertThat(sampled).containsExactly("2026-10-16")

        // ...and a NEW tuple lands after it, with nothing republished.
        append(cat, "2026-10-21", 4)

        val plan = svc.planTable(cat, "ns", "t", cfg)
        // MUTATION: drop the `newBuckets` union from `fetchCandidates`
        // (pass `emptyList()` to `mergeBuckets`) and this reds: the plan
        // has the 10-16 group only, `bucketsAvailable` is 1, and the
        // 10-21 files are never planned until a generation that began
        // after them publishes — the prod-us state.
        assertThat(plan.groups.map { it.partitionValues })
            .containsExactlyInAnyOrder(listOf("2026-10-16"), listOf("2026-10-21"))
        assertThat(plan.groups.single { it.partitionValues == listOf("2026-10-21") }.files).hasSize(4)
        assertThat(plan.bucketsAvailable).isEqualTo(2L)

        // And a sweep compacts it.
        val result = svc.runOnce(cat, cfg)
        assertThat(result.failedGroups).isZero()
        assertThat(result.groupsCompacted).isEqualTo(2)
        assertThat(liveByDay(cat)).isEqualTo(mapOf("2026-10-16" to 1, "2026-10-21" to 1))
        val output = catalogs.listFiles(cat, "ns", "t").single { it.partitionValues == listOf("2026-10-21") }
        assertThat(output.explicitRowIds).isTrue()
        assertThat(output.recordCount).isEqualTo(12L)
    }

    @Test
    fun `new buckets alone are enough when the generation credits nothing`() {
        val cat = fixture()
        // One file: a published generation with NO qualifying bucket for
        // the table (selected < 2), which used to mean an empty fetch.
        append(cat, "2026-10-16", 1)
        publishSample()
        append(cat, "2026-10-21", 3)
        // MUTATION: test `sampledBuckets(...).isEmpty()` before the merge
        // (the old early return) and this plans nothing.
        val plan = svc.planTable(cat, "ns", "t", cfg)
        assertThat(plan.groups.map { it.partitionValues }).containsExactly(listOf("2026-10-21"))
        assertThat(plan.groups.single().files).hasSize(3)
    }

    @Test
    fun `appends to a bucket the sample already knows merge into ONE bucket`() {
        val cat = fixture()
        append(cat, "2026-10-16", 2)
        publishSample()
        // Two more in the SAME tuple after the snapshot: the new-bucket
        // read returns it too (two files clear its floor of two).
        append(cat, "2026-10-16", 2)
        val plan = svc.planTable(cat, "ns", "t", cfg)
        // MUTATION: replace the `merge` in `mergeBuckets` with a plain
        // concatenation and the tuple is offered twice: bucketsAvailable
        // reads 2, and the second probe re-reads the same files.
        assertThat(plan.bucketsAvailable).isEqualTo(1L)
        assertThat(plan.groups.single().files).hasSize(4)
    }

    /**
     * A catalog whose published generation credits the table with NO
     * bucket: one file in one tuple, `selected` 0. So whatever the plan
     * offers comes from the new-bucket read alone, and "nothing offered"
     * is the published-generation-with-no-work path, `empty()`.
     */
    private fun fixtureWithEmptySample(): String {
        val cat = fixture()
        appendMetadataOnly(cat, "2026-10-16", 1, 100)
        publishSample()
        return cat
    }

    @Test
    fun `files appended since the snapshot and ENDED since are not a new bucket`() {
        val cat = fixtureWithEmptySample()
        // A tuple that appeared after the snapshot and whose files have
        // all ended since — compacted elsewhere, or replaced. They are in
        // the begin_snapshot range, so only the liveness predicate keeps
        // them out.
        appendMetadataOnly(cat, "2026-10-21", 3, 100)
        catalogs.createNamespace(cat, "advance")
        db.jdbi.withHandleUnchecked { h ->
            h.createUpdate(
                """
                UPDATE hog_data_file f SET end_snapshot = c.last_snapshot_id
                  FROM hog_catalog c
                 WHERE c.name = :n AND f.catalog_id = c.catalog_id AND f.path LIKE '%/meta-2026-10-21-%'
                """,
            ).bind("n", cat).execute()
        }
        // MUTATION: drop `AND f.end_snapshot IS NULL` from NEW_BUCKETS_SQL
        // and the dead tuple is offered: bucketsAvailable reads 1, and
        // every sweep spends a probe on a bucket with no live file.
        val plan = svc.planTable(cat, "ns", "t", cfg)
        assertThat(plan.bucketsAvailable).isZero()
        assertThat(plan.bucketsConsidered).isZero()
        assertThat(plan.groups).isEmpty()
    }

    @Test
    fun `a new tuple of ONE file is not offered, and no work is still no work`() {
        val cat = fixtureWithEmptySample()
        appendMetadataOnly(cat, "2026-10-21", 1, 100)
        // One file is a copy, not a compaction: under the packer's floor
        // of two it can never form a group, so offering it is a probe per
        // sweep for nothing.
        // MUTATION: drop `HAVING count(*) >= :minFiles` from
        // NEW_BUCKETS_SQL and this reds: the tuple is offered
        // (bucketsAvailable 1) and probed (bucketsConsidered 1), where
        // the published-generation-with-no-work path returns `empty()`.
        val plan = svc.planTable(cat, "ns", "t", cfg)
        assertThat(plan.bucketsAvailable).isZero()
        assertThat(plan.bucketsConsidered).isZero()
        assertThat(plan.candidatesFetched).isZero()
        assertThat(plan.groups).isEmpty()
    }

    @Test
    fun `a FULL sample plus new buckets loses no sampled bucket`() {
        // A few probes a plan, so 50,000 buckets cost a plan of tens of
        // statements, not thousands: candidateBudget = min(2 x 1 x 64,
        // 50,000) = 128, bucketLimit = 128 / 2 = 64.
        val few = cfg.copy(maxGroupsPerRun = 1, maxFanIn = 64)
        val cat = fixture()
        appendMetadataOnly(cat, "s-real", 2, 400)
        publishSample()
        // Fill the PUBLISHED generation to the sample cap with synthetic
        // tier rows of larger files (bytes per file 400 KiB), so every
        // one of them sorts BEHIND the new buckets below. They have no
        // files, which costs a probe each and changes nothing here.
        val sampleCap = CompactionService.MAX_SAMPLED_BUCKETS
        db.jdbi.withHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_maintenance_summary_tier
                    (catalog_id, generation, bucket_key, table_id, spec_id, partition_values, quota, remaining,
                     pending, selected, pending_max_bytes, file_count, small_count, total_bytes, small_bytes,
                     dv_count, record_count, newest_begin_snapshot)
                SELECT p.catalog_id, p.generation, 'synthetic-' || g, p.table_id, p.spec_id,
                       ARRAY['s-' || lpad(g::text, 6, '0')], p.quota, p.remaining,
                       0, 2, 0, 2, 2, 819200, 819200, 0, 6, p.newest_begin_snapshot
                  FROM hog_maintenance_summary_tier p
                  JOIN hog_maintenance_summary s
                    ON s.catalog_id = p.catalog_id AND s.published_generation = p.generation
                  JOIN hog_catalog c ON c.catalog_id = p.catalog_id
                 CROSS JOIN generate_series(1, :n) g
                 WHERE c.name = :cat
                """,
            ).bind("cat", cat).bind("n", sampleCap - 1).execute()
        }
        // Three NEW tuples of tiny files: they sort AHEAD of all of it.
        for (day in listOf("n-1", "n-2", "n-3")) appendMetadataOnly(cat, day, 2, 100)

        // MUTATION: put back `.take(MAX_SAMPLED_BUCKETS)` on the merged
        // list in `mergeBuckets` and this reads 50,000: the three new
        // buckets push the three value-last SAMPLED buckets off the
        // list, where the rotation can never reach them.
        val plan = CompactionService(db.jdbi, store, few).planTable(cat, "ns", "t", few)
        assertThat(plan.bucketsAvailable).isEqualTo(sampleCap + 3L)
        // And the plan groups exactly the buckets with real files: the
        // three new ones (100 B/file, first in value order) and the one
        // real SAMPLED bucket (400 B/file, next), ahead of the synthetic
        // 400 KiB/file rows.
        assertThat(plan.groups.map { it.partitionValues!!.single() })
            .containsExactlyInAnyOrder("n-1", "n-2", "n-3", "s-real")
    }

    @Test
    fun `a window of zero turns the read off and the planner sees the sample alone`() {
        val cat = fixture()
        appendMetadataOnly(cat, "2026-10-16", 2, 100)
        publishSample()
        appendMetadataOnly(cat, "2026-10-21", 2, 100)
        val off = cfg.copy(newBucketWindow = 0)
        // This pins the ANSWER, the pre-change planner exactly. The
        // `newBucketWindow == 0` early return only saves the statement:
        // without it `LIMIT 0` answers the same, so it is not pinned here.
        val plan = CompactionService(db.jdbi, store, off).planTable(cat, "ns", "t", off)
        assertThat(plan.bucketsAvailable).isEqualTo(1L)
        assertThat(plan.groups.map { it.partitionValues }).containsExactly(listOf("2026-10-16"))
        // And on: both.
        assertThat(svc.planTable(cat, "ns", "t", cfg).bucketsAvailable).isEqualTo(2L)
    }

    @Test
    fun `a sweep whose last bucket has gone resumes where it sorted, not at the top`() {
        // ONE BUCKET PER PLAN: candidateBudget = min(1 x 1 x 2, 2) = 2,
        // so the bucket loop considers max(1, 2 / 2) = 1 bucket a sweep,
        // and the rotation cursor is the whole of which bucket that is.
        val oneBucket =
            CompactionConfig(
                targetBytes = 1L shl 20,
                minInputFiles = 2,
                maxInputFiles = 2,
                maxFanIn = 2,
                maxGroupsPerRun = 1,
                candidateHeadroom = 1,
                maxCandidates = 2,
            )
        val planner = CompactionService(db.jdbi, store, oneBucket)
        val cat = fixture()
        // Bytes per file fixes the order: a (100) < b (200) < c (300).
        // `a` and `c` are in the sample; `b` arrives after it, so it is
        // a NEW bucket — the kind whose membership changes sweep to sweep.
        appendMetadataOnly(cat, "a", 2, 100)
        appendMetadataOnly(cat, "c", 2, 300)
        publishSample()
        appendMetadataOnly(cat, "b", 2, 200)

        fun planned() = planner.planTable(cat, "ns", "t", oneBucket).groups.single().partitionValues!!.single()
        assertThat(planned()).isEqualTo("a")
        assertThat(planned()).isEqualTo("b")
        // `b` leaves the list (its files end: compacted elsewhere, or out
        // of the window) while it is the cursor. A new head first: a row
        // cannot end at the snapshot it began in.
        catalogs.createNamespace(cat, "advance")
        db.jdbi.withHandleUnchecked { h ->
            h.createUpdate(
                """
                UPDATE hog_data_file f SET end_snapshot = c.last_snapshot_id
                  FROM hog_catalog c
                 WHERE c.name = :n AND f.catalog_id = c.catalog_id AND f.path LIKE '%/meta-b-%'
                """,
            ).bind("n", cat).execute()
        }
        // MUTATION: put back `if (at < 0) return sampled` in `rotated` and
        // this reads "a": the sweep after a transient bucket restarts at
        // the head of the order, which on a churny spec is every sweep —
        // the starvation the cursor exists to prevent.
        assertThat(planned()).describedAs("resumes after b's position, at c").isEqualTo("c")
        // And it still wraps.
        assertThat(planned()).isEqualTo("a")
    }
}
