package com.posthog.hoglake.compaction

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.MaintenanceStatusService
import com.posthog.hoglake.service.MaintenanceSummarySampler
import com.posthog.hoglake.service.PartitionStatsService
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.random.Random

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CompactionDebtIntegrationTest {
    private val db = PgTestSupport.freshDatabase()

    /**
     * Bounds shared by every sampler in this file. They have to agree:
     * a sampler whose bounds differ from the checkpointed scan's
     * restarts rather than resumes, which would make the resumption
     * tests pass for the wrong reason.
     */
    private val minFiles = 3
    private val maxFiles = CompactionGrouping.DEFAULT_MAX_INPUT_FILES

    private val catalogs = CatalogService(db.jdbi)
    private var seq = 0

    @AfterAll
    fun close() = db.close()

    private fun seed(sizes: List<Long>): Pair<String, Long> {
        val name = "summary-${seq++}"
        val cat = catalogs.createCatalog(name, "s3://bucket/$name")
        catalogs.createNamespace(name, "ns")
        catalogs.createTable(name, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        db.jdbi.withHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, table_id, data_file_id, row_id_start, begin_snapshot,
                    path, record_count, file_size_bytes, stats_state)
                SELECT :cat, 1, n, n-1, 2, 's3://bucket/f' || n, 1, size, 'provided'
                FROM unnest(:sizes::bigint[]) WITH ORDINALITY AS f(size,n)
                """,
            ).bind("cat", cat.catalogId).bindArray("sizes", Long::class.javaObjectType, sizes).execute()
        }
        return name to cat.catalogId
    }

    private fun complete(
        policy: CompactionGrouping,
        minInputFiles: Int = CompactionGrouping.DEFAULT_MIN_INPUT_FILES,
        maxInputFiles: Int = CompactionGrouping.DEFAULT_MAX_INPUT_FILES,
        batch: Int = 17,
    ) {
        val sampler =
            MaintenanceSummarySampler(
                db.jdbi,
                policy.targetBytes,
                minInputFiles,
                maxInputFiles,
                refreshSeconds = 3600,
            )
        var steps = 0
        while (sampler.runOnce(batch)) check(++steps < 10_000)
    }

    @Test
    fun `checkpointed sampling matches planner across mixed sizes ratios and page boundaries`() {
        val random = Random(99171)
        repeat(50) {
            val policy = CompactionGrouping.of(random.nextLong(2, 1_000_000))
            val minInputFiles = random.nextInt(2, 6)
            val maxInputFiles = random.nextInt(minInputFiles, minInputFiles + 30)
            // Sizes straddle the target deliberately: a file at or over
            // it is not a compaction candidate, so the sampler has to
            // leave it out of its packing the same way the planner's
            // query does. Sizes drawn only from below the target would
            // never exercise that.
            val sizes = List(random.nextInt(0, 200)) { random.nextLong(0, policy.targetBytes * 2) }
            val (_, id) = seed(sizes)
            complete(policy, minInputFiles, maxInputFiles, batch = random.nextInt(1, 30))
            val summary =
                db.jdbi.withHandleUnchecked {
                        h ->
                    MaintenanceSummarySampler.read(h, listOf(id)).getValue(id)
                }
            assertThat(summary.sample.smallFiles)
                .isEqualTo(
                    policy.groups(sizes.filter { it < policy.targetBytes }, minInputFiles, maxInputFiles) { it }
                        .sumOf { it.size.toLong() },
                )
        }
    }

    @Test
    fun `partial samples stay unknown and a new sampler resumes the persisted cursor`() {
        val (name, id) = seed(List(17) { 1024L })
        val first = MaintenanceSummarySampler(db.jdbi, 65536, minFiles, maxFiles, 3600)
        assertThat(first.runOnce(3)).isTrue()
        val service =
            MaintenanceStatusService(
                db.jdbi,
                hydratorIntervalMs = 0,
                expiryIntervalMs = 0,
                cleanupIntervalMs = 0,
                compactionIntervalMs = 0,
                verifyIntervalMs = 0,
                retirementIntervalMs = 0,
                smallFileThresholdBytes = 65536,
                minInputFiles = minFiles,
                maxInputFiles = maxFiles,
            )
        assertThat(service.status(name).sampledAt).isNull()
        assertThat(db.jdbi.withHandleUnchecked { h -> MaintenanceSummarySampler.read(h, listOf(id)) }).isEmpty()
        // New object, no in-memory carry: state is in Postgres.
        complete(CompactionGrouping.of(65536), minFiles, maxFiles, batch = 3)
        val result = service.status(name)
        assertThat(result.sampledAt).isNotNull()
        assertThat(
            db.jdbi.withHandleUnchecked {
                    h ->
                MaintenanceSummarySampler.read(h, listOf(id)).getValue(id).sample.smallFiles
            },
        )
            .isEqualTo(17)
    }

    @Test
    fun `dashboard reads do not touch a million-row manifest and batches remain bounded`() {
        val (name, id) = seed(emptyList())
        db.jdbi.withHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, table_id, data_file_id, row_id_start, begin_snapshot,
                    path, record_count, file_size_bytes, stats_state)
                SELECT :cat, 1, n, n-1, 2, 's3://bucket/f' || n, 1, 1024, 'provided'
                FROM generate_series(1, 900003) AS n
                """,
            ).bind("cat", id).execute()
            h.createUpdate(
                """
                INSERT INTO hog_delete_file (catalog_id, table_id, delete_file_id, data_file_id,
                    begin_snapshot, end_snapshot, path, delete_count, file_size_bytes)
                SELECT :cat, 1, n, n, 2, 3, 's3://bucket/dv' || n, 1, 100
                FROM generate_series(1, 10000) AS n
                """,
            ).bind("cat", id).execute()
            val plan =
                h.createQuery(
                    """
                EXPLAIN (COSTS OFF) SELECT 1 FROM hog_delete_file
                WHERE catalog_id = :cat AND data_file_id = 5000 AND begin_snapshot <= 2
                  AND (end_snapshot IS NULL OR 2 < end_snapshot)
                """,
                ).bind("cat", id).mapTo(String::class.java).list().joinToString("\n")
            assertThat(plan).contains("hog_delete_file_data_lookup")
        }
        val service =
            MaintenanceStatusService(
                db.jdbi,
                hydratorIntervalMs = 0,
                expiryIntervalMs = 0,
                cleanupIntervalMs = 0,
                compactionIntervalMs = 0,
                verifyIntervalMs = 0,
                retirementIntervalMs = 0,
                smallFileThresholdBytes = 65536,
                minInputFiles = minFiles,
                maxInputFiles = maxFiles,
            )
        val start = System.nanoTime()
        assertThat(service.status(name).sampledAt).isNull()
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(2))
        val sampler = MaintenanceSummarySampler(db.jdbi, 65536, minFiles, maxFiles, 3600)
        sampler.runOnce(1000)
        val cursor =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT (scan_state->>'file')::bigint FROM hog_maintenance_summary WHERE catalog_id = :cat",
                )
                    .bind("cat", id).mapTo(Long::class.javaObjectType).one()
            }
        assertThat(cursor).isEqualTo(1000)
        complete(CompactionGrouping.of(65536), minFiles, maxFiles, batch = 10000)
        assertThat(
            db.jdbi.withHandleUnchecked {
                    h ->
                MaintenanceSummarySampler.read(h, listOf(id)).getValue(id).sample.smallFiles
            },
        )
            .isEqualTo(900003)
        // Stronger than a timing benchmark: block ALL manifest access;
        // both maintenance endpoints must still complete immediately.
        db.jdbi.open().use { lock ->
            lock.begin()
            lock.execute("LOCK TABLE hog_data_file IN ACCESS EXCLUSIVE MODE")
            val executor = Executors.newSingleThreadExecutor()
            try {
                executor.submit {
                    assertThat(service.status(name).sampledAt).isNotNull()
                    assertThat(service.instanceStatus().catalogs).isNotEmpty()
                    val report =
                        PartitionStatsService(
                            db.jdbi,
                            65536,
                            minFiles,
                            maxFiles,
                        ).partitionStats(name, null, null, 50)
                    assertThat(report.partitions.single().fileCount).isEqualTo(900003)
                    assertThat(report.partitions.single().debtScore).isEqualTo(900003)
                    assertThat(report.partitions.single().dvCount).isEqualTo(10000)
                }.get(2, TimeUnit.SECONDS)
            } finally {
                lock.rollback()
                executor.shutdownNow()
            }
        }
    }

    @Test
    fun `a sample published by a build with a different shape degrades to warmup, not a 500`() {
        val (name, id) = seed(List(6) { 1024L })
        complete(CompactionGrouping.of(65536), minFiles, maxFiles, batch = 3)
        assertThat(db.jdbi.withHandleUnchecked { MaintenanceSummarySampler.read(it, listOf(id)) })
            .isNotEmpty()

        // Exactly what an older build left behind: the bounds this build
        // requires are absent, and a field it has never heard of is
        // present. Both break a strict Jackson bind.
        db.jdbi.withHandleUnchecked { h ->
            h.createUpdate(
                """
                UPDATE hog_maintenance_summary
                SET sample = (sample - 'minInputFiles' - 'maxInputFiles') || '{"tierTarget": 8}'::jsonb
                WHERE catalog_id = :id
                """,
            ).bind("id", id).execute()
        }

        // Read on the REQUEST path: both maintenance endpoints go through
        // here, so a throw would be a dashboard-wide 500 for as long as
        // the stale sample sits there.
        assertThat(db.jdbi.withHandleUnchecked { MaintenanceSummarySampler.read(it, listOf(id)) })
            .isEmpty()
        val service =
            MaintenanceStatusService(
                db.jdbi,
                hydratorIntervalMs = 0,
                expiryIntervalMs = 0,
                cleanupIntervalMs = 0,
                compactionIntervalMs = 0,
                verifyIntervalMs = 0,
                retirementIntervalMs = 0,
                smallFileThresholdBytes = 65536,
                minInputFiles = minFiles,
                maxInputFiles = maxFiles,
            )
        assertThat(service.status(name).sampledAt).isNull()
        assertThat(
            PartitionStatsService(db.jdbi, 65536, minFiles, maxFiles).partitionStats(name, null, null, 50).partitions,
        )
            .isEmpty()

        // And the next completed scan republishes it in this build's shape.
        db.jdbi.withHandleUnchecked { h ->
            h.execute("UPDATE hog_maintenance_summary SET next_batch_at = now() WHERE catalog_id = ?", id)
        }
        complete(CompactionGrouping.of(65536), minFiles, maxFiles, batch = 3)
        assertThat(service.status(name).sampledAt).isNotNull()
    }

    @Test
    fun `an enormous registered file size cannot stop the sampler for other catalogs`() {
        // `file_size_bytes` is writer-supplied and only validated as
        // non-negative, so a value near Long.MAX_VALUE is reachable. It
        // used to overflow the pool's byte accumulator; the throw rolled
        // the scan back, so next_batch_at never advanced, so the same
        // catalog was re-picked every tick -- and one bad row stopped
        // the sampler for EVERY catalog on the instance.
        val (_, poisoned) = seed(List(2) { Long.MAX_VALUE })
        val (_, healthy) = seed(List(6) { 1024L })

        val sampler =
            MaintenanceSummarySampler(db.jdbi, 65536, minFiles, maxFiles, refreshSeconds = 3600)
        var steps = 0
        while (sampler.runOnce(17)) check(++steps < 10_000)

        val samples = db.jdbi.withHandleUnchecked { MaintenanceSummarySampler.read(it, listOf(poisoned, healthy)) }
        assertThat(samples.keys)
            .describedAs("both catalogs publish; neither starves the other")
            .contains(poisoned, healthy)
        assertThat(samples.getValue(healthy).sample.smallFiles).isEqualTo(6)
        // The enormous files are at/over the target, so they are not
        // candidates and carry no debt -- they only had to not throw.
        assertThat(samples.getValue(poisoned).sample.smallFiles).isZero()
    }

    @Test
    fun `refresh preserves the published generation and scans a stable catalog snapshot`() {
        val (name, id) = seed(List(17) { 1024L })
        val policy = CompactionGrouping.of(65536)
        complete(policy, minFiles, maxFiles, batch = 3)
        val old = db.jdbi.withHandleUnchecked { MaintenanceSummarySampler.read(it, listOf(id)).getValue(id) }
        db.jdbi.withHandleUnchecked { h ->
            h.execute("UPDATE hog_maintenance_summary SET next_batch_at = now() WHERE catalog_id = ?", id)
        }
        val worker = MaintenanceSummarySampler(db.jdbi, 65536, minFiles, maxFiles, 3600)
        assertThat(worker.runOnce(3)).isTrue()
        // Commit after the scan began. Its rows must not leak into this generation.
        catalogs.createNamespace(name, "new_snapshot")
        db.jdbi.withHandleUnchecked { h ->
            h.execute(
                """
                INSERT INTO hog_data_file (catalog_id, table_id, data_file_id, row_id_start, begin_snapshot,
                    path, record_count, file_size_bytes, stats_state)
                SELECT ?, 1, n, n-1, 3, 's3://bucket/new' || n, 1, 1024, 'provided'
                FROM generate_series(18, 25) AS n
                """,
                id,
            )
        }
        val during = db.jdbi.withHandleUnchecked { MaintenanceSummarySampler.read(it, listOf(id)).getValue(id) }
        assertThat(during).isEqualTo(old)
        complete(policy, minFiles, maxFiles, batch = 3)
        val firstRefresh = db.jdbi.withHandleUnchecked { MaintenanceSummarySampler.read(it, listOf(id)).getValue(id) }
        assertThat(firstRefresh.sample.snapshotId).isEqualTo(2)
        assertThat(firstRefresh.sample.smallFiles).isEqualTo(17)
        db.jdbi.withHandleUnchecked {
            it.execute("UPDATE hog_maintenance_summary SET next_batch_at = now() WHERE catalog_id = ?", id)
        }
        complete(policy, minFiles, maxFiles, batch = 3)
        val secondRefresh = db.jdbi.withHandleUnchecked { MaintenanceSummarySampler.read(it, listOf(id)).getValue(id) }
        assertThat(secondRefresh.sample.snapshotId).isEqualTo(3)
        assertThat(secondRefresh.sample.smallFiles).isEqualTo(25)
    }

    @Test
    fun `expiry overtaking a scan restarts it without publishing partial counts`() {
        val (name, id) = seed(List(17) { 1024L })
        val worker = MaintenanceSummarySampler(db.jdbi, 65536, minFiles, maxFiles, 3600)
        worker.runOnce(3)
        catalogs.createNamespace(name, "advance")
        db.jdbi.withHandleUnchecked {
            it.execute(
                "UPDATE hog_catalog SET earliest_snapshot_id = 3 WHERE catalog_id = ?",
                id,
            )
        }
        worker.runOnce(3)
        assertThat(db.jdbi.withHandleUnchecked { MaintenanceSummarySampler.read(it, listOf(id)) }).isEmpty()
        complete(CompactionGrouping.of(65536), minFiles, maxFiles, batch = 3)
        val result = db.jdbi.withHandleUnchecked { MaintenanceSummarySampler.read(it, listOf(id)).getValue(id) }
        assertThat(result.sample.snapshotId).isEqualTo(3)
        assertThat(result.sample.smallFiles).isEqualTo(17)
    }
}
