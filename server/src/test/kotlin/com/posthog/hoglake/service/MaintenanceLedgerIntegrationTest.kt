package com.posthog.hoglake.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.hydrator.Hydrator
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceBacklog
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * The hog_maintenance_run ledger: every maintenance task records its runs
 * (loop sweeps AND manual triggers), failures land as status='failed',
 * the cleanup sweep purges rows past the retention knob, and the status
 * service composes live backlogs with last runs. Seeding is direct SQL
 * (the ExpiryServiceIntegrationTest pattern) so no object store is
 * needed: the hydrator claims a pending file, its footer fetch fails
 * against a dead endpoint, and the claim is recorded as transient.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MaintenanceLedgerIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val jdbi get() = db.jdbi
    private val json = ObjectMapper()

    /** Never contacted successfully: 127.0.0.1:9 is the discard port. */
    private val deadStore =
        ObjectStore(
            endpoint = "http://127.0.0.1:9",
            region = "us-east-1",
            accessKey = "unused",
            secretKey = "unused",
            pathStyle = true,
        )
    private val deadRemovals =
        RemovalStore(
            endpoint = "http://127.0.0.1:9",
            region = "us-east-1",
            accessKey = "unused",
            secretKey = "unused",
            pathStyle = true,
        )

    @AfterAll
    fun tearDown() {
        deadStore.close()
        deadRemovals.close()
        db.close()
    }

    // ---- seeding + ledger reads --------------------------------------------

    private fun seedCatalog(
        name: String,
        head: Long = 0,
        retentionSeconds: Long? = null,
    ): Long =
        jdbi.withHandleUnchecked { h ->
            val catalogId =
                h.createQuery(
                    """
                    INSERT INTO hog_catalog
                        (name, data_path, last_snapshot_id, snapshot_retention_seconds)
                    VALUES (:name, 's3://bucket/p', :head, :retention)
                    RETURNING catalog_id
                    """,
                )
                    .bind("name", name)
                    .bind("head", head)
                    .apply {
                        if (retentionSeconds == null) {
                            bindNull("retention", java.sql.Types.BIGINT)
                        } else {
                            bind("retention", retentionSeconds)
                        }
                    }
                    .mapTo(Long::class.java)
                    .one()
            for (s in 0..head) {
                h.execute(
                    """
                    INSERT INTO hog_snapshot (catalog_id, snapshot_id, snapshot_time, schema_version)
                    VALUES (?, ?, now(), 0)
                    """,
                    catalogId,
                    s,
                )
            }
            catalogId
        }

    /** A pending file (the hydrator's claim population) on a one-table catalog. */
    private fun seedPendingFile(catalogId: Long) {
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 1, 0)",
                catalogId,
            )
            h.execute(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                    path, record_count, file_size_bytes, row_id_start, stats_state)
                VALUES (?, 1, 1, 0, 's3://bucket/pending.parquet', 10, 100, 0, 'pending')
                """,
                catalogId,
            )
        }
    }

    private data class LedgerRow(
        val task: String,
        val trigger: String,
        val status: String,
        val error: String?,
        val result: String?,
    )

    /** Ledger rows for [catalog], newest first. */
    private fun ledgerRows(catalog: String): List<LedgerRow> =
        jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT task, run_trigger, status, error, CAST(result AS text) AS result
                  FROM hog_maintenance_run
                 WHERE catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = :name)
                 ORDER BY run_id DESC
                """,
            )
                .bind("name", catalog)
                .map { rs, _ ->
                    LedgerRow(
                        task = rs.getString("task"),
                        trigger = rs.getString("run_trigger"),
                        status = rs.getString("status"),
                        error = rs.getString("error"),
                        result = rs.getString("result"),
                    )
                }
                .list()
        }

    // ---- recording -----------------------------------------------------------

    @Test
    fun `ledger serialization failure never changes a successful task result`() {
        seedCatalog("led-serialization")
        val value =
            object {
                val broken: String get() = error("serialization-probe")
            }
        val result =
            com.posthog.hoglake.persistence.MaintenanceRunStore(jdbi)
                .recorded("led-serialization", MaintenanceTask.VERIFY, MaintenanceTrigger.MANUAL) { value }
        assertThat(result).isSameAs(value)
        assertThat(ledgerRows("led-serialization")).isEmpty()
    }

    @Test
    fun `expiry runOnce records a manual run with the wire-shaped result`() {
        val catalogId = seedCatalog("led-exp", head = 1, retentionSeconds = 60)
        // Age the snapshots out so the sweep has something to expire.
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_snapshot SET snapshot_time = now() - interval '1 hour' WHERE catalog_id = ?",
                catalogId,
            )
        }

        val result = ExpiryService(jdbi).runOnce("led-exp", 10)
        assertThat(result.snapshotsExpired).isEqualTo(1)

        val rows = ledgerRows("led-exp")
        assertThat(rows).hasSize(1)
        val row = rows.single()
        assertThat(row.task).isEqualTo("expiry")
        assertThat(row.trigger).isEqualTo("manual")
        assertThat(row.status).isEqualTo("ok")
        assertThat(row.error).isNull()
        // The payload is the wire shape: snake_case, exactly the POST body.
        val node = json.readTree(row.result)
        assertThat(node["snapshots_expired"].asLong()).isEqualTo(1)
        assertThat(node["new_earliest_snapshot_id"].asLong()).isEqualTo(1)
        assertThat(node.has("floored_by_consumer")).isFalse() // NON_NULL, absent when null
    }

    @Test
    fun `runOnceAllCatalogs records loop-trigger runs`() {
        seedCatalog("led-loop")

        ExpiryService(jdbi).runOnceAllCatalogs(10)

        val row = ledgerRows("led-loop").single()
        assertThat(row.trigger).isEqualTo("loop")
        assertThat(row.status).isEqualTo("ok")
    }

    @Test
    fun `a thrown run records failed status with the error detail`() {
        seedCatalog("led-fail")

        assertThatThrownBy { ExpiryService(jdbi).runOnce("led-fail", 0) }
            .isInstanceOf(HoglakeException.Validation::class.java)

        val row = ledgerRows("led-fail").single()
        assertThat(row.status).isEqualTo("failed")
        assertThat(row.error).contains("batch size must be positive")
        assertThat(row.result).isNull()
    }

    @Test
    fun `verify runOnce records a manual run with the report payload`() {
        seedCatalog("led-verify")

        val report = VerifyService(jdbi).runOnce("led-verify")
        assertThat(report.status).isEqualTo("pass")

        val row = ledgerRows("led-verify").single()
        assertThat(row.task).isEqualTo("verify")
        assertThat(row.trigger).isEqualTo("manual")
        val node = json.readTree(row.result)
        assertThat(node["status"].asText()).isEqualTo("pass")
        assertThat(node["checks"].map { it["check"].asText() }).contains("row_id_tiling")
    }

    @Test
    fun `cleanup records runs and purges rows past the retention window`() {
        val catalogId = seedCatalog("led-purge")
        // A run row that is already past retention, plus a fresh one.
        jdbi.useHandleUnchecked { h ->
            h.execute(
                """
                INSERT INTO hog_maintenance_run
                    (catalog_id, task, run_trigger, started_at, finished_at, status)
                VALUES (?, 'expiry', 'loop', now() - interval '2 hours', now() - interval '2 hours', 'ok'),
                       (?, 'expiry', 'loop', now(), now(), 'ok')
                """,
                catalogId,
                catalogId,
            )
        }

        CleanupService(jdbi, deadRemovals, maintenanceLedgerRetentionSeconds = 3600)
            .runOnce("led-purge", 100)

        val rows = ledgerRows("led-purge")
        // The stale row is purged; the fresh row and this drain's own
        // row (recorded after the purge) survive.
        assertThat(rows.map { it.task }).containsExactlyInAnyOrder("cleanup", "expiry")
        assertThat(rows.single { it.task == "cleanup" }.trigger).isEqualTo("manual")
    }

    @Test
    fun `hydrator records one row per claimed catalog`() {
        val withWork = seedCatalog("led-hyd")
        seedPendingFile(withWork)
        seedCatalog("led-hyd-quiet")

        // The dead endpoint makes the footer fetch transient: claimed 1,
        // hydrated 0, and the file STAYS pending for the next sweep.
        Hydrator(jdbi, deadStore).runOnce()

        val row = ledgerRows("led-hyd").single()
        assertThat(row.task).isEqualTo("hydrator")
        assertThat(row.trigger).isEqualTo("loop")
        assertThat(row.status).isEqualTo("ok")
        val node = json.readTree(row.result)
        assertThat(node["claimed"].asLong()).isEqualTo(1)
        assertThat(node["hydrated"].asLong()).isEqualTo(0)
        assertThat(node["transient"].asLong() + node["failed"].asLong()).isEqualTo(1)

        // The sweep claimed nothing for the quiet catalog: no row.
        assertThat(ledgerRows("led-hyd-quiet")).isEmpty()
    }

    @Test
    fun `rehydrateFailed records a manual hydrator run`() {
        val catalogId = seedCatalog("led-rehyd")
        seedPendingFile(catalogId)
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_data_file SET stats_state = 'failed' WHERE catalog_id = ?",
                catalogId,
            )
        }

        Hydrator(jdbi, deadStore).rehydrateFailed("led-rehyd")

        val row = ledgerRows("led-rehyd").single()
        assertThat(row.task).isEqualTo("hydrator")
        assertThat(row.trigger).isEqualTo("manual")
        assertThat(json.readTree(row.result)["requeued"].asLong()).isEqualTo(1)
    }

    // ---- the read side -------------------------------------------------------

    @Test
    fun `status service reports backlogs and last runs per task`() {
        val catalogId = seedCatalog("led-status", head = 2, retentionSeconds = 3600)
        seedPendingFile(catalogId)
        ExpiryService(jdbi).runOnce("led-status", 10)
        val sampler = MaintenanceSummarySampler(jdbi, 512L * 1024 * 1024, 8, 3600)
        while (sampler.runOnce()) { /* finish async samples */ }

        val status = statusSvc().status("led-status")

        assertThat(status.catalog).isEqualTo("led-status")
        assertThat(status.tasks.map { it.task }).containsExactly(
            MaintenanceTask.HYDRATOR,
            MaintenanceTask.EXPIRY,
            MaintenanceTask.CLEANUP,
            MaintenanceTask.COMPACTION,
            MaintenanceTask.VERIFY,
        )

        val hydrator = status.tasks[0]
        assertThat(hydrator.loopIntervalMs).isEqualTo(5_000)
        assertThat(hydrator.lastRun).isNull()
        assertThat((hydrator.backlog as MaintenanceBacklog.HydratorBacklog).pendingFiles).isEqualTo(1)

        val expiry = status.tasks[1]
        assertThat(expiry.lastRun?.trigger).isEqualTo(MaintenanceTrigger.MANUAL)
        assertThat(expiry.lastRun?.status?.wire).isEqualTo("ok")
        val expiryBacklog = expiry.backlog as MaintenanceBacklog.ExpiryBacklog
        assertThat(expiryBacklog.snapshotRetentionSeconds).isEqualTo(3600)
        assertThat(expiryBacklog.headSnapshotId).isEqualTo(2)

        val cleanup = status.tasks[2]
        assertThat((cleanup.backlog as MaintenanceBacklog.CleanupBacklog).oldestQueuedAgeSeconds).isNull()

        val compaction = status.tasks[3]
        assertThat(compaction.loopIntervalMs).isEqualTo(0)
        val compactionBacklog = compaction.backlog as MaintenanceBacklog.CompactionBacklog
        // One small file, but a 1-file bucket can never reach the
        // planner's next byte quota — so it is NOT actionable debt.
        assertThat(compactionBacklog.smallFiles).isEqualTo(0)
        assertThat(compactionBacklog.targetBytes).isEqualTo(512L * 1024 * 1024)

        val verify = status.tasks[4]
        assertThat(verify.loopIntervalMs).isNull()
        assertThat(verify.lastRun).isNull()
    }

    @Test
    fun `runs history pages newest-first with the before cursor and task filter`() {
        seedCatalog("led-runs")
        val svc = ExpiryService(jdbi)
        svc.runOnce("led-runs", 10)
        svc.runOnce("led-runs", 10)
        VerifyService(jdbi).runOnce("led-runs")

        val statusSvc = statusSvc()

        // All tasks, newest first.
        val all = statusSvc.runs("led-runs", task = null, before = null, limit = 10)
        assertThat(all.runs).hasSize(3)
        assertThat(all.hasMore).isFalse()
        assertThat(all.runs.map { it.runId }).isSortedAccordingTo(Comparator.reverseOrder())

        // Task filter: verify only.
        val verifyOnly = statusSvc.runs("led-runs", MaintenanceTask.VERIFY, before = null, limit = 10)
        assertThat(verifyOnly.runs.map { it.task })
            .containsExactly(MaintenanceTask.VERIFY)

        // Paging: limit 1 -> has_more, and `before` resumes below it.
        val page1 = statusSvc.runs("led-runs", MaintenanceTask.EXPIRY, before = null, limit = 1)
        assertThat(page1.runs).hasSize(1)
        assertThat(page1.hasMore).isTrue()
        val page2 = statusSvc.runs("led-runs", MaintenanceTask.EXPIRY, before = page1.runs[0].runId, limit = 10)
        assertThat(page2.runs).hasSize(1)
        assertThat(page2.hasMore).isFalse()
        assertThat(page2.runs[0].runId).isLessThan(page1.runs[0].runId)

        // Unknown catalog 404s through the service too.
        assertThatThrownBy { statusSvc.runs("led-nope", null, null, 10) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy { statusSvc.runs("led-runs", null, null, 0) }
            .isInstanceOf(HoglakeException.Validation::class.java)
    }

    // ---- the instance-wide reads --------------------------------------------

    private fun statusSvc() =
        MaintenanceStatusService(
            jdbi,
            hydratorIntervalMs = 5_000,
            expiryIntervalMs = 60_000,
            cleanupIntervalMs = 60_000,
            compactionIntervalMs = 0,
            smallFileThresholdBytes = 512L * 1024 * 1024,
        )

    @Test
    fun `instance status rolls up every catalog with its own backlog and last runs`() {
        val aId = seedCatalog("led-inst-a", head = 1, retentionSeconds = 3600)
        seedCatalog("led-inst-b")
        seedPendingFile(aId)
        ExpiryService(jdbi).runOnce("led-inst-a", 10)
        VerifyService(jdbi).runOnce("led-inst-b")

        // (The test database is shared per class; narrow to this test's pair.)
        val sampler = MaintenanceSummarySampler(jdbi, 512L * 1024 * 1024, 8, 3600)
        while (sampler.runOnce()) { /* finish async samples */ }
        val instance = statusSvc().instanceStatus()
        val pair = instance.catalogs.filter { it.catalog.startsWith("led-inst-") }
        assertThat(pair.map { it.catalog }).containsExactly("led-inst-a", "led-inst-b")

        val a = pair[0]
        assertThat((a.tasks[0].backlog as MaintenanceBacklog.HydratorBacklog).pendingFiles)
            .isEqualTo(1)
        assertThat(a.tasks[1].lastRun?.task).isEqualTo(MaintenanceTask.EXPIRY)
        assertThat(a.tasks[1].lastRun?.catalog).isEqualTo("led-inst-a")

        val b = pair[1]
        assertThat((b.tasks[0].backlog as MaintenanceBacklog.HydratorBacklog).pendingFiles)
            .isEqualTo(0)
        assertThat(b.tasks[1].lastRun).isNull()
        assertThat(b.tasks[4].lastRun?.task).isEqualTo(MaintenanceTask.VERIFY)
        assertThat(b.tasks[4].lastRun?.catalog).isEqualTo("led-inst-b")
    }

    @Test
    fun `instance runs feed spans catalogs newest-first with catalog names`() {
        seedCatalog("led-feed-a")
        seedCatalog("led-feed-b")
        ExpiryService(jdbi).runOnce("led-feed-a", 10)
        ExpiryService(jdbi).runOnce("led-feed-b", 10)

        val page = statusSvc().instanceRuns(task = null, before = null, limit = 50)
        // Both catalogs' runs interleaved by run_id, each carrying its name.
        val ours = page.runs.filter { it.catalog in setOf("led-feed-a", "led-feed-b") }
        assertThat(ours.map { it.task }).containsOnly(MaintenanceTask.EXPIRY)
        assertThat(ours.map { it.catalog }.distinct())
            .containsExactlyInAnyOrder("led-feed-a", "led-feed-b")

        val expiryOnly = statusSvc().instanceRuns(MaintenanceTask.CLEANUP, before = null, limit = 50)
        assertThat(expiryOnly.runs.map { it.catalog })
            .noneMatch { it in setOf("led-feed-a", "led-feed-b") }
    }
}
