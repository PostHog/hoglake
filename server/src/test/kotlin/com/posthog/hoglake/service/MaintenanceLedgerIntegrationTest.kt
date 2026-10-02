package com.posthog.hoglake.service

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.api.toDto
import com.posthog.hoglake.compaction.CompactionGrouping
import com.posthog.hoglake.hydrator.Hydrator
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceBacklog
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.persistence.MaintenanceRunStore
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.wireObjectMapper
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
    @Test
    fun `a pre-upgrade compaction row is returned with the counters it predates`() {
        // The stored payload is the raw JSON the API returned at the
        // time, handed back verbatim — so a row written before
        // invalid_data existed has no such field, while the schema
        // lists it as required. Generated clients go out of contract on
        // it and the webui's `!== "0"` guard is TRUE for `undefined`.
        val catalogId = seedCatalog("led-preupgrade")
        val stored =
            """
            {"groups_compacted":2,"files_in":6,"files_out":2,"bytes_in":100,"bytes_out":90,
             "skipped_conflicts":0,"dv_superseded":0,"unconvertible_schema":0,"failed_groups":0}
            """.trimIndent()
        jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_maintenance_run
                    (catalog_id, task, run_trigger, started_at, finished_at, status, result)
                VALUES (:catalogId, 'compaction', 'loop', now(), now(), 'ok', CAST(:result AS jsonb))
                """,
            )
                .bind("catalogId", catalogId)
                .bind("result", stored)
                .execute()
        }
        // It really is absent on the way in — otherwise this test would
        // pass for the wrong reason.
        assertThat(json.readTree(ledgerRows("led-preupgrade").single().result).has("invalid_data"))
            .isFalse()

        val run =
            jdbi.withHandleUnchecked { h ->
                MaintenanceRunStore(jdbi).history(h, catalogId, null, null, 10)
            }.single()
        val wire = wireObjectMapper().valueToTree<JsonNode>(run.toDto())
        val result = wire["result"]
        assertThat(result["invalid_data"].asLong())
            .describedAs("normalized on READ; the counter did not exist, so nothing it counts happened")
            .isZero()
        assertThat(result["heap_budget_exceeded"].asLong())
            .describedAs("and every counter added after it, by the same rule")
            .isZero()
        assertThat(result["claimed_elsewhere"].asLong())
            .describedAs("claimed_elsewhere joined COMPACTION_COUNTERS_ADDED_LATER with the claims")
            .isZero()
        val planMeasures =
            listOf(
                "candidates_fetched",
                "buckets_considered",
                "buckets_available",
                "candidates_truncated",
                "plan_ms",
            )
        for (planMeasure in planMeasures) {
            assertThat(result[planMeasure].asLong())
                .describedAs("the planner's plan measures joined the list when the planner was bounded")
                .isZero()
        }
        // Every other field survives untouched, and the row is complete
        // against the schema's required list.
        assertThat(result["groups_compacted"].asLong()).isEqualTo(2)
        assertThat(result.fieldNames().asSequence().toList())
            .containsExactlyInAnyOrder(
                "groups_compacted", "files_in", "files_out", "bytes_in", "bytes_out",
                "skipped_conflicts", "dv_superseded", "unconvertible_schema",
                "invalid_data", "heap_budget_exceeded", "failed_groups",
                "claimed_elsewhere",
                *planMeasures.toTypedArray(),
            )
    }

    @Test
    fun `a pre-upgrade cleanup row is returned with the counters it predates`() {
        // The compaction twin above, for the cleanup counters. A row
        // written before objects_removed and settled_elsewhere existed
        // has neither, and both are NON_DEFAULT on the stored model, so
        // even a row this build wrote omits a zero. The schema declares
        // them optional for exactly that reason, and the read path fills
        // 0 (CLEANUP_COUNTERS_ADDED_LATER) so a client's zero is a fact
        // and not a guess.
        val catalogId = seedCatalog("led-preupgrade-cleanup")
        val stored = """{"removed":7,"missing":1,"still_referenced":0}"""
        jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_maintenance_run
                    (catalog_id, task, run_trigger, started_at, finished_at, status, result)
                VALUES (:catalogId, 'cleanup', 'loop', now(), now(), 'ok', CAST(:result AS jsonb))
                """,
            )
                .bind("catalogId", catalogId)
                .bind("result", stored)
                .execute()
        }
        // Absent on the way in — otherwise this passes for the wrong reason.
        val raw = json.readTree(ledgerRows("led-preupgrade-cleanup").single().result)
        assertThat(raw.has("objects_removed")).isFalse()
        assertThat(raw.has("settled_elsewhere")).isFalse()

        val run =
            jdbi.withHandleUnchecked { h ->
                MaintenanceRunStore(jdbi).history(h, catalogId, null, null, 10)
            }.single()
        val result = wireObjectMapper().valueToTree<JsonNode>(run.toDto())["result"]
        assertThat(result["objects_removed"].asLong())
            .describedAs("normalized on READ; the counter did not exist, so nothing it counts happened")
            .isZero()
        assertThat(result["settled_elsewhere"].asLong()).isZero()
        assertThat(result["deadline_skipped"].asLong()).isZero()
        // And receipts_purged, the newest of them (#240, V24): the same
        // rule, for the same reason — the purge did not exist when this
        // row was written, so nothing it counts could have happened.
        assertThat(result["receipts_purged"].asLong()).isZero()
        assertThat(result["receipts_purge_failures"].asLong()).isZero()
        // Everything the row did carry survives untouched.
        assertThat(result["removed"].asLong()).isEqualTo(7)
        assertThat(result["missing"].asLong()).isEqualTo(1)
        assertThat(result.fieldNames().asSequence().toList())
            .containsExactlyInAnyOrder(
                "removed",
                "missing",
                "still_referenced",
                "objects_removed",
                "settled_elsewhere",
                "deadline_skipped",
                "receipts_purged",
                "receipts_purge_failures",
            )
    }

    @Test
    fun `a pre-upgrade expiry row is returned with the purge counters it predates`() {
        // The expiry twin of the two above, for the two-phase sweep's
        // four new counters — and for the ONE deliberate exclusion.
        //
        // A row written before the sweep was split carries none of them.
        // `data_files_purged`, `purge_pages`, `purge_failures`,
        // `advance_halvings` and `purge_halvings` are filled with 0 on
        // READ (EXPIRY_COUNTERS_ADDED_LATER), which is honest: the purge
        // did not exist, so nothing it counts could have happened.
        //
        // TWO FIELDS MUST STAY ABSENT, for two different reasons.
        // `purge_truncated` is a BOOLEAN and the filler writes the
        // integer 0, so listing it would put a spec-invalid value in a
        // ledger row. `purge_remaining` is an integer, but its ABSENCE
        // MEANS "unknown": the count is best-effort under a 5 s bound,
        // and a sweep whose count could not finish omits it rather than
        // claim a zero that would contradict `purge_truncated`. Filling
        // it on read would re-introduce exactly that lie. Every console
        // guard on both is undefined-safe.
        val catalogId = seedCatalog("led-preupgrade-expiry")
        val stored =
            """{"snapshots_expired":4,"data_files_queued":9,"delete_files_queued":0,""" +
                """"new_earliest_snapshot_id":41}"""
        jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_maintenance_run
                    (catalog_id, task, run_trigger, started_at, finished_at, status, result)
                VALUES (:catalogId, 'expiry', 'loop', now(), now(), 'ok', CAST(:result AS jsonb))
                """,
            )
                .bind("catalogId", catalogId)
                .bind("result", stored)
                .execute()
        }
        // Absent on the way in — otherwise this passes for the wrong reason.
        val raw = json.readTree(ledgerRows("led-preupgrade-expiry").single().result)
        assertThat(raw.has("data_files_purged")).isFalse()
        assertThat(raw.has("purge_truncated")).isFalse()

        val run =
            jdbi.withHandleUnchecked { h ->
                MaintenanceRunStore(jdbi).history(h, catalogId, null, null, 10)
            }.single()
        val result = wireObjectMapper().valueToTree<JsonNode>(run.toDto())["result"]
        assertThat(result["data_files_purged"].asLong())
            .describedAs("normalized on READ; the purge did not exist, so it purged nothing")
            .isZero()
        assertThat(result["purge_pages"].asLong()).isZero()
        assertThat(result["purge_failures"].asLong()).isZero()
        assertThat(result["advance_halvings"].asLong()).isZero()
        assertThat(result["purge_halvings"].asLong()).isZero()
        assertThat(result.has("purge_truncated"))
            .describedAs("a boolean must NOT be filled with the integer 0")
            .isFalse()
        assertThat(result.has("purge_remaining"))
            .describedAs("and an 'unknown' must NOT be filled with a zero that contradicts it")
            .isFalse()
        // Everything the row did carry survives untouched.
        assertThat(result["snapshots_expired"].asLong()).isEqualTo(4)
        assertThat(result["data_files_queued"].asLong()).isEqualTo(9)
        assertThat(result["new_earliest_snapshot_id"].asLong()).isEqualTo(41)
        assertThat(result.fieldNames().asSequence().toList())
            .containsExactlyInAnyOrder(
                "snapshots_expired",
                "data_files_queued",
                "delete_files_queued",
                "new_earliest_snapshot_id",
                "offsets_released",
                "data_files_purged",
                "purge_pages",
                "purge_failures",
                "advance_halvings",
                "purge_halvings",
            )
    }

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
                .recorded("led-serialization", MaintenanceTask.CLEANUP, MaintenanceTrigger.MANUAL) { value }
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
        val sampler =
            MaintenanceSummarySampler(
                jdbi,
                512L * 1024 * 1024,
                CompactionGrouping.DEFAULT_MIN_INPUT_FILES,
                CompactionGrouping.DEFAULT_MAX_INPUT_FILES,
                3600,
            )
        while (sampler.runOnce()) { /* finish async samples */ }

        val status = statusSvc().status("led-status")

        assertThat(status.catalog).isEqualTo("led-status")
        assertThat(status.tasks.map { it.task }).containsExactly(
            MaintenanceTask.HYDRATOR,
            MaintenanceTask.EXPIRY,
            MaintenanceTask.CLEANUP,
            MaintenanceTask.COMPACTION,
            // Appended, never inserted: this order is the wire order and
            // the central matrix's column order. #261 removing `verify`
            // from in front of this entry is the same hazard in reverse:
            // retirement moved from index 5 to 4 and the positional
            // reads below moved with it.
            MaintenanceTask.RETIREMENT,
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

        val retirement = status.tasks[4]
        assertThat(retirement.loopIntervalMs).isZero()
        assertThat(retirement.lastRun).isNull()
        assertThat(retirement.backlog).isEqualTo(MaintenanceBacklog.RetirementBacklog)
        assertThat(status.tasks.map { it.task })
            .describedAs("#261 removed verify from the task list")
            .doesNotContain(MaintenanceTask.VERIFY)
    }

    @Test
    fun `runs history pages newest-first with the before cursor and task filter`() {
        seedCatalog("led-runs")
        val svc = ExpiryService(jdbi)
        svc.runOnce("led-runs", 10)
        svc.runOnce("led-runs", 10)
        MaintenanceRunStore(jdbi).recorded("led-runs", MaintenanceTask.CLEANUP, MaintenanceTrigger.MANUAL) { 1 }

        val statusSvc = statusSvc()

        // All tasks, newest first.
        val all = statusSvc.runs("led-runs", task = null, before = null, limit = 10)
        assertThat(all.runs).hasSize(3)
        assertThat(all.hasMore).isFalse()
        assertThat(all.runs.map { it.runId }).isSortedAccordingTo(Comparator.reverseOrder())

        // Task filter: cleanup only.
        val cleanupOnly = statusSvc.runs("led-runs", MaintenanceTask.CLEANUP, before = null, limit = 10)
        assertThat(cleanupOnly.runs.map { it.task })
            .containsExactly(MaintenanceTask.CLEANUP)
        // `verify` is still a legal filter even though #261 removed the
        // subsystem: the value must parse rather than 422. This catalog
        // has no such row; `a LEGACY verify ledger row still decodes...`
        // below is the one that proves a row actually comes back.
        assertThat(statusSvc.runs("led-runs", MaintenanceTask.VERIFY, before = null, limit = 10).runs)
            .isEmpty()

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

    /**
     * The twelve-check report the removed `VerifyReport.forLedger()`
     * wrote, byte-for-byte in shape: no `description` on any check
     * (ledger rows dropped them — identical constant prose per row), the
     * `status`/`catalog` rollup, and `violations` as a number.
     *
     * A real row from a dev catalog, trimmed to one sample so the
     * literal stays readable. The check NAMES and their ORDER are the
     * part that matters: they are what the spec's frozen `VerifyCheck`
     * enum still lists.
     */
    private fun legacyVerifyReport(catalog: String) =
        """
        {"catalog":"$catalog","status":"fail","checks":[
          {"check":"row_id_tiling","status":"pass","violations":0,"samples":[]},
          {"check":"delete_vectors","status":"pass","violations":0,"samples":[]},
          {"check":"orphans","status":"pass","violations":0,"samples":[]},
          {"check":"removal_queue","status":"fail","violations":25,
           "samples":["removal_id=7 path='s3://b/f0.parquet' queued but still live-referenced"]},
          {"check":"snapshot_density","status":"pass","violations":0,"samples":[]},
          {"check":"next_row_id","status":"pass","violations":0,"samples":[]},
          {"check":"expiry_floor","status":"pass","violations":0,"samples":[]},
          {"check":"visibility_bounds","status":"pass","violations":0,"samples":[]},
          {"check":"offset_release","status":"pass","violations":0,"samples":[]},
          {"check":"staging_tickets","status":"pass","violations":0,"samples":[]},
          {"check":"upload_claims","status":"pass","violations":0,"samples":[]},
          {"check":"compaction_claims","status":"pass","violations":0,"samples":[]}
        ]}
        """.trimIndent()

    @Test
    fun `a LEGACY verify ledger row still decodes, and comes back filtered AND unfiltered`() {
        // WHY `MaintenanceTask.VERIFY` STAYS IN THE ENUM, asserted on a
        // row rather than argued in a KDoc. #261 removed the subsystem,
        // but `hog_maintenance_run.task`'s CHECK (V2, frozen chain) still
        // admits 'verify' and the ledger HOLDS those rows for
        // HOGLAKE_MAINTENANCE_LEDGER_RETENTION_SECONDS — 7 days by
        // default, longer wherever it is tuned up.
        //
        // `MaintenanceRunStore.runMapper` does `fromWire(...) ?: error(...)`,
        // so dropping the enum value would not degrade one row: it would
        // make every UNFILTERED `GET /maintenance/runs` throw for as long
        // as one historical row survives, which is the console's main
        // screen. That is the failure this pins.
        //
        // MUTATION: delete `VERIFY` from `MaintenanceTask` and this stops
        // compiling; keep it but make `runMapper` skip unknown tasks and
        // the unfiltered assertion reds. The pre-#279 version of this
        // test only asserted that `?task=verify` was a legal FILTER on a
        // catalog with no such row, which passes with no mapper work at
        // all.
        val catalogId = seedCatalog("led-legacy-verify")
        jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_maintenance_run
                    (catalog_id, task, run_trigger, started_at, finished_at, status, result)
                VALUES (:c, 'verify', 'manual', now(), now(), 'ok', CAST(:result AS jsonb))
                """,
            ).bind("c", catalogId).bind("result", legacyVerifyReport("led-legacy-verify")).execute()
        }
        // A second row of a LIVE task, so the unfiltered page has to
        // decode both and the verify row cannot be the only thing there.
        ExpiryService(jdbi).runOnce("led-legacy-verify", 10)

        val statusSvc = statusSvc()

        // 1. The FILTERED page returns it, which needs `fromWire` to
        //    accept the wire value on the way IN as well as out.
        val filtered = statusSvc.runs("led-legacy-verify", MaintenanceTask.VERIFY, before = null, limit = 10)
        assertThat(filtered.runs).hasSize(1)
        val run = filtered.runs.single()
        assertThat(run.task).isEqualTo(MaintenanceTask.VERIFY)
        assertThat(run.trigger).isEqualTo(MaintenanceTrigger.MANUAL)

        // 2. The UNFILTERED page returns it beside the live task's row.
        //    This is the one that throws if the enum loses the value.
        val unfiltered = statusSvc.runs("led-legacy-verify", task = null, before = null, limit = 10)
        assertThat(unfiltered.runs.map { it.task })
            .describedAs("the historical row must not break the page every operator opens")
            .containsExactlyInAnyOrder(MaintenanceTask.VERIFY, MaintenanceTask.EXPIRY)

        // 3. And through the DTO/normalizer, which must hand a verify
        //    payload back VERBATIM: `normalizeLedgerResult` has no arm
        //    for the task, so nothing is injected into a shape no
        //    current code writes.
        val result = wireObjectMapper().valueToTree<JsonNode>(run.toDto())["result"]
        assertThat(result["status"].asText()).isEqualTo("fail")
        assertThat(result["checks"].map { it["check"].asText() })
            .describedAs("all twelve checks, in the order the spec's frozen VerifyCheck enum lists")
            .containsExactly(
                "row_id_tiling", "delete_vectors", "orphans", "removal_queue",
                "snapshot_density", "next_row_id", "expiry_floor", "visibility_bounds",
                "offset_release", "staging_tickets", "upload_claims", "compaction_claims",
            )
        val failing = result["checks"].single { it["status"].asText() == "fail" }
        assertThat(failing["check"].asText()).isEqualTo("removal_queue")
        assertThat(failing["violations"].asLong())
            .describedAs("the TRUE count survives the round trip, not the sample length")
            .isEqualTo(25)
        assertThat(failing["samples"]).hasSize(1)
        assertThat(result["checks"]).allSatisfy {
            assertThat(it.has("description"))
                .describedAs("a ledger row never stored descriptions, and nothing may invent one")
                .isFalse()
        }
        // Nothing was injected: a verify payload has exactly the keys it
        // was written with.
        assertThat(result.fieldNames().asSequence().toList())
            .containsExactlyInAnyOrder("catalog", "status", "checks")
    }

    private fun statusSvc() =
        MaintenanceStatusService(
            jdbi,
            hydratorIntervalMs = 5_000,
            expiryIntervalMs = 60_000,
            cleanupIntervalMs = 60_000,
            compactionIntervalMs = 0,
            retirementIntervalMs = 0,
            smallFileThresholdBytes = 512L * 1024 * 1024,
        )

    @Test
    fun `instance status rolls up every catalog with its own backlog and last runs`() {
        val aId = seedCatalog("led-inst-a", head = 1, retentionSeconds = 3600)
        seedCatalog("led-inst-b")
        seedPendingFile(aId)
        ExpiryService(jdbi).runOnce("led-inst-a", 10)
        MaintenanceRunStore(jdbi).recorded("led-inst-b", MaintenanceTask.CLEANUP, MaintenanceTrigger.MANUAL) { 1 }

        // (The test database is shared per class; narrow to this test's pair.)
        val sampler =
            MaintenanceSummarySampler(
                jdbi,
                512L * 1024 * 1024,
                CompactionGrouping.DEFAULT_MIN_INPUT_FILES,
                CompactionGrouping.DEFAULT_MAX_INPUT_FILES,
                3600,
            )
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
        assertThat(b.tasks[2].lastRun?.task).isEqualTo(MaintenanceTask.CLEANUP)
        assertThat(b.tasks[2].lastRun?.catalog).isEqualTo("led-inst-b")
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

    // ---- the observed loop cadence (#114) -----------------------------------

    /**
     * Ledger rows at chosen ages, newest first: [agesSeconds] is how long
     * ago each run started. Written as SQL rather than through a service
     * because the point is the SHAPE of the history, not the work.
     */
    private fun seedRunsAgo(
        catalogId: Long,
        task: MaintenanceTask,
        trigger: MaintenanceTrigger,
        vararg agesSeconds: Long,
    ) = jdbi.useHandleUnchecked { h ->
        for (age in agesSeconds) {
            h.execute(
                """
                INSERT INTO hog_maintenance_run
                    (catalog_id, task, run_trigger, started_at, finished_at, status)
                VALUES (?, ?, ?, now() - (? * interval '1 second'),
                        now() - (? * interval '1 second'), 'ok')
                """,
                catalogId,
                task.wire,
                trigger.wire,
                age,
                age,
            )
        }
    }

    private fun taskOf(
        catalog: String,
        task: MaintenanceTask,
    ) = statusSvc().status(catalog).tasks.single { it.task == task }

    @Test
    fun `compaction cadence comes from the ledger, not the config of the process answering`() {
        val id = seedCatalog("led-cad-split")
        // The gigahog shape: a maintenance deployment sweeps every
        // minute while the API pod runs with the compaction loop off.
        seedRunsAgo(id, MaintenanceTask.COMPACTION, MaintenanceTrigger.LOOP, 5, 65, 125, 185, 245)

        val task = taskOf("led-cad-split", MaintenanceTask.COMPACTION)
        // statusSvc() is built with compactionIntervalMs = 0, so the
        // responder's own config really does say "off" — this is the
        // page that read COMPACTION DISABLED over a healthy loop (#114).
        assertThat(task.loopIntervalMs).isZero()
        assertThat(task.loop?.intervalMs)
            .describedAs("the gaps the ledger recorded, not the responder's config")
            .isBetween(59_000L, 61_000L)
        assertThat(task.loop?.lastRunAt).isNotNull()
    }

    @Test
    fun `one slow sweep does not move the cadence`() {
        val id = seedCatalog("led-cad-slow")
        // Gaps of 60, 60, 600 (a stalled sweep), 60, 60. A mean would
        // read 168s and invite someone to go looking for a problem.
        seedRunsAgo(id, MaintenanceTask.COMPACTION, MaintenanceTrigger.LOOP, 5, 65, 125, 725, 785, 845)

        assertThat(taskOf("led-cad-slow", MaintenanceTask.COMPACTION).loop?.intervalMs)
            .isBetween(59_000L, 61_000L)
    }

    @Test
    fun `a loop that stopped reports its last run and no cadence`() {
        val id = seedCatalog("led-cad-stopped")
        // A minute apart, but nothing for the last hour.
        seedRunsAgo(id, MaintenanceTask.COMPACTION, MaintenanceTrigger.LOOP, 3600, 3660, 3720, 3780)

        val task = taskOf("led-cad-stopped", MaintenanceTask.COMPACTION)
        assertThat(task.loop?.intervalMs)
            .describedAs("a cadence is a claim about now; this loop is not keeping one")
            .isNull()
        assertThat(task.loop?.lastRunAt).isNotNull()
    }

    @Test
    fun `the hydrator reports its last run but never a cadence`() {
        val id = seedCatalog("led-cad-hydrator")
        // Evenly spaced rows that are NOT evenly spaced sweeps: the
        // hydrator's sweep is instance-wide and records only for the
        // catalogs it claimed files for, so these gaps measure when work
        // arrived here. Reading them as a cadence would be a guess.
        seedRunsAgo(id, MaintenanceTask.HYDRATOR, MaintenanceTrigger.LOOP, 5, 10, 15, 20)

        val task = taskOf("led-cad-hydrator", MaintenanceTask.HYDRATOR)
        assertThat(task.loop?.intervalMs).isNull()
        assertThat(task.loop?.lastRunAt).isNotNull()
        // And it says so, so a reader knows that silence here is not
        // evidence of a stopped loop.
        assertThat(task.loop?.recordsEverySweep).isFalse()
    }

    @Test
    fun `the response says how to read its own silence`() {
        seedCatalog("led-cad-silence")
        val byTask = statusSvc().status("led-cad-silence").tasks.associateBy { it.task }
        // Same empty ledger, two meanings: nothing is compacting, versus
        // the hydrator found no work here. Without this flag a client
        // has to hardcode which tasks are which, and will be wrong the
        // first time another task joins the hydrator's pattern.
        assertThat(byTask.getValue(MaintenanceTask.COMPACTION).loop?.recordsEverySweep).isTrue()
        assertThat(byTask.getValue(MaintenanceTask.HYDRATOR).loop?.recordsEverySweep).isFalse()
        assertThat(byTask.getValue(MaintenanceTask.HYDRATOR).loop?.lastRunAt).isNull()
    }

    @Test
    fun `manual triggers are not a loop`() {
        val id = seedCatalog("led-cad-manual")
        seedRunsAgo(id, MaintenanceTask.COMPACTION, MaintenanceTrigger.MANUAL, 5, 65, 125, 185)

        val task = taskOf("led-cad-manual", MaintenanceTask.COMPACTION)
        assertThat(task.lastRun).describedAs("the runs happened").isNotNull()
        assertThat(task.loop?.intervalMs).isNull()
        assertThat(task.loop?.lastRunAt)
            .describedAs("an operator clicking the button is not a running loop")
            .isNull()
    }

    @Test
    fun `every task reports an observation, so a null loop can only mean an older server`() {
        // Verify used to be the exception here — the one task with no
        // loop, and therefore the one legitimate null — and #261 removed
        // it from the list entirely rather than leaving a null behind.
        // So EVERY task in the list reports an observation even against
        // an empty ledger, and a reader that sees null is talking to a
        // build that predates the field.
        seedCatalog("led-cad-verify")
        val tasks = statusSvc().status("led-cad-verify").tasks
        assertThat(tasks).allSatisfy { assertThat(it.loop).isNotNull() }
        val compaction = tasks.single { it.task == MaintenanceTask.COMPACTION }
        assertThat(compaction.loop?.intervalMs)
            .describedAs("an empty ledger states no cadence, which is not the same as no loop")
            .isNull()
        assertThat(compaction.loop?.recordsEverySweep).isTrue()
    }

    @Test
    fun `the instance rollup observes each catalog's own loop`() {
        val busy = seedCatalog("led-cad-inst-busy")
        seedCatalog("led-cad-inst-idle")
        seedRunsAgo(busy, MaintenanceTask.CLEANUP, MaintenanceTrigger.LOOP, 5, 65, 125, 185)

        val byName =
            statusSvc().instanceStatus(limit = 100).catalogs
                .filter { it.catalog.startsWith("led-cad-inst-") }
                .associateBy { it.catalog }

        fun cleanup(name: String) = byName.getValue(name).tasks.single { it.task == MaintenanceTask.CLEANUP }
        assertThat(cleanup("led-cad-inst-busy").loop?.intervalMs).isBetween(59_000L, 61_000L)
        val idle = cleanup("led-cad-inst-idle").loop
        assertThat(idle).isNotNull()
        assertThat(idle?.intervalMs).isNull()
        assertThat(idle?.lastRunAt).isNull()
    }

    @Test
    fun `the wire always carries the loop key, so absent means an older server`() {
        seedCatalog("led-cad-wire")
        val wire =
            wireObjectMapper().valueToTree<JsonNode>(
                statusSvc().status("led-cad-wire").toDto(),
            )
        val byTask = wire["tasks"].associateBy { it["task"].asText() }
        // Present-and-object for every task in the list; the key is
        // ALWAYS there. A client that sees NEITHER the key nor a value is
        // talking to a build from before this existed, which is a
        // different claim from "no loop runs" and must not render as one.
        assertThat(byTask.keys)
            .describedAs("#261 removed verify from the task list")
            .doesNotContain("verify")
        assertThat(byTask.values).allSatisfy { assertThat(it.has("loop")).isTrue() }
        assertThat(byTask.getValue("compaction")["loop"].isObject).isTrue()
        assertThat(byTask.getValue("compaction")["loop"]["records_every_sweep"].asBoolean()).isTrue()
        assertThat(byTask.getValue("hydrator")["loop"]["records_every_sweep"].asBoolean()).isFalse()
    }
}
