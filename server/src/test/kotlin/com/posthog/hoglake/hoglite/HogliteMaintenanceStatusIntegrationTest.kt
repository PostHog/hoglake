package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceBacklog
import com.posthog.hoglake.model.MaintenanceRunStatus
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.persistence.PartialResult
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Instant

/** Status reads the live tenant without a registry or PostgreSQL. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteMaintenanceStatusIntegrationTest {
    private lateinit var engine: HogliteConformanceEngine
    private val store get() = engine.store

    private fun backend(source: HogliteCatalogStore = store) =
        HogliteMaintenanceStatus(source, 101, 102, 103, 104, 105, 1000)

    @BeforeAll
    fun start() {
        engine = HogliteConformanceEngine()
    }

    @AfterAll
    fun close() {
        if (::engine.isInitialized) engine.close()
    }

    private fun catalog(): String =
        HogliteConformanceEngine.uniqueName("status").also {
            store.createCatalog(it, "s3://hoglake-it/$it")
        }

    private fun write(
        cat: String,
        sql: String,
        vararg args: Any?,
    ) = store.catalogOperation(cat) {
        store.runTx(cat, HogliteCatalogStore.freshTxid("status-fixture"), null, HogliteTx().apply { add(sql, *args) })
    }

    private fun run(
        cat: String,
        task: String,
        trigger: String,
        at: Instant,
    ) {
        write(
            cat,
            "INSERT INTO maintenance_runs(task, run_trigger, started_at, finished_at, status) " +
                "VALUES (?, ?, ?, ?, 'ok')",
            task,
            trigger,
            HogliteSchema.fmt(at),
            HogliteSchema.fmt(at),
        )
    }

    @Test
    fun `empty tenant reports zero backlogs without fleet data`() {
        val cat = catalog()
        val status = backend().status(cat)
        assertThat(status.catalog).isEqualTo(cat)
        assertThat(status.sampledAt).isNull()
        assertThat(status.sampleStartedAt).isNull()
        assertThat(status.sampledSnapshotId).isNull()
        assertThat(status.sampleReanchored).isNull()
        assertThat(status.tasks.map { it.task }).containsExactly(
            MaintenanceTask.HYDRATOR,
            MaintenanceTask.EXPIRY,
            MaintenanceTask.CLEANUP,
            MaintenanceTask.COMPACTION,
            MaintenanceTask.RETIREMENT,
            MaintenanceTask.REINDEX,
        )
        assertThat(status.tasks.map { it.loopIntervalMs }).containsExactly(101L, 102L, 103L, 104L, 105L, null)
        assertThat(status.tasks.map { it.lastRun }).containsOnlyNulls()
        assertThat(status.tasks[0].backlog).isEqualTo(MaintenanceBacklog.HydratorBacklog(0, 0))
        assertThat(status.tasks[2].backlog).isEqualTo(MaintenanceBacklog.CleanupBacklog(0, null))
        assertThat(status.tasks[3].backlog).isEqualTo(MaintenanceBacklog.CompactionBacklog(0, 1000))
        assertThat(status.tasks.last().loop).isNull()
        assertThat(backend().runs(cat, null, null, 50).runs).isEmpty()
    }

    @Test
    fun `status reports live backlogs and successful failed and partial run records`() {
        val cat = catalog()
        write(
            cat,
            "INSERT INTO files(id, table_id, begin_snapshot, end_snapshot, record_count, file_size_bytes, " +
                "row_id_start, stats_state, path) VALUES " +
                "(1, 1, 0, NULL, 1, 100, 0, 'pending', 'pending'), " +
                "(2, 1, 0, NULL, 1, 200, 1, 'failed', 'failed'), " +
                "(3, 1, 0, NULL, 1, 1000, 2, 'ready', 'large'), " +
                "(4, 1, 0, 1, 1, 100, 3, 'pending', 'ended')",
        )
        val old = Instant.now().minusSeconds(120)
        write(
            cat,
            "INSERT INTO removals(path, file_kind, queued_snapshot, queued_at, claimed_by, drained_at) VALUES " +
                "('queued', 'data', 0, ?, NULL, NULL), ('claimed', 'data', 0, ?, 'worker', NULL), " +
                "('drained', 'data', 0, ?, NULL, ?)",
            HogliteSchema.fmt(old),
            HogliteSchema.fmt(old.plusSeconds(60)),
            HogliteSchema.fmt(old.minusSeconds(60)),
            HogliteSchema.fmt(old),
        )
        store.recorded(cat, MaintenanceTask.EXPIRY, MaintenanceTrigger.MANUAL) { mapOf("snapshots_expired" to 2) }
        val failure = IllegalStateException("cannot delete")
        assertThat(
            runCatching {
                store.recorded(cat, MaintenanceTask.CLEANUP, MaintenanceTrigger.MANUAL) { throw failure }
            }.exceptionOrNull(),
        ).isSameAs(failure)
        val partial =
            object : RuntimeException("partial compaction"), PartialResult {
                override val partial: Any = mapOf("groups_compacted" to 3)
            }
        assertThat(
            runCatching {
                store.recorded(cat, MaintenanceTask.COMPACTION, MaintenanceTrigger.MANUAL) { throw partial }
            }.exceptionOrNull(),
        ).isSameAs(partial)
        val tasks = backend().status(cat).tasks.associateBy { it.task }
        assertThat(tasks.getValue(MaintenanceTask.HYDRATOR).backlog).isEqualTo(MaintenanceBacklog.HydratorBacklog(1, 1))
        assertThat(
            tasks.getValue(MaintenanceTask.COMPACTION).backlog,
        ).isEqualTo(MaintenanceBacklog.CompactionBacklog(2, 1000))
        val cleanup = tasks.getValue(MaintenanceTask.CLEANUP).backlog as MaintenanceBacklog.CleanupBacklog
        assertThat(cleanup.queuedRemovals).isEqualTo(2)
        assertThat(cleanup.oldestQueuedAgeSeconds!!).isBetween(120.0, 180.0)
        assertThat(tasks.getValue(MaintenanceTask.EXPIRY).lastRun!!.status).isEqualTo(MaintenanceRunStatus.OK)
        assertThat(tasks.getValue(MaintenanceTask.CLEANUP).lastRun!!.error).isEqualTo("cannot delete")
        val recorded = tasks.getValue(MaintenanceTask.COMPACTION).lastRun!!
        assertThat(recorded.status).isEqualTo(MaintenanceRunStatus.FAILED)
        assertThat(store.json.readTree(recorded.resultJson)["groups_compacted"].asInt()).isEqualTo(3)
        val meta = store.getCatalog(cat)
        assertThat(tasks.getValue(MaintenanceTask.EXPIRY).backlog).isEqualTo(
            MaintenanceBacklog.ExpiryBacklog(
                meta.snapshotRetentionSeconds,
                meta.consumerFloor,
                meta.earliestSnapshotId,
                meta.headSnapshotId,
            ),
        )
        write(cat, "UPDATE files SET stats_state = 'ready' WHERE id = 1")
        assertThat(backend().status(cat).tasks[0].backlog).isEqualTo(MaintenanceBacklog.HydratorBacklog(0, 1))
        assertThat(backend().runs(catalog(), null, null, 50).runs).isEmpty()
    }

    @Test
    fun `run history uses exclusive ids with task filters and preserves unknown task names`() {
        val cat = catalog()
        val now = HogliteSchema.fmt(Instant.now())
        write(
            cat,
            "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i + 1 FROM n WHERE i < 505) " +
                "INSERT INTO maintenance_runs(task, run_trigger, started_at, finished_at, status) " +
                "SELECT CASE WHEN i % 2 = 0 THEN 'expiry' ELSE 'cleanup' END, 'manual', ?, ?, 'ok' FROM n",
            now,
            now,
        )
        run(cat, "future_task", "manual", Instant.now())
        run(cat, "verify", "manual", Instant.now())
        val first = backend().runs(cat, null, null, 2)
        assertThat(first.hasMore).isTrue()
        assertThat(first.runs.map { it.taskName }).containsExactly("verify", "future_task")
        val next = backend().runs(cat, null, first.runs.last().runId, 2)
        assertThat(next.runs.map { it.runId }).containsExactly(505, 504)
        assertThat(backend().runs(cat, MaintenanceTask.EXPIRY, 504, 2).runs.map { it.runId }).containsExactly(502, 500)
        assertThat(backend().runs(cat, MaintenanceTask.EXPIRY, 4, 2).hasMore).isFalse()
        assertThat(backend().runs(cat, null, null, Int.MAX_VALUE).runs).hasSize(500)
        assertThat(backend().runs(cat, null, 1, 1).runs).isEmpty()
        assertThat(backend().status(cat).tasks.map { it.task }).doesNotContain(MaintenanceTask.VERIFY)
        assertThatThrownBy { backend().runs(cat, null, null, 0) }.isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { backend().runs(cat, null, null, -1) }.isInstanceOf(HoglakeException.Validation::class.java)
    }

    @Test
    fun `loop cadence ignores manual runs and retracts stale observations`() {
        val cat = catalog()
        val now = Instant.now()
        for (seconds in listOf(180L, 120L, 60L)) {
            run(cat, "cleanup", "loop", now.minusSeconds(seconds))
            run(cat, "hydrator", "loop", now.minusSeconds(seconds))
            run(cat, "expiry", "loop", now.minusSeconds(seconds + 3600))
        }
        run(cat, "cleanup", "manual", now)
        val tasks = backend().status(cat).tasks.associateBy { it.task }
        val cleanup = tasks.getValue(MaintenanceTask.CLEANUP)
        assertThat(cleanup.lastRun!!.trigger).isEqualTo(MaintenanceTrigger.MANUAL)
        assertThat(cleanup.loop!!.intervalMs).isEqualTo(60_000)
        assertThat(cleanup.loop!!.lastRunAt).isEqualTo(HogliteSchema.parse(HogliteSchema.fmt(now.minusSeconds(60))))
        assertThat(tasks.getValue(MaintenanceTask.HYDRATOR).loop!!.intervalMs).isNull()
        assertThat(tasks.getValue(MaintenanceTask.HYDRATOR).loop!!.recordsEverySweep).isFalse()
        assertThat(tasks.getValue(MaintenanceTask.EXPIRY).loop!!.intervalMs).isNull()
        assertThat(tasks.getValue(MaintenanceTask.EXPIRY).loop!!.lastRunAt).isNotNull()
    }

    @Test
    fun `older schemas report unknown counts instead of walking the manifest`() {
        val cat = catalog()
        run(cat, "cleanup", "manual", Instant.now())
        for (index in listOf("files_live_stats", "files_live_size", "removals_queued_at", "maintenance_runs_loop")) {
            write(cat, "DROP INDEX $index")
        }
        HogliteInterposer(engine.url()).use { proxy ->
            val status = backend(proxy.store).status(cat)
            assertThat(status.tasks[2].lastRun!!.task).isEqualTo(MaintenanceTask.CLEANUP)
            assertThat(backend(proxy.store).runs(cat, null, null, 50).runs).hasSize(1)
            assertThat(status.tasks[0].backlog).isEqualTo(MaintenanceBacklog.HydratorBacklog(null, null))
            assertThat(status.tasks[2].backlog).isEqualTo(MaintenanceBacklog.CleanupBacklog(null, null))
            assertThat(status.tasks[3].backlog).isEqualTo(MaintenanceBacklog.CompactionBacklog(null, 1000))
            assertThat(
                proxy.requests.joinToString(),
            ).doesNotContain("FROM files", "FROM removals", "run_trigger = 'loop'")
        }
    }

    @Test
    fun `status and history reject missing catalogs and mismatched tenant identities`() {
        val missing = HogliteConformanceEngine.uniqueName("absent")
        assertThatThrownBy { backend().status(missing) }.isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy {
            backend().runs(
                missing,
                null,
                null,
                1,
            )
        }.isInstanceOf(HoglakeException.NotFound::class.java)
        val cat = catalog()
        val wrong =
            HogliteCatalogStore(
                engine.client,
                HogliteTenantResolver { HogliteConformanceEngine.TENANTS.tenantFor(cat) },
            )
        assertThatThrownBy { backend(wrong).status("wrong") }.isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy {
            backend(
                wrong,
            ).runs("wrong", null, null, 1)
        }.isInstanceOf(HoglakeException.NotFound::class.java)
    }

    @Test
    fun `production status and history statements use indexed ranges on a large tenant`() {
        val cat = catalog()
        write(
            cat,
            "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i + 1 FROM n WHERE i < 10000) " +
                "INSERT INTO files(id, table_id, begin_snapshot, end_snapshot, record_count, file_size_bytes, " +
                "row_id_start, stats_state, path) SELECT i, 1, 0, CASE WHEN i < 9990 THEN 1 END, " +
                "1, 2000, i, 'ready', 'file-' || i FROM n",
        )
        val now = HogliteSchema.fmt(Instant.now())
        write(
            cat,
            "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i + 1 FROM n WHERE i < 10000) " +
                "INSERT INTO maintenance_runs(task, run_trigger, started_at, finished_at, status) " +
                "SELECT 'cleanup', 'manual', ?, ?, 'ok' FROM n",
            now,
            now,
        )
        write(
            cat,
            "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i + 1 FROM n WHERE i < 10000) " +
                "INSERT INTO removals(path, file_kind, queued_snapshot, queued_at, drained_at) " +
                "SELECT 'object-' || i, 'data', 0, ?, CASE WHEN i < 9990 THEN ? END FROM n",
            now,
            now,
        )
        run(cat, "cleanup", "loop", Instant.now())
        HogliteInterposer(engine.url()).use { proxy ->
            backend(proxy.store).status(cat)
            backend(proxy.store).runs(cat, null, 5000, 2)
            backend(proxy.store).runs(cat, MaintenanceTask.CLEANUP, 5000, 2)
            assertThat(proxy.requests).hasSize(4)
            val statements =
                proxy.requests.flatMap { body ->
                    val root = store.json.readTree(body)
                    root["statements"]?.toList() ?: listOf(root)
                }.filter { it.has("sql") }
            val plans =
                statements.map { statement ->
                    val sql = statement["sql"].asText()
                    val args = statement["args"]?.map { store.json.convertValue(it, Any::class.java) }.orEmpty()
                    val plan =
                        store.catalogOperation(cat) {
                            store.queryCatalog(
                                cat,
                                "EXPLAIN QUERY PLAN $sql",
                                *args.toTypedArray(),
                            ).rows.joinToString { it.last().asString() }
                        }
                    assertThat(
                        plan,
                    ).describedAs(sql).doesNotContain("TEMP B-TREE", "SCAN files", "SCAN maintenance_runs")
                    if ("run_trigger = 'loop'" in sql) assertThat(plan).contains("maintenance_runs_loop")
                    if ("stats_state = 'pending'" in sql) assertThat(plan).contains("files_live_stats")
                    if ("file_size_bytes < ?" in sql) assertThat(plan).contains("files_live_size")
                    if ("FROM removals" in sql) assertThat(plan).contains("removals_queued_at")
                    if ("id < ?" in sql) assertThat(plan).contains("id<?")
                    plan
                }
            assertThat(plans).hasSizeGreaterThan(10)
            assertThat(statements.count { it["sql"].asText().contains("id < ?") }).isEqualTo(2)
        }
    }
}
