package com.posthog.hoglake.observability

import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.testing.ExplainPlan
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.statement.Query
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Plans of the four extended catalog-metrics statements on a
 * PRODUCTION-SHAPED fixture (AGENT.md, scale doctrine), each against the
 * worst case it has to survive:
 *
 *  - catalog `prod` (one dominant catalog, as gigahog-prod-us is):
 *    [LIVE_TABLES] live tables, [DROPPED_TABLES] dropped ones of which
 *    [PENDING_DROPS] still hold a live file; [DATA_FILES] live data files
 *    plus one per pending drop; [DELETE_FILES] DV rows of which
 *    [LIVE_DVS] are live; [SNAPSHOTS] snapshots; a published summary
 *    generation in which ONE table has [BUCKETS] buckets, with an
 *    in-flight generation of the same size beside it; [LEDGER_ROWS]
 *    ledger rows on ONE task (`expiry`) and a few hundred on the others;
 *  - `small`: a published generation, a handful of tables and
 *    [SMALL_LEDGER_ROWS] ledger rows OLDER than all of `prod`'s. So few
 *    that ANALYZE sees one distinct `catalog_id` in the ledger, which is
 *    what makes the planner walk the one-step window by task and filter
 *    on catalog (the review reproduced it on PG 16 and 18; with 100 rows
 *    per small catalog n_distinct was 3 and the hazard hid);
 *  - `unsampled`: tables and files, no published generation;
 *  - `empty`: nothing.
 *
 * Every statement runs serially (`max_parallel_workers_per_gather = 0`)
 * with its production parameters. Assertions are on the SHAPE that
 * bounds the work (which index, which columns are in the Index Cond,
 * how many probes, no rows removed by a filter) and on execution
 * buffers taken from the plan's nodes, never the Planning block. The
 * measured figures are printed and recorded in each statement's KDoc.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CatalogMetricsPlanIntegrationTest {
    private companion object {
        const val LIVE_TABLES = 40

        /** Past the probe cap, so `probes <= cap` separates capped from uncapped. */
        const val DROPPED_TABLES = CatalogMetrics.DROPPED_TABLE_PROBE_CAP + 1_000
        const val PENDING_DROPS = 50
        const val DATA_FILES = 200_000
        const val DELETE_FILES = 100_000
        const val LIVE_DVS = 5_000
        const val SNAPSHOTS = 50_000
        const val BUCKETS = 50_000
        const val LEDGER_ROWS = 500_000
        const val OTHER_TASK_ROWS = 300
        const val SMALL_LEDGER_ROWS = 3
    }

    private val db = PgTestSupport.freshDatabase()

    @AfterAll
    fun tearDown() = db.close()

    @BeforeAll
    fun seed() {
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_catalog (name, data_path, last_snapshot_id, earliest_snapshot_id) VALUES " +
                    "('prod', 's3://prod', $SNAPSHOTS, 1), ('small', 's3://small', 10, 1), " +
                    "('unsampled', 's3://unsampled', 10, 1), ('empty', 's3://empty', 0, 0)",
            )
            // catalog ids 1..4 in that order.
            h.execute(
                "INSERT INTO hog_snapshot (catalog_id, snapshot_id, schema_version, snapshot_time) " +
                    "SELECT 1, g, 0, now() - make_interval(secs => $SNAPSHOTS - g) " +
                    "FROM generate_series(1, $SNAPSHOTS) g",
            )
            h.execute(
                "INSERT INTO hog_snapshot (catalog_id, snapshot_id, schema_version) " +
                    "SELECT c, g, 0 FROM generate_series(2, 3) c, generate_series(1, 10) g",
            )
            h.execute(
                "INSERT INTO hog_namespace (catalog_id, namespace_id, name) " +
                    "SELECT c, 1, 'ns' FROM generate_series(1, 3) c",
            )
            val tables = LIVE_TABLES + DROPPED_TABLES
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot, dropped_snapshot) " +
                    "SELECT 1, g, 1, CASE WHEN g > $LIVE_TABLES THEN 2 END FROM generate_series(1, $tables) g",
            )
            h.execute(
                "INSERT INTO hog_table_version " +
                    "(catalog_id, table_id, begin_snapshot, end_snapshot, namespace_id, name) " +
                    "SELECT 1, g, 1, CASE WHEN g > $LIVE_TABLES THEN 2 END, 1, 't' || g " +
                    "FROM generate_series(1, $tables) g",
            )
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) " +
                    "SELECT c, g, 1 FROM generate_series(2, 3) c, generate_series(1, 5) g",
            )
            h.execute(
                "INSERT INTO hog_table_version (catalog_id, table_id, begin_snapshot, namespace_id, name) " +
                    "SELECT c, g, 1, 1, 't' || g FROM generate_series(2, 3) c, generate_series(1, 5) g",
            )
            // Live files: 80% on table 1 (the events table), the rest
            // spread over the other live tables; then one live file on
            // each of the OLDEST pending drops (low table ids, so the
            // newest-first probe has to walk the drained ones above them).
            h.execute(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, row_id_start)
                SELECT 1, g, CASE WHEN g <= ${DATA_FILES * 4 / 5} THEN 1 ELSE 2 + (g % ${LIVE_TABLES - 1}) END,
                       1, 's3://prod/f' || g, 1000, 40000, g
                  FROM generate_series(1, $DATA_FILES) g
                """,
            )
            h.execute(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, row_id_start)
                SELECT 1, $DATA_FILES + g, $LIVE_TABLES + g, 1, 's3://prod/d' || g, 10, 100, 0
                  FROM generate_series(1, $PENDING_DROPS) g
                """,
            )
            h.execute(
                "INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot, path, " +
                    "record_count, file_size_bytes, row_id_start) " +
                    "SELECT c, g, 1, 1, 's3://u/' || g, 1, 1, g " +
                    "FROM generate_series(2, 3) c, generate_series(1, 100) g",
            )
            // DVs: the live ones scattered, the rest ended (superseded).
            h.execute(
                """
                INSERT INTO hog_delete_file (catalog_id, delete_file_id, table_id, data_file_id, begin_snapshot,
                                             end_snapshot, path, delete_count, file_size_bytes)
                SELECT 1, g, 1, g, 1,
                       CASE WHEN g % ${DELETE_FILES / LIVE_DVS} = 0 THEN NULL ELSE 3 END,
                       's3://prod/dv' || g, 1, 100
                  FROM generate_series(1, $DELETE_FILES) g
                """,
            )
            // Summary: `prod` and `small` published generation 1 with
            // generation 2 in flight; `unsampled` has a row that never
            // published.
            h.execute(
                "INSERT INTO hog_maintenance_summary (catalog_id, generation, published_generation, " +
                    "measures_generation, sampled_at, sample) " +
                    "SELECT c, 2, 1, 1, now(), '{\"snapshotId\": 5}' FROM generate_series(1, 2) c",
            )
            h.execute("INSERT INTO hog_maintenance_summary (catalog_id) VALUES (3)")
            h.execute(
                """
                INSERT INTO hog_maintenance_summary_tier (catalog_id, generation, bucket_key, table_id,
                    partition_values, quota, remaining, pending, file_count, small_count, total_bytes,
                    small_bytes, dv_count, record_count)
                SELECT 1, gen, gen || '-' || g, CASE WHEN g <= $BUCKETS THEN 1 ELSE 2 + (g % ${LIVE_TABLES - 1}) END,
                       ARRAY[g::text], 1, 1, 0, 3, 3, 120000, 120000, 0, 3000
                  FROM generate_series(1, $BUCKETS + 1000) g, generate_series(1, 2) gen
                """,
            )
            h.execute(
                "INSERT INTO hog_maintenance_summary_tier (catalog_id, generation, bucket_key, table_id, quota, " +
                    "remaining, pending, file_count) SELECT 2, gen, gen || '-' || g, g, 1, 1, 0, 2 " +
                    "FROM generate_series(1, 5) g, generate_series(1, 2) gen",
            )
            // Ledger: `small`'s $SMALL_LEDGER_ROWS rows first (older run
            // ids; `unsampled` and `empty` have none), then
            // ONE task with $LEDGER_ROWS rows on `prod` (never failed, so
            // "the newest failed" would walk all of it) and a few hundred
            // on its other tasks. Newest-first by run_id, `prod`'s rows
            // therefore sit in front of every other catalog's in
            // `hog_maintenance_run_task_history (task, run_id DESC)` —
            // the order that index must be walked through, filtering
            // `catalog_id`, to reach them.
            h.execute(
                """
                INSERT INTO hog_maintenance_run (catalog_id, task, run_trigger, started_at, finished_at, status)
                SELECT 2, 'expiry', 'loop', now() - interval '30 days', now() - interval '30 days', 'ok'
                  FROM generate_series(1, $SMALL_LEDGER_ROWS) g
                """,
            )
            h.execute(
                """
                INSERT INTO hog_maintenance_run (catalog_id, task, run_trigger, started_at, finished_at, status)
                SELECT 1, 'expiry', 'loop', now() - make_interval(secs => g * 15),
                       now() - make_interval(secs => g * 15) + interval '1 second', 'ok'
                  FROM generate_series(1, $LEDGER_ROWS) g
                """,
            )
            h.execute(
                """
                INSERT INTO hog_maintenance_run (catalog_id, task, run_trigger, started_at, finished_at, status)
                SELECT 1, t, 'loop', now() - make_interval(secs => g * 60),
                       now() - make_interval(secs => g * 60) + interval '1 second', 'ok'
                  FROM unnest(ARRAY['hydrator', 'cleanup', 'compaction', 'retirement']) t,
                       generate_series(1, $OTHER_TASK_ROWS) g
                """,
            )
            // PIN the ledger's `catalog_id` n_distinct at 1 rather than
            // leave it to ANALYZE's sample: 30,000 rows out of ~501,000
            // include one of `small`'s $SMALL_LEDGER_ROWS roughly one run
            // in three, which makes n_distinct 2, rates `catalog_id` as
            // selective and plans the one-step form through
            // hog_maintenance_run_recent — and the BEFORE half below then
            // fails to show the hazard it exists to show. The pinned value
            // is the production shape the KDoc above describes, now
            // guaranteed rather than probable.
            h.execute("ALTER TABLE hog_maintenance_run ALTER COLUMN catalog_id SET (n_distinct = 1)")
            h.execute("VACUUM ANALYZE")
        }
    }

    private fun explain(
        sql: String,
        bind: (Query) -> Query = { it },
    ): String =
        db.jdbi.inTransactionUnchecked { h ->
            h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
            bind(h.createQuery("EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) $sql"))
                .mapTo(String::class.java)
                .list()
                .joinToString("\n")
        }

    /** Execution part of the plan only: the `Planning:` block carries its own Buffers line. */
    private fun execution(plan: String) =
        plan.lines().takeWhile { !it.trim().startsWith("Planning:") }.joinToString("\n")

    /**
     * SHARED buffers (hit + read) of the root node — the whole
     * execution. Parsed here rather than through `ExplainPlan.nodes`,
     * whose `\bread=` also matches a `temp read=` on a line with no
     * shared read, which a spilling sort produces.
     */
    private fun executionBuffers(plan: String): Long {
        val line = execution(plan).lines().first { it.trim().startsWith("Buffers:") }
        val shared = Regex("""shared hit=(\d+)(?: read=(\d+))?""").find(line) ?: return 0
        return shared.groupValues[1].toLong() + (shared.groupValues[2].ifEmpty { "0" }).toLong()
    }

    /** The detail lines (Index Cond, Filter, ...) of the first node whose line matches [node]. */
    private fun detail(
        plan: String,
        node: Regex,
    ): List<String> {
        val lines = plan.lines()
        val i = lines.indexOfFirst { node.containsMatchIn(it) }
        check(i >= 0) { "no node matching $node in:\n$plan" }

        fun indent(l: String) = l.length - l.trimStart().length
        return lines.drop(i + 1).takeWhile { indent(it) > indent(lines[i]) && !it.contains("->") }
    }

    private fun relpages(relation: String): Long =
        db.jdbi.inTransactionUnchecked { h ->
            h.createQuery("SELECT relpages FROM pg_class WHERE relname = :r").bind("r", relation)
                .mapTo(Long::class.java).one()
        }

    /**
     * The statement MAINTENANCE_RUNS_SQL replaced: the window as one
     * ordered LIMIT over the table. Kept here as the BEFORE half of a
     * measurement, not in production code.
     */
    private val oneStepMaintenanceSql =
        """
        SELECT c.name AS catalog, tasks.task, w.ok_at, w.failed_at, w.last_duration
          FROM hog_catalog c
          CROSS JOIN unnest(:tasks::text[]) AS tasks(task)
          CROSS JOIN LATERAL (
              SELECT extract(epoch FROM max(r.finished_at) FILTER (WHERE r.status = 'ok')) AS ok_at,
                     extract(epoch FROM max(r.finished_at) FILTER (WHERE r.status = 'failed')) AS failed_at,
                     (array_agg(extract(epoch FROM (r.finished_at - r.started_at))
                                ORDER BY r.run_id DESC))[1] AS last_duration
                FROM (SELECT r.run_id, r.status, r.started_at, r.finished_at
                        FROM hog_maintenance_run r
                       WHERE r.catalog_id = c.catalog_id AND r.task = tasks.task
                       ORDER BY r.run_id DESC
                       LIMIT :lookback) r
          ) w
         WHERE w.last_duration IS NOT NULL
        """

    @Test
    fun `MAINTENANCE_RUNS_SQL reads the window index-only on both columns, whatever the skew`() {
        val plan =
            explain(CatalogMetrics.MAINTENANCE_RUNS_SQL) {
                it.bindArray("tasks", String::class.java, MaintenanceTask.entries.map { t -> t.wire })
                    .bind("lookback", CatalogMetrics.MAINTENANCE_RUN_LOOKBACK)
            }
        val buffers = executionBuffers(plan)
        println(
            "[metrics-plan] MAINTENANCE_RUNS_SQL: $LEDGER_ROWS-row task, ${execution(
                plan,
            ).length} chars, $buffers buffers\n$plan",
        )

        // The window: an index-only read of hog_maintenance_run_recent
        // with BOTH catalog_id and task in the Index Cond. The shape this
        // replaced used hog_maintenance_run_task_history and FILTERED on
        // catalog_id, removing the dominant catalog's rows on every loop.
        val window = detail(plan, Regex("""Index Only Scan using hog_maintenance_run_recent"""))
        val cond = window.first { it.trim().startsWith("Index Cond:") }
        assertThat(cond).describedAs(plan).contains("catalog_id").contains("task")
        assertThat(plan).describedAs(plan).doesNotContain("hog_maintenance_run_task_history")
        assertThat(execution(plan)).describedAs(plan).doesNotContain("Seq Scan on hog_maintenance_run")
        // No ledger scan throws rows away. (The Aggregate above them may:
        // its `last_duration IS NOT NULL` drops the empty pairs.)
        val ledgerScans = Regex("""Scan.* on hog_maintenance_run\b""")
        for (scan in execution(plan).lines().filter { ledgerScans.containsMatchIn(it) }) {
            assertThat(detail(plan, Regex(Regex.escape(scan.trim()))).none { it.contains("Rows Removed by Filter") })
                .describedAs("%s filters rows:%n%s", scan.trim(), plan)
                .isTrue()
        }

        // THE BEFORE HALF, on the same rows: the one-step window (an
        // ordered LIMIT over the table itself). On this fixture it walks
        // hog_maintenance_run_task_history (task, run_id DESC) and FILTERS
        // catalog_id — the hazard the two-step form exists to remove.
        // Asserted, so the fixture provably exhibits it; a fixture on
        // which the old form plans well proves nothing about the new one.
        val before =
            explain(oneStepMaintenanceSql) {
                it.bindArray("tasks", String::class.java, MaintenanceTask.entries.map { t -> t.wire })
                    .bind("lookback", CatalogMetrics.MAINTENANCE_RUN_LOOKBACK)
            }
        val beforeBuffers = executionBuffers(before)
        println("[metrics-plan] MAINTENANCE_RUNS_SQL before (one-step window): $beforeBuffers buffers\n$before")
        val beforeScan = detail(before, Regex("""Index Scan using hog_maintenance_run_task_history"""))
        assertThat(beforeScan.any { it.contains("Rows Removed by Filter") })
            .describedAs("the one-step form must show the hazard on this fixture:%n%s", before)
            .isTrue()

        // Budget DERIVED from the window: the rows the windows can hold
        // on this fixture, at 12 buffers per window row — room for PG
        // 16, where `run_id = ANY(...)` is one PK descent per id (~4 per
        // row), and well below the one-step form's filtered walk.
        val windowRows =
            minOf(LEDGER_ROWS, CatalogMetrics.MAINTENANCE_RUN_LOOKBACK) +
                4 * minOf(OTHER_TASK_ROWS, CatalogMetrics.MAINTENANCE_RUN_LOOKBACK) +
                SMALL_LEDGER_ROWS
        val budget = 12L * windowRows
        println("[metrics-plan] MAINTENANCE_RUNS_SQL budget: $windowRows window rows x 12 = $budget")
        assertThat(buffers).describedAs(plan).isLessThanOrEqualTo(budget)
        assertThat(beforeBuffers).describedAs("the budget must exclude the hazard:%n%s", before).isGreaterThan(budget)
    }

    @Test
    fun `TABLE_SQL reads only the published generation's buckets through the tier index`() {
        val plan = explain(CatalogMetrics.TABLE_SQL) { it.bind("cap", CatalogMetrics.MAX_TABLE_SERIES) }
        val buffers = executionBuffers(plan)
        val tierPages = relpages("hog_maintenance_summary_tier")
        println(
            "[metrics-plan] TABLE_SQL: $BUCKETS-bucket table, tier relation $tierPages pages, $buffers buffers\n$plan",
        )

        // The tier is read ONCE (one scan node, one loop) — the published
        // generation is half the relation here (production keeps the
        // published one plus at most one in flight), so the planner may
        // choose a sequential pass over an index; what must not happen is
        // a pass per table or per catalog. Nothing reads the manifest.
        val tierScans =
            ExplainPlan.nodes(execution(plan)).filter {
                Regex("""Scan.* on hog_maintenance_summary_tier\b""").containsMatchIn(it.line)
            }
        assertThat(tierScans).describedAs(plan).hasSize(1)
        assertThat(tierScans.single().loops).describedAs(plan).isEqualTo(1)
        assertThat(plan).describedAs(plan).doesNotContain("hog_data_file")
        // One pass over the tier plus the hog_table/version/namespace
        // reads, derived from the relations rather than written down.
        val budget = tierPages + relpages("hog_table") + relpages("hog_table_version") + 200
        assertThat(buffers).describedAs(plan).isLessThanOrEqualTo(budget)
    }

    @Test
    fun `LIFECYCLE_SQL probes at most the cap of dropped tables, one descent each`() {
        val plan = explain(CatalogMetrics.LIFECYCLE_SQL) { it.bind("probeCap", CatalogMetrics.DROPPED_TABLE_PROBE_CAP) }
        val buffers = executionBuffers(plan)
        println("[metrics-plan] LIFECYCLE_SQL: $DROPPED_TABLES dropped tables, $buffers buffers\n$plan")

        val probeLine = execution(plan).lines().first { it.contains("on hog_data_file") }
        assertThat(probeLine).describedAs(plan).contains("hog_data_file_live")
        val probes = ExplainPlan.nodes(execution(plan)).first { it.line.contains("on hog_data_file") }.loops
        // $DROPPED_TABLES drops, so exactly the cap is probed; an uncapped
        // walk would probe all of them.
        assertThat(probes).describedAs(plan).isEqualTo(CatalogMetrics.DROPPED_TABLE_PROBE_CAP.toLong())
        assertThat(execution(plan)).describedAs(plan).doesNotContain("Seq Scan on hog_data_file")
        // The age probe is one descent of the snapshot PK, not a scan of
        // the 50k retained snapshots.
        assertThat(execution(plan)).describedAs(plan).doesNotContain("Seq Scan on hog_snapshot")
        // ~2 buffers per probe (index-only, all-visible) plus the
        // dropped-table walk; 4 per probe is the ceiling.
        assertThat(buffers).describedAs(plan).isLessThanOrEqualTo(4L * CatalogMetrics.DROPPED_TABLE_PROBE_CAP + 1_000)
    }

    @Test
    fun `DELETE_FILES_SQL reads live DVs only, never the manifest`() {
        val plan = explain(CatalogMetrics.DELETE_FILES_SQL)
        val buffers = executionBuffers(plan)
        val dvPages = relpages("hog_delete_file")
        println(
            "[metrics-plan] DELETE_FILES_SQL: $DELETE_FILES DVs ($LIVE_DVS live), " +
                "relation $dvPages pages, $buffers buffers\n$plan",
        )
        assertThat(plan).describedAs(plan).doesNotContain("hog_data_file")
        // Never more than one pass over the DV relation.
        assertThat(buffers).describedAs(plan).isLessThanOrEqualTo(dvPages + 1_000)
    }

    @Test
    fun `SAMPLE_SQL's oldest snapshot is one descent, not a scan of the retained snapshots`() {
        val plan = explain(CatalogMetrics.SAMPLE_SQL)
        println("[metrics-plan] SAMPLE_SQL: $SNAPSHOTS snapshots\n$plan")
        assertThat(execution(plan)).describedAs(plan).doesNotContain("Seq Scan on hog_snapshot")
        // One PK descent per catalog: <= 4 buffers per loop.
        val snapshotNodes = ExplainPlan.nodes(execution(plan)).filter { it.line.contains("on hog_snapshot") }
        for (node in snapshotNodes) {
            assertThat(node.buffers).describedAs(plan).isLessThanOrEqualTo(4 * node.loops)
        }
    }
}
