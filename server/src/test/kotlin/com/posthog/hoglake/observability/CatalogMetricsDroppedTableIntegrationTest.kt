package com.posthog.hoglake.observability

import com.posthog.hoglake.testing.ExplainPlan
import com.posthog.hoglake.testing.PgTestSupport
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * The catalog-health gauges against a DROPPED TABLE, and against the
 * cost of asking (#193).
 *
 * TWO THINGS CHANGED AT ONCE, and they are the same change. Since the
 * drop stopped end-snapshotting file rows, `end_snapshot IS NULL` is no
 * longer the whole of "live" — a dropped table's rows stay open until
 * the retirement sweep deletes them, which on a catalog with no
 * retention is never. So every `hog_data_file` gauge has to join
 * `hog_table`. Joining five times, once per correlated subquery, per
 * catalog, every 15 seconds, is what made the rewrite mandatory rather
 * than merely tidy: the old shape was measured at 1.6M buffers and
 * 2.2 s per sample at two catalogs and a 5M-row manifest.
 *
 * The buffer assertion is DERIVED from the manifest this fixture holds,
 * not written down, and it is taken from the SCAN NODES rather than the
 * plan's maximum (AGENT.md: EXPLAIN's `Planning:` block carries a
 * `Buffers:` line that dwarfs a well-indexed scan's).
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CatalogMetricsDroppedTableIntegrationTest {
    private companion object {
        /** Live files on the table that stays. */
        const val KEPT_FILES = 20_000

        /** Live files on the table that gets dropped and not yet retired. */
        const val DROPPED_FILES = 40_000

        const val ROWS_PER_FILE = 1_000L

        const val BYTES_PER_FILE = 4_096L
    }

    private val db = PgTestSupport.freshDatabase()
    private val registry = SimpleMeterRegistry()
    private val metrics by lazy { CatalogMetrics(db.jdbi, registry) }
    private var catalogId = 0L

    @AfterAll
    fun tearDown() = db.close()

    @BeforeAll
    fun seed() {
        db.jdbi.useHandleUnchecked { h ->
            catalogId =
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path, last_snapshot_id) " +
                        "VALUES ('gauge-drop', 's3://gauge-drop', 5) RETURNING catalog_id",
                ).mapTo(Long::class.java).one()
            // A neighbour catalog, so `catalog_id` in the grouped scan is
            // doing real work rather than selecting everything.
            h.execute(
                "INSERT INTO hog_catalog (name, data_path) VALUES ('gauge-drop-neighbour', 's3://n')",
            )
            h.execute("INSERT INTO hog_snapshot (catalog_id, snapshot_id, schema_version) VALUES (?, 5, 0)", catalogId)
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 1, 1)",
                catalogId,
            )
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot, dropped_snapshot) " +
                    "VALUES (?, 2, 1, 4)",
                catalogId,
            )
            val tables = listOf(Triple(1L, KEPT_FILES, 0L), Triple(2L, DROPPED_FILES, 1_000_000L))
            for ((tableId, count, base) in tables) {
                h.createUpdate(
                    """
                    INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                               path, record_count, file_size_bytes, row_id_start,
                                               stats_state, missing_field_ids)
                    SELECT :c, :base + g, :t, 1,
                           's3://gauge-drop/t' || :t || '/part-' || g || '.parquet',
                           :rows, :bytes, g, 'pending', true
                    FROM generate_series(1, :n) g
                    """,
                ).bind("c", catalogId).bind("t", tableId).bind("base", base).bind("n", count)
                    .bind("rows", ROWS_PER_FILE).bind("bytes", BYTES_PER_FILE).execute()
            }
            h.execute("VACUUM (ANALYZE) hog_data_file")
            h.execute("ANALYZE hog_table")
            h.execute("ANALYZE hog_catalog")
        }
    }

    private fun gauge(name: String): Double? = registry.find(name).tag("catalog", "gauge-drop").gauge()?.value()

    @Test
    fun `every hog_data_file gauge excludes a dropped table's still-live rows`() {
        metrics.sampleOnce()

        // MUTATION: drop the `t.dropped_snapshot IS NULL` term from any
        // one of these FILTERs and that gauge reds. Before #193 the
        // exclusion came for free from `end_snapshot IS NULL`, because
        // the drop closed every row; it does not any more, and these
        // rows are live for as long as retirement has not reached them.
        assertThat(gauge("hoglake_live_rows")).isEqualTo(KEPT_FILES * ROWS_PER_FILE.toDouble())
        assertThat(gauge("hoglake_live_bytes")).isEqualTo(KEPT_FILES * BYTES_PER_FILE.toDouble())
        assertThat(gauge("hoglake_missing_field_id_files")).isEqualTo(KEPT_FILES.toDouble())
        // stats_pending too, and for a second-order reason: the
        // HYDRATOR no longer claims a dropped table's pending files, so
        // counting them would publish a backlog nothing is draining —
        // an alert that can never clear.
        assertThat(gauge("hoglake_stats_pending_files")).isEqualTo(KEPT_FILES.toDouble())
        assertThat(gauge("hoglake_stats_failed_files")).isEqualTo(0.0)
        // The dropped table is already out of the table count, which is
        // the gauge that was always right.
        assertThat(gauge("hoglake_table_count")).isEqualTo(1.0)

        // A catalog with no file rows at all still publishes zeros: the
        // LEFT JOIN, not an inner one. A MultiGauge refreshed with
        // overwrite = true retires a series that stops appearing, so an
        // inner join would make an empty catalog look deleted.
        assertThat(registry.find("hoglake_live_rows").tag("catalog", "gauge-drop-neighbour").gauge()?.value())
            .isEqualTo(0.0)
    }

    @Test
    fun `the sample reads the manifest once, not once per counter`() {
        val plan =
            db.jdbi.inTransactionUnchecked { h ->
                // Serial: a Gather splits buffer counts across workers and
                // would make every number here depend on the core count.
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                h.createQuery(
                    "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) " +
                        CatalogMetrics.SAMPLE_SQL,
                ).mapTo(String::class.java).list().joinToString("\n")
            }

        // ONE scan of hog_data_file. The old shape had five, one per
        // correlated subquery, each re-read per catalog row — which is
        // the thing this asserts rather than the buffer count, because
        // a buffer budget alone would pass on a plan that read the
        // manifest twice on a small fixture.
        val manifestScans = plan.lines().count { Regex("""Scan.* on hog_data_file\b""").containsMatchIn(it) }
        assertThat(manifestScans)
            .describedAs("the manifest must be read once per sample, not once per counter:%n%s", plan)
            .isEqualTo(1)

        // And it must not run once per CATALOG. A correlated subquery
        // reports `loops=` equal to the number of catalog rows; the
        // grouped scan reports one.
        val manifestNode =
            ExplainPlan.nodes(plan)
                .first { Regex("""Scan.* on hog_data_file\b""").containsMatchIn(it.line) }
        assertThat(manifestNode.loops)
            .describedAs("the manifest scan must not be re-run per catalog:%n%s", plan)
            .isEqualTo(1)

        // The buffer budget, DERIVED from the relation this statement
        // has to read rather than written down: one pass over the
        // manifest's heap plus its hog_table probes, with slack. The
        // shape it excludes is the old one, which read that heap FIVE
        // times per catalog.
        val heapPages =
            db.jdbi.inTransactionUnchecked { h ->
                h.createQuery("SELECT relpages FROM pg_class WHERE relname = 'hog_data_file'")
                    .mapTo(Long::class.java).one()
            }
        val executionBuffers =
            ExplainPlan.nodes(plan.lines().takeWhile { !it.trim().startsWith("Planning:") }.joinToString("\n"))
                .maxOf { it.buffers }
        assertThat(executionBuffers)
            .describedAs(
                "one pass over a %d-page manifest; the shape this excludes reads it five times " +
                    "per catalog:%n%s",
                heapPages,
                plan,
            )
            .isLessThanOrEqualTo(heapPages * 2)

        // THE BEFORE HALF, measured on the same rows rather than quoted
        // from the review that raised it. This is the statement this
        // change replaced, verbatim minus the columns that did not move,
        // so the comparison is like for like.
        val beforePlan =
            db.jdbi.inTransactionUnchecked { h ->
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                h.createQuery(
                    "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) $fiveSubquerySql",
                ).mapTo(String::class.java).list().joinToString("\n")
            }
        val beforeBuffers =
            ExplainPlan.nodes(beforePlan.lines().takeWhile { !it.trim().startsWith("Planning:") }.joinToString("\n"))
                .maxOf { it.buffers }
        println(
            "[#193] metrics sample over a $heapPages-page manifest " +
                "(${KEPT_FILES + DROPPED_FILES} file rows, 2 catalogs): " +
                "five correlated subqueries = $beforeBuffers execution buffers, " +
                "one grouped pass = $executionBuffers",
        )
        assertThat(executionBuffers)
            .describedAs(
                "the rewrite must be cheaper than the shape it replaced, on the SAME rows:%n" +
                    "before:%n%s%nafter:%n%s",
                beforePlan,
                plan,
            )
            .isLessThan(beforeBuffers)
    }

    /**
     * The statement the one-pass sample replaced: five correlated
     * subqueries over `hog_data_file`, each re-run per catalog row.
     * Kept here, in the test, as the BEFORE half of a measurement — not
     * in production code, where it no longer exists.
     */
    private val fiveSubquerySql =
        """
        SELECT c.name,
               (SELECT count(*) FROM hog_data_file f
                 WHERE f.catalog_id = c.catalog_id AND f.stats_state = 'pending') AS stats_pending,
               (SELECT count(*) FROM hog_data_file f
                 WHERE f.catalog_id = c.catalog_id AND f.stats_state = 'failed') AS stats_failed,
               (SELECT count(*) FROM hog_data_file f
                 WHERE f.catalog_id = c.catalog_id AND f.missing_field_ids
                   AND f.end_snapshot IS NULL) AS missing_field_ids,
               (SELECT COALESCE(SUM(f.record_count), 0) FROM hog_data_file f
                 WHERE f.catalog_id = c.catalog_id AND f.end_snapshot IS NULL) AS live_rows,
               (SELECT COALESCE(SUM(f.file_size_bytes), 0) FROM hog_data_file f
                 WHERE f.catalog_id = c.catalog_id AND f.end_snapshot IS NULL) AS live_bytes
          FROM hog_catalog c
        """
}
