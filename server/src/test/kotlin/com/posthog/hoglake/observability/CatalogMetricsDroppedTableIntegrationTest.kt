package com.posthog.hoglake.observability

import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.testing.publishMaintenanceSample
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
 * Since #269 the live totals come from the maintenance summary's
 * published generation, so the fixture publishes one: the scan skips
 * the dropped table (dropped before the scan began), and the stamped
 * totals are the kept table's. The three counts still read the
 * manifest, through the attention indexes, and still need the join.
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

        /** Every Nth file on each table is pending and without field ids. */
        const val ATTENTION_EVERY = 20
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
            // One file in ATTENTION_EVERY on each table is pending and
            // without field ids — a few per cent, as a manifest with a
            // backlog looks, so the planner's choice is the attention
            // index and not (as it is when every row qualifies) a pass
            // over the table. The dropped table's share is what the
            // counts must leave out.
            val tables = listOf(Triple(1L, KEPT_FILES, 0L), Triple(2L, DROPPED_FILES, 1_000_000L))
            for ((tableId, count, base) in tables) {
                h.createUpdate(
                    """
                    INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                               path, record_count, file_size_bytes, row_id_start,
                                               stats_state, missing_field_ids)
                    SELECT :c, :base + g, :t, 1,
                           's3://gauge-drop/t' || :t || '/part-' || g || '.parquet',
                           :rows, :bytes, g,
                           CASE WHEN g % :every = 0 THEN 'pending' ELSE 'provided' END, g % :every = 0
                    FROM generate_series(1, :n) g
                    """,
                ).bind("c", catalogId).bind("t", tableId).bind("base", base).bind("n", count)
                    .bind("every", ATTENTION_EVERY)
                    .bind("rows", ROWS_PER_FILE).bind("bytes", BYTES_PER_FILE).execute()
            }
            h.execute("VACUUM (ANALYZE) hog_data_file")
            h.execute("ANALYZE hog_table")
            h.execute("ANALYZE hog_catalog")
        }
        publishMaintenanceSample(db.jdbi)
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
        assertThat(gauge("hoglake_missing_field_id_files")).isEqualTo((KEPT_FILES / ATTENTION_EVERY).toDouble())
        // stats_pending too, and for a second-order reason: the
        // HYDRATOR no longer claims a dropped table's pending files, so
        // counting them would publish a backlog nothing is draining —
        // an alert that can never clear.
        assertThat(gauge("hoglake_stats_pending_files")).isEqualTo((KEPT_FILES / ATTENTION_EVERY).toDouble())
        assertThat(gauge("hoglake_stats_failed_files")).isEqualTo(0.0)
        // The dropped table is already out of the table count, which is
        // the gauge that was always right.
        assertThat(gauge("hoglake_table_count")).isEqualTo(1.0)

        // A catalog with no file rows at all still publishes zeros once
        // its summary has published a generation: zero is what the
        // publish stamped, and the series is present. (Before that it
        // is ABSENT, with hoglake_live_totals_sampled = 0 — the Gaps
        // test pins that half.)
        assertThat(registry.find("hoglake_live_rows").tag("catalog", "gauge-drop-neighbour").gauge()?.value())
            .isEqualTo(0.0)
        assertThat(gauge("hoglake_live_totals_sampled")).isEqualTo(1.0)
        assertThat(gauge("hoglake_live_files")).isEqualTo(KEPT_FILES.toDouble())
    }

    @Test
    fun `the sample never scans the manifest, whatever a dropped table left in it`() {
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

        // #269: no pass over hog_data_file at all. The old shape read the
        // manifest once per sample (and before that, once per counter);
        // this one reads it only through the three attention indexes
        // (V26), and the live totals not at all. What a dropped table
        // leaves behind is still WALKED where it sits in one of those
        // indexes — this fixture's 2,000 pending rows on the dropped
        // table are index entries the pending count reads and the join
        // discards, index-only — but never scanned, and never summed.
        assertThat(plan)
            .describedAs("the sampler must not scan the manifest:%n%s", plan)
            .doesNotContain("Seq Scan on hog_data_file")
        val manifestIndexLines =
            plan.lines().filter {
                Regex("""Index (Only )?Scan using hog_data_file|Bitmap Index Scan on hog_data_file""")
                    .containsMatchIn(it)
            }
        assertThat(manifestIndexLines).describedAs(plan).isNotEmpty()
        for (line in manifestIndexLines) {
            assertThat(line)
                .describedAs("every manifest read must be through an attention index:%n%s", plan)
                .containsPattern("hog_data_file_(pending|failed|missing_field_ids)\\b")
        }
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
