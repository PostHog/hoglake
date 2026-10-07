package com.posthog.hoglake.persistence

import com.posthog.hoglake.Database
import com.posthog.hoglake.hydrator.Hydrator
import com.posthog.hoglake.observability.CatalogMetrics
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * V26 — the three partial "attention" indexes the hydrator's claim and
 * `CatalogMetrics.SAMPLE_SQL` read the rare states through (#269), and
 * V27 — the summary's stamped live totals. The migrations RUN here
 * against rows seeded before them, so "the index arrives valid, with
 * the right predicate, and the sampler uses it" is observed rather than
 * asserted about a file; re-applying them is a no-op, as a rolled-back
 * deploy requires.
 *
 * The pending index is the one that EXISTED (since V1) and is rebuilt
 * with a liveness term: the test seeds ended pending rows, checks that
 * the old index held them and the new one does not, and that the claim
 * and the count both leave them alone.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class V26AttentionIndexesMigrationIntegrationTest {
    private val db = PgTestSupport.freshDatabaseAt("25")

    @AfterAll
    fun tearDown() = db.close()

    private data class IndexState(val valid: Boolean, val definition: String)

    private fun indexState(name: String): IndexState? =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT i.indisvalid, pg_get_indexdef(c.oid) AS def FROM pg_class c " +
                    "JOIN pg_index i ON i.indexrelid = c.oid WHERE c.relname = :n",
            ).bind("n", name).map { rs, _ -> IndexState(rs.getBoolean(1), rs.getString(2)) }.findOne().orElse(null)
        }

    private fun explainSample(): String =
        db.jdbi.inTransactionUnchecked { h ->
            h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
            h.createQuery(
                "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) " + CatalogMetrics.SAMPLE_SQL,
            )
                .mapTo(String::class.java).list().joinToString("\n")
        }

    @Test
    fun `V26 builds the attention indexes with liveness, V27 adds the totals, and a re-run is a no-op`() {
        var catalogId = 0L
        db.jdbi.useHandleUnchecked { h ->
            catalogId =
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path) VALUES ('v26', 's3://v26') RETURNING catalog_id",
                )
                    .mapTo(Long::class.java).one()
            h.execute("INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 1, 1)", catalogId)
            // Every 50th row ended; failed where g % 100 = 1, pending
            // where g % 101 = 0 (and not failed), flagged every 70th —
            // pending and flagged overlap the ended rows, which the
            // LIVE indexes must leave out.
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot, end_snapshot, path,
                                           record_count, file_size_bytes, row_id_start, stats_state, missing_field_ids)
                SELECT :c, g, 1, 1, CASE WHEN g % 50 = 0 THEN 2 ELSE NULL END, 's3://v26/' || g, 1, 1, g,
                       CASE WHEN g % 100 = 1 THEN 'failed' WHEN g % 101 = 0 THEN 'pending' ELSE 'provided' END,
                       g % 70 = 0
                  FROM generate_series(1, 20200) g
                """,
            ).bind("c", catalogId).execute()
        }
        assertThat(indexState("hog_data_file_failed")).describedAs("absent before V26").isNull()
        assertThat(indexState("hog_data_file_missing_field_ids")).isNull()
        val before = indexState("hog_data_file_pending")!!
        assertThat(before.valid).isTrue()
        assertThat(before.definition).describedAs("the pre-V26 pending index has no liveness term")
            .doesNotContain("end_snapshot")

        Database.migrate(db.dataSource)

        val failed = indexState("hog_data_file_failed")!!
        val missing = indexState("hog_data_file_missing_field_ids")!!
        val pending = indexState("hog_data_file_pending")!!
        assertThat(failed.valid).isTrue()
        assertThat(failed.definition).contains("(catalog_id, table_id)").contains("end_snapshot IS NULL")
        assertThat(missing.valid).isTrue()
        assertThat(missing.definition).contains("(catalog_id, table_id)").contains("end_snapshot IS NULL")
        assertThat(pending.valid).isTrue()
        assertThat(pending.definition)
            .contains("(catalog_id, data_file_id) INCLUDE (table_id)")
            .contains("stats_state = 'pending'").contains("end_snapshot IS NULL")
        assertThat(indexState("hog_data_file_pending_old")).describedAs("the old index is gone").isNull()
        db.jdbi.useHandleUnchecked { h -> h.execute("VACUUM ANALYZE hog_data_file") }

        // V27's columns, NULL until a publish stamps them.
        val stamped =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT count(*) FROM information_schema.columns WHERE table_name = 'hog_maintenance_summary' " +
                        "AND column_name IN ('live_files', 'live_bytes', 'live_rows', 'published_snapshot')",
                ).mapTo(Long::class.java).one()
            }
        assertThat(stamped).isEqualTo(4)

        // The sampler reads the three counts through the new indexes,
        // on rows that predate them. The planner may pick a plain or a
        // bitmap index scan for a few hundred rows; either names the
        // index. (The no-scan property with a sampled catalog beside
        // unsampled ones is CatalogMetricsPlanIntegrationTest's.)
        val plan = explainSample()
        for (index in listOf("hog_data_file_failed", "hog_data_file_missing_field_ids", "hog_data_file_pending")) {
            assertThat(
                plan,
            ).describedAs(plan).containsPattern("(Index (Only )?Scan using|Bitmap Index Scan on) $index\\b")
        }
        assertThat(plan).describedAs(plan).doesNotContain("Seq Scan on hog_data_file")
        val row =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(CatalogMetrics.SAMPLE_SQL)
                    .map { rs, _ ->
                        Triple(rs.getLong("stats_pending"), rs.getLong("stats_failed"), rs.getLong("missing_field_ids"))
                    }.one()
            }
        // Live rows of each state. Pending: 200 multiples of 101, less
        // the 2 the failed case took first (101, 10201), less the 4 that
        // are also multiples of 50 (ended) = 194. Failed: 202 rows with
        // g % 100 = 1, all odd, none ended. Flagged: 288 multiples of 70
        // less the 57 multiples of 350 (ended) = 231.
        assertThat(row).isEqualTo(Triple(194L, 202L, 231L))

        // The claim: the live pending rows only, through the rebuilt
        // index, and an ended pending row never claimed.
        val claimed =
            db.jdbi.inTransactionUnchecked { h ->
                h.createQuery(Hydrator.CLAIM_PENDING_SQL).bind("catalogId", catalogId).bind("after", Long.MIN_VALUE)
                    .bind("limit", 1000).map { rs, _ -> rs.getLong("data_file_id") }.list()
            }
        assertThat(claimed).hasSize(194)
        assertThat(claimed.none { it % 50 == 0L }).isTrue()

        // Re-appliable: a rolled-back deploy simply runs both again.
        db.jdbi.useHandleUnchecked { h -> h.execute("DELETE FROM flyway_schema_history WHERE version::numeric >= 26") }
        Database.migrate(db.dataSource)
        assertThat(indexState("hog_data_file_failed")!!.valid).isTrue()
        assertThat(indexState("hog_data_file_pending")!!.definition).isEqualTo(pending.definition)
        assertThat(indexState("hog_data_file_pending_old")).isNull()
        assertThat(
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT count(*) FROM pg_class WHERE relname LIKE 'hog_data_file_pending%'")
                    .mapTo(Long::class.java).one()
            },
        ).describedAs("exactly one pending index after a re-run").isEqualTo(1)
    }
}
