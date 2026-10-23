package com.posthog.hoglake.service

import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * `AlterService.requireFieldIds`'s PLAN, because the count runs under
 * the per-catalog commit lock and #445 changed what it reads.
 *
 * The guard asks one question — does this table have a LIVE file the
 * hydrator flagged `missing_field_ids` — and the V26 partial index
 * `hog_data_file_missing_field_ids (catalog_id, table_id) WHERE
 * missing_field_ids AND end_snapshot IS NULL` holds exactly that
 * population. Until #445 the statement also counted `stats_state IN
 * ('pending', 'failed')`, an OR no partial index can serve (the planner
 * chose no BitmapOr): measured, it ran as an Index Scan over
 * `hog_data_file_live` on `(catalog_id, table_id)` with the OR as a
 * per-row filter — the table's whole live set, under the commit lock,
 * for every add_column and rename_column — and on this fixture's own
 * seed as a Seq Scan of the manifest (~3.4-4.3k buffers).
 *
 * What this excludes is a statement that drifts back onto the manifest
 * (a Seq Scan, or the live index with a filter), which would pass every
 * behavioural test in the suite and tax the commit lock by the size of
 * the table. The statement comes from
 * `AlterService.REQUIRE_FIELD_IDS_SQL`, the constant production issues.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AlterGuardPlanIntegrationTest {
    private companion object {
        /** The manifest: overwhelmingly `provided` and unflagged, as production is. */
        const val PROVIDED_FILES = 200_000

        /**
         * A deep deferred-stats backlog on the guarded table: what a
         * writer fleet with `deferred_stats` leaves between sweeps, and
         * what the pre-#445 statement walked.
         */
        const val PENDING_LIVE = 20_000

        /** Flagged live files on the guarded table: the guard's actual population. */
        const val FLAGGED_LIVE = 40

        /** Flagged files already ended: in the heap, out of the partial index. */
        const val FLAGGED_ENDED = 2_000

        /** Flagged live files on ANOTHER table of the catalog: the key's second column at work. */
        const val FLAGGED_OTHER_TABLE = 40
    }

    private val db = PgTestSupport.freshDatabase()
    private var catalogId = 0L

    private lateinit var plan: String

    @AfterAll
    fun tearDown() = db.close()

    @BeforeAll
    fun seed() {
        db.jdbi.useHandleUnchecked { h ->
            catalogId =
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path) VALUES ('guard-plan', 's3://guard-plan') " +
                        "RETURNING catalog_id",
                ).mapTo(Long::class.java).one()
            for (t in 1..2) {
                h.execute(
                    "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, ?, 1)",
                    catalogId,
                    t,
                )
            }

            fun files(
                tableId: Int,
                base: Long,
                n: Int,
                statsState: String,
                flagged: Boolean,
                endSnapshot: Long? = null,
            ) {
                h.createUpdate(
                    """
                    INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot, end_snapshot,
                                               path, record_count, file_size_bytes, row_id_start,
                                               stats_state, missing_field_ids)
                    SELECT :c, :base + g, :t, 1, :end, 's3://guard-plan/t' || :t || '/' || :base + g || '.parquet',
                           100, 1024, g, :state, :flagged
                    FROM generate_series(1, :n) g
                    """,
                ).bind("c", catalogId).bind("base", base).bind("t", tableId).bind("n", n)
                    .bind("end", endSnapshot).bind("state", statsState).bind("flagged", flagged)
                    .execute()
            }
            files(1, 1_000_000, PROVIDED_FILES, "provided", flagged = false)
            files(1, 3_000_000, PENDING_LIVE, "pending", flagged = false)
            files(1, 5_000_000, FLAGGED_LIVE, "provided", flagged = true)
            files(1, 6_000_000, FLAGGED_ENDED, "provided", flagged = true, endSnapshot = 3)
            files(2, 7_000_000, FLAGGED_OTHER_TABLE, "provided", flagged = true)
            h.execute("VACUUM (ANALYZE) hog_data_file")
        }
        plan =
            db.jdbi.inTransactionUnchecked { h ->
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                h.createQuery(
                    "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) " +
                        AlterService.REQUIRE_FIELD_IDS_SQL,
                ).bind("catalogId", catalogId).bind("tableId", 1L)
                    .mapTo(String::class.java).list().joinToString("\n")
            }
    }

    @Test
    fun `the guard's count is served by the missing-field-ids partial index, not the manifest`() {
        assertThat(plan)
            .describedAs(
                "the guard runs under the commit lock; a Seq Scan here is a read of the whole " +
                    "manifest per add_column/rename_column:%n%s",
                plan,
            )
            .doesNotContain("Seq Scan on hog_data_file")
        assertThat(plan)
            .describedAs("the count must be driven by the partial missing-field-ids index:%n%s", plan)
            .contains("hog_data_file_missing_field_ids")
        // Nor the pending index: the old statement never used it (an
        // OR'd predicate cannot be served by a partial index without a
        // BitmapOr), and a BitmapOr over it now would be the deep
        // deferred-stats backlog back under the lock.
        assertThat(plan)
            .describedAs("the pending backlog is not the guard's business:%n%s", plan)
            .doesNotContain("hog_data_file_pending")
        // Both key columns in the index condition, so another table's
        // flagged files are never walked.
        assertThat(plan)
            .describedAs("the probe must be on (catalog_id, table_id):%n%s", plan)
            .containsPattern(
                Regex("""Index Cond: \(\(catalog_id = '?\d+'?(::bigint)?\) AND \(table_id = '?1'?(::bigint)?\)\)""")
                    .toPattern(),
            )
    }

    @Test
    fun `the count is right and bounded by the flagged live files of the table`() {
        val count =
            db.jdbi.inTransactionUnchecked { h ->
                h.createQuery(AlterService.REQUIRE_FIELD_IDS_SQL)
                    .bind("catalogId", catalogId).bind("tableId", 1L)
                    .mapTo(Long::class.java).one()
            }
        assertThat(count).isEqualTo(FLAGGED_LIVE.toLong())
        // The rows the scan emitted are the flagged live ones of this
        // table and nothing else: no row was fetched and then filtered
        // away by a predicate the index could not answer.
        assertThat(plan)
            .describedAs("no row removed by a filter above the index:%n%s", plan)
            .doesNotContainPattern(Regex("""Rows Removed by (Filter|Index Recheck): [1-9]""").toPattern())
        println("[#445] alter guard plan:\n$plan")
    }
}
