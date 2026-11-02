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

        /** A truncated table's ended DVs (and files): the emptiness probe's worst case. */
        const val TRUNCATED_DVS = 50_000
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

    /**
     * `set_object_prefix_field`'s emptiness probe
     * ([AlterService.TABLE_HAS_LIVE_FILES_SQL]), also under the commit
     * lock: one descent of `hog_data_file_live` whether the table holds
     * 220k live files (table 1) or none (table 3) — measured 2-4 buffers
     * on this fixture, against the ~4k pages of the manifest. As a bare
     * EXISTS, table 1's probe was a Seq Scan that happened to stop at its
     * first row (2 buffers here, the whole manifest when the table's rows
     * sit at the end of the heap or were all ended by a truncate).
     */
    @Test
    fun `the object-prefix emptiness probe stops at the first live row`() {
        for (table in listOf(1L, 3L)) {
            val (probe, answer) =
                db.jdbi.inTransactionUnchecked { h ->
                    h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                    val explained =
                        h.createQuery(
                            "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) " +
                                AlterService.TABLE_HAS_LIVE_FILES_SQL,
                        ).bind("catalogId", catalogId).bind("tableId", table)
                            .mapTo(String::class.java).list().joinToString("\n")
                    val nonEmpty =
                        h.createQuery(AlterService.TABLE_HAS_LIVE_FILES_SQL)
                            .bind("catalogId", catalogId).bind("tableId", table)
                            .mapTo(Boolean::class.java).one()
                    explained to nonEmpty
                }
            assertThat(answer).describedAs("table %d", table).isEqualTo(table == 1L)
            assertThat(probe).describedAs("table %d:%n%s", table, probe).doesNotContain("Seq Scan on hog_data_file")
            assertThat(probe).contains("hog_data_file_live")
            assertThat(com.posthog.hoglake.testing.ExplainPlan.nodes(probe).first().buffers)
                .describedAs("table %d's probe buffers:%n%s", table, probe)
                .isLessThan(20)
        }
    }

    /**
     * The probe's DV half, in the one state that runs it with a big table
     * behind it: truncated (every DV and file ended, as
     * `CatalogService.truncateTable` ends them) and not yet re-analyzed,
     * so the statistics still say the table's DVs are live and common.
     * The VACUUM (no ANALYZE) clears the old row versions' index entries,
     * as any vacuum would, so what is measured is the plan the statistics
     * pick and not a first scan over dead tuples. As a bare EXISTS the DV
     * half was a Seq Scan of the whole of `hog_delete_file` here (2,041
     * buffers); in range order it is one descent of `hog_delete_file_live`.
     */
    @Test
    fun `the emptiness probe's DV half never scans hog_delete_file after a truncate`() {
        val table = 4L
        db.jdbi.useHandleUnchecked { h ->
            // The stale statistics are the point: no autovacuum may refresh them mid-test.
            h.execute("ALTER TABLE hog_data_file SET (autovacuum_enabled = false)")
            h.execute("ALTER TABLE hog_delete_file SET (autovacuum_enabled = false)")
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, ?, 1)",
                catalogId,
                table,
            )
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot, path, record_count,
                                           file_size_bytes, row_id_start, stats_state)
                SELECT :c, 8000000 + g, :t, 1, 's3://guard-plan/t4/' || g || '.parquet', 100, 1024, g, 'provided'
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("t", table).bind("n", TRUNCATED_DVS).execute()
            h.createUpdate(
                """
                INSERT INTO hog_delete_file (catalog_id, delete_file_id, table_id, data_file_id, begin_snapshot,
                                             path, delete_count, file_size_bytes)
                SELECT :c, g, :t, 8000000 + g, 2, 's3://guard-plan/t4/dv/' || md5('dv' || g) || '.puffin', 1, 64
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("t", table).bind("n", TRUNCATED_DVS).execute()
            h.execute("ANALYZE hog_delete_file")
            // The truncate: DVs first, then files, at one snapshot.
            h.execute(
                "UPDATE hog_delete_file SET end_snapshot = 5 " +
                    "WHERE catalog_id = ? AND table_id = ? AND end_snapshot IS NULL",
                catalogId,
                table,
            )
            h.execute(
                "UPDATE hog_data_file SET end_snapshot = 5 " +
                    "WHERE catalog_id = ? AND table_id = ? AND end_snapshot IS NULL",
                catalogId,
                table,
            )
            h.execute("VACUUM hog_data_file, hog_delete_file")
        }
        val (probe, answer) =
            db.jdbi.inTransactionUnchecked { h ->
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                val explained =
                    h.createQuery(
                        "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) " +
                            AlterService.TABLE_HAS_LIVE_FILES_SQL,
                    ).bind("catalogId", catalogId).bind("tableId", table)
                        .mapTo(String::class.java).list().joinToString("\n")
                val nonEmpty =
                    h.createQuery(AlterService.TABLE_HAS_LIVE_FILES_SQL)
                        .bind("catalogId", catalogId).bind("tableId", table)
                        .mapTo(Boolean::class.java).one()
                explained to nonEmpty
            }
        println("[2.1b] truncated-table emptiness probe:\n$probe")
        assertThat(answer).describedAs("a truncated table is empty").isFalse()
        assertThat(probe).describedAs("%s", probe).doesNotContain("Seq Scan on hog_delete_file")
        assertThat(probe).describedAs("%s", probe).doesNotContain("Seq Scan on hog_data_file")
        assertThat(probe).contains("hog_delete_file_live")
        assertThat(com.posthog.hoglake.testing.ExplainPlan.nodes(probe).first().buffers)
            .describedAs("the truncated table's probe buffers:%n%s", probe)
            .isLessThan(20)
        // The range spelling still answers: a live DV on an ended file
        // (an invariant break, so only by hand) makes the table non-empty.
        val withStrayDv =
            db.jdbi.inTransactionUnchecked { h ->
                h.execute(
                    "INSERT INTO hog_delete_file (catalog_id, delete_file_id, table_id, data_file_id, " +
                        "begin_snapshot, path, delete_count, file_size_bytes) " +
                        "VALUES (?, ?, ?, 8000001, 6, 's3://guard-plan/t4/dv/stray.puffin', 1, 64)",
                    catalogId,
                    TRUNCATED_DVS + 1L,
                    table,
                )
                val nonEmpty =
                    h.createQuery(AlterService.TABLE_HAS_LIVE_FILES_SQL)
                        .bind("catalogId", catalogId).bind("tableId", table)
                        .mapTo(Boolean::class.java).one()
                h.rollback()
                nonEmpty
            }
        assertThat(withStrayDv).describedAs("a live DV alone makes the table non-empty").isTrue()
    }
}
