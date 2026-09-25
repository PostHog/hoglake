package com.posthog.hoglake.hydrator

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
 * `Hydrator.claimPending`'s PLAN, because #193 added a join to it.
 *
 * The claim is the head of the hydrator's sweep and it runs instance-
 * wide, unfiltered by catalog: it asks for the globally oldest
 * `pending` files and takes row locks on them. Before #193 it was a
 * single-table read driven by `hog_data_file_pending`, the partial
 * index on `stats_state = 'pending'`; it now joins `hog_table` so a
 * dropped table's pending rows are never claimed.
 *
 * A JOIN ADDED TO A HOT LOOP WITH NO PLAN TEST is how an index stops
 * being used without anything saying so. The failure this excludes is
 * specific and plausible: the planner deciding to lead with
 * `hog_table` — which is SMALL, so it looks cheap — and hash-joining a
 * SEQUENTIAL SCAN of the whole manifest against it. That plan is
 * correct, passes every behavioural test in the suite, and turns a
 * 15-minute background sweep into a full read of `hog_data_file` on an
 * instance where the pending queue is normally EMPTY.
 *
 * The statement comes from `Hydrator.CLAIM_PENDING_SQL`, the constant
 * production issues, not a copy of it.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HydratorClaimPlanIntegrationTest {
    private companion object {
        /**
         * The manifest. Overwhelmingly `provided`, as production is —
         * every writer in the fleet ships its own footer, so the
         * pending queue is empty in the steady state and the whole
         * point of the partial index is that the sweep pays for the
         * queue rather than for the table.
         */
        const val PROVIDED_FILES = 200_000

        /** Pending files on the live table: the sweep's actual work. */
        const val PENDING_LIVE = 40

        /** Pending files on a DROPPED table: what the join must exclude. */
        const val PENDING_DROPPED = 20_000

        /**
         * Tables in the catalog besides the two the files belong to.
         * A Portola-shaped namespace holds 54,000; what this needs is
         * enough that `hog_table` is not a relation the planner can
         * materialise for free.
         */
        const val OTHER_TABLES = 5_000

        /** The sweep's limit, the statement's own LIMIT. */
        const val LIMIT = 100
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
                    "INSERT INTO hog_catalog (name, data_path) VALUES ('hyd-plan', 's3://hyd-plan') " +
                        "RETURNING catalog_id",
                ).mapTo(Long::class.java).one()
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 1, 1)",
                catalogId,
            )
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot, dropped_snapshot) " +
                    "VALUES (?, 2, 1, 5)",
                catalogId,
            )
            // A REALISTIC hog_table, and this is load-bearing rather
            // than scenery. With two rows the planner materialises the
            // whole relation and joins with a Join Filter -- a correct
            // plan, and a DIFFERENT one from what any real catalog
            // gets. A Portola-shaped namespace holds 54,000 tables; at
            // that size the only sane inner side is a primary-key
            // probe, and that is the plan this file has to pin. A
            // two-row fixture would have made the assertion measure the
            // fixture, which is how V16's first measurement went wrong.
            h.createUpdate(
                """
                INSERT INTO hog_table (catalog_id, table_id, created_snapshot)
                SELECT :c, 1000 + g, 1 FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("n", OTHER_TABLES).execute()
            // The bulk: `provided`, so the partial index excludes every
            // one of them and a plan that reads them is reading the
            // whole manifest for nothing.
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, row_id_start,
                                           stats_state)
                SELECT :c, 1000000 + g, 1, 1, 's3://hyd-plan/t1/part-' || g || '.parquet',
                       100, 1024, g, 'provided'
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("n", PROVIDED_FILES).execute()
            // The DROPPED table's pending backlog sits at the HEAD of
            // (catalog_id, data_file_id), which is the order the claim
            // takes: without the join it is what a LIMIT 100 sweep
            // picks, forever.
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, row_id_start,
                                           stats_state)
                SELECT :c, g, 2, 1, 's3://hyd-plan/t2/part-' || g || '.parquet',
                       100, 1024, g, 'pending'
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("n", PENDING_DROPPED).execute()
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, row_id_start,
                                           stats_state)
                SELECT :c, 9000000 + g, 1, 1, 's3://hyd-plan/t1/pending-' || g || '.parquet',
                       100, 1024, g, 'pending'
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("n", PENDING_LIVE).execute()
            h.execute("VACUUM (ANALYZE) hog_data_file")
            h.execute("ANALYZE hog_table")
        }
        plan =
            db.jdbi.inTransactionUnchecked { h ->
                // Serial: a Gather reports its workers' scans with
                // `loops=` equal to the worker count and splits the
                // buffer counts across them, which would make every
                // number here depend on the machine's core count.
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                h.createQuery(
                    "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) " +
                        Hydrator.CLAIM_PENDING_SQL,
                ).bind("limit", LIMIT).mapTo(String::class.java).list().joinToString("\n")
            }
    }

    private fun detailOf(
        relation: String,
        prefix: String,
    ): Long? {
        val lines = plan.lines()

        fun indent(l: String) = l.length - l.trimStart().length
        val i = lines.indexOfFirst { Regex("""Scan.* on $relation\b""").containsMatchIn(it) }
        if (i < 0) throw AssertionError("no scan of $relation in:\n$plan")
        return lines.drop(i + 1)
            .takeWhile { it.isNotBlank() && indent(it) > indent(lines[i]) }
            .firstOrNull { it.trim().startsWith(prefix) }
            ?.let { Regex("""(\d+)""").find(it.substringAfter(prefix))?.value?.toLong() }
    }

    @Test
    fun `the claim is still driven by the pending partial index, not by the join`() {
        assertThat(plan)
            .describedAs(
                "the sweep must pay for the QUEUE, not for the manifest; a Seq Scan here is a " +
                    "full read of hog_data_file every %s:%n%s",
                "interval",
                plan,
            )
            .doesNotContain("Seq Scan on hog_data_file")
        assertThat(plan)
            .describedAs("the claim must be driven by the partial pending index:%n%s", plan)
            .contains("hog_data_file_pending")
        // The LIMIT must be able to stop the scan: the index's order is
        // the statement's ORDER BY, so there is no Sort above it. A
        // Sort would read every pending row in the instance before the
        // LIMIT could apply — which is the dropped table's whole
        // backlog, the thing this join exists to skip.
        assertThat(plan)
            .describedAs("a Sort would materialise every pending row before the LIMIT:%n%s", plan)
            .doesNotContain("Sort")
    }

    @Test
    fun `the join probes hog_table by primary key, once per candidate row`() {
        // A hash join here would BUILD from hog_table and PROBE with the
        // manifest — which is the plan that comes with a sequential
        // scan, and the one the case above excludes. What it must be
        // instead is a nested loop with an index probe per row the
        // pending index emitted.
        assertThat(plan)
            .describedAs("hog_table must be probed, not hashed:%n%s", plan)
            .contains("hog_table_pkey")
        assertThat(plan)
            .describedAs("no sequential scan of hog_table either:%n%s", plan)
            .doesNotContain("Seq Scan on hog_table")
        // BOUNDED BY DISTINCT TABLES, NOT BY ROWS, and that bound is
        // what a `Memoize` above the probe buys: the scan feeds it
        // 20,040 rows and it descends once per distinct
        // (catalog_id, table_id). The probe count is therefore a
        // property of how many TABLES have pending files, which is
        // small by construction, rather than of how many FILES do —
        // and a plan that lost the Memoize (or the index) would
        // descend per row instead.
        val distinctTables =
            db.jdbi.inTransactionUnchecked { h ->
                h.createQuery(
                    "SELECT count(DISTINCT (catalog_id, table_id)) FROM hog_data_file " +
                        "WHERE stats_state = 'pending'",
                ).mapTo(Long::class.java).one()
            }
        val searches = detailOf("hog_table", "Index Searches:")
        assertThat(searches)
            .describedAs(
                "one descent per distinct table among the pending rows (%d of them), not per " +
                    "row (%d of those):%n%s",
                distinctTables,
                PENDING_DROPPED + PENDING_LIVE,
                plan,
            )
            .isNotNull()
            .satisfies({ assertThat(it).isLessThanOrEqualTo(distinctTables) })
        assertThat(distinctTables)
            .describedAs("the fixture must make per-table and per-row bounds distinguishable")
            .isLessThan((PENDING_DROPPED + PENDING_LIVE) / 100L)
        println("[#193] hydrator claim: hog_table probed with $searches index searches:\n$plan")
    }

    @Test
    fun `a dropped table's backlog is skipped by the index scan, not by a filter above it`() {
        // The dropped table's 20,000 pending rows sit FIRST in the
        // index's order, so the scan does have to walk them — the join
        // cannot push `dropped_snapshot IS NULL` into a `hog_data_file`
        // index that does not carry it. What matters is that the work
        // is bounded by the PENDING QUEUE rather than by the manifest,
        // and that the rows are discarded by the join rather than
        // returned.
        val claimed =
            db.jdbi.inTransactionUnchecked { h ->
                h.createQuery(Hydrator.CLAIM_PENDING_SQL)
                    .bind("limit", LIMIT)
                    .map { rs, _ -> rs.getLong("table_id") }
                    .list()
            }
        assertThat(claimed)
            .describedAs("every claimed row must belong to the live table")
            .isNotEmpty()
            .allMatch { it == 1L }
        assertThat(claimed).hasSize(minOf(LIMIT, PENDING_LIVE))

        // And the cost is the queue's, not the manifest's: derived from
        // the pending population rather than written down.
        val pendingPages =
            db.jdbi.inTransactionUnchecked { h ->
                h.createQuery(
                    "SELECT pg_relation_size('hog_data_file_pending') / current_setting('block_size')::int",
                ).mapTo(Long::class.java).one()
            }
        val manifestPages =
            db.jdbi.inTransactionUnchecked { h ->
                h.createQuery("SELECT relpages FROM pg_class WHERE relname = 'hog_data_file'")
                    .mapTo(Long::class.java).one()
            }
        assertThat(pendingPages)
            .describedAs("the fixture must make the two costs distinguishable")
            .isLessThan(manifestPages / 4)
        println(
            "[#193] hydrator claim: the partial pending index is $pendingPages pages against a " +
                "$manifestPages-page manifest (${PENDING_DROPPED + PENDING_LIVE} pending of " +
                "${PROVIDED_FILES + PENDING_DROPPED + PENDING_LIVE} rows)",
        )
    }
}
