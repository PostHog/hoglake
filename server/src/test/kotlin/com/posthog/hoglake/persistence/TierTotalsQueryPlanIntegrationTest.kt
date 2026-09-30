package com.posthog.hoglake.persistence

import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * The plan behind `TierTotalsRepo.PER_TABLE_SQL` — the statement every
 * head table GET now runs instead of aggregating the manifest (#232).
 *
 * THE STATEMENT ITSELF, not a copy of it: the constant is `internal`
 * precisely so this test can EXPLAIN what production issues. A plan test
 * that retypes its query asserts the plan of a string only it has ever
 * executed (`TableListingQueryPlanIntegrationTest` says the same).
 *
 * WHAT THIS EXISTS TO PIN, and it is not latency. The replaced aggregate
 * was 0.7 s quiet and 9 s loaded on a ~10M-row table; anything indexed
 * wins by orders of magnitude, so a millisecond budget would pass on a
 * plan that was quietly wrong. What can actually go wrong is the ACCESS
 * PATH: the tier table's primary key is `(catalog_id, generation,
 * bucket_key)` where `bucket_key` is a SHA-256 and carries NO table
 * locality, so without V22's `hog_maintenance_summary_tier_table` this
 * read is a scan of the whole catalog's tier rows for one table's
 * answer. That is the regression, and it is invisible in a latency
 * number on a small fixture.
 *
 * AND THE PLANNER HAS TWO SHAPES HERE, which is the other half of why
 * this test is here and why it pins neither of them. Measured on PG 18:
 * as a nested-loop inner scan — which is what this fixture produces —
 * the summary row is already resolved on the outer side, so
 * `published_generation` is a parameterized equality and ALL THREE index
 * columns land in the index condition (`Index Searches: 1`, 18 rows, 4
 * buffers, nothing filtered). Under a merge/hash shape the generation
 * equality is applied after the scan instead, the middle column is
 * skip-scanned, and the read touches every retained generation's buckets
 * for the table before filtering (measured elsewhere at 5,018 rows for a
 * 5,000-bucket table across two generations).
 *
 * Both are fine, and both are bounded by `buckets x retained
 * generations`. So the assertions below pin what is TRUE OF EITHER —
 * the index is used, both `catalog_id` and `table_id` are in the index
 * CONDITION, rows looked at and buffers are bounded — and deliberately
 * not which plan the planner picked, because that would red on a
 * statistics change that broke nothing.
 *
 * FIXTURE SHAPE is what makes the assertions mean anything. Several
 * catalogs, each with thousands of tables, and TWO retained generations:
 *  - many tables, so "this table's buckets" and "the catalog's buckets"
 *    are different row sets. With one table they are the same rows and
 *    the index would look free;
 *  - several catalogs, so `catalog_id` alone cannot narrow to the answer;
 *  - two generations, because the skip scan's cost is per generation and
 *    a single-generation fixture cannot show it.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TierTotalsQueryPlanIntegrationTest {
    private companion object {
        const val INDEX = "hog_maintenance_summary_tier_table"
        const val TIER = "hog_maintenance_summary_tier"

        /** Catalogs in the fixture; the probe reads one. */
        const val CATALOGS = 3

        /** Tables per catalog. */
        const val TABLES = 1_000

        /** Buckets per table, per generation — a coarsely partitioned table. */
        const val BUCKETS = 18

        /**
         * Retained generations. The sampler keeps the published one plus
         * at most one in flight, and the skip scan reads both.
         */
        const val GENERATIONS = 2

        /** The catalog and table the EXPLAIN names — mid-fixture, not first. */
        const val PROBE_CATALOG_ORDINAL = 2
        const val PROBE_TABLE = 500L
    }

    private val db = PgTestSupport.freshDatabase()
    private var probeCatalogId = 0L
    private lateinit var plan: String

    @AfterAll
    fun tearDown() = db.close()

    @BeforeAll
    fun seed() {
        val ids =
            db.jdbi.withHandleUnchecked { h ->
                (1..CATALOGS).map { c ->
                    h.createQuery(
                        "INSERT INTO hog_catalog (name, data_path, last_snapshot_id) " +
                            "VALUES (:n, 's3://' || :n, 1000) RETURNING catalog_id",
                    ).bind("n", "plan-$c").mapTo(Long::class.java).one()
                }
            }
        probeCatalogId = ids[PROBE_CATALOG_ORDINAL]
        db.jdbi.useHandleUnchecked { h ->
            for (catalogId in ids) {
                h.execute(
                    "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) " +
                        "SELECT ?, g, 0 FROM generate_series(1, ?) g",
                    catalogId,
                    TABLES,
                )
                h.execute(
                    "INSERT INTO hog_maintenance_summary (catalog_id, generation, published_generation, " +
                        "measures_generation, sampled_at, sample) " +
                        "VALUES (?, ?, ?, ?, now(), '{}'::jsonb)",
                    catalogId,
                    GENERATIONS,
                    GENERATIONS,
                    GENERATIONS,
                )
                // Buckets for every table in every retained generation.
                // The bucket key is a hash of (table, spec, values,
                // quota) in production, so it is a hash here too: an
                // ordered key would give the heap a table locality the
                // real thing does not have, and the index would look
                // better than it is.
                h.createUpdate(
                    """
                    INSERT INTO $TIER
                        (catalog_id, generation, bucket_key, table_id, spec_id, partition_values,
                         quota, remaining, pending, selected, pending_max_bytes,
                         file_count, small_count, total_bytes, small_bytes, dv_count,
                         record_count, newest_begin_snapshot)
                    SELECT :c, gen, md5(:c || '-' || gen || '-' || t || '-' || b),
                           t, 1, ARRAY[(20000 + b)::text],
                           1000, 1000, 0, 0, 0,
                           3, 2, 3000, 900, 0, 30, 10
                    FROM generate_series(1, :generations) gen,
                         generate_series(1, :tables) t,
                         generate_series(1, :buckets) b
                    """,
                ).bind("c", catalogId).bind("generations", GENERATIONS)
                    .bind("tables", TABLES).bind("buckets", BUCKETS).execute()
            }
            h.execute("VACUUM (ANALYZE) $TIER")
            h.execute("VACUUM (ANALYZE) hog_table")
            h.execute("VACUUM (ANALYZE) hog_maintenance_summary")
        }
        plan = explain()
    }

    /** Serial plans only: a Gather splits the buffer counts across workers. */
    private fun explain(): String =
        db.jdbi.withHandleUnchecked { h ->
            h.begin()
            try {
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                h.createQuery(
                    "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) " +
                        TierTotalsRepo.PER_TABLE_SQL,
                )
                    .bind("catalogId", probeCatalogId)
                    .bind("tableId", PROBE_TABLE)
                    .mapTo(String::class.java).list().joinToString("\n")
            } finally {
                h.rollback()
            }
        }

    /** `relpages` for a relation, the budget source for the buffer bound. */
    private fun relpages(name: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT relpages FROM pg_class WHERE relname = :n")
                .bind("n", name).mapTo(Long::class.java).one()
        }

    /** Buffers attributed to the tier scan node, never the plan's maximum. */
    private fun tierScanBuffers(): Long {
        // AGENT.md: take buffers from the SCAN NODE, not the plan's
        // maximum — EXPLAIN's `Planning:` section carries a `Buffers:`
        // line that dwarfed a well-indexed scan's. So the parse walks
        // to the tier node and reads the FIRST Buffers line under it.
        val lines = plan.lines()
        val at = lines.indexOfFirst { it.contains(TIER) }
        assertThat(at).describedAs("no node names %s:%n%s", TIER, plan).isNotEqualTo(-1)
        val buffers =
            lines.drop(at + 1).takeWhile { !it.trimStart().startsWith("->") && !it.startsWith("Planning") }
                .firstNotNullOfOrNull { Regex("""shared hit=(\d+)(?: read=(\d+))?""").find(it) }
        assertThat(buffers).describedAs("no Buffers line under the %s node:%n%s", TIER, plan).isNotNull()
        return buffers!!.groupValues[1].toLong() + (buffers.groupValues[2].toLongOrNull() ?: 0)
    }

    // ---- the assertions -----------------------------------------------------

    @Test
    fun `the read rides V22's index and never scans the tier table`() {
        assertThat(plan)
            .describedAs("V22's index is the only table-local access path this statement has:%n%s", plan)
            .contains(INDEX)
        assertThat(plan)
            .describedAs(
                "a sequential scan here reads every table's buckets in the catalog to answer " +
                    "about one, which is the regression this index exists to prevent:%n%s",
                plan,
            )
            .doesNotContain("Seq Scan on $TIER")
    }

    @Test
    fun `the index condition is catalog and table, with the generation filtered after`() {
        // THE MEASURED SHAPE, pinned so it cannot change silently.
        // PG 18 skip-scans the middle column of
        // (catalog_id, generation, table_id) rather than probing all
        // three, so the scan reads this table's buckets across EVERY
        // retained generation and applies
        // `p.generation = s.published_generation` afterwards.
        //
        // The assertion is deliberately about the index CONDITION rather
        // than about the absence of a filter: a plan that filtered
        // `table_id` instead would be the V16 failure this repo records
        // (an index scan that is really a prefix scan with the rest
        // demoted), and that is what naming both columns here excludes.
        val cond =
            Regex("""Index Cond: \(([^\n]*)\)""").findAll(plan).map { it.groupValues[1] }.toList() +
                Regex("""Recheck Cond: \(([^\n]*)\)""").findAll(plan).map { it.groupValues[1] }.toList()
        assertThat(cond)
            .describedAs("no index condition at all in:%n%s", plan)
            .isNotEmpty()
        assertThat(cond.joinToString(" | "))
            .describedAs(
                "both keys must be in the index CONDITION. A plan that indexes on catalog_id " +
                    "alone and demotes table_id to a Filter reads the whole catalog's " +
                    "generation and discards it — green on an index-name assertion, 200x the " +
                    "work (AGENT.md records V16's case):%n%s",
                plan,
            )
            .contains("catalog_id")
            .contains("table_id")
    }

    @Test
    fun `the scan reads about this table's buckets, not the catalog's`() {
        // ROWS LOOKED AT, which is `rows + Rows Removed by Filter` — the
        // measure `TableListingQueryPlanIntegrationTest` records as the
        // one a degraded plan fails and an output-row assertion does not.
        //
        // The budget is BUCKETS x GENERATIONS with slack, derived from
        // the fixture rather than written down: the skip scan reads every
        // retained generation's buckets for this table. The number to
        // exclude is the catalog's whole generation
        // (TABLES x BUCKETS = 18,000), which is three orders of
        // magnitude above it.
        val emitted =
            Regex("""on $TIER[^\n]*actual rows=([\d.]+)""").find(plan)
                ?.groupValues?.get(1)?.toDouble()?.toLong() ?: 0
        val filtered =
            Regex("""Rows Removed by Filter: (\d+)""").findAll(plan).sumOf { it.groupValues[1].toLong() }
        val looked = emitted + filtered
        assertThat(looked)
            .describedAs(
                "the scan must read this TABLE's buckets across the retained generations " +
                    "(%d x %d), not the catalog's whole generation (%d):%n%s",
                BUCKETS,
                GENERATIONS,
                TABLES * BUCKETS,
                plan,
            )
            .isLessThanOrEqualTo(BUCKETS.toLong() * GENERATIONS * 4)
    }

    @Test
    fun `the scan reads a handful of pages, not the relation`() {
        // Buffers from the SCAN NODE, with the budget read from
        // pg_class.relpages rather than written as a constant — the
        // constant a sibling test started with sat 1.27x below the
        // degraded plan it had to exclude. A correct plan descends the
        // index and touches a page or two of heap per bucket; the plan
        // to exclude reads the relation.
        val heapPages = relpages(TIER)
        val buffers = tierScanBuffers()
        assertThat(heapPages)
            .describedAs("the fixture must be big enough for the bound to mean something")
            .isGreaterThan(200)
        assertThat(buffers)
            .describedAs(
                "%d buffers against a %d-page tier table: a correct plan reads a small " +
                    "multiple of this table's buckets, not the relation:%n%s",
                buffers,
                heapPages,
                plan,
            )
            .isLessThan(heapPages / 4)
    }

    @Test
    fun `the answer is right, on the fixture the plan was measured on`() {
        // A plan test that never runs the statement for its VALUE can
        // pass on a query that reads the right pages and returns the
        // wrong number. One assertion closes that: the probe table's
        // sums over ONE generation, not two.
        val totals =
            db.jdbi.withHandleUnchecked { h ->
                TierTotalsRepo.totalsFor(h, probeCatalogId, PROBE_TABLE)
            }!!
        assertThat(totals.fileCount)
            .describedAs(
                "%d buckets x 3 files, from the PUBLISHED generation only — twice this would " +
                    "be the skip scan's rows leaking past the generation filter",
                BUCKETS,
            )
            .isEqualTo(BUCKETS * 3L)
        assertThat(totals.recordCount).isEqualTo(BUCKETS * 30L)
        assertThat(totals.fileSizeBytes).isEqualTo(BUCKETS * 3000L)
        assertThat(totals.measured).isTrue()
    }
}
