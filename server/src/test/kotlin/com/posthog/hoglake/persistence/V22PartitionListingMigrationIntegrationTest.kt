package com.posthog.hoglake.persistence

import com.posthog.hoglake.Database
import com.posthog.hoglake.service.PartitionListingService
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
 * V22 against the statement it exists for: the partitions listing's one
 * aggregation over `hog_maintenance_summary_tier`.
 *
 * THE MIGRATION RUNS HERE, in V19's and V16's shape — the database
 * arrives at V21, the buckets are seeded, and only then does
 * [Database.migrate] apply V22. The BEFORE plan is therefore a
 * measurement on the same rows rather than a claim about a file that is
 * not there.
 *
 * WHAT THE INDEX IS FOR: the listing filters `(catalog_id, generation,
 * table_id)` and the primary key is `(catalog_id, generation,
 * bucket_key)`, where the bucket key is a SHA-256 of
 * (table, spec, values, quota) and carries no table locality
 * whatsoever. Without the index the statement is a scan of the WHOLE
 * published generation — every table's buckets read to answer about
 * one. The fixture is five tables of equal size in one generation, so
 * "the catalog's generation" and "this table's slice" are different
 * row sets; with a single table they would be the same rows and the
 * index would look free.
 *
 * `Rows Removed by Filter` is the assertion that matters, not the index
 * name: AGENT.md records V16's case, where a guard no btree could drive
 * still produced an index scan because the leading column alone was
 * enough and the rest was demoted to a Filter. A plan that reads the
 * other four tables' buckets and discards them says so there.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class V22PartitionListingMigrationIntegrationTest {
    private companion object {
        const val INDEX = "hog_maintenance_summary_tier_table"

        /** Buckets per table in the published generation. */
        const val GROUPS_PER_TABLE = 5_000

        /** Tables sharing the generation; the four others are what the index skips. */
        const val TABLES = 5

        /**
         * The pre-V22 form of the listing's statement: the same
         * grouping and the same predicate, minus the two columns V22
         * adds. It exists so the BEFORE plan is the same SHAPE as the
         * after one — a different statement would compare two things.
         */
        val PRE_V22_SQL =
            PartitionListingService.GROUP_ROWS_SQL
                .replace("                   sum(p.record_count) AS record_count,\n", "")
                .replace(
                    "                   max(p.newest_begin_snapshot) AS newest_begin_snapshot\n",
                    "                   sum(p.dv_count) AS dv_count_again\n",
                )
    }

    private val db = PgTestSupport.freshDatabaseAt("21")
    private var catalogId = 0L

    private lateinit var planBefore: String
    private lateinit var planAfter: String

    @AfterAll
    fun tearDown() = db.close()

    @BeforeAll
    fun seedThenMigrate() {
        catalogId =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path, last_snapshot_id) " +
                        "VALUES ('v22', 's3://v22', 1000) RETURNING catalog_id",
                ).mapTo(Long::class.java).one()
            }
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) " +
                    "SELECT ?, g, 0 FROM generate_series(1, ?) g",
                catalogId,
                TABLES,
            )
            h.execute(
                "INSERT INTO hog_maintenance_summary (catalog_id, generation, published_generation) " +
                    "VALUES (?, 1, 1)",
                catalogId,
            )
            // Interleaved across tables by the generate_series, so the
            // one table's buckets are SCATTERED through the heap
            // exactly as a real generation leaves them — a contiguous
            // block would share heap pages and flatter the index.
            h.createUpdate(
                """
                INSERT INTO hog_maintenance_summary_tier
                    (catalog_id, generation, bucket_key, table_id, spec_id, partition_values,
                     quota, remaining, pending, selected, pending_max_bytes,
                     file_count, small_count, total_bytes, small_bytes, dv_count)
                SELECT :c, 1, md5(g::text), 1 + (g % :tables), 1, ARRAY[(20000 + g)::text],
                       1000, 1000, 0, g % 7, 0, 3, 2, 1000 + g, 900, g % 3
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("tables", TABLES)
                .bind("n", GROUPS_PER_TABLE * TABLES).execute()
            h.execute("VACUUM (ANALYZE) hog_maintenance_summary_tier")
        }

        assertThat(indexDef()).describedAs("%s absent before V22", INDEX).isNull()
        planBefore = explain(PRE_V22_SQL)
        Database.migrate(db.dataSource)
        db.jdbi.useHandleUnchecked { it.execute("VACUUM (ANALYZE) hog_maintenance_summary_tier") }
        planAfter = explain(PartitionListingService.GROUP_ROWS_SQL)
    }

    private fun indexDef(): String? =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT indexdef FROM pg_indexes WHERE indexname = :n")
                .bind("n", INDEX).mapTo(String::class.java).findOne().orElse(null)
        }

    /** Serial plans only: a Gather splits the buffer counts across workers. */
    private fun explain(sql: String): String =
        db.jdbi.withHandleUnchecked { h ->
            h.begin()
            try {
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                h.createQuery(
                    "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) $sql",
                )
                    .bind("catalogId", catalogId)
                    .bind("tableId", 1L)
                    .mapTo(String::class.java).list().joinToString("\n")
            } finally {
                h.rollback()
            }
        }

    private fun rowsRemoved(plan: String): Long =
        Regex("""Rows Removed by Filter: (\d+)""").findAll(plan)
            .sumOf { it.groupValues[1].toLong() }

    // ---- the columns ---------------------------------------------------------

    @Test
    fun `the two measures exist with the defaults that mean not-sampled`() {
        val columns =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT column_name, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_name = 'hog_maintenance_summary_tier'
                      AND column_name IN ('record_count', 'newest_begin_snapshot')
                    """,
                ).map { rs, _ ->
                    rs.getString("column_name") to
                        (rs.getString("is_nullable") to rs.getString("column_default"))
                }.list().toMap()
            }
        assertThat(columns).containsOnlyKeys("record_count", "newest_begin_snapshot")
        // NOT NULL DEFAULT 0: the rows a pre-V22 sampler wrote read back
        // as zero, which is why the API's "not sampled" test is the
        // NULL beside it and never this column on its own.
        assertThat(columns["record_count"]!!.first).isEqualTo("NO")
        assertThat(columns["record_count"]!!.second).isEqualTo("0")
        // Nullable with no default: the null IS the signal.
        assertThat(columns["newest_begin_snapshot"]!!.first).isEqualTo("YES")
        assertThat(columns["newest_begin_snapshot"]!!.second).isNull()
    }

    // ---- the index -----------------------------------------------------------

    @Test
    fun `the index keys exactly the columns the listing filters on`() {
        val key =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_class c ON c.oid = i.indexrelid
                    JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                    WHERE c.relname = :n
                    ORDER BY k.ord
                    """,
                ).bind("n", INDEX).mapTo(String::class.java).list()
            }
        // Resolved through pg_index.indkey, not by grepping indexdef —
        // a `contains("table_id")` over the definition is satisfied by
        // the index's own NAME (AGENT.md).
        assertThat(key).containsExactly("catalog_id", "generation", "table_id")
    }

    @Test
    fun `before V22 the listing scans the whole generation, after it reads one table's slice`() {
        assertThat(planBefore)
            .describedAs("the pre-V22 plan has no access path for table_id:\n%s", planBefore)
            .contains("hog_maintenance_summary_tier")
        // The other four tables' buckets, read and discarded.
        assertThat(rowsRemoved(planBefore))
            .describedAs("before V22, the statement reads the catalog's whole generation:\n%s", planBefore)
            .isGreaterThanOrEqualTo((GROUPS_PER_TABLE * (TABLES - 1)).toLong())

        assertThat(planAfter)
            .describedAs("V22's index must serve the listing:\n%s", planAfter)
            .contains(INDEX)
        assertThat(planAfter)
            .describedAs("no sequential scan of the tier table after V22:\n%s", planAfter)
            .doesNotContain("Seq Scan on hog_maintenance_summary_tier")
        // NOTHING filtered: the index descends to this table's slice
        // rather than reading the generation and demoting table_id to a
        // Filter, which is the failure V16 recorded.
        assertThat(rowsRemoved(planAfter))
            .describedAs("the index must not leave table_id as a Filter:\n%s", planAfter)
            .isEqualTo(0)
    }
}
