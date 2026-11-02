package com.posthog.hoglake.persistence

import com.posthog.hoglake.Database
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * V29 — `hog_table_layout` and its live index, migrated onto a database
 * that arrives at V28 holding tables, then proven against the statement
 * it serves ([LayoutRepo.LIVE_SQL]; the commit read's `layouts` CTE has
 * its own plan test in `CommitStatementPlanIntegrationTest`).
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class V29TableLayoutMigrationIntegrationTest {
    private companion object {
        const val TABLES = 10_000

        /** Ended layout rows per table: history the live index must skip. */
        const val ENDED = 3
    }

    private val db = PgTestSupport.freshDatabaseAt("28")
    private var catalogId = 0L

    @AfterAll
    fun tearDown() = db.close()

    @BeforeAll
    fun seedThenMigrate() {
        db.jdbi.useHandleUnchecked { h ->
            catalogId =
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path) VALUES ('v29', 's3://v29') RETURNING catalog_id",
                ).mapTo(Long::class.java).one()
            h.createUpdate(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) " +
                    "SELECT :c, g, 0 FROM generate_series(1, :n) g",
            ).bind("c", catalogId).bind("n", TABLES).execute()
        }
        Database.migrate(db.dataSource)
        db.jdbi.useHandleUnchecked { h ->
            // Every table with ENDED ended rows and one live row: the shape
            // a future unset/re-set would leave, and more history than 2.1
            // can produce, so a scan that reads ended rows shows.
            h.createUpdate(
                """
                INSERT INTO hog_table_layout (catalog_id, table_id, begin_snapshot, end_snapshot, object_prefix_field_id)
                SELECT :c, g, e * 10, CASE WHEN e < :ended THEN e * 10 + 5 END, 1
                FROM generate_series(1, :n) g, generate_series(0, :ended) e
                """,
            ).bind("c", catalogId).bind("n", TABLES).bind("ended", ENDED).execute()
            h.execute("VACUUM (ANALYZE) hog_table_layout")
        }
    }

    @Test
    fun `the live index is unique, partial on end_snapshot IS NULL, keyed (catalog_id, table_id)`() {
        val (unique, predicate, keys) =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT i.indisunique, pg_get_expr(i.indpred, i.indrelid),
                           (SELECT array_agg(a.attname ORDER BY k.ord)
                              FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
                              JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum)
                    FROM pg_index i WHERE i.indexrelid = 'hog_table_layout_live'::regclass
                    """,
                ).map { rs, _ ->
                    Triple(rs.getBoolean(1), rs.getString(2), (rs.getArray(3).array as Array<*>).map { it.toString() })
                }.one()
            }
        assertThat(unique).isTrue()
        assertThat(predicate).isEqualTo("(end_snapshot IS NULL)")
        assertThat(keys).containsExactly("catalog_id", "table_id")
    }

    @Test
    fun `one live layout per table, a CHECK on the range, and the table's drop cascades`() {
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, ?, 0)",
                catalogId,
                99_999,
            )
            h.execute(
                "INSERT INTO hog_table_layout (catalog_id, table_id, begin_snapshot, object_prefix_field_id) " +
                    "VALUES (?, 99999, 5, 1)",
                catalogId,
            )
        }
        assertThatThrownBy {
            db.jdbi.useHandleUnchecked { h ->
                h.execute(
                    "INSERT INTO hog_table_layout (catalog_id, table_id, begin_snapshot, object_prefix_field_id) " +
                        "VALUES (?, 99999, 6, 2)",
                    catalogId,
                )
            }
        }.hasMessageContaining("hog_table_layout_live")
        assertThatThrownBy {
            db.jdbi.useHandleUnchecked { h ->
                h.execute(
                    "INSERT INTO hog_table_layout (catalog_id, table_id, begin_snapshot, end_snapshot, " +
                        "object_prefix_field_id) VALUES (?, 99999, 7, 7, 1)",
                    catalogId,
                )
            }
        }.hasMessageContaining("hog_table_layout_check")
        val left =
            db.jdbi.inTransactionUnchecked { h ->
                h.execute("DELETE FROM hog_table WHERE catalog_id = ? AND table_id = 99999", catalogId)
                h.createQuery("SELECT count(*) FROM hog_table_layout WHERE catalog_id = ? AND table_id = 99999")
                    .bind(0, catalogId).mapTo(Long::class.java).one()
            }
        assertThat(left).isZero()
    }

    @Test
    fun `LayoutRepo's live read descends the live index once per table and reads no history`() {
        val ids = listOf(17L, 4_242L, 9_001L)
        val plan =
            db.jdbi.inTransactionUnchecked { h ->
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                h.createQuery(
                    "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) ${LayoutRepo.LIVE_SQL}",
                )
                    .bind("catalogId", catalogId)
                    .bindArray("tableIds", Long::class.javaObjectType, ids)
                    .mapTo(String::class.java).list().joinToString("\n")
            }
        assertThat(plan).describedAs(plan).contains("hog_table_layout_live").doesNotContain("Seq Scan")
        assertThat(plan).describedAs(plan).doesNotContainPattern("Rows Removed by Filter: [1-9]")
        assertThat(db.jdbi.withHandleUnchecked { h -> LayoutRepo.live(h, catalogId, ids) })
            .isEqualTo(ids.associateWith { 1L })
        assertThat(db.jdbi.withHandleUnchecked { h -> LayoutRepo.layoutAt(h, catalogId, 17, 12) })
            .describedAs("the ended row [10, 15) is visible at 12").isEqualTo(1L)
        assertThat(db.jdbi.withHandleUnchecked { h -> LayoutRepo.layoutAt(h, catalogId, 17, 16) })
            .describedAs("nothing is visible between 15 and the next row at 20").isNull()
    }

    @Test
    fun `V29 is re-appliable`() {
        db.jdbi.useHandleUnchecked { h -> h.execute("DELETE FROM flyway_schema_history WHERE version::numeric >= 29") }
        Database.migrate(db.dataSource)
        val rows =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT count(*) FROM hog_table_layout WHERE catalog_id = ?")
                    .bind(0, catalogId).mapTo(Long::class.java).one()
            }
        assertThat(rows).isGreaterThanOrEqualTo(TABLES.toLong() * (ENDED + 1))
    }
}
