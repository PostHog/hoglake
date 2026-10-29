package com.posthog.hoglake.persistence

import com.posthog.hoglake.Database
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
 * V30 (`hog_data_file.prefix_quarantined`) and V31 (its partial index)
 * RUN here, against manifest rows seeded at V29: every existing row
 * reads false, the index arrives valid with the live predicate and the
 * `(catalog_id, table_id)` key, the gauge's subselect reads through it,
 * and re-applying both is a no-op.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class V30PrefixQuarantineMigrationIntegrationTest {
    private val db = PgTestSupport.freshDatabaseAt("29")

    @AfterAll
    fun tearDown() = db.close()

    private fun indexDef(): Pair<Boolean, String>? =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT i.indisvalid, pg_get_indexdef(c.oid) FROM pg_class c " +
                    "JOIN pg_index i ON i.indexrelid = c.oid WHERE c.relname = 'hog_data_file_prefix_quarantined'",
            ).map { rs, _ -> rs.getBoolean(1) to rs.getString(2) }.findOne().orElse(null)
        }

    @Test
    fun `V30 adds the flag false on existing rows, V31 indexes the live flagged rows, and a re-run is a no-op`() {
        db.jdbi.useHandleUnchecked { h ->
            val c =
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path) VALUES ('v30', 's3://v30') RETURNING catalog_id",
                )
                    .mapTo(Long::class.java).one()
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot, dropped_snapshot) " +
                    "VALUES (?, 1, 1, NULL), (?, 2, 1, 5)",
                c,
                c,
            )
            // Table 2 is dropped; every 50th row is ended.
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot, end_snapshot, path,
                                           record_count, file_size_bytes, row_id_start)
                SELECT :c, g, 1 + g % 2, 1, CASE WHEN g % 50 = 0 THEN 2 END, 's3://v30/' || g, 1, 1, g
                  FROM generate_series(1, 20000) g
                """,
            ).bind("c", c).execute()
        }
        assertThat(indexDef()).describedAs("absent before V31").isNull()

        Database.migrate(db.dataSource)

        val (nullable, default) =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT is_nullable, column_default FROM information_schema.columns " +
                        "WHERE table_name = 'hog_data_file' AND column_name = 'prefix_quarantined'",
                ).map { rs, _ -> rs.getString(1) to rs.getString(2) }.one()
            }
        assertThat(nullable).isEqualTo("NO")
        assertThat(default).isEqualTo("false")
        val flagged =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT count(*) FROM hog_data_file WHERE prefix_quarantined",
                ).mapTo(Long::class.java).one()
            }
        assertThat(flagged).describedAs("no file predates the check as quarantined").isZero()
        val (valid, definition) = indexDef()!!
        assertThat(valid).isTrue()
        assertThat(definition)
            .contains("(catalog_id, table_id)")
            .contains("WHERE (prefix_quarantined AND (end_snapshot IS NULL))")

        // Flag rows of both tables, live and ended; the gauge counts the
        // live table's live ones, through the index.
        db.jdbi.useHandleUnchecked { h ->
            h.execute("UPDATE hog_data_file SET prefix_quarantined = true WHERE data_file_id % 100 IN (0, 1, 2, 3)")
            h.execute("VACUUM ANALYZE hog_data_file")
        }
        val plan =
            db.jdbi.inTransactionUnchecked { h ->
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                h.createQuery("EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false) " + CatalogMetrics.SAMPLE_SQL)
                    .mapTo(String::class.java).list().joinToString("\n")
            }
        assertThat(plan).describedAs(plan)
            .containsPattern("(Index (Only )?Scan using|Bitmap Index Scan on) hog_data_file_prefix_quarantined\\b")
            .doesNotContain("Seq Scan on hog_data_file")
        val count =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(CatalogMetrics.SAMPLE_SQL).map { rs, _ -> rs.getLong("prefix_quarantined") }.one()
            }
        // Table 1 holds the even ids: % 100 = 0 is ended (a multiple of
        // 50), % 100 = 2 is 200 live rows. The odd ids are table 2's, dropped.
        assertThat(count).isEqualTo(200)

        db.jdbi.useHandleUnchecked { h -> h.execute("DELETE FROM flyway_schema_history WHERE version::numeric >= 30") }
        Database.migrate(db.dataSource)
        assertThat(indexDef()).isEqualTo(true to definition)
    }
}
