package com.posthog.hoglake.persistence

import com.posthog.hoglake.Database
import com.posthog.hoglake.service.CatalogService
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
 * V28 against the statement it exists for: the catalog drop's cascade
 * into `hog_file_removal`, i.e. the RI trigger's
 * `DELETE FROM ONLY hog_file_removal WHERE catalog_id = $1` (#412).
 *
 * THE MIGRATION RUNS HERE — the database arrives at V27, a ledger of
 * SETTLED rows is seeded, and [Database.migrate] applies V28 — so the
 * before half is a measurement on the same rows (V16/V17/V19's shape).
 *
 * SETTLED rows are the whole fixture: the drop refuses while any row of
 * its catalog is undrained, so the rows the cascade has to find are
 * exactly the ones both pre-V28 indexes exclude (`drained_at IS NULL`).
 * A fixture of undrained rows would be served by `hog_file_removal_drain`
 * and pass with or without this migration.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class V28FileRemovalCatalogIndexMigrationIntegrationTest {
    private companion object {
        const val INDEX = "hog_file_removal_catalog"

        /** Ledger rows across [CATALOGS] catalogs, all settled. */
        const val ROWS = 200_000
        const val CATALOGS = 20

        /** The cascade's statement, as the RI trigger runs it, bound to an id. */
        const val CASCADE = "DELETE FROM ONLY hog_file_removal WHERE catalog_id = :c"

        /** Index pages the cascade may read for a catalog with no rows left. */
        const val PROBE_BUFFERS = 10L
    }

    private val db = PgTestSupport.freshDatabaseAt("27", productionSession = true)
    private var dropped = 0L
    private lateinit var before: String
    private lateinit var after: String

    @AfterAll
    fun tearDown() = db.close()

    @BeforeAll
    fun seedThenMigrate() {
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_catalog (name, data_path) " +
                    "SELECT 'v28-' || g, 's3://v28/c' || g FROM generate_series(1, $CATALOGS) g",
            )
            // Every row settled; production-length paths so the heap is the
            // production shape per row.
            h.execute(
                """
                INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason, drained_at, drained_outcome)
                SELECT c.catalog_id, 's3://v28/' || repeat('p', 160) || g, 'data', 'snapshot_expiry', now(), 'deleted'
                FROM generate_series(1, $ROWS) g
                JOIN (SELECT catalog_id, row_number() OVER (ORDER BY catalog_id) - 1 AS k FROM hog_catalog) c
                  ON c.k = g % $CATALOGS
                """,
            )
            // A catalog with nothing in the ledger: what a drop looks like
            // once the preconditions hold for its OWN rows — the cascade
            // still has to prove there are none.
            dropped =
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path) VALUES ('v28-empty', 's3://v28/empty') " +
                        "RETURNING catalog_id",
                ).mapTo(Long::class.java).one()
            h.execute("VACUUM (ANALYZE) hog_file_removal")
        }
        assertThat(indexDef()).describedAs("%s absent before V28", INDEX).isNull()
        before = explainCascade()
        Database.migrate(db.dataSource)
        db.jdbi.useHandleUnchecked { h -> h.execute("ANALYZE hog_file_removal") }
        after = explainCascade()
        println("V28 cascade plan BEFORE:\n$before\nV28 cascade plan AFTER:\n$after")
    }

    /** EXPLAIN (ANALYZE, BUFFERS) of the cascade, serial, rolled back. */
    private fun explainCascade(): String =
        db.jdbi.withHandleUnchecked { h ->
            h.begin()
            try {
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                h.createQuery("EXPLAIN (ANALYZE, BUFFERS) $CASCADE").bind("c", dropped)
                    .mapTo(String::class.java).list().joinToString("\n")
            } finally {
                h.rollback()
            }
        }

    private fun indexDef(): String? =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT pg_get_indexdef(oid) FROM pg_class WHERE relname = :n")
                .bind("n", INDEX).mapTo(String::class.java).findOne().orElse(null)
        }

    private fun sharedBuffers(plan: String): Long =
        Regex("""Buffers: shared (?:hit=(\d+))?\s*(?:read=(\d+))?""").find(plan)!!.groupValues
            .drop(1).sumOf { it.toLongOrNull() ?: 0L }

    @Test
    fun `before V28 the cascade reads the whole ledger`() {
        // The defect, measured: no index serves settled rows by catalog.
        assertThat(before).contains("Seq Scan on hog_file_removal")
        assertThat(sharedBuffers(before)).isGreaterThan(1_000)
    }

    @Test
    fun `after V28 the cascade is an index probe`() {
        // MUTATION: making the index partial on `drained_at IS NULL` (the
        // shape of the two it sits beside) puts the Seq Scan back.
        assertThat(after).contains(INDEX).doesNotContain("Seq Scan")
        assertThat(sharedBuffers(after)).isLessThanOrEqualTo(PROBE_BUFFERS)
    }

    @Test
    fun `the index is valid, non-partial, and is what schema_sql declares`() {
        assertThat(indexDef()).isEqualTo("CREATE INDEX $INDEX ON public.hog_file_removal USING btree (catalog_id)")
        val valid =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = :n",
                ).bind("n", INDEX).mapTo(Boolean::class.java).one()
            }
        assertThat(valid).isTrue()
    }

    @Test
    fun `a catalog with settled ledger rows drops through the real service`() {
        // The end-to-end shape the index is for: settled rows of the
        // catalog being dropped, beside other catalogs' rows that stay.
        val catalogs = CatalogService(db.jdbi)
        catalogs.createCatalog("v28-drop", "s3://v28/drop")
        val id = catalogs.getCatalog("v28-drop").catalogId
        val others =
            db.jdbi.withHandleUnchecked { h ->
                h.execute(
                    "INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason, drained_at, drained_outcome) " +
                        "SELECT $id, 's3://v28/drop/f' || g, 'data', 'snapshot_expiry', now(), 'deleted' " +
                        "FROM generate_series(1, 100) g",
                )
                h.createQuery("SELECT count(*) FROM hog_file_removal WHERE catalog_id <> :c").bind("c", id)
                    .mapTo(Long::class.java).one()
            }
        catalogs.dropCatalog("v28-drop", "v28-test", "settled ledger rows cascade")
        db.jdbi.useHandleUnchecked { h ->
            assertThat(
                h.createQuery("SELECT count(*) FROM hog_file_removal WHERE catalog_id = :c").bind("c", id)
                    .mapTo(Long::class.java).one(),
            ).isZero()
            assertThat(
                h.createQuery("SELECT count(*) FROM hog_file_removal WHERE catalog_id <> :c").bind("c", id)
                    .mapTo(Long::class.java).one(),
            ).isEqualTo(others)
        }
    }
}
