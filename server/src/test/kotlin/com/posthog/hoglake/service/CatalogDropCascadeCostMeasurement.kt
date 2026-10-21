package com.posthog.hoglake.service

import com.posthog.hoglake.testing.Measuring
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
 * What a catalog drop's cascade into the removal ledger costs, measured
 * on a ledger of production shape — the number V28 was added for (#412
 * review).
 *
 * # What is measured
 *
 * `CatalogService.dropCatalog` itself, end to end, on a catalog whose own
 * ledger rows are SETTLED (the only kind the drop admits) beside
 * [LEDGER_ROWS] settled rows of [OTHER_CATALOGS] other catalogs, with
 * production-length paths. The cascade's RI trigger runs
 * `DELETE FROM ONLY hog_file_removal WHERE catalog_id = $1`; with V28
 * that is an index probe, and the EXPLAIN printed here says so. The
 * pre-V28 shape is measured too, by dropping the index inside a
 * rolled-back transaction, so the two plans come off the same rows.
 *
 * # What it asserts, and where
 *
 * Everywhere: the plan of the cascade statement is an Index Scan on
 * `hog_file_removal_catalog` (MUTATION: dropping V28's index, or making
 * it partial on `drained_at IS NULL`, puts a Seq Scan here), and its
 * buffers stay flat — a handful of index pages — however large the
 * ledger. Only under `HOGLAKE_MEASURE=1` ([Measuring]): the drop's wall
 * clock is under [DROP_BUDGET_MS], because a shared runner's
 * milliseconds teach nothing.
 *
 * # Why the absolute figures are a floor
 *
 * The fixture ledger fits the container's cache; production's ~137M-row
 * ledger does not. The pre-V28 Seq Scan is linear in the ledger, so its
 * figure here scales by ~137M / [LEDGER_ROWS] cold; the index probe does
 * not scale with the ledger at all, which is the property that transfers.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CatalogDropCascadeCostMeasurement {
    private companion object {
        const val LEDGER_ROWS = 2_000_000
        const val OTHER_CATALOGS = 40

        /** The dropped catalog's own settled rows. */
        const val OWN_ROWS = 1_000

        /** Index pages the cascade probe may read, ledger size notwithstanding. */
        const val PROBE_BUFFERS = 50L

        /** HOGLAKE_MEASURE only: the drop, cascade included, on a quiet host. */
        const val DROP_BUDGET_MS = 1_000L

        const val CASCADE = "DELETE FROM ONLY hog_file_removal WHERE catalog_id = :c"
    }

    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private var dropId = 0L

    @AfterAll
    fun tearDown() = db.close()

    @BeforeAll
    fun seed() {
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_catalog (name, data_path) " +
                    "SELECT 'cost-' || g, 's3://cost/c' || g FROM generate_series(1, $OTHER_CATALOGS) g",
            )
            h.execute(
                """
                INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason, drained_at, drained_outcome)
                SELECT c.catalog_id, 's3://cost/' || repeat('p', 160) || g, 'data', 'snapshot_expiry', now(), 'deleted'
                FROM generate_series(1, $LEDGER_ROWS) g
                JOIN (SELECT catalog_id, row_number() OVER (ORDER BY catalog_id) - 1 AS k FROM hog_catalog) c
                  ON c.k = g % $OTHER_CATALOGS
                """,
            )
        }
        catalogs.createCatalog("cost-dropped", "s3://cost/dropped")
        dropId = catalogs.getCatalog("cost-dropped").catalogId
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason, drained_at, drained_outcome) " +
                    "SELECT $dropId, 's3://cost/dropped/f' || g, 'data', 'snapshot_expiry', now(), 'deleted' " +
                    "FROM generate_series(1, $OWN_ROWS) g",
            )
            h.execute("VACUUM (ANALYZE) hog_file_removal")
        }
    }

    /** EXPLAIN (ANALYZE, BUFFERS) of the cascade statement, serial, rolled back; [withoutIndex] = pre-V28. */
    private fun explain(withoutIndex: Boolean): String =
        db.jdbi.withHandleUnchecked { h ->
            h.begin()
            try {
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                if (withoutIndex) h.execute("DROP INDEX hog_file_removal_catalog")
                h.createQuery("EXPLAIN (ANALYZE, BUFFERS) $CASCADE").bind("c", dropId)
                    .mapTo(String::class.java).list().joinToString("\n")
            } finally {
                h.rollback()
            }
        }

    private fun buffers(plan: String): Long =
        Regex("""Buffers: shared (?:hit=(\d+))?\s*(?:read=(\d+))?""").find(plan)!!.groupValues
            .drop(1).sumOf { it.toLongOrNull() ?: 0L }

    @Test
    fun `the cascade into the ledger is an index probe, and the drop is cheap`() {
        val pre = explain(withoutIndex = true)
        val post = explain(withoutIndex = false)
        println("catalog-drop cascade WITHOUT hog_file_removal_catalog (pre-V28):\n$pre")
        println("catalog-drop cascade WITH it:\n$post")
        assertThat(pre).contains("Seq Scan on hog_file_removal")
        assertThat(post).contains("hog_file_removal_catalog").doesNotContain("Seq Scan")
        // The probe reads the dropped catalog's own index entries and heap
        // rows, never the other 2M.
        assertThat(buffers(post)).isLessThan(buffers(pre) / 10)

        val start = System.nanoTime()
        catalogs.dropCatalog("cost-dropped", "cost-test", "measure the cascade")
        val millis = (System.nanoTime() - start) / 1_000_000
        println("dropCatalog with $OWN_ROWS own + $LEDGER_ROWS other settled ledger rows: $millis ms")
        val left =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT count(*) FROM hog_file_removal WHERE catalog_id = :c").bind("c", dropId)
                    .mapTo(Long::class.java).one()
            }
        assertThat(left).isZero()
        if (Measuring.enabled) assertThat(millis).isLessThan(DROP_BUDGET_MS)
    }

    @Test
    fun `an empty catalog's cascade probe reads a handful of pages`() {
        // The steady-state shape: most dropped catalogs have few or no
        // settled rows left, and the probe must not scale with the ledger.
        catalogs.createCatalog("cost-empty", "s3://cost/empty")
        val id = catalogs.getCatalog("cost-empty").catalogId
        val plan =
            db.jdbi.withHandleUnchecked { h ->
                h.begin()
                try {
                    h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                    h.createQuery("EXPLAIN (ANALYZE, BUFFERS) $CASCADE").bind("c", id)
                        .mapTo(String::class.java).list().joinToString("\n")
                } finally {
                    h.rollback()
                }
            }
        assertThat(plan).contains("hog_file_removal_catalog")
        assertThat(buffers(plan)).isLessThanOrEqualTo(PROBE_BUFFERS)
    }
}
