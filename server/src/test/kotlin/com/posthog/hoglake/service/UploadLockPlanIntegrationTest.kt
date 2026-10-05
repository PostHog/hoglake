package com.posthog.hoglake.service

import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.jdbi.v3.core.statement.Query
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

/**
 * The plans of the statements that lock claim rows in `upload_id` order:
 * [UploadService.RENEW_SQL], [UploadService.ABANDON_SQL] and
 * [UploadService.REGISTER_CLAIMS_SQL].
 *
 * The fixture is a `hog_upload` shaped like a busy catalog's: the
 * registered claims of finished operations, which none of these
 * statements may read, and the active claims of one wide operation, one
 * per output file. Each statement must reach its rows through an index,
 * sort them by `upload_id`, and lock them in that order. Renew and abandon
 * then find the rows they hold by `upload_id` through an index: an UPDATE
 * joined to the locked rows by a hash join would read every claim of the
 * catalog.
 *
 * Measured on this fixture (Postgres 18, 201,000 claims): renew finds and
 * locks its 1,000 rows in 1,023 buffers through `hog_upload_owner`, and its
 * UPDATE reaches them in about 3,000 buffers through `hog_upload_cleanup`,
 * whose partial predicate leaves out the registered claims and whose key
 * includes `upload_id`. Writing the new row versions costs about 17 more
 * buffers a row.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class UploadLockPlanIntegrationTest {
    companion object {
        /** Claims of finished operations, all registered, 100 per owner. */
        const val REGISTERED = 200_000

        /** The active claims of one wide operation. */
        const val ACTIVE = 1_000

        private val SORT_BY_UPLOAD_ID = Regex("""Sort Key: (hog_upload(_\d+)?\.)?upload_id\b""")

        /** The UPDATE finds the rows that the sub-select locked by `upload_id`, through an index. */
        private val PROBE_BY_UPLOAD_ID = Regex("""Index Cond: \(.*upload_id = ANY \(\(InitPlan 1\)""")
    }

    private val db = PgTestSupport.freshDatabase()
    private val owner = UUID.randomUUID()
    private var catalogId = 0L
    private lateinit var paths: List<String>

    @AfterAll
    fun tearDown() = db.close()

    @BeforeAll
    fun seed() {
        db.jdbi.useHandleUnchecked { h ->
            catalogId =
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path) VALUES ('upload-plan', 's3://upload-plan') " +
                        "RETURNING catalog_id",
                ).mapTo(Long::class.java).one()
            h.createUpdate(
                """
                INSERT INTO hog_upload (catalog_id, upload_id, owner, prefix, path, file_kind, state)
                SELECT :c, gen_random_uuid(), md5((g / 100)::text)::uuid, 's3://upload-plan',
                       's3://upload-plan/trino-upload/registered-' || g || '.parquet', 'data', 'registered'
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("n", REGISTERED).execute()
            h.createUpdate(
                """
                INSERT INTO hog_upload (catalog_id, upload_id, owner, prefix, path, file_kind)
                SELECT :c, gen_random_uuid(), :owner, 's3://upload-plan',
                       's3://upload-plan/trino-upload/active-' || g || '.parquet', 'data'
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("owner", owner).bind("n", ACTIVE).execute()
            h.execute("VACUUM (ANALYZE) hog_upload")
            paths =
                h.createQuery("SELECT path FROM hog_upload WHERE catalog_id = :c AND owner = :owner")
                    .bind("c", catalogId).bind("owner", owner).mapTo(String::class.java).list()
        }
    }

    /** The plan of [sql] with its rows, run and rolled back, because EXPLAIN ANALYZE runs an UPDATE. */
    private fun plan(
        sql: String,
        bind: (Query) -> Query,
    ): String =
        db.jdbi.withHandleUnchecked { h: Handle ->
            h.begin()
            try {
                // Serial, so the counts do not depend on the machine's core count
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                bind(h.createQuery("EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) $sql"))
                    .mapTo(String::class.java).list().joinToString("\n")
            } finally {
                h.rollback()
            }
        }

    private fun assertLocksInIdOrder(plan: String) {
        assertThat(plan).describedAs("a Seq Scan reads every claim of the catalog:%n%s", plan)
            .doesNotContain("Seq Scan on hog_upload")
        assertThat(plan).describedAs("the rows must be locked after a sort by upload_id:%n%s", plan)
            .contains("LockRows")
            .containsPattern(SORT_BY_UPLOAD_ID.pattern)
    }

    @Test
    fun `renew finds the claims of its owner by the owner index and updates them by upload_id`() {
        val plan = plan(UploadService.RENEW_SQL) { it.bind("catalog", catalogId).bind("owner", owner) }
        assertLocksInIdOrder(plan)
        assertThat(plan).describedAs(plan).contains("Index Scan using hog_upload_owner")
            .containsPattern(PROBE_BY_UPLOAD_ID.pattern)
    }

    @Test
    fun `abandon finds the claims of its paths by an index and updates them by upload_id`() {
        val plan =
            plan(UploadService.ABANDON_SQL) {
                it.bind("catalog", catalogId).bind("owner", owner).bindArray("paths", String::class.java, paths)
            }
        assertLocksInIdOrder(plan)
        assertThat(plan).describedAs(plan).containsPattern(PROBE_BY_UPLOAD_ID.pattern)
    }

    @Test
    fun `register finds the claims of its paths by the path index`() {
        val plan =
            plan(UploadService.REGISTER_CLAIMS_SQL) {
                it.bind("catalog", catalogId).bindArray("paths", String::class.java, paths)
            }
        assertLocksInIdOrder(plan)
        assertThat(plan).describedAs(plan).contains("hog_upload_catalog_id_path_key")
    }
}
