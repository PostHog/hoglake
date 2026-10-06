package com.posthog.hoglake.service

import com.posthog.hoglake.testing.ExplainPlan
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
 * The plans of the statements that lock claim rows in `upload_id` order
 * ([UploadService.LOCK_OWNER_CLAIMS_SQL], [UploadService.LOCK_OWNER_PATHS_SQL]
 * and [UploadService.REGISTER_CLAIMS_SQL]) and of the UPDATEs that then
 * change the locked rows ([UploadService.RENEW_SQL] and
 * [UploadService.ABANDON_SQL]).
 *
 * The fixture is a `hog_upload` shaped like a long-lived catalog's, whose
 * claims are never deleted: the registered claims of finished operations,
 * swept tombstones that each carry their own `last_scheduled_at`, and the
 * active claims of 21 running operations, written interleaved, with random
 * paths as `claim` makes them. The statements are about the 1,000 active
 * claims of one of those operations. Each must reach exactly its rows
 * through an index, filter out none, and lock them after a sort by
 * `upload_id`.
 *
 * Measured on this fixture (Postgres 18, 261,000 claims): renew's lock
 * reads 283 buffers through `hog_upload_owner` and takes its 1,000 row
 * locks for about 1,000 more, and its UPDATE probes `hog_upload_pkey` 756
 * times in 3,263 buffers, then writes the new row versions for about 18
 * buffers a row. Abandon's UPDATE reads 5,032. Register probes the path key
 * 896 times in 6,486 buffers. The limits are two to four times these. A
 * plan that reached the catalog's other claims through these indexes would
 * filter rows out, which fails first.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class UploadLockPlanIntegrationTest {
    companion object {
        /** Claims of finished operations, all registered, 100 per operation. */
        const val REGISTERED = 200_000

        /** Abandoned claims that the sweep fenced, each at its own time. */
        const val TOMBSTONES = 50_000

        /** The active claims of the operation the statements are about. */
        const val ACTIVE = 1_000

        /** The active claims of the other 20 running operations. */
        const val OTHER_ACTIVE = 10_000

        /** The buffers each scan and lock may read (see the class comment for what they measured). */
        const val BUFFERS_OWNER_SCAN = 1L * ACTIVE
        const val BUFFERS_LOCK = 5L * ACTIVE
        const val BUFFERS_PKEY_SCAN = 10L * ACTIVE
        const val BUFFERS_PATH_SCAN = 13L * ACTIVE

        private const val SORT_BY_UPLOAD_ID = """Sort Key: (hog_upload\.)?upload_id\b"""
        private const val FILTERED_ROWS = """Rows Removed by Filter: [1-9]"""
    }

    private val db = PgTestSupport.freshDatabase()
    private val owner = UUID.randomUUID()
    private var catalogId = 0L
    private lateinit var paths: List<String>
    private lateinit var ids: List<UUID>

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
                SELECT :c, gen_random_uuid(), md5('registered' || (g / 100))::uuid, 's3://upload-plan',
                       's3://upload-plan/trino-upload/' || gen_random_uuid() || '.parquet', 'data', 'registered'
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("n", REGISTERED).execute()
            h.createUpdate(
                """
                INSERT INTO hog_upload (catalog_id, upload_id, owner, prefix, path, file_kind, state,
                                        expires_at, last_scheduled_at)
                SELECT :c, gen_random_uuid(), md5('abandoned' || (g / 100))::uuid, 's3://upload-plan',
                       's3://upload-plan/trino-upload/' || gen_random_uuid() || '.parquet', 'data', 'abandoned',
                       now() - interval '2 days', now() - make_interval(secs => g)
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("n", TOMBSTONES).execute()
            // The running operations write their claims at the same time, so
            // the claims of one operation are spread over the heap.
            h.createUpdate(
                """
                INSERT INTO hog_upload (catalog_id, upload_id, owner, prefix, path, file_kind)
                SELECT :c, gen_random_uuid(), CASE WHEN g <= :active THEN :owner ELSE md5('active' || (g % 20))::uuid END,
                       's3://upload-plan', 's3://upload-plan/trino-upload/' || gen_random_uuid() || '.parquet', 'data'
                FROM generate_series(1, :n) g
                ORDER BY random()
                """,
            ).bind(
                "c",
                catalogId,
            ).bind("owner", owner).bind("active", ACTIVE).bind("n", ACTIVE + OTHER_ACTIVE).execute()
            h.execute("VACUUM (ANALYZE) hog_upload")
            paths =
                h.createQuery("SELECT path FROM hog_upload WHERE catalog_id = :c AND owner = :owner")
                    .bind("c", catalogId).bind("owner", owner).mapTo(String::class.java).list()
            ids =
                h.createQuery("SELECT upload_id FROM hog_upload WHERE catalog_id = :c AND owner = :owner")
                    .bind("c", catalogId).bind("owner", owner).mapTo(UUID::class.java).list()
        }
    }

    /** The plan of [sql] with its counts, run and rolled back, because EXPLAIN ANALYZE runs an UPDATE. */
    private fun plan(
        sql: String,
        bind: (Query) -> Query,
    ): String =
        db.jdbi.withHandleUnchecked { h: Handle ->
            h.begin()
            try {
                bind(h.createQuery("EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) $sql"))
                    .mapTo(String::class.java).list().joinToString("\n")
            } finally {
                h.rollback()
            }
        }

    /** The buffers that the node whose line contains [node] read itself, at most [limit]. */
    private fun assertBuffers(
        plan: String,
        node: String,
        limit: Long,
    ) {
        val nodes = ExplainPlan.nodes(plan).filter { it.line.contains(node) }
        assertThat(nodes).describedAs("one %s in:%n%s", node, plan).hasSize(1)
        assertThat(nodes.single().buffers).describedAs("buffers of %s in:%n%s", node, plan).isBetween(1L, limit)
    }

    private fun assertReadsOnlyItsRows(plan: String) {
        assertThat(plan).describedAs("a Seq Scan reads every claim of the catalog:%n%s", plan)
            .doesNotContain("Seq Scan on hog_upload")
        assertThat(plan).describedAs("rows read and then thrown away:%n%s", plan)
            .doesNotContainPattern(FILTERED_ROWS)
    }

    private fun assertLocksInIdOrder(plan: String) {
        assertReadsOnlyItsRows(plan)
        assertThat(plan).describedAs("the rows must be locked after a sort by upload_id:%n%s", plan)
            .contains("LockRows (actual rows=$ACTIVE")
            .containsPattern(SORT_BY_UPLOAD_ID)
    }

    @Test
    fun `renew locks the claims of its owner through the owner index`() {
        val plan = plan(UploadService.LOCK_OWNER_CLAIMS_SQL) { it.bind("catalog", catalogId).bind("owner", owner) }
        assertLocksInIdOrder(plan)
        assertBuffers(plan, "Index Scan using hog_upload_owner", BUFFERS_OWNER_SCAN)
        assertBuffers(plan, "LockRows", BUFFERS_LOCK)
    }

    @Test
    fun `abandon locks the claims of its paths through an index`() {
        val plan =
            plan(UploadService.LOCK_OWNER_PATHS_SQL) {
                it.bind("catalog", catalogId).bind("owner", owner).bindArray("paths", String::class.java, paths)
            }
        assertLocksInIdOrder(plan)
        assertBuffers(plan, "LockRows", BUFFERS_LOCK)
    }

    @Test
    fun `register locks the claims of its paths through the path key`() {
        val plan =
            plan(UploadService.REGISTER_CLAIMS_SQL) {
                it.bind("catalog", catalogId).bindArray("paths", String::class.java, paths)
            }
        assertLocksInIdOrder(plan)
        assertThat(plan).describedAs("path must be a key of the scan, not a filter:%n%s", plan)
            .containsPattern("""Index Cond: \(.*path = ANY""")
        assertBuffers(plan, "hog_upload_catalog_id_path_key", BUFFERS_PATH_SCAN)
    }

    @Test
    fun `renew and abandon update the locked claims through the primary key`() {
        for (sql in listOf(UploadService.RENEW_SQL, UploadService.ABANDON_SQL)) {
            val plan = plan(sql) { it.bind("catalog", catalogId).bindArray("ids", UUID::class.java, ids) }
            assertReadsOnlyItsRows(plan)
            assertThat(plan).describedAs(plan).containsPattern("""Index Cond: \(.*upload_id = ANY""")
            assertBuffers(plan, "Index Scan using hog_upload_pkey", BUFFERS_PKEY_SCAN)
        }
    }
}
