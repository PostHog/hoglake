package com.posthog.hoglake.compaction

import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.Collections
import java.util.UUID
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * The claim refresh deadlock (gigahog-prod-us, 1.4.2: 21 `deadlock
 * detected` in 17 minutes): two workers of ONE run refresh at once, each
 * renewing its own group row and every one of the run's bucket markers in
 * one UPDATE, so the markers are rows both statements lock.
 * `CompactionClaimRepo.refresh` now takes those locks in `group_key` order
 * before it updates.
 *
 * Both sessions run with index scans disabled, so a plain UPDATE meets
 * the rows in HEAP order (a bitmap heap scan), and the table is the shape
 * that makes heap order unstable: ten thousand rows of other tables over
 * many pages with free space scattered through them, so each refresh's new
 * row versions (never HOT: `expires_at` is indexed) land wherever the
 * free-space map points. A statement whose snapshot predates a sibling's
 * commit then meets the rows in a different order from one that started
 * after it. The pkey `= ANY` index scan returns key order, so the old
 * statement deadlocks only when the planner picks another path, as it did
 * on prod's claim table; the fixed statement's `ORDER BY` holds on every
 * path.
 *
 * The cycle needs a commit to land between one statement's start and a
 * later row lock, a window of microseconds. So the statement is also run
 * WIDENED — the repo's own SQL with a 2 ms `pg_sleep` evaluated per row,
 * between one row lock and the next, eight markers per worker — which
 * makes the 1.4.2 statement deadlock on every run tried (3 of 3) within
 * 300 iterations. The fixed statement is run widened and as the repo calls
 * it.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CompactionClaimRefreshDeadlockIntegrationTest {
    private companion object {
        const val ITERATIONS = 300

        /** The repo's statement with a 2 ms sleep per updated row (adds 0 s to the lease). */
        val WIDENED =
            CompactionClaimRepo.REFRESH_SQL.replace(
                "ELSE :bucketTtlSeconds END)",
                "ELSE :bucketTtlSeconds END) + make_interval(secs => length(pg_sleep(0.002)::text))",
            )
    }

    private val db = PgTestSupport.freshDatabase()
    private val catalogs = AtomicInteger()

    @AfterAll
    fun tearDown() = db.close()

    /** [CompactionClaimRepo.refresh]'s binds on [sql]; true when the group row was renewed. */
    private fun refreshVia(
        h: Handle,
        sql: String,
        catalogId: Long,
        group: String,
        run: UUID,
        markers: List<String>,
    ): Boolean =
        group in
            h.createQuery(sql)
                .bind("catalogId", catalogId).bind("tableId", 1L).bind("groupKey", group)
                .bindArray("keys", String::class.java, (listOf(group) + markers).distinct())
                .bind("claimant", run).bind("ttlSeconds", 3600.0).bind("bucketTtlSeconds", 600.0)
                .mapTo(String::class.java).list()

    /** Two workers of one run, refreshing [ITERATIONS] times each; every error they saw. */
    private fun race(widened: Boolean): List<String?> {
        val catalogId =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("INSERT INTO hog_catalog (name, data_path) VALUES (:n, 's3://r') RETURNING catalog_id")
                    .bind("n", "refresh-order-${catalogs.incrementAndGet()}").mapTo(Long::class.java).one()
            }
        val run = UUID.randomUUID()
        val m1 = CompactionClaimRepo.BUCKET_KEY_PREFIX + "m1"
        val m2 = CompactionClaimRepo.BUCKET_KEY_PREFIX + "m2"

        fun markers(vararg first: String) =
            first.toList() + (3..8).map { CompactionClaimRepo.BUCKET_KEY_PREFIX + "m$it" }
        db.jdbi.useHandleUnchecked { h ->
            // Filler on other tables, half of it deleted and vacuumed, so the
            // table spans many pages with free space scattered through them:
            // `expires_at` is indexed, so a refresh is never a HOT update, and
            // each new row version lands wherever the free-space map points —
            // which is what makes the heap order of the run's rows change
            // from one refresh to the next, as on prod's shared claim table.
            h.createUpdate(
                """
                INSERT INTO hog_compaction_claim
                       (catalog_id, table_id, group_key, input_file_ids, claimant, expires_at)
                SELECT :c, 1000 + g % 50, md5(g::text), ARRAY[g]::bigint[], gen_random_uuid(),
                       now() + interval '1 hour'
                  FROM generate_series(1, 20000) g
                """,
            ).bind("c", catalogId).execute()
            h.createUpdate(
                "DELETE FROM hog_compaction_claim WHERE catalog_id = :c AND table_id >= 1000 AND random() < 0.5",
            )
                .bind("c", catalogId).execute()
            h.execute("VACUUM hog_compaction_claim")
            for (key in listOf("gA", "gB") + markers(m1, m2)) {
                assertThat(CompactionClaimRepo.acquire(h, catalogId, 1L, key, emptyList(), run, 3600)).isTrue()
            }
        }
        val errors = Collections.synchronizedList(mutableListOf<String?>())
        val barrier = CyclicBarrier(2)
        val pool = Executors.newFixedThreadPool(2)

        fun worker(
            group: String,
            markers: List<String>,
        ) = pool.submit {
            db.jdbi.useHandleUnchecked { h ->
                h.execute("SET enable_indexscan = off")
                barrier.await(30, TimeUnit.SECONDS)
                repeat(ITERATIONS) {
                    try {
                        val live =
                            if (widened) {
                                refreshVia(h, WIDENED, catalogId, group, run, markers)
                            } else {
                                CompactionClaimRepo.refresh(
                                    h,
                                    catalogId,
                                    1L,
                                    group,
                                    run,
                                    3600,
                                    bucketKeys = markers,
                                    bucketTtlSeconds = 600,
                                )
                            }
                        if (!live) errors += "$group's own refresh answered false"
                    } catch (e: Exception) {
                        errors += e.message
                    }
                }
            }
        }
        try {
            // The two workers name the markers in OPPOSITE orders, as two
            // workers of one run can.
            val a = worker("gA", markers(m1, m2))
            val b = worker("gB", markers(m2, m1).reversed())
            a.get(180, TimeUnit.SECONDS)
            b.get(180, TimeUnit.SECONDS)
        } finally {
            pool.shutdownNow()
        }
        // Every row still live on its own lease: the gate and the TTLs are
        // the 1.4.2 statement's.
        val left =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT group_key, extract(epoch FROM expires_at - now())::float8 FROM hog_compaction_claim " +
                        "WHERE catalog_id = :c",
                ).bind("c", catalogId).map { rs, _ -> rs.getString(1) to rs.getDouble(2) }.list().toMap()
            }
        if (errors.isEmpty()) {
            assertThat(left.getValue("gA")).isGreaterThan(3_500.0)
            assertThat(left.getValue("gB")).isGreaterThan(3_500.0)
            for (m in markers(m1, m2)) assertThat(left.getValue(m)).describedAs(m).isBetween(500.0, 600.0)
        }
        return errors.toList()
    }

    @Test
    fun `two workers renewing the same markers in opposite orders never deadlock`() {
        // MUTATION: put back the 1.4.2 statement (one plain multi-row
        // UPDATE, locks in scan order) and the widened race fills with
        // `deadlock detected`.
        assertThat(race(widened = true)).describedAs("widened, %d pairs", ITERATIONS).isEmpty()
        assertThat(race(widened = false)).describedAs("the repo's call, %d pairs", ITERATIONS).isEmpty()
    }
}
