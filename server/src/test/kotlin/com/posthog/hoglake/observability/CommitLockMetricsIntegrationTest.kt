package com.posthog.hoglake.observability

import com.posthog.hoglake.persistence.CommitLockPath
import com.posthog.hoglake.persistence.DatabaseHealthRepo
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.testing.ExplainPlan
import com.posthog.hoglake.testing.PgTestSupport
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

/**
 * `hoglake_commit_lock_db_waiters` / `_db_held_seconds` (#439): the
 * commit lock as POSTGRES sees it, published on the sampler tick from
 * the health page's own `pg_locks` read, beside the in-process gauges
 * observing the same convoy from inside this process.
 *
 * And the read's COST: one statement over the lock manager and the
 * activity view plus one `hog_catalog` probe per lock row, on a catalog
 * table padded so a scan of it would show.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CommitLockMetricsIntegrationTest {
    // The holders sit idle in transaction on purpose; the suite's 2 s
    // idle-in-transaction guard would kill them mid-convoy.
    private val db = PgTestSupport.freshDatabase(holdsTransactions = true)

    private companion object {
        const val WAITERS = 3

        /** Catalogs beside the locked one, so a seq scan of hog_catalog is not free. */
        const val OTHER_CATALOGS = 20_000
    }

    @BeforeAll
    fun seed() {
        db.jdbi.useHandleUnchecked { h ->
            h.execute("INSERT INTO hog_catalog (name, data_path) VALUES ('locked', 's3://locked')")
            h.execute(
                "INSERT INTO hog_catalog (name, data_path) " +
                    "SELECT 'pad-' || g, 's3://pad/' || g FROM generate_series(1, $OTHER_CATALOGS) g",
            )
            h.execute("ANALYZE hog_catalog")
        }
    }

    @AfterEach
    fun unbind() = Metrics.clear()

    @AfterAll
    fun tearDown() = db.close()

    private val catalogId: Long by lazy {
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = 'locked'").mapTo(Long::class.java).one()
        }
    }

    /**
     * One holder and [WAITERS] waiters on `locked`, [block] run while
     * they are all queued, then everybody released.
     */
    private fun <T> convoy(block: () -> T): T {
        val held = CountDownLatch(1)
        val release = CountDownLatch(1)
        val holder =
            thread(name = "db-gauge-holder") {
                db.jdbi.inTransactionUnchecked { h ->
                    Locks.acquireCatalogCommitLock(h, catalogId, "locked", CommitLockPath.COMMIT)
                    held.countDown()
                    release.await(60, TimeUnit.SECONDS)
                }
            }
        assertThat(held.await(30, TimeUnit.SECONDS)).isTrue()
        val waiters =
            (1..WAITERS).map { i ->
                thread(name = "db-gauge-waiter-$i") {
                    db.jdbi.inTransactionUnchecked { h ->
                        Locks.acquireCatalogCommitLock(h, catalogId, "locked", CommitLockPath.COMMIT)
                    }
                }
            }
        try {
            await().atMost(Duration.ofSeconds(30)).until {
                db.jdbi.withHandleUnchecked { h -> DatabaseHealthRepo.commitLocks(h) }.count { !it.granted } == WAITERS
            }
            return block()
        } finally {
            release.countDown()
            holder.join(30_000)
            waiters.forEach { it.join(30_000) }
        }
    }

    private fun value(
        registry: SimpleMeterRegistry,
        name: String,
    ): Double? = registry.find(name).tag("catalog", "locked").gauge()?.value()

    @Test
    fun `the sampler publishes the database's waiters and the holder's age, and retires them on release`() {
        val registry = SimpleMeterRegistry()
        Metrics.bind(registry)
        val metrics = CatalogMetrics(db.jdbi, registry)
        convoy {
            Thread.sleep(1_100)
            metrics.sampleOnce()
            assertThat(value(registry, "hoglake_commit_lock_db_waiters")).isEqualTo(WAITERS.toDouble())
            // The holder's transaction age: at least the second slept above.
            assertThat(value(registry, "hoglake_commit_lock_db_held_seconds")).isGreaterThanOrEqualTo(1.0)
            // The in-process view of the same convoy, from this process's
            // own sessions — here they are the same sessions.
            assertThat(value(registry, "hoglake_commit_lock_waiting")).isEqualTo(WAITERS.toDouble())
            assertThat(value(registry, "hoglake_commit_lock_held")).isEqualTo(1.0)
        }
        metrics.sampleOnce()
        // overwrite = true: a released lock's series go, rather than
        // freezing at the convoy's last reading.
        assertThat(value(registry, "hoglake_commit_lock_db_waiters")).isNull()
        assertThat(value(registry, "hoglake_commit_lock_db_held_seconds")).isNull()
        assertThat(value(registry, "hoglake_commit_lock_waiting")).isEqualTo(0.0)
        assertThat(value(registry, "hoglake_commit_lock_held")).isEqualTo(0.0)
    }

    @Test
    fun `an API replica (no extended groups) does not publish the instance-wide lock gauges`() {
        val registry = SimpleMeterRegistry()
        convoy { CatalogMetrics(db.jdbi, registry, extendedGroups = false).sampleOnce() }
        // MUTATION: run the commit-lock read in the core group and every
        // API replica republishes it, multiplying a `sum` by the replicas.
        assertThat(registry.find("hoglake_commit_lock_db_waiters").gauges()).isEmpty()
    }

    @Test
    fun `the read is one statement whose cost is the lock rows, not the catalog table`() {
        val plan =
            convoy {
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery("EXPLAIN (ANALYZE, BUFFERS) ${DatabaseHealthRepo.COMMIT_LOCKS_SQL}")
                        .mapTo(String::class.java)
                        .list()
                        .joinToString("\n")
                }
            }
        println(plan)
        val nodes = ExplainPlan.nodes(plan)
        // The catalog name comes from a key probe per lock row, never a
        // scan of hog_catalog. MUTATION: join on an unindexed expression
        // (`c.catalog_id::text = l.objid::text`) and this is a Seq Scan.
        val catalogScans = nodes.filter { "hog_catalog" in it.line }
        assertThat(catalogScans).isNotEmpty()
        assertThat(catalogScans).allSatisfy { assertThat(it.line).doesNotContain("Seq Scan") }
        // One probe per lock row (the holder and its waiters), not per catalog.
        assertThat(catalogScans.single().loops).isEqualTo(WAITERS + 1L)
        // The lock manager is read twice — the rows and the waiter count's
        // subplan, whose function scan rescans its materialized result
        // rather than calling pg_lock_status() again per row (measured on
        // PG 18: 4 loops at ~0.02 ms, 0.27 ms for the statement).
        assertThat(nodes.count { "Function Scan on pg_lock_status" in it.line }).isEqualTo(2)
        // No other hog_* relation is read at all.
        assertThat(nodes.filter { Regex("""\bhog_(?!catalog)""").containsMatchIn(it.line) }).isEmpty()
        // The whole statement touches a handful of pages: the catalog
        // probes (a few each) and nothing that grows with the manifest.
        val buffers = nodes.firstOrNull()?.buffers ?: 0
        assertThat(buffers).describedAs("buffers of the whole statement").isLessThan(100)
    }
}
