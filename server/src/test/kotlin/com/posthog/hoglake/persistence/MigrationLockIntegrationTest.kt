package com.posthog.hoglake.persistence

import com.posthog.hoglake.Database
import com.posthog.hoglake.testing.PgTestSupport
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread

/**
 * The migration path must never hold a transaction open while a
 * migration runs `CREATE INDEX CONCURRENTLY` (V2__maintenance.sql and
 * V3__changefeed_index.sql both do), which is what
 * [Database.flywayConfig]'s transactional-lock setting buys.
 *
 * Every integration test already depends on that setting — they all
 * migrate — but they depend on it in the worst possible way: losing it
 * does not fail them, it HANGS them. Flyway's own lock transaction sits
 * `idle in transaction` while the concurrent index build waits on its
 * virtualxid, and neither side ever yields; the observed symptom is a
 * suite that runs until CI's job timeout with no failing assertion
 * anywhere. (Verified by deleting the setting: the run stopped at
 * `CREATE INDEX CONCURRENTLY hog_data_file_maintenance_scan`, waiting on
 * `Lock/virtualxid`, for as long as it was left alone.)
 *
 * This test makes that failure fast and legible instead. It migrates
 * over connections carrying a `lock_timeout`, so a migration that waits
 * on a lock at all gives up inside the timeout and throws, turning an
 * indefinite hang into a named red in seconds.
 */
@Tag("integration")
class MigrationLockIntegrationTest {
    @Test
    fun `migrations never wait on a lock held by Flyway's own transaction`() {
        PgTestSupport.freshDatabaseRaw("").use { db ->
            val ds =
                HikariDataSource(
                    HikariConfig().apply {
                        jdbcUrl = db.jdbcUrl
                        username = db.dataSource.username
                        password = db.dataSource.password
                        maximumPoolSize = 4
                        poolName = "migration-lock-probe"
                        // The whole point: a concurrent index build that
                        // ends up behind Flyway's transaction fails here
                        // rather than waiting forever. Comfortably longer
                        // than the honest run, which waits on nothing.
                        connectionInitSql = "SET lock_timeout = '30s'"
                    },
                )
            ds.use {
                // Throws on a lock wait; the assertion below just proves
                // we got the schema rather than an empty database.
                Database.migrate(it)
            }

            val indexes =
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery(
                        "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'",
                    ).mapTo(String::class.java).list()
                }
            // Every index built CONCURRENTLY — i.e. the statements that
            // can only succeed outside a transaction block. V10's is
            // listed here for the same reason as V2's and V3's: if the
            // non-transactional execution ever regresses, the migration
            // fails and the index is simply absent.
            assertThat(indexes)
                .describedAs("indexes created by the CONCURRENTLY migrations")
                .contains(
                    "hog_data_file_maintenance_scan",
                    "hog_data_file_changefeed",
                    "hog_data_file_maintenance_size_scan",
                )
        }
    }

    @Test
    fun `a replica waiting for the migration lock does not park a concurrent build`() {
        // V17's build is `CREATE INDEX CONCURRENTLY` on the biggest
        // table in the schema — 26 s at production size — and CIC waits
        // out every transaction older than itself.
        //
        // A BLOCKING `pg_advisory_lock` is such a transaction. It is an
        // open statement, an open statement publishes an xmin, and the
        // session publishing it is a second replica waiting on the very
        // migration the build belongs to: a chain, not a cycle, so
        // nothing detects it, and it unwinds only when that replica's
        // 60 s `statement_timeout` kills its boot. Measured on a 5M-row
        // fixture with writers running: 26 s alone, 58 s with a
        // blocking waiter (which died at 62 s), 26 s with a polling one.
        //
        // So `Database.awaitMigrationLock` POLLS. This test pins the
        // property that follows from it — a concurrent build completes
        // while a replica is waiting — rather than the implementation,
        // and it is the mutation that reds: put `pg_advisory_lock` back
        // and the build below waits out the whole `statement_timeout`
        // and fails.
        PgTestSupport.freshDatabase().use { db ->
            db.jdbi.useHandleUnchecked { h ->
                h.execute("CREATE TABLE lock_probe (id bigint, payload text)")
                h.execute(
                    "INSERT INTO lock_probe SELECT g, md5(g::text) FROM generate_series(1, 10000) g",
                )
            }
            // A replica mid-migration: the lock is held on its own
            // connection, which then sits IDLE — Flyway runs on other
            // pool connections, so the holder carries no snapshot. That
            // is what makes the WAITER the only candidate for the park.
            db.dataSource.connection.use { holder ->
                holder.createStatement().use { st ->
                    st.execute("SELECT pg_advisory_lock(${Database.MIGRATION_LOCK_KEY})")
                }
                val waiterDone = AtomicBoolean(false)
                val waiterFailure = AtomicReference<Throwable?>(null)
                val waiter =
                    thread(name = "booting-replica") {
                        try {
                            Database.migrate(db.dataSource)
                        } catch (e: Throwable) {
                            waiterFailure.set(e)
                        } finally {
                            waiterDone.set(true)
                        }
                    }
                try {
                    // The waiter is in its poll loop before the build
                    // starts; without this the build could finish before
                    // the waiter ever takes a snapshot and the test
                    // would pass over the blocking implementation too.
                    // Two: the holder's own connection carries the same
                    // query text, so one is the state before the waiter
                    // has asked for anything.
                    await().atMost(Duration.ofSeconds(30)).until { lockSessions(db) >= 2 }
                    // An xid between the waiter's first snapshot and the
                    // build: with nothing writing, the waiter's xmin
                    // equals the build's and no wait is possible, which
                    // would make this test pass for the wrong reason.
                    db.jdbi.useHandleUnchecked { h ->
                        h.execute("INSERT INTO lock_probe VALUES (-1, 'xid')")
                    }
                    assertThat(waiterDone.get())
                        .describedAs("the waiter must still be waiting while the build runs")
                        .isFalse()

                    db.dataSource.connection.use { builder ->
                        builder.createStatement().use { st ->
                            // Bounded, so the blocking implementation
                            // FAILS here instead of hanging the suite.
                            st.execute("SET statement_timeout = '20s'")
                            st.execute("CREATE INDEX CONCURRENTLY lock_probe_id ON lock_probe (id)")
                        }
                    }

                    assertThat(indexIsValid(db, "lock_probe_id"))
                        .describedAs(
                            "a concurrent build must complete while a replica waits for the " +
                                "migration lock; a waiter that holds a snapshot parks it in " +
                                "WaitForOlderSnapshots until its own statement timeout",
                        )
                        .isTrue()
                } finally {
                    holder.createStatement().use { st ->
                        st.execute("SELECT pg_advisory_unlock(${Database.MIGRATION_LOCK_KEY})")
                    }
                    waiter.join(Duration.ofSeconds(60).toMillis())
                }
                // And the waiter got the lock as soon as it was free,
                // then migrated: a poll that never acquires is a boot
                // that never happens.
                assertThat(waiterFailure.get()).describedAs("the waiting replica's migration").isNull()
                assertThat(waiterDone.get()).describedAs("the waiting replica finished").isTrue()
            }
        }
    }

    /**
     * Sessions that have asked for the migration advisory lock, however
     * they ask: the holder, plus any waiter. Matched on the KEY rather
     * than on the function name, so a change from blocking to polling
     * (or back) is visible to this test instead of hidden from it.
     */
    private fun lockSessions(db: PgTestSupport.TestDb): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT count(*) FROM pg_stat_activity
                WHERE query LIKE '%advisory_lock(${Database.MIGRATION_LOCK_KEY})%'
                  AND pid <> pg_backend_pid()
                """,
            ).mapTo(Long::class.java).one()
        }

    private fun indexIsValid(
        db: PgTestSupport.TestDb,
        name: String,
    ): Boolean =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid " +
                    "WHERE c.relname = :n",
            ).bind("n", name).mapTo(Boolean::class.javaObjectType).findOne().orElse(false)
        }
}
