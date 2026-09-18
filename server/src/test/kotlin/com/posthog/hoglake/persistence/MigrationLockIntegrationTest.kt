package com.posthog.hoglake.persistence

import com.posthog.hoglake.Database
import com.posthog.hoglake.testing.PgTestSupport
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

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
            // The two indexes built CONCURRENTLY — i.e. the statements
            // that can only succeed outside a transaction block.
            assertThat(indexes)
                .describedAs("indexes created by the CONCURRENTLY migrations")
                .contains("hog_data_file_maintenance_scan", "hog_data_file_changefeed")
        }
    }
}
