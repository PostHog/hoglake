package com.posthog.hoglake.service

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.statement.SqlLogger
import org.jdbi.v3.core.statement.StatementContext
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

@Tag("integration")
class TruncateExpiryRaceIntegrationTest {
    @Test
    fun `expiry must not erase truncate barrier after floor validation`() {
        PgTestSupport.freshDatabase().use { db ->
            val catalogs = CatalogService(db.jdbi)
            catalogs.createCatalog("review", "s3://bucket/review")
            catalogs.createNamespace("review", "ns")
            val table = catalogs.createTable("review", "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
            val before = catalogs.getCatalog("review").headSnapshotId
            catalogs.truncateTable("review", "ns", "t", table.tableUuid)
            catalogs.createNamespace("review", "advance")
            db.jdbi.useHandle<Exception> { h ->
                h.execute("UPDATE hog_catalog SET snapshot_retention_seconds = 3600, consumer_floor = false")
                h.execute("UPDATE hog_snapshot SET snapshot_time = now() - interval '2 days'")
            }
            val fired = AtomicBoolean()
            db.jdbi.setSqlLogger(
                object : SqlLogger {
                    override fun logBeforeExecution(context: StatementContext) {
                        if (context.rawSql.contains("SELECT c.snapshot_id FROM hog_snapshot_change c") &&
                            fired.compareAndSet(false, true)
                        ) {
                            // JDBI reuses thread-bound handles, so expiry needs another thread
                            // to use a genuinely concurrent transaction.
                            Executors.newSingleThreadExecutor().use { executor ->
                                executor.submit {
                                    ExpiryService(db.jdbi).runOnce("review", batchSize = 1000)
                                }.get(10, TimeUnit.SECONDS)
                            }
                        }
                    }
                },
            )
            assertThatThrownBy { catalogs.changes("review", "ns", "t", before) }
                .isInstanceOf(HoglakeException.ReconciliationRequired::class.java)
            assertThat(fired.get()).isTrue()
            // A new request sees the advanced retention floor, while the in-flight
            // request above retained its truncate barrier on its original MVCC snapshot.
            assertThatThrownBy { catalogs.changes("review", "ns", "t", before) }
                .isInstanceOf(HoglakeException.Expired::class.java)
        }
    }
}
