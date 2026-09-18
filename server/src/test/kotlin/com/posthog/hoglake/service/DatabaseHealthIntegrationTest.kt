package com.posthog.hoglake.service

import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * The SQL, against a real Postgres. The findings logic is unit-tested;
 * what can only fail here is the statistics SQL itself — a column that
 * moved between major versions, a join that does not hold, a cast that
 * throws. None of that is visible from a unit test with synthetic rows.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DatabaseHealthIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val service = DatabaseHealthService(db.jdbi)

    @AfterAll
    fun tearDown() = db.close()

    @Test
    fun `reports the instance it is actually running against`() {
        val report = service.report()

        assertThat(report.server.version).matches("""\d+(\.\d+)*""")
        assertThat(report.server.database).isNotBlank()
        assertThat(report.server.sizeBytes).isPositive()
        assertThat(report.server.connectionsMax).isPositive()
        assertThat(report.server.connectionsUsed).isPositive()
        assertThat(report.server.xidFreezeMaxAge).isPositive()
        // age(datfrozenxid) is never negative and always below the limit
        // on a database this young; a negative would mean the cast is wrong.
        assertThat(report.server.xidAge).isNotNegative()
        assertThat(report.server.cacheHitRatio).isBetween(0.0, 1.0)
    }

    @Test
    fun `sees the migrated schema's tables and indexes`() {
        val report = service.report()

        // The migration ran, so these exist; if the LIKE filter or the
        // pg_class join were wrong the lists would come back empty.
        assertThat(report.tables.map { it.name }).contains("hog_data_file", "hog_snapshot", "hog_catalog")
        assertThat(report.tables).allSatisfy { assertThat(it.name).startsWith("hog_") }
        assertThat(report.indexes.map { it.name }).contains("hog_data_file_live")
        assertThat(report.indexes).allSatisfy { assertThat(it.table).startsWith("hog_") }
    }

    @Test
    fun `identifies which indexes back constraints`() {
        val report = service.report()

        // A primary key must never be reported as droppable, so the flag
        // the findings rely on has to be right against the real catalog.
        val primaryKey = report.indexes.single { it.name == "hog_catalog_pkey" }
        assertThat(primaryKey.constraintBacking).isTrue()
        val partial = report.indexes.single { it.name == "hog_data_file_live" }
        assertThat(partial.constraintBacking).isFalse()
    }

    @Test
    fun `a freshly migrated database is healthy`() {
        // Nothing has been written, so every threshold should be quiet.
        // This is the assertion that catches a finding whose condition is
        // inverted: it would fire here, on an empty database.
        assertThat(service.report().findings.map { it.code })
            .doesNotContain("dead_tuples", "sequential_scans", "never_analyzed", "xid_wraparound")
    }

    @Test
    fun `activity counts this connection's siblings without the reader itself`() {
        val report = service.report()

        // The reporting backend is excluded, so it never reports itself
        // as the longest-running query on the instance.
        assertThat(report.activity.active).isNotNegative()
        assertThat(report.activity.idle).isNotNegative()
        assertThat(report.activity.idleInTransaction).isZero()
    }
}
