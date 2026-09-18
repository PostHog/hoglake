package com.posthog.hoglake.service

import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
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
    fun `the new statistics queries run against a real instance`() {
        // Each of these touches a view or catalog whose shape this code
        // asserts; a wrong column or join fails here and nowhere else.
        val report = service.report()

        // Empty on a clean instance, but the query must execute.
        assertThat(report.replicationSlots).isEmpty()
        assertThat(report.commitLocks).isEmpty()
        assertThat(report.blindSpots).isNotEmpty()
        // Disk fullness is the one an operator must not assume is covered.
        assertThat(report.blindSpots.joinToString(" ")).contains("Disk")
    }

    @Test
    fun `checkpoint counters resolve on whichever view this server has`() {
        // PG 16 keeps them on pg_stat_bgwriter; PG 17 moved them to
        // pg_stat_checkpointer and REMOVED the old columns. The version
        // is chosen at run time, so this asserts the choice was right for
        // the server actually under test rather than for one of them.
        val server = service.report().server
        assertThat(server.checkpointsTimed)
            .describedAs("checkpoint counters on Postgres ${server.version}")
            .isNotNull()
        assertThat(server.checkpointsRequested).isNotNull()
        assertThat(server.tempFiles).isNotNegative()
    }

    @Test
    fun `a held commit lock is observed through the advisory-lock view`() {
        // The page's most schema-specific claim, pinned against a real
        // lock rather than a synthetic row.
        //
        // The holder runs on its own THREAD, which is not fussiness: JDBI
        // reuses the calling thread's handle for a nested call, so taking
        // the lock and reading the page on one thread would share a
        // connection and put report()'s isolation SET mid-transaction.
        // Production never does that — a route handler calls report()
        // fresh — and the holder is always a different session from the
        // observer, which is exactly what this arrangement reproduces.
        val catalogId = 4242L
        val locked = java.util.concurrent.CountDownLatch(1)
        val release = java.util.concurrent.CountDownLatch(1)
        val holder =
            Thread {
                db.jdbi.useHandleUnchecked { handle ->
                    handle.begin()
                    Locks.acquireCatalogCommitLock(handle, catalogId)
                    locked.countDown()
                    release.await()
                    handle.rollback()
                }
            }
        holder.start()
        try {
            assertThat(locked.await(30, java.util.concurrent.TimeUnit.SECONDS))
                .describedAs("holder acquired the lock")
                .isTrue()

            val ours = service.report().commitLocks.single { it.catalogId == catalogId }
            assertThat(ours.granted).isTrue()
            assertThat(ours.waiters).isZero()
            // The catalog id rides the low 32 bits of the advisory key, so
            // a wrong mask would surface here as a mismatched id.
            assertThat(ours.catalogId).isEqualTo(catalogId)
        } finally {
            release.countDown()
            holder.join(30_000)
        }

        // xact-scoped: gone the moment the holder's transaction ends.
        assertThat(service.report().commitLocks).isEmpty()
    }

    @Test
    fun `an invalid index is actually found, not just queried for`() {
        // This caught a real bug: the query lived in a RAW Kotlin string,
        // where `hog\\_%` reaches SQL unprocessed and matches NOTHING —
        // and an empty result is indistinguishable from a healthy
        // instance, so the finding would have been blind forever. A test
        // that only asserts "the query runs" cannot see that; this one
        // creates a genuinely invalid index and demands it come back.
        db.jdbi.useHandleUnchecked { handle ->
            handle.execute("CREATE TABLE IF NOT EXISTS hog_invalid_probe (id bigint)")
            handle.execute("CREATE INDEX hog_invalid_probe_idx ON hog_invalid_probe (id)")
            handle.execute(
                "UPDATE pg_index SET indisvalid = false " +
                    "WHERE indexrelid = 'hog_invalid_probe_idx'::regclass",
            )
        }
        try {
            val findings = service.report().findings
            assertThat(findings.map { it.code }).contains("invalid_indexes")
            assertThat(findings.single { it.code == "invalid_indexes" }.detail)
                .contains("hog_invalid_probe_idx")
        } finally {
            db.jdbi.useHandleUnchecked { it.execute("DROP TABLE IF EXISTS hog_invalid_probe") }
        }
    }

    @Test
    fun `the hog_ table filter matches the catalog's tables and nothing else`() {
        // The same escaping trap as above, on the filter every other
        // query here shares: a wrong pattern yields an empty list, which
        // reads as a clean database rather than as a broken query.
        db.jdbi.useHandleUnchecked { handle ->
            handle.execute("CREATE TABLE IF NOT EXISTS hogXnotours (id bigint)")
        }
        try {
            val names = service.report().tables.map { it.name }
            assertThat(names).contains("hog_data_file")
            // `_` is LIKE's wildcard; unescaped, this table would match.
            assertThat(names).doesNotContain("hogXnotours")
        } finally {
            db.jdbi.useHandleUnchecked { it.execute("DROP TABLE IF EXISTS hogXnotours") }
        }
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
