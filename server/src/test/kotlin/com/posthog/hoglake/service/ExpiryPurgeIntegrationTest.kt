package com.posthog.hoglake.service

import com.posthog.hoglake.Database
import com.posthog.hoglake.observability.ExpiryGauges
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.testing.PgTestSupport
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.jdbi.v3.core.statement.SqlLogger
import org.jdbi.v3.core.statement.StatementContext
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference

/**
 * The two-phase sweep's contract: WHAT RUNS UNDER THE COMMIT LOCK and
 * WHAT DOES NOT.
 *
 * `ExpiryServiceIntegrationTest` covers the floor math and the end state
 * of a sweep whose purge drains — which is every sweep at the compiled
 * defaults, so those expectations are unchanged by the split and are
 * deliberately left where they are. This file covers the part that only
 * exists because the split exists: the lock boundary, the page
 * transactions, a failed page, the run budget, and what a concurrent
 * compaction commit may and may not do to a row the purge is walking.
 *
 * THE LOCK ASSERTIONS ARE TRIGGERS, NOT POLLING. A test that samples
 * `pg_locks` from a second connection while a sweep runs can only fail
 * when it happens to sample at the right moment; a `BEFORE DELETE`
 * trigger that asks "is the catalog commit lock held RIGHT NOW" runs
 * inside the very transaction under test, once per row, and cannot miss.
 * Moving the purge back inside phase A's transaction reds
 * [the file purge never deletes a row while the commit lock is held];
 * taking the lock out of phase A reds its sibling.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExpiryPurgeIntegrationTest {
    private companion object {
        /**
         * Any node that READS `hog_data_file` to produce the page: a
         * sequential scan, an index scan (`Index Scan using <idx> on
         * hog_data_file`), a bitmap heap scan. The `Tid Scan` that
         * fetches the named tuples is excluded at the call site — it is
         * a lookup of a bounded array and says nothing about the cost of
         * finding the page.
         */
        val SCAN_NODE = Regex("""Scan\b.*\bhog_data_file\b""")
    }

    private val db = PgTestSupport.freshDatabase()
    private val jdbi get() = db.jdbi

    @AfterAll
    fun tearDown() = db.close()

    // ---- fixture -----------------------------------------------------------

    /**
     * A catalog with snapshots 0..head, all aged past [retention], one
     * table, and [endedBelowFloor] + [live] file rows INTERLEAVED.
     *
     * Interleaved because that is the only honest shape: production's
     * ended rows are whatever compaction retired, scattered across the
     * manifest, so the purge pays a heap fetch per row (V19's header
     * measured the difference a clustered fixture hides). Ended rows
     * carry `end_snapshot = 1`, which every floor this file advances is
     * at or above.
     *
     * The interleaving is integer Bresenham — `floor(g*ended/total) >
     * floor((g-1)*ended/total)` is true for EXACTLY [endedBelowFloor] of
     * the [live] + [endedBelowFloor] rows, spread evenly — rather than a
     * modulus, which collapses to "every row" as soon as the ended
     * fraction passes a half. The high-fraction fixtures in this file
     * are the ones that matter (a backlog is mostly ended rows), so a
     * seeding shortcut that quietly stopped interleaving there would
     * hide the clustered-heap artifact V19's header exists to warn
     * about.
     */
    private fun seed(
        name: String,
        endedBelowFloor: Int,
        live: Int,
        head: Long = 5,
        retention: Long? = 60,
        stats: Boolean = false,
    ): Long =
        jdbi.withHandleUnchecked { h ->
            val catalogId =
                h.createQuery(
                    """
                    INSERT INTO hog_catalog
                        (name, data_path, last_snapshot_id, snapshot_retention_seconds, consumer_floor)
                    VALUES (:name, 's3://bucket/p', :head, :retention, false)
                    RETURNING catalog_id
                    """,
                )
                    .bind("name", name)
                    .bind("head", head)
                    .apply {
                        if (retention == null) {
                            bindNull("retention", java.sql.Types.BIGINT)
                        } else {
                            bind("retention", retention)
                        }
                    }
                    .mapTo(Long::class.java)
                    .one()
            for (s in 0..head) {
                h.execute(
                    "INSERT INTO hog_snapshot (catalog_id, snapshot_id, snapshot_time, schema_version) " +
                        "VALUES (?, ?, now() - make_interval(secs => 3600), 0)",
                    catalogId,
                    s,
                )
            }
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 1, 0)",
                catalogId,
            )
            // One statement, interleaved, so the two populations share
            // no run of heap pages.
            val total = endedBelowFloor + live
            h.createUpdate(
                """
                INSERT INTO hog_data_file
                    (catalog_id, data_file_id, table_id, begin_snapshot, end_snapshot, path,
                     record_count, file_size_bytes, row_id_start)
                SELECT :c, g, 1, 0,
                       CASE WHEN (g::bigint * :ended) / :total
                                 > ((g - 1)::bigint * :ended) / :total
                            THEN 1 ELSE NULL END,
                       's3://b/' || :c || '/f' || g,
                       120, 4096, g * 120
                FROM generate_series(1, :total) g
                """,
            )
                .bind("c", catalogId)
                .bind("total", total)
                .bind("ended", endedBelowFloor)
                .execute()
            if (stats) {
                // The cascade that dominates the per-row cost in
                // production: 26 stats rows per file on a 25-column
                // table (the reserved row-id field is the 26th), plus
                // one partition value — all three children of
                // `hog_data_file`, so a page fires all three RI
                // triggers as production's does.
                h.createUpdate(
                    """
                    INSERT INTO hog_file_column_stats
                        (catalog_id, data_file_id, field_id, value_count, null_count)
                    SELECT :c, f.data_file_id, s, 120, 0
                    FROM hog_data_file f, generate_series(1, 26) s
                    WHERE f.catalog_id = :c
                    """,
                )
                    .bind("c", catalogId)
                    .execute()
                h.createUpdate(
                    """
                    INSERT INTO hog_file_partition_value
                        (catalog_id, data_file_id, key_index, value)
                    SELECT :c, f.data_file_id, 0, '2026-10-01'
                    FROM hog_data_file f WHERE f.catalog_id = :c
                    """,
                )
                    .bind("c", catalogId)
                    .execute()
            }
            catalogId
        }

    /**
     * One delete vector per ENDED data file, with [endSnapshot] as its
     * own state — `null` for a live vector riding a doomed file (the
     * cascade's victim), a value for a superseded one.
     */
    private fun seedVectorPerEndedFile(
        catalogId: Long,
        endSnapshot: Long?,
    ) = seedVectorFor(catalogId, catalogId, "end_snapshot IS NOT NULL", endSnapshot)

    /** One delete vector per LIVE data file — the superseded arm's population. */
    private fun seedVectorPerLiveFile(
        catalogId: Long,
        endSnapshot: Long?,
    ) = seedVectorFor(catalogId, catalogId, "end_snapshot IS NULL", endSnapshot)

    private fun seedVectorFor(
        catalogId: Long,
        idBase: Long,
        predicate: String,
        endSnapshot: Long?,
    ) = jdbi.useHandleUnchecked { h ->
        h.createUpdate(
            """
            INSERT INTO hog_delete_file
                (catalog_id, delete_file_id, table_id, data_file_id, begin_snapshot,
                 end_snapshot, path, delete_count, file_size_bytes)
            SELECT :c, f.data_file_id, 1, f.data_file_id, 0, :end,
                   's3://b/' || :c || '/dv' || f.data_file_id, 1, 10
            FROM hog_data_file f
            WHERE f.catalog_id = :c AND f.$predicate
            """,
        )
            .bind("c", catalogId)
            .apply {
                if (endSnapshot == null) {
                    bindNull("end", java.sql.Types.BIGINT)
                } else {
                    bind("end", endSnapshot)
                }
            }
            .execute()
    }

    /** One vector on every [nth] data file, whatever its state — a bulk relation. */
    private fun seedVectorEveryNthFile(
        catalogId: Long,
        nth: Int,
    ) = jdbi.useHandleUnchecked { h ->
        h.createUpdate(
            """
            INSERT INTO hog_delete_file
                (catalog_id, delete_file_id, table_id, data_file_id, begin_snapshot,
                 end_snapshot, path, delete_count, file_size_bytes)
            SELECT :c, f.data_file_id, 1, f.data_file_id, 0, NULL,
                   's3://b/' || :c || '/dv' || f.data_file_id, 1, 10
            FROM hog_data_file f
            WHERE f.catalog_id = :c AND f.data_file_id % :nth = 0
            """,
        ).bind("c", catalogId).bind("nth", nth).execute()
    }

    private fun vectorPathsOf(catalogId: Long): Set<String> =
        jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT path FROM hog_delete_file WHERE catalog_id = :c")
                .bind("c", catalogId).mapTo(String::class.java).list().toSet()
        }

    private fun queuedPaths(
        catalogId: Long,
        kind: String,
    ): Set<String> =
        jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT path FROM hog_file_removal WHERE catalog_id = :c AND file_kind = :k " +
                    "AND reason = 'snapshot_expiry'",
            )
                .bind("c", catalogId).bind("k", kind).mapTo(String::class.java).list().toSet()
        }

    private fun count(
        sql: String,
        catalogId: Long,
    ): Long =
        jdbi.withHandleUnchecked { h ->
            h.createQuery(sql).bind("c", catalogId).mapTo(Long::class.java).one()
        }

    private fun fileRows(catalogId: Long) = count("SELECT count(*) FROM hog_data_file WHERE catalog_id = :c", catalogId)

    private fun endedBelowFloor(catalogId: Long) =
        count(
            """
            SELECT count(*) FROM hog_data_file f
            JOIN hog_catalog c ON c.catalog_id = f.catalog_id
            WHERE f.catalog_id = :c AND f.end_snapshot IS NOT NULL
              AND f.end_snapshot <= c.earliest_snapshot_id
            """,
            catalogId,
        )

    private fun queued(catalogId: Long) =
        count(
            "SELECT count(*) FROM hog_file_removal WHERE catalog_id = :c AND reason = 'snapshot_expiry'",
            catalogId,
        )

    /**
     * Every purged path is in the removal queue — the no-orphan
     * property, asserted as a SET rather than as two counts, because two
     * equal counts over disjoint sets would pass.
     */
    private fun purgedPathsAreAllQueued(
        catalogId: Long,
        seededPaths: Set<String>,
    ) {
        val surviving =
            jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT path FROM hog_data_file WHERE catalog_id = :c")
                    .bind("c", catalogId).mapTo(String::class.java).list().toSet()
            }
        val queuedPaths =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT path FROM hog_file_removal WHERE catalog_id = :c AND reason = 'snapshot_expiry'",
                )
                    .bind("c", catalogId).mapTo(String::class.java).list().toSet()
            }
        assertThat(queuedPaths)
            .describedAs("every path whose row the purge deleted must be queued for cleanup")
            .isEqualTo(seededPaths - surviving)
    }

    private fun seededPaths(catalogId: Long): Set<String> =
        jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT path FROM hog_data_file WHERE catalog_id = :c")
                .bind("c", catalogId).mapTo(String::class.java).list().toSet()
        }

    /**
     * A `BEFORE DELETE` trip wire on `hog_data_file` that raises once
     * [rows] rows have been deleted.
     *
     * A COUNTER, NOT A PREDICATE ON THE ROW: a page's `LIMIT` takes rows
     * in no defined order, so "fail on this path" would fail on an
     * arbitrary page number and the test could not say how much work
     * survived. The counter is rolled back with the page that tripped
     * it, which is exactly the semantics under test — the next attempt
     * meets the same wire until the test removes it.
     */
    private fun tripAfter(rows: Int) =
        jdbi.useHandleUnchecked { h ->
            h.execute("DROP TABLE IF EXISTS purge_trip")
            h.execute("CREATE TABLE purge_trip (n int)")
            h.execute("INSERT INTO purge_trip VALUES (?)", rows)
            h.execute(
                """
                CREATE OR REPLACE FUNCTION purge_trip_fn() RETURNS trigger AS ${'$'}${'$'}
                DECLARE left_n int;
                BEGIN
                    UPDATE purge_trip SET n = n - 1 RETURNING n INTO left_n;
                    IF left_n < 0 THEN
                        RAISE EXCEPTION 'tripped on purpose';
                    END IF;
                    RETURN OLD;
                END ${'$'}${'$'} LANGUAGE plpgsql
                """,
            )
            h.execute(
                "CREATE TRIGGER purge_trip_fn BEFORE DELETE ON hog_data_file " +
                    "FOR EACH ROW EXECUTE FUNCTION purge_trip_fn()",
            )
        }

    private fun dropTrip() =
        jdbi.useHandleUnchecked { h ->
            h.execute("DROP TRIGGER IF EXISTS purge_trip_fn ON hog_data_file")
            h.execute("DROP TABLE IF EXISTS purge_trip")
        }

    /** The advisory-lock key `Locks` builds, restated for SQL. */
    private fun lockGuard(
        catalogId: Long,
        table: String,
        event: String,
        message: String,
        negate: Boolean,
    ) = jdbi.useHandleUnchecked { h ->
        val fn = "guard_${table}_$event"
        h.execute(
            """
            CREATE OR REPLACE FUNCTION $fn() RETURNS trigger AS ${'$'}${'$'}
            BEGIN
                IF ${if (negate) "NOT" else ""} EXISTS (
                    SELECT 1 FROM pg_locks
                    WHERE locktype = 'advisory'
                      AND classid = ${Locks.CATALOG_COMMIT_LOCK_CLASS}
                      AND objid = $catalogId
                      AND objsubid = 1
                      AND granted
                ) THEN
                    RAISE EXCEPTION '$message';
                END IF;
                RETURN OLD;
            END ${'$'}${'$'} LANGUAGE plpgsql
            """,
        )
        h.execute("CREATE TRIGGER $fn BEFORE $event ON $table FOR EACH ROW EXECUTE FUNCTION $fn()")
    }

    private fun dropGuard(
        table: String,
        event: String,
    ) = jdbi.useHandleUnchecked { h ->
        h.execute("DROP TRIGGER IF EXISTS guard_${table}_$event ON $table")
    }

    // ---- the lock boundary -------------------------------------------------

    @Test
    fun `the file purge never deletes a row while the commit lock is held`() {
        // THE WHOLE POINT OF THE CHANGE, as an assertion. The
        // 2026-10-01 incident was this statement under this lock: 12,288
        // rows and their stats cascade, 17-25 s of hold a minute, every
        // commit queued behind it for sixteen hours. The trigger fires
        // once per deleted row inside the purge's own transaction, so
        // moving the purge back into phase A fails this immediately and
        // for every row.
        val catalogId = seed("purge-unlocked", endedBelowFloor = 40, live = 60)
        lockGuard(
            catalogId,
            "hog_data_file",
            "DELETE",
            "expiry purged a file row while holding the catalog commit lock",
            negate = false,
        )
        try {
            val result = ExpiryService(jdbi).runOnce("purge-unlocked", batchSize = 100)
            assertThat(result.dataFilesPurged).isEqualTo(40)
            assertThat(result.purgeTruncated).isFalse()
            assertThat(fileRows(catalogId)).isEqualTo(60)
        } finally {
            dropGuard("hog_data_file", "DELETE")
        }
    }

    @Test
    fun `the floor advance still happens under the commit lock`() {
        // The other half, and it has to be asserted too: "nothing holds
        // the lock" would also pass the test above. The snapshot range
        // delete is phase A's, and it must serialize against commits —
        // a sweep that advanced the floor without the lock could expire
        // a snapshot a commit in flight is about to read.
        val catalogId = seed("advance-locked", endedBelowFloor = 4, live = 4)
        lockGuard(
            catalogId,
            "hog_snapshot",
            "DELETE",
            "expiry advanced the floor without holding the catalog commit lock",
            negate = true,
        )
        try {
            val result = ExpiryService(jdbi).runOnce("advance-locked", batchSize = 100)
            assertThat(result.snapshotsExpired).isEqualTo(5)
            assertThat(result.newEarliestSnapshotId).isEqualTo(5)
        } finally {
            dropGuard("hog_snapshot", "DELETE")
        }
    }

    // ---- a page is a transaction -------------------------------------------

    @Test
    fun `a page that fails keeps the pages before it and is counted, not read as idle`() {
        // Each page is its own transaction, so a failure cannot
        // un-delete what earlier pages committed — and it cannot
        // un-queue their paths either, since the delete and the queue
        // insert are one statement.
        //
        // THE FAILURE IS COUNTED BECAUSE ZERO IS AMBIGUOUS. Without
        // `purge_failures` a page that throws reports exactly what an
        // idle sweep reports, which is how the receipt purge's 58 GiB
        // would have sat there looking healthy forever.
        val catalogId = seed("purge-pagefail", endedBelowFloor = 5, live = 5)
        val paths = seededPaths(catalogId)
        jdbi.useHandleUnchecked { h ->
            // A counter, not a predicate on the row: the page's `LIMIT`
            // takes rows in no defined order, so "fail on this path"
            // would fail on an arbitrary page number. The counter is
            // rolled back with its own page, which is exactly the
            // semantics being asserted — the next sweep meets the same
            // trip wire until the test removes it.
            h.execute("CREATE TABLE purge_trip (n int)")
            h.execute("INSERT INTO purge_trip VALUES (2)")
            h.execute(
                """
                CREATE OR REPLACE FUNCTION purge_trip_fn() RETURNS trigger AS ${'$'}${'$'}
                DECLARE left_n int;
                BEGIN
                    UPDATE purge_trip SET n = n - 1 RETURNING n INTO left_n;
                    IF left_n < 0 THEN
                        RAISE EXCEPTION 'tripped on purpose';
                    END IF;
                    RETURN OLD;
                END ${'$'}${'$'} LANGUAGE plpgsql
                """,
            )
            h.execute(
                "CREATE TRIGGER purge_trip_fn BEFORE DELETE ON hog_data_file " +
                    "FOR EACH ROW EXECUTE FUNCTION purge_trip_fn()",
            )
        }
        try {
            val result =
                ExpiryService(jdbi, purgePage = 1).runOnce("purge-pagefail", batchSize = 100)
            assertThat(result.dataFilesPurged)
                .describedAs("two pages committed before the third threw")
                .isEqualTo(2)
            assertThat(result.purgeFailures).isEqualTo(1)
            assertThat(result.purgeTruncated).isTrue()
            assertThat(result.purgeRemaining)
                .describedAs("and the sweep says how much it left")
                .isEqualTo(3)
            assertThat(result.dataFilesQueued)
                .describedAs("queued rides purged, always")
                .isEqualTo(2)
            assertThat(fileRows(catalogId)).isEqualTo(8)
            assertThat(queued(catalogId)).isEqualTo(2)
            purgedPathsAreAllQueued(catalogId, paths)
        } finally {
            jdbi.useHandleUnchecked { h ->
                h.execute("DROP TRIGGER IF EXISTS purge_trip_fn ON hog_data_file")
                h.execute("DROP TABLE IF EXISTS purge_trip")
            }
        }
        // And the next sweep, with the trip wire gone, finishes the job
        // from the oldest eligible row — no cursor to resume, because
        // the predicate IS the cursor.
        val next = ExpiryService(jdbi).runOnce("purge-pagefail", batchSize = 100)
        assertThat(next.dataFilesPurged).isEqualTo(3)
        assertThat(next.purgeFailures).isEqualTo(0)
        assertThat(next.purgeTruncated).isFalse()
        assertThat(next.purgeRemaining).isEqualTo(0)
        assertThat(endedBelowFloor(catalogId)).isEqualTo(0)
        assertThat(fileRows(catalogId)).isEqualTo(5)
    }

    @Test
    fun `a crash between the phases leaves eligible rows and no orphaned object`() {
        // The budget of 0 IS the crash: phase A commits the floor
        // advance and phase B never runs a page. That is the state a pod
        // killed between the two phases leaves, and the properties that
        // have to hold in it are (1) nothing is queued for a row that is
        // still there, and (2) the next sweep drains it even though it
        // advances no floor of its own.
        val catalogId = seed("purge-crash", endedBelowFloor = 20, live = 20)
        val paths = seededPaths(catalogId)
        val crashed =
            ExpiryService(jdbi, purgeBudgetMs = 0).runOnce("purge-crash", batchSize = 100)
        assertThat(crashed.snapshotsExpired).isEqualTo(5)
        assertThat(crashed.newEarliestSnapshotId).isEqualTo(5)
        assertThat(crashed.dataFilesPurged).isEqualTo(0)
        assertThat(crashed.purgeTruncated).isTrue()
        assertThat(crashed.purgeRemaining).isEqualTo(20)
        assertThat(queued(catalogId))
            .describedAs("nothing may be queued for a row that still exists")
            .isEqualTo(0)
        assertThat(endedBelowFloor(catalogId)).isEqualTo(20)

        // The next sweep advances nothing (the floor is already at head)
        // and must still drain. A purge gated on the advance is how this
        // backlog would become permanent.
        val drained = ExpiryService(jdbi).runOnce("purge-crash", batchSize = 100)
        assertThat(drained.snapshotsExpired).isEqualTo(0)
        assertThat(drained.newEarliestSnapshotId).isEqualTo(5)
        assertThat(drained.dataFilesPurged).isEqualTo(20)
        assertThat(drained.purgeTruncated).isFalse()
        assertThat(endedBelowFloor(catalogId)).isEqualTo(0)
        assertThat(fileRows(catalogId)).isEqualTo(20)
        purgedPathsAreAllQueued(catalogId, paths)
    }

    @Test
    fun `every page takes its own delete vectors, queued, across more pages than one`() {
        // THE ORPHAN RULE, and the shape that makes it a fact rather
        // than a gate. `hog_delete_file` FKs to `hog_data_file`
        // ON DELETE CASCADE, so purging a data-file row takes its
        // vectors with it and queues NOTHING — a leaked puffin object
        // that cleanup never hears about and `/verify`'s orphans arm
        // cannot find, because finding it needs a live row.
        //
        // This used to be a cross-arm gate: "drain every eligible vector
        // first, and start the data-file arm once the vector arm's page
        // comes back short". A full page comes back short whenever
        // anything else removed one of its rows — another replica's loop,
        // a manual trigger racing it, `RetirementService.DV_DELETE_SQL`
        // under only the commit lock — so one race leaked objects
        // permanently. Now each page deletes ITS files' vectors in its
        // own statement, and the property is per page.
        //
        // THE FIXTURE IS BIGGER THAN A PAGE on purpose: at 2,500 files
        // over pages of 1,000 the walk is three pages, so the property
        // is asserted across page boundaries rather than inside one
        // statement that happened to see everything.
        // `stats = true` seeds the OTHER two cascading children as well,
        // so this is also the only integration test that drives a page
        // with all three RI triggers loaded — the shape
        // `ExpiryPurgeCostMeasurement` prices.
        val catalogId = seed("purge-dv-pages", endedBelowFloor = 2_500, live = 500, stats = true)
        seedVectorPerEndedFile(catalogId, endSnapshot = null)
        val vectorPaths = vectorPathsOf(catalogId)
        assertThat(vectorPaths).hasSize(2_500)

        val result = ExpiryService(jdbi).runOnce("purge-dv-pages", batchSize = 100)
        assertThat(result.dataFilesPurged).isEqualTo(2_500)
        assertThat(result.deleteFilesQueued)
            .describedAs("every vector riding a purged file is counted")
            .isEqualTo(2_500)
        assertThat(result.purgePages)
            .describedAs("three pages of 1,000 for the files; the vectors ride them")
            .isEqualTo(3)
        assertThat(count("SELECT count(*) FROM hog_delete_file WHERE catalog_id = :c", catalogId))
            .describedAs("no vector may survive its data file")
            .isEqualTo(0)
        // THE ASSERTION A MISSING `vectors` CTE REDS: the cascade would
        // still remove every one of these rows, and not one of their
        // paths would be here.
        assertThat(queuedPaths(catalogId, "delete"))
            .describedAs("and every one of them is queued for cleanup")
            .isEqualTo(vectorPaths)
        assertThat(
            count("SELECT count(*) FROM hog_file_column_stats WHERE catalog_id = :c", catalogId),
        )
            .describedAs("the stats cascade took the purged files' rows and left the live ones")
            .isEqualTo(500L * 26)
        assertThat(
            count("SELECT count(*) FROM hog_file_partition_value WHERE catalog_id = :c", catalogId),
        )
            .isEqualTo(500L)
    }

    @Test
    fun `a walk stopped mid-way leaves no vector without its file and no file without its vector`() {
        // The same property under a STOP, which is where a cross-arm
        // gate used to be able to leak: whatever went, went with its
        // vectors and their paths, and what did not go is untouched.
        //
        // The stop is a trip wire rather than a budget, so the row count
        // is exact: a wall-clock budget small enough to stop mid-walk is
        // a test whose "how far did it get" depends on the machine.
        val catalogId = seed("purge-dv-truncated", endedBelowFloor = 2_000, live = 200)
        seedVectorPerEndedFile(catalogId, endSnapshot = null)
        tripAfter(250)
        val result =
            try {
                ExpiryService(jdbi, purgePage = 100).runOnce("purge-dv-truncated", batchSize = 100)
            } finally {
                dropTrip()
            }
        assertThat(result.purgeTruncated).isTrue()
        assertThat(result.purgeFailures)
            .describedAs("EVERY rung of the ladder is a failed page, not only the give-up")
            .isEqualTo(7)
        // EXACTLY what the wire allowed, which is the halving converging:
        // two pages of 100 commit, the third trips at 50 and rolls back
        // (its counter decrements with it), halves to 50 and commits
        // those, then every further page trips on its first row and
        // halves down to one before the walk gives up. 250 is the trip
        // count — a walk that gave up on the first failure would stop at
        // 200, and one that retried without halving would loop.
        assertThat(result.dataFilesPurged).isEqualTo(250)
        assertThat(
            count(
                """
                SELECT count(*) FROM hog_delete_file dv
                WHERE dv.catalog_id = :c
                  AND NOT EXISTS (SELECT 1 FROM hog_data_file df
                                   WHERE df.catalog_id = dv.catalog_id
                                     AND df.data_file_id = dv.data_file_id)
                """,
                catalogId,
            ),
        )
            .describedAs("a vector whose data file is gone is an orphan by construction")
            .isEqualTo(0)
        assertThat(result.deleteFilesQueued)
            .describedAs("as many vectors queued as files purged, since each file had one")
            .isEqualTo(result.dataFilesPurged)
        assertThat(queuedPaths(catalogId, "delete")).hasSize(result.dataFilesPurged.toInt())
    }

    @Test
    fun `the superseded-vector arm pages, is probed, and counts into purge_remaining`() {
        // The arm that is left standalone: vectors whose OWN
        // end_snapshot is below the floor, riding data files that are
        // still live. They have nothing to ride out, so no data-file
        // page will ever take them.
        //
        // More than one page of them, for the reason the sibling test
        // gives — and a budget that stops inside the arm, to pin that
        // `purge_remaining` counts BOTH tables. An earlier version
        // counted `hog_data_file` only, so a purge stopped in this arm
        // reported `truncated = true, remaining = 0`: "stopped, nothing
        // left", the one thing it must never say.
        val catalogId = seed("purge-superseded", endedBelowFloor = 0, live = 2_500)
        seedVectorPerLiveFile(catalogId, endSnapshot = 1)
        val vectorPaths = vectorPathsOf(catalogId)
        assertThat(vectorPaths).hasSize(2_500)

        val stopped =
            ExpiryService(jdbi, purgePage = 100, purgeBudgetMs = 0)
                .runOnce("purge-superseded", batchSize = 100)
        assertThat(stopped.purgeTruncated).isTrue()
        assertThat(stopped.purgeRemaining)
            .describedAs("purge_remaining must see the vector arm's backlog, not only the data arm's")
            .isGreaterThan(0)
        assertThat(stopped.dataFilesPurged)
            .describedAs("there are no eligible data files in this fixture at all")
            .isEqualTo(0)

        val drained = ExpiryService(jdbi).runOnce("purge-superseded", batchSize = 100)
        assertThat(drained.purgeTruncated).isFalse()
        assertThat(drained.purgePages)
            .describedAs("three pages of 1,000 through the arm's own paging")
            .isEqualTo(3)
        assertThat(drained.deleteFilesQueued + stopped.deleteFilesQueued).isEqualTo(2_500)
        assertThat(queuedPaths(catalogId, "delete")).isEqualTo(vectorPaths)
        assertThat(count("SELECT count(*) FROM hog_delete_file WHERE catalog_id = :c", catalogId))
            .isEqualTo(0)
        assertThat(fileRows(catalogId))
            .describedAs("and the live data files those vectors rode are untouched")
            .isEqualTo(2_500)
    }

    @Test
    fun `a catalog whose floor has never advanced runs no purge statement at all`() {
        // THE GATE, pinned by its only observable: a statement that is
        // never executed cannot fail. The poisoned Jdbi throws for the
        // data-file page's text, so a sweep that reached phase B's walk
        // would report `purge_failures = 1` — and a gated one reports a
        // clean idle sweep.
        //
        // Asserting "no rows were purged" would NOT pin this: with the
        // floor at 0 the page's own predicate matches nothing anyway, so
        // the gate and its absence look identical on every counter. What
        // the gate actually saves is the WORK — in particular the
        // superseded-vector arm's unindexed scan, every interval, on
        // every retention-disabled catalog in the fleet.
        val catalogId = seed("purge-nostatement", endedBelowFloor = 10, live = 10, retention = null)
        val poisoned = Database.jdbi(db.dataSource)
        poisoned.setSqlLogger(
            object : SqlLogger {
                override fun logBeforeExecution(context: StatementContext) {
                    val sql = context.renderedSql ?: return
                    if (sql.contains("WITH page AS") || sql.contains("SELECT EXISTS")) {
                        throw IllegalStateException("injected: phase B must not have run a statement")
                    }
                }
            },
        )
        val result = ExpiryService(poisoned).runOnce("purge-nostatement", batchSize = 100)
        assertThat(result.purgeFailures)
            .describedAs("phase B must not have touched the database on a catalog at floor 0")
            .isEqualTo(0)
        assertThat(result.purgePages).isEqualTo(0)
        assertThat(result.purgeTruncated).isFalse()
        assertThat(fileRows(catalogId)).isEqualTo(20)
    }

    @Test
    fun `a catalog whose floor has never advanced is skipped entirely`() {
        // The gate that keeps phase B off every retention-disabled
        // catalog, and the proof is arithmetic rather than policy: the
        // schema's CHECK (end_snapshot IS NULL OR end_snapshot >
        // begin_snapshot) with begin_snapshot >= 0 makes every ended
        // row's end_snapshot >= 1, so nothing can satisfy
        // `end_snapshot <= 0`. Before the gate, each such catalog paid
        // the superseded arm's unindexed scan every interval for a
        // population that cannot exist.
        //
        // Asserted through the probe's observable effect: with retention
        // NULL the floor stays 0, and a sweep must report no pages at
        // all — not even the probe's.
        val catalogId = seed("purge-nofloor", endedBelowFloor = 10, live = 10, retention = null)
        seedVectorPerEndedFile(catalogId, endSnapshot = 1)
        val result = ExpiryService(jdbi).runOnce("purge-nofloor", batchSize = 100)
        assertThat(result.newEarliestSnapshotId).isEqualTo(0)
        assertThat(result.purgePages).isEqualTo(0)
        assertThat(result.dataFilesPurged).isEqualTo(0)
        assertThat(result.deleteFilesQueued).isEqualTo(0)
        assertThat(result.purgeTruncated).isFalse()
        assertThat(fileRows(catalogId)).isEqualTo(20)

        // And a catalog that HAS expired keeps draining after its
        // retention is switched off — the gate is on the FLOOR, not on
        // the setting, so a backlog cannot be stranded by a PATCH.
        val expiring = seed("purge-floorthen", endedBelowFloor = 10, live = 10)
        ExpiryService(jdbi, purgeBudgetMs = 0).runOnce("purge-floorthen", batchSize = 100)
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_catalog SET snapshot_retention_seconds = NULL WHERE catalog_id = ?",
                expiring,
            )
        }
        val after = ExpiryService(jdbi).runOnce("purge-floorthen", batchSize = 100)
        assertThat(after.dataFilesPurged)
            .describedAs("retention off does not strand a backlog below a floor that already moved")
            .isEqualTo(10)
    }

    @Test
    fun `a data file the page skips keeps its delete vectors`() {
        // THE ASYMMETRY D1 FOUND, and it is worse than the leak the
        // re-check was added to prevent. The data-file DELETE repeats the
        // eligibility predicate, so a row that stops being eligible
        // between the page's selection and its delete is SKIPPED and
        // survives. If the vector delete keys off the PAGE rather than
        // off what was actually deleted, that surviving file loses its
        // deletion vectors AND their puffin objects are queued for S3
        // removal — so rows a vector masked come back, and the object
        // backing them is deleted.
        //
        // Forced with the same `BEFORE DELETE … RETURN NULL` trick the
        // termination test uses: from the statement's point of view a
        // suppressed delete is indistinguishable from a row the
        // re-check's EvalPlanQual follow-up found ineligible.
        val catalogId = seed("purge-skip-keeps-dv", endedBelowFloor = 20, live = 5)
        seedVectorPerEndedFile(catalogId, endSnapshot = null)
        val protectedId =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT min(data_file_id) FROM hog_data_file WHERE catalog_id = :c " +
                        "AND end_snapshot IS NOT NULL",
                ).bind("c", catalogId).mapTo(Long::class.java).one()
            }
        jdbi.useHandleUnchecked { h ->
            h.execute(
                """
                CREATE OR REPLACE FUNCTION keep_one_fn() RETURNS trigger AS ${'$'}${'$'}
                BEGIN
                    IF OLD.data_file_id = $protectedId THEN
                        RETURN NULL;
                    END IF;
                    RETURN OLD;
                END ${'$'}${'$'} LANGUAGE plpgsql
                """,
            )
            h.execute(
                "CREATE TRIGGER keep_one_fn BEFORE DELETE ON hog_data_file " +
                    "FOR EACH ROW EXECUTE FUNCTION keep_one_fn()",
            )
        }
        val result =
            try {
                ExpiryService(jdbi).runOnce("purge-skip-keeps-dv", batchSize = 100)
            } finally {
                jdbi.useHandleUnchecked { h ->
                    h.execute("DROP TRIGGER IF EXISTS keep_one_fn ON hog_data_file")
                }
            }
        assertThat(result.dataFilesPurged).isEqualTo(19)
        assertThat(result.deleteFilesQueued)
            .describedAs("19 vectors, not 20: the skipped file's vector is not one of them")
            .isEqualTo(19)
        // THE ASSERTION THAT REDS ON THE `page`-KEYED FORM.
        val survivingVector =
            count(
                "SELECT count(*) FROM hog_delete_file WHERE catalog_id = :c AND data_file_id = " +
                    protectedId,
                catalogId,
            )
        assertThat(survivingVector)
            .describedAs("the surviving data file must still have its deletion vector")
            .isEqualTo(1)
        assertThat(queuedPaths(catalogId, "delete"))
            .describedAs("and that vector's puffin must NOT be queued for deletion")
            .doesNotContain("s3://b/$catalogId/dv$protectedId")
            .hasSize(19)
        // Every other file went, with its vector, both queued.
        assertThat(count("SELECT count(*) FROM hog_delete_file WHERE catalog_id = :c", catalogId))
            .isEqualTo(1)
        assertThat(fileRows(catalogId)).isEqualTo(6)
    }

    @Test
    fun `the walk continues on a page that purged fewer rows than it examined`() {
        // EXAMINED, NOT PURGED, is what ends the walk, and the
        // difference is a correctness one rather than a throughput one.
        // A page can purge fewer rows than it examined whenever somebody
        // else removed one of them first — another replica's sweep, a
        // manual trigger racing the loop, `RetirementService` under only
        // the commit lock — and a walk that read that as "the eligible
        // set is exhausted" would report `drained` with rows still below
        // the floor. `/verify`'s `expiry_floor` arm then asserts against
        // exactly that state and reports a violation that is really a
        // bookkeeping error. It is also the shape of the unsound gate
        // this design replaced.
        //
        // Forced deterministically with a `BEFORE DELETE` trigger that
        // RETURNS NULL for a few rows, which cancels their delete
        // silently — the same observable as a concurrent deleter from the
        // statement's point of view, without a second thread.
        val catalogId = seed("purge-shortpage", endedBelowFloor = 2_500, live = 100)
        jdbi.useHandleUnchecked { h ->
            h.execute(
                """
                CREATE OR REPLACE FUNCTION skip_some_fn() RETURNS trigger AS ${'$'}${'$'}
                BEGIN
                    IF OLD.data_file_id % 1000 = 7 THEN
                        RETURN NULL;
                    END IF;
                    RETURN OLD;
                END ${'$'}${'$'} LANGUAGE plpgsql
                """,
            )
            h.execute(
                "CREATE TRIGGER skip_some_fn BEFORE DELETE ON hog_data_file " +
                    "FOR EACH ROW EXECUTE FUNCTION skip_some_fn()",
            )
        }
        val result =
            try {
                ExpiryService(jdbi).runOnce("purge-shortpage", batchSize = 100)
            } finally {
                jdbi.useHandleUnchecked { h ->
                    h.execute("DROP TRIGGER IF EXISTS skip_some_fn ON hog_data_file")
                }
            }
        // Three rows are unkillable (ids 7, 1007, 2007 of 2,500), so the
        // walk purges everything else and only stops when a page can no
        // longer be FILLED. A walk keyed on the purged count would have
        // stopped after its first page.
        assertThat(result.dataFilesPurged)
            .describedAs("every eligible row but the three the trigger protects")
            .isEqualTo(2_497)
        assertThat(result.purgePages)
            .describedAs("two full pages plus the short one that ends the walk")
            .isEqualTo(3)
        assertThat(endedBelowFloor(catalogId)).isEqualTo(3)
        assertThat(result.purgeTruncated)
            .describedAs("a short page is 'nothing eligible', not a truncation")
            .isFalse()
    }

    @Test
    fun `a phase-A failure still runs the purge, fails the run, and records what the purge did`() {
        // THE GAP A REVIEW FOUND ON THE LIVE STACK: phase A's bound
        // exists to turn a catch-up sweep into a fast retryable failure,
        // so it fires exactly when the backlog is largest — and the
        // purge used to be skipped on that path, because the exception
        // propagated before it. Three sweeps in a row then reported
        // `failed` with a null result, no counters, and `/verify`'s file
        // arm dark, while 5,000 rows sat below a floor that had already
        // moved.
        //
        // The failure here is a trigger rather than a statement bound, so
        // it is deterministic and is NOT retried by the halving loop
        // (which only retries a cancelled statement).
        val catalogId = seed("purge-advancefail", endedBelowFloor = 30, live = 10)
        // Advance the floor first, so there IS a backlog below a
        // committed floor when the next sweep's phase A dies.
        ExpiryService(jdbi, purgeBudgetMs = 0).runOnce("purge-advancefail", batchSize = 100)
        assertThat(endedBelowFloor(catalogId)).isEqualTo(30)
        jdbi.useHandleUnchecked { h ->
            h.execute(
                """
                CREATE OR REPLACE FUNCTION advance_fail_fn() RETURNS trigger AS ${'$'}${'$'}
                BEGIN
                    RAISE EXCEPTION 'injected: the floor advance cannot commit';
                END ${'$'}${'$'} LANGUAGE plpgsql
                """,
            )
            // On the floor UPDATE, which phase A always runs when it
            // advances — and it advances here because the previous sweep
            // was stopped only in phase B.
            h.execute(
                "CREATE TRIGGER advance_fail_fn BEFORE UPDATE ON hog_catalog " +
                    "FOR EACH ROW WHEN (NEW.catalog_id = $catalogId) EXECUTE FUNCTION advance_fail_fn()",
            )
            // The floor has to be able to move, or phase A never reaches
            // the UPDATE: rewind it and age the snapshots again.
            h.execute("DROP TRIGGER IF EXISTS advance_fail_fn ON hog_catalog")
            h.execute(
                "INSERT INTO hog_snapshot (catalog_id, snapshot_id, snapshot_time, schema_version) " +
                    "VALUES (?, 6, now() - make_interval(secs => 3600), 0)",
                catalogId,
            )
            h.execute("UPDATE hog_catalog SET last_snapshot_id = 6 WHERE catalog_id = ?", catalogId)
            h.execute(
                "CREATE TRIGGER advance_fail_fn BEFORE UPDATE ON hog_catalog " +
                    "FOR EACH ROW WHEN (NEW.catalog_id = $catalogId) EXECUTE FUNCTION advance_fail_fn()",
            )
        }
        val thrown =
            try {
                runCatching { ExpiryService(jdbi).runOnce("purge-advancefail", batchSize = 100) }
            } finally {
                jdbi.useHandleUnchecked { h ->
                    h.execute("DROP TRIGGER IF EXISTS advance_fail_fn ON hog_catalog")
                }
            }
        assertThat(thrown.isFailure)
            .describedAs("the run must still FAIL: the floor did not move")
            .isTrue()
        // And the purge ran anyway.
        assertThat(endedBelowFloor(catalogId))
            .describedAs("phase B drained the backlog even though phase A could not commit")
            .isEqualTo(0)
        assertThat(queuedPaths(catalogId, "data")).hasSize(30)
        // The ledger row is `failed` AND carries what the purge did, through
        // MaintenanceRunStore's PartialResult hook — a failed row with a null
        // result would be the lie that hook exists to prevent.
        val row =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT status, result::text AS result FROM hog_maintenance_run " +
                        "WHERE catalog_id = :c AND task = 'expiry' ORDER BY run_id DESC LIMIT 1",
                )
                    .bind("c", catalogId)
                    .map { rs, _ -> rs.getString("status") to rs.getString("result") }
                    .one()
            }
        assertThat(row.first).isEqualTo("failed")
        assertThat(row.second)
            .describedAs("the failed row must say what the purge managed:%n%s", row.second)
            .isNotNull()
            .contains("\"data_files_purged\": 30")
            .describedAs("and must report the floor where it actually stands, not 0")
            .contains("\"new_earliest_snapshot_id\": 5")
            .describedAs("and must not claim the advance happened")
            .contains("\"snapshots_expired\": 0")
    }

    // ---- the bounds, in force rather than merely computed -------------------

    /**
     * A `BEFORE DELETE` trigger that fails unless `statement_timeout` is
     * exactly [expected] when it fires.
     *
     * The bound is SET on a line, and a line is not a behaviour: the
     * review deleted phase A's `set_config` call and the whole 184-test
     * selection stayed green, which is the same gap this file's KDoc
     * complains about for derivations stated where no test can reach
     * them. `current_setting` inside a trigger reads the value in force
     * for the statement that fired it, from inside the transaction under
     * test, so this cannot pass on a build that forgot to apply it.
     */
    private fun boundGuard(
        table: String,
        expected: String,
    ) = jdbi.useHandleUnchecked { h ->
        h.execute(
            """
            CREATE OR REPLACE FUNCTION bound_guard_$table() RETURNS trigger AS ${'$'}${'$'}
            BEGIN
                IF current_setting('statement_timeout') <> '$expected' THEN
                    RAISE EXCEPTION 'statement_timeout was %, expected $expected',
                        current_setting('statement_timeout');
                END IF;
                RETURN OLD;
            END ${'$'}${'$'} LANGUAGE plpgsql
            """,
        )
        h.execute(
            "CREATE TRIGGER bound_guard_$table BEFORE DELETE ON $table " +
                "FOR EACH ROW EXECUTE FUNCTION bound_guard_$table()",
        )
    }

    private fun dropBoundGuard(table: String) =
        jdbi.useHandleUnchecked { h -> h.execute("DROP TRIGGER IF EXISTS bound_guard_$table ON $table") }

    @Test
    fun `phase A runs its statements under the derived bound, not the session's`() {
        val catalogId = seed("bound-advance", endedBelowFloor = 2, live = 2)
        val svc = ExpiryService(jdbi)
        boundGuard("hog_snapshot", "${svc.advanceBoundMs}ms")
        try {
            val result = svc.runOnce("bound-advance", batchSize = 100)
            assertThat(result.snapshotsExpired)
                .describedAs("the snapshot delete must have run under the derived bound")
                .isEqualTo(5)
        } finally {
            dropBoundGuard("hog_snapshot")
        }
    }

    @Test
    fun `every phase-B page runs under the purge statement bound`() {
        val catalogId = seed("bound-page", endedBelowFloor = 4, live = 4)
        boundGuard("hog_data_file", ExpiryService.PURGE_STATEMENT_TIMEOUT)
        try {
            val result = ExpiryService(jdbi).runOnce("bound-page", batchSize = 100)
            assertThat(result.dataFilesPurged).isEqualTo(4)
            assertThat(result.purgeFailures)
                .describedAs("a page that ran without the bound would have raised")
                .isEqualTo(0)
        } finally {
            dropBoundGuard("hog_data_file")
        }
        assertThat(fileRows(catalogId)).isEqualTo(4)
    }

    @Test
    fun `a page slower than its bound is cancelled by Postgres and counted`() {
        // The other half: that the value is SET is one thing, that
        // Postgres ENFORCES it is another, and only this half proves the
        // 2026-09-28 failure mode (a page that cannot finish) is bounded
        // at five seconds rather than at the session's sixty. A page of
        // one cannot be halved, so the walk gives up after one failure
        // and the sweep still reports its floor.
        val catalogId = seed("bound-slow", endedBelowFloor = 2, live = 2)
        jdbi.useHandleUnchecked { h ->
            h.execute(
                """
                CREATE OR REPLACE FUNCTION slow_page_fn() RETURNS trigger AS ${'$'}${'$'}
                BEGIN
                    PERFORM pg_sleep(30);
                    RETURN OLD;
                END ${'$'}${'$'} LANGUAGE plpgsql
                """,
            )
            h.execute(
                "CREATE TRIGGER slow_page_fn BEFORE DELETE ON hog_data_file " +
                    "FOR EACH ROW EXECUTE FUNCTION slow_page_fn()",
            )
        }
        val started = System.nanoTime()
        val result =
            try {
                ExpiryService(jdbi, purgePage = 1).runOnce("bound-slow", batchSize = 100)
            } finally {
                jdbi.useHandleUnchecked { h ->
                    h.execute("DROP TRIGGER IF EXISTS slow_page_fn ON hog_data_file")
                }
            }
        val elapsedMs = (System.nanoTime() - started) / 1_000_000
        assertThat(result.purgeFailures).isEqualTo(1)
        assertThat(result.dataFilesPurged).isEqualTo(0)
        assertThat(result.purgeTruncated).isTrue()
        assertThat(result.purgeRemaining).isEqualTo(2)
        assertThat(result.snapshotsExpired)
            .describedAs("the floor still advanced: phase B's failure is not phase A's")
            .isEqualTo(5)
        assertThat(elapsedMs)
            .describedAs("bounded at five seconds, not at the session's sixty (took %d ms)", elapsedMs)
            .isLessThan(20_000)
        assertThat(fileRows(catalogId)).isEqualTo(4)
    }

    @Test
    fun `the halving ladder converges across sweeps when the failure is a timeout`() {
        // THE PRODUCTION FAILURE MODE, measured rather than simulated.
        // A rung that dies on PURGE_STATEMENT_TIMEOUT costs five seconds;
        // the default budget is ten, so one sweep affords about two
        // rungs. With the page reset to its configured value every sweep,
        // a page that needs 1,000 -> 125 would burn every sweep forever
        // on 1,000 and 500 and never reach a size that commits — S9's
        // stall in a subtler costume, and invisible because the deadline
        // branch used to report `purge_failures = 0`.
        //
        // A `pg_sleep` PER ROW makes the cost proportional to the page,
        // so the ladder has a rung that commits: at 0.4 s a row a page of
        // 16 needs 6.4 s and dies on the 5 s bound, a page of 8 needs
        // 3.2 s and commits. An instant RAISE cannot measure this — it is
        // why the first round's "nine halvings in one sweep" did not
        // transfer.
        //
        // ONE SERVICE INSTANCE across both sweeps, because the hint it
        // carries is the subject.
        val catalogId = seed("purge-ladder", endedBelowFloor = 40, live = 5)
        jdbi.useHandleUnchecked { h ->
            h.execute(
                """
                CREATE OR REPLACE FUNCTION slow_row_fn() RETURNS trigger AS ${'$'}${'$'}
                BEGIN
                    PERFORM pg_sleep(0.4);
                    RETURN OLD;
                END ${'$'}${'$'} LANGUAGE plpgsql
                """,
            )
            h.execute(
                "CREATE TRIGGER slow_row_fn BEFORE DELETE ON hog_data_file " +
                    "FOR EACH ROW EXECUTE FUNCTION slow_row_fn()",
            )
        }
        val svc = ExpiryService(jdbi, purgePage = 16, purgeBudgetMs = 9_000)
        try {
            val first = System.nanoTime()
            val sweep1 = svc.runOnce("purge-ladder", batchSize = 100)
            val firstMs = (System.nanoTime() - first) / 1_000_000
            val second = System.nanoTime()
            val sweep2 = svc.runOnce("purge-ladder", batchSize = 100)
            val secondMs = (System.nanoTime() - second) / 1_000_000
            println(
                "[expiry purge] ladder with pg_sleep(0.4)/row, page 16 -> 8, budget 9,000 ms: " +
                    "sweep 1 purged ${sweep1.dataFilesPurged} in ${firstMs}ms with " +
                    "${sweep1.purgeFailures} failed pages; sweep 2 purged ${sweep2.dataFilesPurged} " +
                    "in ${secondMs}ms with ${sweep2.purgeFailures} failed pages " +
                    "(a failing rung costs the ${ExpiryService.PURGE_STATEMENT_TIMEOUT} bound)",
            )

            // Sweep one pays for the discovery: at least one rung dies on
            // the bound, and the FAILURE IS REPORTED — an all-timeouts
            // sweep that reported zero failures was the other half of
            // this defect.
            assertThat(sweep1.purgeFailures)
                .describedAs("the page of 16 cannot finish inside the statement bound")
                .isGreaterThanOrEqualTo(1)
            assertThat(sweep1.purgeTruncated).isTrue()

            // Sweep two starts from the rung that committed, so it spends
            // its whole budget purging instead of rediscovering. THE
            // ASSERTION THAT REDS WITHOUT THE HINT: a reset page would
            // fail on 16 again and purge no more than sweep one did.
            assertThat(sweep2.purgeFailures)
                .describedAs("the second sweep must not rediscover the bound")
                .isEqualTo(0)
            assertThat(sweep2.dataFilesPurged)
                .describedAs("and must therefore out-purge the sweep that paid for the discovery")
                .isGreaterThan(sweep1.dataFilesPurged)
        } finally {
            jdbi.useHandleUnchecked { h ->
                h.execute("DROP TRIGGER IF EXISTS slow_row_fn ON hog_data_file")
            }
        }
    }

    @Test
    fun `a floor that cannot be read is a counted failure, not an idle sweep`() {
        // The one path that would otherwise report a database problem as
        // a healthy no-op. Injected with a SqlLogger on a second Jdbi
        // over the SAME pool, because the floor read is a primary-key
        // SELECT and there is no SQL-level way to make one fail: the
        // logger throws for that statement's text and nothing else, so
        // phase A (which reads the catalog through CatalogRepo) still
        // commits.
        val catalogId = seed("purge-floorread", endedBelowFloor = 6, live = 6)
        val poisoned = Database.jdbi(db.dataSource)
        poisoned.setSqlLogger(
            object : SqlLogger {
                override fun logBeforeExecution(context: StatementContext) {
                    // The floor read's whole text, not a fragment:
                    // `CatalogRepo.findByName` also selects that column
                    // from that table, and matching loosely poisoned
                    // phase A instead of phase B.
                    if (context.renderedSql?.contains(
                            "SELECT earliest_snapshot_id FROM hog_catalog WHERE catalog_id = ?",
                        ) == true
                    ) {
                        throw IllegalStateException("injected: the floor read is unavailable")
                    }
                }
            },
        )
        val result = ExpiryService(poisoned).runOnce("purge-floorread", batchSize = 100)
        assertThat(result.snapshotsExpired)
            .describedAs("phase A is unaffected: it resolves the catalog by name, not by floor")
            .isEqualTo(5)
        assertThat(result.purgeFailures).isEqualTo(1)
        assertThat(result.purgeTruncated).isTrue()
        assertThat(result.dataFilesPurged).isEqualTo(0)
        assertThat(endedBelowFloor(catalogId))
            .describedAs("and the rows are still there for the next sweep")
            .isEqualTo(6)
    }

    @Test
    fun `the purge's metrics are emitted, including on the phase-A failure path`() {
        // Mutation (k): deleting both metric calls reddened nothing.
        // They are the only alertable surface for a purge that is behind
        // — the ledger row is not scraped — so they are asserted here
        // rather than trusted.
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        Metrics.bind(registry)
        try {
            val catalogId = seed("purge-metrics", endedBelowFloor = 6, live = 6)
            ExpiryService(jdbi).runOnce("purge-metrics", batchSize = 100)
            assertThat(counter(registry, "hoglake_expiry_purge_rows_total", "purge-metrics"))
                .isEqualTo(6.0)
            assertThat(gauge(registry, "hoglake_expiry_purge_remaining", "purge-metrics"))
                .describedAs("published as 0 by a drained sweep, so an alert can key on > 0")
                .isEqualTo(0.0)

            // A truncated sweep: the truncation counter and the gauge.
            val behind = seed("purge-metrics-behind", endedBelowFloor = 40, live = 4)
            ExpiryService(jdbi, purgeBudgetMs = 0).runOnce("purge-metrics-behind", batchSize = 100)
            assertThat(counter(registry, "hoglake_expiry_purge_truncated_total", "purge-metrics-behind"))
                .isEqualTo(1.0)
            assertThat(gauge(registry, "hoglake_expiry_purge_remaining", "purge-metrics-behind"))
                .isEqualTo(40.0)
            assertThat(gauge(registry, "hoglake_expiry_purge_remaining", "purge-metrics"))
                .describedAs("one catalog's publish must not delete another's row")
                .isEqualTo(0.0)
            assertThat(fileRows(behind)).isEqualTo(44)

            // A failed page: the failure counter and the halvings. On
            // the BEHIND catalog, which still has rows — the drained one
            // has nothing left for a trip wire to trip on.
            tripAfter(0)
            try {
                ExpiryService(jdbi, purgePage = 4).runOnce("purge-metrics-behind", batchSize = 100)
            } finally {
                dropTrip()
            }
            assertThat(counter(registry, "hoglake_expiry_purge_failures_total", "purge-metrics-behind"))
                .describedAs("a page of 4 halves to 2 and to 1 before giving up: three failed pages")
                .isEqualTo(3.0)
            assertThat(
                registry.find("hoglake_expiry_halvings_total")
                    .tags("catalog", "purge-metrics-behind", "phase", "purge").counter()?.count(),
            )
                .describedAs("the page halved on its way down to one")
                .isNotNull()
                .satisfies({ assertThat(it).isGreaterThan(0.0) })
            assertThat(catalogId).isNotZero()
            // And the halvings reach the LEDGER, not only Prometheus:
            // for a sweep whose every page times out they are the only
            // count that moves.
            val laddered =
                ExpiryService(jdbi, purgePage = 4).let { svc ->
                    tripAfter(0)
                    try {
                        svc.runOnce("purge-metrics-behind", batchSize = 100)
                    } finally {
                        dropTrip()
                    }
                }
            assertThat(laddered.purgeHalvings)
                .describedAs("4 -> 2 -> 1 is two halvings, on the wire")
                .isEqualTo(2)
            assertThat(laddered.advanceHalvings).isZero()

            // A DROPPED catalog's gauge row is forgotten by the fleet
            // sweep, which is the only caller that knows the whole set.
            // Otherwise its last value is republished forever and the
            // alert it raises cannot be closed.
            jdbi.useHandleUnchecked { h ->
                h.execute("DELETE FROM hog_catalog WHERE catalog_id = ?", behind)
            }
            ExpiryService(jdbi).runOnceAllCatalogs(100)
            assertThat(gauge(registry, "hoglake_expiry_purge_remaining", "purge-metrics-behind"))
                .describedAs("a dropped catalog must not keep a published gauge row")
                .isEqualTo(-1.0)
            assertThat(gauge(registry, "hoglake_expiry_purge_remaining", "purge-metrics"))
                .describedAs("and the surviving catalogs keep theirs")
                .isEqualTo(0.0)
        } finally {
            Metrics.clear()
            ExpiryGauges.clear()
        }
    }

    private fun counter(
        registry: PrometheusMeterRegistry,
        name: String,
        catalog: String,
    ): Double = registry.find(name).tags("catalog", catalog).counter()?.count() ?: 0.0

    private fun gauge(
        registry: PrometheusMeterRegistry,
        name: String,
        catalog: String,
    ): Double = registry.find(name).tags("catalog", catalog).gauge()?.value() ?: -1.0

    // ---- concurrency -------------------------------------------------------

    @Test
    fun `a commit tail running beside the purge only ends rows above the floor`() {
        // THE IMMUTABILITY ARGUMENT, EXERCISED. Phase B selects rows
        // with `end_snapshot <= floor`; a commit tail (compaction's
        // included) writes the NEW HEAD into `end_snapshot`, which is
        // strictly above the floor by definition of head. So the two
        // cannot contend on a row, and no row the commit ends can be
        // taken by the purge that is running beside it.
        //
        // Real threads, no held transaction: the commits take the
        // per-catalog commit lock for their own short transactions, and
        // the purge takes nothing — which is the property under test.
        val catalogId = seed("purge-concurrent", endedBelowFloor = 2_000, live = 2_000, head = 5)
        val svc = ExpiryService(jdbi, purgePage = 50)
        val started = CountDownLatch(1)
        val failure = AtomicReference<Throwable?>(null)
        var endedByCommits = 0
        val committer =
            Thread {
                try {
                    started.await()
                    repeat(20) { i ->
                        jdbi.inTransactionUnchecked { h ->
                            Locks.acquireCatalogCommitLock(h, catalogId)
                            // FAR above any floor this sweep can reach.
                            // `batchSize` caps the advance at
                            // earliest + 100, so a row ended at 5,000 is
                            // above the floor whichever order the two
                            // threads interleave in — which is what
                            // makes the assertion about the PURGE rather
                            // than about the race.
                            val head = 5_000L + i
                            h.createUpdate(
                                "UPDATE hog_catalog SET last_snapshot_id = :head WHERE catalog_id = :c",
                            ).bind("head", head).bind("c", catalogId).execute()
                            h.execute(
                                "INSERT INTO hog_snapshot (catalog_id, snapshot_id, snapshot_time, " +
                                    "schema_version) VALUES (?, ?, now(), 0)",
                                catalogId,
                                head,
                            )
                            // Exactly what a commit tail does to the
                            // inputs it retires: end them at the snapshot
                            // it is creating.
                            val n =
                                h.createUpdate(
                                    """
                                    UPDATE hog_data_file SET end_snapshot = :head
                                    WHERE catalog_id = :c AND data_file_id IN (
                                        SELECT data_file_id FROM hog_data_file
                                        WHERE catalog_id = :c AND end_snapshot IS NULL
                                        ORDER BY data_file_id LIMIT 10
                                    )
                                    """,
                                ).bind("head", head).bind("c", catalogId).execute()
                            synchronized(this) { endedByCommits += n }
                        }
                    }
                } catch (e: Throwable) {
                    failure.set(e)
                }
            }
        committer.start()
        started.countDown()
        val result = svc.runOnce("purge-concurrent", batchSize = 100)
        committer.join(60_000)
        assertThat(committer.isAlive)
            .describedAs("the concurrent committer must have finished")
            .isFalse()
        assertThat(failure.get()).isNull()

        assertThat(result.dataFilesPurged)
            .describedAs("every row below the floor, and nothing else")
            .isEqualTo(2_000)
        assertThat(result.purgeTruncated).isFalse()
        val survivors =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT count(*) FROM hog_data_file f
                    JOIN hog_catalog c ON c.catalog_id = f.catalog_id
                    WHERE f.catalog_id = :c AND f.end_snapshot IS NOT NULL
                      AND f.end_snapshot > c.earliest_snapshot_id
                    """,
                ).bind("c", catalogId).mapTo(Long::class.java).one()
            }
        assertThat(survivors)
            .describedAs("the concurrent commits' rows are all above the floor and all still here")
            .isEqualTo(endedByCommits.toLong())
        assertThat(fileRows(catalogId)).isEqualTo(2_000)
        assertThat(endedBelowFloor(catalogId)).isEqualTo(0)
    }

    // ---- the budget, at production scale -----------------------------------

    @Test
    fun `the budget truncates with a saturating remaining count and the next sweep continues`() {
        // 200,000 eligible rows: the backlog a sixteen-hour outage, or
        // millpond's 5x promotion, actually produces. The properties are
        // that one sweep cannot be made to spend an unbounded amount of
        // time on it, that it SAYS it did not finish, and that the next
        // one picks up where it stopped without a cursor.
        val catalogId =
            seed("purge-budget", endedBelowFloor = 200_000, live = 20_000, head = 5)
        val stopped =
            ExpiryService(jdbi, purgePage = 500, purgeBudgetMs = 1)
                .runOnce("purge-budget", batchSize = 100)
        assertThat(stopped.purgeTruncated).isTrue()
        assertThat(stopped.purgeRemaining)
            .describedAs("saturating: at the cap it means 'behind by more than a sweep can catch up'")
            .isEqualTo(ExpiryService.PURGE_REMAINING_CAP.toLong())
        assertThat(stopped.dataFilesPurged)
            .describedAs("a 1 ms budget buys at most the one page it is already inside")
            .isLessThanOrEqualTo(500)
        val afterFirst = endedBelowFloor(catalogId)

        val second =
            ExpiryService(jdbi, purgePage = 500, purgeBudgetMs = 2_000)
                .runOnce("purge-budget", batchSize = 100)
        assertThat(second.dataFilesPurged)
            .describedAs("the second sweep continues from the predicate, not from a cursor")
            .isGreaterThan(0)
        assertThat(endedBelowFloor(catalogId)).isEqualTo(afterFirst - second.dataFilesPurged)
        assertThat(second.purgePages)
            .describedAs("and it spends its budget in pages of the configured size")
            .isGreaterThanOrEqualTo(second.dataFilesPurged / 500)
    }

    @Test
    fun `a page's cost is the page, not the backlog's, on 200k eligible rows`() {
        // AGENT.md's plan rule: prove the plan on a production-shaped
        // fixture, and assert ROWS EXAMINED AND BUFFERS rather than only
        // the index name. The statement is EXPLAINed as PRODUCTION runs
        // it — `ExpiryService`'s own constant, bound to the production
        // page — against the BACKLOG shape: 200,000 rows eligible below
        // the floor, interleaved with 20,000 live ones. That is what a
        // sixteen-hour outage leaves, and what millpond's 5x promotion
        // will produce routinely.
        //
        // THE PATH IS NOT ASSERTED HERE, AND THAT IS DELIBERATE. At a
        // 91% ended fraction the planner prefers a sequential scan and
        // is right to: V19's header measured the index's benefit as
        // inversely proportional to the ended fraction, approaching a
        // wash at a high one, and with the page's `LIMIT` the scan stops
        // after 1,000 eligible rows — which at that density is a couple
        // of hundred heap pages. Pinning the index NAME here would pin
        // the wrong thing and would red the day the planner got it
        // right. The index-at-production's-fraction half is
        // `V19DataFileEndedIndexMigrationIntegrationTest`, which
        // EXPLAINs this same constant at 1% ended and asserts
        // `hog_data_file_ended` with a per-page buffer budget.
        //
        // What holds at EVERY fraction is the property paging bought:
        // the rows examined and the buffers track the PAGE, not the
        // eligible set. That is the assertion an `ORDER BY end_snapshot`
        // in the statement would fail — the planner answers an ordered
        // page with a scan over the whole eligible set feeding a sort
        // (2,000 heap blocks for a page of 1,000, measured in V19's
        // file), so the rows DELETED stay bounded and the rows EXAMINED
        // do not.
        val backlog = seed("purge-plan-backlog", endedBelowFloor = 200_000, live = 20_000)
        // A LARGE `hog_delete_file`, because the page's vector arm is the
        // other statement in it and a fixture with no vectors asserts
        // nothing about that arm's access path. One vector per fifth
        // file, which is 44,000 rows — enough that a sequential scan of
        // the relation would be visible in the buffers.
        seedVectorEveryNthFile(backlog, 5)
        jdbi.useHandleUnchecked { h ->
            h.execute("VACUUM (ANALYZE) hog_data_file")
            h.execute("VACUUM (ANALYZE) hog_delete_file")
        }
        ExpiryService(jdbi, purgeBudgetMs = 0).runOnce("purge-plan-backlog", batchSize = 100)
        assertThat(endedBelowFloor(backlog)).isEqualTo(200_000)

        val plan = explainOnePage(backlog)
        assertThat(plan)
            .describedAs("no plan may sort the eligible set to produce a page:%n%s", plan)
            .doesNotContain("Sort Key")
        // THE OUTER DELETE'S ELIGIBILITY RE-CHECK, asserted in the PLAN
        // rather than in behaviour, because the state it defends against
        // needs a concurrent writer this codebase does not contain — that
        // is the whole point of it. Without the repeated predicate the
        // `ctid` array is the Tid Scan's only qual, and under READ
        // COMMITTED `TidRecheck` returns true unconditionally, so a row
        // that an EvalPlanQual follow-up found NO LONGER ELIGIBLE would
        // still be deleted and its path queued for physical removal.
        // Filter quals ARE re-applied after the recheck, so the presence
        // of this Filter on the Tid Scan is the defence.
        val tidScan =
            plan.lines().let { lines ->
                val i = lines.indexOfFirst { it.contains("Tid Scan on hog_data_file") }
                assertThat(i).describedAs("the page must delete by ctid:%n%s", plan).isNotNegative()
                lines.drop(i).take(4).joinToString("\n")
            }
        assertThat(tidScan)
            .describedAs("the outer delete must re-check eligibility, not trust the ctid array:%n%s", tidScan)
            .contains("Filter:")
            .contains("end_snapshot")
        val examined = rowsExamined(plan)
        val buffers = pageBuffers(plan)
        println(
            "[expiry purge] one page of ${ExpiryService.PURGE_PAGE} over 200,000 eligible rows " +
                "examined $examined rows in $buffers buffers\n$plan",
        )
        // THE BUDGET IS A CONSTANT MULTIPLE OF THE PAGE, and the
        // measured constant is recorded rather than rounded away:
        // ~3,700 rows and ~185 heap buffers for a page of 1,000 on this
        // fixture. The multiple is not 1 because a sequential scan stops
        // at a page BOUNDARY rather than at a row, and because one in
        // ten rows here is live and walked past. 10x leaves room for
        // both and none for a plan that reads the eligible set: that
        // would be 200,000 rows and the relation's whole heap.
        val heapPages =
            jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT relpages FROM pg_class WHERE relname = 'hog_data_file'")
                    .mapTo(Long::class.java).one()
            }
        assertThat(examined)
            .describedAs("rows examined is the page, not the eligible set:%n%s", plan)
            .isLessThanOrEqualTo(10 * ExpiryService.PURGE_PAGE)
            .satisfies({ assertThat(it).isLessThan(200_000 / 10) })
        assertThat(buffers.toLong())
            .describedAs("and the buffers with them, against a %d-page relation:%n%s", heapPages, plan)
            .isLessThanOrEqualTo(heapPages / 4)

        // THE VECTOR ARM'S ACCESS PATH, which the KDoc claims is "an
        // index probe per deleted row rather than a scan" and which
        // nothing asserted. `dv.data_file_id IN (SELECT data_file_id FROM
        // doomed)` is a semi-join, and the planner is free to answer it
        // with a hash join over a SEQUENTIAL SCAN of hog_delete_file —
        // exactly the shape this statement's own KDoc rejects for the
        // PK-join form. On a relation with 44,000 vectors that would be
        // hundreds of buffers of pure waste per page, and it would grow
        // with the catalog rather than with the page.
        assertThat(plan)
            .describedAs("the vector arm must probe hog_delete_file by (catalog, data file):%n%s", plan)
            .contains("hog_delete_file_data_lookup")
        assertThat(plan)
            .describedAs("and must never scan the vector relation:%n%s", plan)
            .doesNotContain("Seq Scan on hog_delete_file")
        val vectorBuffers = nodeBuffers(plan, "hog_delete_file_data_lookup")
        println(
            "[expiry purge] the vector arm on a 44,000-row hog_delete_file: $vectorBuffers buffers " +
                "for a page of ${ExpiryService.PURGE_PAGE}",
        )
        assertThat(vectorBuffers)
            .describedAs("one descent per deleted row, not a scan of the relation:%n%s", plan)
            .isLessThanOrEqualTo(6 * ExpiryService.PURGE_PAGE)
    }

    /** Buffers reported by the first node whose line names [marker]. */
    private fun nodeBuffers(
        plan: String,
        marker: String,
    ): Int {
        val lines = plan.lines()
        val i = lines.indexOfFirst { it.contains(marker) }
        assertThat(i).describedAs("no node named %s in:%n%s", marker, plan).isNotNegative()

        fun indent(l: String) = l.length - l.trimStart().length
        val line = lines[i]
        val buffers =
            lines.drop(i + 1).takeWhile { it.isNotBlank() && indent(it) > indent(line) }
                .firstOrNull { it.trim().startsWith("Buffers:") } ?: return 0
        val hit = Regex("""\bhit=(\d+)""").find(buffers)?.groupValues?.get(1)?.toInt() ?: 0
        val read = Regex("""\bread=(\d+)""").find(buffers)?.groupValues?.get(1)?.toInt() ?: 0
        return hit + read
    }

    /**
     * EXPLAIN ANALYZE of ONE production page, rolled back so the rows
     * stay. Serial plans only: a Gather splits its workers' buffer
     * counts, which would make every number depend on the core count.
     */
    private fun explainOnePage(catalogId: Long): String =
        jdbi.withHandleUnchecked { h ->
            h.begin()
            try {
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                h.createQuery(
                    "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) " +
                        ExpiryService.DATA_FILE_EXPIRY_SQL,
                )
                    .bind("catalogId", catalogId)
                    .bind("newEarliest", 5L)
                    .bind("page", ExpiryService.PURGE_PAGE)
                    .mapTo(String::class.java).list().joinToString("\n")
            } finally {
                h.rollback()
            }
        }

    /**
     * Rows the page's INNER scan actually returned — the node under the
     * `Limit`, whichever access path the planner chose for it. Not the
     * `Tid Scan`, which is a bounded array lookup and says nothing about
     * the cost of FINDING the page.
     */
    private fun rowsExamined(plan: String): Int {
        val lines = plan.lines()
        val limit = lines.indexOfFirst { it.contains("->  Limit") }
        assertThat(limit).describedAs("the page must be a Limit:%n%s", plan).isGreaterThanOrEqualTo(0)
        // EVERY SPELLING OF A SCAN, AND NEVER THE TID SCAN. An index
        // scan prints as `Index Scan using hog_data_file_ended on
        // hog_data_file df`, which does NOT contain the literal "Scan on
        // hog_data_file" — so a selector matching only that literal
        // skipped past it to the `Tid Scan`, whose actual rows ARE the
        // page size by construction, and the assertion below would have
        // passed for any plan. This file's sibling (`V19...Test`) had the
        // same bug and grew the same guard.
        val scan =
            lines.drop(limit + 1).first { line ->
                SCAN_NODE.containsMatchIn(line) && !line.contains("Tid Scan")
            }
        assertThat(scan)
            .describedAs("the measured node must be the one that FINDS the page, not the Tid Scan")
            .doesNotContain("Tid Scan")
        val rows =
            Regex("""actual rows=([\d.]+)""").find(scan)?.groupValues?.get(1)?.toDouble()?.toInt()
        val removed =
            lines.dropWhile { it != scan }.drop(1)
                .takeWhile { it.isNotBlank() && it.trimStart() != it }
                .firstOrNull { it.contains("Rows Removed by Filter:") }
                ?.let { Regex("""(\d+)""").find(it.substringAfter("Filter:"))?.value?.toInt() }
                ?: 0
        assertThat(rows).describedAs("no actual rows on the page's scan:%n%s", plan).isNotNull()
        return rows!! + removed
    }

    /** Buffers the page's inner scan touched (hit + read). */
    private fun pageBuffers(plan: String): Int {
        val lines = plan.lines()
        val limit = lines.indexOfFirst { it.contains("->  Limit") }
        val buffers =
            lines.drop(limit).first { it.contains("Buffers:") }
        val hit = Regex("""\bhit=(\d+)""").find(buffers)?.groupValues?.get(1)?.toInt() ?: 0
        val read = Regex("""\bread=(\d+)""").find(buffers)?.groupValues?.get(1)?.toInt() ?: 0
        return hit + read
    }

    @Test
    fun `a truncated purge is not an expiry_floor violation, and a drained one that leaves rows is`() {
        // What /verify now claims, and the two states that separate the
        // designed lag from a real bug. `expiry_floor`'s file-row arm
        // reads the catalog's newest expiry ledger row: a purge that
        // reported `purge_truncated` is draining, and rows below the
        // floor are not a violation; a purge that reported DRAINED and
        // still left rows means its predicate and the floor advance
        // disagree, which no counter would show.
        val catalogId = seed("purge-verify", endedBelowFloor = 10, live = 10)
        val verify = VerifyService(jdbi, retirementIntervalMs = 0)
        ExpiryService(jdbi, purgeBudgetMs = 0).runOnce("purge-verify", batchSize = 100)
        assertThat(endedBelowFloor(catalogId)).isEqualTo(10)
        val lagging = verify.runOnce("purge-verify").checks.single { it.check == "expiry_floor" }
        assertThat(lagging.violations)
            .describedAs("rows below the floor while the purge is behind are not a violation")
            .isEqualTo(0)

        // Now a sweep that drains, with a row hand-planted below the
        // floor afterwards: the invariant the check still owns.
        ExpiryService(jdbi).runOnce("purge-verify", batchSize = 100)
        assertThat(endedBelowFloor(catalogId)).isEqualTo(0)
        assertThat(
            verify.runOnce("purge-verify").checks.single { it.check == "expiry_floor" }.violations,
        ).isEqualTo(0)
        jdbi.useHandleUnchecked { h ->
            h.execute(
                """
                INSERT INTO hog_data_file
                    (catalog_id, data_file_id, table_id, begin_snapshot, end_snapshot, path,
                     record_count, file_size_bytes, row_id_start)
                VALUES (?, 999999, 1, 0, 1, 's3://b/survivor', 1, 1, 0)
                """,
                catalogId,
            )
        }
        val broken = verify.runOnce("purge-verify").checks.single { it.check == "expiry_floor" }
        assertThat(broken.violations)
            .describedAs("a DRAINED purge that left a row below the floor is still a violation")
            .isEqualTo(1)
        assertThat(broken.samples.single()).contains("survived the floor advance")
    }

    @Test
    fun `the purge's own run budget is wall clock, not a row count`() {
        // The budget is checked BETWEEN pages, so the bound it promises
        // is "one page over it" — and that page is bounded by
        // PURGE_STATEMENT_TIMEOUT, which is the half a wall budget
        // cannot do. Asserted as a relation rather than a duration: a
        // generous budget drains a backlog a tight one cannot, on the
        // same fixture.
        val catalogId = seed("purge-wall", endedBelowFloor = 5_000, live = 500)
        ExpiryService(jdbi, purgePage = 10, purgeBudgetMs = 1)
            .runOnce("purge-wall", batchSize = 100)
        val afterTight = endedBelowFloor(catalogId)
        assertThat(afterTight).isGreaterThan(0)
        val generous =
            ExpiryService(jdbi, purgePage = 1_000, purgeBudgetMs = 30_000)
                .runOnce("purge-wall", batchSize = 100)
        assertThat(generous.purgeTruncated).isFalse()
        assertThat(endedBelowFloor(catalogId)).isEqualTo(0)
        assertThat(generous.dataFilesPurged).isEqualTo(afterTight)
    }

    @Test
    fun `the knobs are refused rather than silently misbehaving`() {
        assertThat(runCatching { ExpiryService(jdbi, purgePage = 0) }.exceptionOrNull())
            .isInstanceOf(IllegalArgumentException::class.java)
        assertThat(runCatching { ExpiryService(jdbi, purgeBudgetMs = -1) }.exceptionOrNull())
            .isInstanceOf(IllegalArgumentException::class.java)
        // And the phase-A statement bound is DERIVED, not written down,
        // so lowering the admission bound lowers it with it.
        assertThat(ExpiryService(jdbi, commitLockTimeoutMs = 0).advanceBoundMs)
            .describedAs("no admission bound drops that term rather than zeroing the whole bound")
            .isEqualTo(15_000 / ExpiryService.BOUNDED_STATEMENTS_PER_ADVANCE.toLong())
        assertThat(ExpiryService(jdbi, commitLockTimeoutMs = 2_000).advanceBoundMs)
            .isEqualTo(1_000 / ExpiryService.BOUNDED_STATEMENTS_PER_ADVANCE.toLong())
        assertThat(ExpiryService(jdbi, commitLockTimeoutMs = 1).advanceBoundMs)
            .describedAs("floored at 1 ms: `statement_timeout = 0` means UNLIMITED")
            .isEqualTo(1)
    }
}
